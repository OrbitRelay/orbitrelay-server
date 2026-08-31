//! Small streaming HTTP server for immutable Asset bytes.

use std::{collections::HashMap, sync::Arc, time::Duration};

use orbitrelay_asset::AssetId;
use orbitrelay_asset_runtime::{AssetByteRange, AssetReader};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{parse_range, AssetDeliveryConfig, AssetDeliveryService, RangeParseError};
use crate::ServerError;

const MAX_HEADER_BYTES: usize = 16 * 1024;

/// Owns the independent Asset HTTP listener and its bounded connection tasks.
pub struct AssetHttpListener {
    listener: TcpListener,
    config: AssetDeliveryConfig,
    delivery: Arc<AssetDeliveryService>,
    connections: Arc<Semaphore>,
    downloads: Arc<Semaphore>,
    tasks: JoinSet<Result<(), HttpTaskError>>,
}

impl AssetHttpListener {
    /// Binds an Asset listener without starting its accept loop.
    pub async fn bind(
        config: AssetDeliveryConfig,
        delivery: Arc<AssetDeliveryService>,
    ) -> Result<Self, ServerError> {
        config.validate()?;
        let listener = TcpListener::bind(config.listen_addr())
            .await
            .map_err(|_| ServerError::listener("failed to bind Asset HTTP listener"))?;
        let address = listener
            .local_addr()
            .map_err(|_| ServerError::listener("Asset HTTP listener address unavailable"))?;
        delivery.set_bound_public_base_url(address);
        tracing::info!(address = %address, "Asset HTTP listener bound");
        Ok(Self {
            connections: Arc::new(Semaphore::new(config.max_connections())),
            downloads: Arc::new(Semaphore::new(config.max_active_downloads())),
            listener,
            config,
            delivery,
            tasks: JoinSet::new(),
        })
    }

    /// Returns the OS-selected bound address.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, ServerError> {
        self.listener
            .local_addr()
            .map_err(|_| ServerError::listener("Asset HTTP listener address unavailable"))
    }

    /// Returns the number of active HTTP connection tasks.
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Runs the HTTP accept loop until cancellation.
    pub async fn run(&mut self, cancellation: CancellationToken) -> Result<(), ServerError> {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                completed = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(result) = completed {
                        match result {
                            Ok(Ok(())) => debug!("Asset HTTP connection closed"),
                            Ok(Err(error)) => warn!(error = %error, "Asset HTTP connection failed"),
                            Err(error) => warn!(error = %error, "Asset HTTP task failed"),
                        }
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, peer) = accepted
                        .map_err(|_| ServerError::listener("Asset HTTP accept failed"))?;
                    let Ok(connection_permit) = Arc::clone(&self.connections).try_acquire_owned() else {
                        let _ = reject_connection(stream, 503, "server busy").await;
                        warn!(peer = %peer, "Asset HTTP connection rejected at capacity");
                        continue;
                    };
                    self.tasks.spawn(handle_connection(
                        stream,
                        Arc::clone(&self.delivery),
                        self.config.clone(),
                        Arc::clone(&self.downloads),
                        cancellation.child_token(),
                        connection_permit,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Stops accepting and drains or aborts active HTTP connections.
    pub async fn shutdown(&mut self, grace_period: Duration) {
        let wait = async { while self.tasks.join_next().await.is_some() {} };
        if time::timeout(grace_period, wait).await.is_err() {
            self.tasks.abort_all();
            while self.tasks.join_next().await.is_some() {}
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum HttpTaskError {
    #[error("HTTP request failed")]
    Request,
    #[error("Asset backend failed while streaming")]
    Backend,
}

struct HttpRequest {
    method: String,
    target: String,
    headers: HashMap<String, String>,
}

async fn handle_connection(
    mut stream: TcpStream,
    delivery: Arc<AssetDeliveryService>,
    config: AssetDeliveryConfig,
    downloads: Arc<Semaphore>,
    cancellation: CancellationToken,
    _connection_permit: OwnedSemaphorePermit,
) -> Result<(), HttpTaskError> {
    let request = match time::timeout(config.idle_timeout(), read_request(&mut stream)).await {
        Ok(Ok(request)) => request,
        _ => {
            let _ = write_simple_response(&mut stream, 400, "bad request", &[]).await;
            return Ok(());
        }
    };
    let origin = request.headers.get("origin").cloned();
    if let Some(origin) = origin.as_deref() {
        if !config
            .allowed_origins()
            .iter()
            .any(|allowed| allowed == origin)
        {
            write_simple_response(&mut stream, 403, "forbidden", &[])
                .await
                .map_err(|_| HttpTaskError::Request)?;
            return Ok(());
        }
    }
    if request.method == "OPTIONS" {
        let mut extra = cors_headers(origin.as_deref());
        extra.push(("Content-Length", "0".to_owned()));
        write_response(&mut stream, 204, &extra, None, config.idle_timeout()).await?;
        return Ok(());
    }
    if request.method != "GET" && request.method != "HEAD" {
        write_simple_response(&mut stream, 405, "method not allowed", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    }
    let path = if request.target.contains('?') || request.target.contains('#') {
        write_simple_response(&mut stream, 400, "bad request", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    } else {
        request.target.as_str()
    };
    let Some(asset_text) = path.strip_prefix("/assets/") else {
        write_simple_response(&mut stream, 404, "not found", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    };
    if asset_text.is_empty() || asset_text.contains('/') {
        write_simple_response(&mut stream, 400, "bad request", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    }
    let asset_id: AssetId = match asset_text.parse() {
        Ok(asset_id) => asset_id,
        Err(_) => {
            write_simple_response(&mut stream, 400, "bad request", &[])
                .await
                .map_err(|_| HttpTaskError::Request)?;
            return Ok(());
        }
    };
    let Some(token) = bearer_token(request.headers.get("authorization")) else {
        write_simple_response(&mut stream, 401, "unauthorized", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    };
    if delivery.grant_issuer().validate(token, &asset_id).is_err() {
        write_simple_response(&mut stream, 401, "unauthorized", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    }
    let asset_lookup = match delivery.asset_catalog().get_asset(&asset_id).await {
        Ok(asset) => asset,
        Err(_) => {
            write_simple_response(&mut stream, 503, "service unavailable", &[])
                .await
                .map_err(|_| HttpTaskError::Request)?;
            return Ok(());
        }
    };
    let Some(asset) = asset_lookup else {
        write_simple_response(&mut stream, 404, "not found", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    };
    if asset.media_type().contains(['\r', '\n']) {
        write_simple_response(&mut stream, 500, "internal server error", &[])
            .await
            .map_err(|_| HttpTaskError::Request)?;
        return Ok(());
    }
    if request.method == "HEAD" {
        let mut headers = base_headers(
            asset.media_type(),
            asset.byte_length(),
            asset.content_hash().as_str(),
        );
        headers.extend(cors_headers(origin.as_deref()));
        write_response(&mut stream, 200, &headers, None, config.idle_timeout()).await?;
        return Ok(());
    }

    let resolved = match request.headers.get("range") {
        None => None,
        Some(value) => match parse_range(value, asset.byte_length()) {
            Ok(range) => range,
            Err(RangeParseError::Malformed) => {
                write_simple_response(&mut stream, 400, "bad range", &[])
                    .await
                    .map_err(|_| HttpTaskError::Request)?;
                return Ok(());
            }
            Err(RangeParseError::Unsatisfiable | RangeParseError::Multiple) => {
                let headers = [("Content-Range", format!("bytes */{}", asset.byte_length()))];
                write_simple_response(&mut stream, 416, "range not satisfiable", &headers)
                    .await
                    .map_err(|_| HttpTaskError::Request)?;
                return Ok(());
            }
        },
    };
    let (status, start, length, content_range) = match resolved {
        None => (200, 0, asset.byte_length(), None),
        Some(range) => (
            206,
            range.start(),
            range.length(),
            Some(format!(
                "bytes {}-{}/{}",
                range.start(),
                range.end(),
                asset.byte_length()
            )),
        ),
    };
    if length > 0 {
        let Ok(download_permit) = downloads.try_acquire_owned() else {
            write_simple_response(&mut stream, 503, "server busy", &[])
                .await
                .map_err(|_| HttpTaskError::Request)?;
            return Ok(());
        };
        let mut headers = base_headers(asset.media_type(), length, asset.content_hash().as_str());
        if let Some(content_range) = content_range {
            headers.push(("Content-Range", content_range));
        }
        headers.extend(cors_headers(origin.as_deref()));
        write_response(&mut stream, status, &headers, None, config.idle_timeout()).await?;
        stream_asset(
            &mut stream,
            delivery.asset_reader(),
            asset_id,
            start,
            length,
            config.chunk_size(),
            config.idle_timeout(),
            cancellation,
            download_permit,
        )
        .await?;
    } else {
        let mut headers = base_headers(asset.media_type(), 0, asset.content_hash().as_str());
        if let Some(content_range) = content_range {
            headers.push(("Content-Range", content_range));
        }
        headers.extend(cors_headers(origin.as_deref()));
        write_response(&mut stream, status, &headers, None, config.idle_timeout()).await?;
    }
    Ok(())
}

fn bearer_token(value: Option<&String>) -> Option<&str> {
    let (scheme, value) = value?.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || value.is_empty()
        || value.contains(char::is_whitespace)
    {
        return None;
    }
    Some(value)
}

fn base_headers(media_type: &str, length: u64, hash: &str) -> Vec<(&'static str, String)> {
    vec![
        ("Content-Type", media_type.to_owned()),
        ("Content-Length", length.to_string()),
        ("Accept-Ranges", "bytes".to_owned()),
        ("ETag", format!("\"sha256-{hash}\"")),
        ("Connection", "close".to_owned()),
    ]
}

fn cors_headers(origin: Option<&str>) -> Vec<(&'static str, String)> {
    let Some(origin) = origin else {
        return Vec::new();
    };
    vec![
        ("Access-Control-Allow-Origin", origin.to_owned()),
        (
            "Access-Control-Allow-Methods",
            "GET, HEAD, OPTIONS".to_owned(),
        ),
        (
            "Access-Control-Allow-Headers",
            "Authorization, Range".to_owned(),
        ),
        (
            "Access-Control-Expose-Headers",
            "Content-Length, Content-Range, ETag, Accept-Ranges".to_owned(),
        ),
        ("Vary", "Origin".to_owned()),
    ]
}

#[allow(
    clippy::too_many_arguments,
    reason = "the streaming loop receives each explicit cancellation and ownership boundary"
)]
async fn stream_asset(
    stream: &mut TcpStream,
    reader: Arc<dyn AssetReader>,
    asset_id: AssetId,
    mut offset: u64,
    mut remaining: u64,
    chunk_size: usize,
    idle_timeout: Duration,
    cancellation: CancellationToken,
    _download_permit: OwnedSemaphorePermit,
) -> Result<(), HttpTaskError> {
    while remaining > 0 {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let requested = remaining.min(chunk_size as u64);
        let range = AssetByteRange::new(offset, requested).map_err(|_| HttpTaskError::Backend)?;
        let chunk = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            result = reader.read_range(&asset_id, range) => result.map_err(|_| HttpTaskError::Backend)?,
        };
        if chunk.offset() != offset
            || chunk.bytes().is_empty()
            || chunk.bytes().len() as u64 > requested
            || chunk.bytes().len() as u64 > remaining
        {
            return Err(HttpTaskError::Backend);
        }
        time::timeout(idle_timeout, stream.write_all(chunk.bytes().as_ref()))
            .await
            .map_err(|_| HttpTaskError::Request)?
            .map_err(|_| HttpTaskError::Request)?;
        let written = chunk.bytes().len() as u64;
        offset = offset.checked_add(written).ok_or(HttpTaskError::Backend)?;
        remaining -= written;
    }
    Ok(())
}

async fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, HttpTaskError> {
    let mut buffer = Vec::with_capacity(1024);
    loop {
        if buffer.len() >= MAX_HEADER_BYTES {
            return Err(HttpTaskError::Request);
        }
        let mut chunk = [0_u8; 1024];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|_| HttpTaskError::Request)?;
        if read == 0 {
            return Err(HttpTaskError::Request);
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let text = std::str::from_utf8(&buffer).map_err(|_| HttpTaskError::Request)?;
    let head = text
        .split("\r\n\r\n")
        .next()
        .ok_or(HttpTaskError::Request)?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or(HttpTaskError::Request)?;
    let mut fields = request_line.split_whitespace();
    let method = fields.next().ok_or(HttpTaskError::Request)?.to_owned();
    let target = fields.next().ok_or(HttpTaskError::Request)?.to_owned();
    let version = fields.next().ok_or(HttpTaskError::Request)?;
    if !version.starts_with("HTTP/1.") || fields.next().is_some() {
        return Err(HttpTaskError::Request);
    }
    let mut headers = HashMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').ok_or(HttpTaskError::Request)?;
        if name.is_empty() || value.contains('\r') || value.contains('\n') {
            return Err(HttpTaskError::Request);
        }
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    if headers
        .get("content-length")
        .is_some_and(|value| value != "0")
    {
        return Err(HttpTaskError::Request);
    }
    Ok(HttpRequest {
        method,
        target,
        headers,
    })
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    headers: &[(&str, String)],
    body: Option<&[u8]>,
    timeout: Duration,
) -> Result<(), HttpTaskError> {
    let mut response = format!("HTTP/1.1 {status} {}\r\n", reason(status));
    for (name, value) in headers {
        if value.contains('\r') || value.contains('\n') {
            return Err(HttpTaskError::Request);
        }
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    time::timeout(timeout, stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| HttpTaskError::Request)?
        .map_err(|_| HttpTaskError::Request)?;
    if let Some(body) = body {
        time::timeout(timeout, stream.write_all(body))
            .await
            .map_err(|_| HttpTaskError::Request)?
            .map_err(|_| HttpTaskError::Request)?;
    }
    Ok(())
}

async fn write_simple_response(
    stream: &mut TcpStream,
    status: u16,
    message: &str,
    extra: &[(&str, String)],
) -> Result<(), HttpTaskError> {
    let body = format!("{message}\n");
    let mut headers = vec![
        ("Content-Type", "text/plain; charset=utf-8".to_owned()),
        ("Content-Length", body.len().to_string()),
        ("Connection", "close".to_owned()),
    ];
    headers.extend_from_slice(extra);
    write_response(
        stream,
        status,
        &headers,
        Some(body.as_bytes()),
        Duration::from_secs(5),
    )
    .await
}

async fn reject_connection(
    mut stream: TcpStream,
    status: u16,
    message: &str,
) -> Result<(), HttpTaskError> {
    write_simple_response(&mut stream, status, message, &[]).await
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        416 => "Range Not Satisfiable",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    }
}
