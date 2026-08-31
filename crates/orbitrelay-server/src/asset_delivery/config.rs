//! Asset HTTP listener configuration.

use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use crate::ServerError;

/// Configuration for the independent Asset Delivery HTTP listener.
#[derive(Clone, Debug, PartialEq)]
pub struct AssetDeliveryConfig {
    enabled: bool,
    listen_addr: SocketAddr,
    public_base_url: Option<String>,
    max_connections: usize,
    max_active_downloads: usize,
    chunk_size: usize,
    idle_timeout_milliseconds: u64,
    grant_ttl_seconds: i64,
    max_grants: usize,
    allowed_origins: Vec<String>,
}

impl Default for AssetDeliveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 8081)),
            public_base_url: None,
            max_connections: 64,
            max_active_downloads: 8,
            chunk_size: 64 * 1024,
            idle_timeout_milliseconds: 30_000,
            grant_ttl_seconds: 10 * 60,
            max_grants: 4096,
            allowed_origins: Vec::new(),
        }
    }
}

impl AssetDeliveryConfig {
    /// Validates listener, Grant, URL and CORS invariants.
    pub fn validate(&self) -> Result<(), ServerError> {
        if self.max_connections == 0 || self.max_active_downloads == 0 {
            return Err(ServerError::config(
                "asset delivery connection and download limits must be greater than zero",
            ));
        }
        if self.chunk_size == 0 {
            return Err(ServerError::config(
                "asset delivery chunk size must be greater than zero",
            ));
        }
        if self.idle_timeout_milliseconds == 0 {
            return Err(ServerError::config(
                "asset delivery idle timeout must be greater than zero",
            ));
        }
        if self.grant_ttl_seconds <= 0 {
            return Err(ServerError::config(
                "asset delivery Grant TTL must be positive",
            ));
        }
        if self.max_grants == 0 {
            return Err(ServerError::config(
                "asset delivery max grants must be greater than zero",
            ));
        }
        if let Some(url) = &self.public_base_url {
            validate_public_base_url(url)?;
        } else if self.enabled && !self.listen_addr.ip().is_loopback() {
            return Err(ServerError::config(
                "asset public base URL is required for non-loopback binds",
            ));
        }
        Ok(())
    }

    /// Whether the independent listener should be started.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
    /// Returns the bind address.
    #[must_use]
    pub const fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
    /// Returns the explicitly advertised base URL.
    #[must_use]
    pub fn public_base_url(&self) -> Option<&str> {
        self.public_base_url.as_deref()
    }
    /// Returns the maximum HTTP connections.
    #[must_use]
    pub const fn max_connections(&self) -> usize {
        self.max_connections
    }
    /// Returns the maximum concurrent body downloads.
    #[must_use]
    pub const fn max_active_downloads(&self) -> usize {
        self.max_active_downloads
    }
    /// Returns the bounded body chunk size.
    #[must_use]
    pub const fn chunk_size(&self) -> usize {
        self.chunk_size
    }
    /// Returns the idle timeout.
    #[must_use]
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_timeout_milliseconds)
    }
    /// Returns the Grant TTL in seconds.
    #[must_use]
    pub const fn grant_ttl_seconds(&self) -> i64 {
        self.grant_ttl_seconds
    }
    /// Returns the maximum number of live Grant records.
    #[must_use]
    pub const fn max_grants(&self) -> usize {
        self.max_grants
    }
    /// Returns the configured CORS origins.
    #[must_use]
    pub fn allowed_origins(&self) -> &[String] {
        &self.allowed_origins
    }

    /// Enables or disables Asset Delivery.
    #[must_use]
    pub const fn with_enabled(mut self, value: bool) -> Self {
        self.enabled = value;
        self
    }
    /// Sets the HTTP bind address.
    #[must_use]
    pub const fn with_listen_addr(mut self, value: SocketAddr) -> Self {
        self.listen_addr = value;
        self
    }
    /// Sets the advertised public base URL.
    #[must_use]
    pub fn with_public_base_url(mut self, value: impl Into<String>) -> Self {
        self.public_base_url = Some(value.into());
        self
    }
    /// Clears the advertised public base URL.
    #[must_use]
    pub fn without_public_base_url(mut self) -> Self {
        self.public_base_url = None;
        self
    }
    /// Sets the HTTP connection limit.
    #[must_use]
    pub const fn with_max_connections(mut self, value: usize) -> Self {
        self.max_connections = value;
        self
    }
    /// Sets the active download limit.
    #[must_use]
    pub const fn with_max_active_downloads(mut self, value: usize) -> Self {
        self.max_active_downloads = value;
        self
    }
    /// Sets the bounded body chunk size.
    #[must_use]
    pub const fn with_chunk_size(mut self, value: usize) -> Self {
        self.chunk_size = value;
        self
    }
    /// Sets the idle timeout in milliseconds.
    #[must_use]
    pub const fn with_idle_timeout_milliseconds(mut self, value: u64) -> Self {
        self.idle_timeout_milliseconds = value;
        self
    }
    /// Sets the Grant TTL in seconds.
    #[must_use]
    pub const fn with_grant_ttl_seconds(mut self, value: i64) -> Self {
        self.grant_ttl_seconds = value;
        self
    }
    /// Sets the bounded Grant registry size.
    #[must_use]
    pub const fn with_max_grants(mut self, value: usize) -> Self {
        self.max_grants = value;
        self
    }
    /// Replaces the CORS origin allowlist.
    #[must_use]
    pub fn with_allowed_origins(mut self, origins: impl IntoIterator<Item = String>) -> Self {
        self.allowed_origins = origins.into_iter().collect();
        self
    }
}

fn validate_public_base_url(value: &str) -> Result<(), ServerError> {
    let Some((scheme, remainder)) = value.split_once("://") else {
        return Err(ServerError::config(
            "asset public base URL must be absolute",
        ));
    };
    if scheme != "http" && scheme != "https" || value.chars().any(char::is_whitespace) {
        return Err(ServerError::config(
            "asset public base URL must use http or https",
        ));
    }
    let mut remainder_parts = remainder.splitn(2, '/');
    let authority = remainder_parts.next().unwrap_or_default();
    let path = remainder_parts.next();
    if authority.is_empty()
        || path.is_some_and(|path| !path.is_empty())
        || remainder.contains('?')
        || remainder.contains('#')
    {
        return Err(ServerError::config(
            "asset public base URL must include a host without query or fragment",
        ));
    }
    let host = if let Some(rest) = authority.strip_prefix('[') {
        let Some(close) = rest.find(']') else {
            return Err(ServerError::config(
                "asset public base URL has an invalid IPv6 authority",
            ));
        };
        let host = &rest[..close];
        let suffix = &rest[close + 1..];
        if !suffix.is_empty() && (!suffix.starts_with(':') || suffix[1..].parse::<u16>().is_err()) {
            return Err(ServerError::config(
                "asset public base URL has an invalid port",
            ));
        }
        host
    } else if authority.matches(':').count() > 1 {
        return Err(ServerError::config(
            "asset public base URL has an invalid IPv6 authority",
        ));
    } else if let Some((host, port)) = authority.rsplit_once(':') {
        if host.is_empty() || port.parse::<u16>().is_err() {
            return Err(ServerError::config(
                "asset public base URL has an invalid port",
            ));
        }
        host
    } else {
        authority
    };
    if host.is_empty() {
        return Err(ServerError::config(
            "asset public base URL must include a host",
        ));
    }
    if host
        .trim_matches(['[', ']'])
        .parse::<IpAddr>()
        .is_ok_and(|ip| ip.is_unspecified())
    {
        return Err(ServerError::config(
            "asset public base URL must not advertise an unspecified host",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::AssetDeliveryConfig;

    #[test]
    fn loopback_port_zero_can_derive_public_url_after_bind() {
        let config = AssetDeliveryConfig::default()
            .with_enabled(true)
            .with_listen_addr("127.0.0.1:0".parse::<SocketAddr>().expect("address"));
        assert!(config.validate().is_ok());
        assert!(config.public_base_url().is_none());
    }

    #[test]
    fn non_loopback_bind_requires_explicit_non_unspecified_public_host() {
        let config = AssetDeliveryConfig::default()
            .with_enabled(true)
            .with_listen_addr("0.0.0.0:8081".parse::<SocketAddr>().expect("address"));
        assert!(config.validate().is_err());
        assert!(AssetDeliveryConfig::default()
            .with_enabled(true)
            .with_listen_addr("0.0.0.0:8081".parse::<SocketAddr>().expect("address"))
            .with_public_base_url("http://192.168.1.10:8081")
            .validate()
            .is_ok());
        assert!(AssetDeliveryConfig::default()
            .with_enabled(true)
            .with_public_base_url("http://0.0.0.0:8081")
            .validate()
            .is_err());
        assert!(AssetDeliveryConfig::default()
            .with_public_base_url("http://localhost:not-a-port")
            .validate()
            .is_err());
        assert!(AssetDeliveryConfig::default()
            .with_public_base_url("http://localhost/prefix")
            .validate()
            .is_err());
    }
}
