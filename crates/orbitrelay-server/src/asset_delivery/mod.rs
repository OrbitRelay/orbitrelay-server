//! Server-local Asset Delivery Plane.
//!
//! This module deliberately stays at the application composition boundary. It
//! turns an already-authorized Document relation into a short-lived access
//! grant and serves immutable Asset bytes over a separate HTTP listener.

#![allow(clippy::module_name_repetitions)]

mod access;
mod config;
mod grant;
mod http;
mod range;

pub use access::{
    register_asset_access_query_handler, AssetAccessAuthorization, AssetAccessDescriptor,
    AssetAccessQueryHandler, ASSET_ACCESS_RESOLVE_QUERY_TYPE,
};
pub use config::AssetDeliveryConfig;
pub use grant::{
    AssetAccessGrant, AssetAccessGrantIssuer, DeliveryClock, GrantError, SystemDeliveryClock,
};
pub use http::AssetHttpListener;
pub use range::{parse_range, RangeParseError, ResolvedRange};

use std::sync::{Arc, RwLock};

use orbitrelay_asset_runtime::{AssetCatalog, AssetReader};

use crate::ServerError;

/// Composed, immutable dependencies for the Asset Delivery Plane.
#[derive(Clone)]
pub struct AssetDeliveryService {
    asset_catalog: Arc<dyn AssetCatalog>,
    asset_reader: Arc<dyn AssetReader>,
    grants: Arc<AssetAccessGrantIssuer>,
    public_base_url: Arc<RwLock<Option<String>>>,
}

impl AssetDeliveryService {
    /// Creates a delivery service over immutable Asset ports.
    pub fn new(
        asset_catalog: Arc<dyn AssetCatalog>,
        asset_reader: Arc<dyn AssetReader>,
        config: &AssetDeliveryConfig,
    ) -> Result<Self, ServerError> {
        config.validate()?;
        let clock: Arc<dyn DeliveryClock> = Arc::new(SystemDeliveryClock);
        Ok(Self {
            asset_catalog,
            asset_reader,
            grants: Arc::new(AssetAccessGrantIssuer::new(
                config.grant_ttl_seconds(),
                config.max_grants(),
                clock,
            )),
            public_base_url: Arc::new(RwLock::new(config.public_base_url().map(str::to_owned))),
        })
    }

    /// Returns the immutable Asset metadata port.
    #[must_use]
    pub fn asset_catalog(&self) -> Arc<dyn AssetCatalog> {
        Arc::clone(&self.asset_catalog)
    }

    /// Returns the immutable Asset byte-read port.
    #[must_use]
    pub fn asset_reader(&self) -> Arc<dyn AssetReader> {
        Arc::clone(&self.asset_reader)
    }

    /// Returns the in-memory Grant issuer.
    #[must_use]
    pub fn grant_issuer(&self) -> Arc<AssetAccessGrantIssuer> {
        Arc::clone(&self.grants)
    }

    /// Sets a loopback public URL after a port-zero listener has been bound.
    pub(crate) fn set_bound_public_base_url(&self, address: std::net::SocketAddr) {
        let mut base = self
            .public_base_url
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if base.is_none() {
            *base = Some(format!("http://127.0.0.1:{}", address.port()));
        }
    }

    /// Returns the configured public URL used to build access descriptors.
    pub(crate) fn public_base_url(&self) -> Option<String> {
        self.public_base_url
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}
