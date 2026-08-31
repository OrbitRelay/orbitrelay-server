//! `asset.access.resolve` Query adapter and Delivery DTOs.

use std::sync::Arc;

use async_trait::async_trait;
use orbitrelay_core::Timestamp;
use orbitrelay_document_runtime::{DocumentCatalog, DocumentReadAuthorizer};
use orbitrelay_protocol::Payload;
use orbitrelay_query::{
    QueryActorContext, QueryHandler, QueryHandlerError, QueryRegistry, QueryRegistryError,
    QueryRequest, QueryType, QueryTypeError,
};
use serde::{Deserialize, Serialize};

use super::{grant::GrantError, AssetDeliveryService};

/// Stable Query type for resolving a short-lived Asset access capability.
pub const ASSET_ACCESS_RESOLVE_QUERY_TYPE: &str = "asset.access.resolve";

/// Explicit authorization method carried by an access descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "snake_case")]
pub enum AssetAccessAuthorization {
    /// A short-lived HTTP Bearer token.
    Bearer {
        /// The one-time-disclosed short-lived bearer token.
        token: String,
    },
}

impl AssetAccessAuthorization {
    /// Returns the Bearer token when this descriptor uses Bearer auth.
    #[must_use]
    pub fn bearer_token(&self) -> Option<&str> {
        match self {
            Self::Bearer { token } => Some(token),
        }
    }
}

/// Application-level descriptor for an Asset Delivery locator.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetAccessDescriptor {
    asset_id: orbitrelay_asset::AssetId,
    delivery_kind: String,
    url: String,
    authorization: AssetAccessAuthorization,
    expires_at: Timestamp,
    supports_range: bool,
}

impl AssetAccessDescriptor {
    /// Creates an HTTP Bearer access descriptor.
    #[must_use]
    pub fn http_bearer(
        asset_id: orbitrelay_asset::AssetId,
        url: String,
        token: String,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            asset_id,
            delivery_kind: "http".to_owned(),
            url,
            authorization: AssetAccessAuthorization::Bearer { token },
            expires_at,
            supports_range: true,
        }
    }

    /// Returns the Asset identity.
    #[must_use]
    pub const fn asset_id(&self) -> &orbitrelay_asset::AssetId {
        &self.asset_id
    }
    /// Returns the delivery backend kind.
    #[must_use]
    pub fn delivery_kind(&self) -> &str {
        &self.delivery_kind
    }
    /// Returns the public delivery URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
    /// Returns the authorization method.
    #[must_use]
    pub const fn authorization(&self) -> &AssetAccessAuthorization {
        &self.authorization
    }
    /// Returns the grant expiration timestamp.
    #[must_use]
    pub const fn expires_at(&self) -> &Timestamp {
        &self.expires_at
    }
    /// Whether byte ranges are supported.
    #[must_use]
    pub const fn supports_range(&self) -> bool {
        self.supports_range
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveAssetAccessPayload {
    document_id: orbitrelay_document::DocumentId,
}

/// Handles `asset.access.resolve` with resolve-then-authorize ordering.
pub struct AssetAccessQueryHandler {
    query_type: QueryType,
    document_catalog: Arc<dyn DocumentCatalog>,
    authorizer: Arc<dyn DocumentReadAuthorizer>,
    delivery: Arc<AssetDeliveryService>,
}

impl AssetAccessQueryHandler {
    /// Creates a handler over the Document authorization and Delivery ports.
    pub fn new(
        document_catalog: Arc<dyn DocumentCatalog>,
        authorizer: Arc<dyn DocumentReadAuthorizer>,
        delivery: Arc<AssetDeliveryService>,
    ) -> Result<Self, QueryTypeError> {
        Ok(Self {
            query_type: QueryType::new(ASSET_ACCESS_RESOLVE_QUERY_TYPE)?,
            document_catalog,
            authorizer,
            delivery,
        })
    }
}

#[async_trait]
impl QueryHandler for AssetAccessQueryHandler {
    fn query_type(&self) -> &QueryType {
        &self.query_type
    }

    async fn execute(
        &self,
        actor: &QueryActorContext,
        request: QueryRequest,
    ) -> Result<Payload, QueryHandlerError> {
        let payload: ResolveAssetAccessPayload = decode_payload(request.payload())?;
        let document = self
            .document_catalog
            .get_document(&payload.document_id)
            .await
            .map_err(|_| QueryHandlerError::Unavailable)?
            .ok_or(QueryHandlerError::NotFound)?;

        self.authorizer
            .authorize_session_read(actor, document.session_id(), &self.query_type)
            .await
            .map_err(|error| match error {
                orbitrelay_document_runtime::DocumentReadAuthorizationError::Unauthorized => {
                    QueryHandlerError::Unauthorized
                }
                orbitrelay_document_runtime::DocumentReadAuthorizationError::Unavailable => {
                    QueryHandlerError::Unavailable
                }
                orbitrelay_document_runtime::DocumentReadAuthorizationError::Internal => {
                    QueryHandlerError::Internal
                }
                _ => QueryHandlerError::Internal,
            })?;

        let asset_id = document.source_asset_id().clone();
        let descriptor = self
            .delivery
            .asset_catalog()
            .get_asset(&asset_id)
            .await
            .map_err(|_| QueryHandlerError::Unavailable)?
            .ok_or(QueryHandlerError::Internal)?;
        if descriptor.asset_id() != &asset_id {
            return Err(QueryHandlerError::Internal);
        }
        let Some(base_url) = self.delivery.public_base_url() else {
            return Err(QueryHandlerError::Unavailable);
        };
        let (token, grant) = self
            .delivery
            .grant_issuer()
            .issue(
                asset_id.clone(),
                actor.actor_id().clone(),
                document.session_id().clone(),
                document.document_id().clone(),
            )
            .map_err(|error| match error {
                GrantError::Unavailable => QueryHandlerError::Unavailable,
                GrantError::Randomness => QueryHandlerError::Internal,
                GrantError::Unauthorized => QueryHandlerError::Unauthorized,
            })?;
        let descriptor = AssetAccessDescriptor::http_bearer(
            asset_id,
            format!(
                "{}/assets/{}",
                base_url.trim_end_matches('/'),
                grant.asset_id()
            ),
            token,
            grant.expires_at().clone(),
        );
        encode_payload(&descriptor)
    }
}

/// Registers the Asset access handler in the generic Query registry.
pub fn register_asset_access_query_handler(
    registry: &mut QueryRegistry,
    document_catalog: Arc<dyn DocumentCatalog>,
    authorizer: Arc<dyn DocumentReadAuthorizer>,
    delivery: Arc<AssetDeliveryService>,
) -> Result<(), QueryRegistryError> {
    let handler =
        AssetAccessQueryHandler::new(document_catalog, authorizer, delivery).map_err(|_| {
            QueryRegistryError::DuplicateQueryType {
                query_type: QueryType::new(ASSET_ACCESS_RESOLVE_QUERY_TYPE)
                    .expect("static query type should remain valid"),
            }
        })?;
    registry.register(Arc::new(handler))
}

fn decode_payload<T: for<'de> Deserialize<'de>>(payload: &Payload) -> Result<T, QueryHandlerError> {
    let value = serde_json::to_value(payload).map_err(|_| QueryHandlerError::InvalidQuery)?;
    serde_json::from_value(value).map_err(|_| QueryHandlerError::InvalidQuery)
}

fn encode_payload<T: Serialize>(value: &T) -> Result<Payload, QueryHandlerError> {
    let value = serde_json::to_value(value).map_err(|_| QueryHandlerError::Internal)?;
    serde_json::from_value(value).map_err(|_| QueryHandlerError::Internal)
}

#[cfg(test)]
mod tests {
    use super::{AssetAccessAuthorization, AssetAccessDescriptor};
    use orbitrelay_asset::AssetId;
    use orbitrelay_core::Timestamp;

    #[test]
    fn descriptor_has_explicit_bearer_wire_shape() {
        let descriptor = AssetAccessDescriptor::http_bearer(
            AssetId::new(),
            "http://127.0.0.1:8081/assets/a".to_owned(),
            "token".to_owned(),
            Timestamp::from_unix_timestamp(1_700_000_000).expect("timestamp"),
        );
        let value = serde_json::to_value(&descriptor).expect("descriptor should encode");
        assert_eq!(value["delivery_kind"], "http");
        assert_eq!(value["authorization"]["type"], "bearer");
        assert!(matches!(
            descriptor.authorization(),
            AssetAccessAuthorization::Bearer { .. }
        ));
        assert!(value["url"]
            .as_str()
            .is_some_and(|url| !url.contains("?token=")));
    }
}
