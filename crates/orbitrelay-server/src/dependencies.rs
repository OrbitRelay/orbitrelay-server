//! Explicit external dependency ports used by the composition root.

use std::sync::Arc;

use async_trait::async_trait;
use orbitrelay_document_runtime::{DocumentReadAuthorizationError, DocumentReadAuthorizer};
use orbitrelay_protocol::Action;
use orbitrelay_query::{QueryActorContext, QueryType};
use orbitrelay_runtime::{ActionAuthorizer, AuthorizationError};
use orbitrelay_transport::{
    ActorBinding, IdentityError, IdentityResolver, InboundCredentials,
    SubscriptionAuthorizationError, SubscriptionAuthorizer, SubscriptionRequest,
};

/// Dependencies that must be supplied by the embedding process.
#[derive(Clone)]
pub struct ServerDependencies {
    action_authorizer: Arc<dyn ActionAuthorizer>,
    identity_resolver: Arc<dyn IdentityResolver>,
    subscription_authorizer: Arc<dyn SubscriptionAuthorizer>,
}

impl ServerDependencies {
    /// Creates an explicit dependency set. No production allow-all defaults are provided.
    #[must_use]
    pub fn new(
        action_authorizer: Arc<dyn ActionAuthorizer>,
        identity_resolver: Arc<dyn IdentityResolver>,
        subscription_authorizer: Arc<dyn SubscriptionAuthorizer>,
    ) -> Self {
        Self {
            action_authorizer,
            identity_resolver,
            subscription_authorizer,
        }
    }

    /// Returns the action authorization port.
    #[must_use]
    pub fn action_authorizer(&self) -> Arc<dyn ActionAuthorizer> {
        Arc::clone(&self.action_authorizer)
    }

    /// Returns the connection identity resolver.
    #[must_use]
    pub fn identity_resolver(&self) -> Arc<dyn IdentityResolver> {
        Arc::clone(&self.identity_resolver)
    }

    /// Returns the subscription authorization port.
    #[must_use]
    pub fn subscription_authorizer(&self) -> Arc<dyn SubscriptionAuthorizer> {
        Arc::clone(&self.subscription_authorizer)
    }
}

/// A safe production placeholder that rejects every action until configured.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectAllActionAuthorizer;

#[async_trait]
impl ActionAuthorizer for RejectAllActionAuthorizer {
    async fn authorize(&self, _action: &Action) -> Result<(), AuthorizationError> {
        Err(AuthorizationError::new(
            "action authorization is not configured",
        ))
    }
}

/// A safe production placeholder that rejects every authentication attempt.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectAllIdentityResolver;

#[async_trait]
impl IdentityResolver for RejectAllIdentityResolver {
    async fn resolve(
        &self,
        _connection_id: &orbitrelay_transport::ConnectionId,
        _credentials: &InboundCredentials,
    ) -> Result<ActorBinding, IdentityError> {
        Err(IdentityError::CredentialsRejected {
            detail: "identity resolution is not configured".to_owned(),
        })
    }
}

/// A safe production placeholder that rejects every subscription.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectAllSubscriptionAuthorizer;

#[async_trait]
impl SubscriptionAuthorizer for RejectAllSubscriptionAuthorizer {
    async fn authorize(
        &self,
        _binding: &ActorBinding,
        _request: &SubscriptionRequest,
    ) -> Result<(), SubscriptionAuthorizationError> {
        Err(SubscriptionAuthorizationError::Rejected {
            detail: "subscription authorization is not configured".to_owned(),
        })
    }
}

/// Safe production placeholder that rejects every Document read.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectAllDocumentReadAuthorizer;

#[async_trait]
impl DocumentReadAuthorizer for RejectAllDocumentReadAuthorizer {
    async fn authorize_session_read(
        &self,
        _actor: &QueryActorContext,
        _session_id: &orbitrelay_protocol::SessionId,
        _query_type: &QueryType,
    ) -> Result<(), DocumentReadAuthorizationError> {
        Err(DocumentReadAuthorizationError::Unauthorized)
    }
}
