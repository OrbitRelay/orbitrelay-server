//! Explicitly unsafe development-only identity, authorization, and echo behavior.

use async_trait::async_trait;
use orbitrelay_core::Metadata;
use orbitrelay_document_runtime::{DocumentReadAuthorizationError, DocumentReadAuthorizer};
use orbitrelay_protocol::SessionId;
use orbitrelay_protocol::{Action, ActorId, EventType};
use orbitrelay_query::{QueryActorContext, QueryType};
use orbitrelay_runtime::{
    ActionAuthorizer, ActionHandler, AuthorizationError, EventDraft, HandlerError, RuntimeContext,
};
use orbitrelay_transport::{
    ActorBinding, IdentityError, IdentityResolver, IdentitySource, InboundCredentials,
    SubscriptionAuthorizationError, SubscriptionAuthorizer, SubscriptionRequest,
};
use tracing::{info, warn};

/// Development credential scheme accepted by [`DevelopmentIdentityResolver`].
pub const DEVELOPMENT_IDENTITY_SCHEME: &str = "development";

/// Resolves a development credential containing a plain ActorId.
///
/// This resolver performs no authentication and must never be exposed publicly.
#[derive(Clone, Copy, Debug, Default)]
pub struct DevelopmentIdentityResolver;

#[async_trait]
impl IdentityResolver for DevelopmentIdentityResolver {
    async fn resolve(
        &self,
        connection_id: &orbitrelay_transport::ConnectionId,
        credentials: &InboundCredentials,
    ) -> Result<ActorBinding, IdentityError> {
        if credentials.scheme() != DEVELOPMENT_IDENTITY_SCHEME {
            warn!(connection_id = %connection_id, "development authentication rejected");
            return Err(IdentityError::CredentialsRejected {
                detail: "unsupported development credential scheme".to_owned(),
            });
        }
        let actor_id = ActorId::parse(credentials.credential()).map_err(|_| {
            warn!(connection_id = %connection_id, "development authentication rejected");
            IdentityError::CredentialsRejected {
                detail: "development credential is not a valid actor ID".to_owned(),
            }
        })?;
        info!(connection_id = %connection_id, actor_id = %actor_id, "development authentication succeeded");
        Ok(ActorBinding::new(
            actor_id,
            IdentitySource::new(DEVELOPMENT_IDENTITY_SCHEME),
        ))
    }
}

/// Allows Actions only when the process explicitly runs in development mode.
#[derive(Clone, Copy, Debug, Default)]
pub struct DevelopmentActionAuthorizer;

#[async_trait]
impl ActionAuthorizer for DevelopmentActionAuthorizer {
    async fn authorize(&self, action: &Action) -> Result<(), AuthorizationError> {
        match action.action_type().as_str() {
            "dev.echo"
            | orbitrelay_canvas::STROKE_BEGIN_ACTION_TYPE
            | orbitrelay_canvas::STROKE_APPEND_ACTION_TYPE
            | orbitrelay_canvas::STROKE_END_ACTION_TYPE
            | orbitrelay_canvas::STROKE_CANCEL_ACTION_TYPE
            | orbitrelay_canvas::STROKE_REMOVE_ACTION_TYPE => Ok(()),
            _ => Err(AuthorizationError::new(
                "development action type is not enabled",
            )),
        }
    }
}

/// Allows subscriptions only when the process explicitly runs in development mode.
#[derive(Clone, Copy, Debug, Default)]
pub struct DevelopmentSubscriptionAuthorizer;

#[async_trait]
impl SubscriptionAuthorizer for DevelopmentSubscriptionAuthorizer {
    async fn authorize(
        &self,
        binding: &ActorBinding,
        request: &SubscriptionRequest,
    ) -> Result<(), SubscriptionAuthorizationError> {
        info!(actor_id = %binding.actor_id(), session_id = %request.session_id(), "development subscription authorized");
        Ok(())
    }
}

/// Allows authenticated Development actors to read only the Development
/// Session's immutable Document catalog.
#[derive(Clone, Debug)]
pub struct DevelopmentDocumentReadAuthorizer {
    session_id: SessionId,
}

impl DevelopmentDocumentReadAuthorizer {
    /// Creates an authorizer scoped to one Development Session.
    #[must_use]
    pub fn new(session_id: SessionId) -> Self {
        Self { session_id }
    }

    /// Returns the Session this authorizer permits.
    #[must_use]
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }
}

#[async_trait]
impl DocumentReadAuthorizer for DevelopmentDocumentReadAuthorizer {
    async fn authorize_session_read(
        &self,
        _actor: &QueryActorContext,
        session_id: &SessionId,
        _query_type: &QueryType,
    ) -> Result<(), DocumentReadAuthorizationError> {
        if session_id == &self.session_id {
            Ok(())
        } else {
            Err(DocumentReadAuthorizationError::Unauthorized)
        }
    }
}

pub(crate) struct DevelopmentEchoHandler;

#[async_trait]
impl ActionHandler for DevelopmentEchoHandler {
    async fn validate(
        &self,
        _action: &Action,
        _context: &RuntimeContext,
    ) -> Result<(), HandlerError> {
        Ok(())
    }

    async fn handle(
        &self,
        action: &Action,
        _context: &RuntimeContext,
    ) -> Result<Vec<EventDraft>, HandlerError> {
        Ok(vec![EventDraft::new(
            EventType::new("dev.echoed"),
            action.payload().clone(),
            Metadata::new(),
        )])
    }
}
