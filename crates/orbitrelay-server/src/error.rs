//! Errors at the server composition boundary.

use thiserror::Error;

use crate::LifecycleState;

/// Errors produced by invalid process lifecycle transitions.
#[derive(Debug, Eq, Error, PartialEq)]
#[error("invalid server lifecycle transition from `{from}` to `{to}`")]
pub struct LifecycleError {
    from: LifecycleState,
    to: LifecycleState,
}

impl LifecycleError {
    pub(crate) const fn new(from: LifecycleState, to: LifecycleState) -> Self {
        Self { from, to }
    }

    /// Returns the state from which the transition was attempted.
    #[must_use]
    pub const fn from(&self) -> LifecycleState {
        self.from
    }

    /// Returns the requested target state.
    #[must_use]
    pub const fn to(&self) -> LifecycleState {
        self.to
    }
}

/// Errors produced while loading, composing, or shutting down the server.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServerError {
    /// The process lifecycle rejected a state transition.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),

    /// The process configuration is missing or invalid.
    #[error("invalid server configuration: {message}")]
    Config {
        /// A backend-neutral configuration message.
        message: String,
    },

    /// A required dependency could not be composed.
    #[error("server bootstrap failed: {message}")]
    Bootstrap {
        /// A backend-neutral bootstrap message.
        message: String,
    },

    /// The local node could not be registered or transitioned.
    #[error("node lifecycle failed: {message}")]
    NodeLifecycle {
        /// A backend-neutral lifecycle message.
        message: String,
    },

    /// The WebSocket listener could not bind or accept a connection.
    #[error("WebSocket listener failed: {message}")]
    Listener {
        /// A safe listener diagnostic.
        message: String,
    },

    /// A connection task failed at the server boundary.
    #[error("WebSocket connection failed: {message}")]
    Connection {
        /// A safe connection diagnostic.
        message: String,
    },

    /// The process could not wait for or complete shutdown.
    #[error("server shutdown failed: {message}")]
    Shutdown {
        /// A backend-neutral shutdown message.
        message: String,
    },
}

impl ServerError {
    pub(crate) fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    pub(crate) fn bootstrap(message: impl Into<String>) -> Self {
        Self::Bootstrap {
            message: message.into(),
        }
    }

    pub(crate) fn node_lifecycle(message: impl Into<String>) -> Self {
        Self::NodeLifecycle {
            message: message.into(),
        }
    }

    pub(crate) fn listener(message: impl Into<String>) -> Self {
        Self::Listener {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerError;

    #[test]
    fn formats_without_exposing_backend_details() {
        let error = ServerError::bootstrap("dependency initialization failed");

        assert_eq!(
            error.to_string(),
            "server bootstrap failed: dependency initialization failed"
        );
    }
}
