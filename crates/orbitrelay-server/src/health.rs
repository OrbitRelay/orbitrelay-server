//! Read-only process health state.

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// The externally observable health of the server process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HealthState {
    /// Dependencies and lifecycle state are still initializing.
    Starting,
    /// The process is ready to perform its configured work.
    Ready,
    /// The process is running but one or more dependencies are impaired.
    Degraded,
    /// Graceful shutdown is in progress.
    Stopping,
    /// The process has completed shutdown.
    Stopped,
}

/// A cloneable, read-only view of current server health.
#[derive(Clone)]
pub struct HealthStatus {
    state: Arc<RwLock<HealthState>>,
}

impl HealthStatus {
    pub(crate) fn new(state: HealthState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Returns the current process health state.
    #[must_use]
    pub fn state(&self) -> HealthState {
        *self.read_state()
    }

    /// Returns whether the process currently reports ready.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state() == HealthState::Ready
    }

    pub(crate) fn set_state(&self, state: HealthState) {
        *self.write_state() = state;
    }

    fn read_state(&self) -> RwLockReadGuard<'_, HealthState> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, HealthState> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
