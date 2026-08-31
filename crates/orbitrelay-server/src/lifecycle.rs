//! Process lifecycle state machine.

use std::{
    fmt,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{HealthState, HealthStatus, LifecycleError};

/// The internal lifecycle state of the server process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LifecycleState {
    /// Process initialization is in progress.
    Starting,
    /// Initialization completed and the process can accept work.
    Ready,
    /// Graceful shutdown is in progress.
    Draining,
    /// The process is not running.
    Stopped,
}

impl fmt::Display for LifecycleState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
        };
        formatter.write_str(value)
    }
}

/// A thread-safe state machine for one server process lifecycle.
#[derive(Clone)]
pub struct ServerLifecycle {
    state: Arc<RwLock<LifecycleState>>,
    health: HealthStatus,
}

impl Default for ServerLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerLifecycle {
    /// Creates a lifecycle in the stopped state.
    #[must_use]
    pub fn new() -> Self {
        Self::from_state(LifecycleState::Stopped)
    }

    pub(crate) fn from_state(state: LifecycleState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            health: HealthStatus::new(Self::health_for(state)),
        }
    }

    /// Transitions a stopped process to starting.
    pub fn start(&self) -> Result<(), LifecycleError> {
        self.transition(&[LifecycleState::Stopped], LifecycleState::Starting)
    }

    /// Transitions a starting process to ready.
    pub fn ready(&self) -> Result<(), LifecycleError> {
        self.transition(&[LifecycleState::Starting], LifecycleState::Ready)
    }

    /// Begins graceful shutdown from a starting or ready process.
    pub fn begin_shutdown(&self) -> Result<(), LifecycleError> {
        self.transition(
            &[LifecycleState::Starting, LifecycleState::Ready],
            LifecycleState::Draining,
        )
    }

    /// Completes graceful shutdown from the draining state.
    pub fn stop(&self) -> Result<(), LifecycleError> {
        self.transition(&[LifecycleState::Draining], LifecycleState::Stopped)
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> LifecycleState {
        *self.read_state()
    }

    /// Returns a read-only view of the corresponding health state.
    #[must_use]
    pub const fn health(&self) -> &HealthStatus {
        &self.health
    }

    fn transition(
        &self,
        allowed_from: &[LifecycleState],
        to: LifecycleState,
    ) -> Result<(), LifecycleError> {
        let mut state = self.write_state();
        let from = *state;
        if !allowed_from.contains(&from) {
            return Err(LifecycleError::new(from, to));
        }

        *state = to;
        self.health.set_state(Self::health_for(to));
        Ok(())
    }

    const fn health_for(state: LifecycleState) -> HealthState {
        match state {
            LifecycleState::Starting => HealthState::Starting,
            LifecycleState::Ready => HealthState::Ready,
            LifecycleState::Draining => HealthState::Stopping,
            LifecycleState::Stopped => HealthState::Stopped,
        }
    }

    fn read_state(&self) -> RwLockReadGuard<'_, LifecycleState> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, LifecycleState> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::{LifecycleState, ServerLifecycle};
    use crate::HealthState;

    #[test]
    fn follows_the_complete_lifecycle() {
        let lifecycle = ServerLifecycle::new();

        assert_eq!(lifecycle.state(), LifecycleState::Stopped);
        lifecycle.start().expect("start should succeed");
        assert_eq!(lifecycle.state(), LifecycleState::Starting);
        lifecycle.ready().expect("ready should succeed");
        assert_eq!(lifecycle.state(), LifecycleState::Ready);
        lifecycle.begin_shutdown().expect("shutdown should begin");
        assert_eq!(lifecycle.state(), LifecycleState::Draining);
        lifecycle.stop().expect("stop should succeed");
        assert_eq!(lifecycle.state(), LifecycleState::Stopped);
    }

    #[test]
    fn rejects_invalid_state_transitions() {
        let lifecycle = ServerLifecycle::new();

        let error = lifecycle.ready().expect_err("stopped cannot become ready");
        assert_eq!(error.from(), LifecycleState::Stopped);
        assert_eq!(error.to(), LifecycleState::Ready);
        assert_eq!(lifecycle.state(), LifecycleState::Stopped);
    }

    #[test]
    fn exposes_health_for_each_lifecycle_state() {
        let lifecycle = ServerLifecycle::new();

        assert_eq!(lifecycle.health().state(), HealthState::Stopped);
        lifecycle.start().expect("start should succeed");
        assert_eq!(lifecycle.health().state(), HealthState::Starting);
        lifecycle.ready().expect("ready should succeed");
        assert!(lifecycle.health().is_ready());
        lifecycle.begin_shutdown().expect("shutdown should begin");
        assert_eq!(lifecycle.health().state(), HealthState::Stopping);
        lifecycle.stop().expect("stop should succeed");
        assert_eq!(lifecycle.health().state(), HealthState::Stopped);
        assert_ne!(lifecycle.health().state(), HealthState::Degraded);
    }
}
