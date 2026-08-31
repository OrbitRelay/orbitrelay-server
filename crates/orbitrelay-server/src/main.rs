//! Thin OrbitRelay server process entry point.

use std::{io::Write, sync::Arc};

use orbitrelay_server::{
    Bootstrap, DevelopmentActionAuthorizer, DevelopmentIdentityResolver,
    DevelopmentSubscriptionAuthorizer, RejectAllActionAuthorizer, RejectAllIdentityResolver,
    RejectAllSubscriptionAuthorizer, ServerApplication, ServerConfig, ServerDependencies,
    ServerError,
};

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let config = ServerConfig::load()?;
    let asset_delivery_enabled = config.asset_delivery().enabled();
    let dependencies = if config.development_mode() {
        tracing::warn!("OrbitRelay development authorization is enabled");
        ServerDependencies::new(
            Arc::new(DevelopmentActionAuthorizer),
            Arc::new(DevelopmentIdentityResolver),
            Arc::new(DevelopmentSubscriptionAuthorizer),
        )
    } else {
        ServerDependencies::new(
            Arc::new(RejectAllActionAuthorizer),
            Arc::new(RejectAllIdentityResolver),
            Arc::new(RejectAllSubscriptionAuthorizer),
        )
    };
    let websocket_path = config.websocket_listener().websocket_path().to_owned();
    let bootstrap = Bootstrap::new(config.clone(), dependencies.action_authorizer());
    let context = bootstrap.initialize().await?;
    let mut application = ServerApplication::new(context, config, dependencies);
    application.start().await?;
    println!(
        "ORBITRELAY_LISTENING=ws://{}{}",
        application.local_addr()?,
        websocket_path
    );
    if asset_delivery_enabled {
        println!(
            "ORBITRELAY_ASSET_LISTENING={}",
            application.asset_local_addr()?
        );
    }
    std::io::stdout()
        .flush()
        .map_err(|_| ServerError::Bootstrap {
            message: "failed to report listener address".to_owned(),
        })?;
    if std::env::var_os("ORBITRELAY_TEST_SHUTDOWN_STDIN").is_some() {
        application.run_with_stdin_shutdown().await
    } else {
        application.run().await
    }
}
