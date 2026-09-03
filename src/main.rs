use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use ma_core::config::{Config, MaArgs, SecretBundle};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use ma_irc::acl::{is_allowed, load_transport_acl};
use ma_irc::config::RuntimeConfig;
use ma_irc::did_publish::publish_runtime_did_document;
use ma_irc::root_publish::{
    publish_and_persist_root, restore_root, root_publish_interval, spawn_periodic_root_publish,
};
use ma_irc::rpc_transport::{init_rpc_runtime, run_rpc_loop};
use ma_irc::status::{run_status_server, RuntimeState, StatusConfig};

const MA_DEFAULT_SLUG: &str = "irc";
const STARTUP_DID_PUBLISH_TIMEOUT_SECS: u64 = 120;

#[derive(Debug, Parser)]
#[command(name = "ma-irc")]
#[command(about = "IRC-focused ma runtime prototype")]
struct Cli {
    #[command(flatten)]
    ma: MaArgs,

    #[arg(long)]
    acl_check_did: Option<String>,

    #[arg(long)]
    acl_check_cap: Option<String>,

    #[arg(long)]
    no_status_server: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // rustls is compiled with both the `aws-lc-rs` and `ring` providers enabled
    // (feature-unified through iroh/quinn), so it cannot auto-select a process
    // default and panics on the first TLS handshake (e.g. `ircs://`). Pin the
    // provider explicitly before any TLS connection is made.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install the rustls crypto provider"))?;

    let cli = Cli::parse();

    if cli.ma.gen_headless_config {
        Config::gen_headless(&cli.ma, MA_DEFAULT_SLUG)?;
        return Ok(());
    }

    let config = Config::from_args(&cli.ma, MA_DEFAULT_SLUG)?;
    config
        .init_logging()
        .with_context(|| "initialising logging from config")?;
    info!(slug = %config.slug, "runtime configuration loaded");

    let mut cfg = RuntimeConfig::from_core(&config)?;

    if let Some(acl) = cfg.acl_file.clone() {
        if acl.is_relative() {
            let base = config
                .config_path
                .as_ref()
                .and_then(|path| path.parent())
                .context("config path has no parent directory")?;
            cfg.acl_file = Some(base.join(acl));
        }
    }

    let passphrase = config
        .secret_bundle_passphrase
        .as_deref()
        .context("secret_bundle_passphrase is required (env or config)")?;
    let bundle_path = config.effective_secret_bundle()?;
    let bundle = SecretBundle::load(&bundle_path, passphrase)
        .with_context(|| format!("loading secret bundle {}", bundle_path.display()))?;
    let runtime_did = bundle
        .generate_identity()
        .context("generating runtime identity")?
        .document
        .id;

    let resolver = Arc::new(config.ipfs_gateway_resolver());
    let rpc_runtime = init_rpc_runtime(&runtime_did, &bundle, resolver)
        .await
        .with_context(|| "initialising rpc transport runtime")?;
    let advertised_services = rpc_runtime.services();
    info!(services = ?advertised_services, "startup publish services resolved");

    match tokio::time::timeout(
        std::time::Duration::from_secs(STARTUP_DID_PUBLISH_TIMEOUT_SECS),
        publish_runtime_did_document(&config, &bundle, &runtime_did, advertised_services),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(error).with_context(|| "mandatory startup DID publish failed");
        }
        Err(_) => {
            anyhow::bail!(
                "mandatory startup DID publish timed out after {STARTUP_DID_PUBLISH_TIMEOUT_SECS}s"
            );
        }
    }

    let mut root = restore_root(&config, cfg.owners.clone()).await?;
    if let Some(owner) = cfg.owners.first() {
        if let Err(error) = root.claim(owner) {
            eprintln!("warning: #root claim by configured owner failed: {error}");
        }
    }
    let state = RuntimeState {
        root: Arc::new(Mutex::new(root)),
    };

    {
        let root_for_rpc = state.root.clone();
        let rpc_runtime_for_task = rpc_runtime.clone();
        tokio::spawn(async move {
            if let Err(error) = run_rpc_loop(rpc_runtime_for_task, root_for_rpc).await {
                error!(error = %error, "rpc loop terminated");
            }
        });
    }

    if let Some(acl_file) = cfg.acl_file.as_deref() {
        let acl = load_transport_acl(acl_file)?;
        info!(path = %acl_file.display(), "loaded transport ACL file");
        if let (Some(did), Some(cap)) = (cli.acl_check_did.as_deref(), cli.acl_check_cap.as_deref())
        {
            let allowed = is_allowed(&acl, did, cap);
            info!(did, cap, allowed, "transport ACL probe");
        }
    }

    info!(slug = %cfg.slug, did = %runtime_did, status_bind = %cfg.status_bind, owners = cfg.owners.len(), "runtime boot summary");
    let entity_count = state.root.lock().await.entities().count();
    info!(entities = entity_count, "initial entity count");
    info!("hint: connect with ma-zion and enter #concourse");
    info!("hint: 'dig room' enters or creates it; then :irc-server and :irc-channel, then :irc-connect — every occupant joins the channel as themselves");

    if cli.no_status_server {
        warn!("status server disabled by --no-status-server");
        return Ok(());
    }

    let interval = root_publish_interval(&config);
    let shared_config = Arc::new(Mutex::new(config));
    spawn_periodic_root_publish(shared_config.clone(), state.root.clone(), interval);

    let status_cfg = StatusConfig {
        bind: cfg.status_bind,
        did: runtime_did.clone(),
        slug: cfg.slug,
    };

    tokio::select! {
        result = run_status_server(status_cfg, state.clone()) => result?,
        _ = shutdown_signal() => {
            info!("shutting down; publishing final root manifest");
            let mut config = shared_config.lock().await;
            if let Err(error) = publish_and_persist_root(&mut config, &state.root).await {
                error!(error = %error, "final root manifest publish failed");
            }
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("installing ctrl-c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        signal(SignalKind::terminate())
            .expect("installing SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
