//! Root-manifest persistence.
//!
//! The runtime publishes its in-memory root (rooms, their exits and props) as
//! a single DAG-CBOR node, periodically and on shutdown. Each successful
//! publication updates `root_cid` in `config.yaml`, pins the new CID, and
//! unpins the previous one so the local store only keeps the live head.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use ma_core::config::Config;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::root::{RootActor, RootManifest};

pub const DEFAULT_ROOT_PUBLISH_INTERVAL_SECS: u64 = 300;

pub fn root_cid_setting(config: &Config) -> Option<String> {
    config
        .extra
        .get("root_cid")
        .and_then(serde_yaml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn root_publish_interval(config: &Config) -> Duration {
    let secs = config
        .extra
        .get("root_publish_interval_secs")
        .and_then(serde_yaml::Value::as_u64)
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_ROOT_PUBLISH_INTERVAL_SECS);
    Duration::from_secs(secs)
}

/// Restore the root from the persisted `root_cid`, falling back to a fresh
/// root when no head exists yet.
pub async fn restore_root(config: &Config, owners: Vec<String>) -> Result<RootActor> {
    let Some(cid) = root_cid_setting(config) else {
        return Ok(RootActor::new(owners));
    };
    let manifest: RootManifest = crate::kubo::dag_get(&config.kubo_rpc_url, &cid)
        .await
        .with_context(|| format!("loading root manifest {cid}"))?;
    Ok(RootActor::from_manifest(manifest, owners))
}

/// Publish the current root, pin the new CID, unpin the previous one, and
/// persist `root_cid` to `config.yaml`.
pub async fn publish_and_persist_root(
    config: &mut Config,
    root: &Arc<Mutex<RootActor>>,
) -> Result<String> {
    let manifest = {
        let guard = root.lock().await;
        guard.to_manifest()
    };
    let new_cid = crate::kubo::dag_put(&config.kubo_rpc_url, &manifest).await?;

    if let Some(old_cid) = root_cid_setting(config) {
        if old_cid != new_cid {
            if let Err(error) = crate::kubo::pin_rm(&config.kubo_rpc_url, &old_cid).await {
                warn!(old = %old_cid, new = %new_cid, error = %error, "unpinning previous root CID failed");
            }
        }
    }

    persist_root_cid(config, &new_cid)?;
    Ok(new_cid)
}

fn persist_root_cid(config: &mut Config, root_cid: &str) -> Result<()> {
    if root_cid_setting(config).as_deref() == Some(root_cid) {
        return Ok(());
    }
    config.extra.insert(
        serde_yaml::Value::String("root_cid".to_string()),
        serde_yaml::Value::String(root_cid.to_string()),
    );
    config.save()?;
    Ok(())
}

/// Spawn the periodic root-manifest publisher. The first tick fires
/// immediately (establishing/pinning the head), then repeats on `interval`.
pub fn spawn_periodic_root_publish(
    shared_config: Arc<Mutex<Config>>,
    root: Arc<Mutex<RootActor>>,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let mut config = shared_config.lock().await;
            match publish_and_persist_root(&mut config, &root).await {
                Ok(cid) => info!(root_cid = %cid, "root manifest published"),
                Err(error) => warn!(error = %error, "periodic root manifest publish failed"),
            }
        }
    });
}
