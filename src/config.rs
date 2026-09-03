use std::path::PathBuf;

use anyhow::{Context, Result};
use ma_core::config::Config;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub slug: String,
    pub status_bind: String,
    pub owners: Vec<String>,
    pub acl_file: Option<PathBuf>,
    pub kubo_rpc_url: String,
}

fn default_slug() -> String {
    "irc".to_string()
}

fn default_status_bind() -> String {
    "127.0.0.1:5667".to_string()
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            slug: default_slug(),
            status_bind: default_status_bind(),
            owners: Vec::new(),
            acl_file: None,
            kubo_rpc_url: "http://127.0.0.1:5001".to_string(),
        }
    }
}

impl RuntimeConfig {
    pub fn from_core(config: &Config) -> Result<Self> {
        Ok(Self {
            slug: config.slug.clone(),
            status_bind: extra(config, "status_bind")?.unwrap_or_else(default_status_bind),
            owners: extra(config, "owners")?.unwrap_or_default(),
            acl_file: extra(config, "acl_file")?,
            kubo_rpc_url: config.kubo_rpc_url.clone(),
        })
    }
}

fn extra<T>(config: &Config, key: &str) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    config
        .extra
        .get(serde_yaml::Value::String(key.to_string()))
        .cloned()
        .map(serde_yaml::from_value)
        .transpose()
        .with_context(|| format!("parsing config key '{key}'"))
}
