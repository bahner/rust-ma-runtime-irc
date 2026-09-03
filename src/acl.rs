use std::path::Path;

use anyhow::{Context, Result};
use ma_core::{check_cap, AclMap};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AclFile {
    acl: AclMap,
}

pub fn load_transport_acl(path: &Path) -> Result<AclMap> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading transport ACL file {}", path.display()))?;
    load_transport_acl_from_yaml(&raw)
}

pub fn load_transport_acl_from_yaml(yaml: &str) -> Result<AclMap> {
    let parsed: AclFile = serde_yaml::from_str(yaml).context("parsing ACL YAML")?;
    Ok(parsed.acl)
}

pub fn is_allowed(acl: &AclMap, did: &str, capability: &str) -> bool {
    check_cap(acl, did, capability).is_ok()
}
