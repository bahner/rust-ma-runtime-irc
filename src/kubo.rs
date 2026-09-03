//! Minimal Kubo HTTP wrappers for the DAG operations ma-core keeps private.
//!
//! `ma_core` re-exports publish/pin helpers for DID documents but not the raw
//! `dag/put`, `dag/get`, and `pin/rm` endpoints this runtime needs to persist
//! its root manifest as a single DAG-CBOR node.

use anyhow::{anyhow, Result};
use reqwest::multipart;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Deserialize)]
struct DagPutCid {
    #[serde(rename = "/")]
    slash: String,
}

#[derive(Deserialize)]
struct DagPutResponse {
    #[serde(default, rename = "Cid")]
    cid_upper: Option<DagPutCid>,
    #[serde(default)]
    cid: Option<DagPutCid>,
}

/// Store a serialisable value as a `dag-cbor` IPLD node via Kubo (input is
/// serialised as `dag-json`), pinning it recursively. Returns the CID.
pub async fn dag_put<T: Serialize + Sync>(kubo_url: &str, value: &T) -> Result<String> {
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/dag/put");
    let payload = serde_json::to_vec(value)?;

    let part = multipart::Part::bytes(payload)
        .file_name("node.json")
        .mime_str("application/json")?;
    let form = multipart::Form::new().part("file", part);

    let body = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?
        .post(url)
        .query(&[
            ("store-codec", "dag-cbor"),
            ("input-codec", "dag-json"),
            ("pin", "true"),
        ])
        .multipart(form)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let parsed: DagPutResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow!("failed parsing dag/put response: {e} body={body}"))?;
    parsed
        .cid_upper
        .or(parsed.cid)
        .map(|c| c.slash)
        .ok_or_else(|| anyhow!("missing CID in dag/put response: {body}"))
}

/// Recursively unpin a CID. Used to retire the previous root manifest.
pub async fn pin_rm(kubo_url: &str, cid: &str) -> Result<()> {
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/pin/rm");
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?
        .post(url)
        .query(&[("arg", cid), ("recursive", "true")])
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Fetch an IPLD node from Kubo and deserialise it from `dag-json`.
pub async fn dag_get<T: DeserializeOwned>(kubo_url: &str, cid: &str) -> Result<T> {
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/dag/get");

    let body = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .post(url)
        .query(&[("arg", cid), ("output-codec", "dag-json")])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    serde_json::from_str(&body)
        .map_err(|e| anyhow!("failed to deserialise dag/get response for {cid}: {e} body={body}"))
}
