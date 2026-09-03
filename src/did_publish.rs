use anyhow::{Context, Result};
use ma_core::config::{Config, SecretBundle};
use ma_core::ipfs::{DidDocumentPublishOptions, IpfsDidPublisher};
use ma_core::MaExtension;
use tracing::info;
use zeroize::Zeroizing;

pub async fn publish_runtime_did_document(
    config: &Config,
    bundle: &SecretBundle,
    runtime_did: &str,
    services: Vec<String>,
) -> Result<()> {
    info!(did = %runtime_did, kubo_rpc_url = %config.kubo_rpc_url, "starting mandatory startup DID publication");
    let publisher = IpfsDidPublisher::new(&config.kubo_rpc_url)
        .with_context(|| format!("invalid kubo_rpc_url '{}'", config.kubo_rpc_url))?;
    publisher
        .wait_until_ready(10)
        .await
        .with_context(|| format!("kubo API not ready at {}", config.kubo_rpc_url))?;

    let document = bundle
        .build_document(MaExtension::new().kind("runtime").services(services))
        .context("building DID document from secret bundle")?;
    document
        .validate()
        .context("validating DID document before publication")?;
    document
        .verify()
        .context("verifying DID document proof before publication")?;

    if document.id != runtime_did {
        anyhow::bail!(
            "generated document DID '{}' does not match runtime DID '{}'",
            document.id,
            runtime_did
        );
    }

    let doc_cbor = document
        .encode()
        .context("encoding DID document as dag-cbor")?;

    let published = publisher
        .publish_document(
            doc_cbor,
            Zeroizing::new(bundle.ipns_secret_key.to_vec()),
            DidDocumentPublishOptions {
                key_parts: vec!["runtime".to_string(), config.slug.clone()],
                ..DidDocumentPublishOptions::default()
            },
        )
        .await
        .context("publishing DID document to IPFS/IPNS")?;

    info!(
        did = %runtime_did,
        cid = %published.cid,
        key_alias = %published.key_name,
        ipns_id = %published.ipns_id,
        "did document published"
    );
    Ok(())
}
