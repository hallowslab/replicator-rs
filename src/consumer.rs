use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::config::{BackendConfig, BackgroundJobConfig};
use crate::storage::{StorageBackend, StorageBackendFactory};
use crate::store::ReplicationStore;
#[cfg(feature = "broker")]
use crate::broker::{BrokerClient, DeviceStatus};
#[cfg(feature = "broker")]
use crate::config::BrokerConfig;

/// Long-running replicator: on start it authorizes with the broker (or builds a
/// static backend), then drains the durable store until shut down.
pub struct Consumer {
    store: ReplicationStore,
    backend: Arc<dyn StorageBackend>,
    max_retries: u32,
    poll_interval: Duration,
    print_messages: bool,
}

impl Consumer {
    pub async fn new(config: &BackgroundJobConfig, store: ReplicationStore) -> Result<Self, String> {
        let (backend, print_messages) = build_backend(config).await?;
        Ok(Self {
            store,
            backend,
            max_retries: config.max_retries,
            poll_interval: config.retry_delay_base,
            print_messages,
        })
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                break;
            }

            match self.store.next_pending() {
                Ok(Some(rec)) => {
                    let source = std::path::PathBuf::from(&rec.source_path);
                    match self.backend.upload(&source, &rec.dest_key).await {
                        Ok(()) => {
                            if self.print_messages {
                                println!(
                                    "[Replicator] replicated {} -> {}",
                                    rec.source_path, rec.dest_key
                                );
                            }
                            let _ = self.store.mark_done(rec.id);
                        }
                        Err(e) => {
                            eprintln!(
                                "[Replicator] failed {} (attempt {}): {}",
                                rec.source_path, rec.attempts, e
                            );
                            let _ = self.store.mark_failed(rec.id, self.max_retries);
                        }
                    }
                }
                Ok(None) => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.poll_interval) => {}
                        _ = shutdown.changed() => {}
                    }
                }
                Err(e) => {
                    eprintln!("[Replicator] store error: {}", e);
                    tokio::select! {
                        _ = tokio::time::sleep(self.poll_interval) => {}
                        _ = shutdown.changed() => {}
                    }
                }
            }
        }
    }
}

async fn build_backend(
    config: &BackgroundJobConfig,
) -> Result<(Arc<dyn StorageBackend>, bool), String> {
    #[cfg(feature = "broker")]
    if let Some(broker) = &config.broker {
        return broker_backend(broker).await;
    }
    build_static_backend(config).map(|b| (b, true))
}

/// Authorize with the broker and build the storage backend it hands out.
#[cfg(feature = "broker")]
async fn broker_backend(broker: &BrokerConfig) -> Result<(Arc<dyn StorageBackend>, bool), String> {
    let client = BrokerClient::new(broker).map_err(|e| e.to_string())?;

    match client
        .register_and_status()
        .await
        .map_err(|e| e.to_string())?
    {
        DeviceStatus::Approved => {}
        DeviceStatus::Pending => {
            println!("[Replicator] device registered, waiting for broker approval...");
            client.wait_for_approval().await.map_err(|e| e.to_string())?;
        }
        DeviceStatus::Deauthorized => {
            return Err(
                "device deauthorized — an admin must re-approve it before replication can start"
                    .into(),
            );
        }
        DeviceStatus::Unknown => {
            return Err("broker returned unknown device status".into());
        }
    }

    let token = client.authenticate().await.map_err(|e| e.to_string())?;
    let cfg = client
        .fetch_backend_config(&token)
        .await
        .map_err(|e| e.to_string())?;
    cfg.check_version()
        .map_err(|e| format!("Broker issued invalid backend config: {e}"))?;

    let mut backend_cfg = cfg.backend;
    if let BackendConfig::S3(ref mut s3) = backend_cfg {
        if broker.immutable_naming {
            s3.immutable_naming = true;
        }
    }

    let backend = StorageBackendFactory::build(&backend_cfg).map_err(|e| e.to_string())?;
    Ok((backend, broker.broker_messages))
}

fn build_static_backend(config: &BackgroundJobConfig) -> Result<Arc<dyn StorageBackend>, String> {
    if let Some(versioned) = &config.storage {
        versioned
            .check_version()
            .map_err(|e| format!("Replication disabled: {e}"))?;
        return StorageBackendFactory::build(&versioned.backend)
            .map_err(|e| format!("Replication disabled: {e}"));
    }
    if let Some(remote) = &config.remote_storage {
        let cfg = BackendConfig::try_from(remote.clone())
            .map_err(|e| format!("Replication disabled: {e}"))?;
        return StorageBackendFactory::build(&cfg)
            .map_err(|e| format!("Replication disabled: {e}"));
    }
    Err("Replication disabled: no remote storage configured".into())
}
