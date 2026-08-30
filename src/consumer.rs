use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::config::{BackendConfig, BackgroundJobConfig};
use crate::event::ReplicatorEvent;
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
    config: BackgroundJobConfig,
    tx: Option<mpsc::UnboundedSender<ReplicatorEvent>>,
    max_retries: u32,
    poll_interval: Duration,
}

impl Consumer {
    pub fn new(
        config: &BackgroundJobConfig,
        store: ReplicationStore,
        tx: Option<mpsc::UnboundedSender<ReplicatorEvent>>,
    ) -> Self {
        Self {
            store,
            config: config.clone(),
            tx,
            max_retries: config.max_retries,
            poll_interval: config.retry_delay_base,
        }
    }

    fn emit(&self, ev: ReplicatorEvent) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ev);
        }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let (backend, print_messages) = match build_backend(&self.config, self.tx.as_ref()).await {
            Ok(b) => b,
            Err(e) => {
                self.emit(ReplicatorEvent::BrokerStatus {
                    status: "error".into(),
                    message: Some(e),
                });
                return;
            }
        };

        loop {
            if *shutdown.borrow() {
                break;
            }

            match self.store.next_pending() {
                Ok(Some(rec)) => {
                    let source = std::path::PathBuf::from(&rec.source_path);
                    match backend.upload(&source, &rec.dest_key).await {
                        Ok(()) => {
                            if print_messages {
                                println!(
                                    "[Replicator] replicated {} -> {}",
                                    rec.source_path, rec.dest_key
                                );
                            }
                            self.emit(ReplicatorEvent::Replication {
                                path: rec.dest_key.clone(),
                                ok: true,
                                error: None,
                            });
                            let _ = self.store.mark_done(rec.id);
                        }
                        Err(e) => {
                            let message = e.to_string();
                            eprintln!(
                                "[Replicator] failed {} (attempt {}): {}",
                                rec.source_path, rec.attempts, message
                            );
                            self.emit(ReplicatorEvent::Replication {
                                path: rec.dest_key.clone(),
                                ok: false,
                                error: Some(message),
                            });
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

fn emit_status(tx: Option<&mpsc::UnboundedSender<ReplicatorEvent>>, status: &str, message: Option<&str>) {
    if let Some(tx) = tx {
        let _ = tx.send(ReplicatorEvent::BrokerStatus {
            status: status.into(),
            message: message.map(|s| s.to_string()),
        });
    }
}

async fn build_backend(
    config: &BackgroundJobConfig,
    tx: Option<&mpsc::UnboundedSender<ReplicatorEvent>>,
) -> Result<(Arc<dyn StorageBackend>, bool), String> {
    #[cfg(feature = "broker")]
    if let Some(broker) = &config.broker {
        return broker_backend(broker, tx).await;
    }
    build_static_backend(config).map(|b| (b, true))
}

/// Authorize with the broker and build the storage backend it hands out.
#[cfg(feature = "broker")]
async fn broker_backend(
    broker: &BrokerConfig,
    tx: Option<&mpsc::UnboundedSender<ReplicatorEvent>>,
) -> Result<(Arc<dyn StorageBackend>, bool), String> {
    let client = BrokerClient::new(broker).map_err(|e| e.to_string())?;

    emit_status(tx, "registering", None);
    match client
        .register_and_status()
        .await
        .map_err(|e| e.to_string())?
    {
        DeviceStatus::Approved => {
            emit_status(tx, "registered", None);
        }
        DeviceStatus::Pending => {
            emit_status(tx, "registered", None);
            emit_status(tx, "pending", Some("waiting for approval"));
            client.wait_for_approval().await.map_err(|e| e.to_string())?;
        }
        DeviceStatus::Deauthorized => {
            emit_status(tx, "rejected", Some("device deauthorized — admin must re-approve"));
            return Err(
                "device deauthorized — an admin must re-approve it before replication can start"
                    .into(),
            );
        }
        DeviceStatus::Unknown => {
            return Err("broker returned unknown device status".into());
        }
    }

    emit_status(tx, "approved", None);
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
    emit_status(tx, "active", Some("credentials armed"));
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
