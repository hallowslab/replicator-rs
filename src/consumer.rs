use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::config::{BackendConfig, BackgroundJobConfig};
use crate::event::ReplicatorEvent;
use crate::storage::{StorageBackend, StorageBackendFactory, UploadContext};
use crate::store::ReplicationStore;
#[cfg(feature = "broker")]
use crate::broker::{BrokerClient, DeviceStatus};
#[cfg(feature = "broker")]
use crate::config::BrokerConfig;

/// Give up on a record after this long (from its `created_at`) and mark it a
/// dead-letter rather than retrying forever.
const MAX_AGE_SECS: u64 = 72 * 3600;

/// Long-running replicator: on start it authorizes with the broker (or builds a
/// static backend), then drains the durable store until shut down.
pub struct Consumer {
    store: Arc<ReplicationStore>,
    config: BackgroundJobConfig,
    tx: Option<mpsc::UnboundedSender<ReplicatorEvent>>,
    retry_delay_secs: u64,
    poll_interval: Duration,
}

impl Consumer {
    pub fn new(
        config: &BackgroundJobConfig,
        store: ReplicationStore,
        tx: Option<mpsc::UnboundedSender<ReplicatorEvent>>,
    ) -> Self {
        Self {
            store: Arc::new(store),
            config: config.clone(),
            tx,
            retry_delay_secs: config.retry_delay_base.as_secs().max(1),
            poll_interval: config.retry_delay_base,
        }
    }

    fn emit(&self, ev: ReplicatorEvent) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ev);
        }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        // Recover from a previous run: interrupted uploads + dead-letters are
        // retried.
        let _ = self.store.resume_inflight();
        let _ = self.store.reset_failed();

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

        self.emit(ReplicatorEvent::BrokerStatus {
            status: "active".into(),
            message: Some("replication started".into()),
        });

        loop {
            if *shutdown.borrow() {
                break;
            }

            match self.store.next_pending() {
                Ok(Some(rec)) => {
                    let source = std::path::PathBuf::from(&rec.source_path);

                    // Source file gone locally -> nothing left to replicate, drop it.
                    if !source.exists() {
                        eprintln!(
                            "[Replicator] source gone, dropping record {}: {}",
                            rec.id, rec.source_path
                        );
                        self.emit(ReplicatorEvent::BrokerStatus {
                            status: "active".into(),
                            message: Some(format!("source gone, dropped {}", rec.dest_path)),
                        });
                        self.emit(ReplicatorEvent::Replication {
                            path: rec.dest_key.clone(),
                            ok: false,
                            error: Some("source file missing".into()),
                        });
                        let _ = self.store.remove(rec.id);
                        continue;
                    }

                    self.emit(ReplicatorEvent::BrokerStatus {
                        status: "active".into(),
                        message: Some(format!("processing {} (attempt {})", rec.dest_path, rec.attempts)),
                    });
                    let dest_key = if rec.key_finalized {
                        rec.dest_key.clone()
                    } else {
                        let key = backend.resolve_key(&rec.dest_path);
                        if let Err(e) = self.store.finalize_key(rec.id, &key) {
                            eprintln!(
                                "[Replicator] failed to finalize key for {}: {}",
                                rec.source_path, e
                            );
                        }
                        key
                    };
                    // A hung upload must not block the consumer forever (it
                    // would stall the whole queue and block shutdown). The S3
                    // backend now bounds each request itself (15s connect /
                    // 30min read); this 6h watchdog is only a circuit breaker
                    // for pathological cases that survive request-level limits.
                    // Sized far above any legitimate upload: per-part deadlines
                    // are independent, so a slow-but-working 1 GiB upload
                    // (e.g. 205 x 5 MiB parts) may legitimately take hours.
                    let upload_timeout = Duration::from_secs(6 * 60 * 60);
                    let store = Arc::clone(&self.store);
                    let record_id = rec.id;
                    let mut ctx = UploadContext {
                        multipart_upload_id: rec.mp_upload_id.clone(),
                        parts: rec.mp_parts.clone(),
                        // Persist incrementally after each completed part so a
                        // hard kill mid-upload still resumes from the last
                        // completed part on the next run.
                        on_part: Some(Box::new(move |c| {
                            let _ = store.save_upload_state(
                                record_id,
                                c.multipart_upload_id.as_deref(),
                                &c.parts,
                            );
                        })),
                    };
                    let result = tokio::time::timeout(
                        upload_timeout,
                        backend.upload(&source, &dest_key, &mut ctx),
                    )
                    .await;
                    // Persist resumable-multipart progress on every outcome so
                    // the next attempt continues from the parts uploaded.
                    let _ = self.store.save_upload_state(rec.id, ctx.multipart_upload_id.as_deref(), &ctx.parts);
                    match result {
                        Ok(Ok(())) => {
                            if print_messages {
                                println!(
                                    "[Replicator] replicated {} -> {}",
                                    rec.source_path, dest_key
                                );
                            }
                            self.emit(ReplicatorEvent::Replication {
                                path: dest_key.clone(),
                                ok: true,
                                error: None,
                            });
                            let _ = self.store.remove(rec.id);
                        }
                        Ok(Err(e)) => {
                            let message = e.to_string();
                            eprintln!(
                                "[Replicator] failed {} (attempt {}): {}",
                                rec.source_path, rec.attempts, message
                            );
                            self.emit(ReplicatorEvent::Replication {
                                path: dest_key.clone(),
                                ok: false,
                                error: Some(message),
                            });
                            let _ = self.store.mark_failed(rec.id, self.retry_delay_secs, MAX_AGE_SECS);
                        }
                        Err(_) => {
                            eprintln!(
                                "[Replicator] upload timed out {} (attempt {})",
                                rec.source_path, rec.attempts
                            );
                            self.emit(ReplicatorEvent::Replication {
                                path: dest_key.clone(),
                                ok: false,
                                error: Some("upload timed out after 6 hours".into()),
                            });
                            let _ = self.store.mark_failed(rec.id, self.retry_delay_secs, MAX_AGE_SECS);
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
        return broker_backend(broker, tx, config).await;
    }
    build_static_backend(config).map(|b| (b, true))
}

/// Apply multipart upload overrides (CLI/baked defaults) to an S3 backend
/// config, whatever its origin (broker-fetched or local).
fn apply_s3_upload_overrides(
    backend_cfg: &mut BackendConfig,
    config: &BackgroundJobConfig,
) {
    if let BackendConfig::S3(s3) = backend_cfg {
        if config.multipart_threshold_bytes.is_some() {
            s3.multipart_threshold_bytes = config.multipart_threshold_bytes;
        }
        if config.max_part_size_bytes.is_some() {
            s3.max_part_size_bytes = config.max_part_size_bytes;
        }
    }
}

/// Authorize with the broker and build the storage backend it hands out.
#[cfg(feature = "broker")]
async fn broker_backend(
    broker: &BrokerConfig,
    tx: Option<&mpsc::UnboundedSender<ReplicatorEvent>>,
    config: &BackgroundJobConfig,
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
            emit_status(tx, "rejected", Some("device deauthorized - admin must re-approve"));
            return Err(
                "device deauthorized - an admin must re-approve it before replication can start"
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
    apply_s3_upload_overrides(&mut backend_cfg, config);

    let backend = StorageBackendFactory::build(&backend_cfg).map_err(|e| e.to_string())?;
    emit_status(tx, "active", Some("credentials armed"));
    Ok((backend, broker.broker_messages))
}

fn build_static_backend(config: &BackgroundJobConfig) -> Result<Arc<dyn StorageBackend>, String> {
    if let Some(versioned) = &config.storage {
        versioned
            .check_version()
            .map_err(|e| format!("Replication disabled: {e}"))?;
        let mut backend = versioned.backend.clone();
        apply_s3_upload_overrides(&mut backend, config);
        return StorageBackendFactory::build(&backend)
            .map_err(|e| format!("Replication disabled: {e}"));
    }
    if let Some(remote) = &config.remote_storage {
        let mut cfg = BackendConfig::try_from(remote.clone())
            .map_err(|e| format!("Replication disabled: {e}"))?;
        apply_s3_upload_overrides(&mut cfg, config);
        return StorageBackendFactory::build(&cfg)
            .map_err(|e| format!("Replication disabled: {e}"));
    }
    Err("Replication disabled: no remote storage configured".into())
}
