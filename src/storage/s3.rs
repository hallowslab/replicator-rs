use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, SharedHttpClient};
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_http_client::tls::{self, TrustStore};
use aws_smithy_types::retry::RetryConfig;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use super::traits::{
    BackendCapabilities, Capability, StorageBackend, StorageError, UploadContext,
};
use crate::config::S3Config;

const S3_KEY_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.');

fn generate_immutable_prefix() -> String {
    uuid::Uuid::new_v4().to_string()
}

// Defaults are baked at build time from replicator-rs/build.toml (build.rs).
// They apply whenever the storage/broker config leaves the field unset.
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = crate::S3_DEFAULT_MULTIPART_THRESHOLD_BYTES;
pub const DEFAULT_MAX_PART_SIZE: u64 = crate::S3_DEFAULT_MAX_PART_SIZE_BYTES;
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub const MAX_PART_COUNT: u64 = 10_000;

/// Pick a part size for `size`: as small as needed to stay under
/// `MAX_PART_COUNT`, never below `MIN_PART_SIZE`, never above `max_part`.
/// `max_part` keeps every request body under Cloudflare's proxied upload cap
/// (100 MB on Free) regardless of file size.
pub fn plan_part_size(size: u64, max_part: u64) -> u64 {
    let max_part = max_part.max(MIN_PART_SIZE);
    size.div_ceil(MAX_PART_COUNT)
        .clamp(MIN_PART_SIZE, max_part)
}

pub struct S3Backend {
    client: aws_sdk_s3::Client,
    config: S3Config,
}

impl S3Backend {
    pub fn new(config: S3Config) -> Result<Self, StorageError> {
        let http_client = build_http_client(&config)?;

        let credentials = Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            config.session_token.clone(),
            None,
            "static",
        );

        let mut builder = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(&config.endpoint)
            .credentials_provider(credentials)
            .force_path_style(config.path_style)
            .http_client(http_client)
            .retry_config(RetryConfig::disabled())
            // Per-request bounds. NOTE: aws-smithy's read_timeout is a FIXED
            // deadline for the whole service call (body write + response head),
            // NOT inactivity-based. Sized for the largest request body:
            // 50 MiB part @ 50 KiB/s ~ 17 min, 1.8x margin => 30 min.
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(15))
                    .read_timeout(Duration::from_secs(30 * 60))
                    .build(),
            );

        if let Some(region) = &config.region {
            builder = builder.region(Region::new(region.clone()));
        }

        let client = aws_sdk_s3::Client::from_conf(builder.build());
        Ok(Self { client, config })
    }

    pub fn object_key(&self, path: &str) -> String {
        let trimmed = path.trim_start_matches('/');

        let final_path = if self.config.immutable_naming {
            // Extract filename and prepend UUID
            if let Some(last_slash) = trimmed.rfind('/') {
                let dir = &trimmed[..last_slash];
                let filename = &trimmed[last_slash + 1..];
                let uuid = generate_immutable_prefix();
                format!("{}/{}-{}", dir, uuid, filename)
            } else {
                let uuid = generate_immutable_prefix();
                format!("{}-{}", uuid, trimmed)
            }
        } else {
            trimmed.to_string()
        };

        if self.config.path_prefix.is_empty() {
            final_path
        } else {
            format!(
                "{}/{}",
                self.config.path_prefix.trim_end_matches('/'),
                final_path
            )
        }
    }

    pub fn copy_source(&self, key: &str) -> String {
        format!(
            "{}/{}",
            self.config.bucket,
            utf8_percent_encode(key, S3_KEY_ENCODE)
        )
    }

    pub fn multipart_threshold(&self) -> u64 {
        self.config
            .multipart_threshold_bytes
            .unwrap_or(DEFAULT_MULTIPART_THRESHOLD)
    }

    pub fn max_part_size(&self) -> u64 {
        self.config
            .max_part_size_bytes
            .unwrap_or(DEFAULT_MAX_PART_SIZE)
    }

    async fn single_upload(&self, source_path: &Path, key: &str) -> Result<(), StorageError> {
        let body = ByteStream::from_path(source_path)
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| self.map_sdk_error(e, "upload", key))
    }

    /// Uploads the parts of `key` missing from `ctx.parts` (parts already
    /// persisted as completed are skipped) and appends each new
    /// (part_number, etag) to the context so a later attempt resumes from
    /// exactly this point.
    async fn upload_parts(
        &self,
        source_path: &Path,
        key: &str,
        upload_id: &str,
        part_size: u64,
        size: u64,
        ctx: &mut UploadContext,
    ) -> Result<(), StorageError> {
        let part_count = size.div_ceil(part_size);
        let done: HashMap<i32, String> = ctx.parts.iter().cloned().collect();
        let mut file = tokio::fs::File::open(source_path)
            .await
            .map_err(StorageError::Io)?;

        for part_number in 1..=part_count as i32 {
            if done.contains_key(&part_number) {
                continue;
            }
            let start = (part_number as u64 - 1) * part_size;
            let len = (size - start).min(part_size);
            file.seek(SeekFrom::Start(start))
                .await
                .map_err(StorageError::Io)?;
            let mut body = vec![0u8; len as usize];
            file.read_exact(&mut body)
                .await
                .map_err(StorageError::Io)?;

            let resp = self
                .client
                .upload_part()
                .bucket(&self.config.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(body))
                .send()
                .await
                .map_err(|e| self.map_sdk_error(e, "upload part", key))?;

            let etag = resp
                .e_tag()
                .ok_or_else(|| {
                    StorageError::Transfer(format!(
                        "S3 upload part {} returned no etag for '{}'",
                        part_number, key
                    ))
                })?
                .to_string();
            ctx.parts.push((part_number, etag));
            ctx.notify_progress();
        }

        Ok(())
    }

    async fn multipart_upload(
        &self,
        source_path: &Path,
        key: &str,
        size: u64,
        ctx: &mut UploadContext,
    ) -> Result<(), StorageError> {
        let part_size = plan_part_size(size, self.max_part_size());

        let upload_id = match &ctx.multipart_upload_id {
            Some(id) => id.clone(),
            None => {
                let create = self
                    .client
                    .create_multipart_upload()
                    .bucket(&self.config.bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| self.map_sdk_error(e, "create multipart upload", key))?;
                let id = create.upload_id().ok_or_else(|| {
                    StorageError::Transfer(format!(
                        "S3 create multipart upload returned no upload id for '{}'",
                        key
                    ))
                })?.to_string();
                ctx.multipart_upload_id = Some(id.clone());
                ctx.notify_progress();
                id
            }
        };

        self.upload_parts(source_path, key, &upload_id, part_size, size, ctx)
            .await?;
        // NOTE: on failure we intentionally do NOT abort. The multipart upload
        // stays in-progress on the server and `ctx.parts` records what is
        // already uploaded (persisted by the caller), so the next attempt
        // resumes from the missing parts instead of restarting the whole file.
        // Orphaned uploads are eventually GC'd by garage lifecycle/repair.

        let completed = ctx.parts.iter().fold(
            CompletedMultipartUpload::builder(),
            |builder, (n, e)| {
                builder.parts(
                    CompletedPart::builder()
                        .part_number(*n)
                        .e_tag(e.clone())
                        .build(),
                )
            },
        ).build();

        match self
            .client
            .complete_multipart_upload()
            .bucket(&self.config.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                // Server dropped the in-progress upload (expired/GC'd): reset
                // so the next attempt starts a fresh multipart.
                if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e
                    && se
                        .err()
                        .meta()
                        .code()
                        .map(|c| c == "NoSuchUpload")
                        .unwrap_or(false)
                {
                    *ctx = UploadContext::default();
                }
                Err(self.map_sdk_error(e, "complete multipart upload", key))
            }
        }
    }

    fn map_sdk_error<E>(&self, err: aws_sdk_s3::error::SdkError<E>, op: &str, key: &str) -> StorageError
    where
        E: std::error::Error + ProvideErrorMetadata,
    {
        match err {
            aws_sdk_s3::error::SdkError::ServiceError(se) => {
                let code = se.err().meta().code().unwrap_or("").to_string();
                let status = se.raw().status().as_u16();
                let message = se.err().meta().message().unwrap_or("").to_string();
                match status {
                    301 => StorageError::Config(format!(
                        "S3 {} redirected for '{}': {}",
                        op, key, code
                    )),
                    403 => StorageError::PermissionDenied(format!(
                        "S3 {} access denied for '{}': {}",
                        op, key, message
                    )),
                    404 => StorageError::NotFound(format!(
                        "S3 {} not found '{}': {}",
                        op, key, message
                    )),
                    400 if code == "InvalidAccessKeyId" || code == "SignatureDoesNotMatch" => {
                        StorageError::Auth(format!(
                            "S3 {} authentication failed for '{}': {}",
                            op, key, code
                        ))
                    }
                    5.. => StorageError::Transfer(format!(
                        "S3 {} server error {} for '{}': {}",
                        op, status, key, message
                    )),
                    _ => StorageError::Transfer(format!(
                        "S3 {} failed for '{}' ({}): {}",
                        op, key, code, message
                    )),
                }
            }
            aws_sdk_s3::error::SdkError::ConstructionFailure(_) => StorageError::Config(format!(
                "S3 {} request construction failed for '{}'",
                op, key
            )),
            aws_sdk_s3::error::SdkError::TimeoutError(_) | aws_sdk_s3::error::SdkError::DispatchFailure(_) => {
                StorageError::Connection(format!("S3 {} timed out for '{}'", op, key))
            }
            aws_sdk_s3::error::SdkError::ResponseError(_) => StorageError::Transfer(format!(
                "S3 {} malformed response for '{}'",
                op, key
            )),
            _ => StorageError::Transfer(format!("S3 {} unknown error for '{}'", op, key)),
        }
    }
}

fn build_http_client(config: &S3Config) -> Result<SharedHttpClient, StorageError> {
    let tls_context = if let Some(ca_pem) = &config.ca_cert_pem {
        let mut store = TrustStore::default();
        store.add_pem_certificate(ca_pem.as_bytes().to_vec());
        tls::TlsContext::builder()
            .with_trust_store(store)
            .build()
            .map_err(|e| StorageError::Config(format!("failed to build TLS context: {}", e)))?
    } else {
        tls::TlsContext::default()
    };

    let client = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::Ring,
        ))
        .tls_context(tls_context)
        .build_https();

    Ok(client)
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn upload(
        &self,
        source_path: &Path,
        dest_key: &str,
        ctx: &mut UploadContext,
    ) -> Result<(), StorageError> {
        let size = tokio::fs::metadata(source_path)
            .await
            .map_err(StorageError::Io)?
            .len();

        if size <= self.multipart_threshold() {
            // Single PutObject has no resumable state; discard any stale one
            // (e.g. the source was replaced with a smaller file).
            *ctx = UploadContext::default();
            self.single_upload(source_path, dest_key).await
        } else {
            self.multipart_upload(source_path, dest_key, size, ctx).await
        }
    }

    fn resolve_key(&self, path: &str) -> String {
        self.object_key(path)
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let key = self.object_key(path);
        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| self.map_sdk_error(e, "delete", &key))
    }

    fn name(&self) -> &str {
        "s3"
    }

    fn capabilities(&self) -> Option<&dyn BackendCapabilities> {
        Some(self)
    }
}

#[async_trait]
impl BackendCapabilities for S3Backend {
    fn supports(&self, capability: Capability) -> bool {
        matches!(capability, Capability::Rename)
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> Result<(), StorageError> {
        let old_key = self.object_key(old_path);
        let new_key = self.object_key(new_path);

        self.client
            .copy_object()
            .bucket(&self.config.bucket)
            .key(&new_key)
            .copy_source(self.copy_source(&old_key))
            .send()
            .await
            .map_err(|e| self.map_sdk_error(e, "rename", &old_key))?;

        self.delete(old_path).await
    }
}
