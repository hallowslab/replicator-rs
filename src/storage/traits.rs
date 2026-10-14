use async_trait::async_trait;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Transfer failed: {0}")]
    Transfer(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

impl StorageError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StorageError::Connection(_) | StorageError::Transfer(_) | StorageError::Io(_)
        )
    }
}

/// Per-upload resume state, threaded through [`StorageBackend::upload`].
/// Backends with resumable multipart uploads (S3) read/write this so a retry
/// continues from the parts already uploaded instead of restarting the whole
/// file. Other backends ignore it. Persisted by the caller between attempts.
/// Progress hook invoked by resumable backends after each completed part (and
/// after creating a multipart upload), so the caller can persist state
/// incrementally - a hard kill mid-upload then resumes from the last
/// completed part instead of restarting the file.
pub type PartProgress = Box<dyn FnMut(&UploadContext) + Send>;

#[derive(Default)]
pub struct UploadContext {
    /// In-progress multipart upload id, reused across attempts. `None` when
    /// no multipart is open (either not started, or completed/expired).
    pub multipart_upload_id: Option<String>,
    /// Completed (part_number, etag) pairs. Skipped on resume and used for
    /// `CompleteMultipartUpload`, so the backend never needs bucket-level
    /// listing (works with write-only credentials).
    pub parts: Vec<(i32, String)>,
    /// Progress hook (see [`PartProgress`]).
    pub on_part: Option<PartProgress>,
}

impl std::fmt::Debug for UploadContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadContext")
            .field("multipart_upload_id", &self.multipart_upload_id)
            .field("parts", &self.parts)
            .field("on_part", &self.on_part.is_some())
            .finish()
    }
}

impl UploadContext {
    /// Runs the progress hook if set. Safe to call after any mutation of
    /// `multipart_upload_id`/`parts`.
    pub fn notify_progress(&mut self) {
        let mut cb = self.on_part.take();
        if let Some(f) = cb.as_mut() {
            f(self);
        }
        self.on_part = cb;
    }
}

#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Uploads `source_path` to the exact `dest_key` (already resolved via
    /// [`StorageBackend::resolve_key`]). `ctx` carries resumable-multipart
    /// state in/out so retries can skip completed parts.
    async fn upload(
        &self,
        source_path: &Path,
        dest_key: &str,
        ctx: &mut UploadContext,
    ) -> Result<(), StorageError>;
    async fn delete(&self, path: &str) -> Result<(), StorageError>;
    fn name(&self) -> &str;
    /// Resolves a logical destination path into the final storage key. Callers
    /// invoke this once per record and reuse the result, so non-deterministic
    /// transformations (e.g. `immutable_naming` UUIDs) are only applied once.
    fn resolve_key(&self, path: &str) -> String {
        path.to_string()
    }
    fn capabilities(&self) -> Option<&dyn BackendCapabilities> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    Rename,
    Mkdir,
}

#[async_trait]
pub trait BackendCapabilities: StorageBackend {
    fn supports(&self, _capability: Capability) -> bool {
        false
    }

    async fn rename(&self, _old_path: &str, _new_path: &str) -> Result<(), StorageError> {
        Err(StorageError::Config(format!(
            "rename not supported by backend '{}'",
            self.name()
        )))
    }

    async fn mkdir(&self, _path: &str) -> Result<(), StorageError> {
        Err(StorageError::Config(format!(
            "mkdir not supported by backend '{}'",
            self.name()
        )))
    }
}

pub async fn rename_on(
    backend: &dyn StorageBackend,
    old_path: &str,
    new_path: &str,
) -> Result<(), StorageError> {
    let caps = backend.capabilities().ok_or_else(|| {
        StorageError::Config(format!("rename not supported by backend '{}'", backend.name()))
    })?;
    if !caps.supports(Capability::Rename) {
        return Err(StorageError::Config(format!(
            "rename not supported by backend '{}'",
            backend.name()
        )));
    }
    caps.rename(old_path, new_path).await
}

pub async fn mkdir_on(backend: &dyn StorageBackend, path: &str) -> Result<(), StorageError> {
    let caps = backend.capabilities().ok_or_else(|| {
        StorageError::Config(format!("mkdir not supported by backend '{}'", backend.name()))
    })?;
    if !caps.supports(Capability::Mkdir) {
        return Err(StorageError::Config(format!(
            "mkdir not supported by backend '{}'",
            backend.name()
        )));
    }
    caps.mkdir(path).await
}
