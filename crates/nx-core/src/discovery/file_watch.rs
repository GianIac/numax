use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::dynamic::{AbortOnDropTask, DynamicState};
use super::{
    DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY, DEFAULT_MAX_PEER_CANDIDATES,
    DiscoveryError, DiscoverySnapshot, DiscoveryWatch, PeerAnnouncement, PeerDiscovery,
};

const PROVIDER: &str = "file";
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_MAX_FILE_BYTES: usize = 1024 * 1024;

/// Limits and polling policy for [`FileWatchDiscovery`].
#[derive(Debug, Clone)]
pub struct FileWatchDiscoveryConfig {
    pub path: PathBuf,
    pub cluster_id: String,
    pub poll_interval: Duration,
    pub max_file_bytes: usize,
    pub max_candidates: usize,
    pub event_capacity: usize,
}

impl FileWatchDiscoveryConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            cluster_id: DEFAULT_DISCOVERY_CLUSTER.to_string(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_candidates: DEFAULT_MAX_PEER_CANDIDATES,
            event_capacity: DEFAULT_DISCOVERY_EVENT_CAPACITY,
        }
    }
}

struct Lifecycle {
    stopped: bool,
    shutdown: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

struct Inner {
    config: FileWatchDiscoveryConfig,
    state: Arc<DynamicState>,
    lifecycle: StdMutex<Lifecycle>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let lifecycle = self
            .lifecycle
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(shutdown) = lifecycle.shutdown.take() {
            let _ = shutdown.send(true);
        }
        if let Some(task) = lifecycle.task.take() {
            task.abort();
        }
    }
}

/// Watches an externally managed UTF-8 peer file.
///
/// Each non-empty line is one endpoint; leading/trailing whitespace is removed
/// and lines beginning with `#` are comments. Updates are accepted atomically:
/// an unreadable, oversized, non-UTF-8, or over-limit version leaves the last
/// valid snapshot in place. A missing file is a valid empty snapshot, which
/// supports Kubernetes-style atomic replacement and delayed creation.
pub struct FileWatchDiscovery {
    inner: Arc<Inner>,
}

impl FileWatchDiscovery {
    pub fn new(config: FileWatchDiscoveryConfig) -> Result<Self, DiscoveryError> {
        validate_config(&config)?;
        Ok(Self {
            inner: Arc::new(Inner {
                state: Arc::new(DynamicState::new(config.event_capacity)),
                config,
                lifecycle: StdMutex::new(Lifecycle {
                    stopped: false,
                    shutdown: None,
                    task: None,
                }),
            }),
        })
    }

    async fn ensure_started(&self) -> Result<(), DiscoveryError> {
        {
            let lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if lifecycle.stopped {
                return Err(provider_error("provider is shut down", false));
            }
            if lifecycle.task.is_some() {
                return Ok(());
            }
        }

        let initial = read_peer_file(&self.inner.config).await?;
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if lifecycle.stopped {
            return Err(provider_error("provider is shut down", false));
        }
        if lifecycle.task.is_some() {
            return Ok(());
        }
        self.inner.state.observe(initial);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let config = self.inner.config.clone();
        let state = Arc::clone(&self.inner.state);
        lifecycle.shutdown = Some(shutdown);
        lifecycle.task = Some(tokio::spawn(async move {
            run_file_watch(config, state, shutdown_rx).await;
        }));
        Ok(())
    }
}

#[async_trait]
impl PeerDiscovery for FileWatchDiscovery {
    fn cluster_id(&self) -> &str {
        &self.inner.config.cluster_id
    }

    async fn discover(&self) -> Result<DiscoverySnapshot, DiscoveryError> {
        self.ensure_started().await?;
        Ok(self.inner.state.snapshot())
    }

    async fn announce(&self, _announcement: &PeerAnnouncement) -> Result<(), DiscoveryError> {
        Err(DiscoveryError::Unsupported {
            provider: PROVIDER.to_string(),
            operation: "announcement",
        })
    }

    async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
        self.ensure_started().await?;
        Ok(self.inner.state.watch())
    }

    fn request_shutdown(&self) {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        lifecycle.stopped = true;
        if let Some(shutdown) = lifecycle.shutdown.as_ref() {
            let _ = shutdown.send(true);
        }
    }

    async fn shutdown(&self) -> Result<(), DiscoveryError> {
        self.request_shutdown();
        let task = {
            let mut lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            lifecycle.shutdown.take();
            lifecycle.task.take()
        };
        if let Some(task) = task {
            AbortOnDropTask::new(task)
                .join()
                .await
                .map_err(|error| provider_error(format!("watch task failed: {error}"), false))?;
        }
        self.inner.state.replace(Vec::new());
        Ok(())
    }
}

async fn run_file_watch(
    config: FileWatchDiscoveryConfig,
    state: Arc<DynamicState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The initial view was loaded by ensure_started().
    interval.tick().await;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => match read_peer_file(&config).await {
                Ok(peers) => state.observe(peers),
                Err(error) => tracing::warn!(%error, path = %config.path.display(), "ignoring invalid peer file update"),
            }
        }
    }
}

async fn read_peer_file(config: &FileWatchDiscoveryConfig) -> Result<Vec<String>, DiscoveryError> {
    let file = match tokio::fs::File::open(&config.path).await {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error(&config.path, error)),
    };
    let limit = u64::try_from(config.max_file_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::new();
    file.take(limit)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| io_error(&config.path, error))?;
    if bytes.len() > config.max_file_bytes {
        return Err(provider_error(
            format!(
                "{} exceeds the {} byte limit",
                config.path.display(),
                config.max_file_bytes
            ),
            true,
        ));
    }
    let contents = String::from_utf8(bytes).map_err(|_| {
        provider_error(
            format!("{} is not valid UTF-8", config.path.display()),
            true,
        )
    })?;
    parse_peer_file(&contents, config.max_candidates)
}

fn parse_peer_file(contents: &str, max_candidates: usize) -> Result<Vec<String>, DiscoveryError> {
    let mut seen = HashSet::new();
    let mut peers = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let endpoint = line.trim();
        if endpoint.is_empty() || endpoint.starts_with('#') {
            continue;
        }
        let endpoint = crate::sync_manager::canonicalize_endpoint(endpoint).map_err(|error| {
            provider_error(format!("line {} is invalid: {error}", index + 1), true)
        })?;
        if seen.insert(endpoint.clone()) {
            if peers.len() == max_candidates {
                return Err(provider_error(
                    format!("peer file exceeds the {max_candidates} candidate limit"),
                    true,
                ));
            }
            peers.push(endpoint);
        }
    }
    Ok(peers)
}

fn validate_config(config: &FileWatchDiscoveryConfig) -> Result<(), DiscoveryError> {
    if config.path.as_os_str().is_empty() {
        return Err(invalid("path must not be empty"));
    }
    if config.cluster_id.trim().is_empty() {
        return Err(invalid("cluster_id must not be empty"));
    }
    if config.poll_interval.is_zero() {
        return Err(invalid("poll_interval must be greater than zero"));
    }
    if config.max_file_bytes == 0 || config.max_candidates == 0 || config.event_capacity == 0 {
        return Err(invalid(
            "limits and event_capacity must be greater than zero",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> DiscoveryError {
    DiscoveryError::InvalidConfiguration {
        provider: PROVIDER.to_string(),
        message: message.into(),
    }
}

fn provider_error(message: impl Into<String>, retryable: bool) -> DiscoveryError {
    DiscoveryError::Provider {
        provider: PROVIDER.to_string(),
        message: message.into(),
        retryable,
    }
}

fn io_error(path: &Path, error: std::io::Error) -> DiscoveryError {
    provider_error(format!("cannot read {}: {error}", path.display()), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn replace_file(path: &Path, contents: &str) {
        let staging = path.with_extension("staging");
        tokio::fs::write(&staging, contents).await.unwrap();
        tokio::fs::rename(staging, path).await.unwrap();
    }

    #[test]
    fn parser_preserves_order_and_deduplicates() {
        let peers = parse_peer_file("# peers\n b:2 \na:1\nb:2\n", 2).unwrap();
        assert_eq!(peers, ["b:2", "a:1"]);
    }

    #[test]
    fn parser_rejects_the_whole_over_limit_update() {
        assert!(parse_peer_file("a:1\nb:2\n", 1).is_err());
    }

    #[tokio::test]
    async fn missing_file_is_an_empty_initial_snapshot_and_shutdown_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let discovery = FileWatchDiscovery::new(FileWatchDiscoveryConfig::new(
            directory.path().join("peers"),
        ))
        .unwrap();
        assert!(discovery.discover().await.unwrap().peers().is_empty());
        discovery.shutdown().await.unwrap();
        discovery.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn watch_applies_complete_files_retains_last_good_and_stops_on_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peers");
        let mut config = FileWatchDiscoveryConfig::new(&path);
        config.poll_interval = Duration::from_millis(10);
        let discovery = FileWatchDiscovery::new(config).unwrap();
        let mut watch = discovery.watch().await.unwrap();
        assert!(watch.snapshot().peers().is_empty());

        replace_file(&path, "b.example:2\na.example:1\n").await;
        let peers = super::super::next_changed_peers(&mut watch, &[]).await;
        assert_eq!(peers, ["b.example:2", "a.example:1"]);

        replace_file(&path, "valid.example:3\nnot-an-endpoint\n").await;
        assert!(read_peer_file(&discovery.inner.config).await.is_err());

        tokio::fs::write(&path, [0xff, 0xfe]).await.unwrap();
        assert!(read_peer_file(&discovery.inner.config).await.is_err());
        replace_file(&path, "recovered.example:5\n").await;
        let recovered = super::super::next_changed_peers(&mut watch, &peers).await;
        assert_eq!(recovered, ["recovered.example:5"]);

        tokio::fs::remove_file(&path).await.unwrap();
        assert!(
            super::super::next_changed_peers(&mut watch, &recovered)
                .await
                .is_empty()
        );

        discovery.shutdown().await.unwrap();
        let stopped_revision = discovery.inner.state.snapshot().revision();
        replace_file(&path, "late.example:4\n").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(40), async {
                loop {
                    // Queued observations from before shutdown remain valid;
                    // no event may have been produced after the final revision.
                    assert!(watch.recv().await.unwrap().revision <= stopped_revision);
                }
            })
            .await
            .is_err()
        );
    }
}
