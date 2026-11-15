use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nx_store::Store as NxStore;

use crate::observability::RuntimeMetrics;
use crate::runtime::RuntimeExecutor;
use crate::sync_manager::SyncHandle;

const RESERVED_PREFIX: &[u8] = b"__nx/";
const MODULE_DATA_PREFIX: &[u8] = b"__nx/modules/data/";
const MODULE_META_PREFIX: &[u8] = b"__nx/modules/meta/";
pub const MAX_CONTROL_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleInfo {
    pub id: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleRegistration {
    pub module: ModuleInfo,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub address: String,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRegistration {
    pub address: String,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerCandidate {
    pub address: String,
    pub sources: Vec<String>,
    pub persisted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerView {
    Candidates,
    Connections,
    Membership,
    Recovery,
}

#[derive(Debug, Clone)]
pub enum PeerObservation {
    Candidate {
        candidate: PeerCandidate,
        reconnect: Option<crate::sync_manager::PeerHealthState>,
    },
    Connection(crate::PeerConnectionInfo),
    Membership {
        record: nx_net::MembershipRecord,
        service_error: Option<String>,
    },
    Recovery {
        progress: crate::sync_manager::ReplicationProgress,
        in_flight: bool,
    },
}

impl PeerObservation {
    pub fn cursor_key(&self) -> &str {
        match self {
            Self::Candidate { candidate, .. } => &candidate.address,
            Self::Connection(connection) => &connection.transport_addr,
            Self::Membership { record, .. } => &record.member,
            Self::Recovery { progress, .. } => progress.origin.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    InvalidPeer,
    PeerCapacityExceeded,
    ServiceUnavailable,
    PeerAcceptanceUncertain(String),
    InvalidCursor,
    InvalidLimit,
    ModuleNotFound,
    KeyNotFound,
    InvalidModule(String),
    ModuleExecutionFailed(String),
    Storage(String),
    Internal(String),
}

impl fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPeer => formatter.write_str("invalid peer endpoint"),
            Self::PeerCapacityExceeded => formatter.write_str("peer candidate capacity exceeded"),
            Self::ServiceUnavailable => formatter.write_str("peer service unavailable"),
            Self::PeerAcceptanceUncertain(error) => {
                write!(formatter, "peer acceptance uncertain: {error}")
            }
            Self::InvalidCursor => formatter.write_str("invalid cursor"),
            Self::InvalidLimit => write!(
                formatter,
                "page limit must be between 1 and {MAX_CONTROL_PAGE_SIZE}"
            ),
            Self::ModuleNotFound => formatter.write_str("module not found"),
            Self::KeyNotFound => formatter.write_str("key not found"),
            Self::InvalidModule(error) => write!(formatter, "invalid module: {error}"),
            Self::ModuleExecutionFailed(error) => {
                write!(formatter, "module execution failed: {error}")
            }
            Self::Storage(error) => write!(formatter, "storage error: {error}"),
            Self::Internal(error) => write!(formatter, "internal control error: {error}"),
        }
    }
}

impl Error for ControlError {}

/// Collection methods require a limit between 1 and [`MAX_CONTROL_PAGE_SIZE`].
#[async_trait]
pub trait RuntimeIntrospection: Send + Sync {
    async fn inspect_peers(
        &self,
        view: PeerView,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<PeerObservation>, ControlError>;

    async fn list_modules(
        &self,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<ModuleInfo>, ControlError>;

    async fn get_module(&self, id: String) -> Result<ModuleInfo, ControlError>;

    async fn list_peers(
        &self,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<PeerInfo>, ControlError>;

    async fn list_keys(
        &self,
        prefix: Vec<u8>,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<Vec<u8>>, ControlError>;

    async fn get_value(&self, key: Vec<u8>) -> Result<Vec<u8>, ControlError>;

    fn is_healthy(&self) -> bool;

    fn is_ready(&self) -> bool;
}

#[async_trait]
pub trait RuntimeManagement: Send + Sync {
    async fn add_peer(&self, address: String) -> Result<PeerRegistration, ControlError>;

    async fn register_module(&self, bytes: Vec<u8>) -> Result<ModuleRegistration, ControlError>;

    async fn delete_module(&self, id: String) -> Result<(), ControlError>;

    async fn run_module(&self, id: String) -> Result<(), ControlError>;
}

pub trait RuntimeControl: RuntimeIntrospection + RuntimeManagement {}

impl<T> RuntimeControl for T where T: RuntimeIntrospection + RuntimeManagement {}

pub type SharedRuntimeControl = Arc<dyn RuntimeControl>;

#[derive(Clone)]
pub struct RuntimeControlHandle {
    executor: Arc<RuntimeExecutor>,
    modules: ModuleRegistry,
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,
    sync_handle: Option<SyncHandle>,
}

impl RuntimeControlHandle {
    pub(crate) fn new(
        executor: Arc<RuntimeExecutor>,
        modules: ModuleRegistry,
        store: Arc<NxStore>,
        metrics: Arc<RuntimeMetrics>,
        sync_handle: Option<SyncHandle>,
    ) -> Self {
        Self {
            executor,
            modules,
            store,
            metrics,
            sync_handle,
        }
    }
}

#[async_trait]
impl RuntimeIntrospection for RuntimeControlHandle {
    async fn inspect_peers(
        &self,
        view: PeerView,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<PeerObservation>, ControlError> {
        validate_page_limit(limit)?;
        let cursor = cursor
            .map(String::from_utf8)
            .transpose()
            .map_err(|_| ControlError::InvalidCursor)?;
        let handle = self
            .sync_handle
            .as_ref()
            .ok_or(ControlError::ServiceUnavailable)?;
        let mut items = handle.inspect_peers(view).await?;
        items.sort_by(|left, right| left.cursor_key().cmp(right.cursor_key()));
        items.retain(|item| {
            cursor
                .as_ref()
                .is_none_or(|cursor| item.cursor_key() > cursor.as_str())
        });
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = if has_more {
            items
                .last()
                .map(|item| item.cursor_key().as_bytes().to_vec())
        } else {
            None
        };
        Ok(ControlPage { items, next_cursor })
    }

    async fn list_modules(
        &self,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<ModuleInfo>, ControlError> {
        self.modules.list(cursor.as_deref(), limit)
    }

    async fn get_module(&self, id: String) -> Result<ModuleInfo, ControlError> {
        self.modules.info(&id)?.ok_or(ControlError::ModuleNotFound)
    }

    async fn list_peers(
        &self,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<PeerInfo>, ControlError> {
        validate_page_limit(limit)?;
        let cursor = cursor
            .map(String::from_utf8)
            .transpose()
            .map_err(|_| ControlError::InvalidCursor)?;
        let peers = match &self.sync_handle {
            Some(handle) => handle.connected_peers().await,
            None => Vec::new(),
        };
        let mut items = peers
            .into_iter()
            .filter(|(address, _)| cursor.as_ref().is_none_or(|cursor| address > cursor))
            .map(|(address, node_id)| PeerInfo {
                address,
                node_id: node_id.to_string(),
            })
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = has_more.then(|| {
            items
                .last()
                .expect("a peer page with more items cannot be empty")
                .address
                .as_bytes()
                .to_vec()
        });

        Ok(ControlPage { items, next_cursor })
    }

    async fn list_keys(
        &self,
        prefix: Vec<u8>,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<ControlPage<Vec<u8>>, ControlError> {
        validate_page_limit(limit)?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.starts_with(RESERVED_PREFIX)
                || (!prefix.is_empty() && !cursor.starts_with(&prefix))
        }) {
            return Err(ControlError::InvalidCursor);
        }

        let mut items = self
            .store
            .keys_prefix_page_after(
                &prefix,
                cursor.as_deref(),
                u32::try_from(limit.saturating_add(1)).unwrap_or(u32::MAX),
                Some(RESERVED_PREFIX),
            )
            .map_err(storage_error)?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = has_more.then(|| {
            items
                .last()
                .expect("a key page with more items cannot be empty")
                .clone()
        });

        Ok(ControlPage { items, next_cursor })
    }

    async fn get_value(&self, key: Vec<u8>) -> Result<Vec<u8>, ControlError> {
        if key.starts_with(RESERVED_PREFIX) {
            return Err(ControlError::KeyNotFound);
        }
        self.store
            .get(&key)
            .map_err(storage_error)?
            .ok_or(ControlError::KeyNotFound)
    }

    fn is_healthy(&self) -> bool {
        true
    }

    fn is_ready(&self) -> bool {
        self.metrics.is_ready()
    }
}

#[async_trait]
impl RuntimeManagement for RuntimeControlHandle {
    async fn add_peer(&self, address: String) -> Result<PeerRegistration, ControlError> {
        self.sync_handle
            .as_ref()
            .ok_or(ControlError::ServiceUnavailable)?
            .add_peer(address)
            .await
    }

    async fn register_module(&self, bytes: Vec<u8>) -> Result<ModuleRegistration, ControlError> {
        self.executor
            .validate_module(&bytes)
            .map_err(|error| ControlError::InvalidModule(error.to_string()))?;
        self.modules.register(&bytes)
    }

    async fn delete_module(&self, id: String) -> Result<(), ControlError> {
        self.modules.delete(&id)
    }

    async fn run_module(&self, id: String) -> Result<(), ControlError> {
        let bytes = self
            .modules
            .bytes(&id)?
            .ok_or(ControlError::ModuleNotFound)?;
        self.executor
            .run_module(&bytes, &id)
            .await
            .map_err(|error| ControlError::ModuleExecutionFailed(error.to_string()))
    }
}

#[derive(Clone)]
pub(crate) struct ModuleRegistry {
    store: Arc<NxStore>,
    mutation_lock: Arc<Mutex<()>>,
}

impl ModuleRegistry {
    pub(crate) fn new(store: Arc<NxStore>) -> Self {
        Self {
            store,
            mutation_lock: Arc::new(Mutex::new(())),
        }
    }

    fn register(&self, bytes: &[u8]) -> Result<ModuleRegistration, ControlError> {
        let id = blake3::hash(bytes).to_hex().to_string();
        let data_key = module_key(MODULE_DATA_PREFIX, &id);
        let meta_key = module_key(MODULE_META_PREFIX, &id);
        let _guard = self
            .mutation_lock
            .lock()
            .map_err(|_| ControlError::Internal("module registry lock poisoned".to_string()))?;

        if let Some(existing) = self.store.get(&data_key).map_err(storage_error)? {
            if existing != bytes {
                return Err(ControlError::Internal(
                    "module digest collision detected".to_string(),
                ));
            }
            return Ok(ModuleRegistration {
                module: ModuleInfo {
                    id,
                    size_bytes: bytes.len() as u64,
                },
                created: false,
            });
        }

        let size = (bytes.len() as u64).to_be_bytes();
        self.store
            .apply_batch(&[(&data_key, bytes), (&meta_key, &size)], &[])
            .map_err(storage_error)?;
        self.store.flush().map_err(storage_error)?;

        Ok(ModuleRegistration {
            module: ModuleInfo {
                id,
                size_bytes: bytes.len() as u64,
            },
            created: true,
        })
    }

    fn list(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<ControlPage<ModuleInfo>, ControlError> {
        validate_page_limit(limit)?;
        let start_after = cursor.map(|cursor| module_key(MODULE_META_PREFIX, cursor));
        let rows = self
            .store
            .scan_prefix_page_after(
                MODULE_META_PREFIX,
                start_after.as_deref(),
                u32::try_from(limit.saturating_add(1)).unwrap_or(u32::MAX),
                None,
            )
            .map_err(storage_error)?;
        let mut items = rows
            .into_iter()
            .map(|(key, value)| decode_module_info(&key, &value))
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = has_more.then(|| {
            items
                .last()
                .expect("a module page with more items cannot be empty")
                .id
                .as_bytes()
                .to_vec()
        });

        Ok(ControlPage { items, next_cursor })
    }

    fn info(&self, id: &str) -> Result<Option<ModuleInfo>, ControlError> {
        let meta_key = module_key(MODULE_META_PREFIX, id.as_bytes());
        self.store
            .get(&meta_key)
            .map_err(storage_error)?
            .map(|value| decode_module_info(&meta_key, &value))
            .transpose()
    }

    fn bytes(&self, id: &str) -> Result<Option<Vec<u8>>, ControlError> {
        self.store
            .get(&module_key(MODULE_DATA_PREFIX, id.as_bytes()))
            .map_err(storage_error)
    }

    fn delete(&self, id: &str) -> Result<(), ControlError> {
        let data_key = module_key(MODULE_DATA_PREFIX, id.as_bytes());
        let meta_key = module_key(MODULE_META_PREFIX, id.as_bytes());
        let _guard = self
            .mutation_lock
            .lock()
            .map_err(|_| ControlError::Internal("module registry lock poisoned".to_string()))?;
        self.store
            .apply_batch(&[], &[&data_key, &meta_key])
            .map_err(storage_error)?;
        self.store.flush().map_err(storage_error)
    }
}

fn module_key(prefix: &[u8], id: impl AsRef<[u8]>) -> Vec<u8> {
    let id = id.as_ref();
    let mut key = Vec::with_capacity(prefix.len() + id.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(id);
    key
}

fn decode_module_info(key: &[u8], value: &[u8]) -> Result<ModuleInfo, ControlError> {
    let id = key
        .strip_prefix(MODULE_META_PREFIX)
        .ok_or_else(|| ControlError::Internal("invalid module metadata key".to_string()))?;
    let id = String::from_utf8(id.to_vec())
        .map_err(|_| ControlError::Internal("module ID is not valid UTF-8".to_string()))?;
    let size = <[u8; 8]>::try_from(value)
        .map(u64::from_be_bytes)
        .map_err(|_| ControlError::Internal("invalid module size metadata".to_string()))?;
    Ok(ModuleInfo {
        id,
        size_bytes: size,
    })
}

fn storage_error(error: nx_store::StoreError) -> ControlError {
    ControlError::Storage(error.to_string())
}

fn validate_page_limit(limit: usize) -> Result<(), ControlError> {
    if !(1..=MAX_CONTROL_PAGE_SIZE).contains(&limit) {
        return Err(ControlError::InvalidLimit);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Runtime, RuntimeConfig};
    use tempfile::{TempDir, tempdir};

    fn registry() -> (TempDir, ModuleRegistry) {
        let directory = tempdir().unwrap();
        let registry = ModuleRegistry::new(Arc::new(NxStore::open(directory.path()).unwrap()));
        (directory, registry)
    }

    fn minimal_run_module() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // header
            0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type: () -> ()
            0x03, 0x02, 0x01, 0x00, // function section
            0x07, 0x07, 0x01, 0x03, b'r', b'u', b'n', 0x00, 0x00, // export run
            0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b, // body
        ]
    }

    fn runtime_config(directory: &TempDir) -> RuntimeConfig {
        RuntimeConfig {
            datastore_path: directory.path().to_path_buf(),
            enable_wasi: false,
            ..RuntimeConfig::default()
        }
    }

    #[test]
    fn registry_is_content_idempotent_and_paginates() {
        let (_directory, registry) = registry();
        let first = registry.register(b"first").unwrap();
        let duplicate = registry.register(b"first").unwrap();
        let second = registry.register(b"second").unwrap();

        assert!(first.created);
        assert!(!duplicate.created);
        assert_eq!(first.module, duplicate.module);

        let page = registry.list(None, 1).unwrap();
        assert_eq!(page.items.len(), 1);
        let next = page.next_cursor.unwrap();
        let page = registry.list(Some(&next), 1).unwrap();
        assert_eq!(page.items.len(), 1);
        assert!(page.next_cursor.is_none());
        assert!(page.items == vec![first.module] || page.items == vec![second.module]);
    }

    #[test]
    fn registry_delete_is_idempotent() {
        let (_directory, registry) = registry();
        let registered = registry.register(b"module").unwrap();

        registry.delete(&registered.module.id).unwrap();
        registry.delete(&registered.module.id).unwrap();

        assert!(registry.info(&registered.module.id).unwrap().is_none());
        assert!(registry.bytes(&registered.module.id).unwrap().is_none());
    }

    #[tokio::test]
    async fn collection_limits_are_validated_by_the_shared_control_interface() {
        let directory = tempdir().unwrap();
        let runtime = Runtime::new(runtime_config(&directory)).unwrap();
        let control = runtime.control_handle();

        for populated in [false, true] {
            if populated {
                control.register_module(minimal_run_module()).await.unwrap();
                control.store.set(b"app:key", b"value").unwrap();
            }
            for limit in [0, MAX_CONTROL_PAGE_SIZE + 1, usize::MAX] {
                assert_eq!(
                    control.list_modules(None, limit).await,
                    Err(ControlError::InvalidLimit)
                );
                assert_eq!(
                    control.list_keys(Vec::new(), None, limit).await,
                    Err(ControlError::InvalidLimit)
                );
                assert_eq!(
                    control.list_peers(None, limit).await,
                    Err(ControlError::InvalidLimit)
                );
            }
            for limit in [1, MAX_CONTROL_PAGE_SIZE] {
                assert!(control.list_modules(None, limit).await.is_ok());
                assert!(control.list_keys(Vec::new(), None, limit).await.is_ok());
                assert!(control.list_peers(None, limit).await.is_ok());
            }
        }
    }

    #[tokio::test]
    async fn empty_keys_can_be_read_and_used_as_exclusive_cursors() {
        let directory = tempdir().unwrap();
        let runtime = Runtime::new(runtime_config(&directory)).unwrap();
        let control = runtime.control_handle();
        control.store.set(b"", b"empty-key-value").unwrap();
        control.store.set(b"app:key", b"value").unwrap();
        let first = control.list_keys(Vec::new(), None, 1).await.unwrap();
        assert_eq!(first.items, vec![Vec::<u8>::new()]);
        assert_eq!(first.next_cursor, Some(Vec::new()));
        let next = control
            .list_keys(Vec::new(), first.next_cursor, 1)
            .await
            .unwrap();
        assert_eq!(next.items, vec![b"app:key".to_vec()]);
        assert_eq!(next.next_cursor, None);
        assert_eq!(
            control.get_value(Vec::new()).await.unwrap(),
            b"empty-key-value"
        );
    }

    #[tokio::test]
    async fn control_validates_registers_runs_and_deletes_modules() {
        let directory = tempdir().unwrap();
        let runtime = Runtime::new(runtime_config(&directory)).unwrap();
        let control = runtime.control_handle();

        assert!(matches!(
            control.register_module(b"not wasm".to_vec()).await,
            Err(ControlError::InvalidModule(_))
        ));
        assert!(
            control
                .list_modules(None, 50)
                .await
                .unwrap()
                .items
                .is_empty()
        );

        let wasm = minimal_run_module();
        let first = control.register_module(wasm.clone()).await.unwrap();
        let duplicate = control.register_module(wasm).await.unwrap();
        assert!(first.created);
        assert!(!duplicate.created);
        assert_eq!(first.module, duplicate.module);
        assert_eq!(
            control.get_module(first.module.id.clone()).await.unwrap(),
            first.module
        );

        control.run_module(first.module.id.clone()).await.unwrap();
        control
            .delete_module(first.module.id.clone())
            .await
            .unwrap();
        control
            .delete_module(first.module.id.clone())
            .await
            .unwrap();
        assert!(matches!(
            control.run_module(first.module.id).await,
            Err(ControlError::ModuleNotFound)
        ));
    }

    #[tokio::test]
    async fn registered_modules_survive_runtime_restart() {
        let directory = tempdir().unwrap();
        let module = {
            let runtime = Runtime::new(runtime_config(&directory)).unwrap();
            runtime
                .control_handle()
                .register_module(minimal_run_module())
                .await
                .unwrap()
                .module
        };

        let runtime = Runtime::new(runtime_config(&directory)).unwrap();
        let persisted = runtime
            .control_handle()
            .get_module(module.id.clone())
            .await
            .unwrap();
        assert_eq!(persisted, module);
    }

    #[tokio::test]
    async fn datastore_introspection_is_binary_safe_and_hides_internal_keys() {
        let directory = tempdir().unwrap();
        {
            let store = NxStore::open(directory.path()).unwrap();
            store.set(&[0, 255, b'k'], &[0, 1, 255]).unwrap();
            store.set(b"app:visible", b"value").unwrap();
            store.set(b"__nx/private", b"secret").unwrap();
            store.flush().unwrap();
        }
        let runtime = Runtime::new(runtime_config(&directory)).unwrap();
        let control = runtime.control_handle();

        let page = control.list_keys(Vec::new(), None, 50).await.unwrap();
        assert_eq!(
            page.items,
            vec![vec![0, 255, b'k'], b"app:visible".to_vec()]
        );
        assert_eq!(
            control.get_value(vec![0, 255, b'k']).await.unwrap(),
            vec![0, 1, 255]
        );
        assert_eq!(
            control
                .list_keys(b"app:".to_vec(), None, 50)
                .await
                .unwrap()
                .items,
            vec![b"app:visible".to_vec()]
        );
        assert!(matches!(
            control
                .list_keys(b"app:".to_vec(), Some(b"other:key".to_vec()), 50)
                .await,
            Err(ControlError::InvalidCursor)
        ));
        assert!(matches!(
            control.get_value(b"__nx/private".to_vec()).await,
            Err(ControlError::KeyNotFound)
        ));
        assert!(control.list_peers(None, 50).await.unwrap().items.is_empty());
        assert!(control.is_healthy());
        assert!(control.is_ready());
    }
}
