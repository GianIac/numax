use std::fs;
use std::path::Path;

use crate::StoreError;

#[derive(Clone)]
pub struct Store {
    db: sled::Db,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    pub keys: u64,
    pub bytes: u64,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();

        // if exists and error if not a dir
        if path.exists() && !path.is_dir() {
            return Err(StoreError::NotADirectory(path.display().to_string()));
        }

        // if NOT exists, create dir
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        // open sled in a specific dir
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let v = self.db.get(key)?;
        Ok(v.map(|ivec| ivec.to_vec()))
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.db.insert(key, value)?;
        // For now, there's no explicit flush; sled is safe. I expect a stronger "durability":
        // self.db.flush()?;
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.db.remove(key)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.db.flush()?;
        Ok(())
    }

    pub fn stats(&self) -> Result<StoreStats, StoreError> {
        let mut stats = StoreStats { keys: 0, bytes: 0 };

        for item in self.db.iter() {
            let (key, value) = item?;
            stats.keys += 1;
            stats.bytes += key.len() as u64 + value.len() as u64;
        }

        Ok(stats)
    }

    #[allow(clippy::type_complexity)]
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let mut out = Vec::new();
        for item in self.db.scan_prefix(prefix) {
            let (k, v) = item?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }
}
