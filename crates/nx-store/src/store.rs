use std::fs;
use std::path::Path;

use crate::StoreError;

#[derive(Clone)]
pub struct Store {
    db: sled::Db,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();

        // if exists, deve essere una dir
        if path.exists() && !path.is_dir() {
            return Err(StoreError::NotADirectory(path.display().to_string()));
        }

        // if NOT exists, crea (inclusi parent)
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        // Apri sled dentro quella dir
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let v = self.db.get(key)?;
        Ok(v.map(|ivec| ivec.to_vec()))
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.db.insert(key, value)?;
        // per ora flush esplicito no; sled è safe. Prevedo u n“durability più forte”:
        // self.db.flush()?;
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.db.remove(key)?;
        Ok(())
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
