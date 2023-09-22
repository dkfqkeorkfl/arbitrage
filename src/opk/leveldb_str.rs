// use leveldb::;
use std::{path::Path, sync::Arc};

use leveldb::iterator::Iterable;
use leveldb::options::{ReadOptions, WriteOptions};
use leveldb::{db::Database, options::Options};

#[derive(Debug, Clone)]
pub struct Leveldb {
    db: Arc<Database>,
    wo: WriteOptions,
    ro: ReadOptions,
}

impl Leveldb {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let p = Path::new(path);
        let mut option = Options::new();
        option.create_if_missing = true;
        let db = Database::open(&p, &option)?;
        Ok(Leveldb {
            db: Arc::new(db),
            wo: WriteOptions::new(),
            ro: ReadOptions::new(),
        })
    }

    pub fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let vb = value.as_bytes();
        self.db.put(&self.wo, &key, vb).map_err(anyhow::Error::new)
    }

    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.db
            .get(&self.ro, &key)
            .map(|ret| ret.map(|bytes| String::from_utf8_lossy(bytes.as_slice()).to_string()))
            .map_err(anyhow::Error::new)
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        Self: Sized,
        F: FnMut(usize, String, String),
    {
        let iter = self.db.iter(&self.ro);
        for entry in iter.enumerate() {
            let (i, (key, value)) = entry;
            let key_str = String::from_utf8_lossy(key.as_slice()).to_string();
            let value_str = String::from_utf8_lossy(value.as_slice()).to_string();
            f(i, key_str, value_str);
        }
    }
}
