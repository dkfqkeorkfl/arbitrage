use std::collections::{HashMap, LinkedList};
use std::sync::Arc;

use crate::RwArc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Data<K, V> {
    key: K,
    used: usize,
    data: Arc<V>,
}
type DataRwArc<K, V> = RwArc<Data<K, V>>;

impl<K, V> Data<K, V> {
    pub fn new(key: K, value: Arc<V>) -> DataRwArc<K, V> {
        Arc::new(RwLock::new(Data {
            key: key,
            used: 0,
            data: value,
        }))
    }
}

/* NUR(Not Used Recently) Page Replacement Algorithm -> LFU(Least Recently Used)*/
#[derive(Debug)]
struct Inner<K, V>
where
    K: Eq + std::hash::Hash,
{
    limit: usize,
    datas: HashMap<K, DataRwArc<K, V>>,
    ordered: LinkedList<DataRwArc<K, V>>,
}
type InnerRwArc<K, V> = RwArc<Inner<K, V>>;

impl<K, V> Inner<K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn new(limit: usize) -> Self {
        Inner {
            limit: limit,
            datas: HashMap::<K, DataRwArc<K, V>>::default(),
            ordered: LinkedList::<DataRwArc<K, V>>::default(),
        }
    }

    async fn gabage(&mut self, data: DataRwArc<K, V>) -> Option<Arc<V>> {
        data.write().await.used += 1;
        self.ordered.push_back(data);

        if self.ordered.len() > self.limit {
            let first = self.ordered.pop_front()?;
            let mut locked = first.write().await;
            locked.used -= 1;
            if locked.used == 0 {
                self.datas.remove(&locked.key)?;
                return Some(locked.data.clone());
            }
        }
        None
    }

    pub async fn insert(&mut self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        let ptr = if let Some(old) = self.datas.get(&key) {
            let mut locked = old.write().await;
            locked.data = value;
            old.clone()
        }
        else {
            let ptr = Data::<K, V>::new(key.clone(), value);
            self.datas.insert(key, ptr.clone());
            ptr
        };

        self.gabage(ptr).await    
    }

    pub async fn get(&mut self, key: &K) -> Option<(Arc<V>, Option<Arc<V>>)> {
        let data = self.datas.get(key).cloned()?;
        let ret = data.read().await.data.clone();
        let gabage = self.gabage(data).await;
        Some((ret, gabage))
    }

    pub async fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        let data = self.datas.remove(key)?;
        let mut locked = data.write().await;
        locked.used = 0;
        Some(locked.data.clone())
    }
}

#[derive(Debug, Clone)]
pub struct NurCache<K, V>
where
    K: Eq + std::hash::Hash,
{
    inner: InnerRwArc<K, V>,
}

impl<K, V> NurCache<K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn new(limit: usize) -> Self {
        NurCache {
            inner: Arc::new(RwLock::new(Inner::<K, V>::new(limit))),
        }
    }

    pub async fn insert_raw(&self, key: K, value: V) -> Option<Arc<V>> {
        self.insert(key, Arc::new(value)).await
    }
    pub async fn insert(&self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        self.inner.write().await.insert(key, value).await
    }

    pub async fn get(&self, key: &K) -> Option<(Arc<V>, Option<Arc<V>>)> {
        self.inner.write().await.get(key).await
    }

    pub async fn remove(&self, key: &K) -> Option<Arc<V>> {
        self.inner.write().await.remove(key).await
    }
}
