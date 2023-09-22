use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::opk::websocket::Websocket;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub debug: String,
    pub webserver_port: String,
    pub websocket_port: String,
    pub cert: String,
    pub key: String,

    pub notify_token: String,
    pub notify_log_info: String,
    pub notify_log_warn: String,
    pub notify_log_crit: String,
    pub notify_log_auth: String,

    pub apikey_sheet: String,
    pub apikey_tab: String,

    pub server_eject: String,
    pub server_ping: String,

    pub cache_name: String,
}

pub struct Context {
    pub config: Config,
    pub exchanges: crate::opk::exchange::Manager,
    pub db: crate::opk::leveldb_str::Leveldb,
}

#[async_trait]
pub trait ServiceTrait: Send + Sync + 'static {
    async fn open(&mut self, id: &str, ws: Websocket) -> anyhow::Result<()> {
        Ok(())
    }

    async fn release(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn proccess(
        &mut self,
        context: &Arc<Context>,
        json: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(Default::default())
    }
}

#[derive(Default)]
pub struct ServiceNone;

impl ServiceTrait for ServiceNone {}

pub struct App {
    inner: Arc<Mutex<dyn ServiceTrait>>,
    id: String,
}

impl Default for App {
    fn default() -> Self {
        let inner = Arc::new(Mutex::new(ServiceNone::default()));
        Self {
            inner : inner,
            id : Default::default()
        }
    }
}

impl App {
    pub async fn new<A>(id: String) -> Self
    where
        A: ServiceTrait + Default + Sized,
    {
        let inner = Arc::new(Mutex::new(A::default()));
        Self {
            inner: inner,
            id: id,
        }
    }

    pub async fn open(&self, ws: Websocket) -> anyhow::Result<()> {
        let mut locked = self.inner.lock().await;
        locked.open(&self.id, ws).await
    }

    pub async fn release(&self) -> anyhow::Result<()> {
        let mut locked = self.inner.lock().await;
        locked.release().await
    }

    pub async fn proccess(
        &self,
        context: &Arc<Context>,
        json: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let mut locked = self.inner.lock().await;
        locked.proccess(context, json).await
    }
}
