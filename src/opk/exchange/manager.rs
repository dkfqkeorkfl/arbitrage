use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use tokio::sync::RwLock;

use crate::opk::{protocols::*, websocket::WebsocketParam, exchanges::*, leveldb_str::Leveldb};
use super::{Exchange, ExchangeWeak};

struct Inner {
    tags : Arc<Vec<String>>,
    keys: HashMap<String, Arc<ExchangeKey>>,
    instatnts: HashMap<String, ExchangeWeak>,
    db : Leveldb
}

impl Inner {
    pub fn new(keys: HashMap<String, Arc<ExchangeKey>>, db : Leveldb) -> Self {
        Inner {
            tags : Arc::new(keys.keys().cloned().collect::<Vec<_>>()),
            keys: keys,
            instatnts: HashMap::<String, ExchangeWeak>::default(),
            db : db
        }
    }

    pub async fn instant(&mut self, tag: &str, config : ExchangeConfig) -> anyhow::Result<Exchange> {
        // let config = ExchangeConfig {
        //     ping_interval: chrono::Duration::minutes(1),
        //     eject: chrono::Duration::seconds(5),
        //     sync_expired_duration: chrono::Duration::minutes(5),
        //     state_expired_duration: chrono::Duration::minutes(1),

        //     opt_max_order_chche: 2000,
        //     opt_max_trades_chche: 2000,
        // };

        if let Some(exchange) = self.instatnts.get(tag).and_then(|weak| weak.origin()) {
            return Ok(exchange);
        }

        let key = self.keys.get(tag).ok_or(anyhow::anyhow!("cannot find tag : {}", tag))?;
        let (ws, restapi) = match key.exchange.as_str() {
            "bybit" => {
                let mut ws = WebsocketParam::default();
                ws.url = if key.is_testnet {"wss://stream-testnet.bybit.com"} else {"wss://stream-testnet.bybit.com"}.to_string();
                let mut restapi = RestAPIParam::default();
                restapi.url = if key.is_testnet {"https://api-testnet.bybit.com"} else {"https://api-testnet.bybit.com"}.to_string();
                Some((ws, restapi))
            },
            _=> {
                None
            }
        }.ok_or(anyhow!("invalid exchange name : {}", key.exchange))?;

        let param = ExchangeParam {
            websocket : ws,
            restapi : restapi,
            key : key.clone(),
            config : config,
            kind : MarketOpt::All
        };
        
        let exchange = match key.exchange.as_str() {
            "bybit" => {
                Exchange::new::<bybit::RestAPI,bybit::WebsocketItf>(param, self.db.clone(), None).await        
            },
            _=> {
                Err(anyhow!("invalid exchange name : {}", key.exchange))
            }
        }?;

        self.instatnts.insert(key.tag.clone(), exchange.weak());
        Ok(exchange)
    }
}

#[derive(Clone)]
pub struct Manager {
    ptr : RwArc<Inner>
}


impl Manager {
    pub fn new(keys: HashMap<String, Arc<ExchangeKey>>, db : Leveldb) -> Self {
        let inner = Inner::new(keys, db);
        Self { ptr: Arc::new(RwLock::new(inner)) }
    }

    pub async fn tags(&self) -> Arc<Vec<String>> {
        let locked = self.ptr.read().await;
        locked.tags.clone()
    }

    pub async fn instant(&self, tag: &str, config : ExchangeConfig) -> anyhow::Result<Exchange> {
        let mut locked = self.ptr.write().await;
        locked.instant(tag, config).await
    }
}



