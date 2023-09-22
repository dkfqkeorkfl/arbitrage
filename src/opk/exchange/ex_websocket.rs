use std::{
    collections::{HashMap, HashSet},
    ops::{AddAssign, SubAssign},
    sync::Arc,
};

use anyhow::Ok;
use async_trait::async_trait;
use chrono::Utc;
use futures::{future::BoxFuture, lock::Mutex, FutureExt};

use super::super::{protocols::*, websocket::*};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SubscribeCallback {
    pub callback: Arc<
        Mutex<dyn FnMut(Signal, SubscribeResult) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
    >,
}

impl SubscribeCallback {
    pub fn new<F>(f: F) -> Self
    where
        F: FnMut(Signal, SubscribeResult) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        SubscribeCallback {
            callback: Arc::new(Mutex::new(f)),
        }
    }
}

#[async_trait]
pub trait ExWebsocketTrait: Send + Sync {
    async fn parse_msg(
        &mut self,
        context: &ExchangeContextPtr,
        socket: &mut Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult>;

    async fn subscribe(
        &mut self,
        context: &ExchangeContextPtr,
        client: &mut Websocket,
        s: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<()>;

    async fn make_websocket_param(
        &mut self,
        context: &ExchangeContextPtr,
        group: &String,
        subscribes: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<Option<WebsocketParam>>;

    async fn make_group_and_key(
        &mut self,
        s: &SubscribeType,
        param: &SubscribeParam,
    ) -> Option<(String, String)> {
        let default = serde_json::Value::default();
        let json = match s {
            SubscribeType::Balance | SubscribeType::Position => Some(&default),
            SubscribeType::Order | SubscribeType::Orderbook | SubscribeType::PublicTrades => {
                Some(&param.0)
            }
        };

        if let Some(j) = json {
            let str = format!("{}:{}", s.clone() as u32, j.to_string());
            return Some(("".to_string(), str));
        }

        None
    }
}
type ExWebsocketItfMtxPtr = Arc<Mutex<ExWebsocketInfo>>;

pub struct ExWebsocketInfo {
    pub checkedtime: chrono::DateTime<Utc>,
    pub retryed: u32,
    pub websocket: Websocket,
    pub subscribes: HashMap<SubscribeType, Vec<SubscribeParam>>,
    pub is_authorized: bool,
}
type ExWebsocketInfoRwArc = RwArc<ExWebsocketInfo>;

pub struct ExSharedContext {
    pub context: ExchangeContextPtr,
    pub websockets: RwArc<HashMap<String, ExWebsocketInfoRwArc>>,
    pub websockets_by_id: RwArc<HashMap<i64, ExWebsocketInfoRwArc>>,
    pub interface: Arc<RwLock<dyn ExWebsocketTrait>>,
    pub connected_cnt: Arc<RwLock<usize>>,
    pub callback: SubscribeCallback,
}

struct Inner {
    ctx: Arc<ExSharedContext>,
    subscribes: HashSet<String>,
    callback: ReciveCallback,
}

impl ExSharedContext {
    async fn on_msg(
        &self,
        mut websocket: Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult> {
        match signal {
            Signal::Opened => {
                self.connected_cnt.write().await.add_assign(1);

                log::info!(
                    "opened websocket(total:{}, id:{}) : {}",
                    self.connected_cnt.read().await,
                    websocket.get_id().await,
                    websocket.get_param().await.unwrap_or_default().url
                );
            }
            Signal::Closed => {
                self.connected_cnt.write().await.sub_assign(1);
                log::info!(
                    "closed websocket(total:{}: id:{}) : {}",
                    self.connected_cnt.read().await,
                    websocket.get_id().await,
                    websocket.get_param().await.unwrap_or_default().url
                );
            }
            _ => {}
        }

        let result = self
            .lock_interface()
            .await
            .parse_msg(&self.context, &mut websocket, signal)
            .await?;

        match &result {
            SubscribeResult::Authorized(success) => {
                log::info!(
                    "authorized websocket : {}",
                    websocket.get_param().await.unwrap_or_default().url
                );

                let id = websocket.get_id().await;
                if let Some(info) = self.find_websocket_by_id(&id).await {
                    let mut locked = info.write().await;
                    if *success {
                        let mut ws = locked.websocket.clone();
                        locked.is_authorized = *success;
                        self.lock_interface()
                            .await
                            .subscribe(&self.context, &mut ws, &locked.subscribes)
                            .await?;
                    } else {
                        locked.websocket.close(None).await?;
                    }
                }
            }
            _ => {}
        }

        Ok(result)
    }

    pub fn make_recive_callback(ptr: &Arc<ExSharedContext>) -> ReciveCallback {
        let wpt = Arc::downgrade(ptr);
        ReciveCallback::new(move |websocket, signal| {
            let cloned_wpt = wpt.clone();
            async move {
                if let Some(spt) = cloned_wpt.upgrade() {
                    let result = spt
                        .on_msg(websocket, &signal)
                        .await
                        .unwrap_or_else(|e| SubscribeResult::Err(anyhow::Error::from(e)));
                    let mut callback = spt.callback.callback.lock().await;
                    callback(signal, result).await;
                }
            }
            .boxed()
        })
    }

    pub fn new<Interface>(
        context: ExchangeContextPtr,
        interface: Interface,
        callback: SubscribeCallback,
    ) -> Arc<Self>
    where
        Interface: ExWebsocketTrait + Default + 'static,
    {
        Arc::new(ExSharedContext {
            context: context,
            websockets: Arc::new(RwLock::new(
                HashMap::<String, ExWebsocketInfoRwArc>::default(),
            )),
            websockets_by_id: Arc::new(
                RwLock::new(HashMap::<i64, ExWebsocketInfoRwArc>::default()),
            ),
            interface: Arc::new(RwLock::new(interface)),
            connected_cnt: Arc::new(RwLock::new(0)),
            callback: callback,
        })
    }

    pub async fn insert_websocket(
        &self,
        group: String,
        info: ExWebsocketInfo,
    ) -> Option<ExWebsocketInfoRwArc> {
        let id = info.websocket.get_id().await;
        let ptr = Arc::new(RwLock::new(info));
        self.websockets_by_id.write().await.insert(id, ptr.clone());
        self.websockets.write().await.insert(group, ptr)
    }

    pub async fn change_websocket(&self, group: &str, websocket: Websocket) -> Option<Websocket> {
        let info = self.websockets.read().await.get(group).cloned()?;
        let new_id = websocket.get_id().await;
        let old = {
            let mut locked = info.write().await;
            let old = locked.websocket.clone();
            locked.websocket = websocket;

            let now = Utc::now();
            locked.retryed = 0;
            locked.is_authorized = false;
            locked.checkedtime = Utc::now();
            old
        };
        let mut locked = self.websockets_by_id.write().await;
        let ret = locked.remove(&old.get_id().await);
        locked.insert(new_id, info.clone());
        Some(old)
    }

    pub async fn find_websocket(&self, group: &str) -> Option<ExWebsocketInfoRwArc> {
        self.websockets.read().await.get(group).cloned()
    }

    pub async fn find_websocket_by_id(&self, id: &i64) -> Option<ExWebsocketInfoRwArc> {
        self.websockets_by_id.read().await.get(id).cloned()
    }

    pub async fn lock_interface(&self) -> tokio::sync::RwLockWriteGuard<dyn ExWebsocketTrait> {
        self.interface.write().await
    }

    pub async fn get_connected_cnt(&self) -> usize {
        self.connected_cnt.read().await.clone()
    }

    pub async fn get_websocket_cnt(&self) -> usize {
        self.websockets.read().await.len()
    }
}

impl Inner {
    pub async fn check_eject(ctx: Arc<ExSharedContext>) -> anyhow::Result<()> {
        let checktime = &ctx.context.param.config.ping_interval;
        let websockets = ctx.websockets.read().await.clone();

        for (group, value) in &websockets {
            let now = Utc::now();
            let mut info = value.write().await;

            let penalty = if let Some(v) = 2i32.checked_pow(info.retryed) {
                v
            } else {
                1
            };

            let interval = *checktime * penalty;
            // if eject time is 5, result is 5 10 20 40 80 160 320;
            let dur = now - info.checkedtime;
            if interval > dur {
                continue;
            }

            info.checkedtime = Utc::now();
            if info.websocket.is_connected().await {
                continue;
            }

            log::info!(
                "it is disconnected websocket : url({}), is_connected({})",
                info.websocket.get_param().await.unwrap_or_default().url,
                info.websocket.is_connected().await
            );

            let ws_param = ctx
                .lock_interface()
                .await
                .make_websocket_param(&ctx.context, &group, &info.subscribes)
                .await;
            if let Err(e) = ws_param {
                info.retryed += 1;
                log::error!("occur an error for reconnecting : {}", e);
                continue;
            }

            let mut websocket = Websocket::default();
            let recive_callback = ExSharedContext::make_recive_callback(&ctx);
            let connect_result = websocket.connect(ws_param.unwrap(), &recive_callback).await;
            if let Err(e) = connect_result {
                info.retryed += 1;
                log::error!("occur an error for reconnecting : {}", e);
                continue;
            } else {
                drop(info);
                ctx.change_websocket(group.as_str(), websocket).await;
            }
        }

        Ok(())
    }

    pub async fn lock_interface(&self) -> tokio::sync::RwLockWriteGuard<dyn ExWebsocketTrait> {
        self.ctx.lock_interface().await
    }

    pub fn get_exchange_context(&self) -> &ExchangeContextPtr {
        &self.ctx.context
    }

    pub fn get_websocket_param(&self) -> &WebsocketParam {
        &self.ctx.context.param.websocket
    }

    pub fn get_exchange_param(&self) -> &ExchangeParam {
        &self.ctx.context.param
    }

    pub async fn is_connected(&self) -> bool {
        self.ctx.get_connected_cnt().await == self.ctx.get_websocket_cnt().await
    }

    pub async fn insert_websocket(
        &mut self,
        group: String,
        info: ExWebsocketInfo,
    ) -> Option<ExWebsocketInfoRwArc> {
        self.ctx.insert_websocket(group, info).await
    }

    pub async fn find_websocket(&self, group: &str) -> Option<ExWebsocketInfoRwArc> {
        self.ctx.find_websocket(group).await
    }

    pub async fn new<Interface>(
        context: ExchangeContextPtr,
        callback: SubscribeCallback,
    ) -> anyhow::Result<Arc<RwLock<Inner>>>
    where
        Interface: ExWebsocketTrait + Default + 'static,
    {
        let socketcheck = context.param.config.ping_interval.clone();
        let context = ExSharedContext::new::<Interface>(context, Default::default(), callback);
        let recive_callback = ExSharedContext::make_recive_callback(&context);
        let cloned_context = Arc::downgrade(&context);
        tokio::spawn(async move {
            let interval = socketcheck.to_std().unwrap();
            loop {
                tokio::time::sleep(interval).await;

                if let Some(spt) = cloned_context.upgrade() {
                    if let Err(e) = Inner::check_eject(spt).await {
                        log::error!("occur error for check ping : {}", e.to_string());
                    }
                } else {
                    break;
                }
            }
        });

        let exchange = Inner {
            ctx: context,
            subscribes: HashSet::<String>::new(),
            callback: recive_callback,
        };

        Ok(Arc::new(RwLock::new(exchange)))
    }

    async fn make_websocket(
        &mut self,
        group: &String,
        subscribes: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<ExWebsocketInfo> {
        let client_param = self
            .lock_interface()
            .await
            .make_websocket_param(&self.get_exchange_context(), group, &subscribes)
            .await?;
        let mut websocket = Websocket::default();
        websocket.connect(client_param, &self.callback).await?;

        let ex_websocket = ExWebsocketInfo {
            is_authorized: false,
            checkedtime: Utc::now(),
            retryed: 0,
            websocket: websocket,
            subscribes: HashMap::<SubscribeType, Vec<SubscribeParam>>::default(),
        };
        Ok(ex_websocket)
    }

    async fn is_subscribed(&mut self, s: &SubscribeType, param: &SubscribeParam) -> Option<bool> {
        let (_, key) = self
            .lock_interface()
            .await
            .make_group_and_key(s, param)
            .await?;
        Some(self.subscribes.contains(&key))
    }

    async fn subscribe(&mut self, s: SubscribeType, param: SubscribeParam) -> anyhow::Result<()> {
        let (group, key) = self
            .lock_interface()
            .await
            .make_group_and_key(&s, &param)
            .await
            .ok_or(anyhow::anyhow!("This is an unsupported feature."))?;

        if self.subscribes.contains(&key) {
            return Err(anyhow::anyhow!("already subscribed"));
        }

        log::info!(
            "proccessing subscribe :({}){}",
            serde_json::to_string(&s).unwrap(),
            serde_json::to_string(&param.0).unwrap()
        );
        let vec = vec![param];
        let mut p = HashMap::new();
        p.insert(s.clone(), vec);

        let info = if let Some(websocket) = self.find_websocket(&group).await {
            websocket
        } else {
            log::debug!("make new socket for : {}", &group);
            let websocket = self.make_websocket(&group, &p).await?;
            self.insert_websocket(group.clone(), websocket).await;
            let result = self
                .find_websocket(&group)
                .await
                .ok_or(anyhow::anyhow!("occur error that insert client{}", group))?;
            result
        };

        {
            let mut locked = info.write().await;
            let mut cloned_ws = locked.websocket.clone();
            if locked.is_authorized {
                self.lock_interface()
                    .await
                    .subscribe(&self.get_exchange_context(), &mut cloned_ws, &p)
                    .await?;
            }

            log::info!("success subscribe : {:?}", &s);
            if let Some(v) = locked.subscribes.get_mut(&s) {
                if let Some((k, value)) = p.iter_mut().next() {
                    v.extend(value.drain(..));
                }
            } else {
                locked.subscribes.extend(p);
            }
        }

        self.subscribes.insert(key);
        Ok(())
    }
}

pub struct ExWebsocket {
    ptr: Arc<RwLock<Inner>>,
}

impl ExWebsocket {
    pub async fn new<Interface>(
        context: ExchangeContextPtr,
        callback: SubscribeCallback,
    ) -> anyhow::Result<Self>
    where
        Interface: ExWebsocketTrait + Default + 'static,
    {
        let ptr = Inner::new::<Interface>(context, callback).await?;
        Ok(ExWebsocket { ptr })
    }

    pub async fn is_connected(&self) -> bool {
        let locked = self.ptr.read().await;
        locked.is_connected().await
    }

    pub async fn is_subscribed(
        &self,
        s: &SubscribeType,
        param: &SubscribeParam,
    ) -> Option<bool> {
        let mut locked = self.ptr.write().await;
        locked.is_subscribed(s, param).await
    }

    pub async fn subscribe(
        &self,
        s: SubscribeType,
        param: SubscribeParam,
    ) -> anyhow::Result<()> {
        let mut locked = self.ptr.write().await;
        locked.subscribe(s, param).await?;
        Ok(())
    }
}
