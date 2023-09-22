use std::collections::HashSet;
use std::sync::Weak;
use std::{collections::HashMap, sync::Arc};

use anyhow::{Ok, Result};
use async_trait::async_trait;
use bitflags::bitflags;
use chrono::Utc;
use futures::FutureExt;
use reqwest::ClientBuilder;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;

use crate::opk::protocols;

use super::super::{leveldb_str, protocols::*};
use super::ex_websocket::*;
pub struct RequestParam {
    pub method: reqwest::Method,
    pub path: String,
    pub headers: reqwest::header::HeaderMap,
    pub body: serde_json::Value,
}

impl RequestParam {
    pub fn new_mp(method: reqwest::Method, path: &str) -> Self {
        RequestParam {
            method: method,
            path: path.to_string(),
            headers: reqwest::header::HeaderMap::default(),
            body: serde_json::Value::default(),
        }
    }

    pub fn new_mpb(method: reqwest::Method, path: &str, body: serde_json::Value) -> Self {
        RequestParam {
            method: method,
            path: path.to_string(),
            headers: reqwest::header::HeaderMap::default(),
            body: body,
        }
    }

    pub fn new_mphb(
        method: reqwest::Method,
        path: &str,
        headers: reqwest::header::HeaderMap,
        body: serde_json::Value,
    ) -> Self {
        RequestParam {
            method: method,
            path: path.to_string(),
            headers: headers,
            body: body,
        }
    }
}

#[async_trait]
pub trait RestApiTrait: Send + Sync + 'static {
    async fn sign(&self, context: &ExchangeContextPtr, param: RequestParam)
        -> Result<RequestParam>;
    async fn request(
        &self,
        context: &ExchangeContextPtr,
        param: RequestParam,
    ) -> anyhow::Result<(serde_json::Value, PacketTime)>;

    async fn request_market(
        &mut self,
        context: &ExchangeContextPtr,
    ) -> Result<DataSet<Market, MarketKind>>;
    async fn request_wallet(
        &mut self,
        context: &ExchangeContextPtr,
        optional: &str,
    ) -> Result<DataSet<Asset>>;
    async fn request_position(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> Result<HashMap<MarketKind, PositionSet>>;

    async fn request_orderbook(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        quantity: SubscribeQuantity,
    ) -> Result<OrderBook>;

    async fn request_order_submit(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        params: &[OrderParam],
    ) -> Result<OrderResult>;
    async fn request_order_search(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> Result<OrderSet>;
    async fn request_order_opened(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> Result<OrderSet>;
    async fn request_order_cancel(
        &mut self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> Result<OrderResult>;
    async fn request_order_query(
        &mut self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> Result<OrderResult>;

    async fn req_async(
        &self,
        context: &ExchangeContextPtr,
        param: RequestParam,
    ) -> Result<(reqwest::Response, PacketTime)>
    where
        Self: Sized,
    {
        let signed_result = self.sign(&context, param).await?;
        let (fullpath, body) = if signed_result.method == reqwest::Method::GET {
            let urlcode = crate::opk::json::url_encode(&signed_result.body)?;
            (
                format!(
                    "{}{}?{}",
                    context.param.restapi.url, signed_result.path, urlcode
                ),
                String::default(),
            )
        } else {
            (
                format!("{}{}", context.param.restapi.url, signed_result.path),
                signed_result.body.to_string(),
            )
        };

        log::debug!("requesting url({}), body({})", &fullpath, &body);
        let builder = match signed_result.method {
            reqwest::Method::GET => {
                let b = context.requester.get(&fullpath);
                Ok(b)
            }
            reqwest::Method::PUT => {
                let b = context.requester.put(&fullpath).body(body);
                Ok(b)
            }
            reqwest::Method::DELETE => {
                let b = context.requester.delete(&fullpath).body(body);
                Ok(b)
            }
            reqwest::Method::POST => {
                let b = context.requester.post(&fullpath).body(body);
                Ok(b)
            }
            _ => Err(anyhow::anyhow!(
                "invalid method: {}",
                signed_result.method.to_string()
            )),
        }?;

        let mut time = PacketTime::default();
        let res = builder.headers(signed_result.headers).send().await?;
        time.recvtime = Utc::now();

        // let date = res.headers().get("date").unwrap().to_str()?;
        // let datetime = chrono::DateTime::parse_from_rfc2822(date)?;
        // time.proctime = Utc.from_utc_datetime(&datetime.naive_utc());

        if context.param.config.eject < time.laytency() {
            return Err(anyhow::anyhow!(
                "occur eject {}({})",
                fullpath.to_string(),
                time.laytency().to_string()
            ));
        }
        return Ok((res, time));
    }
}

struct Inner {
    context: ExchangeContextPtr,

    restapi: Arc<Mutex<dyn RestApiTrait>>,
    websocket: ExWebsocket,
}

#[derive(Debug, Clone)]
struct CreateFlag {
    flags: Arc<Mutex<u64>>,
}

bitflags! {
    struct ExchangeCreateOpt: u64 {
        const CheckWallet = 0b00000001;
        const CheckAuth = 0b00000010;
        const IsDirty = 0b00000100;

    }
}

impl CreateFlag {
    fn new() -> Self {
        Self {
            flags: Arc::new(Mutex::new(0)),
        }
    }

    pub async fn set_flag(&mut self, flag: ExchangeCreateOpt, value: bool) {
        let mut locked = self.flags.lock().await;
        if value {
            *locked |= flag.bits();
        } else {
            *locked &= !flag.bits();
        }
    }

    pub async fn has_flag(&self, flag: u64) -> bool {
        let locked = self.flags.lock().await;
        *locked & flag == flag
    }
}

impl Inner {
    pub async fn new<ResAPI, Websocket>(
        option: ExchangeParam,
        recorder: leveldb_str::Leveldb,
        builder: Option<ClientBuilder>,
    ) -> anyhow::Result<Arc<RwLock<Self>>>
    where
        Websocket: ExWebsocketTrait + Default + 'static,
        ResAPI: RestApiTrait + Default,
    {
        let client = builder
            .unwrap_or_else(|| {
                reqwest::Client::builder()
                    .pool_max_idle_per_host(10)
                    .pool_idle_timeout(Some(std::time::Duration::from_secs(30)))
                    .http2_prior_knowledge()
                    .gzip(true)
                    .use_rustls_tls()
            })
            .build()?;
        let context = Arc::new(ExchangeContext::new(option, recorder, client));

        let mut restapi = ResAPI::default();
        let markets = restapi.request_market(&context).await?;
        let assets = restapi.request_wallet(&context, "").await?;

        {
            let mut lock = context.storage.markets.write().await;
            *lock = markets;
        }

        let restapi_ptr = Arc::new(Mutex::new(restapi));
        let is_connected = CreateFlag::new();
        let is_connected_2 = is_connected.clone();
        let restapi_wpt = Arc::downgrade(&restapi_ptr);
        let context_wpt = Arc::downgrade(&context);
        let callback = SubscribeCallback::new(move |signal, subsicrebe| {
            let mut cloned_is_connected = is_connected_2.clone();
            let cloned_restapi_wpt = restapi_wpt.clone();
            let cloned_context_wpt = context_wpt.clone();
            async move {
                if let Some(context_ptr) = cloned_context_wpt.upgrade() {
                    match &signal {
                        Signal::Recived(m) => {
                            if let Message::Pong(_) = m {
                                log::debug!(
                                    "signal({:?}) subsribe({})",
                                    &signal,
                                    serde_json::to_string(&subsicrebe).unwrap_or("".to_string())
                                );
                            }
                        }
                        Signal::Opened => {
                            if let Some(restapi) = cloned_restapi_wpt.upgrade() {
                                let mut locked = restapi.lock().await;

                                let ret = match locked.request_wallet(&context_ptr, "").await {
                                    anyhow::Result::Ok(wallet) => {
                                        context_ptr.update(SubscribeResult::Balance(wallet)).await
                                    },
                                    anyhow::Result::Err(e) => {
                                        Err(e)
                                    }
                                };

                                if let Err(e) = ret {
                                    log::error!("cannot initilize because occured error for connecting : {}", e.to_string());
                                    cloned_is_connected.set_flag(ExchangeCreateOpt::IsDirty, true).await;
                                }
                                else {
                                    cloned_is_connected.set_flag(ExchangeCreateOpt::CheckWallet, true).await;
                                }

                            }
                        }
                        Signal::Closed => {
                            let mut locked = context_ptr.storage.positions.write().await;
                            *locked = HashMap::<MarketKind, PositionSet>::default();
                            drop(locked);

                            let mut locked = context_ptr.storage.assets.write().await;
                            *locked = DataSet::<Asset>::new(
                                PacketTime::from(&Utc::now()),
                                UpdateType::Snapshot,
                            );
                            drop(locked);
                        }
                        _ => {}
                    }

                    if let SubscribeResult::Authorized(success) = &subsicrebe {
                        if *success {
                            cloned_is_connected
                                .set_flag(ExchangeCreateOpt::CheckAuth, true)
                                .await;
                        } else {
                            log::error!("cannot initilize because failed authorize for connecting.");
                            cloned_is_connected
                                .set_flag(ExchangeCreateOpt::IsDirty, true)
                                .await;
                        }
                    }

                    if let Err(e) = context_ptr.update(subsicrebe).await {
                        log::error!("occur error for updating : {}", e.to_string());
                    };
                }
            }
            .boxed()
        });

        let st = SubscribeType::Balance;
        let sp = SubscribeType::balance_param().collect();
        let ws = ExWebsocket::new::<Websocket>(context.clone(), callback).await?;
        if let Some(is_subscribed) = ws.is_subscribed(&st, &sp).await {
            ws.subscribe(st, sp).await?;
            loop {
                if is_connected
                    .has_flag(ExchangeCreateOpt::IsDirty.bits())
                    .await
                {
                    return Err(anyhow::anyhow!(
                        "invalid auth or wallet for connecting websockets"
                    ));
                } else if is_connected
                    .has_flag(
                        ExchangeCreateOpt::CheckWallet.bits() | ExchangeCreateOpt::CheckAuth.bits(),
                    )
                    .await
                {
                    log::debug!("success that connect websocket : url({}), check-wallet(true)  check-auth(true)", context.param.websocket.url);
                    break;
                }

                log::debug!(
                    "waiting for creating websocket : url({}), , check-wallet({})  check-auth({})",
                    context.param.websocket.url,
                    is_connected
                        .has_flag(ExchangeCreateOpt::CheckWallet.bits())
                        .await,
                    is_connected
                        .has_flag(ExchangeCreateOpt::CheckAuth.bits())
                        .await
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }

        let ret = Arc::new(RwLock::new(Inner {
            context: context,
            restapi: restapi_ptr,
            websocket: ws,
        }));

        Ok(ret)
    }

    pub async fn get_markets(&self) -> Vec<protocols::MarketKind> {
        let lock = self.get_storage().markets.read().await;
        lock.get_datas()
            .values()
            .map(|v| &v.kind)
            .collect::<HashSet<_>>()
            .iter()
            .map(|k| (**k).clone())
            .collect::<Vec<_>>()
    }

    pub async fn find_market(&self, kind: &MarketKind) -> Option<MarketPtr> {
        let lock = self.get_storage().markets.read().await;
        lock.get_datas().get(kind).cloned()
    }

    pub fn get_option(&self) -> &ExchangeParam {
        &self.context.param
    }

    fn get_storage(&self) -> &ExchangeStorage {
        &self.context.storage
    }

    pub async fn request_wallet(&self, optional: &str) -> Result<DataSet<Asset>> {
        let stype = SubscribeType::Balance;
        let sparam = SubscribeType::balance_param().collect();

        let (is_subscribed, need_subscribe) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&stype, &sparam).await {
                (
                    self.websocket.is_connected().await && is_subscribed,
                    !is_subscribed,
                )
            } else {
                (false, false)
            };
        let ret = if is_subscribed {
            let locked = self.get_storage().assets.read().await;
            log::debug!(
                "imported asset from storage : date({}), laytency({})",
                locked.get_packet_time().recvtime.to_string(),
                locked.get_packet_time().laytency().to_string()
            );
            locked.clone()
        } else {
            let ret = self
                .restapi
                .lock()
                .await
                .request_wallet(&self.context, optional)
                .await?;
            ret
        };

        if need_subscribe {
            self.websocket.subscribe(stype, sparam).await?;
        }

        Ok(ret)
    }

    pub async fn request_position(
        &self,
        market: &MarketPtr,
    ) -> Result<HashMap<MarketKind, PositionSet>> {
        let stype = SubscribeType::Position;
        let sparam = SubscribeType::position_param()
            .market(market.clone())
            .collect();
        let (is_subscribed, need_subscribe, is_support) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&stype, &sparam).await {
                (
                    self.websocket.is_connected().await && is_subscribed,
                    !is_subscribed,
                    true,
                )
            } else {
                (false, false, false)
            };

        let imported_ret = if is_subscribed {
            let locked = self.get_storage().positions.read().await;
            if let Some(value) = locked.get(&market.kind) {
                log::debug!(
                    "imported position from storage : date({}), laytency({})",
                    value.get_packet_time().recvtime.to_string(),
                    value.get_packet_time().laytency().to_string()
                );
                anyhow::Result::Ok(locked.clone())
            } else {
                log::debug!(
                    "imported position from storage but cannot find market({}).",
                    serde_json::to_string(&market.kind).unwrap_or(String::default())
                );

                anyhow::Result::Err(locked.clone())
            }
        } else {
            anyhow::Result::Err(HashMap::<MarketKind, PositionSet>::new())
        };

        let ret = match imported_ret {
            anyhow::Result::Ok(imported) => imported,
            anyhow::Result::Err(mut imported) => {
                let mut ret = self
                    .restapi
                    .lock()
                    .await
                    .request_position(&self.context, market)
                    .await?;

                if is_support {
                    if !ret.contains_key(&market.kind) {
                        ret.insert(
                            market.kind.clone(),
                            PositionSet::new(
                                PacketTime::from(&Utc::now()),
                                MarketVal::Pointer(market.clone()),
                            ),
                        );
                    }

                    self.context
                        .update(SubscribeResult::Position(ret.clone()))
                        .await?;
                    log::debug!(
                        "synchronized position from requested data to storage : market({})",
                        serde_json::to_string(&market.kind).unwrap_or(String::default())
                    );
                }

                imported.extend(ret);
                imported
            }
        };

        if need_subscribe {
            self.websocket.subscribe(stype, sparam).await?;
        }

        Ok(ret)
    }

    pub async fn request_orderbook(
        &self,
        market: &MarketPtr,
        quantity: SubscribeQuantity,
    ) -> Result<OrderBookPtr> {
        let stype = SubscribeType::Orderbook;
        let sparam = SubscribeType::orderbook_param()
            .market(market.clone())
            .quantity(quantity.clone())
            .speed(SubscribeSpeed::Fastest)
            .collect();

        let (is_subscribed, need_subscribe, is_support) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&stype, &sparam).await {
                (
                    self.websocket.is_connected().await && is_subscribed,
                    !is_subscribed,
                    true,
                )
            } else {
                (false, false, false)
            };

        let ret = if is_subscribed {
            let locked = self.context.storage.orderbook.read().await;
            locked.get(&market.kind).cloned()
        } else {
            None
        };

        let ptr = if let Some(ptr) = ret {
            log::debug!(
                "imported orderbook from storage : date({}), laytency({})",
                ptr.ptime.recvtime.to_string(),
                ptr.get_packet_time().laytency().to_string()
            );
            ptr
        } else {
            let data = self
                .restapi
                .lock()
                .await
                .request_orderbook(&self.context, market, quantity)
                .await?;
            Arc::new(data)
        };

        if need_subscribe {
            self.websocket.subscribe(stype, sparam).await?;
        }
        Ok(ptr)
    }
    pub async fn request_order_submit(
        &self,
        market: &MarketPtr,
        params: &[OrderParam],
    ) -> Result<OrderResult> {
        if params.is_empty() {
            return Err(anyhow::anyhow!("param is empty"));
        }

        let stype = SubscribeType::Order;
        let sparam = SubscribeType::order_param()
            .market(market.clone())
            .collect();
        if !self
            .websocket
            .is_subscribed(&stype, &sparam)
            .await
            .unwrap_or(true)
        {
            self.websocket.subscribe(stype, sparam).await?;
        }

        let mut api = self.restapi.lock().await;
        let ret = api
            .request_order_submit(&self.context, market, params)
            .await?;
        Ok(ret)
    }

    pub async fn request_order_search(
        &self,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> Result<OrderSet> {
        let stype = SubscribeType::Order;
        let sparam = SubscribeType::order_param()
            .market(market.clone())
            .collect();
        if !self
            .websocket
            .is_subscribed(&stype, &sparam)
            .await
            .unwrap_or(true)
        {
            self.websocket.subscribe(stype, sparam).await?;
        }

        let mut api = self.restapi.lock().await;
        let ret = api
            .request_order_search(&self.context, market, param)
            .await?;
        Ok(ret)
    }

    pub async fn request_order_opened(&self, market: &MarketPtr) -> Result<OrderSet> {
        let stype = SubscribeType::Order;
        let sparam = SubscribeType::order_param()
            .market(market.clone())
            .collect();
        if !self
            .websocket
            .is_subscribed(&stype, &sparam)
            .await
            .unwrap_or(true)
        {
            self.websocket.subscribe(stype, sparam).await?;
        }

        let mut api = self.restapi.lock().await;
        let ret = api.request_order_opened(&self.context, market).await?;
        Ok(ret)
    }

    pub async fn request_order_cancel(&self, params: &OrderSet) -> Result<OrderResult> {
        let market = params
            .market_ptr()
            .ok_or(anyhow::anyhow!("the market of order must be point type"))
            .cloned()?;
        let stype = SubscribeType::Order;
        let sparam = SubscribeType::order_param()
            .market(market.clone())
            .collect();
        if !self
            .websocket
            .is_subscribed(&stype, &sparam)
            .await
            .unwrap_or(true)
        {
            self.websocket.subscribe(stype, sparam).await?;
        }

        let (mut restapi_param, already_synced) =
            params.filter_new(|(key, value)| value.state.cancelable());
        if restapi_param.get_datas().is_empty() {
            for (key, value) in already_synced {
                restapi_param.insert_with_key(key, value);
            }
            return Ok(OrderResult::new_with_orders(restapi_param));
        }

        let mut api = self.restapi.lock().await;
        let mut ret = api
            .request_order_cancel(&self.context, &restapi_param)
            .await?;

        for (key, value) in already_synced {
            ret.success.insert_with_key(key, value);
        }

        Ok(ret)
    }

    pub async fn request_order_query(&self, params: &OrderSet) -> Result<OrderResult> {
        // order subscribe 호출
        let market = params
            .market_ptr()
            .cloned()
            .ok_or(anyhow::anyhow!("the market of order must be point type"))?;
        let stype = SubscribeType::Order;
        let sparam = SubscribeType::order_param()
            .market(market.clone())
            .collect();

        let (is_subscribed, need_subscribe, is_support) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&stype, &sparam).await {
                (
                    self.websocket.is_connected().await && is_subscribed,
                    !is_subscribed,
                    true,
                )
            } else {
                (false, false, false)
            };

        let (mut restapi_param, mut already_synced) =
            params.filter_new(|(key, value)| value.state.synchronizable());

        // 유효기간 지나지 않은 데이터 그리고 sync를 할 필요가 없는 데이터만 웹소켓 데이터를 사용한다.
        if is_subscribed {
            let now = Utc::now();
            let mut remover = Vec::<String>::default();
            for (oid, order) in restapi_param.get_datas() {
                let result = self.context.find_order(&oid, &market.kind).await?;
                if let Some(order) = result {
                    remover.push(oid.clone());
                    already_synced.insert(oid.clone(), order.clone());
                }
            }

            for key in remover {
                restapi_param.remove(&key);
            }
        }

        // 동기화 할 것이 없다면 반환
        let mut ret = if restapi_param.get_datas().is_empty() {
            OrderResult::new_with_orders(restapi_param)
        } else {
            let mut api = self.restapi.lock().await;
            let ret = api
                .request_order_query(&self.context, &restapi_param)
                .await?;

            if is_support {
                let sb = HashMap::<MarketKind, OrderSet>::from([(
                    market.kind.clone(),
                    ret.success.clone(),
                )]);
                self.context.update(SubscribeResult::Order(sb)).await?;
            }

            ret
        };

        for (key, value) in already_synced {
            ret.success.insert_with_key(key, value);
        }

        // Ordering은 아직 query가 되지 않는다면 Ordering으로 변환
        // Canceling은 아직 Canceled가 되지 않았다면 Canceling으로 변환
        // 이후는 restapi 결과 값을 그대로 받는다.
        let proccessing = params
            .get_datas()
            .iter()
            .filter(|(key, value)| {
                value.state.is_ordering_or_canceling()
                    && !value
                        .state
                        .is_expired(&self.get_option().config.state_expired_duration)
            })
            .collect::<HashMap<_, _>>();

        for (key, value) in proccessing {
            match &value.state {
                OrderState::Canceling(created) => {
                    if let Some(ptr) = ret.success.get_datas_mut().get_mut(&value.oid) {
                        if !ptr.state.is_cancelled() {
                            let r = Arc::get_mut(ptr).unwrap();
                            r.state = value.state.clone();
                        }
                    }
                }
                OrderState::Ordering(created) => {
                    if ret.errors.contains_key(key) {
                        ret.success.insert(value.clone());
                        ret.errors.remove(key);
                    }
                }
                _ => {}
            }
        }

        if need_subscribe {
            self.websocket.subscribe(stype, sparam).await?;
        }
        Ok(ret)
    }
}

#[derive(Clone)]
pub struct Exchange {
    ptr: Arc<RwLock<Inner>>,
}

#[derive(Clone)]
pub struct ExchangeWeak {
    ptr: Weak<RwLock<Inner>>,
}

impl ExchangeWeak {
    pub fn origin(&self) -> Option<Exchange> {
        let ptr = self.ptr.upgrade()?;
        Some(Exchange { ptr: ptr })
    }
}

impl Exchange {
    pub fn weak(&self) -> ExchangeWeak {
        let wpt = Arc::downgrade(&self.ptr);
        ExchangeWeak { ptr: wpt }
    }

    pub async fn new<ResAPI, Websocket>(
        option: ExchangeParam,
        recorder: leveldb_str::Leveldb,
        builder: Option<ClientBuilder>,
    ) -> anyhow::Result<Self>
    where
        Websocket: ExWebsocketTrait + Default + 'static,
        ResAPI: RestApiTrait + Default,
    {
        let ptr = Inner::new::<ResAPI, Websocket>(option, recorder, builder).await?;
        Ok(Exchange { ptr: ptr })
    }

    pub async fn request_wallet(&self, optional: &str) -> Result<DataSet<Asset>> {
        let locked = self.ptr.read().await;
        locked.request_wallet(optional).await
    }

    pub async fn request_position(
        &self,
        market: &MarketPtr,
    ) -> Result<HashMap<MarketKind, PositionSet>> {
        let locked = self.ptr.read().await;
        locked.request_position(market).await
    }

    pub async fn request_orderbook(
        &self,
        market: &MarketPtr,
        quantity: SubscribeQuantity,
    ) -> Result<OrderBookPtr> {
        let locked = self.ptr.read().await;
        locked.request_orderbook(market, quantity).await
    }

    pub async fn request_order_submit(
        &self,
        market: &MarketPtr,
        params: &[OrderParam],
    ) -> Result<OrderResult> {
        let locked = self.ptr.read().await;
        locked.request_order_submit(market, params).await
    }

    pub async fn request_order_search(
        &self,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> Result<OrderSet> {
        let locked = self.ptr.read().await;
        locked.request_order_search(market, param).await
    }

    pub async fn request_order_opened(&self, market: &MarketPtr) -> Result<OrderSet> {
        let locked = self.ptr.read().await;
        locked.request_order_opened(market).await
    }

    pub async fn request_order_cancel(&self, params: &OrderSet) -> Result<OrderResult> {
        let locked = self.ptr.read().await;
        locked.request_order_cancel(params).await
    }

    pub async fn request_order_query(&self, params: &OrderSet) -> Result<OrderResult> {
        let locked = self.ptr.read().await;
        locked.request_order_query(params).await
    }

    pub async fn get_markets(&self) -> Vec<protocols::MarketKind> {
        let locked: tokio::sync::RwLockReadGuard<'_, Inner> = self.ptr.read().await;
        locked.get_markets().await
    }

    pub async fn find_market(&self, kind: &MarketKind) -> Option<MarketPtr> {
        let locked = self.ptr.read().await;
        locked.find_market(kind).await
    }

    pub async fn get_context(&self) -> Arc<ExchangeContext> {
        let locked = self.ptr.read().await;
        locked.context.clone()
    }
}
