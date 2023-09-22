use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::RwLock;

use crate::opk::{
    exchange::Exchange,
    float::LazyDecimal,
    protocols::*,
    webserver::{
        packet::{Packet, Protocol},
        service::*,
    },
    websocket::Websocket,
};

#[derive(Serialize, Deserialize)]
pub struct ManualParam {
    targets: Vec<String>,
    otype: String,
    side: String,
    price: String,
    amount: String,
    postonly: bool,
    close: bool,
}

#[derive(Serialize, Deserialize)]
enum Params {
    Open((String, String)),
    Subscribe((String, MarketKind, u64)),
    OrderSubmit(ManualParam),
    OrderCancel((String, Arc<Order>)),
    Refresh(Vec<String>),
}

struct LocalVal {
    tag: String,
    exchange: Exchange,
    subscribe: Option<(MarketPtr, std::time::Duration)>,
}

impl LocalVal {
    pub fn new(tag: String, exchange: Exchange) -> Self {
        LocalVal {
            tag: tag,
            exchange: exchange,
            subscribe: None,
        }
    }
}

#[derive(Default)]
pub struct Manual {
    id: String,
    socket: Websocket,
    groups: HashMap<String, RwArc<LocalVal>>,
}

impl Manual {
    pub async fn req_position(
        &self,
        context: &Arc<Context>,
        exchange: &Exchange,
        market: &MarketPtr,
    ) -> anyhow::Result<serde_json::Value> {
        exchange
            .request_position(&market)
            .and_then(|positions| async move {
                let value = positions.iter().map(|(_, value)| value).collect::<Vec<_>>();
                serde_json::to_value(&value).map_err(anyhow::Error::from)
            })
            .await
    }

    pub async fn req_wallet(
        &self,
        context: &Arc<Context>,
        exchange: &Exchange,
    ) -> anyhow::Result<serde_json::Value> {
        exchange
            .request_wallet("")
            .and_then(
                |wallet| async move { serde_json::to_value(&wallet).map_err(anyhow::Error::from) },
            )
            .await
    }

    pub async fn req_histories(
        &self,
        context: &Arc<Context>,
        exchange: &Exchange,
        market: &MarketPtr,
    ) -> anyhow::Result<serde_json::Value> {
        exchange
            .request_order_search(&market, &OrdSerachParam::default())
            .and_then(|os| async move { serde_json::to_value(&os).map_err(anyhow::Error::from) })
            .await
    }

    pub async fn req_openeds(
        &self,
        context: &Arc<Context>,
        exchange: &Exchange,
        market: &MarketPtr,
    ) -> anyhow::Result<serde_json::Value> {
        let value = exchange
            .request_order_opened(&market)
            .and_then(|os| async move { serde_json::to_value(&os).map_err(anyhow::Error::from) })
            .await?;

        Ok(value)
    }

    pub async fn on_proc_refresh(
        &mut self,
        context: &Arc<Context>,
        targets: Vec<String>,
    ) -> anyhow::Result<serde_json::Value> {
        let mut res = Map::<String, serde_json::Value>::default();
        for target in targets {
            let group = self
                .groups
                .get(&target)
                .ok_or(anyhow!("cannot find opened exchange : {}", target))?;

            let (exchagne, tag, market) = {
                let locked = group.read().await;
                if let Some((market, _)) = &locked.subscribe {
                    Ok((locked.exchange.clone(), locked.tag.clone(), market.clone()))
                } else {
                    Err(anyhow!(
                        "cannot find saved market information from {}",
                        target
                    ))
                }
            }?;

            let openeds = self.req_openeds(context, &exchagne, &market).await?;
            let wallet = self.req_wallet(context, &exchagne).await?;
            let histories = self.req_histories(context, &exchagne, &market).await?;
            let value = if let MarketKind::Spot(_) = market.kind {
                serde_json::json!({
                    "wallet" : wallet,
                    "openeds" : openeds,
                    "histories" : histories,
                })
            } else {
                serde_json::json!({
                    "positions" : self.req_position(context, &exchagne, &market).await?,
                    "wallet" : wallet,
                    "openeds" : openeds,
                    "histories" : histories,
                })
            };
            res.insert(tag, value);
        }

        Ok(serde_json::Value::Object(res))
    }

    pub async fn on_proc_order_close(
        &mut self,
        context: &Arc<Context>,
        target: String,
        order: OrderPtr,
    ) -> anyhow::Result<serde_json::Value> {
        let orders = async {
            let group = self
                .groups
                .get(&target)
                .ok_or(anyhow!("cannot find opened exchange : {}", target))?;

            let locked = group.read().await;
            let market = if let Some((market, _)) = &locked.subscribe {
                Ok(market.clone())
            } else {
                Err(anyhow!("invalid market, please check market is valid"))
            }?;

            let params = DataSetWithMarket::<Order, String>::new(
                PacketTime::default(),
                MarketVal::Pointer(market),
            );

            let result = locked.exchange.request_order_cancel(&params).await?;
            let value = serde_json::to_value(result)?;
            let orders = vec![value];
            anyhow::Result::<_, anyhow::Error>::Ok(serde_json::Value::Array(orders))
        }
        .await?;

        let targets = vec![target.clone()];
        let res = {
            let mut res = self.on_proc_refresh(context, targets).await?;
            let object = &mut res[&target].as_object_mut().unwrap();
            object.insert("orders".into(), orders);
            res
        };

        Ok(res)
    }

    pub async fn on_proc_order_submit(
        &mut self,
        context: &Arc<Context>,
        param: ManualParam,
    ) -> anyhow::Result<serde_json::Value> {
        let mut orders = HashMap::<String, OrderResult>::default();
        for target in &param.targets {
            let group = self
                .groups
                .get(target)
                .cloned()
                .ok_or(anyhow!("cannot find opened exchange : {}", target))?;

            let side = if param.side == "BUY" {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };

            let locked = group.read().await;
            let (market, _) = locked
                .subscribe
                .clone()
                .ok_or(anyhow!("please. choice market"))?;

            let orderbook = locked
                .exchange
                .request_orderbook(&market, SubscribeQuantity::Least("5".into()))
                .await?;
            let quotes = if side.is_buy() {
                &orderbook.bid
            } else {
                &orderbook.ask
            };

            let mut price = if param.price == "best" {
                if quotes.len() > 0 {
                    quotes[0].price.clone()
                } else {
                    let desc = if side.is_buy() { "buy" } else { "sell" };
                    return Err(anyhow!("{} is empty", desc));
                }
            } else {
                LazyDecimal::Str(param.price.clone())
            };

            let mut amount = if param.amount == "best" {
                if quotes.len() > 0 {
                    quotes[0].amount.clone()
                } else {
                    let desc = if side.is_buy() { "buy" } else { "sell" };
                    return Err(anyhow!("{} is empty", desc));
                }
            } else {
                LazyDecimal::Str(param.amount.clone())
            };

            let oparam = OrderParam {
                kind: OrderKind::Limit,
                price: price.transf_num().clone(),
                amount: amount.transf_num().clone(),
                side: side,
                is_postonly: param.postonly,
                is_reduce: param.close,
                cid: Default::default(),
            };

            let result = locked
                .exchange
                .request_order_submit(&market, &vec![oparam])
                .await?;

            orders.insert(locked.tag.clone(), result);
        }

        let res = {
            let mut res = self.on_proc_refresh(context, param.targets).await?;
            for (tag, result) in orders {
                let item = serde_json::to_value(result)?;
                let root = res[tag].as_object_mut().unwrap();
                root.insert("orders".into(), item);
            }
            res
        };

        Ok(res)
    }

    pub async fn on_proc_subscribe(
        &mut self,
        context: &Arc<Context>,
        target: String,
        kind: MarketKind,
        milli: u64,
    ) -> anyhow::Result<serde_json::Value> {
        let (group, tag) = async {
            let group = self
                .groups
                .get(&target)
                .ok_or(anyhow!("cannot find opened cetegory : {}", target))?;
            let mut locked = group.write().await;
            let market = locked
                .exchange
                .find_market(&kind)
                .await
                .ok_or(anyhow!("cannot find market : {:?}", kind))?;
            if let Some((market, _)) = &locked.subscribe {
                // needs unsubscribe
                log::warn!("needs unsubscribe in manual")
            }

            locked.subscribe = Some((market.clone(), std::time::Duration::from_millis(milli)));
            Ok::<_, anyhow::Error>((group.clone(), locked.tag.clone()))
        }
        .await?;

        let targets = vec![target];
        let res = self.on_proc_refresh(context, targets).await?;

        let cloned_tag = tag.clone();
        let cloned_protocol = Protocol::Subscribed(self.id.clone());
        let cloned_ws = self.socket.clone();
        let current_weak = Arc::downgrade(&group);
        tokio::spawn(async move {
            let mut last = Utc::now();

            loop {
                let tag = &cloned_tag;
                let protocol = cloned_protocol.clone();
                let ws = cloned_ws.clone();
                let ptr = if let Some(ptr) = current_weak.upgrade() {
                    ptr
                } else {
                    break;
                };

                let reader = ptr.read().await;
                if let Some((market, duration)) = &reader.subscribe {
                    let test = reader
                        .exchange
                        .request_orderbook(market, SubscribeQuantity::Least("5".into()))
                        .and_then(|ob| async move {
                            if last != ob.updated {
                                last = ob.updated.clone();
                                let value = json!({ "orderbook": ob });
                                let packet = Packet::res(protocol, json!({ tag: value }), None);
                                let txt = serde_json::to_string(&packet)?;
                                ws.send_text(txt).await
                            } else {
                                Ok(())
                            }
                        })
                        .await;
                    if let Err(e) = test {
                        log::error!("occur Error that reason : {}", e.to_string());
                    }
                    tokio::time::sleep(duration.clone()).await;
                } else {
                    break;
                }
            } // loop
        });

        Ok(res)
    }

    pub async fn on_proc_open(
        &mut self,
        context: &Arc<Context>,
        target: String,
        tag: String,
    ) -> anyhow::Result<serde_json::Value> {
        let res = if tag.is_empty() {
            self.groups.remove(&target);
            serde_json::json!({
                "wallet" : DataSet::<Asset>::default(),
                "markets" : Vec::<MarketKind>::default()
            })
        } else {
            let exchange = context.exchanges.instant(&tag, Default::default()).await?;
            let markets = exchange.get_markets().await;
            let wallet = self.req_wallet(context, &exchange).await?;
            let old = self.groups.insert(
                target.clone(),
                Arc::new(RwLock::new(LocalVal::new(tag.clone(), exchange))),
            );
            serde_json::json!({
                "wallet" : wallet,
                "markets" : markets
            })
        };

        Ok(json!({ tag: res }))
    }
}

#[async_trait]
impl ServiceTrait for Manual {
    async fn open(&mut self, id: &str, ws: Websocket) -> anyhow::Result<()> {
        self.id = id.to_string();
        self.socket = ws;
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
        let params = serde_json::from_value::<Params>(json)?;
        match params {
            Params::Open((target, tag)) => self.on_proc_open(context, target, tag).await,
            Params::Subscribe((target, kind, milli)) => {
                self.on_proc_subscribe(context, target, kind, milli).await
            }
            Params::Refresh(targets) => self.on_proc_refresh(context, targets).await,
            Params::OrderSubmit(params) => self.on_proc_order_submit(context, params).await,
            Params::OrderCancel((target, oid)) => {
                self.on_proc_order_close(context, target, oid).await
            }
        }
    }
}
