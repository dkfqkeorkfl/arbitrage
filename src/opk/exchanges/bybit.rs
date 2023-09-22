use std::collections::HashMap;

use std::sync::Arc;

use crate::opk;
use crate::opk::exchange::*;
use crate::opk::float::to_decimal_with_json;
use crate::opk::float::LazyDecimal;
use crate::opk::websocket::*;
use anyhow::anyhow;
use anyhow::{Ok, Result};
use async_trait::async_trait;
use chrono::TimeZone;
use chrono::Utc;
use futures::future;
use opk::protocols::*;
use rust_decimal::Decimal;
use serde_json::json;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

#[derive(Default, Debug)]
pub struct RestAPI;

impl RestAPI {
    pub fn orderstate_tostring(s: &OrderState) -> &'static str {
        match s {
            OrderState::Ordering(_) => "Created",
            OrderState::Opened => "New",
            OrderState::Canceling(_) => "PendingCancel",
            OrderState::PartiallyFilled => "PartiallyFilled",
            OrderState::Filled => "Filled",
            OrderState::Rejected => "Rejected",
            OrderState::Cancelled => "Cancelled",
        }
    }
    pub fn parse_order(ptime: PacketTime, json: &mut serde_json::Value) -> Result<OrderPtr> {
        let symbol = json["symbol"].as_str().ok_or(anyhow!("invalid symbol"))?;
        let price = to_decimal_with_json(&json["price"])?;
        let amount = to_decimal_with_json(&json["qty"])?;
        let created = json["createdTime"]
            .as_str()
            .map(|s| s.parse::<i64>().unwrap())
            .unwrap();
        let updated = json["updatedTime"]
            .as_str()
            .map(|s| s.parse::<i64>().unwrap())
            .unwrap();

        let aproc = to_decimal_with_json(&json["cumExecQty"]).unwrap_or(Decimal::ZERO);
        let (avg, procceed) = if aproc != Decimal::ZERO {
            let vproc = to_decimal_with_json(&json["cumExecValue"])?;
            if symbol.ends_with("USD") {
                (aproc / vproc, CurrencyPair::new_quote(aproc))
            } else {
                (vproc / aproc, CurrencyPair::new_base(aproc))
            }
        } else {
            (Decimal::ZERO, CurrencyPair::default())
        };

        let state = match json["orderStatus"].as_str().unwrap() {
            "Created" => OrderState::Ordering(Utc::now()),
            "New" => OrderState::Opened,
            "PendingCancel" => OrderState::Canceling(Utc::now()),
            "PartiallyFilled" => OrderState::PartiallyFilled,
            "Filled" => OrderState::Filled,
            "Cancelled" => OrderState::Cancelled,
            _ => OrderState::Rejected,
        };

        let oid = json["orderId"].as_str().ok_or(anyhow!("oid is invalid"))?;
        let cid = json["orderLinkId"]
            .as_str()
            .ok_or(anyhow!("cid is invalid"))?;
        let order = Order {
            ptime: ptime,
            oid: oid.to_string(),
            cid: cid.to_string(),

            kind: OrderKind::Limit,
            price: price,
            amount: amount,
            side: if let Some(side) = json["side"].as_str().filter(|s| *s == "Buy") {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },

            fee: CurrencyPair::default(),
            state: state,
            avg: avg,
            procceed: procceed,
            is_postonly: json["postOnly"].as_bool().unwrap_or(false),
            is_reduce: json["reduceOnly"].as_bool().unwrap_or(false),
            created: Utc.timestamp_millis_opt(created).unwrap(),
            updated: Utc.timestamp_millis_opt(updated).unwrap(),

            detail: json.take(),
        };

        Ok(Arc::new(order))
    }
}

#[async_trait]
impl RestApiTrait for RestAPI {
    async fn request_order_cancel(
        &mut self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> Result<OrderResult> {
        let before = Utc::now();
        let mut bodies = future::join_all(params.get_datas().iter().map(|pr| {
            let body = json!({
                "symbol": params.get_market().symbol(),
                "order_id": pr.1.oid,
            });

            let readable = &(*self);
            async move {
                let res = readable
                    .request(
                        context,
                        RequestParam::new_mpb(
                            reqwest::Method::POST,
                            "/contract/v3/private/order/cancel",
                            body,
                        ),
                    )
                    .await?;
                Ok(res)
            }
        }))
        .await;

        let mut ret = OrderResult::new(PacketTime::from(&before), params.get_market().clone());
        params
            .get_datas()
            .iter()
            .enumerate()
            .for_each(|(i, param)| match bodies.get_mut(i).unwrap() {
                std::result::Result::Ok((value, ptime)) => {
                    let mut order = param.1.as_ref().clone();
                    order.ptime = std::mem::replace(ptime, Default::default());
                    order.state = OrderState::Canceling(Utc::now());
                    order.detail = value.take();

                    ret.success.insert_raw(order);
                }
                Err(e) => {
                    let error = anyhow::anyhow!(e.to_string());
                    ret.errors.insert(param.0.clone(), error);
                }
            });

        Ok(ret)
    }

    async fn request_order_submit(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        params: &[OrderParam],
    ) -> Result<OrderResult> {
        let before = Utc::now();
        let mut bodies = future::join_all(params.iter().map(|param| {
            let mut body = json!({
                "orderType":"Limit",
                "symbol":market.kind.symbol(),
                "price":param.price.to_string(),
                "qty":param.amount.to_string(),
                "timeInForce": if param.is_postonly {
                    "PostOnly"
                } else {
                    "GoodTillCancel"
                },
            });

            if !param.cid.is_empty() {
                body["clientId"] = serde_json::Value::from(param.cid.clone());
            }

            body["side"] =
                serde_json::Value::from(if param.side.is_buy() { "Buy" } else { "Sell" });
            body["positionIdx"] = serde_json::Value::from(0);

            let readable = &(*self);
            async move {
                let res = readable
                    .request(
                        context,
                        RequestParam::new_mpb(
                            reqwest::Method::POST,
                            "/contract/v3/private/order/create",
                            body,
                        ),
                    )
                    .await?;
                Ok(res)
            }
        }))
        .await;

        let mut ret = OrderResult::new(
            PacketTime::from(&before),
            MarketVal::Pointer(market.clone()),
        );
        params
            .iter()
            .enumerate()
            .for_each(|(i, param)| match bodies.get_mut(i).unwrap() {
                std::result::Result::Ok((value, ptime)) => {
                    let result = value["result"].as_object().unwrap();
                    let oid = result["orderId"].as_str().unwrap().to_string();

                    let mut order = Order::from_order_param(param);
                    order.oid = oid;
                    order.state = OrderState::Ordering(Utc::now());
                    order.detail = value.take();
                    order.ptime = std::mem::replace(ptime, Default::default());

                    ret.success.insert_raw(order);
                }
                Err(e) => {
                    let id = i.to_string();
                    let error = anyhow::anyhow!(e.to_string());
                    ret.errors.insert(id, error);
                }
            });

        Ok(ret)
    }

    async fn request_order_query(
        &mut self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> Result<OrderResult> {
        let before = Utc::now();
        let mut bodies = future::join_all(params.get_datas().iter().map(|param| {
            let body = json!({
                "orderId" : param.1.oid.clone()
            });

            let readable = &(*self);
            async move {
                let res = readable
                    .request(
                        context,
                        RequestParam::new_mpb(
                            reqwest::Method::GET,
                            "/contract/v3/private/order/list",
                            body,
                        ),
                    )
                    .await?;
                Ok(res)
            }
        }))
        .await;

        let mut ret = OrderResult::new(PacketTime::from(&before), params.get_market().clone());
        params
            .get_datas()
            .iter()
            .enumerate()
            .for_each(|(i, (oid, param))| match bodies.get_mut(i).unwrap() {
                std::result::Result::Ok((value, ptime)) => {
                    let result = value["result"].as_object_mut().unwrap();
                    for item in result["list"].as_array_mut().unwrap() {
                        match RestAPI::parse_order(ptime.clone(), item) {
                            std::result::Result::Ok(order) => {
                                ret.success.insert(order);
                            }
                            Err(e) => {
                                ret.errors.insert(oid.clone(), e);
                            }
                        };
                    }
                }
                Err(e) => {
                    let error = anyhow::anyhow!(e.to_string());
                    ret.errors.insert(oid.clone(), error);
                }
            });

        Ok(ret)
    }

    async fn request_order_opened(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> Result<OrderSet> {
        let body = json!({
            "symbol" : market.kind.symbol(),
            "limit" : 50
        });

        let mut pr = self
            .request(
                context,
                RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/contract/v3/private/order/unfilled-orders",
                    body,
                ),
            )
            .await?;

        let result = pr.0["result"].as_object_mut().unwrap();
        let nxt = result["nextPageCursor"].as_str().unwrap();
        let mut ret = OrderSet::new_with_cursor(
            pr.1,
            MarketVal::Pointer(market.clone()),
            CursorType::PrevNxt(String::default(), nxt.to_string()),
        );

        for item in result["list"].as_array_mut().unwrap() {
            let order = RestAPI::parse_order(ret.get_packet_time().clone(), item)?;
            ret.insert(order);
        }

        Ok(ret)
    }

    async fn request_order_search(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> Result<OrderSet> {
        let mut body = json!({
            "symbol" : market.kind.symbol(),
            "limit" : 50
        });

        if let Some(s) = &param.state {
            body["orderStatus"] = serde_json::Value::from(RestAPI::orderstate_tostring(s));
        }

        if let Some((nxt, _)) = &param.page {
            body["cursor"] = serde_json::Value::from(nxt.as_str());
        }

        let mut pr = self
            .request(
                context,
                RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/contract/v3/private/order/list",
                    body,
                ),
            )
            .await?;

        let result = pr.0["result"].as_object_mut().unwrap();
        let nxt = result["nextPageCursor"].as_str().unwrap();
        let mut ret = OrderSet::new_with_cursor(
            pr.1,
            MarketVal::Pointer(market.clone()),
            CursorType::PrevNxt(String::default(), nxt.to_string()),
        );
        for item in result["list"].as_array_mut().unwrap() {
            let order = RestAPI::parse_order(ret.get_packet_time().clone(), item)?;
            ret.insert(order);
        }
        Ok(ret)
    }

    async fn request_orderbook(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        quantity: SubscribeQuantity,
    ) -> Result<OrderBook> {
        let category = match market.kind {
            MarketKind::InverseFuture { .. } | MarketKind::InversePerpetual { .. } => "inverse",
            _ => "",
        };

        let body = json!({
            "symbol": market.kind.symbol(),
            "category": category
        });

        let mut pr = self
            .request(
                context,
                RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/derivatives/v3/public/order-book/L2",
                    body,
                ),
            )
            .await?;

        let result = pr.0["result"].as_object().unwrap();
        let updated = Utc
            .timestamp_millis_opt(result["ts"].as_i64().unwrap())
            .unwrap();
        let mut orb = OrderBook::new(pr.1, MarketVal::Pointer(market.clone()), updated);

        let process_orders = |item: &serde_json::Value| -> OrderBookQuote {
            let items = item.as_array().unwrap();
            let quote = OrderBookQuote::new_with_string(
                items[0].as_str().unwrap_or("0").to_string(),
                items[1].as_str().unwrap_or("0").to_string(),
            );
            quote
        };

        orb.ask = result["a"]
            .as_array()
            .unwrap()
            .iter()
            .map(process_orders)
            .collect::<Vec<_>>();

        orb.bid = result["b"]
            .as_array()
            .unwrap()
            .iter()
            .map(process_orders)
            .collect::<Vec<_>>();

        orb.detail = pr.0.take();
        Ok(orb)
    }

    async fn request_position(
        &mut self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> Result<HashMap<MarketKind, PositionSet>> {
        let mut pr = self
            .request(
                context,
                RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/contract/v3/private/position/list",
                    json!({
                        "symbol": market.kind.symbol()
                    }),
                ),
            )
            .await?;

        let mut positions = HashMap::<MarketKind, PositionSet>::new();
        let result = pr.0["result"].as_object_mut().unwrap();
        for obj in result["list"].as_array_mut().unwrap() {
            let size = to_decimal_with_json(&obj["size"]).unwrap_or(Decimal::ZERO);
            if size == Decimal::ZERO {
                continue;
            }

            let symbol = obj["symbol"].as_str().unwrap().to_string();
            let side = if obj["side"].as_str().unwrap().to_string() == "Buy" {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let avg = to_decimal_with_json(&obj["entryPrice"])?;
            let unrealised_pnl = to_decimal_with_json(&obj["unrealisedPnl"])?;
            let liquidation = to_decimal_with_json(&obj["liqPrice"]).unwrap_or(Decimal::ZERO);
            let leverage = to_decimal_with_json(&obj["leverage"])?;
            let created = obj["createdTime"].as_str().unwrap().parse::<i64>()?;
            let updated = obj["updatedTime"].as_str().unwrap().parse::<i64>()?;

            let position = Position {
                ptime: pr.1.clone(),
                side: side.clone(),
                avg,
                size: size.abs(),
                unrealised_pnl,
                leverage,
                liquidation,
                updated: Utc.timestamp_millis_opt(updated).unwrap(),
                opened: Utc.timestamp_millis_opt(created).unwrap(),

                detail: obj.take(),
            };

            if let Some(set) = positions.get_mut(&market.kind) {
                set.insert_raw(position);
            } else {
                let mut set = PositionSet::new(pr.1.clone(), MarketVal::Symbol(market.kind.clone()));
                set.insert_raw(position);
                positions.insert(market.kind.clone(), set);
            }
        }

        Ok(positions)
    }

    async fn request_wallet(
        &mut self,
        context: &ExchangeContextPtr,
        _: &str,
    ) -> Result<DataSet<Asset>> {
        let mut pr = self
            .request(
                context,
                RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/contract/v3/private/account/wallet/balance",
                    serde_json::Value::default(),
                ),
            )
            .await?;

        let mut assets = DataSet::<Asset>::new(pr.1, UpdateType::Snapshot);
        let result = pr.0["result"].as_object_mut().unwrap();
        for obj in result["list"].as_array_mut().unwrap() {
            let total = to_decimal_with_json(&obj["walletBalance"])?;
            if total == Decimal::ZERO {
                continue;
            }

            let currency = obj["coin"].as_str().unwrap().to_string();
            let free = to_decimal_with_json(&obj["availableBalance"])?;
            let asset = Asset {
                ptime: assets.get_packet_time().clone(),
                updated: assets.get_packet_time().recvtime.clone(),
                currency: currency.clone(),
                lock: total - free,
                free: free.clone(),

                detail: obj.take(),
            };
            assets.insert_raw(currency, asset);
        }

        Ok(assets)
    }

    async fn sign(
        &self,
        ctx: &ExchangeContextPtr,
        mut param: RequestParam,
    ) -> Result<RequestParam> {
        let milli = Utc::now().timestamp_millis();

        param.body["api_key"] = serde_json::Value::from(ctx.param.key.key.clone());
        param.body["timestamp"] = serde_json::Value::from(milli);

        let urlcode = opk::json::url_encode(&param.body)?;
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, ctx.param.key.secret.as_bytes());
        let s = hex::encode(ring::hmac::sign(&key, urlcode.as_bytes()));

        param.body["sign"] = serde_json::Value::from(s);
        return Ok(param);
    }

    async fn request(
        &self,
        context: &ExchangeContextPtr,
        param: RequestParam,
    ) -> anyhow::Result<(serde_json::Value, PacketTime)> {
        let pr = self.req_async(context, param).await?;

        let json: serde_json::Value = pr.0.json().await?;
        if json["retCode"].as_i64().unwrap_or(1) != 0 {
            return Err(anyhow::anyhow!("invalid response : {}", json.to_string()));
        }
        return Ok((json, pr.1));
    }

    async fn request_market(
        &mut self,
        context: &ExchangeContextPtr,
    ) -> Result<DataSet<Market, MarketKind>> {
        let mut pr = self
            .request(
                context,
                RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/derivatives/v3/public/instruments-info",
                    serde_json::Value::default(),
                ),
            )
            .await?;

        let mut markets = DataSet::<Market, MarketKind>::new(pr.1, UpdateType::Snapshot);
        let result = pr.0["result"].as_object_mut().unwrap();
        for item in result["list"].as_array_mut().unwrap() {
            let obj = item.as_object_mut().unwrap();
            let symbol = obj["symbol"].as_str().unwrap().to_string();
            let k = match obj["contractType"].as_str().unwrap() {
                "LinearPerpetual" => MarketKind::LinearPerpetual(symbol.clone()),
                "InversePerpetual" => MarketKind::InversePerpetual(symbol.clone()),
                "InverseFutures" => MarketKind::InverseFuture(symbol.clone()),
                _ => MarketKind::Derivatives(obj["symbol"].as_str().unwrap().to_string()),
            };

            let pinfo = obj["priceFilter"].as_object().unwrap();
            let pp = to_decimal_with_json(&pinfo["tickSize"])?;
            let pmin = to_decimal_with_json(&pinfo["minPrice"])?;
            let pmax = to_decimal_with_json(&pinfo["maxPrice"])?;

            let ainfo = obj["lotSizeFilter"].as_object().unwrap();
            let ap = to_decimal_with_json(&ainfo["qtyStep"])?;
            let amin = to_decimal_with_json(&ainfo["minTradingQty"])?;
            let amax = to_decimal_with_json(&ainfo["maxTradingQty"])?;

            let market = opk::protocols::Market {
                ptime: markets.get_packet_time().clone(),
                kind: k.clone(),
                state: if obj["status"].as_str().unwrap() != "Trading" {
                    MarketState::Disable
                } else {
                    MarketState::Work
                },

                quote_currency: obj["quoteCoin"].as_str().unwrap().to_string(),
                base_currency: obj["baseCoin"].as_str().unwrap().to_string(),
                contract_size: Decimal::ONE,
                fee: FeeInfos::default(),
                amount_limit: [CurrencyPair::new_base(amin), CurrencyPair::new_base(amax)],
                price_limit: [pmin, pmax],
                pp_kind: PrecisionKind::Tick(pp),
                ap_kind: PrecisionKind::Tick(ap),

                updated: markets.get_packet_time().recvtime.clone(),
                detail: item.take(),
            };

            let ptr = Arc::new(market);
            markets.insert(k, ptr.clone());
            markets.insert(MarketKind::Derivatives(symbol), ptr);
        }

        Ok(markets)
    }
}

#[derive(Default)]
pub struct WebsocketItf {
    orderbooks: HashMap<MarketKind, OrderBook>,
    ping_task: Option<JoinHandle<()>>,
}

impl WebsocketItf {
    fn parse_private(
        &mut self,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        let result = match tp {
            "user.position.contractAccount" => {
                let mut positions = HashMap::<MarketKind, PositionSet>::new();
                for data in root["data"].as_array_mut().unwrap() {
                    let symbol = data["symbol"].as_str().unwrap().to_string();
                    let kind = MarketKind::Derivatives(symbol.clone());
                    let side = if data["side"].as_str().unwrap() == "Buy" {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    };

                    let avg = opk::float::to_decimal_with_json(&data["entryPrice"])?;
                    let leverage = opk::float::to_decimal_with_json(&data["leverage"])?;
                    let size = opk::float::to_decimal_with_json(&data["size"])?;
                    let unrealised_pnl = opk::float::to_decimal_with_json(&data["unrealisedPnl"])?;
                    let liquidation = opk::float::to_decimal_with_json(&data["liqPrice"])
                        .unwrap_or(Decimal::ZERO);
                    let opened = data["createdTime"].as_str().unwrap().parse::<i64>()?;
                    let updated = data["updatedTime"].as_str().unwrap().parse::<i64>()?;
                    let updated_datetime = Utc.timestamp_millis_opt(updated).unwrap();

                    let position = Position {
                        ptime: PacketTime::from(&updated_datetime),
                        side: side.clone(),
                        avg: avg,
                        size: size,
                        unrealised_pnl: unrealised_pnl,
                        leverage: leverage,
                        liquidation: liquidation,
                        opened: Utc.timestamp_millis_opt(opened).unwrap(),
                        updated: updated_datetime,
                        detail: std::mem::replace(data, Default::default()),
                    };

                    if let Some(set) = positions.get_mut(&kind) {
                        set.insert_raw(position);
                    } else {
                        let mut set = PositionSet::new(
                            position.get_packet_time().clone(),
                            MarketVal::Symbol(kind.clone()),
                        );
                        set.insert_raw(position);
                        positions.insert(kind, set);
                    }
                }
                SubscribeResult::Position(positions)
            }
            "user.wallet.contractAccount" => {
                let time = Utc::now();
                let mut assets =
                    DataSet::<Asset>::new(PacketTime::from(&time), UpdateType::Partial);
                for data in root["data"].as_array_mut().unwrap() {
                    let coin = data["coin"].as_str().unwrap().to_string();
                    let total = opk::float::to_decimal_with_json(&data["walletBalance"])?;
                    let free = opk::float::to_decimal_with_json(&data["availableBalance"])?;
                    let lock = total - free;
                    let asset = Asset {
                        ptime: PacketTime::from(&time),
                        updated: time.clone(),
                        currency: coin.clone(),
                        lock: lock,
                        free: free,
                        detail: std::mem::replace(data, Default::default()),
                    };
                    assets.insert_raw(coin, asset);
                }
                SubscribeResult::Balance(assets)
            }
            "user.order.contractAccount" => {
                let time = Utc::now();
                let mut ret = HashMap::<MarketKind, OrderSet>::default();
                for data in root["data"].as_array_mut().unwrap() {
                    let symbol = data["symbol"].as_str().unwrap().to_string();
                    let kind = MarketKind::Derivatives(symbol);
                    let os = if let Some(os) = ret.get_mut(&kind) {
                        os
                    } else {
                        let os =
                            OrderSet::new(PacketTime::from(&time), MarketVal::Symbol(kind.clone()));
                        ret.insert(kind.clone(), os);
                        ret.get_mut(&kind).unwrap()
                    };

                    let order = RestAPI::parse_order(PacketTime::from(&time), data)?;
                    os.insert(order);
                }

                SubscribeResult::Order(ret)
            }

            _ => SubscribeResult::None,
        };

        match result {
            SubscribeResult::None => self.parse_public(tp, root),
            _ => Ok(result),
        }
    }

    fn parse_public(
        &mut self,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        let mut topic = tp.split(".").peekable();
        let result = match *topic.peek().unwrap() {
            "orderbook" => {
                let time = Utc
                    .timestamp_millis_opt(root["ts"].as_i64().unwrap())
                    .unwrap();

                let data = &root["data"];
                let stype = &root["type"];
                let symbol = MarketKind::Derivatives(data["s"].as_str().unwrap().to_string());
                let orderbook = if let Some(orderbook) = self.orderbooks.get_mut(&symbol) {
                    if root["type"].as_str().unwrap() == "snapshot" {
                        *orderbook = OrderBook::new(
                            PacketTime::from(&time),
                            MarketVal::Symbol(symbol),
                            time.clone(),
                        );
                        orderbook
                    } else {
                        orderbook.ptime = PacketTime::from(&time);
                        orderbook.updated = time;
                        orderbook
                    }
                } else {
                    let orderbook = OrderBook::new(
                        PacketTime::from(&time),
                        MarketVal::Symbol(symbol.clone()),
                        time.clone(),
                    );

                    self.orderbooks.insert(symbol.clone(), orderbook);
                    self.orderbooks.get_mut(&symbol).unwrap()
                };

                for item in data["b"].as_array().unwrap() {
                    let items = item.as_array().unwrap();
                    orderbook.update_with_json(OrderBookSide::Bid, &items[0], &items[1])?;
                }

                for item in data["a"].as_array().unwrap() {
                    let items = item.as_array().unwrap();
                    orderbook.update_with_json(OrderBookSide::Ask, &items[0], &items[1])?;
                }

                orderbook.detail = root;
                SubscribeResult::Orderbook(orderbook.clone())
            }
            "publicTrade" => {
                let time = Utc
                    .timestamp_millis_opt(root["ts"].as_i64().unwrap())
                    .unwrap();

                let mut ret = HashMap::<MarketKind, PublicTradeSet>::default();
                let datas = root["data"]
                    .as_array_mut()
                    .ok_or(anyhow::anyhow!("invalid data member"))?;
                for data in datas {
                    let side = data["S"].as_str().unwrap();
                    let price = data["p"].as_str().unwrap();
                    let quantity = data["v"].as_str().unwrap();
                    let updated = data["T"].as_i64().unwrap();

                    let trade = PublicTrade {
                        ptime: PacketTime::from(&time),
                        updated: Utc.timestamp_millis_opt(updated).unwrap(),
                        price: LazyDecimal::Str(price.to_string()),
                        amount: LazyDecimal::Str(quantity.to_string()),
                        side: if side == "Buy" {
                            OrderSide::Buy
                        } else {
                            OrderSide::Sell
                        },
                        detail: std::mem::replace(data, Default::default()),
                    };

                    let symbol = data["s"].as_str().unwrap().to_string();
                    let kind = MarketKind::Derivatives(symbol);
                    if let Some(set) = ret.get_mut(&kind) {
                        set.insert_raw(trade);
                    } else {
                        let mut set = PublicTradeSet::new(
                            PacketTime::from(&time),
                            MarketVal::Symbol(kind.clone()),
                            None,
                        );
                        set.insert_raw(trade);
                        ret.insert(kind, set);
                    }
                }
                SubscribeResult::PublicTrades(ret)
            }
            _ => SubscribeResult::None,
        };
        Ok(result)
    }
}
#[async_trait]
impl ExWebsocketTrait for WebsocketItf {
    async fn subscribe(
        &mut self,
        ctx: &ExchangeContextPtr,
        client: &mut Websocket,
        s: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<()> {
        let body = s
            .iter()
            .map(|(k, v)| {
                v.iter()
                    .map(|p| match k {
                        SubscribeType::Order => "user.order.contractAccount".to_string(),
                        SubscribeType::Position => "user.position.contractAccount".to_string(),
                        SubscribeType::Balance => "user.wallet.contractAccount".to_string(),
                        SubscribeType::Orderbook => {
                            let quantity: SubscribeQuantity =
                                serde_json::from_str(p.0["quantity"].as_str().unwrap()).unwrap();
                            let size = match quantity {
                                SubscribeQuantity::Much => 500,
                                SubscribeQuantity::Least(str) | SubscribeQuantity::Fixed(str) => {
                                    let quantity = str.parse::<usize>().unwrap();
                                    let levels = vec![1, 50, 200, 500];
                                    let idx = levels.binary_search(&quantity).unwrap_or_else(|e| {
                                        if e >= levels.len() {
                                            levels.len() - 1
                                        } else {
                                            e
                                        }
                                    });
                                    levels[idx]
                                }
                                _ => 1,
                            };

                            format!("orderbook.{}.{}", size, p.0["symbol"].as_str().unwrap())
                        }
                        SubscribeType::PublicTrades => {
                            format!("publicTrade.{}", p.0["symbol"].as_str().unwrap())
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .flatten()
            .map(|s| json!(s))
            .collect::<Vec<serde_json::Value>>();

        let client_param = json!({
            "op" : "subscribe",
            "args" : json!(body),
        });

        client.send_text(client_param.to_string()).await?;
        Ok(())
    }

    async fn parse_msg(
        &mut self,
        ctx: &ExchangeContextPtr,
        socket: &mut Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult> {
        let result = match signal {
            Signal::Opened => {
                let cloned_socket = socket.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                        if cloned_socket.is_connected().await {
                            let str = json!({
                                "op":"ping"
                            })
                            .to_string();
                            if let Err(e) = cloned_socket.send(Message::Text(str)).await {
                                log::error!(
                                    "occur error for send ping to bybit : {}",
                                    e.to_string()
                                );
                            }
                        } else {
                            break;
                        }
                    }
                });

                if let Some(param) = socket
                    .get_param()
                    .await
                    .filter(|param| param.url.contains("private"))
                {
                    let now = Utc::now().timestamp_millis() + 5000;
                    let payload = format!("GET/realtime{}", now);

                    let key = ring::hmac::Key::new(
                        ring::hmac::HMAC_SHA256,
                        ctx.param.key.secret.as_bytes(),
                    );
                    let sign = hex::encode(ring::hmac::sign(&key, payload.as_bytes()));
                    let json = json!({
                        "op":"auth",
                        "args":[ctx.param.key.key.clone(), now, sign]
                    });
                    socket.send(Message::Text(json.to_string())).await?;
                    SubscribeResult::None
                } else {
                    SubscribeResult::Authorized(true)
                }
            }

            Signal::Recived(data) => match data {
                Message::Text(text) => {
                    let json: serde_json::Value =
                        serde_json::from_str(text.as_str()).unwrap_or_default();
                    if json["op"]
                        .as_str()
                        .map(|str| str == "auth")
                        .unwrap_or(false)
                    {
                        Ok(SubscribeResult::Authorized(
                            json["success"].as_bool().unwrap(),
                        ))
                    } else if let Some(str) = json["topic"].as_str().map(String::from) {
                        self.parse_private(&str, json)
                    } else {
                        Ok(SubscribeResult::None)
                    }?
                }
                _ => SubscribeResult::None,
            },
            _ => SubscribeResult::None,
        };
        Ok(result)
    }

    async fn make_websocket_param(
        &mut self,
        ctx: &ExchangeContextPtr,
        group: &String,
        subscribes: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<Option<WebsocketParam>> {
        let mut ws_param = WebsocketParam::default();
        ws_param.url = format!("{}{}", ctx.param.websocket.url, group);
        Ok(Some(ws_param))
    }

    async fn make_group_and_key(
        &mut self,
        s: &SubscribeType,
        param: &SubscribeParam,
    ) -> Option<(String, String)> {
        match s {
            SubscribeType::Balance => {
                Some(("/contract/private/v3".to_string(), "asset".to_string()))
            }
            SubscribeType::Order | SubscribeType::Position => {
                let symbol = param.0["market"].as_str()?;
                let key = format!("{}{}", s.clone() as u32, symbol);
                Some(("/contract/private/v3".to_string(), key))
            }
            SubscribeType::Orderbook | SubscribeType::PublicTrades => {
                let symbol = param.0["market"].as_str()?;
                let kind = serde_json::from_str::<MarketKind>(symbol).ok()?;
                let group = match kind {
                    MarketKind::LinearFuture(_) | MarketKind::LinearPerpetual(_) => {
                        "/contract/usdt/public/v3"
                    }
                    MarketKind::InverseFuture(_) | MarketKind::InversePerpetual(_) => {
                        "/contract/inverse/public/v3"
                    }
                    _ => return None,
                };
                let key = format!("{}{}", s.clone() as u32, symbol);
                Some((group.to_string(), key))
            }
        }
    }
}
