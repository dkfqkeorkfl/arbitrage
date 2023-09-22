use std::{
    collections::{HashMap, VecDeque},
    hash::{Hash, Hasher},
    str::FromStr,
    sync::Arc,
};

use super::{cache, float::to_decimal_with_json, leveldb_str::Leveldb, websocket::WebsocketParam};
use crate::opk::float::LazyDecimal;
use chrono::prelude::*;
use num::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;

pub type RwArc<T> = Arc<RwLock<T>>;

pub fn serialize_chrono_duration<S>(
    dur: &chrono::Duration,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let std = dur.to_std().map_err(serde::ser::Error::custom)?;
    std.serialize(serializer)
}

pub fn deserialize_chrono_duration<'de, D>(deserializer: D) -> Result<chrono::Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let std = std::time::Duration::deserialize(deserializer)?;
    chrono::Duration::from_std(std).map_err(serde::de::Error::custom)
}

pub fn serialize_anyhow_error<S>(error: &anyhow::Error, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&error.to_string())
}

pub fn deserialize_anyhow_error<'de, D>(deserializer: D) -> Result<anyhow::Error, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let message = String::deserialize(deserializer)?;
    Ok(anyhow::anyhow!(message))
}

fn serialize_error_map<S>(
    errors: &HashMap<String, anyhow::Error>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let errors_as_strings = errors
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect::<HashMap<_, _>>();
    errors_as_strings.serialize(serializer)
}

fn deserialize_error_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, anyhow::Error>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let errors_as_strings = HashMap::<String, String>::deserialize(deserializer)?;
    Ok(errors_as_strings
        .into_iter()
        .map(|(k, v)| (k, anyhow::anyhow!(v)))
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SubscribeSpeed {
    #[default]
    None,
    Fastest,
    Slowest,
    Least(String),
    Fixed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SubscribeQuantity {
    #[default]
    None,
    Much,
    Few,
    Least(String),
    Fixed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderBookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, Hash)]
pub enum OrderSide {
    Buy,
    #[default]
    Sell,
}

impl OrderSide {
    pub fn is_buy(&self) -> bool { 
        match self {
            OrderSide::Buy => true,
            OrderSide::Sell => false,
        }
    }

    pub fn is_sell(&self) -> bool { 
        self.is_buy() == false
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum MarketOpt {
    Spot,
    Margin,
    Derivatives,
    InverseFuture,
    InversePerpetual,
    LinearFuture,
    LinearPerpetual,
    #[default]
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum MarketKind {
    Spot(String),
    Margin(String),
    Derivatives(String),
    LinearFuture(String),
    LinearPerpetual(String),
    InverseFuture(String),
    InversePerpetual(String),
}

impl MarketKind {
    pub fn symbol(&self) -> &str {
        match self {
            MarketKind::Spot(s) => s.as_str(),
            MarketKind::Margin(s) => s.as_str(),
            MarketKind::Derivatives(s) => s.as_str(),
            MarketKind::LinearFuture(s) => s.as_str(),
            MarketKind::LinearPerpetual(s) => s.as_str(),
            MarketKind::InverseFuture(s) => s.as_str(),
            MarketKind::InversePerpetual(s) => s.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum CurrencySide {
    #[default]
    None,
    Quote,
    Base,
    Specific(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TickRange(Decimal, Decimal);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrecisionKind {
    RangeTick(Vec<TickRange>),
    Tick(Decimal),
}

impl PrecisionKind {
    pub fn calculate_with_range_impl(
        val: &Decimal,
        side: OrderBookSide,
        lvl: u64,
        range: &Vec<TickRange>,
    ) -> anyhow::Result<Decimal> {
        let idx = range
            .binary_search_by(|r| r.0.cmp(val))
            .unwrap_or_else(|e| e - 1);
        if range[idx].1 == Decimal::ZERO {
            return Err(anyhow::anyhow!("tick is Zero."));
        }

        let tick = range[idx].1;

        if let OrderBookSide::Ask = side {
            let nxt_range = if let Some(nxt) = range.get(idx + 1) {
                nxt.0
            } else {
                Decimal::MAX
            };

            let sz = ((nxt_range - val) / tick).to_u64().unwrap();
            let fixed_lvl = lvl - sz.min(lvl);
            if fixed_lvl == 0 {
                return Ok(val + (tick * Decimal::from(lvl)));
            }

            return PrecisionKind::calculate_with_range_impl(&nxt_range, side, fixed_lvl, range);
        }

        let sz = ((val - range[idx].0) / tick).to_u64().unwrap();
        let fixed_lvl = lvl - sz.min(lvl);
        if fixed_lvl == 0 {
            return Ok(val - (tick * Decimal::from(lvl)));
        }

        return PrecisionKind::calculate_with_range_impl(&range[idx].0, side, fixed_lvl, range);
    }

    pub fn calculate_with_range(
        val: &Decimal,
        side: OrderBookSide,
        lvl: i64,
        range: &Vec<TickRange>,
    ) -> anyhow::Result<Decimal> {
        if lvl < 0 {
            return PrecisionKind::calculate_with_range(
                val,
                if let OrderBookSide::Ask = side {
                    OrderBookSide::Bid
                } else {
                    OrderBookSide::Ask
                },
                lvl.abs(),
                range,
            );
        }

        let idx = range
            .binary_search_by(|r| r.0.cmp(val))
            .unwrap_or_else(|e| e - 1);
        if range[idx].1 == Decimal::ZERO {
            return Err(anyhow::anyhow!("tick is Zero."));
        }

        let tick = range[idx].1;
        let weight = val / tick;
        let fixed = if let OrderBookSide::Ask = side {
            weight.ceil()
        } else {
            weight.floor()
        } * tick;

        return PrecisionKind::calculate_with_range_impl(&fixed, side, lvl as u64, range);
    }

    pub fn calculate_with_tick(
        val: &Decimal,
        side: OrderBookSide,
        lvl: i64,
        tick: &Decimal,
    ) -> anyhow::Result<Decimal> {
        if tick == &Decimal::ZERO {
            return Err(anyhow::anyhow!("tick is Zero."));
        }

        let weight = val / tick;
        let fixed = if let OrderBookSide::Ask = side {
            weight.ceil()
        } else {
            weight.floor()
        } * tick;

        let d = Decimal::from(lvl * if let OrderBookSide::Ask = side { 1 } else { -1 });
        return Ok(fixed + (tick * d));
    }

    pub fn calculate(
        &self,
        val: &Decimal,
        side: OrderBookSide,
        lvl: i64,
    ) -> anyhow::Result<Decimal> {
        match self {
            PrecisionKind::Tick(tick) => PrecisionKind::calculate_with_tick(val, side, lvl, tick),
            PrecisionKind::RangeTick(ranges) => {
                PrecisionKind::calculate_with_range(val, side, lvl, ranges)
            }
        }
    }
}

impl Default for PrecisionKind {
    fn default() -> Self {
        PrecisionKind::Tick(Decimal::ZERO)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarketState {
    Work,
    Hidden,
    Disable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum OrderKind {
    #[default]
    Limit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum OrderState {
    Ordering(chrono::DateTime<Utc>),
    Opened,
    Canceling(chrono::DateTime<Utc>),
    PartiallyFilled,
    Filled,
    #[default]
    Rejected,
    Cancelled,
}

impl OrderState {
    pub fn is_process(&self) -> bool {
        match self {
            OrderState::Ordering(_)
            | OrderState::Opened
            | OrderState::PartiallyFilled
            | OrderState::Canceling(_) => true,
            _ => false,
        }
    }
    pub fn is_cancelled(&self) -> bool {
        match self {
            OrderState::Rejected | OrderState::Cancelled => true,
            _ => false,
        }
    }
    pub fn is_done(&self) -> bool {
        match self {
            OrderState::Filled => true,
            _ => false,
        }
    }

    pub fn synchronizable(&self) -> bool {
        match self {
            OrderState::Rejected | OrderState::Cancelled => false,
            _ => true,
        }
    }

    pub fn cancelable(&self) -> bool {
        match self {
            OrderState::Ordering(_) | OrderState::Opened | OrderState::PartiallyFilled => true,
            _ => false,
        }
    }

    pub fn get_created(&self) -> Option<&DateTime<Utc>> {
        match self {
            OrderState::Ordering(created) | OrderState::Canceling(created) => Some(&created),
            _ => None,
        }
    }

    pub fn is_ordering_or_canceling(&self) -> bool {
        match self {
            OrderState::Ordering(created) | OrderState::Canceling(created) => true,
            _ => false,
        }
    }
    pub fn is_expired(&self, expired: &chrono::Duration) -> bool {
        if let Some(created) = self.get_created() {
            Utc::now() - *created > *expired
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PacketTime {
    pub sendtime: chrono::DateTime<Utc>,
    pub recvtime: chrono::DateTime<Utc>,
}

impl Default for PacketTime {
    fn default() -> Self {
        Self {
            sendtime: Utc::now(),
            recvtime: Utc::now(),
        }
    }
}

impl PacketTime {
    pub fn from(time: &chrono::DateTime<Utc>) -> Self {
        Self {
            sendtime: time.clone(),
            recvtime: time.clone(),
        }
    }

    pub fn laytency(&self) -> chrono::Duration {
        self.recvtime - self.sendtime
    }
}

pub trait UpdatedTrait {
    fn get_updated(&self) -> &DateTime<Utc>;
}

pub trait KeyTrait<T> {
    fn get_key(&self) -> &T;
}

pub trait PacketTimeTrait {
    fn get_expired_date(&self, dur: &chrono::Duration) -> chrono::DateTime<Utc> {
        self.get_packet_time().recvtime + *dur
    }

    fn get_packet_time(&self) -> &PacketTime;
    fn laytency(&self) -> chrono::Duration {
        self.get_packet_time().laytency()
    }
}

#[repr(u32)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateType {
    Snapshot = 1,
    Partial = 2,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum CursorType {
    #[default]
    None,
    MinMax(usize, usize),
    PrevNxt(String, String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FeeInfos {
    bm: CurrencyPair,
    bt: CurrencyPair,
    sm: CurrencyPair,
    st: CurrencyPair,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CurrencyPair(CurrencySide, Decimal);

impl CurrencyPair {
    pub fn new(c: CurrencySide, v: Decimal) -> Self {
        CurrencyPair { 0: c, 1: v }
    }

    pub fn new_base(v: Decimal) -> Self {
        CurrencyPair {
            0: CurrencySide::Base,
            1: v,
        }
    }

    pub fn new_quote(v: Decimal) -> Self {
        CurrencyPair {
            0: CurrencySide::Quote,
            1: v,
        }
    }

    pub fn quote_max() -> Self {
        CurrencyPair {
            0: CurrencySide::Quote,
            1: Decimal::MAX,
        }
    }

    pub fn quote_zero() -> Self {
        CurrencyPair {
            0: CurrencySide::Quote,
            1: Decimal::ZERO,
        }
    }

    pub fn base_max() -> Self {
        CurrencyPair {
            0: CurrencySide::Base,
            1: Decimal::MAX,
        }
    }

    pub fn base_zero() -> Self {
        CurrencyPair {
            0: CurrencySide::Base,
            1: Decimal::ZERO,
        }
    }
}

// #[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSet<Value, Key = String>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    ptime: PacketTime,
    utype: UpdateType,
    // #[serde_as(as = "Vec<(_, _)>")]
    datas: HashMap<Key, Arc<Value>>,
    cursor: CursorType,
}

impl<Value, Key> PacketTimeTrait for DataSet<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}
impl<Value, Key> Default for DataSet<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    fn default() -> Self {
        let time = Utc.timestamp_millis_opt(0).unwrap();
        DataSet {
            utype: UpdateType::Snapshot,
            ptime: PacketTime::from(&time),
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: CursorType::None,
        }
    }
}

impl<Value, Key> DataSet<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    pub fn get_datas(&self) -> &HashMap<Key, Arc<Value>> {
        &self.datas
    }

    pub fn get_datas_mut(&mut self) -> &mut HashMap<Key, Arc<Value>> {
        &mut self.datas
    }

    pub fn new(ptime: PacketTime, utype: UpdateType) -> Self {
        DataSet {
            utype: utype,
            ptime: ptime,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: CursorType::None,
        }
    }

    pub fn new_with_cursor(ptime: PacketTime, cursor: CursorType) -> Self {
        DataSet {
            utype: UpdateType::Partial,
            ptime: ptime,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: cursor,
        }
    }

    pub fn remove(&mut self, key: &Key) {
        let now = Utc::now();
        self.ptime = PacketTime::from(&now);
        self.datas.remove(key);
    }

    pub fn insert_raw(&mut self, key: Key, value: Value) {
        self.insert(key, Arc::new(value));
    }

    pub fn insert(&mut self, key: Key, value: Arc<Value>) {
        if self.ptime.recvtime < value.get_packet_time().recvtime {
            self.ptime = value.get_packet_time().clone();
        }

        if let Some(ptr) = self.datas.get_mut(&key) {
            if ptr.get_updated() < value.get_updated() {
                *ptr = value;
            }
        } else {
            self.datas.insert(key, value);
        }
    }

    pub fn filter_new<P>(&self, mut predicate: P) -> (Self, HashMap<Key, Arc<Value>>)
    where
        Self: Sized,
        P: FnMut((&Key, &Arc<Value>)) -> bool,
    {
        let mut right = HashMap::<Key, Arc<Value>>::default();
        let mut wrong = HashMap::<Key, Arc<Value>>::default();
        for (key, value) in &self.datas {
            if predicate((key, value)) {
                right.insert(key.clone(), value.clone());
            } else {
                wrong.insert(key.clone(), value.clone());
            }
        }

        let os = DataSet {
            utype: self.utype.clone(),
            ptime: self.ptime.clone(),
            datas: right,
            cursor: CursorType::None,
        };
        (os, wrong)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub kind: MarketKind,
    pub state: MarketState,

    pub quote_currency: String,
    pub base_currency: String,
    pub contract_size: Decimal,
    pub fee: FeeInfos,
    pub amount_limit: [CurrencyPair; 2],
    pub price_limit: [Decimal; 2],

    pub pp_kind: PrecisionKind,
    pub ap_kind: PrecisionKind,
    pub detail: serde_json::Value,
}
pub type MarketPtr = Arc<Market>;

impl Hash for Market {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
    }
}

impl PartialEq for Market {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Eq for Market {}

impl UpdatedTrait for Market {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Market {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

#[derive(Default, Clone, Debug)]
pub enum MarketVal {
    #[default]
    None,
    Symbol(MarketKind),
    Pointer(MarketPtr),
}

impl MarketVal {
    pub fn symbol(&self) -> &str {
        match self {
            MarketVal::Symbol(s) => s.symbol(),
            MarketVal::Pointer(p) => p.kind.symbol(),
            MarketVal::None => "",
        }
    }

    pub fn has(&self) -> bool {
        match self {
            MarketVal::None => false,
            _ => true,
        }
    }

    pub fn is_ptr(&self) -> bool {
        match self {
            MarketVal::Pointer(ptr) => true,
            _ => false,
        }
    }
    pub fn market_ptr(&self) -> Option<&MarketPtr> {
        match self {
            MarketVal::Pointer(ptr) => Some(ptr),
            _ => None,
        }
    }

    pub fn market_kind(&self) -> Option<&MarketKind> {
        match self {
            MarketVal::Pointer(ptr) => Some(&ptr.kind),
            MarketVal::Symbol(kind) => Some(kind),
            _ => None,
        }
    }
}

impl Serialize for MarketVal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            MarketVal::None => serializer.serialize_none(),
            MarketVal::Symbol(kind) => kind.serialize(serializer),
            MarketVal::Pointer(ptr) => ptr.kind.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for MarketVal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
        match value {
            serde_json::Value::Null => Ok(MarketVal::None),
            _ => {
                let kind = serde_json::from_value::<MarketKind>(value).map_err(|err| {
                    serde::de::Error::custom(format!("Failed to deserialize MarketKind: {}", err))
                })?;
                Ok(MarketVal::Symbol(kind))
            }
        }
    }
}

pub trait MarketTrait {
    fn get_market(&self) -> &MarketVal;
    fn get_market_kind(&self) -> Option<&MarketKind> {
        self.get_market().market_kind()
    }
    fn get_symbol(&self) -> &str {
        self.get_market().symbol()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSetWithMarket<Value, Key = String>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    ptime: PacketTime,
    market: MarketVal,
    datas: HashMap<Key, Arc<Value>>,
    cursor: CursorType,
}

impl<Value, Key> MarketTrait for DataSetWithMarket<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    fn get_market(&self) -> &MarketVal {
        &self.market
    }
}

impl<Value, Key> PacketTimeTrait for DataSetWithMarket<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl<Value, Key> DataSetWithMarket<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    pub fn new(ptime: PacketTime, market: MarketVal) -> Self {
        DataSetWithMarket {
            ptime: ptime,
            market: market,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: CursorType::None,
        }
    }

    pub fn new_with_cursor(ptime: PacketTime, market: MarketVal, cursor: CursorType) -> Self {
        DataSetWithMarket {
            ptime: ptime,
            market: market,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: cursor,
        }
    }

    pub fn remove(&mut self, key: &Key) {
        let now = Utc::now();
        self.ptime = PacketTime::from(&now);
        self.datas.remove(key);
    }

    pub fn get_datas(&self) -> &HashMap<Key, Arc<Value>> {
        &self.datas
    }

    pub fn get_datas_mut(&mut self) -> &mut HashMap<Key, Arc<Value>> {
        &mut self.datas
    }

    pub fn filter_new<P>(&self, mut predicate: P) -> (Self, HashMap<Key, Arc<Value>>)
    where
        Self: Sized,
        P: FnMut((&Key, &Arc<Value>)) -> bool,
    {
        let mut right = HashMap::<Key, Arc<Value>>::default();
        let mut wrong = HashMap::<Key, Arc<Value>>::default();
        for (key, value) in &self.datas {
            if predicate((key, value)) {
                right.insert(key.clone(), value.clone());
            } else {
                wrong.insert(key.clone(), value.clone());
            }
        }

        let os = DataSetWithMarket {
            ptime: self.ptime.clone(),
            market: self.market.clone(),
            datas: right,
            cursor: CursorType::None,
        };
        (os, wrong)
    }

    pub fn get_market(&self) -> &MarketVal {
        &self.market
    }

    pub fn market_ptr(&self) -> Option<&MarketPtr> {
        self.market.market_ptr()
    }

    pub fn insert_raw(&mut self, value: Value) {
        self.insert(Arc::new(value));
    }

    pub fn insert(&mut self, value: Arc<Value>) {
        if self.ptime.recvtime < value.get_packet_time().recvtime {
            self.ptime = value.get_packet_time().clone();
        }

        if let Some(ptr) = self.datas.get_mut(value.get_key()) {
            if ptr.get_updated() < value.get_updated() {
                *ptr = value;
            }
        } else {
            self.datas.insert(value.get_key().clone(), value);
        }
    }

    pub fn insert_with_key(&mut self, key: Key, value: Arc<Value>) {
        if self.ptime.recvtime < value.get_packet_time().recvtime {
            self.ptime = value.get_packet_time().clone();
        }

        if let Some(ptr) = self.datas.get_mut(&key) {
            if ptr.get_updated() < value.get_updated() {
                *ptr = value;
            }
        } else {
            self.datas.insert(key, value);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub currency: String,
    pub lock: Decimal,
    pub free: Decimal,

    pub detail: serde_json::Value,
}
pub type AssetPtr = Arc<Asset>;

impl UpdatedTrait for Asset {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Asset {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl Asset {
    pub fn total(&self) -> Decimal {
        self.lock + self.free
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub side: OrderSide, // derive default를 사용하기 위하여. 만약 지정하지 않았으면 none이 맞긴하다(..)
    pub avg: Decimal,
    pub size: Decimal,
    pub unrealised_pnl: Decimal,
    pub leverage: Decimal,
    pub liquidation: Decimal,
    pub opened: chrono::DateTime<Utc>,

    pub detail: serde_json::Value,
}
pub type PositionPtr = Arc<Position>;
pub type PositionSet = DataSetWithMarket<Position, OrderSide>;

impl UpdatedTrait for Position {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Position {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl KeyTrait<OrderSide> for Position {
    fn get_key(&self) -> &OrderSide {
        &self.side
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookQuote {
    pub price: crate::opk::float::LazyDecimal,
    pub amount: crate::opk::float::LazyDecimal,
}

impl OrderBookQuote {
    pub fn new_with_string(price: String, amount: String) -> Self {
        OrderBookQuote {
            price: crate::opk::float::LazyDecimal::Str(price.into()),
            amount: crate::opk::float::LazyDecimal::Str(amount.into()),
        }
    }

    pub fn new_with_decimal(price: Decimal, amount: Decimal) -> Self {
        OrderBookQuote {
            price: crate::opk::float::LazyDecimal::Num(price),
            amount: crate::opk::float::LazyDecimal::Num(amount),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,
    pub market: MarketVal,

    pub ask: Vec<OrderBookQuote>,
    pub bid: Vec<OrderBookQuote>,

    pub detail: serde_json::Value,
}
pub type OrderBookPtr = Arc<OrderBook>;

impl UpdatedTrait for OrderBook {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for OrderBook {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl MarketTrait for OrderBook {
    fn get_market(&self) -> &MarketVal {
        &self.market
    }
}

impl OrderBook {
    pub fn new(time: PacketTime, market: MarketVal, updated: DateTime<Utc>) -> Self {
        OrderBook {
            ptime: time,
            updated: updated,
            market: market,
            ask: Vec::<OrderBookQuote>::new(),
            bid: Vec::<OrderBookQuote>::new(),
            detail: serde_json::Value::default(),
        }
    }

    pub fn update_with_json(
        &mut self,
        side: OrderBookSide,
        price: &serde_json::Value,
        amount: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let dprice = to_decimal_with_json(&price)?;
        let damount = to_decimal_with_json(&amount)?;
        self.update_with_decimal(side, dprice, damount)
    }

    pub fn update_with_decimal(
        &mut self,
        side: OrderBookSide,
        price: Decimal,
        amount: Decimal,
    ) -> anyhow::Result<()> {
        let quotes = if OrderBookSide::Ask == side {
            &mut self.ask
        } else {
            &mut self.bid
        };

        let pos = match side {
            OrderBookSide::Ask => {
                quotes.binary_search_by(|q| q.price.get_num().unwrap().cmp(&price))
            }
            OrderBookSide::Bid => {
                quotes.binary_search_by(|q| price.cmp(q.price.get_num().unwrap()))
            }
        };

        if amount == Decimal::ZERO {
            let idx = pos.map_err(|_| anyhow::anyhow!("invalid price. it's not exist price"))?;
            quotes.remove(idx);
        } else {
            match pos {
                std::result::Result::Ok(idx) => {
                    if let Some(q) = quotes.get_mut(idx) {
                        q.amount = LazyDecimal::Num(amount);
                    }
                }
                std::result::Result::Err(idx) => {
                    quotes.insert(idx, OrderBookQuote::new_with_decimal(price, amount))
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicTrade {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub price: crate::opk::float::LazyDecimal,
    pub amount: crate::opk::float::LazyDecimal,
    pub side: OrderSide,

    pub detail: serde_json::Value,
}
pub type PublicTradePtr = Arc<PublicTrade>;

impl UpdatedTrait for PublicTrade {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for PublicTrade {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicTradeSet {
    ptime: PacketTime,
    datas: VecDeque<PublicTradePtr>,

    market: MarketVal,
    limit: usize,
}

impl Default for PublicTradeSet {
    fn default() -> Self {
        let time = Utc.timestamp_millis_opt(0).unwrap();
        PublicTradeSet {
            ptime: PacketTime::from(&time),
            datas: VecDeque::<PublicTradePtr>::default(),
            market: MarketVal::default(),
            limit: usize::MAX,
        }
    }
}

impl PublicTradeSet {
    pub fn get_datas(&self) -> &VecDeque<PublicTradePtr> {
        &self.datas
    }

    pub fn new(ptime: PacketTime, market: MarketVal, limit: Option<usize>) -> Self {
        PublicTradeSet {
            ptime: ptime,
            market: market,
            datas: VecDeque::<PublicTradePtr>::default(),
            limit: limit.unwrap_or(usize::MAX),
        }
    }

    pub fn insert_raw(&mut self, value: PublicTrade) -> Option<PublicTradePtr> {
        self.insert(Arc::new(value))
    }

    pub fn insert(&mut self, value: PublicTradePtr) -> Option<PublicTradePtr> {
        if self.ptime.recvtime < value.ptime.recvtime {
            self.ptime = value.ptime.clone();
        }

        if let Err(p) = self
            .datas
            .binary_search_by_key(value.get_updated(), |t| *t.get_updated())
        {
            self.datas.insert(p, value);
            if self.datas.len() > self.limit {
                return self.datas.pop_front();
            }
        }

        None
    }

    pub fn update(&mut self, other: Self) -> bool {
        if self.get_symbol() != other.get_symbol() {
            return false;
        }

        self.ptime = other.ptime;
        for trade in other.datas {
            self.insert(trade);
        }
        true
    }
}
impl MarketTrait for PublicTradeSet {
    fn get_market(&self) -> &MarketVal {
        &self.market
    }
}

impl PacketTimeTrait for PublicTradeSet {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderParam {
    pub kind: OrderKind,
    pub price: Decimal,
    pub amount: Decimal,
    pub side: OrderSide,

    pub is_postonly: bool,
    pub is_reduce: bool,
    pub cid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub oid: String,
    pub cid: String,

    pub kind: OrderKind,
    pub price: Decimal,
    pub amount: Decimal,
    pub side: OrderSide,
    pub is_postonly: bool,
    pub is_reduce: bool,

    pub state: OrderState,
    pub avg: Decimal,
    pub procceed: CurrencyPair,
    pub fee: CurrencyPair,

    pub created: chrono::DateTime<Utc>,

    pub detail: serde_json::Value,
}
pub type OrderPtr = Arc<Order>;
pub type OrderSet = DataSetWithMarket<Order>;

impl UpdatedTrait for Order {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Order {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl KeyTrait<String> for Order {
    fn get_key(&self) -> &String {
        &self.oid
    }
}

impl Order {
    pub fn get_recved_duration(&self) -> chrono::Duration {
        Utc::now() - self.get_packet_time().recvtime
    }
    pub fn from_order_param(param: &OrderParam) -> Self {
        Order {
            ptime: PacketTime::default(),

            oid: String::default(),
            cid: param.cid.clone(),

            kind: param.kind.clone(),
            price: param.price.clone(),
            amount: param.amount.clone(),
            side: param.side.clone(),
            is_postonly: param.is_postonly.clone(),
            is_reduce: param.is_reduce.clone(),

            state: OrderState::default(),
            fee: CurrencyPair::default(),
            avg: Decimal::ZERO,
            procceed: CurrencyPair::default(),

            created: Utc::now(),
            updated: Utc::now(),
            detail: serde_json::Value::default(),
        }
    }

    pub fn amount_base(&self) -> Decimal {
        if self.avg == Decimal::ZERO {
            return Decimal::ZERO;
        }

        if self.procceed.0 == CurrencySide::Base {
            self.procceed.1.clone()
        } else {
            self.procceed.1 / self.avg
        }
    }

    pub fn amount_quote(&self) -> Decimal {
        if self.procceed.0 == CurrencySide::Quote {
            self.procceed.1.clone()
        } else {
            self.procceed.1 * self.avg
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderResult {
    pub success: OrderSet,
    #[serde(
        serialize_with = "serialize_error_map",
        deserialize_with = "deserialize_error_map"
    )]
    pub errors: HashMap<String, anyhow::Error>,
}

impl OrderResult {
    pub fn new_with_orders(orders: OrderSet) -> Self {
        OrderResult {
            success: orders,
            errors: HashMap::<String, anyhow::Error>::default(),
        }
    }

    pub fn new(ptime: PacketTime, market: MarketVal) -> Self {
        let os = OrderSet::new(ptime, market);
        OrderResult {
            success: os,
            errors: HashMap::<String, anyhow::Error>::default(),
        }
    }
}

impl PacketTimeTrait for OrderResult {
    fn get_packet_time(&self) -> &PacketTime {
        &self.success.ptime
    }
}

impl MarketTrait for OrderResult {
    fn get_market(&self) -> &MarketVal {
        &self.success.market
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct OrdSerachParam {
    pub state: Option<OrderState>,
    pub page: Option<(String /*limit*/, String /*cur*/)>,
    pub opened: Option<chrono::DateTime<Utc>>,
    pub closed: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeParam(pub serde_json::Value);

#[derive(Default)]
pub struct OrderbookSubscribeBuilder {
    market: Option<MarketPtr>,
    speed: SubscribeSpeed,
    quantity: SubscribeQuantity,
    additional: String,
}

impl OrderbookSubscribeBuilder {
    pub fn market(mut self, market: MarketPtr) -> Self {
        self.market = Some(market.clone());
        self
    }

    pub fn speed(mut self, speed: SubscribeSpeed) -> Self {
        self.speed = speed;
        self
    }

    pub fn quantity(mut self, quantity: SubscribeQuantity) -> Self {
        self.quantity = quantity;
        self
    }
    pub fn additional(mut self, additional: &str) -> Self {
        self.additional = additional.to_string();
        self
    }

    pub fn collect(self) -> SubscribeParam {
        let market = self.market.unwrap();
        let mut ret = json!({
            "market": serde_json::to_string(&market.kind).unwrap(),
            "symbol": market.kind.symbol(),
        });

        if self.speed != SubscribeSpeed::None {
            ret["speed"] = serde_json::Value::from(serde_json::to_string(&self.speed).unwrap());
        }
        if self.quantity != SubscribeQuantity::None {
            ret["quantity"] =
                serde_json::Value::from(serde_json::to_string(&self.quantity).unwrap());
        }
        if !self.additional.is_empty() {
            ret["additional"] = serde_json::Value::from(self.additional);
        }
        SubscribeParam(ret)
    }
}

#[derive(Default)]
pub struct MSASubscribeBuilder {
    market: Option<MarketPtr>,
    speed: SubscribeSpeed,
    additional: String,
}

impl MSASubscribeBuilder {
    pub fn market(mut self, market: MarketPtr) -> Self {
        self.market = Some(market.clone());
        self
    }

    pub fn speed(mut self, speed: SubscribeSpeed) -> Self {
        self.speed = speed;
        self
    }

    pub fn additional(mut self, additional: &str) -> Self {
        self.additional = additional.to_string();
        self
    }

    pub fn collect(self) -> SubscribeParam {
        let market = self.market.unwrap();
        let mut ret = json!({
            "market": serde_json::to_string(&market.kind).unwrap(),
            "symbol": market.kind.symbol(),
        });

        if self.speed != SubscribeSpeed::None {
            ret["speed"] = serde_json::Value::from(serde_json::to_string(&self.speed).unwrap());
        }
        if !self.additional.is_empty() {
            ret["additional"] = serde_json::Value::from(self.additional);
        }
        SubscribeParam(ret)
    }
}

#[derive(Default)]
pub struct MASubscribeBuilder {
    market: Option<MarketPtr>,
    additional: String,
}

impl MASubscribeBuilder {
    pub fn market(mut self, market: MarketPtr) -> Self {
        self.market = Some(market);
        self
    }

    pub fn additional(mut self, additional: String) -> Self {
        self.additional = additional;
        self
    }

    pub fn collect(self) -> SubscribeParam {
        let mut ret = serde_json::Value::default();
        if self.market.is_some() {
            let market = self.market.unwrap();
            ret["market"] = serde_json::Value::from(serde_json::to_string(&market.kind).unwrap());
            ret["symbol"] = serde_json::Value::from(market.kind.symbol());
        }

        if !self.additional.is_empty() {
            ret["additional"] = serde_json::Value::from(self.additional);
        }
        SubscribeParam(ret)
    }
}

#[repr(u32)]
#[derive(Eq, PartialEq, Clone, Debug, Hash, Serialize, Deserialize)]
pub enum SubscribeType {
    Orderbook = 1,
    PublicTrades = 2,
    Order = 3,
    Balance = 4,
    Position = 5,
}

impl SubscribeType {
    pub fn orderbook_param() -> OrderbookSubscribeBuilder {
        OrderbookSubscribeBuilder::default()
    }
    pub fn public_trades_param() -> MSASubscribeBuilder {
        MSASubscribeBuilder::default()
    }
    pub fn order_param() -> MASubscribeBuilder {
        MASubscribeBuilder::default()
    }
    pub fn balance_param() -> MASubscribeBuilder {
        MASubscribeBuilder::default()
    }
    pub fn position_param() -> MASubscribeBuilder {
        MASubscribeBuilder::default()
    }
}

#[derive(Debug)]
pub enum Signal {
    Opened,
    Closed,
    Recived(Message),
    Error(anyhow::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SubscribeResult {
    None,
    Authorized(bool),
    #[serde(
        serialize_with = "serialize_anyhow_error",
        deserialize_with = "deserialize_anyhow_error"
    )]
    Err(anyhow::Error),
    Orderbook(OrderBook),
    PublicTrades(HashMap<MarketKind, PublicTradeSet>),
    Order(HashMap<MarketKind, OrderSet>),
    Balance(DataSet<Asset>),

    Position(HashMap<MarketKind, PositionSet>),
}

#[derive(Debug)]
pub struct ExchangeStorage {
    pub markets: RwArc<DataSet<Market, MarketKind>>,
    pub orderbook: RwArc<HashMap<MarketKind, Arc<OrderBook>>>,
    pub trades: RwArc<HashMap<MarketKind, PublicTradeSet>>,
    pub positions: RwArc<HashMap<MarketKind, PositionSet>>,
    pub orders: cache::NurCache<String, (OrderPtr, MarketPtr)>,
    pub assets: RwArc<DataSet<Asset>>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct RestAPIParam {
    pub url: String,
    pub proxy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExchangeKey {
    pub tag: String,
    pub exchange: String,
    pub is_testnet: bool,

    pub key: String,
    pub secret: String,
    pub passphrase: String,

    pub email: String,
    pub socks5ip: String,
    pub socks5port: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id : String,
    pub password : String,
    pub auth : String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub ping_interval: chrono::Duration,

    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub eject: chrono::Duration,
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub sync_expired_duration: chrono::Duration,
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub state_expired_duration: chrono::Duration,

    pub opt_max_order_chche: usize,
    pub opt_max_trades_chche: usize,
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            ping_interval: chrono::Duration::minutes(1),
            eject: chrono::Duration::seconds(5),
            sync_expired_duration: chrono::Duration::minutes(5),
            state_expired_duration: chrono::Duration::minutes(1),

            opt_max_order_chche: 2000,
            opt_max_trades_chche: 2000,
        }
    }
    
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeParam {
    pub websocket: WebsocketParam,
    pub restapi: RestAPIParam,
    pub key: Arc<ExchangeKey>,
    pub config : ExchangeConfig,
    pub kind: MarketOpt,
}

#[derive(Debug)]
pub struct ExchangeContext {
    pub param: ExchangeParam,
    pub storage: ExchangeStorage,
    pub requester: reqwest::Client,
    pub recorder: Leveldb,
}
pub type ExchangeContextPtr = Arc<ExchangeContext>;

impl ExchangeContext {
    pub fn new(param: ExchangeParam, recorder: Leveldb, requester: reqwest::Client) -> Self {
        let storage = ExchangeStorage {
            markets: Arc::new(RwLock::new(DataSet::<Market, MarketKind>::default())),
            orderbook: Arc::new(RwLock::new(HashMap::<MarketKind, Arc<OrderBook>>::default())),
            trades: Arc::new(RwLock::new(HashMap::<MarketKind, PublicTradeSet>::default())),
            positions: Arc::new(RwLock::new(HashMap::<MarketKind, PositionSet>::default())),
            assets: Arc::new(RwLock::new(DataSet::<Asset>::default())),
            orders: cache::NurCache::<String, (OrderPtr, MarketPtr)>::new(
                param.config.opt_max_order_chche.clone(),
            ),
        };

        ExchangeContext {
            param: param,
            storage: storage,
            requester: requester,
            recorder: recorder,
        }
    }

    pub async fn find_market(&self, kind: &MarketKind) -> Option<MarketPtr> {
        let locked = self.storage.markets.read().await;
        locked.get_datas().get(kind).cloned()
    }

    pub async fn load_db_order(
        &self,
        oid: &str,
        kind: &MarketKind,
    ) -> anyhow::Result<Option<Arc<(OrderPtr, MarketPtr)>>> {
        let str = self.recorder.get(oid)?;
        if str.is_none() {
            return Ok(None);
        }

        let mut value = serde_json::Value::from_str(&str.unwrap())?;
        let items = value
            .as_array_mut()
            .ok_or(anyhow::anyhow!("invalid value"))?;

        let order = serde_json::from_value::<OrderPtr>(std::mem::replace(
            &mut items[0],
            serde_json::Value::default(),
        ))?;
        let kind = serde_json::from_value(std::mem::replace(
            &mut items[1],
            serde_json::Value::default(),
        ))?;
        let market = self
            .find_market(&kind)
            .await
            .ok_or(anyhow::anyhow!("invalid market"))?;
        let ret = Arc::new((order.clone(), market.clone()));
        self.cache_order(ret.clone()).await?;
        Ok(Some(ret))
    }

    pub fn save_db_order(&self, order: OrderPtr, market: MarketPtr) -> anyhow::Result<()> {
        let order_val = serde_json::to_value(&order)?;
        let market_val = serde_json::to_value(&market.kind)?;
        let items = vec![order_val, market_val];
        let str = serde_json::to_string(&items)?;
        self.recorder.put(&order.oid, &str)?;
        Ok(())
    }

    pub async fn cache_order(&self, order: Arc<(OrderPtr, MarketPtr)>) -> anyhow::Result<()> {
        let oid = order.0.oid.clone();
        if let Some(dump) = self.storage.orders.insert(oid, order).await {
            self.save_db_order(dump.0.clone(), dump.1.clone())?;
        }

        Ok(())
    }

    pub async fn find_order(
        &self,
        oid: &String,
        kind: &MarketKind,
    ) -> anyhow::Result<Option<OrderPtr>> {
        let item = if let Some((result, dump_opt)) = self.storage.orders.get(&oid).await {
            if let Some(dump) = dump_opt {
                self.save_db_order(dump.0.clone(), dump.1.clone())?;
            }

            result
        } else {
            let opt = self.load_db_order(oid.as_str(), &kind).await?;
            if opt.is_none() {
                return Ok(None);
            }

            opt.unwrap()
        };

        if item.1.kind != *kind {
            return Ok(None);
        }

        let now = Utc::now();
        let order = item.0.clone();
        if order.state.synchronizable()
            && now > order.get_expired_date(&self.param.config.sync_expired_duration)
        {
            return Ok(None);
        }

        Ok(Some(order))
    }

    pub async fn update(&self, result: SubscribeResult) -> anyhow::Result<()> {
        match result {
            SubscribeResult::Orderbook(mut data) => {
                let market_kind = {
                    let kind = data
                        .market
                        .market_kind()
                        .ok_or(anyhow::anyhow!("invalid kind"))?;
                    let market = self
                        .find_market(kind)
                        .await
                        .ok_or(anyhow::anyhow!("cannot find market"))?;
                    data.market = MarketVal::Pointer(market);
                    data.market.market_kind().unwrap()
                };

                let mut locked = self.storage.orderbook.write().await;
                if let Some(ptr) = locked.get_mut(market_kind) {
                    *ptr = Arc::new(data);
                } else {
                    locked.insert(market_kind.clone(), Arc::new(data));
                }
            }
            SubscribeResult::PublicTrades(data) => {
                let mut locked = self.storage.trades.write().await;
                for (key, value) in data {
                    let set = if let Some(set) = locked.get_mut(&key) {
                        set
                    } else {
                        let market = self
                            .find_market(&key)
                            .await
                            .ok_or(anyhow::anyhow!("invalid market"))?;
                        if let Some(v) = locked.get_mut(&market.kind) {
                            v
                        } else {
                            locked.insert(
                                key,
                                PublicTradeSet::new(
                                    value.get_packet_time().clone(),
                                    MarketVal::Pointer(market.clone()),
                                    Some(self.param.config.opt_max_trades_chche),
                                ),
                            );
                            locked.get_mut(&market.kind).unwrap()
                        }
                    };

                    for v in value.get_datas() {
                        set.insert(v.clone());
                    }
                }
            }
            SubscribeResult::Order(data) => {
                for (key, value) in data {
                    let market = self
                        .find_market(&key)
                        .await
                        .ok_or(anyhow::anyhow!("invalid market"))?;

                    for (oid, order) in value.get_datas() {
                        self.cache_order(Arc::new((order.clone(), market.clone()))).await?;
                    }
                }
            }

            SubscribeResult::Balance(data) => {
                let mut locked = self.storage.assets.write().await;
                for (key, value) in data.get_datas() {
                    if value.total() == Decimal::ZERO {
                        locked.remove(key);
                    } else {
                        locked.insert(key.clone(), value.clone());
                    }
                }
            }
            SubscribeResult::Position(data) => {
                let mut locked = self.storage.positions.write().await;
                for (key, value) in data {
                    let set = if let Some(set) = locked.get_mut(&key) {
                        set
                    } else {
                        let market = self
                            .find_market(&key)
                            .await
                            .ok_or(anyhow::anyhow!("invalid market"))?;
                        if let Some(v) = locked.get_mut(&market.kind) {
                            v
                        } else {
                            locked.insert(
                                market.kind.clone(),
                                PositionSet::new(
                                    value.get_packet_time().clone(),
                                    MarketVal::Pointer(market.clone()),
                                ),
                            );
                            locked.get_mut(&market.kind).unwrap()
                        }
                    };

                    for (k, v) in value.get_datas() {
                        if v.size == Decimal::ZERO {
                            set.remove(k);
                        } else {
                            set.insert(v.clone());
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}
