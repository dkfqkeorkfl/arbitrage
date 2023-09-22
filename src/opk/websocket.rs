use std::{collections::HashMap, sync::Arc};

use anyhow::Ok;
use async_trait::async_trait;
use chrono::Utc;
use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        Mutex, RwLock,
    };

use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::protocol::CloseFrame};

use super::protocols::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketParam {
    pub url: String,
    pub protocol: String,
    pub header: HashMap<String, String>,

    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub eject: chrono::Duration,
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub ping_interval: chrono::Duration,
}

impl Default for WebsocketParam {
    fn default() -> Self {
        Self {
            url: Default::default(),
            protocol: Default::default(),
            header: Default::default(),
            eject: chrono::Duration::seconds(5),
            ping_interval: chrono::Duration::minutes(1),
        }
    }
}

#[async_trait]
pub trait ConnectionItf: Send + Sync + 'static {
    async fn send(&mut self, message: Message) -> anyhow::Result<()>;
    async fn close(&mut self, param: Option<CloseFrame<'static>>) -> anyhow::Result<()>;
    async fn is_connected(&self) -> bool;
    fn get_param(&self) -> Arc<WebsocketParam>;
    fn get_id(&self) -> i64 {
        0
    }
}
type ConnectionRwArc = Arc<RwLock<dyn ConnectionItf>>;

//Behavior
struct ConnectionReal {
    param: Arc<WebsocketParam>,
    sender: UnboundedSender<Message>,
    is_connect: RwArc<bool>,
    created: i64,

    sendping: chrono::DateTime<Utc>,
    recvping: chrono::DateTime<Utc>,
}

#[async_trait]
impl ConnectionItf for ConnectionReal {
    async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        match &message {
            Message::Ping(_) => {}
            _ => {
                log::trace!(
                    "sending message using websocket({}) : {:?}",
                    &self.param.url,
                    message
                );
            }
        }

        self.sender.send(message)?;
        Ok(())
    }

    async fn close(&mut self, param: Option<CloseFrame<'static>>) -> anyhow::Result<()> {
        self.send(Message::Close(param)).await
    }

    async fn is_connected(&self) -> bool {
        self.is_connect.read().await.clone()
    }

    fn get_param(&self) -> Arc<WebsocketParam> {
        self.param.clone()
    }

    fn get_id(&self) -> i64 {
        self.created
    }
}

impl ConnectionReal {
    pub fn laytency(&self) -> chrono::Duration {
        if self.sendping > self.recvping {
            Utc::now() - self.sendping
        } else {
            self.recvping - self.sendping
        }
    }

    pub async fn ping(&mut self) -> anyhow::Result<()> {
        let now = Utc::now();
        let ping = json!(now.timestamp_millis());
        let payload = serde_json::to_vec(&ping)?;
        self.sendping = now;
        self.send(Message::Ping(payload)).await
    }

    pub fn update_pong(&mut self, mili: i64) -> bool {
        if mili == self.sendping.timestamp_millis() {
            self.recvping = Utc::now();
            true
        } else {
            false
        }
    }

    async fn accept<F>(
        param: WebsocketParam,
        stream: tokio_tungstenite::WebSocketStream<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>,
        f: F,
    ) -> anyhow::Result<Arc<RwLock<Self>>>
    where
        F: FnMut(ConnectionRwArc, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let callback = Arc::new(Mutex::new(f));
        let (mut write_half, mut read_half) = stream.split();
        let (sender, mut receiver) = unbounded_channel::<Message>();
        let now = Utc::now();

        let timer = param.ping_interval.to_std()?;
        let is_connected = Arc::new(RwLock::new(true));
        let s = Arc::from(RwLock::new(Self {
            param: Arc::new(param),
            sender: sender.clone(),
            is_connect: is_connected.clone(),
            created: now.timestamp_millis(),

            sendping: now.clone(),
            recvping: now.clone(),
        }));

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&s);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(timer.clone()).await;
                if *cloned_is_connected.read().await == false {
                    break;
                }

                let result = if let Some(ptr) = wpt_ws.upgrade() {
                    let mut locked = ptr.write().await;
                    if locked.laytency() > locked.param.eject {
                        Err(anyhow::anyhow!("occur eject for ping test"))
                    } else {
                        locked.ping().await
                    }
                } else {
                    Err(anyhow::anyhow!("websocket point is NULL"))
                };

                if let Err(e) = result {
                    log::info!("{}", e.to_string());

                    let is_connected = cloned_is_connected.read().await;
                    let result = if *is_connected {
                        sender.send(Message::Close(None))
                    } else {
                        std::result::Result::Ok(())
                    };

                    if let Err(e) = result {
                        log::error!("{}", e.0.to_string());
                    }
                }
            }
        });

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&s);
        let cloned_callback = callback.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                let result = if message.is_close() {
                    write_half.send(message).await
                } else if wpt_ws.upgrade().is_some() {
                    write_half.send(message).await
                } else {
                    std::result::Result::Ok(())
                };

                if *cloned_is_connected.read().await == false {
                    break;
                } else if let Err(e) = result {
                    if let Some(spt) = wpt_ws.upgrade() {
                        let mut callback = cloned_callback.lock().await;
                        callback(spt, Signal::Error(e.into())).await;
                    }
                }
            }
        });

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&s);
        tokio::spawn(async move {
            if let Some(spt) = wpt_ws.upgrade() {
                let mut locked_callback = callback.lock().await;
                locked_callback(spt, Signal::Opened).await;
            }

            while let Some(message) = read_half.next().await {
                if let Some(spt) = wpt_ws.upgrade() {
                    let signal = match message {
                        std::result::Result::Ok(msg) => {
                            if let Message::Pong(payload) = msg {
                                let result =
                                    serde_json::from_slice::<serde_json::Value>(&payload[..])
                                        .map_err(anyhow::Error::from)
                                        .and_then(|json| {
                                            let sendtime = json.as_i64().ok_or(anyhow::anyhow!(
                                                "invalid data from json to i64 for pong"
                                            ))?;
                                            Ok(sendtime)
                                        });

                                match result {
                                    anyhow::Result::Ok(mili) => {
                                        let mut locked = spt.write().await;
                                        locked.update_pong(mili);
                                        Signal::Recived(Message::Pong(payload))
                                    }
                                    Err(e) => Signal::Error(e),
                                }
                            } else {
                                log::trace!("recved : {:?}", msg);
                                Signal::Recived(msg)
                            }
                        }
                        Err(e) => Signal::Error(e.into()),
                    };

                    let mut locked_callback = callback.lock().await;
                    locked_callback(spt, signal).await;
                } else {
                    break;
                }
            }

            *is_connected.write().await = false;
            if let Some(spt) = wpt_ws.upgrade() {
                let mut locked_callback = callback.lock().await;
                locked_callback(spt, Signal::Closed).await;
            }
        });

        Ok(s)
    }

    pub async fn connect<F>(param: WebsocketParam, f: F) -> anyhow::Result<Arc<RwLock<Self>>>
    where
        F: FnMut(ConnectionRwArc, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let callback = Arc::new(Mutex::new(f));
        log::debug!("connecting websocket : {}", &param.url);
        let (stream, _) = connect_async(&param.url).await?;
        log::debug!("success for websocket : {}", &param.url);
        let (mut write_half, mut read_half) = stream.split();
        let (sender, mut receiver) = unbounded_channel::<Message>();
        let now = Utc::now();

        let timer = param.ping_interval.to_std()?;
        let is_connected = Arc::new(RwLock::new(true));
        let s = Arc::from(RwLock::new(Self {
            param: Arc::new(param),
            sender: sender.clone(),
            is_connect: is_connected.clone(),
            created: now.timestamp_millis(),

            sendping: now.clone(),
            recvping: now.clone(),
        }));

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&s);
        tokio::spawn(async move {
            loop {
                if *cloned_is_connected.read().await == false {
                    break;
                }

                let result = if let Some(ptr) = wpt_ws.upgrade() {
                    let mut locked = ptr.write().await;
                    if locked.sendping > locked.recvping {
                        if locked.laytency() > locked.param.eject {
                            Err(anyhow::anyhow!("occur eject for ping test"))
                        } else {
                            Ok(())
                        }
                    } else {
                        locked.ping().await
                    }
                } else {
                    Err(anyhow::anyhow!("websocket point is NULL"))
                };

                if let Err(e) = result {
                    log::info!("{}", e.to_string());

                    let is_connected = cloned_is_connected.read().await;
                    let result = if *is_connected {
                        sender.send(Message::Close(None))
                    } else {
                        std::result::Result::Ok(())
                    };

                    if let Err(e) = result {
                        log::error!("{}", e.0.to_string());
                    }
                } else {
                    tokio::time::sleep(timer.clone()).await;
                }
            }
        });

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&s);
        let cloned_callback = callback.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                let result = if message.is_close() {
                    write_half.send(message).await
                } else if wpt_ws.upgrade().is_some() {
                    write_half.send(message).await
                } else {
                    std::result::Result::Ok(())
                };

                if *cloned_is_connected.read().await == false {
                    break;
                } else if let Err(e) = result {
                    if let Some(spt) = wpt_ws.upgrade() {
                        let mut callback = cloned_callback.lock().await;
                        callback(spt, Signal::Error(e.into())).await;
                    }
                }
            }
        });

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&s);
        tokio::spawn(async move {
            if let Some(spt) = wpt_ws.upgrade() {
                let mut locked_callback = callback.lock().await;
                locked_callback(spt, Signal::Opened).await;
            }

            while let Some(message) = read_half.next().await {
                if let Some(spt) = wpt_ws.upgrade() {
                    let signal = match message {
                        std::result::Result::Ok(msg) => {
                            if let Message::Pong(payload) = msg {
                                let result =
                                    serde_json::from_slice::<serde_json::Value>(&payload[..])
                                        .map_err(anyhow::Error::from)
                                        .and_then(|json| {
                                            let sendtime = json.as_i64().ok_or(anyhow::anyhow!(
                                                "invalid data from json to i64 for pong"
                                            ))?;
                                            Ok(sendtime)
                                        });

                                match result {
                                    anyhow::Result::Ok(mili) => {
                                        let mut locked = spt.write().await;
                                        locked.update_pong(mili);
                                        Signal::Recived(Message::Pong(payload))
                                    }
                                    Err(e) => Signal::Error(e),
                                }
                            } else {
                                log::trace!("recved : {:?}", msg);
                                Signal::Recived(msg)
                            }
                        }
                        Err(e) => Signal::Error(e.into()),
                    };

                    let mut locked_callback = callback.lock().await;
                    locked_callback(spt, signal).await;
                } else {
                    break;
                }
            }

            *is_connected.write().await = false;
            if let Some(spt) = wpt_ws.upgrade() {
                let mut locked_callback = callback.lock().await;
                locked_callback(spt, Signal::Closed).await;
            }
        });

        Ok(s)
    }
}

#[derive(Default)]
pub struct ConnectionNull(pub Arc<WebsocketParam>);

impl ConnectionNull {
    pub async fn new() -> anyhow::Result<Arc<RwLock<Self>>> {
        Ok(Arc::new(RwLock::new(ConnectionNull::default())))
    }
}

#[async_trait]
impl ConnectionItf for ConnectionNull {
    async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        Ok(())
    }
    async fn close(&mut self, param: Option<CloseFrame<'static>>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn is_connected(&self) -> bool {
        true
    }
    fn get_param(&self) -> Arc<WebsocketParam> {
        self.0.clone()
    }
}

#[derive(Default, Clone)]
pub struct Websocket {
    conn: Option<ConnectionRwArc>,
}

#[derive(Clone)]
pub struct ReciveCallback {
    pub callback:
        Arc<Mutex<dyn FnMut(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static>>,
}

impl ReciveCallback {
    pub fn new<F>(f: F) -> Self
    where
        F: FnMut(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        ReciveCallback {
            callback: Arc::new(Mutex::new(f)),
        }
    }
}

impl Websocket {
    pub fn new(conn: ConnectionRwArc) -> Self {
        Self { conn: Some(conn) }
    }

    pub async fn accept(
        &mut self,
        param: WebsocketParam,
        stream: tokio_tungstenite::WebSocketStream<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>,
        callback: &ReciveCallback,
    ) -> anyhow::Result<()> {
        if let Some(c) = &self.conn {
            let locked = c.read().await;

            if locked.is_connected().await {
                return Err(anyhow::anyhow!("socket is already connected"));
            }
        }

        let callback = callback.callback.clone();
        let conn = ConnectionReal::accept(param, stream, move |conn, signal| {
            let cloned_callback = callback.clone();
            async move {
                let s = Self::new(conn);
                let mut locked_callback = cloned_callback.lock().await;
                locked_callback(s, signal).await;
            }
            .boxed()
        })
        .await?;
        self.conn = Some(conn);
        Ok(())
    }

    pub async fn connect(
        &mut self,
        param: Option<WebsocketParam>,
        callback: &ReciveCallback,
    ) -> anyhow::Result<()> {
        if let Some(c) = &self.conn {
            let locked = c.read().await;

            if locked.is_connected().await {
                return Err(anyhow::anyhow!("socket is already connected"));
            }
        }

        self.conn = if let Some(p) = param {
            let callback = callback.callback.clone();
            let conn = ConnectionReal::connect(p, move |conn, signal| {
                let cloned_callback = callback.clone();
                async move {
                    let s = Self::new(conn);
                    let mut locked_callback = cloned_callback.lock().await;
                    locked_callback(s, signal).await;
                }
                .boxed()
            })
            .await?;
            Some(conn)
        } else {
            Some(ConnectionNull::new().await?)
        };
        Ok(())
    }

    pub async fn send(&self, message: Message) -> anyhow::Result<()> {
        if self.is_connected().await == false {
            return Err(anyhow::anyhow!("websocket is disconnected"));
        }

        if let Some(c) = &self.conn {
            let mut locked = c.write().await;
            locked.send(message).await
        } else {
            Err(anyhow::anyhow!("websocket empty is none"))
        }
    }

    pub async fn send_text(&self, text: String) -> anyhow::Result<()> {
        if let Some(c) = &self.conn {
            let mut locked = c.write().await;
            locked.send(Message::Text(text)).await
        } else {
            Err(anyhow::anyhow!("websocket empty is none"))
        }
    }

    pub async fn close(&self, param: Option<CloseFrame<'static>>) -> anyhow::Result<()> {
        if self.is_connected().await == false {
            return Err(anyhow::anyhow!("websocket is already disconnected"));
        }

        if let Some(c) = &self.conn {
            let mut locked = c.write().await;
            locked.close(param).await
        } else {
            Err(anyhow::anyhow!("websocket is empty."))
        }
    }

    pub async fn is_connected(&self) -> bool {
        if let Some(c) = &self.conn {
            let locked = c.read().await;
            return locked.is_connected().await;
        }
        false
    }

    pub async fn get_param(&self) -> Option<Arc<WebsocketParam>> {
        if let Some(c) = &self.conn {
            let locked = c.read().await;
            return Some(locked.get_param());
        }
        None
    }

    pub async fn get_id(&self) -> i64 {
        if let Some(c) = &self.conn {
            let locked = c.read().await;
            return locked.get_id();
        }
        0
    }
}
