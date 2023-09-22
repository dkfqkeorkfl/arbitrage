use std::{
    collections::HashMap,
    io::ErrorKind,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use anyhow::{anyhow, Ok};
use chrono::{TimeZone, Utc};
use futures::{FutureExt, TryFutureExt};
use hyper::{body::HttpBody, *};
use ring::rand::SecureRandom;
use ring::{digest, rand};
use serde_json::json;
use tokio::{fs::File, io::AsyncReadExt, sync::RwLock};

use crate::opk::{
    download_encrypted_sheet,
    exchange::{self},
    leveldb_str::Leveldb,
    protocols::*,
    services::manual::Manual,
    websocket::Websocket,
};

use super::{firewall::Firewall, service::*, packet::*};

struct ClientInner {
    socket: Websocket,
    apps: HashMap<String, App>,
    secret: Option<String>,
}

impl ClientInner {
    pub fn new(socket: Websocket) -> Self {
        ClientInner {
            socket: socket,
            apps: HashMap::<String, App>::default(),
            secret: None,
        }
    }
}

struct Client {
    inner: RwArc<ClientInner>,
}

impl Client {
    pub fn new(ws: Websocket) -> Self {
        let inner = ClientInner::new(ws);
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    async fn on_recv(
        &self,
        context: &Arc<Context>,
        packet: &Packet,
    ) -> anyhow::Result<serde_json::Value> {
        let result = match &packet.protocol {
            Protocol::Open(name) => {
                let id = Utc::now().timestamp_millis().to_string();

                let mut locked = self.inner.write().await;
                let service = match name.as_str() {
                    "manual" => App::new::<Manual>(id.clone()).await,
                    _ => App::default(),
                };

                service.open(locked.socket.clone()).await?;
                locked.apps.insert(id.clone(), service);
                let ret = serde_json::Value::String(id);
                Ok(ret)
            }
            Protocol::Close(id) => {
                let mut locked = self.inner.write().await;
                if let Some(app) = locked.apps.remove(id) {
                    app.release().await?;
                }
                let ret = serde_json::Value::from_str(&id)?;
                Ok(ret)
            }
            Protocol::Proccess(id) => {
                let locked = self.inner.read().await;
                if let Some(v) = locked.apps.get(id) {
                    v.proccess(context, packet.req.clone().unwrap_or_default())
                        .await
                } else {
                    Err(anyhow::anyhow!("cannot find app({})", id))
                }
            }
            _ => Err(anyhow!("invalid protocol")),
        }?;

        Ok(result)
    }

    pub async fn on_recv_with_txt(&self, context: &Arc<Context>, txt: &str) -> Packet {
        let mut packet = match serde_json::from_str::<Packet>(txt) {
            std::result::Result::Ok(packet) => packet,
            Err(e) => {
                return Packet::err(
                    Protocol::Unknown,
                    e.to_string(),
                    Some(serde_json::Value::from(txt)),
                );
            }
        };

        match self.on_recv(context, &packet).await {
            std::result::Result::Ok(result) => {
                packet.res = Some(result);
            }
            Err(e) => {
                packet.err = Some(e.to_string());
            }
        }

        packet.timestamp = Utc::now().timestamp_millis();
        packet
    }
}

struct Shared {
    cert: Vec<u8>,
    addr: IpAddr,
    websocket_port: u16,
    firewall: Firewall,
    accounts: HashMap<String, Account>,
    eject: chrono::Duration,
}

pub struct Application {
    context: Arc<Context>,
    server: super::server::Server,
    sockets: RwArc<HashMap<i64, Client>>,
}

impl Application {
    async fn serve_dispatch(
        req: &mut Request<Body>,
        shared: RwArc<Shared>,
    ) -> anyhow::Result<Response<Body>> {
        let params = if let Some(result) = req.body_mut().data().await {
            let bytes = result?;
            let str = std::str::from_utf8(&bytes)?;
            let json = serde_json::from_str::<serde_json::Value>(&str)?;
            json
        } else {
            Default::default()
        };

        let timestamp = params["timestamp"]
            .as_i64()
            .ok_or(anyhow!("invalid timestamp"))?;
        let time = Utc.timestamp_millis_opt(timestamp).unwrap();
        let eject = shared.read().await.eject.clone();
        if Utc::now() > time + eject {
            return Err(anyhow!("occur eject why timestamp is old"));
        }

        let response = match req.uri().path() {
            "/sign_in" => {
                let addr = req
                    .extensions()
                    .get::<SocketAddr>()
                    .map(|addr| addr.ip())
                    .ok_or(anyhow!("cannot know ip-address"))?;

                let locked = shared.read().await;
                let id = params["id"].as_str().ok_or(anyhow!("invalid id type"))?;
                let password = params["password"]
                    .as_str()
                    .ok_or(anyhow!("invalid id type"))?;
                let hashed_password =
                    hex::encode(digest::digest(&digest::SHA256, password.as_bytes()));

                let account = locked
                    .accounts
                    .get(id)
                    .filter(|account| account.password == hashed_password)
                    .ok_or(anyhow!("invalid id or password"))?;

                let rng = rand::SystemRandom::new();
                let mut random_bytes = [0u8; 32];
                rng.fill(&mut random_bytes).unwrap();
                let hash = digest::digest(&digest::SHA256, &random_bytes);
                let token = hex::encode(hash.as_ref());
                locked.firewall.update(addr, token.clone()).await;

                let json = json!({ "token": token, "url" : locked.addr.to_string(), "port" : locked.websocket_port.clone(), "cert" : locked.cert });
                let str = serde_json::to_string(&json)?;
                Response::builder()
                    .header("Content-Type", "application/json")
                    .body(Body::from(str))
                    .map_err(anyhow::Error::from)
            }
            _ => Err(anyhow!("occur error not exist path")),
        }?;

        Ok(response)
    }
    async fn serve_static(req: &mut Request<Body>) -> anyhow::Result<Response<Body>> {
        let url = match req.uri().path() {
            "/" | "" => "/index.html",
            _ => req.uri().path(),
        };

        let path: PathBuf = format!("./build{}", url).into();
        let response = match tokio::fs::read(&path).await {
            std::result::Result::Ok(file) => {
                let mime_type = mime_guess::from_path(&path).first_or_octet_stream();
                Response::builder()
                    .header("Content-Type", mime_type.as_ref())
                    .body(Body::from(file))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("Not Found".into()),
            Err(_) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Internal Server Error".into()),
        }?;

        Ok(response)
    }

    pub async fn new(sheet: &str, tab: &str, key: &str) -> anyhow::Result<Self> {
        let root = download_encrypted_sheet::<serde_json::Value>(sheet, tab, key).await?;
        let items = root.as_array().ok_or(anyhow::anyhow!("it is not array."))?;
        let converter = items
            .iter()
            .map(|item| {
                let key = item["key"]
                    .as_str()
                    .ok_or(anyhow::anyhow!("invalid key"))?
                    .to_string();
                let val = if item["val"].is_boolean() {
                    item["val"].as_bool().unwrap().to_string()
                } else {
                    item["val"]
                        .as_str()
                        .ok_or(anyhow::anyhow!(
                            "invalid val : {:?}",
                            item["val"].to_string()
                        ))?
                        .to_string()
                };
                Ok((key, val))
            })
            .collect::<anyhow::Result<HashMap<_, _>>>()?;

        let str = serde_json::to_string(&converter)?;
        let config = serde_json::from_str::<Config>(&str)?;

        let keys = download_encrypted_sheet::<Vec<Arc<ExchangeKey>>>(
            &config.apikey_sheet,
            &config.apikey_tab,
            key,
        )
        .await?
        .iter()
        .filter_map(|v| {
            if v.tag.is_empty() {
                None
            } else {
                Some((v.tag.to_string(), v.clone()))
            }
        })
        .collect::<HashMap<_, _>>();

        let accounts =
            download_encrypted_sheet::<Vec<Account>>(&config.apikey_sheet, "account", key)
                .await?
                .iter()
                .map(|v| (v.id.to_string(), v.clone()))
                .collect::<HashMap<_, _>>();

        let is_debug = config.debug.parse::<bool>()?;
        let addr = if is_debug == false {
            get_if_addrs::get_if_addrs()
                .map_err(anyhow::Error::from)
                .and_then(|addrs| {
                    for addr in addrs {
                        if !addr.is_loopback() {
                            return Ok(addr.ip());
                        }
                    }
                    Err(anyhow::anyhow!("err"))
                })?
        } else {
            Ipv4Addr::new(127, 0, 0, 1).into()
        };
        let eject = config.server_eject.parse::<i64>()?;
        let ping_interval = config.server_ping.parse::<i64>()?;
        let param = super::server::Param {
            addr: addr,
            cert: config.cert.clone(),
            key: config.key.clone(),

            eject: chrono::Duration::seconds(eject),
            ping_interval: chrono::Duration::seconds(ping_interval),
        };

        let webserver_port = config.webserver_port.parse::<u16>()?;
        let websocket_port = config.websocket_port.parse::<u16>()?;

        let mut cert = File::open(config.cert.clone()).await?;
        let mut buf = Vec::new();
        cert.read_to_end(&mut buf).await?;

        let shared = Arc::new(RwLock::new(Shared {
            cert: buf,
            addr: param.addr.clone(),
            websocket_port: websocket_port.clone(),
            firewall: Firewall::new(),
            accounts: accounts,
            eject: chrono::Duration::seconds(eject),
        }));

        let cloned_shared = shared.clone();
        let server = super::server::Server::new(param, webserver_port, move |mut req| {
            let shared = cloned_shared.clone();
            async move {
                match req.method().clone() {
                    Method::GET => Application::serve_static(&mut req).await,
                    _ => Application::serve_dispatch(&mut req, shared).await,
                }
            }
            .boxed()
        })
        .await?;

        let tags = Arc::new(serde_json::to_value(
            &keys.keys().cloned().collect::<Vec<_>>(),
        )?);
        let leveldb = Leveldb::new(&config.cache_name)?;
        let ctx = Arc::new(Context {
            config: config,
            exchanges: exchange::Manager::new(keys, leveldb.clone()),
            db: leveldb,
        });

        let sockets = Arc::new(RwLock::new(HashMap::<i64, Client>::default()));
        let cloned_socket = sockets.clone();
        let cloned_ctx = ctx.clone();
        let mut locked = shared.write().await;
        locked.firewall = server
            .open_websocket(websocket_port, move |ws, signal| {
                let cloned_tags = tags.clone();
                let sockets = cloned_socket.clone();
                let ctx = cloned_ctx.clone();
                async move {
                    let id = ws.get_id().await;
                    let ret = match &signal {
                        Signal::Opened => {
                            let packet = Packet::res(Protocol::Init, (*cloned_tags).clone(), None);
                            async move { serde_json::to_string(&packet).map_err(|e| e.into()) }
                                .and_then(|str| async move {
                                    println!("{}", str);
                                    if let Err(err) = ws.send_text(str).await {
                                        ws.close(None).await.and_then(move |_| Err(err))
                                    } else {
                                        let client = Client::new(ws);
                                        let mut locked = sockets.write().await;
                                        locked.insert(id, client);
                                        Ok(())
                                    }
                                })
                                .await
                        }
                        Signal::Closed => {
                            let mut locked = sockets.write().await;
                            locked.remove(&id);
                            Ok(())
                        }
                        Signal::Recived(msg) => {
                            if let tokio_tungstenite::tungstenite::Message::Text(txt) = msg {
                                println!("recv {}", txt);
                                let mut locked = sockets.write().await;
                                async {
                                    locked
                                        .get_mut(&id)
                                        .ok_or(anyhow::anyhow!("cannot find socket for addr"))
                                }
                                .and_then(|client| async move {
                                    client
                                        .on_recv_with_txt(&ctx, &txt)
                                        .then(|packet| async move {
                                            let txt = serde_json::to_string(&packet)?;
                                            println!("send {}", txt);
                                            ws.send_text(txt).await
                                        })
                                        .await
                                })
                                .await
                            } else {
                                Ok(())
                            }
                        }
                        _ => Ok(()),
                    };

                    if let Err(e) = ret {
                        log::error!(
                            "occur error for socket({}) recved msg : {}",
                            id.to_string(),
                            e.to_string()
                        );
                    }
                }
                .boxed()
            })
            .await?;

        Ok(Self {
            context: ctx,
            server: server,
            sockets: sockets,
        })
    }
}
