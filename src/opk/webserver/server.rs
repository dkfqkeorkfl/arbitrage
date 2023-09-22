use std::convert::Infallible;
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::{collections::HashMap, sync::Arc};

use crate::opk::protocols::{RwArc, Signal};
use crate::opk::websocket::{ReciveCallback, Websocket, WebsocketParam};

use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::sync::Mutex;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};
use hyper::*;
use super::firewall::Firewall;
// https://www.runit.cloud/2020/04/https-ssl.html

type TlsListener = tls_listener::TlsListener<
            tls_listener::hyper::WrappedAccept<server::conn::AddrIncoming>,
            tokio_rustls::TlsAcceptor,
        >;

struct SocketSet {
    socket: Websocket,
}
type SocketSetPtr = RwArc<SocketSet>;

impl SocketSet {
    pub fn new(socket: Websocket) -> SocketSetPtr {
        let ss = SocketSet { socket: socket };
        Arc::new(RwLock::new(ss))
    }
}

pub struct Param {
    pub addr: IpAddr,
    pub cert: String,
    pub key: String,

    pub eject: chrono::Duration,
    pub ping_interval: chrono::Duration,
}

pub struct Inner {
    config: Arc<Param>,
    sockets: RwArc<HashMap<i64, SocketSetPtr>>,
}
type InnerPtr = Arc<Inner>;

impl Inner {
    pub fn new(config: Param) -> Self {
        Inner {
            config: Arc::new(config),
            sockets: Arc::new(RwLock::new(HashMap::<i64, SocketSetPtr>::default())),
        }
    }
}

#[derive(Clone)]
pub struct RequestCallback {
    pub callback: Arc<
        Mutex<
            dyn FnMut(Request<Body>) -> BoxFuture<'static, anyhow::Result<Response<Body>>>
                + Send
                + Sync
                + 'static,
        >,
    >,
}

impl RequestCallback {
    pub fn new<F>(f: F) -> Self
    where
        F: FnMut(Request<Body>) -> BoxFuture<'static, anyhow::Result<Response<Body>>>
            + Send
            + Sync
            + 'static,
    {
        RequestCallback {
            callback: Arc::new(Mutex::new(f)),
        }
    }
}

pub struct Server {
    inner: InnerPtr,
}

impl Server {
    fn load_tls_acceptor(cert : &str, key : &str) -> anyhow::Result<TlsAcceptor> {
        let cert_file = File::open(&cert).and_then(|file| {
            rustls_pemfile::certs(&mut BufReader::new(file))
                .map(|mut certs| certs.drain(..).map(Certificate).collect())
        })?;
        let mut private_file = File::open(&key).and_then(|file| {
            rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(file))
                .map(|mut keys| keys.drain(..).map(PrivateKey).collect::<Vec<_>>())
        })?;

        let mut config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert_file, private_file.remove(0))?;
        // http/1.1가 있어야 websocket 소통이 가능
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        // config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    pub async fn new<F>(param: Param, port: u16, callback: F) -> anyhow::Result<Self>
    where
        F: FnMut(Request<Body>) -> BoxFuture<'static, anyhow::Result<Response<Body>>>
            + Send
            + Sync
            + 'static,
    {
        let callback_ptr = Arc::new(Mutex::new(callback));
        let addr = SocketAddr::new(param.addr.clone(), port);

        let make_svc = service::make_service_fn(move |conn: &tokio_rustls::server::TlsStream<server::conn::AddrStream>| {
            let addr = conn.get_ref().0.remote_addr();
            let cloned_callback = callback_ptr.clone();
            let cloned = move |mut req: Request<Body>| {
                req.extensions_mut().insert(addr);
                log::debug!("recived https request : {:?}", req);
                let callback = cloned_callback.clone();
                async move {
                    let mut locked = callback.lock().await;
                    let res = match locked(req).await {
                        Ok(res) => res,
                        Err(e) => Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(e.to_string().into())
                            .unwrap_or_else(|e| Response::<Body>::new(e.to_string().into())),
                    };

                    Ok::<_, Infallible>(res)
                }
                .boxed()
            };

            async move {
                Ok::<_, Infallible>(service::service_fn(cloned))
            }
            .boxed()
        });

        let acceptor = Server::load_tls_acceptor(&param.cert, &param.key)?;
        let listener = TlsListener::new_hyper(acceptor, server::conn::AddrIncoming::bind(&addr)?);
        let mut server = hyper::server::Server::builder(listener).serve(make_svc);

        log::info!("success that open webserver : addr({})", addr.to_string());
        let inner = Arc::new(Inner::new(param));
        let cloned_inner = Arc::downgrade(&inner);
        tokio::spawn(async move {
            loop {
                if cloned_inner.upgrade().is_none() {
                    break;
                }

                if let Err(e) = (&mut server).await {
                    log::error!("server error: {}", e);
                }
            }
        });
        Ok(Server { inner: inner })
    }

    fn parse_urlcode(str: &str) -> anyhow::Result<HashMap<String, String>> {
        str.split('&')
            .map(|part| {
                let mut split = part.split('=');
                let one = split.next().ok_or(anyhow::anyhow!("invalid query key"))?;
                let two = split.next().ok_or(anyhow::anyhow!("invalid query data"))?;
                Ok((one.to_string(), two.to_string()))
            })
            .collect::<anyhow::Result<HashMap<_, _>>>()
    }

    pub async fn open_websocket<F>(&self, port: u16, callback: F) -> anyhow::Result<Firewall>
    where
        F: FnMut(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let addr = SocketAddr::new(self.inner.config.addr, port);
        log::info!("trying that open websocket : addr({})", addr.to_string());

        let raw_acceptor = Server::load_tls_acceptor(&self.inner.config.cert, &self.inner.config.key)?;
        let raw_listener = TcpListener::bind(addr).await?;

        log::info!("success that open websocket : addr({})", addr.to_string());
        let acceptor = Arc::new(raw_acceptor);
        let listener = Arc::new(raw_listener);
        let cloned_inner = Arc::downgrade(&self.inner);

        let recive_callback = ReciveCallback::new(callback);
        let eject = self.inner.config.eject.clone();
        let life_check = self.inner.config.ping_interval.clone();
        let sockets_wpt = Arc::downgrade(&self.inner.sockets);

        let firewall = Firewall::new();
        let firewall_weak = firewall.weak();
        tokio::spawn(async move {
            let accept = move || {
                let cloned_listener = listener.clone();
                let cloned_acceptor = acceptor.clone();
                let cloned_callback = recive_callback.clone();
                let cloned_firewall = firewall_weak.clone();
                async move {
                    let (stream, addr) = cloned_listener.accept().await?;
                    log::info!("trying that accept websocket : addr({})", addr.to_string());
                    let tls = cloned_acceptor.accept(stream).await?;
                    let mut ws = Websocket::default();

                    let param = WebsocketParam {
                        url: addr.to_string(),
                        protocol: Default::default(),
                        header: Default::default(),
                        eject: eject.clone(),
                        ping_interval: life_check.clone(),
                    };

                    let token_option = if let Some(firewall) = cloned_firewall.strong() {
                        firewall.get(&addr.ip()).await
                    } else {
                        Some(Default::default())
                    };

                    let stream: tokio_tungstenite::WebSocketStream<
                        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
                    > = tokio_tungstenite::accept_hdr_async(
                        tls,
                        move |req: &'_ hyper::Request<()>,
                              res: hyper::Response<()>|
                              -> std::result::Result<
                            hyper::Response<()>,
                            hyper::Response<
                                std::option::Option<std::string::String>,
                            >,
                        > {
                            let is_valid = token_option
                                .and_then(|token| {
                                    if token.is_empty() {
                                        return Some(true);
                                    }

                                    req.uri()
                                        .query()
                                        .and_then(|str| {
                                            let params = Server::parse_urlcode(str).ok()?;
                                            params.get("token").cloned()
                                        })
                                        .map(|key| key == token)
                                })
                                .unwrap_or(false);

                            log::info!(
                                "hdr result is {} : url({})",
                                is_valid,
                                req.uri().to_string()
                            );
                            if is_valid {
                                std::result::Result::Ok(res)
                            } else {
                                // let mut a = simple_hyper_server_tls::hyper::Response::<
                                // std::option::Option::<std::string::String>>::new(Some("test".to_string()));
                                std::result::Result::Err(Default::default())
                            }
                        },
                    )
                    .await?;
                    ws.accept(param, stream, &cloned_callback).await?;
                    log::info!("success that accept websocket : addr({})", addr.to_string());
                    Ok::<_, anyhow::Error>(ws)
                }
                .boxed()
            };

            loop {
                if let Some((inner, sockets)) = cloned_inner.upgrade().zip(sockets_wpt.upgrade()) {
                    match accept().await {
                        Ok(ws) => {
                            let mut locked = sockets.write().await;
                            let sid = ws.get_id().await;
                            locked.insert(sid.clone(), SocketSet::new(ws.clone()));
                        }
                        Err(e) => {
                            log::error!("occur error for accept websocket : {}", e.to_string());
                        }
                    }
                } else {
                    break;
                }
            }
        });

        Ok(firewall)
    }
}
