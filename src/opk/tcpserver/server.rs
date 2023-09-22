pub struct Config {
    pub addr: String,
    pub eject: chrono::Duration,
    pub life_check: chrono::Duration,
}

struct SocketSet {
    socket: Websocket,
    ping_send: chrono::DateTime<Utc>,
    ping_recv: chrono::DateTime<Utc>,
}
type SocketSetPtr = RwArc<SocketSet>;

impl SocketSet {
    pub fn from(socket: Websocket) -> SocketSetPtr {
        let now = Utc::now();
        let ss = SocketSet {
            socket: socket,
            ping_send: now.clone(),
            ping_recv: now,
        };
        Arc::new(RwLock::new(ss))
    }
    pub fn laytency(&self) -> chrono::Duration {
        if self.ping_send > self.ping_recv {
            Utc::now() - self.ping_send
        } else {
            self.ping_recv - self.ping_send
        }
    }

    pub async fn ping(&mut self) -> Result<()> {
        let now = Utc::now();
        let ping = json!(now.timestamp_millis());
        let payload = serde_json::to_vec(&ping)?;
        self.ping_send = now;
        self.socket.send(Message::Ping(payload)).await
    }

    pub fn get_ping_last(&self) -> &chrono::DateTime<Utc> {
        &self.ping_send
    }

    pub fn update_pong(&mut self, mili: i64) -> bool {
        if mili == self.ping_send.timestamp_millis() {
            self.ping_recv = Utc::now();
            true
        } else {
            false
        }
    }
}
pub struct Server {
    config: Config,

    listener: Arc<TcpListener>,
    sockets: RwArc<HashMap<std::net::SocketAddr, SocketSetPtr>>,
}

impl Server {
    pub async fn find_socket(&self, addr: &std::net::SocketAddr) -> Option<Socket> {
        let locked = self.sockets.read().await;
        let ss = locked.get(addr)?;
        let socket = ss.read().await.socket.clone();
        Some(socket)
    }

    async fn eject_check(
        sockets: RwArc<HashMap<std::net::SocketAddr, SocketSetPtr>>,
        eject: &chrono::Duration,
    ) {
        let now = Utc::now();
        let mut remover = HashMap::<SocketAddr, anyhow::Error>::default();
        let items = sockets
            .read()
            .await
            .iter()
            .map(|(k, v)| v.clone())
            .collect::<Vec<_>>();
        for v in items {
            let mut locked = v.write().await;

            if !locked.socket.is_connected().await || locked.laytency() > *eject {
                let addr = locked.socket.get_addr().as_ref().clone();
                remover.insert(addr, anyhow::anyhow!("occur eject!"));
            } else {
                if let Err(e) = locked.ping().await {
                    let addr = locked.socket.get_addr().as_ref().clone();
                    remover.insert(addr, e);
                }
            }
        }

        let mut locked = sockets.write().await;
        for (k, v) in remover {
            locked.remove(&k);
            log::warn!("occur error for eject check : {}", v.to_string());
        }
    }

    pub async fn new<F>(config: Config, callback: F) -> anyhow::Result<Self>
    where
        F: FnMut(Socket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static + Clone,
    {
        let listener = TcpListener::bind(&config.addr).await?;
        println!("Listening on: {}", "127.0.0.1:9001");
        let sockets = Arc::new(RwLock::new(
            HashMap::<std::net::SocketAddr, SocketSetPtr>::default(),
        ));
        let inner = Server {
            config: config,
            listener: Arc::new(listener),
            sockets: sockets.clone(),
        };

        let cloned_eject = inner.config.eject.clone();
        let wpt_sockets_eject = Arc::downgrade(&sockets);
        tokio::spawn(async move {
            loop {
                if let Some(ptr) = wpt_sockets_eject.upgrade() {
                    Server::eject_check(ptr, &cloned_eject).await;
                } else {
                    break;
                }
            }
        });

        let wpt_listner = Arc::downgrade(&inner.listener);
        let wpt_sockets_accept = Arc::downgrade(&sockets);
        tokio::spawn(async move {
            loop {
                if let Some((listner, sockets)) =
                    wpt_listner.upgrade().zip(wpt_sockets_accept.upgrade())
                {
                    if let Ok((stream, addr)) = listner.accept().await {
                        match Socket::accept(stream, addr, callback.clone()).await {
                            Ok(socket) => {
                                let addr = socket.get_addr().as_ref().clone();
                                let mut locked = sockets.write().await;
                                locked.insert(addr, SocketSet::from(socket));
                            }
                            Err(e) => {
                                log::error!("occur error for handshake : {}", e.to_string());
                            }
                        }
                    }
                }
            }
        });

        Ok(inner)
    }
}
