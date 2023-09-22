use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Result;
use chrono::Utc;
use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    Mutex, RwLock,
};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::opk::protocols::*;

#[derive(Debug)]
struct Inner {
    sender: UnboundedSender<Message>,
    is_connect: AtomicBool,
    created: i64,
}
pub type SocketInnerPtr = Arc<RwLock<Inner>>;

pub struct ReciveCallback {
    pub callback: Arc<
        Mutex<dyn FnMut(SocketInnerPtr, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
    >,
}

impl ReciveCallback {
    pub fn new<F>(f: F) -> Self
    where
        F: FnMut(SocketInnerPtr, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        ReciveCallback {
            callback: Arc::new(Mutex::new(f)),
        }
    }
}

impl Inner {
    
    pub fn get_id(&self) -> i64 {
        self.created
    }

    pub async fn send(&mut self, message: Message) -> Result<()> {
        self.sender.send(message).map_err(anyhow::Error::new)
    }

    pub async fn close(&mut self) {
        self.is_connect.store(false, Ordering::Relaxed);
    }

    pub fn is_connected(&self) -> bool {
        self.is_connect.load(Ordering::Acquire)
    }

    pub async fn accept<F>(
        stream: tokio::net::TcpStream,
        callback: F,
    ) -> anyhow::Result<SocketInnerPtr>
    where
        F: FnMut(SocketInnerPtr, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let now = Utc::now();
        let ws_stream = accept_async(stream)
            .await
            .expect("Error during the websocket handshake occurred");

        let (mut write, mut read) = ws_stream.split();
        let (sender, mut receiver) = unbounded_channel::<Message>();
        let s = Arc::from(RwLock::new(Self {
            sender: sender,
            is_connect: AtomicBool::new(true),
            created: now.timestamp_millis(),
        }));

        let wpt_for_write = Arc::downgrade(&s);
        let callback_for_read = Arc::new(Mutex::new(callback));
        let callback_for_write = callback_for_read.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                if let Some(spt) = wpt_for_write.upgrade() {
                    if spt.read().await.is_connected() {
                        if let Err(e) = write.send(message).await {
                            let mut callback = callback_for_write.lock().await;
                            callback(spt, Signal::Error(e)).await;
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        });

        let wpt_for_read = Arc::downgrade(&s);
        tokio::spawn(async move {
            if let Some(spt) = wpt_for_read.upgrade() {
                let mut callback = callback_for_read.lock().await;
                callback(spt, Signal::Opened).await;
            }

            while let Some(message) = read.next().await {
                if let Some(spt) = wpt_for_read.upgrade() {
                    if !spt.read().await.is_connected() {
                        break;
                    }

                    let mut callback = callback_for_read.lock().await;
                    let msg = match message {
                        std::result::Result::Ok(m) => {
                            match &m {
                                Message::Pong(_) => {}
                                _ => {
                                    log::trace!("recved : {:?}", m)
                                }
                            }
                            Signal::Recived(m)
                        }
                        Err(e) => Signal::Error(e),
                    };
                    callback(spt, msg).await;
                } else {
                    break;
                }
            }

            if let Some(spt) = wpt_for_read.upgrade() {
                let ptr = spt.write().await;
                ptr.is_connect.store(false, Ordering::SeqCst);
                drop(ptr);

                let mut callback = callback_for_read.lock().await;
                callback(spt, Signal::Closed).await;
            }
        });

        Ok(s)
    }
}

#[derive(Debug, Clone)]
pub struct Socket {
    inner: SocketInnerPtr,
    addr : Arc<std::net::SocketAddr>
}

impl Socket {
    pub fn get_addr(&self) -> &Arc<std::net::SocketAddr> {
        &self.addr
    }
    pub async fn get_id(&self) -> i64 {
        self.inner.read().await.get_id()
    }

    pub async fn send(&self, message: Message) -> Result<()> {
        self.inner.write().await.send(message).await
    }

    pub async fn send_text(&self, text: String) -> Result<()> {
        self.send(Message::Text(text)).await
    }

    pub async fn close(&self) {
        self.inner.write().await.close().await;
    }

    pub async fn is_connected(&self) -> bool {
        self.inner.read().await.is_connected()
    }

    pub async fn accept<F>(
        stream: tokio::net::TcpStream,
        addr: std::net::SocketAddr,
        callback: F,
    ) -> anyhow::Result<Self>
    where
        F: FnMut(Socket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let addr_ptr = Arc::new(addr);

        let addr_ptr_for_callback = addr_ptr.clone();
        let callback_ptr = Arc::new(Mutex::new(callback));
        let inner = Inner::accept(stream, move |inner, signal| {
            let cloned_callback = callback_ptr.clone();
            let cloned_addr = addr_ptr_for_callback.clone();
            async move {
                let mut locked = cloned_callback.lock().await;
                locked(Socket { inner: inner, addr : cloned_addr }, signal).await;
            }
            .boxed()
        })
        .await?;
        Ok(Socket { inner: inner, addr : addr_ptr })
    }
}
