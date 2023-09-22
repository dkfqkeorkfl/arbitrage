use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Weak},
};

use chrono::Utc;
use tokio::sync::RwLock;

use crate::opk::protocols::RwArc;

struct Wall {
    token: String,
    timestamp: chrono::DateTime<Utc>,
}

struct Inner {
    walls: HashMap<IpAddr, Wall>,
    expire: chrono::Duration,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            walls: Default::default(),
            expire: chrono::Duration::seconds(5),
        }
    }
}

#[derive(Clone)]
pub struct Firewall {
    inner: RwArc<Inner>,
}

#[derive(Clone)]
pub struct FirewallWeak {
    inner: Weak<RwLock<Inner>>,
}

impl FirewallWeak {
    pub fn strong(&self) -> Option<Firewall> {
        let inner = self.inner.upgrade()?;
        Some(Firewall { inner: inner })
    }
}

impl Firewall {
    pub fn new() -> Self {
        let inner = Arc::new(RwLock::new(Inner::default()));
        Self { inner: inner }
    }

    pub async fn set_expire(&self, expire: chrono::Duration) {
        let mut locked = self.inner.write().await;
        locked.expire = expire;
    }

    pub async fn update(&self, ip: IpAddr, token: String) {
        let wall = Wall {
            token,
            timestamp: Utc::now(),
        };

        let mut locked = self.inner.write().await;
        locked.walls.insert(ip, wall);
    }

    pub async fn get(&self, ip: &IpAddr) -> Option<String> {
        let locked = self.inner.read().await;
        locked.walls.get(ip).and_then(|wall| {
            if wall.timestamp + locked.expire > Utc::now() {
                Some(wall.token.clone())
            } else {
                None
            }
        })
    }

    pub async fn check(&self, ip: &IpAddr, token: &str) -> bool {
        let locked = self.inner.read().await;
        locked
            .walls
            .get(ip)
            .filter(|wall| wall.timestamp + locked.expire > Utc::now() && wall.token == token)
            .map(|_| true)
            .unwrap_or(false)
    }

    pub fn weak(&self) -> FirewallWeak {
        let wpt = Arc::downgrade(&self.inner);
        FirewallWeak { inner: wpt }
    }
}
