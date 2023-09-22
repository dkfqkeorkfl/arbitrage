use chrono::Utc;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Protocol {
    Unknown,
    Init,
    Open(String /*what*/),
    Close(String /*id*/),
    Proccess(String /*id*/),
    Subscribed(String /*id*/),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet {
    pub protocol: Protocol,
    pub timestamp: i64,
    pub req: Option<serde_json::Value>,
    pub res: Option<serde_json::Value>,
    pub err: Option<String>,
}

impl Packet {
    pub fn res(
        protocol: Protocol,
        result: serde_json::Value,
        req: Option<serde_json::Value>,
    ) -> Self {
        Packet {
            protocol: protocol,
            timestamp: Utc::now().timestamp_millis(),
            req: req,
            res: Some(result),
            err: Default::default(),
        }
    }

    pub fn err(protocol: Protocol, error: String, req: Option<serde_json::Value>) -> Self {
        Packet {
            protocol: protocol,
            timestamp: Utc::now().timestamp_millis(),
            req: req,
            res: Default::default(),
            err: Some(error),
        }
    }
}
