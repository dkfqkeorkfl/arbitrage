pub mod services;
pub mod cache;
pub mod exchange;
pub mod exchanges;
pub mod float;
pub mod json;
pub mod leveldb_str;
pub mod protocols;
pub mod webserver;
pub mod websocket;

use anyhow::Ok;
use serde::de;
use tokio::process::Command;

pub async fn download_google_sheet(
    sheet: &str,
    tab: &str,
    key: &str,
) -> anyhow::Result<serde_json::Value> {
    let url = format!(
        "https://sheets.googleapis.com/v4/spreadsheets/{}/values/{}?key={}",
        sheet, tab, key,
    );

    let res = reqwest::Client::new().get(url).send().await?;
    let json = res.json().await?;
    return Ok(json);
}

pub async fn download_encrypted_sheet<T>(
    sheet: &str,
    tab: &str,
    key: &str,
) -> anyhow::Result<T>
where
T: for<'de> de::Deserialize<'de>,
{
    let cmd = Command::new("node")
        .args(["main.js", sheet, tab, key, key])
        .output()
        .await?;
    let err = String::from_utf8(cmd.stderr)?;
    if err.is_empty() == false {
        return Err(anyhow::anyhow!(err));
    }

    let out = String::from_utf8(cmd.stdout)?;
    let value = serde_json::from_str::<T>(&out)?;
    Ok(value)
}
