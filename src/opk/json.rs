use anyhow::*;

pub fn url_encode(v: &serde_json::Value) -> Result<String> {
    if let Option::Some(datas) = v.as_object() {
        let params = datas
            .iter()
            .map(|v| {
                let v1 = if v.1.is_string() {
                    v.1.as_str().map(|v| v.to_string()).unwrap()
                } else {
                    v.1.to_string()
                };
                (v.0, v1)
            })
            .collect::<Vec<(&String, String)>>();

        let urlcode = serde_urlencoded::to_string(params)?;
        return Ok(urlcode);
    }

    return Err(anyhow::anyhow!("invalid param"));
}
