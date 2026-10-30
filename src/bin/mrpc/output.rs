use super::{Failure, args::Output};
use msgpack_rpc_peer::Value;
use serde_json::{Map, Value as Json, json};
use tokio::io::AsyncWriteExt;

// Conversion is explicit: Serde can silently coerce special MessagePack values.
fn json_value(value: &Value) -> Option<Json> {
    Some(match value {
        Value::Nil => Json::Null,
        Value::Boolean(value) => Json::Bool(*value),
        Value::Integer(value) => {
            if let Some(value) = value.as_i64() {
                value.into()
            } else {
                value.as_u64()?.into()
            }
        }
        Value::F32(value) => serde_json::Number::from_f64(f64::from(*value))?.into(),
        Value::F64(value) => serde_json::Number::from_f64(*value)?.into(),
        Value::String(value) => value.as_str()?.into(),
        Value::Array(values) => Json::Array(values.iter().map(json_value).collect::<Option<_>>()?),
        Value::Map(values) => {
            let mut map = Map::new();
            for (key, value) in values {
                let key = key.as_str()?;
                if map.contains_key(key) {
                    return None;
                }
                map.insert(key.into(), json_value(value)?);
            }
            Json::Object(map)
        }
        Value::Binary(_) | Value::Ext(_, _) => return None,
    })
}
pub(super) fn record(
    kind: &str,
    method: Option<&str>,
    value: Value,
) -> Result<Json, super::Failure> {
    let (encoding, value) = if let Some(value) = json_value(&value) {
        ("json", value)
    } else {
        let bytes = msgpack_rpc_peer::encode_value(&value, &msgpack_rpc_peer::Config::default())?;
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        ("msgpack-hex", Json::String(hex))
    };
    let mut record = json!({"type": kind, "encoding": encoding});
    if let Some(method) = method {
        record["method"] = method.into();
        record["params"] = value;
    } else {
        record["value"] = value;
    }
    Ok(record)
}

pub(super) async fn emit(
    kind: &str,
    method: Option<&str>,
    value: Value,
    format: Output,
    error: bool,
) -> Result<(), Failure> {
    let record = record(kind, method, value)?;
    let text = match format {
        Output::Compact => serde_json::to_string(&record)?,
        Output::Pretty => serde_json::to_string_pretty(&record)?,
    };
    let bytes = format!("{text}\n");
    if error {
        let mut output = tokio::io::stderr();
        output.write_all(bytes.as_bytes()).await?;
        output.flush().await?;
    } else {
        let mut output = tokio::io::stdout();
        output.write_all(bytes.as_bytes()).await?;
        output.flush().await?;
    }
    Ok(())
}
