use std::fmt::{Display, Formatter};
use redis::{Client, Connection, Commands, RedisResult, FromRedisValue, Value};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
struct TimeData {
    id: usize,
    owner: String,
    message: String,
}

impl Display for TimeData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}) [{}] {}", self.id, self.owner, self.message)
    }
}

impl TimeData {
    fn into_redis_args(&self) -> Vec<(String, String)> {
        vec![
            ("id".to_owned(), self.id.to_string()),
            ("owner".to_owned(), self.owner.clone()),
            ("message".to_owned(), self.message.clone()),
        ]
    }

    fn from_fields(fields: HashMap<String, String>) -> Result<Self, &'static str> {
        let id = fields.get("id")
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or("Missing or invalid id field")?;

        let owner = fields.get("owner")
            .ok_or("Missing owner field")?
            .clone();

        let message = fields.get("message")
            .ok_or("Missing message field")?
            .clone();

        Ok(TimeData { id, owner, message })
    }
}

#[derive(Debug)]
struct StreamEntry {
    id: String,
    fields: HashMap<String, String>,
}

impl FromRedisValue for StreamEntry {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        if let Value::Bulk(entries) = v {
            let id = match &entries[0] {
                Value::Data(data) => String::from_utf8_lossy(data).to_string(),
                _ => return Err(redis::RedisError::from((redis::ErrorKind::TypeError, "Invalid ID"))),
            };

            let mut fields = HashMap::new();
            if let Value::Bulk(pairs) = &entries[1] {
                for i in (0..pairs.len()).step_by(2) {
                    let key = match &pairs[i] {
                        Value::Data(data) => String::from_utf8_lossy(data).to_string(),
                        _ => continue,
                    };
                    let value = match &pairs[i + 1] {
                        Value::Data(data) => String::from_utf8_lossy(data).to_string(),
                        _ => continue,
                    };
                    fields.insert(key, value);
                }
            }

            Ok(StreamEntry { id, fields })
        } else {
            Err(redis::RedisError::from((redis::ErrorKind::TypeError, "Invalid entry format")))
        }
    }
}


fn main() -> RedisResult<()> {
    let td = TimeData {
        id: 1,
        owner: "Taro".into(),
        message: "Hello world".into(),
    };

    println! {"{}", td};

    let client: Client = redis::Client::open("redis://localhost").unwrap();
    let mut con: Connection = client.get_connection().unwrap();
    let args: Vec<(String, String)> = td.into_redis_args();
    let _: () = con.xadd("test", "*", &args)?;

    println!("{:?}", client);

    let stream_entries: Vec<StreamEntry> = con.xrange("test", "-", "+").unwrap_or(vec![]);

    stream_entries.iter().for_each(|se| {
        match TimeData::from_fields(se.fields.clone()) {
            Ok(time_data) => {
                println!("{} {}", se.id, time_data);
            }
            _ => {
                println!("unknown data...");
            }
        }
    });

    Ok(())
}
