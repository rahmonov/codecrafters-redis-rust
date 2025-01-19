use anyhow::Result;
use resp::Value;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};

mod db;
mod resp;

#[derive(Debug)]
pub struct DbItem {
    value: String,
    created: Instant,
    expires: usize,
}

impl DbItem {
    fn default() -> Self {
        Self {
            value: String::default(),
            expires: 0,
            created: Instant::now(),
        }
    }
}

type Db = Arc<Mutex<HashMap<String, DbItem>>>;
type Config = Arc<Mutex<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let args: Vec<String> = env::args().collect();

    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let config: Config = Arc::new(Mutex::new(HashMap::new()));

    if args.len() > 2 && (args[1] == "--dir" || args[3] == "--dbfilename") {
        let dir = args[2].to_string();
        let dbfilename = args[4].to_string();

        config
            .lock()
            .unwrap()
            .insert("dir".to_string(), dir.clone());
        config
            .lock()
            .unwrap()
            .insert("dbfilename".to_string(), dbfilename.clone());

        let filename = format!("{dir}/{dbfilename}");
        let path = Path::new(&filename);

        let keys = db::get_rdb_keys(path.to_path_buf()).await.unwrap();

        keys.iter().for_each(|key| {
            db.lock().unwrap().insert(key.clone(), DbItem::default());
        });
    }

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let db = db.clone();
        let config = config.clone();

        tokio::spawn(async {
            handle_connection(stream, db, config).await;
        });
    }
}

async fn handle_connection(stream: TcpStream, db: Db, config: Config) {
    let mut handler = resp::RespHandler::new(stream);
    println!("accepted new connection");

    loop {
        let value = handler.read_value().await.unwrap();

        println!("Got value {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
                "ECHO" => args.first().unwrap().clone(),
                "SET" => handle_set(&db, &args),
                "GET" => handle_get(&db, args[0].clone()),
                "CONFIG" => handle_config(&config, args[0].clone(), args[1].clone()),
                "KEYS" => handle_keys(&db),
                c => panic!("Cannot handle command {}", c),
            }
        } else {
            break;
        };

        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();
    }
}

fn handle_keys(db: &Db) -> Value {
    println!("inside the keys command handler");
    let db = db.lock().unwrap();

    Value::Array(
        db.keys()
            .map(|key| Value::SimpleString(key.to_string()))
            .collect(),
    )
}

fn handle_config(config: &Config, config_command: Value, config_key: Value) -> Value {
    let config_command = unpack_bulk_str(config_command).unwrap();

    match config_command.as_str() {
        "GET" => {
            let config = config.lock().unwrap();
            let config_key_name = unpack_bulk_str(config_key.clone()).unwrap();

            Value::Array(vec![
                config_key,
                Value::BulkString(config.get(&config_key_name).unwrap().to_string()),
            ])
        }
        _ => Value::NullBulkString,
    }
}

fn handle_set(db: &Db, args: &[Value]) -> Value {
    let mut db = db.lock().unwrap();

    let key = unpack_bulk_str(args[0].clone()).unwrap();
    let value = unpack_bulk_str(args[1].clone()).unwrap();

    let expires = if args.len() > 2 {
        let command_name = unpack_bulk_str(args[2].clone()).unwrap();
        if command_name != "px" {
            panic!("wrong precision name!");
        }

        let ms_str = unpack_bulk_str(args[3].clone()).unwrap();
        ms_str.parse::<usize>().unwrap()
    } else {
        0
    };

    let item = DbItem {
        value,
        expires,
        created: Instant::now(),
    };

    db.insert(key, item);

    Value::SimpleString("OK".to_string())
}

fn handle_get(db: &Db, key: Value) -> Value {
    let db = db.lock().unwrap();
    let key = unpack_bulk_str(key).unwrap();

    match db.get(&key) {
        Some(db_item) => {
            let result = Value::BulkString(db_item.value.to_string());

            let is_expired = db_item.expires > 0
                && db_item.created.elapsed().as_millis() > db_item.expires as u128;

            match is_expired {
                true => Value::NullBulkString,
                false => result,
            }
        }
        None => Value::NullBulkString,
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
