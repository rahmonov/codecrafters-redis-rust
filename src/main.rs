use anyhow::Result;
use resp::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};

mod resp;

type Db = Arc<Mutex<HashMap<String, (String, Option<Duration>)>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let db = db.clone();

        tokio::spawn(async {
            handle_connection(stream, db).await;
        });
    }
}

async fn handle_connection(stream: TcpStream, db: Db) {
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
                c => panic!("Cannot handle command {}", c),
            }
        } else {
            break;
        };

        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();
    }
}

fn handle_set(db: &Db, args: &[Value]) -> Value {
    let mut db = db.lock().unwrap();

    let key = unpack_bulk_str(args[0].clone()).unwrap();
    let value = unpack_bulk_str(args[1].clone()).unwrap();

    let px = if args.len() > 2 {
        let command_name = unpack_bulk_str(args[2].clone()).unwrap();
        if command_name != "px" {
            panic!("wrong precision name!");
        }

        let ms_str = unpack_bulk_str(args[3].clone()).unwrap();
        let ms = ms_str.parse::<u64>().unwrap();
        let now = SystemTime::now();
        let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        Some(since_the_epoch + Duration::from_millis(ms))
    } else {
        None
    };

    db.insert(key, (value, px));

    Value::SimpleString("OK".to_string())
}

fn handle_get(db: &Db, key: Value) -> Value {
    let db = db.lock().unwrap();
    let key = unpack_bulk_str(key).unwrap();

    match db.get(&key) {
        Some((value, expiry_time)) => {
            let result = Value::BulkString(value.to_string());

            match expiry_time {
                Some(expiry_time) => {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

                    if expiry_time < &now {
                        return Value::NullBulkString;
                    }

                    result
                }
                None => result,
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
