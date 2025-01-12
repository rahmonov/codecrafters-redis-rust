use resp::Value;
use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod resp;

type Db = Arc<Mutex<HashMap<String, String>>>;

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
                "SET" => {
                    let mut db = db.lock().unwrap();

                    let key = unpack_bulk_str(args[0].clone()).unwrap();
                    let value = unpack_bulk_str(args[1].clone()).unwrap();

                    db.insert(key, value);

                    Value::SimpleString("OK".to_string())
                }
                "GET" => {
                    let db = db.lock().unwrap();
                    let key = unpack_bulk_str(args.first().unwrap().clone()).unwrap();

                    if let Some(value) = db.get(&key) {
                        Value::BulkString(value.to_string())
                    } else {
                        Value::NullBulkString
                    }
                }
                c => panic!("Cannot handle command {}", c)
            }
        } else {
            break;
        };

        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            ))
        }
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}