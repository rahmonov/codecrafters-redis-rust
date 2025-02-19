use std::sync::Arc;

use crate::db::{Db, DbItem};
use crate::repl::{ReplRole, SharedReplicationConfig};
use crate::resp::{Connection, Value};
use crate::Config;
use anyhow::Result;
use tokio::net::TcpStream;
use tokio::sync::broadcast::Sender;
use tokio::time::Instant;

pub async fn handle_connection(
    stream: TcpStream,
    db: Db,
    config: Config,
    shared_repl_conf: SharedReplicationConfig,
    sender: Arc<Sender<Value>>,
) {
    println!("Handling new request...");

    let mut conn = Connection::new(stream);

    loop {
        let value = conn.read_value().await.unwrap();
        let sender = Arc::clone(&sender);

        println!("Got request value {:?}", value);

        let (command, args) = if let Some(v) = value {
            extract_command(v).unwrap()
        } else {
            println!("got nothing, stopping reading");
            break;
        };

        match command.to_uppercase().as_str() {
            "PING" => handle_ping(&mut conn).await,
            "ECHO" => handle_echo(&mut conn, args.first().unwrap().clone()).await,
            "SET" => handle_set(&mut conn, &db, &args, sender).await,
            "GET" => handle_get(&mut conn, &db, args[0].clone()).await,
            "CONFIG" => handle_config(&mut conn, &config, args[0].clone(), args[1].clone()).await,
            "KEYS" => handle_keys(&mut conn, &db).await,
            "INFO" => handle_info(&mut conn, &shared_repl_conf).await,
            "REPLCONF" => handle_replconf(&mut conn).await,
            "PSYNC" => handle_psync(&mut conn, &shared_repl_conf).await,
            c => panic!("Cannot handle command {}", c),
        };

        println!("response has been sent");
    }
}

async fn handle_echo(resp_handler: &mut Connection, what: Value) {
    resp_handler.write_value(what).await.unwrap();
}

async fn handle_ping(resp_handler: &mut Connection) {
    resp_handler
        .write_value(Value::SimpleString("PONG".to_string()))
        .await
        .unwrap();
}

async fn handle_psync(resp_handler: &mut Connection, shared_repl_conf: &SharedReplicationConfig) {
    let repl_conf = shared_repl_conf.lock().await;
    let resp = format!("FULLRESYNC {} 0", repl_conf.master_replid.as_ref().unwrap());
    let resp_val = Value::SimpleString(resp);

    resp_handler.write_value(resp_val).await.unwrap();

    let empty_rdb: Vec<u8> = vec![
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69,
        0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65,
        0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69,
        0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d,
        0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61,
        0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
    ];
    resp_handler
        .write(format!("${}\r\n", empty_rdb.len(),).as_bytes())
        .await
        .unwrap();
    resp_handler.write(&empty_rdb).await.unwrap();
}

async fn handle_replconf(resp_handler: &mut Connection) {
    let resp = Value::SimpleString("OK".to_string());

    resp_handler.write_value(resp).await.unwrap();
}

async fn handle_info(resp_handler: &mut Connection, replication_config: &SharedReplicationConfig) {
    let repl_conf = replication_config.lock().await;
    let mut result_values = vec![format!("role:{}", repl_conf.role)];

    match repl_conf.role {
        ReplRole::Master => {
            let master_replid = repl_conf.master_replid.as_ref().unwrap();
            let master_repl_offset = repl_conf.master_repl_offset.as_ref().unwrap();

            result_values.push(format!("master_replid:{}", master_replid.to_string()));
            result_values.push(format!(
                "master_repl_offset:{}",
                master_repl_offset.to_string()
            ));
        }
        ReplRole::Slave => {}
    }

    let resp = Value::BulkString(result_values.join("\r\n"));

    resp_handler.write_value(resp).await.unwrap();
}

async fn handle_keys(resp_handler: &mut Connection, db: &Db) {
    let db = db.lock().await;

    let resp_val = Value::Array(
        db.keys()
            .map(|key| Value::SimpleString(key.to_string()))
            .collect(),
    );

    resp_handler.write_value(resp_val).await.unwrap();
}

async fn handle_config(
    resp_handler: &mut Connection,
    config: &Config,
    config_command: Value,
    config_key: Value,
) {
    let config_command = unpack_bulk_str(config_command).unwrap();

    let resp_val = match config_command.as_str() {
        "GET" => {
            let config = config.lock().await;
            let config_key_name = unpack_bulk_str(config_key.clone()).unwrap();

            Value::Array(vec![
                config_key,
                Value::BulkString(config.get(&config_key_name).unwrap().to_string()),
            ])
        }
        _ => Value::NullBulkString,
    };

    resp_handler.write_value(resp_val).await.unwrap();
}

async fn handle_set(
    resp_handler: &mut Connection,
    db: &Db,
    args: &[Value],
    sender: Arc<Sender<Value>>,
) {
    let mut db = db.lock().await;

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

    resp_handler
        .write_value(Value::SimpleString("OK".to_string()))
        .await
        .unwrap();
}

async fn handle_get(resp_handler: &mut Connection, db: &Db, key: Value) {
    let db = db.lock().await;
    let key = unpack_bulk_str(key).unwrap();

    let resp_val = match db.get(&key) {
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
    };

    resp_handler.write_value(resp_val).await.unwrap();
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
