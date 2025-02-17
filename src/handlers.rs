use crate::db::{Db, DbItem};
use crate::repl::{ReplRole, SharedReplicationConfig};
use crate::resp::{RespHandler, Value};
use crate::Config;
use anyhow::Result;
use tokio::net::TcpStream;
use tokio::time::Instant;

pub async fn handle_connection(
    stream: TcpStream,
    db: Db,
    config: Config,
    shared_repl_conf: SharedReplicationConfig,
) {
    let mut handler = RespHandler::new(stream);
    println!("Handling new request...");

    loop {
        let value = handler.read_value().await.unwrap();

        println!("Got request value {:?}", value);

        let (command, args) = if let Some(v) = value {
            extract_command(v).unwrap()
        } else {
            println!("got nothing, stopping reading");
            break;
        };

        let response = match command.to_uppercase().as_str() {
            "PING" => Value::SimpleString("PONG".to_string()),
            "ECHO" => args.first().unwrap().clone(),
            "SET" => handle_set(&db, &args),
            "GET" => handle_get(&db, args[0].clone()),
            "CONFIG" => handle_config(&config, args[0].clone(), args[1].clone()),
            "KEYS" => handle_keys(&db),
            "INFO" => handle_info(&shared_repl_conf),
            "REPLCONF" => handle_replconf(),
            "PSYNC" => handle_psync(&shared_repl_conf),
            c => panic!("Cannot handle command {}", c),
        };

        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();

        // psync post-processing
        // todo: refactor all handlers to write themselves
        if command.to_uppercase().as_str() == "PSYNC" {
            let empty_rdb: Vec<u8> = vec![
                0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
                0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
                0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
                0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
                0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
                0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
                0xc0, 0xff, 0x5a, 0xa2,
            ];
            handler
                .write(format!("${}\r\n", empty_rdb.len(),).as_bytes())
                .await
                .unwrap();
            handler.write(&empty_rdb).await.unwrap();
        }
    }
}

fn handle_psync(shared_repl_conf: &SharedReplicationConfig) -> Value {
    let repl_conf = shared_repl_conf.lock().unwrap();
    let resp = format!("FULLRESYNC {} 0", repl_conf.master_replid.as_ref().unwrap());
    Value::SimpleString(resp)
}

fn handle_replconf() -> Value {
    Value::SimpleString("OK".to_string())
}

pub fn handle_info(replication_config: &SharedReplicationConfig) -> Value {
    let repl_conf = replication_config.lock().unwrap();
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

    Value::BulkString(result_values.join("\r\n"))
}

pub fn handle_keys(db: &Db) -> Value {
    let db = db.lock().unwrap();

    Value::Array(
        db.keys()
            .map(|key| Value::SimpleString(key.to_string()))
            .collect(),
    )
}

pub fn handle_config(config: &Config, config_command: Value, config_key: Value) -> Value {
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

pub fn handle_set(db: &Db, args: &[Value]) -> Value {
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

pub fn handle_get(db: &Db, key: Value) -> Value {
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
