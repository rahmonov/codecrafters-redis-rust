use std::sync::Arc;

use crate::connection::Connection;
use crate::db::{Db, DbItem};
use crate::frame::Frame;
use crate::replication::{ReplRole, SharedReplicationConfig};
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
    sender: Arc<Sender<Frame>>,
) {
    println!("Handling new request...");

    let mut conn = Connection::new(stream);

    loop {
        let value = conn.read_frame().await.unwrap();
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

async fn handle_echo(conn: &mut Connection, what: Frame) {
    conn.write_frame(what).await.unwrap();
}

async fn handle_ping(conn: &mut Connection) {
    conn.write_frame(Frame::SimpleString("PONG".to_string()))
        .await
        .unwrap();
}

async fn handle_psync(conn: &mut Connection, shared_repl_conf: &SharedReplicationConfig) {
    let repl_conf = shared_repl_conf.lock().await;
    let resp = format!("FULLRESYNC {} 0", repl_conf.master_replid.as_ref().unwrap());
    let resp_val = Frame::SimpleString(resp);

    conn.write_frame(resp_val).await.unwrap();

    let empty_rdb: Vec<u8> = vec![
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69,
        0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65,
        0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69,
        0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d,
        0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61,
        0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
    ];
    conn.write(format!("${}\r\n", empty_rdb.len(),).as_bytes())
        .await
        .unwrap();
    conn.write(&empty_rdb).await.unwrap();
}

async fn handle_replconf(conn: &mut Connection) {
    let resp = Frame::SimpleString("OK".to_string());

    conn.write_frame(resp).await.unwrap();
}

async fn handle_info(conn: &mut Connection, replication_config: &SharedReplicationConfig) {
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

    let resp = Frame::BulkString(result_values.join("\r\n"));

    conn.write_frame(resp).await.unwrap();
}

async fn handle_keys(conn: &mut Connection, db: &Db) {
    let db = db.lock().await;

    let resp_val = Frame::Array(
        db.keys()
            .map(|key| Frame::SimpleString(key.to_string()))
            .collect(),
    );

    conn.write_frame(resp_val).await.unwrap();
}

async fn handle_config(
    conn: &mut Connection,
    config: &Config,
    config_command: Frame,
    config_key: Frame,
) {
    let config_command = unpack_bulk_str(config_command).unwrap();

    let resp_val = match config_command.to_uppercase().as_str() {
        "GET" => {
            let config = config.lock().await;
            let config_key_name = unpack_bulk_str(config_key.clone()).unwrap();

            Frame::Array(vec![
                config_key,
                Frame::BulkString(config.get(&config_key_name).unwrap().to_string()),
            ])
        }
        _ => Frame::NullBulkString,
    };

    conn.write_frame(resp_val).await.unwrap();
}

async fn handle_set(conn: &mut Connection, db: &Db, args: &[Frame], sender: Arc<Sender<Frame>>) {
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

    conn.write_frame(Frame::SimpleString("OK".to_string()))
        .await
        .unwrap();
}

async fn handle_get(conn: &mut Connection, db: &Db, key: Frame) {
    let db = db.lock().await;
    let key = unpack_bulk_str(key).unwrap();

    let frame = match db.get(&key) {
        Some(db_item) => {
            let result = Frame::BulkString(db_item.value.to_string());

            let is_expired = db_item.expires > 0
                && db_item.created.elapsed().as_millis() > db_item.expires as u128;

            match is_expired {
                true => Frame::NullBulkString,
                false => result,
            }
        }
        None => Frame::NullBulkString,
    };

    conn.write_frame(frame).await.unwrap();
}

fn extract_command(frame: Frame) -> Result<(String, Vec<Frame>)> {
    match frame {
        Frame::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(frame: Frame) -> Result<String> {
    match frame {
        Frame::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
