use args::ServiceArguments;
use clap::Parser;
use db::{Db, DbItem};
use handlers::handle_connection;
use repl::{ReplConfig, ReplRole, SharedReplicationConfig};
use resp::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

mod args;
mod db;
mod handlers;
mod rdb;
mod repl;
mod resp;

// todo: make it a struct too
type Config = Arc<Mutex<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
    let args = ServiceArguments::parse();

    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let config: Config = Arc::new(Mutex::new(HashMap::new()));

    let port = match args.port {
        Some(port) => port,
        _ => 6379,
    };

    if let (Some(dir), Some(dbfilename)) = (args.dir, args.dbfilename) {
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

        let rdb_contents = rdb::parse_rdb_file(path.to_path_buf()).await.unwrap();
        let mut db = db.lock().unwrap();
        *db = rdb_contents;

        println!("Loaded the RDB file successfully");
    }

    let mut repl_config = ReplConfig::default();

    match args.replicaof {
        Some(replicaof) => {
            let master_addr = match replicaof.split_whitespace().collect::<Vec<_>>() {
                ref parts if parts.len() == 2 => {
                    format!("{}:{}", parts[0], parts[1])
                }
                _ => panic!("invalid master address"),
            };

            if let Ok(mut stream) = TcpStream::connect(&master_addr).await {
                println!("Connected to the master server: {}", master_addr);

                let (reader, mut writer) = stream.split();
                let mut reader = BufReader::new(reader);
                let mut response = String::new();
                // TODO: check results of TCP requests (PONG, OK, OK)

                // Step 1: Send PING
                let ping_cmd = Value::Array(vec![Value::BulkString("PING".to_string())]);
                writer
                    .write_all(ping_cmd.serialize().as_bytes())
                    .await
                    .expect("PING didn't succeed");
                writer.flush().await.unwrap();

                reader
                    .read_line(&mut response)
                    .await
                    .expect("Failed to read PONG");
                println!("Handshake Step 1 [PING] succeeded: {:?}", response);
                response.clear();

                // Step 2.1: Send REPLCONF listening-port <port>
                let replconf_cmd = Value::Array(vec![
                    Value::BulkString("REPLCONF".to_string()),
                    Value::BulkString("listening-port".to_string()),
                    Value::BulkString(port.to_string()),
                ]);
                writer
                    .write_all(replconf_cmd.serialize().as_bytes())
                    .await
                    .expect("REPLCONF with listening port didn't succeed");
                writer.flush().await.unwrap();

                reader
                    .read_line(&mut response)
                    .await
                    .expect("Failed to read PONG");
                println!(
                    "Handshake Step 2.1 [REPLCONF with listening port] succeeded: {:?}",
                    response
                );
                response.clear();

                // Step 2.2: Send REPLCONF capa psync2
                let replconf_cmd = Value::Array(vec![
                    Value::BulkString("REPLCONF".to_string()),
                    Value::BulkString("capa".to_string()),
                    Value::BulkString("psync2".to_string()),
                ]);
                writer
                    .write_all(replconf_cmd.serialize().as_bytes())
                    .await
                    .expect("REPLCONF with capabilities didn't succeed");

                reader
                    .read_line(&mut response)
                    .await
                    .expect("Failed to read PONG");

                println!(
                    "Handshake Step 2.2 [REPLCONF with capabilities] succeeded: {:?}",
                    response
                );
                response.clear();
            } else {
                panic!("couldn't connect to the master server: {}", master_addr);
            }

            repl_config.role = ReplRole::Slave;
        }
        None => {
            repl_config.master_replid =
                Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
            repl_config.master_repl_offset = Some(0);
        }
    }

    let shared_repl_conf: SharedReplicationConfig = Arc::new(Mutex::new(repl_config));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    println!("Ready to roll!");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let db = db.clone();
        let config = config.clone();
        let shared_repl_conf = shared_repl_conf.clone();

        tokio::spawn(async {
            handle_connection(stream, db, config, shared_repl_conf).await;
        });
    }
}
