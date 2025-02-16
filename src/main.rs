use args::ServiceArguments;
use clap::Parser;
use db::{Db, DbItem};
use handlers::handle_connection;
use repl::{ReplConfig, ReplRole, SharedReplicationConfig};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

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

    // todo: build the inner type first and only then create the shared types
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let config: Config = Arc::new(Mutex::new(HashMap::new()));
    let shared_repl_conf: SharedReplicationConfig = Arc::new(Mutex::new(ReplConfig::default()));

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

    match args.replicaof {
        Some(_replicaof) => {
            shared_repl_conf.lock().unwrap().role = ReplRole::Slave;
        }
        None => {
            let mut repl_conf = shared_repl_conf.lock().unwrap();
            repl_conf.master_replid = Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
            repl_conf.master_repl_offset = Some(0);
        }
    }

    let port = match args.port {
        Some(port) => port,
        _ => 6379,
    };

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
