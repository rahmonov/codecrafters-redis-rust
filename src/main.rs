use args::ServiceArguments;
use clap::Parser;
use config::Config;
use connection::Connection;
use db::DbItem;
use server::RedisServer;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

mod args;
mod config;
mod connection;
mod db;
mod frame;
mod handlers;
mod rdb;
mod replication;
mod server;

#[tokio::main]
async fn main() {
    let args = ServiceArguments::parse();

    let config = Config::from_args(args);
    let db = Arc::new(Mutex::new(HashMap::new()));
    let server = Arc::new(RedisServer::new(config, db));

    let (sender, _rx) = broadcast::channel(16);
    let sender = Arc::new(sender);

    if server.is_master().await {
        server.load_rdb().await;
    }

    let listener = server.listen().await;

    match server.connect_to_master().await {
        Ok(stream) => {
            if let Some(stream) = stream {
                let mut conn_to_master = Connection::new(stream);
                let sender_for_handshake = Arc::clone(&sender);

                server
                    .handshake_master(&mut conn_to_master, sender_for_handshake)
                    .await;

                let server = Arc::clone(&server);
                let sender = Arc::clone(&sender);

                tokio::spawn(async move {
                    server
                        .handle_connection(conn_to_master, sender, false)
                        .await;
                });
            }
        }
        Err(e) => panic!("Error connecting to master: {e}"),
    }

    loop {
        let (stream, _) = listener.accept().await.unwrap();

        let conn = Connection::new(stream);
        let server = Arc::clone(&server);
        let sender = Arc::clone(&sender);

        tokio::spawn(async move {
            server.handle_connection(conn, sender, true).await;
        });
    }
}
