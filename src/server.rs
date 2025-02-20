use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Result;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast::Sender, Mutex},
};

use crate::{
    config::Config,
    connection::Connection,
    db::{Db, DbItem},
    frame::Frame,
    handlers::{
        extract_command, handle_config, handle_echo, handle_get, handle_info, handle_keys,
        handle_ping, handle_psync, handle_replconf, handle_set,
    },
    rdb,
    replication::{ReplRole, ReplicationConfig},
};

pub struct RedisServer {
    pub replication: Arc<Mutex<ReplicationConfig>>,
    config: Config,
    db: Arc<Mutex<HashMap<String, DbItem>>>,
}

impl RedisServer {
    pub fn new(config: Config, db: Arc<Mutex<Db>>) -> Self {
        RedisServer {
            replication: Arc::new(Mutex::new(ReplicationConfig::from_config(&config))),
            config,
            db,
        }
    }

    pub async fn is_master(&self) -> bool {
        self.replication.lock().await.role == ReplRole::Master
    }

    pub async fn listen(&self) -> TcpListener {
        let addr = format!("127.0.0.1:{}", self.config.port);
        let listener = TcpListener::bind(addr.clone()).await.unwrap();

        println!("Ready to roll at: {addr}");
        listener
    }

    pub async fn connect_to_master(&self) -> Result<Option<TcpStream>> {
        if let Some(replicaof) = self.config.replicaof.clone() {
            println!(
                "replica at {} connecting to master at {}",
                self.config.port, replicaof
            );
            let stream = TcpStream::connect(replicaof).await?;
            return Ok(Some(stream));
        }

        Ok(None)
    }

    pub async fn load_rdb(&self) {
        if let (Some(dir), Some(dbfilename)) =
            (self.config.dir.clone(), self.config.dbfilename.clone())
        {
            let filename = format!("{dir}/{dbfilename}");
            let path = Path::new(&filename);

            let rdb_contents = rdb::parse_rdb_file(path.to_path_buf()).await.unwrap();
            let mut db = self.db.lock().await;
            *db = rdb_contents;

            println!("Loaded the RDB file successfully");
        }
    }

    pub async fn handle_connection(&self, mut conn: Connection, sender: Arc<Sender<Frame>>) {
        let is_master = {
            let guard = self.replication.lock().await;
            guard.role.clone() == ReplRole::Master
        };
        println!("handling new connection...");

        loop {
            let Ok(Some(frames)) = conn.read_frames().await else {
                println!("got nothing, stopping reading");
                break;
            };

            println!("Got request value {:?}", frames);

            let actionable_frames: Vec<_> = frames
                .into_iter()
                .filter(|(f, _)| !matches!(f, Frame::RDBContents()))
                .collect();

            for (frame, consumed_bytes) in actionable_frames {
                let sender = Arc::clone(&sender);
                let (command, args) = extract_command(frame.clone()).unwrap();

                match command.to_uppercase().as_str() {
                    "PING" => handle_ping(&mut conn, is_master).await,
                    "ECHO" => handle_echo(&mut conn, args.first().unwrap().clone()).await,
                    "SET" => {
                        handle_set(&mut conn, Arc::clone(&self.db), frame, sender, is_master).await
                    }
                    "GET" => handle_get(&mut conn, Arc::clone(&self.db), args[0].clone()).await,
                    "CONFIG" => {
                        handle_config(&mut conn, &self.config, args[0].clone(), args[1].clone())
                            .await
                    }
                    "KEYS" => handle_keys(&mut conn, Arc::clone(&self.db)).await,
                    "INFO" => handle_info(&mut conn, Arc::clone(&self.replication)).await,
                    "REPLCONF" => {
                        handle_replconf(&mut conn, Arc::clone(&self.replication), &args).await
                    }
                    "PSYNC" => handle_psync(&mut conn, Arc::clone(&self.replication), sender).await,
                    c => panic!("Cannot handle command {}", c),
                };

                {
                    if !is_master {
                        let mut repl_conf = self.replication.lock().await;

                        repl_conf.slave_repl_offset = repl_conf
                            .slave_repl_offset
                            .map_or(Some(consumed_bytes), |offset| Some(offset + consumed_bytes));
                    }
                }

                println!("response has been sent");
            }
        }
    }

    // TODO: check results of TCP requests (PONG, OK, OK)
    pub async fn handshake_master(&self, conn: &mut Connection) {
        println!("Starting handshake with master...");

        // Step 1: Send PING
        let ping_cmd = Frame::Array(vec![Frame::BulkString("PING".to_string())]);
        conn.write_frame(&ping_cmd)
            .await
            .expect("PING didn't succeed");

        let _frame = conn.read_frames().await.unwrap();
        println!("PING succeeded");

        // Step 2.1: Send REPLCONF listening-port <port>
        let replconf_cmd = Frame::Array(vec![
            Frame::BulkString("REPLCONF".to_string()),
            Frame::BulkString("listening-port".to_string()),
            Frame::BulkString(self.config.port.to_string()),
        ]);
        conn.write_frame(&replconf_cmd)
            .await
            .expect("REPLCONF with listening port didn't succeed");

        let _frame = conn.read_frames().await.unwrap();
        println!("Handshake Step 2.1 [REPLCONF with listening port] succeeded");

        // Step 2.2: Send REPLCONF capa psync2
        let replconf_cmd = Frame::Array(vec![
            Frame::BulkString("REPLCONF".to_string()),
            Frame::BulkString("capa".to_string()),
            Frame::BulkString("psync2".to_string()),
        ]);
        conn.write_frame(&replconf_cmd)
            .await
            .expect("REPLCONF with capabilities didn't succeed");

        let _frame = conn.read_frames().await.unwrap();
        println!("Handshake Step 2.2 [REPLCONF with capabilities] succeeded");

        // Step 3: Send PSYNC
        let psync_cmd = Frame::Array(vec![
            Frame::BulkString("PSYNC".to_string()),
            Frame::BulkString("?".to_string()),
            Frame::BulkString("-1".to_string()),
        ]);
        conn.write_frame(&psync_cmd)
            .await
            .expect("PSYNC didn't succeed");

        let _frame = conn.read_frames().await.unwrap();
        println!("Handshake Step 3 [PSYNC] succeeded");
    }
}
