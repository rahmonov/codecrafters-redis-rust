use anyhow::Result;
use clap::Parser;
use core::fmt;
use resp::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

mod db;
mod resp;

#[derive(Debug)]
pub struct DbItem {
    value: String,
    created: Instant,
    expires: usize,
}

impl DbItem {
    fn new(value: String, created: Instant, expires: usize) -> Self {
        Self {
            value,
            created,
            expires,
        }
    }

    fn _default() -> Self {
        Self {
            value: String::default(),
            expires: 0,
            created: Instant::now(),
        }
    }
}

type Db = Arc<Mutex<HashMap<String, DbItem>>>;
type Config = Arc<Mutex<HashMap<String, String>>>;

enum ReplRole {
    Master,
    Slave,
}

impl fmt::Display for ReplRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplRole::Master => write!(f, "master"),
            ReplRole::Slave => write!(f, "slave"),
        }
    }
}

struct ReplConfig {
    role: ReplRole,
    master_replid: Option<String>,
    master_repl_offset: Option<usize>,
}

impl ReplConfig {
    fn default() -> Self {
        ReplConfig {
            role: ReplRole::Master,
            master_replid: None,
            master_repl_offset: None,
        }
    }
}
type SharedReplicationConfig = Arc<Mutex<ReplConfig>>;

#[derive(Parser)]
struct ServiceArguments {
    #[arg(long)]
    dir: Option<String>,

    #[arg(long)]
    dbfilename: Option<String>,

    #[arg(long)]
    port: Option<usize>,

    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = ServiceArguments::parse();

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

        let rdb_contents = db::parse_rdb_file(path.to_path_buf()).await.unwrap();
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

async fn handle_connection(
    stream: TcpStream,
    db: Db,
    config: Config,
    shared_repl_conf: SharedReplicationConfig,
) {
    let mut handler = resp::RespHandler::new(stream);
    println!("Handling new request...");

    loop {
        let value = handler.read_value().await.unwrap();

        println!("Got request value value {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_uppercase().as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
                "ECHO" => args.first().unwrap().clone(),
                "SET" => handle_set(&db, &args),
                "GET" => handle_get(&db, args[0].clone()),
                "CONFIG" => handle_config(&config, args[0].clone(), args[1].clone()),
                "KEYS" => handle_keys(&db),
                "INFO" => handle_info(&shared_repl_conf),
                c => panic!("Cannot handle command {}", c),
            }
        } else {
            break;
        };

        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();
    }
}

fn handle_info(replication_config: &SharedReplicationConfig) -> Value {
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

fn handle_keys(db: &Db) -> Value {
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
