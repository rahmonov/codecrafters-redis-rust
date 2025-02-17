use core::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

pub enum ReplRole {
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

pub struct ReplConfig {
    pub role: ReplRole,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<usize>,
}

impl ReplConfig {
    pub fn default() -> Self {
        ReplConfig {
            role: ReplRole::Master,
            master_replid: None,
            master_repl_offset: None,
        }
    }
}

pub type SharedReplicationConfig = Arc<Mutex<ReplConfig>>;
