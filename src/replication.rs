use core::fmt;

use crate::config::Config;

#[derive(Clone, PartialEq, Eq)]
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

pub struct ReplicationConfig {
    pub role: ReplRole,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<usize>,
    pub slave_repl_offset: Option<usize>,
}

impl ReplicationConfig {
    pub fn from_config(config: &Config) -> Self {
        match config.replicaof.is_some() {
            true => ReplicationConfig {
                role: ReplRole::Slave,
                master_replid: None,
                master_repl_offset: None,
                slave_repl_offset: Some(0),
            },
            false => ReplicationConfig {
                role: ReplRole::Master,
                master_replid: Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()),
                master_repl_offset: Some(0),
                slave_repl_offset: None,
            },
        }
    }
}
