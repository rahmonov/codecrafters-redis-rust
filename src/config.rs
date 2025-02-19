use crate::args::ServiceArguments;

const DEFAULT_PORT: usize = 6379;

pub struct Config {
    pub port: usize,
    pub dbfilename: Option<String>,
    pub dir: Option<String>,
    pub replicaof: Option<String>,
}

impl Config {
    pub fn from_args(args: ServiceArguments) -> Config {
        Config {
            port: args.port.unwrap_or(DEFAULT_PORT),
            dbfilename: args.dbfilename,
            dir: args.dir,
            replicaof: reformat_replicaof(args.replicaof),
        }
    }

    pub fn get(&self, config_name: String) -> Option<String> {
        match config_name.as_str() {
            "dir" => self.dir.clone(),
            "dbfilename" => self.dbfilename.clone(),
            _ => None,
        }
    }
}

fn reformat_replicaof(replicaof: Option<String>) -> Option<String> {
    if let Some(replicaof) = replicaof {
        return Some(replicaof.replace(' ', ":"));
    }

    None
}
