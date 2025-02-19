use std::collections::HashMap;
use tokio::time::Instant;

#[derive(Debug)]
pub struct DbItem {
    pub value: String,
    pub created: Instant,
    pub expires: usize,
}

impl DbItem {
    pub fn new(value: String, created: Instant, expires: usize) -> Self {
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

pub type Db = HashMap<String, DbItem>;
