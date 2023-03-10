use serde_derive::Deserialize;
use std::{fs::File, io::Read};

pub trait AnyhowWrap {
    type Value;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
    type Value = T;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
        self.map_err(|err| anyhow::anyhow!("{:?}", err))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub rpc_url: String,
    pub database_url: String,
    pub max_pg_pool_connections: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarketConfig {
    pub name: String,
    pub address: String,
}

#[derive(Debug)]
pub struct MarketInfo {
    pub name: String,
    pub address: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
}

pub fn load_markets(path: &str) -> Vec<MarketConfig> {
    let reader = File::open(path).unwrap();
    serde_json::from_reader(reader).unwrap()
}
