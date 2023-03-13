use std::fs::File;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub name: String,
    pub address: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_mint_key: String,
    pub quote_mint_key: String,
    pub base_lot_size: u64,
    pub quote_lot_size: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarketConfig {
    pub name: String,
    pub address: String,
}

pub fn load_markets(path: &str) -> Vec<MarketConfig> {
    let reader = File::open(path).unwrap();
    serde_json::from_reader(reader).unwrap()
}