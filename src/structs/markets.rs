use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Clone, Serialize)]
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

pub fn valid_market(market_name: &str, markets: &Vec<MarketInfo>) -> bool {
    markets.iter().any(|x| x.name == market_name)
}
