use chrono::{DateTime, Utc};
use solana_sdk::pubkey::Pubkey;

pub mod database;

pub struct Candle {}

pub struct MarketInfo {
    pub market_key: Pubkey,
    pub market_name: String,
    pub base_symbol: String,
    pub quote_symbol: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}
