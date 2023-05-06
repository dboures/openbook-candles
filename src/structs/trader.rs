use std::fmt;

use num_traits::ToPrimitive;
use serde::Serialize;
use sqlx::types::Decimal;

use super::openbook::token_factor;

#[derive(Clone, Debug, PartialEq)]
pub struct PgTrader {
    pub open_orders_owner: String,
    pub raw_ask_size: Decimal,
    pub raw_bid_size: Decimal,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum VolumeType {
    Base,
    Quote,
}
impl fmt::Display for VolumeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VolumeType::Base => write!(f, "Base"),
            VolumeType::Quote => write!(f, "Quote"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Trader {
    pub pubkey: String,
    pub volume: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TraderResponse {
    pub start_time: u64,
    pub end_time: u64,
    pub volume_type: String,
    pub traders: Vec<Trader>,
}

// Note that the Postgres queries only return volumes in base or quote
pub fn calculate_trader_volume(trader: PgTrader, decimals: u8) -> Trader {
    let bid_size = trader.raw_bid_size / token_factor(decimals);
    let ask_size = trader.raw_ask_size / token_factor(decimals);

    Trader {
        pubkey: trader.open_orders_owner,
        volume: (bid_size + ask_size).to_f64().unwrap(),
    }
}
