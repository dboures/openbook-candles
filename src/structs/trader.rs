use std::fmt;

use num_traits::ToPrimitive;
use serde::Serialize;
use tokio_postgres::Row;

use super::openbook::token_factor;

#[derive(Clone, Debug, PartialEq)]
pub struct PgTrader {
    pub open_orders_owner: String,
    pub raw_ask_size: f64,
    pub raw_bid_size: f64,
}
impl PgTrader {
    pub fn from_row(row: Row) -> Self {
        PgTrader {
            open_orders_owner: row.get(0),
            raw_ask_size: row.get(1),
            raw_bid_size: row.get(2),
        }
    }
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
