use std::fmt;

use chrono::{DateTime, NaiveDateTime, Utc};
use num_traits::Zero;
use sqlx::types::Decimal;
use strum::EnumIter;

pub mod database;
pub mod fetchers;

pub trait Summary {
    fn summarize(&self) -> String;
}

#[derive(EnumIter)]
pub enum Resolution {
    R1m,
    R3m,
    R5m,
    R15m,
    R30m,
    R1h,
    R2h,
    R4h,
    R1d,
    R1w,
}

impl fmt::Display for Resolution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Resolution::R1m => write!(f, "1M"),
            Resolution::R3m => write!(f, "3M"),
            Resolution::R5m => write!(f, "5M"),
            Resolution::R15m => write!(f, "15M"),
            Resolution::R30m => write!(f, "30M"),
            Resolution::R1h => write!(f, "1H"),
            Resolution::R2h => write!(f, "2H"),
            Resolution::R4h => write!(f, "4H"),
            Resolution::R1d => write!(f, "1D"),
            Resolution::R1w => write!(f, "1W"),
        }
    }
}

impl Resolution {
    pub fn get_constituent_resolution(self) -> Resolution {
        match self {
            Resolution::R1m => panic!("have to use fills to make 1M candles"),
            Resolution::R3m => Resolution::R1m,
            Resolution::R5m => Resolution::R1m,
            Resolution::R15m => Resolution::R5m,
            Resolution::R30m => Resolution::R15m,
            Resolution::R1h => Resolution::R30m,
            Resolution::R2h => Resolution::R1h,
            Resolution::R4h => Resolution::R2h,
            Resolution::R1d => Resolution::R4h,
            Resolution::R1w => Resolution::R1d,
        }
    }

    pub fn get_constituent_resolution_factor(self) -> u8 {
        match self {
            Resolution::R1m => panic!("have to use fills to make 1M candles"),
            Resolution::R3m => 3,
            Resolution::R5m => 5,
            Resolution::R15m => 3,
            Resolution::R30m => 2,
            Resolution::R1h => 2,
            Resolution::R2h => 2,
            Resolution::R4h => 2,
            Resolution::R1d => 6,
            Resolution::R1w => 7,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Candle {
    pub market: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub resolution: String,
    pub open: Decimal,
    pub close: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub volume: Decimal,
    pub complete: bool,
}

impl Candle {
    pub fn create_empty_candle(market: String, resolution: Resolution) -> Candle {
        Candle {
            market,
            start_time: DateTime::from_utc(NaiveDateTime::MIN, Utc),
            end_time: DateTime::from_utc(NaiveDateTime::MIN, Utc),
            resolution: resolution.to_string(),
            open: Decimal::zero(),
            close: Decimal::zero(),
            high: Decimal::zero(),
            low: Decimal::zero(),
            volume: Decimal::zero(),
            complete: false,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct PgOpenBookFill {
    pub time: DateTime<Utc>,
    pub bid: bool,
    pub maker: bool,
    pub native_qty_paid: Decimal,
    pub native_qty_received: Decimal,
    pub native_fee_or_rebate: Decimal,
}

#[derive(Debug)]
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
