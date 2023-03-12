use std::fmt;

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use num_traits::Zero;
use sqlx::types::Decimal;
use strum::EnumIter;

use crate::candle_batching::day;

pub mod fetch;
pub mod initialize;
pub mod insert;

pub trait Summary {
    fn summarize(&self) -> String;
}

#[derive(EnumIter, Copy, Clone, Eq, PartialEq)]
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
        }
    }

    pub fn get_duration(self) -> Duration {
        match self {
            Resolution::R1m => Duration::minutes(1),
            Resolution::R3m => Duration::minutes(3),
            Resolution::R5m => Duration::minutes(5),
            Resolution::R15m => Duration::minutes(15),
            Resolution::R30m => Duration::minutes(30),
            Resolution::R1h => Duration::hours(1),
            Resolution::R2h => Duration::hours(2),
            Resolution::R4h => Duration::hours(4),
            Resolution::R1d => day(),
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
