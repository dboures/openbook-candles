use chrono::{DateTime, NaiveDateTime, Utc};
use num_traits::Zero;
use sqlx::types::Decimal;

use crate::structs::resolution::Resolution;

pub mod fetch;
pub mod initialize;
pub mod insert;

pub trait Summary {
    fn summarize(&self) -> String;
}

#[derive(Clone, Debug)]
pub struct Candle {
    pub market_name: String,
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
    pub fn create_empty_candle(market_name: String, resolution: Resolution) -> Candle {
        Candle {
            market_name,
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


