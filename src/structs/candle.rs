use chrono::{DateTime, NaiveDateTime, Utc};
use num_traits::Zero;
use sqlx::types::Decimal;

use super::resolution::Resolution;

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
