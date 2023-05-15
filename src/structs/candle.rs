use chrono::{DateTime, NaiveDateTime, Utc};
use tokio_postgres::Row;

use super::resolution::Resolution;

#[derive(Clone, Debug)]
pub struct Candle {
    pub market_name: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub resolution: String,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub complete: bool,
}

impl Candle {
    pub fn create_empty_candle(market_name: String, resolution: Resolution) -> Candle {
        Candle {
            market_name,
            start_time: DateTime::from_utc(NaiveDateTime::MIN, Utc),
            end_time: DateTime::from_utc(NaiveDateTime::MIN, Utc),
            resolution: resolution.to_string(),
            open: 0.0,
            close: 0.0,
            high: 0.0,
            low: 0.0,
            volume: 0.0,
            complete: false,
        }
    }

    pub fn from_row(row: Row) -> Self {
        Candle {
            market_name: row.get(0),
            start_time: row.get(1),
            end_time: row.get(2),
            resolution: row.get(3),
            open: row.get(4),
            close: row.get(5),
            high: row.get(6),
            low: row.get(7),
            volume: row.get(8),
            complete: row.get(9),
        }
    }
}
