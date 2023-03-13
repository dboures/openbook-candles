use chrono::Utc;
use num_traits::ToPrimitive;
use serde::Serialize;

use super::candle::Candle;

#[derive(Serialize)]
pub struct TvResponse {
    /// ok, error, no_data
    #[serde(rename(serialize = "s"))]
    pub status: String,
    #[serde(rename(serialize = "errmsg"), skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub time: Vec<u64>,
    pub close: Vec<f64>,
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub volume: Vec<u64>,
    /// Only Some if s == no_data
    #[serde(
        rename(serialize = "nextTime"),
        skip_serializing_if = "Option::is_none"
    )]
    pub next_time: Option<u64>,
}

impl TvResponse {
    pub fn candles_to_tv(candles: Vec<Candle>) -> Self {
        let mut time: Vec<u64> = Vec::new();
        let mut close: Vec<f64> = Vec::new();
        let mut open: Vec<f64> = Vec::new();
        let mut low: Vec<f64> = Vec::new();
        let mut high: Vec<f64> = Vec::new();
        let mut volume: Vec<u64> = Vec::new();

        for c in candles.into_iter() {
            time.push(chrono::DateTime::<Utc>::timestamp(&c.start_time) as u64);
            close.push(c.close.to_f64().unwrap());
            open.push(c.open.to_f64().unwrap());
            high.push(c.high.to_f64().unwrap());
            low.push(c.low.to_f64().unwrap());
            volume.push(c.volume.to_u64().unwrap());
        }

        // Debug checks
        assert_eq!(time.len(), close.len());
        assert_eq!(close.len(), open.len());
        assert_eq!(open.len(), low.len());
        assert_eq!(low.len(), high.len());
        assert_eq!(volume.len(), time.len());

        TvResponse {
            status: "ok".to_owned(),
            error_message: None,
            time,
            close,
            open,
            low,
            high,
            volume,
            next_time: None,
        }
    }
}
