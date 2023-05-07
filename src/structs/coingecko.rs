use num_traits::ToPrimitive;
use serde::Serialize;
use sqlx::types::Decimal;

use super::{markets::MarketInfo, openbook::token_factor};

#[derive(Debug, Clone, Serialize)]
pub struct CoinGeckoPair {
    pub ticker_id: String,
    pub base: String,
    pub target: String,
    pub pool_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoinGeckoTicker {
    pub ticker_id: String,
    pub base_currency: String,
    pub target_currency: String,
    pub last_price: f64,
    pub base_volume: f64,
    pub target_volume: f64,
    pub bid: f64,
    pub ask: f64,
    pub high: f64,
    pub low: f64,
}

pub struct PgCoinGecko24HourVolume {
    pub address: String,
    pub raw_base_size: Decimal,
    pub raw_quote_size: Decimal,
}
impl PgCoinGecko24HourVolume {
    pub fn convert_to_readable(&self, markets: &Vec<MarketInfo>) -> CoinGecko24HourVolume {
        let market = markets.iter().find(|m| m.address == self.address).unwrap();
        let base_volume = (self.raw_base_size / token_factor(market.base_decimals))
            .to_f64()
            .unwrap();
        let target_volume = (self.raw_quote_size / token_factor(market.quote_decimals))
            .to_f64()
            .unwrap();
        CoinGecko24HourVolume {
            market_name: market.name.clone(),
            base_volume,
            target_volume,
        }
    }
}

#[derive(Debug, Default)]
pub struct CoinGecko24HourVolume {
    pub market_name: String,
    pub base_volume: f64,
    pub target_volume: f64,
}

#[derive(Debug, Default)]
pub struct PgCoinGecko24HighLow {
    pub market_name: String,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}
