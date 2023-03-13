use chrono::{Utc, NaiveDateTime};
use openbook_candles::{utils::WebContext, database::{fetch::fetch_tradingview_candles}, structs::{resolution::Resolution, markets::MarketInfo, tradingview::TvResponse}};

use crate::server_error::ServerError;

use {
    actix_web::{get, web, HttpResponse, Scope},
    serde::Deserialize,
};

#[derive(Debug, Deserialize)]
pub struct Params {
    pub market_name: String,
    pub from: u64,
    pub to: u64,
    pub resolution: String,
}

pub fn service() -> Scope {
    web::scope("/tradingview")
        .service(get_candles)
}

#[get("/candles")]
pub async fn get_candles(
    info: web::Query<Params>,
    context: web::Data<WebContext>,
) -> Result<HttpResponse, ServerError> {
    let resolution =
        Resolution::from_str(info.resolution.as_str()).map_err(|_| ServerError::WrongResolution)?;

    if !valid_market(&info.market_name, &context.markets) {
        return Err(ServerError::WrongParameters);
    }

    let from = to_timestampz(info.from);
    let to = to_timestampz(info.to);

    let candles = match fetch_tradingview_candles(&context.pool, &info.market_name, resolution, from, to).await {
        Ok(c) => c,
        Err(_) => return Err(ServerError::DbQueryError)
    };

    Ok(HttpResponse::Ok().json(TvResponse::candles_to_tv(candles)))
}

fn valid_market(market_name: &str, markets: &Vec<MarketInfo>) -> bool {
    markets.iter().any(|x| x.name == market_name)
}

fn to_timestampz(seconds: u64) -> chrono::DateTime<Utc> {
    chrono::DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(seconds as i64, 0), Utc)
}