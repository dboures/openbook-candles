use crate::server_error::ServerError;
use openbook_candles::{
    database::fetch::{
        fetch_top_traders_by_base_volume_from, fetch_top_traders_by_quote_volume_from,
    },
    structs::trader::{calculate_trader_volume, Trader, TraderResponse, VolumeType},
    utils::{to_timestampz, WebContext},
};
use {
    actix_web::{get, web, HttpResponse},
    serde::Deserialize,
};

#[derive(Debug, Deserialize)]
pub struct TraderParams {
    pub market_name: String,
    pub from: u64,
    pub to: u64,
}

#[get("/traders/base-volume")]
pub async fn get_top_traders_by_base_volume(
    info: web::Query<TraderParams>,
    context: web::Data<WebContext>,
) -> Result<HttpResponse, ServerError> {
    let selected_market = context.markets.iter().find(|x| x.name == info.market_name);
    if selected_market.is_none() {
        return Err(ServerError::MarketNotFound);
    }
    let selected_market = selected_market.unwrap();
    let from = to_timestampz(info.from);
    let to = to_timestampz(info.to);

    let mut conn = context.pool.acquire().await.unwrap();
    let raw_traders =
        match fetch_top_traders_by_base_volume_from(&mut conn, &selected_market.address, from, to)
            .await
        {
            Ok(c) => c,
            Err(_) => return Err(ServerError::DbQueryError),
        };

    let traders = raw_traders
        .into_iter()
        .map(|t| calculate_trader_volume(t, selected_market.base_decimals))
        .collect::<Vec<Trader>>();

    let response = TraderResponse {
        start_time: info.from,
        end_time: info.to,
        traders: traders,
        volume_type: VolumeType::Base.to_string(),
    };
    Ok(HttpResponse::Ok().json(response))
}

#[get("/traders/quote-volume")]
pub async fn get_top_traders_by_quote_volume(
    info: web::Query<TraderParams>,
    context: web::Data<WebContext>,
) -> Result<HttpResponse, ServerError> {
    let selected_market = context.markets.iter().find(|x| x.name == info.market_name);
    if selected_market.is_none() {
        return Err(ServerError::MarketNotFound);
    }
    let selected_market = selected_market.unwrap();
    let from = to_timestampz(info.from);
    let to = to_timestampz(info.to);

    let mut conn = context.pool.acquire().await.unwrap();
    let raw_traders =
        match fetch_top_traders_by_quote_volume_from(&mut conn, &selected_market.address, from, to)
            .await
        {
            Ok(c) => c,
            Err(_) => return Err(ServerError::DbQueryError),
        };

    let traders = raw_traders
        .into_iter()
        .map(|t| calculate_trader_volume(t, selected_market.quote_decimals))
        .collect::<Vec<Trader>>();

    let response = TraderResponse {
        start_time: info.from,
        end_time: info.to,
        traders: traders,
        volume_type: VolumeType::Quote.to_string(),
    };
    Ok(HttpResponse::Ok().json(response))
}
