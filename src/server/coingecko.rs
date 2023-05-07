use std::time::{SystemTime, UNIX_EPOCH};

use crate::server_error::ServerError;
use actix_web::{get, web, HttpResponse, Scope};
use futures::join;
use openbook_candles::{
    database::fetch::{fetch_coingecko_24h_high_low, fetch_coingecko_24h_volume},
    structs::{
        coingecko::{
            CoinGecko24HourVolume, CoinGeckoOrderBook, CoinGeckoPair, CoinGeckoTicker,
            PgCoinGecko24HighLow,
        },
        slab::{get_best_bids_and_asks, get_orderbooks_with_depth},
    },
    utils::WebContext,
};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;

pub fn service() -> Scope {
    web::scope("/coingecko")
        .service(pairs)
        .service(tickers)
        .service(orderbook)
}

#[derive(Debug, Deserialize)]
pub struct OrderBookParams {
    pub ticker_id: String, // market_name
    pub depth: usize,
}

#[get("/pairs")]
pub async fn pairs(context: web::Data<WebContext>) -> Result<HttpResponse, ServerError> {
    let markets = context.markets.clone();

    let pairs = markets
        .iter()
        .map(|m| CoinGeckoPair {
            ticker_id: m.name.clone(),
            base: m.base_mint_key.clone(),
            target: m.quote_mint_key.clone(),
            pool_id: m.address.clone(),
        })
        .collect::<Vec<CoinGeckoPair>>();

    Ok(HttpResponse::Ok().json(pairs))
}

#[get("/tickers")]
pub async fn tickers(context: web::Data<WebContext>) -> Result<HttpResponse, ServerError> {
    let client = RpcClient::new(context.rpc_url.clone());
    let markets = &context.markets;

    let mut c1 = context.pool.acquire().await.unwrap();
    let mut c2 = context.pool.acquire().await.unwrap();
    let bba_fut = get_best_bids_and_asks(client, markets);
    let volume_fut = fetch_coingecko_24h_volume(&mut c1);
    let high_low_fut = fetch_coingecko_24h_high_low(&mut c2);

    let ((best_bids, best_asks), volume_query, high_low_quey) =
        join!(bba_fut, volume_fut, high_low_fut,);

    let raw_volumes = match volume_query {
        Ok(c) => c,
        Err(_) => return Err(ServerError::DbQueryError),
    };
    let high_low = match high_low_quey {
        Ok(c) => c,
        Err(_) => return Err(ServerError::DbQueryError),
    };

    let default_hl = PgCoinGecko24HighLow::default();
    let default_volume = CoinGecko24HourVolume::default();
    let volumes: Vec<CoinGecko24HourVolume> = raw_volumes
        .into_iter()
        .map(|v| v.convert_to_readable(&markets))
        .collect();
    let tickers = markets
        .iter()
        .enumerate()
        .map(|(index, m)| {
            let name = m.name.clone();
            let high_low = high_low
                .iter()
                .find(|x| x.market_name == name)
                .unwrap_or(&default_hl);
            let volume = volumes
                .iter()
                .find(|x| x.market_name == name)
                .unwrap_or(&default_volume);
            CoinGeckoTicker {
                ticker_id: m.name.clone(),
                base_currency: m.base_mint_key.clone(),
                target_currency: m.quote_mint_key.clone(),
                last_price: high_low.close.to_string(),
                base_volume: volume.base_volume.to_string(),
                target_volume: volume.target_volume.to_string(),
                bid: best_bids[index].to_string(),
                ask: best_asks[index].to_string(),
                high: high_low.high.to_string(),
                low: high_low.low.to_string(),
            }
        })
        .collect::<Vec<CoinGeckoTicker>>();

    Ok(HttpResponse::Ok().json(tickers))
}

#[get("/orderbook")]
pub async fn orderbook(
    info: web::Query<OrderBookParams>,
    context: web::Data<WebContext>,
) -> Result<HttpResponse, ServerError> {
    let client = RpcClient::new(context.rpc_url.clone());
    let market_name = &info.ticker_id;
    let market = context
        .markets
        .iter()
        .find(|m| m.name == *market_name)
        .ok_or(ServerError::MarketNotFound)?;
    let depth = info.depth;

    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH).unwrap().as_millis();
    let (bid_levels, ask_levels) = get_orderbooks_with_depth(client, market, depth).await;
    let result = CoinGeckoOrderBook {
        timestamp: timestamp.to_string(),
        ticker_id: market.name.clone(),
        bids: bid_levels,
        asks: ask_levels,
    };
    Ok(HttpResponse::Ok().json(result))
}
