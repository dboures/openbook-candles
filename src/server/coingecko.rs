use std::str::FromStr;

use crate::server_error::ServerError;
use actix_web::{get, web, HttpResponse, Scope};
use anchor_lang::prelude::Pubkey;
use futures::join;
use num_traits::ToPrimitive;
use openbook_candles::{
    database::fetch::{fetch_coingecko_24h_high_low, fetch_coingecko_24h_volume},
    structs::coingecko::{
        CoinGecko24HourVolume, CoinGeckoPair, CoinGeckoTicker, PgCoinGecko24HighLow,
    },
    utils::WebContext,
};
use solana_client::nonblocking::rpc_client::RpcClient;

pub fn service() -> Scope {
    web::scope("/coingecko")
        .service(pairs)
        .service(tickers)
        .service(orderbook)
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
    let markets = context.markets.clone();

    // rpc get bid ask liquidity

    let mut conn = context.pool.acquire().await.unwrap();
    let raw_volumes = match fetch_coingecko_24h_volume(&mut conn).await {
        Ok(c) => c,
        Err(_) => return Err(ServerError::DbQueryError),
    };
    let high_low = match fetch_coingecko_24h_high_low(&mut conn).await {
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
        .map(|m| {
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
                last_price: high_low.close.to_f64().unwrap(),
                base_volume: volume.base_volume.to_f64().unwrap(),
                target_volume: volume.target_volume.to_f64().unwrap(),
                liquidity_in_usd: 0.0,
                bid: 0.0,
                ask: 0.0,
                high: high_low.high.to_f64().unwrap(),
                low: high_low.low.to_f64().unwrap(),
            }
        })
        .collect::<Vec<CoinGeckoTicker>>();

    Ok(HttpResponse::Ok().json(tickers))
}

#[get("/orderbook")]
pub async fn orderbook(context: web::Data<WebContext>) -> Result<HttpResponse, ServerError> {
    let client = RpcClient::new(context.rpc_url.clone());

    let markets = context.markets.clone();
    let bid_keys = markets
        .iter()
        .map(|m| Pubkey::from_str(&m.bids_key).unwrap())
        .collect::<Vec<Pubkey>>();
    let ask_keys = markets
        .iter()
        .map(|m| Pubkey::from_str(&m.asks_key).unwrap())
        .collect::<Vec<Pubkey>>();

    // client.get_multiple_accounts(&bid_keys)

    let (bid_results, _ask_results) = join!(
        client.get_multiple_accounts(&bid_keys),
        client.get_multiple_accounts(&ask_keys)
    );

    let x = bid_results.unwrap();

    println!("{:?}", x);

    // decode results

    let markets = context.markets.clone();
    Ok(HttpResponse::Ok().json(markets))
}
