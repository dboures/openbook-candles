use crate::server_error::ServerError;
use actix_web::{get, web, HttpResponse, Scope};
use futures::join;
use num_traits::ToPrimitive;
use openbook_candles::{
    database::fetch::{fetch_coingecko_24h_high_low, fetch_coingecko_24h_volume},
    structs::{
        coingecko::{CoinGecko24HourVolume, CoinGeckoPair, CoinGeckoTicker, PgCoinGecko24HighLow},
        slab::{get_best_bids_and_asks},
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
    let client = RpcClient::new(context.rpc_url.clone());
    let markets = &context.markets;

    let mut c1 = context.pool.acquire().await.unwrap();
    let mut c2 = context.pool.acquire().await.unwrap();
    let bba_fut = get_best_bids_and_asks(client, markets);
    let volume_fut = fetch_coingecko_24h_volume(&mut c1);
    let high_low_fut = fetch_coingecko_24h_high_low(&mut c2);

    let ((best_bids, best_asks), volume_query, high_low_quey) = join!(
        bba_fut,
        volume_fut,
        high_low_fut,
    );

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
                last_price: high_low.close.to_f64().unwrap(),
                base_volume: volume.base_volume.to_f64().unwrap(),
                target_volume: volume.target_volume.to_f64().unwrap(),
                bid: best_bids[index].to_f64().unwrap(),
                ask: best_asks[index].to_f64().unwrap(),
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

    let markets = &context.markets;

    let (best_bids, best_asks) = get_best_bids_and_asks(client, markets).await;

    // let bids = bid_bytes.into_iter().map(|mut x| Slab::new(x.as_mut_slice())).collect::<Vec<_>>();
    // Slab::new(&mut x.data)

    // let mut bb = bid_bytes[0].clone();
    // let data_end = bb.len() - 7;

    // let goo = Slab::new(&mut bb[13..data_end]);

    // decode results

    let markets = context.markets.clone();
    Ok(HttpResponse::Ok().json(markets))
}
