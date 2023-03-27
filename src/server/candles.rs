use openbook_candles::{
    database::fetch::fetch_tradingview_candles,
    structs::{markets::valid_market, resolution::Resolution, tradingview::TvResponse},
    utils::{to_timestampz, WebContext},
};

use crate::server_error::ServerError;

use {
    actix_web::{get, web, HttpResponse},
    serde::Deserialize,
};

#[derive(Debug, Deserialize)]
pub struct Params {
    pub market_name: String,
    pub from: u64,
    pub to: u64,
    pub resolution: String,
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

    let mut conn = context.pool.acquire().await.unwrap();
    let candles =
        match fetch_tradingview_candles(&mut conn, &info.market_name, resolution, from, to)
            .await
        {
            Ok(c) => c,
            Err(_) => return Err(ServerError::DbQueryError),
        };

    Ok(HttpResponse::Ok().json(TvResponse::candles_to_tv(candles)))
}
