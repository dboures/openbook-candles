use actix_web::{
    get,
    middleware::Logger,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use candles::get_candles;
use dotenv;
use openbook_candles::{
    candle_creation::trade_fetching::scrape::fetch_market_infos,
    database::initialize::connect_to_database,
    structs::markets::load_markets,
    utils::{Config, WebContext},
};
use sqlx::{Pool, Postgres};
use traders::get_top_traders_by_base_volume;

mod candles;
mod server_error;
mod traders;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let rpc_url: String = dotenv::var("RPC_URL").unwrap();
    let database_url: String = dotenv::var("DATABASE_URL").unwrap();
    let max_pg_pool_connections: u32 = dotenv::var("MAX_PG_POOL_CONNS_SERVER")
        .unwrap()
        .parse::<u32>()
        .unwrap();

    let config = Config {
        rpc_url: rpc_url.clone(),
        database_url: database_url.clone(),
        max_pg_pool_connections,
    };

    let markets = load_markets("/Users/dboures/dev/openbook-candles/markets.json");
    let market_infos = fetch_market_infos(&config, markets).await.unwrap();
    let pool = connect_to_database(&config).await.unwrap();

    let context = Data::new(WebContext {
        pool,
        markets: market_infos,
    });

    println!("Starting server");
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(context.clone())
            .service(
                web::scope("/api")
                    .service(get_candles)
                    .service(get_top_traders_by_base_volume)
                    // .service(get_top_traders_by_quote_volume)
            )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
