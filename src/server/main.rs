use actix_web::{
    middleware::Logger,
    web::{self, Data},
    App, HttpServer,
};
use candles::get_candles;
use dotenv;
use markets::get_markets;
use openbook_candles::{
    database::initialize::connect_to_database,
    structs::markets::{fetch_market_infos, load_markets},
    utils::{Config, WebContext},
};
use traders::{get_top_traders_by_base_volume, get_top_traders_by_quote_volume};
use std::env;

mod candles;
mod markets;
mod server_error;
mod traders;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 2);
    let path_to_markets_json = &args[1];
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

    let markets = load_markets(path_to_markets_json);
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
                    .service(get_top_traders_by_quote_volume)
                    .service(get_markets),
            )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
