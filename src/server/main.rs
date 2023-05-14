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
use std::env;
use traders::{get_top_traders_by_base_volume, get_top_traders_by_quote_volume};

mod candles;
mod coingecko;
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
    let use_ssl: bool = dotenv::var("USE_SSL").unwrap().parse::<bool>().unwrap();
    let ca_cert_path: String = dotenv::var("CA_CERT_PATH").unwrap();
    let client_key_path: String = dotenv::var("CLIENT_KEY_PATH").unwrap();
    let max_pg_pool_connections: usize = dotenv::var("MAX_PG_POOL_CONNS_WORKER")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    let config = Config {
        rpc_url: rpc_url.clone(),
        database_url,
        max_pg_pool_connections,
        use_ssl,
        ca_cert_path,
        client_key_path,
    };

    let markets = load_markets(path_to_markets_json);
    let market_infos = fetch_market_infos(&config, markets).await.unwrap();
    let pool = connect_to_database(&config).await.unwrap();

    let context = Data::new(WebContext {
        rpc_url,
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
                    .service(get_markets)
                    .service(coingecko::service()),
            )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
