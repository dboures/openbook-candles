use actix_web::{
    middleware::Logger,
    rt::System,
    web::{self, Data},
    App, HttpServer, http::StatusCode,
};
use actix_web_prom::PrometheusMetricsBuilder;
use candles::get_candles;
use prometheus::Registry;

use markets::get_markets;
use openbook_candles::{
    database::initialize::connect_to_database,
    structs::markets::{fetch_market_infos, load_markets},
    utils::{Config, WebContext},
};
use std::env;
use std::thread;
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
    let bind_addr: String = dotenv::var("SERVER_BIND_ADDR").expect("reading bind addr from env");

    let config = Config {
        rpc_url: rpc_url.clone(),
    };

    let markets = load_markets(path_to_markets_json);
    let market_infos = fetch_market_infos(&config, markets).await.unwrap();
    let pool = connect_to_database().await.unwrap();

    let registry = Registry::new();
    // For serving metrics on a private port
    let private_metrics = PrometheusMetricsBuilder::new("openbook_candles_server_private")
        .registry(registry.clone())
        .exclude("/metrics")
        .exclude_status(StatusCode::NOT_FOUND)
        .endpoint("/metrics")
        .build()
        .unwrap();
    // For collecting metrics on the public api, excluding 404s
    let public_metrics = PrometheusMetricsBuilder::new("openbook_candles_server")
        .registry(registry.clone())
        .exclude_status(StatusCode::NOT_FOUND)
        .build()
        .unwrap();

    let context = Data::new(WebContext {
        rpc_url,
        pool,
        markets: market_infos,
    });

    println!("Starting server");
    // Thread to serve public API
    let public_server = thread::spawn(move || {
        let sys = System::new();
        let srv = HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .wrap(public_metrics.clone())
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
        .bind(&bind_addr)
        .unwrap()
        .run();
        sys.block_on(srv).unwrap();
    });

    // Thread to serve metrics endpoint privately
    let private_server = thread::spawn(move || {
        let sys = System::new();
        let srv = HttpServer::new(move || {
            App::new()
                .wrap(private_metrics.clone())
        })
        .bind("0.0.0.0:9091")
        .unwrap()
        .run();
        sys.block_on(srv).unwrap();
    });

    private_server.join().unwrap();
    public_server.join().unwrap();
    Ok(())
}
