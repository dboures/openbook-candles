use actix_web::{
    get,
    middleware::Logger,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use dotenv;
use openbook_candles::{utils::{Config, WebContext}, candle_creation::trade_fetching::scrape::fetch_market_infos, database::initialize::connect_to_database, structs::markets::load_markets};
use sqlx::{Pool, Postgres};

mod candles;
mod server_error;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[get("/trade-count")]
async fn get_total_trades(pool_data: web::Data<Pool<Postgres>>) -> impl Responder {
    let pool = pool_data.get_ref();
    let total_query = sqlx::query!("Select COUNT(*) as total from fills")
        .fetch_one(pool)
        .await
        .unwrap();
    let total_trades: i64 = total_query.total.unwrap_or_else(|| 0);
    HttpResponse::Ok().json(total_trades)
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let rpc_url: String = dotenv::var("RPC_URL").unwrap();
    let database_url: String = dotenv::var("DATABASE_URL").unwrap();
    let max_pg_pool_connections: u32 = dotenv::var("MAX_PG_POOL_CONNS_SERVER").unwrap().parse::<u32>().unwrap();

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

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(Data::new(context.clone()))
            .service(candles::service())
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
