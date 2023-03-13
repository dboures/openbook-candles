use actix_web::{
    get,
    middleware::Logger,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use dotenv;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

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

// #[get("/recent-trades")]
// async fn get_recent_trades(pool_data: web::Data<Pool<Postgres>>, market: String) -> impl Responder {
//     let pool = pool_data.get_ref();
//     let trades_query= sqlx::query!("Select * as total from fills").fetch_one(pool).await.unwrap();
//     let total_trades: i64 = total_query.total.unwrap_or_else(|| 0);
//     HttpResponse::Ok().json(total_trades)
// }

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[actix_web::main]
async fn main() -> anyhow::Result<(), std::io::Error> {
    dotenv::dotenv().ok();
    env_logger::init();

    let database_url = dotenv::var("DATABASE_URL").unwrap();

    // let context = Data::new(Context {
    //     markets: utils::markets::load_markets(markets_path),
    //     pool,
    // });

    let pool = PgPoolOptions::new()
        .max_connections(15)
        .connect(&database_url)
        .await
        .unwrap();

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(Data::new(pool.clone()))
            .service(get_total_trades)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
