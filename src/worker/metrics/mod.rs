use actix_web::{dev::Server, http::StatusCode, App, HttpServer};
use actix_web_prom::PrometheusMetricsBuilder;
use lazy_static::lazy_static;
use prometheus::{
    register_int_counter_vec_with_registry, register_int_gauge_with_registry, IntCounterVec,
    IntGauge, Registry,
};

lazy_static! {
    static ref METRIC_REGISTRY: Registry =
        Registry::new_custom(Some("openbook_candles_worker".to_string()), None).unwrap();
    pub static ref METRIC_TXS_TOTAL: IntCounterVec = register_int_counter_vec_with_registry!(
        "txs_total",
        "Total number of transactions scraped",
        &["market", "status"],
        METRIC_REGISTRY
    )
    .unwrap();
    pub static ref METRIC_FILLS_TOTAL: IntCounterVec = register_int_counter_vec_with_registry!(
        "fills_total",
        "Total number of fills parsed",
        &["market"],
        METRIC_REGISTRY
    )
    .unwrap();
    pub static ref METRIC_CANDLES_TOTAL: IntCounterVec = register_int_counter_vec_with_registry!(
        "candles_total",
        "Total number of candles generated",
        &["market"],
        METRIC_REGISTRY
    )
    .unwrap();
    pub static ref METRIC_FILLS_QUEUE_LENGTH: IntGauge = register_int_gauge_with_registry!(
        "fills_queue_length",
        "Current length of the fills write queue",
        METRIC_REGISTRY
    )
    .unwrap();
    pub static ref METRIC_CANDLES_QUEUE_LENGTH: IntGauge = register_int_gauge_with_registry!(
        "candles_queue_length",
        "Current length of the candles write queue",
        METRIC_REGISTRY
    )
    .unwrap();
    pub static ref METRIC_RPC_ERRORS_TOTAL: IntCounterVec =
        register_int_counter_vec_with_registry!(
            "rpc_errors_total",
            "RPC errors while scraping",
            &["method"],
            METRIC_REGISTRY
        )
        .unwrap();
    pub static ref METRIC_DB_POOL_SIZE: IntGauge = register_int_gauge_with_registry!(
        "db_pool_size",
        "Current size of the DB connection pool",
        METRIC_REGISTRY
    )
    .unwrap();
    pub static ref METRIC_DB_POOL_AVAILABLE: IntGauge = register_int_gauge_with_registry!(
        "db_pool_available",
        "Available DB connections in the pool",
        METRIC_REGISTRY
    )
    .unwrap();
}

pub async fn serve_metrics() -> anyhow::Result<Server> {
    let metrics = PrometheusMetricsBuilder::new("openbook_candles_worker")
        .registry(METRIC_REGISTRY.clone())
        .exclude("/metrics")
        .exclude_status(StatusCode::NOT_FOUND)
        .endpoint("/metrics")
        .build()
        .unwrap();
    let server = HttpServer::new(move || App::new().wrap(metrics.clone()))
        .bind("0.0.0.0:9091")
        .unwrap()
        .disable_signals()
        .run();

    Ok(server)
}
