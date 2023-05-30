use actix_web::{dev::Server, http::StatusCode, App, HttpServer};
use actix_web_prom::PrometheusMetricsBuilder;
use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec_with_registry, IntCounterVec, Registry};

lazy_static! {
    static ref METRIC_REGISTRY: Registry =
        Registry::new_custom(Some("openbook_candles_worker".to_string()), None).unwrap();
    pub static ref METRIC_FILLS_TOTAL: IntCounterVec = register_int_counter_vec_with_registry!(
        "fills_total",
        "Total number of fills scraped",
        &["market"],
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
