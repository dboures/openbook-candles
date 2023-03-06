use serde_derive::Deserialize;

pub trait AnyhowWrap {
    type Value;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
    type Value = T;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
        self.map_err(|err| anyhow::anyhow!("{:?}", err))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub rpc_ws_url: String,
    pub rpc_http_url: String,
    pub database_config: DatabaseConfig,
    pub markets: Vec<MarketConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseConfig {
    pub connection_string: String,
    pub max_pg_pool_connections: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarketConfig {
    pub name: String,
    pub market: String,
}
