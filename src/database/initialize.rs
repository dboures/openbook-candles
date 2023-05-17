use std::{fs, time::Duration};

use deadpool_postgres::{
    Config as PgConfig, ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime, SslMode,
    Timeouts,
};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;

use crate::utils::Config;

pub async fn connect_to_database(config: &Config) -> anyhow::Result<Pool> {
    let mut x = PgConfig::new();

    // TODO: fix
    x.host = Some("".to_owned());
    x.user = Some("".to_owned());
    x.password = Some("".to_owned());
    x.dbname = Some("postgres".to_owned());

    x.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    x.pool = Some(PoolConfig {
        max_size: config.max_pg_pool_connections,
        timeouts: Timeouts::default(),
    });

    // openssl pkcs12 -export -in client.cer -inkey client-key.cer -out client.pks
    // base64 -i ca.cer -o ca.cer.b64 && base64 -i client.pks -o client.pks.b64
    // fly secrets set PG_CA_CERT=- < ./ca.cer.b64 -a mango-fills
    // fly secrets set PG_CLIENT_KEY=- < ./client.pks.b64 -a mango-fills
    let tls = if config.use_ssl {
        x.ssl_mode = Some(SslMode::Require);
        let ca_cert = fs::read(&config.ca_cert_path).expect("reading client cert from file");
        let client_key = fs::read(&config.client_key_path).expect("reading client key from file");
        MakeTlsConnector::new(
            TlsConnector::builder()
                .add_root_certificate(Certificate::from_pem(&ca_cert)?)
                .identity(Identity::from_pkcs12(&client_key, "pass")?)
                .danger_accept_invalid_certs(false)
                .build()?,
        )
    } else {
        MakeTlsConnector::new(
            TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        )
    };

    let pool = x.create_pool(Some(Runtime::Tokio1), tls).unwrap();
    match pool.get().await {
        Ok(_) => println!("Database connected"),
        Err(e) => {
            println!("Failed to connect to database: {}, retrying", e.to_string());
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    Ok(pool)
}

pub async fn setup_database(pool: &Pool) -> anyhow::Result<()> {
    let candles_table_fut = create_candles_table(pool);
    let fills_table_fut = create_fills_table(pool);
    let result = tokio::try_join!(candles_table_fut, fills_table_fut);
    match result {
        Ok(_) => {
            println!("Successfully configured database");
            Ok(())
        }
        Err(e) => {
            println!("Failed to configure database: {e}");
            Err(e)
        }
    }
}

pub async fn create_candles_table(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS candles (
            id serial,
            market_name text,
            start_time timestamptz,
            end_time timestamptz,
            resolution text,
            open double precision,
            close double precision,
            high double precision,
            low double precision,
            volume double precision,
            complete bool
        )",
            &[],
        )
        .await?;

    client.execute(
        "CREATE INDEX IF NOT EXISTS idx_market_time_resolution ON candles (market_name, start_time, resolution)",
        &[]
    ).await?;

    client.execute(
        "DO $$
            BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_candles') THEN
                ALTER TABLE candles ADD CONSTRAINT unique_candles UNIQUE (market_name, start_time, resolution);
            END IF;
        END $$", &[]
    ).await?;

    Ok(())
}

pub async fn create_fills_table(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS fills (
            id numeric PRIMARY KEY,
            time timestamptz not null,
            market text not null,
            open_orders text not null,
            open_orders_owner text not null,
            bid bool not null,
            maker bool not null,
            native_qty_paid double precision not null,
            native_qty_received double precision not null,
            native_fee_or_rebate double precision not null,
            fee_tier text not null,
            order_id text not null
        )",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_id_market ON fills (id, market)",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_market_time ON fills (market, time)",
            &[],
        )
        .await?;
    Ok(())
}
