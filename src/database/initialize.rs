use std::{fs, time::Duration};

use deadpool_postgres::{
    ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime, SslMode, Timeouts,
};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;

use crate::utils::PgConfig;

pub async fn connect_to_database() -> anyhow::Result<Pool> {
    let mut pg_config = PgConfig::from_env()?;

    pg_config.pg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    pg_config.pg.pool = Some(PoolConfig {
        max_size: pg_config.pg_max_pool_connections,
        timeouts: Timeouts::default(),
    });

    // openssl pkcs12 -export -in client.cer -inkey client-key.cer -out client.pks
    // base64 -i ca.cer -o ca.cer.b64 && base64 -i client.pks -o client.pks.b64
    // fly secrets set PG_CA_CERT=- < ./ca.cer.b64 -a mango-fills
    // fly secrets set PG_CLIENT_KEY=- < ./client.pks.b64 -a mango-fills
    let tls = if pg_config.pg_use_ssl {
        pg_config.pg.ssl_mode = Some(SslMode::Require);
        let ca_cert = fs::read(pg_config.pg_ca_cert_path.expect("reading ca cert from env"))
            .expect("reading ca cert from file");
        let client_key = fs::read(
            pg_config
                .pg_client_key_path
                .expect("reading client key from env"),
        )
        .expect("reading client key from file");
        MakeTlsConnector::new(
            TlsConnector::builder()
                .add_root_certificate(Certificate::from_pem(&ca_cert)?)
                // TODO: make this configurable
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

    let pool = pg_config
        .pg
        .create_pool(Some(Runtime::Tokio1), tls)
        .unwrap();
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
    let transactions_table_fut = create_transactions_table(pool);
    let fills_table_fut = create_fills_table(pool);
    let result = tokio::try_join!(candles_table_fut, transactions_table_fut, fills_table_fut);
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
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
            signature text not null,
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
            order_id text not null,
            log_index int4 not null,
            CONSTRAINT fills_pk PRIMARY KEY (signature, log_index)
        )",
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

pub async fn create_transactions_table(pool: &Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS transactions (
                signature text NOT NULL,
                program_pk text NOT NULL,
                block_datetime timestamptz NOT NULL,
                slot bigint NOT NULL,
                err bool NOT NULL,
                processed bool NOT NULL,
                worker_partition int4 NOT NULL,
                CONSTRAINT transactions_pk PRIMARY KEY (signature, worker_partition)
            ) PARTITION BY LIST (worker_partition);",
            &[],
        )
        .await?;

    client.batch_execute(
        "CREATE INDEX IF NOT EXISTS transactions_processed_err_idx ON ONLY transactions (signature) WHERE processed IS NOT TRUE and err IS NOT TRUE;
        CREATE INDEX IF NOT EXISTS transactions_program_pk_idx ON ONLY transactions USING btree (program_pk, slot DESC);

        CREATE TABLE IF NOT EXISTS transactions_0 PARTITION OF transactions  FOR VALUES IN (0);
        CREATE TABLE IF NOT EXISTS transactions_1 PARTITION OF transactions  FOR VALUES IN (1);
        CREATE TABLE IF NOT EXISTS transactions_2 PARTITION OF transactions  FOR VALUES IN (2);
        CREATE TABLE IF NOT EXISTS transactions_3 PARTITION OF transactions  FOR VALUES IN (3);
        CREATE TABLE IF NOT EXISTS transactions_4 PARTITION OF transactions  FOR VALUES IN (4);
        CREATE TABLE IF NOT EXISTS transactions_5 PARTITION OF transactions  FOR VALUES IN (5);
        CREATE TABLE IF NOT EXISTS transactions_6 PARTITION OF transactions  FOR VALUES IN (6);
        CREATE TABLE IF NOT EXISTS transactions_7 PARTITION OF transactions  FOR VALUES IN (7);
        CREATE TABLE IF NOT EXISTS transactions_8 PARTITION OF transactions  FOR VALUES IN (8);
        CREATE TABLE IF NOT EXISTS transactions_9 PARTITION OF transactions  FOR VALUES IN (9);"
    ).await?;

    Ok(())
}
