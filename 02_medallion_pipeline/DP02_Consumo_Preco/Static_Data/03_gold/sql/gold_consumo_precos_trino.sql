-- =============================================================================
-- Gold DDL — consumo_preco
-- Tabelas Iceberg nativas, particionadas por year/month.
-- Carregadas via workflow Flyte (flyte_silver_to_gold.py).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Schema
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://warehouse/gold/');

-- -----------------------------------------------------------------------------
-- Tabela 1: dp_energy_market_hourly
-- Produto analítico principal: consumo + preço + features temporais e de lag.
-- Origem upstream: silver.consumo_hourly + silver.preco_hourly (join por ts_utc)
-- Consumidores: dashboard, API, exploração analítica, base para ML
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.dp_energy_market_hourly (
    ts_utc                   TIMESTAMP(6) WITH TIME ZONE,
    consumo_total            DOUBLE,
    market_price_pt          DOUBLE,
    hora                     INTEGER,
    dia_semana               INTEGER,
    is_weekend               BOOLEAN,
    consumo_lag_1h           DOUBLE,
    consumo_lag_24h          DOUBLE,
    price_lag_1h             DOUBLE,
    rolling_avg_consumo_24h  DOUBLE,
    rolling_avg_price_24h    DOUBLE,
    process_date             DATE,
    year                     INTEGER,
    month                    INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/gold/dp_energy_market_hourly/'
);

ALTER TABLE iceberg.gold.dp_energy_market_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.gold.dp_energy_market_hourly IS
'Produto Gold principal: consumo elétrico nacional horário integrado com preço day-ahead MIBEL PT. Inclui features temporais e de lag para análise e serving.';

COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.ts_utc IS 'Chave de negócio: timestamp UTC canónico da hora.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.consumo_total IS 'Consumo elétrico nacional horário em MWh.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.market_price_pt IS 'Preço day-ahead MIBEL Portugal em €/MWh.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.hora IS 'Hora do dia derivada de ts_utc (0-23).';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.dia_semana IS 'Dia da semana derivado de ts_utc (0=Segunda … 6=Domingo).';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.is_weekend IS 'True se dia_semana >= 5 (Sábado ou Domingo).';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.consumo_lag_1h IS 'Consumo observado na hora anterior a ts_utc.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.consumo_lag_24h IS 'Consumo observado 24 horas antes de ts_utc.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.price_lag_1h IS 'Preço observado na hora anterior a ts_utc.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.rolling_avg_consumo_24h IS 'Média móvel do consumo nas últimas 24 horas (janela de 24 registos).';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.rolling_avg_price_24h IS 'Média móvel do preço nas últimas 24 horas (janela de 24 registos).';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_hourly.process_date IS 'Data lógica da execução do workflow Silver → Gold.';

-- -----------------------------------------------------------------------------
-- Tabela 2: feat_load_forecasting_hourly
-- Feature table para ML: subconjunto do produto analítico + target consumo_next_hour.
-- Origem upstream: gold.dp_energy_market_hourly
-- Consumidores: workflow de treino ML, MLflow
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.feat_load_forecasting_hourly (
    ts_utc                   TIMESTAMP(6) WITH TIME ZONE,
    consumo_total            DOUBLE,
    market_price_pt          DOUBLE,
    hora                     INTEGER,
    dia_semana               INTEGER,
    is_weekend               BOOLEAN,
    consumo_lag_1h           DOUBLE,
    consumo_lag_24h          DOUBLE,
    price_lag_1h             DOUBLE,
    rolling_avg_consumo_24h  DOUBLE,
    rolling_avg_price_24h    DOUBLE,
    consumo_next_hour        DOUBLE,
    process_date             DATE,
    year                     INTEGER,
    month                    INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/gold/feat_load_forecasting_hourly/'
);

ALTER TABLE iceberg.gold.feat_load_forecasting_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.gold.feat_load_forecasting_hourly IS
'Feature table Gold para treino de modelos de previsão de consumo horário. Derivada do produto analítico principal com adição do target consumo_next_hour.';

COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_hourly.ts_utc IS 'Chave temporal: timestamp UTC canónico da hora.';
COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_hourly.consumo_next_hour IS 'TARGET de supervised learning: consumo da hora seguinte (LEAD de consumo_total).';
COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_hourly.consumo_lag_1h IS 'Feature: consumo observado na hora anterior.';
COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_hourly.consumo_lag_24h IS 'Feature: consumo observado 24 horas antes.';
COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_hourly.price_lag_1h IS 'Feature: preço observado na hora anterior.';
COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_hourly.rolling_avg_consumo_24h IS 'Feature: média móvel de consumo nas últimas 24 horas.';
COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_hourly.rolling_avg_price_24h IS 'Feature: média móvel de preço nas últimas 24 horas.';
