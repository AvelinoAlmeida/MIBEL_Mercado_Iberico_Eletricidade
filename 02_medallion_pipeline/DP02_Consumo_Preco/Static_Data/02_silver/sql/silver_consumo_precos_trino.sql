-- =============================================================================
-- Silver DDL — consumo_preco
-- Tabelas Iceberg nativas, particionadas por year/month.
-- Carregadas via workflow Flyte (flyte_bronze_to_silver.py).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Schema
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://warehouse/silver/');

-- -----------------------------------------------------------------------------
-- Tabela 1: consumo_hourly
-- Origem upstream: iceberg.bronze.consumo_raw
-- Transformações: parsing do timestamp UTC, agregação 15min → 1h, kW → MWh
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.consumo_hourly (
    ts_utc      TIMESTAMP(6) WITH TIME ZONE,
    total_mwh   DOUBLE,
    year        INTEGER,
    month       INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/silver/consumo_hourly/'
);

ALTER TABLE iceberg.silver.consumo_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.silver.consumo_hourly IS
'Tabela Silver com consumo elétrico nacional horário normalizado para UTC. Agrega registos de 15 minutos do Bronze e converte kW para MWh.';

COMMENT ON COLUMN iceberg.silver.consumo_hourly.ts_utc IS 'Timestamp UTC canónico que representa o início da hora.';
COMMENT ON COLUMN iceberg.silver.consumo_hourly.total_mwh IS 'Consumo nacional horário agregado em MWh (SUM dos 15 min / 1000).';
COMMENT ON COLUMN iceberg.silver.consumo_hourly.year IS 'Ano derivado de ts_utc. Usado para particionamento e pruning.';
COMMENT ON COLUMN iceberg.silver.consumo_hourly.month IS 'Mês derivado de ts_utc. Usado para particionamento e pruning.';

-- -----------------------------------------------------------------------------
-- Tabela 2: preco_hourly
-- Origem upstream: iceberg.bronze.preco_raw
-- Transformações: date_raw + hour (1-24) → ts_utc; hora 25 descartada (DST)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.preco_hourly (
    ts_utc                  TIMESTAMP(6) WITH TIME ZONE,
    price_portugal_eur_mwh  DOUBLE,
    price_spain_eur_mwh     DOUBLE,
    year                    INTEGER,
    month                   INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/silver/preco_hourly/'
);

ALTER TABLE iceberg.silver.preco_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.silver.preco_hourly IS
'Tabela Silver com preços day-ahead OMIE/MIBEL normalizados para UTC. Converte a numeração original de horas (1-24) para timestamp UTC. Hora 25 (DST outono) é descartada.';

COMMENT ON COLUMN iceberg.silver.preco_hourly.ts_utc IS 'Timestamp UTC canónico que representa o início da hora. Derivado de date_raw + (hour - 1).';
COMMENT ON COLUMN iceberg.silver.preco_hourly.price_portugal_eur_mwh IS 'Preço day-ahead de Portugal em €/MWh.';
COMMENT ON COLUMN iceberg.silver.preco_hourly.price_spain_eur_mwh IS 'Preço day-ahead de Espanha em €/MWh. Preservado para análise comparativa PT vs ES.';
COMMENT ON COLUMN iceberg.silver.preco_hourly.year IS 'Ano derivado de ts_utc. Usado para particionamento e pruning.';
COMMENT ON COLUMN iceberg.silver.preco_hourly.month IS 'Mês derivado de ts_utc. Usado para particionamento e pruning.';
