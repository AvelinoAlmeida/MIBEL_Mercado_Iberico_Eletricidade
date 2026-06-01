-- =============================================================================
-- Silver DDL — Streaming_Data (DP-02 API pipeline)
-- Tabelas com sufixo _api para coexistir com o pipeline estático.
-- Particionadas por year/month. Carregadas via flyte_bronze_to_silver.py.
--
-- Fonte upstream: ENTSO-E Transparency Platform (token obrigatório).
--   consumo_api_hourly  ← iceberg.bronze.consumo_api_raw  (Actual Total Load PT)
--   preco_api_hourly    ← iceberg.bronze.preco_api_raw    (Day-Ahead Prices PT+ES)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.silver;

-- -----------------------------------------------------------------------------
-- Tabela 1: consumo_api_hourly
-- Upstream: iceberg.bronze.consumo_api_raw
-- Transformação: MW → MWh (total * 1.0, granularidade já horária), filtra nulos.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.consumo_api_hourly (
    ts_utc      TIMESTAMP(6) WITH TIME ZONE,  -- timestamp horário canónico em UTC
    total_mwh   DOUBLE,                        -- carga horária em MWh
    year        INTEGER,                       -- ano (partição)
    month       INTEGER                        -- mês (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/silver/consumo_api_hourly/'
);

ALTER TABLE iceberg.silver.consumo_api_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.silver.consumo_api_hourly IS
'Tabela Silver com carga elétrica nacional horária normalizada. Origem: ENTSO-E Transparency Platform (Actual Total Load PT). Equivalente a consumo_hourly mas com fonte dinâmica via API.';

COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.ts_utc IS 'Timestamp UTC canónico do início da hora.';
COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.total_mwh IS 'Carga horária em MWh (= MW × 1h; ENTSO-E já reporta granularidade horária exata).';
COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.year IS 'Ano derivado de ts_utc. Usado para particionamento.';
COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.month IS 'Mês derivado de ts_utc. Usado para particionamento.';

-- -----------------------------------------------------------------------------
-- Tabela 2: preco_api_hourly
-- Upstream: iceberg.bronze.preco_api_raw
-- Transformação: deduplicação por ts_utc (GROUP BY + AVG), filtra nulos PT.
-- PT e ES consultados separadamente na ENTSO-E e unidos por outer join.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.preco_api_hourly (
    ts_utc                  TIMESTAMP(6) WITH TIME ZONE,  -- timestamp horário canónico em UTC
    price_portugal_eur_mwh  DOUBLE,                        -- preço PT em €/MWh
    price_spain_eur_mwh     DOUBLE,                        -- preço ES em €/MWh (NULL se não disponível)
    year                    INTEGER,                       -- ano (partição)
    month                   INTEGER                        -- mês (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/silver/preco_api_hourly/'
);

ALTER TABLE iceberg.silver.preco_api_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.silver.preco_api_hourly IS
'Tabela Silver com preços day-ahead horários normalizados. Origem: ENTSO-E Transparency Platform (Day-Ahead Prices PT+ES, zona MIBEL). Equivalente a preco_hourly mas com fonte dinâmica via API.';

COMMENT ON COLUMN iceberg.silver.preco_api_hourly.ts_utc IS 'Timestamp UTC canónico do início da hora.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.price_portugal_eur_mwh IS 'Preço day-ahead de Portugal em €/MWh.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.price_spain_eur_mwh IS 'Preço day-ahead de Espanha em €/MWh (ENTSO-E zona ES). NULL se não houver dados para o período no outer join PT+ES.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.year IS 'Ano derivado de ts_utc. Usado para particionamento.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.month IS 'Mês derivado de ts_utc. Usado para particionamento.';
