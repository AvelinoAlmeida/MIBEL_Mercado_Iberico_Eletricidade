-- =============================================================================
-- Bronze DDL -- Streaming_Data (DP-02 API pipeline)
-- Tabelas Iceberg com sufixo _api para coexistir com o pipeline estatico.
-- Diferenca face ao estatico: ts_utc ja normalizado na ingesto (sem date_raw/hour).
--
-- Fonte de dados: Energy-Charts API (api.energy-charts.info) — sem autenticacao
--   consumo_api_raw : Carga total nacional PT  -- endpoint total_power?country=pt
--   preco_api_raw   : Precos day-ahead PT+ES   -- endpoint price?bzn=PT / price?bzn=ES
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

-- -----------------------------------------------------------------------------
-- Tabela 1: consumo_api_raw
-- Origem: Energy-Charts API — endpoint total_power?country=pt (ENTSO-E load data)
-- Granularidade horária (MW → convertido para MWh na Silver).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.bronze.consumo_api_raw (
    ts_utc          TIMESTAMP(6) WITH TIME ZONE,  -- timestamp UTC horário (início da hora)
    total           DOUBLE,                        -- carga total nacional (MW)
    source_url      VARCHAR,                       -- URL da chamada API (rastreabilidade)
    fetch_date      DATE,                          -- data em que foi feita a chamada à API
    process_date    DATE                           -- data lógica de ingestão (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/consumo_api_raw/'
);

ALTER TABLE iceberg.bronze.consumo_api_raw
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.bronze.consumo_api_raw IS
'Tabela Bronze com carga elétrica nacional horária obtida via Energy-Charts API (dados ENTSO-E). Timestamp UTC já normalizado na ingestão.';

COMMENT ON COLUMN iceberg.bronze.consumo_api_raw.ts_utc IS 'Timestamp UTC do início da hora, derivado do unix_seconds da resposta da API.';
COMMENT ON COLUMN iceberg.bronze.consumo_api_raw.total IS 'Carga total nacional em MW conforme reportado pela ENTSO-E via Energy-Charts.';
COMMENT ON COLUMN iceberg.bronze.consumo_api_raw.source_url IS 'URL completo da chamada à API para rastreabilidade e reprocessamento.';
COMMENT ON COLUMN iceberg.bronze.consumo_api_raw.fetch_date IS 'Data em que a chamada à API foi realizada.';
COMMENT ON COLUMN iceberg.bronze.consumo_api_raw.process_date IS 'Data lógica de ingestão usada para idempotência e particionamento.';

-- -----------------------------------------------------------------------------
-- Tabela 2: preco_api_raw
-- Origem: Energy-Charts API — endpoints price?bzn=PT e price?bzn=ES (preços OMIE/MIBEL day-ahead)
-- Granularidade horária, €/MWh. Preço ES pode ser NULL se o endpoint não retornar dados.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.bronze.preco_api_raw (
    ts_utc                  TIMESTAMP(6) WITH TIME ZONE,  -- timestamp UTC horário
    price_portugal_eur_mwh  DOUBLE,                        -- preço day-ahead PT (€/MWh)
    price_spain_eur_mwh     DOUBLE,                        -- preço day-ahead ES (€/MWh; NULL se endpoint não retornar dados ES)
    source_url              VARCHAR,                       -- URL da chamada API
    fetch_date              DATE,
    process_date            DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/preco_api_raw/'
);

ALTER TABLE iceberg.bronze.preco_api_raw
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.bronze.preco_api_raw IS
'Tabela Bronze com preços day-ahead horários obtidos via Energy-Charts API (zona de balanço PT). Timestamp UTC já normalizado na ingestão.';

COMMENT ON COLUMN iceberg.bronze.preco_api_raw.ts_utc IS 'Timestamp UTC do início da hora, derivado do unix_seconds da resposta da API.';
COMMENT ON COLUMN iceberg.bronze.preco_api_raw.price_portugal_eur_mwh IS 'Preço day-ahead de Portugal em €/MWh conforme OMIE via Energy-Charts.';
COMMENT ON COLUMN iceberg.bronze.preco_api_raw.price_spain_eur_mwh IS 'Preço day-ahead de Espanha em €/MWh. NULL quando o endpoint não disponibiliza ES separado.';
COMMENT ON COLUMN iceberg.bronze.preco_api_raw.source_url IS 'URL completo da chamada à API para rastreabilidade.';
COMMENT ON COLUMN iceberg.bronze.preco_api_raw.process_date IS 'Data lógica de ingestão usada para idempotência e particionamento.';
