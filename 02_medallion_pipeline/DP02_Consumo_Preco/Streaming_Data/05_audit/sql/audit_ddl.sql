-- =============================================================================
-- DDL — Auditoria operacional (DP-02 Streaming_Data)
-- Schema  : iceberg.audit
-- Tabelas : pipeline_runs | dataset_lineage
-- Catálogo: Iceberg (ACID + schema evolution)
-- Storage : s3a://warehouse/audit/ (MinIO)
--
-- Idempotente — seguro para executar múltiplas vezes (CREATE IF NOT EXISTS).
-- Aplicado automaticamente pelo orquestrador antes de qualquer execução do pipeline.
--
-- Contexto:
--   O DP-02 Streaming regista cada execução em pipeline_runs e propaga o grafo
--   de linhagem em dataset_lineage. Estas tabelas são consultadas pelos checks
--   de qualidade (04_quality/) e pelo dashboard Grafana de monitorização.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.audit
WITH (location = 's3a://warehouse/audit/');

-- ---------------------------------------------------------------------------
-- pipeline_runs — registo de cada execução do pipeline
-- ---------------------------------------------------------------------------
-- Uma linha por execução; status = SUCCESS | FAILED.
-- rows_bronze/silver/gold: contagem de linhas escritas em cada camada nessa run.
-- param_start_date/end_date: janela temporal dos dados ingeridos (ISO 8601).
-- error_message: NULL em execuções bem-sucedidas; stacktrace resumido em FAILED.
-- Particionado por dia (start_ts) para consultas temporais eficientes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.audit.pipeline_runs (
    run_id            VARCHAR,                        -- UUID único por execução
    pipeline_name     VARCHAR,                        -- ex: "dp02_streaming"
    pipeline_version  VARCHAR,                        -- versão semântica do pipeline
    start_ts          TIMESTAMP(6) WITH TIME ZONE,    -- início da execução (UTC)
    end_ts            TIMESTAMP(6) WITH TIME ZONE,    -- fim da execução (UTC)
    duration_seconds  DOUBLE,                         -- end_ts - start_ts em segundos
    status            VARCHAR,                        -- SUCCESS | FAILED
    rows_bronze       BIGINT,                         -- linhas inseridas na camada Bronze
    rows_silver       BIGINT,                         -- linhas inseridas na camada Silver
    rows_gold         BIGINT,                         -- linhas inseridas na camada Gold
    source            VARCHAR,                        -- origem dos dados (ex: "energy-charts-api")
    param_start_date  VARCHAR,                        -- data de início do intervalo ingerido
    param_end_date    VARCHAR,                        -- data de fim do intervalo ingerido
    error_message     VARCHAR                         -- mensagem de erro (NULL se SUCCESS)
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['day(start_ts)']
);

-- ---------------------------------------------------------------------------
-- dataset_lineage — mapa upstream → downstream por execução
-- ---------------------------------------------------------------------------
-- Materializado a cada run; permite rastrear:
--   "que tabelas alimentaram esta gold em run_id=X?"
-- upstream/downstream: nome qualificado da tabela (ex: "hive.bronze.streaming_consumo").
-- Não particionado — volume reduzido (≈ 3–5 linhas por run).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.audit.dataset_lineage (
    run_id      VARCHAR,                        -- referência a pipeline_runs.run_id
    upstream    VARCHAR,                        -- tabela de origem (produtora)
    downstream  VARCHAR,                        -- tabela de destino (consumidora)
    recorded_at TIMESTAMP(6) WITH TIME ZONE     -- momento do registo (UTC)
)
WITH (
    format = 'PARQUET'
);
