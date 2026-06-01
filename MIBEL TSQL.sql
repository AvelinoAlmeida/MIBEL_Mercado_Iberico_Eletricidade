-- =============================================================================
-- MIBEL — Queries de exploração Trino / Iceberg
-- Responde às perguntas analíticas do DP-02 (Consumo + Preço MIBEL)
-- Catálogo: iceberg  |  Camadas: bronze · silver · gold
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0. CONECTIVIDADE — confirmar que o Trino vê os schemas e tabelas
-- -----------------------------------------------------------------------------

SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;


-- =============================================================================
-- 1. BRONZE — dados crus (o que chegou da API hoje?)
-- =============================================================================

-- Última hora ingerida de consumo e quando foi buscada
SELECT ts_utc, total AS carga_mw, fetch_date, process_date
FROM iceberg.bronze.consumo_api_raw
ORDER BY ts_utc DESC
LIMIT 24;

-- Última hora ingerida de preços PT e ES
SELECT ts_utc,
       price_portugal_eur_mwh AS preco_pt,
       price_spain_eur_mwh    AS preco_es,
       process_date
FROM iceberg.bronze.preco_api_raw
ORDER BY ts_utc DESC
LIMIT 24;

-- Quantos registos foram ingeridos por dia? (detectar dias em falta)
SELECT process_date,
       COUNT(*) AS n_horas
FROM iceberg.bronze.consumo_api_raw
GROUP BY process_date
ORDER BY process_date DESC
LIMIT 30;


-- =============================================================================
-- 2. SILVER — dados limpos (a série está completa e correcta?)
-- =============================================================================

-- Cobertura temporal da série de consumo
SELECT
    MIN(ts_utc)  AS inicio,
    MAX(ts_utc)  AS fim,
    COUNT(*)     AS total_horas
FROM iceberg.silver.consumo_api_hourly;

-- Últimas 48h de consumo normalizado (MW → MWh na transformação)
SELECT ts_utc, total_mwh
FROM iceberg.silver.consumo_api_hourly
ORDER BY ts_utc DESC
LIMIT 48;

-- Preço médio mensal PT vs ES (responde: como evoluiu o mercado?)
SELECT year, month,
       ROUND(AVG(price_portugal_eur_mwh), 2) AS avg_preco_pt,
       ROUND(AVG(price_spain_eur_mwh),    2) AS avg_preco_es,
       COUNT(*)                               AS n_horas
FROM iceberg.silver.preco_api_hourly
GROUP BY year, month
ORDER BY year DESC, month DESC;


-- =============================================================================
-- 3. GOLD — produto analítico (perguntas de negócio do DP)
-- =============================================================================

-- Q1: Qual foi o consumo e preço na última hora disponível?
SELECT ts_utc, consumo_total, market_price_pt, hora, is_weekend
FROM iceberg.gold.dp_energy_market_api_hourly
ORDER BY ts_utc DESC
LIMIT 1;

-- Q2: Qual o perfil diário médio? (consumo e preço por hora do dia)
SELECT hora,
       ROUND(AVG(consumo_total),    1) AS avg_consumo_mwh,
       ROUND(AVG(market_price_pt),  2) AS avg_preco_eur_mwh,
       COUNT(*)                        AS n_observacoes
FROM iceberg.gold.dp_energy_market_api_hourly
GROUP BY hora
ORDER BY hora;

-- Q3: Semana vs fim-de-semana — há diferença de consumo?
SELECT is_weekend,
       ROUND(AVG(consumo_total),   1) AS avg_consumo_mwh,
       ROUND(AVG(market_price_pt), 2) AS avg_preco_eur_mwh
FROM iceberg.gold.dp_energy_market_api_hourly
GROUP BY is_weekend;

-- Q4: Evolução mensal — consumo e preço médio por mês
SELECT year, month,
       ROUND(AVG(consumo_total),   1) AS avg_consumo_mwh,
       ROUND(AVG(market_price_pt), 2) AS avg_preco_eur_mwh,
       COUNT(*)                       AS n_horas
FROM iceberg.gold.dp_energy_market_api_hourly
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- Q5: Existe correlação entre consumo e preço? (top 10 horas mais caras)
SELECT ts_utc, hora, is_weekend,
       ROUND(consumo_total,   1) AS consumo_mwh,
       ROUND(market_price_pt, 2) AS preco_eur_mwh
FROM iceberg.gold.dp_energy_market_api_hourly
ORDER BY market_price_pt DESC
LIMIT 10;

-- Q6: Últimas 24h com lags — o modelo tem contexto suficiente?
SELECT ts_utc, consumo_total, market_price_pt,
       consumo_lag_1h, consumo_lag_24h,
       rolling_avg_consumo_24h, rolling_avg_price_24h
FROM iceberg.gold.dp_energy_market_api_hourly
ORDER BY ts_utc DESC
LIMIT 24;


-- =============================================================================
-- 4. ML — métricas do modelo de load forecasting
-- =============================================================================

-- Q7: Qual a performance do modelo mais recente?
SELECT model_name,
       ROUND(mae,  1) AS mae_mwh,
       ROUND(rmse, 1) AS rmse_mwh,
       ROUND(r2,   3) AS r2,
       ROUND(mape, 2) AS mape_pct,
       n_train, n_test,
       run_ts
FROM iceberg.gold.ml_metrics_streaming
ORDER BY run_ts DESC
LIMIT 1;

-- Q8: Quais as features mais importantes para a previsão?
SELECT rank, feature_name,
       ROUND(importance, 4) AS importance
FROM iceberg.gold.ml_feature_importance_streaming
ORDER BY rank;


-- =============================================================================
-- 5. FRESHNESS — os dados estão actualizados?
-- =============================================================================

-- Quanto tempo passou desde o último registo em cada camada?
SELECT
    'bronze.consumo_api_raw'          AS tabela,
    CAST(MAX(ts_utc) AS VARCHAR)      AS ultimo_ts,
    DATE_DIFF('hour', MAX(ts_utc), NOW()) AS horas_atras
FROM iceberg.bronze.consumo_api_raw

UNION ALL

SELECT
    'silver.consumo_api_hourly',
    CAST(MAX(ts_utc) AS VARCHAR),
    DATE_DIFF('hour', MAX(ts_utc), NOW())
FROM iceberg.silver.consumo_api_hourly

UNION ALL

SELECT
    'gold.dp_energy_market_api_hourly',
    CAST(MAX(ts_utc) AS VARCHAR),
    DATE_DIFF('hour', MAX(ts_utc), NOW())
FROM iceberg.gold.dp_energy_market_api_hourly;

-- Snapshots Iceberg da tabela Gold (histórico de writes)
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.gold."dp_energy_market_api_hourly$snapshots"
ORDER BY committed_at DESC
LIMIT 5;
