-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA GOLD (Streaming_Data / API)
-- Tabelas: iceberg.gold.dp_energy_market_api_hourly
--        + iceberg.gold.feat_load_forecasting_api_hourly
--
-- Estrutura de resultado por check:
--   check_name   — identificador no formato <tabela> » <categoria> » <coluna>
--   status       — PASS | WARN | FAIL
--   valor_pct    — valor observado (%, contagem, ou count absoluto conforme o check)
--   threshold_pct— limiar de referência (0.0 = tolerância zero; outro valor = margem aceitável)
--   detalhe      — mensagem legível com contagens concretas
--
-- Categorias de checks:
--   null_rate      — colunas obrigatórias não podem ter nulos
--   uniqueness     — ts_utc deve ser chave primária (sem duplicados)
--   range          — valores temporais devem estar dentro de intervalos válidos
--   lag_null_rate  — features de lag têm nulos esperados apenas nas primeiras N horas
--   join_integrity — contagem Gold deve corresponder ao join Silver×Silver
--   count          — feature table ML deve ter exatamente dp_count - 24 linhas
-- =============================================================================

-- 1. TAXA DE NULOS — ts_utc em dp_energy_market_api_hourly
SELECT
    'gold.dp_energy_market_api_hourly » null_rate » ts_utc'  AS check_name,
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                         AS status,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                          AS valor_pct,
    0.0                                                      AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas') AS detalhe
FROM iceberg.gold.dp_energy_market_api_hourly

UNION ALL

-- 2. TAXA DE NULOS — consumo_total em dp_energy_market_api_hourly
SELECT
    'gold.dp_energy_market_api_hourly » null_rate » consumo_total',
    CASE WHEN SUM(CASE WHEN consumo_total IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_total IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_total IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.gold.dp_energy_market_api_hourly

UNION ALL

-- 3. TAXA DE NULOS — market_price_pt em dp_energy_market_api_hourly
SELECT
    'gold.dp_energy_market_api_hourly » null_rate » market_price_pt',
    CASE WHEN SUM(CASE WHEN market_price_pt IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN market_price_pt IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN market_price_pt IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.gold.dp_energy_market_api_hourly

UNION ALL

-- 4. UNICIDADE — sem ts_utc duplicados em dp_energy_market_api_hourly
SELECT
    'gold.dp_energy_market_api_hourly » uniqueness » ts_utc',
    CASE WHEN dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_count AS VARCHAR), ' timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_count
    FROM (
        SELECT ts_utc
        FROM iceberg.gold.dp_energy_market_api_hourly
        GROUP BY ts_utc
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- 5. RANGE — hora válida (0-23)
SELECT
    'gold.dp_energy_market_api_hourly » range » hora BETWEEN 0 AND 23',
    CASE WHEN SUM(CASE WHEN hora < 0 OR hora > 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN hora < 0 OR hora > 23 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN hora < 0 OR hora > 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com hora inválida')
FROM iceberg.gold.dp_energy_market_api_hourly

UNION ALL

-- 6. RANGE — dia_semana válido (0-6)
SELECT
    'gold.dp_energy_market_api_hourly » range » dia_semana BETWEEN 0 AND 6',
    CASE WHEN SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com dia_semana inválido')
FROM iceberg.gold.dp_energy_market_api_hourly

UNION ALL

-- 7. LAGS — taxa de nulos em consumo_lag_1h (1ª linha de cada sessão é NULL por design; threshold 1%)
SELECT
    'gold.dp_energy_market_api_hourly » lag_null_rate » consumo_lag_1h',
    CASE WHEN ROUND(100.0 * SUM(CASE WHEN consumo_lag_1h IS NULL THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2) <= 1.0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_lag_1h IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    1.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_lag_1h IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em consumo_lag_1h (esperado: apenas 1ª hora)')
FROM iceberg.gold.dp_energy_market_api_hourly

UNION ALL

-- 8. LAGS — taxa de nulos em consumo_lag_24h (primeiras 24h são NULL; threshold 5% para datasets curtos)
SELECT
    'gold.dp_energy_market_api_hourly » lag_null_rate » consumo_lag_24h',
    CASE WHEN ROUND(100.0 * SUM(CASE WHEN consumo_lag_24h IS NULL THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2) <= 5.0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_lag_24h IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    5.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_lag_24h IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em consumo_lag_24h (esperado: primeiras 24h)')
FROM iceberg.gold.dp_energy_market_api_hourly

UNION ALL

-- 9. INTEGRIDADE — Gold deve ter o mesmo COUNT que o INNER JOIN Silver×Silver (tolerância ±5 linhas por possível atraso de ingestão)
SELECT
    'gold.dp_energy_market_api_hourly » join_integrity » count_vs_silver',
    CASE WHEN gold_count = silver_join_count THEN 'PASS'
         WHEN ABS(gold_count - silver_join_count) <= 5 THEN 'WARN'
         ELSE 'FAIL' END,
    CAST(gold_count AS DECIMAL(18,2)),
    CAST(silver_join_count AS DECIMAL(18,2)),
    CONCAT('Gold: ', CAST(gold_count AS VARCHAR),
           ' | Silver join: ', CAST(silver_join_count AS VARCHAR))
FROM (
    SELECT
        (SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_api_hourly) AS gold_count,
        (SELECT COUNT(*)
         FROM iceberg.silver.consumo_api_hourly c
         INNER JOIN iceberg.silver.preco_api_hourly p ON c.ts_utc = p.ts_utc) AS silver_join_count
) AS counts

UNION ALL

-- 10. ML FEATURE TABLE — count deve ser Gold - 24 (target consumo_next_hour requer 24h de lookahead; últimas 24 linhas excluídas)
SELECT
    'gold.feat_load_forecasting_api_hourly » count » dp_minus_1',
    CASE WHEN ABS(feat_count - (dp_count - 24)) <= 50 THEN 'PASS' ELSE 'WARN' END,
    CAST(feat_count AS DECIMAL(18,2)),
    CAST(dp_count - 24 AS DECIMAL(18,2)),
    CONCAT('feat: ', CAST(feat_count AS VARCHAR),
           ' | dp-24: ', CAST(dp_count - 24 AS VARCHAR))
FROM (
    SELECT
        (SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_api_hourly) AS feat_count,
        (SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_api_hourly)      AS dp_count
) AS counts

UNION ALL

-- 11. ML FEATURE TABLE — sem nulos no target consumo_next_hour
SELECT
    'gold.feat_load_forecasting_api_hourly » null_rate » consumo_next_hour',
    CASE WHEN SUM(CASE WHEN consumo_next_hour IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_next_hour IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_next_hour IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos no target (esperado: 0)')
FROM iceberg.gold.feat_load_forecasting_api_hourly

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: estatísticas descritivas da tabela Gold principal
-- Útil para inspecção rápida de valores médios, cobertura temporal e
-- presença de nulos nas rolling features (rolling_avg_consumo_24h fica NULL
-- nas primeiras 24h após cada reinício do pipeline).
-- =============================================================================
SELECT
    COUNT(*)                                                                 AS total_linhas,
    SUM(CASE WHEN rolling_avg_consumo_24h IS NULL THEN 1 ELSE 0 END)        AS null_rolling_consumo,
    ROUND(AVG(consumo_total), 2)                                             AS avg_consumo_mwh,
    ROUND(AVG(market_price_pt), 2)                                           AS avg_price_eur_mwh,
    CAST(MIN(ts_utc) AS VARCHAR)                                             AS inicio,
    CAST(MAX(ts_utc) AS VARCHAR)                                             AS fim
FROM iceberg.gold.dp_energy_market_api_hourly;
