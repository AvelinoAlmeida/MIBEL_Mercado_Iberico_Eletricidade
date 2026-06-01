-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA GOLD (consumo_preco)
-- Executa no Trino. Retorna uma linha por verificação com colunas:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXA DE NULOS — ts_utc em dp_energy_market_hourly (chave de negócio)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » null_rate » ts_utc'    AS check_name,
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                       AS status,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                        AS valor_pct,
    0.0                                                    AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas') AS detalhe
FROM iceberg.gold.dp_energy_market_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 2. TAXA DE NULOS — consumo_total em dp_energy_market_hourly
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » null_rate » consumo_total',
    CASE WHEN SUM(CASE WHEN consumo_total IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_total IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_total IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.gold.dp_energy_market_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 3. TAXA DE NULOS — market_price_pt em dp_energy_market_hourly
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » null_rate » market_price_pt',
    CASE WHEN SUM(CASE WHEN market_price_pt IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN market_price_pt IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN market_price_pt IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.gold.dp_energy_market_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 4. RANGE — hora do dia entre 0 e 23
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » range » hora BETWEEN 0 AND 23',
    CASE WHEN SUM(CASE WHEN hora < 0 OR hora > 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN hora < 0 OR hora > 23 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN hora < 0 OR hora > 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com hora fora de [0,23]')
FROM iceberg.gold.dp_energy_market_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 5. RANGE — dia da semana entre 0 (Segunda) e 6 (Domingo)
--    DAY_OF_WEEK retorna 1=Dom…7=Sáb; o workflow aplica -1 → 0=Seg…6=Dom
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » range » dia_semana BETWEEN 0 AND 6',
    CASE WHEN SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com dia_semana fora de [0,6]')
FROM iceberg.gold.dp_energy_market_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 6. UNICIDADE — ts_utc único em dp_energy_market_hourly
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' ts_utc duplicados')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.gold.dp_energy_market_hourly
        GROUP BY ts_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 7. UNICIDADE — ts_utc único em feat_load_forecasting_hourly
-- -----------------------------------------------------------------------------
SELECT
    'gold.feat_load_forecasting_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' ts_utc duplicados')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.gold.feat_load_forecasting_hourly
        GROUP BY ts_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 8. ML TARGET — consumo_next_hour não deve ter nulos em feat_load_forecasting
--    (o workflow já filtra NULL no LEAD, mas verificamos por segurança)
-- -----------------------------------------------------------------------------
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » consumo_next_hour',
    CASE WHEN SUM(CASE WHEN consumo_next_hour IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_next_hour IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_next_hour IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' exemplos sem target (consumo_next_hour)')
FROM iceberg.gold.feat_load_forecasting_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 9. FEATURES ML — ausência de nulos nos lags críticos de feat_load_forecasting
--    O workflow já filtra estes nulos, mas verifica por segurança
-- -----------------------------------------------------------------------------
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » consumo_lag_1h',
    CASE WHEN SUM(CASE WHEN consumo_lag_1h IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_lag_1h IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_lag_1h IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' exemplos sem consumo_lag_1h')
FROM iceberg.gold.feat_load_forecasting_hourly

UNION ALL

SELECT
    'gold.feat_load_forecasting_hourly » null_rate » consumo_lag_24h',
    CASE WHEN SUM(CASE WHEN consumo_lag_24h IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_lag_24h IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_lag_24h IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' exemplos sem consumo_lag_24h')
FROM iceberg.gold.feat_load_forecasting_hourly

UNION ALL

SELECT
    'gold.feat_load_forecasting_hourly » null_rate » price_lag_1h',
    CASE WHEN SUM(CASE WHEN price_lag_1h IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_lag_1h IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_lag_1h IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' exemplos sem price_lag_1h')
FROM iceberg.gold.feat_load_forecasting_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 10. PARIDADE DE LINHAS — dp e feat devem ter contagens próximas
--     A feat perde as primeiras horas (sem histórico para lags) e a última
--     hora de cada partição (sem LEAD disponível). Tolerância de 48 linhas.
-- -----------------------------------------------------------------------------
SELECT
    'gold » row_count_parity » dp vs feat (diferença <= 48)',
    CASE WHEN ABS(dp_count - feat_count) <= 48 THEN 'PASS' ELSE 'WARN' END,
    CAST(ABS(dp_count - feat_count) AS DECIMAL(18,2)),
    48.0,
    CONCAT('dp=', CAST(dp_count AS VARCHAR), ' feat=', CAST(feat_count AS VARCHAR),
           ' diferença=', CAST(dp_count - feat_count AS VARCHAR))
FROM (
    SELECT
        (SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_hourly)      AS dp_count,
        (SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_hourly) AS feat_count
) AS counts

UNION ALL

-- -----------------------------------------------------------------------------
-- 11. CONSISTÊNCIA DO LAG — consumo_lag_1h deve corresponder ao consumo_total
--     da hora anterior. Verifica por self-join; tolerância de 0.01 MWh.
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » lag_consistency » consumo_lag_1h',
    CASE WHEN pct_inconsistent < 0.1 THEN 'PASS' ELSE 'FAIL' END,
    pct_inconsistent,
    0.1,
    CONCAT(CAST(inconsistent AS VARCHAR), ' de ', CAST(checked AS VARCHAR),
           ' registos amostrados com lag_1h inconsistente (tolerância 0.01 MWh)')
FROM (
    SELECT
        COUNT(*)                                                                AS checked,
        SUM(CASE WHEN ABS(dp.consumo_lag_1h - prev.consumo_total) > 0.01
                 THEN 1 ELSE 0 END)                                             AS inconsistent,
        ROUND(100.0 * SUM(CASE WHEN ABS(dp.consumo_lag_1h - prev.consumo_total) > 0.01
                               THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 4)                                         AS pct_inconsistent
    FROM iceberg.gold.dp_energy_market_hourly AS dp
    JOIN iceberg.gold.dp_energy_market_hourly AS prev
        ON dp.ts_utc = date_add('hour', 1, prev.ts_utc)
    WHERE dp.consumo_lag_1h IS NOT NULL
) AS lag_check

UNION ALL

-- -----------------------------------------------------------------------------
-- 12. CONSISTÊNCIA DO LAG — price_lag_1h deve corresponder ao market_price_pt
--     da hora anterior. Tolerância de 0.01 €/MWh.
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energy_market_hourly » lag_consistency » price_lag_1h',
    CASE WHEN pct_inconsistent < 0.1 THEN 'PASS' ELSE 'FAIL' END,
    pct_inconsistent,
    0.1,
    CONCAT(CAST(inconsistent AS VARCHAR), ' de ', CAST(checked AS VARCHAR),
           ' registos com price_lag_1h inconsistente (tolerância 0.01 €/MWh)')
FROM (
    SELECT
        COUNT(*)                                                                    AS checked,
        SUM(CASE WHEN ABS(dp.price_lag_1h - prev.market_price_pt) > 0.01
                 THEN 1 ELSE 0 END)                                                 AS inconsistent,
        ROUND(100.0 * SUM(CASE WHEN ABS(dp.price_lag_1h - prev.market_price_pt) > 0.01
                               THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 4)                                             AS pct_inconsistent
    FROM iceberg.gold.dp_energy_market_hourly AS dp
    JOIN iceberg.gold.dp_energy_market_hourly AS prev
        ON dp.ts_utc = date_add('hour', 1, prev.ts_utc)
    WHERE dp.price_lag_1h IS NOT NULL
) AS price_lag_check

ORDER BY status DESC, check_name;
