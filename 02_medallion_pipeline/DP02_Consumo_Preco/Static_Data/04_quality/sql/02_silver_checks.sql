-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA SILVER (consumo_preco)
-- Executa no Trino. Retorna uma linha por verificação com colunas:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXA DE NULOS — ts_utc em consumo_hourly (chave temporal crítica)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_hourly » null_rate » ts_utc'           AS check_name,
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                       AS status,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                        AS valor_pct,
    0.0                                                    AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas') AS detalhe
FROM iceberg.silver.consumo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 2. TAXA DE NULOS — total_mwh em consumo_hourly (métrica de consumo agregada)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_hourly » null_rate » total_mwh',
    CASE WHEN SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.consumo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 3. TAXA DE NULOS — ts_utc em preco_hourly
-- -----------------------------------------------------------------------------
SELECT
    'silver.preco_hourly » null_rate » ts_utc',
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.preco_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 4. TAXA DE NULOS — price_portugal_eur_mwh em preco_hourly
-- -----------------------------------------------------------------------------
SELECT
    'silver.preco_hourly » null_rate » price_portugal_eur_mwh',
    CASE WHEN SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.preco_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 5. TAXA DE NULOS — price_spain_eur_mwh em preco_hourly
-- -----------------------------------------------------------------------------
SELECT
    'silver.preco_hourly » null_rate » price_spain_eur_mwh',
    CASE WHEN SUM(CASE WHEN price_spain_eur_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_spain_eur_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_spain_eur_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.preco_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 6. RANGE — consumo horário positivo (> 0 MWh após agregação SUM/1000)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_hourly » range » total_mwh > 0',
    CASE WHEN SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' horas com consumo <= 0 MWh')
FROM iceberg.silver.consumo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 7. RANGE — preço PT não-negativo (mercado MIBEL pode ter preços negativos, WARN)
-- -----------------------------------------------------------------------------
SELECT
    'silver.preco_hourly » range » price_portugal_eur_mwh >= 0',
    CASE WHEN SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' horas com preço PT negativo (possível: mercado negativo)')
FROM iceberg.silver.preco_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 8. UNICIDADE — ts_utc único em consumo_hourly (sem horas duplicadas)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' horas duplicadas')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.silver.consumo_hourly
        GROUP BY ts_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 9. UNICIDADE — ts_utc único em preco_hourly
-- -----------------------------------------------------------------------------
SELECT
    'silver.preco_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' horas duplicadas')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.silver.preco_hourly
        GROUP BY ts_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 10. ALINHAMENTO TEMPORAL — ts_utc deve estar em fronteiras de hora exactas
--     (minuto = 0, segundo = 0): garante que a agregação 15min → 1h foi correcta
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_hourly » temporal » ts_utc em fronteira de hora',
    CASE WHEN SUM(CASE WHEN MINUTE(ts_utc) <> 0 OR SECOND(ts_utc) <> 0
                       THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN MINUTE(ts_utc) <> 0 OR SECOND(ts_utc) <> 0
                            THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN MINUTE(ts_utc) <> 0 OR SECOND(ts_utc) <> 0
                         THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos fora de fronteira de hora (minuto ou segundo != 0)')
FROM iceberg.silver.consumo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 11. COBERTURA DO JOIN — % de horas de consumo com preço correspondente
--     Threshold: >= 95% (pode haver dias sem publicação de preço pelo OMIE)
-- -----------------------------------------------------------------------------
SELECT
    'silver » join_coverage » consumo_hourly com preco_hourly >= 95%',
    CASE WHEN coverage_pct >= 95.0 THEN 'PASS' ELSE 'WARN' END,
    coverage_pct,
    95.0,
    CONCAT(CAST(matched AS VARCHAR), ' de ', CAST(total_consumo AS VARCHAR),
           ' horas com preço correspondente')
FROM (
    SELECT
        COUNT(c.ts_utc)                                                      AS total_consumo,
        COUNT(p.ts_utc)                                                      AS matched,
        ROUND(100.0 * COUNT(p.ts_utc) / NULLIF(COUNT(c.ts_utc), 0), 2)      AS coverage_pct
    FROM iceberg.silver.consumo_hourly AS c
    LEFT JOIN iceberg.silver.preco_hourly AS p ON c.ts_utc = p.ts_utc
) AS join_stats

UNION ALL

-- -----------------------------------------------------------------------------
-- 12. COMPLETUDE DIÁRIA — dias com menos de 23 horas em consumo_hourly
--     Em UTC não há DST: cada dia deve ter exactamente 24 horas
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_hourly » completeness » horas_por_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 horas de consumo')
FROM (
    SELECT CAST(ts_utc AS DATE) AS dt, COUNT(*) AS cnt
    FROM iceberg.silver.consumo_hourly
    GROUP BY CAST(ts_utc AS DATE)
) AS daily_counts

UNION ALL

-- -----------------------------------------------------------------------------
-- 13. COMPLETUDE DIÁRIA — dias com menos de 23 horas em preco_hourly
-- -----------------------------------------------------------------------------
SELECT
    'silver.preco_hourly » completeness » horas_por_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 horas de preços')
FROM (
    SELECT CAST(ts_utc AS DATE) AS dt, COUNT(*) AS cnt
    FROM iceberg.silver.preco_hourly
    GROUP BY CAST(ts_utc AS DATE)
) AS daily_counts

ORDER BY status DESC, check_name;
