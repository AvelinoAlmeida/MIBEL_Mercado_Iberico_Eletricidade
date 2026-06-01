-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA BRONZE (Streaming_Data / API)
-- Tabelas: iceberg.bronze.consumo_api_raw + iceberg.bronze.preco_api_raw
-- Retorna: check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- 1. TAXA DE NULOS — ts_utc em consumo_api_raw
SELECT
    'bronze.consumo_api_raw » null_rate » ts_utc'           AS check_name,
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                        AS status,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                         AS valor_pct,
    0.0                                                     AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas') AS detalhe
FROM iceberg.bronze.consumo_api_raw

UNION ALL

-- 2. TAXA DE NULOS — total em consumo_api_raw
SELECT
    'bronze.consumo_api_raw » null_rate » total',
    CASE WHEN SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.bronze.consumo_api_raw

UNION ALL

-- 3. TAXA DE NULOS — ts_utc em preco_api_raw
SELECT
    'bronze.preco_api_raw » null_rate » ts_utc',
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.bronze.preco_api_raw

UNION ALL

-- 4. TAXA DE NULOS — price_portugal_eur_mwh em preco_api_raw
SELECT
    'bronze.preco_api_raw » null_rate » price_portugal_eur_mwh',
    CASE WHEN SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.bronze.preco_api_raw

UNION ALL

-- 5. RANGE — carga total deve ser positiva (MW); consumo PT tipicamente entre 3 000–11 000 MW
--    WARN (não FAIL) porque a Bronze preserva raw: falsos negativos são filtrados na Silver
SELECT
    'bronze.consumo_api_raw » range » total > 0',
    CASE WHEN SUM(CASE WHEN total <= 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN total <= 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total <= 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com total <= 0 MW')
FROM iceberg.bronze.consumo_api_raw

UNION ALL

-- 6. RANGE — preços não-negativos (MIBEL pode ter preços negativos em excesso renovável)
SELECT
    'bronze.preco_api_raw » range » price_portugal_eur_mwh >= 0',
    CASE WHEN SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com preço negativo (pode ser normal no MIBEL)')
FROM iceberg.bronze.preco_api_raw

UNION ALL

-- 7. UNICIDADE — sem ts_utc duplicados em consumo_api_raw
--    WARN (não FAIL): a Silver deduplica com ROW_NUMBER(); duplicados na Bronze são recuperáveis
SELECT
    'bronze.consumo_api_raw » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT ts_utc
        FROM iceberg.bronze.consumo_api_raw
        GROUP BY ts_utc
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- 8. UNICIDADE — sem ts_utc duplicados em preco_api_raw
--    FAIL (mais severo que consumo): preços duplicados corromperiam joins Silver→Gold sem sinal claro
SELECT
    'bronze.preco_api_raw » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT ts_utc
        FROM iceberg.bronze.preco_api_raw
        GROUP BY ts_utc
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- 9. FRESHNESS — dados de consumo têm menos de 3 dias de atraso
--    ENTSO-E transparency platform publica consumo com até 2 dias de latência; 3 dias dá margem operacional
SELECT
    'bronze.consumo_api_raw » freshness » max_process_date',
    CASE WHEN date_diff('day', MAX(process_date), CURRENT_DATE) <= 3
         THEN 'PASS' ELSE 'WARN' END,
    CAST(date_diff('day', MAX(process_date), CURRENT_DATE) AS DECIMAL(18,2)),
    3.0,
    CONCAT('Último process_date: ', CAST(MAX(process_date) AS VARCHAR),
           ' (', CAST(date_diff('day', MAX(process_date), CURRENT_DATE) AS VARCHAR), ' dias atrás)')
FROM iceberg.bronze.consumo_api_raw

UNION ALL

-- 10. FRESHNESS — dados de preços têm menos de 2 dias de atraso (day-ahead publica D-1)
SELECT
    'bronze.preco_api_raw » freshness » max_process_date',
    CASE WHEN date_diff('day', MAX(process_date), CURRENT_DATE) <= 2
         THEN 'PASS' ELSE 'WARN' END,
    CAST(date_diff('day', MAX(process_date), CURRENT_DATE) AS DECIMAL(18,2)),
    2.0,
    CONCAT('Último process_date: ', CAST(MAX(process_date) AS VARCHAR),
           ' (', CAST(date_diff('day', MAX(process_date), CURRENT_DATE) AS VARCHAR), ' dias atrás)')
FROM iceberg.bronze.preco_api_raw

UNION ALL

-- 11. COMPLETUDE — dias com menos de 23 horas de consumo
--    23h (não 24h) porque nas transições DST (verão/inverno) um dia tem legitimamente 23 ou 25 horas
SELECT
    'bronze.consumo_api_raw » completeness » horas_por_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 horas de dados')
FROM (
    SELECT process_date, COUNT(*) AS cnt
    FROM iceberg.bronze.consumo_api_raw
    GROUP BY process_date
) AS daily_counts

UNION ALL

-- 12. COMPLETUDE — dias com menos de 23 horas de preços (mesmo critério DST do check 11)
SELECT
    'bronze.preco_api_raw » completeness » horas_por_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 horas de preços')
FROM (
    SELECT process_date, COUNT(*) AS cnt
    FROM iceberg.bronze.preco_api_raw
    GROUP BY process_date
) AS daily_counts

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: dias com dados em falta (exploratório)
-- =============================================================================
SELECT
    process_date,
    COUNT(*)                            AS horas_existentes,
    24 - COUNT(*)                       AS horas_em_falta,
    ROUND(100.0 * COUNT(*) / 24.0, 1)  AS pct_completo
FROM iceberg.bronze.consumo_api_raw
GROUP BY process_date
HAVING COUNT(*) < 24
ORDER BY process_date;
