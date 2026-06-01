-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA BRONZE (consumo_preco)
-- Executa no Trino. Retorna uma linha por verificação com colunas:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXA DE NULOS — datahora em consumo_raw (chave temporal crítica)
-- -----------------------------------------------------------------------------
SELECT
    'bronze.consumo_raw » null_rate » datahora'            AS check_name,
    CASE WHEN SUM(CASE WHEN datahora IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                       AS status,
    ROUND(100.0 * SUM(CASE WHEN datahora IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                        AS valor_pct,
    0.0                                                    AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN datahora IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas') AS detalhe
FROM iceberg.bronze.consumo_raw

UNION ALL

-- -----------------------------------------------------------------------------
-- 2. TAXA DE NULOS — total em consumo_raw (métrica principal de consumo)
-- -----------------------------------------------------------------------------
SELECT
    'bronze.consumo_raw » null_rate » total',
    CASE WHEN SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.bronze.consumo_raw

UNION ALL

-- -----------------------------------------------------------------------------
-- 3. TAXA DE NULOS — price_portugal_raw em preco_raw
-- -----------------------------------------------------------------------------
SELECT
    'bronze.preco_raw » null_rate » price_portugal_raw',
    CASE WHEN SUM(CASE WHEN price_portugal_raw IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_raw IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_raw IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.bronze.preco_raw

UNION ALL

-- -----------------------------------------------------------------------------
-- 4. TAXA DE NULOS — price_spain_raw em preco_raw
-- -----------------------------------------------------------------------------
SELECT
    'bronze.preco_raw » null_rate » price_spain_raw',
    CASE WHEN SUM(CASE WHEN price_spain_raw IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_spain_raw IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_spain_raw IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.bronze.preco_raw

UNION ALL

-- -----------------------------------------------------------------------------
-- 5. RANGE — consumo total deve ser positivo (kW por intervalo de 15 min)
-- -----------------------------------------------------------------------------
SELECT
    'bronze.consumo_raw » range » total > 0',
    CASE WHEN SUM(CASE WHEN total <= 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN total <= 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total <= 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com total <= 0 kW')
FROM iceberg.bronze.consumo_raw

UNION ALL

-- -----------------------------------------------------------------------------
-- 6. RANGE — hora dos preços entre 1 e 25 (hora 25 = DST outono, filtrada na Silver)
-- -----------------------------------------------------------------------------
SELECT
    'bronze.preco_raw » range » hour BETWEEN 1 AND 25',
    CASE WHEN SUM(CASE WHEN hour < 1 OR hour > 25 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN hour < 1 OR hour > 25 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN hour < 1 OR hour > 25 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos fora do intervalo 1-25')
FROM iceberg.bronze.preco_raw

UNION ALL

-- -----------------------------------------------------------------------------
-- 7. RANGE — preços não-negativos (preços MIBEL podem ser 0 mas raramente negativos)
-- -----------------------------------------------------------------------------
SELECT
    'bronze.preco_raw » range » price_portugal_raw >= 0',
    CASE WHEN SUM(CASE WHEN price_portugal_raw < 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_raw < 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_raw < 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com preço negativo (mercado pode ter preços negativos)')
FROM iceberg.bronze.preco_raw

UNION ALL

-- -----------------------------------------------------------------------------
-- 8. UNICIDADE — sem (datahora, process_date) duplicados em consumo_raw
-- -----------------------------------------------------------------------------
SELECT
    'bronze.consumo_raw » uniqueness » (datahora, process_date)',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' grupos de timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT datahora, process_date
        FROM iceberg.bronze.consumo_raw
        GROUP BY datahora, process_date
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 9. UNICIDADE — sem (date_raw, hour, process_date) duplicados em preco_raw
-- -----------------------------------------------------------------------------
SELECT
    'bronze.preco_raw » uniqueness » (date_raw, hour, process_date)',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' grupos de (data, hora) duplicados')
FROM (
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT date_raw, hour, process_date
        FROM iceberg.bronze.preco_raw
        GROUP BY date_raw, hour, process_date
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 10. COMPLETUDE — dias com menos de 80 registos de consumo (espera-se 96/dia)
--     Threshold de 80 (83% mínimo) para tolerar dados em falta pontuais
-- -----------------------------------------------------------------------------
SELECT
    'bronze.consumo_raw » completeness » registos_por_dia >= 80',
    CASE WHEN SUM(CASE WHEN cnt < 80 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 80 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 80 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 80 registos de consumo (esperados ~96/dia)')
FROM (
    SELECT process_date, COUNT(*) AS cnt
    FROM iceberg.bronze.consumo_raw
    GROUP BY process_date
) AS daily_counts

UNION ALL

-- -----------------------------------------------------------------------------
-- 11. COMPLETUDE — dias com menos de 23 registos de preços (espera-se 24/dia)
--     Hora 25 (DST) aceite como extra; threshold de 23 para folga mínima
-- -----------------------------------------------------------------------------
SELECT
    'bronze.preco_raw » completeness » registos_por_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 registos de preços')
FROM (
    SELECT date_raw, COUNT(*) AS cnt
    FROM iceberg.bronze.preco_raw
    GROUP BY date_raw
) AS daily_counts

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: dias com dados de consumo em falta (análise exploratória)
-- =============================================================================
SELECT
    process_date,
    COUNT(*)                            AS registos_existentes,
    96 - COUNT(*)                       AS registos_em_falta,
    ROUND(100.0 * COUNT(*) / 96.0, 1)  AS pct_completo
FROM iceberg.bronze.consumo_raw
GROUP BY process_date
HAVING COUNT(*) < 96
ORDER BY process_date;
