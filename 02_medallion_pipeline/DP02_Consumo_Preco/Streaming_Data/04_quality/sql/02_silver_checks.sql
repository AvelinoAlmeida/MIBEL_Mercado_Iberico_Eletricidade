-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA SILVER (Streaming_Data / API)
-- Tabelas: iceberg.silver.consumo_api_hourly + iceberg.silver.preco_api_hourly
-- Retorna: check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
--
-- Critério geral de severidade nesta camada:
--   FAIL  → invariante estrutural quebrado; Gold não deve ser construído
--   WARN  → anomalia de domínio recuperável ou desvio temporal esperado na API
-- =============================================================================

-- -------------------------------------------------------------------------
-- SECÇÃO 1 — TAXA DE NULOS (campos obrigatórios)
-- Silver herda os dados depois de deduplicação; nulos nos campos-chave
-- indicam problema no fetch ou na transformação Bronze→Silver.
-- -------------------------------------------------------------------------

-- 1. TAXA DE NULOS — ts_utc em consumo_api_hourly
SELECT
    'silver.consumo_api_hourly » null_rate » ts_utc'        AS check_name,
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                        AS status,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                         AS valor_pct,
    0.0                                                     AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas') AS detalhe
FROM iceberg.silver.consumo_api_hourly

UNION ALL

-- 2. TAXA DE NULOS — total_mwh em consumo_api_hourly
--    FAIL: consumo nulo inutiliza o registo para forecasting e para o join com preços
SELECT
    'silver.consumo_api_hourly » null_rate » total_mwh',
    CASE WHEN SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.consumo_api_hourly

UNION ALL

-- 3. TAXA DE NULOS — ts_utc em preco_api_hourly
SELECT
    'silver.preco_api_hourly » null_rate » ts_utc',
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.preco_api_hourly

UNION ALL

-- 4. TAXA DE NULOS — price_portugal_eur_mwh em preco_api_hourly
--    FAIL: preço nulo impede o cálculo de features de mercado na camada Gold
SELECT
    'silver.preco_api_hourly » null_rate » price_portugal_eur_mwh',
    CASE WHEN SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.preco_api_hourly

UNION ALL

-- -------------------------------------------------------------------------
-- SECÇÃO 2 — RANGE (limites de domínio Portugal)
-- -------------------------------------------------------------------------

-- 5. RANGE — consumo positivo (MWh); Portugal tipicamente entre 3 000–11 000 MWh/h
--    WARN (não FAIL): valores zero esporádicos podem ocorrer em manutenção de medidores;
--    a Gold tolera a presença mas sinaliza nos flags de qualidade
SELECT
    'silver.consumo_api_hourly » range » total_mwh > 0',
    CASE WHEN SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' horas com total_mwh <= 0')
FROM iceberg.silver.consumo_api_hourly

UNION ALL

-- 6. RANGE — preços não-negativos (EUR/MWh)
--    WARN: o MIBEL pode publicar preços negativos em períodos de excesso renovável
--    (fenómeno real; não é erro de dados — é informação válida para análise de mercado)
SELECT
    'silver.preco_api_hourly » range » price_portugal_eur_mwh >= 0',
    CASE WHEN SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_eur_mwh < 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com preço negativo (pode ser normal no MIBEL)')
FROM iceberg.silver.preco_api_hourly

UNION ALL

-- -------------------------------------------------------------------------
-- SECÇÃO 3 — UNICIDADE
-- Silver aplica ROW_NUMBER() para deduplicar; qualquer duplicado aqui indica
-- falha na lógica de transformação Bronze→Silver (FAIL em ambas as tabelas).
-- -------------------------------------------------------------------------

-- 7. UNICIDADE — sem ts_utc duplicados em consumo_api_hourly
SELECT
    'silver.consumo_api_hourly » uniqueness » ts_utc',
    CASE WHEN dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_count AS VARCHAR), ' timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_count
    FROM (
        SELECT ts_utc
        FROM iceberg.silver.consumo_api_hourly
        GROUP BY ts_utc
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- 8. UNICIDADE — sem ts_utc duplicados em preco_api_hourly
--    Duplicados de preço corromperiam silenciosamente os joins Silver→Gold
--    (um consumo ficaria emparelhado com dois preços, distorcendo médias e features lag)
SELECT
    'silver.preco_api_hourly » uniqueness » ts_utc',
    CASE WHEN dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_count AS VARCHAR), ' timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_count
    FROM (
        SELECT ts_utc
        FROM iceberg.silver.preco_api_hourly
        GROUP BY ts_utc
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- -------------------------------------------------------------------------
-- SECÇÃO 4 — INTEGRIDADE REFERENCIAL (join consumo ↔ preço)
-- WARN (não FAIL): a API ENTSO-E e a API OMIE têm latências de publicação
-- distintas — um desfasamento de poucas horas é normal durante o ingest.
-- A Gold agrega por dia, por isso lacunas horárias pontuais não bloqueiam.
-- -------------------------------------------------------------------------

-- 9. INTEGRIDADE REFERENCIAL — horas de consumo sem par de preço
SELECT
    'silver » join_integrity » consumo_sem_preco',
    CASE WHEN orphan_count = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(orphan_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(orphan_count AS VARCHAR), ' horas de consumo sem preço correspondente')
FROM (
    SELECT COUNT(*) AS orphan_count
    FROM iceberg.silver.consumo_api_hourly c
    LEFT JOIN iceberg.silver.preco_api_hourly p ON c.ts_utc = p.ts_utc
    WHERE p.ts_utc IS NULL
) AS orphan_summary

UNION ALL

-- 10. INTEGRIDADE REFERENCIAL — horas de preço sem par de consumo
SELECT
    'silver » join_integrity » preco_sem_consumo',
    CASE WHEN orphan_count = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(orphan_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(orphan_count AS VARCHAR), ' horas de preço sem consumo correspondente')
FROM (
    SELECT COUNT(*) AS orphan_count
    FROM iceberg.silver.preco_api_hourly p
    LEFT JOIN iceberg.silver.consumo_api_hourly c ON p.ts_utc = c.ts_utc
    WHERE c.ts_utc IS NULL
) AS orphan_summary

UNION ALL

-- -------------------------------------------------------------------------
-- SECÇÃO 5 — DEDUPLICAÇÃO (Silver ≤ Bronze em número de registos)
-- A Silver remove duplicados mas nunca gera linhas novas.
-- Silver > Bronze indica um bug no INSERT da transformação.
-- -------------------------------------------------------------------------

-- 11. DEDUPLICAÇÃO — Silver consumo tem <= registos que Bronze consumo
SELECT
    'silver.consumo_api_hourly » dedup » count_vs_bronze',
    CASE WHEN silver_count <= bronze_count THEN 'PASS' ELSE 'FAIL' END,
    CAST(silver_count AS DECIMAL(18,2)),
    CAST(bronze_count AS DECIMAL(18,2)),
    CONCAT('Silver: ', CAST(silver_count AS VARCHAR), ' horas | Bronze: ', CAST(bronze_count AS VARCHAR), ' registos')
FROM (
    SELECT
        (SELECT COUNT(*) FROM iceberg.silver.consumo_api_hourly) AS silver_count,
        (SELECT COUNT(*) FROM iceberg.bronze.consumo_api_raw)    AS bronze_count
) AS counts

UNION ALL

-- 12. DEDUPLICAÇÃO — Silver preço tem <= registos que Bronze preço
SELECT
    'silver.preco_api_hourly » dedup » count_vs_bronze',
    CASE WHEN silver_count <= bronze_count THEN 'PASS' ELSE 'FAIL' END,
    CAST(silver_count AS DECIMAL(18,2)),
    CAST(bronze_count AS DECIMAL(18,2)),
    CONCAT('Silver: ', CAST(silver_count AS VARCHAR), ' horas | Bronze: ', CAST(bronze_count AS VARCHAR), ' registos')
FROM (
    SELECT
        (SELECT COUNT(*) FROM iceberg.silver.preco_api_hourly) AS silver_count,
        (SELECT COUNT(*) FROM iceberg.bronze.preco_api_raw)    AS bronze_count
) AS counts

UNION ALL

-- -------------------------------------------------------------------------
-- SECÇÃO 6 — COMPLETUDE DIÁRIA
-- Threshold: 23 horas (não 24) para acomodar transições DST em Portugal
-- (verão→inverno: dia com 25 h; inverno→verão: dia com 23 h).
-- -------------------------------------------------------------------------

-- 13. COMPLETUDE — dias com menos de 23 horas de consumo em Silver
SELECT
    'silver.consumo_api_hourly » completeness » horas_por_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 horas de consumo')
FROM (
    SELECT CAST(ts_utc AS DATE) AS dia, COUNT(*) AS cnt
    FROM iceberg.silver.consumo_api_hourly
    GROUP BY CAST(ts_utc AS DATE)
) AS daily_counts

UNION ALL

-- 14. COMPLETUDE — dias com menos de 23 horas de preços em Silver
SELECT
    'silver.preco_api_hourly » completeness » horas_por_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 horas de preços')
FROM (
    SELECT CAST(ts_utc AS DATE) AS dia, COUNT(*) AS cnt
    FROM iceberg.silver.preco_api_hourly
    GROUP BY CAST(ts_utc AS DATE)
) AS daily_counts

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: dias com dados em falta na Silver — consumo (exploratório)
-- =============================================================================
SELECT
    CAST(ts_utc AS DATE)                   AS dia,
    COUNT(*)                               AS horas_existentes,
    24 - COUNT(*)                          AS horas_em_falta,
    ROUND(100.0 * COUNT(*) / 24.0, 1)     AS pct_completo
FROM iceberg.silver.consumo_api_hourly
GROUP BY CAST(ts_utc AS DATE)
HAVING COUNT(*) < 24
ORDER BY dia;
