# Quality Gates — DP-02 Streaming_Data

Os quality gates são executados pelo Flyte (`workflows/flyte_quality_checks.py`) após cada camada. Checks com status **FAIL** bloqueiam a promoção para a camada seguinte. Checks **WARN** são registados mas não bloqueiam.

SQL: `04_quality/sql/` (dentro de `Streaming_Data/`)

---

## 1. Bronze (12 checks)

Ficheiro: `04_quality/sql/01_bronze_checks.sql`

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| B01 | Nulos em `ts_utc` (consumo) | 0 nulos | FAIL |
| B02 | Nulos em `total` (consumo) | 0 nulos | FAIL |
| B03 | Nulos em `ts_utc` (preço) | 0 nulos | FAIL |
| B04 | Nulos em `price_portugal_eur_mwh` (preço) | 0 nulos | FAIL |
| B05 | `total > 0` (consumo positivo em MW) | MW positivo | WARN — valores negativos possíveis na API |
| B06 | Preço PT não-negativo | Preços negativos MIBEL são válidos | WARN |
| B07 | Unicidade `ts_utc` — consumo | Sem duplicados | WARN |
| B08 | Unicidade `ts_utc` — preço | Sem duplicados | FAIL |
| B09 | Freshness consumo | Atraso máx. 3 dias | WARN |
| B10 | Freshness preço | Atraso máx. 2 dias (day-ahead publica D-1) | WARN |
| B11 | Completude diária — consumo | Dias com < 23 horas → WARN | WARN |
| B12 | Completude diária — preço | Dias com < 23 horas → WARN | WARN |

---

## 2. Silver (14 checks)

Ficheiro: `04_quality/sql/02_silver_checks.sql`

### Secção 1 — Taxa de Nulos (campos obrigatórios)

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| S01 | Nulos em `ts_utc` (consumo) | null rate = 0% | FAIL |
| S02 | Nulos em `total_mwh` (consumo) | null rate = 0% — consumo nulo inutiliza o registo para join e forecasting | FAIL |
| S03 | Nulos em `ts_utc` (preço) | null rate = 0% | FAIL |
| S04 | Nulos em `price_portugal_eur_mwh` (preço) | null rate = 0% — preço nulo impede features Gold | FAIL |

### Secção 2 — Range (limites de domínio Portugal)

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| S05 | `total_mwh > 0` (consumo positivo) | Portugal tipicamente 3 000–11 000 MWh/h; WARN porque valores zero esporádicos podem ocorrer | WARN |
| S06 | `price_portugal_eur_mwh >= 0` (preço não-negativo) | Preços negativos são legítimos no MIBEL (excesso renovável) — contabilizados, não bloqueados | WARN |

### Secção 3 — Unicidade

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| S07 | Unicidade `ts_utc` após deduplicação (consumo) | Sem duplicados — a Silver aplica `ROW_NUMBER()` para deduplicar; duplicado aqui indica bug na transformação | FAIL |
| S08 | Unicidade `ts_utc` após deduplicação (preço) | Sem duplicados — preços duplicados corromperiam joins Silver→Gold silenciosamente | FAIL |

### Secção 4 — Integridade Referencial (join consumo ↔ preço)

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| S09 | Horas de consumo sem par de preço (`consumo_sem_preco`) | Desfasamento de poucas horas é normal dado que as APIs têm latências distintas | WARN |
| S10 | Horas de preço sem par de consumo (`preco_sem_consumo`) | Idem — WARN; a Gold resolve via INNER JOIN | WARN |

### Secção 5 — Deduplicação (Silver ≤ Bronze em nº de registos)

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| S11 | `COUNT(silver.consumo) <= COUNT(bronze.consumo)` | Silver nunca gera linhas novas — Silver > Bronze indica bug no INSERT | FAIL |
| S12 | `COUNT(silver.preco) <= COUNT(bronze.preco)` | Idem para tabela de preços | FAIL |

### Secção 6 — Completude Diária

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| S13 | Dias com < 23 horas de consumo | Threshold 23h (não 24h) para acomodar transições DST em Portugal | WARN |
| S14 | Dias com < 23 horas de preços | Idem — dia DST inverno→verão tem legitimamente 23 horas | WARN |

---

## 3. Gold (9 checks)

Ficheiro: `04_quality/sql/03_gold_checks.sql`

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| G01 | Unicidade `ts_utc` (dp) | `COUNT(*) = COUNT(DISTINCT ts_utc)` | FAIL |
| G02 | Nulos em `consumo_total` | null rate = 0% | FAIL |
| G03 | Nulos em `market_price_pt` | null rate = 0% | FAIL |
| G04 | `consumo_total >= 0` | Consumo não pode ser negativo | FAIL |
| G05 | `hora` BETWEEN 0 AND 23 | Slot horário válido | FAIL |
| G06 | `dia_semana` BETWEEN 0 AND 6 | Dia da semana válido | FAIL |
| G07 | Consistência `consumo_lag_1h` | `ABS(lag - consumo(t-1)) <= 0.001 MWh` (máx. 0.1% inconsistências) | FAIL |
| G08 | Taxa de junção consumo × preço | `linhas_gold / MIN(linhas_consumo_silver, linhas_preco_silver) >= 98%` | FAIL |
| G09 | Preços negativos (WARN) | Contabilizados e reportados — não bloqueiam | WARN |

### Checks adicionais — Feature table

| ID | Check | Critério | Nível |
|----|-------|---------|-------|
| F01 | Nulos em `consumo_next_hour` | null rate = 0% | FAIL |
| F02 | Nulos em features lag e rolling | Todos NOT NULL | FAIL |
| F03 | Paridade feat table vs dp | `COUNT(feat) >= COUNT(dp) - 48` | WARN |

---

## 4. Comportamento do quality gate

```
quality_gate(layer="bronze")
  PASS → regista no log, não bloqueia
  WARN → regista com aviso, não bloqueia (dados promovidos com ressalva)
  FAIL → FlyteRecoverableException (retries=2) — bloqueia promoção
```

Se existir pelo menos um check FAIL, a execução aborta antes de promover a camada. Os dados Bronze/Silver ficam preservados para diagnóstico.

---

## 5. Execução standalone

```powershell
# Verificar qualidade de uma camada sem correr o pipeline completo
pyflyte run workflows/flyte_quality_checks.py quality_gate_bronze
pyflyte run workflows/flyte_quality_checks.py quality_gate_silver
pyflyte run workflows/flyte_quality_checks.py quality_gate_gold
```

---

## 6. Verificação manual via Trino

```sql
-- Unicidade e nulos — Gold
SELECT
    COUNT(*)                                              AS total_registos,
    COUNT(DISTINCT ts_utc)                               AS ts_unicos,
    SUM(CASE WHEN consumo_total IS NULL THEN 1 ELSE 0 END) AS nulos_consumo,
    SUM(CASE WHEN market_price_pt IS NULL THEN 1 ELSE 0 END) AS nulos_preco,
    SUM(CASE WHEN market_price_pt < 0 THEN 1 ELSE 0 END)  AS precos_negativos
FROM iceberg.gold.dp_energy_market_api_hourly;

-- Freshness
SELECT
    MAX(ts_utc)                                            AS ultimo_registo,
    CURRENT_TIMESTAMP                                      AS agora,
    date_diff('hour', MAX(ts_utc), CURRENT_TIMESTAMP)     AS horas_atraso
FROM iceberg.gold.dp_energy_market_api_hourly;

-- Completude diária — Bronze
SELECT
    CAST(ts_utc AS DATE) AS dia,
    COUNT(*)             AS n_horas
FROM iceberg.bronze.consumo_api_raw
GROUP BY CAST(ts_utc AS DATE)
HAVING COUNT(*) < 23
ORDER BY dia;
```
