# DP02 — Consumo e Preço: Pipeline Medallion

**Projeto:** MIBEL — Mercado Ibérico de Eletricidade
**Pipeline:** DP02 Consumo e Preço
**Arquitetura:** Medallion (Bronze → Silver → Gold)
**Motor SQL:** Trino / Iceberg

---

## Visão Geral

O DP02 ingere dados de consumo elétrico nacional (Portugal) e preços day-ahead do mercado ibérico (OMIE) para produzir um dataset analítico horário pronto para forecasting com machine learning. Existem dois sub-pipelines paralelos que coexistem:

| Sub-pipeline | Fonte | Granularidade | Período |
|---|---|---|---|
| **Static_Data** | CSVs históricos (REN + OMIE) | 15 min (consumo), 1h (preço) | 2023-01-01 → 2026-03-11 |
| **Streaming_Data** | APIs tempo real (Energy-Charts / ENTSO-E) | 15 min / 1h | Configurável (últimos N meses) |

As tabelas Streaming coexistem com o sufixo `_api` para não colidir com as Static.

---

## Estrutura de Pastas

```
02_medallion_pipeline/
└── DP02_Consumo_Preco/
    ├── Static_Data/
    │   ├── 01_bronze/
    │   │   ├── BRONZE.md
    │   │   ├── bronze_consumo_precos_trino.sql        (DDL)
    │   │   ├── data/raw/                              (CSVs fonte)
    │   │   └── scripts/python/
    │   │       └── bronze_clean_upload_consumo_precos.py
    │   ├── 02_silver/
    │   │   ├── FASE_SILVER.md
    │   │   └── sql/silver_consumo_precos_trino.sql    (DDL)
    │   ├── 03_gold/
    │   │   ├── FASE_GOLD.md
    │   │   └── sql/gold_consumo_precos_trino.sql      (DDL)
    │   ├── 04_quality/
    │   │   ├── QUALITY_GATES.md
    │   │   └── sql/
    │   │       ├── 01_bronze_checks.sql               (11 verificações)
    │   │       ├── 02_silver_checks.sql               (13 verificações)
    │   │       └── 03_gold_checks.sql                 (12 verificações)
    │   ├── docs/                                      (contratos e especificações)
    │   ├── workflows/                                 (DAGs Flyte)
    │   ├── Mibel_Consumo_Preco.md
    │   └── run_medallion_consumo_precos.py            (orquestrador)
    │
    └── Streaming_Data/
        ├── 01_bronze/
        │   ├── bronze_streaming_ddl.sql
        │   └── scripts/python/                        (fetch APIs)
        ├── 02_silver/
        │   └── sql/silver_streaming_ddl.sql
        ├── 03_gold/
        │   └── sql/gold_streaming_ddl.sql
        ├── 04_quality/
        │   └── sql/
        ├── 05_audit/
        │   └── sql/audit_ddl.sql
        ├── workflows/
        ├── DP02_Pipeline.md
        └── run_streaming_pipeline.py                  (orquestrador)
```

---

## Camada Bronze — Ingestão Fiel

### Filosofia

A camada Bronze preserva os dados **exatamente como chegam da fonte**, sem limpeza nem transformação. O objetivo é garantir rastreabilidade total e a possibilidade de re-processar as camadas superiores a qualquer momento. Os problemas de qualidade são registados mas não corrigidos aqui.

---

### Static_Data — Tabelas Bronze

#### `iceberg.bronze.consumo_raw`
**Fonte:** REN (Redes Energéticas Nacionais) — ficheiro CSV `consumo-total-nacional.csv`
**Granularidade:** 15 minutos
**Partição:** `process_date` (para idempotência de re-execuções)

| Coluna | Tipo | Descrição |
|---|---|---|
| `datahora` | TIMESTAMP(6) WITH TIME ZONE | Timestamp original da REN, em UTC |
| `dia` | INTEGER | Dia do mês (campo redundante da fonte, preservado) |
| `mes` | INTEGER | Mês (campo redundante da fonte, preservado) |
| `ano` | INTEGER | Ano (campo redundante da fonte, preservado) |
| `date_raw` | VARCHAR | String de data original tal como vem no CSV |
| `time_raw` | VARCHAR | String de hora original tal como vem no CSV |
| `bt` | DOUBLE | Consumo em Baixa Tensão (kW) |
| `mt` | DOUBLE | Consumo em Média Tensão (kW) |
| `at` | DOUBLE | Consumo em Alta Tensão (kW) |
| `mat` | DOUBLE | Consumo em Muito Alta Tensão (kW) |
| `total` | DOUBLE | Consumo total nacional agregado (kW) |
| `process_date` | DATE | Data de execução do pipeline (partição) |

**Problemas de qualidade conhecidos e preservados:**
- Gap de 376 dias: o CSV da REN termina em 2025-02-28 enquanto os preços chegam até 2026-03-11
- Início irregular: 2023-01-01 começa às 03:00h (os primeiros registos do dia estão ausentes)
- 24 timestamps duplicados causados por DST (mudança de verão para inverno — hora repetida)
- Dias incompletos: 2025-10-13, 2025-10-14, 2026-03-04

---

#### `iceberg.bronze.preco_raw`
**Fonte:** OMIE (Operador del Mercado Ibérico de Energía) — ficheiros CSV `Day-ahead Market Prices_*.csv`
**Granularidade:** Horária (hora 1–24, com hora 25 em transição DST outono)
**Partição:** `process_date`

| Coluna | Tipo | Descrição |
|---|---|---|
| `date_raw` | VARCHAR | Data original do OMIE (string, formato YYYY-MM-DD) |
| `hour` | INTEGER | Hora do dia, escala 1–24 (25 em DST outono) |
| `price_portugal_raw` | DOUBLE | Preço day-ahead Portugal em €/MWh |
| `price_spain_raw` | DOUBLE | Preço day-ahead Espanha em €/MWh |
| `process_date` | DATE | Data de execução do pipeline (partição) |

**Problemas de qualidade conhecidos e preservados:**
- 879 horas com preço zero (~3.1%): corresponde a horas em que o mercado não liquidou (especialmente 2023-01-01)
- 504 horas com preço negativo: fenómeno real de mercado (oversupply de solar/eólica)
- Hora 25 em dias de transição DST outono: preservada no Bronze, descartada no Silver
- Hora 2 ausente em dias de transição DST primavera: gap real, não erro

---

### Streaming_Data — Tabelas Bronze

#### `iceberg.bronze.consumo_api_raw`
**Fonte:** Energy-Charts API (`api.energy-charts.info/public_power?country=pt`) ou ENTSO-E Transparency Platform
**Granularidade:** 15 minutos

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | TIMESTAMP(6) WITH TIME ZONE | Timestamp UTC já normalizado na ingestão (diferente do Static) |
| `total` | DOUBLE | Carga total em MW |
| `source_url` | VARCHAR | URL completo da chamada API (rastreabilidade) |
| `fetch_date` | DATE | Data em que a chamada foi feita |
| `process_date` | DATE | Data de execução do pipeline (partição) |

#### `iceberg.bronze.preco_api_raw`
**Fonte:** Energy-Charts API (`/price?bzn=PT` e `/price?bzn=ES`) ou ENTSO-E Transparency Platform

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | TIMESTAMP(6) WITH TIME ZONE | Timestamp UTC (já em UTC, sem escala hora 1–25) |
| `price_portugal_eur_mwh` | DOUBLE | Preço day-ahead Portugal em €/MWh |
| `price_spain_eur_mwh` | DOUBLE | Preço day-ahead Espanha em €/MWh (NULL se API não retornar) |
| `source_url` | VARCHAR | URL da chamada API |
| `fetch_date` | DATE | Data da chamada |
| `process_date` | DATE | Partição |

---

### Quality Gate Bronze — 11 Verificações

| # | Tabela | Tipo de Check | Coluna(s) | Severidade |
|---|---|---|---|---|
| 1 | `consumo_raw` | Null rate | `datahora` | FAIL |
| 2 | `consumo_raw` | Null rate | `total` | FAIL |
| 3 | `preco_raw` | Null rate | `price_portugal_raw` | FAIL |
| 4 | `preco_raw` | Null rate | `price_spain_raw` | FAIL |
| 5 | `consumo_raw` | Range | `total > 0` | WARN |
| 6 | `preco_raw` | Range | `hour BETWEEN 1 AND 25` | FAIL |
| 7 | `preco_raw` | Range | `price_portugal_raw >= 0` | WARN |
| 8 | `consumo_raw` | Uniqueness | `(datahora, process_date)` | WARN |
| 9 | `preco_raw` | Uniqueness | `(date_raw, hour, process_date)` | FAIL |
| 10 | `consumo_raw` | Completeness | registos por dia ≥ 80 | WARN |
| 11 | `preco_raw` | Completeness | registos por dia ≥ 23 | WARN |

---

## Camada Silver — Normalização e Limpeza

### Filosofia

A camada Silver **normaliza, padroniza e resolve ambiguidades** dos dados Bronze. O objetivo é produzir dados limpos, sem duplicados, com timestamps canónicos em UTC e unidades consistentes. Nenhuma feature analítica é calculada aqui — isso é trabalho da camada Gold.

### O que muda do Bronze para o Silver

| Problema Bronze | Solução Silver |
|---|---|
| Consumo em kW, granularidade 15 min | Agregado para MWh/hora com `SUM(total)/1000 * (15/60)` |
| Hora OMIE na escala 1–24 (ou 25) | Convertida para timestamp UTC real |
| Hora 25 (DST outono) no OMIE | **Descartada** com `WHERE hour BETWEEN 1 AND 24` |
| Timestamps duplicados (DST) no consumo | **Deduplucados** via `GROUP BY DATE_TRUNC('hour', datahora)` |
| Strings de data (`date_raw`) no OMIE | Convertidas para TIMESTAMP com `DATE_PARSE` + `DATE_ADD` |
| Unidades mistas (kW, MW) | Tudo normalizado para MWh |
| Partição por `process_date` | Reparticionado por `year/month` (pruning eficiente) |

**Partição:** `year` e `month` (em vez de `process_date`) para acelerar queries temporais.

---

### Static_Data — Tabelas Silver

#### `iceberg.silver.consumo_hourly`
**Origem:** `bronze.consumo_raw` — agregação de 4 registos de 15 min para 1 hora

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | TIMESTAMP(6) WITH TIME ZONE | Timestamp UTC canónico, início da hora (ex: 2024-03-15 14:00:00 UTC) |
| `total_mwh` | DOUBLE | Consumo horário em MWh (SUM dos 4 intervalos de 15 min / 1000) |
| `year` | INTEGER | Partição — ano |
| `month` | INTEGER | Partição — mês |

**Transformação principal:**
```sql
SELECT
    DATE_TRUNC('hour', datahora) AS ts_utc,
    SUM(total) / 1000.0          AS total_mwh,   -- kW × 4 períodos / 1000 = MWh
    YEAR(DATE_TRUNC('hour', datahora)) AS year,
    MONTH(DATE_TRUNC('hour', datahora)) AS month
FROM bronze.consumo_raw
WHERE datahora IS NOT NULL AND total IS NOT NULL
GROUP BY DATE_TRUNC('hour', datahora)
```

O `GROUP BY DATE_TRUNC('hour')` resolve automaticamente os 24 duplicados de DST (agrega todos os registos da mesma hora UTC).

---

#### `iceberg.silver.preco_hourly`
**Origem:** `bronze.preco_raw` — conversão de hora inteira OMIE para timestamp UTC

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | TIMESTAMP(6) WITH TIME ZONE | Timestamp UTC (hora 1 do OMIE = 00:00 UTC do mesmo dia) |
| `price_portugal_eur_mwh` | DOUBLE | Preço day-ahead Portugal em €/MWh (ROUND 2 casas decimais) |
| `price_spain_eur_mwh` | DOUBLE | Preço day-ahead Espanha em €/MWh (ROUND 2 casas decimais) |
| `year` | INTEGER | Partição — ano |
| `month` | INTEGER | Partição — mês |

**Transformação principal:**
```sql
SELECT
    DATE_PARSE(date_raw, '%Y-%m-%d')
        + INTERVAL '1' HOUR * (hour - 1)               AS ts_utc,
    ROUND(AVG(price_portugal_raw), 2)                  AS price_portugal_eur_mwh,
    ROUND(AVG(price_spain_raw), 2)                     AS price_spain_eur_mwh,
    ...
FROM bronze.preco_raw
WHERE hour BETWEEN 1 AND 24                            -- descarta hora 25 (DST outono)
GROUP BY DATE_PARSE(date_raw, '%Y-%m-%d') + INTERVAL '1' HOUR * (hour - 1)
```

A hora 25 (DST outono) é descartada porque, no sistema UTC, a hora repetida é tratada automaticamente pelo timestamp UTC — guardá-la causaria duplicados.

---

### Streaming_Data — Tabelas Silver

Schema **idêntico** ao Static, apenas a origem é diferente (API em vez de CSV):
- `iceberg.silver.consumo_api_hourly` — mesmas colunas que `consumo_hourly`
- `iceberg.silver.preco_api_hourly` — mesmas colunas que `preco_hourly`

A diferença de processamento é mínima: o Bronze Streaming já entrega `ts_utc` normalizado (a API devolve UTC diretamente), pelo que a Silver faz apenas agregação e arredondamento.

---

### Quality Gate Silver — 13 Verificações

| # | Tabela | Tipo de Check | Coluna(s) | Severidade |
|---|---|---|---|---|
| 1 | `consumo_hourly` | Null rate | `ts_utc` | FAIL |
| 2 | `consumo_hourly` | Null rate | `total_mwh` | FAIL |
| 3 | `preco_hourly` | Null rate | `ts_utc` | FAIL |
| 4 | `preco_hourly` | Null rate | `price_portugal_eur_mwh` | FAIL |
| 5 | `preco_hourly` | Null rate | `price_spain_eur_mwh` | FAIL |
| 6 | `consumo_hourly` | Range | `total_mwh > 0` | WARN |
| 7 | `preco_hourly` | Range | `price_portugal_eur_mwh >= 0` | WARN |
| 8 | `consumo_hourly` | Uniqueness | `ts_utc` | FAIL |
| 9 | `preco_hourly` | Uniqueness | `ts_utc` | FAIL |
| 10 | `consumo_hourly` | Alinhamento temporal | `ts_utc` em fronteira de hora (min=0, sec=0) | FAIL |
| 11 | `consumo_hourly` | Cobertura de join | LEFT JOIN com `preco_hourly` ≥ 95% horas com preço | WARN |
| 12 | `consumo_hourly` | Completeness | ≥ 23 horas por dia | WARN |
| 13 | `preco_hourly` | Completeness | ≥ 23 horas por dia | WARN |

---

## Camada Gold — Enriquecimento Analítico

### Filosofia

A camada Gold **cruza as duas fontes Silver** (consumo × preço) e adiciona **features analíticas e de machine learning**. É o produto de dados final, diretamente consumível por modelos de forecasting e dashboards de mercado.

### O que muda do Silver para o Gold

| Silver | Gold |
|---|---|
| Duas tabelas separadas (consumo e preço) | JOIN único por `ts_utc` (só horas com ambos os dados) |
| Sem contexto temporal | Features de calendário: `hora`, `dia_semana`, `is_weekend` |
| Sem histórico relativo | Lags: consumo 1h atrás, consumo 24h atrás, preço 1h atrás |
| Sem tendência | Rolling average 24h para consumo e preço |
| Sem variável target | `consumo_next_hour` como target de forecasting (LEAD) |

---

### Static_Data — Tabelas Gold

#### `iceberg.gold.dp_energy_market_hourly`
**Produto analítico principal.** Dataset horário com consumo, preço e features de contexto.
**Origem:** INNER JOIN entre `silver.consumo_hourly` e `silver.preco_hourly` em `ts_utc`
**Partição:** `year/month`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | TIMESTAMP(6) WITH TIME ZONE | Chave natural — hora UTC |
| `consumo_total` | DOUBLE | Consumo horário total em MWh (de Silver) |
| `market_price_pt` | DOUBLE | Preço day-ahead Portugal €/MWh (de Silver) |
| `hora` | INTEGER | Hora do dia (0–23), calculada com `HOUR(ts_utc)` |
| `dia_semana` | INTEGER | Dia da semana (0=Segunda ... 6=Domingo), via `DAY_OF_WEEK(ts_utc) - 1` |
| `is_weekend` | BOOLEAN | `TRUE` se `dia_semana >= 5` (Sábado ou Domingo) |
| `consumo_lag_1h` | DOUBLE | Consumo da hora anterior — `LAG(consumo_total, 1) OVER (ORDER BY ts_utc)` |
| `consumo_lag_24h` | DOUBLE | Consumo da mesma hora do dia anterior — `LAG(consumo_total, 24) OVER (ORDER BY ts_utc)` |
| `price_lag_1h` | DOUBLE | Preço da hora anterior — `LAG(market_price_pt, 1) OVER (ORDER BY ts_utc)` |
| `rolling_avg_consumo_24h` | DOUBLE | Média móvel 24h do consumo — `AVG(consumo_total) OVER (ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)` |
| `rolling_avg_price_24h` | DOUBLE | Média móvel 24h do preço — `AVG(market_price_pt) OVER (ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)` |
| `process_date` | DATE | Data de execução do pipeline |
| `year` | INTEGER | Partição — ano |
| `month` | INTEGER | Partição — mês |

**Transformação principal:**
```sql
WITH joined AS (
    SELECT
        c.ts_utc,
        c.total_mwh                              AS consumo_total,
        p.price_portugal_eur_mwh                 AS market_price_pt
    FROM silver.consumo_hourly c
    INNER JOIN silver.preco_hourly p ON c.ts_utc = p.ts_utc   -- só horas com ambos
),
features AS (
    SELECT
        ts_utc,
        consumo_total,
        market_price_pt,
        HOUR(ts_utc)                             AS hora,
        DAY_OF_WEEK(ts_utc) - 1                  AS dia_semana,
        (DAY_OF_WEEK(ts_utc) - 1) >= 5           AS is_weekend,
        LAG(consumo_total, 1)  OVER (ORDER BY ts_utc) AS consumo_lag_1h,
        LAG(consumo_total, 24) OVER (ORDER BY ts_utc) AS consumo_lag_24h,
        LAG(market_price_pt, 1) OVER (ORDER BY ts_utc) AS price_lag_1h,
        AVG(consumo_total) OVER (
            ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        )                                        AS rolling_avg_consumo_24h,
        AVG(market_price_pt) OVER (
            ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        )                                        AS rolling_avg_price_24h
    FROM joined
)
SELECT *, CURRENT_DATE AS process_date, YEAR(ts_utc) AS year, MONTH(ts_utc) AS month
FROM features
```

**Nota crítica sobre Window Functions:** As funções `LAG`/`AVG OVER` não têm `PARTITION BY`, o que é intencional — garantir que os lags em fronteiras de mês/ano sejam calculados corretamente sobre o histórico completo.

**Impacto do INNER JOIN:** Apenas horas com dados de consumo E preço entram na Gold (~98% de cobertura). As primeiras horas de 2023-01-01 (onde consumo começa às 03:00) ficam excluídas.

---

#### `iceberg.gold.feat_load_forecasting_hourly`
**Feature table para machine learning.** Extensão direta de `dp_energy_market_hourly` com o target de forecasting.
**Origem:** `dp_energy_market_hourly` com LEAD adicional

| Coluna Adicional | Tipo | Descrição |
|---|---|---|
| *(todas as colunas de `dp_energy_market_hourly`)* | — | Herdadas diretamente |
| `consumo_next_hour` | DOUBLE | **TARGET ML** — consumo da hora seguinte, via `LEAD(consumo_total, 1) OVER (ORDER BY ts_utc)` |

**Transformação adicional:**
```sql
SELECT
    *,
    LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour
FROM dp_energy_market_hourly
WHERE
    consumo_next_hour IS NOT NULL    -- exclui a última hora (sem "próxima")
    AND consumo_lag_1h  IS NOT NULL  -- exclui a 1ª hora (sem lag 1h)
    AND consumo_lag_24h IS NOT NULL  -- exclui as primeiras 24 horas (sem lag 24h)
    AND price_lag_1h    IS NOT NULL  -- exclui a 1ª hora de preço
```

**Efeito dos filtros:** A feature table tem ~25 linhas a menos que `dp_energy_market_hourly` (primeiras 24h + última hora). Isto é verificado pelo check de row parity (diferença ≤ 48).

**Uso esperado:** Input direto para modelos Gradient Boosted Regressor (GBR) de load forecasting. O target `consumo_next_hour` é o que o modelo tenta prever dado o contexto da hora atual.

---

### Streaming_Data — Tabelas Gold Adicionais

As tabelas `dp_energy_market_api_hourly` e `feat_load_forecasting_api_hourly` têm o schema idêntico às Static, mas com dois extras exclusivos do Streaming:

#### `iceberg.gold.ml_metrics_streaming`
Registo das métricas do último run do modelo GBR treinado sobre dados de streaming.

| Coluna | Tipo | Descrição |
|---|---|---|
| `run_id` | VARCHAR | Identificador único do run |
| `n_train` | INTEGER | Número de registos usados para treino |
| `n_test` | INTEGER | Número de registos usados para teste |
| `mae` | DOUBLE | Mean Absolute Error |
| `rmse` | DOUBLE | Root Mean Square Error |
| `r2` | DOUBLE | Coeficiente de determinação R² |
| `mape` | DOUBLE | Mean Absolute Percentage Error |
| `hyperparameters` | VARCHAR | Hiperparâmetros do modelo (JSON) |

#### `iceberg.gold.ml_feature_importance_streaming`
Importância de cada feature no modelo GBR.

| Coluna | Tipo | Descrição |
|---|---|---|
| `run_id` | VARCHAR | Ligação ao run de `ml_metrics_streaming` |
| `feature_name` | VARCHAR | Nome da feature (ex: `consumo_lag_24h`) |
| `importance` | DOUBLE | Importância relativa (0–1, soma = 1) |
| `rank` | INTEGER | Ranking da feature (1 = mais importante) |

---

### Quality Gate Gold — 12 Verificações

| # | Tabela | Tipo de Check | Coluna(s) | Severidade |
|---|---|---|---|---|
| 1 | `dp_energy_market_hourly` | Null rate | `ts_utc` | FAIL |
| 2 | `dp_energy_market_hourly` | Null rate | `consumo_total` | FAIL |
| 3 | `dp_energy_market_hourly` | Null rate | `market_price_pt` | FAIL |
| 4 | `dp_energy_market_hourly` | Range | `hora BETWEEN 0 AND 23` | FAIL |
| 5 | `dp_energy_market_hourly` | Range | `dia_semana BETWEEN 0 AND 6` | FAIL |
| 6 | `dp_energy_market_hourly` | Uniqueness | `ts_utc` | FAIL |
| 7 | `feat_load_forecasting_hourly` | Uniqueness | `ts_utc` | FAIL |
| 8 | `feat_load_forecasting_hourly` | Null rate ML | `consumo_next_hour` | FAIL |
| 9 | `feat_load_forecasting_hourly` | Null rate ML | `consumo_lag_1h` | FAIL |
| 10 | `feat_load_forecasting_hourly` | Null rate ML | `consumo_lag_24h` | FAIL |
| 11 | `feat_load_forecasting_hourly` | Null rate ML | `price_lag_1h` | FAIL |
| 12 | Ambas | Row parity | `ABS(count(dp) - count(feat)) <= 48` | WARN |
| 13 | `dp_energy_market_hourly` | Lag consistency | `ABS(consumo_lag_1h - prev.consumo_total) <= 0.01` (< 0.1% erros) | FAIL |
| 14 | `dp_energy_market_hourly` | Lag consistency | `ABS(price_lag_1h - prev.market_price_pt) <= 0.01` (< 0.1% erros) | FAIL |

---

## Auditoria (Streaming_Data)

### `iceberg.audit.pipeline_runs`
Registo de cada execução do pipeline.

| Coluna | Tipo | Descrição |
|---|---|---|
| `run_id` | VARCHAR | ID único da execução |
| `status` | VARCHAR | `SUCCESS`, `FAILED`, `PARTIAL` |
| `started_at` | TIMESTAMP | Início da execução |
| `duration_seconds` | INTEGER | Duração total |
| `rows_bronze` | INTEGER | Linhas ingeridas no Bronze |
| `rows_silver` | INTEGER | Linhas após Silver |
| `rows_gold` | INTEGER | Linhas após Gold |

### `iceberg.audit.dataset_lineage`
Mapa de dependências por run.

| Coluna | Tipo | Descrição |
|---|---|---|
| `run_id` | VARCHAR | Ligação a `pipeline_runs` |
| `upstream_table` | VARCHAR | Tabela de origem |
| `downstream_table` | VARCHAR | Tabela de destino |

---

## SLA e Contrato de Dados

| Métrica | Valor | Ação se violado |
|---|---|---|
| Frescura máxima | T+45 min após fecho da hora | Alerta crítico de despacho |
| Alerta de frescura | > 4 horas sem atualização | WARN |
| Cobertura join consumo × preço | ≥ 98% | WARN se < 95% |
| Nulos em métricas core (Gold) | 0% | FAIL (bloqueia pipeline) |
| Nulos nas features ML | 0% | FAIL (bloqueia modelo) |
| Unicidade de `ts_utc` | 100% | FAIL (bloqueia pipeline) |

---

## Fluxo de Dados Resumido

```
Fontes Externas
├── REN CSV (consumo 15 min, kW)          ─────────────┐
└── OMIE CSV (preço horário, €/MWh)       ─────────────┤
                                                        ▼
                                              BRONZE (ingestão fiel)
                                          ┌─ consumo_raw (15 min, kW)
                                          └─ preco_raw (hora 1-25, €/MWh)
                                                        │
                                          11 quality checks
                                                        │
                                                        ▼
                                              SILVER (normalização)
                                          ┌─ consumo_hourly (1h, MWh, UTC)
                                          └─ preco_hourly (1h, €/MWh, UTC)
                                                        │
                                          13 quality checks
                                                        │
                                                        ▼
                                              GOLD (enriquecimento)
                                          ┌─ dp_energy_market_hourly
                                          │   (JOIN + calendário + lags + rolling)
                                          └─ feat_load_forecasting_hourly
                                              (+ consumo_next_hour como target ML)
                                                        │
                                          12 quality checks
                                                        │
                                                        ▼
                                           Consumo por Dashboards e Modelos GBR

APIs Tempo Real (Energy-Charts / ENTSO-E)
├── consumo_api_raw  ──► consumo_api_hourly  ──► dp_energy_market_api_hourly
└── preco_api_raw    ──►  preco_api_hourly   ──► feat_load_forecasting_api_hourly
                                                 + ml_metrics_streaming
                                                 + ml_feature_importance_streaming
```

---

## Fontes de Dados

| Fonte | Tipo | URL / Endpoint | Autenticação |
|---|---|---|---|
| REN | CSV histórico | Portal REN — `consumo-total-nacional.csv` | Nenhuma |
| OMIE | CSV histórico | Portal OMIE — `Day-ahead Market Prices_*.csv` | Nenhuma |
| Energy-Charts | API REST | `api.energy-charts.info` | Nenhuma |
| ENTSO-E | API REST | `transparency.entsoe.eu` | Token (`ENTSOE_TOKEN`) |

---

## Scripts T-SQL de Exploração

Motor: **Trino** | Catálogo: **iceberg** | Ficheiro: `MIBEL TSQL.sql`

---

### 0. Conectividade — Verificar schemas e tabelas

```sql
SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;
```

---

### 1. Bronze — O que chegou da API hoje?

```sql
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
```

---

### 2. Silver — A série está completa e correcta?

```sql
-- Cobertura temporal da série de consumo
SELECT
    MIN(ts_utc)  AS inicio,
    MAX(ts_utc)  AS fim,
    COUNT(*)     AS total_horas
FROM iceberg.silver.consumo_api_hourly;

-- Últimas 48h de consumo normalizado (MWh)
SELECT ts_utc, total_mwh
FROM iceberg.silver.consumo_api_hourly
ORDER BY ts_utc DESC
LIMIT 48;

-- Preço médio mensal PT vs ES (evolução do mercado)
SELECT year, month,
       ROUND(AVG(price_portugal_eur_mwh), 2) AS avg_preco_pt,
       ROUND(AVG(price_spain_eur_mwh),    2) AS avg_preco_es,
       COUNT(*)                               AS n_horas
FROM iceberg.silver.preco_api_hourly
GROUP BY year, month
ORDER BY year DESC, month DESC;
```

---

### 3. Gold — Perguntas de negócio

```sql
-- Q1: Consumo e preço na última hora disponível
SELECT ts_utc, consumo_total, market_price_pt, hora, is_weekend
FROM iceberg.gold.dp_energy_market_api_hourly
ORDER BY ts_utc DESC
LIMIT 1;

-- Q2: Perfil diário médio (consumo e preço por hora do dia)
SELECT hora,
       ROUND(AVG(consumo_total),    1) AS avg_consumo_mwh,
       ROUND(AVG(market_price_pt),  2) AS avg_preco_eur_mwh,
       COUNT(*)                        AS n_observacoes
FROM iceberg.gold.dp_energy_market_api_hourly
GROUP BY hora
ORDER BY hora;

-- Q3: Semana vs fim-de-semana — diferença de consumo e preço
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

-- Q5: Top 10 horas mais caras (correlação consumo vs preço)
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
```

---

### 4. ML — Métricas do modelo de load forecasting

```sql
-- Q7: Performance do modelo mais recente
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

-- Q8: Features mais importantes para a previsão
SELECT rank, feature_name,
       ROUND(importance, 4) AS importance
FROM iceberg.gold.ml_feature_importance_streaming
ORDER BY rank;
```

---

### 5. Freshness — Os dados estão actualizados?

```sql
-- Tempo decorrido desde o último registo em cada camada
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
```
