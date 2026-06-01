# Fase Gold — consumo_preco (DP-02)

## Objetivo

Construção dos dois data products da camada Gold: o produto analítico principal `dp_energy_market_hourly` e a feature table de ML `feat_load_forecasting_hourly`. A Gold materializa o join consumo × preço, adiciona features temporais e de lag/rolling e expõe os dados prontos para serving (API, dashboard) e treino de modelos.

---

## Data Products

### DP-02a — `iceberg.gold.dp_energy_market_hourly`

Produto analítico principal. Combina consumo horário com preço day-ahead e acrescenta features de calendário e de lag para análise e serving.

**Consumidores**: API HTTP (porta 8000), dashboard Grafana, base para feature table ML.

### DP-02b — `iceberg.gold.feat_load_forecasting_hourly`

Feature table para ML. Derivada do produto analítico com adição da variável target `consumo_next_hour` (LEAD +1h). Consumida exclusivamente pelo workflow de treino do modelo de previsão de carga.

**Consumidores**: workflow de treino ML (`03_ml_pipeline/preco_consumo_mlflow_flow.py`), MLflow.

---

## Transformações

### `silver.consumo_hourly` × `silver.preco_hourly` → `dp_energy_market_hourly`

| Passo | Operação |
|---|---|
| **JOIN** | `INNER JOIN` por `ts_utc` — só horas com consumo E preço são promovidas |
| **Features de calendário** | `HOUR(ts_utc)` → `hora`; `DAY_OF_WEEK - 1` → `dia_semana` (0=Seg…6=Dom); `dia_semana >= 5` → `is_weekend` |
| **Lag consumo 1h** | `LAG(consumo_total, 1) OVER (ORDER BY ts_utc)` |
| **Lag consumo 24h** | `LAG(consumo_total, 24) OVER (ORDER BY ts_utc)` |
| **Lag preço 1h** | `LAG(market_price_pt, 1) OVER (ORDER BY ts_utc)` |
| **Rolling consumo 24h** | `AVG(consumo_total) OVER (ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)` |
| **Rolling preço 24h** | `AVG(market_price_pt) OVER (ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)` |

As window functions operam sobre **todo o histórico** da Silver (não por mês) para que os lags/rolling das primeiras horas de cada mês não fiquem truncados.

Idempotência: `DELETE WHERE 1=1` antes de `INSERT` — re-materialização completa.

### `dp_energy_market_hourly` → `feat_load_forecasting_hourly`

| Passo | Operação |
|---|---|
| **Target** | `LEAD(consumo_total, 1) OVER (ORDER BY ts_utc)` → `consumo_next_hour` |
| **Filtro de nulos** | `WHERE consumo_next_hour IS NOT NULL AND consumo_lag_1h IS NOT NULL AND consumo_lag_24h IS NOT NULL AND price_lag_1h IS NOT NULL` |
| **Resultado** | Dataset limpo sem nulos nos campos críticos, pronto para treino supervisionado |

A última linha do produto analítico é descartada (sem `consumo_next_hour` disponível). As primeiras 24 horas perdem os lags e são igualmente filtradas.

---

## Tabelas Gold (Iceberg)

### `iceberg.gold.dp_energy_market_hourly`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Chave de negócio — hora UTC |
| `consumo_total` | `DOUBLE` | Consumo horário nacional em MWh |
| `market_price_pt` | `DOUBLE` | Preço day-ahead Portugal em €/MWh |
| `hora` | `INTEGER` | Hora do dia (0-23) |
| `dia_semana` | `INTEGER` | Dia da semana (0=Segunda … 6=Domingo) |
| `is_weekend` | `BOOLEAN` | `TRUE` se sábado ou domingo |
| `consumo_lag_1h` | `DOUBLE` | Consumo da hora anterior (MWh) |
| `consumo_lag_24h` | `DOUBLE` | Consumo da mesma hora no dia anterior (MWh) |
| `price_lag_1h` | `DOUBLE` | Preço da hora anterior (€/MWh) |
| `rolling_avg_consumo_24h` | `DOUBLE` | Média móvel de consumo nas últimas 24h (MWh) |
| `rolling_avg_price_24h` | `DOUBLE` | Média móvel de preço nas últimas 24h (€/MWh) |
| `process_date` | `DATE` | Data lógica da execução do workflow |
| `year` | `INTEGER` | Ano (**partição**) |
| `month` | `INTEGER` | Mês (**partição**) |

Localização MinIO: `s3a://warehouse/gold/dp_energy_market_hourly/`
Schema version: `v1` | Product version: `v1`

### `iceberg.gold.feat_load_forecasting_hourly`

Todas as colunas do produto analítico mais:

| Coluna adicional | Tipo | Descrição |
|---|---|---|
| `consumo_next_hour` | `DOUBLE` | **TARGET ML**: consumo da hora seguinte (MWh) |

Localização MinIO: `s3a://warehouse/gold/feat_load_forecasting_hourly/`
Feature schema version: `1` — versionada independentemente para rastreabilidade de treino.

---

## Ficheiros

| Ficheiro | Descrição |
|---|---|
| `sql/gold_consumo_precos_trino.sql` | DDL das duas tabelas Gold (idempotente com `IF NOT EXISTS`) |

---

## Fluxo de Transformação

```
silver.consumo_hourly × silver.preco_hourly
    │
    ▼ [Flyte: flyte_silver_to_gold.py → build_dp_energy_market_full()]
    │  INNER JOIN + features calendário + LAG + AVG rolling
    ▼
gold.dp_energy_market_hourly (produto analítico principal)
    │
    ▼ [Flyte: flyte_silver_to_gold.py → build_feat_load_forecasting_full()]
    │  LEAD(consumo_total, 1) + filtro nulos
    ▼
gold.feat_load_forecasting_hourly (feature table ML)
```

A task `build_feat_load_forecasting_full` tem dependência explícita sobre `build_dp_energy_market_full` (via parâmetro `upstream_rows`) para garantir sequenciamento correto no DAG Flyte.

---

## Critérios de Qualidade (Gold)

Verificações executadas após transformação (`04_quality/sql/03_gold_checks.sql`):

| Check | Threshold | Ação |
|---|---|---|
| Nulos em `ts_utc` (dp) | 0% | FAIL |
| Nulos em `consumo_total` | 0% | FAIL |
| Nulos em `market_price_pt` | 0% | FAIL |
| `hora` entre 0 e 23 | 100% | FAIL |
| `dia_semana` entre 0 e 6 | 100% | FAIL |
| `ts_utc` único em `dp_energy_market_hourly` | 0 duplicados | FAIL |
| `ts_utc` único em `feat_load_forecasting_hourly` | 0 duplicados | FAIL |
| Nulos em `consumo_next_hour` | 0% | FAIL |
| Nulos em `consumo_lag_1h` (feat) | 0% | FAIL |
| Nulos em `consumo_lag_24h` (feat) | 0% | FAIL |
| Nulos em `price_lag_1h` (feat) | 0% | FAIL |
| Paridade de linhas dp vs feat (diff ≤ 48) | ≤ 48 linhas | WARN |
| Consistência `consumo_lag_1h` vs hora anterior | < 0.1% erros | FAIL |
| Consistência `price_lag_1h` vs hora anterior | < 0.1% erros | FAIL |

---

## SLAs/SLOs (contrato de dados)

| SLA/SLO | Valor |
|---|---|
| Atualização máxima | T+45 min após fecho da hora |
| Freshness máxima | 4 horas |
| Taxa join consumo × preço | ≥ 98% das horas do período |
| Nulos em métricas core | 0% |
| Unicidade `ts_utc` | 100% |

---

## Decisões de Design

- **INNER JOIN**: só horas com dados de consumo E preço entram na Gold. O threshold de 98% garante que discrepâncias pontuais (publicação tardia OMIE) não bloqueiam o pipeline.
- **Window functions sobre histórico completo**: lags e rolling calculados sobre todo o histórico evitam truncamentos nos limites de mês — crítico para features de ML.
- **Feature table separada**: `feat_load_forecasting_hourly` é derivada e versionada independentemente do produto analítico, permitindo evolução das features sem afetar o serving.
- **Partição `year/month`**: pruning eficiente nas queries mensais e no serving da API.
- **`consumo_next_hour` como target**: target de 1h-ahead simples e interpretável, adequado ao horizonte do despacho energético.
