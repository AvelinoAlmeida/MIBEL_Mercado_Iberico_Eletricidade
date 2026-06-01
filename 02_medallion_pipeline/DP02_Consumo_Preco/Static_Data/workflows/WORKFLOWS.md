# Workflows Flyte — consumo_preco (DP-02)

## Visão Geral

Os workflows deste pipeline são implementados com **Flytekit** e executados localmente via `pyflyte run`. Cada ficheiro corresponde a uma fase da pipeline Medallion ou a uma gate de qualidade transversal.

| Ficheiro | Fase | Workflows disponíveis |
|---|---|---|
| `flyte_ingest_bronze.py` | Bronze | `ingest_bronze_full` |
| `flyte_bronze_to_silver.py` | Silver | `bronze_to_silver`, `bronze_to_silver_full` |
| `flyte_silver_to_gold.py` | Gold | `silver_to_gold_full` |
| `flyte_quality_checks.py` | Quality | `quality_gate_bronze`, `quality_gate_silver`, `quality_gate_gold` |

---

## `flyte_ingest_bronze.py`

**Propósito**: Lê os CSVs raw do MinIO e insere na camada Bronze Iceberg via Trino.

**Pré-requisito**: Os CSVs devem estar em MinIO `warehouse/raw/` (feito pelo `run_medallion_consumo_precos.py`).

### Tasks

| Task | Descrição | Retries |
|---|---|---|
| `ingest_consumo_full()` | Lê `raw/consumo-total-nacional.csv` do MinIO; `DELETE + INSERT` completo em `bronze.consumo_raw`; agrupa por dia para respeitar limites de partições do Iceberg | 3 |
| `ingest_preco_full()` | Lê `raw/Day-ahead Market Prices_*.csv` do MinIO; `DELETE + INSERT` completo em `bronze.preco_raw`; agrupa por dia | 3 |

### Workflows

| Workflow | Descrição |
|---|---|
| `ingest_bronze_full` | Executa `ingest_consumo_full` + `ingest_preco_full` em sequência. Trunca e re-carrega todo o histórico. |

**Idempotência**: `DELETE WHERE 1=1` antes de `INSERT` — re-execuções seguras.

**Otimização de INSERTs**: batches de 5000 linhas com máx. 60 partições por statement, evitando o limite de writers do Trino/Iceberg.

```bash
pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze_full
```

---

## `flyte_bronze_to_silver.py`

**Propósito**: Agrega consumo de 15 min para horário (kW → MWh) e normaliza preços para timestamp UTC canónico.

### Tasks

| Task | Descrição | Retries |
|---|---|---|
| `transform_consumo_silver(process_date)` | Para o dia indicado: `DELETE` das horas Silver + `INSERT` com `DATE_TRUNC('hour')` + `SUM(total)/1000` | 3 |
| `transform_preco_silver(process_date)` | Para o dia indicado: `DELETE` das horas Silver + `INSERT` com conversão `date + (hour-1) → ts_utc`, filtro `hour ≤ 24` | 3 |
| `transform_consumo_silver_full()` | Histórico completo: `DELETE WHERE 1=1` + `INSERT` de todo o Bronze | 3 |
| `transform_preco_silver_full()` | Histórico completo: `DELETE WHERE 1=1` + `INSERT` de todo o Bronze | 3 |

### Workflows

| Workflow | Parâmetro | Descrição |
|---|---|---|
| `bronze_to_silver` | `process_date: date` | Transforma apenas o dia indicado. Idempotente ao nível diário. |
| `bronze_to_silver_full` | — | Transforma todo o histórico Bronze. |

**Idempotência**: por dia (`DELETE WHERE ts_utc >= start AND ts_utc < end`).

**Tratamento DST**: a hora 25 (clock-back) é filtrada (`hour BETWEEN 1 AND 24`) para manter unicidade UTC.

```bash
# Dia específico
pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver --process_date 2023-01-01

# Histórico completo
pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_full
```

---

## `flyte_silver_to_gold.py`

**Propósito**: Constrói os dois data products Gold — produto analítico e feature table ML — com join, features de calendário, lags e rolling averages.

### Tasks

| Task | Descrição | Retries |
|---|---|---|
| `build_dp_energy_market_full()` | `DELETE WHERE 1=1` + `INSERT` com INNER JOIN Silver × Silver, features de calendário (hora, dia_semana, is_weekend) e window functions (LAG 1h, LAG 24h, AVG rolling 24h) | 3 |
| `build_feat_load_forecasting_full(upstream_rows)` | `DELETE WHERE 1=1` + `INSERT` com LEAD(consumo_total, 1) sobre o produto analítico + filtro de nulos; dependência explícita sobre `build_dp_energy_market_full` via `upstream_rows` | 3 |

### Workflows

| Workflow | Descrição |
|---|---|
| `silver_to_gold_full` | Constrói ambos os produtos Gold para todo o histórico Silver. Sempre full — as window functions precisam do histórico completo para calcular lags/rolling nas fronteiras de mês. |

**Window functions**: calculadas sobre todo o histórico (não por mês) para evitar truncamentos nos lags/rolling nas fronteiras de período.

**Sequenciamento**: `build_feat_load_forecasting_full` depende de `build_dp_energy_market_full` via parâmetro `upstream_rows` — garante a ordem correcta no DAG Flyte.

```bash
pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_full
```

---

## `flyte_quality_checks.py`

**Propósito**: Gate de qualidade entre camadas. Executa os SQLs de qualidade e bloqueia a pipeline em caso de `FAIL`.

### Tasks

| Task | Parâmetro | Comportamento |
|---|---|---|
| `quality_gate(layer)` | `layer: str` ('bronze'/'silver'/'gold') | Executa SQL da camada, imprime resultados; em caso de FAIL → `FlyteRecoverableException` |

### Workflows

| Workflow | Descrição |
|---|---|
| `quality_gate_bronze` | Gate para a camada Bronze |
| `quality_gate_silver` | Gate para a camada Silver |
| `quality_gate_gold` | Gate para a camada Gold |

**Lógica de bloqueio**: qualquer check com status `FAIL` lança `FlyteRecoverableException` (com `retries=2`). Checks `WARN` são registados mas não bloqueiam.

```bash
pyflyte run workflows/flyte_quality_checks.py quality_gate_bronze
pyflyte run workflows/flyte_quality_checks.py quality_gate_silver
pyflyte run workflows/flyte_quality_checks.py quality_gate_gold
```

---

## Configuração (variáveis de ambiente)

Todos os workflows lêem configuração de variáveis de ambiente com defaults para execução local:

| Variável | Default | Descrição |
|---|---|---|
| `TRINO_HOST` | `localhost` | Host do Trino |
| `TRINO_PORT` | `8080` | Porta do Trino |
| `MINIO_ENDPOINT` | `http://localhost:9000` | Endpoint MinIO S3 |
| `MINIO_ACCESS_KEY` | `minioadmin` | Access key MinIO |
| `MINIO_SECRET_KEY` | `minioadmin` | Secret key MinIO |
| `RAW_BUCKET` | `warehouse` | Bucket MinIO com os CSVs raw |
| `CONSUMO_KEY` | `raw/consumo-total-nacional.csv` | Caminho do CSV de consumo no MinIO |
| `PRECO_KEY` | `raw/Day-ahead Market Prices_20230101_20260311.csv` | Caminho do CSV de preços no MinIO |

Para execução em ambiente Flyte sandbox (pods isolados), substituir `localhost` por `host.docker.internal`.

---

## Dependências

```
flytekit
trino
boto3
pandas
```

Instaladas automaticamente pelo `run_medallion_consumo_precos.py` no venv `.venv_medallion_consumo_preco/`.

---

## DAG da Pipeline Completa

```
upload_raw_csvs_to_minio()          [run script]
    │
    ▼
ingest_bronze_full                  [flyte_ingest_bronze.py]
    ├─ ingest_consumo_full()
    └─ ingest_preco_full()
    │
    ▼
quality_gate_bronze                 [flyte_quality_checks.py]
    │
    ▼
bronze_to_silver_full               [flyte_bronze_to_silver.py]
    ├─ transform_consumo_silver_full()
    └─ transform_preco_silver_full()
    │
    ▼
quality_gate_silver                 [flyte_quality_checks.py]
    │
    ▼
silver_to_gold_full                 [flyte_silver_to_gold.py]
    ├─ build_dp_energy_market_full()
    └─ build_feat_load_forecasting_full(upstream_rows=...)
    │
    ▼
quality_gate_gold                   [flyte_quality_checks.py]
    │
    ▼
Dados prontos: API + Grafana + MLflow
```
