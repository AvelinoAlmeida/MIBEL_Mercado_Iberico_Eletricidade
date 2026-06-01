# MIBEL — DP-02: Consumo vs Preço

> Data Product 02 da plataforma de análise do mercado energético português (MIBEL), construída sobre uma arquitetura **Medallion** (Bronze → Silver → Gold) com Trino + Apache Iceberg + MinIO.
>
> Projeto académico — MEI / TEAD 2.0 | ESTG-IPP

---

## Pergunta Central

**Como evoluiu o preço day-ahead em Portugal? Quanto custou cada hora de consumo? Como se relacionam o preço e a carga elétrica?**

O DP-02 opera em dois modos independentes:

| Modo | Fonte | Cobertura |
|---|---|---|
| **Static** | OMIE CSV (preços day-ahead) | 2022–2024, histórico completo |
| **Streaming** | ENTSO-E API / Energy-Charts API | Últimos 12 meses, incremental |

---

## Fluxo de Dados

### DP-02 Static — Histórico OMIE (CSV)

```
OMIE CSVs (2022–2024)
  └── precos_consumo_horario.csv  (€/MWh + MWh, hora a hora)
         │
         ▼ Bronze (Hive external)
  hive.bronze.consumo_preco_raw
         │
         ▼ Silver (Iceberg) — dedup, market_hour ∈ [1,25], range €/MWh
  iceberg.silver.consumo_preco_hourly
         │
         ▼ Gold (Iceberg) — join consumo × preço, custo estimado, lags
  iceberg.gold.dp_energy_market_hourly
    ├── preco_eur_mwh (day-ahead PT)
    ├── consumo_mwh
    ├── custo_estimado_eur = preco × consumo
    └── lag_1h, lag_24h, rolling_24h_consumo

  iceberg.gold.feat_load_forecasting_hourly
    └── consumo_next_hour (LEAD 1h) — target ML

  Orquestração: Flyte Sandbox (K3s) — tasks correm em pods isolados
```

**Quality Gate DP-02 Static:** 38 checks (Bronze + Silver + Gold) — validação `market_hour`, lag consistency, null rate do target.

---

### DP-02 Streaming — API em Tempo Real (ENTSO-E / Energy-Charts)

```
ENTSO-E API / Energy-Charts API
  ├── Carga eléctrica PT (MW, horária)
  └── Preços day-ahead PT + ES (€/MWh)
         │
         ▼ Bronze (Iceberg, particionado por process_date)
  iceberg.bronze.consumo_api_raw
  iceberg.bronze.preco_api_raw
         │
         ▼ Silver (Iceberg) — MW→MWh, dedup ts_utc, alinhamento horário
  iceberg.silver.consumo_api_hourly
  iceberg.silver.preco_api_hourly
         │
         ▼ Gold (Iceberg) — join, calendário, lags 1h/24h, rolling 24h
  iceberg.gold.dp_energy_market_api_hourly
  iceberg.gold.feat_load_forecasting_api_hourly

         ▼ Audit (Iceberg)
  iceberg.audit.pipeline_runs    ← run_id, status, duração, rows
  iceberg.audit.dataset_lineage  ← grafo upstream→downstream por run

  Funcionalidades:
  ✓ Retry exponencial 3× (2s → 4s → 8s)
  ✓ Compactação automática Iceberg após cada run
  ✓ SLA: freshness ≤ 7 dias · runtime ≤ 45 min
  ✓ Logging estruturado com run_id
```

**Quality Gate DP-02 Streaming:** 26 checks — freshness ≤ 3 dias (consumo) / ≤ 2 dias (preço), unicidade `ts_utc`, completude ≥ 23 h/dia. Preços negativos → WARN (fenómeno real do MIBEL).

---

## Qualidade de Dados

```
┌─────────────────────────────────────────────────────────────────────────┐
│  QUALITY GATE ARCHITECTURE                                              │
│                                                                         │
│  Cada check retorna: check_name | status | valor_pct | threshold | det  │
│                                                                         │
│  PASS  ──▶ continua pipeline                                            │
│  WARN  ──▶ regista aviso, continua (ex: preços negativos MIBEL)         │
│  FAIL  ──▶ SystemExit — pipeline bloqueada, dados não chegam à Gold     │
└─────────────────────────────────────────────────────────────────────────┘

  DP-02 Static    │  38 checks   (Bronze 11 + Silver 13 + Gold 14)
  DP-02 Streaming │  26 checks   (Bronze + Silver + Gold)
```

| Camada | Checks |
|---|---|
| Bronze | Null rates, completude por período, tipos de dados |
| Silver | Range físico (market_hour ∈ [1,25], €/MWh), unicidade, deduplicação, alinhamento temporal |
| Gold | Lag consistency, null rate targets ML, custo_estimado = preco × consumo |

---

## Machine Learning

Dois modelos independentes com **feature tables pré-computadas na Gold** — sem risco de training-serving skew. Split temporal (sem shuffle) para evitar data leakage.

```
Script                              │ Modelo         │ Target                  │ Feature Table
─────────────────────────────────── │ ────────────── │ ──────────────────────  │ ──────────────────────────────────────
preco_consumo_mlflow_flow.py        │ GB Regressor   │ consumo_next_hour       │ feat_load_forecasting_hourly
preco_consumo_streaming_mlflow.py   │ GB Regressor   │ consumo_next_hour       │ feat_load_forecasting_api_hourly
```

---

## Infraestrutura

### Flyte Sandbox (Orquestração DP-02 Static)

O Flyte corre num container separado com Kubernetes interno (K3s). Os tasks executam em pods isolados e acedem ao stack Compose via `host.docker.internal`.

```text
run_medallion_consumo_precos.py
  ├── auto-inicia Flyte Sandbox se não estiver ativo
  ├── constrói imagem mibel-flyte:latest
  ├── regista workflows remotamente (fast registration via MinIO)
  └── executa tasks em pods K3s:
      ├── bronze_task  →  upload CSV + DDL Hive
      ├── silver_task  →  transformação + quality gate
      └── gold_task    →  features + quality gate
```

### Stack Docker Compose (serviços relevantes para DP-02)

```
01_docker_stack/
  ├── trino/etc/catalog/
  │   ├── iceberg.properties   ──▶ tabelas Silver + Gold (ACID, time-travel)
  │   ├── hive.properties      ──▶ tabelas Bronze Static (Parquet externo)
  │   └── kafka.properties     ──▶ tópicos Redpanda (Streaming)
  ├── redpanda/                Broker Kafka-compatible
  └── flyte/                   Dockerfile + Helm overrides Flyte Sandbox
```

---

## Dashboard Grafana

| Dashboard | Painéis Principais |
|---|---|
| **DP-02 Static** Consumo vs Preço | Preço médio day-ahead · Custo estimado por hora · Correlação |
| **DP-02 Streaming** (quasi-realtime) | Rolling 24h · Freshness indicator · Auditoria de execuções |

Datasource: Trino sobre `iceberg.gold.*` — sem ETL adicional, Gold é a única fonte de verdade.

---

## Estrutura do Repositório

```text
mibel/
├── 02_medallion_pipeline/
│   └── DP02_Consumo_Preco/
│       ├── Static_Data/          Pipeline OMIE CSV (Flyte remoto K3s)
│       ├── Streaming_Data/       Pipeline ENTSO-E API (Flyte local + audit)
│       └── docs/                 Contratos, schemas, decisões de design
│
├── 03_ml_pipeline/
│   ├── preco_consumo_mlflow_flow.py
│   └── preco_consumo_streaming_mlflow_flow.py
│
└── 04_application/
    └── grafana/
        └── dashboards/           dp02_static.json · dp02_streaming.json
```

---

## Arranque Rápido

### Pré-requisitos

- Docker Engine v20.10+ e Docker Compose v2
- ~8 GB RAM disponível para Docker (Trino + Flyte são intensivos)
- ~10 GB disco livre

### 1. Infraestrutura

```powershell
cd 01_docker_stack
docker compose up -d --build

# Verificar (~30 s de startup)
docker compose ps
docker compose exec trino trino --execute "SHOW CATALOGS;"
# Esperado: hive, iceberg, kafka, system, tpcds, tpch
```

### 2. Pipelines DP-02

```powershell
# DP-02 Static: Consumo vs Preço (OMIE CSV)
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker

# DP-02 Streaming: Consumo vs Preço (ENTSO-E API, últimos 12 meses)
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker
```

Os orchestrators são **idempotentes** — seguros para re-executar. Gerem automaticamente o venv local e as dependências Python.

### 3. ML Training

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install pandas scikit-learn trino mlflow boto3

python 03_ml_pipeline/preco_consumo_mlflow_flow.py
python 03_ml_pipeline/preco_consumo_streaming_mlflow_flow.py
```

### 4. Visualização

Dashboards já provisionados automaticamente. Abre o Grafana em [http://localhost:3300](http://localhost:3300) (admin / admin).

---

## Endpoints

| Serviço | URL | Credenciais |
|---|---|---|
| Grafana | [http://localhost:3300](http://localhost:3300) | admin / admin |
| Trino UI | [http://localhost:8080](http://localhost:8080) | — |
| MLflow | [http://localhost:15000](http://localhost:15000) | — |
| MinIO Console | [http://localhost:9001](http://localhost:9001) | minioadmin / minioadmin |
| Flyte UI | [http://localhost:30080](http://localhost:30080) | — |

---

## Flags dos Orchestrators

| Flag | Efeito |
|---|---|
| `--skip-docker` | Não faz `compose up` (stack já a correr) |
| `--skip-ddl` | Não aplica DDL (tabelas já existem) |
| `--no-quality` | Salta quality gates (útil em dev) |
| `--date-from DATE` | Data de início do fetch/reprocessamento |
| `--date-to DATE` | Data de fim |
| `--skip-flyte` | Não verifica/inicia Flyte Sandbox (Static apenas) |
| `--source {energycharts\|entsoe}` | Fonte de dados (Streaming apenas) |
| `--clean` | Apaga e recria tabelas (Streaming apenas) |
| `--full` | Carrega histórico completo desde 2022 (Streaming apenas) |

---

## Documentação Técnica Detalhada

| Documento | Conteúdo |
|---|---|
| [01_docker_stack/README.md](01_docker_stack/README.md) | Setup do stack, catálogos Trino, troubleshooting |
| [02_medallion_pipeline/DP02_Consumo_Preco/docs/](02_medallion_pipeline/DP02_Consumo_Preco/docs/) | Contratos DP-02, schemas, decisões de design, ML |
| [05_relatorio/relatorio.md](05_relatorio/relatorio.md) | Especificação formal: SLAs, SLOs, schema evolution, contratos |
