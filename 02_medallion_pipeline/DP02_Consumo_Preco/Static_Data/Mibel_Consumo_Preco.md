# DP-02 — Consumo vs Preço (consumo_preco)

## Resumo

Pipeline Medallion do Data Product 02 da plataforma MIBEL. Integra dados de consumo elétrico nacional (REN, granularidade 15 min) com preços day-ahead do mercado ibérico MIBEL (OMIE, granularidade horária) para produzir um produto analítico horário pronto para serving e uma feature table para ML.

**Tabela Gold principal**: `iceberg.gold.dp_energy_market_hourly`
**Feature table ML**: `iceberg.gold.feat_load_forecasting_hourly`
**Período coberto**: 2023-01-01 → 2026-03-11

---

## Arquitetura

```
Fontes Externas
├── REN: consumo-total-nacional.csv     (15 min, kW)
└── OMIE: Day-ahead Market Prices*.csv  (horário, €/MWh)
        │
        ▼ Upload para MinIO warehouse/raw/
        │
        ▼ BRONZE — preservação fiel
        ├── bronze.consumo_raw          (15 min, partição process_date)
        └── bronze.preco_raw            (horas 1-25, partição process_date)
        │
        ▼ Quality Gate Bronze
        │
        ▼ SILVER — normalização UTC, agregação horária
        ├── silver.consumo_hourly       (1h, MWh, partição year/month)
        └── silver.preco_hourly         (1h, €/MWh, partição year/month)
        │
        ▼ Quality Gate Silver
        │
        ▼ GOLD — join + features + lag/rolling
        ├── gold.dp_energy_market_hourly         (produto analítico)
        └── gold.feat_load_forecasting_hourly    (feature table ML)
        │
        ▼ Quality Gate Gold
        │
        ├── API HTTP (porta 8000)
        ├── Dashboard Grafana
        └── ML: preco_consumo_mlflow_flow.py
```

---

## Execução

### Pipeline completa (recomendado para primeira execução)

```powershell
cd c:\Users\Avelino\Documents\GitHub\mibel
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py
```

### Pipeline com Docker já a correr

```powershell
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker
```

### Pipeline sem re-aplicar DDL (tabelas já criadas)

```powershell
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker --skip-ddl
```

### Pipeline sem re-upload de CSVs (já estão no MinIO)

```powershell
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker --skip-ddl --skip-upload
```

### Mês específico

```powershell
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker --year 2024 --month 6
```

---

## Stack Tecnológica

| Componente | Tecnologia | Papel |
|---|---|---|
| Storage | MinIO (S3-compatible) | Parquet files + CSVs raw |
| Lakehouse | Apache Iceberg | Tabelas Bronze/Silver/Gold |
| Query Engine | Trino | DDL + transformações SQL |
| Orquestração | Flyte (flytekit local) | DAG de tarefas por camada |
| ML Observability | MLflow | Tracking de modelos |
| Containerização | Docker Compose | Stack completa |

---

## Dados e Transformações

### Bronze

- Ingestão fiel dos CSVs raw para Iceberg via Trino
- Sem transformações de negócio; apenas metadados de ingestão (`process_date`)
- Dois desafios de qualidade reais: duplicados de timestamp (consumo) e hora 25 DST (preços)

### Silver

- **Consumo**: `DATE_TRUNC('hour')` + `SUM(total)/1000` — agrega 15 min → 1h, kW → MWh
- **Preços**: `date + (hour-1) → ts_utc`, filtro `hour ≤ 24` — normaliza OMIE para UTC canónico
- Deduplicação implícita via `GROUP BY`
- Partição por `year/month` para pruning eficiente na Gold

### Gold

- `INNER JOIN` Silver consumo × Silver preço por `ts_utc`
- Features de calendário: `hora`, `dia_semana` (0=Seg), `is_weekend`
- Window functions sobre histórico completo: `LAG(1h)`, `LAG(24h)`, `AVG rolling 24h`
- Feature table ML: adiciona `LEAD(1h)` como target `consumo_next_hour`, filtra nulos

---

## Schema Gold Principal (`dp_energy_market_hourly`)

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Chave de negócio — hora UTC |
| `consumo_total` | `DOUBLE` | Consumo nacional em MWh |
| `market_price_pt` | `DOUBLE` | Preço day-ahead PT em €/MWh |
| `hora` | `INTEGER` | Hora do dia 0-23 |
| `dia_semana` | `INTEGER` | 0=Segunda … 6=Domingo |
| `is_weekend` | `BOOLEAN` | Sáb/Dom |
| `consumo_lag_1h` | `DOUBLE` | Consumo hora anterior |
| `consumo_lag_24h` | `DOUBLE` | Consumo 24h antes |
| `price_lag_1h` | `DOUBLE` | Preço hora anterior |
| `rolling_avg_consumo_24h` | `DOUBLE` | Média móvel consumo 24h |
| `rolling_avg_price_24h` | `DOUBLE` | Média móvel preço 24h |
| `process_date` | `DATE` | Data da execução |
| `year` / `month` | `INTEGER` | Partição Iceberg |

---

## Quality Gates

3 gates executados após cada camada (Bronze → Silver → Gold):

- **Bronze**: 11 checks (nulos, ranges, unicidade, completude)
- **Silver**: 13 checks (+ alinhamento temporal, cobertura join, completude diária)
- **Gold**: 14 checks (+ consistência de lag por self-join, nulos ML, paridade de linhas)

Qualquer `FAIL` bloqueia a promoção com retry automático (Flyte, até 2x). `WARN` não bloqueia.

---

## SLAs/SLOs (Contrato de Dados v1)

| Métrica | Valor |
| --- | --- |
| Atualização | T+45 min após fecho da hora |
| Freshness máxima | 4 horas |
| Taxa join consumo × preço | ≥ 98% das horas |
| Nulos em métricas core | 0% |
| Unicidade `ts_utc` | 100% |
| Versionamento | Schema v1, Product v1 |

---

## ML Pipeline

**Modelo**: Gradient Boosting Regressor (GBR) para previsão de carga horária.
**Target**: `consumo_next_hour` (MWh) — horizonte de 1h.
**Features**: consumo atual, preço atual, lags 1h/24h, rolling 24h, hora, dia_semana, is_weekend.
**Feature table**: `iceberg.gold.feat_load_forecasting_hourly`
**Tracking**: MLflow em `http://localhost:15000`

```powershell
python 03_ml_pipeline/preco_consumo_mlflow_flow.py
```

---

## Estrutura de Ficheiros

```
02_medallion_pipeline/consumo_preco/
├── run_medallion_consumo_precos.py      # Orquestrador principal
├── Mibel_Consumo_Preco.md               # Este ficheiro
├── 01_bronze/
│   ├── FASE_BRONZE.md                   # Documentação da fase Bronze
│   ├── bronze_consumo_precos_trino.sql  # DDL Bronze
│   ├── data/raw/                        # CSVs originais
│   └── scripts/python/                  # Script de limpeza standalone
├── 02_silver/
│   ├── FASE_SILVER.md                   # Documentação da fase Silver
│   └── sql/silver_consumo_precos_trino.sql
├── 03_gold/
│   ├── FASE_GOLD.md                     # Documentação da fase Gold
│   └── sql/gold_consumo_precos_trino.sql
├── 04_quality/
│   ├── QUALITY_GATES.md                 # Documentação dos quality gates
│   └── sql/
│       ├── 01_bronze_checks.sql
│       ├── 02_silver_checks.sql
│       └── 03_gold_checks.sql
├── docs/                                # Especificações e contratos
└── workflows/
    ├── WORKFLOWS.md                     # Documentação dos workflows
    ├── flyte_ingest_bronze.py
    ├── flyte_bronze_to_silver.py
    ├── flyte_silver_to_gold.py
    └── flyte_quality_checks.py
```

---

## Referências

- [Fase Bronze](01_bronze/FASE_BRONZE.md)
- [Fase Silver](02_silver/FASE_SILVER.md)
- [Fase Gold](03_gold/FASE_GOLD.md)
- [Quality Gates](04_quality/QUALITY_GATES.md)
- [Workflows Flyte](workflows/WORKFLOWS.md)
- [Especificação do Data Product](docs/02_data_products_spec.md)
- [Data Contract](docs/07_data_contract.md)
