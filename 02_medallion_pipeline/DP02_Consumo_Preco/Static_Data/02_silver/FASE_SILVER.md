# Fase Silver — consumo_preco (DP-02)

## Objetivo

Normalização, agregação e validação dos dados Bronze para um modelo canónico horário em UTC. A camada Silver resolve os desafios de qualidade identificados no Bronze (duplicados, granularidade 15 min → 1h, conversão de unidades, tratamento DST) e produz tabelas prontas para joins e feature engineering na Gold.

---

## Transformações

### `bronze.consumo_raw` → `silver.consumo_hourly`

| Transformação | Detalhe |
|---|---|
| **Agregação temporal** | `DATE_TRUNC('hour', datahora)` — agrega 4 registos de 15 min numa única hora UTC |
| **Conversão de unidades** | `SUM(total) / 1000.0` — kW por intervalo de 15 min → MWh por hora |
| **Chave canónica** | `ts_utc` — timestamp UTC sem fuso (início da hora) |
| **Deduplicação** | O `GROUP BY DATE_TRUNC('hour', datahora)` absorve duplicados residuais do Bronze |
| **Partição** | `year` e `month` derivados de `ts_utc` |

Idempotência: `DELETE WHERE ts_utc >= start AND ts_utc < end` antes de `INSERT` por dia lógico.

### `bronze.preco_raw` → `silver.preco_hourly`

| Transformação | Detalhe |
|---|---|
| **Parsing de timestamp** | `DATE_PARSE(date_raw, '%Y-%m-%d')` + `date_add('hour', hour - 1, ...)` → timestamp UTC |
| **Filtro DST** | `WHERE hour BETWEEN 1 AND 24` — descarta hora 25 (clock-back DST outono) |
| **Deduplicação** | `GROUP BY ts_utc` + `AVG(price_*)` — absorve eventuais duplicados do Bronze |
| **Arredondamento** | `ROUND(AVG(...), 2)` — 2 casas decimais nos preços |
| **Chave canónica** | `ts_utc` alinhado com UTC (sem ajuste de fuso, OMIE publica em hora local CET/CEST) |
| **Partição** | `year` e `month` derivados de `ts_utc` |

> **Nota sobre DST**: O CSV OMIE usa horas de 1 a 24 para dias normais e 1 a 25 para dias com DST de outono (clock-back). A hora 25 local equivale à segunda ocorrência da hora 1 UTC do dia seguinte e é descartada na Silver para manter a unicidade de `ts_utc`. Em dias de DST de primavera (clock-forward), a hora 2 local não existe no CSV — não é necessário tratamento especial.

---

## Tabelas Silver (Iceberg)

### `iceberg.silver.consumo_hourly`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Timestamp UTC canónico (início da hora) — **chave** |
| `total_mwh` | `DOUBLE` | Consumo horário agregado em MWh |
| `year` | `INTEGER` | Ano derivado de `ts_utc` (**partição**) |
| `month` | `INTEGER` | Mês derivado de `ts_utc` (**partição**) |

Localização MinIO: `s3a://warehouse/silver/consumo_hourly/`
Upstream: `iceberg.bronze.consumo_raw`

### `iceberg.silver.preco_hourly`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ts_utc` | `TIMESTAMP(6) WITH TIME ZONE` | Timestamp UTC canónico (início da hora) — **chave** |
| `price_portugal_eur_mwh` | `DOUBLE` | Preço day-ahead Portugal em €/MWh |
| `price_spain_eur_mwh` | `DOUBLE` | Preço day-ahead Espanha em €/MWh |
| `year` | `INTEGER` | Ano derivado de `ts_utc` (**partição**) |
| `month` | `INTEGER` | Mês derivado de `ts_utc` (**partição**) |

Localização MinIO: `s3a://warehouse/silver/preco_hourly/`
Upstream: `iceberg.bronze.preco_raw`

---

## Ficheiros

| Ficheiro | Descrição |
|---|---|
| `sql/silver_consumo_precos_trino.sql` | DDL das duas tabelas Silver (idempotente com `IF NOT EXISTS`) |

---

## Fluxo de Transformação

```
bronze.consumo_raw (15 min, kW)
    │
    ▼ [Flyte: flyte_bronze_to_silver.py]
    │  DATE_TRUNC('hour') + SUM(total)/1000
    ▼
silver.consumo_hourly (1h, MWh) — particionado year/month

bronze.preco_raw (horas 1-25, €/MWh)
    │
    ▼ [Flyte: flyte_bronze_to_silver.py]
    │  date + (hour-1) → ts_utc | filtro hour ≤ 24 | GROUP BY + AVG
    ▼
silver.preco_hourly (1h, €/MWh) — particionado year/month
```

---

## Critérios de Qualidade (Silver)

Verificações executadas após transformação (`04_quality/sql/02_silver_checks.sql`):

| Check | Threshold | Ação |
|---|---|---|
| Nulos em `ts_utc` (consumo) | 0% | FAIL |
| Nulos em `total_mwh` | 0% | FAIL |
| Nulos em `ts_utc` (preço) | 0% | FAIL |
| Nulos em `price_portugal_eur_mwh` | 0% | FAIL |
| Nulos em `price_spain_eur_mwh` | 0% | FAIL |
| `total_mwh > 0` | 100% | WARN |
| `price_portugal_eur_mwh >= 0` | 100% | WARN |
| `ts_utc` único em `consumo_hourly` | 0 duplicados | FAIL |
| `ts_utc` único em `preco_hourly` | 0 duplicados | FAIL |
| `ts_utc` em fronteira de hora (min=0, sec=0) | 100% | FAIL |
| Cobertura join consumo × preço ≥ 95% | ≥ 95% horas | WARN |
| Dias com ≥ 23 horas em `consumo_hourly` | todos | WARN |
| Dias com ≥ 23 horas em `preco_hourly` | todos | WARN |

---

## Decisões de Design

- **UTC canónico**: toda a Silver usa timestamps UTC sem fuso horário local, eliminando ambiguidades DST.
- **Grain horário**: a agregação 15 min → 1h é o menor grão que alinha consumo (15 min) e preços (1h), necessário para o join da Gold.
- **Partição `year/month`**: suporta pruning eficiente nas queries da Gold e nos quality checks mensais.
- **`price_spain_eur_mwh` preservado**: mantido para análise comparativa PT vs ES, mesmo não sendo métricas do produto analítico principal.
- **Preços negativos permitidos**: o mercado MIBEL admite preços negativos (excesso de produção renovável). A Silver regista-os como `WARN` mas não os filtra.
