# Schemas das Tabelas — DP-02 Streaming_Data

Todas as tabelas usam sufixo `_api` para coexistir com o pipeline Static_Data (CSV).

---

## 1. Bronze

### `iceberg.bronze.consumo_api_raw`

**Origem (default):** Energy-Charts API — `GET /total_power?country=pt`  
**Origem (alternativa):** ENTSO-E — `query_load('PT')` (requer `ENTSOE_TOKEN`)  
**Granularidade:** horária (MW)  
**Partição:** `process_date`  
**Localização:** `s3a://warehouse/bronze/consumo_api_raw/`

| Coluna | Tipo | Obrigatória | Descrição |
|--------|------|-------------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Sim | Timestamp UTC do início da hora |
| `total` | DOUBLE | Sim | Carga total nacional em **MW** (valor bruto da API) |
| `source_url` | VARCHAR | Não | URL da chamada à API (rastreabilidade) |
| `fetch_date` | DATE | Não | Data em que a chamada foi feita |
| `process_date` | DATE | Sim | Data lógica de ingestão — chave de partição |

**Regras:**
- Preserva o valor bruto em MW tal como retornado pela API — sem conversão de unidades
- `process_date` é a chave de partição para DELETE + INSERT idempotente
- Duplicados ocasionais da API são preservados em Bronze; resolvidos em Silver

---

### `iceberg.bronze.preco_api_raw`

**Origem (default):** Energy-Charts API — `GET /price?bzn=PT` + `GET /price?bzn=ES`  
**Origem (alternativa):** ENTSO-E — `query_day_ahead_prices('PT'/'ES')` (requer `ENTSOE_TOKEN`)  
**Granularidade:** horária (€/MWh)  
**Partição:** `process_date`  
**Localização:** `s3a://warehouse/bronze/preco_api_raw/`

| Coluna | Tipo | Obrigatória | Descrição |
|--------|------|-------------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Sim | Timestamp UTC do início da hora |
| `price_portugal_eur_mwh` | DOUBLE | Sim | Preço day-ahead Portugal em €/MWh |
| `price_spain_eur_mwh` | DOUBLE | Não | Preço day-ahead Espanha em €/MWh |
| `source_url` | VARCHAR | Não | URL da chamada à API |
| `fetch_date` | DATE | Não | Data da chamada |
| `process_date` | DATE | Sim | Data lógica de ingestão — chave de partição |

**Regras:**
- Preços negativos são preservados — são dados de mercado legítimos (oversupply renovável)
- `price_spain_eur_mwh` mantida para análise comparativa PT/ES

---

## 2. Silver

### `iceberg.silver.consumo_api_hourly`

**Upstream:** `bronze.consumo_api_raw`  
**Granularidade:** horária (MWh, UTC canónico)  
**Partição:** `year`, `month`  
**Localização:** `s3a://warehouse/silver/consumo_api_hourly/`

| Coluna | Tipo | Obrigatória | Derivação | Descrição |
|--------|------|-------------|-----------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Sim | `DATE_TRUNC('hour', ts_utc)` | Timestamp UTC canónico alinhado à hora |
| `total_mwh` | DOUBLE | Sim | `ROUND(AVG(total), 3)` | Carga horária em **MWh** (deduplicada, arredondada a 3 casas) |
| `year` | INTEGER | Sim | `YEAR(ts_utc)` | Ano — coluna de partição |
| `month` | INTEGER | Sim | `MONTH(ts_utc)` | Mês — coluna de partição |

**Propriedades Iceberg:** `upstream_table = bronze.consumo_api_raw`, `grain = hourly`

---

### `iceberg.silver.preco_api_hourly`

**Upstream:** `bronze.preco_api_raw`  
**Granularidade:** horária (€/MWh, UTC canónico)  
**Partição:** `year`, `month`  
**Localização:** `s3a://warehouse/silver/preco_api_hourly/`

| Coluna | Tipo | Obrigatória | Derivação | Descrição |
|--------|------|-------------|-----------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Sim | `DATE_TRUNC('hour', ts_utc)` | Timestamp UTC canónico alinhado à hora |
| `price_portugal_eur_mwh` | DOUBLE | Sim | `ROUND(AVG(price_portugal_eur_mwh), 2)` | Preço day-ahead PT em €/MWh (deduplicado) |
| `price_spain_eur_mwh` | DOUBLE | Não | `ROUND(AVG(price_spain_eur_mwh), 2)` | Preço day-ahead ES em €/MWh (deduplicado) |
| `year` | INTEGER | Sim | `YEAR(ts_utc)` | Ano — coluna de partição |
| `month` | INTEGER | Sim | `MONTH(ts_utc)` | Mês — coluna de partição |

**Propriedades Iceberg:** `upstream_table = bronze.preco_api_raw`, `grain = hourly`

---

## 3. Gold

### `iceberg.gold.dp_energy_market_api_hourly`

**Tipo:** Produto analítico principal  
**Upstream:** `silver.consumo_api_hourly` INNER JOIN `silver.preco_api_hourly`  
**Partição:** `year`, `month`  
**Localização:** `s3a://warehouse/gold/dp_energy_market_api_hourly/`

| Coluna | Tipo | Nullable | Categoria | Descrição |
|--------|------|----------|-----------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Não | Chave | Timestamp UTC — chave de negócio única |
| `consumo_total` | DOUBLE | Não | Métrica | Carga eléctrica horária nacional Portugal (MWh) |
| `market_price_pt` | DOUBLE | Não | Métrica | Preço day-ahead Portugal (€/MWh) |
| `hora` | INTEGER [0-23] | Não | Calendário | Hora do dia extraída de `ts_utc` |
| `dia_semana` | INTEGER [0-6] | Não | Calendário | 0=Segunda … 6=Domingo |
| `is_weekend` | BOOLEAN | Não | Calendário | TRUE se dia_semana IN (5, 6) |
| `consumo_lag_1h` | DOUBLE | Sim | Lag | Consumo 1h antes (NULL nas primeiras horas) |
| `consumo_lag_24h` | DOUBLE | Sim | Lag | Consumo 24h antes (NULL nas primeiras 24h) |
| `price_lag_1h` | DOUBLE | Sim | Lag | Preço 1h antes (NULL na primeira hora) |
| `rolling_avg_consumo_24h` | DOUBLE | Sim | Rolling | Média móvel de consumo das últimas 24h |
| `rolling_avg_price_24h` | DOUBLE | Sim | Rolling | Média móvel de preço das últimas 24h |
| `process_date` | DATE | Não | Meta | Data de execução do pipeline |
| `year` | INTEGER | Não | Partição | Ano |
| `month` | INTEGER | Não | Partição | Mês |

**Propriedades Iceberg:** `product_version=v1`, `schema_version=1`, `deprecated=false`

---

### `iceberg.gold.feat_load_forecasting_api_hourly`

**Tipo:** Feature table ML  
**Upstream:** `gold.dp_energy_market_api_hourly`  
**Nota:** Primeiras 24 linhas (lags nulos) e última linha (sem target) excluídas — dataset pronto para treino.

Todas as colunas de `dp_energy_market_api_hourly`, mais:

| Coluna | Tipo | Nullable | Papel | Descrição |
|--------|------|----------|-------|-----------|
| `consumo_next_hour` | DOUBLE | Não | **TARGET** | Consumo da hora seguinte — `LEAD(consumo_total, 1)` |

**Propriedades Iceberg:** `product_version=v1`, `schema_version=1`, `feature_schema_version=1`, `upstream_table=gold.dp_energy_market_api_hourly`

---

## 4. Campos preservados vs descartados

### Bronze → Silver (consumo)

| Campo Bronze | Destino Silver | Operação |
|-------------|---------------|----------|
| `ts_utc` | `ts_utc` | `DATE_TRUNC('hour', ts_utc)` — alinhamento horário |
| `total` (MW) | `total_mwh` (MWh) | `ROUND(AVG(total), 3)` — deduplicação + conversão |
| `source_url` | — | Descartado (rastreabilidade já garantida por `fetch_date`) |
| `fetch_date` | — | Descartado (partição passa a `year`/`month`) |
| `process_date` | — | Descartado |

### Bronze → Silver (preço)

| Campo Bronze | Destino Silver | Operação |
|-------------|---------------|----------|
| `ts_utc` | `ts_utc` | `DATE_TRUNC('hour', ts_utc)` |
| `price_portugal_eur_mwh` | `price_portugal_eur_mwh` | `ROUND(AVG(...), 2)` — deduplicação |
| `price_spain_eur_mwh` | `price_spain_eur_mwh` | `ROUND(AVG(...), 2)` — deduplicação |
| `source_url` | — | Descartado |
| `fetch_date` | — | Descartado |
| `process_date` | — | Descartado |
