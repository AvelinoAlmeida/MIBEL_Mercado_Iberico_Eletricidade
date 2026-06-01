# Desenho do Lakehouse — DP-02 Streaming_Data

## 1. Objetivo

O lakehouse organiza os dados em três camadas — Bronze, Silver e Gold — para garantir rastreabilidade da origem, normalização progressiva e disponibilização de produtos prontos a consumir.

| Camada | Função |
|--------|--------|
| **Bronze** | Preservação raw da resposta da API ENTSO-E (sem transformação semântica) |
| **Silver** | Limpeza, deduplicação, normalização UTC, conversão de unidades |
| **Gold** | Data products com features analíticas, lags e médias móveis; ML-ready |

UTC é o tempo canónico em todas as camadas Silver e Gold.

---

## 2. Tabelas por camada

### Bronze

| Tabela | Origem (default) | Origem (alternativa) |
|--------|-----------------|----------------------|
| `iceberg.bronze.consumo_api_raw` | Energy-Charts `total_power?country=pt` | ENTSO-E `query_load('PT')` |
| `iceberg.bronze.preco_api_raw` | Energy-Charts `price?bzn=PT` + `price?bzn=ES` | ENTSO-E `query_day_ahead_prices('PT'/'ES')` |

### Silver

| Tabela | Upstream |
|--------|----------|
| `iceberg.silver.consumo_api_hourly` | `bronze.consumo_api_raw` |
| `iceberg.silver.preco_api_hourly` | `bronze.preco_api_raw` |

### Gold

| Tabela | Tipo | Upstream |
|--------|------|----------|
| `iceberg.gold.dp_energy_market_api_hourly` | Produto analítico | `silver.consumo_api_hourly` × `silver.preco_api_hourly` |
| `iceberg.gold.feat_load_forecasting_api_hourly` | Feature table ML | `gold.dp_energy_market_api_hourly` |

---

## 3. Estratégia de particionamento

| Camada | Coluna(s) | Justificação |
|--------|-----------|--------------|
| Bronze | `process_date` (DATE) | Rastreabilidade da ingestão diária; backfill por dia idempotente |
| Silver | `year`, `month` (INTEGER) | ~720 linhas/partição; consultas por período temporal eficientes |
| Gold | `year`, `month` (INTEGER) | Consumidores analíticos e ML filtram por intervalo temporal |

---

## 4. Formato e armazenamento

| Atributo | Valor |
|----------|-------|
| **Formato** | Parquet (colunar, compressão eficiente) |
| **Versão Iceberg** | format_version=2 (row-level deletes, merge-on-read) |
| **Object store** | MinIO — `s3a://warehouse/` |
| **Layout** | `object_store_layout_enabled=true` (distribui ficheiros, evita hot spots) |

### Localização MinIO

```
s3a://warehouse/
  bronze/consumo_api_raw/
  bronze/preco_api_raw/
  silver/consumo_api_hourly/
  silver/preco_api_hourly/
  gold/dp_energy_market_api_hourly/
  gold/feat_load_forecasting_api_hourly/
```

---

## 5. Idempotência e compaction

### Estratégia de idempotência

Todos os workflows usam **DELETE + INSERT por partição**:
- Bronze: `DELETE WHERE process_date IN (intervalo)` antes de cada ingestão
- Silver/Gold: `DELETE WHERE year=X AND month=Y` antes de cada materialização

Este padrão garante que re-execuções do mesmo intervalo produzem sempre o mesmo resultado.

### Compaction

Para volumes normais do projeto não é necessária compaction automática. Se existirem reprocessamentos parciais frequentes:

```sql
-- Compaction manual via Trino (Iceberg OPTIMIZE)
ALTER TABLE iceberg.gold.dp_energy_market_api_hourly
    EXECUTE optimize
    WHERE year = 2024 AND month = 1;
```

Em produção com ingestão diária recomenda-se compaction mensal após fecho de cada partição `year`/`month`.

---

## 6. Propriedades Iceberg (catálogo embutido)

Todas as tabelas têm `extra_properties` que funcionam como catálogo mínimo. Consultáveis via Trino:

```sql
SHOW CREATE TABLE iceberg.gold.dp_energy_market_api_hourly;
DESCRIBE iceberg.gold.dp_energy_market_api_hourly;
```

**Bronze:**

| Propriedade | Valor |
|-------------|-------|
| `layer` | `bronze` |
| `domain` | `consumo_preco` |
| `schema_version` | `1` |
| `retention_policy` | `indefinite` |
| `source_system` | `energycharts_api` (default) / `entsoe_api` (alternativa) |

**Silver:**

| Propriedade | Valor |
|-------------|-------|
| `layer` | `silver` |
| `domain` | `consumo_preco` |
| `schema_version` | `1` |
| `grain` | `hourly` |
| `upstream_table` | `bronze.consumo_api_raw` / `bronze.preco_api_raw` |

**Gold:**

| Propriedade | Valor |
|-------------|-------|
| `layer` | `gold` |
| `data_product` | `dp_energy_market_api_hourly` |
| `schema_version` | `1` |
| `product_version` | `v1` |
| `deprecated` | `false` |
| `domain` | `consumo_preco` |
| `grain` | `hourly` |
| `feature_schema_version` | (só feat table) `1` |
| `upstream_table` | (só feat table) `gold.dp_energy_market_api_hourly` |

---

## 7. Convenções de naming

### Schemas

`bronze`, `silver`, `gold` — nomes curtos sem ambiguidade.

### Tabelas

- `snake_case` em todas as tabelas
- Sufixo `_api` — distingue das tabelas homólogas do pipeline Static_Data (CSV)
- Prefixo semântico: `dp_` (data product analítico), `feat_` (feature table ML), sem prefixo em Bronze/Silver

### Colunas

- `snake_case` em todas as colunas
- Chave temporal canónica: `ts_utc`
- Partições: `year`, `month`, `process_date`
- Features temporais: `hora`, `dia_semana`, `is_weekend`
- Métricas com unidade: `total_mwh`, `price_portugal_eur_mwh`

---

## 8. Princípios de design

- Preservar a origem em Bronze sem transformações semânticas
- Limpar e harmonizar em Silver (UTC canónico, granularidade horária, MW→MWh)
- Expor consumo analítico em Gold (features, lags, rolling averages, target ML)
- Separar claramente dados operacionais, intermédios e finais
- Iceberg format_version=2 em todas as tabelas (suporte a schema evolution e row-level deletes)
- Idempotência via DELETE + INSERT por partição em todos os workflows
- Documentar intenção via Iceberg table properties e comentários de coluna
