# Transformações por Camada — DP-02 Streaming_Data

## 1. Bronze — Ingestão raw da API

### Objetivo

Preservar a resposta da API sem transformações semânticas. Adicionar apenas metadados de rastreabilidade (`source_url`, `fetch_date`, `process_date`).

O pipeline suporta duas fontes intercambiáveis — as tabelas Bronze são idênticas em ambos os casos:

| Fonte | Workflow | Autenticação |
|-------|----------|--------------|
| **Energy-Charts API** (default) | `flyte_fetch_bronze_energycharts.py` | Nenhuma |
| ENTSO-E Transparency Platform | `flyte_fetch_bronze_api.py` | `ENTSOE_TOKEN` |

### Workflows Flyte

**Fonte default (Energy-Charts):**

```
fetch_bronze_energycharts
  ├── fetch_consumo_ec  →  iceberg.bronze.consumo_api_raw
  └── fetch_preco_ec    →  iceberg.bronze.preco_api_raw
```

**Fonte alternativa (ENTSO-E):**

```
fetch_bronze_api
  ├── fetch_consumo_api  →  iceberg.bronze.consumo_api_raw
  └── fetch_preco_api    →  iceberg.bronze.preco_api_raw
```

As tasks correm **em paralelo** e são **idempotentes** (eliminam partições do intervalo antes de inserir).

### Pseudo-código de ingestão (Energy-Charts — default)

```python
# Para cada chunk do intervalo pedido:
conn.execute(f"DELETE FROM bronze.consumo_api_raw WHERE process_date IN ({dates})")

for row in energycharts_client.get_total_power(country='pt', start=start, end=end):
    INSERT INTO bronze.consumo_api_raw (ts_utc, total, source_url, fetch_date, process_date)
    VALUES (row.ts_utc, row.total_mw, url, today, today)
```

---

## 2. Silver — Normalização e limpeza

### Objetivo

Produzir séries temporais horárias limpas, sem duplicados, em UTC canónico, com unidades normalizadas.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_bronze_to_silver.py`

| Workflow | Descrição |
|----------|-----------|
| `bronze_to_silver_api` | Transforma um `process_date` específico |
| `bronze_to_silver_api_full` | Materializa todo o histórico Bronze |

As duas tasks (consumo + preço) correm em paralelo. Ambas são idempotentes.

### Transformações — Consumo

**`bronze.consumo_api_raw` → `silver.consumo_api_hourly`**

| Passo | Operação SQL | Motivo |
|-------|-------------|--------|
| 1. Filtrar nulos | `WHERE ts_utc IS NOT NULL AND total IS NOT NULL AND total > 0` | Remove linhas inválidas da API |
| 2. Alinhar à hora | `DATE_TRUNC('hour', ts_utc)` | Garante granularidade horária exacta |
| 3. Deduplicar | `GROUP BY DATE_TRUNC('hour', ts_utc)` | Resolve duplicados ocasionais da ENTSO-E |
| 4. Converter unidade | `ROUND(AVG(total), 3) AS total_mwh` | MW × 1h = MWh (granularidade já horária) |
| 5. Derivar partição | `YEAR(ts_utc)`, `MONTH(ts_utc)` | Colunas de partição Iceberg |

```sql
INSERT INTO iceberg.silver.consumo_api_hourly (ts_utc, total_mwh, year, month)
SELECT
    DATE_TRUNC('hour', ts_utc)       AS ts_utc,
    ROUND(AVG(total), 3)             AS total_mwh,
    YEAR(DATE_TRUNC('hour', ts_utc)) AS year,
    MONTH(DATE_TRUNC('hour', ts_utc)) AS month
FROM iceberg.bronze.consumo_api_raw
WHERE ts_utc IS NOT NULL
  AND total IS NOT NULL
  AND total > 0
GROUP BY DATE_TRUNC('hour', ts_utc)
```

### Transformações — Preço

**`bronze.preco_api_raw` → `silver.preco_api_hourly`**

| Passo | Operação SQL | Motivo |
|-------|-------------|--------|
| 1. Filtrar nulos | `WHERE ts_utc IS NOT NULL AND price_portugal_eur_mwh IS NOT NULL` | Remove linhas inválidas |
| 2. Alinhar à hora | `DATE_TRUNC('hour', ts_utc)` | Granularidade horária exacta |
| 3. Deduplicar | `GROUP BY DATE_TRUNC('hour', ts_utc)` | Resolve duplicados ocasionais |
| 4. Arredondar | `ROUND(AVG(...), 2)` | Preços em €/MWh com 2 casas decimais |
| 5. Derivar partição | `YEAR(ts_utc)`, `MONTH(ts_utc)` | Colunas de partição Iceberg |

**Nota:** preços negativos são filtrados como WARN mas **não são removidos** — são dados legítimos de mercado (oversupply solar/eólico).

---

## 3. Gold — Enriquecimento analítico e ML

### Objetivo

Construir os dois data products a partir de um INNER JOIN entre Silver consumo e Silver preço, enriquecidos com features de calendário, lags temporais e médias móveis.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_silver_to_gold.py`  
**Workflow:** `silver_to_gold_api_full`

```
silver_to_gold_api_full
  ├── build_dp_energy_market_api_full      →  dp_energy_market_api_hourly
  └── build_feat_load_forecasting_api_full →  feat_load_forecasting_api_hourly
        (depende do upstream — executa depois)
```

As window functions operam sobre o **histórico completo** para garantir lags e médias móveis corretos nas fronteiras de data.

### Transformações — Produto analítico

**`silver.consumo_api_hourly` × `silver.preco_api_hourly` → `gold.dp_energy_market_api_hourly`**

```sql
WITH joined AS (
    SELECT
        c.ts_utc,
        c.total_mwh                              AS consumo_total,
        p.price_portugal_eur_mwh                 AS market_price_pt,
        HOUR(c.ts_utc)                           AS hora,
        DAY_OF_WEEK(c.ts_utc) - 1               AS dia_semana,
        DAY_OF_WEEK(c.ts_utc) >= 6              AS is_weekend,
        YEAR(c.ts_utc)                           AS year,
        MONTH(c.ts_utc)                          AS month
    FROM iceberg.silver.consumo_api_hourly c
    INNER JOIN iceberg.silver.preco_api_hourly p ON c.ts_utc = p.ts_utc
),
with_windows AS (
    SELECT
        *,
        LAG(consumo_total, 1)  OVER (ORDER BY ts_utc) AS consumo_lag_1h,
        LAG(consumo_total, 24) OVER (ORDER BY ts_utc) AS consumo_lag_24h,
        LAG(market_price_pt, 1) OVER (ORDER BY ts_utc) AS price_lag_1h,
        AVG(consumo_total) OVER (
            ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_consumo_24h,
        AVG(market_price_pt) OVER (
            ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_price_24h
    FROM joined
)
SELECT *, CURRENT_DATE AS process_date FROM with_windows
```

**Impacto do INNER JOIN:** apenas horas com consumo E preço disponíveis são incluídas. Taxa de junção alvo ≥ 98%.

### Transformações — Feature table ML

**`gold.dp_energy_market_api_hourly` → `gold.feat_load_forecasting_api_hourly`**

```sql
WITH with_lead AS (
    SELECT
        *,
        LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour
    FROM iceberg.gold.dp_energy_market_api_hourly
)
SELECT * FROM with_lead
WHERE consumo_next_hour IS NOT NULL   -- remove última linha (sem target futuro)
  AND consumo_lag_1h    IS NOT NULL   -- remove primeiras horas sem lag 1h
  AND consumo_lag_24h   IS NOT NULL   -- remove primeiras 24h sem lag 24h
  AND price_lag_1h      IS NOT NULL
```

**Resultado:** dataset limpo e completo, pronto para treino do modelo GBR sem pré-processamento adicional.

---

## 4. Regras de negócio

| Regra | Onde se aplica |
|-------|---------------|
| `ts_utc` é sempre UTC — sem ajuste de DST ou fuso local | Todas as camadas |
| Preços negativos são registados como WARN mas não removidos | Silver → Gold |
| Duplicados ocasionais da ENTSO-E resolvidos por AVG | Silver |
| INNER JOIN consumo × preço — apenas horas com ambos disponíveis | Gold |
| Lags e rolling averages calculados sobre histórico completo | Gold (window functions) |
| Últimas 25 linhas potencialmente excluídas da feat table | Gold feat |
