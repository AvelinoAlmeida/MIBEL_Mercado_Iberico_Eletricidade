# Fontes de Dados — DP-02 Streaming_Data

O pipeline suporta **duas fontes** para o fetch Bronze, selecionadas via `--source`:

| Fonte | Flag | Autenticação | Default |
|-------|------|--------------|---------|
| Energy-Charts API (Fraunhofer ISE) | `--source energycharts` | Nenhuma | **Sim** |
| ENTSO-E Transparency Platform | `--source entsoe` | Token gratuito | Não |

As tabelas Bronze de destino (`consumo_api_raw`, `preco_api_raw`) são as mesmas independentemente da fonte — Silver, Gold e quality checks não mudam.

---

## 1. Energy-Charts API (default)

| Atributo | Valor |
|----------|-------|
| **Nome** | Energy-Charts API (Fraunhofer ISE) |
| **URL** | https://api.energy-charts.info |
| **Tipo** | REST API pública — sem autenticação |
| **Biblioteca Python** | `requests` |
| **Cobertura histórica** | 2022-01-01 → presente |

### Endpoints utilizados

| Dado | Endpoint | Unidade | Granularidade |
|------|----------|---------|---------------|
| Carga eléctrica nacional Portugal | `GET /total_power?country=pt` | MW | Horária |
| Preço day-ahead Portugal (MIBEL) | `GET /price?bzn=PT` | €/MWh | Horária |
| Preço day-ahead Espanha (MIBEL) | `GET /price?bzn=ES` | €/MWh | Horária |

O endpoint `/total_power` redistribui dados ENTSO-E (Actual Total Load). O endpoint `/price` disponibiliza preços OMIE/MIBEL para a zona de balanço PT e ES.

**Nota:** Para alguns períodos históricos, `/total_power` pode retornar granularidade 15-min. O Silver agrega com `DATE_TRUNC('hour') + AVG`, pelo que é transparente para as camadas seguintes.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_fetch_bronze_energycharts.py`
**Workflow:** `fetch_bronze_energycharts`

---

## 2. ENTSO-E Transparency Platform (alternativa)

| Atributo | Valor |
|----------|-------|
| **Nome** | ENTSO-E Transparency Platform |
| **URL** | https://transparency.entsoe.eu/ |
| **Tipo** | REST API pública (requer token gratuito) |
| **Biblioteca Python** | `entsoe-py` |
| **Cobertura histórica** | 2022-01-01 → presente |

### Autenticação

O acesso à API requer um token gratuito:

1. Enviar email para `transparency@entsoe.eu`
2. Assunto: `"RESTful API access"`
3. Resposta em ~3 dias úteis com o token
4. Definir variável de ambiente antes de executar o pipeline:

```powershell
$env:ENTSOE_TOKEN = "<o-teu-token>"
```

### Endpoints utilizados

| Dado | Método entsoe-py | Unidade | Granularidade |
|------|-----------------|---------|---------------|
| Carga eléctrica nacional Portugal | `query_load('PT')` | MW | Horária |
| Preço day-ahead Portugal (MIBEL) | `query_day_ahead_prices('PT')` | €/MWh | Horária |
| Preço day-ahead Espanha (MIBEL) | `query_day_ahead_prices('ES')` | €/MWh | Horária |

#### `query_load('PT')` — Actual Total Load

Retorna a carga eléctrica real total de Portugal para o período pedido. Os dados são publicados com um atraso de ~15–30 minutos após fecho da hora. A unidade de retorno é **MW**; é convertida para **MWh** na camada Silver (granularidade horária: `total_mwh = ROUND(AVG(total), 3)`).

#### `query_day_ahead_prices('PT'/'ES')` — Day-Ahead Prices

Retorna os preços do mercado ibérico de energia (MIBEL) para o dia seguinte. Os preços são publicados às ~12h00 do dia D-1 para todas as 24 horas do dia D. Preços negativos são válidos e ocorrem quando existe excesso de geração renovável.

### Comportamento da API

**Duplicados ocasionais:** A API ENTSO-E pode retornar duplicados de timestamps para um mesmo período. A camada Silver resolve-os com `GROUP BY ts_utc` e `AVG(total)`.

**DST (Daylight Saving Time):** A API retorna todos os timestamps em UTC — não há ambiguidade de hora 25 (ao contrário do pipeline CSV Static_Data).

**Freshness:**

| Dado | Publicação | Atraso máximo esperado |
|------|------------|------------------------|
| Consumo (Actual Load) | Real-time, ~30 min após hora | ≤ 3 dias |
| Preços day-ahead | Dia D-1, ~12h00 | ≤ 2 dias |

### Workflow Flyte

**Ficheiro:** `workflows/flyte_fetch_bronze_api.py`
**Workflow:** `fetch_bronze_api`

As duas tasks (`fetch_consumo_api` e `fetch_preco_api`) correm **em paralelo**. Cada task é **idempotente**: apaga as partições `process_date` do intervalo antes de inserir.

```
fetch_bronze_api
  ├── fetch_consumo_api  →  iceberg.bronze.consumo_api_raw
  └── fetch_preco_api    →  iceberg.bronze.preco_api_raw
```

---

## 3. Tabelas Bronze criadas

Ambas as tabelas têm sufixo `_api` para coexistência com o pipeline Static_Data.

### `iceberg.bronze.consumo_api_raw`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC do início da hora |
| `total` | DOUBLE | Carga total nacional em **MW** (valor bruto da API) |
| `source_url` | VARCHAR | URL da chamada à API (rastreabilidade) |
| `fetch_date` | DATE | Data em que a chamada foi feita |
| `process_date` | DATE | Data lógica de ingestão — **coluna de partição** |

### `iceberg.bronze.preco_api_raw`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC do início da hora |
| `price_portugal_eur_mwh` | DOUBLE | Preço day-ahead Portugal em €/MWh |
| `price_spain_eur_mwh` | DOUBLE | Preço day-ahead Espanha em €/MWh (NULL se não disponível) |
| `source_url` | VARCHAR | URL da chamada à API |
| `fetch_date` | DATE | Data da chamada |
| `process_date` | DATE | Data lógica de ingestão — **coluna de partição** |

---

## 4. Scripts standalone de ingestão

Permitem ingerir dados directamente sem correr o pipeline completo. Úteis para verificar o token, testar a API ou repopular um intervalo pontual.

| Script | Fonte | Descrição |
|--------|-------|-----------|
| `01_bronze/scripts/python/fetch_consumo_entsoe.py` | ENTSO-E | Carga eléctrica nacional PT (Actual Total Load) |
| `01_bronze/scripts/python/fetch_preco_entsoe.py` | ENTSO-E | Preços day-ahead PT + ES (Day-Ahead Prices) |

```powershell
$env:ENTSOE_TOKEN = "<o-teu-token>"

# Consumo — últimos 7 dias
python 01_bronze/scripts/python/fetch_consumo_entsoe.py --days 7

# Preços — intervalo específico
python 01_bronze/scripts/python/fetch_preco_entsoe.py --start 2024-01-01 --end 2024-01-31
```

Dependências: `entsoe-py`, `pandas`.
