# DP-02 Streaming_Data — Resumo do Pipeline Medallion

Pipeline de ingestão contínua de dados de consumo e preços de eletricidade para Portugal via
**ENTSO-E Transparency Platform** (transparency.entsoe.eu).

Coexiste com o pipeline estático (Static_Data): todas as tabelas têm sufixo `_api`.

---

## Arquitetura geral

```
ENTSO-E API
  ├── Actual Total Load PT  (query_load)
  └── Day-Ahead Prices PT+ES  (query_day_ahead_prices)
        │
        ▼
   BRONZE  — ingestão raw, 1 linha por hora
        │
        ▼  deduplicação · filtragem nulos · unidade MW→MWh
   SILVER  — dados limpos e normalizados
        │
        ▼  join · features calendário · lags · rolling averages
     GOLD  — produto analítico + feature table ML
```

Formato de armazenamento: **Apache Parquet** em MinIO (`s3a://warehouse/`), gerido por **Apache Iceberg** via **Trino**.

Orquestração: workflows **Flyte** em `workflows/`, executados localmente via `pyflyte run`.

---

## Bronze — Ingestão raw

### Fonte de dados

O pipeline suporta duas fontes intercambiáveis. As tabelas Bronze são idênticas em ambos os casos — a camada Silver, Gold e os quality checks não requerem alterações conforme a fonte escolhida.

#### Fonte padrão: Energy-Charts API (Fraunhofer ISE) — sem autenticação

| Dado | Endpoint | Unidade | Granularidade |
| ---- | -------- | ------- | ------------- |
| Carga eléctrica nacional PT | `GET /public_power?country=pt` | MW | Horária (pode devolver 15 min — agregado para horário) |
| Preço day-ahead Portugal | `GET /price?bzn=PT` | €/MWh | Horária |
| Preço day-ahead Espanha | `GET /price?bzn=ES` | €/MWh | Horária |

Não requer registo. Dados ENTSO-E redistribuídos pela Fraunhofer ISE.

> **Nota sobre granularidade:** a Energy-Charts API pode devolver dados a 15 min para alguns períodos. A função `_aggregate_to_hourly()` agrega por hora via média antes da inserção; o Silver faz o mesmo com `DATE_TRUNC('hour') + AVG`, garantindo idempotência independente da granularidade.

#### Fonte alternativa: ENTSO-E Transparency Platform — token obrigatório

| Dado | Endpoint ENTSO-E | Unidade | Granularidade |
|------|-----------------|---------|---------------|
| Carga eléctrica nacional PT | `query_load('PT')` | MW | Horária |
| Preço day-ahead Portugal | `query_day_ahead_prices('PT')` | €/MWh | Horária |
| Preço day-ahead Espanha | `query_day_ahead_prices('ES')` | €/MWh | Horária |

**Autenticação:** variável de ambiente `ENTSOE_TOKEN` (token gratuito — email para transparency@entsoe.eu, assunto "RESTful API access", resposta em ~3 dias úteis).

### Tabelas criadas

#### `iceberg.bronze.consumo_api_raw`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC do início da hora |
| `total` | DOUBLE | Carga total nacional em MW |
| `source_url` | VARCHAR | URL da chamada à API (rastreabilidade) |
| `fetch_date` | DATE | Data em que a chamada foi feita |
| `process_date` | DATE | Data lógica de ingestão (coluna de partição) |

Particionada por `process_date`.

#### `iceberg.bronze.preco_api_raw`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC do início da hora |
| `price_portugal_eur_mwh` | DOUBLE | Preço day-ahead Portugal em €/MWh |
| `price_spain_eur_mwh` | DOUBLE | Preço day-ahead Espanha em €/MWh |
| `source_url` | VARCHAR | URL da chamada à API |
| `fetch_date` | DATE | Data da chamada |
| `process_date` | DATE | Data lógica de ingestão (coluna de partição) |

Particionada por `process_date`.

### Workflows Flyte

Existem dois workflows de Bronze, um por fonte. As tabelas de destino são as mesmas em ambos os casos.

| Ficheiro | Workflow | Fonte | Autenticação |
| -------- | -------- | ----- | ------------ |
| `workflows/flyte_fetch_bronze_energycharts.py` | `fetch_bronze_energycharts` | Energy-Charts API | Nenhuma (padrão) |
| `workflows/flyte_fetch_bronze_api.py` | `fetch_bronze_api` | ENTSO-E Transparency | `ENTSOE_TOKEN` obrigatório |

Em ambos, as duas tarefas (consumo + preço) correm **em paralelo** e são **idempotentes** — apagam as partições `process_date` do intervalo antes de inserir.

```
fetch_bronze_energycharts          fetch_bronze_api
  ├── fetch_consumo_ec  ─┐           ├── fetch_consumo_api  ─┐
  └── fetch_preco_ec    ─┴─►  consumo_api_raw + preco_api_raw
                               └── fetch_preco_api    ─┘
```

### Quality gate Bronze (12 checks)

| # | Check | Tabela | Status |
| - | ----- | ------ | ------ |
| 1 | Nulos em `ts_utc` | `consumo_api_raw` | FAIL |
| 2 | Nulos em `total` | `consumo_api_raw` | FAIL |
| 3 | Nulos em `ts_utc` | `preco_api_raw` | FAIL |
| 4 | Nulos em `price_portugal_eur_mwh` | `preco_api_raw` | FAIL |
| 5 | Range `total > 0` (MW positivo) | `consumo_api_raw` | WARN |
| 6 | Preço PT não-negativo (negativos possíveis no MIBEL) | `preco_api_raw` | WARN |
| 7 | Unicidade `ts_utc` (duplicados recuperáveis pela Silver) | `consumo_api_raw` | WARN |
| 8 | Unicidade `ts_utc` (duplicados corrompem joins Gold) | `preco_api_raw` | FAIL |
| 9 | Freshness máx. 3 dias de atraso | `consumo_api_raw` | WARN |
| 10 | Freshness máx. 2 dias (day-ahead publica D-1) | `preco_api_raw` | WARN |
| 11 | Completude diária ≥ 23 horas (tolerância DST) | `consumo_api_raw` | WARN |
| 12 | Completude diária ≥ 23 horas (tolerância DST) | `preco_api_raw` | WARN |

---

## Silver — Normalização e limpeza

### Transformações aplicadas

#### Consumo (`bronze.consumo_api_raw` → `silver.consumo_api_hourly`)

1. **Filtragem de nulos** — exclui linhas com `ts_utc IS NULL`, `total IS NULL` ou `total <= 0`
2. **Deduplicação** — `GROUP BY ts_utc` com `AVG(total)` (resolve duplicados ocasionais da API)
3. **Conversão de unidade** — MW × 1h = MWh (dado que a granularidade já é horária, `total_mwh = ROUND(AVG(total), 3)`)
4. **Derivação de partição** — colunas `year` e `month` extraídas de `ts_utc`

#### Preços (`bronze.preco_api_raw` → `silver.preco_api_hourly`)

1. **Filtragem de nulos** — exclui linhas com `ts_utc IS NULL` ou `price_portugal_eur_mwh IS NULL`
2. **Alinhamento horário** — `DATE_TRUNC('hour', ts_utc)` (garante granularidade horária exacta)
3. **Deduplicação** — `GROUP BY DATE_TRUNC('hour', ts_utc)` com `ROUND(AVG(...), 2)` para PT e ES
4. **Derivação de partição** — `year` e `month`

### Tabelas criadas

#### `iceberg.silver.consumo_api_hourly`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC canónico |
| `total_mwh` | DOUBLE | Carga horária em MWh (arredondado a 3 casas) |
| `year` | INTEGER | Ano — partição |
| `month` | INTEGER | Mês — partição |

#### `iceberg.silver.preco_api_hourly`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC canónico |
| `price_portugal_eur_mwh` | DOUBLE | Preço day-ahead PT em €/MWh (arredondado a 2 casas) |
| `price_spain_eur_mwh` | DOUBLE | Preço day-ahead ES em €/MWh (arredondado a 2 casas) |
| `year` | INTEGER | Ano — partição |
| `month` | INTEGER | Mês — partição |

Ambas as tabelas particionadas por `year / month`.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_bronze_to_silver.py`

| Workflow | Descrição |
|----------|-----------|
| `bronze_to_silver_api` | Transforma um `process_date` específico |
| `bronze_to_silver_api_full` | Materializa todo o histórico Bronze |

As duas tarefas (consumo + preço) correm em paralelo. Ambas são idempotentes (apagam o intervalo antes de inserir).

---

## Gold — Enriquecimento analítico e ML

### Transformações aplicadas

O Gold constrói dois produtos a partir de um **INNER JOIN** entre Silver consumo e Silver preço por `ts_utc`.

#### Produto analítico: `dp_energy_market_api_hourly`

1. **Join** — `silver.consumo_api_hourly INNER JOIN silver.preco_api_hourly ON ts_utc`
   (apenas horas com consumo **e** preço disponíveis são incluídas)
2. **Features de calendário:**
   - `hora` — hora do dia (0–23), extraída de `ts_utc`
   - `dia_semana` — dia da semana (0 = Segunda … 6 = Domingo)
   - `is_weekend` — `TRUE` para Sábado e Domingo
3. **Lags temporais** (window functions sobre a série ordenada por `ts_utc`):
   - `consumo_lag_1h` — consumo 1 hora antes
   - `consumo_lag_24h` — consumo 24 horas antes (mesmo período do dia anterior)
   - `price_lag_1h` — preço 1 hora antes
4. **Médias móveis** (janela de 24 linhas anteriores + linha atual):
   - `rolling_avg_consumo_24h` — média de consumo das últimas 24 horas
   - `rolling_avg_price_24h` — média de preço das últimas 24 horas

#### Feature table ML: `feat_load_forecasting_api_hourly`

Derivada de `dp_energy_market_api_hourly` com adição de:

5. **Variável alvo:** `consumo_next_hour = LEAD(consumo_total, 1)` — consumo da hora seguinte
6. **Filtragem de exemplos incompletos:**
   - Remove a última linha da série (sem target futuro)
   - Remove as primeiras 24 linhas (lags ainda nulos)

### Tabelas criadas

#### `iceberg.gold.dp_energy_market_api_hourly`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Chave temporal primária |
| `consumo_total` | DOUBLE | Carga horária em MWh |
| `market_price_pt` | DOUBLE | Preço day-ahead PT em €/MWh |
| `hora` | INTEGER | Hora do dia (0–23) |
| `dia_semana` | INTEGER | Dia da semana (0=Segunda … 6=Domingo) |
| `is_weekend` | BOOLEAN | Sábado ou Domingo |
| `consumo_lag_1h` | DOUBLE | Consumo 1h antes (NULL nas primeiras horas) |
| `consumo_lag_24h` | DOUBLE | Consumo 24h antes (NULL nas primeiras 24h) |
| `price_lag_1h` | DOUBLE | Preço 1h antes (NULL na primeira hora) |
| `rolling_avg_consumo_24h` | DOUBLE | Média móvel 24h de consumo |
| `rolling_avg_price_24h` | DOUBLE | Média móvel 24h de preço |
| `process_date` | DATE | Data de execução do pipeline |
| `year` | INTEGER | Ano — partição |
| `month` | INTEGER | Mês — partição |

#### `iceberg.gold.feat_load_forecasting_api_hourly`

Todas as colunas de `dp_energy_market_api_hourly`, mais:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `consumo_next_hour` | DOUBLE | Consumo da hora seguinte — **variável alvo ML** |

Última linha e linhas com lags nulos são excluídas → dataset pronto para treino.

#### `iceberg.gold.ml_metrics_streaming`

Métricas do último run de treino ML (GBR load forecasting sobre dados streaming). Mantém apenas o run mais recente.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `run_ts` | TIMESTAMP WITH TIME ZONE | Timestamp do run MLflow |
| `model_name` | VARCHAR | Nome do algoritmo |
| `n_train` | BIGINT | Nº de exemplos de treino |
| `n_test` | BIGINT | Nº de exemplos de teste |
| `ts_train_start` / `ts_train_end` | TIMESTAMP WITH TIME ZONE | Janela temporal de treino |
| `ts_test_start` / `ts_test_end` | TIMESTAMP WITH TIME ZONE | Janela temporal de teste |
| `mae` | DOUBLE | Mean Absolute Error (MWh) |
| `rmse` | DOUBLE | Root Mean Squared Error (MWh) |
| `r2` | DOUBLE | R² score |
| `mape` | DOUBLE | Mean Absolute Percentage Error (%) |
| `n_estimators` / `max_depth` / `learning_rate` | INT / INT / DOUBLE | Hiperparâmetros GBR |

#### `iceberg.gold.ml_feature_importance_streaming`

Importância de features do último run de treino ML streaming. Mantém apenas o run mais recente.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `run_ts` | TIMESTAMP WITH TIME ZONE | Timestamp do run MLflow |
| `feature_name` | VARCHAR | Nome da feature |
| `importance` | DOUBLE | Importância normalizada [0, 1] |
| `rank` | INTEGER | Posição por importância descendente |

Ambas as tabelas são populadas por `03_ml_pipeline/preco_consumo_streaming_mlflow_flow.py`.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_silver_to_gold.py`

Dois modos de execução:

| Workflow | Modo | Quando usar |
| -------- | ---- | ----------- |
| `silver_to_gold_api_full` | Full — reconstrói todo o histórico Silver | Ingestão inicial ou reset completo |
| `silver_to_gold_api_incremental` | Incremental — reconstrói a partir de `since_date - 1 dia` | Execuções diárias (o recuo de 1 dia garante lags corretos nas fronteiras) |

As window functions (`LAG`, `AVG OVER`) operam **sem `PARTITION BY`** para garantir lags e médias móveis corretos nas fronteiras de mês/ano. As duas tarefas do workflow têm dependência sequencial — a feature table ML depende do produto analítico.

```
silver_to_gold_api_full / silver_to_gold_api_incremental
  ├── build_dp_energy_market_api_*      →  dp_energy_market_api_hourly
  └── build_feat_load_forecasting_api_* →  feat_load_forecasting_api_hourly
        (executa depois — depende do upstream)
```

---

## Auditoria e Observabilidade (v1.1.0)

### Tabelas de auditoria

Cada execução do pipeline persiste automaticamente dois registos em `iceberg.audit`:

#### `iceberg.audit.pipeline_runs`

Registo de cada execução — uma linha por run.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `run_id` | VARCHAR | UUID único desta execução |
| `pipeline_name` | VARCHAR | `dp02_streaming` |
| `pipeline_version` | VARCHAR | Versão do orquestrador (ex.: `1.1.0`) |
| `start_ts` | TIMESTAMP WITH TIME ZONE | Início UTC da execução |
| `end_ts` | TIMESTAMP WITH TIME ZONE | Fim UTC da execução |
| `duration_seconds` | DOUBLE | Duração total em segundos |
| `status` | VARCHAR | `SUCCESS` ou `FAILED` |
| `rows_bronze` | BIGINT | Linhas totais nas tabelas Bronze (consumo + preço) |
| `rows_silver` | BIGINT | Linhas totais nas tabelas Silver |
| `rows_gold` | BIGINT | Linhas em `dp_energy_market_api_hourly` |
| `source` | VARCHAR | Fonte de dados usada (`energycharts` ou `entsoe`) |
| `param_start_date` | VARCHAR | Parâmetro `--start` passado ao orquestrador |
| `param_end_date` | VARCHAR | Parâmetro `--end` passado ao orquestrador |
| `error_message` | VARCHAR | Mensagem de erro (vazio em `SUCCESS`) |

Particionada por `day(start_ts)` para consultas temporais eficientes.

#### `iceberg.audit.dataset_lineage`

Mapa upstream → downstream persistido a cada execução bem-sucedida.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `run_id` | VARCHAR | Referência ao `pipeline_runs.run_id` |
| `upstream` | VARCHAR | Tabela de origem (ex.: `bronze.consumo_api_raw`) |
| `downstream` | VARCHAR | Tabela de destino (ex.: `silver.consumo_api_hourly`) |
| `recorded_at` | TIMESTAMP WITH TIME ZONE | Timestamp de registo |

**Grafo de lineage materializado por execução:**

```
bronze.consumo_api_raw  ──►  silver.consumo_api_hourly  ──►  gold.dp_energy_market_api_hourly
bronze.preco_api_raw    ──►  silver.preco_api_hourly    ──►  gold.dp_energy_market_api_hourly
                                                         ──►  gold.feat_load_forecasting_api_hourly
```

**DDL:** `05_audit/sql/audit_ddl.sql` — aplicado automaticamente no arranque (CREATE IF NOT EXISTS, idempotente).

### Logging estruturado com run_id

Cada linha de log é prefixada com os primeiros 8 caracteres do UUID da execução:

```
10:42:01  INFO    [3f2a1b9c] Pipeline iniciada — run_id=3f2a1b9c-...  período=2024-01-01→2024-12-31  fonte=energycharts
10:42:05  INFO    [3f2a1b9c] FASE 1 - Bronze fetch (ENERGYCHARTS) (2024-01-01 -> 2024-12-31)
10:52:11  INFO    [3f2a1b9c] Bronze ingerido: 17544 linhas totais (consumo + preço)
10:52:11  INFO    [3f2a1b9c] Pipeline SUCCESS em 541s  (bronze=17544  silver=8772  gold=8748)
```

**Consulta de auditoria — últimas execuções:**

```sql
SELECT run_id, status, duration_seconds, rows_gold, param_start_date, param_end_date, start_ts
FROM iceberg.audit.pipeline_runs
ORDER BY start_ts DESC
LIMIT 10;
```

**Consulta de lineage de uma execução específica:**

```sql
SELECT upstream, downstream
FROM iceberg.audit.dataset_lineage
WHERE run_id = '<uuid-completo>'
ORDER BY upstream;
```

---

## Resiliência e SLA

### Retry com backoff exponencial

Todas as chamadas a workflows Flyte (`pyflyte run`) executam com **3 tentativas automáticas** e backoff exponencial entre tentativas (2s → 4s). Erros transitórios de rede ou timeout da API não abortam o pipeline.

```
Tentativa 1  ──✗──►  aguarda 2s
Tentativa 2  ──✗──►  aguarda 4s
Tentativa 3  ──✗──►  FALHA propagada
```

### SLA operacional definido

| SLA             | Valor                               | Comportamento                        |
|-----------------|-------------------------------------|--------------------------------------|
| **Freshness**   | Máx. 7 dias de atraso no `end_date` | WARNING em log se ultrapassado       |
| **Runtime**     | Máx. 45 minutos por execução        | WARNING em log se ultrapassado       |

Ambos os SLA geram aviso no log e são persistidos na tabela `pipeline_runs` para análise histórica em Grafana.

### Quality gate — modelo de severidade

| Status | Comportamento                                                        |
|--------|----------------------------------------------------------------------|
| `PASS` | Log informativo; pipeline continua                                   |
| `WARN` | Log de aviso; pipeline continua (ex.: gaps DST, precos negativos)    |
| `FAIL` | Pipeline abortada; `error_message` persistido na tabela de auditoria |

---

## Manutenção Iceberg

### Compaction automática (small-files problem)

Cada execução incremental gera ficheiros Parquet pequenos. O orquestrador executa automaticamente `ALTER TABLE ... EXECUTE optimize` nas tabelas Gold após cada run bem-sucedido.

```sql
-- Executado automaticamente pelo orquestrador (pós-Gold, pré-validação)
ALTER TABLE iceberg.gold.dp_energy_market_api_hourly EXECUTE optimize;
ALTER TABLE iceberg.gold.feat_load_forecasting_api_hourly EXECUTE optimize;
```

**Resultado:** redução de ficheiros fragmentados, melhoria de performance em queries analíticas e ML, sem necessidade de manutenção manual.

### Política de retenção de dados

| Camada | Retenção recomendada | Justificação |
|--------|---------------------|--------------|
| Bronze (`_api_raw`) | 90 dias | Dados raw reprocessáveis a partir da API |
| Silver (`_api_hourly`) | 2 anos | Dados limpos; custo de reprocessamento moderado |
| Gold (`dp_*`, `feat_*`) | Histórico completo | Window functions requerem série contínua |
| Audit (`pipeline_runs`) | Histórico completo | Rastreabilidade regulatória e SLA |

---

## Como executar

### Sem token (Energy-Charts / Fraunhofer ISE) — fonte por defeito

Sem necessidade de registo. O comportamento padrão (sem flags) corre os **últimos 12 meses** via Energy-Charts.

```powershell
# Padrão: últimos 12 meses, fonte Energy-Charts (sem token)
python run_streaming_pipeline.py --skip-docker

# Limpar tabelas e recarregar tudo desde o início (~2022 até hoje)
python run_streaming_pipeline.py --skip-docker --clean --full

# Período específico
python run_streaming_pipeline.py --skip-docker --start 2024-01-01 --end 2024-12-31

# Apenas hoje, sem quality gates
python run_streaming_pipeline.py --skip-docker --today --no-quality

# Sem recriar tabelas (DDL já aplicado)
python run_streaming_pipeline.py --skip-docker --skip-ddl
```

### Com token ENTSO-E (acesso direto à plataforma)

Obter token gratuito: email para `transparency@entsoe.eu`, assunto "RESTful API access" (~3 dias úteis).

```powershell
# Definir token
$env:ENTSOE_TOKEN = "<o-teu-token>"

# Padrão: últimos 12 meses, fonte ENTSO-E direta
python run_streaming_pipeline.py --skip-docker --source entsoe

# Limpar e recarregar histórico completo via ENTSO-E
python run_streaming_pipeline.py --skip-docker --source entsoe --clean --full

# Período específico via ENTSO-E
python run_streaming_pipeline.py --skip-docker --source entsoe --start 2024-01-01 --end 2024-12-31
```

---

## Scripts standalone (debug / ingestão manual)

Os dois scripts abaixo permitem ingerir dados diretamente via ENTSO-E sem correr o pipeline completo. Úteis para verificar a API, testar o token ou repopular um intervalo pontual.

| Script | Descrição |
|--------|-----------|
| `01_bronze/scripts/python/fetch_consumo_entsoe.py` | Carga eléctrica nacional PT (Actual Total Load) |
| `01_bronze/scripts/python/fetch_preco_entsoe.py` | Preços day-ahead PT + ES (Day-Ahead Prices) |

```powershell
$env:ENTSOE_TOKEN = "<o-teu-token>"

# Consumo — últimos 7 dias
python 01_bronze/scripts/python/fetch_consumo_entsoe.py --days 7

# Preços — intervalo específico
python 01_bronze/scripts/python/fetch_preco_entsoe.py --start 2024-01-01 --end 2024-01-31
```

Dependências: `entsoe-py`, `pandas` (instalar via `pip install entsoe-py pandas`).

---

## Documentação técnica

| Ficheiro | Conteúdo |
| --- | --- |
| [`.env.example`](.env.example) | **Template de variáveis de ambiente** — copia para `.env` e preenche o `ENTSOE_TOKEN` |
| [`../docs/product.yaml`](../docs/product.yaml) | **Data Product** — entrega, produção, lineage, observabilidade, governança |
| [`../docs/contract.yaml`](../docs/contract.yaml) | **Data Contract** — schema formal, semântica, qualidade, SLAs/SLOs, exemplos SQL |
