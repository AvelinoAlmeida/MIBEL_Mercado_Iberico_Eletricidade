# DP-02 Consumo vs Preço — Visão Geral

## 1. Contexto de Negócio

A organização atua no setor energético em Portugal e pretende monitorizar a evolução do consumo elétrico nacional e a sua relação com o preço horário do mercado MIBEL (Mercado Ibérico de Energia). O objetivo é suportar análise operacional, reporting e previsão de consumo.

**Pergunta de negócio central:** Em que intervalo horário diário é mais vantajoso vender energia renovável à rede?

---

## 2. Pipeline — Streaming_Data

O DP-02 ingere dados de consumo e preço via API, com duas fontes intercambiáveis:

| Fonte | Flag | Autenticação | Default |
|-------|------|--------------|---------|
| **Energy-Charts API** (Fraunhofer ISE — redistribui ENTSO-E) | `--source energycharts` | Nenhuma | **Sim** |
| ENTSO-E Transparency Platform (dados primários) | `--source entsoe` | Token gratuito | Não |

As tabelas Bronze de destino são idênticas em ambos os casos — Silver, Gold e quality checks não mudam conforme a fonte.

> A pasta `Static_Data/` mantém um pipeline legado sobre CSVs históricos (REN + OMIE). A documentação em `00_Docs/` refere-se **exclusivamente** ao pipeline Streaming_Data.

### Arquitetura geral

```
Energy-Charts API (default, sem token)
  ├── total_power?country=pt  →  carga eléctrica nacional PT
  └── price?bzn=PT / price?bzn=ES  →  preços day-ahead MIBEL
  │
  │   (alternativa: ENTSO-E API com ENTSOE_TOKEN)
  │
  ▼
   BRONZE  — ingestão raw, 1 linha por hora
        │
        ▼  deduplicação · filtragem nulos · unidade MW→MWh
   SILVER  — dados limpos e normalizados
        │
        ▼  join · features calendário · lags · rolling averages
     GOLD  — produto analítico + feature table ML
        │
        ▼
   Grafana Dashboard  +  MLflow (GBR load forecasting)
```

**Formato de armazenamento:** Apache Parquet em MinIO (`s3a://warehouse/`), gerido por Apache Iceberg via Trino.  
**Orquestração:** workflows Flyte locais em `workflows/`, executados via `pyflyte run`.

---

## 3. Data Products entregues

### DP-1: Produto analítico — `dp_energy_market_api_hourly`

| Atributo | Valor |
|----------|-------|
| **Tabela Iceberg** | `iceberg.gold.dp_energy_market_api_hourly` |
| **Grão** | 1 linha por hora UTC |
| **Chave** | `ts_utc` — TIMESTAMP(6) WITH TIME ZONE |
| **Cobertura** | 2022-01-01 → presente (incremental) |
| **Versão** | v1 (`product_version=v1`, `schema_version=1`) |
| **Consumidores** | Dashboard Grafana, analistas (query Trino), base para feat table ML |

**Métricas principais:**
- `consumo_total` — carga eléctrica horária nacional Portugal (MWh)
- `market_price_pt` — preço day-ahead Portugal (€/MWh)

**Features derivadas:** `hora`, `dia_semana`, `is_weekend`, lags 1h e 24h, médias móveis 24h.

### DP-2: Feature table ML — `feat_load_forecasting_api_hourly`

| Atributo | Valor |
|----------|-------|
| **Tabela Iceberg** | `iceberg.gold.feat_load_forecasting_api_hourly` |
| **Grão** | 1 linha por hora UTC (primeiras 24h e última excluídas) |
| **Target** | `consumo_next_hour` — consumo da hora seguinte (LEAD 1h) |
| **Versão** | v1 (`feature_schema_version=1`) |
| **Consumidor** | `03_ml_pipeline/preco_consumo_mlflow_flow.py` (GBR, R²=0.989, MAPE=1.30%) |

---

## 4. Convenção temporal

UTC é o tempo canónico em todo o pipeline:
- **Bronze:** timestamps UTC preservados tal como retornados pela API ENTSO-E
- **Silver:** `DATE_TRUNC('hour', ts_utc)` garante alinhamento horário exacto
- **Gold:** operações de window function ordenam por `ts_utc` UTC

---

## 5. Estratégia de schema evolution

| Tipo | Definição | Ação |
|------|-----------|------|
| **Minor** | Adicionar coluna nullable | Incrementar `schema_version`; sem impacto nos consumidores |
| **Breaking** | Remover coluna, alterar tipo, alterar grão | Criar `_v2`, marcar v1 `deprecated=true`; coexistência mínima 30 dias |

Propriedades Iceberg usadas: `schema_version`, `product_version`, `feature_schema_version`, `deprecated`.

---

## 6. Ficheiros desta pasta

| Ficheiro | Tema |
|----------|------|
| `01_overview.md` | Este ficheiro — visão geral e contexto |
| `02_fontes_dados.md` | Fontes de dados (Energy-Charts / ENTSO-E), endpoints, autenticação |
| `03_lakehouse_design.md` | Arquitetura Iceberg, particionamento, naming |
| `04_schemas.md` | Schemas Bronze, Silver, Gold (tabelas `_api`) |
| `05_transformacoes.md` | Transformações por camada |
| `06_qualidade.md` | Quality gates (Bronze, Silver, Gold) |
| `07_ml_pipeline.md` | Pipeline ML, GBR, MLflow |
| `contract.yaml` | Contrato de dados formal |
| `product.yaml` | Especificação do data product |
