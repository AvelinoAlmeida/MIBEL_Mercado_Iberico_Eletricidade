# Pipeline ML — Previsão de Consumo Horário

## 1. Contexto

A feature table `iceberg.gold.feat_load_forecasting_api_hourly` contém dados históricos horários (desde 2022) prontos para treino supervisionado. O pipeline ML treina um modelo de previsão de consumo e regista tudo em MLflow.

**Script:** `03_ml_pipeline/preco_consumo_mlflow_flow.py`

---

## 2. Problema

| Atributo | Valor |
|----------|-------|
| **Tipo** | Regressão supervisionada |
| **Target** | `consumo_next_hour` — consumo nacional na hora seguinte (MWh) |
| **Pergunta** | Dado o consumo atual, o preço de mercado e o histórico recente, qual será o consumo na próxima hora? |
| **Fonte** | `iceberg.gold.feat_load_forecasting_api_hourly` |

---

## 3. Feature Table

**Acesso:** Trino DBAPI — `localhost:8080` (local) ou `host.docker.internal:8080` (container)

### Features (10)

| Feature | Tipo | Categoria | Descrição |
|---------|------|-----------|-----------|
| `hora` | INTEGER | Temporal | Hora do dia (0–23) |
| `dia_semana` | INTEGER | Temporal | Dia da semana (0=Segunda, 6=Domingo) |
| `is_weekend` | BOOLEAN | Temporal | TRUE para Sábado e Domingo |
| `consumo_total` | DOUBLE | Métrica | Consumo atual (MWh) |
| `market_price_pt` | DOUBLE | Métrica | Preço day-ahead atual (€/MWh) |
| `consumo_lag_1h` | DOUBLE | Lag | Consumo 1h atrás |
| `consumo_lag_24h` | DOUBLE | Lag | Consumo 24h atrás |
| `price_lag_1h` | DOUBLE | Lag | Preço 1h atrás |
| `rolling_avg_consumo_24h` | DOUBLE | Rolling | Média móvel consumo 24h |
| `rolling_avg_price_24h` | DOUBLE | Rolling | Média móvel preço 24h |

### Target (1)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `consumo_next_hour` | DOUBLE | Consumo da hora seguinte (MWh) — LEAD 1h |

---

## 4. Split temporal

O dataset é uma série temporal — o split respeita a ordem temporal para evitar data leakage.

| Conjunto | Critério | Proporção |
|----------|---------|-----------|
| Treino | Dados mais antigos | 80% |
| Teste | Dados mais recentes | 20% |

```python
df_sorted  = df.sort_values('ts_utc')
split_idx  = int(len(df_sorted) * 0.8)
train      = df_sorted.iloc[:split_idx]
test       = df_sorted.iloc[split_idx:]
# Sem shuffle — baralhar uma série temporal introduz leakage futuro → passado
```

---

## 5. Modelo

**GradientBoostingRegressor** (scikit-learn) — melhor desempenho em séries com padrões não-lineares de carga horária.

### Métricas alcançadas (run de referência)

| Métrica | Valor | Descrição |
|---------|-------|-----------|
| R² | **0.989** | 98.9% da variância explicada |
| MAPE | **1.30%** | Erro relativo médio de 1.30% |

### Modelos candidatos para comparação

| Modelo | Framework | Uso |
|--------|-----------|-----|
| GradientBoostingRegressor | scikit-learn | **Modelo principal** |
| RandomForestRegressor | scikit-learn | Baseline robusto |
| LinearRegression | scikit-learn | Baseline simples |

---

## 6. MLflow Tracking

**Experiment:** `consumo-preco-load-forecast`  
**Registered model:** `consumo_preco_load_forecaster`  
**Tracking URI:** `http://localhost:15000`

### O que registar por run

```python
mlflow.set_experiment("consumo-preco-load-forecast")
with mlflow.start_run():
    # Parâmetros do modelo
    mlflow.log_param("model_type",             "GradientBoostingRegressor")
    mlflow.log_param("feature_schema_version", 1)
    mlflow.log_param("train_start",            str(train['ts_utc'].min()))
    mlflow.log_param("train_end",              str(train['ts_utc'].max()))
    mlflow.log_param("test_start",             str(test['ts_utc'].min()))
    mlflow.log_param("test_end",               str(test['ts_utc'].max()))
    mlflow.log_param("n_train_rows",           len(train))
    mlflow.log_param("n_test_rows",            len(test))

    # Métricas de avaliação
    mlflow.log_metric("mae",  mae)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2",   r2)
    mlflow.log_metric("mape", mape)

    # Modelo + importâncias
    mlflow.sklearn.log_model(model, "model",
        registered_model_name="consumo_preco_load_forecaster")
    mlflow.log_dict(feature_importances, "feature_importances.json")
```

O registo de `train_start`/`train_end`/`feature_schema_version` garante rastreabilidade completa: é possível saber **que dados foram usados em cada versão do modelo**.

---

## 7. Diagrama de rastreabilidade

```
iceberg.gold.feat_load_forecasting_api_hourly
        │
        ├─[preco_consumo_mlflow_flow.py]─► MLflow Run
        │                                    ├─ parâmetros (dados, feature_schema_version)
        │                                    ├─ métricas (MAE, RMSE, R², MAPE)
        │                                    └─ modelo: consumo_preco_load_forecaster
        │
        └─[Grafana]─► Painel "Feature Table ML"
                        ├─ COUNT(*) total de exemplos
                        ├─ AVG(consumo_next_hour) target médio
                        └─ nulos no target
```

---

## 8. Execução

```powershell
# Activar venv com dependências
.venv\Scripts\activate

# Treinar e registar no MLflow
python 03_ml_pipeline/preco_consumo_mlflow_flow.py
```

**Pré-requisitos:**
- Stack Docker em execução (`docker compose up -d`)
- `gold.feat_load_forecasting_api_hourly` populada (pipeline Streaming executado)
- MLflow acessível em `http://localhost:15000`
- Dependências: `scikit-learn`, `pandas`, `mlflow`, `trino`

**Verificação pós-treino em `http://localhost:15000`:**
1. Experiment `consumo-preco-load-forecast` criado
2. Run com `train_start`, `train_end` e `feature_schema_version` registados
3. Métricas R² ≈ 0.989, MAPE ≈ 1.30% visíveis
4. Modelo registado em `Models` → `consumo_preco_load_forecaster`
5. Artefacto `feature_importances.json` disponível
