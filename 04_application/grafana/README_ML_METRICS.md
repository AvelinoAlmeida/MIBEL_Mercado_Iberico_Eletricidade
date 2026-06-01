# Como usar o output do treino Flyte/MLflow no Grafana (forma dinâmica)

Boa pergunta: para ser **dinâmico no teu projeto**, não deves fazer `INSERT` manual.
A abordagem certa é automatizar o fluxo:

**Flyte task de treino -> task de persistência de métricas -> tabela Iceberg -> Grafana (Trino).**

---

## Porque o output não aparece diretamente no dashboard?

O dashboard `producao_consumo_overview.json` usa datasource `trino-iceberg`.
Ou seja, ele lê SQL no Trino/Iceberg, e **não lê diretamente** o JSON retornado por uma execução Flyte.

Exemplo de output da tua task:

```json
{
  "experiment": "producao-consumo-defice",
  "registered_model": "producao_consumo_defice_classifier",
  "metrics": {
    "accuracy": 0.40733309011309743,
    "precision": 0.39866628008118293,
    "recall": 0.5392156862745098,
    "f1": 0.458409734955826,
    "roc_auc": 0.3369637268277025
  }
}
```

---

## Arquitetura dinâmica recomendada

1. O treino corre e devolve `result` (dict com `experiment`, `registered_model`, `metrics`).
2. Uma segunda task Flyte recebe esse `result` e grava uma linha numa tabela de métricas.
3. O Grafana consulta sempre o último valor (Stat) ou histórico (Time series).

Assim, cada novo treino atualiza automaticamente o dashboard.

---

## 1) Criar tabela de métricas (uma vez)

```sql
CREATE TABLE IF NOT EXISTS iceberg.gold.ml_training_metrics (
  event_ts TIMESTAMP(6),
  workflow_name VARCHAR,
  run_id VARCHAR,
  experiment VARCHAR,
  registered_model VARCHAR,
  accuracy DOUBLE,
  precision DOUBLE,
  recall DOUBLE,
  f1 DOUBLE,
  roc_auc DOUBLE
);
```

> Dica: `run_id` ajuda a rastrear cada execução no MLflow/Flyte.

---

## 2) Persistência automática dentro do workflow Flyte

No teu `producao_consumo_mlflow_flow.py`, cria uma task para persistir métricas.

```python
from flytekit import task, workflow
import json
import trino

@task
def persist_training_metrics(result_json: str, workflow_name: str) -> None:
    result = json.loads(result_json)
    metrics = result.get("metrics", {})

    conn = trino.dbapi.connect(
        host="localhost",      # ajustar ao teu ambiente
        port=8081,              # ajustar ao teu ambiente
        user="etl",
        catalog="iceberg",
        schema="gold",
    )

    sql = f"""
    INSERT INTO iceberg.gold.ml_training_metrics VALUES (
      CURRENT_TIMESTAMP,
      '{workflow_name}',
      NULL,
      '{result.get("experiment", "")}',
      '{result.get("registered_model", "")}',
      {float(metrics.get("accuracy", 0.0))},
      {float(metrics.get("precision", 0.0))},
      {float(metrics.get("recall", 0.0))},
      {float(metrics.get("f1", 0.0))},
      {float(metrics.get("roc_auc", 0.0))}
    )
    """

    with conn.cursor() as cur:
        cur.execute(sql)

@workflow
def producao_consumo_training_wf() -> str:
    result_json = train_producao_consumo_model(test_ratio=0.2, random_state=42)
    persist_training_metrics(
        result_json=result_json,
        workflow_name="producao_consumo_mlflow_flow.train_producao_consumo_model",
    )
    return result_json
```

### Importante (boas práticas)

- Evita SQL com f-string em produção; prefere parâmetros/prepared statements se o driver suportar.
- Guarda também `run_id` (MLflow) para correlação entre dashboard e tracking.
- Em caso de falha de escrita, decide se queres:
  - falhar workflow (consistência forte), ou
  - logar warning e seguir (resiliência operacional).

---

## 3) Queries dinâmicas no Grafana

### Stat: último `accuracy`

```sql
SELECT accuracy AS value
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1;
```

### Stat: último `f1`

```sql
SELECT f1 AS value
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts DESC
LIMIT 1;
```

### Time series: evolução do `roc_auc`

```sql
SELECT event_ts AS time, roc_auc AS value
FROM iceberg.gold.ml_training_metrics
WHERE registered_model = 'producao_consumo_defice_classifier'
ORDER BY event_ts;
```

---

## 4) Opção mais robusta para ambientes reais

Se quiseres desacoplar completamente treino e dashboard:

- treino publica evento (Kafka/queue/webhook),
- consumer grava em Iceberg,
- Grafana lê apenas Iceberg.

Vantagem: não bloqueias treino por problema temporário no destino analítico.

---

## Resumo direto

Para ficar dinâmico no teu projeto, implementa uma task Flyte de persistência no próprio workflow.
A partir daí, qualquer novo treino atualiza automaticamente os painéis do Grafana via Trino/Iceberg.
