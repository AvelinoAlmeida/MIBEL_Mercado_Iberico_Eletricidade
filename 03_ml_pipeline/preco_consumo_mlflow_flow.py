"""Flow de treino para preço vs consumo (load forecasting) com tracking no MLflow.

Execução local:
    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install --upgrade pip setuptools wheel
    python -m pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3
    python 03_ml_pipeline/preco_consumo_mlflow_flow.py

Execução remota no Flyte:
    pyflyte run --remote -p flytesnacks -d development \
      03_ml_pipeline/preco_consumo_mlflow_flow.py preco_consumo_training_wf
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Tuple

try:
    from flytekit import ImageSpec, Resources, task, workflow
except ModuleNotFoundError:  # execução local sem Flyte instalado
    class ImageSpec:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class Resources:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    def task(*args, **kwargs):  # type: ignore[override]
        def decorator(func):
            return func

        if args and callable(args[0]) and len(args) == 1 and not kwargs:
            return args[0]
        return decorator

    def workflow(func=None, **kwargs):  # type: ignore[override]
        if func is not None and callable(func):
            return func

        def decorator(f):
            return f

        return decorator

image_spec = ImageSpec(
    name="ml-preco-consumo-image",
    registry="localhost:30000",
    packages=[
        "pandas==2.2.3",
        "scikit-learn==1.6.1",
        "trino==0.336.0",
        "mlflow==3.10.1",
        "boto3",
    ],
)

TRINO_HOST = os.getenv("TRINO_HOST", "host.docker.internal")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "tead")
TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "http")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "gold")
TRINO_TABLE = os.getenv("TRINO_TABLE", "feat_load_forecasting_hourly")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://host.docker.internal:15000")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "preco-consumo-forecasting")

_S3_ENDPOINT_DEFAULT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://host.docker.internal:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def _resolved_host(host: str, port: int = 80) -> str:
    try:
        socket.getaddrinfo(host, None)
    except socket.gaierror:
        if host == "host.docker.internal":
            return "localhost"
        raise
    # DNS resolved but verify TCP reachability; on Windows, host.docker.internal
    # resolves via hosts file but its IP is Docker Desktop's VM gateway — not
    # reachable from a native Python process (only from inside containers).
    try:
        with socket.create_connection((host, port), timeout=3):
            pass
        return host
    except OSError:
        if host == "host.docker.internal":
            return "localhost"
        raise


def _get_trino_connection():
    import trino

    resolved_trino_host = _resolved_host(TRINO_HOST, TRINO_PORT)
    return trino.dbapi.connect(
        host=resolved_trino_host,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme=TRINO_HTTP_SCHEME,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        session_properties={
            "query_max_run_time": "10m",
            "query_max_execution_time": "8m",
        },
    )


def _load_gold_table():
    import pandas as pd

    # A tabela feat_load_forecasting_hourly já contém todas as features de lag
    # e a variável target consumo_next_hour, produzidas na camada gold.
    sql = f"""
        SELECT
            ts_utc,
            consumo_total,
            market_price_pt,
            hora,
            dia_semana,
            is_weekend,
            consumo_lag_1h,
            consumo_lag_24h,
            price_lag_1h,
            rolling_avg_consumo_24h,
            rolling_avg_price_24h,
            consumo_next_hour
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
        WHERE ts_utc IS NOT NULL
          AND consumo_next_hour IS NOT NULL
        ORDER BY ts_utc
    """

    conn = _get_trino_connection()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df.empty:
        raise ValueError("Tabela gold está vazia; não há dados para treino.")

    df.columns = [c.strip().lower() for c in df.columns]
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df = df.sort_values("ts_utc").reset_index(drop=True)

    required = {
        "consumo_total",
        "market_price_pt",
        "consumo_lag_1h",
        "consumo_lag_24h",
        "price_lag_1h",
        "rolling_avg_consumo_24h",
        "rolling_avg_price_24h",
        "consumo_next_hour",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltam colunas obrigatórias na tabela gold: {sorted(missing)}")

    return df


def _build_features(df):
    import pandas as pd

    data = df.copy()

    numeric_cols = [
        "consumo_total",
        "market_price_pt",
        "consumo_lag_1h",
        "consumo_lag_24h",
        "price_lag_1h",
        "rolling_avg_consumo_24h",
        "rolling_avg_price_24h",
    ]
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    feature_cols: List[str] = [
        "consumo_total",
        "market_price_pt",
        "hora",
        "dia_semana",
        "is_weekend",
        "consumo_lag_1h",
        "consumo_lag_24h",
        "price_lag_1h",
        "rolling_avg_consumo_24h",
        "rolling_avg_price_24h",
    ]

    target_col = "consumo_next_hour"
    model_df = data[["ts_utc", target_col] + feature_cols].dropna().copy()

    if model_df.empty:
        raise ValueError("Sem dados suficientes após limpeza de nulos.")

    model_df[target_col] = pd.to_numeric(model_df[target_col], errors="coerce")
    model_df = model_df.dropna(subset=[target_col])

    X = model_df[feature_cols]
    y = model_df[target_col]
    ts = model_df["ts_utc"]
    return X, y, ts, feature_cols


@task(container_image=image_spec)
def train_preco_consumo_model(test_ratio: float = 0.2, random_state: int = 42) -> str:
    try:
        import mlflow
        import mlflow.sklearn
    except ModuleNotFoundError as exc:
        if exc.name == "mlflow":
            raise ModuleNotFoundError(
                "Dependência ausente: mlflow. Cria/ativa um venv e instala com "
                "`python -m pip install mlflow==3.10.1`."
            ) from exc
        raise

    import numpy as np
    import pandas as pd
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import GradientBoostingRegressor

    raw_df = _load_gold_table()
    X, y, ts, feature_cols = _build_features(raw_df)

    # Split temporal (sem embaralhar) para respeitar série temporal.
    split_index = int(len(X) * (1 - test_ratio))
    if split_index <= 0 or split_index >= len(X):
        raise ValueError("test_ratio gerou split inválido.")

    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
    ts_train, ts_test = ts.iloc[:split_index], ts.iloc[split_index:]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                feature_cols,
            )
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "regressor",
                GradientBoostingRegressor(
                    n_estimators=300,
                    max_depth=5,
                    learning_rate=0.05,
                    subsample=0.8,
                    min_samples_leaf=5,
                    random_state=random_state,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    mae = float(mean_absolute_error(y_test, preds))
    rmse = float(np.sqrt(mean_squared_error(y_test, preds)))
    r2 = float(r2_score(y_test, preds))

    # Mean Absolute Percentage Error (evita divisão por zero)
    mask = y_test != 0
    mape = float(np.mean(np.abs((y_test[mask] - preds[mask]) / y_test[mask])) * 100) if mask.any() else float("nan")

    feature_importances = dict(
        zip(feature_cols, model.named_steps["regressor"].feature_importances_.tolist())
    )

    from urllib.parse import urlparse, urlunparse

    def _resolve_url(url: str) -> str:
        p = urlparse(url)
        host = _resolved_host(p.hostname, p.port or 80)
        return urlunparse(p._replace(netloc=f"{host}:{p.port or 80}"))

    mlflow.set_tracking_uri(_resolve_url(MLFLOW_TRACKING_URI))
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = _resolve_url(_S3_ENDPOINT_DEFAULT)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="gbr-consumo-next-hour"):
        mlflow.set_tags(
            {
                "domain": "energia",
                "dataset": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
                "target": "consumo_next_hour",
                "model_type": "GradientBoostingRegressor",
                "task": "regression",
            }
        )

        mlflow.log_params(
            {
                "test_ratio": test_ratio,
                "random_state": random_state,
                "n_estimators": 300,
                "max_depth": 5,
                "learning_rate": 0.05,
                "subsample": 0.8,
                "min_samples_leaf": 5,
                "n_train": len(X_train),
                "n_test": len(X_test),
                "train_start": str(ts_train.min()),
                "train_end": str(ts_train.max()),
                "test_start": str(ts_test.min()),
                "test_end": str(ts_test.max()),
            }
        )

        mlflow.log_metrics(
            {
                "mae": mae,
                "rmse": rmse,
                "r2": r2,
                "mape": mape,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            importances_path = os.path.join(tmpdir, "feature_importances.json")
            features_path = os.path.join(tmpdir, "feature_columns.json")

            with open(importances_path, "w", encoding="utf-8") as f:
                json.dump(feature_importances, f, indent=2)

            with open(features_path, "w", encoding="utf-8") as f:
                json.dump(feature_cols, f, indent=2)

            mlflow.log_artifact(importances_path, artifact_path="evaluation")
            mlflow.log_artifact(features_path, artifact_path="metadata")

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name="preco_consumo_load_forecaster",
        )

        summary = {
            "experiment": MLFLOW_EXPERIMENT,
            "registered_model": "preco_consumo_load_forecaster",
            "metrics": {
                "mae": mae,
                "rmse": rmse,
                "r2": r2,
                "mape": mape,
            },
        }
        return json.dumps(summary, ensure_ascii=False)


@workflow
def preco_consumo_training_wf(test_ratio: float = 0.2, random_state: int = 42) -> str:
    return train_preco_consumo_model(test_ratio=test_ratio, random_state=random_state)


if __name__ == "__main__":
    result = train_preco_consumo_model(test_ratio=0.2, random_state=42)
    print(result)
