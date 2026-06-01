"""ML — GradientBoostingRegressor para previsão de consumo (dados streaming ENTSO-E).

Lê de iceberg.gold.feat_load_forecasting_api_hourly.
Escreve métricas em iceberg.gold.ml_metrics_streaming e
iceberg.gold.ml_feature_importance_streaming (para Grafana).
Regista run no MLflow.

Execução:
    python 03_ml_pipeline/preco_consumo_streaming_mlflow_flow.py
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "tead")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:15000")
MLFLOW_EXPERIMENT  = os.getenv("MLFLOW_EXPERIMENT", "preco-consumo-streaming-forecasting")

os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def _get_conn():
    import trino
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, http_scheme="http",
        session_properties={"query_max_run_time": "15m"},
    )


def _load_data():
    import pandas as pd

    sql = """
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
        FROM iceberg.gold.feat_load_forecasting_api_hourly
        WHERE ts_utc IS NOT NULL
          AND consumo_next_hour IS NOT NULL
        ORDER BY ts_utc
    """
    max_retries, delay = 10, 15
    for attempt in range(1, max_retries + 1):
        conn = _get_conn()
        try:
            df = pd.read_sql(sql, conn)
            conn.close()
            break
        except Exception as exc:
            conn.close()
            if "SERVER_STARTING_UP" in str(exc) and attempt < max_retries:
                print(f"Trino ainda a inicializar, aguardando {delay}s... (tentativa {attempt}/{max_retries})")
                time.sleep(delay)
            else:
                raise

    if df.empty:
        raise ValueError("feat_load_forecasting_api_hourly está vazia.")

    df.columns = [c.strip().lower() for c in df.columns]
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df = df.sort_values("ts_utc").reset_index(drop=True)
    return df


FEATURE_COLS = [
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
TARGET = "consumo_next_hour"


def _write_metrics_to_trino(run_ts, metrics, params, ts_train, ts_test):
    conn = _get_conn()
    cur = conn.cursor()

    # remove previous run rows (keep only latest)
    try:
        cur.execute("DELETE FROM iceberg.gold.ml_metrics_streaming WHERE run_ts IS NOT NULL")
        cur.fetchall()
    except Exception:
        pass

    sql = f"""
        INSERT INTO iceberg.gold.ml_metrics_streaming VALUES (
            TIMESTAMP '{run_ts.strftime('%Y-%m-%d %H:%M:%S.000000')} UTC',
            'GradientBoostingRegressor',
            BIGINT '{params['n_train']}',
            BIGINT '{params['n_test']}',
            TIMESTAMP '{ts_train[0].strftime('%Y-%m-%d %H:%M:%S.000000')} UTC',
            TIMESTAMP '{ts_train[1].strftime('%Y-%m-%d %H:%M:%S.000000')} UTC',
            TIMESTAMP '{ts_test[0].strftime('%Y-%m-%d %H:%M:%S.000000')} UTC',
            TIMESTAMP '{ts_test[1].strftime('%Y-%m-%d %H:%M:%S.000000')} UTC',
            DOUBLE '{metrics['mae']:.6f}',
            DOUBLE '{metrics['rmse']:.6f}',
            DOUBLE '{metrics['r2']:.6f}',
            DOUBLE '{metrics['mape']:.6f}',
            INTEGER '{params['n_estimators']}',
            INTEGER '{params['max_depth']}',
            DOUBLE '{params['learning_rate']}'
        )
    """
    cur.execute(sql)
    cur.fetchall()
    conn.close()


def _write_importances_to_trino(run_ts, importances):
    conn = _get_conn()
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM iceberg.gold.ml_feature_importance_streaming WHERE run_ts IS NOT NULL")
        cur.fetchall()
    except Exception:
        pass

    sorted_imp = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    for rank, (feat, imp) in enumerate(sorted_imp, 1):
        sql = f"""
            INSERT INTO iceberg.gold.ml_feature_importance_streaming VALUES (
                TIMESTAMP '{run_ts.strftime('%Y-%m-%d %H:%M:%S.000000')} UTC',
                '{feat}',
                DOUBLE '{imp:.8f}',
                INTEGER '{rank}'
            )
        """
        cur.execute(sql)
        cur.fetchall()

    conn.close()


def train():
    import numpy as np
    import pandas as pd
    import mlflow
    import mlflow.sklearn
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    print("Carregando dados de iceberg.gold.feat_load_forecasting_api_hourly...")
    df = _load_data()
    print(f"  {len(df)} linhas | {df['ts_utc'].min()} ate {df['ts_utc'].max()}")

    numeric_cols = [c for c in FEATURE_COLS if c not in ("hora", "dia_semana", "is_weekend")]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    model_df = df[["ts_utc", TARGET] + FEATURE_COLS].dropna().copy()
    model_df[TARGET] = pd.to_numeric(model_df[TARGET], errors="coerce")
    model_df = model_df.dropna(subset=[TARGET])
    print(f"  {len(model_df)} linhas após limpeza de nulos")

    split = int(len(model_df) * 0.8)
    X_train = model_df[FEATURE_COLS].iloc[:split]
    X_test  = model_df[FEATURE_COLS].iloc[split:]
    y_train = model_df[TARGET].iloc[:split]
    y_test  = model_df[TARGET].iloc[split:]
    ts_all  = model_df["ts_utc"]

    preprocessor = ColumnTransformer([
        ("num", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler",  StandardScaler()),
        ]), FEATURE_COLS)
    ])

    gbr_params = dict(n_estimators=300, max_depth=5, learning_rate=0.05,
                      subsample=0.8, min_samples_leaf=5, random_state=42)

    model = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor",    GradientBoostingRegressor(**gbr_params)),
    ])

    print("Treinando GradientBoostingRegressor...")
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae   = float(mean_absolute_error(y_test, preds))
    rmse  = float(np.sqrt(mean_squared_error(y_test, preds)))
    r2    = float(r2_score(y_test, preds))
    mask  = y_test != 0
    mape  = float(np.mean(np.abs((y_test[mask] - preds[mask]) / y_test[mask])) * 100) if mask.any() else float("nan")

    importances = dict(zip(FEATURE_COLS, model.named_steps["regressor"].feature_importances_.tolist()))

    print(f"\n  MAE  = {mae:.2f} MWh")
    print(f"  RMSE = {rmse:.2f} MWh")
    print(f"  R²   = {r2:.4f}")
    print(f"  MAPE = {mape:.2f}%")

    # MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="gbr-streaming-consumo-next-hour"):
        mlflow.set_tags({
            "domain": "energia", "fonte": "ENTSO-E streaming",
            "dataset": "iceberg.gold.feat_load_forecasting_api_hourly",
            "target": TARGET, "model_type": "GradientBoostingRegressor",
        })
        mlflow.log_params({**gbr_params, "n_train": split, "n_test": len(X_test)})
        mlflow.log_metrics({"mae": mae, "rmse": rmse, "r2": r2, "mape": mape})

        with tempfile.TemporaryDirectory() as tmp:
            fi_path = os.path.join(tmp, "feature_importances.json")
            with open(fi_path, "w") as f:
                json.dump(importances, f, indent=2)
            mlflow.log_artifact(fi_path, artifact_path="evaluation")

        mlflow.sklearn.log_model(model, artifact_path="model",
                                 registered_model_name="preco_consumo_streaming_forecaster")

    print("\nEscrevendo métricas em iceberg.gold.ml_metrics_streaming...")
    run_ts = datetime.now(timezone.utc)
    _write_metrics_to_trino(
        run_ts,
        {"mae": mae, "rmse": rmse, "r2": r2, "mape": mape},
        {**gbr_params, "n_train": split, "n_test": len(X_test)},
        (ts_all.iloc[0].to_pydatetime(), ts_all.iloc[split - 1].to_pydatetime()),
        (ts_all.iloc[split].to_pydatetime(),  ts_all.iloc[-1].to_pydatetime()),
    )
    print("Escrevendo feature importance em iceberg.gold.ml_feature_importance_streaming...")
    _write_importances_to_trino(run_ts, importances)

    print("\nConcluido.")
    return {"mae": mae, "rmse": rmse, "r2": r2, "mape": mape}


if __name__ == "__main__":
    train()
