"""
Flyte workflow: ingestão Bronze para consumo_preco (DP-02 Static).

Fontes no MinIO warehouse/raw/:
  - consumo-total-nacional.csv        → REN, granularidade 15 min, unidade kW
  - Day-ahead Market Prices_*.csv     → OMIE PT+ES, granularidade horária (horas 1-25)

Execução:
    pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze_full
"""

from __future__ import annotations

import os
from datetime import date
from io import BytesIO

import boto3
import pandas as pd
import trino
from flytekit import task, workflow, ImageSpec

# Ignorado em modo local; o processo Python do host é usado em vez do container.
ingest_image = ImageSpec(
    name="dp02_ingest_bronze",
    registry="localhost:30000",
    packages=["trino>=0.328.0", "boto3>=1.34.0", "pandas>=2.2.0"],
)

TRINO_HOST       = os.getenv("TRINO_HOST", "host.docker.internal")
TRINO_PORT       = int(os.getenv("TRINO_PORT", "8080"))

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET       = os.getenv("RAW_BUCKET", "warehouse")

CONSUMO_KEY = os.getenv("CONSUMO_KEY", "raw/consumo-total-nacional.csv")
PRECO_KEY   = os.getenv("PRECO_KEY",   "raw/Day-ahead Market Prices_20230101_20260311.csv")

BATCH_SIZE = 5000               # Trino rejeita payloads demasiado grandes
MAX_PARTITIONS_PER_INSERT = 60  # Iceberg abre um writer por partição; acima de ~100 causa OOM


def _trino_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="bronze",
    )


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _insert_partition_batches(
    cur,
    table: str,
    columns: str,
    daily_rows: list[tuple[date | str, list[str]]],
) -> int:
    """Agrupa dias em batches, respeitando os limites MAX_PARTITIONS e BATCH_SIZE."""
    inserted = 0
    batch_rows: list[str] = []
    batch_partitions: set[str] = set()

    def flush() -> None:
        nonlocal inserted, batch_rows, batch_partitions
        if not batch_rows:
            return
        cur.execute(f"INSERT INTO {table} ({columns}) VALUES {', '.join(batch_rows)}")
        cur.fetchall()
        inserted += len(batch_rows)
        print(
            f"[insert] {table}: {len(batch_rows)} linhas, "
            f"{len(batch_partitions)} partições"
        )
        batch_rows = []
        batch_partitions = set()

    for partition, rows in daily_rows:
        partition_key = str(partition)
        would_exceed_partitions = (
            partition_key not in batch_partitions
            and len(batch_partitions) >= MAX_PARTITIONS_PER_INSERT
        )
        would_exceed_rows = len(batch_rows) + len(rows) > BATCH_SIZE
        if would_exceed_partitions or would_exceed_rows:
            flush()

        batch_partitions.add(partition_key)
        batch_rows.extend(rows)

    flush()
    return inserted


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


@task(retries=3, container_image=ingest_image)
def ingest_consumo_full() -> int:
    """Trunca Bronze e re-insere o CSV REN completo. Particionado por datahora para respeitar limites Iceberg."""
    s3 = _s3_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=CONSUMO_KEY)

    df = pd.read_csv(
        BytesIO(obj["Body"].read()),
        encoding="utf-8-sig",
        parse_dates=["datahora"],
    )
    df.columns = [c.strip().lower() for c in df.columns]
    df["datahora"] = pd.to_datetime(df["datahora"], utc=True)
    df["_date"] = df["datahora"].dt.date

    def _float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return "NULL"

    def _int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return "NULL"

    conn = _trino_conn()
    cur = conn.cursor()
    _exec(cur, "DELETE FROM iceberg.bronze.consumo_raw WHERE 1=1")

    cols = "datahora, dia, mes, ano, date_raw, time_raw, bt, mt, at, mat, total, process_date"
    daily_rows: list[tuple[date, list[str]]] = []
    for proc, df_day in df.groupby("_date"):
        rows = []
        for _, r in df_day.iterrows():
            ts = r["datahora"]
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
            rows.append(
                f"(TIMESTAMP '{ts_str}', "
                f"{_int(r.get('dia', 'NULL'))}, "
                f"{_int(r.get('mes', 'NULL'))}, "
                f"{_int(r.get('ano', 'NULL'))}, "
                f"'{r.get('date', '')}', "
                f"'{r.get('time', '')}', "
                f"{_float(r.get('bt', 'NULL'))}, "
                f"{_float(r.get('mt', 'NULL'))}, "
                f"{_float(r.get('at', 'NULL'))}, "
                f"{_float(r.get('mat', 'NULL'))}, "
                f"{_float(r.get('total', 'NULL'))}, "
                f"DATE '{proc}')"
            )
        daily_rows.append((proc, rows))
        print(f"[consumo_full] preparado {proc}: {len(rows)} registos.")

    total = _insert_partition_batches(cur, "iceberg.bronze.consumo_raw", cols, daily_rows)

    conn.close()
    print(f"[consumo_full] Total: {total} registos inseridos.")
    return total


@task(retries=3, container_image=ingest_image)
def ingest_preco_full() -> int:
    """
    Trunca Bronze e re-insere o CSV OMIE completo.
    Hora 25 (recuo DST de outono) é preservada aqui e descartada na Silver.
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=PRECO_KEY)

    df = pd.read_csv(
        BytesIO(obj["Body"].read()),
        sep=";",
        skiprows=2,
        encoding="utf-8-sig",
    )
    df.columns = [c.strip() for c in df.columns]

    col_map = {c.lower(): c for c in df.columns}
    df = df.rename(columns={
        col_map.get("date", "Date"): "date_raw",
        col_map.get("hour", "Hour"): "hour",
        col_map.get("portugal", "Portugal"): "price_portugal_raw",
        col_map.get("spain", "Spain"): "price_spain_raw",
    })

    conn = _trino_conn()
    cur = conn.cursor()
    _exec(cur, "DELETE FROM iceberg.bronze.preco_raw WHERE 1=1")

    cols = "date_raw, hour, price_portugal_raw, price_spain_raw, process_date"
    daily_rows: list[tuple[str, list[str]]] = []
    for proc, df_day in df.groupby("date_raw"):
        rows = []
        for _, r in df_day.iterrows():
            try:
                hour_val = int(r["hour"])
            except (TypeError, ValueError):
                continue

            try:
                pt = float(r["price_portugal_raw"])
            except (TypeError, ValueError):
                pt = "NULL"

            try:
                es = float(r["price_spain_raw"])
            except (TypeError, ValueError):
                es = "NULL"

            rows.append(
                f"('{r['date_raw']}', {hour_val}, {pt}, {es}, DATE '{r['date_raw']}')"
            )
        if rows:
            daily_rows.append((proc, rows))
            print(f"[preco_full] preparado {proc}: {len(rows)} registos.")

    total = _insert_partition_batches(cur, "iceberg.bronze.preco_raw", cols, daily_rows)

    conn.close()
    print(f"[preco_full] Total: {total} registos inseridos.")
    return total


@workflow
def ingest_bronze_full() -> None:
    """
    Pré-requisito: CSVs já enviados para MinIO warehouse/raw/ pelo run script.
    Execução directa: pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze_full
    """
    ingest_consumo_full()
    ingest_preco_full()
