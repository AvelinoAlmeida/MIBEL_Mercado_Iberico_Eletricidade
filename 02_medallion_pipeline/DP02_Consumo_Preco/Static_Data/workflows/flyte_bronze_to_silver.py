"""
Workflow Flyte: Bronze → Silver para consumo_preco.

Agrega consumo de 15 min para horário (kW → MWh) e normaliza preços day-ahead
para timestamp UTC. Tarefas independentes e idempotentes ao nível diário.

Execução diária:
    pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver --process_date 2023-01-01

Execução completa (todo o histórico):
    pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_full
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import trino
from flytekit import task, workflow, ImageSpec

# Ignorado em execução local; aplicado apenas em execução remota no K3s sandbox.
silver_image = ImageSpec(
    name="dp02_bronze_silver",
    registry="localhost:30000",
    packages=["trino>=0.328.0"],
)

TRINO_HOST = os.getenv("TRINO_HOST", "host.docker.internal")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))


def _trino_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="silver",
    )


def _exec(cur, sql: str) -> None:
    # fetchall necessário para que statements DML/DDL completem antes de continuar.
    cur.execute(sql)
    cur.fetchall()


def _day_bounds(process_date: date) -> tuple[str, str]:
    next_day = process_date + timedelta(days=1)
    return (
        f"{process_date} 00:00:00 UTC",
        f"{next_day} 00:00:00 UTC",
    )


# ---------------------------------------------------------------------------
# Task 1: consumo Bronze → Silver
# ---------------------------------------------------------------------------
@task(retries=3, container_image=silver_image)
def transform_consumo_silver(process_date: date) -> int:
    """Agrega intervalos de 15 min → 1 hora; kW/intervalo → MWh (÷ 1000). DELETE+INSERT para idempotência diária."""
    conn = _trino_conn()
    cur = conn.cursor()

    start_ts, end_ts = _day_bounds(process_date)

    _exec(cur, f"""
        DELETE FROM iceberg.silver.consumo_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)

    _exec(cur, f"""
        INSERT INTO iceberg.silver.consumo_hourly (ts_utc, total_mwh, year, month)
        SELECT
            DATE_TRUNC('hour', datahora)        AS ts_utc,
            SUM(total) / 1000.0                 AS total_mwh,
            YEAR(DATE_TRUNC('hour', datahora))  AS year,
            MONTH(DATE_TRUNC('hour', datahora)) AS month
        FROM iceberg.bronze.consumo_raw
        WHERE process_date = DATE '{process_date}'
        GROUP BY DATE_TRUNC('hour', datahora)
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.silver.consumo_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver consumo] {n} horas inseridas para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Task 2: preços Bronze → Silver
# ---------------------------------------------------------------------------
@task(retries=3, container_image=silver_image)
def transform_preco_silver(process_date: date) -> int:
    """Normaliza preços OMIE (hour 1-24) para UTC. Hour 25 (DST outono) é descartada. GROUP BY+AVG absorve duplicados Bronze."""
    conn = _trino_conn()
    cur = conn.cursor()

    start_ts, end_ts = _day_bounds(process_date)

    _exec(cur, f"""
        DELETE FROM iceberg.silver.preco_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)

    # date_add('hour', hour-1, ...) converte hora OMIE (1-based) para UTC 0-based
    _exec(cur, f"""
        INSERT INTO iceberg.silver.preco_hourly
            (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(price_portugal_raw), 2) AS price_portugal_eur_mwh,
            ROUND(AVG(price_spain_raw), 2)    AS price_spain_eur_mwh,
            YEAR(ts_utc)                      AS year,
            MONTH(ts_utc)                     AS month
        FROM (
            SELECT
                date_add('hour', hour - 1,
                    CAST(DATE_PARSE(date_raw, '%Y-%m-%d') AS TIMESTAMP(6))
                ) AT TIME ZONE 'UTC'  AS ts_utc,
                price_portugal_raw,
                price_spain_raw
            FROM iceberg.bronze.preco_raw
            WHERE process_date = DATE '{process_date}'
              AND hour BETWEEN 1 AND 24
        ) AS normalized
        GROUP BY ts_utc
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.silver.preco_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver preco] {n} horas inseridas para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Task 3: consumo Bronze → Silver completo
# ---------------------------------------------------------------------------
@task(retries=3, container_image=silver_image)
def transform_consumo_silver_full() -> int:
    """Trunca Silver.consumo_hourly e rematerializa todo o histórico Bronze."""
    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.silver.consumo_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.silver.consumo_hourly (ts_utc, total_mwh, year, month)
        SELECT
            DATE_TRUNC('hour', datahora)        AS ts_utc,
            SUM(total) / 1000.0                 AS total_mwh,
            YEAR(DATE_TRUNC('hour', datahora))  AS year,
            MONTH(DATE_TRUNC('hour', datahora)) AS month
        FROM iceberg.bronze.consumo_raw
        GROUP BY DATE_TRUNC('hour', datahora)
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.silver.consumo_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver consumo full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Task 4: preços Bronze → Silver completo
# ---------------------------------------------------------------------------
@task(retries=3, container_image=silver_image)
def transform_preco_silver_full() -> int:
    """Trunca Silver.preco_hourly e rematerializa todo o histórico Bronze."""
    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.silver.preco_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.silver.preco_hourly
            (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(price_portugal_raw), 2) AS price_portugal_eur_mwh,
            ROUND(AVG(price_spain_raw), 2)    AS price_spain_eur_mwh,
            YEAR(ts_utc)                      AS year,
            MONTH(ts_utc)                     AS month
        FROM (
            SELECT
                date_add('hour', hour - 1,
                    CAST(DATE_PARSE(date_raw, '%Y-%m-%d') AS TIMESTAMP(6))
                ) AT TIME ZONE 'UTC'  AS ts_utc,
                price_portugal_raw,
                price_spain_raw
            FROM iceberg.bronze.preco_raw
            WHERE hour BETWEEN 1 AND 24
        ) AS normalized
        GROUP BY ts_utc
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.silver.preco_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver preco full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
@workflow
def bronze_to_silver(process_date: date = date(2023, 1, 1)) -> None:
    """Transforma Bronze → Silver para o process_date indicado. Consumo e preço correm em paralelo."""
    transform_consumo_silver(process_date=process_date)
    transform_preco_silver(process_date=process_date)


@workflow
def bronze_to_silver_full() -> None:
    """Transforma Bronze → Silver para todo o histórico disponível nos dois raw files."""
    transform_consumo_silver_full()
    transform_preco_silver_full()
