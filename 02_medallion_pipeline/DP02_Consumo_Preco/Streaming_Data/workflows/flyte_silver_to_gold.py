"""
Workflow Flyte: Silver → Gold — DP-02 Streaming (API).

Materializa dois produtos Gold a partir das tabelas Silver API:
  - dp_energy_market_api_hourly      : produto analítico (consumo + preço + features de calendário e janela)
  - feat_load_forecasting_api_hourly : feature table ML (idem + target consumo_next_hour via LEAD)

Schema idêntico ao pipeline estático (DP-02 Static) — permite comparação direta entre fontes CSV e API.

Dois modos de execução:
  full        — reconstrói todo o histórico Silver disponível; usar após ingestão inicial ou reset.
  incremental — reconstrói apenas [since_date - 1 dia, fim do histórico]; o recuo de 1 dia garante
                que os lags LAG(1h)/LAG(24h) e rolling avg 24h do primeiro dia novo são calculados
                correctamente sobre o histórico Silver anterior.

As window functions (LAG, AVG OVER) operam sobre a partição completa (sem PARTITION BY) para
garantir lags correctos em fronteiras de mês/ano.

Execução (modo full):
    pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_api_full

Execução (modo incremental):
    pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_api_incremental \\
        --since_date 2024-06-01
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import trino
from flytekit import task, workflow

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))


def _trino_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="gold",
        request_timeout=300,
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# Task 1: produto analítico — reconstrução completa do histórico
# ---------------------------------------------------------------------------
@task(retries=3)
def build_dp_energy_market_api_full() -> int:
    """
    Reconstrói gold.dp_energy_market_api_hourly para todo o histórico Silver API.

    Passos:
    1. INNER JOIN iceberg.silver.consumo_api_hourly × preco_api_hourly por ts_utc
    2. Features de calendário: hora (0-23), dia_semana (0 = Segunda), is_weekend
    3. Window lags: consumo_lag_1h, consumo_lag_24h, price_lag_1h
    4. Rolling averages 24h: rolling_avg_consumo_24h, rolling_avg_price_24h

    Só horas com correspondência em ambas as Silver são incluídas (INNER JOIN).
    Idempotente: apaga toda a tabela Gold antes de reinserir.
    """
    conn = _trino_conn()
    cur  = conn.cursor()
    try:
        _exec(cur, "DELETE FROM iceberg.gold.dp_energy_market_api_hourly WHERE 1=1")

        _exec(cur, """
            INSERT INTO iceberg.gold.dp_energy_market_api_hourly (
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                process_date, year, month
            )
            WITH joined AS (
                SELECT
                    c.ts_utc,
                    c.total_mwh                                  AS consumo_total,
                    p.price_portugal_eur_mwh                     AS market_price_pt,
                    HOUR(c.ts_utc)                               AS hora,
                    DAY_OF_WEEK(c.ts_utc) - 1                   AS dia_semana,
                    DAY_OF_WEEK(c.ts_utc) >= 6                  AS is_weekend,
                    YEAR(c.ts_utc)                               AS year,
                    MONTH(c.ts_utc)                              AS month
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
            SELECT
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                CURRENT_DATE AS process_date,
                year, month
            FROM with_windows
        """)

        cur.execute("SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_api_hourly")
        n = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"[gold dp_energy_market_api full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Task 2: feature table ML — reconstrução completa do histórico
# ---------------------------------------------------------------------------
@task(retries=3)
def build_feat_load_forecasting_api_full(upstream_rows: int) -> int:
    """
    Reconstrói gold.feat_load_forecasting_api_hourly para todo o histórico.

    Derivada de dp_energy_market_api_hourly com adição de:
      consumo_next_hour = LEAD(consumo_total, 1) OVER (ORDER BY ts_utc)

    Linhas sem target (última hora) e linhas com lags nulos (primeiras 24h) são
    excluídas pelo filtro WHERE — produz apenas exemplos treináveis.
    O parâmetro upstream_rows cria dependência explícita sobre a Task 1, garantindo
    que a feature table só é escrita após o produto analítico estar completo.

    Idempotente: apaga toda a tabela antes de reinserir.
    """
    _ = upstream_rows

    conn = _trino_conn()
    cur  = conn.cursor()
    try:
        _exec(cur, "DELETE FROM iceberg.gold.feat_load_forecasting_api_hourly WHERE 1=1")

        _exec(cur, """
            INSERT INTO iceberg.gold.feat_load_forecasting_api_hourly (
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                consumo_next_hour, process_date, year, month
            )
            WITH with_lead AS (
                SELECT
                    ts_utc, consumo_total, market_price_pt,
                    hora, dia_semana, is_weekend,
                    consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                    rolling_avg_consumo_24h, rolling_avg_price_24h,
                    LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour,
                    process_date, year, month
                FROM iceberg.gold.dp_energy_market_api_hourly
            )
            SELECT
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                consumo_next_hour, process_date, year, month
            FROM with_lead
            WHERE consumo_next_hour IS NOT NULL
              AND consumo_lag_1h    IS NOT NULL
              AND consumo_lag_24h   IS NOT NULL
              AND price_lag_1h      IS NOT NULL
        """)

        cur.execute("SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_api_hourly")
        n = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"[gold feat_load_forecasting_api full] {n} exemplos inseridos.")
    return n


# ---------------------------------------------------------------------------
# Task 3: produto analítico — janela incremental (since_date - 1 dia em diante)
# ---------------------------------------------------------------------------
@task(retries=3)
def build_dp_energy_market_api_since(since_date: date) -> int:
    """
    Reconstrói gold.dp_energy_market_api_hourly a partir de since_date - 1 dia.

    O recuo de 1 dia (window_start = since_date - 1) é necessário para que os
    lags LAG(24h) e rolling avg 24h das primeiras horas novas sejam calculados
    sobre o contexto Silver completo — sem o recuo, as primeiras 24 linhas da
    janela ficariam com lags nulos.

    O INSERT usa todo o Silver como base para as window functions mas filtra
    o resultado (WHERE ts_utc >= window_ts) para só escrever a janela nova.
    Idempotente: apaga as linhas >= window_ts antes de reinserir.
    """
    window_start = since_date - timedelta(days=1)
    window_ts    = f"{window_start} 00:00:00"

    conn = _trino_conn()
    cur  = conn.cursor()
    try:
        _exec(cur, f"DELETE FROM iceberg.gold.dp_energy_market_api_hourly "
                   f"WHERE ts_utc >= TIMESTAMP '{window_ts}'")

        _exec(cur, f"""
            INSERT INTO iceberg.gold.dp_energy_market_api_hourly (
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                process_date, year, month
            )
            WITH joined AS (
                SELECT
                    c.ts_utc,
                    c.total_mwh                                  AS consumo_total,
                    p.price_portugal_eur_mwh                     AS market_price_pt,
                    HOUR(c.ts_utc)                               AS hora,
                    DAY_OF_WEEK(c.ts_utc) - 1                   AS dia_semana,
                    DAY_OF_WEEK(c.ts_utc) >= 6                  AS is_weekend,
                    YEAR(c.ts_utc)                               AS year,
                    MONTH(c.ts_utc)                              AS month
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
            SELECT
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                CURRENT_DATE AS process_date,
                year, month
            FROM with_windows
            WHERE ts_utc >= TIMESTAMP '{window_ts}'
        """)

        cur.execute(f"SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_api_hourly "
                    f"WHERE ts_utc >= TIMESTAMP '{window_ts}'")
        n = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"[gold dp_energy_market_api since {since_date}] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Task 4: feature table ML — janela incremental (since_date - 1 dia em diante)
# ---------------------------------------------------------------------------
@task(retries=3)
def build_feat_load_forecasting_api_since(since_date: date, upstream_rows: int) -> int:
    """
    Reconstrói gold.feat_load_forecasting_api_hourly a partir de since_date - 1 dia.

    Lê dp_energy_market_api_hourly (já actualizada pela Task 3), calcula
    consumo_next_hour = LEAD(consumo_total, 1) e filtra linhas sem target ou
    sem lags (WHERE consumo_next_hour IS NOT NULL AND lags IS NOT NULL).
    O filtro adicional AND ts_utc >= window_ts restringe a escrita à janela nova.

    O parâmetro upstream_rows cria dependência explícita sobre a Task 3, garantindo
    que a feature table só é escrita após o produto analítico estar actualizado.
    Idempotente: apaga as linhas >= window_ts antes de reinserir.
    """
    _ = upstream_rows
    window_start = since_date - timedelta(days=1)
    window_ts    = f"{window_start} 00:00:00"

    conn = _trino_conn()
    cur  = conn.cursor()
    try:
        _exec(cur, f"DELETE FROM iceberg.gold.feat_load_forecasting_api_hourly "
                   f"WHERE ts_utc >= TIMESTAMP '{window_ts}'")

        _exec(cur, f"""
            INSERT INTO iceberg.gold.feat_load_forecasting_api_hourly (
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                consumo_next_hour, process_date, year, month
            )
            WITH with_lead AS (
                SELECT
                    ts_utc, consumo_total, market_price_pt,
                    hora, dia_semana, is_weekend,
                    consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                    rolling_avg_consumo_24h, rolling_avg_price_24h,
                    LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour,
                    process_date, year, month
                FROM iceberg.gold.dp_energy_market_api_hourly
            )
            SELECT
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                consumo_next_hour, process_date, year, month
            FROM with_lead
            WHERE consumo_next_hour IS NOT NULL
              AND consumo_lag_1h    IS NOT NULL
              AND consumo_lag_24h   IS NOT NULL
              AND price_lag_1h      IS NOT NULL
              AND ts_utc >= TIMESTAMP '{window_ts}'
        """)

        cur.execute(f"SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_api_hourly "
                    f"WHERE ts_utc >= TIMESTAMP '{window_ts}'")
        n = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"[gold feat_load_forecasting_api since {since_date}] {n} exemplos inseridos.")
    return n


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
@workflow
def silver_to_gold_api_full() -> None:
    """
    Reconstrói os dois produtos Gold API para todo o histórico Silver disponível.

    Cadeia de dependência: Task 1 → Task 2 (via upstream_rows).
    Usar após ingestão inicial ou quando é necessário recalcular todo o histórico.

    Execução:
        pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_api_full
    """
    dp_rows = build_dp_energy_market_api_full()
    build_feat_load_forecasting_api_full(upstream_rows=dp_rows)


@workflow
def silver_to_gold_api_incremental(since_date: date) -> None:
    """
    Reconstrói Gold apenas para a janela [since_date - 1 dia, fim do histórico Silver].

    O histórico anterior a since_date - 1 é preservado intacto.
    Usar em execuções diárias após a camada Silver ter sido actualizada para novos dias.

    Cadeia de dependência: Task 3 → Task 4 (via upstream_rows).

    Execução:
        pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_api_incremental \\
            --since_date 2024-06-01
    """
    dp_rows = build_dp_energy_market_api_since(since_date=since_date)
    build_feat_load_forecasting_api_since(since_date=since_date, upstream_rows=dp_rows)
