"""
Workflow Flyte — camada Bronze (DP-02 Streaming): ingestão via Energy-Charts API.

Fonte: Energy-Charts API (Fraunhofer ISE) — api.energy-charts.info
  Consumo horário PT : GET /public_power?country=pt  (dados ENTSO-E redistribuídos)
  Preços day-ahead PT: GET /price?bzn=PT              (OMIE/MIBEL zona Portugal)
  Preços day-ahead ES: GET /price?bzn=ES              (OMIE/MIBEL zona Espanha)

Sem autenticação — fonte alternativa ao flyte_fetch_bronze_api.py (ENTSO-E)
quando ENTSOE_TOKEN não está disponível. As tabelas de destino Bronze são
idênticas (sufixo _api_raw), pelo que a camada Silver, Gold e os quality
checks não requerem alterações.

Nota sobre granularidade: a Energy-Charts API pode devolver dados a 15 min
para alguns períodos. A função _aggregate_to_hourly() agrega por hora via
média antes da inserção; o Silver faz o mesmo com DATE_TRUNC('hour') + AVG,
garantindo idempotência independente da granularidade da fonte.

Execução standalone (sem orquestrador):
    pyflyte run workflows/flyte_fetch_bronze_energycharts.py fetch_bronze_energycharts \\
        --start_date 2024-01-01 --end_date 2024-01-07

Via orquestrador do pipeline:
    python run_streaming_pipeline.py --source energycharts --skip-docker --days 7
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import requests
import trino
from flytekit import task, workflow

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

BASE_URL       = "https://api.energy-charts.info"
LOAD_TYPE_NAME = "Load"   # campo "name" do tipo de produção em /public_power que corresponde à carga
BATCH_SIZE     = 2000     # nº máximo de linhas por INSERT para evitar timeouts do Trino


# ---------------------------------------------------------------------------
# Utilitários internos partilhados pelas duas tarefas
# ---------------------------------------------------------------------------

def _trino_conn(schema: str = "bronze") -> trino.dbapi.Connection:
    """Cria ligação ao Trino com catálogo iceberg e schema configurável."""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema=schema,
    )


def _exec(cur, sql: str) -> None:
    """Executa SQL sem retornar resultados (DDL / DELETE)."""
    cur.execute(sql)
    cur.fetchall()


def _flush_batch(cur, table: str, cols: str, batch: list[str]) -> int:
    """Insere o batch atual na tabela Trino e limpa a lista. Devolve nº de linhas inseridas."""
    if not batch:
        return 0
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES {', '.join(batch)}")
    cur.fetchall()
    n = len(batch)
    batch.clear()
    return n


def _ec_get(endpoint: str, params: dict) -> dict:
    """Faz GET à Energy-Charts API e devolve o JSON; lança exceção em caso de erro HTTP."""
    url  = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _find_load_series(production_types: list[dict]) -> list | None:
    """Localiza a série de carga na lista production_types da resposta /public_power.

    A comparação é case-insensitive para tolerar variações na API (ex: "load" vs "Load").
    Devolve a lista de valores ou None se o tipo não for encontrado.
    """
    for pt in production_types:
        if pt.get("name", "").strip().lower() == LOAD_TYPE_NAME.lower():
            return pt.get("data")
    return None


def _aggregate_to_hourly(ts_value_pairs: list[tuple[datetime, float]]) -> dict[datetime, float]:
    """Agrega pares (timestamp, valor) sub-horários para granularidade horária via média.

    Trunca o timestamp ao início da hora e calcula a média de todos os valores
    que caem nessa janela. Necessário porque a Energy-Charts API pode devolver
    dados a 15 min dependendo do período e da zona de balanço.
    """
    buckets: dict[datetime, list[float]] = {}
    for ts, val in ts_value_pairs:
        hour_ts = ts.replace(minute=0, second=0, microsecond=0)
        buckets.setdefault(hour_ts, []).append(val)
    return {ts: sum(vals) / len(vals) for ts, vals in buckets.items()}


# ---------------------------------------------------------------------------
# Task 1: consumo horário PT — Energy-Charts /public_power
# ---------------------------------------------------------------------------

@task(retries=3)
def fetch_consumo_ec(start_date: date, end_date: date) -> int:
    """Obtém carga eléctrica horária de Portugal via Energy-Charts /public_power.

    Sem autenticação. Idempotente: remove os registos do intervalo (por process_date)
    antes de inserir, para que re-execuções não criem duplicados.
    Destino: iceberg.bronze.consumo_api_raw — schema compatível com o pipeline ENTSO-E.
    Devolve o número total de linhas inseridas.
    """
    source_url = f"{BASE_URL}/public_power?country=pt&start={start_date}&end={end_date}"
    print(f"[fetch_consumo EC] GET {source_url}")

    data = _ec_get(
        "public_power",
        {"country": "pt", "start": start_date.isoformat(), "end": end_date.isoformat()},
    )

    unix_seconds = data.get("unix_seconds", [])
    load_data    = _find_load_series(data.get("production_types", []))

    if not load_data or not unix_seconds:
        print(f"[fetch_consumo EC] Sem dados de carga para {start_date}--{end_date}.")
        return 0

    # A API pode devolver granularidade 15-min; agrega para horário antes da inserção
    raw_pairs = [
        (datetime.fromtimestamp(ts, tz=timezone.utc), float(v))
        for ts, v in zip(unix_seconds, load_data)
        if v is not None
    ]
    hourly_map = _aggregate_to_hourly(raw_pairs)

    fetch_dt = date.today()
    records: list[dict] = []

    for ts_utc, value in sorted(hourly_map.items()):
        if ts_utc.date() < start_date or ts_utc.date() > end_date:
            continue
        records.append({
            "ts_utc":       ts_utc,
            "total":        round(value, 3),
            "source_url":   source_url,
            "fetch_date":   fetch_dt,
            "process_date": ts_utc.date(),
        })

    if not records:
        print(f"[fetch_consumo EC] Serie vazia para {start_date}--{end_date}.")
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd_date in process_dates:
        _exec(
            cur,
            f"DELETE FROM iceberg.bronze.consumo_api_raw "
            f"WHERE process_date = DATE '{pd_date}'",
        )
        print(f"[fetch_consumo EC] Limpa {pd_date}.")

    cols  = "ts_utc, total, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0

    for r in records:
        ts_str = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        safe_url = r["source_url"].replace("'", "''")
        batch.append(
            f"(TIMESTAMP '{ts_str}', {r['total']}, '{safe_url}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush_batch(cur, "iceberg.bronze.consumo_api_raw", cols, batch)

    total += _flush_batch(cur, "iceberg.bronze.consumo_api_raw", cols, batch)
    conn.close()
    print(f"[fetch_consumo EC] Inseridos: {total} registos ({start_date}--{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Task 2: preços day-ahead PT + ES — Energy-Charts /price
# ---------------------------------------------------------------------------

@task(retries=3)
def fetch_preco_ec(start_date: date, end_date: date) -> int:
    """Obtém preços day-ahead de Portugal e Espanha via Energy-Charts /price.

    Sem autenticação. Idempotente: remove os registos do intervalo (por process_date)
    antes de inserir, para que re-execuções não criem duplicados.
    Destino: iceberg.bronze.preco_api_raw — schema compatível com o pipeline ENTSO-E.
    Devolve o número total de linhas inseridas (união PT ∪ ES).
    """
    print(f"[fetch_preco EC] GET precos PT+ES: {start_date} -> {end_date}")

    source_url = f"{BASE_URL}/price?bzn=PT&start={start_date}&end={end_date}"

    # --- PT
    try:
        pt_data   = _ec_get("price", {"bzn": "PT", "start": start_date.isoformat(), "end": end_date.isoformat()})
        pt_unix   = pt_data.get("unix_seconds", [])
        pt_prices = pt_data.get("price", [])
    except Exception as exc:
        print(f"[fetch_preco EC] Erro PT: {exc}")
        pt_unix, pt_prices = [], []

    # --- ES
    try:
        es_data   = _ec_get("price", {"bzn": "ES", "start": start_date.isoformat(), "end": end_date.isoformat()})
        es_unix   = es_data.get("unix_seconds", [])
        es_prices = es_data.get("price", [])
    except Exception as exc:
        print(f"[fetch_preco EC] Erro ES: {exc}")
        es_unix, es_prices = [], []

    # Agrega PT e ES de sub-horário (15-min) para horário via média, se aplicável
    pt_map = _aggregate_to_hourly([
        (datetime.fromtimestamp(ts, tz=timezone.utc), float(p))
        for ts, p in zip(pt_unix, pt_prices) if p is not None
    ])
    es_map = _aggregate_to_hourly([
        (datetime.fromtimestamp(ts, tz=timezone.utc), float(p))
        for ts, p in zip(es_unix, es_prices) if p is not None
    ])

    all_ts = sorted(set(pt_map) | set(es_map))
    if not all_ts:
        print(f"[fetch_preco EC] Sem dados para {start_date}--{end_date}.")
        return 0

    fetch_dt = date.today()
    records: list[dict] = []

    for ts_utc in all_ts:
        if ts_utc.date() < start_date or ts_utc.date() > end_date:
            continue
        records.append({
            "ts_utc":                 ts_utc,
            "price_portugal_eur_mwh": pt_map.get(ts_utc),
            "price_spain_eur_mwh":    es_map.get(ts_utc),
            "source_url":             source_url,
            "fetch_date":             fetch_dt,
            "process_date":           ts_utc.date(),
        })

    if not records:
        print(f"[fetch_preco EC] Sem registos no intervalo {start_date}--{end_date}.")
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd_date in process_dates:
        _exec(
            cur,
            f"DELETE FROM iceberg.bronze.preco_api_raw "
            f"WHERE process_date = DATE '{pd_date}'",
        )
        print(f"[fetch_preco EC] Limpa {pd_date}.")

    cols  = "ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0

    for r in records:
        ts_str  = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        pt_val  = str(r["price_portugal_eur_mwh"]) if r["price_portugal_eur_mwh"] is not None else "NULL"
        es_val  = str(r["price_spain_eur_mwh"])    if r["price_spain_eur_mwh"]    is not None else "NULL"
        safe_url = r["source_url"].replace("'", "''")
        batch.append(
            f"(TIMESTAMP '{ts_str}', {pt_val}, {es_val}, '{safe_url}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)

    total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)
    conn.close()
    print(f"[fetch_preco EC] Inseridos: {total} registos ({start_date}--{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Workflow principal: fetch consumo + preço em paralelo
# ---------------------------------------------------------------------------

@workflow
def fetch_bronze_energycharts(
    start_date: date = date.today() - timedelta(days=6),
    end_date:   date = date.today(),
) -> None:
    """Ingest de consumo e preços Energy-Charts para a camada Bronze (DP-02 Streaming).

    Alternativa sem autenticação ao workflow ENTSO-E (flyte_fetch_bronze_api.py).
    As tarefas fetch_consumo_ec e fetch_preco_ec não têm dependências entre si
    e são executadas em paralelo pelo Flyte.

    Janela por omissão: últimos 7 dias (start_date=hoje-6, end_date=hoje).

    Execução:
        pyflyte run workflows/flyte_fetch_bronze_energycharts.py fetch_bronze_energycharts \\
            --start_date 2024-01-01 --end_date 2024-01-07
    """
    fetch_consumo_ec(start_date=start_date, end_date=end_date)
    fetch_preco_ec(start_date=start_date, end_date=end_date)
