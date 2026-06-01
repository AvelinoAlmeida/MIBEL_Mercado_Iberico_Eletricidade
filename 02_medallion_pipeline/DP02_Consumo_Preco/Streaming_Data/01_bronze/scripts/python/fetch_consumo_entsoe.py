"""
Fetch de carga elétrica nacional horária via ENTSO-E Transparency Platform.

Fonte: https://transparency.entsoe.eu/
Dados: Actual Total Load Portugal, granularidade horária, em MW.
Autenticação: token gratuito (ver instruções abaixo).

Como obter o token ENTSO-E:
    1. Envia email para transparency@entsoe.eu
       Assunto: "RESTful API access"
       Corpo:   "Hello, I would like to request API access for research purposes."
    2. Resposta em ~3 dias úteis com o token.
    3. Define a variável de ambiente:
         PowerShell: $env:ENTSOE_TOKEN = "<o-teu-token>"
         Linux/Mac:  export ENTSOE_TOKEN=<o-teu-token>

Porquê ENTSO-E em vez de Energy-Charts (fonte anterior):
    - Dados primários direto da ENTSO-E (Energy-Charts era um redistribuidor)
    - Sem endpoints "deprecated"
    - Granularidade horária exata (Energy-Charts devolvia 15 min para preços)

Uso standalone:
    python fetch_consumo_entsoe.py --start 2024-01-01 --end 2024-01-07
    python fetch_consumo_entsoe.py --days 7
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

ENTSOE_TOKEN = os.getenv("ENTSOE_TOKEN", "")

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

BATCH_SIZE = 2000


def _get_client() -> EntsoePandasClient:
    if not ENTSOE_TOKEN:
        raise EnvironmentError(
            "\n[ERRO] ENTSOE_TOKEN nao definido.\n"
            "Como obter token gratuito:\n"
            "  1. Email para transparency@entsoe.eu -- 'RESTful API access'\n"
            "  2. Aguarda ~3 dias uteis\n"
            "  3. PowerShell: $env:ENTSOE_TOKEN = '<token>'\n"
        )
    return EntsoePandasClient(api_key=ENTSOE_TOKEN)


def fetch_load_data(start: date, end: date) -> list[dict]:
    """
    Consulta a ENTSO-E API e devolve lista de registos com ts_utc e total (MW).

    O intervalo pedido à API é [start, end+1 dia) em UTC para garantir que o
    último dia fica completo, independentemente do fuso horário local.
    Registos com valor NaN são descartados silenciosamente.
    """
    client   = _get_client()
    start_ts = pd.Timestamp(start.isoformat(), tz="UTC")
    end_ts   = pd.Timestamp((end + timedelta(days=1)).isoformat(), tz="UTC")
    source   = f"https://transparency.entsoe.eu/ ENTSO-E Actual Total Load PT {start}/{end}"

    print(f"[fetch_consumo ENTSOE] GET Actual Total Load PT {start} -> {end}")

    try:
        raw = client.query_load("PT", start=start_ts, end=end_ts)
    except NoMatchingDataError:
        print(f"[fetch_consumo ENTSOE] Sem dados para {start}--{end}.")
        return []

    series = raw.iloc[:, 0] if isinstance(raw, pd.DataFrame) else raw

    records = []
    fetch_dt = date.today()

    for ts_idx, value in series.items():
        if pd.isna(value):
            continue
        dt = ts_idx.to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        records.append({
            "ts_utc":       dt,
            "total":        float(value),
            "source_url":   source,
            "fetch_date":   fetch_dt,
            "process_date": dt.date(),
        })

    print(f"[fetch_consumo ENTSOE] {len(records)} registos obtidos para {start}--{end}.")
    return records


def _trino_conn():
    import trino
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="bronze",
    )


def insert_to_bronze(records: list[dict], process_dates: list[date]) -> int:
    """
    Insere registos em iceberg.bronze.consumo_api_raw.

    Idempotente: elimina as partições process_date afetadas antes de inserir,
    pelo que é seguro re-executar para o mesmo intervalo de datas.
    Os registos são enviados em lotes de BATCH_SIZE para evitar payloads gigantes.
    """
    if not records:
        print("[fetch_consumo ENTSOE] Nenhum registo para inserir.")
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    for pd_date in process_dates:
        cur.execute(
            f"DELETE FROM iceberg.bronze.consumo_api_raw "
            f"WHERE process_date = DATE '{pd_date}'"
        )
        cur.fetchall()
        print(f"[fetch_consumo ENTSOE] Limpa process_date={pd_date}.")

    cols = "ts_utc, total, source_url, fetch_date, process_date"
    inserted = 0
    batch: list[str] = []

    def flush():
        nonlocal inserted
        if not batch:
            return
        cur.execute(
            f"INSERT INTO iceberg.bronze.consumo_api_raw ({cols}) "
            f"VALUES {', '.join(batch)}"
        )
        cur.fetchall()
        inserted += len(batch)
        batch.clear()

    for r in records:
        ts_str = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        batch.append(
            f"(TIMESTAMP '{ts_str}', {r['total']}, '{r['source_url']}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            flush()

    flush()
    conn.close()
    print(f"[fetch_consumo ENTSOE] Total inserido: {inserted} registos.")
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch consumo horario via ENTSO-E (Actual Total Load PT)"
    )
    parser.add_argument("--start", type=str, help="Data de início YYYY-MM-DD")
    parser.add_argument("--end",   type=str, help="Data de fim YYYY-MM-DD")
    parser.add_argument("--days",  type=int, default=7, help="Últimos N dias (default: 7)")
    args = parser.parse_args()

    if args.start and args.end:
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end)
    else:
        end   = date.today()
        start = end - timedelta(days=args.days - 1)

    print(f"[fetch_consumo ENTSOE] Periodo: {start} -> {end}")

    try:
        import trino  # noqa: F401
    except ImportError:
        import subprocess
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "trino", "entsoe-py", "pandas", "-q"],
            check=True,
        )

    records = fetch_load_data(start, end)
    process_dates = sorted({r["process_date"] for r in records})
    insert_to_bronze(records, process_dates)


if __name__ == "__main__":
    main()
