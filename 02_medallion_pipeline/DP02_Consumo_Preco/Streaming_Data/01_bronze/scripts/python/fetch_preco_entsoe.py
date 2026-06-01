"""
Fetch de preços day-ahead MIBEL horários via ENTSO-E Transparency Platform.

Fonte: https://transparency.entsoe.eu/
Dados: Day-Ahead Prices Portugal + Espanha, granularidade horária, EUR/MWh.
Autenticação: token gratuito (ver fetch_consumo_entsoe.py para instruções completas).

Porquê ENTSO-E em vez de Energy-Charts (fonte anterior):
    - price_spain_eur_mwh agora preenchido (ES disponível separadamente na ENTSO-E)
    - Dados horários exatos (24 valores/dia — Energy-Charts retornava 15 min para preços)
    - Sem endpoints "deprecated" na resposta

Uso standalone:
    python fetch_preco_entsoe.py --start 2024-01-01 --end 2024-01-07
    python fetch_preco_entsoe.py --days 7
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
            "\n[ERRO] ENTSOE_TOKEN não definido.\n"
            "Ver instruções de obtenção do token em fetch_consumo_entsoe.py\n"
        )
    return EntsoePandasClient(api_key=ENTSOE_TOKEN)


def _to_series(result) -> pd.Series:
    if isinstance(result, pd.DataFrame):
        return result.iloc[:, 0]
    return result


def fetch_price_data(start: date, end: date) -> list[dict]:
    """
    Consulta a ENTSO-E API e devolve lista de registos com ts_utc, preço PT e preço ES.

    O intervalo pedido à API é [start, end+1 dia) em UTC para garantir que o
    último dia fica completo, independentemente do fuso horário local.
    PT e ES são consultados separadamente e unidos por outer join no índice temporal.
    Valores NaN são preservados como None (colunas independentes podem falhar individualmente).
    """
    client   = _get_client()
    start_ts = pd.Timestamp(start.isoformat(), tz="UTC")
    end_ts   = pd.Timestamp((end + timedelta(days=1)).isoformat(), tz="UTC")
    source   = f"https://transparency.entsoe.eu/ ENTSO-E Day-Ahead Prices PT+ES {start}/{end}"

    print(f"[fetch_preco ENTSOE] GET Day-Ahead Prices PT+ES {start} -> {end}")

    # -- PT
    try:
        pt_series = _to_series(
            client.query_day_ahead_prices("PT", start=start_ts, end=end_ts)
        ).rename("pt")
    except NoMatchingDataError:
        print(f"[fetch_preco ENTSOE] Sem precos PT para {start}--{end}.")
        pt_series = pd.Series(dtype=float, name="pt")

    # -- ES
    try:
        es_series = _to_series(
            client.query_day_ahead_prices("ES", start=start_ts, end=end_ts)
        ).rename("es")
    except NoMatchingDataError:
        print(f"[fetch_preco ENTSOE] Sem precos ES para {start}--{end}.")
        es_series = pd.Series(dtype=float, name="es")

    merged = pt_series.to_frame().join(es_series.to_frame(), how="outer")

    if merged.empty:
        print(f"[fetch_preco ENTSOE] Sem dados para {start}--{end}.")
        return []

    records = []
    fetch_dt = date.today()

    for ts_idx, row in merged.iterrows():
        dt = ts_idx.to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        pt_price = None if pd.isna(row.get("pt")) else float(row["pt"])
        es_price = None if pd.isna(row.get("es")) else float(row["es"])
        records.append({
            "ts_utc":                dt,
            "price_portugal_eur_mwh": pt_price,
            "price_spain_eur_mwh":    es_price,
            "source_url":            source,
            "fetch_date":            fetch_dt,
            "process_date":          dt.date(),
        })

    print(f"[fetch_preco ENTSOE] {len(records)} registos obtidos para {start}--{end}.")
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
    Insere registos em iceberg.bronze.preco_api_raw.

    Idempotente: elimina as partições process_date afetadas antes de inserir,
    pelo que é seguro re-executar para o mesmo intervalo de datas.
    Os registos são enviados em lotes de BATCH_SIZE para evitar payloads gigantes.
    """
    if not records:
        print("[fetch_preco ENTSOE] Nenhum registo para inserir.")
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    for pd_date in process_dates:
        cur.execute(
            f"DELETE FROM iceberg.bronze.preco_api_raw "
            f"WHERE process_date = DATE '{pd_date}'"
        )
        cur.fetchall()
        print(f"[fetch_preco ENTSOE] Limpa process_date={pd_date}.")

    cols = "ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, source_url, fetch_date, process_date"
    inserted = 0
    batch: list[str] = []

    def flush():
        nonlocal inserted
        if not batch:
            return
        cur.execute(
            f"INSERT INTO iceberg.bronze.preco_api_raw ({cols}) "
            f"VALUES {', '.join(batch)}"
        )
        cur.fetchall()
        inserted += len(batch)
        batch.clear()

    for r in records:
        ts_str   = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        pt_val   = str(r["price_portugal_eur_mwh"]) if r["price_portugal_eur_mwh"] is not None else "NULL"
        es_val   = str(r["price_spain_eur_mwh"])    if r["price_spain_eur_mwh"]    is not None else "NULL"
        batch.append(
            f"(TIMESTAMP '{ts_str}', {pt_val}, {es_val}, '{r['source_url']}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            flush()

    flush()
    conn.close()
    print(f"[fetch_preco ENTSOE] Total inserido: {inserted} registos.")
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch precos day-ahead PT+ES via ENTSO-E"
    )
    parser.add_argument("--start", type=str, help="Data inicio YYYY-MM-DD")
    parser.add_argument("--end",   type=str, help="Data fim YYYY-MM-DD")
    parser.add_argument("--days",  type=int, default=7, help="Ultimos N dias (default: 7)")
    args = parser.parse_args()

    if args.start and args.end:
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end)
    else:
        end   = date.today()
        start = end - timedelta(days=args.days - 1)

    print(f"[fetch_preco ENTSOE] Periodo: {start} -> {end}")

    try:
        import trino  # noqa: F401
    except ImportError:
        import subprocess
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "trino", "entsoe-py", "pandas", "-q"],
            check=True,
        )

    records = fetch_price_data(start, end)
    process_dates = sorted({r["process_date"] for r in records})
    insert_to_bronze(records, process_dates)


if __name__ == "__main__":
    main()
