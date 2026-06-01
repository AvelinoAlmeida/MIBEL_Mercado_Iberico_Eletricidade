"""
Workflow Flyte: gate de qualidade por camada — DP-02 Streaming (API Energy-Charts).

Executa o primeiro bloco SQL de cada ficheiro em 04_quality/sql/ contra o Trino
e classifica os resultados como PASS / WARN / FAIL. Qualquer check FAIL levanta
FlyteRecoverableException, bloqueando as camadas a jusante.

Workflows expostos (um por camada, para permitir re-runs granulares):
    pyflyte run workflows/flyte_quality_checks.py quality_gate_bronze_api
    pyflyte run workflows/flyte_quality_checks.py quality_gate_silver_api
    pyflyte run workflows/flyte_quality_checks.py quality_gate_gold_api

Variáveis de ambiente:
    TRINO_HOST  — hostname do Trino (padrão: localhost)
    TRINO_PORT  — porta do Trino (padrão: 8080)
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import trino
from flytekit import task, workflow
from flytekit.exceptions.user import FlyteRecoverableException

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

_SQL_DIR = Path(__file__).parent.parent / "04_quality" / "sql"

_LAYER_SQL = {
    "bronze": _SQL_DIR / "01_bronze_checks.sql",
    "silver": _SQL_DIR / "02_silver_checks.sql",
    "gold":   _SQL_DIR / "03_gold_checks.sql",
}


def _trino_conn() -> trino.dbapi.Connection:
    # catalog=iceberg porque os checks de Bronze lêem tabelas hive via views iceberg;
    # Silver e Gold são tabelas iceberg nativas — um único catalog cobre as três camadas.
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="bronze",
    )


def _run_checks(layer: str) -> list[dict]:
    sql_path = _LAYER_SQL[layer]
    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL de qualidade não encontrado: {sql_path}\n"
            f"Verifique se 04_quality/sql/{sql_path.name} existe."
        )

    raw_sql = sql_path.read_text(encoding="utf-8")
    # Remove comentários SQL (--) antes de localizar o primeiro ponto-e-vírgula,
    # evitando falsos positivos em linhas comentadas que contenham ";".
    sql_no_comments = re.sub(r"--[^\n]*", "", raw_sql)
    first_semi = sql_no_comments.find(";")
    # Executa apenas o primeiro statement (SELECT de resumo); o resto do ficheiro
    # pode conter queries auxiliares/debug que não devem correr no gate.
    summary_sql = (sql_no_comments[:first_semi] if first_semi != -1 else sql_no_comments).strip()

    conn = _trino_conn()
    cur  = conn.cursor()
    cur.execute(summary_sql)
    columns = [d[0] for d in cur.description]
    rows    = [dict(zip(columns, row)) for row in cur.fetchall()]
    conn.close()
    return rows


@task(retries=2)
def quality_gate_api(layer: str) -> int:
    """
    Executa os checks de qualidade para a camada indicada e devolve o nº de PASS.

    Args:
        layer: 'bronze', 'silver' ou 'gold'.

    Returns:
        Número de checks com status PASS.

    Raises:
        FlyteRecoverableException: se existir pelo menos um check FAIL (permite retry).
        ValueError: se a camada não for reconhecida.
    """
    if layer not in _LAYER_SQL:
        raise ValueError(f"Camada inválida: '{layer}'. Aceites: 'bronze', 'silver', 'gold'.")

    results = _run_checks(layer)
    fails   = [r for r in results if r.get("status") == "FAIL"]
    warns   = [r for r in results if r.get("status") == "WARN"]
    passes  = [r for r in results if r.get("status") == "PASS"]

    print(f"\n{'=' * 60}")
    print(f"QUALITY GATE API — {layer.upper()} ({len(results)} checks)")
    print(f"{'=' * 60}")
    print(f"  PASS : {len(passes)}")
    print(f"  WARN : {len(warns)}")
    print(f"  FAIL : {len(fails)}")
    print()

    for r in passes:
        print(f"  [PASS] {r['check_name']}")
    for r in warns:
        print(f"  [WARN] {r['check_name']} -> {r.get('detalhe', '')}")
    for r in fails:
        print(f"  [FAIL] {r['check_name']} -> {r.get('detalhe', '')}")

    if fails:
        fail_names = "; ".join(r["check_name"] for r in fails)
        raise FlyteRecoverableException(
            f"[streaming_dp02] Quality gate FALHOU na camada {layer}: "
            f"{len(fails)} check(s) FAIL: {fail_names}"
        )

    print(f"  -> Gate APROVADO para camada {layer}.\n")
    return len(passes)


@workflow
def quality_gate_bronze_api() -> int:
    """Gate de qualidade Bronze — Streaming_Data."""
    return quality_gate_api(layer="bronze")


@workflow
def quality_gate_silver_api() -> int:
    """Gate de qualidade Silver — Streaming_Data."""
    return quality_gate_api(layer="silver")


@workflow
def quality_gate_gold_api() -> int:
    """Gate de qualidade Gold — Streaming_Data."""
    return quality_gate_api(layer="gold")
