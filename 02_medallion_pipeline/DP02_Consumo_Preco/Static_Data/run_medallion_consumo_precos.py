#!/usr/bin/env python3
"""Orquestrador da pipeline Medallion de consumo_preco (DP-02 Static).

Fontes de dados:
  - Consumo: consumo-total-nacional.csv (REN, granularidade 15 min)
  - Preços:  Day-ahead Market Prices_*.csv (OMIE, granularidade horária)

Fluxo de execução:
  FASE 0   — Aplica DDL Bronze, Silver e Gold via Trino       (saltar com --skip-ddl)
  FASE 0.5 — Upload dos CSVs raw para MinIO (warehouse/raw/)  (saltar com --skip-upload)
  FASE 1   — Ingestão Bronze: pyflyte run (execução local)
             Quality gate Bronze                               (saltar com --no-quality)
  FASE 2   — Transformação Bronze → Silver: pyflyte run local
             Quality gate Silver                               (saltar com --no-quality)
  FASE 3   — Transformação Silver → Gold: pyflyte run local   (sempre full — window functions)
             Quality gate Gold                                 (saltar com --no-quality)
  FINAL    — Validação COUNT e intervalo das tabelas Gold

  Pré-fase: sobe stack Docker Compose e aguarda Trino          (saltar com --skip-docker)

Flags disponíveis:
  --build         faz --build no compose up
  --skip-docker   salta o compose up (stack já a correr)
  --skip-ddl      salta a aplicação de DDL (tabelas já existem)
  --skip-upload   salta upload dos CSVs para MinIO (já lá estão)
  --no-quality    salta todos os quality gates
  --year YYYY     processa apenas o mês indicado (requer --month)
  --month M       processa apenas o mês indicado (requer --year)

Exemplos:
    # Carga completa (cria e povoa todas as camadas)
    python run_medallion_consumo_precos.py

    # Stack já a correr
    python run_medallion_consumo_precos.py --skip-docker

    # Stack e DDL já aplicados, CSVs já no MinIO
    python run_medallion_consumo_precos.py --skip-docker --skip-ddl --skip-upload

    # Mês específico (Silver incremental, Gold sempre full)
    python run_medallion_consumo_precos.py --skip-docker --year 2024 --month 3

    # Incluir build das imagens Docker
    python run_medallion_consumo_precos.py --build
"""
from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers de subprocess
# ---------------------------------------------------------------------------

def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> None:
    print(f"\n>>> {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        input=input_text,
        capture_output=(input_text is not None),
    )
    if input_text is not None:
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd,
            output=getattr(result, "stdout", None),
            stderr=getattr(result, "stderr", None),
        )


def run_with_retry(
    cmd: list[str],
    *,
    max_attempts: int = 3,
    delay: int = 5,
    **kwargs,
) -> None:
    """Executa `run` com retry automático; lança CalledProcessError na última tentativa."""
    for attempt in range(1, max_attempts + 1):
        try:
            run(cmd, **kwargs)
            return
        except subprocess.CalledProcessError as exc:
            if attempt == max_attempts:
                raise
            print(
                f"[AVISO] Tentativa {attempt}/{max_attempts} falhou (exit {exc.returncode}). "
                f"A repetir em {delay}s...",
                file=sys.stderr,
            )
            time.sleep(delay)


def must_exist(path: Path, description: str) -> None:
    if not path.exists():
        raise SystemExit(f"Erro: {description} não encontrado: {path}")


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

def docker_engine_running() -> bool:
    return subprocess.run(["docker", "info"], text=True, capture_output=True).returncode == 0


def try_start_docker_engine() -> bool:
    system = platform.system().lower()
    start_commands: list[list[str]] = []
    if system == "linux":
        start_commands = [["systemctl", "start", "docker"], ["service", "docker", "start"]]
    elif system == "darwin":
        start_commands = [["open", "-a", "Docker"]]
    elif system == "windows":
        start_commands = [[
            "powershell", "-NoProfile", "-Command",
            "Start-Process 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'",
        ]]
    for cmd in start_commands:
        if shutil.which(cmd[0]) is None:
            continue
        print(f">>> Docker indisponível. A tentar arrancar: {' '.join(cmd)}")
        subprocess.run(cmd, text=True, capture_output=True)
        for _ in range(20):
            if docker_engine_running():
                print(">>> Docker Engine disponível.")
                return True
            time.sleep(1)
    return docker_engine_running()


def ensure_docker_engine_running() -> None:
    if docker_engine_running():
        return
    if try_start_docker_engine():
        return
    raise SystemExit(
        "Erro: Docker Engine não está a correr.\n"
        "Arranca o Docker manualmente e repete."
    )


def wait_for_trino(compose_file: Path, attempts: int = 30, sleep_seconds: int = 2) -> None:
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino", "--execute", "SELECT 1;",
    ]
    for attempt in range(1, attempts + 1):
        print(f"\n>>> Trino disponível? ({attempt}/{attempts})")
        if subprocess.run(cmd, text=True, capture_output=True).returncode == 0:
            print(">>> Trino OK.")
            return
        time.sleep(sleep_seconds)
    raise SystemExit("Erro: Trino não ficou disponível dentro do tempo esperado.")


# ---------------------------------------------------------------------------
# Venv helper
# ---------------------------------------------------------------------------

def create_local_venv(pipeline_root: Path, base_python: str) -> Path:
    venv_dir = pipeline_root / ".venv_medallion_consumo_preco"
    venv_python = (
        venv_dir / "Scripts" / "python.exe"
        if os.name == "nt"
        else venv_dir / "bin" / "python"
    )
    if not venv_python.exists():
        print(f"\n>>> A criar virtualenv em: {venv_dir}")
        run([base_python, "-m", "venv", str(venv_dir)])
    return venv_python


# ---------------------------------------------------------------------------
# DDL helper — aplica ficheiro SQL statement a statement via Trino CLI
# ---------------------------------------------------------------------------

def _split_sql(sql: str) -> list[str]:
    """Divide SQL em statements pelo ';', preservando aspas simples escapadas ('')."""
    statements: list[str] = []
    buf: list[str] = []
    in_string = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_string:
            in_string = True
            buf.append(ch)
        elif ch == "'" and in_string:
            buf.append(ch)
            if i + 1 < len(sql) and sql[i + 1] == "'":
                buf.append("'")
                i += 1
            else:
                in_string = False
        elif ch == ";" and not in_string:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    stmt = "".join(buf).strip()
    if stmt:
        statements.append(stmt)
    return statements


def apply_ddl(compose_file: Path, sql_file: Path, stage_name: str) -> None:
    print(f"\n>>> DDL {stage_name}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    statements = _split_sql(sql_clean)
    for i, stmt in enumerate(statements, 1):
        result = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "exec", "-T", "trino", "trino"],
            input=(stmt + ";").encode("utf-8"),
            capture_output=True,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            stdout = result.stdout.decode("utf-8", errors="replace").strip()
            preview = (stmt[:120] + "...") if len(stmt) > 120 else stmt
            raise SystemExit(
                f"\nDDL {stage_name} falhou no statement {i}:\n"
                f"  SQL: {preview}\n"
                f"  STDOUT: {stdout}\n"
                f"  STDERR: {stderr}"
            )
    print(f"    DDL {stage_name} aplicado ({len(statements)} statements).")


# ---------------------------------------------------------------------------
# Trino execute helper (com saída visível e retry)
# ---------------------------------------------------------------------------

def trino_execute(compose_file: Path, sql: str, *, max_attempts: int = 3) -> None:
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino",
    ]
    for attempt in range(1, max_attempts + 1):
        result = subprocess.run(cmd, input=sql.encode("utf-8"), capture_output=True)
        if result.stdout:
            print(result.stdout.decode("utf-8", errors="replace"))
        if result.returncode == 0:
            return
        if attempt < max_attempts:
            print(
                f"[AVISO] Tentativa {attempt}/{max_attempts} falhou. A repetir em 5s...",
                file=sys.stderr,
            )
            if result.stderr:
                print(result.stderr.decode("utf-8", errors="replace"), file=sys.stderr)
            time.sleep(5)
        else:
            if result.stderr:
                print(result.stderr.decode("utf-8", errors="replace"), file=sys.stderr)
            raise subprocess.CalledProcessError(result.returncode, cmd)


# ---------------------------------------------------------------------------
# Quality gate — executa SQL de checks e bloqueia em FAIL
# ---------------------------------------------------------------------------

def run_quality_checks(compose_file: Path, sql_file: Path, layer_name: str) -> None:
    print(f"\n>>> Quality gate — {layer_name}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    if not statements:
        print(f"    [AVISO] Nenhum statement encontrado em {sql_file.name}.")
        return

    check_sql = statements[0] + ";"
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino",
        "--output-format", "TSV_HEADER",
        "--execute", check_sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(
            f"Quality gate {layer_name} falhou ao executar checks:\n{result.stderr.strip()}"
        )

    lines = result.stdout.strip().splitlines()
    if len(lines) < 2:
        print(f"    [AVISO] Quality gate {layer_name}: sem resultados.")
        return

    fail_checks, warn_checks, pass_checks = [], [], []
    for line in lines[1:]:
        cols = line.split("\t")
        status = cols[1].strip() if len(cols) > 1 else "?"
        if status == "FAIL":
            fail_checks.append(line)
        elif status == "WARN":
            warn_checks.append(line)
        else:
            pass_checks.append(line)

    print(f"\n    PASS: {len(pass_checks)}  WARN: {len(warn_checks)}  FAIL: {len(fail_checks)}")
    for line in warn_checks:
        print(f"    [WARN] {line.split(chr(9))[0]} — {line.split(chr(9))[-1]}")
    for line in fail_checks:
        print(f"    [FAIL] {line.split(chr(9))[0]} — {line.split(chr(9))[-1]}")

    if fail_checks:
        raise SystemExit(
            f"\nQuality gate {layer_name} bloqueado: {len(fail_checks)} check(s) em FAIL."
        )
    print(f"    Quality gate {layer_name} OK.")


# ---------------------------------------------------------------------------
# MinIO upload helper — envia CSVs raw para o bucket warehouse/raw/
# ---------------------------------------------------------------------------

def upload_raw_csvs_to_minio(raw_dir: Path) -> None:
    print("\n>>> A verificar/instalar boto3 para upload MinIO...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "boto3", "-q"],
        check=True,
    )

    import boto3  # noqa: PLC0415

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    bucket = "warehouse"
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
        print(f"    Bucket '{bucket}' criado.")

    uploads = [
        (raw_dir / "consumo-total-nacional.csv",
         "raw/consumo-total-nacional.csv"),
        (raw_dir / "Day-ahead Market Prices_20230101_20260311.csv",
         "raw/Day-ahead Market Prices_20230101_20260311.csv"),
    ]
    for local_path, s3_key in uploads:
        if not local_path.exists():
            raise SystemExit(f"Erro: ficheiro raw não encontrado: {local_path}")
        print(f"    Upload: {local_path.name} -> s3://{bucket}/{s3_key}")
        s3.upload_file(str(local_path), bucket, s3_key)

    print(">>> Upload de CSVs raw para MinIO concluído.")


# ---------------------------------------------------------------------------
# pyflyte local runner — executa workflow via `pyflyte run` no processo local
#   (sem cluster remoto; equivalente a pyflyte run sem --remote)
# ---------------------------------------------------------------------------

def pyflyte_local(
    venv_python: Path,
    pipeline_root: Path,
    workflow_file: str,
    workflow_name: str,
    params: dict[str, str] | None = None,
) -> None:
    cmd = [
        str(venv_python), "-m", "flytekit.clis.sdk_in_container.pyflyte",
        "run",
        workflow_file,
        workflow_name,
    ]
    for key, value in (params or {}).items():
        cmd += [f"--{key}", value]
    run(cmd, cwd=pipeline_root)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestrador Medallion consumo_preco (DP-02 Static)"
    )
    parser.add_argument("--build",        action="store_true", help="faz --build no compose up")
    parser.add_argument("--skip-docker",  action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",     action="store_true",
                        help="salta a aplicação de DDL (tabelas já existem)")
    parser.add_argument("--skip-upload",  action="store_true",
                        help="salta upload dos CSVs para MinIO (já lá estão)")
    parser.add_argument("--no-quality",   action="store_true", help="salta os quality gates")
    parser.add_argument("--year",  type=int, help="Ano a processar (ex: 2024)")
    parser.add_argument("--month", type=int, help="Mês a processar (1-12)")
    args = parser.parse_args()

    if (args.year is None) != (args.month is None):
        raise SystemExit("Erro: --year e --month têm de ser usados em conjunto.")

    month_mode = args.year is not None
    process_date = f"{args.year}-{args.month:02d}-01" if month_mode else None

    pipeline_root = Path(__file__).resolve().parent
    repo_root     = pipeline_root.parent.parent.parent

    compose_file = repo_root / "01_docker_stack" / "docker-compose.yml"
    raw_dir      = pipeline_root / "01_bronze" / "data" / "raw"
    bronze_sql   = pipeline_root / "01_bronze" / "bronze_consumo_precos_trino.sql"
    silver_sql   = pipeline_root / "02_silver" / "sql" / "silver_consumo_precos_trino.sql"
    gold_sql     = pipeline_root / "03_gold"   / "sql" / "gold_consumo_precos_trino.sql"
    quality_dir  = pipeline_root / "04_quality" / "sql"

    must_exist(compose_file, "docker-compose.yml")
    must_exist(raw_dir,      "pasta com CSVs raw (01_bronze/data/raw/)")
    if not args.skip_ddl:
        must_exist(bronze_sql, "DDL Bronze SQL")
        must_exist(silver_sql, "DDL Silver SQL")
        must_exist(gold_sql,   "DDL Gold SQL")

    if shutil.which("docker") is None:
        raise SystemExit("Erro: comando 'docker' não encontrado no PATH.")

    python_cmd = sys.executable
    if not python_cmd or "WindowsApps" in python_cmd or not Path(python_cmd).exists():
        raise SystemExit(
            "Python inválido. Usa o executável real do Python, não o alias WindowsApps."
        )

    venv_python = create_local_venv(pipeline_root, python_cmd)

    # -------------------------------------------------------------------------
    # pip install
    # -------------------------------------------------------------------------
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    requirements = pipeline_root / "workflows" / "requirements.txt"
    if requirements.exists():
        run([str(venv_python), "-m", "pip", "install", "-r", str(requirements), "-q"])
    else:
        run([str(venv_python), "-m", "pip", "install",
             "flytekit", "trino", "boto3", "pandas", "-q"])

    # -------------------------------------------------------------------------
    # Docker compose up
    # -------------------------------------------------------------------------
    if not args.skip_docker:
        ensure_docker_engine_running()
        compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
        if args.build:
            compose_up.append("--build")
        run(compose_up)
        wait_for_trino(compose_file)
    else:
        print("\n>>> --skip-docker: compose up ignorado.")

    # -------------------------------------------------------------------------
    # FASE 0 — DDL (Bronze + Silver + Gold)
    # -------------------------------------------------------------------------
    if not args.skip_ddl:
        print("\n" + "=" * 60)
        print("FASE 0 — DDL (schemas e tabelas Iceberg)")
        print("=" * 60)
        apply_ddl(compose_file, bronze_sql, "Bronze")
        apply_ddl(compose_file, silver_sql, "Silver")
        apply_ddl(compose_file, gold_sql,   "Gold")
    else:
        print("\n>>> --skip-ddl: DDL ignorado.")

    # -------------------------------------------------------------------------
    # FASE 0.5 — Upload CSVs raw para MinIO
    # -------------------------------------------------------------------------
    if not args.skip_upload:
        print("\n" + "=" * 60)
        print("FASE 0.5 — Upload CSVs raw para MinIO (warehouse/raw/)")
        print("=" * 60)
        upload_raw_csvs_to_minio(raw_dir)
    else:
        print("\n>>> --skip-upload: upload de CSVs ignorado.")

    # Variáveis de ambiente para as tasks Flyte locais
    os.environ.setdefault("TRINO_HOST",            "localhost")
    os.environ.setdefault("TRINO_PORT",            "8080")
    os.environ.setdefault("MINIO_ENDPOINT",        "http://localhost:9000")
    os.environ.setdefault("MINIO_ACCESS_KEY",      "minioadmin")
    os.environ.setdefault("MINIO_SECRET_KEY",      "minioadmin")
    os.environ.setdefault("RAW_BUCKET",            "warehouse")

    def wf(workflow_file: str, workflow_name: str, params: dict[str, str] | None = None) -> None:
        pyflyte_local(venv_python, pipeline_root,
                      f"workflows/{workflow_file}", workflow_name, params)

    # -------------------------------------------------------------------------
    # FASE 1 — Bronze ingest
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("FASE 1 — Bronze ingest COMPLETO")
    print("=" * 60)
    wf("flyte_ingest_bronze.py", "ingest_bronze_full")

    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Bronze")
        print("=" * 60)
        bronze_checks = quality_dir / "01_bronze_checks.sql"
        if bronze_checks.exists():
            run_quality_checks(compose_file, bronze_checks, "Bronze")
        else:
            print(f"    [AVISO] {bronze_checks} não encontrado — a saltar Bronze checks.")

    # -------------------------------------------------------------------------
    # FASE 2 — Bronze → Silver
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    if month_mode:
        print(f"FASE 2 — Bronze → Silver (process_date={process_date})")
    else:
        print("FASE 2 — Bronze → Silver COMPLETO")
    print("=" * 60)

    if month_mode:
        wf("flyte_bronze_to_silver.py", "bronze_to_silver",
           {"process_date": process_date})
    else:
        wf("flyte_bronze_to_silver.py", "bronze_to_silver_full")

    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Silver")
        print("=" * 60)
        silver_checks = quality_dir / "02_silver_checks.sql"
        if silver_checks.exists():
            run_quality_checks(compose_file, silver_checks, "Silver")
        else:
            print(f"    [AVISO] {silver_checks} não encontrado — a saltar Silver checks.")

    # -------------------------------------------------------------------------
    # FASE 3 — Silver → Gold (sempre full — lag/rolling window functions exigem histórico completo)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("FASE 3 — Silver → Gold COMPLETO")
    print("=" * 60)
    wf("flyte_silver_to_gold.py", "silver_to_gold_full")

    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Gold")
        print("=" * 60)
        gold_checks = quality_dir / "03_gold_checks.sql"
        if gold_checks.exists():
            run_quality_checks(compose_file, gold_checks, "Gold")
        else:
            print(f"    [AVISO] {gold_checks} não encontrado — a saltar Gold checks.")

    # -------------------------------------------------------------------------
    # Validação final
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("VALIDAÇÃO FINAL — contagem e intervalo das tabelas Gold")
    print("=" * 60)
    validation_sql = (
        "SELECT 'dp_energy_market_hourly' AS tabela, COUNT(*) AS total_linhas, "
        "CAST(MIN(ts_utc) AS VARCHAR) AS inicio, CAST(MAX(ts_utc) AS VARCHAR) AS fim "
        "FROM iceberg.gold.dp_energy_market_hourly "
        "UNION ALL "
        "SELECT 'feat_load_forecasting_hourly', COUNT(*), "
        "CAST(MIN(ts_utc) AS VARCHAR), CAST(MAX(ts_utc) AS VARCHAR) "
        "FROM iceberg.gold.feat_load_forecasting_hourly;"
    )
    trino_execute(compose_file, validation_sql)

    modo = f"mês {args.year}-{args.month:02d}" if month_mode else "todo o período disponível"
    print(f"\n{'=' * 60}")
    print("Pipeline Medallion consumo_preco (DP-02 Static) concluída com sucesso!")
    print(f"  Modo     : {modo}")
    print(f"  Bronze   : bronze.consumo_raw + bronze.preco_raw")
    print(f"  Silver   : silver.consumo_hourly + silver.preco_hourly")
    print(f"  Gold     : gold.dp_energy_market_hourly + gold.feat_load_forecasting_hourly")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou (exit code {exc.returncode})", file=sys.stderr)
        sys.exit(exc.returncode)
