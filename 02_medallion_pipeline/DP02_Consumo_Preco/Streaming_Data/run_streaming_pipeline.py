#!/usr/bin/env python3
"""
Orquestrador - Streaming_Data pipeline (DP-02 API).

Obtém dados de consumo e preços via ENTSO-E Transparency Platform
e executa a pipeline Medallion completa:
  Bronze (API fetch) -> Quality -> Silver -> Quality -> Gold -> Quality

Fontes (ENTSO-E Transparency Platform -- transparency.entsoe.eu):
  - Consumo horario PT : Actual Total Load query_load('PT')       -- MW horario
  - Precos day-ahead PT: query_day_ahead_prices('PT')             -- EUR/MWh
  - Precos day-ahead ES: query_day_ahead_prices('ES')             -- EUR/MWh (novo!)

PRE-REQUISITO: variavel de ambiente ENTSOE_TOKEN
  PowerShell : $env:ENTSOE_TOKEN = "<o-teu-token>"
  Linux/Mac  : export ENTSOE_TOKEN=<o-teu-token>

  Token gratuito: email para transparency@entsoe.eu -- 'RESTful API access' (~3 dias)

Tabelas Iceberg com sufixo _api coexistem com o pipeline estatico.

Exemplos:
    python run_streaming_pipeline.py --days 7 --skip-docker
    python run_streaming_pipeline.py --start 2024-01-01 --end 2024-12-31 --skip-docker
    python run_streaming_pipeline.py --today --skip-docker --no-quality
    python run_streaming_pipeline.py --full --skip-docker --skip-ddl
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Workflows registry — centraliza nomes de ficheiros e funções Flyte
# ---------------------------------------------------------------------------

WORKFLOWS: dict[str, tuple[str, str]] = {
    "bronze_entsoe":       ("flyte_fetch_bronze_api.py",          "fetch_bronze_api"),
    "bronze_ec":           ("flyte_fetch_bronze_energycharts.py",  "fetch_bronze_energycharts"),
    "silver_full":         ("flyte_bronze_to_silver.py",           "bronze_to_silver_api_full"),
    "silver_incremental":  ("flyte_bronze_to_silver.py",           "bronze_to_silver_api"),
    "gold_full":           ("flyte_silver_to_gold.py",             "silver_to_gold_api_full"),
    "gold_incremental":    ("flyte_silver_to_gold.py",             "silver_to_gold_api_incremental"),
    "quality_bronze":      ("flyte_quality_checks.py",             "quality_gate_bronze_api"),
    "quality_silver":      ("flyte_quality_checks.py",             "quality_gate_silver_api"),
    "quality_gold":        ("flyte_quality_checks.py",             "quality_gate_gold_api"),
}

# ---------------------------------------------------------------------------
# Constantes operacionais — versão, SLA, auditoria
# ---------------------------------------------------------------------------

PIPELINE_VERSION    = "1.1.0"
MAX_RUNTIME_MINUTES = 45   # SLA: duração máxima esperada da pipeline completa
MAX_FRESHNESS_DAYS  = 7    # SLA: atraso máximo tolerado no end_date pedido


# ---------------------------------------------------------------------------
# Helpers de subprocess
# ---------------------------------------------------------------------------

def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict | None = None,
    timeout: int = 600,
) -> None:
    """Executa um comando externo e lança exceção se o exit code não for zero."""
    log.info("$ %s", " ".join(str(c) for c in cmd))
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd)


def must_exist(path: Path, description: str) -> None:
    """Aborta com mensagem legível se um ficheiro ou pasta obrigatório não existir."""
    if not path.exists():
        raise SystemExit(f"Erro: {description} não encontrado em: {path}")


# ---------------------------------------------------------------------------
# Concorrência — impede execuções paralelas da mesma pipeline
# ---------------------------------------------------------------------------

def acquire_pipeline_lock(repo_root: Path):
    """Retorna o ficheiro de lock aberto. Aborta se outra instância estiver a correr."""
    lock_path = repo_root / ".streaming_pipeline.lock"
    lock_fh = open(lock_path, "w")
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(lock_fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_fh.close()
        raise SystemExit(
            "Erro: outra instância do pipeline já está a correr. "
            f"Se não for o caso, apaga {lock_path} manualmente."
        )
    return lock_fh


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

def docker_engine_running() -> bool:
    try:
        return subprocess.run(
            ["docker", "info"], text=True, capture_output=True, timeout=10
        ).returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def try_start_docker_engine() -> bool:
    system = platform.system().lower()
    start_commands: list[list[str]] = []
    if system == "linux":
        start_commands = [["systemctl", "start", "docker"]]
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
        subprocess.run(cmd, text=True, capture_output=True)
        for _ in range(20):
            if docker_engine_running():
                return True
            time.sleep(1)
    return docker_engine_running()


def ensure_docker_engine_running() -> None:
    if docker_engine_running():
        return
    if try_start_docker_engine():
        return
    raise SystemExit("Erro: Docker Engine não está a correr. Arranca o Docker e repete.")


def wait_for_trino(compose_file: Path, attempts: int = 40, sleep_seconds: int = 3) -> None:
    import urllib.request
    import urllib.error

    base_cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino",
    ]

    # Fase 1: aguardar que o Trino CLI (via docker exec) responda — confirma o processo interno
    trino_ready = False
    for attempt in range(1, attempts + 1):
        log.info("Trino CLI disponível? (%d/%d)", attempt, attempts)
        result = subprocess.run(
            base_cmd + ["--execute", "SELECT 1;"],
            text=True, capture_output=True, timeout=10,
        )
        if result.returncode == 0:
            trino_ready = True
            break
        time.sleep(sleep_seconds)
    if not trino_ready:
        raise SystemExit("Erro: Trino não ficou disponível dentro do tempo esperado.")

    # Fase 2: aguardar que o endpoint HTTP localhost:8080 responda sem SERVER_STARTING_UP
    # O cliente Python trino usa HTTP direto — é necessário verificar separadamente.
    http_ready = False
    for attempt in range(1, attempts + 1):
        log.info("Trino HTTP (localhost:8080) disponível? (%d/%d)", attempt, attempts)
        try:
            req = urllib.request.Request(
                "http://localhost:8080/v1/info",
                headers={"X-Trino-User": "admin"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                import json as _json
                info = _json.loads(resp.read())
                if info.get("starting") is False:
                    http_ready = True
                    break
                log.info("Trino ainda a inicializar (starting=true) — a aguardar...")
        except Exception:
            pass
        time.sleep(sleep_seconds)
    if not http_ready:
        raise SystemExit(
            "Erro: Trino HTTP (localhost:8080) não ficou disponível dentro do tempo esperado."
        )

    # Fase 3: verificar que o catálogo iceberg está funcional (depende de Hive Metastore + MinIO)
    result = subprocess.run(
        base_cmd + ["--execute", "SHOW SCHEMAS FROM iceberg;"],
        text=True, capture_output=True, timeout=30,
    )
    if result.returncode != 0:
        raise SystemExit(
            "Erro: Trino online mas catálogo 'iceberg' não disponível. "
            "Verifica Hive Metastore e MinIO.\n" + result.stderr
        )
    log.info("Trino + catálogo iceberg OK.")


# ---------------------------------------------------------------------------
# Venv helper
# ---------------------------------------------------------------------------

def create_local_venv(repo_root: Path, base_python: str) -> Path:
    venv_dir = repo_root / ".venv_streaming_dp02"
    venv_python = (
        venv_dir / "Scripts" / "python.exe"
        if os.name == "nt"
        else venv_dir / "bin" / "python"
    )
    if not venv_python.exists():
        if venv_dir.exists():
            log.warning("Venv corrompido detectado — a recriar: %s", venv_dir)
            shutil.rmtree(venv_dir)
        log.info("A criar virtualenv em: %s", venv_dir)
        run([base_python, "-m", "venv", str(venv_dir)], timeout=120)
    return venv_python


def requirements_changed(venv_dir: Path, requirements: Path) -> bool:
    """Retorna True se requirements.txt mudou desde a última instalação.

    Persiste o hash em disco como efeito secundário — chamar duas vezes seguidas
    devolve False na segunda, mesmo que a primeira tenha devolvido True.
    """
    hash_file = venv_dir / ".requirements_hash"
    current_hash = hashlib.md5(requirements.read_bytes()).hexdigest()
    if hash_file.exists() and hash_file.read_text(encoding="utf-8").strip() == current_hash:
        return False
    hash_file.write_text(current_hash, encoding="utf-8")
    return True


# ---------------------------------------------------------------------------
# DDL helper
# ---------------------------------------------------------------------------

def trino_exec(compose_file: Path, sql: str, timeout: int = 60) -> int:
    """Executa SQL no Trino CLI via docker exec. Devolve o exit code (não lança exceção)."""
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "exec", "-T", "trino", "trino",
         "--execute", sql],
        text=True, capture_output=True, timeout=timeout,
    )
    return result.returncode


def clean_streaming_tables(compose_file: Path) -> None:
    tables = [
        "iceberg.bronze.consumo_api_raw",
        "iceberg.bronze.preco_api_raw",
        "iceberg.silver.consumo_api_hourly",
        "iceberg.silver.preco_api_hourly",
        "iceberg.gold.dp_energy_market_api_hourly",
        "iceberg.gold.feat_load_forecasting_api_hourly",
    ]
    log.info("=" * 60)
    log.info("CLEANUP — DROP tabelas _api (Iceberg remove dados do MinIO)")
    log.info("=" * 60)
    for table in tables:
        log.info("  DROP TABLE IF EXISTS %s", table)
        trino_exec(compose_file, f"DROP TABLE IF EXISTS {table};")
    log.info("Cleanup concluído.")


def apply_ddl(compose_file: Path, sql_file: Path, stage_name: str) -> None:
    """Executa o ficheiro SQL inteiro via stdin do Trino CLI.

    Usa --file /dev/stdin para evitar parsing frágil por split(';').
    Falhas de DDL são propagadas imediatamente — não há execução silenciosa.
    """
    log.info("DDL %s: %s", stage_name, sql_file.name)
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino", "--file", "/dev/stdin",
        ],
        input=sql_file.read_bytes(),
        capture_output=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Erro DDL '{stage_name}' ({sql_file.name}):\n"
            + result.stderr.decode(errors="replace")
        )
    log.info("DDL %s aplicado.", stage_name)


# ---------------------------------------------------------------------------
# Watermark — detecção do último dia já carregado no Bronze
# ---------------------------------------------------------------------------

def get_bronze_watermark(compose_file: Path) -> date | None:
    """Retorna MAX(process_date) de bronze.consumo_api_raw, ou None se a tabela estiver vazia."""
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino",
            "--execute",
            "SELECT CAST(MAX(process_date) AS VARCHAR) FROM iceberg.bronze.consumo_api_raw;",
        ],
        text=True, capture_output=True, timeout=30,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.strip().splitlines():
        cleaned = line.strip().strip('"')
        if re.match(r"\d{4}-\d{2}-\d{2}", cleaned):
            return date.fromisoformat(cleaned)
    return None


# ---------------------------------------------------------------------------
# pyflyte runner
# ---------------------------------------------------------------------------

def pyflyte_run(
    venv_python: Path,
    workflows_dir: Path,
    workflow_file: str,
    workflow_name: str,
    params: dict[str, str],
) -> None:
    cmd = [
        str(venv_python), "-m", "flytekit.clis.sdk_in_container.pyflyte",
        "run",
        str(workflows_dir / workflow_file),
        workflow_name,
    ]
    for key, value in params.items():
        cmd += [f"--{key}", value]
    run(cmd, cwd=workflows_dir, timeout=600)


def pyflyte_run_with_retry(
    venv_python: Path,
    workflows_dir: Path,
    workflow_file: str,
    workflow_name: str,
    params: dict[str, str],
    *,
    max_attempts: int = 3,
) -> None:
    """Executa pyflyte_run com retry e backoff exponencial (2s, 4s, …)."""
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            pyflyte_run(venv_python, workflows_dir, workflow_file, workflow_name, params)
            return
        except subprocess.CalledProcessError as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            wait = 2 ** attempt
            log.warning(
                "pyflyte '%s' tentativa %d/%d falhou — retry em %ds...",
                workflow_name, attempt, max_attempts, wait,
            )
            time.sleep(wait)
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Audit helpers — lineage, observabilidade, compaction
# ---------------------------------------------------------------------------

class _RunLog(logging.LoggerAdapter):
    """Logger que prefixa todas as mensagens com o run_id curto."""
    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        return f"[{self.extra['run_id']}] {msg}", kwargs


def get_row_count(compose_file: Path, table: str) -> int:
    """Retorna COUNT(*) de uma tabela Iceberg via Trino CLI. -1 em caso de erro."""
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino",
            "--execute", f"SELECT COUNT(*) FROM {table};",
        ],
        text=True, capture_output=True, timeout=30,
    )
    if result.returncode != 0:
        return -1
    for line in result.stdout.strip().splitlines():
        cleaned = line.strip().strip('"')
        if cleaned.isdigit():
            return int(cleaned)
    return -1


def _ts_str(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S") + " UTC"


def write_audit_record(compose_file: Path, record: dict) -> None:
    """INSERT de um registo completo na tabela iceberg.audit.pipeline_runs."""
    err = record.get("error_message", "").replace("'", "''")[:500]
    sql = (
        "INSERT INTO iceberg.audit.pipeline_runs VALUES ("
        f"'{record['run_id']}', "
        f"'{record['pipeline_name']}', "
        f"'{record['pipeline_version']}', "
        f"TIMESTAMP '{record['start_ts']}', "
        f"TIMESTAMP '{record['end_ts']}', "
        f"{record['duration_seconds']:.1f}, "
        f"'{record['status']}', "
        f"{record['rows_bronze']}, "
        f"{record['rows_silver']}, "
        f"{record['rows_gold']}, "
        f"'{record['source']}', "
        f"'{record['param_start_date']}', "
        f"'{record['param_end_date']}', "
        f"'{err}'"
        ");"
    )
    rc = trino_exec(compose_file, sql, timeout=30)
    if rc != 0:
        log.warning("Falha ao persistir registo de auditoria (run_id=%s)", record["run_id"])


def write_lineage_records(compose_file: Path, run_id: str) -> None:
    """INSERT de lineage upstream→downstream para esta execução."""
    lineage = [
        ("bronze.consumo_api_raw",    "silver.consumo_api_hourly"),
        ("bronze.preco_api_raw",      "silver.preco_api_hourly"),
        ("silver.consumo_api_hourly", "gold.dp_energy_market_api_hourly"),
        ("silver.preco_api_hourly",   "gold.dp_energy_market_api_hourly"),
        ("silver.consumo_api_hourly", "gold.feat_load_forecasting_api_hourly"),
        ("silver.preco_api_hourly",   "gold.feat_load_forecasting_api_hourly"),
    ]
    now_ts = _ts_str(datetime.datetime.now(datetime.timezone.utc))
    for upstream, downstream in lineage:
        sql = (
            f"INSERT INTO iceberg.audit.dataset_lineage VALUES "
            f"('{run_id}', '{upstream}', '{downstream}', TIMESTAMP '{now_ts}');"
        )
        trino_exec(compose_file, sql, timeout=15)


def run_iceberg_optimize(compose_file: Path) -> None:
    """Compaction Iceberg nas tabelas Gold — resolve o small-files problem de ingestões incrementais."""
    tables = [
        "iceberg.gold.dp_energy_market_api_hourly",
        "iceberg.gold.feat_load_forecasting_api_hourly",
    ]
    for table in tables:
        log.info("Iceberg optimize: %s", table)
        rc = trino_exec(compose_file, f"ALTER TABLE {table} EXECUTE optimize;", timeout=120)
        if rc != 0:
            log.warning("optimize falhou para %s (não fatal)", table)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestrador Streaming_Data pipeline (DP-02 API)"
    )

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--days",  type=int, metavar="N",
                     help="Últimos N dias (default: 7)")
    grp.add_argument("--today", action="store_true",
                     help="Apenas hoje")
    grp.add_argument("--full",  action="store_true",
                     help="Período máximo disponível na API (~2 anos)")

    parser.add_argument("--start", type=str, metavar="YYYY-MM-DD",
                        help="Data início (usar com --end)")
    parser.add_argument("--end",   type=str, metavar="YYYY-MM-DD",
                        help="Data fim (usar com --start)")

    parser.add_argument(
        "--source",
        choices=["entsoe", "energycharts"],
        default="energycharts",
        help=(
            "Fonte de dados para o fetch Bronze (default: energycharts). "
            "Sem autenticação, redistribui ENTSO-E via Fraunhofer ISE. "
            "Use 'entsoe' com ENTSOE_TOKEN definido para acesso direto à plataforma."
        ),
    )

    parser.add_argument("--build",       action="store_true", help="faz --build no compose up")
    parser.add_argument("--skip-docker", action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",    action="store_true", help="salta a aplicação de DDL")
    parser.add_argument("--clean",       action="store_true", help="DROP das 6 tabelas _api antes de recriar (limpa dados MinIO)")
    parser.add_argument("--no-quality",  action="store_true", help="salta os quality gates")

    args = parser.parse_args()

    today = date.today()

    # watermark_mode: a data de início só pode ser calculada após DDL estar aplicado,
    # porque a tabela bronze pode ainda não existir neste ponto. O start_date
    # é um placeholder e é substituído mais abaixo, após a query ao Bronze.
    watermark_mode = False
    if args.start and args.end:
        start_date = date.fromisoformat(args.start)
        end_date   = date.fromisoformat(args.end)
    elif args.today:
        start_date = end_date = today
    elif args.full:
        start_date = date(2022, 1, 1)
        end_date   = today
    elif args.days:
        end_date   = today
        start_date = today - timedelta(days=args.days - 1)
    else:
        watermark_mode = True
        start_date = date(2022, 1, 1)
        end_date   = today

    # use_full_rebuild: Silver e Gold fazem DELETE completo + reload histórico
    use_full_rebuild = args.full or args.clean

    start_str = start_date.isoformat()
    end_str   = end_date.isoformat()

    # --- Identidade e observabilidade desta execução ---
    run_id         = str(uuid.uuid4())
    run_start_mono = time.monotonic()
    pipeline_start = datetime.datetime.now(datetime.timezone.utc)
    run_log        = _RunLog(log, {"run_id": run_id[:8]})
    run_log.info(
        "Pipeline iniciada — run_id=%s  período=%s→%s  fonte=%s  versão=%s",
        run_id, start_str, end_str, args.source, PIPELINE_VERSION,
    )

    # SLA: freshness check — avisar se o período pedido está muito no passado
    days_stale = (today - end_date).days
    if days_stale > MAX_FRESHNESS_DAYS:
        run_log.warning(
            "FRESHNESS WARN: end_date=%s está %d dias no passado (limite recomendado: %d dias)",
            end_str, days_stale, MAX_FRESHNESS_DAYS,
        )

    streaming_root = Path(__file__).resolve().parent
    repo_root      = streaming_root.parent.parent.parent

    compose_file  = repo_root / "01_docker_stack" / "docker-compose.yml"
    workflows_dir = streaming_root / "workflows"

    bronze_sql = streaming_root / "01_bronze" / "bronze_streaming_ddl.sql"
    silver_sql = streaming_root / "02_silver" / "sql" / "silver_streaming_ddl.sql"
    gold_sql   = streaming_root / "03_gold"   / "sql" / "gold_streaming_ddl.sql"

    must_exist(compose_file,  "docker-compose.yml")
    must_exist(workflows_dir, "pasta workflows/")
    if not args.skip_ddl:
        must_exist(bronze_sql, "DDL Bronze SQL")
        must_exist(silver_sql, "DDL Silver SQL")
        must_exist(gold_sql,   "DDL Gold SQL")

    if shutil.which("docker") is None:
        raise SystemExit("Erro: comando 'docker' não encontrado no PATH.")

    python_cmd = sys.executable
    if not python_cmd or "WindowsApps" in python_cmd or not Path(python_cmd).exists():
        raise SystemExit("Python inválido. Usa o executável real do Python.")

    # --- Lock de concorrência ---
    pipeline_lock = acquire_pipeline_lock(repo_root)
    run_log.debug("Lock adquirido: %s", pipeline_lock.name)

    rows_bronze = rows_silver = rows_gold = -1
    error_message = ""
    status = "FAILED"

    try:
        # --- venv ---
        venv_dir    = repo_root / ".venv_streaming_dp02"
        venv_python = create_local_venv(repo_root, python_cmd)
        requirements = workflows_dir / "requirements.txt"
        if requirements.exists() and requirements_changed(venv_dir, requirements):
            run_log.info("requirements.txt alterado — a instalar dependências...")
            run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"], timeout=120)
            try:
                run([str(venv_python), "-m", "pip", "install", "--no-cache-dir", "-r", str(requirements)], timeout=300)
            except (subprocess.CalledProcessError, KeyboardInterrupt):
                run_log.warning("pip install falhou — a recriar venv e a tentar novamente...")
                shutil.rmtree(venv_dir, ignore_errors=True)
                run([python_cmd, "-m", "venv", str(venv_dir)], timeout=120)
                run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"], timeout=120)
                run([str(venv_python), "-m", "pip", "install", "--no-cache-dir", "-r", str(requirements)], timeout=300)
            requirements_changed(venv_dir, requirements)
        else:
            run_log.info("Dependências já instaladas e sem alterações — a saltar pip install.")

        # --- Docker ---
        if not args.skip_docker:
            ensure_docker_engine_running()
            compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
            if args.build:
                compose_up.append("--build")
            run(compose_up, timeout=300)
        else:
            run_log.info("--skip-docker: compose up ignorado.")
        # Sempre verificar que Trino está pronto (HTTP + iceberg), mesmo com --skip-docker.
        # O cliente Python trino usa HTTP direto — pode falhar se Trino ainda estiver a inicializar.
        wait_for_trino(compose_file)

        # --- Cleanup (opcional) ---
        if args.clean:
            clean_streaming_tables(compose_file)

        # --- DDL: auditoria (sempre) + medallion (controlado por --skip-ddl) ---
        run_log.info("=" * 60)
        run_log.info("FASE 0 - DDL")
        run_log.info("=" * 60)
        audit_sql = streaming_root / "05_audit" / "sql" / "audit_ddl.sql"
        apply_ddl(compose_file, audit_sql, "Audit")
        if not args.skip_ddl:
            apply_ddl(compose_file, bronze_sql, "Bronze API")
            apply_ddl(compose_file, silver_sql, "Silver API")
            apply_ddl(compose_file, gold_sql,   "Gold API")
        else:
            run_log.info("--skip-ddl: DDL medallion ignorado.")

        # --- Watermark: resolver start_date após DDL (tabela já existe) ---
        if watermark_mode:
            watermark = get_bronze_watermark(compose_file)
            if watermark is None:
                run_log.info("Primeira execução — histórico completo desde %s", start_date)
            else:
                start_date = watermark + timedelta(days=1)
                start_str  = start_date.isoformat()
                run_log.info(
                    "Modo incremental: watermark=%s → start_date=%s", watermark, start_date
                )
            if start_date > end_date:
                run_log.info(
                    "Dados já atualizados até %s. Nada a processar.", end_date
                )
                status = "SUCCESS"
                return

        run_log.info("=" * 60)
        run_log.info("Pipeline Streaming_Data (DP-02 API) - %s -> %s", start_str, end_str)
        run_log.info("=" * 60)

        # --- FASE 1: Bronze fetch ---
        run_log.info("=" * 60)
        run_log.info("FASE 1 - Bronze fetch (%s) (%s -> %s)", args.source.upper(), start_str, end_str)
        run_log.info("=" * 60)

        if args.source == "entsoe":
            if not os.environ.get("ENTSOE_TOKEN"):
                raise SystemExit(
                    "\n[ERRO] ENTSOE_TOKEN nao definido.\n"
                    "Token gratuito ENTSO-E Transparency Platform:\n"
                    "  1. Email: transparency@entsoe.eu -- assunto 'RESTful API access'\n"
                    "  2. Aguarda ~3 dias uteis\n"
                    "  3. PowerShell: $env:ENTSOE_TOKEN = '<o-teu-token>'\n"
                    "     Linux/Mac : export ENTSOE_TOKEN=<o-teu-token>\n"
                    "\nAlternativa sem token:\n"
                    "  python run_streaming_pipeline.py --source energycharts --skip-docker --days 7\n"
                )
            bronze_wf = WORKFLOWS["bronze_entsoe"]
        else:
            run_log.info("[INFO] Energy-Charts: sem autenticacao necessaria (Fraunhofer ISE).")
            bronze_wf = WORKFLOWS["bronze_ec"]

        # Períodos longos são divididos em chunks anuais para evitar timeouts da API
        # e limitar o tamanho das respostas JSON (~180 dias é o ponto de rutura observado).
        CHUNK_THRESHOLD_DAYS = 180
        total_days = (end_date - start_date).days
        if total_days > CHUNK_THRESHOLD_DAYS:
            chunk_start = start_date
            while chunk_start <= end_date:
                chunk_end = min(date(chunk_start.year, 12, 31), end_date)
                run_log.info("Chunk: %s -> %s", chunk_start, chunk_end)
                pyflyte_run_with_retry(
                    venv_python, workflows_dir, *bronze_wf,
                    {"start_date": chunk_start.isoformat(), "end_date": chunk_end.isoformat()},
                )
                chunk_start = date(chunk_start.year + 1, 1, 1)
        else:
            pyflyte_run_with_retry(
                venv_python, workflows_dir, *bronze_wf,
                {"start_date": start_str, "end_date": end_str},
            )

        rows_bronze = (
            get_row_count(compose_file, "iceberg.bronze.consumo_api_raw")
            + get_row_count(compose_file, "iceberg.bronze.preco_api_raw")
        )
        run_log.info("Bronze ingerido: %d linhas totais (consumo + preço)", rows_bronze)

        # --- Quality gate Bronze ---
        if not args.no_quality:
            run_log.info("=" * 60)
            run_log.info("QUALITY GATE - Bronze API")
            run_log.info("=" * 60)
            pyflyte_run_with_retry(venv_python, workflows_dir, *WORKFLOWS["quality_bronze"], {})

        # --- FASE 2: Silver transform ---
        run_log.info("=" * 60)
        if use_full_rebuild:
            run_log.info("FASE 2 - Silver transform COMPLETO")
            run_log.info("=" * 60)
            pyflyte_run_with_retry(venv_python, workflows_dir, *WORKFLOWS["silver_full"], {})
        else:
            total_days = (end_date - start_date).days + 1
            run_log.info("FASE 2 - Silver transform INCREMENTAL (%d dias)", total_days)
            run_log.info("=" * 60)
            current = start_date
            while current <= end_date:
                pyflyte_run_with_retry(
                    venv_python, workflows_dir, *WORKFLOWS["silver_incremental"],
                    {"process_date": current.isoformat()},
                )
                current += timedelta(days=1)

        rows_silver = (
            get_row_count(compose_file, "iceberg.silver.consumo_api_hourly")
            + get_row_count(compose_file, "iceberg.silver.preco_api_hourly")
        )
        run_log.info("Silver transformado: %d linhas totais", rows_silver)

        # --- Quality gate Silver ---
        if not args.no_quality:
            run_log.info("=" * 60)
            run_log.info("QUALITY GATE - Silver API")
            run_log.info("=" * 60)
            pyflyte_run_with_retry(venv_python, workflows_dir, *WORKFLOWS["quality_silver"], {})

        # --- FASE 3: Gold transform ---
        run_log.info("=" * 60)
        if use_full_rebuild:
            run_log.info("FASE 3 - Gold transform COMPLETO")
            run_log.info("=" * 60)
            pyflyte_run_with_retry(venv_python, workflows_dir, *WORKFLOWS["gold_full"], {})
        else:
            run_log.info("FASE 3 - Gold transform INCREMENTAL (janela desde %s)", start_date)
            run_log.info("=" * 60)
            pyflyte_run_with_retry(
                venv_python, workflows_dir, *WORKFLOWS["gold_incremental"],
                {"since_date": start_str},
            )

        rows_gold = get_row_count(compose_file, "iceberg.gold.dp_energy_market_api_hourly")
        run_log.info("Gold produzido: %d linhas em dp_energy_market_api_hourly", rows_gold)

        # --- Quality gate Gold ---
        if not args.no_quality:
            run_log.info("=" * 60)
            run_log.info("QUALITY GATE - Gold API")
            run_log.info("=" * 60)
            pyflyte_run_with_retry(venv_python, workflows_dir, *WORKFLOWS["quality_gold"], {})

        # --- Compaction Iceberg (resolve small-files acumulados por ingestões incrementais) ---
        run_log.info("=" * 60)
        run_log.info("MANUTENCAO - Iceberg optimize (compaction)")
        run_log.info("=" * 60)
        run_iceberg_optimize(compose_file)

        # --- Validação final ---
        run_log.info("=" * 60)
        run_log.info("VALIDACAO FINAL - tabelas Gold API")
        run_log.info("=" * 60)
        validation_sql = (
            "SELECT 'dp_energy_market_api_hourly' AS tabela, COUNT(*) AS total_linhas, "
            "CAST(MIN(ts_utc) AS VARCHAR) AS inicio, CAST(MAX(ts_utc) AS VARCHAR) AS fim "
            "FROM iceberg.gold.dp_energy_market_api_hourly "
            "UNION ALL "
            "SELECT 'feat_load_forecasting_api_hourly', COUNT(*), "
            "CAST(MIN(ts_utc) AS VARCHAR), CAST(MAX(ts_utc) AS VARCHAR) "
            "FROM iceberg.gold.feat_load_forecasting_api_hourly;"
        )
        # Validação só para display — não falha a pipeline se a query tiver problemas.
        subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "exec", "-T", "trino", "trino",
                "--execute", validation_sql,
            ],
            text=True,
            timeout=60,
        )

        status = "SUCCESS"
        fonte_label = (
            "Energy-Charts API (Fraunhofer ISE) — sem autenticação"
            if args.source == "energycharts"
            else "ENTSO-E Transparency Platform (token)"
        )
        run_log.info("=" * 60)
        run_log.info("Streaming_Data pipeline (DP-02 API) concluída com sucesso!")
        run_log.info("  Período  : %s -> %s", start_str, end_str)
        run_log.info("  Bronze   : bronze.consumo_api_raw + bronze.preco_api_raw  (%d linhas)", rows_bronze)
        run_log.info("  Silver   : silver.consumo_api_hourly + silver.preco_api_hourly  (%d linhas)", rows_silver)
        run_log.info("  Gold     : gold.dp_energy_market_api_hourly  (%d linhas)", rows_gold)
        run_log.info("  Fonte    : %s", fonte_label)
        run_log.info("  run_id   : %s", run_id)
        run_log.info("=" * 60)

    except Exception as exc:
        error_message = str(exc)[:500]
        raise

    finally:
        duration_s = time.monotonic() - run_start_mono
        end_ts     = datetime.datetime.now(datetime.timezone.utc)

        if duration_s > MAX_RUNTIME_MINUTES * 60:
            run_log.warning(
                "SLA BREACH: pipeline demorou %.0fs (limite: %d min)",
                duration_s, MAX_RUNTIME_MINUTES,
            )

        try:
            write_audit_record(compose_file, {
                "run_id":           run_id,
                "pipeline_name":    "dp02_streaming",
                "pipeline_version": PIPELINE_VERSION,
                "start_ts":         _ts_str(pipeline_start),
                "end_ts":           _ts_str(end_ts),
                "duration_seconds": duration_s,
                "status":           status,
                "rows_bronze":      max(rows_bronze, 0),
                "rows_silver":      max(rows_silver, 0),
                "rows_gold":        max(rows_gold, 0),
                "source":           args.source,
                "param_start_date": start_str,
                "param_end_date":   end_str,
                "error_message":    error_message,
            })
            if status == "SUCCESS":
                write_lineage_records(compose_file, run_id)
        except Exception as audit_exc:
            run_log.warning("Falha ao escrever auditoria (não fatal): %s", audit_exc)

        run_log.info(
            "Pipeline %s em %.0fs  (bronze=%d  silver=%d  gold=%d)",
            status, duration_s,
            max(rows_bronze, 0), max(rows_silver, 0), max(rows_gold, 0),
        )


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        log.error("Comando falhou (exit code %d)", exc.returncode)
        sys.exit(exc.returncode)
