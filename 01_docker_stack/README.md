# 01 — Docker Stack (Infraestrutura Local)

Infraestrutura completa do projecto MIBEL em Docker Compose. Fornece o lakehouse local (MinIO + Hive + Trino + Iceberg), o tracking de modelos ML (MLflow), o streaming (Redpanda) e a visualização (Grafana), com o Flyte Sandbox como orquestrador externo de workflows.

---

## Diagrama de Arquitectura

```
┌──────────────────────────────────────────────────────────────────────┐
│                       Docker Compose Stack                           │
│                                                                      │
│  ┌──────────┐    ┌──────────────────┐    ┌───────────────────────┐  │
│  │  MinIO   │───►│  Hive Metastore  │───►│        Trino          │  │
│  │  (S3)    │    │  (MariaDB)       │    │  catalogs: iceberg,   │  │
│  │:9000/9001│    │  :9083           │    │  hive, kafka, tpcds   │  │
│  └────┬─────┘    └──────────────────┘    └───────────┬───────────┘  │
│       │                                              │               │
│       │          ┌──────────────────┐    ┌───────────▼───────────┐  │
│       └─────────►│     MLflow       │    │      Grafana          │  │
│       │          │  (Postgres)      │    │  dashboards + Trino   │  │
│       │          │  :15000          │    │  :3300                │  │
│       │          └──────────────────┘    └───────────────────────┘  │
│       │                                                              │
│       │          ┌──────────────────┐                               │
│       └─────────►│    Redpanda      │                               │
│                  │  (Kafka-compat.) │                               │
│                  │  :19092          │                               │
│                  └──────────────────┘                               │
└──────────────────────────────────────────────────────────────────────┘
              ▲
              │  Flyte Sandbox (container externo — K3s interno)
              │  Acede via host.docker.internal:<porta>
```

**Fluxo de dados:**

```
Fontes externas (CSVs / Open-Meteo API / Redpanda)
        │
        ▼
   MinIO (s3://warehouse/)
        │
        ▼ Hive external tables (Bronze)
   Trino ──► Iceberg managed tables (Silver → Gold)
        │
        ├──► MLflow (artefactos de modelos ML)
        └──► Grafana (dashboards de visualização)
```

---

## Estrutura de Ficheiros

```
01_docker_stack/
├── docker-compose.yml              # Orquestrador principal (9 serviços)
├── hive/
│   └── conf/
│       ├── metastore-site.xml      # Hive Metastore → MinIO S3
│       ├── hive-site.xml
│       └── core-site.xml
├── trino/
│   └── etc/
│       ├── config.properties       # Coordinator, memória, discovery
│       ├── jvm.config              # JVM heap e GC
│       ├── node.properties         # Identificação do nó
│       ├── log.properties          # Nível de log por pacote
│       └── catalog/
│           ├── iceberg.properties  # Tabelas geridas Silver/Gold (ACID)
│           ├── hive.properties     # Tabelas externas Bronze (Parquet)
│           ├── kafka.properties    # Catálogo Redpanda/Kafka
│           ├── tpcds.properties    # Benchmark TPC-DS
│           └── tpch.properties     # Benchmark TPC-H
├── kafka_config/
│   ├── web_events.json             # Schema Trino para tópico web_events
│   └── producao_consumo_events.json # Schema Trino para tópico producao_consumo_events
├── mlflow/
│   └── Dockerfile                  # Imagem tead-mlflow:v3.10.1 (MLflow + psycopg2)
└── flyte/
    ├── Dockerfile                  # Patch sandbox Flyte (timeout Helm 30 min)
    └── flyte-core-overrides.yaml   # Helm overrides para flyte-core
```

> Os dashboards e datasources do Grafana estão em `04_application/grafana/` e são montados automaticamente pelo Compose via bind mounts.

---

## Serviços Docker Compose

| Serviço | Imagem | Função | Porta(s) |
|---|---|---|---|
| `minio` | `minio/minio:latest` | Object storage S3-compatível (Bronze + artefactos ML) | `9000`, `9001` |
| `mc` | `minio/mc:latest` | Bootstrap one-shot: cria buckets `warehouse`, `mlflow`, `flyte` | — |
| `metastore-db` | `mariadb:10.11` | Backend persistente do Hive Metastore | interno |
| `hive-metastore` | `bitsondatadev/hive-metastore` | Serviço Thrift de metadados para Trino | `9083` |
| `redpanda` | `redpandadata/redpanda:v23.2.1` | Broker Kafka-compatível | `19092` |
| `trino` | `trinodb/trino:468` | Motor de query SQL distribuído | `8080` |
| `mlflow-db` | `postgres:16` | Backend persistente do MLflow | interno |
| `mlflow` | `tead-mlflow:v3.10.1` (build local) | Tracking server + artifact store (MinIO) | `15000` |
| `grafana` | `grafana/grafana:11.1.0` | Dashboards de visualização (datasource Trino) | `3300` |

> **`mc`** termina após criar os buckets — comportamento esperado (bootstrapper one-shot).

---

## Catálogos Trino

| Catálogo | Tipo | Função |
|---|---|---|
| `iceberg` | Iceberg | Tabelas geridas Silver e Gold (ACID, time-travel) |
| `hive` | Hive Metastore | Tabelas externas Bronze (Parquet no MinIO) |
| `kafka` | Kafka | Leitura de tópicos Redpanda |
| `tpcds` | TPC-DS | Dados de benchmark |
| `tpch` | TPC-H | Dados de benchmark |

---

## Passo a Passo de Execução

### Pré-requisitos

- Docker Engine ≥ 20.10 e Docker Compose v2 (`docker compose`, não `docker-compose`)
- ≥ 8 GB RAM disponível para Docker
- ≥ 10 GB espaço livre em disco

### Arrancar o stack

```powershell
cd 01_docker_stack
docker compose up -d --build
```

O flag `--build` é necessário na primeira execução (imagem MLflow customizada). Execuções subsequentes podem omiti-lo.

### Verificar o estado

```powershell
docker compose ps
```

Todos os serviços (excepto `mc`, que termina após criar os buckets) devem mostrar `running`.

### Validar Trino

```powershell
docker compose exec trino trino --execute "SHOW CATALOGS;"
# Esperado: iceberg, hive, kafka, system, tpcds, tpch
```

### Validar tópicos expostos no catálogo Kafka

```powershell
docker compose exec trino trino --execute "SHOW TABLES FROM kafka.default;"
```

Esperado incluir:
- `web_events`
- `producao_consumo_events`

Se `producao_consumo_events` não aparecer, valide o ficheiro `kafka_config/producao_consumo_events.json` e reinicie o Trino:

```powershell
docker compose restart trino
```

### Parar o stack

```powershell
docker compose down          # preserva volumes (dados)
docker compose down -v       # reset completo (apaga dados)
```

---

## Endpoints

| Serviço | URL | Credenciais |
| --- | --- | --- |
| Trino UI | <http://localhost:8080> | — |
| MinIO Console | <http://localhost:9001> | minioadmin / minioadmin |
| MinIO S3 API | <http://localhost:9000> | minioadmin / minioadmin |
| MLflow | <http://localhost:15000> | — |
| Grafana | <http://localhost:3300> | admin / admin |
| Hive Metastore | `thrift://localhost:9083` | — |
| Redpanda | `localhost:19092` | — |

---

## Flyte Sandbox (orquestrador externo)

O Flyte Sandbox corre como container separado com K3s interno — não faz parte do Compose. A configuração está em `flyte/`.

```bash
flytectl demo start   # aguarda vários minutos (Helm + K3s)
# UI: http://localhost:30080
```

Os tasks Flyte acedem aos serviços Compose via `host.docker.internal:<porta>` (Docker Desktop Windows/macOS funciona nativamente; Linux requer `--add-host host.docker.internal:host-gateway`).

| Serviço | URL a partir dos tasks Flyte |
| --- | --- |
| MinIO S3 API | `http://host.docker.internal:9000` |
| MLflow | `http://host.docker.internal:15000` |
| Trino | `http://host.docker.internal:8080` |
| Hive Metastore | `thrift://host.docker.internal:9083` |

---

## Troubleshooting

**`mc` termina imediatamente** — comportamento esperado. Ver logs: `docker compose logs mc`.

**Trino sem catálogos** — Hive Metastore pode não ter terminado de arrancar. Reiniciar: `docker compose restart trino`.

**Trino mostra "unhealthy" no `docker compose ps`** — o healthcheck tem um URL malformado na imagem actual; o serviço funciona normalmente. Validar com `docker compose exec trino trino --execute "SHOW CATALOGS;"`.

**Grafana sem dados** — verificar se o Trino está `running` e se o datasource está configurado em `04_application/grafana/provisioning/datasources/`. Recarregar dashboards sem reiniciar o stack:

```powershell
curl -X POST http://localhost:3300/api/admin/provisioning/dashboards/reload -u admin:admin
```
