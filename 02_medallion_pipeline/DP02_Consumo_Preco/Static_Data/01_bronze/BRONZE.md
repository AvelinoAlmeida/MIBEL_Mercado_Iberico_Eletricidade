# Bronze — DP-02 Consumo vs Preço (Static Data)

> **Âmbito:** ingestão fiel dos dois ficheiros CSV históricos (REN + OMIE) para o lakehouse Iceberg.
> A camada Bronze preserva a fonte sem transformações de negócio; acrescenta apenas metadados
> de ingestão e particionamento por `process_date`.
>
> **Nota (2026-05-23):** a análise programática dos CSVs revelou limitações que justificam
> a criação do pipeline paralelo `Streaming_Data` — ver [§ 5](#5-limitações-e-pipeline-streaming).

---

## 1. Fontes de Dados

### 1.1 Consumo Total Nacional — REN

| Atributo | Valor |
|---|---|
| **Origem** | Redes Energéticas Nacionais (REN) — download manual |
| **Ficheiro** | `data/raw/consumo-total-nacional.csv` |
| **Formato** | CSV, separador vírgula, encoding UTF-8 com BOM |
| **Granularidade** | 15 minutos |
| **Unidade** | kW (kilowatt) |
| **Tamanho** | 13 MB · 111 085 linhas |
| **Período real** | 2023-01-01 → **2025-02-28** |
| **Dias cobertos** | 1 159 de 1 159 esperados |
| **Colunas originais** | `datahora, dia, mes, ano, date, time, bt, mt, at, mat, total` |
| **Nulos em qualquer coluna** | 0 |

#### Problemas de qualidade identificados

**Gap de 376 dias sem dados de consumo**

O ficheiro termina em **2025-02-28**; o ficheiro de preços cobre até 2026-03-11.
A tabela Gold perde ≈ 1 ano de análise integrada consumo↔preço.

```
Consumo  : ████████████████████░░░░░░░░░░░░░░░░░░░░
               2023-01-01        2025-02-28   2026-03-11
Preços   : ████████████████████████████████████████
```

**Início irregular — primeiros dias incompletos**

```
2023-01-01T03:00:00  ← começa às 3h (sem as 3 horas iniciais)
2023-01-01T13:30:00  ← salta 10h30
2023-01-04T22:00:00  ← salta 3 dias inteiros
```
O portal REN publicava apenas amostras nos primeiros dias. A Silver agrega estas horas incompletas sem alertar.

**Dias incompletos (< 96 registos de 15 min)**

| Data | Registos | Situação |
|---|---|---|
| 2025-10-13 | 93 | 3 registos em falta na transição DST de outono |
| 2025-10-14 | 3 | Download truncado — apenas 3 registos do dia |
| 2026-03-04 | 1 | Registo órfão fora do período real coberto |

**Timestamps duplicados (DST)**

24 timestamps duplicados nas transições de hora de verão/inverno.
O CSV usa timestamps locais ambíguos: na mudança de outubro a hora `01:xx`
existe duas vezes em horário local → dois registos com o mesmo timestamp UTC.

| Exemplo | Ocorrências |
|---|---|
| `2024-10-27T01:00:00+00:00` | 2× |
| `2025-03-30T01:00:00+00:00` | 2× |
| `2025-03-30T01:30:00+00:00` | 2× |

A Bronze faz `DELETE WHERE 1=1` + `INSERT` completo → duplicados entram sem deduplicação.
**A Silver é responsável por os resolver.**

---

### 1.2 Preços Day-Ahead MIBEL — OMIE

| Atributo | Valor |
|---|---|
| **Origem** | OMIE — Operador del Mercado Ibérico de Energía |
| **Ficheiro** | `data/raw/Day-ahead Market Prices_20230101_20260311.csv` |
| **Formato** | CSV, separador `;`, 3 linhas de cabeçalho (2 de metadados + 1 de colunas) |
| **Granularidade** | Horária (horas 1-25) |
| **Unidade** | €/MWh *(cabeçalho diz €/MW — erro tipográfico histórico do portal OMIE)* |
| **Tamanho** | 710 KB · 27 984 linhas |
| **Período real** | 2023-01-01 → 2026-03-11 |
| **Dias cobertos** | 1 166 de 1 166 esperados |
| **Colunas originais** | `Date, Hour, Portugal, Spain` |

#### Problemas de qualidade identificados

**Preços zero (879 horas · 3,1%)**

299 blocos de preço zero. O maior: **2023-01-01 horas 1–13** — o mercado não liquidou
no primeiro dia do ano. Nos restantes blocos ocorrem maioritariamente em fins-de-semana
com excesso de geração renovável. **São dados de mercado legítimos**, não erros.

**Preços negativos (504 horas)**

Gama observada: **−5,00 €/MWh → 240,00 €/MWh**.
Preços negativos são um fenómeno real de mercado (oversupply solar/eólica em baixa procura).
A Silver sinaliza como `WARN` mas não filtra.

**Tratamento DST correcto**

| Evento DST | Dias | Comportamento |
|---|---|---|
| Spring forward (março) | 2023-03-26, 2024-03-31, 2025-03-30 | 23 horas — hora 2 ausente |
| Fall back (outubro) | 2023-10-29, 2024-10-27, 2025-10-26 | 25 horas — hora 25 extra |

O OMIE codifica corretamente ambas as situações. A Bronze preserva a hora 25; a Silver descarta-a.

---

## 2. Tabelas Bronze (Iceberg)

### `iceberg.bronze.consumo_raw`

| Coluna | Tipo | Descrição |
|---|---|---|
| `datahora` | `TIMESTAMP(6) WITH TIME ZONE` | Timestamp original da fonte (UTC) |
| `dia` | `INTEGER` | Dia do mês (campo redundante da fonte) |
| `mes` | `INTEGER` | Mês (campo redundante da fonte) |
| `ano` | `INTEGER` | Ano (campo redundante da fonte) |
| `date_raw` | `VARCHAR` | Campo date original em string |
| `time_raw` | `VARCHAR` | Campo time original em string |
| `bt` | `DOUBLE` | Consumo BT em kW |
| `mt` | `DOUBLE` | Consumo MT em kW |
| `at` | `DOUBLE` | Consumo AT em kW |
| `mat` | `DOUBLE` | Consumo MAT em kW |
| `total` | `DOUBLE` | Consumo total nacional em kW |
| `process_date` | `DATE` | Data lógica de ingestão (**partição**) |

Particionamento: `['process_date']` · Localização: `s3a://warehouse/bronze/consumo_raw/`

### `iceberg.bronze.preco_raw`

| Coluna | Tipo | Descrição |
|---|---|---|
| `date_raw` | `VARCHAR` | Data original da linha (string OMIE) |
| `hour` | `INTEGER` | Hora OMIE (1-24 normal; 25 em DST outono) |
| `price_portugal_raw` | `DOUBLE` | Preço day-ahead Portugal em €/MWh |
| `price_spain_raw` | `DOUBLE` | Preço day-ahead Espanha em €/MWh |
| `process_date` | `DATE` | Data lógica de ingestão (**partição**) |

Particionamento: `['process_date']` · Localização: `s3a://warehouse/bronze/preco_raw/`

---

## 3. Fluxo de Ingestão

```
CSVs raw (data/raw/)
    │
    ▼  run_medallion_consumo_precos.py → upload_raw_csvs_to_minio()
MinIO: warehouse/raw/
    │
    ▼  Flyte Remoto (K3s sandbox): flyte_ingest_bronze.py → ingest_bronze_full
    │
    ├─ ingest_consumo_full()  → lê consumo-total-nacional.csv → DELETE + INSERT consumo_raw
    └─ ingest_preco_full()    → lê Day-ahead Market Prices_*.csv → DELETE + INSERT preco_raw
```

**Idempotência:** `DELETE WHERE 1=1` antes de cada `INSERT` — re-execuções seguras.

**Batching:** INSERTs agrupados em lotes de 5 000 linhas e máx. 60 partições por statement
para respeitar os limites de writers concorrentes do Trino/Iceberg.

---

## 4. Critérios de Qualidade Bronze

Verificações em `04_quality/sql/01_bronze_checks.sql` — executadas pelo quality gate após ingestão:

| Check | Threshold | Nível |
|---|---|---|
| Nulos em `datahora` | 0% | FAIL |
| Nulos em `total` | 0% | FAIL |
| Nulos em `price_portugal_raw` | 0% | FAIL |
| `hour` entre 1 e 25 | 100% | FAIL |
| Unicidade `(date_raw, hour, process_date)` | 0 duplicados | FAIL |
| `total > 0` | 100% | WARN |
| `price_portugal_raw >= 0` | 100% | WARN |
| Unicidade `(datahora, process_date)` | 0 duplicados | WARN |
| Completude consumo ≥ 80 reg/dia | todos os dias | WARN |
| Completude preços ≥ 23 reg/dia | todos os dias | WARN |

---

## 5. Limitações e Pipeline Streaming

As limitações identificadas na análise dos CSVs são a motivação directa para a criação
do pipeline `DP02_Consumo_Preco/Streaming_Data` em paralelo:

| Limitação Static Data | Solução Streaming Data |
|---|---|
| Gap de 376 dias no consumo (CSV desatualizado) | REN DataHub API com ingestão incremental |
| Download manual do ficheiro OMIE | Fetch automático por dia via OMIE/ESIOS |
| Timestamps duplicados DST sem resolução na fonte | Normalização UTC no ingest API |
| Início irregular (primeiros dias de 2023 incompletos) | Dados históricos via API desde qualquer data |
| Ficheiro bulk de 13 MB relido completo em cada run | Ingestão incremental — só processa novos registos |

**Papel de cada pipeline:**
- **Static Data** — fonte histórica auditável; dados tal como descarregados do REN e OMIE
- **Streaming Data** — cobre o período actual e futuro com qualidade superior

### Fontes alternativas identificadas (referência futura)

| Fonte | URL / Endpoint | Vantagem |
|---|---|---|
| REN DataHub API | `https://datahub.ren.pt/en/api-instructions/` | Elimina gap; atualização a cada 15 min |
| OMIE CSV diário | `marginalpdbc_YYYYMMDD.1` (portal OMIE) | Ingestão incremental por dia |
| REE ESIOS API | `https://api.esios.ree.es/` | REST JSON, histórico completo PT+ES |
| ENTSO-E Transparency | `https://transparency.entsoe.eu/` | Cobertura europeia desde 2015 |

---

## 6. Ficheiros de Referência

| Ficheiro | Descrição |
|---|---|
| `bronze_consumo_precos_trino.sql` | DDL das duas tabelas Bronze (idempotente — `IF NOT EXISTS`) |
| `data/raw/consumo-total-nacional.csv` | CSV raw de consumo (REN) |
| `data/raw/Day-ahead Market Prices_20230101_20260311.csv` | CSV raw de preços (OMIE) |
| `scripts/python/bronze_clean_upload_consumo_precos.py` | Script standalone de exploração e upload |
| `../workflows/flyte_ingest_bronze.py` | Tasks Flyte de ingestão Bronze (execução remota K3s) |
| `../04_quality/sql/01_bronze_checks.sql` | SQL de quality gate Bronze |

---

## 7. Decisões de Design

- **Preservação fiel:** o Bronze não altera unidades, não interpreta timestamps, não converte tipos — garante reprodutibilidade e auditabilidade da fonte original.
- **Hora 25 preservada:** dias com mudança DST de outono têm 25 horas no CSV OMIE. O Bronze preserva esta hora extra; a Silver filtra-a para manter o modelo UTC puro.
- **Partição por `process_date`:** permite backfill eficiente; re-ingestões de dias específicos não afectam outras partições.
- **Fonte CSV vs API:** optou-se por CSV descarregado manualmente porque REN e OMIE não disponibilizavam API pública gratuita para dados históricos completos à data da implementação. O ficheiro OMIE cobre 2023-2026 com granularidade horária completa.
- **Execução remota Flyte:** os tasks de ingestão correm em pods K3s (sandbox `mibel-flyte-sandbox`); ligam ao Trino e MinIO via `host.docker.internal`.
