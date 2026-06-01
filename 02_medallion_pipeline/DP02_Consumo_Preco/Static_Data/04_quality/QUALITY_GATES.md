# Quality Gates — consumo_preco (DP-02)

## Arquitetura de Qualidade

Os quality gates são implementados como queries SQL executadas no Trino após cada camada da pipeline. Cada verificação retorna uma linha com o esquema:

```
check_name | status | valor_pct | threshold_pct | detalhe
```

| Status | Significado | Impacto |
|---|---|---|
| `PASS` | Verificação aprovada | Nenhum |
| `WARN` | Anomalia tolerável detectada | Registo no log; promoção continua |
| `FAIL` | Violação crítica de contrato | Pipeline bloqueada; retry via Flyte |

O workflow `flyte_quality_checks.py` executa os SQLs e lança `FlyteRecoverableException` em caso de qualquer `FAIL`, com até 2 retries automáticos.

---

## Ficheiros SQL

| Ficheiro | Camada | Nº de checks |
|---|---|---|
| `sql/01_bronze_checks.sql` | Bronze | 11 + 1 detalhe |
| `sql/02_silver_checks.sql` | Silver | 13 |
| `sql/03_gold_checks.sql` | Gold | 12 |

---

## Bronze Checks (`01_bronze_checks.sql`)

| # | Check | Tabela | Tipo | Threshold | Status |
|---|---|---|---|---|---|
| 1 | Nulos em `datahora` | `consumo_raw` | Null rate | 0% | FAIL |
| 2 | Nulos em `total` | `consumo_raw` | Null rate | 0% | FAIL |
| 3 | Nulos em `price_portugal_raw` | `preco_raw` | Null rate | 0% | FAIL |
| 4 | Nulos em `price_spain_raw` | `preco_raw` | Null rate | 0% | FAIL |
| 5 | `total > 0` | `consumo_raw` | Range | 100% positivos | WARN |
| 6 | `hour BETWEEN 1 AND 25` | `preco_raw` | Range | 0 fora do intervalo | FAIL |
| 7 | `price_portugal_raw >= 0` | `preco_raw` | Range | 100% não-neg. | WARN |
| 8 | Unicidade `(datahora, process_date)` | `consumo_raw` | Uniqueness | 0 duplicados | WARN |
| 9 | Unicidade `(date_raw, hour, process_date)` | `preco_raw` | Uniqueness | 0 duplicados | FAIL |
| 10 | Completude ≥ 80 registos/dia | `consumo_raw` | Completeness | 0 dias abaixo | WARN |
| 11 | Completude ≥ 23 registos/dia | `preco_raw` | Completeness | 0 dias abaixo | WARN |

**Query de detalhe adicional**: dias com menos de 96 registos de consumo (análise exploratória, não bloqueia).

**Fundamento dos thresholds**:
- 80 registos/dia consumo = 83% dos 96 esperados (15 min × 24h) — tolera dados em falta pontuais
- 23 registos/dia preços = 23/24 — tolera uma hora em falta por dia sem bloquear
- `WARN` em `total = 0`: consumo zero não é impossível (madrugada, ilhas), mas é suspeito
- `WARN` em preços negativos: mercado MIBEL aceita preços negativos em excesso de renovável

---

## Silver Checks (`02_silver_checks.sql`)

| # | Check | Tabela | Tipo | Threshold | Status |
|---|---|---|---|---|---|
| 1 | Nulos em `ts_utc` | `consumo_hourly` | Null rate | 0% | FAIL |
| 2 | Nulos em `total_mwh` | `consumo_hourly` | Null rate | 0% | FAIL |
| 3 | Nulos em `ts_utc` | `preco_hourly` | Null rate | 0% | FAIL |
| 4 | Nulos em `price_portugal_eur_mwh` | `preco_hourly` | Null rate | 0% | FAIL |
| 5 | Nulos em `price_spain_eur_mwh` | `preco_hourly` | Null rate | 0% | FAIL |
| 6 | `total_mwh > 0` | `consumo_hourly` | Range | 100% positivos | WARN |
| 7 | `price_portugal_eur_mwh >= 0` | `preco_hourly` | Range | 100% não-neg. | WARN |
| 8 | `ts_utc` único | `consumo_hourly` | Uniqueness | 0 duplicados | FAIL |
| 9 | `ts_utc` único | `preco_hourly` | Uniqueness | 0 duplicados | FAIL |
| 10 | `ts_utc` em fronteira de hora | `consumo_hourly` | Temporal | min=0, sec=0 | FAIL |
| 11 | Cobertura join consumo × preço ≥ 95% | cross-table | Join coverage | ≥ 95% | WARN |
| 12 | Dias com ≥ 23h em consumo | `consumo_hourly` | Completeness | 0 dias abaixo | WARN |
| 13 | Dias com ≥ 23h em preços | `preco_hourly` | Completeness | 0 dias abaixo | WARN |

**Fundamento**:
- Check 10 (fronteira de hora): valida que a agregação 15 min → 1h foi feita corretamente
- Check 11 (cobertura 95%): margin para dias sem publicação OMIE; 98% é o SLO de produção
- `FAIL` em unicidade Silver: duplicados na Silver corrompem as window functions da Gold

---

## Gold Checks (`03_gold_checks.sql`)

| # | Check | Tabela | Tipo | Threshold | Status |
|---|---|---|---|---|---|
| 1 | Nulos em `ts_utc` | `dp_energy_market_hourly` | Null rate | 0% | FAIL |
| 2 | Nulos em `consumo_total` | `dp_energy_market_hourly` | Null rate | 0% | FAIL |
| 3 | Nulos em `market_price_pt` | `dp_energy_market_hourly` | Null rate | 0% | FAIL |
| 4 | `hora BETWEEN 0 AND 23` | `dp_energy_market_hourly` | Range | 0 fora | FAIL |
| 5 | `dia_semana BETWEEN 0 AND 6` | `dp_energy_market_hourly` | Range | 0 fora | FAIL |
| 6 | `ts_utc` único | `dp_energy_market_hourly` | Uniqueness | 0 duplicados | FAIL |
| 7 | `ts_utc` único | `feat_load_forecasting_hourly` | Uniqueness | 0 duplicados | FAIL |
| 8 | Nulos em `consumo_next_hour` | `feat_load_forecasting_hourly` | Null rate (ML) | 0% | FAIL |
| 9 | Nulos em `consumo_lag_1h` | `feat_load_forecasting_hourly` | Null rate (ML) | 0% | FAIL |
| 10 | Nulos em `consumo_lag_24h` | `feat_load_forecasting_hourly` | Null rate (ML) | 0% | FAIL |
| 11 | Nulos em `price_lag_1h` | `feat_load_forecasting_hourly` | Null rate (ML) | 0% | FAIL |
| 12 | Paridade linhas dp vs feat (diff ≤ 48) | cross-table | Row count | diff ≤ 48 | WARN |
| 13 | Consistência `consumo_lag_1h` vs hora anterior | `dp_energy_market_hourly` | Lag consistency | < 0.1% erros | FAIL |
| 14 | Consistência `price_lag_1h` vs hora anterior | `dp_energy_market_hourly` | Lag consistency | < 0.1% erros | FAIL |

**Fundamento**:
- Checks 8-11 (`FAIL` em nulos ML): a feature table deve estar 100% limpa para treino — nulos corrompem modelos scikit-learn sem tratamento explícito
- Check 12 (paridade ≤ 48): a feat perde as primeiras 24h (sem lags) e a última hora (sem LEAD) — a diferença esperada é ~25; 48 dá margem para DST e fronteiras de mês
- Checks 13-14 (consistência de lag): verificação por self-join; tolerância 0.01 para arredondamentos em float — garante que as window functions foram calculadas corretamente

---

## Integração no Pipeline

```
Bronze ingest (Flyte)
    │
    ▼ quality_gate_bronze()
    │   → FAIL? FlyteRecoverableException (retry x2) → pipeline bloqueada
    │   → WARN? log + continua
    │   → PASS? continua
    ▼
Silver transform (Flyte)
    │
    ▼ quality_gate_silver()
    │   (mesma lógica)
    ▼
Gold transform (Flyte)
    │
    ▼ quality_gate_gold()
    │   (mesma lógica)
    ▼
Dados prontos para serving e ML
```

---

## Como executar manualmente

```bash
# Apenas gate Bronze
pyflyte run workflows/flyte_quality_checks.py quality_gate_bronze

# Apenas gate Silver
pyflyte run workflows/flyte_quality_checks.py quality_gate_silver

# Apenas gate Gold
pyflyte run workflows/flyte_quality_checks.py quality_gate_gold
```

Ou via Trino diretamente (útil para debug):

```bash
docker compose -f 01_docker_stack/docker-compose.yml exec trino trino \
  --file 02_medallion_pipeline/consumo_preco/04_quality/sql/03_gold_checks.sql
```
