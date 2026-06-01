#!/usr/bin/env python3
"""
Pipeline Bronze para os ficheiros:
- consumo-total-nacional.csv
- Day-ahead Market Prices_20230101_20260311.csv

O script faz 4 coisas principais:
1) Lê os CSV originais (raw)
2) Limpa e normaliza os dados com pandas
3) Grava versões limpas em Parquet
4) Opcionalmente faz upload para o MinIO/S3
   - mantém o upload dos ficheiros limpos (clean)
   - acrescenta também o upload dos ficheiros raw

Exemplo de execução:
python bronze_clean_upload_consumo_precos.py \
  --consumo consumo-total-nacional.csv \
  --precos "Day-ahead Market Prices_20230101_20260311.csv" \
  --out-dir ./output \
  --upload

Variáveis de ambiente para upload:
S3_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET=warehouse
S3_PREFIX=bronze/clean
"""
from __future__ import annotations

import argparse
import os
import uuid
from pathlib import Path

import pandas as pd


def _strip_bom(text: str) -> str:
    return text.replace("\ufeff", "")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_strip_bom(str(c)).strip().lower() for c in df.columns]
    return df


def _clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype("string").str.strip()
    return df


def _require_columns(df: pd.DataFrame, required: list[str], dataset_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(
            f"{dataset_name}: colunas obrigatórias em falta: {missing}. "
            f"Colunas disponíveis: {list(df.columns)}"
        )


def _rank_duplicates(
    df: pd.DataFrame,
    key_col: str,
    value_col: str,
) -> pd.DataFrame:
    df = df.copy()

    value_num = pd.to_numeric(df[value_col], errors="coerce")
    zero_penalty = value_num.fillna(0).eq(0).astype(int)

    df["duplicate_count"] = df.groupby(key_col)[key_col].transform("size").astype("Int64")
    df["flag_duplicate_timestamp"] = df["duplicate_count"] > 1

    temp = pd.DataFrame(
        {
            "key": df[key_col],
            "zero_penalty": zero_penalty,
            "value_num": value_num,
            "row_pos": range(len(df)),
        }
    )

    temp = temp.sort_values(
        by=["key", "zero_penalty", "value_num", "row_pos"],
        ascending=[True, True, False, True],
        kind="mergesort",
    )
    temp["duplicate_rank"] = temp.groupby("key").cumcount() + 1
    temp = temp.sort_values("row_pos")

    df["duplicate_rank"] = temp["duplicate_rank"].astype("Int64").to_numpy()
    return df


def clean_consumo(path: Path, run_id: str, ingest_ts_utc: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _normalize_columns(df)
    _require_columns(
        df,
        ["datahora", "dia", "mes", "ano", "date", "time", "bt", "mt", "at", "mat", "total"],
        "consumo-total-nacional.csv",
    )
    df = _clean_string_columns(df)

    df["timestamp_utc"] = pd.to_datetime(df["datahora"], errors="coerce", utc=True)
    df["timestamp_utc"] = df["timestamp_utc"].dt.tz_localize(None)

    df["data_local"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    for col in ["dia", "mes", "ano"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["bt", "mt", "at", "mat", "total"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["flag_bad_timestamp"] = df["timestamp_utc"].isna()
    df["flag_bad_date"] = pd.isna(df["data_local"])
    df["flag_bad_total"] = df["total"].isna()
    df["flag_zero_total"] = df["total"].eq(0) & df["total"].notna()

    derived_date_from_timestamp = pd.to_datetime(df["timestamp_utc"], errors="coerce").dt.date
    df["flag_date_mismatch"] = (
        pd.Series(derived_date_from_timestamp).notna()
        & pd.Series(df["data_local"]).notna()
        & (pd.Series(derived_date_from_timestamp) != pd.Series(df["data_local"]))
    )

    df = _rank_duplicates(df, "timestamp_utc", "total")

    out = pd.DataFrame(
        {
            "datahora_raw": df["datahora"],
            "timestamp_utc": df["timestamp_utc"],
            "dia": df["dia"],
            "mes": df["mes"],
            "ano": df["ano"],
            "data_local": df["data_local"],
            "hora_local_raw": df["time"],
            "consumo_bt_kwh": df["bt"],
            "consumo_mt_kwh": df["mt"],
            "consumo_at_kwh": df["at"],
            "consumo_mat_kwh": df["mat"],
            "consumo_total_kwh": df["total"],
            "duplicate_count": df["duplicate_count"],
            "duplicate_rank": df["duplicate_rank"],
            "flag_duplicate_timestamp": df["flag_duplicate_timestamp"],
            "flag_bad_timestamp": df["flag_bad_timestamp"],
            "flag_bad_date": df["flag_bad_date"],
            "flag_bad_total": df["flag_bad_total"],
            "flag_zero_row": df["flag_zero_total"],
            "flag_date_mismatch": df["flag_date_mismatch"],
            "source_file_name": path.name,
            "run_id": run_id,
            "ingest_ts_utc": ingest_ts_utc,
        }
    )

    return out.sort_values(["timestamp_utc", "duplicate_rank"], kind="mergesort").reset_index(drop=True)


def clean_precos(
    path: Path,
    run_id: str,
    ingest_ts_utc: pd.Timestamp,
    market_timezone: str = "Europe/Lisbon",
) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", skiprows=2)
    df = _normalize_columns(df)
    _require_columns(
        df,
        ["date", "hour", "portugal", "spain"],
        "Day-ahead Market Prices_20230101_20260311.csv",
    )
    df = _clean_string_columns(df)

    df["delivery_date_local"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=False).dt.date
    df["market_hour"] = pd.to_numeric(df["hour"], errors="coerce").astype("Int64")
    df["preco_pt_eur_mwh"] = pd.to_numeric(df["portugal"], errors="coerce")
    df["preco_es_eur_mwh"] = pd.to_numeric(df["spain"], errors="coerce")

    naive_local_ts = pd.to_datetime(df["date"], errors="coerce") + pd.to_timedelta(
        pd.to_numeric(df["hour"], errors="coerce") - 1, unit="h"
    )

    localized = naive_local_ts.dt.tz_localize(
        market_timezone,
        ambiguous="NaT",
        nonexistent="NaT",
    )
    df["timestamp_utc"] = localized.dt.tz_convert("UTC").dt.tz_localize(None)

    df["flag_bad_timestamp"] = df["timestamp_utc"].isna()
    df["flag_bad_date"] = pd.isna(df["delivery_date_local"])
    df["flag_bad_hour"] = df["market_hour"].isna() | ~df["market_hour"].between(1, 25)
    df["flag_bad_preco_pt"] = df["preco_pt_eur_mwh"].isna()
    df["flag_bad_preco_es"] = df["preco_es_eur_mwh"].isna()
    df["flag_zero_preco_pt"] = df["preco_pt_eur_mwh"].eq(0) & df["preco_pt_eur_mwh"].notna()

    df = _rank_duplicates(df, "timestamp_utc", "preco_pt_eur_mwh")

    out = pd.DataFrame(
        {
            "date_raw": df["date"],
            "delivery_date_local": df["delivery_date_local"],
            "market_hour": df["market_hour"],
            "timestamp_utc": df["timestamp_utc"],
            "preco_pt_eur_mwh": df["preco_pt_eur_mwh"],
            "preco_es_eur_mwh": df["preco_es_eur_mwh"],
            "duplicate_count": df["duplicate_count"],
            "duplicate_rank": df["duplicate_rank"],
            "flag_duplicate_timestamp": df["flag_duplicate_timestamp"],
            "flag_bad_timestamp": df["flag_bad_timestamp"],
            "flag_bad_date": df["flag_bad_date"],
            "flag_bad_hour": df["flag_bad_hour"],
            "flag_bad_preco_pt": df["flag_bad_preco_pt"],
            "flag_bad_preco_es": df["flag_bad_preco_es"],
            "flag_zero_preco_pt": df["flag_zero_preco_pt"],
            "market_timezone_assumed": market_timezone,
            "source_file_name": path.name,
            "run_id": run_id,
            "ingest_ts_utc": ingest_ts_utc,
        }
    )

    return out.sort_values(["timestamp_utc", "duplicate_rank"], kind="mergesort").reset_index(drop=True)


def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_path, index=False, engine="pyarrow")
    except ImportError as exc:
        raise SystemExit(
            "Falta a dependência 'pyarrow'. Instala com: pip install pyarrow"
        ) from exc


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit(
            "Falta a dependência 'boto3'. Instala com: pip install boto3"
        ) from exc

    endpoint = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3.upload_file(str(local_path), bucket, key)


def print_quality_report(name: str, df: pd.DataFrame, total_col: str) -> None:
    print(f"\n=== {name} ===")
    print(f"Rows: {len(df):,}")
    print(f"Distinct timestamps: {df['timestamp_utc'].nunique(dropna=True):,}")
    print(f"Min timestamp: {df['timestamp_utc'].min()}")
    print(f"Max timestamp: {df['timestamp_utc'].max()}")
    print(f"Duplicate timestamp rows: {int(df['flag_duplicate_timestamp'].sum()):,}")
    print(f"Bad timestamps: {int(df['flag_bad_timestamp'].sum()):,}")

    for flag_col in [c for c in df.columns if c.startswith("flag_")]:
        print(f"{flag_col}: {int(df[flag_col].sum()):,}")

    print("\nSample duplicate timestamps:")
    sample = (
        df.loc[df["flag_duplicate_timestamp"], ["timestamp_utc", total_col, "duplicate_rank"]]
        .head(8)
        .to_string(index=False)
    )
    print(sample if not sample.strip().startswith("Empty") else "No duplicates found.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumo", required=True, type=Path)
    parser.add_argument("--precos", required=True, type=Path)
    parser.add_argument("--out-dir", default=Path("./output"), type=Path)
    parser.add_argument("--price-timezone", default="Europe/Lisbon")
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()

    run_id = uuid.uuid4().hex
    ingest_ts_utc = pd.Timestamp.now("UTC").tz_localize(None)

    consumo_df = clean_consumo(args.consumo, run_id=run_id, ingest_ts_utc=ingest_ts_utc)
    precos_df = clean_precos(
        args.precos,
        run_id=run_id,
        ingest_ts_utc=ingest_ts_utc,
        market_timezone=args.price_timezone,
    )

    consumo_out = args.out_dir / "consumo_total_nacional_clean.parquet"
    precos_out = args.out_dir / "energy_market_prices_pt_es_clean.parquet"

    write_parquet(consumo_df, consumo_out)
    write_parquet(precos_df, precos_out)

    print_quality_report("Consumo", consumo_df, "consumo_total_kwh")
    print_quality_report("Preços day-ahead", precos_df, "preco_pt_eur_mwh")

    print(f"\nWrote: {consumo_out}")
    print(f"Wrote: {precos_out}")
    print(f"Run ID: {run_id}")
    print(f"Ingest TS UTC: {ingest_ts_utc}")

    if args.upload:
        bucket = os.environ.get("S3_BUCKET", "warehouse")
        prefix = os.environ.get("S3_PREFIX", "bronze/clean").strip("/")

        raw_consumo_key = "bronze/raw/consumo_total_nacional/consumo-total-nacional.csv"
        raw_precos_key = "bronze/raw/energy_market_prices/Day-ahead Market Prices_20230101_20260311.csv"
        upload_file(args.consumo, bucket, raw_consumo_key)
        upload_file(args.precos, bucket, raw_precos_key)

        clean_consumo_key = f"{prefix}/consumo_total_nacional/{consumo_out.name}"
        clean_precos_key = f"{prefix}/energy_market_prices/{precos_out.name}"
        upload_file(consumo_out, bucket, clean_consumo_key)
        upload_file(precos_out, bucket, clean_precos_key)

        print(f"\nUploaded raw:   s3://{bucket}/{raw_consumo_key}")
        print(f"Uploaded raw:   s3://{bucket}/{raw_precos_key}")
        print(f"Uploaded clean: s3://{bucket}/{clean_consumo_key}")
        print(f"Uploaded clean: s3://{bucket}/{clean_precos_key}")


if __name__ == "__main__":
    main()