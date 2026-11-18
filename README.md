# Bank Ledger Lakehouse

A batch-first lakehouse for synthetic Kenyan core-banking data: accounts, transactions and loans modelled with CDC-style loads on Apache Iceberg, with Snowflake and dbt on top.

## What this project demonstrates

- Incremental `MERGE` loads from daily change files into Iceberg tables on S3
- SCD2 history on accounts
- Iceberg schema evolution and time travel
- Handling messy, multi-format source data with data quality tests
- Analytics modelling in Snowflake with dbt

## Status

| Component | Status |
| --- | --- |
| Synthetic data generator | Done |
| S3 and Terraform foundation | Planned |
| Spark ingestion and Iceberg MERGE loads | Planned |
| SCD2 on accounts | Planned |
| Snowflake and dbt models | Planned |
| Airflow orchestration and CI/CD | Planned |

## Stack

Python, PySpark, Apache Iceberg, AWS (S3, EMR Serverless, ECR, IAM/OIDC), Snowflake, dbt, Airflow, Docker, Terraform, GitHub Actions.

## Synthetic data generator

`generator/gen_core_banking.py` produces seeded, reproducible daily batch files for a year of activity. Every run with the same seed and settings yields identical output.

### Tables

| Table | Format | Behaviour |
| --- | --- | --- |
| branches | CSV | Reference data, written on day 0 |
| customers | CSV | CDC rows: `op`, `change_ts` plus all columns |
| accounts | CSV | CDC rows, including hard deletes |
| transactions | JSON Lines | Append-only, with reversals, corrections and late arrivals |
| loans | Parquet | CDC rows with status transitions |
| loan_repayments | Parquet | Append-only |

Files land in `dt=YYYY-MM-DD` folders, one per day.

### Scale

| Preset | Flag | Approx. size |
| --- | --- | --- |
| dev | `--scale 0.05` | ~6k accounts, ~150k transactions |
| full | `--scale 1.0` | 30k accounts, ~3M transactions |

### Messiness

Eleven defect types are injected at configurable rates: duplicate rows, nulls in required fields, orphan foreign keys, mixed date formats and timezones, phone number variants, name formatting, amounts as strings, currency variants, non-positive amounts, out-of-order updates and test records.

Schema drift is built in: a new column on accounts, a renamed column on loans, and a type change on transactions.

Every injected defect is logged in `defects_manifest.csv`, which acts as ground truth for testing the cleaning layer.

### Usage

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r generator/requirements.txt
```

```bash
python generator/gen_core_banking.py --scale 0.05 --out-dir data/out_dev
```

```bash
python generator/gen_core_banking.py --scale 1.0 --seed 7 --out-dir data/out_full
```

Override a single defect rate:

```bash
python generator/gen_core_banking.py --rate duplicate_row=0.05 --out-dir data/out_dev --force
```

Generated data is gitignored and can be regenerated from the seed.

## Repository layout

```text
generator/   synthetic data generator
data/        generated output (gitignored)
```

More folders (`spark/`, `dbt/`, `airflow/`, `terraform/`, `docker/`, `tests/`, `docs/`) are added as each milestone starts.