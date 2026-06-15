# E-commerce Data Quality Pipeline

[![CI](https://github.com/semsuddin/ecommerce-data-quality/actions/workflows/ci.yml/badge.svg)](https://github.com/semsuddin/ecommerce-data-quality/actions/workflows/ci.yml)

A data-validation pipeline on GCP. Messy e-commerce data is ingested into
BigQuery, transformed with dbt, aggregated with Spark, validated at multiple
layers, and surfaced in Looker Studio, all orchestrated by Airflow.

The point of the project is the validation. A generator seeds a known number of
data-quality defects into the source (duplicate keys, broken foreign keys, bad
emails, negative quantities, future dates, totals that do not reconcile), and
the pipeline catches them. Because the defect counts are known up front, you can
show the framework finding exactly what was planted.

## What this demonstrates

| Requirement | Where it lives |
|---|---|
| Designing and executing data-validation frameworks for ETL/ELT | `validation/` (declarative `checks.yml` + `run_checks.py`) and dbt tests in `dbt/ecommerce/models/**/schema.yml` |
| Looker Studio / low-code analytics | `dashboards/looker_studio_setup.md` (business page + data-quality page) |
| GCP | BigQuery as the warehouse; datasets, service account, and load steps documented below |
| Validating pipelines built on Airflow, dbt, Spark | `airflow/dags/` orchestrates a dbt build, a PySpark job, and two validation gates |

## Run it locally first (no cloud account, a few minutes)

The whole core runs against DuckDB and local Spark, so you can prove it before
touching GCP.

```bash
pip install -r requirements.txt

make gen        # generate raw CSVs with seeded defects
make validate   # run the validation framework against the raw data
make spark      # run the PySpark daily-revenue aggregation
# or: make local   (all three)
```

`make validate` prints a report and exits non-zero because the seeded source is
intentionally dirty. That non-zero exit is the gate. The violation counts line
up with the defect counts the generator printed.

## Run it on GCP (the showcase version)

You run every step under your own identity. The repo never contains credentials;
`.gitignore` excludes `.env` and key files.

1. **Project and datasets**

   ```bash
   gcloud config set project YOUR_PROJECT
   bq --location=EU mk -d ecommerce_raw
   bq --location=EU mk -d ecommerce_marts
   ```

2. **Service account** with `BigQuery Data Editor` + `BigQuery Job User`,
   download a key, and point to it:

   ```bash
   cp .env.example .env   # fill in project, datasets, bucket
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   export GCP_PROJECT=YOUR_PROJECT
   ```

3. **Load + transform + validate**

   ```bash
   python generate_data.py --rows 5000 --out data/raw
   for t in customers products orders order_items; do \
     bq load --replace --autodetect --source_format=CSV \
       $GCP_PROJECT:ecommerce_raw.$t data/raw/$t.csv; done

   cd validation && python run_checks.py --engine bigquery \
     --bq-project $GCP_PROJECT --bq-dataset ecommerce_raw \
     --no-fail --results-out /tmp/dq_results.csv && cd ..

   cd dbt/ecommerce && dbt deps && dbt build --target bq && cd ../..

   cd validation && python run_checks.py --engine bigquery \
     --bq-project $GCP_PROJECT --bq-dataset ecommerce_marts \
     --checks checks_marts.yml && cd ..
   ```

4. **Airflow** ties it together. Use the official Airflow image or the Astro CLI
   (`astro dev start`), mount this repo at `PROJECT_DIR`, set the env vars from
   `.env`, and trigger the `ecommerce_dataquality` DAG. This keeps you on local
   Airflow so there is no Cloud Composer cost.

5. **Dashboard**: follow `dashboards/looker_studio_setup.md` to connect Looker
   Studio to `fct_orders` and `dq_results`.

## Continuous integration

GitHub Actions runs on every push and pull request (`.github/workflows/ci.yml`):

- **Validation framework tests** (`tests/test_validation.py`): generates seeded
  data and asserts the validator flags every planted defect and passes the clean
  checks. The dirty data is expected, so a green test means the validator
  correctly caught it. The job also prints the full validation report to the
  workflow summary.
- **PySpark smoke test**: sets up Java, runs the aggregation end to end, and
  confirms it produces output.

Run the tests locally with `pip install -r requirements-dev.txt && pytest`.

## Repo layout

```
generate_data.py              synthetic data + seeded defects (ground truth)
validation/
  checks.yml                  declarative checks for the raw source
  checks_marts.yml            hard-gate checks for the cleaned marts
  run_checks.py               the validation engine (DuckDB or BigQuery)
  soda_checks.example.yml     the same checks in Soda syntax (reference)
dbt/ecommerce/
  models/staging/             cast + rename, no repair (defects stay visible)
  models/marts/               dim_customers, dim_products, fct_orders
  models/**/schema.yml        dbt tests
spark/daily_revenue_agg.py    PySpark aggregation with in-job validation
airflow/dags/                 the orchestration DAG (two validation gates)
dashboards/                   Looker Studio setup
docs/architecture.md          data model + validation strategy + decisions
tests/test_validation.py      asserts the validator catches the seeded defects
.github/workflows/ci.yml      CI: framework tests + Spark smoke on every push
```

## What was verified locally vs runs on GCP

Verified end to end on this machine against DuckDB and local Spark: the
generator, the validation framework (it catches the seeded defects and gates),
and the PySpark aggregation (it validates and writes output). The dbt models,
the BigQuery load, the Airflow DAG, and the Spark-to-BigQuery path are written
for the GCP environment and run there; they are not exercised in the local
DuckDB run.

## Talking points for an interview

- Two gates, two roles: source validation is *soft* (it measures incoming
  quality and publishes it to a dashboard without blocking), marts validation is
  *hard* (it blocks publish if the cleaning regressed). Knowing when to alert vs
  when to block is the real data-quality judgement.
- dbt test severity is tuned deliberately: structural source issues are `warn`
  so the build completes, mart keys are `error`. Flip source tests to `error` to
  block at the dbt layer in production.
- The validation framework is engine-portable (same `checks.yml` on DuckDB and
  BigQuery) and maps 1:1 onto Great Expectations and Soda; the lightweight
  runner exists only to keep the project self-contained.
- Reconciliation and referential checks (line-total math, foreign keys) are the
  ones that catch real pipeline bugs, not just schema drift.
- A couple of order-level violation counts come out one higher than the seeded
  count because a duplicated order row also carried another defect. That is
  correct row-level counting, and a good example of how defects compound.
