"""
Airflow DAG: e-commerce data-quality pipeline.

Flow:
    generate_data        produce raw CSVs (stands in for an extract step)
        -> load_raw_to_bq    load CSVs into the BigQuery raw dataset
        -> validate_source   run the declarative checks on RAW (soft / observability;
                             writes dq_results for the dashboard, does not block)
        -> dbt_build         transform raw -> cleaned marts and run dbt tests
        -> spark_agg         PySpark daily-revenue-by-category aggregation
        -> validate_marts    run the checks on the CLEANED marts (HARD gate; blocks
                             publish if cleaning regressed)
        -> publish           mark the marts/dashboard as refreshed

Two gates, two jobs: source validation quantifies incoming quality without
stopping the run; marts validation enforces that the pipeline produced clean,
trustworthy output before anything downstream consumes it.

Designed to run on local Airflow (Docker / the official image or Astro CLI) so
there is no Cloud Composer cost. The worker needs `bq`, `dbt`, `spark-submit`,
and python on PATH, plus the repo mounted at PROJECT_DIR and GCP credentials in
GOOGLE_APPLICATION_CREDENTIALS.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# --- configuration via environment ----------------------------------------
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/project")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "my-gcp-project")
RAW_DATASET = os.environ.get("RAW_DATASET", "ecommerce_raw")
MARTS_DATASET = os.environ.get("MARTS_DATASET", "ecommerce_marts")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "my-bucket")
RAW_TABLES = ["customers", "products", "orders", "order_items"]

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_dataquality",
    description="Ingest, transform, aggregate and validate e-commerce data on BigQuery",
    schedule="0 5 * * *",            # daily 05:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "bigquery", "spark", "data-quality"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=(
            f"cd {PROJECT_DIR} && python generate_data.py --rows 5000 --out data/raw"
        ),
    )

    load_raw_to_bq = BashOperator(
        task_id="load_raw_to_bq",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f'for t in {" ".join(RAW_TABLES)}; do '
            f"bq load --replace --autodetect --source_format=CSV "
            f"{GCP_PROJECT}:{RAW_DATASET}.$t data/raw/$t.csv; done"
        ),
    )

    # SOFT gate: measure source quality, publish results, never block.
    validate_source = BashOperator(
        task_id="validate_source",
        bash_command=(
            f"cd {PROJECT_DIR}/validation && "
            f"python run_checks.py --engine bigquery "
            f"--bq-project {GCP_PROJECT} --bq-dataset {RAW_DATASET} "
            f"--checks checks.yml --reference-ts now "
            f"--no-fail --results-out /tmp/dq_results.csv && "
            f"bq load --replace --autodetect --source_format=CSV "
            f"{GCP_PROJECT}:{MARTS_DATASET}.dq_results /tmp/dq_results.csv"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_DIR}/dbt/ecommerce && dbt build --target bq"
        ),
    )

    spark_agg = BashOperator(
        task_id="spark_agg",
        bash_command=(
            f"cd {PROJECT_DIR}/spark && spark-submit "
            f"--packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0 "
            f"daily_revenue_agg.py --source bigquery "
            f"--bq-project {GCP_PROJECT} --bq-dataset {MARTS_DATASET} "
            f"--out gs://{GCS_BUCKET}/agg/daily_category_revenue"
        ),
    )

    # HARD gate: marts must be clean post-transform or the DAG fails here.
    validate_marts = BashOperator(
        task_id="validate_marts",
        bash_command=(
            f"cd {PROJECT_DIR}/validation && "
            f"python run_checks.py --engine bigquery "
            f"--bq-project {GCP_PROJECT} --bq-dataset {MARTS_DATASET} "
            f"--checks checks_marts.yml --reference-ts now"
        ),
    )

    publish = EmptyOperator(task_id="publish")

    generate_data >> load_raw_to_bq >> validate_source >> dbt_build
    dbt_build >> [spark_agg, validate_marts]
    [spark_agg, validate_marts] >> publish
