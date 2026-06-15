# Architecture

## Overview

```
                       ┌──────────────┐
   generate_data  ───▶ │  CSV (raw)   │   synthetic e-commerce data
                       └──────┬───────┘   with seeded quality defects
                              │
                     load_raw_to_bq
                              │
                       ┌──────▼───────────────┐
                       │ BigQuery: ecommerce_raw│
                       └──────┬─────────────────┘
                              │
              ┌───── validate_source (SOFT) ──────┐  run_checks.py + checks.yml
              │   measures source quality,        │  → writes dq_results
              │   never blocks the run            │
              └───────────────┬───────────────────┘
                              │
                          dbt_build              dbt: staging → marts (+ dbt tests)
                              │
                ┌─────────────┴──────────────┐
                │                            │
            spark_agg                 validate_marts (HARD)   run_checks.py +
        daily revenue by             marts must be clean      checks_marts.yml
        category (PySpark)           or the DAG fails here
                │                            │
                └─────────────┬──────────────┘
                              │
                          publish  ──▶  Looker Studio (business + DQ pages)
```

## Data model

Raw (source) tables, loaded as-is after casting:

| table        | grain          | key            | notable columns                         |
|--------------|----------------|----------------|-----------------------------------------|
| `customers`  | one per customer | `customer_id` | `email`, `country`, `created_at`        |
| `products`   | one per product  | `product_id`  | `category`, `unit_price`                |
| `orders`     | one per order    | `order_id`    | `customer_id` (FK), `order_date`, `status` |
| `order_items`| one per line     | `order_item_id` | `order_id` (FK), `product_id` (FK), `quantity`, `unit_price`, `line_total` |

Marts (cleaned, dimensional):

- `dim_customers`, deduplicated on `customer_id`, adds `email_is_valid`.
- `dim_products`, deduplicated on `product_id`.
- `fct_orders`, order grain; reconstructs `order_amount`, `item_count`,
  `total_quantity` from `order_items`, and an `order_is_valid` flag.

## The validation strategy (three layers, two roles)

The core idea: validate at more than one layer, and be explicit about which
checks *alert* and which checks *block*.

1. **dbt tests** (`schema.yml`) run inside `dbt build`. Structural source defects
   are set to `severity: warn` so the build completes and the marts/dashboard
   populate; mart primary-key tests stay at `error`. In production you flip the
   source tests to `error` to block at the dbt layer.

2. **Declarative checks framework** (`run_checks.py` + `checks.yml` /
   `checks_marts.yml`). A small engine that turns each declarative check into a
   "count of violating rows" query and reports pass/warn/fail. It runs in two
   roles:
   - against **raw** as a *soft* gate (`--no-fail`), quantifies incoming
     quality and writes `dq_results` for the dashboard;
   - against the **cleaned marts** as a *hard* gate, non-zero exit fails the
     Airflow task and blocks `publish` if the cleaning logic regressed.

3. **In-job Spark validation**, the PySpark aggregation asserts non-negative,
   non-null revenue before it writes, so a bad aggregate never reaches the
   dashboard.

The same `checks.yml` runs unchanged on DuckDB (local/CI) and BigQuery (GCP);
only the `--engine` flag changes. The check types supported: `not_null`,
`unique`, `accepted_values`, `min_value`/`max_value`, `regex`, `not_in_future`,
`relationship` (foreign key), `expression` (cross-column rules, e.g. line-total
reconciliation), and `row_count_min`. This maps 1:1 onto Great Expectations
expectations and Soda checks, `soda_checks.example.yml` shows the identical
checks in Soda syntax.

## Why these tools

- **BigQuery**, serverless warehouse, generous free tier, native Looker Studio
  connection. No cluster to manage.
- **dbt**, SQL transforms with dependency management, plus tests and docs as
  first-class citizens, which is exactly the in-pipeline validation layer.
- **Spark (PySpark)**, the distributed-processing step; runs locally for the
  demo and on Dataproc/`spark-submit` with the BigQuery connector in production.
- **Airflow**, orchestration and, importantly, the place the two validation
  gates are wired into the run so quality is enforced, not optional.
- **Looker Studio**, free, native, and good enough to carry both a business
  page and a data-quality page.

## Cost

Designed for near-zero spend: Airflow and Spark run locally in Docker (no Cloud
Composer ≈ $300+/mo, no standing Dataproc cluster), and BigQuery storage/query
for this volume sits inside the free tier. Looker Studio is free. The only
reason to spend is if you specifically want managed Composer/Dataproc on the CV,
which is a legitimate choice, see the README.
