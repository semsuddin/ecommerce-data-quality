# Looker Studio dashboard

Looker Studio is the analytics/low-code layer for this project. It connects
natively to BigQuery (no connector cost, no extra infra), which is why it is
the featured tool here. Two pages tell the story: business metrics, and data
quality.

## Connect

1. Open https://lookerstudio.google.com → **Create → Data source → BigQuery**.
2. Authorise, pick your project, then add these tables/queries from the
   `ecommerce_marts` dataset:
   - `fct_orders`, order-grain fact (totals, status, validity flag)
   - `dim_products`, for category breakdowns (join on a custom query, below)
   - `dq_results`, the validation report the DAG loads each run
3. For category revenue, add a **custom query** data source instead of the bare
   table:

   ```sql
   SELECT
     DATE(o.order_date)        AS order_day,
     p.category                AS category,
     SUM(i.line_total)         AS revenue,
     COUNT(DISTINCT o.order_id) AS orders
   FROM `PROJECT.ecommerce_marts.fct_orders` o
   JOIN `PROJECT.ecommerce_raw.order_items` i USING (order_id)
   JOIN `PROJECT.ecommerce_marts.dim_products` p USING (product_id)
   WHERE i.quantity > 0 AND i.unit_price >= 0
   GROUP BY 1, 2
   ```

   (Or point Looker Studio at the Spark output in GCS if you ran `spark_agg`.)

## Page 1, Business metrics

- **Scorecards:** total revenue, order count, average order value
  (`order_amount` from `fct_orders`, filter `order_is_valid = true`).
- **Time series:** revenue by `order_day`.
- **Bar chart:** revenue by `category` (custom query source).
- **Table:** top orders by `order_amount`.
- **Control:** date-range filter on `order_date`.

The `order_is_valid` flag lets you toggle between "all rows" and "trustworthy
rows" with one filter, a concrete way to show the value of the validation work.

## Page 2, Data quality (the differentiator)

Built on `dq_results`:

- **Scorecard:** count of checks where `status = 'fail'` (the headline DQ number).
- **Bar chart:** `violations` by `check_id`, coloured by `severity`.
- **Table:** `check_id`, `table`, `severity`, `violations`, `status` for the
  latest `run_ts`.
- **Scorecard:** % of error-severity checks passing.

This page is what separates a data-QA portfolio piece from a generic dashboard:
it shows you do not just move data, you measure and report its quality each run.

## Alternative: Retool

The same `fct_orders` and `dq_results` tables drop straight into a Retool app
(BigQuery resource → SQL query → table/chart components) if you want an
internal-tool style surface instead of a published report. Looker Studio is
featured here because it is free and native to BigQuery; Retool is the better
fit when you need write-back or operational actions, which is worth being able
to articulate in an interview.
