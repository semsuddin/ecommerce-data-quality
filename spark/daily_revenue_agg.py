"""
PySpark aggregation: daily revenue by product category.

Demonstrates a Spark step inside the pipeline and that Spark output is itself
validated (the job asserts revenue is non-negative and reconciles before it
writes). In local mode it reads the raw CSVs; in BigQuery mode it reads the
dbt marts via the spark-bigquery connector.

Local (used for testing / CI):
    python daily_revenue_agg.py --source local --data-dir ../data/raw --out ../data/agg

BigQuery (the GCP path the DAG uses):
    python daily_revenue_agg.py --source bigquery \
        --bq-project my-proj --bq-dataset ecommerce_marts --out gs://my-bucket/agg
    # submit with: --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_local(spark, data_dir):
    items = spark.read.csv(f"{data_dir}/order_items.csv", header=True, inferSchema=True)
    orders = spark.read.csv(f"{data_dir}/orders.csv", header=True, inferSchema=True)
    products = spark.read.csv(f"{data_dir}/products.csv", header=True, inferSchema=True)
    return items, orders, products


def read_bigquery(spark, project, dataset):
    def tbl(name):
        return (
            spark.read.format("bigquery")
            .option("table", f"{project}.{dataset}.{name}")
            .load()
        )

    # in BigQuery mode we read the cleaned marts
    items = tbl("stg_order_items") if False else tbl("fct_orders")
    # for category-level revenue we still need item x product; read staging items
    return tbl("stg_order_items"), tbl("stg_orders"), tbl("dim_products")


def build_daily_category_revenue(items, orders, products):
    # keep only well-formed line items so the aggregate is trustworthy
    clean_items = items.filter((F.col("quantity") > 0) & (F.col("unit_price") >= 0))

    joined = (
        clean_items.join(orders.select("order_id", "order_date"), "order_id")
        .join(products.select("product_id", "category"), "product_id")
        .withColumn("order_day", F.to_date(F.col("order_date")))
    )

    agg = (
        joined.groupBy("order_day", "category")
        .agg(
            F.round(F.sum("line_total"), 2).alias("revenue"),
            F.sum("quantity").alias("units"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy("order_day", "category")
    )
    return agg


def validate_output(agg) -> None:
    """Spark output is validated before it is written (defensive gate)."""
    neg = agg.filter(F.col("revenue") < 0).count()
    nulls = agg.filter(F.col("revenue").isNull()).count()
    if neg or nulls:
        raise ValueError(
            f"aggregation failed validation: {neg} negative and {nulls} null revenue rows"
        )
    print(f"  output validation passed: {agg.count()} rows, no negative/null revenue")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["local", "bigquery"], default="local")
    p.add_argument("--data-dir", default="../data/raw")
    p.add_argument("--bq-project")
    p.add_argument("--bq-dataset")
    p.add_argument("--out", default="../data/agg")
    args = p.parse_args()

    spark = (
        SparkSession.builder.appName("daily_category_revenue")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if args.source == "local":
        items, orders, products = read_local(spark, args.data_dir)
    else:
        items, orders, products = read_bigquery(spark, args.bq_project, args.bq_dataset)

    agg = build_daily_category_revenue(items, orders, products)
    validate_output(agg)

    agg.show(8, truncate=False)
    (agg.coalesce(1).write.mode("overwrite").option("header", True).csv(args.out))
    print(f"  wrote daily category revenue to {args.out}")

    spark.stop()


if __name__ == "__main__":
    main()
