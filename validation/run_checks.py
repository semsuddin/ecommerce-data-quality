"""
Declarative data-quality validation runner.

Reads validation/checks.yml, converts every check into a "count of violating
rows" query, runs it against the configured warehouse, prints a report, and
exits non-zero if any `error`-severity check fails. That non-zero exit is what
lets Airflow use this as a hard gate between transform and publish.

Engines:
    --engine duckdb    load the raw CSVs into an in-memory DuckDB and validate
                       (used locally and in CI; no cloud account needed)
    --engine bigquery  validate tables already loaded into a BigQuery dataset
                       (the GCP path the Airflow DAG uses)

Examples:
    python run_checks.py --engine duckdb --data-dir ../data/raw
    python run_checks.py --engine bigquery --bq-project my-proj --bq-dataset ecommerce_raw --reference-ts now

Exit codes: 0 = all error checks passed, 1 = one or more error checks failed.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

import yaml

RAW_TABLES = ["customers", "products", "orders", "order_items"]


@dataclass
class Result:
    check_id: str
    table: str
    severity: str
    violations: int
    passed: bool


# --------------------------------------------------------------------------- #
# Dialect helpers: the only SQL that differs between DuckDB and BigQuery.
# --------------------------------------------------------------------------- #
def regex_predicate(col: str, pattern: str, engine: str) -> str:
    if engine == "bigquery":
        return f"REGEXP_CONTAINS(CAST({col} AS STRING), r'{pattern}')"
    return f"regexp_matches(CAST({col} AS VARCHAR), '{pattern}')"


def to_timestamp(expr: str, engine: str) -> str:
    # both dialects accept CAST(... AS TIMESTAMP) for 'YYYY-MM-DD HH:MM:SS'
    return f"CAST({expr} AS TIMESTAMP)"


def now_expr(reference_ts: str, engine: str) -> str:
    if reference_ts.lower() == "now":
        return "CURRENT_TIMESTAMP"
    return to_timestamp(f"'{reference_ts}'", engine)


# --------------------------------------------------------------------------- #
# Build the COUNT-of-violations SQL for a single check.
# --------------------------------------------------------------------------- #
def build_violation_sql(check: dict, fqtn, engine: str, reference_ts: str) -> str:
    t = fqtn(check["table"])
    ctype = check["type"]
    col = check.get("column")

    if ctype == "not_null":
        return f"SELECT COUNT(*) FROM {t} WHERE {col} IS NULL"

    if ctype == "unique":
        # number of key values that appear more than once
        return (
            f"SELECT COUNT(*) FROM (SELECT {col} FROM {t} "
            f"WHERE {col} IS NOT NULL GROUP BY {col} HAVING COUNT(*) > 1) d"
        )

    if ctype == "accepted_values":
        vals = ", ".join(f"'{v}'" for v in check["values"])
        return f"SELECT COUNT(*) FROM {t} WHERE {col} IS NOT NULL AND {col} NOT IN ({vals})"

    if ctype == "min_value":
        return f"SELECT COUNT(*) FROM {t} WHERE {col} < {check['min']}"

    if ctype == "max_value":
        return f"SELECT COUNT(*) FROM {t} WHERE {col} > {check['max']}"

    if ctype == "regex":
        pred = regex_predicate(col, check["pattern"], engine)
        return f"SELECT COUNT(*) FROM {t} WHERE {col} IS NOT NULL AND NOT {pred}"

    if ctype == "not_in_future":
        ref = now_expr(reference_ts, engine)
        return f"SELECT COUNT(*) FROM {t} WHERE {to_timestamp(col, engine)} > {ref}"

    if ctype == "relationship":
        ref_t = fqtn(check["ref_table"])
        ref_c = check["ref_column"]
        return (
            f"SELECT COUNT(*) FROM {t} WHERE {col} IS NOT NULL AND {col} NOT IN "
            f"(SELECT {ref_c} FROM {ref_t} WHERE {ref_c} IS NOT NULL)"
        )

    if ctype == "expression":
        # `expression` must hold for every row; violations are rows where it fails
        return f"SELECT COUNT(*) FROM {t} WHERE NOT ({check['expression']})"

    if ctype == "row_count_min":
        # 0 violations if the table meets the minimum, else 1
        return f"SELECT CASE WHEN COUNT(*) >= {check['min']} THEN 0 ELSE 1 END FROM {t}"

    raise ValueError(f"unknown check type: {ctype}")


# --------------------------------------------------------------------------- #
# Engines
# --------------------------------------------------------------------------- #
def run_duckdb(checks_doc: dict, data_dir: str, reference_ts: str):
    import duckdb

    con = duckdb.connect()
    for tbl in RAW_TABLES:
        con.execute(
            f"CREATE TABLE {tbl} AS SELECT * FROM read_csv_auto('{data_dir}/{tbl}.csv', header=true)"
        )

    def fqtn(name: str) -> str:
        return name

    results = []
    for check in checks_doc["checks"]:
        sql = build_violation_sql(check, fqtn, "duckdb", reference_ts)
        violations = int(con.execute(sql).fetchone()[0])
        sev = check.get("severity", "error")
        passed = violations == 0 or sev == "warn"
        results.append(Result(check["id"], check["table"], sev, violations, passed))
    return results


def run_bigquery(checks_doc: dict, project: str, dataset: str, reference_ts: str):
    from google.cloud import bigquery  # imported only when this engine is used

    client = bigquery.Client(project=project)

    def fqtn(name: str) -> str:
        return f"`{project}.{dataset}.{name}`"

    results = []
    for check in checks_doc["checks"]:
        sql = build_violation_sql(check, fqtn, "bigquery", reference_ts)
        violations = int(list(client.query(sql).result())[0][0])
        sev = check.get("severity", "error")
        passed = violations == 0 or sev == "warn"
        results.append(Result(check["id"], check["table"], sev, violations, passed))
    return results


# --------------------------------------------------------------------------- #
def report(results) -> int:
    width = max(len(r.check_id) for r in results)
    print("\nData-quality validation report")
    print("=" * (width + 34))
    print(f"{'check':<{width}}  {'table':<12} {'sev':<6} {'viol':>6}  status")
    print("-" * (width + 34))
    n_err_fail = 0
    n_warn = 0
    for r in results:
        if not r.passed:
            status = "FAIL"
            n_err_fail += 1
        elif r.severity == "warn" and r.violations > 0:
            status = "warn"
            n_warn += 1
        else:
            status = "pass"
        print(
            f"{r.check_id:<{width}}  {r.table:<12} {r.severity:<6} "
            f"{r.violations:>6}  {status}"
        )
    print("-" * (width + 34))
    print(
        f"{len(results)} checks | {n_err_fail} failed (error) | "
        f"{n_warn} warnings | "
        f"{len(results) - n_err_fail - n_warn} clean"
    )
    return 1 if n_err_fail > 0 else 0


def write_results_csv(results, path: str) -> None:
    import csv
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).isoformat()
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_ts", "check_id", "table", "severity", "violations", "status"])
        for r in results:
            status = "pass" if r.passed and r.violations == 0 else (
                "warn" if r.severity == "warn" else "fail"
            )
            w.writerow([ts, r.check_id, r.table, r.severity, r.violations, status])
    print(f"  wrote results to {path}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--engine", choices=["duckdb", "bigquery"], default="duckdb")
    p.add_argument("--checks", default="checks.yml")
    p.add_argument("--data-dir", default="../data/raw", help="duckdb engine: CSV dir")
    p.add_argument("--bq-project")
    p.add_argument("--bq-dataset")
    p.add_argument(
        "--reference-ts",
        default=None,
        help="override freshness reference; 'now' uses CURRENT_TIMESTAMP",
    )
    p.add_argument(
        "--no-fail",
        action="store_true",
        help="report only; always exit 0 (use for soft/observability runs)",
    )
    p.add_argument("--results-out", help="write the report to this CSV path")
    args = p.parse_args()

    with open(args.checks) as f:
        doc = yaml.safe_load(f)

    reference_ts = args.reference_ts or doc.get("reference_ts", "now")

    if args.engine == "duckdb":
        results = run_duckdb(doc, args.data_dir, reference_ts)
    else:
        if not (args.bq_project and args.bq_dataset):
            sys.exit("bigquery engine requires --bq-project and --bq-dataset")
        results = run_bigquery(doc, args.bq_project, args.bq_dataset, reference_ts)

    code = report(results)
    if args.results_out:
        write_results_csv(results, args.results_out)
    sys.exit(0 if args.no_fail else code)


if __name__ == "__main__":
    main()
