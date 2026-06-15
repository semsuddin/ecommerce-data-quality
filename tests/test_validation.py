"""
Tests that prove the validation framework works: it must detect exactly the
defects the generator seeds. Because the generator is seeded (deterministic),
the violation counts are reproducible, so we assert them precisely.

A couple of order-level counts come out one higher than the raw seeded number
because a duplicated order row also carried another defect (e.g. an orphan
order_id that got duplicated). That is correct row-level counting, so the
expected values below are the *detected* counts, not the seeded counts.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "validation"))

import yaml                        # noqa: E402
import generate_data as gen        # noqa: E402
from run_checks import run_duckdb   # noqa: E402

# parameters matching `generate_data.py --rows 5000`
N_ORDERS = 5000
N_CUSTOMERS = N_ORDERS // 5        # 1000
N_PRODUCTS = N_ORDERS // 20        # 250
N_ITEMS = N_ORDERS * 2             # 10000
REFERENCE_TS = "2025-06-01 00:00:00"

EXPECTED_FAIL_COUNTS = {
    "orders_pk_unique": 50,
    "orders_customer_fk": 74,
    "orders_status_accepted": 68,
    "orders_not_future": 52,
    "items_product_fk": 164,
    "items_qty_positive": 112,
    "items_price_nonneg": 107,
    "items_line_total_reconciles": 140,
}
EXPECTED_CLEAN = {
    "customers_pk_not_null",
    "customers_pk_unique",
    "products_pk_unique",
    "products_price_nonneg",
    "orders_rowcount_min",
    "items_order_fk",
}
EXPECTED_WARN_COUNTS = {
    "customers_email_present": 16,
    "customers_email_format": 15,
}


def _run(tmp_path):
    cust, prod, orders, items, _ = gen.generate(
        N_CUSTOMERS, N_PRODUCTS, N_ORDERS, N_ITEMS
    )
    cust.to_csv(tmp_path / "customers.csv", index=False)
    prod.to_csv(tmp_path / "products.csv", index=False)
    orders.to_csv(tmp_path / "orders.csv", index=False)
    items.to_csv(tmp_path / "order_items.csv", index=False)
    doc = yaml.safe_load((ROOT / "validation" / "checks.yml").read_text())
    results = run_duckdb(doc, str(tmp_path), REFERENCE_TS)
    return {r.check_id: r for r in results}


def test_error_checks_detect_seeded_defects(tmp_path):
    results = _run(tmp_path)
    failing = {
        cid for cid, r in results.items()
        if r.severity == "error" and r.violations > 0
    }
    assert failing == set(EXPECTED_FAIL_COUNTS), failing
    for cid, expected in EXPECTED_FAIL_COUNTS.items():
        assert results[cid].violations == expected, (cid, results[cid].violations)


def test_clean_error_checks_pass(tmp_path):
    results = _run(tmp_path)
    for cid in EXPECTED_CLEAN:
        assert results[cid].severity == "error"
        assert results[cid].violations == 0, (cid, results[cid].violations)
        assert results[cid].passed


def test_warning_checks_flag_email_issues(tmp_path):
    results = _run(tmp_path)
    for cid, expected in EXPECTED_WARN_COUNTS.items():
        assert results[cid].severity == "warn"
        assert results[cid].violations == expected, (cid, results[cid].violations)


def test_gate_would_block(tmp_path):
    # at least one error-severity check fails, so run_checks exits non-zero
    results = _run(tmp_path)
    n_fail = sum(
        1 for r in results.values() if r.severity == "error" and r.violations > 0
    )
    assert n_fail == 8
