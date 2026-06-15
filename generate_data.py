"""
Synthetic e-commerce data generator.

Produces four raw CSV tables (customers, products, orders, order_items) and
*deliberately* seeds a known number of data-quality defects into them. The
validation layer (dbt tests + the declarative checks framework) is expected to
catch exactly these defects, which makes the project demonstrable: you know the
ground truth, so you can show the framework finding it.

Run:
    python generate_data.py --rows 5000 --out data/raw

Every defect type is controlled by a rate constant below and the exact count
seeded is printed at the end so it can be compared against validation output.
"""

from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta

import pandas as pd

SEED = 42

CATEGORIES = ["Electronics", "Home", "Garden", "Toys", "Apparel", "Sports", "Beauty"]
COUNTRIES = ["BA", "SE", "DE", "SI", "HR", "AT", "NO", "FR"]
ORDER_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]

# --- defect injection rates (fraction of eligible rows) ---------------------
RATE_DUP_ORDER_ID = 0.010        # duplicate order_id (breaks uniqueness)
RATE_ORPHAN_ORDER = 0.015        # order.customer_id not present in customers
RATE_ORPHAN_ITEM = 0.015         # order_item.product_id not present in products
RATE_NULL_EMAIL = 0.020          # customer.email is null
RATE_BAD_EMAIL = 0.020           # customer.email malformed (no @)
RATE_NEG_QTY = 0.010             # order_item.quantity <= 0
RATE_NEG_PRICE = 0.010           # order_item.unit_price < 0
RATE_FUTURE_ORDER = 0.010        # order.order_date in the future
RATE_BAD_STATUS = 0.010          # order.status outside the accepted set
RATE_LINE_MISMATCH = 0.015       # order_item.line_total != quantity * unit_price


def _rng() -> random.Random:
    return random.Random(SEED)


def generate(n_customers: int, n_products: int, n_orders: int, n_items: int):
    rng = _rng()
    counts: dict[str, int] = {}
    now = datetime(2025, 6, 1)

    # ---- customers ---------------------------------------------------------
    customers = []
    for i in range(1, n_customers + 1):
        created = now - timedelta(days=rng.randint(1, 900))
        email = f"user{i}@example.com"
        roll = rng.random()
        if roll < RATE_NULL_EMAIL:
            email = None
            counts["null_email"] = counts.get("null_email", 0) + 1
        elif roll < RATE_NULL_EMAIL + RATE_BAD_EMAIL:
            email = f"user{i}.example.com"  # missing @
            counts["bad_email"] = counts.get("bad_email", 0) + 1
        customers.append(
            {
                "customer_id": i,
                "email": email,
                "country": rng.choice(COUNTRIES),
                "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    customer_ids = [c["customer_id"] for c in customers]

    # ---- products ----------------------------------------------------------
    products = []
    for i in range(1, n_products + 1):
        products.append(
            {
                "product_id": i,
                "product_name": f"Product {i}",
                "category": rng.choice(CATEGORIES),
                "unit_price": round(rng.uniform(2.0, 400.0), 2),
            }
        )
    product_ids = [p["product_id"] for p in products]
    product_price = {p["product_id"]: p["unit_price"] for p in products}

    # ---- orders ------------------------------------------------------------
    orders = []
    next_order_id = 1
    for _ in range(n_orders):
        oid = next_order_id
        next_order_id += 1

        cust = rng.choice(customer_ids)
        if rng.random() < RATE_ORPHAN_ORDER:
            cust = max(customer_ids) + rng.randint(1, 9999)  # nonexistent customer
            counts["orphan_order"] = counts.get("orphan_order", 0) + 1

        order_date = now - timedelta(days=rng.randint(0, 365))
        if rng.random() < RATE_FUTURE_ORDER:
            order_date = now + timedelta(days=rng.randint(5, 120))  # future
            counts["future_order"] = counts.get("future_order", 0) + 1

        status = rng.choice(ORDER_STATUSES)
        if rng.random() < RATE_BAD_STATUS:
            status = rng.choice(["unknown", "PENDING", "done", "n/a"])  # not in set
            counts["bad_status"] = counts.get("bad_status", 0) + 1

        orders.append(
            {
                "order_id": oid,
                "customer_id": cust,
                "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
            }
        )

        # duplicate order_id (same id emitted twice)
        if rng.random() < RATE_DUP_ORDER_ID:
            dup = dict(orders[-1])
            orders.append(dup)
            counts["dup_order_id"] = counts.get("dup_order_id", 0) + 1

    valid_order_ids = list({o["order_id"] for o in orders})

    # ---- order_items -------------------------------------------------------
    items = []
    for j in range(1, n_items + 1):
        oid = rng.choice(valid_order_ids)
        pid = rng.choice(product_ids)
        if rng.random() < RATE_ORPHAN_ITEM:
            pid = max(product_ids) + rng.randint(1, 9999)  # nonexistent product
            counts["orphan_item"] = counts.get("orphan_item", 0) + 1
            price = round(rng.uniform(2.0, 400.0), 2)
        else:
            price = product_price[pid]

        qty = rng.randint(1, 6)
        if rng.random() < RATE_NEG_QTY:
            qty = rng.choice([0, -1, -2])
            counts["neg_qty"] = counts.get("neg_qty", 0) + 1

        if rng.random() < RATE_NEG_PRICE:
            price = -abs(price)
            counts["neg_price"] = counts.get("neg_price", 0) + 1

        line_total = round(qty * price, 2)
        if rng.random() < RATE_LINE_MISMATCH:
            line_total = round(line_total + rng.uniform(1.0, 50.0), 2)  # wrong math
            counts["line_mismatch"] = counts.get("line_mismatch", 0) + 1

        items.append(
            {
                "order_item_id": j,
                "order_id": oid,
                "product_id": pid,
                "quantity": qty,
                "unit_price": price,
                "line_total": line_total,
            }
        )

    return (
        pd.DataFrame(customers),
        pd.DataFrame(products),
        pd.DataFrame(orders),
        pd.DataFrame(items),
        counts,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000, help="approx orders")
    parser.add_argument("--out", default="data/raw")
    args = parser.parse_args()

    n_orders = args.rows
    n_customers = max(100, n_orders // 5)
    n_products = max(50, n_orders // 20)
    n_items = n_orders * 2

    os.makedirs(args.out, exist_ok=True)
    cust, prod, orders, items, counts = generate(
        n_customers, n_products, n_orders, n_items
    )

    cust.to_csv(os.path.join(args.out, "customers.csv"), index=False)
    prod.to_csv(os.path.join(args.out, "products.csv"), index=False)
    orders.to_csv(os.path.join(args.out, "orders.csv"), index=False)
    items.to_csv(os.path.join(args.out, "order_items.csv"), index=False)

    print(f"Wrote raw CSVs to {args.out}/")
    print(f"  customers   : {len(cust):>7}")
    print(f"  products    : {len(prod):>7}")
    print(f"  orders      : {len(orders):>7}")
    print(f"  order_items : {len(items):>7}")
    print("\nSeeded data-quality defects (ground truth for validation):")
    for k in sorted(counts):
        print(f"  {k:<16}: {counts[k]:>6}")


if __name__ == "__main__":
    main()
