#!/usr/bin/env python3
"""Self-check the Day 5 Bootcamp Project without cloud credentials.

The checker executes the actual dbt model SQL against the Greenery CSV fixtures
using SQLite after resolving dbt source()/ref() Jinja calls. This validates the
model graph, column names, joins, and the three challenge calculations.
"""

from __future__ import annotations

import csv
import math
import re
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PROJECT = REPO_ROOT / "05-analytics-engineering" / "week5-project"
DATA = REPO_ROOT / "dataset" / "greenery"

STAGING = [
    "stg_greenery__users",
    "stg_greenery__orders",
    "stg_greenery__addresses",
    "stg_greenery__products",
    "stg_greenery__order_items",
    "stg_greenery__events",
    "stg_greenery__promos",
]

PROJECT_MODELS = [
    ("int_orders_products__joined", "models/intermediate/int_orders_products__joined.sql"),
    ("int_products_orders__joined", "models/intermediate/int_products_orders__joined.sql"),
    ("fct_orders", "models/marts/fct_orders.sql"),
    ("user_repeat_rate", "models/marts/user_repeat_rate.sql"),
    ("add_to_cart_rate", "models/marts/add_to_cart_rate.sql"),
    ("conversion_rate_by_product", "models/marts/conversion_rate_by_product.sql"),
]

SOURCE_FILES = {
    "users": "users.csv",
    "orders": "orders.csv",
    "addresses": "addresses.csv",
    "products": "products.csv",
    "order_items": "order_items.csv",
    "events": "events.csv",
    "promos": "promos.csv",
}


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    raise SystemExit(1)


def require_files() -> None:
    required = [PROJECT / "dbt_project.yml", PROJECT / "models/staging/greenery/_src.yml"]
    required += [PROJECT / f"models/staging/greenery/{name}.sql" for name in STAGING]
    required += [PROJECT / rel for _, rel in PROJECT_MODELS]
    missing = [str(path.relative_to(REPO_ROOT)) for path in required if not path.is_file()]
    if missing:
        fail("missing required project files:\n  " + "\n  ".join(missing))

    required_tokens = {
        "models/staging/greenery/stg_greenery__products.sql": [
            "product_id as product_guid",
            "name as product_name",
            "price",
            "inventory",
        ],
        "models/staging/greenery/stg_greenery__order_items.sql": [
            "order_id as order_guid",
            "product_id as product_guid",
            "quantity",
        ],
        "models/staging/greenery/stg_greenery__events.sql": [
            "event_id as event_guid",
            "session_id as session_guid",
            "created_at as event_created_at_utc",
            "event_type",
        ],
        "models/staging/greenery/stg_greenery__promos.sql": [
            "promo_id as promo_guid",
            "status as promo_status",
        ],
        "models/marts/user_repeat_rate.sql": ["user_orders >= 2", "repeat_rate"],
        "models/marts/add_to_cart_rate.sql": ["event_type = 'add_to_cart'", "add_to_cart_rate"],
        "models/marts/conversion_rate_by_product.sql": [
            "event_type = 'page_view'",
            "event_type = 'checkout'",
            "conversion_rate",
        ],
    }
    for rel, tokens in required_tokens.items():
        text = (PROJECT / rel).read_text(encoding="utf-8").lower()
        for token in tokens:
            if token.lower() not in text:
                fail(f"{rel} does not contain required project logic: {token}")


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def load_csv(conn: sqlite3.Connection, source_name: str, filename: str) -> int:
    path = DATA / filename
    if not path.is_file():
        fail(f"missing fixture {path.relative_to(REPO_ROOT)}")
    table = f"raw_{source_name}"
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            headers = next(reader)
        except StopIteration:
            fail(f"empty fixture: {filename}")
        conn.execute(
            f"create table {qident(table)} ("
            + ", ".join(f"{qident(col)} text" for col in headers)
            + ")"
        )
        placeholders = ",".join("?" for _ in headers)
        rows = 0
        batch: list[list[str]] = []
        for row in reader:
            batch.append(row)
            rows += 1
            if len(batch) >= 1000:
                conn.executemany(f"insert into {qident(table)} values ({placeholders})", batch)
                batch.clear()
        if batch:
            conn.executemany(f"insert into {qident(table)} values ({placeholders})", batch)
    if rows == 0:
        fail(f"fixture has no data rows: {filename}")
    return rows


CONFIG_RE = re.compile(r"\{\{\s*config\(.*?\)\s*\}\}", re.S | re.I)
SOURCE_RE = re.compile(
    r"\{\{\s*source\(\s*['\"]greenery['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
    re.I,
)
REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", re.I)
RAW_STRING_RE = re.compile(r"\br'([^']*)'")


def render_sql(path: Path) -> str:
    sql = path.read_text(encoding="utf-8")
    sql = CONFIG_RE.sub("", sql)
    sql = SOURCE_RE.sub(lambda m: f"raw_{m.group(1)}", sql)
    sql = REF_RE.sub(lambda m: m.group(1), sql)
    sql = RAW_STRING_RE.sub(lambda m: "'" + m.group(1).replace("''", "'").replace("'", "''") + "'", sql)
    if "{{" in sql or "{%" in sql:
        fail(f"unresolved dbt Jinja in {path.relative_to(REPO_ROOT)}")
    return sql.strip().rstrip(";")


def create_model(conn: sqlite3.Connection, name: str, path: Path) -> int:
    sql = render_sql(path)
    try:
        conn.execute(f"create view {qident(name)} as {sql}")
        # CREATE VIEW can defer column errors in SQLite, so force evaluation.
        conn.execute(f"select * from {qident(name)} limit 1").fetchall()
        return conn.execute(f"select count(*) from {qident(name)}").fetchone()[0]
    except sqlite3.Error as exc:
        print(f"\nRendered SQL for {path.relative_to(REPO_ROOT)}:\n{sql}\n")
        fail(f"SQL execution failed for {name}: {exc}")
    raise AssertionError("unreachable")


def scalar(conn: sqlite3.Connection, sql: str) -> float:
    row = conn.execute(sql).fetchone()
    if row is None or row[0] is None:
        fail(f"query returned no scalar: {sql}")
    return float(row[0])


def main() -> int:
    print("=== Day 5 Bootcamp Project self-check ===")
    require_files()
    print("PASS: required model files and project formulas")

    conn = sqlite3.connect(":memory:")
    conn.create_function(
        "regexp_contains",
        2,
        lambda value, pattern: 0 if value is None else int(re.search(pattern, str(value)) is not None),
    )

    raw_counts = {}
    for source, filename in SOURCE_FILES.items():
        raw_counts[source] = load_csv(conn, source, filename)
    print("PASS: loaded Greenery fixtures", raw_counts)

    stage_counts = {}
    for model in STAGING:
        path = PROJECT / f"models/staging/greenery/{model}.sql"
        stage_counts[model] = create_model(conn, model, path)
        if stage_counts[model] <= 0:
            fail(f"{model} produced no rows")
    print("PASS: all 7 staging models execute", stage_counts)

    model_counts = {}
    for model, rel in PROJECT_MODELS:
        model_counts[model] = create_model(conn, model, PROJECT / rel)
    if model_counts["fct_orders"] <= 0:
        fail("fct_orders produced no rows")
    if model_counts["conversion_rate_by_product"] <= 0:
        fail("conversion_rate_by_product produced no rows")
    print("PASS: intermediate + fact + challenge models execute", model_counts)

    repeat_rate = scalar(conn, "select repeat_rate from user_repeat_rate")
    if not (0.0 <= repeat_rate <= 1.0) or not math.isfinite(repeat_rate):
        fail(f"invalid repeat_rate: {repeat_rate}")

    add_row = conn.execute(
        "select number_of_unique_add_to_cart_sessions, number_of_unique_sessions, add_to_cart_rate "
        "from add_to_cart_rate"
    ).fetchone()
    if add_row is None:
        fail("add_to_cart_rate returned no row")
    add_sessions, all_sessions, add_rate = int(add_row[0]), int(add_row[1]), float(add_row[2])
    if not (0 < add_sessions <= all_sessions and 0.0 <= add_rate <= 1.0 and math.isfinite(add_rate)):
        fail(f"invalid add-to-cart metrics: {add_row}")

    conversions = conn.execute(
        "select product_name, checkout_count, page_view_count, conversion_rate "
        "from conversion_rate_by_product order by product_name"
    ).fetchall()
    if not conversions:
        fail("conversion_rate_by_product returned no products")
    for product_name, checkout_count, page_view_count, rate in conversions:
        if product_name is None or int(page_view_count) <= 0 or rate is None or not math.isfinite(float(rate)):
            fail(f"invalid conversion row: {(product_name, checkout_count, page_view_count, rate)}")
        if float(rate) < 0:
            fail(f"negative conversion rate: {(product_name, rate)}")

    # Independent consistency checks against raw source counts.
    raw_order_count = scalar(conn, "select count(*) from raw_orders")
    staged_order_count = scalar(conn, "select count(*) from stg_greenery__orders")
    if raw_order_count != staged_order_count:
        fail(f"orders staging count changed unexpectedly: raw={raw_order_count}, staged={staged_order_count}")

    expected_repeat = scalar(
        conn,
        "with x as (select user_id, count(distinct order_id) c from raw_orders group by user_id) "
        "select cast(sum(case when c >= 2 then 1 else 0 end) as float64) / cast(count(*) as float64) from x",
    )
    if abs(repeat_rate - expected_repeat) > 1e-12:
        fail(f"repeat rate mismatch: model={repeat_rate}, expected={expected_repeat}")

    expected_add = scalar(
        conn,
        "select cast(count(distinct case when event_type='add_to_cart' then session_id end) as float64) "
        "/ cast(count(distinct session_id) as float64) from raw_events",
    )
    if abs(add_rate - expected_add) > 1e-12:
        fail(f"add-to-cart rate mismatch: model={add_rate}, expected={expected_add}")

    print(f"PASS: user repeat rate = {repeat_rate:.6f}")
    print(
        "PASS: add-to-cart rate = "
        f"{add_rate:.6f} ({add_sessions}/{all_sessions} unique sessions)"
    )
    print(f"PASS: conversion rate by product rows = {len(conversions)}")
    print("PASS: raw-to-staging count and independent metric cross-checks")
    print("\nDAY 5 BOOTCAMP PROJECT CHECKS PASSED ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
