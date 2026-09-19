from __future__ import annotations

from datetime import date

import pandas as pd


REQUIRED_COLUMNS = {
    "order_guid",
    "order_created_at_utc",
    "state",
    "quantity",
    "product_name",
    "price",
}


def _fmt_products(series: pd.Series, limit: int = 3) -> str:
    ranked = series.sort_values(ascending=False).head(limit)
    return ", ".join(f"{name} ({int(value):,} units)" for name, value in ranked.items())


def _fmt_states(series: pd.Series, limit: int = 3) -> str:
    ranked = series.sort_values(ascending=False).head(limit)
    return ", ".join(f"{name} ({int(value):,} units)" for name, value in ranked.items())


def build_context_chunks(frame: pd.DataFrame, processed_date: str | date) -> pd.DataFrame:
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"fct_orders is missing required columns: {sorted(missing)}")

    df = frame.copy()
    if df.empty:
        raise ValueError("fct_orders returned no rows")

    df["order_created_at_utc"] = pd.to_datetime(
        df["order_created_at_utc"], utc=True, errors="coerce"
    )
    df = df.dropna(
        subset=["order_guid", "order_created_at_utc", "state", "product_name"]
    )
    if df.empty:
        raise ValueError("No usable rows remained after cleaning fct_orders")

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    df["line_revenue"] = df["quantity"] * df["price"]
    df["month"] = (
        df["order_created_at_utc"]
        .dt.tz_convert(None)
        .dt.to_period("M")
        .astype(str)
    )

    processed = pd.Timestamp(processed_date).date()
    min_month = df["month"].min()
    max_month = df["month"].max()
    rows: list[dict] = []

    for state, group in df.groupby("state", dropna=True):
        product_units = group.groupby("product_name")["quantity"].sum()
        total_orders = int(group["order_guid"].nunique())
        total_units = int(round(group["quantity"].sum()))
        revenue = float(group["line_revenue"].sum())
        rows.append(
            {
                "chunk_type": "state_summary",
                "state": str(state),
                "product_name": None,
                "source_period": f"{min_month} to {max_month}",
                "text": (
                    f"Greenery state summary for {state}, covering {min_month} to {max_month}: "
                    f"{total_orders:,} unique orders contained about {total_units:,} items. "
                    f"The top products by units were {_fmt_products(product_units)}. "
                    f"Approximate merchandise revenue from quantity multiplied by product price "
                    f"was $\${revenue:,.2f}."
                ),
                "processed_date": processed,
            }
        )

    for product, group in df.groupby("product_name", dropna=True):
        state_units = group.groupby("state")["quantity"].sum()
        total_orders = int(group["order_guid"].nunique())
        total_units = int(round(group["quantity"].sum()))
        revenue = float(group["line_revenue"].sum())
        rows.append(
            {
                "chunk_type": "product_summary",
                "state": None,
                "product_name": str(product),
                "source_period": f"{min_month} to {max_month}",
                "text": (
                    f"Greenery product summary for {product}, covering {min_month} to {max_month}: "
                    f"{total_units:,} units were included in {total_orders:,} unique orders. "
                    f"The leading states by units were {_fmt_states(state_units)}. "
                    f"Approximate merchandise revenue from this product was $\${revenue:,.2f}."
                ),
                "processed_date": processed,
            }
        )

    for month, group in df.groupby("month", sort=True):
        product_units = group.groupby("product_name")["quantity"].sum()
        state_units = group.groupby("state")["quantity"].sum()
        total_orders = int(group["order_guid"].nunique())
        total_units = int(round(group["quantity"].sum()))
        revenue = float(group["line_revenue"].sum())
        rows.append(
            {
                "chunk_type": "monthly_trend",
                "state": None,
                "product_name": None,
                "source_period": str(month),
                "text": (
                    f"Greenery national monthly trend for {month}: "
                    f"{total_orders:,} unique orders contained about {total_units:,} items. "
                    f"Top products by units were {_fmt_products(product_units)}. "
                    f"Top states by units were {_fmt_states(state_units)}. "
                    f"Approximate merchandise revenue was $\${revenue:,.2f}."
                ),
                "processed_date": processed,
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        raise ValueError("No contextual chunks were produced")
    return result.sort_values(
        ["chunk_type", "source_period", "state", "product_name"],
        na_position="last",
    ).reset_index(drop=True)
