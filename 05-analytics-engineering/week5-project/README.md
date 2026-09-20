# Day 5 Bootcamp Project — Greenery Analytics Engineering

This directory is the Week 5 **Bootcamp Project** implementation based on the supplied Day 5 project brief.

## Project scope

1. Finish the Staging layer for all Greenery sources:
   - users
   - orders
   - addresses
   - products
   - order_items
   - events
   - promos
2. Build `int_orders_products__joined` and the `fct_orders` fact model.
3. Build `int_products_orders__joined` for product/order analysis.
4. Answer the three project challenges:
   - `user_repeat_rate`
   - `add_to_cart_rate`
   - `conversion_rate_by_product`
5. Keep the marts ready for visualization from BigQuery / Looker Studio.

## Model graph

```text
Greenery BigQuery sources
        |
        v
stg_greenery__*
        |
        +--> int_orders_products__joined --> fct_orders
        |
        +--> user_repeat_rate
        +--> add_to_cart_rate
        |
        +--> int_products_orders__joined --> conversion_rate_by_product
```

## Automated validation

`tools/verify_project.py` runs the **actual dbt model SQL** against the repository's Greenery CSV fixtures using SQLite after resolving `source()` and `ref()` references. It checks:

- every required Day 5 project model exists;
- all 7 staging models execute;
- both intermediate models execute;
- `fct_orders` executes;
- all three challenge marts execute;
- repeat rate and add-to-cart rate match independent calculations from raw data;
- conversion-rate output is non-empty and numerically valid.

GitHub Actions also runs dbt dependency resolution and project parsing/compilation so Jinja/YAML/dbt graph errors are caught before merge.

## Looker Studio

The project outputs are designed for the final step in the brief: connect Looker Studio to the BigQuery project/dataset and select the desired mart (`fct_orders`, `user_repeat_rate`, `add_to_cart_rate`, or `conversion_rate_by_product`) as a report data source.
