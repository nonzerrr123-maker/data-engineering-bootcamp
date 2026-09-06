# Week 5 Complete Workshop - Analytics Engineering with dbt

This folder is the canonical, non-secret solution used by `bootstrap_week5.sh`.

It follows the Day 5 Workshop:
- BigQuery source declarations for all 7 Greenery tables
- dbt models, materialization, docs
- generic + singular + unit tests
- snapshot
- dbt_expectations 0.10.10
- staging / intermediate / marts model layers
- challenge models: users, orders, highest-order state, basket size, US regions
- Airflow + Astronomer Cosmos preparation

Secrets are never stored here. `bootstrap_week5.sh` discovers the local service-account
JSON file and writes `profiles.yml` only inside the Codespace working copy.
