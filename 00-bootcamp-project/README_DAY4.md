# Day 4 - Bootcamp Project

This folder implements the Greenery Airflow pipelines from the Day 4 Bootcamp Project.

## Pipelines

Non-partitioned tables:

- `addresses`
- `order_items`
- `products`
- `promos`

Partitioned tables:

- `events`
- `orders`
- `users`

Each pipeline follows:

`Extract API -> GCS raw -> Spark transform -> GCS cleaned -> BigQuery`

Partitioned DAGs use `BranchPythonOperator` so dates with no data follow the `do_nothing` branch and finish at `end` with `trigger_rule="one_success"`.

## Prepare environment

```bash
cd 00-bootcamp-project
mkdir -p ./dags ./config ./logs ./plugins ./tests ./pyspark ./spark-events
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Place the service-account JSON used by the workshop in `pyspark/`. The DAGs currently expect:

```text
/opt/spark/pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json
```

`pyspark/*.json` is ignored by Git and must not be committed.

## Start Airflow + Spark

```bash
docker compose up --build -d
```

Check services:

```bash
docker compose ps
```

## Spark connection

Create the Airflow connection used by `SparkSubmitOperator`:

```text
Connection ID: my_spark
Connection Type: Spark
Host: spark://spark-master
Port: 7077
Deploy mode: client
Spark binary: spark-submit
```

## Backfill partitioned tables

Enter the scheduler container:

```bash
docker compose exec airflow-scheduler bash
```

Events:

```bash
airflow backfill create --dag-id greenery_events_data_pipeline --from-date 2021-02-01 --to-date 2021-02-25
```

Orders:

```bash
airflow backfill create --dag-id greenery_orders_data_pipeline --from-date 2021-02-10 --to-date 2021-02-11
```

Users:

```bash
airflow backfill create --dag-id greenery_users_data_pipeline --from-date 2020-01-05 --to-date 2020-12-26
```

To reprocess completed runs, append:

```text
--reprocess-behavior completed
```

## GCP targets

```text
Project: project-b9bafacf-46f9-43ef-bcc
Bucket: deb-bootcamp-005-non
Dataset: deb_bootcamp
Raw: gs://deb-bootcamp-005-non/raw/greenery/...
Cleaned: gs://deb-bootcamp-005-non/cleaned/greenery/...
```

Spark writes cleaned data as Parquet. The BigQuery load tasks therefore read `*.parquet` from the cleaned zone.
