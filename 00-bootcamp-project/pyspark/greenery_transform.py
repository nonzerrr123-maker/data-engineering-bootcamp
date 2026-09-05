import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--keyfile", required=True)
    parser.add_argument("--ds")
    return parser.parse_args()


args = parse_args()

spark = (
    SparkSession.builder
    .appName(f"greenery_{args.table}_transform")
    # The GCS connector is shipped in both the Airflow and Spark images.
    # Use Hadoop-prefixed Spark configs so they are actually propagated to
    # the Hadoop FileSystem used by the driver and executors.
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", args.keyfile)
    # Newer GCS connector releases also understand the fs.gs.auth.* form.
    .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", args.keyfile)
    .getOrCreate()
)

if args.ds:
    source_path = f"gs://{args.bucket}/raw/greenery/{args.table}/{args.table}-{args.ds}.csv"
    output_path = f"gs://{args.bucket}/cleaned/greenery/{args.table}/ds={args.ds}"
else:
    source_path = f"gs://{args.bucket}/raw/greenery/{args.table}/{args.table}.csv"
    output_path = f"gs://{args.bucket}/cleaned/greenery/{args.table}"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
)

df.show(5)
df.printSchema()

df.createOrReplaceTempView(args.table)
result = spark.sql(f"SELECT * FROM {args.table}")
result.write.mode("overwrite").parquet(output_path)

print(f"Transformed {args.table}: {source_path} -> {output_path}")
spark.stop()
