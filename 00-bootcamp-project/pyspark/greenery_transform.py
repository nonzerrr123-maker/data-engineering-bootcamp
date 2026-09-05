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
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "5g")
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("google.cloud.auth.service.account.enable", "true")
    .config("google.cloud.auth.service.account.json.keyfile", args.keyfile)
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
