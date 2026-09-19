import pandas as pd
from google.cloud import bigquery

from week6_common import (
    SAMPLE_TABLE_ID,
    full_table_id,
    get_bigquery_client,
    get_embedding,
    get_genai_client,
)


SAMPLE_TEXTS = [
    "QR codes systems for COVID-19.\nSimple tools for bars, restaurants, offices, and other small proximity businesses.",
    "QR code, beacon, and other mobile transactions\nWe have created web and mobile tools which enable both companies and consumers to benefit from mobile transaction technologies (QR codes, beacon, and more). These benefits include mobile commerce, social media, lead generation, analytics, networking, and more. ...",
    "Turning experience into better medicine.\nIodine is creating a massive community of people sharing their experience with what works - and what doesn't - in medicine.\nWe believe Iodine is transforming the consumer experience around health, by providing personal, clear, actionable, and trustworthy resources ...",
]


def build_dataframe(genai_client):
    df = pd.DataFrame({"text": SAMPLE_TEXTS})
    df["embedding"] = df["text"].map(
        lambda text: get_embedding(genai_client, text).values
    )
    return df


def load_embeddings(bigquery_client, df, table_id=SAMPLE_TABLE_ID):
    schema = [
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    destination = full_table_id(table_id)
    load_job = bigquery_client.load_table_from_dataframe(
        df, destination, job_config=job_config
    )
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {destination}")


def main():
    genai_client = get_genai_client()
    bigquery_client = get_bigquery_client()
    df = build_dataframe(genai_client)
    print(df[["text"]].head())
    print(f"Embedding dimension: {len(df.iloc[0]['embedding'])}")
    load_embeddings(bigquery_client, df)


if __name__ == "__main__":
    main()
