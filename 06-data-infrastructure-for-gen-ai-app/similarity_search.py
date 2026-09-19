from week6_common import (
    DATASET_ID,
    GCP_PROJECT_ID,
    SAMPLE_TABLE_ID,
    get_bigquery_client,
    get_embedding,
    get_genai_client,
)


def build_vector_search_query(vec, table_id=SAMPLE_TABLE_ID, top_k=3):
    return f"""
        SELECT
            base.text,
            distance
        FROM
        VECTOR_SEARCH(
            TABLE \`{GCP_PROJECT_ID}.{DATASET_ID}.{table_id}\`,
            'embedding',
            (SELECT {vec} AS embedding),
            top_k => {int(top_k)},
            distance_type => 'EUCLIDEAN'
        )
    """


def search_similar_texts(bigquery_client, vec, table_id=SAMPLE_TABLE_ID, top_k=3):
    query_job = bigquery_client.query(
        build_vector_search_query(vec, table_id=table_id, top_k=top_k)
    )
    return list(query_job.result())


def main():
    genai_client = get_genai_client()
    bigquery_client = get_bigquery_client()

    query_text = (
        "QR codes systems for COVID-19.\n"
        "Simple tools for bars, restaurants, offices, and other small proximity businesses."
    )
    vec = get_embedding(genai_client, query_text).values
    for row in search_similar_texts(bigquery_client, vec):
        print(row.text)
        print("distance:", row.distance)


if __name__ == "__main__":
    main()
