import pandas as pd
from google.cloud import bigquery

from week6_common import (
    COURSE_TABLE_ID,
    DATASET_ID,
    GCP_PROJECT_ID,
    ask_gemini,
    full_table_id,
    get_bigquery_client,
    get_embedding,
    get_genai_client,
)


COURSE_TEXTS = [
    "คอร์ส Probability for Data Science - ความน่าจะเป็นถูกนำมาใช้ในงาน Data Science ในงานด้านวิเคราะห์ข้อมูลและสร้างโมเดล เพื่อทำให้มั่นใจได้ว่าข้อมูลที่ได้มามีความหมายและมี Insight รวมถึงการนำข้อมูลไปสร้างโมเดล Machine Learning และการทำนาย",
    "คอร์ส Data Pipelines with Airflow - คอร์สสำหรับ Data Engineer ที่สอนการสร้าง End-to-End Data Pipelines ด้วย Airflow ตั้งแต่การอ่านข้อมูล ทำความสะอาด ปรับรูปแบบ และโหลดเข้า Data Lake/Data Warehouse แบบอัตโนมัติ",
]


def load_course_embeddings(bigquery_client, genai_client):
    df = pd.DataFrame({"text": COURSE_TEXTS})
    df["embedding"] = df["text"].map(
        lambda text: get_embedding(genai_client, text).values
    )

    schema = [
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    destination = full_table_id(COURSE_TABLE_ID)
    load_job = bigquery_client.load_table_from_dataframe(
        df, destination, job_config=job_config
    )
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {destination}")


def build_vector_search_query(vec, top_k=3):
    return f"""
        SELECT
            base.text,
            distance
        FROM
        VECTOR_SEARCH(
            TABLE \`{GCP_PROJECT_ID}.{DATASET_ID}.{COURSE_TABLE_ID}\`,
            'embedding',
            (SELECT {vec} AS embedding),
            top_k => {int(top_k)},
            distance_type => 'EUCLIDEAN'
        )
    """


def search_similar_texts(bigquery_client, vec, top_k=3):
    results = bigquery_client.query(build_vector_search_query(vec, top_k)).result()
    similar_texts = []
    for row in results:
        similar_texts.append(row.text)
        print(row.text)
        print("distance:", row.distance)
    return similar_texts


def main():
    genai_client = get_genai_client()
    bigquery_client = get_bigquery_client()

    # The workshop intentionally refreshes the small course corpus before retrieval.
    load_course_embeddings(bigquery_client, genai_client)

    question = "อยากทำสาย Data Engineer ควรเรียนคอร์สอะไรดี?"
    vec = get_embedding(genai_client, question).values
    similar_texts = search_similar_texts(bigquery_client, vec)

    context = " / ".join(similar_texts)
    prompt_with_context = f"""
Use only the supplied course context when possible.

Context:
{context}

Question:
{question}
"""
    response = ask_gemini(
        genai_client,
        prompt=prompt_with_context,
        system_instruction=[
            "You are a course recommender.",
            "Your mission is to recommend courses for people who want to upskill and switch careers.",
        ],
    )
    print(response)


if __name__ == "__main__":
    main()
