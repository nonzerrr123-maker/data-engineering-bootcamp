import json
import os
from pathlib import Path

import streamlit as st
from google import genai
from google.cloud import bigquery
from google.oauth2 import service_account


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-b9bafacf-46f9-43ef-bcc")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "deb_bootcamp")
TABLE_ID = os.getenv("GREENERY_RAG_TABLE_ID", "greenery_rag_context")
KEYFILE = Path(
    os.getenv(
        "GCP_KEYFILE",
        str(
            PROJECT_DIR
            / "pyspark"
            / "project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json"
        ),
    )
)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GENERATION_MODEL = os.getenv("GEMINI_GENERATION_MODEL", "gemini-2.5-flash-lite")
EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001")

SYSTEM_INSTRUCTION = [
    "You are a data analyst who works for the Greenery company.",
    "Your mission is to summarize Greenery data and prepare useful reports for management.",
    "Greenery is a tech startup that delivers flowers and houseplants. "
    "Focus on actionable insights that can help grow revenue and acquire customers.",
    "Use the retrieved Greenery context as the factual basis for your answer. "
    "If the context is insufficient, say what information is missing instead of inventing facts.",
]


def require_api_key():
    if not GEMINI_API_KEY:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Use a free-tier Google AI Studio API key."
        )
    return GEMINI_API_KEY


def get_bigquery_client():
    if not KEYFILE.exists():
        raise FileNotFoundError(f"GCP service-account key not found: {KEYFILE}")
    with KEYFILE.open(encoding="utf-8") as handle:
        info = json.load(handle)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)


def get_embedding(client, text):
    result = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=text,
    )
    return result.embeddings[0].values


def build_vector_search_query(vector, top_k=5):
    table = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    return f"""
        SELECT
            base.chunk_type,
            base.state,
            base.product_name,
            base.source_period,
            base.text,
            distance
        FROM VECTOR_SEARCH(
            TABLE \`{table}\`,
            'embedding',
            (SELECT {vector} AS embedding),
            top_k => {int(top_k)},
            distance_type => 'EUCLIDEAN'
        )
        ORDER BY distance
    """


def search_context(bigquery_client, vector, top_k=5):
    rows = bigquery_client.query(
        build_vector_search_query(vector, top_k=top_k)
    ).result()
    return [dict(row.items()) for row in rows]


def ask_gemini(client, prompt):
    response = client.models.generate_content(
        model=GENERATION_MODEL,
        contents=prompt,
        config={
            "system_instruction": SYSTEM_INSTRUCTION,
        },
    )
    return response.text


def answer_question(question, top_k=5):
    genai_client = genai.Client(api_key=require_api_key())
    bigquery_client = get_bigquery_client()

    vector = get_embedding(genai_client, question)
    retrieved = search_context(bigquery_client, vector, top_k=top_k)
    if not retrieved:
        return "No Greenery context was retrieved.", []

    context = "\n\n".join(
        f"[Context {index + 1}] {row['text']}"
        for index, row in enumerate(retrieved)
    )

    prompt = f"""
Given the context below, find the actionable insights and answer the question.
Explain clearly and simply, like I'm 10. Use numbers when the context provides them.
Do not make up data that is not present in the context.

Context:
{context}

Question:
{question}
"""
    return ask_gemini(genai_client, prompt), retrieved


def main():
    st.set_page_config(page_title="Greenery AI Assistant", page_icon="🌿")
    st.title("🌿 Greenery AI Assistant")
    st.caption("Week 6 Bootcamp Project — RAG with Gemini + BigQuery Vector Search")

    question = st.text_input(
        "Ask a question about Greenery:",
        placeholder="What products are popular in California?",
    )
    top_k = st.slider("Retrieved context chunks", min_value=2, max_value=10, value=5)

    if question:
        try:
            with st.spinner("Searching Greenery data and preparing the answer..."):
                answer, retrieved = answer_question(question, top_k=top_k)

            st.subheader("Answer")
            st.write(answer)

            with st.expander("Retrieved context"):
                for index, row in enumerate(retrieved, start=1):
                    st.markdown(
                        f"**{index}. {row['chunk_type']} — distance {row['distance']:.4f}**"
                    )
                    st.write(row["text"])
        except Exception as exc:
            st.error(f"Request failed: {exc}")


if __name__ == "__main__":
    main()
