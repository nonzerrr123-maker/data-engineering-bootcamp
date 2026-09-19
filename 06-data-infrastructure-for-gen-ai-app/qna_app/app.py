import sys
from pathlib import Path

import streamlit as st

PARENT = Path(__file__).resolve().parents[1]
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from week6_common import (  # noqa: E402
    COURSE_TABLE_ID,
    DATASET_ID,
    GCP_PROJECT_ID,
    ask_gemini,
    get_bigquery_client,
    get_embedding,
    get_genai_client,
)


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
    return [row.text for row in results]


def answer_question(user_question):
    genai_client = get_genai_client()
    bigquery_client = get_bigquery_client()

    vec = get_embedding(genai_client, user_question).values
    similar_texts = search_similar_texts(bigquery_client, vec)
    context = " / ".join(similar_texts)

    prompt_with_context = f"""
Use only the supplied course context when possible.

Context:
{context}

Question:
{user_question}
"""
    return ask_gemini(
        genai_client,
        prompt_with_context,
        system_instruction=[
            "You are a course recommender.",
            "Your mission is to recommend courses for people who want to upskill and switch careers.",
        ],
    )


def main():
    st.title("Q&A App")
    st.caption("Week 6 RAG workshop: Gemini + BigQuery Vector Search")

    user_question = st.text_input(
        "Ask a question:",
        placeholder="อยากทำสาย Data Engineer ควรเรียนคอร์สอะไรดี?",
    )

    if user_question:
        try:
            with st.spinner("Cooking up a response... 🍳", show_time=True):
                response = answer_question(user_question)
            st.subheader("AI Assistant:")
            st.write(response)
        except Exception as exc:
            st.error(f"Request failed: {exc}")


if __name__ == "__main__":
    main()
