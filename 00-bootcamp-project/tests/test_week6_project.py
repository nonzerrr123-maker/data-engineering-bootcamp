from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pandas as pd


PROJECT = Path(__file__).resolve().parents[1]
DAGS = PROJECT / "dags"
QNA = PROJECT / "qna_app"

if str(DAGS) not in sys.path:
    sys.path.insert(0, str(DAGS))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def sample_fct_orders() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "order_guid": "o1",
                "order_created_at_utc": "2025-03-05T00:00:00Z",
                "state": "California",
                "quantity": 2,
                "product_name": "Monstera",
                "price": 30,
            },
            {
                "order_guid": "o2",
                "order_created_at_utc": "2025-03-11T00:00:00Z",
                "state": "California",
                "quantity": 4,
                "product_name": "Monstera",
                "price": 30,
            },
            {
                "order_guid": "o3",
                "order_created_at_utc": "2025-03-15T00:00:00Z",
                "state": "California",
                "quantity": 1,
                "product_name": "Snake Plant",
                "price": 20,
            },
            {
                "order_guid": "o4",
                "order_created_at_utc": "2025-04-02T00:00:00Z",
                "state": "Texas",
                "quantity": 5,
                "product_name": "Succulent",
                "price": 10,
            },
            {
                "order_guid": "o5",
                "order_created_at_utc": "2025-04-12T00:00:00Z",
                "state": "Arizona",
                "quantity": 3,
                "product_name": "Succulent",
                "price": 10,
            },
        ]
    )


def test_contextual_chunk_strategy_matches_project_questions():
    utils = load_module("greenery_rag_utils_test", DAGS / "greenery_rag_utils.py")
    chunks = utils.build_context_chunks(sample_fct_orders(), "2026-09-19")

    assert set(chunks["chunk_type"]) == {
        "state_summary",
        "product_summary",
        "monthly_trend",
    }

    california = chunks[
        (chunks["chunk_type"] == "state_summary")
        & (chunks["state"] == "California")
    ].iloc[0]
    assert "Monstera (6 units)" in california["text"]
    assert "3 unique orders" in california["text"]
    assert "USD 200.00" in california["text"]

    succulent = chunks[
        (chunks["chunk_type"] == "product_summary")
        & (chunks["product_name"] == "Succulent")
    ].iloc[0]
    assert "Texas (5 units)" in succulent["text"]
    assert "Arizona (3 units)" in succulent["text"]

    march = chunks[
        (chunks["chunk_type"] == "monthly_trend")
        & (chunks["source_period"] == "2025-03")
    ].iloc[0]
    assert "3 unique orders" in march["text"]
    assert march["processed_date"].isoformat() == "2026-09-19"


def test_context_builder_rejects_missing_transaction_columns():
    utils = load_module("greenery_rag_utils_missing", DAGS / "greenery_rag_utils.py")
    try:
        utils.build_context_chunks(pd.DataFrame({"state": ["CA"]}), "2026-09-19")
    except ValueError as exc:
        assert "missing required columns" in str(exc)
    else:
        raise AssertionError("Expected missing-column validation to fail")


def test_airflow_project_dag_structure_and_storage_design():
    path = DAGS / "greenery_llm_rag_pipeline.py"
    source = path.read_text(encoding="utf-8")
    assert ast.parse(source)

    required = [
        'dag_id="greenery_llm_rag_pipeline"',
        'task_id="gather_data"',
        'task_id="get_embeddings"',
        'task_id="load_data_to_bigquery"',
        "gather_data >> get_embeddings >> load_data_to_bigquery",
        '"fct_orders"',
        '"greenery_rag_context"',
        '"processed_date", "DATE"',
        "TimePartitioning",
        '"gemini-embedding-2"',
        "batch_size = 20",
    ]
    for token in required:
        assert token in source, f"Missing project requirement: {token}"


def test_greenery_qna_prompt_and_vector_search_design():
    path = QNA / "app.py"
    source = path.read_text(encoding="utf-8")
    assert ast.parse(source)

    required = [
        "You are a data analyst who works for the Greenery company.",
        "prepare useful reports for management",
        "Given the context below, find the actionable insights",
        "Explain clearly and simply, like I'm 10",
        "VECTOR_SEARCH",
        "greenery_rag_context",
        "gemini-2.5-flash-lite",
        "gemini-embedding-2",
    ]
    for token in required:
        assert token in source, f"Missing Q&A requirement: {token}"


def test_no_week6_project_secrets_are_committed():
    paths = [
        DAGS / "greenery_llm_rag_pipeline.py",
        QNA / "app.py",
        PROJECT / "start_week6_project.sh",
    ]
    forbidden = [
        'GEMINI_API_KEY = "',
        "YOUR_GEMINI_API_KEY",
        "AIza",
        "BEGIN PRIVATE KEY",
    ]
    for path in paths:
        source = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in source, f"Secret-like token found in {path}: {token}"
