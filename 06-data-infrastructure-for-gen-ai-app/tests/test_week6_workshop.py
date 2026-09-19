import ast
import importlib.util
import sys
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_module(name, relative_path):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_cosine_similarity_math():
    cosine = load_module("cosine_similarity_test", "cosine_similarity.py")
    assert cosine.cosine_similarity([1, 0], [1, 0]) == 1.0
    assert abs(cosine.cosine_similarity([1, 0], [0, 1])) < 1e-12
    assert np.isclose(
        cosine.cosine_similarity([1, 1], [1, 1]),
        1.0,
    )


def test_similarity_search_uses_fully_qualified_bigquery_table():
    module = load_module("similarity_search_test", "similarity_search.py")
    query = module.build_vector_search_query([0.1, 0.2], top_k=2)
    assert (
        "project-b9bafacf-46f9-43ef-bcc.deb_bootcamp.my_embeddings"
        in query
    )
    assert "top_k => 2" in query
    assert "VECTOR_SEARCH" in query


def test_course_assistant_uses_course_embedding_table():
    module = load_module(
        "course_assistant_test",
        "ai_assistant_for_course_recommendations.py",
    )
    query = module.build_vector_search_query([0.1, 0.2], top_k=2)
    assert (
        "project-b9bafacf-46f9-43ef-bcc.deb_bootcamp.course_embeddings"
        in query
    )
    assert "top_k => 2" in query


def test_week6_sources_do_not_hardcode_secrets_or_retired_models():
    files = [
        "llm_api_with_standalone_prompts.py",
        "llm_api_with_context_enhanced_prompts_textual.py",
        "llm_api_with_context_enhanced_prompts_transactional.py",
        "embeddings.py",
        "cosine_similarity.py",
        "store_embeddings_in_bigquery.py",
        "similarity_search.py",
        "ai_assistant_for_course_recommendations.py",
        "week6_common.py",
        "qna_app/app.py",
    ]
    forbidden = [
        'GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"',
        "YOUR_GCP_PROJECT_ID",
        "YOUR_DATASET_ID",
        "YOUR_KEYFILE",
        "gemini-2.0-flash-001",
        "gemini-embedding-exp-03-07",
    ]
    for relative in files:
        content = (ROOT / relative).read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in content, f"{token!r} remains in {relative}"


def test_airflow_dag_has_exact_workshop_task_chain():
    dag_path = REPO_ROOT / "00-bootcamp-project" / "dags" / "llm_course_data_pipeline.py"
    content = dag_path.read_text(encoding="utf-8")
    tree = ast.parse(content)

    assert 'dag_id="llm_course_data_pipeline"' in content
    assert 'task_id="gather_data"' in content
    assert 'task_id="get_embeddings"' in content
    assert 'task_id="load_data_to_bigquery"' in content
    assert "gather_data >> get_embeddings >> load_data_to_bigquery" in content
    assert "course_embeddings" in content
    assert "gemini-embedding-001" in content
    assert tree is not None


def test_airflow_dag_uses_environment_for_gemini_key():
    dag_path = REPO_ROOT / "00-bootcamp-project" / "dags" / "llm_course_data_pipeline.py"
    content = dag_path.read_text(encoding="utf-8")
    assert 'os.getenv("GEMINI_API_KEY")' in content
    assert "YOUR_GEMINI_API_KEY" not in content
