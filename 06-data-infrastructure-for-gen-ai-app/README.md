# Building Data Infrastructure for Gen AI Application — Week 6

เวิร์คชอปนี้สร้าง RAG workflow ตั้งแต่เรียก LLM, เพิ่ม context, สร้าง embeddings,
เก็บ vector ใน BigQuery, similarity search, สร้าง course recommendation assistant,
ทำ Streamlit Q&A และปิดท้ายด้วย Airflow pipeline สำหรับ ingest → embed → store.

## Workshop checklist

| หัวข้อ | ไฟล์/ระบบ |
|---|---|
| Standalone Prompt | `llm_api_with_standalone_prompts.py` |
| Textual Context | `llm_api_with_context_enhanced_prompts_textual.py` |
| Transactional Context | `llm_api_with_context_enhanced_prompts_transactional.py` |
| Generate Embeddings | `embeddings.py` |
| Cosine Similarity | `cosine_similarity.py` |
| Store Embeddings in BigQuery | `store_embeddings_in_bigquery.py` → `deb_bootcamp.my_embeddings` |
| BigQuery VECTOR_SEARCH | `similarity_search.py` |
| Course Recommendation RAG | `ai_assistant_for_course_recommendations.py` → `deb_bootcamp.course_embeddings` |
| Streamlit Q&A | `qna_app/app.py` |
| Automated RAG Pipeline | `../00-bootcamp-project/dags/llm_course_data_pipeline.py` |

## Configuration

Secrets are intentionally **not** committed.

The code uses these defaults for this bootcamp environment:

- GCP project: `project-b9bafacf-46f9-43ef-bcc`
- BigQuery dataset: `deb_bootcamp`
- Sample embedding table: `my_embeddings`
- Course RAG table: `course_embeddings`
- Generation model: `gemini-3.1-flash-lite`
- Embedding model: `gemini-embedding-001`

The service-account JSON remains local under
`00-bootcamp-project/pyspark/` and is already ignored by Git.

Set a Google AI Studio **free-tier** API key locally:

```bash
export GEMINI_API_KEY="YOUR_FREE_TIER_KEY"
```

Do not paste the key into source code and do not commit `.env`.

## Run individual workshop steps

From this folder:

```bash
poetry install
poetry run python llm_api_with_standalone_prompts.py
poetry run python llm_api_with_context_enhanced_prompts_textual.py
poetry run python llm_api_with_context_enhanced_prompts_transactional.py
poetry run python embeddings.py
poetry run python cosine_similarity.py
poetry run python store_embeddings_in_bigquery.py
poetry run python similarity_search.py
poetry run python ai_assistant_for_course_recommendations.py
```

For Streamlit:

```bash
cd qna_app
poetry install
poetry run streamlit run app.py
```

## Automated RAG pipeline in Airflow

The workshop DAG has exactly three main tasks:

```text
gather_data
    ↓
get_embeddings
    ↓
load_data_to_bigquery
```

To rebuild Airflow, verify the Gemini key, trigger the DAG, wait for completion,
and verify rows in BigQuery with one command:

```bash
cd /workspaces/data-engineering-bootcamp
bash 00-bootcamp-project/start_week6.sh
```

A successful runtime ends with:

```text
WEEK 6 WORKSHOP RUNTIME PASSED ✅
```

The script does not print the Gemini API key.

## Notes

- The PDF was authored with older Gemini model IDs. The workshop code keeps the
  same learning flow while using current model IDs through environment-configurable defaults.
- The Vector Index section is optional for this tiny sample: BigQuery requires a
  much larger table before an IVF vector index is useful/eligible. `VECTOR_SEARCH`
  still demonstrates retrieval over the workshop table without creating that index.
- The Qdrant section in the workshop is a bonus introduction; the required workflow
  above uses BigQuery as the vector store, matching the main workshop.
