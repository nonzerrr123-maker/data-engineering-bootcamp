# Week 6 Bootcamp Project — Greenery RAG Assistant

โปรเจกต์นี้พัฒนา AI Assistant สำหรับบริษัท Greenery โดยใช้แนวคิด
Retrieval-Augmented Generation (RAG) กับข้อมูลธุรกรรมที่ถูกแปลงเป็น
contextual summaries ก่อนสร้าง embeddings และเก็บใน BigQuery เพื่อค้นคืนด้วย
Vector Search

## Architecture

\`\`\`text
Greenery fct_orders
      |
      v
Airflow: gather_data
      |
      v
Contextual chunks
(state summaries / product summaries / monthly trends)
      |
      v
Airflow: get_embeddings
      |
      v
Gemini Embeddings
      |
      v
Airflow: load_data_to_bigquery
      |
      v
BigQuery deb_bootcamp.greenery_rag_context
      |
      v
BigQuery VECTOR_SEARCH
      |
      v
Streamlit Greenery AI Assistant
      |
      v
Gemini generation model
\`\`\`

## Contextual data design

The raw transaction rows are intentionally not embedded one row at a time.
The pipeline produces three kinds of natural-language chunks:

- \`state_summary\` — answers questions such as "What products are popular in California?"
- \`product_summary\` — answers questions such as "Which states buy the most succulents?"
- \`monthly_trend\` — answers questions such as "How many orders were placed last month?"

Each BigQuery row contains:

- \`chunk_type\`
- \`state\`
- \`product_name\`
- \`source_period\`
- \`text\`
- \`processed_date\`
- \`embedding\`

The destination table is partitioned by \`processed_date\`.

## Source table

The pipeline looks for \`fct_orders\` in this order:

1. \`GREENERY_SOURCE_DATASET_ID\` if configured
2. \`dbt_suntisuk_reporting\`
3. \`deb_bootcamp\`
4. \`dbt_suntisuk\`

This makes the project work with the dbt output from Week 5 while remaining
portable to a different BigQuery dataset.

## Models

Defaults:

- Generation: \`gemini-2.5-flash-lite\`
- Embeddings: \`gemini-embedding-2\`

Both can be overridden through environment variables.

## Secrets

No API key or service-account JSON is committed.

The runtime expects:

- \`GEMINI_API_KEY\` from Google AI Studio
- the existing local GCP service-account JSON under \`00-bootcamp-project/pyspark/\`

## Airflow RAG pipeline

DAG: \`greenery_llm_rag_pipeline\`

Task graph:

\`\`\`text
gather_data
    ↓
get_embeddings
    ↓
load_data_to_bigquery
\`\`\`

The target BigQuery table is:

\`\`\`text
project-b9bafacf-46f9-43ef-bcc.deb_bootcamp.greenery_rag_context
\`\`\`

## Run the full project runtime

From the repository root:

\`\`\`bash
export GEMINI_API_KEY="YOUR_FREE_TIER_KEY"
bash 00-bootcamp-project/start_week6_project.sh
\`\`\`

The script checks the credentials, starts Airflow, validates the DAG, triggers it,
waits for completion, and verifies the RAG table in BigQuery.

A successful run ends with:

\`\`\`text
WEEK 6 BOOTCAMP PROJECT RUNTIME PASSED ✅
\`\`\`

## Run the Streamlit Q&A app

After the Airflow pipeline has created the BigQuery RAG table:

\`\`\`bash
cd 00-bootcamp-project/qna_app
bash run.sh
\`\`\`

Example questions:

- What products are popular in California?
- Which states buy the most succulents?
- What was Greenery's strongest month?
- What products should management pay attention to?

The prompt instructs the assistant to use the retrieved Greenery context,
surface actionable insights, explain clearly, and avoid inventing unsupported data.
