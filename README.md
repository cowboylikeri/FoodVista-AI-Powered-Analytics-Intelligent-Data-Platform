# FoodVista – AI-Powered Analytics & Intelligent Data Platform

A batch data pipeline that takes food-delivery data from raw CSVs to analytics and AI-assisted exploration:

**Zomato/Food Delivery Dataset → Amazon S3 → Snowflake → dbt → Airflow → Google Gemini**

The dataset lands in an S3 data lake and flows into Snowflake through a storage integration. dbt transforms it through medallion layers: RAW (Bronze) tables loaded via `COPY INTO`, cleaned STAGING (Silver) views, and business-ready MARTS (Gold) with dimensions, incremental facts, and aggregate marts. Apache Airflow orchestrates the pipeline as a daily DAG.

On top of the warehouse is an AI layer powered by **Google Gemini**. Gemini-based review enrichment converts selected free-text reviews into structured, queryable fields; retrieval-augmented generation (RAG) lets users ask questions about reviews; and text-to-SQL supports natural-language questions against the warehouse. Streamlit provides the interfaces for the AI applications.

![Architecture](docs/architecture.png)

> 📂 **Dataset + project slides:** [Google Drive folder](https://drive.google.com/drive/folders/1FEnGWMHhHzzTUCZOw1-YnH2v3DMuM-rs?usp=sharing) — download the CSVs here and place them under `data/` (they're too large to commit to the repo).

## What gets built

| Layer | Where | What |
|---|---|---|
| **Source** | `data/` (local) | 4 dimension CSVs (restaurants, users, food, menu) plus 3 fact files: **10M orders**, **~23M order items**, and **300K+ source reviews** |
| **Lake** | Amazon S3 | One bucket, with a `raw/<table>/` folder for each CSV |
| **Bronze** | Snowflake `ZOMATO.RAW` | `COPY INTO` from S3 through a keyless storage integration |
| **Silver** | Snowflake `ZOMATO.STAGING` | dbt staging views to clean, type, and rename source fields |
| **Gold** | Snowflake `ZOMATO.MARTS` | Dimensions, incremental facts (MERGE), business marts, and an SCD2 snapshot |
| **AI** | Snowflake `ZOMATO.AI` | Gemini-enriched review records, RAG chat, and text-to-SQL |
| **Orchestration** | Airflow (Docker) | A daily DAG for loading, transformation, review enrichment, and AI marts |

**Review-processing note:** the source contains 300K+ reviews, but the number actually enriched by Gemini depends on the configured batch/sample limit and API quota. The source row count should not be interpreted as a claim that every review has already been enriched.

## Tech stack

Python · Pandas · Amazon S3 · Snowflake · dbt (`dbt-snowflake`) · Apache Airflow 3 (Docker) · Google Gemini API (review enrichment and text generation) · Gemini embeddings (semantic retrieval) · Streamlit

## Repository structure

```text
├── airflow/                       # Airflow 3 on Docker
│   ├── Dockerfile                 # Snowflake dependencies and Gemini SDK; dbt in its own venv
│   ├── docker-compose.yaml        # Postgres, API server, scheduler; credentials injected via env vars
│   ├── example.env                # Template for SNOWFLAKE_* / GEMINI_API_KEY / SAMPLE_N
│   └── dags/zomato_batch.py       # Pipeline DAG (4 tasks)
├── zomato/                        # dbt project
│   ├── models/staging/            # 7 staging views (Silver), sources, and tests
│   ├── models/marts/              # Dimensions, incremental facts, and business marts (Gold)
│   └── macros/                    # Custom schema-name macro
├── ai/                            # AI layer
│   ├── enrich_reviews.py          # Gemini enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   ├── rag_chat.py                # RAG — chat with reviews (Streamlit)
│   ├── text_to_sql.py             # Text-to-SQL — chat with the warehouse (Streamlit)
│   └── example.env                # Template for AI credentials
├── snowflake/                     # Snowflake setup SQL (run in Snowsight, in order)
│   ├── 01_setup.sql               # Warehouse ZOMATO_WH, database ZOMATO, schemas, role
│   ├── 02_storage_integration.sql # Keyless S3 link (pairs with aws/iam/)
│   ├── 03_stage_and_formats.sql   # External stage and CSV file format
│   ├── 04_raw_tables.sql          # RAW (Bronze) table DDL; column order matches CSVs
│   └── 05_copy_into.sql           # COPY INTO RAW from the stage
├── aws/iam/                       # IAM policy and role-trust policies for S3 ↔ Snowflake
└── docs/architecture.png          # Architecture diagram
```

> `data/` (~2.3 GB of CSVs), logs, and dbt `target/` artifacts are intentionally not committed. Get the dataset and slides from the [Google Drive folder](https://drive.google.com/drive/folders/1FEnGWMHhHzzTUCZOw1-YnH2v3DMuM-rs?usp=sharing).

## How the pipeline works

### 1 · Data lands in S3

The seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/` — one folder per table: `restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, and `reviews/`.

### 2 · S3 → Snowflake: one keyless handshake

Snowflake reads the bucket with **no stored AWS access keys**, using a storage integration and an IAM role. The Snowflake-side SQL is [`snowflake/02_storage_integration.sql`](snowflake/02_storage_integration.sql); the AWS JSON documents live in [`aws/iam/`](aws/iam/):

| File | Used for |
|---|---|
| [`s3-read-policy.json`](aws/iam/s3-read-policy.json) | IAM policy `zomato-s3-read` — read-only access to the bucket |
| [`snowflake-role-trust-policy-initial.json`](aws/iam/snowflake-role-trust-policy-initial.json) | IAM role `snowflake-s3-role` — placeholder trust used at creation time |
| [`snowflake-role-trust-policy-final.json`](aws/iam/snowflake-role-trust-policy-final.json) | Final trust — Snowflake's IAM user ARN and external ID from `DESC INTEGRATION` |

The order matters: create the AWS policy and role → create the Snowflake `STORAGE INTEGRATION` pointing at the role ARN → run `DESC INTEGRATION` to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` → paste both into the role's trust policy. The trust `Principal` must be Snowflake's IAM user ARN, not `:root`. Avoid re-running `CREATE OR REPLACE` on the integration afterward, as it regenerates the external ID and breaks the trust configuration.

### 3 · Load — `COPY INTO`

The table DDL ([`snowflake/04_raw_tables.sql`](snowflake/04_raw_tables.sql)) matches each CSV's column order. [`snowflake/05_copy_into.sql`](snowflake/05_copy_into.sql) loads the files from the stage into `ZOMATO.RAW` tables, including the 10M orders, ~23M order items, and 300K+ source reviews.

### 4 · Transform — dbt medallion layers

- **Staging (Silver)** — a view per source, including type cleanup and standardisation (for example, `--` to null, `₹ 200` to `200`, lowercased emails, and a derived `is_delivered` field).
- **Dimensions (Gold)** — `dim_restaurants`, `dim_customer` (with age segments), `dim_food`, and a generated `dim_date` calendar.
- **Facts (Gold, incremental)** — `fct_orders` and `fact_order_items` use `materialized='incremental'` with a MERGE strategy, so subsequent runs can process new rows rather than rebuilding all existing rows.
- **Marts (Gold)** — tables for business questions such as daily city revenue (GMV/AOV/cancel rate), restaurant performance, delivery SLA (p50/p90 by city and hour), and review insights.
- **Tests** — `unique`, `not_null`, `relationships`, and `accepted_values` checks plus a singular reconciliation test. `dbt build` runs models and tests in dependency order.

### 5 · Orchestrate — Airflow

The daily DAG, [`zomato_batch`](airflow/dags/zomato_batch.py), connects the pipeline tasks:

```text
reload_raw  →  dbt_build_core  →  enrich_reviews  →  dbt_build_ai
(COPY from S3)  (dbt build + tests)  (Gemini enrichment)  (AI mart)
```

Credentials are injected through environment variables rather than hard-coded in source files. Docker Compose supplies the `SNOWFLAKE_*` variables (read by dbt's `profiles.yml` through `env_var()`) and an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection for the COPY task. The Gemini key is supplied as `GEMINI_API_KEY` to the AI task.

### 6 · AI layer — three capabilities

1. **Review enrichment** (`ai/enrich_reviews.py`) — treats the language model as a transformation step. The script sends review text to a configured Google Gemini model and requests structured output such as sentiment and topic, then writes enrichment results to `ZOMATO.AI.REVIEW_ENRICHED`, which dbt can model into `mart_review_insights`. Processing is sample/batch-capped with `SAMPLE_N` to help control quota use. The 300K+ source-review count does not mean all reviews have been enriched; actual coverage depends on completed runs and API limits.
2. **RAG** (`ai/rag_chat.py`) — lets users chat with reviews. Review text is represented with Gemini embeddings, semantically relevant records are retrieved for a question, and Gemini generates an answer grounded in the retrieved review context. Source reviews should be shown with answers where supported by the implementation.
3. **Text-to-SQL** (`ai/text_to_sql.py`) — lets users ask questions about the warehouse in plain English. Gemini receives the relevant mart schema and generates candidate Snowflake SQL. A SELECT-only validation step checks the query before execution under the restricted `DBT_ROLE`.

## Running it

### Prerequisites

- Access to an AWS account and an S3 bucket.
- A Snowflake account with permissions to create the required warehouse, database, schemas, role, stage, and storage integration.
- Python and the project dependencies; Docker for the Airflow stack.
- A Google Gemini API key. Set it in your local environment or the appropriate `.env` file; **do not commit real API keys or credentials**.

### Snowflake setup

Run `snowflake/01_setup.sql` through `snowflake/05_copy_into.sql` in Snowsight in order. This creates the warehouse `ZOMATO_WH`, database `ZOMATO`, schemas `RAW`, `STAGING`, `MARTS`, `SNAPSHOTS`, and `AI`, the `DBT_ROLE`, and the staged CSV load. Configure the AWS policy and role using the files under `aws/iam/` as described above.

### dbt

```bash
cd zomato
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai
```

### Airflow

```bash
cd airflow
cp example.env .env
# Edit .env and set the required SNOWFLAKE_* values, GEMINI_API_KEY, and SAMPLE_N.
docker compose build
docker compose up -d
```

Open `http://localhost:8080`, un-pause `zomato_batch`, and trigger the DAG. Ensure that the environment file contains a valid `GEMINI_API_KEY`; do not put the key directly into the DAG or commit it to Git.

### Run the AI components directly

From the repository root, set the key in your shell (or configure it using the project's supported local environment-file workflow):

```bash
export GEMINI_API_KEY="your-gemini-api-key"
python ai/enrich_reviews.py
streamlit run ai/rag_chat.py       # chat with reviews
streamlit run ai/text_to_sql.py    # chat with the warehouse
```

Use a small `SAMPLE_N` during initial tests, monitor Gemini API quota responses, and resume processing in controlled batches. Confirm the configured key variable and model settings match those expected by the scripts in your checkout.

## Notes on API configuration

- This project uses **Google Gemini** for LLM-based review enrichment and text generation, plus Gemini embeddings for semantic retrieval.
- The setup examples use `GEMINI_API_KEY`; make sure the Python scripts and environment files use the same variable name.
- API quotas can limit how many source reviews are enriched in a run. Report source review volume separately from the number successfully enriched.
- Never commit `.env` files, credentials, API keys, logs containing secrets, or generated data artifacts.
