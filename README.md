# Conversational Analytics Reference Pattern

This project shows how to use [Apache Airflow®](https://airflow.apache.org/) for 

- [Data ingestion and ETL](/dags/ingestion/) including using Cosmos to orchestrate dbt Core
- [Context engineering](/dags/context/) giving the AI agent access to the warehouse schema, semantic search over support tickets, and Dag run history indicating last runs of relevant pipelines as well as failures, aiding the agent in assessing which data might be stale
- [AI agent orchestration](dags/agent/) including AI as a judge
- [AI evals](/dags/evals/) including trace ingestion and processing

You can run this project locally using the Postgres database defined in the [`docker-compose.override.yml`](/docker-compose.override.yml) file.

The only external service you need is OpenAI with an OpenAI API key.

![The AI evals plugin dashboard: summary stats, rate per dimension, and every question answered](/source/analytics_evals_dashboard.png)

## Requirements

- Docker
- The [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- An OpenAI API key

## Setup

```bash
cp .env_example .env      # add your OPENAI_API_KEY
astro dev start
```

`astro dev start` starts seven containers:

- `scheduler`
- `triggerer`
- `dag-processor`
- `api-server`
- `postgres`: Airflow's own metadata database
- `warehouse`: a second Postgres, `pgvector/pgvector:pg17`, for the project's data
- `otel-collector`: to collect GenAI spans including agent traces the evals Dag ingests into the pg database.

Connections to the `warehouse` and `otel-collector` containers are defined in `.env_example` (`AIRFLOW_CONN_*`).

## How to run the example

Run the Dags in the following order:

| # | Dag | Dags that run automatically |
|---|---|
| 0 | Unpause all Dags in the UI or using `astro dev run dags unpause --treat-dag-id-as-regex -y ".*"` |
| 1 | `initialize_warehouse` | - |
| 2 | `load_reference_data`, `ingest_application_database`, `ingest_salesforce_crm`, `ingest_zendesk_support`, `ingest_stripe_payments` | `transform_warehouse` |
| 3 | `capture_warehouse_catalog`, `embed_support_tickets`, `capture_pipeline_metadata` |
| 4 | `answer_analytics_question` | `ingest_agent_traces` |

## The Dags

### `dags/setup/`

| Dag | What it does |
|---|---|
| `initialize_warehouse` | Creates the `vector` extension, the five schemas, the read-only `analytics_reader` role, and the context and agent tables. Idempotent. |
| `reset_warehouse` | Drops everything in the warehouse to start fresh. Tick `confirm` in the trigger form. |

### `dags/ingestion/`

Five extract Dags, one per source system, plus `ingest_all` to trigger all five at once.

![Ingestion Dags and assets](/source/ingest_all_requested_asset_graph.png)

| Dag | What it does |
|---|---|
| `ingest_all` | Triggers all five ingestion Dags via the `ingest_all_requested` asset |
| `load_reference_data` | Ingests data from local CSVs |
| `ingest_application_database` | Ingest data from local CSVs simulating an internal application database, loading orders incrementally via a watermark |
| `ingest_salesforce_crm` | Ingest data similar to salesforce data from a mocked API |
| `ingest_zendesk_support` | Ingest data similar to a zendesk export from JSON |
| `ingest_stripe_payments` | Ingest data similar to a stripe export from JSON |
| `transform_warehouse` | A Cosmos `DbtDag` that builds the silver and gold layers in the database |

### `dags/context/`

A set of context engineering Dags preparing data for the AI agent, plus `context_all`
to trigger all three at once.

![Context engineering Dags and assets](/source/context_all_requested_asset_graph.png)

| Dag | What it does |
|---|---|
| `context_all` | Triggers all three context-engineering Dags via the `context_all_requested` asset |
| `capture_warehouse_catalog` | Reads the gold schema, its column types, its dbt descriptions and its `pg_stats` statistics and saves it in a table accessible to the AI agent |
| `embed_support_tickets` | Chunks ticket text and embeds it with pgvector and `text-embedding-3-small`, embeddings are loaded into the postgres database |
| `capture_pipeline_metadata` | Reads Airflow's REST API for Dag source, run history and asset events and saves them in a table |

### `dags/agent/`

![build_prompt, answer_question, judge_answer, persist_response](/source/answer_analytics_question_task_graph.png)

| Dag | What it does |
|---|---|
| `answer_analytics_question` | Takes a question as a Dag param, answers it with `@task.agent`, then scores the answer with an LLM judge in `@task.llm`. Writes results to postgres |

The agent has three tools: read-only SQL over the twelve gold models, semantic search
over ticket text, and pipeline health returning relevant Dag run history from the database.

![The agent calling query, get_pipeline_health, and search_tickets in one run](/source/agent_tool_call_sequence.png)

### `dags/evals`

![The AI evals plugin, showing an answer scored across four dimensions](/source/analytics_evals_plugin.png)


| Dag | What it does |
|---|---|
| `ingest_agent_traces` | Reads the GenAI spans the run emitted and writes them to `agent.trace_spans`. asset-triggered to run after every agent Dag run |

Note that in order for Airflow to emit traces you need to set the following env vars in `.env`:

```
AIRFLOW__COMMON_AI__OTEL_EXPORT_ENABLED=True
AIRFLOW__COMMON_AI__CAPTURE_CONTENT=True
```

## Schema

| Schema | Contents |
|---|---|
| `bronze` | 29 tables, one per source payload. Every column is `text`; typing happens in silver |
| `silver` | 21 views: cast, conformed, consistently named |
| `gold` | 12 tables: 4 dimensions, 5 facts, 3 marts. What the agent queries |
| `context` | Ticket embeddings, the gold catalog and its rendered prompt, Airflow's own Dag source and run history, and derived per-Dag pipeline health |
| `agent` | Questions, answers, judge scores, trace spans |

Two connections point at the warehouse. `warehouse_default` is read-write.
`warehouse_readonly` connects as `analytics_reader`, a role with `SELECT` on `gold`.

## AI evals plugin

Airflow exports its Dag and task spans over OTLP to the `otel-collector`
container, which writes them to `traces/spans.jsonl`. The agent's GenAI spans nest
under those task spans, which is how a model call is attributed back to the task instance that
made it. `ingest_agent_traces` reads the file into `agent.trace_spans` in Postgres. The evals plugin 
imports the trace data from Postgres.
