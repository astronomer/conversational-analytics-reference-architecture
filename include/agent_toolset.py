"""The three tools the analytics agent is given. Their docstrings are prompt text:
the model reads them to decide which tool to call.

The SQL toolset connects as analytics_reader, which has SELECT on gold and nothing
else. That database role, not allowed_tables, is the actual boundary. allowed_functions
is left unset: measured against sqlglot 30.17.0, every aggregate, window function,
FILTER clause, join and CTE an analytics query needs already passes with an empty
allowlist.
"""

from __future__ import annotations

from airflow.providers.common.ai.toolsets import SQLToolset
from pydantic_ai.toolsets import FunctionToolset

from include import embeddings, pipeline_state

GOLD_TABLES = [
    "dim_customer",
    "dim_date",
    "dim_location",
    "dim_product",
    "fct_order_items",
    "fct_orders",
    "fct_payments",
    "fct_shipments",
    "fct_support_tickets",
    "mart_customer_360",
    "mart_product_performance",
    "mart_route_performance",
]

gold_sql = SQLToolset(
    db_conn_id="warehouse_readonly",
    allowed_tables=GOLD_TABLES,
    schema="gold",
    allow_writes=False,
    max_rows=50,
    max_result_bytes=32_768,
)


def search_tickets(query: str) -> str:
    """Search the text customers wrote in their support tickets.

    Use this for questions about sentiment, complaints, reasons, or wording - things
    a count cannot answer. Use the SQL tools instead for anything numeric: how many
    tickets, which product, what revenue.

    Every passage comes back with a chunk_id in square brackets. List the chunk_ids
    you relied on in the chunks_used field of your answer.

    :param query: What to look for, for example "deliveries arriving late".
    """
    results = embeddings.search_tickets(query, limit=5)
    if not results:
        return (
            "No ticket passages found. context.ticket_chunks may be empty; "
            "the embed_support_tickets Dag populates it."
        )
    return "\n\n".join(
        f"[{r['chunk_id']}] ticket {r['ticket_id']}"
        f"{' product ' + r['product_sku'] if r['product_sku'] else ''}\n"
        f"{r['heading']}\n{r['body']}"
        for r in results
    )


def get_pipeline_health(dag_ids: str) -> str:
    """Check whether the pipelines behind a number succeeded.

    Call this before reporting any figure that looks surprising - unusually low
    revenue, a sudden drop, a suspiciously round number. A broken pipeline produces
    a plausible wrong answer with no error anywhere, and this is the only way to
    tell that apart from a real change in the business.

    Which Dag builds what:
      - load_reference_data, ingest_application_database, ingest_salesforce_crm,
        ingest_zendesk_support, ingest_stripe_payments all load raw source data.
      - transform_warehouse builds every gold table from that raw data.

    Report what you find in the freshness_note field of your answer.

    :param dag_ids: Comma-separated Dag ids, for example
        "ingest_stripe_payments,transform_warehouse".
    """
    wanted = [d.strip() for d in dag_ids.split(",") if d.strip()]
    if not wanted:
        return "No dag_ids given."
    return pipeline_state.health_summary(wanted)


analytics_tools = FunctionToolset([search_tickets, get_pipeline_health])
