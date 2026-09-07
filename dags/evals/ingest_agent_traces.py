"""Persists the GenAI trace spans an agent run produced into the agent schema.

Asset-triggered because the collector batches its writes, so the spans for a task
that has just finished may not be on disk yet.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, task

from include import agent_store
from include.otel_traces import read_agent_runs

AGENT_TRACE_SPANS = Asset("agent_trace_spans")


@dag(
    schedule=[Asset("agent_responses")],
    start_date=datetime(2026, 9, 1),
    max_active_runs=1,
    tags=["Agentic analytics"],
    doc_md=__doc__,
)
def ingest_agent_traces():

    @task
    def read_spans() -> list[dict]:
        runs = read_agent_runs()
        agent_store.report_agent_runs(runs)
        return runs

    @task
    def match_to_responses(runs: list[dict]) -> list[dict]:
        return agent_store.match_to_responses(runs)

    @task(outlets=[AGENT_TRACE_SPANS])
    def persist_traces(runs: list[dict]) -> None:
        agent_store.persist_trace_spans(runs)

    persist_traces(match_to_responses(read_spans()))


ingest_agent_traces()
