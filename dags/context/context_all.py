"""Triggers every context-engineering Dag from one manual run, via an asset the
three Dags schedule on.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, task

CONTEXT_ALL_REQUESTED = Asset("context_all_requested")


@dag(
    schedule=None,
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Context engineering"],
    doc_md=__doc__,
)
def context_all():

    @task(outlets=[CONTEXT_ALL_REQUESTED])
    def request() -> None:
        print(
            "requested a full context-engineering run: capture_warehouse_catalog, "
            "embed_support_tickets, capture_pipeline_metadata"
        )

    request()


context_all()
