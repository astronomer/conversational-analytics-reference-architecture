"""Extract from the support helpdesk: nested JSON, with the pagination cursor
persisted to an Airflow Variable after a successful load.

The ticket bodies loaded here are the only long-form text in this architecture and
the sole embedding target downstream.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, get_current_context, task

from include import extract

ZENDESK_SUPPORT = Asset("bronze_zendesk_support")


@dag(
    schedule=[Asset("ingest_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Source ingestion"],
    doc_md=__doc__,
)
def ingest_zendesk_support():

    @task
    def read_cursor() -> str | None:
        return extract.zendesk_cursor()

    @task
    def fetch_page(cursor: str | None) -> dict:
        return extract.fetch_zendesk_page(cursor)

    @task
    def load_tickets(payload: dict) -> dict:
        return extract.load_zendesk_tickets(payload, get_current_context()["run_id"])

    @task(outlets=[ZENDESK_SUPPORT])
    def commit_cursor(result: dict) -> None:
        extract.commit_zendesk_cursor(result)

    commit_cursor(load_tickets(fetch_page(read_cursor())))


ingest_zendesk_support()
