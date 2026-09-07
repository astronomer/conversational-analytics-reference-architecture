"""Extract from the CRM. Each object arrives as a set of 100-record page files, and
each page loads independently so one page can be replaced by itself.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, get_current_context, task

from include import extract

SALESFORCE_CRM = Asset("bronze_salesforce_crm")


@dag(
    schedule=[Asset("ingest_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Source ingestion"],
    doc_md=__doc__,
)
def ingest_salesforce_crm():

    @task
    def discover_pages() -> list[dict]:
        return extract.salesforce_pages()

    @task
    def load_page(page: dict) -> dict:
        return extract.load_salesforce_page(page, get_current_context()["run_id"])

    @task(outlets=[SALESFORCE_CRM])
    def report(results: list[dict]) -> None:
        extract.report_salesforce(results)

    report(load_page.expand(page=discover_pages()))


ingest_salesforce_crm()
