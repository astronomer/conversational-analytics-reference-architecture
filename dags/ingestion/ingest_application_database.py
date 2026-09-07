"""Incremental extract from the internal application database: customers as a full
refresh, orders and their children from a high-water mark held in bronze.

The fixtures are static, so a second run legitimately loads zero new rows.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, get_current_context, task

from include import extract

APPLICATION_DATABASE = Asset("bronze_application_database")


@dag(
    schedule=[Asset("ingest_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Source ingestion"],
    doc_md=__doc__,
)
def ingest_application_database():

    @task
    def load_customers() -> dict:
        return extract.load_appdb_customers(get_current_context()["run_id"])

    @task
    def read_watermark() -> str | None:
        return extract.appdb_watermark()

    @task
    def load_orders_since(watermark: str | None) -> dict:
        return extract.load_appdb_since(watermark, get_current_context()["run_id"])

    @task(outlets=[APPLICATION_DATABASE])
    def report(customers: dict, incremental: dict) -> None:
        extract.report_appdb(customers, incremental)

    report(
        customers=load_customers(),
        incremental=load_orders_since(read_watermark()),
    )


ingest_application_database()
