"""Full refresh of the reference CSVs into bronze, one mapped task per file."""

from datetime import datetime

from airflow.sdk import Asset, dag, get_current_context, task

from include import extract

REFERENCE_DATA = Asset("bronze_reference_data")


@dag(
    schedule=[Asset("ingest_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Source ingestion"],
    doc_md=__doc__,
)
def load_reference_data():

    @task
    def list_reference_files() -> list[str]:
        return extract.reference_files()

    @task
    def load_file(path: str) -> dict:
        return extract.load_reference_file(path, get_current_context()["run_id"])

    @task(outlets=[REFERENCE_DATA])
    def report(results: list[dict]) -> int:
        return extract.report_reference(results)

    report(load_file.expand(path=list_reference_files()))


load_reference_data()
