"""Extract from the payment processor: one file of event envelopes per stream,
unwrapped into bronze. Amounts stay text; the numeric cast belongs in silver.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, get_current_context, task

from include import extract

STRIPE_PAYMENTS = Asset("bronze_stripe_payments")


@dag(
    schedule=[Asset("ingest_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Source ingestion"],
    doc_md=__doc__,
)
def ingest_stripe_payments():

    @task
    def list_streams() -> list[str]:
        return extract.STRIPE_STREAMS

    @task
    def load_stream(stream: str) -> dict:
        return extract.load_stripe_stream(stream, get_current_context()["run_id"])

    @task(outlets=[STRIPE_PAYMENTS])
    def report(results: list[dict]) -> None:
        extract.report_stripe(results)

    report(load_stream.expand(stream=list_streams()))


ingest_stripe_payments()
