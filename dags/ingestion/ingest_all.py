"""Triggers every ingestion Dag from one manual run, via an asset the five Dags
schedule on.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, task

INGEST_ALL_REQUESTED = Asset("ingest_all_requested")


@dag(
    schedule=None,
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Source ingestion"],
    doc_md=__doc__,
)
def ingest_all():

    @task(outlets=[INGEST_ALL_REQUESTED])
    def request() -> None:
        print(
            "requested a full ingestion run: load_reference_data, "
            "ingest_application_database, ingest_salesforce_crm, "
            "ingest_zendesk_support, ingest_stripe_payments"
        )

    request()


ingest_all()
