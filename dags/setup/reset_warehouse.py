"""Drops everything initialize_warehouse created, so the whole demo can be run again
from scratch. Tick `confirm` in the trigger form; without it nothing is dropped.
"""

from datetime import datetime

from airflow.sdk import Param, chain, dag, get_current_context, task

from include import extract, warehouse_schema


@dag(
    schedule=None,
    start_date=datetime(2026, 9, 1),
    tags=["Setup", "Destructive"],
    params={
        "confirm": Param(
            False,
            type="boolean",
            description="Drop every schema, the reader role, and the vector extension.",
        )
    },
    doc_md=__doc__,
)
def reset_warehouse():

    @task
    def check_confirmation() -> None:
        warehouse_schema.require_confirmation(get_current_context()["params"]["confirm"])

    @task
    def drop_schemas() -> None:
        warehouse_schema.drop_schemas()

    @task
    def drop_reader_role() -> None:
        warehouse_schema.drop_reader_role()

    @task
    def drop_extension() -> None:
        warehouse_schema.drop_extension()

    @task
    def clear_ingestion_state() -> None:
        extract.reset_zendesk_cursor()

    @task
    def verify_empty() -> None:
        warehouse_schema.verify_empty()

    chain(
        check_confirmation(),
        drop_schemas(),
        drop_reader_role(),
        drop_extension(),
        clear_ingestion_state(),
        verify_empty(),
    )


reset_warehouse()
