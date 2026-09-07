"""Captures the gold schema and its column statistics into the context schema, which
is what the agent is told about the warehouse before it is asked anything.

Scoped to gold: bronze and silver are not the agent's business.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, task

from include import catalog


@dag(
    schedule=[Asset("context_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Context engineering"],
    doc_md=__doc__,
)
def capture_warehouse_catalog():

    @task
    def analyze_gold() -> list[str]:
        return catalog.analyze_gold_tables()

    @task
    def capture_tables(tables: list[str]) -> int:
        return catalog.capture_tables(tables)

    @task
    def capture_columns(tables: list[str]) -> int:
        return catalog.capture_columns()

    @task
    def load(table_count: int, column_count: int) -> None:
        catalog.report_catalog(table_count, column_count)

    tables = analyze_gold()
    load(table_count=capture_tables(tables), column_count=capture_columns(tables))


capture_warehouse_catalog()
