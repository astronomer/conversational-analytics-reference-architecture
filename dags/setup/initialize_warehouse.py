"""Creates the schemas, tables, pgvector extension and read-only role every other
Dag writes to. Run this first, once, against an empty Postgres.

Idempotent, and re-runnable: unlike a /docker-entrypoint-initdb.d script, which only
runs while the data directory is empty.
"""

from datetime import datetime

from airflow.sdk import chain, dag, task

from include import warehouse_schema


@dag(
    schedule=None,
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Setup"],
    doc_md=__doc__,
)
def initialize_warehouse():

    @task
    def create_extension() -> None:
        warehouse_schema.create_extension()

    @task
    def create_schemas() -> None:
        warehouse_schema.create_schemas()

    @task
    def create_reader_role() -> None:
        warehouse_schema.create_reader_role()

    @task
    def create_context_tables() -> None:
        warehouse_schema.create_context_tables()

    @task
    def create_agent_tables() -> None:
        warehouse_schema.create_agent_tables()

    chain(
        create_extension(),
        create_schemas(),
        create_reader_role(),
        create_context_tables(),
        create_agent_tables(),
    )


initialize_warehouse()
