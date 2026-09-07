"""Builds silver and gold from bronze with dbt, rendered task-per-model by Cosmos.

ExecutionMode.WATCHER runs one `dbt build` in a producer task and completes each
model's consumer sensor as dbt reports that node finishing, which is why the graph
shows a producer alongside a sensor per model.
"""

from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode, InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/dbt/conversational_analytics")

profile_config = ProfileConfig(
    profile_name="conversational_analytics",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="warehouse_default",
        profile_args={"schema": "silver", "threads": 8},
        disable_event_tracking=True,
    ),
)

transform_warehouse = DbtDag(
    dag_id="transform_warehouse",
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        invocation_mode=InvocationMode.DBT_RUNNER,
    ),
    operator_args={"install_deps": True},
    max_active_runs=1,
    schedule=[
        Asset("bronze_reference_data"),
        Asset("bronze_application_database"),
        Asset("bronze_salesforce_crm"),
        Asset("bronze_zendesk_support"),
        Asset("bronze_stripe_payments"),
    ],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Source ingestion", "dbt"],
    doc_md=__doc__,
)
