"""Captures Airflow's own state, so the agent can flag a figure as stale when the
pipeline that produced it failed.

Everything comes from the Airflow REST API. No component reads the metadata database.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, get_current_context, task

from include import pipeline_state


@dag(
    schedule=[Asset("context_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Context engineering"],
    doc_md=__doc__,
)
def capture_pipeline_metadata():

    @task
    def fetch_dags() -> list[dict]:
        return pipeline_state.fetch_dags()

    @task
    def capture_source(dags: list[dict]) -> int:
        return pipeline_state.capture_dag_source(dags)

    @task
    def capture_runs(dags: list[dict]) -> int:
        return pipeline_state.capture_dag_runs(dags)

    @task
    def derive_health(dags: list[dict], run_count: int) -> None:
        pipeline_state.derive_health(
            dags, get_current_context()["run_id"], run_count
        )

    dags = fetch_dags()
    capture_source(dags)
    derive_health(dags=dags, run_count=capture_runs(dags))


capture_pipeline_metadata()
