"""Embeds support ticket text into pgvector, giving the agent semantic search over
what customers wrote in their own words.

Incremental on a content checksum: a second run embeds nothing and costs nothing.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, task

from include import embeddings


@dag(
    schedule=[Asset("context_all_requested")],
    start_date=datetime(2026, 9, 1),
    default_args={"retries": 2},
    tags=["Context engineering"],
    doc_md=__doc__,
)
def embed_support_tickets():

    @task
    def read_tickets() -> list[dict]:
        return embeddings.read_tickets()

    @task
    def build_chunks(tickets: list[dict]) -> list[dict]:
        return embeddings.chunk_tickets(tickets)

    @task
    def select_changed(chunks: list[dict]) -> list[dict]:
        return embeddings.select_changed(chunks)

    @task
    def embed_and_load(chunks: list[dict]) -> int:
        return embeddings.embed_and_load(chunks)

    embed_and_load(select_changed(build_chunks(read_tickets())))


embed_support_tickets()
