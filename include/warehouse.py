"""Connection handling and the bronze load path. Every Dag connects to Postgres
through PostgresHook.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg import sql

log = logging.getLogger(__name__)

DEFAULT_CONN_ID = "warehouse_default"
READONLY_CONN_ID = "warehouse_readonly"

LOAD_METADATA_COLUMNS = ("_loaded_at", "_source_file", "_ingest_run_id")


def hook(conn_id: str = DEFAULT_CONN_ID) -> PostgresHook:
    return PostgresHook(postgres_conn_id=conn_id)


@contextmanager
def warehouse_connection(conn_id: str = DEFAULT_CONN_ID, *, vectors: bool = False):
    """A psycopg connection from PostgresHook, committed on clean exit.

    vectors=True registers the pgvector types, needed to bind a Python list to a
    vector column or read one back.
    """
    conn = hook(conn_id).get_conn()
    if vectors:
        from pgvector.psycopg import register_vector

        register_vector(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _split_table(table: str) -> tuple[str, str]:
    if "." not in table:
        raise ValueError(f"table must be schema-qualified, got {table!r}")
    schema, name = table.split(".", 1)
    return schema, name


def _columns_of(records: list[dict]) -> list[str]:
    keys: set[str] = set()
    for record in records:
        keys.update(record)
    reserved = keys & set(LOAD_METADATA_COLUMNS)
    if reserved:
        raise ValueError(
            f"source payload uses reserved load-metadata columns: {sorted(reserved)}"
        )
    return sorted(keys)


def _as_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def load_records(
    table: str,
    records: list[dict],
    source_file: str,
    ingest_run_id: str,
    conn_id: str = DEFAULT_CONN_ID,
) -> int:
    """Replace everything previously loaded from source_file, then insert records.

    Keying the replace on _source_file is what makes a re-run idempotent for a full
    refresh while still letting an incremental load append: a stable source_file
    replaces, a new one appends. Bronze columns are all text; typing is dbt's job in
    silver.
    """
    if not records:
        log.info("no records for %s from %s, nothing to load", table, source_file)
        return 0

    schema, name = _split_table(table)
    columns = _columns_of(records)
    qualified = sql.Identifier(schema, name)

    create = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        qualified,
        sql.SQL(", ").join(
            [sql.SQL("{} text").format(sql.Identifier(c)) for c in columns]
            + [
                sql.SQL("_loaded_at timestamptz"),
                sql.SQL("_source_file text"),
                sql.SQL("_ingest_run_id text"),
            ]
        ),
    )

    insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        qualified,
        sql.SQL(", ").join(
            [sql.Identifier(c) for c in columns]
            + [sql.Identifier(c) for c in LOAD_METADATA_COLUMNS]
        ),
        sql.SQL(", ").join(
            [sql.Placeholder()] * len(columns)
            + [sql.SQL("now()"), sql.Placeholder(), sql.Placeholder()]
        ),
    )

    rows = [
        [_as_text(record.get(c)) for c in columns] + [source_file, ingest_run_id]
        for record in records
    ]

    with warehouse_connection(conn_id) as conn, conn.cursor() as cur:
        # Mapped pages of the same object share a table and can create it
        # concurrently; IF NOT EXISTS alone still races on the pg_type catalog
        # entry, so serialize creation per table with a transaction-scoped lock.
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (table,))
        cur.execute(create)
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE _source_file = %s").format(qualified),
            (source_file,),
        )
        cur.executemany(insert, rows)

    log.info("loaded %s rows into %s from %s", len(rows), table, source_file)
    return len(rows)


def fetch_one(query: str, params: tuple = (), conn_id: str = DEFAULT_CONN_ID):
    """Read a single value, returning None when the relation does not exist yet."""
    import psycopg

    try:
        records = hook(conn_id).get_records(query, parameters=params or None)
    except psycopg.errors.UndefinedTable:
        log.info("relation absent, treating as first run: %s", query)
        return None
    return records[0][0] if records else None
