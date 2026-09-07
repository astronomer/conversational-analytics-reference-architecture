"""Airflow's own state, read over the REST API and written into the context schema:
Dag source, run history, and the derived per-Dag health the agent reads.
"""

from __future__ import annotations

import logging

from include.airflow_api import (
    get_asset_events,
    get_dag_source,
    get_import_errors,
    list_dag_runs,
    list_dag_versions,
    list_dags,
)
from include.warehouse import warehouse_connection

log = logging.getLogger(__name__)

RUN_HISTORY_LIMIT = 50

ASSET_SAMPLE_SIZE = 5

_UPSERT_SOURCE = """
INSERT INTO context.dag_source
    (dag_id, version_number, source_code, source_chars, captured_at)
VALUES (%s, %s, %s, %s, now())
ON CONFLICT (dag_id, version_number) DO UPDATE SET
    source_code  = excluded.source_code,
    source_chars = excluded.source_chars,
    captured_at  = now()
"""

_UPSERT_RUNS = """
INSERT INTO context.dag_run_history
    (dag_id, run_id, run_type, state, run_after, logical_date,
     run_started_at, run_ended_at, duration_secs, captured_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (dag_id, run_id) DO UPDATE SET
    run_type       = excluded.run_type,
    state          = excluded.state,
    run_after      = excluded.run_after,
    run_started_at = excluded.run_started_at,
    run_ended_at   = excluded.run_ended_at,
    duration_secs  = excluded.duration_secs,
    captured_at    = now()
"""

_UPSERT_HEALTH = """
INSERT INTO context.pipeline_health
    (dag_id, is_paused, has_import_error, total_runs, last_state,
     last_run_at, last_success_at, consecutive_failures,
     last_asset_event_at, produces_assets, captured_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (dag_id) DO UPDATE SET
    is_paused            = excluded.is_paused,
    has_import_error     = excluded.has_import_error,
    total_runs           = excluded.total_runs,
    last_state           = excluded.last_state,
    last_run_at          = excluded.last_run_at,
    last_success_at      = excluded.last_success_at,
    consecutive_failures = excluded.consecutive_failures,
    last_asset_event_at  = excluded.last_asset_event_at,
    produces_assets      = excluded.produces_assets,
    captured_at          = now()
"""

_DERIVE_HEALTH = """
WITH ranked AS (
    SELECT dag_id, state, run_after, run_started_at,
           row_number() OVER (
               PARTITION BY dag_id ORDER BY run_after DESC NULLS LAST
           ) AS rn
      FROM context.dag_run_history
     WHERE run_id <> %s
),
first_success AS (
    SELECT dag_id, min(rn) AS success_rn
      FROM ranked WHERE state = 'success' GROUP BY dag_id
)
SELECT r.dag_id,
       count(*)                                                 AS total_runs,
       max(r.state)     FILTER (WHERE r.rn = 1)                 AS last_state,
       max(r.run_after) FILTER (WHERE r.rn = 1)                 AS last_run_at,
       max(r.run_after) FILTER (WHERE r.state = 'success')       AS last_success_at,
       count(*) FILTER (
           WHERE r.state = 'failed'
             AND r.rn < coalesce(f.success_rn, 2147483647)
       )                                                        AS consecutive_failures
  FROM ranked r
  LEFT JOIN first_success f ON f.dag_id = r.dag_id
 GROUP BY r.dag_id
"""

_READ_HEALTH = """
SELECT dag_id, last_state, last_run_at, last_success_at,
       consecutive_failures, is_paused, has_import_error
  FROM context.pipeline_health
 WHERE dag_id = ANY(%s)
 ORDER BY dag_id
"""


def _plain(value):
    return getattr(value, "value", value)


def _summarise_assets(names: set) -> str | None:
    clean = sorted(str(n).rsplit("/", 1)[-1] for n in names if n)
    if not clean:
        return None
    if len(clean) <= ASSET_SAMPLE_SIZE:
        return ", ".join(clean)
    shown = ", ".join(clean[:ASSET_SAMPLE_SIZE])
    return f"{shown}, and {len(clean) - ASSET_SAMPLE_SIZE} more"


def fetch_dags() -> list[dict]:
    dags = [
        {
            "dag_id": d["dag_id"],
            "is_paused": bool(d.get("is_paused")),
            "has_import_error": bool(d.get("has_import_errors")),
        }
        for d in list_dags()
    ]
    print(f"{len(dags)} dags registered")
    return dags


def capture_dag_source(dags: list[dict]) -> int:
    rows = []
    for entry in dags:
        dag_id = entry["dag_id"]
        for version in list_dag_versions(dag_id):
            number = version.get("version_number")
            source = get_dag_source(dag_id, number)
            if not source:
                continue
            content = source.get("content") or ""
            rows.append((dag_id, number, content, len(content)))

    with warehouse_connection() as conn, conn.cursor() as cur:
        cur.executemany(_UPSERT_SOURCE, rows)
    print(f"captured source for {len(rows)} dag versions")
    return len(rows)


def capture_dag_runs(dags: list[dict]) -> int:
    """Run history is written to Postgres and only a count is returned. derive_health
    reads it back from the table, since the history is too big for XCom.
    """
    rows = []
    for entry in dags:
        dag_id = entry["dag_id"]
        for run in list_dag_runs(dag_id, limit=RUN_HISTORY_LIMIT):
            started, ended = run.get("start_date"), run.get("end_date")
            rows.append(
                (
                    dag_id,
                    run["dag_run_id"],
                    _plain(run.get("run_type")),
                    _plain(run.get("state")),
                    run.get("run_after"),
                    run.get("logical_date"),
                    started,
                    ended,
                    (ended - started).total_seconds() if started and ended else None,
                )
            )

    with warehouse_connection() as conn, conn.cursor() as cur:
        cur.executemany(_UPSERT_RUNS, rows)
    print(f"captured {len(rows)} runs across {len(dags)} dags")
    return len(rows)


def derive_health(dags: list[dict], this_run_id: str, run_count: int) -> None:
    """Derived in SQL against real timestamptz columns, ordered by run_after, which
    the API always populates. logical_date is null for unscheduled runs and Postgres
    sorts NULLs first on DESC, so ordering by it would rank those newest.

    this_run_id is excluded because it is still in flight while this task runs and
    would otherwise make capture_pipeline_metadata permanently report itself running.
    """
    errored = {e.get("filename") for e in get_import_errors()}

    assets_by_dag: dict[str, set] = {}
    last_event_by_dag: dict[str, object] = {}
    for event in get_asset_events(limit=250):
        source_dag = event.get("source_dag_id")
        if not source_dag:
            continue
        assets_by_dag.setdefault(source_dag, set()).add(event.get("name"))
        stamp = event.get("timestamp")
        previous = last_event_by_dag.get(source_dag)
        if stamp and (previous is None or stamp > previous):
            last_event_by_dag[source_dag] = stamp

    with warehouse_connection() as conn:
        derived = {
            row[0]: row[1:]
            for row in conn.execute(_DERIVE_HEALTH, (this_run_id,)).fetchall()
        }

        rows = []
        for entry in dags:
            dag_id = entry["dag_id"]
            total, last_state, last_run, last_success, consecutive = derived.get(
                dag_id, (0, None, None, None, 0)
            )
            rows.append(
                (
                    dag_id,
                    entry["is_paused"],
                    entry["has_import_error"]
                    or any(dag_id in (f or "") for f in errored),
                    total,
                    last_state,
                    last_run,
                    last_success,
                    consecutive,
                    last_event_by_dag.get(dag_id),
                    _summarise_assets(assets_by_dag.get(dag_id, set())),
                )
            )

        with conn.cursor() as cur:
            cur.executemany(_UPSERT_HEALTH, rows)

    print(f"derived health for {len(rows)} dags from {run_count} captured runs")
    print()
    print("=== pipeline health, as the agent will read it ===")
    print(f"{'dag_id':<32} {'runs':>5} {'last':<9} {'fails':>5}  last success")
    for row in sorted(rows):
        last_success = str(row[6])[:19] if row[6] else "never"
        print(f"{row[0]:<32} {row[3]:>5} {str(row[4] or '-'):<9} {row[7]:>5}  {last_success}")


def health_summary(dag_ids: list[str]) -> str:
    """One prose line per Dag, for the agent's pipeline-health tool."""
    with warehouse_connection() as conn:
        rows = conn.execute(_READ_HEALTH, (dag_ids,)).fetchall()

    if not rows:
        return (
            f"No health recorded for {', '.join(dag_ids)}. "
            "The capture_pipeline_metadata Dag populates context.pipeline_health."
        )

    lines = []
    for dag_id, state, last_run, last_success, failures, paused, import_error in rows:
        note = f"{dag_id}: last run {state or 'never'}"
        if last_run:
            note += f" at {str(last_run)[:19]}"
        note += f"; last success {str(last_success)[:19] if last_success else 'never'}"
        if failures:
            note += f"; {failures} consecutive failures since then"
        if paused:
            note += "; PAUSED, so its data is not being refreshed"
        if import_error:
            note += "; HAS AN IMPORT ERROR, so it cannot run at all"
        lines.append(note)

    missing = sorted(set(dag_ids) - {r[0] for r in rows})
    if missing:
        lines.append(f"no health recorded for: {', '.join(missing)}")
    return "\n".join(lines)
