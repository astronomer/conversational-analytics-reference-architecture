"""Airflow UI page for the analytics agent's answers and their eval scores.

Reads the agent schema in Postgres and nothing else; the Dags are the only writers.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent

_ICON_DATA_URI = "data:image/svg+xml;base64," + base64.b64encode(
    (BASE_DIR / "assets" / "icon.svg").read_bytes()
).decode("ascii")

app = FastAPI(title="Analytics Evals")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/assets", StaticFiles(directory=BASE_DIR / "assets"), name="assets")

EVALS_QUERY = """
SELECT r.dag_id,
       r.run_id,
       r.question,
       r.answer,
       r.sql_used,
       r.tables_used,
       r.chunks_used,
       r.freshness_note,
       r.caveats,
       r.confidence,
       coalesce(r.model, t.model)  AS model,
       r.created_at,
       s.scores,
       s.scored_at,
       t.input_tokens,
       t.output_tokens,
       t.reasoning_tokens,
       t.cost,
       t.duration_ms,
       t.tool_calls,
       t.tools_available,
       t.trace_id,
       t.span_id
  FROM agent.responses r
  LEFT JOIN agent.answer_scores s ON s.dag_id = r.dag_id AND s.run_id = r.run_id
  LEFT JOIN agent.trace_spans  t ON t.dag_id = r.dag_id AND t.run_id = r.run_id
                                AND t.task_id = %(agent_task_id)s
 ORDER BY coalesce(s.scored_at, r.created_at) DESC
"""


def _records() -> list[dict]:
    """Returns no rows when the tables are empty or unreachable, so the page renders
    an empty state before any Dag has run.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from include.agent_store import AGENT_TASK_ID

    try:
        hook = PostgresHook(postgres_conn_id="warehouse_default")
        with hook.get_conn() as conn:
            result = conn.execute(EVALS_QUERY, {"agent_task_id": AGENT_TASK_ID})
            columns = [c.name for c in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as error:
        log.warning("cannot read the agent schema: %s", error)
        return []


def _is_dimension(value) -> bool:
    return isinstance(value, dict) and "score" in value


def load_evals() -> list[dict]:
    rows = _records()

    out = []
    for row in rows:
        scores = row.get("scores") or {}
        out.append(
            {
                "eval_id": f"{row['dag_id']}:{row['run_id']}",
                "run_id": row["run_id"],
                "question": row["question"],
                "answer": row["answer"],
                "sql_used": row.get("sql_used") or [],
                "tables_used": row.get("tables_used") or [],
                "chunks_used": row.get("chunks_used") or [],
                "freshness_note": row.get("freshness_note"),
                "caveats": row.get("caveats"),
                "confidence": row.get("confidence"),
                "model": row.get("model"),
                "trace_id": row.get("trace_id"),
                "span_id": row.get("span_id"),
                "input_tokens": row.get("input_tokens"),
                "output_tokens": row.get("output_tokens"),
                "reasoning_tokens": row.get("reasoning_tokens"),
                "cost": float(row["cost"]) if row.get("cost") is not None else None,
                "duration_ms": row.get("duration_ms"),
                "tool_calls": row.get("tool_calls") or [],
                "tools_available": row.get("tools_available") or [],
                "dimensions": {k: v for k, v in scores.items() if _is_dimension(v)},
                "flags": {k: v for k, v in scores.items() if not _is_dimension(v)},
                "scored_at": str(row["scored_at"]) if row.get("scored_at") else None,
            }
        )
    return out


def load_summary() -> dict:
    evals = load_evals()
    total = len(evals)
    if not total:
        return {
            "scored": 0,
            "dimensions": [],
            "avg_reasoning_tokens": None,
            "avg_duration_ms": None,
            "avg_tool_calls": None,
        }

    names: list[str] = []
    for record in evals:
        for name in record["dimensions"]:
            if name not in names:
                names.append(name)

    dimensions = []
    for name in names:
        present = [r["dimensions"][name] for r in evals if name in r["dimensions"]]
        dimensions.append(
            {
                "name": name,
                "counted": len(present),
                "good": sum(1 for d in present if d.get("score") == "good"),
                "acceptable": sum(1 for d in present if d.get("score") == "acceptable"),
                "poor": sum(1 for d in present if d.get("score") == "poor"),
                "low_confidence": sum(1 for d in present if d.get("confidence") == "low"),
            }
        )

    reasoned = [r["reasoning_tokens"] for r in evals if r["reasoning_tokens"] is not None]
    timed = [r["duration_ms"] for r in evals if r["duration_ms"] is not None]
    tools = [len(r["tool_calls"]) for r in evals]

    return {
        "scored": total,
        "dimensions": dimensions,
        "avg_reasoning_tokens": round(sum(reasoned) / len(reasoned)) if reasoned else None,
        "avg_duration_ms": round(sum(timed) / len(timed), 1) if timed else None,
        "avg_tool_calls": round(sum(tools) / total, 1) if total else None,
    }


@app.get("/ui", response_class=FileResponse)
async def serve_ui():
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/api/summary")
async def get_summary():
    return await asyncio.to_thread(load_summary)


@app.get("/api/evals")
async def get_evals():
    return await asyncio.to_thread(load_evals)


class AnalyticsEvalsPlugin(AirflowPlugin):
    name = "analytics_evals_plugin"

    fastapi_apps = [
        {
            "app": app,
            "url_prefix": "/analytics-evals",
            "name": "Analytics Evals",
        }
    ]

    external_views = [
        {
            "name": "Analytics Evals",
            "href": "analytics-evals/ui",
            "destination": "nav",
            "url_route": "analytics-evals",
            "nav_top_level": True,
            "icon": _ICON_DATA_URI,
        }
    ]
