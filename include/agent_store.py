"""Writes into the agent schema: answers, judge scores, and GenAI trace spans.
The Dags are the only writers; the evals plugin only reads.
"""

from __future__ import annotations

import json
import logging

from include.warehouse import warehouse_connection

log = logging.getLogger(__name__)

_UPSERT_RESPONSE = """
INSERT INTO agent.responses
    (dag_id, run_id, task_id, question, answer, sql_used, tables_used,
     chunks_used, freshness_note, caveats, confidence, model, run_after)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dag_id, run_id) DO UPDATE SET
    answer         = excluded.answer,
    sql_used       = excluded.sql_used,
    tables_used    = excluded.tables_used,
    chunks_used    = excluded.chunks_used,
    freshness_note = excluded.freshness_note,
    caveats        = excluded.caveats,
    confidence     = excluded.confidence
"""

_UPSERT_SCORES = """
INSERT INTO agent.answer_scores (dag_id, run_id, scores)
VALUES (%s, %s, %s)
ON CONFLICT (dag_id, run_id) DO UPDATE SET
    scores    = excluded.scores,
    scored_at = now()
"""

_UPSERT_SPANS = """
INSERT INTO agent.trace_spans
    (trace_id, span_id, dag_id, run_id, task_id, map_index, try_number,
     service_name, model, input_tokens, output_tokens, reasoning_tokens,
     cost, duration_ms, tool_calls, tools_available, prompt, output,
     start_unix_nano, ingested_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, now())
ON CONFLICT (trace_id, span_id) DO UPDATE SET
    model            = excluded.model,
    input_tokens     = excluded.input_tokens,
    output_tokens    = excluded.output_tokens,
    reasoning_tokens = excluded.reasoning_tokens,
    duration_ms      = excluded.duration_ms,
    tool_calls       = excluded.tool_calls,
    tools_available  = excluded.tools_available,
    prompt           = excluded.prompt,
    output           = excluded.output,
    ingested_at      = now()
"""

AGENT_TASK_ID = "answer_question"


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else value.model_dump()


def configured_model() -> str | None:
    from airflow.sdk import Connection

    try:
        return (Connection.get("pydanticai_default").extra_dejson or {}).get("model")
    except Exception:
        return None


def persist_answer(
    dag_id: str,
    run_id: str,
    run_after,
    question: str,
    answer,
    scores,
) -> None:
    answer = _as_dict(answer)
    scores = _as_dict(scores)

    with warehouse_connection() as conn, conn.cursor() as cur:
        cur.execute(
            _UPSERT_RESPONSE,
            (
                dag_id,
                run_id,
                AGENT_TASK_ID,
                question,
                answer["answer"],
                json.dumps(answer["sql_used"]),
                json.dumps(answer["tables_used"]),
                json.dumps(answer["chunks_used"]),
                answer["freshness_note"],
                answer["caveats"],
                answer["confidence"],
                configured_model(),
                run_after,
            ),
        )
        cur.execute(_UPSERT_SCORES, (dag_id, run_id, json.dumps(scores)))

    print("=== answer ===")
    print(answer["answer"])
    print()
    print(f"confidence: {answer['confidence']}")
    print(f"freshness:  {answer['freshness_note']}")
    print(f"tables:     {', '.join(answer['tables_used'])}")
    print()
    print("=== judge ===")
    for name, value in scores.items():
        if isinstance(value, dict) and "score" in value:
            print(f"  {name:<22} {value['score']:<11} ({value['confidence']} confidence)")
            print(f"      {value['reasoning']}")
        else:
            print(f"  {name:<22} {value}")


def report_agent_runs(runs: list[dict]) -> None:
    print(f"{len(runs)} agent run spans in the export")
    for run in runs:
        print(
            f"  {run.get('dag_id')}/{run.get('task_id')} "
            f"model={run.get('model')} "
            f"tokens={run.get('input_tokens')}/{run.get('output_tokens')} "
            f"tools={len(run.get('tool_calls') or [])}"
        )


def match_to_responses(runs: list[dict]) -> list[dict]:
    """Matches every span whose (dag_id, run_id) has a response row. Both the
    answer_question and judge_answer tasks of one run match, since agent.responses is
    keyed on the run, not the task; the evals plugin filters to answer_question when
    it reads agent.trace_spans. Only a run with no response row at all is unmatched,
    for example spans left over from before a warehouse reset.
    """
    with warehouse_connection() as conn:
        known = {
            (row[0], row[1])
            for row in conn.execute(
                "SELECT dag_id, run_id FROM agent.responses"
            ).fetchall()
        }

    matched, unmatched = [], []
    for run in runs:
        key = (run.get("dag_id"), run.get("run_id"))
        (matched if key in known else unmatched).append(run)

    print(f"{len(matched)} runs matched a response row")
    if unmatched:
        print(f"{len(unmatched)} runs did not match one:")
        for run in unmatched:
            print(f"  {run.get('dag_id')}/{run.get('task_id')} run_id={run.get('run_id')}")
        print(
            "These belong to a run with no response row at all, for example spans "
            "left over from before a warehouse reset. A run from "
            "answer_analytics_question appearing here means the join is broken."
        )
    return matched


def persist_trace_spans(runs: list[dict]) -> None:
    """cost stays null: pydantic-ai 2.28.0 emits no operation.cost, and a zero there
    would read as a free run.
    """
    if not runs:
        print("no matched agent runs to persist")
        return

    rows = [
        (
            run["trace_id"],
            run["span_id"],
            run.get("dag_id"),
            run.get("run_id"),
            run.get("task_id"),
            run.get("map_index"),
            run.get("try_number"),
            run.get("service_name"),
            run.get("model"),
            run.get("input_tokens"),
            run.get("output_tokens"),
            run.get("reasoning_tokens"),
            run.get("cost"),
            run.get("duration_ms"),
            json.dumps(run.get("tool_calls") or []),
            json.dumps(run.get("tools_available") or []),
            json.dumps(run.get("prompt")),
            json.dumps(run.get("output")),
            run.get("start_unix_nano"),
        )
        for run in runs
    ]

    with warehouse_connection() as conn, conn.cursor() as cur:
        cur.executemany(_UPSERT_SPANS, rows)

    print(f"{len(rows)} trace spans persisted to agent.trace_spans")
