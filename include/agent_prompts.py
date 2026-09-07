"""What the analytics agent and the judge are told, and what each must return.

The Field descriptions on the output models are prompt text: the model reads them.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from include.catalog import read_catalog_prompt
from include.embeddings import cited_passages

DEFAULT_QUESTION = (
    "Which products generate the most support tickets relative to the revenue "
    "they bring in, and what are customers actually complaining about?"
)

AGENT_SYSTEM_PROMPT = """\
You are an analytics assistant for an ecommerce marketplace. You answer questions \
about the business from its data warehouse.

Only the `gold` schema is queryable. It is a star schema: dimensions prefixed `dim_`, \
facts prefixed `fct_`, and pre-aggregated marts prefixed `mart_`. Most questions are \
answered fastest from a mart.

Rules for the SQL tools:
- Write table names UNQUOTED, as fct_orders or gold.fct_orders. The query validator \
refuses quoted table identifiers such as "gold"."fct_orders" and the query will fail.
- The `query` tool returns a columnar result: {"columns": [...], "rows": [[...]], \
"row_count": N}. When a result is capped it also includes `truncated_by`. Read values \
positionally against `columns`.
- This dataset is a frozen window ending 2026-08-31. Measure recency against \
dim_date.is_window_end, never against current_date, which would return nothing.

Rules for the other tools:
- Use search_tickets for what customers said in their own words. Use SQL for \
anything countable. Put the chunk_ids you relied on in chunks_used.
- Before reporting any figure that looks surprising, call get_pipeline_health for the \
Dags that build the tables you used, and say what you found in freshness_note. \
transform_warehouse builds every gold table; the ingest_* Dags load the raw data.

Answer from what the tools return and nothing else. If the data cannot support an \
answer, say so in the answer rather than estimating. Put every statement you ran in \
sql_used and every table you read in tables_used.\
"""

JUDGE_SYSTEM_PROMPT = """\
You score an analytics answer against the evidence that produced it.

You are shown the question, the answer, and the SQL statements the agent ran, and the \
full text of any support ticket passages it cited. You are NOT shown the result rows \
the SQL returned. Judge whether the statements could produce the figures claimed, not \
whether you can recompute them. Missing result rows are a limit of the evidence, not a \
fault in the answer. Judge qualitative claims about what customers said against the \
cited passages you are given.

Do not re-run the SQL, do not compute your own answer, and do not penalise an answer \
for being brief if it is correct and complete. An answer that states honestly where \
the data did not support a conclusion is better than one that fills the gap.\
"""


class AnalyticsAnswer(BaseModel):
    answer: str = Field(description="The answer, in prose, citing the actual numbers.")
    sql_used: list[str] = Field(description="Every SQL statement you ran.")
    tables_used: list[str] = Field(description="Every gold table you read.")
    chunks_used: list[str] = Field(
        description="chunk_ids of ticket passages the answer relied on. Empty if none."
    )
    freshness_note: str = Field(
        description=(
            "What get_pipeline_health reported for the Dags behind these tables, and "
            "whether it affects the answer."
        )
    )
    caveats: str = Field(description="What this answer does not establish.")
    confidence: Literal["high", "medium", "low"] = Field(
        description="Your confidence, given what the tools returned."
    )


class Dimension(BaseModel):
    score: Literal["good", "acceptable", "poor"]
    confidence: Literal["high", "medium", "low"]
    reasoning: str = Field(description="One sentence supporting the score.")


class AnswerScore(BaseModel):
    """Both booleans are phrased so True is the good outcome: a True-is-bad flag
    invites a double negative that models get wrong.
    """

    grounded: Dimension = Field(
        description="Is every number in the answer traceable to the SQL that was run."
    )
    answers_question: Dimension = Field(
        description="Does the answer address the question that was asked."
    )
    sql_correct: Dimension = Field(
        description="Does the SQL compute what the answer claims it computes."
    )
    caveats_appropriate: Dimension = Field(
        description="Are the stated caveats and freshness note honest and sufficient."
    )
    all_figures_traceable: bool = Field(
        description=(
            "True when every figure in the answer appears as a column selected by, or "
            "a computation performed in, the SQL shown. False only when a figure could "
            "not have come from those statements."
        )
    )
    data_was_fresh: bool = Field(
        description=(
            "True when the freshness note shows the pipelines behind these tables "
            "succeeded recently. False when it shows a failure, a pause, or staleness "
            "that affects the figures reported."
        )
    )


def build_question_prompt(question: str) -> str:
    catalog = read_catalog_prompt()
    print(f"question: {question}")
    print(f"catalog: {len(catalog)} characters")
    return f"Question: {question}\n\nThe gold schema available to you:\n\n{catalog}"


def build_judge_prompt(question: str, answer: dict) -> str:
    """The cited ticket passages are fetched and shown to the judge. Without them it
    can only validate the numeric half of an answer.
    """
    rows = cited_passages(answer.get("chunks_used") or [])
    passages = (
        "Ticket passages the answer cited:\n"
        + "\n\n".join(
            f"[{cid}] ticket {tid}\n{heading}\n{body}" for cid, tid, heading, body in rows
        )
        if rows
        else "The answer cited no ticket passages."
    )
    return (
        f"Question asked:\n{question}\n\n"
        f"Answer given:\n{answer['answer']}\n\n"
        f"SQL the agent ran:\n" + "\n".join(answer["sql_used"]) + "\n\n"
        f"Tables read: {', '.join(answer['tables_used'])}\n\n"
        f"Freshness note: {answer['freshness_note']}\n\n"
        f"Caveats stated: {answer['caveats']}\n\n"
        f"Confidence claimed: {answer['confidence']}\n\n"
        f"{passages}"
    )
