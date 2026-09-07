"""Answers a natural-language question about the business, then scores the answer
with an LLM judge. Put your question in the `question` param and trigger.

The agent gets read-only SQL over gold, semantic search over support ticket text, and
the health of the pipelines that built every table it queries.
"""

from datetime import datetime, timedelta

from airflow.sdk import Asset, Param, dag, get_current_context, task
from pydantic_ai.usage import UsageLimits

from include import agent_prompts, agent_store
from include.agent_toolset import analytics_tools, gold_sql

AGENT_RESPONSES = Asset("agent_responses")

AGENT_MODEL_SETTINGS = {
    "openai_reasoning_effort": "low",
    "timeout": 90,
    "max_tokens": 4000,
}
JUDGE_MODEL_SETTINGS = {
    "openai_reasoning_effort": "low",
    "timeout": 60,
    "max_tokens": 2000,
}


@dag(
    schedule=None,
    start_date=datetime(2026, 9, 1),
    tags=["Agentic analytics"],
    params={"question": Param(agent_prompts.DEFAULT_QUESTION, type="string")},
    doc_md=__doc__,
)
def answer_analytics_question():

    @task
    def build_prompt() -> str:
        question = get_current_context()["params"]["question"]
        return agent_prompts.build_question_prompt(question)

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=agent_prompts.AGENT_SYSTEM_PROMPT,
        output_type=agent_prompts.AnalyticsAnswer,
        toolsets=[gold_sql, analytics_tools],
        serialize_output=True,
        usage_limits=UsageLimits(request_limit=8, tool_calls_limit=10),
        agent_params={"model_settings": AGENT_MODEL_SETTINGS},
        execution_timeout=timedelta(minutes=4),
    )
    def answer_question(prompt: str) -> str:
        return prompt

    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt=agent_prompts.JUDGE_SYSTEM_PROMPT,
        output_type=agent_prompts.AnswerScore,
        serialize_output=True,
        usage_limits=UsageLimits(request_limit=3),
        agent_params={"model_settings": JUDGE_MODEL_SETTINGS},
        execution_timeout=timedelta(minutes=2),
    )
    def judge_answer(answer: dict) -> str:
        question = get_current_context()["params"]["question"]
        return agent_prompts.build_judge_prompt(question, answer)

    @task(outlets=[AGENT_RESPONSES])
    def persist_response(answer: dict, scores: dict) -> None:
        context = get_current_context()
        agent_store.persist_answer(
            dag_id=context["dag"].dag_id,
            run_id=context["dag_run"].run_id,
            run_after=context["dag_run"].run_after,
            question=context["params"]["question"],
            answer=answer,
            scores=scores,
        )

    answer = answer_question(build_prompt())
    persist_response(answer=answer, scores=judge_answer(answer))


answer_analytics_question()
