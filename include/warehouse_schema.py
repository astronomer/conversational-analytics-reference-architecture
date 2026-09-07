"""DDL for every object the project writes to, plus the read-only role the agent
connects as. Called by the Dags in dags/setup/.
"""

from __future__ import annotations

from include.warehouse import warehouse_connection

SCHEMAS = ["bronze", "silver", "gold", "context", "agent"]

READER_ROLE = "analytics_reader"

_CREATE_READER_ROLE = f"""
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{READER_ROLE}') THEN
        CREATE ROLE {READER_ROLE} WITH LOGIN PASSWORD '{READER_ROLE}';
    END IF;
END
$$
"""

_READER_GRANTS = [
    f"GRANT CONNECT ON DATABASE warehouse TO {READER_ROLE}",
    f"GRANT USAGE ON SCHEMA gold TO {READER_ROLE}",
    f"GRANT SELECT ON ALL TABLES IN SCHEMA gold TO {READER_ROLE}",
    f"ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO {READER_ROLE}",
]

_READER_PRIVILEGE_CHECK = f"""
SELECT has_schema_privilege('{READER_ROLE}', 'gold', 'USAGE'),
       has_schema_privilege('{READER_ROLE}', 'bronze', 'USAGE'),
       has_schema_privilege('{READER_ROLE}', 'context', 'USAGE'),
       has_schema_privilege('{READER_ROLE}', 'agent', 'USAGE')
"""

CONTEXT_TABLES = """
CREATE TABLE IF NOT EXISTS context.warehouse_catalog (
    table_schema      text NOT NULL,
    table_name        text NOT NULL,
    table_description text,
    column_count      integer,
    row_count         bigint,
    captured_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_schema, table_name)
);

CREATE TABLE IF NOT EXISTS context.column_profile (
    table_schema       text NOT NULL,
    table_name         text NOT NULL,
    column_name        text NOT NULL,
    ordinal_position   integer,
    data_type          text,
    is_nullable        boolean,
    column_description text,
    null_fraction      real,
    n_distinct         real,
    most_common_values text,
    captured_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_schema, table_name, column_name)
);

CREATE TABLE IF NOT EXISTS context.catalog_prompt (
    table_schema text NOT NULL PRIMARY KEY,
    prompt_text  text NOT NULL,
    char_count   integer NOT NULL,
    captured_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS context.ticket_chunks (
    chunk_id        uuid PRIMARY KEY,
    source_kind     text NOT NULL,
    ticket_id       text NOT NULL,
    comment_id      text,
    customer_id     text,
    product_sku     text,
    chunk_ordinal   integer NOT NULL,
    heading         text,
    body            text NOT NULL,
    checksum        text NOT NULL,
    embedding       vector(1536),
    embedding_model text NOT NULL,
    embedded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ticket_chunks_embedding_hnsw
    ON context.ticket_chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS ticket_chunks_ticket_id
    ON context.ticket_chunks (ticket_id);

CREATE TABLE IF NOT EXISTS context.dag_source (
    dag_id         text NOT NULL,
    version_number integer NOT NULL,
    source_code    text,
    source_chars   integer,
    captured_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, version_number)
);

CREATE TABLE IF NOT EXISTS context.dag_run_history (
    dag_id         text NOT NULL,
    run_id         text NOT NULL,
    run_type       text,
    state          text,
    run_after      timestamptz,
    logical_date   timestamptz,
    run_started_at timestamptz,
    run_ended_at   timestamptz,
    duration_secs  real,
    captured_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, run_id)
);

CREATE TABLE IF NOT EXISTS context.pipeline_health (
    dag_id               text PRIMARY KEY,
    is_paused            boolean,
    has_import_error     boolean,
    total_runs           integer,
    last_state           text,
    last_run_at          timestamptz,
    last_success_at      timestamptz,
    consecutive_failures integer,
    last_asset_event_at  timestamptz,
    produces_assets      text,
    captured_at          timestamptz NOT NULL DEFAULT now()
);
"""

AGENT_TABLES = """
CREATE TABLE IF NOT EXISTS agent.responses (
    dag_id         text NOT NULL,
    run_id         text NOT NULL,
    task_id        text NOT NULL,
    question       text NOT NULL,
    answer         text,
    sql_used       jsonb,
    tables_used    jsonb,
    chunks_used    jsonb,
    freshness_note text,
    caveats        text,
    confidence     text,
    model          text,
    run_after      timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, run_id)
);

CREATE TABLE IF NOT EXISTS agent.answer_scores (
    dag_id      text NOT NULL,
    run_id      text NOT NULL,
    scores      jsonb NOT NULL,
    judge_model text,
    scored_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, run_id),
    FOREIGN KEY (dag_id, run_id) REFERENCES agent.responses (dag_id, run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS agent.trace_spans (
    trace_id         text NOT NULL,
    span_id          text NOT NULL,
    dag_id           text,
    run_id           text,
    task_id          text,
    map_index        integer,
    try_number       integer,
    service_name     text,
    model            text,
    input_tokens     integer,
    output_tokens    integer,
    reasoning_tokens integer,
    cost             numeric(18, 6),
    duration_ms      real,
    tool_calls       jsonb,
    tools_available  jsonb,
    prompt           jsonb,
    output           jsonb,
    start_unix_nano  bigint,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS trace_spans_run ON agent.trace_spans (dag_id, run_id);
"""


def create_extension() -> None:
    with warehouse_connection() as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        version = conn.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
        ).fetchone()
    print(f"pgvector {version[0] if version else 'MISSING'}")


def create_schemas() -> None:
    with warehouse_connection() as conn:
        for schema in SCHEMAS:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"schemas: {', '.join(SCHEMAS)}")


def create_reader_role() -> None:
    """ALTER DEFAULT PRIVILEGES is what makes the grant apply to the gold tables dbt
    has not created yet.
    """
    with warehouse_connection() as conn:
        conn.execute(_CREATE_READER_ROLE)
        for grant in _READER_GRANTS:
            conn.execute(grant)
        gold, bronze, context, agent = conn.execute(_READER_PRIVILEGE_CHECK).fetchone()
    print(f"{READER_ROLE} can read gold={gold}")
    print(
        f"  and cannot read bronze={not bronze} "
        f"context={not context} agent={not agent}"
    )


def create_context_tables() -> None:
    with warehouse_connection() as conn:
        conn.execute(CONTEXT_TABLES)
    print("context tables and the pgvector HNSW index created")


def create_agent_tables() -> None:
    with warehouse_connection() as conn:
        conn.execute(AGENT_TABLES)
        rows = conn.execute(
            """
            SELECT table_schema, count(*)
              FROM information_schema.tables
             WHERE table_schema = ANY(%s)
             GROUP BY table_schema ORDER BY table_schema
            """,
            (SCHEMAS,),
        ).fetchall()
    print("agent tables created")
    print("warehouse ready:")
    for schema, count in rows:
        print(f"  {schema:<8} {count} tables")


def require_confirmation(confirmed: bool) -> None:
    if not confirmed:
        raise ValueError(
            "Nothing was dropped. Set confirm to true in the trigger form: this Dag "
            f"drops the {', '.join(SCHEMAS)} schemas and everything in them."
        )
    print("confirmed")


def drop_schemas() -> None:
    with warehouse_connection() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {', '.join(SCHEMAS)} CASCADE")
    print(f"dropped: {', '.join(SCHEMAS)}")


def drop_reader_role() -> None:
    """DROP OWNED BY first, or the DROP fails on the role's CONNECT privilege on the
    database, which is not stored in any schema and so is not removed with it.
    """
    with warehouse_connection() as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", (READER_ROLE,)
        ).fetchone()
        if not exists:
            print(f"{READER_ROLE} does not exist")
            return
        conn.execute(f"DROP OWNED BY {READER_ROLE}")
        conn.execute(f"DROP ROLE {READER_ROLE}")
    print(f"dropped role {READER_ROLE}")


def drop_extension() -> None:
    with warehouse_connection() as conn:
        conn.execute("DROP EXTENSION IF EXISTS vector")
    print("dropped the vector extension")


def verify_empty() -> None:
    with warehouse_connection() as conn:
        schemas = conn.execute(
            "SELECT count(*) FROM pg_namespace WHERE nspname = ANY(%s)", (SCHEMAS,)
        ).fetchone()[0]
        extension = conn.execute(
            "SELECT count(*) FROM pg_extension WHERE extname = 'vector'"
        ).fetchone()[0]
        role = conn.execute(
            "SELECT count(*) FROM pg_roles WHERE rolname = %s", (READER_ROLE,)
        ).fetchone()[0]
    print(f"project schemas remaining: {schemas}")
    print(f"vector extension present:  {bool(extension)}")
    print(f"{READER_ROLE} present:       {bool(role)}")
    if schemas or extension or role:
        raise RuntimeError("the warehouse is not empty, see the counts above")
    print()
    print("The warehouse is back to the state a clone starts in.")
    print("Run initialize_warehouse next.")
