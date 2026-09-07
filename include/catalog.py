"""The gold catalog: capturing table and column metadata into the context schema,
and rendering it as the schema description the agent is given.
"""

from __future__ import annotations

import logging

from include.warehouse import warehouse_connection

log = logging.getLogger(__name__)

GOLD_SCHEMA = "gold"

CATALOG_PROMPT_CHAR_BUDGET = 20000

_UPSERT_TABLES = """
INSERT INTO context.warehouse_catalog
    (table_schema, table_name, table_description, column_count, row_count, captured_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (table_schema, table_name) DO UPDATE SET
    table_description = excluded.table_description,
    column_count      = excluded.column_count,
    row_count         = excluded.row_count,
    captured_at       = now()
"""

_TABLE_METADATA = """
SELECT obj_description(c.oid),
       (SELECT count(*) FROM information_schema.columns col
         WHERE col.table_schema = n.nspname AND col.table_name = c.relname)
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = %s AND c.relname = %s
"""

_COLUMN_METADATA = """
SELECT col.table_name,
       col.column_name,
       col.ordinal_position,
       col.data_type,
       col.is_nullable = 'YES',
       col_description(c.oid, col.ordinal_position),
       stat.null_frac,
       stat.n_distinct,
       left(stat.most_common_vals::text, 500)
  FROM information_schema.columns col
  JOIN pg_class c ON c.relname = col.table_name
  JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = col.table_schema
  LEFT JOIN pg_stats stat
         ON stat.schemaname = col.table_schema
        AND stat.tablename = col.table_name
        AND stat.attname = col.column_name
 WHERE col.table_schema = %s
 ORDER BY col.table_name, col.ordinal_position
"""

_UPSERT_COLUMNS = """
INSERT INTO context.column_profile
    (table_schema, table_name, column_name, ordinal_position, data_type,
     is_nullable, column_description, null_fraction, n_distinct,
     most_common_values, captured_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (table_schema, table_name, column_name) DO UPDATE SET
    ordinal_position   = excluded.ordinal_position,
    data_type          = excluded.data_type,
    is_nullable        = excluded.is_nullable,
    column_description = excluded.column_description,
    null_fraction      = excluded.null_fraction,
    n_distinct         = excluded.n_distinct,
    most_common_values = excluded.most_common_values,
    captured_at        = now()
"""


def analyze_gold_tables() -> list[str]:
    """ANALYZE first: pg_stats comes from the planner's statistics collector, so on a
    freshly built warehouse it is empty and every null_fraction would come back null.
    """
    with warehouse_connection() as conn:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s ORDER BY table_name",
                (GOLD_SCHEMA,),
            ).fetchall()
        ]
        for table in tables:
            conn.execute(f'ANALYZE {GOLD_SCHEMA}."{table}"')
    print(f"analyzed {len(tables)} tables in {GOLD_SCHEMA}")
    return tables


def capture_tables(tables: list[str]) -> int:
    with warehouse_connection() as conn:
        rows = []
        for table in tables:
            count = conn.execute(
                f'SELECT count(*) FROM {GOLD_SCHEMA}."{table}"'
            ).fetchone()[0]
            meta = conn.execute(_TABLE_METADATA, (GOLD_SCHEMA, table)).fetchone()
            description, column_count = meta if meta else (None, None)
            rows.append((GOLD_SCHEMA, table, description, column_count, count))

        with conn.cursor() as cur:
            cur.executemany(_UPSERT_TABLES, rows)
    print(f"captured {len(rows)} tables")
    return len(rows)


def capture_columns() -> int:
    with warehouse_connection() as conn:
        rows = conn.execute(_COLUMN_METADATA, (GOLD_SCHEMA,)).fetchall()
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_COLUMNS, [(GOLD_SCHEMA, *row) for row in rows])
    with_stats = sum(1 for row in rows if row[6] is not None)
    print(f"captured {len(rows)} columns, {with_stats} carrying statistics")
    return len(rows)


_UPSERT_PROMPT = """
INSERT INTO context.catalog_prompt (table_schema, prompt_text, char_count, captured_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (table_schema) DO UPDATE SET
    prompt_text = excluded.prompt_text,
    char_count  = excluded.char_count,
    captured_at = now()
"""


def render_catalog_prompt(char_budget: int = CATALOG_PROMPT_CHAR_BUDGET) -> str:
    """The gold schema as prose for the agent's prompt.

    Trimmed to a budget so the schema does not crowd out the question. Column
    descriptions on the fact tables are dropped first, because the marts are what
    most questions need.
    """
    with warehouse_connection() as conn:
        tables = conn.execute(
            """
            SELECT table_name, table_description, row_count
              FROM context.warehouse_catalog
             WHERE table_schema = %s
             ORDER BY table_name
            """,
            (GOLD_SCHEMA,),
        ).fetchall()
        columns = conn.execute(
            """
            SELECT table_name, column_name, data_type, column_description
              FROM context.column_profile
             WHERE table_schema = %s
             ORDER BY table_name, ordinal_position
            """,
            (GOLD_SCHEMA,),
        ).fetchall()

    if not tables:
        raise RuntimeError(
            "context.warehouse_catalog is empty. Run the capture_warehouse_catalog Dag."
        )

    by_table: dict[str, list[tuple]] = {}
    for table_name, column_name, data_type, description in columns:
        by_table.setdefault(table_name, []).append((column_name, data_type, description))

    def build(include_fact_column_docs: bool) -> str:
        blocks = []
        for table_name, table_description, row_count in tables:
            lines = [f"{GOLD_SCHEMA}.{table_name} ({row_count} rows)"]
            if table_description:
                lines.append(f"  {table_description}")
            keep_docs = include_fact_column_docs or not table_name.startswith("fct_")
            for column_name, data_type, description in by_table.get(table_name, []):
                if description and keep_docs:
                    lines.append(f"  - {column_name} ({data_type}): {description}")
                else:
                    lines.append(f"  - {column_name} ({data_type})")
            blocks.append("\n".join(lines))
        return "\n\n".join(blocks)

    rendered = build(include_fact_column_docs=True)
    if len(rendered) > char_budget:
        rendered = build(include_fact_column_docs=False)
        log.info("catalog prompt trimmed: dropped fact-table column descriptions")
    return rendered


def persist_catalog_prompt(char_budget: int = CATALOG_PROMPT_CHAR_BUDGET) -> str:
    """Renders the catalog prompt once and stores it, so the agent reads a captured
    snapshot instead of re-querying warehouse_catalog and column_profile itself.
    """
    prompt = render_catalog_prompt(char_budget)
    with warehouse_connection() as conn, conn.cursor() as cur:
        cur.execute(_UPSERT_PROMPT, (GOLD_SCHEMA, prompt, len(prompt)))
    return prompt


def read_catalog_prompt() -> str:
    with warehouse_connection() as conn:
        row = conn.execute(
            "SELECT prompt_text FROM context.catalog_prompt WHERE table_schema = %s",
            (GOLD_SCHEMA,),
        ).fetchone()
    if not row:
        raise RuntimeError(
            "context.catalog_prompt is empty. Run the capture_warehouse_catalog Dag."
        )
    return row[0]


def report_catalog(table_count: int, column_count: int) -> None:
    prompt = persist_catalog_prompt()
    print(f"{table_count} tables, {column_count} columns captured")
    print(f"rendered catalog prompt: {len(prompt)} characters")
    print()
    print("=== exactly what the agent will be told about the warehouse ===")
    print(prompt)
