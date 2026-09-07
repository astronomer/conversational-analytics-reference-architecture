"""Support ticket text: chunking, embedding into pgvector, and semantic search."""

from __future__ import annotations

import hashlib
import logging
import os
import uuid

from include.warehouse import warehouse_connection

log = logging.getLogger(__name__)

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_BATCH_SIZE = 256

MIN_COMMENT_CHARS = 200

_UPSERT_CHUNKS = """
INSERT INTO context.ticket_chunks (
    chunk_id, source_kind, ticket_id, comment_id, customer_id,
    product_sku, chunk_ordinal, heading, body, checksum,
    embedding, embedding_model, embedded_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (chunk_id) DO UPDATE SET
    body            = excluded.body,
    heading         = excluded.heading,
    checksum        = excluded.checksum,
    embedding       = excluded.embedding,
    embedding_model = excluded.embedding_model,
    embedded_at     = now()
"""

_NEAREST_CHUNKS = """
SELECT chunk_id::text, source_kind, ticket_id, comment_id, customer_id,
       product_sku, heading, body,
       1 - (embedding <=> %s::vector) AS similarity
  FROM context.ticket_chunks
 WHERE embedding IS NOT NULL
 ORDER BY embedding <=> %s::vector
 LIMIT %s
"""

_SEARCH_RESULT_KEYS = (
    "chunk_id", "source_kind", "ticket_id", "comment_id", "customer_id",
    "product_sku", "heading", "body", "similarity",
)


def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _chunk_id(ticket_id: str, source_kind: str, discriminator: str) -> str:
    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"{ticket_id}:{source_kind}:{discriminator}")
    )


def read_tickets() -> list[dict]:
    with warehouse_connection() as conn:
        tickets = conn.execute(
            """
            SELECT ticket_id, customer_id, product_sku, subject, body
              FROM silver.tickets
             ORDER BY ticket_id
            """
        ).fetchall()
        comments = conn.execute(
            """
            SELECT ticket_id, comment_id, author_role, body, created_at::text
              FROM silver.ticket_comments
             ORDER BY ticket_id, created_at, comment_id
            """
        ).fetchall()

    by_ticket: dict[str, list[dict]] = {}
    for ticket_id, comment_id, author_role, body, created_at in comments:
        by_ticket.setdefault(ticket_id, []).append(
            {
                "comment_id": comment_id,
                "author_role": author_role,
                "body": body,
                "created_at": created_at,
            }
        )

    rows = [
        {
            "ticket_id": t[0],
            "customer_id": t[1],
            "product_sku": t[2],
            "subject": t[3],
            "body": t[4],
            "comments": by_ticket.get(t[0], []),
        }
        for t in tickets
    ]
    print(f"read {len(rows)} tickets and {len(comments)} comments from silver")
    return rows


def chunk_ticket(ticket: dict, comments: list[dict]) -> list[dict]:
    """One chunk for the ticket, one per comment over MIN_COMMENT_CHARS.

    Ticket bodies run 98 to 121 words, so a ticket is already the natural retrieval
    unit and no token windowing is needed. Short comments are skipped: the threads in
    this dataset end in near-identical acknowledgements, and embedding those buries
    the substantive bodies under a cluster of interchangeable one-liners.

    The opening comment is skipped outright rather than length-filtered: Zendesk's own
    API returns a ticket's description as its first comment too, so that comment is
    always an exact duplicate of ticket["body"] and would otherwise be embedded twice.
    """
    heading = ticket["subject"]
    chunks: list[dict] = []

    def build(kind: str, discriminator: str, ordinal: int, body: str, comment_id=None) -> dict:
        return {
            "chunk_id": _chunk_id(ticket["ticket_id"], kind, discriminator),
            "source_kind": kind,
            "ticket_id": ticket["ticket_id"],
            "comment_id": comment_id,
            "customer_id": ticket.get("customer_id"),
            "product_sku": ticket.get("product_sku"),
            "chunk_ordinal": ordinal,
            "heading": heading,
            "body": body,
            "checksum": _checksum(body),
        }

    chunks.append(build("ticket", "0", 0, ticket["body"]))

    ordered = sorted(comments, key=lambda c: (c.get("created_at") or "", c["comment_id"]))
    for ordinal, comment in enumerate(ordered, start=1):
        if comment["body"] == ticket["body"]:
            continue
        if len(comment["body"]) < MIN_COMMENT_CHARS:
            continue
        chunks.append(
            build(
                "comment",
                comment["comment_id"],
                ordinal,
                f"{comment['author_role']}: {comment['body']}",
                comment_id=comment["comment_id"],
            )
        )

    return chunks


def chunk_tickets(tickets: list[dict]) -> list[dict]:
    chunks: list[dict] = []
    for ticket in tickets:
        chunks.extend(chunk_ticket(ticket, ticket["comments"]))
    kinds: dict[str, int] = {}
    for chunk in chunks:
        kinds[chunk["source_kind"]] = kinds.get(chunk["source_kind"], 0) + 1
    print(f"built {len(chunks)} chunks: {kinds}")
    return chunks


def embed_text_for(chunk: dict) -> str:
    """Heading is stored separately and prepended only for the embedding, so a
    retrieved passage includes its thread's subject without printing it twice.
    """
    return f"{chunk['heading']}\n\n{chunk['body']}"


def embed_texts(texts: list[str], model: str = EMBEDDING_MODEL) -> list[list[float]]:
    if not texts:
        return []
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Copy .env_example to .env and add your key, "
            "then restart the stack so the containers pick it up."
        )

    from openai import OpenAI

    client = OpenAI()
    vectors: list[list[float]] = []
    for start in range(0, len(texts), EMBEDDING_BATCH_SIZE):
        batch = texts[start : start + EMBEDDING_BATCH_SIZE]
        response = client.embeddings.create(model=model, input=batch)
        vectors.extend(
            item.embedding for item in sorted(response.data, key=lambda d: d.index)
        )
    return vectors


def select_changed(chunks: list[dict]) -> list[dict]:
    """The incremental skip path: a second run with unchanged text embeds nothing."""
    with warehouse_connection() as conn:
        known = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT chunk_id::text, checksum FROM context.ticket_chunks "
                "WHERE embedding IS NOT NULL"
            ).fetchall()
        }
    changed = [c for c in chunks if known.get(c["chunk_id"]) != c["checksum"]]
    print(f"{len(known)} chunks already embedded")
    print(f"{len(changed)} chunks new or changed, and they are what will be embedded")
    if not changed:
        print("nothing changed, so this run makes zero OpenAI calls and costs nothing")
    return changed


def embed_and_load(chunks: list[dict]) -> int:
    if not chunks:
        print("no chunks to embed, skipping the OpenAI call entirely")
        return 0

    print(f"embedding {len(chunks)} chunks with {EMBEDDING_MODEL}")
    vectors = embed_texts([embed_text_for(c) for c in chunks])
    with warehouse_connection(vectors=True) as conn, conn.cursor() as cur:
        cur.executemany(
            _UPSERT_CHUNKS,
            [
                (
                    chunk["chunk_id"], chunk["source_kind"], chunk["ticket_id"],
                    chunk["comment_id"], chunk["customer_id"], chunk["product_sku"],
                    chunk["chunk_ordinal"], chunk["heading"], chunk["body"],
                    chunk["checksum"], vector, EMBEDDING_MODEL,
                )
                for chunk, vector in zip(chunks, vectors)
            ],
        )
    return len(chunks)


def search_tickets(query: str, limit: int = 5) -> list[dict]:
    """Support passages closest to a query, most similar first.

    Cosine distance, matching the HNSW index's vector_cosine_ops.
    """
    vector = embed_texts([query])[0]
    with warehouse_connection(vectors=True) as conn:
        rows = conn.execute(_NEAREST_CHUNKS, (vector, vector, limit)).fetchall()

    if not rows:
        raise RuntimeError(
            "context.ticket_chunks is empty. Run the embed_support_tickets Dag."
        )
    return [dict(zip(_SEARCH_RESULT_KEYS, row)) for row in rows]


def cited_passages(chunk_ids: list[str]) -> list[tuple]:
    if not chunk_ids:
        return []
    with warehouse_connection() as conn:
        return conn.execute(
            "SELECT chunk_id::text, ticket_id, heading, body "
            "FROM context.ticket_chunks WHERE chunk_id = ANY(%s::uuid[])",
            (chunk_ids,),
        ).fetchall()
