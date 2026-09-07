"""Reading the committed source fixtures and loading them into bronze, one function
group per source system. Called by the Dags in dags/ingestion/.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

from airflow.sdk import Variable

from include.warehouse import fetch_one, load_records

SOURCE_ROOT = (
    Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include" / "sources"
)

STRIPE_STREAMS = ["charges", "refunds", "subscriptions"]

SALESFORCE_OBJECTS = ["accounts", "contacts", "opportunities"]

ZENDESK_CURSOR_VARIABLE = "zendesk_tickets_cursor"


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def reference_files() -> list[str]:
    paths = sorted(str(p) for p in (SOURCE_ROOT / "reference").glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"no reference CSVs found in {SOURCE_ROOT / 'reference'}")
    return paths


def load_reference_file(path: str, run_id: str) -> dict:
    source = Path(path)
    table = f"bronze.reference__{source.stem}"
    return {"table": table, "rows": load_records(table, _read_csv(source), source.name, run_id)}


def report_reference(results: list[dict]) -> int:
    total = sum(r["rows"] for r in results)
    for result in sorted(results, key=lambda r: r["table"]):
        print(f"{result['rows']:>6}  {result['table']}")
    print(f"{total:>6}  total across {len(results)} tables")
    return total


def load_appdb_customers(run_id: str) -> dict:
    rows = _read_csv(SOURCE_ROOT / "appdb" / "customers.csv")
    return {
        "table": "bronze.appdb__customers",
        "rows": load_records("bronze.appdb__customers", rows, "customers.csv", run_id),
        "mode": "full refresh",
    }


def appdb_watermark() -> str | None:
    """Bronze columns are text and these timestamps are ISO-8601 UTC, so the
    lexicographic max is also the chronological max.
    """
    watermark = fetch_one("SELECT max(ordered_at) FROM bronze.appdb__orders")
    print(f"watermark: {watermark or 'none, first run'}")
    return watermark


def load_appdb_since(watermark: str | None, run_id: str) -> dict:
    orders = _read_csv(SOURCE_ROOT / "appdb" / "orders.csv")
    new_orders = [r for r in orders if watermark is None or r["ordered_at"] > watermark]
    order_ids = {r["order_id"] for r in new_orders}

    loaded = {
        "bronze.appdb__orders": load_records(
            "bronze.appdb__orders", new_orders, f"orders.csv:{watermark}", run_id
        ),
        "bronze.appdb__order_items": load_records(
            "bronze.appdb__order_items",
            [
                r
                for r in _read_csv(SOURCE_ROOT / "appdb" / "order_items.csv")
                if r["order_id"] in order_ids
            ],
            f"order_items.csv:{watermark}",
            run_id,
        ),
        "bronze.appdb__shipments": load_records(
            "bronze.appdb__shipments",
            [
                r
                for r in _read_csv(SOURCE_ROOT / "appdb" / "shipments.csv")
                if r["order_id"] in order_ids
            ],
            f"shipments.csv:{watermark}",
            run_id,
        ),
    }
    return {"watermark": watermark, "loaded": loaded, "candidates": len(orders)}


def report_appdb(customers: dict, incremental: dict) -> None:
    print(f"{customers['rows']:>6}  {customers['table']} ({customers['mode']})")
    for table, rows in sorted(incremental["loaded"].items()):
        print(f"{rows:>6}  {table}")
    if not any(incremental["loaded"].values()):
        print(
            "no new rows past the watermark. Expected on a re-run: the source "
            "fixtures are static, so everything is already loaded."
        )


def salesforce_pages() -> list[dict]:
    """Sorted, not bare glob: glob order is filesystem-dependent, which would make
    the loaded row order differ between machines.
    """
    directory = SOURCE_ROOT / "salesforce"
    pages = []
    for name in SALESFORCE_OBJECTS:
        found = sorted(directory.glob(f"{name}_page_*.jsonl"))
        if not found:
            raise FileNotFoundError(f"no page files for {name} in {directory}")
        pages.extend({"object": name, "path": str(p)} for p in found)
    return pages


def load_salesforce_page(page: dict, run_id: str) -> dict:
    source = Path(page["path"])
    table = f"bronze.salesforce__{page['object']}"
    return {
        "table": table,
        "page": source.name,
        "rows": load_records(table, _read_jsonl(source), source.name, run_id),
    }


def report_salesforce(results: list[dict]) -> None:
    by_table: dict[str, dict] = {}
    for result in results:
        entry = by_table.setdefault(result["table"], {"pages": 0, "rows": 0})
        entry["pages"] += 1
        entry["rows"] += result["rows"]
    for table, entry in sorted(by_table.items()):
        print(f"{entry['rows']:>6} rows across {entry['pages']:>2} pages  {table}")


def zendesk_cursor() -> str | None:
    cursor = Variable.get(ZENDESK_CURSOR_VARIABLE, default=None)
    print(f"resuming from cursor: {cursor or 'none, first sync'}")
    return cursor


def fetch_zendesk_page(cursor: str | None) -> dict:
    payload = _read_json(SOURCE_ROOT / "zendesk" / "tickets.json")
    print(
        f"fetched {payload['count']} tickets, "
        f"next_cursor={payload['next_cursor']}, "
        f"end_of_stream={payload['end_of_stream']}"
    )
    return payload


def load_zendesk_tickets(payload: dict, run_id: str) -> dict:
    """Comments arrive nested inside each ticket and are flattened into their own
    bronze table, which both the silver model and the embedding chunker read.
    """
    ticket_rows = []
    comment_rows = []
    for ticket in payload["tickets"]:
        ticket_rows.append({k: v for k, v in ticket.items() if k != "comments"})
        for comment in ticket["comments"]:
            comment_rows.append({"ticket_id": ticket["ticket_id"], **comment})

    loaded = {
        "bronze.zendesk__tickets": load_records(
            "bronze.zendesk__tickets", ticket_rows, "tickets.json", run_id
        ),
        "bronze.zendesk__ticket_comments": load_records(
            "bronze.zendesk__ticket_comments",
            comment_rows,
            "tickets.json:comments",
            run_id,
        ),
    }
    return {"loaded": loaded, "next_cursor": payload["next_cursor"]}


def commit_zendesk_cursor(result: dict) -> None:
    for table, rows in sorted(result["loaded"].items()):
        print(f"{rows:>6}  {table}")
    Variable.set(ZENDESK_CURSOR_VARIABLE, result["next_cursor"])
    print(f"cursor advanced to {result['next_cursor']}")


def load_stripe_stream(stream: str, run_id: str) -> dict:
    """The envelope's id and type are kept alongside the unwrapped object, so bronze
    records the envelope as well as the object inside it.
    """
    events = _read_json(SOURCE_ROOT / "stripe" / f"{stream}.json")
    rows = [
        {
            "event_id": event["id"],
            "event_type": event["type"],
            "event_created": event["created"],
            **event["data"]["object"],
        }
        for event in events
    ]
    table = f"bronze.stripe__{stream}"
    return {
        "table": table,
        "rows": load_records(table, rows, f"{stream}.json", run_id),
        "event_types": sorted({r["event_type"] for r in rows}),
    }


def report_stripe(results: list[dict]) -> None:
    for result in sorted(results, key=lambda r: r["table"]):
        print(
            f"{result['rows']:>6}  {result['table']}  "
            f"[{', '.join(result['event_types'])}]"
        )


def reset_zendesk_cursor() -> None:
    if Variable.get(ZENDESK_CURSOR_VARIABLE, default=None) is None:
        print(f"no {ZENDESK_CURSOR_VARIABLE} Variable to delete")
        return
    Variable.delete(ZENDESK_CURSOR_VARIABLE)
    print(f"deleted the {ZENDESK_CURSOR_VARIABLE} Variable")
