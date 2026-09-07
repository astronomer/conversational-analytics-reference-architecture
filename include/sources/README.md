# Source fixtures

Every payload here is generated and committed. No DAG in this project contacts a
real Salesforce, Zendesk, or Stripe API.

## What is generated versus vendored

| Directory | Origin |
|---|---|
| `reference/` | 17 CSVs vendored from `astronomer/astro-event-demo`, `include/csvs`. Reference data only: no customers, orders, tickets, or payments |
| `appdb/` | Generated. The internal application database: customers, orders, order_items, shipments |
| `salesforce/` | Generated. CRM accounts, contacts, opportunities, split into 100-record page files to mimic a paginated API |
| `zendesk/` | Generated. Support tickets, with comments nested inside each ticket the way the Zendesk API returns them. These bodies are the only text the embedding DAG has to work with |
| `stripe/` | Generated. Charges, refunds, subscriptions, wrapped in event envelopes |

Every foreign key is drawn from `reference/`, so the gold-layer joins resolve. The
dbt `relationships` tests are what catch it if that ever drifts.

## Regenerating

```bash
docker exec -i $(docker ps --format '{{.Names}}' | grep scheduler | head -1) \
  bash -c "cd /usr/local/airflow && python -c \"
from pathlib import Path
from include.fixtures.generate_sources import generate_all
print(generate_all(Path('include/sources')))
\""
```

Generation is seeded (`SEED = 42`) and byte-for-byte reproducible. `MANIFEST.txt`
lists a sha256 per generated file, so regenerating and seeing no diff in that file
is the proof that nothing drifted. Verify explicitly with `sha256sum -c MANIFEST.txt`
from this directory. `reference/` and this README are excluded from the manifest.

## The frozen date window

All generated timestamps fall between **2026-03-01** and **2026-08-31**. The
generator never reads the wall clock.

The window is declared in **two places** and both must change together:

1. `include/fixtures/generate_sources.py`: `DATA_WINDOW_START` / `DATA_WINDOW_END`
2. `include/dbt/conversational_analytics/dbt_project.yml`: the `data_window_start` and
   `data_window_end` dbt vars

dbt SQL cannot import the Python constants, which is why the value is duplicated.
Gold models compute recency against the dbt var, never against `current_date`, so
"last 30 days" metrics do not silently return zero rows once the calendar moves past
the window. A dbt test enforces that.

## Tickets with no product attached

About 4 percent of tickets (24 of 600) have a null `product_sku`. This is here for
the agent, not for data-quality theatre: it is the case where the agent can overclaim
about a product the ticket never named, and the judge should catch it when it does.
`silver.tickets` keeps those rows with the null intact, and there is deliberately no
`relationships` test on that column.
