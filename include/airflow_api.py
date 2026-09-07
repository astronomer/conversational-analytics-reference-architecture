"""Airflow's own state, read over its REST API. Nothing here reads the metadata
database.

Unauthenticated locally: `astro dev` injects an admin token for every request. A
real deployment needs one in the connection's extra, as {"token": "..."}.
"""

from __future__ import annotations

import logging

from airflow.sdk import Connection

log = logging.getLogger(__name__)

CONN_ID = "airflow_api_default"
PAGE_LIMIT = 100

_PAGE_ATTRIBUTES = ("dags", "dag_runs", "import_errors", "asset_events", "dag_versions")


def _client():
    """Imported on call, not at module scope: the generated client is expensive to
    import and Dag files are parsed far more often than these functions are called.
    """
    import airflow_client.client as client

    return client


def _base_url(conn) -> str:
    if conn.host and conn.host.startswith("http"):
        return conn.host.rstrip("/")
    scheme = conn.schema or "http"
    port = f":{conn.port}" if conn.port else ""
    return f"{scheme}://{conn.host}{port}"


def api_client(conn_id: str = CONN_ID):
    client = _client()
    conn = Connection.get(conn_id)
    configuration = client.Configuration(host=_base_url(conn))

    token = (conn.extra_dejson or {}).get("token")
    if token:
        configuration.access_token = token
    else:
        log.info(
            "no token on connection %s; relying on simple_auth_manager_all_admins, "
            "which is local development only",
            conn_id,
        )
    return client.ApiClient(configuration)


def _rows(page) -> list[dict]:
    for attribute in _PAGE_ATTRIBUTES:
        items = getattr(page, attribute, None)
        if items is not None:
            return [item.to_dict() for item in items]
    return []


def list_dags() -> list[dict]:
    client = _client()
    with api_client() as api:
        return _rows(client.DAGApi(api).get_dags(limit=PAGE_LIMIT))


def list_dag_versions(dag_id: str) -> list[dict]:
    client = _client()
    with api_client() as api:
        return _rows(
            client.DagVersionApi(api).get_dag_versions(dag_id=dag_id, limit=PAGE_LIMIT)
        )


def get_dag_source(dag_id: str, version_number: int | None = None) -> dict | None:
    client = _client()
    with api_client() as api:
        try:
            source = client.DagSourceApi(api).get_dag_source(
                dag_id=dag_id, version_number=version_number
            )
        except client.ApiException as error:
            log.warning(
                "no source for %s version %s: %s", dag_id, version_number, error.status
            )
            return None
        return source.to_dict()


def list_dag_runs(dag_id: str, limit: int = 50) -> list[dict]:
    client = _client()
    with api_client() as api:
        return _rows(
            client.DagRunApi(api).get_dag_runs(
                dag_id=dag_id, limit=limit, order_by=["-logical_date"]
            )
        )


def get_import_errors() -> list[dict]:
    client = _client()
    with api_client() as api:
        return _rows(client.ImportErrorApi(api).get_import_errors(limit=PAGE_LIMIT))


def get_asset_events(limit: int = 250) -> list[dict]:
    client = _client()
    with api_client() as api:
        return _rows(
            client.AssetApi(api).get_asset_events(limit=limit, order_by=["-timestamp"])
        )
