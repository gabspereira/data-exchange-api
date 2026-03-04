from __future__ import annotations

from typing import Any

from databricks import sql as dbsql

from src.api.config import settings

_connection: Any = None


def get_connection():
    """Return a cached Databricks SQL connection."""
    global _connection
    if _connection is None:
        _connection = dbsql.connect(
            server_hostname=settings.databricks_host.replace("https://", ""),
            http_path=settings.databricks_http_path,
            access_token=settings.databricks_token,
        )
    return _connection


def execute_query(sql: str, params: dict | None = None) -> list[dict]:
    """Execute a SQL query and return rows as a list of dicts."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(sql, params or {})
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]
