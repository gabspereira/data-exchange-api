from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from src.api.config import settings
from src.api.db.connector import execute_query
from src.api.schemas.demand import DemandReportItem, DemandReportPage

router = APIRouter(prefix="/demand", tags=["Demand"])

GOLD_TABLE = f"{settings.databricks_catalog}.gold.demand_rpt"


@router.get(
    "/report",
    response_model=DemandReportPage,
    summary="List demand report records",
    description=(
        "Returns paginated records from the Gold demand report table. "
        "Optionally filter by plant, order_type, or a specific business KEY."
    ),
)
def list_demand_report(
    plant: str | None = Query(None, description="Filter by plant code (e.g. PLNT-01)"),
    order_type: str | None = Query(None, description="Filter by order type"),
    key: str | None = Query(None, description="Filter by business grain KEY (segment+plant+material)"),  # noqa: E501
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(50, ge=1, le=500, description="Records per page"),
) -> DemandReportPage:
    filters = []
    if plant:
        filters.append(f"plant = '{plant}'")
    if order_type:
        filters.append(f"order_type = '{order_type}'")
    if key:
        filters.append(f"KEY = '{key}'")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    offset = (page - 1) * page_size

    count_sql = f"SELECT COUNT(*) as cnt FROM {GOLD_TABLE} {where_clause}"
    count_result = execute_query(count_sql)
    total = count_result[0]["cnt"] if count_result else 0

    data_sql = (
        f"SELECT * FROM {GOLD_TABLE} {where_clause} "
        f"ORDER BY delivery_date DESC, order_number "
        f"LIMIT {page_size} OFFSET {offset}"
    )
    rows = execute_query(data_sql)
    items = [DemandReportItem(**row) for row in rows]

    return DemandReportPage(total=total, page=page, page_size=page_size, items=items)


@router.get(
    "/report/{key}",
    response_model=list[DemandReportItem],
    summary="Get demand records for a specific KEY",
    description=(
        "Returns all order lines for a given business grain KEY "
        "(segment_profit_center + plant + material)."
    ),
)
def get_demand_by_key(key: str) -> list[DemandReportItem]:
    sql = f"SELECT * FROM {GOLD_TABLE} WHERE KEY = '{key}' ORDER BY delivery_date DESC"
    rows = execute_query(sql)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No records found for KEY={key!r}")
    return [DemandReportItem(**row) for row in rows]
