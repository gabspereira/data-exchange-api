from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, Field


class DemandReportItem(BaseModel):
    """A single enriched demand order line from gold.demand_rpt."""

    sk_demand: str = Field(..., description="Surrogate key (md5 hash of order+item+line)")
    order_number: str
    order_item: str
    schedule_line: str
    delivery_date: date | None = None
    material_number: str
    plant: str
    item_category: str | None = None
    mrp_controller: str | None = None
    profit_center: str | None = None
    segment_profit_center: str | None = None
    department: str | None = None
    purchasing_group: str | None = None
    base_uom: str | None = None
    sales_uom: str | None = None
    quantity_base_uom: int | None = None
    order_qty: Decimal | None = None
    material_description: str | None = None
    unit_price: Decimal | None = None
    valuation_class: str | None = None
    order_type: str | None = None
    sales_org: str | None = None
    distribution_channel: str | None = None
    customer_number: str | None = None
    key: str | None = Field(  # noqa: E501
        None, alias="KEY", description="Business grain key: segment+plant+material"
    )

    model_config = {"populate_by_name": True}


class DemandReportPage(BaseModel):
    """Paginated response for the demand report endpoint."""

    total: int
    page: int
    page_size: int
    items: list[DemandReportItem]
