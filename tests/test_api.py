"""
tests/test_api.py

Integration tests for the FastAPI demand endpoints.
Uses httpx TestClient with a mocked Databricks connector so no
real Databricks connection is required locally.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)

# ---------------------------------------------------------------------------
# Sample data returned by the mocked DB connector
# ---------------------------------------------------------------------------

SAMPLE_ROW = {
    "sk_demand": "abc123def456",
    "order_number": "ORD-000001001",
    "order_item": "000010",
    "schedule_line": "0001",
    "delivery_date": "2025-06-30",
    "material_number": "MAT-000001",
    "plant": "PLNT-01",
    "item_category": "NORM",
    "mrp_controller": "MRP-001",
    "profit_center": "PC-0001",
    "segment_profit_center": "SEG-001",
    "department": "DEPT-A",
    "purchasing_group": "EK1",
    "base_uom": "ST",
    "sales_uom": "ST",
    "quantity_base_uom": 100,
    "order_qty": "100.000",
    "material_description": "Product MAT-000001",
    "unit_price": "15.50",
    "valuation_class": "4000",
    "order_type": "ORD-TYPE-A",
    "sales_org": "SORG-01",
    "distribution_channel": "O1",
    "customer_number": "CUST-0001",
    "KEY": "SEG-001PLNT-01MAT-000001",
    "_processed_at": "2025-03-01T06:00:00",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    def test_health_returns_ok(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestDemandReportList:
    @patch("src.api.routes.demand.execute_query")
    def test_list_returns_paginated_response(self, mock_exec):
        mock_exec.side_effect = [
            [{"cnt": 1}],      # count query
            [SAMPLE_ROW],      # data query
        ]
        response = client.get("/api/v1/demand/report")
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 1
        assert body["page"] == 1
        assert body["page_size"] == 50
        assert len(body["items"]) == 1

    @patch("src.api.routes.demand.execute_query")
    def test_list_item_fields_are_correct(self, mock_exec):
        mock_exec.side_effect = [[{"cnt": 1}], [SAMPLE_ROW]]
        response = client.get("/api/v1/demand/report")
        item = response.json()["items"][0]
        assert item["sk_demand"] == "abc123def456"
        assert item["order_number"] == "ORD-000001001"
        assert item["material_number"] == "MAT-000001"
        assert item["plant"] == "PLNT-01"
        assert item["quantity_base_uom"] == 100

    @patch("src.api.routes.demand.execute_query")
    def test_list_empty_result(self, mock_exec):
        mock_exec.side_effect = [[{"cnt": 0}], []]
        response = client.get("/api/v1/demand/report")
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 0
        assert body["items"] == []

    @patch("src.api.routes.demand.execute_query")
    def test_list_pagination_params(self, mock_exec):
        mock_exec.side_effect = [[{"cnt": 5}], [SAMPLE_ROW]]
        response = client.get("/api/v1/demand/report?page=2&page_size=1")
        assert response.status_code == 200
        body = response.json()
        assert body["page"] == 2
        assert body["page_size"] == 1

    @patch("src.api.routes.demand.execute_query")
    def test_list_invalid_page_size(self, mock_exec):
        """page_size must be between 1 and 500."""
        response = client.get("/api/v1/demand/report?page_size=0")
        assert response.status_code == 422

    @patch("src.api.routes.demand.execute_query")
    def test_list_filter_by_plant(self, mock_exec):
        """Plant filter must be passed to the query."""
        mock_exec.side_effect = [[{"cnt": 1}], [SAMPLE_ROW]]
        response = client.get("/api/v1/demand/report?plant=PLNT-01")
        assert response.status_code == 200
        # Verify the SQL contained the plant filter
        call_args = mock_exec.call_args_list[0][0][0]
        assert "PLNT-01" in call_args


class TestDemandReportByKey:
    @patch("src.api.routes.demand.execute_query")
    def test_get_by_key_returns_items(self, mock_exec):
        mock_exec.return_value = [SAMPLE_ROW]
        response = client.get("/api/v1/demand/report/SEG-001PLNT-01MAT-000001")
        assert response.status_code == 200
        items = response.json()
        assert isinstance(items, list)
        assert len(items) == 1
        assert items[0]["material_number"] == "MAT-000001"

    @patch("src.api.routes.demand.execute_query")
    def test_get_by_key_not_found(self, mock_exec):
        mock_exec.return_value = []
        response = client.get("/api/v1/demand/report/NONEXISTENT-KEY")
        assert response.status_code == 404
        assert "records found" in response.json()["detail"].lower()
