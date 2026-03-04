"""
tests/test_transformations.py

Unit tests for the Gold demand_rpt transformation logic.
Uses pandas (not PySpark) for fast local execution — the same logic
is applied in PySpark at scale in Databricks.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures — minimal in-memory DataFrames that mirror Silver tables
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_orders() -> pd.DataFrame:
    return pd.DataFrame({
        "order_number":   ["ORD-000001001", "ORD-000001001", "ORD-000001002"],
        "order_item":     ["000010", "000020", "000010"],
        "schedule_line":  ["0001", "0001", "0001"],
        "delivery_date":  pd.to_datetime(["2025-06-30", "2025-07-31", "2025-08-31"]),
        "material_number":["MAT-000001", "MAT-000002", "MAT-000001"],
        "plant":          ["PLNT-01", "PLNT-01", "PLNT-02"],
        "item_category":  ["NORM", "NORM", "NORM"],
        "order_qty":      [Decimal("100"), Decimal("50"), Decimal("200")],
        "sales_uom_vbap": ["ST", "KG", "ST"],
        "order_type":     ["ORD-TYPE-A", "ORD-TYPE-A", "ORD-TYPE-B"],
        "sales_org":      ["SORG-01", "SORG-01", "SORG-01"],
        "customer_number":["CUST-0001", "CUST-0001", "CUST-0002"],
    })


@pytest.fixture()
def sample_materials() -> pd.DataFrame:
    return pd.DataFrame({
        "material_number": ["MAT-000001", "MAT-000001", "MAT-000002"],
        "plant":           ["PLNT-01", "PLNT-02", "PLNT-01"],
        "base_uom":        ["ST", "ST", "KG"],
        "mrp_controller":  ["MRP-001", "MRP-001", "MRP-002"],
        "profit_center":   ["PC-0001", "PC-0002", "PC-0001"],
        "purchasing_group":["EK1", "EK1", "EK2"],
        "material_description": ["Product MAT-000001", "Product MAT-000001", "Product MAT-000002"],
    })


@pytest.fixture()
def sample_valuation() -> pd.DataFrame:
    return pd.DataFrame({
        "material_number": ["MAT-000001", "MAT-000001", "MAT-000002"],
        "plant":           ["PLNT-01", "PLNT-02", "PLNT-01"],
        "moving_avg_price":[Decimal("15.50"), Decimal("15.50"), Decimal("8.25")],
        "valuation_class": ["4000", "4000", "3100"],
    })


@pytest.fixture()
def sample_profit_centers() -> pd.DataFrame:
    return pd.DataFrame({
        "profit_center":          ["PC-0001", "PC-0002"],
        "segment_profit_center":  ["SEG-001", "SEG-002"],
        "department":             ["DEPT-A", "DEPT-B"],
    })


# ---------------------------------------------------------------------------
# Helper — the same surrogate key logic used in the Gold notebook
# ---------------------------------------------------------------------------

def compute_sk_demand(order_number: str, order_item: str, schedule_line: str) -> str:
    raw = f"{order_number}|{order_item}|{schedule_line}"
    return hashlib.md5(raw.encode()).hexdigest()


def apply_gold_transform(
    orders: pd.DataFrame,
    materials: pd.DataFrame,
    valuation: pd.DataFrame,
    profit_centers: pd.DataFrame,
    plant_codes: list[str],
    order_types: list[str],
) -> pd.DataFrame:
    """Pandas implementation of the Gold demand_rpt logic."""
    # Filter
    df = orders[orders["plant"].isin(plant_codes) & orders["order_type"].isin(order_types)].copy()

    # Join material attrs
    df = df.merge(materials, on=["material_number", "plant"], how="left")

    # UOM: same UOM → no conversion; different → quantity stays as-is (no MARM in this unit test)
    df["quantity_base_uom"] = df["order_qty"].apply(lambda x: int(x))

    # Join valuation
    df = df.merge(valuation, on=["material_number", "plant"], how="left")

    # Join profit centers
    df = df.merge(profit_centers, on="profit_center", how="left")

    # KEY and surrogate key
    df["KEY"] = (
        df["segment_profit_center"].fillna("") +
        df["plant"] +
        df["material_number"]
    )
    df["sk_demand"] = df.apply(
        lambda r: compute_sk_demand(r["order_number"], r["order_item"], r["schedule_line"]),
        axis=1,
    )

    # Drop rows without org structure (matches Gold notebook filter)
    df = df[df["segment_profit_center"].notna() & df["department"].notna()]
    return df


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGoldDemandRpt:
    def test_row_count_after_filters(
        self, sample_orders, sample_materials, sample_valuation, sample_profit_centers
    ):
        """Only orders matching plant_codes AND order_types should appear."""
        result = apply_gold_transform(
            sample_orders, sample_materials, sample_valuation, sample_profit_centers,
            plant_codes=["PLNT-01"],
            order_types=["ORD-TYPE-A"],
        )
        # Only rows with plant=PLNT-01 and order_type=ORD-TYPE-A
        assert len(result) == 2

    def test_no_rows_if_no_matching_plant(
        self, sample_orders, sample_materials, sample_valuation, sample_profit_centers
    ):
        result = apply_gold_transform(
            sample_orders, sample_materials, sample_valuation, sample_profit_centers,
            plant_codes=["PLNT-99"],
            order_types=["ORD-TYPE-A"],
        )
        assert len(result) == 0

    def test_sk_demand_is_unique(
        self, sample_orders, sample_materials, sample_valuation, sample_profit_centers
    ):
        """Each row must have a unique surrogate key."""
        result = apply_gold_transform(
            sample_orders, sample_materials, sample_valuation, sample_profit_centers,
            plant_codes=["PLNT-01", "PLNT-02"],
            order_types=["ORD-TYPE-A", "ORD-TYPE-B"],
        )
        assert result["sk_demand"].nunique() == len(result), "sk_demand must be unique"

    def test_sk_demand_is_deterministic(self):
        """Same inputs must always produce the same surrogate key."""
        sk1 = compute_sk_demand("ORD-000001001", "000010", "0001")
        sk2 = compute_sk_demand("ORD-000001001", "000010", "0001")
        assert sk1 == sk2

    def test_sk_demand_differs_for_different_inputs(self):
        """Different order/item/line combinations must produce different keys."""
        sk1 = compute_sk_demand("ORD-000001001", "000010", "0001")
        sk2 = compute_sk_demand("ORD-000001001", "000020", "0001")
        assert sk1 != sk2

    def test_key_column_format(
        self, sample_orders, sample_materials, sample_valuation, sample_profit_centers
    ):
        """KEY must be non-empty and follow the segment+plant+material pattern."""
        result = apply_gold_transform(
            sample_orders, sample_materials, sample_valuation, sample_profit_centers,
            plant_codes=["PLNT-01"],
            order_types=["ORD-TYPE-A"],
        )
        assert (result["KEY"] != "").all(), "KEY must not be empty"
        for _, row in result.iterrows():
            assert row["plant"] in row["KEY"], "plant code must be part of KEY"
            assert row["material_number"] in row["KEY"], "material number must be part of KEY"

    def test_quantity_base_uom_is_positive(
        self, sample_orders, sample_materials, sample_valuation, sample_profit_centers
    ):
        """quantity_base_uom must be a positive integer."""
        result = apply_gold_transform(
            sample_orders, sample_materials, sample_valuation, sample_profit_centers,
            plant_codes=["PLNT-01", "PLNT-02"],
            order_types=["ORD-TYPE-A", "ORD-TYPE-B"],
        )
        assert (result["quantity_base_uom"] > 0).all(), "quantity_base_uom must be positive"
        assert result["quantity_base_uom"].dtype in (int, "int64"), \
            "quantity_base_uom must be integer"

    def test_rows_without_org_structure_excluded(
        self, sample_orders, sample_materials, sample_valuation
    ):
        """Rows with no matching profit center (no segment/department) must be excluded."""
        # Provide an empty profit_centers table
        empty_pc = pd.DataFrame(columns=["profit_center", "segment_profit_center", "department"])
        result = apply_gold_transform(
            sample_orders, sample_materials, sample_valuation, empty_pc,
            plant_codes=["PLNT-01", "PLNT-02"],
            order_types=["ORD-TYPE-A", "ORD-TYPE-B"],
        )
        assert len(result) == 0, "Rows without org structure should be excluded"
