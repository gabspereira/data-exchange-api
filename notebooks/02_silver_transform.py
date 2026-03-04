# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning & Joining
# MAGIC
# MAGIC **Purpose:** Read Bronze tables, apply type casting, data quality checks,
# MAGIC and produce cleaned Silver tables ready for Gold aggregations.
# MAGIC
# MAGIC **Pipeline stage:** Silver (conform + validate)
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `silver.orders` — Joined VBAK + VBEP + VBAP (active schedule lines only)
# MAGIC - `silver.materials` — Joined MARA + MARC + MAKT (English descriptions)
# MAGIC - `silver.valuation` — MBEW with typed price fields
# MAGIC - `silver.uom_conversion` — MARM with numeric ratios
# MAGIC - `silver.profit_centers` — CEPC with current-validity filter

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType
from datetime import datetime

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
CATALOG = "main"
DQ_LOG_TABLE = f"{CATALOG}.{SILVER_SCHEMA}._dq_log"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

# MAGIC %md ## Data Quality helper

# COMMAND ----------

dq_results = []

def dq_check(df, table: str, check_name: str, expr) -> None:
    """
    Run a data quality assertion. Logs result to dq_results list.
    Prints a warning but does NOT halt the pipeline (warn-only mode).
    """
    fail_count = df.filter(~expr).count()
    status = "PASS" if fail_count == 0 else "WARN"
    dq_results.append({
        "run_id": RUN_ID,
        "table": table,
        "check": check_name,
        "status": status,
        "fail_count": fail_count,
        "checked_at": datetime.utcnow().isoformat(),
    })
    icon = "✓" if status == "PASS" else "⚠"
    print(f"  [{status}] {icon} {table}.{check_name} — {fail_count} failures")


def persist_dq_log() -> None:
    """Append DQ results to the Delta DQ log table."""
    if not dq_results:
        return
    schema_cols = ["run_id","table","check","status","fail_count","checked_at"]
    rows = [{k: r[k] for k in schema_cols} for r in dq_results]
    dq_df = spark.createDataFrame(rows)
    (
        dq_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(DQ_LOG_TABLE)
    )
    print(f"\nDQ log persisted to {DQ_LOG_TABLE} ({len(rows)} checks)")

# COMMAND ----------

# MAGIC %md ## Silver: orders
# MAGIC
# MAGIC Joins VBAK (header) + VBEP (schedule lines) + VBAP (items).
# MAGIC Filters to active, non-rejected lines.

# COMMAND ----------

print("=== Building silver.orders ===")

vbak = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vbak")
vbep = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vbep")
vbap = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vbap")

# Cast types
vbak = (
    vbak
    .withColumn("erdat", F.to_date("ERDAT", "yyyyMMdd"))
    .withColumn("netwr", F.col("NETWR").cast(DecimalType(18, 2)))
    .select(
        F.col("VBELN").alias("order_number"),
        F.col("erdat").alias("order_date"),
        F.col("AUART").alias("order_type"),
        F.col("VKORG").alias("sales_org"),
        F.col("VTWEG").alias("distribution_channel"),
        F.col("SPART").alias("division"),
        F.col("netwr").alias("header_net_value"),
        F.col("WAERK").alias("currency"),
        F.col("KUNNR").alias("customer_number"),
        F.col("BSTNK").alias("customer_po"),
    )
)

vbep = (
    vbep
    .withColumn("edatu", F.to_date("EDATU", "yyyyMMdd"))
    .withColumn("confirmed_qty", F.col("BMENG").cast(DecimalType(18, 3)))
    .filter(F.col("ETENR") == "0001")  # first schedule line only
    .select(
        F.col("VBELN").alias("order_number"),
        F.col("POSNR").alias("order_item"),
        F.col("ETENR").alias("schedule_line"),
        F.col("edatu").alias("delivery_date"),
        F.col("confirmed_qty"),
        F.col("VRKME").alias("sales_uom"),
        F.col("MEINS").alias("base_uom_vbep"),
    )
)

vbap = (
    vbap
    .withColumn("order_qty", F.col("KWMENG").cast(DecimalType(18, 3)))
    .withColumn("net_price", F.col("NETPR").cast(DecimalType(18, 2)))
    .withColumn("net_value", F.col("NETWR").cast(DecimalType(18, 2)))
    .filter(F.col("ABGRU").isNull() | (F.col("ABGRU") == ""))  # exclude rejected items
    .filter(F.col("PSTYV") != "YOPR")                          # exclude irrelevant item categories
    .select(
        F.col("VBELN").alias("order_number"),
        F.col("POSNR").alias("order_item"),
        F.col("MATNR").alias("material_number"),
        F.col("WERKS").alias("plant"),
        F.col("PSTYV").alias("item_category"),
        F.col("order_qty"),
        F.col("VRKME").alias("sales_uom_vbap"),
        F.col("MEINS").alias("base_uom_vbap"),
        F.col("net_price"),
        F.col("net_value"),
        F.col("currency").alias("item_currency"),
        F.col("PRCTR").alias("profit_center"),
        F.col("ARKTX").alias("item_description"),
        F.col("EKGRP").alias("purchasing_group"),
    )
)

# Join: VBAK ⋈ VBEP ⋈ VBAP
orders_raw = (
    vbak.join(vbep, on="order_number", how="inner")
        .join(vbap, on=["order_number", "order_item"], how="inner")
)

# Resolve UOM (prefer VBAP's base_uom)
orders = orders_raw.withColumn(
    "base_uom",
    F.coalesce(F.col("base_uom_vbap"), F.col("base_uom_vbep")),
).drop("base_uom_vbap", "base_uom_vbep")

# DQ checks
dq_check(orders, "silver.orders", "not_null:order_number", F.col("order_number").isNotNull() & (F.col("order_number") != ""))
dq_check(orders, "silver.orders", "not_null:material_number", F.col("material_number").isNotNull() & (F.col("material_number") != ""))
dq_check(orders, "silver.orders", "not_null:plant", F.col("plant").isNotNull() & (F.col("plant") != ""))
dq_check(orders, "silver.orders", "positive:order_qty", F.col("order_qty") > 0)
dq_check(orders, "silver.orders", "positive:net_price", F.col("net_price") >= 0)
dq_check(orders, "silver.orders", "valid_delivery_date", F.col("delivery_date") >= F.lit("2015-01-01").cast(DateType()))

(
    orders.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.orders")
)
print(f"  ✓ silver.orders: {orders.count():,} rows")

# COMMAND ----------

# MAGIC %md ## Silver: materials
# MAGIC
# MAGIC Joins MARA + MARC (plant-level) + MAKT (English descriptions).

# COMMAND ----------

print("=== Building silver.materials ===")

mara = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.mara")
marc = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.marc")
makt = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.makt").filter(F.col("SPRAS") == "E")

mara_sel = mara.select(
    F.col("MATNR").alias("material_number"),
    F.col("MEINS").alias("base_uom"),
    F.col("MATKL").alias("material_group"),
    F.col("MTART").alias("material_type"),
)

marc_sel = marc.select(
    F.col("MATNR").alias("material_number"),
    F.col("WERKS").alias("plant"),
    F.col("DISPO").alias("mrp_controller"),
    F.col("PRCTR").alias("profit_center"),
    F.col("EKGRP").alias("purchasing_group"),
    F.col("DISMM").alias("mrp_type"),
)

makt_sel = makt.select(
    F.col("MATNR").alias("material_number"),
    F.col("MAKTX").alias("material_description"),
)

materials = (
    marc_sel
    .join(mara_sel, on="material_number", how="left")
    .join(makt_sel, on="material_number", how="left")
)

dq_check(materials, "silver.materials", "not_null:material_number", F.col("material_number").isNotNull() & (F.col("material_number") != ""))
dq_check(materials, "silver.materials", "not_null:plant", F.col("plant").isNotNull() & (F.col("plant") != ""))
# Unique on (material_number, plant)
dup_count = materials.count() - materials.dropDuplicates(["material_number", "plant"]).count()
if dup_count:
    print(f"  [WARN] silver.materials: {dup_count} duplicate (material, plant) pairs")

(
    materials.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.materials")
)
print(f"  ✓ silver.materials: {materials.count():,} rows")

# COMMAND ----------

# MAGIC %md ## Silver: valuation

# COMMAND ----------

print("=== Building silver.valuation ===")

mbew = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.mbew")
valuation = (
    mbew
    .withColumn("moving_avg_price", F.col("VERPR").cast(DecimalType(18, 4)))
    .withColumn("standard_price",   F.col("STPRS").cast(DecimalType(18, 4)))
    .select(
        F.col("MATNR").alias("material_number"),
        F.col("BWKEY").alias("plant"),
        F.col("moving_avg_price"),
        F.col("standard_price"),
        F.col("BKLAS").alias("valuation_class"),
        F.col("VPRSV").alias("price_control"),
    )
)

dq_check(valuation, "silver.valuation", "not_null:material_number", F.col("material_number").isNotNull() & (F.col("material_number") != ""))
dq_check(valuation, "silver.valuation", "not_null:plant", F.col("plant").isNotNull() & (F.col("plant") != ""))

(
    valuation.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.valuation")
)
print(f"  ✓ silver.valuation: {valuation.count():,} rows")

# COMMAND ----------

# MAGIC %md ## Silver: uom_conversion

# COMMAND ----------

print("=== Building silver.uom_conversion ===")

marm = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.marm")
uom = (
    marm
    .withColumn("numerator",   F.col("UMREZ").cast(DecimalType(18, 6)))
    .withColumn("denominator", F.col("UMREN").cast(DecimalType(18, 6)))
    .select(
        F.col("MATNR").alias("material_number"),
        F.col("MEINH").alias("alt_uom"),
        F.col("numerator"),
        F.col("denominator"),
    )
    .filter(F.col("denominator") > 0)  # avoid divide-by-zero
)

(
    uom.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.uom_conversion")
)
print(f"  ✓ silver.uom_conversion: {uom.count():,} rows")

# COMMAND ----------

# MAGIC %md ## Silver: profit_centers
# MAGIC
# MAGIC Keeps only currently valid rows (DATAB <= today <= DATBI).

# COMMAND ----------

print("=== Building silver.profit_centers ===")

cepc = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.cepc")
today = F.current_date()

profit_centers = (
    cepc
    .withColumn("valid_from", F.to_date("DATAB", "yyyyMMdd"))
    .withColumn("valid_to",   F.to_date("DATBI", "yyyyMMdd"))
    .filter(F.col("valid_from") <= today)
    .filter(F.col("valid_to")   >= today)
    .filter(F.col("SPRAS") == "P")
    .select(
        F.col("PRCTR").alias("profit_center"),
        F.col("NPRCTR").alias("segment_profit_center"),
        F.col("ABTEI").alias("department"),
        F.col("valid_from"),
        F.col("valid_to"),
    )
    .dropDuplicates(["profit_center"])
)

dq_check(profit_centers, "silver.profit_centers", "not_null:profit_center", F.col("profit_center").isNotNull() & (F.col("profit_center") != ""))
dq_check(profit_centers, "silver.profit_centers", "not_null:segment_profit_center", F.col("segment_profit_center").isNotNull() & (F.col("segment_profit_center") != ""))

(
    profit_centers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.profit_centers")
)
print(f"  ✓ silver.profit_centers: {profit_centers.count():,} rows")

# COMMAND ----------

# MAGIC %md ## Persist DQ log & summary

# COMMAND ----------

persist_dq_log()
print("\n=== Silver transformation complete ===")
print(f"Run ID: {RUN_ID}")
pass_count = sum(1 for r in dq_results if r["status"] == "PASS")
warn_count = sum(1 for r in dq_results if r["status"] != "PASS")
print(f"DQ results: {pass_count} PASS / {warn_count} WARN")
