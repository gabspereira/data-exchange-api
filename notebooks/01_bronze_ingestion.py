# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest anonymized SAP CSV exports into Delta Bronze tables.
# MAGIC
# MAGIC **Pipeline stage:** Bronze (raw ingestion with schema enforcement)
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `bronze.vbak` — Sales order headers
# MAGIC - `bronze.vbep` — Sales order schedule lines
# MAGIC - `bronze.vbap` — Sales order items
# MAGIC - `bronze.marc` — Plant-level material data
# MAGIC - `bronze.mara` — General material master
# MAGIC - `bronze.makt` — Material descriptions
# MAGIC - `bronze.mbew` — Material valuation
# MAGIC - `bronze.mvke` — Sales organization data
# MAGIC - `bronze.marm` — Unit of measure conversions
# MAGIC - `bronze.cepc` — Profit center master

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

# Source path: anonymized CSVs uploaded to DBFS or Unity Catalog volume
SOURCE_PATH = "/Volumes/main/demand_planning/raw"  # adjust to your DBFS/UC path
BRONZE_SCHEMA = "bronze"
CATALOG = "main"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Schema definitions
# MAGIC
# MAGIC Explicit schemas prevent silent type mismatches and make data contracts visible.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType, DateType, TimestampType,
)
from pyspark.sql import functions as F
from datetime import datetime

INGEST_TS = datetime.utcnow().isoformat()

# Sales order header
VBAK_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("VBELN",  StringType(),  False),  # PK: order number
    StructField("ERDAT",  StringType(),  True),   # creation date (YYYYMMDD)
    StructField("AUART",  StringType(),  True),   # order type
    StructField("VKORG",  StringType(),  True),   # sales org
    StructField("VTWEG",  StringType(),  True),   # distribution channel
    StructField("SPART",  StringType(),  True),   # division
    StructField("NETWR",  StringType(),  True),   # net value
    StructField("WAERK",  StringType(),  True),   # currency
    StructField("KUNNR",  StringType(),  True),   # customer number
    StructField("BSTNK",  StringType(),  True),   # customer PO number
])

# Schedule lines
VBEP_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("VBELN",  StringType(),  False),  # FK → VBAK
    StructField("POSNR",  StringType(),  False),  # item number
    StructField("ETENR",  StringType(),  False),  # schedule line number (PK component)
    StructField("EDATU",  StringType(),  True),   # requested delivery date (YYYYMMDD)
    StructField("WMENG",  StringType(),  True),   # confirmed quantity
    StructField("BMENG",  StringType(),  True),   # confirmed quantity (base UOM)
    StructField("VRKME",  StringType(),  True),   # sales UOM
    StructField("MEINS",  StringType(),  True),   # base UOM
])

# Order items
VBAP_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("VBELN",  StringType(),  False),  # FK → VBAK
    StructField("POSNR",  StringType(),  False),  # PK component
    StructField("MATNR",  StringType(),  True),   # material number
    StructField("WERKS",  StringType(),  True),   # plant
    StructField("PSTYV",  StringType(),  True),   # item category
    StructField("ABGRU",  StringType(),  True),   # rejection reason (empty = active)
    StructField("KWMENG", StringType(),  True),   # order quantity
    StructField("VRKME",  StringType(),  True),   # sales UOM
    StructField("MEINS",  StringType(),  True),   # base UOM
    StructField("NETPR",  StringType(),  True),   # net price
    StructField("NETWR",  StringType(),  True),   # net value
    StructField("WAERK",  StringType(),  True),   # currency
    StructField("PRCTR",  StringType(),  True),   # profit center
    StructField("ARKTX",  StringType(),  True),   # item description
    StructField("EKGRP",  StringType(),  True),   # purchasing group
])

# Plant material data
MARC_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("MATNR",  StringType(),  False),  # PK component
    StructField("WERKS",  StringType(),  False),  # PK component
    StructField("DISPO",  StringType(),  True),   # MRP controller
    StructField("PRCTR",  StringType(),  True),   # profit center
    StructField("EKGRP",  StringType(),  True),   # purchasing group
    StructField("DISMM",  StringType(),  True),   # MRP type
])

# General material master
MARA_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("MATNR",  StringType(),  False),  # PK
    StructField("MEINS",  StringType(),  True),   # base UOM
    StructField("MATKL",  StringType(),  True),   # material group
    StructField("MTART",  StringType(),  True),   # material type
    StructField("SPART",  StringType(),  True),   # division
])

# Material descriptions
MAKT_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("MATNR",  StringType(),  False),  # PK component
    StructField("SPRAS",  StringType(),  False),  # language (PK component)
    StructField("MAKTX",  StringType(),  True),   # short description
])

# Material valuation
MBEW_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("MATNR",  StringType(),  False),  # PK component
    StructField("BWKEY",  StringType(),  False),  # valuation area = plant (PK component)
    StructField("VERPR",  StringType(),  True),   # moving average price
    StructField("STPRS",  StringType(),  True),   # standard price
    StructField("BKLAS",  StringType(),  True),   # valuation class
    StructField("VPRSV",  StringType(),  True),   # price control indicator
])

# Sales-org material data
MVKE_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("MATNR",  StringType(),  False),  # PK component
    StructField("VKORG",  StringType(),  False),  # sales org (PK component)
    StructField("VTWEG",  StringType(),  False),  # distribution channel (PK component)
    StructField("DWERK",  StringType(),  True),   # delivering plant
    StructField("VRKME",  StringType(),  True),   # sales UOM
    StructField("MVGR2",  StringType(),  True),   # material group 2
])

# UOM conversions
MARM_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("MATNR",  StringType(),  False),  # PK component
    StructField("MEINH",  StringType(),  False),  # alternative UOM (PK component)
    StructField("UMREZ",  StringType(),  True),   # numerator
    StructField("UMREN",  StringType(),  True),   # denominator
])

# Profit center master
CEPC_SCHEMA = StructType([
    StructField("MANDT",  StringType(),  False),
    StructField("PRCTR",  StringType(),  False),  # profit center (PK component)
    StructField("DATBI",  StringType(),  False),  # valid to date (PK component)
    StructField("KOKRS",  StringType(),  False),  # controlling area (PK component)
    StructField("DATAB",  StringType(),  True),   # valid from date
    StructField("ABTEI",  StringType(),  True),   # department
    StructField("NPRCTR", StringType(),  True),   # node profit center (segment)
    StructField("SPRAS",  StringType(),  True),   # language key
])

# COMMAND ----------

# MAGIC %md ## Ingestion helper

# COMMAND ----------

def ingest_csv_to_bronze(
    table_name: str,
    schema: StructType,
    source_cols: list[str],
    pk_cols: list[str],
) -> None:
    """
    Read a CSV file, select & cast to declared schema columns,
    add ingestion metadata, and write as Delta Bronze table.

    Args:
        table_name: Target Bronze table name (without schema prefix).
        schema:     Declared schema (used for column selection + casting).
        source_cols: Subset of schema columns to read from CSV.
        pk_cols:    Primary key columns for the not_null + unique DQ checks.
    """
    path = f"{SOURCE_PATH}/{table_name.upper()}.csv"
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    print(f"  Ingesting {path} → {full_table}")

    # Read with schema inference disabled — all columns land as string first
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )

    # Select only the columns we declared (avoids issues with extra SAP columns)
    available = [f.name for f in schema.fields if f.name in df.columns]
    df = df.select(available)

    # Add ingestion metadata
    df = df.withColumn("_ingested_at", F.lit(INGEST_TS)) \
           .withColumn("_source_file", F.lit(path))

    # --- Data Quality: not_null on PKs ---
    for col in pk_cols:
        null_count = df.filter(F.col(col).isNull() | (F.col(col) == "")).count()
        if null_count > 0:
            print(f"    [DQ WARNING] {col} has {null_count} null/empty values")
        else:
            print(f"    [DQ OK] {col} not_null check passed")

    # --- Data Quality: unique on PK ---
    total = df.count()
    distinct_pk = df.dropDuplicates(pk_cols).count()
    if total != distinct_pk:
        dup_count = total - distinct_pk
        print(f"    [DQ WARNING] PK ({', '.join(pk_cols)}) has {dup_count} duplicate rows")
    else:
        print(f"    [DQ OK] PK ({', '.join(pk_cols)}) unique check passed")

    # Write as Delta — overwrite for idempotent re-runs
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )
    print(f"    ✓ Written {total} rows to {full_table}")

# COMMAND ----------

# MAGIC %md ## Run ingestion for all tables

# COMMAND ----------

tables = [
    ("vbak", VBAK_SCHEMA, ["MANDT","VBELN","ERDAT","AUART","VKORG","VTWEG","SPART","NETWR","WAERK","KUNNR","BSTNK"], ["VBELN"]),
    ("vbep", VBEP_SCHEMA, ["MANDT","VBELN","POSNR","ETENR","EDATU","WMENG","BMENG","VRKME","MEINS"], ["VBELN","POSNR","ETENR"]),
    ("vbap", VBAP_SCHEMA, ["MANDT","VBELN","POSNR","MATNR","WERKS","PSTYV","ABGRU","KWMENG","VRKME","MEINS","NETPR","NETWR","WAERK","PRCTR","ARKTX","EKGRP"], ["VBELN","POSNR"]),
    ("marc", MARC_SCHEMA, ["MANDT","MATNR","WERKS","DISPO","PRCTR","EKGRP","DISMM"], ["MATNR","WERKS"]),
    ("mara", MARA_SCHEMA, ["MANDT","MATNR","MEINS","MATKL","MTART","SPART"], ["MATNR"]),
    ("makt", MAKT_SCHEMA, ["MANDT","MATNR","SPRAS","MAKTX"], ["MATNR","SPRAS"]),
    ("mbew", MBEW_SCHEMA, ["MANDT","MATNR","BWKEY","VERPR","STPRS","BKLAS","VPRSV"], ["MATNR","BWKEY"]),
    ("mvke", MVKE_SCHEMA, ["MANDT","MATNR","VKORG","VTWEG","DWERK","VRKME","MVGR2"], ["MATNR","VKORG","VTWEG"]),
    ("marm", MARM_SCHEMA, ["MANDT","MATNR","MEINH","UMREZ","UMREN"], ["MATNR","MEINH"]),
    ("cepc", CEPC_SCHEMA, ["MANDT","PRCTR","DATBI","KOKRS","DATAB","ABTEI","NPRCTR","SPRAS"], ["PRCTR","DATBI","KOKRS"]),
]

print("=== Bronze Ingestion ===")
for table_name, schema, source_cols, pk_cols in tables:
    ingest_csv_to_bronze(table_name, schema, source_cols, pk_cols)

print("\n=== Bronze ingestion complete ===")

# COMMAND ----------

# MAGIC %md ## Verify row counts

# COMMAND ----------

for table_name, *_ in tables:
    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}").count()
    print(f"  {BRONZE_SCHEMA}.{table_name}: {count:,} rows")
