"""
scripts/anonymize.py

Reads raw SAP CSV exports from knowledge_base/raw_tables/,
anonymizes all confidential fields (material numbers, plant codes,
profit centers, customer numbers, order numbers, person names, etc.)
with consistent cross-table mappings, then generates synthetic order
data (VBAK + VBEP + VBAP) with full referential integrity.

Output: data/anonymized/*.csv
"""

from __future__ import annotations

import datetime
import hashlib
import random
from pathlib import Path

import pandas as pd

RAW_DIR = Path("knowledge_base/raw_tables")
OUT_DIR = Path("data/anonymized")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# ---------------------------------------------------------------------------
# 1. Load all raw tables
# ---------------------------------------------------------------------------

def load(name: str) -> pd.DataFrame:
    path = RAW_DIR / f"{name}.csv"
    return pd.read_csv(path, encoding="utf-8-sig", low_memory=False, dtype=str).fillna("")


print("Loading raw tables...")
mara = load("MARA")
marc = load("MARC")
makt = load("MAKT")
mbew = load("MBEW")
mvke = load("MVKE")
marm = load("MARM")
cepc = load("CEPC")

# ---------------------------------------------------------------------------
# 2. Build consistent ID mappings
# ---------------------------------------------------------------------------

def make_mapper(series_list: list[pd.Series], prefix: str, pad: int = 6) -> dict[str, str]:
    """Build a deterministic {real_value: anonymized_value} mapping."""
    unique_vals = set()
    for s in series_list:
        unique_vals.update(s[s != ""].unique())
    mapper = {}
    for i, val in enumerate(sorted(unique_vals), start=1):
        mapper[val] = f"{prefix}{str(i).zfill(pad)}"
    return mapper


print("Building ID mappings...")

# Material numbers — consistent across all tables
matnr_map = make_mapper(
    [mara["MATNR"], marc["MATNR"], makt["MATNR"], mbew["MATNR"], mvke["MATNR"], marm["MATNR"]],
    prefix="MAT-",
    pad=6,
)

# Plant codes
werks_map = make_mapper(
    [marc["WERKS"], mbew["BWKEY"], mvke["DWERK"]],
    prefix="PLNT-",
    pad=2,
)

# Profit centers
prctr_map = make_mapper(
    [cepc["PRCTR"]],
    prefix="PC-",
    pad=4,
)
nprctr_map = make_mapper(
    [cepc["NPRCTR"]],
    prefix="SEG-",
    pad=3,
)

# Sales orgs
vkorg_map = make_mapper([mvke["VKORG"]], prefix="SORG-", pad=2)

# ---------------------------------------------------------------------------
# 3. Helper — apply a mapping to a column (leave unmapped values as-is)
# ---------------------------------------------------------------------------

def apply_map(df: pd.DataFrame, col: str, mapping: dict[str, str]) -> pd.DataFrame:
    if col in df.columns:
        df[col] = df[col].map(lambda v: mapping.get(v, v))
    return df


def hash_field(value: str) -> str:
    if not value:
        return value
    return hashlib.sha256(value.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# 4. Anonymize material master tables
# ---------------------------------------------------------------------------

print("Anonymizing MARA...")
mara = apply_map(mara, "MATNR", matnr_map)
mara["ERNAM"] = ""   # creator name
mara["AENAM"] = ""   # last modifier

print("Anonymizing MARC...")
marc = apply_map(marc, "MATNR", matnr_map)
marc = apply_map(marc, "WERKS", werks_map)
marc = apply_map(marc, "PRCTR", prctr_map)
marc["DISPO"] = marc["DISPO"].apply(hash_field)  # MRP controller (already hashed in export)

print("Anonymizing MAKT...")
makt = apply_map(makt, "MATNR", matnr_map)
# Material descriptions: replace with generic names
makt["MAKTX"] = makt["MATNR"].apply(lambda m: f"Product {m}")
makt["MAKTG"] = makt["MAKTX"]

print("Anonymizing MBEW...")
mbew = apply_map(mbew, "MATNR", matnr_map)
mbew = apply_map(mbew, "BWKEY", werks_map)  # valuation area = plant

print("Anonymizing MVKE...")
mvke = apply_map(mvke, "MATNR", matnr_map)
mvke = apply_map(mvke, "VKORG", vkorg_map)
mvke = apply_map(mvke, "DWERK", werks_map)

print("Anonymizing MARM...")
marm = apply_map(marm, "MATNR", matnr_map)

print("Anonymizing CEPC...")
cepc = apply_map(cepc, "PRCTR", prctr_map)
cepc = apply_map(cepc, "NPRCTR", nprctr_map)
# Remove person names
for col in ["VERAK", "VERAK_USER", "NAME1", "NAME2", "NAME3", "NAME4", "USNAM"]:
    if col in cepc.columns:
        cepc[col] = ""

# ---------------------------------------------------------------------------
# 5. Generate synthetic orders with full referential integrity
#    We need VBAK + VBEP + VBAP with consistent keys that JOIN to
#    the anonymized material master above.
# ---------------------------------------------------------------------------

print("Generating synthetic orders...")

# ---- Pick materials that have full master data (MARA + MARC + MAKT + MBEW) ----
anon_matnr = list(matnr_map.values())
marc_matnr_plant = list(zip(marc["MATNR"].tolist(), marc["WERKS"].tolist()))

# Filter to materials that also have MBEW (valuation price) entries
mbew_keys = set(zip(mbew["MATNR"].tolist(), mbew["BWKEY"].tolist()))
valid_pairs = [
    (m, w) for m, w in marc_matnr_plant
    if (m, w) in mbew_keys and m != ""
]
if not valid_pairs:
    # Fallback: use all marc entries
    valid_pairs = [(m, w) for m, w in marc_matnr_plant if m != ""]

random.shuffle(valid_pairs)
valid_pairs = valid_pairs[:200]  # cap at 200 unique material/plant combinations

# ---- CEPC lookup: map (nprctr → prctr) ----
# Build a dict: prctr → nprctr + abtei
cepc_lookup = (
    cepc[cepc["PRCTR"] != ""][["PRCTR", "NPRCTR", "ABTEI"]]
    .drop_duplicates("PRCTR")
    .set_index("PRCTR")
    .to_dict("index")
)

# ---- MARA lookup: matnr → base UOM ----
mara_lookup_uom = mara.set_index("MATNR")["MEINS"].to_dict()

# ---- MARC lookup: matnr+werks → prctr, ekgrp ----
marc_lookup = (
    marc[marc["MATNR"] != ""][["MATNR", "WERKS", "PRCTR", "EKGRP"]]
    .drop_duplicates(["MATNR", "WERKS"])
    .set_index(["MATNR", "WERKS"])
    .to_dict("index")
)

# ---- MAKT lookup: matnr → description ----
makt_lookup = makt.set_index("MATNR")["MAKTX"].to_dict()

# ---- MBEW lookup: (matnr, werks) → verpr ----
mbew_lookup = (
    mbew[mbew["MATNR"] != ""][["MATNR", "BWKEY", "VERPR"]]
    .drop_duplicates(["MATNR", "BWKEY"])
    .set_index(["MATNR", "BWKEY"])["VERPR"]
    .to_dict()
)

# ---- MVKE lookup: (matnr, werks) → vrkme, mvgr2 ----
mvke_lookup = (
    mvke[mvke["MATNR"] != ""][["MATNR", "DWERK", "VRKME", "MVGR2", "VKORG", "VTWEG"]]
    .drop_duplicates(["MATNR", "DWERK"])
    .set_index(["MATNR", "DWERK"])
    .to_dict("index")
)

# ---- Date helpers ----


def random_date(start_year: int = 2023, end_year: int = 2025) -> str:
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    delta = (end - start).days
    d = start + datetime.timedelta(days=random.randint(0, delta))
    return d.strftime("%Y%m%d")


def sap_date(d: datetime.date) -> str:
    return d.strftime("%Y%m%d")


ORDER_TYPES = ["ORD-TYPE-A", "ORD-TYPE-B", "ORD-TYPE-C"]  # anonymized SAP order types
PRCTR_KEYS = [k for k in cepc_lookup.keys() if k]

vbak_rows = []
vbep_rows = []
vbap_rows = []

order_id_counter = 1000000

for i in range(300):  # 300 synthetic orders
    vbeln = f"ORD-{str(order_id_counter + i).zfill(9)}"
    cust_num = f"CUST-{str(random.randint(1, 50)).zfill(4)}"
    order_date = random_date(2023, 2025)
    order_type = random.choice(ORDER_TYPES)
    sorg = random.choice(list(vkorg_map.values())) if vkorg_map else "SORG-01"

    # Order header (VBAK)
    vbak_rows.append({
        "MANDT": "500",
        "VBELN": vbeln,
        "ERDAT": order_date,
        "AUART": order_type,
        "VKORG": sorg,
        "VTWEG": "O1",
        "SPART": "D0",
        "NETWR": str(round(random.uniform(500, 50000), 2)),
        "WAERK": "ZBR",
        "KUNNR": cust_num,
        "BSTNK": f"CPO-{str(random.randint(100000, 999999))}",
        "TELF1": "",  # cleared
        "BNAME": "",  # cleared
        "ERNAM": "",  # cleared
    })

    # 1-3 line items per order
    n_items = random.randint(1, 3)
    for j in range(n_items):
        if not valid_pairs:
            continue
        matnr, werks = random.choice(valid_pairs)
        posnr = str((j + 1) * 10).zfill(6)

        marc_data = marc_lookup.get((matnr, werks), {})
        prctr = marc_data.get("PRCTR", "")
        ekgrp = marc_data.get("EKGRP", "")
        meins = mara_lookup_uom.get(matnr, "ST") or "ST"
        mvke_data = mvke_lookup.get((matnr, werks), {})
        vrkme = mvke_data.get("VRKME", meins) or meins
        unit_price = float(mbew_lookup.get((matnr, werks), "1.00") or "1.00")
        kwmeng = random.randint(1, 500)
        netpr = round(unit_price * kwmeng, 2)

        # Schedule line (VBEP)
        delivery_date_offset = random.randint(14, 180)
        delivery_date = datetime.date.today() + datetime.timedelta(days=delivery_date_offset)

        vbep_rows.append({
            "MANDT": "500",
            "VBELN": vbeln,
            "POSNR": posnr,
            "ETENR": "0001",
            "EDATU": sap_date(delivery_date),
            "WMENG": str(float(kwmeng)),
            "BMENG": str(float(kwmeng)),
            "VRKME": vrkme,
            "MEINS": meins,
        })

        # Order item (VBAP)
        vbap_rows.append({
            "MANDT": "500",
            "VBELN": vbeln,
            "POSNR": posnr,
            "MATNR": matnr,
            "WERKS": werks,
            "PSTYV": "NORM",
            "ABGRU": "",  # empty = not rejected
            "KWMENG": str(float(kwmeng)),
            "VRKME": vrkme,
            "MEINS": meins,
            "NETPR": str(unit_price),
            "NETWR": str(netpr),
            "WAERK": "ZBR",
            "PRCTR": prctr,
            "ARKTX": makt_lookup.get(matnr, f"Product {matnr}"),
            "EKGRP": ekgrp,
        })

print(f"Generated {len(vbak_rows)} orders, {len(vbap_rows)} items, {len(vbep_rows)} schedule lines")

# ---------------------------------------------------------------------------
# 6. Save anonymized tables
# ---------------------------------------------------------------------------

print("Saving anonymized tables...")

mara.to_csv(OUT_DIR / "MARA.csv", index=False)
marc.to_csv(OUT_DIR / "MARC.csv", index=False)
makt.to_csv(OUT_DIR / "MAKT.csv", index=False)
mbew.to_csv(OUT_DIR / "MBEW.csv", index=False)
mvke.to_csv(OUT_DIR / "MVKE.csv", index=False)
marm.to_csv(OUT_DIR / "MARM.csv", index=False)
cepc.to_csv(OUT_DIR / "CEPC.csv", index=False)
pd.DataFrame(vbak_rows).to_csv(OUT_DIR / "VBAK.csv", index=False)
pd.DataFrame(vbep_rows).to_csv(OUT_DIR / "VBEP.csv", index=False)
pd.DataFrame(vbap_rows).to_csv(OUT_DIR / "VBAP.csv", index=False)

# ---------------------------------------------------------------------------
# 7. Export the config constants used in the Gold model
# ---------------------------------------------------------------------------

all_werks = sorted(set(werks_map.values()))
config_lines = [
    "# Auto-generated by scripts/anonymize.py",
    "# Use these constants in the Gold notebook filters",
    "",
    f"PLANT_CODES = {all_werks!r}",
    f"ORDER_TYPES = {ORDER_TYPES!r}",
    "",
    "# Mapping reference (original → anonymized) — DO NOT COMMIT to git",
    f"# MATNR: {len(matnr_map)} materials mapped",
    f"# WERKS: {len(werks_map)} plants mapped",
    f"# PRCTR: {len(prctr_map)} profit centers mapped",
    f"# NPRCTR: {len(nprctr_map)} segments mapped",
]
(OUT_DIR / "pipeline_config.py").write_text("\n".join(config_lines))

print("Done! Anonymized files saved to data/anonymized/")
print(f"  Plant codes in use: {all_werks}")
print(f"  Order types in use: {ORDER_TYPES}")
