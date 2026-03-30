"""
PhonePe Pulse - ETL Script
Extracts JSON data from the cloned PhonePe Pulse GitHub repo
and loads it into a SQLite database (10 tables).

Usage:
    python etl.py

Configuration via environment variables (optional):
    PHONEPE_DATA_PATH  — path to the cloned pulse/data directory
                         default: ./pulse/data  (relative to this script)
    PHONEPE_DB_PATH    — path for the output SQLite database
                         default: ./phonepe.db  (relative to this script)

"""

import os
import json
import sqlite3
import pandas as pd
from pathlib import Path

# ──────────────────────────────────────────────
# CONFIGURATION  (env vars override defaults)
# ──────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent

DATA_PATH = Path(os.environ.get(
    "PHONEPE_DATA_PATH",
    _SCRIPT_DIR / "pulse" / "data"
))

DB_PATH = Path(os.environ.get(
    "PHONEPE_DB_PATH",
    _SCRIPT_DIR / "phonepe.db"
))

# ──────────────────────────────────────────────
# DB SETUP
# ──────────────────────────────────────────────

def get_conn():
    return sqlite3.connect(DB_PATH)

def create_tables(conn):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS aggregated_transaction (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        transaction_type TEXT, transaction_count INTEGER, transaction_amount REAL
    );
    CREATE TABLE IF NOT EXISTS aggregated_user (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        registered_users INTEGER, app_opens INTEGER
    );
    CREATE TABLE IF NOT EXISTS aggregated_user_device (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        brand TEXT, device_count INTEGER, percentage REAL
    );
    CREATE TABLE IF NOT EXISTS aggregated_insurance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        transaction_count INTEGER, transaction_amount REAL
    );
    CREATE TABLE IF NOT EXISTS map_transaction (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        district TEXT, transaction_count INTEGER, transaction_amount REAL
    );
    CREATE TABLE IF NOT EXISTS map_user (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        district TEXT, registered_users INTEGER, app_opens INTEGER
    );
    CREATE TABLE IF NOT EXISTS map_insurance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        district TEXT, transaction_count INTEGER, transaction_amount REAL
    );
    CREATE TABLE IF NOT EXISTS top_transaction (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        entity_name TEXT, entity_type TEXT,
        transaction_count INTEGER, transaction_amount REAL
    );
    CREATE TABLE IF NOT EXISTS top_user (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        entity_name TEXT, entity_type TEXT, registered_users INTEGER
    );
    CREATE TABLE IF NOT EXISTS top_insurance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT, year INTEGER, quarter INTEGER,
        entity_name TEXT, entity_type TEXT,
        transaction_count INTEGER, transaction_amount REAL
    );
    """)
    conn.commit()
    print("✅ All 10 tables created.")

def create_indexes(conn):
    """Create indexes on frequently queried columns for dashboard speed."""
    cur = conn.cursor()
    indexes = [
        ("idx_agg_txn_state_year",   "aggregated_transaction(state, year, quarter)"),
        ("idx_agg_user_state_year",  "aggregated_user(state, year, quarter)"),
        ("idx_agg_ins_state_year",   "aggregated_insurance(state, year, quarter)"),
        ("idx_map_txn_state",        "map_transaction(state, year)"),
        ("idx_map_user_state",       "map_user(state, year)"),
        ("idx_map_ins_state",        "map_insurance(state, year)"),
        ("idx_top_txn_type",         "top_transaction(entity_type, year)"),
        ("idx_top_user_type",        "top_user(entity_type, year)"),
        ("idx_dev_brand",            "aggregated_user_device(brand)"),
    ]
    for name, cols in indexes:
        cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {cols}")
    conn.commit()
    print(f"✅ {len(indexes)} indexes created.")

# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def load_json(path):
    """Safe JSON loader — returns None and prints warning on failure."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠️  Skipping {path.name}: {e}")
        return None

def iter_state_year_quarter(base_path):
    """Yields (state_name, year, quarter, filepath) for state-level data."""
    state_dir = base_path / "state"
    if not state_dir.exists():
        return
    for state in sorted(state_dir.iterdir()):
        if not state.is_dir():
            continue
        for year_dir in sorted(state.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue
            for qfile in sorted(year_dir.glob("*.json")):
                try:
                    quarter = int(qfile.stem)
                except ValueError:
                    continue
                yield state.name, year, quarter, qfile

def iter_country_year_quarter(base_path):
    """Yields (year, quarter, filepath) for country-level aggregates."""
    india = base_path / "country" / "india"
    if not india.exists():
        return
    for year_dir in sorted(india.iterdir()):
        if not year_dir.is_dir():
            continue
        try:
            year = int(year_dir.name)
        except ValueError:
            continue
        for qfile in sorted(year_dir.glob("*.json")):
            try:
                quarter = int(qfile.stem)
            except ValueError:
                continue
            yield year, quarter, qfile

# ──────────────────────────────────────────────
# AGGREGATED TRANSACTION
# ──────────────────────────────────────────────

def load_aggregated_transaction(conn):
    rows = []
    for year, quarter, fpath in iter_country_year_quarter(DATA_PATH / "aggregated" / "transaction"):
        data = load_json(fpath)
        if not data:
            continue
        for item in data.get("data", {}).get("transactionData", []):
            for pi in item.get("paymentInstruments", []):
                rows.append(("india", year, quarter, item["name"], pi["count"], pi["amount"]))

    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "aggregated" / "transaction" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        for item in data.get("data", {}).get("transactionData", []):
            for pi in item.get("paymentInstruments", []):
                rows.append((state, year, quarter, item["name"], pi["count"], pi["amount"]))

    df = pd.DataFrame(rows, columns=["state","year","quarter","transaction_type","transaction_count","transaction_amount"])
    df.to_sql("aggregated_transaction", conn, if_exists="replace", index=False)
    print(f"✅ aggregated_transaction: {len(df):,} rows")

# ──────────────────────────────────────────────
# AGGREGATED USER + DEVICE BRANDS
# ──────────────────────────────────────────────

def load_aggregated_user(conn):
    user_rows   = []
    device_rows = []

    for year, quarter, fpath in iter_country_year_quarter(DATA_PATH / "aggregated" / "user"):
        data = load_json(fpath)
        if not data:
            continue
        d = data.get("data", {})
        user_rows.append(("india", year, quarter,
                          d.get("aggregated", {}).get("registeredUsers", 0),
                          d.get("aggregated", {}).get("appOpens", 0)))
        for brand_info in d.get("usersByDevice", []) or []:
            device_rows.append(("india", year, quarter,
                                brand_info.get("brand", "Unknown"),
                                brand_info.get("count", 0),
                                brand_info.get("percentage", 0.0)))

    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "aggregated" / "user" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        d = data.get("data", {})
        user_rows.append((state, year, quarter,
                          d.get("aggregated", {}).get("registeredUsers", 0),
                          d.get("aggregated", {}).get("appOpens", 0)))
        for brand_info in d.get("usersByDevice", []) or []:
            device_rows.append((state, year, quarter,
                                brand_info.get("brand", "Unknown"),
                                brand_info.get("count", 0),
                                brand_info.get("percentage", 0.0)))

    df_user = pd.DataFrame(user_rows, columns=["state","year","quarter","registered_users","app_opens"])
    df_user.to_sql("aggregated_user", conn, if_exists="replace", index=False)
    print(f"✅ aggregated_user: {len(df_user):,} rows")

    df_dev = pd.DataFrame(device_rows, columns=["state","year","quarter","brand","device_count","percentage"])
    df_dev.to_sql("aggregated_user_device", conn, if_exists="replace", index=False)
    print(f"✅ aggregated_user_device: {len(df_dev):,} rows")

# ──────────────────────────────────────────────
# AGGREGATED INSURANCE
# ──────────────────────────────────────────────

def load_aggregated_insurance(conn):
    rows = []
    for year, quarter, fpath in iter_country_year_quarter(DATA_PATH / "aggregated" / "insurance"):
        data = load_json(fpath)
        if not data:
            continue
        for item in data.get("data", {}).get("transactionData", []):
            for pi in item.get("paymentInstruments", []):
                rows.append(("india", year, quarter, pi["count"], pi["amount"]))

    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "aggregated" / "insurance" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        for item in data.get("data", {}).get("transactionData", []):
            for pi in item.get("paymentInstruments", []):
                rows.append((state, year, quarter, pi["count"], pi["amount"]))

    df = pd.DataFrame(rows, columns=["state","year","quarter","transaction_count","transaction_amount"])
    df.to_sql("aggregated_insurance", conn, if_exists="replace", index=False)
    print(f"✅ aggregated_insurance: {len(df):,} rows")

# ──────────────────────────────────────────────
# MAP TRANSACTION
# ──────────────────────────────────────────────

def load_map_transaction(conn):
    rows = []
    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "map" / "transaction" / "hover" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        for item in data.get("data", {}).get("hoverDataList", []):
            for m in item.get("metric", []):
                rows.append((state, year, quarter, item["name"], m["count"], m["amount"]))

    df = pd.DataFrame(rows, columns=["state","year","quarter","district","transaction_count","transaction_amount"])
    df.to_sql("map_transaction", conn, if_exists="replace", index=False)
    print(f"✅ map_transaction: {len(df):,} rows")

# ──────────────────────────────────────────────
# MAP USER
# ──────────────────────────────────────────────

def load_map_user(conn):
    rows = []
    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "map" / "user" / "hover" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        hover = data.get("data", {}).get("hoverData", {})
        for district, vals in hover.items():
            rows.append((state, year, quarter, district,
                         vals.get("registeredUsers", 0),
                         vals.get("appOpens", 0)))

    df = pd.DataFrame(rows, columns=["state","year","quarter","district","registered_users","app_opens"])
    df.to_sql("map_user", conn, if_exists="replace", index=False)
    print(f"✅ map_user: {len(df):,} rows")

# ──────────────────────────────────────────────
# MAP INSURANCE
# ──────────────────────────────────────────────

def load_map_insurance(conn):
    rows = []
    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "map" / "insurance" / "hover" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        for item in data.get("data", {}).get("hoverDataList", []):
            for m in item.get("metric", []):
                rows.append((state, year, quarter, item["name"], m["count"], m["amount"]))

    df = pd.DataFrame(rows, columns=["state","year","quarter","district","transaction_count","transaction_amount"])
    df.to_sql("map_insurance", conn, if_exists="replace", index=False)
    print(f"✅ map_insurance: {len(df):,} rows")

# ──────────────────────────────────────────────
# TOP TRANSACTION
# ──────────────────────────────────────────────

def load_top_transaction(conn):
    rows = []
    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "top" / "transaction" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        for etype in ["districts", "pincodes"]:
            for item in data.get("data", {}).get(etype, []):
                rows.append((state, year, quarter, item["entityName"], etype,
                              item["metric"]["count"], item["metric"]["amount"]))

    for year, quarter, fpath in iter_country_year_quarter(DATA_PATH / "top" / "transaction"):
        data = load_json(fpath)
        if not data:
            continue
        for etype in ["states", "districts", "pincodes"]:
            for item in data.get("data", {}).get(etype, []):
                rows.append(("india", year, quarter, item["entityName"], etype,
                              item["metric"]["count"], item["metric"]["amount"]))

    df = pd.DataFrame(rows, columns=["state","year","quarter","entity_name","entity_type","transaction_count","transaction_amount"])
    df.to_sql("top_transaction", conn, if_exists="replace", index=False)
    print(f"✅ top_transaction: {len(df):,} rows")

# ──────────────────────────────────────────────
# TOP USER
# ──────────────────────────────────────────────

def load_top_user(conn):
    rows = []
    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "top" / "user" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        for etype in ["districts", "pincodes"]:
            for item in data.get("data", {}).get(etype, []):
                rows.append((state, year, quarter, item["name"], etype, item["registeredUsers"]))

    for year, quarter, fpath in iter_country_year_quarter(DATA_PATH / "top" / "user"):
        data = load_json(fpath)
        if not data:
            continue
        for etype in ["states", "districts", "pincodes"]:
            for item in data.get("data", {}).get(etype, []):
                rows.append(("india", year, quarter, item["name"], etype, item["registeredUsers"]))

    df = pd.DataFrame(rows, columns=["state","year","quarter","entity_name","entity_type","registered_users"])
    df.to_sql("top_user", conn, if_exists="replace", index=False)
    print(f"✅ top_user: {len(df):,} rows")

# ──────────────────────────────────────────────
# TOP INSURANCE
# ──────────────────────────────────────────────

def load_top_insurance(conn):
    rows = []
    for state, year, quarter, fpath in iter_state_year_quarter(DATA_PATH / "top" / "insurance" / "country" / "india"):
        data = load_json(fpath)
        if not data:
            continue
        for etype in ["districts", "pincodes"]:
            for item in data.get("data", {}).get(etype, []):
                rows.append((state, year, quarter, item["entityName"], etype,
                              item["metric"]["count"], item["metric"]["amount"]))

    for year, quarter, fpath in iter_country_year_quarter(DATA_PATH / "top" / "insurance"):
        data = load_json(fpath)
        if not data:
            continue
        for etype in ["states", "districts", "pincodes"]:
            for item in data.get("data", {}).get(etype, []):
                rows.append(("india", year, quarter, item["entityName"], etype,
                              item["metric"]["count"], item["metric"]["amount"]))

    df = pd.DataFrame(rows, columns=["state","year","quarter","entity_name","entity_type","transaction_count","transaction_amount"])
    df.to_sql("top_insurance", conn, if_exists="replace", index=False)
    print(f"✅ top_insurance: {len(df):,} rows")

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print(f"📂 Data path : {DATA_PATH}")
    print(f"💾 DB path   : {DB_PATH}")

    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Data path not found: {DATA_PATH}\n"
            "Clone the PhonePe Pulse repo first:\n"
            "  git clone https://github.com/PhonePe/pulse.git\n"
            "Or set the PHONEPE_DATA_PATH environment variable."
        )

    if DB_PATH.exists():
        DB_PATH.unlink()
        print("🗑  Removed old database.")

    conn = get_conn()
    create_tables(conn)

    print("\n📦 Loading data into SQLite...\n")
    load_aggregated_transaction(conn)
    load_aggregated_user(conn)          # also loads device brands
    load_aggregated_insurance(conn)
    load_map_transaction(conn)
    load_map_user(conn)
    load_map_insurance(conn)
    load_top_transaction(conn)
    load_top_user(conn)
    load_top_insurance(conn)

    print("\n🔍 Creating indexes...")
    create_indexes(conn)

    conn.close()
    print(f"\n🎉 ETL complete! Database saved to: {DB_PATH}")
