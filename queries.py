"""
PhonePe Pulse - SQL Analysis Queries
Covers all 10 business use cases + device brands.
Returns DataFrames from SQLite.
"""

import sqlite3
import pandas as pd

DB_PATH = r"C:\Users\AKSHAY SOM\Desktop\phonepe_project\phonepe.db"

def get_conn():
    return sqlite3.connect(DB_PATH)

# ──────────────────────────────────────────────
# 1. CUSTOMER SEGMENTATION
#    Top transaction types by volume & value
# ──────────────────────────────────────────────
def q1_customer_segmentation():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            transaction_type,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_transaction
        WHERE state != 'india'
        GROUP BY transaction_type
        ORDER BY total_count DESC
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 2. FRAUD DETECTION
#    States with anomalously high avg transaction value
# ──────────────────────────────────────────────
def q2_fraud_detection():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            state,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn,
            ROUND(SUM(transaction_amount)/NULLIF(SUM(transaction_count),0), 2) AS avg_txn_value
        FROM aggregated_transaction
        WHERE state != 'india'
        GROUP BY state
        ORDER BY avg_txn_value DESC
        LIMIT 15
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 3. GEOGRAPHICAL INSIGHTS
#    State-level transaction totals
# ──────────────────────────────────────────────
def q3_geographical_insights():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            state,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_transaction
        WHERE state != 'india'
        GROUP BY state
        ORDER BY total_amount_bn DESC
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 4. PAYMENT PERFORMANCE
#    Year-on-year growth per transaction type
# ──────────────────────────────────────────────
def q4_payment_performance():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            year,
            transaction_type,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_transaction
        WHERE state != 'india'
        GROUP BY year, transaction_type
        ORDER BY year, total_count DESC
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 5. USER ENGAGEMENT
#    Registered users and app opens by state
# ──────────────────────────────────────────────
def q5_user_engagement():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            state,
            SUM(registered_users) AS total_users,
            SUM(app_opens) AS total_app_opens,
            ROUND(SUM(app_opens)*1.0/NULLIF(SUM(registered_users),0), 2) AS opens_per_user
        FROM aggregated_user
        WHERE state != 'india'
        GROUP BY state
        ORDER BY total_users DESC
        LIMIT 20
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 6. PRODUCT DEVELOPMENT
#    Top 20 districts by transaction volume
# ──────────────────────────────────────────────
def q6_product_development():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            state,
            district,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM map_transaction
        GROUP BY state, district
        ORDER BY total_count DESC
        LIMIT 20
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 7. INSURANCE INSIGHTS
#    Top states for insurance transactions
# ──────────────────────────────────────────────
def q7_insurance_insights():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            state,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_insurance
        WHERE state != 'india'
        GROUP BY state
        ORDER BY total_amount_bn DESC
        LIMIT 15
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 8. MARKETING OPTIMIZATION
#    Quarterly breakdown by transaction type
# ──────────────────────────────────────────────
def q8_marketing_optimization():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            year,
            quarter,
            transaction_type,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_transaction
        WHERE state != 'india'
        GROUP BY year, quarter, transaction_type
        ORDER BY year, quarter
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# 9. TREND ANALYSIS
#    Total transactions per year-quarter
# ──────────────────────────────────────────────
def q9_trend_analysis():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            year,
            quarter,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_transaction
        WHERE state != 'india'
        GROUP BY year, quarter
        ORDER BY year, quarter
    """, conn)
    conn.close()
    df["period"] = df["year"].astype(str) + " Q" + df["quarter"].astype(str)
    return df

# ──────────────────────────────────────────────
# 10. COMPETITIVE BENCHMARKING
#     Top 10 states vs bottom 10 (transaction amount)
# ──────────────────────────────────────────────
def q10_competitive_benchmarking():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT
            state,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn,
            SUM(transaction_count) AS total_count
        FROM aggregated_transaction
        WHERE state != 'india'
        GROUP BY state
        ORDER BY total_amount_bn DESC
    """, conn)
    conn.close()
    top10 = df.head(10).copy(); top10["rank_group"] = "Top 10"
    bot10 = df.tail(10).copy(); bot10["rank_group"] = "Bottom 10"
    return pd.concat([top10, bot10])

# ──────────────────────────────────────────────
# DEVICE BRANDS (from DB — populated by ETL)
# ──────────────────────────────────────────────
def q_device_brands(top_n=10):
    """Top N device brands by total count across all states & quarters."""
    conn = get_conn()
    df = pd.read_sql(f"""
        SELECT
            brand,
            SUM(device_count) AS total_count,
            ROUND(AVG(percentage), 2) AS avg_pct
        FROM aggregated_user_device
        WHERE brand != 'Unknown'
        GROUP BY brand
        ORDER BY total_count DESC
        LIMIT {top_n}
    """, conn)
    conn.close()
    return df

# ──────────────────────────────────────────────
# DISTRICT-LEVEL MAP DATA
# ──────────────────────────────────────────────
def q_district_map(state_name: str = None):
    """District-level transaction data, optionally filtered by state."""
    conn = get_conn()
    where = f"WHERE state = '{state_name}'" if state_name else ""
    df = pd.read_sql(f"""
        SELECT
            state, district,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e6, 2) AS total_amount_mn
        FROM map_transaction
        {where}
        GROUP BY state, district
        ORDER BY total_amount_mn DESC
    """, conn)
    conn.close()
    return df


if __name__ == "__main__":
    print("=== Q1: Customer Segmentation ===")
    print(q1_customer_segmentation().to_string(index=False))

    print("\n=== Q2: Fraud Detection (top avg txn value) ===")
    print(q2_fraud_detection().to_string(index=False))

    print("\n=== Q3: Top States by Amount ===")
    print(q3_geographical_insights().head(10).to_string(index=False))

    print("\n=== Q9: Trend Analysis ===")
    print(q9_trend_analysis().to_string(index=False))

    print("\n=== Q10: Competitive Benchmarking ===")
    print(q10_competitive_benchmarking().to_string(index=False))

    print("\n=== Device Brands ===")
    print(q_device_brands().to_string(index=False))

    print("\n✅ All queries working.")
