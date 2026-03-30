"""
PhonePe Pulse - Streamlit Dashboard
All 10 business use-cases visualised.

Run with:  streamlit run app.py
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import requests
from pathlib import Path

# DB path — configurable via environment variable so it works on any machine
DB_PATH = Path(os.environ.get("PHONEPE_DB_PATH", Path(__file__).resolve().parent / "phonepe.db"))

# ─────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PhonePe Pulse Insights",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0f0f1a; }
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #5a189a;
        border-radius: 12px;
        padding: 16px 20px;
        text-align: center;
        margin-bottom: 8px;
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #a855f7; }
    .metric-label { font-size: 0.85rem; color: #94a3b8; margin-top: 4px; }
    .section-title { color: #a855f7; font-size: 1.3rem; font-weight: 600; margin: 20px 0 10px; }
    [data-testid="stSidebar"] { background-color: #1a1a2e; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────
# DB HELPER
# ─────────────────────────────────────────────────────────
@st.cache_data
def run_query(sql):
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

@st.cache_data
def load_india_geojson():
    """Load India state-level GeoJSON from a public CDN (cached)."""
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    try:
        r = requests.get(url, timeout=10)
        return r.json()
    except Exception:
        return None

PHONEPE_COLORS = ["#5a189a", "#7b2ff7", "#a855f7", "#c084fc", "#e9d5ff",
                  "#3b82f6", "#06b6d4", "#10b981", "#f59e0b", "#ef4444"]

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color="white"
)

def csv_download(df, filename):
    st.download_button(
        label="⬇️ Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )

# ─────────────────────────────────────────────────────────
# STATE NAME NORMALISER
# Maps DB slug names to GeoJSON feature names
# ─────────────────────────────────────────────────────────
STATE_NAME_MAP = {
    "andaman-&-nicobar-islands": "Andaman & Nicobar Island",
    "andhra-pradesh": "Andhra Pradesh",
    "arunachal-pradesh": "Arunachal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra-&-nagar-haveli-&-daman-&-diu": "Dadara & Nagar Havelli",
    "delhi": "NCT of Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal-pradesh": "Himachal Pradesh",
    "jammu-&-kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Ladakh",
    "lakshadweep": "Lakshadweep",
    "madhya-pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamil-nadu": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "uttar-pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west-bengal": "West Bengal",
}

def normalise_state(df, col="state"):
    df = df.copy()
    df["state_geo"] = df[col].map(STATE_NAME_MAP).fillna(
        df[col].str.replace("-", " ").str.title()
    )
    return df

# ─────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5f/PhonePe_Logo.svg/200px-PhonePe_Logo.svg.png",
        width=140
    )
    st.markdown("## 📊 PhonePe Pulse")
    st.markdown("---")
    section = st.radio("Navigate to", [
        "🏠 Overview",
        "💳 Transaction Analysis",
        "🚨 Fraud Detection",
        "👥 User Insights",
        "🗺️ Geographical View",
        "📈 Trend Analysis",
        "📣 Marketing Optimisation",
        "🛡️ Insurance Insights",
        "🏆 Top Performers",
        "⚖️ Competitive Benchmarking",
    ])
    st.markdown("---")

    all_years = run_query(
        "SELECT DISTINCT year FROM aggregated_transaction ORDER BY year"
    )["year"].tolist()
    year_filter = st.multiselect("Filter by Year", all_years, default=all_years[-3:])
    if not year_filter:
        year_filter = all_years
    year_str = ",".join(str(y) for y in year_filter)

    quarter_filter = st.multiselect("Filter by Quarter", [1, 2, 3, 4], default=[1, 2, 3, 4])
    if not quarter_filter:
        quarter_filter = [1, 2, 3, 4]
    quarter_str = ",".join(str(q) for q in quarter_filter)

# ─────────────────────────────────────────────────────────
# PAGE: OVERVIEW
# ─────────────────────────────────────────────────────────
if section == "🏠 Overview":
    st.title("📱 PhonePe Pulse — Transaction Insights")
    st.markdown("A comprehensive analysis of India's digital payment ecosystem powered by PhonePe data.")

    kpi = run_query(f"""
        SELECT
            SUM(transaction_count) AS total_txns,
            ROUND(SUM(transaction_amount)/1e12, 2) AS total_amount_tr,
            COUNT(DISTINCT state) AS states_covered
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
    """).iloc[0]

    users_kpi = run_query(f"""
        SELECT SUM(registered_users) AS total_users
        FROM aggregated_user
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
    """).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    for col, val, label in [
        (c1, f"{kpi['total_txns']/1e9:.1f}B", "Total Transactions"),
        (c2, f"₹{kpi['total_amount_tr']:.1f}T", "Total Amount Processed"),
        (c3, str(int(kpi['states_covered'])), "States & UTs Covered"),
        (c4, f"{users_kpi['total_users']/1e6:.0f}M", "Registered Users"),
    ]:
        with col:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value">{val}</div>
                <div class="metric-label">{label}</div></div>""", unsafe_allow_html=True)

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<p class="section-title">Transaction Type Distribution</p>', unsafe_allow_html=True)
        df = run_query(f"""
            SELECT transaction_type, SUM(transaction_count) AS count
            FROM aggregated_transaction
            WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY transaction_type ORDER BY count DESC
        """)
        fig = px.pie(df, names="transaction_type", values="count",
                     color_discrete_sequence=PHONEPE_COLORS, hole=0.45)
        fig.update_layout(**CHART_LAYOUT, legend_font_size=11)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-title">Top 10 States by Transaction Amount</p>', unsafe_allow_html=True)
        df = run_query(f"""
            SELECT state, ROUND(SUM(transaction_amount)/1e9,1) AS amount_bn
            FROM aggregated_transaction
            WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY state ORDER BY amount_bn DESC LIMIT 10
        """)
        df["state"] = df["state"].str.replace("-", " ").str.title()
        fig = px.bar(df, x="amount_bn", y="state", orientation="h",
                     color="amount_bn", color_continuous_scale="Purples",
                     labels={"amount_bn": "Amount (₹ Billion)", "state": ""})
        fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────
# PAGE: TRANSACTION ANALYSIS
# ─────────────────────────────────────────────────────────
elif section == "💳 Transaction Analysis":
    st.title("💳 Transaction Analysis")

    df_seg = run_query(f"""
        SELECT transaction_type,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY transaction_type ORDER BY total_count DESC
    """)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Transaction Count by Type")
        fig = px.bar(df_seg, x="transaction_type", y="total_count",
                     color="transaction_type", color_discrete_sequence=PHONEPE_COLORS,
                     labels={"total_count": "Count", "transaction_type": "Type"})
        fig.update_layout(**CHART_LAYOUT, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Transaction Amount by Type (₹ Billions)")
        fig = px.bar(df_seg, x="transaction_type", y="total_amount_bn",
                     color="transaction_type", color_discrete_sequence=PHONEPE_COLORS,
                     labels={"total_amount_bn": "Amount (Bn)", "transaction_type": "Type"})
        fig.update_layout(**CHART_LAYOUT, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📊 Quarterly Performance by Transaction Type")
    df_qtr = run_query(f"""
        SELECT year, quarter, transaction_type,
            SUM(transaction_count) AS total_count
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY year, quarter, transaction_type ORDER BY year, quarter
    """)
    df_qtr["period"] = df_qtr["year"].astype(str) + " Q" + df_qtr["quarter"].astype(str)
    fig = px.line(df_qtr, x="period", y="total_count", color="transaction_type",
                  color_discrete_sequence=PHONEPE_COLORS,
                  labels={"total_count": "Transaction Count", "period": "Quarter"})
    fig.update_layout(**CHART_LAYOUT, xaxis=dict(tickangle=45))
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📋 Raw Data")
    csv_download(df_seg, "transaction_type_summary.csv")
    st.dataframe(df_seg, use_container_width=True)

# ─────────────────────────────────────────────────────────
# PAGE: FRAUD DETECTION
# ─────────────────────────────────────────────────────────
elif section == "🚨 Fraud Detection":
    st.title("🚨 Fraud Detection Signals")
    st.markdown(
        "States with anomalously high average transaction values may indicate "
        "unusual payment patterns that warrant further investigation."
    )

    df_fraud = run_query(f"""
        SELECT
            state,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn,
            ROUND(SUM(transaction_amount)/NULLIF(SUM(transaction_count),0), 2) AS avg_txn_value
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY state
        ORDER BY avg_txn_value DESC
    """)
    df_fraud["state"] = df_fraud["state"].str.replace("-", " ").str.title()

    # KPI row
    c1, c2, c3 = st.columns(3)
    overall_avg = (df_fraud["total_amount_bn"].sum() * 1e9 / df_fraud["total_count"].sum())
    highest_state = df_fraud.iloc[0]
    with c1:
        st.metric("National Avg Txn Value (₹)", f"{overall_avg:,.0f}")
    with c2:
        st.metric("Highest Avg State", highest_state["state"], f"₹{highest_state['avg_txn_value']:,.0f}")
    with c3:
        outliers = df_fraud[df_fraud["avg_txn_value"] > overall_avg * 1.5].shape[0]
        st.metric("States with >1.5× Avg", str(outliers), delta="flag for review", delta_color="inverse")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Avg Transaction Value by State")
        df_top15 = df_fraud.head(15)
        fig = px.bar(df_top15, x="avg_txn_value", y="state", orientation="h",
                     color="avg_txn_value", color_continuous_scale="Reds",
                     labels={"avg_txn_value": "Avg Txn Value (₹)", "state": ""})
        fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        # Add benchmark line
        fig.add_vline(x=overall_avg, line_dash="dash", line_color="#f59e0b",
                      annotation_text="National avg", annotation_position="top right")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Volume vs Avg Value (Anomaly Bubble Chart)")
        fig = px.scatter(
            df_fraud,
            x="total_count", y="avg_txn_value", text="state",
            size="avg_txn_value", color="avg_txn_value",
            color_continuous_scale="OrRd",
            labels={"avg_txn_value": "Avg Txn Value (₹)", "total_count": "Total Transactions"},
        )
        fig.update_traces(textposition="top center", textfont=dict(size=8))
        fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False)
        # Horizontal line at national avg
        fig.add_hline(y=overall_avg, line_dash="dash", line_color="#f59e0b",
                      annotation_text="National avg")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📋 Full State Fraud Signals Table")
    df_display = df_fraud.copy()
    df_display["flag"] = df_display["avg_txn_value"].apply(
        lambda x: "🔴 High" if x > overall_avg * 1.5 else ("🟡 Watch" if x > overall_avg * 1.2 else "🟢 Normal")
    )
    csv_download(df_display, "fraud_signals.csv")
    st.dataframe(df_display.reset_index(drop=True), use_container_width=True)

# ─────────────────────────────────────────────────────────
# PAGE: USER INSIGHTS
# ─────────────────────────────────────────────────────────
elif section == "👥 User Insights":
    st.title("👥 User Insights")

    df_users = run_query(f"""
        SELECT state, SUM(registered_users) AS total_users, SUM(app_opens) AS total_opens
        FROM aggregated_user
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY state ORDER BY total_users DESC LIMIT 20
    """)
    df_users["state"] = df_users["state"].str.replace("-", " ").str.title()
    df_users["opens_per_user"] = (df_users["total_opens"] / df_users["total_users"]).round(1)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 States — Registered Users")
        fig = px.bar(df_users, x="total_users", y="state", orientation="h",
                     color="total_users", color_continuous_scale="Purples",
                     labels={"total_users": "Registered Users", "state": ""})
        fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("App Opens vs Registered Users")
        fig = px.scatter(df_users, x="total_users", y="total_opens", text="state",
                         size="total_users", color="opens_per_user",
                         color_continuous_scale="Viridis",
                         labels={"total_users": "Registered Users", "total_opens": "App Opens"})
        fig.update_traces(textposition="top center", textfont=dict(size=8))
        fig.update_layout(**CHART_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("📱 User Growth Over Time")
        df_growth = run_query(f"""
            SELECT year, quarter, SUM(registered_users) AS total_users
            FROM aggregated_user
            WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY year, quarter ORDER BY year, quarter
        """)
        df_growth["period"] = df_growth["year"].astype(str) + " Q" + df_growth["quarter"].astype(str)
        fig = px.area(df_growth, x="period", y="total_users",
                      color_discrete_sequence=["#a855f7"],
                      labels={"total_users": "Registered Users", "period": "Quarter"})
        fig.update_layout(**CHART_LAYOUT, xaxis=dict(tickangle=45))
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("📱 Top Device Brands")
        df_devices = run_query("""
            SELECT brand, SUM(device_count) AS total_count
            FROM aggregated_user_device
            WHERE brand != 'Unknown'
            GROUP BY brand ORDER BY total_count DESC LIMIT 10
        """)
        if df_devices.empty:
            st.info("Device brand data not available — re-run ETL to populate aggregated_user_device table.")
        else:
            fig = px.pie(df_devices, names="brand", values="total_count",
                         color_discrete_sequence=PHONEPE_COLORS, hole=0.4)
            fig.update_layout(**CHART_LAYOUT, legend_font_size=10)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📋 State User Data")
    csv_download(df_users, "user_engagement.csv")
    st.dataframe(df_users.reset_index(drop=True), use_container_width=True)

# ─────────────────────────────────────────────────────────
# PAGE: GEOGRAPHICAL VIEW  (with choropleth map)
# ─────────────────────────────────────────────────────────
elif section == "🗺️ Geographical View":
    st.title("🗺️ Geographical View")

    view_type = st.radio(
        "Metric",
        ["Transaction Amount", "Transaction Count", "Registered Users"],
        horizontal=True
    )

    df_state = run_query(f"""
        SELECT state,
            ROUND(SUM(transaction_amount)/1e9,2) AS amount_bn,
            SUM(transaction_count) AS total_count
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY state ORDER BY amount_bn DESC
    """)
    df_users_state = run_query(f"""
        SELECT state, SUM(registered_users) AS total_users
        FROM aggregated_user
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY state ORDER BY total_users DESC
    """)
    df_state = df_state.merge(df_users_state, on="state", how="left")
    df_state = normalise_state(df_state)

    col_map = {
        "Transaction Amount": ("amount_bn", "Amount (₹ Bn)", "Purples"),
        "Transaction Count":  ("total_count", "Transaction Count", "Blues"),
        "Registered Users":   ("total_users", "Registered Users", "Greens"),
    }
    col_val, label, color_scale = col_map[view_type]

    # ── Choropleth map ──
    geojson = load_india_geojson()
    if geojson:
        fig_map = px.choropleth(
            df_state,
            geojson=geojson,
            featureidkey="properties.ST_NM",
            locations="state_geo",
            color=col_val,
            color_continuous_scale=color_scale,
            labels={col_val: label},
            title=f"India — {view_type} by State",
        )
        fig_map.update_geos(fitbounds="locations", visible=False)
        fig_map.update_layout(
            **CHART_LAYOUT,
            margin=dict(l=0, r=0, t=40, b=0),
            height=520,
            coloraxis_colorbar=dict(title=label, thickness=12)
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("⚠️ Could not load GeoJSON (check internet connection). Showing treemap instead.")
        df_state["state_display"] = df_state["state"].str.replace("-", " ").str.title()
        fig = px.treemap(df_state, path=["state_display"], values=col_val,
                         color=col_val, color_continuous_scale=color_scale,
                         labels={col_val: label})
        fig.update_layout(**CHART_LAYOUT, height=500)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Districts by Transaction Amount")
        df_dist = run_query(f"""
            SELECT state, district,
                SUM(transaction_count) AS total_count,
                ROUND(SUM(transaction_amount)/1e9,2) AS amount_bn
            FROM map_transaction
            WHERE year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY state, district ORDER BY amount_bn DESC LIMIT 10
        """)
        df_dist["state"] = df_dist["state"].str.replace("-", " ").str.title()
        df_dist["district"] = df_dist["district"].str.title()
        fig = px.bar(df_dist, x="amount_bn", y="district", color="state",
                     orientation="h", color_discrete_sequence=PHONEPE_COLORS,
                     labels={"amount_bn": "Amount (₹ Bn)", "district": ""})
        fig.update_layout(**CHART_LAYOUT, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("State-wise Summary Table")
        df_display = df_state[["state_geo", "amount_bn", "total_count", "total_users"]].copy()
        df_display.columns = ["State", "Amount (₹ Bn)", "Transactions", "Registered Users"]
        df_display = df_display.sort_values("Amount (₹ Bn)", ascending=False).reset_index(drop=True)
        csv_download(df_display, "state_summary.csv")
        st.dataframe(df_display, use_container_width=True, height=380)

# ─────────────────────────────────────────────────────────
# PAGE: TREND ANALYSIS
# ─────────────────────────────────────────────────────────
elif section == "📈 Trend Analysis":
    st.title("📈 Trend Analysis")

    df_trend = run_query(f"""
        SELECT year, quarter,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY year, quarter ORDER BY year, quarter
    """)
    df_trend["period"] = df_trend["year"].astype(str) + " Q" + df_trend["quarter"].astype(str)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Transaction Count Growth")
        fig = px.line(df_trend, x="period", y="total_count",
                      markers=True, color_discrete_sequence=["#a855f7"],
                      labels={"total_count": "Transactions", "period": "Quarter"})
        fig.update_layout(**CHART_LAYOUT, xaxis=dict(tickangle=45))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Transaction Amount Growth (₹ Billions)")
        fig = px.area(df_trend, x="period", y="total_amount_bn",
                      color_discrete_sequence=["#7b2ff7"],
                      labels={"total_amount_bn": "Amount (Bn)", "period": "Quarter"})
        fig.update_layout(**CHART_LAYOUT, xaxis=dict(tickangle=45))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📊 Year-on-Year Growth by State (Top 10)")
    df_yoy = run_query("""
        SELECT year, state, ROUND(SUM(transaction_amount)/1e9,1) AS amount_bn
        FROM aggregated_transaction WHERE state != 'india'
        GROUP BY year, state ORDER BY year, amount_bn DESC
    """)
    top_states = df_yoy.groupby("state")["amount_bn"].sum().nlargest(10).index.tolist()
    df_yoy_top = df_yoy[df_yoy["state"].isin(top_states)].copy()
    df_yoy_top["state"] = df_yoy_top["state"].str.replace("-", " ").str.title()
    fig = px.line(df_yoy_top, x="year", y="amount_bn", color="state",
                  color_discrete_sequence=PHONEPE_COLORS, markers=True,
                  labels={"amount_bn": "Amount (₹ Bn)", "year": "Year"})
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📋 Quarterly Data")
    csv_download(df_trend, "trend_analysis.csv")
    st.dataframe(df_trend, use_container_width=True)

# ─────────────────────────────────────────────────────────
# PAGE: MARKETING OPTIMISATION  (Use Case 8)
# ─────────────────────────────────────────────────────────
elif section == "📣 Marketing Optimisation":
    st.title("📣 Marketing Optimisation")
    st.markdown(
        "Understand *when* and *where* to focus marketing spend "
        "by analysing quarterly seasonality and transaction-type momentum."
    )

    # ── Quarterly seasonality heatmap ──
    st.subheader("Seasonal Heatmap — Transaction Count by Year & Quarter")
    df_heat = run_query(f"""
        SELECT year, quarter, SUM(transaction_count) AS total_count
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY year, quarter ORDER BY year, quarter
    """)
    pivot = df_heat.pivot(index="year", columns="quarter", values="total_count").fillna(0)
    pivot.columns = [f"Q{c}" for c in pivot.columns]
    fig_heat = px.imshow(
        pivot,
        text_auto=".2s",
        color_continuous_scale="Purples",
        labels={"color": "Transactions"},
        title="Transactions by Year & Quarter (darker = higher volume)"
    )
    fig_heat.update_layout(**CHART_LAYOUT, height=350)
    st.plotly_chart(fig_heat, use_container_width=True)

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Quarterly Breakdown by Transaction Type")
        df_qtype = run_query(f"""
            SELECT year, quarter, transaction_type,
                SUM(transaction_count) AS total_count,
                ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn
            FROM aggregated_transaction
            WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY year, quarter, transaction_type
            ORDER BY year, quarter
        """)
        df_qtype["period"] = df_qtype["year"].astype(str) + " Q" + df_qtype["quarter"].astype(str)
        fig = px.bar(
            df_qtype, x="period", y="total_count", color="transaction_type",
            color_discrete_sequence=PHONEPE_COLORS, barmode="stack",
            labels={"total_count": "Transaction Count", "period": "Quarter",
                    "transaction_type": "Type"}
        )
        fig.update_layout(**CHART_LAYOUT, xaxis=dict(tickangle=45))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Average Quarter-wise Transaction Amount (₹ Bn)")
        df_qavg = run_query(f"""
            SELECT quarter,
                ROUND(AVG(quarterly_amount), 2) AS avg_amount_bn
            FROM (
                SELECT year, quarter,
                    ROUND(SUM(transaction_amount)/1e9, 4) AS quarterly_amount
                FROM aggregated_transaction
                WHERE state != 'india' AND year IN ({year_str})
                GROUP BY year, quarter
            )
            GROUP BY quarter ORDER BY quarter
        """)
        df_qavg["quarter_label"] = df_qavg["quarter"].apply(lambda q: f"Q{q}")
        fig = px.bar(
            df_qavg, x="quarter_label", y="avg_amount_bn",
            color="avg_amount_bn", color_continuous_scale="Purples",
            labels={"avg_amount_bn": "Avg Amount (₹ Bn)", "quarter_label": "Quarter"},
            text="avg_amount_bn"
        )
        fig.update_traces(texttemplate="₹%{text}Bn", textposition="outside")
        fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📋 Quarterly Marketing Data Export")
    csv_download(df_qtype, "marketing_optimisation.csv")
    st.dataframe(df_qtype.drop(columns=["year","quarter"]).reset_index(drop=True),
                 use_container_width=True)

# ─────────────────────────────────────────────────────────
# PAGE: INSURANCE INSIGHTS
# ─────────────────────────────────────────────────────────
elif section == "🛡️ Insurance Insights":
    st.title("🛡️ Insurance Insights")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top States — Insurance Transactions")
        df_ins = run_query(f"""
            SELECT state, SUM(transaction_count) AS count,
                ROUND(SUM(transaction_amount)/1e9, 2) AS amount_bn
            FROM aggregated_insurance
            WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY state ORDER BY amount_bn DESC LIMIT 15
        """)
        df_ins["state"] = df_ins["state"].str.replace("-", " ").str.title()
        fig = px.bar(df_ins, x="amount_bn", y="state", orientation="h",
                     color="amount_bn", color_continuous_scale="Teal",
                     labels={"amount_bn": "Amount (₹ Bn)", "state": ""})
        fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Insurance Growth Over Time")
        df_ins_trend = run_query(f"""
            SELECT year, quarter,
                SUM(transaction_count) AS count,
                ROUND(SUM(transaction_amount)/1e9, 2) AS amount_bn
            FROM aggregated_insurance
            WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY year, quarter ORDER BY year, quarter
        """)
        df_ins_trend["period"] = df_ins_trend["year"].astype(str) + " Q" + df_ins_trend["quarter"].astype(str)
        fig = px.line(df_ins_trend, x="period", y="amount_bn", markers=True,
                      color_discrete_sequence=["#06b6d4"],
                      labels={"amount_bn": "Amount (₹ Bn)", "period": "Quarter"})
        fig.update_layout(**CHART_LAYOUT, xaxis=dict(tickangle=45))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Top Districts for Insurance")
    df_ins_dist = run_query(f"""
        SELECT district, state, SUM(transaction_count) AS count,
            ROUND(SUM(transaction_amount)/1e6, 2) AS amount_mn
        FROM map_insurance
        WHERE year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY district, state ORDER BY amount_mn DESC LIMIT 20
    """)
    df_ins_dist["state"] = df_ins_dist["state"].str.replace("-", " ").str.title()
    df_ins_dist["district"] = df_ins_dist["district"].str.title()
    csv_download(df_ins_dist, "insurance_districts.csv")
    st.dataframe(df_ins_dist.reset_index(drop=True), use_container_width=True)

# ─────────────────────────────────────────────────────────
# PAGE: TOP PERFORMERS
# ─────────────────────────────────────────────────────────
elif section == "🏆 Top Performers":
    st.title("🏆 Top Performers")

    tab1, tab2, tab3 = st.tabs(["🏙️ States", "🏘️ Districts", "📮 Pincodes"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Top States — Transactions")
            df = run_query(f"""
                SELECT entity_name AS state,
                    SUM(transaction_count) AS count,
                    ROUND(SUM(transaction_amount)/1e9,2) AS amount_bn
                FROM top_transaction
                WHERE entity_type='states' AND year IN ({year_str}) AND quarter IN ({quarter_str})
                GROUP BY entity_name ORDER BY amount_bn DESC LIMIT 10
            """)
            df["state"] = df["state"].str.replace("-"," ").str.title()
            fig = px.bar(df, x="amount_bn", y="state", orientation="h",
                         color="amount_bn", color_continuous_scale="Purples",
                         labels={"amount_bn":"Amount (₹ Bn)","state":""})
            fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Top States — Registered Users")
            df = run_query(f"""
                SELECT entity_name AS state, SUM(registered_users) AS users
                FROM top_user
                WHERE entity_type='states' AND year IN ({year_str}) AND quarter IN ({quarter_str})
                GROUP BY entity_name ORDER BY users DESC LIMIT 10
            """)
            df["state"] = df["state"].str.replace("-"," ").str.title()
            fig = px.bar(df, x="users", y="state", orientation="h",
                         color="users", color_continuous_scale="Blues",
                         labels={"users":"Registered Users","state":""})
            fig.update_layout(**CHART_LAYOUT, coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Top 20 Districts by Transaction Amount")
        df = run_query(f"""
            SELECT entity_name AS district, state,
                SUM(transaction_count) AS count,
                ROUND(SUM(transaction_amount)/1e9,2) AS amount_bn
            FROM top_transaction
            WHERE entity_type='districts' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY entity_name, state ORDER BY amount_bn DESC LIMIT 20
        """)
        df["state"] = df["state"].str.replace("-"," ").str.title()
        df["district"] = df["district"].str.title()
        fig = px.bar(df, x="amount_bn", y="district", color="state",
                     orientation="h", color_discrete_sequence=PHONEPE_COLORS,
                     labels={"amount_bn":"Amount (₹ Bn)","district":""})
        fig.update_layout(**CHART_LAYOUT, yaxis=dict(autorange="reversed"), height=550)
        st.plotly_chart(fig, use_container_width=True)
        csv_download(df, "top_districts.csv")

    with tab3:
        st.subheader("Top 20 Pincodes by Transaction Amount")
        df = run_query(f"""
            SELECT entity_name AS pincode, state,
                SUM(transaction_count) AS count,
                ROUND(SUM(transaction_amount)/1e6,2) AS amount_mn
            FROM top_transaction
            WHERE entity_type='pincodes' AND year IN ({year_str}) AND quarter IN ({quarter_str})
            GROUP BY entity_name, state ORDER BY amount_mn DESC LIMIT 20
        """)
        df["state"] = df["state"].str.replace("-"," ").str.title()
        fig = px.bar(df, x="amount_mn", y="pincode", color="state",
                     orientation="h", color_discrete_sequence=PHONEPE_COLORS,
                     labels={"amount_mn":"Amount (₹ Mn)","pincode":""})
        fig.update_layout(**CHART_LAYOUT, yaxis=dict(autorange="reversed"), height=550)
        st.plotly_chart(fig, use_container_width=True)
        csv_download(df, "top_pincodes.csv")

# ─────────────────────────────────────────────────────────
# PAGE: COMPETITIVE BENCHMARKING
# ─────────────────────────────────────────────────────────
elif section == "⚖️ Competitive Benchmarking":
    st.title("⚖️ Competitive Benchmarking")
    st.markdown("Comparing the top-performing states against the bottom-performing ones to identify the digital payment divide.")

    df_all = run_query(f"""
        SELECT
            state,
            ROUND(SUM(transaction_amount)/1e9, 2) AS total_amount_bn,
            SUM(transaction_count) AS total_count,
            ROUND(SUM(transaction_amount)/NULLIF(SUM(transaction_count),0), 2) AS avg_txn_value
        FROM aggregated_transaction
        WHERE state != 'india' AND year IN ({year_str}) AND quarter IN ({quarter_str})
        GROUP BY state
        ORDER BY total_amount_bn DESC
    """)
    df_all["state"] = df_all["state"].str.replace("-", " ").str.title()

    top10 = df_all.head(10).copy(); top10["rank_group"] = "Top 10"
    bot10 = df_all.tail(10).copy(); bot10["rank_group"] = "Bottom 10"
    df_bench = pd.concat([top10, bot10])

    # KPIs
    c1, c2, c3 = st.columns(3)
    with c1:
        ratio = top10["total_amount_bn"].sum() / max(bot10["total_amount_bn"].sum(), 0.01)
        st.metric("Top 10 / Bottom 10 Amount Ratio", f"{ratio:.0f}×")
    with c2:
        st.metric("Top State", top10.iloc[0]["state"], f"₹{top10.iloc[0]['total_amount_bn']:,} Bn")
    with c3:
        st.metric("Bottom State", bot10.iloc[-1]["state"], f"₹{bot10.iloc[-1]['total_amount_bn']:,} Bn")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Transaction Amount — Top 10 vs Bottom 10")
        color_map = {"Top 10": "#a855f7", "Bottom 10": "#ef4444"}
        fig = px.bar(
            df_bench, x="total_amount_bn", y="state", color="rank_group",
            orientation="h",
            color_discrete_map=color_map,
            labels={"total_amount_bn": "Amount (₹ Bn)", "state": "", "rank_group": "Group"},
        )
        fig.update_layout(**CHART_LAYOUT, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Transaction Count — Top 10 vs Bottom 10")
        fig = px.bar(
            df_bench, x="total_count", y="state", color="rank_group",
            orientation="h",
            color_discrete_map=color_map,
            labels={"total_count": "Transaction Count", "state": "", "rank_group": "Group"},
        )
        fig.update_layout(**CHART_LAYOUT, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Average Transaction Value Comparison")
    fig = px.scatter(
        df_bench,
        x="total_count", y="avg_txn_value",
        color="rank_group", size="total_amount_bn",
        text="state",
        color_discrete_map=color_map,
        labels={
            "avg_txn_value": "Avg Txn Value (₹)",
            "total_count": "Total Transactions",
            "rank_group": "Group"
        },
    )
    fig.update_traces(textposition="top center", textfont=dict(size=9))
    fig.update_layout(**CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📋 Full Benchmarking Data")
    csv_download(df_bench, "competitive_benchmarking.csv")
    st.dataframe(df_bench.reset_index(drop=True), use_container_width=True)
