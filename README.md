<h1 align="center">PhonePe Transaction Insights</h1>

<p align="center">
  <b>Exploratory Data Analysis + Interactive Streamlit Dashboard</b><br/>
  Analysing India's digital payment ecosystem using PhonePe Pulse open data (2018–2024)
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?logo=python" />
  <img src="https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit" />
  <img src="https://img.shields.io/badge/SQLite-Database-003B57?logo=sqlite" />
  <img src="https://img.shields.io/badge/Plotly-Visualisation-3F4F75?logo=plotly" />
  <img src="https://img.shields.io/badge/Domain-FinTech%20%2F%20Payments-purple" />
</p>

---

## 📌 Project Overview

This project performs a comprehensive **Exploratory Data Analysis (EDA)** on the [PhonePe Pulse](https://github.com/PhonePe/pulse) dataset — one of India's largest open digital payment datasets. The analysis covers **235 billion transactions** across all 36 Indian states and union territories from **2018 Q1 to 2024 Q4**, surfacing actionable insights for marketing, product, risk, and insurance teams.

A production-ready **Streamlit dashboard** with 10 business use cases is included, enabling real-time, interactive exploration of the data.

---

## 🗂️ Repository Structure

```
PhonePe-Transaction-Insights/
│
├── app.py                        # Streamlit dashboard (10 business use cases)
├── etl.py                        # ETL pipeline — clones Pulse repo & loads SQLite DB
├── queries.py                    # Reusable SQL query functions for all 10 use cases
├── PhonePe_EDA_Complete.ipynb    # Full EDA notebook (20 charts, UBM framework)
│
├── docs/
│   ├── PhonePe_Insights_Report.pdf   # Project insights document
│   └── PhonePe_Presentation.pptx    # Project presentation slides
│
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

---

## 🚀 Quick Start

### 1. Clone this repository
```bash
git clone https://github.com/akshaysom21/PhonePe-Transaction-Insights.git
cd PhonePe-Transaction-Insights
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the ETL pipeline (builds the SQLite database)
```bash
python etl.py
```
> This clones the PhonePe Pulse GitHub repository locally and loads all JSON data into `phonepe.db`.

### 4. Launch the Streamlit dashboard
```bash
streamlit run app.py
```

### 5. Run the EDA notebook
```bash
jupyter notebook PhonePe_EDA_Complete.ipynb
```

---

## 📦 Requirements

```
streamlit>=1.32.0
pandas>=2.0.0
plotly>=5.18.0
seaborn>=0.13.0
matplotlib>=3.8.0
numpy>=1.26.0
requests>=2.31.0
```

Install via:
```bash
pip install -r requirements.txt
```

> **Database path**: The app uses an environment variable `PHONEPE_DB_PATH` for the SQLite DB location. Set it if your `phonepe.db` is not in the project root:
> ```bash
> export PHONEPE_DB_PATH=/path/to/phonepe.db   # Linux/Mac
> set PHONEPE_DB_PATH=C:\path\to\phonepe.db    # Windows
> ```

---

## 📊 Dashboard — 10 Business Use Cases

| # | Section | Description |
|---|---------|-------------|
| 1 | 🏠 **Overview** | KPI summary — total transactions, value, states, registered users |
| 2 | 💳 **Transaction Analysis** | Customer segmentation by payment type (count + value) |
| 3 | 🚨 **Fraud Detection** | States with anomalously high average transaction values |
| 4 | 👥 **User Insights** | Registered users, app opens, engagement per state + device brands |
| 5 | 🗺️ **Geographical View** | Interactive India choropleth map + district-level breakdown |
| 6 | 📈 **Trend Analysis** | Quarterly growth, YoY state trends, payment performance |
| 7 | 📣 **Marketing Optimisation** | Seasonal heatmap, quarterly type breakdown for campaign planning |
| 8 | 🛡️ **Insurance Insights** | State-level insurance adoption + growth over time |
| 9 | 🏆 **Top Performers** | Top states, districts, and pincodes by transaction value |
| 10 | ⚖️ **Competitive Benchmarking** | Top 10 vs Bottom 10 state comparison — digital divide analysis |

---

## 📈 Key Findings (from EDA)

| Metric | Finding |
|--------|---------|
| **#1 by transaction volume** | Merchant payments — 130.24 Billion transactions |
| **#1 by transaction value** | Peer-to-peer — ₹266,527 Billion |
| **#1 state by value** | Telangana — ₹41,656 Billion |
| **#1 district** | Bengaluru Urban — 17,108 Million transactions |
| **Peak quarter** | 2024 Q4 — 28.2 Billion transactions |
| **Fastest growing segment** | Insurance — 46× growth (2020→2024) |
| **Top device brand** | Xiaomi — 25.1% market share (1,739.1M devices) |
| **Most engaged state** | Rajasthan — 87.3 app opens per registered user |
| **Fraud signal states** | Manipur (₹2,550), Nagaland (₹2,400), Mizoram (₹2,350) avg txn value |
| **Count-value correlation** | 0.99 — near-perfect linear relationship across all states |

---

## 🧠 EDA Structure (UBM Framework)

The notebook follows the **Univariate → Bivariate → Multivariate** analysis pattern with 20 charts:

**Univariate (Charts 1–4)**
- Chart 1: Transaction Volume by Payment Type
- Chart 2: Transaction Value by Payment Type
- Chart 3: Distribution of Registered Users Across States
- Chart 4: Top 10 Device Brands

**Bivariate (Charts 5–13)**
- Chart 5: Top 15 States by Transaction Amount
- Chart 6: Transaction Count vs Amount Scatter
- Chart 7: Quarterly Transaction Volume & Value Over Time
- Chart 8: Registered Users vs App Opens by State
- Chart 9: Year-on-Year Transaction Count by Payment Type
- Chart 10: Average Transaction Value Distribution (Box Plot)
- Chart 11: Top 15 States by Insurance Value
- Chart 12: Insurance Growth Over Time
- Chart 13: Top 10 vs Bottom 10 States

**Multivariate (Charts 14–15)**
- Chart 14: Correlation Heatmap
- Chart 15: Pair Plot

**Business Use Case Charts (Charts 16–20)**
- Chart 16: Fraud Signal — Avg Transaction Value by State
- Chart 17: Seasonal Heatmap — Year × Quarter
- Chart 18: Top 10 Districts by Transaction Count
- Chart 19: Device Brand Market Share (Pie Chart)
- Chart 20: User Engagement — Opens per User by State

---

## 🗄️ Database Schema

The ETL pipeline populates a SQLite database with 10 tables:

| Table | Description |
|-------|-------------|
| `aggregated_transaction` | State-level transactions by type, year, quarter |
| `aggregated_user` | State-level registered users and app opens |
| `aggregated_user_device` | Device brand usage by state |
| `aggregated_insurance` | State-level insurance transactions |
| `map_transaction` | District-level transaction data |
| `map_user` | District-level user data |
| `map_insurance` | District-level insurance data |
| `top_transaction` | Top states, districts, pincodes by transactions |
| `top_user` | Top states, districts, pincodes by users |
| `top_insurance` | Top states, districts, pincodes by insurance |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Language** | Python 3.10+ |
| **Dashboard** | Streamlit + Plotly |
| **Database** | SQLite (via sqlite3) |
| **EDA** | Pandas, Matplotlib, Seaborn |
| **ETL** | Git clone + JSON parsing + SQLite ingestion |
| **Geospatial** | Plotly Choropleth + GeoJSON |

---

## 📁 Data Source

**PhonePe Pulse** — Official open-source dataset by PhonePe  
🔗 https://github.com/PhonePe/pulse

The dataset is publicly available and updated regularly. It covers all Indian states and union territories from 2018 onwards.

---

## 👤 Author

**Akshay Som**  
📧 GitHub: [@akshaysom21](https://github.com/akshaysom21)  
🔗 Project Repository: [PhonePe-Transaction-Insights](https://github.com/akshaysom21/PhonePe-Transaction-Insights)

---

## 📄 License

This project is licensed under the MIT License.  
PhonePe Pulse data is licensed under the [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/).

---

<p align="center">Made with Python, Streamlit, and the PhonePe Pulse open dataset</p>

