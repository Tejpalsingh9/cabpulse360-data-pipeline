import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

st.set_page_config(page_title="CabPulse 360 - Team2", layout="wide")

# =========================
# CUSTOM CSS
# =========================
st.markdown("""
<style>
    .main { background-color: #0f1117; }
    .block-container { padding-top: 1.5rem; }

    .dashboard-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #FFD700;
        text-align: center;
        margin-bottom: 0.2rem;
    }
    .dashboard-subtitle {
        text-align: center;
        color: #aaaaaa;
        font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }

    .kpi-box {
        background: linear-gradient(135deg, #1e2130, #2a2f45);
        border: 1px solid #3a3f55;
        border-radius: 12px;
        padding: 1.2rem 1rem;
        text-align: center;
        margin-bottom: 1rem;
    }
    .kpi-label {
        font-size: 0.78rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 0.4rem;
    }
    .kpi-value {
        font-size: 1.9rem;
        font-weight: 700;
        color: #FFD700;
    }
    .kpi-value.green  { color: #00e096; }
    .kpi-value.blue   { color: #60b4ff; }
    .kpi-value.orange { color: #ff9f43; }

    .section-header {
        font-size: 1.15rem;
        font-weight: 600;
        color: #FFD700;
        border-left: 4px solid #FFD700;
        padding-left: 0.6rem;
        margin: 1.5rem 0 0.8rem;
    }

    .dataframe thead th {
        background-color: #1e2130 !important;
        color: #FFD700 !important;
        font-size: 0.8rem;
        text-transform: uppercase;
    }
    .dataframe tbody tr:nth-child(even) { background-color: #1a1d2e; }
    .dataframe tbody tr:hover { background-color: #2a2f45; }

    div[data-testid="stMetric"] {
        background: #1e2130;
        border: 1px solid #3a3f55;
        border-radius: 10px;
        padding: 0.8rem 1rem;
    }
</style>
""", unsafe_allow_html=True)

# =========================
# HEADER
# =========================
st.markdown('<div class="dashboard-title">CabPulse 360 — NYC Yellow Taxi Analytics</div>', unsafe_allow_html=True)
st.markdown('<div class="dashboard-subtitle">Team2 &nbsp;|&nbsp; Batch 01 &nbsp;|&nbsp; January 2019 &nbsp;|&nbsp; New York City</div>', unsafe_allow_html=True)

# =========================
# LOAD KPI DATA
# =========================
df_kpi = session.sql("""
    SELECT COUNT(*)                     AS TOTAL_TRIPS,
           SUM(PASSENGER_COUNT)         AS TOTAL_PASSENGERS,
           ROUND(AVG(FARE_AMOUNT), 2)   AS AVG_FARE,
           ROUND(AVG(TRIP_DISTANCE), 2) AS AVG_DISTANCE,
           ROUND(AVG(TIP_AMOUNT), 2)    AS AVG_TIP,
           ROUND(SUM(TOTAL_AMOUNT), 0)  AS TOTAL_REVENUE
    FROM GOLD.FACT_TRIPS
""").to_pandas()

total_trips      = int(df_kpi['TOTAL_TRIPS'][0])
total_passengers = int(df_kpi['TOTAL_PASSENGERS'][0])
avg_fare         = df_kpi['AVG_FARE'][0]
avg_distance     = df_kpi['AVG_DISTANCE'][0]
avg_tip          = df_kpi['AVG_TIP'][0]
total_revenue    = int(df_kpi['TOTAL_REVENUE'][0])

# =========================
# KPI CARDS
# =========================
st.markdown('<div class="section-header">Key Metrics</div>', unsafe_allow_html=True)

k1, k2, k3, k4, k5, k6 = st.columns(6)

k1.markdown(f"""
<div class="kpi-box">
  <div class="kpi-label">Total Trips</div>
  <div class="kpi-value">{total_trips:,}</div>
</div>""", unsafe_allow_html=True)

k2.markdown(f"""
<div class="kpi-box">
  <div class="kpi-label">Passengers</div>
  <div class="kpi-value blue">{total_passengers:,}</div>
</div>""", unsafe_allow_html=True)

k3.markdown(f"""
<div class="kpi-box">
  <div class="kpi-label">Total Revenue</div>
  <div class="kpi-value green">${total_revenue:,}</div>
</div>""", unsafe_allow_html=True)

k4.markdown(f"""
<div class="kpi-box">
  <div class="kpi-label">Avg Fare</div>
  <div class="kpi-value orange">${avg_fare}</div>
</div>""", unsafe_allow_html=True)

k5.markdown(f"""
<div class="kpi-box">
  <div class="kpi-label">Avg Distance</div>
  <div class="kpi-value">{avg_distance} mi</div>
</div>""", unsafe_allow_html=True)

k6.markdown(f"""
<div class="kpi-box">
  <div class="kpi-label">Avg Tip</div>
  <div class="kpi-value green">${avg_tip}</div>
</div>""", unsafe_allow_html=True)

st.divider()

# =========================
# ROW 1: Daily Revenue + Borough
# =========================
col1, col2 = st.columns([3, 2])
with col1:
    st.markdown('<div class="section-header">Daily Revenue</div>', unsafe_allow_html=True)

    df_daily = session.sql("""
        SELECT 
            TRIP_DATE, 
            ROUND(SUM(TOTAL_AMOUNT), 2) AS REVENUE
        FROM GOLD.FACT_TRIPS_SNAPSHOT
        GROUP BY TRIP_DATE 
        ORDER BY TRIP_DATE
    """).to_pandas()

    st.line_chart(df_daily.set_index("TRIP_DATE"), color="#FFD700", height=280)

with col2:
    st.markdown('<div class="section-header">Trips by Borough</div>', unsafe_allow_html=True)
    df_borough = session.sql("""
        SELECT tz.BOROUGH, COUNT(*) AS TRIP_COUNT
        FROM GOLD.FACT_TRIPS f
        JOIN GOLD.DIM_TAXI_ZONE tz ON f.PICKUP_ZONE_SK = tz.ZONE_SK
        GROUP BY tz.BOROUGH ORDER BY TRIP_COUNT DESC
    """).to_pandas()
    st.bar_chart(df_borough.set_index("BOROUGH"), color="#60b4ff", height=280)

# =========================
# ROW 2: Payment Types + Time Block
# =========================
col3, col4 = st.columns(2)

with col3:
    st.markdown('<div class="section-header">Payment Types</div>', unsafe_allow_html=True)
    df_pay = session.sql("""
        SELECT pt.PAYMENT_TYPE_NAME, COUNT(*) AS CNT
        FROM GOLD.FACT_TRIPS f
        JOIN GOLD.DIM_PAYMENT_TYPE pt ON f.PAYMENT_TYPE_SK = pt.PAYMENT_TYPE_SK
        GROUP BY pt.PAYMENT_TYPE_NAME ORDER BY CNT DESC
    """).to_pandas()
    st.bar_chart(df_pay.set_index("PAYMENT_TYPE_NAME"), color="#00e096", height=260)

# =========================
# TRIPS BY TIME OF DAY — shortened labels to fix truncation
# =========================
with col4:
    st.markdown('<div class="section-header">Trips by Time of Day</div>', unsafe_allow_html=True)
    df_time = session.sql("""
        SELECT
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                tb.TIME_BLOCK,
                'EARLY_MORNING', 'Early M'),
                'LATE_NIGHT',    'Late Night'),
                'MORNING',       'Morning'),
                'AFTERNOON',     'Afternoon'),
                'EVENING',       'Evening'
            ) AS TIME_BLOCK,
            COUNT(*) AS TRIP_COUNT
        FROM GOLD.FACT_TRIPS f
        JOIN GOLD.DIM_TIME_BLOCK tb ON f.TIME_BLOCK_SK = tb.TIME_BLOCK_SK
        GROUP BY TIME_BLOCK
        ORDER BY TRIP_COUNT DESC
    """).to_pandas()
    st.bar_chart(df_time.set_index("TIME_BLOCK"), color="#ff9f43", height=260)

st.divider()

# =========================
# ROW 3: Fare Category + Top Zones
# =========================
col5, col6 = st.columns(2)

# =========================
# FARE CATEGORY — derive from FARE_AMOUNT directly since column missing in fact_trips
# =========================
with col5:
    st.markdown('<div class="section-header">Fare Categories</div>', unsafe_allow_html=True)
    df_fare_cat = session.sql("""
        SELECT
            CASE
                WHEN FARE_AMOUNT <= 0  THEN 'Zero/Neg'
                WHEN FARE_AMOUNT <= 10 THEN 'Low'
                WHEN FARE_AMOUNT <= 30 THEN 'Medium'
                WHEN FARE_AMOUNT <= 60 THEN 'High'
                ELSE 'Premium'
            END AS FARE_CATEGORY,
            COUNT(*) AS TRIP_COUNT
        FROM GOLD.FACT_TRIPS
        GROUP BY FARE_CATEGORY
        ORDER BY TRIP_COUNT DESC
    """).to_pandas()
    st.bar_chart(df_fare_cat.set_index("FARE_CATEGORY"), color="#a29bfe", height=260)

with col6:
    st.markdown('<div class="section-header">Top 10 Pickup Zones</div>', unsafe_allow_html=True)
    df_zones = session.sql("""
        SELECT
            tz.ZONE,
            COUNT(*) AS TRIP_COUNT
        FROM GOLD.FACT_TRIPS f
        JOIN GOLD.DIM_TAXI_ZONE tz ON f.PICKUP_ZONE_SK = tz.ZONE_SK
        GROUP BY tz.ZONE
        ORDER BY TRIP_COUNT DESC
        LIMIT 10
    """).to_pandas()

    import altair as alt

    base = alt.Chart(df_zones)

    bars = base.mark_bar(color="#fd79a8").encode(
        x=alt.X("TRIP_COUNT:Q",
                axis=alt.Axis(title="Trip Count")),
        y=alt.Y("ZONE:N",
                sort="-x",
                axis=alt.Axis(
                    labelFontSize=12,
                    labelLimit=300,
                    title=None
                ))
    )

    text = base.mark_text(
        align="left",
        baseline="middle",
        dx=6,
        fontSize=11,
        color="white",
        fontWeight="bold"
    ).encode(
        x=alt.X("TRIP_COUNT:Q"),
        y=alt.Y("ZONE:N", sort="-x"),
        text=alt.Text("ZONE:N")
    )

    chart = (bars + text).properties(height=320)

    st.altair_chart(chart, use_container_width=True)
st.divider()

# =========================
# SCD2 DEMO
# =========================
st.markdown('<div class="section-header">SCD2 Demo — Vendor Name History</div>', unsafe_allow_html=True)
st.caption("CMT vendor changed name to CMT Digital — both rows tracked with effective dates")

df_scd2 = session.sql("""
    SELECT VENDOR_ID, VENDOR_NAME, EFFECTIVE_DATE, END_DATE,
           CASE WHEN IS_CURRENT = 1 THEN 'Active' ELSE 'Historical' END AS STATUS
    FROM GOLD.DIM_VENDOR
    ORDER BY VENDOR_ID, EFFECTIVE_DATE
""").to_pandas()
st.dataframe(df_scd2, use_container_width=True, height=250)

st.divider()

# =========================
# SAMPLE TRIPS
# =========================
st.markdown('<div class="section-header">Sample Trip Records (Latest 50)</div>', unsafe_allow_html=True)

df_sample = session.sql("""
    SELECT
        f.TRIP_DATE,
        v.VENDOR_NAME,
        pu.BOROUGH  AS PICKUP_BOROUGH,
        pu.ZONE     AS PICKUP_ZONE,
        do.BOROUGH  AS DROPOFF_BOROUGH,
        do.ZONE     AS DROPOFF_ZONE,
        f.PASSENGER_COUNT,
        f.TRIP_DISTANCE,
        f.FARE_AMOUNT,
        f.TIP_AMOUNT,
        f.TOTAL_AMOUNT
    FROM GOLD.FACT_TRIPS f
    LEFT JOIN GOLD.DIM_VENDOR    v  ON f.VENDOR_SK       = v.VENDOR_SK
    LEFT JOIN GOLD.DIM_TAXI_ZONE pu ON f.PICKUP_ZONE_SK  = pu.ZONE_SK
    LEFT JOIN GOLD.DIM_TAXI_ZONE do ON f.DROPOFF_ZONE_SK = do.ZONE_SK
    ORDER BY f.TRIP_DATE DESC
    LIMIT 50
""").to_pandas()
st.dataframe(df_sample, use_container_width=True)

# =========================
# FOOTER
# =========================
st.divider()
st.markdown(
    '<div style="text-align:center; color:#555; font-size:0.8rem;">CabPulse 360 &nbsp;|&nbsp; Team2 &nbsp;|&nbsp; Built on Databricks + Snowflake + Streamlit</div>',
    unsafe_allow_html=True
)