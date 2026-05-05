import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
import numpy as np
import os
from dotenv import load_dotenv
from azure.cosmos import CosmosClient
import re
import joblib

# ---------------- LOAD ENV ---------------- #
load_dotenv()

# ---------------- PAGE CONFIG ---------------- #
st.set_page_config(
    page_title="WattWise — Energy Monitoring",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------- CUSTOM CSS ---------------- #
st.markdown("""
<style>
    /* Import Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Global font */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Main title styling */
    .main-title {
        font-size: 2.4rem;
        font-weight: 700;
        background: linear-gradient(135deg, #00C2FF, #7B61FF, #FF6B6B);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
        letter-spacing: -0.5px;
    }

    .sub-caption {
        color: #8B8FA3;
        font-size: 0.95rem;
        margin-top: -4px;
        letter-spacing: 1.5px;
        text-transform: uppercase;
    }

    /* KPI card styling */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(28, 31, 38, 0.9), rgba(14, 17, 23, 0.95));
        border: 1px solid rgba(0, 194, 255, 0.15);
        border-radius: 16px;
        padding: 20px 24px;
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.3), 0 0 40px rgba(0, 194, 255, 0.05);
        transition: all 0.3s ease;
    }

    div[data-testid="stMetric"]:hover {
        border-color: rgba(0, 194, 255, 0.35);
        box-shadow: 0 4px 32px rgba(0, 0, 0, 0.4), 0 0 60px rgba(0, 194, 255, 0.1);
        transform: translateY(-2px);
    }

    div[data-testid="stMetricLabel"] {
        font-size: 0.85rem;
        font-weight: 500;
        color: #8B8FA3;
        letter-spacing: 0.5px;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
        color: #FFFFFF;
    }

    /* Status banner */
    .status-banner {
        padding: 14px 24px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.95rem;
        margin-bottom: 16px;
        display: flex;
        align-items: center;
        gap: 10px;
    }

    .status-ok {
        background: linear-gradient(135deg, rgba(0, 200, 83, 0.12), rgba(0, 200, 83, 0.05));
        border: 1px solid rgba(0, 200, 83, 0.3);
        color: #00C853;
    }

    .status-alert {
        background: linear-gradient(135deg, rgba(255, 82, 82, 0.12), rgba(255, 82, 82, 0.05));
        border: 1px solid rgba(255, 82, 82, 0.3);
        color: #FF5252;
        animation: pulse-alert 2s infinite;
    }

    @keyframes pulse-alert {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }

    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0E1117 0%, #161B22 100%);
        border-right: 1px solid rgba(0, 194, 255, 0.1);
    }

    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stSlider label {
        color: #8B8FA3;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 1px;
    }

    /* Chart containers */
    .stPlotlyChart {
        border-radius: 16px;
        overflow: hidden;
    }

    /* Section headers */
    h2, h3 {
        color: #E8EAED;
        font-weight: 600;
    }

    /* Divider */
    hr {
        border-color: rgba(0, 194, 255, 0.1) !important;
    }

    /* Dataframe styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    /* Download button */
    .stDownloadButton button {
        background: linear-gradient(135deg, #00C2FF, #7B61FF);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 12px 32px;
        font-weight: 600;
        font-size: 0.9rem;
        transition: all 0.3s ease;
        letter-spacing: 0.5px;
    }

    .stDownloadButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(0, 194, 255, 0.3);
    }

    /* Hide Streamlit default elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ---------------- HEADER ---------------- #
st.markdown('<p class="main-title">⚡ WattWise Energy Monitoring</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-caption">Real-Time Analytics • ML Anomaly Detection • Cloud-Powered</p>', unsafe_allow_html=True)
st.divider()

# ---------------- SIDEBAR ---------------- #
with st.sidebar:
    st.markdown("### ⚙️ Dashboard Controls")
    st.markdown("---")

    refresh_rate = st.slider(
        "🔄 Auto-Refresh (sec)",
        min_value=2, max_value=30, value=5,
        help="How often the dashboard fetches new data"
    )

    st.markdown("---")
    st.markdown("### ⏱ Time Filter")
    time_range = st.selectbox(
        "Select Time Range",
        ["Last 5 min", "Last 15 min", "Last 1 hour", "All"],
        index=3
    )

    st.markdown("---")
    st.markdown(
        '<p style="color:#555; font-size:0.75rem; text-align:center;">'
        'WattWise v1.0 • Powered by Azure</p>',
        unsafe_allow_html=True
    )

# ---------------- AUTO REFRESH ---------------- #
st_autorefresh(interval=refresh_rate * 1000, key="datarefresh")

# ---------------- COSMOS DB CONNECTION ---------------- #
COSMOS_URL = os.getenv("COSMOS_URL")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

if not all([COSMOS_URL, COSMOS_KEY, DATABASE_NAME, CONTAINER_NAME]):
    st.error("❌ Missing Cosmos DB environment variables. Check your `.env` file.")
    st.info("Required: `COSMOS_URL`, `COSMOS_KEY`, `DATABASE_NAME`, `CONTAINER_NAME`")
    st.stop()

try:
    client = CosmosClient(COSMOS_URL, credential=COSMOS_KEY)
    database = client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)
except Exception as e:
    st.error(f"❌ Failed to connect to Cosmos DB: {e}")
    st.stop()

# ---------------- LOAD DATA ---------------- #
@st.cache_data(ttl=5)
def load_data():
    query = """
    SELECT * FROM c 
    ORDER BY c.timestamp DESC 
    OFFSET 0 LIMIT 500
    """
    try:
        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        return pd.DataFrame(items)
    except Exception as e:
        st.error(f"❌ Error querying Cosmos DB: {e}")
        return pd.DataFrame()

df = load_data()

# ---------------- HANDLE EMPTY DATA ---------------- #
if df.empty:
    st.warning("⏳ No telemetry data available yet. Start the simulator to begin streaming data.")
    st.info("Run `python function_app/simulator.py` to start sending events.")
    st.stop()

# ---------------- DATA CLEANING ---------------- #
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
df = df.dropna(subset=["timestamp"])
df = df.sort_values("timestamp")

# Ensure numeric columns
for col in ["power_kw", "voltage", "current_a", "temperature_c", "occupancy"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

now = pd.Timestamp.now(tz="UTC")

# ---------------- TIME FILTER ---------------- #
if time_range == "Last 5 min":
    df = df[df["timestamp"] > now - pd.Timedelta(minutes=5)]
elif time_range == "Last 15 min":
    df = df[df["timestamp"] > now - pd.Timedelta(minutes=15)]
elif time_range == "Last 1 hour":
    df = df[df["timestamp"] > now - pd.Timedelta(hours=1)]

# If filtering removes everything
if df.empty:
    st.warning("⏳ No data available for the selected time range. Try selecting 'All'.")
    st.stop()

# ---------------- LOAD ML MODEL ---------------- #
@st.cache_resource
def load_model():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    model_path = os.path.join(BASE_DIR, "models", "energy_anomaly_model.pkl")
    scaler_path = os.path.join(BASE_DIR, "models", "scaler.pkl")

    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        return None, None

    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)

    return model, scaler

model, scaler = load_model()

# ---------------- ML ANOMALY DETECTION ---------------- #
# The ML model was trained on features: ["voltage", "current", "power"]
# The simulator sends:  voltage, current_a (→ current), power_kw (→ power)
# We map the live telemetry column names to the training feature names.

LIVE_TO_MODEL_MAP = {
    "voltage": "voltage",    # same name
    "current_a": "current",  # simulator sends current_a, model trained on current
    "power_kw": "power",     # simulator sends power_kw, model trained on power
}

live_feature_cols = list(LIVE_TO_MODEL_MAP.keys())   # columns in the dataframe
model_feature_order = list(LIVE_TO_MODEL_MAP.values())  # order expected by scaler

if model is not None and scaler is not None and all(col in df.columns for col in live_feature_cols):
    clean_df = df.dropna(subset=live_feature_cols)

    if not clean_df.empty:
        # Rename columns to match model training names before scaling
        X = clean_df[live_feature_cols].rename(columns=LIVE_TO_MODEL_MAP)
        X = X[model_feature_order]  # ensure correct column order

        X_scaled = scaler.transform(X)
        predictions = model.predict(X_scaled)

        df.loc[clean_df.index, "ml_anomaly"] = (predictions == -1)
    else:
        df["ml_anomaly"] = False
else:
    # Fallback: use the simple threshold from the EventHub function
    if "is_anomaly" in df.columns:
        df["ml_anomaly"] = df["is_anomaly"].astype(bool)
    else:
        df["ml_anomaly"] = False

# Ensure boolean type
df["ml_anomaly"] = df["ml_anomaly"].fillna(False).astype(bool)

# ---------------- SYSTEM STATUS BANNER ---------------- #
anomaly_rate = df["ml_anomaly"].mean()
if anomaly_rate > 0.1:
    st.markdown(
        '<div class="status-banner status-alert">'
        '🔴 HIGH ANOMALY RATE DETECTED — '
        f'{anomaly_rate:.1%} of readings flagged</div>',
        unsafe_allow_html=True
    )
else:
    st.markdown(
        '<div class="status-banner status-ok">'
        '🟢 System Operating Normally — '
        f'{anomaly_rate:.1%} anomaly rate</div>',
        unsafe_allow_html=True
    )

# ---------------- KPI ROW ---------------- #
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    avg_power = f"{df['power_kw'].mean():.2f} kW" if "power_kw" in df.columns else "N/A"
    st.metric("⚡ Avg Power", avg_power)

with col2:
    peak_power = f"{df['power_kw'].max():.2f} kW" if "power_kw" in df.columns else "N/A"
    st.metric("📈 Peak Power", peak_power)

with col3:
    anomaly_count = int(df["ml_anomaly"].sum())
    st.metric("🚨 Anomalies", anomaly_count)

with col4:
    device_count = df["device"].nunique() if "device" in df.columns else "N/A"
    st.metric("🖥 Active Devices", device_count)

with col5:
    total_records = len(df)
    st.metric("📊 Data Points", f"{total_records:,}")

st.divider()

# ---------------- DEVICE FILTER ---------------- #
def natural_sort_key(s):
    return [int(text) if text.isdigit() else text
            for text in re.split(r'(\d+)', s)]

if "device" in df.columns:
    devices = sorted(df["device"].dropna().unique(), key=natural_sort_key)
    device = st.sidebar.selectbox("🔌 Select Device", devices)
    filtered = df[df["device"] == device]
else:
    st.error("❌ Device column missing in data.")
    st.stop()

anomalies = filtered[filtered["ml_anomaly"] == True]

# ================================================================ #
#                          CHARTS                                    #
# ================================================================ #

# Common chart layout settings
CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=16, r=16, t=48, b=16),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
        font=dict(size=11, color="#8B8FA3")
    ),
    xaxis=dict(
        gridcolor="rgba(255,255,255,0.04)",
        title_font=dict(size=12, color="#8B8FA3"),
        tickfont=dict(size=10, color="#555")
    ),
    yaxis=dict(
        gridcolor="rgba(255,255,255,0.04)",
        title_font=dict(size=12, color="#8B8FA3"),
        tickfont=dict(size=10, color="#555")
    ),
    font=dict(family="Inter, sans-serif")
)

# ---------------- POWER CHART ---------------- #
st.subheader(f"📊 Power Usage — {device}")

if "power_kw" in filtered.columns and not filtered.empty:
    fig_power = go.Figure()

    # Main power line
    fig_power.add_trace(go.Scatter(
        x=filtered["timestamp"],
        y=filtered["power_kw"],
        mode="lines",
        name="Power (kW)",
        line=dict(width=2, color="#00C2FF"),
        fill="tozeroy",
        fillcolor="rgba(0, 194, 255, 0.06)"
    ))

    # Rolling average
    fig_power.add_trace(go.Scatter(
        x=filtered["timestamp"],
        y=filtered["power_kw"].rolling(10, min_periods=1).mean(),
        mode="lines",
        name="Rolling Avg",
        line=dict(dash="dash", width=2, color="#7B61FF")
    ))

    # Anomaly markers
    if not anomalies.empty:
        fig_power.add_trace(go.Scatter(
            x=anomalies["timestamp"],
            y=anomalies["power_kw"],
            mode="markers",
            name="Anomaly",
            marker=dict(size=10, color="#FF5252", symbol="x",
                        line=dict(width=2, color="#FF8A80"))
        ))

    fig_power.update_layout(
        **CHART_LAYOUT,
        xaxis_title="Time",
        yaxis_title="Power (kW)",
        height=400
    )

    st.plotly_chart(fig_power, use_container_width=True)
else:
    st.info("No power data available for this device.")

# ---------------- VOLTAGE & CURRENT CHARTS ---------------- #
st.subheader(f"⚡ Voltage & Current — {device}")

col_v, col_c = st.columns(2)

with col_v:
    if "voltage" in filtered.columns and filtered["voltage"].notna().any():
        fig_volt = go.Figure()
        fig_volt.add_trace(go.Scatter(
            x=filtered["timestamp"],
            y=filtered["voltage"],
            mode="lines",
            name="Voltage (V)",
            line=dict(width=2, color="#FFD600"),
            fill="tozeroy",
            fillcolor="rgba(255, 214, 0, 0.06)"
        ))
        fig_volt.update_layout(
            **CHART_LAYOUT,
            yaxis_title="Voltage (V)",
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig_volt, use_container_width=True)
    else:
        st.info("Voltage data not available.")

with col_c:
    if "current_a" in filtered.columns and filtered["current_a"].notna().any():
        fig_curr = go.Figure()
        fig_curr.add_trace(go.Scatter(
            x=filtered["timestamp"],
            y=filtered["current_a"],
            mode="lines",
            name="Current (A)",
            line=dict(width=2, color="#00E676"),
            fill="tozeroy",
            fillcolor="rgba(0, 230, 118, 0.06)"
        ))
        fig_curr.update_layout(
            **CHART_LAYOUT,
            yaxis_title="Current (A)",
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig_curr, use_container_width=True)
    else:
        st.info("Current data not available.")

# ---------------- DEVICE POWER DISTRIBUTION ---------------- #
st.subheader("📈 Power Distribution Across Devices")

if "power_kw" in df.columns and "device" in df.columns:
    device_stats = df.groupby("device")["power_kw"].agg(["mean", "max", "count"]).reset_index()
    device_stats.columns = ["Device", "Avg Power (kW)", "Peak Power (kW)", "Readings"]
    device_stats = device_stats.sort_values("Avg Power (kW)", ascending=True)

    fig_dist = go.Figure()
    fig_dist.add_trace(go.Bar(
        y=device_stats["Device"],
        x=device_stats["Avg Power (kW)"],
        orientation="h",
        name="Avg Power",
        marker=dict(
            color=device_stats["Avg Power (kW)"],
            colorscale=[[0, "#00C2FF"], [0.5, "#7B61FF"], [1, "#FF5252"]],
            cornerradius=6
        ),
        text=device_stats["Avg Power (kW)"].round(2),
        textposition="outside",
        textfont=dict(color="#8B8FA3", size=10)
    ))
    fig_dist.update_layout(
        **CHART_LAYOUT,
        xaxis_title="Avg Power (kW)",
        yaxis_title="",
        height=max(250, len(device_stats) * 35),
        showlegend=False
    )
    st.plotly_chart(fig_dist, use_container_width=True)

# ---------------- ANOMALY TABLE ---------------- #
st.subheader("🚨 Recent Anomalies")

if not anomalies.empty:
    display_cols = [c for c in ["timestamp", "device", "power_kw", "voltage", "current_a"] if c in anomalies.columns]
    st.dataframe(
        anomalies[display_cols].sort_values("timestamp", ascending=False).head(15),
        use_container_width=True,
        hide_index=True
    )
else:
    st.success(f"✅ No anomalies detected for **{device}**.")

# ---------------- DOWNLOAD BUTTON ---------------- #
st.divider()

col_dl, col_info = st.columns([1, 3])
with col_dl:
    st.download_button(
        "⬇ Download Data",
        df.to_csv(index=False),
        file_name="wattwise_telemetry.csv",
        mime="text/csv"
    )
with col_info:
    st.caption(f"📋 Showing {len(df)} records • Last updated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")