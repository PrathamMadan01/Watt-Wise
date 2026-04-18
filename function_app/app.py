import streamlit as st 
import pandas as pd
import plotly.graph_objects as go
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
    page_title="WattWise Monitoring",
    layout="wide"
)

st.title("⚡ Watt-Wise Energy Monitoring")
st.caption("Real-Time Energy Analytics")
st.divider()

# ---------------- SIDEBAR ---------------- #
with st.sidebar:
    st.title("⚙ Controls")

    refresh_rate = st.slider("Refresh Rate (seconds)", 2, 30, 5)

    st.subheader("⏱ Time Filter")
    time_range = st.selectbox(
        "Select Time Range",
        ["Last 5 min", "Last 15 min", "Last 1 hour", "All"]
    )

# ---------------- AUTO REFRESH ---------------- #
st_autorefresh(interval=refresh_rate * 1000, key="datarefresh")

# ---------------- COSMOS DB CONNECTION ---------------- #
COSMOS_URL = os.getenv("COSMOS_URL")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

client = CosmosClient(COSMOS_URL, credential=COSMOS_KEY)
database = client.get_database_client(DATABASE_NAME)
container = database.get_container_client(CONTAINER_NAME)

# ---------------- LOAD DATA ---------------- #
@st.cache_data(ttl=5)
def load_data():
    query = """
    SELECT * FROM c 
    ORDER BY c.timestamp DESC 
    OFFSET 0 LIMIT 500
    """
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    return pd.DataFrame(items)

df = load_data()

# ---------------- HANDLE EMPTY DATA ---------------- #
if df.empty:
    st.warning("No data available yet.")
    st.stop()

# ---------------- DATA CLEANING ---------------- #
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
df = df.dropna(subset=["timestamp"])
df = df.sort_values("timestamp")

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
    st.warning("No data available for selected time range.")
    st.stop()

# ---------------- LOAD ML MODEL ---------------- #
@st.cache_resource
def load_model():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    model_path = os.path.join(BASE_DIR, "models", "energy_anomaly_model.pkl")
    scaler_path = os.path.join(BASE_DIR, "models", "scaler.pkl")

    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)

    return model, scaler

model, scaler = load_model()

# ---------------- ML ANOMALY DETECTION ---------------- #
feature_columns = ["power_kw", "voltage", "current_a"]

if all(col in df.columns for col in feature_columns):
    clean_df = df.dropna(subset=feature_columns)

    if not clean_df.empty:
        X = clean_df[feature_columns]
        X_scaled = scaler.transform(X)
        predictions = model.predict(X_scaled)

        df.loc[clean_df.index, "ml_anomaly"] = (predictions == -1)
    else:
        df["ml_anomaly"] = False
else:
    df["ml_anomaly"] = False

# ---------------- SYSTEM STATUS ---------------- #
if df["ml_anomaly"].mean() > 0.1:
    st.error("🔴 High anomaly rate detected!")
else:
    st.success("🟢 System operating normally")

# ---------------- KPI ROW ---------------- #
col1, col2, col3, col4 = st.columns(4)

col1.metric("⚡ Avg Power", f"{df['power_kw'].mean():.2f} kW" if "power_kw" in df else "N/A")
col2.metric("📈 Peak Power", f"{df['power_kw'].max():.2f} kW" if "power_kw" in df else "N/A")
col3.metric("🚨 Anomalies", int(df["ml_anomaly"].sum()))
col4.metric("🖥 Active Devices", df["device"].nunique() if "device" in df else "N/A")

st.divider()

# ---------------- DEVICE FILTER ---------------- #
def natural_sort_key(s):
    return [int(text) if text.isdigit() else text 
            for text in re.split(r'(\d+)', s)]

if "device" in df.columns:
    devices = sorted(df["device"].dropna().unique(), key=natural_sort_key)
    device = st.sidebar.selectbox("Select Device", devices)
    filtered = df[df["device"] == device]
else:
    st.error("Device column missing in data.")
    st.stop()

anomalies = filtered[filtered["ml_anomaly"] == True]

# ---------------- POWER CHART ---------------- #
st.subheader("📊 Power Usage Over Time")

fig = go.Figure()

if "power_kw" in filtered.columns:
    fig.add_trace(go.Scatter(
        x=filtered["timestamp"],
        y=filtered["power_kw"],
        mode="lines",
        name="Power Usage",
        line=dict(width=3)
    ))

    fig.add_trace(go.Scatter(
        x=filtered["timestamp"],
        y=filtered["power_kw"].rolling(10).mean(),
        mode="lines",
        name="Rolling Avg",
        line=dict(dash="dash")
    ))

    fig.add_trace(go.Scatter(
        x=anomalies["timestamp"],
        y=anomalies["power_kw"],
        mode="markers",
        name="Anomalies",
        marker=dict(size=10, color="red")
    ))

fig.update_layout(
    template="plotly_white",
    xaxis_title="Time",
    yaxis_title="Power (kW)",
    margin=dict(l=10, r=10, t=40, b=10),
    legend=dict(orientation="h")
)

st.plotly_chart(fig, use_container_width=True)

# ---------------- VOLTAGE & CURRENT ---------------- #
st.subheader("⚡ Voltage & Current Trends")

col1, col2 = st.columns(2)

with col1:
    if "voltage" in filtered.columns:
        st.line_chart(filtered.set_index("timestamp")["voltage"])

with col2:
    if "current_a" in filtered.columns:
        st.line_chart(filtered.set_index("timestamp")["current_a"])

# ---------------- ANOMALY TABLE ---------------- #
st.subheader("🚨 Recent Anomalies")

if not anomalies.empty:
    st.dataframe(
        anomalies.sort_values("timestamp", ascending=False).head(10),
        use_container_width=True
    )
else:
    st.success("No anomalies detected for selected device.")

# ---------------- DOWNLOAD BUTTON ---------------- #
st.download_button(
    "⬇ Download Data",
    df.to_csv(index=False),
    file_name="telemetry.csv"
)