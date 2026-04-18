from numpy._core.multiarray import scalar
import pandas as pd
import joblib

model = joblib.load("models/energy_anomaly_model.pkl")
scaler = joblib.load("models/scaler.pkl") 


df = pd.read_csv(
    "household_power_consumption.txt",
    sep=";",
    low_memory=False,
    na_values=["?"]
)

df["timestamp"] = pd.to_datetime(
    df["Date"] + " " + df["Time"],
    dayfirst=True
)

df = df.rename(columns={
    "Voltage": "voltage",
    "Global_intensity": "current",
    "Global_active_power": "power"
})

df = df[["timestamp", "voltage", "current", "power"]]

df["voltage"] = pd.to_numeric(df["voltage"], errors="coerce")
df["current"] = pd.to_numeric(df["current"], errors="coerce")
df["power"] = pd.to_numeric(df["power"], errors="coerce")

df = df.dropna()

features = df[["voltage", "current", "power"]]

X = scaler.transform(features)

predictions = model.predict(X)

df["anomaly"] = predictions

print(df["anomaly"].value_counts())