import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import os

# ---------------- LOAD DATA ---------------- #

df = pd.read_csv(
    "household_power_consumption.txt",
    sep=";",
    low_memory=False,
    na_values=["?"]
)

print("Raw dataset shape:", df.shape)

# ---------------- DATA CLEANING ---------------- #

# Combine Date + Time into timestamp
df["timestamp"] = pd.to_datetime(
    df["Date"] + " " + df["Time"],
    dayfirst=True
)

# Rename columns
df = df.rename(columns={
    "Voltage": "voltage",
    "Global_intensity": "current",
    "Global_active_power": "power"
})

# Keep only required columns
df = df[["timestamp", "voltage", "current", "power"]]

# Convert to numeric (important!)
df["voltage"] = pd.to_numeric(df["voltage"], errors="coerce")
df["current"] = pd.to_numeric(df["current"], errors="coerce")
df["power"] = pd.to_numeric(df["power"], errors="coerce")

# Drop missing values
df = df.dropna()

print("Cleaned dataset shape:", df.shape)

# ---------------- FEATURE SELECTION ---------------- #

features = df[["voltage", "current", "power"]]

# ---------------- FEATURE SCALING ---------------- #

scaler = StandardScaler()
X_scaled = scaler.fit_transform(features)

# ---------------- TRAIN MODEL ---------------- #

model = IsolationForest(
    n_estimators=200,
    contamination=0.03,
    random_state=42
)

model.fit(X_scaled)

# ---------------- SAVE MODEL ---------------- #

os.makedirs("models", exist_ok=True)

joblib.dump(model, "models/energy_anomaly_model.pkl")
joblib.dump(scaler, "models/scaler.pkl")

print("Model training complete.")