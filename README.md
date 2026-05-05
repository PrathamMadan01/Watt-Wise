# ⚡ WattWise — Real-Time Energy Monitoring Platform

> ML-powered anomaly detection • Azure Event Hub streaming • CosmosDB telemetry • Streamlit dashboard • CI/CD pipelines

![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Functions-0078D4?logo=microsoft-azure&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&logoColor=white)

---

## 🏗 Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐     ┌──────────────────┐
│  Simulator   │────▶│  Azure Event Hub │────▶│ Azure Func   │────▶│   Cosmos DB      │
│ (telemetry)  │     │  (telemetryhub)  │     │ (EventHub →  │     │  (telemetrydb/   │
│              │     │                  │     │   CosmosDB)  │     │   telemetry)     │
└──────────────┘     └──────────────────┘     └──────────────┘     └────────┬─────────┘
                                                                            │
                                                                            ▼
                                                                   ┌──────────────────┐
                                                                   │  Streamlit       │
                                                                   │  Dashboard       │
                                                                   │  + ML Anomaly    │
                                                                   │    Detection     │
                                                                   └──────────────────┘
```

## 📁 Project Structure

```
Watt-Wise/
├── .github/workflows/
│   ├── ci.yml                     # CI: lint, test, validate
│   └── cd.yml                     # CD: deploy Function + Dashboard
├── .streamlit/
│   └── config.toml                # Dark theme configuration
├── function_app/
│   ├── EventHubFunction/
│   │   ├── __init__.py            # Azure Function: EventHub → CosmosDB
│   │   └── function.json          # Function trigger bindings
│   ├── models/
│   │   ├── energy_anomaly_model.pkl   # Trained IsolationForest model
│   │   └── scaler.pkl                 # StandardScaler for feature normalization
│   ├── anomaly_model.py           # ML model class definition
│   ├── app.py                     # Streamlit dashboard
│   ├── simulator.py               # Telemetry event simulator
│   ├── requirements.txt           # Python dependencies
│   └── .env                       # Environment variables (gitignored)
├── tests/
│   └── test_wattwise.py           # Automated tests
├── train_model.py                 # Model training script
├── Dockerfile                     # Dashboard container image
├── azure-pipelines.yml            # Azure DevOps pipeline (alternative)
├── host.json                      # Azure Functions host config
└── requirements.txt               # Root-level deps for Docker
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Azure subscription with Event Hub, Cosmos DB, and Function App
- Docker (for containerized deployment)

### 1. Clone & Install

```bash
git clone https://github.com/PrathamMadan01/Watt-Wise.git
cd Watt-Wise
pip install -r requirements.txt
```

### 2. Configure Environment

Create `function_app/.env`:

```env
EVENTHUB_CONNECTION_SEND=<your-eventhub-send-connection-string>
COSMOS_URL=<your-cosmosdb-url>
COSMOS_KEY=<your-cosmosdb-key>
DATABASE_NAME=telemetrydb
CONTAINER_NAME=telemetry
```

### 3. Train the ML Model

```bash
python train_model.py
```

### 4. Start the Simulator

```bash
python function_app/simulator.py
```

### 5. Launch the Dashboard

```bash
streamlit run function_app/app.py
```

## 🤖 ML Pipeline

| Component | Details |
|-----------|---------|
| **Algorithm** | Isolation Forest (unsupervised) |
| **Training Data** | UCI Household Power Consumption dataset |
| **Features** | `voltage`, `current`, `power` |
| **Contamination** | 3% (expected anomaly rate) |
| **Scaler** | StandardScaler |

## 🔄 CI/CD Pipelines

### GitHub Actions (`.github/workflows/`)

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `ci.yml` | Push / PR to `main` | Flake8 lint → import smoke test → model load test |
| `cd.yml` | Push to `main` | Deploy Azure Function + Build/Push Docker → Deploy Web App |

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `AZURE_FUNCTIONAPP_PUBLISH_PROFILE` | Publish profile for Azure Function App |
| `ACR_LOGIN_SERVER` | ACR server (e.g., `wattwiseacr.azurecr.io`) |
| `ACR_USERNAME` | ACR admin username |
| `ACR_PASSWORD` | ACR admin password |
| `AZURE_WEBAPP_NAME` | Azure Web App name for dashboard |
| `AZURE_WEBAPP_PUBLISH_PROFILE` | Publish profile for Azure Web App |

### Azure DevOps (alternative)

The `azure-pipelines.yml` provides an equivalent pipeline for Azure DevOps with multi-stage build and deploy.

## 📊 Dashboard Features

- **Real-time auto-refresh** (configurable 2–30s)
- **ML anomaly detection** on live data
- **Power usage charts** with rolling averages
- **Voltage & current monitoring** per device
- **Device power distribution** comparison
- **Time range filtering** (5 min / 15 min / 1 hour / All)
- **Anomaly table** with recent flagged events
- **CSV download** for raw telemetry data

## 📜 License

This project is for educational and placement demonstration purposes.
