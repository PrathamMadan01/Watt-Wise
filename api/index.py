"""
WattWise — Vercel Serverless API Entrypoint
Serves telemetry data from Azure Cosmos DB as JSON.
"""

from http.server import BaseHTTPRequestHandler
import json
import os
import joblib
import numpy as np

from azure.cosmos import CosmosClient


# ──────────────── Lazy Singletons ──────────────── #
_cosmos_container = None
_model = None
_scaler = None


def _get_cosmos():
    """Return a cached Cosmos container client."""
    global _cosmos_container
    if _cosmos_container is None:
        client = CosmosClient(
            os.environ["COSMOS_URL"],
            credential=os.environ["COSMOS_KEY"],
        )
        db = client.get_database_client(os.environ["DATABASE_NAME"])
        _cosmos_container = db.get_container_client(
            os.environ["CONTAINER_NAME"]
        )
    return _cosmos_container


def _get_model():
    """Load the ML model + scaler once per cold-start."""
    global _model, _scaler
    if _model is None:
        base = os.path.dirname(os.path.abspath(__file__))
        model_path = os.path.join(
            base, "..", "function_app", "models", "energy_anomaly_model.pkl"
        )
        scaler_path = os.path.join(
            base, "..", "function_app", "models", "scaler.pkl"
        )
        if os.path.exists(model_path) and os.path.exists(scaler_path):
            _model = joblib.load(model_path)
            _scaler = joblib.load(scaler_path)
    return _model, _scaler


# ──────────────── ML Inference ──────────────── #
LIVE_TO_MODEL = {
    "voltage": "voltage",
    "current_a": "current",
    "power_kw": "power",
}


def _run_anomaly_detection(items: list) -> list:
    """Attach ml_anomaly boolean to each item in-place."""
    model, scaler = _get_model()

    for item in items:
        item["ml_anomaly"] = False  # default

    if model is None or scaler is None:
        # Fallback: use the is_anomaly flag from EventHub function
        for item in items:
            item["ml_anomaly"] = bool(item.get("is_anomaly", False))
        return items

    live_cols = list(LIVE_TO_MODEL.keys())

    for item in items:
        try:
            vals = [float(item[c]) for c in live_cols]
            row = np.array(vals).reshape(1, -1)
            row_scaled = scaler.transform(row)
            pred = model.predict(row_scaled)
            item["ml_anomaly"] = bool(pred[0] == -1)
        except (KeyError, ValueError, TypeError):
            item["ml_anomaly"] = bool(item.get("is_anomaly", False))

    return items


# ──────────────── Request Handler ──────────────── #
class handler(BaseHTTPRequestHandler):
    """Vercel Python serverless function handler."""

    def do_GET(self):
        try:
            container = _get_cosmos()
            query = (
                "SELECT * FROM c "
                "ORDER BY c.timestamp DESC "
                "OFFSET 0 LIMIT 500"
            )
            items = list(
                container.query_items(
                    query=query,
                    enable_cross_partition_query=True,
                )
            )

            # Strip Cosmos metadata keys
            clean = []
            for item in items:
                clean.append(
                    {
                        k: v
                        for k, v in item.items()
                        if not k.startswith("_")
                    }
                )

            clean = _run_anomaly_detection(clean)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Cache-Control", "public, max-age=3, stale-while-revalidate=5"
            )
            self.end_headers()
            self.wfile.write(json.dumps(clean).encode())

        except Exception as exc:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(
                json.dumps({"error": str(exc)}).encode()
            )

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
