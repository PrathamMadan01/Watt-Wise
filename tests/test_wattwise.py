"""
WattWise — Basic tests
Validates imports, model loading, and data pipeline logic.
"""

import os
import sys
import pytest

# Ensure the project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestImports:
    """Verify all critical packages can be imported."""

    def test_pandas_import(self):
        import pandas
        assert pandas is not None

    def test_numpy_import(self):
        import numpy
        assert numpy is not None

    def test_sklearn_import(self):
        from sklearn.ensemble import IsolationForest
        assert IsolationForest is not None

    def test_joblib_import(self):
        import joblib
        assert joblib is not None

    def test_plotly_import(self):
        import plotly.graph_objects as go
        assert go is not None

    def test_streamlit_import(self):
        import streamlit
        assert streamlit is not None

    def test_cosmos_import(self):
        from azure.cosmos import CosmosClient
        assert CosmosClient is not None


class TestModelLoading:
    """Verify the ML model and scaler can be loaded."""

    MODEL_PATH = os.path.join("function_app", "models", "energy_anomaly_model.pkl")
    SCALER_PATH = os.path.join("function_app", "models", "scaler.pkl")

    @pytest.mark.skipif(
        not os.path.exists(os.path.join("function_app", "models", "energy_anomaly_model.pkl")),
        reason="Model file not present (gitignored)"
    )
    def test_model_loads(self):
        import joblib
        model = joblib.load(self.MODEL_PATH)
        assert hasattr(model, "predict"), "Model must have a predict method"

    @pytest.mark.skipif(
        not os.path.exists(os.path.join("function_app", "models", "scaler.pkl")),
        reason="Scaler file not present (gitignored)"
    )
    def test_scaler_loads(self):
        import joblib
        scaler = joblib.load(self.SCALER_PATH)
        assert hasattr(scaler, "transform"), "Scaler must have a transform method"

    @pytest.mark.skipif(
        not os.path.exists(os.path.join("function_app", "models", "energy_anomaly_model.pkl")),
        reason="Model file not present (gitignored)"
    )
    def test_model_predicts(self):
        import joblib
        import numpy as np

        model = joblib.load(self.MODEL_PATH)
        scaler = joblib.load(self.SCALER_PATH)

        # Simulate a normal reading: voltage=230, current=10, power=2.3
        sample = np.array([[230.0, 10.0, 2.3]])
        scaled = scaler.transform(sample)
        prediction = model.predict(scaled)

        assert prediction[0] in [-1, 1], "Prediction must be -1 (anomaly) or 1 (normal)"


class TestSimulatorEvent:
    """Verify the simulator generates valid event payloads."""

    def _get_generate_event(self):
        """Import generate_event while mocking the EventHub producer."""
        from unittest.mock import MagicMock, patch
        import importlib

        # Mock the EventHub module so simulator.py doesn't need a real connection
        mock_producer = MagicMock()
        with patch.dict("os.environ", {"EVENTHUB_CONNECTION_SEND": "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==;EntityPath=test"}):
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "function_app"))
            # Force re-import in case it was cached
            if "simulator" in sys.modules:
                del sys.modules["simulator"]
            import simulator
            return simulator.generate_event

    def test_event_has_required_fields(self):
        generate_event = self._get_generate_event()
        event = generate_event()
        required_keys = ["id", "device", "timestamp", "building_id",
                         "power_kw", "voltage", "current_a", "temperature_c", "occupancy"]

        for key in required_keys:
            assert key in event, f"Missing key: {key}"

    def test_event_values_in_range(self):
        generate_event = self._get_generate_event()
        event = generate_event()
        assert 0.5 <= event["power_kw"] <= 10.0
        assert 225 <= event["voltage"] <= 235
        assert 0.5 <= event["current_a"] <= 25.0
        assert 18 <= event["temperature_c"] <= 35
        assert 0 <= event["occupancy"] <= 200
