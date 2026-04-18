import numpy as np
from sklearn.ensemble import IsolationForest

class EnergyAnomalyDetector:
    def __init__(self):
        self.model = IsolationForest(
            n_estimators=100,
            contamination=0.05,
            random_state=42
        )
        self.is_trained = False

    def train(self, data):
        self.model.fit(data)
        self.is_trained = True

    def predict(self, data):
        return self.model.predict(data)  # -1 = anomaly, 1 = normal
