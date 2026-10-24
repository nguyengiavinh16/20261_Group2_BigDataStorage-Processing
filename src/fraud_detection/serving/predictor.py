from pathlib import Path
import joblib
import pandas as pd
from fraud_detection.processing.features import prepare_features

class Predictor:
    def __init__(self, model_path: str, threshold: float | None = None):
        path = Path(model_path)
        if not path.exists():
            raise FileNotFoundError(f"Model not found: {path}. Run the training command first.")
        bundle = joblib.load(path)
        self.model = bundle["model"]
        self.threshold = float(threshold if threshold is not None else bundle.get("threshold", 0.5))
        self.model_version = bundle.get("model_version", "unknown")

    def predict_one(self, payload: dict) -> dict:
        frame = pd.DataFrame([payload])
        x = prepare_features(frame)
        probability = float(self.model.predict_proba(x)[0][1])
        return {"is_fraud": probability >= self.threshold, "fraud_probability": probability,
                "threshold": self.threshold, "model_version": self.model_version}
