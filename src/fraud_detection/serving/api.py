import os
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from fraud_detection.schemas.transactions import Transaction, PredictionResponse
from fraud_detection.serving.predictor import Predictor

app = FastAPI(title="Real-time Fraud Detection API", version="0.2.0")
MODEL_PATH = os.getenv("MODEL_PATH", "artifacts/fraud_model.joblib")
THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
_predictor = None

def get_predictor():
    global _predictor
    if _predictor is None:
        _predictor = Predictor(MODEL_PATH, THRESHOLD)
    return _predictor

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": _predictor is not None}

@app.post("/predict", response_model=PredictionResponse)
def predict(transaction: Transaction):
    try:
        result = get_predictor().predict_one(transaction.model_dump(exclude={"isFraud"}))
        result["created_at"] = datetime.now(timezone.utc).isoformat()
        uri = os.getenv("MONGODB_URI")
        if uri:
            try:
                with MongoClient(uri, serverSelectionTimeoutMS=800) as client:
                    db = client[os.getenv("MONGODB_DATABASE", "fraud_detection")]
                    db[os.getenv("MONGODB_COLLECTION", "predictions")].insert_one({**result, "transaction": transaction.model_dump()})
            except Exception:
                # Prediction remains available if the optional persistence layer is down.
                pass
        return result
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Prediction failed: {exc}") from exc
