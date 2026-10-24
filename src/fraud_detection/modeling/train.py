"""Train a reproducible baseline and save model + metrics."""
import argparse, json, logging
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import (average_precision_score, precision_score, recall_score,
                             f1_score, roc_auc_score, confusion_matrix, classification_report)
from sklearn.model_selection import train_test_split
from fraud_detection.processing.features import RAW_FEATURES, TARGET, prepare_features, validate_columns

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def build_model(model_name: str = "logistic"):
    numeric = [c for c in RAW_FEATURES if c != "type"]
    pre = ColumnTransformer([
        ("numeric", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), numeric),
        ("type", OneHotEncoder(handle_unknown="ignore"), ["type"]),
    ])
    if model_name == "forest":
        estimator = RandomForestClassifier(n_estimators=250, class_weight="balanced_subsample",
            min_samples_leaf=2, n_jobs=-1, random_state=42)
    else:
        estimator = LogisticRegression(max_iter=1000, class_weight="balanced", solver="liblinear", random_state=42)
    return Pipeline([("preprocess", pre), ("classifier", estimator)])

def train(input_path: str, model_path: str, metrics_path: str, sample_rows: int | None = None,
          model_name: str = "logistic") -> dict:
    df = pd.read_csv(input_path, nrows=sample_rows)
    validate_columns(df, require_target=True)
    df = df.dropna(subset=[TARGET]).copy()
    df[TARGET] = pd.to_numeric(df[TARGET], errors="raise").astype(int)
    if set(df[TARGET].unique()) - {0, 1}:
        raise ValueError("isFraud must contain only 0/1")
    if df[TARGET].nunique() < 2:
        raise ValueError("Training data must contain both classes; use a larger/more representative sample")
    X = prepare_features(df)
    y = df[TARGET]
    # Stratified random split is baseline; a later experiment should compare time-based split by step.
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
        random_state=42, stratify=y)
    model = build_model(model_name)
    model.fit(X_train, y_train)
    probs = model.predict_proba(X_test)[:, 1]
    threshold = 0.5
    pred = (probs >= threshold).astype(int)
    metrics = {
        "model": model_name, "rows": int(len(df)), "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)), "positive_rate": float(y.mean()), "threshold": threshold,
        "precision": float(precision_score(y_test, pred, zero_division=0)),
        "recall": float(recall_score(y_test, pred, zero_division=0)),
        "f1": float(f1_score(y_test, pred, zero_division=0)),
        "pr_auc": float(average_precision_score(y_test, probs)),
        "roc_auc": float(roc_auc_score(y_test, probs)),
        "confusion_matrix": confusion_matrix(y_test, pred).tolist(),
        "classification_report": classification_report(y_test, pred, output_dict=True, zero_division=0),
        "warning": "Random stratified split is a baseline; evaluate a temporal split before claiming production performance."
    }
    mp, ep = Path(model_path), Path(metrics_path)
    mp.parent.mkdir(parents=True, exist_ok=True); ep.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "features": RAW_FEATURES, "threshold": threshold,
                 "model_version": f"{model_name}-baseline-v1"}, mp)
    ep.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    logging.info("Saved model to %s and metrics to %s", mp, ep)
    return metrics

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/raw/paysim_dataset.csv")
    p.add_argument("--model", default="artifacts/fraud_model.joblib")
    p.add_argument("--metrics", default="reports/model_metrics.json")
    p.add_argument("--sample-rows", type=int, default=None)
    p.add_argument("--algorithm", choices=["logistic", "forest"], default="logistic")
    a = p.parse_args()
    print(json.dumps(train(a.input, a.model, a.metrics, a.sample_rows, a.algorithm), indent=2))
