"""Shared feature construction. Never use the target column as a model feature."""
import pandas as pd

RAW_FEATURES = ["step", "type", "amount", "oldbalanceOrg", "newbalanceOrig",
                "oldbalanceDest", "newbalanceDest", "isFlaggedFraud"]
TARGET = "isFraud"
REQUIRED_COLUMNS = RAW_FEATURES + ["nameOrig", "nameDest", TARGET]


def validate_columns(frame: pd.DataFrame, require_target: bool = True) -> None:
    required = set(RAW_FEATURES + ([TARGET] if require_target else []))
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def prepare_features(frame: pd.DataFrame) -> pd.DataFrame:
    validate_columns(frame, require_target=False)
    x = frame[RAW_FEATURES].copy()
    x["type"] = x["type"].astype("string").fillna("UNKNOWN")
    for col in [c for c in RAW_FEATURES if c != "type"]:
        x[col] = pd.to_numeric(x[col], errors="coerce")
        x[col] = x[col].fillna(0)
    return x
