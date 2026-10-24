import pandas as pd
from fraud_detection.processing.features import prepare_features

def test_prepare_features_ignores_identifiers_and_target():
    frame=pd.DataFrame([{"step":1,"type":"PAYMENT","amount":10,"nameOrig":"A","oldbalanceOrg":10,
      "newbalanceOrig":0,"nameDest":"B","oldbalanceDest":0,"newbalanceDest":10,"isFlaggedFraud":0,"isFraud":1}])
    x=prepare_features(frame)
    assert "isFraud" not in x.columns
    assert "nameOrig" not in x.columns
    assert x.loc[0,"type"] == "PAYMENT"

def test_missing_numeric_values_are_filled():
    frame=pd.DataFrame([{"step":1,"type":None,"amount":None,"oldbalanceOrg":0,"newbalanceOrig":0,
      "oldbalanceDest":0,"newbalanceDest":0,"isFlaggedFraud":0}])
    x=prepare_features(frame)
    assert x.loc[0,"amount"] == 0
    assert x.loc[0,"type"] == "UNKNOWN"
