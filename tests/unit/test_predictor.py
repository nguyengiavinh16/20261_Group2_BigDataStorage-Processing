import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.base import BaseEstimator, ClassifierMixin
import joblib
from fraud_detection.serving.predictor import Predictor

class FixedClassifier(BaseEstimator, ClassifierMixin):
    def fit(self, X, y=None): self.classes_=[0,1]; return self
    def predict_proba(self, X): return [[0.1,0.9] for _ in range(len(X))]

def test_predictor_returns_probability(tmp_path):
    from sklearn.pipeline import Pipeline
    model=Pipeline([("identity",FunctionTransformer()),("classifier",FixedClassifier())])
    model.fit(pd.DataFrame([{"x": 1.0}]), [0])
    path=tmp_path/"model.joblib"
    joblib.dump({"model":model,"threshold":0.5,"model_version":"test"},path)
    predictor=Predictor(str(path))
    result=predictor.predict_one({"step":1,"type":"TRANSFER","amount":50,"oldbalanceOrg":50,
       "newbalanceOrig":0,"oldbalanceDest":0,"newbalanceDest":50,"isFlaggedFraud":0})
    assert result["is_fraud"] is True
    assert result["fraud_probability"] == 0.9
