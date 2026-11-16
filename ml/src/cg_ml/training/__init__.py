"""Model training (MLS-08, phase P4).

Plan:
- Data: cg.gold.symbol_features_1m (exchange='ALL' and per exchange) joined with gold.pump_cases
  → label = 1 for windows within [announce - 2 min, announce + 10 min], else 0. Split BY EVENT/TIME, never random rows.
- Models: IsolationForest (unsupervised baseline), XGBoost/LightGBM (supervised, class weights).
- Track in MLflow (experiment from config mlflow.experiment); register as 'cg-pump-model'.
- Detector (DET-11) loads the registered model and adds reason MODEL_SCORE_HIGH.
"""
