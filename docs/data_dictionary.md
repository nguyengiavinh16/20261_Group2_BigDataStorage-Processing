# Dataset and Feature Notes

## Dataset
PaySim is a synthetic mobile-money transaction simulator dataset. It is not a
direct record of real bank customers. Check the dataset host's current terms and
inspect the actual downloaded CSV before relying on any schema assumption.

## Commonly seen fields (verify against the actual file)
| Field | Typical meaning | Potential use |
|---|---|---|
| `step` | Relative simulation time step | Event-time ordering after documented conversion |
| `type` | Transaction type | Candidate categorical feature |
| `amount` | Transaction amount | Candidate numeric feature |
| `nameOrig` | Origin account identifier | Usually identifier, not automatically a model feature |
| `oldbalanceOrg` | Origin balance before transaction | Candidate numeric feature |
| `newbalanceOrig` | Origin balance after transaction | Audit availability and target leakage risk |
| `nameDest` | Destination identifier | Usually identifier / possible history key |
| `oldbalanceDest` | Destination balance before transaction | Candidate numeric feature if available at scoring time |
| `newbalanceDest` | Destination balance after transaction | Audit availability and target leakage risk |
| `isFraud` | Fraud label | Target only; never inference feature |
| `isFlaggedFraud` | Existing rule flag in common versions | Exclude from model features unless the experiment explicitly justifies it |

## Feature policy
Feature availability must be judged at the moment a transaction is scored. Do
not use labels, future events, or post-transaction values unavailable at that
moment. Account-history features require state and a defined look-back window.

## Class imbalance
Fraud labels are usually rare. Accuracy alone can look high while fraud recall is
poor. Report precision, recall, F1, PR-AUC, confusion matrix, and false-positive
and false-negative counts. Do not claim calibrated probabilities without testing
calibration.
