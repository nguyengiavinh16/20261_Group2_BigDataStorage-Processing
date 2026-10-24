# Data directory

- `raw/`: full downloaded dataset; do not commit it.
- `sample/`: tiny synthetic fixture for tests only.
- `processed/`: generated clean datasets; do not commit large outputs.

The commonly distributed PaySim dataset contains fields such as `step`, `type`,
`amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`,
`oldbalanceDest`, `newbalanceDest`, `isFraud`, and `isFlaggedFraud`.
The inspector must be run on the actual file before the pipeline relies on this
schema. Variants can differ. PaySim is simulated data, not a direct dump of
real bank transactions.
