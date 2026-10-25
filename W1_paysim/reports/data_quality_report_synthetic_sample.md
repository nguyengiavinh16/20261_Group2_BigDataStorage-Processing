# PaySim Data Quality Report

- Source: `test_data/paysim_valid_sample.csv`
- Generated (UTC): 2026-10-25T09:14:46+00:00
- Rows: 300
- Verdict: **PASS**

## Validation checks

| Code | Severity | Violations | Rule |
|---|---|---:|---|
| NULL_VALUES | ERROR | 0 | No missing values in any required column |
| TYPE_DOMAIN | ERROR | 0 | type must be one of CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| STEP_RANGE | ERROR | 0 | step must be an integer in [1, 744] |
| AMOUNT_NONNEGATIVE | ERROR | 0 | amount must not be negative |
| BALANCE_NONNEGATIVE | ERROR | 0 | All balance fields must be >= 0 |
| FRAUD_LABEL_DOMAIN | ERROR | 0 | isFraud must be 0 or 1 |
| FLAG_LABEL_DOMAIN | ERROR | 0 | isFlaggedFraud must be 0 or 1 |
| ORIG_ID_FORMAT | ERROR | 0 | nameOrig must be 'C' followed by digits |
| DEST_ID_FORMAT | ERROR | 0 | nameDest must be 'C' or 'M' followed by digits |
| FLAG_RULE | ERROR | 0 | isFlaggedFraud=1 only for TRANSFER with amount > 200,000 |
| ORIG_BALANCE_MATCH | WARN | 0 | newbalanceOrig should equal oldbalanceOrg + amount (CASH_IN) or - amount (others) |
| DEST_BALANCE_MATCH | WARN | 0 | TRANSFER to a customer: newbalanceDest should equal oldbalanceDest + amount |
| MERCHANT_DEST_NO_BALANCE | WARN | 0 | Merchant destinations (M*) are expected to carry zero destination balances |
| MERCHANT_DEST_TYPE | WARN | 0 | Merchant destinations (M*) are expected only for PAYMENT |
| FRAUD_TYPE_PROFILE | WARN | 0 | Fraud labels are expected only on TRANSFER and CASH_OUT |
| AMOUNT_ZERO | WARN | 0 | amount equals 0 (observed in source data; review before use) |
| DUPLICATE_ROWS | WARN | 0 | Exact duplicate rows |

## Completeness

| Column | Missing values |
|---|---:|
| step | 0 |
| type | 0 |
| amount | 0 |
| nameOrig | 0 |
| oldbalanceOrg | 0 |
| newbalanceOrig | 0 |
| nameDest | 0 |
| oldbalanceDest | 0 |
| newbalanceDest | 0 |
| isFraud | 0 |
| isFlaggedFraud | 0 |

Duplicate rows: 0

## Transaction type profile

| Type | Rows | Fraud | Fraud rate | Median amount | Total amount |
|---|---:|---:|---:|---:|---:|
| CASH_IN | 60 | 0 | 0.0000% | 29,338.35 | 3,346,384.38 |
| CASH_OUT | 103 | 2 | 1.9417% | 24,777.70 | 3,964,960.69 |
| DEBIT | 19 | 0 | 0.0000% | 17,872.48 | 1,012,625.11 |
| PAYMENT | 71 | 0 | 0.0000% | 21,221.39 | 3,007,411.13 |
| TRANSFER | 47 | 1 | 2.1277% | 19,384.87 | 1,749,358.42 |

## Other statistics

- step range observed: 7 to 744 (allowed 1 to 744)
- Fraud rows (isFraud=1): 3
- Flagged rows (isFlaggedFraud=1): 1
- amount quantiles: p0=1,549.99, p50=23,938.58, p90=94,535.04, p99=289,591.74, p100=516,697.58
