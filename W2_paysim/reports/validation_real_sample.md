# PaySim Data Quality Report

- Source: `test_data/paysim_real_sample.json`
- Generated (UTC): 2026-10-25T09:22:24+00:00
- Rows: 1,000
- Verdict: **PASS (with warnings)**

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
| ORIG_BALANCE_MATCH | WARN | 475 | newbalanceOrig should equal oldbalanceOrg + amount (CASH_IN) or - amount (others) |
| DEST_BALANCE_MATCH | WARN | 98 | TRANSFER to a customer: newbalanceDest should equal oldbalanceDest + amount |
| MERCHANT_DEST_NO_BALANCE | WARN | 0 | Merchant destinations (M*) are expected to carry zero destination balances |
| MERCHANT_DEST_TYPE | WARN | 0 | Merchant destinations (M*) are expected only for PAYMENT |
| FRAUD_TYPE_PROFILE | WARN | 0 | Fraud labels are expected only on TRANSFER and CASH_OUT |
| AMOUNT_ZERO | WARN | 0 | amount equals 0 (observed in source data; review before use) |
| DUPLICATE_ROWS | WARN | 0 | Exact duplicate rows |

## Example violating rows (0-based row index in the file)

- `ORIG_BALANCE_MATCH`: [8, 10, 13, 15, 16]
- `DEST_BALANCE_MATCH`: [2, 19, 24, 58, 78]

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
| CASH_IN | 183 | 0 | 0.0000% | 141,131.23 | 28,557,538.04 |
| CASH_OUT | 230 | 5 | 2.1739% | 113,670.37 | 36,179,257.94 |
| DEBIT | 51 | 0 | 0.0000% | 4,510.22 | 254,447.49 |
| PAYMENT | 437 | 0 | 0.0000% | 4,593.59 | 2,625,877.47 |
| TRANSFER | 99 | 4 | 4.0404% | 326,349.91 | 50,582,404.38 |

## Other statistics

- step range observed: 1 to 1 (allowed 1 to 744)
- Fraud rows (isFraud=1): 9
- Flagged rows (isFlaggedFraud=1): 0
- amount quantiles: p0=8.73, p50=14,658.08, p90=312,144.78, p99=1,429,163.92, p100=2,545,478.01
