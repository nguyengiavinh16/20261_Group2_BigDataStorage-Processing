# PaySim Data Quality Report

- Source: `data/raw/paysim.csv`
- Generated (UTC): 2026-10-25T09:17:21+00:00
- Rows: 6,362,620
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
| ORIG_BALANCE_MATCH | WARN | 3,779,503 | newbalanceOrig should equal oldbalanceOrg + amount (CASH_IN) or - amount (others) |
| DEST_BALANCE_MATCH | WARN | 120,811 | TRANSFER to a customer: newbalanceDest should equal oldbalanceDest + amount |
| MERCHANT_DEST_NO_BALANCE | WARN | 0 | Merchant destinations (M*) are expected to carry zero destination balances |
| MERCHANT_DEST_TYPE | WARN | 0 | Merchant destinations (M*) are expected only for PAYMENT |
| FRAUD_TYPE_PROFILE | WARN | 0 | Fraud labels are expected only on TRANSFER and CASH_OUT |
| AMOUNT_ZERO | WARN | 16 | amount equals 0 (observed in source data; review before use) |
| DUPLICATE_ROWS | WARN | 0 | Exact duplicate rows |

## Example violating rows (0-based row index in the file)

- `ORIG_BALANCE_MATCH`: [8, 10, 13, 15, 16]
- `DEST_BALANCE_MATCH`: [2, 19, 24, 58, 78]
- `AMOUNT_ZERO`: [2736447, 3247298, 3760289, 5563714, 5996408]

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
| CASH_IN | 1,399,284 | 0 | 0.0000% | 143,427.71 | 236,367,391,912.46 |
| CASH_OUT | 2,237,500 | 4,116 | 0.1840% | 147,072.18 | 394,412,995,224.49 |
| DEBIT | 41,432 | 0 | 0.0000% | 3,048.99 | 227,199,221.28 |
| PAYMENT | 2,151,495 | 0 | 0.0000% | 9,482.19 | 28,093,371,138.37 |
| TRANSFER | 532,909 | 4,097 | 0.7688% | 486,308.39 | 485,291,987,263.17 |

## Other statistics

- step range observed: 1 to 743 (allowed 1 to 744)
- Fraud rows (isFraud=1): 8,213
- Flagged rows (isFlaggedFraud=1): 16
- amount quantiles: p0=0.00, p50=74,871.94, p90=365,423.31, p99=1,615,979.47, p100=92,445,516.64
