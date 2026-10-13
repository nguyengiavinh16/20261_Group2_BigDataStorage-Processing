# PaySim Data Dictionary

**Owner:** Dat, Batch and ML Lead  
**Document version:** 1.0 draft  
**Reference date:** 2026-10-13  
**Scope:** The 11 raw columns in the local PaySim CSV. Field semantics and proposed validation rules remain subject to team review and verification against the confirmed dataset source.

## Dataset Reference

| Property | Value |
|---|---|
| Raw file | `data/paysim dataset.csv` |
| Format | CSV with a header; field order is preserved below |
| Profiled rows | 6,362,620 |
| Profiled columns | 11 |
| Missing cells | 0 |
| Exact duplicate rows beyond the first occurrence | 0 |
| Currency unit | Unverified; values remain in the original dataset unit |
| Step unit | Unverified; the original EDA describes simulated hours |
| Snapshot SHA-256 | `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b` |

Counts, data types, ranges, and identifier prefixes below are observed in this snapshot. Descriptions of transaction direction and balance timing are the project's working interpretation recorded in the schema and original EDA; they are not independently verified publisher definitions. Logical types describe field meaning. Pandas types describe the profiling DataFrame, not an approved Spark or streaming schema.

## Raw Columns

| Column | Working definition | Logical type | Observed pandas type | Expected domain / role |
|---|---|---|---|---|
| `step` | Index of the simulation step associated with a transaction. It is not a calendar timestamp. | Integer | `int64` | Positive integer; simulation ordering field. |
| `type` | Transaction category. | Categorical string | `category` | `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, or `TRANSFER`. |
| `amount` | Amount recorded for the transaction, in the dataset's original unit. | Number | `float64` | Finite, nonnegative value; candidate transaction feature. Zero is reported separately. |
| `nameOrig` | Identifier of the origin account or entity. | String identifier | `string` | Nonblank identifier; traceability and account grouping. It is not a transaction ID. |
| `oldbalanceOrg` | Recorded origin balance before the transaction, under the working balance interpretation. | Number | `float64` | Finite value in the original balance unit; scoring availability requires review. Preserve the raw spelling `Org`. |
| `newbalanceOrig` | Recorded origin balance after the transaction, under the working balance interpretation. | Number | `float64` | Finite value; potentially post-transaction information. |
| `nameDest` | Identifier of the destination account or entity. | String identifier | `string` | Nonblank identifier; traceability and account grouping. It is not a transaction ID. |
| `oldbalanceDest` | Recorded destination balance before the transaction, under the working balance interpretation. | Number | `float64` | Finite value; zero may reflect a recorded balance or unavailable balance coverage. |
| `newbalanceDest` | Recorded destination balance after the transaction, under the working balance interpretation. | Number | `float64` | Finite value; potentially post-transaction information. Zero may reflect unavailable balance coverage. |
| `isFraud` | Fraud outcome label used as the target in the project. | Binary integer | `int64` | `0` = non-fraud; `1` = fraud. Ground truth for training/evaluation, excluded from scoring features. |
| `isFlaggedFraud` | Existing system flag recorded alongside the transaction. It is separate from the fraud label. | Binary integer | `int64` | `0` = not flagged; `1` = flagged. Separate rule baseline, excluded from default scoring features. |

Numeric types are inferred before validation. Narrow integer casts are not evidence of valid domains: step integrality and binary label membership are checked separately. CSV itself does not retain pandas categorical or numeric type metadata.

## Transaction Categories

The names below have these working interpretations. Exact publisher definitions and simulation mechanics remain unverified.

| Value | Working interpretation | Observed transactions | Observed fraud rows |
|---|---|---:|---:|
| `CASH_IN` | Funds entering an account through a cash-in transaction. | 1,399,284 | 0 |
| `CASH_OUT` | Funds leaving an account through a cash-out transaction. | 2,237,500 | 4,116 |
| `DEBIT` | A debit transaction recorded by the simulation. | 41,432 | 0 |
| `PAYMENT` | A payment transaction; destination identifiers are M-prefixed in this snapshot. | 2,151,495 | 0 |
| `TRANSFER` | A transfer transaction between entities represented by the identifiers. | 532,909 | 4,097 |

Fraud appears in `CASH_OUT` and `TRANSFER` in this snapshot. This is a profiled observation, not a rule that other transaction types can never be fraudulent. The existing flag's rule and threshold have not been verified from the source.

## Observed Numeric Ranges and Zero Values

All values use the original dataset unit. These maxima describe the snapshot; they are not upper validation limits.

| Column | Minimum | Maximum | Zero-valued rows |
|---|---:|---:|---:|
| `step` | 1 | 743 | 0 |
| `amount` | 0.00 | 92,445,516.64 | 16 |
| `oldbalanceOrg` | 0.00 | 59,585,040.37 | 2,102,449 |
| `newbalanceOrig` | 0.00 | 49,585,040.37 | 3,609,566 |
| `oldbalanceDest` | 0.00 | 356,015,889.35 | 2,704,388 |
| `newbalanceDest` | 0.00 | 356,179,278.92 | 2,439,433 |

The snapshot contains no negative or nonfinite values in these numeric columns. Nonnegative balances are an observed property; the current schema only proposes a nonnegative domain restriction for `amount`. A future balance validation rule requires separate review.

| Binary field | Rows with value 0 | Rows with value 1 |
|---|---:|---:|
| `isFraud` | 6,354,407 | 8,213 |
| `isFlaggedFraud` | 6,362,604 | 16 |

## Identifiers, Missingness, and Balance Coverage

- `nameOrig` has 6,353,307 distinct values; every origin identifier is C-prefixed in this snapshot.
- `nameDest` has 2,722,362 distinct values. Destination rows include 4,211,125 C-prefixed and 2,151,495 M-prefixed identifiers. These are row counts, not distinct-account counts.
- The original EDA interprets C-prefixed identifiers as customer accounts and M-prefixed identifiers as merchants. Prefixes are descriptive observations here, not an approved identifier-format validator.
- There are no nulls in any column and no blank strings in `type`, `nameOrig`, or `nameDest`. This does not imply that every underlying balance is available or meaningful.
- Zero amounts and zero balances are retained in the profiling analysis. No verified sentinel-value specification distinguishes a true zero balance from unrecorded balance coverage. Zeros are not automatically converted to null or imputed.
- Balance residual mismatches are diagnostic observations under the notebook's type-dependent formulas. They are not automatic validation failures or fraud labels. Merchant destinations and records with both destination balances equal to zero are excluded from the applicable destination residual checks.
- The CSV has no explicit transaction identifier or real event timestamp. Account identifiers and `step` are not unique transaction keys. Any generated transaction ID or event-time mapping belongs to the ingestion/event contract and must retain its source lineage.

## Feature Availability Context

| Fields | Proposed use | Availability status |
|---|---|---|
| `amount`, `type` | Candidate MVP transaction features. | Candidate fields; their inclusion in the scoring event is pending contract review. |
| `step` | Simulation ordering and temporal profiling. | Step semantics and the scoring-time representation remain unverified. |
| `oldbalanceOrg`, `oldbalanceDest` | Potential pre-transaction balance features. | Available-at-scoring status and balance coverage require confirmation. |
| `newbalanceOrig`, `newbalanceDest` | Balance profiling and diagnostics. | Potential post-transaction information; excluded from default scoring features. |
| `nameOrig`, `nameDest` | Traceability, joins, and possible historical aggregation. | Identifiers rather than continuous numeric measurements; not default one-hot model inputs. |
| `isFraud` | Training target and evaluation ground truth. | Outcome information; excluded from scoring features. |
| `isFlaggedFraud` | Evaluation of the existing system flag. | Separate baseline decision; excluded from default scoring features. |

Balance-derived residuals inherit the availability limitations of their input fields. A strong association with the label does not establish that a field is available when a prediction must be made. No final training feature list is approved by this dictionary.

## Proposed Domain Checks

The profiling reference schema specifies all 11 columns as required for the raw dataset; this does not mean all fields belong in a scoring event. Proposed checks cover nulls, blank identifier/category strings, finite numeric values, positive integer steps, nonnegative amounts, known transaction categories, and binary labels.

The quality report proposes quarantine and audit for domain violations. Zero values, amount IQR outliers, and duplicates remain separate review conditions. Exact duplicate rows are reported beyond the first occurrence; no deduplication key or automatic removal policy has been approved. Rule counts may overlap and do not sum to the number of affected rows.

The full-dataset profiling found zero rows violating these proposed domain checks. This result does not validate the future ETL implementation or the handling of malformed external inputs.

