# PaySim Data Source

The Fraud Detection project uses a local PaySim CSV for data profiling, Spark batch processing, model training and transaction replay. This document identifies the file used by the team and records the information needed to obtain and verify the same dataset.

**Owner:** Đạt, Batch and ML Lead  
**Local verification date:** 2026-10-13  
**Status:** Local file metadata and checksum verified. Download provenance remains to be confirmed.

## Source Information

| Field | Value |
|---|---|
| Dataset name | PaySim |
| Data description | Synthetic financial transaction data, as described in the existing project EDA notebook |
| Download page URL | https://www.kaggle.com/datasets/mtalaltariq/paysim-data |
| Dataset author or publisher |Muhammad Talal Tariq |
| Publisher dataset version | Version 1 |
| Date downloaded by the team | Not recorded|
| License and license reference |Unknown |
| Download method | |
The repository contains Kaggle search results with multiple dataset candidates. The available file metadata does not associate the local CSV with a specific candidate. The download URL, publisher, version and license therefore need to be confirmed from the actual download record.

## Local File Identification

| Field | Verified value |
|---|---|
| Local file name | `paysim dataset.csv` |
| Repository-relative path | `data/paysim dataset.csv` |
| Format | CSV with a header row |
| File size | 493,534,783 bytes |
| Number of columns | 11 |
| Local snapshot identifier | SHA-256 checksum below |
| Changes made after download | Not documented; confirm any renaming or content changes |

**SHA-256**

```text
16910F90577B0D981BF8FF289714510BB89BC71BFF7D3F220F024E287E4EEA6B
```

The following column names were verified directly from the local CSV header:

```text
step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
```

## Obtaining and Preparing the Data

The exact retrieval instructions will be completed once the download page and dataset version are confirmed. Record the selected version, original file name and any extraction or renaming steps.

After obtaining the file:

1. Create the `data/` directory at the repository root if it does not exist.
2. Extract the dataset if it is supplied as an archive.
3. Place the CSV at `data/paysim dataset.csv`. Record any filename change in this document.
4. Calculate the file size and SHA-256 checksum using the commands below.
5. Compare the values with the verified local file identification. A different checksum identifies different file contents and requires investigation before treating the file as the same snapshot.

Run these PowerShell commands from the repository root:

```powershell
(Get-Item -LiteralPath "data/paysim dataset.csv").Length

(Get-FileHash -LiteralPath "data/paysim dataset.csv" -Algorithm SHA256).Hash
```



## Intended Use and Data Context

The dataset supports:

- Profiling transaction characteristics and data quality.
- Creating processed Parquet data through Spark ETL.
- Training and evaluating fraud detection models.
- Replaying transactions to test the Kafka and streaming pipeline.

The existing [EDA notebook](../paysim_basic_eda.ipynb) describes `isFraud` as the fraud label and `isFlaggedFraud` as an existing system flag. Ground-truth labels are kept separate from scoring input. Detailed column definitions, feature availability and validation rules belong in the data dictionary and feature documentation.

The data is synthetic. Findings from this dataset do not establish performance on real banking transactions. Any timestamps generated for replay must be documented as simulated event times.

## Profiling Reference

See [the current PaySim EDA notebook](../paysim_basic_eda.ipynb) for row counts, label distribution, transaction types, missing values and duplicate checks.

Its saved outputs are previous profiling results. This source note verifies the local file size, header and checksum; it does not rerun the full profiling analysis.
