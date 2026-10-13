# Project configurations

| File | Contents |
|---|---|
| `data_profiling.json` | Dataset and output paths, CSV parsing, numeric percentiles, balance checks, sampling and visualization settings. |
| `paysim_schema.json` | Ordered raw columns, field roles, expected domains and feature availability context. |

The notebook `notebooks/01_data_profiling.ipynb` loads both files. Dataset paths, CSV parsing, schema domains, percentiles, IQR bounds, balance formulas, sampling, exploratory temporal split fractions and output paths use these settings. Configuration fingerprints are recorded in the profiling manifest.

Temporal split fractions describe an exploratory analysis. Boundaries keep whole steps together and are selected from cumulative transaction counts; actual row fractions may differ from the targets. These boundaries are not an approved training split.

Paths are relative to the repository root. Unknown currency and step units remain `null`. The reference checksum identifies the local CSV snapshot rather than a publisher version. Balance formulas and domain checks are profiling assumptions pending team review, not approved ETL policies. Secrets and machine-specific credentials do not belong in these files.
