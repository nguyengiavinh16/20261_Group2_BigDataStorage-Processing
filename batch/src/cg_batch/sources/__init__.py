"""Pure parsers for batch data sources (no Spark/Airflow imports). See docs/source-notes.md.

| Module | Source | Used by |
|---|---|---|
| binance_files | data.binance.vision daily trades (URLs, .CHECKSUM, timestamp unit) | BAT-01, BAT-03 |
| ofac | OFAC SDN.XML digital currency addresses | BAT-01, BAT-06 |
| darklist | MyEtherWallet ethereum-lists addresses-darklist.json | BAT-01, BAT-06 |
| pump_dataset | SystemsLab-Sapienza pump_telegram.csv | BAT-01, BAT-06 |
| evm | address normalisation (incl. 32-byte padded AWS addresses) | all of the above, BAT-07 |
"""
