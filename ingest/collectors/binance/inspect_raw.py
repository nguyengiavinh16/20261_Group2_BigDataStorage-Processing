import json
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FILE_PATH = PROJECT_ROOT / "data" / "raw" / "binance" / "trades.jsonl"


def main():
    total_records = 0
    symbols = Counter()
    print("Reading raw data...\n")

    with open(FILE_PATH, "r", encoding="utf-8") as file:
        for line in file:
            record = json.loads(line)
            trade = record["raw_message"]["data"]
            total_records += 1
            symbols[trade["s"]] += 1
            if total_records <= 5:
                print(json.dumps(record, indent=2, ensure_ascii=False))

    print("\n========== SUMMARY ==========")
    print(f"Total records: {total_records}")
    print("\nRecords per symbol:")
    for symbol, count in symbols.items():
        print(f"{symbol}: {count}")
    print("==========================")


if __name__ == "__main__":
    main()
