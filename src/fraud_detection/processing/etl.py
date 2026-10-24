"""Local, memory-conscious CSV validation and Parquet conversion."""
from pathlib import Path
import pandas as pd
from fraud_detection.processing.features import RAW_FEATURES, TARGET, validate_columns


def convert_csv_to_parquet(input_path: str, output_path: str, chunksize: int = 250_000) -> dict:
    src, dst = Path(input_path), Path(output_path)
    if not src.exists():
        raise FileNotFoundError(src)
    dst.parent.mkdir(parents=True, exist_ok=True)
    # Parquet is written after concatenation for portability; use Spark ETL for very large files.
    chunks = []
    total = 0
    for chunk in pd.read_csv(src, chunksize=chunksize):
        validate_columns(chunk, require_target=True)
        chunks.append(chunk)
        total += len(chunk)
    if not chunks:
        raise ValueError("Input CSV is empty")
    frame = pd.concat(chunks, ignore_index=True)
    frame.to_parquet(dst, index=False)
    return {"input": str(src), "output": str(dst), "rows": total, "columns": list(frame.columns)}
