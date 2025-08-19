# io_utils.py
from pathlib import Path
import json
import polars as pl

def read_json_records(dir_path: str) -> list[dict]:
    records = []
    for p in Path(dir_path).glob("*.json"):
        with open(p, "r") as f:
            txt = f.read().strip()
            # support either JSON array or JSONL files
            if txt.startswith("["):
                records.extend(json.loads(txt))
            else:
                for line in txt.splitlines():
                    if line.strip():
                        records.append(json.loads(line))
    return records

def read_csv(dir_path: str) -> pl.DataFrame:
    # Read all CSVs and vstack
    frames = []
    for p in Path(dir_path).glob("*.csv"):
        frames.append(pl.read_csv(p))
    return pl.concat(frames) if frames else pl.DataFrame()
