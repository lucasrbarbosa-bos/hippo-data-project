from pathlib import Path
import json
import pandas as pd
from typing import List, Dict

def read_json_records(dir_path: str) -> list[dict]:
    """
    Read all *.json files in a directory. Supports JSON arrays and JSONL.
    Returns a list of Python dicts.
    """
    records: list[dict] = []
    for p in Path(dir_path).glob("*.json"):
        with open(p, "r", encoding="utf-8") as f:
            txt = f.read().strip()
            if not txt:
                continue
            if txt.lstrip().startswith("["):
                try:
                    data = json.loads(txt)
                    if isinstance(data, list):
                        records.extend(data)
                except Exception:
                    # fallback: try to parse as JSONL line-by-line
                    for line in txt.splitlines():
                        line = line.strip()
                        if line:
                            records.append(json.loads(line))
            else:
                for line in txt.splitlines():
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
    return records

def read_csv_concat(dir_path: str) -> pd.DataFrame:
    """
    Read and vertically concatenate all *.csv files in a directory.
    Returns empty DataFrame if no files.
    """
    frames: List[pd.DataFrame] = []
    for p in Path(dir_path).glob("*.csv"):
        frames.append(pd.read_csv(p))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
