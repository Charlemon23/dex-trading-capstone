from pathlib import Path
import pandas as pd
from typing import Optional

def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)

def save_df_csv(df: pd.DataFrame, out_dir: str, filename: Optional[str] = None) -> str:
    ensure_dir(out_dir)
    name = filename or "dataset.csv"
    out_path = Path(out_dir) / name
    df.to_csv(out_path, index=False)
    return str(out_path)
