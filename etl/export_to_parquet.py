"""
CSV adatok exportálása Parquet formátumba.
Pandas + PyArrow – symbol szerinti particionálás, Hadoop nélkül (Windows kompatibilis).
Futtatás: python -m etl.export_to_parquet   vagy   python etl/export_to_parquet.py
"""
from pathlib import Path

import pandas as pd

# Projekt gyökér (etl mappa szülője)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_ROOT / "data" / "parquet"


def get_dataset_path() -> Path:
    """Dataset elérési útja (kagglehub cache)."""
    import kagglehub
    return Path(kagglehub.dataset_download(
        "debashis74017/stock-market-data-nifty-50-stocks-1-min-data"
    ))


def main():
    print("Parquet export indítása...")
    data_path = get_dataset_path()
    csv_files = list(data_path.glob("*_minute.csv"))

    PARQUET_PATH.mkdir(parents=True, exist_ok=True)

    for i, csv_file in enumerate(csv_files, 1):
        symbol = csv_file.stem.replace("_minute", "")
        print(f"  [{i}/{len(csv_files)}] {symbol}...", end=" ", flush=True)
        df = pd.read_csv(csv_file, parse_dates=["date"])
        df["symbol"] = symbol
        out_dir = PARQUET_PATH / f"symbol={symbol}"
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_dir / "data.parquet", index=False, engine="pyarrow")
        print("ok", flush=True)

    print(f"\nKész. Parquet: {PARQUET_PATH}")


if __name__ == "__main__":
    main()
