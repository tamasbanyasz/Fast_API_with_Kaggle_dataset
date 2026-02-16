"""
ETL folyamat egyszeri indítása – letöltés és Parquet export.
Használat: python -m etl.run_etl [--clean]
  --clean   Törli a data/parquet mappát és a Kaggle cache-t, majd nulláról letölt
"""
import argparse
import os
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_ROOT / "data" / "parquet"
DATASET_ID = "debashis74017/stock-market-data-nifty-50-stocks-1-min-data"


def get_kagglehub_dataset_path() -> Path:
    """Dataset elérési útja (letöltés vagy cache)."""
    import kagglehub
    return Path(kagglehub.dataset_download(DATASET_ID))


def clean_parquet():
    """Törli a projekt data/parquet mappáját."""
    if PARQUET_PATH.exists():
        print(f"Törlés: {PARQUET_PATH}")
        shutil.rmtree(PARQUET_PATH)
        print("  Parquet mappa törölve.")
    else:
        print("Parquet mappa nem létezik – kihagyva.")


def get_kagglehub_dataset_cache_path() -> Path:
    """Kaggle cache mappa útvonala letöltés nélkül."""
    cache = Path.home() / ".cache" / "kagglehub"
    if "KAGGLEHUB_CACHE" in os.environ:
        cache = Path(os.environ["KAGGLEHUB_CACHE"])
    # datasets/owner/dataset-name
    return cache / "datasets" / "debashis74017" / "stock-market-data-nifty-50-stocks-1-min-data"


def clean_kagglehub_cache():
    """Törli a kagglehub cache-ben lévő CSV adatokat (újra letöltéshez)."""
    try:
        path = get_kagglehub_dataset_cache_path()
        if path.exists():
            print(f"Törlés: Kaggle cache ({path})")
            shutil.rmtree(path)
            print("  Kaggle cache törölve.")
        else:
            print("Kaggle cache üres – kihagyva.")
    except Exception as e:
        print(f"Figyelmeztetés: Kaggle cache törlése sikertelen: {e}")


def run_export():
    """Parquet export futtatása."""
    from etl.export_to_parquet import main as export_main
    export_main()


def main():
    parser = argparse.ArgumentParser(
        description="ETL: Kaggle letöltés → Parquet export",
        epilog="Példa: python -m etl.run_etl --clean",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Törli a Parquet-ot és a Kaggle CSV cache-t, majd nulláról indít",
    )
    args = parser.parse_args()

    if args.clean:
        print("=== Tisztítás ===")
        clean_parquet()
        clean_kagglehub_cache()
        print()

    print("=== ETL indítása ===")
    print("1. Kaggle letöltés (CSV)...")
    get_kagglehub_dataset_path()
    print("   Kész.")

    print("\n2. Parquet export...")
    run_export()
    print("\n=== ETL kész. ===")


if __name__ == "__main__":
    main()
