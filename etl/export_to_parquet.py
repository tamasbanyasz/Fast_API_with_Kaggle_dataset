"""
CSV → Parquet export – Polars vagy DuckDB, párhuzamos feldolgozás.
Polars: Rust alapú, 5–10× gyorsabb Pandasnál.
DuckDB: SQL-lel közvetlen CSV → Parquet.
Futtatás: python -m etl.export_to_parquet [--engine polars|duckdb] [--workers N]
"""
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_ROOT / "data" / "parquet"


def get_dataset_path() -> Path:
    """Dataset elérési útja (kagglehub cache)."""
    import kagglehub
    return Path(kagglehub.dataset_download(
        "debashis74017/stock-market-data-nifty-50-stocks-1-min-data"
    ))


def _process_file(args: tuple) -> tuple[str, bool, str]:
    """
    Egy CSV feldolgozása (worker – ProcessPoolExecutor-ban fut).
    args: (csv_path, parquet_base, symbol, engine)
    Returns: (symbol, success, msg)
    """
    csv_path, parquet_base, symbol, engine = args
    csv_path = Path(csv_path)
    parquet_base = Path(parquet_base)
    try:
        if engine == "duckdb":
            _export_duckdb(csv_path, parquet_base, symbol)
        else:
            _export_polars(csv_path, parquet_base, symbol)
        return (symbol, True, "ok")
    except Exception as e:
        return (symbol, False, str(e))


def _export_polars(csv_path: Path, parquet_base: Path, symbol: str) -> None:
    """Polars: CSV olvasás → symbol oszlop → Parquet írás."""
    import polars as pl

    df = pl.read_csv(csv_path, try_parse_dates=True)
    df = df.with_columns(pl.lit(symbol).alias("symbol"))
    out_dir = parquet_base / f"symbol={symbol}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_dir / "data.parquet")


def _export_duckdb(csv_path: Path, parquet_base: Path, symbol: str) -> None:
    """DuckDB: SQL-lel közvetlen CSV → Parquet."""
    import duckdb

    out_dir = parquet_base / f"symbol={symbol}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        COPY (
            SELECT *, ?::VARCHAR as symbol
            FROM read_csv_auto(?)
        ) TO ? (FORMAT PARQUET)
        """,
        [symbol, str(csv_path), str(out_path)],
    )
    conn.close()


def main(engine: str = "polars", workers: int | None = None):
    """Parquet export – párhuzamosan, Polars vagy DuckDB motorral."""
    if workers is None:
        workers = min(os.cpu_count() or 4, 8)  # max 8 worker

    print(f"Parquet export ({engine}, {workers} worker)...")
    data_path = get_dataset_path()
    csv_files = sorted(data_path.glob("*_minute.csv"))
    total = len(csv_files)

    PARQUET_PATH.mkdir(parents=True, exist_ok=True)

    args_list = [
        (str(f), str(PARQUET_PATH), f.stem.replace("_minute", ""), engine)
        for f in csv_files
    ]

    done = 0
    errors = []

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_file, a): a for a in args_list}
        for future in as_completed(futures):
            symbol, success, msg = future.result()
            done += 1
            status = "ok" if success else f"HIBA: {msg}"
            print(f"  [{done}/{total}] {symbol}... {status}", flush=True)
            if not success:
                errors.append((symbol, msg))

    if errors:
        print(f"\nFigyelmeztetés: {len(errors)} fájl sikertelen.")
    else:
        print(f"\nKész. Parquet: {PARQUET_PATH}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CSV → Parquet export")
    parser.add_argument(
        "--engine",
        choices=["polars", "duckdb"],
        default="polars",
        help="Motor: polars (gyors) vagy duckdb (SQL)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Párhuzamos workerök száma (alap: CPU magok)",
    )
    args = parser.parse_args()
    main(engine=args.engine, workers=args.workers)
