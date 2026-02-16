"""DuckDB adatelérés – Parquet lekérdezések."""
import duckdb
from fastapi import HTTPException

from .config import PARQUET_PATH


def get_conn():
    """DuckDB kapcsolat – Parquet lekérdezéshez."""
    return duckdb.connect(":memory:")


def parquet_exists() -> bool:
    """Parquet mappa létezik és tartalmaz fájlokat."""
    if not PARQUET_PATH.exists():
        return False
    return any(PARQUET_PATH.rglob("*.parquet"))


def require_parquet():
    """Dependency: hibát dob, ha a Parquet adat nem elérhető."""
    if not parquet_exists():
        raise HTTPException(
            503,
            "Parquet adatok még nem elérhetők. Futtasd: python etl/export_to_parquet.py",
        )


def get_parquet_glob() -> str:
    """Parquet fájlok glob útvonala (minden szimbólum)."""
    return str(PARQUET_PATH / "**" / "*.parquet").replace("\\", "/")


def get_parquet_glob_for_symbol(symbol: str) -> str | None:
    """Parquet glob csak egy szimbólumhoz – gyorsabb, kevesebb I/O."""
    p = PARQUET_PATH / f"symbol={symbol.upper().strip()}" / "*.parquet"
    path_str = str(p).replace("\\", "/")
    return path_str if p.parent.exists() else None
