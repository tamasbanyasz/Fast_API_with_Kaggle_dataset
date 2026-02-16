"""Szimbólumok listázása."""
from fastapi import APIRouter

from ..database import get_conn, get_parquet_glob, require_parquet
from ..schemas import SymbolsResponse

router = APIRouter(prefix="/api", tags=["Symbols"])


@router.get("/symbols", response_model=SymbolsResponse)
def list_symbols():
    """Elérhető szimbólumok listája."""
    require_parquet()

    conn = get_conn()
    result = conn.execute(
        "SELECT DISTINCT symbol FROM read_parquet(?) ORDER BY symbol",
        [get_parquet_glob()],
    ).fetchall()
    conn.close()

    symbols = [r[0] for r in result]
    return {"symbols": symbols, "count": len(symbols)}
