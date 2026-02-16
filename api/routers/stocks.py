"""Részvényadatok és statisztikák endpointok."""
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from ..database import get_conn, get_parquet_glob, get_parquet_glob_for_symbol, require_parquet
from ..schemas import OHLCVRow, StatsResponse, StockResponse

router = APIRouter(prefix="/api/stocks", tags=["Stocks"])


def _build_where_clause(start: date | None, end: date | None) -> tuple[str, list]:
    """WHERE feltételek építése (symbol nélkül – partition már szűr)."""
    where_parts = []
    params: list = []
    if start:
        where_parts.append("date >= ?")
        params.append(str(start))
    if end:
        where_parts.append("date <= ?")
        params.append(str(end))
    where_sql = " AND ".join(where_parts) if where_parts else "1=1"
    return where_sql, params


def _get_parquet_and_where(symbol: str, start: date | None, end: date | None) -> tuple[str, str, list]:
    """Parquet path és WHERE – symbol partition vagy full scan."""
    symbol = symbol.upper().strip()
    path = get_parquet_glob_for_symbol(symbol)
    if path:
        where_sql, where_params = _build_where_clause(start, end)
        return path, where_sql, where_params
    path = get_parquet_glob()
    where_sql, where_params = _build_where_clause(start, end)
    return path, f"symbol = ? AND ({where_sql})", [symbol, *where_params]


@router.get("/{symbol}", response_model=StockResponse)
def get_stocks(
    symbol: str,
    start: date | None = Query(None),
    end: date | None = Query(None),
    limit: int = Query(1000, ge=1, le=2_000_000),
    offset: int = Query(0, ge=0),
):
    """OHLCV adatok lekérése szimbólum és dátum szerint."""
    require_parquet()

    symbol = symbol.upper().strip()
    parquet_path, where_sql, where_params = _get_parquet_and_where(symbol, start, end)
    params = [parquet_path, *where_params, limit, offset]

    conn = get_conn()
    query = f"""
        SELECT date, open, high, low, close, volume
        FROM read_parquet(?)
        WHERE {where_sql}
        ORDER BY date
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [
        OHLCVRow(date=r[0], open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5])
        for r in rows
    ]
    return {"symbol": symbol, "data": data, "count": len(data)}


@router.get("/{symbol}/stats", response_model=StatsResponse)
def get_stats(
    symbol: str,
    start: date | None = Query(None),
    end: date | None = Query(None),
):
    """Statisztikák egy szimbólumhoz."""
    require_parquet()

    symbol = symbol.upper().strip()
    parquet_path, where_sql, where_params = _get_parquet_and_where(symbol, start, end)
    params = [parquet_path, *where_params]

    conn = get_conn()
    query = f"""
        SELECT
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as row_count,
            MIN(low) as min_price,
            MAX(high) as max_price
        FROM read_parquet(?)
        WHERE {where_sql}
    """
    row = conn.execute(query, params).fetchone()
    conn.close()

    if not row or row[2] == 0:
        raise HTTPException(404, f"Nincs adat a(z) {symbol} szimbólumhoz.")

    return StatsResponse(
        symbol=symbol,
        date_range={"min": str(row[0]), "max": str(row[1])},
        row_count=row[2],
        price_range={"min": row[3], "max": row[4]},
    )
