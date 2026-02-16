"""Health check és meta endpointok."""
from fastapi import APIRouter

from ..database import get_conn, get_parquet_glob, parquet_exists, require_parquet
from ..schemas import DateRangeResponse

router = APIRouter(prefix="/api", tags=["System"])


@router.get("/health")
def health():
    """API állapot."""
    return {"status": "ok", "parquet_available": parquet_exists()}


@router.get("/date-range", response_model=DateRangeResponse)
def get_date_range():
    """Legkorábbi és legutolsó dátum az összes adatban (lekérdezhető tartomány)."""
    require_parquet()

    conn = get_conn()
    row = conn.execute(
        "SELECT MIN(date), MAX(date) FROM read_parquet(?)",
        [get_parquet_glob()],
    ).fetchone()
    conn.close()

    return DateRangeResponse(min_date=str(row[0]), max_date=str(row[1]))
