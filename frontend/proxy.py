"""Proxy – lekérdezés a 8000-es portú adat API-ról."""
import time
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from .config import DATA_API_URL

router = APIRouter(tags=["Proxy"])

_CACHE_TTL = 300  # 5 perc
_cache: dict[str, tuple[Any, float]] = {}

# Megosztott client – connection pooling, kevesebb TCP handshake
_http_client: httpx.AsyncClient | None = None


async def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(
            timeout=60.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
    return _http_client


async def _fetch(path: str, params: dict[str, Any] | None = None) -> dict | list:
    """HTTP GET a data API-ra. 4xx/5xx hibákat HTTPException-ként továbbadja."""
    url = f"{DATA_API_URL.rstrip('/')}{path}"
    client = await _get_client()
    resp = await client.get(url, params=params)
    if resp.is_error:
        try:
            body = resp.json()
            detail = body.get("detail", body) if isinstance(body, dict) else body
        except Exception:
            detail = resp.text or f"HTTP {resp.status_code}"
        raise HTTPException(resp.status_code, detail)
    return resp.json()


async def _cached_fetch(key: str, path: str, params: dict | None = None) -> dict | list:
    """Cache-elt fetch – symbols, date-range ritkán változnak."""
    now = time.time()
    if key in _cache:
        data, ts = _cache[key]
        if now - ts < _CACHE_TTL:
            return data
    data = await _fetch(path, params)
    _cache[key] = (data, now)
    return data


@router.get("/api/health")
async def proxy_health():
    """Data API állapot proxy."""
    try:
        return await _fetch("/api/health")
    except httpx.RequestError as e:
        raise HTTPException(502, f"Data API nem elérhető: {e}")


@router.get("/api/date-range")
async def proxy_date_range():
    """Globális dátumtartomány proxy (cache-elve)."""
    try:
        return await _cached_fetch("date_range", "/api/date-range")
    except httpx.RequestError as e:
        raise HTTPException(502, f"Data API nem elérhető: {e}")


@router.get("/api/symbols")
async def proxy_symbols():
    """Szimbólumok listája proxy (cache-elve)."""
    try:
        return await _cached_fetch("symbols", "/api/symbols")
    except httpx.RequestError as e:
        raise HTTPException(502, f"Data API nem elérhető: {e}")


@router.get("/api/stocks/{symbol}")
async def proxy_stocks(
    symbol: str,
    start: str | None = Query(None),
    end: str | None = Query(None),
    limit: int = Query(1000, ge=1, le=2_000_000),
    offset: int = Query(0, ge=0),
):
    """Részvényadatok proxy."""
    params = {"limit": limit, "offset": offset}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    try:
        return await _fetch(f"/api/stocks/{symbol}", params)
    except httpx.RequestError as e:
        raise HTTPException(502, f"Data API nem elérhető: {e}")


@router.get("/api/stocks/{symbol}/stats")
async def proxy_stats(
    symbol: str,
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Statisztikák proxy. 404 = nincs adat a dátumtartományban."""
    params = {k: v for k, v in [("start", start), ("end", end)] if v}
    try:
        return await _fetch(f"/api/stocks/{symbol}/stats", params or None)
    except httpx.HTTPStatusError as e:
        try:
            detail = e.response.json().get("detail", e.response.text)
        except Exception:
            detail = e.response.text or str(e)
        raise HTTPException(e.response.status_code, detail)
    except httpx.RequestError as e:
        raise HTTPException(502, f"Data API nem elérhető: {e}")
