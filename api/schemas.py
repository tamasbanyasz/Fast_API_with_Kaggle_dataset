"""Pydantic modellek – request/response séma."""
from datetime import datetime

from pydantic import BaseModel


class OHLCVRow(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class StockResponse(BaseModel):
    symbol: str
    data: list[OHLCVRow]
    count: int


class SymbolsResponse(BaseModel):
    symbols: list[str]
    count: int


class StatsResponse(BaseModel):
    symbol: str
    date_range: dict
    row_count: int
    price_range: dict


class DateRangeResponse(BaseModel):
    """Globális dátumtartomány az összes adatra."""
    min_date: str
    max_date: str
