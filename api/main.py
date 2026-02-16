"""
Nifty 50 Stock Data API – FastAPI
Parquet adatok olvasása DuckDB-vel.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .routers import health_router, stocks_router, symbols_router

app = FastAPI(
    title="Nifty 50 Stock API",
    description="OHLCV adatok lekérdezése Parquet-ból",
    version="1.0.0",
)

app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(symbols_router)
app.include_router(stocks_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
