"""
Nifty 50 Stock – Frontend
Port 8001 – lekérdezi a 8000-es API-t és megjeleníti az adatokat.
"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .proxy import router as proxy_router

app = FastAPI(
    title="Nifty 50 Stock – Frontend",
    description="Adatok megjelenítése a Data API (8000) alapján",
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

app.include_router(proxy_router)


@app.get("/", response_class=HTMLResponse)
def index():
    """Főoldal – adatok megjelenítése."""
    html = Path(__file__).parent / "index.html"
    return HTMLResponse(html.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
