# Nifty 50 Stock – Frontend

Port **8001** – lekérdezi a Data API (8000) endpointjait és megjeleníti az adatokat.

## Előfeltétel

A **Data API** (port 8000) futnia kell:
```bash
python -m uvicorn api.main:app --port 8000
```

## Futtatás

```bash
python -m uvicorn frontend.main:app --port 8001
```

Vagy: `python -m frontend.main`

## Használat

- **Főoldal:** http://localhost:8001 – interaktív UI (szimbólum, dátum, grafikon, táblázat)
- **Proxy API:** ugyanazok az endpointok mint a 8000-n, de a 8001-en keresztül (pl. `/api/symbols`, `/api/stocks/RELIANCE`)

## Architektúra

```
Böngésző  ←→  Frontend (8001)  ←→  Data API (8000)  ←→  Parquet (DuckDB)
```

A frontend proxy-ként működik: a böngésző a 8001-et hívja, a 8001 pedig a 8000-et.
