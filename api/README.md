# Nifty 50 Stock API

FastAPI backend a Parquetben tárolt részvényadatok lekérdezéséhez.

## Előkészítés

1. **Parquet export** (egyszer, a projekt gyökerében):
   ```bash
   python etl/export_to_parquet.py
   ```

2. **Függőségek**:
   ```bash
   pip install fastapi uvicorn duckdb
   ```

## Futtatás

Projekt gyökeréből:
```bash
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Vagy: `python -m api.main`

## API végpontok

| Metódus | Endpoint | Leírás |
|---------|----------|--------|
| GET | `/api/health` | API állapot |
| GET | `/api/date-range` | Legkorábbi és legutolsó dátum (lekérdezhető tartomány) |
| GET | `/api/symbols` | Szimbólumok listája |
| GET | `/api/stocks/{symbol}` | OHLCV adatok (start, end, limit, offset) |
| GET | `/api/stocks/{symbol}/stats` | Statisztikák |

**Példák:**
- `http://localhost:8000/api/symbols`
- `http://localhost:8000/api/stocks/RELIANCE?limit=100`
- `http://localhost:8000/api/stocks/TCS?start=2024-01-01&end=2024-01-31`
- `http://localhost:8000/docs` – Swagger UI
