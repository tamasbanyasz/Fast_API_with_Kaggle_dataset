"""API konfiguráció – útvonalak, állandók."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_PATH = PROJECT_ROOT / "data" / "parquet"
