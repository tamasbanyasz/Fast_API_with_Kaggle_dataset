"""
Stock Market Data letöltése a Kaggle-ról
Dataset: debashis74017/stock-market-data-nifty-50-stocks-1-min-data
"""
import kagglehub

# Legutolsó verzió letöltése
path = kagglehub.dataset_download("debashis74017/stock-market-data-nifty-50-stocks-1-min-data")

print("A dataset fájljainak elérési útja:", path)
