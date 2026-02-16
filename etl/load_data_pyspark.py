"""
Stock Market Data betöltése PySpark-kal
Nifty 50 – 1 perces OHLCV adatok

Előfeltétel: Java 8+ (OpenJDK) telepítve.
  Windows: winget install Microsoft.OpenJDK.17
"""
import os
from pathlib import Path

# JAVA_HOME auto-detekció (Windows – Microsoft OpenJDK)
if not os.environ.get("JAVA_HOME"):
    for base in [
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Microsoft",
        Path("C:\\Program Files\\Eclipse Adoptium"),
    ]:
        if base.exists():
            for jdk in sorted(base.glob("jdk-*"), reverse=True):
                if (jdk / "bin" / "java.exe").exists():
                    os.environ["JAVA_HOME"] = str(jdk)
                    break
            if os.environ.get("JAVA_HOME"):
                break

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType


def get_dataset_path() -> Path:
    """Dataset elérési útja (kagglehub cache vagy letöltés)."""
    import kagglehub
    path = kagglehub.dataset_download(
        "debashis74017/stock-market-data-nifty-50-stocks-1-min-data"
    )
    return Path(path)


def create_spark_session() -> SparkSession:
    """SparkSession létrehozása."""
    return (
        SparkSession.builder
        .appName("Nifty50-StockData")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_stock_data(spark: SparkSession, data_path: Path):
    """
    Összes CSV betöltése PySpark DataFrame-be.
    A szimbólum (pl. RELIANCE, TCS) a fájlnévből kerül a 'symbol' oszlopba.
    """
    from pyspark.sql import functions as F

    csv_files = list(data_path.glob("*_minute.csv"))
    dfs = []
    for csv_file in csv_files:
        symbol = csv_file.stem.replace("_minute", "")
        single_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(csv_file))
        )
        single_df = (
            single_df.withColumn("date", F.to_timestamp("date"))
            .withColumn("open", F.col("open").cast(DoubleType()))
            .withColumn("high", F.col("high").cast(DoubleType()))
            .withColumn("low", F.col("low").cast(DoubleType()))
            .withColumn("close", F.col("close").cast(DoubleType()))
            .withColumn("volume", F.col("volume").cast(LongType()))
            .withColumn("symbol", F.lit(symbol))
        )
        dfs.append(single_df)

    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d)

    return df


def main():
    print("Spark session inicializálása...")
    spark = create_spark_session()

    print("Dataset útvonal betöltése...")
    data_path = get_dataset_path()
    print(f"Adatok: {data_path}")

    print("CSV fájlok betöltése PySpark-kal...")
    df = load_stock_data(spark, data_path)

    df.cache()
    print(f"\nSorok száma: {df.count():,}")
    print(f"Egyedi szimbólumok: {df.select('symbol').distinct().count()}")
    print("\nSchema:")
    df.printSchema()
    print("\nElső 5 sor:")
    df.show(5, truncate=False)

    return spark, df


if __name__ == "__main__":
    spark, df = main()
