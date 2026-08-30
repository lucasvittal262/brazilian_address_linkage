from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pathlib import Path
from time import time
import logging
import os


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_data_from_bq(
    spark: SparkSession,
    project_id: str,
    dataset: str,
    table: str,
    date_column: str,
    year: int,
    month: int
) -> DataFrame:

    full_table_name = f"{project_id}.{dataset}.{table}"
    logger.info(f"📊 Starting data fetch from BigQuery table '{full_table_name}'...")
    logger.info(f"🔎 Getting data for {month}-{year}")

    query = f"""
        SELECT *
        FROM `{full_table_name}`
        WHERE EXTRACT(MONTH FROM {date_column}) = {month}
        AND EXTRACT(YEAR FROM {date_column}) = {year}
    """

    start_time = time()
    try:
        df = (
            spark.read.format("bigquery")
            .option("query", query)
            .option("viewsEnabled", "true")
            .load()
        )
        elapsed = time() - start_time
        logger.info(
            f"✅ Successfully loaded data from '{full_table_name}' in {elapsed:.2f}s"
        )
        return df
    except Exception as e:
        logger.error(f"❌ Failed to load data from '{full_table_name}': {e}")
        raise


def save_to_parquet(df: DataFrame, path: str):
    logger.info(f"💾 Saving DataFrame to parquet at '{path}'...")

    start_time = time()
    try:
        df.write.mode("overwrite").parquet(path)
        elapsed = time() - start_time
        logger.info(f"✅ Data successfully saved to '{path}' in {elapsed:.2f}s")
    except Exception as e:
        logger.error(f"❌ Failed to save data to '{path}': {e}")
        raise

import logging

logger = logging.getLogger(__name__)


def extract_all_data(
    spark,
    output_dir,
    ano_min,
    ano_max,
    months,
    label,
    project_id,
    dataset_name,
    table,
    date_column,
):
    ano_min, ano_max = int(ano_min), int(ano_max)
    total = (ano_max - ano_min + 1) * months

    logger.info(f"🚀 Starting extraction of '{table}' ({ano_min}-{ano_max}, {total} files to go)")

    count = 0
    for year in range(ano_min, ano_max + 1):
        for month in range(1, months + 1):
            count += 1
            output_path = output_dir / f"extracted_data_{label}_{table}_{year}_{month}.parquet"

            logger.info(f"📥 ({count}/{total}) Fetching {year}-{month:02d}...")

            try:
                df = get_data_from_bq(
                    spark, project_id, dataset_name, table, date_column, year=year, month=month
                )
                save_to_parquet(df, str(output_path))
                logger.info(f"✅ ({count}/{total}) Saved {output_path.name}")
            except Exception:
                logger.exception(f"❌ ({count}/{total}) Failed on {year}-{month:02d}")
                raise

    logger.info(f"🎉 Done! {count} files saved to {output_dir}")
    
if __name__ == "__main__":
    ANO_MIN = "2021"
    ANO_MAX = "2026"
    PROJECT_ID = "basedosdados"
    DATASET_NAME = "br_rf_cnpj"
    TABLE = "estabelecimentos"
    LABEL = "cnpj"
    DATE_COLUMN = "data_referencia"
    MONTHS = 12

    time_start = time()
    spark = (
        SparkSession.builder.appName(f"Extract {LABEL.upper()} Data")
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-4.0-bigquery:0.44.2,"
            "javax.inject:javax.inject:1",
        )
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "8g")
        .getOrCreate()
    )

    project_path = Path(__file__).resolve().parent.parent.parent.parent
    output_dir = Path(project_path) / "data" / "raw" / LABEL
    print(f"Output directory: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    extract_all_data(
        spark,
        output_dir,
        ANO_MIN,
        ANO_MAX,
        MONTHS,
        LABEL,
        PROJECT_ID,
        DATASET_NAME,
        TABLE,
        DATE_COLUMN,
    )

    spark.stop()
    time_end = time()
    print(f"Execution time: {time_end - time_start: 0.2f} seconds")
