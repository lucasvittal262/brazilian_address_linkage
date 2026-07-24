import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import LongType, StructField, StructType, StringType, IntegerType, DoubleType
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cnefe_integration")


def list_folders(raw_data_dir: str) -> list:
    logger.info(f"Scanning raw data directory: {raw_data_dir}")
    folders = [f"{raw_data_dir}/{folder}" for folder in os.listdir(raw_data_dir)]
    logger.info(f"Found {len(folders)} folders to process")
    return folders


def integrate_municipality_data(raw_data_dir: str, schema: StructType) -> DataFrame:
    spark = SparkSession.builder.appName("Integrating Data").getOrCreate()

    folders = list_folders(raw_data_dir)

    dfs = []
    pbar = tqdm(folders, desc="Loading municipality data", unit="folder")
    for folder_path in pbar:
        folder_name = os.path.basename(folder_path)
        state_label = folder_name.split("_")[1]
        pbar.set_postfix_str(f"UF={state_label}")

        logger.info(f"Reading CSV files from '{folder_path}' (UF={state_label})")
        df_folder = spark.read.csv(folder_path, header=True, schema=schema, sep=";")
        df_folder = df_folder.withColumn("UF", lit(state_label))

        rows = df_folder.count()
        logger.info(f"Got {rows:,} rows for Federation Unit '{state_label}'")

        dfs.append(df_folder)

    logger.info("Unioning all municipality DataFrames together...")
    # unionByName is safer than union() if column order ever differs between files
    integrated_df = dfs[0]
    for df in tqdm(dfs[1:], desc="Merging DataFrames", unit="df"):
        integrated_df = integrated_df.unionByName(df)

    logger.info("Integration complete. Final schema:")
    integrated_df.printSchema()

    total_rows = integrated_df.count()
    logger.info(f"Total rows in the integrated DataFrame: {total_rows:,}")

    return integrated_df


def save_integrated_data(integrated_df: DataFrame, output_file_path: str) -> None:
    output_dir = os.path.dirname(output_file_path)
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Writing integrated data to '{output_file_path}' (mode=overwrite)...")
    integrated_df.write.mode("overwrite").parquet(output_file_path)
    logger.info(f"✅ Integrated data saved to {output_file_path}")


if __name__ == "__main__":
    import time
    
    start_time = time.time()
    spark = SparkSession.builder.appName("Integrating Data").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # quiet down Spark's own noisy logs

    RAW_DATA_DIR = "data/raw"
    OUTPUT_FILE_PATH = "data/integrated/integrated_cnefe_addresses.parquet"

    CNEFE_SCHEMA = StructType([
        StructField("COD_UNICO_ENDERECO", LongType(), True),
        StructField("COD_UF", IntegerType(), True),
        StructField("COD_MUNICIPIO", LongType(), True),
        StructField("COD_DISTRITO", LongType(), True),
        StructField("COD_SUBDISTRITO", LongType(), True),
        StructField("COD_SETOR", StringType(), True),
        StructField("NUM_QUADRA", IntegerType(), True),
        StructField("NUM_FACE", IntegerType(), True),
        StructField("CEP", StringType(), True),
        StructField("DSC_LOCALIDADE", StringType(), True),
        StructField("NOM_TIPO_SEGLOGR", StringType(), True),
        StructField("NOM_TITULO_SEGLOGR", StringType(), True),
        StructField("NOM_SEGLOGR", StringType(), True),
        StructField("NUM_ENDERECO", IntegerType(), True),
        StructField("DSC_MODIFICADOR", StringType(), True),
        StructField("NOM_COMP_ELEM1", StringType(), True),
        StructField("VAL_COMP_ELEM1", StringType(), True),
        StructField("NOM_COMP_ELEM2", StringType(), True),
        StructField("VAL_COMP_ELEM2", StringType(), True),
        StructField("NOM_COMP_ELEM3", StringType(), True),
        StructField("VAL_COMP_ELEM3", StringType(), True),
        StructField("NOM_COMP_ELEM4", StringType(), True),
        StructField("VAL_COMP_ELEM4", StringType(), True),
        StructField("NOM_COMP_ELEM5", StringType(), True),
        StructField("VAL_COMP_ELEM5", StringType(), True),
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        StructField("NV_GEO_COORD", IntegerType(), True),
        StructField("COD_ESPECIE", IntegerType(), True),
        StructField("DSC_ESTABELECIMENTO", StringType(), True),
        StructField("COD_INDICADOR_ESTAB_ENDERECO", StringType(), True),
        StructField("COD_INDICADOR_CONST_ENDERECO", StringType(), True),
        StructField("COD_INDICADOR_FINALIDADE_CONST", StringType(), True),
        StructField("COD_TIPO_ESPECI", IntegerType(), True),
    ])

    logger.info("=== Starting CNEFE data integration pipeline ===")
    integrated_df = integrate_municipality_data(RAW_DATA_DIR, CNEFE_SCHEMA)
    save_integrated_data(integrated_df, OUTPUT_FILE_PATH)
    logger.info("=== Pipeline finished, stopping Spark session ===")
    spark.stop()
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Total elapsed time: {elapsed_time:.2f} seconds")
    