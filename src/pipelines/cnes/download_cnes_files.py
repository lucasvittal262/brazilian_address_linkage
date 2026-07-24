import logging
import os
import urllib.request
from datetime import date
from time import time
from typing import List, Tuple

from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import zipfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("cnes_download.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("download_cnes_files.py")


class TqdmUpTo(tqdm):
    def update_to(self, blocks_transferred=1, block_size=1, total_size=None):
        if total_size is not None:
            self.total = total_size
        self.update(blocks_transferred * block_size - self.n)


def generate_year_months(start_date, end_date) -> List[str]:
    """Generate a list of 'YYYYMM' strings between start_date and end_date (inclusive)."""
    logger.info("Generating year-month range from %s to %s", start_date, end_date)

    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)

    current = date(start_date.year, start_date.month, 1)
    end = date(end_date.year, end_date.month, 1)

    year_months = []
    while current <= end:
        year_months.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)

    logger.info("Generated %d year-month periods", len(year_months))
    logger.debug("Year-months: %s", year_months)
    return year_months


def get_not_downloaded_year_months(year_months: List[str], output_dir: str) -> List[str]:
    """Return a list of year_months that have not yet been downloaded to output_dir."""
    logger.info("Checking for already downloaded files in %s", output_dir)

    not_downloaded = []
    for year_month in year_months:
        expected_file = os.path.join(output_dir, f"BASE_DE_DADOS_CNES_{year_month}")
        if not os.path.isdir(expected_file):
            not_downloaded.append(year_month)

    logger.info(
        "%d out of %d files need to be downloaded",
        len(not_downloaded),
        len(year_months),
    )
    return not_downloaded 


def download_cnes_file(year_month: str, output_dir: str) -> None:
    """Download a single CNES ZIP file for the given year_month, showing a progress bar."""
    url = f"https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_{year_month}.ZIP"
    output_file = f"{output_dir}/BASE_DE_DADOS_CNES_{year_month}.ZIP"

    logger.info("Starting download for %s -> %s", year_month, output_file)

    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with TqdmUpTo(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            desc=f"Downloading BASE_DE_DADOS_CNES_{year_month}.zip",
        ) as t:
            urllib.request.urlretrieve(url, output_file, reporthook=t.update_to)

        logger.info("Successfully downloaded %s", year_month)

    except Exception as err:
        logger.error("Failed to download %s: %s", year_month, err, exc_info=True)
        raise err


def download_cnes_files(year_months: List[str], output_dir: str) -> Tuple[int, int]:
    """Download multiple CNES files, tracking successes and failures."""
    logger.info("Starting batch download of %d files", len(year_months))

    count_success = 0
    count_failures = 0

    for year_month in tqdm(year_months, desc="Overall progress", unit="file"):
        try:
            download_cnes_file(year_month, output_dir)
            count_success += 1
        except Exception as err:
            logger.error("Error occurred while downloading %s: %s", year_month, err)
            count_failures += 1

    logger.info(
        "Batch download finished: %d succeeded, %d failed",
        count_success,
        count_failures,
    )
    return count_success, count_failures


def list_zip_files(input_folder: str) -> List[str]:
    """Return a sorted list of full paths to .zip files found in input_folder."""
    logger.info("Listing zip files in %s", input_folder)

    if not os.path.isdir(input_folder):
        logger.warning("Input folder does not exist: %s", input_folder)
        return []

    zip_files = sorted(
        os.path.join(input_folder, f)
        for f in os.listdir(input_folder)
        if f.lower().endswith(".zip")
    )

    logger.info("Found %d zip file(s) in %s", len(zip_files), input_folder)
    logger.debug("Zip files: %s", zip_files)
    return zip_files


def extract_zip_file(zip_path: str, output_folder: str) -> None:
    """Extract a single zip file into output_folder, showing a progress bar for its members."""
    logger.info("Extracting %s -> %s", zip_path, output_folder)

    try:
        folder_name = os.path.splitext(os.path.basename(zip_path))[0].replace(".zip", "")
        os.makedirs(os.path.join(output_folder, folder_name), exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            members = zip_ref.namelist()
            for member in tqdm(
                members,
                desc=f"Extracting {os.path.basename(zip_path)}",
                unit="file",
            ):
                zip_ref.extract(member, os.path.join(output_folder, folder_name))

        logger.info("Successfully extracted %s (%d files)", zip_path, len(members))

    except Exception as err:
        logger.error("Failed to extract %s: %s", zip_path, err, exc_info=True)
        raise err


def extract_and_clean_zips(output_folder: str) -> Tuple[int, int]:
    """
    Extract every zip file found in input_folder into output_folder,
    deleting each zip after a successful extraction.

    Returns a tuple of (count_success, count_failures).
    """
    zip_files = list_zip_files(output_folder)
    logger.info("Starting extraction of %d zip file(s)", len(zip_files))

    count_success = 0
    count_failures = 0

    for zip_path in tqdm(zip_files, desc="Overall extraction progress", unit="zip"):
        try:
            extract_zip_file(zip_path, output_folder)
            os.remove(zip_path)
            logger.info("Deleted zip file after extraction: %s", zip_path)
            count_success += 1
        except Exception as err:
            logger.error("Error occurred while processing %s: %s", zip_path, err)
            count_failures += 1

    logger.info(
        "Extraction finished: %d succeeded, %d failed",
        count_success,
        count_failures,
    )
    return count_success, count_failures


if __name__ == "__main__":
    OUTPUT_DIR = "data/raw/cnes"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    time_start = time()

    START_DATE = "2017-01-01"
    END_DATE = date.today().strftime("%Y-%m-%d")
    BASE_URL = "https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_{year_month}.ZIP"

    logger.info("Script started | START_DATE=%s | END_DATE=%s", START_DATE, END_DATE)

    year_months = generate_year_months(START_DATE, END_DATE)
    not_downloaded_year_months = get_not_downloaded_year_months(year_months, OUTPUT_DIR)
    success, failures = download_cnes_files(not_downloaded_year_months, output_dir=OUTPUT_DIR)
    extracted_success, extracted_failures = extract_and_clean_zips(OUTPUT_DIR)

    elapsed_time = time() - time_start

    logger.info("Script finished in %.2f seconds", elapsed_time)
    logger.info("Successful downloads: %d", success)
    logger.info("Failed downloads: %d", failures)

    print(f"Successful downloads: {success}")
    print(f"Failed downloads: {failures}")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
