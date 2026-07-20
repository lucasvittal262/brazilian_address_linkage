import logging
import os
import re
import zipfile
from typing import Dict, List

import requests
import urllib.request
from bs4 import BeautifulSoup
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s]: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_states_folders(year_census: int, base_url: str) -> List[str]:
    census_url = base_url.format(year=year_census)
    logger.info(f"Fetching state folder listing for {year_census} from {census_url}")

    page_html = requests.get(census_url).text
    soup = BeautifulSoup(page_html, "html.parser")

    folders = [
        a["href"].rstrip("/")
        for a in soup.find_all("a", href=True)
        if a["href"].endswith("/") and a.get_text(strip=True) != "Parent Directory"
    ]

    pattern = re.compile(r"^\d{2}_[A-Z]{2}$")
    uf_folders = [f for f in folders if pattern.match(f)]

    logger.info(f"Found {len(uf_folders)} state folders: {uf_folders}")
    return uf_folders


def get_municipalities_files_urls(base_url: str, state_folder: str, year_census: int) -> List[str]:
    census_url = base_url.format(year=year_census)
    state_url = f"{census_url}/{state_folder}/"
    logger.info(f"[{state_folder}] Fetching municipality file list from {state_url}")

    page_html = requests.get(state_url).text
    soup = BeautifulSoup(page_html, "html.parser")

    files = [
        state_url + a["href"].rstrip("/")
        for a in soup.find_all("a", href=True)
        if a["href"].endswith(".zip") and a.get_text(strip=True) != "Parent Directory"
    ]

    logger.info(f"[{state_folder}] Found {len(files)} zip file(s)")
    return files


def unzip_file(zip_file_path: str, extract_to: str) -> None:
    logger.info(f"Extracting {os.path.basename(zip_file_path)} -> {extract_to}")
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    logger.info(f"Finished extracting {os.path.basename(zip_file_path)}")


def download_municipality_state_files(state_folder: str, municipality_file: str, raw_data_dir: str) -> str:
    state_dir = f"{raw_data_dir}/{state_folder}"
    os.makedirs(state_dir, exist_ok=True)

    file_name = os.path.basename(municipality_file)
    file_path = f"{state_dir}/{file_name}"

    logger.info(f"[{state_folder}] Downloading {file_name} from {municipality_file}")
    urllib.request.urlretrieve(municipality_file, file_path)
    logger.info(f"[{state_folder}] Download complete: {file_name}")

    unzip_file(file_path, state_dir)

    os.remove(file_path)
    #logger.info(f"[{state_folder}] Removed zip file after extraction: {file_name}")

    return file_path


def download_all_municipality_state_files(
    state_folders: List[str], raw_data_dir: str
) -> None:
    logger.info(f"Starting download for {len(state_folders)} state(s) into '{raw_data_dir}'")

    for state_folder in tqdm(state_folders, desc="States", unit="state"):
        files_for_state =get_municipalities_files_urls(BASE_URL, state_folder, 2022)
           
        for municipality_file in tqdm(
            files_for_state,
            desc=f"{state_folder}",
            unit="file",
            leave=False,
        ):
            print(municipality_file)
            download_municipality_state_files(state_folder, municipality_file, raw_data_dir)

    logger.info("All states processed successfully.")
if __name__ == "__main__":
    import time
    
    start_time = time.time()
    BASE_URL = "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_{year}/Arquivos_CNEFE/CSV/Municipio"
    RAW_DATA_DIR = "data/raw"

    state_folders = get_states_folders(2022, BASE_URL)
    download_all_municipality_state_files(state_folders,  RAW_DATA_DIR)

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")