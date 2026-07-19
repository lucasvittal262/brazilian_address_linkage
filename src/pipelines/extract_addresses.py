
from bs4 import BeautifulSoup
import requests
from typing import List
import re

def get_states_folders(year_census: int, base_url: str) -> List[str]:
    census_url = base_url.format(year=year_census)
    page_html = requests.get(census_url).text
    soup = BeautifulSoup(page_html, "html.parser")
    folders = [
        a["href"].rstrip("/")
        for a in soup.find_all("a", href=True)
        if a["href"].endswith("/") and a.get_text(strip=True) != "Parent Directory"
    ]
    pattern = re.compile(r"^\d{2}_[A-Z]{2}$")
    uf_folders = [f for f in folders if pattern.match(f)]
    
    return uf_folders


def get_municipalities_files(state_folder: str) -> List[str]:
    pass

def download_municipality_state_files(state_folder: str, municipality_file: str) -> str:
    pass


if __name__ == "__main__":
    BASE_URL = "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_{year}/Arquivos_CNEFE/CSV/Municipio/"
    state_folders = get_states_folders(2022, BASE_URL)
    print(state_folders)
    print(len(state_folders))