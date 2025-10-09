import time
import zipfile
import os
from pathlib import Path
from urllib import error, request


# =================== Dowload ===================
def dowload(url: str, destino_zip: Path) -> Path:
    destino_zip = Path(destino_zip)
    host = request.urlparse(url).hostname or ""
    print(f"Host: {host}")
    print(url)
    req = request.Request(url, headers={"User-Agent": "CNES-PIN/1.0"})
    print(req.text)

# =================== Execução ===================

def main() -> None:
    url = "https://www.gov.br/cnes/pt-br/centrais-de-conteudo/downloads/arquivos-de-cnes/arquivos-de-cnes-2024/2024-06/BASE_DE_DADOS_CNES_06_2024.zip"
    destino_zip = Path("BASE_DE_DADOS_CNES_06_2024.zip")
    destino_dir = Path("cnes_data")

    arq = dowload(url, destino_zip)

if __name__ == "__main__":
    main()
    print("Fim")