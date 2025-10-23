#!/usr/bin/env python
# coding: utf-8

# ## CNES_Utils

# ## Imports
import zipfile as z
import os
import requests
from pathlib import Path
from datetime import datetime, timedelta
import re
import certifi
from urllib.parse import urljoin


# ### Criando pasta tmp para download
def FazerDownload(origem, destino):
    """
    Faz o download de um arquivo a partir de uma URL de origem para o caminho de destino.
    """
    data = requests.get(origem, verify=True)
    with open(destino, 'wb') as file:
        file.write(data.content)

def FormatarURL(url_base: str, data: datetime) -> str:
    """
    Gera a URL completa do arquivo ZIP CNES a partir de uma data.

    Parâmetros
    ----------
    url_base : str
        URL base do serviço. Ex: "https://cnes.datasus.gov.br/EstatisticasServlet?path="
    data : datetime
        Data desejada para formatar no padrão YYYYMM.

    Retorna
    -------
    str
        URL completa para download do ZIP.
    """
    nome_arquivo = f"BASE_DE_DADOS_CNES_{data}.ZIP"
    return f"{url_base}{nome_arquivo}"

def ObterArquivosCNES(url_base, html):
    """
    Extrai os links dos arquivos ZIP CNES a partir do bloco HTML da dropdown.

    Parâmetros
    ----------
    url_base : str
        Ex: "https://cnes.datasus.gov.br"
    html : str
        HTML da página já carregada com BeautifulSoup ou raw.

    Retorna
    -------
    tuple
        (lista_urls_completas, url_mais_recente, anoMes_mais_recente)
    """
    pattern = r'href="(/EstatisticasServlet\?path=BASE_DE_DADOS_CNES_(\d{6})\.ZIP)"'
    matches = re.findall(pattern, html)
    print("[DEBUG] Matches encontrados:", matches)

    if not matches:
        raise ValueError("Nenhum arquivo ZIP CNES encontrado no HTML fornecido.")

    urls_completas = [urljoin(url_base, m[0]) for m in matches]

    matches.sort(key=lambda x: x[1], reverse=True)
    anoMes_recente = matches[0][1]

    url_mais_recente_relativa = next(m[0] for m in matches if m[1] == anoMes_recente)
    url_mais_recente = urljoin(url_base, url_mais_recente_relativa)

    return urls_completas, url_mais_recente, anoMes_recente

def SalvarZipURLCNES(url, tempDiretorio, datalakeDestino, listZips, data):
    """
    Faz o download de um arquivo ZIP CNES, extrai os arquivos relacionados a 'estabelecimentos'
    e envia os CSVs extraídos para o destino especificado no datalake.

    Parâmetros
    ----------
    url : str
        URL base para formar o caminho do ZIP.
    tempDiretorio : str
        Diretório temporário local para salvar e extrair os arquivos.
    datalakeDestino : str
        Caminho no datalake onde os arquivos CSV extraídos devem ser copiados.
    listZips : str
        Nome base do arquivo ZIP, sem extensão.
    data : str
        Data no formato 'YYYYMM' para compor o nome do arquivo ZIP.
    """
    urlAtual = FormatarURL(url, data)
    print(f"[INFO] URL de download: {urlAtual}")

    nomeArquivo = listZips
    diretorioZip = os.path.join(tempDiretorio, nomeArquivo, "ZIP")
    diretorioCSV = os.path.join(tempDiretorio, nomeArquivo, "CSV")

    os.makedirs(diretorioZip, exist_ok=True)
    os.makedirs(diretorioCSV, exist_ok=True)

    pathZip = os.path.join(diretorioZip, nomeArquivo + ".zip")

    # Fazer o download do ZIP
    FazerDownload(urlAtual, pathZip)

    with z.ZipFile(pathZip, 'r') as root:
        arquivos_zip = root.namelist()
        encontrados = [a for a in arquivos_zip if "tbestabelecimento" in a.lower()]

        if not encontrados:
            print("⚠️ Nenhum arquivo com 'estabelecimentos' encontrado no ZIP.")
        else:
            for arquivo in encontrados:
                print(f"🗂️ Extraindo: {arquivo}")
                root.extract(arquivo, diretorioCSV)

    # Copiar para datalake (assumindo ambiente mssparkutils)
    print(f"[INFO] Copiando arquivos extraídos para: {datalakeDestino}")
    #mssparkutils.fs.cp(
    #    src='file:' + diretorioCSV,
    #    dest=datalakeDestino,
    #    recurse=True
    #)

# ## APAGAR: Códigos auxiliares para execução interativa

# ## Execução
# Obter a data e hora atuais menos 1200 horas (~50 dias atrás)
now = datetime.now() - timedelta(hours=1200)
mesAno = now.strftime("%Y%m")

urlBase = "https://cnes.datasus.gov.br/EstatisticasServlet?path="
urlFinal = FormatarURL(urlBase, mesAno)
print(urlFinal)

# Definir caminhos
pathCSV = "URL/CNES/Estabelecimentos/"
pathTemp = '/tmp/CNES/'
pathBR = 'URL/CNES/Estabelecimentos'
listaArquivo = f'BASE_DE_DADOS_CNES_{mesAno}'

# Executar processo
SalvarZipURLCNES(urlBase, pathTemp, pathCSV, listaArquivo, mesAno)
