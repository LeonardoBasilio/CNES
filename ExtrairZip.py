from  datetime import datetime, timedelta
import zipfile as z
import os
import shutil
def SalvarZipURLCNES( tempDiretorio, datalakeDestino, listZips, data):
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
    nomeArquivo = listZips

    diretorioZip = os.path.join(tempDiretorio, nomeArquivo, "ZIP")
    #construcao do diretorio onde vai salvar
    diretorioCSV = os.path.join(tempDiretorio, nomeArquivo, "CSV")

    os.makedirs(diretorioZip, exist_ok=True)
    os.makedirs(diretorioCSV, exist_ok=True)
    #Onde está o arquivo zip baixado
    pathZip = os.path.join(tempDiretorio, nomeArquivo + ".zip")


    with z.ZipFile(pathZip, 'r') as root:
        arquivos_zip = root.namelist()
        encontrados = [a for a in arquivos_zip if "tbestabelecimento" in a.lower()]

        if not encontrados:
            print("⚠️ Nenhum arquivo com 'estabelecimentos' encontrado no ZIP.")
        else:
            for arquivo in encontrados:
                print(f"🗂️ Extraindo: {arquivo}")
                #pra onde ele vai
                root.extract(arquivo, diretorioCSV)

    # Copiar para datalake (assumindo ambiente mssparkutils)
    print(f"[INFO] Copiando arquivos extraídos para: {datalakeDestino}")
    # Remove prefixo 'file:' se estiver presente
    origem = diretorioCSV.replace("file:", "")
    destino = datalakeDestino

    # Cria diretório destino, se não existir
    os.makedirs(destino, exist_ok=True)

    # Copia todos os arquivos e subpastas
    shutil.copytree(origem, destino, dirs_exist_ok=True)

# ## APAGAR: Códigos auxiliares para execução interativa

# ## Execução
# Obter a data e hora atuais menos 1200 horas (~50 dias atrás)
now = datetime.now() - timedelta(hours=1800)
mesAno = now.strftime("%Y%m")
print(mesAno)

urlBase = "https://cnes.datasus.gov.br/EstatisticasServlet?path="

# Definir caminhos
pathCSV = "URL/CNES/Estabelecimentos/"
pathTemp = r'E:\Estudos\CNES'
pathBR = 'URL/CNES/Estabelecimentos'
listaArquivo = f'BASE_DE_DADOS_CNES_{mesAno}'

# Executar processo
SalvarZipURLCNES(pathTemp, pathCSV, listaArquivo, mesAno)