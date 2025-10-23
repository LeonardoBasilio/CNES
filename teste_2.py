import subprocess
subprocess.run([
    "curl", "-o", "arquivo.zip",
    "-L", "--insecure",
    "https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_202509.ZIP"
])
