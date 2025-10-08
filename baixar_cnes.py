# baixar_cnes.py
# Executável: baixa e extrai o ZIP do CNES usando a store do Windows.

import logging
from pathlib import Path

from cnes_downloader import (
    CnesDownloader,
    TLSOptions,
    ProxyOptions,
)  # ajuste o import se o arquivo tiver outro nome

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

URL = (
    "https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_202508.ZIP"
)
ARQ_ZIP = Path("BASE_DE_DADOS_CNES_202508.ZIP")
SAIDA = Path("CNES_202508")

tls = TLSOptions(
    use_windows_store=True,  # <- agora funciona porque você instalou wincertstore
    extra_certs_pem=(),  # opcional: PEMs corporativos extras, se houver
    cafile=None,
    allow_insecure_fallback=False,
    pin_sha256_hex=None,
)

proxy = ProxyOptions(
    proxy_url=None,  # ex.: "http://usuario:senha@proxy.corp:3128" caso a rede exija proxy
    proxy_username=None,
    proxy_password=None,
)

try:
    # 1) Download com retomada
    arq = CnesDownloader.download_with_resume(
        URL, ARQ_ZIP, tls=tls, proxy=proxy, max_retries=6, backoff_factor=1.5
    )

    # 2) Verificação rápida do ZIP
    CnesDownloader.test_zip_integrity(arq)

    # 3) Extração com progresso
    CnesDownloader.extract_zip(arq, SAIDA)

    # 4) Hash do arquivo para auditoria (opcional)
    sha = CnesDownloader.sha256_file(arq)
    print(f"SHA-256 do ZIP: {sha}")

except Exception as e:
    logging.error("Falha no processo: %s", e)
    raise
