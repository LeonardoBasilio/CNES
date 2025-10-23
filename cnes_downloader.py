# baixar_cnes_pin.py
# -*- coding: utf-8 -*-
"""
Download do CNES (~600 MB) com retomada, validação/extração de ZIP e
TLS por pinning do certificado leaf (SHA-256). NÃO requer wincertstore.
"""

from __future__ import annotations

import hashlib
import logging
import os
import socket
import ssl
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Dict, Optional, Union
from urllib import error, request

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOGGER = logging.getLogger("CNES_PIN")


# =================== TLS helpers (pinning) ===================


def leaf_sha256_hex(host: str, port: int = 443) -> str:
    """Retorna SHA-256 (hex) do certificado leaf apresentado pelo host (sem verificação de cadeia)."""
    ctx = ssl._create_unverified_context()
    ctx.check_hostname = False
    with socket.create_connection((host, port), timeout=10) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            der = ssock.getpeercert(True)
    return hashlib.sha256(der).hexdigest()


def unverified_ssl_context() -> ssl.SSLContext:
    """Contexto SSL sem verificação de cadeia (apenas para usar junto com pinning)."""
    ctx = ssl._create_unverified_context()
    ctx.check_hostname = False
    return ctx


# =================== Networking (urllib) ===================


def build_opener(
    context: ssl.SSLContext, proxy_url: Optional[str]
) -> request.OpenerDirector:
    handlers = []
    if proxy_url:
        proxies: Dict[str, str] = {"http": proxy_url, "https": proxy_url}
        handlers.append(request.ProxyHandler(proxies))
    handlers.append(request.HTTPSHandler(context=context))
    return request.build_opener(*handlers)


# =================== Download com retomada ===================


def human(n: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {u}"
        n //= 1024
    return f"{n} PB"


def remote_size(url: str, opener) -> Optional[int]:
    try:
        with opener.open(request.Request(url, method="HEAD"), timeout=30) as resp:
            length = resp.headers.get("Content-Length")
            if length and length.isdigit():
                return int(length)
    except Exception:
        return None
    return None


def supports_range(url: str, opener) -> bool:
    try:
        with opener.open(request.Request(url, method="HEAD"), timeout=30) as resp:
            return "bytes" in (resp.headers.get("Accept-Ranges") or "").lower()
    except Exception:
        return False


def progress(prefix: str, done: int, total: Optional[int]) -> None:
    if total and total > 0:
        pct = (done / total) * 100
        sys.stdout.write(f"\r{prefix}: {human(done)} / {human(total)} ({pct:5.1f}%)")
    else:
        sys.stdout.write(f"\r{prefix}: {human(done)}")
    sys.stdout.flush()


def download_with_resume_pigitnned(
    url: str,
    destino: Union[str, Path],
    *,
    pin_hex: str,
    proxy_url: Optional[str] = None,
    max_retries: int = 6,
    backoff_factor: float = 1.5,
    chunk_size: int = 1024 * 512,  # 512 KiB
    user_agent: str = "python-urllib/3 CNES-PIN",
) -> Path:
    """
    Baixa com **retomada** e **pinning do certificado leaf**:
    - Valida que o leaf SHA-256 do host corresponde a `pin_hex`.
    - Usa contexto sem verificação de cadeia (porque a cadeia falha no seu ambiente),
      mas mantém autenticidade via pin.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)

    # 1) Valida o pin do leaf antes do download
    host = request.urlparse(url).hostname or ""
    current = leaf_sha256_hex(host)
    if current.lower() != pin_hex.lower():
        raise ssl.SSLError(
            f"Pin SHA-256 divergente para {host}. Esperado={pin_hex}, obtido={current}"
        )

    # 2) Prepara opener com contexto "unverified" (cadeia ignorada)
    context = unverified_ssl_context()
    opener = build_opener(context, proxy_url)

    # 3) Descobre tamanho remoto e suporte a Range
    remote_total = remote_size(url, opener)
    accept_range = supports_range(url, opener)

    attempt = 0
    while True:
        attempt += 1
        try:
            existing = destino.stat().st_size if destino.exists() else 0
            headers = {"User-Agent": user_agent}
            if accept_range and existing > 0:
                headers["Range"] = f"bytes={existing}-"

            # Revalida o pin antes de cada nova conexão (robustez)
            current = leaf_sha256_hex(host)
            if current.lower() != pin_hex.lower():
                raise ssl.SSLError(
                    f"Pin SHA-256 divergente para {host} durante retomada. Esperado={pin_hex}, obtido={current}"
                )

            req = request.Request(url, headers=headers)
            with opener.open(req, timeout=120) as resp:
                mode = "ab" if "Range" in headers else "wb"
                downloaded = existing
                total_for_display = remote_total
                cr = resp.headers.get("Content-Range")
                if cr and "/" in cr:
                    try:
                        total_for_display = int(cr.split("/")[-1])
                    except Exception:
                        pass

                progress("Baixando", downloaded, total_for_display)
                with destino.open(mode) as f:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if downloaded - existing >= 1024 * 1024:
                            progress("Baixando", downloaded, total_for_display)
                            existing = downloaded
                progress("Baixando", downloaded, total_for_display)
                sys.stdout.write("\n")

            if remote_total and destino.stat().st_size != remote_total:
                logging.warning(
                    "Tamanho final (%s) difere do esperado (%s).",
                    destino.stat().st_size,
                    remote_total,
                )
            logging.info("Download concluído: %s", destino.resolve())
            return destino

        except (
            error.URLError,
            error.HTTPError,
            ssl.SSLError,
            ConnectionError,
            TimeoutError,
        ) as exc:
            if attempt >= max_retries:
                raise RuntimeError(
                    f"Falhou após {max_retries} tentativas: {exc}"
                ) from exc
            sleep_for = backoff_factor * attempt
            logging.warning(
                "Falha (%s/%s): %s. Aguardando %ss para nova tentativa...",
                attempt,
                max_retries,
                exc,
                sleep_for,
            )
            time.sleep(sleep_for)


# =================== Verificação & Extração ===================


def sha256_file(path: Union[str, Path], chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def test_zip_integrity(path: Union[str, Path]) -> None:
    with zipfile.ZipFile(Path(path), "r") as zf:
        bad = zf.testzip()
        if bad is not None:
            raise zipfile.BadZipFile(
                f"Arquivo ZIP corrompido. Primeiro arquivo com erro: {bad}"
            )


def extract_zip(path: Union[str, Path], destino: Union[str, Path]) -> Path:
    zip_path = Path(path)
    out_dir = Path(destino)
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        infos = zf.infolist()
        total = sum(i.file_size for i in infos)
        done = 0
        progress("Extraindo", 0, total)
        for info in infos:
            target = out_dir / info.filename
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, target.open("wb") as dst:
                while True:
                    chunk = src.read(1024 * 512)
                    if not chunk:
                        break
                    dst.write(chunk)
                    done += len(chunk)
                    if done % (1024 * 1024) < 512 * 1024:
                        progress("Extraindo", done, total)
        progress("Extraindo", total, total)
        sys.stdout.write("\n")

    logging.info("Extração concluída: %s", out_dir.resolve())
    return out_dir


# =================== Execução ===================


def main() -> None:
    url = "https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_202508.ZIP"
    destino_zip = Path("BASE_DE_DADOS_CNES_202508.ZIP")
    pasta_saida = Path("CNES_202508")

    # Proxy (se sua rede exigir). Ex.: "http://usuario:senha@proxy.corp:3128"
    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None

    # 1) Descobre o SHA-256 do leaf atual (TOFU)
    host = request.urlparse(url).hostname or "cnes.datasus.gov.br"
    pin = leaf_sha256_hex(host)
    logging.info("Pin (SHA-256) do leaf atual de %s: %s", host, pin)

    # 2) Baixa com retomada, PIN obrigatório e cadeia desabilitada
    arq = download_with_resume_pinned(
        url,
        destino_zip,
        pin_hex=pin,
        proxy_url=proxy_url,
        max_retries=8,
        backoff_factor=1.5,
    )

    # 3) Verifica e extrai
    test_zip_integrity(arq)
    extract_zip(arq, pasta_saida)

    # 4) Hash do arquivo para auditoria (opcional)
    print("SHA-256 do ZIP:", sha256_file(arq))


if __name__ == "__main__":
    main()
