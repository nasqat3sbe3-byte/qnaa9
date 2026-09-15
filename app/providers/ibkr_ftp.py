import asyncio
import ftplib
import io
import re
from datetime import datetime

FTP_HOST = "ftp2.interactivebrokers.com"
FTP_USER = "shortstock"
FTP_PASSWORD = ""
FTP_FILE = "usa.txt"


def _download_usa_file_sync(timeout=20):
    """Download IBKR's public US short-stock file. No database writes."""
    ftp = ftplib.FTP(timeout=timeout)
    try:
        ftp.connect(FTP_HOST, 21)
        ftp.login(FTP_USER, FTP_PASSWORD)
        data = io.BytesIO()
        ftp.retrbinary(f"RETR {FTP_FILE}", data.write)
        return data.getvalue().decode("utf-8", errors="replace")
    finally:
        try:
            ftp.quit()
        except Exception:
            try:
                ftp.close()
            except Exception:
                pass


async def download_usa_file(timeout=20):
    return await asyncio.wait_for(asyncio.to_thread(_download_usa_file_sync, timeout), timeout=timeout + 5)


def find_symbol_lines(text, symbol):
    symbol = symbol.upper().strip()
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        fields = [x.strip().strip('"') for x in re.split(r"[|,\t]", line)]
        if symbol in {f.upper() for f in fields}:
            lines.append(line)
    return lines


async def probe_ibkr_ftp(symbol):
    started = datetime.utcnow()
    text = await download_usa_file()
    matches = find_symbol_lines(text, symbol)
    return {
        "symbol": symbol.upper().strip(),
        "source": "IBKR public FTP usa.txt",
        "fetched_at": datetime.utcnow().isoformat(),
        "elapsed_seconds": round((datetime.utcnow() - started).total_seconds(), 2),
        "file_bytes": len(text.encode("utf-8", errors="ignore")),
        "matches": matches[:20],
    }
