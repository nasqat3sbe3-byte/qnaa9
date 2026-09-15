import asyncio
import ftplib
import io
import re
import time
from datetime import datetime

FTP_HOST = "ftp2.interactivebrokers.com"
FTP_USER = "shortstock"
FTP_PASSWORD = ""
FTP_FILE = "usa.txt"
_CACHE_ROWS = None
_CACHE_META = None
_CACHE_AT = 0.0
_CACHE_TTL = 30.0
_CACHE_LOCK = None


def _download_usa_file_sync(timeout=20):
    ftp = ftplib.FTP(timeout=timeout)
    try:
        ftp.connect(FTP_HOST, 21)
        ftp.login(FTP_USER, FTP_PASSWORD)
        data = io.BytesIO()
        ftp.retrbinary(f"RETR {FTP_FILE}", data.write)
        return data.getvalue().decode("utf-8", errors="replace")
    finally:
        try: ftp.quit()
        except Exception:
            try: ftp.close()
            except Exception: pass


async def download_usa_file(timeout=20):
    return await asyncio.wait_for(asyncio.to_thread(_download_usa_file_sync, timeout), timeout=timeout + 5)


def _fields(raw):
    return [x.strip().strip('"') for x in re.split(r"[|\t]", raw.strip())]


def parse_usa_file(text):
    out = {}
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        f = _fields(raw)
        if len(f) < 8 or f[1].upper().strip() != "USD":
            continue
        symbol = f[0].upper().strip()
        if not symbol:
            continue
        try:
            rebate = float(f[5]); fee = float(f[6]); available = float(f[7].replace(",", ""))
        except (ValueError, TypeError):
            continue
        if available < 0 or fee < -1000 or fee > 10000 or rebate < -10000 or rebate > 10000:
            continue
        out[symbol] = {
            "available_shares": available,
            "fee_rate": fee,
            "rebate_rate": rebate,
            "source": "IBKR public FTP usa.txt",
            "company": f[2] or None,
            "contract_id": f[3] or None,
            "reported_at": None,
            "exchange": "us",
        }
    return out


async def fetch_all_borrow_snapshots(timeout=20, force=False):
    global _CACHE_ROWS, _CACHE_META, _CACHE_AT, _CACHE_LOCK
    if not force and _CACHE_ROWS is not None and time.monotonic() - _CACHE_AT < _CACHE_TTL:
        return _CACHE_ROWS, _CACHE_META
    if _CACHE_LOCK is None:
        _CACHE_LOCK = asyncio.Lock()
    async with _CACHE_LOCK:
        if not force and _CACHE_ROWS is not None and time.monotonic() - _CACHE_AT < _CACHE_TTL:
            return _CACHE_ROWS, _CACHE_META
        started = datetime.utcnow()
        text = await download_usa_file(timeout=timeout)
        rows = parse_usa_file(text)
        meta = {
            "source": "IBKR public FTP usa.txt",
            "fetched_at": datetime.utcnow(),
            "elapsed_seconds": round((datetime.utcnow() - started).total_seconds(), 2),
            "file_bytes": len(text.encode("utf-8", errors="ignore")),
            "symbols": len(rows),
        }
        _CACHE_ROWS, _CACHE_META, _CACHE_AT = rows, meta, time.monotonic()
        return rows, meta


async def fetch_borrow_snapshot_ftp(symbol, timeout=20):
    rows, meta = await fetch_all_borrow_snapshots(timeout=timeout)
    snap = rows.get(symbol.upper().strip())
    if snap:
        snap = dict(snap); snap["fetched_at"] = meta["fetched_at"]
    return snap


def find_symbol_lines(text, symbol):
    symbol = symbol.upper().strip(); lines = []
    for raw in text.splitlines():
        f = _fields(raw)
        if f and f[0].upper().strip() == symbol: lines.append(raw.strip())
    return lines


async def probe_ibkr_ftp(symbol):
    started = datetime.utcnow(); text = await download_usa_file(); matches = find_symbol_lines(text, symbol)
    parsed = parse_usa_file(text).get(symbol.upper().strip())
    return {"symbol":symbol.upper().strip(),"source":"IBKR public FTP usa.txt","fetched_at":datetime.utcnow().isoformat(),"elapsed_seconds":round((datetime.utcnow()-started).total_seconds(),2),"file_bytes":len(text.encode("utf-8",errors="ignore")),"parsed":parsed,"matches":matches[:20]}
