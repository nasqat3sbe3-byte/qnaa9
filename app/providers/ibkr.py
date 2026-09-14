import asyncio
import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


_EXCHANGES = ("nasdaq", "nyse", "amex", "otc")
_LATEST_RE = re.compile(
    r"As of\s+(.+?),\s+there\s+(?:were|was)\s+([\d,]+)\s+shares?\s+available\s+with\s+a\s+fee\s+of\s+(-?[\d.]+)%",
    re.IGNORECASE,
)
_IB_FEE_RE = re.compile(r"Borrow fee\s+(-?[\d.]+)%", re.IGNORECASE)
_IB_AVAILABLE_RE = re.compile(r"Shares available\s+([\d.]+)\s*([KMB]?)", re.IGNORECASE)
_IB_UPDATED_RE = re.compile(r"Updated\s+(.+?)(?:\s+·|\s+change shown|$)", re.IGNORECASE)

_JINA_LOCK = None
_JINA_LAST_CALL = 0.0
_JINA_MIN_INTERVAL_NO_KEY = 3.2
_JINA_MIN_INTERVAL_WITH_KEY = 0.16


def _jina_api_key():
    return (os.getenv("JINA_API_KEY") or "").strip()


def _jina_min_interval():
    return _JINA_MIN_INTERVAL_WITH_KEY if _jina_api_key() else _JINA_MIN_INTERVAL_NO_KEY


def _parse_dt(raw: str):
    try:
        value = dtparser.parse(raw)
        if value.tzinfo is not None:
            value = value.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        return value
    except Exception:
        return datetime.utcnow()


def _parse_chart_exchange(text: str):
    m = _LATEST_RE.search(text)
    if not m:
        return None
    reported_raw, available_raw, fee_raw = m.groups()
    available = float(available_raw.replace(",", ""))
    fee = float(fee_raw)
    if available < 0 or fee < -1000 or fee > 10000:
        return None
    return {
        "available_shares": available,
        "fee_rate": fee,
        "rebate_rate": None,
        "reported_at": _parse_dt(reported_raw),
    }


def _scaled_number(value: str, suffix: str):
    n = float(value)
    suffix = suffix.upper()
    if suffix == "K":
        n *= 1_000
    elif suffix == "M":
        n *= 1_000_000
    elif suffix == "B":
        n *= 1_000_000_000
    return n


def _parse_iborrowdesk(text: str):
    fee = _IB_FEE_RE.search(text)
    available = _IB_AVAILABLE_RE.search(text)
    if not fee or not available:
        return None

    available_value = _scaled_number(available.group(1), available.group(2))
    fee_value = float(fee.group(1))
    if available_value < 0 or fee_value < -1000 or fee_value > 10000:
        return None

    updated = _IB_UPDATED_RE.search(text)
    return {
        "available_shares": available_value,
        "fee_rate": fee_value,
        "rebate_rate": None,
        "reported_at": _parse_dt(updated.group(1)) if updated else datetime.utcnow(),
    }


async def _get_with_retry(client: httpx.AsyncClient, url: str):
    for attempt in range(4):
        try:
            r = await client.get(url)
            if r.status_code == 200:
                return r
            if r.status_code not in (408, 425, 429, 500, 502, 503, 504):
                return None
        except (httpx.TimeoutException, httpx.TransportError):
            pass
        await asyncio.sleep(1.0 + attempt * 1.5)
    return None


async def _fetch_via_jina(client: httpx.AsyncClient, target_url: str):
    global _JINA_LOCK, _JINA_LAST_CALL

    if _JINA_LOCK is None:
        _JINA_LOCK = asyncio.Lock()

    async with _JINA_LOCK:
        wait_for = _jina_min_interval() - (time.monotonic() - _JINA_LAST_CALL)
        if wait_for > 0:
            await asyncio.sleep(wait_for)

        headers = {"Accept": "text/plain", "X-Timeout": "20"}
        key = _jina_api_key()
        if key:
            headers["Authorization"] = f"Bearer {key}"

        reader_url = f"https://r.jina.ai/{target_url}"
        for attempt in range(3):
            try:
                r = await client.get(reader_url, headers=headers, timeout=30)
                _JINA_LAST_CALL = time.monotonic()
                if r.status_code == 200:
                    return r.text
                if r.status_code not in (408, 425, 429, 500, 502, 503, 504):
                    return None
            except (httpx.TimeoutException, httpx.TransportError):
                _JINA_LAST_CALL = time.monotonic()
            await asyncio.sleep(1.0 + attempt)
        return None


async def fetch_borrow_snapshot(symbol: str, client: httpx.AsyncClient | None = None):
    """Fetch current Available + CTB from public IBKR-derived sources.

    Order:
    1) ChartExchange direct.
    2) IBorrowDesk direct.
    3) ChartExchange through Jina Reader.

    If JINA_API_KEY is configured, the official authenticated Reader path uses
    a much faster conservative request interval. Rebate stays None until a
    trusted source exposes a real numeric rebate value.
    """
    clean_symbol = symbol.upper().strip()
    symbol_lower = clean_symbol.lower()
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/153 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
            },
        )

    try:
        target_urls = []
        for exchange in _EXCHANGES:
            url = f"https://chartexchange.com/symbol/{exchange}-{symbol_lower}/borrow-fee/"
            target_urls.append((exchange, url))
            r = await _get_with_retry(client, url)
            if r is None:
                continue
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            parsed = _parse_chart_exchange(text)
            if parsed:
                parsed.update({"source": "ChartExchange/IBKR", "exchange": exchange})
                return parsed

        url = f"https://www.iborrowdesk.com/report/{clean_symbol}"
        r = await _get_with_retry(client, url)
        if r is not None:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            parsed = _parse_iborrowdesk(text)
            if parsed:
                parsed.update({"source": "IBorrowDesk/IBKR", "exchange": None})
                return parsed

        for exchange, target_url in target_urls:
            text = await _fetch_via_jina(client, target_url)
            if not text:
                continue
            parsed = _parse_chart_exchange(text)
            if parsed:
                parsed.update({"source": "JinaCache/ChartExchange/IBKR", "exchange": exchange})
                return parsed

        return None
    finally:
        if own_client and client is not None:
            await client.aclose()
