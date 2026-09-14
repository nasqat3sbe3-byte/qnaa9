import asyncio
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

# Create asyncio primitives lazily inside a running event loop. This avoids
# startup/runtime compatibility problems on newer Python versions used by Render.
_JINA_LOCK = None
_JINA_LAST_CALL = 0.0
_JINA_MIN_INTERVAL = 3.2


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
    return {
        "available_shares": float(available_raw.replace(",", "")),
        "fee_rate": float(fee_raw),
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

    updated = _IB_UPDATED_RE.search(text)
    return {
        "available_shares": _scaled_number(available.group(1), available.group(2)),
        "fee_rate": float(fee.group(1)),
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
    """Read a public page through Jina Reader as a last-resort cache layer."""
    global _JINA_LOCK, _JINA_LAST_CALL

    if _JINA_LOCK is None:
        _JINA_LOCK = asyncio.Lock()

    async with _JINA_LOCK:
        wait_for = _JINA_MIN_INTERVAL - (time.monotonic() - _JINA_LAST_CALL)
        if wait_for > 0:
            await asyncio.sleep(wait_for)

        reader_url = f"https://r.jina.ai/{target_url}"
        try:
            r = await client.get(
                reader_url,
                headers={"Accept": "text/plain", "X-Timeout": "20"},
                timeout=30,
            )
            _JINA_LAST_CALL = time.monotonic()
            if r.status_code == 200:
                return r.text
        except (httpx.TimeoutException, httpx.TransportError):
            _JINA_LAST_CALL = time.monotonic()
        return None


async def fetch_borrow_snapshot(symbol: str, client: httpx.AsyncClient | None = None):
    """Fetch current Available + CTB from public IBKR-derived sources.

    Order:
    1) ChartExchange direct.
    2) IBorrowDesk direct.
    3) Cached ChartExchange through Jina Reader.

    Rebate stays None until a source exposes a real live numeric rebate value.
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
