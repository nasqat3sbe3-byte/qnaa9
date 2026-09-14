import asyncio
import re
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


async def fetch_borrow_snapshot(symbol: str, client: httpx.AsyncClient | None = None):
    """Fetch current IBKR-derived Available + CTB from public sources.

    ChartExchange is primary; IBorrowDesk is fallback. Rebate remains None until
    a source exposes an actual live numeric rebate value.
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
        # Primary source: ChartExchange / IBKR.
        for exchange in _EXCHANGES:
            url = f"https://chartexchange.com/symbol/{exchange}-{symbol_lower}/borrow-fee/"
            r = await _get_with_retry(client, url)
            if r is None:
                continue
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            parsed = _parse_chart_exchange(text)
            if parsed:
                parsed.update({"source": "ChartExchange/IBKR", "exchange": exchange})
                return parsed

        # Fallback source: IBorrowDesk, also derived from IBKR stock-loan data.
        url = f"https://www.iborrowdesk.com/report/{clean_symbol}"
        r = await _get_with_retry(client, url)
        if r is not None:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            parsed = _parse_iborrowdesk(text)
            if parsed:
                parsed.update({"source": "IBorrowDesk/IBKR", "exchange": None})
                return parsed

        return None
    finally:
        if own_client and client is not None:
            await client.aclose()
