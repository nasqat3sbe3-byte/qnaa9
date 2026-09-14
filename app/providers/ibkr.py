import asyncio
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


# US listings in the reverse-split universe are mainly on these venues.
# OTC is included because some post-split names move off the major exchanges.
_EXCHANGES = ("nasdaq", "nyse", "amex", "otc")

# ChartExchange's public wording has changed slightly over time, so keep the
# parser tolerant of singular/plural shares and negative fee values.
_LATEST_RE = re.compile(
    r"As of\s+(.+?),\s+there\s+(?:were|was)\s+([\d,]+)\s+shares?\s+available\s+with\s+a\s+fee\s+of\s+(-?[\d.]+)%",
    re.IGNORECASE,
)


def _parse_snapshot(text: str):
    m = _LATEST_RE.search(text)
    if not m:
        return None

    reported_raw, available_raw, fee_raw = m.groups()
    try:
        reported_at = dtparser.parse(reported_raw)
        if reported_at.tzinfo is not None:
            reported_at = reported_at.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    except Exception:
        reported_at = datetime.utcnow()

    return {
        "available_shares": float(available_raw.replace(",", "")),
        "fee_rate": float(fee_raw),
        "rebate_rate": None,
        "reported_at": reported_at,
    }


async def _get_with_retry(client: httpx.AsyncClient, url: str):
    """Small retry/backoff for transient 429/5xx responses from the public page."""
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
    """Fetch latest public IBKR-derived borrow data from ChartExchange.

    Returns current available shares + borrow fee/CTB. The public page does not
    expose a live numeric rebate rate, so rebate_rate deliberately remains None.
    """
    symbol = symbol.lower().strip()
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
        for exchange in _EXCHANGES:
            url = f"https://chartexchange.com/symbol/{exchange}-{symbol}/borrow-fee/"
            r = await _get_with_retry(client, url)
            if r is None:
                continue

            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            parsed = _parse_snapshot(text)
            if not parsed:
                continue

            parsed.update({
                "source": "ChartExchange/IBKR",
                "exchange": exchange,
            })
            return parsed

        return None
    finally:
        if own_client and client is not None:
            await client.aclose()
