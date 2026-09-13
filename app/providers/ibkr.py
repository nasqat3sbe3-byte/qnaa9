import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


_EXCHANGES = ("nasdaq", "nyse", "amex")
_LATEST_RE = re.compile(
    r"As of\s+(.+?),\s+there were\s+([\d,]+)\s+shares available with a fee of\s+([\d.]+)%",
    re.IGNORECASE,
)


async def fetch_borrow_snapshot(symbol: str, client: httpx.AsyncClient | None = None):
    """Fetch latest public IBKR-derived borrow data from ChartExchange.

    Returns available shares + borrow fee/CTB. ChartExchange does not expose a
    live numeric rebate value on the public page, so rebate_rate remains None.
    """
    symbol = symbol.lower().strip()
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(
            timeout=20,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 QanasDataEngine/1.0"},
        )

    try:
        for exchange in _EXCHANGES:
            url = f"https://chartexchange.com/symbol/{exchange}-{symbol}/borrow-fee/"
            try:
                r = await client.get(url)
                if r.status_code != 200:
                    continue
                text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
                m = _LATEST_RE.search(text)
                if not m:
                    continue

                reported_raw, available_raw, fee_raw = m.groups()
                reported_at = None
                try:
                    reported_at = dtparser.parse(reported_raw)
                    if reported_at.tzinfo is not None:
                        reported_at = reported_at.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
                except Exception:
                    reported_at = None

                return {
                    "available_shares": float(available_raw.replace(",", "")),
                    "fee_rate": float(fee_raw),
                    "rebate_rate": None,
                    "source": "ChartExchange/IBKR",
                    "reported_at": reported_at or datetime.utcnow(),
                    "exchange": exchange,
                }
            except Exception:
                continue
        return None
    finally:
        if own_client and client is not None:
            await client.aclose()
