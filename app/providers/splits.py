import re
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
from ..config import settings

RATIO_RE = re.compile(r"([\d.]+)\s+for\s+([\d.]+)", re.I)
RECENT_SPLITS_URL = "https://stockanalysis.com/actions/splits/"

async def _fetch_rows(url: str):
    headers = {"User-Agent": "Mozilla/5.0 QanasDataEngine/1.0"}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


def _parse_reverse_splits(html: str):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.select("table tbody tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(tds) < 5 or tds[3].lower() != "reverse":
            continue
        m = RATIO_RE.search(tds[4])
        if not m:
            continue
        rows.append({
            "effective_date": datetime.strptime(tds[0], "%b %d, %Y").date(),
            "symbol": tds[1].upper(),
            "company_name": tds[2],
            "split_from": float(m.group(1)),
            "split_to": float(m.group(2)),
        })
    return rows


async def fetch_reverse_splits():
    # Merge the full-year page with the recent page so newly announced
    # splits appear even if the annual page is cached slightly behind.
    year_html, recent_html = await _fetch_rows(settings.stockanalysis_splits_url), await _fetch_rows(RECENT_SPLITS_URL)
    merged = {}
    for row in _parse_reverse_splits(year_html) + _parse_reverse_splits(recent_html):
        key = (row["symbol"], row["effective_date"])
        merged[key] = row
    return sorted(merged.values(), key=lambda r: (r["effective_date"], r["symbol"]))
