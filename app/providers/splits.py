import re
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
from ..config import settings

RATIO_RE = re.compile(r"([\d.]+)\s+for\s+([\d.]+)", re.I)

async def fetch_reverse_splits():
    headers = {"User-Agent": "Mozilla/5.0 QanasDataEngine/1.0"}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
        response = await client.get(settings.stockanalysis_splits_url)
        response.raise_for_status()
        html = response.text
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
