import asyncio
from datetime import date, datetime, time, timedelta, timezone
import httpx
from bs4 import BeautifulSoup

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
STOCKANALYSIS_HISTORY_URL = "https://stockanalysis.com/stocks/{symbol}/history/"


def _period_ts(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


def _number(text: str):
    text = text.strip().replace(",", "").replace("$", "")
    if not text or text in {"-", "N/A"}:
        return None
    return float(text)


async def _fetch_stockanalysis_bars(client: httpx.AsyncClient, symbol: str, start: date, end: date):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/152 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    bars_by_date = {}
    # 3 pages x 50 rows covers the whole Jun-Sep window with room to spare.
    for page in range(1, 4):
        url = STOCKANALYSIS_HISTORY_URL.format(symbol=symbol.lower())
        params = {"p": page} if page > 1 else None
        response = await client.get(url, params=params, headers=headers)
        if response.status_code in (403, 404, 429):
            if page == 1:
                return []
            break
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("table tbody tr")
        if not rows:
            break
        found_older = False
        for tr in rows:
            cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if len(cells) < 6:
                continue
            try:
                trade_date = datetime.strptime(cells[0], "%b %d, %Y").date()
            except ValueError:
                continue
            if trade_date < start:
                found_older = True
                continue
            if trade_date > end:
                continue
            try:
                o, h, l, c = map(_number, cells[1:5])
                volume = _number(cells[7]) if len(cells) > 7 else None
            except (ValueError, TypeError):
                continue
            if None in (o, h, l, c):
                continue
            bars_by_date[trade_date] = {
                "trade_date": trade_date,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": volume,
                "source": "stockanalysis",
            }
        if found_older:
            break
        await asyncio.sleep(0.15)
    return [bars_by_date[d] for d in sorted(bars_by_date)]


async def _fetch_yahoo_bars(client: httpx.AsyncClient, symbol: str, start: date, end: date):
    params = {
        "period1": _period_ts(start),
        "period2": _period_ts(end + timedelta(days=1)),
        "interval": "1d",
        "events": "history",
        "includeAdjustedClose": "true",
    }
    url = YAHOO_CHART_URL.format(symbol=symbol)
    last_error = None
    for attempt in range(3):
        try:
            response = await client.get(url, params=params)
            if response.status_code == 429:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
            payload = response.json()
            result = (payload.get("chart", {}).get("result") or [None])[0]
            if not result:
                return []
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            opens = quote.get("open") or []
            highs = quote.get("high") or []
            lows = quote.get("low") or []
            closes = quote.get("close") or []
            volumes = quote.get("volume") or []
            bars = []
            for i, ts in enumerate(timestamps):
                if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes):
                    continue
                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                if None in (o, h, l, c):
                    continue
                trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                if trade_date < start or trade_date > end:
                    continue
                bars.append({
                    "trade_date": trade_date,
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "volume": float(volumes[i]) if i < len(volumes) and volumes[i] is not None else None,
                    "source": "yahoo_fallback",
                })
            return bars
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.8 * (attempt + 1))
    raise last_error or RuntimeError(f"Failed to fetch {symbol}")


async def fetch_daily_bars(client: httpx.AsyncClient, symbol: str, start: date, end: date):
    # Accuracy first: use StockAnalysis/S&P history when available. Yahoo is fallback only.
    bars = await _fetch_stockanalysis_bars(client, symbol, start, end)
    if bars:
        return bars
    return await _fetch_yahoo_bars(client, symbol, start, end)
