import asyncio
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
import httpx
from bs4 import BeautifulSoup

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
STOCKANALYSIS_HISTORY_URL = "https://stockanalysis.com/stocks/{symbol}/history/"
NY = ZoneInfo("America/New_York")


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
    for page in range(1, 9):
        url = STOCKANALYSIS_HISTORY_URL.format(symbol=symbol.lower())
        params = {"p": page} if page > 1 else None
        response = await client.get(url, params=params, headers=headers)
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
            }
        if found_older:
            break
        await asyncio.sleep(0.12)
    return [bars_by_date[d] for d in sorted(bars_by_date)]


async def fetch_daily_bars(client: httpx.AsyncClient, symbol: str, start: date, end: date):
    bars = await _fetch_stockanalysis_bars(client, symbol, start, end)
    if not bars:
        raise RuntimeError(f"No trusted daily bars returned for {symbol}")
    return bars


async def _fetch_intraday(client: httpx.AsyncClient, symbol: str, start: date, end: date, interval: str):
    params = {
        "period1": _period_ts(start),
        "period2": _period_ts(end + timedelta(days=1)),
        "interval": interval,
        "events": "history",
        "includeAdjustedClose": "false",
        # Qanas split-day reference must include the full trading day, including
        # pre-market/after-hours, because reverse-split spikes can occur there.
        "includePrePost": "true",
    }
    url = YAHOO_CHART_URL.format(symbol=symbol)
    response = await client.get(url, params=params)
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
    rows = []
    for i, ts in enumerate(timestamps):
        if i >= min(len(opens), len(highs), len(lows), len(closes)):
            continue
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        if None in (o, h, l, c):
            continue
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt_ny = dt_utc.astimezone(NY)
        d = dt_ny.date()
        if d < start or d > end:
            continue
        rows.append({
            "ts": dt_utc.replace(tzinfo=None),
            "date": d,
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "volume": float(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0.0,
        })
    return rows


async def fetch_four_hour_bars(client: httpx.AsyncClient, symbol: str, start: date, end: date):
    """Build 4h candles from intraday data, including extended hours.

    Try Yahoo 60m first, then 30m. The split-day high is the maximum High
    across all generated 4h candles on the effective date, so the underlying
    intraday maximum is preserved.
    """
    last_error = None
    intraday = []
    interval = None
    for candidate in ("60m", "30m"):
        try:
            intraday = await _fetch_intraday(client, symbol, start, end, candidate)
            if intraday:
                interval = candidate
                break
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.25)
    if not intraday:
        if last_error:
            raise last_error
        return []

    per_chunk = 4 if interval == "60m" else 8
    by_day = {}
    for row in intraday:
        by_day.setdefault(row["date"], []).append(row)

    out = []
    for d in sorted(by_day):
        day_bars = sorted(by_day[d], key=lambda x: x["ts"])
        for idx in range(0, len(day_bars), per_chunk):
            chunk = day_bars[idx:idx + per_chunk]
            if not chunk:
                continue
            out.append({
                "bar_time": chunk[0]["ts"],
                "open": chunk[0]["open"],
                "high": max(x["high"] for x in chunk),
                "low": min(x["low"] for x in chunk),
                "close": chunk[-1]["close"],
                "volume": sum(x["volume"] for x in chunk),
                "source": f"yahoo_{interval}_prepost",
            })
    return out
