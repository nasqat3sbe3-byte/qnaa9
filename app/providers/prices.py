import asyncio
from datetime import date, datetime, time, timedelta, timezone
import httpx

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _period_ts(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


async def fetch_daily_bars(client: httpx.AsyncClient, symbol: str, start: date, end: date):
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
                })
            return bars
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.8 * (attempt + 1))
    raise last_error or RuntimeError(f"Failed to fetch {symbol}")
