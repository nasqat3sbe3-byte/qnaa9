import asyncio
import time
from datetime import datetime, timezone
import httpx

YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_CACHE = {}
_CACHE_TTL = 60
_LOCK = asyncio.Lock()

async def _fetch_one(client, sem, symbol):
    async with sem:
        try:
            r = await client.get(YAHOO.format(symbol=symbol), params={"range":"1d","interval":"1m","includePrePost":"true","events":"history"})
            r.raise_for_status()
            result = (r.json().get("chart",{}).get("result") or [None])[0]
            if not result: return symbol, None
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            closes = quote.get("close") or []
            highs = quote.get("high") or []
            lows = quote.get("low") or []
            valid=[]
            for i, ts in enumerate(timestamps):
                if i < len(closes) and closes[i] is not None and float(closes[i]) > 0:
                    valid.append((int(ts), float(closes[i])))
            if not valid: return symbol, None
            ts, price = max(valid, key=lambda x:x[0])
            hi=[float(x) for x in highs if x is not None and float(x)>0]
            lo=[float(x) for x in lows if x is not None and float(x)>0]
            return symbol, {"live_price":price,"live_timestamp":datetime.fromtimestamp(ts,tz=timezone.utc).isoformat(),"live_day_high":max(hi) if hi else price,"live_day_low":min(lo) if lo else price,"live_source":"yahoo_1m_prepost"}
        except Exception:
            return symbol, None

async def get_live_prices(symbols):
    symbols=sorted({str(s).upper().strip() for s in symbols if s})
    now=time.monotonic()
    async with _LOCK:
        fresh={s:_CACHE[s][1] for s in symbols if s in _CACHE and now-_CACHE[s][0] < _CACHE_TTL}
        missing=[s for s in symbols if s not in fresh]
        if missing:
            # Keep concurrency conservative: Yahoo rejects bursts more often than
            # a slightly smaller pool, especially when mobile refreshes overlap.
            sem=asyncio.Semaphore(12)
            limits=httpx.Limits(max_connections=16,max_keepalive_connections=12)
            async with httpx.AsyncClient(timeout=8,follow_redirects=True,headers={"User-Agent":"Mozilla/5.0 QanasLivePrice/1.0"},limits=limits) as client:
                results=await asyncio.gather(*(_fetch_one(client,sem,s) for s in missing))
            stamp=time.monotonic()
            for s,row in results:
                if row is not None:
                    _CACHE[s]=(stamp,row); fresh[s]=row
        return fresh
