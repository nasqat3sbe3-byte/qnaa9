import asyncio
import os
import time
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI

app = FastAPI(title="Qanas Watcher", version="0.2.0")
BOOTED_AT = datetime.now(timezone.utc)

# Small test basket only. This keeps the 256 MB sandbox light while we prove
# that market monitoring works without a browser or the Render web app.
SYMBOLS = ("AAPL", "NVDA", "TSLA")
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

STATE = {
    "status": "starting",
    "heartbeat": None,
    "heartbeat_count": 0,
    "market_scan_count": 0,
    "last_market_scan": None,
    "last_market_error": None,
    "booted_at": BOOTED_AT.isoformat(),
    "pid": os.getpid(),
}
QUOTES = {}

async def heartbeat_loop():
    while True:
        STATE["status"] = "running"
        STATE["heartbeat"] = datetime.now(timezone.utc).isoformat()
        STATE["heartbeat_count"] += 1
        await asyncio.sleep(10)

async def fetch_quote(client, symbol):
    r = await client.get(
        YAHOO.format(symbol=symbol),
        params={"range": "1d", "interval": "1m", "includePrePost": "true", "events": "history"},
    )
    r.raise_for_status()
    result = (r.json().get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError("no chart result")
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    valid = [
        (int(ts), float(closes[i]))
        for i, ts in enumerate(timestamps)
        if i < len(closes) and closes[i] is not None and float(closes[i]) > 0
    ]
    if not valid:
        raise RuntimeError("no valid price")
    ts, price = max(valid, key=lambda x: x[0])
    return {
        "symbol": symbol,
        "price": price,
        "market_timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "source": "yahoo_1m_prepost",
    }

async def market_loop():
    headers = {"User-Agent": "Mozilla/5.0 QanasWatcher/0.2"}
    limits = httpx.Limits(max_connections=4, max_keepalive_connections=4)
    async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=headers, limits=limits) as client:
        while True:
            try:
                rows = await asyncio.gather(*(fetch_quote(client, s) for s in SYMBOLS), return_exceptions=True)
                errors = []
                for symbol, row in zip(SYMBOLS, rows):
                    if isinstance(row, Exception):
                        errors.append(f"{symbol}: {type(row).__name__}")
                    else:
                        QUOTES[symbol] = row
                STATE["market_scan_count"] += 1
                STATE["last_market_scan"] = datetime.now(timezone.utc).isoformat()
                STATE["last_market_error"] = "; ".join(errors) if errors else None
            except Exception as exc:
                STATE["last_market_error"] = type(exc).__name__
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(market_loop())

@app.get("/")
async def root():
    return {
        "service": "qanas-watcher",
        "message": "Qanas Engine is alive",
        "version": "0.2.0",
        **STATE,
        "tracked_symbols": list(SYMBOLS),
        "quotes_ready": len(QUOTES),
        "uptime_seconds": int(time.time() - BOOTED_AT.timestamp()),
        "endpoints": ["/health", "/prices"],
    }

@app.get("/health")
async def health():
    last = STATE["heartbeat"]
    age = None
    if last:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
    return {"ok": bool(last) and age < 30, "heartbeat_age_seconds": age, **STATE}

@app.get("/prices")
async def prices():
    return {
        "service": "qanas-watcher",
        "source": "yahoo_1m_prepost",
        "last_market_scan": STATE["last_market_scan"],
        "market_scan_count": STATE["market_scan_count"],
        "last_market_error": STATE["last_market_error"],
        "count": len(QUOTES),
        "quotes": QUOTES,
    }
