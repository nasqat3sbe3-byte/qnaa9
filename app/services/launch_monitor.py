from datetime import datetime
import asyncio
import httpx
from sqlalchemy import select
from ..db import SessionLocal
from ..models import HuntSignal, Stock

YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
LAUNCH_PCT = 40.0

async def _live_high(client, symbol):
    r = await client.get(YAHOO.format(symbol=symbol), params={"range":"1d","interval":"1m","includePrePost":"true","events":"history"})
    r.raise_for_status()
    result = (r.json().get("chart",{}).get("result") or [None])[0]
    if not result: return None
    highs = (((result.get("indicators") or {}).get("quote") or [{}])[0].get("high") or [])
    vals = [float(x) for x in highs if x is not None and float(x) > 0]
    return max(vals) if vals else None

async def refresh_launches():
    """Only watches already-recorded ready signals. Does not alter price/borrow metrics."""
    db = SessionLocal()
    try:
        rows = db.execute(select(HuntSignal, Stock).join(Stock, Stock.id == HuntSignal.stock_id)).all()
        if not rows: return {"checked":0,"launched":0}
        sem = asyncio.Semaphore(8)
        async with httpx.AsyncClient(timeout=12, follow_redirects=True, headers={"User-Agent":"Mozilla/5.0 QanasLaunchMonitor/1.0"}) as client:
            async def one(sig, stock):
                async with sem:
                    try: return sig, await _live_high(client, stock.symbol)
                    except Exception: return sig, None
            results = await asyncio.gather(*(one(sig, stock) for sig, stock in rows))
        launched = 0
        for sig, high in results:
            if high is None or not sig.ready_price or sig.ready_price <= 0: continue
            best = max(float(sig.max_price_after_ready or 0), high)
            rise = ((best / sig.ready_price) - 1) * 100
            sig.max_price_after_ready = best
            sig.max_rise_pct = round(rise, 2)
            if sig.launched_at is None and rise >= LAUNCH_PCT:
                sig.launched_at = datetime.utcnow()
                sig.launch_price = high
                launched += 1
        db.commit()
        return {"checked":len(rows),"launched":launched,"threshold_pct":LAUNCH_PCT}
    finally:
        db.close()
