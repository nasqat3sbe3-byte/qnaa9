from datetime import date, datetime, timezone
import asyncio
import httpx
from sqlalchemy import func, select
from ..db import SessionLocal
from ..models import DailyBar, HuntSignal, Split, Stock

YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
LAUNCH_PCT = 40.0
HOLD_TRADING_SESSIONS = 10

async def _today_quotes(client, symbol, ready_at=None):
    r = await client.get(YAHOO.format(symbol=symbol), params={"range":"1d","interval":"1m","includePrePost":"true","events":"history"})
    r.raise_for_status()
    result = (r.json().get("chart",{}).get("result") or [None])[0]
    if not result: return None
    timestamps=result.get("timestamp") or []
    quote=((result.get("indicators") or {}).get("quote") or [{}])[0]
    highs=quote.get("high") or []
    closes=quote.get("close") or []
    all_high=[]; after_ready=[]; latest=None
    ready_epoch=None
    if ready_at is not None:
        ready_epoch=ready_at.replace(tzinfo=timezone.utc).timestamp() if ready_at.tzinfo is None else ready_at.timestamp()
    for i,ts in enumerate(timestamps):
        h=float(highs[i]) if i<len(highs) and highs[i] is not None else None
        c=float(closes[i]) if i<len(closes) and closes[i] is not None else None
        if h is not None and h>0:
            all_high.append(h)
            if ready_epoch is None or ts>=ready_epoch: after_ready.append(h)
        if c is not None and c>0: latest=(int(ts),c)
    if not all_high and latest is None: return None
    return {"day_high":max(all_high) if all_high else latest[1],"after_ready_high":max(after_ready) if after_ready else None,"latest":latest[1] if latest else None}

def _previous_close(db, stock_id, before_date):
    return db.scalar(select(DailyBar.close).where(DailyBar.stock_id==stock_id,DailyBar.trade_date<before_date).order_by(DailyBar.trade_date.desc()).limit(1))

def _sessions_since(db, stock_id, launched_at):
    return db.scalar(select(func.count(DailyBar.id)).where(DailyBar.stock_id==stock_id,DailyBar.trade_date>launched_at.date())) or 0

async def refresh_launches():
    """Extended-hours launch layer only. Never changes Split, DailyBar or borrow metrics."""
    db=SessionLocal()
    try:
        # After 10 completed trading sessions, remove the old launch cycle so the
        # stock returns to the normal board and can qualify again from fresh readings.
        existing=db.scalars(select(HuntSignal)).all()
        expired=0
        for sig in list(existing):
            if sig.launched_at is not None and _sessions_since(db,sig.stock_id,sig.launched_at)>=HOLD_TRADING_SESSIONS:
                db.delete(sig); expired+=1
        if expired: db.commit()

        today=date.today()
        rows=db.execute(select(Split,Stock).join(Stock,Stock.id==Split.stock_id).where(Split.effective_date<=today).order_by(Stock.symbol,Split.effective_date.desc(),Split.id.desc())).all()
        latest={}
        for sp,stock in rows:
            if stock.symbol not in latest: latest[stock.symbol]=(sp,stock)
        if not latest: return {"checked":0,"launched":0,"expired":expired}

        signals={s.split_id:s for s in db.scalars(select(HuntSignal)).all()}
        sem=asyncio.Semaphore(8)
        async with httpx.AsyncClient(timeout=12,follow_redirects=True,headers={"User-Agent":"Mozilla/5.0 QanasLaunchMonitor/2.0"}) as client:
            async def one(sp,stock):
                sig=signals.get(sp.id)
                async with sem:
                    try: return sp,stock,sig,await _today_quotes(client,stock.symbol,sig.ready_at if sig else None)
                    except Exception: return sp,stock,sig,None
            results=await asyncio.gather(*(one(sp,stock) for sp,stock in latest.values()))

        launched=0
        for sp,stock,sig,q in results:
            if not q: continue
            prev=_previous_close(db,stock.id,today)
            # Rule A: any Qanas stock moving +40% today, including pre/after market,
            # enters the launch list even if it was not formally ready beforehand.
            day_rise=((q["day_high"]/float(prev))-1)*100 if prev and prev>0 else None
            if day_rise is not None and day_rise>=LAUNCH_PCT:
                if sig is None:
                    sig=HuntSignal(split_id=sp.id,stock_id=stock.id,ready_at=datetime.utcnow(),ready_price=float(prev),ready_low=sp.post_split_low,ready_available=None,max_price_after_ready=q["day_high"],max_rise_pct=round(day_rise,2),launched_at=datetime.utcnow(),launch_price=q["day_high"])
                    db.add(sig); signals[sp.id]=sig; launched+=1
                elif sig.launched_at is None:
                    sig.launched_at=datetime.utcnow(); sig.launch_price=q["day_high"]; launched+=1
                sig.max_price_after_ready=max(float(sig.max_price_after_ready or 0),q["day_high"])
                sig.max_rise_pct=max(float(sig.max_rise_pct or 0),round(day_rise,2))
                continue
            # Rule B: preserve the original ready-price lifecycle, but only count
            # intraday bars timestamped after readiness (no earlier same-day spike).
            if sig is not None and sig.ready_price and sig.ready_price>0 and q["after_ready_high"] is not None:
                best=max(float(sig.max_price_after_ready or 0),q["after_ready_high"])
                rise=((best/float(sig.ready_price))-1)*100
                sig.max_price_after_ready=best; sig.max_rise_pct=round(rise,2)
                if sig.launched_at is None and rise>=LAUNCH_PCT:
                    sig.launched_at=datetime.utcnow(); sig.launch_price=q["after_ready_high"]; launched+=1
        db.commit()
        return {"checked":len(results),"launched":launched,"expired":expired,"hold_trading_sessions":HOLD_TRADING_SESSIONS,"threshold_pct":LAUNCH_PCT}
    finally:
        db.close()
