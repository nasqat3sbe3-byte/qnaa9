import httpx
import time
from fastapi import APIRouter
from sqlalchemy import select
from ..db import SessionLocal
from ..models import Stock
from ..services.live_prices import get_live_prices

router=APIRouter()
_HALT_CACHE={"at":0,"rows":[]}

@router.get('/live-prices')
async def live_prices():
    db=SessionLocal()
    try:
        symbols=db.scalars(select(Stock.symbol)).all()
    finally:
        db.close()
    prices=await get_live_prices(symbols)
    return [{"symbol":symbol,**row} for symbol,row in prices.items()]


@router.get("/halts")
async def halts():
    now=time.time()
    if now-_HALT_CACHE["at"]<120:return _HALT_CACHE["rows"]
    try:
        async with httpx.AsyncClient(timeout=4,follow_redirects=True) as c:
            r=await c.get("https://www.nasdaqtrader.com/dynamic/symdir/tradinghalts.txt");r.raise_for_status()
        rows=[]
        for line in r.text.splitlines():
            p=line.split("|")
            if len(p)>=6 and p[0] and p[0]!="Halt Date":
                rows.append({"symbol":p[2].upper(),"reason":p[5],"halt_time":p[1],"halt_date":p[0]})
        _HALT_CACHE.update({"at":now,"rows":rows})
    except Exception:pass
    return _HALT_CACHE["rows"]
