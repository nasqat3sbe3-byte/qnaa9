from fastapi import APIRouter
import time, httpx
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


@router.get('/halts')
async def halts():
    global _HALT_CACHE
    now=time.time()
    if now-_HALT_CACHE["at"]<60:return _HALT_CACHE["rows"]
    url="https://www.nasdaqtrader.com/dynamic/symdir/tradinghalts.txt"
    try:
        async with httpx.AsyncClient(timeout=6,follow_redirects=True) as c:
            r=await c.get(url);r.raise_for_status()
        rows=[]
        for line in r.text.splitlines():
            p=line.split("|")
            if len(p)>=6 and p[0] and p[0]!="Halt Date":
                rows.append({"halt_date":p[0],"halt_time":p[1],"symbol":p[2].upper(),"name":p[3],"market":p[4],"reason":p[5],"resume_time":p[8] if len(p)>8 else ""})
        _HALT_CACHE={"at":now,"rows":rows}
    except Exception:pass
    return _HALT_CACHE["rows"]
