from fastapi import APIRouter
from sqlalchemy import select
from ..db import SessionLocal
from ..models import Stock
from ..services.live_prices import get_live_prices

router=APIRouter()

@router.get('/live-prices')
async def live_prices():
    db=SessionLocal()
    try:
        symbols=db.scalars(select(Stock.symbol)).all()
    finally:
        db.close()
    prices=await get_live_prices(symbols)
    return [{"symbol":symbol,**row} for symbol,row in prices.items()]
