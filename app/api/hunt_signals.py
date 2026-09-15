import asyncio
from datetime import datetime
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import BorrowSnapshot, HuntSignal, Split, Stock
from ..services.launch_monitor import refresh_launches

router = APIRouter()

def get_db():
    db=SessionLocal()
    try: yield db
    finally: db.close()

def _latest_borrow(db, stock_id):
    return db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==stock_id).order_by(BorrowSnapshot.ts.desc(),BorrowSnapshot.id.desc()).limit(1))

def _register_ready(db):
    rows=db.execute(select(Split,Stock).join(Stock,Stock.id==Split.stock_id).where(Split.effective_date<=datetime.utcnow().date())).all()
    added=0
    for sp,stock in rows:
        if db.scalar(select(HuntSignal).where(HuntSignal.split_id==sp.id)): continue
        b=_latest_borrow(db,stock.id)
        if not b or b.available_shares is None or sp.current_price is None: continue
        if b.available_shares<=10000 and sp.distance_from_low_pct is not None and sp.distance_from_low_pct<=10 and (sp.stability_sessions or 0)>=4:
            db.add(HuntSignal(split_id=sp.id,stock_id=stock.id,ready_at=datetime.utcnow(),ready_price=sp.current_price,ready_low=sp.post_split_low,ready_available=b.available_shares,max_price_after_ready=sp.current_price,max_rise_pct=0.0));added+=1
    db.commit();return added

@router.get('/hunt-signals')
def hunt_signals(db:Session=Depends(get_db)):
    _register_ready(db)
    rows=db.execute(select(HuntSignal,Stock).join(Stock,Stock.id==HuntSignal.stock_id).order_by(HuntSignal.ready_at.desc())).all()
    return [{"symbol":s.symbol,"ready_at":h.ready_at,"ready_price":h.ready_price,"ready_low":h.ready_low,"ready_available":h.ready_available,"launched_at":h.launched_at,"launch_price":h.launch_price,"max_price_after_ready":h.max_price_after_ready,"max_rise_pct":h.max_rise_pct,"is_launched":h.launched_at is not None} for h,s in rows]

@router.get('/sync/launches')
async def sync_launches(db:Session=Depends(get_db)):
    _register_ready(db)
    return await refresh_launches()
