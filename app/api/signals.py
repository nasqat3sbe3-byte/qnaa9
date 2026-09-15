from datetime import date, datetime
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import BorrowSnapshot, DailyBar, HuntSignal, Split, Stock
from ..services.launch_monitor import refresh_launches

router = APIRouter()
RANGE_START=date(2026,5,1); RANGE_END=date(2026,9,30)
READY_AVAILABLE=10000
READY_DISTANCE_PCT=10
READY_SESSIONS=4
NEAR_DISTANCE_PCT=20
WATCH_AVAILABLE=5000
LAUNCH_PCT=40

def get_db():
    db=SessionLocal()
    try: yield db
    finally: db.close()

def latest_borrow(db, stock_id):
    return db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==stock_id).order_by(BorrowSnapshot.ts.desc(),BorrowSnapshot.id.desc()).limit(1))

def highest_after(db, stock_id, ready_at, fallback):
    d=ready_at.date()
    highs=db.scalars(select(DailyBar.high).where(DailyBar.stock_id==stock_id,DailyBar.trade_date>d)).all()
    vals=[float(x) for x in highs if x is not None]
    if fallback is not None: vals.append(float(fallback))
    return max(vals) if vals else None

def readiness_state(sp,b):
    available=None if b is None else b.available_shares
    available_ok=available is not None and available<=READY_AVAILABLE
    scarce=available is not None and available<=WATCH_AVAILABLE
    distance=sp.distance_from_low_pct
    distance_ok=distance is not None and distance<=READY_DISTANCE_PCT
    distance_near=distance is not None and distance<=NEAR_DISTANCE_PCT
    sessions=int(sp.stability_sessions or 0)
    sessions_ok=sessions>=READY_SESSIONS
    price_ok=sp.current_price is not None and sp.current_price>0
    full=available_ok and distance_ok and sessions_ok and price_ok
    tier=100 if full else None; missing=None
    if not full and price_ok:
        # 90%: all core conditions are complete except one small final step.
        if available_ok and sessions_ok and distance is not None and READY_DISTANCE_PCT < distance <= NEAR_DISTANCE_PCT:
            tier=90; missing=f'يرجع أقرب للقاع: الآن {distance:.2f}% والهدف ≤ {READY_DISTANCE_PCT}%'
        elif available_ok and distance_ok and sessions==READY_SESSIONS-1:
            tier=90; missing='جلسة ثبات واحدة للوصول إلى 4/4'
        # 80%: very selective watch tier. Available must be especially scarce,
        # and the stock can only be one stability session short while still ≤20% from low.
        elif scarce and distance_near and sessions==READY_SESSIONS-1:
            tier=80; missing=f'جلسة ثبات واحدة + الاقتراب من القاع (الآن {distance:.2f}%)'
    strengths=[]
    if available_ok: strengths.append(f'Available {int(available):,} ✓')
    if distance_ok: strengths.append(f'عن القاع {distance:.2f}% ✓')
    elif distance_near: strengths.append(f'قريب من القاع {distance:.2f}%')
    if sessions_ok: strengths.append('ثبات 4/4 ✓')
    elif sessions==3: strengths.append('ثبات 3/4')
    return {'full':full,'tier':tier,'near_ready':tier in (80,90),'missing':missing,'strength':' | '.join(strengths)}

def update_signal(db, sp, stock):
    b=latest_borrow(db,stock.id)
    sig=db.scalar(select(HuntSignal).where(HuntSignal.split_id==sp.id))
    qualifies=readiness_state(sp,b)['full']
    if sig is None and qualifies:
        sig=HuntSignal(split_id=sp.id,stock_id=stock.id,ready_at=datetime.utcnow(),ready_price=sp.current_price,ready_low=sp.post_split_low,ready_available=b.available_shares,max_price_after_ready=sp.current_price,max_rise_pct=0.0)
        db.add(sig); db.flush()
    if sig is not None and sig.launched_at is None:
        hi=highest_after(db,stock.id,sig.ready_at,sp.current_price)
        if hi is not None and sig.ready_price>0:
            sig.max_price_after_ready=max(float(sig.max_price_after_ready or 0),hi)
            sig.max_rise_pct=round((sig.max_price_after_ready/sig.ready_price-1)*100,2)
            if sig.max_rise_pct>=LAUNCH_PCT:
                sig.launched_at=datetime.utcnow(); sig.launch_price=hi
    return sig

@router.get('/signals')
async def signals(db:Session=Depends(get_db)):
    today=date.today()
    rows=db.execute(select(Split,Stock).join(Stock,Stock.id==Split.stock_id).where(Split.effective_date>=RANGE_START,Split.effective_date<=RANGE_END,Split.effective_date<=today).order_by(Stock.symbol,Split.effective_date.desc(),Split.id.desc())).all()
    latest={}
    for sp,s in rows:
        if s.symbol not in latest: latest[s.symbol]=(sp,s)
    states={}
    for sp,s in latest.values():
        states[s.symbol]=readiness_state(sp,latest_borrow(db,s.id))
        update_signal(db,sp,s)
    db.commit()
    await refresh_launches()
    db.expire_all()
    sig_by_stock={sig.stock_id:sig for sig in db.scalars(select(HuntSignal)).all()}
    out=[]
    for sp,s in latest.values():
        sig=sig_by_stock.get(s.id); state=states.get(s.symbol,{})
        if sig:
            out.append({'symbol':s.symbol,'ready':sig.launched_at is None,'readiness_pct':100 if sig.launched_at is None else None,'near_ready':False,'missing':None,'strength':state.get('strength'),'launched':sig.launched_at is not None,'ready_at':sig.ready_at,'ready_price':sig.ready_price,'launched_at':sig.launched_at,'launch_price':sig.launch_price,'rise_pct':sig.max_rise_pct,'max_price_after_ready':sig.max_price_after_ready})
        elif state.get('tier'):
            out.append({'symbol':s.symbol,'ready':False,'readiness_pct':state['tier'],'near_ready':True,'missing':state.get('missing'),'strength':state.get('strength'),'launched':False,'ready_at':None,'ready_price':None,'launched_at':None,'launch_price':None,'rise_pct':None,'max_price_after_ready':None})
    return out
