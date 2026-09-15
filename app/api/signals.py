from datetime import date, datetime
import asyncio
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import BorrowSnapshot, DailyBar, HuntSignal, Split, Stock
from ..services.launch_monitor import refresh_launches
from ..services.live_prices import get_live_prices

router = APIRouter()
RANGE_START=date(2026,5,1); RANGE_END=date(2026,9,30)
READY_AVAILABLE=10000
READY_DISTANCE_PCT=10
READY_SESSIONS=4
CLOSE_AVAILABLE=20000
CLOSE_DISTANCE_PCT=20
CLOSE_SESSIONS=2
LAUNCH_PCT=40
_launch_task=None

def get_db():
    db=SessionLocal()
    try: yield db
    finally: db.close()

def latest_borrow(db, stock_id):
    return db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==stock_id).order_by(BorrowSnapshot.ts.desc(),BorrowSnapshot.id.desc()).limit(1))

def highest_after(db, stock_id, ready_at, fallback):
    d=ready_at.date(); highs=db.scalars(select(DailyBar.high).where(DailyBar.stock_id==stock_id,DailyBar.trade_date>d)).all()
    vals=[float(x) for x in highs if x is not None]
    if fallback is not None: vals.append(float(fallback))
    return max(vals) if vals else None

def readiness_state(sp,b,live=None):
    av=None if b is None else b.available_shares
    live_low=(live or {}).get('live_day_low'); live_price=(live or {}).get('live_price')
    new_low=bool(live_low is not None and sp.post_split_low is not None and float(live_low)<float(sp.post_split_low))
    effective_low=float(live_low) if new_low else sp.post_split_low
    price=float(live_price) if live_price is not None else sp.current_price
    dist=((price/effective_low)-1)*100 if price is not None and effective_low not in (None,0) else sp.distance_from_low_pct
    sessions=0 if new_low else int(sp.stability_sessions or 0)
    price_ok=price is not None and price>0
    half_ok=bool(sp.half_level_reached)
    av_ok=av is not None and av<=READY_AVAILABLE
    dist_ok=dist is not None and dist<=READY_DISTANCE_PCT
    sess_ok=sessions>=READY_SESSIONS
    missing=[]; close=True
    if not half_ok:
        missing.append(f'يحقق شرط النصف ≤ {sp.half_level:.4f}' if sp.half_level is not None else 'حساب مستوى النصف'); close=False
    if new_low:
        missing.append('كوّن قاع جديد اليوم: يبدأ الثبات من 0/4'); close=False
    if not av_ok:
        missing.append('Available ينزل إلى ≤10K' if av is not None else 'قراءة Available'); close=close and av is not None and av<=CLOSE_AVAILABLE
    if not dist_ok:
        missing.append(f'يرجع أقرب للقاع: الآن {dist:.2f}% والهدف ≤10%' if dist is not None else 'حساب البعد عن القاع'); close=close and dist is not None and dist<=CLOSE_DISTANCE_PCT
    if not sess_ok and not new_low:
        need=max(0,READY_SESSIONS-sessions); missing.append(f'{need} جلسة ثبات إضافية للوصول إلى 4/4'); close=close and sessions>=CLOSE_SESSIONS
    if not price_ok:
        missing.append('تحديث السعر الحالي'); close=False
    full=price_ok and half_ok and not new_low and av_ok and dist_ok and sess_ok
    missing_count=len(missing)
    shortlist=full or (price_ok and half_ok and not new_low and close and 1<=missing_count<=2)
    if av is None: av_pts=0
    elif av<=READY_AVAILABLE: av_pts=45
    elif av<=CLOSE_AVAILABLE: av_pts=45-15*((av-READY_AVAILABLE)/(CLOSE_AVAILABLE-READY_AVAILABLE))
    else: av_pts=0
    if dist is None: dist_pts=0
    elif dist<=READY_DISTANCE_PCT: dist_pts=30
    elif dist<=CLOSE_DISTANCE_PCT: dist_pts=30-15*((dist-READY_DISTANCE_PCT)/(CLOSE_DISTANCE_PCT-READY_DISTANCE_PCT))
    else: dist_pts=0
    sess_pts=25 if sessions>=4 else 19 if sessions==3 else 12 if sessions==2 else 6 if sessions==1 else 0
    pct=100.0 if full else round(min(99.0,av_pts+dist_pts+sess_pts),1)
    strengths=[]
    if half_ok: strengths.append('شرط النصف ✓')
    if av_ok: strengths.append(f'Available {int(av):,} ✓')
    if dist_ok: strengths.append(f'عن القاع {dist:.2f}% ✓')
    if sess_ok: strengths.append('ثبات 4/4 ✓')
    return {'full':full,'shortlist':shortlist,'readiness_pct':pct,'missing_count':missing_count,'missing':' + '.join(missing) if missing else 'مكتمل ✓','strength':' | '.join(strengths),'new_low_today':new_low,'effective_low':effective_low,'effective_distance_pct':dist,'effective_sessions':sessions}

def update_signal(db,sp,stock,state=None):
    b=latest_borrow(db,stock.id); sig=db.scalar(select(HuntSignal).where(HuntSignal.split_id==sp.id)); qualifies=(state or readiness_state(sp,b))['full']
    if sig is None and qualifies:
        sig=HuntSignal(split_id=sp.id,stock_id=stock.id,ready_at=datetime.utcnow(),ready_price=sp.current_price,ready_low=sp.post_split_low,ready_available=b.available_shares,max_price_after_ready=sp.current_price,max_rise_pct=0.0); db.add(sig); db.flush()
    if sig is not None and sig.launched_at is None:
        hi=highest_after(db,stock.id,sig.ready_at,sp.current_price)
        if hi is not None and sig.ready_price>0:
            sig.max_price_after_ready=max(float(sig.max_price_after_ready or 0),hi); sig.max_rise_pct=round((sig.max_price_after_ready/sig.ready_price-1)*100,2)
            if sig.max_rise_pct>=LAUNCH_PCT: sig.launched_at=datetime.utcnow(); sig.launch_price=hi
    return sig

def kick_launch_refresh():
    global _launch_task
    try:
        if _launch_task is None or _launch_task.done(): _launch_task=asyncio.create_task(refresh_launches())
    except RuntimeError: pass

@router.get('/signals')
async def signals(db:Session=Depends(get_db)):
    today=date.today(); rows=db.execute(select(Split,Stock).join(Stock,Stock.id==Split.stock_id).where(Split.effective_date>=RANGE_START,Split.effective_date<=RANGE_END,Split.effective_date<=today).order_by(Stock.symbol,Split.effective_date.desc(),Split.id.desc())).all()
    latest={}
    for sp,s in rows:
        if s.symbol not in latest: latest[s.symbol]=(sp,s)
    live=await get_live_prices(latest.keys())
    states={}
    for sp,s in latest.values():
        states[s.symbol]=readiness_state(sp,latest_borrow(db,s.id),live.get(s.symbol)); update_signal(db,sp,s,states[s.symbol])
    db.commit(); db.expire_all()
    sig_by_stock={sig.stock_id:sig for sig in db.scalars(select(HuntSignal)).all()}; out=[]
    for sp,s in latest.values():
        sig=sig_by_stock.get(s.id); st=states[s.symbol]; launched=bool(sig and sig.launched_at is not None); ready=bool(sig and sig.launched_at is None and st['full'])
        out.append({'symbol':s.symbol,'effective_date':sp.effective_date,'ready':ready,'near_ready':st['shortlist'] and not st['full'] and not launched,'shortlist':st['shortlist'] and not launched,'readiness_pct':100.0 if ready else st['readiness_pct'],'missing_count':st['missing_count'],'missing':st['missing'],'strength':st['strength'],'new_low_today':st['new_low_today'],'effective_low':st['effective_low'],'effective_distance_pct':st['effective_distance_pct'],'effective_sessions':st['effective_sessions'],'launched':launched,'ready_at':sig.ready_at if sig else None,'ready_price':sig.ready_price if sig else None,'launched_at':sig.launched_at if sig else None,'launch_price':sig.launch_price if sig else None,'rise_pct':sig.max_rise_pct if sig else None,'max_price_after_ready':sig.max_price_after_ready if sig else None})
    kick_launch_refresh(); return out
