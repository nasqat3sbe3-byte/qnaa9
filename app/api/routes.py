import asyncio
import json
from datetime import date, datetime
import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import BorrowSnapshot, DailyBar, FourHourBar, Split, Stock, SyncRun, HuntSignal
from ..providers.ibkr import fetch_borrow_snapshot
from ..providers.ibkr_ftp import probe_ibkr_ftp
from ..services.metrics import refresh_split_metrics
from ..services.sync_prices import PRICE_SYNC_STATUS, run_price_sync
from ..services.sync_splits import sync_splits
from .hunt_signals import router as hunt_signals_router

router = APIRouter(); router.include_router(hunt_signals_router)
RANGE_START=date(2026,5,1); RANGE_END=date(2026,9,30)
BORROW_SYNC_STATUS={"running":False,"processed":0,"total":0,"saved":0,"not_found":0,"errors":[],"last_finished":None}; BORROW_SYNC_CONCURRENCY=32

def get_db():
    db=SessionLocal()
    try:yield db
    finally:db.close()

def _four_hour_stats(db,stock_id,effective_date):
    bars=db.scalars(select(FourHourBar).where(FourHourBar.stock_id==stock_id).order_by(FourHourBar.bar_time.asc())).all(); bars=[b for b in bars if b.bar_time.date()>=effective_date and b.open and b.open>0]
    if not bars:return {"four_hour_highest_rise_pct":None,"four_hour_highest_rise_open":None,"four_hour_highest_rise_high":None,"four_hour_highest_rise_time":None,"four_hour_highest_price":None,"four_hour_source":None}
    best=max(bars,key=lambda b:((b.high/b.open)-1)*100); return {"four_hour_highest_rise_pct":round(((best.high/best.open)-1)*100,2),"four_hour_highest_rise_open":best.open,"four_hour_highest_rise_high":best.high,"four_hour_highest_rise_time":best.bar_time.isoformat(),"four_hour_highest_price":max(b.high for b in bars),"four_hour_source":best.source}

def _latest_borrow(db,stock_id):return db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==stock_id).order_by(BorrowSnapshot.ts.desc(),BorrowSnapshot.id.desc()).limit(1))
def _latest_split_rows(db,include_future=False):
    today=date.today();q=select(Split,Stock).join(Stock,Stock.id==Split.stock_id).where(Split.effective_date>=RANGE_START,Split.effective_date<=RANGE_END)
    if not include_future:q=q.where(Split.effective_date<=today)
    rows=db.execute(q.order_by(Stock.symbol.asc(),Split.effective_date.desc(),Split.id.desc())).all();latest={}
    for sp,s in rows:
        if s.symbol not in latest:latest[s.symbol]=(sp,s)
    return list(latest.values())
def _progressive_score(sp,b):
    av=b.available_shares if b else None; dist=sp.distance_from_low_pct; sessions=int(sp.stability_sessions or 0)
    if av is None: ap=0
    elif av<=10000: ap=45
    elif av<=20000: ap=45-15*((av-10000)/10000)
    else: ap=0
    if dist is None: dp=0
    elif dist<=10: dp=30
    elif dist<=20: dp=30-15*((dist-10)/10)
    else: dp=0
    spoints=25 if sessions>=4 else 19 if sessions==3 else 12 if sessions==2 else 6 if sessions==1 else 0
    return round(min(100,ap+dp+spoints),1)
def _split_payload(db,sp,s):
    b=_latest_borrow(db,s.id);score=_progressive_score(sp,b);p={"symbol":s.symbol,"company":s.company_name,"effective_date":sp.effective_date,"ratio":f"{sp.split_from:g} for {sp.split_to:g}","status":sp.status,"post_split_open":sp.post_split_open,"post_split_high":sp.post_split_high,"post_split_low":sp.post_split_low,"current_price":sp.current_price,"distance_from_low_pct":sp.distance_from_low_pct,"stability_sessions":sp.stability_sessions,"half_level":sp.half_level,"half_level_reference":"highest_price_since_split","half_level_reached":sp.half_level_reached,"ready_score":score,"available":b.available_shares if b else None,"ctb":b.fee_rate if b else None,"fee_rate":b.fee_rate if b else None,"rebate_rate":b.rebate_rate if b else None,"borrow_source":b.source if b else None,"borrow_timestamp":b.ts if b else None};p.update(_four_hour_stats(db,s.id,sp.effective_date));return p
async def _borrow_fetch_one(client,sem,symbol):
    async with sem:
        try:return symbol,await fetch_borrow_snapshot(symbol,client=client),None
        except Exception as exc:return symbol,None,str(exc)
def _run_payload(r):
    if not r:return {"running":False,"processed":0,"total":0,"saved":0,"not_found":0,"errors":[],"missing_symbols":[],"last_finished":None}
    try:errors=json.loads(r.errors_json or "[]")
    except:errors=[]
    missing=[e.get("symbol") for e in errors if isinstance(e,dict) and e.get("error")=="borrow data not found" and e.get("symbol")];return {"running":r.running,"processed":r.processed,"total":r.total,"saved":r.saved,"not_found":r.not_found,"errors":errors,"missing_symbols":missing,"started_at":r.started_at,"last_finished":r.finished_at}
async def run_borrow_sync():
    if BORROW_SYNC_STATUS["running"]:return BORROW_SYNC_STATUS
    BORROW_SYNC_STATUS.update({"running":True,"processed":0,"total":0,"saved":0,"not_found":0,"errors":[],"last_finished":None});db=SessionLocal();run=None
    try:
        run=SyncRun(kind="borrow",running=True);db.add(run);db.commit();db.refresh(run);await sync_splits(db,start=RANGE_START,end=RANGE_END);today=date.today();missing_prices=db.scalar(select(func.count(Split.id)).where(Split.effective_date>=RANGE_START,Split.effective_date<=RANGE_END,Split.effective_date<=today,Split.post_split_open.is_(None))) or 0
        if missing_prices and not PRICE_SYNC_STATUS["running"]:asyncio.create_task(run_price_sync(start=RANGE_START,end=RANGE_END))
        rows=_latest_split_rows(db);targets={s.symbol:(sp,s) for sp,s in rows};BORROW_SYNC_STATUS["total"]=len(targets);run.total=len(targets);db.commit();sem=asyncio.Semaphore(BORROW_SYNC_CONCURRENCY);headers={"User-Agent":"Mozilla/5.0 QanasDataEngine/1.0","Cache-Control":"no-cache, no-store","Pragma":"no-cache"};limits=httpx.Limits(max_connections=64,max_keepalive_connections=40)
        async with httpx.AsyncClient(timeout=30,follow_redirects=True,headers=headers,limits=limits) as client:
            tasks=[asyncio.create_task(_borrow_fetch_one(client,sem,symbol)) for symbol in targets]
            for task in asyncio.as_completed(tasks):
                symbol,snap,err=await task;sp,stock=targets[symbol];BORROW_SYNC_STATUS["processed"]+=1;run.processed+=1
                if err:BORROW_SYNC_STATUS["errors"].append({"symbol":symbol,"error":err[:180]})
                elif not snap:BORROW_SYNC_STATUS["not_found"]+=1;run.not_found+=1;BORROW_SYNC_STATUS["errors"].append({"symbol":symbol,"error":"borrow data not found"})
                else:db.add(BorrowSnapshot(stock_id=stock.id,ts=datetime.utcnow(),available_shares=snap["available_shares"],fee_rate=snap["fee_rate"],rebate_rate=snap["rebate_rate"],source=snap["source"]));db.flush();BORROW_SYNC_STATUS["saved"]+=1;run.saved+=1;refresh_split_metrics(db,sp)
                run.errors_json=json.dumps(BORROW_SYNC_STATUS["errors"][:200]);db.commit()
        run.running=False;run.finished_at=datetime.utcnow();run.errors_json=json.dumps(BORROW_SYNC_STATUS["errors"][:200]);db.commit();BORROW_SYNC_STATUS["last_finished"]=run.finished_at.isoformat()
    except Exception as exc:
        db.rollback();BORROW_SYNC_STATUS["errors"].append({"symbol":"SYSTEM","error":str(exc)[:300]})
        if run:
            try:run=db.get(SyncRun,run.id);run.running=False;run.finished_at=datetime.utcnow();run.errors_json=json.dumps(BORROW_SYNC_STATUS["errors"]);db.commit()
            except:db.rollback()
    finally:BORROW_SYNC_STATUS["running"]=False;db.close()
    return BORROW_SYNC_STATUS
@router.get("/health")
def health():return {"ok":True}
@router.get("/test/ibkr-ftp/{symbol}")
async def test_ibkr_ftp(symbol:str):
    try:return await probe_ibkr_ftp(symbol)
    except Exception as exc:raise HTTPException(status_code=502,detail=f"IBKR FTP test failed: {str(exc)[:240]}")
@router.get("/sync/splits")
async def sync_splits_now(db:Session=Depends(get_db)):return await sync_splits(db,start=RANGE_START,end=RANGE_END)
@router.get("/sync/prices")
async def sync_prices_now():
    if not PRICE_SYNC_STATUS["running"]:asyncio.create_task(run_price_sync(start=RANGE_START,end=RANGE_END))
    return {"started":True,**PRICE_SYNC_STATUS}
@router.get("/sync/prices/status")
def sync_prices_status():return PRICE_SYNC_STATUS
@router.get("/sync/borrow/all")
async def sync_borrow_all():
    if not BORROW_SYNC_STATUS["running"]:asyncio.create_task(run_borrow_sync())
    return {"started":True,**BORROW_SYNC_STATUS}
@router.get("/sync/borrow/status")
def sync_borrow_status(db:Session=Depends(get_db)):
    r=db.scalar(select(SyncRun).where(SyncRun.kind=="borrow").order_by(SyncRun.id.desc()).limit(1));return _run_payload(r)
@router.get("/sync/borrow/{symbol}")
async def sync_borrow_symbol(symbol:str,db:Session=Depends(get_db)):
    symbol=symbol.upper().strip();stock=db.scalar(select(Stock).where(Stock.symbol==symbol))
    if not stock:raise HTTPException(status_code=404,detail="symbol not found")
    snapshot=await fetch_borrow_snapshot(symbol)
    if not snapshot:raise HTTPException(status_code=404,detail="borrow data not found")
    row=BorrowSnapshot(stock_id=stock.id,ts=datetime.utcnow(),available_shares=snapshot["available_shares"],fee_rate=snapshot["fee_rate"],rebate_rate=snapshot["rebate_rate"],source=snapshot["source"]);db.add(row);db.flush();sp=db.scalar(select(Split).where(Split.stock_id==stock.id).order_by(Split.effective_date.desc(),Split.id.desc()).limit(1))
    if sp:refresh_split_metrics(db,sp)
    db.commit();return {"symbol":symbol,"available":row.available_shares,"ctb":row.fee_rate,"fee_rate":row.fee_rate,"rebate_rate":row.rebate_rate,"source":row.source,"timestamp":row.ts,"source_reported_at":snapshot.get("reported_at"),"exchange":snapshot.get("exchange")}
@router.get("/borrow/{symbol}/history")
def borrow_history(symbol:str,limit:int=100,db:Session=Depends(get_db)):
    symbol=symbol.upper().strip();stock=db.scalar(select(Stock).where(Stock.symbol==symbol))
    if not stock:raise HTTPException(status_code=404,detail="symbol not found")
    limit=max(1,min(limit,1000));rows=db.scalars(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==stock.id).order_by(BorrowSnapshot.ts.desc(),BorrowSnapshot.id.desc()).limit(limit)).all();return [{"timestamp":r.ts,"available":r.available_shares,"ctb":r.fee_rate,"fee_rate":r.fee_rate,"rebate_rate":r.rebate_rate,"source":r.source} for r in rows]

@router.get("/lab-stats")
def lab_stats(db:Session=Depends(get_db)):
    rows=db.scalars(select(HuntSignal).order_by(HuntSignal.ready_at.desc())).all()
    total=len(rows); launched=sum(1 for r in rows if r.launched_at is not None)
    return {"ready_cycles":total,"launched_cycles":launched,"observed_launch_rate_pct":round(launched/total*100,1) if total else None,
      "recent":[{"stock_id":r.stock_id,"ready_at":r.ready_at,"ready_price":r.ready_price,"ready_available":r.ready_available,"launched_at":r.launched_at,"max_rise_pct":r.max_rise_pct} for r in rows[:30]],
      "note":"Observed historical journal only; not a prediction."}

@router.get("/events")
def events(limit:int=40,db:Session=Depends(get_db)):
    limit=max(1,min(limit,100)); out=[]
    for sp,st in _latest_split_rows(db):
        snaps=db.scalars(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==st.id).order_by(BorrowSnapshot.ts.desc(),BorrowSnapshot.id.desc()).limit(2)).all()
        if snaps:
            cur=snaps[0]
            if cur.available_shares==0: out.append({"symbol":st.symbol,"kind":"zero_short","at":cur.ts,"text":"Available وصل 0"})
            if len(snaps)>1 and cur.available_shares is not None and snaps[1].available_shares is not None and cur.available_shares<snaps[1].available_shares:
                out.append({"symbol":st.symbol,"kind":"borrow_drop","at":cur.ts,"text":f"Available {snaps[1].available_shares:g} → {cur.available_shares:g}"})
        if sp.stability_sessions and sp.stability_sessions>=4:
            out.append({"symbol":st.symbol,"kind":"stable","at":datetime.combine(sp.last_low_date or sp.effective_date,datetime.min.time()),"text":"ثبات 4 جلسات"})
    out.sort(key=lambda x:x["at"] or datetime.min,reverse=True)
    return out[:limit]

@router.get("/borrow-trends")
def borrow_trends(db:Session=Depends(get_db)):
    out={}
    for sp,st in _latest_split_rows(db):
        rows=db.scalars(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==st.id).order_by(BorrowSnapshot.ts.desc(),BorrowSnapshot.id.desc()).limit(8)).all()
        if not rows:continue
        vals=[{"at":r.ts,"available":r.available_shares} for r in reversed(rows)]
        out[st.symbol]={"history":vals,"just_zero":len(rows)>1 and rows[0].available_shares==0 and rows[1].available_shares!=0}
    return out

@router.get("/splits")
def splits(db:Session=Depends(get_db)):
    rows=_latest_split_rows(db,include_future=True);rows.sort(key=lambda x:(x[0].effective_date,x[1].symbol),reverse=True);return [_split_payload(db,sp,s) for sp,s in rows]
@router.get("/stock/{symbol}")
def stock_detail(symbol:str,db:Session=Depends(get_db)):
    symbol=symbol.upper().strip();row=db.execute(select(Split,Stock).join(Stock,Stock.id==Split.stock_id).where(Stock.symbol==symbol,Split.effective_date>=RANGE_START,Split.effective_date<=RANGE_END).order_by(Split.effective_date.desc(),Split.id.desc()).limit(1)).first()
    if not row:raise HTTPException(status_code=404,detail="symbol not found")
    sp,s=row;return _split_payload(db,sp,s)
@router.get("/hunt")
def hunt(db:Session=Depends(get_db)):
    rows=_latest_split_rows(db);rows.sort(key=lambda x:_progressive_score(x[0],_latest_borrow(db,x[1].id)),reverse=True);return [_split_payload(db,sp,s) for sp,s in rows]
