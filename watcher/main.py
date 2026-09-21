import asyncio
import ftplib
import io
import os
import re
import time
from datetime import datetime, timezone, date

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI

app = FastAPI(title="Qanas Watcher", version="0.3.0")
BOOTED_AT = datetime.now(timezone.utc)
QANAS_WEB = "https://qnaa9.onrender.com"
UNIVERSE_SEED = ["MSGY","WCT","NCT","EPOW","CPOP","LGCL","NRSN","HUBC","MGN","FGL","OMH","AIXI","SFWL","TNMG","LRHC","RCON","CXAI","YYAI","YXT","RBNE","CISS","IZM","GAUZ","LGHL","UCAR","HLSQ","ALP","GTBP","GOSS","JAGX","NFE","IPDN","NXXT","ENLV","STKH","TRIB","FFAI"]
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FTP_HOST, FTP_USER, FTP_PASSWORD, FTP_FILE = "ftp2.interactivebrokers.com", "shortstock", "", "usa.txt"
SPLITS_URLS = ("https://stockanalysis.com/actions/splits/2026/", "https://stockanalysis.com/actions/splits/")

STATE = {
    "status":"starting","heartbeat":None,"heartbeat_count":0,"booted_at":BOOTED_AT.isoformat(),
    "universe_count":len(UNIVERSE_SEED),"last_universe_sync":None,"universe_error":None,
    "market_scan_count":0,"last_market_scan":None,"market_ok":0,"market_failed":0,"last_market_error":None,
    "borrow_scan_count":0,"last_borrow_scan":None,"borrow_ok":0,"borrow_missing":0,"last_borrow_error":None,
    "pid":os.getpid(),
}
UNIVERSE = {s:{"symbol":s,"effective_date":None,"source":"seed"} for s in UNIVERSE_SEED}
QUOTES = {}
BORROW = {}
EVENTS = []

def utcnow(): return datetime.now(timezone.utc)
def add_event(symbol, kind, text, data=None):
    EVENTS.insert(0,{"symbol":symbol,"kind":kind,"text":text,"at":utcnow().isoformat(),"data":data or {}})
    del EVENTS[100:]

async def heartbeat_loop():
    while True:
        STATE["status"]="running"; STATE["heartbeat"]=utcnow().isoformat(); STATE["heartbeat_count"]+=1
        await asyncio.sleep(10)

async def fetch_direct_universe(client):
    # Yield immediately so the web server can finish binding to :8080 first.
    await asyncio.sleep(0)
    merged={}
    for url in SPLITS_URLS:
        r=await client.get(url,timeout=30); r.raise_for_status()
        soup=BeautifulSoup(r.text,"html.parser")
        for tr in soup.select("table tbody tr"):
            tds=[td.get_text(" ",strip=True) for td in tr.select("td")]
            if len(tds)<5 or tds[3].lower()!="reverse": continue
            try: eff=datetime.strptime(tds[0],"%b %d, %Y").date()
            except Exception: continue
            if eff < date(2026,6,1) or eff > utcnow().date(): continue
            sym=tds[1].upper().strip()
            if sym: merged[sym]={"symbol":sym,"company":tds[2],"effective_date":eff.isoformat(),"ratio":tds[4],"source":"stockanalysis"}
    if not merged: raise RuntimeError("empty direct split feed")
    UNIVERSE.clear(); UNIVERSE.update(merged)
    STATE["universe_count"]=len(UNIVERSE); STATE["last_universe_sync"]=utcnow().isoformat(); STATE["universe_error"]=None
    return True

async def sync_universe(client):
    last_error=None
    try:
        if await fetch_direct_universe(client): return
    except Exception as exc:
        last_error=f"direct: {type(exc).__name__}"
    for path in ("/api/hunt","/api/splits"):
        try:
            r=await client.get(QANAS_WEB+path,timeout=60); r.raise_for_status(); rows=r.json()
            if not isinstance(rows,list) or not rows: raise RuntimeError("empty universe")
            fresh={}
            today=utcnow().date().isoformat()
            for x in rows:
                sym=str(x.get("symbol") or "").upper().strip()
                eff=str(x.get("effective_date") or "")[:10]
                if sym and (not eff or eff<=today): fresh[sym]=x
            if fresh:
                UNIVERSE.clear(); UNIVERSE.update(fresh)
                STATE["universe_count"]=len(UNIVERSE); STATE["last_universe_sync"]=utcnow().isoformat()
                STATE["universe_error"]=None
                return
        except Exception as exc: last_error=f"{path}: {type(exc).__name__}"
    # Render can be slow to wake up. Never leave the watcher empty while it retries.
    if not UNIVERSE:
        UNIVERSE.update({s:{"symbol":s,"effective_date":None,"source":"seed"} for s in UNIVERSE_SEED})
        STATE["universe_count"]=len(UNIVERSE)
    STATE["universe_error"]=last_error or "unknown"

async def universe_loop():
    # Keep Northflank ingress healthy before any external scraping starts.
    await asyncio.sleep(15)
    headers={"User-Agent":"Mozilla/5.0 QanasWatcher/0.3"}
    async with httpx.AsyncClient(follow_redirects=True,headers=headers) as client:
        while True:
            await sync_universe(client)
            await asyncio.sleep(120)

async def fetch_quote(client, sem, symbol):
    async with sem:
        try:
            r=await client.get(YAHOO.format(symbol=symbol),params={"range":"1d","interval":"1m","includePrePost":"true","events":"history"})
            r.raise_for_status(); result=(r.json().get("chart",{}).get("result") or [None])[0]
            if not result:return symbol,None
            ts=result.get("timestamp") or []; q=((result.get("indicators") or {}).get("quote") or [{}])[0]
            closes=q.get("close") or []; highs=q.get("high") or []; lows=q.get("low") or []
            valid=[(int(t),float(closes[i])) for i,t in enumerate(ts) if i<len(closes) and closes[i] is not None and float(closes[i])>0]
            if not valid:return symbol,None
            t,p=max(valid,key=lambda z:z[0]); hi=[float(v) for v in highs if v is not None and float(v)>0]; lo=[float(v) for v in lows if v is not None and float(v)>0]
            return symbol,{"symbol":symbol,"price":p,"day_high":max(hi) if hi else p,"day_low":min(lo) if lo else p,
                "market_timestamp":datetime.fromtimestamp(t,tz=timezone.utc).isoformat(),"received_at":utcnow().isoformat(),"source":"yahoo_1m_prepost"}
        except Exception:return symbol,None

async def market_loop():
    headers={"User-Agent":"Mozilla/5.0 QanasWatcher/0.3"}
    limits=httpx.Limits(max_connections=12,max_keepalive_connections=10)
    async with httpx.AsyncClient(timeout=8,follow_redirects=True,headers=headers,limits=limits) as client:
        while True:
            started=time.monotonic()
            syms=list(UNIVERSE)
            if syms:
                sem=asyncio.Semaphore(10)
                rows=await asyncio.gather(*(fetch_quote(client,sem,s) for s in syms))
                ok=0
                for s,row in rows:
                    if row is not None: QUOTES[s]=row; ok+=1
                STATE["market_scan_count"]+=1; STATE["last_market_scan"]=utcnow().isoformat()
                STATE["market_ok"]=ok; STATE["market_failed"]=len(syms)-ok
                STATE["last_market_error"]=None if ok else "no quotes returned"
            await asyncio.sleep(max(5,60-(time.monotonic()-started)))

def download_ibkr():
    ftp=ftplib.FTP(timeout=20)
    try:
        ftp.connect(FTP_HOST,21); ftp.login(FTP_USER,FTP_PASSWORD); data=io.BytesIO(); ftp.retrbinary("RETR "+FTP_FILE,data.write)
        return data.getvalue().decode("utf-8",errors="replace")
    finally:
        try: ftp.quit()
        except Exception:
            try: ftp.close()
            except Exception: pass

def parse_ibkr(text):
    out={}
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):continue
        f=[x.strip().strip('"') for x in re.split(r"[|\t]",raw.strip())]
        if len(f)<8 or f[1].upper().strip()!="USD":continue
        sym=f[0].upper().strip()
        try: rebate=float(f[5]); fee=float(f[6]); available=float(f[7].replace(",",""))
        except Exception:continue
        if sym and available>=0:out[sym]={"available":available,"ctb":fee,"rebate":rebate,"source":"IBKR public FTP usa.txt"}
    return out

async def delayed_borrow_start():
    await asyncio.sleep(45)
    await borrow_loop()

async def borrow_loop():
    while True:
        try:
            text=await asyncio.wait_for(asyncio.to_thread(download_ibkr),timeout=30)
            rows=parse_ibkr(text); now=utcnow().isoformat(); changed=0
            for sym in list(UNIVERSE):
                new=rows.get(sym)
                if not new:continue
                new={**new,"received_at":now}; old=BORROW.get(sym)
                if old:
                    oa,na=old.get("available"),new.get("available")
                    if oa is not None and na is not None and na<oa:
                        changed+=1; add_event(sym,"available_down",f"Available {oa:g} -> {na:g}",{"old":oa,"new":na})
                    if oa!=0 and na==0:add_event(sym,"available_zero","Available reached 0",{"old":oa,"new":0})
                BORROW[sym]=new
            STATE["borrow_scan_count"]+=1; STATE["last_borrow_scan"]=now; STATE["borrow_ok"]=sum(1 for s in UNIVERSE if s in rows)
            STATE["borrow_missing"]=max(0,len(UNIVERSE)-STATE["borrow_ok"]); STATE["last_borrow_error"]=None
        except Exception as exc: STATE["last_borrow_error"]=f"{type(exc).__name__}: {str(exc)[:120]}"
        await asyncio.sleep(300)

@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat_loop()); asyncio.create_task(universe_loop())
    asyncio.create_task(market_loop()); asyncio.create_task(delayed_borrow_start())

@app.get("/")
async def root():
    return {"service":"qanas-watcher","message":"Qanas Engine is alive","version":"0.3.0",**STATE,
        "prices_ready":len(QUOTES),"borrow_ready":len(BORROW),"events":len(EVENTS),
        "uptime_seconds":int(time.time()-BOOTED_AT.timestamp()),
        "endpoints":["/health","/universe","/prices","/borrow","/snapshot","/events"]}

@app.get("/health")
async def health():
    last=STATE["heartbeat"]; age=(utcnow()-datetime.fromisoformat(last)).total_seconds() if last else None
    return {"ok":bool(last) and age<30,"heartbeat_age_seconds":age,**STATE}

@app.get("/universe")
async def universe():
    return {"count":len(UNIVERSE),"last_sync":STATE["last_universe_sync"],"error":STATE["universe_error"],
        "symbols":sorted(UNIVERSE)}

@app.get("/prices")
async def prices():
    return {"source":"yahoo_1m_prepost","last_market_scan":STATE["last_market_scan"],"market_scan_count":STATE["market_scan_count"],
        "ok":STATE["market_ok"],"failed":STATE["market_failed"],"count":len(QUOTES),"quotes":QUOTES}

@app.get("/borrow")
async def borrow():
    return {"source":"IBKR public FTP usa.txt","last_borrow_scan":STATE["last_borrow_scan"],"borrow_scan_count":STATE["borrow_scan_count"],
        "ok":STATE["borrow_ok"],"missing":STATE["borrow_missing"],"error":STATE["last_borrow_error"],"count":len(BORROW),"rows":BORROW}

@app.get("/snapshot")
async def snapshot():
    rows={}
    for sym,meta in UNIVERSE.items():
        rows[sym]={"symbol":sym,"effective_date":meta.get("effective_date"),"price":QUOTES.get(sym),"borrow":BORROW.get(sym)}
    return {"generated_at":utcnow().isoformat(),"count":len(rows),"rows":rows}

@app.get("/events")
async def events():
    return {"count":len(EVENTS),"events":EVENTS}
