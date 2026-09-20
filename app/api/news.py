from fastapi import APIRouter
import time, re
from datetime import date, datetime, timedelta, timezone
import httpx

router=APIRouter()
_CACHE={"at":0,"positive":[],"negative":[],"neutral":[],"mixed":[]}
_COMPANY_CACHE={}
_TICKERS={"at":0,"map":{}}
POS=("contract","award","approval","approved","partnership","partner","collaboration","acquisition","merger","license","milestone","successful","launch","order","agreement","patent","clearance")
NEG=("prospectus","at-the-market","atm offering","offering","registered direct","private placement","bankruptcy","delisting","deficiency","reverse split","warrant exercise","going concern","default","dilution","noncompliance","non-compliance","chapter 11","at-the-market","atm offering")

def tone(title):
    t=(title or "").lower()
    pos=any(w in t for w in POS); neg=any(w in t for w in NEG)
    if pos and neg: return "mixed"
    if neg: return "negative"
    if pos: return "positive"
    return "neutral"

def _published_date(value):
    try: return datetime.fromisoformat((value or "").replace("Z","+00:00")).date()
    except Exception: return None

def _window_start(split_date=None):
    sixty=date.today()-timedelta(days=60)
    if not split_date: return sixty
    try: sd=date.fromisoformat(str(split_date)[:10])
    except Exception: return sixty
    return max(sixty,sd)

SEC_FORMS={"S-1","S-1/A","S-3","S-3/A","F-1","F-1/A","F-3","F-3/A","424B3","424B4","424B5","EFFECT","6-K","8-K"}
async def _company_filings(symbol,start):
    global _TICKERS
    key=(symbol,start.isoformat()); now=time.time()
    hit_cache=_COMPANY_CACHE.get(key)
    if hit_cache and now-hit_cache["at"]<600:return hit_cache["items"]
    headers={"User-Agent":"Qanas market research admin@qanas.local"}
    async with httpx.AsyncClient(timeout=10,headers=headers,follow_redirects=True) as c:
        if now-_TICKERS["at"]>21600 or not _TICKERS["map"]:
            r=await c.get("https://www.sec.gov/files/company_tickers.json"); r.raise_for_status()
            _TICKERS={"at":now,"map":{str(v.get("ticker","")).upper():v for v in r.json().values()}}
        hit=_TICKERS["map"].get(symbol)
        if not hit:return []
        cik=str(hit["cik_str"]).zfill(10)
        r=await c.get("https://data.sec.gov/submissions/CIK"+cik+".json"); r.raise_for_status()
        z=r.json().get("filings",{}).get("recent",{}); out=[]
        for i,form in enumerate(z.get("form",[])):
            if i>=len(z.get("filingDate",[])):break
            fd=_published_date(z["filingDate"][i])
            if not fd or fd<start:continue
            form=str(form).upper()
            if form not in SEC_FORMS:continue
            acc=z.get("accessionNumber",[])[i]; doc=z.get("primaryDocument",[])[i]
            url="https://www.sec.gov/Archives/edgar/data/"+str(int(cik))+"/"+acc.replace("-","")+"/"+doc
            body=""
            try:
                rr=await c.get(url); rr.raise_for_status(); body=re.sub(r"<[^>]+>"," ",rr.text); body=" ".join(body.split())[:100000]
            except Exception:pass
            t=tone(body)
            if form in {"S-1","S-1/A","S-3","S-3/A","F-1","F-1/A","F-3","F-3/A","424B3","424B4","424B5","EFFECT"} and t=="neutral":t="negative"
            out.append({"symbol":symbol,"title":form+" · SEC filing","published_at":z["filingDate"][i],"url":url,"source":"SEC","tone":t,"form":form})
        _COMPANY_CACHE[key]={"at":now,"items":out}
        return out

async def _load():
    global _CACHE
    now=time.time()
    if now-_CACHE["at"]<120: return _CACHE
    url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=100&output=atom"
    headers={"User-Agent":"Qanas market research contact admin@qanas.local","Accept":"application/atom+xml"}
    try:
        async with httpx.AsyncClient(timeout=6,headers=headers) as c:
            r=await c.get(url); r.raise_for_status(); txt=r.text
        positive=[]; negative=[]; neutral=[]; mixed=[]
        for e in re.findall(r"<entry>(.*?)</entry>",txt,re.S):
            tm=re.search(r"<title[^>]*>(.*?)</title>",e,re.S)
            title=re.sub(r"<[^>]+>"," ",tm.group(1) if tm else "")
            title=" ".join(title.split())
            m=re.search(r"\(([A-Z]{1,6})\)",title)
            if not m: continue
            lm=re.search(r'href="([^"]+)"',e); um=re.search(r"<updated>(.*?)</updated>",e,re.S)
            item={"symbol":m.group(1),"title":title,"published_at":um.group(1) if um else "","url":lm.group(1) if lm else "","source":"SEC"}
            t=tone(title)
            if t=="positive": positive.append({**item,"tone":"positive","positive":True})
            elif t=="negative": negative.append({**item,"tone":"negative","negative":True})
            elif t=="mixed": mixed.append({**item,"tone":"mixed"})
            else: neutral.append({**item,"tone":"neutral"})
        _CACHE={"at":now,"positive":positive,"negative":negative,"neutral":neutral,"mixed":mixed}
    except Exception:
        if not _CACHE["at"]: _CACHE={"at":now,"positive":[],"negative":[],"neutral":[],"mixed":[]}
    return _CACHE

@router.get("/news-positive")
async def news_positive():
    return (await _load())["positive"]

@router.get("/news-negative")
async def news_negative():
    return (await _load())["negative"]

@router.get("/news-radar")
async def news_radar():
    c=await _load()
    return {"positive":c["positive"],"negative":c["negative"]}


@router.get("/news-context/{symbol}")
async def news_context(symbol:str, split_date:str|None=None):
    symbol=symbol.upper().strip()
    c=await _load(); start=_window_start(split_date)
    try: items=await _company_filings(symbol,start)
    except Exception: items=[]
    for bucket in ("negative","positive","mixed","neutral"):
        for x in c.get(bucket,[]):
            if x.get("symbol")!=symbol: continue
            pd=_published_date(x.get("published_at"))
            if pd and pd < start: continue
            items.append(x)
    items.sort(key=lambda x:x.get("published_at") or "",reverse=True)
    counts={k:sum(1 for x in items if x.get("tone")==k) for k in ("positive","negative","mixed","neutral")}
    if counts["negative"] and counts["positive"]: overall="mixed"
    elif counts["negative"]: overall="negative"
    elif counts["positive"]: overall="positive"
    elif counts["mixed"]: overall="mixed"
    elif items: overall="neutral"
    else: overall="none"
    return {"symbol":symbol,"window_start":start.isoformat(),"window_end":date.today().isoformat(),"overall":overall,"counts":counts,"items":items,"note":"SEC company filings plus filing-content classification; informational only and does not change readiness."}


@router.get("/news-context-batch")
async def news_context_batch(symbols:str="",split_dates:str=""):
    syms=[x.strip().upper() for x in symbols.split(",") if x.strip()][:250]
    dates=[x.strip() for x in split_dates.split(",")]
    out={}
    for i,sym in enumerate(syms):
        sd=dates[i] if i<len(dates) else None
        out[sym]=await news_context(sym,sd)
    return out
