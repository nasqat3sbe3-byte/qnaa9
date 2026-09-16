from fastapi import APIRouter
import time, re
import httpx

router=APIRouter()
_CACHE={"at":0,"positive":[],"negative":[]}
POS=("contract","award","approval","approved","partnership","partner","collaboration","acquisition","merger","license","milestone","successful","launch","order","agreement","patent","clearance")
NEG=("offering","registered direct","private placement","bankruptcy","delisting","deficiency","reverse split","warrant exercise","going concern","default","dilution","noncompliance","non-compliance","chapter 11")

def tone(title):
    t=(title or "").lower()
    if any(w in t for w in NEG): return "negative"
    if any(w in t for w in POS): return "positive"
    return "neutral"

async def _load():
    global _CACHE
    now=time.time()
    if now-_CACHE["at"]<120: return _CACHE
    url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=100&output=atom"
    headers={"User-Agent":"Qanas market research contact admin@qanas.local","Accept":"application/atom+xml"}
    try:
        async with httpx.AsyncClient(timeout=6,headers=headers) as c:
            r=await c.get(url); r.raise_for_status(); txt=r.text
        positive=[]; negative=[]
        for e in re.findall(r"<entry>(.*?)</entry>",txt,re.S):
            tm=re.search(r"<title[^>]*>(.*?)</title>",e,re.S)
            title=re.sub(r"<[^>]+>"," ",tm.group(1) if tm else "")
            title=" ".join(title.split())
            m=re.search(r"\(([A-Z]{1,6})\)",title)
            if not m: continue
            lm=re.search(r'href="([^"]+)"',e); um=re.search(r"<updated>(.*?)</updated>",e,re.S)
            item={"symbol":m.group(1),"title":title,"published_at":um.group(1) if um else "","url":lm.group(1) if lm else "","source":"SEC"}
            t=tone(title)
            if t=="positive": positive.append({**item,"positive":True})
            elif t=="negative": negative.append({**item,"negative":True})
        _CACHE={"at":now,"positive":positive,"negative":negative}
    except Exception:
        if not _CACHE["at"]: _CACHE={"at":now,"positive":[],"negative":[]}
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
