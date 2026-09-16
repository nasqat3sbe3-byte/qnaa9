from fastapi import APIRouter
import asyncio, time, re
import httpx

router=APIRouter()
_CACHE={"at":0,"data":[]}
POS=("contract","award","approval","approved","partnership","partner","collaboration","acquisition","merger","license","milestone","positive","successful","launch","order","agreement","patent","clearance")
NEG=("offering","registered direct","private placement","bankruptcy","delisting","deficiency","reverse split","warrant exercise","going concern","default")

def tone(title):
    t=(title or "").lower()
    if any(w in t for w in NEG): return False
    return any(w in t for w in POS)

async def _load():
    global _CACHE
    now=time.time()
    if now-_CACHE["at"]<120: return _CACHE["data"]
    # SEC current filings feed; fail-open to empty so site performance is unaffected.
    url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&company=&dateb=&owner=include&count=100&output=atom"
    headers={"User-Agent":"Qanas market research contact admin@qanas.local","Accept":"application/atom+xml"}
    try:
        async with httpx.AsyncClient(timeout=6,headers=headers) as c:
            r=await c.get(url); r.raise_for_status(); txt=r.text
        entries=re.findall(r"<entry>(.*?)</entry>",txt,re.S)
        out=[]
        for e in entries:
            title=re.sub(r"<[^>]+>"," ",re.search(r"<title[^>]*>(.*?)</title>",e,re.S).group(1) if re.search(r"<title[^>]*>(.*?)</title>",e,re.S) else "")
            link=(re.search(r'href="([^"]+)"',e) or [None,""])[1]
            updated=(re.search(r"<updated>(.*?)</updated>",e,re.S) or [None,""])[1]
            m=re.search(r"\(([A-Z]{1,6})\)",title)
            if m and tone(title): out.append({"symbol":m.group(1),"title":" ".join(title.split()),"published_at":updated,"url":link,"positive":True,"source":"SEC"})
        _CACHE={"at":now,"data":out}
    except Exception:
        if not _CACHE["data"]: _CACHE={"at":now,"data":[]}
    return _CACHE["data"]

@router.get("/news-positive")
async def news_positive():
    return await _load()
