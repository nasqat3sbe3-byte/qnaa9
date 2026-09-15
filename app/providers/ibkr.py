import asyncio
import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

_EXCHANGES=("nasdaq","nyse","nyseamerican","otc")
_EXCHANGE_ALIASES={"nasdaq":"nasdaq","nasdaqgs":"nasdaq","nasdaqgm":"nasdaq","nasdaqcm":"nasdaq","nyse":"nyse","nyseamerican":"nyseamerican","amex":"nyseamerican","american":"nyseamerican","otc":"otc"}
_LATEST_RE=re.compile(r"As of\s+(.+?),\s+there\s+(?:were|was)\s+([\d,]+)\s+shares?\s+available\s+with\s+a\s+fee\s+of\s+(-?[\d.]+)%",re.IGNORECASE)
_JINA_LOCK=None;_JINA_SEM=None;_JINA_LAST_CALL=0.0;_JINA_MIN_INTERVAL_NO_KEY=3.2;_JINA_KEY_CONCURRENCY=8

def _jina_api_key():return (os.getenv("JINA_API_KEY") or "").strip()
def _normalize_exchange(exchange):
    if not exchange:return None
    return _EXCHANGE_ALIASES.get(str(exchange).lower().replace(" ","").replace("-",""))
def _parse_dt(raw):
    try:
        value=dtparser.parse(raw)
        if value.tzinfo is not None:value=value.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        return value
    except Exception:return datetime.utcnow()
def _parse_chart_exchange(text):
    m=_LATEST_RE.search(text)
    if not m:return None
    reported_raw,available_raw,fee_raw=m.groups();available=float(available_raw.replace(",",""));fee=float(fee_raw)
    if available<0 or fee < -1000 or fee>10000:return None
    return {"available_shares":available,"fee_rate":fee,"rebate_rate":None,"reported_at":_parse_dt(reported_raw)}
async def _get_with_retry(client,url):
    for attempt in range(3):
        try:
            r=await client.get(url,headers={"Cache-Control":"no-cache","Pragma":"no-cache"})
            if r.status_code==200:return r
            if r.status_code not in (408,425,429,500,502,503,504):return None
        except (httpx.TimeoutException,httpx.TransportError):pass
        await asyncio.sleep(1+attempt)
    return None
async def _jina_request(client,target_url,key):
    headers={"Accept":"text/plain","X-Timeout":"20","X-No-Cache":"true","Cache-Control":"no-cache","Pragma":"no-cache"}
    if key:headers["Authorization"]=f"Bearer {key}"
    for attempt in range(3):
        try:
            r=await client.get(f"https://r.jina.ai/{target_url}",headers=headers,timeout=30)
            if r.status_code==200:return r.text
            if r.status_code not in (408,425,429,500,502,503,504):return None
        except (httpx.TimeoutException,httpx.TransportError):pass
        await asyncio.sleep(1+attempt)
    return None
async def _fetch_via_jina(client,target_url):
    global _JINA_LOCK,_JINA_SEM,_JINA_LAST_CALL
    key=_jina_api_key()
    if key:
        if _JINA_SEM is None:_JINA_SEM=asyncio.Semaphore(_JINA_KEY_CONCURRENCY)
        async with _JINA_SEM:return await _jina_request(client,target_url,key)
    if _JINA_LOCK is None:_JINA_LOCK=asyncio.Lock()
    async with _JINA_LOCK:
        wait_for=_JINA_MIN_INTERVAL_NO_KEY-(time.monotonic()-_JINA_LAST_CALL)
        if wait_for>0:await asyncio.sleep(wait_for)
        text=await _jina_request(client,target_url,"");_JINA_LAST_CALL=time.monotonic();return text
async def _fetch_exchange(client,symbol_lower,exchange):
    url=f"https://chartexchange.com/symbol/{exchange}-{symbol_lower}/borrow-fee/"
    r=await _get_with_retry(client,url)
    if r is not None:
        parsed=_parse_chart_exchange(BeautifulSoup(r.text,"html.parser").get_text(" ",strip=True))
        if parsed:parsed.update({"source":"ChartExchange/IBKR","exchange":exchange,"url":url});return parsed
    text=await _fetch_via_jina(client,url)
    if text:
        parsed=_parse_chart_exchange(text)
        if parsed:parsed.update({"source":"JinaFresh/ChartExchange/IBKR","exchange":exchange,"url":url});return parsed
    return None
async def fetch_borrow_snapshot(symbol,client=None,exchange=None):
    clean_symbol=symbol.upper().strip();symbol_lower=clean_symbol.lower();own_client=client is None
    if own_client:client=httpx.AsyncClient(timeout=30,follow_redirects=True,headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/153 Safari/537.36","Accept-Language":"en-US,en;q=0.9","Cache-Control":"no-cache","Pragma":"no-cache"})
    try:
        exact=_normalize_exchange(exchange)
        exchanges=(exact,) if exact else _EXCHANGES
        for ex in exchanges:
            parsed=await _fetch_exchange(client,symbol_lower,ex)
            if parsed:return parsed
        return None
    finally:
        if own_client and client is not None:await client.aclose()
