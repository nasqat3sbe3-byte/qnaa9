import asyncio
import time
from datetime import datetime, timezone

_QUOTES = {}
_TASK = None
_SYMBOLS = set()

def turbo_quotes():
    return dict(_QUOTES)

def _price(msg):
    if not isinstance(msg, dict):
        return None
    for k in ("price","regularMarketPrice"):
        try:
            v=float(msg.get(k) or 0)
            if v>0:return v
        except Exception:pass
    return None

async def _runner():
    global _TASK
    try:
        import yfinance as yf
        ws=yf.AsyncWebSocket()
        await ws.subscribe(sorted(_SYMBOLS))
        async def handler(msg):
            p=_price(msg)
            sym=str(msg.get("id") or msg.get("symbol") or "").upper()
            if p>0 and sym:
                _QUOTES[sym]={"live_price":p,"live_timestamp":datetime.now(timezone.utc).isoformat(),"live_source":"yahoo_websocket","turbo_received_at":time.time()}
        await ws.listen(handler)
    except asyncio.CancelledError:
        raise
    except Exception:
        # Turbo is optional. Existing Yahoo 1m engine remains the fallback.
        await asyncio.sleep(5)
    finally:
        _TASK=None

async def ensure_turbo(symbols):
    global _TASK,_SYMBOLS
    wanted={str(s).upper().strip() for s in symbols if s}
    if wanted!=_SYMBOLS:
        _SYMBOLS=wanted
        if _TASK and not _TASK.done():_TASK.cancel()
        _TASK=None
    if _SYMBOLS and (_TASK is None or _TASK.done()):
        _TASK=asyncio.create_task(_runner())
    return turbo_quotes()
