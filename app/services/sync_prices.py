import asyncio
from datetime import date
import httpx
from sqlalchemy import select
from ..db import SessionLocal
from ..models import Split, Stock, DailyBar
from ..providers.prices import fetch_daily_bars
from .metrics import refresh_split_metrics

PRICE_SYNC_STATUS = {
    "running": False,
    "processed": 0,
    "total": 0,
    "bars_upserted": 0,
    "errors": [],
    "last_finished": None,
}


async def _fetch_one(client, sem, symbol, start, end):
    async with sem:
        try:
            bars = await fetch_daily_bars(client, symbol, start, end)
            return symbol, bars, None
        except Exception as exc:
            return symbol, [], str(exc)


async def run_price_sync(start=date(2026, 6, 1), end=date(2026, 9, 30)):
    if PRICE_SYNC_STATUS["running"]:
        return PRICE_SYNC_STATUS

    PRICE_SYNC_STATUS.update({"running": True, "processed": 0, "total": 0, "bars_upserted": 0, "errors": [], "last_finished": None})
    db = SessionLocal()
    try:
        today = date.today()
        rows = db.execute(
            select(Split, Stock)
            .join(Stock, Stock.id == Split.stock_id)
            .where(
                Split.effective_date >= start,
                Split.effective_date <= end,
                Split.effective_date <= today,
            )
            .order_by(Split.effective_date.asc(), Stock.symbol.asc())
        ).all()
        PRICE_SYNC_STATUS["total"] = len(rows)

        sem = asyncio.Semaphore(5)
        headers = {"User-Agent": "Mozilla/5.0 QanasDataEngine/1.0"}
        async with httpx.AsyncClient(timeout=25, follow_redirects=True, headers=headers) as client:
            tasks = [_fetch_one(client, sem, stock.symbol, split.effective_date, today) for split, stock in rows]
            results = await asyncio.gather(*tasks)

        result_map = {symbol: (bars, err) for symbol, bars, err in results}

        for split, stock in rows:
            bars, err = result_map.get(stock.symbol, ([], "missing result"))
            if err:
                PRICE_SYNC_STATUS["errors"].append({"symbol": stock.symbol, "error": err[:180]})
                PRICE_SYNC_STATUS["processed"] += 1
                continue

            for bar in bars:
                existing = db.scalar(select(DailyBar).where(DailyBar.stock_id == stock.id, DailyBar.trade_date == bar["trade_date"]))
                if existing:
                    existing.open = bar["open"]
                    existing.high = bar["high"]
                    existing.low = bar["low"]
                    existing.close = bar["close"]
                    existing.volume = bar["volume"]
                else:
                    db.add(DailyBar(stock_id=stock.id, **bar))
                PRICE_SYNC_STATUS["bars_upserted"] += 1

            db.flush()
            refresh_split_metrics(db, split)
            PRICE_SYNC_STATUS["processed"] += 1

        db.commit()
        PRICE_SYNC_STATUS["last_finished"] = date.today().isoformat()
    except Exception as exc:
        db.rollback()
        PRICE_SYNC_STATUS["errors"].append({"symbol": "SYSTEM", "error": str(exc)[:300]})
    finally:
        PRICE_SYNC_STATUS["running"] = False
        db.close()
    return PRICE_SYNC_STATUS
