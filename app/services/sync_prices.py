import asyncio
from datetime import date
import httpx
from sqlalchemy import select, delete
from ..db import SessionLocal
from ..models import Split, Stock, DailyBar, FourHourBar
from ..providers.prices import fetch_daily_bars, fetch_four_hour_bars
from .metrics import refresh_split_metrics
from .sync_splits import sync_splits

PRICE_SYNC_STATUS = {
    "running": False,
    "processed": 0,
    "total": 0,
    "bars_upserted": 0,
    "four_hour_bars_upserted": 0,
    "errors": [],
    "last_finished": None,
}


def _clear_metrics(split: Split):
    split.status = "awaiting_trade"
    split.post_split_open = None
    split.post_split_high = None
    split.post_split_low = None
    split.current_price = None
    split.distance_from_low_pct = None
    split.half_level = None
    split.half_level_reached = False
    split.stability_sessions = 0
    split.last_low_date = None
    split.ready_score = None


async def _fetch_one(client, sem, symbol, start, end):
    async with sem:
        daily = None
        fourh = []
        daily_err = None
        fourh_err = None
        try:
            daily = await fetch_daily_bars(client, symbol, start, end)
        except Exception as exc:
            daily_err = str(exc)
        try:
            fourh = await fetch_four_hour_bars(client, symbol, start, end)
        except Exception as exc:
            fourh_err = str(exc)
        return symbol, daily, fourh, daily_err, fourh_err


async def run_price_sync(start=date(2026, 6, 1), end=date(2026, 9, 30)):
    if PRICE_SYNC_STATUS["running"]:
        return PRICE_SYNC_STATUS

    PRICE_SYNC_STATUS.update({
        "running": True,
        "processed": 0,
        "total": 0,
        "bars_upserted": 0,
        "four_hour_bars_upserted": 0,
        "errors": [],
        "last_finished": None,
    })
    db = SessionLocal()
    try:
        # Corporate actions are refreshed before prices so stale ratios (e.g. YMT)
        # are corrected before any downstream calculations run.
        await sync_splits(db, start=start, end=end)

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

        sem = asyncio.Semaphore(4)
        headers = {"User-Agent": "Mozilla/5.0 QanasDataEngine/1.0"}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
            tasks = [_fetch_one(client, sem, stock.symbol, split.effective_date, today) for split, stock in rows]
            results = await asyncio.gather(*tasks)

        result_map = {symbol: (daily, fourh, daily_err, fourh_err) for symbol, daily, fourh, daily_err, fourh_err in results}

        for split, stock in rows:
            daily, fourh, daily_err, fourh_err = result_map.get(stock.symbol, (None, [], "missing result", "missing result"))

            # Trusted daily data: purge stale legacy/Yahoo values first. If the
            # trusted source is unavailable we leave metrics blank rather than wrong.
            db.execute(delete(DailyBar).where(
                DailyBar.stock_id == stock.id,
                DailyBar.trade_date >= split.effective_date,
            ))
            if daily_err or not daily:
                _clear_metrics(split)
                PRICE_SYNC_STATUS["errors"].append({"symbol": stock.symbol, "layer": "daily", "error": (daily_err or "no bars")[:180]})
            else:
                for bar in daily:
                    db.add(DailyBar(stock_id=stock.id, **bar))
                    PRICE_SYNC_STATUS["bars_upserted"] += 1
                db.flush()
                refresh_split_metrics(db, split)

            # 4h layer is deliberately separate and can fail without poisoning
            # trusted daily metrics.
            db.execute(delete(FourHourBar).where(
                FourHourBar.stock_id == stock.id,
                FourHourBar.bar_time >= split.effective_date,
            ))
            if fourh_err:
                PRICE_SYNC_STATUS["errors"].append({"symbol": stock.symbol, "layer": "4h", "error": fourh_err[:180]})
            else:
                for bar in fourh:
                    db.add(FourHourBar(stock_id=stock.id, **bar))
                    PRICE_SYNC_STATUS["four_hour_bars_upserted"] += 1

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
