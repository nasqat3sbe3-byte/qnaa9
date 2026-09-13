from datetime import date
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import Split, Stock, BorrowSnapshot, FourHourBar, DailyBar
from ..services.sync_splits import sync_splits
from ..services.sync_prices import PRICE_SYNC_STATUS, run_price_sync

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _four_hour_stats(db: Session, stock_id: int, effective_date: date):
    bars = db.scalars(
        select(FourHourBar)
        .where(FourHourBar.stock_id == stock_id)
        .order_by(FourHourBar.bar_time.asc())
    ).all()
    bars = [b for b in bars if b.bar_time.date() >= effective_date and b.open and b.open > 0]
    if not bars:
        return {
            "four_hour_highest_rise_pct": None,
            "four_hour_highest_rise_open": None,
            "four_hour_highest_rise_high": None,
            "four_hour_highest_rise_time": None,
            "four_hour_highest_price": None,
            "four_hour_source": None,
        }
    best = max(bars, key=lambda b: ((b.high / b.open) - 1) * 100)
    highest_price = max(b.high for b in bars)
    return {
        "four_hour_highest_rise_pct": round(((best.high / best.open) - 1) * 100, 2),
        "four_hour_highest_rise_open": best.open,
        "four_hour_highest_rise_high": best.high,
        "four_hour_highest_rise_time": best.bar_time.isoformat(),
        "four_hour_highest_price": highest_price,
        "four_hour_source": best.source,
    }


def _split_day_stats(db: Session, stock_id: int, effective_date: date):
    first_bar = db.scalar(
        select(DailyBar)
        .where(DailyBar.stock_id == stock_id, DailyBar.trade_date >= effective_date)
        .order_by(DailyBar.trade_date.asc())
        .limit(1)
    )
    if not first_bar:
        return {
            "split_day_date": None,
            "split_day_open": None,
            "split_day_high": None,
            "split_day_low": None,
            "split_day_close": None,
        }
    return {
        "split_day_date": first_bar.trade_date,
        "split_day_open": first_bar.open,
        "split_day_high": first_bar.high,
        "split_day_low": first_bar.low,
        "split_day_close": first_bar.close,
    }


def _split_payload(db: Session, sp: Split, s: Stock):
    payload = {
        "symbol": s.symbol,
        "company": s.company_name,
        "effective_date": sp.effective_date,
        "ratio": f"{sp.split_from:g} for {sp.split_to:g}",
        "status": sp.status,
        "post_split_open": sp.post_split_open,
        "post_split_high": sp.post_split_high,
        "post_split_low": sp.post_split_low,
        "current_price": sp.current_price,
        "distance_from_low_pct": sp.distance_from_low_pct,
        "stability_sessions": sp.stability_sessions,
        "half_level": sp.half_level,
        "half_level_reference": "split_day_high",
        "half_level_reached": sp.half_level_reached,
        "ready_score": sp.ready_score,
    }
    payload.update(_split_day_stats(db, s.id, sp.effective_date))
    payload.update(_four_hour_stats(db, s.id, sp.effective_date))
    return payload


@router.get("/health")
def health():
    return {"ok": True}


@router.get("/sync/splits")
async def sync_splits_now(db: Session = Depends(get_db)):
    return await sync_splits(db, start=date(2026, 6, 1), end=date(2026, 9, 30))


@router.get("/sync/prices")
async def sync_prices_now():
    import asyncio
    if not PRICE_SYNC_STATUS["running"]:
        asyncio.create_task(run_price_sync())
    return {"started": True, **PRICE_SYNC_STATUS}


@router.get("/sync/prices/status")
def sync_prices_status():
    return PRICE_SYNC_STATUS


@router.get("/splits")
def splits(db: Session = Depends(get_db)):
    rows = db.execute(
        select(Split, Stock)
        .join(Stock, Stock.id == Split.stock_id)
        .where(Split.effective_date >= date(2026, 6, 1), Split.effective_date <= date(2026, 9, 30))
        .order_by(Split.effective_date.desc(), Stock.symbol.asc())
    ).all()
    return [_split_payload(db, sp, s) for sp, s in rows]


@router.get("/stock/{symbol}")
def stock_detail(symbol: str, db: Session = Depends(get_db)):
    symbol = symbol.upper().strip()
    row = db.execute(
        select(Split, Stock)
        .join(Stock, Stock.id == Split.stock_id)
        .where(Stock.symbol == symbol)
        .order_by(Split.effective_date.desc())
        .limit(1)
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="symbol not found")
    sp, s = row
    payload = _split_payload(db, sp, s)
    b = db.scalar(
        select(BorrowSnapshot)
        .where(BorrowSnapshot.stock_id == s.id)
        .order_by(BorrowSnapshot.ts.desc())
        .limit(1)
    )
    payload.update({
        "available": b.available_shares if b else None,
        "fee_rate": b.fee_rate if b else None,
        "rebate_rate": b.rebate_rate if b else None,
        "borrow_source": b.source if b else None,
        "borrow_timestamp": b.ts if b else None,
    })
    return payload


@router.get("/hunt")
def hunt(db: Session = Depends(get_db)):
    rows = db.execute(
        select(Split, Stock)
        .join(Stock)
        .where(Split.status == "active")
        .order_by(Split.ready_score.desc().nullslast())
    ).all()
    out = []
    for sp, s in rows:
        b = db.scalar(
            select(BorrowSnapshot)
            .where(BorrowSnapshot.stock_id == s.id)
            .order_by(BorrowSnapshot.ts.desc())
            .limit(1)
        )
        item = {
            "symbol": s.symbol,
            "ready_score": sp.ready_score,
            "available": b.available_shares if b else None,
            "fee_rate": b.fee_rate if b else None,
            "low": sp.post_split_low,
            "current": sp.current_price,
            "distance_from_low_pct": sp.distance_from_low_pct,
            "stability_sessions": sp.stability_sessions,
            "half_level": sp.half_level,
            "half_level_reference": "split_day_high",
            "half_level_reached": sp.half_level_reached,
            "post_split_open": sp.post_split_open,
            "post_split_high": sp.post_split_high,
        }
        item.update(_split_day_stats(db, s.id, sp.effective_date))
        item.update(_four_hour_stats(db, s.id, sp.effective_date))
        out.append(item)
    return out
