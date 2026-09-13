from datetime import date
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import Split, Stock, BorrowSnapshot, FourHourBar, DailyBar
from ..providers.ibkr import fetch_borrow_snapshot
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
    bars = db.scalars(select(FourHourBar).where(FourHourBar.stock_id == stock_id).order_by(FourHourBar.bar_time.asc())).all()
    bars = [b for b in bars if b.bar_time.date() >= effective_date and b.open and b.open > 0]
    if not bars:
        return {"four_hour_highest_rise_pct": None, "four_hour_highest_rise_open": None, "four_hour_highest_rise_high": None, "four_hour_highest_rise_time": None, "four_hour_highest_price": None, "four_hour_source": None}
    best = max(bars, key=lambda b: ((b.high / b.open) - 1) * 100)
    return {
        "four_hour_highest_rise_pct": round(((best.high / best.open) - 1) * 100, 2),
        "four_hour_highest_rise_open": best.open,
        "four_hour_highest_rise_high": best.high,
        "four_hour_highest_rise_time": best.bar_time.isoformat(),
        "four_hour_highest_price": max(b.high for b in bars),
        "four_hour_source": best.source,
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
        "half_level_reference": "highest_price_since_split",
        "half_level_reached": sp.half_level_reached,
        "ready_score": sp.ready_score,
    }
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


@router.get("/sync/borrow/{symbol}")
async def sync_borrow_symbol(symbol: str, db: Session = Depends(get_db)):
    symbol = symbol.upper().strip()
    stock = db.scalar(select(Stock).where(Stock.symbol == symbol))
    if not stock:
        raise HTTPException(status_code=404, detail="symbol not found")

    snapshot = await fetch_borrow_snapshot(symbol)
    if not snapshot:
        raise HTTPException(status_code=404, detail="borrow data not found")

    row = BorrowSnapshot(
        stock_id=stock.id,
        ts=snapshot["reported_at"],
        available_shares=snapshot["available_shares"],
        fee_rate=snapshot["fee_rate"],
        rebate_rate=snapshot["rebate_rate"],
        source=snapshot["source"],
    )
    db.add(row)
    db.commit()
    return {
        "symbol": symbol,
        "available": row.available_shares,
        "ctb": row.fee_rate,
        "fee_rate": row.fee_rate,
        "rebate_rate": row.rebate_rate,
        "source": row.source,
        "timestamp": row.ts,
        "exchange": snapshot.get("exchange"),
    }


@router.get("/borrow/{symbol}/history")
def borrow_history(symbol: str, limit: int = 100, db: Session = Depends(get_db)):
    symbol = symbol.upper().strip()
    stock = db.scalar(select(Stock).where(Stock.symbol == symbol))
    if not stock:
        raise HTTPException(status_code=404, detail="symbol not found")
    limit = max(1, min(limit, 1000))
    rows = db.scalars(
        select(BorrowSnapshot)
        .where(BorrowSnapshot.stock_id == stock.id)
        .order_by(BorrowSnapshot.ts.desc())
        .limit(limit)
    ).all()
    return [
        {
            "timestamp": r.ts,
            "available": r.available_shares,
            "ctb": r.fee_rate,
            "fee_rate": r.fee_rate,
            "rebate_rate": r.rebate_rate,
            "source": r.source,
        }
        for r in rows
    ]


@router.get("/splits")
def splits(db: Session = Depends(get_db)):
    rows = db.execute(select(Split, Stock).join(Stock, Stock.id == Split.stock_id).where(Split.effective_date >= date(2026, 6, 1), Split.effective_date <= date(2026, 9, 30)).order_by(Split.effective_date.desc(), Stock.symbol.asc())).all()
    return [_split_payload(db, sp, s) for sp, s in rows]


@router.get("/stock/{symbol}")
def stock_detail(symbol: str, db: Session = Depends(get_db)):
    symbol = symbol.upper().strip()
    row = db.execute(select(Split, Stock).join(Stock, Stock.id == Split.stock_id).where(Stock.symbol == symbol).order_by(Split.effective_date.desc()).limit(1)).first()
    if not row:
        raise HTTPException(status_code=404, detail="symbol not found")
    sp, s = row
    payload = _split_payload(db, sp, s)
    b = db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id == s.id).order_by(BorrowSnapshot.ts.desc()).limit(1))
    payload.update({
        "available": b.available_shares if b else None,
        "ctb": b.fee_rate if b else None,
        "fee_rate": b.fee_rate if b else None,
        "rebate_rate": b.rebate_rate if b else None,
        "borrow_source": b.source if b else None,
        "borrow_timestamp": b.ts if b else None,
    })
    return payload


@router.get("/hunt")
def hunt(db: Session = Depends(get_db)):
    rows = db.execute(select(Split, Stock).join(Stock).where(Split.status == "active").order_by(Split.ready_score.desc().nullslast())).all()
    out = []
    for sp, s in rows:
        b = db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id == s.id).order_by(BorrowSnapshot.ts.desc()).limit(1))
        item = {
            "symbol": s.symbol,
            "ready_score": sp.ready_score,
            "available": b.available_shares if b else None,
            "ctb": b.fee_rate if b else None,
            "fee_rate": b.fee_rate if b else None,
            "rebate_rate": b.rebate_rate if b else None,
            "post_split_low": sp.post_split_low,
            "current_price": sp.current_price,
            "distance_from_low_pct": sp.distance_from_low_pct,
            "stability_sessions": sp.stability_sessions,
            "half_level": sp.half_level,
            "half_level_reference": "highest_price_since_split",
            "half_level_reached": sp.half_level_reached,
            "post_split_open": sp.post_split_open,
            "post_split_high": sp.post_split_high,
        }
        item.update(_four_hour_stats(db, s.id, sp.effective_date))
        out.append(item)
    return out
