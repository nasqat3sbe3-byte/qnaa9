import asyncio
from datetime import date

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import BorrowSnapshot, DailyBar, FourHourBar, Split, Stock
from ..providers.ibkr import diagnose_borrow_sources, fetch_borrow_snapshot
from ..services.metrics import refresh_split_metrics
from ..services.sync_prices import PRICE_SYNC_STATUS, run_price_sync
from ..services.sync_splits import sync_splits

router = APIRouter()

BORROW_SYNC_STATUS = {
    "running": False,
    "processed": 0,
    "total": 0,
    "saved": 0,
    "not_found": 0,
    "errors": [],
    "last_finished": None,
}


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
    return {
        "four_hour_highest_rise_pct": round(((best.high / best.open) - 1) * 100, 2),
        "four_hour_highest_rise_open": best.open,
        "four_hour_highest_rise_high": best.high,
        "four_hour_highest_rise_time": best.bar_time.isoformat(),
        "four_hour_highest_price": max(b.high for b in bars),
        "four_hour_source": best.source,
    }


def _latest_borrow(db: Session, stock_id: int):
    return db.scalar(
        select(BorrowSnapshot)
        .where(BorrowSnapshot.stock_id == stock_id)
        .order_by(BorrowSnapshot.ts.desc())
        .limit(1)
    )


def _split_payload(db: Session, sp: Split, s: Stock):
    b = _latest_borrow(db, s.id)
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
        "available": b.available_shares if b else None,
        "ctb": b.fee_rate if b else None,
        "fee_rate": b.fee_rate if b else None,
        "rebate_rate": b.rebate_rate if b else None,
        "borrow_source": b.source if b else None,
        "borrow_timestamp": b.ts if b else None,
    }
    payload.update(_four_hour_stats(db, s.id, sp.effective_date))
    return payload


async def _borrow_fetch_one(client, sem, symbol):
    async with sem:
        try:
            return symbol, await fetch_borrow_snapshot(symbol, client=client), None
        except Exception as exc:
            return symbol, None, str(exc)


async def run_borrow_sync():
    if BORROW_SYNC_STATUS["running"]:
        return BORROW_SYNC_STATUS

    BORROW_SYNC_STATUS.update({
        "running": True,
        "processed": 0,
        "total": 0,
        "saved": 0,
        "not_found": 0,
        "errors": [],
        "last_finished": None,
    })

    db = SessionLocal()
    try:
        today = date.today()
        rows = db.execute(
            select(Split, Stock)
            .join(Stock, Stock.id == Split.stock_id)
            .where(
                Split.effective_date >= date(2026, 6, 1),
                Split.effective_date <= date(2026, 9, 30),
                Split.effective_date <= today,
            )
            .order_by(Stock.symbol.asc())
        ).all()

        by_symbol = {}
        for sp, stock in rows:
            by_symbol[stock.symbol] = (sp, stock)
        targets = list(by_symbol.items())
        BORROW_SYNC_STATUS["total"] = len(targets)

        sem = asyncio.Semaphore(6)
        headers = {"User-Agent": "Mozilla/5.0 QanasDataEngine/1.0"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=headers) as client:
            results = await asyncio.gather(*[
                _borrow_fetch_one(client, sem, symbol) for symbol, _ in targets
            ])

        result_map = {symbol: (snap, err) for symbol, snap, err in results}

        for symbol, (sp, stock) in targets:
            snap, err = result_map.get(symbol, (None, "missing result"))
            BORROW_SYNC_STATUS["processed"] += 1

            if err:
                BORROW_SYNC_STATUS["errors"].append({"symbol": symbol, "error": err[:180]})
                continue
            if not snap:
                BORROW_SYNC_STATUS["not_found"] += 1
                continue

            latest = _latest_borrow(db, stock.id)
            same_snapshot = bool(
                latest
                and latest.ts == snap["reported_at"]
                and latest.available_shares == snap["available_shares"]
                and latest.fee_rate == snap["fee_rate"]
            )
            if not same_snapshot:
                db.add(BorrowSnapshot(
                    stock_id=stock.id,
                    ts=snap["reported_at"],
                    available_shares=snap["available_shares"],
                    fee_rate=snap["fee_rate"],
                    rebate_rate=snap["rebate_rate"],
                    source=snap["source"],
                ))
                db.flush()
                BORROW_SYNC_STATUS["saved"] += 1

            refresh_split_metrics(db, sp)

        db.commit()
        BORROW_SYNC_STATUS["last_finished"] = date.today().isoformat()
    except Exception as exc:
        db.rollback()
        BORROW_SYNC_STATUS["errors"].append({"symbol": "SYSTEM", "error": str(exc)[:300]})
    finally:
        BORROW_SYNC_STATUS["running"] = False
        db.close()

    return BORROW_SYNC_STATUS


@router.get("/health")
def health():
    return {"ok": True}


@router.get("/sync/splits")
async def sync_splits_now(db: Session = Depends(get_db)):
    return await sync_splits(db, start=date(2026, 6, 1), end=date(2026, 9, 30))


@router.get("/sync/prices")
async def sync_prices_now():
    if not PRICE_SYNC_STATUS["running"]:
        asyncio.create_task(run_price_sync())
    return {"started": True, **PRICE_SYNC_STATUS}


@router.get("/sync/prices/status")
def sync_prices_status():
    return PRICE_SYNC_STATUS


@router.get("/debug/borrow/{symbol}")
async def debug_borrow(symbol: str):
    return await diagnose_borrow_sources(symbol)


@router.get("/sync/borrow/all")
async def sync_borrow_all():
    if not BORROW_SYNC_STATUS["running"]:
        asyncio.create_task(run_borrow_sync())
    return {"started": True, **BORROW_SYNC_STATUS}


@router.get("/sync/borrow/status")
def sync_borrow_status():
    return BORROW_SYNC_STATUS


@router.get("/sync/borrow/{symbol}")
async def sync_borrow_symbol(symbol: str, db: Session = Depends(get_db)):
    symbol = symbol.upper().strip()
    stock = db.scalar(select(Stock).where(Stock.symbol == symbol))
    if not stock:
        raise HTTPException(status_code=404, detail="symbol not found")

    snapshot = await fetch_borrow_snapshot(symbol)
    if not snapshot:
        raise HTTPException(status_code=404, detail="borrow data not found")

    latest = _latest_borrow(db, stock.id)
    same_snapshot = bool(
        latest
        and latest.ts == snapshot["reported_at"]
        and latest.available_shares == snapshot["available_shares"]
        and latest.fee_rate == snapshot["fee_rate"]
    )
    if same_snapshot:
        row = latest
    else:
        row = BorrowSnapshot(
            stock_id=stock.id,
            ts=snapshot["reported_at"],
            available_shares=snapshot["available_shares"],
            fee_rate=snapshot["fee_rate"],
            rebate_rate=snapshot["rebate_rate"],
            source=snapshot["source"],
        )
        db.add(row)
        db.flush()

    sp = db.scalar(
        select(Split)
        .where(Split.stock_id == stock.id)
        .order_by(Split.effective_date.desc())
        .limit(1)
    )
    if sp:
        refresh_split_metrics(db, sp)
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
    rows = db.execute(
        select(Split, Stock)
        .join(Stock, Stock.id == Split.stock_id)
        .where(
            Split.effective_date >= date(2026, 6, 1),
            Split.effective_date <= date(2026, 9, 30),
        )
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
    return _split_payload(db, sp, s)


@router.get("/hunt")
def hunt(db: Session = Depends(get_db)):
    rows = db.execute(
        select(Split, Stock)
        .join(Stock)
        .where(Split.status == "active")
        .order_by(Split.ready_score.desc().nullslast(), Stock.symbol.asc())
    ).all()
    return [_split_payload(db, sp, s) for sp, s in rows]
