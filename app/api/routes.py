from datetime import date
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import Split, Stock, BorrowSnapshot
from ..services.sync_splits import sync_splits

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/health")
def health():
    return {"ok": True}

@router.get("/sync/splits")
async def sync_splits_now(db: Session = Depends(get_db)):
    return await sync_splits(db, start=date(2026, 6, 1), end=date(2026, 9, 30))

@router.get("/splits")
def splits(db: Session = Depends(get_db)):
    rows = db.execute(
        select(Split, Stock)
        .join(Stock, Stock.id == Split.stock_id)
        .where(Split.effective_date >= date(2026, 6, 1), Split.effective_date <= date(2026, 9, 30))
        .order_by(Split.effective_date.desc(), Stock.symbol.asc())
    ).all()
    return [{"symbol": s.symbol, "company": s.company_name, "effective_date": sp.effective_date, "ratio": f"{sp.split_from:g} for {sp.split_to:g}", "status": sp.status, "post_split_open": sp.post_split_open, "post_split_high": sp.post_split_high, "post_split_low": sp.post_split_low, "current_price": sp.current_price, "distance_from_low_pct": sp.distance_from_low_pct, "stability_sessions": sp.stability_sessions, "half_level": sp.half_level, "half_level_reached": sp.half_level_reached, "ready_score": sp.ready_score} for sp, s in rows]

@router.get("/hunt")
def hunt(db: Session = Depends(get_db)):
    rows = db.execute(select(Split, Stock).join(Stock).where(Split.status == "active").order_by(Split.ready_score.desc().nullslast())).all()
    out = []
    for sp, s in rows:
        b = db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id == s.id).order_by(BorrowSnapshot.ts.desc()).limit(1))
        out.append({"symbol": s.symbol, "ready_score": sp.ready_score, "available": b.available_shares if b else None, "fee_rate": b.fee_rate if b else None, "low": sp.post_split_low, "current": sp.current_price, "distance_from_low_pct": sp.distance_from_low_pct, "stability_sessions": sp.stability_sessions, "half_level_reached": sp.half_level_reached, "post_split_open": sp.post_split_open, "post_split_high": sp.post_split_high})
    return out
