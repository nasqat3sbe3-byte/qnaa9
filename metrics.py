from datetime import date
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..models import Split, DailyBar, BorrowSnapshot
from .scoring import compute_score

def refresh_split_metrics(db: Session, split: Split):
    bars = db.scalars(select(DailyBar).where(DailyBar.stock_id==split.stock_id, DailyBar.trade_date>=split.effective_date).order_by(DailyBar.trade_date)).all()
    if not bars:
        return
    split.status = "active"
    split.post_split_open = bars[0].open
    split.post_split_high = max(b.high for b in bars)
    new_low = min(b.low for b in bars)
    split.post_split_low = new_low
    split.current_price = bars[-1].close
    split.distance_from_low_pct = ((split.current_price / new_low) - 1) * 100 if new_low else None
    split.half_level = split.post_split_open / 2 if split.post_split_open else None
    split.half_level_reached = bool(split.half_level and new_low <= split.half_level)

    low_index = max(i for i,b in enumerate(bars) if b.low == new_low)
    # Completed sessions after the most recent session that set the current low; capped at 4.
    split.stability_sessions = min(4, max(0, len(bars)-1-low_index))
    split.last_low_date = bars[low_index].trade_date

    latest_borrow = db.scalar(select(BorrowSnapshot).where(BorrowSnapshot.stock_id==split.stock_id).order_by(BorrowSnapshot.ts.desc()).limit(1))
    available = latest_borrow.available_shares if latest_borrow else None
    split.ready_score = compute_score(available, split.stability_sessions, split.half_level_reached, split.distance_from_low_pct)
