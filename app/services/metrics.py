from sqlalchemy import select
from sqlalchemy.orm import Session
from ..models import Split, DailyBar, BorrowSnapshot
from .scoring import compute_score


def refresh_split_metrics(db: Session, split: Split):
    bars = db.scalars(
        select(DailyBar)
        .where(
            DailyBar.stock_id == split.stock_id,
            DailyBar.trade_date >= split.effective_date,
        )
        .order_by(DailyBar.trade_date)
    ).all()
    if not bars:
        return

    split.status = "active"
    split.post_split_open = bars[0].open

    # Dynamic extremes from the split onward. Every sync recalculates them from
    # the complete trusted post-split history, so a new high/low updates automatically.
    post_split_high = max(b.high for b in bars)
    post_split_low = min(b.low for b in bars)
    split.post_split_high = post_split_high
    split.post_split_low = post_split_low

    split.current_price = bars[-1].close
    split.distance_from_low_pct = (
        ((split.current_price / post_split_low) - 1) * 100
        if post_split_low else None
    )

    # Qanas half-level rule: always use the highest price reached since the split.
    # Example: the stock later makes a new high at 7 => half-level becomes 3.50.
    split.half_level = post_split_high / 2 if post_split_high else None

    # Re-evaluate against the full post-split history whenever the high changes.
    split.half_level_reached = bool(
        split.half_level is not None
        and any(b.low <= split.half_level for b in bars)
    )

    # Stability resets whenever a newer all-time post-split low appears.
    low_index = max(i for i, b in enumerate(bars) if b.low == post_split_low)
    split.stability_sessions = min(4, max(0, len(bars) - 1 - low_index))
    split.last_low_date = bars[low_index].trade_date

    latest_borrow = db.scalar(
        select(BorrowSnapshot)
        .where(BorrowSnapshot.stock_id == split.stock_id)
        .order_by(BorrowSnapshot.ts.desc())
        .limit(1)
    )
    available = latest_borrow.available_shares if latest_borrow else None
    split.ready_score = compute_score(
        available,
        split.stability_sessions,
        split.half_level_reached,
        split.distance_from_low_pct,
    )
