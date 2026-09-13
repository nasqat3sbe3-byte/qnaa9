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

    # First actual trading session after the reverse split.
    split_day = bars[0]
    split.post_split_open = split_day.open

    # Highest price seen after the split (all sessions).
    split.post_split_high = max(b.high for b in bars)

    # Post-split low/current metrics.
    new_low = min(b.low for b in bars)
    split.post_split_low = new_low
    split.current_price = bars[-1].close
    split.distance_from_low_pct = ((split.current_price / new_low) - 1) * 100 if new_low else None

    # Qanas half-level rule:
    # Do NOT use the opening price. Use the highest price reached on the
    # first post-split trading day. Example: open 3, split-day high 7 => half=3.50.
    split_day_high = split_day.high
    split.half_level = split_day_high / 2 if split_day_high else None

    # Consider the level reached if any post-split session traded at/below it.
    split.half_level_reached = bool(
        split.half_level is not None
        and any(b.low <= split.half_level for b in bars)
    )

    # Stability resets naturally whenever a newer all-time post-split low appears.
    low_index = max(i for i, b in enumerate(bars) if b.low == new_low)
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
