from sqlalchemy import select
from sqlalchemy.orm import Session
from ..models import Split, DailyBar, FourHourBar, BorrowSnapshot
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

    # Highest trusted daily price seen after the split (all sessions).
    split.post_split_high = max(b.high for b in bars)

    # Post-split low/current metrics.
    new_low = min(b.low for b in bars)
    split.post_split_low = new_low
    split.current_price = bars[-1].close
    split.distance_from_low_pct = ((split.current_price / new_low) - 1) * 100 if new_low else None

    # Qanas half-level rule:
    # Reference = highest *intraday* price reached on the split effective date,
    # including pre-market/after-hours. Do not fall back to the daily candle high;
    # if intraday data is unavailable, leave this signal unknown instead of wrong.
    intraday_split_day = db.scalars(
        select(FourHourBar)
        .where(FourHourBar.stock_id == split.stock_id)
        .order_by(FourHourBar.bar_time.asc())
    ).all()
    intraday_split_day = [
        b for b in intraday_split_day
        if b.bar_time.date() == split.effective_date
    ]
    split_day_intraday_high = max((b.high for b in intraday_split_day), default=None)
    split.half_level = split_day_intraday_high / 2 if split_day_intraday_high else None

    # Once the intraday reference exists, consider the half level reached if any
    # trusted post-split daily low traded at or below it.
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
