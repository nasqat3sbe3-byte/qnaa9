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

    intraday = db.scalars(
        select(FourHourBar)
        .where(FourHourBar.stock_id == split.stock_id)
        .order_by(FourHourBar.bar_time.asc())
    ).all()
    intraday = [b for b in intraday if b.bar_time.date() >= split.effective_date]

    split.status = "active"
    split.post_split_open = bars[0].open

    # Dynamic extremes from the split onward. Use intraday/extended-hours bars when
    # available so spikes such as YMT's $5 print are not lost in the daily candle.
    daily_high = max(b.high for b in bars)
    daily_low = min(b.low for b in bars)
    intraday_high = max((b.high for b in intraday), default=None)
    intraday_low = min((b.low for b in intraday), default=None)

    post_split_high = max(v for v in (daily_high, intraday_high) if v is not None)
    post_split_low = min(v for v in (daily_low, intraday_low) if v is not None)

    split.post_split_high = post_split_high
    split.post_split_low = post_split_low
    split.current_price = bars[-1].close
    split.distance_from_low_pct = (
        ((split.current_price / post_split_low) - 1) * 100
        if post_split_low else None
    )

    # Qanas half-level rule: highest actual price reached since the split / 2.
    split.half_level = post_split_high / 2 if post_split_high else None

    # Once a peak is made, count the half-level only if price reaches/touches it
    # afterward. Prefer chronological intraday bars when available.
    if intraday and split.half_level is not None:
        peak_index = max(range(len(intraday)), key=lambda i: intraday[i].high)
        split.half_level_reached = any(
            b.low <= split.half_level for b in intraday[peak_index:]
        )
    else:
        # Daily fallback when intraday data is unavailable.
        peak_index = max(range(len(bars)), key=lambda i: bars[i].high)
        split.half_level_reached = bool(
            split.half_level is not None
            and any(b.low <= split.half_level for b in bars[peak_index:])
        )

    # Stability is session-based, so keep this calculation on daily sessions.
    # A new lower daily-session low resets the counter.
    session_low = min(b.low for b in bars)
    low_index = max(i for i, b in enumerate(bars) if b.low == session_low)
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
