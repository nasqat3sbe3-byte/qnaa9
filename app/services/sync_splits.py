from datetime import date
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..models import Stock, Split
from ..providers.splits import fetch_reverse_splits

DEFAULT_START = date(2026, 5, 1)
DEFAULT_END = date(2026, 9, 30)

async def sync_splits(db: Session, start: date = DEFAULT_START, end: date = DEFAULT_END):
    rows = await fetch_reverse_splits()
    added = 0
    updated = 0
    matched = 0

    for r in rows:
        effective_date = r["effective_date"]
        if effective_date < start or effective_date > end:
            continue
        matched += 1

        stock = db.scalar(select(Stock).where(Stock.symbol == r["symbol"]))
        if not stock:
            stock = Stock(symbol=r["symbol"], company_name=r["company_name"])
            db.add(stock)
            db.flush()
        elif r.get("company_name") and stock.company_name != r["company_name"]:
            stock.company_name = r["company_name"]

        split = db.scalar(
            select(Split).where(
                Split.stock_id == stock.id,
                Split.effective_date == effective_date,
            )
        )
        status = "pending" if effective_date > date.today() else "awaiting_trade"
        if not split:
            split = Split(
                stock_id=stock.id,
                effective_date=effective_date,
                split_from=r["split_from"],
                split_to=r["split_to"],
                split_type="reverse",
                status=status,
            )
            db.add(split)
            added += 1
        else:
            changed = False
            for field, value in (
                ("split_from", r["split_from"]),
                ("split_to", r["split_to"]),
                ("split_type", "reverse"),
            ):
                if getattr(split, field) != value:
                    setattr(split, field, value)
                    changed = True
            if split.status == "pending" and effective_date <= date.today():
                split.status = "awaiting_trade"
                changed = True
            if changed:
                updated += 1

    db.commit()
    total = db.scalar(
        select(__import__("sqlalchemy").func.count(Split.id)).where(
            Split.effective_date >= start,
            Split.effective_date <= end,
            Split.split_type == "reverse",
        )
    )
    return {
        "source_rows_in_range": matched,
        "added": added,
        "updated": updated,
        "database_total_in_range": int(total or 0),
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
