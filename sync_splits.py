from datetime import date
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..models import Stock, Split
from ..providers.splits import fetch_reverse_splits

async def sync_splits(db: Session, start=date(2026,6,1)):
    rows = await fetch_reverse_splits()
    count = 0
    for r in rows:
        if r["effective_date"] < start:
            continue
        stock = db.scalar(select(Stock).where(Stock.symbol==r["symbol"]))
        if not stock:
            stock = Stock(symbol=r["symbol"], company_name=r["company_name"])
            db.add(stock); db.flush()
        split = db.scalar(select(Split).where(Split.stock_id==stock.id, Split.effective_date==r["effective_date"]))
        if not split:
            split = Split(stock_id=stock.id, effective_date=r["effective_date"], split_from=r["split_from"], split_to=r["split_to"], status="pending" if r["effective_date"] > date.today() else "awaiting_trade")
            db.add(split); count += 1
    db.commit()
    return count
