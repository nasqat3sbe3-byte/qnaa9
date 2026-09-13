import asyncio
from app.db import init_db, SessionLocal
from app.services.sync_splits import sync_splits

async def main():
    init_db()
    db = SessionLocal()
    try:
        n = await sync_splits(db)
        print(f"added {n} splits")
    finally:
        db.close()

asyncio.run(main())
