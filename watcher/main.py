import asyncio
import os
import time
from datetime import datetime, timezone

from fastapi import FastAPI

app = FastAPI(title="Qanas Watcher", version="0.1.0")
BOOTED_AT = datetime.now(timezone.utc)
STATE = {
    "status": "starting",
    "heartbeat": None,
    "heartbeat_count": 0,
    "booted_at": BOOTED_AT.isoformat(),
    "pid": os.getpid(),
}

async def heartbeat_loop():
    while True:
        STATE["status"] = "running"
        STATE["heartbeat"] = datetime.now(timezone.utc).isoformat()
        STATE["heartbeat_count"] += 1
        await asyncio.sleep(10)

@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat_loop())

@app.get("/")
async def root():
    return {
        "service": "qanas-watcher",
        "message": "Qanas Engine is alive",
        **STATE,
        "uptime_seconds": int(time.time() - BOOTED_AT.timestamp()),
    }

@app.get("/health")
async def health():
    last = STATE["heartbeat"]
    age = None
    if last:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
    return {
        "ok": bool(last) and age < 30,
        "heartbeat_age_seconds": age,
        **STATE,
    }
