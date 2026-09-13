from fastapi import FastAPI
from .db import init_db
from .api.routes import router

app=FastAPI(title="Qanas Data Engine", version="0.1.0")
@app.on_event("startup")
def startup(): init_db()
app.include_router(router, prefix="/api")
