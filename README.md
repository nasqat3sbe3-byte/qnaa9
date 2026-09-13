# Qanas Data Engine v0.1

Backend foundation for **قنص**: reverse-split discovery, post-split metrics, borrow snapshots, and the **القنص بالركادة** readiness feed.

## Current logic
- Imports reverse splits from 2026-06-01 onward.
- Lifecycle: `pending -> awaiting_trade -> active`.
- Stores first post-split open, highest high, current post-split low, current price, distance from low, 50% level, whether it was reached, and stability sessions (0-4).
- Any new lower low resets stability naturally because metrics are recomputed from the latest current low.
- Borrow history is stored as snapshots; latest Available can feed the readiness score.
- `/api/hunt` exposes the fields needed for **القنص بالركادة**.

## Important
The score weights in `app/services/scoring.py` are deliberately provisional. The owner has defined the criteria and priority, but not the final point weights yet.

Live price bars and authenticated IBKR borrow collection are provider-specific and should be connected next. The IBKR adapter is scaffolded; it requires an authenticated Client Portal/Web API Gateway session.

## Local run
```bash
cp .env.example .env
pip install -r requirements.txt
python scripts/sync_splits.py
uvicorn app.main:app --reload
```

Open `/docs`, `/api/splits`, or `/api/hunt`.

## Render
Connect this repo to Render and deploy using `render.yaml`. Use paid persistent Postgres for production history.
