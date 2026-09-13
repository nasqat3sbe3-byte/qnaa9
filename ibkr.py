import httpx
from ..config import settings

# IBKR Client Portal/Web API adapter. Requires an authenticated IBKR Gateway session.
# Fields vary by endpoint/account permissions; mapping is isolated here on purpose.
async def fetch_borrow_snapshot(conid: str):
    url = f"{settings.ibkr_base_url}/iserver/marketdata/snapshot"
    params = {"conids": conid, "fields": "236,7633"}
    async with httpx.AsyncClient(verify=settings.ibkr_verify_ssl, timeout=20) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    if not data:
        return None
    row = data[0]
    return {
        "available_shares": _num(row.get("236")),
        "fee_rate": _num(row.get("7633")),
        "rebate_rate": None,
        "source": "IBKR",
    }

def _num(v):
    if v in (None, "", "N/A"):
        return None
    try:
        return float(str(v).replace(",", "").replace("%", ""))
    except ValueError:
        return None
