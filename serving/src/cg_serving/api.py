"""CryptoGuard API (MLS-03/05/07). Skeleton: routes and shapes are fixed, handlers are TODO.

Run locally: uvicorn cg_serving.api:app --reload --port 8000   (PYTHONPATH=serving/src)
Every response returns contract-shaped documents (Alert v1, WalletRisk v1) minus Mongo's _id.
Timestamps stay *_ts_us integers; the UI formats them.
"""

from fastapi import FastAPI, HTTPException, Query, WebSocket

app = FastAPI(title="CryptoGuard API", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/alerts")
def list_alerts(
    symbol: str | None = None,
    exchange: str | None = None,
    alert_type: str | None = None,
    min_risk: float = Query(0.0, ge=0, le=1),
    from_ts_us: int | None = None,
    to_ts_us: int | None = None,
    limit: int = Query(50, le=500),
    cursor: str | None = None,
) -> dict:
    """Latest alerts, newest first. Returns {"items": [Alert v1...], "next_cursor": str|null}."""
    raise HTTPException(501, "MLS-03")


@app.get("/alerts/{alert_id}")
def get_alert(alert_id: str) -> dict:
    raise HTTPException(501, "MLS-03")


@app.get("/cases")
def list_cases(
    status: str | None = None, symbol: str | None = None, limit: int = Query(50, le=200)
) -> dict:
    raise HTTPException(501, "MLS-05")


@app.get("/cases/{case_id}")
def get_case(case_id: str) -> dict:
    """Case + its alerts + features from 30 min before to 30 min after (symbol_features_recent)."""
    raise HTTPException(501, "MLS-05")


@app.get("/symbols/{symbol}/features")
def symbol_features(
    symbol: str, exchange: str = "ALL", from_ts_us: int | None = None, to_ts_us: int | None = None
) -> dict:
    raise HTTPException(501, "MLS-05")


@app.get("/wallets/{address}/risk")
def wallet_risk(address: str) -> dict:
    """WalletRisk v1 for a lowercase 0x address; 404 if never seen."""
    raise HTTPException(501, "MLS-07")


@app.get("/wallets/{address}/graph")
def wallet_graph(address: str, hops: int = Query(2, ge=1, le=3)) -> dict:
    """Nodes/edges around an address from gold.wallet_edges (P4, optional)."""
    raise HTTPException(501, "MLS-07")


@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket) -> None:
    """Push new alerts (from a Mongo change stream or a Kafka consumer) to the dashboard."""
    await ws.accept()
    await ws.close(code=1011, reason="MLS-05")
