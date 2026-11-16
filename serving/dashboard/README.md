# Dashboard (MLS-05)

Decide the stack in P1 (record in an ADR): plain HTML + JS served by FastAPI static files (simplest, like the W1 mock),
or a small React/Vite app. Pages:

1. **Live feed**: WebSocket `/ws/alerts`, newest first, filter by type/severity/symbol.
2. **Case page**: price/volume chart around the case (from `/cases/{id}`), reasons table, linked alerts.
3. **Wallet page**: `/wallets/{address}/risk`, path to nearest bad wallet, (P4) graph view.
4. **Data quality** (O7) and **system** (O8) live in Grafana/Superset, linked from the header.
