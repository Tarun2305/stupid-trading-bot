# Naive Alpaca paper experiment

**PAPER TRADING ONLY.** This is an intentionally naive experiment, not a recommended trading strategy. The client is constructed with `paper=True`; startup verifies its paper flag and endpoint, and every trading HTTP request is restricted to the paper endpoint. There is no live-mode option. Supply paper-account credentials only; the program cannot identify a credential's account type from its text.

For each of the 30 symbols in `bot.py`'s `SYMBOLS` list, sample the latest IEX trade price every 15 seconds. A higher price buys $1,000 if flat; a lower price sells the entire current long quantity; an equal price does nothing. The first sample establishes a baseline. Open/recent orders and cooldowns block further orders. There are no indicators, stops, predictions, shorts, or leverage sizing logic. Existing shorts are ignored. Using available buying power is not a cash-only account guarantee.

## Architecture and reliability

- One `StockDataStream` thread subscribes to all configured symbols using `DataFeed.IEX`. Its callback only updates prices and receipt times under a lock. The main thread samples; incoming ticks never submit orders.
- Alpaca's clock gates trading. Closed markets are checked every 60 seconds and the next open is logged every five minutes. Session changes and reconnects reset baselines. Prices older than 120 seconds are treated as missing; the stream reconnects if silent for 120 seconds during an open market.
- Each cycle reads Alpaca positions, open orders, and orders submitted within the cooldown. Market buys use notional amounts. Market sells use the entire reported long quantity, including fractional shares, with `DAY` and `extended_hours=False`.
- A fresh clock check precedes each order, with a 20-second closing-boundary guard to avoid queuing a next-session DAY order. Network latency means the server ultimately determines acceptance/execution timing.
- `order_state.json` is atomically written before each submission, using a unique client order ID. Orders are never blindly retried. After restart, the bot queries each ID and retains its symbol lock until the order is terminal, cooldown has elapsed, and positions agree with filled quantity. Partial fills and delayed position updates remain protected.
- An ambiguous request that remains absent from Alpaca is deliberately kept blocked. Check the paper account's orders and the logged client ID. Only after confirming no order was accepted, stop the bot and remove that symbol's entry from `order_state.json`. Do not delete state to bypass an unresolved order. Manual trades can also cause a position reconciliation mismatch.
- Read-only requests retry transient network/429/5xx errors with bounded exponential backoff; failed cycles back off up to about 60 seconds. Authentication/permission and configuration failures stop the process. SDK 0.44.0 manages one socket with sequential reconnects and backoff. Small documented private-SDK hooks enforce timeouts, paper guards, and permanent-error handling; test before changing its pinned version.
- Run **one instance per paper account**, with no competing manual trades or other bots. An OS lock prevents two processes in the same data directory; it is not a distributed/account-wide lock. Persist the data directory across restarts.

## Configuration

Required environment variables: `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`. Supply the required paper-account values locally or through your host's secret environment settings. Never commit credentials.

| Variable | Default |
| --- | --- |
| `NOTIONAL_PER_TRADE` | `1000` dollars |
| `SIGNAL_INTERVAL_SECONDS` | `15` |
| `ORDER_COOLDOWN_SECONDS` | `20` |

All numeric settings must be finite and positive. Edit `SYMBOLS` in `bot.py` to change the universe. The default list contains exactly the requested 30 stocks. Environment variables override `.env`. IEX and paper trading are fixed.

## Local run

Install Python 3.12, then from this directory:

```sh
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows PowerShell instead:
# .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Copy `.env.example` to `.env` (`cp .env.example .env`, or `Copy-Item .env.example .env` in PowerShell). Fill in the two required variables locally, then:

```sh
python -u bot.py
```

Startup validates settings before API access and prints paper mode, equity, cash, buying power, symbol count, notional, and interval. Insufficient funds, unsupported fractional orders, or asset restrictions are logged as order rejections; there is no alternative strategy fallback.

## Docker

```sh
docker build -t naive-alpaca-paper .
docker volume create alpaca-paper-data
docker run -d --name alpaca-paper --env-file .env --mount source=alpaca-paper-data,target=/data naive-alpaca-paper
docker logs -f alpaca-paper
docker stop --time 60 alpaca-paper
```

The image runs `python -u /app/bot.py` directly. Docker's `/data` working directory stores `trades.csv`, `order_state.json`, and the instance lock. The image copies only source and dependencies; `.dockerignore` excludes secrets and runtime data. Rebuild after changing symbols or code. To copy the supplementary CSV:

```sh
docker cp alpaca-paper:/data/trades.csv ./trades.csv
```

## Always-on deployment and operations

Use a continuously running container/VM with outbound HTTPS and WebSocket access, accurate system time, the required secret environment variables, and a persistent writable working directory (mount `/data` for Docker). Keep replicas at one and avoid overlapping rolling deployments. Use bounded crash restarts and failure alerts; do not configure endless restarts for invalid credentials. Disable request-driven sleep/scale-to-zero. Set a shutdown grace period of at least 60 seconds and configure host log rotation/disk monitoring. The append-only CSV can grow; archive it only while stopped.

Console logs and CSV timestamps use UTC. Logs include signals, sent order IDs, cycle counts, closed-market checks, and errors. `trades.csv` records submission intentions, known responses, and reconciled terminal statuses with the requested eight columns. It is supplementary and can contain several rows per order. Alpaca's paper account is the authoritative position/order history; pending orders can finish while the bot is offline.

Stop locally with Ctrl+C or send SIGTERM; use `docker stop --time 60` in Docker. Shutdown stops sampling and the stream. It **does not liquidate positions or cancel accepted orders**. Inspect pending orders in the paper account before changing configuration or removing state. Restarting resets sampled prices and rereads actual positions and orders.

Alpaca's free IEX feed covers IEX rather than the full consolidated SIP market. Paper trading does not perfectly reproduce live execution, spread, slippage, market impact, or queue position. See the official [stock streaming SDK reference](https://alpaca.markets/sdks/python/api_reference/data/stock/live.html) and [order SDK reference](https://alpaca.markets/sdks/python/api_reference/trading/orders.html).

## Offline verification

```sh
python -m unittest discover -s tests -v
python -m compileall -q bot.py tests
```

Tests use mocks and do not connect to Alpaca or place orders. An authenticated paper-account smoke test still requires your environment variables. Docker must be installed to build/test the image.
