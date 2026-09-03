# V18 Immediate CLOB Architecture

This is the current architecture. The older trade-tape queue-simulation design
is retained only in `fill_simulator.py` and `market_feed.py` for regression
compatibility; **`bot.py` does not use them for execution**.

## Current flow

`strategy candidate → executable fine-band screen → capital-first scheduler →
public CLOB snapshot → strict in-band ask walk → paper ledger BUY at VWAP`

The executor is all-or-nothing for the requested notional. It can consume
multiple ask levels, but every consumed ask must remain inside the selected fine
band. If liquidity is insufficient, nothing is committed to the paper ledger.

## Distribution control

The scheduler uses both verified trade-share and capital-share targets. Capital
gets the stronger weight because a few HIGH-price fills can otherwise consume a
disproportionate amount of dollars. Cumulative trade quotas prevent a single
liquid band from monopolizing the stream.

Current four-asset regime targets are CHEAP 48.4%, MID 30.6%, CORE 12.2%, HIGH
8.8%. The fine-band capital targets remain the verified empirical targets.

## State and restart safety

Runtime initialization occurs inside `main()`. Importing `bot.py` does not touch
`/app/data`, create paper state, or delete anything. Existing paper state is
loaded and the strategy distribution is rebuilt from its durable BUY records.

Immediate-fill shadow records are reconciled to actual execution/ledger data on restart; unresolved stale signals are marked ABANDONED and cannot become synthetic positions.

The main loop throttles scans when no execution attempt occurs, preventing cash/exposure blocks from creating a tight CLOB polling loop.

The ledger keeps the complete trade/settlement history rather than truncating it
after 10,000 records; truncation would corrupt long-run realized-P&L auditing and
restart distribution restoration.

## Limits

Every paper fill is constrained by:

- `MAX_TOTAL_EXPOSURE`
- `MAX_ASSET_EXPOSURE`
- `MAX_MARKET_EXPOSURE`
- `MAX_ORDER_USD`
- available paper cash
- `MIN_PAPER_FILL_USD`

## Important non-goals

This build does not attempt to predict winners, maximize win rate, or optimize
P&L. It is a behavioral replication and execution-model experiment.

### Immediate CLOB execution fixes in this build
- Execution bands are classified from the currently available ask, while the bid remains the signal reference. A bid/ask crossing a fine-band boundary is no longer silently discarded.
- Emergency scheduling is a liveness fallback only; recently filled markets are deprioritized when another executable market exists, preventing one liquid market from monopolizing the stream.
- Exact `1.00` asks remain non-executable because the paper ledger requires execution prices strictly below `1.00`.
- `MARKET_REUSE_COOLDOWN_SECONDS` defaults to 8 seconds and only deprioritizes recently filled markets when alternatives exist; it does not create synthetic fills.


### Immediate CLOB selection and anti-monopoly
The production path is paper-only and uses public CLOB asks as the executable source. The behavioral signal uses the observed bid/movement, while the execution fine band and entry sizing are recalculated from the currently visible ask. Before normal scheduler selection, recently filled markets are removed when a fresh executable market exists; if every executable opportunity is still within the cooldown, the bot retains the full pool for liveness.
