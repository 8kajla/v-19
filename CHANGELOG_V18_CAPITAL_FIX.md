# V18 Immediate CLOB — Capital Allocation Fix

## What changed

1. **Fine-band scheduler is now capital-aware.**
   - The scheduler still enforces the verified cumulative trade-share quota.
   - Among currently eligible/executable bands, it now prioritizes the band that best repairs both trade-share and capital-share deviation.
   - Capital deviation is weighted more strongly because HIGH bands carry much larger notionals.

2. **Emergency selection is capital-aware too.**
   - If every available band is outside its normal trade quota, the emergency path penalizes capital overshoot heavily instead of repeatedly selecting a high-price band.

3. **Immediate CLOB execution remains strict.**
   - The selected fine band is never silently changed to another band to obtain liquidity.
   - Only public CLOB asks inside the selected fine band are consumed.
   - No live order submission exists in this project.

4. **Strategy identity is unchanged.**
   - `V17_FULL_SCALE_TRADER_REPLICA_40PCT` remains the strategy version.
   - The verified trade-share, capital-share, entry sizing, trajectory, side-persistence and cadence inputs are retained.
   - This is a scheduling correction, not a profit-optimization strategy.

## Validation

- Python compile check: passed.
- Pytest: **56 passed**.
- Existing 3,000-trade distribution convergence test: passed.
- Added regression test reproducing the latest Railway capital-skew state: passed.
- No Docker build was claimed because Docker is not installed in the build environment.
- No live trading was performed.


## V18 Capital Fix v2 hardening

- Moved filesystem-backed bot initialization behind `main()` so importing bot helpers never touches `/app/data` or paper state.
- Kept the mandatory `PAPER_TRADING=true` safety lock at runtime startup.
- Enforced `MAX_ASSET_EXPOSURE` and `MAX_MARKET_EXPOSURE` in the immediate-CLOB execution path in addition to `MAX_TOTAL_EXPOSURE` and cash.
- Removed duplicate capital-target initialization.
- Removed the 10,000-trade ledger truncation; complete paper trade/settlement history is now retained so realized P&L reconciliation and distribution restoration cannot silently forget older activity.
- Added regression coverage for import safety, exposure-cap wiring, and long-history audit retention.

## V18 Capital Fix V4 — Final Railway hardening

- Fixed a scheduler deadlock where all currently executable bands could be temporarily over the cumulative trade quota, causing the bot to stop generating signals indefinitely.
- Production candidate selection now uses the scheduler's explicit `allow_over_quota=True` emergency path only when no executable band is quota-eligible.
- Emergency selection remains execution-gated: no synthetic fill, no cross-band fallback, and distribution state changes only after a real paper CLOB fill.
- Removed terminal `1.00` ask acceptance from the immediate executor so it cannot produce a paper fill that the ledger rejects; execution prices remain strictly below `1.00`.
- Added regression coverage for the production emergency selection path and terminal `1.00` ask handling.
- Cleaned production ZIP caches before packaging.


## Final hardening pass
- Applied market-reuse cooldown before **normal** distribution-band selection, not only emergency selection.
- Removed unused `signal_markets` state; only filled positions enter the resolution map, while unfilled instant signals are closed as `UNFILLED` shadow records.
- Replaced per-decision full-history market scans with a compact in-memory per-condition entry index rebuilt once at startup and updated on every fill. Full ledger history remains intact.
- Added regression coverage for normal-path anti-monopoly selection, index reconstruction/runtime updates, and dead-state removal.
- Updated documentation to describe the bid-signal / ask-execution-band architecture accurately.
- Validation target for this package: full pytest suite plus compile/AST/security/ZIP integrity checks.


## V18 Final adversarial audit fixes
- Fixed immediate-fill shadow accounting so successful signals are updated with the actual CLOB VWAP, executed notional, and shares before resolution.
- Added restart reconciliation from `execution_comparison.csv` and the durable paper ledger; stale shadow signals without durable fill evidence are marked `ABANDONED` and are never resolved as trades.
- Fixed the main-loop no-execution path so empty books, unavailable bands, and cash/exposure blocks cannot cause continuous CLOB scans at `LOOP_SECONDS`.
- Started the empirical post-attempt cadence from the actual CLOB attempt completion time rather than the stale pre-network scan timestamp.
- Made market-discovery boolean parsing explicit and environment-configurable for `CLOB_API`.
- Added regression coverage for shadow accounting, crash/restart reconciliation, no-execution throttling, and boolean parsing.

### Current validation
- **79 pytest tests passed**.
- AST parse: all Python files passed.
- Production import smoke test: all production modules passed.
- No live order mutation calls found.
- No private-key/API-secret patterns found.
- `trader_behavior.json`: 13 bands; trade-share sum = 1.000000; capital-share sum = 1.000000.
- No live trading performed.
- Docker/Railway image build was not claimed because Docker is unavailable in the validation environment.
