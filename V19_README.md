# V19.1 — Raw Behavioral Trader Replica + Immediate Public CLOB Paper Execution

## Objective
Replicate the observed trader process, not an invented profitability strategy.
The behavioral model controls fine-band frequency, capital allocation, entry
sizing, cadence, trajectory tendencies and side persistence. The public CLOB is
used only after the decision to obtain the currently available ask price.

## Critical separation
TRADER BEHAVIOR -> market/side/segment/size decision -> PUBLIC CLOB ASK -> paper fill.

The current ask is never used to rewrite the behavioral segment. If the trader
replica selects H95_100 and the current ask is 0.91, the trade remains an H95_100
behavioral trade in the audit, while the paper fill is recorded at 0.91.

There is no emergency quota forcing, market cooldown, P&L optimizer, win-rate
filter, or live order submission. Repeated markets remain possible.

Paper only: `PAPER_TRADING=true` is mandatory. Only public GET/book reads are used.


## Validation contract
The scheduler uses the raw 13 fine-band trade shares and raw fine-band capital
shares from `trader_behavior.json`. It does not convert them into the older
V17 four-regime 48.4/30.6/12.2/8.8 benchmark. Behavioral band selection is
performed before CLOB executability is considered. If the selected band has no
valid current ask, the decision is recorded as unfilled; another band is never
substituted. The configured minimum trade gap, when non-zero, is applied as a
lower bound on the sampled empirical cadence.
