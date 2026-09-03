import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from market_discovery import discover, book, resolve
from paper_ledger import PaperLedger
from research_logger import ResearchLogger
from strategy import CapitalFirstStrategy
from immediate_clob_executor import ImmediateClobExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bot")


def prepare_data_dir():
    data_dir = Path(os.getenv("DATA_DIR", "/app/data")).expanduser()
    fresh = os.getenv("FRESH_START", "false").lower() in ("1", "true", "yes", "on")
    if str(data_dir) in ("/", ".", ""):
        raise RuntimeError(f"Refusing unsafe DATA_DIR={data_dir!r}")
    data_dir.mkdir(parents=True, exist_ok=True)
    if fresh:
        for child in data_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    return data_dir


# Runtime objects are initialized only when main() starts. Importing bot.py must
# never create/delete files, touch /app/data, or mutate paper state.
DATA = None
strategy = None
ledger = None
research = None

markets = {}
histories = {}
resolution_markets = {}
last_trade = {}
market_entry_index = {}
ob_last = {}
last_disc = 0.0
last_report = 0.0
next_trade_at = 0.0
consecutive_errors = 0

scan_metrics = {
    "scannable": 0,
    "candidates": 0,
    "executable": 0,
    "last_attempt_ts": None,
}

BURST_GAP_SECONDS = float(os.getenv("BURST_GAP_SECONDS", "18"))
DISCOVERY_INTERVAL_SECONDS = max(
    2.0,
    float(os.getenv("DISCOVERY_INTERVAL_SECONDS", "5")),
)
BOOK_WORKERS = max(2, int(os.getenv("BOOK_WORKERS", "8")))
LOOP_SECONDS = max(0.05, float(os.getenv("LOOP_SECONDS", "0.25")))
ORDERBOOK_SAMPLE_SECONDS = max(
    0.1,
    float(os.getenv("ORDERBOOK_SAMPLE_SECONDS", "1")),
)

MAX_MARKET_EXPOSURE = max(
    0.0,
    float(os.getenv("MAX_MARKET_EXPOSURE", "300")),
)
MAX_ASSET_EXPOSURE = max(
    0.0,
    float(os.getenv("MAX_ASSET_EXPOSURE", "300")),
)
MAX_ORDER_USD = max(
    0.0,
    float(os.getenv("MAX_ORDER_USD", "300")),
)
MIN_PAPER_FILL_USD = max(
    0.01,
    float(os.getenv("MIN_PAPER_FILL_USD", "0.10")),
)

BOOK_EXECUTOR = ThreadPoolExecutor(
    max_workers=BOOK_WORKERS,
    thread_name_prefix="v19-book",
)

executor = ImmediateClobExecutor(
    timeout=float(os.getenv("CLOB_EXECUTION_TIMEOUT_SECONDS", "5")),
    retries=int(os.getenv("CLOB_EXECUTION_RETRIES", "2")),
)


def initialize_runtime():
    """Initialize all filesystem-backed runtime state exactly once."""
    global DATA, strategy, ledger, research

    if DATA is not None:
        return

    DATA = prepare_data_dir()

    if os.getenv("PAPER_TRADING", "true").lower() != "true":
        raise SystemExit("SAFETY LOCK: PAPER_TRADING must be true")

    strategy = CapitalFirstStrategy(
        bankroll=float(os.getenv("STARTING_CAPITAL", "1000")),
        max_total_exposure=float(
            os.getenv("MAX_TOTAL_EXPOSURE", "300")
        ),
        start_sec=float(
            os.getenv("START_TRADING_SECOND", "0")
        ),
        stop_sec=float(
            os.getenv("STOP_TRADING_SECOND", "240")
        ),
        hard_cutoff_seconds=float(
            os.getenv("HARD_CUTOFF_SECONDS", "60")
        ),
        min_trade_gap_seconds=float(
            os.getenv("MIN_TRADE_GAP_SECONDS", "0")
        ),
    )

    ledger = PaperLedger(
        DATA / "paper_state.json",
        strategy.bankroll,
    )

    # Create the state file on a fresh install; existing state is loaded by
    # PaperLedger without being overwritten.
    ledger.save()

    strategy.restore_policy_state(ledger.trades)

    # Rebuild compact per-market entry state once at startup. Runtime decisions
    # must not scan/sort the full audit history on every market.
    rebuild_market_entry_index(ledger.trades)

    research = ResearchLogger(DATA, ledger)

    recover_position_markets()


def position_exposure(*, asset=None, market=None, condition=None):
    """Return current open-cost exposure for the requested scope."""
    total = 0.0

    for p in ledger.positions.values():
        if condition is not None and str(
            p.get("condition")
        ) != str(condition):
            continue

        if asset is not None and str(
            p.get("asset", "")
        ).upper() != str(asset).upper():
            continue

        if market is not None and str(
            p.get("market", "")
        ) != str(market):
            continue

        total += float(p.get("cost", 0.0))

    return total


def _empty_market_entry_state():
    return {
        "count": 0,
        "first_ts": None,
        "previous_ts": None,
        "side": None,
        "price": None,
        "burst_position": 0,
    }


def _observe_market_entry(condition, trade):
    """Update compact entry state from one persisted/runtime BUY record."""
    condition = str(condition or "")

    if not condition:
        return

    try:
        ts = float(trade.get("ts"))
    except (TypeError, ValueError):
        return

    state = market_entry_index.setdefault(
        condition,
        _empty_market_entry_state(),
    )

    previous = state.get("previous_ts")

    if previous is None:
        burst = 1
        first_ts = ts
    else:
        gap = max(
            0.0,
            ts - float(previous),
        )

        burst = (
            int(state.get("burst_position") or 1) + 1
            if gap <= BURST_GAP_SECONDS
            else 1
        )

        first_ts = (
            state.get("first_ts")
            if state.get("first_ts") is not None
            else ts
        )

    state.update(
        {
            "count": int(state.get("count") or 0) + 1,
            "first_ts": float(first_ts),
            "previous_ts": ts,
            "side": trade.get("side"),
            "price": trade.get("price"),
            "burst_position": burst,
        }
    )

    last_trade[condition] = ts


def rebuild_market_entry_index(trades):
    market_entry_index.clear()
    last_trade.clear()

    buys = [
        t
        for t in trades
        if t.get("action") == "BUY"
        and t.get("condition")
    ]

    buys.sort(
        key=lambda t: float(
            t.get("ts", 0.0)
        )
    )

    for trade in buys:
        _observe_market_entry(
            trade.get("condition"),
            trade,
        )


def market_entry_state(condition, now):
    state = market_entry_index.get(
        str(condition)
    )

    if not state:
        return {
            "count": 0,
            "seconds_since_first": 0.0,
            "seconds_since_previous": None,
            "side": None,
            "price": None,
            "burst_position": 0,
        }

    first = state.get("first_ts")
    previous = state.get("previous_ts")

    return {
        "count": int(state.get("count") or 0),
        "seconds_since_first": (
            max(
                0.0,
                float(now) - float(first),
            )
            if first is not None
            else 0.0
        ),
        "seconds_since_previous": (
            max(
                0.0,
                float(now) - float(previous),
            )
            if previous is not None
            else None
        ),
        "side": state.get("side"),
        "price": state.get("price"),
        "burst_position": int(
            state.get("burst_position") or 0
        ),
    }


def prepare_histories(h, now):
    for side in ("Up", "Down"):
        h[side] = [
            x
            for x in h.get(side, [])
            if float(x[0]) >= now - 60.0
        ]


def startup_check():
    required = [
        "decisions.jsonl",
        "orderbooks.jsonl",
        "trades.csv",
        "markets.csv",
        "resolutions.csv",
        "pnl_1min.csv",
        "paper_state.json",
    ]

    missing = [
        x
        for x in required
        if not (DATA / x).exists()
    ]

    if missing:
        raise RuntimeError(
            f"DATA STORE INITIALIZATION FAILED: {missing}"
        )


def recover_position_markets():
    recovered = 0

    for position in list(ledger.positions.values()):
        condition = position.get("condition")

        if not condition or condition in resolution_markets:
            continue

        end_ts = position.get("end_ts")

        if end_ts is None:
            continue

        try:
            end_ts = float(end_ts)
        except (TypeError, ValueError):
            continue

        market = {
            "condition": str(condition),
            "id": str(
                position.get("market_id")
                or position.get("id")
                or ""
            ),
            "slug": str(
                position.get("slug") or ""
            ),
            "asset": str(
                position.get("asset") or ""
            ),
            "market": str(
                position.get("market")
                or position.get("asset")
                or ""
            ),
            "start_ts": float(
                position.get("start_ts")
                or max(
                    0.0,
                    end_ts - 300.0,
                )
            ),
            "end_ts": end_ts,
            "up": str(
                position.get("up_token") or ""
            ),
            "down": str(
                position.get("down_token") or ""
            ),
        }

        if market["id"] or market["slug"]:
            resolution_markets[
                str(condition)
            ] = market

            recovered += 1

    if recovered:
        log.info(
            "STATE RECOVERY | resolution_markets=%d recovered_positions=%d",
            len(resolution_markets),
            recovered,
        )


def resolve_pending(now):
    for condition in list(resolution_markets):
        market = (
            resolution_markets.get(condition)
            or markets.get(condition)
        )

        if (
            not market
            or now
            < float(
                market.get("end_ts", 0)
            ) + 2
        ):
            continue

        try:
            token, outcome, status = resolve(market)

            if token:
                closed = (
                    ledger.settle(
                        condition,
                        token,
                    )
                    if condition in resolution_markets
                    else []
                )

                pnl = sum(
                    float(x["pnl"])
                    for x in closed
                )

                research.record_resolution(
                    ts=now,
                    market=market,
                    winner=outcome or token,
                    winner_token=token,
                    closed=closed,
                )

                research.record_instant_resolution(
                    market,
                    token,
                    now,
                )

                # IMPORTANT:
                # Show settlement P&L and cumulative realized P&L explicitly.
                # This makes a resolved loss/win visible even when unrealized
                # P&L happens to offset it in the total equity figure.
                log.info(
                    "RESOLUTION | %s | winner=%s | "
                    "settlement_pnl=%+.4f | "
                    "realized_total=$%+.4f | "
                    "cash=$%.2f | closed=%d",
                    market["slug"],
                    outcome or token,
                    pnl,
                    ledger.realized,
                    ledger.cash,
                    len(closed),
                )

                resolution_markets.pop(
                    condition,
                    None
                )

                markets.pop(
                    condition,
                    None
                )

                histories.pop(
                    condition,
                    None
                )

            elif status == "CLOSED_UNRESOLVED":
                research.record_resolution_error(
                    ts=now,
                    market=market,
                    status=status,
                )

        except Exception as exc:
            research.record_resolution_error(
                ts=now,
                market=market,
                status=f"ERROR:{type(exc).__name__}",
            )

            log.warning(
                "RESOLUTION ERROR | %s | %s",
                market.get("slug"),
                exc,
            )


def pending_reserved():
    # There are no resting paper orders in this version.
    # A signal either consumes the current CLOB asks immediately
    # or is recorded as unfilled.
    return 0.0


def record_signal(
    market,
    token,
    signal,
    target,
    notion,
    meta,
    now,
):
    order = {
        "order_id": (
            f"instant-{market['condition']}-{now:.6f}"
        ),
        "condition": market["condition"],
        "token": token,
        "market": market["market"],
        "side": signal["side"],
        "target_price": target,
        "notional": notion,
        "placed_ts": now,
        "meta": meta,
    }

    research.record_instant_signal(order)

    return order


def record_filled_trade(
    market,
    token,
    signal,
    result,
    meta,
    now,
    state,
):
    # One ledger BUY at the VWAP represents the aggregate ask walk.
    trade = ledger.buy(
        market["condition"],
        token,
        market["market"],
        signal["side"],
        float(result["vwap"]),
        float(result["executed_notional"]),
        now,
        meta,
    )

    resolution_markets[
        market["condition"]
    ] = market

    _observe_market_entry(
        market["condition"],
        trade,
    )

    research.record_trade(
        trade=trade,
        market=market,
        elapsed=(
            now
            - float(
                market.get(
                    "start_ts",
                    now,
                )
            )
        ),
        left=max(
            0.0,
            float(
                market.get(
                    "end_ts",
                    now,
                )
            )
            - now,
        ),
        entry_count_before=state["count"],
        burst_position=state["burst_position"],
        seconds_since_previous=state[
            "seconds_since_previous"
        ],
        up_bid=meta.get("up_bid"),
        up_ask=meta.get("up_ask"),
        up_depth=meta.get("up_depth"),
        down_bid=meta.get("down_bid"),
        down_ask=meta.get("down_ask"),
        down_depth=meta.get("down_depth"),
        score=meta.get(
            "trajectory_likelihood"
        ),
        momentum=meta.get("movement"),
        cash_after=ledger.cash,
        exposure_after=ledger.exposure(
            market["condition"]
        ),
        fine_band=meta.get("fine_band"),
    )

    ledger.save()

    return trade


def report(books):
    global last_report

    now = time.time()

    interval = float(
        os.getenv(
            "REPORT_INTERVAL_SECONDS",
            "60",
        )
    )

    if now - last_report < interval:
        return

    last_report = now

    metrics = ledger.mark(books)

    metrics["positions"] = len(
        ledger.positions
    )

    research.record_pnl(
        now,
        metrics,
    )

    es = executor.stats()

    log.info(
        "SIGNAL SCAN | scannable=%d candidates=%d "
        "executable=%d next_trade_in=%.2fs",
        int(
            scan_metrics.get(
                "scannable",
                0,
            )
        ),
        int(
            scan_metrics.get(
                "candidates",
                0,
            )
        ),
        int(
            scan_metrics.get(
                "executable",
                0,
            )
        ),
        max(
            0.0,
            float(next_trade_at) - now,
        ),
    )

    # IMPORTANT:
    # `pnl` is total mark-to-market equity P&L.
    # `realized` is cumulative settlement P&L.
    # `unrealized` is the current value of open positions minus their cost.
    #
    # Showing all three avoids the previous situation where total P&L could
    # remain close to zero even after a real settlement because unrealized
    # P&L happened to offset realized P&L.
    log.info(
        "P&L total=$%+.2f | realized=$%+.2f | "
        "unrealized=$%+.2f | equity=$%.2f | "
        "cash=$%.2f | open=$%.2f | positions=%d | "
        "signals=%d filled=%d fill_rate=%.1f%% "
        "unfilled=%d book_errors=%d",
        metrics["pnl"],
        metrics["realized"],
        metrics["unrealized"],
        metrics["equity"],
        metrics["cash"],
        metrics["open_cost"],
        metrics["positions"],
        es["signals"],
        es["filled"],
        100.0 * es["fill_rate"],
        es["unfilled"],
        es["book_errors"],
    )

    snap = strategy.distribution_snapshot()

    for band, actual in snap["trade"].items():
        target = strategy.scheduler.trade_targets.get(
            band,
            0.0,
        )

        cap_actual = snap["capital"].get(
            band,
            0.0,
        )

        cap_target = strategy.scheduler.capital_targets.get(
            band,
            0.0,
        )

        log.info(
            "BAND | %s | trades=%.4f target=%.4f | "
            "capital=%.4f target=%.4f",
            band,
            actual,
            target,
            cap_actual,
            cap_target,
        )


def candidate_execution_key(candidate, now=None):
    """Rank executable candidates without letting one market monopolize a band."""
    if now is None:
        now = time.time()

    condition = str(
        (
            candidate.get("_market")
            or {}
        ).get(
            "condition",
            "",
        )
    )

    last = (
        last_trade.get(condition)
        if condition
        else None
    )

    age = (
        float("inf")
        if last is None
        else max(
            0.0,
            float(now) - float(last),
        )
    )

    return (
        age,
        float(
            candidate.get(
                "trajectory_likelihood",
                0.0,
            )
        ),
        1 if candidate.get("same_side") else 0,
        float(
            candidate.get("depth")
            or 0.0
        ),
        -float(
            candidate.get("bid")
            or 0.0
        ),
    )


def prepare_immediate_candidate(
    candidate,
    strategy,
    market,
    state,
):
    """Keep the behavioral signal band; CLOB is execution-only.

    The candidate's band is the trader-replication segment.
    The live ask may be in another price band; that must not rewrite
    the behavioral decision.
    """
    ask = candidate.get("ask")

    if ask is None:
        candidate["_clob_executable"] = False
        return candidate

    try:
        ask = float(ask)
    except (TypeError, ValueError):
        candidate["_clob_executable"] = False
        return candidate

    if not 0.0 < ask < 1.0:
        candidate["_clob_executable"] = False
        return candidate

    candidate["signal_band"] = candidate.get(
        "band"
    )

    candidate["signal_bid"] = float(
        candidate["bid"]
    )

    candidate["execution_band"] = None

    (
        candidate["execution_ask_band"],
        candidate["execution_regime"],
    ) = strategy.fine_band(ask)

    candidate["target"] = strategy.entry_target(
        float(
            candidate.get(
                "bid",
                ask,
            )
        ),
        market=market["asset"],
        entry_count=state["count"],
    )

    candidate["_clob_executable"] = True

    return candidate


def choose_executable_band(
    candidates,
    strategy,
    allow_emergency=False,
    now=None,
    reuse_cooldown_seconds=0.0,
):
    """Select behavioral band first, then expose only executable rows.

    CLOB availability must never choose a different behavioral band.
    A band is selected from the full candidate set; if that selected
    band has no current ask, this decision is simply unfilled and the
    next cadence decision starts fresh. The legacy arguments remain
    for compatibility but have no effect.
    """
    del allow_emergency
    del reuse_cooldown_seconds

    if not candidates:
        return None, False, []

    now = (
        time.time()
        if now is None
        else float(now)
    )

    band = strategy.choose_distribution_band(
        candidates
    )

    if band is None:
        return None, False, []

    rows = [
        c
        for c in candidates
        if (
            c.get("band") == band
            and c.get("_clob_executable")
        )
    ]

    rows.sort(
        key=lambda c: candidate_execution_key(
            c,
            now,
        ),
        reverse=True,
    )

    return band, False, rows


def main():
    global last_disc
    global next_trade_at
    global consecutive_errors
    global scan_metrics

    initialize_runtime()

    startup_check()

    log.info(
        "BOT B | PAPER ONLY | "
        "IMMEDIATE CLOB TAKER SIMULATION | "
        "V19 RAW BEHAVIORAL REPLICA | "
        "CLOB EXECUTION ONLY"
    )

    while True:
        try:
            now = time.time()

            if (
                now - last_disc
                >= DISCOVERY_INTERVAL_SECONDS
            ):
                for m in discover():
                    markets[
                        m["condition"]
                    ] = m

                for condition, m in list(
                    markets.items()
                ):
                    if any(
                        p.get("condition")
                        == condition
                        for p in ledger.positions.values()
                    ):
                        resolution_markets[
                            condition
                        ] = m

                    if (
                        m.get("end_ts", 0)
                        < now - 30
                        and condition
                        not in resolution_markets
                    ):
                        markets.pop(
                            condition,
                            None,
                        )

                last_disc = now

                log.info(
                    "MARKETS | active=%d resolution=%d",
                    len(markets),
                    len(resolution_markets),
                )

            resolve_pending(now)

            books = {}
            execution_attempted = False

            if now >= next_trade_at:
                eligible = []
                scannable = []

                for m in list(
                    markets.values()
                ):
                    elapsed = (
                        now
                        - float(
                            m["start_ts"]
                        )
                    )

                    left = (
                        float(
                            m["end_ts"]
                        )
                        - now
                    )

                    if (
                        left <= 0
                        or elapsed < 0
                        or elapsed > 300
                        or not m.get(
                            "accepting_orders"
                        )
                    ):
                        continue

                    scannable.append(
                        (
                            m,
                            elapsed,
                            left,
                        )
                    )

                scan_metrics[
                    "scannable"
                ] = len(scannable)

                scan_metrics[
                    "candidates"
                ] = 0

                scan_metrics[
                    "executable"
                ] = 0

                scan_metrics[
                    "last_attempt_ts"
                ] = now

                futures = {
                    BOOK_EXECUTOR.submit(
                        book,
                        token,
                    ): (
                        m,
                        tname,
                    )
                    for m, _, _ in scannable
                    for tname, token in (
                        ("up", m["up"]),
                        ("down", m["down"]),
                    )
                    if token
                }

                mb = {}

                for future in as_completed(
                    futures
                ):
                    m, tname = futures[
                        future
                    ]

                    try:
                        mb.setdefault(
                            m["condition"],
                            {},
                        )[tname] = future.result()

                    except Exception as exc:
                        log.warning(
                            "BOOK ERROR | %s | %s",
                            m.get("slug"),
                            exc,
                        )

                for m, elapsed, left in scannable:
                    pair = mb.get(
                        m["condition"],
                        {},
                    )

                    # A temporary book failure on one token must not suppress
                    # the other side. A market is unusable only when neither
                    # side has a book snapshot.
                    if not pair:
                        continue

                    ub, ua, ud, uad = pair.get(
                        "up",
                        (
                            None,
                            None,
                            0.0,
                            0.0,
                        ),
                    )

                    db, da, dd, dad = pair.get(
                        "down",
                        (
                            None,
                            None,
                            0.0,
                            0.0,
                        ),
                    )

                    if m.get("up"):
                        books[
                            m["up"]
                        ] = ub

                    if m.get("down"):
                        books[
                            m["down"]
                        ] = db

                    h = histories.setdefault(
                        m["condition"],
                        {
                            "Up": [],
                            "Down": [],
                        },
                    )

                    if ub is not None:
                        h["Up"].append(
                            (
                                now,
                                ub,
                            )
                        )

                    if db is not None:
                        h["Down"].append(
                            (
                                now,
                                db,
                            )
                        )

                    prepare_histories(
                        h,
                        now,
                    )

                    if (
                        now
                        - ob_last.get(
                            m["condition"],
                            0,
                        )
                        >= ORDERBOOK_SAMPLE_SECONDS
                    ):
                        research.record_orderbook(
                            ts=now,
                            market=m,
                            elapsed=elapsed,
                            left=left,
                            up_bid=ub,
                            up_ask=ua,
                            up_depth=ud,
                            down_bid=db,
                            down_ask=da,
                            down_depth=dd,
                            up_ask_depth=uad,
                            down_ask_depth=dad,
                        )

                        ob_last[
                            m["condition"]
                        ] = now

                    state = market_entry_state(
                        m["condition"],
                        now,
                    )

                    candidates = (
                        strategy.build_candidates_for_market(
                            elapsed,
                            ua,
                            da,
                            ub,
                            db,
                            h["Up"],
                            h["Down"],
                            now,
                            asset=m["asset"],
                            market=m["asset"],
                            thesis_side=state["side"],
                            market_entry_count=state[
                                "count"
                            ],
                            seconds_since_first_entry=state[
                                "seconds_since_first"
                            ],
                            up_depth=ud,
                            down_depth=dd,
                            up_ask_depth=uad,
                            down_ask_depth=dad,
                        )
                    )

                    scan_metrics[
                        "candidates"
                    ] += len(candidates)

                    for candidate in candidates:
                        candidate.update(
                            _market=m,
                            _state=state,
                            _elapsed=elapsed,
                            _left=left,
                        )

                        # Inspect current ask for execution availability.
                        # This does not change behavioral signal band.
                        prepare_immediate_candidate(
                            candidate,
                            strategy,
                            m,
                            state,
                        )

                        if candidate.get(
                            "_clob_executable"
                        ):
                            scan_metrics[
                                "executable"
                            ] += 1

                        eligible.append(
                            candidate
                        )

                if eligible:
                    # IMPORTANT:
                    # Behavioral distribution selection happens BEFORE CLOB
                    # executability is allowed to influence the chosen band.
                    #
                    # If the chosen band has no ask, the decision is unfilled.
                    # Never substitute another band simply because it is liquid.
                    (
                        target_band,
                        emergency_selection,
                        band_candidates,
                    ) = choose_executable_band(
                        eligible,
                        strategy,
                        allow_emergency=False,
                        now=now,
                        reuse_cooldown_seconds=0.0,
                    )

                    if emergency_selection:
                        log.info(
                            "SCHEDULER EMERGENCY | "
                            "band=%s | executable_candidates=%d | "
                            "reason=all_quota_ineligible",
                            target_band,
                            len(
                                band_candidates
                            ),
                        )

                    if band_candidates:
                        # Try more than one market/side inside the chosen band.
                        # A second asset can satisfy the same behavioral band
                        # without changing the strategy band.
                        for best in band_candidates:
                            market = best[
                                "_market"
                            ]

                            state = best[
                                "_state"
                            ]

                            signal = (
                                strategy.choose_process_candidate(
                                    [best],
                                    target_band,
                                )
                            )

                            if not signal:
                                continue

                            reserved = (
                                pending_reserved()
                            )

                            current_total = (
                                ledger.total_open_cost()
                            )

                            current_asset = (
                                position_exposure(
                                    asset=market[
                                        "asset"
                                    ]
                                )
                            )

                            current_market = (
                                position_exposure(
                                    market=market[
                                        "market"
                                    ]
                                )
                            )

                            remaining_total = max(
                                0.0,
                                strategy.max_total_exposure
                                - current_total
                                - reserved,
                            )

                            remaining_asset = max(
                                0.0,
                                MAX_ASSET_EXPOSURE
                                - current_asset
                                - reserved,
                            )

                            remaining_market = max(
                                0.0,
                                MAX_MARKET_EXPOSURE
                                - current_market
                                - reserved,
                            )

                            available = max(
                                0.0,
                                ledger.cash
                                - reserved,
                            )

                            notion = min(
                                float(
                                    signal[
                                        "target"
                                    ]
                                ),
                                MAX_ORDER_USD,
                                available,
                                remaining_total,
                                remaining_asset,
                                remaining_market,
                            )

                            if (
                                notion
                                < MIN_PAPER_FILL_USD
                                or best[
                                    "_left"
                                ]
                                <= strategy.hard_cutoff_seconds
                            ):
                                continue

                            token = (
                                market["up"]
                                if signal[
                                    "side"
                                ]
                                == "Up"
                                else market[
                                    "down"
                                ]
                            )

                            # `best["band"]` is the behavioral fine band
                            # selected by the replication model.
                            # The live ask is execution-only.
                            band = str(
                                best["band"]
                            )

                            regime = str(
                                best["regime"]
                            )

                            target = float(
                                signal["bid"]
                            )

                            meta = {
                                "slug": market[
                                    "slug"
                                ],
                                "asset": market[
                                    "asset"
                                ],
                                "market": market[
                                    "market"
                                ],
                                "start_ts": market[
                                    "start_ts"
                                ],
                                "end_ts": market[
                                    "end_ts"
                                ],
                                "market_id": market[
                                    "id"
                                ],
                                "up_token": market[
                                    "up"
                                ],
                                "down_token": market[
                                    "down"
                                ],
                                "model_version": (
                                    strategy.VERSION
                                ),
                                "entry_count_before": (
                                    state["count"]
                                ),
                                "burst_position": (
                                    state[
                                        "burst_position"
                                    ]
                                ),
                                "seconds_since_first_entry": (
                                    state[
                                        "seconds_since_first"
                                    ]
                                ),
                                "seconds_since_previous_trade": (
                                    state[
                                        "seconds_since_previous"
                                    ]
                                ),
                                "regime": regime,
                                "fine_band": band,
                                "signal_fine_band": (
                                    best.get(
                                        "signal_band"
                                    )
                                ),
                                "execution_mode": (
                                    "IMMEDIATE_CLOB_TAKER"
                                ),
                                "target_capital": float(
                                    signal["target"]
                                ),
                                "strategy_reference_bid": (
                                    float(
                                        best.get(
                                            "signal_bid",
                                            target,
                                        )
                                    )
                                ),
                                "strategy_reference_ask": (
                                    best.get(
                                        "ask"
                                    )
                                ),
                                "bid_size": (
                                    signal.get(
                                        "depth"
                                    )
                                    or 0.0
                                ),
                                "up_bid": (
                                    best.get(
                                        "bid"
                                    )
                                    if best.get(
                                        "side"
                                    )
                                    == "Up"
                                    else None
                                ),
                                "up_ask": (
                                    best.get(
                                        "ask"
                                    )
                                    if best.get(
                                        "side"
                                    )
                                    == "Up"
                                    else None
                                ),
                                "up_depth": (
                                    best.get(
                                        "depth"
                                    )
                                    if best.get(
                                        "side"
                                    )
                                    == "Up"
                                    else None
                                ),
                                "down_bid": (
                                    best.get(
                                        "bid"
                                    )
                                    if best.get(
                                        "side"
                                    )
                                    == "Down"
                                    else None
                                ),
                                "down_ask": (
                                    best.get(
                                        "ask"
                                    )
                                    if best.get(
                                        "side"
                                    )
                                    == "Down"
                                    else None
                                ),
                                "down_depth": (
                                    best.get(
                                        "depth"
                                    )
                                    if best.get(
                                        "side"
                                    )
                                    == "Down"
                                    else None
                                ),
                                "trajectory_likelihood": (
                                    signal[
                                        "trajectory_likelihood"
                                    ]
                                ),
                                "movement": signal.get(
                                    "movement"
                                ),
                            }

                            order = record_signal(
                                market,
                                token,
                                signal,
                                target,
                                notion,
                                meta,
                                now,
                            )

                            execution_attempted = True

                            result = executor.execute(
                                token=token,
                                notional=notion,
                                strategy_band=band,
                                signal_bid=target,
                                strategy=strategy,
                            )

                            # The empirical gap governs accepted trade cadence,
                            # regardless of whether this CLOB attempt filled.
                            sampled_gap = max(
                                0.0,
                                strategy.cadence.sample_gap(),
                            )

                            configured_gap = max(
                                0.0,
                                strategy.min_trade_gap_seconds,
                            )

                            next_trade_at = (
                                time.time()
                                + max(
                                    sampled_gap,
                                    configured_gap,
                                )
                            )

                            if result["filled"]:
                                executed = float(
                                    result[
                                        "executed_notional"
                                    ]
                                )

                                meta.update(
                                    {
                                        "execution_price": float(
                                            result[
                                                "vwap"
                                            ]
                                        ),
                                        "execution_vwap": float(
                                            result[
                                                "vwap"
                                            ]
                                        ),
                                        "execution_slippage": float(
                                            result[
                                                "slippage"
                                            ]
                                        ),
                                        "execution_levels": (
                                            result[
                                                "levels"
                                            ]
                                        ),
                                        "execution_book_ts": (
                                            result.get(
                                                "book_ts"
                                            )
                                        ),
                                        "execution_book_levels": (
                                            result.get(
                                                "book_levels_seen"
                                            )
                                        ),
                                    }
                                )

                                trade = record_filled_trade(
                                    market,
                                    token,
                                    signal,
                                    result,
                                    meta,
                                    now,
                                    state,
                                )

                                strategy.observe_trade_distribution(
                                    band,
                                    executed,
                                )

                                order.update(
                                    {
                                        "fill_ts": now,
                                        "fill_price": float(
                                            result[
                                                "vwap"
                                            ]
                                        ),
                                        "fill_latency_s": 0.0,
                                    }
                                )

                                research.record_execution_comparison_fill(
                                    order,
                                    trade,
                                )

                                log.info(
                                    "CLOB FILL | %s | %s | "
                                    "band=%s | ref=%.4f "
                                    "VWAP=%.4f | slippage=%+.4f | "
                                    "notional=$%.2f | levels=%d",
                                    market["asset"],
                                    signal["side"],
                                    band,
                                    target,
                                    result["vwap"],
                                    result["slippage"],
                                    executed,
                                    len(
                                        result[
                                            "levels"
                                        ]
                                    ),
                                )

                                break

                            log.info(
                                "CLOB UNFILLED | %s | %s | "
                                "band=%s | ref=%.4f | "
                                "notional=$%.2f | reason=%s",
                                market["asset"],
                                signal["side"],
                                band,
                                target,
                                notion,
                                result["reason"],
                            )

                            research.record_instant_unfilled(
                                order,
                                result.get(
                                    "reason",
                                    "unfilled",
                                ),
                                now,
                            )

                        else:
                            # Every candidate in the selected distribution
                            # band was rejected/invalid.
                            log.info(
                                "CLOB BAND MISS | band=%s | "
                                "candidates=%d | "
                                "reason=no_candidate_filled",
                                target_band,
                                len(
                                    band_candidates
                                ),
                            )

            if (
                now >= next_trade_at
                and not execution_attempted
            ):
                # No execution attempt occurred.
                # Re-scan at order-book cadence instead of spinning
                # at LOOP_SECONDS.
                next_trade_at = (
                    time.time()
                    + max(
                        LOOP_SECONDS,
                        ORDERBOOK_SAMPLE_SECONDS,
                    )
                )

            report(books)

            time.sleep(
                LOOP_SECONDS
            )

            consecutive_errors = 0

        except KeyboardInterrupt:
            BOOK_EXECUTOR.shutdown(
                wait=False,
                cancel_futures=True,
            )
            raise

        except Exception as exc:
            consecutive_errors += 1

            log.exception(
                "LOOP ERROR #%d | %s",
                consecutive_errors,
                exc,
            )

            time.sleep(
                min(
                    10.0,
                    LOOP_SECONDS
                    * max(
                        2,
                        consecutive_errors,
                    ),
                )
            )


if __name__ == "__main__":
    main()
