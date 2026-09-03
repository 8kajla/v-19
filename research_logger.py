import csv
import json
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path


REGIMES = ("CHEAP", "MID", "CORE", "HIGH")

SCHEMAS = {
    "trades.csv": [
        "trade_id", "timestamp", "market_id", "condition", "slug", "asset",
        "market", "side", "token", "price", "shares", "notional",
        "seconds_into_market", "seconds_remaining", "entry_count_before",
        "burst_position", "seconds_since_previous_trade",
        "up_bid", "up_ask", "up_depth", "down_bid", "down_ask", "down_depth",
        "spread", "score", "momentum", "signal_reason", "cash_after",
        "market_exposure_after", "fine_band", "regime",
    ],
    "markets.csv": [
        "market_id", "condition", "slug", "asset", "market", "start_ts",
        "end_ts", "winner", "entries", "total_cost", "total_shares",
        "avg_entry", "first_entry", "last_entry", "max_exposure", "up_cost",
        "down_cost", "up_shares", "down_shares", "winning_cost",
        "losing_cost", "payout", "realized_pnl", "roi", "resolved_ts",
    ],
    "resolutions.csv": [
        "timestamp", "market_id", "condition", "slug", "asset", "winner",
        "winner_token", "entries", "cost", "payout", "pnl", "roi", "status",
    ],
    "settlement_details.csv": [
        "timestamp", "market_id", "condition", "slug", "asset", "trade_id",
        "side", "token", "regime", "price", "shares", "cost",
        "settlement_per_share", "payout", "pnl", "roi", "outcome",
    ],
    "regime_1min.csv": [
        "timestamp", "regime", "trades", "notional", "trade_share",
        "settled_trades", "wins", "losses", "win_rate", "settled_cost",
        "settled_pnl", "settled_roi", "avg_settled_pnl", "open_cost",
    ],
    "trade_details.csv": [
        "trade_id", "timestamp", "market_id", "condition", "slug", "asset",
        "market", "side", "token", "regime", "fine_band", "price", "shares",
        "notional", "seconds_into_market", "seconds_remaining",
        "entry_count_before", "burst_position", "seconds_since_previous_trade",
        "spread", "score", "momentum", "cash_after",
        "market_exposure_after", "up_bid", "up_ask", "up_depth",
        "down_bid", "down_ask", "down_depth", "signal_reason",
        "trajectory_likelihood",
    ],
    "pnl_1min.csv": [
        "timestamp", "equity", "total_pnl", "realized_pnl",
        "unrealized_pnl", "cash", "open_cost", "market_value",
        "drawdown", "positions", "marked",
    ],
    "pending_orders.csv": ["timestamp","order_id","condition","token","market","side","target_price","notional","depth_ahead","window_end_ts","status","fill_price","fill_ts","fill_latency_s","cumulative_volume","regime","fine_band"],
    "fill_progress.csv": ["timestamp","order_id","token","trade_price","trade_size","cumulative_volume","target_price","depth_ahead","status"],
    "fills.csv": ["timestamp","order_id","condition","token","side","target_price","fill_price","notional","depth_ahead","cumulative_volume","fill_latency_s","regime","fine_band"],
    "unfilled_orders.csv": ["timestamp","order_id","condition","token","side","target_price","notional","depth_ahead","cumulative_volume","unfilled_for_s","regime","fine_band"],
    "fill_errors.csv": ["timestamp","order_id","token","trade_price","error"],
    "instant_fill_shadow.csv": ["timestamp","order_id","condition","token","side","price","notional","shares","regime","fine_band","winner","payout","pnl","status","resolved_ts"],
    "execution_comparison.csv": ["timestamp","order_id","condition","target_price","fill_price","notional","fill_latency_s","status"],
}


class ResearchLogger:
    """Auditable logger for paper execution and trader-behavior research."""

    def __init__(self, data_dir, ledger=None):
        self.root = Path(data_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

        self._trade_cache = defaultdict(list)
        self.market_stats = defaultdict(lambda: {
            "entries": 0, "cost": 0.0, "shares": 0.0,
            "first_entry": None, "last_entry": None, "max_exposure": 0.0,
            "asset": "", "market": "", "up_cost": 0.0, "down_cost": 0.0,
            "up_shares": 0.0, "down_shares": 0.0, "slug": "",
            "market_id": "", "start_ts": 0.0, "end_ts": 0.0,
        })
        self.regime_stats = {
            regime: {
                "trades": 0, "notional": 0.0, "settled_trades": 0,
                "wins": 0, "losses": 0, "settled_cost": 0.0,
                "settled_pnl": 0.0, "open_cost": 0.0,
            }
            for regime in REGIMES
        }
        self.last_resolution_error = {}
        self._ensure_files()

        if ledger is not None:
            self.rebuild_from_ledger(ledger)
        # Older V18 builds wrote successful immediate fills as OPEN shadow rows.
        # Reconcile those rows from the durable execution-comparison audit so a
        # restart never treats a filled trade as an unfilled/incorrect-price
        # shadow position.
        self.reconcile_instant_shadow(ledger)

    def _ensure_files(self):
        for filename, fields in SCHEMAS.items():
            path = self.root / filename
            if not path.exists() or path.stat().st_size == 0:
                with path.open("w", newline="", encoding="utf-8") as fh:
                    csv.writer(fh).writerow(fields)
        for filename in ("decisions.jsonl", "orderbooks.jsonl"):
            (self.root / filename).touch(exist_ok=True)

    def _append_csv(self, filename, row):
        with self.lock, (self.root / filename).open(
            "a", newline="", encoding="utf-8"
        ) as fh:
            writer = csv.DictWriter(
                fh, fieldnames=SCHEMAS[filename], extrasaction="ignore"
            )
            writer.writerow(row)
            fh.flush()

    def _append_jsonl(self, filename, obj):
        with self.lock, (self.root / filename).open(
            "a", encoding="utf-8"
        ) as fh:
            fh.write(
                json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
                + "\n"
            )
            fh.flush()

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _regime(price):
        p = ResearchLogger._safe_float(price, -1.0)
        if 0.01 <= p < 0.30:
            return "CHEAP"
        if 0.30 <= p < 0.70:
            return "MID"
        if 0.70 <= p < 0.90:
            return "CORE"
        if 0.90 <= p < 1.00:
            return "HIGH"
        return "OTHER"

    @staticmethod
    def _movement(price, history, now):
        p = ResearchLogger._safe_float(price, 0.0)
        out = {}
        for seconds in (1, 3, 5, 10, 30):
            eligible = []
            for item in history or []:
                try:
                    ts = float(item[0] if not isinstance(item, dict) else item["ts"])
                    hp = float(
                        item[1] if not isinstance(item, dict)
                        else item.get("best_bid", item.get("mid"))
                    )
                    if ts <= now - seconds:
                        eligible.append((ts, hp))
                except (TypeError, ValueError, KeyError, IndexError):
                    continue
            out[f"m{seconds}"] = p - eligible[-1][1] if eligible else 0.0
        return out

    @staticmethod
    def _depth_imbalance(bid_depth, ask_depth):
        b = ResearchLogger._safe_float(bid_depth)
        a = ResearchLogger._safe_float(ask_depth)
        total = b + a
        return (b - a) / total if total else 0.0

    def record_decision(self, **kw):
        market = kw["market"]
        signal = kw.get("signal")
        ts = float(kw["ts"])

        ub, ua = kw.get("up_bid"), kw.get("up_ask")
        db, da = kw.get("down_bid"), kw.get("down_ask")
        up_history = kw.get("up_history") or []
        down_history = kw.get("down_history") or []

        up_mid = None
        down_mid = None
        if ub is not None and ua is not None:
            up_mid = (float(ub) + float(ua)) / 2.0
        if db is not None and da is not None:
            down_mid = (float(db) + float(da)) / 2.0

        self._append_jsonl("decisions.jsonl", {
            "t": round(ts, 3),
            "m": market["id"],
            "c": market["condition"],
            "s": market["slug"],
            "a": market["asset"],
            "e": round(float(kw["elapsed"]), 3),
            "r": round(float(kw["left"]), 3),
            "ub": ub, "ua": ua, "ud": kw.get("up_depth"),
            "db": db, "da": da, "dd": kw.get("down_depth"),
            "us": float(ua) - float(ub) if ua is not None and ub is not None else None,
            "ds": float(da) - float(db) if da is not None and db is not None else None,
            "ui": self._depth_imbalance(kw.get("up_depth"), kw.get("up_ask_depth")),
            "di": self._depth_imbalance(kw.get("down_depth"), kw.get("down_ask_depth")),
            "x": signal.side if signal else "WAIT",
            "p": signal.price if signal else None,
            "score": signal.score if signal else None,
            "n": signal.notional if signal else 0.0,
            "reason": signal.reason if signal else "no_signal",
            "ex": kw.get("exposure", 0.0),
            "cash": kw.get("cash", 0.0),
            "entry_count": kw.get("entry_count", 0),
            "burst_position": kw.get("burst_position", 0),
            "seconds_since_previous_trade": kw.get("seconds_since_previous"),
            "up_movement": self._movement(up_mid or ub, up_history, ts) if (up_mid or ub) else {},
            "down_movement": self._movement(down_mid or db, down_history, ts) if (down_mid or db) else {},
            # Explicit event label: this record is a trade if a signal existed.
            "event": "TRADE_CANDIDATE" if signal else "NON_TRADE",
            "capture_latency_ms": kw.get("capture_latency_ms"),
        })

    def record_orderbook(self, **kw):
        market = kw["market"]
        ts = float(kw["ts"])
        up_bid, up_ask = kw.get("up_bid"), kw.get("up_ask")
        down_bid, down_ask = kw.get("down_bid"), kw.get("down_ask")

        self._append_jsonl("orderbooks.jsonl", {
            "t": round(ts, 3),
            "m": market["id"],
            "c": market["condition"],
            "s": market["slug"],
            "a": market["asset"],
            "e": round(float(kw["elapsed"]), 3),
            "r": round(float(kw["left"]), 3),
            "ub": up_bid,
            "ua": up_ask,
            "ud": kw.get("up_depth"),
            "uad": kw.get("up_ask_depth"),
            "db": down_bid,
            "da": down_ask,
            "dd": kw.get("down_depth"),
            "dad": kw.get("down_ask_depth"),
            "ui": self._depth_imbalance(kw.get("up_depth"), kw.get("up_ask_depth")),
            "di": self._depth_imbalance(kw.get("down_depth"), kw.get("down_ask_depth")),
            "us": float(up_ask) - float(up_bid) if up_bid is not None and up_ask is not None else None,
            "ds": float(down_ask) - float(down_bid) if down_bid is not None and down_ask is not None else None,
        })

    def record_trade(self, **kw):
        trade = kw["trade"]
        market = kw["market"]
        trade_id = str(trade.get("trade_id") or f"paper-{uuid.uuid4().hex}")

        price = self._safe_float(trade.get("price"))
        regime = self._regime(price)
        band = trade.get("fine_band")
        trade["trade_id"] = trade_id
        trade["regime"] = regime

        ub, ua = kw.get("up_bid"), kw.get("up_ask")
        db, da = kw.get("down_bid"), kw.get("down_ask")

        if trade["side"] == "Up" and ua is not None and ub is not None:
            spread = float(ua) - float(ub)
        elif trade["side"] == "Down" and da is not None and db is not None:
            spread = float(da) - float(db)
        else:
            spread = None

        row = {
            "trade_id": trade_id,
            "timestamp": trade["ts"],
            "market_id": trade.get("market_id", market["id"]),
            "condition": trade["condition"],
            "slug": trade.get("slug", market["slug"]),
            "asset": trade.get("asset", market["asset"]),
            "market": trade.get("market", market["market"]),
            "side": trade["side"],
            "token": trade["token"],
            "price": trade["price"],
            "shares": trade["shares"],
            "notional": trade["notional"],
            "seconds_into_market": kw["elapsed"],
            "seconds_remaining": kw["left"],
            "entry_count_before": kw.get("entry_count_before", 0),
            "burst_position": kw.get("burst_position", 0),
            "seconds_since_previous_trade": kw.get("seconds_since_previous"),
            "up_bid": ub, "up_ask": ua, "up_depth": kw.get("up_depth"),
            "down_bid": db, "down_ask": da, "down_depth": kw.get("down_depth"),
            "spread": spread,
            "score": kw.get("score"),
            "momentum": kw.get("momentum"),
            "signal_reason": kw.get("reason"),
            "cash_after": kw.get("cash_after"),
            "market_exposure_after": kw.get("exposure_after"),
            "fine_band": band,
            "regime": regime,
        }
        self._append_csv("trades.csv", row)

        trajectory_likelihood = kw.get("score")
        self._append_csv("trade_details.csv", {
            **row,
            "trajectory_likelihood": trajectory_likelihood,
        })

        self._trade_cache[trade["condition"]].append(dict(trade))

        stats = self.market_stats[trade["condition"]]
        notional = self._safe_float(trade["notional"])
        shares = self._safe_float(trade["shares"])

        stats["entries"] += 1
        stats["cost"] += notional
        stats["shares"] += shares
        stats["first_entry"] = (
            trade["ts"] if stats["first_entry"] is None
            else min(stats["first_entry"], trade["ts"])
        )
        stats["last_entry"] = trade["ts"]
        stats["max_exposure"] = max(
            stats["max_exposure"],
            self._safe_float(kw.get("exposure_after")),
        )
        stats["asset"] = trade.get("asset", market["asset"])
        stats["market"] = trade.get("market", market["market"])
        stats["slug"] = market["slug"]
        stats["market_id"] = market["id"]
        stats["start_ts"] = market["start_ts"]
        stats["end_ts"] = market["end_ts"]

        if trade["side"] == "Up":
            stats["up_cost"] += notional
            stats["up_shares"] += shares
        else:
            stats["down_cost"] += notional
            stats["down_shares"] += shares

        if regime in self.regime_stats:
            bucket = self.regime_stats[regime]
            bucket["trades"] += 1
            bucket["notional"] += notional
            bucket["open_cost"] += notional

    def record_resolution(self, **kw):
        market = kw["market"]
        closed = kw["closed"]
        condition = market["condition"]

        stats = self.market_stats[condition]
        trades = self._trade_cache[condition]
        cost = self._safe_float(stats["cost"])
        pnl = sum(self._safe_float(x.get("pnl")) for x in closed)
        payout = cost + pnl

        by_regime = {}
        for item in trades:
            regime = item.get("regime") or self._regime(item.get("price"))
            trade_cost = self._safe_float(item.get("notional"))
            shares = self._safe_float(item.get("shares"))
            won = item.get("token") == kw["winner_token"]
            trade_payout = shares if won else 0.0
            trade_pnl = trade_payout - trade_cost
            roi = trade_pnl / trade_cost if trade_cost else 0.0

            bucket = by_regime.setdefault(regime, {
                "trades": 0, "wins": 0, "losses": 0,
                "cost": 0.0, "pnl": 0.0,
            })
            bucket["trades"] += 1
            bucket["wins"] += int(won)
            bucket["losses"] += int(not won)
            bucket["cost"] += trade_cost
            bucket["pnl"] += trade_pnl

            self._append_csv("settlement_details.csv", {
                "timestamp": kw["ts"],
                "market_id": market["id"],
                "condition": condition,
                "slug": market["slug"],
                "asset": market["asset"],
                "trade_id": item.get("trade_id", ""),
                "side": item.get("side", ""),
                "token": item.get("token", ""),
                "regime": regime,
                "price": item.get("price", 0.0),
                "shares": shares,
                "cost": trade_cost,
                "settlement_per_share": 1.0 if won else 0.0,
                "payout": trade_payout,
                "pnl": trade_pnl,
                "roi": roi,
                "outcome": "WIN" if won else "LOSS",
            })

            if regime in self.regime_stats:
                bucket = self.regime_stats[regime]
                bucket["settled_trades"] += 1
                bucket["settled_cost"] += trade_cost
                bucket["settled_pnl"] += trade_pnl
                bucket["open_cost"] = max(
                    0.0, bucket["open_cost"] - trade_cost
                )
                if won:
                    bucket["wins"] += 1
                else:
                    bucket["losses"] += 1

        self._append_csv("resolutions.csv", {
            "timestamp": kw["ts"],
            "market_id": market["id"],
            "condition": condition,
            "slug": market["slug"],
            "asset": market["asset"],
            "winner": kw["winner"],
            "winner_token": kw["winner_token"],
            "entries": stats["entries"],
            "cost": cost,
            "payout": payout,
            "pnl": pnl,
            "roi": pnl / cost if cost else 0.0,
            "status": "RESOLVED",
        })

        winning_cost = sum(
            self._safe_float(t.get("notional"))
            for t in trades
            if t.get("token") == kw["winner_token"]
        )

        self._append_csv("markets.csv", {
            "market_id": market["id"],
            "condition": condition,
            "slug": market["slug"],
            "asset": market["asset"],
            "market": market["market"],
            "start_ts": market["start_ts"],
            "end_ts": market["end_ts"],
            "winner": kw["winner"],
            "entries": stats["entries"],
            "total_cost": cost,
            "total_shares": stats["shares"],
            "avg_entry": cost / stats["shares"] if stats["shares"] else 0.0,
            "first_entry": stats["first_entry"],
            "last_entry": stats["last_entry"],
            "max_exposure": stats["max_exposure"],
            "up_cost": stats["up_cost"],
            "down_cost": stats["down_cost"],
            "up_shares": stats["up_shares"],
            "down_shares": stats["down_shares"],
            "winning_cost": winning_cost,
            "losing_cost": max(0.0, cost - winning_cost),
            "payout": payout,
            "realized_pnl": pnl,
            "roi": pnl / cost if cost else 0.0,
            "resolved_ts": kw["ts"],
        })

        self._trade_cache.pop(condition, None)
        self.market_stats.pop(condition, None)

    def record_pending_order(self, order):
        self._append_csv("pending_orders.csv", {
            "timestamp": order["placed_ts"], "order_id": order["order_id"],
            "condition": order["condition"], "token": order["token"],
            "market": order["market"], "side": order["side"],
            "target_price": order["target_price"], "notional": order["notional"],
            "depth_ahead": order["depth_ahead"], "window_end_ts": order["window_end_ts"],
            "status": order["status"], "fill_price": None, "fill_ts": None,
            "fill_latency_s": None, "cumulative_volume": 0.0,
            "regime": (order.get("meta") or {}).get("regime"),
            "fine_band": (order.get("meta") or {}).get("fine_band"),
        })

    def record_fill_progress(self, order, price, size, ts):
        self._append_csv("fill_progress.csv", {"timestamp":ts,"order_id":order["order_id"],"token":order["token"],"trade_price":price,"trade_size":size,"cumulative_volume":order.get("cumulative_volume_through_price",0.0),"target_price":order["target_price"],"depth_ahead":order["depth_ahead"],"status":order.get("status")})

    def record_fill(self, order, trade):
        self._append_csv("fills.csv", {"timestamp":order.get("fill_ts"),"order_id":order["order_id"],"condition":order["condition"],"token":order["token"],"side":order["side"],"target_price":order["target_price"],"fill_price":order.get("fill_price"),"notional":order["notional"],"depth_ahead":order["depth_ahead"],"cumulative_volume":order.get("cumulative_volume_through_price"),"fill_latency_s":order.get("fill_latency_s"),"regime":(order.get("meta") or {}).get("regime"),"fine_band":(order.get("meta") or {}).get("fine_band")})

    def record_unfilled(self, order):
        self._append_csv("unfilled_orders.csv", {"timestamp":order.get("expired_ts"),"order_id":order["order_id"],"condition":order["condition"],"token":order["token"],"side":order["side"],"target_price":order["target_price"],"notional":order["notional"],"depth_ahead":order["depth_ahead"],"cumulative_volume":order.get("cumulative_volume_through_price",0.0),"unfilled_for_s":order.get("unfilled_for_s"),"regime":(order.get("meta") or {}).get("regime"),"fine_band":(order.get("meta") or {}).get("fine_band")})

    def record_fill_error(self, order, price, ts):
        self._append_csv("fill_errors.csv", {"timestamp":ts,"order_id":order["order_id"],"token":order["token"],"trade_price":price,"error":"ledger_callback_failed"})

    def record_instant_signal(self, order):
        self._append_csv("instant_fill_shadow.csv", {"timestamp":order["placed_ts"],"order_id":order["order_id"],"condition":order["condition"],"token":order["token"],"side":order["side"],"price":order["target_price"],"notional":order["notional"],"shares":order["notional"]/order["target_price"],"regime":(order.get("meta") or {}).get("regime"),"fine_band":(order.get("meta") or {}).get("fine_band"),"winner":None,"payout":None,"pnl":None,"status":"OPEN"})

    def record_execution_comparison_fill(self, order, trade):
        # Keep the immediate-fill shadow aligned with the actual paper ledger
        # fill first. The shadow is research/audit data, not a second position
        # ledger; the execution-comparison CSV remains the independent audit
        # trail and is written immediately afterward.
        self.record_instant_fill(order, trade)
        self._append_csv("execution_comparison.csv", {"timestamp":order.get("fill_ts"),"order_id":order["order_id"],"condition":order["condition"],"target_price":order["target_price"],"fill_price":order.get("fill_price"),"notional":order["notional"],"fill_latency_s":order.get("fill_latency_s"),"status":"FILLED"})

    def record_instant_fill(self, order, trade):
        """Mark an immediate shadow signal FILLED using actual execution terms."""
        path = self.root / "instant_fill_shadow.csv"
        if not path.exists():
            return
        with path.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        changed = False
        actual_price = self._safe_float(
            trade.get("price", order.get("fill_price")), 0.0
        )
        actual_notional = self._safe_float(
            trade.get("notional", order.get("notional")), 0.0
        )
        actual_shares = self._safe_float(
            trade.get("shares"),
            actual_notional / actual_price if actual_price > 0 else 0.0,
        )
        for row in rows:
            if row.get("order_id") != order.get("order_id"):
                continue
            if row.get("status") != "OPEN":
                continue
            row.update({
                "price": actual_price,
                "notional": actual_notional,
                "shares": actual_shares,
                "status": "FILLED",
            })
            changed = True
            break
        if changed:
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh, fieldnames=SCHEMAS["instant_fill_shadow.csv"]
                )
                writer.writeheader()
                writer.writerows(rows)

    def reconcile_instant_shadow(self, ledger=None):
        """Reconcile legacy OPEN shadow rows from durable execution evidence.

        Comparison-audit rows are preferred. If a process crashed after the
        ledger BUY but before the comparison/shadow update, the exact BUY
        timestamp/notional/token is sufficient to recover the actual execution
        terms. Any remaining OPEN row is marked ABANDONED so it can never later
        be mistaken for a filled position.
        """
        shadow = self.root / "instant_fill_shadow.csv"
        comparison = self.root / "execution_comparison.csv"
        if not shadow.exists():
            return
        with shadow.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        fills = {}
        if comparison.exists():
            with comparison.open(newline="", encoding="utf-8") as fh:
                fills = {
                    r.get("order_id"): r
                    for r in csv.DictReader(fh)
                    if r.get("status") == "FILLED"
                }
        buys = list(getattr(ledger, "trades", []) or []) if ledger is not None else []
        changed = False
        for row in rows:
            if row.get("status") != "OPEN":
                continue

            fill = fills.get(row.get("order_id"))
            price = self._safe_float(fill.get("fill_price"), 0.0) if fill else 0.0
            notional = self._safe_float(fill.get("notional"), 0.0) if fill else 0.0

            # Crash-safe fallback: the ledger BUY is durable even if the
            # secondary research CSV update did not complete.
            if price <= 0 or notional <= 0:
                target_ts = self._safe_float(row.get("timestamp"), -1.0)
                target_notional = self._safe_float(row.get("notional"), -1.0)
                matches = [
                    t for t in buys
                    if t.get("action") == "BUY"
                    and str(t.get("condition")) == str(row.get("condition"))
                    and str(t.get("token")) == str(row.get("token"))
                    and abs(self._safe_float(t.get("ts"), -2.0) - target_ts) <= 1e-6
                    and abs(self._safe_float(t.get("notional"), -2.0) - target_notional) <= 1e-8
                ]
                if matches:
                    trade = matches[0]
                    price = self._safe_float(trade.get("price"), 0.0)
                    notional = self._safe_float(trade.get("notional"), 0.0)

            if price > 0 and notional > 0:
                row["price"] = price
                row["notional"] = notional
                row["shares"] = notional / price
                row["status"] = "FILLED"
            else:
                row["status"] = "ABANDONED"
            changed = True

        if changed:
            with shadow.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh, fieldnames=SCHEMAS["instant_fill_shadow.csv"]
                )
                writer.writeheader()
                writer.writerows(rows)

    def record_instant_unfilled(self, order, reason, ts=None):
        """Close an immediate-execution shadow signal without pretending it filled."""
        path = self.root / "instant_fill_shadow.csv"
        if not path.exists():
            return
        rows = []
        with path.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        changed = False
        for row in rows:
            if row.get("order_id") != order.get("order_id"):
                continue
            if row.get("status") != "OPEN":
                continue
            row["status"] = "UNFILLED"
            row["resolved_ts"] = ts if ts is not None else time.time()
            changed = True
            break
        if changed:
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh, fieldnames=SCHEMAS["instant_fill_shadow.csv"]
                )
                writer.writeheader()
                writer.writerows(rows)

    def record_instant_resolution(self, market, winner_token, ts):
        path=self.root/"instant_fill_shadow.csv"
        if not path.exists(): return
        rows=[]
        with path.open(newline="",encoding="utf-8") as fh: rows=list(csv.DictReader(fh))
        changed=False
        for r in rows:
            # Only actual immediate fills are settled here. UNFILLED signals and
            # legacy OPEN signals without a corresponding execution audit must
            # never become synthetic positions/payouts.
            if r.get("condition")!=market["condition"] or r.get("status")!="FILLED": continue
            shares=self._safe_float(r.get("shares")); cost=self._safe_float(r.get("notional")); win=r.get("token")==winner_token
            payout=shares if win else 0.0; r.update({"winner":"WIN" if win else "LOSS","payout":payout,"pnl":payout-cost,"status":"RESOLVED","resolved_ts":ts}); changed=True
        if changed:
            with path.open("w",newline="",encoding="utf-8") as fh:
                fields=SCHEMAS["instant_fill_shadow.csv"]; writer=csv.DictWriter(fh,fieldnames=fields); writer.writeheader(); writer.writerows(rows)

    def record_resolution_error(self, **kw):
        market = kw["market"]
        self.last_resolution_error[market["condition"]] = {
            "timestamp": kw["ts"],
            "status": kw["status"],
        }

    def record_pnl(self, timestamp, metrics):
        self._append_csv("pnl_1min.csv", {
            "timestamp": timestamp,
            "equity": metrics.get("equity"),
            "total_pnl": metrics.get("pnl"),
            "realized_pnl": metrics.get("realized"),
            "unrealized_pnl": metrics.get("unrealized"),
            "cash": metrics.get("cash"),
            "open_cost": metrics.get("open_cost"),
            "market_value": metrics.get("market_value"),
            "drawdown": metrics.get("drawdown"),
            "positions": metrics.get("positions"),
            "marked": metrics.get("marked"),
        })

        total_trades = sum(v["trades"] for v in self.regime_stats.values())
        for regime, stats in self.regime_stats.items():
            settled = stats["settled_trades"]
            self._append_csv("regime_1min.csv", {
                "timestamp": timestamp,
                "regime": regime,
                "trades": stats["trades"],
                "notional": stats["notional"],
                "trade_share": (
                    stats["trades"] / total_trades if total_trades else 0.0
                ),
                "settled_trades": settled,
                "wins": stats["wins"],
                "losses": stats["losses"],
                "win_rate": stats["wins"] / settled if settled else 0.0,
                "settled_cost": stats["settled_cost"],
                "settled_pnl": stats["settled_pnl"],
                "settled_roi": (
                    stats["settled_pnl"] / stats["settled_cost"]
                    if stats["settled_cost"] else 0.0
                ),
                "avg_settled_pnl": (
                    stats["settled_pnl"] / settled if settled else 0.0
                ),
                "open_cost": stats["open_cost"],
            })

    def rebuild_from_ledger(self, ledger):
        """Rebuild current in-memory research state after a restart."""
        self._trade_cache.clear()
        self.market_stats.clear()
        self.regime_stats = {
            regime: {
                "trades": 0, "notional": 0.0, "settled_trades": 0,
                "wins": 0, "losses": 0, "settled_cost": 0.0,
                "settled_pnl": 0.0, "open_cost": 0.0,
            }
            for regime in REGIMES
        }

        settled_conditions = {
            item.get("condition")
            for item in ledger.trades
            if item.get("action") == "SETTLE"
        }
        open_conditions = {
            position.get("condition")
            for position in ledger.positions.values()
        }
        active_conditions = open_conditions | {
            condition
            for condition in self._trade_conditions_for_unsettled(
                ledger.trades, settled_conditions
            )
        }

        for item in ledger.trades:
            if item.get("action") != "BUY":
                continue
            condition = item.get("condition")
            if condition not in active_conditions:
                continue

            trade = dict(item)
            self._trade_cache[condition].append(trade)

            regime = trade.get("regime") or self._regime(trade.get("price"))
            notional = self._safe_float(trade.get("notional"))
            shares = self._safe_float(trade.get("shares"))

            stats = self.market_stats[condition]
            stats["entries"] += 1
            stats["cost"] += notional
            stats["shares"] += shares
            stats["first_entry"] = (
                trade.get("ts")
                if stats["first_entry"] is None
                else min(stats["first_entry"], trade.get("ts"))
            )
            stats["last_entry"] = trade.get("ts")
            stats["max_exposure"] = max(
                stats["max_exposure"],
                self._safe_float(trade.get("market_exposure_after")),
            )
            stats["asset"] = trade.get("asset", "")
            stats["market"] = trade.get("market", "")
            stats["slug"] = trade.get("slug", "")
            stats["market_id"] = trade.get("market_id", "")
            stats["start_ts"] = self._safe_float(trade.get("start_ts"))
            stats["end_ts"] = self._safe_float(trade.get("end_ts"))

            if trade.get("side") == "Up":
                stats["up_cost"] += notional
                stats["up_shares"] += shares
            else:
                stats["down_cost"] += notional
                stats["down_shares"] += shares

            if regime in self.regime_stats:
                bucket = self.regime_stats[regime]
                bucket["trades"] += 1
                bucket["notional"] += notional
                bucket["open_cost"] += notional

    @staticmethod
    def _trade_conditions_for_unsettled(trades, settled_conditions):
        seen = []
        for item in trades:
            if item.get("action") != "BUY":
                continue
            condition = item.get("condition")
            if condition and condition not in settled_conditions and condition not in seen:
                seen.append(condition)
        return seen

    def maintenance(self):
        decision_days = 7
        orderbook_days = 2
        self._prune_jsonl("decisions.jsonl", decision_days)
        self._prune_jsonl("orderbooks.jsonl", orderbook_days)

    def _prune_jsonl(self, filename, retention_days):
        path = self.root / filename
        if not path.exists():
            return

        cutoff = time.time() - retention_days * 86400.0
        tmp = path.with_suffix(path.suffix + ".tmp")

        with self.lock, path.open("r", encoding="utf-8") as src, tmp.open(
            "w", encoding="utf-8"
        ) as dst:
            for line in src:
                try:
                    obj = json.loads(line)
                    if float(obj.get("t", 0)) >= cutoff:
                        dst.write(line)
                except Exception:
                    # Preserve malformed historical records for audit rather
                    # than silently destroying them.
                    dst.write(line)

        tmp.replace(path)
