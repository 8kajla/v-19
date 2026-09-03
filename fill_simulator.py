
"""Realistic shadow-order fill simulation for V17.

This module never places a live order. It shadows a resting maker order against
observed public trade prints and uses the book depth captured at placement as
an explicitly approximate queue-position proxy.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional


class FillSimulator:
    TERMINAL = {"FILLED", "EXPIRED_UNFILLED", "CANCELLED"}

    def __init__(
        self,
        path: Path,
        ledger,
        research,
        fill_callback: Callable[[dict, float, float], dict],
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.ledger = ledger
        self.research = research
        self.fill_callback = fill_callback
        self.lock = threading.RLock()
        self.orders: Dict[str, dict] = {}
        self._progress_buffer = []
        self._progress_flush_seconds = max(0.25, float(__import__("os").getenv("FILL_PROGRESS_FLUSH_SECONDS", "1")))
        self._last_progress_flush = 0.0
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            rows = data.get("orders", []) if isinstance(data, dict) else []
            self.orders = {str(x["order_id"]): x for x in rows if x.get("order_id")}
        except Exception as exc:
            raise RuntimeError(f"fill simulator state corrupt/unreadable: {exc}")

    def save(self) -> None:
        with self.lock:
            tmp = self.path.with_suffix(".tmp")
            payload = {"orders": list(self.orders.values())[-10000:]}
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(self.path)

    def restore(self) -> None:
        # The ledger remains the source of truth for actual filled positions.
        # Orders stay here only to survive restarts and to audit unfilled signals.
        return

    @staticmethod
    def _normal_depth(v) -> float:
        try:
            return max(0.0, float(v))
        except (TypeError, ValueError):
            return 0.0

    def place(self, *, condition, token, market, side, target_price, notional,
              placed_ts, window_end_ts, depth_ahead, meta) -> dict:
        target_price = float(target_price)
        notional = float(notional)
        if not 0.0 < target_price < 1.0:
            raise ValueError("invalid shadow-order target_price")
        if notional <= 0:
            raise ValueError("shadow-order notional must be positive")
        order = {
            "order_id": f"shadow-{uuid.uuid4().hex}",
            "condition": str(condition),
            "token": str(token),
            "market": market,
            "side": str(side),
            "target_price": target_price,
            "notional": notional,
            "placed_ts": float(placed_ts),
            "window_end_ts": float(window_end_ts),
            # Approximation: current resting size at our target price is used
            # as queue position. This cannot identify our exact queue place.
            "depth_ahead": self._normal_depth(depth_ahead),
            "cumulative_volume_through_price": 0.0,
            "fill_latency_s": None,
            "fill_price": None,
            "fill_ts": None,
            "status": "PENDING",
            "meta": dict(meta or {}),
        }
        with self.lock:
            self.orders[order["order_id"]] = order
            self.save()
        self.research.record_pending_order(order)
        return order

    def pending_for_token(self, token) -> list[dict]:
        with self.lock:
            return [
                x for x in self.orders.values()
                if x.get("status") == "PENDING" and x.get("token") == str(token)
            ]

    def _existing_trade_for_order(self, order_id):
        for trade in reversed(getattr(self.ledger, "trades", [])):
            if trade.get("action") != "BUY":
                continue
            if (trade.get("meta") or {}).get("shadow_order_id") == order_id or trade.get("shadow_order_id") == order_id:
                return trade
        return None

    def on_trade_print(self, token, trade_price, trade_size, trade_ts=None) -> list[dict]:
        trade_ts = time.time() if trade_ts is None else float(trade_ts)
        try:
            trade_price = float(trade_price)
            trade_size = max(0.0, float(trade_size))
        except (TypeError, ValueError):
            return []
        if not 0.0 < trade_price <= 1.0 or trade_size <= 0:
            return []

        filled = []
        with self.lock:
            for order in self.pending_for_token(token):
                # Explicit maker-side approximation for the BUY side:
                # any real print at or below our resting bid means opposing
                # liquidity reached our level or better.
                if order["side"] not in {"Up", "Down"}:
                    continue
                if trade_price > float(order["target_price"]):
                    continue

                order["cumulative_volume_through_price"] = (
                    float(order.get("cumulative_volume_through_price", 0.0))
                    + trade_size
                )
                self._progress_buffer.append({"order": dict(order), "price": trade_price, "size": trade_size, "ts": trade_ts})

                # With zero depth ahead the first qualifying print is enough.
                # Otherwise wait until observed volume reaches the captured
                # queue-depth proxy.
                if order["cumulative_volume_through_price"] < float(order["depth_ahead"]):
                    continue

                try:
                    trade = self.mark_filled(order, fill_price=trade_price, fill_ts=trade_ts)
                    filled.append(trade)
                except Exception:
                    # Do not leave an order half-marked as filled when ledger
                    # execution fails. Keep it pending for the next loop and
                    # surface the error through the research logger.
                    self.research.record_fill_error(order, trade_price, trade_ts)
        return filled

    def mark_filled(self, order, *, fill_price, fill_ts) -> dict:
        with self.lock:
            if order.get("status") != "PENDING":
                return {}
            fill_price = float(fill_price)
            fill_ts = float(fill_ts)
            if not 0.0 < fill_price < 1.0:
                raise ValueError("invalid observed fill price")

            # Crash-safe/idempotent recovery: if the ledger was committed but
            # the simulator process died before persisting FILLED, never buy a
            # second time on restart/reconnect.
            # Compute and persist latency BEFORE invoking the callback. The
            # callback runs synchronously and is allowed to log/read the order;
            # the old implementation populated this field only after callback
            # return, which caused Railway fills to crash with None formatting.
            fill_latency = max(0.0, fill_ts - float(order["placed_ts"]))
            order["fill_price"] = fill_price
            order["fill_ts"] = fill_ts
            order["fill_latency_s"] = fill_latency

            existing = self._existing_trade_for_order(order["order_id"])
            if existing is not None:
                trade = existing
            else:
                meta = dict(order.get("meta") or {})
                meta.update({
                    "shadow_order_id": order["order_id"],
                    "fill_latency_s": fill_latency,
                    "target_price": order["target_price"],
                    "depth_ahead": order["depth_ahead"],
                    "cumulative_volume_through_price": order["cumulative_volume_through_price"],
                    "fill_simulation": "TRADE_TAPE_QUEUE_PROXY",
                })
                # Keep the order PENDING until the ledger buy/callback succeeds.
                # If the callback raises after committing the ledger, the next
                # tape print is idempotently recovered by _existing_trade_for_order.
                original_meta = order.get("meta")
                order["meta"] = meta
                try:
                    trade = self.fill_callback(order, fill_price, fill_ts)
                except Exception:
                    # Restore the signal metadata while retaining the observed
                    # fill attempt fields for diagnostics/retry.
                    order["meta"] = original_meta if original_meta is not None else {}
                    raise

            order["status"] = "FILLED"
            order["fill_price"] = float(trade.get("price", fill_price)) if isinstance(trade, dict) else fill_price
            order["fill_ts"] = float(trade.get("ts", fill_ts)) if isinstance(trade, dict) else fill_ts
            order["fill_latency_s"] = max(0.0, order["fill_ts"] - float(order["placed_ts"]))
            self.research.record_fill(order, trade)
            self.save()
            return trade

    def flush_progress(self, force=False, now=None) -> int:
        now = time.time() if now is None else float(now)
        with self.lock:
            if not self._progress_buffer:
                return 0
            if not force and now - self._last_progress_flush < self._progress_flush_seconds:
                return 0
            rows = self._progress_buffer
            self._progress_buffer = []
            self._last_progress_flush = now
        written = 0
        for item in rows:
            try:
                self.research.record_fill_progress(item["order"], item["price"], item["size"], item["ts"])
                written += 1
            except Exception:
                # Research logging must never block or break execution.
                pass
        return written

    def expire_due(self, now=None) -> list[dict]:
        now = time.time() if now is None else float(now)
        expired = []
        with self.lock:
            for order in self.orders.values():
                if order.get("status") != "PENDING":
                    continue
                if now < float(order["window_end_ts"]):
                    continue
                order["status"] = "EXPIRED_UNFILLED"
                order["expired_ts"] = now
                order["unfilled_for_s"] = max(0.0, now - float(order["placed_ts"]))
                expired.append(order.copy())
                self.research.record_unfilled(order)
            if expired:
                self.save()
        return expired

    def active_count(self) -> int:
        with self.lock:
            return sum(x.get("status") == "PENDING" for x in self.orders.values())

    def stats(self) -> dict:
        with self.lock:
            rows = list(self.orders.values())
        total = len(rows)
        filled = [x for x in rows if x.get("status") == "FILLED"]
        unfilled = [x for x in rows if x.get("status") == "EXPIRED_UNFILLED"]
        latency = [float(x["fill_latency_s"]) for x in filled if x.get("fill_latency_s") is not None]
        def rate(n): return n / total if total else 0.0
        by_regime = {}
        for x in rows:
            regime = (x.get("meta") or {}).get("regime") or "OTHER"
            bucket = by_regime.setdefault(regime, {"signals":0, "filled":0, "expired":0})
            bucket["signals"] += 1
            bucket["filled"] += int(x.get("status") == "FILLED")
            bucket["expired"] += int(x.get("status") == "EXPIRED_UNFILLED")
        for bucket in by_regime.values():
            bucket["fill_rate"] = bucket["filled"] / bucket["signals"] if bucket["signals"] else 0.0
            bucket["expiry_rate"] = bucket["expired"] / bucket["signals"] if bucket["signals"] else 0.0
        return {
            "signals": total,
            "filled": len(filled),
            "expired_unfilled": len(unfilled),
            "fill_rate": rate(len(filled)),
            "expiry_rate": rate(len(unfilled)),
            "avg_fill_latency_s": sum(latency) / len(latency) if latency else None,
            "p50_fill_latency_s": sorted(latency)[len(latency)//2] if latency else None,
            "by_regime": by_regime,
        }
