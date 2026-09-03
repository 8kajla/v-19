import json
import os
import time
import uuid
from pathlib import Path


class PaperLedger:
    """Atomic paper ledger with auditable realized P&L.

    Realized P&L is derived from SETTLE records. The persisted `realized`
    value is treated as a cache and reconciled on every load/save. This makes
    it impossible for an unlogged side-path to silently change realized P&L.
    """
    def __init__(self, path, initial_cash=1000):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.cash = float(initial_cash)
        self.realized = 0.0
        self.positions = {}
        self.trades = []
        self.start_equity = float(initial_cash)
        self.peak_equity = float(initial_cash)
        self.last_equity = float(initial_cash)
        self.realized_audit = 0.0
        self._load()

    @staticmethod
    def _settlement_total(trades):
        return sum(float(t.get("pnl", 0.0)) for t in trades if t.get("action") == "SETTLE")

    def _reconcile_realized(self):
        audit = self._settlement_total(self.trades)
        self.realized_audit = audit
        self.realized = audit

    def _load(self):
        if not self.path.exists():
            return
        try:
            d = json.loads(self.path.read_text())
            self.cash = float(d["cash"])
            self.positions = d.get("positions", {})
            self.trades = d.get("trades", [])
            self.start_equity = float(d.get("start_equity", self.cash))
            self.peak_equity = float(d.get("peak_equity", self.start_equity))
            self.last_equity = float(d.get("last_equity", self.start_equity))
            persisted = float(d.get("realized", 0.0))
            self._reconcile_realized()
            # Persisted realized must never silently override the auditable sum.
            self.realized_cache_mismatch = persisted - self.realized
        except Exception as e:
            raise RuntimeError(f"paper state corrupt/unreadable: {e}")

    def save(self):
        self._reconcile_realized()
        tmp = self.path.with_suffix(".tmp")
        payload = {
            "cash": self.cash,
            "realized": self.realized,
            "realized_audit": self.realized_audit,
            "positions": self.positions,
            # Keep the complete auditable trade/settlement history. Truncating
            # this list corrupts realized-P&L reconciliation and, after restart,
            # causes the strategy's distribution state to forget older fills.
            "trades": self.trades,
            "start_equity": self.start_equity,
            "peak_equity": self.peak_equity,
            "last_equity": self.last_equity,
        }
        tmp.write_text(json.dumps(payload, indent=2))
        os.replace(tmp, self.path)

    def total_open_cost(self):
        return sum(float(p.get("cost", 0.0)) for p in self.positions.values())

    def exposure(self, condition):
        return sum(float(p.get("cost", 0.0)) for p in self.positions.values() if p.get("condition") == condition)

    def buy(self, condition, token, market, side, price, notional, ts, meta=None):
        price = float(price)
        notional = min(float(notional), self.cash)
        if not 0.0 < price < 1.0: raise ValueError("invalid paper execution price")
        if notional <= 0: raise ValueError("insufficient paper cash")
        shares = notional / price
        key = f"{condition}:{token}"
        p = self.positions.get(key, {"condition":condition,"token":token,"market":market,"side":side,"shares":0.0,"cost":0.0,"avg":0.0})
        p["shares"] += shares; p["cost"] += notional; p["avg"] = p["cost"] / p["shares"]; p["last_price"] = price; p.update(meta or {})
        self.positions[key] = p
        self.cash -= notional
        t = {"ts":ts,"action":"BUY","condition":condition,"token":token,"market":market,"side":side,"price":price,"shares":shares,"notional":notional,"status":"OPEN"}
        t.update(meta or {}); self.trades.append(t); self.save(); return t

    def mark(self, books):
        value = unreal = 0.0; marked = 0
        for p in self.positions.values():
            bid = books.get(p["token"]); px = bid if bid is not None else p["avg"]; v = p["shares"] * px
            value += v; unreal += v - p["cost"]; marked += 1 if bid is not None else 0
        equity = self.cash + value
        self.peak_equity = max(self.peak_equity, equity); self.last_equity = equity; self.save()
        return {"cash":self.cash,"open_cost":self.total_open_cost(),"market_value":value,"unrealized":unreal,"realized":self.realized,"equity":equity,"pnl":equity-self.start_equity,"drawdown":equity-self.peak_equity,"marked":marked}

    def settle(self, condition, winner_token):
        # Build the complete settlement first. Nothing is mutated until every
        # position has a valid numeric cost/share record.
        closed = []
        for key, p in list(self.positions.items()):
            if p.get("condition") != condition: continue
            shares = float(p["shares"]); cost = float(p["cost"])
            payout = shares if p.get("token") == winner_token else 0.0
            closed.append({"key":key,"pnl":payout-cost,"settlement_per_share":1.0 if p.get("token")==winner_token else 0.0,
                           "shares":shares,"cost":cost,"payout":payout,"side":p.get("side","")})
        if not closed: return []
        ts = time.time()
        records=[]
        for x in closed:
            p=self.positions[x["key"]]
            records.append({"ts":ts,"settlement_id":uuid.uuid4().hex,"action":"SETTLE","condition":condition,"token":p["token"],"side":p.get("side",""),"price":p.get("avg",0.0),"shares":x["shares"],"notional":x["cost"],"payout":x["payout"],"pnl":x["pnl"],"settlement_per_share":x["settlement_per_share"],"status":"WIN" if x["pnl"]>=0 else "LOSS"})
        # Atomic state transition in memory, followed by one durable save.
        self.cash += sum(x["payout"] for x in closed)
        self.trades.extend(records)
        for x in closed: del self.positions[x["key"]]
        self._reconcile_realized()
        self.save()
        return closed
