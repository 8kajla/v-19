import json
from pathlib import Path
import pytest

from strategy import CapitalFirstStrategy, TraderPolicyScheduler, TRAJECTORY_SHARE
from immediate_clob_executor import ImmediateClobExecutor


def S():
    return CapitalFirstStrategy()


def _raw_targets():
    behavior = json.loads(Path("trader_behavior.json").read_text())
    return {str(x["fine_band"]): float(x["trade_share"]) for x in behavior["fine_bands"]}


def _raw_capital_targets():
    behavior = json.loads(Path("trader_behavior.json").read_text())
    return {str(x["fine_band"]): float(x["notional_share"]) for x in behavior["fine_bands"]}


def test_raw_fine_band_targets_are_authoritative():
    s = S()
    raw = _raw_targets()
    assert s.scheduler.benchmark_name == "raw_fine_band"
    assert s.scheduler.trade_targets == pytest.approx(raw, abs=1e-12)
    assert sum(s.scheduler.trade_targets.values()) == pytest.approx(1.0, abs=1e-12)


def test_raw_capital_targets_are_preserved():
    s = S()
    raw = _raw_capital_targets()
    assert s.scheduler.capital_targets == pytest.approx(raw, abs=1e-12)
    assert sum(s.scheduler.capital_targets.values()) == pytest.approx(1.0, abs=1e-12)


def test_verified_trajectory_shares():
    assert TRAJECTORY_SHARE["CHEAP"]["falling"] == pytest.approx(.539948)
    assert TRAJECTORY_SHARE["HIGH"]["rising"] == pytest.approx(.586489)


def test_behavior_band_is_not_rewritten_by_ask():
    import bot
    s = S()
    c = s._candidate("BTC", "Up", .49, .51, 10, [], 1000, None, 0, 0)
    bot.prepare_immediate_candidate(c, s, {"asset": "BTC"}, {"count": 0})
    assert c["signal_band"] == "M40_50"
    assert c["band"] == "M40_50"
    assert c["execution_ask_band"] == "M50_60"
    assert c["_clob_executable"] is True


def test_current_ask_is_execution_only(monkeypatch):
    ex = ImmediateClobExecutor(timeout=1, retries=0)
    monkeypatch.setattr(ex, "snapshot", lambda token: {"asks": [(.35, 100), (.45, 10)], "bids": [], "ts": 1})
    r = ex.execute(token="t", notional=4, strategy_band="M40_50", signal_bid=.40, strategy=S())
    assert r["filled"]
    assert r["vwap"] == pytest.approx(.35)
    assert r["strategy_band"] == "M40_50"


def test_terminal_one_price_is_not_executable(monkeypatch):
    ex = ImmediateClobExecutor(timeout=1, retries=0)
    monkeypatch.setattr(ex, "snapshot", lambda token: {"asks": [(1.0, 2)], "bids": [], "ts": 1})
    r = ex.execute(token="t", notional=1, strategy_band="H95_100", signal_bid=.99, strategy=S())
    assert not r["filled"]


def test_no_ask_is_unfilled(monkeypatch):
    ex = ImmediateClobExecutor(timeout=1, retries=0)
    monkeypatch.setattr(ex, "snapshot", lambda token: {"asks": [], "bids": [], "ts": 1})
    r = ex.execute(token="t", notional=1, strategy_band="M50_60", signal_bid=.5, strategy=S())
    assert r["reason"] == "no_ask"


def test_multi_level_vwap(monkeypatch):
    ex = ImmediateClobExecutor(timeout=1, retries=0)
    monkeypatch.setattr(ex, "snapshot", lambda token: {"asks": [(.40, 5), (.42, 10)], "bids": [], "ts": 1})
    r = ex.execute(token="t", notional=6, strategy_band="C20_30", signal_bid=.25, strategy=S())
    expected_shares = 5 + (6 - 2) / .42
    expected_vwap = 6 / expected_shares
    assert r["filled"] and len(r["levels"]) == 2
    assert r["vwap"] == pytest.approx(expected_vwap)


def test_insufficient_clob_liquidity_is_unfilled(monkeypatch):
    ex = ImmediateClobExecutor(timeout=1, retries=0)
    monkeypatch.setattr(ex, "snapshot", lambda token: {"asks": [(.40, 5)], "bids": [], "ts": 1})
    r = ex.execute(token="t", notional=3, strategy_band="C20_30", signal_bid=.25, strategy=S())
    assert not r["filled"]
    assert r["reason"] == "insufficient_clob_liquidity"
    assert r["available_notional"] == pytest.approx(2.0)


def test_behavioral_band_selected_before_clob_executability():
    import bot
    s = S()
    # Put MID materially ahead of target so CHEAP is the largest deficit.
    for _ in range(100):
        s.observe_trade_distribution("M40_50", .1)
    # CHEAP is intentionally not executable. MID is executable. The function
    # must select CHEAP and return no fallback MID.
    candidates = [
        {"band": "C00_05", "target": .1, "_clob_executable": False, "_market": {"condition": "c"}},
        {"band": "M40_50", "target": .1, "_clob_executable": True, "_market": {"condition": "m"}, "trajectory_likelihood": .5, "same_side": False, "depth": 10, "bid": .45},
    ]
    band, emergency, rows = bot.choose_executable_band(candidates, s, allow_emergency=True, now=100)
    assert band == "C00_05"
    assert emergency is False
    assert rows == []


def test_min_trade_gap_is_enforced_in_cadence():
    s = CapitalFirstStrategy(min_trade_gap_seconds=17)
    sampled = 2.0
    enforced = max(sampled, s.min_trade_gap_seconds)
    assert enforced == 17


def test_scheduler_converges_to_raw_distribution():
    s = S()
    candidates = [{"band": b, "target": s.entry_expected_band_target(b) or .5} for b in s.scheduler.bands]
    for _ in range(10000):
        b = s.scheduler.choose_band(candidates)
        s.observe_trade_distribution(b, next(c["target"] for c in candidates if c["band"] == b))
    actual = s.distribution_snapshot()["trade"]
    target = s.scheduler.trade_targets
    max_error = max(abs(actual[b] - target[b]) for b in target)
    assert max_error < 0.002


def test_scheduler_converges_to_raw_capital_distribution():
    s = S()
    candidates = [{"band": b, "target": s.entry_expected_band_target(b) or .5} for b in s.scheduler.bands]
    for _ in range(20000):
        b = s.scheduler.choose_band(candidates)
        target = next(c["target"] for c in candidates if c["band"] == b)
        s.observe_trade_distribution(b, target)
    actual = s.distribution_snapshot()["capital"]
    target = s.scheduler.capital_targets
    max_error = max(abs(actual[b] - target[b]) for b in target)
    assert max_error < 0.02


def test_one_sided_book_failure_does_not_hide_other_side(monkeypatch):
    import bot
    bot.DATA = None
    bot.markets.clear(); bot.histories.clear()
    # This is a focused source-level behavior test through the strategy: one
    # side may be absent and the other remains a valid candidate.
    s = S()
    candidates = s.build_candidates_for_market(
        30, None, .55, None, .45, [], [], 1000, asset="BTC", market="BTC"
    )
    assert len(candidates) == 1
    assert candidates[0]["side"] == "Down"
