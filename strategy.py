from __future__ import annotations
from dataclasses import dataclass
import json, time, random, math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

BANDS: Tuple[Tuple[str, float, float, str], ...] = (
    ("C00_05", 0.00, 0.05, "CHEAP"),
    ("C05_10", 0.05, 0.10, "CHEAP"),
    ("C10_15", 0.10, 0.15, "CHEAP"),
    ("C15_20", 0.15, 0.20, "CHEAP"),
    ("C20_30", 0.20, 0.30, "CHEAP"),
    ("M30_40", 0.30, 0.40, "MID"),
    ("M40_50", 0.40, 0.50, "MID"),
    ("M50_60", 0.50, 0.60, "MID"),
    ("M60_70", 0.60, 0.70, "MID"),
    ("R70_80", 0.70, 0.80, "CORE"),
    ("R80_90", 0.80, 0.90, "CORE"),
    ("H90_95", 0.90, 0.95, "HIGH"),
    ("H95_100", 0.95, 1.00, "HIGH"),
)

# Full-scale trader-history trajectory shares: 778,116 trades / 76,154 markets.
TRAJECTORY_SHARE = {
    # Verified from the 9,651 consecutive same-market pairs.
    "CHEAP": {"rising": 0.137582, "falling": 0.539948, "flat": 0.322470},
    "MID":   {"rising": 0.341446, "falling": 0.422447, "flat": 0.236107},
    "CORE":  {"rising": 0.499717, "falling": 0.280757, "flat": 0.219526},
    "HIGH":  {"rising": 0.586489, "falling": 0.133042, "flat": 0.280469},
}
TRAJECTORY_THRESHOLD = 0.005

BAND_INDEX = {band: i for i, (band, *_rest) in enumerate(BANDS)}


@dataclass
class Signal:
    side: str
    price: float
    score: float
    notional: float
    reason: str


class EmpiricalTraderProcess:
    """
    Observable trader-process model.

    Uses measured distributions for:
      * global intertrade cadence
      * fine price-band frequency
      * same-side continuation
      * fine-band x entry-number notional

    It deliberately does NOT claim to know the trader's hidden trigger.
    """
    PERSISTENCE = 0.8805590616

    def __init__(self, behavior: dict, seed: int = 20260831):
        self.rng = random.Random(seed)

        gap_rows = behavior.get("intertrade_gap_histogram_seconds") or []
        self.gaps = [float(x["gap_seconds"]) for x in gap_rows]
        self.gap_weights = [float(x["count"]) for x in gap_rows]

        band_rows = behavior.get("fine_bands") or []
        self.band_values = [str(x["fine_band"]) for x in band_rows]
        self.band_weights = [float(x["trade_share"]) for x in band_rows]

        if not self.gaps or not any(self.gap_weights):
            raise ValueError("trader_behavior.json missing intertrade gap distribution")
        if not self.band_values or not any(self.band_weights):
            raise ValueError("trader_behavior.json missing fine-band distribution")

    def sample_gap(self) -> float:
        """
        Sample the observed intertrade cadence while preventing a rare
        extreme historical gap from creating a long artificial pause.

        The empirical distribution is preserved for normal gaps. A maximum
        wait of 20 seconds prevents rare multi-minute observations from
        making the live paper simulator appear inactive.
        """
        sampled = float(
            self.rng.choices(
                self.gaps,
                weights=self.gap_weights,
                k=1,
            )[0]
        )
        return min(sampled, 20.0)

    # Backward-compatible name used by older bot builds.
    def sample_delay(self) -> float:
        return self.sample_gap()

    def sample_target_band(self) -> str:
        return str(self.rng.choices(self.band_values, weights=self.band_weights, k=1)[0])

    def should_continue_side(self) -> bool:
        return self.rng.random() < self.PERSISTENCE

    @staticmethod
    def distance_to_band(actual_band: str, target_band: str) -> int:
        return abs(BAND_INDEX.get(actual_band, 999) - BAND_INDEX.get(target_band, 999))


class TraderPolicyScheduler:
    """Behavioral allocation controller.

    This is intentionally not a market-signal generator. It only reproduces
    the observed fine-band trade-share and capital-share distributions. The
    CLOB is consulted only after a band/size decision has been made.
    """

    def __init__(self, behavior, seed=20260831):
        self.rng = random.Random(seed)

        raw = {
            str(x["fine_band"]): float(x["trade_share"])
            for x in behavior.get("fine_bands", [])
        }

        self.capital_targets = {
            str(x["fine_band"]): float(x["notional_share"])
            for x in behavior.get("fine_bands", [])
        }

        if not raw or set(raw) != set(self.capital_targets):
            raise ValueError("invalid trader fine-band distribution")

        # The raw fine-band distribution is the source of truth. Do not
        # renormalize it into an older four-regime benchmark: doing so changes
        # the observed trader behavior we are trying to reproduce.
        self.benchmark_name = "raw_fine_band"

        total_trade_share = sum(raw.values())
        if total_trade_share <= 0:
            raise ValueError("invalid trader fine-band trade shares")

        self.trade_targets = {
            b: v / total_trade_share
            for b, v in raw.items()
        }

        self.bands = list(self.trade_targets)
        self.trade_counts = {b: 0 for b in self.bands}
        self.capital = {b: 0.0 for b in self.bands}

    def observe(self, band, notional):
        if band not in self.trade_counts:
            return

        self.trade_counts[band] += 1
        self.capital[band] += max(0.0, float(notional))

    def restore(self, trades, fine_band_fn):
        self.trade_counts = {b: 0 for b in self.bands}
        self.capital = {b: 0.0 for b in self.bands}

        for t in trades or []:
            if t.get("action") != "BUY":
                continue

            band = t.get("fine_band")

            if band not in self.trade_counts:
                try:
                    band, _ = fine_band_fn(
                        float(t.get("signal_price", t.get("price")))
                    )
                except Exception:
                    continue

            if band in self.trade_counts:
                self.trade_counts[band] += 1
                self.capital[band] += max(
                    0.0,
                    float(t.get("notional", t.get("cost", 0))),
                )

    def projected_state(self, band, notional):
        nt = sum(self.trade_counts.values()) + 1
        nc = sum(self.capital.values()) + max(0, float(notional))

        tr = {
            b: (self.trade_counts[b] + (b == band)) / nt
            for b in self.bands
        }

        ca = {
            b: (
                self.capital[b]
                + (float(notional) if b == band else 0)
            ) / nc
            if nc
            else 0
            for b in self.bands
        }

        return tr, ca

    def _need(self, band, notional):
        tr, ca = self.projected_state(band, notional)

        td = (
            self.trade_targets[band] - tr[band]
        ) / max(self.trade_targets[band], 1e-9)

        cd = (
            self.capital_targets[band] - ca[band]
        ) / max(self.capital_targets[band], 1e-9)

        return 2.0 * td + 2.0 * cd

    def choose_band(self, candidates, allow_over_quota=False):
        del allow_over_quota

        if not candidates:
            return None

        by = {}

        for c in candidates:
            band = c.get("band")

            if band in self.trade_targets:
                by.setdefault(band, []).append(c)

        if not by:
            return None

        scored = []

        for band, rows in by.items():
            # Candidate size matters because the capital target is part of the
            # observed behavior. Do not inspect CLOB executability here.
            row = max(
                rows,
                key=lambda x: self._need(
                    band,
                    float(x.get("target", 0.0)),
                ),
            )

            scored.append(
                (
                    self._need(
                        band,
                        float(row.get("target", 0.0)),
                    ),
                    -BAND_INDEX.get(band, 999),
                    band,
                )
            )

        scored.sort(reverse=True)

        return scored[0][2]

    def shares(self):
        nt = sum(self.trade_counts.values())
        nc = sum(self.capital.values())

        return {
            "trade": {
                b: self.trade_counts[b] / nt if nt else 0
                for b in self.bands
            },
            "capital": {
                b: self.capital[b] / nc if nc else 0
                for b in self.bands
            },
        }

    def target_report(self):
        a = self.shares()

        return {
            b: {
                "target_trade_share": self.trade_targets[b],
                "actual_trade_share": a["trade"][b],
                "target_capital_share": self.capital_targets[b],
                "actual_capital_share": a["capital"][b],
            }
            for b in self.bands
        }


class CapitalFirstStrategy:
    VERSION = "V19_RAW_BEHAVIORAL_CLOB_REPLICA"
    DATA_FILE = Path(__file__).with_name("trader_behavior.json")
    BANDS = BANDS
    HARD_CUTOFF = 60.0

    # Full-scale entry-count sizing direction from the 778,116-trade analysis.
    # Ratios are applied to each fine band's first-entry empirical median so
    # price-band sizing is retained while entry-count direction is corrected.
    ENTRY_POSITION_RATIOS = {
        "CHEAP": {
            "first": 1.0,
            "2nd-3rd": 0.7241379,
            "4th+": 0.3793103,
        },
        "MID": {
            "first": 1.0,
            "2nd-3rd": 1.0,
            "4th+": 0.9405941,
        },
        "CORE": {
            "first": 1.0,
            "2nd-3rd": 0.9051282,
            "4th+": 0.5641026,
        },
        "HIGH": {
            "first": 1.0,
            "2nd-3rd": 1.0810811,
            "4th+": 0.6752252,
        },
    }

    def __init__(
        self,
        bankroll=1000,
        start_sec=0,
        stop_sec=240,
        hard_cutoff_seconds=60,
        max_total_exposure=300,
        min_trade_gap_seconds=0,
        behavior_file=None,
        seed=20260831,
        **_,
    ):
        self.bankroll = float(bankroll)
        self.start_sec = max(0.0, float(start_sec))
        self.stop_sec = min(300.0, float(stop_sec))
        self.hard_cutoff_seconds = max(
            60.0,
            float(hard_cutoff_seconds),
        )
        self.max_total_exposure = max(
            0.0,
            float(max_total_exposure),
        )
        self.min_trade_gap_seconds = max(
            0.0,
            float(min_trade_gap_seconds),
        )
        self._last_trade_at: Optional[float] = None

        path = Path(behavior_file) if behavior_file else self.DATA_FILE

        with path.open(encoding="utf-8") as f:
            self.behavior = json.load(f)

        self.notional_scale = float(
            self.behavior.get("notional_scale", 0.4)
        )

        self.process = EmpiricalTraderProcess(
            self.behavior,
            seed=seed,
        )

        self.cadence = self.process

        self.scheduler = TraderPolicyScheduler(
            self.behavior,
            seed=seed,
        )

        self.fine_band_trade_share = {
            str(x["fine_band"]): float(x["trade_share"])
            for x in self.behavior.get("fine_bands", [])
        }

        self.entry_medians = self.behavior[
            "entry_median_by_fine_band"
        ]

        self.band_size_multiplier = (
            self._derive_band_size_multipliers()
        )

    def _derive_band_size_multipliers(self):
        """Calibrate each fine-band size curve to the trader's observed
        aggregate dollars while preserving the configured notional scale.

        Entry-number medians describe the shape of sizing, while the raw
        band totals provide the trustworthy aggregate dollar target. The
        multiplier bridges those two measurements without inventing a new
        cross-band sizing rule.
        """
        stats_by_band = self.behavior.get(
            "entry_stats_by_fine_band",
            {},
        )

        rows = []

        for x in self.behavior.get("fine_bands", []):
            band = str(x["fine_band"])
            stats = stats_by_band.get(band, {})

            total_n = sum(
                int(v.get("n", 0))
                for v in stats.values()
            )

            if total_n <= 0:
                rows.append(
                    (
                        band,
                        float(x["trade_share"]),
                        1.0,
                        1.0,
                    )
                )
                continue

            model_avg = sum(
                int(v.get("n", 0))
                * float(
                    v.get(
                        "scaled_median_notional",
                        0.0,
                    )
                )
                for v in stats.values()
            ) / total_n

            target_avg = (
                float(x["notional"])
                / float(x["trades"])
            ) * self.notional_scale

            ratio = (
                target_avg / model_avg
                if model_avg > 0
                else 1.0
            )

            rows.append(
                (
                    band,
                    float(x["trade_share"]),
                    model_avg,
                    ratio,
                )
            )

        base_total = sum(
            trade_share * avg
            for _, trade_share, avg, _ in rows
        )

        weighted_total = sum(
            trade_share * avg * ratio
            for _, trade_share, avg, ratio in rows
        )

        common = (
            base_total / weighted_total
            if weighted_total > 0
            else 1.0
        )

        return {
            band: ratio * common
            for band, _, _, ratio in rows
        }

    def entry_expected_band_target(self, band):
        """Expected 40%-scale notional for a representative entry in a band."""
        stats = self.behavior.get(
            "entry_stats_by_fine_band",
            {},
        ).get(str(band), {})

        total_n = sum(
            int(v.get("n", 0))
            for v in stats.values()
        )

        if total_n <= 0:
            return 0.0

        base = sum(
            int(v.get("n", 0))
            * float(
                v.get(
                    "scaled_median_notional",
                    0.0,
                )
            )
            for v in stats.values()
        ) / total_n

        return (
            base
            * self.band_size_multiplier.get(
                str(band),
                1.0,
            )
        )

    @classmethod
    def fine_band(cls, price):
        p = float(price)

        for band, lo, hi, regime in cls.BANDS:
            if lo <= p < hi:
                return band, regime

        if p == 1.0:
            return "H95_100", "HIGH"

        return None, None

    def entry_target(
        self,
        price,
        market="BTC",
        entry_count=0,
    ):
        del market

        band, _ = self.fine_band(price)

        if not band:
            return 0.0

        lookup = self.entry_medians.get(
            band,
            {},
        )

        first = float(
            lookup.get("1", 0.0)
        )

        if first <= 0:
            return 0.0

        _, regime = self.fine_band(price)

        n = int(entry_count)

        position = (
            "first"
            if n == 0
            else (
                "2nd-3rd"
                if n <= 2
                else "4th+"
            )
        )

        ratio = self.ENTRY_POSITION_RATIOS[
            regime
        ][position]

        # Keep V16's empirical band calibration, but correct the entry-count
        # direction using the full-scale trader-history ratios.
        multiplier = self.band_size_multiplier.get(
            band,
            1.0,
        )

        return max(
            0.10,
            first * ratio * multiplier,
        )

    capital_target = entry_target

    @staticmethod
    def _points(history):
        out = []

        for item in history or []:
            try:
                if isinstance(item, dict):
                    ts = float(item["ts"])
                    price = float(
                        item.get(
                            "best_bid",
                            item.get("mid"),
                        )
                    )
                else:
                    ts, price = (
                        float(item[0]),
                        float(item[1]),
                    )

                if 0.0 < price < 1.0:
                    out.append((ts, price))

            except (
                TypeError,
                ValueError,
                KeyError,
                IndexError,
            ):
                continue

        return sorted(out)

    @classmethod
    def movement(cls, price, history, now):
        points = cls._points(history)
        result = {}

        for seconds in (1, 3, 5, 10, 30):
            previous = [
                p
                for ts, p in points
                if ts <= float(now) - seconds
            ]

            result[f"m{seconds}"] = (
                float(price) - previous[-1]
                if previous
                else 0.0
            )

        return result

    @staticmethod
    def _trajectory_class(delta):
        if delta > TRAJECTORY_THRESHOLD:
            return "rising"

        if delta < -TRAJECTORY_THRESHOLD:
            return "falling"

        return "flat"

    def _candidate(
        self,
        market,
        side,
        bid,
        ask,
        depth,
        history,
        now,
        thesis_side,
        entries,
        burst_age,
    ):
        if bid is None:
            return None

        try:
            bid = float(bid)
            ask = (
                None
                if ask is None
                else float(ask)
            )
            depth = (
                None
                if depth is None
                else float(depth)
            )

        except (TypeError, ValueError):
            return None

        if not 0.0 < bid < 1.0:
            return None

        if ask is not None and not (
            0.0 < ask <= 1.0
        ):
            return None

        if ask is not None and ask < bid:
            return None

        band, regime = self.fine_band(bid)

        if not regime:
            return None

        mv = self.movement(
            bid,
            history,
            now,
        )

        trajectory = self._trajectory_class(
            mv["m5"]
        )

        trajectory_share = (
            TRAJECTORY_SHARE[regime][trajectory]
        )

        target = self.entry_target(
            bid,
            market,
            entries,
        )

        return {
            "side": side,
            "bid": bid,
            "ask": ask,
            "depth": depth,
            "band": band,
            "regime": regime,
            "trajectory": trajectory,
            "trajectory_likelihood": trajectory_share,
            "band_prior": self.fine_band_trade_share.get(
                band,
                0.0,
            ),
            "same_side": bool(
                thesis_side
                and side == thesis_side
            ),
            "target": target,
            "movement": mv,
            "entries": int(entries),
            "burst_age": float(burst_age),
            "reason": (
                f"{self.VERSION} "
                f"target_band={band} "
                f"band={band} "
                f"regime={regime} "
                f"trajectory={trajectory} "
                f"band_share="
                f"{self.fine_band_trade_share.get(band, 0.0):.6f} "
                f"trajectory_share="
                f"{trajectory_share:.3f} "
                f"same_side="
                f"{bool(thesis_side and side == thesis_side)} "
                f"behavioral_target=${target:.2f} "
                f"entry_count={int(entries)} "
                f"burst_age={float(burst_age):.1f}s "
                f"bid={bid:.4f} "
                f"ask="
                f"{ask if ask is not None else 0.0:.4f} "
                f"depth="
                f"{depth if depth is not None else 0.0:.2f} "
                f"m1={mv['m1']:+.4f} "
                f"m3={mv['m3']:+.4f} "
                f"m5={mv['m5']:+.4f} "
                f"m10={mv['m10']:+.4f} "
                f"m30={mv['m30']:+.4f}"
            ),
        }

    def build_candidates_for_market(
        self,
        elapsed,
        up_ask,
        down_ask,
        up_bid,
        down_bid,
        up_history,
        down_history,
        now,
        asset=None,
        market=None,
        thesis_side=None,
        market_entry_count=0,
        seconds_since_first_entry=0,
        up_depth=0,
        down_depth=0,
        up_ask_depth=None,
        down_ask_depth=None,
    ):
        """Return all currently eligible side candidates for one market.

        Side persistence is sampled once for this market's scheduled decision,
        then the global scheduler chooses the fine band across all markets.
        This prevents per-market band sampling from distorting the trader's
        global distribution.
        """
        elapsed = float(elapsed)
        now = float(now)

        if (
            elapsed < self.start_sec
            or elapsed >= self.stop_sec
        ):
            return []

        if (
            self.stop_sec - elapsed
            <= self.hard_cutoff_seconds
        ):
            return []

        m = str(
            market or asset or "BTC"
        ).upper()

        # Immediate taker selection should rank executable liquidity using ask
        # depth. Keep bid depth as a fallback for legacy callers/tests.
        up_exec_depth = (
            up_ask_depth
            if up_ask_depth is not None
            else up_depth
        )

        down_exec_depth = (
            down_ask_depth
            if down_ask_depth is not None
            else down_depth
        )

        candidates = [
            c
            for c in (
                self._candidate(
                    m,
                    "Up",
                    up_bid,
                    up_ask,
                    up_exec_depth,
                    up_history,
                    now,
                    thesis_side,
                    market_entry_count,
                    seconds_since_first_entry,
                ),
                self._candidate(
                    m,
                    "Down",
                    down_bid,
                    down_ask,
                    down_exec_depth,
                    down_history,
                    now,
                    thesis_side,
                    market_entry_count,
                    seconds_since_first_entry,
                ),
            )
            if c is not None
        ]

        if (
            not thesis_side
            or len(candidates) <= 1
        ):
            return candidates

        same = [
            c
            for c in candidates
            if c["side"] == thesis_side
        ]

        flip = [
            c
            for c in candidates
            if c["side"] != thesis_side
        ]

        if self.process.should_continue_side():
            return same or candidates

        return flip or candidates

    def choose_process_candidate(
        self,
        candidates,
        target_band=None,
        thesis_side=None,
    ):
        if not candidates:
            return None

        if target_band is None:
            target_band = self.scheduler.choose_band(
                candidates
            )

        if target_band is None:
            return None

        # STRICT: no fallback to a different fine price band.
        targeted = [
            c
            for c in candidates
            if c["band"] == target_band
        ]

        if not targeted:
            return None

        return min(
            targeted,
            key=lambda c: (
                -c["trajectory_likelihood"],
                c["bid"],
            ),
        )

    def choose_distribution_band(
        self,
        candidates,
    ):
        return self.scheduler.choose_band(
            candidates
        )

    def restore_policy_state(self, trades):
        self.scheduler.restore(
            trades,
            self.fine_band,
        )

    def observe_trade_distribution(
        self,
        band,
        notional,
    ):
        self.scheduler.observe(
            band,
            notional,
        )

    def distribution_snapshot(self):
        return self.scheduler.shares()

    def distribution_report(self):
        return self.scheduler.target_report()

    def sample_target_band(self):
        return self.process.sample_target_band()

    def sample_delay(self):
        return self.process.sample_gap()

    def decide(
        self,
        elapsed,
        up_ask,
        down_ask,
        up_bid,
        down_bid,
        up_history,
        down_history,
        current_exposure,
        available_cash,
        up_depth=0,
        down_depth=0,
        now=None,
        asset_exposure=0,
        total_exposure=0,
        market_entry_count=0,
        seconds_since_first_entry=0,
        thesis_side=None,
        thesis_price=None,
        asset=None,
        market=None,
        process_target_band=None,
    ):
        del (
            current_exposure,
            asset_exposure,
            thesis_price,
        )

        now = (
            time.time()
            if now is None
            else float(now)
        )

        candidates = self.build_candidates_for_market(
            elapsed,
            up_ask,
            down_ask,
            up_bid,
            down_bid,
            up_history,
            down_history,
            now,
            asset=asset,
            market=market,
            thesis_side=thesis_side,
            market_entry_count=market_entry_count,
            seconds_since_first_entry=seconds_since_first_entry,
            up_depth=up_depth,
            down_depth=down_depth,
        )

        if not candidates:
            return None

        target_band = (
            process_target_band
            or self.scheduler.choose_band(
                candidates
            )
        )

        best = self.choose_process_candidate(
            candidates,
            target_band,
        )

        if best is None:
            return None

        remaining = max(
            0.0,
            self.max_total_exposure
            - float(total_exposure),
        )

        target = float(best["target"])

        notion = min(
            target,
            max(0.0, float(available_cash)),
            remaining,
        )

        if notion < 0.10:
            return None

        self._last_trade_at = now

        mv = best["movement"]

        reason = (
            f"{self.VERSION} "
            f"target_band={target_band} "
            f"band={best['band']} "
            f"regime={best['regime']} "
            f"trajectory={best['trajectory']} "
            f"band_share={best['band_prior']:.6f} "
            f"trajectory_share="
            f"{best['trajectory_likelihood']:.3f} "
            f"same_side={best['same_side']} "
            f"behavioral_target=${target:.2f} "
            f"entry_count={market_entry_count} "
            f"burst_age="
            f"{float(seconds_since_first_entry):.1f}s "
            f"bid={best['bid']:.4f} "
            f"ask="
            f"{best['ask'] if best['ask'] is not None else 0:.4f} "
            f"depth="
            f"{best['depth'] if best['depth'] is not None else 0:.2f} "
            f"m1={mv['m1']:+.4f} "
            f"m3={mv['m3']:+.4f} "
            f"m5={mv['m5']:+.4f} "
            f"m10={mv['m10']:+.4f} "
            f"m30={mv['m30']:+.4f} "
            f"elapsed={float(elapsed):.1f}s "
            f"left={self.stop_sec-float(elapsed):.1f}s"
        )

        return Signal(
            best["side"],
            best["bid"],
            best["trajectory_likelihood"],
            round(notion, 2),
            reason,
        )

    def size(
        self,
        price,
        regime=None,
        market="BTC",
        entry_count=0,
        **_,
    ):
        del regime

        return self.entry_target(
            price,
            market,
            entry_count,
        )
