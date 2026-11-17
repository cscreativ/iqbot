from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from iqopt import patterns as P  # noqa: E402
from iqopt.patterns import Bar, PatternConfig, detect, detect_candles  # noqa: E402
from iqopt.strategy import PatternStrategy  # noqa: E402


def bar(o: float, h: float, l: float, c: float) -> dict:
    return {"open": o, "high": h, "low": l, "close": c}


def names(hits) -> set:
    return {h.name for h in hits}


def directional(hits):
    return [h for h in hits if h.direction in (P.BUY, P.SELL)]


class PatternDetectionTests(unittest.TestCase):
    def test_doji(self):
        hits = detect([bar(1.10, 1.12, 1.08, 1.10)])
        self.assertIn("doji", names(hits))
        self.assertEqual(directional(hits), [])

    def test_hammer_after_downtrend(self):
        bars = [
            bar(1.21, 1.215, 1.195, 1.20),
            bar(1.19, 1.195, 1.175, 1.18),
            bar(1.17, 1.175, 1.155, 1.16),
            bar(1.15, 1.155, 1.135, 1.14),
            bar(1.13, 1.136, 1.10, 1.135),
        ]
        hits = detect(bars)
        self.assertIn("hammer", names(hits))
        self.assertEqual(hits[0].direction, P.BUY)

    def test_hammer_requires_downtrend(self):
        bars = [
            bar(1.10, 1.105, 1.09, 1.10),
            bar(1.11, 1.115, 1.10, 1.11),
            bar(1.12, 1.125, 1.11, 1.12),
            bar(1.13, 1.135, 1.12, 1.13),
            bar(1.14, 1.146, 1.11, 1.145),  # meche basse mais contexte haussier
        ]
        self.assertNotIn("hammer", names(detect(bars)))

    def test_shooting_star_after_uptrend(self):
        bars = [
            bar(1.10, 1.106, 1.095, 1.105),
            bar(1.116, 1.121, 1.111, 1.12),
            bar(1.131, 1.136, 1.126, 1.135),
            bar(1.146, 1.151, 1.141, 1.15),
            bar(1.17, 1.20, 1.1689, 1.169),
        ]
        hits = detect(bars)
        self.assertIn("shooting_star", names(hits))
        shooting = [h for h in hits if h.name == "shooting_star"][0]
        self.assertEqual(shooting.direction, P.SELL)

    def test_bullish_engulfing(self):
        bars = [bar(1.10, 1.105, 1.045, 1.05), bar(1.04, 1.115, 1.035, 1.11)]
        hits = detect(bars)
        self.assertIn("bullish_engulfing", names(hits))
        eng = [h for h in hits if h.name == "bullish_engulfing"][0]
        self.assertEqual(eng.direction, P.BUY)

    def test_bearish_engulfing(self):
        bars = [bar(1.05, 1.105, 1.045, 1.10), bar(1.11, 1.115, 1.035, 1.04)]
        hits = detect(bars)
        self.assertEqual(names(hits), {"bearish_engulfing"})
        self.assertEqual(hits[0].direction, P.SELL)

    def test_morning_star(self):
        bars = [
            bar(1.20, 1.205, 1.135, 1.14),
            bar(1.135, 1.138, 1.128, 1.132),
            bar(1.135, 1.195, 1.13, 1.19),
        ]
        hits = detect(bars)
        self.assertIn("morning_star", names(hits))
        morning = [h for h in hits if h.name == "morning_star"][0]
        self.assertEqual(morning.direction, P.BUY)

    def test_three_white_soldiers(self):
        bars = [
            bar(1.10, 1.132, 1.098, 1.13),
            bar(1.13, 1.162, 1.128, 1.16),
            bar(1.16, 1.192, 1.158, 1.19),
        ]
        hits = detect(bars)
        self.assertEqual(names(hits), {"three_white_soldiers"})
        self.assertEqual(hits[0].direction, P.BUY)

    def test_three_black_crows(self):
        bars = [
            bar(1.19, 1.192, 1.158, 1.16),
            bar(1.16, 1.162, 1.128, 1.13),
            bar(1.13, 1.132, 1.098, 1.10),
        ]
        hits = detect(bars)
        self.assertEqual(names(hits), {"three_black_crows"})
        self.assertEqual(hits[0].direction, P.SELL)

    def test_short_input_is_safe(self):
        self.assertEqual(detect([]), [])
        self.assertIsInstance(detect([bar(1.0, 1.0, 1.0, 1.0)]), list)

    def test_accepts_bar_dataclasses(self):
        hits = detect_candles([Bar(1.04, 1.115, 1.035, 1.11)])
        self.assertIsInstance(hits, list)


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.engulf = [bar(1.10, 1.105, 1.045, 1.05), bar(1.04, 1.115, 1.035, 1.11)]

    def test_buys_on_engulfing(self):
        decision = PatternStrategy().evaluate_candles(self.engulf)
        self.assertIsNotNone(decision)
        self.assertEqual(decision.direction, P.BUY)
        self.assertEqual(decision.pattern, "bullish_engulfing")

    def test_min_strength_blocks(self):
        strict = PatternStrategy(min_strength=0.99)
        self.assertIsNone(strict.evaluate_candles(self.engulf))

    def test_allowed_filter(self):
        only_hammer = PatternStrategy(allowed=["hammer"])
        self.assertIsNone(only_hammer.evaluate_candles(self.engulf))
        only_engulf = PatternStrategy(allowed=["bullish_engulfing"])
        decision = only_engulf.evaluate_candles(self.engulf)
        self.assertIsNotNone(decision)

    def test_doji_is_not_a_trade(self):
        doji = [bar(1.10, 1.12, 1.08, 1.10)]
        self.assertIsNone(PatternStrategy().evaluate_candles(doji))


if __name__ == "__main__":
    unittest.main()
