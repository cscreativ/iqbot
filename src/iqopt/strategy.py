"""Strategie de trading basee sur les formations de bougies.

Prend les detections du module `patterns`, filtre par seuil de force et par
liste blanche, et renvoie une decision unique BUY / SELL (ou None).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

from .patterns import (
    BUY,
    NONE,
    SELL,
    Bar,
    Hit,
    PatternConfig,
    detect,
    to_bars,
)


@dataclass
class Decision:
    direction: str
    strength: float
    pattern: str
    note: str = ""


class PatternStrategy:
    """Choisit la figure directionnelle la plus forte au-dessus du seuil.

    - `min_strength` : force minimale (0..1) pour trader.
    - `allowed`      : noms de figures autorisees (None = toutes).
    - `require_trend`: si True, n'accepte que les signaux dans le sens de la
      tendance des bougies precedentes (filtre momentum).
    """

    def __init__(
        self,
        config: Optional[PatternConfig] = None,
        min_strength: float = 0.5,
        allowed: Optional[Iterable[str]] = None,
        require_trend: bool = False,
        trend_lookback: int = 5,
    ) -> None:
        self.config = config or PatternConfig()
        self.min_strength = float(min_strength)
        self.allowed = set(allowed) if allowed else None
        self.require_trend = require_trend
        self.trend_lookback = int(trend_lookback)

    def _trend_ok(self, bars: Sequence[Bar], direction: str) -> bool:
        if not self.require_trend:
            return True
        if len(bars) < self.trend_lookback + 1:
            return False
        start = bars[-1 - self.trend_lookback].close
        end = bars[-1].close
        if start == 0:
            return False
        change = (end - start) / abs(start)
        if direction == BUY:
            return change > 0
        if direction == SELL:
            return change < 0
        return False

    def evaluate(self, bars: Sequence[Bar]) -> Optional[Decision]:
        bars = list(bars)
        hits = detect(bars, self.config)
        candidates: List[Hit] = [h for h in hits if h.direction in (BUY, SELL)]
        if self.allowed is not None:
            candidates = [h for h in candidates if h.name in self.allowed]
        candidates = [h for h in candidates if h.strength >= self.min_strength]
        candidates = [h for h in candidates if self._trend_ok(bars, h.direction)]
        if not candidates:
            return None

        candidates.sort(key=lambda h: h.strength, reverse=True)
        best = candidates[0]
        if (
            len(candidates) > 1
            and abs(candidates[1].strength - best.strength) < 1e-9
            and candidates[1].direction != best.direction
        ):
            # Force egale dans deux sens opposes -> ambigu, on ne trade pas.
            return None
        return Decision(best.direction, best.strength, best.name, best.note)

    def evaluate_candles(self, candles: Iterable) -> Optional[Decision]:
        candles = list(candles)
        if not candles:
            return None
        return self.evaluate(to_bars(candles))
