"""Portage Python de l'indicateur Pine 'AGS M5 PRECISION SCALPING V3.1'.

Logique :
- 'setup' = bougie M5 precedente avec meche longue (ratio) et corps >= ATR*min.
- signal = sur la bougie M5 EN COURS, dans les `observe_sec` premieres secondes :
  * BUY  (setup haussier) : pression baissiere (prix < open) PUIS reprise au-dessus (prix > open)
  * SELL (setup baissier) : pression haussiere (prix > open) PUIS reprise en-dessous (prix < open)
- filtre M15 optionnel (EMA).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence


@dataclass
class AGSConfig:
    wick_ratio: float = 2.0
    min_body_atr: float = 0.15
    atr_length: int = 14
    observe_sec: int = 45
    use_m15: bool = False
    ema_len: int = 50


def _bar(c: Any):
    if isinstance(c, dict):
        return float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    return float(c.open), float(c.high), float(c.low), float(c.close)


def _atr(closed: Sequence[Any], length: int) -> float:
    """ATR approximé (moyenne simple des True Range sur les bougies cloturées)."""
    if not closed:
        return 0.0
    trs: List[float] = []
    prev_close: Optional[float] = None
    for c in closed:
        o, h, l, cl = _bar(c)
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = cl
    tail = trs[-length:] if length > 0 else trs
    return sum(tail) / len(tail) if tail else 0.0


def setup(candles: Sequence[Any], cfg: AGSConfig) -> str:
    """Direction du setup d'apres la bougie M5 PRECEDENTE (l'avant-derniere)."""
    if len(candles) < 3:
        return ""
    prev = candles[-2]  # bougie cloturée précédente
    o, h, l, c = _bar(prev)
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    atr = _atr(list(candles[:-1]), cfg.atr_length)
    min_body = atr * cfg.min_body_atr

    if c > o and upper >= max(body, 1e-9) * cfg.wick_ratio and body >= min_body:
        return "BUY"
    if c < o and lower >= max(body, 1e-9) * cfg.wick_ratio and body >= min_body:
        return "SELL"
    return ""


def rebound_signal(
    direction: str,
    candle_open: float,
    ticks: Iterable[Dict[str, float]],
    candle_ts: float,
    observe_sec: int,
) -> bool:
    """Pression initiale puis retournement, dans la fenetre observe_sec."""
    seen_pressure = False
    for t in ticks:
        try:
            ts = float(t["ts"])
            price = float(t["price"])
        except (KeyError, TypeError, ValueError):
            continue
        elapsed = ts - candle_ts
        if elapsed < 0 or elapsed > observe_sec:
            continue
        if direction == "BUY":
            if not seen_pressure and price < candle_open:
                seen_pressure = True
            elif seen_pressure and price > candle_open:
                return True
        else:  # SELL
            if not seen_pressure and price > candle_open:
                seen_pressure = True
            elif seen_pressure and price < candle_open:
                return True
    return False


def _ema(values: List[float], length: int) -> Optional[float]:
    if not values or length <= 0:
        return None
    k = 2.0 / (length + 1.0)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


def m15_filter(candles: Sequence[Any], cfg: AGSConfig, direction: str) -> bool:
    """True si le filtre M15 autorise le sens (close M15 vs EMA)."""
    if not cfg.use_m15:
        return True
    if not candles:
        return True
    # agrege les bougies 300s en 15 min (3 x 300s)
    buckets: Dict[int, float] = {}
    for c in candles:
        try:
            ts = float(c["ts"]) if isinstance(c, dict) else float(c.ts)
            cl = float(c["close"]) if isinstance(c, dict) else float(c.close)
        except (KeyError, TypeError, ValueError):
            continue
        b = int(ts // 900) * 900
        buckets[b] = cl
    closes = [buckets[k] for k in sorted(buckets)]
    if len(closes) < 2:
        return True
    ema = _ema(closes[:-1], cfg.ema_len)  # EMA sur bougies 15m completes
    if ema is None:
        return True
    last = closes[-1]
    return (last > ema) if direction == "BUY" else (last < ema)
