"""Detection de formations de bougies japonaises (pur, testable, sans I/O).

Chaque detecteur travaille sur la DERNIERE bougie de la sequence fournie
(ou les N dernieres pour les figures a 2-3 bougies). Le bot appelle
`detect()` sur des bougies CLOTUREES pour decider BUY / SELL / NONE.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

BUY = "BUY"
SELL = "SELL"
NONE = "NONE"

_EPS = 1e-12

CandleLike = Union[Dict[str, Any], Any]


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


@dataclass(frozen=True)
class Bar:
    open: float
    high: float
    low: float
    close: float

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def range(self) -> float:
        return max(_EPS, self.high - self.low)

    @property
    def upper(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def bullish(self) -> bool:
        return self.close > self.open

    @property
    def bearish(self) -> bool:
        return self.close < self.open

    @property
    def mid(self) -> float:
        return (self.open + self.close) / 2.0


@dataclass(frozen=True)
class Hit:
    name: str
    direction: str
    strength: float
    bars: int
    note: str = ""


@dataclass(frozen=True)
class PatternConfig:
    doji_body_ratio: float = 0.10
    long_shadow_ratio: float = 2.0
    small_shadow_ratio: float = 0.5
    body_zone: float = 0.6
    engulf_min_body_ratio: float = 1.0
    harami_max_body_ratio: float = 0.6
    double_wick_ratio: float = 0.6
    long_leg_range_ratio: float = 0.20
    star_small_body_ratio: float = 0.5
    soldier_body_ratio: float = 0.5
    trend_lookback: int = 3
    trend_threshold: float = 0.0


def _field(candle: CandleLike, name: str) -> float:
    if isinstance(candle, dict):
        return float(candle[name])
    return float(getattr(candle, name))


def to_bars(candles: Sequence[CandleLike]) -> List[Bar]:
    return [
        Bar(
            _field(c, "open"),
            _field(c, "high"),
            _field(c, "low"),
            _field(c, "close"),
        )
        for c in candles
    ]


def _prior_trend(bars: Sequence[Bar], lookback: int, threshold: float) -> int:
    """+1 hausse, -1 baisse, 0 neutre, mesure sur les bougies AVANT la figure."""
    if len(bars) < lookback + 1:
        return 0
    start = bars[-1 - lookback].close
    end = bars[-1].close
    if start == 0:
        return 0
    change = (end - start) / abs(start)
    if change > threshold:
        return 1
    if change < -threshold:
        return -1
    return 0


# --------------------------------------------------------------------------
# Figures 1 bougie
# --------------------------------------------------------------------------
def detect_doji(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    b = bars[-1]
    rng = b.range
    if b.body > cfg.doji_body_ratio * rng:
        return []
    return [Hit("doji", NONE, _clamp(1.0 - b.body / rng), 1, "corps minuscule (indecision)")]


def detect_hammer(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    if len(bars) < 2:
        return []
    b = bars[-1]
    body = b.body
    if b.lower < cfg.long_shadow_ratio * max(body, _EPS):
        return []
    if b.upper > cfg.small_shadow_ratio * max(body, _EPS):
        return []
    if min(b.open, b.close) < b.low + cfg.body_zone * b.range:
        return []
    if _prior_trend(bars[:-1], cfg.trend_lookback, cfg.trend_threshold) >= 0:
        return []
    strength = _clamp(0.4 + 0.6 * (b.lower / b.range))
    return [Hit("hammer", BUY, strength, 1, "meche basse longue apres baisse")]


def detect_shooting_star(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    if len(bars) < 2:
        return []
    b = bars[-1]
    body = b.body
    if b.upper < cfg.long_shadow_ratio * max(body, _EPS):
        return []
    if b.lower > cfg.small_shadow_ratio * max(body, _EPS):
        return []
    if max(b.open, b.close) > b.high - cfg.body_zone * b.range:
        return []
    if _prior_trend(bars[:-1], cfg.trend_lookback, cfg.trend_threshold) <= 0:
        return []
    strength = _clamp(0.4 + 0.6 * (b.upper / b.range))
    return [Hit("shooting_star", SELL, strength, 1, "meche haute longue apres hausse")]


# --------------------------------------------------------------------------
# Figures 2 bougies
# --------------------------------------------------------------------------
def detect_engulfing(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    if len(bars) < 2:
        return []
    prev, cur = bars[-2], bars[-1]
    out: List[Hit] = []
    if prev.bearish and cur.bullish:
        if (
            cur.open <= prev.close
            and cur.close >= prev.open
            and cur.body >= cfg.engulf_min_body_ratio * prev.body
        ):
            dom = cur.body / (cur.body + prev.body + _EPS)
            out.append(
                Hit(
                    "bullish_engulfing",
                    BUY,
                    _clamp(0.5 + (dom - 0.5)),
                    2,
                    "avalement haussier",
                )
            )
    if prev.bullish and cur.bearish:
        if (
            cur.open >= prev.close
            and cur.close <= prev.open
            and cur.body >= cfg.engulf_min_body_ratio * prev.body
        ):
            dom = cur.body / (cur.body + prev.body + _EPS)
            out.append(
                Hit(
                    "bearish_engulfing",
                    SELL,
                    _clamp(0.5 + (dom - 0.5)),
                    2,
                    "avalement baissier",
                )
            )
    return out


def detect_harami(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    if len(bars) < 2:
        return []
    prev, cur = bars[-2], bars[-1]
    if prev.body <= 0:
        return []
    out: List[Hit] = []
    small = cur.body <= cfg.harami_max_body_ratio * prev.body
    if prev.bearish and cur.bullish and small:
        if cur.open >= prev.close and cur.close <= prev.open:
            strength = _clamp(0.45 + 0.2 * (1.0 - cur.body / prev.body))
            out.append(Hit("bullish_harami", BUY, strength, 2, "harami haussier"))
    if prev.bullish and cur.bearish and small:
        if cur.open <= prev.close and cur.close >= prev.open:
            strength = _clamp(0.45 + 0.2 * (1.0 - cur.body / prev.body))
            out.append(Hit("bearish_harami", SELL, strength, 2, "harami baissier"))
    return out


def detect_bullish_inside(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Deux bougies VERTES ; la 2e PLUS PETITE avec une LONGUE MECHE BASSE.

    Regle utilisateur : a la cloture de la 2e bougie -> SELL (retournement baissier).
    Le sens de la meche compte : meche BASSE dominante.
    """
    if len(bars) < 2:
        return []
    prev, cur = bars[-2], bars[-1]
    if not (prev.bullish and cur.bullish):
        return []
    if prev.body <= 0 or cur.body <= 0 or cur.range <= 0:
        return []
    # 2e bougie plus petite que la 1re.
    if cur.body > prev.body:
        return []
    # Longue meche BASSE et dominante (sens de la meche).
    if cur.lower < cfg.long_shadow_ratio * max(cur.body, _EPS):
        return []
    if cur.lower <= cur.upper:
        return []
    strength = _clamp(0.45 + 0.35 * (cur.lower / cur.range))
    return [
        Hit(
            "bullish_inside",
            SELL,
            strength,
            2,
            "2 vertes, 2e plus petite + longue meche BASSE -> SELL",
        )
    ]


def detect_bullish_big_lower(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Bougie ROUGE suivie d'une plus GRANDE bougie VERTE dont le corps englobe
    le corps de la rouge -> a la cloture de la 2e : SELL.

    Pas d'exigence de meche : seule l'engloutissement du corps compte.
    """
    if len(bars) < 2:
        return []
    prev, cur = bars[-2], bars[-1]
    if not (prev.bearish and cur.bullish):
        return []
    if prev.body <= 0 or cur.body <= 0:
        return []
    # La 2e verte doit etre plus GRANDE que la rouge.
    if cur.body <= prev.body:
        return []
    # Le corps de la verte englobe le corps de la rouge.
    if not (cur.open <= prev.close and cur.close >= prev.open):
        return []
    strength = 0.40
    return [
        Hit(
            "bullish_big_lower",
            SELL,
            strength,
            2,
            "rouge + plus grande verte qui englobe son corps -> SELL",
        )
    ]


def detect_green_upper_pin(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Bougie VERTE avec LONGUE meche haute (pin/etoile filante) -> a sa cloture : SELL.

    Retournement de marche vert vers la baisse. Ne s'applique PAS apres une
    bougie rouge (ce cas = inverted hammer -> BUY, voir detect_inverted_hammer).
    """
    b = bars[-1]
    if not b.bullish:
        return []
    if len(bars) >= 2 and bars[-2].bearish:
        return []
    if b.body <= 0 or b.range <= 0:
        return []
    if b.upper < cfg.long_shadow_ratio * max(b.body, _EPS):
        return []
    # Sens de meche : la meche HAUTE doit dominer la meche basse.
    if b.upper <= b.lower:
        return []
    strength = _clamp(0.45 + 0.35 * (b.upper / b.range))
    return [
        Hit(
            "green_upper_pin",
            SELL,
            strength,
            1,
            "bougie verte + longue meche haute -> SELL",
        )
    ]


def detect_inverted_hammer(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Bougie ROUGE suivie d'une bougie VERTE plus petite a LONGUE meche HAUTE
    dominante (inverted hammer) -> a la cloture de la 2e : BUY.
    """
    if len(bars) < 2:
        return []
    prev, cur = bars[-2], bars[-1]
    if not (prev.bearish and cur.bullish):
        return []
    if prev.body <= 0 or cur.body <= 0 or cur.range <= 0:
        return []
    if cur.body > prev.body:
        return []
    # Longue meche HAUTE dominante (sens de la meche).
    if cur.upper < cfg.long_shadow_ratio * max(cur.body, _EPS):
        return []
    if cur.upper <= cur.lower:
        return []
    strength = _clamp(0.45 + 0.35 * (cur.upper / cur.range))
    return [
        Hit(
            "inverted_hammer",
            BUY,
            strength,
            2,
            "rouge + verte a longue meche HAUTE (inverted hammer) -> BUY",
        )
    ]


def detect_bearish_hammer(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Bougie ROUGE precedente + 2e bougie (verte OU rouge) a petit corps avec
    LONGUE meche basse (marteau) -> a la cloture de la 2e : BUY.
    """
    if len(bars) < 2:
        return []
    prev, cur = bars[-2], bars[-1]
    if not prev.bearish:
        return []
    if prev.body <= 0 or cur.body <= 0:
        return []
    if cur.body > cfg.harami_max_body_ratio * prev.body:
        return []
    if cur.lower < cfg.long_shadow_ratio * max(cur.body, _EPS):
        return []
    # Sens de meche : la meche BASSE doit dominer la meche haute.
    if cur.lower <= cur.upper:
        return []
    if cur.range <= 0:
        return []
    strength = _clamp(0.45 + 0.35 * (cur.lower / cur.range))
    return [
        Hit(
            "bearish_hammer",
            BUY,
            strength,
            2,
            "rouge + 2e (verte/rouge) petit corps + longue meche basse -> BUY",
        )
    ]


# --------------------------------------------------------------------------
# Figures 3 bougies
# --------------------------------------------------------------------------
def detect_stars(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    if len(bars) < 3:
        return []
    a, b, c = bars[-3], bars[-2], bars[-1]
    if a.body <= 0 or c.body <= 0:
        return []
    small = cfg.star_small_body_ratio
    out: List[Hit] = []
    if a.bearish and c.bullish:
        if b.body <= small * min(a.body, c.body) and b.close <= a.close and c.close >= a.mid:
            strength = _clamp(0.5 + 0.3 * min(1.0, c.body / (a.body + _EPS)))
            out.append(Hit("morning_star", BUY, strength, 3, "etoile du matin"))
    if a.bullish and c.bearish:
        if b.body <= small * min(a.body, c.body) and b.close >= a.close and c.close <= a.mid:
            strength = _clamp(0.5 + 0.3 * min(1.0, c.body / (a.body + _EPS)))
            out.append(Hit("evening_star", SELL, strength, 3, "etoile du soir"))
    return out


def detect_triple(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    if len(bars) < 3:
        return []
    s = list(bars[-3:])
    avg_range = sum(x.range for x in s) / 3.0
    min_body = cfg.soldier_body_ratio * avg_range
    out: List[Hit] = []

    if all(x.bullish for x in s) and s[0].close < s[1].close < s[2].close:
        if all(x.body >= min_body for x in s) and all(x.upper <= 0.5 * x.body for x in s):
            ratio = sum(x.body for x in s) / (3.0 * avg_range)
            out.append(
                Hit("three_white_soldiers", BUY, _clamp(0.5 + 0.5 * ratio), 3, "trois soldats blancs")
            )

    if all(x.bearish for x in s) and s[0].close > s[1].close > s[2].close:
        if all(x.body >= min_body for x in s) and all(x.lower <= 0.5 * x.body for x in s):
            ratio = sum(x.body for x in s) / (3.0 * avg_range)
            out.append(
                Hit("three_black_crows", SELL, _clamp(0.5 + 0.5 * ratio), 3, "trois corbeaux noirs")
            )
    return out


# --------------------------------------------------------------------------
# Motifs DECOUVERTS par data mining (2 bougies, valides sur 2 periodes)
# --------------------------------------------------------------------------
DISCOVERED_PATTERNS = [
    ("d1", ("Ulms", "Dmsl"), BUY, "grande verte (mèche haute moyenne) puis rouge à longue mèche basse"),
    ("d2", ("Dsml", "Ulss"), BUY, "petite rouge à longue mèche basse puis grande verte propre"),
    ("d3", ("Ulsm", "Dlss"), SELL, "grande verte (mèche basse moyenne) puis grande rouge propre"),
    ("d4", ("Dlsm", "Dlms"), BUY, "grande rouge (mèche basse moyenne) puis grande rouge (mèche haute moyenne)"),
    ("d5", ("Ulms", "Ulsm"), BUY, "grande verte (mèche haute moyenne) puis grande verte (mèche basse moyenne)"),
    ("d6", ("Dlsm", "Ulsm"), SELL, "grande rouge (mèche basse moyenne) puis grande verte (mèche basse moyenne)"),
    ("d7", ("Dlms", "Ulms"), SELL, "grande rouge (mèche haute moyenne) puis grande verte (mèche haute moyenne)"),
    ("d8", ("Ulss", "Ummm"), SELL, "grande verte propre puis verte moyenne à mèches moyennes"),
    ("d9", ("Dlsm", "Dlsm"), SELL, "deux grandes rouges à mèche basse moyenne"),
    ("e1", ("Usls", "Usml"), SELL, "verte petit corps (mèche haute longue) puis verte petit corps (mèche basse longue)"),
    ("e2", ("Ummm", "Usll"), BUY, "verte moyenne équilibrée puis verte petit corps à mèches haute et basse longues"),
    ("e3", ("Dmmm", "Usls"), BUY, "rouge moyenne équilibrée puis verte petit corps à longue mèche haute"),
    ("e4", ("Dlsm", "Usll"), BUY, "grande rouge puis verte petit corps à mèches haute et basse longues"),
    ("e5", ("Uslm", "Dmls"), BUY, "verte petit corps (mèche haute longue) puis rouge moyenne à longue mèche haute"),
    ("e6", ("Dmls", "Dsml"), BUY, "rouge moyenne à longue mèche haute puis rouge petit corps à longue mèche basse"),
    ("e7", ("Dmml", "Ulss"), BUY, "rouge moyenne à longue mèche basse puis grande verte propre"),
    ("e8", ("Ussl", "Dlss"), BUY, "verte petit corps à longue mèche basse puis grande rouge propre"),
]

import json as _json
from pathlib import Path as _Path


def _code_desc(code: str) -> str:
    if not code or len(code) < 4:
        return code or ""
    d, bt, ut, lt = code[0], code[1], code[2], code[3]
    col = {"U": "verte", "D": "rouge", "=": "doji"}.get(d, d)
    b = {"s": "petit corps", "m": "corps moyen", "l": "grand corps"}.get(bt, "")
    u = {"s": "mèche haute courte", "m": "mèche haute moyenne", "l": "mèche haute longue"}.get(ut, "")
    lo = {"s": "mèche basse courte", "m": "mèche basse moyenne", "l": "mèche basse longue"}.get(lt, "")
    return f"{col} {b}, {u}, {lo}"


def _load_extra_patterns():
    try:
        path = _Path(__file__).resolve().parents[2] / "data" / "discovered_extra.json"
        data = _json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    out = []
    for i, it in enumerate(data, 1):
        try:
            t1, t2, dr = str(it["t1"]), str(it["t2"]), str(it["dir"]).upper()
            direction = BUY if dr == "BUY" else SELL
            desc = _code_desc(t1) + " puis " + _code_desc(t2)
            out.append((f"q{i}", (t1, t2), direction, desc))
        except Exception:  # noqa: BLE001
            continue
    return out


DISCOVERED_PATTERNS = list(DISCOVERED_PATTERNS) + _load_extra_patterns()


def _candle_code(b: Bar) -> Optional[str]:
    rng = b.range
    if rng <= 0:
        return None
    d = "U" if b.bullish else ("D" if b.bearish else "=")
    bp = b.body / rng
    u = b.upper / rng
    lw = b.lower / rng
    bt = "s" if bp < 0.30 else ("m" if bp < 0.60 else "l")
    ut = "s" if u < 0.15 else ("m" if u < 0.35 else "l")
    lt = "s" if lw < 0.15 else ("m" if lw < 0.35 else "l")
    return d + bt + ut + lt


def detect_triple_reversal(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Motif 3 bougies valide hors echantillon (~61%, n=49) -> BUY.
    rouge petit corps (2 meches longues) + grande rouge (longue meche basse)
    + verte (petit corps, longue meche basse).
    """
    if len(bars) < 3:
        return []
    b1, b2, b3 = bars[-3], bars[-2], bars[-1]
    for b in (b1, b2, b3):
        if b.range <= 0:
            return []
    if not b1.bearish or b1.body / b1.range >= 0.45:
        return []
    if b1.upper / b1.range <= 0.18 or b1.lower / b1.range <= 0.18:
        return []
    if not b2.bearish or b2.body / b2.range <= 0.40:
        return []
    if b2.lower / b2.range <= 0.25 or b2.upper / b2.range >= 0.35:
        return []
    if not b3.bullish or b3.body / b3.range >= 0.55:
        return []
    if b3.lower / b3.range <= 0.30 or b3.upper / b3.range >= 0.25:
        return []
    return [
        Hit(
            "t3",
            BUY,
            0.6,
            3,
            "3 bougies (rouge 2 meches, grande rouge, verte meche basse) -> BUY",
        )
    ]


def detect_reversal3(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Retournement valide hors echantillon (~61%, n=61) -> BUY.
    3 bougies rouges puis une verte qui englobe la precedente (petit corps).
    """
    if len(bars) < 4:
        return []
    prev3 = bars[-4:-1]
    if not all(b.bearish for b in prev3):
        return []
    prev = bars[-2]
    cur = bars[-1]
    if not cur.bullish or cur.range <= 0 or prev.range <= 0:
        return []
    if cur.body / cur.range > 0.30:
        return []
    if not (cur.open <= prev.close and cur.close >= prev.open):
        return []
    return [Hit("r1", BUY, 0.6, 4, "3 rouges + engulfing verte -> BUY")]


def _rsi14(closes: Sequence[float]) -> float:
    n = 14
    if len(closes) < n + 1:
        return 50.0
    g = l = 0.0
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gg = d if d > 0 else 0.0
        ll = -d if d < 0 else 0.0
        if i <= n:
            g += gg
            l += ll
        else:
            g = g * (n - 1) / n + gg
            l = l * (n - 1) / n + ll
    al = l / n
    return 100 - 100 / (1 + ((g / n) / al if al else 1e9))


def detect_red_upper_buy(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Rouge a longue meche haute (corps moyen) + RSI>50 -> BUY (~57-60% OOS)."""
    if len(bars) < 20:
        return []
    b = bars[-1]
    if not b.bearish or b.range <= 0:
        return []
    bp = b.body / b.range
    if not (0.35 <= bp <= 1.0):
        return []
    if b.upper / b.range < 0.45:
        return []
    if b.lower / b.range > 0.20:
        return []
    if _rsi14([x.close for x in bars]) <= 50:
        return []
    return [
        Hit("r2", BUY, 0.6, 1, "rouge longue meche haute + RSI>50 -> BUY")
    ]


def _ema(closes: Sequence[float], n: int) -> float:
    if not closes:
        return 0.0
    k = 2.0 / (n + 1)
    e = closes[0]
    for v in closes[1:]:
        e = v * k + e * (1 - k)
    return e


def detect_green_upper_pullback_buy(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    """Verte a longue meche haute, tendance haussiere (EMA20>EMA50), RSI<50 -> BUY.
    Valide hors echantillon ~59-61%, session Londres/NY exigee cote bot.
    """
    if len(bars) < 50:
        return []
    b = bars[-1]
    if not b.bullish or b.range <= 0:
        return []
    bp = b.body / b.range
    if not (0.35 <= bp <= 1.0):
        return []
    if b.upper / b.range < 0.35:
        return []
    if b.lower / b.range > 0.20:
        return []
    closes = [x.close for x in bars]
    if _ema(closes, 20) <= _ema(closes, 50):
        return []
    if _rsi14(closes) >= 50:
        return []
    return [Hit("r3", BUY, 0.6, 1, "verte longue meche haute + uptrend + RSI<50 -> BUY")]


def detect_discovered(bars: Sequence[Bar], cfg: PatternConfig) -> List[Hit]:
    if len(bars) < 2:
        return []
    c1 = _candle_code(bars[-2])
    c2 = _candle_code(bars[-1])
    if c1 is None or c2 is None:
        return []
    out: List[Hit] = []
    for name, (t1, t2), direction, desc in DISCOVERED_PATTERNS:
        if c1 == t1 and c2 == t2:
            out.append(Hit(name, direction, 0.60, 2, desc))
    return out


# --------------------------------------------------------------------------
# Agregation
# --------------------------------------------------------------------------
def detect(bars: Sequence[Bar], cfg: Optional[PatternConfig] = None) -> List[Hit]:
    """Toutes les figures qui se terminent sur la derniere bougie de `bars`.

    Accepte des `Bar`, des dicts OHLC ou des objets avec des attributs OHLC.
    """
    cfg = cfg or PatternConfig()
    normalized = [
        b
        if isinstance(b, Bar)
        else Bar(_field(b, "open"), _field(b, "high"), _field(b, "low"), _field(b, "close"))
        for b in bars
    ]
    if not normalized:
        return []
    hits: List[Hit] = []
    hits += detect_doji(normalized, cfg)
    hits += detect_hammer(normalized, cfg)
    hits += detect_shooting_star(normalized, cfg)
    hits += detect_engulfing(normalized, cfg)
    hits += detect_harami(normalized, cfg)
    hits += detect_bullish_inside(normalized, cfg)
    hits += detect_bullish_big_lower(normalized, cfg)
    hits += detect_bearish_hammer(normalized, cfg)
    hits += detect_green_upper_pin(normalized, cfg)
    hits += detect_inverted_hammer(normalized, cfg)
    hits += detect_discovered(normalized, cfg)
    hits += detect_triple_reversal(normalized, cfg)
    hits += detect_reversal3(normalized, cfg)
    hits += detect_red_upper_buy(normalized, cfg)
    hits += detect_green_upper_pullback_buy(normalized, cfg)
    hits += detect_stars(normalized, cfg)
    hits += detect_triple(normalized, cfg)
    return hits


def detect_candles(
    candles: Sequence[CandleLike], cfg: Optional[PatternConfig] = None
) -> List[Hit]:
    """Comme `detect` mais accepte des dicts (sortie SQLite) ou des dataclasses."""
    if not candles:
        return []
    return detect(to_bars(candles), cfg)
