from __future__ import annotations

from typing import Optional, Sequence

from .storage import Database


def ema(values: Sequence[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(value * k + out[-1] * (1.0 - k))
    return out


def rsi(values: Sequence[float], period: int = 14) -> list[Optional[float]]:
    if len(values) < period + 1:
        return [None] * len(values)
    out: list[Optional[float]] = [None] * len(values)
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        delta = values[i] - values[i - 1]
        gains += max(delta, 0.0)
        losses += max(-delta, 0.0)
    avg_gain = gains / period
    avg_loss = losses / period
    out[period] = _rsi_value(avg_gain, avg_loss)
    for i in range(period + 1, len(values)):
        delta = values[i] - values[i - 1]
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        out[i] = _rsi_value(avg_gain, avg_loss)
    return out


def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def generate_signals(
    db: Database, asset: str, timeframe: int = 60, limit: int = 300, period: int = 14
) -> list[dict]:
    asset = asset.upper()
    candles = db.get_candles(asset, timeframe, limit)
    closes = [c["close"] for c in candles]
    if len(closes) < period + 2:
        return []

    rsi_series = rsi(closes, period)
    fast = ema(closes, 9)
    slow = ema(closes, 21)
    last_rsi = rsi_series[-1]
    if last_rsi is None:
        return []

    signals: list[dict] = []
    bullish = fast[-1] > slow[-1]
    if last_rsi < 30 and bullish:
        signals.append(
            {"kind": "BUY", "strength": round(100 - last_rsi, 2), "note": f"RSI={last_rsi:.1f} EMA9>EMA21"}
        )
    elif last_rsi > 70 and not bullish:
        signals.append(
            {"kind": "SELL", "strength": round(last_rsi, 2), "note": f"RSI={last_rsi:.1f} EMA9<EMA21"}
        )

    for signal in signals:
        db.insert_signal(asset, signal["kind"], signal["strength"], signal["note"])
    return signals
