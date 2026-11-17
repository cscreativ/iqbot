from __future__ import annotations

from typing import Optional

AICall = callable


def load_candles(db, asset: str, timeframe: int, limit: int) -> list[dict]:
    """Bougies serveur pour >=300 s, sinon bougies reconstruites depuis les ticks (scalping)."""
    if timeframe >= 300:
        return db.get_candles(asset, timeframe, limit)
    return db.get_candles_from_ticks(asset, timeframe, limit)


def market_summary(candles: list[dict], asset: str, timeframe: int) -> str:
    from .signals import ema, rsi

    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    last = closes[-1]
    first = closes[0]
    change = ((last - first) / first * 100.0) if first else 0.0
    avg_range = sum(h - l for h, l in zip(highs, lows)) / len(candles)
    fast = ema(closes, 9)[-1]
    slow = ema(closes, 21)[-1]
    rsi_val = rsi(closes, 14)[-1]

    lines = [
        f"Actif: {asset} (timeframe {timeframe}s), {len(candles)} bougies",
        f"Dernier prix: {last:.5f}",
        f"Variation sur la fenetre: {change:.2f}%",
        f"Plus haut: {max(highs):.5f} / Plus bas: {min(lows):.5f}",
        f"Range moyen par bougie: {avg_range:.5f}",
        "EMA9 " + (">" if fast > slow else "<") + " EMA21 (tendance "
        + ("haussiere" if fast > slow else "baissiere") + ")",
        ("RSI14: %.1f" % rsi_val) if rsi_val is not None else "RSI14: n/a",
        "20 dernieres bougies (ts,open,high,low,close):",
    ]
    for c in candles[-20:]:
        lines.append(f"{int(c['ts'])},{c['open']},{c['high']},{c['low']},{c['close']}")
    return "\n".join(lines)


def build_context(candles: list[dict], window: int) -> str:
    lines = ["ts,open,high,low,close"]
    for c in candles[-window:]:
        lines.append(f"{int(c['ts'])},{c['open']},{c['high']},{c['low']},{c['close']}")
    return "\n".join(lines)


def simulate_one(candles: list[dict], index: int, direction: str, horizon: int) -> Optional[dict]:
    n = len(candles)
    exit_index = min(index + horizon, n - 1)
    if exit_index <= index:
        return None
    entry_price = float(candles[index]["close"])
    exit_price = float(candles[exit_index]["close"])
    if entry_price == 0:
        return None
    if direction == "BUY":
        pnl_pct = (exit_price - entry_price) / entry_price * 100.0
    else:
        pnl_pct = (entry_price - exit_price) / entry_price * 100.0

    segment = candles[index : exit_index + 1]
    hi = max(float(c["high"]) for c in segment)
    lo = min(float(c["low"]) for c in segment)
    if direction == "BUY":
        mfe = (hi - entry_price) / entry_price * 100.0
        mae = (lo - entry_price) / entry_price * 100.0
    else:
        mfe = (entry_price - lo) / entry_price * 100.0
        mae = (entry_price - hi) / entry_price * 100.0

    return {
        "index": index,
        "ts": candles[index]["ts"],
        "direction": direction,
        "entry": entry_price,
        "exit": exit_price,
        "exit_index": exit_index,
        "pnl_pct": round(pnl_pct, 4),
        "win": pnl_pct > 0,
        "mfe_pct": round(mfe, 4),
        "mae_pct": round(mae, 4),
    }


def compute_metrics(trades: list[dict]) -> dict:
    n = len(trades)
    wins = sum(1 for t in trades if t["win"])
    losses = n - wins
    total = sum(t["pnl_pct"] for t in trades)
    gains = sum(t["pnl_pct"] for t in trades if t["pnl_pct"] > 0)
    drops = -sum(t["pnl_pct"] for t in trades if t["pnl_pct"] < 0)
    if drops > 0:
        profit_factor = gains / drops
    elif gains > 0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0
    return {
        "trades": n,
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / n * 100.0) if n else 0.0, 2),
        "total_pnl_pct": round(total, 4),
        "avg_pnl_pct": round((total / n) if n else 0.0, 4),
        "profit_factor": round(profit_factor, 3),
    }


def plan_replay(
    db,
    asset: str,
    timeframe: int,
    prompt: str,
    lookback: int = 200,
    horizon: int = 10,
    window: int = 20,
    step: int = 5,
    max_steps: int = 10,
) -> dict:
    candles = load_candles(db, asset, timeframe, lookback)
    if len(candles) < window + horizon + 2:
        raise ValueError(
            f"Pas assez d'historique pour {asset} en {timeframe}s "
            f"({len(candles)} bougies). Ouvre l'actif dans le traderoom pour charger l'historique."
        )
    end = len(candles) - horizon - 1
    start = max(window - 1, end - step * (max_steps - 1))
    indices = list(range(start, end + 1, step))[-max_steps:]
    contexts = [build_context(candles[: index + 1], window) for index in indices]
    return {
        "asset": asset,
        "timeframe": timeframe,
        "prompt": prompt,
        "candles": candles,
        "indices": indices,
        "contexts": contexts,
        "horizon": horizon,
        "window": window,
        "step": step,
    }


def finalize_replay(plan: dict, decisions: list, min_confidence: float) -> dict:
    candles = plan["candles"]
    indices = plan["indices"]
    horizon = plan["horizon"]
    trades: list[dict] = []
    decs: list[dict] = []

    for index, raw in zip(indices, decisions):
        if isinstance(raw, Exception):
            decs.append({"index": index, "error": str(raw)[:160]})
            continue
        if not isinstance(raw, dict):
            decs.append({"index": index, "error": "reponse invalide"})
            continue
        direction = str(raw.get("direction") or "NONE").upper()
        try:
            confidence = float(raw.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0.0
        decs.append(
            {
                "index": index,
                "ts": candles[index]["ts"],
                "direction": direction,
                "confidence": round(confidence, 3),
                "reason": str(raw.get("explanation") or "")[:200],
            }
        )
        if direction in {"BUY", "SELL"} and confidence >= min_confidence:
            trade = simulate_one(candles, index, direction, horizon)
            if trade:
                trade["confidence"] = round(confidence, 3)
                trade["reason"] = str(raw.get("explanation") or "")[:200]
                trades.append(trade)

    trades.sort(key=lambda t: t["index"])
    return {
        "asset": plan["asset"],
        "timeframe": plan["timeframe"],
        "method_name": plan["prompt"][:80],
        "candles_used": len(candles),
        "window": plan["window"],
        "step": plan["step"],
        "horizon": horizon,
        "decisions": decs,
        "trades": trades,
        "metrics": compute_metrics(trades),
    }
