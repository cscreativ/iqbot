"""Construit une strategie a partir des trades appris, en ne gardant que
les methodes (figures) a ROI positif. Les figures explicitement exclues
(bearish_engulfing, bearish_harami) ne sont jamais utilisees."""

from __future__ import annotations

import statistics
from collections import Counter
from typing import Any, Dict, List, Optional

BLACKLIST = {"bearish_engulfing", "bearish_harami", "evening_star"}


def _norm_dir(value: Any) -> Optional[str]:
    d = str(value or "").upper()
    if d in ("CALL", "BUY"):
        return "BUY"
    if d in ("PUT", "SELL"):
        return "SELL"
    return None


def pattern_stats(samples: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Stats par figure sur TOUS les trades (gagnants + perdants)."""
    g: Dict[str, Dict[str, Any]] = {}
    for s in samples:
        r = str(s.get("result") or "").lower()
        try:
            amt = float(s.get("amount") or 0)
        except (TypeError, ValueError):
            amt = 0.0
        p = s.get("profit")
        for tok in str(s.get("patterns") or "").split(","):
            tok = tok.strip()
            if not tok:
                continue
            d = g.setdefault(
                tok, {"wins": 0, "losses": 0, "net": 0.0, "ratios": []}
            )
            if r in ("win", "won"):
                d["wins"] += 1
                gain = (float(p) - amt) if p is not None else amt * 0.84
                d["net"] += gain
                if amt:
                    d["ratios"].append(gain / amt)
            elif r in ("loose", "lost", "loss"):
                d["losses"] += 1
                d["net"] -= amt
                if amt:
                    d["ratios"].append(-1.0)
    out: Dict[str, Dict[str, Any]] = {}
    for k, d in g.items():
        done = d["wins"] + d["losses"]
        out[k] = {
            "trades": done,
            "wins": d["wins"],
            "losses": d["losses"],
            "rate": round(100 * d["wins"] / done) if done else 0,
            "net": round(d["net"], 2),
            "avg_roi": round(100 * sum(d["ratios"]) / len(d["ratios"]), 2) if d["ratios"] else 0,
        }
    return out


def build_strategy(
    samples: List[Dict[str, Any]],
    min_trades: int = 3,
    max_amount: float = 100.0,
    tradable_assets: Optional[set] = None,
) -> Optional[Dict[str, Any]]:
    """Ne garde que les figures a ROI positif (hors blacklist)."""
    samples = list(samples or [])
    if not samples:
        return None

    stats = pattern_stats(samples)
    eligible = [
        (name, v)
        for name, v in stats.items()
        if v["avg_roi"] > 0 and v["trades"] >= min_trades and name not in BLACKLIST
    ]
    if not eligible:
        return None
    # meilleure figure = ROI moyen le plus eleve
    eligible.sort(key=lambda kv: (kv[1]["avg_roi"], kv[1]["rate"]), reverse=True)
    best_name, best = eligible[0]

    eligible_names = {name for name, _ in eligible}

    def _toks(s):
        return {t.strip() for t in str(s.get("patterns") or "").split(",") if t.strip()}

    # On n'apprend que des gains RATTACHES a une methode (figure) valide.
    win_samples = [
        s
        for s in samples
        if str(s.get("result") or "").lower() in ("win", "won")
        and (_toks(s) & eligible_names)
    ]

    def majority_dir(names) -> str:
        b = q = 0
        for s in win_samples:
            toks = {t.strip() for t in str(s.get("patterns") or "").split(",") if t.strip()}
            if names & toks:
                d = _norm_dir(s.get("direction"))
                if d == "BUY":
                    b += 1
                elif d == "SELL":
                    q += 1
        return "BUY" if b >= q else "SELL"

    directions = {name: majority_dir({name}) for name, _ in eligible}

    amounts: List[float] = []
    exps: Counter = Counter()
    assets: Counter = Counter()
    # On se base sur les trades gagnants RECENTS (plus representatifs).
    recent = win_samples[:60]
    for s in recent:
        try:
            a = float(s.get("amount") or 0)
            if a > 0:
                amounts.append(a)
        except (TypeError, ValueError):
            pass
        es = s.get("expiration_size")
        if es:
            try:
                exps[int(es)] += 1
            except (TypeError, ValueError):
                pass
        assets[str(s.get("asset") or "?")] += 1

    amount = statistics.median(amounts) if amounts else 1.0
    amount = max(1.0, min(float(max_amount), round(amount, 2)))
    expiry = exps.most_common(1)[0][0] if exps else 300
    # Prefere une paire tradable (binaire), OTC ou non, selon les trades reussis.
    pref = assets
    if tradable_assets:
        filtered = {a: c for a, c in assets.items() if a in tradable_assets}
        if filtered:
            pref = Counter(filtered)
    asset = pref.most_common(1)[0][0] if pref else None

    return {
        "pattern": best_name,
        "direction": directions[best_name],
        "allowed": [n for n, _ in eligible],
        "directions": directions,
        "amount": amount,
        "expiry": int(expiry),
        "asset": asset,
        "avg_roi": best["avg_roi"],
        "trades": best["trades"],
        "rate": best["rate"],
        "excluded": sorted(BLACKLIST),
    }
