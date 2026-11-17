from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from iqopt.pattern_bot import run  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bot de formations de bougies (lit les bougies du collector, "
        "soumet des ordres en dry-run par defaut)."
    )
    parser.add_argument("--asset", default=None, help="Actif (defaut: IQ_ASSET, sinon EURUSD)")
    parser.add_argument(
        "--source",
        choices=["ticks", "candles"],
        default="ticks",
        help="ticks: bougies reconstruites depuis les ticks (scalping) ; candles: bougies serveur",
    )
    parser.add_argument("--interval", type=int, default=60, help="Taille des bougies ticks en secondes")
    parser.add_argument("--timeframe", type=int, default=60, help="Timeframe des bougies serveur (source=candles)")
    parser.add_argument("--limit", type=int, default=200, help="Nombre de bougies chargees")
    parser.add_argument("--amount", type=float, default=1.0, help="Mise par trade")
    parser.add_argument("--min-strength", type=float, default=0.5, help="Force minimale 0..1")
    parser.add_argument("--poll", type=float, default=5.0, help="Intervalle de reevaluation (s)")
    parser.add_argument("--max-trades", type=int, default=0, help="0 = illimite")
    parser.add_argument("--cooldown", type=float, default=60.0, help="Delai mini entre deux trades (s)")
    parser.add_argument("--require-trend", action="store_true", help="Filtre momentum")
    parser.add_argument(
        "--allow",
        default=None,
        help="Figures autorisees, separees par des virgules (ex: hammer,bullish_engulfing)",
    )
    parser.add_argument("--no-trade", action="store_true", help="Signaux uniquement, aucun ordre")
    args = parser.parse_args()

    allowed = [x.strip() for x in args.allow.split(",") if x.strip()] if args.allow else None
    run(
        asset=args.asset,
        source=args.source,
        interval=args.interval,
        timeframe=args.timeframe,
        limit=args.limit,
        amount=args.amount,
        min_strength=args.min_strength,
        poll=args.poll,
        auto_trade=not args.no_trade,
        max_trades=args.max_trades,
        cooldown=args.cooldown,
        require_trend=args.require_trend,
        allowed=allowed,
    )


if __name__ == "__main__":
    main()
