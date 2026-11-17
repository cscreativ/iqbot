from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from iqopt.collector import run  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Collecte le flux temps reel du traderoom IQ Option")
    parser.add_argument("--asset", default=None, help="Actif par defaut (ex: EURUSD)")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Navigateur invisible (necessite une session deja sauvegardee)",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Force le navigateur visible (pour se connecter)",
    )
    args = parser.parse_args()

    headless = True if args.headless else False if args.visible else None
    run(asset=args.asset, headless=headless)


if __name__ == "__main__":
    main()
