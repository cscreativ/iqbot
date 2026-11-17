"""Bot de formations de bougies : lit les bougies du collector, detecte une
figure sur la derniere bougie CLOTUREE et soumet un ordre via l'executeur garde.

Par defaut l'execution est en DRY-RUN (aucun argent engage). Le bot suppose
que `run_collector.py` tourne deja et remplit la base SQLite.

Bougies CLOTUREES uniquement : la derniere bougie (en cours de formation) est
ignoree, sinon la figure serait incomplete/fausse.
"""

from __future__ import annotations

import os
import time
from typing import Any, List, Optional

from .config import Settings, load_settings
from .executor import OrderExecutor
from .patterns import PatternConfig
from .storage import Database
from .strategy import Decision, PatternStrategy


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


class PatternBot:
    def __init__(
        self,
        asset: Optional[str] = None,
        source: str = "ticks",
        interval: int = 60,
        timeframe: int = 60,
        limit: int = 200,
        amount: float = 1.0,
        min_strength: float = 0.5,
        poll: float = 5.0,
        auto_trade: bool = True,
        max_trades: int = 0,
        cooldown: float = 60.0,
        require_trend: bool = False,
        allowed: Optional[List[str]] = None,
        settings: Optional[Settings] = None,
        db: Optional[Database] = None,
    ) -> None:
        self.settings = settings or load_settings()
        self.asset = (asset or self.settings.asset).upper()
        self.source = source
        self.interval = int(interval)
        self.timeframe = int(timeframe)
        self.limit = int(limit)
        self.amount = float(amount)
        self.poll = float(poll)
        self.auto_trade = bool(auto_trade)
        self.max_trades = int(max_trades)
        self.cooldown = float(cooldown)

        self.db = db or Database(self.settings.db_path)
        self.db.init()
        self.executor = OrderExecutor(self.settings, self.db)
        self.strategy = PatternStrategy(
            PatternConfig(),
            min_strength=min_strength,
            allowed=allowed,
            require_trend=require_trend,
        )

        self._last_bar_ts: Optional[float] = None
        self._last_trade_ts = 0.0
        self._trades = 0
        self._started = time.time()

    # ------------------------------------------------------------------
    def _candles(self) -> List[dict[str, Any]]:
        if self.source == "ticks":
            return self.db.get_candles_from_ticks(self.asset, self.interval, self.limit)
        return self.db.get_candles(self.asset, self.timeframe, self.limit)

    def _describe(self) -> str:
        tf = f"{self.interval}s (ticks)" if self.source == "ticks" else f"{self.timeframe}s (serveur)"
        ex = "DRY-RUN" if not self.settings.enable_execution else "EXECUTION"
        return (
            f"[pattern] {self.asset} bougies {tf} | force>={self.strategy.min_strength} "
            f"| amount={self.amount} | mode={ex} | auto_trade={self.auto_trade}"
        )

    def step(self) -> Optional[Decision]:
        candles = self._candles()
        # Il faut assez de bougies pour une figure a 3 bougies + tendance.
        if len(candles) < 5:
            print(
                f"[pattern] pas assez de bougies ({len(candles)}). "
                "Le collector tourne-t-il et l'actif est-il ouvert ?",
                flush=True,
            )
            return None

        # La derniere bougie est en cours de formation -> on l'ecarte.
        closed = candles[:-1]
        if not closed:
            return None

        bar_ts = float(closed[-1]["ts"])
        if self._last_bar_ts is not None and bar_ts <= self._last_bar_ts:
            return None
        self._last_bar_ts = bar_ts

        decision = self.strategy.evaluate_candles(closed)
        if decision is None:
            return None

        note = (
            f"{decision.pattern} force={decision.strength:.2f} "
            f"bougie_ts={bar_ts:.0f} ({decision.note})"
        )
        self.db.insert_signal(
            self.asset, decision.direction, round(decision.strength * 100.0, 2), note
        )
        print(
            f"[pattern] {self.asset} {decision.direction} via {decision.pattern} "
            f"(force {decision.strength:.2f}) bougie_ts={bar_ts:.0f}",
            flush=True,
        )

        if not self.auto_trade:
            return decision
        if self.max_trades and self._trades >= self.max_trades:
            print(f"[pattern] limite de {self.max_trades} trades atteinte, signal ignore", flush=True)
            return decision
        if time.time() - self._last_trade_ts < self.cooldown:
            print("[pattern] cooldown actif, signal ignore", flush=True)
            return decision

        order_id = self.executor.submit(self.asset, decision.direction, self.amount)
        self._last_trade_ts = time.time()
        self._trades += 1
        print(f"[pattern] ordre #{order_id} soumis ({self._trades} trade(s) cette session)", flush=True)
        return decision

    # ------------------------------------------------------------------
    def run(self) -> None:
        print(self._describe(), flush=True)
        while True:
            try:
                self.step()
            except KeyboardInterrupt:
                raise
            except Exception as exc:  # noqa: BLE001
                print(f"[pattern] erreur: {exc}", flush=True)
            time.sleep(self.poll)


def run(
    asset: Optional[str] = None,
    source: str = "ticks",
    interval: int = 60,
    timeframe: int = 60,
    limit: int = 200,
    amount: float = 1.0,
    min_strength: float = 0.5,
    poll: float = 5.0,
    auto_trade: bool = True,
    max_trades: int = 0,
    cooldown: float = 60.0,
    require_trend: bool = False,
    allowed: Optional[List[str]] = None,
) -> None:
    bot = PatternBot(
        asset=asset,
        source=source,
        interval=interval,
        timeframe=timeframe,
        limit=limit,
        amount=amount,
        min_strength=min_strength,
        poll=poll,
        auto_trade=auto_trade,
        max_trades=max_trades,
        cooldown=cooldown,
        require_trend=require_trend,
        allowed=allowed,
    )
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n[pattern] arret demande")
