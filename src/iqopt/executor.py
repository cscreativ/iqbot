from __future__ import annotations

import os
from typing import Optional

from .config import Settings, load_settings
from .storage import Database


class ExecutionBlocked(RuntimeError):
    pass


class OrderExecutor:
    """Executeur garde.

    Par securite, l'execution reelle est DESACTIVEE par defaut. Ce module
    enregistre les intentions d'ordre (dry-run) afin de valider la logique
    avant tout branchement sur le DOM du traderoom.

    Pour activer (a tes risques) :
      export IQ_ENABLE_EXECUTION=1
      # live uniquement avec une confirmation explicite en plus :
      export IQ_DEMO_ONLY=0
      export IQ_CONFIRM_LIVE=YES
    """

    def __init__(self, settings: Optional[Settings] = None, db: Optional[Database] = None) -> None:
        self.settings = settings or load_settings()
        self.db = db or Database(self.settings.db_path)
        self.db.init()

    def can_execute(self) -> tuple[bool, str]:
        if not self.settings.enable_execution:
            return False, "IQ_ENABLE_EXECUTION n'est pas active"
        if not self.settings.demo_only and os.getenv("IQ_CONFIRM_LIVE") != "YES":
            return False, "compte reel sans IQ_CONFIRM_LIVE=YES"
        return True, "ok"

    def submit(self, asset: str, direction: str, amount: float) -> int:
        allowed, reason = self.can_execute()
        direction = direction.upper()
        if direction not in {"BUY", "SELL"}:
            raise ValueError("direction doit etre BUY ou SELL")
        if amount <= 0:
            raise ValueError("montant doit etre > 0")

        if not allowed:
            order_id = self.db.insert_order(asset, direction, amount, "dry_run", reason)
            print(f"[executor] DRY-RUN {direction} {asset} {amount} (raison: {reason}) -> ordre #{order_id}")
            return order_id

        # Point d'extension : brancher ici le clic sur les boutons du DOM
        # (selecteurs a calibrer sur la version courante du traderoom).
        order_id = self.db.insert_order(
            asset, direction, amount, "not_wired", "selecteurs DOM non configures"
        )
        print(
            f"[executor] execution autorisee mais branchement DOM absent "
            f"-> ordre #{order_id} marque not_wired"
        )
        return order_id
