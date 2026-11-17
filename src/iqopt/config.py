from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "oui"}


def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value).expanduser().resolve() if value else default


@dataclass(frozen=True)
class Settings:
    base_dir: Path = BASE_DIR
    traderoom_url: str = os.getenv("IQ_TRADEROOM_URL", "https://iqoption.com/traderoom")
    asset: str = os.getenv("IQ_ASSET", "EURUSD")
    headless: bool = _env_bool("IQ_HEADLESS", False)
    capture_raw: bool = _env_bool("IQ_CAPTURE_RAW", True)
    data_dir: Path = _env_path("IQ_DATA_DIR", BASE_DIR / "data")
    captures_dir: Path = _env_path("IQ_CAPTURES_DIR", BASE_DIR / "captures")
    storage_state: Path = _env_path("IQ_STORAGE_STATE", BASE_DIR / "playwright_state.json")

    # Garde-fous execution d'ordres : DESACTIVE par defaut.
    enable_execution: bool = _env_bool("IQ_ENABLE_EXECUTION", False)
    demo_only: bool = _env_bool("IQ_DEMO_ONLY", True)

    dashboard_host: str = os.getenv("IQ_DASHBOARD_HOST", "127.0.0.1")
    dashboard_port: int = int(os.getenv("IQ_DASHBOARD_PORT", "8765"))
    # Jeton d'acces optionnel : si renseigne, le dashboard exige ?token=...
    # (cookie pose ensuite automatiquement). Recommande en acces telephone/LAN.
    dashboard_token: str = os.getenv("IQ_DASHBOARD_TOKEN", "")

    @property
    def db_path(self) -> Path:
        return self.data_dir / "iqopt.sqlite3"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.captures_dir.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    settings = Settings()
    settings.ensure_dirs()
    return settings
