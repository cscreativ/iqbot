from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    asset   TEXT    NOT NULL,
    ts      REAL    NOT NULL,
    price   REAL    NOT NULL,
    source  TEXT    NOT NULL DEFAULT 'ws'
);
CREATE INDEX IF NOT EXISTS idx_ticks_asset_ts ON ticks (asset, ts);
CREATE INDEX IF NOT EXISTS idx_ticks_asset_id ON ticks (asset, id);

CREATE TABLE IF NOT EXISTS candles (
    asset      TEXT    NOT NULL,
    timeframe  INTEGER NOT NULL,
    ts         REAL    NOT NULL,
    open       REAL    NOT NULL,
    high       REAL    NOT NULL,
    low        REAL    NOT NULL,
    close      REAL    NOT NULL,
    volume     REAL,
    PRIMARY KEY (asset, timeframe, ts)
);

CREATE TABLE IF NOT EXISTS assets (
    active_id  INTEGER PRIMARY KEY,
    name       TEXT    NOT NULL,
    type       TEXT,
    precision  INTEGER
);

CREATE TABLE IF NOT EXISTS signals (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        REAL    NOT NULL,
    asset     TEXT    NOT NULL,
    kind      TEXT    NOT NULL,
    strength  REAL    NOT NULL DEFAULT 0,
    note      TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL    NOT NULL,
    asset      TEXT    NOT NULL,
    direction  TEXT    NOT NULL,
    amount     REAL    NOT NULL,
    status     TEXT    NOT NULL,
    note       TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT
);

CREATE TABLE IF NOT EXISTS analyses (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL    NOT NULL,
    asset        TEXT,
    pattern      TEXT,
    direction    TEXT,
    confidence   REAL,
    explanation  TEXT,
    action       TEXT,
    raw          TEXT
);

CREATE TABLE IF NOT EXISTS strategies (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL    NOT NULL,
    name         TEXT,
    asset        TEXT,
    timeframe    INTEGER,
    prompt       TEXT,
    params       TEXT,
    metrics      TEXT,
    trades_json  TEXT
);

CREATE TABLE IF NOT EXISTS trader_samples (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               REAL    NOT NULL,
    position_id      INTEGER,
    asset            TEXT,
    direction        TEXT,
    amount           REAL,
    entry            REAL,
    expiry           REAL,
    expiration_size  INTEGER,
    patterns         TEXT,
    trend            INTEGER,
    hour             INTEGER,
    source           TEXT,
    result           TEXT,
    profit           REAL
);

CREATE TABLE IF NOT EXISTS learnings (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        REAL    NOT NULL,
    kind      TEXT,
    text      TEXT,
    evidence  TEXT
);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def init(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            # Index compatibles avec les requetes "COLLATE NOCASE" (sinon scan complet).
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ticks_asset_nc_ts "
                    "ON ticks(asset COLLATE NOCASE, ts)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ticks_asset_nc_id "
                    "ON ticks(asset COLLATE NOCASE, id)"
                )
            except Exception:  # noqa: BLE001
                pass
            cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
            for name, typ in (
                ("position_id", "INTEGER"),
                ("entry", "REAL"),
                ("expiry", "REAL"),
                ("win_return", "REAL"),
                ("result", "TEXT"),
                ("profit", "REAL"),
                ("req_expiry", "INTEGER"),
            ):
                if name not in cols:
                    conn.execute(f"ALTER TABLE orders ADD COLUMN {name} {typ}")
            acols = {row[1] for row in conn.execute("PRAGMA table_info(assets)").fetchall()}
            if "suspended" not in acols:
                conn.execute("ALTER TABLE assets ADD COLUMN suspended INTEGER DEFAULT 0")
            if "schedule" not in acols:
                conn.execute("ALTER TABLE assets ADD COLUMN schedule TEXT")
            tcols = {row[1] for row in conn.execute("PRAGMA table_info(trader_samples)").fetchall()}
            for name, typ in (
                ("entry_offset", "REAL"),
                ("sec_before_close", "REAL"),
            ):
                if name not in tcols:
                    conn.execute(f"ALTER TABLE trader_samples ADD COLUMN {name} {typ}")

    # -- writes ---------------------------------------------------------
    def insert_tick(self, asset: str, ts: float, price: float, source: str = "ws") -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO ticks (asset, ts, price, source) VALUES (?, ?, ?, ?)",
                (asset, ts, price, source),
            )

    def upsert_candle(
        self,
        asset: str,
        timeframe: int,
        ts: float,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Optional[float] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO candles (asset, timeframe, ts, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset, timeframe, ts) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume
                """,
                (asset, timeframe, ts, open_, high, low, close, volume),
            )

    def insert_signal(self, asset: str, kind: str, strength: float, note: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO signals (ts, asset, kind, strength, note) VALUES (?, ?, ?, ?, ?)",
                (time.time(), asset, kind, strength, note),
            )

    def clear_signals(self, asset: Optional[str] = None) -> int:
        with self._connect() as conn:
            if asset:
                cur = conn.execute("DELETE FROM signals WHERE asset = ? COLLATE NOCASE", (asset,))
            else:
                cur = conn.execute("DELETE FROM signals")
            return int(cur.rowcount or 0)

    def insert_order(
        self,
        asset: str,
        direction: str,
        amount: float,
        status: str,
        note: str = "",
        req_expiry: Optional[int] = None,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO orders (ts, asset, direction, amount, status, note, req_expiry) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (time.time(), asset, direction, amount, status, note, req_expiry),
            )
            return int(cur.lastrowid)

    def next_queued_order(self) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, ts, asset, direction, amount, status, note, req_expiry FROM orders "
                "WHERE status = 'queued' ORDER BY id ASC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def update_order_status(self, order_id: int, status: str, note: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE orders SET status = ?, note = ? WHERE id = ?",
                (status, note, int(order_id)),
            )

    def get_order_positions(self) -> set:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT position_id FROM orders WHERE position_id IS NOT NULL"
            ).fetchall()
        return {int(r[0]) for r in rows if r[0] is not None}

    def has_order_position(self, position_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM orders WHERE position_id = ? LIMIT 1", (int(position_id),)
            ).fetchone()
        return row is not None

    def get_order(self, order_id: int) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM orders WHERE id = ?", (int(order_id),)
            ).fetchone()
        return dict(row) if row else None

    def get_recent_orders(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, asset, direction, amount, status, note FROM orders "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def set_order_fill(
        self,
        order_id: int,
        position_id: Optional[int],
        entry: Optional[float],
        expiry: Optional[float],
        win_return: Optional[float] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE orders SET position_id = ?, entry = ?, expiry = ?, win_return = ? WHERE id = ?",
                (position_id, entry, expiry, win_return, int(order_id)),
            )

    def get_trades(self, limit: int = 30) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, asset, direction, amount, status, position_id, entry, expiry, "
                "win_return, result, profit "
                "FROM orders WHERE position_id IS NOT NULL AND expiry IS NOT NULL "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def clear_trades(self) -> int:
        """Vide les resultats : supprime les ordres executes (positions)."""
        with self._connect() as conn:
            n = conn.execute("DELETE FROM orders WHERE position_id IS NOT NULL").rowcount
        return int(n or 0)

    def set_order_result(
        self, position_id: int, result: str, profit: Optional[float] = None
    ) -> int:
        """Enregistre le resultat officiel IQ (win/loose/opened) pour une position."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE orders SET result = ?, profit = ? WHERE position_id = ?",
                (result, profit, int(position_id)),
            )
            return int(cur.rowcount or 0)

    # -- apprentissage --------------------------------------------------
    def has_position(self, position_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM orders WHERE position_id = ? LIMIT 1", (int(position_id),)
            ).fetchone()
        return row is not None

    def add_sample(
        self,
        ts: float,
        position_id: int,
        asset: str,
        direction: str,
        amount: float,
        entry: Optional[float],
        expiry: Optional[float],
        expiration_size: Optional[int],
        patterns: str,
        trend: int,
        hour: int,
        source: str,
        entry_offset: Optional[float] = None,
        sec_before_close: Optional[float] = None,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO trader_samples "
                "(ts, position_id, asset, direction, amount, entry, expiry, expiration_size, "
                "patterns, trend, hour, source, entry_offset, sec_before_close) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ts,
                    int(position_id),
                    asset,
                    direction,
                    amount,
                    entry,
                    expiry,
                    expiration_size,
                    patterns,
                    trend,
                    hour,
                    source,
                    entry_offset,
                    sec_before_close,
                ),
            )
            return int(cur.lastrowid)

    def update_sample_result(self, position_id: int, result: str, profit: Optional[float]) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE trader_samples SET result = ?, profit = ? WHERE position_id = ?",
                (result, profit, int(position_id)),
            )

    def get_samples(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM trader_samples ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_sample(self, position_id: int) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM trader_samples WHERE position_id = ? ORDER BY id DESC LIMIT 1",
                (int(position_id),),
            ).fetchone()
        return dict(row) if row else None

    def max_sample_id(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM trader_samples").fetchone()
        return int(row[0] or 0)

    def count_auto_trades(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM orders WHERE status = 'auto'").fetchone()
        return int(row[0] or 0)

    def add_learning(self, kind: str, text: str, evidence: str = "") -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO learnings (ts, kind, text, evidence) VALUES (?, ?, ?, ?)",
                (time.time(), kind, text, evidence),
            )
            return int(cur.lastrowid)

    def get_learnings(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, kind, text, evidence FROM learnings ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_learning(self, learning_id: int) -> bool:
        """Supprime une note d'apprentissage et le trade associe (si retrouve)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT evidence FROM learnings WHERE id = ?", (int(learning_id),)
            ).fetchone()
            if not row:
                return False
            try:
                ev = json.loads(row[0] or "{}")
                oid = ev.get("option_id")
                if oid is not None:
                    conn.execute(
                        "DELETE FROM trader_samples WHERE position_id = ?", (int(oid),)
                    )
            except Exception:  # noqa: BLE001
                pass
            conn.execute("DELETE FROM learnings WHERE id = ?", (int(learning_id),))
        return True

    def clear_learnings(self) -> tuple[int, int]:
        with self._connect() as conn:
            a = conn.execute("DELETE FROM learnings").rowcount
            b = conn.execute("DELETE FROM trader_samples").rowcount
        return int(a or 0), int(b or 0)

    def price_at(self, asset: str, ts: float) -> Optional[float]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT price FROM ticks WHERE asset = ? COLLATE NOCASE AND ts <= ? ORDER BY ts DESC LIMIT 1",
                (asset, ts),
            ).fetchone()
        return float(row[0]) if row else None

    # -- reads ----------------------------------------------------------
    def upsert_asset(
        self,
        active_id: int,
        name: str,
        type_: Optional[str] = None,
        precision: Optional[int] = None,
        suspended: int = 0,
        schedule: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO assets (active_id, name, type, precision, suspended, schedule) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(active_id) DO UPDATE SET name = excluded.name, "
                "type = excluded.type, precision = excluded.precision, "
                "suspended = excluded.suspended, schedule = excluded.schedule",
                (int(active_id), name, type_, precision, int(suspended or 0), schedule),
            )

    def get_asset_map(self) -> dict[int, str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT active_id, name FROM assets").fetchall()
        return {int(r[0]): r[1] for r in rows}

    def get_assets(self) -> list[dict[str, Any]]:
        """Catalogue complet des instruments IQ Option connus."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT active_id, name, type, precision, suspended, schedule FROM assets ORDER BY name"
            ).fetchall()
        return [dict(r) for r in rows]

    def latest_tick_asset(self) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute("SELECT asset FROM ticks ORDER BY id DESC LIMIT 1").fetchone()
        return row[0] if row else None

    def get_tick_assets(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT asset FROM ticks ORDER BY asset").fetchall()
        return [r[0] for r in rows]

    def top_tick_asset(self) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT asset FROM ticks GROUP BY asset ORDER BY COUNT(*) DESC LIMIT 1"
            ).fetchone()
        return row[0] if row else None

    def get_recent_ticks(self, asset: str, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, price FROM ticks WHERE asset = ? COLLATE NOCASE ORDER BY id DESC LIMIT ?",
                (asset, limit),
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def get_ticks_range(
        self, asset: str, t0: float, t1: float, limit: int = 600
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT ts, price FROM ticks WHERE asset = ? COLLATE NOCASE AND ts >= ? AND ts <= ? "
                "ORDER BY ts ASC LIMIT ?",
                (asset, t0, t1, limit),
            ).fetchall()
        return [{"ts": r[0], "price": r[1]} for r in rows]

    def get_ticks_since(
        self, last_id: int, asset: Optional[str] = None, limit: int = 500
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if asset:
                rows = conn.execute(
                    "SELECT id, ts, price, asset FROM ticks WHERE id > ? AND asset = ? COLLATE NOCASE ORDER BY id ASC LIMIT ?",
                    (last_id, asset, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, ts, price, asset FROM ticks WHERE id > ? ORDER BY id ASC LIMIT ?",
                    (last_id, limit),
                ).fetchall()
        return [dict(r) for r in rows]

    def get_candles(self, asset: str, timeframe: int, limit: int = 300) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT ts, open, high, low, close, volume FROM candles "
                "WHERE asset = ? COLLATE NOCASE AND timeframe = ? ORDER BY ts DESC LIMIT ?",
                (asset, timeframe, limit),
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def get_candle_assets(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT asset FROM candles").fetchall()
        return [r[0] for r in rows if r[0]]

    def get_recent_candle_assets(self, min_ts: float) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT asset FROM candles WHERE ts >= ?", (float(min_ts),)
            ).fetchall()
        return [r[0] for r in rows if r[0]]

    def get_orders_with_results(self, limit: int = 3000) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, asset, direction, amount, status, note, result, profit "
                "FROM orders WHERE position_id IS NOT NULL ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_candles_from_ticks(self, asset: str, interval: int, limit: int = 200) -> list[dict[str, Any]]:
        """Construit des bougies OHLC a partir des ticks (scalping : 5-60 s)."""
        interval = max(1, int(interval))
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(ts) FROM ticks WHERE asset = ? COLLATE NOCASE", (asset,)).fetchone()
            if not row or row[0] is None:
                return []
            end = float(row[0])
            start = end - interval * limit
            rows = conn.execute(
                "SELECT ts, price FROM ticks WHERE asset = ? COLLATE NOCASE AND ts >= ? ORDER BY ts",
                (asset, start),
            ).fetchall()
        buckets: dict[int, dict[str, Any]] = {}
        for ts, price in rows:
            bucket = int(float(ts) // interval) * interval
            candle = buckets.get(bucket)
            if candle is None:
                buckets[bucket] = {
                    "ts": float(bucket),
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0.0,
                }
            else:
                if price > candle["high"]:
                    candle["high"] = price
                if price < candle["low"]:
                    candle["low"] = price
                candle["close"] = price
        return [buckets[k] for k in sorted(buckets)][-limit:]

    def get_recent_signals(self, asset: str, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, kind, strength, note FROM signals WHERE asset = ? COLLATE NOCASE ORDER BY id DESC LIMIT ?",
                (asset, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            # MAX(id) est instantane (vs COUNT(*) sur des millions de lignes).
            row = conn.execute("SELECT MAX(id) FROM ticks").fetchone()
            ticks = int(row[0] or 0)
            candles = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
            assets = conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        return {
            "ticks": ticks,
            "candles": candles,
            "assets": assets,
        }

    def set_meta(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def get_meta(self, key: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def insert_analysis(
        self,
        asset: str,
        decision: dict[str, Any],
        action: str,
        raw: str = "",
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO analyses (ts, asset, pattern, direction, confidence, explanation, action, raw) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    time.time(),
                    asset,
                    decision.get("pattern_name"),
                    decision.get("direction"),
                    decision.get("confidence"),
                    decision.get("explanation"),
                    action,
                    raw,
                ),
            )
            return int(cur.lastrowid)

    def get_recent_analyses(self, asset: Optional[str] = None, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if asset:
                rows = conn.execute(
                    "SELECT id, ts, asset, pattern, direction, confidence, explanation, action "
                    "FROM analyses WHERE asset = ? COLLATE NOCASE ORDER BY id DESC LIMIT ?",
                    (asset, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, ts, asset, pattern, direction, confidence, explanation, action "
                    "FROM analyses ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [dict(r) for r in rows]

    def insert_strategy(
        self,
        name: str,
        asset: str,
        timeframe: int,
        prompt: str,
        params: dict,
        metrics: dict,
        trades: list,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO strategies (ts, name, asset, timeframe, prompt, params, metrics, trades_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    time.time(),
                    name,
                    asset,
                    timeframe,
                    prompt,
                    json.dumps(params, ensure_ascii=False),
                    json.dumps(metrics, ensure_ascii=False),
                    json.dumps(trades, ensure_ascii=False),
                ),
            )
            return int(cur.lastrowid)

    def get_strategy(self, strategy_id: int) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, ts, name, asset, timeframe, prompt, params, metrics "
                "FROM strategies WHERE id = ?",
                (strategy_id,),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        for key in ("params", "metrics"):
            try:
                item[key] = json.loads(item[key] or "{}")
            except json.JSONDecodeError:
                item[key] = {}
        return item

    def get_strategies(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, name, asset, timeframe, prompt, params, metrics "
                "FROM strategies ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out = []
        for r in rows:
            item = dict(r)
            try:
                item["params"] = json.loads(item["params"] or "{}")
            except json.JSONDecodeError:
                item["params"] = {}
            try:
                item["metrics"] = json.loads(item["metrics"] or "{}")
            except json.JSONDecodeError:
                item["metrics"] = {}
            out.append(item)
        return out
