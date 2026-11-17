from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

TICK_NAMES = ("tick", "quote", "quotes", "spot", "price", "instrument-quote")
CANDLE_NAMES = ("candle", "candles", "ohlc", "history", "chart", "current-candle")
ASSET_MAP_NAMES = ("underlying-list", "underlying", "assets-list")

PRICE_KEYS = ("price", "last", "value", "close", "ask", "bid")
TICK_PRICE_KEYS = ("price", "last", "value", "ask", "bid")
TIME_KEYS = ("time", "timestamp", "ts", "at", "from", "created_at", "t")
ASSET_KEYS = ("asset", "symbol", "pair", "instrument", "active", "active_id", "ticker")
OHLC_KEYS = ("open", "high", "low", "close")

# Au-dela de cette valeur, ce n'est pas un prix : c'est un timestamp (epoch).
MAX_PLAUSIBLE_PRICE = 1e9

# Cles "structurees" : un dict qui en contient une n'est pas un mapping actif->prix.
RESERVED_KEYS = {
    str(k).lower()
    for k in PRICE_KEYS + TIME_KEYS + ASSET_KEYS + OHLC_KEYS + ("volume", "vol", "asset_id", "id", "request_id")
}
SYMBOL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9/_\-]{2,15}$")


def _plausible_price(value: float) -> bool:
    return 0 < value < MAX_PLAUSIBLE_PRICE


def _dedupe_ticks(ticks: list["Tick"]) -> list["Tick"]:
    seen: set[tuple] = set()
    out: list["Tick"] = []
    for tick in ticks:
        key = (tick.asset, round(tick.ts, 3), round(tick.price, 8))
        if key in seen:
            continue
        seen.add(key)
        out.append(tick)
    return out


def _dedupe_candles(candles: list["Candle"]) -> list["Candle"]:
    seen: set[tuple] = set()
    out: list["Candle"] = []
    for candle in candles:
        key = (candle.asset, candle.timeframe, round(candle.ts, 3))
        if key in seen:
            continue
        seen.add(key)
        out.append(candle)
    return out


@dataclass
class Tick:
    asset: str
    ts: float
    price: float


@dataclass
class Candle:
    asset: str
    timeframe: int
    ts: float
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None


@dataclass
class AssetInfo:
    active_id: int
    name: str
    type: Optional[str] = None
    precision: Optional[int] = None
    suspended: bool = False


@dataclass
class ParsedFrame:
    name: Optional[str] = None
    ticks: list[Tick] = field(default_factory=list)
    candles: list[Candle] = field(default_factory=list)
    assets: list[AssetInfo] = field(default_factory=list)


# --------------------------------------------------------------------------
# Decodage des trames brutes (texte JSON ou binaire length-prefixed)
# --------------------------------------------------------------------------
def _json_stream(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    out: list[dict] = []
    idx = 0
    length = len(text)
    while idx < length:
        while idx < length and text[idx] in " \r\n\t":
            idx += 1
        if idx >= length:
            break
        try:
            obj, end = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            break
        idx = end
        if isinstance(obj, dict):
            out.append(obj)
        elif isinstance(obj, list):
            out.extend(item for item in obj if isinstance(item, dict))
    return out


def decode_payload(payload: str | bytes) -> list[dict]:
    """Retourne une liste de messages JSON contenus dans une trame WebSocket."""
    if isinstance(payload, str):
        return _json_stream(payload)

    # 1) la trame entiere est du JSON texte
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        text = None
    if text:
        msgs = _json_stream(text)
        if msgs:
            return msgs

    # 2) format IQ : 4 octets de longueur big-endian puis le JSON
    out: list[dict] = []
    offset = 0
    total = len(payload)
    while offset + 4 <= total:
        length = int.from_bytes(payload[offset : offset + 4], "big")
        if length <= 0 or offset + 4 + length > total:
            break
        chunk = payload[offset + 4 : offset + 4 + length]
        try:
            msgs = _json_stream(chunk.decode("utf-8"))
        except UnicodeDecodeError:
            break
        if not msgs:
            break
        out.extend(msgs)
        offset += 4 + length
    return out


# --------------------------------------------------------------------------
# Parseur heuristique
# --------------------------------------------------------------------------
def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "."))
        except ValueError:
            return None
    return None


def _norm_time(value: Any) -> Optional[float]:
    number = _to_float(value)
    if number is None:
        return None
    if number > 1e12:  # millisecondes
        number /= 1000.0
    return number


def _pick(obj: dict, keys: Iterable[str]) -> Any:
    lowered = {str(k).lower(): v for k, v in obj.items()}
    for key in keys:
        if key in lowered:
            return lowered[key]
    return None


class FrameParser:
    """Parseur tolerant : on ne connait pas a l'avance le schema exact d'IQ.
    On collecte des statistiques sur les noms inconnus pour affiner ensuite."""

    def __init__(self, default_asset: str = "EURUSD", default_timeframe: int = 60) -> None:
        self.default_asset = default_asset.upper()
        self.default_timeframe = default_timeframe
        self.unknown_names: dict[str, int] = {}

    def parse(self, messages: list[dict], context_asset: Optional[str] = None) -> ParsedFrame:
        frame = ParsedFrame()
        for message in messages:
            name = str(message.get("name") or message.get("type") or message.get("topic") or "").lower()
            payload = message.get("msg", message.get("data", message))
            if name:
                frame.name = name

            is_tick = any(token in name for token in TICK_NAMES)
            is_candle = any(token in name for token in CANDLE_NAMES)
            is_asset_map = any(token in name for token in ASSET_MAP_NAMES)
            if name and not is_tick and not is_candle and not is_asset_map:
                self.unknown_names[name] = self.unknown_names.get(name, 0) + 1

            if is_asset_map:
                self._collect_assets(payload, frame)
            elif is_candle:
                self._collect_candles(payload, frame, context_asset)
            elif is_tick:
                self._maybe_flat_quotes(payload, frame)
                self._collect_ticks(payload, frame)
            # sinon : message non marche (profil, positions, trading-params...)
            # -> ignore, sinon on fabrique des faux prix.

        frame.ticks = _dedupe_ticks(frame.ticks)
        frame.candles = _dedupe_candles(frame.candles)
        return frame

    # -- helpers --------------------------------------------------------
    def _walk(self, obj: Any, depth: int = 0):
        if depth > 6:
            return
        if isinstance(obj, dict):
            yield obj
            for value in obj.values():
                yield from self._walk(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                yield from self._walk(item, depth + 1)

    def _maybe_flat_quotes(self, payload: Any, frame: ParsedFrame) -> None:
        """"{EURUSD: 1.08, GBPUSD: 1.26}" -> ticks. Refuse tout dict structure."""
        if not isinstance(payload, dict) or not payload:
            return
        items: list[tuple[str, float]] = []
        for key, value in payload.items():
            name = str(key)
            if name.lower() in RESERVED_KEYS or not SYMBOL_RE.match(name):
                return
            price = _to_float(value)
            if price is None or not _plausible_price(price):
                return
            items.append((name.upper(), price))
        if not items:
            return
        now = time.time()
        for asset, price in items:
            frame.ticks.append(Tick(asset=asset, ts=now, price=price))

    def _asset_of(self, node: dict) -> str:
        raw = _pick(node, ASSET_KEYS)
        if raw is None:
            return self.default_asset
        return str(raw).upper()

    def _node_to_candle(self, node: dict, frame: ParsedFrame, context_asset: Optional[str] = None) -> None:
        # IQ utilise min/max pour low/high.
        lowered = {str(k).lower(): v for k, v in node.items()}
        # L'actif peut venir du noeud lui-meme, ou du contexte de la requete
        # (les reponses "candles"/"first-candles" ne portent pas d'active_id).
        if any(key in lowered for key in ASSET_KEYS):
            asset = self._asset_of(node)
        elif context_asset:
            asset = str(context_asset)
        else:
            return
        open_ = _to_float(lowered.get("open"))
        close = _to_float(lowered.get("close"))
        low = _to_float(lowered.get("low", lowered.get("min")))
        high = _to_float(lowered.get("high", lowered.get("max")))
        if None in (open_, close, low, high):
            return
        if not all(_plausible_price(v) for v in (open_, close, low, high) if v is not None):
            return

        ts = (
            _norm_time(lowered.get("from"))
            or _norm_time(lowered.get("to"))
            or _norm_time(lowered.get("at"))
            or time.time()
        )
        to = _norm_time(lowered.get("to"))
        size_raw = lowered.get("size")
        if size_raw is None:
            size_raw = lowered.get("timeframe")
        timeframe = int(_to_float(size_raw) or 0)
        if not timeframe and to:
            timeframe = max(1, int(round(to - ts)))
        if not timeframe:
            timeframe = self.default_timeframe

        vol_raw = lowered.get("volume")
        if vol_raw is None:
            vol_raw = lowered.get("vol")
        volume = _to_float(vol_raw)

        frame.candles.append(
            Candle(
                asset=asset,
                timeframe=timeframe,
                ts=ts,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
        )
        # Prix live derive de la bougie (candle-generated est pousse en continu).
        # 'at'/'phase' signalent une mise a jour temps reel -> on horodate maintenant.
        live = "at" in lowered or "phase" in lowered
        frame.ticks.append(Tick(asset=asset, ts=time.time() if live else ts, price=close))

    def _node_to_tick(self, node: dict, frame: ParsedFrame) -> None:
        price = _to_float(_pick(node, TICK_PRICE_KEYS))
        if price is None or not _plausible_price(price):
            return
        ts = _norm_time(_pick(node, TIME_KEYS)) or time.time()
        frame.ticks.append(Tick(asset=self._asset_of(node), ts=ts, price=price))

    def _collect_ticks(self, payload: Any, frame: ParsedFrame) -> None:
        for node in self._walk(payload):
            self._node_to_tick(node, frame)

    def _collect_candles(self, payload: Any, frame: ParsedFrame, context_asset: Optional[str] = None) -> None:
        for node in self._walk(payload):
            self._node_to_candle(node, frame, context_asset)

    def _collect_assets(self, payload: Any, frame: ParsedFrame) -> None:
        if isinstance(payload, dict):
            items = payload.get("items") or payload.get("underlying") or payload.get("assets")
        else:
            items = payload
        if not isinstance(items, list):
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            active_id = item.get("active_id")
            name = item.get("name") or item.get("underlying") or item.get("ticker")
            if active_id is None or name is None:
                continue
            try:
                aid = int(active_id)
            except (TypeError, ValueError):
                continue
            frame.assets.append(
                AssetInfo(
                    active_id=aid,
                    name=str(name),
                    type=item.get("active_type"),
                    precision=item.get("precision"),
                    suspended=bool(item.get("is_suspended")),
                )
            )
