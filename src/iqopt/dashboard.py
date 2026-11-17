from __future__ import annotations

import asyncio
import bisect
import json
import os
import re
import secrets
import time
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .ai import analyze_image, analyze_with_fallback, load_ai_config, suggest_method
from .config import Settings, load_settings
from .executor import OrderExecutor
from .pattern_bot import PatternBot
from .replay import build_context, finalize_replay, load_candles, market_summary, plan_replay
from .storage import Database

REPLAY_JOBS: dict[str, dict] = {}
LIVE: dict = {"running": False, "task": None}
PATTERN: dict = {"running": False, "task": None}


class AnalyzeRequest(BaseModel):
    image: str
    prompt: str = ""
    asset: Optional[str] = None
    context: str = ""
    auto_trade: bool = False
    min_confidence: float = 0.7


class ReplayRequest(BaseModel):
    prompt: str
    asset: Optional[str] = None
    timeframe: int = 30
    lookback: int = 200
    horizon: int = 6
    window: int = 40
    step: int = 4
    max_steps: int = 10
    min_confidence: float = 0.6
    save_if_good: bool = True


class SuggestRequest(BaseModel):
    asset: Optional[str] = None
    timeframe: int = 300
    lookback: int = 160


class LiveStartRequest(BaseModel):
    strategy_id: Optional[int] = None
    prompt: Optional[str] = None
    asset: Optional[str] = None
    timeframe: int = 30
    window: int = 40
    min_confidence: float = 0.55
    auto_trade: bool = True
    interval: int = 10


class AIConfigRequest(BaseModel):
    base_url: Optional[str] = None
    model: Optional[str] = None
    api_key: Optional[str] = None


class OrderRequest(BaseModel):
    asset: Optional[str] = None
    direction: str
    amount: float = 1.0
    confirm: Optional[str] = None
    expiry: Optional[int] = None


class AccountRequest(BaseModel):
    mode: str


class SwitchRequest(BaseModel):
    asset: str


class AutoRequest(BaseModel):
    enabled: bool


class AmountRequest(BaseModel):
    amount: float


class PatternStartRequest(BaseModel):
    asset: Optional[str] = None
    source: str = "ticks"
    interval: int = 60
    timeframe: int = 60
    amount: float = 1.0
    min_strength: float = 0.5
    auto_trade: bool = False
    max_trades: int = 0
    cooldown: float = 60.0
    require_trend: bool = False
    allowed: Optional[list] = None
    poll: float = 5.0


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or load_settings()
    db = Database(settings.db_path)
    db.init()

    app = FastAPI(title="IQ Traderoom Live", version="0.1.0")
    static_dir = settings.base_dir / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.middleware("http")
    async def _auth(request, call_next):
        # Jeton optionnel : protege l'acces depuis le telephone / le LAN.
        token = settings.dashboard_token
        if not token:
            return await call_next(request)
        supplied = request.query_params.get("token") or request.cookies.get("iq_token")
        if supplied != token:
            return JSONResponse(
                {"detail": "acces refuse (jeton manquant ou invalide)"}, status_code=401
            )
        response = await call_next(request)
        if request.query_params.get("token") == token:
            response.set_cookie(
                "iq_token", token, max_age=60 * 60 * 24 * 30, httponly=True, samesite="lax"
            )
        return response

    async def _live_loop(params: dict) -> None:
        cfg = load_ai_config(db)
        while LIVE.get("running"):
            try:
                candles = load_candles(db, params["asset"], params["timeframe"], params["window"] + 5)
                if len(candles) < params["window"] + 2:
                    LIVE["last_error"] = "pas assez d'historique"
                else:
                    context = build_context(candles, params["window"])
                    decision = None
                    for _attempt in range(3):
                        try:
                            decision = await asyncio.to_thread(
                                analyze_image, cfg, "", params["prompt"], params["asset"], context
                            )
                            break
                        except Exception as exc:  # noqa: BLE001
                            LIVE["last_error"] = str(exc)[:180]
                            await asyncio.sleep(1)
                    if decision:
                        LIVE["checks"] = LIVE.get("checks", 0) + 1
                        LIVE["last_check"] = time.time()
                        LIVE["last_decision"] = decision
                        LIVE["last_error"] = None
                        direction = str(decision.get("direction") or "NONE").upper()
                        try:
                            confidence = float(decision.get("confidence") or 0)
                        except (TypeError, ValueError):
                            confidence = 0.0
                        if direction in {"BUY", "SELL"} and confidence >= params["min_confidence"]:
                            key = (direction, round(candles[-1]["ts"]))
                            if LIVE.get("last_signal_key") != key:
                                LIVE["last_signal_key"] = key
                                LIVE["last_signal"] = {
                                    "direction": direction,
                                    "confidence": confidence,
                                    "price": candles[-1]["close"],
                                    "ts": time.time(),
                                    "reason": str(decision.get("explanation") or "")[:180],
                                }
                                db.insert_signal(
                                    params["asset"],
                                    direction,
                                    confidence,
                                    str(decision.get("explanation") or ""),
                                )
                                if params["auto_trade"]:
                                    executor = OrderExecutor(settings=settings, db=db)
                                    amount = float(os.getenv("IQ_TRADE_AMOUNT", "1"))
                                    order_id = executor.submit(params["asset"], direction, amount)
                                    LIVE["orders"] = LIVE.get("orders", 0) + 1
                                    LIVE["last_order"] = order_id
            except Exception as exc:  # noqa: BLE001
                LIVE["last_error"] = str(exc)[:180]
            await asyncio.sleep(max(10, params["interval"]))

    async def _pattern_loop(bot: PatternBot, poll: float) -> None:
        while PATTERN.get("running"):
            try:
                decision = await asyncio.to_thread(bot.step)
                PATTERN["checks"] = PATTERN.get("checks", 0) + 1
                PATTERN["trades"] = bot._trades
                PATTERN["last_bar_ts"] = bot._last_bar_ts
                PATTERN["last_error"] = None
                if decision is not None:
                    PATTERN["events"] = PATTERN.get("events", 0) + 1
                    PATTERN["last_decision"] = {
                        "direction": decision.direction,
                        "pattern": decision.pattern,
                        "strength": round(decision.strength, 3),
                        "note": decision.note,
                        "ts": time.time(),
                    }
            except Exception as exc:  # noqa: BLE001
                PATTERN["last_error"] = str(exc)[:180]
            await asyncio.sleep(max(1.0, poll))

    async def _run_replay_job(job_id: str, asset: str, req: ReplayRequest) -> None:
        job = REPLAY_JOBS[job_id]
        try:
            cfg = load_ai_config(db)

            def decide(context: str) -> dict:
                return analyze_image(cfg, "", req.prompt, asset, context)

            plan = await asyncio.to_thread(
                plan_replay,
                db,
                asset,
                req.timeframe,
                req.prompt,
                req.lookback,
                req.horizon,
                req.window,
                req.step,
                req.max_steps,
            )
            contexts = plan["contexts"]
            job["total"] = len(contexts)
            job["progress"] = 0
            semaphore = asyncio.Semaphore(4)

            async def one(context: str):
                async with semaphore:
                    last_error: Optional[Exception] = None
                    for _attempt in range(3):
                        try:
                            result = await asyncio.to_thread(decide, context)
                            job["progress"] = job.get("progress", 0) + 1
                            return result
                        except Exception as exc:  # noqa: BLE001
                            last_error = exc
                            await asyncio.sleep(0.4)
                    job["progress"] = job.get("progress", 0) + 1
                    return last_error

            decisions = await asyncio.gather(
                *(one(context) for context in contexts), return_exceptions=True
            )
            result = finalize_replay(plan, list(decisions), req.min_confidence)
            metrics = result["metrics"]
            good = bool(
                metrics["trades"] >= 5 and metrics["win_rate"] >= 55 and metrics["total_pnl_pct"] > 0
            )
            result["good"] = good
            result["saved_strategy_id"] = None
            if good and req.save_if_good:
                result["saved_strategy_id"] = db.insert_strategy(
                    result["method_name"] or "Méthode",
                    asset,
                    req.timeframe,
                    req.prompt,
                    {
                        "lookback": req.lookback,
                        "horizon": req.horizon,
                        "window": req.window,
                        "step": req.step,
                        "max_steps": req.max_steps,
                        "min_confidence": req.min_confidence,
                    },
                    metrics,
                    result["trades"],
                )
            job.update(status="done", result=result, progress=req.max_steps)
        except Exception as exc:  # noqa: BLE001
            job.update(status="error", error=str(exc))

    @app.get("/")
    async def index():
        return FileResponse(
            static_dir / "index.html",
            headers={"Cache-Control": "no-store, max-age=0"},
        )

    _stats_cache = {"t": 0.0, "v": {}}

    def _stats_cached() -> dict:
        now = time.time()
        if now - _stats_cache["t"] > 5 or not _stats_cache["v"]:
            _stats_cache["v"] = db.stats()
            _stats_cache["t"] = now
        return _stats_cache["v"]

    @app.get("/api/status")
    def status():
        # Suivre l'actif qui recoit des ticks EN CE MOMENT (temps reel),
        # pas celui qui a le plus d'historique accumule.
        focus = db.latest_tick_asset() or db.top_tick_asset() or settings.asset
        names = set(db.get_tick_assets())
        names.add(focus)
        now = time.time()
        live_assets = []
        for name in names:
            recent = db.get_recent_ticks(name, 1)
            if not recent:
                continue
            age = now - recent[-1]["ts"]
            if age <= 180:
                live_assets.append({"name": name, "age": round(age, 1)})
        live_assets.sort(key=lambda item: item["age"])
        return JSONResponse(
            {
                "settings": {
                    "asset": settings.asset,
                    "traderoom_url": settings.traderoom_url,
                    "execution_enabled": settings.enable_execution,
                    "demo_only": settings.demo_only,
                },
                "stats": _stats_cached(),
                "asset_names": sorted(names),
                "focus_asset": focus,
                "live_assets": live_assets,
            }
        )

    @app.get("/api/candles")
    async def candles(asset: Optional[str] = None, timeframe: int = 60, limit: int = 300):
        asset = (asset or settings.asset).upper()
        return {"asset": asset, "candles": db.get_candles(asset, timeframe, limit)}

    @app.get("/api/assets")
    def assets_catalog():
        """Catalogue complet des paires/instruments IQ Option (table assets)."""
        return {"assets": db.get_assets()}

    @app.get("/api/ticks")
    async def ticks(asset: Optional[str] = None, limit: int = 200):
        asset = (asset or settings.asset).upper()
        return {"asset": asset, "ticks": db.get_recent_ticks(asset, limit)}

    @app.get("/api/signals")
    def signals(asset: Optional[str] = None, limit: int = 20):
        asset = (asset or settings.asset).upper()
        return {"asset": asset, "signals": db.get_recent_signals(asset, limit)}

    @app.delete("/api/signals")
    async def clear_signals(asset: Optional[str] = None):
        deleted = db.clear_signals(asset.upper() if asset else None)
        return {"deleted": deleted}

    @app.post("/api/order")
    async def place_order(req: OrderRequest):
        direction = (req.direction or "").upper()
        if direction not in {"BUY", "SELL"}:
            raise HTTPException(status_code=400, detail="direction doit etre BUY ou SELL")
        try:
            amount = float(req.amount)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="montant invalide")
        max_amount = float(os.getenv("IQ_MAX_ORDER_AMOUNT", "100"))
        if amount <= 0 or amount > max_amount:
            raise HTTPException(
                status_code=400, detail=f"montant hors limites (0 < montant <= {max_amount})"
            )
        if req.confirm != "YES":
            raise HTTPException(status_code=400, detail="confirmation requise (confirm=YES)")
        asset = (req.asset or settings.asset).upper()
        req_expiry = int(req.expiry) if req.expiry else None
        order_id = db.insert_order(
            asset, direction, amount, "queued", "depuis dashboard", req_expiry
        )
        return {
            "order_id": order_id,
            "status": "queued",
            "asset": asset,
            "direction": direction,
            "amount": amount,
            "expiry": req_expiry,
            "execution_enabled": settings.enable_execution,
            "demo_only": settings.demo_only,
        }

    # ---- Webhook d'alertes indicateur (ex: AGS) -------------------------
    def _alert_token() -> str:
        tok = db.get_meta("alert_token") or ""
        if not tok:
            tok = secrets.token_urlsafe(18)
            db.set_meta("alert_token", tok)
        return tok

    def _norm_asset(raw: str) -> str:
        s = str(raw or "").upper()
        for ch in ("/", " ", "-", "_", "."):
            s = s.replace(ch, "")
        return s

    def _resolve_asset(raw: str) -> str:
        target = _norm_asset(raw)
        if not target:
            return (settings.asset or "").upper()
        try:
            rows = db.get_assets()
        except Exception:  # noqa: BLE001
            rows = []

        def binary(r) -> bool:
            return "marginal" not in str(r.get("type") or "").lower()

        cands: list = []
        for r in rows:
            nm = r.get("name")
            if nm and binary(r) and _norm_asset(nm) == target:
                cands.append(nm)
        for r in rows:
            nm = r.get("name")
            if not nm or not binary(r):
                continue
            base = _norm_asset(nm)
            for suf in ("OTC", "OP"):
                if base == target + suf:
                    cands.append(nm)
        cands = list(dict.fromkeys(cands))
        if not cands:
            return str(raw).upper()
        best, best_ts = cands[0], -1.0
        for nm in cands:
            try:
                tk = db.get_recent_ticks(nm, 1)
                ts = float(tk[-1]["ts"]) if tk else -1.0
            except Exception:  # noqa: BLE001
                ts = -1.0
            if ts > best_ts:
                best_ts, best = ts, nm
        if best_ts < time.time() - 180:
            otc = [c for c in cands if c.endswith("-OTC")]
            if otc:
                return otc[0]
        return best

    def _norm_direction(raw: str) -> str:
        s = str(raw or "").strip().lower()
        if s in ("buy", "call", "long", "up", "achat", "haut", "higher", "hausse"):
            return "BUY"
        if s in ("sell", "put", "short", "down", "vente", "bas", "lower", "baisse"):
            return "SELL"
        return ""

    def _queue_alert(asset_raw, direction_raw, amount_raw, expiry_raw, token) -> dict:
        if token != _alert_token():
            raise HTTPException(status_code=403, detail="token invalide")
        direction = _norm_direction(direction_raw)
        if not direction:
            raise HTTPException(status_code=400, detail="direction invalide (buy/sell)")
        asset = _resolve_asset(asset_raw)
        try:
            amount = (
                float(amount_raw)
                if amount_raw not in (None, "")
                else float(db.get_meta("auto_amount") or 1)
            )
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="montant invalide")
        max_amount = float(os.getenv("IQ_MAX_ORDER_AMOUNT", "100"))
        if amount <= 0 or amount > max_amount:
            raise HTTPException(
                status_code=400, detail=f"montant hors limites (0 < montant <= {max_amount})"
            )
        try:
            expiry = int(expiry_raw) if expiry_raw not in (None, "") else None
        except (TypeError, ValueError):
            expiry = None
        order_id = db.insert_order(
            asset, direction, amount, "queued", "alerte indicateur", expiry
        )
        return {
            "status": "queued",
            "order_id": order_id,
            "asset": asset,
            "direction": direction,
            "amount": amount,
            "expiry": expiry,
        }

    @app.get("/api/alert/info")
    async def alert_info(request: Request):
        base = str(request.base_url).rstrip("/")
        tok = _alert_token()
        return {
            "url": f"{base}/api/alert",
            "token": tok,
            "exemple": (
                f"{base}/api/alert?token={tok}&asset=GBPUSD&direction=sell"
                "&expiry=300&amount=1"
            ),
        }

    def _pick(data: dict, q, *keys):
        for k in keys:
            v = data.get(k)
            if v is not None and v != "":
                return v
        for k in keys:
            v = q.get(k)
            if v is not None and v != "":
                return v
        return None

    @app.get("/api/alert")
    async def alert_get(request: Request):
        q = request.query_params
        return _queue_alert(
            _pick({}, q, "asset", "symbol", "ticker", "pair"),
            _pick({}, q, "direction", "action", "side", "signal"),
            _pick({}, q, "amount", "size", "stake"),
            _pick({}, q, "expiry", "duration", "expiration"),
            _pick({}, q, "token", "secret") or "",
        )

    @app.post("/api/alert")
    async def alert_post(request: Request):
        q = request.query_params
        data: dict = {}
        try:
            body = await request.body()
        except Exception:  # noqa: BLE001
            body = b""
        if body:
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    data = parsed
            except Exception:  # noqa: BLE001
                try:
                    from urllib.parse import parse_qs

                    form = parse_qs(body.decode("utf-8", "ignore"))
                    data = {k: v[0] for k, v in form.items() if v}
                except Exception:  # noqa: BLE001
                    data = {}
        return _queue_alert(
            _pick(data, q, "asset", "symbol", "ticker", "pair"),
            _pick(data, q, "direction", "action", "side", "signal"),
            _pick(data, q, "amount", "size", "stake"),
            _pick(data, q, "expiry", "duration", "expiration"),
            _pick(data, q, "token", "secret") or "",
        )

    @app.get("/api/settings/amount")
    def get_amount():
        try:
            v = float(db.get_meta("auto_amount") or 1)
        except (TypeError, ValueError):
            v = 1.0
        return {"amount": v}

    @app.post("/api/settings/amount")
    async def set_amount(req: AmountRequest):
        try:
            amount = float(req.amount)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="montant invalide")
        if amount <= 0:
            raise HTTPException(status_code=400, detail="montant invalide")
        db.set_meta("auto_amount", str(round(amount, 2)))
        return {"amount": round(amount, 2)}

    @app.get("/api/orders")
    async def orders(limit: int = 20):
        return {"orders": db.get_recent_orders(limit)}

    @app.get("/api/account")
    def get_account():
        mode = (db.get_meta("order_account") or "demo").lower()
        balances = []
        try:
            for b in json.loads(db.get_meta("balances") or "[]"):
                balances.append(
                    {
                        "id": b.get("id"),
                        "type": b.get("type"),
                        "amount": b.get("amount"),
                        "currency": b.get("currency"),
                    }
                )
        except Exception:  # noqa: BLE001
            pass
        return {"mode": mode, "balances": balances, "real_allowed": True}

    @app.post("/api/account")
    async def set_account(req: AccountRequest):
        mode = (req.mode or "").lower()
        if mode not in ("demo", "real"):
            raise HTTPException(status_code=400, detail="mode doit etre demo ou real")
        db.set_meta("order_account", mode)
        return {"mode": mode}

    @app.get("/api/learnings")
    def learnings(limit: int = 30):
        return {"learnings": db.get_learnings(limit)}

    @app.get("/api/methods")
    def methods():
        try:
            items = json.loads(db.get_meta("active_methods") or "[]")
        except Exception:  # noqa: BLE001
            items = []
        return {"methods": items}

    @app.get("/api/eval-rules")
    async def eval_rules(limit: int = 1200, recent_hours: int = 72):
        from .patterns import (
            PatternConfig,
            detect_bearish_hammer,
            detect_bullish_inside,
            detect_green_upper_pin,
            to_bars,
        )

        cfg = PatternConfig()
        rules = {
            "bullish_inside": (detect_bullish_inside, "SELL", 300),
            "bearish_hammer": (detect_bearish_hammer, "BUY", 300),
            "green_upper_pin": (detect_green_upper_pin, "SELL", 300),
        }
        bt = {
            k: {"trades": 0, "wins": 0, "losses": 0, "net": 0.0, "staked": 0.0}
            for k in rules
        }
        cutoff = time.time() - max(1, int(recent_hours)) * 3600
        try:
            assets = db.get_recent_candle_assets(cutoff)[:120]
        except Exception:  # noqa: BLE001
            assets = []
        for asset in assets:
            try:
                candles = db.get_candles(asset, 300, int(limit))
            except Exception:  # noqa: BLE001
                continue
            candles = [c for c in candles if float(c.get("ts") or 0) >= cutoff]
            if len(candles) < 3:
                continue
            try:
                bars = to_bars(candles)
            except Exception:  # noqa: BLE001
                continue
            for i in range(1, len(bars) - 1):
                window = bars[max(0, i - 1) : i + 1]
                entry = bars[i].close
                nxt = bars[i + 1].close
                for name, (fn, dirn, _exp) in rules.items():
                    try:
                        hits = fn(window, cfg)
                    except Exception:  # noqa: BLE001
                        hits = []
                    if not hits:
                        continue
                    s = bt[name]
                    s["trades"] += 1
                    s["staked"] += 1.0
                    win = (nxt < entry) if dirn == "SELL" else (nxt > entry)
                    if win:
                        s["wins"] += 1
                        s["net"] += 0.84
                    else:
                        s["losses"] += 1
                        s["net"] -= 1.0

        # Live : trades reels tagues par regle (note = pattern=<regle>)
        live = {k: {"trades": 0, "wins": 0, "losses": 0, "net": 0.0} for k in rules}
        try:
            orders = db.get_orders_with_results(3000)
        except Exception:  # noqa: BLE001
            orders = []
        for o in orders:
            mm = re.search(r"pattern=([a-z_]+)", str(o.get("note") or ""))
            if not mm:
                continue
            name = mm.group(1)
            if name not in rules:
                continue
            res = str(o.get("result") or "").lower()
            if res not in ("win", "won", "loose", "lost", "loss"):
                continue
            amt = float(o.get("amount") or 1.0)
            lv = live[name]
            lv["trades"] += 1
            if res in ("win", "won"):
                lv["wins"] += 1
                p = o.get("profit")
                gain = (float(p) - amt) if p is not None else amt * 0.84
                lv["net"] += gain
            else:
                lv["losses"] += 1
                lv["net"] -= amt

        out = []
        for name, (_fn, dirn, exp) in rules.items():
            b = bt[name]
            done = b["wins"] + b["losses"]
            lv = live[name]
            ldone = lv["wins"] + lv["losses"]
            out.append(
                {
                    "name": name,
                    "direction": dirn,
                    "expiry": exp,
                    "backtest": {
                        "trades": b["trades"],
                        "wins": b["wins"],
                        "losses": b["losses"],
                        "rate": round(100 * b["wins"] / done, 1) if done else 0,
                        "net": round(b["net"], 2),
                        "roi": round(100 * b["net"] / b["staked"], 1) if b["staked"] else 0,
                    },
                    "live": {
                        "trades": lv["trades"],
                        "wins": lv["wins"],
                        "losses": lv["losses"],
                        "rate": round(100 * lv["wins"] / ldone, 1) if ldone else 0,
                        "net": round(lv["net"], 2),
                    },
                }
            )
        return {"rules": out, "assets": len(assets)}

    @app.get("/api/eval-rules-horizons")
    async def eval_rules_horizons(
        recent_hours: int = 24, horizons: str = "30,60,120,300", assets_cap: int = 40
    ):
        from .patterns import (
            PatternConfig,
            detect_bearish_hammer,
            detect_bullish_inside,
            detect_green_upper_pin,
            to_bars,
        )

        cfg = PatternConfig()
        rules = {
            "bullish_inside": (detect_bullish_inside, "SELL"),
            "bearish_hammer": (detect_bearish_hammer, "BUY"),
            "green_upper_pin": (detect_green_upper_pin, "SELL"),
        }
        try:
            hs = sorted({int(x) for x in str(horizons).split(",") if x.strip()})
        except ValueError:
            hs = [30, 60, 120, 300]
        cutoff = time.time() - max(1, int(recent_hours)) * 3600
        try:
            assets = db.get_recent_candle_assets(cutoff)[: max(1, int(assets_cap))]
        except Exception:  # noqa: BLE001
            assets = []

        stats = {
            name: {h: {"trades": 0, "wins": 0, "net": 0.0, "staked": 0.0} for h in hs}
            for name in rules
        }
        for asset in assets:
            try:
                candles = db.get_candles(asset, 300, 1200)
            except Exception:  # noqa: BLE001
                continue
            candles = [c for c in candles if float(c.get("ts") or 0) >= cutoff]
            if len(candles) < 3:
                continue
            try:
                bars = to_bars(candles)
            except Exception:  # noqa: BLE001
                continue
            t0 = float(candles[0]["ts"])
            t1 = float(candles[-1]["ts"]) + 300 + max(hs) + 10
            try:
                ticks = db.get_ticks_range(asset, t0, t1, 400000)
            except Exception:  # noqa: BLE001
                ticks = []
            if not ticks:
                continue
            tss = [float(t["ts"]) for t in ticks]
            prs = [float(t["price"]) for t in ticks]
            for i in range(1, len(bars) - 1):
                window = bars[max(0, i - 1) : i + 1]
                entry_ts = float(candles[i]["ts"]) + 300.0  # cloture de la bougie signal
                entry = bars[i].close
                for name, (fn, dirn) in rules.items():
                    try:
                        if not fn(window, cfg):
                            continue
                    except Exception:  # noqa: BLE001
                        continue
                    for h in hs:
                        target = entry_ts + h
                        idx = bisect.bisect_left(tss, target)
                        if idx >= len(tss):
                            continue
                        px = prs[idx]
                        win = (px < entry) if dirn == "SELL" else (px > entry)
                        s = stats[name][h]
                        s["trades"] += 1
                        s["staked"] += 1.0
                        if win:
                            s["wins"] += 1
                            s["net"] += 0.84
                        else:
                            s["net"] -= 1.0

        out = []
        for name, (_fn, dirn) in rules.items():
            hout = {}
            for h in hs:
                s = stats[name][h]
                done = s["trades"]
                hout[str(h)] = {
                    "trades": s["trades"],
                    "wins": s["wins"],
                    "rate": round(100 * s["wins"] / done, 1) if done else 0,
                    "net": round(s["net"], 2),
                    "roi": round(100 * s["net"] / s["staked"], 1) if s["staked"] else 0,
                }
            out.append({"name": name, "direction": dirn, "horizons": hout})
        return {"rules": out, "assets": len(assets), "horizons": hs}

    @app.get("/api/observations")
    def observations(limit: int = 20):
        out = []
        for s in db.get_samples(limit):
            out.append(
                {
                    "position_id": s.get("position_id"),
                    "asset": s.get("asset"),
                    "direction": s.get("direction"),
                    "amount": s.get("amount"),
                    "patterns": s.get("patterns"),
                    "result": s.get("result"),
                    "profit": s.get("profit"),
                    "sec_before_close": s.get("sec_before_close"),
                    "entry_offset": s.get("entry_offset"),
                    "source": s.get("source"),
                    "ts": s.get("ts"),
                }
            )
        return {"observations": out}

    @app.delete("/api/learnings")
    async def clear_learnings():
        a, b = db.clear_learnings()
        return {"deleted_learnings": a, "deleted_samples": b}

    @app.delete("/api/learnings/{learning_id}")
    async def delete_learning(learning_id: int):
        if not db.delete_learning(learning_id):
            raise HTTPException(status_code=404, detail="apprentissage introuvable")
        return {"deleted": True}

    @app.get("/api/learn/auto")
    def get_auto():
        from .learner import build_strategy

        enabled = (db.get_meta("learn_auto") or "0") == "1"
        tradable = {
            str(a.get("name") or "").upper()
            for a in db.get_assets()
            if not str(a.get("type") or "").lower().startswith("marginal")
        }
        def age(key):
            try:
                v = float(db.get_meta(key) or 0)
            except (TypeError, ValueError):
                return None
            return round(time.time() - v) if v > 0 else None

        monitored = [a for a in (db.get_meta("auto_monitored") or "").split(",") if a]
        # Retirer les paires suspendues ; elles reviennent quand elles redeviennent actives.
        try:
            suspended = {
                str(r.get("name") or "").upper()
                for r in db.get_assets()
                if r.get("suspended")
            }
        except Exception:  # noqa: BLE001
            suspended = set()
        if suspended:
            monitored = [a for a in monitored if str(a).upper() not in suspended]
        try:
            _scan = json.loads(db.get_meta("auto_scan") or "[]")
        except Exception:  # noqa: BLE001
            _scan = []
        if suspended and _scan:
            _scan = [
                x for x in _scan if str(x.get("asset") or "").upper() not in suspended
            ]
        return {
            "enabled": enabled,
            "strategy": build_strategy(db.get_samples(3000), tradable_assets=tradable),
            "heartbeat_age": age("auto_heartbeat"),
            "last_check_age": age("auto_last_check"),
            "last_trade_age": age("auto_last_trade_ts"),
            "monitored": monitored,
            "last_signal": db.get_meta("auto_last_signal") or None,
            "signal": db.get_meta("auto_signal") or None,
            "signal_age": age("auto_signal_ts"),
            "error": db.get_meta("auto_last_error") or None,
            "error_age": age("auto_last_error_ts"),
            "auto_trades": db.count_auto_trades(),
            "scan": _scan,
            "scan_age": age("auto_scan_ts"),
            "learn_trades": (
                int(db.get_meta("learn_trades") or 0)
                if str(db.get_meta("learn_trades") or "0").isdigit()
                else 0
            ),
            "learn_age": age("learn_last_update"),
        }

    @app.post("/api/learn/auto")
    async def set_auto(req: AutoRequest):
        db.set_meta("learn_auto", "1" if req.enabled else "0")
        return {"enabled": bool(req.enabled)}

    @app.get("/api/trader-profile")
    def trader_profile(limit: int = 5000):
        from .learner import BLACKLIST

        # Apprentissage = uniquement les trades reussis.
        samples = [
            s
            for s in db.get_samples(limit)
            if str(s.get("result") or "").lower() in ("win", "won")
        ]
        wins = len(samples)
        # Compter aussi les PERDANTS (sur tous les trades clotures).
        all_rows = db.get_samples(limit)
        losses = sum(
            1
            for s in all_rows
            if str(s.get("result") or "").lower() in ("loose", "lost", "loss")
        )
        closed_all = wins + losses
        buys = sells = 0
        net = 0.0
        ratios: list = []
        assets: dict = {}
        pats: dict = {}
        hours: dict = {}
        exps: dict = {}
        amounts = []
        for s in samples:
            amt = float(s.get("amount") or 0)
            p = s.get("profit")
            gain = (float(p) - amt) if p is not None else amt * 0.84
            net += gain
            if amt:
                ratios.append(gain / amt)
            d = str(s.get("direction") or "").upper()
            if d in ("CALL", "BUY"):
                buys += 1
            elif d in ("PUT", "SELL"):
                sells += 1
            a = s.get("asset") or "?"
            assets[a] = assets.get(a, 0) + 1
            for tok in str(s.get("patterns") or "").split(","):
                tok = tok.strip()
                if tok and tok not in BLACKLIST:
                    pats[tok] = pats.get(tok, 0) + 1
            h = s.get("hour")
            if h is not None:
                hours[str(h)] = hours.get(str(h), 0) + 1
            if amt:
                amounts.append(amt)
            es = s.get("expiration_size")
            if es:
                exps[str(es)] = exps.get(str(es), 0) + 1

        def top(d: dict, n: int = 3):
            return [k for k, _ in sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]]

        # Timing d'entree : moyenne sur TOUS les trades (pas seulement les gains),
        # pour comprendre A QUEL MOMENT l'utilisateur entre dans la bougie.
        timing = [
            float(s["sec_before_close"])
            for s in db.get_samples(limit)
            if s.get("sec_before_close") is not None
        ]

        return {
            "total": len(samples),
            "wins": wins,
            "losses": losses,
            "closed": closed_all,
            "rate": round(100 * wins / closed_all, 1) if closed_all else 0,
            "profit": round(net, 2),
            "avg_roi": round(100 * sum(ratios) / len(ratios), 2) if ratios else 0,
            "assets": top(assets),
            "patterns": top(pats, 5),
            "hours": top(hours, 3),
            "expirations": top(exps, 3),
            "direction": {"buy": buys, "sell": sells},
            "avg_amount": round(sum(amounts) / len(amounts), 2) if amounts else 0,
            "entry_before_close": round(sum(timing) / len(timing), 1) if timing else None,
            "timing_count": len(timing),
        }

    @app.get("/api/evaluate")
    async def evaluate(limit: int = 2000):
        samples = db.get_samples(limit)

        def agg(rows):
            wins = losses = 0
            net = 0.0
            staked = 0.0
            ratios = []
            for s in rows:
                r = str(s.get("result") or "").lower()
                amt = float(s.get("amount") or 0)
                p = s.get("profit")
                if r in ("win", "won"):
                    wins += 1
                    gain = (float(p) - amt) if p is not None else amt * 0.84
                    net += gain
                    staked += amt
                    if amt:
                        ratios.append(gain / amt)
                elif r in ("loose", "lost", "loss"):
                    losses += 1
                    net -= amt
                    staked += amt
                    if amt:
                        ratios.append(-1.0)
            done = wins + losses
            return {
                "trades": done,
                "wins": wins,
                "losses": losses,
                "rate": round(100 * wins / done) if done else 0,
                "net": round(net, 2),
                "staked": round(staked, 2),
                "roi": round(100 * net / staked, 2) if staked else 0,
                "avg_roi": round(100 * sum(ratios) / len(ratios), 2) if ratios else 0,
            }

        def group(rows, keyfn):
            g = {}
            for s in rows:
                for k in keyfn(s):
                    if not k:
                        continue
                    g.setdefault(k, []).append(s)
            out = []
            for k, rows2 in g.items():
                a = agg(rows2)
                a["name"] = k
                out.append(a)
            # classement par ROI moyen par trade (equipondere)
            out.sort(key=lambda x: (x["avg_roi"], x["rate"]), reverse=True)
            return out

        def pats(s):
            return [t.strip() for t in str(s.get("patterns") or "").split(",") if t.strip()]

        def src(s):
            return [str(s.get("source") or "?")]

        def direction(s):
            d = str(s.get("direction") or "").upper()
            if d in ("CALL", "BUY"):
                return ["BUY"]
            if d in ("PUT", "SELL"):
                return ["SELL"]
            return []

        def asset(s):
            return [str(s.get("asset") or "?")]

        def hour(s):
            return [str(s.get("hour"))] if s.get("hour") is not None else []

        from .learner import BLACKLIST

        all_by_pattern = group(samples, pats)
        eligible = {
            x["name"]
            for x in all_by_pattern
            if x["avg_roi"] > 0 and x["name"] not in BLACKLIST
        }
        # On recalcule sur les trades utilisant UNIQUEMENT les methodes positives.
        filtered = [s for s in samples if set(pats(s)) & eligible]
        by_pattern = [x for x in all_by_pattern if x["name"] in eligible]
        best = next((p for p in by_pattern if p["trades"] >= 5), None)
        if best is None:
            best = next((p for p in by_pattern if p["trades"] >= 3), None)

        return {
            "overall": agg(filtered),
            "by_pattern": by_pattern,
            "by_source": group(filtered, src),
            "by_direction": group(filtered, direction),
            "by_asset": group(filtered, asset),
            "by_hour": group(filtered, hour),
            "best": best,
            "eligible": sorted(eligible),
        }

    @app.get("/api/simulate")
    async def simulate(stake: float = 10.0, limit: int = 2000):
        from .learner import BLACKLIST, pattern_stats

        if stake <= 0:
            stake = 10.0
        stake = round(min(stake, 1_000_000.0), 2)
        samples = db.get_samples(limit)
        stats = pattern_stats(samples)
        eligible = {
            n for n, v in stats.items() if v["avg_roi"] > 0 and n not in BLACKLIST
        }

        def toks(s):
            return [t.strip() for t in str(s.get("patterns") or "").split(",") if t.strip()]

        filtered = [s for s in samples if set(toks(s)) & eligible]

        def ratio(s):
            r = str(s.get("result") or "").lower()
            amt = float(s.get("amount") or 0)
            p = s.get("profit")
            if r in ("win", "won"):
                if p is not None and amt:
                    return (float(p) - amt) / amt
                return 0.84
            if r in ("loose", "lost", "loss"):
                return -1.0
            return None

        wins = losses = 0
        sum_ratio = 0.0
        per: dict = {}
        for s in filtered:
            rr = ratio(s)
            if rr is None:
                continue
            if rr >= 0:
                wins += 1
            else:
                losses += 1
            sum_ratio += rr
            for t in toks(s):
                if t in eligible:
                    d = per.setdefault(t, {"sum": 0.0, "wins": 0, "losses": 0})
                    d["sum"] += rr
                    if rr >= 0:
                        d["wins"] += 1
                    else:
                        d["losses"] += 1

        done = wins + losses
        by_pattern = []
        for name, d in per.items():
            n = d["wins"] + d["losses"]
            by_pattern.append(
                {
                    "name": name,
                    "trades": n,
                    "rate": round(100 * d["wins"] / n) if n else 0,
                    "net": round(stake * d["sum"], 2),
                    "avg_roi": round(100 * d["sum"] / n, 2) if n else 0,
                }
            )
        by_pattern.sort(key=lambda x: x["net"], reverse=True)

        return {
            "stake": stake,
            "trades": done,
            "wins": wins,
            "losses": losses,
            "rate": round(100 * wins / done) if done else 0,
            "net": round(stake * sum_ratio, 2),
            "roi_pct": round(100 * sum_ratio / done, 2) if done else 0,
            "by_pattern": by_pattern,
        }

    @app.post("/api/switch")
    async def switch_asset(req: SwitchRequest):
        asset = (req.asset or "").upper()
        if not asset:
            raise HTTPException(status_code=400, detail="asset requis")
        order_id = db.insert_order(asset, "SWITCH", 0.0, "queued", "depuis dashboard")
        return {"order_id": order_id}

    @app.delete("/api/positions")
    async def clear_positions():
        # On masque les resultats affiches sans effacer l'apprentissage/evaluation.
        upto = db.max_sample_id()
        db.set_meta("results_cleared_id", str(upto))
        return {"cleared_upto": upto}

    @app.get("/api/positions")
    def positions(limit: int = 200):
        samples = db.get_samples(max(int(limit), 3000))
        try:
            cleared = int(db.get_meta("results_cleared_id") or 0)
        except (TypeError, ValueError):
            cleared = 0
        samples = [s for s in samples if int(s.get("id") or 0) > cleared]
        # Ne montrer QUE les trades issus d'un ordre confirme (pas l'historique).
        try:
            confirmed = db.get_order_positions()
        except Exception:  # noqa: BLE001
            confirmed = set()
        if confirmed:
            samples = [
                s for s in samples if int(s.get("position_id") or 0) in confirmed
            ]
        # Ignorer les trades sans methode (pattern vide).
        samples = [s for s in samples if str(s.get("patterns") or "").strip()]
        now = time.time()
        out = []
        wins = losses = pending = 0
        total_gain = 0.0
        daily = {}

        def ndir(d):
            d = str(d or "").upper()
            if d in ("CALL", "BUY"):
                return "BUY"
            if d in ("PUT", "SELL"):
                return "SELL"
            return d

        for s in samples:
            asset = s.get("asset")
            exp = s.get("expiry")
            entry = s.get("entry")
            exit_price = db.price_at(asset, exp) if (asset and exp) else None
            official = str(s.get("result") or "").lower()
            direction = ndir(s.get("direction"))
            if official in ("win", "won"):
                outcome = "WIN"
            elif official in ("loose", "lost", "loss"):
                outcome = "LOSS"
            elif official in ("opened", "open"):
                outcome = "PENDING" if (exp and exp > now) else "?"
            elif exp and exp > now:
                outcome = "PENDING"
            elif entry is None or exit_price is None:
                outcome = "?"
            elif direction == "BUY":
                outcome = "WIN" if exit_price > entry else "LOSS"
            else:
                outcome = "WIN" if exit_price < entry else "LOSS"
            amount = float(s.get("amount") or 0)
            profit = s.get("profit")
            gain = None
            if outcome == "WIN":
                gain = (float(profit) - amount) if profit is not None else amount * 0.84
            elif outcome == "LOSS":
                gain = -amount
            if outcome == "WIN":
                wins += 1
            elif outcome == "LOSS":
                losses += 1
            elif outcome == "PENDING":
                pending += 1
            if gain is not None:
                total_gain += gain
            if outcome in ("WIN", "LOSS"):
                try:
                    day = time.strftime("%Y-%m-%d", time.localtime(float(s.get("ts") or 0)))
                except (TypeError, ValueError, OSError):
                    day = "?"
                dg = daily.setdefault(
                    day, {"trades": 0, "wins": 0, "losses": 0, "net": 0.0}
                )
                dg["trades"] += 1
                if outcome == "WIN":
                    dg["wins"] += 1
                else:
                    dg["losses"] += 1
                if gain is not None:
                    dg["net"] += gain
            # Etat EN DIRECT pour les positions encore ouvertes.
            current_price = None
            live = None
            if outcome == "PENDING":
                try:
                    current_price = db.price_at(asset, time.time()) if asset else None
                except Exception:  # noqa: BLE001
                    current_price = None
                if current_price is not None and entry is not None:
                    if direction == "BUY":
                        live = "WIN" if current_price > entry else ("LOSS" if current_price < entry else "EVEN")
                    else:
                        live = "WIN" if current_price < entry else ("LOSS" if current_price > entry else "EVEN")
            out.append(
                {
                    "id": s.get("id"),
                    "position_id": s.get("position_id"),
                    "ts": s.get("ts"),
                    "asset": asset,
                    "direction": direction,
                    "amount": amount,
                    "patterns": s.get("patterns") or "",
                    "entry": entry,
                    "expiry": exp,
                    "exit_price": exit_price,
                    "outcome": outcome,
                    "gain": round(gain, 2) if gain is not None else None,
                    "current_price": current_price,
                    "live": live,
                }
            )
        seen_pos = set()
        deduped = []
        for t in out:
            pid = t.get("position_id")
            if pid is not None:
                if pid in seen_pos:
                    continue
                seen_pos.add(pid)
            deduped.append(t)
        out = deduped
        days = []
        for day in sorted(daily.keys(), reverse=True):
            g = daily[day]
            closed = g["wins"] + g["losses"]
            days.append(
                {
                    "day": day,
                    "trades": g["trades"],
                    "wins": g["wins"],
                    "losses": g["losses"],
                    "rate": round(100.0 * g["wins"] / closed, 1) if closed else 0,
                    "net": round(g["net"], 2),
                }
            )
        return {
            "trades": out,
            "wins": wins,
            "losses": losses,
            "pending": pending,
            "total_gain": round(total_gain, 2),
            "daily": days,
        }

    @app.get("/api/ai/config")
    async def get_ai_config():
        cfg = load_ai_config(db)
        return {"base_url": cfg.base_url, "model": cfg.model, "has_key": bool(cfg.api_key)}

    @app.post("/api/ai/config")
    async def set_ai_config(req: AIConfigRequest):
        if req.base_url is not None:
            db.set_meta("ai_base_url", req.base_url.strip())
        if req.model is not None:
            db.set_meta("ai_model", req.model.strip())
        if req.api_key:
            db.set_meta("ai_api_key", req.api_key.strip())
        return {"ok": True}

    @app.get("/api/analyses")
    async def analyses(asset: Optional[str] = None, limit: int = 20):
        return {"analyses": db.get_recent_analyses(asset, limit)}

    @app.post("/api/replay")
    async def replay(req: ReplayRequest):
        asset = (req.asset or settings.asset).upper()
        job_id = uuid.uuid4().hex[:12]
        REPLAY_JOBS[job_id] = {
            "status": "running",
            "progress": 0,
            "total": req.max_steps,
            "asset": asset,
        }
        asyncio.create_task(_run_replay_job(job_id, asset, req))
        return {"job_id": job_id, "status": "running"}

    @app.get("/api/replay/{job_id}")
    async def replay_status(job_id: str):
        job = REPLAY_JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job inconnu")
        return job

    @app.get("/api/strategies")
    async def strategies(limit: int = 50):
        return {"strategies": db.get_strategies(limit)}

    @app.post("/api/suggest-method")
    async def suggest_method_endpoint(req: SuggestRequest):
        asset = (req.asset or settings.asset).upper()
        candles = load_candles(db, asset, req.timeframe, max(40, req.lookback))
        if len(candles) < 40:
            raise HTTPException(
                status_code=400, detail="Pas assez d'historique pour proposer une méthode."
            )
        summary = market_summary(candles, asset, req.timeframe)
        cfg = load_ai_config(db)
        try:
            method = await asyncio.to_thread(suggest_method, cfg, asset, req.timeframe, summary)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"asset": asset, "timeframe": req.timeframe, "summary": summary, "method": method}

    @app.post("/api/live/start")
    async def live_start(req: LiveStartRequest):
        old_task = LIVE.get("task")
        if old_task and not old_task.done():
            LIVE["running"] = False
            old_task.cancel()
        prompt = req.prompt
        name = "Méthode manuelle"
        timeframe = req.timeframe
        if req.strategy_id is not None:
            strat = db.get_strategy(req.strategy_id)
            if not strat:
                raise HTTPException(status_code=404, detail="Stratégie inconnue")
            prompt = strat["prompt"]
            name = strat["name"] or name
            timeframe = strat["timeframe"] or timeframe
        if not prompt:
            raise HTTPException(status_code=400, detail="Aucune méthode fournie")
        asset = (req.asset or settings.asset).upper()
        LIVE.update(
            {
                "running": True,
                "strategy": name,
                "asset": asset,
                "timeframe": timeframe,
                "min_confidence": req.min_confidence,
                "auto_trade": req.auto_trade,
                "interval": req.interval,
                "last_signal": None,
                "last_decision": None,
                "last_error": None,
                "last_check": None,
                "orders": 0,
                "checks": 0,
                "last_signal_key": None,
                "started_at": time.time(),
            }
        )
        params = {
            "prompt": prompt,
            "asset": asset,
            "timeframe": timeframe,
            "window": req.window,
            "min_confidence": req.min_confidence,
            "auto_trade": req.auto_trade,
            "interval": req.interval,
        }
        LIVE["task"] = asyncio.create_task(_live_loop(params))
        return {"running": True, "strategy": name, "asset": asset}

    @app.post("/api/live/stop")
    async def live_stop():
        LIVE["running"] = False
        task = LIVE.get("task")
        if task:
            task.cancel()
        LIVE["task"] = None
        return {"running": False}

    @app.get("/api/live")
    async def live_status():
        return {key: value for key, value in LIVE.items() if key != "task"}

    @app.post("/api/pattern/start")
    async def pattern_start(req: PatternStartRequest):
        old_task = PATTERN.get("task")
        if old_task and not old_task.done():
            PATTERN["running"] = False
            old_task.cancel()
        asset = (req.asset or settings.asset).upper()
        bot = PatternBot(
            asset=asset,
            source=req.source,
            interval=req.interval,
            timeframe=req.timeframe,
            amount=req.amount,
            min_strength=req.min_strength,
            poll=req.poll,
            auto_trade=req.auto_trade,
            max_trades=req.max_trades,
            cooldown=req.cooldown,
            require_trend=req.require_trend,
            allowed=req.allowed,
            settings=settings,
            db=db,
        )
        PATTERN.update(
            {
                "running": True,
                "asset": asset,
                "source": req.source,
                "interval": req.interval,
                "auto_trade": req.auto_trade,
                "min_strength": req.min_strength,
                "checks": 0,
                "trades": 0,
                "events": 0,
                "last_decision": None,
                "last_error": None,
                "last_bar_ts": None,
                "started_at": time.time(),
            }
        )
        PATTERN["task"] = asyncio.create_task(_pattern_loop(bot, req.poll))
        return {"running": True, "asset": asset}

    @app.post("/api/pattern/stop")
    async def pattern_stop():
        PATTERN["running"] = False
        task = PATTERN.get("task")
        if task:
            task.cancel()
        PATTERN["task"] = None
        return {"running": False}

    @app.get("/api/pattern")
    async def pattern_status():
        return {key: value for key, value in PATTERN.items() if key != "task"}

    @app.post("/api/analyze")
    async def analyze(req: AnalyzeRequest):
        asset = (req.asset or settings.asset).upper()
        cfg = load_ai_config(db)
        try:
            decision, used_cfg = await asyncio.to_thread(
                analyze_with_fallback, db, req.image, req.prompt, asset, req.context
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        action = "none"
        order_id = None
        if (
            req.auto_trade
            and decision.get("pattern_found")
            and decision.get("direction") in {"BUY", "SELL"}
            and decision.get("confidence", 0) >= req.min_confidence
        ):
            executor = OrderExecutor(settings=settings, db=db)
            amount = float(os.getenv("IQ_TRADE_AMOUNT", "1"))
            order_id = executor.submit(asset, decision["direction"], amount)
            action = "order_submitted"

        db.insert_analysis(asset, decision, action, raw=json.dumps(decision, ensure_ascii=False))
        return {
            "asset": asset,
            "decision": decision,
            "action": action,
            "order_id": order_id,
            "model": used_cfg.model,
            "provider_url": used_cfg.base_url,
        }

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket, asset: Optional[str] = None):
        token = settings.dashboard_token
        if token:
            supplied = websocket.query_params.get("token") or websocket.cookies.get("iq_token")
            if supplied != token:
                await websocket.close(code=1008)
                return
        await websocket.accept()
        asset = (asset or settings.asset).upper()
        recent = db.get_recent_ticks(asset, 1)
        last_id = recent[-1]["id"] if recent else 0
        last_candle = None
        try:
            await websocket.send_json({"type": "hello", "asset": asset})
            while True:
                rows = db.get_ticks_since(last_id, asset, 500)
                if rows:
                    last_id = rows[-1]["id"]
                    await websocket.send_json({"type": "ticks", "data": rows})
                latest = db.get_candles(asset, 300, 1)
                if latest:
                    cur = latest[-1]
                    if cur != last_candle:
                        await websocket.send_json({"type": "candle", "timeframe": 300, "data": cur})
                        last_candle = cur
                await websocket.send_json({"type": "ping", "stats": _stats_cached()})
                await _sleep(0.5)
        except WebSocketDisconnect:
            return
        except Exception:  # noqa: BLE001
            return

    return app


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


app = create_app()
