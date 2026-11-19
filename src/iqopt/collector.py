from __future__ import annotations

import asyncio
import base64
import copy
import json
import os
import re
import time
from typing import Optional

from playwright.async_api import async_playwright

from .config import Settings, load_settings
from .learner import BLACKLIST
from .parser import FrameParser, decode_payload
from .storage import Database

# Expose les sockets de la page pour pouvoir envoyer nos propres requetes
# (recuperation de l'historique des bougies). Injecte avant tout script de page.
WS_HOOK = """
(function () {
  if (window.__iqSockets) return;
  const NativeWS = window.WebSocket;
  window.__iqSockets = [];
  function Wrapped(url, protocols) {
    const ws = protocols ? new NativeWS(url, protocols) : new NativeWS(url);
    try { window.__iqSockets.push(ws); } catch (e) {}
    return ws;
  }
  Wrapped.prototype = NativeWS.prototype;
  try { Object.assign(Wrapped, NativeWS); } catch (e) {}
  window.WebSocket = Wrapped;
})();
"""

JS_SEND = """
(payload) => {
  const list = window.__iqSockets || [];
  let n = 0;
  for (const w of list) {
    if (w && w.readyState === 1) {
      try { w.send(payload); n++; } catch (e) {}
    }
  }
  return n;
}
"""


_NAMED_METHOD_DESC = {
    "bullish_inside": "2 bougies vertes, la 2ᵉ plus petite avec mèche BASSE longue",
    "bullish_big_lower": "rouge + plus grande verte qui englobe son corps",
    "bearish_hammer": "bougie rouge + 2ᵉ à mèche BASSE dominante (longue)",
    "green_upper_pin": "bougie verte à mèche HAUTE dominante (longue)",
    "inverted_hammer": "rouge + verte à mèche HAUTE dominante (inverted hammer)",
    "t3": "3 bougies (rouge 2 mèches, grande rouge, verte mèche basse) -> BUY",
    "r1": "3 rouges + engulfing verte -> BUY",
}


def forced_methods_from_env() -> list:
    """Methodes declarees dans IQ_FORCED_METHODS (nom:SENS:expiration), pour l'affichage."""
    from .patterns import DISCOVERED_PATTERNS

    desc = {n: d for n, _t, _dir, d in DISCOVERED_PATTERNS}
    desc.update(_NAMED_METHOD_DESC)
    out = []
    for part in (os.getenv("IQ_FORCED_METHODS", "") or "").split(","):
        bits = [b.strip() for b in part.strip().split(":")]
        if not bits or not bits[0]:
            continue
        direction = bits[1].upper() if len(bits) > 1 else "SELL"
        expiry = int(bits[2]) if len(bits) > 2 and bits[2].isdigit() else 300
        out.append(
            {
                "name": bits[0],
                "direction": direction if direction in ("BUY", "SELL") else "SELL",
                "expiry": expiry,
                "desc": desc.get(bits[0], ""),
                "pairs": [],
            }
        )
    return out


class Collector:
    def __init__(
        self,
        settings: Optional[Settings] = None,
        asset: Optional[str] = None,
        headless: Optional[bool] = None,
    ) -> None:
        self.settings = settings or load_settings()
        self.asset = (asset or self.settings.asset).upper()
        self.headless = self.settings.headless if headless is None else headless
        self.db = Database(self.settings.db_path)
        self.db.init()
        self.parser = FrameParser(default_asset=self.asset)
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.queue: Optional[asyncio.Queue] = None
        self.stats = {"ws": 0, "frames": 0, "parsed_msgs": 0, "ticks": 0, "candles": 0, "assets": 0}
        self._capture_handle = None
        self._started = time.time()
        self.asset_map: dict[int, str] = self.db.get_asset_map()
        # Reverse maps pour construire les ordres (nom -> id / precision).
        self.asset_ids: dict[str, int] = {}
        self.precisions: dict[str, int] = {}
        self.asset_types: dict[str, str] = {}
        self.asset_suspended: dict[int, bool] = {}
        for row in self.db.get_assets():
            name = str(row.get("name") or "").upper()
            if not name:
                continue
            try:
                self.asset_ids[name] = int(row.get("active_id"))
            except (TypeError, ValueError):
                pass
            if row.get("type"):
                self.asset_types[name] = str(row.get("type"))
            try:
                self.asset_suspended[int(row.get("active_id"))] = bool(row.get("suspended"))
            except (TypeError, ValueError):
                pass
            prec = row.get("precision")
            if prec is not None:
                try:
                    self.precisions[name] = int(prec)
                except (TypeError, ValueError):
                    pass
        # Config grid traderoom (plotters activeId) pour changer l'actif.
        self.grid_config: Optional[dict] = None
        self.grid_version: int = 2
        # Dernier etat du grid emis par l'app (pour detecter une application directe).
        self.app_grid_visible: list = []
        self.app_grid_ts: float = 0.0
        # Paire a laquelle on s'est abonne cote flux (independamment du graphique).
        self.subscribed_asset_id: Optional[int] = None
        # Positions deja enregistrees pour l'apprentissage.
        self.seen_positions: set = set()
        # Positions deja notees comme gagnantes (apprentissage = trades reussis).
        self.noted_positions: set = set()
        # Derniere bougie traitee par le mode auto (par paire).
        self._auto_last_bars: dict = {}
        self.auto_subscribed: set = set()
        self.auto_subscribed_tf: set = set()
        self.pending_auto: dict = {}
        self.auto_sub_at: dict = {}
        self.ags_seen: set = set()
        self.preclose_seen: set = set()
        self.mg: dict = {}
        self.active_seen: dict = {}
        self.open_trades: list = []
        # Modele d'ordre capture sur un trade manuel (message WS binary-options.open-option).
        self.order_template: Optional[dict] = None
        try:
            raw = self.db.get_meta("order_template")
            if raw:
                self.order_template = json.loads(raw)
        except Exception:  # noqa: BLE001
            pass
        # Confirmations d'ordre recues par WS, indexees par request_id.
        self.order_results: dict[str, dict] = {}
        # Soldes par type : 1 = reel, 4 = demo (convention IQ).
        self.balance_ids: dict[int, int] = {}

        # Correlation request_id -> {active_id, size} pour attribuer l'historique.
        self.request_ctx: dict[str, dict] = {}
        # Dernier id de bougie connu par actif (pour paginer l'historique).
        self.hist_to_id: dict[int, int] = {}
        # Requetes d'historique en attente de reponse.
        self.pending: dict[str, dict] = {}
        self._history_started = False

    def _resolve_asset(self, asset: str) -> str:
        key = str(asset)
        if key.isdigit():
            return self.asset_map.get(int(key), key)
        return key

    # ------------------------------------------------------------------
    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.queue = asyncio.Queue(maxsize=20000)
        if self.settings.capture_raw:
            stamp = time.strftime("%Y%m%d-%H%M%S")
            path = self.settings.captures_dir / f"frames-{stamp}.jsonl"
            self._capture_handle = path.open("a", encoding="utf-8")
            print(f"[collector] capture brute -> {path}", flush=True)

        consumer = asyncio.create_task(self._consume())

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context_args: dict = {
                "viewport": {"width": 1400, "height": 900},
                "user_agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            }
            if self.settings.storage_state.exists():
                context_args["storage_state"] = str(self.settings.storage_state)
                print("[collector] session existante rechargee", flush=True)

            context = await browser.new_context(**context_args)
            await context.add_init_script(WS_HOOK)
            page = await context.new_page()
            page.on("websocket", self._on_websocket)

            print(f"[collector] ouverture {self.settings.traderoom_url}", flush=True)
            await page.goto(self.settings.traderoom_url, wait_until="domcontentloaded", timeout=90000)

            try:
                await self._wait_until_live(context, page)
                order_task = asyncio.create_task(self._order_worker(page))
                auto_task = asyncio.create_task(self._auto_loop(page))
                ags_task = asyncio.create_task(self._ags_loop(page))
                preclose_task = asyncio.create_task(self._preclose_loop(page))
                counter_task = asyncio.create_task(self._counter_loop(page))
                while True:
                    await asyncio.sleep(30)
                    await self._save_state(context)
                    self._log_stats()
            finally:
                try:
                    order_task.cancel()
                    auto_task.cancel()
                    ags_task.cancel()
                    preclose_task.cancel()
                    counter_task.cancel()
                except Exception:  # noqa: BLE001
                    pass
                await self._save_state(context)
                consumer.cancel()
                if self._capture_handle:
                    self._capture_handle.close()
                await browser.close()

    # ------------------------------------------------------------------
    # Execution des ordres venant du dashboard (file "orders" -> statut "queued")
    # ------------------------------------------------------------------
    async def _order_worker(self, page) -> None:
        mode = "EXECUTION" if self.settings.enable_execution else "DRY-RUN"
        print(f"[orders] worker demarre (mode={mode})", flush=True)
        while True:
            try:
                order = self.db.next_queued_order()
                if order:
                    await self._execute_order(page, order)
            except Exception as exc:  # noqa: BLE001
                print(f"[orders] erreur worker: {exc}", flush=True)
            await asyncio.sleep(0.05)

    async def _execute_order(self, page, order: dict) -> None:
        oid = int(order["id"])
        direction = str(order["direction"]).upper()
        asset = str(order["asset"])
        amount = float(order["amount"])

        if direction == "DIAG":
            info = await self._diagnose(page)
            payload = json.dumps(info)
            self.db.update_order_status(oid, "diag", payload[:900])
            print("[diag] " + payload[:2500], flush=True)
            return

        if direction == "SWITCH":
            aid = self.asset_ids.get(asset.upper())
            if aid is None:
                self.db.update_order_status(oid, "switch", "active_id inconnu")
                return
            try:
                # 1) S'abonner au flux de la paire (marche meme si non affichee).
                sub = {
                    "name": "subscribeMessage",
                    "request_id": "sub" + str(int(time.time() * 1000)),
                    "msg": {
                        "name": "candle-generated",
                        "params": {"routingFilters": {"active_id": int(aid), "size": 300}},
                    },
                }
                sent = await self._send_ws(page, json.dumps(sub))
                self.subscribed_asset_id = int(aid)

                # 2) Best-effort : pointer le grid serveur sur cette paire (sans reload).
                if isinstance(self.grid_config, dict):
                    try:
                        cfg = copy.deepcopy(self.grid_config)
                        plots = cfg.get("plotters") or []
                        found = False
                        for p in plots:
                            try:
                                same = int(p.get("activeId") or -1) == int(aid)
                            except (TypeError, ValueError):
                                same = False
                            if same:
                                p["isMinimized"] = False
                                found = True
                            else:
                                p["isMinimized"] = True
                        if not found and plots:
                            tmpl = copy.deepcopy(plots[0])
                            atype = self.asset_types.get(asset.upper()) or ""
                            tmpl["activeId"] = int(aid)
                            tmpl["activeType"] = atype if "marginal" in atype else "blitz-option"
                            tmpl["isMinimized"] = False
                            plots.append(tmpl)
                        cfg["plotters"] = plots
                        cid = str(int(time.time() * 1000)) + "000"
                        gmsg = {
                            "name": "sendMessage",
                            "request_id": cid,
                            "msg": {
                                "name": "set-user-settings",
                                "version": "1.0",
                                "body": {
                                    "name": "traderoom_gl_grid",
                                    "version": self.grid_version or 2,
                                    "client_id": cid,
                                    "config": cfg,
                                },
                            },
                        }
                        await self._send_ws(page, json.dumps(gmsg))
                    except Exception:  # noqa: BLE001
                        pass
                self.db.update_order_status(oid, "switch", f"abonne {aid} (ws {sent})")
                print(f"[switch] abonne flux {aid} ({asset})", flush=True)
            except Exception as exc:  # noqa: BLE001
                self.db.update_order_status(oid, "switch", "echec: " + str(exc)[:140])
            return

        if not self.settings.enable_execution:
            modele = "pret" if self.order_template else "absent (place un trade manuel pour calibrer)"
            note = f"dry-run; modele={modele}"
            self.db.update_order_status(oid, "dry_run", note)
            print(f"[orders] #{oid} DRY-RUN {direction} {asset} {amount} -> {note}", flush=True)
            return
        if not self.settings.demo_only and os.getenv("IQ_CONFIRM_LIVE") != "YES":
            self.db.update_order_status(oid, "rejected", "compte reel sans IQ_CONFIRM_LIVE=YES")
            return

        self.db.update_order_status(oid, "placing", "en cours")

        # L'executeur est binaire (option). Les instruments CFD/marge
        # (forex hors OTC, crypto, actions) ne passent pas par ce flux.
        atype = str(self.asset_types.get(asset.upper()) or "").lower()
        if "marginal" in atype:
            self.db.update_order_status(
                oid,
                "failed",
                f"instrument CFD ({atype}) non supporte: choisis une paire -OTC",
            )
            return

        # On peut trader toute paire dont on connait l'active_id et qui a du flux,
        # independamment du graphique affiche dans IQ.
        if self.asset_ids.get(asset.upper()) is None:
            self.db.update_order_status(oid, "failed", f"active_id inconnu pour {asset}")
            return
        recent = self.db.get_recent_ticks(asset, 1)
        if not recent or (time.time() - float(recent[-1]["ts"])) > 180:
            self.db.update_order_status(oid, "failed", f"pas de flux recent pour {asset}")
            return
        if not self.order_template:
            self.db.update_order_status(
                oid,
                "failed",
                "aucun modele d'ordre: place un trade manuel une fois dans la tradingroom",
            )
            return
        try:
            req_expiry = order.get("req_expiry")
            payload = self._build_order_message(
                direction, asset, amount, int(req_expiry) if req_expiry else None
            )
        except Exception as exc:  # noqa: BLE001
            self.db.update_order_status(oid, "failed", f"construction ordre: {exc}")
            return
        try:
            sent = await self._send_ws(page, json.dumps(payload, separators=(",", ":")))
        except Exception as exc:  # noqa: BLE001
            self.db.update_order_status(oid, "failed", f"envoi ws echoue: {exc}")
            return
        if not sent:
            self.db.update_order_status(oid, "failed", "aucune socket disponible")
            return
        rid = str(payload.get("request_id"))
        result = await self._wait_order_result(rid)
        if result and result.get("success") and result.get("option_id") is not None:
            body = payload["msg"]["body"]
            precision = self.precisions.get(asset.upper(), 6)
            entry = None
            if body.get("value") is not None:
                entry = float(body["value"]) / (10 ** precision)
            win_return = None
            pi = result.get("profit_income")
            if pi is not None:
                try:
                    win_return = float(pi) / 100.0
                except (TypeError, ValueError):
                    win_return = None
            self.db.set_order_fill(
                oid, int(result["option_id"]), entry, body.get("expired"), win_return
            )
            self.db.update_order_status(
                oid,
                "filled",
                f"position #{result.get('option_id')} {result.get('direction')} "
                f"exp={payload['msg']['body'].get('expiration_size')}s",
            )
            print(
                f"[orders] #{oid} {direction} {asset} {amount} -> FILLED #{result.get('option_id')}",
                flush=True,
            )
        elif result and result.get("success") is False:
            self.db.update_order_status(oid, "failed", "rejete par IQ")
            print(f"[orders] #{oid} -> rejete par IQ", flush=True)
        else:
            self.db.update_order_status(oid, "sent", "envoye (confirmation non recue)")
            print(f"[orders] #{oid} {direction} {asset} {amount} -> envoye (sans confirmation)", flush=True)

    def _build_order_message(
        self, direction: str, asset: str, amount: float, expiry: Optional[int] = None
    ) -> dict:
        template = dict(self.order_template or {})
        name = str(asset).upper()
        active_id = self.asset_ids.get(name)
        if active_id is None:
            raise ValueError(f"active_id inconnu pour {asset}")
        precision = self.precisions.get(name, 6)
        ticks = self.db.get_recent_ticks(asset, 1)
        if ticks:
            price = float(ticks[-1]["price"])
        else:
            cs = self.db.get_candles(asset, 300, 1) or self.db.get_candles(asset, 60, 1)
            if not cs:
                raise ValueError(f"aucun prix recent pour {asset}")
            price = float(cs[-1]["close"])
        if expiry:
            expiration = int(expiry)
        else:
            expiration = int(template.get("expiration_size") or 300)
        body = dict(template)
        body["active_id"] = int(active_id)
        body["direction"] = "call" if direction == "BUY" else "put"
        body["price"] = float(amount)
        body["value"] = int(round(price * (10 ** precision)))
        body["expired"] = int(time.time()) + expiration
        body.setdefault("refund_value", 0)
        # Choix du compte : 1 = reel, 4 = demo. Le mode demo est force par defaut.
        mode = (self.db.get_meta("order_account") or "demo").lower()
        if mode == "real":
            if self.settings.demo_only:
                raise ValueError("reel refuse: collector en mode demo (IQ_DEMO_ONLY=1)")
            if os.getenv("IQ_CONFIRM_LIVE") != "YES":
                raise ValueError("reel refuse: IQ_CONFIRM_LIVE!=YES")
            bid = self.balance_ids.get(1)
            if bid is None:
                raise ValueError("solde reel introuvable")
        else:
            bid = self.balance_ids.get(4)
            if bid is None:
                raise ValueError("solde demo introuvable")
        body["user_balance_id"] = int(bid)
        return {
            "name": "sendMessage",
            "request_id": str(int(time.time() * 1000)),
            "msg": {"name": "binary-options.open-option", "version": "2.0", "body": body},
        }

    async def _set_order_amount(self, page, amount: float) -> bool:
        selectors = [
            "input[aria-label*='montant' i]",
            "input[aria-label*='amount' i]",
            "input[aria-label*='invest' i]",
            "input[data-test*='amount' i]",
            "input[inputmode='decimal']",
        ]
        for frame in page.frames:
            for sel in selectors:
                try:
                    loc = frame.locator(sel).first
                    if await loc.count():
                        await loc.fill(str(amount), timeout=2000)
                        return True
                except Exception:  # noqa: BLE001
                    continue
        return False

    async def _find_order_button(self, page, direction: str):
        names = {
            "BUY": ["Acheter", "Achat", "Buy", "Call", "Hausse", "Haut", "Higher", "Up"],
            "SELL": ["Vendre", "Vente", "Sell", "Put", "Baisse", "Bas", "Lower", "Down"],
        }.get(direction, [])
        token = "buy" if direction == "BUY" else "sell"
        for frame in page.frames:
            for name in names:
                try:
                    loc = frame.get_by_role("button", name=re.compile(re.escape(name), re.IGNORECASE))
                    if await loc.count():
                        return loc.first, name
                except Exception:  # noqa: BLE001
                    continue
            try:
                loc = frame.locator(f"button[class*='{token}' i], [data-test*='{token}' i]").first
                if await loc.count():
                    return loc, token
            except Exception:  # noqa: BLE001
                pass
            for name in names:
                try:
                    loc = frame.get_by_text(re.compile(re.escape(name), re.IGNORECASE))
                    if await loc.count():
                        return loc.first, name + "(text)"
                except Exception:  # noqa: BLE001
                    continue
        return None, ""

    async def _diagnose(self, page) -> dict:
        try:
            return await page.evaluate(
                """() => {
                  const keys = Object.keys(window).filter(function (k) {
                    return /(iq|trade|option|store|instr|active|room|redux|apollo|chart|socket)/i.test(k);
                  }).slice(0, 80);
                  const globals = {};
                  ['__store__','store','__REDUX_STORE__','__APOLLO_CLIENT__','__IQ','__iqOption',
                   '__tradeStore','__traderoom','ng','angular'].forEach(function (k) {
                    globals[k] = typeof window[k] + (window[k] && window[k].getState ? ':getState' : '');
                  });
                  const inputs = Array.from(document.querySelectorAll('input')).slice(0, 20).map(function (i) {
                    return {ph: i.placeholder || '', al: i.getAttribute('aria-label') || '',
                            type: i.type, cls: (typeof i.className === 'string' ? i.className : '').slice(0, 50)};
                  });
                  let lskeys = []; try { lskeys = Object.keys(localStorage).slice(0, 50); } catch (e) {}
                  const ss = {}; try { for (let i = 0; i < sessionStorage.length && i < 60; i++) { const k = sessionStorage.key(i); ss[k] = String(sessionStorage.getItem(k)).slice(0, 140); } } catch (e) {}
                  const lsm = {}; try {
                    for (let i = 0; i < localStorage.length; i++) {
                      const k = localStorage.key(i);
                      if (/active|instr|asset|traderoom|selected|chart/i.test(k)) lsm[k] = String(localStorage.getItem(k)).slice(0, 200);
                    }
                  } catch (e) {}
                  const tags = {};
                  document.querySelectorAll('*').forEach(function (e) {
                    const t = e.tagName.toLowerCase(); tags[t] = (tags[t] || 0) + 1;
                  });
                  return { url: location.href, canvases: document.querySelectorAll('canvas').length,
                           inputs: inputs, globals: globals, keys: keys, lskeys: lskeys,
                           sessionStorage: ss, ls_match: lsm, tags: tags };
                }"""
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    async def _list_buttons(self, page) -> str:
        parts = []
        for frame in page.frames:
            try:
                d = await frame.evaluate(
                    """() => {
                      const re = /(acheter|vendre|buy|sell|call|put|higher|lower|haut|bas|montant|amount|invest|trade)/i;
                      const out = [];
                      const seen = new Set();
                      let shadows = 0;
                      function walk(root) {
                        for (const e of root.querySelectorAll('*')) {
                          if (e.shadowRoot) { shadows++; walk(e.shadowRoot); }
                          const t = (e.innerText || '').replace(/\\s+/g, ' ').trim();
                          const al = (e.getAttribute && e.getAttribute('aria-label')) || '';
                          const title = (e.getAttribute && e.getAttribute('title')) || '';
                          const cls = (typeof e.className === 'string' ? e.className : '');
                          const combo = [t, al, title, cls].filter(Boolean).join(' | ');
                          if (!re.test(combo) || t.length > 50) continue;
                          const tag = e.tagName ? e.tagName.toLowerCase() : '?';
                          const line = tag + (cls ? '.' + cls.slice(0, 45) : '') + ' :: ' + (t || al || title);
                          if (seen.has(line)) continue;
                          seen.add(line);
                          out.push(line);
                          if (out.length >= 50) return;
                        }
                      }
                      walk(document);
                      return { url: location.href, shadows: shadows, elems: document.querySelectorAll('*').length,
                               body: (document.body ? document.body.innerText : '').replace(/\\s+/g, ' ').slice(0, 90),
                               candidates: out };
                    }"""
                )
                parts.append(
                    f"[{d.get('url')}] shadows={d.get('shadows')} elems={d.get('elems')} "
                    f"body='{d.get('body')}' cand={d.get('candidates')}"
                )
            except Exception as exc:  # noqa: BLE001
                parts.append(f"[frame err] {exc}")
        return " ".join(parts)[:1400]

    # ------------------------------------------------------------------
    def _on_websocket(self, ws) -> None:
        self.stats["ws"] += 1
        print(f"[collector] websocket #{self.stats['ws']} -> {ws.url}", flush=True)
        ws.on("framereceived", lambda payload: self._enqueue("in", payload, ws.url))
        ws.on("framesent", lambda payload: self._enqueue("out", payload, ws.url))

    def _enqueue(self, direction: str, payload, url: str) -> None:
        if self.queue is None or self.loop is None:
            return
        try:
            self.queue.put_nowait((direction, payload, url))
        except asyncio.QueueFull:
            pass

    async def _consume(self) -> None:
        assert self.queue is not None
        while True:
            direction, payload, url = await self.queue.get()
            try:
                self._process(direction, payload, url)
            except Exception as exc:  # noqa: BLE001
                print(f"[collector] erreur traitement trame: {exc}", flush=True)
            finally:
                self.queue.task_done()

    def _process(self, direction: str, payload, url: str) -> None:
        self.stats["frames"] += 1
        if self._capture_handle:
            record = {"ts": time.time(), "dir": direction, "url": url}
            if isinstance(payload, bytes):
                record["b64"] = base64.b64encode(payload).decode("ascii")
            else:
                record["text"] = payload
            self._capture_handle.write(json.dumps(record, ensure_ascii=False) + "\n")

        messages = decode_payload(payload)
        if not messages:
            return
        self.stats["parsed_msgs"] += len(messages)

        for message in messages:
            name = str(message.get("name") or message.get("type") or message.get("topic") or "").lower()
            if direction == "out":
                self._learn_order_template(message)
                self._learn_context(message)
                self._learn_app_grid(message)
                continue

            # Confirmations d'ordre cote serveur (result / option).
            try:
                self._observe_order_result(message)
            except Exception:  # noqa: BLE001
                pass

            # Soldes (pour choisir demo/reel).
            if name == "balances" and isinstance(message.get("msg"), list):
                for b in message["msg"]:
                    try:
                        self.balance_ids[int(b.get("type"))] = int(b.get("id"))
                    except (TypeError, ValueError):
                        continue
                try:
                    self.db.set_meta("balances", json.dumps(message["msg"]))
                except Exception:  # noqa: BLE001
                    pass

            if name == "candle-generated" and isinstance(message.get("msg"), dict):
                aid = message["msg"].get("active_id")
                if aid is not None:
                    try:
                        self.active_seen[int(aid)] = time.time()
                    except (TypeError, ValueError):
                        pass

            if name == "user-settings" and isinstance(message.get("msg"), dict):
                for c in message["msg"].get("configs", []) or []:
                    if not isinstance(c, dict):
                        continue
                    if c.get("name") == "traderoom_gl_grid":
                        try:
                            self.grid_config = c.get("config")
                            self.grid_version = int(c.get("version") or 2)
                        except (TypeError, ValueError):
                            pass

            # Resultats officiels des positions (win / loose / opened).
            if name in ("position-changed", "positions", "history-positions"):
                try:
                    self._observe_positions(message)
                except Exception:  # noqa: BLE001
                    pass
            if name == "option":
                try:
                    self._confirm_auto_order(message)
                except Exception:  # noqa: BLE001
                    pass

            # "first-candles" ne contient que la toute premiere bougie de chaque
            # timeframe (outlier a des mois) : inutile et fausse l'echelle.
            if name == "first-candles":
                continue

            context_asset = None
            if "candle" in name:
                rid = str(message.get("request_id", ""))
                ctx = self.request_ctx.get(rid)
                if ctx and ctx.get("active_id") is not None:
                    context_asset = str(ctx["active_id"])
                self._observe_pending(rid, message)

            frame = self.parser.parse([message], context_asset=context_asset)
            self._store_frame(frame)

        if self.stats["frames"] % 200 == 0:
            self._log_stats()

    def _store_frame(self, frame) -> None:
        for asset in frame.assets:
            self.db.upsert_asset(
                asset.active_id, asset.name, asset.type, asset.precision,
                1 if getattr(asset, "suspended", False) else 0,
            )
            self.asset_map[asset.active_id] = asset.name
            self.asset_suspended[int(asset.active_id)] = bool(getattr(asset, "suspended", False))
            self.stats["assets"] = self.stats.get("assets", 0) + 1
        for tick in frame.ticks:
            self.db.insert_tick(self._resolve_asset(tick.asset), tick.ts, tick.price)
            self.stats["ticks"] += 1
        for candle in frame.candles:
            self.db.upsert_candle(
                self._resolve_asset(candle.asset),
                candle.timeframe,
                candle.ts,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            )
            self.stats["candles"] += 1

    # -- correlation / historique --------------------------------------
    def _learn_order_template(self, message: dict) -> None:
        """Memorise le dernier ordre manuel (message WS) comme modele a rejouer."""
        msg = message.get("msg")
        if not isinstance(msg, dict):
            return
        if str(msg.get("name") or "") != "binary-options.open-option":
            return
        body = msg.get("body")
        if not isinstance(body, dict):
            return
        try:
            self.order_template = dict(body)
            self.db.set_meta("order_template", json.dumps(self.order_template))
            print(
                f"[orders] modele capture: active_id={body.get('active_id')} "
                f"option_type={body.get('option_type_id')} expiration={body.get('expiration_size')}",
                flush=True,
            )
        except Exception:  # noqa: BLE001
            pass

    def _publish_active_methods(self, only_if_empty: bool = False) -> None:
        """Publie la liste des methodes pour le dashboard, sans attendre d'avoir appris
        une strategie (affichage uniquement : n'influence pas les decisions de trading)."""
        try:
            if only_if_empty:
                try:
                    if json.loads(self.db.get_meta("active_methods") or "[]"):
                        return
                except Exception:  # noqa: BLE001
                    pass
            items = forced_methods_from_env()
            if items:
                self.db.set_meta("active_methods", json.dumps(items, ensure_ascii=False))
                print(f"[methods] {len(items)} methodes publiees pour le dashboard", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[methods] publication impossible: {exc}", flush=True)

    async def _auto_loop(self, page) -> None:
        print("[auto] boucle apprentissage prete (compte selon le mode choisi)", flush=True)
        self._publish_active_methods()
        try:
            period = int(os.getenv("IQ_ALIGN_PERIOD", "300") or 300)
        except ValueError:
            period = 300
        period = max(30, period)
        try:
            offset = float(os.getenv("IQ_ALIGN_OFFSET", "1.0") or 1.0)
        except ValueError:
            offset = 0.5
        while True:
            # 1) Attendre la prochaine cloture de bougie (+offset). On ne trade
            # JAMAIS au demarrage au milieu d'une bougie.
            now = time.time()
            nxt = (int(now // period) + 1) * period + offset
            # Eviter une boucle serree quand on entre AVANT la cloture (offset<0).
            if nxt <= now + 0.5:
                nxt += period
            while True:
                remain = nxt - time.time()
                if remain <= 0:
                    break
                try:
                    self.db.set_meta("auto_heartbeat", str(time.time()))
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(min(5.0, remain))
            # 2) Juste apres la cloture : detecter et trader.
            try:
                self.db.set_meta("auto_heartbeat", str(time.time()))
                if (self.db.get_meta("learn_auto") or "0") == "1":
                    self.db.set_meta("auto_last_check", str(time.time()))
                    await self._auto_step(page)
                self._publish_active_methods(only_if_empty=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[auto] erreur: {exc}", flush=True)

    # ------------------------------------------------------------------
    # Strategie AGS (portage Pine 'AGS M5 PRECISION SCALPING V3.1')
    # ------------------------------------------------------------------
    async def _ags_loop(self, page) -> None:
        from .ags import AGSConfig

        cfg = AGSConfig()
        print("[ags] boucle AGS prete", flush=True)
        while True:
            try:
                if (self.db.get_meta("ags_enabled") or "1") == "1" and os.getenv(
                    "IQ_METHODS_ONLY", "0"
                ) != "1":
                    await self._ags_step(page, cfg)
            except Exception as exc:  # noqa: BLE001
                print(f"[ags] erreur: {exc}", flush=True)
            await asyncio.sleep(2)

    async def _ags_step(self, page, cfg) -> None:
        from .ags import m15_filter, rebound_signal, setup

        mon = [a for a in (self.db.get_meta("auto_monitored") or "").split(",") if a]
        # Suivre AUSSI la paire affichee sur le graphique (celle que tu regardes
        # et ou l'indicateur AGS affiche ses fleches), meme sans demo prealable.
        try:
            now_ts = time.time()
            for aid, seen_ts in self.active_seen.items():
                if now_ts - seen_ts > 180:
                    continue
                nm = self.asset_map.get(aid)
                if nm and nm not in mon:
                    mon.append(nm)
        except Exception:  # noqa: BLE001
            pass
        if not mon:
            return
        if os.getenv("IQ_PRECLOSE_ONLY", "1") == "1":
            return  # mode pre-cloture (T-5s) actif -> gere par _preclose_step
        try:
            amount = float(self.db.get_meta("auto_amount") or 0) or 1.0
        except (TypeError, ValueError):
            amount = 1.0
        try:
            expiry = int(os.getenv("IQ_AGS_EXPIRY", "300") or 300)
        except ValueError:
            expiry = 300

        now = time.time()
        for asset in mon:
            aid = self.asset_ids.get(asset.upper())
            if aid is None:
                continue
            if "marginal" in str(self.asset_types.get(asset.upper()) or "").lower():
                continue
            if aid not in self.auto_subscribed:
                sub = {
                    "name": "subscribeMessage",
                    "request_id": "ags" + str(int(time.time() * 1000)),
                    "msg": {
                        "name": "candle-generated",
                        "params": {"routingFilters": {"active_id": int(aid), "size": tf}},
                    },
                }
                await self._send_ws(page, json.dumps(sub))
                self.auto_subscribed.add(int(aid))
                self.auto_sub_at[int(aid)] = time.time()

            candles = self.db.get_candles(asset, 300, 6)
            if len(candles) < 3:
                continue
            direction = setup(candles, cfg)
            if not direction:
                continue
            if not m15_filter(candles, cfg, direction):
                continue
            current = candles[-1]
            try:
                c_ts = float(current["ts"])
                c_open = float(current["open"])
            except (KeyError, TypeError, ValueError):
                continue
            if now - c_ts > cfg.observe_sec:
                continue
            ticks = self.db.get_ticks_range(asset, c_ts, c_ts + cfg.observe_sec, 600)
            if not ticks:
                continue
            if not rebound_signal(direction, c_open, ticks, c_ts, cfg.observe_sec):
                continue
            key = (asset, int(c_ts), direction)
            if key in self.ags_seen:
                continue
            self.ags_seen.add(key)
            if len(self.ags_seen) > 800:
                self.ags_seen = set(list(self.ags_seen)[-400:])

            try:
                payload = self._build_auto_message(direction, asset, amount, expiry)
            except Exception as exc:  # noqa: BLE001
                print(f"[ags] build {asset}: {exc}", flush=True)
                continue
            sent = await self._send_ws(page, json.dumps(payload))
            try:
                self.db.insert_order(
                    asset, direction, amount, "auto", f"AGS exp={expiry}s ws={sent}"
                )
            except Exception:  # noqa: BLE001
                pass
            print(f"[ags] {direction} {asset} {amount}$ exp={expiry}s sent={sent}", flush=True)
            try:
                self.db.set_meta("auto_last_signal", f"AGS {direction} {asset} {amount}$")
                self.db.set_meta("auto_last_trade_ts", str(time.time()))
            except Exception:  # noqa: BLE001
                pass

    async def _preclose_loop(self, page) -> None:
        print("[preclose] boucle pre-cloture prete (T-5s)", flush=True)
        while True:
            try:
                if (self.db.get_meta("learn_auto") or "1") == "1":
                    await self._preclose_step(page)
            except Exception as exc:  # noqa: BLE001
                print(f"[preclose] erreur: {exc}", flush=True)
            await asyncio.sleep(1.5)

    async def _preclose_step(self, page) -> None:
        if os.getenv("IQ_METHODS_ONLY", "0") == "1":
            return  # mode methodes seules : la direction de bougie n'est pas une methode
        if os.getenv("IQ_PRECLOSE_ONLY", "1") != "1":
            return
        mon = [a for a in (self.db.get_meta("auto_monitored") or "").split(",") if a]
        if not mon:
            return
        try:
            amount = float(self.db.get_meta("auto_amount") or 0) or 1.0
        except (TypeError, ValueError):
            amount = 1.0
        try:
            expiry = int(os.getenv("IQ_PRECLOSE_EXPIRY", "30") or 30)
        except ValueError:
            expiry = 30
        try:
            lead = float(os.getenv("IQ_PRECLOSE_SEC", "5") or 5)
        except ValueError:
            lead = 5.0

        now = time.time()
        for asset in mon:
            aid = self.asset_ids.get(asset.upper())
            if aid is None:
                continue
            if "marginal" in str(self.asset_types.get(asset.upper()) or "").lower():
                continue
            if aid not in self.auto_subscribed:
                sub = {
                    "name": "subscribeMessage",
                    "request_id": "pre" + str(int(time.time() * 1000)),
                    "msg": {
                        "name": "candle-generated",
                        "params": {"routingFilters": {"active_id": int(aid), "size": 300}},
                    },
                }
                await self._send_ws(page, json.dumps(sub))
                self.auto_subscribed.add(int(aid))
            candles = self.db.get_candles(asset, 300, 4)
            if len(candles) < 2:
                continue
            forming = candles[-1]
            try:
                ts = float(forming["ts"])
                op = float(forming["open"])
                cl = float(forming["close"])
            except (KeyError, TypeError, ValueError):
                continue
            remaining = (ts + 300) - now
            if remaining <= 0 or remaining > lead:
                continue
            key = (asset, int(ts))
            if key in self.preclose_seen:
                continue
            self.preclose_seen.add(key)
            if len(self.preclose_seen) > 1200:
                self.preclose_seen = set(list(self.preclose_seen)[-600:])

            direction = "BUY" if cl >= op else "SELL"
            trade_amount = amount if amount >= 1 else 1.0
            try:
                payload = self._build_auto_message(direction, asset, trade_amount, expiry)
            except Exception as exc:  # noqa: BLE001
                print(f"[preclose] build {asset}: {exc}", flush=True)
                continue
            sent = await self._send_ws(page, json.dumps(payload))
            try:
                self.db.insert_order(
                    asset, direction, trade_amount, "auto", f"preclose exp={expiry}s ws={sent}"
                )
            except Exception:  # noqa: BLE001
                pass
            # Suivi pour le contre-trade a T-4s si le trade devient perdant.
            try:
                tks = self.db.get_recent_ticks(asset, 1)
                entry_px = float(tks[-1]["price"]) if tks else None
            except Exception:  # noqa: BLE001
                entry_px = None
            self.open_trades.append(
                {
                    "asset": asset,
                    "direction": direction,
                    "amount": trade_amount,
                    "expiry_ts": time.time() + expiry,
                    "entry": entry_px,
                    "handled": False,
                }
            )
            print(
                f"[preclose] {direction} {asset} {trade_amount}$ exp={expiry}s "
                f"T-{remaining:.0f}s sent={sent}",
                flush=True,
            )
            try:
                self.db.set_meta("auto_last_signal", f"PRE {direction} {asset} {trade_amount}$")
                self.db.set_meta("auto_last_trade_ts", str(time.time()))
            except Exception:  # noqa: BLE001
                pass

    def _result_for(self, asset: str, ts: float, direction: Optional[str] = None) -> Optional[str]:
        """Resultat (win/loose) du trade le plus proche de `ts` pour cet actif."""
        try:
            samples = self.db.get_samples(400)
        except Exception:  # noqa: BLE001
            return None
        best = None
        bestd = 1e9
        for s in samples:
            if str(s.get("asset") or "").upper() != str(asset).upper():
                continue
            try:
                d = abs(float(s.get("ts") or 0) - float(ts))
            except (TypeError, ValueError):
                continue
            if d < bestd:
                bestd = d
                best = s
        if best is None or bestd > 300:
            return None
        return str(best.get("result") or "").lower() or None

    async def _counter_loop(self, page) -> None:
        print("[counter] boucle contre-trade prete (T-4s)", flush=True)
        while True:
            try:
                if os.getenv("IQ_COUNTER_ENABLED", "0") == "1" and os.getenv(
                    "IQ_METHODS_ONLY", "0"
                ) != "1" and (
                    self.db.get_meta("learn_auto") or "1"
                ) == "1":
                    await self._counter_step(page)
            except Exception as exc:  # noqa: BLE001
                print(f"[counter] erreur: {exc}", flush=True)
            await asyncio.sleep(1)

    async def _counter_step(self, page) -> None:
        now = time.time()
        keep = []
        for t in self.open_trades:
            if t.get("handled"):
                continue
            expiry_ts = float(t.get("expiry_ts") or 0)
            if now < expiry_ts - 4:
                keep.append(t)
                continue
            # Fenetre : 4 s avant l'expiration du trade suivi.
            asset = t.get("asset")
            direction = t.get("direction")
            amt = float(t.get("amount") or 0)
            entry = t.get("entry")
            cur = self.db.price_at(asset, now) if asset else None
            losing = False
            if cur is not None and entry is not None:
                losing = (cur < entry) if direction == "BUY" else (cur > entry)
            if losing and asset:
                try:
                    max_dbl = int(os.getenv("IQ_MARTINGALE_MAX", "4") or 4)
                except ValueError:
                    max_dbl = 4
                base = float(self.db.get_meta("auto_amount") or 0) or 1.0
                cap = base * (2 ** max(0, max_dbl))
                amt2 = min(cap, (amt * 2) if amt else base * 2)
                if amt2 < 1:
                    amt2 = 1.0
                opp = "SELL" if direction == "BUY" else "BUY"
                try:
                    expiry2 = int(os.getenv("IQ_COUNTER_EXPIRY", "30") or 30)
                except ValueError:
                    expiry2 = 30
                try:
                    payload = self._build_auto_message(opp, asset, amt2, expiry2)
                    sent = await self._send_ws(page, json.dumps(payload))
                    self.db.insert_order(
                        asset, opp, amt2, "auto", f"counter exp={expiry2}s ws={sent}"
                    )
                    print(
                        f"[counter] {opp} {asset} {amt2}$ (contre {direction} perdant) sent={sent}",
                        flush=True,
                    )
                    try:
                        tks = self.db.get_recent_ticks(asset, 1)
                        e2 = float(tks[-1]["price"]) if tks else None
                    except Exception:  # noqa: BLE001
                        e2 = None
                    keep.append(
                        {
                            "asset": asset,
                            "direction": opp,
                            "amount": amt2,
                            "expiry_ts": time.time() + expiry2,
                            "entry": e2,
                            "handled": False,
                        }
                    )
                    try:
                        self.db.set_meta("auto_last_signal", f"CONTRE {opp} {asset} {amt2}$")
                        self.db.set_meta("auto_last_trade_ts", str(time.time()))
                    except Exception:  # noqa: BLE001
                        pass
                except Exception as exc:  # noqa: BLE001
                    print(f"[counter] build {asset}: {exc}", flush=True)
            # Le trade d'origine est traite (contre passe ou gagnant) -> on l'enleve.
        keep = [t for t in keep if float(t.get("expiry_ts") or 0) > now - 120]
        self.open_trades = keep

    def _candles300(self, asset: str, n: int = 6) -> list:
        """Bougies 300s ; si insuffisantes, reconstruites depuis les ticks."""
        try:
            c = self.db.get_candles(asset, 300, n)
        except Exception:  # noqa: BLE001
            c = []
        if len(c) >= 3:
            try:
                last_ts = float(c[-1]["ts"])
            except (KeyError, TypeError, ValueError):
                last_ts = 0.0
            if time.time() - last_ts <= 300:
                return c
        try:
            ticks = self.db.get_recent_ticks(asset, 3000)
        except Exception:  # noqa: BLE001
            return c
        buckets = {}
        for t in ticks:
            try:
                ts = float(t["ts"])
                pr = float(t["price"])
            except (KeyError, TypeError, ValueError):
                continue
            b = int(ts // 300) * 300
            d = buckets.get(b)
            if d is None:
                buckets[b] = {"ts": b, "open": pr, "high": pr, "low": pr, "close": pr}
            else:
                d["high"] = max(d["high"], pr)
                d["low"] = min(d["low"], pr)
                d["close"] = pr
        built = [buckets[k] for k in sorted(buckets)]
        return built[-n:] if built else c

    async def _auto_step(self, page) -> None:
        from collections import Counter

        from .learner import build_strategy
        from .patterns import detect_candles

        tradable = {
            n
            for n, t in self.asset_types.items()
            if not str(t or "").lower().startswith("marginal")
        }
        imitation_general = os.getenv("IQ_IMITATION", "0") == "2"
        imitation_strict = os.getenv("IQ_IMITATION", "0") == "1"
        try:
            real_mode = (self.db.get_meta("order_account") or "demo").lower() == "real"
        except Exception:  # noqa: BLE001
            real_mode = False
        # Mode "methodes seules" : on ne suit QUE les methodes de IQ_FORCED_METHODS
        # (sens + expiration fixes), sans strategie apprise ni filtre issu des trades.
        methods_only = os.getenv("IQ_METHODS_ONLY", "0") == "1"
        all_samples = self.db.get_samples(3000)
        strat = build_strategy(all_samples, tradable_assets=tradable)
        if not strat and not (imitation_general or imitation_strict or methods_only):
            return
        if imitation_general or imitation_strict:
            # On apprend directement les figures de tes demos gagnantes (min 1).
            seen = set()
            for s in all_samples:
                if str(s.get("result") or "").lower() in ("win", "won"):
                    for t in str(s.get("patterns") or "").split(","):
                        t = t.strip()
                        if t and t not in BLACKLIST:
                            seen.add(t)
            if imitation_general and strat is not None:
                # Ne garder QUE les methodes qui ont reussi (ROI positif valide).
                allowed = list(strat.get("allowed") or [])
            else:
                allowed = list(seen)
        else:
            allowed = list(strat.get("allowed") or []) if strat else []
        if methods_only:
            allowed = []
        # Methodes forcees par l'utilisateur (ex: 2 bougies vertes -> SELL).
        forced: dict = {}
        try:
            raw_forced = os.getenv("IQ_FORCED_METHODS", "") or ""
            for part in raw_forced.split(","):
                part = part.strip()
                if not part:
                    continue
                bits = part.split(":")
                fname = bits[0].strip()
                fd = (bits[1].strip().upper() if len(bits) > 1 else "SELL")
                fe = int(bits[2]) if len(bits) > 2 and bits[2].strip().isdigit() else 300
                if fname:
                    forced[fname] = (fd if fd in ("BUY", "SELL") else "SELL", fe)
        except Exception:  # noqa: BLE001
            forced = {}
        if forced:
            allowed = list(dict.fromkeys(list(allowed) + list(forced)))
            _desc = {
                "bullish_inside": "2 bougies vertes, la 2ᵉ plus petite avec mèche BASSE longue",
                "bullish_big_lower": "rouge + plus grande verte qui englobe son corps",
                "bearish_hammer": "bougie rouge + 2ᵉ à mèche BASSE dominante (longue)",
                "green_upper_pin": "bougie verte à mèche HAUTE dominante (longue)",
                "inverted_hammer": "rouge + verte à mèche HAUTE dominante (inverted hammer)",
                "d1": "grande verte (mèche haute moyenne) puis rouge à longue mèche basse",
                "d2": "petite rouge à longue mèche basse puis grande verte propre",
                "d3": "grande verte (mèche basse moyenne) puis grande rouge propre",
                "d4": "grande rouge (mèche basse moyenne) puis grande rouge (mèche haute moyenne)",
                "d5": "grande verte (mèche haute moyenne) puis grande verte (mèche basse moyenne)",
                "d6": "grande rouge (mèche basse moyenne) puis grande verte (mèche basse moyenne)",
                "d7": "grande rouge (mèche haute moyenne) puis grande verte (mèche haute moyenne)",
                "d8": "grande verte propre puis verte moyenne à mèches moyennes",
                "d9": "deux grandes rouges à mèche basse moyenne",
                "e1": "verte (mèche haute longue) puis verte (mèche basse longue)",
                "e2": "verte moyenne puis verte à mèches haute et basse longues",
                "e3": "rouge moyenne puis verte à longue mèche haute",
                "e4": "grande rouge puis verte à mèches haute et basse longues",
                "e5": "verte (mèche haute longue) puis rouge (mèche haute longue)",
                "e6": "rouge (mèche haute longue) puis rouge (mèche basse longue)",
                "e7": "rouge (mèche basse longue) puis grande verte propre",
                "e8": "verte (mèche basse longue) puis grande rouge propre",
            }
            try:
                self.db.set_meta(
                    "active_methods",
                    json.dumps(
                        [
                            {
                                "name": n,
                                "direction": d,
                                "expiry": e,
                                "desc": _desc.get(n, ""),
                            }
                            for n, (d, e) in forced.items()
                        ]
                    ),
                )
            except Exception:  # noqa: BLE001
                pass
        # Liste blanche LOGIQUE : on ne trade que les setups definis par
        # l'utilisateur (pas les figures generiques type doji/harami/engulfing).
        try:
            logic_raw = (
                os.getenv("IQ_LOGIC_METHODS", "bearish_hammer,green_upper_pin") or ""
            ).strip()
        except Exception:  # noqa: BLE001
            logic_raw = "bearish_hammer,green_upper_pin"
        if logic_raw in ("*", "extra"):
            # "*" = tous les motifs decouverts ; "extra" = uniquement les q*
            # (jeu haute reussite 68-72%).
            try:
                from .patterns import DISCOVERED_PATTERNS as _DP

                if logic_raw == "extra":
                    forced = {
                        n: (d, 300)
                        for n, (_t1, _t2), d, _d in _DP
                        if str(n).startswith("q")
                    }
                else:
                    forced = {n: (d, 300) for n, (_t1, _t2), d, _d in _DP}
                    forced["t3"] = ("BUY", 300)
                    forced["r1"] = ("BUY", 300)
                logic = set(forced)
                try:
                    self.db.set_meta(
                        "active_methods",
                        json.dumps(
                            [
                                {
                                    "name": "auto_decouverts",
                                    "direction": "MIXTE",
                                    "expiry": int(os.getenv("IQ_FORCE_EXPIRY", "300") or 300),
                                    "desc": f"{len(forced)} motifs haute réussite (68-72%)",
                                    "pairs": [],
                                }
                            ]
                        ),
                    )
                except Exception:  # noqa: BLE001
                    pass
            except Exception:  # noqa: BLE001
                logic = set()
        else:
            logic = {x.strip() for x in logic_raw.split(",") if x.strip()}
        # Restrictions PAR PAIRE : motif -> paires ou il reussit (meta pair_rules).
        try:
            pr = json.loads(self.db.get_meta("pair_rules") or "{}")
        except Exception:  # noqa: BLE001
            pr = {}
        pair_allow: dict = {}
        if isinstance(pr, dict):
            for pat, pairs in pr.items():
                if isinstance(pairs, list):
                    pair_allow[str(pat)] = {str(x) for x in pairs if x}
        if methods_only:
            pair_allow = {}
        if pair_allow:
            logic = logic | set(pair_allow)
            if forced:
                try:
                    self.db.set_meta(
                        "active_methods",
                        json.dumps(
                            [
                                {
                                    "name": n,
                                    "direction": d,
                                    "expiry": e,
                                    "desc": _desc.get(n, ""),
                                    "pairs": sorted(pair_allow.get(n, [])),
                                }
                                for n, (d, e) in forced.items()
                            ]
                        ),
                    )
                except Exception:  # noqa: BLE001
                    pass
        # FILTRE LIVE : ne garder que les motifs qui gagnent reellement.
        live_ok = None
        try:
            if os.getenv("IQ_LIVE_FILTER", "1") == "1":
                min_n = int(os.getenv("IQ_LIVE_MIN_N", "3") or 3)
                min_rate = float(os.getenv("IQ_LIVE_MIN_RATE", "100") or 100)
                cutoff_live = time.time() - 12 * 3600
                st: dict = {}
                for s in all_samples:
                    try:
                        if float(s.get("ts") or 0) < cutoff_live:
                            continue
                    except (TypeError, ValueError):
                        continue
                    res = str(s.get("result") or "").lower()
                    if res not in ("win", "won", "loose", "lost", "loss"):
                        continue
                    win = res in ("win", "won")
                    for t in str(s.get("patterns") or "").split(","):
                        t = t.strip()
                        if not t:
                            continue
                        d = st.setdefault(t, [0, 0])
                        d[0] += 1
                        if win:
                            d[1] += 1
                ok = {
                    t
                    for t, (n, w) in st.items()
                    if n >= min_n and (100.0 * w / n) >= min_rate
                }
                if ok:
                    live_ok = ok
        except Exception:  # noqa: BLE001
            live_ok = None
        if methods_only:
            live_ok = None
        if not allowed and not (imitation_strict or imitation_general):
            return
        if strat is None or methods_only:
            # Mode imitation : pas de strategie globale, valeurs par defaut.
            strat = {"allowed": allowed, "directions": {}, "expiry": 300}
        # Mise auto : le bot suit STRICTEMENT la mise saisie par l'utilisateur.
        try:
            auto_amt = float(self.db.get_meta("auto_amount") or 0)
        except (TypeError, ValueError):
            auto_amt = 0.0
        amount = auto_amt if auto_amt > 0 else 1.0

        # === Avantage statistique par (paire, figure) sur TOUS les trades. ===
        all_samples = self.db.get_samples(3000)

        def _toks(s):
            return {t.strip() for t in str(s.get("patterns") or "").split(",") if t.strip()}

        def _ndir(d):
            d = str(d or "").upper()
            return "BUY" if d in ("CALL", "BUY") else ("SELL" if d in ("PUT", "SELL") else None)

        edge = {}
        exp_edge = {}
        acount = Counter()
        wins_all = Counter()
        pair_stat = {}
        model_wdir = {}
        model_exp = {}
        for s in all_samples:
            r = str(s.get("result") or "").lower()
            if r not in ("win", "won", "loose", "lost", "loss"):
                continue
            a = str(s.get("asset") or "?")
            if r in ("win", "won"):
                wins_all[a] += 1
            try:
                amt = float(s.get("amount") or 0)
            except (TypeError, ValueError):
                amt = 0.0
            p = s.get("profit")
            d = _ndir(s.get("direction"))
            ps = pair_stat.setdefault(a, {"w": 0, "l": 0, "net": 0.0})
            if r in ("win", "won"):
                ps["w"] += 1
                ps["net"] += (float(p) - amt) if p is not None else amt * 0.84
            else:
                ps["l"] += 1
                ps["net"] -= amt
            for t in _toks(s):
                if t not in allowed:
                    continue
                e = edge.setdefault((a, t), {"w": 0, "l": 0, "net": 0.0, "wdir": Counter()})
                if r in ("win", "won"):
                    e["w"] += 1
                    e["net"] += (float(p) - amt) if p is not None else amt * 0.84
                    if d:
                        e["wdir"][d] += 1
                        model_wdir.setdefault(t, Counter())[d] += 1
                    acount[a] += 1
                else:
                    e["l"] += 1
                    e["net"] -= amt
                es = s.get("expiration_size")
                if es:
                    ee = exp_edge.setdefault((a, t, int(es)), {"w": 0, "l": 0})
                    if r in ("win", "won"):
                        ee["w"] += 1
                        model_exp.setdefault(t, Counter())[int(es)] += 1
                    else:
                        ee["l"] += 1

        try:
            min_n = int(os.getenv("IQ_AUTO_MIN_SAMPLES", "3") or 3)
        except ValueError:
            min_n = 3
        try:
            min_rate = float(os.getenv("IQ_AUTO_MIN_RATE", "60") or 60)
        except ValueError:
            min_rate = 60.0

        # Combinaisons (paire, figure, sens) a avantage positif -> les seules tradées.
        qualified = {}
        for (a, t), e in edge.items():
            n = e["w"] + e["l"]
            if n < min_n:
                continue
            rate = 100.0 * e["w"] / n if n else 0.0
            # On qualifie par TAUX DE REUSSITE (le net est fausse par les grosses mises).
            if rate >= min_rate:
                dr = e["wdir"].most_common(1)[0][0] if e["wdir"] else (
                    (strat.get("directions") or {}).get(t, "BUY")
                )
                qualified[(a, t)] = dr

        # Methodes qualifiees GLOBALEMENT (toutes paires) : meme config sur d'autres paires.
        trade_all = os.getenv("IQ_AUTO_TRADE_ALL", "0") == "1"
        global_edge = {}
        for (_a, t), e in edge.items():
            g = global_edge.setdefault(t, {"w": 0, "l": 0, "net": 0.0, "wdir": Counter()})
            g["w"] += e["w"]
            g["l"] += e["l"]
            g["net"] += e["net"]
            g["wdir"].update(e["wdir"])
        global_qualified = {}
        for t, g in global_edge.items():
            n = g["w"] + g["l"]
            if n < min_n:
                continue
            rate = 100.0 * g["w"] / n if n else 0.0
            if rate >= min_rate and g["net"] > 0:
                global_qualified[t] = g["wdir"].most_common(1)[0][0] if g["wdir"] else (
                    (strat.get("directions") or {}).get(t, "BUY")
                )

        # Echeance optimale par combinaison (meilleur taux avec n>=3).
        qualified_expiry = {}
        for (a, t) in qualified:
            best = None
            for (aa, tt, es), ee in exp_edge.items():
                if aa != a or tt != t:
                    continue
                n = ee["w"] + ee["l"]
                if n < 1:
                    continue
                rate = 100.0 * ee["w"] / n
                if best is None or rate > best[0]:
                    best = (rate, es)
            if best:
                qualified_expiry[(a, t)] = best[1]

        approved = {a for (a, _t) in qualified}
        # Modele appris depuis tes demos : figure -> sens majoritaire gagnant.
        imitation_general = os.getenv("IQ_IMITATION", "0") == "2"
        model_patterns = {
            t: c.most_common(1)[0][0] for t, c in model_wdir.items() if c
        }
        model_expiry = {
            t: c.most_common(1)[0][0] for t, c in model_exp.items() if c
        }
        # Pas d'echeance 60 s : on la remplace par 300 s.
        model_expiry = {
            t: (300 if int(e) == 60 else int(e)) for t, e in model_expiry.items()
        }

        # Apprentissage continu : on met a jour l'etat a chaque cycle.
        try:
            closed_n = sum(
                1
                for s in all_samples
                if str(s.get("result") or "").lower()
                in ("win", "won", "loose", "lost", "loss")
            )
            self.db.set_meta("learn_trades", str(closed_n))
            self.db.set_meta("learn_last_update", str(time.time()))
        except Exception:  # noqa: BLE001
            pass

        try:
            max_pairs = int(os.getenv("IQ_AUTO_MAX_PAIRS", "50") or 50)
        except ValueError:
            max_pairs = 50
        max_pairs = max(1, max_pairs)

        def is_tradable(name):
            return not str(self.asset_types.get(name.upper()) or "").lower().startswith("marginal")

        # "Ses paires" = les paires apprises du profil (top par gains).
        try:
            top_pairs = int(os.getenv("IQ_AUTO_TOP_PAIRS", "3") or 3)
        except ValueError:
            top_pairs = 3
        try:
            min_pair_n = int(os.getenv("IQ_PAIR_MIN_SAMPLES", "5") or 5)
        except ValueError:
            min_pair_n = 5
        try:
            min_pair_rate = float(os.getenv("IQ_PAIR_MIN_RATE", "55") or 55)
        except ValueError:
            min_pair_rate = 55.0

        # On surveille/trade les paires qui ont une COMBINAISON qualifiee
        # (le taux global de la paire ne doit pas bloquer une bonne combinaison).
        # Priorite aux combinaisons qualifiees, puis on ajoute des paires OTC actives
        # (le preclose trade la direction de la bougie sur les paires surveillees).
        cands = []
        used = set()
        for a, _ in acount.most_common(400):
            if a in used or not is_tradable(a):
                continue
            if a in approved:
                cands.append(a)
                used.add(a)
            if len(cands) >= max_pairs:
                break
        try:
            target = int(os.getenv("IQ_PRECLOSE_PAIRS", "6") or 6)
        except ValueError:
            target = 6
        target = min(max_pairs, max(1, target))
        if len(cands) < target:
            otc = [
                n
                for n in self.asset_types
                if n not in used and is_tradable(n) and n.endswith("-OTC")
            ]
            otc.sort(key=lambda n: -acount.get(n, 0))
            for a in otc:
                cands.append(a)
                used.add(a)
                if len(cands) >= target:
                    break
        # Liste de surveillance explicite (IQ_WATCH_PAIRS="EURUSD-op,BTCUSD-op,EURUSD-OTC").
        try:
            watch = (os.getenv("IQ_WATCH_PAIRS") or "").strip()
        except Exception:  # noqa: BLE001
            watch = ""
        if watch:
            name_map = {n.upper(): n for n in self.asset_types}
            chosen = []
            for w in watch.split(","):
                real = name_map.get(w.strip().upper())
                if real and is_tradable(real) and real not in chosen:
                    chosen.append(real)
            if chosen:
                cands = chosen[:max_pairs]
        # Priorite : paires ACTIVES sur IQ Option (celles qui emettent du flux).
        try:
            now_ts = time.time()
            act = []
            for aid, seen_ts in self.active_seen.items():
                if now_ts - seen_ts > 120:
                    continue
                nm = self.asset_map.get(aid)
                if nm and is_tradable(nm) and nm not in act:
                    act.append(nm)
            if act:
                cands = act[:max_pairs]
        except Exception:  # noqa: BLE001
            pass
        trade_all_learned = os.getenv("IQ_TRADE_ALL_LEARNED", "0") == "1"
        if trade_all_learned:
            # Toutes les paires OTC (binaires) tradables, les plus tradées d'abord.
            otc_all = [
                n for n in self.asset_types if is_tradable(n) and n.endswith("-OTC")
            ]
            otc_all.sort(key=lambda n: (-acount.get(n, 0), n))
            cands = otc_all[:max_pairs]
        # Exclure les actifs suspendus (non tradables a l'instant).
        def _active(name: str) -> bool:
            aid = self.asset_ids.get(str(name).upper())
            return aid is not None and not self.asset_suspended.get(int(aid), False)

        # Imitation generalisee : methode apprise appliquee a TOUTES les paires.
        # En reel -> toutes les paires tradables hors OTC (les OTC sont refuses
        # par IQ avec "Actif indisponible"). En demo -> toutes les OTC.
        if imitation_general:
            if real_mode:
                pool = [
                    n
                    for n in self.asset_types
                    if is_tradable(n)
                    and (
                        n.upper().endswith("-OTC")
                        or n.upper().endswith("-OP")
                    )
                ]
            else:
                pool = [
                    n for n in self.asset_types if is_tradable(n) and n.endswith("-OTC")
                ]
            pool = [n for n in pool if _active(n)]
            pool.sort(key=lambda n: (-acount.get(n, 0), n))
            cands = pool[:max_pairs]
        # Imitation stricte : on ne surveille QUE les paires que tu as demontrees
        # (celles avec une combinaison apprise), sans elargir.
        if os.getenv("IQ_IMITATION", "0") == "1":
            strict = [
                a for a, _ in acount.most_common(400) if a in approved and is_tradable(a)
            ]
            # Surveiller aussi les paires de tes demos recentes (meme sans figure
            # detectee) et la paire affichee, pour etre pret a imiter.
            recent: list = []
            for s in all_samples[:40]:
                a = str(s.get("asset") or "").strip()
                if a and is_tradable(a) and a not in recent:
                    recent.append(a)
            try:
                now_ts = time.time()
                for aid, seen_ts in self.active_seen.items():
                    if now_ts - seen_ts > 180:
                        continue
                    nm = self.asset_map.get(aid)
                    if nm and is_tradable(nm) and nm not in recent:
                        recent.append(nm)
            except Exception:  # noqa: BLE001
                pass
            merged = strict + [a for a in recent if a not in strict]
            cands = merged[:max_pairs]
        # Trade uniquement les paires reussies : surveiller l'union des paires.
        if pair_allow:
            union: list = []
            for _pairs in pair_allow.values():
                for a in _pairs:
                    if a not in union and is_tradable(a):
                        union.append(a)
            if union:
                cands = union[:max_pairs]
        try:
            self.db.set_meta("auto_monitored", ",".join(cands))
        except Exception:  # noqa: BLE001
            pass
        if not cands:
            return

        if not self.order_template:
            return

        # Anti-rafale : au plus 1 trade auto par intervalle.
        try:
            _default_interval = 30 if imitation_general else (strat.get("expiry") or 30)
            min_interval = float(os.getenv("IQ_AUTO_MIN_INTERVAL") or _default_interval)
        except (TypeError, ValueError):
            min_interval = 30.0
        min_interval = max(5.0, min_interval)
        try:
            last_trade = float(self.db.get_meta("auto_last_trade_ts") or 0)
        except (TypeError, ValueError):
            last_trade = 0.0
        # Plusieurs trades autorises par cycle : toutes les paires qui matchent.
        try:
            max_cycle = int(os.getenv("IQ_AUTO_MAX_TRADES_PER_CYCLE", "0") or 0)
        except ValueError:
            max_cycle = 0
        placed_this_step = 0

        tf = int(os.getenv("IQ_TF", "300") or 300)
        tf = max(30, tf)
        trade_hours = set()
        try:
            raw_h = (os.getenv("IQ_TRADE_HOURS", "") or "").strip()
            if raw_h:
                trade_hours = {int(x) for x in raw_h.split(",") if x.strip().isdigit()}
        except ValueError:
            trade_hours = set()
        scan = []
        for asset in cands:
            aid = self.asset_ids.get(asset.upper())
            if aid is None:
                continue
            sub_key = (int(aid), int(tf))
            if sub_key not in self.auto_subscribed_tf:
                sub = {
                    "name": "subscribeMessage",
                    "request_id": "auto" + str(int(time.time() * 1000)),
                    "msg": {
                        "name": "candle-generated",
                        "params": {"routingFilters": {"active_id": int(aid), "size": tf}},
                    },
                }
                await self._send_ws(page, json.dumps(sub))
                self.auto_subscribed_tf.add(sub_key)
                self.auto_sub_at[int(aid)] = time.time()
            candles = self._candles_tf(asset, tf, 8)
            if len(candles) < 3:
                scan.append({"asset": asset, "bars": len(candles), "patterns": [], "matched": []})
                continue
            try:
                pre_entry = float(os.getenv("IQ_ALIGN_OFFSET", "1") or 1) < 0
            except ValueError:
                pre_entry = False
            # Entree 2s AVANT la cloture : la derniere bougie (presque finie)
            # sert de bougie signal ; sinon on exclut la bougie en formation.
            closed = candles if pre_entry else candles[:-1]
            try:
                bar_ts = float(closed[-1]["ts"])
            except (TypeError, ValueError, KeyError):
                continue
            try:
                hits = sorted({h.name for h in detect_candles(closed)})
            except Exception:  # noqa: BLE001
                hits = []
            # Respect de l'apprentissage : on ne trade que les figures positives apprises.
            if methods_only:
                # Uniquement les methodes actives (IQ_FORCED_METHODS) detectees.
                inter = [p for p in allowed if p in hits and p in forced]
            elif imitation_general:
                if logic:
                    # Uniquement les setups de la logique, et par paire autorisee.
                    inter = [
                        p
                        for p in hits
                        if p in logic
                        and (p not in pair_allow or asset in pair_allow[p])
                    ]
                else:
                    inter = [p for p in hits if p in model_patterns and p in allowed]
            elif trade_all_learned:
                inter = [p for p in allowed if p in hits and p in global_qualified]
            else:
                inter = [
                    p
                    for p in allowed
                    if p in hits and (p in forced or (asset, p) in qualified)
                ]
            # Filtre live : uniquement les motifs qui gagnent reellement.
            if live_ok:
                inter = [p for p in inter if p in live_ok]
            scan.append({"asset": asset, "bars": len(candles), "patterns": hits, "matched": inter})
            # Mode pre-cloture actif : les ordres sont passes par _preclose_step (T-5s).
            if os.getenv("IQ_PRECLOSE_ONLY", "1") == "1" and not methods_only:
                continue
            trade_anyway = os.getenv("IQ_AUTO_TRADE_EVERY_BAR", "1") == "1"
            if not inter and not trade_anyway:
                continue
            if self._auto_last_bars.get(asset) == bar_ts:
                continue
            # Ignorer uniquement les bougies TROP ANCIENNES (fraicheur < 10 min).
            if bar_ts < time.time() - max(900, 3 * tf):
                continue
            # Plusieurs trades par cycle : on prend toutes les paires qui matchent
            # (cap optionnel via IQ_AUTO_MAX_TRADES_PER_CYCLE, 0 = illimite).
            if max_cycle and placed_this_step >= max_cycle:
                break
            if inter:
                pat = inter[0]
                if pat in forced:
                    direction, expiry = forced[pat]
                elif imitation_general:
                    direction = model_patterns.get(pat) or "BUY"
                    expiry = model_expiry.get(pat) or strat.get("expiry") or 30
                else:
                    direction = qualified.get((asset, pat)) or global_qualified.get(pat) or (strat.get("directions") or {}).get(pat) or "BUY"
                    expiry = qualified_expiry.get((asset, pat)) or strat.get("expiry") or 30
            else:
                # Pas de methode : on ne trade pas (methode vide interdite).
                continue
                try:
                    lc = closed[-1]
                    direction = "BUY" if float(lc["close"]) >= float(lc["open"]) else "SELL"
                except (KeyError, TypeError, ValueError):
                    direction = "BUY"
                expiry = strat.get("expiry") or 300
            try:
                _fo = (os.getenv("IQ_FORCE_EXPIRY", "") or "").strip()
                if _fo:
                    expiry = int(_fo)
            except Exception:  # noqa: BLE001
                pass
            # Filtre expert : n'autoriser que certaines heures (sessions).
            if trade_hours and time.localtime().tm_hour not in trade_hours:
                continue
            # SIGNAL : publier AVANT l'envoi pour que le dashboard l'affiche.
            try:
                self.db.set_meta("auto_signal", f"{direction} {asset} {amount}$ ({pat})")
                self.db.set_meta("auto_signal_ts", str(time.time()))
            except Exception:  # noqa: BLE001
                pass
            try:
                payload = self._build_auto_message(direction, asset, amount, expiry)
            except Exception as exc:  # noqa: BLE001
                print(f"[auto] build {asset}: {exc}", flush=True)
                continue
            oid = None
            try:
                oid = self.db.insert_order(
                    asset, direction, amount, "auto",
                    f"pattern={pat} exp={expiry}s",
                )
            except Exception:  # noqa: BLE001
                oid = None
            rid = str(payload.get("request_id") or "")
            if oid is not None and rid:
                self.pending_auto[rid] = (int(oid), asset)
            sent = await self._send_ws(page, json.dumps(payload))
            if oid is not None:
                try:
                    self.db.update_order_status(
                        int(oid), "sent" if sent else "failed",
                        f"pattern={pat} exp={expiry}s ws={sent}",
                    )
                except Exception:  # noqa: BLE001
                    pass
                if not sent and rid:
                    self.pending_auto.pop(rid, None)
            print(
                f"[auto] {direction} {asset} {amount}$ "
                f"pattern={pat} exp={expiry}s sent={sent}",
                flush=True,
            )
            try:
                self.db.set_meta("auto_last_signal", f"{direction} {asset} {amount}$ ({pat})")
                self.db.set_meta("auto_last_trade_ts", str(time.time()))
            except Exception:  # noqa: BLE001
                pass
            placed_this_step += 1
            last_trade = time.time()
            self._auto_last_bars[asset] = bar_ts
        try:
            self.db.set_meta("auto_scan", json.dumps(scan[:60]))
            self.db.set_meta("auto_scan_ts", str(time.time()))
        except Exception:  # noqa: BLE001
            pass


    def _candles_tf(self, asset: str, tf: int, n: int = 6) -> list:
        """Bougies de timeframe `tf` ; reconstruites depuis les ticks si besoin."""
        try:
            c = self.db.get_candles(asset, tf, n)
        except Exception:  # noqa: BLE001
            c = []
        if len(c) >= 3:
            try:
                last_ts = float(c[-1]["ts"])
            except (KeyError, TypeError, ValueError):
                last_ts = 0.0
            if time.time() - last_ts <= tf:
                return c
        try:
            ticks = self.db.get_recent_ticks(asset, 4000)
        except Exception:  # noqa: BLE001
            return c
        buckets = {}
        for t in ticks:
            try:
                ts = float(t["ts"])
                pr = float(t["price"])
            except (KeyError, TypeError, ValueError):
                continue
            b = int(ts // tf) * tf
            d = buckets.get(b)
            if d is None:
                buckets[b] = {"ts": b, "open": pr, "high": pr, "low": pr, "close": pr}
            else:
                d["high"] = max(d["high"], pr)
                d["low"] = min(d["low"], pr)
                d["close"] = pr
        out = [buckets[k] for k in sorted(buckets)][-n:]
        return out or c

    def _build_auto_message(self, direction: str, asset: str, amount: float, expiry_size: int) -> dict:
        # Garde-fou commun aux boucles auto/AGS/preclose/counter : sans
        # IQ_ENABLE_EXECUTION=1, aucun ordre n'est construit ni envoye.
        if not self.settings.enable_execution:
            raise ValueError("execution desactivee (IQ_ENABLE_EXECUTION=0)")
        template = dict(self.order_template or {})
        name = asset.upper()
        aid = self.asset_ids.get(name)
        if aid is None:
            raise ValueError(f"active_id inconnu pour {asset}")
        precision = self.precisions.get(name, 6)
        ticks = self.db.get_recent_ticks(asset, 1)
        if ticks:
            price = float(ticks[-1]["price"])
        else:
            cs = self.db.get_candles(asset, 300, 1) or self.db.get_candles(asset, 60, 1)
            if not cs:
                raise ValueError(f"aucun prix pour {asset}")
            price = float(cs[-1]["close"])
        body = dict(template)
        body["active_id"] = int(aid)
        body["direction"] = "call" if direction == "BUY" else "put"
        body["price"] = float(amount)
        body["value"] = int(round(price * (10 ** precision)))
        body["expiration_size"] = int(expiry_size)
        body["expired"] = int(time.time()) + int(expiry_size)
        body.setdefault("refund_value", 0)
        # Le bot suit le compte choisi (demo ou reel), avec les memes garde-fous.
        mode = (self.db.get_meta("order_account") or "demo").lower()
        if mode == "real":
            if self.settings.demo_only:
                raise ValueError("reel refuse: collector en mode demo (IQ_DEMO_ONLY=1)")
            if os.getenv("IQ_CONFIRM_LIVE") != "YES":
                raise ValueError("reel refuse: IQ_CONFIRM_LIVE!=YES")
            bid = self.balance_ids.get(1)
            if bid is None:
                raise ValueError("solde reel introuvable")
        else:
            bid = self.balance_ids.get(4)
            if bid is None:
                raise ValueError("solde demo introuvable")
        body["user_balance_id"] = int(bid)
        return {
            "name": "sendMessage",
            "request_id": str(int(time.time() * 1000)),
            "msg": {"name": "binary-options.open-option", "version": "2.0", "body": body},
        }

    def _learn_app_grid(self, message: dict) -> None:
        """Suit le grid que l'app emet (config fraiche + plotter visible)."""
        msg = message.get("msg")
        if not isinstance(msg, dict):
            return
        if str(msg.get("name")) != "set-user-settings":
            return
        body = msg.get("body")
        if not isinstance(body, dict) or body.get("name") != "traderoom_gl_grid":
            return
        cfg = body.get("config")
        if not isinstance(cfg, dict):
            return
        try:
            self.grid_config = cfg
            self.grid_version = int(body.get("version") or 2)
        except (TypeError, ValueError):
            pass
        vis = []
        for p in cfg.get("plotters") or []:
            if isinstance(p, dict) and not p.get("isMinimized"):
                vis.append(p.get("activeId"))
        self.app_grid_visible = vis
        self.app_grid_ts = time.time()

    def _observe_positions(self, message: dict) -> None:
        """Met a jour le resultat officiel (win/loose) par position_id."""
        msg = message.get("msg")
        if not isinstance(msg, dict):
            return
        positions = msg.get("positions")
        if isinstance(positions, list):
            items = positions
        elif isinstance(msg.get("raw_event"), dict):
            items = [msg]
        else:
            items = []
        for p in items:
            if not isinstance(p, dict):
                continue
            raw = p.get("raw_event")
            changed = raw.get("binary_options_option_changed1") if isinstance(raw, dict) else None
            if not isinstance(changed, dict):
                continue
            oid = changed.get("option_id")
            res = changed.get("result")
            if oid is None or not res:
                continue
            profit = changed.get("profit_amount")
            try:
                self.db.set_order_result(
                    int(oid), str(res), float(profit) if profit is not None else None
                )
            except (TypeError, ValueError):
                continue
            # Apprentissage : on n'enregistre QUE les trades reussis (gagnants).
            try:
                r = str(res)
                if r == "opened":
                    # N'enregistrer QUE les positions issues d'un ordre confirme
                    # (evite d'importer tout l'historique du compte).
                    try:
                        known = self.db.get_sample(int(oid)) is not None or self.db.has_order_position(int(oid))
                    except Exception:  # noqa: BLE001
                        known = False
                    if known:
                        self._record_sample(changed)
                elif r in ("win", "won", "loose", "lost"):
                    final = "win" if r in ("win", "won") else "loose"
                    self.db.update_sample_result(
                        int(oid), final, float(profit) if profit is not None else None
                    )
                    if final == "win" and int(oid) not in self.noted_positions:
                        self._record_win_note(int(oid))
                        self.noted_positions.add(int(oid))
            except Exception:  # noqa: BLE001
                pass

    def _confirm_auto_order(self, message: dict) -> None:
        """Confirme un ordre auto via la reponse IQ (option acceptee / erreur)."""
        rid = str(message.get("request_id") or "")
        if not rid:
            return
        entry = self.pending_auto.pop(rid, None)
        if entry is None:
            return
        if isinstance(entry, tuple):
            oid, asset = entry[0], entry[1]
        else:
            oid, asset = entry, ""
        msg = message.get("msg")
        if not isinstance(msg, dict):
            return
        try:
            cur = self.db.get_order(int(oid)) or {}
            note = str(cur.get("note") or "")
        except Exception:  # noqa: BLE001
            note = ""
        mid = msg.get("id")
        if mid is not None:
            try:
                self.db.set_order_fill(int(oid), int(mid), None, None, None)
            except Exception:  # noqa: BLE001
                pass
            try:
                self.db.update_order_status(
                    int(oid), "filled",
                    (note + " " if note else "") + f"| accepte pos #{mid}",
                )
            except Exception:  # noqa: BLE001
                pass
            # Creer tout de suite l'echantillon (position en cours visible).
            try:
                if self.db.get_sample(int(mid)) is None:
                    pats = ""
                    if "pattern=" in note:
                        pats = note.split("pattern=", 1)[1].split(" ", 1)[0]
                    dirn = str(cur.get("direction") or "").upper()
                    direction = "CALL" if dirn == "BUY" else ("PUT" if dirn == "SELL" else dirn)
                    exp = int(cur.get("req_expiry") or 300)
                    price0 = None
                    try:
                        _t = self.db.get_recent_ticks(asset, 1)
                        if _t:
                            price0 = float(_t[-1]["price"])
                        else:
                            _c = self.db.get_candles(asset, 300, 1)
                            if _c:
                                price0 = float(_c[-1]["close"])
                    except Exception:  # noqa: BLE001
                        price0 = None
                    self.db.add_sample(
                        time.time(),
                        int(mid),
                        str(cur.get("asset") or asset),
                        direction,
                        float(cur.get("amount") or 1),
                        price0,
                        time.time() + exp,
                        exp,
                        pats,
                        "",
                        time.localtime().tm_hour,
                        "auto",
                    )
            except Exception:  # noqa: BLE001
                pass
            return
        err = msg.get("message")
        if err:
            try:
                self.db.update_order_status(
                    int(oid), "failed",
                    (note + " " if note else "") + "| " + str(err)[:120],
                )
            except Exception:  # noqa: BLE001
                pass
            # Information a afficher en banniere (refus IQ, ex: actif suspendu).
            try:
                self.db.set_meta("auto_last_error", f"{asset} — {err}")
                self.db.set_meta("auto_last_error_ts", str(time.time()))
            except Exception:  # noqa: BLE001
                pass

    def _sample_context(self, asset: str):
        try:
            candles = self._candles300(asset, 10)
        except Exception:  # noqa: BLE001
            candles = []
        names = ""
        if candles:
            try:
                from .patterns import detect_candles

                hits = detect_candles(candles)
                # Apprentissage : on ignore les figures desactivees.
                names = ",".join(sorted({h.name for h in hits} - BLACKLIST))
            except Exception:  # noqa: BLE001
                names = ""
        trend = 0
        closes = [c["close"] for c in candles if c.get("close") is not None]
        if len(closes) >= 5:
            if closes[-1] > closes[-5]:
                trend = 1
            elif closes[-1] < closes[-5]:
                trend = -1
        return names, trend

    def _record_sample(self, changed: dict) -> None:
        try:
            oid = int(changed.get("option_id"))
        except (TypeError, ValueError):
            return
        if oid in self.seen_positions:
            return
        try:
            if self.db.get_sample(oid) is not None:
                self.seen_positions.add(oid)
                return
        except Exception:  # noqa: BLE001
            pass
        self.seen_positions.add(oid)
        aid = changed.get("active_id")
        try:
            asset = self.asset_map.get(int(aid), str(aid)) if aid is not None else "?"
        except (TypeError, ValueError):
            asset = str(aid)
        direction = str(changed.get("direction") or "").upper()
        try:
            amount = float(changed.get("enrolled_amount") or changed.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        entry = None
        try:
            _tk = self.db.get_recent_ticks(asset, 1)
            if _tk:
                entry = float(_tk[-1]["price"])
        except Exception:  # noqa: BLE001
            entry = None
        if entry is None:
            try:
                entry = float(changed.get("value")) / (10 ** self.precisions.get(asset.upper(), 6))
            except (TypeError, ValueError, KeyError):
                entry = None
        expiry = changed.get("expiration_time") or changed.get("actual_expire")
        size = changed.get("expiration_size")
        source = "dashboard" if self.db.has_position(oid) else "manuel"
        patterns, trend = self._sample_context(asset)
        # Apprentissage base sur les METHODES ACTIVES : si ce trade vient d'un
        # motif actif (d1..d9), on l'ajoute aux figures de l'echantillon.
        try:
            for o in self.db.get_recent_orders(12):
                if str(o.get("asset") or "") != str(asset):
                    continue
                if time.time() - float(o.get("ts") or 0) > 45:
                    continue
                note = str(o.get("note") or "")
                if "pattern=" not in note:
                    continue
                pat = note.split("pattern=", 1)[1].split(" ", 1)[0]
                if pat and pat[0].isalpha() and pat[1:].isdigit():
                    patterns = (patterns + "," + pat) if patterns else pat
                break
        except Exception:  # noqa: BLE001
            pass
        hour = time.localtime().tm_hour
        # Timing dans la bougie de 5 min : ou l'utilisateur entre (offset) et
        # combien de secondes restent avant la cloture.
        entry_ts = time.time()
        tf = 300
        bucket = int(entry_ts // tf) * tf
        entry_offset = round(entry_ts - bucket, 1)
        sec_before_close = round(tf - entry_offset, 1)
        try:
            self.db.add_sample(
                entry_ts,
                oid,
                asset,
                direction,
                amount,
                float(entry) if entry is not None else None,
                float(expiry) if expiry is not None else None,
                int(size) if size is not None else None,
                patterns,
                trend,
                hour,
                source,
                entry_offset,
                sec_before_close,
            )
        except Exception:  # noqa: BLE001
            return
        # NOTE : on n'ecrit PAS encore de note d'apprentissage ici.
        # La note n'est creee qu'en cas de gain (voir _record_win_note).
        print(f"[learn] observe {direction} {asset} {amount:g}$ ({source})", flush=True)

    def _record_win_note(self, position_id: int) -> None:
        s = self.db.get_sample(position_id)
        if not s:
            return
        pats = str(s.get("patterns") or "").strip()
        direction = str(s.get("direction") or "").upper()
        dir_txt = "ACHAT" if direction in ("CALL", "BUY") else "VENTE"
        trend = int(s.get("trend") or 0)
        tr = "hausse" if trend > 0 else ("baisse" if trend < 0 else "neutre")
        try:
            amount = float(s.get("amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        text = (
            f"RÉUSSI: {dir_txt} {s.get('asset')} {amount:g}$ exp={s.get('expiration_size')}s "
            f"— figures: {s.get('patterns') or 'aucune'}, tendance {tr}"
        )
        try:
            sb = s.get("sec_before_close")
            off = s.get("entry_offset")
        except Exception:  # noqa: BLE001
            sb = off = None
        if sb is not None:
            text += f", entrée à {float(sb):.0f}s de la clôture (offset {float(off):.0f}s)"
        self.db.add_learning("win", text, evidence=json.dumps({"option_id": int(position_id)}))
        print("[learn] " + text, flush=True)

    def _observe_order_result(self, message: dict) -> None:
        rid = str(message.get("request_id") or "")
        if not rid:
            return
        name = str(message.get("name") or "")
        msg = message.get("msg")
        if name == "result" and isinstance(msg, dict):
            self.order_results.setdefault(rid, {})["success"] = bool(msg.get("success"))
        elif name == "option" and isinstance(msg, dict):
            entry = self.order_results.setdefault(rid, {})
            entry["option_id"] = msg.get("id")
            entry["status"] = msg.get("status")
            entry["direction"] = msg.get("direction")
            entry["price"] = msg.get("price")
            entry["profit_income"] = msg.get("profit_income")
            if entry.get("success") is None:
                entry["success"] = True
        if len(self.order_results) > 300:
            for key in list(self.order_results)[:150]:
                self.order_results.pop(key, None)

    async def _wait_order_result(self, rid: str, timeout: float = 4.0) -> Optional[dict]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            entry = self.order_results.get(rid)
            if entry and ("option_id" in entry or entry.get("success") is False):
                return entry
            await asyncio.sleep(0.2)
        return self.order_results.get(rid)

    def _learn_context(self, message: dict) -> None:
        msg = message.get("msg")
        if not isinstance(msg, dict):
            return
        body = msg.get("body")
        if not isinstance(body, dict):
            return
        active_id = body.get("active_id")
        if active_id is None:
            return
        try:
            aid = int(active_id)
        except (TypeError, ValueError):
            return
        rid = str(message.get("request_id", ""))
        if not rid:
            return
        size = body.get("size")
        self.request_ctx[rid] = {"active_id": aid, "size": size}
        if size == 300 and body.get("to_id") is not None:
            try:
                self.hist_to_id[aid] = int(body["to_id"])
            except (TypeError, ValueError):
                pass

    def _observe_pending(self, rid: str, message: dict) -> None:
        info = self.pending.get(rid)
        if not info:
            return
        msg = message.get("msg")
        candles = msg.get("candles") if isinstance(msg, dict) else None
        if not isinstance(candles, list):
            return
        ids = [c.get("id") for c in candles if isinstance(c, dict) and c.get("id") is not None]
        info["count"] = len(candles)
        info["min_id"] = min(ids) if ids else None
        ev = info.get("event")
        if ev is not None:
            ev.set()

    async def _send_ws(self, page, payload: str) -> int:
        total = 0
        # La socket peut etre dans le document principal ou dans un iframe :
        # on essaie chaque frame (avec une petite attente si rien au 1er essai).
        for attempt in range(2):
            total = 0
            for frame in page.frames:
                try:
                    total += int(await frame.evaluate(JS_SEND, payload))
                except Exception:  # noqa: BLE001
                    continue
            if total:
                return total
            if attempt == 0:
                await asyncio.sleep(1.0)
        return total

    async def _bootstrap_history(self, page) -> None:
        # Attendre que la page ait demande des bougies (ids connus).
        for _ in range(25):
            if self.hist_to_id:
                break
            await asyncio.sleep(1)
        targets = dict(self.hist_to_id)
        if not targets:
            print("[collector] historique: aucun id de bougie detecte", flush=True)
            return
        for active_id, to_id in targets.items():
            await self._fetch_pages(page, active_id, to_id)

    async def _fetch_pages(
        self, page, active_id: int, to_id: int, size: int = 300, page_size: int = 200, max_pages: int = 12
    ) -> None:
        total = 0
        cur_to = int(to_id)
        for i in range(max_pages):
            from_id = max(1, cur_to - page_size + 1)
            rid = f"hist_{active_id}_{int(time.time() * 1000)}_{i}"
            ev = asyncio.Event()
            self.pending[rid] = {"event": ev, "min_id": None, "count": 0}
            self.request_ctx[rid] = {"active_id": active_id, "size": size}
            message = {
                "name": "sendMessage",
                "request_id": rid,
                "local_time": int(time.time() * 1000),
                "msg": {
                    "name": "get-candles",
                    "version": "2.0",
                    "body": {
                        "active_id": active_id,
                        "size": size,
                        "from_id": from_id,
                        "to_id": cur_to,
                        "split_normalization": True,
                        "only_closed": True,
                    },
                },
            }
            if not await self._send_ws(page, json.dumps(message)):
                self.pending.pop(rid, None)
                break
            try:
                await asyncio.wait_for(ev.wait(), timeout=6)
            except asyncio.TimeoutError:
                self.pending.pop(rid, None)
                break
            info = self.pending.pop(rid, None) or {}
            count = info.get("count") or 0
            total += count
            if count == 0:
                break
            nxt = (info.get("min_id") or from_id) - 1
            if nxt >= cur_to:
                break
            cur_to = nxt
            if cur_to < 1:
                break
            await asyncio.sleep(0.05)
        label = self._resolve_asset(str(active_id))
        print(f"[collector] historique {label} ({size}s): +{total} bougies", flush=True)

    # ------------------------------------------------------------------
    async def _wait_until_live(self, context, page) -> None:
        # Phase 1 : attendre la connexion / authentification.
        auth_deadline = time.time() + 180
        while time.time() < auth_deadline and self.stats["frames"] == 0:
            if not getattr(self, "_hint_auth", False):
                if self.headless:
                    print("[collector] mode headless: connexion requise, utilise --visible", flush=True)
                else:
                    print("[collector] connecte-toi dans la fenetre Chromium (login / 2FA) ...", flush=True)
                self._hint_auth = True
            await asyncio.sleep(1)
        await self._save_state(context)

        # Phase 2 : s'assurer d'etre sur le traderoom.
        if "traderoom" not in (page.url or ""):
            print(f"[collector] retour au traderoom ({page.url})", flush=True)
            try:
                await page.goto(self.settings.traderoom_url, wait_until="domcontentloaded", timeout=90000)
            except Exception as exc:  # noqa: BLE001
                print(f"[collector] navigation traderoom: {exc}", flush=True)

        # Phase 3 : attendre des donnees de marche.
        market_deadline = time.time() + 180
        printed = False
        while time.time() < market_deadline:
            if self.stats["ticks"] > 0 or self.stats["candles"] > 0:
                print("[collector] donnees de marche detectees (ticks/bougies)", flush=True)
                await self._save_state(context)
                if not self._history_started:
                    self._history_started = True
                    asyncio.create_task(self._bootstrap_history(page))
                return
            if not printed:
                print(
                    "[collector] en attente des donnees de marche : ouvre le graphique d'un actif "
                    "(ex. EUR/USD) dans la fenetre",
                    flush=True,
                )
                printed = True
            await asyncio.sleep(2)
        print("[collector] ATTENTION: pas de donnees de marche. Ouvre un actif dans la fenetre.", flush=True)

    async def _save_state(self, context) -> None:
        try:
            await context.storage_state(path=str(self.settings.storage_state))
        except Exception as exc:  # noqa: BLE001
            print(f"[collector] impossible de sauver la session: {exc}", flush=True)

    def _log_stats(self) -> None:
        uptime = int(time.time() - self._started)
        known = dict(sorted(self.parser.unknown_names.items(), key=lambda kv: -kv[1])[:8])
        print(
            f"[collector] {uptime}s ws={self.stats['ws']} frames={self.stats['frames']} "
            f"ticks={self.stats['ticks']} candles={self.stats['candles']} "
            f"noms_inconnus={known}",
            flush=True,
        )


def run(asset: Optional[str] = None, headless: Optional[bool] = None) -> None:
    collector = Collector(asset=asset, headless=headless)
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        print("\n[collector] arret demande")
