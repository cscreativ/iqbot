from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

SYSTEM_PROMPT = (
    "Tu es un analyste technique de trading professionnel. On te fournit une capture "
    "d'ecran d'un graphique (souvent en bougies japonaises) et/ou les donnees OHLC des "
    "bougies de la zone selectionnee, plus une consigne decrivant un schema a reperer. "
    "Si l'image n'est pas exploitable, base ton analyse sur les donnees OHLC fournies. "
    "Tu dois :\n"
    "1) Determiner si le schema decrit est present.\n"
    "2) Si oui, proposer une direction de trade : BUY, SELL ou NONE.\n"
    "3) Estimer une confiance entre 0 et 1.\n"
    "4) Expliquer brievement en francais (2-3 phrases maximum).\n"
    "Reponds STRICTEMENT en JSON, sans aucun texte autour, avec exactement ces cles :\n"
    '{"pattern_found": true|false, "pattern_name": "...", "direction": "BUY"|"SELL"|"NONE", '
    '"confidence": 0.0-1.0, "explanation": "..."}'
)

METHOD_SYSTEM = (
    "Tu es un analyste quantitatif specialise en SCALPING. A partir d'un resume de marche "
    "(prix, variation, range, tendance EMA, RSI, dernieres bougies), propose UNE methode de "
    "SCALPING simple, claire et testable, adaptee a ce marche. Horizon court (quelques minutes), "
    "objectifs et stops serres, entrees frequentes. Donne des regles d'entree et de sortie "
    "precises, un filtrage et une gestion du risque. La description et les regles doivent "
    "etre assez explicites pour qu'un autre modele puisse decider BUY/SELL/NONE sur une "
    "fenetre de bougies. Reponds STRICTEMENT en JSON, sans texte autour :\n"
    '{"name":"...","description":"...","rules":["..."],"rationale":"..."}'
)


@dataclass
class AIConfig:
    base_url: str
    api_key: str
    model: str
    timeout: int
    vision: bool = True


def load_ai_config(db) -> AIConfig:
    pref = (os.getenv("IQ_AI_PROVIDER") or db.get_meta("ai_provider") or "").strip().lower() or None
    kilo = _read_kilo_ai(pref)
    base = os.getenv("IQ_AI_BASE_URL") or db.get_meta("ai_base_url") or kilo.get("base_url") or "https://api.openai.com/v1"
    model = os.getenv("IQ_AI_MODEL") or db.get_meta("ai_model") or kilo.get("model") or "gpt-4o-mini"
    key = os.getenv("IQ_AI_API_KEY") or db.get_meta("ai_api_key") or kilo.get("api_key") or ""
    try:
        timeout = int(os.getenv("IQ_AI_TIMEOUT", "120"))
    except ValueError:
        timeout = 120
    vision_env = os.getenv("IQ_AI_VISION")
    if vision_env is None:
        vision = bool(kilo.get("vision", True))
    else:
        vision = vision_env.strip().lower() in {"1", "true", "yes", "on", "oui"}
    return AIConfig(base_url=base.rstrip("/"), api_key=key, model=model, timeout=timeout, vision=vision)


def _strip_jsonc(text: str) -> str:
    """Retire les commentaires // et /* */ en respectant les chaines."""
    out: list[str] = []
    i = 0
    n = len(text)
    in_str = False
    esc = False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            in_str = True
            out.append(c)
            i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            i += 2
            while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _read_kilo_ai(preferred: Optional[str] = None) -> dict:
    """Lit la config IA de la machine (~/.config/kilo/kilo.jsonc) si presente."""
    path = Path.home() / ".config" / "kilo" / "kilo.jsonc"
    if not path.exists():
        return {}
    try:
        raw = _strip_jsonc(path.read_text(encoding="utf-8"))
        raw = re.sub(r",\s*([}\]])", r"\1", raw)
        cfg = json.loads(raw)
    except Exception:
        return {}

    providers = cfg.get("provider") or {}
    defaults = {
        "openai": "https://api.openai.com/v1",
        "deepseek": "https://api.deepseek.com/v1",
        "openrouter": "https://openrouter.ai/api/v1",
    }
    # openai en premier (vision) sauf si un provider est demande explicitement.
    order = ["openai", "openrouter", "deepseek"]
    if preferred and preferred in order:
        order = [preferred] + [x for x in order if x != preferred]
    for name in order:
        prov = providers.get(name)
        if not isinstance(prov, dict):
            continue
        opts = prov.get("options") or {}
        key = opts.get("apiKey") or opts.get("api_key")
        if not key:
            continue
        base = opts.get("baseURL") or opts.get("baseUrl") or opts.get("base_url") or defaults.get(name)
        models = prov.get("models") or {}
        model = next(iter(models), None) if isinstance(models, dict) and models else None
        return {
            "base_url": base or "https://api.openai.com/v1",
            "model": model or "gpt-4o-mini",
            "api_key": key,
            "provider": name,
            "vision": name in {"openai", "openrouter"},
        }
    return {}


def analyze_with_fallback(db, image, prompt, asset, context="") -> tuple[dict, AIConfig]:
    """Essaie le provider configure puis les autres providers de la machine."""
    primary = load_ai_config(db)
    candidates = [primary]
    for name in ("openai", "openrouter", "deepseek"):
        k = _read_kilo_ai(name)
        if not k:
            continue
        cand = AIConfig(
            base_url=k["base_url"],
            api_key=k["api_key"],
            model=k["model"],
            timeout=primary.timeout,
            vision=bool(k.get("vision", True)),
        )
        if (cand.base_url, cand.model, cand.api_key) != (primary.base_url, primary.model, primary.api_key):
            candidates.append(cand)

    errors: list[str] = []
    for cfg in candidates:
        try:
            return analyze_image(cfg, image, prompt, asset, context), cfg
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{cfg.model}: {exc}")
    raise RuntimeError("Aucun provider IA utilisable -> " + " | ".join(errors)[:600])


def _mock_decision() -> dict:
    return {
        "pattern_found": True,
        "pattern_name": "mock-pattern",
        "direction": "BUY",
        "confidence": 0.8,
        "explanation": "(mode mock) schema simule pour tester le flux.",
    }


def _extract_json(text: str) -> dict:
    match = re.search(r"\{.*\}", text or "", re.S)
    if not match:
        raise RuntimeError("La reponse de l'IA n'est pas du JSON exploitable.")
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"JSON IA invalide: {exc}") from exc


def _normalize(obj: dict) -> dict:
    direction = str(obj.get("direction", "NONE")).upper().strip()
    if direction not in {"BUY", "SELL", "NONE"}:
        direction = "NONE"
    try:
        confidence = float(obj.get("confidence", 0))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return {
        "pattern_found": bool(obj.get("pattern_found", False)),
        "pattern_name": str(obj.get("pattern_name") or ""),
        "direction": direction,
        "confidence": confidence,
        "explanation": str(obj.get("explanation") or ""),
    }


def _chat_text(cfg: AIConfig, system: str, text: str, max_tokens: int = 16000) -> str:
    payload = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": text},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    effort = os.getenv("IQ_AI_REASONING_EFFORT", "low").strip()
    if effort:
        payload["reasoning_effort"] = effort
    request = urllib.request.Request(
        cfg.base_url + "/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {cfg.api_key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=cfg.timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"Erreur API IA ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Impossible de joindre l'API IA: {exc.reason}") from exc
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("Reponse IA inattendue.") from exc
    if isinstance(content, list):
        content = " ".join(part.get("text", "") for part in content if isinstance(part, dict))
    return content


def suggest_method(cfg: AIConfig, asset: str, timeframe: int, summary: str) -> dict:
    text = (
        f"Actif {asset}, timeframe {timeframe}s.\n\n{summary}\n\n"
        "Propose maintenant UNE methode exploitable pour ce marche, en JSON."
    )
    content = _chat_text(cfg, METHOD_SYSTEM, text, 8000)
    obj = _extract_json(content)
    name = str(obj.get("name") or "Méthode IA")[:80]
    description = str(obj.get("description") or "")[:800]
    raw_rules = obj.get("rules") or []
    if not isinstance(raw_rules, list):
        raw_rules = [raw_rules]
    rules = [str(r)[:300] for r in raw_rules][:10]
    rationale = str(obj.get("rationale") or "")[:600]
    prompt = name + "\n" + description + "\nRègles:\n" + "\n".join("- " + r for r in rules)
    return {
        "name": name,
        "description": description,
        "rules": rules,
        "rationale": rationale,
        "prompt": prompt[:1500],
    }


def analyze_image(cfg: AIConfig, image_data_url: str, prompt: str, asset: str, context: str = "") -> dict:
    if os.getenv("IQ_AI_MOCK") == "1" or cfg.base_url.startswith("mock://"):
        return _mock_decision()
    if not (image_data_url and image_data_url.startswith("data:image")) and not context:
        raise RuntimeError("Image invalide et aucune donnee OHLC fournie.")
    if not cfg.api_key:
        raise RuntimeError(
            "Cle IA manquante : renseigne-la dans « Config IA » du dashboard "
            "ou via la variable IQ_AI_API_KEY."
        )

    text = f"Actif: {asset}.\nConsigne de l'utilisateur: {prompt}"
    if context:
        text += (
            "\n\nDonnees OHLC de la zone selectionnee (CSV, une bougie par ligne; "
            "si format tick: ts,price) :\n" + context[:20000]
        )
    content = [{"type": "text", "text": text}]
    if cfg.vision and image_data_url and image_data_url.startswith("data:image"):
        content.append({"type": "image_url", "image_url": {"url": image_data_url}})

    try:
        max_tokens = int(os.getenv("IQ_AI_MAX_TOKENS", "16000"))
    except ValueError:
        max_tokens = 16000
    payload = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    effort = os.getenv("IQ_AI_REASONING_EFFORT", "low").strip()
    if effort:
        payload["reasoning_effort"] = effort
    request = urllib.request.Request(
        cfg.base_url + "/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=cfg.timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"Erreur API IA ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Impossible de joindre l'API IA: {exc.reason}") from exc

    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("Reponse IA inattendue.") from exc
    if isinstance(content, list):
        content = " ".join(part.get("text", "") for part in content if isinstance(part, dict))
    return _normalize(_extract_json(content))
