# IQ Traderoom — lecture temps réel (lecture seule par défaut)

Projet Python + Playwright qui lit le flux temps réel du traderoom IQ Option
(`https://iqoption.com/traderoom`) en **interceptant le WebSocket** de la page,
au lieu de scraper le canvas (illisible et fragile).

> ⚠️ Avertissement : l'accès automatisé au traderoom peut contrevenir aux CGU
> d'IQ Option. Le trading automatisé présente un risque financier réel.
> La collecte est en lecture seule et l'exécution d'ordres est **désactivée par défaut**.

## Architecture

```
Playwright (navigateur connecté)
   └── page.on("websocket") ──► trames brutes
          ├── captures/frames-*.jsonl   (traçabilité / debug)
          └── parser heuristique ──► ticks + bougies ──► SQLite (data/iqopt.sqlite3)
                                                              │
                                         FastAPI + WebSocket ──► dashboard live (static/index.html)
```

- `src/iqopt/collector.py` : pilote Chromium, se connecte, sauvegarde la session, intercepte les trames.
- `src/iqopt/parser.py` : décodage binaire length-prefixed + parsing tolérant (ticks/bougies).
- `src/iqopt/storage.py` : SQLite (ticks, bougies, signaux, ordres).
- `src/iqopt/dashboard.py` : API + WebSocket temps réel.
- `src/iqopt/signals.py` : RSI + EMA (exemple de génération de signaux).
- `src/iqopt/patterns.py` : détection de formations de bougies (pur, sans I/O).
- `src/iqopt/strategy.py` : sélection de la figure la plus forte → décision BUY/SELL.
- `src/iqopt/pattern_bot.py` : bot qui lit les bougies, détecte et passe les ordres.
- `src/iqopt/executor.py` : exécuteur gardé, **dry-run par défaut**.

## Installation

```bash
cd ~/Projects/iqoption-traderoom
source .venv/bin/activate          # déjà créé
pip install -r requirements.txt
playwright install chromium
```

## 1. Première connexion (navigateur visible)

```bash
python run_collector.py --visible
```

- Une fenêtre Chromium s'ouvre sur le traderoom.
- Connecte-toi (login + 2FA). Ouvre un actif (ex. EUR/USD).
- Dès que des trames arrivent, la session est sauvegardée dans `playwright_state.json`
  et tu verras `flux de marche detecte`.
- `Ctrl+C` pour arrêter.

## 2. Collecte (session déjà sauvegardée)

```bash
python run_collector.py --headless
# ou avec un actif par défaut :
IQ_ASSET=EURUSD python run_collector.py --headless
```

Données écrites dans `data/iqopt.sqlite3`, trames brutes dans `captures/`.

## 3. Dashboard live

Dans un autre terminal :

```bash
python run_dashboard.py
# http://127.0.0.1:8765
```

Affiche le prix en temps réel, l'historique des ticks et les signaux.
Le graphique est verrouillé sur des **bougies 5 min** (temps réel : bougies serveur
mises à jour en direct, sinon reconstruites depuis les ticks).

## 4. Utilisation depuis le téléphone (web)

Le dashboard est une app web : accessible depuis le navigateur du téléphone
tant que le Mac et le téléphone sont sur le **même Wi-Fi**.

```bash
# écoute sur 0.0.0.0 et affiche l'adresse à ouvrir depuis le téléphone
python run_dashboard.py --lan
# [dashboard] telephone: http://192.168.x.x:8765/
```

- L'interface est **responsive** (graphique + panneaux empilés sur petit écran).
- Le sélecteur d'actif liste **tout le catalogue IQ Option** (Forex, crypto,
  indices, actions, ETF, CFD), groupé par catégorie ; un **●** marque les paires
  qui reçoivent du flux en direct.
- Le panneau **« Bot formations de bougies »** permet de démarrer/arrêter le bot
  depuis le téléphone (source, taille de bougie, force, mise, filtre tendance,
  ordre démo ou signaux seuls) et d'afficher son état en direct.
- Pour protéger l'accès réseau, définis un jeton :

```bash
IQ_DASHBOARD_TOKEN=monCode python run_dashboard.py --lan
# ouvrir une fois : http://192.168.x.x:8765/?token=monCode
# (le cookie est ensuite mémorisé par le navigateur)
```

> Le feu vert (HTTP) sur le Wi-Fi local suffit pour un usage perso. N'expose pas
> ce port sur Internet sans HTTPS et mot de passe.

## Variables d'environnement

| Variable | Défaut | Rôle |
|---|---|---|
| `IQ_ASSET` | `EURUSD` | Actif par défaut |
| `IQ_TRADEROOM_URL` | `https://iqoption.com/traderoom` | URL cible |
| `IQ_HEADLESS` | `false` | Navigateur invisible |
| `IQ_CAPTURE_RAW` | `true` | Écrire les trames brutes en JSONL |
| `IQ_DATA_DIR` / `IQ_CAPTURES_DIR` | `./data` / `./captures` | Emplacements |
| `IQ_ENABLE_EXECUTION` | `false` | Active la soumission d'ordres |
| `IQ_DEMO_ONLY` | `true` | N'autorise que le compte démo |
| `IQ_CONFIRM_LIVE` | (vide) | Doit valoir `YES` pour un compte réel |
| `IQ_DASHBOARD_HOST` / `IQ_DASHBOARD_PORT` | `127.0.0.1` / `8765` | Serveur |
| `IQ_DASHBOARD_TOKEN` | (vide) | Jeton d'accès optionnel au dashboard (`?token=`) |

## Exécution d'ordres (avancé, désactivé)

`src/iqopt/executor.py` enregistre d'abord des ordres en **dry-run**.
Le branchement réel sur les boutons du DOM n'est **pas** fourni : les sélecteurs
d'IQ Option changent et doivent être calibrés sur ta version, avec un compte démo.
Garde-fous : `IQ_ENABLE_EXECUTION=1`, `IQ_DEMO_ONLY=1` (défaut), et
`IQ_CONFIRM_LIVE=YES` requis pour le live.

## Formats confirmés (vérifiés sur trafic réel)

- **Prix/bougies live** : message `candle-generated` (`microserviceName: quotes`) avec
  `active_id`, `size` (timeframe en s), `from`, `open`, `close`, `min` (= low), `max` (= high).
  Le prix live est dérivé du `close` de chaque mise à jour.
- **Catalogue** : `underlying-list` (`items` ou `underlying`) → mapping `active_id` → nom
  (résolu automatiquement, ex. `76` → `EURUSD-OTC`).
- Les réponses historiques `candles` / `first-candles` ne portent pas d'`active_id` et ne sont
  donc pas attribuées (ignorées) : l'historique se construit à partir des `candle-generated`.

Le parseur ignore désormais les messages non-marché (`trading-params`, `positions`, `currency-updated`…)
qui produisaient de faux prix. Les trames brutes restent dans `captures/*.jsonl`.

## Sélection de zone + analyse IA

Dans le panneau **IA — Pattern** du dashboard :

1. **Sélectionner la zone à capturer** → dessine un rectangle sur le graphique
   (l'image de cette zone est capturée et affichée en aperçu).
2. Décris le **schéma à surveiller** (« double bottom », « triangle ascendant »…).
3. **Analyser** → l'image + ta consigne sont envoyées à une IA vision qui répond
   en JSON (`pattern_found`, `direction`, `confidence`, `explanation`).
4. **Surveil** (10/30/60 s) → ré-analyse automatiquement la zone à intervalle régulier.
5. **Passer l'ordre (démo)** → si le schéma est trouvé et la confiance ≥ seuil,
   un ordre est soumis via l'exécuteur **gardé** (dry-run par défaut).

### Configurer l'IA

Le dashboard **lit automatiquement la config IA de la machine**
(`~/.config/kilo/kilo.jsonc`) : providers `openai` / `deepseek`, clé et modèle,
avec un **fallback** automatique entre providers disponibles.

- Sur cette machine : `openai/gpt-6-luna` (vision) est **à court de crédits** (429),
  donc l'analyse utilise `deepseek/deepseek-v4-pro` (texte) avec les **données OHLC**
  de la zone sélectionnée. Cela fonctionne sans crédits vision.
- Si tu ajoutes des crédits OpenAI (ou un autre modèle vision), il analysera
  directement **l'image**.

Priorité : variables d'environnement > `Config IA` (dashboard) > `kilo.jsonc`.

```bash
export IQ_AI_PROVIDER=deepseek      # ou openai
export IQ_AI_MODEL=deepseek-v4-pro
export IQ_AI_MAX_TOKENS=8000        # requis pour les modèles de raisonnement
export IQ_AI_MOCK=1                 # test sans appel API
```

> ⚠️ L'exécution réelle des ordres reste **désactivée** : les ordres sont
> enregistrés en `dry_run`. Le branchement DOM et le passage en réel exigent une
> configuration explicite (`IQ_ENABLE_EXECUTION`, `IQ_DEMO_ONLY`,
> `IQ_CONFIRM_LIVE`) et restent à ta charge.

## Replay / Backtest IA

> **Mode scalping** : par défaut le replay et la séquence live utilisent des bougies
> courtes **15 / 30 / 60 s**, reconstruites depuis les ticks (les bougies serveur ne
> servent qu'à partir de 5 min). Horizons et sorties sont courts.

Bouton **Replay** dans l'en-tête du dashboard :

1. Clique sur **Générer une méthode IA** : l'IA analyse le marché (résumé OHLC,
   EMA9/21, RSI14) et propose une **méthode testable**. Le replay se lance
   automatiquement (option décochable). Tu peux aussi saisir ta propre méthode.
2. Paramètres : bougies d'historique, fenêtre IA, pas, étapes max, horizon de sortie, confiance min.
3. L'IA rejoue l'historique 5 min **pas à pas** — à chaque étape elle ne voit que le **passé**
   (pas de biais de futur) — détecte les entrées, **simule** les positions (entrée/sortie,
   PnL %, MFE/MAE) et calcule win rate, PnL total et profit factor.
4. Si la méthode est **bonne** (≥ 5 trades, ≥ 55 % de réussite, PnL &gt; 0), elle est
   **enregistrée** dans « Méthodes enregistrées ».
5. Le bouton **Utiliser** charge la méthode dans le panneau IA live pour l'appliquer
   (Analyser / Surveil).

Le replay tourne en **tâche de fond** avec progression (les étapes IA sont parallélisées),
sur les bougies 300 s disponibles.

## Séquence live (appliquer une méthode)

Une méthode enregistrée peut être exécutée en continu sur le marché réel :

- Dans **Méthodes enregistrées** → bouton **Live** (démarre directement), ou
  **Utiliser** puis **Démarrer** dans la section **Séquence live**.
- Toutes les `interval` secondes, le runner réévalue la méthode sur les **20 dernières
  bougies** (fenêtre passée uniquement), enregistre les signaux dans la table `signals`
  et, si `auto_trade` est activé et la confiance ≥ seuil, soumet un ordre **démo (dry-run)**.
- Les signaux sont dédupliqués par `(direction, bougie)` pour éviter les doublons.
- `Arrêter` stoppe la séquence.

> L'exécution réelle reste désactivée : les ordres sont enregistrés en `dry_run`.

## Bot formations de bougies

Lit les bougies du collector, détecte une **formation de bougies sur la dernière
bougie clôturée** et soumet un ordre via l'exécuteur gardé (dry-run par défaut).

Figures détectées : `doji`, `hammer`, `shooting_star`, `bullish_engulfing`,
`bearish_engulfing`, `bullish_harami`, `bearish_harami`, `morning_star`,
`evening_star`, `three_white_soldiers`, `three_black_crows`.

```bash
# 1) le collector doit tourner dans un autre terminal
python run_collector.py --headless

# 2) bot (bougies 60 s reconstruites depuis les ticks, dry-run)
python run_pattern_bot.py --asset EURUSD-OTC --interval 300

# signaux seulement (aucun ordre), figures autorisées, seuil de force
python run_pattern_bot.py --no-trade --allow hammer,bullish_engulfing --min-strength 0.6

# bougies serveur plutôt que ticks
python run_pattern_bot.py --source candles --timeframe 300
```

- La dernière bougie (en cours de formation) est **ignorée** : on ne décide que
  sur des bougies clôturées.
- Un signal n'est traité qu'**une seule fois par bougie** (dédup par `ts`).
- Garde-fous : `--max-trades` (0 = illimité), `--cooldown` (s entre 2 trades),
  `--min-strength` (0..1), `--require-trend` (filtre momentum).
- L'exécution reste en **dry-run** tant que `IQ_ENABLE_EXECUTION` n'est pas activé
  (voir « Exécution d'ordres »).

Tests : `python -m unittest discover -s tests -v`.

## Positions Achat / Vente (exécution)

Le dashboard expose des boutons **ACHAT / VENTE** (panneau « Position »). Un clic
crée un ordre dans la file `orders` (statut `queued`) ; le **collector** (qui possède
le navigateur connecté) l'exécute et met à jour son statut (`dry_run`, `sent`,
`failed`…).

> Le traderoom IQ est rendu en **canvas** (pas de boutons dans le DOM). Les ordres
> sont donc envoyés via le **message WebSocket** qu'IQ utilise lui-même :
> `binary-options.open-option`, **rejoué depuis un modèle capturé** sur un de tes
> trades manuels. Cela garantit le bon compte, le bon `option_type` et le bon payout.

### Activation (toujours tester en démo d'abord)

1. Dans la fenêtre Chromium, place **un trade manuel** (compte démo, durée souhaitée
   ex. 5 min). Le collector affiche `[orders] modele capture: ...` et mémorise le modèle.
2. Relance le collector avec l'exécution activée :

```bash
# démo
IQ_ENABLE_EXECUTION=1 IQ_DEMO_ONLY=1 python run_collector.py --visible
# réel (à tes risques) : nécessite en plus IQ_CONFIRM_LIVE=YES
IQ_ENABLE_EXECUTION=1 IQ_DEMO_ONLY=0 IQ_CONFIRM_LIVE=YES python run_collector.py --visible
```

3. Utilise les boutons ACHAT / VENTE du dashboard.

Garde-fous : **dry-run par défaut**, plafond `IQ_MAX_ORDER_AMOUNT` (défaut 100),
confirmation dans l'UI, refus si la paire n'est pas active, refus du réel sans
`IQ_CONFIRM_LIVE=YES`, et refus si aucun modèle n'a été capturé.

## Prochaines étapes

1. Afficher de vraies bougies (pas seulement la ligne de prix) dans le dashboard.
2. Corréler `request_id` pour attribuer l'historique `candles` / `first-candles`.
3. Brancher les signaux sur les bougies, puis (optionnel) l'exécution démo.
4. Exposer le bot de formations dans le dashboard (bouton « Live » dédié).
