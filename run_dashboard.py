from __future__ import annotations

import argparse
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import uvicorn  # noqa: E402

from iqopt.config import load_settings  # noqa: E402
from iqopt.dashboard import create_app  # noqa: E402


def _lan_ip() -> str:
    """IP locale de la machine sur le reseau (pour ouvrir depuis le telephone)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        sock.close()


def main() -> None:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Dashboard web IQ Traderoom (accessible depuis le telephone)")
    parser.add_argument("--host", default=settings.dashboard_host, help="Adresse d'ecoute")
    parser.add_argument("--port", type=int, default=settings.dashboard_port, help="Port")
    parser.add_argument(
        "--lan",
        action="store_true",
        help="Ecoute sur 0.0.0.0 pour etre joignable depuis le telephone (meme Wi-Fi)",
    )
    args = parser.parse_args()

    host = "0.0.0.0" if args.lan else args.host
    app = create_app(settings)

    if host in {"0.0.0.0", "::"}:
        ip = _lan_ip()
        token = settings.dashboard_token
        suffix = f"?token={token}" if token else ""
        print(f"[dashboard] local   : http://127.0.0.1:{args.port}/", flush=True)
        print(f"[dashboard] telephone: http://{ip}:{args.port}/{suffix}", flush=True)
        if not token:
            print(
                "[dashboard] astuce: definis IQ_DASHBOARD_TOKEN=... pour proteger l'acces "
                "depuis le telephone",
                flush=True,
            )
    else:
        print(f"[dashboard] http://{host}:{args.port}", flush=True)

    uvicorn.run(app, host=host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
