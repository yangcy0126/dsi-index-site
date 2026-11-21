from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from build_wdsi_data import OUTPUT_DIR, build_visitor_snapshot


def published_snapshot_is_stale(expected: dict[str, object]) -> bool:
    url = f"https://dsi-index.org/data/visitor_stats.json?check={int(time.time()) // 300}"
    request = Request(url, headers={"Accept": "application/json", "Cache-Control": "no-cache"})
    try:
        with urlopen(request, timeout=20) as response:
            published = json.load(response)
    except HTTPError as exc:
        if exc.code == 404:
            return True
        raise
    return comparable_snapshot(expected) != comparable_snapshot(published)


def load_existing_snapshot(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def comparable_snapshot(payload: dict[str, object] | None) -> dict[str, object] | None:
    if payload is None:
        return None
    comparable = dict(payload)
    comparable.pop("generated_at", None)
    return comparable


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    destination = OUTPUT_DIR / "visitor_stats.json"
    existing = load_existing_snapshot(destination)

    try:
        payload = build_visitor_snapshot()
    except Exception as exc:
        raise SystemExit(f"Visitor snapshot not updated: {exc}") from exc

    if comparable_snapshot(existing) == comparable_snapshot(payload):
        print("Visitor snapshot unchanged.")
        return

    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Updated visitor snapshot in {destination}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--needs-deploy", action="store_true",
                        help="Compare the saved snapshot to the one on the live site.")
    if parser.parse_args().needs_deploy:
        snapshot = json.loads((OUTPUT_DIR / "visitor_stats.json").read_bytes())
        print("true" if published_snapshot_is_stale(snapshot) else "false")
    else:
        main()
