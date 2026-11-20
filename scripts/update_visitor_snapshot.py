from __future__ import annotations

import json
from pathlib import Path

from build_wdsi_data import OUTPUT_DIR, build_visitor_snapshot


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
    main()
