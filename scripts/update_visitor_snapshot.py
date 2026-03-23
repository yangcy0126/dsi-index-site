from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from build_wdsi_data import OUTPUT_DIR, VISITOR_COUNTER_ID, build_visitor_snapshot


ROLLED_INTO_CHINA = ["Hong Kong", "Macau", "Taiwan"]


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


def unavailable_snapshot() -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "available": False,
        "counter_id": VISITOR_COUNTER_ID,
        "source": "Flag Counter public overview",
        "rolled_into_china": ROLLED_INTO_CHINA,
        "total_views": None,
        "total_visitors": None,
        "countries": [],
        "top_countries": [],
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    destination = OUTPUT_DIR / "visitor_stats.json"
    existing = load_existing_snapshot(destination)

    try:
        payload = build_visitor_snapshot()
    except Exception as exc:
        print(f"Visitor snapshot update failed: {exc}")
        if existing is not None:
            print("Keeping the existing visitor snapshot.")
            return
        payload = unavailable_snapshot()

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
