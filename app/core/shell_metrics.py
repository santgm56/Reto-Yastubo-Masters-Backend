from __future__ import annotations

from threading import Lock

_lock = Lock()
_totals: dict[str, int] = {"admin": 0, "seller": 0, "customer": 0}
_paths: dict[str, dict[str, int]] = {"admin": {}, "seller": {}, "customer": {}}


def increment_shell_disabled(realm: str, path: str) -> None:
    normalized_realm = str(realm or "").strip().lower()
    normalized_path = str(path or "").strip() or "/"
    if normalized_realm not in _totals:
        return

    with _lock:
        _totals[normalized_realm] += 1
        bucket = _paths[normalized_realm]
        bucket[normalized_path] = int(bucket.get(normalized_path) or 0) + 1


def get_snapshot() -> dict[str, object]:
    with _lock:
        return {
            "totals": dict(_totals),
            "paths": {
                "admin": dict(_paths["admin"]),
                "seller": dict(_paths["seller"]),
                "customer": dict(_paths["customer"]),
            },
        }


def reset_for_tests() -> None:
    with _lock:
        _totals["admin"] = 0
        _totals["seller"] = 0
        _totals["customer"] = 0
        _paths["admin"].clear()
        _paths["seller"].clear()
        _paths["customer"].clear()