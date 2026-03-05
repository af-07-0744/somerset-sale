from __future__ import annotations

import re
from pathlib import Path
from typing import Any

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]


DEFAULT_SUBJECT_ADDRESS = "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2"
DEFAULT_STREET_PORTION = ""


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    return _normalize_space(str(value))


def _tool_sale_table(payload: dict[str, Any]) -> dict[str, Any]:
    tool_obj = payload.get("tool")
    if not isinstance(tool_obj, dict):
        return {}
    sale_obj = tool_obj.get("sale")
    if not isinstance(sale_obj, dict):
        return {}
    return sale_obj


def load_sale_settings(pyproject_path: Path = Path("pyproject.toml")) -> dict[str, str]:
    settings = {
        "subject_address": DEFAULT_SUBJECT_ADDRESS,
        "street_portion": DEFAULT_STREET_PORTION,
    }

    if tomllib is None or not pyproject_path.exists():
        return settings

    try:
        payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except Exception:
        return settings

    sale_table = _tool_sale_table(payload)
    if not sale_table:
        return settings

    subject_address = _to_text(sale_table.get("subject_address"))
    street_portion = _to_text(sale_table.get("street_portion"))

    if subject_address:
        settings["subject_address"] = subject_address
    if street_portion:
        settings["street_portion"] = street_portion
    return settings
