#!/usr/bin/env python3

"""Suggest likely full addresses from a partial query using OpenStreetMap Nominatim."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
DEFAULT_USER_AGENT = "codex-sale-docs-osm-tools/1.0"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "List possible full addresses from a partial query using "
            "OpenStreetMap Nominatim."
        )
    )
    parser.add_argument("query", help="Partial address or location query.")
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of addresses to return (default: 10).",
    )
    parser.add_argument(
        "--countrycodes",
        default="",
        help="Optional ISO-3166-1 alpha-2 country codes (comma separated, e.g. ca,us).",
    )
    parser.add_argument(
        "--language",
        default="en",
        help="Preferred language (default: en).",
    )
    parser.add_argument(
        "--email",
        default="",
        help="Optional contact email for Nominatim usage policy compliance.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help=f"HTTP User-Agent header (default: {DEFAULT_USER_AGENT}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output instead of numbered lines.",
    )
    return parser


def _fetch_search_results(
    query: str,
    limit: int,
    countrycodes: str,
    language: str,
    email: str,
    user_agent: str,
    timeout_seconds: int = 20,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "dedupe": 1,
        "limit": limit,
        "accept-language": language,
    }
    normalized_codes = ",".join(
        code.strip().lower() for code in countrycodes.split(",") if code.strip()
    )
    if normalized_codes:
        params["countrycodes"] = normalized_codes
    if email.strip():
        params["email"] = email.strip()

    url = f"{NOMINATIM_SEARCH_URL}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": user_agent.strip() or DEFAULT_USER_AGENT})

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload_text = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from Nominatim: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling Nominatim: {exc}") from exc

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from Nominatim: {exc}") from exc

    if not isinstance(payload, list):
        raise RuntimeError("Unexpected Nominatim response format.")

    results: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        results.append(item)
    return results


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit must be >= 1.")
    if args.limit > 500:
        parser.error("--limit must be <= 500.")

    raw_results = _fetch_search_results(
        query=args.query,
        limit=args.limit,
        countrycodes=args.countrycodes,
        language=args.language,
        email=args.email,
        user_agent=args.user_agent,
    )

    seen: set[tuple[str, str, str]] = set()
    addresses: list[dict[str, str]] = []
    for item in raw_results:
        display_name = str(item.get("display_name", "")).strip()
        lat = str(item.get("lat", "")).strip()
        lon = str(item.get("lon", "")).strip()
        if not display_name:
            continue
        key = (display_name, lat, lon)
        if key in seen:
            continue
        seen.add(key)
        addresses.append(
            {
                "display_name": display_name,
                "lat": lat,
                "lon": lon,
                "osm_type": str(item.get("osm_type", "")).strip(),
                "osm_id": str(item.get("osm_id", "")).strip(),
            }
        )

    if args.json:
        print(json.dumps(addresses, indent=2))
        return 0

    if not addresses:
        print("No matching addresses found.")
        return 0

    for index, item in enumerate(addresses, start=1):
        display_name = item["display_name"]
        lat = item["lat"]
        lon = item["lon"]
        osm_type = item["osm_type"]
        osm_id = item["osm_id"]
        suffix = f" | lat={lat}, lon={lon}"
        if osm_type and osm_id:
            suffix += f" | {osm_type}:{osm_id}"
        print(f"{index}. {display_name}{suffix}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
