#!/usr/bin/env python3

"""Resolve an address to latitude/longitude using OpenStreetMap Nominatim."""

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
        description="Convert an exact address into latitude/longitude via OpenStreetMap Nominatim."
    )
    parser.add_argument("address", help="Exact address string.")
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
        "--limit",
        type=int,
        default=5,
        help="Number of candidates to request when matching the address (default: 5).",
    )
    parser.add_argument(
        "--result-index",
        type=int,
        default=0,
        help="Candidate index to use (default: 0).",
    )
    parser.add_argument(
        "--latlng-only",
        action="store_true",
        help="Print only 'lat,lon'.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output.",
    )
    return parser


def _fetch_search_results(
    address: str,
    limit: int,
    countrycodes: str,
    language: str,
    email: str,
    user_agent: str,
    timeout_seconds: int = 20,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "q": address,
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
        if isinstance(item, dict):
            results.append(item)
    return results


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit must be >= 1.")
    if args.limit > 50:
        parser.error("--limit must be <= 50.")
    if args.result_index < 0:
        parser.error("--result-index must be >= 0.")

    request_limit = max(args.limit, args.result_index + 1)
    results = _fetch_search_results(
        address=args.address,
        limit=request_limit,
        countrycodes=args.countrycodes,
        language=args.language,
        email=args.email,
        user_agent=args.user_agent,
    )

    if not results:
        print("No geocoding results found.")
        return 0
    if args.result_index >= len(results):
        raise RuntimeError(
            f"Requested --result-index {args.result_index}, but only {len(results)} result(s) returned."
        )

    selected = results[args.result_index]
    lat = str(selected.get("lat", "")).strip()
    lon = str(selected.get("lon", "")).strip()
    if not lat or not lon:
        raise RuntimeError("Selected result does not include latitude/longitude.")

    output = {
        "input": args.address,
        "display_name": str(selected.get("display_name", "")).strip(),
        "lat": lat,
        "lon": lon,
        "osm_type": str(selected.get("osm_type", "")).strip(),
        "osm_id": str(selected.get("osm_id", "")).strip(),
    }

    if args.json:
        print(json.dumps(output, indent=2))
        return 0
    if args.latlng_only:
        print(f"{lat},{lon}")
        return 0

    print(f"Display name: {output['display_name']}")
    print(f"Latitude: {lat}")
    print(f"Longitude: {lon}")
    print(f"LatLng: {lat},{lon}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
