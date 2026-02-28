#!/usr/bin/env python3

"""Suggest full addresses for a partial query via Google Places Autocomplete."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


AUTOCOMPLETE_URL = "https://maps.googleapis.com/maps/api/place/autocomplete/json"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "List possible full addresses from a partial query using "
            "Google Places Autocomplete."
        )
    )
    parser.add_argument("query", help="Partial address or search term.")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("GOOGLE_MAPS_API_KEY", ""),
        help="Google Maps API key (defaults to GOOGLE_MAPS_API_KEY).",
    )
    parser.add_argument(
        "--country",
        default="",
        help="Optional country restriction as a 2-letter code (e.g. CA, US).",
    )
    parser.add_argument(
        "--language",
        default="en",
        help="Response language (default: en).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of suggestions to print (default: 10).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output instead of numbered lines.",
    )
    return parser


def _fetch_predictions(
    query: str,
    api_key: str,
    country: str,
    language: str,
    timeout_seconds: int = 20,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "input": query,
        "types": "address",
        "language": language,
        "key": api_key,
    }
    if country:
        params["components"] = f"country:{country.strip().lower()}"

    url = f"{AUTOCOMPLETE_URL}?{urlencode(params)}"
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload_text = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from Google Places API: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling Google Places API: {exc}") from exc

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from Google Places API: {exc}") from exc

    status = payload.get("status", "")
    if status == "ZERO_RESULTS":
        return []
    if status != "OK":
        message = payload.get("error_message", "Unknown API error.")
        raise RuntimeError(f"Google Places API status={status}: {message}")

    predictions = payload.get("predictions", [])
    if not isinstance(predictions, list):
        raise RuntimeError("Google Places API response had an unexpected format.")
    return predictions


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    api_key = args.api_key.strip()
    if not api_key:
        parser.error("Missing API key. Set GOOGLE_MAPS_API_KEY or pass --api-key.")
    if args.limit < 1:
        parser.error("--limit must be >= 1.")

    predictions = _fetch_predictions(
        query=args.query,
        api_key=api_key,
        country=args.country,
        language=args.language,
    )

    seen: set[tuple[str, str]] = set()
    addresses: list[dict[str, str]] = []
    for prediction in predictions:
        description = str(prediction.get("description", "")).strip()
        place_id = str(prediction.get("place_id", "")).strip()
        if not description:
            continue
        key = (description, place_id)
        if key in seen:
            continue
        seen.add(key)
        addresses.append({"address": description, "place_id": place_id})
        if len(addresses) >= args.limit:
            break

    if args.json:
        print(json.dumps(addresses, indent=2))
        return 0

    if not addresses:
        print("No matching addresses found.")
        return 0

    for index, item in enumerate(addresses, start=1):
        address = item["address"]
        place_id = item["place_id"]
        suffix = f" | place_id={place_id}" if place_id else ""
        print(f"{index}. {address}{suffix}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
