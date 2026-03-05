#!/usr/bin/env python3

"""Resolve an exact address to latitude/longitude with Google Geocoding API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert an exact address into latitude/longitude via Google Geocoding API."
    )
    parser.add_argument("address", help="Exact address to geocode.")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("GOOGLE_MAPS_API_KEY", ""),
        help="Google Maps API key (defaults to GOOGLE_MAPS_API_KEY).",
    )
    parser.add_argument(
        "--language",
        default="en",
        help="Response language (default: en).",
    )
    parser.add_argument(
        "--region",
        default="",
        help="Optional region bias as a ccTLD code (e.g. ca, us).",
    )
    parser.add_argument(
        "--result-index",
        type=int,
        default=0,
        help="Which geocoding result to use when multiple are returned (default: 0).",
    )
    parser.add_argument(
        "--latlng-only",
        action="store_true",
        help="Print only 'lat,lng'.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output.",
    )
    return parser


def _geocode_address(
    address: str,
    api_key: str,
    language: str,
    region: str,
    timeout_seconds: int = 20,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "address": address,
        "language": language,
        "key": api_key,
    }
    if region:
        params["region"] = region.strip().lower()

    url = f"{GEOCODE_URL}?{urlencode(params)}"
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload_text = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from Google Geocoding API: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling Google Geocoding API: {exc}") from exc

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from Google Geocoding API: {exc}") from exc

    status = payload.get("status", "")
    if status == "ZERO_RESULTS":
        return []
    if status != "OK":
        message = payload.get("error_message", "Unknown API error.")
        raise RuntimeError(f"Google Geocoding API status={status}: {message}")

    results = payload.get("results", [])
    if not isinstance(results, list):
        raise RuntimeError("Google Geocoding API response had an unexpected format.")
    return results


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    api_key = args.api_key.strip()
    if not api_key:
        parser.error("Missing API key. Set GOOGLE_MAPS_API_KEY or pass --api-key.")
    if args.result_index < 0:
        parser.error("--result-index must be >= 0.")

    results = _geocode_address(
        address=args.address,
        api_key=api_key,
        language=args.language,
        region=args.region,
    )
    if not results:
        print("No geocoding results found.")
        return 0

    if args.result_index >= len(results):
        raise RuntimeError(
            f"Requested --result-index {args.result_index}, but only {len(results)} result(s) returned."
        )

    selected = results[args.result_index]
    geometry = selected.get("geometry", {})
    location = geometry.get("location", {})
    lat = location.get("lat")
    lng = location.get("lng")
    if lat is None or lng is None:
        raise RuntimeError("Selected result does not include latitude/longitude.")

    output = {
        "input": args.address,
        "formatted_address": selected.get("formatted_address", ""),
        "place_id": selected.get("place_id", ""),
        "lat": lat,
        "lng": lng,
    }

    if args.json:
        print(json.dumps(output, indent=2))
        return 0

    if args.latlng_only:
        print(f"{lat},{lng}")
        return 0

    print(f"Formatted address: {output['formatted_address']}")
    print(f"Latitude: {lat}")
    print(f"Longitude: {lng}")
    print(f"LatLng: {lat},{lng}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
