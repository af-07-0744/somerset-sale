import argparse
import json
import re
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
CALGARY_PARCEL_ADDRESS_URL = "https://data.calgary.ca/resource/9zvu-p8uz.json"
DEFAULT_USER_AGENT = "codex-sale-docs-osm-tools/1.0"


def _parse_countrycodes(raw_value: str) -> str:
    codes = [code.strip().lower() for code in raw_value.split(",") if code.strip()]
    return ",".join(codes)


def _has_house_number(query: str) -> bool:
    return bool(re.search(r"\b\d{1,6}[A-Za-z]?\b", query))


def _http_get_json(
    url: str,
    params: dict[str, str | int],
    user_agent: str,
    timeout_seconds: int,
) -> Any:
    full_url = f"{url}?{urlencode(params)}"
    request = Request(full_url, headers={"User-Agent": user_agent})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload_text = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from upstream API: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling upstream API: {exc}") from exc

    try:
        return json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from upstream API: {exc}") from exc


def _http_post_json(url: str, data: str, user_agent: str, timeout_seconds: int) -> Any:
    encoded = data.encode("utf-8")
    request = Request(
        url,
        data=encoded,
        headers={
            "User-Agent": user_agent,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload_text = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from upstream API: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling upstream API: {exc}") from exc

    try:
        return json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from upstream API: {exc}") from exc


def _nominatim_search(
    query: str,
    limit: int,
    countrycodes: str,
    language: str,
    email: str,
    user_agent: str,
    timeout_seconds: int = 25,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "dedupe": 1,
        "limit": limit,
        "accept-language": language,
    }
    normalized_countrycodes = _parse_countrycodes(countrycodes)
    if normalized_countrycodes:
        params["countrycodes"] = normalized_countrycodes
    if email.strip():
        params["email"] = email.strip()

    payload = _http_get_json(
        NOMINATIM_SEARCH_URL,
        params=params,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
    )
    if not isinstance(payload, list):
        raise RuntimeError("Unexpected Nominatim response format.")
    return [item for item in payload if isinstance(item, dict)]


def _parse_bbox(candidate: dict[str, Any]) -> tuple[float, float, float, float] | None:
    raw_bbox = candidate.get("boundingbox")
    if not isinstance(raw_bbox, list) or len(raw_bbox) != 4:
        return None
    try:
        south = float(raw_bbox[0])
        north = float(raw_bbox[1])
        west = float(raw_bbox[2])
        east = float(raw_bbox[3])
    except (ValueError, TypeError):
        return None
    return (south, west, north, east)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _bbox_center(candidate: dict[str, Any], bbox: tuple[float, float, float, float]) -> tuple[float, float]:
    south, west, north, east = bbox
    center_lat = (south + north) / 2.0
    center_lon = (west + east) / 2.0
    try:
        center_lat = float(candidate.get("lat", center_lat))
    except (TypeError, ValueError):
        center_lat = (south + north) / 2.0
    try:
        center_lon = float(candidate.get("lon", center_lon))
    except (TypeError, ValueError):
        center_lon = (west + east) / 2.0
    return (center_lat, center_lon)


def _expanded_bboxes_for_street(
    candidate: dict[str, Any], bbox: tuple[float, float, float, float]
) -> list[tuple[float, float, float, float]]:
    south, west, north, east = bbox
    center_lat, center_lon = _bbox_center(candidate, bbox)
    half_lat = max((north - south) / 2.0, 0.0003)
    half_lon = max((east - west) / 2.0, 0.0003)
    span_levels = [
        (half_lat, half_lon),
        (max(half_lat, 0.0050), max(half_lon, 0.0050)),
        (max(half_lat, 0.0150), max(half_lon, 0.0150)),
    ]

    expanded: list[tuple[float, float, float, float]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for span_lat, span_lon in span_levels:
        candidate_south = _clamp(center_lat - span_lat, -90.0, 90.0)
        candidate_north = _clamp(center_lat + span_lat, -90.0, 90.0)
        candidate_west = _clamp(center_lon - span_lon, -180.0, 180.0)
        candidate_east = _clamp(center_lon + span_lon, -180.0, 180.0)
        key = (
            f"{candidate_south:.6f}",
            f"{candidate_west:.6f}",
            f"{candidate_north:.6f}",
            f"{candidate_east:.6f}",
        )
        if key in seen:
            continue
        seen.add(key)
        expanded.append((candidate_south, candidate_west, candidate_north, candidate_east))
    return expanded


def _street_name(candidate: dict[str, Any]) -> str:
    address = candidate.get("address", {})
    if isinstance(address, dict):
        for key in ("road", "pedestrian", "residential", "path", "street"):
            value = str(address.get(key, "")).strip()
            if value:
                return value
    display_name = str(candidate.get("display_name", "")).strip()
    if display_name:
        return display_name.split(",", maxsplit=1)[0].strip()
    return ""


def _looks_like_street(candidate: dict[str, Any]) -> bool:
    osm_type = str(candidate.get("osm_type", "")).strip().lower()
    category = str(candidate.get("category", "")).strip().lower()
    location_type = str(candidate.get("type", "")).strip().lower()
    street_types = {
        "road",
        "residential",
        "service",
        "tertiary",
        "secondary",
        "primary",
        "unclassified",
        "pedestrian",
        "living_street",
        "trunk",
        "motorway",
        "footway",
    }
    if osm_type == "way" and (category in {"highway", "place"} or location_type in street_types):
        return True
    return location_type in street_types


def _overpass_query_for_street(street_name: str, bbox: tuple[float, float, float, float]) -> str:
    south, west, north, east = bbox
    _ = street_name
    return (
        "[out:json][timeout:25];"
        "("
        f'node["addr:housenumber"]({south},{west},{north},{east});'
        f'way["addr:housenumber"]({south},{west},{north},{east});'
        f'relation["addr:housenumber"]({south},{west},{north},{east});'
        ");"
        "out body center;"
    )


def _sort_house_number(raw_value: str) -> tuple[int, str]:
    token = raw_value.strip().upper()
    match = re.match(r"(\d+)", token)
    if match:
        return (int(match.group(1)), token)
    return (10**9, token)


def _normalize_street_text(value: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9]+", value.upper())
    if not tokens:
        return ""
    canonical_map = {
        "NORTH": "N",
        "SOUTH": "S",
        "EAST": "E",
        "WEST": "W",
        "NORTHEAST": "NE",
        "NORTHWEST": "NW",
        "SOUTHEAST": "SE",
        "SOUTHWEST": "SW",
        "STREET": "ST",
        "AVENUE": "AVE",
        "BOULEVARD": "BLVD",
        "DRIVE": "DR",
        "COURT": "CT",
        "CRESCENT": "CRES",
        "PLACE": "PL",
        "TERRACE": "TER",
        "ROAD": "RD",
        "TRAIL": "TRL",
    }
    normalized = [canonical_map.get(token, token) for token in tokens]
    return " ".join(normalized)


def _street_matches(candidate_street: str, target_street: str) -> bool:
    normalized_candidate = _normalize_street_text(candidate_street)
    normalized_target = _normalize_street_text(target_street)
    if not normalized_candidate or not normalized_target:
        return False
    if normalized_candidate == normalized_target:
        return True
    if normalized_candidate.startswith(normalized_target):
        return True
    if normalized_target.startswith(normalized_candidate):
        return True
    candidate_tokens = set(normalized_candidate.split())
    target_tokens = set(normalized_target.split())
    return bool(candidate_tokens) and candidate_tokens == target_tokens


def _nominatim_candidate_street(candidate: dict[str, Any]) -> str:
    address = candidate.get("address", {})
    if not isinstance(address, dict):
        return ""
    for key in ("road", "pedestrian", "residential", "path", "street"):
        value = str(address.get(key, "")).strip()
        if value:
            return value
    return ""


def _nominatim_candidate_house_number(candidate: dict[str, Any]) -> str:
    address = candidate.get("address", {})
    if isinstance(address, dict):
        raw_house = str(address.get("house_number", "")).strip()
        if raw_house:
            return raw_house
    display_name = str(candidate.get("display_name", "")).strip()
    match = re.match(r"^\s*([0-9][0-9A-Za-z\-]*)\b", display_name)
    if match:
        return match.group(1)
    return ""


def _numeric_house_number(raw_value: str) -> int | None:
    match = re.match(r"^\s*(\d+)\s*$", raw_value)
    if not match:
        return None
    return int(match.group(1))


def _canonical_street_type_token(raw_type: str) -> str:
    normalized = raw_type.strip().upper()
    if not normalized:
        return ""
    reverse = {
        "ST": "STREET",
        "AVE": "AVENUE",
        "BLVD": "BOULEVARD",
        "DR": "DRIVE",
        "CT": "COURT",
        "CRES": "CRESCENT",
        "PL": "PLACE",
        "TER": "TERRACE",
        "RD": "ROAD",
        "TRL": "TRAIL",
        "CL": "CLOSE",
        "PT": "POINT",
        "LN": "LANE",
        "LI": "LINK",
        "VW": "VIEW",
        "WK": "WALK",
        "GA": "GATE",
    }
    if normalized in reverse:
        return reverse[normalized]
    return normalized


def _calgary_street_type_code(raw_type: str) -> str:
    canonical = _canonical_street_type_token(raw_type)
    mapping = {
        "STREET": "ST",
        "AVENUE": "AV",
        "BOULEVARD": "BV",
        "DRIVE": "DR",
        "COURT": "CO",
        "CRESCENT": "CR",
        "PLACE": "PL",
        "TERRACE": "TC",
        "ROAD": "RD",
        "TRAIL": "TR",
        "CLOSE": "CL",
        "POINT": "PT",
        "LANE": "LI",
        "LINK": "LI",
        "VIEW": "VW",
        "WALK": "WK",
        "GATE": "GA",
    }
    return mapping.get(canonical, canonical[:2] if canonical else "")


def _parse_street_components(street_name: str) -> tuple[str, str, str] | None:
    tokens = [token for token in re.findall(r"[A-Za-z0-9]+", street_name.upper()) if token]
    if len(tokens) < 2:
        return None
    quad = ""
    if tokens[-1] in {"NW", "NE", "SW", "SE"}:
        quad = tokens.pop()
    if not tokens:
        return None
    street_type_token = tokens.pop()
    if not tokens:
        return None
    street_base = " ".join(tokens).strip()
    street_type_code = _calgary_street_type_code(street_type_token)
    if not street_base or not street_type_code:
        return None
    return (street_base, street_type_code, quad)


def _calgary_query_context_enabled(
    query: str, context_tail: str, countrycodes: str, setting: str
) -> bool:
    if setting == "off":
        return False
    if setting == "on":
        return True
    country_tokens = {
        token.strip().lower() for token in countrycodes.split(",") if token.strip()
    }
    if country_tokens and "ca" not in country_tokens:
        return False
    text = f"{query} {context_tail}".lower()
    return "calgary" in text


def _calgary_parcel_rows_for_street(
    street_name: str,
    user_agent: str,
    timeout_seconds: int = 25,
) -> list[dict[str, Any]]:
    parsed = _parse_street_components(street_name)
    if parsed is None:
        return []
    street_base, street_type_code, quad = parsed
    where_parts = [
        f"upper(street_name) = '{street_base}'",
        f"upper(street_type) = '{street_type_code}'",
    ]
    if quad:
        where_parts.append(f"upper(street_quad) = '{quad}'")
    where_clause = " AND ".join(where_parts)

    params: dict[str, str | int] = {
        "$select": "address,house_number,street_name,street_type,street_quad,longitude,latitude",
        "$where": where_clause,
        "$limit": 50000,
    }
    payload = _http_get_json(
        CALGARY_PARCEL_ADDRESS_URL,
        params=params,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
    )
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _calgary_rows_from_parcel(
    street_name: str,
    context_tail: str,
    user_agent: str,
    existing_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    rows = _calgary_parcel_rows_for_street(street_name=street_name, user_agent=user_agent)
    if not rows:
        return []

    existing_numbers = {
        number
        for row in existing_rows
        if (number := _numeric_house_number(row.get("house_number", ""))) is not None
    }

    output: list[dict[str, str]] = []
    seen_numbers: set[int] = set()
    for row in rows:
        raw_house = str(row.get("house_number", "")).strip()
        numeric_house = _numeric_house_number(raw_house)
        if numeric_house is None:
            continue
        if numeric_house in existing_numbers or numeric_house in seen_numbers:
            continue
        lat = str(row.get("latitude", "")).strip()
        lon = str(row.get("longitude", "")).strip()
        if not lat or not lon:
            continue
        display_name = f"{numeric_house} {street_name}"
        if context_tail:
            display_name = f"{display_name}, {context_tail}"
        else:
            display_name = f"{display_name}, Calgary, Alberta, Canada"
        output.append(
            {
                "display_name": display_name,
                "lat": lat,
                "lon": lon,
                "house_number": str(numeric_house),
                "osm_type": "",
                "osm_id": "",
                "source": "city-parcel",
            }
        )
        seen_numbers.add(numeric_house)

    output.sort(key=lambda row: _sort_house_number(row["house_number"]))
    return output


def _street_candidates_from_overpass(
    street_name: str,
    bbox: tuple[float, float, float, float],
    context_tail: str,
    user_agent: str,
    timeout_seconds: int = 25,
) -> list[dict[str, str]]:
    payload = _http_post_json(
        OVERPASS_API_URL,
        data=urlencode({"data": _overpass_query_for_street(street_name, bbox)}),
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
    )
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected Overpass response format.")
    elements = payload.get("elements", [])
    if not isinstance(elements, list):
        raise RuntimeError("Unexpected Overpass response format.")

    results: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for element in elements:
        if not isinstance(element, dict):
            continue
        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            continue
        house_number = str(tags.get("addr:housenumber", "")).strip()
        if not house_number:
            continue
        tagged_street = str(tags.get("addr:street", "")).strip()
        if not tagged_street:
            tagged_street = str(tags.get("addr:place", "")).strip()
        if not tagged_street:
            tagged_street = str(tags.get("name", "")).strip()
        if tagged_street and not _street_matches(tagged_street, street_name):
            continue
        if not tagged_street:
            continue
        resolved_street = tagged_street
        lat: float | None = None
        lon: float | None = None
        if "lat" in element and "lon" in element:
            try:
                lat = float(element["lat"])
                lon = float(element["lon"])
            except (ValueError, TypeError):
                lat = None
                lon = None
        if lat is None or lon is None:
            center = element.get("center", {})
            if isinstance(center, dict):
                try:
                    lat = float(center["lat"])
                    lon = float(center["lon"])
                except (KeyError, ValueError, TypeError):
                    lat = None
                    lon = None
        if lat is None or lon is None:
            continue

        display_name = f"{house_number} {resolved_street}"
        if context_tail:
            display_name = f"{display_name}, {context_tail}"
        key = (house_number.upper(), f"{lat:.7f}", f"{lon:.7f}")
        if key in seen:
            continue
        seen.add(key)
        results.append(
            {
                "display_name": display_name,
                "lat": f"{lat:.7f}",
                "lon": f"{lon:.7f}",
                "house_number": house_number,
                "osm_type": str(element.get("type", "")).strip(),
                "osm_id": str(element.get("id", "")).strip(),
                "source": "overpass",
            }
        )

    results.sort(key=lambda row: _sort_house_number(row["house_number"]))
    return results


def _nominatim_candidates_to_rows(candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for candidate in candidates:
        display_name = str(candidate.get("display_name", "")).strip()
        lat = str(candidate.get("lat", "")).strip()
        lon = str(candidate.get("lon", "")).strip()
        if not display_name:
            continue
        key = (display_name, lat, lon)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "display_name": display_name,
                "lat": lat,
                "lon": lon,
                "osm_type": str(candidate.get("osm_type", "")).strip(),
                "osm_id": str(candidate.get("osm_id", "")).strip(),
                "source": "nominatim",
            }
        )
    return rows


def _probe_missing_number_rows(
    street_name: str,
    context_tail: str,
    existing_rows: list[dict[str, str]],
    countrycodes: str,
    language: str,
    email: str,
    user_agent: str,
) -> list[dict[str, str]]:
    existing_numeric = sorted(
        {
            number
            for row in existing_rows
            if (number := _numeric_house_number(row.get("house_number", ""))) is not None
        }
    )
    if len(existing_numeric) < 2:
        return []
    if not all(number % 1000 == 0 for number in existing_numeric):
        return []

    lower = existing_numeric[0]
    upper = existing_numeric[-1]
    if upper <= lower:
        return []

    candidates_to_probe = [
        number
        for number in range(lower, upper + 1000, 1000)
        if number not in set(existing_numeric)
    ]
    if not candidates_to_probe:
        return []

    probed_rows: list[dict[str, str]] = []
    for number in candidates_to_probe:
        query = f"{number} {street_name}"
        if context_tail:
            query = f"{query}, {context_tail}"
        matches = _nominatim_search(
            query=query,
            limit=5,
            countrycodes=countrycodes,
            language=language,
            email=email,
            user_agent=user_agent,
        )
        for match in matches:
            house_number = _nominatim_candidate_house_number(match)
            numeric_house = _numeric_house_number(house_number)
            if numeric_house != number:
                continue
            candidate_street = _nominatim_candidate_street(match)
            if not candidate_street or not _street_matches(candidate_street, street_name):
                continue
            converted = _nominatim_candidates_to_rows([match])
            if not converted:
                continue
            row = converted[0]
            row["house_number"] = house_number
            row["source"] = "nominatim-probe"
            probed_rows.append(row)
            break
    return probed_rows


def _merge_rows(primary: list[dict[str, str]], secondary: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for row in [*primary, *secondary]:
        display_name = row.get("display_name", "").strip()
        lat = row.get("lat", "").strip()
        lon = row.get("lon", "").strip()
        key = (display_name.lower(), lat, lon)
        if not display_name or key in seen_keys:
            continue
        seen_keys.add(key)
        merged.append(row)
    return merged


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Single-keyless OpenStreetMap lookup tool: suggest likely addresses, "
            "geocode exact addresses, and expand street-only queries into house numbers."
        )
    )
    parser.add_argument("query", help="Address or partial address query.")
    parser.add_argument(
        "--mode",
        choices=["auto", "suggest", "geocode"],
        default="auto",
        help="Lookup mode (default: auto).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows to print (default: 10).",
    )
    parser.add_argument(
        "--result-index",
        type=int,
        default=0,
        help="Result index for geocode mode (default: 0).",
    )
    parser.add_argument(
        "--countrycodes",
        default="",
        help="Optional ISO-3166-1 alpha-2 country codes (comma separated, e.g. ca,us).",
    )
    parser.add_argument(
        "--language",
        default="en",
        help="Preferred response language (default: en).",
    )
    parser.add_argument(
        "--email",
        default="",
        help="Optional contact email for Nominatim usage-policy compliance.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help=f"HTTP User-Agent header (default: {DEFAULT_USER_AGENT}).",
    )
    parser.add_argument(
        "--street-expansion",
        choices=["auto", "on", "off"],
        default="auto",
        help="When suggesting, expand street-only queries into house-number addresses.",
    )
    parser.add_argument(
        "--expand-limit",
        type=int,
        default=500,
        help="Max rows to fetch from Overpass before output limiting (default: 500).",
    )
    parser.add_argument(
        "--calgary-assessment-probe",
        choices=["auto", "on", "off"],
        default="auto",
        help="For Calgary queries, add missing civic numbers from City Parcel Address data.",
    )
    parser.add_argument(
        "--latlng-only",
        action="store_true",
        help="In geocode mode, print only 'lat,lon'.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit must be >= 1.")
    if args.limit > 1000:
        parser.error("--limit must be <= 1000.")
    if args.expand_limit < 1:
        parser.error("--expand-limit must be >= 1.")
    if args.expand_limit > 5000:
        parser.error("--expand-limit must be <= 5000.")
    if args.result_index < 0:
        parser.error("--result-index must be >= 0.")

    mode = args.mode
    if mode == "auto":
        mode = "geocode" if _has_house_number(args.query) else "suggest"

    search_limit = max(args.limit, args.result_index + 1, 20)
    nominatim_results = _nominatim_search(
        query=args.query,
        limit=search_limit,
        countrycodes=args.countrycodes,
        language=args.language,
        email=args.email,
        user_agent=args.user_agent,
    )

    if mode == "geocode":
        rows = _nominatim_candidates_to_rows(nominatim_results)
        if not rows:
            if args.json:
                print(json.dumps({"mode": mode, "query": args.query, "result": None}, indent=2))
            else:
                print("No geocoding results found.")
            return 0
        if args.result_index >= len(rows):
            raise RuntimeError(
                f"Requested --result-index {args.result_index}, but only {len(rows)} result(s) returned."
            )
        selected = rows[args.result_index]
        if args.json:
            print(
                json.dumps(
                    {
                        "mode": mode,
                        "query": args.query,
                        "result_index": args.result_index,
                        "result": selected,
                    },
                    indent=2,
                )
            )
            return 0
        if args.latlng_only:
            print(f"{selected['lat']},{selected['lon']}")
            return 0
        print(f"Display name: {selected['display_name']}")
        print(f"Latitude: {selected['lat']}")
        print(f"Longitude: {selected['lon']}")
        print(f"LatLng: {selected['lat']},{selected['lon']}")
        return 0

    rows = _nominatim_candidates_to_rows(nominatim_results)
    should_expand = args.street_expansion == "on" or (
        args.street_expansion == "auto" and not _has_house_number(args.query)
    )
    if should_expand and nominatim_results:
        street_candidate = next((item for item in nominatim_results if _looks_like_street(item)), None)
        if street_candidate is not None:
            street_name = _street_name(street_candidate)
            bbox = _parse_bbox(street_candidate)
            context_tail = str(street_candidate.get("display_name", "")).strip()
            if "," in context_tail:
                context_tail = context_tail.split(",", maxsplit=1)[1].strip()
            else:
                context_tail = ""
            if street_name and bbox is not None:
                expanded_rows: list[dict[str, str]] = []
                for bbox_candidate in _expanded_bboxes_for_street(street_candidate, bbox):
                    overpass_rows = _street_candidates_from_overpass(
                        street_name=street_name,
                        bbox=bbox_candidate,
                        context_tail=context_tail,
                        user_agent=args.user_agent,
                    )
                    if overpass_rows:
                        expanded_rows = _merge_rows(expanded_rows, overpass_rows)
                    if len(expanded_rows) >= args.expand_limit:
                        break
                if expanded_rows:
                    expanded_rows = expanded_rows[: args.expand_limit]
                    probed_rows = _probe_missing_number_rows(
                        street_name=street_name,
                        context_tail=context_tail,
                        existing_rows=expanded_rows,
                        countrycodes=args.countrycodes,
                        language=args.language,
                        email=args.email,
                        user_agent=args.user_agent,
                    )
                    merged_expansion = _merge_rows(expanded_rows, probed_rows)
                else:
                    merged_expansion = []

                if _calgary_query_context_enabled(
                    query=args.query,
                    context_tail=context_tail,
                    countrycodes=args.countrycodes,
                    setting=args.calgary_assessment_probe,
                ):
                    calgary_rows = _calgary_rows_from_parcel(
                        street_name=street_name,
                        context_tail=context_tail,
                        user_agent=args.user_agent,
                        existing_rows=merged_expansion,
                    )
                    merged_expansion = _merge_rows(merged_expansion, calgary_rows)

                if merged_expansion:
                    rows = _merge_rows(merged_expansion, rows)

    output_rows = rows[: args.limit]
    if args.json:
        print(
            json.dumps(
                {
                    "mode": mode,
                    "query": args.query,
                    "count": len(output_rows),
                    "results": output_rows,
                },
                indent=2,
            )
        )
        return 0

    if not output_rows:
        print("No matching addresses found.")
        return 0
    for index, row in enumerate(output_rows, start=1):
        display_name = row["display_name"]
        lat = row["lat"]
        lon = row["lon"]
        osm_type = row["osm_type"]
        osm_id = row["osm_id"]
        source = row.get("source", "")
        suffix = f" | lat={lat}, lon={lon}"
        if osm_type and osm_id:
            suffix += f" | {osm_type}:{osm_id}"
        if source:
            suffix += f" | source={source}"
        print(f"{index}. {display_name}{suffix}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
