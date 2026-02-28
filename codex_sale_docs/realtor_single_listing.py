import argparse
import csv
import datetime as dt
import html
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_TIMEOUT_SECONDS = 25
DEFAULT_MAX_RETRIES = 2
DEFAULT_ACCEPT_LANGUAGE = "en-CA,en;q=0.9"
DEFAULT_SEARCH_RADIUS_KM = 1.2
DEFAULT_MAX_MATCHES = 25
DEFAULT_USER_AGENT = (
    "RealtorCA-Internal-QA-SingleExtract/1.1 "
    "(authorized usability/accuracy audit; contact=qa-team@example.com)"
)

PROPERTY_SEARCH_ENDPOINTS = [
    "https://api2.realtor.ca/Listing.svc/PropertySearch_Post",
    "https://api37.realtor.ca/Listing.svc/PropertySearch_Post",
]
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"

OUTPUT_FIELDNAMES = [
    "input_mode",
    "input_address",
    "url",
    "matched_listing_id",
    "matched_mls_number",
    "fetched_at_utc",
    "http_status",
    "extract_source",
    "address_realtor",
    "sqft",
    "bathrooms",
    "bedrooms",
    "parking_spots",
    "storage_units",
    "maintenance_fee",
    "recurring_fees",
    "price_per_sqft",
    "error",
]

MATCH_FIELDNAMES = [
    "rank",
    "match_score",
    "listing_id",
    "mls_number",
    "address_realtor",
    "url",
    "price",
]

JSON_LD_SCRIPT_RE = re.compile(
    r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
NEXT_DATA_RE = re.compile(
    r"<script[^>]*id=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)

PRICE_PER_SQFT_TEXT_RE = re.compile(
    r"\$\s*([0-9]+(?:\.[0-9]+)?)\s*/\s*(?:sq\s*\.?\s*ft|sqft|square\s*foot)",
    re.IGNORECASE,
)

ADDRESS_STOPWORDS = {
    "ab",
    "alberta",
    "bc",
    "british",
    "columbia",
    "calgary",
    "canada",
    "court",
    "ct",
    "drive",
    "dr",
    "street",
    "st",
    "road",
    "rd",
    "avenue",
    "ave",
    "unit",
    "suite",
}

MAINTENANCE_KEYWORDS = {
    "maintenance_fee",
    "maintenancefee",
    "maintenancefees",
    "condofee",
    "condofees",
    "stratafee",
    "stratafees",
    "monthlymaintenance",
    "associationfee",
    "hoafee",
    "commonexpense",
}

PRICE_PER_SQFT_KEYWORDS = {
    "pricepersquarefoot",
    "pricepersquarefeet",
    "price_per_square_foot",
    "price_per_sqft",
    "pricepersqft",
    "dollarspersquarefoot",
}

CITY_CENTER_FALLBACKS: dict[str, tuple[float, float]] = {
    "calgary": (51.0447, -114.0719),
    "edmonton": (53.5461, -113.4938),
    "vancouver": (49.2827, -123.1207),
    "toronto": (43.6532, -79.3832),
    "ottawa": (45.4215, -75.6972),
    "montreal": (45.5019, -73.5674),
    "winnipeg": (49.8951, -97.1384),
    "halifax": (44.6488, -63.5752),
}


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _normalize_address_key(value: str) -> str:
    lowered = _normalize_space(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered)
    return _normalize_space(cleaned)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = _normalize_space(str(value))
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", raw)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    if value.is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _extract_number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    raw = _normalize_space(str(value))
    if not raw:
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)", raw.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _best_numeric(current: float | None, candidate: float | None) -> float | None:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return candidate if candidate > current else current


def _http_request_text(
    url: str,
    *,
    method: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout_seconds: int,
    max_retries: int,
) -> tuple[str, int, str]:
    error_message = ""
    for attempt in range(max_retries + 1):
        request = Request(url, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace"), int(response.status), ""
        except HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            error_message = f"HTTP {exc.code}: {body_text[:600]}"
            if exc.code in {429, 500, 502, 503, 504} and attempt < max_retries:
                time.sleep((attempt + 1) * 2)
                continue
            return "", exc.code, error_message
        except URLError as exc:
            error_message = f"URL error: {exc.reason}"
            if attempt < max_retries:
                time.sleep((attempt + 1) * 2)
                continue
            return "", 0, error_message
    return "", 0, error_message


def _http_get_text(
    url: str,
    *,
    timeout_seconds: int,
    max_retries: int,
    user_agent: str,
    accept_language: str,
) -> tuple[str, int, str]:
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": accept_language,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    return _http_request_text(
        url,
        method="GET",
        headers=headers,
        body=None,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )


def _http_get_json(
    url: str,
    *,
    params: dict[str, Any],
    timeout_seconds: int,
    max_retries: int,
    user_agent: str,
    accept_language: str,
) -> tuple[Any, int, str]:
    full_url = f"{url}?{urlencode(params, doseq=True)}" if params else url
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": accept_language,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    text, status, error = _http_request_text(
        full_url,
        method="GET",
        headers=headers,
        body=None,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    if error:
        return None, status, error
    try:
        return json.loads(text), status, ""
    except json.JSONDecodeError:
        return None, status, f"Invalid JSON from {full_url}"


def _http_post_form_json(
    url: str,
    *,
    form_data: dict[str, Any],
    timeout_seconds: int,
    max_retries: int,
    user_agent: str,
    accept_language: str,
) -> tuple[Any, int, str]:
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": accept_language,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.realtor.ca",
        "Referer": "https://www.realtor.ca/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    body = urlencode(form_data, doseq=True).encode("utf-8")
    text, status, error = _http_request_text(
        url,
        method="POST",
        headers=headers,
        body=body,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    if error:
        return None, status, error
    try:
        return json.loads(text), status, ""
    except json.JSONDecodeError:
        return None, status, f"Invalid JSON from {url}"


def _iter_json_leaves(value: Any, path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = path + (str(key),)
            yield from _iter_json_leaves(child, child_path)
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            child_path = path + (str(index),)
            yield from _iter_json_leaves(child, child_path)
        return
    yield path, value


def _compose_address(value: Any) -> str:
    if isinstance(value, str):
        return _normalize_space(value)
    if not isinstance(value, dict):
        return ""
    parts = [
        value.get("streetAddress"),
        value.get("addressLocality"),
        value.get("addressRegion"),
        value.get("postalCode"),
    ]
    return _normalize_space(", ".join(_normalize_space(str(part)) for part in parts if part))


def _extract_price_per_sqft_from_text(value: str) -> float | None:
    match = PRICE_PER_SQFT_TEXT_RE.search(value)
    if not match:
        return None
    return _extract_number(match.group(1))


def _is_maintenance_key(key: str, path_key: str) -> bool:
    compact = key.replace("_", "")
    path_compact = path_key.replace("_", "")
    if compact in MAINTENANCE_KEYWORDS:
        return True
    return any(keyword in path_compact for keyword in MAINTENANCE_KEYWORDS)


def _is_price_per_sqft_key(key: str, path_key: str) -> bool:
    compact = key.replace("_", "")
    path_compact = path_key.replace("_", "")
    if compact in PRICE_PER_SQFT_KEYWORDS:
        return True
    if any(keyword in path_compact for keyword in PRICE_PER_SQFT_KEYWORDS):
        return True
    return "price" in compact and ("sqft" in compact or "squarefoot" in compact)


def _extract_candidate(blob: Any, source: str) -> dict[str, Any]:
    candidate = {
        "extract_source": source,
        "address_realtor": "",
        "price": None,
        "sqft": None,
        "bathrooms": None,
        "bedrooms": None,
        "parking_spots": None,
        "storage_units": None,
        "maintenance_fee": None,
        "recurring_fees": None,
        "price_per_sqft": None,
        "score": 0,
    }

    if isinstance(blob, dict):
        direct_address = _compose_address(blob.get("address"))
        if direct_address:
            candidate["address_realtor"] = direct_address

    for path, leaf_value in _iter_json_leaves(blob):
        if not path:
            continue
        key = _normalize_header(path[-1])
        path_key = "_".join(_normalize_header(part) for part in path)
        value_text = _normalize_space(str(leaf_value))
        lower_text = value_text.lower()

        if key in {"address", "fulladdress", "propertyaddress", "streetaddress"} and not candidate["address_realtor"]:
            maybe_address = _compose_address(leaf_value)
            if maybe_address and re.search(r"\d{2,6}", maybe_address):
                candidate["address_realtor"] = maybe_address
        if not candidate["address_realtor"] and re.search(r"\d{2,6}", value_text):
            if any(token in lower_text for token in (" st", " ave", " road", " rd", " drive", " dr", " court", " ct")):
                candidate["address_realtor"] = value_text

        if key in {"price", "listprice", "listingprice", "askingprice", "leaseprice"}:
            candidate["price"] = _best_numeric(candidate["price"], _extract_number(leaf_value))

        if key in {"sizeinterior", "sqft", "squarefeet", "livingarea", "interiorsize", "buildingarea"}:
            candidate["sqft"] = _best_numeric(candidate["sqft"], _extract_number(leaf_value))
        elif "sqft" in key or "squarefoot" in key:
            candidate["sqft"] = _best_numeric(candidate["sqft"], _extract_number(leaf_value))
        elif "area" in key and "lot" not in key and "land" not in key and "interior" in path_key:
            candidate["sqft"] = _best_numeric(candidate["sqft"], _extract_number(leaf_value))

        if key in {"bedrooms", "bedrooms_total", "bedroomstotal", "beds", "bedroom"} or (
            "bedroom" in key and "above" not in key and "below" not in key
        ):
            candidate["bedrooms"] = _best_numeric(candidate["bedrooms"], _extract_number(leaf_value))

        if key in {"bathrooms", "bathrooms_total", "bathroomstotal", "baths", "bathroom"} or "bath" in key:
            candidate["bathrooms"] = _best_numeric(candidate["bathrooms"], _extract_number(leaf_value))

        if "parking" in key or key in {"garage", "garage_spaces", "parkingspaces"}:
            candidate["parking_spots"] = _best_numeric(candidate["parking_spots"], _extract_number(leaf_value))
        elif "garage" in lower_text and candidate["parking_spots"] is None:
            candidate["parking_spots"] = _extract_number(leaf_value)

        if any(token in key for token in ("storage", "locker", "lockers")):
            storage_value = _extract_number(leaf_value)
            if storage_value is None and isinstance(leaf_value, str):
                if any(token in lower_text for token in ("locker", "storage")):
                    storage_value = 1.0
            candidate["storage_units"] = _best_numeric(candidate["storage_units"], storage_value)
        elif any(token in lower_text for token in ("locker included", "includes locker", "storage locker")):
            candidate["storage_units"] = _best_numeric(candidate["storage_units"], 1.0)

        if _is_maintenance_key(key, path_key):
            fee_value = _extract_number(leaf_value)
            candidate["maintenance_fee"] = _best_numeric(candidate["maintenance_fee"], fee_value)

        if "fee" in key or "tax" in key or "monthly" in key:
            recurring_value = _extract_number(leaf_value)
            if recurring_value is not None:
                candidate["recurring_fees"] = _best_numeric(candidate["recurring_fees"], recurring_value)

        if _is_price_per_sqft_key(key, path_key):
            candidate["price_per_sqft"] = _best_numeric(candidate["price_per_sqft"], _extract_number(leaf_value))
        elif isinstance(leaf_value, str):
            textual = _extract_price_per_sqft_from_text(value_text)
            candidate["price_per_sqft"] = _best_numeric(candidate["price_per_sqft"], textual)

    if candidate["recurring_fees"] is None and candidate["maintenance_fee"] is not None:
        candidate["recurring_fees"] = candidate["maintenance_fee"]

    if candidate["price_per_sqft"] is None and candidate["price"] is not None and candidate["sqft"]:
        if candidate["sqft"] > 0:
            candidate["price_per_sqft"] = candidate["price"] / candidate["sqft"]

    score = 0
    if candidate["address_realtor"]:
        score += 3
    if candidate["sqft"] is not None:
        score += 2
    if candidate["bedrooms"] is not None:
        score += 2
    if candidate["bathrooms"] is not None:
        score += 2
    if candidate["parking_spots"] is not None:
        score += 2
    if candidate["storage_units"] is not None:
        score += 2
    if candidate["maintenance_fee"] is not None:
        score += 1
    if candidate["price_per_sqft"] is not None:
        score += 1
    candidate["score"] = score
    return candidate


def _extract_from_html(html_text: str) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []

    for blob_text in JSON_LD_SCRIPT_RE.findall(html_text):
        unescaped = html.unescape(blob_text).strip()
        if not unescaped:
            continue
        try:
            parsed = json.loads(unescaped)
        except json.JSONDecodeError:
            continue
        candidates.append(_extract_candidate(parsed, source="jsonld"))

    for blob_text in NEXT_DATA_RE.findall(html_text):
        unescaped = html.unescape(blob_text).strip()
        if not unescaped:
            continue
        try:
            parsed = json.loads(unescaped)
        except json.JSONDecodeError:
            continue
        candidates.append(_extract_candidate(parsed, source="next_data"))

    if not candidates:
        return {
            "extract_source": "",
            "address_realtor": "",
            "price": None,
            "sqft": None,
            "bathrooms": None,
            "bedrooms": None,
            "parking_spots": None,
            "storage_units": None,
            "maintenance_fee": None,
            "recurring_fees": None,
            "price_per_sqft": None,
            "score": 0,
        }
    return max(candidates, key=lambda item: int(item.get("score", 0)))


def _address_tokens(value: str) -> list[str]:
    tokens = [token for token in re.findall(r"[a-z0-9]+", _normalize_address_key(value)) if token]
    return [token for token in tokens if token not in ADDRESS_STOPWORDS]


def _score_address_match(query_address: str, candidate_address: str) -> int:
    if not candidate_address:
        return 0
    normalized_query = _normalize_address_key(query_address)
    normalized_candidate = _normalize_address_key(candidate_address)
    query_tokens = _address_tokens(query_address)
    candidate_tokens = _address_tokens(candidate_address)

    score = 0
    if normalized_query == normalized_candidate:
        score += 200
    if normalized_query and normalized_query in normalized_candidate:
        score += 80

    query_set = set(query_tokens)
    candidate_set = set(candidate_tokens)
    if query_set and candidate_set:
        overlap = len(query_set & candidate_set)
        union = len(query_set | candidate_set)
        score += overlap * 12
        if union:
            score += int((overlap / union) * 40)

    number_query = {token for token in query_tokens if token.isdigit()}
    number_candidate = {token for token in candidate_tokens if token.isdigit()}
    if number_query:
        if number_query <= number_candidate:
            score += 40
        elif number_query & number_candidate:
            score += 8
        else:
            score -= 25
    return score


def _geocode_address(
    address: str,
    *,
    timeout_seconds: int,
    max_retries: int,
    user_agent: str,
    accept_language: str,
    geocode_email: str,
) -> tuple[float | None, float | None, str]:
    query = address
    if "canada" not in address.lower():
        query = f"{address}, Canada"

    params: dict[str, Any] = {"format": "jsonv2", "limit": 1, "q": query}
    if geocode_email:
        params["email"] = geocode_email

    payload, _status, error = _http_get_json(
        NOMINATIM_SEARCH_URL,
        params=params,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        user_agent=user_agent,
        accept_language=accept_language,
    )
    if error:
        return None, None, f"Geocoding failed: {error}"
    if not isinstance(payload, list) or not payload:
        return None, None, f"No geocoding result for address: {address}"
    row = payload[0]
    lat = _to_float(row.get("lat"))
    lon = _to_float(row.get("lon"))
    if lat is None or lon is None:
        return None, None, f"Geocoding result missing lat/lon for address: {address}"
    return lat, lon, ""


def _city_center_fallback(address: str) -> tuple[float, float, str] | None:
    normalized = _normalize_address_key(address)
    for city_name, (lat, lon) in CITY_CENTER_FALLBACKS.items():
        if city_name in normalized:
            return lat, lon, city_name
    return None


def _bbox_from_center(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    lat_delta = radius_km / 111.0
    cos_lat = math.cos(math.radians(lat))
    if abs(cos_lat) < 1e-6:
        lon_delta = radius_km / 111.0
    else:
        lon_delta = radius_km / (111.0 * cos_lat)
    lat_min = lat - lat_delta
    lat_max = lat + lat_delta
    lon_min = lon - lon_delta
    lon_max = lon + lon_delta
    return lat_min, lat_max, lon_min, lon_max


def _extract_matches_from_search_payload(payload: Any, query_address: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("Results")
    if not isinstance(rows, list):
        return []

    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        listing_id = str(row.get("Id", "")).strip()
        mls_number = str(row.get("MlsNumber", "")).strip()

        property_obj = row.get("Property") if isinstance(row.get("Property"), dict) else {}
        address_obj = property_obj.get("Address") if isinstance(property_obj.get("Address"), dict) else {}
        address_realtor = _normalize_space(str(address_obj.get("AddressText", "")))

        relative_details_url = str(row.get("RelativeDetailsURL", "")).strip()
        if relative_details_url.startswith("/"):
            url = f"https://www.realtor.ca{relative_details_url}"
        elif relative_details_url:
            url = relative_details_url
        elif listing_id:
            url = f"https://www.realtor.ca/real-estate/{listing_id}"
        else:
            url = ""

        price_value = _extract_number(property_obj.get("Price"))
        score = _score_address_match(query_address, address_realtor)

        matches.append(
            {
                "rank": 0,
                "match_score": score,
                "listing_id": listing_id,
                "mls_number": mls_number,
                "address_realtor": address_realtor,
                "url": url,
                "price": _format_float(price_value),
            }
        )
    return matches


def _search_listings_by_address(
    address: str,
    *,
    radius_km: float,
    max_matches: int,
    timeout_seconds: int,
    max_retries: int,
    user_agent: str,
    accept_language: str,
    geocode_email: str,
    center_lat: float | None,
    center_lon: float | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    geocode_warning = ""
    geo_source = "nominatim"
    lat: float | None = center_lat
    lon: float | None = center_lon
    if lat is None or lon is None:
        lat, lon, geo_error = _geocode_address(
            address,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            user_agent=user_agent,
            accept_language=accept_language,
            geocode_email=geocode_email,
        )
        if geo_error:
            fallback = _city_center_fallback(address)
            if fallback:
                lat, lon, fallback_city = fallback
                geocode_warning = (
                    f"Geocoding failed ({geo_error}); using city center fallback for '{fallback_city}'."
                )
                geo_source = f"fallback_city_center:{fallback_city}"
            else:
                return [], {"geocode_error": geo_error}, geo_error
    else:
        geo_source = "manual_center"

    if lat is None or lon is None:
        return [], {"geocode_error": "No usable center coordinates."}, "No usable center coordinates."

    base_radius = max(radius_km, 0.4)
    if geo_source.startswith("fallback_city_center"):
        base_radius = max(base_radius, 10.0)

    radii = [base_radius, max(base_radius * 2.0, 0.8), max(base_radius * 4.0, 1.6)]
    aggregated: list[dict[str, Any]] = []
    attempted_endpoints: list[dict[str, Any]] = []

    for active_radius in radii:
        lat_min, lat_max, lon_min, lon_max = _bbox_from_center(lat, lon, active_radius)
        form_data = {
            "CultureId": "1",
            "ApplicationId": "1",
            "PropertySearchTypeId": "1",
            "TransactionTypeId": "2",
            "PriceMin": "0",
            "PriceMax": "0",
            "BedRange": "0-0",
            "BathRange": "0-0",
            "ParkingSpaceRange": "0-0",
            "OwnershipTypeGroupId": "0",
            "RecordsPerPage": str(min(max(max_matches * 3, 60), 200)),
            "CurrentPage": "1",
            "Sort": "6-D",
            "Currency": "CAD",
            "PropertyTypeGroupID": "1",
            "LatitudeMin": f"{lat_min:.6f}",
            "LatitudeMax": f"{lat_max:.6f}",
            "LongitudeMin": f"{lon_min:.6f}",
            "LongitudeMax": f"{lon_max:.6f}",
            "ZoomLevel": "13",
            "Version": "7.0",
        }

        payload = None
        payload_error = ""
        payload_status = 0
        endpoint_used = ""
        for endpoint in PROPERTY_SEARCH_ENDPOINTS:
            payload, payload_status, payload_error = _http_post_form_json(
                endpoint,
                form_data=form_data,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                user_agent=user_agent,
                accept_language=accept_language,
            )
            attempted_endpoints.append(
                {
                    "endpoint": endpoint,
                    "status": payload_status,
                    "error": payload_error,
                    "radius_km": active_radius,
                }
            )
            if payload_error:
                continue
            endpoint_used = endpoint
            break

        if payload_error:
            continue

        matches = _extract_matches_from_search_payload(payload, query_address=address)
        if matches:
            for row in matches:
                row["search_radius_km"] = _format_float(active_radius)
                row["endpoint"] = endpoint_used
            aggregated.extend(matches)
            if len(aggregated) >= max_matches:
                break

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in sorted(aggregated, key=lambda item: int(item.get("match_score", 0)), reverse=True):
        dedupe_key = "|".join(
            [
                row.get("listing_id", ""),
                row.get("mls_number", ""),
                _normalize_address_key(row.get("address_realtor", "")),
                row.get("url", ""),
            ]
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(row)
        if len(deduped) >= max_matches:
            break

    for index, row in enumerate(deduped, start=1):
        row["rank"] = index

    meta = {
        "geocoded_lat": _format_float(lat),
        "geocoded_lon": _format_float(lon),
        "geo_source": geo_source,
        "geocode_warning": geocode_warning,
        "attempted_endpoints": attempted_endpoints,
    }
    if not deduped:
        return [], meta, "No candidate listings found near the provided address."
    return deduped, meta, ""


def _write_csv_row(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    if exists:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
        if header and header != OUTPUT_FIELDNAMES:
            raise RuntimeError(f"Header mismatch in {path}; expected {OUTPUT_FIELDNAMES} but got {header}")
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDNAMES)
        if not exists:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in OUTPUT_FIELDNAMES})


def _write_matches_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MATCH_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in MATCH_FIELDNAMES})


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Address-first REALTOR.ca extractor for authorized QA. "
            "Step 1: list candidate listings by address. "
            "Step 2: extract one listing by exact address or URL."
        )
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--url", default="", help="REALTOR.ca listing URL to inspect.")
    input_group.add_argument(
        "--address",
        default="",
        help="Address query. Use with --list-matches first, then run again with the exact REALTOR.ca address.",
    )
    parser.add_argument(
        "--list-matches",
        action="store_true",
        help="Address mode: list matching listings and exit without extracting listing fields.",
    )
    parser.add_argument("--max-matches", type=int, default=DEFAULT_MAX_MATCHES, help="Max address matches to return.")
    parser.add_argument(
        "--search-radius-km",
        type=float,
        default=DEFAULT_SEARCH_RADIUS_KM,
        help="Initial search radius around geocoded address (expands automatically).",
    )
    parser.add_argument(
        "--center-lat",
        type=float,
        default=None,
        help="Optional manual center latitude for address search (skips geocoding).",
    )
    parser.add_argument(
        "--center-lon",
        type=float,
        default=None,
        help="Optional manual center longitude for address search (skips geocoding).",
    )
    parser.add_argument(
        "--geocode-email",
        default="",
        help="Optional contact email sent to geocoder (helps avoid strict anonymous blocking).",
    )
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout.")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retry count.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="Audit user-agent string.")
    parser.add_argument("--accept-language", default=DEFAULT_ACCEPT_LANGUAGE, help="Accept-Language header.")
    parser.add_argument("--output-json", default="", help="Optional file path for JSON output.")
    parser.add_argument("--output-csv", default="", help="Optional file path for CSV output.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON to stdout.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    json_kwargs: dict[str, Any] = {}
    if args.pretty:
        json_kwargs = {"indent": 2, "sort_keys": True}

    selected_url = args.url
    matched_listing_id = ""
    matched_mls_number = ""
    input_mode = "url"

    if args.address:
        input_mode = "address"
        matches, search_meta, search_error = _search_listings_by_address(
            args.address,
            radius_km=args.search_radius_km,
            max_matches=max(args.max_matches, 1),
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
            user_agent=args.user_agent,
            accept_language=args.accept_language,
            geocode_email=args.geocode_email,
            center_lat=args.center_lat,
            center_lon=args.center_lon,
        )

        if args.list_matches:
            list_payload = {
                "input_mode": "address_search",
                "query_address": args.address,
                "match_count": len(matches),
                "matches": [
                    {key: row.get(key, "") for key in MATCH_FIELDNAMES}
                    for row in matches
                ],
                "search_meta": search_meta,
                "error": search_error,
            }
            print(json.dumps(list_payload, **json_kwargs))

            if args.output_json:
                output_json_path = Path(args.output_json)
                output_json_path.parent.mkdir(parents=True, exist_ok=True)
                with output_json_path.open("w", encoding="utf-8") as handle:
                    json.dump(list_payload, handle, indent=2, sort_keys=True)
                    handle.write("\n")

            if args.output_csv:
                _write_matches_csv(Path(args.output_csv), matches)

            if search_error:
                return 3
            return 0

        normalized_input = _normalize_address_key(args.address)
        exact_matches = [
            row for row in matches if _normalize_address_key(str(row.get("address_realtor", ""))) == normalized_input
        ]

        if len(exact_matches) != 1:
            failure_payload = {
                "input_mode": "address_extract",
                "query_address": args.address,
                "error": (
                    f"Expected exactly 1 exact address match, found {len(exact_matches)}. "
                    "Run with --list-matches and copy an exact address_realtor value."
                ),
                "match_count": len(matches),
                "matches": [
                    {key: row.get(key, "") for key in MATCH_FIELDNAMES}
                    for row in matches[: args.max_matches]
                ],
                "search_meta": search_meta,
            }
            print(json.dumps(failure_payload, **json_kwargs))
            if args.output_json:
                output_json_path = Path(args.output_json)
                output_json_path.parent.mkdir(parents=True, exist_ok=True)
                with output_json_path.open("w", encoding="utf-8") as handle:
                    json.dump(failure_payload, handle, indent=2, sort_keys=True)
                    handle.write("\n")
            return 3

        chosen = exact_matches[0]
        selected_url = str(chosen.get("url", "")).strip()
        matched_listing_id = str(chosen.get("listing_id", "")).strip()
        matched_mls_number = str(chosen.get("mls_number", "")).strip()
        if not selected_url:
            error_payload = {
                "input_mode": "address_extract",
                "query_address": args.address,
                "error": "Exact address matched but no listing URL was available.",
                "match": {key: chosen.get(key, "") for key in MATCH_FIELDNAMES},
            }
            print(json.dumps(error_payload, **json_kwargs))
            return 4

    fetched_at = dt.datetime.now(dt.UTC)
    html_text, http_status, error = _http_get_text(
        selected_url,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        user_agent=args.user_agent,
        accept_language=args.accept_language,
    )

    extracted = _extract_from_html(html_text) if html_text else _extract_from_html("")
    result = {
        "input_mode": input_mode,
        "input_address": args.address,
        "url": selected_url,
        "matched_listing_id": matched_listing_id,
        "matched_mls_number": matched_mls_number,
        "fetched_at_utc": fetched_at.isoformat(),
        "http_status": http_status,
        "extract_source": extracted.get("extract_source", ""),
        "address_realtor": _normalize_space(str(extracted.get("address_realtor", ""))),
        "sqft": _format_float(_to_float(extracted.get("sqft"))),
        "bathrooms": _format_float(_to_float(extracted.get("bathrooms"))),
        "bedrooms": _format_float(_to_float(extracted.get("bedrooms"))),
        "parking_spots": _format_float(_to_float(extracted.get("parking_spots"))),
        "storage_units": _format_float(_to_float(extracted.get("storage_units"))),
        "maintenance_fee": _format_float(_to_float(extracted.get("maintenance_fee"))),
        "recurring_fees": _format_float(_to_float(extracted.get("recurring_fees"))),
        "price_per_sqft": _format_float(_to_float(extracted.get("price_per_sqft"))),
        "error": _normalize_space(error),
    }

    print(json.dumps(result, **json_kwargs))

    if args.output_json:
        output_json_path = Path(args.output_json)
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        with output_json_path.open("w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, sort_keys=True)
            handle.write("\n")

    if args.output_csv:
        _write_csv_row(Path(args.output_csv), result)

    if result["error"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
