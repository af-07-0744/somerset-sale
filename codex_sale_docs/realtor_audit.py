import argparse
import csv
import datetime as dt
import html
import json
import re
import time
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_OUTPUT_DIR = Path("data/realtor_accuracy_audit")
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 25
DEFAULT_PRICE_TOLERANCE = 0.0
DEFAULT_NUMERIC_TOLERANCE = 0.0
DEFAULT_GEO_TOLERANCE = 0.001
DEFAULT_STALE_THRESHOLD_HOURS = 24.0

DEFAULT_USER_AGENT = (
    "RealtorCA-Internal-QA-Audit/1.0 "
    "(authorized usability/accuracy audit; contact=qa-team@example.com)"
)

SCRAPE_FIELDNAMES = [
    "row_index",
    "url",
    "complaint_flag",
    "expected_listing_id",
    "extracted_listing_id",
    "listing_id_used",
    "http_status",
    "fetched_at_utc",
    "extract_source",
    "price",
    "beds",
    "baths",
    "address",
    "latitude",
    "longitude",
    "status",
    "last_updated",
    "error",
]

DIFF_FIELDNAMES = [
    "row_index",
    "url",
    "complaint_flag",
    "listing_id_used",
    "in_truth",
    "overall_mismatch",
    "mismatch_count",
    "stale_flag",
    "freshness_lag_hours",
    "price_match",
    "beds_match",
    "baths_match",
    "address_match",
    "status_match",
    "geo_match",
    "truth_price",
    "scrape_price",
    "truth_beds",
    "scrape_beds",
    "truth_baths",
    "scrape_baths",
    "truth_address",
    "scrape_address",
    "truth_status",
    "scrape_status",
    "truth_latitude",
    "scrape_latitude",
    "truth_longitude",
    "scrape_longitude",
    "truth_last_updated",
    "scrape_last_updated",
]

JSON_LD_SCRIPT_RE = re.compile(
    r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
NEXT_DATA_RE = re.compile(
    r"<script[^>]*id=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)

MLS_RE = re.compile(r"\bMLS(?:\s*#|®)?\s*([A-Z]?\d{5,})\b", re.IGNORECASE)
URL_LISTING_ID_RE = re.compile(r"/real-estate/(\d{5,})", re.IGNORECASE)
DATEISH_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

KEYS_ID = {"mls", "mlsnumber", "mls_number", "listingid", "listing_id", "listingnumber", "listing_number"}
KEYS_PRICE = {"price", "listprice", "listingprice", "askingprice", "offerprice"}
KEYS_BEDS = {"beds", "bedrooms", "bedroom", "bedroomstotal", "beds_total"}
KEYS_BATHS = {"baths", "bathrooms", "bathroom", "bathroomstotal", "baths_total"}
KEYS_STATUS = {"status", "listingstatus", "availability", "propertystatus"}
KEYS_LAT = {"latitude", "lat"}
KEYS_LON = {"longitude", "long", "lng", "lon"}
KEYS_UPDATED = {
    "lastupdated",
    "last_updated",
    "updatedat",
    "updated_at",
    "modificationdate",
    "modification_date",
    "dateupdated",
    "date_updated",
}


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", raw)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _to_boolish(value: Any) -> bool:
    raw = _normalize_space(str(value)).lower()
    if raw in {"1", "true", "yes", "y", "t", "complaint", "reported"}:
        return True
    if raw in {"0", "false", "no", "n", "f", ""}:
        return False
    return bool(raw)


def _normalize_listing_id(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip().upper()
    if not raw:
        return ""
    mls_match = MLS_RE.search(raw)
    if mls_match:
        return re.sub(r"[^A-Z0-9]", "", mls_match.group(1))
    compact = re.sub(r"[^A-Z0-9]", "", raw)
    if compact.startswith("MLS"):
        compact = compact[3:]
    if len(compact) < 5:
        return ""
    return compact


def _canonical_status(value: Any) -> str:
    text = _normalize_space(str(value)).lower()
    if not text:
        return ""
    if any(token in text for token in ("sold", "closed", "terminated")):
        return "sold"
    if any(token in text for token in ("pending", "conditional")):
        return "pending"
    if any(token in text for token in ("active", "for sale", "new listing", "listed")):
        return "active"
    return re.sub(r"[^a-z0-9]+", "", text)


def _parse_datetime(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.UTC) if value.tzinfo else value.replace(tzinfo=dt.UTC)
    raw = _normalize_space(str(value))
    if not raw:
        return None
    if raw.isdigit():
        try:
            epoch = int(raw)
        except ValueError:
            return None
        if epoch > 1_000_000_000_000:
            epoch = epoch // 1000
        if epoch > 0:
            return dt.datetime.fromtimestamp(epoch, tz=dt.UTC)
    iso_try = raw.replace("Z", "+00:00")
    try:
        parsed_iso = dt.datetime.fromisoformat(iso_try)
        return parsed_iso.astimezone(dt.UTC) if parsed_iso.tzinfo else parsed_iso.replace(tzinfo=dt.UTC)
    except ValueError:
        pass
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%b %d, %Y",
    ]
    for fmt in formats:
        try:
            parsed = dt.datetime.strptime(raw, fmt)
            return parsed.replace(tzinfo=dt.UTC)
        except ValueError:
            continue
    if DATEISH_RE.search(raw):
        token = DATEISH_RE.search(raw)
        if token:
            return _parse_datetime(token.group(0))
    return None


def _datetime_to_iso(value: dt.datetime | None) -> str:
    if not value:
        return ""
    as_utc = value.astimezone(dt.UTC) if value.tzinfo else value.replace(tzinfo=dt.UTC)
    return as_utc.isoformat()


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    if value.is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _pick_column(
    fieldnames: list[str],
    explicit: str,
    candidates: list[str],
    label: str,
    required: bool = False,
) -> str:
    if explicit:
        if explicit in fieldnames:
            return explicit
        lowered = {field.lower(): field for field in fieldnames}
        if explicit.lower() in lowered:
            return lowered[explicit.lower()]
        normalized = { _normalize_header(field): field for field in fieldnames }
        explicit_norm = _normalize_header(explicit)
        if explicit_norm in normalized:
            return normalized[explicit_norm]
        raise RuntimeError(f"Could not find explicit {label} column '{explicit}'.")

    normalized = {_normalize_header(field): field for field in fieldnames}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    lower_to_original = {field.lower(): field for field in fieldnames}
    for candidate in candidates:
        candidate_readable = candidate.replace("_", " ")
        for lowered, original in lower_to_original.items():
            if candidate in lowered or candidate_readable in lowered:
                return original
    if required:
        raise RuntimeError(f"Could not detect required {label} column. Fieldnames: {fieldnames}")
    return ""


def _load_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
        return reader.fieldnames or [], rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _http_get_text(
    url: str,
    *,
    timeout_seconds: int,
    user_agent: str,
    accept_language: str,
    max_retries: int,
) -> tuple[str, int, str]:
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": accept_language,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    error_message = ""
    for attempt in range(max_retries + 1):
        request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace"), int(response.status), ""
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            error_message = f"HTTP {exc.code}: {body[:500]}"
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


def _compose_address(address_obj: Any) -> str:
    if isinstance(address_obj, str):
        return _normalize_space(address_obj)
    if not isinstance(address_obj, dict):
        return ""
    pieces = [
        address_obj.get("streetAddress"),
        address_obj.get("addressLocality"),
        address_obj.get("addressRegion"),
        address_obj.get("postalCode"),
    ]
    rendered = ", ".join(_normalize_space(str(piece)) for piece in pieces if piece)
    return _normalize_space(rendered)


def _extract_candidate_from_json_blob(blob: Any, source: str) -> dict[str, Any]:
    candidate: dict[str, Any] = {
        "extract_source": source,
        "listing_id": "",
        "price": None,
        "beds": None,
        "baths": None,
        "address": "",
        "latitude": None,
        "longitude": None,
        "status": "",
        "last_updated": "",
        "score": 0,
    }
    parsed_updated: dt.datetime | None = None

    if isinstance(blob, dict):
        direct_address = _compose_address(blob.get("address"))
        if direct_address:
            candidate["address"] = direct_address
        geo = blob.get("geo")
        if isinstance(geo, dict):
            candidate["latitude"] = _to_float(geo.get("latitude"))
            candidate["longitude"] = _to_float(geo.get("longitude"))

    for path, value in _iter_json_leaves(blob):
        if not path:
            continue
        key = _normalize_header(path[-1])

        if key in KEYS_ID and not candidate["listing_id"]:
            maybe_id = _normalize_listing_id(value)
            if maybe_id:
                candidate["listing_id"] = maybe_id

        if key in KEYS_PRICE and candidate["price"] is None:
            candidate["price"] = _to_float(value)

        if key in KEYS_BEDS and candidate["beds"] is None:
            candidate["beds"] = _to_float(value)

        if key in KEYS_BATHS and candidate["baths"] is None:
            candidate["baths"] = _to_float(value)

        if key in KEYS_STATUS and not candidate["status"]:
            candidate["status"] = _canonical_status(value)

        if key in KEYS_LAT and candidate["latitude"] is None:
            candidate["latitude"] = _to_float(value)

        if key in KEYS_LON and candidate["longitude"] is None:
            candidate["longitude"] = _to_float(value)

        if key in KEYS_UPDATED or ("date" in key and not parsed_updated):
            parsed = _parse_datetime(value)
            if parsed:
                parsed_updated = parsed

        if key == "address" and not candidate["address"]:
            candidate["address"] = _compose_address(value)

        if isinstance(value, str):
            if not candidate["listing_id"]:
                id_match = MLS_RE.search(value)
                if id_match:
                    candidate["listing_id"] = _normalize_listing_id(id_match.group(1))
            if not candidate["address"]:
                maybe_address = _normalize_space(value)
                if re.search(r"\d{2,6}", maybe_address) and any(
                    token in maybe_address.lower() for token in ("st", "ave", "road", "rd", "dr", "court", "ct")
                ):
                    candidate["address"] = maybe_address

    if parsed_updated:
        candidate["last_updated"] = _datetime_to_iso(parsed_updated)

    score = 0
    if candidate["listing_id"]:
        score += 4
    if candidate["price"] is not None:
        score += 3
    if candidate["address"]:
        score += 2
    if candidate["beds"] is not None:
        score += 1
    if candidate["baths"] is not None:
        score += 1
    if candidate["status"]:
        score += 1
    if candidate["latitude"] is not None and candidate["longitude"] is not None:
        score += 1
    if candidate["last_updated"]:
        score += 1
    candidate["score"] = score
    return candidate


def _extract_from_html(html_text: str, url: str) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []

    for match in JSON_LD_SCRIPT_RE.findall(html_text):
        unescaped = html.unescape(match).strip()
        if not unescaped:
            continue
        try:
            parsed = json.loads(unescaped)
        except json.JSONDecodeError:
            continue
        candidates.append(_extract_candidate_from_json_blob(parsed, source="jsonld"))

    for match in NEXT_DATA_RE.findall(html_text):
        unescaped = html.unescape(match).strip()
        if not unescaped:
            continue
        try:
            parsed = json.loads(unescaped)
        except json.JSONDecodeError:
            continue
        candidates.append(_extract_candidate_from_json_blob(parsed, source="next_data"))

    fallback: dict[str, Any] = {
        "extract_source": "html_regex",
        "listing_id": "",
        "price": None,
        "beds": None,
        "baths": None,
        "address": "",
        "latitude": None,
        "longitude": None,
        "status": "",
        "last_updated": "",
        "score": 0,
    }

    id_from_url = URL_LISTING_ID_RE.search(url)
    if id_from_url:
        fallback["listing_id"] = _normalize_listing_id(id_from_url.group(1))
        fallback["score"] += 2
    mls_from_html = MLS_RE.search(html_text)
    if mls_from_html and not fallback["listing_id"]:
        fallback["listing_id"] = _normalize_listing_id(mls_from_html.group(1))
        fallback["score"] += 2

    price_match = re.search(r'"(?:listingPrice|listPrice|price)"\s*:\s*"?([0-9,\.]+)"?', html_text, re.IGNORECASE)
    if price_match:
        fallback["price"] = _to_float(price_match.group(1))
        fallback["score"] += 2

    status_match = re.search(
        r'"(?:listingStatus|status|propertyStatus)"\s*:\s*"([^"]+)"',
        html_text,
        re.IGNORECASE,
    )
    if status_match:
        fallback["status"] = _canonical_status(status_match.group(1))
        fallback["score"] += 1

    if fallback["score"] > 0:
        candidates.append(fallback)

    if not candidates:
        return fallback

    return max(candidates, key=lambda item: int(item.get("score", 0)))


def _normalize_address_for_compare(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", _normalize_space(value).lower())


def _addresses_match(left: str, right: str) -> bool | None:
    if not left or not right:
        return None
    a = _normalize_address_for_compare(left)
    b = _normalize_address_for_compare(right)
    if a == b:
        return True
    if len(a) >= 8 and len(b) >= 8 and (a in b or b in a):
        return True
    a_tokens = {token for token in a.split() if token}
    b_tokens = {token for token in b.split() if token}
    if not a_tokens or not b_tokens:
        return None
    overlap = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    if union == 0:
        return None
    return (overlap / union) >= 0.75


def _compare_numeric(left: float | None, right: float | None, tolerance: float) -> bool | None:
    if left is None or right is None:
        return None
    return abs(left - right) <= tolerance


def _compare_status(left: str, right: str) -> bool | None:
    if not left or not right:
        return None
    return _canonical_status(left) == _canonical_status(right)


def _compare_geo(
    truth_lat: float | None,
    truth_lon: float | None,
    scrape_lat: float | None,
    scrape_lon: float | None,
    tolerance: float,
) -> bool | None:
    if truth_lat is None or truth_lon is None or scrape_lat is None or scrape_lon is None:
        return None
    return abs(truth_lat - scrape_lat) <= tolerance and abs(truth_lon - scrape_lon) <= tolerance


def _bool_to_csv(value: bool | None) -> str:
    if value is None:
        return ""
    return "1" if value else "0"


def _prepare_truth_index(args: argparse.Namespace) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    truth_fieldnames, truth_rows = _load_csv_rows(Path(args.truth_csv))
    if not truth_fieldnames:
        raise RuntimeError(f"No header found in truth CSV: {args.truth_csv}")

    id_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_id_col,
        candidates=["listing_id", "mls_number", "mls", "listingid", "id"],
        label="truth id",
        required=True,
    )
    price_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_price_col,
        candidates=["price", "list_price", "listing_price", "asking_price", "sale_price"],
        label="truth price",
    )
    beds_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_beds_col,
        candidates=["beds", "bedrooms", "bedrooms_total", "bedroom_count"],
        label="truth beds",
    )
    baths_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_baths_col,
        candidates=["baths", "bathrooms", "bathrooms_total", "bathroom_count"],
        label="truth baths",
    )
    address_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_address_col,
        candidates=["address", "full_address", "property_address"],
        label="truth address",
    )
    status_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_status_col,
        candidates=["status", "listing_status", "property_status"],
        label="truth status",
    )
    lat_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_lat_col,
        candidates=["latitude", "lat", "geo_latitude"],
        label="truth latitude",
    )
    lon_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_lon_col,
        candidates=["longitude", "lon", "lng", "geo_longitude"],
        label="truth longitude",
    )
    updated_col = _pick_column(
        truth_fieldnames,
        explicit=args.truth_last_updated_col,
        candidates=["last_updated", "updated_at", "updated", "modified_at", "modification_date"],
        label="truth last_updated",
    )

    truth_by_id: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for row in truth_rows:
        listing_id = _normalize_listing_id(row.get(id_col, ""))
        if not listing_id:
            continue
        normalized = {
            "listing_id": listing_id,
            "price": _to_float(row.get(price_col, "")) if price_col else None,
            "beds": _to_float(row.get(beds_col, "")) if beds_col else None,
            "baths": _to_float(row.get(baths_col, "")) if baths_col else None,
            "address": _normalize_space(str(row.get(address_col, ""))) if address_col else "",
            "status": _canonical_status(row.get(status_col, "")) if status_col else "",
            "latitude": _to_float(row.get(lat_col, "")) if lat_col else None,
            "longitude": _to_float(row.get(lon_col, "")) if lon_col else None,
            "last_updated": _datetime_to_iso(_parse_datetime(row.get(updated_col, ""))) if updated_col else "",
        }
        if listing_id in truth_by_id:
            duplicate_count += 1
        truth_by_id[listing_id] = normalized

    detected_columns = {
        "id_col": id_col,
        "price_col": price_col,
        "beds_col": beds_col,
        "baths_col": baths_col,
        "address_col": address_col,
        "status_col": status_col,
        "lat_col": lat_col,
        "lon_col": lon_col,
        "updated_col": updated_col,
        "duplicate_count": str(duplicate_count),
    }
    return truth_by_id, detected_columns


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Authorized REALTOR.ca QA accuracy audit template: crawl listing pages, "
            "extract key fields, compare to source-of-truth CSV, and emit CSV/JSON metrics."
        )
    )
    parser.add_argument("--truth-csv", required=True, help="CSV containing source-of-truth listing data.")
    parser.add_argument("--urls-csv", required=True, help="CSV containing public listing URLs to audit.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for scraped_snapshot.csv, field_diff.csv, summary_metrics.json.",
    )
    parser.add_argument("--max-pages", type=int, default=0, help="Max URL rows to crawl (0 = all rows).")
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Delay between requests (seconds). Keep >=1 for low-impact audit runs.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout.")
    parser.add_argument("--max-retries", type=int, default=2, help="Retries for transient HTTP failures.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="Explicit audit user-agent.")
    parser.add_argument("--accept-language", default="en-CA,en;q=0.9", help="Accept-Language header value.")
    parser.add_argument(
        "--price-tolerance",
        type=float,
        default=DEFAULT_PRICE_TOLERANCE,
        help="Allowed absolute price difference before mismatch.",
    )
    parser.add_argument(
        "--numeric-tolerance",
        type=float,
        default=DEFAULT_NUMERIC_TOLERANCE,
        help="Allowed absolute difference for beds/baths before mismatch.",
    )
    parser.add_argument(
        "--geo-tolerance",
        type=float,
        default=DEFAULT_GEO_TOLERANCE,
        help="Allowed absolute lat/lon delta before mismatch.",
    )
    parser.add_argument(
        "--stale-threshold-hours",
        type=float,
        default=DEFAULT_STALE_THRESHOLD_HOURS,
        help="Lag threshold for stale listing rate (truth vs scraped last_updated).",
    )
    parser.add_argument("--urls-url-col", default="", help="Override URL column in urls CSV.")
    parser.add_argument("--urls-id-col", default="", help="Override listing id column in urls CSV.")
    parser.add_argument("--urls-complaint-col", default="", help="Override complaint flag column in urls CSV.")
    parser.add_argument("--truth-id-col", default="", help="Override listing id column in truth CSV.")
    parser.add_argument("--truth-price-col", default="", help="Override price column in truth CSV.")
    parser.add_argument("--truth-beds-col", default="", help="Override beds column in truth CSV.")
    parser.add_argument("--truth-baths-col", default="", help="Override baths column in truth CSV.")
    parser.add_argument("--truth-address-col", default="", help="Override address column in truth CSV.")
    parser.add_argument("--truth-status-col", default="", help="Override status column in truth CSV.")
    parser.add_argument("--truth-lat-col", default="", help="Override latitude column in truth CSV.")
    parser.add_argument("--truth-lon-col", default="", help="Override longitude column in truth CSV.")
    parser.add_argument("--truth-last-updated-col", default="", help="Override last_updated column in truth CSV.")
    parser.add_argument("--debug", action="store_true", help="Print per-row debug information.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    truth_by_id, truth_columns = _prepare_truth_index(args)
    urls_fieldnames, urls_rows = _load_csv_rows(Path(args.urls_csv))
    if not urls_fieldnames:
        raise RuntimeError(f"No header found in urls CSV: {args.urls_csv}")

    url_col = _pick_column(
        urls_fieldnames,
        explicit=args.urls_url_col,
        candidates=["url", "listing_url", "page_url", "link"],
        label="urls url",
        required=True,
    )
    id_col = _pick_column(
        urls_fieldnames,
        explicit=args.urls_id_col,
        candidates=["listing_id", "mls_number", "mls", "id", "listingid"],
        label="urls id",
    )
    complaint_col = _pick_column(
        urls_fieldnames,
        explicit=args.urls_complaint_col,
        candidates=["is_complaint", "complaint_flag", "complaint", "complaint_reported", "has_complaint"],
        label="urls complaint",
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    scrape_csv_path = output_dir / "scraped_snapshot.csv"
    diff_csv_path = output_dir / "field_diff.csv"
    summary_json_path = output_dir / "summary_metrics.json"

    max_pages = args.max_pages if args.max_pages > 0 else len(urls_rows)
    scrape_rows: list[dict[str, Any]] = []
    diff_rows: list[dict[str, Any]] = []

    metrics = {
        "total_urls_input": len(urls_rows),
        "total_urls_audited": 0,
        "http_success_count": 0,
        "http_error_count": 0,
        "matched_truth_count": 0,
        "missing_truth_count": 0,
        "complaint_rows": 0,
        "complaint_reproduced_count": 0,
        "field_stats": {
            "price": {"comparable": 0, "mismatch": 0},
            "beds": {"comparable": 0, "mismatch": 0},
            "baths": {"comparable": 0, "mismatch": 0},
            "address": {"comparable": 0, "mismatch": 0},
            "status": {"comparable": 0, "mismatch": 0},
            "geo": {"comparable": 0, "mismatch": 0},
        },
        "freshness": {"comparable": 0, "stale": 0},
        "detected_columns": {
            "urls": {
                "url_col": url_col,
                "id_col": id_col,
                "complaint_col": complaint_col,
            },
            "truth": truth_columns,
        },
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
    }

    last_request_finished = time.monotonic() - args.delay_seconds
    for index, url_row in enumerate(urls_rows[:max_pages], start=1):
        metrics["total_urls_audited"] += 1
        raw_url = _normalize_space(str(url_row.get(url_col, "")))
        if not raw_url:
            continue
        expected_id = _normalize_listing_id(url_row.get(id_col, "")) if id_col else ""
        complaint_flag = _to_boolish(url_row.get(complaint_col, "")) if complaint_col else False
        if complaint_flag:
            metrics["complaint_rows"] += 1

        elapsed = time.monotonic() - last_request_finished
        wait_seconds = args.delay_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        fetched_at = dt.datetime.now(dt.UTC)
        html_text, http_status, error = _http_get_text(
            raw_url,
            timeout_seconds=args.timeout_seconds,
            user_agent=args.user_agent,
            accept_language=args.accept_language,
            max_retries=args.max_retries,
        )
        last_request_finished = time.monotonic()

        if 200 <= http_status < 300:
            metrics["http_success_count"] += 1
        else:
            metrics["http_error_count"] += 1

        extracted = _extract_from_html(html_text, raw_url) if html_text else _extract_from_html("", raw_url)
        extracted_id = _normalize_listing_id(extracted.get("listing_id", ""))
        listing_id_used = expected_id or extracted_id

        scrape_row = {
            "row_index": index,
            "url": raw_url,
            "complaint_flag": "1" if complaint_flag else "0",
            "expected_listing_id": expected_id,
            "extracted_listing_id": extracted_id,
            "listing_id_used": listing_id_used,
            "http_status": http_status,
            "fetched_at_utc": fetched_at.isoformat(),
            "extract_source": extracted.get("extract_source", ""),
            "price": _format_float(_to_float(extracted.get("price"))),
            "beds": _format_float(_to_float(extracted.get("beds"))),
            "baths": _format_float(_to_float(extracted.get("baths"))),
            "address": _normalize_space(str(extracted.get("address", ""))),
            "latitude": _format_float(_to_float(extracted.get("latitude"))),
            "longitude": _format_float(_to_float(extracted.get("longitude"))),
            "status": _canonical_status(extracted.get("status", "")),
            "last_updated": _datetime_to_iso(_parse_datetime(extracted.get("last_updated"))),
            "error": _normalize_space(error),
        }
        scrape_rows.append(scrape_row)

        truth = truth_by_id.get(listing_id_used)
        in_truth = bool(truth)
        if in_truth:
            metrics["matched_truth_count"] += 1
        else:
            metrics["missing_truth_count"] += 1

        scrape_price = _to_float(scrape_row["price"])
        scrape_beds = _to_float(scrape_row["beds"])
        scrape_baths = _to_float(scrape_row["baths"])
        scrape_lat = _to_float(scrape_row["latitude"])
        scrape_lon = _to_float(scrape_row["longitude"])
        scrape_address = scrape_row["address"]
        scrape_status = scrape_row["status"]
        scrape_updated_dt = _parse_datetime(scrape_row["last_updated"])

        truth_price = truth.get("price") if truth else None
        truth_beds = truth.get("beds") if truth else None
        truth_baths = truth.get("baths") if truth else None
        truth_lat = truth.get("latitude") if truth else None
        truth_lon = truth.get("longitude") if truth else None
        truth_address = truth.get("address", "") if truth else ""
        truth_status = truth.get("status", "") if truth else ""
        truth_updated_dt = _parse_datetime(truth.get("last_updated")) if truth else None

        price_match = _compare_numeric(truth_price, scrape_price, args.price_tolerance) if truth else None
        beds_match = _compare_numeric(truth_beds, scrape_beds, args.numeric_tolerance) if truth else None
        baths_match = _compare_numeric(truth_baths, scrape_baths, args.numeric_tolerance) if truth else None
        address_match = _addresses_match(truth_address, scrape_address) if truth else None
        status_match = _compare_status(truth_status, scrape_status) if truth else None
        geo_match = (
            _compare_geo(truth_lat, truth_lon, scrape_lat, scrape_lon, args.geo_tolerance) if truth else None
        )

        freshness_lag_hours: float | None = None
        stale_flag = False
        if truth and truth_updated_dt and scrape_updated_dt:
            freshness_lag_hours = abs((scrape_updated_dt - truth_updated_dt).total_seconds()) / 3600
            metrics["freshness"]["comparable"] += 1
            if freshness_lag_hours > args.stale_threshold_hours:
                stale_flag = True
                metrics["freshness"]["stale"] += 1

        comparisons: dict[str, bool | None] = {
            "price": price_match,
            "beds": beds_match,
            "baths": baths_match,
            "address": address_match,
            "status": status_match,
            "geo": geo_match,
        }
        mismatch_count = 0
        for field, result in comparisons.items():
            if result is None:
                continue
            metrics["field_stats"][field]["comparable"] += 1
            if not result:
                metrics["field_stats"][field]["mismatch"] += 1
                mismatch_count += 1
        if stale_flag:
            mismatch_count += 1
        overall_mismatch = mismatch_count > 0
        if complaint_flag and overall_mismatch:
            metrics["complaint_reproduced_count"] += 1

        diff_rows.append(
            {
                "row_index": index,
                "url": raw_url,
                "complaint_flag": "1" if complaint_flag else "0",
                "listing_id_used": listing_id_used,
                "in_truth": "1" if in_truth else "0",
                "overall_mismatch": "1" if overall_mismatch else "0",
                "mismatch_count": mismatch_count,
                "stale_flag": "1" if stale_flag else "0",
                "freshness_lag_hours": _format_float(freshness_lag_hours),
                "price_match": _bool_to_csv(price_match),
                "beds_match": _bool_to_csv(beds_match),
                "baths_match": _bool_to_csv(baths_match),
                "address_match": _bool_to_csv(address_match),
                "status_match": _bool_to_csv(status_match),
                "geo_match": _bool_to_csv(geo_match),
                "truth_price": _format_float(truth_price),
                "scrape_price": _format_float(scrape_price),
                "truth_beds": _format_float(truth_beds),
                "scrape_beds": _format_float(scrape_beds),
                "truth_baths": _format_float(truth_baths),
                "scrape_baths": _format_float(scrape_baths),
                "truth_address": truth_address,
                "scrape_address": scrape_address,
                "truth_status": truth_status,
                "scrape_status": scrape_status,
                "truth_latitude": _format_float(truth_lat),
                "scrape_latitude": _format_float(scrape_lat),
                "truth_longitude": _format_float(truth_lon),
                "scrape_longitude": _format_float(scrape_lon),
                "truth_last_updated": _datetime_to_iso(truth_updated_dt),
                "scrape_last_updated": _datetime_to_iso(scrape_updated_dt),
            }
        )

        if args.debug:
            print(
                f"DEBUG row={index} status={http_status} "
                f"id_expected='{expected_id}' id_extracted='{extracted_id}' "
                f"id_used='{listing_id_used}' mismatch_count={mismatch_count} error='{error[:80]}'"
            )

    for field, values in metrics["field_stats"].items():
        comparable = values["comparable"]
        mismatch = values["mismatch"]
        values["mismatch_rate"] = round((mismatch / comparable), 6) if comparable else None

    freshness_comparable = metrics["freshness"]["comparable"]
    freshness_stale = metrics["freshness"]["stale"]
    metrics["freshness"]["stale_rate"] = (
        round((freshness_stale / freshness_comparable), 6) if freshness_comparable else None
    )

    complaint_rows = metrics["complaint_rows"]
    complaint_reproduced = metrics["complaint_reproduced_count"]
    metrics["complaint_reproduction_rate"] = (
        round((complaint_reproduced / complaint_rows), 6) if complaint_rows else None
    )

    _write_csv(scrape_csv_path, SCRAPE_FIELDNAMES, scrape_rows)
    _write_csv(diff_csv_path, DIFF_FIELDNAMES, diff_rows)
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"Audit complete. URLs audited: {metrics['total_urls_audited']}")
    print(f"HTTP success: {metrics['http_success_count']}, errors: {metrics['http_error_count']}")
    print(f"Truth matched: {metrics['matched_truth_count']}, missing truth: {metrics['missing_truth_count']}")
    print(f"Complaint reproduction rate: {metrics['complaint_reproduction_rate']}")
    print(f"Wrote: {scrape_csv_path}")
    print(f"Wrote: {diff_csv_path}")
    print(f"Wrote: {summary_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
