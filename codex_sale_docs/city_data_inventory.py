import argparse
import csv
import datetime as dt
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from codex_sale_docs.open_calgary import _extract_subject_unit, _normalize_space, _strip_unit_tokens, _to_float
from codex_sale_docs.sale_config import load_sale_settings


_SALE_SETTINGS = load_sale_settings()

DEFAULT_INPUT_CSV = Path("data/open_calgary_somervale_raw_rows_flat.csv")
DEFAULT_SUBJECT_ADDRESS = _SALE_SETTINGS["subject_address"]
DEFAULT_COMMUNITY_NAME = ""
DEFAULT_ALL_PROPERTIES_CSV = Path("data/open_calgary_somervale_all_properties.csv")
DEFAULT_CONDO_UNITS_CSV = Path("data/open_calgary_somervale_condo_units.csv")
DEFAULT_PARKING_UNITS_CSV = Path("data/open_calgary_somervale_parking_units.csv")
DEFAULT_STORAGE_UNITS_CSV = Path("data/open_calgary_somervale_storage_units.csv")
DEFAULT_OTHER_PROPERTIES_CSV = Path("data/open_calgary_somervale_other_properties.csv")
DEFAULT_UNIT_LINK_INDEX_CSV = Path("data/open_calgary_somervale_unit_link_index.csv")
DEFAULT_SUBJECT_PROFILE_JSON = Path("data/open_calgary_somervale_sale_subject_profile.json")
DEFAULT_SUBJECT_PROFILE_CSV = Path("data/open_calgary_somervale_sale_subject_profile.csv")
DEFAULT_BUILDING_SUMMARY_CSV = Path("data/open_calgary_somervale_building_summary.csv")
DEFAULT_SUB_PROPERTY_USE_COUNTS_CSV = Path("data/open_calgary_somervale_building_sub_property_use_counts.csv")
DEFAULT_SUMMARY_JSON = Path("data/open_calgary_somervale_inventory_summary.json")

SUBJECT_PROFILE_FIELDS = [
    "address",
    "roll_number",
    "unique_key",
    "cpid",
    "comm_code",
    "comm_name",
    "assessed_value",
    "re_assessed_value",
    "nr_assessed_value",
    "fl_assessed_value",
    "roll_year",
    "property_type",
    "assessment_class",
    "assessment_class_description",
    "land_use_designation",
    "sub_property_use",
    "year_of_construction",
    "land_size_sm",
    "land_size_sf",
    "land_size_ac",
    "mod_date",
]


def _unit_token_from_address(address: str) -> str:
    text = _normalize_space(address).upper()
    match = re.match(r"^\s*([A-Z0-9\-]+)\s+\d{3,6}[A-Z]?\b", text)
    if not match:
        return ""
    return match.group(1)


def _building_key_from_address(address: str) -> str:
    text = _normalize_space(address).upper()
    if not text:
        return "UNKNOWN"
    parts = text.split()
    if len(parts) >= 2 and parts[1].isdigit():
        return _normalize_space(f"{parts[1]} {' '.join(parts[2:])}")
    if parts[0].isdigit():
        return _normalize_space(f"{parts[0]} {' '.join(parts[1:])}")
    return text


def _base_unit_token(unit_token: str) -> str:
    token = re.sub(r"[^A-Z0-9]", "", unit_token.upper())
    match = re.match(r"^(\d+)[A-Z]$", token)
    if match:
        return match.group(1)
    return token


def _property_bucket(row: dict[str, str]) -> tuple[str, str]:
    sub_property_use = _normalize_space(row.get("sub_property_use", "")).upper()
    property_type = _normalize_space(row.get("property_type", "")).upper()
    class_desc = _normalize_space(row.get("assessment_class_description", "")).upper()

    if sub_property_use == "R201":
        return "condo_unit", "sub_property_use=R201"
    if sub_property_use == "A004":
        return "parking_unit", "sub_property_use=A004"
    if sub_property_use == "A005":
        return "storage_unit", "sub_property_use=A005"
    if sub_property_use:
        return "other_property", f"sub_property_use={sub_property_use}"
    if property_type and property_type != "LI":
        return "other_property", f"property_type={property_type}"
    if class_desc and class_desc != "RESIDENTIAL":
        return "other_property", f"assessment_class_description={class_desc}"
    return "other_property", "unclassified"


def _civic_number(text: str) -> str:
    match = re.match(r"^\s*(\d{3,6})\b", _normalize_space(text))
    if not match:
        return ""
    return match.group(1)


def _canonical_building_text(text: str) -> str:
    value = f" {_normalize_space(text).upper()} "
    replacements = {
        " COURT ": " CO ",
        " CT ": " CO ",
        " STREET ": " ST ",
        " AVENUE ": " AV ",
        " ROAD ": " RD ",
        " DRIVE ": " DR ",
    }
    for source, target in replacements.items():
        value = value.replace(source, target)
    return _normalize_space(value)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _subject_row_score(
    row: dict[str, str],
    subject_building: str,
    subject_civic: str,
    subject_unit: str,
    subject_local_address: str,
) -> int:
    score = 0
    if row.get("building_key", "") == subject_building:
        score += 50
    row_civic = _civic_number(row.get("building_key", ""))
    if subject_civic and row_civic == subject_civic:
        score += 80
    if _canonical_building_text(row.get("building_key", "")) == _canonical_building_text(subject_building):
        score += 40
    if subject_unit and row.get("unit_token", "") == subject_unit:
        score += 30
    if subject_unit and row.get("base_unit_token", "") == subject_unit:
        score += 10
    if row.get("address", "") == subject_local_address:
        score += 40
    bucket = row.get("property_bucket", "")
    if bucket == "condo_unit":
        score += 5
    return score


def build_city_data_inventory(
    *,
    input_csv: Path,
    subject_address: str,
    community_name: str,
    all_properties_csv: Path,
    condo_units_csv: Path,
    parking_units_csv: Path,
    storage_units_csv: Path,
    other_properties_csv: Path,
    unit_link_index_csv: Path,
    subject_profile_json: Path,
    subject_profile_csv: Path,
    building_summary_csv: Path,
    sub_property_use_counts_csv: Path,
    summary_json: Path,
) -> dict[str, Any]:
    if not input_csv.exists():
        raise RuntimeError(f"Missing input CSV: {input_csv}")

    with input_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if not rows:
        raise RuntimeError(f"No rows found in input CSV: {input_csv}")

    community_filter = _normalize_space(community_name).upper()
    community_field = "comm_name" if "comm_name" in fieldnames else ""
    if community_filter and community_field:
        rows = [row for row in rows if _normalize_space(row.get(community_field, "")).upper() == community_filter]
    if not rows:
        raise RuntimeError(
            f"No rows remained after applying community filter comm_name={community_filter or '(none)'}."
        )

    enriched_rows: list[dict[str, str]] = []
    for row in rows:
        enriched = dict(row)
        address = _normalize_space(str(enriched.get("address", "")).upper())
        unit_token = _unit_token_from_address(address)
        bucket, bucket_reason = _property_bucket(enriched)

        enriched["address"] = address
        enriched["building_key"] = _building_key_from_address(address)
        enriched["unit_token"] = unit_token
        enriched["base_unit_token"] = _base_unit_token(unit_token)
        enriched["property_bucket"] = bucket
        enriched["property_bucket_reason"] = bucket_reason
        enriched_rows.append(enriched)

    subject_address_normalized = _normalize_space(subject_address).upper()
    subject_local = _normalize_space(_strip_unit_tokens(subject_address).split(",")[0]).upper()
    subject_unit = _extract_subject_unit(subject_address).upper()
    subject_building = _building_key_from_address(subject_local)
    subject_civic = _civic_number(subject_building)

    subject_candidates = sorted(
        enriched_rows,
        key=lambda row: _subject_row_score(row, subject_building, subject_civic, subject_unit, subject_local),
        reverse=True,
    )
    subject_row = subject_candidates[0]

    linked_rows = [
        row
        for row in enriched_rows
        if row.get("building_key", "") == subject_row.get("building_key", "")
        and row.get("base_unit_token", "")
        and row.get("base_unit_token", "") == subject_row.get("base_unit_token", "")
    ]
    linked_bucket_counts = Counter(row.get("property_bucket", "other_property") for row in linked_rows)

    subject_profile_payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "subject_address_input": subject_address_normalized,
        "subject_address_matched": subject_row.get("address", ""),
        "subject_building_key": subject_row.get("building_key", ""),
        "subject_unit_token": subject_row.get("unit_token", ""),
        "subject_base_unit_token": subject_row.get("base_unit_token", ""),
        "community_name_filter": community_filter,
        "fields_worth_lookup": SUBJECT_PROFILE_FIELDS,
        "subject_bits": {field: subject_row.get(field, "") for field in SUBJECT_PROFILE_FIELDS},
        "linked_records_count": len(linked_rows),
        "linked_bucket_counts": dict(linked_bucket_counts),
    }
    _write_json(subject_profile_json, subject_profile_payload)

    subject_profile_rows = [
        {"field": field, "value": str(subject_profile_payload["subject_bits"].get(field, ""))}
        for field in SUBJECT_PROFILE_FIELDS
    ]
    subject_profile_rows.extend(
        [
            {"field": "subject_address_input", "value": subject_profile_payload["subject_address_input"]},
            {"field": "subject_address_matched", "value": subject_profile_payload["subject_address_matched"]},
            {"field": "subject_building_key", "value": subject_profile_payload["subject_building_key"]},
            {"field": "subject_unit_token", "value": subject_profile_payload["subject_unit_token"]},
            {"field": "subject_base_unit_token", "value": subject_profile_payload["subject_base_unit_token"]},
            {"field": "linked_records_count", "value": str(subject_profile_payload["linked_records_count"])},
            {"field": "linked_bucket_counts", "value": json.dumps(subject_profile_payload["linked_bucket_counts"])},
        ]
    )
    _write_csv(subject_profile_csv, ["field", "value"], subject_profile_rows)

    enriched_fieldnames = sorted({key for row in enriched_rows for key in row.keys()})
    _write_csv(all_properties_csv, enriched_fieldnames, enriched_rows)

    condo_rows = [row for row in enriched_rows if row.get("property_bucket") == "condo_unit"]
    parking_rows = [row for row in enriched_rows if row.get("property_bucket") == "parking_unit"]
    storage_rows = [row for row in enriched_rows if row.get("property_bucket") == "storage_unit"]
    other_rows = [row for row in enriched_rows if row.get("property_bucket") == "other_property"]

    _write_csv(condo_units_csv, enriched_fieldnames, condo_rows)
    _write_csv(parking_units_csv, enriched_fieldnames, parking_rows)
    _write_csv(storage_units_csv, enriched_fieldnames, storage_rows)
    _write_csv(other_properties_csv, enriched_fieldnames, other_rows)

    unit_link_rows: list[dict[str, str]] = []
    grouped_links: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in enriched_rows:
        base_unit = row.get("base_unit_token", "")
        building_key = row.get("building_key", "")
        if not base_unit or not building_key:
            continue
        grouped_links[(building_key, base_unit)].append(row)

    for (building_key, base_unit), items in sorted(grouped_links.items()):
        counts = Counter(item.get("property_bucket", "other_property") for item in items)
        addresses = sorted({item.get("address", "") for item in items if item.get("address", "")})
        unit_tokens = sorted({item.get("unit_token", "") for item in items if item.get("unit_token", "")})
        unit_link_rows.append(
            {
                "building_key": building_key,
                "base_unit_token": base_unit,
                "record_count": str(len(items)),
                "condo_count": str(counts.get("condo_unit", 0)),
                "parking_count": str(counts.get("parking_unit", 0)),
                "storage_count": str(counts.get("storage_unit", 0)),
                "other_count": str(counts.get("other_property", 0)),
                "unit_tokens": "; ".join(unit_tokens),
                "addresses": "; ".join(addresses),
            }
        )
    _write_csv(
        unit_link_index_csv,
        [
            "building_key",
            "base_unit_token",
            "record_count",
            "condo_count",
            "parking_count",
            "storage_count",
            "other_count",
            "unit_tokens",
            "addresses",
        ],
        unit_link_rows,
    )

    building_rows: list[dict[str, str]] = []
    by_building: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in enriched_rows:
        by_building[row.get("building_key", "UNKNOWN")].append(row)

    for building_key, items in sorted(by_building.items()):
        assessed_values = [_to_float(item.get("assessed_value")) for item in items]
        assessed = [value for value in assessed_values if value is not None]
        sub_property_uses = {_normalize_space(item.get("sub_property_use", "")) for item in items}
        land_use_designations = {_normalize_space(item.get("land_use_designation", "")) for item in items}
        bucket_counts = Counter(item.get("property_bucket", "other_property") for item in items)
        building_rows.append(
            {
                "building_key": building_key,
                "row_count": str(len(items)),
                "distinct_roll_numbers": str(
                    len({_normalize_space(item.get("roll_number", "")) for item in items if item.get("roll_number", "")})
                ),
                "distinct_sub_property_use": str(len({value for value in sub_property_uses if value})),
                "distinct_land_use_designation": str(len({value for value in land_use_designations if value})),
                "min_assessed_value": f"{min(assessed):.0f}" if assessed else "",
                "max_assessed_value": f"{max(assessed):.0f}" if assessed else "",
                "condo_rows": str(bucket_counts.get("condo_unit", 0)),
                "parking_rows": str(bucket_counts.get("parking_unit", 0)),
                "storage_rows": str(bucket_counts.get("storage_unit", 0)),
                "other_rows": str(bucket_counts.get("other_property", 0)),
            }
        )
    _write_csv(
        building_summary_csv,
        [
            "building_key",
            "row_count",
            "distinct_roll_numbers",
            "distinct_sub_property_use",
            "distinct_land_use_designation",
            "min_assessed_value",
            "max_assessed_value",
            "condo_rows",
            "parking_rows",
            "storage_rows",
            "other_rows",
        ],
        building_rows,
    )

    sub_property_use_rows: list[dict[str, str]] = []
    for building_key, items in sorted(by_building.items()):
        counts = Counter(_normalize_space(item.get("sub_property_use", "")) for item in items)
        for sub_property_use, row_count in sorted(counts.items(), key=lambda item: (item[0], item[1])):
            sub_property_use_rows.append(
                {
                    "building_key": building_key,
                    "sub_property_use": sub_property_use,
                    "row_count": str(row_count),
                }
            )
    _write_csv(sub_property_use_counts_csv, ["building_key", "sub_property_use", "row_count"], sub_property_use_rows)

    bucket_counts = Counter(row.get("property_bucket", "other_property") for row in enriched_rows)
    summary_payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "input_csv": str(input_csv),
        "community_name_filter": community_filter,
        "rows_total": len(enriched_rows),
        "rows_condo": bucket_counts.get("condo_unit", 0),
        "rows_parking": bucket_counts.get("parking_unit", 0),
        "rows_storage": bucket_counts.get("storage_unit", 0),
        "rows_other": bucket_counts.get("other_property", 0),
        "distinct_buildings": len(by_building),
        "subject_profile_json": str(subject_profile_json),
        "subject_profile_csv": str(subject_profile_csv),
        "all_properties_csv": str(all_properties_csv),
        "condo_units_csv": str(condo_units_csv),
        "parking_units_csv": str(parking_units_csv),
        "storage_units_csv": str(storage_units_csv),
        "other_properties_csv": str(other_properties_csv),
        "unit_link_index_csv": str(unit_link_index_csv),
        "building_summary_csv": str(building_summary_csv),
        "sub_property_use_counts_csv": str(sub_property_use_counts_csv),
    }
    _write_json(summary_json, summary_payload)

    return summary_payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a comprehensive city-data property inventory for the fetched dataset (street scope by default), "
            "including condo/parking/storage/other splits and subject property profile outputs."
        )
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV), help="Input flat city-data CSV.")
    parser.add_argument("--subject-address", default=DEFAULT_SUBJECT_ADDRESS, help="Subject sale address.")
    parser.add_argument("--community-name", default=DEFAULT_COMMUNITY_NAME, help="Community filter (comm_name).")
    parser.add_argument("--all-properties-csv", default=str(DEFAULT_ALL_PROPERTIES_CSV), help="All-property output CSV.")
    parser.add_argument("--condo-units-csv", default=str(DEFAULT_CONDO_UNITS_CSV), help="Condo-unit output CSV.")
    parser.add_argument("--parking-units-csv", default=str(DEFAULT_PARKING_UNITS_CSV), help="Parking-unit output CSV.")
    parser.add_argument("--storage-units-csv", default=str(DEFAULT_STORAGE_UNITS_CSV), help="Storage-unit output CSV.")
    parser.add_argument(
        "--other-properties-csv",
        default=str(DEFAULT_OTHER_PROPERTIES_CSV),
        help="Other/exceptional-property output CSV.",
    )
    parser.add_argument(
        "--unit-link-index-csv",
        default=str(DEFAULT_UNIT_LINK_INDEX_CSV),
        help="Unit linkage index output CSV.",
    )
    parser.add_argument(
        "--subject-profile-json",
        default=str(DEFAULT_SUBJECT_PROFILE_JSON),
        help="Subject profile output JSON.",
    )
    parser.add_argument(
        "--subject-profile-csv",
        default=str(DEFAULT_SUBJECT_PROFILE_CSV),
        help="Subject profile output CSV.",
    )
    parser.add_argument(
        "--building-summary-csv",
        default=str(DEFAULT_BUILDING_SUMMARY_CSV),
        help="Building-level summary output CSV.",
    )
    parser.add_argument(
        "--sub-property-use-counts-csv",
        default=str(DEFAULT_SUB_PROPERTY_USE_COUNTS_CSV),
        help="Building/sub_property_use counts output CSV.",
    )
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON), help="Inventory run summary JSON.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        result = build_city_data_inventory(
            input_csv=Path(args.input_csv),
            subject_address=args.subject_address,
            community_name=args.community_name,
            all_properties_csv=Path(args.all_properties_csv),
            condo_units_csv=Path(args.condo_units_csv),
            parking_units_csv=Path(args.parking_units_csv),
            storage_units_csv=Path(args.storage_units_csv),
            other_properties_csv=Path(args.other_properties_csv),
            unit_link_index_csv=Path(args.unit_link_index_csv),
            subject_profile_json=Path(args.subject_profile_json),
            subject_profile_csv=Path(args.subject_profile_csv),
            building_summary_csv=Path(args.building_summary_csv),
            sub_property_use_counts_csv=Path(args.sub_property_use_counts_csv),
            summary_json=Path(args.summary_json),
        )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1

    print(f"Rows total: {result['rows_total']}")
    print(f"Rows condo: {result['rows_condo']}")
    print(f"Rows parking: {result['rows_parking']}")
    print(f"Rows storage: {result['rows_storage']}")
    print(f"Rows other: {result['rows_other']}")
    print(f"Wrote summary JSON: {args.summary_json}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
