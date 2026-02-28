import argparse
import csv
import datetime as dt
import json
import re
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DEFAULT_INPUT_CSV = Path("data/open_calgary_somervale_raw_rows_flat.csv")
DEFAULT_OUTPUT_DIR = Path("source/city_data")
DEFAULT_INDEX_RST = Path("source/91_city_data_index.rst")
DEFAULT_RAW_JSON_PATH = Path("data/open_calgary_somervale_raw_rows.json")
DEFAULT_INDEX_TITLE = "City Data: Somervale Court SW"
DEFAULT_NON_RESIDENTIAL_TITLE = "Non-Residential / Exceptional Records (Somervale Court SW)"

DEFAULT_FIELD_ORDER = [
    "address",
    "roll_number",
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
    "comm_code",
    "comm_name",
    "cpid",
    "unique_key",
    "mod_date",
    "entry_notes",
]


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "item"


def _unit_token_from_address(address: str) -> str:
    text = _normalize_space(address).upper()
    match = re.match(r"^\s*([A-Z0-9\-]+)\s+(\d{3,6}[A-Z]?)\b", text)
    if not match:
        return ""
    return match.group(1)


def _building_key_from_address(address: str) -> str:
    text = _normalize_space(address).upper()
    parts = text.split()
    if not parts:
        return "UNKNOWN"
    if len(parts) >= 2 and parts[1].isdigit():
        civic = parts[1]
        tail = " ".join(parts[2:])
        return _normalize_space(f"{civic} {tail}")
    civic = parts[0]
    tail = " ".join(parts[1:])
    return _normalize_space(f"{civic} {tail}")


def _building_sort_key(building_key: str) -> tuple[int, int, str, str]:
    text = building_key.upper().strip()
    token = text.split()[0] if text else ""
    match = re.match(r"^(\d+)([A-Z]*)$", token)
    if match:
        return (0, int(match.group(1)), match.group(2), text)
    return (1, 10**9, "", text)


def _address_sort_key(address: str) -> tuple[int, str]:
    return (0, address.upper())


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        return float(raw)
    except ValueError:
        return None


def _is_non_residential_bucket(row: dict[str, str]) -> bool:
    property_type = str(row.get("property_type", "")).strip().upper()
    class_desc = str(row.get("assessment_class_description", "")).strip().upper()
    land_use = str(row.get("land_use_designation", "")).strip().upper()
    sub_property_use = str(row.get("sub_property_use", "")).strip().upper()
    if property_type and property_type != "LI":
        return True
    if class_desc and class_desc != "RESIDENTIAL":
        return True
    if land_use in {"S-SPR", "S-UN"}:
        return True
    if sub_property_use in {"A006", "X057"}:
        return True
    return False


def _iqr_bounds(values: list[float]) -> tuple[float, float] | None:
    if len(values) < 4:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 0:
        lower = ordered[:mid]
        upper = ordered[mid:]
    else:
        lower = ordered[:mid]
        upper = ordered[mid + 1 :]
    if not lower or not upper:
        return None

    def _median(seq: list[float]) -> float:
        size = len(seq)
        middle = size // 2
        if size % 2 == 0:
            return (seq[middle - 1] + seq[middle]) / 2.0
        return seq[middle]

    q1 = _median(lower)
    q3 = _median(upper)
    iqr = q3 - q1
    return (q1 - 1.5 * iqr, q3 + 1.5 * iqr)


def _prepare_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    prepared: list[dict[str, str]] = []
    for row in rows:
        cloned = dict(row)
        cloned["address"] = _normalize_space(str(cloned.get("address", "")).upper())
        cloned["building_key"] = _building_key_from_address(cloned["address"])
        cloned["unit_token"] = _unit_token_from_address(cloned["address"])
        prepared.append(cloned)

    address_counts_by_building: dict[tuple[str, str], int] = Counter(
        (row["building_key"], row["address"]) for row in prepared
    )
    roll_counts = Counter(row.get("roll_number", "").strip() for row in prepared if row.get("roll_number", "").strip())

    r201_unit_bases: dict[str, set[str]] = defaultdict(set)
    for row in prepared:
        if str(row.get("sub_property_use", "")).strip().upper() != "R201":
            continue
        unit_token = row.get("unit_token", "").strip().upper()
        if re.fullmatch(r"\d+", unit_token):
            r201_unit_bases[row["building_key"]].add(unit_token)

    bounds_by_building: dict[str, tuple[float, float]] = {}
    for building_key in {row["building_key"] for row in prepared}:
        values = [
            value
            for value in (_to_float(row.get("assessed_value")) for row in prepared if row["building_key"] == building_key)
            if value is not None
        ]
        bounds = _iqr_bounds(values)
        if bounds:
            bounds_by_building[building_key] = bounds

    for row in prepared:
        notes: list[str] = []
        duplicate_count = address_counts_by_building.get((row["building_key"], row["address"]), 0)
        if duplicate_count > 1:
            notes.append(f"duplicate_address_x{duplicate_count}")

        roll = row.get("roll_number", "").strip()
        if roll and roll_counts.get(roll, 0) > 1:
            notes.append(f"duplicate_roll_number_x{roll_counts[roll]}")

        unit_token = row.get("unit_token", "").strip().upper()
        if not unit_token:
            notes.append("blank_unit_token")

        sub_property_use = str(row.get("sub_property_use", "")).strip().upper()
        if sub_property_use in {"A004", "A005", "A006", "X057"}:
            notes.append(f"accessory_or_exceptional_code:{sub_property_use}")

        if sub_property_use == "A004":
            match = re.fullmatch(r"(\d+)V", unit_token)
            if not match:
                notes.append("a004_unexpected_unit_token")
            elif match.group(1) in r201_unit_bases[row["building_key"]]:
                notes.append("a004_base_matches_r201")
            else:
                notes.append("a004_base_missing_r201")

        if sub_property_use == "A005":
            match = re.fullmatch(r"(\d+)S", unit_token)
            if not match:
                notes.append("a005_unexpected_unit_token")
            elif match.group(1) in r201_unit_bases[row["building_key"]]:
                notes.append("a005_base_matches_r201")
            else:
                notes.append("a005_base_missing_r201")

        assessed_value = _to_float(row.get("assessed_value"))
        if assessed_value is not None and row["building_key"] in bounds_by_building:
            low, high = bounds_by_building[row["building_key"]]
            if assessed_value < low or assessed_value > high:
                notes.append("assessed_value_outlier_iqr")

        if _is_non_residential_bucket(row):
            notes.append("non_residential_bucket")

        row["entry_notes"] = "; ".join(sorted(set(notes)))
    return prepared


def _select_fields(rows: list[dict[str, str]], include_multipolygon: bool) -> list[str]:
    discovered = sorted({key for row in rows for key in row.keys()})
    omitted = {"building_key", "unit_token"}
    if not include_multipolygon:
        omitted.add("multipolygon")
    ordered = [field for field in DEFAULT_FIELD_ORDER if field in discovered and field not in omitted]
    extras = [field for field in discovered if field not in ordered and field not in omitted]
    return [*ordered, *extras]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _safe_toctree_path(source_root: Path, rst_path: Path) -> str:
    relative = rst_path.relative_to(source_root)
    return str(relative.with_suffix("")).replace("\\", "/")


def _render_building_page(
    *,
    source_root: Path,
    output_dir: Path,
    tables_dir: Path,
    building_key: str,
    rows: list[dict[str, str]],
    fieldnames: list[str],
) -> Path:
    building_slug = _slugify(building_key)
    page_path = output_dir / f"building_{building_slug}.rst"
    building_table_dir = tables_dir / building_slug
    building_table_dir.mkdir(parents=True, exist_ok=True)

    by_address: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_address[row["address"]].append(row)

    duplicate_addresses = {address: len(items) for address, items in by_address.items() if len(items) > 1}
    entry_note_counts = Counter(
        note
        for row in rows
        for note in [part.strip() for part in row.get("entry_notes", "").split(";") if part.strip()]
    )

    title = f"City Data: {building_key.title()}"
    lines = [title, "=" * len(title), ""]
    lines.append("All entries are included exactly as fetched from City of Calgary open data.")
    lines.append("")
    lines.append(f"- Total rows: ``{len(rows)}``")
    lines.append(f"- Distinct addresses: ``{len(by_address)}``")
    lines.append(f"- Distinct roll numbers: ``{len({row.get('roll_number', '') for row in rows if row.get('roll_number', '')})}``")
    lines.append("")

    if duplicate_addresses:
        lines.append(".. warning::")
        lines.append("   Duplicate address entries are present in this building.")
        for address, count in sorted(duplicate_addresses.items(), key=lambda item: item[0]):
            lines.append(f"   - ``{address}`` appears ``{count}`` times.")
        lines.append("")

    lines.append(".. note::")
    lines.append("   Rows flagged as odd/auxiliary are intentionally retained. Review ``entry_notes`` per row.")
    lines.append("")

    if entry_note_counts:
        lines.append("Flag Summary")
        lines.append("------------")
        lines.append("")
        for note, count in sorted(entry_note_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- ``{note}``: ``{count}``")
        lines.append("")

    lines.append("Address Index")
    lines.append("-------------")
    lines.append("")
    lines.append(".. contents::")
    lines.append("   :local:")
    lines.append("   :depth: 1")
    lines.append("")

    address_name_counts: dict[str, int] = defaultdict(int)
    for address in sorted(by_address, key=_address_sort_key):
        address_rows = sorted(by_address[address], key=lambda row: row.get("roll_number", ""))
        address_name_counts[address] += 1
        address_slug = _slugify(address)
        suffix = f"_{address_name_counts[address]}" if address_name_counts[address] > 1 else ""
        csv_name = f"{address_slug}{suffix}.csv"
        csv_path = building_table_dir / csv_name
        _write_csv(csv_path, fieldnames, address_rows)

        lines.append(address)
        lines.append("^" * len(address))
        lines.append("")
        lines.append(f"- Row count for this address: ``{len(address_rows)}``")
        distinct_rolls = {row.get("roll_number", "") for row in address_rows if row.get("roll_number", "")}
        lines.append(f"- Distinct roll numbers: ``{len(distinct_rolls)}``")
        if len(address_rows) > 1:
            lines.append("")
            lines.append(".. warning::")
            lines.append("   This address has multiple rows in the source data.")
        lines.append("")
        note_counts = Counter(
            note
            for row in address_rows
            for note in [part.strip() for part in row.get("entry_notes", "").split(";") if part.strip()]
        )
        if note_counts:
            lines.append(".. note::")
            lines.append("   Flags in this address block:")
            for note, count in sorted(note_counts.items(), key=lambda item: (-item[1], item[0])):
                lines.append(f"   - ``{note}`` (``{count}`` row(s))")
            lines.append("")
        lines.append(f".. csv-table:: Records for {address}")
        lines.append(f"   :file: _tables/{building_slug}/{csv_name}")
        lines.append("   :header-rows: 1")
        lines.append("   :widths: auto")
        lines.append("")

    page_path.write_text("\n".join(lines), encoding="utf-8")
    return page_path


def _render_non_residential_page(
    *,
    output_dir: Path,
    tables_dir: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str],
    title: str,
) -> Path:
    page_path = output_dir / "non_residential_somervale.rst"
    table_path = tables_dir / "non_residential_rows.csv"
    _write_csv(table_path, fieldnames, rows)

    by_building = Counter(row["building_key"] for row in rows)
    by_code = Counter(row.get("sub_property_use", "") for row in rows)

    lines = [title, "=" * len(title), ""]
    lines.append("This page consolidates rows flagged as non-residential or exceptional.")
    lines.append("")
    lines.append(f"- Total rows: ``{len(rows)}``")
    lines.append("")
    lines.append("Counts By Building")
    lines.append("------------------")
    lines.append("")
    for building, count in sorted(by_building.items(), key=lambda item: _building_sort_key(item[0])):
        lines.append(f"- ``{building}``: ``{count}``")
    lines.append("")
    lines.append("Counts By ``sub_property_use``")
    lines.append("------------------------------")
    lines.append("")
    for code, count in sorted(by_code.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- ``{code}``: ``{count}``")
    lines.append("")
    lines.append(".. warning::")
    lines.append("   These rows are retained for completeness even when they appear non-unit or exceptional.")
    lines.append("")
    lines.append(".. csv-table:: Non-Residential / Exceptional City Data Rows")
    lines.append("   :file: _tables/non_residential_rows.csv")
    lines.append("   :header-rows: 1")
    lines.append("   :widths: auto")
    lines.append("")
    page_path.write_text("\n".join(lines), encoding="utf-8")
    return page_path


def render_city_data_rst(
    *,
    input_csv: Path,
    output_dir: Path,
    index_rst: Path,
    raw_json_path: Path,
    include_multipolygon: bool,
    index_title: str,
    non_residential_title: str,
) -> dict[str, Any]:
    if not input_csv.exists():
        raise RuntimeError(f"Missing input CSV: {input_csv}")
    with input_csv.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise RuntimeError(f"No rows found in input CSV: {input_csv}")

    prepared = _prepare_rows(rows)
    fieldnames = _select_fields(prepared, include_multipolygon=include_multipolygon)

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = output_dir / "_tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    residential_rows = [row for row in prepared if not _is_non_residential_bucket(row)]
    non_residential_rows = [row for row in prepared if _is_non_residential_bucket(row)]

    rows_by_building: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in residential_rows:
        rows_by_building[row["building_key"]].append(row)

    building_pages: list[Path] = []
    for building_key in sorted(rows_by_building, key=_building_sort_key):
        page = _render_building_page(
            source_root=index_rst.parent,
            output_dir=output_dir,
            tables_dir=tables_dir,
            building_key=building_key,
            rows=rows_by_building[building_key],
            fieldnames=fieldnames,
        )
        building_pages.append(page)

    non_res_page = _render_non_residential_page(
        output_dir=output_dir,
        tables_dir=tables_dir,
        rows=sorted(non_residential_rows, key=lambda row: (_building_sort_key(row["building_key"]), row["address"])),
        fieldnames=fieldnames,
        title=non_residential_title,
    )

    run_timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    index_lines = [index_title, "=" * len(index_title), ""]
    index_lines.append("Generated city-data navigation pages grouped by building and address.")
    index_lines.append("")
    index_lines.append(f"- Generated at: ``{run_timestamp}``")
    index_lines.append(f"- Source CSV: ``{input_csv}``")
    index_lines.append(f"- Raw JSON: ``{raw_json_path}``")
    index_lines.append(f"- Residential rows: ``{len(residential_rows)}``")
    index_lines.append(f"- Non-residential / exceptional rows: ``{len(non_residential_rows)}``")
    index_lines.append("")
    index_lines.append(".. toctree::")
    index_lines.append("   :maxdepth: 1")
    index_lines.append("   :caption: City Data Pages")
    index_lines.append("")
    index_lines.append(f"   {_safe_toctree_path(index_rst.parent, non_res_page)}")
    for page in building_pages:
        index_lines.append(f"   {_safe_toctree_path(index_rst.parent, page)}")
    index_lines.append("")
    index_rst.parent.mkdir(parents=True, exist_ok=True)
    index_rst.write_text("\n".join(index_lines), encoding="utf-8")

    return {
        "input_rows": len(rows),
        "residential_rows": len(residential_rows),
        "non_residential_rows": len(non_residential_rows),
        "building_pages": len(building_pages),
        "output_dir": str(output_dir),
        "index_rst": str(index_rst),
        "non_residential_page": str(non_res_page),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert fetched City of Calgary flat CSV data into navigable rST pages "
            "(per building + non-residential summary + index page)."
        )
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV), help="Input flat CSV from fetch_city_data.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for building pages.")
    parser.add_argument("--index-rst", default=str(DEFAULT_INDEX_RST), help="Output top-level city-data index rST.")
    parser.add_argument(
        "--raw-json-path",
        default=str(DEFAULT_RAW_JSON_PATH),
        help="Raw JSON source path for provenance note.",
    )
    parser.add_argument(
        "--include-multipolygon",
        action="store_true",
        help="Include the full multipolygon geometry field in generated tables.",
    )
    parser.add_argument("--index-title", default=DEFAULT_INDEX_TITLE, help="Title for the city-data index page.")
    parser.add_argument(
        "--non-residential-title",
        default=DEFAULT_NON_RESIDENTIAL_TITLE,
        help="Title for the non-residential/exceptional page.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        result = render_city_data_rst(
            input_csv=Path(args.input_csv),
            output_dir=Path(args.output_dir),
            index_rst=Path(args.index_rst),
            raw_json_path=Path(args.raw_json_path),
            include_multipolygon=bool(args.include_multipolygon),
            index_title=args.index_title,
            non_residential_title=args.non_residential_title,
        )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1

    print(f"Input rows: {result['input_rows']}")
    print(f"Residential rows: {result['residential_rows']}")
    print(f"Non-residential / exceptional rows: {result['non_residential_rows']}")
    print(f"Building pages: {result['building_pages']}")
    print(f"Wrote city-data pages to: {result['output_dir']}")
    print(f"Wrote index page: {result['index_rst']}")
    print(f"Wrote non-residential page: {result['non_residential_page']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
