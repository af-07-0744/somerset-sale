import csv
import sys
from pathlib import Path


DATA_DIR = Path("data")
COMPS_FILE = DATA_DIR / "comps_clean.csv"
SOURCES_FILE = DATA_DIR / "source_registry.csv"

REQUIRED_COMP_COLUMNS = {
    "comp_id",
    "address",
    "status",
    "list_price",
    "sold_price",
    "sale_date",
    "sqft",
    "dom",
    "source_ids",
}

REQUIRED_SOURCE_COLUMNS = {
    "source_id",
    "comp_id",
    "source_type",
    "mls_number",
    "url",
    "publisher",
    "captured_at",
    "captured_by",
    "file_path",
    "file_sha256",
    "claims_supported",
    "notes",
}


def _read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        columns = reader.fieldnames or []
    return rows, columns


def _split_ids(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(";") if item.strip()]


def validate() -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    comps_rows, comps_columns = _read_csv(COMPS_FILE)
    source_rows, source_columns = _read_csv(SOURCES_FILE)

    missing_comp_columns = REQUIRED_COMP_COLUMNS.difference(comps_columns)
    if missing_comp_columns:
        errors.append(
            f"{COMPS_FILE}: missing columns: {', '.join(sorted(missing_comp_columns))}"
        )

    missing_source_columns = REQUIRED_SOURCE_COLUMNS.difference(source_columns)
    if missing_source_columns:
        errors.append(
            f"{SOURCES_FILE}: missing columns: {', '.join(sorted(missing_source_columns))}"
        )

    source_ids_by_row: dict[str, dict[str, str]] = {}
    for row_index, row in enumerate(source_rows, start=2):
        source_id = row.get("source_id", "").strip()
        comp_id = row.get("comp_id", "").strip()
        url = row.get("url", "").strip()
        mls_number = row.get("mls_number", "").strip()
        file_path_value = row.get("file_path", "").strip()

        if not source_id:
            errors.append(f"{SOURCES_FILE}:{row_index}: source_id is required")
            continue

        source_ids_by_row[source_id] = row

        if not comp_id:
            errors.append(f"{SOURCES_FILE}:{row_index}: comp_id is required")

        if not any([url, mls_number, file_path_value]):
            errors.append(
                f"{SOURCES_FILE}:{row_index}: include at least one of url, mls_number, or file_path"
            )

        if file_path_value and not Path(file_path_value).exists():
            warnings.append(
                f"{SOURCES_FILE}:{row_index}: file_path does not exist yet: {file_path_value}"
            )

    for row_index, row in enumerate(comps_rows, start=2):
        comp_id = row.get("comp_id", "").strip()
        source_ids = _split_ids(row.get("source_ids", ""))

        if not comp_id:
            errors.append(f"{COMPS_FILE}:{row_index}: comp_id is required")

        if not source_ids:
            errors.append(f"{COMPS_FILE}:{row_index}: source_ids is required")
            continue

        for source_id in source_ids:
            if source_id not in source_ids_by_row:
                errors.append(
                    f"{COMPS_FILE}:{row_index}: source_id '{source_id}' not found in {SOURCES_FILE}"
                )

    if not comps_rows:
        warnings.append(f"{COMPS_FILE}: no comps rows yet")
    if not source_rows:
        warnings.append(f"{SOURCES_FILE}: no source rows yet")

    return errors, warnings


def main() -> int:
    try:
        errors, warnings = validate()
    except FileNotFoundError as exception:
        print(str(exception))
        return 1

    for warning in warnings:
        print(f"WARNING: {warning}")

    if errors:
        print("Provenance validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Provenance validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
