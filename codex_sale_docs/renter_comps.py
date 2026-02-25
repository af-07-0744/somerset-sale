import argparse
import csv
import datetime as dt
import os
from pathlib import Path
from typing import Any


DEFAULT_INPUT_CSV = Path("data/open_calgary_assessment_comps.csv")
DEFAULT_OUTPUT_CSV = Path("data/renter_comps_table.csv")
DEFAULT_OUTPUT_RST = Path("source/03b_renter_comps_generated.rst")

OUTPUT_FIELDNAMES = [
    "Rank",
    "Address",
    "Community",
    "Assessment Value (CAD)",
    "Difference vs Subject (CAD)",
    "Difference vs Subject (%)",
    "Property Type",
    "Sqft",
    "Source ID",
]


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _format_currency(value: float | None) -> str:
    if value is None:
        return ""
    if value < 0:
        return f"-${abs(value):,.0f}"
    return f"${value:,.0f}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value * 100:+.2f}%"


def _load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise RuntimeError(f"Missing input file: {path}")
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise RuntimeError(f"No data rows in input file: {path}")
    return rows


def _sort_key(row: dict[str, str]) -> tuple[float, float]:
    delta_pct = _to_float(row.get("value_delta_pct"))
    delta_abs = abs(delta_pct) if delta_pct is not None else 10**9
    assessed = _to_float(row.get("assessed_value")) or 0.0
    return (delta_abs, -assessed)


def _prepare_rows(
    rows: list[dict[str, str]],
    top_n: int,
) -> list[dict[str, str]]:
    sorted_rows = sorted(rows, key=_sort_key)[:top_n]
    output_rows: list[dict[str, str]] = []
    for index, row in enumerate(sorted_rows, start=1):
        assessed_value = _to_float(row.get("assessed_value"))
        delta_value = _to_float(row.get("value_delta"))
        delta_pct = _to_float(row.get("value_delta_pct"))
        output_rows.append(
            {
                "Rank": str(index),
                "Address": row.get("address", ""),
                "Community": row.get("community", ""),
                "Assessment Value (CAD)": _format_currency(assessed_value),
                "Difference vs Subject (CAD)": _format_currency(delta_value),
                "Difference vs Subject (%)": _format_pct(delta_pct),
                "Property Type": row.get("property_type", ""),
                "Sqft": row.get("sqft", ""),
                "Source ID": row.get("source_id", ""),
            }
        )
    return output_rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_rst(
    input_csv_path: Path,
    output_csv_path: Path,
    output_rst_path: Path,
    included_rows: int,
    dataset_ids: list[str],
    row_count: int,
) -> str:
    dataset_text = ", ".join(dataset_ids) if dataset_ids else "unknown"
    generated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    csv_path_for_rst = os.path.relpath(output_csv_path, output_rst_path.parent).replace("\\", "/")
    lines = [
        "Renter-Facing Comparable Table",
        "==============================",
        "",
        "This page is auto-generated from open-data assessment proxies.",
        "It is useful for price positioning, but it is not a substitute for MLS sold comparables.",
        "",
        f"- Source dataset(s): ``{dataset_text}``",
        f"- Input rows scanned: ``{row_count}``",
        f"- Rows included in renter table: ``{included_rows}``",
        f"- Generated at: ``{generated_at}``",
        f"- Input file: ``{input_csv_path}``",
        "",
        ".. csv-table:: Assessment Proxy Comparables (Open Data)",
        f"   :file: {csv_path_for_rst}",
        "   :header-rows: 1",
        "",
        "Source details and provenance IDs are listed in :doc:`90_appendix_sources`.",
        "",
    ]
    return "\n".join(lines)


def _write_rst(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate renter-facing comparable table files from "
            "data/open_calgary_assessment_comps.csv."
        )
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    parser.add_argument("--output-rst", default=str(DEFAULT_OUTPUT_RST))
    parser.add_argument("--top-n", type=int, default=12)
    return parser


def main() -> int:
    args = _parser().parse_args()
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)
    output_rst = Path(args.output_rst)
    top_n = max(1, args.top_n)

    try:
        source_rows = _load_rows(input_csv)
        output_rows = _prepare_rows(source_rows, top_n)
        _write_csv(output_csv, output_rows)

        dataset_ids = sorted({row.get("dataset_id", "").strip() for row in source_rows if row.get("dataset_id")})
        rst_content = _build_rst(
            input_csv_path=input_csv,
            output_csv_path=output_csv,
            output_rst_path=output_rst,
            included_rows=len(output_rows),
            dataset_ids=dataset_ids,
            row_count=len(source_rows),
        )
        _write_rst(output_rst, rst_content)

        print(f"Wrote renter table CSV: {output_csv}")
        print(f"Wrote renter table RST: {output_rst}")
        print(f"Rows exported: {len(output_rows)}")
        return 0
    except Exception as exc:  # pragma: no cover - CLI entry
        print(f"ERROR: {exc}")
        if "Missing input file" in str(exc):
            print('Hint: run "poetry run fetch-open-calgary --subject-address \\"<address>\\"" first.')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
