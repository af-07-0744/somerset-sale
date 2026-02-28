import argparse
import os
from pathlib import Path

from codex_sale_docs.city_data_fetch import (
    DEFAULT_OUTPUT_FIELD_PROFILE_CSV,
    DEFAULT_OUTPUT_FLAT_CSV,
    DEFAULT_OUTPUT_JSON,
    DEFAULT_OUTPUT_META_JSON,
    DEFAULT_STREET_TYPE_ALIASES,
    DEFAULT_SUBJECT_ADDRESS,
    fetch_city_data,
)
from codex_sale_docs.city_data_rst import (
    DEFAULT_INDEX_RST,
    DEFAULT_INDEX_TITLE,
    DEFAULT_NON_RESIDENTIAL_TITLE,
    DEFAULT_OUTPUT_DIR,
    render_city_data_rst,
)
from codex_sale_docs.open_calgary import DEFAULT_DATASET_ID, _normalize_space


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch comprehensive City of Calgary street-level data and generate navigable "
            "rST pages in one command. All fetch/rST parameters are overridable."
        )
    )

    parser.add_argument("--subject-address", default=DEFAULT_SUBJECT_ADDRESS, help="Subject address used for defaults.")
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID, help="Socrata dataset id.")
    parser.add_argument(
        "--app-token",
        default=os.environ.get("SOCRATA_APP_TOKEN", ""),
        help="Socrata app token (defaults to SOCRATA_APP_TOKEN).",
    )
    parser.add_argument("--timeout-seconds", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument("--page-size", type=int, default=1000, help="Pagination page size.")
    parser.add_argument("--max-rows", type=int, default=60000, help="Maximum rows per where-clause request.")
    parser.add_argument("--street-portion", default="", help="Street portion override.")
    parser.add_argument(
        "--street-type-aliases",
        default=DEFAULT_STREET_TYPE_ALIASES,
        help="Comma-delimited street type aliases (default: CO,COURT,CT).",
    )
    parser.add_argument(
        "--include-street-name-only",
        action="store_true",
        help="Also include a broad clause matching just the street name token.",
    )
    parser.add_argument(
        "--extra-where-clause",
        action="append",
        default=[],
        help="Additional raw SoQL $where clause (repeatable).",
    )
    parser.add_argument("--address-field", default="", help="Optional address field override.")
    parser.add_argument("--no-dedupe", action="store_true", help="Disable dedupe in fetch output.")
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON), help="Raw rows JSON output path.")
    parser.add_argument("--output-flat-csv", default=str(DEFAULT_OUTPUT_FLAT_CSV), help="Flat CSV output path.")
    parser.add_argument("--output-meta-json", default=str(DEFAULT_OUTPUT_META_JSON), help="Fetch metadata JSON output path.")
    parser.add_argument(
        "--output-field-profile-csv",
        default=str(DEFAULT_OUTPUT_FIELD_PROFILE_CSV),
        help="Field coverage CSV output path.",
    )

    parser.add_argument("--rst-input-csv", default="", help="Input CSV for rST generation. Defaults to --output-flat-csv.")
    parser.add_argument("--rst-output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for generated rST pages.")
    parser.add_argument("--rst-index-rst", default=str(DEFAULT_INDEX_RST), help="Top-level city-data index rST path.")
    parser.add_argument(
        "--rst-raw-json-path",
        default="",
        help="Raw JSON path shown in the generated index note. Defaults to --output-json.",
    )
    parser.add_argument(
        "--rst-include-multipolygon",
        action="store_true",
        help="Include full multipolygon geometry field in generated tables.",
    )
    parser.add_argument("--rst-index-title", default=DEFAULT_INDEX_TITLE, help="Title for city-data index page.")
    parser.add_argument(
        "--rst-non-residential-title",
        default=DEFAULT_NON_RESIDENTIAL_TITLE,
        help="Title for non-residential / exceptional records page.",
    )

    parser.add_argument("--skip-fetch", action="store_true", help="Skip fetch step and run rST generation only.")
    parser.add_argument("--skip-rst", action="store_true", help="Skip rST generation step and run fetch only.")
    parser.add_argument("--debug", action="store_true", help="Print debug details during fetch.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.skip_fetch and args.skip_rst:
        print("ERROR: --skip-fetch and --skip-rst cannot both be enabled.")
        return 1

    try:
        if not args.skip_fetch:
            fetch_result = fetch_city_data(
                subject_address=_normalize_space(args.subject_address),
                dataset_id=args.dataset_id,
                app_token=args.app_token,
                timeout_seconds=args.timeout_seconds,
                page_size=args.page_size,
                max_rows=args.max_rows,
                street_portion=args.street_portion,
                street_type_aliases_csv=args.street_type_aliases,
                include_street_name_only=bool(args.include_street_name_only),
                extra_where_clauses=list(args.extra_where_clause),
                address_field_override=args.address_field,
                dedupe=not bool(args.no_dedupe),
                output_json=Path(args.output_json),
                output_flat_csv=Path(args.output_flat_csv),
                output_meta_json=Path(args.output_meta_json),
                output_field_profile_csv=Path(args.output_field_profile_csv),
                debug=bool(args.debug),
            )
            print(
                "Fetch complete: "
                f"rows_raw={fetch_result['rows_raw']} rows_deduped={fetch_result['rows_deduped']} "
                f"flat_csv={args.output_flat_csv}"
            )

        if not args.skip_rst:
            rst_input_csv = Path(args.rst_input_csv) if args.rst_input_csv.strip() else Path(args.output_flat_csv)
            rst_raw_json = Path(args.rst_raw_json_path) if args.rst_raw_json_path.strip() else Path(args.output_json)
            rst_result = render_city_data_rst(
                input_csv=rst_input_csv,
                output_dir=Path(args.rst_output_dir),
                index_rst=Path(args.rst_index_rst),
                raw_json_path=rst_raw_json,
                include_multipolygon=bool(args.rst_include_multipolygon),
                index_title=args.rst_index_title,
                non_residential_title=args.rst_non_residential_title,
            )
            print(
                "rST generation complete: "
                f"building_pages={rst_result['building_pages']} index={rst_result['index_rst']}"
            )
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"ERROR: {exc}")
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
