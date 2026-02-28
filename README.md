# Codex Sale Docs (rST + Sphinx + Poetry)

This project now uses a Sphinx-first workflow with reStructuredText (`.rst`) as the canonical documentation format.

## Canonical Structure
- `source/` — Sphinx pages for the pricing memo and evidence appendix.
- `data/` — structured inputs (`property.yml`, comps CSVs, assumptions, source registry).
- `evidence/` — saved artifacts (MLS exports, screenshots, web captures).
- `comms/` — renter-facing text/email drafts.
- `codex_sale_docs/` — Poetry script entry points (`build`, `auto`, `clean`, provenance check).

## Quick Start
1. Install dependencies: `poetry install`
2. Generate local VS Code settings: `poetry run settings`
3. Validate provenance links: `poetry run check-provenance`
4. Build docs: `poetry run build`
5. Open `build/html/index.html` and use browser **Save as PDF**

## Script Commands
- `poetry run settings` - writes `.vscode/settings.json` from `.devcontainer/devcontainer.json` placeholders.
- `poetry run check-provenance` - validates `data/comps_clean.csv` and `data/source_registry.csv`.
- `poetry run build` - builds Sphinx HTML into `build/html`.
- `poetry run esbonio` - builds Sphinx HTML into `build/esbonio/html` for language server previews.
- `poetry run auto` - starts `sphinx-autobuild` with live reload on `127.0.0.1` and an auto-selected port.
- `SPHINX_AUTOBUILD_HOST=0.0.0.0 poetry run auto` - enables container/network binding when needed.
- `SPHINX_AUTOBUILD_REQUIRE_SERVER=1 poetry run auto` - fail instead of fallback if live server bind is unavailable.
- `poetry run clean` - removes `build/` and `dist/` directories.
- `poetry run clean -- build/esbonio` - removes a specific build path.
- `poetry run audit-realtor-accuracy --truth-csv data/truth.csv --urls-csv data/audit_urls.csv` - runs an authorized QA crawl+diff and writes outputs under `data/realtor_accuracy_audit/`.
- `poetry run osm-address-lookup "somervale ct sw, calgary" --mode suggest --countrycodes ca --street-expansion on --email "you@example.com"` - keyless OSM lookup with optional street house-number expansion.
- `poetry run fetch_city_data` - fetches full street-level city rows for Somervale Court SW into raw JSON + flat CSV artifacts.
- `poetry run city_data_to_rst` - converts fetched flat CSV into per-building rST pages + non-residential page + city-data index.
- `poetry run get_city_data` - runs both steps with sensible defaults for `3000 Somervale Court SW #209` (all args overridable).
- `poetry run city_data_enums` - builds an enumeration dictionary CSV and a formatted rST report (wired into docs toctree) using default explain-fields.

## REALTOR.ca Accuracy Audit Template
- Purpose: internal/authorized QA checks for listing-field accuracy against a trusted internal export.
- Input 1 (`--truth-csv`): source-of-truth listing rows (must include an id column such as `listing_id` or `mls_number`).
- Input 2 (`--urls-csv`): URL sample to audit (must include `url`; can optionally include `listing_id` and `complaint_flag`).
- Default outputs:
  - `data/realtor_accuracy_audit/scraped_snapshot.csv`
  - `data/realtor_accuracy_audit/field_diff.csv`
  - `data/realtor_accuracy_audit/summary_metrics.json`
- Example run:
  `poetry run audit-realtor-accuracy --truth-csv data/truth.csv --urls-csv data/audit_urls.csv --delay-seconds 1 --stale-threshold-hours 24`
- The tool computes field mismatch rates (`price`, `beds`, `baths`, `address`, `status`, `geo`), stale listing rate (from `last_updated` lag), and complaint reproduction rate (when `complaint_flag` is supplied).

## REALTOR.ca Single-Listing Extract
- Purpose: address-first extraction for a single listing at a time.
- Browser candidate finder (works through a real browser session when direct API calls are blocked):
  - Install browser runtime once: `poetry run playwright install chromium`
  - Find matching listings by address in Chrome:
    `poetry run find-realtor-listings-browser --address "3000 Somervale Court SW Calgary AB" --browser chrome --pretty`
  - Safari-like option (WebKit engine):
    `poetry run find-realtor-listings-browser --address "3000 Somervale Court SW Calgary AB" --browser webkit --pretty`
  - In the opened browser, complete any prompts/challenges and ensure listing results are visible, then press Enter in terminal.
  - Output includes candidate `address_realtor` + `url` so you can choose the exact listing manually.
- Step 1 (find matches by address and choose exact REALTOR.ca address text):
  `poetry run extract-realtor-listing --address "3000 Somervale Court SW Calgary AB" --list-matches --pretty`
- If geocoding is blocked (403), retry with contact email or manual center:
  - `poetry run extract-realtor-listing --address "3000 Somervale Court SW Calgary AB" --list-matches --geocode-email "you@company.com" --pretty`
  - `poetry run extract-realtor-listing --address "3000 Somervale Court SW Calgary AB" --list-matches --center-lat 51.0447 --center-lon -114.0719 --search-radius-km 12 --pretty`
- Step 2 (extract using the exact address string from step 1):
  `poetry run extract-realtor-listing --address "3000 Somervale Court SW #209, Calgary, Alberta" --pretty`
- URL mode still works:
  `poetry run extract-realtor-listing --url "https://www.realtor.ca/real-estate/12345678/example" --pretty`
- Fields extracted: `sqft`, `bathrooms`, `bedrooms`, `parking_spots`, `storage_units`, `address_realtor`, `maintenance_fee`, `recurring_fees`, `price_per_sqft`.
- Optional outputs:
  - `--output-json data/realtor_single/listing.json`
  - `--output-csv data/realtor_single/listings.csv`

## Open Calgary Comparables
- `poetry run fetch-open-calgary --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2"` - pulls assessment-based comparables into `data/open_calgary_assessment_comps.csv` and writes evidence artifacts.
- Optional app token: `SOCRATA_APP_TOKEN=... poetry run fetch-open-calgary --subject-address "..."`
- Add rows to `data/comps_raw.csv`: `poetry run fetch-open-calgary --subject-address "..." --append-comps-raw`
- Dry run query preview: `poetry run fetch-open-calgary --subject-address "..." --dry-run`
- Include unit in the subject address whenever possible (`# 209`) so subject matching avoids parking/ancillary records.
- Default filters exclude virtual suite tokens ending in `V`, enforce `--match-subject-property-type`, and keep assessments at/above `50000`.
- Use `--exclude-property-types` only when you explicitly want to remove a class (for this condo dataset, `LI` is often the correct class).
- Use `--debug` to inspect query behavior and detected field mappings.
- Generate renter-facing table page and CSV: `poetry run prepare-renter-comps`
- Prefer stronger high-end rows in renter output: `poetry run prepare-renter-comps --prefer-high-end`
- Build a seller-leaning shortlist from inferred unit comps:
  `poetry run prepare-renter-comps --input-csv data/open_calgary_inferred_unit_comps.csv --prefer-high-end --exclude-negative-deltas --min-unit-score 100 --allow-unit-match "same_unit,same_stack+adjacent_floor,same_stack+near_floor" --top-n 12`
- A stretch appendix page is generated by default at `source/03c_renter_comps_stretch_generated.rst` using same-floor positive-delta defaults.
- Tune stretch defaults with `--stretch-top-n`, `--stretch-min-value-delta-pct`, `--stretch-max-value-delta-pct`, `--stretch-min-unit-score`, and `--stretch-allow-unit-match`.
- Additional shortlist controls: `--min-value-delta-pct`, `--max-value-delta-pct`, `--exclude-negative-deltas`, `--min-unit-score`, `--allow-unit-match`.
- Infer unit-level comps across related buildings (defaults include `1000/2000/3000/5500/7000 Somervale Court SW` and `720 Stoney Trail SW`):
  `poetry run infer-open-calgary-units --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2" --debug`
- Recommended focused run (3 target buildings, no default extras, `$where` subject lookup):
  `poetry run infer-open-calgary-units --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2" --subject-search-mode where_only --related-civic-numbers 1000,2000,3000 --no-include-default-extra-buildings --min-assessed-value 150000 --max-comps 80 --debug`
- Focused run including additional known buildings (for example `5500` and `7000`):
  `poetry run infer-open-calgary-units --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2" --subject-search-mode where_only --related-civic-numbers 1000,2000,3000,5500,7000 --no-include-default-extra-buildings --min-assessed-value 150000 --max-comps 80 --debug`
- Scan all civic numbers detected on the subject street:
  `poetry run infer-open-calgary-units --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2" --subject-search-mode where_only --all-street-buildings --no-include-default-extra-buildings --min-assessed-value 150000 --max-per-stack 3 --max-comps 80 --debug`
- The unit inference export now includes additional floor-plan metrics (assessment class, land-use, sub-property-use, assessed components, metric match counts, and plan signatures).
- Floor-plan lists are generated automatically:
  - `data/open_calgary_same_floorplan_units.csv`
  - `data/open_calgary_similar_floorplan_units.csv`
  - `data/open_calgary_floorplan_groups.csv`
- Use `--max-comps 0` to keep all qualifying units (default), or set `--max-comps N` to cap output.
- Use `--max-per-stack N` to diversify the shortlist by capping rows from any one stack (`stack:xx`) after ranking.
- Example diversified shortlist:
  `poetry run infer-open-calgary-units --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2" --subject-search-mode where_only --related-civic-numbers 1000,2000,3000,5500,7000 --no-include-default-extra-buildings --min-assessed-value 150000 --max-per-stack 3 --max-comps 80 --debug`
- Add or override building targets explicitly:
  `poetry run infer-open-calgary-units --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2" --related-civic-numbers "1000,2000,3000,5500,7000" --building-address "720 Stoney Trail SW, Calgary AB T2Y 4M1" --building-address "4500 Somervale Court SW" --debug`
- Important: this command produces assessment proxies from open data, not MLS closed-sale comparables.

## City Data Pages (Per Building)
- One-command default flow:
  `poetry run get_city_data`
- Fetch-only (keeps full raw rows; does not generate pages):
  `poetry run fetch_city_data`
- Convert existing fetched CSV to rST pages:
  `poetry run city_data_to_rst --input-csv data/open_calgary_somervale_raw_rows_flat.csv`
- Generate enumeration/value dictionary from fetched CSV (default explain-fields):
  `poetry run city_data_enums --include-blank`
- Defaults target:
  `land_use_designation, sub_property_use, year_of_construction, land_size_sm, land_size_sf, land_size_ac, comm_code`
- The generated enum report now includes:
  - field-by-field enum tables
  - `Last Run Metadata` (run timestamp, query URLs, file inventory, field provenance)
  - `Explanatory Data Dictionary` at the bottom
  - downloadable file links for source/generated artifacts
- Auto-discover low-cardinality enum fields instead:
  `poetry run city_data_enums --auto-fields --max-distinct 25 --include-blank`
- Override with explicit fields:
  `poetry run city_data_enums --field assessment_class --field property_type --include-blank`
- If fetch metadata JSON is in a non-default location:
  `poetry run city_data_enums --run-meta-json data/open_calgary_somervale_raw_rows_meta.json --include-blank`
- Generated docs:
  - `source/91_city_data_index.rst`
  - `source/city_data/building_*.rst` (one per building)
  - `source/city_data/non_residential_somervale.rst`
- Generated dictionary:
  - `data/open_calgary_street_requested_field_dictionary.csv`
  - `source/92_city_data_enum_dictionary.rst`
  - `source/city_data/_tables/enums/*.csv`
- Useful overrides for `get_city_data`:
  - `--subject-address "<address>"`
  - `--street-portion "<street text>"`
  - `--dataset-id <id>`
  - `--output-flat-csv <path>`
  - `--rst-output-dir <path>`
  - `--rst-index-rst <path>`
  - `--rst-include-multipolygon`

## Evidence Rules
- Every price, DOM, or feature claim in `source/` must cite one or more `source_id` values from `data/source_registry.csv`.
- Every row in `data/comps_clean.csv` must include `source_ids` (semicolon-delimited).
- If a comp has no source artifacts, exclude it from the valuation conclusion.

## Notes
- Existing folders from prior drafts are kept, but the Sphinx workflow above is now the source of truth.
