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

## Open Calgary Comparables
- `poetry run fetch-open-calgary --subject-address "3000 Somervale Court SW # 209, Calgary AB T2Y 4J2"` - pulls assessment-based comparables into `data/open_calgary_assessment_comps.csv` and writes evidence artifacts.
- Optional app token: `SOCRATA_APP_TOKEN=... poetry run fetch-open-calgary --subject-address "..."`
- Add rows to `data/comps_raw.csv`: `poetry run fetch-open-calgary --subject-address "..." --append-comps-raw`
- Dry run query preview: `poetry run fetch-open-calgary --subject-address "..." --dry-run`
- Generate renter-facing table page and CSV: `poetry run prepare-renter-comps`
- Important: this command produces assessment proxies from open data, not MLS closed-sale comparables.

## Evidence Rules
- Every price, DOM, or feature claim in `source/` must cite one or more `source_id` values from `data/source_registry.csv`.
- Every row in `data/comps_clean.csv` must include `source_ids` (semicolon-delimited).
- If a comp has no source artifacts, exclude it from the valuation conclusion.

## Notes
- Existing folders from prior drafts are kept, but the Sphinx workflow above is now the source of truth.
