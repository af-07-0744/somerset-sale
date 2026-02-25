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
2. Validate provenance links: `poetry run python -m codex_sale_docs.provenance`
3. Build docs: `poetry run build`
4. Open `build/html/index.html` and use browser **Save as PDF**

## Evidence Rules
- Every price, DOM, or feature claim in `source/` must cite one or more `source_id` values from `data/source_registry.csv`.
- Every row in `data/comps_clean.csv` must include `source_ids` (semicolon-delimited).
- If a comp has no source artifacts, exclude it from the valuation conclusion.

## Notes
- Existing folders from prior drafts are kept, but the Sphinx workflow above is now the source of truth.
