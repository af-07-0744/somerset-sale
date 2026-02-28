import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Any


DEFAULT_START_URL = "https://www.realtor.ca/"
DEFAULT_MAX_MATCHES = 25
DEFAULT_WAIT_AFTER_READY_SECONDS = 2.0
DEFAULT_BROWSER = "chrome"

MATCH_FIELDNAMES = [
    "rank",
    "match_score",
    "address_realtor",
    "url",
    "listing_id",
    "mls_number",
    "price",
    "source",
]

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


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize_address_key(value: str) -> str:
    lowered = _normalize_space(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered)
    return _normalize_space(cleaned)


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


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    if value.is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _extract_listing_id_from_url(url: str) -> str:
    match = re.search(r"/real-estate/(\d+)", url)
    if match:
        return match.group(1)
    return ""


def _extract_candidates_from_payload(payload: dict[str, Any], query_address: str) -> list[dict[str, Any]]:
    rows = payload.get("Results")
    if not isinstance(rows, list):
        return []

    candidates: list[dict[str, Any]] = []
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

        candidates.append(
            {
                "rank": 0,
                "match_score": _score_address_match(query_address, address_realtor),
                "address_realtor": address_realtor,
                "url": url,
                "listing_id": listing_id,
                "mls_number": mls_number,
                "price": _format_float(price_value),
                "source": "network_payload",
            }
        )
    return candidates


def _extract_candidates_from_dom(page: Any, query_address: str) -> list[dict[str, Any]]:
    js = """
    () => {
      const anchors = Array.from(document.querySelectorAll('a[href*="/real-estate/"]')).slice(0, 400);
      return anchors.map((a) => {
        const href = a.getAttribute('href') || '';
        const url = href.startsWith('http') ? href : `https://www.realtor.ca${href}`;
        const card = a.closest('article,li,div');
        const text = (card ? card.innerText : a.innerText) || '';
        return { url, text };
      });
    }
    """
    try:
        rows = page.evaluate(js)
    except Exception:
        return []

    if not isinstance(rows, list):
        return []

    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        url = _normalize_space(str(row.get("url", "")))
        text = str(row.get("text", ""))
        if not url:
            continue

        lines = [_normalize_space(line) for line in text.splitlines() if _normalize_space(line)]
        address_realtor = ""
        for line in lines:
            if re.search(r"\d{2,6}", line) and any(
                token in line.lower() for token in ("st", "ave", "road", "rd", "drive", "dr", "court", "ct")
            ):
                address_realtor = line
                break

        mls_match = re.search(r"MLS\s*(?:®)?\s*(?:Number\s*)?:?\s*([A-Z0-9\-]+)", text, re.IGNORECASE)
        mls_number = mls_match.group(1).strip() if mls_match else ""

        price_match = re.search(r"\$\s*([0-9][0-9,\.]+)", text)
        price = _format_float(_extract_number(price_match.group(1))) if price_match else ""

        listing_id = _extract_listing_id_from_url(url)
        candidates.append(
            {
                "rank": 0,
                "match_score": _score_address_match(query_address, address_realtor),
                "address_realtor": address_realtor,
                "url": url,
                "listing_id": listing_id,
                "mls_number": mls_number,
                "price": price,
                "source": "dom_fallback",
            }
        )
    return candidates


def _dedupe_and_rank(candidates: list[dict[str, Any]], max_matches: int) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in sorted(candidates, key=lambda item: int(item.get("match_score", 0)), reverse=True):
        key = "|".join(
            [
                _normalize_address_key(str(row.get("address_realtor", ""))),
                str(row.get("url", "")).strip(),
                str(row.get("listing_id", "")).strip(),
                str(row.get("mls_number", "")).strip(),
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
        if len(deduped) >= max_matches:
            break

    for index, row in enumerate(deduped, start=1):
        row["rank"] = index
    return deduped


def _write_matches_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MATCH_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in MATCH_FIELDNAMES})


def _auto_fill_query(page: Any, address_query: str) -> bool:
    selectors = [
        "input[placeholder*='Search']",
        "input[type='search']",
        "input[name='searchText']",
        "input[id*='search']",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.click(timeout=1200)
            locator.fill(address_query, timeout=1200)
            locator.press("Enter", timeout=1200)
            return True
        except Exception:
            continue
    return False


def _browser_and_channel(playwright: Any, browser_name: str) -> tuple[Any, str]:
    if browser_name == "chrome":
        return playwright.chromium, "chrome"
    if browser_name == "chromium":
        return playwright.chromium, ""
    if browser_name == "firefox":
        return playwright.firefox, ""
    if browser_name == "webkit":
        return playwright.webkit, ""
    raise RuntimeError(f"Unsupported --browser '{browser_name}'")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Find REALTOR.ca listing candidates via a real browser session. "
            "Intended for authorized QA when direct API calls are blocked."
        )
    )
    parser.add_argument("--address", required=True, help="Address query to search for.")
    parser.add_argument(
        "--browser",
        default=DEFAULT_BROWSER,
        choices=["chrome", "chromium", "firefox", "webkit"],
        help="Browser engine to automate (webkit is Safari-like).",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser headless (not recommended for manual challenge flows).")
    parser.add_argument(
        "--start-url",
        default=DEFAULT_START_URL,
        help="Initial page to open before searching.",
    )
    parser.add_argument("--max-matches", type=int, default=DEFAULT_MAX_MATCHES, help="Max listing matches to return.")
    parser.add_argument(
        "--wait-after-ready-seconds",
        type=float,
        default=DEFAULT_WAIT_AFTER_READY_SECONDS,
        help="Extra wait after manual confirmation before collecting results.",
    )
    parser.add_argument(
        "--no-prompt",
        action="store_true",
        help="Do not wait for Enter key; capture after an automatic delay instead.",
    )
    parser.add_argument(
        "--no-auto-fill",
        action="store_true",
        help="Disable automatic search-box fill attempt.",
    )
    parser.add_argument("--output-json", default="", help="Optional output file path for JSON results.")
    parser.add_argument("--output-csv", default="", help="Optional output file path for CSV results.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON to stdout.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Playwright is not installed. Run `poetry install` then `poetry run playwright install chromium`."
        ) from exc

    captured_payloads: list[dict[str, Any]] = []

    with sync_playwright() as playwright:
        browser_type, channel = _browser_and_channel(playwright, args.browser)
        launch_kwargs: dict[str, Any] = {"headless": args.headless}
        if channel:
            launch_kwargs["channel"] = channel

        browser = browser_type.launch(**launch_kwargs)
        context = browser.new_context(locale="en-CA")
        page = context.new_page()

        def on_response(response: Any) -> None:
            url = response.url
            if "Listing.svc/PropertySearch_Post" not in url:
                return
            status = response.status
            if status != 200:
                return
            try:
                payload = response.json()
            except Exception:
                return
            if not isinstance(payload, dict):
                return
            results = payload.get("Results")
            if not isinstance(results, list) or not results:
                return
            captured_payloads.append(
                {
                    "response_url": url,
                    "result_count": len(results),
                    "captured_at": time.time(),
                    "payload": payload,
                }
            )

        page.on("response", on_response)

        page.goto(args.start_url, wait_until="domcontentloaded", timeout=120000)

        auto_fill_attempted = False
        auto_fill_success = False
        if not args.no_auto_fill:
            auto_fill_attempted = True
            auto_fill_success = _auto_fill_query(page, args.address)

        if args.no_prompt:
            time.sleep(max(args.wait_after_ready_seconds, 0.0))
        else:
            print("Browser is open.")
            if auto_fill_attempted:
                if auto_fill_success:
                    print("Auto-fill attempted. Review results/challenges in the browser window.")
                else:
                    print("Auto-fill could not find a search field. Please perform search manually in the browser.")
            print("When listings are visible, press Enter here to capture candidates.")
            try:
                input()
            except EOFError:
                pass
            if args.wait_after_ready_seconds > 0:
                time.sleep(args.wait_after_ready_seconds)

        if captured_payloads:
            latest = max(captured_payloads, key=lambda item: int(item.get("result_count", 0)))
            candidates = _extract_candidates_from_payload(latest["payload"], query_address=args.address)
            source = "network_payload"
        else:
            candidates = _extract_candidates_from_dom(page, query_address=args.address)
            source = "dom_fallback"

        ranked = _dedupe_and_rank(candidates, max_matches=max(args.max_matches, 1))

        payload = {
            "query_address": args.address,
            "match_count": len(ranked),
            "matches": [{key: row.get(key, "") for key in MATCH_FIELDNAMES} for row in ranked],
            "source": source,
            "captured_search_calls": len(captured_payloads),
            "auto_fill_attempted": auto_fill_attempted,
            "auto_fill_success": auto_fill_success,
            "error": "" if ranked else "No listing candidates captured from browser session.",
        }

        browser.close()

    json_kwargs: dict[str, Any] = {}
    if args.pretty:
        json_kwargs = {"indent": 2, "sort_keys": True}
    print(json.dumps(payload, **json_kwargs))

    if args.output_json:
        output_json_path = Path(args.output_json)
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        with output_json_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")

    if args.output_csv:
        _write_matches_csv(Path(args.output_csv), ranked)

    return 0 if ranked else 3


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
