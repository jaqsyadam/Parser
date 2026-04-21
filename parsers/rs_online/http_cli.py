"""Command-line entrypoint for the lightweight RS Online HTTP parser."""

import argparse
import json
from pathlib import Path
from typing import Dict, List

from .rs_online_http_parser import DEFAULT_URLS, RSOnlineHTTPParser

def build_parser() -> argparse.ArgumentParser:
    """Create CLI options for the requests/cloudscraper RS parser."""
    parser = argparse.ArgumentParser(
        description="Requests-based RS Online parser without browser automation."
    )
    parser.add_argument("urls", nargs="*", help="One or more RS Online category URLs.")
    parser.add_argument(
        "--format",
        choices=["json", "csv", "excel", "all", "print"],
        default="excel",
        help="How to save the result.",
    )
    parser.add_argument("--output", default=RSOnlineHTTPParser.DEFAULT_OUTPUT, help="Output filename without extension.")
    parser.add_argument("--template", default=RSOnlineHTTPParser.TEMPLATE_FILE, help="Excel template path.")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--retries", type=int, default=3, help="How many times to retry failed requests.")
    parser.add_argument("--retry-delay", type=float, default=2.0, help="Delay between retries in seconds.")
    parser.add_argument("--request-delay", type=float, default=0.5, help="Delay between requests in seconds.")
    parser.add_argument("--max-pages", type=int, default=0, help="How many pages per category to process. 0 means all.")
    parser.add_argument("--debug-html-file", default="rs_online_http_debug_page.html", help="Where to save the last loaded RS HTML.")
    parser.add_argument("--no-translate", action="store_true", help="Disable Russian translation for text fields.")
    return parser


def main() -> None:
    """Run the HTTP parser and write the requested output format."""
    args = build_parser().parse_args()
    parser = RSOnlineHTTPParser(
        timeout=args.timeout,
        retries=args.retries,
        retry_delay=args.retry_delay,
        request_delay=args.request_delay,
        max_pages=args.max_pages,
        translate_to_ru=not args.no_translate,
        debug_html_file=args.debug_html_file,
    )

    urls = args.urls or DEFAULT_URLS
    if not urls:
        print("No RS Online URLs provided.")
        raise SystemExit(1)

    all_results: List[Dict[str, str]] = []
    output_base = Path(args.output)

    for url_index, start_url in enumerate(urls, start=1):
        print(f"\nStarting URL {url_index}/{len(urls)}")
        try:
            category_urls = parser.discover_category_urls(start_url)
        except Exception as exc:
            print(f"Failed to discover category URLs for {start_url}: {exc}")
            continue

        print(f"Discovered category URLs: {len(category_urls)}")
        for category_url in category_urls:
            print(f"Processing category: {category_url}")
            url_results: List[Dict[str, str]] = []

            def handle_item(item: Dict[str, str], page_number: int) -> None:
                url_results.append(item)
                all_results.append(item)
                parser.save_results([item], output_base, args.template, args.format)
                print(
                    f"Saved product from page {page_number}: "
                    f"{item.get('part_number', '') or item.get('order_code', '')}"
                )

            try:
                parser.parse_category(category_url, item_callback=handle_item)
            except Exception as exc:
                print(f"Failed category {category_url}: {exc}")
                continue

            if not url_results:
                print(f"No matching products found for category: {category_url}")

    if not all_results:
        print("No matching products found for the provided URLs.")
        return

    if args.format == "print":
        print(json.dumps(all_results, ensure_ascii=False, indent=2))
    else:
        print(f"Completed with {len(all_results)} products.")


if __name__ == "__main__":
    main()
