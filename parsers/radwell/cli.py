"""Command-line entrypoint for the Radwell parser."""

import argparse
from pathlib import Path
from typing import Dict, List

import requests

from .radwell_parser import RadwellParser

def build_parser() -> argparse.ArgumentParser:
    """Create CLI options for Radwell discovery and listing parsing."""
    parser = argparse.ArgumentParser(
        description="Parses Radwell pages and exports filtered products."
    )
    parser.add_argument("urls", nargs="*", help="One or more Radwell page URLs.")
    parser.add_argument(
        "--format",
        choices=["json", "csv", "excel", "all", "print"],
        default="excel",
        help="How to return the result.",
    )
    parser.add_argument(
        "--output",
        default="radwell_results",
        help="Output filename without extension.",
    )
    parser.add_argument(
        "--template",
        default=RadwellParser.TEMPLATE_FILE,
        help="Excel template path for strict export structure.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="How many times to retry failed requests.",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=2.0,
        help="Delay between retries in seconds.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="How many listing pages to process per URL. 0 means until no cards are found.",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.0,
        help="Delay between opening product cards in seconds.",
    )
    parser.add_argument(
        "--limit-items",
        type=int,
        default=0,
        help="Limit how many cards to check per page. 0 means all.",
    )
    parser.add_argument(
        "--no-translate",
        action="store_true",
        help="Disable Russian translation for text fields.",
    )
    return parser


def main() -> None:
    """Run Radwell parsing from explicit URLs or automatic discovery."""
    args = build_parser().parse_args()
    parser = RadwellParser(
        timeout=args.timeout,
        retries=args.retries,
        retry_delay=args.retry_delay,
        request_delay=args.request_delay,
        limit_items=args.limit_items,
        translate_to_ru=not args.no_translate,
    )

    if args.urls:
        urls = args.urls
    else:
        urls = parser.discover_listing_urls()
        if not urls:
            print("No URLs were discovered automatically.")
            raise SystemExit(1)

    all_results: List[Dict[str, str]] = []
    output_base = Path(args.output)
    for url_index, url in enumerate(urls, start=1):
        print(f"\nStarting URL {url_index}/{len(urls)}")
        try:
            results = parser.parse_all_pages(url, max_pages=args.max_pages)
        except requests.RequestException as exc:
            print(f"Failed to fetch URL #{url_index}: {exc}")
            continue

        if not results:
            print(f"No matching products found for URL #{url_index}")
            continue

        all_results.extend(results)
        print(f"Completed URL {url_index}/{len(urls)} with {len(results)} products")
        parser.save_results(results, output_base, args.template, args.format)

    if not all_results:
        print("No matching products found for the provided URLs.")
        raise SystemExit(1)

    if args.format in {"print", "all"}:
        parser.print_results(all_results)

    if args.format in {"csv", "all"}:
        csv_path = f"{output_base}.csv"
        parser.save_to_csv(all_results, csv_path)
        print(f"Saved CSV: {csv_path}")

    if args.format in {"json", "all"}:
        json_path = f"{output_base}.json"
        parser.save_to_json(all_results, json_path)
        print(f"Saved JSON: {json_path}")

    print(f"Parsed products: {len(all_results)}")


if __name__ == "__main__":
    main()
