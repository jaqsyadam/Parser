"""Command-line entrypoint for the ESD Equipment parser."""

import argparse
import time
from typing import Dict, List

import requests

from .esd_equipment_parser import DEFAULT_URLS, EsdEquipmentParser

def build_parser() -> argparse.ArgumentParser:
    """Create CLI options for ESD category parsing."""
    parser = argparse.ArgumentParser(
        description="Parses esd.equipment listing pages and exports filtered products."
    )
    parser.add_argument("urls", nargs="*", help="One or more esd.equipment category URLs.")
    parser.add_argument(
        "--format",
        choices=["json", "csv", "excel", "all"],
        default="excel",
        help="How to save the result.",
    )
    parser.add_argument("--output", default="esd_equipment_results", help="Output filename without extension.")
    parser.add_argument(
        "--template",
        default=EsdEquipmentParser.TEMPLATE_FILE,
        help="Excel template path for strict export structure.",
    )
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--retries", type=int, default=3, help="How many times to retry failed requests.")
    parser.add_argument("--retry-delay", type=float, default=2.0, help="Delay between retries in seconds.")
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
    parser.add_argument("--no-translate", action="store_true", help="Disable Russian translation for text fields.")
    return parser


def main() -> None:
    """Run the ESD parser with checkpoint-aware page saving."""
    args = build_parser().parse_args()
    parser = EsdEquipmentParser(
        timeout=args.timeout,
        retries=args.retries,
        retry_delay=args.retry_delay,
        request_delay=args.request_delay,
        limit_items=args.limit_items,
        translate_to_ru=not args.no_translate,
    )

    urls = args.urls or DEFAULT_URLS
    if not urls:
        print("No ESD URLs provided. Pass listing URLs in the terminal or add them to DEFAULT_URLS.")
        raise SystemExit(1)

    all_results: List[Dict[str, str]] = []
    output_base = EsdEquipmentParser.resolve_local_output_base(args.output)
    print(f"Output base path: {output_base}")
    checkpoint = EsdEquipmentParser.load_checkpoint()
    if checkpoint:
        print(f"Resuming from checkpoint: {EsdEquipmentParser.checkpoint_path()}")

    try:
        for url_index, url in enumerate(urls, start=1):
            if checkpoint:
                checkpoint_root_url = str(checkpoint.get("root_url", "")).strip()
                checkpoint_url_index = int(checkpoint.get("url_index", 0) or 0)
                if checkpoint_root_url and checkpoint_root_url != url:
                    continue
                if not checkpoint_root_url and checkpoint_url_index and url_index < checkpoint_url_index:
                    continue
            print(f"\nStarting URL {url_index}/{len(urls)}")
            url_results: List[Dict[str, str]] = []
            start_page = 1
            if checkpoint and str(checkpoint.get("root_url", "")).strip() == url:
                start_page = int(checkpoint.get("next_page", 1) or 1)
                print(f"Resuming URL from page {start_page}")

            def handle_page_results(page_results: List[Dict[str, str]], page_number: int) -> None:
                if not page_results:
                    return
                url_results.extend(page_results)
                all_results.extend(page_results)
                parser.save_results(page_results, output_base, args.template, args.format)
                EsdEquipmentParser.save_checkpoint(
                    {
                        "root_url": url,
                        "url_index": url_index,
                        "next_page": page_number + 1,
                        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                print(
                    f"Saved page {page_number} for URL {url_index}/{len(urls)} "
                    f"with {len(page_results)} products"
                )
                print(f"Checkpoint saved: URL {url_index}/{len(urls)}, next page {page_number + 1}")

            try:
                parser.parse_all_pages_with_callback(
                    url,
                    max_pages=args.max_pages,
                    page_callback=handle_page_results,
                    start_page=start_page,
                )
            except requests.RequestException as exc:
                print(f"Failed to fetch URL #{url_index}: {exc}")
                continue

            if not url_results:
                print(f"No matching products found for URL #{url_index}")
                continue

            print(f"Completed URL {url_index}/{len(urls)} with {len(url_results)} products")
            checkpoint = {}
            EsdEquipmentParser.save_checkpoint(
                {
                    "root_url": urls[url_index] if url_index < len(urls) else "",
                    "url_index": url_index + 1 if url_index < len(urls) else 0,
                    "next_page": 1,
                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            if url_index < len(urls):
                print(f"Checkpoint saved: next URL {url_index + 1}/{len(urls)}, page 1")
    except KeyboardInterrupt:
        print("\nParsing interrupted by user. Already saved pages remain in the output file.")

    if not all_results:
        print("No matching products found for the provided URLs.")
        raise SystemExit(1)

    if args.format in {"csv", "all"}:
        csv_path = f"{output_base}.csv"
        parser.save_to_csv(all_results, csv_path)
        print(f"Saved CSV: {csv_path}")

    if args.format in {"json", "all"}:
        json_path = f"{output_base}.json"
        parser.save_to_json(all_results, json_path)
        print(f"Saved JSON: {json_path}")

    print(f"Parsed products: {len(all_results)}")
    EsdEquipmentParser.clear_checkpoint()


if __name__ == "__main__":
    main()
