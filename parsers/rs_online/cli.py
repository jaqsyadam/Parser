"""Command-line entrypoint for the RS Online browser parser.

This module is intentionally thin: it owns CLI arguments, checkpoint orchestration,
and incremental saving. The scraping and normalization logic stays in
`rs_online_browser_parser.py`.
"""

import argparse
import time
from typing import Dict, List

from .rs_online_browser_parser import DEFAULT_URLS, RSOnlineParser

def build_parser() -> argparse.ArgumentParser:
    """Create the terminal interface without touching parser internals."""
    parser = argparse.ArgumentParser(
        description="Parses RS Online category pages and exports products above 2000 USD."
    )
    parser.add_argument("urls", nargs="*", help="One or more RS Online category URLs.")
    parser.add_argument(
        "--format",
        choices=["json", "csv", "excel", "all", "print"],
        default="excel",
        help="How to save the result.",
    )
    parser.add_argument("--output", default=RSOnlineParser.DEFAULT_OUTPUT, help="Output filename without extension.")
    parser.add_argument("--template", default=RSOnlineParser.TEMPLATE_FILE, help="Excel template path.")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP/browser timeout in seconds.")
    parser.add_argument("--retries", type=int, default=3, help="How many times to retry failed requests.")
    parser.add_argument("--retry-delay", type=float, default=2.0, help="Delay between retries in seconds.")
    parser.add_argument("--request-delay", type=float, default=1.5, help="Delay between product pages in seconds.")
    parser.add_argument("--max-pages", type=int, default=0, help="How many listing pages to process. 0 means all.")
    parser.add_argument("--cdp-port", type=int, default=9223, help="Remote debugging port for local Chrome/Edge.")
    parser.add_argument("--browser-path", default="", help="Optional full path to chrome.exe or msedge.exe.")
    parser.add_argument("--keep-browser-open", action="store_true", help="Keep the launched browser open after the script ends.")
    parser.add_argument("--debug-html-file", default="rs_online_debug_page.html", help="Where to save the last loaded RS HTML.")
    parser.add_argument("--no-translate", action="store_true", help="Disable Russian translation for text fields.")
    return parser


def main() -> None:
    """Run a resumable RS Online scrape from command-line arguments."""
    args = build_parser().parse_args()
    resolved_debug_html = RSOnlineParser.resolve_local_debug_html_file(args.debug_html_file)
    parser = RSOnlineParser(
        timeout=args.timeout,
        retries=args.retries,
        retry_delay=args.retry_delay,
        request_delay=args.request_delay,
        max_pages=args.max_pages,
        cdp_port=args.cdp_port,
        browser_path=args.browser_path,
        keep_browser_open=args.keep_browser_open,
        debug_html_file=resolved_debug_html,
        translate_to_ru=not args.no_translate,
    )

    urls = args.urls or DEFAULT_URLS
    if not urls:
        print("No RS Online URLs provided.")
        raise SystemExit(1)

    all_results: List[Dict[str, str]] = []
    output_base = RSOnlineParser.resolve_local_output_base(args.output)
    print(f"Output base path: {output_base}")
    checkpoint = RSOnlineParser.load_checkpoint()
    if checkpoint:
        print(f"Resuming from checkpoint: {RSOnlineParser.checkpoint_path()}")

    try:
        for url_index, url in enumerate(urls, start=1):
            if checkpoint and checkpoint.get("root_url") not in (None, url):
                root_url = str(checkpoint.get("root_url", ""))
                if root_url and root_url != url:
                    continue
            print(f"\nStarting URL {url_index}/{len(urls)}")
            try:
                if parser._is_product_url(url):
                    url_results: List[Dict[str, str]] = []
                    item = parser.parse_product_page(url)
                    if item:
                        url_results.append(item)
                        all_results.append(item)
                        parser.save_results([item], output_base, args.template, args.format)
                        print(
                            f"Saved product for URL {url_index}/{len(urls)}: "
                            f"{item.get('part_number', '') or item.get('order_code', '')}"
                        )
                    if not url_results:
                        print(f"No matching products found for URL #{url_index}")
                        continue
                    print(f"Completed URL {url_index}/{len(urls)} with {len(url_results)} products")
                    continue

                checkpoint_category_url = ""
                checkpoint_category_index_value = 0
                if checkpoint and str(checkpoint.get("root_url", "")) == url:
                    checkpoint_category_url = str(checkpoint.get("category_url", "")).strip()
                    checkpoint_category_index_value = int(checkpoint.get("category_index", 0) or 0)

                category_urls = parser.discover_category_urls(url)
                suspicious_single_url = (
                    len(category_urls) == 1
                    and parser._normalize_url(category_urls[0]) == parser._normalize_url(parser._merge_sort_query(url, url))
                )
                if checkpoint_category_url and checkpoint_category_url not in category_urls:
                    category_urls.append(checkpoint_category_url)
                if checkpoint_category_url and suspicious_single_url:
                    category_urls = [checkpoint_category_url]
                    print(f"Resume mode: using checkpoint category only: {checkpoint_category_url}")
                display_category_total = max(len(category_urls), checkpoint_category_index_value or 0)
                print(f"Discovered category URLs for URL {url_index}/{len(urls)}: {len(category_urls)}")

                total_url_results = 0
                resume_checkpoint_active = bool(checkpoint_category_url)
                for category_index, category_url in enumerate(category_urls, start=1):
                    absolute_category_index = (
                        checkpoint_category_index_value + category_index - 1
                        if resume_checkpoint_active and checkpoint_category_index_value
                        else category_index
                    )
                    if checkpoint and resume_checkpoint_active:
                        checkpoint_category_url = str(checkpoint.get("category_url", ""))
                        normalized_checkpoint_category_url = parser._normalize_url(checkpoint_category_url) if checkpoint_category_url else ""
                        normalized_category_url = parser._normalize_url(category_url)
                        checkpoint_category_index = int(checkpoint.get("category_index", 0) or 0)
                        if normalized_checkpoint_category_url and normalized_checkpoint_category_url != normalized_category_url:
                            continue
                        if not checkpoint_category_url and checkpoint_category_index and category_index < checkpoint_category_index:
                            continue
                    print(f"Processing category {absolute_category_index}/{display_category_total}: {category_url}")
                    url_results: List[Dict[str, str]] = []
                    page_buffer: List[Dict[str, str]] = []
                    start_page = 1
                    if checkpoint and parser._normalize_url(str(checkpoint.get("category_url", ""))) == parser._normalize_url(category_url):
                        start_page = int(checkpoint.get("next_page", 1))
                        print(f"Resuming category from page {start_page}")

                    def handle_item(item: Dict[str, str], page_number: int) -> None:
                        url_results.append(item)
                        all_results.append(item)
                        page_buffer.append(item)

                    def handle_page_complete(page_number: int) -> None:
                        if page_buffer:
                            parser.save_results(page_buffer, output_base, args.template, args.format)
                            print(
                                f"Saved {len(page_buffer)} products from page {page_number} "
                                f"for category {absolute_category_index}/{display_category_total}"
                            )
                            page_buffer.clear()
                        RSOnlineParser.save_checkpoint(
                            {
                                "root_url": url,
                                "category_url": category_url,
                                "category_index": absolute_category_index,
                                "next_page": page_number + 1,
                                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                        print(
                            f"Checkpoint saved: category {absolute_category_index}/{display_category_total}, "
                            f"next page {page_number + 1}"
                        )

                    parser.parse_all_pages(
                        category_url,
                        item_callback=handle_item,
                        start_page=start_page,
                        page_callback=handle_page_complete,
                    )
                    resume_checkpoint_active = False
                    if parser.last_stop_reason:
                        print(
                            f"Category stopped early: {parser.last_stop_reason}. "
                            f"Moving to next category/link."
                        )
                    if not url_results:
                        print(f"No matching products found for category: {category_url}")
                        continue
                    total_url_results += len(url_results)
                    checkpoint = {}
                    next_absolute_category_index = absolute_category_index + 1
                    next_category_url = category_urls[category_index] if category_index < len(category_urls) else ""
                    RSOnlineParser.save_checkpoint(
                        {
                            "root_url": url,
                            "category_url": next_category_url,
                            "category_index": next_absolute_category_index,
                            "next_page": 1,
                            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
                    if next_category_url:
                        print(
                            f"Checkpoint saved: next category {next_absolute_category_index}/{display_category_total}, page 1"
                        )
                    else:
                        print(
                            f"Checkpoint saved: next category index {next_absolute_category_index}, page 1. "
                            "Next run will rediscover the full category list."
                        )
            except Exception as exc:
                print(f"Failed to fetch URL #{url_index}: {exc}")
                continue

            if not total_url_results:
                print(f"No matching products found for URL #{url_index}")
                continue

            print(f"Completed URL {url_index}/{len(urls)} with {total_url_results} products")

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

        if args.format == "print":
            for item in all_results:
                print(f"{item.get('price', '')} | MOQ {item.get('moq', '')} | {item.get('product_url', '')}")

        print(f"Parsed products: {len(all_results)}")
        RSOnlineParser.clear_checkpoint()
    finally:
        parser.close()


if __name__ == "__main__":
    main()
