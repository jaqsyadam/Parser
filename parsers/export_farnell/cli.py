"""Command-line entrypoint for the Export Farnell parser.

The CLI module keeps terminal concerns away from the site parser: argument parsing,
checkpoint routing, and output decisions live here.
"""

import argparse
import time
from pathlib import Path
from typing import Dict, List

from .export_farnell_parser import DEFAULT_URLS, ExportFarnellParser

def build_parser() -> argparse.ArgumentParser:
    """Create CLI options for live and local-HTML Farnell parsing."""
    parser = argparse.ArgumentParser(
        description="Parses export.farnell.com listing pages and exports products to a separate Excel file."
    )
    parser.add_argument("urls", nargs="*", help="One or more export.farnell.com listing URLs.")
    parser.add_argument(
        "--html-files",
        nargs="*",
        default=[],
        help="One or more locally saved Farnell listing HTML files for offline parsing.",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv", "excel", "all", "print"],
        default="excel",
        help="How to save the result.",
    )
    parser.add_argument(
        "--output",
        default=ExportFarnellParser.DEFAULT_OUTPUT,
        help="Output filename without extension.",
    )
    parser.add_argument(
        "--template",
        default=ExportFarnellParser.TEMPLATE_FILE,
        help="Excel template path for export structure.",
    )
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--retries", type=int, default=3, help="How many times to retry failed requests.")
    parser.add_argument("--retry-delay", type=float, default=2.0, help="Delay between retries in seconds.")
    parser.add_argument("--request-delay", type=float, default=0.0, help="Delay between product pages in seconds.")
    parser.add_argument("--max-pages", type=int, default=0, help="How many pages to process per URL. 0 means all.")
    parser.add_argument("--cdp-port", type=int, default=9222, help="Remote debugging port for local Chrome/Edge.")
    parser.add_argument("--browser-path", default="", help="Optional full path to chrome.exe or msedge.exe.")
    parser.add_argument("--keep-browser-open", action="store_true", help="Keep the launched browser open after the script ends.")
    parser.add_argument("--debug-html-file", default="farnell_debug_page.html", help="Where to save the last loaded Farnell HTML.")
    parser.add_argument("--no-translate", action="store_true", help="Disable Russian translation for text fields.")
    return parser


def main() -> None:
    """Run a Farnell scrape from CLI arguments."""
    args = build_parser().parse_args()
    debug_html_file = ExportFarnellParser.resolve_local_debug_html_file(args.debug_html_file)
    parser = ExportFarnellParser(
        timeout=args.timeout,
        retries=args.retries,
        retry_delay=args.retry_delay,
        request_delay=args.request_delay,
        max_pages=args.max_pages,
        cdp_port=args.cdp_port,
        browser_path=args.browser_path,
        keep_browser_open=args.keep_browser_open,
        debug_html_file=debug_html_file,
        translate_to_ru=not args.no_translate,
    )

    urls = args.urls or DEFAULT_URLS
    if not urls and not args.html_files:
        print("No export.farnell.com URLs provided.")
        raise SystemExit(1)

    all_results: List[Dict[str, str]] = []
    output_base = ExportFarnellParser.resolve_local_output_base(args.output)
    print(f"Output base path: {output_base}")
    checkpoint = ExportFarnellParser.load_checkpoint()
    if checkpoint:
        print(f"Resuming from checkpoint: {ExportFarnellParser.checkpoint_path()}")
    try:
        if args.html_files:
            all_results.extend(parser.parse_local_html_files(args.html_files))
        else:
            for url_index, url in enumerate(urls, start=1):
                if checkpoint and checkpoint.get("root_url") not in (None, url):
                    root_url = str(checkpoint.get("root_url", ""))
                    if root_url and root_url != url:
                        continue
                print(f"\nStarting URL {url_index}/{len(urls)}")
                try:
                    top_category_urls = parser.discover_category_urls(url)
                    print(f"Discovered top-level category URLs for URL {url_index}/{len(urls)}: {len(top_category_urls)}")
                    total_url_results = 0
                    for top_index, top_category_url in enumerate(top_category_urls, start=1):
                        print(f"Processing top-level category {top_index}/{len(top_category_urls)}: {top_category_url}")
                        category_urls = parser.discover_category_urls(top_category_url)
                        if category_urls == [parser._merge_sort_query(top_category_url, top_category_url)]:
                            category_urls = [top_category_url]
                        print(f"Discovered leaf categories for top-level {top_index}/{len(top_category_urls)}: {len(category_urls)}")

                        for category_index, category_url in enumerate(category_urls, start=1):
                            if checkpoint:
                                checkpoint_category_url = str(checkpoint.get("category_url", ""))
                                if checkpoint_category_url and checkpoint_category_url != category_url:
                                    continue

                            print(
                                f"Processing category {category_index}/{len(category_urls)} "
                                f"inside top-level {top_index}/{len(top_category_urls)}: {category_url}"
                            )
                            url_results: List[Dict[str, str]] = []
                            start_page = 1
                            if checkpoint and str(checkpoint.get("category_url", "")) == category_url:
                                start_page = int(checkpoint.get("next_page", 1))
                                print(f"Resuming category from page {start_page}")

                            def handle_item(item: Dict[str, str], page_number: int) -> None:
                                url_results.append(item)
                                all_results.append(item)
                                parser.save_results([item], output_base, args.template, args.format)
                                print(
                                    f"Saved product from page {page_number} for category {category_index}/{len(category_urls)}: "
                                    f"{item.get('part_number', '') or item.get('order_code', '')}"
                                )

                            def handle_page_complete(page_number: int) -> None:
                                ExportFarnellParser.save_checkpoint(
                                    {
                                        "root_url": url,
                                        "category_url": category_url,
                                        "next_page": page_number + 1,
                                        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                    }
                                )

                            parser.parse_all_pages(
                                category_url,
                                item_callback=handle_item,
                                start_page=start_page,
                                page_callback=handle_page_complete,
                            )
                            if parser.last_stop_reason:
                                print(f"Category stopped early: {parser.last_stop_reason}. Moving to next category/link.")
                            if not url_results:
                                print(f"No matching products found for category: {category_url}")
                                continue

                            total_url_results += len(url_results)
                            checkpoint = {}
                            ExportFarnellParser.save_checkpoint(
                                {
                                    "root_url": url,
                                    "category_url": "",
                                    "next_page": 1,
                                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                }
                            )
                except Exception as exc:
                    print(f"Failed to fetch URL #{url_index}: {exc}")
                    continue

                if not total_url_results:
                    print(f"No products found for URL #{url_index}")
                    continue

                print(f"Completed URL {url_index}/{len(urls)} with {total_url_results} products")

        if not all_results:
            if args.html_files:
                print("No products found in the provided local HTML files.")
            else:
                print("No products found for the provided URLs.")
            raise SystemExit(1)

        if args.format in {"excel", "all"}:
            excel_path = f"{output_base}.xlsx"
            if not Path(excel_path).exists():
                parser.save_to_excel(all_results, excel_path, args.template)
                print(f"Saved Excel: {excel_path}")

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
                print(
                    f"{item.get('category', '')} | {item.get('manufacturer', '')} | "
                    f"{item.get('part_number', '')} | MOQ {item.get('moq', '')} | {item.get('product_url', '')}"
                )

        print(f"Parsed products: {len(all_results)}")
        ExportFarnellParser.clear_checkpoint()
    finally:
        parser.close()


if __name__ == "__main__":
    main()
