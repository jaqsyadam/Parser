# Product Parsers

This repository contains product parsers for industrial supplier websites.
The code is organized by site, while root-level scripts remain as compatibility
entrypoints for simple command-line usage.

## Project Layout

```text
.
|-- parsers/
|   |-- rs_online/
|   |   |-- rs_online_browser_parser.py   # core browser implementation
|   |   |-- rs_online_http_parser.py      # core HTTP implementation
|   |   |-- browser_runtime.py            # Chrome/CDP lifecycle mixin
|   |   |-- cli.py                        # browser CLI
|   |   `-- http_cli.py                   # HTTP CLI
|   |-- export_farnell/
|   |   |-- export_farnell_parser.py      # core implementation
|   |   `-- cli.py
|   |-- esd_equipment/
|   |   |-- esd_equipment_parser.py       # core implementation
|   |   `-- cli.py
|   `-- radwell/
|       |-- radwell_parser.py             # core implementation
|       `-- cli.py
|-- tools/
|   `-- clean_rs_online_excel.py
|-- *_parser.py                           # thin compatibility wrappers
|-- clean_rs_online_excel.py              # thin compatibility wrapper
|-- requirements.txt
`-- README.md
```

The parser implementations live under `parsers/<site>/`. Each site has a small
CLI module and a separate core parser module. Root-level scripts only import and
run the CLI `main()` functions so old commands continue to work.

## Parsers

| Entrypoint | CLI | Core implementation | Site | Notes |
| --- | --- | --- | --- | --- |
| `rs_online_parser.py` | `parsers/rs_online/cli.py` | `parsers/rs_online/rs_online_browser_parser.py` + `browser_runtime.py` | RS Online US | Browser-based parser with category discovery, checkpointing, and resilient page reloads. |
| `rs_online_http_parser.py` | `parsers/rs_online/http_cli.py` | `parsers/rs_online/rs_online_http_parser.py` | RS Online US | Requests/cloudscraper version for simpler RS pages. Keep the browser parser as the main option. |
| `export_farnell_parser.py` | `parsers/export_farnell/cli.py` | `parsers/export_farnell/export_farnell_parser.py` | Export Farnell | Browser-based listing parser with category discovery and checkpointing. |
| `esd_equipment_parser.py` | `parsers/esd_equipment/cli.py` | `parsers/esd_equipment/esd_equipment_parser.py` | ESD Equipment | Category parser with local checkpointing and price filtering. |
| `radwell_parser.py` | `parsers/radwell/cli.py` | `parsers/radwell/radwell_parser.py` | Radwell UK | Discovers Radwell listing URLs automatically when no URL is passed. |
| `clean_rs_online_excel.py` | root wrapper | `tools/clean_rs_online_excel.py` | Local Excel utility | Merges RS Online Excel files and separates duplicate or placeholder-image rows. |

## What Is Not Source Code

Runtime data is intentionally ignored by `.gitignore`:

- Excel, CSV, and JSON outputs.
- Debug HTML snapshots.
- Browser profile folders.
- `urls.txt`.
- `data_template.xlsx`.

The parsers can run without committing those files. If `data_template.xlsx` exists, the
scripts use it as an output template. If it is not present, each parser falls back to
its internal header list.

## Install

Use Python 3.10+ if possible. Python 3.11 is known to work with the current scripts.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m playwright install chromium
```

`urllib3<2` is pinned because some scraper dependencies still import older urllib3
modules on clean machines.

## Run

RS Online main parser:

```powershell
python rs_online_parser.py
python rs_online_parser.py "https://us.rs-online.com/products/"
python rs_online_parser.py --max-pages 1 --format print
python rs_online_parser.py --no-translate
```

Equivalent module command:

```powershell
python -m parsers.rs_online.cli --max-pages 1 --format print
```

Export Farnell:

```powershell
python export_farnell_parser.py
python export_farnell_parser.py "https://export.farnell.com/"
python export_farnell_parser.py --max-pages 1 --format print
```

Equivalent module command:

```powershell
python -m parsers.export_farnell.cli --max-pages 1 --format print
```

ESD Equipment:

```powershell
python esd_equipment_parser.py "https://esd.equipment/en/arbeitsplatzsysteme.html"
python esd_equipment_parser.py "https://esd.equipment/en/arbeitsplatzsysteme.html" --max-pages 1
```

Equivalent module command:

```powershell
python -m parsers.esd_equipment.cli "https://esd.equipment/en/arbeitsplatzsysteme.html" --max-pages 1
```

Radwell:

```powershell
python radwell_parser.py
python radwell_parser.py "https://www.radwell.co.uk/Brand?Page=1&..."
```

Equivalent module command:

```powershell
python -m parsers.radwell.cli
```

Clean/merge RS Online Excel files:

```powershell
python clean_rs_online_excel.py --inputs `
  "C:\Users\erasy\AppData\Local\RSOnlineParser\rs_online_results_a.xlsx" `
  "C:\Users\erasy\AppData\Local\RSOnlineParser\rs_online_results_b.xlsx" `
  --cleaned "C:\Users\erasy\AppData\Local\RSOnlineParser\rs_online_results_cleaned.xlsx" `
  --removed "C:\Users\erasy\AppData\Local\RSOnlineParser\rs_online_results_removed.xlsx"
```

## Output Locations

Most long-running parsers write their default output and checkpoints to:

```text
%LOCALAPPDATA%\<ParserName>\
```

Examples:

```text
C:\Users\<user>\AppData\Local\RSOnlineParser\
C:\Users\<user>\AppData\Local\ExportFarnellParser\
C:\Users\<user>\AppData\Local\EsdEquipmentParser\
```

This keeps OneDrive/project folders cleaner and reduces the chance of Excel save
conflicts during long runs.

## Checkpoints

Checkpoint files are JSON files in the parser's local app directory. They let a parser
resume after a network disconnect, reboot, or manual interruption.

RS Online checkpoint example:

```json
{
  "root_url": "https://us.rs-online.com/products/",
  "category_url": "https://us.rs-online.com/motors-motor-controls/?sortBy=price&sortDir=descending&page=1",
  "category_index": 11,
  "next_page": 384,
  "updated_at": "2026-04-21 00:00:00"
}
```

Only clear a checkpoint when you intentionally want a fresh run.

## Business Rules

The current parsers follow the project rules that were used during collection:

- Keep products priced at `2000 USD` or higher.
- Stop a sorted category after five consecutive products below the minimum price.
- Translate product text to Russian when enabled, but do not translate brand names or product codes.
- Convert prices to KZT using script-level fixed rates.
- Leave wholesale/minimum-wholesale columns empty.
- Use quantity `1000` for available products.
- Skip unavailable/discontinued products unless the parser has a site-specific backorder rule.
- Preserve product codes in uppercase.

## Architecture Notes

Each site parser is intentionally isolated. A selector change for Farnell should not
touch RS Online, and an Excel cleaning change should not touch any scraper. Inside
each parser, keep the responsibilities separated:

- Fetching and retry logic.
- Listing/category discovery.
- Row extraction and product normalization.
- Output mapping and file writing.
- Checkpoint persistence.

`cli.py` means "command-line interface". It is the small layer that knows how to
read terminal arguments, choose output paths, and call the parser. The parser modules
should not know about terminal UX unless there is a very practical reason.

Root entrypoint files should stay thin. Do not put parsing logic into them. Put new
site-specific code under `parsers/<site>/`, and put local maintenance utilities under
`tools/`.

Avoid placing generated data or one-off URL lists back into source files. Pass URLs
through CLI arguments or keep temporary lists outside version control.

## Validation

Before handing over changes, run:

```powershell
python -m py_compile `
  rs_online_parser.py `
  rs_online_http_parser.py `
  export_farnell_parser.py `
  esd_equipment_parser.py `
  radwell_parser.py `
  clean_rs_online_excel.py `
  parsers/rs_online/rs_online_browser_parser.py `
  parsers/rs_online/browser_runtime.py `
  parsers/rs_online/rs_online_http_parser.py `
  parsers/rs_online/cli.py `
  parsers/rs_online/http_cli.py `
  parsers/export_farnell/export_farnell_parser.py `
  parsers/export_farnell/cli.py `
  parsers/esd_equipment/esd_equipment_parser.py `
  parsers/esd_equipment/cli.py `
  parsers/radwell/radwell_parser.py `
  parsers/radwell/cli.py `
  tools/clean_rs_online_excel.py
```

For network behavior, test with a small page limit first:

```powershell
python rs_online_parser.py --max-pages 1 --format print
```
