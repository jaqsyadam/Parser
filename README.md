# Product Parsers

This repository contains product parsers for industrial supplier websites.
The code is organized by site, and each parser is launched as a Python module.

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
|-- requirements.txt
`-- README.md
```

The parser implementations live under `parsers/<site>/`. Each site has a small
CLI module and a separate core parser module.

## Parsers

| Command module | Core implementation | Site | Notes |
| --- | --- | --- | --- |
| `parsers.rs_online.cli` | `parsers/rs_online/rs_online_browser_parser.py` + `browser_runtime.py` | RS Online US | Browser-based parser with category discovery, checkpointing, and resilient page reloads. |
| `parsers.rs_online.http_cli` | `parsers/rs_online/rs_online_http_parser.py` | RS Online US | Requests/cloudscraper version for simpler RS pages. Keep the browser parser as the main option. |
| `parsers.export_farnell.cli` | `parsers/export_farnell/export_farnell_parser.py` | Export Farnell | Browser-based listing parser with category discovery and checkpointing. |
| `parsers.esd_equipment.cli` | `parsers/esd_equipment/esd_equipment_parser.py` | ESD Equipment | Category parser with local checkpointing and price filtering. |
| `parsers.radwell.cli` | `parsers/radwell/radwell_parser.py` | Radwell UK | Discovers Radwell listing URLs automatically when no URL is passed. |
| `tools.clean_rs_online_excel` | `tools/clean_rs_online_excel.py` | Local Excel utility | Merges RS Online Excel files and separates duplicate or placeholder-image rows. |

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
python -m parsers.rs_online.cli
python -m parsers.rs_online.cli "https://us.rs-online.com/products/"
python -m parsers.rs_online.cli --max-pages 1 --format print
python -m parsers.rs_online.cli --no-translate
```

Export Farnell:

```powershell
python -m parsers.export_farnell.cli
python -m parsers.export_farnell.cli "https://export.farnell.com/"
python -m parsers.export_farnell.cli --max-pages 1 --format print
```

ESD Equipment:

```powershell
python -m parsers.esd_equipment.cli "https://esd.equipment/en/arbeitsplatzsysteme.html"
python -m parsers.esd_equipment.cli "https://esd.equipment/en/arbeitsplatzsysteme.html" --max-pages 1
```

Radwell:

```powershell
python -m parsers.radwell.cli
python -m parsers.radwell.cli "https://www.radwell.co.uk/Brand?Page=1&..."
```

Clean/merge RS Online Excel files:

```powershell
python -m tools.clean_rs_online_excel --inputs `
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

Put new site-specific code under `parsers/<site>/`, and put local maintenance
utilities under `tools/`.

Avoid placing generated data or one-off URL lists back into source files. Pass URLs
through CLI arguments or keep temporary lists outside version control.

## Validation

Before handing over changes, run:

```powershell
python -m py_compile `
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
python -m parsers.rs_online.cli --max-pages 1 --format print
```
