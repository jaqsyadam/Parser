# Radwell Parser

Parses Radwell pages and exports products by these rules:

- opens each product card
- keeps only these conditions:
  `Never Used Radwell Packaging`, `Never Used Original Packaging`, `New Product`
- chooses the cheapest available allowed option
- keeps only products priced at `2000 USD` or higher
- stops processing the current URL early after `5` cards in a row with allowed options priced below `2000 USD`
- writes quantity as `1000`
- translates text fields to Russian except brand/code/manufacturer values
- can use URLs passed in the terminal or URLs hardcoded in the script
- processes all pages for each URL until a page without cards is found
- appends new rows to the existing Excel file instead of overwriting old rows
- removes `DISCONTINUED BY MANUFACTURER` from descriptions and search queries

## Install

```bash
python -m pip install -r requirements.txt
```

## Run

```bash
python radwell_parser.py
python radwell_parser.py "<URL>" --format excel
python radwell_parser.py "<URL1>" "<URL2>" --format excel
python radwell_parser.py "<URL>" --format excel --max-pages 1 --limit-items 5 --request-delay 1.5
python radwell_parser.py "<URL>" --format excel --timeout 60 --retries 5 --retry-delay 3
python radwell_parser.py "<URL>" --format excel --no-translate
```

## Excel mapping

- `Код_товара` <- `SearchItemPartNo`
- `Название_позиции` <- brand + code
- `Поисковые_запросы` <- description without commas
- `Описание` <- description
- `Тип_товара` <- first category before `/`
- `Цена` <- price in KZT without decimals
- `Валюта` <- `KZT`
- `Единица_измерения` <- `шт`
- `Оптовая_цена` <- empty
- `Наличие` <- `+`
- `Количество` <- `1000`
- `Возможность_поставки` <- empty
- `Срок_поставки` <- empty
- `Уникальный_идентификатор` <- `SearchItemId`
- `Идентификатор_товара` <- name

The file structure of `data_template.xlsx` is preserved.

## Translation

The script translates text fields to Russian through `deep-translator`.
Brand and manufacturer values are not translated.
