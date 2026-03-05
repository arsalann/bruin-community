# stock-market

Tracks daily stock prices and quarterly financial statements for S&P 500 companies using free data sources (Yahoo Finance and Wikipedia).

## Overview

- **Ticker universe**: Current S&P 500 constituents (~500 tickers), sourced from Wikipedia.
- **Daily prices**: Open, high, low, close, adjusted close, and volume via yfinance.
- **Quarterly financials**: Income statements, balance sheets, and cash flow statements via yfinance (last ~4–5 quarters per ticker).
- **Schedule**: Runs daily to capture new price data and any newly available quarterly reports.

## Data Sources


| Source        | Type           | What It Provides                                                                     | Limitation                                           |
| ------------- | -------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| **Wikipedia** | Web scrape     | S&P 500 constituent list with sector, sub-industry, headquarters, CIK, founding year | Current constituents only — no historical membership |
| **yfinance**  | Python library | Daily OHLCV prices (full history), quarterly financials (last ~4–5 quarters)         | Unofficial API, no SLA, may throttle                 |


No paid API key is required. All data is fetched from free, public sources.

## Pipeline Configuration


| Setting            | Value                                  |
| ------------------ | -------------------------------------- |
| Name               | `stock-market`                         |
| Schedule           | `daily`                                |
| Start date         | `2000-01-01`                           |
| Default connection | `google_cloud_platform: "gcp-default"` |
| Asset connection   | `gcp-default` (BigQuery)  |


## Assets

### Raw (Python ingestion)


| Asset                                | File                           | Source    | Strategy | Description                                                                                                                                                                                     |
| ------------------------------------ | ------------------------------ | --------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `stock_market_raw._tickers`           | `raw/raw_tickers.py`           | Wikipedia | `append` | S&P 500 constituent tickers with company name, GICS sector, sub-industry, headquarters, date added, CIK, and founding year                                                                      |
| `stock_market_raw._prices_daily`      | `raw/raw_prices_daily.py`      | yfinance  | `append` | Daily OHLCV + adjusted close for S&P 500 tickers, fetched in batches of 50. Date range controlled by `BRUIN_START_DATE` / `BRUIN_END_DATE`                                                      |
| `stock_market_raw._income_statements` | `raw/raw_income_statements.py` | yfinance  | `append` | Quarterly income statements: revenue, COGS, gross profit, operating expenses, operating income, net income, basic/diluted EPS, EBITDA                                                           |
| `stock_market_raw._balance_sheets`    | `raw/raw_balance_sheets.py`    | yfinance  | `append` | Quarterly balance sheets: total assets, total liabilities, stockholders' equity, retained earnings, current assets/liabilities, cash, debt (current, long-term, total, net), shares outstanding |
| `stock_market_raw._cash_flows`        | `raw/raw_cash_flows.py`        | yfinance  | `append` | Quarterly cash flow statements: operating/investing/financing cash flows, capex, free cash flow, D&A, stock-based compensation, change in working capital                                       |


### Reports (BigQuery SQL transformations)


| Asset                               | File                               | Strategy         | Depends On                                                                     | Description                                                                                                                                                                                       |
| ----------------------------------- | ---------------------------------- | ---------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `stock_market.prices_daily`         | `reports/prices_daily.sql`         | `create+replace` | `raw_prices_daily`, `raw_tickers`                                              | Deduped daily prices with daily return %, 5/20/50/200-day SMAs, 52-week high/low, distance from 52-week high, calendar fields, and sector/industry enrichment                                     |
| `stock_market.financials_quarterly` | `reports/financials_quarterly.sql` | `create+replace` | `raw_income_statements`, `raw_balance_sheets`, `raw_cash_flows`, `raw_tickers` | Joined income + balance sheet + cash flow with derived ratios: gross/operating/net margins, ROE, ROA, debt-to-equity, current ratio, revenue QoQ/YoY growth, EPS QoQ growth, book value per share |


## Processing Details

### Ingestion (`raw/`)

1. **Tickers** (`raw_tickers.py`): Scrapes the S&P 500 Wikipedia page, extracts the constituents table, normalizes ticker symbols (`.` → `-` for Yahoo Finance format), and adds an `extracted_at` timestamp.
2. **Daily prices** (`raw_prices_daily.py`): Fetches the S&P 500 ticker list from Wikipedia, then downloads OHLCV data from yfinance in batches of 50 tickers. The date range is controlled by `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables. An optional `STOCK_TICKER_LIMIT` variable can restrict the number of tickers for testing.
3. **Income statements** (`raw_income_statements.py`): Iterates through each S&P 500 ticker and fetches `quarterly_income_stmt` from yfinance. Column names are converted to snake_case. Processes tickers sequentially with a 1-second pause every 50 tickers to avoid throttling.
4. **Balance sheets** (`raw_balance_sheets.py`): Same pattern as income statements — fetches `quarterly_balance_sheet` per ticker from yfinance.
5. **Cash flows** (`raw_cash_flows.py`): Same pattern — fetches `quarterly_cashflow` per ticker from yfinance.

### Transformation (`reports/`)

1. **Daily prices** (`prices_daily.sql`): Deduplicates raw prices on `(ticker, date)` keeping the latest `extracted_at`. Adds window-function-based metrics: daily return %, simple moving averages (5, 20, 50, 200-day), 52-week rolling high/low, and percentage from 52-week high. Joins with `raw_tickers` for sector and sub-industry.
2. **Quarterly financials** (`financials_quarterly.sql`): Deduplicates each raw financial table on `(ticker, period_ending)`. Joins income statements, balance sheets, and cash flows on `(ticker, period_ending)`. Enriches with company metadata from `raw_tickers`. Computes derived metrics: margin percentages, ROE/ROA (annualized from quarterly), debt-to-equity, current ratio, revenue growth (QoQ and YoY), and EPS growth (QoQ).

## Dependencies

Python packages required by raw assets (`raw/requirements.txt`):

```
pandas
yfinance
lxml
requests
```

## Environment Variables


| Variable             | Description                                                           |
| -------------------- | --------------------------------------------------------------------- |
| `BRUIN_START_DATE`   | Start of price ingestion window (YYYY-MM-DD). Default: `2020-01-01`   |
| `BRUIN_END_DATE`     | End of price ingestion window (YYYY-MM-DD). Default: `2020-01-03`     |
| `STOCK_TICKER_LIMIT` | Optional. Limits the number of tickers processed (useful for testing) |
| `LOG_LEVEL`          | Optional. Logging level (default: `INFO`)                             |


## Running

```bash
# Validate the pipeline
bruin validate stock-market/

# Run the full pipeline
bruin run stock-market/

# Or run individual assets:

# Ingest S&P 500 ticker list
bruin run stock-market/assets/raw/raw_tickers.py

# Ingest daily prices (set date range via env vars)
bruin run --start-date 2020-01-01 --end-date 2026-03-05 stock-market/assets/raw/raw_prices_daily.py

# Ingest quarterly financials
bruin run stock-market/assets/raw/raw_income_statements.py
bruin run stock-market/assets/raw/raw_balance_sheets.py
bruin run stock-market/assets/raw/raw_cash_flows.py

# Build report tables
bruin run stock-market/assets/reports/prices_daily.sql
bruin run stock-market/assets/reports/financials_quarterly.sql
```

## Limitations

- **yfinance** is an unofficial Yahoo Finance API with no SLA — it may throttle or break without notice.
- Quarterly financials from yfinance typically cover only the **last 4–5 quarters**, not full history.
- Ticker universe reflects **current** S&P 500 membership only (survivorship bias — delisted companies are not included).
- Financial statement fields returned by yfinance may vary by company; the pipeline captures all available columns and converts them to snake_case.

