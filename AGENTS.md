# AGENTS.md

## Querying Data

**For any data analysis, only query the reports tables** in the `stock_market` dataset. Do not query raw tables directly.

### Reports Tables

There are two reports tables available:

#### `stock_market.prices_daily`

Daily stock prices for ~~503 S&P 500 tickers from 2000-01-03 to present (~~2.9M rows).

Key columns:

- **Identity**: `ticker` (PK), `date` (PK)
- **Price**: `open`, `high`, `low`, `close`, `adj_close`
- **Volume**: `volume`
- **Computed metrics**: `daily_return_pct`, `sma_5`, `sma_20`, `sma_50`, `sma_200`, `high_52w`, `low_52w`, `pct_from_52w_high`
- **Calendar**: `day_of_week`, `month`, `quarter`, `year`
- **Enrichment**: `company_name`, `sector` (11 GICS sectors), `sub_industry` (127 sub-industries)

#### `stock_market.financials_quarterly`

Quarterly financial statements for the companies. Joins income statements, balance sheets, and cash flow statements into a single table.

Key columns:

- **Identity**: `ticker` (PK), `period_ending` (PK), `fiscal_year`, `fiscal_quarter`
- **Enrichment**: `company_name`, `sector`, `sub_industry`
- **Income statement**: `total_revenue`, `cost_of_revenue`, `gross_profit`, `operating_expense`, `operating_income`, `net_income`, `basic_eps`, `diluted_eps`, `ebitda`, `interest_expense`, `tax_provision`, `research_and_development`, `selling_general_and_administration`, `diluted_average_shares`
- **Balance sheet**: `total_assets`, `total_liabilities`, `stockholders_equity`, `retained_earnings`, `cash_and_equivalents`, `current_assets`, `current_liabilities`, `current_debt`, `long_term_debt`, `total_debt`, `net_debt`, `goodwill`, `net_tangible_assets`, `inventory`, `accounts_receivable`, `accounts_payable`, `working_capital`, `shares_outstanding`, `book_value_per_share`
- **Cash flow**: `operating_cash_flow`, `capital_expenditure`, `free_cash_flow`, `investing_cash_flow`, `financing_cash_flow`, `depreciation_and_amortization`, `stock_based_compensation`, `change_in_working_capital`, `dividends_paid`, `share_repurchases`
- **Derived ratios**: `gross_margin_pct`, `operating_margin_pct`, `net_margin_pct`, `roe_pct`, `roa_pct`, `debt_to_equity`, `current_ratio`, `revenue_qoq_pct`, `revenue_yoy_pct`, `eps_qoq_pct`

### Joining the Two Tables

The tables share `ticker`, `sector`, `sub_industry`, and `company_name`. Join on `ticker` and align on dates:

- **Exact date join**: `prices_daily.date = financials_quarterly.period_ending` — matches the ~28K rows where a trading day falls on a fiscal quarter end date. Useful for getting the closing price on the exact quarter-end date.
- **Nearest-date join**: For broader analysis, join each financial quarter to a range of daily prices (e.g. all trading days within that quarter) or use a window to find the closest trading day to each `period_ending`.
- **Sector-level analysis**: Both tables have `sector` and `sub_industry` for grouping and aggregation without joining.

### How to Query

Use the `bruin query` CLI command with the `gcp-default` connection:

```bash
bruin query -c gcp-default -q "SELECT ticker, date, close, daily_return_pct FROM stock_market.prices_daily WHERE ticker = 'AAPL' ORDER BY date DESC LIMIT 10"
```

Options:

- `-c gcp-default` — the BigQuery connection (required)
- `-q "SQL"` — the SQL query to run
- `-o plain|json|csv` — output format (default: `plain`)
- `-l N` — limit rows returned
- `--description "reason"` — describe why you ran the query

When using the bruin MCP server, refer to bruin documentation via `bruin_get_docs_tree` and `bruin_get_doc_content` tools for CLI usage details.