"""@bruin

name: stock_market_raw.income_statements
description: |
  Fetches quarterly income statements for S&P 500 constituents via yfinance.
  Uses BRUIN_START_DATE and BRUIN_END_DATE to filter by fiscal period end date.
  Captures all available income statement fields from Yahoo Finance.

  Data source: Yahoo Finance (via yfinance library)
  Limitation: yfinance typically returns the last 4-5 quarters of data.
connection: gcp-default

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: gcp-default
    inject_as: gcp-default

tags:
  - financial-data
  - income-statement
  - sp500
  - quarterly
  - yahoo-finance

columns:
  - name: ticker
    type: STRING
    description: Stock ticker symbol
    primary_key: true
    checks:
      - name: not_null
  - name: period_ending
    type: DATE
    description: Fiscal quarter end date
    primary_key: true
    checks:
      - name: not_null
  - name: fiscal_year
    type: INTEGER
    description: Fiscal year derived from period ending date
    checks:
      - name: not_null
  - name: fiscal_quarter
    type: INTEGER
    description: Fiscal quarter (1-4) derived from period ending date
    checks:
      - name: not_null
      - name: min
        value: 1
      - name: max
        value: 4
  - name: total_revenue
    type: FLOAT
    description: Total revenue in USD
  - name: cost_of_revenue
    type: FLOAT
    description: Cost of revenue / cost of goods sold in USD
  - name: gross_profit
    type: FLOAT
    description: Gross profit (revenue minus COGS) in USD
  - name: operating_expense
    type: FLOAT
    description: Total operating expenses in USD
  - name: operating_income
    type: FLOAT
    description: Operating income (EBIT proxy) in USD
  - name: net_income
    type: FLOAT
    description: Net income attributable to common shareholders in USD
  - name: basic_eps
    type: FLOAT
    description: Basic earnings per share in USD
  - name: diluted_eps
    type: FLOAT
    description: Diluted earnings per share in USD
  - name: ebitda
    type: FLOAT
    description: Earnings before interest, taxes, depreciation, and amortization in USD
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched
    checks:
      - name: not_null
  - name: tax_effect_of_unusual_items
    type: FLOAT
    description: Tax impact of unusual or non-recurring items in USD
  - name: tax_rate_for_calcs
    type: FLOAT
    description: Tax rate used for calculations (as decimal, e.g., 0.21 for 21%)
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: normalized_ebitda
    type: FLOAT
    description: EBITDA adjusted for unusual items and normalized for comparison in USD
  - name: total_unusual_items
    type: FLOAT
    description: Total amount of unusual or non-recurring items in USD
  - name: total_unusual_items_excluding_goodwill
    type: FLOAT
    description: Total unusual items excluding goodwill impairments in USD
  - name: net_income_from_continuing_operation_net_minority_interest
    type: FLOAT
    description: Net income from continuing operations after minority interest in USD
  - name: reconciled_depreciation
    type: FLOAT
    description: Reconciled depreciation expense from cash flow and income statements in USD
  - name: reconciled_cost_of_revenue
    type: FLOAT
    description: Reconciled cost of revenue from multiple statement sources in USD
  - name: ebit
    type: FLOAT
    description: Earnings before interest and taxes in USD
  - name: net_interest_income
    type: FLOAT
    description: Net interest income (interest income minus interest expense) in USD
  - name: interest_expense
    type: FLOAT
    description: Total interest expense on debt and borrowings in USD
    checks:
      - name: non_negative
  - name: interest_income
    type: FLOAT
    description: Interest income from investments and deposits in USD
    checks:
      - name: non_negative
  - name: normalized_income
    type: FLOAT
    description: Net income adjusted for unusual items and normalized for comparison in USD
  - name: net_income_from_continuing_and_discontinued_operation
    type: FLOAT
    description: Total net income including both continuing and discontinued operations in USD
  - name: total_expenses
    type: FLOAT
    description: Total operating and non-operating expenses in USD
  - name: total_operating_income_as_reported
    type: FLOAT
    description: Operating income as originally reported by the company in USD
  - name: diluted_average_shares
    type: FLOAT
    description: Weighted average diluted shares outstanding during the period
    checks:
      - name: positive
  - name: basic_average_shares
    type: FLOAT
    description: Weighted average basic shares outstanding during the period
    checks:
      - name: positive
  - name: diluted_ni_availto_com_stockholders
    type: FLOAT
    description: Diluted net income available to common stockholders in USD
  - name: net_income_common_stockholders
    type: FLOAT
    description: Net income available to common stockholders after preferred dividends in USD
  - name: minority_interests
    type: FLOAT
    description: Net income attributable to minority/non-controlling interests in USD
  - name: net_income_including_noncontrolling_interests
    type: FLOAT
    description: Net income including non-controlling (minority) interests in USD
  - name: net_income_discontinuous_operations
    type: FLOAT
    description: Net income from discontinued operations in USD
  - name: net_income_continuous_operations
    type: FLOAT
    description: Net income from continuing operations in USD
  - name: earnings_from_equity_interest_net_of_tax
    type: FLOAT
    description: Earnings from equity investments net of tax effects in USD
  - name: tax_provision
    type: FLOAT
    description: Income tax provision/expense for the period in USD
  - name: pretax_income
    type: FLOAT
    description: Income before income tax provision in USD
  - name: other_income_expense
    type: FLOAT
    description: Other miscellaneous income and expenses in USD
  - name: other_non_operating_income_expenses
    type: FLOAT
    description: Non-operating income and expenses outside core business in USD
  - name: special_income_charges
    type: FLOAT
    description: Special charges and non-recurring income items in USD
  - name: gain_on_sale_of_business
    type: FLOAT
  - name: restructuring_and_mergern_acquisition
    type: FLOAT
  - name: gain_on_sale_of_security
    type: FLOAT
  - name: net_non_operating_interest_income_expense
    type: FLOAT
  - name: interest_expense_non_operating
    type: FLOAT
  - name: interest_income_non_operating
    type: FLOAT
  - name: research_and_development
    type: FLOAT
    description: Research and development expenses in USD
    checks:
      - name: non_negative
  - name: selling_general_and_administration
    type: FLOAT
    description: Combined selling, general and administrative expenses in USD
    checks:
      - name: non_negative
  - name: general_and_administrative_expense
    type: FLOAT
    description: General and administrative expenses in USD
    checks:
      - name: non_negative
  - name: other_gand_a
    type: FLOAT
    description: Other general and administrative expenses not elsewhere classified in USD
  - name: salaries_and_wages
    type: FLOAT
    description: Employee salaries and wages expense in USD
    checks:
      - name: non_negative
  - name: operating_revenue
    type: FLOAT
  - name: write_off
    type: FLOAT
  - name: depreciation_amortization_depletion_income_statement
    type: FLOAT
    description: Combined depreciation, amortization and depletion from income statement in USD
    checks:
      - name: non_negative
  - name: depreciation_and_amortization_in_income_statement
    type: FLOAT
  - name: amortization
    type: FLOAT
  - name: amortization_of_intangibles_income_statement
    type: FLOAT
  - name: otherunder_preferred_stock_dividend
    type: FLOAT
  - name: other_special_charges
    type: FLOAT
  - name: other_operating_expenses
    type: FLOAT
  - name: average_dilution_earnings
    type: FLOAT
  - name: selling_and_marketing_expense
    type: FLOAT
  - name: gain_on_sale_of_ppe
    type: FLOAT
  - name: impairment_of_capital_assets
    type: FLOAT
  - name: provision_for_doubtful_accounts
    type: FLOAT
  - name: loss_adjustment_expense
    type: FLOAT
  - name: net_policyholder_benefits_and_claims
    type: FLOAT
  - name: policyholder_benefits_gross
    type: FLOAT
  - name: policyholder_benefits_ceded
    type: FLOAT
  - name: earnings_from_equity_interest
    type: FLOAT
  - name: total_other_finance_cost
    type: FLOAT
  - name: depreciation_income_statement
    type: FLOAT
  - name: preferred_stock_dividends
    type: FLOAT
  - name: other_taxes
    type: FLOAT
  - name: excise_taxes
    type: FLOAT
  - name: occupancy_and_equipment
    type: FLOAT
  - name: professional_expense_and_contract_services_expense
    type: FLOAT
  - name: other_non_interest_expense
    type: FLOAT
  - name: rent_expense_supplemental
    type: FLOAT
  - name: rent_and_landing_fees
    type: FLOAT
  - name: insurance_and_claims
    type: FLOAT
  - name: net_income_from_tax_loss_carryforward
    type: FLOAT
  - name: net_income_extraordinary
    type: FLOAT
  - name: securities_amortization
    type: FLOAT
  - name: depletion_income_statement
    type: FLOAT

@bruin"""

import io
import logging
import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
import yfinance as yf

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HEADERS = {"User-Agent": "bruin-data-pipeline/1.0 (stock-market)"}


def get_sp500_tickers() -> list[str]:
    resp = requests.get(SP500_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    return tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()


def to_snake_case(name: str) -> str:
    s = str(name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.lower().strip("_")


def materialize():
    tickers = get_sp500_tickers()
    limit = os.environ.get("STOCK_TICKER_LIMIT")
    if limit:
        tickers = tickers[: int(limit)]

    logger.info(
        "Fetching quarterly income statements for %d tickers (all available quarters)",
        len(tickers),
    )

    all_frames: list[pd.DataFrame] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            t = yf.Ticker(symbol)
            df = t.quarterly_income_stmt

            if df is None or df.empty:
                logger.debug("No income statement data for %s", symbol)
                continue

            df_t = df.T
            df_t.index.name = "period_ending"
            df_t = df_t.reset_index()
            df_t["ticker"] = symbol
            df_t.columns = [to_snake_case(c) for c in df_t.columns]
            df_t["period_ending"] = pd.to_datetime(df_t["period_ending"]).dt.date

            if df_t.empty:
                continue

            all_frames.append(df_t)
            success += 1
        except Exception as e:
            logger.warning("Failed for %s: %s", symbol, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d tickers processed", i + 1, len(tickers))
            time.sleep(1)

    if not all_frames:
        raise RuntimeError(
            f"No income statement data fetched. {failed} failures out of {len(tickers)} tickers."
        )

    result = pd.concat(all_frames, ignore_index=True)
    result["fiscal_year"] = pd.to_datetime(result["period_ending"]).dt.year
    result["fiscal_quarter"] = pd.to_datetime(result["period_ending"]).dt.quarter
    result["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Fetched %d rows for %d tickers (%d failed)",
        len(result), success, failed,
    )
    return result
