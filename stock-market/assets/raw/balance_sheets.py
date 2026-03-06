"""@bruin

name: stock_market_raw.balance_sheets
description: |
  Fetches quarterly balance sheets for S&P 500 constituents via yfinance.
  Uses BRUIN_START_DATE and BRUIN_END_DATE to filter by fiscal period end date.
  Captures all available balance sheet fields from Yahoo Finance.

  Data source: Yahoo Finance (via yfinance library)
  Limitation: yfinance typically returns the last 4-5 quarters of data.
connection: gcp-default
tags:
  - financial
  - balance_sheet
  - quarterly
  - s&p_500
  - yahoo_finance
  - raw_data

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: gcp-default
    inject_as: gcp-default

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
      - name: accepted_values
        value:
          - 1
          - 2
          - 3
          - 4
  - name: total_assets
    type: FLOAT
    description: Total assets in USD
    checks:
      - name: positive
  - name: total_liabilities_net_minority_interest
    type: FLOAT
    description: Total liabilities excluding minority interest in USD
    checks:
      - name: positive
  - name: stockholders_equity
    type: FLOAT
    description: Total stockholders equity in USD
  - name: retained_earnings
    type: FLOAT
    description: Retained earnings in USD
  - name: total_current_assets
    type: DOUBLE
    description: Total current assets in USD
  - name: total_current_liabilities
    type: DOUBLE
    description: Total current liabilities in USD
  - name: cash_and_cash_equivalents
    type: FLOAT
    description: Cash and cash equivalents in USD
    checks:
      - name: non_negative
  - name: current_debt
    type: FLOAT
    description: Short-term / current portion of debt in USD
    checks:
      - name: non_negative
  - name: long_term_debt
    type: FLOAT
    description: Long-term debt in USD
    checks:
      - name: non_negative
  - name: total_debt
    type: FLOAT
    description: Total debt (short + long term) in USD
    checks:
      - name: non_negative
  - name: net_debt
    type: FLOAT
    description: Net debt (total debt minus cash) in USD
  - name: ordinary_shares_number
    type: FLOAT
    description: Number of ordinary shares outstanding
    checks:
      - name: positive
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched
    checks:
      - name: not_null
  - name: treasury_shares_number
    type: FLOAT
    description: Number of treasury shares held by the company
    checks:
      - name: non_negative
  - name: share_issued
    type: FLOAT
    description: Total number of shares issued by the company
    checks:
      - name: positive
  - name: tangible_book_value
    type: FLOAT
    description: Book value of tangible assets minus liabilities in USD
  - name: invested_capital
    type: FLOAT
    description: Total capital invested in the business in USD
  - name: working_capital
    type: FLOAT
    description: Current assets minus current liabilities in USD
  - name: net_tangible_assets
    type: FLOAT
    description: Total tangible assets minus total liabilities in USD
  - name: capital_lease_obligations
    type: FLOAT
    description: Total capital lease obligations in USD
    checks:
      - name: non_negative
  - name: common_stock_equity
    type: FLOAT
    description: Total common stockholders equity in USD
  - name: total_capitalization
    type: FLOAT
    description: Total capitalization (debt + equity) in USD
    checks:
      - name: positive
  - name: total_equity_gross_minority_interest
    type: FLOAT
    description: Total equity including minority interest in USD
  - name: minority_interest
    type: FLOAT
    description: Value of minority shareholders' interest in USD
  - name: gains_losses_not_affecting_retained_earnings
    type: FLOAT
    description: Unrealized gains/losses not included in retained earnings in USD
  - name: other_equity_adjustments
    type: FLOAT
    description: Other adjustments to shareholders equity in USD
  - name: treasury_stock
    type: FLOAT
    description: Cost of shares repurchased by the company in USD
    checks:
      - name: non_negative
  - name: additional_paid_in_capital
    type: FLOAT
    description: Amounts paid by shareholders above par value in USD
  - name: capital_stock
    type: FLOAT
    description: Total value of issued capital stock in USD
    checks:
      - name: non_negative
  - name: common_stock
    type: FLOAT
    description: Par value of outstanding common stock in USD
    checks:
      - name: non_negative
  - name: total_non_current_liabilities_net_minority_interest
    type: FLOAT
    description: Total long-term liabilities excluding minority interest in USD
    checks:
      - name: non_negative
  - name: other_non_current_liabilities
    type: FLOAT
    description: Other miscellaneous long-term liabilities in USD
    checks:
      - name: non_negative
  - name: liabilities_heldfor_sale_non_current
    type: FLOAT
    description: Long-term liabilities associated with assets held for sale in USD
    checks:
      - name: non_negative
  - name: employee_benefits
    type: FLOAT
    description: Employee benefit obligations in USD
    checks:
      - name: non_negative
  - name: non_current_pension_and_other_postretirement_benefit_plans
    type: FLOAT
    description: Long-term pension and postretirement benefit obligations in USD
    checks:
      - name: non_negative
  - name: tradeand_other_payables_non_current
    type: FLOAT
    description: Long-term trade and other payables in USD
    checks:
      - name: non_negative
  - name: non_current_deferred_liabilities
    type: FLOAT
    description: Long-term deferred liabilities in USD
    checks:
      - name: non_negative
  - name: non_current_deferred_taxes_liabilities
    type: FLOAT
    description: Long-term deferred tax liabilities in USD
    checks:
      - name: non_negative
  - name: long_term_debt_and_capital_lease_obligation
    type: FLOAT
    description: Total long-term debt plus capital lease obligations in USD
    checks:
      - name: non_negative
  - name: long_term_capital_lease_obligation
    type: FLOAT
    description: Long-term portion of capital lease obligations in USD
    checks:
      - name: non_negative
  - name: current_liabilities
    type: FLOAT
    description: Total current liabilities due within one year in USD
    checks:
      - name: non_negative
  - name: other_current_liabilities
    type: FLOAT
    description: Other miscellaneous current liabilities in USD
  - name: current_deferred_liabilities
    type: FLOAT
    description: Current portion of deferred liabilities in USD
    checks:
      - name: non_negative
  - name: current_deferred_revenue
    type: FLOAT
    description: Revenue received but not yet earned (current portion) in USD
    checks:
      - name: non_negative
  - name: current_debt_and_capital_lease_obligation
    type: FLOAT
    description: Current portion of debt and capital lease obligations in USD
    checks:
      - name: non_negative
  - name: current_capital_lease_obligation
    type: FLOAT
    description: Current portion of capital lease obligations in USD
    checks:
      - name: non_negative
  - name: other_current_borrowings
    type: FLOAT
    description: Other short-term borrowings and credit facilities in USD
  - name: commercial_paper
    type: FLOAT
    description: Short-term unsecured promissory notes in USD
  - name: pensionand_other_post_retirement_benefit_plans_current
    type: FLOAT
    description: Current portion of pension and postretirement obligations in USD
    checks:
      - name: non_negative
  - name: payables_and_accrued_expenses
    type: FLOAT
    description: Total payables and accrued expenses in USD
    checks:
      - name: non_negative
  - name: current_accrued_expenses
    type: FLOAT
    description: Accrued expenses payable within one year in USD
    checks:
      - name: non_negative
  - name: payables
    type: FLOAT
    description: Total amounts owed to suppliers and vendors in USD
    checks:
      - name: non_negative
  - name: total_tax_payable
    type: FLOAT
    description: Total taxes payable to government authorities in USD
    checks:
      - name: non_negative
  - name: income_tax_payable
    type: FLOAT
    description: Income taxes owed to government in USD
    checks:
      - name: non_negative
  - name: accounts_payable
    type: FLOAT
    description: Amounts owed to trade creditors in USD
    checks:
      - name: non_negative
  - name: total_non_current_assets
    type: FLOAT
    description: Total long-term assets in USD
    checks:
      - name: non_negative
  - name: other_non_current_assets
    type: FLOAT
    description: Other miscellaneous long-term assets in USD
    checks:
      - name: non_negative
  - name: defined_pension_benefit
    type: FLOAT
    description: Defined benefit pension plan assets in USD
    checks:
      - name: non_negative
  - name: non_current_deferred_assets
    type: FLOAT
    description: Long-term deferred tax and other assets in USD
    checks:
      - name: non_negative
  - name: non_current_deferred_taxes_assets
    type: FLOAT
    description: Long-term deferred tax assets in USD
    checks:
      - name: non_negative
  - name: non_current_accounts_receivable
    type: FLOAT
    description: Long-term accounts receivable in USD
    checks:
      - name: non_negative
  - name: investments_and_advances
    type: FLOAT
    description: Long-term investments and advances in USD
    checks:
      - name: non_negative
  - name: long_term_equity_investment
    type: FLOAT
    description: Long-term equity investments in other companies in USD
    checks:
      - name: non_negative
  - name: goodwill_and_other_intangible_assets
    type: FLOAT
    description: Total goodwill and intangible assets in USD
    checks:
      - name: non_negative
  - name: other_intangible_assets
    type: FLOAT
    description: Intangible assets excluding goodwill in USD
  - name: goodwill
    type: FLOAT
    description: Goodwill from business acquisitions in USD
    checks:
      - name: non_negative
  - name: net_ppe
    type: FLOAT
    description: Net property, plant and equipment (gross PPE minus accumulated depreciation) in USD
    checks:
      - name: non_negative
  - name: accumulated_depreciation
    type: FLOAT
    description: Total accumulated depreciation on fixed assets in USD (negative value)
  - name: gross_ppe
    type: FLOAT
    description: Gross property, plant and equipment before depreciation in USD
    checks:
      - name: non_negative
  - name: construction_in_progress
    type: FLOAT
    description: Value of construction projects not yet completed in USD
    checks:
      - name: non_negative
  - name: other_properties
    type: FLOAT
    description: Other property assets not categorized elsewhere in USD
  - name: machinery_furniture_equipment
    type: FLOAT
    description: Machinery, furniture and equipment at cost in USD
    checks:
      - name: non_negative
  - name: buildings_and_improvements
    type: FLOAT
    description: Buildings and building improvements at cost in USD
    checks:
      - name: non_negative
  - name: land_and_improvements
    type: FLOAT
    description: Land and land improvements at cost in USD
    checks:
      - name: non_negative
  - name: properties
    type: FLOAT
    description: Real estate properties at cost in USD
    checks:
      - name: non_negative
  - name: current_assets
    type: FLOAT
    description: Total current assets (assets expected to be converted to cash within one year) in USD
    checks:
      - name: non_negative
  - name: other_current_assets
    type: FLOAT
    description: Other miscellaneous current assets in USD
    checks:
      - name: non_negative
  - name: hedging_assets_current
    type: FLOAT
    description: Current hedging instruments and derivative assets in USD
    checks:
      - name: non_negative
  - name: assets_held_for_sale_current
    type: FLOAT
    description: Current assets held for sale or disposal in USD
    checks:
      - name: non_negative
  - name: prepaid_assets
    type: FLOAT
    description: Prepaid expenses and other prepaid assets in USD
    checks:
      - name: non_negative
  - name: inventory
    type: FLOAT
    description: Total inventory including raw materials, WIP, and finished goods in USD
    checks:
      - name: non_negative
  - name: finished_goods
    type: FLOAT
    description: Finished goods inventory ready for sale in USD
    checks:
      - name: non_negative
  - name: work_in_process
    type: FLOAT
    description: Work-in-process inventory in USD
    checks:
      - name: non_negative
  - name: raw_materials
    type: FLOAT
    description: Raw materials inventory in USD
    checks:
      - name: non_negative
  - name: receivables
    type: FLOAT
    description: Total receivables from customers and other parties in USD
    checks:
      - name: non_negative
  - name: other_receivables
    type: FLOAT
    description: Receivables other than trade accounts receivable in USD
    checks:
      - name: non_negative
  - name: accounts_receivable
    type: FLOAT
    description: Net accounts receivable from customers in USD
    checks:
      - name: non_negative
  - name: allowance_for_doubtful_accounts_receivable
    type: FLOAT
    description: Allowance for uncollectible accounts receivable (negative value) in USD
  - name: gross_accounts_receivable
    type: FLOAT
    description: Gross accounts receivable before allowance for doubtful accounts in USD
    checks:
      - name: non_negative
  - name: cash_cash_equivalents_and_short_term_investments
    type: FLOAT
    description: Total cash, cash equivalents and short-term investments in USD
    checks:
      - name: non_negative
  - name: other_short_term_investments
    type: FLOAT
    description: Short-term investments other than cash equivalents in USD
    checks:
      - name: non_negative
  - name: preferred_stock
    type: FLOAT
    description: Par value of outstanding preferred stock in USD
    checks:
      - name: non_negative
  - name: long_term_provisions
    type: FLOAT
    description: Long-term provisions for future liabilities in USD
    checks:
      - name: non_negative
  - name: current_provisions
    type: FLOAT
    description: Current provisions for future liabilities in USD
    checks:
      - name: non_negative
  - name: inventories_adjustments_allowances
    type: FLOAT
    description: Inventory adjustments and allowances (typically negative) in USD
  - name: other_payable
    type: FLOAT
    description: Other miscellaneous payables in USD
    checks:
      - name: non_negative
  - name: dividends_payable
    type: FLOAT
    description: Dividends declared but not yet paid in USD
    checks:
      - name: non_negative
  - name: other_investments
    type: FLOAT
    description: Other investment securities not categorized elsewhere in USD
    checks:
      - name: non_negative
  - name: investmentin_financial_assets
    type: FLOAT
    description: Investments in financial assets and securities in USD
    checks:
      - name: non_negative
  - name: available_for_sale_securities
    type: FLOAT
    description: Securities available for sale at fair value in USD
    checks:
      - name: non_negative
  - name: other_equity_interest
    type: FLOAT
    description: Other equity interests and investments in USD
  - name: non_current_deferred_revenue
    type: FLOAT
    description: Long-term deferred revenue in USD
    checks:
      - name: non_negative
  - name: leases
    type: FLOAT
    description: Lease assets and obligations in USD
    checks:
      - name: non_negative
  - name: cash_equivalents
    type: FLOAT
    description: Highly liquid short-term investments equivalent to cash in USD
    checks:
      - name: non_negative
  - name: cash_financial
    type: FLOAT
    description: Cash and cash equivalents for financial institutions in USD
    checks:
      - name: non_negative
  - name: dueto_related_parties_current
    type: FLOAT
    description: Current amounts owed to related parties in USD
    checks:
      - name: non_negative
  - name: duefrom_related_parties_current
    type: FLOAT
    description: Current amounts owed by related parties in USD
    checks:
      - name: non_negative
  - name: preferred_securities_outside_stock_equity
    type: FLOAT
    description: Preferred securities not included in stockholders equity in USD
    checks:
      - name: non_negative
  - name: interest_payable
    type: FLOAT
    description: Accrued interest payable on debt in USD
    checks:
      - name: non_negative
  - name: non_current_note_receivables
    type: FLOAT
    description: Long-term notes receivable in USD
    checks:
      - name: non_negative
  - name: investments_in_other_ventures_under_equity_method
    type: FLOAT
    description: Equity method investments in joint ventures and associates in USD
    checks:
      - name: non_negative
  - name: investmentsin_associatesat_cost
    type: FLOAT
    description: Cost-method investments in associate companies in USD
    checks:
      - name: non_negative
  - name: restricted_cash
    type: FLOAT
    description: Cash restricted for specific purposes or by agreements in USD
    checks:
      - name: non_negative
  - name: foreign_currency_translation_adjustments
    type: FLOAT
    description: Cumulative foreign currency translation adjustments in USD
  - name: minimum_pension_liabilities
    type: FLOAT
    description: Minimum pension liability adjustments in USD
  - name: unrealized_gain_loss
    type: FLOAT
    description: Unrealized gains or losses on investments in USD
  - name: current_notes_payable
    type: FLOAT
    description: Short-term notes payable in USD
    checks:
      - name: non_negative
  - name: loans_receivable
    type: FLOAT
    description: Loans made to other parties receivable in USD
    checks:
      - name: non_negative
  - name: derivative_product_liabilities
    type: FLOAT
    description: Liabilities from derivative financial instruments in USD
  - name: line_of_credit
    type: FLOAT
    description: Outstanding amounts drawn on lines of credit in USD
  - name: non_current_prepaid_assets
    type: FLOAT
    description: Long-term prepaid assets in USD
    checks:
      - name: non_negative
  - name: financial_assets
    type: FLOAT
    description: Financial assets at fair value in USD
    checks:
      - name: non_negative
  - name: current_deferred_assets
    type: FLOAT
    description: Current portion of deferred tax and other assets in USD
    checks:
      - name: non_negative
  - name: taxes_receivable
    type: FLOAT
    description: Tax refunds and overpayments receivable in USD
    checks:
      - name: non_negative
  - name: receivables_adjustments_allowances
    type: FLOAT
    description: Adjustments and allowances against receivables (typically negative) in USD
  - name: preferred_stock_equity
    type: FLOAT
    description: Preferred stockholders equity in USD
    checks:
      - name: non_negative
  - name: investmentsin_joint_venturesat_cost
    type: FLOAT
    description: Cost-method investments in joint ventures in USD
    checks:
      - name: non_negative
  - name: investment_properties
    type: FLOAT
    description: Real estate held for investment purposes in USD
    checks:
      - name: non_negative
  - name: notes_receivable
    type: FLOAT
    description: Promissory notes receivable from third parties in USD
    checks:
      - name: non_negative
  - name: preferred_shares_number
    type: FLOAT
    description: Number of preferred shares outstanding
    checks:
      - name: non_negative
  - name: other_inventories
    type: FLOAT
    description: Inventory not categorized elsewhere in USD
  - name: cash_cash_equivalents_and_federal_funds_sold
    type: FLOAT
    description: Cash, cash equivalents and federal funds sold (banking) in USD
    checks:
      - name: non_negative
  - name: non_current_accrued_expenses
    type: FLOAT
    description: Long-term accrued expenses in USD
    checks:
      - name: non_negative
  - name: held_to_maturity_securities
    type: FLOAT
    description: Securities held to maturity at amortized cost in USD
    checks:
      - name: non_negative
  - name: trading_securities
    type: FLOAT
    description: Securities held for trading at fair value in USD
    checks:
      - name: non_negative
  - name: financial_assets_designatedas_fair_value_through_profitor_loss_total
    type: FLOAT
    description: Financial assets designated at fair value through profit or loss in USD
    checks:
      - name: non_negative
  - name: dueto_related_parties_non_current
    type: FLOAT
    description: Long-term amounts owed to related parties in USD
    checks:
      - name: non_negative
  - name: investmentsin_subsidiariesat_cost
    type: FLOAT
    description: Cost-method investments in subsidiary companies in USD
    checks:
      - name: non_negative
  - name: current_deferred_taxes_assets
    type: FLOAT
    description: Current portion of deferred tax assets in USD
    checks:
      - name: non_negative
  - name: accrued_interest_receivable
    type: FLOAT
    description: Interest earned but not yet received in USD
    checks:
      - name: non_negative
  - name: duefrom_related_parties_non_current
    type: FLOAT
    description: Long-term amounts owed by related parties in USD
    checks:
      - name: non_negative

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
        "Fetching quarterly balance sheets for %d tickers (all available quarters)",
        len(tickers),
    )

    all_frames: list[pd.DataFrame] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            t = yf.Ticker(symbol)
            df = t.quarterly_balance_sheet

            if df is None or df.empty:
                logger.debug("No balance sheet data for %s", symbol)
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
            f"No balance sheet data fetched. {failed} failures out of {len(tickers)} tickers."
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
