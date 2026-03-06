"""@bruin

name: stock_market_raw.cash_flows
description: |
  Fetches quarterly cash flow statements for S&P 500 constituents via yfinance.
  Uses BRUIN_START_DATE and BRUIN_END_DATE to filter by fiscal period end date.
  Captures all available cash flow fields from Yahoo Finance.

  Data source: Yahoo Finance (via yfinance library)
  Limitation: yfinance typically returns the last 4-5 quarters of data.
connection: gcp-default
tags:
  - financial
  - cash-flow
  - quarterly
  - yfinance
  - sp500

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
  - name: period_ending
    type: DATE
    description: Fiscal quarter end date
    primary_key: true
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
  - name: operating_cash_flow
    type: FLOAT
    description: Net cash from operating activities in USD
  - name: capital_expenditure
    type: FLOAT
    description: Capital expenditure in USD (typically negative)
  - name: free_cash_flow
    type: FLOAT
    description: Free cash flow (operating cash flow + capex) in USD
  - name: investing_cash_flow
    type: FLOAT
    description: Net cash from investing activities in USD
  - name: financing_cash_flow
    type: FLOAT
    description: Net cash from financing activities in USD
  - name: end_cash_position
    type: FLOAT
    description: Cash position at end of period in USD
  - name: depreciation_and_amortization
    type: FLOAT
    description: Depreciation and amortization expense in USD
  - name: stock_based_compensation
    type: FLOAT
    description: Stock-based compensation expense in USD
  - name: change_in_working_capital
    type: FLOAT
    description: Net change in working capital in USD
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched
    checks:
      - name: not_null
  - name: repurchase_of_capital_stock
    type: FLOAT
    description: Cash paid for repurchasing company's own stock in USD (typically negative)
  - name: repayment_of_debt
    type: FLOAT
    description: Cash paid for debt repayment in USD (typically negative)
  - name: issuance_of_debt
    type: FLOAT
    description: Cash received from issuing new debt in USD (typically positive)
  - name: other_cash_adjustment_outside_changein_cash
    type: FLOAT
    description: Other cash adjustments outside of changes in cash in USD
  - name: beginning_cash_position
    type: FLOAT
    description: Cash position at beginning of period in USD
  - name: effect_of_exchange_rate_changes
    type: FLOAT
    description: Impact of foreign exchange rate changes on cash in USD
  - name: changes_in_cash
    type: FLOAT
    description: Net change in cash and cash equivalents in USD
  - name: cash_flow_from_continuing_financing_activities
    type: FLOAT
    description: Cash flow from continuing financing activities in USD
  - name: net_other_financing_charges
    type: FLOAT
    description: Net other financing charges and fees in USD
  - name: proceeds_from_stock_option_exercised
    type: FLOAT
    description: Cash received from stock option exercises in USD
  - name: cash_dividends_paid
    type: FLOAT
    description: Total cash dividends paid to shareholders in USD (typically negative)
  - name: common_stock_dividend_paid
    type: FLOAT
    description: Cash dividends paid on common stock in USD (typically negative)
  - name: net_common_stock_issuance
    type: FLOAT
    description: Net cash from common stock issuance activities in USD
  - name: common_stock_payments
    type: FLOAT
    description: Cash payments related to common stock transactions in USD (typically negative)
  - name: net_issuance_payments_of_debt
    type: FLOAT
    description: Net cash from debt issuance minus debt payments in USD
  - name: net_short_term_debt_issuance
    type: FLOAT
    description: Net cash from short-term debt issuance minus payments in USD
  - name: net_long_term_debt_issuance
    type: FLOAT
    description: Net cash from long-term debt issuance minus payments in USD
  - name: long_term_debt_payments
    type: FLOAT
    description: Cash payments for long-term debt repayment in USD (typically negative)
  - name: long_term_debt_issuance
    type: FLOAT
    description: Cash received from issuing long-term debt in USD (typically positive)
  - name: cash_flow_from_continuing_investing_activities
    type: FLOAT
    description: Cash flow from continuing investing activities in USD
  - name: net_other_investing_changes
    type: FLOAT
    description: Net other investing activity changes in USD
  - name: net_investment_purchase_and_sale
    type: FLOAT
    description: Net cash from investment purchases and sales in USD
  - name: sale_of_investment
    type: FLOAT
    description: Cash received from investment sales in USD (typically positive)
  - name: purchase_of_investment
    type: FLOAT
    description: Cash paid for investment purchases in USD (typically negative)
  - name: net_business_purchase_and_sale
    type: FLOAT
    description: Net cash from business acquisitions and disposals in USD
  - name: sale_of_business
    type: FLOAT
    description: Cash received from business disposals in USD (typically positive)
  - name: net_ppe_purchase_and_sale
    type: FLOAT
    description: Net cash from property, plant & equipment purchases and sales in USD
  - name: sale_of_ppe
    type: FLOAT
    description: Cash received from property, plant & equipment sales in USD (typically positive)
  - name: purchase_of_ppe
    type: FLOAT
    description: Cash paid for property, plant & equipment purchases in USD (typically negative)
  - name: cash_flow_from_continuing_operating_activities
    type: FLOAT
    description: Cash flow from continuing operating activities in USD
  - name: change_in_payables_and_accrued_expense
    type: FLOAT
    description: Change in accounts payable and accrued expenses in USD
  - name: change_in_payable
    type: FLOAT
    description: Change in accounts payable balances in USD
  - name: change_in_account_payable
    type: FLOAT
    description: Change in account payable balances in USD
  - name: change_in_tax_payable
    type: FLOAT
    description: Change in tax payable balances in USD
  - name: change_in_income_tax_payable
    type: FLOAT
    description: Change in income tax payable balances in USD
  - name: change_in_inventory
    type: FLOAT
    description: Change in inventory balances in USD
  - name: change_in_receivables
    type: FLOAT
    description: Change in accounts receivable balances in USD
  - name: changes_in_account_receivables
    type: FLOAT
    description: Change in account receivable balances in USD
  - name: other_non_cash_items
    type: FLOAT
    description: Other non-cash items affecting cash flow in USD
  - name: deferred_tax
    type: FLOAT
    description: Deferred tax expense (benefit) in USD
  - name: deferred_income_tax
    type: FLOAT
    description: Deferred income tax expense (benefit) in USD
  - name: depreciation_amortization_depletion
    type: FLOAT
    description: Depreciation, amortization and depletion expense in USD
  - name: operating_gains_losses
    type: FLOAT
    description: Operating gains and losses in USD
  - name: pension_and_employee_benefit_expense
    type: FLOAT
    description: Pension and employee benefit expenses in USD
  - name: gain_loss_on_sale_of_business
    type: FLOAT
    description: Gain or loss on sale of business units in USD
  - name: net_income_from_continuing_operations
    type: FLOAT
    description: Net income from continuing operations in USD
  - name: purchase_of_business
    type: FLOAT
    description: Cash paid for business acquisitions in USD (typically negative)
  - name: capital_expenditure_reported
    type: FLOAT
    description: Capital expenditure as reported in USD (typically negative)
  - name: change_in_other_working_capital
    type: FLOAT
    description: Change in other working capital components in USD
  - name: asset_impairment_charge
    type: FLOAT
    description: Asset impairment charges in USD
  - name: amortization_cash_flow
    type: FLOAT
    description: Amortization expense affecting cash flow in USD
  - name: amortization_of_intangibles
    type: FLOAT
    description: Amortization of intangible assets in USD
  - name: depreciation
    type: FLOAT
    description: Depreciation expense in USD
  - name: interest_paid_supplemental_data
    type: FLOAT
    description: Interest paid as supplemental cash flow data in USD
  - name: short_term_debt_payments
    type: FLOAT
    description: Cash payments for short-term debt in USD (typically negative)
  - name: short_term_debt_issuance
    type: FLOAT
    description: Cash received from short-term debt issuance in USD (typically positive)
  - name: change_in_prepaid_assets
    type: FLOAT
    description: Change in prepaid assets balances in USD
  - name: provisionand_write_offof_assets
    type: FLOAT
    description: Provisions and write-offs of assets in USD
  - name: issuance_of_capital_stock
    type: FLOAT
    description: Cash received from capital stock issuance in USD (typically positive)
  - name: income_tax_paid_supplemental_data
    type: FLOAT
    description: Income tax paid as supplemental cash flow data in USD
  - name: common_stock_issuance
    type: FLOAT
    description: Cash received from common stock issuance in USD (typically positive)
  - name: change_in_other_current_liabilities
    type: FLOAT
    description: Change in other current liabilities in USD
  - name: change_in_other_current_assets
    type: FLOAT
    description: Change in other current assets in USD
  - name: change_in_accrued_expense
    type: FLOAT
    description: Change in accrued expense balances in USD
  - name: unrealized_gain_loss_on_investment_securities
    type: FLOAT
    description: Unrealized gains or losses on investment securities in USD
  - name: gain_loss_on_sale_of_ppe
    type: FLOAT
    description: Gain or loss on sale of property, plant & equipment in USD
  - name: cash_from_discontinued_investing_activities
    type: FLOAT
    description: Cash flow from discontinued investing activities in USD
  - name: cash_from_discontinued_operating_activities
    type: FLOAT
    description: Cash flow from discontinued operating activities in USD
  - name: other_cash_adjustment_inside_changein_cash
    type: FLOAT
    description: Other cash adjustments inside changes in cash in USD
  - name: net_preferred_stock_issuance
    type: FLOAT
    description: Net cash from preferred stock issuance activities in USD
  - name: preferred_stock_issuance
    type: FLOAT
    description: Cash received from preferred stock issuance in USD (typically positive)
  - name: gain_loss_on_investment_securities
    type: FLOAT
    description: Gain or loss on investment securities in USD
  - name: net_foreign_currency_exchange_gain_loss
    type: FLOAT
    description: Net foreign currency exchange gains or losses in USD
  - name: amortization_of_securities
    type: FLOAT
    description: Amortization of securities premiums or discounts in USD
  - name: earnings_losses_from_equity_investments
    type: FLOAT
    description: Earnings or losses from equity method investments in USD
  - name: preferred_stock_dividend_paid
    type: FLOAT
    description: Cash dividends paid on preferred stock in USD (typically negative)
  - name: dividend_received_cfo
    type: FLOAT
    description: Dividends received in operating cash flow in USD
  - name: net_investment_properties_purchase_and_sale
    type: FLOAT
    description: Net cash from investment property purchases and sales in USD
  - name: sale_of_investment_properties
    type: FLOAT
    description: Cash received from investment property sales in USD (typically positive)
  - name: purchase_of_investment_properties
    type: FLOAT
    description: Cash paid for investment property purchases in USD (typically negative)
  - name: preferred_stock_payments
    type: FLOAT
    description: Cash payments related to preferred stock transactions in USD (typically negative)
  - name: cash_from_discontinued_financing_activities
    type: FLOAT
    description: Cash flow from discontinued financing activities in USD
  - name: cash_flow_from_discontinued_operation
    type: FLOAT
    description: Total cash flow from discontinued operations in USD
  - name: net_intangibles_purchase_and_sale
    type: FLOAT
    description: Net cash from intangible asset purchases and sales in USD
  - name: purchase_of_intangibles
    type: FLOAT
    description: Cash paid for intangible asset purchases in USD (typically negative)
  - name: dividends_received_cfi
    type: FLOAT
    description: Dividends received in investing cash flow in USD
  - name: sale_of_intangibles
    type: FLOAT
    description: Cash received from intangible asset sales in USD (typically positive)
  - name: change_in_interest_payable
    type: FLOAT
    description: Change in interest payable balances in USD
  - name: excess_tax_benefit_from_stock_based_compensation
    type: FLOAT
    description: Excess tax benefit from stock-based compensation in USD
  - name: depletion
    type: FLOAT
    description: Depletion expense (typically for natural resources) in USD
  - name: interest_paid_cff
    type: FLOAT
    description: Interest paid in financing cash flow in USD (typically negative)
  - name: dividend_paid_cfo
    type: FLOAT
    description: Dividends paid in operating cash flow in USD (typically negative)
  - name: cash_flowsfromusedin_operating_activities_direct
    type: FLOAT
    description: Direct method cash flows from operating activities in USD
  - name: classesof_cash_payments
    type: FLOAT
    description: Classes of cash payments in operating activities in USD (typically negative)
  - name: other_cash_paymentsfrom_operating_activities
    type: FLOAT
    description: Other cash payments from operating activities in USD (typically negative)
  - name: paymentson_behalfof_employees
    type: FLOAT
    description: Payments on behalf of employees in USD (typically negative)
  - name: classesof_cash_receiptsfrom_operating_activities
    type: FLOAT
    description: Classes of cash receipts from operating activities in USD (typically positive)
  - name: other_cash_receiptsfrom_operating_activities
    type: FLOAT
    description: Other cash receipts from operating activities in USD (typically positive)
  - name: interest_received_cfi
    type: FLOAT
    description: Interest received in investing cash flow in USD (typically positive)
  - name: taxes_refund_paid
    type: FLOAT
    description: Tax refunds received or taxes paid in USD
  - name: interest_paid_cfo
    type: FLOAT
    description: Interest paid in operating cash flow in USD (typically negative)

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
        "Fetching quarterly cash flows for %d tickers (all available quarters)",
        len(tickers),
    )

    all_frames: list[pd.DataFrame] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            t = yf.Ticker(symbol)
            df = t.quarterly_cashflow

            if df is None or df.empty:
                logger.debug("No cash flow data for %s", symbol)
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
            f"No cash flow data fetched. {failed} failures out of {len(tickers)} tickers."
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
