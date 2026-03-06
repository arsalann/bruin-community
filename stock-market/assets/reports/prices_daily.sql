/* @bruin

name: stock_market.prices_daily
type: bq.sql
description: |
  Transforms raw daily stock prices into an analysis-ready table.
  Deduplicates on (ticker, date), adds daily return %, moving averages,
  52-week high/low metrics, and enriches with sector/industry from tickers.
connection: gcp-default

materialization:
  type: table
  strategy: create+replace

depends:
  - stock_market_raw.prices_daily
  - stock_market_raw.tickers

secrets:
  - key: gcp-default
    inject_as: gcp-default

columns:
  - name: ticker
    type: STRING
    description: Stock ticker symbol
    primary_key: true
    nullable: false
  - name: date
    type: DATE
    description: Trading date
    primary_key: true
    nullable: false
  - name: open
    type: FLOAT
    description: Opening price in USD
  - name: high
    type: FLOAT
    description: Intraday high price in USD
  - name: low
    type: FLOAT
    description: Intraday low price in USD
  - name: close
    type: FLOAT
    description: Closing price in USD
  - name: adj_close
    type: FLOAT
    description: Split and dividend adjusted close price in USD
  - name: volume
    type: INTEGER
    description: Number of shares traded
  - name: daily_return_pct
    type: FLOAT
    description: Daily return percentage based on adjusted close
  - name: sma_5
    type: FLOAT
    description: 5-day simple moving average of adjusted close
  - name: sma_20
    type: FLOAT
    description: 20-day simple moving average of adjusted close
  - name: sma_50
    type: FLOAT
    description: 50-day simple moving average of adjusted close
  - name: sma_200
    type: FLOAT
    description: 200-day simple moving average of adjusted close
  - name: high_52w
    type: FLOAT
    description: 52-week (252 trading day) rolling high of adjusted close
  - name: low_52w
    type: FLOAT
    description: 52-week (252 trading day) rolling low of adjusted close
  - name: pct_from_52w_high
    type: FLOAT
    description: Percentage distance from 52-week high (negative = below high)
  - name: day_of_week
    type: INTEGER
    description: Day of week (1=Sunday through 7=Saturday)
  - name: month
    type: INTEGER
    description: Month of year (1-12)
  - name: quarter
    type: INTEGER
    description: Quarter of year (1-4)
  - name: year
    type: INTEGER
    description: Calendar year
  - name: company_name
    type: STRING
    description: Company name from S&P 500 constituents
  - name: sector
    type: STRING
    description: GICS sector classification
  - name: sub_industry
    type: STRING
    description: GICS sub-industry classification

@bruin */

WITH deduped AS (
    SELECT *
    FROM stock_market_raw.prices_daily
    WHERE date IS NOT NULL
      AND close IS NOT NULL
      AND close > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY extracted_at DESC) = 1
),

enriched AS (
    SELECT
        p.ticker,
        p.date,
        p.open,
        p.high,
        p.low,
        p.close,
        p.adj_close,
        CAST(p.volume AS INT64) AS volume,

        ROUND(
            (p.adj_close - LAG(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date))
            / NULLIF(LAG(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date), 0) * 100,
            4
        ) AS daily_return_pct,

        ROUND(AVG(p.adj_close) OVER (
            PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 4) AS sma_5,

        ROUND(AVG(p.adj_close) OVER (
            PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 4) AS sma_20,

        ROUND(AVG(p.adj_close) OVER (
            PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ), 4) AS sma_50,

        ROUND(AVG(p.adj_close) OVER (
            PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ), 4) AS sma_200,

        MAX(p.adj_close) OVER (
            PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS high_52w,

        MIN(p.adj_close) OVER (
            PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS low_52w,

        EXTRACT(DAYOFWEEK FROM p.date) AS day_of_week,
        EXTRACT(MONTH FROM p.date) AS month,
        EXTRACT(QUARTER FROM p.date) AS quarter,
        EXTRACT(YEAR FROM p.date) AS year,

        t.company_name,
        t.sector,
        t.sub_industry

    FROM deduped p
    LEFT JOIN stock_market_raw.tickers t ON p.ticker = t.ticker
)

SELECT
    ticker,
    date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    daily_return_pct,
    sma_5,
    sma_20,
    sma_50,
    sma_200,
    high_52w,
    low_52w,
    ROUND(
        (adj_close - high_52w) / NULLIF(high_52w, 0) * 100,
        2
    ) AS pct_from_52w_high,
    day_of_week,
    month,
    quarter,
    year,
    company_name,
    sector,
    sub_industry
FROM enriched
ORDER BY ticker, date
