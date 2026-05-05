"""@bruin

name: ops.daily_stock_summary_slack_report
description: |
  Builds and delivers a one-page PDF stock summary for AAPL and MSFT to Slack.
  The report always uses the latest completed trading day before the run date,
  so weekend and market-holiday runs still produce the most recent market close.

depends:
  - stock_market.prices_daily

image: python:3.11

tags:
  - stock-market
  - delivery
  - slack
  - pdf
  - daily-report

@bruin"""

import csv
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from slack_sdk import WebClient

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE = REPO_ROOT / ".bruin.yml"

REPORT_QUERY = """
WITH prior_trading_day AS (
  SELECT MAX(date) AS report_date
  FROM stock_market.prices_daily
  WHERE date < DATE('{run_date}')
    AND ticker IN ('AAPL', 'MSFT')
)
SELECT
  p.date AS report_date,
  p.ticker,
  p.company_name,
  p.open,
  p.high,
  p.low,
  p.close,
  p.volume,
  ROUND(p.daily_return_pct, 2) AS daily_return_pct,
  ROUND(p.sma_5, 2) AS sma_5,
  ROUND(p.sma_20, 2) AS sma_20,
  ROUND(p.pct_from_52w_high, 2) AS pct_from_52w_high,
  CASE WHEN ABS(p.daily_return_pct) >= 5 THEN 'ALERT' ELSE 'OK' END AS status
FROM stock_market.prices_daily p
CROSS JOIN prior_trading_day d
WHERE p.date = d.report_date
  AND p.ticker IN ('AAPL', 'MSFT')
ORDER BY p.ticker
"""

SLACK_QUERY = """
WITH prior_trading_day AS (
  SELECT MAX(date) AS report_date
  FROM stock_market.prices_daily
  WHERE date < DATE('{run_date}')
    AND ticker IN ('AAPL', 'MSFT')
)
SELECT
  p.date AS report_date,
  STRING_AGG(
    FORMAT(
      '%s %s %.2f%% (close %.2f)',
      p.ticker,
      CASE
        WHEN p.daily_return_pct > 0 THEN 'up'
        WHEN p.daily_return_pct < 0 THEN 'down'
        ELSE 'flat'
      END,
      ABS(p.daily_return_pct),
      p.close
    ),
    '; ' ORDER BY p.ticker
  ) AS stock_blurb,
  MAX(CASE WHEN ABS(p.daily_return_pct) >= 5 THEN 1 ELSE 0 END) AS has_alert
FROM stock_market.prices_daily p
CROSS JOIN prior_trading_day d
WHERE p.date = d.report_date
  AND p.ticker IN ('AAPL', 'MSFT')
GROUP BY p.date
"""


def current_run_date() -> str:
    return os.environ.get("BRUIN_EXECUTION_DATE") or datetime.now(timezone.utc).date().isoformat()


def run_bruin_query(query: str, description: str) -> list[dict[str, str]]:
    child_env = {key: value for key, value in os.environ.items() if not key.startswith("BRUIN_")}
    command = [
        "bruin",
        "query",
        "--connection",
        "gcp-default",
        "--config-file",
        str(CONFIG_FILE),
        "--description",
        description,
        "--output",
        "csv",
        "--query",
        query.strip(),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            env=child_env,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() or exc.stdout.strip() or "No error output captured."
        raise RuntimeError(f"bruin query failed for '{description}': {stderr}") from exc
    rows = list(csv.DictReader(completed.stdout.splitlines()))
    return rows


def as_decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"Invalid numeric value: {value!r}") from exc


def format_money(value: str) -> str:
    return f"{as_decimal(value):,.2f}"


def format_pct(value: str) -> str:
    return f"{as_decimal(value):.2f}%"


def format_volume(value: str) -> str:
    return f"{int(Decimal(value)):,}"


def build_executive_summary(report_date: str, rows: list[dict[str, str]], has_alert: bool) -> str:
    if has_alert:
        tickers = ", ".join(row["ticker"] for row in rows if row["status"] == "ALERT")
        return f"{tickers} exceeded the 5% daily move threshold on {report_date}."
    return f"AAPL and MSFT both remained within the 5% alert threshold on {report_date}."


def build_slack_message(report_date: str, stock_blurb: str, rows: list[dict[str, str]], has_alert: bool) -> str:
    if has_alert:
        alert_tickers = [row["ticker"] for row in rows if row["status"] == "ALERT"]
        if len(alert_tickers) == 1:
            alert_line = f"ALERT: {alert_tickers[0]} moved more than 5% on {report_date}."
        else:
            alert_line = f"ALERT: {' and '.join(alert_tickers)} moved more than 5% on {report_date}."
        return f"{alert_line}\n{stock_blurb}"
    return f"Daily Stock Summary for {report_date}: {stock_blurb}"


def render_pdf(report_date: str, rows: list[dict[str, str]], summary: str) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="daily-stock-report-"))
    pdf_path = temp_dir / f"daily-stock-summary-{report_date}.pdf"

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=landscape(letter),
        leftMargin=0.4 * inch,
        rightMargin=0.4 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.35 * inch,
    )

    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    title_style.fontSize = 20
    title_style.leading = 24

    body_style = ParagraphStyle(
        "BodySmall",
        parent=styles["BodyText"],
        fontSize=10,
        leading=13,
    )

    table_rows: list[list[str]] = [[
        "Ticker",
        "Company",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Daily Return %",
        "SMA 5",
        "SMA 20",
        "% From 52W High",
        "Status",
    ]]

    for row in rows:
        table_rows.append([
            row["ticker"],
            row["company_name"],
            format_money(row["open"]),
            format_money(row["high"]),
            format_money(row["low"]),
            format_money(row["close"]),
            format_volume(row["volume"]),
            format_pct(row["daily_return_pct"]),
            format_money(row["sma_5"]),
            format_money(row["sma_20"]),
            format_pct(row["pct_from_52w_high"]),
            row["status"],
        ])

    table = Table(
        table_rows,
        repeatRows=1,
        colWidths=[0.55, 1.6, 0.7, 0.7, 0.7, 0.75, 0.95, 0.95, 0.7, 0.7, 1.1, 0.65],
    )
    table.setStyle(
        TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#17324d")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#eaf0f6")]),
            ("ALIGN", (2, 1), (10, -1), "RIGHT"),
            ("ALIGN", (11, 1), (11, -1), "CENTER"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("ALIGN", (1, 1), (1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 8.5),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#94a3b8")),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ])
    )

    story = [
        Paragraph(f"Daily Stock Summary - {report_date}", title_style),
        Spacer(1, 0.15 * inch),
        Paragraph(summary, body_style),
        Spacer(1, 0.2 * inch),
        table,
    ]
    doc.build(story)
    return pdf_path


def maybe_copy_report(pdf_path: Path) -> None:
    output_dir = os.environ.get("STOCK_REPORT_OUTPUT_DIR")
    if not output_dir:
        return

    destination_dir = Path(output_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(pdf_path, destination_dir / pdf_path.name)
    logger.info("Saved report copy to %s", destination_dir / pdf_path.name)


def upload_to_slack(message: str, pdf_path: Path, report_date: str) -> None:
    dry_run = os.environ.get("STOCK_REPORT_SLACK_DRY_RUN", "").strip() == "1"
    if dry_run:
        logger.info("Dry run enabled; skipping Slack upload.")
        print(message)
        return

    token = os.environ.get("SLACK_BOT_TOKEN")
    channel_id = os.environ.get("SLACK_CHANNEL_ID")
    if not token or not channel_id:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID are required unless STOCK_REPORT_SLACK_DRY_RUN=1.")

    client = WebClient(token=token)
    client.files_upload_v2(
        channel=channel_id,
        file=str(pdf_path),
        filename=pdf_path.name,
        title=f"Daily Stock Summary - {report_date}",
        initial_comment=message,
    )


def ensure_expected_rows(rows: list[dict[str, str]], slack_rows: list[dict[str, str]]) -> tuple[str, str, bool]:
    if len(rows) != 2:
        raise RuntimeError(f"Expected 2 stock rows for AAPL and MSFT, received {len(rows)}.")
    if len(slack_rows) != 1:
        raise RuntimeError(f"Expected 1 Slack summary row, received {len(slack_rows)}.")

    report_date = rows[0]["report_date"]
    if any(row["report_date"] != report_date for row in rows):
        raise RuntimeError("Report rows returned mismatched report dates.")
    if slack_rows[0]["report_date"] != report_date:
        raise RuntimeError("Slack summary row report date does not match detailed rows.")

    has_alert = slack_rows[0]["has_alert"] == "1"
    return report_date, slack_rows[0]["stock_blurb"], has_alert


def main() -> None:
    run_date = current_run_date()
    report_rows = run_bruin_query(
        REPORT_QUERY.format(run_date=run_date),
        "Fetch prior trading day AAPL/MSFT stock summary rows for scheduled PDF report",
    )
    slack_rows = run_bruin_query(
        SLACK_QUERY.format(run_date=run_date),
        "Fetch one-line Slack summary text and alert flag for prior trading day AAPL/MSFT report",
    )

    report_date, stock_blurb, has_alert = ensure_expected_rows(report_rows, slack_rows)
    summary = build_executive_summary(report_date, report_rows, has_alert)
    message = build_slack_message(report_date, stock_blurb, report_rows, has_alert)

    pdf_path = render_pdf(report_date, report_rows, summary)
    maybe_copy_report(pdf_path)
    upload_to_slack(message, pdf_path, report_date)


if __name__ == "__main__":
    main()
