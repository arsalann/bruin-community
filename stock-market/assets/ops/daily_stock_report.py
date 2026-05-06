"""@bruin

name: stock_market_ops.daily_stock_report
type: python
description: |
  Builds a one-page PDF stock summary for AAPL and MSFT using the most recent
  completed trading day before the run date, then posts the summary and PDF to
  Slack.
image: python:3.11
depends:
  - stock_market.prices_daily
secrets:
  - key: gcp-default
    inject_as: GCP_DEFAULT
tags:
  - stock-market
  - reporting
  - slack
  - pdf

@bruin"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


DETAIL_QUERY = """
WITH prior_trading_day AS (
  SELECT MAX(date) AS report_date
  FROM stock_market.prices_daily
  WHERE date < @run_date
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

SUMMARY_QUERY = """
WITH prior_trading_day AS (
  SELECT MAX(date) AS report_date
  FROM stock_market.prices_daily
  WHERE date < @run_date
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
    '; '
    ORDER BY p.ticker
  ) AS stock_blurb,
  MAX(CASE WHEN ABS(p.daily_return_pct) >= 5 THEN 1 ELSE 0 END) AS has_alert
FROM stock_market.prices_daily p
CROSS JOIN prior_trading_day d
WHERE p.date = d.report_date
  AND p.ticker IN ('AAPL', 'MSFT')
GROUP BY p.date
"""


def _get_run_date() -> date:
    execution_date = os.environ.get("BRUIN_EXECUTION_DATE")
    if execution_date:
        return date.fromisoformat(execution_date)
    return datetime.now(UTC).date()


def _load_bq_client() -> bigquery.Client:
    payload_raw = os.environ.get("GCP_DEFAULT")
    if not payload_raw:
        raise RuntimeError("GCP_DEFAULT secret is required for the stock report.")

    payload = json.loads(payload_raw)
    if payload.get("type") == "service_account" and payload.get("client_email"):
        service_account_info = payload
        project_id = payload.get("project_id")
    else:
        service_account_json = (
            payload.get("service_account_json")
            or payload.get("credentials_json")
            or payload.get("service_account")
        )
        if isinstance(service_account_json, str):
            service_account_info = json.loads(service_account_json)
        elif isinstance(service_account_json, dict):
            service_account_info = service_account_json
        else:
            raise RuntimeError(
                "GCP_DEFAULT must contain a BigQuery service account JSON payload."
            )
        project_id = payload.get("project_id") or service_account_info.get("project_id")

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )
    return bigquery.Client(project=project_id, credentials=credentials)


def _run_query(client: bigquery.Client, query: str, run_date: date) -> list[dict[str, Any]]:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date.isoformat())
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [dict(row.items()) for row in rows]


def _format_price(value: float) -> str:
    return f"{value:,.2f}"


def _format_pct(value: float) -> str:
    return f"{value:,.2f}%"


def _format_volume(value: int) -> str:
    return f"{value:,}"


def _build_message(
    detail_rows: list[dict[str, Any]], summary_row: dict[str, Any]
) -> tuple[str, str]:
    report_date = summary_row["report_date"].isoformat()
    alerts = [row for row in detail_rows if row["status"] == "ALERT"]
    if alerts:
        alert_moves = ", ".join(
            f"{row['ticker']} {row['daily_return_pct']:+.2f}%"
            for row in alerts
        )
        lead_line = f"ALERT: {alert_moves} on {report_date}."
    else:
        lead_line = f"Daily Stock Summary for {report_date}."

    message = f"{lead_line}\n{summary_row['stock_blurb']}\nPDF attached."
    return lead_line, message


def _build_executive_summary(
    detail_rows: list[dict[str, Any]], summary_row: dict[str, Any]
) -> str:
    report_date = summary_row["report_date"].isoformat()
    alerts = [row for row in detail_rows if row["status"] == "ALERT"]
    if alerts:
        alert_desc = ", ".join(
            f"{row['ticker']} ({row['daily_return_pct']:+.2f}%)"
            for row in alerts
        )
        return (
            f"For {report_date}, {alert_desc} breached the 5% daily move threshold; "
            "see the table below for the full trading-day summary."
        )

    moves = ", ".join(
        f"{row['ticker']} ({row['daily_return_pct']:+.2f}%)" for row in detail_rows
    )
    return (
        f"For {report_date}, {moves} stayed below the 5% alert threshold while the "
        "latest close, volume, and moving-average context remained in range."
    )


def _render_pdf(
    detail_rows: list[dict[str, Any]], summary_row: dict[str, Any]
) -> Path:
    report_date = summary_row["report_date"].isoformat()
    output_dir = Path("/tmp/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = output_dir / f"daily-stock-summary-{report_date}.pdf"

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=landscape(letter),
        leftMargin=0.45 * inch,
        rightMargin=0.45 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.4 * inch,
    )

    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    title_style.fontName = "Helvetica-Bold"
    title_style.fontSize = 22
    title_style.leading = 26

    summary_style = ParagraphStyle(
        "summary",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10.5,
        leading=14,
        textColor=colors.HexColor("#334155"),
    )

    headers = [
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
    ]

    table_rows = [headers]
    for row in detail_rows:
        table_rows.append(
            [
                row["ticker"],
                row["company_name"],
                _format_price(row["open"]),
                _format_price(row["high"]),
                _format_price(row["low"]),
                _format_price(row["close"]),
                _format_volume(row["volume"]),
                _format_pct(row["daily_return_pct"]),
                _format_price(row["sma_5"]),
                _format_price(row["sma_20"]),
                _format_pct(row["pct_from_52w_high"]),
                row["status"],
            ]
        )

    table = Table(
        table_rows,
        colWidths=[
            0.56 * inch,
            1.55 * inch,
            0.72 * inch,
            0.72 * inch,
            0.72 * inch,
            0.72 * inch,
            1.02 * inch,
            0.95 * inch,
            0.72 * inch,
            0.78 * inch,
            1.02 * inch,
            0.56 * inch,
        ],
        repeatRows=1,
    )

    table_style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8.5),
            ("LEADING", (0, 0), (-1, -1), 10),
            ("ALIGN", (2, 1), (-2, -1), "RIGHT"),
            ("ALIGN", (-1, 1), (-1, -1), "CENTER"),
            ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f8fafc")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]
    )

    for row_index, row in enumerate(detail_rows, start=1):
        if row["status"] == "ALERT":
            table_style.add("BACKGROUND", (-1, row_index), (-1, row_index), colors.HexColor("#fee2e2"))
            table_style.add("TEXTCOLOR", (-1, row_index), (-1, row_index), colors.HexColor("#991b1b"))
            table_style.add("FONTNAME", (-1, row_index), (-1, row_index), "Helvetica-Bold")
        else:
            table_style.add("TEXTCOLOR", (-1, row_index), (-1, row_index), colors.HexColor("#166534"))
            table_style.add("FONTNAME", (-1, row_index), (-1, row_index), "Helvetica-Bold")

    table.setStyle(table_style)

    story = [
        Paragraph(f"Daily Stock Summary - {report_date}", title_style),
        Spacer(1, 0.16 * inch),
        Paragraph(_build_executive_summary(detail_rows, summary_row), summary_style),
        Spacer(1, 0.2 * inch),
        table,
    ]

    doc.build(story)
    return pdf_path


def _get_channel_id() -> str:
    vars_payload = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    return vars_payload.get("slack_channel_id", "").strip() or os.environ.get(
        "SLACK_CHANNEL_ID", ""
    ).strip()


def _post_to_slack(pdf_path: Path, message: str, title: str) -> None:
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    channel_id = _get_channel_id()
    dry_run = os.environ.get("SLACK_DRY_RUN", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }

    if dry_run:
        print(message)
        print(f"PDF generated: {pdf_path.name}")
        return

    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN must be set for non-dry-run executions.")
    if not channel_id:
        raise RuntimeError(
            "slack_channel_id pipeline variable or SLACK_CHANNEL_ID env var is required."
        )

    with pdf_path.open("rb") as handle:
        pdf_bytes = handle.read()

    headers = {"Authorization": f"Bearer {token}"}
    upload_request = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        headers=headers,
        data={"filename": pdf_path.name, "length": str(len(pdf_bytes))},
        timeout=30,
    )
    upload_request.raise_for_status()
    upload_payload = upload_request.json()
    if not upload_payload.get("ok"):
        raise RuntimeError(
            f"Slack upload URL request failed: {upload_payload.get('error', 'unknown_error')}"
        )

    upload_response = requests.post(
        upload_payload["upload_url"],
        files={"file": (pdf_path.name, pdf_bytes, "application/pdf")},
        timeout=60,
    )
    upload_response.raise_for_status()

    complete_request = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        json={
            "files": [{"id": upload_payload["file_id"], "title": title}],
            "channel_id": channel_id,
            "initial_comment": message,
        },
        timeout=30,
    )
    complete_request.raise_for_status()
    complete_payload = complete_request.json()
    if not complete_payload.get("ok"):
        raise RuntimeError(
            f"Slack file share failed: {complete_payload.get('error', 'unknown_error')}"
        )


def main() -> None:
    run_date = _get_run_date()
    client = _load_bq_client()
    detail_rows = _run_query(client, DETAIL_QUERY, run_date)
    summary_rows = _run_query(client, SUMMARY_QUERY, run_date)

    if len(detail_rows) != 2:
        raise RuntimeError(
            f"Expected 2 stock rows for AAPL and MSFT, received {len(detail_rows)}."
        )
    if len(summary_rows) != 1:
        raise RuntimeError(
            f"Expected 1 summary row for AAPL and MSFT, received {len(summary_rows)}."
        )

    summary_row = summary_rows[0]
    _, message = _build_message(detail_rows, summary_row)
    pdf_path = _render_pdf(detail_rows, summary_row)
    _post_to_slack(
        pdf_path,
        message,
        f"Daily Stock Summary - {summary_row['report_date'].isoformat()}",
    )


if __name__ == "__main__":
    main()
