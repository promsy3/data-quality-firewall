"""
report_gen.py — Generates a per-scan PDF summary using ReportLab Platypus.
"""

from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


# ── Palette ───────────────────────────────────────────────────────────────────
GREEN  = colors.HexColor("#27ae60")
RED    = colors.HexColor("#e74c3c")
ORANGE = colors.HexColor("#e67e22")
BLUE   = colors.HexColor("#2980b9")
DARK   = colors.HexColor("#2c3e50")
LIGHT  = colors.HexColor("#ecf0f1")


def generate_pdf_report(result: dict, output_path: str) -> str:
    """Build the PDF at *output_path* and return the path."""

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    styles = getSampleStyleSheet()
    story  = []

    # ── Styles ────────────────────────────────────────────────────────────────
    title_style = ParagraphStyle(
        "FirewallTitle",
        parent=styles["Title"],
        fontSize=20,
        textColor=DARK,
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        "Subtitle",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.grey,
        spaceAfter=12,
    )
    heading_style = ParagraphStyle(
        "SectionHead",
        parent=styles["Heading2"],
        fontSize=13,
        textColor=BLUE,
        spaceBefore=14,
        spaceAfter=4,
    )
    body_style  = styles["Normal"]
    issue_style = ParagraphStyle(
        "Issue",
        parent=styles["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#7f8c8d"),
        leftIndent=10,
        spaceAfter=2,
    )

    # ── Header ────────────────────────────────────────────────────────────────
    story.append(Paragraph("🔥 Data Quality Firewall", title_style))
    story.append(Paragraph("Automated Scan Report", subtitle_style))
    story.append(HRFlowable(width="100%", thickness=2, color=DARK))
    story.append(Spacer(1, 0.4 * cm))

    # ── Status badge ─────────────────────────────────────────────────────────
    passed       = result.get("passed", False)
    status_color = GREEN if passed else RED
    status_text  = "PASSED — File moved to processed/" if passed else "REJECTED — Issues detected"

    status_data = [[Paragraph(f"<b>{status_text}</b>", ParagraphStyle(
        "StatusText", parent=body_style, textColor=colors.white, fontSize=12
    ))]]
    status_table = Table(status_data, colWidths=["100%"])
    status_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), status_color),
        ("ROUNDEDCORNERS", [6]),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LEFTPADDING",   (0, 0), (-1, -1), 14),
    ]))
    story.append(status_table)
    story.append(Spacer(1, 0.5 * cm))

    # ── File metadata table ───────────────────────────────────────────────────
    story.append(Paragraph("File Details", heading_style))

    meta_rows = [
        ["Field", "Value"],
        ["Filename",      result.get("filename", "—")],
        ["Scan started",  result.get("started",  "—")],
        ["Scan finished", result.get("finished", "—")],
        ["Rows",          str(result.get("rows",    "—"))],
        ["Columns",       str(result.get("columns", "—"))],
        ["Null issues",   str(len(result.get("null_issues",    [])))],
        ["Outlier issues",str(len(result.get("outlier_issues", [])))],
    ]

    meta_table = Table(meta_rows, colWidths=[5 * cm, 11.5 * cm])
    meta_table.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), DARK),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [LIGHT, colors.white]),
        ("GRID",          (0, 0), (-1, -1), 0.5, colors.HexColor("#bdc3c7")),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
    ]))
    story.append(meta_table)

    # ── Issues ────────────────────────────────────────────────────────────────
    null_issues    = result.get("null_issues",    [])
    outlier_issues = result.get("outlier_issues", [])

    if null_issues:
        story.append(Paragraph("Null Value Issues", heading_style))
        for issue in null_issues:
            story.append(Paragraph(f"• {issue}", issue_style))

    if outlier_issues:
        story.append(Paragraph("Outlier Issues  (3-sigma rule)", heading_style))
        for issue in outlier_issues:
            story.append(Paragraph(f"• {issue}", issue_style))

    if not null_issues and not outlier_issues and not result.get("error"):
        story.append(Spacer(1, 0.3 * cm))
        story.append(Paragraph(
            "No issues found. All quality checks passed.",
            ParagraphStyle("OK", parent=body_style, textColor=GREEN, fontSize=11),
        ))

    # ── Error block ───────────────────────────────────────────────────────────
    if result.get("error"):
        story.append(Paragraph("Processing Error", heading_style))
        story.append(Paragraph(result["error"], ParagraphStyle(
            "ErrText", parent=body_style, textColor=RED, fontSize=9
        )))

    # ── Footer ────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 1 * cm))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    story.append(Paragraph(
        f"Generated by Data Quality Firewall · {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ParagraphStyle("Footer", parent=body_style, fontSize=7, textColor=colors.grey, alignment=1),
    ))

    doc.build(story)
    return output_path