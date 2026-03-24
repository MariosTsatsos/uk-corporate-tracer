#!/usr/bin/env python3
"""
generate_report.py — UK Corporate Tracer PDF Intelligence Summary
Produces a professionally formatted PDF report from the pipeline database.
Data sources: asset_discovery.db (Companies House + Land Registry CCOD)
              HM Land Registry Price Paid Data (pp-complete.txt, full dataset)
"""

import sqlite3
from datetime import date
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether
)
from reportlab.platypus.flowables import Flowable

from config import DB_PATH

OUTPUT_PATH = "output/asset_intelligence_summary.pdf"

# ── Colour palette ────────────────────────────────────────────────────────────
DARK_NAVY   = colors.HexColor("#0D1B2A")
MID_BLUE    = colors.HexColor("#1B4F72")
ACCENT_BLUE = colors.HexColor("#2E86C1")
LIGHT_BLUE  = colors.HexColor("#D6EAF8")
LIGHT_GREY  = colors.HexColor("#F4F6F7")
MID_GREY    = colors.HexColor("#BDC3C7")
RED_ALERT   = colors.HexColor("#C0392B")
AMBER       = colors.HexColor("#E67E22")
GREEN_OK    = colors.HexColor("#1E8449")
WHITE       = colors.white
BLACK       = colors.black

# ── Styles ────────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

def make_style(name, parent="Normal", **kwargs):
    return ParagraphStyle(name, parent=styles[parent], **kwargs)

S_TITLE      = make_style("OV_Title",    "Normal",  fontSize=26, textColor=WHITE,
                           fontName="Helvetica-Bold", leading=32, alignment=TA_CENTER)
S_SUBTITLE   = make_style("OV_Sub",      "Normal",  fontSize=13, textColor=LIGHT_BLUE,
                           fontName="Helvetica", leading=18, alignment=TA_CENTER)
S_H1         = make_style("OV_H1",       "Normal",  fontSize=14, textColor=WHITE,
                           fontName="Helvetica-Bold", leading=18)
S_H2         = make_style("OV_H2",       "Normal",  fontSize=11, textColor=MID_BLUE,
                           fontName="Helvetica-Bold", leading=15, spaceBefore=6)
S_H3         = make_style("OV_H3",       "Normal",  fontSize=10, textColor=DARK_NAVY,
                           fontName="Helvetica-Bold", leading=13, spaceBefore=4)
S_BODY       = make_style("OV_Body",     "Normal",  fontSize=9,  textColor=BLACK,
                           fontName="Helvetica", leading=13, spaceAfter=3)
S_BODY_SMALL = make_style("OV_Small",    "Normal",  fontSize=8,  textColor=colors.HexColor("#333333"),
                           fontName="Helvetica", leading=11)
S_MONO       = make_style("OV_Mono",     "Normal",  fontSize=8,  textColor=DARK_NAVY,
                           fontName="Courier", leading=11)
S_SPEC       = make_style("OV_Spec",     "Normal",  fontSize=8.5, textColor=colors.HexColor("#7D3C98"),
                           fontName="Helvetica-Oblique", leading=12)
S_ALERT      = make_style("OV_Alert",    "Normal",  fontSize=9,  textColor=RED_ALERT,
                           fontName="Helvetica-Bold", leading=13)
S_AMBER      = make_style("OV_Amber",    "Normal",  fontSize=9,  textColor=AMBER,
                           fontName="Helvetica-Bold", leading=13)
S_TOC        = make_style("OV_TOC",      "Normal",  fontSize=10, textColor=DARK_NAVY,
                           fontName="Helvetica", leading=16, leftIndent=10)

# ── Helper flowables ──────────────────────────────────────────────────────────

def section_header(title, subtitle=None):
    """Full-width coloured section header block."""
    items = []
    data = [[Paragraph(title, S_H1)]]
    if subtitle:
        data.append([Paragraph(subtitle, make_style("_sh_sub", "Normal",
                               fontSize=9, textColor=LIGHT_BLUE,
                               fontName="Helvetica", leading=12))])
    t = Table(data, colWidths=[170*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0,0), (-1,-1), MID_BLUE),
        ("LEFTPADDING",  (0,0), (-1,-1), 8),
        ("RIGHTPADDING", (0,0), (-1,-1), 8),
        ("TOPPADDING",   (0,0), (0,0),   6),
        ("BOTTOMPADDING",(0,-1),(-1,-1), 6),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[MID_BLUE]),
    ]))
    items.append(Spacer(1, 6))
    items.append(t)
    items.append(Spacer(1, 4))
    return items


def kv_table(rows, col_widths=(60*mm, 110*mm), alt=True):
    """Simple two-column key-value table."""
    data = []
    for k, v in rows:
        data.append([
            Paragraph(str(k), S_BODY_SMALL),
            Paragraph(str(v), S_BODY_SMALL),
        ])
    t = Table(data, colWidths=list(col_widths))
    style = [
        ("GRID",        (0,0), (-1,-1), 0.3, MID_GREY),
        ("LEFTPADDING",  (0,0), (-1,-1), 5),
        ("RIGHTPADDING", (0,0), (-1,-1), 5),
        ("TOPPADDING",   (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("FONTNAME",     (0,0), (0,-1),  "Helvetica-Bold"),
        ("FONTSIZE",     (0,0), (-1,-1), 8),
    ]
    if alt:
        for i in range(0, len(data), 2):
            style.append(("BACKGROUND", (0,i), (-1,i), LIGHT_GREY))
    t.setStyle(TableStyle(style))
    return t


def property_table(headers, rows, col_widths=None):
    """Multi-column property data table."""
    all_rows = [headers] + rows
    data = []
    for r in all_rows:
        data.append([Paragraph(str(c), S_BODY_SMALL) for c in r])

    if col_widths is None:
        cw = [170*mm // len(headers)] * len(headers)
    else:
        cw = col_widths

    t = Table(data, colWidths=cw, repeatRows=1)
    style = [
        ("BACKGROUND",   (0,0), (-1,0),  MID_BLUE),
        ("TEXTCOLOR",    (0,0), (-1,0),  WHITE),
        ("FONTNAME",     (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",     (0,0), (-1,-1), 7.5),
        ("GRID",         (0,0), (-1,-1), 0.3, MID_GREY),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING",   (0,0), (-1,-1), 3),
        ("BOTTOMPADDING",(0,0), (-1,-1), 3),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, LIGHT_GREY]),
    ]
    t.setStyle(TableStyle(style))
    return t


def note_box(text, colour=LIGHT_BLUE, border=ACCENT_BLUE, style=None):
    """Coloured note / callout box."""
    s = style or S_BODY_SMALL
    data = [[Paragraph(text, s)]]
    t = Table(data, colWidths=[170*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), colour),
        ("LEFTPADDING",  (0,0), (-1,-1), 8),
        ("RIGHTPADDING", (0,0), (-1,-1), 8),
        ("TOPPADDING",   (0,0), (-1,-1), 5),
        ("BOTTOMPADDING",(0,0), (-1,-1), 5),
        ("BOX",          (0,0), (-1,-1), 1, border),
    ]))
    return t


# ── Page template ─────────────────────────────────────────────────────────────

def make_doc(path):
    doc = SimpleDocTemplate(
        path,
        pagesize=A4,
        leftMargin=20*mm,
        rightMargin=20*mm,
        topMargin=20*mm,
        bottomMargin=18*mm,
        title="UK Corporate Tracer — Asset Intelligence Summary",
        author="Automated Pipeline",
        subject="Corporate Asset Intelligence Report",
    )
    return doc


def header_footer(canvas, doc):
    canvas.saveState()
    w, h = A4
    # Header bar
    canvas.setFillColor(DARK_NAVY)
    canvas.rect(0, h - 14*mm, w, 14*mm, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawString(20*mm, h - 9*mm, "UK CORPORATE TRACER — CONFIDENTIAL")
    canvas.setFont("Helvetica", 8)
    canvas.drawRightString(w - 20*mm, h - 9*mm, f"Generated: {date.today().strftime('%d %B %Y')}")
    # Footer
    canvas.setFillColor(DARK_NAVY)
    canvas.rect(0, 0, w, 10*mm, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica", 7.5)
    canvas.drawString(20*mm, 3.5*mm, "STRICTLY CONFIDENTIAL — For authorised recipients only — Not for distribution")
    canvas.drawRightString(w - 20*mm, 3.5*mm, f"Page {doc.page}")
    canvas.restoreState()


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    properties = conn.execute("""
        SELECT p.title_number, p.property_address, p.postcode, p.tenure,
               p.price_paid, p.owner_company, p.owner_company_number,
               c.status as company_status, c.company_name as current_name
        FROM properties p
        LEFT JOIN companies c ON c.company_number = p.owner_company_number
        ORDER BY p.property_address
    """).fetchall()

    charges_out = conn.execute("""
        SELECT DISTINCT dc.company_name, c.company_number, c.charge_description,
               c.charge_holder, c.charge_status, c.charge_created
        FROM charges c
        JOIN director_companies dc ON dc.company_number = c.company_number
        WHERE c.charge_status = 'outstanding'
        ORDER BY dc.company_name, c.charge_created
    """).fetchall()

    companies_active = conn.execute("""
        SELECT DISTINCT dc.company_number, dc.company_name, c.status,
                        c.registered_office, c.company_name as current_name
        FROM director_companies dc
        LEFT JOIN companies c ON c.company_number = dc.company_number
        WHERE c.status NOT IN ('dissolved', 'liquidation')
        ORDER BY dc.company_name
    """).fetchall()

    companies_liq = conn.execute("""
        SELECT DISTINCT dc.company_number, dc.company_name, c.status,
                        c.registered_office, c.company_name as current_name
        FROM director_companies dc
        LEFT JOIN companies c ON c.company_number = dc.company_number
        WHERE c.status IN ('liquidation')
        ORDER BY dc.company_name
    """).fetchall()

    companies_dissolved = conn.execute("""
        SELECT DISTINCT dc.company_number, dc.company_name, c.status
        FROM director_companies dc
        LEFT JOIN companies c ON c.company_number = dc.company_number
        WHERE c.status = 'dissolved'
        ORDER BY dc.company_name
    """).fetchall()

    # HOME SUSSEX subsidiaries via corporate PSC
    subsidiaries = conn.execute("""
        SELECT DISTINCT p.company_number, dc.company_name, p.psc_name, p.natures_of_control
        FROM psc p
        LEFT JOIN director_companies dc ON dc.company_number = p.company_number
        WHERE (p.psc_name LIKE '%HOME SUSSEX%' OR p.psc_name LIKE '%AMF PROPERTY%')
          AND p.ceased_on IS NULL
    """).fetchall()

    conn.close()
    return properties, charges_out, companies_active, companies_liq, companies_dissolved, subsidiaries


# ── Price Paid Data (known results from PPD scan) ────────────────────────────
# These results were derived from a single-pass scan of HM Land Registry
# Price Paid Data (pp-complete.txt, 30.9M rows, full dataset to 2026-03-08).
# PPD does NOT contain buyer/seller names; matches are by postcode + PAON.
# All prices are as recorded in the PPD; no adjustments made.

# PPD_RESULTS: Populate this dict with results from your ppd_scan.py run.
# Each key is a property address. Each value contains the transaction history
# and metadata for that address as found in the HM Land Registry Price Paid Data.
#
# Example structure (replace with your own findings):
PPD_RESULTS = {
    "1 Example Street, Anytown AB1 2CD": {
        "title": "EX123456",
        "company": "EXAMPLE PROPERTY LTD (12345678)",
        "note": "Freehold purchased; converted to flats. Bare title only remains.",
        "transactions": [
            ("2020-01-15", "£800,000",  "Freehold",  "Other/Commercial", "1 Example Street — freehold purchase"),
            ("2021-06-10", "£175,000",  "Leasehold", "Flat/Maisonette",  "Flat 1, 1 Example Street — leasehold sold"),
            ("2021-09-20", "£180,000",  "Leasehold", "Flat/Maisonette",  "Flat 2, 1 Example Street — leasehold sold"),
        ],
        "gross_sales": "£355,000",
        "cost":        "£800,000",
        "speculative": False,
    },
    "99 Sample Road, City BC2 3DE": {
        "title": "Unknown — not in CCOD",
        "company": "UNKNOWN (freehold not registered to any company in network)",
        "note": "Address found in charge description. Freehold not in CCOD under any network company. "
                "May be personally held. Requires LR title search to confirm.",
        "transactions": [
            ("2018-05-01", "£500,000",  "Freehold", "Semi-detached", "99 Sample Road — freehold transaction"),
        ],
        "gross_sales": "n/a",
        "cost":        "£500,000 (est.)",
        "speculative": True,
        "spec_note":   "SPECULATIVE: Freehold buyer is not confirmed from PPD (contains no names). "
                       "Connection inferred from charge description only. Verify with £3 LR title search.",
    },
}


# ── Immediate Actions ─────────────────────────────────────────────────────────

# ACTIONS: Populate this list with case-specific recommended actions.
# Each entry will be rendered as a prioritised action item in the PDF.
# Example structure (replace with your own findings):
ACTIONS = [
    {
        "priority": "P1 — URGENT",
        "colour": RED_ALERT,
        "title": "LR title search on primary enforcement candidate",
        "detail": (
            "Cost: £3 at landregistry.gov.uk (title register search). "
            "Obtain current registered proprietor and confirm any outstanding charges still appear. "
            "Replace this placeholder with the specific property title number and address from your investigation."
        ),
    },
    {
        "priority": "P2 — HIGH",
        "colour": AMBER,
        "title": "CCJ search against target individuals",
        "detail": (
            "Search for county court judgments at Trust Online (trustonline.org.uk), £6 per search. "
            "Multiple CCJs indicate competing creditor pressure and affect enforcement priority. "
            "Replace this placeholder with the full legal names and DOBs of your target individuals."
        ),
    },
    {
        "priority": "P2 — HIGH",
        "colour": AMBER,
        "title": "Check insolvency and planning registers",
        "detail": (
            "Check the Insolvency Register (insolvency-practitioner.service.gov.uk) for IVA or "
            "bankruptcy orders against target individuals. "
            "Search local planning portals for applications listing them as applicant or agent — "
            "may reveal personally held development sites not captured by CCOD."
        ),
    },
    {
        "priority": "P3 — MEDIUM",
        "colour": GREEN_OK,
        "title": "Verify outstanding charges with lenders",
        "detail": (
            "Outstanding charge status at Companies House does NOT confirm live debt. "
            "Lenders frequently fail to file satisfaction forms (MR04) after repayment. "
            "Contact lenders directly to confirm current debt position before assuming senior priority."
        ),
    },
]


# ── BUILD PDF ─────────────────────────────────────────────────────────────────

def build_pdf():
    properties, charges_out, companies_active, companies_liq, companies_dissolved, subsidiaries = load_data()

    doc = make_doc(OUTPUT_PATH)
    story = []

    # ════════════════════════════════════════════════════════════════════════
    # COVER PAGE
    # ════════════════════════════════════════════════════════════════════════
    story.append(Spacer(1, 25*mm))

    cover_data = [[Paragraph("ASSET INTELLIGENCE SUMMARY", S_TITLE)],
                  [Paragraph("UK Corporate Ownership & Property Tracing Report", S_SUBTITLE)],
                  [Spacer(1, 4)],
                  [Paragraph("Confidential — Internal Use Only", make_style("_cs", "Normal",
                              fontSize=10, textColor=LIGHT_BLUE, fontName="Helvetica-Oblique",
                              alignment=TA_CENTER))]]
    cover_table = Table(cover_data, colWidths=[170*mm])
    cover_table.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), DARK_NAVY),
        ("LEFTPADDING",  (0,0), (-1,-1), 15),
        ("RIGHTPADDING", (0,0), (-1,-1), 15),
        ("TOPPADDING",   (0,0), (0,0),   20),
        ("BOTTOMPADDING",(0,-1),(-1,-1), 20),
        ("ROWBACKGROUNDS",(0,0),(-1,-1), [DARK_NAVY]),
    ]))
    story.append(cover_table)
    story.append(Spacer(1, 10*mm))

    from datetime import date as _date
    meta = [
        ("Date of Report",   _date.today().strftime("%d %B %Y")),
        ("Subject",          "— see config.py —"),
        ("Purpose",          "Corporate ownership mapping and property asset tracing"),
        ("Data sources",     "Companies House API; CCOD; OCOD;\nHM Land Registry Price Paid Data;\nCH Bulk Data (PSC snapshot)"),
        ("Properties confirmed\n(CCOD current)", f"{len(properties)} properties across ~{len(companies_active)} active companies"),
        ("Companies in network", f"{len(companies_active) + len(companies_liq) + len(companies_dissolved)} identified (active, dissolved and in liquidation)"),
        ("Classification",   "STRICTLY CONFIDENTIAL"),
    ]
    story.append(kv_table(meta, col_widths=(55*mm, 115*mm)))
    story.append(Spacer(1, 8*mm))

    story.append(note_box(
        "<b>Purpose of this document:</b> This report summarises asset-tracing findings for debt recovery "
        "proceedings against the above individuals and their associated corporate network. "
        "All data is drawn from public registers and automated pipeline analysis. "
        "Where findings are speculative or require verification they are clearly labelled. "
        "This document does not constitute legal advice.",
        colour=LIGHT_BLUE, border=ACCENT_BLUE
    ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 1 — EXECUTIVE SUMMARY
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("1. Executive Summary")

    story.append(Paragraph(
        "The pipeline has identified "
        f"<b>{len(properties)} properties currently registered in the Land Registry CCOD</b> "
        "(Corporate and Commercial Ownership Dataset) across the active companies in the network.",
        S_BODY
    ))
    story.append(Spacer(1, 3))
    story.append(Paragraph(
        "Price Paid Data (PPD) analysis reveals the historical transaction pattern: multiple properties "
        "have already been developed and sold, generating gross proceeds that substantially exceed "
        "acquisition costs. Several companies with outstanding bank charges no longer hold the "
        "underlying assets — a common pattern where lenders fail to file satisfaction notices "
        "after loan repayment.",
        S_BODY
    ))
    story.append(Spacer(1, 3))

    # Key flags — populated dynamically from database
    # Replace this placeholder list with findings from your own pipeline run.
    flags = [
        ["⚠ CRITICAL", f"Companies in liquidation: {len(companies_liq)}. "
         "Register as creditor with the appointed liquidator(s) immediately using proof of debt."],
        ["⚠ ASSETS", f"{len(properties)} properties confirmed in CCOD across {len(companies_active)} active companies. "
         "See Section 2 for full list with tenure and charge status."],
        ["⚑ TO VERIFY", "Personally held properties are NOT captured by CCOD. "
         "LR title searches (£3 each) required to confirm any personally held assets."],
        ["! NOTE", "Outstanding charge status at Companies House does not confirm live debt. "
         "Verify with lenders before assuming senior creditor priority."],
    ]
    flag_data = [[Paragraph(f[0], S_ALERT if "CRITICAL" in f[0] or "KEY" in f[0] else
                              (S_AMBER if "VERIFY" in f[0] or "SOLD" in f[0] else S_BODY)),
                  Paragraph(f[1], S_BODY_SMALL)] for f in flags]
    ft = Table(flag_data, colWidths=[28*mm, 142*mm])
    ft.setStyle(TableStyle([
        ("GRID",         (0,0), (-1,-1), 0.3, MID_GREY),
        ("LEFTPADDING",  (0,0), (-1,-1), 5),
        ("RIGHTPADDING", (0,0), (-1,-1), 5),
        ("TOPPADDING",   (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",(0,0), (-1,-1), 4),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
        ("ROWBACKGROUNDS",(0,0),(-1,-1), [colors.HexColor("#FDECEA"), WHITE,
                                          colors.HexColor("#FEF9E7"), colors.HexColor("#FEF9E7")]),
    ]))
    story.append(ft)
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 2 — CONFIRMED PROPERTY HOLDINGS (CCOD)
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("2. Confirmed Current Property Holdings",
                            "Source: HM Land Registry CCOD March 2026 — registered proprietors as at data cut-off")

    story.append(Paragraph(
        "The following properties are <b>confirmed as currently registered</b> at HM Land Registry "
        "in the name of companies within the investigated network. "
        "Price paid figures (where shown) are as recorded in CCOD at time of registration and may not "
        "reflect current market value. Properties where development is ongoing or planned may be subject "
        "to charges that take priority over unsecured creditors.",
        S_BODY
    ))
    story.append(Spacer(1, 4))

    # Group by company status
    active_props = [p for p in properties if p["company_status"] not in ("dissolved", "liquidation")]
    liq_props    = [p for p in properties if p["company_status"] == "liquidation"]
    dis_props    = [p for p in properties if p["company_status"] == "dissolved"]

    def fmt_price(p):
        if p and p.isdigit() and int(p) > 0:
            return f"£{int(p):,}"
        return "n/r"

    def prop_rows(prop_list):
        rows = []
        for p in prop_list:
            rows.append([
                p["title_number"] or "",
                p["property_address"],
                p["tenure"] or "",
                fmt_price(p["price_paid"]),
                p["current_name"] or p["owner_company"],
                p["company_status"].upper() if p["company_status"] else "",
            ])
        return rows

    hdrs = ["Title No.", "Address", "Tenure", "Price Paid", "Registered Company", "Co. Status"]
    cws  = [22*mm, 58*mm, 18*mm, 22*mm, 38*mm, 14*mm]

    if active_props:
        story.append(Paragraph("Active company holdings:", S_H2))
        story.append(property_table(hdrs, prop_rows(active_props), col_widths=cws))
        story.append(Spacer(1, 4))

    if liq_props:
        story.append(Paragraph("Holdings in companies under liquidation (⚠ liquidator has priority):", S_H2))
        story.append(property_table(hdrs, prop_rows(liq_props), col_widths=cws))
        story.append(Spacer(1, 4))
        story.append(note_box(
            "<b>Note on liquidation assets:</b> Properties held by companies in liquidation fall under the "
            "control of the appointed liquidator. Unsecured creditors rank behind secured lenders and "
            "liquidator fees. Register as creditor immediately using proof of debt / personal guarantee.",
            colour=colors.HexColor("#FDECEA"), border=RED_ALERT
        ))
        story.append(Spacer(1, 4))

    if dis_props:
        story.append(Paragraph("Holdings in dissolved companies (title may be bona vacantia):", S_H2))
        story.append(property_table(hdrs, prop_rows(dis_props), col_widths=cws))
        story.append(note_box(
            "<b>Note on dissolved company holdings:</b> If a dissolved company still holds title in CCOD, "
            "the property may have vested in the Crown as bona vacantia. "
            "Practical enforcement value is likely nil unless the company is restored to the register.",
            colour=colors.HexColor("#FEF9E7"), border=AMBER
        ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 3 — PRICE PAID DATA ANALYSIS
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("3. Transaction History — HM Land Registry Price Paid Data",
                            "Source: pp-complete.txt — full PPD dataset, 30.9M transactions. "
                            "PPD does NOT contain buyer/seller names. Matches are by postcode + house number.")

    story.append(Paragraph(
        "Price Paid Data provides a historical record of all UK property transactions since 1995. "
        "Because PPD contains no buyer or seller names, matches rely on postcodes and house numbers "
        "derived from the Companies House charge descriptions and CCOD records. "
        "Speculative findings are clearly marked. Where a finding is speculative, "
        "<b>no legal reliance should be placed on it without independent verification</b>.",
        S_BODY
    ))
    story.append(Spacer(1, 5))

    for prop_name, data in PPD_RESULTS.items():
        items = []
        items.append(KeepTogether([
            Paragraph(prop_name, S_H2),
        ]))

        meta_rows = [
            ("LR Title",    data["title"]),
            ("Company",     data["company"]),
            ("Summary",     data["note"]),
            ("Gross sales", data["gross_sales"]),
            ("Acquisition", data["cost"]),
        ]
        items.append(kv_table(meta_rows, col_widths=(35*mm, 135*mm)))
        items.append(Spacer(1, 3))

        if data.get("speculative"):
            items.append(note_box(
                f"<b>⚑ SPECULATIVE:</b> {data['spec_note']}",
                colour=colors.HexColor("#FEF9E7"), border=AMBER,
                style=S_SPEC
            ))
            items.append(Spacer(1, 3))

        # Transaction table
        txn_headers = ["Date", "Price", "Tenure", "Type", "Description"]
        txn_cws = [22*mm, 25*mm, 22*mm, 28*mm, 73*mm]
        txn_rows = list(data["transactions"])
        items.append(property_table(txn_headers, txn_rows, col_widths=txn_cws))
        items.append(Spacer(1, 6))
        story.append(KeepTogether(items))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 4 — OUTSTANDING CHARGES
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("4. Outstanding Charges Register",
                            "Source: Companies House API — charges as at 8 March 2026")

    story.append(Paragraph(
        "Outstanding charges are recorded at Companies House against each company's assets. "
        "<b>Important caveat:</b> an 'outstanding' status at Companies House does not necessarily mean "
        "the underlying debt is unpaid. Lenders frequently fail to file satisfaction forms (MR04) "
        "after repayment. Treat outstanding charges as potential senior creditor claims "
        "requiring verification, not as confirmed live debt. "
        "Charges are listed chronologically per company.",
        S_BODY
    ))
    story.append(Spacer(1, 5))

    ch_headers = ["Date", "Company", "Charge Holder", "Property / Description (truncated)"]
    ch_cws = [20*mm, 42*mm, 40*mm, 68*mm]
    ch_rows = []
    for c in charges_out:
        desc = (c["charge_description"] or "").strip()
        desc = desc[:140] + "…" if len(desc) > 140 else desc
        ch_rows.append([
            c["charge_created"] or "",
            f"{c['company_name'] or '—'}\n({c['company_number']})",
            c["charge_holder"] or "—",
            desc or "(no description filed)",
        ])

    story.append(property_table(ch_headers, ch_rows, col_widths=ch_cws))
    story.append(Spacer(1, 4))
    story.append(note_box(
        f"<b>Total outstanding charges: {len(charges_out)}</b>  |  "
        "Satisfied charges are not listed here. Each outstanding charge should be verified with the relevant "
        "lender before assuming senior priority status.",
        colour=LIGHT_GREY, border=MID_GREY
    ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 5 — COMPANIES IN LIQUIDATION
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("5. Companies in Liquidation — Detail")

    if companies_liq:
        story.append(Paragraph(
            "The following companies in the network are currently in liquidation. "
            "Assets held by these companies fall under the control of the appointed liquidator. "
            "The liquidator's powers may extend to shareholdings in subsidiary companies.",
            S_BODY
        ))
        story.append(Spacer(1, 4))

        for co in companies_liq:
            liq_details = [
                ("Company No.",   co["company_number"]),
                ("Name",          co["current_name"] or co["company_name"] or "—"),
                ("Status",        "LIQUIDATION"),
                ("Reg. office",   co["registered_office"] or "—"),
            ]
            story.append(Paragraph(co["current_name"] or co["company_name"] or co["company_number"], S_H3))
            story.append(kv_table(liq_details, col_widths=(45*mm, 125*mm)))
            story.append(Spacer(1, 4))

        story.append(note_box(
            "<b>Action required:</b> Register as creditor with each liquidator immediately. "
            "Submit proof of debt and any personal guarantee documentation. "
            "Enquire whether the liquidator intends to realise property assets and/or corporate shareholdings.",
            colour=colors.HexColor("#FDECEA"), border=RED_ALERT, style=S_ALERT
        ))
    else:
        story.append(Paragraph("No companies in liquidation found in the current dataset.", S_BODY))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 6 — CORPORATE NETWORK MAP
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("6. Corporate Network — Key Active Companies")

    story.append(Paragraph(
        "The following table lists active companies in the network with current "
        "LR property holdings and any associated charges, drawn directly from the pipeline database.",
        S_BODY
    ))
    story.append(Spacer(1, 4))

    # Build key companies table dynamically from database
    import sqlite3 as _sq
    _conn = _sq.connect(DB_PATH)
    _conn.row_factory = _sq.Row
    key_companies_rows = _conn.execute("""
        SELECT DISTINCT dc.company_number, dc.company_name,
               c.status, c.registered_office,
               p.title_number, p.property_address, p.tenure, p.price_paid,
               GROUP_CONCAT(DISTINCT ch.charge_holder) as charge_holders
        FROM director_companies dc
        LEFT JOIN companies c ON c.company_number = dc.company_number
        LEFT JOIN properties p ON p.owner_company_number = dc.company_number
        LEFT JOIN charges ch ON ch.company_number = dc.company_number AND ch.charge_status = 'outstanding'
        WHERE c.status NOT IN ('dissolved', 'liquidation')
          AND p.title_number IS NOT NULL
        GROUP BY dc.company_number, p.title_number
        ORDER BY dc.company_name
        LIMIT 30
    """).fetchall()
    _conn.close()

    if key_companies_rows:
        key_companies = [
            (r["company_name"] or r["company_number"],
             r["company_number"],
             r["title_number"] or "—",
             (r["property_address"] or "")[:40],
             r["tenure"] or "—",
             f"£{int(r['price_paid']):,}" if (r["price_paid"] and str(r["price_paid"]).isdigit()) else "n/r",
             (r["charge_holders"] or "None")[:30])
            for r in key_companies_rows
        ]
        nc_headers = ["Company", "No.", "Title", "Address", "Tenure", "Price", "Key Charge(s)"]
        nc_cws = [38*mm, 18*mm, 18*mm, 38*mm, 15*mm, 13*mm, 32*mm]
        story.append(property_table(nc_headers, key_companies, col_widths=nc_cws))
    else:
        story.append(Paragraph("No active property-holding companies found in the current dataset.", S_BODY))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 7 — PERSONAL PROPERTY & INVESTIGATION LEADS
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("7. Personal Property Leads and Unresolved Queries",
                            "Items below require further investigation — clearly marked as SPECULATIVE where unconfirmed")

    story.append(Paragraph(
        "The CCOD and OCOD datasets only record properties held by UK or overseas companies. "
        "Personally held properties do not appear in these datasets. "
        "HM Land Registry does not offer a public name-based proprietor search — "
        "title searches require a known address or title number (£3 each at landregistry.gov.uk). "
        "The following leads have been identified but require verification.",
        S_BODY
    ))
    story.append(Spacer(1, 5))

    # leads: populate this list with investigation-specific unresolved queries.
    # Each entry is rendered as a labelled speculative finding in the PDF.
    # Example structure — replace with your own findings:
    leads = [
        {
            "ref": "LEAD 1",
            "title": "Possible personally held freehold — requires LR title search",
            "speculative": True,
            "body": (
                "A property address found in charge descriptions does not appear in CCOD under any "
                "company in the network. The freehold may be personally held by one of the target "
                "individuals, placing it outside the corporate structure. "
                "\n\n<b>If personally held, this would be a direct enforcement target.</b> "
                "\n\nAction: £3 LR title search by address at landregistry.gov.uk. "
                "Replace this placeholder with the specific address from your investigation."
            ),
        },
        {
            "ref": "LEAD 2",
            "title": "Outstanding charge referencing title not in CCOD",
            "speculative": True,
            "body": (
                "A company in the network holds an outstanding charge referencing a title number "
                "that does not appear in the current CCOD. The property may have been sold, "
                "transferred, or the title may be unregistered. "
                "\n\nOutstanding charge status at Companies House does not confirm live debt — "
                "lenders frequently fail to file satisfaction forms after repayment. "
                "\n\nAction: £3 LR title search by address or contact the charge holder directly "
                "to confirm current status. Replace this placeholder with specifics from your investigation."
            ),
        },
    ]

    for lead in leads:
        l_items = [
            Paragraph(f"{lead['ref']}: {lead['title']}", S_H3),
        ]
        if lead["speculative"]:
            l_items.append(note_box(
                "<b>⚑ SPECULATIVE — requires independent verification before legal action</b>",
                colour=colors.HexColor("#FEF9E7"), border=AMBER
            ))
            l_items.append(Spacer(1, 2))
        l_items.append(Paragraph(lead["body"].replace("\n\n", "<br/><br/>"), S_BODY))
        l_items.append(Spacer(1, 6))
        story.append(KeepTogether(l_items))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 8 — RECOMMENDED ACTIONS
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("8. Recommended Immediate Actions")

    for action in ACTIONS:
        colour = action["colour"]
        col_hex = colors.HexColor("#FDECEA") if colour == RED_ALERT else (
            colors.HexColor("#FEF9E7") if colour == AMBER else colors.HexColor("#EAFAF1"))
        border = colour

        a_items = [
            Paragraph(f"{action['priority']} — {action['title']}", make_style(
                "_at", "Normal", fontSize=10, textColor=colour,
                fontName="Helvetica-Bold", leading=14)),
            Paragraph(action["detail"], S_BODY),
            Spacer(1, 5),
        ]
        story.append(KeepTogether(a_items))

    story.append(HRFlowable(width="100%", thickness=0.5, color=MID_GREY))
    story.append(Spacer(1, 5))

    # ════════════════════════════════════════════════════════════════════════
    # SECTION 9 — DATA SOURCES & CAVEATS
    # ════════════════════════════════════════════════════════════════════════
    story += section_header("9. Data Sources and Methodology")

    sources = [
        ("Companies House API",    "Director appointments, company details, PSC, officers and charges "
                                   "retrieved via CH REST API."),
        ("CH Bulk Data (PSC)",     "PSC records from CH bulk snapshot. "
                                   "Used for network expansion via BFS traversal."),
        ("CH Bulk Data (companies)","BasicCompanyDataAsOneFile — full company register."),
        ("CCOD",                   "HM Land Registry Corporate and Commercial Ownership Dataset — "
                                   "current-state registered proprietors of UK property held by companies. "
                                   "Does not include personally held property or dispositions made "
                                   "after the data cut-off."),
        ("OCOD",                   "HM Land Registry Overseas corporate ownership dataset."),
        ("Price Paid Data",        "pp-complete.txt — full HM Land Registry PPD. "
                                   "No buyer/seller names. Matches by postcode + PAON (house number). "
                                   "Scanned in a single pass (dual-mode: postcode index + street index)."),
        ("Pipeline code",          "Python 3 pipeline (asset_discovery.db / ch_bulk.db). "
                                   "SQLite databases. See github.com for source."),
    ]
    story.append(kv_table(sources, col_widths=(50*mm, 120*mm)))
    story.append(Spacer(1, 5))

    story.append(note_box(
        "<b>Limitations:</b> (1) CCOD is current-state only — sold properties do not appear. "
        "PPD is used to reconstruct historical transactions. "
        "(2) PPD has no names — address matching carries a small false-positive risk for "
        "common street names. All results should be confirmed by LR title search. "
        "(3) Companies House charge status ('outstanding') does not guarantee live debt — "
        "lenders frequently omit to file satisfaction forms. "
        "(4) LR does not offer a public name-based proprietor search. "
        "Personal property holdings are not captured by this pipeline. "
        "(5) This report does not constitute legal advice.",
        colour=LIGHT_GREY, border=MID_GREY, style=S_BODY_SMALL
    ))

    # Build
    doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
    print(f"PDF written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    build_pdf()
