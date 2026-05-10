# -*- coding: utf-8 -*-
"""
Build a single PDF of Encyclopedia v2 using reportlab (pure-Python, no GTK/system deps).

Parses markdown chapters and renders to PDF with cover page, part dividers,
tables, headers, blockquotes — all styled for institutional research review.

Output: ~/Downloads/OBQ_Encyclopedia_v2_<timestamp>.pdf
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak,
    Table, TableStyle, KeepTogether,
)
from reportlab.pdfgen import canvas as rl_canvas

ENCYCLOPEDIA_DIR = Path(r'D:/OBQ_AI/OBQ_Encyclopedia_v2')
DOWNLOADS = Path.home() / 'Downloads'

PART_ORDER = [
    'Part_II_Valuation',
    'Part_III_Profitability',
    'Part_IV_Financial_Strength',
    'Part_V_Momentum',
    'Part_VI_Growth',
    'Part_VII_Moat_Capital',
    'Part_VIII_Two_Factor_Combos',
    'Part_IX_OBQ_Composites',
    # CYC-004 pure factor baselines
    'Part_XI_CYC004_Tier1',
    'Part_XII_CYC004_Tier2',
    'Part_XIII_CYC004_Tier3',
    # CYC-005 sector-specific novel factors
    'Part_XIV_CYC005_Sector',
]

# Color palette
ACCENT      = colors.HexColor("#0066cc")
ACCENT_DARK = colors.HexColor("#1e3a8a")
TEXT        = colors.HexColor("#111827")
DIM         = colors.HexColor("#475569")
MUTED       = colors.HexColor("#94a3b8")
BORDER      = colors.HexColor("#cbd5e1")
ROW_ALT     = colors.HexColor("#f8fafc")
TH_BG       = colors.HexColor("#f1f5f9")
QUOTE_BG    = colors.HexColor("#eff6ff")


def _make_styles():
    base = getSampleStyleSheet()
    s = {}
    s['cover_title'] = ParagraphStyle(
        'CoverTitle', parent=base['Title'],
        fontName='Helvetica-Bold', fontSize=30, textColor=TEXT,
        alignment=TA_CENTER, spaceAfter=10, leading=36,
    )
    s['cover_sub'] = ParagraphStyle(
        'CoverSub', parent=base['Normal'],
        fontName='Helvetica', fontSize=14, textColor=DIM,
        alignment=TA_CENTER, spaceAfter=20,
    )
    s['cover_meta'] = ParagraphStyle(
        'CoverMeta', parent=base['Normal'],
        fontName='Helvetica', fontSize=10, textColor=DIM,
        alignment=TA_CENTER, leading=16,
    )
    s['part_title'] = ParagraphStyle(
        'PartTitle', parent=base['Title'],
        fontName='Helvetica-Bold', fontSize=24, textColor=ACCENT,
        alignment=TA_CENTER, spaceAfter=10,
    )
    s['part_sub'] = ParagraphStyle(
        'PartSub', parent=base['Normal'],
        fontName='Helvetica', fontSize=12, textColor=DIM,
        alignment=TA_CENTER,
    )
    s['h1'] = ParagraphStyle(
        'H1', parent=base['Heading1'],
        fontName='Helvetica-Bold', fontSize=16, textColor=TEXT,
        spaceBefore=6, spaceAfter=8, leading=20,
    )
    s['h2'] = ParagraphStyle(
        'H2', parent=base['Heading2'],
        fontName='Helvetica-Bold', fontSize=12, textColor=ACCENT_DARK,
        spaceBefore=10, spaceAfter=3, leading=14,
    )
    s['h3'] = ParagraphStyle(
        'H3', parent=base['Heading3'],
        fontName='Helvetica-Bold', fontSize=10, textColor=TEXT,
        spaceBefore=6, spaceAfter=2, leading=12,
    )
    s['body'] = ParagraphStyle(
        'Body', parent=base['Normal'],
        fontName='Helvetica', fontSize=9, textColor=TEXT,
        leading=12, spaceBefore=2, spaceAfter=3,
    )
    s['meta'] = ParagraphStyle(
        'Meta', parent=base['Normal'],
        fontName='Helvetica-Oblique', fontSize=8.5, textColor=DIM,
        leading=11, spaceAfter=4,
    )
    s['quote'] = ParagraphStyle(
        'Quote', parent=base['Normal'],
        fontName='Helvetica', fontSize=8.5, textColor=ACCENT_DARK,
        leftIndent=10, rightIndent=6, leading=11,
        spaceBefore=4, spaceAfter=4,
        backColor=QUOTE_BG, borderPadding=(5, 8, 5, 8),
    )
    s['bullet'] = ParagraphStyle(
        'Bullet', parent=base['Normal'],
        fontName='Helvetica', fontSize=9, textColor=TEXT,
        leftIndent=12, leading=12, spaceBefore=1, spaceAfter=2,
    )
    s['tbl_cell'] = ParagraphStyle(
        'TblCell', parent=base['Normal'],
        fontSize=7.5, leading=9.5, textColor=TEXT,
    )
    s['tbl_hdr'] = ParagraphStyle(
        'TblHdr', parent=base['Normal'],
        fontSize=7, leading=8.5, textColor=ACCENT_DARK,
        fontName='Helvetica-Bold',
    )
    return s

# ── Markdown inline parser ────────────────────────────────────────────────────

def _inline(text: str) -> str:
    """Convert inline markdown to ReportLab XML markup."""
    text = re.sub(r'\*\*([^*\n]+)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'(?<!\*)\*([^*\n]+)\*(?!\*)', r'<i>\1</i>', text)
    text = re.sub(r'`([^`\n]+)`', r'<font name="Courier">\1</font>', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)  # strip links
    return text


def _parse_md_table(lines: list, start: int) -> tuple:
    """Parse markdown table, return (list_of_row_lists, end_index)."""
    rows = []
    i = start
    while i < len(lines):
        l = lines[i].strip()
        if not (l.startswith('|') and l.endswith('|')):
            break
        if re.match(r'^\|[\s\-:|]+\|$', l):  # separator row
            i += 1
            continue
        rows.append([c.strip() for c in l.strip('|').split('|')])
        i += 1
    return rows, i


def _table_flowable(rows: list, styles: dict):
    if not rows:
        return None
    n_cols = max(len(r) for r in rows)
    page_w = letter[0] - 1.2 * inch
    col_w = page_w / n_cols
    parsed = []
    for r_idx, row in enumerate(rows):
        new_row = []
        for cell in row:
            cell = cell.replace('<br/>', ' ').replace('<br>', ' ')
            st = styles['tbl_hdr'] if r_idx == 0 else styles['tbl_cell']
            new_row.append(Paragraph(_inline(cell), st))
        # pad
        pad_st = styles['tbl_cell']
        while len(new_row) < n_cols:
            new_row.append(Paragraph('', pad_st))
        parsed.append(new_row)

    tbl = Table(parsed, colWidths=[col_w] * n_cols, repeatRows=1)
    tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0, 0), (-1, 0), TH_BG),
        ('LINEBELOW',     (0, 0), (-1, 0), 0.8, MUTED),
        ('LINEBELOW',     (0, 1), (-1, -1), 0.3, BORDER),
        ('ROWBACKGROUNDS',(0, 1), (-1, -1), [colors.white, ROW_ALT]),
        ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING',   (0, 0), (-1, -1), 5),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 5),
        ('TOPPADDING',    (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    return tbl


def _md_to_flowables(md_text: str, styles: dict) -> list:
    """Parse a markdown chapter into reportlab flowables."""
    out = []
    lines = md_text.split('\n')
    i = 0
    while i < len(lines):
        raw = lines[i]
        s = raw.strip()
        if not s:
            i += 1
            continue
        # H1
        if s.startswith('# '):
            out.append(Paragraph(_inline(s[2:]), styles['h1']))
            i += 1; continue
        # H2
        if s.startswith('## '):
            out.append(Paragraph(_inline(s[3:]), styles['h2']))
            i += 1; continue
        # H3
        if s.startswith('### '):
            out.append(Paragraph(_inline(s[4:]), styles['h3']))
            i += 1; continue
        # HR
        if s in ('---', '***', '___'):
            out.append(Spacer(1, 3))
            i += 1; continue
        # Blockquote
        if s.startswith('>'):
            qlines = []
            while i < len(lines) and lines[i].lstrip().startswith('>'):
                qlines.append(lines[i].lstrip().lstrip('>').strip())
                i += 1
            out.append(Paragraph(_inline(' '.join(qlines)), styles['quote']))
            continue
        # Italic-only meta line *...*
        if s.startswith('*') and s.endswith('*') and len(s) > 2 and not s.startswith('**'):
            out.append(Paragraph(_inline(s[1:-1]), styles['meta']))
            i += 1; continue
        # Table
        if s.startswith('|') and s.endswith('|'):
            rows, new_i = _parse_md_table(lines, i)
            tbl = _table_flowable(rows, styles)
            if tbl:
                out.append(KeepTogether(tbl))
                out.append(Spacer(1, 4))
            i = new_i; continue
        # Bullet list
        if s.startswith('- ') or s.startswith('* '):
            while i < len(lines) and (lines[i].lstrip().startswith('- ') or lines[i].lstrip().startswith('* ')):
                item = lines[i].lstrip()[2:].strip()
                out.append(Paragraph(f'\u2022 {_inline(item)}', styles['bullet']))
                i += 1
            continue
        # Body paragraph
        out.append(Paragraph(_inline(s), styles['body']))
        i += 1
    return out


def _page_decorator(c: rl_canvas.Canvas, doc):
    """Header + footer on every page after cover."""
    if doc.page <= 1:
        return
    c.saveState()
    c.setFont('Helvetica', 7)
    c.setFillColor(MUTED)
    c.drawRightString(letter[0] - 0.6*inch, letter[1] - 0.4*inch,
                      'OBQ FactorLab  \u00b7  CYC-003 R3000')
    c.setFont('Helvetica', 8)
    c.setFillColor(DIM)
    c.drawCentredString(letter[0]/2, 0.4*inch,
                        f'OBQ Factor Encyclopedia v2  \u00b7  Page {doc.page}')
    c.restoreState()

# ── Legacy CSS note (kept for reference only — not used by reportlab) ─────────
_CSS_REFERENCE_ONLY = """
@page {
    size: Letter;
    margin: 0.75in 0.6in 0.85in 0.6in;
    @bottom-center {
        content: "OBQ Factor Encyclopedia v2  ·  Page " counter(page) " of " counter(pages);
        font-family: 'Segoe UI', sans-serif;
        font-size: 8pt;
        color: #6b7280;
    }
    @top-right {
        content: "OBQ FactorLab  ·  CYC-003 R3000";
        font-family: 'Segoe UI', sans-serif;
        font-size: 7.5pt;
        color: #9ca3af;
    }
}
@page:first {
    @top-right { content: none; }
    @bottom-center { content: none; }
}

html, body {
    font-family: 'Segoe UI', 'Calibri', 'Arial', sans-serif;
    font-size: 9.5pt;
    line-height: 1.42;
    color: #111827;
}

h1 {
    font-size: 18pt;
    color: #0f172a;
    border-bottom: 3px solid #0066cc;
    padding-bottom: 6pt;
    margin-top: 24pt;
    margin-bottom: 12pt;
    page-break-before: always;
    page-break-after: avoid;
}
h1.cover-title { page-break-before: avoid; }

h2 {
    font-size: 13pt;
    color: #1e3a8a;
    margin-top: 18pt;
    margin-bottom: 6pt;
    border-bottom: 1px solid #cbd5e1;
    padding-bottom: 3pt;
    page-break-after: avoid;
}

h3 {
    font-size: 11pt;
    color: #1e293b;
    margin-top: 12pt;
    margin-bottom: 4pt;
    page-break-after: avoid;
}

p, ul, ol { margin: 4pt 0; }
li { margin: 2pt 0; }

table {
    border-collapse: collapse;
    width: 100%;
    margin: 6pt 0 10pt 0;
    font-size: 8.5pt;
    page-break-inside: avoid;
}
th {
    background: #f1f5f9;
    color: #1e293b;
    font-weight: 700;
    text-align: left;
    padding: 5pt 7pt;
    border-bottom: 2px solid #94a3b8;
    font-size: 8pt;
    text-transform: uppercase;
    letter-spacing: 0.4px;
}
td {
    padding: 4pt 7pt;
    border-bottom: 1px solid #e2e8f0;
    vertical-align: top;
}
tr:nth-child(even) td { background: #f8fafc; }
strong { color: #0066cc; font-weight: 700; }
em { color: #475569; }

code, pre {
    font-family: 'Consolas', 'Cascadia Mono', monospace;
    background: #f1f5f9;
    padding: 1pt 4pt;
    border-radius: 2pt;
    font-size: 8.5pt;
}

blockquote {
    border-left: 3px solid #0066cc;
    background: #eff6ff;
    padding: 6pt 10pt;
    margin: 6pt 0;
    color: #1e3a8a;
    font-size: 9pt;
}

hr {
    border: none;
    border-top: 1px solid #cbd5e1;
    margin: 10pt 0;
}

a { color: #0066cc; text-decoration: none; }

.cover {
    text-align: center;
    padding-top: 1.8in;
    page-break-after: always;
}
.cover h1 {
    font-size: 32pt;
    border: none;
    margin: 0;
    color: #0f172a;
}
.cover .subtitle {
    font-size: 14pt;
    color: #64748b;
    margin: 6pt 0 24pt 0;
}
.cover .meta {
    font-size: 10pt;
    color: #475569;
    line-height: 1.6;
}
.part-divider {
    page-break-before: always;
    text-align: center;
    padding-top: 2.2in;
}
.part-divider h1 {
    font-size: 26pt;
    border: none;
    color: #0066cc;
    margin: 0;
    page-break-before: avoid;
}
.part-divider .subtitle {
    font-size: 12pt;
    color: #64748b;
    margin-top: 8pt;
}
"""


def main():
    if not ENCYCLOPEDIA_DIR.exists():
        print(f"ERROR: {ENCYCLOPEDIA_DIR} not found. Run generate_all.py first.")
        sys.exit(1)

    styles = _make_styles()

    # Collect chapter paths in order
    parts_info: list = []
    chapter_paths: list = []
    for part in PART_ORDER:
        part_dir = ENCYCLOPEDIA_DIR / part
        if not part_dir.exists():
            continue
        chs = sorted(
            part_dir.glob('Chapter_*.md'),
            key=lambda p: int(re.search(r'Chapter_(\d+)', p.name).group(1))
        )
        if not chs:
            continue
        parts_info.append((part, len(chs)))
        for ch in chs:
            chapter_paths.append((part, ch))

    total = len(chapter_paths)
    n_parts = len(parts_info)
    print(f"Found {total} chapters across {n_parts} parts.")
    if total == 0:
        print("ERROR: no chapters found.")
        sys.exit(1)

    # ── Build story ───────────────────────────────────────────────────────────
    story: list = []

    # Cover
    story.append(Spacer(1, 1.5 * inch))
    story.append(Paragraph("OBQ Factor Encyclopedia", styles['cover_title']))
    story.append(Paragraph("Volume II \u2014 Russell 3000 Edition", styles['cover_sub']))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph(
        f"<b>{total}</b> factor chapters across <b>{n_parts}</b> parts<br/><br/>"
        f"Universe: Russell 3000 (PIT, survivorship-bias free)<br/>"
        f"Period: 1995-03-31 \u2192 2024-12-31<br/>"
        f"Rebalance: semi-annual \u00b7 Hold: 6 months<br/><br/>"
        f"OBQ Master Score = 25% ICIR + 25% Staircase + 20% AlphaWin + 20% AlphaMag + 10% IC-Hit<br/>"
        f"Staircase Score = Q1-Q5 spread \u00d7 monotonicity \u00d7 step-uniformity<br/><br/>"
        f"Source: OBQ FactorLab \u00b7 CYC-003-GPU batch run",
        styles['cover_meta']
    ))
    story.append(Spacer(1, 1.0 * inch))
    story.append(Paragraph(
        f"Generated {time.strftime('%Y-%m-%d')}",
        ParagraphStyle('Date', parent=styles['cover_meta'], textColor=MUTED, fontSize=9)
    ))
    story.append(PageBreak())

    # INDEX
    idx_path = ENCYCLOPEDIA_DIR / 'INDEX.md'
    if idx_path.exists():
        print("  Adding INDEX...")
        story.extend(_md_to_flowables(idx_path.read_text(encoding='utf-8'), styles))
        story.append(PageBreak())

    # Leaderboard
    lb_path = ENCYCLOPEDIA_DIR / 'Leaderboard.md'
    if lb_path.exists():
        print("  Adding Leaderboard...")
        story.extend(_md_to_flowables(lb_path.read_text(encoding='utf-8'), styles))
        story.append(PageBreak())

    # Chapter 1 — Database & Methodology (always first chapter regardless of Part)
    ch1_path = ENCYCLOPEDIA_DIR / 'Chapter_01_Database_and_Methodology.md'
    if ch1_path.exists():
        print("  Adding Chapter 1: Database & Methodology...")
        story.extend(_md_to_flowables(ch1_path.read_text(encoding='utf-8'), styles))
        story.append(PageBreak())

    # Chapters with part dividers
    current_part = None
    parts_dict = dict(parts_info)
    for idx, (part, ch_path) in enumerate(chapter_paths, start=1):
        if part != current_part:
            pretty = part.replace('_', ' ')
            story.append(Spacer(1, 2.0 * inch))
            story.append(Paragraph(pretty, styles['part_title']))
            story.append(Paragraph(f"{parts_dict[part]} chapters", styles['part_sub']))
            story.append(PageBreak())
            current_part = part

        if idx % 20 == 0 or idx == 1 or idx == total:
            print(f"  Processing chapter {idx}/{total}: {ch_path.name}")
        story.extend(_md_to_flowables(ch_path.read_text(encoding='utf-8'), styles))
        story.append(PageBreak())

    # ── Write PDF ─────────────────────────────────────────────────────────────
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime('%Y%m%d_%H%M%S')
    pdf_path = DOWNLOADS / f'OBQ_Encyclopedia_v2_{ts}.pdf'

    print(f"\nWriting PDF to: {pdf_path}")
    print("(Rendering 91 chapters + tables, ~30-90 seconds...)")
    t0 = time.time()

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=letter,
        leftMargin=0.6*inch, rightMargin=0.6*inch,
        topMargin=0.65*inch, bottomMargin=0.65*inch,
        title="OBQ Factor Encyclopedia v2",
        author="OBQ FactorLab",
    )
    doc.build(
        story,
        onFirstPage=lambda c, d: None,
        onLaterPages=_page_decorator,
    )
    elapsed = time.time() - t0
    sz = pdf_path.stat().st_size / 1e6

    print(f"\n{'='*60}")
    print(f"PDF written: {pdf_path}")
    print(f"  Size:   {sz:.1f} MB")
    print(f"  Time:   {elapsed:.1f}s")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
