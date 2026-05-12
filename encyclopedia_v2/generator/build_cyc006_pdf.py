# -*- coding: utf-8 -*-
"""Build a standalone PDF of just the CYC-006 Rebalance Timing Study."""
import re, sys, time
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, PageBreak,
                                 Table, TableStyle, KeepTogether)
from reportlab.pdfgen import canvas as rl_canvas

CYC006_DIR = Path(r'D:/OBQ_AI/OBQ_Encyclopedia_v2/Part_XV_CYC006_Timing')
DOWNLOADS  = Path.home() / 'Downloads'

ACCENT      = colors.HexColor("#0066cc")
ACCENT_DARK = colors.HexColor("#1e3a8a")
TEXT        = colors.HexColor("#111827")
DIM         = colors.HexColor("#475569")
MUTED       = colors.HexColor("#94a3b8")
BORDER      = colors.HexColor("#cbd5e1")
ROW_ALT     = colors.HexColor("#f8fafc")
TH_BG       = colors.HexColor("#f1f5f9")
QUOTE_BG    = colors.HexColor("#eff6ff")
TIMING_BG   = colors.HexColor("#fef9c3")   # yellow highlight for timing tables

def _make_styles():
    base = getSampleStyleSheet()
    s = {}
    s['cover_title'] = ParagraphStyle('CoverTitle', parent=base['Title'],
        fontName='Helvetica-Bold', fontSize=28, textColor=TEXT, alignment=TA_CENTER,
        spaceAfter=8, leading=34)
    s['cover_sub']  = ParagraphStyle('CoverSub', parent=base['Normal'],
        fontName='Helvetica', fontSize=13, textColor=DIM, alignment=TA_CENTER, spaceAfter=18)
    s['cover_meta'] = ParagraphStyle('CoverMeta', parent=base['Normal'],
        fontName='Helvetica', fontSize=10, textColor=DIM, alignment=TA_CENTER, leading=16)
    s['h1'] = ParagraphStyle('H1', parent=base['Heading1'],
        fontName='Helvetica-Bold', fontSize=16, textColor=TEXT, spaceBefore=6, spaceAfter=8, leading=20)
    s['h2'] = ParagraphStyle('H2', parent=base['Heading2'],
        fontName='Helvetica-Bold', fontSize=12, textColor=ACCENT_DARK, spaceBefore=10, spaceAfter=3)
    s['h3'] = ParagraphStyle('H3', parent=base['Heading3'],
        fontName='Helvetica-Bold', fontSize=10, textColor=TEXT, spaceBefore=6, spaceAfter=2)
    s['body'] = ParagraphStyle('Body', parent=base['Normal'],
        fontName='Helvetica', fontSize=9, textColor=TEXT, leading=12, spaceBefore=2, spaceAfter=3)
    s['meta'] = ParagraphStyle('Meta', parent=base['Normal'],
        fontName='Helvetica-Oblique', fontSize=8.5, textColor=DIM, leading=11, spaceAfter=4)
    s['quote'] = ParagraphStyle('Quote', parent=base['Normal'],
        fontName='Helvetica', fontSize=8.5, textColor=ACCENT_DARK, leftIndent=10, rightIndent=6,
        leading=11, spaceBefore=4, spaceAfter=4, backColor=QUOTE_BG, borderPadding=(5, 8, 5, 8))
    s['bullet'] = ParagraphStyle('Bullet', parent=base['Normal'],
        fontName='Helvetica', fontSize=9, textColor=TEXT, leftIndent=12, leading=12, spaceBefore=1)
    s['code'] = ParagraphStyle('Code', parent=base['Normal'],
        fontName='Courier', fontSize=7.5, textColor=TEXT, leftIndent=8, leading=10,
        spaceBefore=2, spaceAfter=2, backColor=colors.HexColor("#f8fafc"))
    s['tbl_cell'] = ParagraphStyle('TblCell', parent=base['Normal'],
        fontSize=7, leading=8.5, textColor=TEXT)
    s['tbl_hdr'] = ParagraphStyle('TblHdr', parent=base['Normal'],
        fontSize=7, leading=8.5, textColor=ACCENT_DARK, fontName='Helvetica-Bold')
    return s

def _inline(text: str) -> str:
    text = re.sub(r'\*\*([^*\n]+)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'(?<!\*)\*([^*\n]+)\*(?!\*)', r'<i>\1</i>', text)
    text = re.sub(r'`([^`\n]+)`', r'<font name="Courier">\1</font>', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    return text

def _parse_table(lines, start):
    rows = []
    i = start
    while i < len(lines):
        l = lines[i].strip()
        if not (l.startswith('|') and l.endswith('|')):
            break
        if re.match(r'^\|[\s\-:|]+\|$', l):
            i += 1; continue
        rows.append([c.strip() for c in l.strip('|').split('|')])
        i += 1
    return rows, i

def _table_flowable(rows, styles):
    if not rows: return None
    n_cols = max(len(r) for r in rows)
    page_w = letter[0] - 1.2*inch
    col_w  = page_w / n_cols
    parsed = []
    for ri, row in enumerate(rows):
        nr = []
        for cell in row:
            cell = cell.replace('<br/>',' ').replace('<br>',' ')
            st = styles['tbl_hdr'] if ri == 0 else styles['tbl_cell']
            nr.append(Paragraph(_inline(cell), st))
        while len(nr) < n_cols:
            nr.append(Paragraph('', styles['tbl_cell']))
        parsed.append(nr)
    t = Table(parsed, colWidths=[col_w]*n_cols, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0), TH_BG),
        ('LINEBELOW',     (0,0),(-1,0), 0.8, MUTED),
        ('LINEBELOW',     (0,1),(-1,-1), 0.3, BORDER),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [colors.white, ROW_ALT]),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
        ('LEFTPADDING',   (0,0),(-1,-1), 4),
        ('RIGHTPADDING',  (0,0),(-1,-1), 4),
        ('TOPPADDING',    (0,0),(-1,-1), 2),
        ('BOTTOMPADDING', (0,0),(-1,-1), 2),
    ]))
    return t

def _md_to_flowables(md_text, styles):
    out = []
    lines = md_text.split('\n')
    i = 0
    while i < len(lines):
        s = lines[i].strip()
        if not s: i += 1; continue
        if s.startswith('# '):
            out.append(Paragraph(_inline(s[2:]), styles['h1'])); i += 1; continue
        if s.startswith('## '):
            out.append(Paragraph(_inline(s[3:]), styles['h2'])); i += 1; continue
        if s.startswith('### '):
            out.append(Paragraph(_inline(s[4:]), styles['h3'])); i += 1; continue
        if s in ('---','***','___'):
            out.append(Spacer(1,3)); i += 1; continue
        if s.startswith('>'):
            qlines = []
            while i < len(lines) and lines[i].lstrip().startswith('>'):
                qlines.append(lines[i].lstrip().lstrip('>').strip()); i += 1
            out.append(Paragraph(_inline(' '.join(qlines)), styles['quote'])); continue
        if s.startswith('*') and s.endswith('*') and len(s)>2 and not s.startswith('**'):
            out.append(Paragraph(_inline(s[1:-1]), styles['meta'])); i += 1; continue
        if s.startswith('```'):
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('```'):
                code_lines.append(lines[i]); i += 1
            i += 1
            for cl in code_lines:
                out.append(Paragraph(cl.replace(' ','&nbsp;'), styles['code']))
            continue
        if s.startswith('|') and s.endswith('|'):
            rows, new_i = _parse_table(lines, i)
            t = _table_flowable(rows, styles)
            if t: out.append(KeepTogether(t)); out.append(Spacer(1,4))
            i = new_i; continue
        if s.startswith('- ') or s.startswith('* '):
            while i < len(lines) and (lines[i].lstrip().startswith('- ') or lines[i].lstrip().startswith('* ')):
                item = lines[i].lstrip()[2:].strip()
                out.append(Paragraph(f'\u2022 {_inline(item)}', styles['bullet'])); i += 1
            continue
        out.append(Paragraph(_inline(s), styles['body'])); i += 1
    return out

def _page_decorator(c, doc):
    if doc.page <= 1: return
    c.saveState()
    c.setFont('Helvetica', 7); c.setFillColor(MUTED)
    c.drawRightString(letter[0]-0.6*inch, letter[1]-0.4*inch, 'OBQ FactorLab  \u00b7  CYC-006 Rebalance Timing Research')
    c.setFont('Helvetica', 8); c.setFillColor(DIM)
    c.drawCentredString(letter[0]/2, 0.4*inch, f'CYC-006 Rebalance Timing Study  \u00b7  Page {doc.page}')
    c.restoreState()

def main():
    chapters = sorted(CYC006_DIR.glob("Chapter_*.md"),
                       key=lambda p: int(re.search(r'Chapter_(\d+)', p.name).group(1)))
    if not chapters:
        print(f"No chapters found in {CYC006_DIR}")
        sys.exit(1)

    print(f"Found {len(chapters)} chapters")
    styles = _make_styles()
    story  = []

    # Cover
    story.append(Spacer(1, 1.3*inch))
    story.append(Paragraph("OBQ Factor Encyclopedia v2", styles['cover_title']))
    story.append(Paragraph("CYC-006: Rebalance Timing Research", styles['cover_sub']))
    story.append(Paragraph(
        "The Definitive Study of When to Rebalance<br/><br/>"
        "139 factors tested across 10 timing variants<br/>"
        "Russell 3000 Universe  \u00b7  1995\u20132024  \u00b7  60 semi-annual baseline periods<br/><br/>"
        "Key Finding: Annual June 30 rebalancing outperforms the<br/>"
        "standard semi-annual Jun/Dec baseline for the most factors",
        styles['cover_meta']))
    story.append(Spacer(1, 1.0*inch))
    story.append(Paragraph(f"Generated {time.strftime('%Y-%m-%d')}  \u00b7  Part XV of Encyclopedia v2",
        ParagraphStyle('Date', parent=styles['cover_meta'], textColor=MUTED, fontSize=9)))
    story.append(PageBreak())

    # Chapters
    for i, ch_path in enumerate(chapters, 1):
        print(f"  Processing chapter {i}/{len(chapters)}: {ch_path.name}")
        story.extend(_md_to_flowables(ch_path.read_text(encoding='utf-8'), styles))
        story.append(PageBreak())

    # Build PDF
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime('%Y%m%d_%H%M%S')
    pdf_path = DOWNLOADS / f'OBQ_CYC006_Rebalance_Timing_Study_{ts}.pdf'

    print(f"\nBuilding PDF: {pdf_path}")
    t0 = time.time()
    doc = SimpleDocTemplate(str(pdf_path), pagesize=letter,
                             leftMargin=0.6*inch, rightMargin=0.6*inch,
                             topMargin=0.65*inch, bottomMargin=0.65*inch,
                             title="OBQ CYC-006 Rebalance Timing Research",
                             author="OBQ FactorLab")
    doc.build(story, onFirstPage=lambda c,d: None, onLaterPages=_page_decorator)
    elapsed = time.time()-t0
    sz = pdf_path.stat().st_size/1e6

    print(f"\n{'='*60}")
    print(f"PDF written: {pdf_path}")
    print(f"  Size:    {sz:.1f} MB")
    print(f"  Time:    {elapsed:.1f}s")
    print(f"  Chapters: {len(chapters)}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
