"""PDF generation for OBQ FactorLab tearsheets."""
from pathlib import Path
import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                Paragraph, Spacer, PageBreak)

TEAL  = colors.HexColor("#00c5a8")
WHITE = colors.white
LITE  = colors.HexColor("#1a1a2e")
GREY  = colors.HexColor("#94a3b8")


def _pf(v, d=1):
    if v is None: return "—"
    return f"{float(v)*100:.{d}f}%"

def _nf(v, d=2):
    if v is None: return "—"
    return f"{float(v):.{d}f}"


def generate(result: dict, path: Path) -> None:
    mode = result.get("mode", "topn")
    m    = result.get("portfolio_metrics" if mode=="topn" else "quintile_metrics", {})
    if mode == "quintile":
        m = m.get("Q1", {})
    obs  = result.get("obsidian_score", {})

    doc = SimpleDocTemplate(str(path), pagesize=letter,
                            leftMargin=0.5*inch, rightMargin=0.5*inch,
                            topMargin=0.5*inch, bottomMargin=0.5*inch)

    H1   = ParagraphStyle("H1",   fontName="Helvetica-Bold", fontSize=16, textColor=TEAL, spaceAfter=6)
    H2   = ParagraphStyle("H2",   fontName="Helvetica-Bold", fontSize=10, textColor=TEAL, spaceAfter=4, spaceBefore=12)
    META = ParagraphStyle("META", fontName="Helvetica",      fontSize=8,  textColor=GREY)

    story = []

    # Header
    story.append(Paragraph("OBQ FACTOR LAB", H1))
    story.append(Paragraph(
        f"Factor: {result.get('factor','').upper()}  |  Mode: {mode.upper()}  |  "
        f"{result.get('start_date','')} to {result.get('end_date','')}  |  "
        f"{result.get('n_periods',0)} periods  |  "
        f"Elapsed: {result.get('elapsed_s','')}s", META))
    story.append(Spacer(1, 10))

    # KPI row
    story.append(Paragraph("KEY METRICS", H2))
    headers = ["CAGR","SHARPE","MAX DD","SORTINO","CALMAR","ALPHA","BETA","INFO RATIO"]
    values  = [
        _pf(m.get("cagr")), _nf(m.get("sharpe")), _pf(m.get("max_dd")),
        _nf(m.get("sortino")), _nf(m.get("calmar")),
        _pf(m.get("alpha")), _nf(m.get("beta")), _nf(m.get("info_ratio")),
    ]
    kpi_tbl = Table([headers, values], colWidths=[0.88*inch]*8)
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,0), TEAL),
        ("TEXTCOLOR",     (0,0),(-1,0), WHITE),
        ("FONTNAME",      (0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,-1), 8),
        ("ALIGN",         (0,0),(-1,-1), "CENTER"),
        ("FONTNAME",      (0,1),(-1,1), "Helvetica-Bold"),
        ("FONTSIZE",      (0,1),(-1,1), 11),
        ("ROWBACKGROUNDS",(0,1),(-1,1), [LITE]),
        ("TEXTCOLOR",     (0,1),(-1,1), WHITE),
        ("BOX",           (0,0),(-1,-1), 1, TEAL),
        ("GRID",          (0,0),(-1,-1), 0.3, colors.HexColor("#2a2a4a")),
    ]))
    story.append(kpi_tbl)
    story.append(Spacer(1, 10))

    # Full metrics in 2 columns
    story.append(Paragraph("FULL RISK & RETURN METRICS", H2))
    all_mets = [
        ("CAGR",              _pf(m.get("cagr"))),
        ("Total Return",      _pf(m.get("total_return"))),
        ("Ann Volatility",    _pf(m.get("ann_vol"))),
        ("Sharpe",            _nf(m.get("sharpe"))),
        ("Smart Sharpe",      _nf(m.get("smart_sharpe"))),
        ("Sortino",           _nf(m.get("sortino"))),
        ("Calmar",            _nf(m.get("calmar"))),
        ("Omega",             _nf(m.get("omega"))),
        ("Max Drawdown",      _pf(m.get("max_dd"))),
        ("Avg Drawdown",      _pf(m.get("avg_dd"))),
        ("Ulcer Index",       _nf(m.get("ulcer_index"),3)),
        ("Pain Index",        _nf(m.get("pain_index"),3)),
        ("VaR 95",            _pf(m.get("var_95"))),
        ("CVaR 95",           _pf(m.get("cvar_95"))),
        ("Skewness",          _nf(m.get("skewness"))),
        ("Kurtosis",          _nf(m.get("kurtosis"))),
        ("Win Rate (mo)",     _pf(m.get("win_rate_monthly"))),
        ("Win Rate (yr)",     _pf(m.get("win_rate_yearly"))),
        ("Best Month",        _pf(m.get("best_month"))),
        ("Worst Month",       _pf(m.get("worst_month"))),
        ("Tail Ratio",        _nf(m.get("tail_ratio"))),
        ("Recovery Factor",   _nf(m.get("recovery_factor"))),
        ("K-Ratio",           _nf(m.get("k_ratio"))),
        ("PSR",               _pf(m.get("psr"))),
        ("Sharpe t-stat",     _nf(m.get("sharpe_tstat"))),
        ("Haircut Sharpe",    _nf(m.get("haircut_sharpe"))),
        ("Alpha",             _pf(m.get("alpha"))),
        ("Beta",              _nf(m.get("beta"))),
        ("R-Squared",         _nf(m.get("r_squared"))),
        ("Tracking Error",    _pf(m.get("tracking_error"))),
        ("Info Ratio",        _nf(m.get("info_ratio"))),
        ("Up Capture",        _pf(m.get("up_capture"))),
        ("Down Capture",      _pf(m.get("down_capture"))),
        ("Treynor",           _nf(m.get("treynor_ratio"))),
        ("M-Squared",         _pf(m.get("m_squared"))),
        ("IC Mean",           _nf(m.get("ic_mean"),4)),
        ("ICIR",              _nf(m.get("icir"),3)),
        ("IC Hit Rate",       _pf(m.get("ic_hit_rate"))),
        ("Q1-Q5 Spread",      _pf(m.get("q1q5_spread_cagr"))),
        ("Turnover",          _pf(m.get("portfolio_turnover_pct"))),
    ]
    half = len(all_mets)//2 + len(all_mets)%2
    left, right = all_mets[:half], all_mets[half:]
    rows = []
    for i in range(max(len(left), len(right))):
        l = left[i]  if i < len(left)  else ("","")
        r = right[i] if i < len(right) else ("","")
        rows.append([l[0], l[1], " ", r[0], r[1]])
    t = Table(rows, colWidths=[1.7*inch, 0.9*inch, 0.1*inch, 1.7*inch, 0.9*inch])
    t.setStyle(TableStyle([
        ("FONTNAME",  (0,0),(-1,-1), "Helvetica"),
        ("FONTSIZE",  (0,0),(-1,-1), 8),
        ("TEXTCOLOR", (0,0),(0,-1), colors.HexColor("#555")),
        ("TEXTCOLOR", (3,0),(3,-1), colors.HexColor("#555")),
        ("FONTNAME",  (1,0),(1,-1), "Helvetica-Bold"),
        ("FONTNAME",  (4,0),(4,-1), "Helvetica-Bold"),
        ("ALIGN",     (1,0),(1,-1), "RIGHT"),
        ("ALIGN",     (4,0),(4,-1), "RIGHT"),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[WHITE, colors.HexColor("#f8f9fa")]),
        ("LINEBELOW", (0,0),(-1,-1), 0.25, colors.HexColor("#e5e7eb")),
    ]))
    story.append(t)

    # Obsidian score
    if obs and mode == "quintile":
        story.append(PageBreak())
        story.append(Paragraph(
            f"OBSIDIAN BACKTEST SCORE: {obs.get('total',0):.2f}/5.0  —  {obs.get('rating','')}",
            H2))
        dim_names = {
            "benchmark_win_rate":   "Benchmark Win Rate",
            "quintile_progression": "Quintile Progression",
            "sector_consistency":   "Sector Consistency",
            "absolute_performance": "Absolute Performance",
            "yoy_consistency":      "Year-over-Year Consistency",
        }
        dims = obs.get("dimension_scores", {})
        obs_rows = [["Dimension","Score","Stars"]]
        for k, v in dims.items():
            obs_rows.append([dim_names.get(k,k), f"{v:.2f}", "★"*round(v/0.25)])
        obs_tbl = Table(obs_rows, colWidths=[2.5*inch, 0.8*inch, 1.5*inch])
        obs_tbl.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0), TEAL),
            ("TEXTCOLOR", (0,0),(-1,0), WHITE),
            ("FONTNAME",  (0,0),(-1,0), "Helvetica-Bold"),
            ("FONTSIZE",  (0,0),(-1,-1), 9),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE, colors.HexColor("#f8f9fa")]),
            ("GRID",      (0,0),(-1,-1), 0.5, colors.HexColor("#e5e7eb")),
        ]))
        story.append(obs_tbl)

    doc.build(story)
