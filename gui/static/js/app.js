// OBQ Factor Lab — app.js  (Options Scanner skin)

// ── Theme ───────────────────────────────────────────────────────────────
localStorage.removeItem("fl-theme");  // always start light
let _theme = "";
document.body.setAttribute("data-theme", "");
function toggleTheme() {
  _theme = (_theme === "") ? "dark" : (_theme === "dark" ? "night" : "");
  document.body.setAttribute("data-theme", _theme);
}

// ── Screenshot (dev tool — no save, display only) ─────────────────────
function snapScreen() {
  const btn = document.querySelector(".nav-snap-btn");
  btn.textContent = "...";
  fetch("/api/snap").then(r => r.json()).then(d => {
    btn.textContent = "SNAP";
    if (d.img) {
      const w = window.open("", "_blank", "width=1200,height=800");
      w.document.write(`<html><body style="margin:0;background:#111"><img src="data:image/png;base64,${d.img}" style="max-width:100%;display:block"/></body></html>`);
    }
  }).catch(() => { btn.textContent = "SNAP"; });
}

// ── Init ─────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("cfg-end").valueAsDate = new Date();
  checkGPU();
  loadDateRange();
  setTimeout(startSPY, 500);
});



async function checkGPU() {
  document.getElementById("gpu-indicator").textContent = "GPU RTX 3090";
  document.getElementById("gpu-indicator").style.color = "var(--accent)";
}

async function loadDateRange() {
  const f = document.getElementById("cfg-factor").value;
  const r = await fetch(`/api/config/factor_range?factor=${f}`).then(r=>r.json()).catch(()=>null);
  if (r?.min_date) {
    document.getElementById("cfg-start").value = r.min_date;
    document.getElementById("status-bar").textContent =
      `Factor "${f}" available ${r.min_date} → ${r.max_date}  (${(r.symbols||0).toLocaleString()} symbols)`;
  }
}

document.getElementById("cfg-factor").addEventListener("change", loadDateRange);

function onModeChange() {
  const mode = document.getElementById("cfg-mode").value;
  document.getElementById("fld-topn").style.display  = mode === "topn"     ? "" : "none";
  document.getElementById("fld-bins").style.display  = mode === "quintile" ? "" : "none";
}

// ── Tab switching (stub — layout is always the same split view) ──────────
function switchTab(tab) {
  document.querySelectorAll(".view-tab").forEach(t => t.classList.remove("active"));
  document.querySelector(`[data-tab="${tab}"]`)?.classList.add("active");
  // Could filter table by type in future
}

// ── Run registry ──────────────────────────────────────────────────────────
let _runs = [];       // [{run_id, run_label, status, cfg, metrics}]
let _sortKey = "cagr";
let _sortDir = -1;    // -1 = desc
let _activeRunId = null;

// ── Build config from form ────────────────────────────────────────────────
function buildCfg() {
  const costs = parseFloat(document.getElementById("cfg-costs").value) || 15;
  return {
    model_type: document.getElementById("cfg-mode").value,
    factor:     document.getElementById("cfg-factor").value,
    index: "OBQ Investable Universe (Top 3000)",
    start_date: document.getElementById("cfg-start").value,
    end_date:   document.getElementById("cfg-end").value || null,
    rebalance:  document.getElementById("cfg-rebal").value,
    top_n:      parseInt(document.getElementById("cfg-topn").value) || 30,
    n_quintiles:parseInt(document.getElementById("cfg-bins").value) || 5,
    min_price:  parseFloat(document.getElementById("cfg-minprice").value) || 2.0,
    commission_bps: costs / 2,
    slippage_bps:   costs / 2,
    na_handling: "Exclude",
    winsorize: true,
    rf_annual: 0.04,
    initial_capital: 1000000,
  };
}

// ── Start a run ───────────────────────────────────────────────────────────
async function startRun(overrides = {}) {
  const cfg = { ...buildCfg(), ...overrides };
  const btn = document.getElementById("run-btn");
  btn.disabled = true; btn.textContent = "⌛";
  setStatus("Launching backtest...");
  setProgress(5);

  const resp = await fetch("/api/backtest/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(cfg),
  }).then(r=>r.json()).catch(e => ({error:String(e)}));

  if (resp.error) {
    setStatus("Error: " + resp.error);
    btn.disabled = false; btn.textContent = "▷ RUN";
    return;
  }

  const run_id = resp.run_id;
  const label = `${cfg.model_type === "topn" ? "TOP-"+cfg.top_n : "Q"+cfg.n_quintiles} · ${cfg.factor.toUpperCase()} · ${cfg.start_date.slice(0,7)}`;
  const runObj = { run_id, run_label: label, status: "running", cfg, metrics: null, result: null };
  _runs.unshift(runObj);
  renderTable();
  setActiveRow(run_id);

  // SSE log → progress
  const sse = new EventSource(`/api/backtest/stream/${run_id}`);
  let pct = 10;
  sse.onmessage = e => {
    try {
      const d = JSON.parse(e.data);
      if (d.msg) { setStatus(d.msg); pct = Math.min(pct + 3, 85); setProgress(pct); }
    } catch {}
  };
  sse.onerror = () => sse.close();

  // Poll for completion
  const poll = async () => {
    const info = await fetch(`/api/backtest/status/${run_id}`).then(r=>r.json()).catch(()=>null);
    if (!info || info.status === "running") { setTimeout(poll, 2000); return; }
    sse.close();
    const result = await fetch(`/api/backtest/result/${run_id}`).then(r=>r.json()).catch(()=>null);
    const run = _runs.find(r=>r.run_id === run_id);
    if (run) {
      run.status = result?.status === "error" ? "error" : "complete";
      run.result = result;
      run.metrics = extractMetrics(result);
    }
    renderTable();
    setProgress(100);
    setTimeout(() => setProgress(0), 1000);
    btn.disabled = false; btn.textContent = "▷ RUN";
    setStatus(result?.status === "error" ? "Error: " + result.error : `Done in ${result?.elapsed_s}s`);
    if (result?.status !== "error" && _activeRunId === run_id) showTearsheet(run_id);
  };
  setTimeout(poll, 3000);
}

// ── Auto-run SPY benchmark ────────────────────────────────────────────────
function autoRun() {
  startSPY();
}

let _spyLoaded = false;
async function startSPY() {
  if (_spyLoaded) return;  // prevent double-run
  const btn = document.getElementById("run-btn");
  if (btn) { btn.disabled = true; btn.textContent = "⌛"; }
  setStatus("Loading SPY benchmark..."); setProgress(10);

  // Use preloaded cache — poll until ready (server computes at startup)
  const pollPreloaded = async () => {
    const resp = await fetch("/api/backtest/spy_preloaded").then(r=>r.json()).catch(()=>null);
    if (!resp || resp.status === "loading") {
      setProgress(Math.min(parseInt(document.getElementById("prog-fill").style.width||10)+10, 85));
      setTimeout(pollPreloaded, 2000);
      return;
    }
    if (btn) { btn.disabled = false; btn.textContent = "▷ RUN"; }
    if (resp.status === "error") { setStatus("SPY error: " + resp.error); return; }

    // Inject as a completed run
    const run_id = "spy-benchmark";
    // Remove any existing SPY entry
    const existing = _runs.findIndex(r => r.run_id === run_id);
    if (existing >= 0) _runs.splice(existing, 1);

    const result = { ...resp, mode: "spy", run_id };
    const runObj = {
      run_id,
      run_label: "SPY Long-Only Benchmark",
      status: "complete",
      cfg: {},
      result,
      metrics: extractMetrics(result),
    };
    _runs.unshift(runObj);
    renderTable();
    setProgress(100);
    setTimeout(() => setProgress(0), 800);
    const m = runObj.metrics;
    setStatus("SPY B&H  " + result.start_date + " to " + result.end_date + "  CAGR " + ((m?.cagr||0)*100).toFixed(1) + "%  Sharpe " + (m?.sharpe||0).toFixed(2));
    _spyLoaded = true;
    setActiveRow(run_id);
    showTearsheet(run_id);
  };

  pollPreloaded();
}

// ── Extract flat metrics from result ─────────────────────────────────────
function extractMetrics(result) {
  if (!result) return {};
  const mode = result.mode;
  if (mode === "topn" || mode === "spy") {
    const pm = result.portfolio_metrics || {};
    return { ...pm, elapsed_s: result.elapsed_s, mode };
  } else {
    const q1 = (result.quintile_metrics||{})["Q1"] || {};
    return { ...q1, elapsed_s: result.elapsed_s, mode,
      obsidian: result.obsidian_score?.total };
  }
}

// ── Table rendering ────────────────────────────────────────────────────────
function sortBy(key) {
  if (_sortKey === key) _sortDir *= -1;
  else { _sortKey = key; _sortDir = -1; }
  // Update arrow indicators
  document.querySelectorAll(".fl-th .fl-sort-arrow").forEach(a => a.textContent = "");
  const th = document.querySelector(`[onclick="sortBy('${key}')"] .fl-sort-arrow`);
  if (th) th.textContent = _sortDir === -1 ? "▼" : "▲";
  renderTable();
}

function renderTable() {
  const body = document.getElementById("tbl-body");
  const empty = document.getElementById("tbl-empty");
  if (_runs.length === 0) { if (empty) empty.style.display="block"; return; }
  if (empty) empty.style.display = "none";

  const sorted = [..._runs].sort((a,b) => {
    // SPY always pins to top
    const aSpy = a.run_id && a.run_id.startsWith("spy-");
    const bSpy = b.run_id && b.run_id.startsWith("spy-");
    if (aSpy && !bSpy) return -1;
    if (bSpy && !aSpy) return 1;
    const av = (a.metrics || {})[_sortKey] ?? a[_sortKey];
    const bv = (b.metrics || {})[_sortKey] ?? b[_sortKey];
    if (av == null) return 1; if (bv == null) return -1;
    return _sortDir * (bv - av);
  });

  body.innerHTML = "";
  sorted.forEach(run => {
    const m = run.metrics || {};
    const isActive = run.run_id === _activeRunId;
    const isRunning = run.status === "running";
    const cagr = m.cagr;
    const rowCls = isRunning ? "" : (cagr == null ? "" : cagr >= 0 ? "pos" : "neg");
    const tr = document.createElement("div");
    tr.className = `fl-tr ${rowCls} ${isActive ? "active" : ""}`;
    tr.onclick = () => { setActiveRow(run.run_id); showTearsheet(run.run_id); };
    const icon = isRunning ? "⌛ " : (run.status === "error" ? "✗ " : "✓ ");
    tr.innerHTML = `
      <div class="fl-td dim" style="flex:0 0 80px">${icon}${run.run_label.split(" · ")[0]}</div>
      <div class="fl-td ${cagr>=0?'g':'r'}" style="flex:0 0 60px">${cagr!=null ? (cagr*100).toFixed(1)+"%" : "—"}</div>
      <div class="fl-td" style="flex:0 0 55px">${m.sharpe!=null ? (+m.sharpe).toFixed(2) : "—"}</div>
      <div class="fl-td r" style="flex:0 0 60px">${m.max_dd!=null ? (m.max_dd*100).toFixed(1)+"%" : "—"}</div>
      <div class="fl-td" style="flex:0 0 55px">${m.calmar!=null ? (+m.calmar).toFixed(2) : "—"}</div>
      <div class="fl-td" style="flex:0 0 55px">${m.sortino!=null ? (+m.sortino).toFixed(2) : "—"}</div>
      <div class="fl-td ${(m.alpha||0)>=0?'g':'r'}" style="flex:0 0 55px">${m.alpha!=null ? (m.alpha*100).toFixed(1)+"%" : "—"}</div>
      <div class="fl-td" style="flex:0 0 55px">${m.info_ratio!=null ? (+m.info_ratio).toFixed(2) : "—"}</div>
      <div class="fl-td dim" style="flex:0 0 50px">${run.metrics?.elapsed_s!=null ? run.metrics.elapsed_s+"s" : "—"}</div>
      <div class="fl-td dim" style="flex:1">${run.run_label.split(" · ").slice(1).join(" · ")}</div>
    `;
    body.appendChild(tr);
  });
}

function setActiveRow(run_id) {
  _activeRunId = run_id;
  renderTable();
}

// ── Show tearsheet ────────────────────────────────────────────────────────
function showTearsheet(run_id) {
  const run = _runs.find(r => r.run_id === run_id);
  if (!run || !run.result || run.result.status === "error") return;
  _renderTearsheet(run_id);
}

function _renderTearsheet(run_id) {
  const run = _runs.find(r => r.run_id === run_id);
  if (!run || !run.result) return;

  const tsEmpty = document.getElementById("ts-empty");
  const content  = document.getElementById("ts-content");
  tsEmpty.style.display = "none";
  content.classList.add("active");
  content.innerHTML = "";

  const result = run.result;
  const m      = run.metrics || {};
  const bm     = result.bm_metrics || {};
  const isSpy  = result.mode === "spy" || result.mode === "topn";
  const equity = result.portfolio_equity || [];
  const dates  = result.equity_dates || [];

  // Unique ID generator for Plotly divs
  let _uid = 0;
  function uid() { return "plt_" + run_id.replace(/[^a-z0-9]/gi,"_") + "_" + (++_uid); }

  // Plotly chart box — returns {box, id}
  function pBox(title, heightPx, wide) {
    const box = el("div","ts-chart-box");
    if (wide) box.style.gridColumn = "1/-1";
    if (title) box.innerHTML = `<h6>${title}</h6>`;
    const d = el("div"); d.id = uid();
    d.style.cssText = `height:${heightPx}px;width:100%`;
    box.appendChild(d);
    return { box, id: d.id };
  }

  function sec(title) {
    const h = el("div","ts-sec-hdr"); h.textContent = title;
    content.appendChild(h);
  }

  // ── 1. HEADER ──────────────────────────────────────────────────────────────
  const hdr = el("div","ts-topbar");
  const hdrL = el("div");
  hdrL.innerHTML = `<div class="ts-name">${run.run_label}</div>
    <div class="ts-meta">${result.start_date} &#8594; ${result.end_date} &nbsp;&middot;&nbsp; ${result.n_periods} monthly periods</div>`;
  const btnWrap = el("div"); btnWrap.style.cssText = "display:flex;gap:6px;align-items:center";
  const pdfBtn = el("button","btn-pdf");
  pdfBtn.textContent = "PDF"; pdfBtn.onclick = () => exportPDF(run);
  const csvBtn = el("button","btn-pdf");
  csvBtn.style.background = "#16a34a";
  csvBtn.textContent = "CSV"; csvBtn.onclick = () => exportCSV(run);
  btnWrap.append(csvBtn, pdfBtn);
  hdr.append(hdrL, btnWrap);
  content.appendChild(hdr);

  // ── 2. KPI STRIP ───────────────────────────────────────────────────────────
  const kpiRow = el("div","ts-kpi-grid");
  const kpis = isSpy ? [
    ["CAGR",     pct(m.cagr),            m.cagr>=0.08?"g":m.cagr>=0?"":"r"],
    ["SHARPE",   num(m.sharpe),           m.sharpe>=1?"g":m.sharpe>=0.5?"":"r"],
    ["MAX DD",   pct(m.max_dd),           "r"],
    ["SORTINO",  num(m.sortino),          m.sortino>=1?"g":""],
    ["CALMAR",   num(m.calmar),           m.calmar>=0.5?"g":""],
    ["WIN RATE", pct(m.win_rate_monthly), m.win_rate_monthly>=0.55?"g":""],
  ] : [
    ["Q1 CAGR",  pct(m.cagr),            m.cagr>=0?"g":"r"],
    ["SHARPE",   num(m.sharpe),           ""],
    ["MAX DD",   pct(m.max_dd),           "r"],
    ["ICIR",     num(m.icir,3),           "a"],
    ["IC HIT",   pct(m.ic_hit_rate),      ""],
    ["WIN RATE", pct(m.win_rate_monthly), ""],
  ];
  kpis.forEach(([l,v,c]) => {
    const b = el("div","ts-kpi");
    b.innerHTML = `<div class="ts-kpi-lbl">${l}</div><div class="ts-kpi-val ${c}">${v}</div>`;
    kpiRow.appendChild(b);
  });
  content.appendChild(kpiRow);

  // 3. EQUITY + DRAWDOWN (joined subplot, full width)
  sec("EQUITY CURVE & DRAWDOWN");
  const eqRow = el("div","ts-charts wide"); content.appendChild(eqRow);
  const {box:eqBox, id:eqId} = pBox("", 380, true);
  eqRow.appendChild(eqBox);

  // ── 4. HEATMAP ──────────────────────────────────────────────────────────────
  if (result.monthly_heatmap) {
    sec("MONTHLY RETURN HEATMAP");
    const hw = el("div"); hw.style.padding = "8px 12px";
    hw.appendChild(buildHeatmapTable(result.monthly_heatmap));
    content.appendChild(hw);
  }

  // ── 5. ANNUAL RETURNS ───────────────────────────────────────────────────────
  let annId = null;
  if (result.annual_ret_by_year?.length) {
    sec("ANNUAL RETURNS");
    const annRow = el("div","ts-charts wide"); content.appendChild(annRow);
    const {box:annBox, id} = pBox("", 180, true);
    annRow.appendChild(annBox); annId = id;
  }

  // ── 6. ROLLING METRICS (Sharpe + Sortino + Vol) ────────────────────────────
  sec("ROLLING METRICS");
  const rRow = el("div","ts-charts"); content.appendChild(rRow);
  const {box:rsBox, id:rsId} = pBox("ROLLING 12-MO SHARPE", 180);
  rRow.appendChild(rsBox);
  const {box:rstBox, id:rstId} = pBox("ROLLING 12-MO SORTINO", 180);
  rRow.appendChild(rstBox);

  sec("ROLLING VOLATILITY & DISTRIBUTION");
  const rRow2 = el("div","ts-charts"); content.appendChild(rRow2);
  const {box:rvBox, id:rvId} = pBox("ROLLING 12-MO VOLATILITY", 180);
  rRow2.appendChild(rvBox);
  let distId = null;
  if (result.period_data?.length) {
    const monthlyRets = result.period_data.map(d => d.portfolio_return || 0);
    const {box:dBox, id:dId} = pBox("MONTHLY RETURN DISTRIBUTION", 180);
    rRow2.appendChild(dBox); distId = dId;
    rRow2._monthlyRets = monthlyRets;
  }

  // 6b. DRAWDOWN DEEP DIVE TABLE
  if (dates.length > 0 && equity.length > 0) {
    sec("DRAWDOWN ANALYSIS - TOP PERIODS");
    const ddWrap = el("div"); ddWrap.style.cssText = "padding:8px 12px;flex-shrink:0";
    ddWrap.appendChild(buildDrawdownTable(dates, equity, 20));
    content.appendChild(ddWrap);
  }

  // ── 7. FULL METRICS TABLE ───────────────────────────────────────────────────
  sec("PERFORMANCE METRICS");
  const mGrid = el("div","ts-mets-grid");

  function mrow(label, val, cls) {
    const r = el("div","ts-met-row");
    r.innerHTML = `<span class="ts-met-lbl">${label}</span><span class="ts-met-val ${cls||""}">${val}</span>`;
    mGrid.appendChild(r);
  }
  function msec(title) {
    const r = el("div","ts-met-sec"); r.textContent = title;
    mGrid.appendChild(r);
  }
  const g = v => v >= 0 ? "g" : "r";

  msec("PERIOD");
  mrow("Start Date",         result.start_date||"—");
  mrow("End Date",           result.end_date||"—");
  mrow("Years",              m.n_years!=null?num(m.n_years):"—");
  mrow("Periods (mo)",       result.n_periods||"—");

  msec("RETURNS");
  mrow("CAGR",               pct(m.cagr),              g(m.cagr));
  mrow("Total Return",       pct(m.total_return),       g(m.total_return));
  mrow("Expected Monthly",   pct(m.expected_monthly),   g(m.expected_monthly));
  mrow("Expected Annual",    m.expected_annual!=null?pct(m.expected_annual):"—", g(m.expected_annual));
  mrow("Ann. Volatility",    pct(m.ann_vol));
  mrow("Exposure",           m.exposure!=null?pct(m.exposure):"—");

  msec("RISK-ADJUSTED");
  mrow("Sharpe Ratio",       num(m.sharpe),             m.sharpe>=1?"g":m.sharpe>=0.5?"":"r");
  mrow("Smart Sharpe",       num(m.smart_sharpe));
  mrow("Sortino Ratio",      num(m.sortino),            m.sortino>=1?"g":"");
  mrow("Smart Sortino",      m.smart_sortino!=null?num(m.smart_sortino):"—");
  mrow("Calmar Ratio",       num(m.calmar),             m.calmar>=0.5?"g":"");
  mrow("MAR Ratio",          m.mar_ratio!=null?num(m.mar_ratio):"—", m.mar_ratio>=0.5?"g":"");
  mrow("Omega Ratio",        num(m.omega),              m.omega>=1?"g":"");
  mrow("Gain/Pain Ratio",    m.gain_pain_ratio!=null?num(m.gain_pain_ratio):"—");
  mrow("Serenity Ratio",     num(m.serenity));
  mrow("Pain Ratio",         num(m.pain_ratio));
  mrow("Recovery Factor",    num(m.recovery_factor),    m.recovery_factor>=1?"g":"r");
  mrow("SystemScore",        m.system_score!=null?num(m.system_score):"—", m.system_score>=1?"g":"");
  mrow("K-Ratio",            num(m.k_ratio));
  mrow("Prob. Sharpe",       pct(m.psr),                m.psr>=0.95?"g":"");

  msec("OBQ SUREFIRE SUITE");
  mrow("IUDR",               m.iudr!=null?num(m.iudr):"—",           m.iudr>=10?"g":m.iudr>=1?"":"r");
  mrow("Surefire Ratio",     m.surefire_ratio!=null?num(m.surefire_ratio):"—", m.surefire_ratio>=10?"g":m.surefire_ratio>=1?"":"r");
  mrow("Integrated DD",      m.integrated_dd!=null?num(m.integrated_dd,4):"—");
  mrow("Integrated Upside",  m.integrated_up!=null?num(m.integrated_up,4):"—", "g");

  msec("RISK");
  mrow("Max Drawdown",       pct(m.max_dd),             "r");
  mrow("Avg Drawdown",       pct(m.avg_dd),             "r");
  mrow("Ulcer Index",        num(m.ulcer_index,3));
  mrow("Pain Index",         num(m.pain_index,3));
  mrow("Lake Ratio",         num(m.lake_ratio,3));
  mrow("VaR 95%",            pct(m.var_95),             "r");
  mrow("CVaR 95%",           pct(m.cvar_95),            "r");
  mrow("VaR 99%",            pct(m.var_99),             "r");
  mrow("CVaR 99%",           pct(m.cvar_99),            "r");

  msec("DISTRIBUTION");
  mrow("Skewness",           num(m.skewness));
  mrow("Kurtosis",           num(m.kurtosis));
  mrow("Tail Ratio",         num(m.tail_ratio));
  mrow("Best Month",         pct(m.best_month),         "g");
  mrow("Worst Month",        pct(m.worst_month),        "r");
  mrow("Best Year",          m.best_year!=null?pct(m.best_year):"—","g");
  mrow("Worst Year",         m.worst_year!=null?pct(m.worst_year):"—","r");

  msec("WIN / LOSS");
  mrow("Win Rate (Mo)",      pct(m.win_rate_monthly),   m.win_rate_monthly>=0.55?"g":"");
  mrow("Win Rate (Qtr)",     m.win_rate_quarterly!=null?pct(m.win_rate_quarterly):"—");
  mrow("Win Rate (Yr)",      m.win_rate_yearly!=null?pct(m.win_rate_yearly):"—");
  mrow("Avg Up Month",       pct(m.avg_up_month),       "g");
  mrow("Avg Down Month",     pct(m.avg_down_month),     "r");
  mrow("Payoff Ratio",       num(m.payoff_ratio),       m.payoff_ratio>=1.5?"g":"");
  mrow("Profit Factor",      num(m.profit_factor),      m.profit_factor>=1?"g":"r");
  mrow("Common Sense",       num(m.common_sense_ratio));
  mrow("CPC Index",          num(m.cpc_index));
  mrow("Max Consec. Wins",   m.max_consec_wins!=null?m.max_consec_wins:"—");
  mrow("Max Consec. Losses", m.max_consec_losses!=null?m.max_consec_losses:"—","r");
  mrow("Exposure",           m.exposure!=null?pct(m.exposure):"—");

  msec("STATISTICAL");
  mrow("Sharpe t-stat",      num(m.sharpe_tstat));
  mrow("Haircut Sharpe",     num(m.haircut_sharpe));
  mrow("Sharpe 95% CI",      m.sharpe_ci_95?`[${num(m.sharpe_ci_95[0])}, ${num(m.sharpe_ci_95[1])}]`:"—");
  mrow("Jarque-Bera p",      m.jarque_bera_p!=null?m.jarque_bera_p.toFixed(4):"—");

  msec("VS BENCHMARK");
  mrow("Alpha (Ann)",        bm.alpha!=null?pct(bm.alpha):"—",       bm.alpha>=0?"g":"r");
  mrow("Beta",               bm.beta!=null?num(bm.beta):"—");
  mrow("Info Ratio",         bm.info_ratio!=null?num(bm.info_ratio):"—");
  mrow("Tracking Error",     bm.tracking_error!=null?pct(bm.tracking_error):"—");
  mrow("R²",                 bm.r_squared!=null?num(bm.r_squared):"—");
  mrow("Up Capture",         bm.up_capture!=null?pct(bm.up_capture):"—");
  mrow("Down Capture",       bm.down_capture!=null?pct(bm.down_capture):"—");
  mrow("Treynor Ratio",      bm.treynor_ratio!=null?num(bm.treynor_ratio):"—");
  mrow("M²",                 bm.m_squared!=null?pct(bm.m_squared):"—");

  if (result.mode === "quintile") {
    msec("FACTOR METRICS");
    mrow("IC Mean",         m.ic_mean!=null?num(m.ic_mean,4):"—", m.ic_mean>0?"g":"r");
    mrow("ICIR",            m.icir!=null?num(m.icir,3):"—");
    mrow("IC Hit Rate",     m.ic_hit_rate!=null?pct(m.ic_hit_rate):"—");
    mrow("Q1-Q5 Spread",    m.q1q5_spread_cagr!=null?pct(m.q1q5_spread_cagr):"—","g");
    mrow("Portfolio T/O",   m.portfolio_turnover_pct!=null?pct(m.portfolio_turnover_pct):"—");
  }
  content.appendChild(mGrid);

  // Quintile extras
  if (result.mode==="quintile" && result.annual_returns) {
    sec("QUINTILE ANNUAL RETURNS");
    const aw=el("div");aw.style.padding="8px 12px";
    aw.appendChild(buildAnnualTable(result.annual_returns, parseInt(document.getElementById("cfg-bins").value)||5));
    content.appendChild(aw);
  }

  let scId=null, icId2=null;
  if (result.mode==="quintile" && result.sector_analysis?.length) {
    sec("SECTOR ATTRIBUTION");
    const sr=el("div","ts-charts wide"); content.appendChild(sr);
    const {box:sb,id:si}=pBox("",200,true); sr.appendChild(sb); scId=si;
  }
  if (result.mode==="quintile" && result.ic_data?.length) {
    sec("IC SERIES");
    const ir=el("div","ts-charts wide"); content.appendChild(ir);
    const {box:ib,id:ii}=pBox("",160,true); ir.appendChild(ib); icId2=ii;
  }

  // ── Draw all Plotly charts after DOM is rendered ───────────────────────────
  setTimeout(() => {
    try { drawEquityWithDD(eqId, dates, equity); } catch(e) { console.error("equity+dd",e); }
    if (annId && result.annual_ret_by_year?.length)
      try { drawAnnualBars(annId, result.annual_ret_by_year); } catch(e) { console.error("ann",e); }
    try { drawRollingSharpe(rsId, dates, equity); } catch(e) { console.error("rs",e); }
    try { drawRollingSortino(rstId, dates, equity); } catch(e) { console.error("rst",e); }
    try { drawRollingVol(rvId, dates, equity); } catch(e) { console.error("rv",e); }
    if (distId && rRow2._monthlyRets)
      try { drawDistribution(distId, rRow2._monthlyRets); } catch(e) { console.error("dist",e); }
    if (scId) try { drawSectorBar(scId, result.sector_analysis); } catch(e) {}
    if (icId2) try { drawIC(icId2, result.ic_data); } catch(e) {}
  }, 50);
}

// ── CSV export ────────────────────────────────────────────────────────────
async function exportCSV(run) {
  const btn = document.querySelector(".btn-pdf[style*='16a34a']");
  if (btn) { btn.textContent = "..."; btn.disabled = true; }
  const resp = await fetch("/api/export/csv", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ run_id: run.run_id, run_label: run.run_label,
                           result: run.result, metrics: run.metrics }),
  }).then(r=>r.json()).catch(e=>({error:String(e)}));
  if (btn) { btn.textContent = "CSV"; btn.disabled = false; }
  if (resp.path) showToast(`CSV saved: ${resp.filename}`, "success");
  else showToast("CSV failed: "+(resp.error||"unknown"), "error");
}

// ── PDF export ────────────────────────────────────────────────────────────
async function exportPDF(run) {
  const btn = document.querySelector(".btn-pdf:not([style*='16a34a'])");
  if (btn) { btn.textContent = "..."; btn.disabled = true; }
  const resp = await fetch("/api/export/pdf", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ run_id: run.run_id, run_label: run.run_label,
                           result: run.result, metrics: run.metrics }),
  }).then(r=>r.json()).catch(e=>({error:String(e)}));
  if (btn) { btn.textContent = "PDF"; btn.disabled = false; }
  if (resp.path) showToast(`PDF saved to Downloads: ${resp.filename}`, "success");
  else showToast("PDF failed: "+(resp.error||"unknown"), "error");
}

// ── Toast ─────────────────────────────────────────────────────────────────
function showToast(msg, type="info") {
  let tc = document.getElementById("toast-container");
  if (!tc) { tc = document.createElement("div"); tc.id="toast-container"; document.body.appendChild(tc); }
  const t = document.createElement("div");
  t.className = `toast toast-${type}`;
  t.textContent = msg;
  tc.appendChild(t);
  requestAnimationFrame(() => t.classList.add("show"));
  setTimeout(() => { t.classList.remove("show"); setTimeout(()=>t.remove(), 300); }, 4000);
}

// ── UI helpers ────────────────────────────────────────────────────────────
function setStatus(msg) { document.getElementById("status-bar").textContent = msg; }
function setProgress(pct) { document.getElementById("prog-fill").style.width = pct + "%"; }
function el(tag, cls) { const d = document.createElement(tag); if(cls) d.className=cls; return d; }
function makeCanvas(h) { const c=document.createElement("canvas"); c.style.height=h+"px"; c.style.width="100%"; return c; }

function pct(v, d=1) { if(v==null||isNaN(v)) return "—"; return (v*100).toFixed(d)+"%"; }
function num(v, d=2) { if(v==null||isNaN(v)) return "—"; return (+v).toFixed(d); }

function buildMetDefs(m, bm) {
  return [
    ["CAGR",          pct(m.cagr),          m.cagr>=0?"g":"r"],
    ["Total Return",  pct(m.total_return),   m.total_return>=0?"g":"r"],
    ["Ann Volatility",pct(m.ann_vol),        ""],
    ["Sharpe",        num(m.sharpe),         ""],
    ["Smart Sharpe",  num(m.smart_sharpe),   ""],
    ["Sortino",       num(m.sortino),        ""],
    ["Calmar",        num(m.calmar),         ""],
    ["Omega",         num(m.omega),          ""],
    ["Max Drawdown",  pct(m.max_dd),         "r"],
    ["Avg Drawdown",  pct(m.avg_dd),         "r"],
    ["Ulcer Index",   num(m.ulcer_index,3),  ""],
    ["VaR 95",        pct(m.var_95),         "r"],
    ["CVaR 95",       pct(m.cvar_95),        "r"],
    ["Skewness",      num(m.skewness),       ""],
    ["Kurtosis",      num(m.kurtosis),       ""],
    ["Win Rate (mo)", pct(m.win_rate_monthly),""],
    ["Tail Ratio",    num(m.tail_ratio),     ""],
    ["K-Ratio",       num(m.k_ratio),        ""],
    ["PSR",           pct(m.psr),            ""],
    ["Alpha",         m.alpha!=null?pct(m.alpha):"—",   m.alpha>=0?"g":"r"],
    ["Beta",          m.beta!=null?num(m.beta):"—",     ""],
    ["Info Ratio",    m.info_ratio!=null?num(m.info_ratio):"—", ""],
    ["Up Capture",    m.up_capture!=null?pct(m.up_capture):"—", ""],
    ["Down Capture",  m.down_capture!=null?pct(m.down_capture):"—",""],
    ["IC Mean",       m.ic_mean!=null?num(m.ic_mean,4):"—",  m.ic_mean>0?"g":"r"],
    ["ICIR",          m.icir!=null?num(m.icir,3):"—",        ""],
    ["IC Hit Rate",   m.ic_hit_rate!=null?pct(m.ic_hit_rate):"—",""],
  ];
}
