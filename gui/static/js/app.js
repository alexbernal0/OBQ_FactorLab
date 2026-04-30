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
  const label = `${cfg.model_type === "topn" ? "TOP-"+cfg.top_n : "Q"+cfg.n_quintiles} · ${cfg.factor.toUpperCase()} · ${cfg.start_date.slice(0,10)}`;
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
    const icon = isRunning ? "~ " : (run.status === "error" ? "x " : "v ");
    const sf = m.surefire_ratio;
    const sfCls = sf!=null?(sf>=10?"g":sf>=1?"":"r"):"";
    tr.innerHTML = `
      <div class="fl-td dim" style="flex:0 0 75px;font-size:10px">${icon}${run.run_label.split(" · ")[0].slice(0,10)}</div>
      <div class="fl-td ${cagr>=0?'g':'r'}" style="flex:0 0 55px">${cagr!=null?(cagr*100).toFixed(1)+"%":"—"}</div>
      <div class="fl-td ${m.sharpe>=1?'g':m.sharpe>=0.5?'':''}" style="flex:0 0 50px">${m.sharpe!=null?(+m.sharpe).toFixed(2):"—"}</div>
      <div class="fl-td r" style="flex:0 0 55px">${m.max_dd!=null?(m.max_dd*100).toFixed(1)+"%":"—"}</div>
      <div class="fl-td ${(m.win_rate_monthly||0)>=0.55?'g':''}" style="flex:0 0 48px">${m.win_rate_monthly!=null?((m.win_rate_monthly)*100).toFixed(0)+"%":"—"}</div>
      <div class="fl-td" style="flex:0 0 48px">${m.sortino!=null?(+m.sortino).toFixed(2):"—"}</div>
      <div class="fl-td" style="flex:0 0 48px">${m.ann_vol!=null?((m.ann_vol)*100).toFixed(1)+"%":"—"}</div>
      <div class="fl-td" style="flex:0 0 48px">${m.calmar!=null?(+m.calmar).toFixed(2):"—"}</div>
      <div class="fl-td ${sfCls}" style="flex:0 0 55px">${sf!=null?(+sf).toFixed(1):"—"}</div>
      <div class="fl-td" style="flex:0 0 48px">${m.omega!=null?(+m.omega).toFixed(2):"—"}</div>
      <div class="fl-td" style="flex:0 0 45px">${m.lake_ratio!=null?(+m.lake_ratio).toFixed(3):"—"}</div>
      <div class="fl-td dim" style="flex:0 0 40px">${m.n_periods||run.result?.n_periods||"—"}</div>
      <div class="fl-td ${(m.profit_factor||0)>=1.5?'g':''}" style="flex:0 0 45px">${m.profit_factor!=null?(+m.profit_factor).toFixed(2):"—"}</div>
      <div class="fl-td ${(m.system_score||0)>=1?'g':''}" style="flex:0 0 50px">${m.system_score!=null?(+m.system_score).toFixed(2):"—"}</div>
      <div class="fl-td ${(m.equity_r2||0)>=0.95?'g':(m.equity_r2||0)>=0.85?'':''}" style="flex:0 0 42px">${m.equity_r2!=null?(+m.equity_r2).toFixed(3):"—"}</div>
      <div class="fl-td dim" style="flex:0 0 38px">${run.metrics?.elapsed_s!=null?run.metrics.elapsed_s+"s":"—"}</div>
      <div class="fl-td dim" style="flex:1;font-size:10px">${run.run_label.split(" · ").slice(1).join(" · ")}</div>
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
  const imgBtn = el("button","btn-pdf");
  imgBtn.style.background = "#7c3aed";
  imgBtn.textContent = "IMG"; imgBtn.onclick = () => exportImage(run);
  btnWrap.append(imgBtn, csvBtn, pdfBtn);
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

  // ── 6. ROLLING RISK METRICS (4 charts in 2x2) ─────────────────────────────
  sec("ROLLING RISK METRICS");
  const rRow = el("div","ts-charts"); content.appendChild(rRow);
  const {box:rcBox, id:rcId} = pBox("ROLLING SHARPE & SORTINO", 180);
  rRow.appendChild(rcBox);
  const {box:rmdBox, id:rmdId} = pBox("ROLLING MAX DRAWDOWN", 180);
  rRow.appendChild(rmdBox);

  const rRow2 = el("div","ts-charts"); content.appendChild(rRow2);
  const {box:rvBox, id:rvId} = pBox("ROLLING ANNUALIZED VOLATILITY", 180);
  rRow2.appendChild(rvBox);
  const {box:rsBox, id:rsId} = pBox("ROLLING 12-MO SORTINO (solo)", 180);
  rRow2.appendChild(rsBox);

  // ── 6b. DISTRIBUTION ANALYSIS ──────────────────────────────────────────────
  // Extract monthly returns from period_data OR equity curve
  let _monthlyRets = null;
  if (result.period_data?.length) {
    _monthlyRets = result.period_data.map(d => d.portfolio_return || 0);
  } else if (equity.length > 1) {
    _monthlyRets = equity.slice(1).map((v,i) => v/(equity[i]||1)-1);
  }

  let distId=null, omegaId=null, acfId=null, pacfId=null;
  if (_monthlyRets) {
    sec("RETURN DISTRIBUTION ANALYSIS");
    const dRow1 = el("div","ts-charts"); content.appendChild(dRow1);
    const {box:dBox, id:dId} = pBox("MONTHLY RETURN DISTRIBUTION", 180);
    dRow1.appendChild(dBox); distId = dId;
    const {box:oBox, id:oId} = pBox("OMEGA RATIO CURVE", 180);
    dRow1.appendChild(oBox); omegaId = oId;

    const dRow2 = el("div","ts-charts"); content.appendChild(dRow2);
    const {box:aBox, id:aId} = pBox("ACF — MONTHLY RETURNS", 180);
    dRow2.appendChild(aBox); acfId = aId;
    const {box:pBox2, id:pId} = pBox("PACF — MONTHLY RETURNS", 180);
    dRow2.appendChild(pBox2); pacfId = pId;
  }

  // ── 6c. NEW PHASE 1 SECTIONS ──────────────────────────────────────────────

  // Trailing returns bar chart
  sec("TRAILING PERIOD RETURNS");
  const trRow = el("div","ts-charts wide"); content.appendChild(trRow);
  const {box:trBox, id:trId} = pBox("", 160, true);
  trRow.appendChild(trBox);

  // Benchmark comparison 2x2 (only when real benchmark)
  let bmCmpId=null, capId=null, scatterId=null, annVsBmId=null;
  const bmEq = result.bm_equity || [];
  const hasDiffBenchmark = bmEq.length > 1 && equity.length > 1 &&
    bmEq[bmEq.length-1] !== equity[equity.length-1];
  if (hasDiffBenchmark) {
    sec("BENCHMARK ANALYSIS");
    const bmRow = el("div","ts-charts wide"); content.appendChild(bmRow);
    const {box:bmBox, id:bmId} = pBox("BENCHMARK COMPARISON (4-PANEL)", 360, true);
    bmRow.appendChild(bmBox); bmCmpId = bmId;

    const bmRow2 = el("div","ts-charts"); content.appendChild(bmRow2);
    const {box:capBox, id:cId} = pBox("UP/DOWN CAPTURE RATIOS", 200);
    bmRow2.appendChild(capBox); capId = cId;
    const {box:scBox, id:sId} = pBox("MONTHLY RETURN SCATTER", 200);
    bmRow2.appendChild(scBox); scatterId = sId;
  }

  // Equity log scale + Seasonality
  sec("ADDITIONAL ANALYSIS");
  const addRow = el("div","ts-charts"); content.appendChild(addRow);
  const {box:logBox, id:logId} = pBox("EQUITY CURVE (LOG SCALE)", 200);
  addRow.appendChild(logBox);
  const {box:seasonBox, id:seasonId} = pBox("SEASONALITY (AVG RETURN BY MONTH)", 200);
  addRow.appendChild(seasonBox);

  // Q-Q Plot + Drawdown duration histogram
  const addRow2 = el("div","ts-charts"); content.appendChild(addRow2);
  let qqId=null, ddHistId=null;
  if (_monthlyRets) {
    const {box:qqBox, id:qqI} = pBox("Q-Q PLOT VS NORMAL", 200);
    addRow2.appendChild(qqBox); qqId = qqI;
  }
  const {box:ddHBox, id:ddHId} = pBox("DRAWDOWN DURATION HISTOGRAM", 200);
  addRow2.appendChild(ddHBox); ddHistId = ddHId;

  // Intra-month DD heatmap (full width)
  sec("INTRA-MONTH MAX DRAWDOWN HEATMAP");
  const imRow = el("div","ts-charts wide"); content.appendChild(imRow);
  const {box:imBox, id:imId} = pBox("", 220, true);
  imRow.appendChild(imBox);

  // Annual performance vs benchmark table
  sec("ANNUAL PERFORMANCE — STRATEGY vs BENCHMARK");
  const annVsWrap = el("div"); annVsWrap.style.cssText = "padding:8px 12px;flex-shrink:0";
  annVsWrap.appendChild(buildAnnualVsBenchmarkTable(dates, equity, hasDiffBenchmark?bmEq:[]));
  content.appendChild(annVsWrap);

  // ── 6d. ACTIVE RETURNS + BEST/WORST ──────────────────────────────────────
  // Always show Best/Worst months table
  sec("BEST / WORST MONTHS");
  const bwWrap = el("div"); bwWrap.style.cssText = "flex-shrink:0";
  bwWrap.appendChild(buildBestWorstTable(dates, equity, 5));
  content.appendChild(bwWrap);

  // Active returns only if real benchmark differs from strategy
  let activeId = null;
  if (hasDiffBenchmark) {
    sec("MONTHLY ACTIVE RETURNS (Strategy - Benchmark)");
    const aRow = el("div","ts-charts wide"); content.appendChild(aRow);
    const {box:arBox, id:arId} = pBox("", 180, true);
    aRow.appendChild(arBox); activeId = arId;
  }

  // ── 6d. CRISIS PERIODS 2x4 GRID ───────────────────────────────────────────
  sec("CRISIS PERIODS ANALYSIS");
  const crisisGrid = el("div");
  crisisGrid.id = "crisis_grid_" + run_id.replace(/[^a-z0-9]/gi,"_");
  crisisGrid.style.cssText = "display:grid;grid-template-columns:repeat(4,1fr);gap:8px;padding:10px 12px;flex-shrink:0";
  content.appendChild(crisisGrid);
  const crisisId = crisisGrid.id;

  // ── 6e. DRAWDOWN TABLE ─────────────────────────────────────────────────────
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
  mrow("Burke Ratio",        m.burke_ratio!=null?num(m.burke_ratio):"—",  m.burke_ratio>=0.5?"g":"");
  mrow("Sterling Ratio",     m.sterling_ratio!=null?num(m.sterling_ratio):"—", m.sterling_ratio>=0.5?"g":"");
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
  mrow("Max DD Duration",    m.max_dd_duration!=null?num(m.max_dd_duration,0)+" mo":"—","r");
  mrow("Avg DD Duration",    m.avg_dd_duration!=null?num(m.avg_dd_duration,1)+" mo":"—","r");
  mrow("Avg Recovery Time",  m.avg_recovery_time!=null?num(m.avg_recovery_time,1)+" mo":"—");
  mrow("% Time in DD",       m.pct_time_in_dd!=null?pct(m.pct_time_in_dd):"—","r");
  mrow("N Drawdowns",        m.n_drawdowns!=null?m.n_drawdowns:"—");
  mrow("CDaR 95%",           m.cdar_95!=null?pct(m.cdar_95):"—","r");
  mrow("Ulcer Index",        num(m.ulcer_index,3));
  mrow("Pain Index",         num(m.pain_index,3));
  mrow("Lake Ratio",         num(m.lake_ratio,3));
  mrow("VaR 95%",            pct(m.var_95),             "r");
  mrow("CVaR 95%",           pct(m.cvar_95),            "r");
  mrow("CF VaR 95%",         m.var_95_cf!=null?pct(m.var_95_cf):"—","r");
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
  mrow("Outlier Win Ratio",  m.outlier_win_ratio!=null?num(m.outlier_win_ratio):"—");
  mrow("Outlier Loss Ratio", m.outlier_loss_ratio!=null?num(m.outlier_loss_ratio):"—","r");
  mrow("Payoff Ratio",       num(m.payoff_ratio),       m.payoff_ratio>=1.5?"g":"");
  mrow("Profit Factor",      num(m.profit_factor),      m.profit_factor>=1?"g":"r");
  mrow("Common Sense",       num(m.common_sense_ratio));
  mrow("CPC Index",          num(m.cpc_index));
  mrow("Max Consec. Wins",   m.max_consec_wins!=null?m.max_consec_wins:"—");
  mrow("Max Consec. Losses", m.max_consec_losses!=null?m.max_consec_losses:"—","r");
  mrow("Exposure",           m.exposure!=null?pct(m.exposure):"—");

  msec("TRAILING RETURNS");
  mrow("1 Month",            m.trailing_1m!=null?pct(m.trailing_1m):     "—", g(m.trailing_1m));
  mrow("3 Months",           m.trailing_3m!=null?pct(m.trailing_3m):     "—", g(m.trailing_3m));
  mrow("6 Months",           m.trailing_6m!=null?pct(m.trailing_6m):     "—", g(m.trailing_6m));
  mrow("1 Year",             m.trailing_1y!=null?pct(m.trailing_1y):     "—", g(m.trailing_1y));
  mrow("3 Year (ann.)",      m.trailing_3y!=null?pct(m.trailing_3y):     "—", g(m.trailing_3y));
  mrow("5 Year (ann.)",      m.trailing_5y!=null?pct(m.trailing_5y):     "—", g(m.trailing_5y));
  mrow("10 Year (ann.)",     m.trailing_10y!=null?pct(m.trailing_10y):   "—", g(m.trailing_10y));

  msec("STATISTICAL");
  mrow("Equity R²",          m.equity_r2!=null?num(m.equity_r2,4):"—", m.equity_r2>=0.95?"g":m.equity_r2>=0.85?"":"r");
  mrow("DSR",                m.dsr!=null?num(m.dsr,4):"—",              m.dsr>=0.95?"g":"");
  mrow("Min TRL (months)",   m.min_trl_months!=null?num(m.min_trl_months,1):"—");
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

  // ── Draw all Plotly charts — two-pass: draw at 200ms, force resize at 1000ms ─
  function _drawAll() {
    // Equity + Drawdown joined
    try { drawEquityWithDD(eqId, dates, equity); } catch(e) { console.error("equity+dd",e); }
    // Annual bars
    if (annId && result.annual_ret_by_year?.length)
      try { drawAnnualBars(annId, result.annual_ret_by_year); } catch(e) {}
    // Rolling combined + max DD + vol + sortino
    try { drawRollingCombined(rcId, dates, equity); } catch(e) { console.error("rc",e); }
    try { drawRollingMaxDD(rmdId, dates, equity); } catch(e) { console.error("rmd",e); }
    try { drawRollingVol(rvId, dates, equity); } catch(e) { console.error("rv",e); }
    try { drawRollingSortino(rsId, dates, equity); } catch(e) { console.error("rs_sortino",e); }
    // Distribution analysis
    if (_monthlyRets) {
      if (distId)   try { drawDistribution(distId, _monthlyRets); } catch(e) { console.error("dist",e); }
      if (omegaId)  try { drawOmegaCurve(omegaId, _monthlyRets); } catch(e) { console.error("omega",e); }
      if (acfId)    try { drawACF(acfId, _monthlyRets, false); } catch(e) { console.error("acf",e); }
      if (pacfId)   try { drawACF(pacfId, _monthlyRets, true); } catch(e) { console.error("pacf",e); }
    }
    // Active returns vs benchmark
    if (activeId && bmEq.length > 1)
      try { drawActiveReturns(activeId, dates, equity, bmEq); } catch(e) { console.error("active",e); }
    // Crisis grid
    try { drawCrisisGrid(crisisId, dates, equity); } catch(e) { console.error("crisis",e); }

    // ── Phase 1 new charts ────────────────────────────────────────────────────
    // Trailing returns
    try { drawTrailingReturns(trId, m); } catch(e) { console.error("trailing",e); }
    // Benchmark comparison (only with real benchmark)
    if (bmCmpId && hasDiffBenchmark)
      try { drawBenchmarkComparison(bmCmpId, dates, equity, bmEq); } catch(e) { console.error("bmcmp",e); }
    if (capId && hasDiffBenchmark)
      try { drawCapture(capId, equity, bmEq, 12); } catch(e) { console.error("capture",e); }
    if (scatterId && hasDiffBenchmark)
      try { drawReturnScatter(scatterId, equity, bmEq); } catch(e) { console.error("scatter",e); }
    // Equity log scale
    try { drawEquityLog(logId, dates, equity); } catch(e) { console.error("logEq",e); }
    // Seasonality
    try { drawSeasonality(seasonId, dates, equity); } catch(e) { console.error("season",e); }
    // Q-Q plot
    if (qqId && _monthlyRets) try { drawQQ(qqId, _monthlyRets); } catch(e) { console.error("qq",e); }
    // Drawdown duration histogram
    try { drawDDDurationHist(ddHistId, dates, equity); } catch(e) { console.error("ddhist",e); }
    // Intra-month DD heatmap
    try { drawIntraMonthDDHeatmap(imId, dates, equity); } catch(e) { console.error("imdd",e); }

    // Quintile-specific
    if (scId) try { drawSectorBar(scId, result.sector_analysis); } catch(e) {}
    if (icId2) try { drawIC(icId2, result.ic_data); } catch(e) {}

    // Multi-pass resize — force Plotly to recalculate after layout settles
    [400, 800, 1500].forEach(delay => {
      setTimeout(() => {
        document.querySelectorAll("#ts-content [id^=plt_]").forEach(d => {
          try { if (d.data) Plotly.Plots.resize(d); } catch(e) {}
        });
      }, delay);
    });
  }

  // 300ms — grid layout should be complete
  setTimeout(_drawAll, 300);
}

// ── Image export — renders tearsheet entirely within app using Plotly.toImage ─
async function exportImage(run) {
  const btn = document.querySelector(".btn-pdf[style*='7c3aed']");
  if (btn) { btn.textContent = "..."; btn.disabled = true; }

  try {
    // 1. Collect all Plotly chart divs in ts-content that have data
    const tsContent = document.getElementById("ts-content");
    const plotDivs = Array.from(tsContent.querySelectorAll("[id^='plt_'], [id^='crisis_']"))
      .filter(d => d.data && d.data.length > 0 && d.offsetHeight > 50);

    // 2. Export each chart to PNG via Plotly.toImage (2x resolution for hi-res)
    const chartImages = [];
    for (const div of plotDivs) {
      try {
        const imgData = await Plotly.toImage(div, {
          format: "png", width: div.offsetWidth * 2, height: div.offsetHeight * 2, scale: 2
        });
        const title = div.previousElementSibling?.querySelector("h6")?.textContent || div.id;
        chartImages.push({ title, img: imgData, w: div.offsetWidth, h: div.offsetHeight });
      } catch(e) { console.warn("toImage failed for", div.id, e); }
    }

    if (!chartImages.length) {
      showToast("No charts to export", "error");
      if (btn) { btn.textContent = "IMG"; btn.disabled = false; }
      return;
    }

    // 3. Composite onto a single canvas
    const PAD = 20, LABEL_H = 18, COLS = 2;
    const COL_W = 800, CHART_H = 300;
    const rows = Math.ceil(chartImages.length / COLS);
    const HEADER_H = 80;
    const totalW = COL_W * COLS + PAD * (COLS + 1);
    const totalH = HEADER_H + rows * (CHART_H + LABEL_H + PAD) + PAD;

    const canvas = document.createElement("canvas");
    canvas.width = totalW; canvas.height = totalH;
    const ctx = canvas.getContext("2d");

    // Background
    ctx.fillStyle = "#ffffff"; ctx.fillRect(0, 0, totalW, totalH);

    // Header
    ctx.fillStyle = "#0d1b2a"; ctx.fillRect(0, 0, totalW, HEADER_H);
    ctx.fillStyle = "#c9a84c"; ctx.font = "bold 22px Segoe UI, Arial";
    ctx.fillText(run.run_label, PAD, 36);
    ctx.fillStyle = "#8fa3ba"; ctx.font = "13px Segoe UI, Arial";
    const m = run.metrics || {};
    ctx.fillText(
      `CAGR: ${((m.cagr||0)*100).toFixed(1)}%  |  Sharpe: ${(m.sharpe||0).toFixed(2)}  |  Max DD: ${((m.max_dd||0)*100).toFixed(1)}%  |  Win Rate: ${((m.win_rate_monthly||0)*100).toFixed(1)}%`,
      PAD, 58
    );
    ctx.fillText(`Generated: ${new Date().toISOString().slice(0,19)}  |  OBQ Factor Lab`, PAD, 74);

    // Draw each chart image
    const loadImg = src => new Promise((res, rej) => {
      const img = new Image(); img.onload = () => res(img); img.onerror = rej;
      img.src = src;
    });

    for (let i = 0; i < chartImages.length; i++) {
      const col = i % COLS, row = Math.floor(i / COLS);
      const x = PAD + col * (COL_W + PAD);
      const y = HEADER_H + PAD + row * (CHART_H + LABEL_H + PAD);

      // Label
      ctx.fillStyle = "#374151"; ctx.font = "bold 11px Segoe UI, Arial";
      ctx.fillText(chartImages[i].title, x, y + LABEL_H - 4);

      // Chart image
      try {
        const img = await loadImg(chartImages[i].img);
        ctx.drawImage(img, x, y + LABEL_H, COL_W, CHART_H);
      } catch(e) {}

      // Border
      ctx.strokeStyle = "#e5e7eb"; ctx.lineWidth = 1;
      ctx.strokeRect(x, y + LABEL_H, COL_W, CHART_H);
    }

    // 4. Get PNG blob and POST to server for saving to Downloads
    canvas.toBlob(async (blob) => {
      const reader = new FileReader();
      reader.onload = async () => {
        const b64 = reader.result.split(",")[1];
        const resp = await fetch("/api/export/image_data", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ run_label: run.run_label, image_b64: b64 }),
        }).then(r=>r.json()).catch(e=>({error:String(e)}));
        if (btn) { btn.textContent = "IMG"; btn.disabled = false; }
        if (resp.path) showToast("Image saved: " + resp.filename, "success");
        else showToast("Image failed: "+(resp.error||"unknown"), "error");
      };
      reader.readAsDataURL(blob);
    }, "image/png");

  } catch(e) {
    if (btn) { btn.textContent = "IMG"; btn.disabled = false; }
    showToast("Image error: " + e.message, "error");
  }
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
