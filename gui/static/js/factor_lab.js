// factor_lab.js — Factor Models tab
// Completely independent of app.js — safe to load alongside it.
// All state is module-scoped with _fl_ prefix to avoid conflicts.

(function () {
  "use strict";

  // ── Module state ────────────────────────────────────────────────────────────
  let _fl_runs      = [];     // [{run_id, run_label, status, result, metrics}]
  let _fl_active    = null;   // active run_id
  let _fl_sortKey   = "quintile_spread_cagr";
  let _fl_sortDir   = -1;
  let _fl_scores    = {};     // {score_col: display_name}
  let _fl_uid_ctr   = 0;

  function _uid() { return "flt_" + (++_fl_uid_ctr); }

  // ── Init — called when FACTOR MODELS tab is activated ──────────────────────
  window.factorLabInit = function () {
    _loadScores();
    _renderRunsTable();
    // Set end date to today
    const endEl = document.getElementById("fl-end-date");
    if (endEl && !endEl.value) endEl.valueAsDate = new Date();
  };

  // ── Load available scores from API ─────────────────────────────────────────
  async function _loadScores() {
    const r = await fetch("/api/factor/scores").then(r => r.json()).catch(() => null);
    if (!r || !r.scores) return;
    _fl_scores = r.scores;
    const sel = document.getElementById("fl-score-col");
    if (!sel) return;
    sel.innerHTML = "";
    Object.entries(r.scores).forEach(([col, name]) => {
      const opt = document.createElement("option");
      opt.value = col; opt.textContent = name;
      if (col === "jcn_full_composite") opt.selected = true;
      sel.appendChild(opt);
    });
    _loadScoreRange();
  }

  // ── Load date range when score changes ─────────────────────────────────────
  window.flScoreChanged = async function () {
    await _loadScoreRange();
  };

  async function _loadScoreRange() {
    const col = document.getElementById("fl-score-col")?.value;
    if (!col) return;
    const r = await fetch(`/api/factor/score_range?score=${col}`).then(r => r.json()).catch(() => null);
    if (!r || r.error) return;
    const info = document.getElementById("fl-score-info");
    if (info) info.textContent = `${r.min_date} → ${r.max_date}  ·  ${r.symbols?.toLocaleString()} symbols`;
    const start = document.getElementById("fl-start-date");
    if (start && !start.value) start.value = r.min_date > "1990-07-31" ? r.min_date : "1990-07-31";
  }

  // ── Run button ─────────────────────────────────────────────────────────────
  window.flRunBacktest = async function () {
    const btn   = document.getElementById("fl-run-btn");
    const score = document.getElementById("fl-score-col")?.value;
    const start = document.getElementById("fl-start-date")?.value;
    const end   = document.getElementById("fl-end-date")?.value || new Date().toISOString().slice(0, 7) + "-31";
    const nBuc  = parseInt(document.getElementById("fl-n-buckets")?.value) || 5;
    const hold  = parseInt(document.getElementById("fl-hold-months")?.value) || 6;
    const price = parseFloat(document.getElementById("fl-min-price")?.value) || 5.0;
    const adv   = parseFloat(document.getElementById("fl-min-adv")?.value) || 1_000_000;
    const cap   = document.getElementById("fl-cap-tier")?.value || "all";
    const rebal = document.getElementById("fl-rebal-freq")?.value || "semi-annual";
    const cost  = parseFloat(document.getElementById("fl-cost-bps")?.value) || 15.0;

    const scoreNames = _fl_scores[score] || score;
    const label = `${scoreNames} | ${nBuc}Q | ${hold}mo | ${cap} | ${rebal}`;

    if (btn) { btn.disabled = true; btn.textContent = "Running..."; }
    _flSetStatus("Running factor backtest...");

    const payload = {
      score_column: score, n_buckets: nBuc, hold_months: hold,
      start_date: start, end_date: end,
      min_price: price, min_adv_usd: adv, cap_tier: cap,
      rebalance_freq: rebal, cost_bps: cost,
      run_label: label,
    };

    const resp = await fetch("/api/factor/run", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload),
    }).then(r => r.json()).catch(e => ({ error: String(e) }));

    if (resp.error) {
      _flSetStatus("Error: " + resp.error);
      if (btn) { btn.disabled = false; btn.textContent = "RUN FACTOR"; }
      return;
    }

    const run_id = resp.run_id;
    const runObj = { run_id, run_label: label, status: "running", result: null, factor_metrics: null };
    _fl_runs.unshift(runObj);
    _renderRunsTable();
    _fl_active = run_id;
    _renderRunsTable();

    // SSE stream
    const sse = new EventSource(`/api/factor/stream/${run_id}`);
    sse.onmessage = e => {
      try {
        const d = JSON.parse(e.data);
        if (d.msg) _flSetStatus(d.msg);
      } catch {}
    };
    sse.onerror = () => sse.close();

    // Poll for result
    const poll = async () => {
      const info = await fetch(`/api/factor/status/${run_id}`).then(r => r.json()).catch(() => null);
      if (!info || info.status === "running") { setTimeout(poll, 2000); return; }
      sse.close();
      const result = await fetch(`/api/factor/result/${run_id}`).then(r => r.json()).catch(() => null);
      const run = _fl_runs.find(r => r.run_id === run_id);
      if (run) {
        run.status  = result?.status === "error" ? "error" : "complete";
        run.result  = result;
        run.factor_metrics = result?.factor_metrics || {};
      }
      _renderRunsTable();
      if (btn) { btn.disabled = false; btn.textContent = "RUN FACTOR"; }
      if (result?.status === "error") {
        _flSetStatus("Error: " + result.error);
      } else {
        const fm = result?.factor_metrics || {};
        _flSetStatus(
          `Done  |  Q1-Q5 Spread: ${((fm.quintile_spread_cagr||0)*100).toFixed(2)}%  |  ` +
          `IC: ${(fm.ic_mean||0).toFixed(4)}  |  ICIR: ${(fm.icir||0).toFixed(2)}  |  ` +
          `Monotonicity: ${((fm.monotonicity_score||0)*100).toFixed(0)}%  |  ` +
          `${result?.elapsed_s}s`
        );
        _showTearsheet(run_id);
      }
    };
    setTimeout(poll, 2000);
  };

  // ── Runs table ──────────────────────────────────────────────────────────────
  function _renderRunsTable() {
    const body  = document.getElementById("fl-tbl-body");
    const empty = document.getElementById("fl-tbl-empty");
    if (!body) return;
    if (!_fl_runs.length) {
      if (empty) empty.style.display = "block";
      body.innerHTML = "";
      return;
    }
    if (empty) empty.style.display = "none";

    const sorted = [..._fl_runs].sort((a, b) => {
      const av = (a.factor_metrics || {})[_fl_sortKey];
      const bv = (b.factor_metrics || {})[_fl_sortKey];
      if (av == null) return 1; if (bv == null) return -1;
      return _fl_sortDir * (bv - av);
    });

    body.innerHTML = "";
    sorted.forEach(run => {
      const fm = run.factor_metrics || {};
      const isRunning = run.status === "running";
      const isActive  = run.run_id === _fl_active;
      const tr = document.createElement("div");
      tr.className = "fl-tr" + (isActive ? " active" : "");
      tr.onclick = () => { _fl_active = run.run_id; _renderRunsTable(); _showTearsheet(run.run_id); };

      function pct(v, d=1)  { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
      function num(v, d=2)  { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }
      function pctR(v, d=0) { if(v==null||isNaN(v)) return "—"; return (v*100).toFixed(d)+"%"; }

      const icon = isRunning ? "⌛" : (run.status === "error" ? "✗" : "✓");
      const spread = fm.quintile_spread_cagr;
      const sCol = spread != null ? (spread >= 0 ? "g" : "r") : "";

      const _sid = run.result?.strategy_id || run.run_id;
      tr.innerHTML = `
        <div class="fl-td dim" style="flex:0 0 80px;font-size:10px">${icon} ${run.run_label.slice(0,12)}</div>
        <div class="fl-td ${sCol}" style="flex:0 0 65px">${pct(spread,2)}</div>
        <div class="fl-td" style="flex:0 0 46px">${num(fm.icir,2)}</div>
        <div class="fl-td" style="flex:0 0 44px">${pctR(fm.ic_hit_rate,0)}</div>
        <div class="fl-td" style="flex:0 0 48px">${num(fm.monotonicity_score!=null?fm.monotonicity_score*100:null,0)}%</div>
        <div class="fl-td" style="flex:0 0 50px">${pct(fm.q1_cagr,1)}</div>
        <div class="fl-td" style="flex:0 0 44px">${num(fm.q1_sharpe,2)}</div>
        <div class="fl-td dim" style="flex:1;font-size:9px;min-width:0;overflow:hidden;text-overflow:ellipsis">${run.run_label.slice(0,40)}</div>
        ${!isRunning && run.status !== "error" ? `<button
          title="Send Q1 to Results tab"
          onclick="event.stopPropagation();flPromoteToResults('${_sid}')"
          style="flex:0 0 20px;width:20px;margin:0 3px;padding:1px 4px;background:var(--accent2);color:#fff;border:none;border-radius:3px;cursor:pointer;font-size:10px;font-weight:900;line-height:1.4;align-self:center"
        >+</button>` : "<div style='flex:0 0 20px'></div>"}
      `;
      body.appendChild(tr);
    });
  }

  window.flSortBy = function (key) {
    if (_fl_sortKey === key) _fl_sortDir *= -1;
    else { _fl_sortKey = key; _fl_sortDir = -1; }
    _renderRunsTable();
  };

  // ── Factor Tearsheet renderer ────────────────────────────────────────────────
  function _showTearsheet(run_id) {
    const run = _fl_runs.find(r => r.run_id === run_id);
    if (!run || !run.result || run.result.status === "error") return;

    const tsEmpty   = document.getElementById("fl-ts-empty");
    const tsContent = document.getElementById("fl-ts-content");
    if (!tsEmpty || !tsContent) return;

    tsEmpty.style.display    = "none";
    tsContent.style.display  = "flex";
    tsContent.style.flexDirection = "column";
    tsContent.style.overflowY = "auto";
    tsContent.innerHTML = "";

    const result  = run.result;
    const fm      = result.factor_metrics || {};
    const buckets = result.buckets || [1,2,3,4,5];
    const n       = buckets.length;

    // ── Header ────────────────────────────────────────────────────────────────
    const hdr = document.createElement("div");
    hdr.style.cssText = "display:flex;align-items:center;justify-content:space-between;padding:8px 14px;background:var(--bg-panel);border-bottom:1px solid var(--border);flex-shrink:0";
    // Build full config description line
    const cfg = result.config || {};
    const scoreDisplay = cfg.score_column || run.run_label.split('|')[0].trim();
    // Score display name lookup
    const scoreNames = {
      jcn_full_composite:'JCN Composite', jcn_qarp:'JCN QARP', jcn_garp:'JCN GARP',
      jcn_quality_momentum:'JCN Quality-Mom', jcn_value_momentum:'JCN Value-Mom',
      jcn_growth_quality_momentum:'JCN GQM', jcn_fortress:'JCN Fortress',
      jcn_alpha_trifecta:'JCN Alpha Trifecta', value_score:'Value', quality_score:'Quality',
      growth_score:'Growth', finstr_score:'FinStr', momentum_score:'Momentum',
      momentum_af_score:'Momentum AF', momentum_fip_score:'Momentum FIP',
      momentum_sys_score:'Momentum Sys', value_score_universe:'Value (Universe)',
      quality_score_universe:'Quality (Universe)', growth_score_universe:'Growth (Universe)',
      finstr_score_universe:'FinStr (Universe)', af_universe_score:'AF (Universe)',
      longeq_rank:'LongEQ Rank', rulebreaker_rank:'Rulebreaker Rank',
      fundsmith_rank:'Fundsmith Rank', moat_score:'Moat Score', moat_rank:'Moat Rank',
    };
    const scoreName = scoreNames[cfg.score_column] || cfg.score_column || '';
    const cfgLine = [
      scoreName             ? `Factor: ${scoreName}`               : null,
      cfg.n_buckets         ? `${cfg.n_buckets}Q`                  : null,
      cfg.hold_months       ? `${cfg.hold_months}mo hold`          : null,
      cfg.rebalance_freq    ? cfg.rebalance_freq                   : null,
      cfg.cap_tier && cfg.cap_tier !== 'all' ? cfg.cap_tier + ' cap' : (cfg.min_market_cap > 0 ? `$${(cfg.min_market_cap/1e9).toFixed(0)}B+ mktcap` : 'All-Cap'),
      cfg.start_date        ? `${cfg.start_date.slice(0,4)}–${(cfg.end_date||'').slice(0,4)}` : null,
      cfg.cost_bps          ? `${cfg.cost_bps}bps cost`            : null,
    ].filter(Boolean).join('  ·  ');

    const stratId = result.strategy_id || run.run_id || '';
    hdr.innerHTML = `
      <div style="flex:1;min-width:0">
        <div style="font-size:13px;font-weight:800;color:var(--accent2);letter-spacing:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${run.run_label}</div>
        <div style="font-size:9px;color:var(--text-muted);margin-top:2px">${result.dates?.[0]||""} → ${result.dates?.[result.dates.length-1]||""}  ·  ${result.n_obs||0} periods  ·  ~${Math.round(result.n_stocks_avg||0)} stocks/period${stratId ? '  ·  ' + stratId : ''}</div>
        <div style="font-size:8.5px;color:var(--accent2);margin-top:3px;opacity:0.85">${cfgLine}</div>
      </div>
      <div style="display:flex;gap:6px">
        <button onclick="flExportCSV('${run_id}')" style="background:#16a34a;color:#fff;border:none;padding:4px 10px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">&#128202; CSV</button>
        <button onclick="flExportPDF('${run_id}')" style="background:#0066cc;color:#fff;border:none;padding:4px 10px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">&#128196; PDF</button>
        <button onclick="flSnapTearsheet('${run_id}')" style="background:#7c3aed;color:#fff;border:none;padding:4px 10px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">&#128247; PNG</button>
      </div>
    `;
    tsContent.appendChild(hdr);

    // ── KPI strip ─────────────────────────────────────────────────────────────
    const kpiRow = document.createElement("div");
    kpiRow.style.cssText = "display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--border);flex-shrink:0";
    // OBQ Fund Score: 30% AlphaWin + 25% AlphaMag + 20% DDProtect + 15% DnCapture + 10% AlphaSharpe
    const obqFund = fm.obq_fund_score;
    const obqFundStr = obqFund != null ? obqFund.toFixed(3) : "—";
    const obqFundCol = obqFund != null ? (obqFund >= 0.5 ? "#16a34a" : obqFund >= 0.3 ? "#f59e0b" : "#dc2626") : null;

    // Staircase = avg CAGR step Q1→Q2→Q3→Q4→Q5 (positive = clean monotonic separation)
    const stair = fm.staircase_score;
    const stairStr = stair != null ? (stair >= 0 ? "+" : "") + (stair * 100).toFixed(2) + "%" : "—";
    const stairCol = stair != null ? (stair >= 0.02 ? "#16a34a" : stair >= 0 ? null : "#dc2626") : null;

    const kpis = [
      ["Q1-Q5 SPREAD",    ((fm.quintile_spread_cagr||0)*100).toFixed(2)+"%", (fm.quintile_spread_cagr||0)>0?"#16a34a":"#dc2626"],
      ["IC MEAN",          (fm.ic_mean||0).toFixed(4),                        (fm.ic_mean||0)>0.05?"#16a34a":null],
      ["ICIR",             (fm.icir||0).toFixed(2),                           (fm.icir||0)>=0.5?"#16a34a":null],
      ["IC HIT RATE",      ((fm.ic_hit_rate||0)*100).toFixed(1)+"%",          (fm.ic_hit_rate||0)>=0.55?"#16a34a":null],
      ["STAIRCASE",        stairStr,                                           stairCol],
      ["OBQ FUND SCORE",   obqFundStr,                                        obqFundCol],
    ];
    kpis.forEach(([lbl, val, col]) => {
      const box = document.createElement("div");
      box.style.cssText = "background:var(--bg-panel);padding:8px 6px;text-align:center";
      const valColor = col || "var(--text)";
      box.innerHTML = `<div style="font-size:8px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.8px;margin-bottom:3px">${lbl}</div><div style="font-size:15px;font-weight:700;color:${valColor}">${val}</div>`;
      kpiRow.appendChild(box);
    });
    tsContent.appendChild(kpiRow);

    // Helper: create a chart row
    function chartRow(wide) {
      const r = document.createElement("div");
      r.style.cssText = `display:grid;grid-template-columns:${wide?"1fr":"1fr 1fr"};gap:8px;padding:10px 12px;flex-shrink:0`;
      tsContent.appendChild(r);
      return r;
    }
    function chartBox(title, h) {
      const box = document.createElement("div");
      box.style.cssText = "background:var(--bg-panel);border:1px solid var(--border);border-radius:4px;padding:8px 10px;overflow:hidden";
      if (title) box.innerHTML = `<div style="font-size:8px;font-weight:700;color:var(--text-dim);letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">${title}</div>`;
      const d = document.createElement("div");
      d.id = _uid(); d.style.cssText = `height:${h}px;width:100%`;
      box.appendChild(d);
      return { box, id: d.id };
    }
    function sec(title) {
      const h = document.createElement("div");
      h.className = "ts-sec-hdr";
      h.textContent = title; tsContent.appendChild(h);
    }

    // ── 1. TORTORIELLO MAIN TABLE (Figure 2.3) ──────────────────────────────
    sec("QUINTILE PERFORMANCE SUMMARY — " + result.dates?.[0]?.slice(0,4) + " – " + result.dates?.[result.dates.length-1]?.slice(0,4));
    if (typeof ftBuildMainTable === "function") {
      tsContent.appendChild(ftBuildMainTable(result));
    }

    // ── 2. Excess Returns bar + Rolling 3Y chart ───────────────────────────
    const r_excess = chartRow(false);
    const {box:exBox, id:exId} = chartBox("AVERAGE EXCESS RETURN VS. UNIVERSE", 220);
    r_excess.appendChild(exBox);
    const {box:r3yBox, id:r3yId} = chartBox("ROLLING 3-YEAR ANNUALIZED EXCESS RETURNS: TOP vs BOTTOM", 220);
    r_excess.appendChild(r3yBox);

    // ── 3. IC ANALYSIS ──────────────────────────────────────────────────────
    sec("IC ANALYSIS");
    const r2 = chartRow(false);
    const {box:icBox, id:icId}     = chartBox("IC (INFORMATION COEFFICIENT) OVER TIME", 180);
    r2.appendChild(icBox);
    const {box:spBox, id:spId}     = chartBox("Q1 - Q5 SPREAD (CUMULATIVE GROWTH)", 180);
    r2.appendChild(spBox);

    // ── 4. CUMULATIVE EQUITY + PERIOD HEATMAP ─────────────────────────────
    sec("EQUITY CURVES & PERIOD RETURNS");
    const r1 = chartRow(false);
    const {box:ceqBox, id:ceqId} = chartBox("CUMULATIVE EQUITY BY QUINTILE", 220);
    r1.appendChild(ceqBox);
    const {box:qbarBox, id:qbarId} = chartBox("CAGR BY QUINTILE (BAR)", 220);
    r1.appendChild(qbarBox);

    sec("PERIOD RETURNS HEATMAP");
    const r3 = chartRow(true);
    const {box:hmBox, id:hmId} = chartBox("", 280);  // taller for rotated x labels
    r3.appendChild(hmBox);

    // ── 5. ANNUAL RETURNS ─────────────────────────────────────────────────
    sec("ANNUAL RETURNS BY QUINTILE");
    const r4 = chartRow(true);
    const {box:annBox, id:annId} = chartBox("", 200);
    r4.appendChild(annBox);

    // ── 6. SECTOR TABLES (Figure 2.4) ────────────────────────────────────
    if (result.sector_attribution && result.sector_attribution.length > 0) {
      sec("SECTOR SUMMARY — TOP & BOTTOM QUINTILE");
      if (typeof ftBuildSectorTables === "function") {
        tsContent.appendChild(ftBuildSectorTables(result));
      }
      // Sector bar chart
      const r5 = chartRow(true);
      const {box:secBox, id:secId} = chartBox("AVERAGE EXCESS RETURN BY SECTOR", 240);
      r5.appendChild(secBox);
      setTimeout(() => {
        try { ftDrawSectorAttribution(secId, result.sector_attribution, n); } catch(e){}
      }, 400);
    }

    // ── 7. FULL METRICS TABLE ─────────────────────────────────────────────
    sec("FULL FACTOR METRICS");
    tsContent.appendChild(ftBuildMetricsTable(fm, buckets, result.bucket_metrics || {}));

    // ── Q1 TRADE LOG ────────────────────────────────────────────────────────
    sec("Q1 TRADE LOG — FULL AUDIT (every Q1 position entry/exit)");
    tsContent.appendChild(_buildFactorTradeLog(result.trade_log || []));

    // ── Draw all Plotly charts after DOM settles ────────────────────────────
    setTimeout(() => {
      try { ftDrawExcessReturnBar(exId, buckets, result.tortoriello||{}, result.universe_metrics||{}); } catch(e){ console.error("excess",e); }
      try { ftDrawRolling3YTopBottom(r3yId, buckets, result.tortoriello||{}); } catch(e){ console.error("r3y",e); }
      try { ftDrawIC(icId, result.ic_data||[]); } catch(e){ console.error("ic",e); }
      try { ftDrawSpread(spId, result.dates||[], result.bucket_equity||{}, n); } catch(e){ console.error("spread",e); }
      try { ftDrawCumulativeEquity(ceqId, result.dates||[], buckets, result.bucket_equity||{}); } catch(e){ console.error("ceq",e); }
      try { ftDrawQuintileBar(qbarId, buckets, result.bucket_metrics, 0); } catch(e){ console.error("qbar",e); }
      try { ftDrawPeriodHeatmap(hmId, result.period_data||[], n); } catch(e){ console.error("hm",e); }
      try { ftDrawAnnualBars(annId, result.annual_ret_by_bucket||{}, n); } catch(e){ console.error("ann",e); }
    }, 200);

    // Force Plotly resize after layout completes — multiple passes for bank-loaded tearsheets
    [300, 600, 1000, 1800, 3000, 5000].forEach(delay => setTimeout(() => {
      document.querySelectorAll("#fl-ts-content [id^=flt_]").forEach(d => {
        try {
          // Force explicit height then resize
          if (d.offsetHeight === 0 && d.parentElement) {
            const ph = parseInt(d.style.height) || 200;
            d.style.height = ph + "px";
          }
          Plotly.Plots.resize(d);
        } catch(e){}
      });
    }, delay));
  }

  // ── Factor Trade Log table ──────────────────────────────────────────────────
  function _buildFactorTradeLog(tradeLog) {
    const wrap = document.createElement("div");
    wrap.style.cssText = "overflow:auto;flex-shrink:0;max-height:400px;margin:0 12px 12px;border:1px solid var(--border);border-radius:4px";

    if (!tradeLog || !tradeLog.length) {
      wrap.innerHTML = '<div style="padding:20px;text-align:center;font-size:10px;color:var(--text-muted)">No trade log data — re-run backtest to generate</div>';
      return wrap;
    }

    const SH = 'style="padding:4px 8px;background:var(--bg-panel2);color:var(--text-dim);font-size:8px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid var(--border);white-space:nowrap;position:sticky;top:0;z-index:1"';
    const S  = 'style="padding:3px 8px;font-size:9px;border-bottom:1px solid var(--border);white-space:nowrap"';

    let html = `<table style="width:100%;border-collapse:collapse;font-size:9px;font-family:'Segoe UI',sans-serif">
      <thead><tr>
        <th ${SH}>ENTRY DATE</th>
        <th ${SH}>SYMBOL</th>
        <th ${SH}>SECTOR</th>
        <th ${SH}>SCORE</th>
        <th ${SH}>MKT CAP ($B)</th>
        <th ${SH}>EXIT DATE</th>
        <th ${SH}>RETURN %</th>
        <th ${SH}>W/L</th>
      </tr></thead><tbody>`;

    tradeLog.forEach((t, i) => {
      const ret = t.return_pct;
      const retColor = ret != null ? (ret >= 0 ? "color:#16a34a" : "color:#dc2626") : "color:var(--text-dim)";
      const wl = ret != null ? (ret >= 0 ? "W" : "L") : "—";
      const wlColor = ret != null ? (ret >= 0 ? "color:#16a34a;font-weight:700" : "color:#dc2626;font-weight:700") : "";
      const bg = i%2===0 ? "background:var(--bg)" : "background:var(--bg-panel)";
      html += `<tr style="${bg}">
        <td ${S}>${t.entry_date || "—"}</td>
        <td ${S} style="font-weight:700;font-family:monospace">${t.symbol || "—"}</td>
        <td ${S} style="font-size:8px;color:var(--text-dim)">${(t.sector||"").replace(" ","<br/>")}</td>
        <td ${S} style="text-align:right;color:#7c3aed">${t.score != null ? t.score.toFixed(1) : "—"}</td>
        <td ${S} style="text-align:right">${t.market_cap_B != null ? "$"+t.market_cap_B.toFixed(1)+"B" : "—"}</td>
        <td ${S}>${t.exit_date || "—"}</td>
        <td ${S} style="text-align:right;${retColor}">${ret != null ? (ret>=0?"+":"")+ ret.toFixed(2)+"%" : "—"}</td>
        <td ${S} style="${wlColor};text-align:center">${wl}</td>
      </tr>`;
    });

    const wins = tradeLog.filter(t => t.return_pct != null && t.return_pct >= 0).length;
    const total = tradeLog.filter(t => t.return_pct != null).length;
    const avgRet = total > 0 ? tradeLog.filter(t=>t.return_pct!=null).reduce((s,t)=>s+t.return_pct,0)/total : 0;
    html += `<tr style="background:var(--bg-panel2);font-weight:700">
      <td ${S} colspan="6" style="font-size:9px;color:var(--text-dim)">
        SUMMARY: ${tradeLog.length} trades | ${total > 0 ? ((wins/total)*100).toFixed(1) : "—"}% win rate | avg return ${avgRet>=0?"+":""}${avgRet.toFixed(2)}%
      </td>
      <td ${S} style="text-align:right;color:${avgRet>=0?"#16a34a":"#dc2626"}">${avgRet>=0?"+":""}${avgRet.toFixed(2)}%</td>
      <td ${S}></td>
    </tr>`;

    html += "</tbody></table>";
    wrap.innerHTML = html;
    return wrap;
  }

  // ── PDF Export ─────────────────────────────────────────────────────────────
  async function _flExportPDF(run_id) {
    const run = _fl_runs.find(r => r.run_id === run_id);
    if (!run || !run.result) return;
    _flSetStatus("Generating PDF...");
    const resp = await fetch("/api/export/pdf", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({ run_label: run.run_label, result: run.result, metrics: run.factor_metrics || {} }),
    }).then(r => r.json()).catch(e => ({ error: String(e) }));
    if (resp.path) _flSetStatus("PDF saved: " + resp.filename);
    else _flSetStatus("PDF failed: " + (resp.error || "unknown"));
  }
  window.flExportPDF = _flExportPDF;

  // ── PNG / Snap ──────────────────────────────────────────────────────────────
  async function _flSnapTearsheet(run_id) {
    _flSetStatus("Capturing screenshot...");
    const resp = await fetch("/api/snap").then(r => r.json()).catch(() => null);
    if (resp && resp.path)     _flSetStatus("Screenshot saved: " + (resp.filename || resp.path));
    else if (resp && resp.img) _flSetStatus("Screenshot captured (base64)");
    else                       _flSetStatus("Snap failed — check PyWebView window");
  }
  window.flSnapTearsheet = _flSnapTearsheet;

  // ── Promote factor model Q1 to Results tab ─────────────────────────────────
  // Packages Q1 equity curve + metrics as a Results-compatible run entry,
  // switches to Results tab, and auto-selects the row.
  window.flPromoteToResults = async function (strategyId) {
    // Fetch full model from bank
    _flSetStatus("Promoting " + strategyId + " to Results tab...");
    const full = await fetch("/api/factor/bank/" + strategyId).then(r=>r.json()).catch(()=>null);
    if (!full) { _flSetStatus("Error loading " + strategyId); return; }

    function _arr(f) { return Array.isArray(f) ? f : []; }
    function _obj(f) { return (f && typeof f === "object" && !Array.isArray(f)) ? f : {}; }

    const bm    = _obj(full.bucket_metrics_json);
    const dates = _arr(full.dates_json);
    const be    = _obj(full.bucket_equity_json);
    const annRet= _obj(full.annual_ret_json);
    const q1m   = _obj(bm["1"]);

    // Build a Results-compatible run object from Q1 data
    const run_id    = "promoted-" + strategyId;
    const run_label = (full.run_label || strategyId) + " · Q1";

    // Map factor bucket_metrics to Results metrics shape
    const metrics = {
      cagr:             q1m.cagr,
      sharpe:           q1m.sharpe,
      max_dd:           q1m.max_dd,
      sortino:          q1m.sortino,
      ann_vol:          q1m.ann_vol,
      calmar:           q1m.calmar,
      omega:            q1m.omega,
      surefire_ratio:   q1m.surefire_ratio,
      win_rate_monthly: q1m.win_rate_monthly,
      equity_r2:        q1m.equity_r2,
      profit_factor:    q1m.profit_factor,
      system_score:     q1m.system_score,
      lake_ratio:       q1m.lake_ratio,
      n_periods:        dates.length,
    };

    // Build equity dates (prepend start)
    const startDate = dates.length > 0
      ? new Date(new Date(dates[0]).getTime() - 180*24*60*60*1000).toISOString().slice(0,10)
      : "2000-01-01";
    const equityDates = [startDate, ...dates];
    const q1Equity    = _arr(be["1"]);

    // Annual returns for bar chart [{year, ret}]
    const annualRetList = _arr(annRet["1"]);

    const result = {
      status:           "complete",
      run_label,
      factor:           "factor-q1",
      mode:             "quintile",
      n_periods:        dates.length,
      portfolio_equity: q1Equity,
      equity_dates:     equityDates.slice(0, q1Equity.length),
      bm_equity:        [],
      bm_metrics:       {},
      portfolio_metrics: metrics,
      annual_ret_by_year: annualRetList,
      // Tag as promoted factor model
      _promoted_from:   strategyId,
    };

    // Inject into Results tab _runs array
    if (typeof _runs !== "undefined") {
      // Remove any previous promotion of same model
      const existing = _runs.findIndex(r => r.run_id === run_id);
      if (existing >= 0) _runs.splice(existing, 1);
      _runs.unshift({
        run_id,
        run_label,
        status: "complete",
        cfg: { factor: "factor-q1", model_type: "quintile" },
        metrics,
        result,
      });
      if (typeof renderTable === "function") renderTable();
    }

    // Switch to Results tab and select the row
    if (typeof switchMainTab === "function") switchMainTab("results");
    setTimeout(function() {
      if (typeof setActiveRow === "function") setActiveRow(run_id);
      if (typeof showTearsheet === "function") showTearsheet(run_id);
    }, 200);

    _flSetStatus("Promoted " + strategyId + " → Results tab as " + run_label);
  };

  // ── CSV Export ──────────────────────────────────────────────────────────────
  window.flExportCSV = async function (run_id) {
    const run = _fl_runs.find(r => r.run_id === run_id);
    if (!run || !run.result) return;
    const resp = await fetch("/api/export/csv", {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ run_label: run.run_label, result: run.result,
                             metrics: run.factor_metrics || {} }),
    }).then(r => r.json()).catch(e => ({ error: String(e) }));
    if (resp.path) _flSetStatus("CSV saved: " + resp.filename);
    else _flSetStatus("CSV failed: " + (resp.error || "unknown"));
  };

  function _flSetStatus(msg) {
    const el = document.getElementById("fl-status-bar");
    if (el) el.textContent = msg;
  }

  // ── Bank row click handler (shared between flLoadBank and cycles_lab resort) ──
  window.flBankRowClick = async function (m) {
    function pct(v,d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
    function num(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }
    _flSetStatus("Loading saved model " + m.strategy_id + "...");
    const full = await fetch("/api/factor/bank/"+m.strategy_id).then(r=>r.json()).catch(()=>null);
    if (!full) { _flSetStatus("Error loading " + m.strategy_id); return; }
    // Helper: safely extract parsed JSON field (get_model() already parses them)
    function _arr(f) { return Array.isArray(f) ? f : []; }
    function _obj(f) { return (f && typeof f === "object" && !Array.isArray(f)) ? f : {}; }

    const dates        = _arr(full.dates_json);
    const bucketMetrics= _obj(full.bucket_metrics_json);
    const icData       = _arr(full.ic_data_json);
    const bucketEquity = _obj(full.bucket_equity_json);
    const annualRet    = _obj(full.annual_ret_json);
    const tort         = _obj(full.tortoriello_json);
    const univMetrics  = _obj(full.universe_metrics_json);
    const periodData   = _arr(full.period_data_json);
    const sectorAttr   = _arr(full.sector_attribution_json);
    const spyMetrics   = _obj(full.spy_metrics_json);
    const tradeLog     = _arr(full.trade_log_json);
    const cfg          = _obj(full.config_json);

    // Reconstruct universe_terminal:
    // 1. From stored universe_equity (most accurate: final equity × $10K)
    // 2. From tort data (Q1 terminal_wealth as proxy if available)
    // 3. Compute from universe CAGR + n_years
    let univTerminal = null;
    const univEq = _arr(full.universe_equity_json || full.universe_equity);
    if (univEq.length > 0) {
      univTerminal = Math.round(10000 * univEq[univEq.length - 1]);
    } else if (univMetrics.cagr && univMetrics.n_years) {
      univTerminal = Math.round(10000 * Math.pow(1 + univMetrics.cagr, univMetrics.n_years));
    }

    // Build factor_metrics from ALL scalar columns in the bank row (m)
    // PLUS any additional fields from factor_metrics_json blob (newer models)
    // Scalar columns always take priority — they are the authoritative source.
    const fmJson    = _obj(full.factor_metrics_json);  // may be null for old models
    const fmFromBank = {
      // Signal quality (scalar columns from DB)
      ic_mean:              m.ic_mean,
      ic_std:               m.ic_std,
      icir:                 m.icir,
      ic_hit_rate:          m.ic_hit_rate,
      spearman_rho:         m.spearman_rho,
      monotonicity_score:   m.monotonicity_score,
      // Return metrics
      quintile_spread_cagr: m.quintile_spread_cagr,
      q1_cagr:              m.q1_cagr,
      q1_sharpe:            m.q1_sharpe,
      q1_max_dd:            m.q1_max_dd,
      q1_calmar:            m.q1_calmar,
      q1_surefire:          m.q1_surefire,
      qn_cagr:              m.qn_cagr,
      qn_sharpe:            m.qn_sharpe,
      // Fitness scalars (all from DB scalar columns)
      staircase_score:      m.staircase_score,
      alpha_win_rate:       m.alpha_win_rate,
      avg_annual_alpha:     m.avg_annual_alpha,
      bear_score:           m.bear_score,
      bull_score:           m.bull_score,
      downside_capture:     m.downside_capture,
      alpha_sharpe:         m.alpha_sharpe,
      obq_fund_score:       m.obq_fund_score,
      // Meta
      n_obs:                m.n_obs,
      n_stocks_avg:         m.n_stocks_avg,
      n_buckets:            m.n_buckets,
      hold_months:          m.hold_months,
      // Merge any extra fields from JSON blob (e.g. per-bucket details)
      ...fmJson,
      // Per-bucket CAGR from bucket_metrics_json
      ...Object.fromEntries(
        Object.entries(bucketMetrics).map(([b, bm]) => [
          `q${b}_cagr`,   (bm || {}).cagr
        ])
      ),
      // Re-apply scalar overrides so DB values win over stale JSON
      staircase_score:      m.staircase_score,
      obq_fund_score:       m.obq_fund_score,
      alpha_win_rate:       m.alpha_win_rate,
      bear_score:           m.bear_score,
      bull_score:           m.bull_score,
    };

    const runObj = {
      run_id: m.strategy_id,
      run_label: m.run_label || m.strategy_id,
      status: "complete",
      factor_metrics: fmFromBank,
      result: {
        status: "complete",
        run_label: m.run_label,
        dates,
        buckets: Array.from({length: m.n_buckets || 5}, (_, i) => i + 1),
        bucket_metrics:       bucketMetrics,
        ic_data:              icData,
        bucket_equity:        bucketEquity,
        annual_ret_by_bucket: annualRet,
        tortoriello:          tort,
        universe_metrics:     univMetrics,
        universe_terminal:    univTerminal,
        spy_metrics:          spyMetrics,
        period_data:          periodData,
        sector_attribution:   sectorAttr,
        trade_log:            tradeLog,
        factor_metrics:       fmFromBank,
        n_obs:       m.n_obs,
        n_stocks_avg:m.n_stocks_avg,
        config:      cfg,
      }
    };
    if (!_fl_runs.find(r => r.run_id === m.strategy_id)) {
      _fl_runs.unshift(runObj);
      _renderRunsTable();
    }
    _fl_active = m.strategy_id;
    _renderRunsTable();
    _showTearsheet(m.strategy_id);
    _flSetStatus("Loaded: " + m.strategy_id + " | ICIR=" + num(m.icir,3) + " | Spread=" + pct(m.quintile_spread_cagr,2));
  };

  // ── Load saved models from bank ──────────────────────────────────────────────
  window.flLoadBank = async function () {
    const empty = document.getElementById("fl-bank-empty");
    const r = await fetch("/api/factor/bank").then(r => r.json()).catch(() => null);
    if (!r || !r.models) {
      if (empty) empty.textContent = "Error loading bank";
      return;
    }
    const models = r.models || [];

    // Share data with cycles_lab for resort and cycle linking
    if (typeof flSetBankData  === "function") flSetBankData(models);
    if (typeof flLinkStratsToCycle === "function") flLinkStratsToCycle(models);

    // Use cycles_lab renderer (handles sort + column layout)
    const sortEl = document.getElementById("fl-log-sort");
    const sortKey = sortEl ? sortEl.value : "quintile_spread_cagr";
    if (typeof flRenderBankRows === "function") {
      flRenderBankRows(models, sortKey);
    }
  };

  // Auto-load bank on init
  const _origFactorLabInit = window.factorLabInit;
  window.factorLabInit = function () {
    if (_origFactorLabInit) _origFactorLabInit();
    window.flLoadBank();
  };

})();
