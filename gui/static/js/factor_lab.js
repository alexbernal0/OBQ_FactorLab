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
    if (start && !start.value) start.value = r.min_date > "2005-01-31" ? r.min_date : "2005-01-31";
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

      tr.innerHTML = `
        <div class="fl-td dim" style="flex:0 0 80px;font-size:10px">${icon} ${run.run_label.slice(0,12)}</div>
        <div class="fl-td ${sCol}" style="flex:0 0 65px">${pct(spread,2)}</div>
        <div class="fl-td" style="flex:0 0 50px">${num(fm.ic_mean,4)}</div>
        <div class="fl-td" style="flex:0 0 48px">${num(fm.icir,2)}</div>
        <div class="fl-td" style="flex:0 0 48px">${pctR(fm.ic_hit_rate,0)}</div>
        <div class="fl-td" style="flex:0 0 52px">${num(fm.monotonicity_score!=null?fm.monotonicity_score*100:null,0)}%</div>
        <div class="fl-td" style="flex:0 0 50px">${pct(fm.q1_cagr,1)}</div>
        <div class="fl-td" style="flex:0 0 48px">${num(fm.q1_sharpe,2)}</div>
        <div class="fl-td dim" style="flex:1;font-size:9px">${run.run_label.slice(13,60)}</div>
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
    hdr.style.cssText = "display:flex;align-items:center;justify-content:space-between;padding:8px 14px;background:#f8f9fa;border-bottom:1px solid #e5e7eb;flex-shrink:0";
    hdr.innerHTML = `
      <div>
        <div style="font-size:13px;font-weight:800;color:#0066cc;letter-spacing:1px">${run.run_label}</div>
        <div style="font-size:9px;color:#6b7280;margin-top:2px">${result.dates?.[0]||""} → ${result.dates?.[result.dates.length-1]||""}  ·  ${result.n_obs||0} periods  ·  ~${Math.round(result.n_stocks_avg||0)} stocks/period</div>
      </div>
      <div style="display:flex;gap:6px">
        <button onclick="flExportCSV('${run_id}')" style="background:#16a34a;color:#fff;border:none;padding:4px 10px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">CSV</button>
      </div>
    `;
    tsContent.appendChild(hdr);

    // ── KPI strip ─────────────────────────────────────────────────────────────
    const kpiRow = document.createElement("div");
    kpiRow.style.cssText = "display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:#e5e7eb;flex-shrink:0";
    const kpis = [
      ["Q1-Q5 SPREAD", ((fm.quintile_spread_cagr||0)*100).toFixed(2)+"%", (fm.quintile_spread_cagr||0)>0?"#16a34a":"#dc2626"],
      ["IC MEAN",       (fm.ic_mean||0).toFixed(4),                        (fm.ic_mean||0)>0.05?"#16a34a":"#374151"],
      ["ICIR",          (fm.icir||0).toFixed(2),                           (fm.icir||0)>=0.5?"#16a34a":"#374151"],
      ["IC HIT RATE",   ((fm.ic_hit_rate||0)*100).toFixed(1)+"%",          (fm.ic_hit_rate||0)>=0.55?"#16a34a":"#374151"],
      ["MONOTONICITY",  ((fm.monotonicity_score||0)*100).toFixed(0)+"%",   (fm.monotonicity_score||0)>=0.8?"#16a34a":"#374151"],
      ["Q1 SHARPE",     (fm.q1_sharpe||0).toFixed(2),                     (fm.q1_sharpe||0)>=0.5?"#16a34a":"#374151"],
    ];
    kpis.forEach(([lbl, val, col]) => {
      const box = document.createElement("div");
      box.style.cssText = "background:#fff;padding:8px 6px;text-align:center";
      box.innerHTML = `<div style="font-size:8px;color:#6b7280;text-transform:uppercase;letter-spacing:.8px;margin-bottom:3px">${lbl}</div><div style="font-size:15px;font-weight:700;color:${col}">${val}</div>`;
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
      box.style.cssText = "background:#fff;border:1px solid #e5e7eb;border-radius:4px;padding:8px 10px;overflow:hidden";
      if (title) box.innerHTML = `<div style="font-size:8px;font-weight:700;color:#6b7280;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">${title}</div>`;
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
    const {box:hmBox, id:hmId} = chartBox("", 200);
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

    // Force Plotly resize after layout completes
    [600, 1200, 2000].forEach(delay => setTimeout(() => {
      document.querySelectorAll("#fl-ts-content [id^=flt_]").forEach(d => {
        try { if(d.data) Plotly.Plots.resize(d); } catch(e){}
      });
    }, delay));
  }

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

  // ── Load saved models from bank into bottom-left panel ──────────────────────
  window.flLoadBank = async function () {
    const body  = document.getElementById("fl-bank-body");
    const empty = document.getElementById("fl-bank-empty");
    const count = document.getElementById("fl-bank-count");
    if (!body) return;

    const r = await fetch("/api/factor/bank").then(r => r.json()).catch(() => null);
    if (!r || !r.models) {
      if (empty) empty.textContent = "Error loading bank";
      return;
    }

    const models = r.models || [];
    if (count) count.textContent = models.length + " saved";

    if (!models.length) {
      if (empty) { empty.style.display="block"; empty.textContent = "No saved models yet — run a backtest"; }
      body.innerHTML = "";
      return;
    }
    if (empty) empty.style.display = "none";
    body.innerHTML = "";

    function pct(v,d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
    function num(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

    models.forEach(m => {
      const tr = document.createElement("div");
      tr.className = "fl-tr";
      tr.style.cssText = "font-size:10px";
      const spread = m.quintile_spread_cagr;
      const sCol = spread!=null?(spread>=0.05?"g":spread>=0?"":"r"):"";
      tr.innerHTML = `
        <div class="fl-td dim" style="flex:0 0 115px;font-family:monospace;font-size:9px">${(m.strategy_id||"").slice(0,16)}</div>
        <div class="fl-td ${sCol}" style="flex:0 0 60px">${pct(spread,2)}</div>
        <div class="fl-td" style="flex:0 0 44px">${num(m.icir,2)}</div>
        <div class="fl-td" style="flex:0 0 44px">${m.ic_hit_rate!=null?((m.ic_hit_rate)*100).toFixed(0)+"%":"—"}</div>
        <div class="fl-td ${(m.q1_cagr||0)>=0?"g":"r"}" style="flex:0 0 50px">${pct(m.q1_cagr,1)}</div>
        <div class="fl-td dim" style="flex:1;font-size:9px">${(m.run_label||"").slice(0,35)}</div>
      `;
      // Clicking a saved model loads it into the tearsheet
      tr.onclick = async () => {
        document.querySelectorAll("#fl-bank-body .fl-tr").forEach(r => r.classList.remove("active"));
        tr.classList.add("active");
        _flSetStatus("Loading saved model " + m.strategy_id + "...");
        // Fetch full result from bank
        const full = await fetch("/api/factor/bank/"+m.strategy_id).then(r=>r.json()).catch(()=>null);
        if (!full) { _flSetStatus("Error loading " + m.strategy_id); return; }
        // Reconstruct a run object from bank data
        const runObj = {
          run_id: m.strategy_id,
          run_label: m.run_label || m.strategy_id,
          status: "complete",
          factor_metrics: m,
          result: {
            status: "complete",
            run_label: m.run_label,
            dates: [],
            buckets: Array.from({length:m.n_buckets||5},(_,i)=>i+1),
            bucket_metrics: typeof full.bucket_metrics_json==="object"?full.bucket_metrics_json:{},
            ic_data: typeof full.ic_data_json==="object"?full.ic_data_json:[],
            bucket_equity: typeof full.bucket_equity_json==="object"?full.bucket_equity_json:{},
            annual_ret_by_bucket: typeof full.annual_ret_json==="object"?full.annual_ret_json:{},
            factor_metrics: m,
            n_obs: m.n_obs,
            n_stocks_avg: m.n_stocks_avg,
            config: typeof full.config_json==="object"?full.config_json:{},
          }
        };
        if (!_fl_runs.find(r => r.run_id === m.strategy_id)) {
          _fl_runs.unshift(runObj);
          _renderRunsTable();
        }
        _fl_active = m.strategy_id;
        _renderRunsTable();
        if (typeof _showTearsheet === "function") _showTearsheet(m.strategy_id);
        _flSetStatus("Loaded: " + m.strategy_id + " | ICIR=" + num(m.icir,3) + " | Spread=" + pct(m.quintile_spread_cagr,2));
      };
      body.appendChild(tr);
    });
  };

  // Auto-load bank on init
  const _origFactorLabInit = window.factorLabInit;
  window.factorLabInit = function () {
    if (_origFactorLabInit) _origFactorLabInit();
    window.flLoadBank();
  };

})();
