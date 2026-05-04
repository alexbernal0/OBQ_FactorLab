// portfolio_lab.js — Portfolio Models tab
// Top-N portfolio backtests with sector caps and stop-loss support.
// Tearsheet uses the standard tearsheet.js renderer (same as Results tab).

(function () {
  "use strict";

  let _pm_runs   = [];      // [{run_id, run_label, status, result, metrics}]
  let _pm_active = null;
  let _pm_uid    = 0;
  let _pm_scores = {};      // {col: display_name}

  function _uid() { return "plt_pm_" + (++_pm_uid); }

  // ── Init ──────────────────────────────────────────────────────────────────
  window.pmLabInit = function () {
    _loadPmScores();
    pmLoadBank();
    const endEl = document.getElementById("pm-end-date");
    if (endEl && !endEl.value) endEl.valueAsDate = new Date();
  };

  async function _loadPmScores() {
    const r = await fetch("/api/portfolio/scores").then(r => r.json()).catch(() => null);
    if (!r || !r.scores) return;
    _pm_scores = r.scores;
    const sel = document.getElementById("pm-score-col");
    if (!sel) return;
    sel.innerHTML = "";
    Object.entries(r.scores).forEach(([col, name]) => {
      const opt = document.createElement("option");
      opt.value = col; opt.textContent = name;
      if (col === "jcn_full_composite") opt.selected = true;
      sel.appendChild(opt);
    });
  }

  // ── Run ───────────────────────────────────────────────────────────────────
  window.pmRunBacktest = async function () {
    const btn      = document.getElementById("pm-run-btn");
    const score    = document.getElementById("pm-score-col")?.value;
    const topN     = parseInt(document.getElementById("pm-top-n")?.value) || 20;
    const secMax   = parseInt(document.getElementById("pm-sector-max")?.value) || 5;
    const rebal    = document.getElementById("pm-rebal-freq")?.value || "quarterly";
    const start    = document.getElementById("pm-start-date")?.value;
    const end      = document.getElementById("pm-end-date")?.value;
    const capTier  = document.getElementById("pm-cap-tier")?.value || "all";
    const minPrice = parseFloat(document.getElementById("pm-min-price")?.value) || 5.0;
    const costBps  = parseFloat(document.getElementById("pm-cost-bps")?.value) || 15.0;
    const stopLoss = parseFloat(document.getElementById("pm-stop-loss")?.value) || 0.0;

    const scoreName = _pm_scores[score] || score;
    const label = `${scoreName} | Top-${topN} | ${rebal} | ${capTier} | ${start?.slice(0,4)}-${end?.slice(0,4)}${stopLoss > 0 ? ` | SL${stopLoss}%` : ""}`;

    if (btn) { btn.disabled = true; btn.textContent = "Running..."; }
    _pmSetStatus("Running portfolio backtest...");
    _pmSetProgress(5);

    const payload = {
      score_column:   score,
      top_n:          topN,
      sector_max:     secMax,
      rebalance_freq: rebal,
      start_date:     start,
      end_date:       end,
      cap_tier:       capTier,
      min_price:      minPrice,
      cost_bps:       costBps,
      stop_loss_pct:  stopLoss / 100,
      run_label:      label,
    };

    const resp = await fetch("/api/portfolio/run", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload),
    }).then(r => r.json()).catch(e => ({ error: String(e) }));

    if (resp.error) {
      _pmSetStatus("Error: " + resp.error);
      if (btn) { btn.disabled = false; btn.textContent = "RUN PORTFOLIO"; }
      _pmSetProgress(0);
      return;
    }

    const run_id = resp.run_id;
    const runObj = { run_id, run_label: label, status: "running", result: null, metrics: null };
    _pm_runs.unshift(runObj);
    _pm_active = run_id;
    _renderPmTable();

    // SSE progress stream
    const sse = new EventSource(`/api/portfolio/stream/${run_id}`);
    sse.onmessage = e => {
      try {
        const d = JSON.parse(e.data);
        if (d.msg) _pmSetStatus(d.msg);
      } catch {}
    };
    sse.onerror = () => sse.close();

    // Poll
    const poll = async () => {
      const info = await fetch(`/api/portfolio/status/${run_id}`).then(r => r.json()).catch(() => null);
      if (!info || info.status === "running") { _pmSetProgress(50); setTimeout(poll, 2000); return; }
      sse.close();
      _pmSetProgress(90);

      const result = await fetch(`/api/portfolio/result/${run_id}`).then(r => r.json()).catch(() => null);
      const run = _pm_runs.find(r => r.run_id === run_id);
      if (run) {
        run.status  = result?.status === "error" ? "error" : "complete";
        run.result  = result;
        run.metrics = result?.portfolio_metrics || {};
      }
      _renderPmTable();
      _pmSetProgress(0);
      if (btn) { btn.disabled = false; btn.textContent = "RUN PORTFOLIO"; }

      if (result?.status === "error") {
        _pmSetStatus("Error: " + result.error);
      } else {
        const pm = result?.portfolio_metrics || {};
        const spy = result?.spy_metrics || {};
        _pmSetStatus(
          `Done | CAGR: ${((pm.cagr||0)*100).toFixed(2)}%` +
          ` | Sharpe: ${(pm.sharpe||0).toFixed(2)}` +
          ` | MaxDD: ${((pm.max_dd||0)*100).toFixed(1)}%` +
          ` | vs SPY: ${((spy.cagr||0)*100).toFixed(2)}%` +
          ` | ${result?.elapsed_s}s`
        );
        if (info.strategy_id) {
          run.strategy_id = info.strategy_id;
          _pmSetStatus(_pmGetStatus() + " | Saved: " + info.strategy_id);
          pmLoadBank();
        }
        _showPmTearsheet(run_id);
      }
    };
    setTimeout(poll, 2000);
  };

  function _pmGetStatus() {
    return document.getElementById("pm-status-bar")?.textContent || "";
  }

  // ── Run table ─────────────────────────────────────────────────────────────
  function _renderPmTable() {
    const body  = document.getElementById("pm-tbl-body");
    const empty = document.getElementById("pm-tbl-empty");
    if (!body) return;
    if (!_pm_runs.length) { if (empty) empty.style.display = "block"; return; }
    if (empty) empty.style.display = "none";
    body.innerHTML = "";

    _pm_runs.forEach(run => {
      const m = run.metrics || {};
      const isActive  = run.run_id === _pm_active;
      const isRunning = run.status === "running";
      const tr = document.createElement("div");
      tr.className = "fl-tr" + (isActive ? " active" : "");
      tr.onclick = () => { _pm_active = run.run_id; _renderPmTable(); _showPmTearsheet(run.run_id); };

      function pct(v,d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
      function num(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

      const icon = isRunning ? "⌛" : (run.status === "error" ? "✗" : "✓");
      const cagr = m.cagr;

      tr.innerHTML = `
        <div class="fl-td ${(cagr||0)>=0?"g":"r"}" style="flex:0 0 60px">${pct(cagr,2)}</div>
        <div class="fl-td" style="flex:0 0 46px">${num(m.sharpe,2)}</div>
        <div class="fl-td r" style="flex:0 0 55px">${pct(m.max_dd,1)}</div>
        <div class="fl-td" style="flex:0 0 46px">${m.win_rate_monthly!=null?((m.win_rate_monthly)*100).toFixed(0)+"%":"—"}</div>
        <div class="fl-td" style="flex:0 0 46px">${num(m.calmar,2)}</div>
        <div class="fl-td dim" style="flex:1;font-size:9px">${icon} ${run.run_label.slice(0,50)}</div>
      `;
      body.appendChild(tr);
    });
  }

  // ── Tearsheet ─────────────────────────────────────────────────────────────
  // Delegates to tearsheet.js _renderTearsheet logic by injecting the run
  // into a temporary slot then calling it.
  function _showPmTearsheet(run_id) {
    const run = _pm_runs.find(r => r.run_id === run_id);
    if (!run || !run.result || run.result.status === "error") return;

    const tsEmpty   = document.getElementById("pm-ts-empty");
    const tsContent = document.getElementById("pm-ts-content");
    if (!tsEmpty || !tsContent) return;

    tsEmpty.style.display = "none";
    tsContent.style.display = "flex";
    tsContent.style.flexDirection = "column";
    tsContent.style.overflowY = "auto";
    tsContent.innerHTML = "";

    const result  = run.result;
    const pm      = result.portfolio_metrics || {};
    const spy     = result.spy_metrics || {};
    const bm      = result.bm_metrics  || {};

    // ── Header ───────────────────────────────────────────────────────────────
    const hdr = document.createElement("div");
    hdr.style.cssText = "display:flex;align-items:center;justify-content:space-between;padding:8px 14px;background:var(--bg-panel);border-bottom:1px solid var(--border);flex-shrink:0";
    hdr.innerHTML = `
      <div>
        <div style="font-size:13px;font-weight:800;color:#0066cc;letter-spacing:1px">${run.run_label}</div>
        <div style="font-size:9px;color:var(--text-muted);margin-top:2px">
          ${result.equity_dates?.[0]||""} → ${result.equity_dates?.[result.equity_dates.length-1]||""}
          · ${result.n_periods||0} periods
          · Top-${result.config?.top_n||"?"} | ${result.config?.sector_max||"?"}/ sector
          ${run.strategy_id ? "· " + run.strategy_id : ""}
        </div>
      </div>
      <div style="display:flex;gap:6px">
        <button onclick="pmExportCSV('${run_id}')"  style="background:#16a34a;color:#fff;border:none;padding:4px 10px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">&#128202; CSV</button>
        <button onclick="pmExportPDF('${run_id}')"  style="background:#0066cc;color:#fff;border:none;padding:4px 10px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">&#128196; PDF</button>
        <button onclick="pmSnapTearsheet('${run_id}')" style="background:#7c3aed;color:#fff;border:none;padding:4px 10px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">&#128247; PNG</button>
      </div>
    `;
    tsContent.appendChild(hdr);

    // ── KPI strip: Portfolio vs SPY (2 columns) ───────────────────────────────
    const kpiGrid = document.createElement("div");
    kpiGrid.style.cssText = "display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--border);flex-shrink:0";

    function kpiGroup(title, color, metrics) {
      const block = document.createElement("div");
      block.style.cssText = "background:var(--bg-panel);padding:10px 14px";
      let rows = `<div style="font-size:9px;font-weight:800;color:${color};letter-spacing:1.2px;text-transform:uppercase;margin-bottom:8px;border-bottom:2px solid ${color};padding-bottom:4px">${title}</div>`;
      metrics.forEach(([lbl, val, valColor]) => {
        rows += `<div style="display:flex;justify-content:space-between;margin-bottom:5px">
          <span style="font-size:10px;color:var(--text-dim)">${lbl}</span>
          <span style="font-size:11px;font-weight:700;color:${valColor||"var(--text)"}">${val}</span>
        </div>`;
      });
      block.innerHTML = rows;
      return block;
    }

    function pct(v,d=2,sign=false) { if(v==null||isNaN(+v)) return "—"; const s=(v*100).toFixed(d); return (sign&&v>=0?"+":"")+s+"%"; }
    function num(v,d=2) { if(v==null||isNaN(+v)) return "—"; return Number(v).toFixed(d); }
    function green(v) { return (v||0)>=0?"#16a34a":"#dc2626"; }

    const alpha = pm.alpha; // vs SPY since that's now the benchmark
    kpiGrid.appendChild(kpiGroup("Portfolio", "#0066cc", [
      ["CAGR",          pct(pm.cagr,2,true),                     green(pm.cagr)],
      ["Sharpe Ratio",  num(pm.sharpe,3),                        (pm.sharpe||0)>=1?"#16a34a":(pm.sharpe||0)>=0.5?"var(--text)":"#dc2626"],
      ["Max Drawdown",  pct(pm.max_dd,1),                        "#dc2626"],
      ["Calmar (GIPS)", num(pm.calmar_gips||pm.calmar,3),       (pm.calmar_gips||pm.calmar||0)>=0.5?"#16a34a":"var(--text)"],
      ["Sortino Ratio", num(pm.sortino,2),                       "var(--text)"],
      ["Win Rate (Mo)", pm.win_rate_monthly!=null?(pm.win_rate_monthly*100).toFixed(0)+"%":"—","var(--text)"],
      ["Alpha vs SPY",  alpha!=null?pct(alpha,2,true):"—",       green(alpha)],
      ["Beta vs SPY",   num(pm.beta,2),                          "var(--text)"],
    ]));
    kpiGrid.appendChild(kpiGroup("S&amp;P 500 (SPY Benchmark)", "#f59e0b", [
      ["CAGR",          pct(spy.cagr,2,true),                    green(spy.cagr)],
      ["Sharpe Ratio",  num(spy.sharpe,3),                       "var(--text)"],
      ["Max Drawdown",  pct(spy.max_dd,1),                       "#dc2626"],
      ["Calmar Ratio",  num(spy.calmar,3),                       "var(--text)"],
      ["Sortino Ratio", num(spy.sortino,2),                      "var(--text)"],
      ["Win Rate (Mo)", spy.win_rate_monthly!=null?(spy.win_rate_monthly*100).toFixed(0)+"%":"—","var(--text)"],
      ["Excess CAGR",   pct((pm.cagr||0)-(spy.cagr||0),2,true), green((pm.cagr||0)-(spy.cagr||0))],
      ["Value $10K",    spy.terminal_wealth!=null?"$"+Math.round(spy.terminal_wealth).toLocaleString():"—","var(--text)"],
    ]));
    tsContent.appendChild(kpiGrid);

    // ── Charts using tearsheet.js helpers ────────────────────────────────────
    // Inject this run into the global _runs array temporarily so _renderTearsheet works
    // Actually: render charts directly using Plotly since tearsheet.js is tightly coupled
    // to the Results tab DOM. We'll build the key charts inline.

    function chartRow2(cols) {
      const r = document.createElement("div");
      r.style.cssText = `display:grid;grid-template-columns:${cols===1?"1fr":"1fr 1fr"};gap:8px;padding:10px 12px;flex-shrink:0`;
      tsContent.appendChild(r);
      return r;
    }
    function chartBox2(title, h) {
      const box = document.createElement("div");
      box.style.cssText = "background:var(--bg-panel);border:1px solid var(--border);border-radius:4px;padding:8px 10px;overflow:hidden";
      if (title) box.innerHTML = `<div style="font-size:8px;font-weight:700;color:var(--text-dim);letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">${title}</div>`;
      const d = document.createElement("div");
      d.id = _uid(); d.style.cssText = `height:${h}px;width:100%`;
      box.appendChild(d);
      return { box, id: d.id };
    }
    function sec2(title) {
      const h = document.createElement("div");
      h.className = "ts-sec-hdr"; h.textContent = title;
      tsContent.appendChild(h);
    }

    sec2("EQUITY CURVES");
    const r1 = chartRow2(1);
    const { box: eqBox, id: eqId } = chartBox2("CUMULATIVE RETURN % (both indexed to 0% at start)", 280);
    r1.appendChild(eqBox);

    sec2("ANNUAL RETURNS");
    const r2 = chartRow2(1);
    const { box: annBox, id: annId } = chartBox2("", 240);
    r2.appendChild(annBox);

    sec2("DRAWDOWN ANALYSIS");
    const rDD = chartRow2(2);
    const { box: ddBox,  id: ddId  } = chartBox2("PORTFOLIO DRAWDOWN (Underwater Curve)", 200);
    const { box: ddBar,  id: ddBarId} = chartBox2("WORST PERIODS (Quarterly Returns)", 200);
    rDD.appendChild(ddBox);
    rDD.appendChild(ddBar);

    sec2("ROLLING RISK METRICS (8-PERIOD WINDOW)");
    const rRoll = chartRow2(2);
    const { box: rsBox, id: rsId } = chartBox2("ROLLING SHARPE RATIO", 200);
    const { box: rsoBox,id: rsoId} = chartBox2("ROLLING SORTINO RATIO", 200);
    rRoll.appendChild(rsBox);
    rRoll.appendChild(rsoBox);

    sec2("RETURN DISTRIBUTION");
    const rDist = chartRow2(2);
    const { box: distBox, id: distId } = chartBox2("PERIOD RETURN HISTOGRAM — Portfolio vs SPY", 200);
    const { box: volBox,  id: volId  } = chartBox2("ROLLING VOLATILITY (8-PERIOD)", 200);
    rDist.appendChild(distBox);
    rDist.appendChild(volBox);

    sec2("PERIOD RETURNS HEATMAP");
    const r3 = chartRow2(1);
    const { box: hmBox, id: hmId } = chartBox2("", 200);
    r3.appendChild(hmBox);

    sec2("PORTFOLIO COMPOSITION — MARKET CAP DISTRIBUTION");
    const r4 = chartRow2(2);
    const { box: mcHistBox, id: mcHistId } = chartBox2("MARKET CAP DISTRIBUTION (ALL PERIODS COMBINED)", 240);
    const { box: mcTimeBox, id: mcTimeId } = chartBox2("MEDIAN MARKET CAP OVER TIME ($B)", 240);
    r4.appendChild(mcHistBox);
    r4.appendChild(mcTimeBox);

    sec2("FULL METRICS");
    tsContent.appendChild(_buildPmMetricsTable(pm, spy, bm, result.config || {}, result.period_data || []));

    // ── TRADE LOG ────────────────────────────────────────────────────────────
    sec2("TRADE LOG — FULL AUDIT (every position entry/exit)");
    tsContent.appendChild(_buildPmTradeLog(result.trade_log || []));

    // Draw charts after DOM settles
    const FTC = {
      get bg()      { return getComputedStyle(document.body).getPropertyValue("--bg").trim()||"#ffffff"; },
      get plot_bg() { return getComputedStyle(document.body).getPropertyValue("--bg-panel").trim()||"#f8f9fa"; },
      get grid()    { return getComputedStyle(document.body).getPropertyValue("--border").trim()||"#e5e7eb"; },
      get tick()    { return getComputedStyle(document.body).getPropertyValue("--text-dim").trim()||"#374151"; },
    };
    const CFG = { displayModeBar: false, responsive: true };

    // Hoist shared data outside try blocks so all chart sections can access them
    const dates    = result.equity_dates     || [];
    const portEq   = result.portfolio_equity || [];
    const spyEqRaw = result.spy_equity       || [];

    setTimeout(() => {
      // ── Equity chart ──────────────────────────────────────────────────────
      try {

        // Both equity arrays are now resampled to the SAME quarterly dates by the backend.
        // SPY is already normalized to 1.0 at start. Convert both to % gain.
        const portPct = portEq.map(v => +((v / portEq[0] - 1) * 100).toFixed(2));
        const spyPct  = spyEqRaw.length === portEq.length
          ? spyEqRaw.map(v => +((v - 1) * 100).toFixed(2))   // already normalized to 1.0 base
          : [];

        const traces = [
          { x: dates.slice(0, portPct.length), y: portPct,
            type:"scatter", mode:"lines", name:"Portfolio",
            line:{ color:"#0066cc", width:2.5 },
            hovertemplate: "%{x}<br>Portfolio: %{y:.1f}%<extra></extra>" },
        ];
        if (spyPct.length > 0) {
          traces.push({
            x: dates.slice(0, spyPct.length), y: spyPct,
            type:"scatter", mode:"lines", name:"SPY",
            line:{ color:"#f59e0b", width:1.8, dash:"dash" },
            hovertemplate: "%{x}<br>SPY: %{y:.1f}%<extra></extra>"
          });
        }
        // Alpha shade between the two lines
        if (spyPct.length === portPct.length) {
          traces.push({
            x: [...dates.slice(0, portPct.length), ...dates.slice(0, portPct.length).reverse()],
            y: [...portPct, ...spyPct.slice().reverse()],
            fill:"toself", fillcolor:"rgba(0,102,204,0.07)",
            line:{color:"transparent"}, showlegend:false, hoverinfo:"skip",
            type:"scatter"
          });
        }

        Plotly.newPlot(eqId, traces, {
          paper_bgcolor: FTC.bg, plot_bgcolor: FTC.plot_bg,
          margin: { l:58, r:12, t:12, b:40 },
          font: { family:"Segoe UI,Arial", size:9, color: FTC.tick },
          xaxis: { type:"date", gridcolor: FTC.grid, tickfont:{size:8} },
          yaxis: {
            gridcolor: FTC.grid, tickfont:{size:8},
            ticksuffix:"%",
            zeroline:true, zerolinecolor:FTC.tick, zerolinewidth:1,
            title:{ text:"Cumulative Return %", font:{size:8} }
          },
          legend: { orientation:"h", x:0, y:1.08, font:{size:9} },
          hovermode: "x unified",
        }, CFG);
      } catch(e) { console.error("equity chart", e); }

      // ── Annual returns bar chart ──────────────────────────────────────────
      try {
        const annual = result.annual_ret_by_year || [];
        const years  = annual.map(d => String(d.year));
        const portR  = annual.map(d => +((d.portfolio_ret||0)*100).toFixed(2));
        const spyR   = annual.map(d => d.spy_ret != null ? +((d.spy_ret)*100).toFixed(2) : null);

        const annTraces = [
          { x: years, y: portR, type:"bar", name:"Portfolio",
            marker:{ color: portR.map(v => v>=0?"rgba(0,102,204,0.85)":"rgba(220,38,38,0.85)") },
            text: portR.map(v=>(v>=0?"+":"")+v+"%"), textposition:"outside",
            textfont:{size:8, color:"var(--text-dim)"}, cliponaxis:false },
          { x: years, y: spyR, type:"scatter", mode:"lines+markers", name:"SPY",
            line:{ color:"#f59e0b", width:2 }, marker:{size:5, color:"#f59e0b"} },
        ].filter(t => t.y && t.y.some(v=>v!=null));

        Plotly.newPlot(annId, annTraces, {
          paper_bgcolor: FTC.bg, plot_bgcolor: FTC.plot_bg,
          margin: { l:48, r:8, t:28, b:40 },
          font: { family:"Segoe UI,Arial", size:9, color: FTC.tick },
          barmode: "overlay",
          xaxis: { type:"-", gridcolor: FTC.grid, tickfont:{size:8} },
          yaxis: { gridcolor: FTC.grid, tickfont:{size:8}, ticksuffix:"%",
                   zeroline:true, zerolinecolor:FTC.tick },
          legend: { orientation:"h", x:0, y:1.12, font:{size:9} },
        }, CFG);
      } catch(e) { console.error("annual chart", e); }

      // ── Drawdown underwater chart ─────────────────────────────────────────
      try {
        if (portEq.length > 1) {
          let peak = portEq[0];
          const ddPct = portEq.map(v => { peak = Math.max(peak, v); return +((v/peak - 1)*100).toFixed(2); });
          Plotly.newPlot(ddId, [{
            x: dates.slice(0, ddPct.length), y: ddPct,
            type:"scatter", mode:"lines", name:"Drawdown",
            line:{color:"#dc2626",width:1.5},
            fill:"tozeroy", fillcolor:"rgba(220,38,38,0.18)",
            hovertemplate:"%{x}<br>DD: %{y:.1f}%<extra></extra>",
          }], {
            paper_bgcolor:FTC.bg, plot_bgcolor:FTC.plot_bg,
            margin:{l:52,r:8,t:8,b:40},
            font:{family:"Segoe UI,Arial",size:9,color:FTC.tick},
            xaxis:{type:"date",gridcolor:FTC.grid,tickfont:{size:8}},
            yaxis:{gridcolor:FTC.grid,tickfont:{size:8},ticksuffix:"%",
                   zeroline:true,zerolinecolor:FTC.tick,title:{text:"DD%",font:{size:8}}},
            showlegend:false,
          }, CFG);

          // Worst N periods bar
          const pd = result.period_data || [];
          const sortedWorst = [...pd].sort((a,b)=>a.portfolio_return-b.portfolio_return).slice(0,15);
          const sortedBest  = [...pd].sort((a,b)=>b.portfolio_return-a.portfolio_return).slice(0,5);
          const worstAll = [...sortedWorst, ...sortedBest].sort((a,b)=>a.portfolio_return-b.portfolio_return);
          Plotly.newPlot(ddBarId, [{
            x: worstAll.map(p=>p.date),
            y: worstAll.map(p=>+(p.portfolio_return*100).toFixed(2)),
            type:"bar", name:"Period Return",
            marker:{color: worstAll.map(p=>p.portfolio_return>=0?"rgba(22,163,74,0.8)":"rgba(220,38,38,0.8)")},
            text: worstAll.map(p=>(p.portfolio_return>=0?"+":"")+((p.portfolio_return||0)*100).toFixed(1)+"%"),
            textposition:"outside", textfont:{size:8}, cliponaxis:false,
            hovertemplate:"%{x}<br>%{text}<extra></extra>",
          }], {
            paper_bgcolor:FTC.bg, plot_bgcolor:FTC.plot_bg,
            margin:{l:44,r:8,t:28,b:60},
            font:{family:"Segoe UI,Arial",size:8,color:FTC.tick},
            xaxis:{type:"-",tickangle:-45,tickfont:{size:7},gridcolor:FTC.grid},
            yaxis:{gridcolor:FTC.grid,tickfont:{size:8},ticksuffix:"%",zeroline:true,zerolinecolor:FTC.tick},
            title:{text:"Best 5 + Worst 15 Periods",font:{size:9,color:FTC.tick},x:0.5,xanchor:"center"},
            showlegend:false,
          }, CFG);
        }
      } catch(e) { console.error("drawdown", e); }

      // ── Rolling Sharpe + Sortino ──────────────────────────────────────────
      try {
        const portRets = portEq.slice(1).map((v,i) => v/portEq[i]-1);
        const W = 8;
        const rf_pp = 0.04 / 4;  // quarterly rf

        function rolling(arr, w, fn) {
          return arr.map((_,i) => i < w-1 ? null : fn(arr.slice(i-w+1,i+1)));
        }

        const rSharpe = rolling(portRets, W, w => {
          const m = w.reduce((a,b)=>a+b,0)/w.length;
          const s = Math.sqrt(w.reduce((a,b)=>a+(b-m)**2,0)/(w.length-1));
          return s>0 ? +((m - rf_pp)/s * Math.sqrt(4)).toFixed(3) : null;
        });
        const rSortino = rolling(portRets, W, w => {
          const m = w.reduce((a,b)=>a+b,0)/w.length;
          const neg = w.filter(v=>v<rf_pp);
          const dsd = neg.length>=2 ? Math.sqrt(neg.reduce((a,b)=>a+b**2,0)/neg.length) * Math.sqrt(4) : null;
          return dsd && dsd>0 ? +((m*4 - 0.04)/dsd).toFixed(3) : null;
        });

        const rollX = dates.slice(W);
        [[rsId, rSharpe, "Rolling Sharpe","#1d4ed8"], [rsoId, rSortino, "Rolling Sortino","#b45309"]].forEach(([divId, data, name, color]) => {
          const validX = rollX.filter((_,i)=>data[i]!=null);
          const validY = data.filter(v=>v!=null);
          Plotly.newPlot(divId, [
            {x:validX, y:validY, type:"scatter",mode:"lines",name,
             line:{color,width:2},fill:"tozeroy",fillcolor:color.replace(")",",0.1)").replace("rgb","rgba"),
             hovertemplate:`%{x}<br>${name}: %{y:.2f}<extra></extra>`},
            {x:validX, y:validX.map(()=>0), type:"scatter",mode:"lines",line:{color:FTC.tick,width:0.8},showlegend:false},
          ], {
            paper_bgcolor:FTC.bg, plot_bgcolor:FTC.plot_bg,
            margin:{l:48,r:8,t:8,b:36},
            font:{family:"Segoe UI,Arial",size:9,color:FTC.tick},
            xaxis:{type:"date",gridcolor:FTC.grid,tickfont:{size:8}},
            yaxis:{gridcolor:FTC.grid,tickfont:{size:8},zeroline:true,zerolinecolor:FTC.tick,
                   title:{text:name,font:{size:8}}},
            showlegend:false,
          }, CFG);
        });
      } catch(e) { console.error("rolling", e); }

      // ── Return distribution histogram ─────────────────────────────────────
      try {
        const pd2 = result.period_data || [];
        const portR2 = pd2.map(p => +((p.portfolio_return||0)*100).toFixed(2));
        const spyR2  = pd2.map(p => +((p.spy_return||0)*100).toFixed(2)).filter(v=>!isNaN(v));

        Plotly.newPlot(distId, [
          {x:portR2, type:"histogram", name:"Portfolio", opacity:0.75,
           marker:{color:"rgba(0,102,204,0.7)"}, nbinsx:20,
           hovertemplate:"Portfolio %{x:.1f}%<br>Count: %{y}<extra></extra>"},
        ], {
          paper_bgcolor:FTC.bg, plot_bgcolor:FTC.plot_bg,
          margin:{l:44,r:8,t:8,b:40},
          font:{family:"Segoe UI,Arial",size:9,color:FTC.tick},
          xaxis:{gridcolor:FTC.grid,tickfont:{size:8},ticksuffix:"%",title:{text:"Period Return",font:{size:8}}},
          yaxis:{gridcolor:FTC.grid,tickfont:{size:8},title:{text:"Count",font:{size:8}}},
          legend:{orientation:"h",x:0,y:1.08,font:{size:9}},
          barmode:"overlay",
        }, CFG);

        // Rolling vol
        const portRets2 = portEq.length > 1 ? portEq.slice(1).map((v,i)=>v/portEq[i]-1) : [];
        const W2=8;
        const rVol = portRets2.map((_,i) => {
          if(i<W2-1) return null;
          const w = portRets2.slice(i-W2+1,i+1);
          const m = w.reduce((a,b)=>a+b,0)/w.length;
          return +( Math.sqrt(w.reduce((a,b)=>a+(b-m)**2,0)/(w.length-1)) * Math.sqrt(4) * 100).toFixed(2);
        });
        const volX = dates.slice(W2).filter((_,i)=>rVol[i]!=null);
        const volY = rVol.filter(v=>v!=null);
        Plotly.newPlot(volId, [{
          x:volX, y:volY, type:"scatter",mode:"lines",name:"Rolling Vol",
          line:{color:"#7c3aed",width:1.5},fill:"tozeroy",fillcolor:"rgba(124,58,237,0.1)",
        }], {
          paper_bgcolor:FTC.bg, plot_bgcolor:FTC.plot_bg,
          margin:{l:48,r:8,t:8,b:36},
          font:{family:"Segoe UI,Arial",size:9,color:FTC.tick},
          xaxis:{type:"date",gridcolor:FTC.grid,tickfont:{size:8}},
          yaxis:{gridcolor:FTC.grid,tickfont:{size:8},ticksuffix:"%",
                 title:{text:"Ann. Vol %",font:{size:8}}},
          showlegend:false,
        }, CFG);
      } catch(e) { console.error("distribution", e); }

      // ── Period Returns Heatmap (quarterly/monthly/semi-annual aware) ────────
      try {
        const hm = result.monthly_heatmap || {};
        const years = Object.keys(hm).sort();

        // Detect period labels from the data (Q1/Q2/Q3/Q4 or Jan/Feb/... or H1/H2)
        const allLabels = new Set();
        years.forEach(yr => Object.keys(hm[yr] || {}).forEach(l => allLabels.add(l)));

        // Sort labels in logical order
        const quarterOrder = ["Q1","Q2","Q3","Q4"];
        const halfOrder    = ["H1","H2"];
        const monthOrder   = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
        let periodLabels;
        if (allLabels.has("Q1") || allLabels.has("Q2")) {
          periodLabels = quarterOrder.filter(l => allLabels.has(l));
        } else if (allLabels.has("H1") || allLabels.has("H2")) {
          periodLabels = halfOrder.filter(l => allLabels.has(l));
        } else {
          periodLabels = monthOrder.filter(l => allLabels.has(l));
        }

        if (periodLabels.length === 0) throw new Error("no period labels");

        const zData = periodLabels.map(lbl =>
          years.map(yr => hm[yr]?.[lbl] ?? null)
        );

        // Dynamic color range based on actual data
        const flatVals = zData.flat().filter(v => v != null);
        const maxAbs   = Math.min(Math.max(...flatVals.map(Math.abs)), 60);

        Plotly.newPlot(hmId, [{
          z: zData, x: years, y: periodLabels,
          type: "heatmap",
          colorscale: [[0,"#b91c1c"],[0.5,"#fefce8"],[1,"#15803d"]],
          zmid: 0, zmin: -maxAbs, zmax: maxAbs,
          text: zData.map(row => row.map(v => v==null?"":v.toFixed(1)+"%")),
          texttemplate: "%{text}", textfont:{size:9},
          showscale: true,
          colorbar:{ title:{text:"Ret%",side:"right"}, tickfont:{size:8}, thickness:14, len:0.9 },
          xgap:2, ygap:2,
        }], {
          paper_bgcolor: FTC.bg, plot_bgcolor: FTC.plot_bg,
          margin: { l:44, r:80, t:8, b:70 },
          font: { family:"Segoe UI,Arial", size:9, color: FTC.tick },
          xaxis: { tickangle:-45, tickfont:{size:8}, showgrid:false },
          yaxis: { tickfont:{size:11, color:FTC.tick}, showgrid:false, autorange:"reversed" },
        }, CFG);
      } catch(e) { console.error("heatmap", e); }

      // ── Market cap distribution charts ─────────────────────────────────────
      try {
        const holdingsLog = result.holdings_log || [];

        // Collect ALL market caps across ALL periods (in $B)
        const allMcaps = [];
        const periodMedians = [];  // [{date, median_mcap, min, max, q25, q75}]

        holdingsLog.forEach(period => {
          const mcs = (period.holdings || [])
            .map(h => h.market_cap)
            .filter(v => v != null && v > 0);
          if (mcs.length === 0) return;
          mcs.forEach(v => allMcaps.push(v));
          const sorted = [...mcs].sort((a,b) => a-b);
          const q = (arr, p) => arr[Math.floor(p * (arr.length-1))];
          periodMedians.push({
            date:   period.date,
            median: q(sorted, 0.5),
            q25:    q(sorted, 0.25),
            q75:    q(sorted, 0.75),
            min:    sorted[0],
            max:    sorted[sorted.length-1],
          });
        });

        // ── Chart 1: Histogram of all market caps (log scale) ──────────────
        if (allMcaps.length > 0) {
          // Cap tier boundaries in $B
          const tiers = [
            { name:"Micro\n<$0.3B",  min:0,    max:0.3   },
            { name:"Small\n$0.3-2B", min:0.3,  max:2     },
            { name:"Mid\n$2-10B",    min:2,    max:10    },
            { name:"Large\n$10-200B",min:10,   max:200   },
            { name:"Mega\n>$200B",   min:200,  max:Infinity },
          ];
          const tierCounts = tiers.map(t =>
            allMcaps.filter(v => v >= t.min && v < t.max).length
          );
          const tierColors = ["#dc2626","#f59e0b","#16a34a","#0066cc","#7c3aed"];
          const tierPct = tierCounts.map(c => +((c/allMcaps.length)*100).toFixed(1));

          Plotly.newPlot(mcHistId, [{
            x: tiers.map(t => t.name),
            y: tierCounts,
            type:"bar",
            marker:{ color: tierColors, opacity:0.85 },
            text: tierPct.map(p => p+"%"),
            textposition:"outside",
            textfont:{size:10},
            cliponaxis:false,
            hovertemplate: "%{x}<br>Count: %{y}<br>%{text} of holdings<extra></extra>",
          }], {
            paper_bgcolor:FTC.bg, plot_bgcolor:FTC.plot_bg,
            margin:{l:44,r:8,t:28,b:50},
            font:{family:"Segoe UI,Arial",size:9,color:FTC.tick},
            xaxis:{gridcolor:FTC.grid,tickfont:{size:8}},
            yaxis:{gridcolor:FTC.grid,tickfont:{size:8},title:{text:"# Holdings",font:{size:8}}},
            showlegend:false,
            annotations:[{
              text:`n=${allMcaps.length} total holdings across ${holdingsLog.length} periods`,
              xref:"paper",yref:"paper",x:0.5,y:1.06,
              showarrow:false,font:{size:8,color:FTC.tick},xanchor:"center"
            }]
          }, CFG);
        }

        // ── Chart 2: Median market cap over time (with IQR band) ───────────
        if (periodMedians.length > 0) {
          const ptDates  = periodMedians.map(p => p.date);
          const medians  = periodMedians.map(p => +(p.median).toFixed(2));
          const q25s     = periodMedians.map(p => +(p.q25).toFixed(2));
          const q75s     = periodMedians.map(p => +(p.q75).toFixed(2));

          Plotly.newPlot(mcTimeId, [
            // IQR band (Q25-Q75)
            {
              x: [...ptDates, ...ptDates.slice().reverse()],
              y: [...q75s,    ...q25s.slice().reverse()],
              fill:"toself", fillcolor:"rgba(0,102,204,0.12)",
              line:{color:"transparent"}, showlegend:true,
              name:"IQR (25-75%)", hoverinfo:"skip", type:"scatter"
            },
            // Median line
            {
              x: ptDates, y: medians,
              type:"scatter", mode:"lines+markers",
              name:"Median Mkt Cap",
              line:{color:"#0066cc",width:2},
              marker:{size:3,color:"#0066cc"},
              hovertemplate:"%{x}<br>Median: $%{y}B<extra></extra>",
            },
          ], {
            paper_bgcolor:FTC.bg, plot_bgcolor:FTC.plot_bg,
            margin:{l:52,r:8,t:12,b:40},
            font:{family:"Segoe UI,Arial",size:9,color:FTC.tick},
            xaxis:{type:"date",gridcolor:FTC.grid,tickfont:{size:8}},
            yaxis:{
              gridcolor:FTC.grid,tickfont:{size:8},
              ticksuffix:"B",
              title:{text:"Market Cap ($B)",font:{size:8}},
            },
            legend:{orientation:"h",x:0,y:1.08,font:{size:8}},
            hovermode:"x unified",
          }, CFG);
        }
      } catch(e) { console.error("mktcap charts", e); }

    }, 200);

    // Resize charts — multiple passes including late pass for bank-loaded tearsheets
    [300, 600, 1000, 1800, 3000, 5000].forEach(delay => setTimeout(() => {
      document.querySelectorAll("#pm-ts-content [id^=plt_pm_]").forEach(d => {
        try {
          if (d.offsetHeight === 0 && d.parentElement) {
            const ph = parseInt(d.style.height) || 200;
            d.style.height = ph + "px";
          }
          Plotly.Plots.resize(d);
        } catch(e) {}
      });
    }, delay));
  }

  // ── Metrics table ─────────────────────────────────────────────────────────
  function _buildPmMetricsTable(pm, spy, bm, cfg, periodData) {
    cfg = cfg || {};
    periodData = periodData || [];
    const wrap = document.createElement("div");
    wrap.style.cssText = "overflow-x:auto;flex-shrink:0;border:1px solid var(--border);border-radius:4px;margin:0 12px 12px";

    function pct(v,d=2,sign=false) { if(v==null||isNaN(+v)) return "—"; const s=(v*100).toFixed(d); return (sign&&v>=0?"+":"")+s+"%"; }
    function num(v,d=3) { if(v==null||isNaN(+v)) return "—"; return Number(v).toFixed(d); }
    function dollar(v) { if(v==null||isNaN(+v)) return "—"; return "$"+Math.round(v).toLocaleString(); }

    const S  = 'style="padding:4px 10px;text-align:right;border-bottom:1px solid var(--border);font-size:9px"';
    const SL = 'style="padding:4px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:9px;font-weight:600;color:var(--text)"';
    const SH = 'style="padding:5px 10px;text-align:right;background:var(--bg-panel2);color:var(--text-dim);font-size:8px;font-weight:700;text-transform:uppercase;border-bottom:2px solid var(--border)"';

    const rows = [
      ["CAGR",                    v=>pct(v,2,true), pm.cagr,              spy.cagr],
      ["Total Return",            v=>pct(v,1,true), pm.total_return,      spy.total_return],
      ["Value of $10,000",        v=>dollar(v!=null?10000*(1+(v||0)):null),pm.total_return,spy.total_return],
      ["Ann. Volatility",         v=>pct(v,2),      pm.ann_vol,           spy.ann_vol],
      ["Sharpe Ratio",            v=>num(v,3),      pm.sharpe,            spy.sharpe],
      ["Sortino Ratio",           v=>num(v,3),      pm.sortino,           spy.sortino],
      ["Calmar (GIPS: CAGR/MaxDD)",v=>num(v,3),      pm.calmar_gips||pm.calmar, spy.calmar_gips||spy.calmar],
      ["Calmar (OBQ: CAGR×Win/DD)",v=>num(v,3),     pm.calmar,            spy.calmar],
      ["Max Drawdown",            v=>pct(v,1),      pm.max_dd,            spy.max_dd],
      ["Max DD Duration (months)",v=>v!=null?Math.round(v)+"mo":"—", pm.max_dd_duration, spy.max_dd_duration],
      ["Win Rate (Monthly)",      v=>pct(v,1),      pm.win_rate_monthly,  spy.win_rate_monthly],
      ["Best Month",              v=>pct(v,1,true), pm.best_month,        spy.best_month],
      ["Worst Month",             v=>pct(v,1),      pm.worst_month,       spy.worst_month],
      ["Omega Ratio",             v=>num(v,3),      pm.omega,             spy.omega],
      ["Surefire Ratio",          v=>num(v,1),      pm.surefire_ratio,    spy.surefire_ratio],
      ["Equity R²",               v=>num(v,4),      pm.equity_r2,         spy.equity_r2],
      ["Alpha vs SPY (Ann.)",     v=>pct(v,2,true), pm.alpha,             null],
      ["Beta vs SPY",             v=>num(v,3),      pm.beta,              null],
      ["Info Ratio",              v=>num(v,3),      pm.info_ratio,        null],
      ["Tracking Error",          v=>pct(v,2),      pm.tracking_error,    null],
      ["Up Capture vs SPY",       v=>pct(v,1),      pm.up_capture,        null],
      ["Down Capture vs SPY",     v=>pct(v,1),      pm.down_capture,      null],
      ["Omega Ratio",             v=>num(v,3),      pm.omega,             spy.omega],
      ["Surefire Ratio",          v=>num(v,1),      pm.surefire_ratio,    spy.surefire_ratio],
      ["Equity R²",               v=>num(v,4),      pm.equity_r2,         spy.equity_r2],
      ["Lake Ratio",              v=>num(v,4),      pm.lake_ratio,        spy.lake_ratio],
      ["Pain Ratio",              v=>num(v,3),      pm.pain_ratio,        spy.pain_ratio],
    ];

    // Compute optimization metrics from period data
    const avgTurnover = periodData.length > 0
      ? periodData.reduce((s,p) => s + (p.turnover_pct||0), 0) / periodData.length
      : null;
    const portRetsArr = periodData.map(p => p.portfolio_return||0);
    const bestQ  = portRetsArr.length ? Math.max(...portRetsArr) : null;
    const worstQ = portRetsArr.length ? Math.min(...portRetsArr) : null;
    const avgPeriodRet = portRetsArr.length ? portRetsArr.reduce((a,b)=>a+b,0)/portRetsArr.length : null;

    let html = `<table style="width:100%;border-collapse:collapse;font-size:9px;font-family:'Segoe UI',sans-serif">
      <thead><tr>
        <th ${SH} style="text-align:left;min-width:200px">METRIC</th>
        <th ${SH} style="color:#0066cc;min-width:110px">PORTFOLIO</th>
        <th ${SH} style="color:#f59e0b;min-width:110px">S&amp;P 500 (SPY)</th>
      </tr></thead><tbody>`;

    rows.forEach(([label, fmt, pVal, sVal], ri) => {
      const bg = ri%2===0?"background:var(--bg)":"background:var(--bg-panel)";
      function cell(v) {
        if (v == null) return `<td ${S} style="${S.slice(7,-1)};color:var(--text-dim)">—</td>`;
        return `<td ${S}>${fmt(v)}</td>`;
      }
      html += `<tr style="${bg}"><td ${SL}>${label}</td>${cell(pVal)}${cell(sVal)}</tr>`;
    });

    // ── Strategy Configuration section ────────────────────────────────────────
    html += `<tr><td colspan="3" style="padding:6px 10px 2px;background:var(--bg-panel2);font-size:8px;font-weight:700;color:var(--accent2);letter-spacing:1px;text-transform:uppercase;border-top:2px solid var(--border)">STRATEGY CONFIGURATION</td></tr>`;
    const cfgRows = [
      ["Score Factor",        cfg.score_column || "—"],
      ["Top N Holdings",      cfg.top_n != null ? cfg.top_n : "—"],
      ["Sector Max",          cfg.sector_max != null ? cfg.sector_max + " / sector" : "—"],
      ["Rebalance Frequency", cfg.rebalance_freq || "—"],
      ["Start Date",          cfg.start_date || "—"],
      ["End Date",            cfg.end_date || "—"],
      ["Transaction Cost",    cfg.cost_bps != null ? cfg.cost_bps + " bps" : "—"],
      ["Stop Loss",           cfg.stop_loss_pct > 0 ? (cfg.stop_loss_pct*100).toFixed(0)+"%" : "None"],
      ["Weight Scheme",       cfg.weight_scheme || "equal"],
    ];
    const optRows = [
      ["Avg Turnover / Period",  avgTurnover != null ? avgTurnover.toFixed(1)+"%" : "—"],
      ["Total Periods",          periodData.length || "—"],
      ["Best Single Period",     bestQ != null ? (bestQ>=0?"+":"")+(bestQ*100).toFixed(1)+"%" : "—"],
      ["Worst Single Period",    worstQ != null ? (worstQ*100).toFixed(1)+"%" : "—"],
      ["Avg Period Return",      avgPeriodRet != null ? (avgPeriodRet>=0?"+":"")+(avgPeriodRet*100).toFixed(2)+"%" : "—"],
    ];
    [...cfgRows, ...optRows].forEach(([lbl, val], ri) => {
      const bg = ri%2===0?"background:var(--bg)":"background:var(--bg-panel)";
      html += `<tr style="${bg}">
        <td ${SL}>${lbl}</td>
        <td colspan="2" style="padding:4px 10px;font-size:9px;color:var(--text)">${val}</td>
      </tr>`;
    });

    html += "</tbody></table>";
    wrap.innerHTML = html;
    return wrap;
  }

  // ── CSV Export ────────────────────────────────────────────────────────────
  window.pmExportCSV = async function (run_id) {
    const run = _pm_runs.find(r => r.run_id === run_id);
    if (!run || !run.result) return;
    const resp = await fetch("/api/export/csv", {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ run_label: run.run_label, result: run.result,
                             metrics: run.metrics || {} }),
    }).then(r => r.json()).catch(e => ({ error: String(e) }));
    if (resp.path) _pmSetStatus("CSV saved: " + resp.filename);
    else _pmSetStatus("CSV failed: " + (resp.error||"unknown"));
  };

  // ── PDF Export ────────────────────────────────────────────────────────────
  window.pmExportPDF = async function (run_id) {
    const run = _pm_runs.find(r => r.run_id === run_id);
    if (!run || !run.result) return;
    _pmSetStatus("Generating PDF...");
    const resp = await fetch("/api/export/pdf", {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ run_label: run.run_label, result: run.result,
                             metrics: run.metrics || {} }),
    }).then(r => r.json()).catch(e => ({ error: String(e) }));
    if (resp.path) _pmSetStatus("PDF saved: " + resp.filename);
    else _pmSetStatus("PDF failed: " + (resp.error||"unknown"));
  };

  // ── PNG Snap ──────────────────────────────────────────────────────────────
  window.pmSnapTearsheet = async function (run_id) {
    _pmSetStatus("Capturing screenshot...");
    const resp = await fetch("/api/snap").then(r => r.json()).catch(() => null);
    if (resp && resp.path)     _pmSetStatus("Screenshot saved: " + (resp.filename || resp.path));
    else if (resp && resp.img) _pmSetStatus("Screenshot captured");
    else                       _pmSetStatus("Snap failed");
  };

  // ── Bank load ─────────────────────────────────────────────────────────────
  window.pmLoadBank = async function () {
    const body  = document.getElementById("pm-bank-body");
    const empty = document.getElementById("pm-bank-empty");
    const count = document.getElementById("pm-bank-count");
    if (!body) return;

    const r = await fetch("/api/portfolio/bank").then(r => r.json()).catch(() => null);
    if (!r || !r.models) { if (empty) empty.textContent = "Error loading bank"; return; }

    const models = r.models || [];
    if (count) count.textContent = models.length + " saved";

    if (!models.length) {
      body.innerHTML = "";
      if (empty) { empty.style.display = "block"; empty.textContent = "No portfolio models yet"; }
      return;
    }
    if (empty) empty.style.display = "none";
    body.innerHTML = "";

    function pct(v,d=2) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
    function num(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

    models.forEach(m => {
      const tr = document.createElement("div");
      tr.className = "fl-tr";
      const cagr = m.cagr;

      const pmSid = m.strategy_id || "";
      tr.innerHTML = `
        <div class="fl-td dim" style="flex:0 0 140px;font-family:monospace;font-size:8px" title="${pmSid}">${pmSid}</div>
        <div class="fl-td ${(cagr||0)>=0?"g":"r"}" style="flex:0 0 55px">${pct(cagr,2)}</div>
        <div class="fl-td" style="flex:0 0 44px">${num(m.sharpe,2)}</div>
        <div class="fl-td r" style="flex:0 0 52px">${pct(m.max_dd,1)}</div>
        <div class="fl-td" style="flex:0 0 44px">${m.win_rate_monthly!=null?((m.win_rate_monthly)*100).toFixed(0)+"%":"—"}</div>
        <div class="fl-td" style="flex:0 0 44px">${num(m.calmar,2)}</div>
        <div class="fl-td dim" style="flex:1;font-size:8px;overflow:hidden;text-overflow:ellipsis;min-width:0">${(m.run_label||"").slice(0,35)}</div>
        <button
          title="Send to Results tab"
          onclick="event.stopPropagation();pmPromoteToResults('${pmSid}')"
          style="flex:0 0 22px;width:22px;height:18px;margin:0 3px;padding:0;background:var(--accent2);color:#fff;border:none;border-radius:3px;cursor:pointer;font-size:12px;font-weight:900;line-height:18px;text-align:center;align-self:center"
        >+</button>
      `;

      tr.onclick = async () => {
        document.querySelectorAll("#pm-bank-body .fl-tr").forEach(r => r.classList.remove("active"));
        tr.classList.add("active");
        _pmSetStatus("Loading " + m.strategy_id + "...");

        const full = await fetch("/api/portfolio/bank/" + m.strategy_id).then(r=>r.json()).catch(()=>null);
        if (!full) { _pmSetStatus("Error loading " + m.strategy_id); return; }

        const runObj = {
          run_id:      m.strategy_id,
          run_label:   m.run_label || m.strategy_id,
          strategy_id: m.strategy_id,
          status:      "complete",
          metrics:     full.portfolio_metrics_json || {},
          result: {
            status:             "complete",
            run_label:          m.run_label,
            config:             full.config_json || {},
            portfolio_equity:   Array.isArray(full.portfolio_equity_json) ? full.portfolio_equity_json : [],
            spy_equity:         Array.isArray(full.spy_equity_json)       ? full.spy_equity_json       : [],
            equity_dates:       Array.isArray(full.equity_dates_json)     ? full.equity_dates_json     : [],
            portfolio_metrics:  full.portfolio_metrics_json || {},
            spy_metrics:        full.spy_metrics_json        || {},
            period_data:        Array.isArray(full.period_data_json)      ? full.period_data_json      : [],
            annual_ret_by_year: Array.isArray(full.annual_ret_json)       ? full.annual_ret_json       : [],
            monthly_heatmap:    full.monthly_heatmap_json    || {},
            holdings_log:       Array.isArray(full.holdings_log_json)     ? full.holdings_log_json     : [],
            trade_log:          Array.isArray(full.trade_log_json)        ? full.trade_log_json        : [],
            n_periods:          m.n_periods,
          }
        };

        if (!_pm_runs.find(r => r.run_id === m.strategy_id)) {
          _pm_runs.unshift(runObj);
          _renderPmTable();
        }
        _pm_active = m.strategy_id;
        _renderPmTable();
        _showPmTearsheet(m.strategy_id);
        _pmSetStatus("Loaded: " + m.strategy_id + " | CAGR=" + pct(m.cagr,2) + " | Sharpe=" + num(m.sharpe,2));
      };

      body.appendChild(tr);
    });
  };

  // ── Helpers ───────────────────────────────────────────────────────────────
  function _pmSetStatus(msg) {
    const el = document.getElementById("pm-status-bar");
    if (el) el.textContent = msg;
  }

  function _pmSetProgress(pct) {
    const el = document.getElementById("pm-prog-fill");
    if (el) el.style.width = pct + "%";
  }

  // ── Portfolio Trade Log table ─────────────────────────────────────────────
  function _buildPmTradeLog(tradeLog) {
    const wrap = document.createElement("div");
    wrap.style.cssText = "overflow:auto;flex-shrink:0;max-height:450px;margin:0 12px 12px;border:1px solid var(--border);border-radius:4px";

    if (!tradeLog || !tradeLog.length) {
      wrap.innerHTML = '<div style="padding:20px;text-align:center;font-size:10px;color:var(--text-muted)">No trade log — re-run backtest to generate</div>';
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
        <th ${SH}>WEIGHT</th>
        <th ${SH}>ENTRY $</th>
        <th ${SH}>EXIT DATE</th>
        <th ${SH}>EXIT $</th>
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
        <td ${S} style="font-weight:700;font-family:monospace;font-size:10px">${t.symbol || "—"}</td>
        <td ${S} style="font-size:8px;color:var(--text-dim)">${(t.sector||"").split(" ").slice(0,2).join(" ")}</td>
        <td ${S} style="text-align:right;color:#7c3aed">${t.score != null ? t.score.toFixed(1) : "—"}</td>
        <td ${S} style="text-align:right">${t.market_cap_B != null ? "$"+t.market_cap_B.toFixed(1)+"B" : "—"}</td>
        <td ${S} style="text-align:right">${t.weight != null ? (t.weight*100).toFixed(1)+"%" : "—"}</td>
        <td ${S} style="text-align:right">${t.entry_price != null ? "$"+t.entry_price.toFixed(2) : "—"}</td>
        <td ${S}>${t.exit_date || "—"}</td>
        <td ${S} style="text-align:right">${t.exit_price != null ? "$"+t.exit_price.toFixed(2) : "—"}</td>
        <td ${S} style="text-align:right;${retColor}">${ret != null ? (ret>=0?"+":"")+ ret.toFixed(2)+"%" : "—"}</td>
        <td ${S} style="${wlColor};text-align:center">${wl}</td>
      </tr>`;
    });

    const wins  = tradeLog.filter(t => t.return_pct != null && t.return_pct >= 0).length;
    const total = tradeLog.filter(t => t.return_pct != null).length;
    const avgRet = total > 0 ? tradeLog.filter(t=>t.return_pct!=null).reduce((s,t)=>s+t.return_pct,0)/total : 0;
    const bestTrade  = total > 0 ? Math.max(...tradeLog.filter(t=>t.return_pct!=null).map(t=>t.return_pct)) : null;
    const worstTrade = total > 0 ? Math.min(...tradeLog.filter(t=>t.return_pct!=null).map(t=>t.return_pct)) : null;
    html += `<tr style="background:var(--bg-panel2);font-weight:700">
      <td ${S} colspan="9" style="font-size:9px;color:var(--text-dim)">
        ${tradeLog.length} trades | Win rate: ${total > 0 ? ((wins/total)*100).toFixed(1) : "—"}% | 
        Best: ${bestTrade!=null?(bestTrade>=0?"+":"")+bestTrade.toFixed(1)+"%":"—"} | 
        Worst: ${worstTrade!=null?worstTrade.toFixed(1)+"%":"—"}
      </td>
      <td ${S} style="text-align:right;color:${avgRet>=0?"#16a34a":"#dc2626"}">${avgRet>=0?"+":""}${avgRet.toFixed(2)}%</td>
      <td ${S}></td>
    </tr>`;

    html += "</tbody></table>";
    wrap.innerHTML = html;
    return wrap;
  }

  // ── Promote portfolio model to Results tab ────────────────────────────────
  window.pmPromoteToResults = async function(strategyId) {
    _pmSetStatus("Promoting " + strategyId + " to Results...");
    const full = await fetch("/api/portfolio/bank/" + strategyId).then(r=>r.json()).catch(()=>null);
    if (!full) { _pmSetStatus("Error loading " + strategyId); return; }

    function _arr(f) { return Array.isArray(f) ? f : []; }
    function _obj(f) { return (f && typeof f === "object" && !Array.isArray(f)) ? f : {}; }

    const pm      = _obj(full.portfolio_metrics_json);
    const eq      = _arr(full.portfolio_equity_json);
    const dates   = _arr(full.equity_dates_json);
    const annRet  = _arr(full.annual_ret_json);
    const cfg     = _obj(full.config_json);

    const run_id    = "pm-promoted-" + strategyId;
    const run_label = (full.run_label || strategyId);

    const metrics = {
      cagr: pm.cagr, sharpe: pm.sharpe, max_dd: pm.max_dd,
      sortino: pm.sortino, ann_vol: pm.ann_vol, calmar: pm.calmar,
      omega: pm.omega, surefire_ratio: pm.surefire_ratio,
      win_rate_monthly: pm.win_rate_monthly, equity_r2: pm.equity_r2,
      profit_factor: pm.profit_factor, system_score: pm.system_score,
      n_periods: full.n_periods,
    };

    const result = {
      status: "complete", run_label, factor: "portfolio",
      mode: "topn", n_periods: full.n_periods,
      portfolio_equity: eq, equity_dates: dates,
      bm_equity: [], bm_metrics: {},
      portfolio_metrics: metrics,
      annual_ret_by_year: annRet,
      _promoted_from: strategyId,
    };

    if (typeof _runs !== "undefined") {
      const existing = _runs.findIndex(r => r.run_id === run_id);
      if (existing >= 0) _runs.splice(existing, 1);
      _runs.unshift({ run_id, run_label, status:"complete",
                      cfg:{factor:"portfolio",model_type:"topn"}, metrics, result });
      if (typeof renderTable === "function") renderTable();
    }
    if (typeof switchMainTab === "function") switchMainTab("results");
    setTimeout(() => {
      if (typeof setActiveRow === "function") setActiveRow(run_id);
      if (typeof showTearsheet === "function") showTearsheet(run_id);
    }, 200);
    _pmSetStatus("Promoted " + strategyId + " to Results tab");
  };

  // ── Cycle panel toggles ───────────────────────────────────────────────────
  window.pmToggleCycles = function () {
    const body = document.getElementById("pm-cycles-body");
    const chev = document.getElementById("pm-cycles-chevron");
    if (!body) return;
    const hidden = body.style.display === "none";
    body.style.display = hidden ? "" : "none";
    if (chev) chev.textContent = hidden ? "▾" : "▸";
  };
  window.pmToggleCycleDetail = function () {
    const detail = document.getElementById("pm-cyc001-detail");
    const btn    = document.getElementById("pm-cyc001-btn");
    if (!detail) return;
    const open = detail.style.display !== "none";
    detail.style.display = open ? "none" : "block";
    if (btn) btn.classList.toggle("open", !open);
  };

  // Auto-load bank on DOMContentLoaded via pmLabInit (called from tabs.js on tab switch)

})();
