// results_lab.js — Results tab: saved/promoted models from strategy bank

(function () {
  "use strict";

  let _res_active_id = null;
  let _res_models    = [];

  // Called when Results tab is activated
  window.resInit = function () {
    resRefresh();
  };

  // Load all saved models from bank
  window.resRefresh = async function () {
    const statusEl = document.getElementById("res-status");
    const typeFilter = document.getElementById("res-filter-type")?.value || "all";
    const sortCol    = document.getElementById("res-sort-col")?.value || "icir";
    if (statusEl) statusEl.textContent = "Loading from strategy bank...";

    const r = await fetch("/api/factor/bank").then(r => r.json()).catch(() => null);
    if (!r) { if (statusEl) statusEl.textContent = "Error loading bank"; return; }

    _res_models = (r.models || []).filter(m => {
      if (typeFilter === "factor")    return m.strategy_id?.startsWith("FM-");
      if (typeFilter === "portfolio") return m.strategy_id?.startsWith("PS-");
      return true;
    });

    const cnt = document.getElementById("res-count-badge");
    if (cnt) cnt.textContent = _res_models.length + " models";
    if (statusEl) statusEl.textContent = `${_res_models.length} models loaded  |  Best ICIR: ${r.summary?.best_icir?.toFixed(3)||"—"}  |  Best Spread: ${r.summary?.best_spread!=null?((r.summary.best_spread)*100).toFixed(2)+"% CAGR":"—"}`;

    _renderTable();
  };

  function _renderTable() {
    const body  = document.getElementById("res-tbl-body");
    const empty = document.getElementById("res-tbl-empty");
    if (!body) return;

    if (!_res_models.length) {
      if (empty) empty.style.display = "block";
      body.innerHTML = "";
      return;
    }
    if (empty) empty.style.display = "none";
    body.innerHTML = "";

    _res_models.forEach(m => {
      const isActive = m.strategy_id === _res_active_id;
      const isFactor = m.strategy_id?.startsWith("FM-");
      const tr = document.createElement("div");
      tr.className = "fl-tr" + (isActive ? " active" : "");
      tr.onclick   = () => { _res_active_id = m.strategy_id; _renderTable(); _showModel(m); };

      function pct(v, d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
      function num(v, d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

      const typeLabel = isFactor
        ? `<span style="background:#ede9fe;color:#7c3aed;border:1px solid #c4b5fd;border-radius:3px;padding:1px 5px;font-size:8px;font-weight:700">FM</span>`
        : `<span style="background:#dbeafe;color:#1d4ed8;border:1px solid #93c5fd;border-radius:3px;padding:1px 5px;font-size:8px;font-weight:700">PS</span>`;

      const dateStr = m.created_at ? String(m.created_at).slice(0,10) : "—";
      const spread  = m.quintile_spread_cagr;
      const sCol    = spread != null ? (spread >= 0.05 ? "g" : spread >= 0 ? "" : "r") : "";

      tr.innerHTML = `
        <div class="fl-td" style="flex:0 0 120px;font-size:9px;font-family:monospace;color:#374151">${m.strategy_id||"—"}</div>
        <div class="fl-td" style="flex:0 0 48px">${typeLabel}</div>
        <div class="fl-td ${sCol}" style="flex:0 0 60px">${pct(spread,2)}</div>
        <div class="fl-td" style="flex:0 0 48px">${num(m.ic_mean,4)}</div>
        <div class="fl-td" style="flex:0 0 44px">${num(m.icir,2)}</div>
        <div class="fl-td ${(m.q1_cagr||0)>=0?"g":"r"}" style="flex:0 0 50px">${pct(m.q1_cagr,1)}</div>
        <div class="fl-td" style="flex:0 0 48px">${num(m.q1_sharpe,2)}</div>
        <div class="fl-td dim" style="flex:0 0 60px">${dateStr}</div>
        <div class="fl-td dim" style="flex:1;font-size:9px">${(m.run_label||"").slice(0,45)}</div>
      `;
      body.appendChild(tr);
    });
  }

  function _showModel(m) {
    const tsEmpty   = document.getElementById("res-ts-empty");
    const tsContent = document.getElementById("res-ts-content");
    const activateBtn = document.getElementById("res-activate-btn");
    if (!tsEmpty || !tsContent) return;

    // Show activate button
    if (activateBtn) { activateBtn.style.display = "inline-block"; activateBtn.dataset.sid = m.strategy_id; }
    tsEmpty.style.display = "none";
    tsContent.classList.add("active");
    tsContent.innerHTML   = "";

    // Header
    const hdr = document.createElement("div");
    hdr.style.cssText = "display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#f8f9fa;border-bottom:1px solid #e5e7eb;flex-shrink:0";
    hdr.innerHTML = `
      <div>
        <div style="font-size:12px;font-weight:800;color:#0066cc;font-family:monospace">${m.strategy_id}</div>
        <div style="font-size:9px;color:#9ca3af;margin-top:2px">${m.run_label||""}</div>
        <div style="font-size:8px;color:#9ca3af">Saved ${(m.created_at||"").slice(0,19)} | ${m.n_obs||0} periods | ~${Math.round(m.n_stocks_avg||0)} stocks</div>
      </div>
      <div style="display:flex;gap:6px">
        <button class="btn-activate" onclick="resActivateSelected()">ACTIVATE IN TRACKER</button>
      </div>`;
    tsContent.appendChild(hdr);

    // KPI strip
    const kpi = document.createElement("div");
    kpi.style.cssText = "display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:#e5e7eb;flex-shrink:0";
    function kpiBox(lbl, val, col) {
      return `<div style="background:#fff;padding:8px 6px;text-align:center">
        <div style="font-size:8px;color:#6b7280;text-transform:uppercase;letter-spacing:.8px;margin-bottom:3px">${lbl}</div>
        <div style="font-size:14px;font-weight:700;color:${col}">${val}</div></div>`;
    }
    function pct(v,d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
    function num(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

    kpi.innerHTML = [
      kpiBox("SPREAD", pct(m.quintile_spread_cagr,2), (m.quintile_spread_cagr||0)>=0.05?"#16a34a":"#374151"),
      kpiBox("ICIR",   num(m.icir,3),              (m.icir||0)>=1?"#16a34a":"#374151"),
      kpiBox("IC HIT", m.ic_hit_rate!=null?((m.ic_hit_rate)*100).toFixed(1)+"%":"—", (m.ic_hit_rate||0)>=0.6?"#16a34a":"#374151"),
      kpiBox("Q1 CAGR",pct(m.q1_cagr),              (m.q1_cagr||0)>=0?"#16a34a":"#dc2626"),
      kpiBox("Q1 SHRPE",num(m.q1_sharpe,2),          (m.q1_sharpe||0)>=0.5?"#16a34a":"#374151"),
      kpiBox("MONO%",  m.monotonicity_score!=null?((m.monotonicity_score)*100).toFixed(0)+"%":"—", (m.monotonicity_score||0)>=0.8?"#16a34a":"#374151"),
    ].join("");
    tsContent.appendChild(kpi);

    // Metrics table
    const mt = document.createElement("div");
    mt.style.cssText = "padding:12px;flex:1;overflow-y:auto";
    mt.innerHTML = `
      <table style="width:100%;border-collapse:collapse;font-size:10px">
        <tr style="background:#f1f5f9"><th colspan="2" style="padding:6px 10px;text-align:left;font-size:9px;color:#374151;font-weight:700;letter-spacing:1px;text-transform:uppercase">SIGNAL QUALITY</th></tr>
        ${_metRow("IC Mean",             num(m.ic_mean,4))}
        ${_metRow("IC Std Dev",           num(m.ic_std,4))}
        ${_metRow("ICIR",                 num(m.icir,3))}
        ${_metRow("IC Hit Rate",          m.ic_hit_rate!=null?((m.ic_hit_rate)*100).toFixed(1)+"%":"—")}
        ${_metRow("Spearman Rho",         num(m.spearman_rho,3))}
        ${_metRow("Monotonicity",         m.monotonicity_score!=null?((m.monotonicity_score)*100).toFixed(1)+"%":"—")}
        <tr style="background:#f1f5f9"><th colspan="2" style="padding:6px 10px;text-align:left;font-size:9px;color:#374151;font-weight:700;letter-spacing:1px;text-transform:uppercase">QUINTILE PERFORMANCE</th></tr>
        ${_metRow("Q1-Q5 Spread",         pct(m.quintile_spread_cagr,2), (m.quintile_spread_cagr||0)>=0.05?"#16a34a":"#374151")}
        ${_metRow("Q1 CAGR",              pct(m.q1_cagr,2),              (m.q1_cagr||0)>=0?"#16a34a":"#dc2626")}
        ${_metRow("Q1 Sharpe",            num(m.q1_sharpe,3))}
        ${_metRow("Q1 Max DD",            pct(m.q1_max_dd,2),            "#dc2626")}
        ${_metRow("Q1 Surefire",          num(m.q1_surefire,1))}
        ${_metRow("Q1 Equity R²",         num(m.q1_equity_r2,4))}
        ${_metRow("Qn CAGR",              pct(m.qn_cagr,2),             (m.qn_cagr||0)>0?"#374151":"#dc2626")}
        <tr style="background:#f1f5f9"><th colspan="2" style="padding:6px 10px;text-align:left;font-size:9px;color:#374151;font-weight:700;letter-spacing:1px;text-transform:uppercase">CONFIG</th></tr>
        ${_metRow("Score Column",         m.score_column||"—")}
        ${_metRow("N Buckets",            m.n_buckets||"—")}
        ${_metRow("Hold Period",          m.hold_months?m.hold_months+"mo":"—")}
        ${_metRow("Rebalance",            m.rebalance_freq||"—")}
        ${_metRow("Cap Tier",             m.cap_tier||"—")}
        ${_metRow("Min Price",            m.min_price?"$"+m.min_price:"—")}
        ${_metRow("Period",               (m.start_date||"")+" → "+(m.end_date||""))}
        ${_metRow("N Periods",            m.n_obs||"—")}
        ${_metRow("Avg Stocks",           m.n_stocks_avg?Math.round(m.n_stocks_avg):"—")}
        ${m.notes ? _metRow("Notes", m.notes) : ""}
      </table>`;
    tsContent.appendChild(mt);
  }

  function _metRow(lbl, val, col="#374151") {
    return `<tr><td style="padding:3px 10px;color:#6b7280;border-bottom:1px solid #f3f4f6">${lbl}</td><td style="padding:3px 10px;text-align:right;font-weight:600;color:${col};border-bottom:1px solid #f3f4f6">${val}</td></tr>`;
  }

  window.resActivateSelected = function () {
    const sid = _res_active_id || document.getElementById("res-activate-btn")?.dataset?.sid;
    if (!sid) { alert("Select a model first"); return; }
    const model = _res_models.find(m => m.strategy_id === sid);
    if (!model) return;
    if (typeof trkActivate === "function") trkActivate(model);
    switchMainTab("tracker");
  };

})();
