// tracker_lab.js — Tracker tab: production live positions + rebalance report

(function () {
  "use strict";

  let _trk_strategy   = null;  // active strategy model dict
  let _trk_positions  = [];    // [{symbol, entry_date, entry_price, curr_price, curr_rank, score, sector, action, target_wt}]
  let _trk_candidates = [];    // today's new/sell candidates

  window.trkInit = function () {
    if (_trk_strategy) _renderTracker();
    else {
      const st = document.getElementById("trk-status");
      if (st) st.textContent = "No active strategy — go to Results tab and click ACTIVATE IN TRACKER";
    }
  };

  // Called from Results tab when user clicks ACTIVATE
  window.trkActivate = function (model) {
    _trk_strategy  = model;
    _trk_positions = [];
    _trk_candidates= [];

    const badge = document.getElementById("trk-strategy-badge");
    if (badge) {
      badge.textContent = model.strategy_id;
      badge.style.background = "#ede9fe";
      badge.style.color = "#7c3aed";
      badge.style.borderColor = "#c4b5fd";
    }

    const rBtn = document.getElementById("trk-report-btn");
    if (rBtn) rBtn.style.display = "block";

    _showStrategyPanel();
    _renderPositions();

    const st = document.getElementById("trk-status");
    if (st) st.textContent = `Active: ${model.strategy_id}  |  ${model.run_label}  |  ${model.rebalance_freq} rebalance`;
  };

  window.trkRefresh = async function () {
    const st = document.getElementById("trk-status");
    if (!_trk_strategy) {
      if (st) st.textContent = "No active strategy";
      return;
    }
    if (st) st.textContent = "Refreshing ranks from database...";
    // In production this would re-score all positions against latest scores
    // For now show placeholder
    if (st) st.textContent = `Active: ${_trk_strategy.strategy_id}  |  Ranks last updated: ${new Date().toLocaleTimeString()}`;
  };

  window.trkGenerateReport = function () {
    if (!_trk_strategy) return;
    const lines = [
      "OBQ FACTOR LAB — DAILY PORTFOLIO REPORT",
      `Strategy: ${_trk_strategy.strategy_id}`,
      `Generated: ${new Date().toISOString()}`,
      "",
      "CURRENT POSITIONS:",
      ..._trk_positions.map(p =>
        `  ${p.symbol.padEnd(8)} Rank:${String(p.curr_rank||"?").padEnd(5)} Score:${String(p.score||"?").padEnd(6)} PnL:${p.pnl_pct||"?"}`
      ),
      "",
      "TODAY'S ACTIONS:",
      ..._trk_candidates.map(c =>
        `  [${c.action.toUpperCase()}] ${c.symbol.padEnd(8)} Target Wt: ${c.target_wt||"?"}%`
      ),
    ];
    const blob = new Blob([lines.join("\n")], {type:"text/plain"});
    const a    = document.createElement("a");
    a.href     = URL.createObjectURL(blob);
    a.download = `OBQ_Report_${new Date().toISOString().slice(0,10)}.txt`;
    a.click();
  };

  function _showStrategyPanel() {
    const tsEmpty   = document.getElementById("trk-ts-empty");
    const tsContent = document.getElementById("trk-ts-content");
    if (!tsEmpty || !tsContent) return;
    if (!_trk_strategy) return;

    const m = _trk_strategy;
    tsEmpty.style.display = "none";
    tsContent.classList.add("active");
    tsContent.innerHTML   = "";

    function pct(v,d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
    function num(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

    tsContent.innerHTML = `
      <div style="padding:14px;flex:1">
        <div style="font-size:14px;font-weight:800;color:#7c3aed;font-family:monospace;margin-bottom:4px">${m.strategy_id}</div>
        <div style="font-size:10px;color:#6b7280;margin-bottom:16px">${m.run_label||""}</div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#e5e7eb;margin-bottom:16px">
          ${_kv("Q1-Q5 Spread", pct(m.quintile_spread_cagr,2), (m.quintile_spread_cagr||0)>=0.05?"#16a34a":"#374151")}
          ${_kv("ICIR",          num(m.icir,3), (m.icir||0)>=1?"#16a34a":"#374151")}
          ${_kv("IC Hit Rate",   m.ic_hit_rate!=null?((m.ic_hit_rate)*100).toFixed(1)+"%":"—")}
          ${_kv("Monotonicity",  m.monotonicity_score!=null?((m.monotonicity_score)*100).toFixed(0)+"%":"—")}
          ${_kv("Q1 CAGR",       pct(m.q1_cagr,2), (m.q1_cagr||0)>=0?"#16a34a":"#dc2626")}
          ${_kv("Q1 Sharpe",     num(m.q1_sharpe,2))}
        </div>

        <div style="background:#fef3c7;border:1px solid #fcd34d;border-radius:6px;padding:12px;margin-bottom:16px">
          <div style="font-size:10px;font-weight:700;color:#92400e;margin-bottom:8px">&#9888; PRODUCTION STATUS</div>
          <div style="font-size:9px;color:#78350f;line-height:1.6">
            This strategy is now ACTIVE in the Tracker.<br/>
            Add positions manually or connect to your broker API to sync.<br/>
            <strong>Rebalance frequency:</strong> ${m.rebalance_freq||"semi-annual"}<br/>
            <strong>Hold period:</strong> ${m.hold_months||6} months<br/>
            <strong>Top bucket held:</strong> Q1 (${m.n_buckets||5}-quintile model)
          </div>
        </div>

        <div style="font-size:9px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">ADD POSITION MANUALLY</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <input id="trk-add-symbol" placeholder="Symbol (e.g. AAPL.US)" style="font-size:10px;padding:5px 8px;border:1px solid #d1d5db;border-radius:3px;width:130px"/>
          <input id="trk-add-price"  placeholder="Entry price $" type="number" style="font-size:10px;padding:5px 8px;border:1px solid #d1d5db;border-radius:3px;width:110px"/>
          <input id="trk-add-date"   type="date" style="font-size:10px;padding:5px 8px;border:1px solid #d1d5db;border-radius:3px;width:130px"/>
          <button onclick="trkAddPosition()" style="background:#7c3aed;color:#fff;border:none;padding:5px 12px;font-size:9px;font-weight:700;cursor:pointer;border-radius:3px">+ ADD</button>
        </div>
      </div>`;
  }

  function _kv(lbl, val, col="#374151") {
    return `<div style="display:flex;justify-content:space-between;padding:5px 10px;background:#fff;border-bottom:1px solid #f3f4f6">
      <span style="font-size:10px;color:#6b7280">${lbl}</span>
      <span style="font-size:10px;font-weight:700;color:${col}">${val}</span></div>`;
  }

  window.trkAddPosition = function () {
    const sym   = document.getElementById("trk-add-symbol")?.value?.trim().toUpperCase();
    const price = parseFloat(document.getElementById("trk-add-price")?.value);
    const date  = document.getElementById("trk-add-date")?.value;
    if (!sym || isNaN(price)) { alert("Enter symbol and entry price"); return; }

    _trk_positions.push({
      symbol:      sym,
      entry_date:  date || new Date().toISOString().slice(0,10),
      entry_price: price,
      curr_price:  price,
      curr_rank:   "—",
      score:       "—",
      sector:      "—",
      action:      "hold",
      target_wt:   null,
      pnl_pct:     "0.00%",
    });

    // Clear inputs
    ["trk-add-symbol","trk-add-price","trk-add-date"].forEach(id => {
      const el = document.getElementById(id); if(el) el.value="";
    });

    _renderPositions();
  };

  function _renderPositions() {
    const body  = document.getElementById("trk-pos-body");
    const empty = document.getElementById("trk-pos-empty");
    if (!body) return;

    if (!_trk_positions.length) {
      if (empty) empty.style.display = "block";
      body.innerHTML = "";
      return;
    }
    if (empty) empty.style.display = "none";
    body.innerHTML = "";

    _trk_positions.forEach((p, idx) => {
      const pnl = ((p.curr_price - p.entry_price) / p.entry_price * 100).toFixed(2);
      const pnlCol = parseFloat(pnl) >= 0 ? "#16a34a" : "#dc2626";
      const actionBadge = {
        hold: '<span class="tracker-badge hold">HOLD</span>',
        sell: '<span class="tracker-badge sell">SELL</span>',
        buy:  '<span class="tracker-badge buy">BUY</span>',
        new:  '<span class="tracker-badge new">NEW</span>',
      }[p.action] || "";

      const tr = document.createElement("div");
      tr.className = "fl-tr";
      tr.innerHTML = `
        <div class="fl-td" style="flex:0 0 80px;font-weight:700;color:#374151">${p.symbol}</div>
        <div class="fl-td dim" style="flex:0 0 55px">${p.curr_rank}</div>
        <div class="fl-td dim" style="flex:0 0 55px">${p.score}</div>
        <div class="fl-td dim" style="flex:0 0 60px">${p.entry_date}</div>
        <div class="fl-td dim" style="flex:0 0 55px">$${p.entry_price?.toFixed(2)||"—"}</div>
        <div class="fl-td dim" style="flex:0 0 55px">$${p.curr_price?.toFixed(2)||"—"}</div>
        <div class="fl-td" style="flex:0 0 55px;font-weight:700;color:${pnlCol}">${pnl>=0?"+":""}${pnl}%</div>
        <div class="fl-td" style="flex:0 0 60px">${actionBadge}</div>
        <div class="fl-td dim" style="flex:1;font-size:9px">${p.sector}</div>
      `;
      body.appendChild(tr);
    });
  }

})();
