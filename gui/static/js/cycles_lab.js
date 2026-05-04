// cycles_lab.js — Research Cycle panel management
// Cycles are persisted in localStorage. No backend needed.
// Integrates with factor_lab.js bank for auto-linking strategy IDs.

(function () {
  "use strict";

  const STORE_KEY = "fl_research_cycles";
  let _cycles = [];
  let _activeCycleId = null;
  let _bankData = [];  // cache of bank rows for resort

  // ── Storage helpers ─────────────────────────────────────────────────────────
  function _save() {
    try { localStorage.setItem(STORE_KEY, JSON.stringify(_cycles)); } catch(e) {}
  }
  function _load() {
    try {
      const raw = localStorage.getItem(STORE_KEY);
      _cycles = raw ? JSON.parse(raw) : [];
    } catch(e) { _cycles = []; }
  }

  function _genId() {
    const n = _cycles.length + 1;
    return "CYC-" + String(n).padStart(3, "0");
  }

  // ── Render cycles list ───────────────────────────────────────────────────────
  function _renderCycles() {
    const body  = document.getElementById("fl-cycles-body");
    const empty = document.getElementById("fl-cycles-empty");
    const count = document.getElementById("fl-cycles-count");
    if (!body) return;

    if (count) count.textContent = _cycles.length + " cycle" + (_cycles.length !== 1 ? "s" : "");

    if (!_cycles.length) {
      body.innerHTML = "";
      if (empty) { empty.style.display = "block"; }
      return;
    }
    if (empty) empty.style.display = "none";

    body.innerHTML = "";
    // Show newest first
    [..._cycles].reverse().forEach(cyc => {
      const isActive = cyc.id === _activeCycleId;
      const row = document.createElement("div");
      row.className = "fl-cycle-row" + (isActive ? " active" : "");
      row.dataset.id = cyc.id;

      const nStrats = (cyc.strategy_ids || []).length;
      row.innerHTML = `
        <div class="fl-cycle-header" onclick="flSelectCycle('${cyc.id}')">
          <span class="fl-cycle-num">${cyc.id}</span>
          <span class="fl-cycle-name" title="${_esc(cyc.name)}">${_esc(cyc.name)}</span>
          <span class="fl-cycle-date">${cyc.created}</span>
          <span class="fl-cycle-count">${nStrats} strat${nStrats !== 1 ? "s" : ""}</span>
          <button class="fl-cycle-expand-btn" id="fl-cbtn-${cyc.id}" onclick="event.stopPropagation();flToggleCycleDetail('${cyc.id}')">&#9654;</button>
        </div>
        <div class="fl-cycle-detail" id="fl-cycle-detail-${cyc.id}" style="display:none">${_esc(cyc.scope || "(no scope defined)")}</div>
      `;
      body.appendChild(row);
    });
  }

  function _esc(s) {
    return String(s || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  }

  // ── Public API ───────────────────────────────────────────────────────────────
  window.flLoadCycles = function () {
    _load();
    _seedDefaultCycle();   // create CYC-001 if nothing stored yet
    // Set active cycle if none selected
    if (!_activeCycleId && _cycles.length > 0) {
      _activeCycleId = _cycles[_cycles.length - 1].id;
    }
    _renderCycles();
    // Auto-expand the active cycle's detail on first load
    if (_activeCycleId) {
      setTimeout(function () {
        var detail = document.getElementById("fl-cycle-detail-" + _activeCycleId);
        var btn    = document.getElementById("fl-cbtn-" + _activeCycleId);
        if (detail && detail.style.display === "none") {
          detail.style.display = "block";
          if (btn) btn.classList.add("open");
        }
      }, 150);
    }
  };

  window.flNewCycle = function () {
    const form = document.getElementById("fl-cycle-new-form");
    if (!form) return;
    form.style.display = form.style.display === "none" ? "block" : "none";
    if (form.style.display === "block") {
      const inp = document.getElementById("fl-cycle-name-input");
      if (inp) inp.focus();
    }
  };

  window.flSaveCycle = function () {
    const nameEl  = document.getElementById("fl-cycle-name-input");
    const scopeEl = document.getElementById("fl-cycle-scope-input");
    const name  = (nameEl && nameEl.value.trim()) || "Untitled Cycle";
    const scope = scopeEl ? scopeEl.value.trim() : "";

    const cyc = {
      id:           _genId(),
      num:          _cycles.length + 1,
      name:         name,
      created:      new Date().toISOString().slice(0, 10),
      scope:        scope,
      strategy_ids: [],
      active:       true,
    };
    // Deactivate all others
    _cycles.forEach(c => { c.active = false; });
    _cycles.push(cyc);
    _save();

    _activeCycleId = cyc.id;

    // Reset form
    if (nameEl)  nameEl.value  = "";
    if (scopeEl) scopeEl.value = "";
    const form = document.getElementById("fl-cycle-new-form");
    if (form) form.style.display = "none";

    _renderCycles();
  };

  window.flSelectCycle = function (id) {
    _activeCycleId = id;
    _cycles.forEach(c => { c.active = (c.id === id); });
    _save();
    _renderCycles();
  };

  window.flToggleCycleDetail = function (id) {
    const detail = document.getElementById("fl-cycle-detail-" + id);
    const btn    = document.getElementById("fl-cbtn-" + id);
    if (!detail) return;
    const open = detail.style.display !== "none";
    detail.style.display = open ? "none" : "block";
    if (btn) btn.classList.toggle("open", !open);
  };

  window.flToggleCyclesPanel = function () {
    const body  = document.getElementById("fl-cycles-body");
    const form  = document.getElementById("fl-cycle-new-form");
    const chev  = document.getElementById("fl-cycles-chevron");
    if (!body) return;
    const hidden = body.style.display === "none";
    body.style.display = hidden ? "" : "none";
    if (form) form.style.display = "none";
    if (chev) chev.textContent = hidden ? "▾" : "▸";
  };

  window.flToggleRunsPanel = function () {
    const col  = document.getElementById("fl-runs-collapse");
    const chev = document.getElementById("fl-runs-chevron");
    if (!col) return;
    const hidden = col.style.display === "none";
    col.style.display = hidden ? "" : "none";
    if (chev) chev.textContent = hidden ? "▾" : "▸";
  };

  // ── Auto-link new strategies to active cycle ─────────────────────────────────
  // Called by factor_lab.js flLoadBank() after bank loads — pass the list of model objects
  window.flLinkStratsToCycle = function (models) {
    if (!_activeCycleId || !models || !models.length) return;
    const cyc = _cycles.find(c => c.id === _activeCycleId);
    if (!cyc) return;
    let changed = false;
    models.forEach(m => {
      const sid = m.strategy_id || m.id;
      if (sid && !cyc.strategy_ids.includes(sid)) {
        cyc.strategy_ids.push(sid);
        changed = true;
      }
    });
    if (changed) {
      _save();
      _renderCycles();
    }
  };

  // ── Strategy Log sort ────────────────────────────────────────────────────────
  // Caches bank data; called when sort dropdown changes
  window.flSetBankData = function (data) {
    _bankData = data || [];
  };

  window.flResortLog = function () {
    const sel = document.getElementById("fl-log-sort");
    const key = sel ? sel.value : "quintile_spread_cagr";
    if (!_bankData.length) {
      window.flLoadBank && window.flLoadBank();
      return;
    }
    _renderBankRows(_bankData, key);
  };

  function _renderBankRows(models, sortKey) {
    const body  = document.getElementById("fl-bank-body");
    const empty = document.getElementById("fl-bank-empty");
    const count = document.getElementById("fl-bank-count");
    if (!body) return;

    if (count) count.textContent = models.length + " saved";

    if (!models.length) {
      body.innerHTML = "";
      if (empty) { empty.style.display = "block"; empty.textContent = "No saved models yet"; }
      return;
    }
    if (empty) empty.style.display = "none";

    // Sort
    const sorted = [...models].sort((a, b) => {
      if (sortKey === "date") {
        return (b.saved_at || b.strategy_id || "").localeCompare(a.saved_at || a.strategy_id || "");
      }
      const av = a[sortKey]; const bv = b[sortKey];
      if (av == null) return 1; if (bv == null) return -1;
      return bv - av;  // descending
    });

    function pct(v, d=1) { if (v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
    function num(v, d=2) { if (v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

    body.innerHTML = "";
    sorted.forEach(m => {
      const tr = document.createElement("div");
      tr.className = "fl-tr";

      function pct2(v,d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
      function num2(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }
      function col(v, threshGood, threshNeutral) {
        if(v==null||isNaN(v)) return "";
        return v >= threshGood ? "g" : v >= threshNeutral ? "" : "r";
      }

      const spread   = m.quintile_spread_cagr;
      const fund     = m.obq_fund_score;
      const alphaWin = m.alpha_win_rate;
      const stair    = m.staircase_score;
      const bear     = m.bear_score;
      const bull     = m.bull_score;
      const calm     = m.q1_calmar;

      // Color coding
      const fundCol  = fund!=null ? (fund>=0.5?"g":fund>=0.3?"":"r") : "";
      const spreadCol= spread!=null?(spread>=0.06?"g":spread>=0?"":"r"):"";
      const bearCol  = bear!=null?(bear>=0.02?"g":bear>=0?"":"r"):"";
      const bullCol  = bull!=null?(bull>=0.02?"g":bull>=0?"":"r"):"";
      const calmCol  = calm!=null?(calm>=0.5?"g":calm>=0.2?"":"r"):"";

      const sid = _esc(m.strategy_id || "");
      tr.innerHTML = `
        <div class="fl-td dim" style="flex:0 0 140px;font-family:monospace;font-size:8px" title="${_esc(m.strategy_id||"")}">${_esc(m.strategy_id||"")}</div>
        <div class="fl-td ${fundCol}"  style="flex:0 0 44px;font-weight:700;font-size:8px">${fund!=null?num2(fund,3):"—"}</div>
        <div class="fl-td ${alphaWin!=null&&alphaWin>=0.6?"g":alphaWin!=null&&alphaWin>=0.5?"":"r"}" style="flex:0 0 38px;font-size:8px">${alphaWin!=null?(alphaWin*100).toFixed(0)+"%":"—"}</div>
        <div class="fl-td ${spreadCol}" style="flex:0 0 46px;font-size:8px">${pct2(spread,1)}</div>
        <div class="fl-td ${stair!=null&&stair>=0.02?"g":stair!=null&&stair>=0?"":"r"}" style="flex:0 0 38px;font-size:8px">${stair!=null?pct2(stair,1):"—"}</div>
        <div class="fl-td ${bearCol}"  style="flex:0 0 36px;font-size:8px">${bear!=null?pct2(bear,1):"—"}</div>
        <div class="fl-td ${bullCol}"  style="flex:0 0 36px;font-size:8px">${bull!=null?pct2(bull,1):"—"}</div>
        <div class="fl-td"             style="flex:0 0 36px;font-size:8px">${num2(m.icir,2)}</div>
        <div class="fl-td"             style="flex:0 0 36px;font-size:8px">${m.ic_hit_rate!=null?((m.ic_hit_rate)*100).toFixed(0)+"%":"—"}</div>
        <div class="fl-td ${calmCol}" style="flex:0 0 38px;font-size:8px">${calm!=null?num2(calm,2):"—"}</div>
        <button
          title="Send Q1 to Results tab for validation"
          onclick="event.stopPropagation();flPromoteToResults('${sid}')"
          style="flex:0 0 22px;width:22px;height:18px;margin:0 2px;padding:0;background:var(--accent2);color:#fff;border:none;border-radius:3px;cursor:pointer;font-size:12px;font-weight:900;line-height:18px;text-align:center;align-self:center"
        >+</button>
      `;

      tr.onclick = async () => {
        document.querySelectorAll("#fl-bank-body .fl-tr").forEach(r => r.classList.remove("active"));
        tr.classList.add("active");
        // Delegate to factor_lab's bank click handler via a synthetic bank load
        if (typeof flBankRowClick === "function") {
          flBankRowClick(m);
        }
      };
      body.appendChild(tr);
    });
  }

  // Expose render so factor_lab can call it after loading bank
  window.flRenderBankRows = _renderBankRows;

  // ── Seed default Cycle 001 if storage is empty ──────────────────────────────
  function _seedDefaultCycle() {
    if (_cycles.length > 0) return;  // already have cycles, don't overwrite
    const cyc = {
      id:           "CYC-001",
      num:          1,
      name:         "Pre-Calibration & Dry Testing",
      created:      new Date().toISOString().slice(0, 10),
      scope:        "Pre-calibration and dry testing all templates right now.\n\nObjective: Validate the full factor backtest pipeline — confirm data integrity, tearsheet accuracy, bank storage, and fitness scoring are all working correctly before starting live optimization cycles.\n\nSuccess criteria: All tearsheet data points populated, bank saves/loads correctly, all quintile charts render with proper dates.",
      strategy_ids: [],
      active:       true,
    };
    _cycles.push(cyc);
    _save();
    _activeCycleId = cyc.id;
  }

  // ── Init ─────────────────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", function () {
    window.flLoadCycles();
  });

})();
