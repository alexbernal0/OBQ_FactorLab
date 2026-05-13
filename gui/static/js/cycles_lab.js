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

  // ── Sort state for Strategy Log ──────────────────────────────────────────────
  let _bankSortKey = "obq_fund_score";
  let _bankSortDir = -1;  // -1 = descending, +1 = ascending

  window.flSortBank = function (key) {
    if (_bankSortKey === key) {
      _bankSortDir *= -1;  // toggle direction on repeated click
    } else {
      _bankSortKey = key;
      _bankSortDir = (key === "strategy_id") ? 1 : -1;  // IDs sort asc by default
    }
    // Update arrow indicators
    document.querySelectorAll("[id^='fl-sort-arrow-']").forEach(el => { el.textContent = ""; });
    const arrow = document.getElementById("fl-sort-arrow-" + key);
    if (arrow) arrow.textContent = _bankSortDir === -1 ? " ▼" : " ▲";
    // Re-render with new sort
    _renderBankRows(_bankData, _bankSortKey);
  };

  function _renderBankRows(models, sortKey) {
    const body  = document.getElementById("fl-bank-body");
    const empty = document.getElementById("fl-bank-empty");
    const count = document.getElementById("fl-bank-count");
    if (!body) return;

    // Use current sort state (sortKey param may be stale if called from flResortLog)
    const key = _bankSortKey || sortKey || "obq_fund_score";

    // Cycle filter
    const cycleEl     = document.getElementById("fl-log-cycle");
    const cycleFilter = cycleEl ? cycleEl.value : "all";
    let filtered = models;
    if (cycleFilter && cycleFilter !== "all") {
      filtered = models.filter(m => (m.run_label || "").includes(cycleFilter));
    }

    if (count) count.textContent = filtered.length + " / " + models.length + " models";

    if (!filtered.length) {
      body.innerHTML = "<div style='padding:20px;color:var(--text-muted);font-size:11px'>No models match filter</div>";
      if (empty) empty.style.display = "none";
      return;
    }
    if (empty) empty.style.display = "none";

    // Sort — nulls always last
    const dir = _bankSortDir;
    const sorted = [...filtered].sort((a, b) => {
      const av = a[key]; const bv = b[key];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "string") return dir * av.localeCompare(bv);
      return dir * (bv - av);
    });

    // Update arrow on current sort column
    document.querySelectorAll("[id^='fl-sort-arrow-']").forEach(el => { el.textContent = ""; });
    const arrow = document.getElementById("fl-sort-arrow-" + key);
    if (arrow) arrow.textContent = dir === -1 ? " ▼" : " ▲";

    function pct(v,d=1) { if(v==null||isNaN(v)) return "—"; return ((v*100)>=0?"+":"")+(v*100).toFixed(d)+"%"; }
    function num(v,d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }

    body.innerHTML = "";
    const CHUNK = 150;

    function _appendChunk(startIdx) {
      const frag = document.createDocumentFragment();
      const end  = Math.min(startIdx + CHUNK, sorted.length);
      for (let ci = startIdx; ci < end; ci++) {
        const m  = sorted[ci];
        const tr = document.createElement("div");
        tr.className = "fl-tr";

        const fund     = m.obq_fund_score;
        const alphaWin = m.alpha_win_rate;
        const spread   = m.quintile_spread_cagr;
        const stair    = m.staircase_score;
        const mono     = m.monotonicity_score;
        const avgAlpha = m.avg_annual_alpha;
        const icir     = m.icir;
        const hitRate  = m.ic_hit_rate;
        const sid      = _esc(m.strategy_id || "");

        const fundCol    = fund!=null    ? (fund>=0.5?"g":fund>=0.3?"":"r")       : "";
        const winCol     = alphaWin!=null? (alphaWin>=0.6?"g":alphaWin>=0.5?"":"r"): "";
        const spreadCol  = spread!=null  ? (spread>=0.06?"g":spread>=0?"":"r")    : "";
        const stairCol   = stair!=null   ? (stair>=0.02?"g":stair>=0?"":"r")      : "";
        const monoCol    = mono!=null    ? (mono>=0.8?"g":mono>=0.6?"":"r")       : "";
        const alphaCol   = avgAlpha!=null? (avgAlpha>=0.03?"g":avgAlpha>=0?"":"r"): "";
        const icirCol    = icir!=null    ? (icir>=1.0?"g":icir>=0.5?"":"r")       : "";
        const hitCol     = hitRate!=null ? (hitRate>=0.6?"g":hitRate>=0.5?"":"r") : "";

        tr.innerHTML = `
          <div class="fl-td dim" style="flex:0 0 140px;font-family:monospace;font-size:8px" title="${_esc(m.run_label||"")}">${sid}</div>
          <div class="fl-td ${fundCol}"   style="flex:0 0 44px;font-weight:700;font-size:8px">${fund!=null?num(fund,3):"—"}</div>
          <div class="fl-td ${winCol}"    style="flex:0 0 38px;font-size:8px">${alphaWin!=null?(alphaWin*100).toFixed(0)+"%":"—"}</div>
          <div class="fl-td ${spreadCol}" style="flex:0 0 46px;font-size:8px">${pct(spread,1)}</div>
          <div class="fl-td ${stairCol}"  style="flex:0 0 38px;font-size:8px">${stair!=null?pct(stair,1):"—"}</div>
          <div class="fl-td ${monoCol}"   style="flex:0 0 38px;font-size:8px">${mono!=null?(mono*100).toFixed(0)+"%":"—"}</div>
          <div class="fl-td ${alphaCol}"  style="flex:0 0 38px;font-size:8px">${avgAlpha!=null?pct(avgAlpha,1):"—"}</div>
          <div class="fl-td ${icirCol}"   style="flex:0 0 36px;font-size:8px">${num(icir,2)}</div>
          <div class="fl-td ${hitCol}"    style="flex:0 0 36px;font-size:8px">${hitRate!=null?(hitRate*100).toFixed(0)+"%":"—"}</div>
          <button
            title="Send Q1 to Results tab"
            onclick="event.stopPropagation();flPromoteToResults('${sid}')"
            style="flex:0 0 22px;width:22px;height:18px;margin:0 2px;padding:0;background:var(--accent2);color:#fff;border:none;border-radius:3px;cursor:pointer;font-size:12px;font-weight:900;line-height:18px;text-align:center;align-self:center"
          >+</button>
        `;

        tr.onclick = function () {
          document.querySelectorAll("#fl-bank-body .fl-tr").forEach(r => r.classList.remove("active"));
          tr.classList.add("active");
          if (typeof flBankRowClick === "function") flBankRowClick(m);
        };
        frag.appendChild(tr);
      }
      body.appendChild(frag);
      if (end < sorted.length) {
        setTimeout(function () { _appendChunk(end); }, 0);
      }
    }

    _appendChunk(0);
  }

  // Expose render so factor_lab can call it after loading bank
  window.flRenderBankRows = _renderBankRows;

  // ── Seed default Cycle 001 if storage is empty ──────────────────────────────
  // The 6 real research cycles completed to date.
  // Always authoritative — clears stale localStorage and re-seeds on version bump.
  const _REAL_CYCLES = [
    {
      id: "CYC-003", num: 3, name: "R3000 Baseline — 91 Factors",
      created: "2026-04-28",
      scope: "91 factors × 3 cap tiers (all/large/$10B+/mid). Established baseline OBQ scores across JCN composites, CYC-002 factors, and universe scores. Top: JCN Alpha Trifecta OBQ 0.839, JCN QARP OBQ 0.848 (large-cap).",
    },
    {
      id: "CYC-004", num: 4, name: "Pure Factor Baselines — 37 Factors",
      created: "2026-04-30",
      scope: "37 new pure fundamental factors × 3 cap tiers. Key findings: OCF/Assets OBQ 0.777, F-Score OBQ 0.759, EBIT/Assets OBQ 0.739. Established the pure-factor benchmark library.",
    },
    {
      id: "CYC-005", num: 5, name: "Sector Intelligence — 15 Champions × 11 Sectors",
      created: "2026-05-02",
      scope: "15 champion factors × 11 GICS sectors + 11 novel sector-specialist factors. IT + JCN Alpha Trifecta OBQ 0.883 (highest in study). Health Care OCF/Assets OBQ 0.876 with +35% Q1-Q5 spread (widest in study).",
    },
    {
      id: "CYC-006", num: 6, name: "Rebalance Timing Study — 120 Factors × 10 Timings",
      created: "2026-05-07",
      scope: "120 factors × 10 timing variants (quarterly/semi-annual rotations/annual). KEY: Annual Jun 30 (A-Q2) wins for most factors (26/120). Composites/Growth prefer Dec 31; Value/Quality prefer Jun 30; Momentum prefers SA-APR-OCT.",
    },
    {
      id: "CYC-006b", num: 7, name: "Staggered Tranche Rebalancing — 9 Configs",
      created: "2026-05-09",
      scope: "9 tranche configs × 120 factors (CPU post-processing). KEY: 2T-MAR-SEP (50/50) wins most with +0.221 avg OBQ gain. 4T-MAR-JUN-SEP-DEC improves 90/119 factors. jcn_qarp reaches OBQ 0.922 with 2T-JUN-DEC.",
    },
    {
      id: "CYC-007", num: 8, name: "Sector-Optimized Composites — 9 Composites",
      created: "2026-05-12",
      scope: "9 per-sector multi-factor composites (HC, IT, FIN, CD, CS, IND, MAT). Built by combining best within-sector factors from CYC-005. Run on GPU with within-sector mask.",
    },
  ];

  function _seedDefaultCycle() {
    // The 6 real cycles are always authoritative — definitions never come from localStorage.
    // Only strategy_ids links are preserved from whatever was previously stored.
    const prevById = {};
    _cycles.forEach(c => { prevById[c.id] = c; });

    _cycles = _REAL_CYCLES.map(rc => ({
      ...rc,
      strategy_ids: (prevById[rc.id] || {}).strategy_ids || [],
      active: false,
    }));

    _activeCycleId = _cycles[_cycles.length - 1].id;
    _cycles[_cycles.length - 1].active = true;
    _save();
  }

  // ── Init ─────────────────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", function () {
    window.flLoadCycles();
  });

})();
