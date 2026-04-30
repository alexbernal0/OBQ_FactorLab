// findings_lab.js — Research Findings Journal
// Stores cycle intelligence: what we learned from each backtest cycle
// Persisted to /api/findings/* (DuckDB bank)

(function () {
  "use strict";

  var _findings = [];
  var _activeFilter = "all";

  // ── Init ───────────────────────────────────────────────────────────────────
  window.findingsInit = function () {
    _loadFindings();
  };

  // ── Load from server ───────────────────────────────────────────────────────
  async function _loadFindings() {
    const r = await fetch("/api/findings").then(r => r.json()).catch(() => null);
    if (!r) return;
    _findings = r.findings || [];
    _updateCount();
    _renderTable();
  }

  function _updateCount() {
    const el = document.getElementById("fnd-count-badge");
    if (el) el.textContent = _findings.length + " findings";
  }

  // ── Filter ─────────────────────────────────────────────────────────────────
  window.findingsFilter = function () {
    _activeFilter = document.getElementById("fnd-filter-tag")?.value || "all";
    _renderTable();
  };

  // ── Show new form ──────────────────────────────────────────────────────────
  window.findingsNew = function () {
    const form = document.getElementById("fnd-new-form");
    if (form) {
      form.style.display = form.style.display === "none" ? "block" : "none";
      if (form.style.display === "block") {
        document.getElementById("fnd-title")?.focus();
      }
    }
  };

  // ── Save finding ───────────────────────────────────────────────────────────
  window.findingsSave = async function () {
    const title   = document.getElementById("fnd-title")?.value?.trim();
    const body    = document.getElementById("fnd-body")?.value?.trim();
    const sid     = document.getElementById("fnd-strategy-id")?.value?.trim();
    const tag     = document.getElementById("fnd-tag")?.value || "factor";
    const actions = document.getElementById("fnd-actions")?.value?.trim();

    if (!title) { alert("Title is required"); return; }

    const finding = {
      title,
      body: body || "",
      strategy_id: sid || null,
      tag,
      actions: actions || "",
      created_at: new Date().toISOString(),
    };

    const r = await fetch("/api/findings", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify(finding),
    }).then(r => r.json()).catch(() => null);

    if (!r || r.error) {
      alert("Save failed: " + (r?.error || "unknown"));
      return;
    }

    // Clear form
    ["fnd-title","fnd-body","fnd-strategy-id","fnd-actions"].forEach(id => {
      const el = document.getElementById(id); if (el) el.value = "";
    });
    document.getElementById("fnd-new-form").style.display = "none";

    const statusEl = document.getElementById("fnd-status");
    if (statusEl) statusEl.textContent = "Finding saved: " + title;

    // Reload
    await _loadFindings();
  };

  // ── Export as text ─────────────────────────────────────────────────────────
  window.findingsExport = function () {
    if (!_findings.length) { alert("No findings to export"); return; }

    const lines = [
      "OBQ FACTOR LAB — RESEARCH FINDINGS JOURNAL",
      "Generated: " + new Date().toLocaleString(),
      "Total Findings: " + _findings.length,
      "=".repeat(70),
    ];

    const filtered = _activeFilter === "all" ? _findings : _findings.filter(f => f.tag === _activeFilter);

    filtered.forEach((f, i) => {
      lines.push("");
      lines.push("[" + (i+1) + "] " + f.title);
      lines.push("Date: " + new Date(f.created_at).toLocaleString());
      lines.push("Tag: " + TAG_LABELS[f.tag] || f.tag);
      if (f.strategy_id) lines.push("Strategy: " + f.strategy_id);
      lines.push("-".repeat(50));
      if (f.body) lines.push(f.body);
      if (f.actions) { lines.push(""); lines.push("Action Items: " + f.actions); }
    });

    const blob = new Blob([lines.join("\n")], {type:"text/plain"});
    const a    = document.createElement("a");
    a.href     = URL.createObjectURL(blob);
    a.download = "OBQ_FactorLab_Findings_" + new Date().toISOString().slice(0,10) + ".txt";
    a.click();
  };

  // ── Tag metadata ────────────────────────────────────────────────────────────
  var TAG_LABELS = {
    factor:    "Factor Research",
    portfolio: "Portfolio Research",
    risk:      "Risk Insight",
    data:      "Data Quality",
    bias:      "Bias / Gotcha",
  };
  var TAG_COLORS = {
    factor:    {bg:"rgba(124,58,237,0.15)",  color:"#a78bfa", border:"rgba(124,58,237,0.4)"},
    portfolio: {bg:"rgba(29,78,216,0.15)",   color:"#60a5fa", border:"rgba(29,78,216,0.4)"},
    risk:      {bg:"rgba(220,38,38,0.15)",   color:"#f87171", border:"rgba(220,38,38,0.4)"},
    data:      {bg:"rgba(245,158,11,0.15)",  color:"#fbbf24", border:"rgba(245,158,11,0.4)"},
    bias:      {bg:"rgba(239,68,68,0.15)",   color:"#fc8181", border:"rgba(239,68,68,0.4)"},
  };

  // ── Render table ───────────────────────────────────────────────────────────
  function _renderTable() {
    const body  = document.getElementById("fnd-table-body");
    const empty = document.getElementById("fnd-empty");
    if (!body) return;

    const filtered = _activeFilter === "all"
      ? _findings
      : _findings.filter(f => f.tag === _activeFilter);

    if (!filtered.length) {
      body.innerHTML = "";
      if (empty) { empty.style.display = "block"; empty.innerHTML = "No findings" + (_activeFilter!=="all"?" for this tag":"") + " yet"; }
      return;
    }
    if (empty) empty.style.display = "none";
    body.innerHTML = "";

    filtered.forEach((f, i) => {
      const tc = TAG_COLORS[f.tag] || TAG_COLORS.factor;
      const dateStr = f.created_at ? new Date(f.created_at).toLocaleString("en-US", {month:"short",day:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}) : "—";
      const rowId   = "fnd-row-" + i;
      const detailId = "fnd-detail-" + i;

      const row = document.createElement("div");
      row.style.cssText = "border-bottom:1px solid var(--border)";

      row.innerHTML = `
        <div class="fl-tr" style="align-items:flex-start;cursor:pointer" onclick="fndToggle('${detailId}','${rowId}')">
          <div class="fl-td dim" style="flex:0 0 30px;padding-top:2px">
            <span id="${rowId}-icon" style="font-size:10px;transition:transform .2s;display:inline-block">&#9654;</span>
          </div>
          <div class="fl-td dim" style="flex:0 0 85px;font-size:10px">${dateStr.split(",")[0]||dateStr}</div>
          <div style="flex:0 0 65px;padding:0 4px">
            <span style="background:${tc.bg};color:${tc.color};border:1px solid ${tc.border};border-radius:10px;padding:2px 7px;font-size:8px;font-weight:700;white-space:nowrap">${TAG_LABELS[f.tag]||f.tag}</span>
          </div>
          <div class="fl-td" style="flex:0 0 150px;font-family:monospace;font-size:9px;color:var(--text-muted)">${f.strategy_id||"—"}</div>
          <div class="fl-td" style="flex:1;font-weight:600;color:var(--text)">${f.title||"Untitled"}</div>
          <div style="flex:0 0 60px;padding:0 4px;display:flex;gap:4px;align-items:center">
            ${f.actions ? '<span style="background:rgba(6,182,212,0.15);color:#06b6d4;border:1px solid rgba(6,182,212,0.3);border-radius:3px;padding:1px 5px;font-size:8px;font-weight:700">ACTIONS</span>' : ''}
            <button onclick="fndDelete(${f.id||i},event)" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:11px;padding:0 2px" title="Delete">&#10005;</button>
          </div>
        </div>
        <div id="${detailId}" style="display:none;padding:12px 16px 14px 48px;background:var(--bg-panel);border-top:1px solid var(--border)">
          ${f.body ? `<div style="font-size:11px;color:var(--text);line-height:1.7;white-space:pre-wrap;margin-bottom:${f.actions?'10px':'0'}">${_escHtml(f.body)}</div>` : ''}
          ${f.actions ? `
            <div style="background:rgba(6,182,212,0.08);border:1px solid rgba(6,182,212,0.25);border-radius:4px;padding:8px 12px;margin-top:${f.body?'8px':'0'}">
              <div style="font-size:8px;font-weight:700;color:#06b6d4;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">&#9658; NEXT ACTIONS</div>
              <div style="font-size:11px;color:var(--text)">${_escHtml(f.actions)}</div>
            </div>` : ''}
          <div style="margin-top:10px;font-size:9px;color:var(--text-muted)">${dateStr} ${f.strategy_id?'| Strategy: '+f.strategy_id:''}</div>
        </div>
      `;
      body.appendChild(row);
    });
  }

  // ── Toggle expand ───────────────────────────────────────────────────────────
  window.fndToggle = function (detailId, rowId) {
    const detail = document.getElementById(detailId);
    const icon   = document.getElementById(rowId + "-icon");
    if (!detail) return;
    const open = detail.style.display !== "none";
    detail.style.display = open ? "none" : "block";
    if (icon) icon.style.transform = open ? "rotate(0deg)" : "rotate(90deg)";
  };

  // ── Delete ──────────────────────────────────────────────────────────────────
  window.fndDelete = async function (id, evt) {
    evt.stopPropagation();
    if (!confirm("Delete this finding?")) return;
    const r = await fetch("/api/findings/" + id, {method:"DELETE"}).then(r=>r.json()).catch(()=>null);
    if (r && !r.error) await _loadFindings();
  };

  function _escHtml(s) {
    return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\n/g,"<br/>");
  }

})();
