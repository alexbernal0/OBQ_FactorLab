// wiki_lab.js — OBQ FactorLab Research Encyclopedia
// Searchable, accordion-style wiki backed by wiki_data.json

(function () {
  "use strict";

  let _wikiData   = null;   // loaded once, cached
  let _filtered   = [];     // current filtered list
  let _activeId   = null;   // expanded entry id
  let _activePart = "all";  // active part filter

  // Part color palette
  const PART_COLORS = {
    "Part I":    "#0066cc",
    "Part II":   "#16a34a",
    "Part III":  "#7c3aed",
    "Part IV":   "#f59e0b",
    "Part V":    "#06b6d4",
    "Part VI":   "#ec4899",
    "OBQ":       "#c9a84c",
    "Reference": "#94a3b8",
    "default":   "#6b7280",
  };

  function _partColor(part) {
    for (const key of Object.keys(PART_COLORS)) {
      if (part && part.includes(key)) return PART_COLORS[key];
    }
    return PART_COLORS.default;
  }

  // ── Init ──────────────────────────────────────────────────────────────────
  window.wikiInit = function () {
    const outer = document.getElementById("view-wiki");
    if (!outer) return;

    // Always render into an inner wrapper — never touch the outer container's display
    // (tabs.js controls outer display via the "active" class)
    let container = document.getElementById("wiki-inner");
    if (!container) {
      container = document.createElement("div");
      container.id = "wiki-inner";
      container.style.cssText = "display:flex;flex-direction:column;flex:1;overflow:hidden;min-height:0;width:100%;height:100%";
      outer.appendChild(container);
    }

    if (_wikiData) { _renderWiki(container); return; }

    container.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-muted);font-size:11px">Loading encyclopedia…</div>`;

    fetch("/static/data/wiki_data.json")
      .then(r => r.json())
      .then(data => {
        _wikiData = data;
        _filtered = data;
        _renderWiki(container);
      })
      .catch(e => {
        container.innerHTML = `<div style="padding:20px;color:#dc2626">Failed to load wiki: ${e}</div>`;
      });
  };

  // ── Render full wiki layout ───────────────────────────────────────────────
  function _renderWiki(container) {
    container.innerHTML = "";
    // Don't override display on the inner container — it's already flex from init
    container.style.background = "var(--bg)";

    // ── Header bar ────────────────────────────────────────────────────────
    const hdr = document.createElement("div");
    hdr.style.cssText = "display:flex;align-items:center;gap:12px;padding:10px 16px;background:var(--bg-panel);border-bottom:2px solid var(--border);flex-shrink:0";
    hdr.innerHTML = `
      <div>
        <div style="font-size:13px;font-weight:800;color:var(--accent);letter-spacing:1px">&#128218; FACTOR RESEARCH ENCYCLOPEDIA</div>
        <div style="font-size:9px;color:var(--text-muted);margin-top:2px">
          Synthesized from 10 books (8,201 corpus entries) · Tortoriello · O'Shaughnessy · Berkin/Swedroe · Ilmanen · Chincarini/Kim · Gray/Vogel · Gray/Carlisle · Arnott · Coqueret · OBQ Proprietary
        </div>
      </div>
      <div style="margin-left:auto;display:flex;gap:8px;align-items:center">
        <span id="wiki-count" style="font-size:9px;color:var(--text-muted)">${_wikiData.length} entries</span>
        <input id="wiki-search" placeholder="Search… (factor name, concept, author)" 
               oninput="wikiSearch(this.value)"
               style="width:260px;padding:5px 10px;background:var(--bg);border:1px solid var(--border);color:var(--text);font-size:10px;border-radius:3px;font-family:var(--font);outline:none"/>
      </div>
    `;
    container.appendChild(hdr);

    // ── Main content area ────────────────────────────────────────────────
    const body = document.createElement("div");
    body.style.cssText = "display:flex;flex:1;overflow:hidden;min-height:0";

    // Sidebar
    const sidebar = document.createElement("div");
    sidebar.style.cssText = "width:220px;flex-shrink:0;border-right:1px solid var(--border);overflow-y:auto;background:var(--bg-panel);display:flex;flex-direction:column";
    sidebar.id = "wiki-sidebar";
    _renderSidebar(sidebar);
    body.appendChild(sidebar);

    // Content
    const content = document.createElement("div");
    content.style.cssText = "flex:1;overflow-y:auto;padding:12px 16px";
    content.id = "wiki-content";
    _renderEntries(content, _filtered);
    body.appendChild(content);

    container.appendChild(body);
  }

  // ── Sidebar ───────────────────────────────────────────────────────────────
  function _renderSidebar(sidebar) {
    sidebar.innerHTML = "";

    const allBtn = document.createElement("div");
    allBtn.style.cssText = `padding:8px 12px;cursor:pointer;font-size:10px;font-weight:700;color:${_activePart==="all"?"var(--accent)":"var(--text-dim)"};background:${_activePart==="all"?"var(--bg-panel2)":"transparent"};border-bottom:1px solid var(--border)`;
    allBtn.textContent = `ALL ENTRIES (${_wikiData.length})`;
    allBtn.onclick = () => { _activePart = "all"; wikiFilterPart("all"); };
    sidebar.appendChild(allBtn);

    // Group by part
    const parts = {};
    _wikiData.forEach(e => {
      const key = e.part || "Other";
      if (!parts[key]) parts[key] = [];
      parts[key].push(e);
    });

    Object.entries(parts).forEach(([part, entries]) => {
      const color = _partColor(part);
      const btn = document.createElement("div");
      const isActive = _activePart === part;
      btn.style.cssText = `padding:8px 12px 8px 16px;cursor:pointer;font-size:9px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;border-bottom:1px solid var(--border);border-left:3px solid ${isActive?color:"transparent"};background:${isActive?"var(--bg-panel2)":"transparent"};color:${isActive?color:"var(--text-dim)"};transition:all .1s`;
      btn.innerHTML = `<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${part.split("—")[0].trim()}</div><div style="font-size:8px;font-weight:400;color:var(--text-muted);margin-top:1px">${entries.length} entries</div>`;
      btn.onclick = () => { _activePart = part; wikiFilterPart(part); };
      btn.onmouseover = () => { if (!isActive) btn.style.background = "var(--bg-panel2)"; };
      btn.onmouseout  = () => { if (!isActive) btn.style.background = "transparent"; };
      sidebar.appendChild(btn);
    });
  }

  // ── Entry list ───────────────────────────────────────────────────────────
  function _renderEntries(content, entries) {
    content.innerHTML = "";
    const count = document.getElementById("wiki-count");
    if (count) count.textContent = `${entries.length} entries`;

    if (!entries.length) {
      content.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted);font-size:11px">No entries match your search.</div>`;
      return;
    }

    entries.forEach(entry => {
      const card = _buildCard(entry);
      content.appendChild(card);
    });
  }

  // ── Single accordion card ────────────────────────────────────────────────
  function _buildCard(entry) {
    const color   = _partColor(entry.part);
    const isOpen  = _activeId === entry.id;

    const card = document.createElement("div");
    card.style.cssText = `margin-bottom:8px;border:1px solid var(--border);border-radius:5px;overflow:hidden;border-left:4px solid ${color}`;
    card.id = "wiki-card-" + entry.id;

    // Card header (always visible)
    const head = document.createElement("div");
    head.style.cssText = `display:flex;align-items:flex-start;gap:10px;padding:10px 14px;cursor:pointer;background:${isOpen?"var(--bg-panel2)":"var(--bg-panel)"};transition:background .1s`;
    head.onclick = () => _toggleCard(entry.id);
    head.onmouseover = () => { if (!isOpen) head.style.background = "var(--bg-panel2)"; };
    head.onmouseout  = () => { if (!isOpen) head.style.background = "var(--bg-panel)"; };

    head.innerHTML = `
      <div style="flex:1;min-width:0">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px">
          <span style="font-size:11px;font-weight:700;color:var(--text)">${entry.title}</span>
          <span style="font-size:8px;padding:1px 7px;border-radius:10px;background:${color}22;color:${color};border:1px solid ${color}44;font-weight:700;white-space:nowrap">${entry.section}</span>
        </div>
        <div style="font-size:9.5px;color:var(--text-dim);line-height:1.5">${entry.summary}</div>
        <div style="display:flex;gap:5px;flex-wrap:wrap;margin-top:5px">
          ${(entry.tags||[]).slice(0,5).map(t=>`<span style="font-size:7.5px;padding:1px 5px;background:var(--bg-panel2);border:1px solid var(--border);border-radius:3px;color:var(--text-muted)">${t}</span>`).join("")}
          ${entry.source ? `<span style="font-size:7.5px;padding:1px 5px;background:rgba(201,168,76,0.1);border:1px solid rgba(201,168,76,0.3);border-radius:3px;color:#c9a84c">${entry.source}</span>` : ""}
        </div>
      </div>
      <span style="font-size:14px;color:var(--text-muted);flex-shrink:0;padding-top:2px">${isOpen ? "▾" : "▸"}</span>
    `;
    card.appendChild(head);

    // Card body (collapsible)
    const body = document.createElement("div");
    body.style.cssText = `display:${isOpen?"block":"none"};padding:14px 16px;background:var(--bg);border-top:1px solid var(--border)`;
    body.innerHTML = _renderMarkdown(entry.content || "");
    card.appendChild(body);

    return card;
  }

  function _toggleCard(id) {
    _activeId = _activeId === id ? null : id;
    const content = document.getElementById("wiki-content");
    if (content) _renderEntries(content, _filtered);
    // Scroll to opened card
    if (_activeId) {
      setTimeout(() => {
        const c = document.getElementById("wiki-card-" + _activeId);
        if (c) c.scrollIntoView({ behavior:"smooth", block:"nearest" });
      }, 50);
    }
  }

  // ── Markdown renderer ────────────────────────────────────────────────────
  function _renderMarkdown(md) {
    if (!md) return "";
    let html = md;

    // Code blocks ```...```
    html = html.replace(/```[\s\S]*?```/g, m => {
      const code = m.slice(3, -3).replace(/^[a-z]*\n/, "");
      return `<pre style="background:var(--bg-panel2);border:1px solid var(--border);border-radius:4px;padding:8px 12px;font-family:'Courier New',monospace;font-size:9px;overflow-x:auto;margin:8px 0;white-space:pre-wrap">${_esc(code)}</pre>`;
    });

    // Tables
    html = html.replace(/(\|.+\|\n)+/g, m => {
      const rows = m.trim().split("\n").filter(r => r.trim() && !r.match(/^\|[\s-|]+\|$/));
      if (!rows.length) return m;
      let tbl = `<div style="overflow-x:auto;margin:8px 0"><table style="width:100%;border-collapse:collapse;font-size:9px">`;
      rows.forEach((row, ri) => {
        const cells = row.split("|").slice(1,-1).map(c => c.trim());
        const tag   = ri === 0 ? "th" : "td";
        const bg    = ri === 0 ? "background:var(--bg-panel2);font-weight:700;letter-spacing:.5px;text-transform:uppercase" : (ri%2===0?"background:var(--bg)":"background:var(--bg-panel)");
        tbl += `<tr>${cells.map(c=>`<${tag} style="padding:4px 8px;border:1px solid var(--border);color:var(--text);${bg}">${c}</${tag}>`).join("")}</tr>`;
      });
      tbl += "</table></div>";
      return tbl;
    });

    // Headers ## → h3, ### → h4
    html = html.replace(/^### (.+)$/gm, '<h4 style="font-size:10px;font-weight:700;color:var(--accent2);margin:12px 0 4px;letter-spacing:.5px;text-transform:uppercase">$1</h4>');
    html = html.replace(/^## (.+)$/gm,  '<h3 style="font-size:12px;font-weight:800;color:var(--accent);margin:14px 0 6px;border-bottom:1px solid var(--border);padding-bottom:4px">$1</h3>');
    html = html.replace(/^# (.+)$/gm,   '<h2 style="font-size:14px;font-weight:800;color:var(--accent);margin:16px 0 8px">$1</h2>');

    // Bold **text**
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong style="color:var(--text);font-weight:700">$1</strong>');

    // Inline code `code`
    html = html.replace(/`([^`]+)`/g, '<code style="background:var(--bg-panel2);border:1px solid var(--border);border-radius:2px;padding:0 4px;font-family:monospace;font-size:8.5px;color:var(--accent2)">$1</code>');

    // Blockquote > text
    html = html.replace(/^> (.+)$/gm, '<blockquote style="border-left:3px solid var(--accent);margin:8px 0;padding:6px 12px;background:var(--bg-panel);color:var(--text-dim);font-style:italic;font-size:10px">$1</blockquote>');

    // Lists - item
    html = html.replace(/^- (.+)$/gm, '<div style="display:flex;gap:6px;margin:2px 0;font-size:10px"><span style="color:var(--accent);flex-shrink:0">▸</span><span style="color:var(--text)">$1</span></div>');
    html = html.replace(/^(\d+)\. (.+)$/gm, '<div style="display:flex;gap:6px;margin:2px 0;font-size:10px"><span style="color:var(--accent2);min-width:16px;font-weight:700">$1.</span><span style="color:var(--text)">$2</span></div>');

    // Paragraphs — double newline → paragraph break
    html = html.replace(/\n\n+/g, '</p><p style="font-size:10px;color:var(--text-dim);line-height:1.6;margin:6px 0">');
    html = '<p style="font-size:10px;color:var(--text-dim);line-height:1.6;margin:6px 0">' + html + '</p>';

    // Single newlines within paragraphs
    html = html.replace(/([^>])\n([^<])/g, '$1<br>$2');

    return html;
  }

  function _esc(s) {
    return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  }

  // ── Public API ────────────────────────────────────────────────────────────
  window.wikiSearch = function (query) {
    if (!_wikiData) return;
    const q = query.toLowerCase().trim();
    _filtered = q
      ? _wikiData.filter(e =>
          (e.title||"").toLowerCase().includes(q) ||
          (e.summary||"").toLowerCase().includes(q) ||
          (e.content||"").toLowerCase().includes(q) ||
          (e.tags||[]).some(t => t.toLowerCase().includes(q)) ||
          (e.source||"").toLowerCase().includes(q) ||
          (e.section||"").toLowerCase().includes(q)
        )
      : _activePart === "all"
          ? _wikiData
          : _wikiData.filter(e => e.part === _activePart);

    const content = document.getElementById("wiki-content");
    if (content) _renderEntries(content, _filtered);
  };

  window.wikiFilterPart = function (part) {
    _activePart = part;
    const q = document.getElementById("wiki-search")?.value || "";
    _filtered = _wikiData.filter(e =>
      (part === "all" || e.part === part) &&
      (!q || (e.title||"").toLowerCase().includes(q.toLowerCase()) ||
             (e.summary||"").toLowerCase().includes(q.toLowerCase()) ||
             (e.tags||[]).some(t => t.toLowerCase().includes(q.toLowerCase())))
    );
    const sidebar = document.getElementById("wiki-sidebar");
    if (sidebar) _renderSidebar(sidebar);
    const content = document.getElementById("wiki-content");
    if (content) _renderEntries(content, _filtered);
  };

})();
