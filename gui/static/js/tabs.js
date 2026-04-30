// tabs.js — Main tab switching and app-level utilities
// Loaded LAST after all other scripts so it can override safely

(function () {
  "use strict";

  var TABS = ["factor", "portfolio", "results", "tracker", "findings"];

  function switchMainTab(tab) {
    TABS.forEach(function (t) {
      var view = document.getElementById("view-" + t);
      var btn  = document.getElementById("tab-btn-" + t);
      if (view) view.classList.toggle("active", t === tab);
      if (btn)  btn.classList.toggle("active",  t === tab);
    });

  // Per-tab init callbacks
  if (tab === "factor"    && typeof factorLabInit    === "function") factorLabInit();
  if (tab === "tracker"   && typeof trkInit          === "function") trkInit();
  if (tab === "findings"  && typeof findingsInit     === "function") findingsInit();
  // RESULTS tab hosts the strategy backtest (app.js) — auto-load SPY on first visit
  if (tab === "results") {
    if (typeof _spyLoaded !== "undefined" && !_spyLoaded && typeof startSPY === "function") {
      setTimeout(startSPY, 300);
    }
  }

    // Resize Plotly after layout settles
    setTimeout(function () {
      document.querySelectorAll("#view-" + tab + " [id^=plt_], #view-" + tab + " [id^=flt_]")
        .forEach(function (d) {
          try { if (d.data) Plotly.Plots.resize(d); } catch (e) {}
        });
    }, 250);
  }

  // Expose globally — this is the ONLY authoritative definition
  window.switchMainTab  = switchMainTab;
  window.switchToFactor = function () { switchMainTab("factor"); };

  // Override app.js switchTab (it calls switchTab('topn') etc → route to results)
  window.switchTab = function () { switchMainTab("results"); };

  // ── Resizable dividers ─────────────────────────────────────────
  function makeDivider(divId, leftId, minLeft, minRight, plotSel) {
    var div  = document.getElementById(divId);
    var left = document.getElementById(leftId);
    if (!div || !left) return;
    var drag = false, sx, sw;
    div.addEventListener("mousedown", function (e) {
      drag = true; sx = e.clientX; sw = left.offsetWidth;
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      div.classList.add("dragging");
    });
    document.addEventListener("mousemove", function (e) {
      if (!drag) return;
      var w = Math.max(minLeft, Math.min(sw + e.clientX - sx, window.innerWidth - minRight));
      left.style.width = w + "px";
      left.style.flex  = "none";
    });
    document.addEventListener("mouseup", function () {
      if (!drag) return;
      drag = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      div.classList.remove("dragging");
      if (plotSel) document.querySelectorAll(plotSel).forEach(function (d) {
        try { if (d.data) Plotly.Plots.resize(d); } catch (e) {}
      });
    });
  }

  // ── Theme ──────────────────────────────────────────────────────
  var _theme = "";  // default = light (:root vars, white bg)

  function _applyTheme(t) {
    document.body.setAttribute("data-theme", t);
    document.documentElement.setAttribute("data-theme", t);
  }

  _applyTheme("");  // start light
  localStorage.removeItem("fl-theme");

  window.toggleTheme = function () {
    _theme = _theme === "" ? "dark" : _theme === "dark" ? "night" : "";
    _applyTheme(_theme);
  };

  // ── Snap ───────────────────────────────────────────────────────
  window.snapScreen = function () {
    fetch("/api/snap").then(function (r) { return r.json(); }).then(function (d) {
      if (d.img) {
        var w = window.open("", "_blank", "width=1400,height=900");
        w.document.write(
          '<html><body style="margin:0;background:#0d1b2a"><img src="data:image/png;base64,' +
          d.img + '" style="max-width:100%;display:block"/></body></html>'
        );
      }
    }).catch(function () {});
  };

  // ── Init on DOMContentLoaded ───────────────────────────────────
  document.addEventListener("DOMContentLoaded", function () {
    // Wire up dividers
    makeDivider("divider",     "left-pane",     200, 350, "#view-portfolio [id^=plt_]");
    makeDivider("fl-divider",  "fl-left-pane",  350, 350, "#view-factor [id^=flt_]");
    makeDivider("res-divider", "res-left-pane", 350, 350, "#view-results [id^=plt_],[id^=flt_]");
    makeDivider("trk-divider", "trk-left-pane", 350, 350, "#view-tracker [id^=plt_]");

    // GPU badge
    var badge = document.getElementById("gpu-badge");
    if (badge) badge.textContent = "GPU RTX 3090";

    // End date defaults
    ["fl-end-date", "cfg-end"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el && !el.value) el.valueAsDate = new Date();
    });

    // Start on Factor Models tab
    switchMainTab("factor");
  });

})();
