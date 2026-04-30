// factor_tearsheet.js — Plotly charts for the Factor Models tearsheet
// Completely separate from tearsheet.js — handles quintile-specific visualizations

const FT_COLORS = {
  q: ["#1d4ed8","#16a34a","#ca8a04","#ea580c","#dc2626"],  // Q1..Q5
  bg: "#ffffff", plot_bg: "#fafafa",
  grid: "#eeeeee", tick: "#374151", legend: "#374151",
  ic_pos: "rgba(22,163,74,0.7)", ic_neg: "rgba(220,38,38,0.7)",
};
const FT_CFG = { displayModeBar: false, responsive: true };

function _ftLayout(extra) {
  return Object.assign({
    paper_bgcolor: FT_COLORS.bg, plot_bgcolor: FT_COLORS.plot_bg,
    font: { family:"Segoe UI,Arial,sans-serif", size:10, color:FT_COLORS.tick },
    margin: { l:52, r:12, t:10, b:36 },
    legend: { orientation:"h", x:0, y:1.12, font:{size:9}, bgcolor:"transparent" },
    xaxis: { type:"date", gridcolor:FT_COLORS.grid, linecolor:"#cccccc",
             tickfont:{size:8,color:FT_COLORS.tick}, showgrid:true },
    yaxis: { gridcolor:FT_COLORS.grid, linecolor:"#cccccc",
             tickfont:{size:8,color:FT_COLORS.tick}, autorange:true, zeroline:true,
             zerolinecolor:"#374151", zerolinewidth:1 },
    hovermode: "x unified", autosize: true,
  }, extra || {});
}

// ── 1. Quintile Bar Chart (CAGR per bucket, excess vs benchmark) ─────────────
function ftDrawQuintileBar(divId, buckets, bucketMetrics, benchmarkCagr) {
  const labels = buckets.map(b => "Q" + b);
  const cagrs  = buckets.map(b => {
    const m = bucketMetrics[String(b)] || {};
    return +((m.cagr || 0) * 100).toFixed(2);
  });
  const excess = cagrs.map(c => +(c - (benchmarkCagr || 0) * 100).toFixed(2));

  Plotly.newPlot(divId, [
    { x: labels, y: cagrs,  type:"bar", name:"CAGR %",
      marker:{ color: cagrs.map((_,i) => FT_COLORS.q[i] || "#888") },
      text: cagrs.map(v=>(v>=0?"+":"")+v+"%"), textposition:"outside" },
    { x: labels, y: excess, type:"bar", name:"Excess vs BM",
      marker:{ color: excess.map(v=>v>=0?"rgba(22,163,74,0.4)":"rgba(220,38,38,0.4)") },
      text: excess.map(v=>(v>=0?"+":"")+v+"%"), textposition:"inside",
      textfont:{size:8}, visible:"legendonly" },
  ], _ftLayout({
    barmode:"overlay",
    yaxis:{ ...{ gridcolor:FT_COLORS.grid, linecolor:"#cccccc", tickfont:{size:8}, autorange:true,
                 zeroline:true, zerolinecolor:"#374151", zerolinewidth:1 }, ticksuffix:"%" },
    xaxis:{ type:"-", gridcolor:FT_COLORS.grid, tickfont:{size:9} },
    margin:{ l:48, r:8, t:8, b:36 },
    showlegend:true,
  }), FT_CFG);
}

// ── 2. Cumulative Equity by Quintile (5 lines, Q1 should be highest) ─────────
function ftDrawCumulativeEquity(divId, dates, buckets, bucketEquity) {
  // dates are rebalance dates — equity arrays have one extra element (starting 1.0)
  const traces = buckets.map((b, i) => {
    const eq = bucketEquity[String(b)] || [1.0];
    // Equity has n+1 points: [1.0, after_period_1, after_period_2, ...]
    // X dates have n points — prepend start date
    const startDate = dates.length > 0
      ? new Date(new Date(dates[0]).getTime() - 180*24*60*60*1000).toISOString().slice(0,10)
      : "2000-01-01";
    const xDates = [startDate, ...dates];
    return {
      x: xDates.slice(0, eq.length),
      y: eq.map(v => +v.toFixed(4)),
      type:"scatter", mode:"lines",
      name:"Q"+b,
      line:{ color: FT_COLORS.q[i] || "#888", width: b===1||b===buckets[buckets.length-1] ? 2.5 : 1.5,
             dash: b===buckets[buckets.length-1] ? "dash" : "solid" },
    };
  });
  Plotly.newPlot(divId, traces, _ftLayout({
    margin:{ l:52, r:8, t:8, b:36 },
    yaxis:{ gridcolor:FT_COLORS.grid, linecolor:"#cccccc", tickfont:{size:8},
            autorange:true, tickformat:".2f" },
  }), FT_CFG);
}

// ── 3. IC (Information Coefficient) bar chart over time ──────────────────────
function ftDrawIC(divId, icData) {
  if (!icData || !icData.length) return;
  const x = icData.map(d => d.date);
  const y = icData.map(d => +(d.ic_value * 100).toFixed(2));
  const mean_ic = y.reduce((a,b)=>a+b,0)/y.length;

  Plotly.newPlot(divId, [
    { x, y, type:"bar", name:"IC (%)",
      marker:{ color: y.map(v=>v>=0 ? FT_COLORS.ic_pos : FT_COLORS.ic_neg) } },
    { x:[x[0],x[x.length-1]], y:[mean_ic,mean_ic], type:"scatter", mode:"lines",
      name:"Mean IC", line:{color:"#374151",width:1.5,dash:"dot"}, showlegend:true },
    { x:[x[0],x[x.length-1]], y:[0,0], type:"scatter", mode:"lines",
      line:{color:"#9ca3af",width:0.8}, showlegend:false },
  ], _ftLayout({
    margin:{ l:44, r:8, t:8, b:36 },
    yaxis:{ gridcolor:FT_COLORS.grid, linecolor:"#cccccc", tickfont:{size:8},
            ticksuffix:"%", autorange:true, title:{text:"IC%",font:{size:8}} },
  }), FT_CFG);
}

// ── 4. Period Returns Heatmap (periods × buckets) ────────────────────────────
function ftDrawPeriodHeatmap(divId, periodData, n_buckets) {
  if (!periodData || !periodData.length) return;
  const dates  = periodData.map(d => d.date);
  const zData  = [];
  for (let b = 1; b <= n_buckets; b++) {
    zData.push(periodData.map(d => {
      const v = d["q"+b+"_ret"];
      return v != null ? +(v*100).toFixed(2) : null;
    }));
  }
  const yLabels = Array.from({length:n_buckets}, (_,i)=>"Q"+(i+1));

  Plotly.newPlot(divId, [{
    z: zData, x: dates, y: yLabels,
    type:"heatmap",
    colorscale:[[0,"#b91c1c"],[0.5,"#fefce8"],[1,"#15803d"]],
    zmid:0, zmin:-20, zmax:20,
    text: zData.map(row=>row.map(v=>v==null?"":v.toFixed(1)+"%")),
    texttemplate:"%{text}", textfont:{size:7},
    showscale:true,
    colorbar:{ title:{text:"%",side:"right"}, tickfont:{size:7}, len:0.8 },
  }], {
    paper_bgcolor:FT_COLORS.bg, plot_bgcolor:FT_COLORS.plot_bg,
    margin:{l:36,r:60,t:8,b:40},
    font:{family:"Segoe UI,Arial",size:8,color:FT_COLORS.tick},
    xaxis:{type:"date",tickfont:{size:7},gridcolor:FT_COLORS.grid},
    yaxis:{tickfont:{size:8}},
  }, FT_CFG);
}

// ── 5. Annual Returns by Quintile (grouped bar chart by year) ─────────────────
function ftDrawAnnualBars(divId, annualRetByBucket, n_buckets) {
  if (!annualRetByBucket) return;
  const years = [...new Set(
    Object.values(annualRetByBucket).flat().map(d=>d.year)
  )].sort();

  const traces = Array.from({length:n_buckets}, (_,i) => {
    const b = i + 1;
    const data = annualRetByBucket[String(b)] || [];
    const byYear = Object.fromEntries(data.map(d=>[d.year, d.ret]));
    return {
      x: years.map(String),
      y: years.map(yr => +((byYear[yr]||0)*100).toFixed(2)),
      type:"bar", name:"Q"+b,
      marker:{ color: FT_COLORS.q[i] || "#888", opacity:0.85 },
    };
  });

  Plotly.newPlot(divId, traces, _ftLayout({
    barmode:"group",
    xaxis:{type:"-", tickfont:{size:8}, gridcolor:FT_COLORS.grid},
    yaxis:{gridcolor:FT_COLORS.grid, tickfont:{size:8}, ticksuffix:"%", autorange:true,
           zeroline:true, zerolinecolor:"#374151"},
    margin:{l:48,r:8,t:8,b:36},
  }), FT_CFG);
}

// ── 6. Quintile Spread over time (Q1 - Q5 cumulative) ───────────────────────
function ftDrawSpread(divId, dates, bucketEquity, n_buckets) {
  const q1 = bucketEquity["1"] || [];
  const qn = bucketEquity[String(n_buckets)] || [];
  const len = Math.min(q1.length, qn.length);
  const spread = Array.from({length:len}, (_,i) => +(q1[i] - qn[i]).toFixed(4));
  const startDate = dates.length > 0
    ? new Date(new Date(dates[0]).getTime() - 180*24*60*60*1000).toISOString().slice(0,10)
    : "2000-01-01";
  const x = [startDate, ...dates].slice(0, len);

  Plotly.newPlot(divId, [{
    x, y: spread, type:"scatter", mode:"lines",
    name:"Q1 - Q5 Spread",
    line:{ color:"#7c3aed", width:2 },
    fill:"tozeroy", fillcolor:"rgba(124,58,237,0.10)",
  },{
    x:[x[0],x[x.length-1]], y:[0,0], type:"scatter", mode:"lines",
    line:{color:"#9ca3af",width:1}, showlegend:false,
  }], _ftLayout({
    margin:{l:52,r:8,t:8,b:36},
    yaxis:{gridcolor:FT_COLORS.grid, tickfont:{size:8}, autorange:true,
           title:{text:"Q1-Q5 Growth",font:{size:8}}},
  }), FT_CFG);
}

// ── 7. Sector Attribution bar chart ─────────────────────────────────────────
function ftDrawSectorAttribution(divId, sectorAttribution, n_buckets) {
  if (!sectorAttribution || !sectorAttribution.length) return;
  const sectors = sectorAttribution.map(s=>s.sector);
  const traces = Array.from({length:n_buckets}, (_,i) => {
    const b = i+1;
    return {
      x: sectorAttribution.map(s=>+((s["q"+b]||0)*100).toFixed(2)),
      y: sectors,
      type:"bar", orientation:"h",
      name:"Q"+b,
      marker:{ color: FT_COLORS.q[i]||"#888", opacity:0.8 },
    };
  });

  Plotly.newPlot(divId, traces, _ftLayout({
    barmode:"group",
    xaxis:{ type:"-", gridcolor:FT_COLORS.grid, tickfont:{size:8}, ticksuffix:"%" },
    yaxis:{ gridcolor:FT_COLORS.grid, tickfont:{size:8}, autorange:true },
    margin:{ l:130, r:8, t:8, b:36 },
  }), FT_CFG);
}

// ── Build Factor Tearsheet metrics table ────────────────────────────────────
function ftBuildMetricsTable(factorMetrics, buckets, bucketMetrics) {
  const fm = factorMetrics || {};
  const wrap = document.createElement("div");
  wrap.style.cssText = "display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#e5e7eb;margin:0 12px 12px";

  function msec(title) {
    const d = document.createElement("div");
    d.style.cssText = "grid-column:1/-1;font-size:8px;font-weight:700;color:#0066cc;letter-spacing:1px;text-transform:uppercase;padding:5px 10px 2px;background:#eff6ff;border-top:1px solid #dbeafe";
    d.textContent = title; wrap.appendChild(d);
  }
  function mrow(label, val, cls) {
    const r = document.createElement("div");
    r.style.cssText = "display:flex;justify-content:space-between;padding:3px 10px;background:#fff;border-bottom:1px solid #f3f4f6";
    const valColor = cls==="g"?"#15803d":cls==="r"?"#b91c1c":"#111";
    r.innerHTML = `<span style="font-size:10px;color:#6b7280">${label}</span><span style="font-size:10px;font-weight:600;color:${valColor}">${val}</span>`;
    wrap.appendChild(r);
  }
  function pct(v) { if(v==null||isNaN(v))return"—"; return ((v*100)>=0?"+":"")+((v*100).toFixed(2))+"%" }
  function num(v,d=2) { if(v==null||isNaN(v))return"—"; return Number(v).toFixed(d) }

  msec("FACTOR SIGNAL QUALITY");
  mrow("IC Mean",          num(fm.ic_mean,4),       fm.ic_mean>0?"g":"r");
  mrow("IC Std Dev",       num(fm.ic_std,4));
  mrow("ICIR",             num(fm.icir,3),           fm.icir>=0.5?"g":"");
  mrow("IC Hit Rate",      fm.ic_hit_rate!=null?(fm.ic_hit_rate*100).toFixed(1)+"%":"—", fm.ic_hit_rate>=0.55?"g":"");
  mrow("Spearman Rho",     num(fm.spearman_rho,3),   Math.abs(fm.spearman_rho||0)>=0.7?"g":"");
  mrow("Monotonicity",     fm.monotonicity_score!=null?(fm.monotonicity_score*100).toFixed(1)+"%":"—", (fm.monotonicity_score||0)>=0.8?"g":"");

  msec("QUINTILE PERFORMANCE");
  mrow("Q1-Q5 Spread (CAGR)", pct(fm.quintile_spread_cagr), (fm.quintile_spread_cagr||0)>0?"g":"r");
  mrow("Q1 CAGR",          pct(fm.q1_cagr),          (fm.q1_cagr||0)>0?"g":"r");
  mrow("Q1 Sharpe",        num(fm.q1_sharpe,3),       (fm.q1_sharpe||0)>=0.5?"g":"");
  mrow("Q1 Max DD",        pct(fm.q1_max_dd),         "r");
  mrow("Qn CAGR",          pct(fm.qn_cagr),           (fm.qn_cagr||0)>0?"g":"r");
  mrow("Qn Sharpe",        num(fm.qn_sharpe,3));

  msec("BACKTEST SETTINGS");
  mrow("N Periods",        fm.n_obs||"—");
  mrow("Avg Stocks/Period",fm.n_stocks_avg!=null?Math.round(fm.n_stocks_avg):"—");
  mrow("Hold Period",      fm.hold_months?fm.hold_months+"mo":"—");
  mrow("N Buckets",        fm.n_buckets||"—");

  // Per-bucket compact table
  msec("PER-BUCKET METRICS");
  const hdrs = ["BUCKET","CAGR","SHARPE","MAX DD","SUREFIRE","EQ R²"];
  const tbl = document.createElement("div");
  tbl.style.cssText = "grid-column:1/-1;overflow-x:auto;background:#fff";
  let html = `<table style="width:100%;border-collapse:collapse;font-size:9px">
    <thead><tr>${hdrs.map(h=>`<th style="padding:4px 8px;background:#f1f5f9;color:#374151;font-size:8px;font-weight:700;text-align:right;border-bottom:2px solid #e5e7eb">${h}</th>`).join("")}</tr></thead><tbody>`;
  (buckets||[]).forEach((b,i) => {
    const bm = (bucketMetrics||{})[String(b)] || {};
    const c = `color:${FT_COLORS.q[i]||"#888"};font-weight:700`;
    html += `<tr style="background:${i%2?"#f9fafb":"#fff"}">
      <td style="padding:3px 8px;${c};text-align:right">Q${b}</td>
      <td style="padding:3px 8px;text-align:right;color:${(bm.cagr||0)>=0?"#15803d":"#b91c1c"};font-weight:600">${pct(bm.cagr)}</td>
      <td style="padding:3px 8px;text-align:right">${num(bm.sharpe)}</td>
      <td style="padding:3px 8px;text-align:right;color:#b91c1c">${pct(bm.max_dd)}</td>
      <td style="padding:3px 8px;text-align:right;color:${(bm.surefire_ratio||0)>=10?"#7c3aed":"#374151"}">${num(bm.surefire_ratio,1)}</td>
      <td style="padding:3px 8px;text-align:right">${num(bm.equity_r2,4)}</td>
    </tr>`;
  });
  html += "</tbody></table>";
  tbl.innerHTML = html;
  wrap.appendChild(tbl);

  return wrap;
}
