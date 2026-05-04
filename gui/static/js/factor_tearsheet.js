// factor_tearsheet.js — Plotly charts for the Factor Models tearsheet
// Completely separate from tearsheet.js — handles quintile-specific visualizations

const FT_COLORS = {
  q: ["#1d4ed8","#16a34a","#ca8a04","#ea580c","#dc2626"],  // Q1..Q5
  get bg()      { return getComputedStyle(document.body).getPropertyValue("--bg").trim()||"#ffffff"; },
  get plot_bg() { return getComputedStyle(document.body).getPropertyValue("--bg-panel").trim()||"#f8f9fa"; },
  get grid()    { return getComputedStyle(document.body).getPropertyValue("--border").trim()||"#e5e7eb"; },
  get tick()    { return getComputedStyle(document.body).getPropertyValue("--text-dim").trim()||"#374151"; },
  get legend()  { return getComputedStyle(document.body).getPropertyValue("--text").trim()||"#111111"; },
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
      text: cagrs.map(v=>(v>=0?"+":"")+v+"%"), textposition:"outside",
      textfont:{size:10, color:FT_COLORS.tick}, cliponaxis:false },
    { x: labels, y: excess, type:"bar", name:"Excess vs BM",
      marker:{ color: excess.map(v=>v>=0?"rgba(22,163,74,0.4)":"rgba(220,38,38,0.4)") },
      text: excess.map(v=>(v>=0?"+":"")+v+"%"), textposition:"inside",
      textfont:{size:8}, visible:"legendonly" },
  ], _ftLayout({
    barmode:"overlay",
    yaxis:{ ...{ gridcolor:FT_COLORS.grid, linecolor:"#cccccc", tickfont:{size:8}, autorange:true,
                 zeroline:true, zerolinecolor:"#374151", zerolinewidth:1 }, ticksuffix:"%" },
    xaxis:{ type:"-", gridcolor:FT_COLORS.grid, tickfont:{size:9} },
    margin:{ l:48, r:8, t:32, b:36 },  // t:32 gives room for outside labels
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
  if (!icData || !icData.length) {
    const el = document.getElementById(divId);
    if (el) el.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;font-size:10px;color:var(--text-muted)">No IC data available</div>';
    return;
  }
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
  if (!periodData || !periodData.length) {
    const el = document.getElementById(divId);
    if (el) el.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;font-size:10px;color:var(--text-muted)">No period data available</div>';
    return;
  }
  const dates  = periodData.map(d => d.date);
  const zData  = [];
  for (let b = 1; b <= n_buckets; b++) {
    zData.push(periodData.map(d => {
      const v = d["q"+b+"_ret"];
      return v != null ? +(v*100).toFixed(2) : null;
    }));
  }
  const yLabels = Array.from({length:n_buckets}, (_,i)=>"Q"+(i+1));

  // Format dates as "MMM YY" for readable x-axis labels
  const xLabels = dates.map(d => {
    const dt = new Date(d);
    return dt.toLocaleDateString("en-US", {month:"short", year:"2-digit"});
  });

  Plotly.newPlot(divId, [{
    z: zData, x: xLabels, y: yLabels,
    type:"heatmap",
    colorscale:[[0,"#b91c1c"],[0.45,"#fff7ed"],[0.5,"#fefce8"],[0.55,"#f0fdf4"],[1,"#15803d"]],
    zmid:0, zmin:-25, zmax:25,
    // Show value inside each cell — larger font, no % sign clutter
    text: zData.map(row=>row.map(v=>v==null?"":v.toFixed(1)+"%")),
    texttemplate:"%{text}",
    textfont:{size:8, color:"#111111"},
    showscale:true,
    colorbar:{ title:{text:"Period Ret %",side:"right"}, tickfont:{size:8}, len:0.9, thickness:12 },
    xgap:1, ygap:1,
  }], {
    paper_bgcolor:FT_COLORS.bg, plot_bgcolor:FT_COLORS.plot_bg,
    margin:{l:44,r:80,t:8,b:80},  // b:80 for rotated labels
    font:{family:"Segoe UI,Arial",size:9,color:FT_COLORS.tick},
    xaxis:{
      tickfont:{size:8,color:FT_COLORS.tick},
      tickangle:-45,    // rotate 45° so dates are readable
      gridcolor:FT_COLORS.grid,
      showgrid:false,
    },
    yaxis:{tickfont:{size:10,color:FT_COLORS.tick}, showgrid:false},
  }, FT_CFG);
}

// ── 5. Annual Returns by Quintile (grouped bar chart by year) ─────────────────
function ftDrawAnnualBars(divId, annualRetByBucket, n_buckets) {
  if (!annualRetByBucket || !Object.keys(annualRetByBucket).length) {
    const el = document.getElementById(divId);
    if (el) el.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;font-size:10px;color:var(--text-muted)">No annual return data available</div>';
    return;
  }
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

// ── Tortoriello Standard Backtest Table ──────────────────────────────────────
// Rows = metrics, Cols = Q1..Qn + Universe
// This is the institutional standard from Quantitative Strategies for Achieving Alpha
function ftBuildTortorielloTable(buckets, bucketMetrics, tortoriello, universeMetrics, universeMeta) {
  const wrap = document.createElement("div");
  wrap.style.cssText = "overflow-x:auto;margin:0 12px 12px;flex-shrink:0";

  const n = buckets.length;
  const um = universeMetrics || {};
  const ut = universeMeta  || {};  // {terminal_wealth, cagr}

  function pct(v, d=1, sign=false) {
    if(v==null||isNaN(v)) return "—";
    const s = (v*100).toFixed(d);
    return sign && v>=0 ? "+"+s+"%" : s+"%";
  }
  function num(v, d=2) { if(v==null||isNaN(v)) return "—"; return Number(v).toFixed(d); }
  function dollar(v)   { if(v==null||isNaN(v)) return "—"; return "$"+Math.round(v).toLocaleString(); }

  // Column headers
  const qCols = buckets.map(b => "Q"+b);
  const cols   = [...qCols, "UNIVERSE"];
  const colColors = [...buckets.map((_,i) => FT_COLORS.q[i]||"#374151"), "#6b7280"];

  // Rows definition: [label, accessor_fn, format_fn, color_fn]
  const rows = [
    ["CAGR — Period Rebalance",
      b => (bucketMetrics[b]||{}).cagr,
      v => pct(v,2,true),
      v => v>=0.08?"#15803d":v>=0?"#374151":"#b91c1c"],

    ["Avg Excess Return vs Universe",
      b => (tortoriello[b]||{}).avg_excess_vs_univ,
      v => pct(v,2,true),
      v => v>0.01?"#15803d":v>0?"#374151":"#b91c1c"],

    ["Value of $10,000 Invested",
      b => (tortoriello[b]||{}).terminal_wealth,
      v => dollar(v),
      v => v>15000?"#15803d":"#374151"],

    ["% of Periods Beats Universe",
      b => (tortoriello[b]||{}).pct_1y_beats_univ,
      v => pct(v,1),
      v => v>=0.60?"#15803d":v>=0.50?"#374151":"#b91c1c"],

    ["% Rolling 3-Yr Beats Universe",
      b => (tortoriello[b]||{}).pct_3y_beats_univ,
      v => pct(v,1),
      v => v>=0.70?"#15803d":v>=0.60?"#374151":"#b91c1c"],

    ["Maximum Gain (1 Period)",
      b => (tortoriello[b]||{}).max_gain,
      v => pct(v,2,true),
      v => "#374151"],

    ["Maximum Loss (1 Period)",
      b => (tortoriello[b]||{}).max_loss,
      v => pct(v,2),
      v => "#b91c1c"],

    ["Sharpe Ratio",
      b => (bucketMetrics[b]||{}).sharpe,
      v => num(v,3),
      v => v>=1?"#15803d":v>=0.5?"#374151":"#b91c1c"],

    ["Standard Deviation (Ann.)",
      b => (tortoriello[b]||{}).std_dev_ann,
      v => pct(v,2),
      v => "#374151"],

    ["Beta (vs Universe)",
      b => (tortoriello[b]||{}).beta_vs_univ,
      v => num(v,3),
      v => "#374151"],

    ["Alpha (vs Universe, Ann.)",
      b => (tortoriello[b]||{}).alpha_vs_univ,
      v => pct(v,2,true),
      v => v>0?"#15803d":"#b91c1c"],

    ["Calmar Ratio",
      b => (bucketMetrics[b]||{}).calmar,
      v => num(v,3),
      v => v>=0.5?"#15803d":"#374151"],

    ["Max Drawdown",
      b => (bucketMetrics[b]||{}).max_dd,
      v => pct(v,2),
      v => "#b91c1c"],

    ["Avg Portfolio Size",
      b => (tortoriello[b]||{}).avg_portfolio_size,
      v => v!=null?Math.round(v):"—",
      v => "#374151"],

    ["Avg Companies Beating Universe",
      b => (tortoriello[b]||{}).avg_beat_universe,
      v => v!=null?Math.round(v):"—",
      v => "#15803d"],

    ["Avg Companies Lagging Universe",
      b => (tortoriello[b]||{}).avg_lag_universe,
      v => v!=null?Math.round(v):"—",
      v => "#b91c1c"],

    ["Median Factor Score (Bucket)",
      b => (tortoriello[b]||{}).median_factor_score,
      v => num(v,1),
      v => "#7c3aed"],

    ["Avg Market Cap ($M)",
      b => (tortoriello[b]||{}).avg_market_cap,
      v => v!=null?("$"+Math.round(v/1e6).toLocaleString()+"M"):"—",
      v => "#374151"],
  ];

  // Universe values for the last column
  const univVals = {
    cagr: um.cagr,
    terminal_wealth: universeMeta?.terminal_wealth,
    avg_excess_vs_univ: 0,
    pct_1y_beats_univ: null,
    pct_3y_beats_univ: null,
    max_gain: ut.max_gain || (um.best_month),
    max_loss: ut.max_loss || (um.worst_month),
    sharpe: um.sharpe,
    std_dev_ann: um.ann_vol,
    beta_vs_univ: 1.0,
    alpha_vs_univ: 0,
    calmar: um.calmar,
    max_dd: um.max_dd,
    avg_portfolio_size: null,
    avg_beat_universe: null,
    avg_lag_universe: null,
    median_factor_score: null,
    avg_market_cap: null,
  };

  let html = `<table style="width:100%;border-collapse:collapse;font-size:9px;font-family:'Segoe UI',sans-serif;white-space:nowrap">
    <thead><tr>
      <th style="text-align:left;padding:5px 10px;background:#1e3a5f;color:#c9a84c;font-size:8px;font-weight:700;position:sticky;left:0;min-width:200px;border-right:2px solid #c9a84c">METRIC</th>
      ${cols.map((c,i)=>`<th style="text-align:right;padding:5px 10px;background:#1e3a5f;color:${colColors[i]};font-size:9px;font-weight:700;min-width:80px">${c}</th>`).join("")}
    </tr></thead><tbody>`;

  rows.forEach(([label, accessor, formatter, colorFn], ri) => {
    const bg = ri%2===0?"#ffffff":"#f9fafb";
    html += `<tr style="background:${bg}">
      <td style="padding:4px 10px;color:#374151;font-weight:600;position:sticky;left:0;background:${bg};border-right:2px solid #e5e7eb;border-bottom:1px solid #f3f4f6">${label}</td>`;

    // Bucket columns
    buckets.forEach((b,i) => {
      const raw = accessor(String(b));
      const val = formatter(raw);
      const col = colorFn(raw);
      html += `<td style="padding:4px 10px;text-align:right;font-weight:700;color:${col};border-bottom:1px solid #f3f4f6">${val}</td>`;
    });

    // Universe column
    const rowKey = Object.keys(univVals)[ri];
    const uRaw   = univVals[rowKey];
    const uVal   = uRaw!=null ? formatter(uRaw) : "—";
    const uCol   = uRaw!=null ? colorFn(uRaw) : "#9ca3af";
    html += `<td style="padding:4px 10px;text-align:right;color:${uCol};border-bottom:1px solid #f3f4f6">${uVal}</td></tr>`;
  });

  html += "</tbody></table>";
  wrap.innerHTML = html;
  return wrap;
}

// ═══════════════════════════════════════════════════════════════════
// TORTORIELLO-STYLE TABLES & CHARTS
// ═══════════════════════════════════════════════════════════════════

// ── Figure 2.3: Main Quintile Summary Table ─────────────────────────────────
// Rows = metrics, Cols = Q1..Qn + Universe + S&P500(benchmark)
function ftBuildMainTable(result) {
  const buckets = result.buckets || [1,2,3,4,5];
  const n = buckets.length;
  const bm = result.bucket_metrics || {};
  const tort = result.tortoriello || {};
  const um = result.universe_metrics || {};
  const fm = result.factor_metrics || {};
  const spy = result.spy_metrics || {};  // SPY benchmark column
  const hasSpy = Object.keys(spy).length > 0;

  function pct(v,d=1,sign=false) {
    if(v==null||isNaN(+v)) return "—";
    const s = (v*100).toFixed(d);
    return (sign&&v>=0?"+":"")+s+"%";
  }
  function num(v,d=2) { if(v==null||isNaN(+v)) return "—"; return Number(v).toFixed(d); }
  function dollar(v)  { if(v==null||isNaN(+v)) return "—"; return "$"+Math.round(v).toLocaleString(); }

  const S = 'style="padding:4px 9px;text-align:right;border-bottom:1px solid var(--border);white-space:nowrap"';
  const SL = 'style="padding:4px 9px;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap;font-weight:600;color:var(--text)"';
  const SH = 'style="padding:5px 9px;text-align:right;background:var(--bg-panel2);color:var(--text-dim);font-size:8px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;white-space:nowrap;border-bottom:2px solid var(--border)"';

  // Color helper for a value: green if good, red if bad
  function vc(v, isGoodPositive=true) {
    if(v==null||isNaN(+v)) return "color:var(--text-dim)";
    return isGoodPositive ? (v>0?"color:#4ade80":"color:#f87171") : (v<0?"color:#4ade80":"color:#f87171");
  }

  // Column data accessors: returns value for each column
  const getCols = (getterFn) => {
    const vals = buckets.map(b => getterFn(String(b)));
    const univVal = getterFn("univ");
    return [...vals, univVal];
  };

  // Row definitions: [label, getter, formatter, colorFn]
  const rows = [
    // getter signature: b = "1".."5" for buckets, "univ" for universe, "spy" for SPY
    ["CAGR — Period Rebalance",
      b => b==="univ"?(um.cagr):b==="spy"?(spy.cagr):((bm[b]||{}).cagr),
      v => pct(v,1,true), v => vc(v)],

    ["Avg Excess Return vs. Universe",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).avg_excess_vs_univ),
      v => pct(v,1,true), v => vc(v)],

    ["Value of $10,000 Invested",
      b => b==="univ"?(result.universe_terminal):b==="spy"?(spy.terminal_wealth):((tort[b]||{}).terminal_wealth),
      v => dollar(v), () => "color:var(--text)"],

    ["% of 1-Period Strategy Outperforms Universe",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).pct_1y_beats_univ),
      v => pct(v,1), v => v!=null?(v>=0.6?"color:#4ade80":v>=0.5?"color:var(--text)":"color:#f87171"):"color:var(--text-dim)"],

    ["% Rolling 3-Year Periods Strategy Outperforms",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).pct_3y_beats_univ),
      v => pct(v,1), v => v!=null?(v>=0.7?"color:#4ade80":v>=0.6?"color:var(--text)":"color:#f87171"):"color:var(--text-dim)"],

    ["Maximum Gain",
      b => b==="univ"?(um.best_month):b==="spy"?(spy.best_month):((tort[b]||{}).max_gain),
      v => pct(v,1,true), () => "color:#4ade80"],

    ["Maximum Loss",
      b => b==="univ"?(um.worst_month):b==="spy"?(spy.worst_month):((tort[b]||{}).max_loss),
      v => pct(v,1), () => "color:#f87171"],

    ["Sharpe Ratio",
      b => b==="univ"?(um.sharpe):b==="spy"?(spy.sharpe):((bm[b]||{}).sharpe),
      v => num(v,2), v => v!=null?(v>=1?"color:#4ade80":v>=0.5?"color:var(--text)":"color:#f87171"):"color:var(--text-dim)"],

    ["Standard Deviation of Returns (Ann.)",
      b => b==="univ"?(um.ann_vol):b==="spy"?(spy.ann_vol):((tort[b]||{}).std_dev_ann),
      v => pct(v,2), () => "color:var(--text)"],

    ["Beta (vs. Universe)",
      b => b==="univ"?1.0:b==="spy"?null:((tort[b]||{}).beta_vs_univ),
      v => v!=null?num(v,2):"—", () => "color:var(--text)"],

    ["Alpha (vs. Universe, Ann.)",
      b => b==="univ"?0:b==="spy"?null:((tort[b]||{}).alpha_vs_univ),
      v => pct(v,2,true), v => vc(v)],

    ["Average Portfolio Size",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).avg_portfolio_size),
      v => v!=null?Math.round(v):"—", () => "color:var(--text)"],

    ["Avg Companies Outperforming",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).avg_beat_universe),
      v => v!=null?Math.round(v):"—", () => "color:#4ade80"],

    ["Avg Companies Underperforming",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).avg_lag_universe),
      v => v!=null?Math.round(v):"—", () => "color:#f87171"],

    ["Median Factor Score (Bucket)",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).median_factor_score),
      v => num(v,1), () => "color:#a78bfa"],

    ["Average Market Cap ($M)",
      b => b==="univ"?null:b==="spy"?null:((tort[b]||{}).avg_market_cap),
      v => v!=null?("$"+Math.round(v/1e6).toLocaleString()+"M"):"—", () => "color:var(--text)"],

    ["Max Drawdown",
      b => b==="univ"?(um.max_dd):b==="spy"?(spy.max_dd):((bm[b]||{}).max_dd),
      v => pct(v,1), () => "color:#f87171"],

    ["Calmar Ratio",
      b => b==="univ"?(um.calmar):b==="spy"?(spy.calmar):((bm[b]||{}).calmar),
      v => num(v,2), v => v!=null?(v>=0.5?"color:#4ade80":v>=0.2?"color:var(--text)":"color:#f87171"):"color:var(--text-dim)"],
  ];

  // Column headers: Quintiles + Universe + SPY (if available)
  const spyCols   = hasSpy ? ["<b>S&amp;P 500</b>"] : [];
  const spyColors = hasSpy ? ["#f59e0b"] : [];
  const colHeaders = [
    ...buckets.map(b=>"<b>"+b+(b===1?"st":b===2?"nd":b===3?"rd":"th")+" Quintile</b>"),
    "<b>Universe</b>",
    ...spyCols,
  ];
  const colColors = [
    ...buckets.map((_,i)=>FT_COLORS.q[i]||"#aaa"),
    "#94a3b8",
    ...spyColors,
  ];

  let html = `<table style="width:100%;border-collapse:collapse;font-size:9.5px;font-family:'Segoe UI',sans-serif">
    <thead><tr>
      <th ${SH} style="text-align:left;min-width:220px">${result.dates?.[0]?.slice(0,4)||""} — ${result.dates?.[result.dates.length-1]?.slice(0,4)||""}</th>
      ${colHeaders.map((h,i)=>`<th ${SH} style="color:${colColors[i]};min-width:80px">${h}</th>`).join("")}
    </tr></thead><tbody>`;

  rows.forEach(([label, getter, fmt, colorFn], ri) => {
    const bg = ri%2===0?"background:var(--bg)":"background:var(--bg-panel)";
    html += `<tr style="${bg}"><td ${SL}>${label}</td>`;
    // Bucket columns + Universe + optional SPY
    const colKeys = [...buckets.map(b=>String(b)), "univ", ...(hasSpy?["spy"]:[])];
    colKeys.forEach(key => {
      const v = getter(key);
      const formatted = v!=null ? fmt(v) : "—";
      const color = v!=null ? colorFn(v) : "color:var(--text-dim)";
      html += `<td ${S} style="${S.slice(7,-1)};${color}">${formatted}</td>`;
    });
    html += "</tr>";
  });

  html += "</tbody></table>";
  const wrap = document.createElement("div");
  wrap.style.cssText = "overflow-x:auto;flex-shrink:0;border:1px solid var(--border);border-radius:4px;margin:0 12px 12px";
  wrap.innerHTML = html;
  return wrap;
}

// ── Figure 2.4: Sector Summary Tables (Top + Bottom Quintile) ────────────────
function ftBuildSectorTables(result) {
  const n = (result.buckets||[1,2,3,4,5]).length;
  const tort = result.tortoriello || {};
  const sa = result.sector_attribution || [];
  if (!sa.length) return document.createElement("div");

  const sectors = sa.map(s=>s.sector).filter(Boolean);

  function pct(v,d=1,sign=false) {
    if(v==null||isNaN(+v)) return "—";
    const s=(v*100).toFixed(d); return (sign&&v>=0?"+":"")+s+"%";
  }
  function vc(v) { if(v==null) return "color:var(--text-dim)"; return v>0?"color:#4ade80":"color:#f87171"; }

  const S = 'style="padding:3px 8px;text-align:right;border-bottom:1px solid var(--border);white-space:nowrap;font-size:9px"';
  const SL = 'style="padding:3px 8px;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap;font-size:9px;font-weight:600;color:var(--text)"';
  const SH = 'style="padding:4px 8px;text-align:right;background:var(--bg-panel2);color:var(--text-dim);font-size:7.5px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;border-bottom:2px solid var(--border)"';

  function buildTable(bucketNum, title, accentColor) {
    const bStr = String(bucketNum);
    const t = tort[bStr] || {};

    // Build sector data for this bucket
    const secData = {};
    sa.forEach(s => { secData[s.sector] = { q_ret: s["q"+bucketNum]||0, spread: s.spread||0 }; });

    let html = `<div style="font-size:10px;font-weight:800;color:${accentColor};padding:8px 10px 4px;background:var(--bg-panel);border-bottom:2px solid ${accentColor};letter-spacing:.5px">${title}</div>`;
    html += `<table style="width:100%;border-collapse:collapse;font-size:9px;font-family:'Segoe UI',sans-serif">
      <thead><tr>
        <th ${SH} style="text-align:left;min-width:140px">METRIC</th>
        ${sectors.map(s=>`<th ${SH}>${s.replace(" ","<br/>")}</th>`).join("")}
        <th ${SH}>Universe</th>
      </tr></thead><tbody>`;

    const rows2 = [
      ["CAGR – Quintile", s => secData[s]?.q_ret, v=>pct(v,1,true), vc],
      ["Excess Return vs. Universe", s => (secData[s]?.q_ret||0)-(result.universe_metrics?.cagr||0), v=>pct(v,1,true), vc],
      ["% 1-Period Outperform", () => t.pct_1y_beats_univ, v=>pct(v,1), v=>v>=0.6?"color:#4ade80":"color:var(--text)"],
      ["% 3-Year Outperform",  () => t.pct_3y_beats_univ, v=>pct(v,1), v=>v>=0.7?"color:#4ade80":"color:var(--text)"],
      ["Maximum Gain",  () => t.max_gain,  v=>pct(v,1,true), ()=>"color:#4ade80"],
      ["Maximum Loss",  () => t.max_loss,  v=>pct(v,1),      ()=>"color:#f87171"],
      ["Sharpe Ratio",  () => (result.bucket_metrics||{})[bStr]?.sharpe, v=>v?.toFixed(2)||"—", v=>v>=0.5?"color:#4ade80":"color:var(--text)"],
      ["Beta (vs. Universe)", () => t.beta_vs_univ, v=>v?.toFixed(2)||"—", ()=>"color:var(--text)"],
      ["Alpha (vs. Universe)", () => t.alpha_vs_univ, v=>pct(v,2,true), vc],
      ["Portfolio Size",       () => t.avg_portfolio_size, v=>v?Math.round(v):"—", ()=>"color:var(--text)"],
    ];

    rows2.forEach(([label, getter, fmt, colorFn], ri) => {
      const bg = ri%2===0?"background:var(--bg)":"background:var(--bg-panel)";
      html += `<tr style="${bg}"><td ${SL}>${label}</td>`;
      sectors.forEach(sec => {
        const raw = typeof getter === "function" ? (getter.length>0?getter(sec):getter()) : null;
        const formatted = raw!=null ? fmt(raw) : "—";
        const color = raw!=null ? colorFn(raw) : "color:var(--text-dim)";
        html += `<td ${S} style="${S.slice(7,-1)};${color}">${formatted}</td>`;
      });
      // Universe column
      const uRaw = typeof getter === "function" && getter.length===0 ? getter() : null;
      html += `<td ${S} style="${S.slice(7,-1)};color:var(--text-dim)">${uRaw!=null?fmt(uRaw):"—"}</td>`;
      html += "</tr>";
    });

    html += "</tbody></table>";
    return html;
  }

  const wrap = document.createElement("div");
  wrap.style.cssText = "overflow-x:auto;flex-shrink:0;border:1px solid var(--border);border-radius:4px;margin:0 12px 12px";
  wrap.innerHTML = buildTable(1, "TOP QUINTILE (Q1)", "#4ade80")
                 + '<div style="height:4px;background:var(--border)"></div>'
                 + buildTable(n, "BOTTOM QUINTILE (Q"+n+")", "#f87171");
  return wrap;
}

// ── Average Excess Returns vs Universe bar chart ─────────────────────────────
function ftDrawExcessReturnBar(divId, buckets, tort, universeMetrics) {
  const labels = buckets.map(b=>"Q"+b);
  const vals   = buckets.map(b => {
    const t = (tort||{})[String(b)] || {};
    return t.avg_excess_vs_univ!=null ? +(t.avg_excess_vs_univ*100).toFixed(2) : null;
  });
  Plotly.newPlot(divId, [{
    x: labels, y: vals, type:"bar",
    marker:{ color: vals.map((v,i)=>v!=null?(v>=0?"rgba(74,222,128,0.8)":"rgba(248,113,113,0.8)"):"rgba(148,163,184,0.5)") },
    text: vals.map(v=>v!=null?((v>=0?"+":"")+v+"%"):""),
    textposition:"outside", textfont:{size:9,color:FT_COLORS.tick},
    showlegend:false,
  },{
    x:["Q1",labels[labels.length-1]], y:[0,0], type:"scatter", mode:"lines",
    line:{color:FT_COLORS.tick,width:0.8}, showlegend:false,
  }], {
    paper_bgcolor:FT_COLORS.bg, plot_bgcolor:FT_COLORS.plot_bg,
    margin:{l:44,r:8,t:30,b:36},
    title:{text:"Average Excess Returns vs. Universe",font:{size:10,color:FT_COLORS.tick},x:0.5,xanchor:"center"},
    font:{family:"Segoe UI,Arial",size:9,color:FT_COLORS.tick},
    xaxis:{gridcolor:FT_COLORS.grid,tickfont:{size:9,color:FT_COLORS.tick}},
    yaxis:{gridcolor:FT_COLORS.grid,tickfont:{size:9,color:FT_COLORS.tick},
           ticksuffix:"%",zeroline:true,zerolinecolor:FT_COLORS.tick,zerolinewidth:1,autorange:true},
    showlegend:false,
  }, FT_CFG);
}

// ── Rolling 3-Year Annualized Excess Returns: Top vs Bottom ──────────────────
function ftDrawRolling3YTopBottom(divId, buckets, tort) {
  const n = buckets.length;
  const t1 = (tort||{})["1"] || {};
  const tn = (tort||{})[String(n)] || {};
  const x1  = t1.roll_3y_dates || [];
  const y1  = (t1.roll_3y_excess||[]).map(v=>+(v*100).toFixed(2));
  const xn  = tn.roll_3y_dates || [];
  const yn  = (tn.roll_3y_excess||[]).map(v=>+(v*100).toFixed(2));

  if(!x1.length && !xn.length) {
    const el = document.getElementById(divId);
    if (el) el.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;font-size:10px;color:var(--text-muted)">Need ≥3 years of data for rolling chart</div>';
    return;
  }

  Plotly.newPlot(divId, [
    { x:x1, y:y1, type:"scatter", mode:"lines",
      name:"Top Quintile (Q1)",
      line:{color:"rgba(74,222,128,0.9)",width:1.5},
      fill:"tozeroy", fillcolor:"rgba(74,222,128,0.12)" },
    { x:xn, y:yn, type:"scatter", mode:"lines",
      name:"Bottom Quintile (Q"+n+")",
      line:{color:"rgba(248,113,113,0.9)",width:1.5},
      fill:"tozeroy", fillcolor:"rgba(248,113,113,0.12)" },
    { x:x1.length?x1:xn, y:(x1.length?x1:xn).map(()=>0),
      type:"scatter", mode:"lines",
      line:{color:FT_COLORS.tick,width:0.8}, showlegend:false },
  ], {
    paper_bgcolor:FT_COLORS.bg, plot_bgcolor:FT_COLORS.plot_bg,
    margin:{l:48,r:8,t:30,b:36},
    title:{text:"Rolling 3-Year Periods, Annualized Excess Returns: Top/Bottom vs. Universe",
           font:{size:10,color:FT_COLORS.tick},x:0.5,xanchor:"center"},
    font:{family:"Segoe UI,Arial",size:9,color:FT_COLORS.tick},
    legend:{orientation:"h",x:0,y:1.12,font:{size:9,color:FT_COLORS.tick}},
    xaxis:{type:"date",gridcolor:FT_COLORS.grid,tickfont:{size:8,color:FT_COLORS.tick}},
    yaxis:{gridcolor:FT_COLORS.grid,tickfont:{size:8,color:FT_COLORS.tick},
           ticksuffix:"%",zeroline:true,zerolinecolor:FT_COLORS.tick,zerolinewidth:1,autorange:true},
    hovermode:"x unified",
  }, FT_CFG);
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
