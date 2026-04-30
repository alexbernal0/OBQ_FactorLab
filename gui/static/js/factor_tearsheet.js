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

// ── Rolling 3-Year Excess Return Chart ───────────────────────────────────────
function ftDrawRolling3Y(divId, tortoriello, buckets) {
  const traces = [];
  [0, buckets.length-1].forEach(bi => {
    const b = buckets[bi];
    const t = (tortoriello||{})[String(b)] || {};
    const x = t.roll_3y_dates || [];
    const y = (t.roll_3y_excess||[]).map(v=>+(v*100).toFixed(2));
    if(!x.length) return;
    traces.push({
      x, y, type:"scatter", mode:"lines",
      name:"Q"+b+" excess",
      line:{color:FT_COLORS.q[bi]||"#888",width:2},
      fill:"tozeroy",
      fillcolor:bi===0?"rgba(22,163,74,0.10)":"rgba(220,38,38,0.10)",
    });
  });
  traces.push({
    x:traces[0]?.x||[], y:(traces[0]?.x||[]).map(()=>0),
    type:"scatter", mode:"lines",
    line:{color:"#9ca3af",width:1,dash:"dot"}, showlegend:false,
  });
  Plotly.newPlot(divId, traces, _ftLayout({
    margin:{l:52,r:8,t:8,b:36},
    yaxis:{...{gridcolor:FT_COLORS.grid,linecolor:"#cccccc",tickfont:{size:8},autorange:true,
               zeroline:true,zerolinecolor:"#374151",zerolinewidth:1},
           ticksuffix:"%",title:{text:"Excess vs Universe",font:{size:8}}},
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
