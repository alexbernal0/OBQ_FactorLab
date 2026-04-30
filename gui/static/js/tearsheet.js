// tearsheet.js — Plotly-based charts for OBQ FactorLab

const PLOT_LAYOUT = {
  paper_bgcolor: "#ffffff",
  plot_bgcolor:  "#fafafa",
  font:  { family: "Segoe UI, Arial, sans-serif", size: 10, color: "#374151" },
  margin: { l: 52, r: 12, t: 10, b: 36 },
  legend: { orientation: "h", x: 0, y: 1.14, font: { size: 9, color:"#374151" }, bgcolor: "transparent" },
  xaxis: { type: "date", gridcolor: "#eeeeee", linecolor: "#cccccc", tickfont: { size: 8, color:"#374151" }, tickcolor: "#9ca3af", showgrid: true },
  yaxis: { gridcolor: "#eeeeee", linecolor: "#cccccc", tickfont: { size: 8, color:"#374151" }, tickcolor: "#9ca3af",
           zeroline: true, zerolinecolor: "#374151", zerolinewidth: 1, autorange: true },
  hovermode: "x unified",
  autosize: true,
};
const PLOT_CFG = { displayModeBar: false, responsive: true, staticPlot: false };

function _L(extra) { return Object.assign({}, PLOT_LAYOUT, extra || {}); }

// ── Equity + Drawdown joined subplot ────────────────────────────────────────
function drawEquityWithDD(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const x = dates.map(d => String(d).slice(0,10));
  let peak = equity[0];
  const dd = equity.map(v => { peak = Math.max(peak, v); return (v / peak - 1) * 100; });

  Plotly.newPlot(divId, [
    // Top: equity curve
    { x, y: equity, type:"scatter", mode:"lines", name:"Portfolio",
      line:{ color:"#0066cc", width:1.5 },
      fill:"tozeroy", fillcolor:"rgba(0,102,204,0.07)",
      yaxis:"y" },
    // Bottom: drawdown
    { x, y: dd, type:"scatter", mode:"lines", name:"Drawdown",
      line:{ color:"#dc2626", width:1 },
      fill:"tozeroy", fillcolor:"rgba(220,38,38,0.18)",
      yaxis:"y2" },
  ], {
    paper_bgcolor:"#ffffff", plot_bgcolor:"#ffffff",
    margin:{ l:56, r:12, t:10, b:36 },
    hovermode:"x unified",
    legend:{ orientation:"h", x:0, y:1.06, font:{size:9} },
    grid:{ rows:2, columns:1, subplots:[["xy"],["xy2"]], roworder:"top to bottom" },
    yaxis:{
      domain:[0.35,1], gridcolor:"#f3f4f6", linecolor:"#e5e7eb",
      tickfont:{size:8}, tickformat:".2f", title:{text:"Growth (×)", font:{size:8}},
    },
    yaxis2:{
      domain:[0,0.3], gridcolor:"#f3f4f6", linecolor:"#e5e7eb",
      tickfont:{size:8}, ticksuffix:"%", tickformat:".1f",
      title:{text:"DD%", font:{size:8}},
    },
    xaxis:{ gridcolor:"#f3f4f6", tickfont:{size:8}, anchor:"y2" },
  }, PLOT_CFG);
}

// Keep standalone versions for other uses
function drawEquity(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const x = dates.map(d => String(d).slice(0,10));
  Plotly.newPlot(divId, [{
    x, y: equity, type:"scatter", mode:"lines", name:"Portfolio",
    line:{ color:"#0066cc", width:1.5 },
    fill:"tozeroy", fillcolor:"rgba(0,102,204,0.07)",
  }], _L({ yaxis:{ ...PLOT_LAYOUT.yaxis, tickformat:".2f" }, margin:{l:52,r:12,t:8,b:36} }), PLOT_CFG);
}

function drawDrawdown(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const x = dates.map(d => String(d).slice(0,10));
  let peak = equity[0];
  const dd = equity.map(v => { peak = Math.max(peak,v); return (v/peak-1)*100; });
  Plotly.newPlot(divId,[{
    x, y:dd, type:"scatter", mode:"lines", name:"Drawdown",
    line:{color:"#dc2626",width:1}, fill:"tozeroy", fillcolor:"rgba(220,38,38,0.15)",
  }], _L({ yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%",tickformat:".1f"}, margin:{l:52,r:12,t:8,b:36} }), PLOT_CFG);
}

// ── Distribution histogram ───────────────────────────────────────────────────
function drawDistribution(divId, monthlyRets) {
  if (!monthlyRets?.length) return;
  const vals = monthlyRets.map(v => +(v*100).toFixed(3));
  const mean = vals.reduce((a,b)=>a+b,0)/vals.length;
  Plotly.newPlot(divId, [
    { x:vals, type:"histogram", name:"Returns", histnorm:"probability density",
      marker:{color:"rgba(0,102,204,0.5)", line:{color:"#0066cc",width:0.5}}, nbinsx:25 },
    { x:[mean,mean], y:[0,0.5], type:"scatter", mode:"lines", name:`Mean ${mean.toFixed(2)}%`,
      line:{color:"#16a34a",width:2,dash:"dot"} },
  ], _L({ xaxis:{...PLOT_LAYOUT.xaxis,title:{text:"Monthly Return (%)",font:{size:8}}},
          yaxis:{...PLOT_LAYOUT.yaxis,title:{text:"Density",font:{size:8}}},
          showlegend:true, margin:{l:44,r:8,t:8,b:40}, barmode:"overlay" }), PLOT_CFG);
}

// ── Rolling Volatility ───────────────────────────────────────────────────────
function drawRollingVol(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const W=12;
  const rets = equity.slice(1).map((v,i)=>v/(equity[i]||1)-1);
  const rolVol = rets.map((_,i)=>{
    if(i<W-1) return null;
    const w=rets.slice(i-W+1,i+1);
    const mu=w.reduce((a,b)=>a+b,0)/W;
    const sd=Math.sqrt(w.reduce((s,v)=>s+(v-mu)**2,0)/W);
    return +(sd*Math.sqrt(12)*100).toFixed(2);
  });
  const x=dates.slice(1).map(d=>String(d).slice(0,10));
  Plotly.newPlot(divId,[{
    x, y:rolVol, type:"scatter", mode:"lines", name:"Ann. Vol %",
    line:{color:"#7c3aed",width:2.5}, fill:"tozeroy", fillcolor:"rgba(124,58,237,0.15)",
  }], _L({
    yaxis:{...PLOT_LAYOUT.yaxis,autorange:true,ticksuffix:"%",title:{text:"Vol %",font:{size:8}}},
    margin:{l:48,r:8,t:8,b:36}
  }), PLOT_CFG);
}

// ── Rolling Sortino ──────────────────────────────────────────────────────────
function drawRollingSortino(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const W=12;
  const rets=equity.slice(1).map((v,i)=>v/(equity[i]||1)-1);
  const rs=rets.map((_,i)=>{
    if(i<W-1) return null;
    const w=rets.slice(i-W+1,i+1);
    const mu=w.reduce((a,b)=>a+b,0)/W;
    const dn=w.filter(v=>v<0); const ds=dn.length>0?Math.sqrt(dn.reduce((s,v)=>s+v*v,0)/dn.length):0;
    return ds>0?+Math.min(10, mu/ds*Math.sqrt(12)).toFixed(3):null;
  });
  const x=dates.slice(1).map(d=>String(d).slice(0,10));
  Plotly.newPlot(divId,[
    { x, y:rs, type:"scatter", mode:"lines", name:"Sortino",
      line:{color:"#b45309",width:2.5}, fill:"tozeroy", fillcolor:"rgba(180,83,9,0.15)" },
    { x:[x[0],x[x.length-1]], y:[0,0], type:"scatter", mode:"lines",
      line:{color:"#6b7280",width:1}, showlegend:false },
  ], _L({ yaxis:{...PLOT_LAYOUT.yaxis,autorange:true,title:{text:"Sortino",font:{size:8}}}, margin:{l:48,r:8,t:8,b:36} }), PLOT_CFG);
}

// ── Drawdown table (top N) rendered as HTML ──────────────────────────────────
function buildDrawdownTable(dates, equity, topN) {
  topN = topN || 20;
  if (!dates?.length || !equity?.length) return document.createElement("div");
  let peak=equity[0], peakIdx=0, inDD=false, start=0, trough=equity[0], troughIdx=0;
  const rows=[];
  for(let i=1;i<equity.length;i++){
    if(!inDD && equity[i]<peak){ inDD=true; start=i-1; trough=equity[i]; troughIdx=i; }
    else if(inDD && equity[i]<trough){ trough=equity[i]; troughIdx=i; }
    else if(inDD && equity[i]>=peak){ 
      const depth=(trough/peak-1)*100;
      if(depth<=-1) rows.push({start:dates[start],trough:dates[troughIdx],recovery:dates[i],
        depth:depth.toFixed(2),dur:troughIdx-start,rec:i-troughIdx,total:i-start});
      inDD=false; peak=equity[i]; peakIdx=i;
    }
    if(equity[i]>peak){ peak=equity[i]; peakIdx=i; }
  }
  if(inDD){ const depth=(trough/peak-1)*100; if(depth<=-1)
    rows.push({start:dates[start],trough:dates[troughIdx],recovery:"Ongoing",
      depth:depth.toFixed(2),dur:troughIdx-start,rec:"—",total:"—"}); }
  rows.sort((a,b)=>parseFloat(a.depth)-parseFloat(b.depth));
  const top=rows.slice(0,topN);
  const wrap=document.createElement("div"); wrap.style.overflowX="auto";
  const ths=["Rank","Start","Trough","Recovery","Depth %","DD Days","Rec Days","Total Days"];
  let html=`<table style="width:100%;border-collapse:collapse;font-size:9px;font-family:'Segoe UI',sans-serif">
    <thead><tr>${ths.map(h=>`<th style="padding:4px 6px;background:#f1f5f9;color:#374151;font-weight:700;font-size:8px;text-align:right;border-bottom:2px solid #e5e7eb;white-space:nowrap">${h}</th>`).join("")}</tr></thead><tbody>`;
  top.forEach((r,i)=>{
    const dc=parseFloat(r.depth)<-15?"#b91c1c":parseFloat(r.depth)<-8?"#d97706":"#374151";
    html+=`<tr style="background:${i%2?"#f9fafb":"#fff"}">
      <td style="padding:3px 6px;text-align:right;color:#6b7280">${i+1}</td>
      <td style="padding:3px 6px;text-align:right;white-space:nowrap">${String(r.start).slice(0,10)}</td>
      <td style="padding:3px 6px;text-align:right;white-space:nowrap">${String(r.trough).slice(0,10)}</td>
      <td style="padding:3px 6px;text-align:right;white-space:nowrap;color:#16a34a">${String(r.recovery).slice(0,10)}</td>
      <td style="padding:3px 6px;text-align:right;font-weight:700;color:${dc}">${r.depth}%</td>
      <td style="padding:3px 6px;text-align:right">${r.dur}</td>
      <td style="padding:3px 6px;text-align:right">${r.rec}</td>
      <td style="padding:3px 6px;text-align:right">${r.total}</td>
    </tr>`;
  });
  html+="</tbody></table>";
  wrap.innerHTML=html; return wrap;
}

function drawDrawdown(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  let peak = equity[0];
  const dd = equity.map(v => { peak=Math.max(peak,v); return (v/peak-1)*100; });
  Plotly.newPlot(divId, [{
    x: dates.map(d=>String(d).slice(0,10)), y: dd,
    type:"scatter", mode:"lines", name:"Drawdown",
    line:{ color:"#dc2626", width:1 },
    fill:"tozeroy", fillcolor:"rgba(220,38,38,0.15)",
  }], _L({ yaxis:{...PLOT_LAYOUT.yaxis, ticksuffix:"%", tickformat:".1f"}, margin:{l:52,r:12,t:8,b:36} }), PLOT_CFG);
}

function drawAnnualBars(divId, annualData) {
  if (!annualData?.length) return;
  const x = annualData.map(d=>String(d.year));
  const y = annualData.map(d=>+(d.ret*100).toFixed(2));
  Plotly.newPlot(divId, [{
    x, y, type:"bar",
    marker:{ color: y.map(v=>v>=0?"rgba(22,163,74,0.75)":"rgba(220,38,38,0.75)") },
    name:"Annual Return",
  }], _L({ yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%"}, showlegend:false, margin:{l:44,r:8,t:6,b:36} }), PLOT_CFG);
}

function drawRollingSharpe(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const W=12;
  const rets = equity.slice(1).map((v,i)=>v/(equity[i]||1)-1);
  const rs = rets.map((_,i)=>{
    if(i<W-1) return null;
    const w=rets.slice(i-W+1,i+1);
    const mu=w.reduce((a,b)=>a+b,0)/W;
    const sd=Math.sqrt(w.reduce((s,v)=>s+(v-mu)**2,0)/W);
    return sd>0?mu/sd*Math.sqrt(12):null;
  });
  const x=dates.slice(1).map(d=>String(d).slice(0,10));
  Plotly.newPlot(divId, [
    { x, y:rs, type:"scatter", mode:"lines", name:"Rolling Sharpe", line:{color:"#0066cc",width:1.5}, fill:"tozeroy", fillcolor:"rgba(0,102,204,0.07)" },
    { x:[x[0],x[x.length-1]], y:[0,0], type:"scatter", mode:"lines", line:{color:"#9ca3af",width:1,dash:"dot"}, showlegend:false },
  ], _L({ yaxis:{...PLOT_LAYOUT.yaxis}, margin:{l:44,r:8,t:6,b:36} }), PLOT_CFG);
}

function drawIC(divId, ic_data) {
  if (!ic_data?.length) return;
  const x=ic_data.map(d=>String(d.date||"").slice(0,10));
  const y=ic_data.map(d=>+((d.ic_value??d.ic??0)*100).toFixed(2));
  Plotly.newPlot(divId,[{x,y,type:"bar",marker:{color:y.map(v=>v>=0?"rgba(0,102,204,0.6)":"rgba(220,38,38,0.6)")},name:"IC (%)"}],
    _L({yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%"},showlegend:false}),PLOT_CFG);
}

function drawSectorBar(divId, data) {
  const s=[...data].sort((a,b)=>b.spread-a.spread);
  const y=s.map(d=>d.gic_sector||d.index||"Other");
  const x=s.map(d=>+((d.spread||0)*100).toFixed(2));
  Plotly.newPlot(divId,[{x,y,type:"bar",orientation:"h",marker:{color:x.map(v=>v>=0?"rgba(22,163,74,0.7)":"rgba(220,38,38,0.7)")},name:"Q1-Q5 Spread"}],
    _L({xaxis:{...PLOT_LAYOUT.xaxis,ticksuffix:"%"},showlegend:false,margin:{l:120,r:12,t:6,b:36}}),PLOT_CFG);
}

function buildHeatmapTable(hm) {
  if (!hm) return document.createElement("div");
  const MN=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const wrap=document.createElement("div"); wrap.style.overflowX="auto";
  let html=`<table style="width:100%;border-collapse:collapse;font-size:9px;font-family:'Segoe UI',sans-serif">
  <thead><tr><th style="text-align:left;padding:3px 6px;color:#6b7280;font-size:8px;border-bottom:2px solid #e5e7eb;white-space:nowrap">YR</th>
  ${MN.map(m=>`<th style="text-align:center;padding:3px 3px;color:#6b7280;font-size:8px;border-bottom:2px solid #e5e7eb">${m}</th>`).join("")}
  <th style="text-align:right;padding:3px 6px;color:#6b7280;font-size:8px;border-bottom:2px solid #e5e7eb">ANN</th></tr></thead><tbody>`;
  hm.years.forEach((yr,yi)=>{
    const row=hm.data[yi]||[]; let ann=1.0;
    row.forEach(v=>{ if(v!=null) ann*=(1+v/100); }); ann=(ann-1)*100;
    html+=`<tr><td style="padding:2px 6px;color:#374151;font-weight:600;border-bottom:1px solid #f3f4f6;white-space:nowrap">${yr}</td>`;
    row.forEach(v=>{
      if(v==null){html+=`<td style="padding:2px 3px;text-align:center;color:#9ca3af;font-size:8px">—</td>`;return;}
      const i=Math.min(Math.abs(v)/10,1);
      const bg=v>=0?`rgba(22,163,74,${0.12+i*0.68})`:`rgba(220,38,38,${0.12+i*0.68})`;
      const tc=i>0.55?"#fff":(v>=0?"#15803d":"#b91c1c");
      html+=`<td style="padding:2px 3px;text-align:center;background:${bg};color:${tc};font-size:8.5px;white-space:nowrap">${v.toFixed(1)}</td>`;
    });
    const ac=ann>=0?"#15803d":"#b91c1c"; const abg=ann>=0?"rgba(22,163,74,0.10)":"rgba(220,38,38,0.10)";
    html+=`<td style="padding:2px 6px;text-align:right;font-weight:700;color:${ac};background:${abg};white-space:nowrap">${ann.toFixed(1)}%</td></tr>`;
  });
  html+=`</tbody></table>`; wrap.innerHTML=html; return wrap;
}

function buildAnnualTable(ann, nq) {
  if (!ann?.year) return document.createElement("div");
  const years=Object.values(ann.year); const wrap=document.createElement("div"); wrap.style.overflowX="auto";
  let th=`<tr><th>YEAR</th>`;
  for(let q=1;q<=nq;q++) th+=`<th>Q${q}</th>`;
  th+=`<th>UNIVERSE</th></tr>`;
  let rows="";
  years.forEach((y,i)=>{
    rows+=`<tr><td>${y}</td>`;
    for(let q=1;q<=nq;q++){const v=ann[`Q${q}`]?.[i]??null;const c=v!=null?(v>=0?"#15803d":"#b91c1c"):"#9ca3af";rows+=`<td style="color:${c}">${v!=null?((v*100).toFixed(1)+"%"):"—"}</td>`;}
    const u=ann.Universe?.[i]??null;
    rows+=`<td style="color:${u!=null?(u>=0?"#15803d":"#b91c1c"):"#9ca3af"}">${u!=null?((u*100).toFixed(1)+"%"):"—"}</td></tr>`;
  });
  wrap.innerHTML=`<table style="width:100%;border-collapse:collapse;font-size:10px;font-family:'Segoe UI',sans-serif"><thead style="background:#f8f9fa;color:#6b7280;font-size:8px;font-weight:700;text-transform:uppercase">${th}</thead><tbody>${rows}</tbody></table>`;
  return wrap;
}

// ── Rolling Sharpe + Sortino combined ────────────────────────────────────────
function drawRollingCombined(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const W=12, rets=equity.slice(1).map((v,i)=>v/(equity[i]||1)-1);
  const rs=[], rso=[];
  for (let i=0;i<rets.length;i++) {
    if (i<W-1) { rs.push(null); rso.push(null); continue; }
    const w=rets.slice(i-W+1,i+1);
    const mu=w.reduce((a,b)=>a+b,0)/W;
    const sd=Math.sqrt(w.reduce((s,v)=>s+(v-mu)**2,0)/W);
    rs.push(sd>0?+(mu/sd*Math.sqrt(12)).toFixed(3):null);
    const dn=w.filter(v=>v<0); const ds=dn.length>0?Math.sqrt(dn.reduce((s,v)=>s+v*v,0)/dn.length):0;
    rso.push(ds>0?+Math.min(10, mu/ds*Math.sqrt(12)).toFixed(3):null);
  }
  // Use full date strings — Plotly needs YYYY-MM-DD for date axis
  const x=dates.slice(1).map(d=>String(d).slice(0,10));
  Plotly.newPlot(divId,[
    {x,y:rs, type:"scatter",mode:"lines",name:"Sharpe", line:{color:"#1d4ed8",width:2.5},fill:"tozeroy",fillcolor:"rgba(29,78,216,0.12)"},
    {x,y:rso,type:"scatter",mode:"lines",name:"Sortino",line:{color:"#b45309",width:2,dash:"dash"}},
    {x:[x[0],x[x.length-1]],y:[0,0],type:"scatter",mode:"lines",line:{color:"#6b7280",width:1},showlegend:false},
  ],_L({margin:{l:48,r:8,t:8,b:36},yaxis:{...PLOT_LAYOUT.yaxis,autorange:true,title:{text:"Ratio",font:{size:8}}}}),PLOT_CFG);
}

// ── Rolling Max Drawdown ─────────────────────────────────────────────────────
function drawRollingMaxDD(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const W=12, rets=equity.slice(1).map((v,i)=>v/(equity[i]||1)-1);
  const rmdd=rets.map((_,i)=>{
    if(i<W-1) return null;
    const w=rets.slice(i-W+1,i+1);
    let eq=1, peak=1, mdd=0;
    w.forEach(r=>{eq*=(1+r);peak=Math.max(peak,eq);mdd=Math.min(mdd,(eq/peak-1)*100);});
    return +mdd.toFixed(2);
  });
  const x=dates.slice(1).map(d=>String(d).slice(0,10));
  Plotly.newPlot(divId,[
    {x,y:rmdd,type:"scatter",mode:"lines",name:"Rolling Max DD",
     line:{color:"#dc2626",width:2.5},fill:"tozeroy",fillcolor:"rgba(220,38,38,0.30)"},
    {x:[x[0],x[x.length-1]],y:[0,0],type:"scatter",mode:"lines",line:{color:"#6b7280",width:1},showlegend:false},
  ],_L({
    margin:{l:52,r:8,t:8,b:36},
    yaxis:{...PLOT_LAYOUT.yaxis,autorange:true,ticksuffix:"%",title:{text:"Max DD%",font:{size:8}},
           zeroline:true,zerolinecolor:"#374151",zerolinewidth:1}
  }),PLOT_CFG);
}

// ── Omega Ratio Curve ────────────────────────────────────────────────────────
function drawOmegaCurve(divId, monthlyRets) {
  if (!monthlyRets?.length) return;
  const vals=monthlyRets.map(v=>v*100);
  const thresholds=Array.from({length:61},(_,i)=>-3+i*0.1);
  const omega=thresholds.map(thr=>{
    let g=0,l=0; vals.forEach(v=>{const e=v-thr; if(e>0)g+=e; else l+=Math.abs(e);});
    return l>0?+(g/l).toFixed(4):null;
  });
  Plotly.newPlot(divId,[
    {x:thresholds,y:omega,type:"scatter",mode:"lines",name:"Omega",line:{color:"#0066cc",width:1.5}},
    {x:[-3,3],y:[1,1],type:"scatter",mode:"lines",name:"Omega=1",line:{color:"#dc2626",width:1,dash:"dash"},showlegend:true},
    {x:[0,0],y:[0,8],type:"scatter",mode:"lines",name:"Thr=0",line:{color:"#9ca3af",width:1,dash:"dot"},showlegend:false},
  ],_L({
    margin:{l:44,r:8,t:6,b:36},
    yaxis:{...PLOT_LAYOUT.yaxis,range:[0,8],title:{text:"Omega Ratio",font:{size:8}}},
    xaxis:{...PLOT_LAYOUT.xaxis,ticksuffix:"%",title:{text:"Threshold (%)",font:{size:8}}},
  }),PLOT_CFG);
}

// ── ACF / PACF ───────────────────────────────────────────────────────────────
function drawACF(divId, monthlyRets, isPartial) {
  if (!monthlyRets?.length) return;
  const vals=monthlyRets.map(v=>v*100);
  const n=vals.length, nlags=Math.min(24,Math.floor(n/2));
  const mean=vals.reduce((a,b)=>a+b,0)/n;
  const d=vals.map(v=>v-mean);
  const c0=d.reduce((s,v)=>s+v*v,0)/n;
  const acfVals=[];
  for (let lag=1;lag<=nlags;lag++){
    let ck=0; for(let i=0;i<n-lag;i++) ck+=d[i]*d[i+lag];
    acfVals.push(c0>0?(ck/n)/c0:0);
  }
  let plotVals=acfVals;
  if (isPartial) {
    // Durbin-Levinson approximation
    const pacf=[acfVals[0]];
    let prev=[acfVals[0]];
    for(let k=1;k<nlags;k++){
      const num=acfVals[k]-prev.reduce((s,p,j)=>s+p*acfVals[k-j-2]||0,0);
      const den=1-prev.reduce((s,p,j)=>s+p*acfVals[j],0);
      const phi=den!==0?num/den:0;
      pacf.push(isFinite(phi)?+phi.toFixed(4):0);
      prev=[...Array(k+1)].map((_,j)=>j<k?prev[j]-phi*prev[k-1-j]:phi);
    }
    plotVals=pacf;
  }
  const lags=Array.from({length:nlags},(_,i)=>i+1);
  const ci=1.96/Math.sqrt(n);
  Plotly.newPlot(divId,[
    {x:lags,y:plotVals,type:"bar",name:isPartial?"PACF":"ACF",
     marker:{color:plotVals.map(v=>v>=0?"rgba(0,102,204,0.7)":"rgba(220,38,38,0.7)")}},
    {x:[1,nlags],y:[ci,ci],type:"scatter",mode:"lines",line:{color:"#9ca3af",width:1,dash:"dash"},showlegend:false},
    {x:[1,nlags],y:[-ci,-ci],type:"scatter",mode:"lines",line:{color:"#9ca3af",width:1,dash:"dash"},showlegend:false},
  ],_L({
    margin:{l:44,r:8,t:6,b:36},showlegend:false,
    xaxis:{...PLOT_LAYOUT.xaxis,title:{text:"Lag (months)",font:{size:8}}},
    yaxis:{...PLOT_LAYOUT.yaxis,title:{text:isPartial?"Partial ACF":"ACF",font:{size:8}}},
  }),PLOT_CFG);
}

// ── Monthly Active Returns ───────────────────────────────────────────────────
function drawActiveReturns(divId, dates, equity, bmEquity) {
  if (!dates?.length || !equity?.length || !bmEquity?.length) return;
  // Build monthly returns from equity arrays
  const mRets=[],mBm=[],mLabels=[];
  for(let i=1;i<equity.length;i++){
    mRets.push(equity[i]/equity[i-1]-1);
    mBm.push(bmEquity[i]?bmEquity[i]/bmEquity[i-1]-1:0);
    mLabels.push(String(dates[i]).slice(0,10));
  }
  const active=mRets.map((r,i)=>+((r-mBm[i])*100).toFixed(3));
  Plotly.newPlot(divId,[
    {x:mLabels,y:active,type:"bar",name:"Active Return",
     marker:{color:active.map(v=>v>=0?"rgba(22,163,74,0.7)":"rgba(220,38,38,0.7)")}},
  ],_L({
    margin:{l:44,r:8,t:6,b:36},showlegend:false,
    yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%",title:{text:"Active Ret %",font:{size:8}}},
  }),PLOT_CFG);
}

// ── Best/Worst months table ──────────────────────────────────────────────────
function buildBestWorstTable(dates, equity, topN) {
  topN=topN||5;
  if(!dates?.length||!equity?.length) return document.createElement("div");
  const months=[];
  for(let i=1;i<equity.length;i++){
    months.push({label:String(dates[i]).slice(0,10), ret:+((equity[i]/equity[i-1]-1)*100).toFixed(2)});
  }
  months.sort((a,b)=>b.ret-a.ret);
  const best=months.slice(0,topN), worst=months.slice(-topN).reverse();
  const wrap=document.createElement("div");
  wrap.style.cssText="display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:8px 12px";
  const buildHalf=(title,rows,color)=>{
    let html=`<div><div style="font-size:8px;font-weight:700;color:${color};letter-spacing:1px;text-transform:uppercase;margin-bottom:4px">${title}</div><table style="width:100%;border-collapse:collapse;font-size:9px">`;
    rows.forEach((r,i)=>{ html+=`<tr style="background:${i%2?"#f9fafb":"#fff"}"><td style="padding:3px 6px;color:#374151">${r.label}</td><td style="padding:3px 6px;text-align:right;font-weight:700;color:${color}">${r.ret>0?"+":""}${r.ret}%</td></tr>`; });
    html+="</table></div>";
    const d=document.createElement("div"); d.innerHTML=html; return d;
  };
  wrap.appendChild(buildHalf("TOP 5 BEST MONTHS",best,"#15803d"));
  wrap.appendChild(buildHalf("TOP 5 WORST MONTHS",worst,"#b91c1c"));
  return wrap;
}

// ── Crisis Periods 2x4 Grid ──────────────────────────────────────────────────
const CRISIS_PERIODS=[
  {name:"Dot-Com Crash",  start:"2000-03-24",end:"2002-10-09"},
  {name:"9/11 Aftermath", start:"2001-09-10",end:"2001-09-21"},
  {name:"GFC",            start:"2007-10-09",end:"2009-03-09"},
  {name:"European Debt",  start:"2011-05-02",end:"2011-10-04"},
  {name:"2015-16 Corr.",  start:"2015-08-10",end:"2016-02-11"},
  {name:"Volmageddon",    start:"2018-01-26",end:"2018-02-09"},
  {name:"COVID-19",       start:"2020-02-20",end:"2020-03-23"},
  {name:"2022 Rate Hike", start:"2022-01-03",end:"2022-05-16"},
];

function drawCrisisGrid(containerId, dates, equity) {
  const container=document.getElementById(containerId);
  if(!container||!dates?.length||!equity?.length) return;
  const dStr=dates.map(d=>String(d).slice(0,10));
  CRISIS_PERIODS.forEach((c,idx)=>{
    const cell=document.createElement("div");
    cell.style.cssText="background:#fff;border:1px solid #e5e7eb;border-radius:3px;padding:6px;overflow:hidden";
    const title=document.createElement("div");
    title.style.cssText="font-size:8px;font-weight:700;color:#374151;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis";
    title.textContent=c.name; cell.appendChild(title);
    // Filter to crisis window
    const si=dStr.findIndex(d=>d>=c.start);
    const ei=dStr.findIndex(d=>d>c.end);
    const endIdx=ei===-1?dStr.length:ei;
    if(si===-1||endIdx<=si){
      const na=document.createElement("div");
      na.style.cssText="font-size:9px;color:#9ca3af;padding:20px 0;text-align:center";
      na.textContent="N/A — No history"; cell.appendChild(na);
    } else {
      const sliceEq=equity.slice(si,endIdx);
      const sliceDt=dStr.slice(si,endIdx);
      const base=sliceEq[0]||1;
      const norm=sliceEq.map(v=>+((v/base*100)).toFixed(2));
      const divId="crisis_"+idx+"_"+containerId;
      const d=document.createElement("div"); d.id=divId; d.style.cssText="height:100px;width:100%";
      cell.appendChild(d);
      // MUST append cell to container BEFORE Plotly.newPlot so the div exists in DOM
      container.appendChild(cell);
      const col=norm[norm.length-1]>=100?"#0066cc":"#dc2626";
      Plotly.newPlot(divId,[
        {x:sliceDt,y:norm,type:"scatter",mode:"lines",line:{color:col,width:1.5},showlegend:false},
        {x:[sliceDt[0],sliceDt[sliceDt.length-1]],y:[100,100],type:"scatter",mode:"lines",
         line:{color:"#9ca3af",width:0.8,dash:"dot"},showlegend:false},
      ],{
        paper_bgcolor:"#fff",plot_bgcolor:"#fafafa",
        margin:{l:28,r:4,t:2,b:24},
        xaxis:{type:"date",gridcolor:"#eeeeee",tickfont:{size:6},tickcolor:"#9ca3af",nticks:3},
        yaxis:{gridcolor:"#eeeeee",tickfont:{size:6},tickcolor:"#9ca3af",autorange:true},
        hovermode:false,
      },{displayModeBar:false,responsive:true});
        return; // cell already appended above
    }
    container.appendChild(cell);
  });
}

// ── Equity curve LOG scale ────────────────────────────────────────────────────
function drawEquityLog(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  const x = dates.map(d=>String(d).slice(0,10));
  Plotly.newPlot(divId, [{
    x, y: equity, type:"scatter", mode:"lines", name:"Portfolio (log)",
    line:{color:"#1d4ed8",width:2}, fill:"tozeroy", fillcolor:"rgba(29,78,216,0.07)",
  }], _L({
    margin:{l:52,r:8,t:8,b:36},
    yaxis:{...PLOT_LAYOUT.yaxis, type:"log", autorange:true, title:{text:"Growth (log)",font:{size:8}}},
  }), PLOT_CFG);
}

// ── Benchmark comparison 2×2 grid ─────────────────────────────────────────────
// Four subplots: (1) cumulative returns, (2) log scale, (3) vol-matched, (4) cumulative alpha
function drawBenchmarkComparison(divId, dates, equity, bmEquity) {
  if (!dates?.length || !equity?.length || !bmEquity?.length) return;
  const x = dates.map(d=>String(d).slice(0,10));

  // Compute returns series
  const stRets = equity.slice(1).map((v,i)=>v/(equity[i]||1)-1);
  const bmRets = bmEquity.slice(1).map((v,i)=>v/(bmEquity[i]||1)-1);
  const len = Math.min(stRets.length, bmRets.length);

  // Cumulative (normalized to start at 1)
  let cumSt=[1], cumBm=[1];
  for(let i=0;i<len;i++){
    cumSt.push(cumSt[cumSt.length-1]*(1+stRets[i]));
    cumBm.push(cumBm[cumBm.length-1]*(1+bmRets[i]));
  }

  // Vol-matched benchmark
  const stVol = Math.sqrt(stRets.reduce((s,v)=>{const m=stRets.reduce((a,b)=>a+b,0)/stRets.length; return s+(v-m)**2;},0)/stRets.length);
  const bmVol = Math.sqrt(bmRets.reduce((s,v)=>{const m=bmRets.reduce((a,b)=>a+b,0)/bmRets.length; return s+(v-m)**2;},0)/bmRets.length);
  const volAdj = bmVol>0?stVol/bmVol:1;
  let cumBmVol=[1];
  for(let i=0;i<len;i++) cumBmVol.push(cumBmVol[cumBmVol.length-1]*(1+bmRets[i]*volAdj));

  // Cumulative alpha
  const xShort = x.slice(0, len+1);
  const alpha = cumSt.map((v,i)=>+(v-cumBm[i]).toFixed(4));

  const base = {type:"scatter",mode:"lines",line:{width:1.5}};
  Plotly.newPlot(divId,[
    // Chart 1: cumulative (xaxis, yaxis)
    {...base,x:xShort,y:cumSt,name:"Strategy",line:{color:"#1d4ed8",width:2},xaxis:"x",yaxis:"y"},
    {...base,x:xShort,y:cumBm,name:"Benchmark",line:{color:"#f59e0b",width:1.5,dash:"dash"},xaxis:"x",yaxis:"y"},
    // Chart 2: log scale (xaxis2, yaxis2)
    {...base,x:xShort,y:cumSt,name:"Strategy",line:{color:"#1d4ed8",width:2},showlegend:false,xaxis:"x2",yaxis:"y2"},
    {...base,x:xShort,y:cumBm,name:"Benchmark",line:{color:"#f59e0b",width:1.5,dash:"dash"},showlegend:false,xaxis:"x2",yaxis:"y2"},
    // Chart 3: vol-matched (xaxis3, yaxis3)
    {...base,x:xShort,y:cumSt,name:"Strategy",line:{color:"#1d4ed8",width:2},showlegend:false,xaxis:"x3",yaxis:"y3"},
    {...base,x:xShort,y:cumBmVol,name:"BM Vol-Matched",line:{color:"#6d28d9",width:1.5,dash:"dot"},showlegend:false,xaxis:"x3",yaxis:"y3"},
    // Chart 4: cumulative alpha (xaxis4, yaxis4)
    {...base,x:xShort,y:alpha,name:"Cum. Alpha",line:{color:alpha[alpha.length-1]>=0?"#16a34a":"#dc2626",width:2},
     fill:"tozeroy",fillcolor:alpha[alpha.length-1]>=0?"rgba(22,163,74,0.12)":"rgba(220,38,38,0.12)",
     showlegend:false,xaxis:"x4",yaxis:"y4"},
  ], {
    paper_bgcolor:"#fff", plot_bgcolor:"#fafafa",
    margin:{l:44,r:8,t:30,b:36},
    font:{family:"Segoe UI,Arial",size:8,color:"#374151"},
    legend:{orientation:"h",x:0,y:1.08,font:{size:9}},
    grid:{rows:2,columns:2,pattern:"independent",ygap:0.12,xgap:0.1},
    annotations:[
      {text:"Cumulative Returns",xref:"x domain",yref:"y domain",x:0.5,y:1.04,showarrow:false,font:{size:8,color:"#374151"},xanchor:"center"},
      {text:"Log Scale",xref:"x2 domain",yref:"y2 domain",x:0.5,y:1.04,showarrow:false,font:{size:8,color:"#374151"},xanchor:"center"},
      {text:"Vol-Matched",xref:"x3 domain",yref:"y3 domain",x:0.5,y:1.04,showarrow:false,font:{size:8,color:"#374151"},xanchor:"center"},
      {text:"Cumulative Alpha",xref:"x4 domain",yref:"y4 domain",x:0.5,y:1.04,showarrow:false,font:{size:8,color:"#374151"},xanchor:"center"},
    ],
    xaxis:{type:"date",gridcolor:"#eee",tickfont:{size:7}},
    yaxis:{gridcolor:"#eee",tickfont:{size:7},autorange:true},
    xaxis2:{type:"date",gridcolor:"#eee",tickfont:{size:7}},
    yaxis2:{type:"log",gridcolor:"#eee",tickfont:{size:7},autorange:true},
    xaxis3:{type:"date",gridcolor:"#eee",tickfont:{size:7}},
    yaxis3:{gridcolor:"#eee",tickfont:{size:7},autorange:true},
    xaxis4:{type:"date",gridcolor:"#eee",tickfont:{size:7}},
    yaxis4:{gridcolor:"#eee",tickfont:{size:7},zeroline:true,zerolinecolor:"#374151",autorange:true},
    hovermode:"x unified",
  }, PLOT_CFG);
}

// ── Up/Down capture bar chart ──────────────────────────────────────────────────
function drawCapture(divId, equity, bmEquity, periods_per_year) {
  if (!equity?.length || !bmEquity?.length) return;
  const ppy = periods_per_year || 12;
  const stRets = equity.slice(1).map((v,i)=>v/(equity[i]||1)-1);
  const bmRets = bmEquity.slice(1).map((v,i)=>v/(bmEquity[i]||1)-1);
  const len = Math.min(stRets.length, bmRets.length);
  const upMask=[], dnMask=[];
  for(let i=0;i<len;i++){
    if(bmRets[i]>0) upMask.push(i);
    else if(bmRets[i]<0) dnMask.push(i);
  }
  function annualize(idxs, rets) {
    if(!idxs.length) return 0;
    const cum = idxs.reduce((acc,i)=>acc*(1+rets[i]),1);
    return cum**(ppy/idxs.length)-1;
  }
  const stUp=annualize(upMask,stRets), bmUp=annualize(upMask,bmRets);
  const stDn=annualize(dnMask,stRets), bmDn=annualize(dnMask,bmRets);
  const upCap = bmUp!==0?+(stUp/bmUp*100).toFixed(1):0;
  const dnCap = bmDn!==0?+(stDn/bmDn*100).toFixed(1):0;
  const capRatio = dnCap!==0?+(upCap/dnCap).toFixed(2):0;

  Plotly.newPlot(divId,[
    {x:["Up Capture","Down Capture"],y:[upCap,dnCap],type:"bar",
     marker:{color:[upCap>=100?"rgba(22,163,74,0.8)":"rgba(220,38,38,0.8)",
                    dnCap<=100?"rgba(22,163,74,0.8)":"rgba(220,38,38,0.8)"]},
     text:[upCap+"%",dnCap+"%"],textposition:"outside",showlegend:false},
    {x:[-0.5,1.5],y:[100,100],type:"scatter",mode:"lines",
     line:{color:"#374151",width:1.5,dash:"dash"},showlegend:false},
  ],_L({
    margin:{l:44,r:8,t:30,b:36},
    yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%",range:[0,Math.max(upCap,dnCap)*1.25+10]},
    title:{text:"Capture Ratios  (Ratio="+capRatio+")",font:{size:9,color:"#374151"},x:0.5,xanchor:"center"},
    showlegend:false,
  }), PLOT_CFG);
}

// ── Seasonality heatmap (avg return by month-of-year) ─────────────────────────
function drawSeasonality(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  // Build monthly returns keyed by month 1-12
  const byMonth = {};
  for(let i=1;i<equity.length;i++){
    const ret = equity[i]/equity[i-1]-1;
    const mo = parseInt(String(dates[i]).slice(5,7),10);
    if(!byMonth[mo]) byMonth[mo]=[];
    byMonth[mo].push(ret*100);
  }
  const MONTHS=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const avgs=MONTHS.map((_,idx)=>{
    const arr=byMonth[idx+1]||[];
    if(!arr.length) return null;
    return +(arr.reduce((a,b)=>a+b,0)/arr.length).toFixed(3);
  });
  const colors=avgs.map(v=>v==null?"#e5e7eb":v>=0?"rgba(22,163,74,0.7)":"rgba(220,38,38,0.7)");
  Plotly.newPlot(divId,[{
    x:MONTHS, y:avgs.map(v=>v||0),
    type:"bar", marker:{color:colors},
    text:avgs.map(v=>v==null?"n/a":(v>=0?"+":"")+v+"%"),
    textposition:"outside", showlegend:false,
  }],_L({
    margin:{l:44,r:8,t:8,b:36},
    yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%",autorange:true,
           zeroline:true,zerolinecolor:"#374151"},
    showlegend:false,
  }), PLOT_CFG);
}

// ── Q-Q plot vs normal ─────────────────────────────────────────────────────────
function drawQQ(divId, monthlyRets) {
  if (!monthlyRets?.length) return;
  const vals = monthlyRets.map(v=>v*100).sort((a,b)=>a-b);
  const n = vals.length;
  // Theoretical quantiles from normal distribution
  function normInvCDF(p) {
    // Rational approximation (Abramowitz and Stegun)
    if(p<=0) return -6; if(p>=1) return 6;
    const a=[0,-3.969683028665376e+01,2.209460984245205e+02,-2.759285104469687e+02,1.383577518672690e+02,-3.066479806614716e+01,2.506628277459239e+00];
    const b=[0,-5.447609879822406e+01,1.615858368580409e+02,-1.556989798598866e+02,6.680131188771972e+01,-1.328068155288572e+01];
    const c=[0,-7.784894002430293e-03,-3.223964580411365e-01,-2.400758277161838e+00,-2.549732539343734e+00,4.374664141464968e+00,2.938163982698783e+00];
    const d=[0,7.784695709041462e-03,3.224671290700398e-01,2.445134137142996e+00,3.754408661907416e+00];
    const pLow=0.02425, pHigh=1-pLow;
    let q,r2;
    if(p<pLow){q=Math.sqrt(-2*Math.log(p));return (((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6])/((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1);}
    if(p<=pHigh){q=p-0.5;r2=q*q;return (((((a[1]*r2+a[2])*r2+a[3])*r2+a[4])*r2+a[5])*r2+a[6])*q/(((((b[1]*r2+b[2])*r2+b[3])*r2+b[4])*r2+b[5])*r2+1);}
    q=Math.sqrt(-2*Math.log(1-p));return -(((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6])/((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1);
  }
  const theoretical = vals.map((_,i)=>+(normInvCDF((i+0.5)/n)*vals.reduce((s,v)=>{return s+(v-vals.reduce((a,b)=>a+b,0)/n)**2;},0)/n>0?Math.sqrt(vals.reduce((s,v)=>{return s+(v-vals.reduce((a,b)=>a+b,0)/n)**2;},0)/n):1).toFixed(3));
  // Simpler: just use z-scores of sorted ranks
  const mu = vals.reduce((a,b)=>a+b,0)/n;
  const sd = Math.sqrt(vals.reduce((s,v)=>s+(v-mu)**2,0)/n);
  const theor = vals.map((_,i)=>+normInvCDF((i+0.375)/(n+0.25)).toFixed(3));
  // Reference line: from 1st to 3rd quartile
  const q1t=normInvCDF(0.25), q3t=normInvCDF(0.75);
  const q1s=vals[Math.floor(n*0.25)], q3s=vals[Math.floor(n*0.75)];
  const slope=(q3s-q1s)/(q3t-q1t||1), intercept=q1s-slope*q1t;
  const xLine=[theor[0],theor[theor.length-1]];
  const yLine=xLine.map(x=>+(slope*x+intercept).toFixed(3));

  Plotly.newPlot(divId,[
    {x:theor,y:vals,type:"scatter",mode:"markers",name:"Sample",
     marker:{color:"#1d4ed8",size:4,opacity:0.7},showlegend:false},
    {x:xLine,y:yLine,type:"scatter",mode:"lines",name:"Normal ref",
     line:{color:"#dc2626",width:1.5,dash:"dash"},showlegend:true},
  ],_L({
    margin:{l:48,r:8,t:8,b:40},
    xaxis:{...PLOT_LAYOUT.xaxis,title:{text:"Theoretical Quantiles",font:{size:8}}},
    yaxis:{...PLOT_LAYOUT.yaxis,autorange:true,title:{text:"Sample Quantiles",font:{size:8}}},
  }),PLOT_CFG);
}

// ── Monthly scatter vs benchmark ──────────────────────────────────────────────
function drawReturnScatter(divId, equity, bmEquity) {
  if (!equity?.length || !bmEquity?.length) return;
  const stRets = equity.slice(1).map((v,i)=>+(v/(equity[i]||1)-1)*100).slice(0,bmEquity.length-1);
  const bmRets = bmEquity.slice(1).map((v,i)=>+(v/(bmEquity[i]||1)-1)*100).slice(0,equity.length-1);
  const len=Math.min(stRets.length,bmRets.length);
  const xs=bmRets.slice(0,len), ys=stRets.slice(0,len);

  // OLS regression line
  const n=len, mx=xs.reduce((a,b)=>a+b,0)/n, my=ys.reduce((a,b)=>a+b,0)/n;
  const cov=xs.reduce((s,v,i)=>s+(v-mx)*(ys[i]-my),0)/n;
  const varX=xs.reduce((s,v)=>s+(v-mx)**2,0)/n;
  const slope=varX>0?+(cov/varX).toFixed(3):1, intercept=+(my-slope*mx).toFixed(3);
  const xMin=Math.min(...xs)-1, xMax=Math.max(...xs)+1;

  Plotly.newPlot(divId,[
    {x:xs,y:ys,type:"scatter",mode:"markers",name:"Monthly returns",
     marker:{color:"#1d4ed8",size:5,opacity:0.65},showlegend:false},
    {x:[xMin,xMax],y:[slope*xMin+intercept,slope*xMax+intercept],type:"scatter",mode:"lines",
     name:"OLS (β="+slope+")",line:{color:"#dc2626",width:1.5},showlegend:true},
    {x:[xMin,xMax],y:[xMin,xMax],type:"scatter",mode:"lines",
     name:"y=x",line:{color:"#9ca3af",width:1,dash:"dot"},showlegend:true},
  ],_L({
    margin:{l:48,r:8,t:8,b:44},
    xaxis:{...PLOT_LAYOUT.xaxis,ticksuffix:"%",title:{text:"Benchmark Return %",font:{size:8}}},
    yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%",title:{text:"Strategy Return %",font:{size:8}},autorange:true},
  }),PLOT_CFG);
}

// ── Drawdown duration histogram ───────────────────────────────────────────────
function drawDDDurationHist(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  // Compute drawdown period durations
  const durations=[];
  let inDD=false, startIdx=0;
  for(let i=0;i<equity.length;i++){
    let peak=equity[0];
    for(let j=0;j<i;j++) if(equity[j]>peak) peak=equity[j];
    const dd=(equity[i]/peak-1)*100;
    if(!inDD && dd<0){inDD=true;startIdx=i;}
    else if(inDD && dd>=-0.01){durations.push(i-startIdx);inDD=false;}
  }
  if(!durations.length) return;
  Plotly.newPlot(divId,[{
    x:durations,type:"histogram",name:"DD Duration",
    marker:{color:"rgba(220,38,38,0.6)",line:{color:"#dc2626",width:0.5}},
    nbinsx:Math.min(30,Math.ceil(durations.length/2)),
  }],_L({
    margin:{l:44,r:8,t:8,b:36},
    xaxis:{...PLOT_LAYOUT.xaxis,title:{text:"Duration (periods)",font:{size:8}}},
    yaxis:{...PLOT_LAYOUT.yaxis,title:{text:"Count",font:{size:8}}},
    showlegend:false,
  }),PLOT_CFG);
}

// ── Trailing returns bar chart ────────────────────────────────────────────────
function drawTrailingReturns(divId, metrics) {
  const periods=["1M","3M","6M","1Y","3Y*","5Y*","10Y*"];
  const keys=["trailing_1m","trailing_3m","trailing_6m","trailing_1y","trailing_3y","trailing_5y","trailing_10y"];
  const vals=keys.map(k=>metrics[k]!=null?+(metrics[k]*100).toFixed(2):null);
  const validX=[], validY=[];
  periods.forEach((p,i)=>{if(vals[i]!=null){validX.push(p);validY.push(vals[i]);}});
  if(!validX.length) return;
  Plotly.newPlot(divId,[{
    x:validX, y:validY, type:"bar",
    marker:{color:validY.map(v=>v>=0?"rgba(22,163,74,0.75)":"rgba(220,38,38,0.75)")},
    text:validY.map(v=>(v>=0?"+":"")+v+"%"),
    textposition:"outside", showlegend:false,
  }],_L({
    margin:{l:44,r:8,t:8,b:36},
    yaxis:{...PLOT_LAYOUT.yaxis,ticksuffix:"%",autorange:true,zeroline:true,zerolinecolor:"#374151"},
    showlegend:false,
  }),PLOT_CFG);
}

// ── Intra-month max DD heatmap ────────────────────────────────────────────────
function drawIntraMonthDDHeatmap(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  // Group by year-month, compute worst intra-period DD
  const byYearMonth={};
  for(let i=1;i<equity.length;i++){
    const ym=String(dates[i]).slice(0,7);
    if(!byYearMonth[ym]) byYearMonth[ym]=[];
    byYearMonth[ym].push(equity[i]);
  }
  // Build pivot: years × months
  const years=[...new Set(Object.keys(byYearMonth).map(k=>k.slice(0,4)))].sort();
  const mos=["01","02","03","04","05","06","07","08","09","10","11","12"];
  const MNAMES=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const zData=years.map(yr=>mos.map(mo=>{
    const prices=byYearMonth[yr+"-"+mo];
    if(!prices||prices.length<2) return null;
    let pk=prices[0],mdd=0;
    prices.forEach(p=>{pk=Math.max(pk,p);mdd=Math.min(mdd,(p/pk-1)*100);});
    return +mdd.toFixed(2);
  }));
  const textData=zData.map(row=>row.map(v=>v==null?"":v.toFixed(1)+"%"));
  Plotly.newPlot(divId,[{
    z:zData, x:MNAMES, y:years,
    type:"heatmap",
    colorscale:[[0,"#b91c1c"],[0.5,"#fff7ed"],[1,"#f0fdf4"]],
    zmid:-5, zmax:0, zmin:-30,
    text:textData, texttemplate:"%{text}",
    textfont:{size:7}, showscale:false,
    hoverongaps:false,
    colorbar:{title:"%",tickfont:{size:7}},
  }],{
    paper_bgcolor:"#fff",plot_bgcolor:"#fff",
    margin:{l:36,r:8,t:8,b:36},
    font:{family:"Segoe UI,Arial",size:8,color:"#374151"},
    xaxis:{tickfont:{size:7}},
    yaxis:{tickfont:{size:7},autorange:"reversed"},
  },PLOT_CFG);
}

// ── Annual performance table: Strategy vs Benchmark per year ──────────────────
function buildAnnualVsBenchmarkTable(dates, equity, bmEquity) {
  if (!dates?.length || !equity?.length) return document.createElement("div");
  // Build annual returns by year
  const byYear={}, bmByYear={};
  for(let i=1;i<equity.length;i++){
    const yr=String(dates[i]).slice(0,4);
    const stRet=equity[i]/(equity[i-1]||1)-1;
    if(!byYear[yr]) byYear[yr]=[];
    byYear[yr].push(stRet);
    if(bmEquity&&bmEquity[i]){
      const bmRet=bmEquity[i]/(bmEquity[i-1]||1)-1;
      if(!bmByYear[yr]) bmByYear[yr]=[];
      bmByYear[yr].push(bmRet);
    }
  }
  const years=Object.keys(byYear).sort();
  const compound=arr=>arr.reduce((a,v)=>a*(1+v),1)-1;
  const wrap=document.createElement("div"); wrap.style.overflowX="auto";
  let html=`<table style="width:100%;border-collapse:collapse;font-size:9px;font-family:'Segoe UI',sans-serif">
    <thead><tr>
      <th style="padding:3px 6px;background:#f1f5f9;color:#374151;font-size:8px;border-bottom:2px solid #e5e7eb;text-align:left">YEAR</th>
      <th style="padding:3px 6px;background:#f1f5f9;color:#374151;font-size:8px;border-bottom:2px solid #e5e7eb;text-align:right">STRATEGY</th>
      <th style="padding:3px 6px;background:#f1f5f9;color:#374151;font-size:8px;border-bottom:2px solid #e5e7eb;text-align:right">BENCHMARK</th>
      <th style="padding:3px 6px;background:#f1f5f9;color:#374151;font-size:8px;border-bottom:2px solid #e5e7eb;text-align:right">EXCESS</th>
    </tr></thead><tbody>`;
  years.forEach((yr,i)=>{
    const st=compound(byYear[yr])*100;
    const bm=bmByYear[yr]?compound(bmByYear[yr])*100:null;
    const ex=bm!=null?st-bm:null;
    const stC=st>=0?"#15803d":"#b91c1c";
    const bmC=bm!=null?(bm>=0?"#15803d":"#b91c1c"):"#9ca3af";
    const exC=ex!=null?(ex>=0?"#15803d":"#b91c1c"):"#9ca3af";
    html+=`<tr style="background:${i%2?"#f9fafb":"#fff"}">
      <td style="padding:2px 6px;font-weight:600;color:#374151">${yr}</td>
      <td style="padding:2px 6px;text-align:right;font-weight:700;color:${stC}">${st>=0?"+":""}${st.toFixed(1)}%</td>
      <td style="padding:2px 6px;text-align:right;color:${bmC}">${bm!=null?(bm>=0?"+":"")+bm.toFixed(1)+"%":"—"}</td>
      <td style="padding:2px 6px;text-align:right;font-weight:600;color:${exC}">${ex!=null?(ex>=0?"+":"")+ex.toFixed(1)+"%":"—"}</td>
    </tr>`;
  });
  html+="</tbody></table>";
  wrap.innerHTML=html; return wrap;
}
