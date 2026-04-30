// tearsheet.js — Plotly-based charts for OBQ FactorLab

const PLOT_LAYOUT = {
  paper_bgcolor: "#ffffff",
  plot_bgcolor:  "#ffffff",
  font:  { family: "Segoe UI, Arial, sans-serif", size: 10, color: "#374151" },
  margin: { l: 48, r: 12, t: 10, b: 36 },
  legend: { orientation: "h", x: 0, y: 1.14, font: { size: 9 }, bgcolor: "transparent" },
  xaxis: { gridcolor: "#f3f4f6", linecolor: "#e5e7eb", tickfont: { size: 8 }, tickcolor: "#9ca3af" },
  yaxis: { gridcolor: "#f3f4f6", linecolor: "#e5e7eb", tickfont: { size: 8 }, tickcolor: "#9ca3af", zeroline: true, zerolinecolor: "#e5e7eb" },
  hovermode: "x unified",
};
const PLOT_CFG = { displayModeBar: false, responsive: true };

function _L(extra) { return Object.assign({}, PLOT_LAYOUT, extra || {}); }

function drawEquity(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  Plotly.newPlot(divId, [{
    x: dates.map(d => String(d).slice(0,7)), y: equity,
    type:"scatter", mode:"lines", name:"Portfolio",
    line:{ color:"#0066cc", width:1.5 },
    fill:"tozeroy", fillcolor:"rgba(0,102,204,0.07)",
  }], _L({ yaxis:{...PLOT_LAYOUT.yaxis, tickformat:".2f"}, margin:{l:52,r:12,t:8,b:36} }), PLOT_CFG);
}

function drawDrawdown(divId, dates, equity) {
  if (!dates?.length || !equity?.length) return;
  let peak = equity[0];
  const dd = equity.map(v => { peak=Math.max(peak,v); return (v/peak-1)*100; });
  Plotly.newPlot(divId, [{
    x: dates.map(d=>String(d).slice(0,7)), y: dd,
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
  const x=dates.slice(1).map(d=>String(d).slice(0,7));
  Plotly.newPlot(divId, [
    { x, y:rs, type:"scatter", mode:"lines", name:"Rolling Sharpe", line:{color:"#0066cc",width:1.5}, fill:"tozeroy", fillcolor:"rgba(0,102,204,0.07)" },
    { x:[x[0],x[x.length-1]], y:[0,0], type:"scatter", mode:"lines", line:{color:"#9ca3af",width:1,dash:"dot"}, showlegend:false },
  ], _L({ yaxis:{...PLOT_LAYOUT.yaxis}, margin:{l:44,r:8,t:6,b:36} }), PLOT_CFG);
}

function drawIC(divId, ic_data) {
  if (!ic_data?.length) return;
  const x=ic_data.map(d=>String(d.date||"").slice(0,7));
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
