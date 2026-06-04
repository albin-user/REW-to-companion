"""SPL dashboard page served at GET / by the bridge.

The HTML/CSS/JS is embedded as a string (no external files, no CDN) so it is
frozen-safe under PyInstaller and works on an isolated venue LAN. It fetches
/api/spl for live values + bridge-tracked max, /api/config for the editable
thresholds, and POSTs to /api/config (persisted) and /api/control (reset max).
"""

DASHBOARD_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>REW SPL</title>
<style>
  :root{
    --bg:#0a0a0c; --panel:#141417; --panel-edge:#26262b; --muted:#8a8a93;
    --green:#16c060; --orange:#ffae00; --red:#ff3b30; --neutral:#f2f2f5;
  }
  *{box-sizing:border-box; margin:0; padding:0;}
  html,body{height:100%;}
  body{
    background:var(--bg); color:#fff; min-height:100%;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    display:flex; flex-direction:column;
  }
  .bar{display:flex; align-items:center; gap:14px; flex-wrap:wrap; padding:10px 16px; background:#000; border-bottom:1px solid var(--panel-edge);}
  .bar .title{font-weight:700; letter-spacing:.5px;}
  .bar .spacer{flex:1;}
  .pill{display:inline-flex; align-items:center; gap:7px; font-size:.85rem; color:var(--muted);}
  .dot{width:11px; height:11px; border-radius:50%; background:var(--muted);}
  .dot.live{background:var(--green);} .dot.warn{background:var(--orange);} .dot.off{background:var(--red);}
  .btn{background:#1d1d22; color:#fff; border:1px solid var(--panel-edge); border-radius:8px; padding:7px 12px; font-size:.85rem; cursor:pointer;}
  .btn:hover{background:#26262d;}
  .grid{flex:1; display:grid; gap:10px; padding:10px; grid-template-columns:repeat(auto-fit, minmax(300px, 1fr)); grid-auto-rows:1fr;}
  .panel{background:var(--panel); border:1px solid var(--panel-edge); border-radius:14px; display:flex; flex-direction:column; padding:14px 18px; min-height:200px; position:relative; overflow:hidden;}
  .panel .label{font-size:clamp(1rem,2.4vw,1.5rem); color:#d8d8de; font-weight:600;}
  .panel .value{flex:1; display:flex; align-items:center; justify-content:flex-start; font-weight:800; line-height:1; letter-spacing:-2px; font-size:clamp(3.2rem, 16vmin, 9rem); font-variant-numeric:tabular-nums; color:var(--neutral); transition:color .15s ease;}
  .panel .barline{height:5px; border-radius:3px; background:#2a2a30; margin:6px 0 10px;}
  .panel .foot{display:flex; align-items:center; justify-content:space-between; gap:10px;}
  .panel .max{color:var(--muted); font-size:clamp(.95rem,2.2vw,1.25rem);}
  .panel .units{color:#9a9aa2; font-size:clamp(.8rem,1.8vw,1.05rem); text-align:right;}
  .panel .statusdot{position:absolute; top:16px; right:18px; width:18px; height:18px; border-radius:50%; background:var(--muted);}
  .green  .value{color:var(--green);}  .green  .barline{background:var(--green);}  .green  .statusdot{background:var(--green);}
  .orange .value{color:var(--orange);} .orange .barline{background:var(--orange);} .orange .statusdot{background:var(--orange);}
  .red    .value{color:var(--red);}    .red    .barline{background:var(--red);}    .red    .statusdot{background:var(--red); animation:pulse 1s infinite;}
  .neutral .value{color:var(--neutral);} .neutral .barline{background:#3a3a42;} .neutral .statusdot{background:#3a3a42;}
  .stale .value{color:#6a6a72 !important;} .stale .barline{background:#2a2a30 !important;}
  @keyframes pulse{0%,100%{opacity:1;}50%{opacity:.35;}}
  .overlay{position:fixed; inset:0; background:rgba(0,0,0,.7); display:none; align-items:center; justify-content:center; padding:16px;}
  .overlay.open{display:flex;}
  .modal{background:#161619; border:1px solid var(--panel-edge); border-radius:14px; padding:20px; width:min(520px,100%); max-height:90vh; overflow:auto;}
  .modal h2{font-size:1.1rem; margin-bottom:6px;}
  .row{display:grid; grid-template-columns:1fr 90px 90px; gap:10px; align-items:center; margin-bottom:10px;}
  .row .name{color:#d8d8de;}
  .row input{width:100%; background:#0e0e11; border:1px solid var(--panel-edge); color:#fff; border-radius:8px; padding:8px; font-size:1rem; text-align:center;}
  .row .hdr{color:var(--muted); font-size:.78rem; text-align:center;}
  .modal .actions{display:flex; gap:10px; justify-content:flex-end; margin-top:16px;}
  .btn.save{background:#1f6f3f; border-color:#28814b;}
  .hint{color:var(--muted); font-size:.8rem; margin:4px 0 14px;}
</style>
</head>
<body>
  <div class="bar">
    <span class="title">REW&nbsp;SPL</span>
    <span class="pill"><span id="connDot" class="dot"></span><span id="connText">connecting…</span></span>
    <span class="pill" id="elapsed">elapsed&nbsp;--:--</span>
    <span class="spacer"></span>
    <button class="btn" onclick="resetMax()">Reset Max</button>
    <button class="btn" onclick="openSettings()">&#9881; Limits</button>
  </div>
  <div class="grid" id="grid"></div>
  <div class="overlay" id="overlay">
    <div class="modal">
      <h2>Threshold limits (dB)</h2>
      <div class="hint">Green below orange &middot; orange up to red &middot; red and above. Leave blank for no colour. Saved on the bridge (survives reboot).</div>
      <div class="row"><span class="hdr"></span><span class="hdr">Orange &ge;</span><span class="hdr">Red &ge;</span></div>
      <div id="settingsRows"></div>
      <div class="actions">
        <button class="btn" onclick="closeSettings()">Cancel</button>
        <button class="btn save" onclick="saveSettings()">Save</button>
      </div>
    </div>
  </div>
<script>
const PANELS = [
  {key:"spl_a_slow", label:"SPL A Slow", units:"dB SPL A Slow"},
  {key:"leq_2min",   label:"2-min Leq",  units:"dB LAeq 2 min"},
  {key:"leq_15min",  label:"15-min Leq", units:"dB LAeq 15 min"},
];
let thresholds = {};

const grid = document.getElementById("grid");
for(const p of PANELS){
  grid.insertAdjacentHTML("beforeend",
   `<div class="panel neutral" id="p_${p.key}">
      <div class="label">${p.label}</div>
      <div class="statusdot"></div>
      <div class="value" id="v_${p.key}">--</div>
      <div class="barline"></div>
      <div class="foot"><span class="max" id="m_${p.key}">Max: --</span><span class="units">${p.units}</span></div>
    </div>`);
}

function colorFor(key, val){
  const t = thresholds[key];
  if(t==null || val==null) return "neutral";
  if(t.red!=null && val>=t.red) return "red";
  if(t.orange!=null && val>=t.orange) return "orange";
  return "green";
}
function setConn(state, text){
  document.getElementById("connDot").className = "dot " + state;
  document.getElementById("connText").textContent = text;
}
function fmt(v){ return (v==null) ? "--" : v.toFixed(1); }

function render(d){
  const active = d.measurement_active === true;
  for(const p of PANELS){
    const el = document.getElementById("p_"+p.key);
    const vEl = document.getElementById("v_"+p.key);
    const v = d[p.key];
    const mx = d["max_"+p.key];
    document.getElementById("m_"+p.key).textContent = "Max: " + fmt(mx);
    if(v==null){
      vEl.textContent = (p.key==="leq_2min" && active) ? "···" : "--";
      el.className = "panel neutral";
      continue;
    }
    vEl.textContent = v.toFixed(1);
    let cls = colorFor(p.key, v);
    if(!active) cls = "neutral stale";
    el.className = "panel " + cls;
  }
  if(active){ setConn("live", "LIVE"); }
  else if(d.rew_running){ setConn("warn", "no signal"); }
  else { setConn("warn", "REW connecting…"); }
  const s = Math.floor(d.elapsed_time || 0);
  document.getElementById("elapsed").textContent =
    "elapsed " + String(Math.floor(s/60)).padStart(2,"0") + ":" + String(s%60).padStart(2,"0");
}
function setOffline(){
  setConn("off", "OFFLINE");
  for(const p of PANELS){
    document.getElementById("p_"+p.key).className = "panel neutral stale";
    document.getElementById("v_"+p.key).textContent = "OFFLINE";
    document.getElementById("m_"+p.key).textContent = "Max: --";
  }
}
async function poll(){
  try{
    const r = await fetch("/api/spl", {cache:"no-store"});
    if(!r.ok) throw new Error("status "+r.status);
    render(await r.json());
  }catch(e){ setOffline(); }
}
async function loadConfig(){
  try{ const r = await fetch("/api/config"); thresholds = (await r.json()).thresholds || {}; }catch(e){}
}
async function resetMax(){
  try{ await fetch("/api/control", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({action:"reset_max"})}); }catch(e){}
}
function openSettings(){
  const box = document.getElementById("settingsRows"); box.innerHTML = "";
  for(const p of PANELS){
    const t = thresholds[p.key] || {};
    box.insertAdjacentHTML("beforeend",
     `<div class="row"><span class="name">${p.label}</span>
        <input id="o_${p.key}" type="number" step="0.1" placeholder="—" value="${t.orange??""}">
        <input id="r_${p.key}" type="number" step="0.1" placeholder="—" value="${t.red??""}"></div>`);
  }
  document.getElementById("overlay").classList.add("open");
}
function closeSettings(){ document.getElementById("overlay").classList.remove("open"); }
async function saveSettings(){
  const body = {thresholds:{}};
  for(const p of PANELS){
    const o = document.getElementById("o_"+p.key).value;
    const r = document.getElementById("r_"+p.key).value;
    body.thresholds[p.key] = (o===""&&r==="") ? null : {orange:o===""?null:+o, red:r===""?null:+r};
  }
  try{
    const resp = await fetch("/api/config", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
    if(!resp.ok){ const e = await resp.json().catch(()=>({})); alert("Save failed: " + (e.detail || resp.status)); return; }
    thresholds = (await resp.json()).thresholds || thresholds;
    closeSettings();
  }catch(e){ alert("Save failed: " + e); }
}

loadConfig();
poll();
setInterval(poll, 300);
</script>
</body>
</html>'''
