from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from .api.routes import router
from .db import init_db

app = FastAPI(title="Qanas Data Engine", version="0.3.0")


@app.on_event("startup")
def startup():
    init_db()


app.include_router(router, prefix="/api")


DASHBOARD_HTML = r'''<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>قنص | Reverse Split Intelligence</title>
  <meta name="theme-color" content="#071019" />
  <style>
    :root{--bg:#071019;--panel:#0b1722;--panel2:#0e1d2a;--line:#193041;--text:#edf5f8;--muted:#8296a5;--gold:#d7ad59;--gold2:#f0d18b;--green:#4bd39a;--red:#ff6b78;--cyan:#5cc8ff;--shadow:0 20px 70px rgba(0,0,0,.35)}
    *{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(circle at 85% -10%,rgba(215,173,89,.14),transparent 28%),radial-gradient(circle at 10% 0,rgba(92,200,255,.07),transparent 25%),var(--bg);color:var(--text);font-family:"Segoe UI",Tahoma,Arial,sans-serif;min-height:100vh}
    body:before{content:"";position:fixed;inset:0;pointer-events:none;background-image:linear-gradient(rgba(255,255,255,.014) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.014) 1px,transparent 1px);background-size:32px 32px;mask-image:linear-gradient(to bottom,black,transparent 78%)}
    .wrap{max-width:1500px;margin:auto;padding:24px}.top{display:flex;align-items:center;justify-content:space-between;gap:18px;margin-bottom:22px}.brand{display:flex;align-items:center;gap:14px}.mark{width:54px;height:54px;border:1px solid rgba(215,173,89,.35);background:linear-gradient(145deg,#132536,#09131d);border-radius:17px;display:grid;place-items:center;box-shadow:inset 0 0 25px rgba(215,173,89,.08),var(--shadow);font-weight:900;font-size:25px;color:var(--gold2)}
    .brand h1{margin:0;font-size:30px;letter-spacing:.2px}.brand small{color:var(--muted);display:block;margin-top:4px}.actions{display:flex;gap:10px;flex-wrap:wrap}.btn{border:1px solid var(--line);background:#0d1a26;color:var(--text);padding:11px 15px;border-radius:12px;cursor:pointer;font-weight:700;transition:.2s}.btn:hover{transform:translateY(-1px);border-color:#365168}.btn.gold{background:linear-gradient(135deg,#b88b3a,#e7c272);color:#10151a;border:0}.btn:disabled{opacity:.5;cursor:wait;transform:none}
    .hero{display:grid;grid-template-columns:1.4fr .6fr;gap:16px;margin-bottom:16px}.hero-main,.syncbox{border:1px solid var(--line);background:linear-gradient(145deg,rgba(14,29,42,.96),rgba(8,18,27,.95));border-radius:22px;box-shadow:var(--shadow)}.hero-main{padding:27px;position:relative;overflow:hidden}.hero-main:after{content:"";position:absolute;width:220px;height:220px;border:1px solid rgba(215,173,89,.13);border-radius:50%;left:-65px;top:-80px;box-shadow:0 0 70px rgba(215,173,89,.06)}
    .eyebrow{color:var(--gold2);font-size:12px;font-weight:900;letter-spacing:2px}.hero h2{font-size:32px;margin:8px 0 8px}.hero p{margin:0;color:var(--muted);line-height:1.8;max-width:780px}.syncbox{padding:22px;display:flex;flex-direction:column;justify-content:center}.syncrow{display:flex;justify-content:space-between;align-items:center;margin:6px 0;color:var(--muted)}.syncrow strong{color:var(--text)}.dot{width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 15px var(--green);display:inline-block;margin-left:7px}
    .stats{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:16px}.stat{background:rgba(11,23,34,.88);border:1px solid var(--line);border-radius:17px;padding:17px}.stat span{color:var(--muted);font-size:12px}.stat b{font-size:25px;display:block;margin-top:8px}.stat em{font-size:11px;color:#5f7788;font-style:normal}
    .toolbar{display:flex;gap:10px;align-items:center;justify-content:space-between;background:rgba(11,23,34,.8);border:1px solid var(--line);padding:12px;border-radius:16px 16px 0 0}.search{flex:1;max-width:420px;position:relative}.search input{width:100%;background:#08141e;border:1px solid #1e3546;color:var(--text);border-radius:11px;padding:11px 14px;outline:none}.search input:focus{border-color:#42637a}.chips{display:flex;gap:7px;flex-wrap:wrap}.chip{font-size:12px;padding:8px 10px;border:1px solid var(--line);background:#0a1823;color:var(--muted);border-radius:999px;cursor:pointer}.chip.active{color:#151515;background:var(--gold2);border-color:var(--gold2);font-weight:800}
    .tablebox{border:1px solid var(--line);border-top:0;border-radius:0 0 18px 18px;overflow:auto;background:rgba(7,16,25,.82)}table{width:100%;border-collapse:collapse;min-width:1220px}th{position:sticky;top:0;background:#0b1924;color:#6f8798;font-size:11px;text-transform:uppercase;letter-spacing:.4px;text-align:right;padding:13px 12px;border-bottom:1px solid var(--line);white-space:nowrap}td{padding:13px 12px;border-bottom:1px solid rgba(25,48,65,.62);font-size:13px;white-space:nowrap}tbody tr{cursor:pointer;transition:.15s}tbody tr:hover{background:#0d1d29}.ticker{font-size:16px;font-weight:900;color:#fff}.company{font-size:11px;color:#61798b;margin-top:3px;max-width:170px;overflow:hidden;text-overflow:ellipsis}.score{display:inline-grid;place-items:center;width:43px;height:43px;border-radius:12px;font-weight:900;border:1px solid #274154;background:#0b1b27}.score.hot{color:#101812;background:linear-gradient(135deg,#79e7b3,#33bf80);border:0}.score.mid{color:#1e1607;background:linear-gradient(135deg,#f3d183,#c99b42);border:0}.good{color:var(--green);font-weight:800}.bad{color:var(--red);font-weight:800}.goldtext{color:var(--gold2);font-weight:800}.muted{color:var(--muted)}.pill{border:1px solid #244052;padding:5px 8px;border-radius:8px;font-size:11px}.empty{text-align:center;padding:55px;color:var(--muted)}
    .drawer{position:fixed;inset:0;background:rgba(0,0,0,.58);backdrop-filter:blur(5px);display:none;z-index:50}.drawer.open{display:block}.sheet{position:absolute;left:0;top:0;height:100%;width:min(540px,94vw);background:#09151f;border-right:1px solid #22394a;padding:24px;overflow:auto;box-shadow:40px 0 100px rgba(0,0,0,.5)}.close{float:left;background:transparent;color:#8ca0ad;border:0;font-size:26px;cursor:pointer}.sheet h2{font-size:33px;margin:20px 0 4px}.sheet .sub{color:var(--muted);margin-bottom:22px}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}.mini{border:1px solid var(--line);background:#0c1a26;border-radius:14px;padding:14px}.mini span{display:block;color:var(--muted);font-size:11px;margin-bottom:7px}.mini b{font-size:18px}.history{margin-top:18px}.history h3{font-size:14px}.histrow{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;border-bottom:1px solid var(--line);padding:10px 0;font-size:12px}.toast{position:fixed;bottom:24px;right:24px;background:#102330;border:1px solid #2c4658;padding:13px 16px;border-radius:12px;box-shadow:var(--shadow);z-index:80;display:none}.toast.show{display:block}
    @media(max-width:980px){.hero{grid-template-columns:1fr}.stats{grid-template-columns:repeat(2,1fr)}.top{align-items:flex-start}.actions{justify-content:flex-end}.hero h2{font-size:26px}}@media(max-width:620px){.wrap{padding:14px}.top{display:block}.actions{margin-top:14px;justify-content:stretch}.actions .btn{flex:1}.brand h1{font-size:25px}.stats{grid-template-columns:1fr 1fr}.toolbar{align-items:stretch;flex-direction:column}.search{max-width:none}.hero-main{padding:20px}.stat b{font-size:21px}}
  </style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div class="brand"><div class="mark">ق</div><div><h1>قنص</h1><small>Reverse Split Intelligence</small></div></div>
    <div class="actions"><button class="btn" onclick="loadData()">تحديث العرض</button><button id="borrowBtn" class="btn gold" onclick="startBorrowSync()">تحديث Available + CTB</button></div>
  </div>

  <section class="hero">
    <div class="hero-main"><div class="eyebrow">QANAS / LIVE BOARD</div><h2>القنص بالركادة.</h2><p>لوحة واحدة تراقب أسهم التقسيم العكسي من يوم التقسيم: القمة، القاع، المسافة من القاع، الاستقرار، مستوى النصف، Available وCTB — مرتبة حسب الجاهزية.</p></div>
    <div class="syncbox"><div class="syncrow"><span><i class="dot"></i> حالة البيانات</span><strong id="syncState">متصل</strong></div><div class="syncrow"><span>آخر Borrow</span><strong id="lastBorrow">—</strong></div><div class="syncrow"><span>المصدر</span><strong>IBKR / ChartExchange</strong></div><div class="syncrow"><span>Rebate</span><strong class="muted">بانتظار مصدر موثوق</strong></div></div>
  </section>

  <section class="stats">
    <div class="stat"><span>أسهم فعالة</span><b id="sTotal">—</b><em>بعد بدء التداول</em></div>
    <div class="stat"><span>Available ≤ 10K</span><b id="sScarce">—</b><em>قلة المعروض</em></div>
    <div class="stat"><span>وصلت مستوى النصف</span><b id="sHalf">—</b><em>إشارة مكتملة</em></div>
    <div class="stat"><span>قريبة من القاع ≤ 10%</span><b id="sNear">—</b><em>مسافة السعر</em></div>
    <div class="stat"><span>جاهزية 70+</span><b id="sReady">—</b><em>الوزن الحالي تجريبي</em></div>
  </section>

  <section>
    <div class="toolbar"><div class="search"><input id="q" placeholder="ابحث بالرمز أو اسم الشركة..." oninput="render()"></div><div class="chips"><button class="chip active" data-f="all" onclick="setFilter('all',this)">الكل</button><button class="chip" data-f="scarce" onclick="setFilter('scarce',this)">Available منخفض</button><button class="chip" data-f="half" onclick="setFilter('half',this)">وصل النصف</button><button class="chip" data-f="near" onclick="setFilter('near',this)">قريب من القاع</button></div></div>
    <div class="tablebox"><table><thead><tr><th>السهم</th><th>الجاهزية</th><th>Available</th><th>CTB</th><th>السعر</th><th>القاع</th><th>عن القاع</th><th>القمة</th><th>النصف</th><th>الاستقرار</th><th>4H Max</th><th>التقسيم</th></tr></thead><tbody id="rows"><tr><td colspan="12" class="empty">جاري تحميل البيانات...</td></tr></tbody></table></div>
  </section>
</div>

<div id="drawer" class="drawer" onclick="if(event.target===this)closeDrawer()"><div class="sheet"><button class="close" onclick="closeDrawer()">×</button><div id="detail"></div></div></div>
<div id="toast" class="toast"></div>
<script>
let data=[], filter='all';
const fmt=n=>n==null?'—':Number(n).toLocaleString('en-US',{maximumFractionDigits:2});
const money=n=>n==null?'—':'$'+Number(n).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4});
const pct=n=>n==null?'—':Number(n).toFixed(2)+'%';
function toast(t){const e=document.getElementById('toast');e.textContent=t;e.classList.add('show');setTimeout(()=>e.classList.remove('show'),3000)}
function setFilter(f,el){filter=f;document.querySelectorAll('.chip').forEach(x=>x.classList.remove('active'));el.classList.add('active');render()}
async function loadData(){try{document.getElementById('syncState').textContent='يتم التحديث...';const r=await fetch('/api/hunt');data=await r.json();document.getElementById('syncState').textContent='متصل';summaries();render()}catch(e){document.getElementById('syncState').textContent='خطأ اتصال';toast('تعذر تحميل البيانات')}}
function summaries(){document.getElementById('sTotal').textContent=data.length;document.getElementById('sScarce').textContent=data.filter(x=>x.available!=null&&x.available<=10000).length;document.getElementById('sHalf').textContent=data.filter(x=>x.half_level_reached).length;document.getElementById('sNear').textContent=data.filter(x=>x.distance_from_low_pct!=null&&x.distance_from_low_pct<=10).length;document.getElementById('sReady').textContent=data.filter(x=>(x.ready_score||0)>=70).length;const d=data.map(x=>x.borrow_timestamp).filter(Boolean).sort().pop();document.getElementById('lastBorrow').textContent=d?new Date(d+'Z').toLocaleString('ar-SA'):'—'}
function render(){const q=document.getElementById('q').value.trim().toUpperCase();let arr=data.filter(x=>!q||x.symbol.includes(q)||(x.company||'').toUpperCase().includes(q));if(filter==='scarce')arr=arr.filter(x=>x.available!=null&&x.available<=10000);if(filter==='half')arr=arr.filter(x=>x.half_level_reached);if(filter==='near')arr=arr.filter(x=>x.distance_from_low_pct!=null&&x.distance_from_low_pct<=10);arr.sort((a,b)=>(b.ready_score||0)-(a.ready_score||0));const body=document.getElementById('rows');if(!arr.length){body.innerHTML='<tr><td colspan="12" class="empty">لا توجد نتائج</td></tr>';return}body.innerHTML=arr.map(x=>`<tr onclick="openStock('${x.symbol}')"><td><div class="ticker">${x.symbol}</div><div class="company">${x.company||''}</div></td><td><span class="score ${(x.ready_score||0)>=70?'hot':(x.ready_score||0)>=50?'mid':''}">${fmt(x.ready_score)}</span></td><td class="${x.available!=null&&x.available<=10000?'good':''}">${fmt(x.available)}</td><td class="goldtext">${x.ctb==null?'—':pct(x.ctb)}</td><td>${money(x.current_price)}</td><td>${money(x.post_split_low)}</td><td class="${x.distance_from_low_pct!=null&&x.distance_from_low_pct<=10?'good':''}">${pct(x.distance_from_low_pct)}</td><td>${money(x.post_split_high)}</td><td><span class="pill ${x.half_level_reached?'good':'muted'}">${money(x.half_level)} ${x.half_level_reached?'✓':''}</span></td><td>${fmt(x.stability_sessions)} / 4</td><td>${pct(x.four_hour_highest_rise_pct)}</td><td>${x.ratio}<div class="company">${x.effective_date}</div></td></tr>`).join('')}
async function openStock(symbol){document.getElementById('drawer').classList.add('open');document.getElementById('detail').innerHTML='<div class="empty">جاري التحميل...</div>';try{const [s,h]=await Promise.all([fetch('/api/stock/'+symbol).then(r=>r.json()),fetch('/api/borrow/'+symbol+'/history?limit=12').then(r=>r.json())]);document.getElementById('detail').innerHTML=`<h2>${s.symbol}</h2><div class="sub">${s.company||''} · ${s.ratio} · ${s.effective_date}</div><div class="grid2"><div class="mini"><span>Ready Score</span><b>${fmt(s.ready_score)}</b></div><div class="mini"><span>Available</span><b>${fmt(s.available)}</b></div><div class="mini"><span>CTB</span><b>${s.ctb==null?'—':pct(s.ctb)}</b></div><div class="mini"><span>Rebate</span><b>${s.rebate_rate==null?'—':pct(s.rebate_rate)}</b></div><div class="mini"><span>Current</span><b>${money(s.current_price)}</b></div><div class="mini"><span>Low</span><b>${money(s.post_split_low)}</b></div><div class="mini"><span>High</span><b>${money(s.post_split_high)}</b></div><div class="mini"><span>Half Level</span><b>${money(s.half_level)} ${s.half_level_reached?'✓':''}</b></div><div class="mini"><span>Distance from Low</span><b>${pct(s.distance_from_low_pct)}</b></div><div class="mini"><span>Stability</span><b>${fmt(s.stability_sessions)} / 4</b></div></div><div class="history"><h3>آخر قراءات الاقتراض</h3>${h.length?h.map(r=>`<div class="histrow"><span>${r.timestamp?new Date(r.timestamp+'Z').toLocaleString('ar-SA'):''}</span><b>${fmt(r.available)}</b><b>${r.ctb==null?'—':pct(r.ctb)}</b></div>`).join(''):'<div class="muted">لا يوجد سجل بعد</div>'}</div>`}catch(e){document.getElementById('detail').innerHTML='<div class="empty">تعذر تحميل التفاصيل</div>'}}
function closeDrawer(){document.getElementById('drawer').classList.remove('open')}
async function startBorrowSync(){const b=document.getElementById('borrowBtn');b.disabled=true;b.textContent='جاري بدء التحديث...';try{await fetch('/api/sync/borrow/all');toast('بدأ تحديث Available و CTB لكل الأسهم');pollBorrow()}catch(e){toast('تعذر بدء التحديث');b.disabled=false;b.textContent='تحديث Available + CTB'}}
async function pollBorrow(){const b=document.getElementById('borrowBtn');try{const s=await fetch('/api/sync/borrow/status').then(r=>r.json());b.textContent=s.running?`تحديث ${s.processed}/${s.total}`:'تحديث Available + CTB';if(s.running){setTimeout(pollBorrow,2500)}else{b.disabled=false;toast(`اكتمل التحديث: ${s.saved} قراءة جديدة`);loadData()}}catch(e){b.disabled=false;b.textContent='تحديث Available + CTB'}}
loadData();setInterval(loadData,120000);
</script>
</body>
</html>'''


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)
