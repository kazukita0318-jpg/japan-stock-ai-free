from flask import Flask, jsonify, request, render_template_string
import sqlite3, os, math, statistics, json
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.1-V2'

SCHEMA="""
CREATE TABLE IF NOT EXISTS events(
 id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, event_type TEXT NOT NULL,
 event_time TEXT NOT NULL, severity TEXT DEFAULT 'unknown', source TEXT DEFAULT '',
 pred20 REAL, pred126 REAL, pred252 REAL, status TEXT DEFAULT 'open'
);
CREATE TABLE IF NOT EXISTS outcomes(
 id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL, horizon INTEGER NOT NULL,
 realized_return REAL, error REAL, direction_hit INTEGER, measured_at TEXT,
 FOREIGN KEY(event_id) REFERENCES events(id)
);
CREATE TABLE IF NOT EXISTS calibration(
 event_type TEXT NOT NULL, horizon INTEGER NOT NULL, n INTEGER NOT NULL,
 mean_error REAL, median_error REAL, mae REAL, direction_hit REAL, mean_realized REAL,
 last_updated TEXT, PRIMARY KEY(event_type,horizon)
);
CREATE TABLE IF NOT EXISTS symbol_event_calibration(
 symbol TEXT NOT NULL, event_type TEXT NOT NULL, horizon INTEGER NOT NULL, n INTEGER NOT NULL,
 mean_error REAL, median_error REAL, mae REAL, direction_hit REAL, mean_realized REAL,
 last_updated TEXT, PRIMARY KEY(symbol,event_type,horizon)
);
"""
def db():
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; return c
with db() as c: c.executescript(SCHEMA); c.commit()
def now(): return datetime.now(timezone.utc).isoformat()

def refresh_cal(c,event_type,h):
    rows=c.execute("""SELECT o.error,o.realized_return,o.direction_hit
      FROM outcomes o JOIN events e ON e.id=o.event_id
      WHERE e.event_type=? AND o.horizon=? AND o.realized_return IS NOT NULL""",(event_type,h)).fetchall()
    if not rows:return
    errs=[r["error"] for r in rows if r["error"] is not None]
    rets=[r["realized_return"] for r in rows]
    hits=[r["direction_hit"] for r in rows if r["direction_hit"] is not None]
    c.execute("""INSERT INTO calibration VALUES(?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(event_type,horizon) DO UPDATE SET n=excluded.n,mean_error=excluded.mean_error,
      median_error=excluded.median_error,mae=excluded.mae,direction_hit=excluded.direction_hit,
      mean_realized=excluded.mean_realized,last_updated=excluded.last_updated""",
      (event_type,h,len(rows),statistics.mean(errs) if errs else None,
       statistics.median(errs) if errs else None,statistics.mean(abs(x) for x in errs) if errs else None,
       statistics.mean(hits) if hits else None,statistics.mean(rets),now()))

def refresh_symbol_cal(c,symbol,event_type,h):
    rows=c.execute("""SELECT o.error,o.realized_return,o.direction_hit
      FROM outcomes o JOIN events e ON e.id=o.event_id
      WHERE e.symbol=? AND e.event_type=? AND o.horizon=? AND o.realized_return IS NOT NULL""",
      (symbol,event_type,h)).fetchall()
    if not rows:return
    errs=[r["error"] for r in rows if r["error"] is not None]
    rets=[r["realized_return"] for r in rows]
    hits=[r["direction_hit"] for r in rows if r["direction_hit"] is not None]
    c.execute("""INSERT INTO symbol_event_calibration VALUES(?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(symbol,event_type,horizon) DO UPDATE SET n=excluded.n,mean_error=excluded.mean_error,
      median_error=excluded.median_error,mae=excluded.mae,direction_hit=excluded.direction_hit,
      mean_realized=excluded.mean_realized,last_updated=excluded.last_updated""",
      (symbol,event_type,h,len(rows),statistics.mean(errs) if errs else None,
       statistics.median(errs) if errs else None,statistics.mean(abs(x) for x in errs) if errs else None,
       statistics.mean(hits) if hits else None,statistics.mean(rets),now()))

def corrected(c,symbol,event_type,h,base):
    if base is None:return None,"unavailable"
    er=c.execute("SELECT * FROM calibration WHERE event_type=? AND horizon=?",(event_type,h)).fetchone()
    if not er or er["n"]<10:return base,"insufficient_oos_samples"
    ew=min(.6,math.sqrt(er["n"])/20)
    event_pred=base-(er["mean_error"] or 0)*ew
    sr=c.execute("SELECT * FROM symbol_event_calibration WHERE symbol=? AND event_type=? AND horizon=?",
                 (symbol,event_type,h)).fetchone()
    if not sr or sr["n"]<5:return event_pred,"event_oos_bias_corrected"
    sw=min(.7,max(.15,math.sqrt(sr["n"])/8))
    sym_pred=base-(sr["mean_error"] or 0)*sw
    return (1-sw)*event_pred+sw*sym_pred,"symbol_event_oos_blended"

@APP.get("/api/health")
def health(): return jsonify(version=VERSION,status="ok")

@APP.post("/api/events")
def add_event():
    d=request.get_json(force=True)
    if not d.get("symbol") or not d.get("event_type"): return jsonify(error="symbol and event_type are required"),400
    with db() as c:
        cur=c.execute("""INSERT INTO events(symbol,event_type,event_time,severity,source,pred20,pred126,pred252)
          VALUES(?,?,?,?,?,?,?,?)""",(d["symbol"],d["event_type"],d.get("event_time",now()),
          d.get("severity","unknown"),d.get("source",""),d.get("pred20"),d.get("pred126"),d.get("pred252")))
        c.commit(); eid=cur.lastrowid
    return jsonify(event_id=eid,status="open")

@APP.get("/api/events")
def events():
    with db() as c: rows=[dict(r) for r in c.execute("SELECT * FROM events ORDER BY id DESC LIMIT 200")]
    return jsonify(rows=rows)

@APP.post("/api/outcomes")
def outcome():
    d=request.get_json(force=True); eid=d.get("event_id"); h=int(d.get("horizon"))
    with db() as c:
        e=c.execute("SELECT * FROM events WHERE id=?",(eid,)).fetchone()
        if not e:return jsonify(error="event not found"),404
        realized=d.get("realized_return")
        pred={20:e["pred20"],126:e["pred126"],252:e["pred252"]}.get(h)
        err=(realized-pred) if realized is not None and pred is not None else None
        hit=(1 if realized*pred>0 else 0) if realized is not None and pred not in (None,0) else None
        c.execute("""INSERT INTO outcomes(event_id,horizon,realized_return,error,direction_hit,measured_at)
          VALUES(?,?,?,?,?,?)""",(eid,h,realized,err,hit,d.get("measured_at",now())))
        refresh_cal(c,e["event_type"],h); refresh_symbol_cal(c,e["symbol"],e["event_type"],h); c.commit()
        cal=c.execute("SELECT * FROM calibration WHERE event_type=? AND horizon=?",(e["event_type"],h)).fetchone()
    return jsonify(event_id=eid,horizon=h,error=err,direction_hit=hit,calibration=dict(cal) if cal else None)

@APP.get("/api/calibration")
def calibration():
    with db() as c: rows=[dict(r) for r in c.execute("SELECT * FROM calibration ORDER BY event_type,horizon")]
    return jsonify(rows=rows)

@APP.get("/api/personal-calibration")
def personal():
    with db() as c: rows=[dict(r) for r in c.execute("SELECT * FROM symbol_event_calibration ORDER BY symbol,event_type,horizon")]
    return jsonify(rows=rows)

@APP.post("/api/revalue")
def revalue():
    d=request.get_json(force=True); symbol=d.get("symbol",""); et=d.get("event_type","")
    bases={20:d.get("base20"),126:d.get("base126"),252:d.get("base252")}
    with db() as c:
        out={str(h):{"prediction":corrected(c,symbol,et,h,b)[0],"mode":corrected(c,symbol,et,h,b)[1]}
             for h,b in bases.items()}
    return jsonify(results=out)


def jq_config():
    return {
        "base": os.getenv("JQUANTS_BASE", "https://api.jquants.com/v2").rstrip("/"),
        "api_key": os.getenv("JQUANTS_API_KEY", "").strip(),
    }


def jq_request(path, params=None):
    cfg = jq_config()
    if not cfg["api_key"]:
        raise RuntimeError("J-Quants API繧ｭ繝ｼ縺梧悴險ｭ螳壹〒縺�")

    headers = {
        "Accept": "application/json",
        "x-api-key": cfg["api_key"],
    }
    r = requests.get(
        cfg["base"] + path,
        params=params or {},
        headers=headers,
        timeout=20,
    )
    if r.status_code != 200:
        raise RuntimeError(f"J-Quants HTTP {r.status_code}: {r.text[:300]}")
    return r.json()


def jquants_code(code):
    code = str(code or "").strip()
    if code.isdigit() and len(code) == 4:
        return code + "0"
    return code


def normalize_quote_rows(data):
    rows = (
        data.get("data")
        or data.get("daily_quotes")
        or data.get("prices")
        or data.get("bars")
        or []
    )

    out = []
    for x in rows:
        close = x.get("AdjC")
        if close is None:
            close = x.get("AdjustmentClose")
        if close is None:
            close = x.get("C")
        if close is None:
            close = x.get("Close")
        if close is None:
            continue

        open_ = x.get("AdjO")
        if open_ is None:
            open_ = x.get("AdjustmentOpen")
        if open_ is None:
            open_ = x.get("O")
        if open_ is None:
            open_ = x.get("Open")

        high = x.get("AdjH")
        if high is None:
            high = x.get("AdjustmentHigh")
        if high is None:
            high = x.get("H")
        if high is None:
            high = x.get("High")

        low = x.get("AdjL")
        if low is None:
            low = x.get("AdjustmentLow")
        if low is None:
            low = x.get("L")
        if low is None:
            low = x.get("Low")

        volume = x.get("AdjVo")
        if volume is None:
            volume = x.get("AdjustmentVolume")
        if volume is None:
            volume = x.get("Vo")
        if volume is None:
            volume = x.get("Volume")

        turnover = x.get("Va")
        if turnover is None:
            turnover = x.get("TurnoverValue")

        out.append({
            "date": x.get("Date"),
            "code": x.get("Code"),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "turnover": turnover,
        })

    out.sort(key=lambda x: x["date"] or "")
    return out


def technical_snapshot(rows):
    closes = [
        r["close"]
        for r in rows
        if isinstance(r.get("close"), (int, float))
    ]
    if not closes:
        return {"status": "insufficient_data"}

    def ret(n):
        if len(closes) <= n:
            return None
        return (closes[-1] / closes[-1 - n] - 1) * 100

    def vol(n=20):
        rs = []
        for i in range(max(1, len(closes) - n), len(closes)):
            if closes[i - 1]:
                rs.append(closes[i] / closes[i - 1] - 1)
        return (
            statistics.pstdev(rs) * 100 * (252 ** 0.5)
            if len(rs) > 1
            else None
        )

    return {
        "status": "ok",
        "last_close": closes[-1],
        "last_date": rows[-1]["date"],
        "return_20d": ret(20),
        "return_126d": ret(126),
        "return_252d": ret(252),
        "volatility_20d_annualized": vol(20),
        "high_20d": max(closes[-20:]) if len(closes) >= 20 else max(closes),
        "low_20d": min(closes[-20:]) if len(closes) >= 20 else min(closes),
        "sample_count": len(closes),
    }


def jquants_status():
    return bool(os.getenv("JQUANTS_API_KEY", "").strip())


@APP.get("/api/mobile/status")
def mobile_status():
    return jsonify(
        version=VERSION,
        jquants_configured=jquants_status(),
        policy="no_fabrication",
        server_time=now(),
    )


@APP.get("/api/mobile/quote")
def mobile_quote():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400
    if not jquants_status():
        return jsonify(
            status="unavailable",
            reason="J-Quants API繧ｭ繝ｼ縺梧悴險ｭ螳壹〒縺�",
        )

    try:
        jq_code = jquants_code(code)
        data = jq_request("/equities/bars/daily", {"code": jq_code})
        rows = normalize_quote_rows(data)
        snap = technical_snapshot(rows)
        return jsonify(
            status="ok",
            code=code,
            jquants_code=jq_code,
            source="J-Quants API v2",
            snapshot=snap,
            rows=rows[-30:],
            pagination_key=data.get("pagination_key"),
        )
    except Exception as e:
        return jsonify(
            status="error",
            code=code,
            reason=str(e),
        ), 502


@APP.get("/api/mobile/final-status")
def final_status():
    return jsonify(
        version=VERSION,
        installable=True,
        pwa=True,
        jquants_configured=jquants_status(),
        mode="mobile_final",
        policy="no_fabrication",
    )


@APP.get("/api/mobile/quotes")
def mobile_quotes():
    code = request.args.get("code", "").strip()
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    if not code:
        return jsonify(error="code required"), 400
    if not jquants_status():
        return jsonify(
            status="unavailable",
            reason="J-Quants API繧ｭ繝ｼ縺梧悴險ｭ螳壹〒縺�",
        )

    try:
        jq_code = jquants_code(code)
        p = {"code": jq_code}
        if from_date:
            p["from"] = from_date
        if to_date:
            p["to"] = to_date

        data = jq_request("/equities/bars/daily", p)
        rows = normalize_quote_rows(data)

        return jsonify(
            status="ok",
            code=code,
            jquants_code=jq_code,
            source="J-Quants API v2",
            snapshot=technical_snapshot(rows),
            rows=rows,
            pagination_key=data.get("pagination_key"),
        )
    except Exception as e:
        return jsonify(
            status="error",
            code=code,
            reason=str(e),
        ), 502


@APP.get("/api/free/status")
def free_status():
    return jsonify(
        version=VERSION,
        mode="free",
        jquants_required=False,
        data_policy="no_fabrication",
        message="辟｡譁吶Δ繝ｼ繝峨よ�ｪ謗｢遲峨�遒ｺ隱咲畑繝ｪ繝ｳ繧ｯ縺ｨ縺励※蛻ｩ逕ｨ縺励∬�蜍輔せ繧ｯ繝ｬ繧､繝斐Φ繧ｰ縺励∪縺帙ｓ縲�"
    )

@APP.post("/api/free/analyze")
def free_analyze():
    d=request.get_json(force=True) or {}
    code=str(d.get("code","")).strip()
    if not code:
        return jsonify(error="code required"),400

    def num(name):
        v=d.get(name)
        if v in (None,""): return None
        try: return float(v)
        except Exception: return None

    price=num("price")
    r20=num("return20")
    r126=num("return126")
    r252=num("return252")
    earnings=num("earnings_score")
    policy=num("policy_score")
    supply=num("supply_score")

    supplied=[x for x in [r20,r126,r252,earnings,policy,supply] if x is not None]
    if not supplied:
        return jsonify(
            status="insufficient_data", code=code,
            reason="蛻�梵縺ｫ菴ｿ縺医ｋ螳溘ョ繝ｼ繧ｿ縺悟�蜉帙＆繧後※縺�∪縺帙ｓ",
            expected_value={"20d":None,"126d":None,"252d":None}
        )

    positives=sum(x>0 for x in supplied)
    negatives=sum(x<0 for x in supplied)
    state="mixed"
    if positives >= max(2, negatives+1): state="positive"
    if negatives >= max(2, positives+1): state="negative"

    # Deliberately no fabricated expected-return number.
    return jsonify(
        status="ok", code=code, mode="free_manual_evidence",
        price=price,
        signal={
            "state":state,
            "positive_count":positives,
            "negative_count":negatives,
            "evidence_count":len(supplied)
        },
        expected_value={
            "status":"unavailable",
            "20d":None,"126d":None,"252d":None,
            "reason":"OOS譬｡豁｣貂医∩縺ｮ辟｡譁吶ョ繝ｼ繧ｿ螻･豁ｴ縺悟香蛻�↓闢�ｩ阪＆繧後ｋ縺ｾ縺ｧ譛溷ｾ�､縺ｯ陦ｨ遉ｺ縺励∪縺帙ｓ"
        },
        sources={
            "market_data":"user-entered / permitted public source",
            "kabutan":"reference_only_no_scraping"
        }
    )


HTML=r"""<!doctype html><html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#111827"><link rel="manifest" href="/static/manifest.webmanifest">
<title>譌･譛ｬ譬ｪAI FREE</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#f3f4f6;color:#111827;font-family:system-ui,-apple-system,"Noto Sans JP",sans-serif}
main{max-width:760px;margin:auto;padding:12px 12px 80px}.top{background:#111827;color:#fff;padding:15px;border-radius:0 0 18px 18px;position:sticky;top:0;z-index:3}
h1{font-size:20px;margin:0}.sub,.muted{font-size:11px;color:#6b7280}.top .sub{color:#d1d5db}.card{background:#fff;border-radius:16px;padding:14px;margin:10px 0;box-shadow:0 2px 10px #0001}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}
input,select,button,a.btn{width:100%;min-height:44px;border-radius:11px;font-size:15px;padding:9px}
input,select{border:1px solid #d1d5db;background:#fff}button,a.btn{border:0;background:#111827;color:#fff;font-weight:700;text-decoration:none;display:flex;align-items:center;justify-content:center}
.secondary{background:#e5e7eb!important;color:#111827!important}.kpi{background:#f9fafb;border-radius:12px;padding:10px}.kpi b{display:block;font-size:18px}
.row{display:flex;gap:7px;margin:7px 0}.badge{display:inline-block;padding:4px 8px;border-radius:99px;background:#eef2ff;font-size:11px;margin:2px}.warn{color:#b45309}.ok{color:#047857}
</style></head><body><main>
<div class="top"><h1>嶋 譌･譛ｬ譬ｪAI FREE</h1><div class="sub">螳悟�辟｡譁吶Δ繝ｼ繝� / 繧ｹ繝槭�逕ｨ / 譬ｪ謗｢縺ｯ遒ｺ隱咲畑繝ｻ閾ｪ蜍募叙蠕励↑縺�</div></div>

<div class="card"><h3>識 驫俶氛</h3>
<div class="grid"><input id="code" inputmode="numeric" placeholder="驫俶氛繧ｳ繝ｼ繝� 萓� 7203"><input id="price" inputmode="decimal" placeholder="迴ｾ蝨ｨ蛟､�井ｻｻ諢擾ｼ�"></div>
<div class="row"><a id="kabutan" class="btn secondary" target="_blank" rel="noopener">譬ｪ謗｢縺ｧ遒ｺ隱�</a><button onclick="analyze()">蛻�梵縺吶ｋ</button></div>
<p class="muted">譬ｪ謗｢縺ｮ謗ｲ霈画ュ蝣ｱ縺ｯ繝悶Λ繧ｦ繧ｶ縺ｧ遒ｺ隱阪☆繧九◆繧√�陬懷勧蟆守ｷ壹〒縺吶ゅい繝励Μ縺九ｉ閾ｪ蜍輔せ繧ｯ繝ｬ繧､繝斐Φ繧ｰ縺励∪縺帙ｓ縲�</p></div>

<div class="card"><h3>投 譬ｪ萓｡繝ｻ繝�け繝九き繝ｫ螳溽ｸｾ</h3>
<div class="grid3"><div><span class="muted">20譌･鬨ｰ關ｽ邇� %</span><input id="r20" inputmode="decimal" placeholder="萓� 5.2"></div>
<div><span class="muted">126譌･ %</span><input id="r126" inputmode="decimal" placeholder="萓� 12.4"></div>
<div><span class="muted">252譌･ %</span><input id="r252" inputmode="decimal" placeholder="萓� 18.0"></div></div></div>

<div class="card"><h3>ｧｩ 陬懷勧隧穂ｾ｡</h3><p class="muted">蛻�°繧矩��岼縺�縺大�蜉帙よ悴蜈･蜉帙�0轤ｹ縺ｧ縺ｯ縺ｪ縺上御ｸ肴�縲阪→縺励※謇ｱ縺�∪縺吶�</p>
<div class="grid3"><div><span class="muted">豎ｺ邂� -100縲�100</span><input id="earn" inputmode="decimal"></div>
<div><span class="muted">謾ｿ遲� -100縲�100</span><input id="policy" inputmode="decimal"></div>
<div><span class="muted">髴邨ｦ -100縲�100</span><input id="supply" inputmode="decimal"></div></div></div>

<div class="card"><h3>ｧ� 蛻�梵邨先棡</h3>
<div class="grid3"><div class="kpi"><span class="muted">迥ｶ諷�</span><b id="state">窶�</b></div>
<div class="kpi"><span class="muted">繝励Λ繧ｹ譬ｹ諡�</span><b id="pos">窶�</b></div>
<div class="kpi"><span class="muted">繝槭う繝翫せ譬ｹ諡�</span><b id="neg">窶�</b></div></div>
<p id="result" class="muted">螳溘ョ繝ｼ繧ｿ繧貞�蜉帙☆繧九→蛻�梵縺励∪縺吶�</p></div>

<div class="card"><h3>軸 遏ｭ譛溘�荳ｭ譛溘�髟ｷ譛滓悄蠕�､</h3>
<div class="grid3"><div class="kpi"><span class="muted">20譌･</span><b>窶�</b></div><div class="kpi"><span class="muted">126譌･</span><b>窶�</b></div><div class="kpi"><span class="muted">252譌･</span><b>窶�</b></div></div>
<p class="muted">辟｡譁吶ョ繝ｼ繧ｿ縺ｮOOS螳溽ｸｾ縺悟香蛻�↓闢�ｩ阪＆繧後ｋ縺ｾ縺ｧ縺ｯ縲∵楔遨ｺ縺ｮ譛溷ｾ�Μ繧ｿ繝ｼ繝ｳ繧定｡ｨ遉ｺ縺励∪縺帙ｓ縲�</p></div>

<div class="card"><h3>直 菫晄怏譬ｪ</h3><div class="grid"><input id="holdCode" placeholder="驫俶氛繧ｳ繝ｼ繝�"><input id="holdWeight" inputmode="decimal" placeholder="豈皮紫 %"></div>
<button onclick="addHold()" style="margin-top:8px">菫晏ｭ�</button><div id="holds"></div></div>

<div class="card"><h3>操 繧ｦ繧ｩ繝�メ繝ｪ繧ｹ繝�</h3><div class="row"><input id="watchCode" placeholder="驫俶氛繧ｳ繝ｼ繝�"><button onclick="addWatch()">霑ｽ蜉�</button></div><div id="watchs"></div></div>

<div class="card"><h3>導 繧｢繝励Μ縺ｨ縺励※菴ｿ縺�</h3><p class="muted">繧ｯ繝ｩ繧ｦ繝牙�髢句ｾ後、ndroid Chrome縺ｮ縲後�繝ｼ繝�逕ｻ髱｢縺ｫ霑ｽ蜉�縲阪∪縺溘�縲後い繝励Μ繧偵う繝ｳ繧ｹ繝医�繝ｫ縲阪〒襍ｷ蜍輔〒縺阪∪縺吶�</p></div>

<script>
const $=x=>document.getElementById(x);
function val(id){let v=$(id).value.trim();return v===''?null:Number(v)}
function local(k){try{return JSON.parse(localStorage.getItem(k)||'[]')}catch(e){return[]}}
function save(k,v){localStorage.setItem(k,JSON.stringify(v))}
function render(){
 $('holds').innerHTML=local('free_holds').map(x=>`<div class="badge">${x.code} ${x.weight}%</div>`).join('')||'<p class="muted">譛ｪ逋ｻ骭ｲ</p>';
 $('watchs').innerHTML=local('free_watch').map(x=>`<div class="badge">${x}</div>`).join('')||'<p class="muted">譛ｪ逋ｻ骭ｲ</p>';
}
function addHold(){let c=$('holdCode').value.trim();if(!c)return;let a=local('free_holds');a.push({code:c,weight:val('holdWeight')||0});save('free_holds',a);render()}
function addWatch(){let c=$('watchCode').value.trim();if(!c)return;let a=local('free_watch');if(!a.includes(c))a.push(c);save('free_watch',a);render()}
function updateKabutan(){let c=$('code').value.trim();$('kabutan').href=c?'https://kabutan.jp/stock/?code='+encodeURIComponent(c):'https://kabutan.jp/'}
$('code').addEventListener('input',updateKabutan);updateKabutan();
async function analyze(){
 let d={code:$('code').value.trim(),price:val('price'),return20:val('r20'),return126:val('r126'),return252:val('r252'),earnings_score:val('earn'),policy_score:val('policy'),supply_score:val('supply')};
 if(!d.code){$('result').textContent='驫俶氛繧ｳ繝ｼ繝峨ｒ蜈･蜉帙＠縺ｦ縺ｭ';return}
 let r=await fetch('/api/free/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});let x=await r.json();
 if(x.status!=='ok'){$('result').textContent='笞��� '+(x.reason||x.error||'繝��繧ｿ荳崎ｶｳ');return}
 $('state').textContent=x.signal.state;$('pos').textContent=x.signal.positive_count;$('neg').textContent=x.signal.negative_count;
 $('result').textContent='譬ｹ諡� '+x.signal.evidence_count+'莉ｶ縺ｧ蛻､螳壹よ悄蠕�､縺ｯOOS譬｡豁｣繝��繧ｿ縺悟香蛻�↓縺ｪ繧九∪縺ｧ譛ｪ陦ｨ遉ｺ縺ｧ縺吶�';
}
render();
if('serviceWorker'in navigator)navigator.serviceWorker.register('/static/sw.js').catch(()=>{});
</script></main></body></html>"""

@APP.get("/")
def index(): return render_template_string(HTML)

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
