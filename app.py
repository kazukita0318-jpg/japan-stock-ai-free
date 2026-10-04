from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.2-AUTO'

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
        raise RuntimeError("J-Quants API key is not configured")

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
            reason="J-Quants API key is not configured",
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
            reason="J-Quants API key is not configured",
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
        message="Free mode. External stock sites are reference links only; no automatic scraping."
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
            reason="No real market data is available for analysis",
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
            "reason":"Expected values stay unavailable until enough OOS-calibrated history is collected"
        },
        sources={
            "market_data":"user-entered / permitted public source",
            "kabutan":"reference_only_no_scraping"
        }
    )


HTML = base64.b64decode(
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9LnN1YiwubXV0ZWR7Zm9udC1zaXplOjEycHg7Y29sb3I6IzZiNzI4MH0udG9wIC5zdWJ7Y29sb3I6I2QxZDVkYn0KLmNhcmR7YmFja2dyb3VuZDojZmZmO2JvcmRlci1yYWRpdXM6MTZweDtwYWRkaW5nOjE0cHg7bWFyZ2luOjEwcHggMDtib3gtc2hhZG93OjAgMnB4IDEwcHggIzAwMDF9Ci5ncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcjtnYXA6OHB4fS5ncmlkM3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCgzLDFmcik7Z2FwOjdweH0KaW5wdXQsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXR7Ym9yZGVyOjFweCBzb2xpZCAjZDFkNWRiO2JhY2tncm91bmQ6I2ZmZn0KYnV0dG9uLGEuYnRue2JvcmRlcjowO2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO2ZvbnQtd2VpZ2h0OjcwMDt0ZXh0LWRlY29yYXRpb246bm9uZTtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2p1c3RpZnktY29udGVudDpjZW50ZXJ9Ci5zZWNvbmRhcnl7YmFja2dyb3VuZDojZTVlN2ViIWltcG9ydGFudDtjb2xvcjojMTExODI3IWltcG9ydGFudH0ua3Bpe2JhY2tncm91bmQ6I2Y5ZmFmYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzoxMHB4fS5rcGkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4fQoucm93e2Rpc3BsYXk6ZmxleDtnYXA6N3B4O21hcmdpbjo3cHggMH0uYmFkZ2V7ZGlzcGxheTppbmxpbmUtYmxvY2s7cGFkZGluZzo0cHggOHB4O2JvcmRlci1yYWRpdXM6OTlweDtiYWNrZ3JvdW5kOiNlZWYyZmY7Zm9udC1zaXplOjExcHg7bWFyZ2luOjJweH0KLm9re2NvbG9yOiMwNDc4NTd9LmVycntjb2xvcjojYjkxYzFjfS5zb3VyY2V7YmFja2dyb3VuZDojZWNmZGY1O2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHh9Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+PG1haW4+CjxkaXYgY2xhc3M9InRvcCI+PGgxPvCfk4gg5pel5pys5qCqQUkgRlJFRTwvaDE+PGRpdiBjbGFzcz0ic3ViIj5KLVF1YW50c+Wun+ODh+ODvOOCv+mAo+aQuiAvIOOCueODnuODm+eUqCAvIOaOqOa4rOWApOOCkuaNj+mAoOOBl+OBquOBhDwvZGl2PjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+PGgzPvCfjq8g6YqY5p+EPC9oMz4KPGRpdiBjbGFzcz0iZ3JpZCI+PGlucHV0IGlkPSJjb2RlIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kg5L6LIDcyMDMiPjxpbnB1dCBpZD0icHJpY2UiIHBsYWNlaG9sZGVyPSLnj77lnKjlgKQiIHJlYWRvbmx5PjwvZGl2Pgo8ZGl2IGNsYXNzPSJyb3ciPjxhIGlkPSJrYWJ1dGFuIiBjbGFzcz0iYnRuIHNlY29uZGFyeSIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPuagquaOouOBp+eiuuiqjTwvYT48YnV0dG9uIGlkPSJhbmFseXplQnRuIiBvbmNsaWNrPSJhbmFseXplKCkiPuWun+ODh+ODvOOCv+OBp+WIhuaekDwvYnV0dG9uPjwvZGl2Pgo8cCBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeOCjOOBpuaKvOOBmeOBqOOAgUotUXVhbnRz44GL44KJ5Y+W5b6X44Gn44GN44KL5pyA5paw44Gu5a6f44OH44O844K/44KS6Ieq5YuV5YWl5Yqb44GX44G+44GZ44CCPC9wPgo8ZGl2IGlkPSJzb3VyY2VCb3giIGNsYXNzPSJzb3VyY2UgbXV0ZWQiPuODh+ODvOOCv+acquWPluW+lzwvZGl2PjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+PGgzPvCfk4og5qCq5L6h44O744OG44Kv44OL44Kr44Or5a6f57i+PC9oMz4KPGRpdiBjbGFzcz0iZ3JpZDMiPjxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemosOiQveeOhyAlPC9zcGFuPjxpbnB1dCBpZD0icjIwIiByZWFkb25seT48L2Rpdj4KPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjEyNuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjEyNiIgcmVhZG9ubHk+PC9kaXY+CjxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yNTLml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIyNTIiIHJlYWRvbmx5PjwvZGl2PjwvZGl2Pgo8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6auY5YCkPC9zcGFuPjxiIGlkPSJoaWdoMjAiPuKAlDwvYj48L2Rpdj4KPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel5a6J5YCkPC9zcGFuPjxiIGlkPSJsb3cyMCI+4oCUPC9iPjwvZGl2Pgo8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlubTnjofjg5zjg6k8L3NwYW4+PGIgaWQ9InZvbDIwIj7igJQ8L2I+PC9kaXY+CjwvZGl2PjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+PGgzPvCfp6kg6KOc5Yqp6KmV5L6hPC9oMz48cCBjbGFzcz0ibXV0ZWQiPuWIhuOBi+OCi+mgheebruOBoOOBkeWFpeWKm+OAguacquWFpeWKm+OBrzDngrnjgafjga/jgarjgY/jgIzkuI3mmI7jgI3jgajjgZfjgabmibHjgYTjgb7jgZnjgII8L3A+CjxkaXYgY2xhc3M9ImdyaWQzIj48ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5rG6566XIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJlYXJuIiBpbnB1dG1vZGU9ImRlY2ltYWwiPjwvZGl2Pgo8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5pS/562WIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJwb2xpY3kiIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CjxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7pnIDntaYgLTEwMOOAnDEwMDwvc3Bhbj48aW5wdXQgaWQ9InN1cHBseSIgaW5wdXRtb2RlPSJkZWNpbWFsIj48L2Rpdj48L2Rpdj48L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPjxoMz7wn6egIOWIhuaekOe1kOaenDwvaDM+CjxkaXYgY2xhc3M9ImdyaWQzIj48ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+54q25oWLPC9zcGFuPjxiIGlkPSJzdGF0ZSI+4oCUPC9iPjwvZGl2Pgo8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OX44Op44K55qC55ougPC9zcGFuPjxiIGlkPSJwb3MiPuKAlDwvYj48L2Rpdj4KPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODnuOCpOODiuOCueagueaLoDwvc3Bhbj48YiBpZD0ibmVnIj7igJQ8L2I+PC9kaXY+PC9kaXY+CjxwIGlkPSJyZXN1bHQiIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44CM5a6f44OH44O844K/44Gn5YiG5p6Q44CN44KS5oq844GX44Gm44GP44Gg44GV44GE44CCPC9wPjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+PGgzPvCfkrwg5L+d5pyJ5qCqPC9oMz4KPGRpdiBjbGFzcz0iZ3JpZCI+PGlucHV0IGlkPSJob2xkQ29kZSIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+PGlucHV0IGlkPSJob2xkV2VpZ2h0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLmr5TnjocgJSI+PC9kaXY+CjxidXR0b24gb25jbGljaz0iYWRkSG9sZCgpIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPuS/neWtmDwvYnV0dG9uPjxkaXYgaWQ9ImhvbGRzIj48L2Rpdj48L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPjxoMz7wn5GAIOOCpuOCqeODg+ODgeODquOCueODiDwvaDM+CjxkaXYgY2xhc3M9InJvdyI+PGlucHV0IGlkPSJ3YXRjaENvZGUiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPjxidXR0b24gb25jbGljaz0iYWRkV2F0Y2goKSI+6L+95YqgPC9idXR0b24+PC9kaXY+PGRpdiBpZD0id2F0Y2hzIj48L2Rpdj48L2Rpdj4KCjxzY3JpcHQ+CmNvbnN0ICQ9eD0+ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoeCk7CmZ1bmN0aW9uIHZhbChpZCl7bGV0IHY9JChpZCkudmFsdWUudHJpbSgpO3JldHVybiB2PT09Jyc/bnVsbDpOdW1iZXIodil9CmZ1bmN0aW9uIGxvY2FsKGspe3RyeXtyZXR1cm4gSlNPTi5wYXJzZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrKXx8J1tdJyl9Y2F0Y2goZSl7cmV0dXJuW119fQpmdW5jdGlvbiBzYXZlKGssdil7bG9jYWxTdG9yYWdlLnNldEl0ZW0oayxKU09OLnN0cmluZ2lmeSh2KSl9CmZ1bmN0aW9uIGZtdCh2LGQ9Mil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKGQpfQpmdW5jdGlvbiBzdGF0ZUphKHMpe3JldHVybiBzPT09J3Bvc2l0aXZlJz8n44OX44Op44K55YSq5YuiJzpzPT09J25lZ2F0aXZlJz8n44Oe44Kk44OK44K55YSq5YuiJzon5ouu5oqXJ30KZnVuY3Rpb24gcmVuZGVyKCl7CiAkKCdob2xkcycpLmlubmVySFRNTD1sb2NhbCgnZnJlZV9ob2xkcycpLm1hcCh4PT5gPGRpdiBjbGFzcz0iYmFkZ2UiPiR7eC5jb2RlfSAke3gud2VpZ2h0fSU8L2Rpdj5gKS5qb2luKCcnKXx8JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7CiAkKCd3YXRjaHMnKS5pbm5lckhUTUw9bG9jYWwoJ2ZyZWVfd2F0Y2gnKS5tYXAoeD0+YDxkaXYgY2xhc3M9ImJhZGdlIj4ke3h9PC9kaXY+YCkuam9pbignJyl8fCc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nOwp9CmZ1bmN0aW9uIGFkZEhvbGQoKXtsZXQgYz0kKCdob2xkQ29kZScpLnZhbHVlLnRyaW0oKTtpZighYylyZXR1cm47bGV0IGE9bG9jYWwoJ2ZyZWVfaG9sZHMnKTthLnB1c2goe2NvZGU6Yyx3ZWlnaHQ6dmFsKCdob2xkV2VpZ2h0Jyl8fDB9KTtzYXZlKCdmcmVlX2hvbGRzJyxhKTtyZW5kZXIoKX0KZnVuY3Rpb24gYWRkV2F0Y2goKXtsZXQgYz0kKCd3YXRjaENvZGUnKS52YWx1ZS50cmltKCk7aWYoIWMpcmV0dXJuO2xldCBhPWxvY2FsKCdmcmVlX3dhdGNoJyk7aWYoIWEuaW5jbHVkZXMoYykpYS5wdXNoKGMpO3NhdmUoJ2ZyZWVfd2F0Y2gnLGEpO3JlbmRlcigpfQpmdW5jdGlvbiB1cGRhdGVLYWJ1dGFuKCl7bGV0IGM9JCgnY29kZScpLnZhbHVlLnRyaW0oKTskKCdrYWJ1dGFuJykuaHJlZj1jPydodHRwczovL2thYnV0YW4uanAvc3RvY2svP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYyk6J2h0dHBzOi8va2FidXRhbi5qcC8nfQokKCdjb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLHVwZGF0ZUthYnV0YW4pO3VwZGF0ZUthYnV0YW4oKTsKCmFzeW5jIGZ1bmN0aW9uIGFuYWx5emUoKXsKIGNvbnN0IGNvZGU9JCgnY29kZScpLnZhbHVlLnRyaW0oKTsKIGlmKCFjb2RlKXskKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44GtJztyZXR1cm59CiBjb25zdCBidG49JCgnYW5hbHl6ZUJ0bicpO2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJzsKICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSdKLVF1YW50c+OBi+OCieWun+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBhOOBvuOBmeKApic7CiB0cnl7CiAgIGNvbnN0IHFyPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9xdW90ZT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgIGNvbnN0IHE9YXdhaXQgcXIuanNvbigpOwogICBpZihxLnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHEucmVhc29ufHxxLmVycm9yfHwn5a6f44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgIGNvbnN0IHM9cS5zbmFwc2hvdHx8e307CiAgICQoJ3ByaWNlJykudmFsdWU9cy5sYXN0X2Nsb3NlPT1udWxsPycnOmZtdChzLmxhc3RfY2xvc2UsMSk7CiAgICQoJ3IyMCcpLnZhbHVlPWZtdChzLnJldHVybl8yMGQpOyQoJ3IxMjYnKS52YWx1ZT1mbXQocy5yZXR1cm5fMTI2ZCk7JCgncjI1MicpLnZhbHVlPWZtdChzLnJldHVybl8yNTJkKTsKICAgJCgnaGlnaDIwJykudGV4dENvbnRlbnQ9Zm10KHMuaGlnaF8yMGQsMSk7JCgnbG93MjAnKS50ZXh0Q29udGVudD1mbXQocy5sb3dfMjBkLDEpOwogICAkKCd2b2wyMCcpLnRleHRDb250ZW50PXMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZD09bnVsbD8n4oCUJzpmbXQocy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkKSsnJSc7CiAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0nPGIgY2xhc3M9Im9rIj7inIUgSi1RdWFudHPlrp/jg4fjg7zjgr/lj5blvpdPSzwvYj48YnI+5pyA57WC44OH44O844K/5pelOiAnKyhzLmxhc3RfZGF0ZXx8J+KAlCcpKycgLyDntYLlgKQ6ICcrZm10KHMubGFzdF9jbG9zZSwxKSsnIC8g44K144Oz44OX44OrOiAnKyhzLnNhbXBsZV9jb3VudD8/J+KAlCcpKyfku7YnOwogICBjb25zdCBkPXtjb2RlOmNvZGUscHJpY2U6cy5sYXN0X2Nsb3NlLHJldHVybjIwOnMucmV0dXJuXzIwZCxyZXR1cm4xMjY6cy5yZXR1cm5fMTI2ZCxyZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCxlYXJuaW5nc19zY29yZTp2YWwoJ2Vhcm4nKSxwb2xpY3lfc2NvcmU6dmFsKCdwb2xpY3knKSxzdXBwbHlfc2NvcmU6dmFsKCdzdXBwbHknKX07CiAgIGNvbnN0IGFyPWF3YWl0IGZldGNoKCcvYXBpL2ZyZWUvYW5hbHl6ZScse21ldGhvZDonUE9TVCcsaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSxib2R5OkpTT04uc3RyaW5naWZ5KGQpLGNhY2hlOiduby1zdG9yZSd9KTsKICAgY29uc3QgeD1hd2FpdCBhci5qc29uKCk7CiAgIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfliIbmnpDjg4fjg7zjgr/jgYzkuI3otrPjgZfjgabjgYTjgb7jgZknKTsKICAgJCgnc3RhdGUnKS50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTskKCdwb3MnKS50ZXh0Q29udGVudD14LnNpZ25hbC5wb3NpdGl2ZV9jb3VudDskKCduZWcnKS50ZXh0Q29udGVudD14LnNpZ25hbC5uZWdhdGl2ZV9jb3VudDsKICAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCvyAnK3guc2lnbmFsLmV2aWRlbmNlX2NvdW50Kyfku7bjgpLmoLnmi6DjgavliKTlrprjgILmnJ/lvoXlgKTjga9PT1PmoKHmraPjg4fjg7zjgr/jgYzljYHliIbjgavjgarjgovjgb7jgafmnKrooajnpLrjgafjgZnjgIInOwogfWNhdGNoKGUpewogICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n4pqg77iPICcrZS5tZXNzYWdlOwogICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9JzxzcGFuIGNsYXNzPSJlcnIiPuWPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UrJzwvc3Bhbj4nOwogfWZpbmFsbHl7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn5YiG5p6QJ30KfQpyZW5kZXIoKTsKaWYoJ3NlcnZpY2VXb3JrZXInIGluIG5hdmlnYXRvcil7bmF2aWdhdG9yLnNlcnZpY2VXb3JrZXIuZ2V0UmVnaXN0cmF0aW9ucygpLnRoZW4ocnM9PlByb21pc2UuYWxsKHJzLm1hcChyPT5yLnVucmVnaXN0ZXIoKSkpKS5jYXRjaCgoKT0+e30pfQppZignY2FjaGVzJyBpbiB3aW5kb3cpe2NhY2hlcy5rZXlzKCkudGhlbihrZXlzPT5Qcm9taXNlLmFsbChrZXlzLm1hcChrPT5jYWNoZXMuZGVsZXRlKGspKSkpLmNhdGNoKCgpPT57fSl9Cjwvc2NyaXB0PjwvbWFpbj48L2JvZHk+PC9odG1sPg=="
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
