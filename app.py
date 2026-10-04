from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.8-PRIORITY-ACTIONS'
SECURITY_CACHE={}

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


def security_info(code):
    raw_code = str(code or "").strip()
    jq_code = jquants_code(raw_code)
    cache_key = jq_code
    if cache_key in SECURITY_CACHE:
        return SECURITY_CACHE[cache_key]

    data = jq_request("/equities/master", {"code": jq_code})
    rows = data.get("data") or data.get("info") or []
    row = rows[0] if rows else {}

    info = {
        "code": raw_code,
        "jquants_code": jq_code,
        "name": (
            row.get("CoName")
            or row.get("CompanyName")
            or row.get("Name")
            or ""
        ),
        "name_en": (
            row.get("CoNameEn")
            or row.get("CompanyNameEnglish")
            or row.get("NameEnglish")
            or ""
        ),
        "market": (
            row.get("MktNm")
            or row.get("MarketCodeName")
            or row.get("MarketName")
            or ""
        ),
        "sector17": (
            row.get("S17Nm")
            or row.get("Sector17CodeName")
            or ""
        ),
        "sector33": (
            row.get("S33Nm")
            or row.get("Sector33CodeName")
            or ""
        ),
    }
    SECURITY_CACHE[cache_key] = info
    return info


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

    def percentile(values, p):
        if not values:
            return None
        xs = sorted(values)
        if len(xs) == 1:
            return xs[0]
        k = (len(xs) - 1) * p
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return xs[int(k)]
        return xs[f] * (c - k) + xs[c] * (k - f)

    def forward_stats(n):
        vals = []
        for i in range(0, len(closes) - n):
            a = closes[i]
            b = closes[i + n]
            if a:
                vals.append((b / a - 1) * 100)

        if len(vals) < 20:
            return {
                "status": "insufficient_data",
                "horizon": n,
                "n": len(vals),
            }

        return {
            "status": "ok",
            "horizon": n,
            "n": len(vals),
            "mean": statistics.fmean(vals),
            "median": statistics.median(vals),
            "positive_rate": sum(1 for x in vals if x > 0) / len(vals) * 100,
            "p10": percentile(vals, 0.10),
            "p90": percentile(vals, 0.90),
            "min": min(vals),
            "max": max(vals),
            "method": "rolling_historical_forward_return",
        }

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
        "forward_return_stats": {
            "20d": forward_stats(20),
            "126d": forward_stats(126),
            "252d": forward_stats(252),
        },
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


@APP.get("/api/mobile/security")
def mobile_security():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400
    if not jquants_status():
        return jsonify(
            status="unavailable",
            reason="J-Quants API key is not configured",
        )
    try:
        info = security_info(code)
        if not info.get("name"):
            return jsonify(
                status="not_found",
                code=code,
                reason="security name was not found",
            ), 404
        return jsonify(status="ok", **info)
    except Exception as e:
        return jsonify(
            status="error",
            code=code,
            reason=str(e),
        ), 502


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
        company = security_info(code)
        data = jq_request("/equities/bars/daily", {"code": jq_code})
        rows = normalize_quote_rows(data)
        snap = technical_snapshot(rows)
        return jsonify(
            status="ok",
            code=code,
            jquants_code=jq_code,
            company=company,
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
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KLnByaW9yaXR5LWNvZGV7d2hpdGUtc3BhY2U6bm93cmFwO2ZvbnQtd2VpZ2h0OjgwMH0KCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5wb3J0cm93e2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyfX0KCgoKQG1lZGlhKG1heC13aWR0aDo0ODBweCl7LmdyaWQze2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyIDFmcn0ua3BpIGJ7Zm9udC1zaXplOjE2cHh9fQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8bWFpbj4KPGRpdiBjbGFzcz0idG9wIj4KICA8aDE+8J+TiCDml6XmnKzmoKpBSSBGUkVFPC9oMT4KICA8ZGl2IGNsYXNzPSJzdWIiPkotUXVhbnRz5a6f44OH44O844K/IC8g5LuK5pel44Gu5YSq5YWI44Ki44Kv44K344On44OzIC8g44Od44O844OI44OV44Kp44Oq44KqIC8g5L+d5pyJ5Yik5patPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfjq8g6YqY5p+E5YiG5p6QPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkIj4KICAgIDxpbnB1dCBpZD0iY29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIOS+iyA3MjAzIj4KICAgIDxpbnB1dCBpZD0icHJpY2UiIHBsYWNlaG9sZGVyPSLlj5blvpfntYLlgKQiIHJlYWRvbmx5PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbXBhbnlOYW1lIiBjbGFzcz0ic291cmNlIG11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBmeOCi+OBqOS8muekvuWQjeOCkuihqOekuuOBl+OBvuOBmTwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8YSBpZD0ia2FidXRhbiIgY2xhc3M9ImJ0biBzZWNvbmRhcnkiIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7moKrmjqLjgafnorroqo08L2E+CiAgICA8YnV0dG9uIGlkPSJhbmFseXplQnRuIiBvbmNsaWNrPSJhbmFseXplKCkiPuWun+ODh+ODvOOCv+OBp+WIhuaekDwvYnV0dG9uPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl44KM44Gm5oq844GZ44Go44CBSi1RdWFudHPjgYvjgonlj5blvpfjgafjgY3jgovlrp/jg4fjg7zjgr/jgpLoh6rli5XlhaXlipvjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBpZD0ic291cmNlQm94IiBjbGFzcz0ic291cmNlIG11dGVkIj7jg4fjg7zjgr/mnKrlj5blvpc8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+TiiDmoKrkvqHjg7vjg4bjgq/jg4vjgqvjg6vlrp/nuL48L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemosOiQveeOhyAlPC9zcGFuPjxpbnB1dCBpZD0icjIwIiByZWFkb25seT48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4xMjbml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIxMjYiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjI1MuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjI1MiIgcmVhZG9ubHk+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6Xpq5jlgKQ8L3NwYW4+PGIgaWQ9ImhpZ2gyMCI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel5a6J5YCkPC9zcGFuPjxiIGlkPSJsb3cyMCI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel5bm0546H44Oc44OpPC9zcGFuPjxiIGlkPSJ2b2wyMCI+4oCUPC9iPjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn6epIOijnOWKqeipleS+oTwvaDM+CiAgPHAgY2xhc3M9Im11dGVkIj7liIbjgYvjgovpoIXnm67jgaDjgZHlhaXlipvjgILmnKrlhaXlipvjga8w54K544Gn44Gv44Gq44GP44CM5LiN5piO44CN44Go44GX44Gm5omx44GE44G+44GZ44CCPC9wPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpcgLTEwMOOAnDEwMDwvc3Bhbj48aW5wdXQgaWQ9ImVhcm4iIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5pS/562WIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJwb2xpY3kiIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZyA57WmIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJzdXBwbHkiIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6Ag5YiG5p6Q57WQ5p6cPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+54q25oWLPC9zcGFuPjxiIGlkPSJzdGF0ZSI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODl+ODqeOCueagueaLoDwvc3Bhbj48YiBpZD0icG9zIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44Oe44Kk44OK44K55qC55ougPC9zcGFuPjxiIGlkPSJuZWciPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KICA8cCBpZD0icmVzdWx0IiBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOAjOWun+ODh+ODvOOCv+OBp+WIhuaekOOAjeOCkuaKvOOBl+OBpuOBj+OBoOOBleOBhOOAgjwvcD4KICA8ZGl2IGlkPSJhbmFseXNpc0V2IiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfmqgg5LuK5pel44Gu5YSq5YWI44Ki44Kv44K344On44OzPC9oMz4KICA8ZGl2IGlkPSJwcmlvcml0eUFjdGlvbnMiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCBwb3J0Zm9saW8iPgogIDxoMz7wn6etIOODneODvOODiOODleOCqeODquOCquWFqOS9kzwvaDM+CiAgPGRpdiBpZD0icG9ydGZvbGlvU3VtbWFyeSI+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOiHquWLlembhuioiOOBl+OBvuOBmeOAgjwvcD4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+SvCDkv53mnInmoKrjg7vmkI3liIfjgoov5Yip56K6PC9oMz4KICA8ZGl2IGNsYXNzPSJub3RlIj4KICAgIOaQjeWIh+OCiuODu+WIqeeiuuODu+ODiOODrOODvOODquODs+OCsOOBr+WPguiAg+ODqeOCpOODs+OAguS/neacieWIpOaWreOBr+Wun+ODh+ODvOOCv+OBqOioreWumuODqeOCpOODs+OBruODq+ODvOODq+WIpOWumuOBp+OBmeOAguefreS4remVt+OBr+mBjuWOu+OBruODreODvOODquODs+OCsOWun+e4vuWIhuW4g+OCkue1seioiOWPguiAg+OBqOOBl+OBpuihqOekuuOBl+OBvuOBmeOAggogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPgogICAgPGlucHV0IGlkPSJob2xkQ29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxpbnB1dCBpZD0iaG9sZENvc3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgcGxhY2Vob2xkZXI9IuWPluW+l+WNmOS+oSI+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NnB4IDJweCAwIj7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGlucHV0IGlkPSJob2xkU2hhcmVzIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLmoKrmlbAiPgogICAgPHNlbGVjdCBpZD0iZmVlTW9kZSI+CiAgICAgIDxvcHRpb24gdmFsdWU9Im5vbXVyYV9uZXQiPumHjuadkeOCquODs+ODqeOCpOODs+WwgueUqOaUr+W6l+ODu+ePvueJqTwvb3B0aW9uPgogICAgICA8b3B0aW9uIHZhbHVlPSJub25lIj7miYvmlbDmlpnjgarjgZfvvIjmr5TovIPnlKjvvIk8L29wdGlvbj4KICAgIDwvc2VsZWN0PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiiAlPC9zcGFuPjxpbnB1dCBpZD0ic3RvcFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iOCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K6ICU8L3NwYW4+PGlucHV0IGlkPSJ0YWtlUGN0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHZhbHVlPSIxNSI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844OrICU8L3NwYW4+PGlucHV0IGlkPSJ0cmFpbFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iNyI+PC9kaXY+CiAgPC9kaXY+CiAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRIb2xkaW5nKCkiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPuWun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmDwvYnV0dG9uPgogIDxwIGNsYXNzPSJtdXRlZCI+6YeO5p2R44ON44OD44OI77yG44Kz44O844Or77yP44G744Gj44Go44OA44Kk44Os44Kv44OI44Gu5Zu95YaF54++54mp44O744Kq44Oz44Op44Kk44Oz5rOo5paH44Gu56iO6L685omL5pWw5paZ6KGo44KS5L2/55So44CCPC9wPgogIDxkaXYgaWQ9ImhvbGRpbmdzIj48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+RgCDjgqbjgqnjg4Pjg4Hjg6rjgrnjg4g8L2gzPgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8aW5wdXQgaWQ9IndhdGNoQ29kZSIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8YnV0dG9uIG9uY2xpY2s9ImFkZFdhdGNoKCkiPui/veWKoDwvYnV0dG9uPgogIDwvZGl2PgogIDxkaXYgaWQ9IndhdGNoQ29tcGFueU5hbWUiIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbjo0cHggMnB4IDhweCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBpZD0id2F0Y2hzIj48L2Rpdj4KPC9kaXY+Cgo8c2NyaXB0Pgpjb25zdCAkPXg9PmRvY3VtZW50LmdldEVsZW1lbnRCeUlkKHgpOwpmdW5jdGlvbiB2YWwoaWQpe2xldCB2PSQoaWQpLnZhbHVlLnRyaW0oKTtyZXR1cm4gdj09PScnP251bGw6TnVtYmVyKHYpfQpmdW5jdGlvbiBsb2NhbChrKXt0cnl7cmV0dXJuIEpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oayl8fCdbXScpfWNhdGNoKGUpe3JldHVybltdfX0KZnVuY3Rpb24gc2F2ZShrLHYpe2xvY2FsU3RvcmFnZS5zZXRJdGVtKGssSlNPTi5zdHJpbmdpZnkodikpfQpmdW5jdGlvbiBmbXQodixkPTIpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZChkKX0KZnVuY3Rpb24geWVuKHYpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzonwqUnK01hdGgucm91bmQoTnVtYmVyKHYpKS50b0xvY2FsZVN0cmluZygnamEtSlAnKX0KZnVuY3Rpb24gc3RhdGVKYShzKXtyZXR1cm4gcz09PSdwb3NpdGl2ZSc/J+ODl+ODqeOCueWEquWLoic6cz09PSduZWdhdGl2ZSc/J+ODnuOCpOODiuOCueWEquWLoic6J+aLruaKlyd9CmZ1bmN0aW9uIHBjdCh2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoMikrJyUnfQpmdW5jdGlvbiBzdGF0Rm9yKGgsa2V5KXtyZXR1cm4gaCYmaC5mb3J3YXJkX3N0YXRzJiZoLmZvcndhcmRfc3RhdHNba2V5XT9oLmZvcndhcmRfc3RhdHNba2V5XTpudWxsfQpmdW5jdGlvbiBjb25maWRlbmNlRm9yKGgpewogIGNvbnN0IHZhbHM9W2gucmV0dXJuXzIwZCxoLnJldHVybl8xMjZkLGgucmV0dXJuXzI1MmRdLm1hcChOdW1iZXIpLmZpbHRlcihOdW1iZXIuaXNGaW5pdGUpOwogIGNvbnN0IHN0YXRzPVsnMjBkJywnMTI2ZCcsJzI1MmQnXS5tYXAoaz0+c3RhdEZvcihoLGspKS5maWx0ZXIocz0+cyYmcy5zdGF0dXM9PT0nb2snKTsKCiAgaWYoIU51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSl8fCFoLmFzb2YpcmV0dXJuIDA7CgogIGNvbnN0IGNvdmVyYWdlPXZhbHMubGVuZ3RoLzM7CiAgbGV0IGFncmVlbWVudD0wLjU7CiAgaWYodmFscy5sZW5ndGgpewogICAgY29uc3QgcG9zPXZhbHMuZmlsdGVyKHg9Png+MCkubGVuZ3RoOwogICAgY29uc3QgbmVnPXZhbHMuZmlsdGVyKHg9Png8MCkubGVuZ3RoOwogICAgYWdyZWVtZW50PU1hdGgubWF4KHBvcyxuZWcpL3ZhbHMubGVuZ3RoOwogIH0KICBjb25zdCBzdGF0Q292ZXJhZ2U9c3RhdHMubGVuZ3RoLzM7CgogIHJldHVybiBNYXRoLnJvdW5kKChjb3ZlcmFnZSowLjQ1ICsgYWdyZWVtZW50KjAuMjUgKyBzdGF0Q292ZXJhZ2UqMC4zMCkqMTAwKTsKfQoKZnVuY3Rpb24gZGVjaXNpb25Gb3IoaCxjKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFoLmFzb2YpewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVmScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjon5a6f44OH44O844K/5pyq5Y+W5b6X44CC5Y+z5LiK44Gu44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgY/jgaDjgZXjgYTjgIInLAogICAgICBjb25maWRlbmNlOjAKICAgIH07CiAgfQoKICBjb25zdCByMjA9TnVtYmVyKGgucmV0dXJuXzIwZCksIHIxMjY9TnVtYmVyKGgucmV0dXJuXzEyNmQpLCByMjUyPU51bWJlcihoLnJldHVybl8yNTJkKTsKICBjb25zdCBjb25maWRlbmNlPWNvbmZpZGVuY2VGb3IoaCk7CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5pCN5YiH44KK5qSc6KiOJyxjbHM6J2Qtc3RvcCcscmVhc29uOifoqK3lrprjgZfjgZ/mkI3liIfjgorlj4LogIPjg6njgqTjg7Pku6XkuIsnLGNvbmZpZGVuY2V9OwogIH0KICBpZihjdXI+PWMudGFrZVByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+WIqeeiuuaknOiojicsY2xzOidkLXRha2UnLHJlYXNvbjon6Kit5a6a44GX44Gf5Yip56K65Y+C6ICD44Op44Kk44Oz5Lul5LiKJyxjb25maWRlbmNlfTsKICB9CiAgaWYoY3VyPD1jLnRyYWlsUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon6K2m5oiSJyxjbHM6J2Qtd2F0Y2gnLHJlYXNvbjonMjDml6Xpq5jlgKTln7rmupbjga7jg4jjg6zjg7zjg6rjg7PjgrDlj4LogIPjg6njgqTjg7Pku6XkuIsnLGNvbmZpZGVuY2V9OwogIH0KCiAgbGV0IHBvc2l0aXZlPTAsIG5lZ2F0aXZlPTA7CiAgW3IyMCxyMTI2LHIyNTJdLmZvckVhY2goeD0+ewogICAgaWYoTnVtYmVyLmlzRmluaXRlKHgpKXsKICAgICAgaWYoeD4wKXBvc2l0aXZlKys7CiAgICAgIGlmKHg8MCluZWdhdGl2ZSsrOwogICAgfQogIH0pOwoKICBpZihuZWdhdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpeODuzEyNuaXpeODuzI1MuaXpeOBruOBhuOBoeODnuOCpOODiuOCueWCvuWQkeOBjOWEquWLoicsY29uZmlkZW5jZX07CiAgfQogIGlmKHBvc2l0aXZlPj0yKXsKICAgIHJldHVybiB7bGFiZWw6J+S/neaciee2mee2micsY2xzOidkLWhvbGQnLHJlYXNvbjon6Kit5a6a44Op44Kk44Oz5YaF44Gn44CB6KSH5pWw5pyf6ZaT44Gu5L6h5qC844OI44Os44Oz44OJ44GM44OX44Op44K5Jyxjb25maWRlbmNlfTsKICB9CiAgcmV0dXJuIHtsYWJlbDon5L+d5pyJ57aZ57aa77yI5qeY5a2Q6KaL77yJJyxjbHM6J2QtaG9sZCcscmVhc29uOifoqK3lrprjg6njgqTjg7PlhoXjgILmnJ/plpPliKXjg4jjg6zjg7Pjg4njga/lvLflvLHjgYzmt7flnKgnLGNvbmZpZGVuY2V9Owp9CmZ1bmN0aW9uIGV2SHRtbCh0aXRsZSxzKXsKICBpZighc3x8cy5zdGF0dXMhPT0nb2snKXsKICAgIGNvbnN0IG49cyYmcy5uIT09dW5kZWZpbmVkP3MubjowOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7jg4fjg7zjgr/kuI3otrM8L2I+PHNtYWxsPuaomeacrCAke2595Lu2PC9zbWFsbD48L2Rpdj5gOwogIH0KICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+CiAgICA8Yj7lubPlnYcgJHtwY3Qocy5tZWFuKX08L2I+CiAgICA8c21hbGw+5Lit5aSu5YCkICR7cGN0KHMubWVkaWFuKX08L3NtYWxsPgogICAgPHNtYWxsPuS4iuaYh+eOhyAke3BjdChzLnBvc2l0aXZlX3JhdGUpfTwvc21hbGw+CiAgICA8c21hbGw+UDEw44CcUDkwICR7cGN0KHMucDEwKX0g44CcICR7cGN0KHMucDkwKX08L3NtYWxsPgogICAgPHNtYWxsPuaomeacrCAke3Mubn3ku7Y8L3NtYWxsPgogIDwvZGl2PmA7Cn0KCgpmdW5jdGlvbiBkaXN0YW5jZUluZm8oY3VyLHRhcmdldCxraW5kKXsKICBjdXI9TnVtYmVyKGN1cik7IHRhcmdldD1OdW1iZXIodGFyZ2V0KTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IU51bWJlci5pc0Zpbml0ZSh0YXJnZXQpKXJldHVybiAn4oCUJzsKICBjb25zdCBkaWZmPSh0YXJnZXQvY3VyLTEpKjEwMDsKICBpZihraW5kPT09J3N0b3AnKXsKICAgIGlmKGRpZmY+PTApcmV0dXJuICfjg6njgqTjg7PliLDpgZTmuIjjgb8nOwogICAgcmV0dXJuIE1hdGguYWJzKGRpZmYpLnRvRml4ZWQoMikrJyUg5LiLJzsKICB9CiAgaWYoa2luZD09PSd0YWtlJyl7CiAgICBpZihkaWZmPD0wKXJldHVybiAn44Op44Kk44Oz5Yiw6YGU5riI44G/JzsKICAgIHJldHVybiBkaWZmLnRvRml4ZWQoMikrJyUg5LiKJzsKICB9CiAgcmV0dXJuIChkaWZmPj0wPycrJzonJykrZGlmZi50b0ZpeGVkKDIpKyclJzsKfQoKZnVuY3Rpb24gcHJpY2VSYW5nZShjdXIscyl7CiAgY3VyPU51bWJlcihjdXIpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhc3x8cy5zdGF0dXMhPT0nb2snKXJldHVybiBudWxsOwogIHJldHVybiB7CiAgICBsb3c6Y3VyKigxK051bWJlcihzLnAxMCkvMTAwKSwKICAgIGhpZ2g6Y3VyKigxK051bWJlcihzLnA5MCkvMTAwKSwKICAgIG1lZGlhbjpjdXIqKDErTnVtYmVyKHMubWVkaWFuKS8xMDApCiAgfTsKfQoKZnVuY3Rpb24gcmFuZ2VIdG1sKHRpdGxlLGN1cixzKXsKICBjb25zdCByPXByaWNlUmFuZ2UoY3VyLHMpOwogIGlmKCFyKXsKICAgIGNvbnN0IG49cyYmcy5uIT09dW5kZWZpbmVkP3MubjowOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJyYW5nZWJveCI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7jg4fjg7zjgr/kuI3otrM8L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7mqJnmnKwgJHtufeS7tjwvc3Bhbj48L2Rpdj5gOwogIH0KICByZXR1cm4gYDxkaXYgY2xhc3M9InJhbmdlYm94Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX0gUDEw44CcUDkwPC9zcGFuPgogICAgPGI+JHt5ZW4oci5sb3cpfSDjgJwgJHt5ZW4oci5oaWdoKX08L2I+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuS4reWkruWApOaPm+eulyAke3llbihyLm1lZGlhbil9PC9zcGFuPgogIDwvZGl2PmA7Cn0KCmZ1bmN0aW9uIGFjdGlvblRleHQoaCxjLGQpewogIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZJylyZXR1cm4gJ+OBvuOBmuWun+ODh+ODvOOCv+OCkuabtOaWsOOBl+OBpuOBi+OCieWIpOaWreOAgic7CiAgaWYoZC5sYWJlbD09PSfmkI3liIfjgormpJzoqI4nKXJldHVybiAn5pCN5YiH44KK5Y+C6ICD44Op44Kk44Oz44KS5LiL5Zue44Gj44Gm44GE44G+44GZ44CC5a6f6Zqb44Gu54++5Zyo5YCk44Go5rOo5paH5p2h5Lu244KS56K66KqN44GX44Gm44CB57iu5bCP44O75pKk6YCA44KS5qSc6KiO44CCJzsKICBpZihkLmxhYmVsPT09J+WIqeeiuuaknOiojicpcmV0dXJuICfliKnnorrlj4LogIPjg6njgqTjg7PjgavliLDpgZTjgZfjgabjgYTjgb7jgZnjgILlhajpg6jlo7LljbTjgaDjgZHjgafjgarjgY/jgIHliIblibLliKnnorrjgoLlgJnoo5zjgIInOwogIGlmKGQubGFiZWw9PT0n6K2m5oiSJylyZXR1cm4gJ+itpuaIkuOCvuODvOODs+OAguODiOODrOODvOODquODs+OCsOODqeOCpOODs+OBqOS4reefreacn+OBruWApOWLleOBjeOCkuWEquWFiOOBl+OBpueiuuiqjeOAgic7CiAgcmV0dXJuICfoqK3lrprjg6njgqTjg7PlhoXjgILkv53mnInntpnntprlgJnoo5zjgafjgZnjgYzjgIHnhKHmlpnjg4fjg7zjgr/jga/pgYXlu7bjgZnjgovjgZ/jgoHlrp/pmpvjga7nj77lnKjlgKTjgoLnorroqo3jgIInOwp9CgoKZnVuY3Rpb24gbm9tdXJhTmV0RmVlKGFtb3VudCl7CiAgYW1vdW50PU51bWJlcihhbW91bnR8fDApOwogIGlmKGFtb3VudDw9MClyZXR1cm4gMDsKICBpZihhbW91bnQ8PTEwMDAwMClyZXR1cm4gMTUyOwogIGlmKGFtb3VudDw9MzAwMDAwKXJldHVybiAzMzA7CiAgaWYoYW1vdW50PD01MDAwMDApcmV0dXJuIDUyNDsKICBpZihhbW91bnQ8PTEwMDAwMDApcmV0dXJuIDEwNDg7CiAgaWYoYW1vdW50PD0yMDAwMDAwKXJldHVybiAyMDk1OwogIGlmKGFtb3VudDw9MzAwMDAwMClyZXR1cm4gMzE0MzsKICBpZihhbW91bnQ8PTUwMDAwMDApcmV0dXJuIDUyMzg7CiAgaWYoYW1vdW50PD0xMDAwMDAwMClyZXR1cm4gMTA0NzY7CiAgaWYoYW1vdW50PD0yMDAwMDAwMClyZXR1cm4gMjA5NTI7CiAgaWYoYW1vdW50PD0zMDAwMDAwMClyZXR1cm4gMzE0Mjk7CiAgaWYoYW1vdW50PD01MDAwMDAwMClyZXR1cm4gNDE5MDU7CiAgcmV0dXJuIDc4NTcxOwp9CmZ1bmN0aW9uIGZlZUZvcihhbW91bnQsbW9kZSl7cmV0dXJuIG1vZGU9PT0nbm9tdXJhX25ldCc/bm9tdXJhTmV0RmVlKGFtb3VudCk6MH0KCmZ1bmN0aW9uIGNhbGNIb2xkaW5nKGgpewogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBjb25zdCBjb3N0PU51bWJlcihoLmNvc3QpOwogIGNvbnN0IHNoYXJlcz1OdW1iZXIoaC5zaGFyZXMpOwogIGNvbnN0IGJ1eVZhbHVlPWNvc3Qqc2hhcmVzOwogIGNvbnN0IGJ1eUZlZT1mZWVGb3IoYnV5VmFsdWUsaC5mZWVfbW9kZSk7CiAgY29uc3QgY3VycmVudFZhbHVlPWN1cipzaGFyZXM7CiAgY29uc3Qgc2VsbEZlZT1mZWVGb3IoY3VycmVudFZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGludmVzdGVkPWJ1eVZhbHVlK2J1eUZlZTsKICBjb25zdCBuZXROb3c9Y3VycmVudFZhbHVlLXNlbGxGZWUtaW52ZXN0ZWQ7CiAgY29uc3QgbmV0Tm93UGN0PWludmVzdGVkP25ldE5vdy9pbnZlc3RlZCoxMDA6bnVsbDsKCiAgY29uc3Qgc3RvcFByaWNlPWNvc3QqKDEtTnVtYmVyKGguc3RvcF9wY3QpLzEwMCk7CiAgY29uc3QgdGFrZVByaWNlPWNvc3QqKDErTnVtYmVyKGgudGFrZV9wY3QpLzEwMCk7CiAgY29uc3QgaGlnaDIwPU51bWJlcihoLmhpZ2hfMjBkfHxjdXIpOwogIGNvbnN0IHRyYWlsUHJpY2U9aGlnaDIwKigxLU51bWJlcihoLnRyYWlsX3BjdCkvMTAwKTsKCiAgY29uc3Qgc3RvcFZhbHVlPXN0b3BQcmljZSpzaGFyZXM7CiAgY29uc3QgdGFrZVZhbHVlPXRha2VQcmljZSpzaGFyZXM7CiAgY29uc3Qgc3RvcE5ldD1zdG9wVmFsdWUtZmVlRm9yKHN0b3BWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKICBjb25zdCB0YWtlTmV0PXRha2VWYWx1ZS1mZWVGb3IodGFrZVZhbHVlLGguZmVlX21vZGUpLWludmVzdGVkOwoKICByZXR1cm4ge2J1eVZhbHVlLGJ1eUZlZSxjdXJyZW50VmFsdWUsc2VsbEZlZSxpbnZlc3RlZCxuZXROb3csbmV0Tm93UGN0LHN0b3BQcmljZSx0YWtlUHJpY2UsdHJhaWxQcmljZSxzdG9wTmV0LHRha2VOZXR9Owp9CgoKY29uc3QgbmFtZVRpbWVycz17fTsKY29uc3QgbmFtZUNhY2hlPXt9OwoKZnVuY3Rpb24gZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4PScnKXsKICBpZighdGFyZ2V0KXJldHVybjsKICBpZighaW5mb3x8IWluZm8ubmFtZSl7CiAgICB0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nOwogICAgcmV0dXJuOwogIH0KICBsZXQgZXh0cmFzPVtdOwogIGlmKGluZm8ubWFya2V0KWV4dHJhcy5wdXNoKGluZm8ubWFya2V0KTsKICBpZihpbmZvLnNlY3RvcjMzKWV4dHJhcy5wdXNoKGluZm8uc2VjdG9yMzMpOwogIHRhcmdldC5pbm5lckhUTUw9JzxiPicrcHJlZml4K2luZm8ubmFtZSsnPC9iPicrKGV4dHJhcy5sZW5ndGg/Jzxicj48c3BhbiBjbGFzcz0ibXV0ZWQiPicrZXh0cmFzLmpvaW4oJyAvICcpKyc8L3NwYW4+JzonJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldENvbXBhbnkoY29kZSl7CiAgY29uc3QgYz1TdHJpbmcoY29kZXx8JycpLnRyaW0oKTsKICBpZighYylyZXR1cm4gbnVsbDsKICBpZihuYW1lQ2FjaGVbY10pcmV0dXJuIG5hbWVDYWNoZVtjXTsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9zZWN1cml0eT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn6YqY5p+E5ZCN44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgbmFtZUNhY2hlW2NdPXg7CiAgcmV0dXJuIHg7Cn0KCmZ1bmN0aW9uIHNjaGVkdWxlQ29tcGFueUxvb2t1cChpbnB1dElkLHRhcmdldElkLHByZWZpeD0nJyl7CiAgY2xlYXJUaW1lb3V0KG5hbWVUaW1lcnNbaW5wdXRJZF0pOwogIGNvbnN0IGM9JChpbnB1dElkKS52YWx1ZS50cmltKCk7CiAgY29uc3QgdGFyZ2V0PSQodGFyZ2V0SWQpOwoKICBpZihjLmxlbmd0aDw0KXsKICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrigJQnOwogICAgcmV0dXJuOwogIH0KCiAgbmFtZVRpbWVyc1tpbnB1dElkXT1zZXRUaW1lb3V0KGFzeW5jKCk9PnsKICAgIHRyeXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD0n6YqY5p+E5ZCN44KS56K66KqN5Lit4oCmJzsKICAgICAgY29uc3QgaW5mbz1hd2FpdCBnZXRDb21wYW55KGMpOwogICAgICBkaXNwbGF5Q29tcGFueSh0YXJnZXQsaW5mbyxwcmVmaXgpOwogICAgfWNhdGNoKGUpewogICAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIH0KICB9LDQ1MCk7Cn0KCgoKZnVuY3Rpb24gcHJpb3JpdHlBY3Rpb25Gb3IoaCl7CiAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgY29uc3QgbmFtZT1oLmNvbXBhbnlfbmFtZXx8Jyc7CiAgY29uc3QgbGFiZWw9KGguY29kZXx8JycpKyhuYW1lPycgJytuYW1lOicnKTsKICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CgogIGlmKCF2YWxpZCl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NiwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+Wun+ODh+ODvOOCv+OCkuabtOaWsCcsCiAgICAgIGRldGFpbDon5pyA5paw5Y+W5b6X57WC5YCk44GM44GC44KK44G+44Gb44KT44CC44G+44Ga44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgIInLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IHN0b3BEaXN0PShjdXIvYy5zdG9wUHJpY2UtMSkqMTAwOwogIGNvbnN0IHRha2VEaXN0PShjLnRha2VQcmljZS9jdXItMSkqMTAwOwogIGNvbnN0IHRyYWlsRGlzdD0oY3VyL2MudHJhaWxQcmljZS0xKSoxMDA7CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6MTAwLAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5pCN5YiH44KK44Op44Kk44Oz5Yiw6YGUJywKICAgICAgZGV0YWlsOmDmnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4oY3VyKX0gLyDmkI3liIfjgorlj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoc3RvcERpc3Q8PTMpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTQsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOOBguOBqCAke3N0b3BEaXN0LnRvRml4ZWQoMil9JSDjgafmkI3liIfjgorlj4LogIPjg6njgqTjg7NgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKGN1cj49Yy50YWtlUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTAsCiAgICAgIGNsczoncHJpb3JpdHktdGFrZScsCiAgICAgIHRpdGxlOifliKnnorrjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOWIqeeiuuWPguiAgyAke3llbihjLnRha2VQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZih0YWtlRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo4NCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7dGFrZURpc3QudG9GaXhlZCgyKX0lIOOBp+WIqeeiuuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoZC5sYWJlbD09PSforabmiJInKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjgwLAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOiforabmiJLliKTlrponLAogICAgICBkZXRhaWw6ZC5yZWFzb24sCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoTnVtYmVyLmlzRmluaXRlKHRyYWlsRGlzdCkmJnRyYWlsRGlzdDw9Mi41KXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjc2LAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOifjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOODiOODrOODvOODquODs+OCsOWPguiAgyAke3llbihjLnRyYWlsUHJpY2UpfSDjgb7jgacgJHt0cmFpbERpc3QudG9GaXhlZCgyKX0lYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICByZXR1cm4gewogICAgc2NvcmU6MzAsCiAgICBjbHM6J3ByaW9yaXR5LWdvb2QnLAogICAgdGl0bGU6J+mAmuW4uOebo+imlicsCiAgICBkZXRhaWw6ZC5yZWFzb258fCfoqK3lrprjg6njgqTjg7PlhoUnLAogICAgbGFiZWwKICB9Owp9CgpmdW5jdGlvbiByZW5kZXJQcmlvcml0eUFjdGlvbnMoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwcmlvcml0eUFjdGlvbnMnKTsKICBpZighYm94KXJldHVybjsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go44CB5YSq5YWI44GX44Gm56K66KqN44GZ44KL6YqY5p+E44KS6Ieq5YuV6KGo56S644GX44G+44GZ44CCPC9wPic7CiAgICByZXR1cm47CiAgfQoKICBsZXQgYWN0aW9ucz1hLm1hcChwcmlvcml0eUFjdGlvbkZvcik7CgogIC8vIENvbmNlbnRyYXRpb24gYWxlcnQgKHBvcnRmb2xpby1sZXZlbCkKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wJiZoLmFzb2YpOwogIGNvbnN0IHRvdGFsPXZhbGlkLnJlZHVjZSgocyxoKT0+cytOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApLDApOwogIGlmKHRvdGFsPjApewogICAgbGV0IG1heEhvbGRpbmc9bnVsbCwgbWF4VmFsdWU9MDsKICAgIHZhbGlkLmZvckVhY2goaD0+ewogICAgICBjb25zdCB2PU51bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCk7CiAgICAgIGlmKHY+bWF4VmFsdWUpe21heFZhbHVlPXY7bWF4SG9sZGluZz1ofQogICAgfSk7CiAgICBjb25zdCBjb25jZW50cmF0aW9uPW1heFZhbHVlL3RvdGFsKjEwMDsKICAgIGlmKGNvbmNlbnRyYXRpb24+PTYwICYmIG1heEhvbGRpbmcpewogICAgICBhY3Rpb25zLnB1c2goewogICAgICAgIHNjb3JlOjcyLAogICAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgICB0aXRsZTon6ZuG5Lit5bqm44KS56K66KqNJywKICAgICAgICBkZXRhaWw6YCR7bWF4SG9sZGluZy5jb2RlfSR7bWF4SG9sZGluZy5jb21wYW55X25hbWU/JyAnK21heEhvbGRpbmcuY29tcGFueV9uYW1lOicnfSDjgYzjg53jg7zjg4jjg5Xjgqnjg6rjgqrjga4gJHtjb25jZW50cmF0aW9uLnRvRml4ZWQoMSl9JWAsCiAgICAgICAgbGFiZWw6J+ODneODvOODiOODleOCqeODquOCqicKICAgICAgfSk7CiAgICB9CiAgfQoKICBhY3Rpb25zLnNvcnQoKHgseSk9Pnkuc2NvcmUteC5zY29yZSk7CgogIGNvbnN0IGltcG9ydGFudD1hY3Rpb25zLmZpbHRlcih4PT54LnNjb3JlPj03MCk7CiAgY29uc3Qgc2hvd249KGltcG9ydGFudC5sZW5ndGg/aW1wb3J0YW50OmFjdGlvbnMpLnNsaWNlKDAsNCk7CgogIGJveC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9InByaW9yaXR5LXdyYXAiPiR7CiAgICBzaG93bi5tYXAoKHgsaSk9PmAKICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktaXRlbSAke3guY2xzfSI+CiAgICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktbGluZSI+CiAgICAgICAgICA8ZGl2PgogICAgICAgICAgICA8c3BhbiBjbGFzcz0icHJpb3JpdHktcmFuayI+UFJJT1JJVFkgJHtpKzF9PC9zcGFuPgogICAgICAgICAgICA8Yj4ke3gudGl0bGV9PC9iPgogICAgICAgICAgPC9kaXY+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1jb2RlIj4ke3gubGFiZWx9PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjVweCI+JHt4LmRldGFpbH08L2Rpdj4KICAgICAgPC9kaXY+CiAgICBgKS5qb2luKCcnKQogIH08L2Rpdj5gICsgKAogICAgaW1wb3J0YW50Lmxlbmd0aAogICAgICA/ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+4oC75YSq5YWI5bqm44Gv6Kit5a6a44Op44Kk44Oz5o6l6L+R44O75Yik5a6a54q25oWL44O744OH44O844K/5pyJ54Sh44O76ZuG5Lit5bqm44GL44KJ5L2c44KL56K66KqN6aCG44Gn44GZ44CC6Ieq5YuV5aOy6LK35oyH56S644Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPicKICAgICAgOiAnPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPue3iuaApeW6puOBrumrmOOBhOmgheebruOBr+OBguOCiuOBvuOBm+OCk+OAgumAmuW4uOebo+imluOCkue2mee2muOAgjwvcD4nCiAgKTsKfQoKZnVuY3Rpb24gcG9ydGZvbGlvTWVhbkZvcihhLGtleSl7CiAgY29uc3QgdmFsaWQ9YS5maWx0ZXIoaD0+TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCk7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw8PTApcmV0dXJuIG51bGw7CgogIGxldCBudW09MCwgZGVuPTA7CiAgdmFsaWQuZm9yRWFjaChoPT57CiAgICBjb25zdCBzdD1zdGF0Rm9yKGgsa2V5KTsKICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGlmKHN0JiZzdC5zdGF0dXM9PT0nb2snJiZOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHN0Lm1lYW4pKSYmdj4wKXsKICAgICAgbnVtICs9IHYqTnVtYmVyKHN0Lm1lYW4pOwogICAgICBkZW4gKz0gdjsKICAgIH0KICB9KTsKICByZXR1cm4gZGVuPjA/bnVtL2RlbjpudWxsOwp9CgpmdW5jdGlvbiByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBib3g9JCgncG9ydGZvbGlvU3VtbWFyeScpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwogICAgcmV0dXJuOwogIH0KCiAgbGV0IHRvdGFsQ29zdD0wLCB0b3RhbFZhbHVlPTAsIHRvdGFsTmV0PTA7CiAgY29uc3Qgcm93cz1bXTsKICBjb25zdCBkZWNpc2lvbnM9e2hvbGQ6MCx3YXRjaDowLHRha2U6MCxzdG9wOjAscGVuZGluZzowfTsKCiAgYS5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogICAgY29uc3QgY3VycmVudFZhbHVlPXZhbGlkP2N1cipzaGFyZXM6MDsKICAgIGNvbnN0IGludmVzdGVkPU51bWJlcihoLmNvc3R8fDApKnNoYXJlcytjLmJ1eUZlZTsKCiAgICB0b3RhbENvc3QgKz0gaW52ZXN0ZWQ7CgogICAgaWYodmFsaWQpewogICAgICB0b3RhbFZhbHVlICs9IGN1cnJlbnRWYWx1ZTsKICAgICAgdG90YWxOZXQgKz0gYy5uZXROb3c7CgogICAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICAgIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylkZWNpc2lvbnMuc3RvcCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yip56K65qSc6KiOJylkZWNpc2lvbnMudGFrZSsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n6K2m5oiSJylkZWNpc2lvbnMud2F0Y2grKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmScpZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgZWxzZSBkZWNpc2lvbnMuaG9sZCsrOwoKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTpjdXJyZW50VmFsdWUsbmV0OmMubmV0Tm93fSk7CiAgICB9ZWxzZXsKICAgICAgZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTowLG5ldDpudWxsfSk7CiAgICB9CiAgfSk7CgogIGNvbnN0IG5ldFBjdD10b3RhbENvc3Q+MD90b3RhbE5ldC90b3RhbENvc3QqMTAwOm51bGw7CiAgY29uc3QgbWF4VmFsdWU9cm93cy5yZWR1Y2UoKG0scik9Pk1hdGgubWF4KG0sci52YWx1ZSksMCk7CiAgY29uc3QgY29uY2VudHJhdGlvbj10b3RhbFZhbHVlPjA/bWF4VmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKCiAgY29uc3QgbWVhbjIwPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjBkJyk7CiAgY29uc3QgbWVhbjEyNj1wb3J0Zm9saW9NZWFuRm9yKGEsJzEyNmQnKTsKICBjb25zdCBtZWFuMjUyPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjUyZCcpOwoKICBjb25zdCBhbGxvY2F0aW9ucz1yb3dzCiAgICAuZmlsdGVyKHI9PnIudmFsdWU+MCkKICAgIC5zb3J0KCh4LHkpPT55LnZhbHVlLXgudmFsdWUpCiAgICAubWFwKHI9PnsKICAgICAgY29uc3Qgdz10b3RhbFZhbHVlPjA/ci52YWx1ZS90b3RhbFZhbHVlKjEwMDowOwogICAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImFsbG9jIj4KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+PGI+JHtyLmNvZGV9PC9iPiR7ci5uYW1lPycgJytyLm5hbWU6Jyd9IC8gJHt3LnRvRml4ZWQoMSl9JTwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImFsbG9jYmFyIj48c3BhbiBzdHlsZT0id2lkdGg6JHtNYXRoLm1pbigxMDAsdyl9JSI+PC9zcGFuPjwvZGl2PgogICAgICA8L2Rpdj5gOwogICAgfSkuam9pbignJyk7CgogIGJveC5pbm5lckhUTUw9YAogICAgPGRpdiBjbGFzcz0icG9ydHJvdyI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+aKleizh+mhjTwvc3Bhbj48Yj4ke3llbih0b3RhbENvc3QpfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6KmV5L6h6aGNPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbFZhbHVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vOaQjeebijwvc3Bhbj48YiBjbGFzcz0iJHt0b3RhbE5ldD49MD8ncG9zJzonbmVnJ30iPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbE5ldCk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHtuZXRQY3Q9PT1udWxsPyfigJQnOm5ldFBjdC50b0ZpeGVkKDIpKyclJ308L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuacgOWkp+mKmOafhOavlOeOhzwvc3Bhbj48Yj4ke3RvdGFsVmFsdWU+MD9jb25jZW50cmF0aW9uLnRvRml4ZWQoMSkrJyUnOifigJQnfTwvYj48L2Rpdj4KICAgIDwvZGl2PgoKICAgIDxkaXYgY2xhc3M9InBvcnRyb3ciIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS/neaciee2mee2mjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy5ob2xkfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6K2m5oiSPC9zcGFuPjxiPiR7ZGVjaXNpb25zLndhdGNofTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K65qSc6KiOPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnRha2V9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoov5L+d55WZPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnN0b3ArZGVjaXNpb25zLnBlbmRpbmd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TiiDoqZXkvqHpoY3liqDph43jga7pgY7ljrvlubPlnYfjg6rjgr/jg7zjg7M8L2g0PgogICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+55+t5pyfMjDml6U8L3NwYW4+PGI+JHttZWFuMjA9PT1udWxsPyfigJQnOm1lYW4yMC50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7kuK3mnJ8xMjbml6U8L3NwYW4+PGI+JHttZWFuMTI2PT09bnVsbD8n4oCUJzptZWFuMTI2LnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPumVt+acnzI1MuaXpTwvc3Bhbj48Yj4ke21lYW4yNTI9PT1udWxsPyfigJQnOm1lYW4yNTIudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgPC9kaXY+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+WQhOmKmOafhOOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODs+OCkuePvuWcqOOBruipleS+oemhjeOBp+WKoOmHjeOBl+OBn+WPguiAg+WApOOBp+OBmeOAguebuOmWouOCkuiAg+aFruOBl+OBn+WwhuadpeS6iOa4rOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgNXB4Ij7wn5OmIOmKmOafhOani+aIkDwvaDQ+CiAgICAke2FsbG9jYXRpb25zfHwnPHAgY2xhc3M9Im11dGVkIj7lrp/jg4fjg7zjgr/mnKrlj5blvpc8L3A+J30KICBgOwogIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwp9CmZ1bmN0aW9uIHJlbmRlckhvbGRpbmdzKCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBpZighYS5sZW5ndGgpeyQoJ2hvbGRpbmdzJykuaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nO3JlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTtyZXR1cm59CiAgJCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9YS5tYXAoKGgsaSk9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCB2YWxpZFByaWNlPU51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZjsKICAgIGNvbnN0IGNscz12YWxpZFByaWNlPyhjLm5ldE5vdz49MD8ncG9zJzonbmVnJyk6Jyc7CiAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CgogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJob2xkaW5nIj4KICAgICAgPGRpdiBjbGFzcz0iaG9sZGluZy1oZWFkIj4KICAgICAgICA8ZGl2PgogICAgICAgICAgPGI+JHtoLmNvZGV9PC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPiR7aC5jb21wYW55X25hbWV8fCIifTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9InJvdyI+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hIb2xkaW5nKCR7aX0pIj7mm7TmlrA8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlSG9sZGluZygke2l9KSI+5YmK6ZmkPC9idXR0b24+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuODh+ODvOOCv+aXpSAke2guYXNvZnx8J+KAlCd9IC8g5pyA5paw5Y+W5b6X57WC5YCkICR7dmFsaWRQcmljZT95ZW4oaC5jdXJyZW50X3ByaWNlKTon4oCUJ30gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZGVjaXNpb24gJHtkLmNsc30iPgogICAgICAgICR7ZC5sYWJlbH0KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NHB4Ij4ke2QucmVhc29ufTwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuOBvuOBpzwvc3Bhbj4KICAgICAgICAgIDxiIGNsYXNzPSJkaXN0YW5jZSI+JHt2YWxpZFByaWNlP2Rpc3RhbmNlSW5mbyhjdXIsYy5zdG9wUHJpY2UsJ3N0b3AnKTon4oCUJ308L2I+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWPguiAgyAke3llbihjLnN0b3BQcmljZSl9PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuOBvuOBpzwvc3Bhbj4KICAgICAgICAgIDxiIGNsYXNzPSJkaXN0YW5jZSI+JHt2YWxpZFByaWNlP2Rpc3RhbmNlSW5mbyhjdXIsYy50YWtlUHJpY2UsJ3Rha2UnKTon4oCUJ308L2I+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWPguiAgyAke3llbihjLnRha2VQcmljZSl9PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWIpOWumuS/oemgvOW6pjwvc3Bhbj4KICAgICAgICAgIDxiPiR7ZC5jb25maWRlbmNlfSU8L2I+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJnYXVnZSI+PHNwYW4gc3R5bGU9IndpZHRoOiR7ZC5jb25maWRlbmNlfSUiPjwvc3Bhbj48L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjVweCI+4oC75Yik5a6a5L+h6aC85bqm44Gv44CB44OH44O844K/5YWF6Laz5bqm44O75pyf6ZaT44OI44Os44Oz44OJ44Gu5LiA6Ie05bqm44O75pyf5b6F5YCk57Wx6KiI44Gu5pyJ54Sh44GL44KJ5L2c44KL5Y+C6ICD5oyH5qiZ44Gn44CB55qE5Lit56K6546H44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgoKICAgICAgPGRpdiBjbGFzcz0iYWN0aW9uYm94Ij4KICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuS7iuOBqeOBhuOBmeOCi++8nzwvc3Bhbj4KICAgICAgICA8YiBzdHlsZT0iZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweCI+JHthY3Rpb25UZXh0KGgsYyxkKX08L2I+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vOaQjeebijwvc3Bhbj48YiBjbGFzcz0iJHtjbHN9Ij4ke3ZhbGlkUHJpY2U/eWVuKGMubmV0Tm93KTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3ZhbGlkUHJpY2U/Zm10KGMubmV0Tm93UGN0KSsnJSc6J+KAlCd9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7osrfku5jmiYvmlbDmlpk8L3NwYW4+PGI+JHt5ZW4oYy5idXlGZWUpfTwvYj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5aOy5Y205omL5pWw5paZKOS7iik8L3NwYW4+PGI+JHt2YWxpZFByaWNlP3llbihjLnNlbGxGZWUpOifigJQnfTwvYj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KK5Y+C6ICDPC9zcGFuPjxiPiR7eWVuKGMuc3RvcFByaWNlKX08L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrwgJHt5ZW4oYy5zdG9wTmV0KX08L3NwYW4+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnRha2VQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMudGFrZU5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg4jjg6zjg7zjg6rjg7PjgrDlj4LogIM8L3NwYW4+PGI+JHt2YWxpZFByaWNlP3llbihjLnRyYWlsUHJpY2UpOifigJQnfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6auY5YCk5Z+65rqWPC9zcGFuPjwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA3cHgiPvCfjq8g5a6f57i+44OZ44O844K55L6h5qC844Os44Oz44K4PC9oND4KICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICAgICR7cmFuZ2VIdG1sKCfnn63mnJ8gMjDml6UnLGN1cixzdGF0Rm9yKGgsJzIwZCcpKX0KICAgICAgICAke3JhbmdlSHRtbCgn5Lit5pyfIDEyNuaXpScsY3VyLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke3JhbmdlSHRtbCgn6ZW35pyfIDI1MuaXpScsY3VyLHN0YXRGb3IoaCwnMjUyZCcpKX0KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn5OQIOWun+e4vuODmeODvOOCueacn+W+heWApO+8iOe1seioiOWPguiAg++8iTwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke2V2SHRtbCgn55+t5pyfIDIw5pelJyxzdGF0Rm9yKGgsJzIwZCcpKX0KICAgICAgICAke2V2SHRtbCgn5Lit5pyfIDEyNuaXpScsc3RhdEZvcihoLCcxMjZkJykpfQogICAgICAgICR7ZXZIdG1sKCfplbfmnJ8gMjUy5pelJyxzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgogICAgICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+S+oeagvOODrOODs+OCuOODu+acn+W+heWApOOBr+WwhuadpeS6iOa4rOOBp+OBr+OBquOBj+OAgeWPluW+l+WPr+iDveOBqumBjuWOu+agquS+oeOBruODreODvOODquODs+OCsOWJjeaWueODquOCv+ODvOODs+WIhuW4g+OCkuacgOaWsOWPluW+l+e1guWApOOBq+W9k+OBpuOBr+OCgeOBn+e1seioiOWPguiAg+OBp+OBmeOAguacn+mWk+OBjOmHjeOBquOCi+aomeacrOOCkuWQq+OBv+OBvuOBmeOAgjwvcD4KICAgIDwvZGl2PmA7CiAgfSkuam9pbignJyk7CiAgcmVuZGVyUG9ydGZvbGlvU3VtbWFyeSgpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRRdW90ZShjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9xdW90ZT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgcT1hd2FpdCByLmpzb24oKTsKICBpZihxLnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHEucmVhc29ufHxxLmVycm9yfHwn5a6f44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgcmV0dXJuIHE7Cn0KCmFzeW5jIGZ1bmN0aW9uIGFkZEhvbGRpbmcoKXsKICBjb25zdCBjb2RlPSQoJ2hvbGRDb2RlJykudmFsdWUudHJpbSgpOwogIGNvbnN0IGNvc3Q9dmFsKCdob2xkQ29zdCcpLCBzaGFyZXM9dmFsKCdob2xkU2hhcmVzJyk7CiAgY29uc3Qgc3RvcD12YWwoJ3N0b3BQY3QnKSwgdGFrZT12YWwoJ3Rha2VQY3QnKSwgdHJhaWw9dmFsKCd0cmFpbFBjdCcpOwogIGNvbnN0IGZlZU1vZGU9JCgnZmVlTW9kZScpLnZhbHVlOwogIGlmKCFjb2RlfHwhY29zdHx8IXNoYXJlcyl7YWxlcnQoJ+mKmOafhOOCs+ODvOODieODu+WPluW+l+WNmOS+oeODu+agquaVsOOCkuWFpeWKm+OBl+OBpuOBrScpO3JldHVybn0KICBjb25zdCBidG49ZXZlbnQ/LnRhcmdldDsgaWYoYnRuKXtidG4uZGlzYWJsZWQ9dHJ1ZTtidG4udGV4dENvbnRlbnQ9J+WPluW+l+S4reKApid9CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShjb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgICBjb25zdCBoPXsKICAgICAgY29kZSwKICAgICAgY29tcGFueV9uYW1lOihxLmNvbXBhbnkmJnEuY29tcGFueS5uYW1lKXx8JycsCiAgICAgIGNvbXBhbnlfbWFya2V0OihxLmNvbXBhbnkmJnEuY29tcGFueS5tYXJrZXQpfHwnJywKICAgICAgY29tcGFueV9zZWN0b3IzMzoocS5jb21wYW55JiZxLmNvbXBhbnkuc2VjdG9yMzMpfHwnJywKICAgICAgY29zdCwgc2hhcmVzLCBmZWVfbW9kZTpmZWVNb2RlLAogICAgICBzdG9wX3BjdDpzdG9wPz84LCB0YWtlX3BjdDp0YWtlPz8xNSwgdHJhaWxfcGN0OnRyYWlsPz83LAogICAgICBjdXJyZW50X3ByaWNlOnMubGFzdF9jbG9zZSwgaGlnaF8yMGQ6cy5oaWdoXzIwZCwgbG93XzIwZDpzLmxvd18yMGQsCiAgICAgIHJldHVybl8yMGQ6cy5yZXR1cm5fMjBkLCByZXR1cm5fMTI2ZDpzLnJldHVybl8xMjZkLCByZXR1cm5fMjUyZDpzLnJldHVybl8yNTJkLAogICAgICBmb3J3YXJkX3N0YXRzOnMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9LAogICAgICBhc29mOnMubGFzdF9kYXRlLCB1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKQogICAgfTsKICAgIGNvbnN0IGlkeD1hLmZpbmRJbmRleCh4PT54LmNvZGU9PT1jb2RlKTsKICAgIGlmKGlkeD49MClhW2lkeF09aDsgZWxzZSBhLnB1c2goaCk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSl9CiAgZmluYWxseXtpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmCd9fQp9Cgphc3luYyBmdW5jdGlvbiByZWZyZXNoSG9sZGluZyhpKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpLCBoPWFbaV07IGlmKCFoKXJldHVybjsKICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGguY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBoLmNvbXBhbnlfbmFtZT0ocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fGguY29tcGFueV9uYW1lfHwnJzsKICAgIGguY29tcGFueV9tYXJrZXQ9KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fGguY29tcGFueV9tYXJrZXR8fCcnOwogICAgaC5jb21wYW55X3NlY3RvcjMzPShxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fGguY29tcGFueV9zZWN0b3IzM3x8Jyc7CiAgICBoLmN1cnJlbnRfcHJpY2U9cy5sYXN0X2Nsb3NlOyBoLmhpZ2hfMjBkPXMuaGlnaF8yMGQ7IGgubG93XzIwZD1zLmxvd18yMGQ7CiAgICBoLnJldHVybl8yMGQ9cy5yZXR1cm5fMjBkOyBoLnJldHVybl8xMjZkPXMucmV0dXJuXzEyNmQ7IGgucmV0dXJuXzI1MmQ9cy5yZXR1cm5fMjUyZDsKICAgIGguZm9yd2FyZF9zdGF0cz1zLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fTsKICAgIGguYXNvZj1zLmxhc3RfZGF0ZTsgaC51cGRhdGVkX2F0PW5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTsKICAgIGFbaV09aDsgc2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpOyByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5pu05paw44Ko44Op44O8OiAnK2UubWVzc2FnZSl9Cn0KZnVuY3Rpb24gcmVtb3ZlSG9sZGluZyhpKXtjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpO2Euc3BsaWNlKGksMSk7c2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpO3JlbmRlckhvbGRpbmdzKCl9CgpmdW5jdGlvbiByZW5kZXJXYXRjaCgpewogICQoJ3dhdGNocycpLmlubmVySFRNTD1sb2NhbCgnZnJlZV93YXRjaCcpLm1hcCh4PT57CiAgICBpZih0eXBlb2YgeD09PSdzdHJpbmcnKXJldHVybiBgPGRpdiBjbGFzcz0iYmFkZ2UiPiR7eH08L2Rpdj5gOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJiYWRnZSI+PGI+JHt4LmNvZGV9PC9iPiR7eC5uYW1lPycgJyt4Lm5hbWU6Jyd9PC9kaXY+YDsKICB9KS5qb2luKCcnKXx8JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7Cn0KYXN5bmMgZnVuY3Rpb24gYWRkV2F0Y2goKXsKICBsZXQgYz0kKCd3YXRjaENvZGUnKS52YWx1ZS50cmltKCk7IGlmKCFjKXJldHVybjsKICBsZXQgaW5mbz1udWxsOwogIHRyeXtpbmZvPWF3YWl0IGdldENvbXBhbnkoYyl9Y2F0Y2goZSl7fQogIGxldCBhPWxvY2FsKCdmcmVlX3dhdGNoJyk7CiAgY29uc3QgZXhpc3RzPWEuc29tZSh4PT4odHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSk9PT1jKTsKICBpZighZXhpc3RzKWEucHVzaCh7Y29kZTpjLG5hbWU6aW5mbyYmaW5mby5uYW1lP2luZm8ubmFtZTonJ30pOwogIHNhdmUoJ2ZyZWVfd2F0Y2gnLGEpOwogIHJlbmRlcldhdGNoKCk7Cn0KZnVuY3Rpb24gdXBkYXRlS2FidXRhbigpe2xldCBjPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7JCgna2FidXRhbicpLmhyZWY9Yz8naHR0cHM6Ly9rYWJ1dGFuLmpwL3N0b2NrLz9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpOidodHRwczovL2thYnV0YW4uanAvJ30KJCgnY29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+e3VwZGF0ZUthYnV0YW4oKTtzY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2NvZGUnLCdjb21wYW55TmFtZScsJycpfSk7dXBkYXRlS2FidXRhbigpOwokKCdob2xkQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCdob2xkQ29kZScsJ2hvbGRDb21wYW55TmFtZScsJycpKTsKJCgnd2F0Y2hDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ3dhdGNoQ29kZScsJ3dhdGNoQ29tcGFueU5hbWUnLCcnKSk7Cgphc3luYyBmdW5jdGlvbiBhbmFseXplKCl7CiAgY29uc3QgY29kZT0kKCdjb2RlJykudmFsdWUudHJpbSgpOwogIGlmKCFjb2RlKXskKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44GtJztyZXR1cm59CiAgY29uc3QgYnRuPSQoJ2FuYWx5emVCdG4nKTsgYnRuLmRpc2FibGVkPXRydWU7IGJ0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJzsKICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0nSi1RdWFudHPjgYvjgonlrp/jg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKYnOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICAkKCdwcmljZScpLnZhbHVlPXMubGFzdF9jbG9zZT09bnVsbD8nJzpmbXQocy5sYXN0X2Nsb3NlLDEpOwogICAgJCgncjIwJykudmFsdWU9Zm10KHMucmV0dXJuXzIwZCk7JCgncjEyNicpLnZhbHVlPWZtdChzLnJldHVybl8xMjZkKTskKCdyMjUyJykudmFsdWU9Zm10KHMucmV0dXJuXzI1MmQpOwogICAgJCgnaGlnaDIwJykudGV4dENvbnRlbnQ9Zm10KHMuaGlnaF8yMGQsMSk7JCgnbG93MjAnKS50ZXh0Q29udGVudD1mbXQocy5sb3dfMjBkLDEpOwogICAgJCgndm9sMjAnKS50ZXh0Q29udGVudD1zLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQ9PW51bGw/J+KAlCc6Zm10KHMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZCkrJyUnOwogICAgZGlzcGxheUNvbXBhbnkoJCgnY29tcGFueU5hbWUnKSxxLmNvbXBhbnl8fG51bGwsJycpOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPSc8YiBjbGFzcz0ib2siPuKchSBKLVF1YW50c+Wun+ODh+ODvOOCv+WPluW+l09LPC9iPjxicj7mnIDntYLjg4fjg7zjgr/ml6U6ICcrKHMubGFzdF9kYXRlfHwn4oCUJykrJyAvIOe1guWApDogJytmbXQocy5sYXN0X2Nsb3NlLDEpKycgLyDjgrXjg7Pjg5fjg6s6ICcrKHMuc2FtcGxlX2NvdW50Pz8n4oCUJykrJ+S7tic7CiAgICAkKCdhbmFseXNpc0V2JykuaW5uZXJIVE1MPWV2SHRtbCgn55+t5pyfMjDml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzIwZCddKStldkh0bWwoJ+S4reacnzEyNuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMTI2ZCddKStldkh0bWwoJ+mVt+acnzI1MuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjUyZCddKTsKICAgIGNvbnN0IGQ9e2NvZGUscHJpY2U6cy5sYXN0X2Nsb3NlLHJldHVybjIwOnMucmV0dXJuXzIwZCxyZXR1cm4xMjY6cy5yZXR1cm5fMTI2ZCxyZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCxlYXJuaW5nc19zY29yZTp2YWwoJ2Vhcm4nKSxwb2xpY3lfc2NvcmU6dmFsKCdwb2xpY3knKSxzdXBwbHlfc2NvcmU6dmFsKCdzdXBwbHknKX07CiAgICBjb25zdCBhcj1hd2FpdCBmZXRjaCgnL2FwaS9mcmVlL2FuYWx5emUnLHttZXRob2Q6J1BPU1QnLGhlYWRlcnM6eydDb250ZW50LVR5cGUnOidhcHBsaWNhdGlvbi9qc29uJ30sYm9keTpKU09OLnN0cmluZ2lmeShkKSxjYWNoZTonbm8tc3RvcmUnfSk7CiAgICBjb25zdCB4PWF3YWl0IGFyLmpzb24oKTsKICAgIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfliIbmnpDjg4fjg7zjgr/jgYzkuI3otrPjgZfjgabjgYTjgb7jgZknKTsKICAgICQoJ3N0YXRlJykudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7JCgncG9zJykudGV4dENvbnRlbnQ9eC5zaWduYWwucG9zaXRpdmVfY291bnQ7JCgnbmVnJykudGV4dENvbnRlbnQ9eC5zaWduYWwubmVnYXRpdmVfY291bnQ7CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n5a6f44OH44O844K/ICcreC5zaWduYWwuZXZpZGVuY2VfY291bnQrJ+S7tuOCkuagueaLoOOBq+WIpOWumuOAguacn+W+heWApOOBr09PU+agoeato+ODh+ODvOOCv+OBjOWNgeWIhuOBq+OBquOCi+OBvuOBp+acquihqOekuuOBp+OBmeOAgic7CiAgfWNhdGNoKGUpewogICAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+KaoO+4jyAnK2UubWVzc2FnZTsKICAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0nPHNwYW4gY2xhc3M9ImVyciI+5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSsnPC9zcGFuPic7CiAgfWZpbmFsbHl7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn5YiG5p6QJ30KfQoKcmVuZGVySG9sZGluZ3MoKTtyZW5kZXJXYXRjaCgpOwppZignc2VydmljZVdvcmtlcicgaW4gbmF2aWdhdG9yKXtuYXZpZ2F0b3Iuc2VydmljZVdvcmtlci5nZXRSZWdpc3RyYXRpb25zKCkudGhlbihycz0+UHJvbWlzZS5hbGwocnMubWFwKHI9PnIudW5yZWdpc3RlcigpKSkpLmNhdGNoKCgpPT57fSl9CmlmKCdjYWNoZXMnIGluIHdpbmRvdyl7Y2FjaGVzLmtleXMoKS50aGVuKGtleXM9PlByb21pc2UuYWxsKGtleXMubWFwKGs9PmNhY2hlcy5kZWxldGUoaykpKSkuY2F0Y2goKCk9Pnt9KX0KPC9zY3JpcHQ+CjwvbWFpbj4KPC9ib2R5Pgo8L2h0bWw+"
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
