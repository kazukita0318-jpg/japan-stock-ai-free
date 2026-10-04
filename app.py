from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.6-DECISION-PANEL'
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
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9Ci5kaXN0YW5jZXtmb250LXdlaWdodDo4MDB9CgoKQG1lZGlhKG1heC13aWR0aDo0ODBweCl7LmdyaWQze2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyIDFmcn0ua3BpIGJ7Zm9udC1zaXplOjE2cHh9fQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8bWFpbj4KPGRpdiBjbGFzcz0idG9wIj4KICA8aDE+8J+TiCDml6XmnKzmoKpBSSBGUkVFPC9oMT4KICA8ZGl2IGNsYXNzPSJzdWIiPkotUXVhbnRz5a6f44OH44O844K/IC8g5L+d5pyJ5Yik5patIC8g5L6h5qC844Os44Oz44K4IC8g5Yik5a6a5L+h6aC85bqmIC8g6YqY5p+E5ZCNPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfjq8g6YqY5p+E5YiG5p6QPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkIj4KICAgIDxpbnB1dCBpZD0iY29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIOS+iyA3MjAzIj4KICAgIDxpbnB1dCBpZD0icHJpY2UiIHBsYWNlaG9sZGVyPSLlj5blvpfntYLlgKQiIHJlYWRvbmx5PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbXBhbnlOYW1lIiBjbGFzcz0ic291cmNlIG11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBmeOCi+OBqOS8muekvuWQjeOCkuihqOekuuOBl+OBvuOBmTwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8YSBpZD0ia2FidXRhbiIgY2xhc3M9ImJ0biBzZWNvbmRhcnkiIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7moKrmjqLjgafnorroqo08L2E+CiAgICA8YnV0dG9uIGlkPSJhbmFseXplQnRuIiBvbmNsaWNrPSJhbmFseXplKCkiPuWun+ODh+ODvOOCv+OBp+WIhuaekDwvYnV0dG9uPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl44KM44Gm5oq844GZ44Go44CBSi1RdWFudHPjgYvjgonlj5blvpfjgafjgY3jgovlrp/jg4fjg7zjgr/jgpLoh6rli5XlhaXlipvjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBpZD0ic291cmNlQm94IiBjbGFzcz0ic291cmNlIG11dGVkIj7jg4fjg7zjgr/mnKrlj5blvpc8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+TiiDmoKrkvqHjg7vjg4bjgq/jg4vjgqvjg6vlrp/nuL48L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemosOiQveeOhyAlPC9zcGFuPjxpbnB1dCBpZD0icjIwIiByZWFkb25seT48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4xMjbml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIxMjYiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjI1MuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjI1MiIgcmVhZG9ubHk+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6Xpq5jlgKQ8L3NwYW4+PGIgaWQ9ImhpZ2gyMCI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel5a6J5YCkPC9zcGFuPjxiIGlkPSJsb3cyMCI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel5bm0546H44Oc44OpPC9zcGFuPjxiIGlkPSJ2b2wyMCI+4oCUPC9iPjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn6epIOijnOWKqeipleS+oTwvaDM+CiAgPHAgY2xhc3M9Im11dGVkIj7liIbjgYvjgovpoIXnm67jgaDjgZHlhaXlipvjgILmnKrlhaXlipvjga8w54K544Gn44Gv44Gq44GP44CM5LiN5piO44CN44Go44GX44Gm5omx44GE44G+44GZ44CCPC9wPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpcgLTEwMOOAnDEwMDwvc3Bhbj48aW5wdXQgaWQ9ImVhcm4iIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5pS/562WIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJwb2xpY3kiIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZyA57WmIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJzdXBwbHkiIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6Ag5YiG5p6Q57WQ5p6cPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+54q25oWLPC9zcGFuPjxiIGlkPSJzdGF0ZSI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODl+ODqeOCueagueaLoDwvc3Bhbj48YiBpZD0icG9zIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44Oe44Kk44OK44K55qC55ougPC9zcGFuPjxiIGlkPSJuZWciPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KICA8cCBpZD0icmVzdWx0IiBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOAjOWun+ODh+ODvOOCv+OBp+WIhuaekOOAjeOCkuaKvOOBl+OBpuOBj+OBoOOBleOBhOOAgjwvcD4KICA8ZGl2IGlkPSJhbmFseXNpc0V2IiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkrwg5L+d5pyJ5qCq44O75pCN5YiH44KKL+WIqeeiujwvaDM+CiAgPGRpdiBjbGFzcz0ibm90ZSI+CiAgICDmkI3liIfjgorjg7vliKnnorrjg7vjg4jjg6zjg7zjg6rjg7PjgrDjga/lj4LogIPjg6njgqTjg7PjgILkv53mnInliKTmlq3jga/lrp/jg4fjg7zjgr/jgajoqK3lrprjg6njgqTjg7Pjga7jg6vjg7zjg6vliKTlrprjgafjgZnjgILnn63kuK3plbfjga/pgY7ljrvjga7jg63jg7zjg6rjg7PjgrDlrp/nuL7liIbluIPjgpLntbHoqIjlj4LogIPjgajjgZfjgabooajnpLrjgZfjgb7jgZnjgIIKICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZENvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8aW5wdXQgaWQ9ImhvbGRDb3N0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLlj5blvpfljZjkvqEiPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjZweCAycHggMCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZFNoYXJlcyIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i5qCq5pWwIj4KICAgIDxzZWxlY3QgaWQ9ImZlZU1vZGUiPgogICAgICA8b3B0aW9uIHZhbHVlPSJub211cmFfbmV0Ij7ph47mnZHjgqrjg7Pjg6njgqTjg7PlsILnlKjmlK/lupfjg7vnj77niak8L29wdGlvbj4KICAgICAgPG9wdGlvbiB2YWx1ZT0ibm9uZSI+5omL5pWw5paZ44Gq44GX77yI5q+U6LyD55So77yJPC9vcHRpb24+CiAgICA8L3NlbGVjdD4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoogJTwvc3Bhbj48aW5wdXQgaWQ9InN0b3BQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjgiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuiAlPC9zcGFuPjxpbnB1dCBpZD0idGFrZVBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iMTUiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODqyAlPC9zcGFuPjxpbnB1dCBpZD0idHJhaWxQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjciPjwvZGl2PgogIDwvZGl2PgogIDxidXR0b24gb25jbGljaz0iYWRkSG9sZGluZygpIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij7lrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZg8L2J1dHRvbj4KICA8cCBjbGFzcz0ibXV0ZWQiPumHjuadkeODjeODg+ODiO+8huOCs+ODvOODq++8j+OBu+OBo+OBqOODgOOCpOODrOOCr+ODiOOBruWbveWGheePvueJqeODu+OCquODs+ODqeOCpOODs+azqOaWh+OBrueojui+vOaJi+aVsOaWmeihqOOCkuS9v+eUqOOAgjwvcD4KICA8ZGl2IGlkPSJob2xkaW5ncyI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkYAg44Km44Kp44OD44OB44Oq44K544OIPC9oMz4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGlucHV0IGlkPSJ3YXRjaENvZGUiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPgogICAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRXYXRjaCgpIj7ov73liqA8L2J1dHRvbj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NHB4IDJweCA4cHgiPumKmOafhOWQje+8muKAlDwvZGl2PgogIDxkaXYgaWQ9IndhdGNocyI+PC9kaXY+CjwvZGl2PgoKPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKZnVuY3Rpb24gdmFsKGlkKXtsZXQgdj0kKGlkKS52YWx1ZS50cmltKCk7cmV0dXJuIHY9PT0nJz9udWxsOk51bWJlcih2KX0KZnVuY3Rpb24gbG9jYWwoayl7dHJ5e3JldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKGspfHwnW10nKX1jYXRjaChlKXtyZXR1cm5bXX19CmZ1bmN0aW9uIHNhdmUoayx2KXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrLEpTT04uc3RyaW5naWZ5KHYpKX0KZnVuY3Rpb24gZm10KHYsZD0yKXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoZCl9CmZ1bmN0aW9uIHllbih2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6J8KlJytNYXRoLnJvdW5kKE51bWJlcih2KSkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyl9CmZ1bmN0aW9uIHN0YXRlSmEocyl7cmV0dXJuIHM9PT0ncG9zaXRpdmUnPyfjg5fjg6njgrnlhKrli6InOnM9PT0nbmVnYXRpdmUnPyfjg57jgqTjg4rjgrnlhKrli6InOifmi67mipcnfQpmdW5jdGlvbiBwY3Qodil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDIpKyclJ30KZnVuY3Rpb24gc3RhdEZvcihoLGtleSl7cmV0dXJuIGgmJmguZm9yd2FyZF9zdGF0cyYmaC5mb3J3YXJkX3N0YXRzW2tleV0/aC5mb3J3YXJkX3N0YXRzW2tleV06bnVsbH0KZnVuY3Rpb24gY29uZmlkZW5jZUZvcihoKXsKICBjb25zdCB2YWxzPVtoLnJldHVybl8yMGQsaC5yZXR1cm5fMTI2ZCxoLnJldHVybl8yNTJkXS5tYXAoTnVtYmVyKS5maWx0ZXIoTnVtYmVyLmlzRmluaXRlKTsKICBjb25zdCBzdGF0cz1bJzIwZCcsJzEyNmQnLCcyNTJkJ10ubWFwKGs9PnN0YXRGb3IoaCxrKSkuZmlsdGVyKHM9PnMmJnMuc3RhdHVzPT09J29rJyk7CgogIGlmKCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpfHwhaC5hc29mKXJldHVybiAwOwoKICBjb25zdCBjb3ZlcmFnZT12YWxzLmxlbmd0aC8zOwogIGxldCBhZ3JlZW1lbnQ9MC41OwogIGlmKHZhbHMubGVuZ3RoKXsKICAgIGNvbnN0IHBvcz12YWxzLmZpbHRlcih4PT54PjApLmxlbmd0aDsKICAgIGNvbnN0IG5lZz12YWxzLmZpbHRlcih4PT54PDApLmxlbmd0aDsKICAgIGFncmVlbWVudD1NYXRoLm1heChwb3MsbmVnKS92YWxzLmxlbmd0aDsKICB9CiAgY29uc3Qgc3RhdENvdmVyYWdlPXN0YXRzLmxlbmd0aC8zOwoKICByZXR1cm4gTWF0aC5yb3VuZCgoY292ZXJhZ2UqMC40NSArIGFncmVlbWVudCowLjI1ICsgc3RhdENvdmVyYWdlKjAuMzApKjEwMCk7Cn0KCmZ1bmN0aW9uIGRlY2lzaW9uRm9yKGgsYyl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhaC5hc29mKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246J+Wun+ODh+ODvOOCv+acquWPluW+l+OAguWPs+S4iuOBruOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44GX44Gm44GP44Gg44GV44GE44CCJywKICAgICAgY29uZmlkZW5jZTowCiAgICB9OwogIH0KCiAgY29uc3QgcjIwPU51bWJlcihoLnJldHVybl8yMGQpLCByMTI2PU51bWJlcihoLnJldHVybl8xMjZkKSwgcjI1Mj1OdW1iZXIoaC5yZXR1cm5fMjUyZCk7CiAgY29uc3QgY29uZmlkZW5jZT1jb25maWRlbmNlRm9yKGgpOwoKICBpZihjdXI8PWMuc3RvcFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+aQjeWIh+OCiuaknOiojicsY2xzOidkLXN0b3AnLHJlYXNvbjon6Kit5a6a44GX44Gf5pCN5YiH44KK5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifliKnnorrmpJzoqI4nLGNsczonZC10YWtlJyxyZWFzb246J+ioreWumuOBl+OBn+WIqeeiuuWPguiAg+ODqeOCpOODs+S7peS4iicsY29uZmlkZW5jZX07CiAgfQogIGlmKGN1cjw9Yy50cmFpbFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel6auY5YCk5Z+65rqW44Gu44OI44Os44O844Oq44Oz44Kw5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CgogIGxldCBwb3NpdGl2ZT0wLCBuZWdhdGl2ZT0wOwogIFtyMjAscjEyNixyMjUyXS5mb3JFYWNoKHg9PnsKICAgIGlmKE51bWJlci5pc0Zpbml0ZSh4KSl7CiAgICAgIGlmKHg+MClwb3NpdGl2ZSsrOwogICAgICBpZih4PDApbmVnYXRpdmUrKzsKICAgIH0KICB9KTsKCiAgaWYobmVnYXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon6K2m5oiSJyxjbHM6J2Qtd2F0Y2gnLHJlYXNvbjonMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7jgYbjgaHjg57jgqTjg4rjgrnlgr7lkJHjgYzlhKrli6InLGNvbmZpZGVuY2V9OwogIH0KICBpZihwb3NpdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntponLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOBp+OAgeikh+aVsOacn+mWk+OBruS+oeagvOODiOODrOODs+ODieOBjOODl+ODqeOCuScsY29uZmlkZW5jZX07CiAgfQogIHJldHVybiB7bGFiZWw6J+S/neaciee2mee2mu+8iOanmOWtkOimi++8iScsY2xzOidkLWhvbGQnLHJlYXNvbjon6Kit5a6a44Op44Kk44Oz5YaF44CC5pyf6ZaT5Yil44OI44Os44Oz44OJ44Gv5by35byx44GM5re35ZyoJyxjb25maWRlbmNlfTsKfQpmdW5jdGlvbiBldkh0bWwodGl0bGUscyl7CiAgaWYoIXN8fHMuc3RhdHVzIT09J29rJyl7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzbWFsbD7mqJnmnKwgJHtufeS7tjwvc21hbGw+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPgogICAgPGI+5bmz5Z2HICR7cGN0KHMubWVhbil9PC9iPgogICAgPHNtYWxsPuS4reWkruWApCAke3BjdChzLm1lZGlhbil9PC9zbWFsbD4KICAgIDxzbWFsbD7kuIrmmIfnjocgJHtwY3Qocy5wb3NpdGl2ZV9yYXRlKX08L3NtYWxsPgogICAgPHNtYWxsPlAxMOOAnFA5MCAke3BjdChzLnAxMCl9IOOAnCAke3BjdChzLnA5MCl9PC9zbWFsbD4KICAgIDxzbWFsbD7mqJnmnKwgJHtzLm595Lu2PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgoKZnVuY3Rpb24gZGlzdGFuY2VJbmZvKGN1cix0YXJnZXQsa2luZCl7CiAgY3VyPU51bWJlcihjdXIpOyB0YXJnZXQ9TnVtYmVyKHRhcmdldCk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFOdW1iZXIuaXNGaW5pdGUodGFyZ2V0KSlyZXR1cm4gJ+KAlCc7CiAgY29uc3QgZGlmZj0odGFyZ2V0L2N1ci0xKSoxMDA7CiAgaWYoa2luZD09PSdzdG9wJyl7CiAgICBpZihkaWZmPj0wKXJldHVybiAn44Op44Kk44Oz5Yiw6YGU5riI44G/JzsKICAgIHJldHVybiBNYXRoLmFicyhkaWZmKS50b0ZpeGVkKDIpKyclIOS4iyc7CiAgfQogIGlmKGtpbmQ9PT0ndGFrZScpewogICAgaWYoZGlmZjw9MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gZGlmZi50b0ZpeGVkKDIpKyclIOS4iic7CiAgfQogIHJldHVybiAoZGlmZj49MD8nKyc6JycpK2RpZmYudG9GaXhlZCgyKSsnJSc7Cn0KCmZ1bmN0aW9uIHByaWNlUmFuZ2UoY3VyLHMpewogIGN1cj1OdW1iZXIoY3VyKTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IXN8fHMuc3RhdHVzIT09J29rJylyZXR1cm4gbnVsbDsKICByZXR1cm4gewogICAgbG93OmN1ciooMStOdW1iZXIocy5wMTApLzEwMCksCiAgICBoaWdoOmN1ciooMStOdW1iZXIocy5wOTApLzEwMCksCiAgICBtZWRpYW46Y3VyKigxK051bWJlcihzLm1lZGlhbikvMTAwKQogIH07Cn0KCmZ1bmN0aW9uIHJhbmdlSHRtbCh0aXRsZSxjdXIscyl7CiAgY29uc3Qgcj1wcmljZVJhbmdlKGN1cixzKTsKICBpZighcil7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5qiZ5pysICR7bn3ku7Y8L3NwYW4+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJyYW5nZWJveCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9IFAxMOOAnFA5MDwvc3Bhbj4KICAgIDxiPiR7eWVuKHIubG93KX0g44CcICR7eWVuKHIuaGlnaCl9PC9iPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7kuK3lpK7lgKTmj5vnrpcgJHt5ZW4oci5tZWRpYW4pfTwvc3Bhbj4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiBhY3Rpb25UZXh0KGgsYyxkKXsKICBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmScpcmV0dXJuICfjgb7jgZrlrp/jg4fjg7zjgr/jgpLmm7TmlrDjgZfjgabjgYvjgonliKTmlq3jgIInOwogIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylyZXR1cm4gJ+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+OCkuS4i+WbnuOBo+OBpuOBhOOBvuOBmeOAguWun+mam+OBruePvuWcqOWApOOBqOazqOaWh+adoeS7tuOCkueiuuiqjeOBl+OBpuOAgee4ruWwj+ODu+aSpOmAgOOCkuaknOiojuOAgic7CiAgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKXJldHVybiAn5Yip56K65Y+C6ICD44Op44Kk44Oz44Gr5Yiw6YGU44GX44Gm44GE44G+44GZ44CC5YWo6YOo5aOy5Y2044Gg44GR44Gn44Gq44GP44CB5YiG5Ymy5Yip56K644KC5YCZ6KOc44CCJzsKICBpZihkLmxhYmVsPT09J+itpuaIkicpcmV0dXJuICforabmiJLjgr7jg7zjg7PjgILjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PjgajkuK3nn63mnJ/jga7lgKTli5XjgY3jgpLlhKrlhYjjgZfjgabnorroqo3jgIInOwogIHJldHVybiAn6Kit5a6a44Op44Kk44Oz5YaF44CC5L+d5pyJ57aZ57aa5YCZ6KOc44Gn44GZ44GM44CB54Sh5paZ44OH44O844K/44Gv6YGF5bu244GZ44KL44Gf44KB5a6f6Zqb44Gu54++5Zyo5YCk44KC56K66KqN44CCJzsKfQoKCmZ1bmN0aW9uIG5vbXVyYU5ldEZlZShhbW91bnQpewogIGFtb3VudD1OdW1iZXIoYW1vdW50fHwwKTsKICBpZihhbW91bnQ8PTApcmV0dXJuIDA7CiAgaWYoYW1vdW50PD0xMDAwMDApcmV0dXJuIDE1MjsKICBpZihhbW91bnQ8PTMwMDAwMClyZXR1cm4gMzMwOwogIGlmKGFtb3VudDw9NTAwMDAwKXJldHVybiA1MjQ7CiAgaWYoYW1vdW50PD0xMDAwMDAwKXJldHVybiAxMDQ4OwogIGlmKGFtb3VudDw9MjAwMDAwMClyZXR1cm4gMjA5NTsKICBpZihhbW91bnQ8PTMwMDAwMDApcmV0dXJuIDMxNDM7CiAgaWYoYW1vdW50PD01MDAwMDAwKXJldHVybiA1MjM4OwogIGlmKGFtb3VudDw9MTAwMDAwMDApcmV0dXJuIDEwNDc2OwogIGlmKGFtb3VudDw9MjAwMDAwMDApcmV0dXJuIDIwOTUyOwogIGlmKGFtb3VudDw9MzAwMDAwMDApcmV0dXJuIDMxNDI5OwogIGlmKGFtb3VudDw9NTAwMDAwMDApcmV0dXJuIDQxOTA1OwogIHJldHVybiA3ODU3MTsKfQpmdW5jdGlvbiBmZWVGb3IoYW1vdW50LG1vZGUpe3JldHVybiBtb2RlPT09J25vbXVyYV9uZXQnP25vbXVyYU5ldEZlZShhbW91bnQpOjB9CgpmdW5jdGlvbiBjYWxjSG9sZGluZyhoKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgY29zdD1OdW1iZXIoaC5jb3N0KTsKICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzKTsKICBjb25zdCBidXlWYWx1ZT1jb3N0KnNoYXJlczsKICBjb25zdCBidXlGZWU9ZmVlRm9yKGJ1eVZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGN1cnJlbnRWYWx1ZT1jdXIqc2hhcmVzOwogIGNvbnN0IHNlbGxGZWU9ZmVlRm9yKGN1cnJlbnRWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBpbnZlc3RlZD1idXlWYWx1ZStidXlGZWU7CiAgY29uc3QgbmV0Tm93PWN1cnJlbnRWYWx1ZS1zZWxsRmVlLWludmVzdGVkOwogIGNvbnN0IG5ldE5vd1BjdD1pbnZlc3RlZD9uZXROb3cvaW52ZXN0ZWQqMTAwOm51bGw7CgogIGNvbnN0IHN0b3BQcmljZT1jb3N0KigxLU51bWJlcihoLnN0b3BfcGN0KS8xMDApOwogIGNvbnN0IHRha2VQcmljZT1jb3N0KigxK051bWJlcihoLnRha2VfcGN0KS8xMDApOwogIGNvbnN0IGhpZ2gyMD1OdW1iZXIoaC5oaWdoXzIwZHx8Y3VyKTsKICBjb25zdCB0cmFpbFByaWNlPWhpZ2gyMCooMS1OdW1iZXIoaC50cmFpbF9wY3QpLzEwMCk7CgogIGNvbnN0IHN0b3BWYWx1ZT1zdG9wUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHRha2VWYWx1ZT10YWtlUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHN0b3BOZXQ9c3RvcFZhbHVlLWZlZUZvcihzdG9wVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CiAgY29uc3QgdGFrZU5ldD10YWtlVmFsdWUtZmVlRm9yKHRha2VWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKCiAgcmV0dXJuIHtidXlWYWx1ZSxidXlGZWUsY3VycmVudFZhbHVlLHNlbGxGZWUsaW52ZXN0ZWQsbmV0Tm93LG5ldE5vd1BjdCxzdG9wUHJpY2UsdGFrZVByaWNlLHRyYWlsUHJpY2Usc3RvcE5ldCx0YWtlTmV0fTsKfQoKCmNvbnN0IG5hbWVUaW1lcnM9e307CmNvbnN0IG5hbWVDYWNoZT17fTsKCmZ1bmN0aW9uIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeD0nJyl7CiAgaWYoIXRhcmdldClyZXR1cm47CiAgaWYoIWluZm98fCFpbmZvLm5hbWUpewogICAgdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIHJldHVybjsKICB9CiAgbGV0IGV4dHJhcz1bXTsKICBpZihpbmZvLm1hcmtldClleHRyYXMucHVzaChpbmZvLm1hcmtldCk7CiAgaWYoaW5mby5zZWN0b3IzMylleHRyYXMucHVzaChpbmZvLnNlY3RvcjMzKTsKICB0YXJnZXQuaW5uZXJIVE1MPSc8Yj4nK3ByZWZpeCtpbmZvLm5hbWUrJzwvYj4nKyhleHRyYXMubGVuZ3RoPyc8YnI+PHNwYW4gY2xhc3M9Im11dGVkIj4nK2V4dHJhcy5qb2luKCcgLyAnKSsnPC9zcGFuPic6JycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRDb21wYW55KGNvZGUpewogIGNvbnN0IGM9U3RyaW5nKGNvZGV8fCcnKS50cmltKCk7CiAgaWYoIWMpcmV0dXJuIG51bGw7CiAgaWYobmFtZUNhY2hlW2NdKXJldHVybiBuYW1lQ2FjaGVbY107CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvc2VjdXJpdHk/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+mKmOafhOWQjeOCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIG5hbWVDYWNoZVtjXT14OwogIHJldHVybiB4Owp9CgpmdW5jdGlvbiBzY2hlZHVsZUNvbXBhbnlMb29rdXAoaW5wdXRJZCx0YXJnZXRJZCxwcmVmaXg9JycpewogIGNsZWFyVGltZW91dChuYW1lVGltZXJzW2lucHV0SWRdKTsKICBjb25zdCBjPSQoaW5wdXRJZCkudmFsdWUudHJpbSgpOwogIGNvbnN0IHRhcmdldD0kKHRhcmdldElkKTsKCiAgaWYoYy5sZW5ndGg8NCl7CiAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya4oCUJzsKICAgIHJldHVybjsKICB9CgogIG5hbWVUaW1lcnNbaW5wdXRJZF09c2V0VGltZW91dChhc3luYygpPT57CiAgICB0cnl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9J+mKmOafhOWQjeOCkueiuuiqjeS4reKApic7CiAgICAgIGNvbnN0IGluZm89YXdhaXQgZ2V0Q29tcGFueShjKTsKICAgICAgZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4KTsKICAgIH1jYXRjaChlKXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICB9CiAgfSw0NTApOwp9CgpmdW5jdGlvbiByZW5kZXJIb2xkaW5ncygpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgaWYoIWEubGVuZ3RoKXskKCdob2xkaW5ncycpLmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JztyZXR1cm59CiAgJCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9YS5tYXAoKGgsaSk9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCB2YWxpZFByaWNlPU51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZjsKICAgIGNvbnN0IGNscz12YWxpZFByaWNlPyhjLm5ldE5vdz49MD8ncG9zJzonbmVnJyk6Jyc7CiAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CgogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJob2xkaW5nIj4KICAgICAgPGRpdiBjbGFzcz0iaG9sZGluZy1oZWFkIj4KICAgICAgICA8ZGl2PgogICAgICAgICAgPGI+JHtoLmNvZGV9PC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPiR7aC5jb21wYW55X25hbWV8fCIifTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9InJvdyI+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hIb2xkaW5nKCR7aX0pIj7mm7TmlrA8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlSG9sZGluZygke2l9KSI+5YmK6ZmkPC9idXR0b24+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuODh+ODvOOCv+aXpSAke2guYXNvZnx8J+KAlCd9IC8g5pyA5paw5Y+W5b6X57WC5YCkICR7dmFsaWRQcmljZT95ZW4oaC5jdXJyZW50X3ByaWNlKTon4oCUJ30gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZGVjaXNpb24gJHtkLmNsc30iPgogICAgICAgICR7ZC5sYWJlbH0KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NHB4Ij4ke2QucmVhc29ufTwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuOBvuOBpzwvc3Bhbj4KICAgICAgICAgIDxiIGNsYXNzPSJkaXN0YW5jZSI+JHt2YWxpZFByaWNlP2Rpc3RhbmNlSW5mbyhjdXIsYy5zdG9wUHJpY2UsJ3N0b3AnKTon4oCUJ308L2I+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWPguiAgyAke3llbihjLnN0b3BQcmljZSl9PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuOBvuOBpzwvc3Bhbj4KICAgICAgICAgIDxiIGNsYXNzPSJkaXN0YW5jZSI+JHt2YWxpZFByaWNlP2Rpc3RhbmNlSW5mbyhjdXIsYy50YWtlUHJpY2UsJ3Rha2UnKTon4oCUJ308L2I+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWPguiAgyAke3llbihjLnRha2VQcmljZSl9PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWIpOWumuS/oemgvOW6pjwvc3Bhbj4KICAgICAgICAgIDxiPiR7ZC5jb25maWRlbmNlfSU8L2I+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJnYXVnZSI+PHNwYW4gc3R5bGU9IndpZHRoOiR7ZC5jb25maWRlbmNlfSUiPjwvc3Bhbj48L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjVweCI+4oC75Yik5a6a5L+h6aC85bqm44Gv44CB44OH44O844K/5YWF6Laz5bqm44O75pyf6ZaT44OI44Os44Oz44OJ44Gu5LiA6Ie05bqm44O75pyf5b6F5YCk57Wx6KiI44Gu5pyJ54Sh44GL44KJ5L2c44KL5Y+C6ICD5oyH5qiZ44Gn44CB55qE5Lit56K6546H44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgoKICAgICAgPGRpdiBjbGFzcz0iYWN0aW9uYm94Ij4KICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuS7iuOBqeOBhuOBmeOCi++8nzwvc3Bhbj4KICAgICAgICA8YiBzdHlsZT0iZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweCI+JHthY3Rpb25UZXh0KGgsYyxkKX08L2I+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vOaQjeebijwvc3Bhbj48YiBjbGFzcz0iJHtjbHN9Ij4ke3ZhbGlkUHJpY2U/eWVuKGMubmV0Tm93KTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3ZhbGlkUHJpY2U/Zm10KGMubmV0Tm93UGN0KSsnJSc6J+KAlCd9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7osrfku5jmiYvmlbDmlpk8L3NwYW4+PGI+JHt5ZW4oYy5idXlGZWUpfTwvYj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5aOy5Y205omL5pWw5paZKOS7iik8L3NwYW4+PGI+JHt2YWxpZFByaWNlP3llbihjLnNlbGxGZWUpOifigJQnfTwvYj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KK5Y+C6ICDPC9zcGFuPjxiPiR7eWVuKGMuc3RvcFByaWNlKX08L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrwgJHt5ZW4oYy5zdG9wTmV0KX08L3NwYW4+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnRha2VQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMudGFrZU5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg4jjg6zjg7zjg6rjg7PjgrDlj4LogIM8L3NwYW4+PGI+JHt2YWxpZFByaWNlP3llbihjLnRyYWlsUHJpY2UpOifigJQnfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6auY5YCk5Z+65rqWPC9zcGFuPjwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA3cHgiPvCfjq8g5a6f57i+44OZ44O844K55L6h5qC844Os44Oz44K4PC9oND4KICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICAgICR7cmFuZ2VIdG1sKCfnn63mnJ8gMjDml6UnLGN1cixzdGF0Rm9yKGgsJzIwZCcpKX0KICAgICAgICAke3JhbmdlSHRtbCgn5Lit5pyfIDEyNuaXpScsY3VyLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke3JhbmdlSHRtbCgn6ZW35pyfIDI1MuaXpScsY3VyLHN0YXRGb3IoaCwnMjUyZCcpKX0KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn5OQIOWun+e4vuODmeODvOOCueacn+W+heWApO+8iOe1seioiOWPguiAg++8iTwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke2V2SHRtbCgn55+t5pyfIDIw5pelJyxzdGF0Rm9yKGgsJzIwZCcpKX0KICAgICAgICAke2V2SHRtbCgn5Lit5pyfIDEyNuaXpScsc3RhdEZvcihoLCcxMjZkJykpfQogICAgICAgICR7ZXZIdG1sKCfplbfmnJ8gMjUy5pelJyxzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgogICAgICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+S+oeagvOODrOODs+OCuOODu+acn+W+heWApOOBr+WwhuadpeS6iOa4rOOBp+OBr+OBquOBj+OAgeWPluW+l+WPr+iDveOBqumBjuWOu+agquS+oeOBruODreODvOODquODs+OCsOWJjeaWueODquOCv+ODvOODs+WIhuW4g+OCkuacgOaWsOWPluW+l+e1guWApOOBq+W9k+OBpuOBr+OCgeOBn+e1seioiOWPguiAg+OBp+OBmeOAguacn+mWk+OBjOmHjeOBquOCi+aomeacrOOCkuWQq+OBv+OBvuOBmeOAgjwvcD4KICAgIDwvZGl2PmA7CiAgfSkuam9pbignJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldFF1b3RlKGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3F1b3RlP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCBxPWF3YWl0IHIuanNvbigpOwogIGlmKHEuc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IocS5yZWFzb258fHEuZXJyb3J8fCflrp/jg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4gcTsKfQoKYXN5bmMgZnVuY3Rpb24gYWRkSG9sZGluZygpewogIGNvbnN0IGNvZGU9JCgnaG9sZENvZGUnKS52YWx1ZS50cmltKCk7CiAgY29uc3QgY29zdD12YWwoJ2hvbGRDb3N0JyksIHNoYXJlcz12YWwoJ2hvbGRTaGFyZXMnKTsKICBjb25zdCBzdG9wPXZhbCgnc3RvcFBjdCcpLCB0YWtlPXZhbCgndGFrZVBjdCcpLCB0cmFpbD12YWwoJ3RyYWlsUGN0Jyk7CiAgY29uc3QgZmVlTW9kZT0kKCdmZWVNb2RlJykudmFsdWU7CiAgaWYoIWNvZGV8fCFjb3N0fHwhc2hhcmVzKXthbGVydCgn6YqY5p+E44Kz44O844OJ44O75Y+W5b6X5Y2Y5L6h44O75qCq5pWw44KS5YWl5Yqb44GX44Gm44GtJyk7cmV0dXJufQogIGNvbnN0IGJ0bj1ldmVudD8udGFyZ2V0OyBpZihidG4pe2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJ30KICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICAgIGNvbnN0IGg9ewogICAgICBjb2RlLAogICAgICBjb21wYW55X25hbWU6KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHwnJywKICAgICAgY29tcGFueV9tYXJrZXQ6KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fCcnLAogICAgICBjb21wYW55X3NlY3RvcjMzOihxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fCcnLAogICAgICBjb3N0LCBzaGFyZXMsIGZlZV9tb2RlOmZlZU1vZGUsCiAgICAgIHN0b3BfcGN0OnN0b3A/PzgsIHRha2VfcGN0OnRha2U/PzE1LCB0cmFpbF9wY3Q6dHJhaWw/PzcsCiAgICAgIGN1cnJlbnRfcHJpY2U6cy5sYXN0X2Nsb3NlLCBoaWdoXzIwZDpzLmhpZ2hfMjBkLCBsb3dfMjBkOnMubG93XzIwZCwKICAgICAgcmV0dXJuXzIwZDpzLnJldHVybl8yMGQsIHJldHVybl8xMjZkOnMucmV0dXJuXzEyNmQsIHJldHVybl8yNTJkOnMucmV0dXJuXzI1MmQsCiAgICAgIGZvcndhcmRfc3RhdHM6cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30sCiAgICAgIGFzb2Y6cy5sYXN0X2RhdGUsIHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpCiAgICB9OwogICAgY29uc3QgaWR4PWEuZmluZEluZGV4KHg9PnguY29kZT09PWNvZGUpOwogICAgaWYoaWR4Pj0wKWFbaWR4XT1oOyBlbHNlIGEucHVzaChoKTsKICAgIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICAgIHJlbmRlckhvbGRpbmdzKCk7CiAgfWNhdGNoKGUpe2FsZXJ0KCflj5blvpfjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KICBmaW5hbGx5e2lmKGJ0bil7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn6KiI566X44GX44Gm5L+d5a2YJ319Cn0KCmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hIb2xkaW5nKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyksIGg9YVtpXTsgaWYoIWgpcmV0dXJuOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoaC5jb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgIGguY29tcGFueV9uYW1lPShxLmNvbXBhbnkmJnEuY29tcGFueS5uYW1lKXx8aC5jb21wYW55X25hbWV8fCcnOwogICAgaC5jb21wYW55X21hcmtldD0ocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8aC5jb21wYW55X21hcmtldHx8Jyc7CiAgICBoLmNvbXBhbnlfc2VjdG9yMzM9KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8aC5jb21wYW55X3NlY3RvcjMzfHwnJzsKICAgIGguY3VycmVudF9wcmljZT1zLmxhc3RfY2xvc2U7IGguaGlnaF8yMGQ9cy5oaWdoXzIwZDsgaC5sb3dfMjBkPXMubG93XzIwZDsKICAgIGgucmV0dXJuXzIwZD1zLnJldHVybl8yMGQ7IGgucmV0dXJuXzEyNmQ9cy5yZXR1cm5fMTI2ZDsgaC5yZXR1cm5fMjUyZD1zLnJldHVybl8yNTJkOwogICAgaC5mb3J3YXJkX3N0YXRzPXMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9OwogICAgaC5hc29mPXMubGFzdF9kYXRlOyBoLnVwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogICAgYVtpXT1oOyBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7IHJlbmRlckhvbGRpbmdzKCk7CiAgfWNhdGNoKGUpe2FsZXJ0KCfmm7TmlrDjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KfQpmdW5jdGlvbiByZW1vdmVIb2xkaW5nKGkpe2NvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7YS5zcGxpY2UoaSwxKTtzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7cmVuZGVySG9sZGluZ3MoKX0KCmZ1bmN0aW9uIHJlbmRlcldhdGNoKCl7CiAgJCgnd2F0Y2hzJykuaW5uZXJIVE1MPWxvY2FsKCdmcmVlX3dhdGNoJykubWFwKHg9PnsKICAgIGlmKHR5cGVvZiB4PT09J3N0cmluZycpcmV0dXJuIGA8ZGl2IGNsYXNzPSJiYWRnZSI+JHt4fTwvZGl2PmA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImJhZGdlIj48Yj4ke3guY29kZX08L2I+JHt4Lm5hbWU/JyAnK3gubmFtZTonJ308L2Rpdj5gOwogIH0pLmpvaW4oJycpfHwnPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JzsKfQphc3luYyBmdW5jdGlvbiBhZGRXYXRjaCgpewogIGxldCBjPSQoJ3dhdGNoQ29kZScpLnZhbHVlLnRyaW0oKTsgaWYoIWMpcmV0dXJuOwogIGxldCBpbmZvPW51bGw7CiAgdHJ5e2luZm89YXdhaXQgZ2V0Q29tcGFueShjKX1jYXRjaChlKXt9CiAgbGV0IGE9bG9jYWwoJ2ZyZWVfd2F0Y2gnKTsKICBjb25zdCBleGlzdHM9YS5zb21lKHg9Pih0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKT09PWMpOwogIGlmKCFleGlzdHMpYS5wdXNoKHtjb2RlOmMsbmFtZTppbmZvJiZpbmZvLm5hbWU/aW5mby5uYW1lOicnfSk7CiAgc2F2ZSgnZnJlZV93YXRjaCcsYSk7CiAgcmVuZGVyV2F0Y2goKTsKfQpmdW5jdGlvbiB1cGRhdGVLYWJ1dGFuKCl7bGV0IGM9JCgnY29kZScpLnZhbHVlLnRyaW0oKTskKCdrYWJ1dGFuJykuaHJlZj1jPydodHRwczovL2thYnV0YW4uanAvc3RvY2svP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYyk6J2h0dHBzOi8va2FidXRhbi5qcC8nfQokKCdjb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT57dXBkYXRlS2FidXRhbigpO3NjaGVkdWxlQ29tcGFueUxvb2t1cCgnY29kZScsJ2NvbXBhbnlOYW1lJywnJyl9KTt1cGRhdGVLYWJ1dGFuKCk7CiQoJ2hvbGRDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2hvbGRDb2RlJywnaG9sZENvbXBhbnlOYW1lJywnJykpOwokKCd3YXRjaENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnd2F0Y2hDb2RlJywnd2F0Y2hDb21wYW55TmFtZScsJycpKTsKCmFzeW5jIGZ1bmN0aW9uIGFuYWx5emUoKXsKICBjb25zdCBjb2RlPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7CiAgaWYoIWNvZGUpeyQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfpipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjga0nO3JldHVybn0KICBjb25zdCBidG49JCgnYW5hbHl6ZUJ0bicpOyBidG4uZGlzYWJsZWQ9dHJ1ZTsgYnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnOwogICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSdKLVF1YW50c+OBi+OCieWun+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBhOOBvuOBmeKApic7CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShjb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgICQoJ3ByaWNlJykudmFsdWU9cy5sYXN0X2Nsb3NlPT1udWxsPycnOmZtdChzLmxhc3RfY2xvc2UsMSk7CiAgICAkKCdyMjAnKS52YWx1ZT1mbXQocy5yZXR1cm5fMjBkKTskKCdyMTI2JykudmFsdWU9Zm10KHMucmV0dXJuXzEyNmQpOyQoJ3IyNTInKS52YWx1ZT1mbXQocy5yZXR1cm5fMjUyZCk7CiAgICAkKCdoaWdoMjAnKS50ZXh0Q29udGVudD1mbXQocy5oaWdoXzIwZCwxKTskKCdsb3cyMCcpLnRleHRDb250ZW50PWZtdChzLmxvd18yMGQsMSk7CiAgICAkKCd2b2wyMCcpLnRleHRDb250ZW50PXMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZD09bnVsbD8n4oCUJzpmbXQocy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkKSsnJSc7CiAgICBkaXNwbGF5Q29tcGFueSgkKCdjb21wYW55TmFtZScpLHEuY29tcGFueXx8bnVsbCwnJyk7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9JzxiIGNsYXNzPSJvayI+4pyFIEotUXVhbnRz5a6f44OH44O844K/5Y+W5b6XT0s8L2I+PGJyPuacgOe1guODh+ODvOOCv+aXpTogJysocy5sYXN0X2RhdGV8fCfigJQnKSsnIC8g57WC5YCkOiAnK2ZtdChzLmxhc3RfY2xvc2UsMSkrJyAvIOOCteODs+ODl+ODqzogJysocy5zYW1wbGVfY291bnQ/PyfigJQnKSsn5Lu2JzsKICAgICQoJ2FuYWx5c2lzRXYnKS5pbm5lckhUTUw9ZXZIdG1sKCfnn63mnJ8yMOaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjBkJ10pK2V2SHRtbCgn5Lit5pyfMTI25pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycxMjZkJ10pK2V2SHRtbCgn6ZW35pyfMjUy5pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycyNTJkJ10pOwogICAgY29uc3QgZD17Y29kZSxwcmljZTpzLmxhc3RfY2xvc2UscmV0dXJuMjA6cy5yZXR1cm5fMjBkLHJldHVybjEyNjpzLnJldHVybl8xMjZkLHJldHVybjI1MjpzLnJldHVybl8yNTJkLGVhcm5pbmdzX3Njb3JlOnZhbCgnZWFybicpLHBvbGljeV9zY29yZTp2YWwoJ3BvbGljeScpLHN1cHBseV9zY29yZTp2YWwoJ3N1cHBseScpfTsKICAgIGNvbnN0IGFyPWF3YWl0IGZldGNoKCcvYXBpL2ZyZWUvYW5hbHl6ZScse21ldGhvZDonUE9TVCcsaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSxib2R5OkpTT04uc3RyaW5naWZ5KGQpLGNhY2hlOiduby1zdG9yZSd9KTsKICAgIGNvbnN0IHg9YXdhaXQgYXIuanNvbigpOwogICAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+WIhuaekOODh+ODvOOCv+OBjOS4jei2s+OBl+OBpuOBhOOBvuOBmScpOwogICAgJCgnc3RhdGUnKS50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTskKCdwb3MnKS50ZXh0Q29udGVudD14LnNpZ25hbC5wb3NpdGl2ZV9jb3VudDskKCduZWcnKS50ZXh0Q29udGVudD14LnNpZ25hbC5uZWdhdGl2ZV9jb3VudDsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr8gJyt4LnNpZ25hbC5ldmlkZW5jZV9jb3VudCsn5Lu244KS5qC55oug44Gr5Yik5a6a44CC5pyf5b6F5YCk44GvT09T5qCh5q2j44OH44O844K/44GM5Y2B5YiG44Gr44Gq44KL44G+44Gn5pyq6KGo56S644Gn44GZ44CCJzsKICB9Y2F0Y2goZSl7CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n4pqg77iPICcrZS5tZXNzYWdlOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPSc8c3BhbiBjbGFzcz0iZXJyIj7lj5blvpfjgqjjg6njg7w6ICcrZS5tZXNzYWdlKyc8L3NwYW4+JzsKICB9ZmluYWxseXtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafliIbmnpAnfQp9CgpyZW5kZXJIb2xkaW5ncygpO3JlbmRlcldhdGNoKCk7CmlmKCdzZXJ2aWNlV29ya2VyJyBpbiBuYXZpZ2F0b3Ipe25hdmlnYXRvci5zZXJ2aWNlV29ya2VyLmdldFJlZ2lzdHJhdGlvbnMoKS50aGVuKHJzPT5Qcm9taXNlLmFsbChycy5tYXAocj0+ci51bnJlZ2lzdGVyKCkpKSkuY2F0Y2goKCk9Pnt9KX0KaWYoJ2NhY2hlcycgaW4gd2luZG93KXtjYWNoZXMua2V5cygpLnRoZW4oa2V5cz0+UHJvbWlzZS5hbGwoa2V5cy5tYXAoaz0+Y2FjaGVzLmRlbGV0ZShrKSkpKS5jYXRjaCgoKT0+e30pfQo8L3NjcmlwdD4KPC9tYWluPgo8L2JvZHk+CjwvaHRtbD4="
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
