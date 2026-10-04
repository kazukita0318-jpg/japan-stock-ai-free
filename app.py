from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.5-COMPANY-NAME'
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
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KLmV2IHNtYWxse2Rpc3BsYXk6YmxvY2s7Y29sb3I6IzZiNzI4MDtsaW5lLWhlaWdodDoxLjQ1fQoKQG1lZGlhKG1heC13aWR0aDo0ODBweCl7LmdyaWQze2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyIDFmcn0ua3BpIGJ7Zm9udC1zaXplOjE2cHh9fQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8bWFpbj4KPGRpdiBjbGFzcz0idG9wIj4KICA8aDE+8J+TiCDml6XmnKzmoKpBSSBGUkVFPC9oMT4KICA8ZGl2IGNsYXNzPSJzdWIiPkotUXVhbnRz5a6f44OH44O844K/IC8g6YqY5p+E5ZCN6Ieq5YuV6KGo56S6IC8g5L+d5pyJ5Yik5patIC8g55+t5Lit6ZW35pyf5b6F5YCkPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfjq8g6YqY5p+E5YiG5p6QPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkIj4KICAgIDxpbnB1dCBpZD0iY29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIOS+iyA3MjAzIj4KICAgIDxpbnB1dCBpZD0icHJpY2UiIHBsYWNlaG9sZGVyPSLlj5blvpfntYLlgKQiIHJlYWRvbmx5PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbXBhbnlOYW1lIiBjbGFzcz0ic291cmNlIG11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBmeOCi+OBqOS8muekvuWQjeOCkuihqOekuuOBl+OBvuOBmTwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8YSBpZD0ia2FidXRhbiIgY2xhc3M9ImJ0biBzZWNvbmRhcnkiIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7moKrmjqLjgafnorroqo08L2E+CiAgICA8YnV0dG9uIGlkPSJhbmFseXplQnRuIiBvbmNsaWNrPSJhbmFseXplKCkiPuWun+ODh+ODvOOCv+OBp+WIhuaekDwvYnV0dG9uPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl44KM44Gm5oq844GZ44Go44CBSi1RdWFudHPjgYvjgonlj5blvpfjgafjgY3jgovlrp/jg4fjg7zjgr/jgpLoh6rli5XlhaXlipvjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBpZD0ic291cmNlQm94IiBjbGFzcz0ic291cmNlIG11dGVkIj7jg4fjg7zjgr/mnKrlj5blvpc8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+TiiDmoKrkvqHjg7vjg4bjgq/jg4vjgqvjg6vlrp/nuL48L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemosOiQveeOhyAlPC9zcGFuPjxpbnB1dCBpZD0icjIwIiByZWFkb25seT48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4xMjbml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIxMjYiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjI1MuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjI1MiIgcmVhZG9ubHk+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6Xpq5jlgKQ8L3NwYW4+PGIgaWQ9ImhpZ2gyMCI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel5a6J5YCkPC9zcGFuPjxiIGlkPSJsb3cyMCI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel5bm0546H44Oc44OpPC9zcGFuPjxiIGlkPSJ2b2wyMCI+4oCUPC9iPjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn6epIOijnOWKqeipleS+oTwvaDM+CiAgPHAgY2xhc3M9Im11dGVkIj7liIbjgYvjgovpoIXnm67jgaDjgZHlhaXlipvjgILmnKrlhaXlipvjga8w54K544Gn44Gv44Gq44GP44CM5LiN5piO44CN44Go44GX44Gm5omx44GE44G+44GZ44CCPC9wPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpcgLTEwMOOAnDEwMDwvc3Bhbj48aW5wdXQgaWQ9ImVhcm4iIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5pS/562WIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJwb2xpY3kiIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZyA57WmIC0xMDDjgJwxMDA8L3NwYW4+PGlucHV0IGlkPSJzdXBwbHkiIGlucHV0bW9kZT0iZGVjaW1hbCI+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6Ag5YiG5p6Q57WQ5p6cPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+54q25oWLPC9zcGFuPjxiIGlkPSJzdGF0ZSI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODl+ODqeOCueagueaLoDwvc3Bhbj48YiBpZD0icG9zIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44Oe44Kk44OK44K55qC55ougPC9zcGFuPjxiIGlkPSJuZWciPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KICA8cCBpZD0icmVzdWx0IiBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOAjOWun+ODh+ODvOOCv+OBp+WIhuaekOOAjeOCkuaKvOOBl+OBpuOBj+OBoOOBleOBhOOAgjwvcD4KICA8ZGl2IGlkPSJhbmFseXNpc0V2IiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkrwg5L+d5pyJ5qCq44O75pCN5YiH44KKL+WIqeeiujwvaDM+CiAgPGRpdiBjbGFzcz0ibm90ZSI+CiAgICDmkI3liIfjgorjg7vliKnnorrjg7vjg4jjg6zjg7zjg6rjg7PjgrDjga/lj4LogIPjg6njgqTjg7PjgILkv53mnInliKTmlq3jga/lrp/jg4fjg7zjgr/jgajoqK3lrprjg6njgqTjg7Pjga7jg6vjg7zjg6vliKTlrprjgafjgZnjgILnn63kuK3plbfjga/pgY7ljrvjga7jg63jg7zjg6rjg7PjgrDlrp/nuL7liIbluIPjgpLntbHoqIjlj4LogIPjgajjgZfjgabooajnpLrjgZfjgb7jgZnjgIIKICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZENvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8aW5wdXQgaWQ9ImhvbGRDb3N0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLlj5blvpfljZjkvqEiPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjZweCAycHggMCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZFNoYXJlcyIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i5qCq5pWwIj4KICAgIDxzZWxlY3QgaWQ9ImZlZU1vZGUiPgogICAgICA8b3B0aW9uIHZhbHVlPSJub211cmFfbmV0Ij7ph47mnZHjgqrjg7Pjg6njgqTjg7PlsILnlKjmlK/lupfjg7vnj77niak8L29wdGlvbj4KICAgICAgPG9wdGlvbiB2YWx1ZT0ibm9uZSI+5omL5pWw5paZ44Gq44GX77yI5q+U6LyD55So77yJPC9vcHRpb24+CiAgICA8L3NlbGVjdD4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoogJTwvc3Bhbj48aW5wdXQgaWQ9InN0b3BQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjgiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuiAlPC9zcGFuPjxpbnB1dCBpZD0idGFrZVBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iMTUiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODqyAlPC9zcGFuPjxpbnB1dCBpZD0idHJhaWxQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjciPjwvZGl2PgogIDwvZGl2PgogIDxidXR0b24gb25jbGljaz0iYWRkSG9sZGluZygpIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij7lrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZg8L2J1dHRvbj4KICA8cCBjbGFzcz0ibXV0ZWQiPumHjuadkeODjeODg+ODiO+8huOCs+ODvOODq++8j+OBu+OBo+OBqOODgOOCpOODrOOCr+ODiOOBruWbveWGheePvueJqeODu+OCquODs+ODqeOCpOODs+azqOaWh+OBrueojui+vOaJi+aVsOaWmeihqOOCkuS9v+eUqOOAgjwvcD4KICA8ZGl2IGlkPSJob2xkaW5ncyI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkYAg44Km44Kp44OD44OB44Oq44K544OIPC9oMz4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGlucHV0IGlkPSJ3YXRjaENvZGUiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPgogICAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRXYXRjaCgpIj7ov73liqA8L2J1dHRvbj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NHB4IDJweCA4cHgiPumKmOafhOWQje+8muKAlDwvZGl2PgogIDxkaXYgaWQ9IndhdGNocyI+PC9kaXY+CjwvZGl2PgoKPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKZnVuY3Rpb24gdmFsKGlkKXtsZXQgdj0kKGlkKS52YWx1ZS50cmltKCk7cmV0dXJuIHY9PT0nJz9udWxsOk51bWJlcih2KX0KZnVuY3Rpb24gbG9jYWwoayl7dHJ5e3JldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKGspfHwnW10nKX1jYXRjaChlKXtyZXR1cm5bXX19CmZ1bmN0aW9uIHNhdmUoayx2KXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrLEpTT04uc3RyaW5naWZ5KHYpKX0KZnVuY3Rpb24gZm10KHYsZD0yKXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoZCl9CmZ1bmN0aW9uIHllbih2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6J8KlJytNYXRoLnJvdW5kKE51bWJlcih2KSkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyl9CmZ1bmN0aW9uIHN0YXRlSmEocyl7cmV0dXJuIHM9PT0ncG9zaXRpdmUnPyfjg5fjg6njgrnlhKrli6InOnM9PT0nbmVnYXRpdmUnPyfjg57jgqTjg4rjgrnlhKrli6InOifmi67mipcnfQpmdW5jdGlvbiBwY3Qodil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDIpKyclJ30KZnVuY3Rpb24gc3RhdEZvcihoLGtleSl7cmV0dXJuIGgmJmguZm9yd2FyZF9zdGF0cyYmaC5mb3J3YXJkX3N0YXRzW2tleV0/aC5mb3J3YXJkX3N0YXRzW2tleV06bnVsbH0KZnVuY3Rpb24gZGVjaXNpb25Gb3IoaCxjKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgcjIwPU51bWJlcihoLnJldHVybl8yMGQpLCByMTI2PU51bWJlcihoLnJldHVybl8xMjZkKSwgcjI1Mj1OdW1iZXIoaC5yZXR1cm5fMjUyZCk7CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5pCN5YiH44KK5qSc6KiOJyxjbHM6J2Qtc3RvcCcscmVhc29uOiflj5blvpfljZjkvqHjgYvjgonoqK3lrprjgZfjgZ/mkI3liIfjgorlj4LogIPjg6njgqTjg7Pku6XkuIsnfTsKICB9CiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifliKnnorrmpJzoqI4nLGNsczonZC10YWtlJyxyZWFzb246J+WPluW+l+WNmOS+oeOBi+OCieioreWumuOBl+OBn+WIqeeiuuWPguiAg+ODqeOCpOODs+S7peS4iid9OwogIH0KICBpZihjdXI8PWMudHJhaWxQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpemrmOWApOWfuua6luOBruODiOODrOODvOODquODs+OCsOWPguiAg+ODqeOCpOODs+S7peS4iyd9OwogIH0KCiAgbGV0IHBvc2l0aXZlPTAsIG5lZ2F0aXZlPTA7CiAgW3IyMCxyMTI2LHIyNTJdLmZvckVhY2goeD0+ewogICAgaWYoTnVtYmVyLmlzRmluaXRlKHgpKXsKICAgICAgaWYoeD4wKXBvc2l0aXZlKys7CiAgICAgIGlmKHg8MCluZWdhdGl2ZSsrOwogICAgfQogIH0pOwoKICBpZihuZWdhdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpeODuzEyNuaXpeODuzI1MuaXpeOBruOBhuOBoeODnuOCpOODiuOCueWCvuWQkeOBjOWEquWLoid9OwogIH0KICBpZihwb3NpdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntponLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOBp+OAgeikh+aVsOacn+mWk+OBruS+oeagvOODiOODrOODs+ODieOBjOODl+ODqeOCuSd9OwogIH0KICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntprvvIjmp5jlrZDopovvvIknLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOAguacn+mWk+WIpeODiOODrOODs+ODieOBr+W8t+W8seOBjOa3t+WcqCd9Owp9CmZ1bmN0aW9uIGV2SHRtbCh0aXRsZSxzKXsKICBpZighc3x8cy5zdGF0dXMhPT0nb2snKXsKICAgIGNvbnN0IG49cyYmcy5uIT09dW5kZWZpbmVkP3MubjowOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7jg4fjg7zjgr/kuI3otrM8L2I+PHNtYWxsPuaomeacrCAke2595Lu2PC9zbWFsbD48L2Rpdj5gOwogIH0KICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+CiAgICA8Yj7lubPlnYcgJHtwY3Qocy5tZWFuKX08L2I+CiAgICA8c21hbGw+5Lit5aSu5YCkICR7cGN0KHMubWVkaWFuKX08L3NtYWxsPgogICAgPHNtYWxsPuS4iuaYh+eOhyAke3BjdChzLnBvc2l0aXZlX3JhdGUpfTwvc21hbGw+CiAgICA8c21hbGw+UDEw44CcUDkwICR7cGN0KHMucDEwKX0g44CcICR7cGN0KHMucDkwKX08L3NtYWxsPgogICAgPHNtYWxsPuaomeacrCAke3Mubn3ku7Y8L3NtYWxsPgogIDwvZGl2PmA7Cn0KCgpmdW5jdGlvbiBub211cmFOZXRGZWUoYW1vdW50KXsKICBhbW91bnQ9TnVtYmVyKGFtb3VudHx8MCk7CiAgaWYoYW1vdW50PD0wKXJldHVybiAwOwogIGlmKGFtb3VudDw9MTAwMDAwKXJldHVybiAxNTI7CiAgaWYoYW1vdW50PD0zMDAwMDApcmV0dXJuIDMzMDsKICBpZihhbW91bnQ8PTUwMDAwMClyZXR1cm4gNTI0OwogIGlmKGFtb3VudDw9MTAwMDAwMClyZXR1cm4gMTA0ODsKICBpZihhbW91bnQ8PTIwMDAwMDApcmV0dXJuIDIwOTU7CiAgaWYoYW1vdW50PD0zMDAwMDAwKXJldHVybiAzMTQzOwogIGlmKGFtb3VudDw9NTAwMDAwMClyZXR1cm4gNTIzODsKICBpZihhbW91bnQ8PTEwMDAwMDAwKXJldHVybiAxMDQ3NjsKICBpZihhbW91bnQ8PTIwMDAwMDAwKXJldHVybiAyMDk1MjsKICBpZihhbW91bnQ8PTMwMDAwMDAwKXJldHVybiAzMTQyOTsKICBpZihhbW91bnQ8PTUwMDAwMDAwKXJldHVybiA0MTkwNTsKICByZXR1cm4gNzg1NzE7Cn0KZnVuY3Rpb24gZmVlRm9yKGFtb3VudCxtb2RlKXtyZXR1cm4gbW9kZT09PSdub211cmFfbmV0Jz9ub211cmFOZXRGZWUoYW1vdW50KTowfQoKZnVuY3Rpb24gY2FsY0hvbGRpbmcoaCl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IGNvc3Q9TnVtYmVyKGguY29zdCk7CiAgY29uc3Qgc2hhcmVzPU51bWJlcihoLnNoYXJlcyk7CiAgY29uc3QgYnV5VmFsdWU9Y29zdCpzaGFyZXM7CiAgY29uc3QgYnV5RmVlPWZlZUZvcihidXlWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBjdXJyZW50VmFsdWU9Y3VyKnNoYXJlczsKICBjb25zdCBzZWxsRmVlPWZlZUZvcihjdXJyZW50VmFsdWUsaC5mZWVfbW9kZSk7CiAgY29uc3QgaW52ZXN0ZWQ9YnV5VmFsdWUrYnV5RmVlOwogIGNvbnN0IG5ldE5vdz1jdXJyZW50VmFsdWUtc2VsbEZlZS1pbnZlc3RlZDsKICBjb25zdCBuZXROb3dQY3Q9aW52ZXN0ZWQ/bmV0Tm93L2ludmVzdGVkKjEwMDpudWxsOwoKICBjb25zdCBzdG9wUHJpY2U9Y29zdCooMS1OdW1iZXIoaC5zdG9wX3BjdCkvMTAwKTsKICBjb25zdCB0YWtlUHJpY2U9Y29zdCooMStOdW1iZXIoaC50YWtlX3BjdCkvMTAwKTsKICBjb25zdCBoaWdoMjA9TnVtYmVyKGguaGlnaF8yMGR8fGN1cik7CiAgY29uc3QgdHJhaWxQcmljZT1oaWdoMjAqKDEtTnVtYmVyKGgudHJhaWxfcGN0KS8xMDApOwoKICBjb25zdCBzdG9wVmFsdWU9c3RvcFByaWNlKnNoYXJlczsKICBjb25zdCB0YWtlVmFsdWU9dGFrZVByaWNlKnNoYXJlczsKICBjb25zdCBzdG9wTmV0PXN0b3BWYWx1ZS1mZWVGb3Ioc3RvcFZhbHVlLGguZmVlX21vZGUpLWludmVzdGVkOwogIGNvbnN0IHRha2VOZXQ9dGFrZVZhbHVlLWZlZUZvcih0YWtlVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CgogIHJldHVybiB7YnV5VmFsdWUsYnV5RmVlLGN1cnJlbnRWYWx1ZSxzZWxsRmVlLGludmVzdGVkLG5ldE5vdyxuZXROb3dQY3Qsc3RvcFByaWNlLHRha2VQcmljZSx0cmFpbFByaWNlLHN0b3BOZXQsdGFrZU5ldH07Cn0KCgpjb25zdCBuYW1lVGltZXJzPXt9Owpjb25zdCBuYW1lQ2FjaGU9e307CgpmdW5jdGlvbiBkaXNwbGF5Q29tcGFueSh0YXJnZXQsaW5mbyxwcmVmaXg9JycpewogIGlmKCF0YXJnZXQpcmV0dXJuOwogIGlmKCFpbmZvfHwhaW5mby5uYW1lKXsKICAgIHRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICByZXR1cm47CiAgfQogIGxldCBleHRyYXM9W107CiAgaWYoaW5mby5tYXJrZXQpZXh0cmFzLnB1c2goaW5mby5tYXJrZXQpOwogIGlmKGluZm8uc2VjdG9yMzMpZXh0cmFzLnB1c2goaW5mby5zZWN0b3IzMyk7CiAgdGFyZ2V0LmlubmVySFRNTD0nPGI+JytwcmVmaXgraW5mby5uYW1lKyc8L2I+JysoZXh0cmFzLmxlbmd0aD8nPGJyPjxzcGFuIGNsYXNzPSJtdXRlZCI+JytleHRyYXMuam9pbignIC8gJykrJzwvc3Bhbj4nOicnKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0Q29tcGFueShjb2RlKXsKICBjb25zdCBjPVN0cmluZyhjb2RlfHwnJykudHJpbSgpOwogIGlmKCFjKXJldHVybiBudWxsOwogIGlmKG5hbWVDYWNoZVtjXSlyZXR1cm4gbmFtZUNhY2hlW2NdOwogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3NlY3VyaXR5P2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYykse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfpipjmn4TlkI3jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICBuYW1lQ2FjaGVbY109eDsKICByZXR1cm4geDsKfQoKZnVuY3Rpb24gc2NoZWR1bGVDb21wYW55TG9va3VwKGlucHV0SWQsdGFyZ2V0SWQscHJlZml4PScnKXsKICBjbGVhclRpbWVvdXQobmFtZVRpbWVyc1tpbnB1dElkXSk7CiAgY29uc3QgYz0kKGlucHV0SWQpLnZhbHVlLnRyaW0oKTsKICBjb25zdCB0YXJnZXQ9JCh0YXJnZXRJZCk7CgogIGlmKGMubGVuZ3RoPDQpewogICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muKAlCc7CiAgICByZXR1cm47CiAgfQoKICBuYW1lVGltZXJzW2lucHV0SWRdPXNldFRpbWVvdXQoYXN5bmMoKT0+ewogICAgdHJ5ewogICAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PSfpipjmn4TlkI3jgpLnorroqo3kuK3igKYnOwogICAgICBjb25zdCBpbmZvPWF3YWl0IGdldENvbXBhbnkoYyk7CiAgICAgIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeCk7CiAgICB9Y2F0Y2goZSl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nOwogICAgfQogIH0sNDUwKTsKfQoKZnVuY3Rpb24gcmVuZGVySG9sZGluZ3MoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGlmKCFhLmxlbmd0aCl7JCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7cmV0dXJufQogICQoJ2hvbGRpbmdzJykuaW5uZXJIVE1MPWEubWFwKChoLGkpPT57CiAgICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogICAgY29uc3QgY2xzPWMubmV0Tm93Pj0wPydwb3MnOiduZWcnOwogICAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJob2xkaW5nIj4KICAgICAgPGRpdiBjbGFzcz0iaG9sZGluZy1oZWFkIj4KICAgICAgICA8ZGl2PjxiPiR7aC5jb2RlfTwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+JHtoLmNvbXBhbnlfbmFtZXx8IiJ9PC9kaXY+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0icm93Ij4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIHNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaEhvbGRpbmcoJHtpfSkiPuabtOaWsDwvYnV0dG9uPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gZGFuZ2VyIiBvbmNsaWNrPSJyZW1vdmVIb2xkaW5nKCR7aX0pIj7liYrpmaQ8L2J1dHRvbj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+44OH44O844K/5pelICR7aC5hc29mfHwn4oCUJ30gLyDmnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4oaC5jdXJyZW50X3ByaWNlKX0gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZGVjaXNpb24gJHtkLmNsc30iPgogICAgICAgICR7ZC5sYWJlbH0KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NHB4Ij4ke2QucmVhc29ufTwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrzmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7Y2xzfSI+JHt5ZW4oYy5uZXROb3cpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7Zm10KGMubmV0Tm93UGN0KX0lPC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7osrfku5jmiYvmlbDmlpk8L3NwYW4+PGI+JHt5ZW4oYy5idXlGZWUpfTwvYj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5aOy5Y205omL5pWw5paZKOS7iik8L3NwYW4+PGI+JHt5ZW4oYy5zZWxsRmVlKX08L2I+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnN0b3BQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMuc3RvcE5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy50YWtlUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnRha2VOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844Oq44Oz44Kw5Y+C6ICDPC9zcGFuPjxiPiR7eWVuKGMudHJhaWxQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6Xpq5jlgKTln7rmupY8L3NwYW4+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TkCDlrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtldkh0bWwoJ+efreacnyAyMOaXpScsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+S4reacnyAxMjbml6UnLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke2V2SHRtbCgn6ZW35pyfIDI1MuaXpScsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KICAgICAgPHAgY2xhc3M9Im11dGVkIj7igLvlsIbmnaXkuojmuKzjgafjga/jgarjgY/jgIHjgZPjga7pipjmn4Tjga7lj5blvpflj6/og73jgarpgY7ljrvmoKrkvqHjgYvjgonoqIjnrpfjgZfjgZ/jg63jg7zjg6rjg7PjgrDliY3mlrnjg6rjgr/jg7zjg7PliIbluIPjgafjgZnjgILmnJ/plpPjgYzph43jgarjgovmqJnmnKzjgpLlkKvjgb/jgb7jgZnjgII8L3A+CiAgICA8L2Rpdj5gOwogIH0pLmpvaW4oJycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRRdW90ZShjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9xdW90ZT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgcT1hd2FpdCByLmpzb24oKTsKICBpZihxLnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHEucmVhc29ufHxxLmVycm9yfHwn5a6f44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgcmV0dXJuIHE7Cn0KCmFzeW5jIGZ1bmN0aW9uIGFkZEhvbGRpbmcoKXsKICBjb25zdCBjb2RlPSQoJ2hvbGRDb2RlJykudmFsdWUudHJpbSgpOwogIGNvbnN0IGNvc3Q9dmFsKCdob2xkQ29zdCcpLCBzaGFyZXM9dmFsKCdob2xkU2hhcmVzJyk7CiAgY29uc3Qgc3RvcD12YWwoJ3N0b3BQY3QnKSwgdGFrZT12YWwoJ3Rha2VQY3QnKSwgdHJhaWw9dmFsKCd0cmFpbFBjdCcpOwogIGNvbnN0IGZlZU1vZGU9JCgnZmVlTW9kZScpLnZhbHVlOwogIGlmKCFjb2RlfHwhY29zdHx8IXNoYXJlcyl7YWxlcnQoJ+mKmOafhOOCs+ODvOODieODu+WPluW+l+WNmOS+oeODu+agquaVsOOCkuWFpeWKm+OBl+OBpuOBrScpO3JldHVybn0KICBjb25zdCBidG49ZXZlbnQ/LnRhcmdldDsgaWYoYnRuKXtidG4uZGlzYWJsZWQ9dHJ1ZTtidG4udGV4dENvbnRlbnQ9J+WPluW+l+S4reKApid9CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShjb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgICBjb25zdCBoPXsKICAgICAgY29kZSwKICAgICAgY29tcGFueV9uYW1lOihxLmNvbXBhbnkmJnEuY29tcGFueS5uYW1lKXx8JycsCiAgICAgIGNvbXBhbnlfbWFya2V0OihxLmNvbXBhbnkmJnEuY29tcGFueS5tYXJrZXQpfHwnJywKICAgICAgY29tcGFueV9zZWN0b3IzMzoocS5jb21wYW55JiZxLmNvbXBhbnkuc2VjdG9yMzMpfHwnJywKICAgICAgY29zdCwgc2hhcmVzLCBmZWVfbW9kZTpmZWVNb2RlLAogICAgICBzdG9wX3BjdDpzdG9wPz84LCB0YWtlX3BjdDp0YWtlPz8xNSwgdHJhaWxfcGN0OnRyYWlsPz83LAogICAgICBjdXJyZW50X3ByaWNlOnMubGFzdF9jbG9zZSwgaGlnaF8yMGQ6cy5oaWdoXzIwZCwgbG93XzIwZDpzLmxvd18yMGQsCiAgICAgIHJldHVybl8yMGQ6cy5yZXR1cm5fMjBkLCByZXR1cm5fMTI2ZDpzLnJldHVybl8xMjZkLCByZXR1cm5fMjUyZDpzLnJldHVybl8yNTJkLAogICAgICBmb3J3YXJkX3N0YXRzOnMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9LAogICAgICBhc29mOnMubGFzdF9kYXRlLCB1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKQogICAgfTsKICAgIGNvbnN0IGlkeD1hLmZpbmRJbmRleCh4PT54LmNvZGU9PT1jb2RlKTsKICAgIGlmKGlkeD49MClhW2lkeF09aDsgZWxzZSBhLnB1c2goaCk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSl9CiAgZmluYWxseXtpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmCd9fQp9Cgphc3luYyBmdW5jdGlvbiByZWZyZXNoSG9sZGluZyhpKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpLCBoPWFbaV07IGlmKCFoKXJldHVybjsKICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGguY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBoLmNvbXBhbnlfbmFtZT0ocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fGguY29tcGFueV9uYW1lfHwnJzsKICAgIGguY29tcGFueV9tYXJrZXQ9KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fGguY29tcGFueV9tYXJrZXR8fCcnOwogICAgaC5jb21wYW55X3NlY3RvcjMzPShxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fGguY29tcGFueV9zZWN0b3IzM3x8Jyc7CiAgICBoLmN1cnJlbnRfcHJpY2U9cy5sYXN0X2Nsb3NlOyBoLmhpZ2hfMjBkPXMuaGlnaF8yMGQ7IGgubG93XzIwZD1zLmxvd18yMGQ7CiAgICBoLnJldHVybl8yMGQ9cy5yZXR1cm5fMjBkOyBoLnJldHVybl8xMjZkPXMucmV0dXJuXzEyNmQ7IGgucmV0dXJuXzI1MmQ9cy5yZXR1cm5fMjUyZDsKICAgIGguZm9yd2FyZF9zdGF0cz1zLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fTsKICAgIGguYXNvZj1zLmxhc3RfZGF0ZTsgaC51cGRhdGVkX2F0PW5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTsKICAgIGFbaV09aDsgc2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpOyByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5pu05paw44Ko44Op44O8OiAnK2UubWVzc2FnZSl9Cn0KZnVuY3Rpb24gcmVtb3ZlSG9sZGluZyhpKXtjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpO2Euc3BsaWNlKGksMSk7c2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpO3JlbmRlckhvbGRpbmdzKCl9CgpmdW5jdGlvbiByZW5kZXJXYXRjaCgpewogICQoJ3dhdGNocycpLmlubmVySFRNTD1sb2NhbCgnZnJlZV93YXRjaCcpLm1hcCh4PT57CiAgICBpZih0eXBlb2YgeD09PSdzdHJpbmcnKXJldHVybiBgPGRpdiBjbGFzcz0iYmFkZ2UiPiR7eH08L2Rpdj5gOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJiYWRnZSI+PGI+JHt4LmNvZGV9PC9iPiR7eC5uYW1lPycgJyt4Lm5hbWU6Jyd9PC9kaXY+YDsKICB9KS5qb2luKCcnKXx8JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7Cn0KYXN5bmMgZnVuY3Rpb24gYWRkV2F0Y2goKXsKICBsZXQgYz0kKCd3YXRjaENvZGUnKS52YWx1ZS50cmltKCk7IGlmKCFjKXJldHVybjsKICBsZXQgaW5mbz1udWxsOwogIHRyeXtpbmZvPWF3YWl0IGdldENvbXBhbnkoYyl9Y2F0Y2goZSl7fQogIGxldCBhPWxvY2FsKCdmcmVlX3dhdGNoJyk7CiAgY29uc3QgZXhpc3RzPWEuc29tZSh4PT4odHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSk9PT1jKTsKICBpZighZXhpc3RzKWEucHVzaCh7Y29kZTpjLG5hbWU6aW5mbyYmaW5mby5uYW1lP2luZm8ubmFtZTonJ30pOwogIHNhdmUoJ2ZyZWVfd2F0Y2gnLGEpOwogIHJlbmRlcldhdGNoKCk7Cn0KZnVuY3Rpb24gdXBkYXRlS2FidXRhbigpe2xldCBjPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7JCgna2FidXRhbicpLmhyZWY9Yz8naHR0cHM6Ly9rYWJ1dGFuLmpwL3N0b2NrLz9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpOidodHRwczovL2thYnV0YW4uanAvJ30KJCgnY29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+e3VwZGF0ZUthYnV0YW4oKTtzY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2NvZGUnLCdjb21wYW55TmFtZScsJycpfSk7dXBkYXRlS2FidXRhbigpOwokKCdob2xkQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCdob2xkQ29kZScsJ2hvbGRDb21wYW55TmFtZScsJycpKTsKJCgnd2F0Y2hDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ3dhdGNoQ29kZScsJ3dhdGNoQ29tcGFueU5hbWUnLCcnKSk7Cgphc3luYyBmdW5jdGlvbiBhbmFseXplKCl7CiAgY29uc3QgY29kZT0kKCdjb2RlJykudmFsdWUudHJpbSgpOwogIGlmKCFjb2RlKXskKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44GtJztyZXR1cm59CiAgY29uc3QgYnRuPSQoJ2FuYWx5emVCdG4nKTsgYnRuLmRpc2FibGVkPXRydWU7IGJ0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJzsKICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0nSi1RdWFudHPjgYvjgonlrp/jg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKYnOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICAkKCdwcmljZScpLnZhbHVlPXMubGFzdF9jbG9zZT09bnVsbD8nJzpmbXQocy5sYXN0X2Nsb3NlLDEpOwogICAgJCgncjIwJykudmFsdWU9Zm10KHMucmV0dXJuXzIwZCk7JCgncjEyNicpLnZhbHVlPWZtdChzLnJldHVybl8xMjZkKTskKCdyMjUyJykudmFsdWU9Zm10KHMucmV0dXJuXzI1MmQpOwogICAgJCgnaGlnaDIwJykudGV4dENvbnRlbnQ9Zm10KHMuaGlnaF8yMGQsMSk7JCgnbG93MjAnKS50ZXh0Q29udGVudD1mbXQocy5sb3dfMjBkLDEpOwogICAgJCgndm9sMjAnKS50ZXh0Q29udGVudD1zLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQ9PW51bGw/J+KAlCc6Zm10KHMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZCkrJyUnOwogICAgZGlzcGxheUNvbXBhbnkoJCgnY29tcGFueU5hbWUnKSxxLmNvbXBhbnl8fG51bGwsJycpOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPSc8YiBjbGFzcz0ib2siPuKchSBKLVF1YW50c+Wun+ODh+ODvOOCv+WPluW+l09LPC9iPjxicj7mnIDntYLjg4fjg7zjgr/ml6U6ICcrKHMubGFzdF9kYXRlfHwn4oCUJykrJyAvIOe1guWApDogJytmbXQocy5sYXN0X2Nsb3NlLDEpKycgLyDjgrXjg7Pjg5fjg6s6ICcrKHMuc2FtcGxlX2NvdW50Pz8n4oCUJykrJ+S7tic7CiAgICAkKCdhbmFseXNpc0V2JykuaW5uZXJIVE1MPWV2SHRtbCgn55+t5pyfMjDml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzIwZCddKStldkh0bWwoJ+S4reacnzEyNuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMTI2ZCddKStldkh0bWwoJ+mVt+acnzI1MuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjUyZCddKTsKICAgIGNvbnN0IGQ9e2NvZGUscHJpY2U6cy5sYXN0X2Nsb3NlLHJldHVybjIwOnMucmV0dXJuXzIwZCxyZXR1cm4xMjY6cy5yZXR1cm5fMTI2ZCxyZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCxlYXJuaW5nc19zY29yZTp2YWwoJ2Vhcm4nKSxwb2xpY3lfc2NvcmU6dmFsKCdwb2xpY3knKSxzdXBwbHlfc2NvcmU6dmFsKCdzdXBwbHknKX07CiAgICBjb25zdCBhcj1hd2FpdCBmZXRjaCgnL2FwaS9mcmVlL2FuYWx5emUnLHttZXRob2Q6J1BPU1QnLGhlYWRlcnM6eydDb250ZW50LVR5cGUnOidhcHBsaWNhdGlvbi9qc29uJ30sYm9keTpKU09OLnN0cmluZ2lmeShkKSxjYWNoZTonbm8tc3RvcmUnfSk7CiAgICBjb25zdCB4PWF3YWl0IGFyLmpzb24oKTsKICAgIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfliIbmnpDjg4fjg7zjgr/jgYzkuI3otrPjgZfjgabjgYTjgb7jgZknKTsKICAgICQoJ3N0YXRlJykudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7JCgncG9zJykudGV4dENvbnRlbnQ9eC5zaWduYWwucG9zaXRpdmVfY291bnQ7JCgnbmVnJykudGV4dENvbnRlbnQ9eC5zaWduYWwubmVnYXRpdmVfY291bnQ7CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n5a6f44OH44O844K/ICcreC5zaWduYWwuZXZpZGVuY2VfY291bnQrJ+S7tuOCkuagueaLoOOBq+WIpOWumuOAguacn+W+heWApOOBr09PU+agoeato+ODh+ODvOOCv+OBjOWNgeWIhuOBq+OBquOCi+OBvuOBp+acquihqOekuuOBp+OBmeOAgic7CiAgfWNhdGNoKGUpewogICAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+KaoO+4jyAnK2UubWVzc2FnZTsKICAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0nPHNwYW4gY2xhc3M9ImVyciI+5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSsnPC9zcGFuPic7CiAgfWZpbmFsbHl7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn5YiG5p6QJ30KfQoKcmVuZGVySG9sZGluZ3MoKTtyZW5kZXJXYXRjaCgpOwppZignc2VydmljZVdvcmtlcicgaW4gbmF2aWdhdG9yKXtuYXZpZ2F0b3Iuc2VydmljZVdvcmtlci5nZXRSZWdpc3RyYXRpb25zKCkudGhlbihycz0+UHJvbWlzZS5hbGwocnMubWFwKHI9PnIudW5yZWdpc3RlcigpKSkpLmNhdGNoKCgpPT57fSl9CmlmKCdjYWNoZXMnIGluIHdpbmRvdyl7Y2FjaGVzLmtleXMoKS50aGVuKGtleXM9PlByb21pc2UuYWxsKGtleXMubWFwKGs9PmNhY2hlcy5kZWxldGUoaykpKSkuY2F0Y2goKCk9Pnt9KX0KPC9zY3JpcHQ+CjwvbWFpbj4KPC9ib2R5Pgo8L2h0bWw+"
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
