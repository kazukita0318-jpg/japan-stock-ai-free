from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.9-FUNDAMENTAL-SCORE'
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



def to_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def clamp(value, lo=-100.0, hi=100.0):
    return max(lo, min(hi, value))


def growth_pct(current, previous):
    current = to_float(current)
    previous = to_float(previous)
    if current is None or previous in (None, 0):
        return None
    return (current / previous - 1.0) * 100.0


def growth_score(g):
    if g is None:
        return None
    if g >= 20:
        return 100.0
    if g >= 10:
        return 75.0
    if g >= 3:
        return 45.0
    if g >= 0:
        return 20.0
    if g > -5:
        return -20.0
    if g > -15:
        return -55.0
    return -90.0


def financial_summary(code):
    jq_code = jquants_code(code)
    data = jq_request("/fins/summary", {"code": jq_code})
    rows = (
        data.get("data")
        or data.get("summary")
        or data.get("statements")
        or data.get("financials")
        or []
    )

    norm = []
    for row in rows:
        norm.append({
            "date": row.get("DiscDate") or row.get("DisclosedDate") or "",
            "period": row.get("CurPerType") or row.get("TypeOfCurrentPeriod") or "",
            "sales": to_float(row.get("Sales") if "Sales" in row else row.get("NetSales")),
            "op": to_float(row.get("OP") if "OP" in row else row.get("OperatingProfit")),
            "np": to_float(row.get("NP") if "NP" in row else row.get("Profit")),
            "eps": to_float(row.get("EPS") if "EPS" in row else row.get("EarningsPerShare")),
            "feps": to_float(row.get("FEPS") if "FEPS" in row else row.get("ForecastEarningsPerShare")),
            "equity": to_float(row.get("Eq") if "Eq" in row else row.get("Equity")),
            "assets": to_float(row.get("TA") if "TA" in row else row.get("TotalAssets")),
        })

    norm = [r for r in norm if r["date"]]
    norm.sort(key=lambda r: r["date"])
    if not norm:
        return {
            "status": "insufficient_data",
            "score": None,
            "reason": "financial summary not available",
        }

    latest = norm[-1]
    previous = None

    # Prefer the previous disclosure with the same accounting period type,
    # which makes year-on-year comparison less misleading.
    for row in reversed(norm[:-1]):
        if latest["period"] and row["period"] == latest["period"]:
            previous = row
            break

    if previous is None and len(norm) >= 2:
        previous = norm[-2]

    components = []
    detail = {}

    if previous is not None:
        for key, weight in [
            ("sales", 0.25),
            ("op", 0.35),
            ("np", 0.20),
            ("eps", 0.20),
        ]:
            g = growth_pct(latest.get(key), previous.get(key))
            s = growth_score(g)
            detail[key + "_growth_pct"] = g
            if s is not None:
                components.append((s, weight))

    if components:
        weight_sum = sum(w for _, w in components)
        score = sum(s * w for s, w in components) / weight_sum
        coverage = weight_sum
    else:
        score = None
        coverage = 0.0

    equity_ratio = None
    if latest.get("equity") is not None and latest.get("assets"):
        equity_ratio = latest["equity"] / latest["assets"] * 100.0

    return {
        "status": "ok" if score is not None else "partial",
        "score": round(score, 2) if score is not None else None,
        "coverage": round(coverage * 100.0, 1),
        "latest": latest,
        "previous": previous,
        "metrics": detail,
        "equity_ratio_pct": equity_ratio,
        "source": "J-Quants API v2 /fins/summary",
        "method": "comparable-period growth rule score",
    }


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
    valid_rows = [
        r for r in rows
        if isinstance(r.get("close"), (int, float))
    ]
    closes = [r["close"] for r in valid_rows]
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

    r20 = ret(20)
    r126 = ret(126)
    r252 = ret(252)
    high20 = max(closes[-20:]) if len(closes) >= 20 else max(closes)
    low20 = min(closes[-20:]) if len(closes) >= 20 else min(closes)

    volumes = [
        to_float(r.get("volume"))
        for r in valid_rows
        if to_float(r.get("volume")) is not None
    ]
    volume_ratio_5_20 = None
    if len(volumes) >= 20:
        avg5 = statistics.fmean(volumes[-5:])
        avg20 = statistics.fmean(volumes[-20:])
        if avg20:
            volume_ratio_5_20 = avg5 / avg20

    range_position = None
    if high20 != low20:
        range_position = (closes[-1] - low20) / (high20 - low20)

    supply_parts = []
    if r20 is not None:
        supply_parts.append(clamp(r20 * 4.0, -60.0, 60.0))
    if volume_ratio_5_20 is not None and r20 is not None:
        if volume_ratio_5_20 >= 1.20:
            supply_parts.append(30.0 if r20 >= 0 else -30.0)
        elif volume_ratio_5_20 <= 0.80:
            supply_parts.append(-5.0 if r20 >= 0 else 5.0)
        else:
            supply_parts.append(0.0)
    if range_position is not None:
        supply_parts.append(clamp((range_position - 0.5) * 40.0, -20.0, 20.0))

    supply_score = (
        statistics.fmean(supply_parts)
        if supply_parts
        else None
    )

    return {
        "status": "ok",
        "last_close": closes[-1],
        "last_date": valid_rows[-1]["date"],
        "return_20d": r20,
        "return_126d": r126,
        "return_252d": r252,
        "volatility_20d_annualized": vol(20),
        "high_20d": high20,
        "low_20d": low20,
        "sample_count": len(closes),
        "supply_proxy": {
            "status": "ok" if supply_score is not None else "insufficient_data",
            "score": round(supply_score, 2) if supply_score is not None else None,
            "volume_ratio_5_20": volume_ratio_5_20,
            "range_position": range_position,
            "method": "real-price-volume proxy; not margin-balance data",
        },
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


@APP.get("/api/mobile/fundamentals")
def mobile_fundamentals():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400
    if not jquants_status():
        return jsonify(
            status="unavailable",
            reason="J-Quants API key is not configured",
        )
    try:
        data = financial_summary(code)
        return jsonify(status="ok", code=code, fundamentals=data)
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

    def trend_score():
        parts=[]
        if r20 is not None:
            parts.append((clamp(r20*5.0), 0.40))
        if r126 is not None:
            parts.append((clamp(r126*3.0), 0.35))
        if r252 is not None:
            parts.append((clamp(r252*2.0), 0.25))
        if not parts:
            return None
        w=sum(weight for _,weight in parts)
        return sum(score*weight for score,weight in parts)/w

    tech=trend_score()
    factor_defs=[
        ("technical", tech, 0.40),
        ("earnings", earnings, 0.30),
        ("supply", supply, 0.20),
        ("policy", policy, 0.10),
    ]
    available=[x for x in factor_defs if x[1] is not None]
    total_weight=sum(x[2] for x in available)
    raw_score=(
        sum(clamp(x[1])*x[2] for x in available)/total_weight
        if total_weight else None
    )
    score100=(
        round((raw_score+100.0)/2.0, 1)
        if raw_score is not None else None
    )

    return jsonify(
        status="ok", code=code, mode="real_data_rule_score",
        price=price,
        signal={
            "state":state,
            "positive_count":positives,
            "negative_count":negatives,
            "evidence_count":len(supplied)
        },
        score={
            "score100":score100,
            "raw_minus100_to100":round(raw_score,2) if raw_score is not None else None,
            "coverage_pct":round(total_weight*100.0,1),
            "technical":round(tech,2) if tech is not None else None,
            "earnings":earnings,
            "supply":supply,
            "policy":policy,
            "method":"deterministic available-factor weighted score",
        },
        expected_value={
            "status":"unavailable",
            "20d":None,"126d":None,"252d":None,
            "reason":"Expected values stay unavailable until enough OOS-calibrated history is collected"
        },
        sources={
            "market_data":"J-Quants real market data",
            "earnings":"J-Quants financial summary when available",
            "supply":"real price/volume proxy; not margin balance",
            "policy":"manual only unless separately connected"
        }
    )


HTML = base64.b64decode(
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KLnNjb3JlaGVybyBie2ZvbnQtc2l6ZTozNHB4O2Rpc3BsYXk6YmxvY2s7bGluZS1oZWlnaHQ6MX0KQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmZhY3RvcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5wb3J0cm93e2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyfX0KCgoKQG1lZGlhKG1heC13aWR0aDo0ODBweCl7LmdyaWQze2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyIDFmcn0ua3BpIGJ7Zm9udC1zaXplOjE2cHh9fQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8bWFpbj4KPGRpdiBjbGFzcz0idG9wIj4KICA8aDE+8J+TiCDml6XmnKzmoKpBSSBGUkVFPC9oMT4KICA8ZGl2IGNsYXNzPSJzdWIiPkotUXVhbnRz5a6f44OH44O844K/IC8g5rG6566X6Ieq5YuV5o6h54K5IC8g6ZyA57WmcHJveHkgLyDnt4/lkIjmjqHngrkgLyDkv53mnInliKTmlq08L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+OryDpipjmn4TliIbmnpA8L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQiPgogICAgPGlucHV0IGlkPSJjb2RlIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kg5L6LIDcyMDMiPgogICAgPGlucHV0IGlkPSJwcmljZSIgcGxhY2Vob2xkZXI9IuWPluW+l+e1guWApCIgcmVhZG9ubHk+CiAgPC9kaXY+CiAgPGRpdiBpZD0iY29tcGFueU5hbWUiIGNsYXNzPSJzb3VyY2UgbXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GZ44KL44Go5Lya56S+5ZCN44KS6KGo56S644GX44G+44GZPC9kaXY+CiAgPGRpdiBjbGFzcz0icm93Ij4KICAgIDxhIGlkPSJrYWJ1dGFuIiBjbGFzcz0iYnRuIHNlY29uZGFyeSIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPuagquaOouOBp+eiuuiqjTwvYT4KICAgIDxidXR0b24gaWQ9ImFuYWx5emVCdG4iIG9uY2xpY2s9ImFuYWx5emUoKSI+5a6f44OH44O844K/44Gn5YiG5p6QPC9idXR0b24+CiAgPC9kaXY+CiAgPHAgY2xhc3M9Im11dGVkIj7pipjmn4TjgrPjg7zjg4njgpLlhaXjgozjgabmirzjgZnjgajjgIFKLVF1YW50c+OBi+OCieWPluW+l+OBp+OBjeOCi+Wun+ODh+ODvOOCv+OCkuiHquWLleWFpeWKm+OBl+OBvuOBmeOAgjwvcD4KICA8ZGl2IGlkPSJzb3VyY2VCb3giIGNsYXNzPSJzb3VyY2UgbXV0ZWQiPuODh+ODvOOCv+acquWPluW+lzwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5OKIOagquS+oeODu+ODhuOCr+ODi+OCq+ODq+Wun+e4vjwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6aiw6JC9546HICU8L3NwYW4+PGlucHV0IGlkPSJyMjAiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjEyNuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjEyNiIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjUy5pelICU8L3NwYW4+PGlucHV0IGlkPSJyMjUyIiByZWFkb25seT48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemrmOWApDwvc3Bhbj48YiBpZD0iaGlnaDIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlronlgKQ8L3NwYW4+PGIgaWQ9ImxvdzIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlubTnjofjg5zjg6k8L3NwYW4+PGIgaWQ9InZvbDIwIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6kg5a6f44OH44O844K/6KaB5ZugPC9oMz4KICA8cCBjbGFzcz0ibXV0ZWQiPuaxuueul+OBr0otUXVhbnRz6LKh5YuZ44K144Oe44Oq44O844CB6ZyA57Wm44Gv5a6f5qCq5L6h44O75Ye65p2l6auYcHJveHnjgYvjgonoh6rli5XmjqHngrnjgILlm73nrZbjgaDjgZHjga/nj77mmYLngrnjgafjga/miYvlhaXlipvjgafjgIHmnKrlhaXlipvjga/jgIzkuI3mmI7jgI3jgafjgZnjgII8L3A+CiAgPGRpdiBjbGFzcz0iZmFjdG9yZ3JpZCI+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5rG6566XPC9zcGFuPjxiIGlkPSJlYXJuQXV0byI+4oCUPC9iPjxzbWFsbCBpZD0iZWFybkRldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrlj5blvpc8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPumcgOe1pnByb3h5PC9zcGFuPjxiIGlkPSJzdXBwbHlBdXRvIj7igJQ8L2I+PHNtYWxsIGlkPSJzdXBwbHlEZXRhaWwiIGNsYXNzPSJtdXRlZCI+5pyq5Y+W5b6XPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7lm73nrZY8L3NwYW4+PGIgaWQ9InBvbGljeVN0YXRlIj7kuI3mmI48L2I+PHNtYWxsIGNsYXNzPSJtdXRlZCI+6Ieq5YuV5o6l57aa44Gv5qyh5q616ZqOPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg4fjg7zjgr/lhYXotrM8L3NwYW4+PGIgaWQ9ImNvdmVyYWdlIj7igJQ8L2I+PHNtYWxsIGNsYXNzPSJtdXRlZCI+57eP5ZCI5o6h54K544Gr5L2/44GI44Gf6YeN44G/PC9zbWFsbD48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IHN0eWxlPSJtYXJnaW4tdG9wOjlweCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetluOCueOCs+OCou+8iOS7u+aEj++8iSAtMTAw44CcMTAwPC9zcGFuPgogICAgPGlucHV0IGlkPSJwb2xpY3kiIGlucHV0bW9kZT0iZGVjaW1hbCIgcGxhY2Vob2xkZXI9IuacquWFpeWKm+OBquOCieS4jeaYjiI+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6Ag5YiG5p6Q57WQ5p6cPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+54q25oWLPC9zcGFuPjxiIGlkPSJzdGF0ZSI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODl+ODqeOCueagueaLoDwvc3Bhbj48YiBpZD0icG9zIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44Oe44Kk44OK44K55qC55ougPC9zcGFuPjxiIGlkPSJuZWciPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJzY29yZUhlcm8iIGNsYXNzPSJzY29yZWhlcm8iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7nt4/lkIjmjqHngrnvvIjlrp/jg4fjg7zjgr/jg7vjg6vjg7zjg6vjg5njg7zjgrnvvIk8L3NwYW4+CiAgICA8YiBpZD0ic2NvcmUxMDAiPuKAlDwvYj4KICAgIDxzcGFuIGlkPSJzY29yZUJyZWFrZG93biIgY2xhc3M9Im11dGVkIj48L3NwYW4+CiAgPC9kaXY+CiAgPHAgaWQ9InJlc3VsdCIgY2xhc3M9Im11dGVkIj7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjgIzlrp/jg4fjg7zjgr/jgafliIbmnpDjgI3jgpLmirzjgZfjgabjgY/jgaDjgZXjgYTjgII8L3A+CiAgPGRpdiBpZD0iYW5hbHlzaXNFdiIgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPjwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5qoIOS7iuaXpeOBruWEquWFiOOCouOCr+OCt+ODp+ODszwvaDM+CiAgPGRpdiBpZD0icHJpb3JpdHlBY3Rpb25zIj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go44CB5YSq5YWI44GX44Gm56K66KqN44GZ44KL6YqY5p+E44KS6Ieq5YuV6KGo56S644GX44G+44GZ44CCPC9wPgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQgcG9ydGZvbGlvIj4KICA8aDM+8J+nrSDjg53jg7zjg4jjg5Xjgqnjg6rjgqrlhajkvZM8L2gzPgogIDxkaXYgaWQ9InBvcnRmb2xpb1N1bW1hcnkiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkrwg5L+d5pyJ5qCq44O75pCN5YiH44KKL+WIqeeiujwvaDM+CiAgPGRpdiBjbGFzcz0ibm90ZSI+CiAgICDmkI3liIfjgorjg7vliKnnorrjg7vjg4jjg6zjg7zjg6rjg7PjgrDjga/lj4LogIPjg6njgqTjg7PjgILkv53mnInliKTmlq3jga/lrp/jg4fjg7zjgr/jgajoqK3lrprjg6njgqTjg7Pjga7jg6vjg7zjg6vliKTlrprjgafjgZnjgILnn63kuK3plbfjga/pgY7ljrvjga7jg63jg7zjg6rjg7PjgrDlrp/nuL7liIbluIPjgpLntbHoqIjlj4LogIPjgajjgZfjgabooajnpLrjgZfjgb7jgZnjgIIKICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZENvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8aW5wdXQgaWQ9ImhvbGRDb3N0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLlj5blvpfljZjkvqEiPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjZweCAycHggMCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZFNoYXJlcyIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i5qCq5pWwIj4KICAgIDxzZWxlY3QgaWQ9ImZlZU1vZGUiPgogICAgICA8b3B0aW9uIHZhbHVlPSJub211cmFfbmV0Ij7ph47mnZHjgqrjg7Pjg6njgqTjg7PlsILnlKjmlK/lupfjg7vnj77niak8L29wdGlvbj4KICAgICAgPG9wdGlvbiB2YWx1ZT0ibm9uZSI+5omL5pWw5paZ44Gq44GX77yI5q+U6LyD55So77yJPC9vcHRpb24+CiAgICA8L3NlbGVjdD4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoogJTwvc3Bhbj48aW5wdXQgaWQ9InN0b3BQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjgiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuiAlPC9zcGFuPjxpbnB1dCBpZD0idGFrZVBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iMTUiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODqyAlPC9zcGFuPjxpbnB1dCBpZD0idHJhaWxQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjciPjwvZGl2PgogIDwvZGl2PgogIDxidXR0b24gb25jbGljaz0iYWRkSG9sZGluZygpIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij7lrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZg8L2J1dHRvbj4KICA8cCBjbGFzcz0ibXV0ZWQiPumHjuadkeODjeODg+ODiO+8huOCs+ODvOODq++8j+OBu+OBo+OBqOODgOOCpOODrOOCr+ODiOOBruWbveWGheePvueJqeODu+OCquODs+ODqeOCpOODs+azqOaWh+OBrueojui+vOaJi+aVsOaWmeihqOOCkuS9v+eUqOOAgjwvcD4KICA8ZGl2IGlkPSJob2xkaW5ncyI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkYAg44Km44Kp44OD44OB44Oq44K544OIPC9oMz4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGlucHV0IGlkPSJ3YXRjaENvZGUiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPgogICAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRXYXRjaCgpIj7ov73liqA8L2J1dHRvbj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NHB4IDJweCA4cHgiPumKmOafhOWQje+8muKAlDwvZGl2PgogIDxkaXYgaWQ9IndhdGNocyI+PC9kaXY+CjwvZGl2PgoKPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKZnVuY3Rpb24gdmFsKGlkKXtsZXQgdj0kKGlkKS52YWx1ZS50cmltKCk7cmV0dXJuIHY9PT0nJz9udWxsOk51bWJlcih2KX0KZnVuY3Rpb24gbG9jYWwoayl7dHJ5e3JldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKGspfHwnW10nKX1jYXRjaChlKXtyZXR1cm5bXX19CmZ1bmN0aW9uIHNhdmUoayx2KXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrLEpTT04uc3RyaW5naWZ5KHYpKX0KZnVuY3Rpb24gZm10KHYsZD0yKXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoZCl9CmZ1bmN0aW9uIHllbih2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6J8KlJytNYXRoLnJvdW5kKE51bWJlcih2KSkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyl9CmZ1bmN0aW9uIHN0YXRlSmEocyl7cmV0dXJuIHM9PT0ncG9zaXRpdmUnPyfjg5fjg6njgrnlhKrli6InOnM9PT0nbmVnYXRpdmUnPyfjg57jgqTjg4rjgrnlhKrli6InOifmi67mipcnfQpmdW5jdGlvbiBwY3Qodil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDIpKyclJ30KZnVuY3Rpb24gc3RhdEZvcihoLGtleSl7cmV0dXJuIGgmJmguZm9yd2FyZF9zdGF0cyYmaC5mb3J3YXJkX3N0YXRzW2tleV0/aC5mb3J3YXJkX3N0YXRzW2tleV06bnVsbH0KZnVuY3Rpb24gY29uZmlkZW5jZUZvcihoKXsKICBjb25zdCB2YWxzPVtoLnJldHVybl8yMGQsaC5yZXR1cm5fMTI2ZCxoLnJldHVybl8yNTJkXS5tYXAoTnVtYmVyKS5maWx0ZXIoTnVtYmVyLmlzRmluaXRlKTsKICBjb25zdCBzdGF0cz1bJzIwZCcsJzEyNmQnLCcyNTJkJ10ubWFwKGs9PnN0YXRGb3IoaCxrKSkuZmlsdGVyKHM9PnMmJnMuc3RhdHVzPT09J29rJyk7CgogIGlmKCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpfHwhaC5hc29mKXJldHVybiAwOwoKICBjb25zdCBjb3ZlcmFnZT12YWxzLmxlbmd0aC8zOwogIGxldCBhZ3JlZW1lbnQ9MC41OwogIGlmKHZhbHMubGVuZ3RoKXsKICAgIGNvbnN0IHBvcz12YWxzLmZpbHRlcih4PT54PjApLmxlbmd0aDsKICAgIGNvbnN0IG5lZz12YWxzLmZpbHRlcih4PT54PDApLmxlbmd0aDsKICAgIGFncmVlbWVudD1NYXRoLm1heChwb3MsbmVnKS92YWxzLmxlbmd0aDsKICB9CiAgY29uc3Qgc3RhdENvdmVyYWdlPXN0YXRzLmxlbmd0aC8zOwoKICByZXR1cm4gTWF0aC5yb3VuZCgoY292ZXJhZ2UqMC40NSArIGFncmVlbWVudCowLjI1ICsgc3RhdENvdmVyYWdlKjAuMzApKjEwMCk7Cn0KCmZ1bmN0aW9uIGRlY2lzaW9uRm9yKGgsYyl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhaC5hc29mKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246J+Wun+ODh+ODvOOCv+acquWPluW+l+OAguWPs+S4iuOBruOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44GX44Gm44GP44Gg44GV44GE44CCJywKICAgICAgY29uZmlkZW5jZTowCiAgICB9OwogIH0KCiAgY29uc3QgcjIwPU51bWJlcihoLnJldHVybl8yMGQpLCByMTI2PU51bWJlcihoLnJldHVybl8xMjZkKSwgcjI1Mj1OdW1iZXIoaC5yZXR1cm5fMjUyZCk7CiAgY29uc3QgY29uZmlkZW5jZT1jb25maWRlbmNlRm9yKGgpOwoKICBpZihjdXI8PWMuc3RvcFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+aQjeWIh+OCiuaknOiojicsY2xzOidkLXN0b3AnLHJlYXNvbjon6Kit5a6a44GX44Gf5pCN5YiH44KK5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifliKnnorrmpJzoqI4nLGNsczonZC10YWtlJyxyZWFzb246J+ioreWumuOBl+OBn+WIqeeiuuWPguiAg+ODqeOCpOODs+S7peS4iicsY29uZmlkZW5jZX07CiAgfQogIGlmKGN1cjw9Yy50cmFpbFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel6auY5YCk5Z+65rqW44Gu44OI44Os44O844Oq44Oz44Kw5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CgogIGxldCBwb3NpdGl2ZT0wLCBuZWdhdGl2ZT0wOwogIFtyMjAscjEyNixyMjUyXS5mb3JFYWNoKHg9PnsKICAgIGlmKE51bWJlci5pc0Zpbml0ZSh4KSl7CiAgICAgIGlmKHg+MClwb3NpdGl2ZSsrOwogICAgICBpZih4PDApbmVnYXRpdmUrKzsKICAgIH0KICB9KTsKCiAgaWYobmVnYXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon6K2m5oiSJyxjbHM6J2Qtd2F0Y2gnLHJlYXNvbjonMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7jgYbjgaHjg57jgqTjg4rjgrnlgr7lkJHjgYzlhKrli6InLGNvbmZpZGVuY2V9OwogIH0KICBpZihwb3NpdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntponLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOBp+OAgeikh+aVsOacn+mWk+OBruS+oeagvOODiOODrOODs+ODieOBjOODl+ODqeOCuScsY29uZmlkZW5jZX07CiAgfQogIHJldHVybiB7bGFiZWw6J+S/neaciee2mee2mu+8iOanmOWtkOimi++8iScsY2xzOidkLWhvbGQnLHJlYXNvbjon6Kit5a6a44Op44Kk44Oz5YaF44CC5pyf6ZaT5Yil44OI44Os44Oz44OJ44Gv5by35byx44GM5re35ZyoJyxjb25maWRlbmNlfTsKfQpmdW5jdGlvbiBldkh0bWwodGl0bGUscyl7CiAgaWYoIXN8fHMuc3RhdHVzIT09J29rJyl7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzbWFsbD7mqJnmnKwgJHtufeS7tjwvc21hbGw+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPgogICAgPGI+5bmz5Z2HICR7cGN0KHMubWVhbil9PC9iPgogICAgPHNtYWxsPuS4reWkruWApCAke3BjdChzLm1lZGlhbil9PC9zbWFsbD4KICAgIDxzbWFsbD7kuIrmmIfnjocgJHtwY3Qocy5wb3NpdGl2ZV9yYXRlKX08L3NtYWxsPgogICAgPHNtYWxsPlAxMOOAnFA5MCAke3BjdChzLnAxMCl9IOOAnCAke3BjdChzLnA5MCl9PC9zbWFsbD4KICAgIDxzbWFsbD7mqJnmnKwgJHtzLm595Lu2PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgoKZnVuY3Rpb24gZGlzdGFuY2VJbmZvKGN1cix0YXJnZXQsa2luZCl7CiAgY3VyPU51bWJlcihjdXIpOyB0YXJnZXQ9TnVtYmVyKHRhcmdldCk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFOdW1iZXIuaXNGaW5pdGUodGFyZ2V0KSlyZXR1cm4gJ+KAlCc7CiAgY29uc3QgZGlmZj0odGFyZ2V0L2N1ci0xKSoxMDA7CiAgaWYoa2luZD09PSdzdG9wJyl7CiAgICBpZihkaWZmPj0wKXJldHVybiAn44Op44Kk44Oz5Yiw6YGU5riI44G/JzsKICAgIHJldHVybiBNYXRoLmFicyhkaWZmKS50b0ZpeGVkKDIpKyclIOS4iyc7CiAgfQogIGlmKGtpbmQ9PT0ndGFrZScpewogICAgaWYoZGlmZjw9MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gZGlmZi50b0ZpeGVkKDIpKyclIOS4iic7CiAgfQogIHJldHVybiAoZGlmZj49MD8nKyc6JycpK2RpZmYudG9GaXhlZCgyKSsnJSc7Cn0KCmZ1bmN0aW9uIHByaWNlUmFuZ2UoY3VyLHMpewogIGN1cj1OdW1iZXIoY3VyKTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IXN8fHMuc3RhdHVzIT09J29rJylyZXR1cm4gbnVsbDsKICByZXR1cm4gewogICAgbG93OmN1ciooMStOdW1iZXIocy5wMTApLzEwMCksCiAgICBoaWdoOmN1ciooMStOdW1iZXIocy5wOTApLzEwMCksCiAgICBtZWRpYW46Y3VyKigxK051bWJlcihzLm1lZGlhbikvMTAwKQogIH07Cn0KCmZ1bmN0aW9uIHJhbmdlSHRtbCh0aXRsZSxjdXIscyl7CiAgY29uc3Qgcj1wcmljZVJhbmdlKGN1cixzKTsKICBpZighcil7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5qiZ5pysICR7bn3ku7Y8L3NwYW4+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJyYW5nZWJveCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9IFAxMOOAnFA5MDwvc3Bhbj4KICAgIDxiPiR7eWVuKHIubG93KX0g44CcICR7eWVuKHIuaGlnaCl9PC9iPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7kuK3lpK7lgKTmj5vnrpcgJHt5ZW4oci5tZWRpYW4pfTwvc3Bhbj4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiBhY3Rpb25UZXh0KGgsYyxkKXsKICBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmScpcmV0dXJuICfjgb7jgZrlrp/jg4fjg7zjgr/jgpLmm7TmlrDjgZfjgabjgYvjgonliKTmlq3jgIInOwogIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylyZXR1cm4gJ+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+OCkuS4i+WbnuOBo+OBpuOBhOOBvuOBmeOAguWun+mam+OBruePvuWcqOWApOOBqOazqOaWh+adoeS7tuOCkueiuuiqjeOBl+OBpuOAgee4ruWwj+ODu+aSpOmAgOOCkuaknOiojuOAgic7CiAgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKXJldHVybiAn5Yip56K65Y+C6ICD44Op44Kk44Oz44Gr5Yiw6YGU44GX44Gm44GE44G+44GZ44CC5YWo6YOo5aOy5Y2044Gg44GR44Gn44Gq44GP44CB5YiG5Ymy5Yip56K644KC5YCZ6KOc44CCJzsKICBpZihkLmxhYmVsPT09J+itpuaIkicpcmV0dXJuICforabmiJLjgr7jg7zjg7PjgILjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PjgajkuK3nn63mnJ/jga7lgKTli5XjgY3jgpLlhKrlhYjjgZfjgabnorroqo3jgIInOwogIHJldHVybiAn6Kit5a6a44Op44Kk44Oz5YaF44CC5L+d5pyJ57aZ57aa5YCZ6KOc44Gn44GZ44GM44CB54Sh5paZ44OH44O844K/44Gv6YGF5bu244GZ44KL44Gf44KB5a6f6Zqb44Gu54++5Zyo5YCk44KC56K66KqN44CCJzsKfQoKCmZ1bmN0aW9uIG5vbXVyYU5ldEZlZShhbW91bnQpewogIGFtb3VudD1OdW1iZXIoYW1vdW50fHwwKTsKICBpZihhbW91bnQ8PTApcmV0dXJuIDA7CiAgaWYoYW1vdW50PD0xMDAwMDApcmV0dXJuIDE1MjsKICBpZihhbW91bnQ8PTMwMDAwMClyZXR1cm4gMzMwOwogIGlmKGFtb3VudDw9NTAwMDAwKXJldHVybiA1MjQ7CiAgaWYoYW1vdW50PD0xMDAwMDAwKXJldHVybiAxMDQ4OwogIGlmKGFtb3VudDw9MjAwMDAwMClyZXR1cm4gMjA5NTsKICBpZihhbW91bnQ8PTMwMDAwMDApcmV0dXJuIDMxNDM7CiAgaWYoYW1vdW50PD01MDAwMDAwKXJldHVybiA1MjM4OwogIGlmKGFtb3VudDw9MTAwMDAwMDApcmV0dXJuIDEwNDc2OwogIGlmKGFtb3VudDw9MjAwMDAwMDApcmV0dXJuIDIwOTUyOwogIGlmKGFtb3VudDw9MzAwMDAwMDApcmV0dXJuIDMxNDI5OwogIGlmKGFtb3VudDw9NTAwMDAwMDApcmV0dXJuIDQxOTA1OwogIHJldHVybiA3ODU3MTsKfQpmdW5jdGlvbiBmZWVGb3IoYW1vdW50LG1vZGUpe3JldHVybiBtb2RlPT09J25vbXVyYV9uZXQnP25vbXVyYU5ldEZlZShhbW91bnQpOjB9CgpmdW5jdGlvbiBjYWxjSG9sZGluZyhoKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgY29zdD1OdW1iZXIoaC5jb3N0KTsKICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzKTsKICBjb25zdCBidXlWYWx1ZT1jb3N0KnNoYXJlczsKICBjb25zdCBidXlGZWU9ZmVlRm9yKGJ1eVZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGN1cnJlbnRWYWx1ZT1jdXIqc2hhcmVzOwogIGNvbnN0IHNlbGxGZWU9ZmVlRm9yKGN1cnJlbnRWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBpbnZlc3RlZD1idXlWYWx1ZStidXlGZWU7CiAgY29uc3QgbmV0Tm93PWN1cnJlbnRWYWx1ZS1zZWxsRmVlLWludmVzdGVkOwogIGNvbnN0IG5ldE5vd1BjdD1pbnZlc3RlZD9uZXROb3cvaW52ZXN0ZWQqMTAwOm51bGw7CgogIGNvbnN0IHN0b3BQcmljZT1jb3N0KigxLU51bWJlcihoLnN0b3BfcGN0KS8xMDApOwogIGNvbnN0IHRha2VQcmljZT1jb3N0KigxK051bWJlcihoLnRha2VfcGN0KS8xMDApOwogIGNvbnN0IGhpZ2gyMD1OdW1iZXIoaC5oaWdoXzIwZHx8Y3VyKTsKICBjb25zdCB0cmFpbFByaWNlPWhpZ2gyMCooMS1OdW1iZXIoaC50cmFpbF9wY3QpLzEwMCk7CgogIGNvbnN0IHN0b3BWYWx1ZT1zdG9wUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHRha2VWYWx1ZT10YWtlUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHN0b3BOZXQ9c3RvcFZhbHVlLWZlZUZvcihzdG9wVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CiAgY29uc3QgdGFrZU5ldD10YWtlVmFsdWUtZmVlRm9yKHRha2VWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKCiAgcmV0dXJuIHtidXlWYWx1ZSxidXlGZWUsY3VycmVudFZhbHVlLHNlbGxGZWUsaW52ZXN0ZWQsbmV0Tm93LG5ldE5vd1BjdCxzdG9wUHJpY2UsdGFrZVByaWNlLHRyYWlsUHJpY2Usc3RvcE5ldCx0YWtlTmV0fTsKfQoKCmNvbnN0IG5hbWVUaW1lcnM9e307CmNvbnN0IG5hbWVDYWNoZT17fTsKCmZ1bmN0aW9uIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeD0nJyl7CiAgaWYoIXRhcmdldClyZXR1cm47CiAgaWYoIWluZm98fCFpbmZvLm5hbWUpewogICAgdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIHJldHVybjsKICB9CiAgbGV0IGV4dHJhcz1bXTsKICBpZihpbmZvLm1hcmtldClleHRyYXMucHVzaChpbmZvLm1hcmtldCk7CiAgaWYoaW5mby5zZWN0b3IzMylleHRyYXMucHVzaChpbmZvLnNlY3RvcjMzKTsKICB0YXJnZXQuaW5uZXJIVE1MPSc8Yj4nK3ByZWZpeCtpbmZvLm5hbWUrJzwvYj4nKyhleHRyYXMubGVuZ3RoPyc8YnI+PHNwYW4gY2xhc3M9Im11dGVkIj4nK2V4dHJhcy5qb2luKCcgLyAnKSsnPC9zcGFuPic6JycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRDb21wYW55KGNvZGUpewogIGNvbnN0IGM9U3RyaW5nKGNvZGV8fCcnKS50cmltKCk7CiAgaWYoIWMpcmV0dXJuIG51bGw7CiAgaWYobmFtZUNhY2hlW2NdKXJldHVybiBuYW1lQ2FjaGVbY107CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvc2VjdXJpdHk/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+mKmOafhOWQjeOCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIG5hbWVDYWNoZVtjXT14OwogIHJldHVybiB4Owp9CgpmdW5jdGlvbiBzY2hlZHVsZUNvbXBhbnlMb29rdXAoaW5wdXRJZCx0YXJnZXRJZCxwcmVmaXg9JycpewogIGNsZWFyVGltZW91dChuYW1lVGltZXJzW2lucHV0SWRdKTsKICBjb25zdCBjPSQoaW5wdXRJZCkudmFsdWUudHJpbSgpOwogIGNvbnN0IHRhcmdldD0kKHRhcmdldElkKTsKCiAgaWYoYy5sZW5ndGg8NCl7CiAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya4oCUJzsKICAgIHJldHVybjsKICB9CgogIG5hbWVUaW1lcnNbaW5wdXRJZF09c2V0VGltZW91dChhc3luYygpPT57CiAgICB0cnl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9J+mKmOafhOWQjeOCkueiuuiqjeS4reKApic7CiAgICAgIGNvbnN0IGluZm89YXdhaXQgZ2V0Q29tcGFueShjKTsKICAgICAgZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4KTsKICAgIH1jYXRjaChlKXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICB9CiAgfSw0NTApOwp9CgoKCmZ1bmN0aW9uIHByaW9yaXR5QWN0aW9uRm9yKGgpewogIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogIGNvbnN0IG5hbWU9aC5jb21wYW55X25hbWV8fCcnOwogIGNvbnN0IGxhYmVsPShoLmNvZGV8fCcnKSsobmFtZT8nICcrbmFtZTonJyk7CiAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwoKICBpZighdmFsaWQpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTYsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOiflrp/jg4fjg7zjgr/jgpLmm7TmlrAnLAogICAgICBkZXRhaWw6J+acgOaWsOWPluW+l+e1guWApOOBjOOBguOCiuOBvuOBm+OCk+OAguOBvuOBmuOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44CCJywKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBzdG9wRGlzdD0oY3VyL2Muc3RvcFByaWNlLTEpKjEwMDsKICBjb25zdCB0YWtlRGlzdD0oYy50YWtlUHJpY2UvY3VyLTEpKjEwMDsKICBjb25zdCB0cmFpbERpc3Q9KGN1ci9jLnRyYWlsUHJpY2UtMSkqMTAwOwoKICBpZihjdXI8PWMuc3RvcFByaWNlKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjEwMCwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+aQjeWIh+OCiuODqeOCpOODs+WIsOmBlCcsCiAgICAgIGRldGFpbDpg5pyA5paw5Y+W5b6X57WC5YCkICR7eWVuKGN1cil9IC8g5pCN5YiH44KK5Y+C6ICDICR7eWVuKGMuc3RvcFByaWNlKX1gLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKHN0b3BEaXN0PD0zKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjk0LAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5pCN5YiH44KK44Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjgYLjgaggJHtzdG9wRGlzdC50b0ZpeGVkKDIpfSUg44Gn5pCN5YiH44KK5Y+C6ICD44Op44Kk44OzYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihjdXI+PWMudGFrZVByaWNlKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjkwLAogICAgICBjbHM6J3ByaW9yaXR5LXRha2UnLAogICAgICB0aXRsZTon5Yip56K644Op44Kk44Oz5Yiw6YGUJywKICAgICAgZGV0YWlsOmDmnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4oY3VyKX0gLyDliKnnorrlj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYodGFrZURpc3Q8PTMpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6ODQsCiAgICAgIGNsczoncHJpb3JpdHktdGFrZScsCiAgICAgIHRpdGxlOifliKnnorrjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOOBguOBqCAke3Rha2VEaXN0LnRvRml4ZWQoMil9JSDjgafliKnnorrlj4LogIPjg6njgqTjg7NgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKGQubGFiZWw9PT0n6K2m5oiSJyl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo4MCwKICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICB0aXRsZTon6K2m5oiS5Yik5a6aJywKICAgICAgZGV0YWlsOmQucmVhc29uLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKE51bWJlci5pc0Zpbml0ZSh0cmFpbERpc3QpJiZ0cmFpbERpc3Q8PTIuNSl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo3NiwKICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICB0aXRsZTon44OI44Os44O844Oq44Oz44Kw44Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjg4jjg6zjg7zjg6rjg7PjgrDlj4LogIMgJHt5ZW4oYy50cmFpbFByaWNlKX0g44G+44GnICR7dHJhaWxEaXN0LnRvRml4ZWQoMil9JWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgcmV0dXJuIHsKICAgIHNjb3JlOjMwLAogICAgY2xzOidwcmlvcml0eS1nb29kJywKICAgIHRpdGxlOifpgJrluLjnm6PoppYnLAogICAgZGV0YWlsOmQucmVhc29ufHwn6Kit5a6a44Op44Kk44Oz5YaFJywKICAgIGxhYmVsCiAgfTsKfQoKZnVuY3Rpb24gcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBib3g9JCgncHJpb3JpdHlBY3Rpb25zJyk7CiAgaWYoIWJveClyZXR1cm47CgogIGlmKCFhLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOOAgeWEquWFiOOBl+OBpueiuuiqjeOBmeOCi+mKmOafhOOCkuiHquWLleihqOekuuOBl+OBvuOBmeOAgjwvcD4nOwogICAgcmV0dXJuOwogIH0KCiAgbGV0IGFjdGlvbnM9YS5tYXAocHJpb3JpdHlBY3Rpb25Gb3IpOwoKICAvLyBDb25jZW50cmF0aW9uIGFsZXJ0IChwb3J0Zm9saW8tbGV2ZWwpCiAgY29uc3QgdmFsaWQ9YS5maWx0ZXIoaD0+TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCYmaC5hc29mKTsKICBjb25zdCB0b3RhbD12YWxpZC5yZWR1Y2UoKHMsaCk9PnMrTnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKSwwKTsKICBpZih0b3RhbD4wKXsKICAgIGxldCBtYXhIb2xkaW5nPW51bGwsIG1heFZhbHVlPTA7CiAgICB2YWxpZC5mb3JFYWNoKGg9PnsKICAgICAgY29uc3Qgdj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApOwogICAgICBpZih2Pm1heFZhbHVlKXttYXhWYWx1ZT12O21heEhvbGRpbmc9aH0KICAgIH0pOwogICAgY29uc3QgY29uY2VudHJhdGlvbj1tYXhWYWx1ZS90b3RhbCoxMDA7CiAgICBpZihjb25jZW50cmF0aW9uPj02MCAmJiBtYXhIb2xkaW5nKXsKICAgICAgYWN0aW9ucy5wdXNoKHsKICAgICAgICBzY29yZTo3MiwKICAgICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgICAgdGl0bGU6J+mbhuS4reW6puOCkueiuuiqjScsCiAgICAgICAgZGV0YWlsOmAke21heEhvbGRpbmcuY29kZX0ke21heEhvbGRpbmcuY29tcGFueV9uYW1lPycgJyttYXhIb2xkaW5nLmNvbXBhbnlfbmFtZTonJ30g44GM44Od44O844OI44OV44Kp44Oq44Kq44GuICR7Y29uY2VudHJhdGlvbi50b0ZpeGVkKDEpfSVgLAogICAgICAgIGxhYmVsOifjg53jg7zjg4jjg5Xjgqnjg6rjgqonCiAgICAgIH0pOwogICAgfQogIH0KCiAgYWN0aW9ucy5zb3J0KCh4LHkpPT55LnNjb3JlLXguc2NvcmUpOwoKICBjb25zdCBpbXBvcnRhbnQ9YWN0aW9ucy5maWx0ZXIoeD0+eC5zY29yZT49NzApOwogIGNvbnN0IHNob3duPShpbXBvcnRhbnQubGVuZ3RoP2ltcG9ydGFudDphY3Rpb25zKS5zbGljZSgwLDQpOwoKICBib3guaW5uZXJIVE1MPWA8ZGl2IGNsYXNzPSJwcmlvcml0eS13cmFwIj4kewogICAgc2hvd24ubWFwKCh4LGkpPT5gCiAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWl0ZW0gJHt4LmNsc30iPgogICAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWxpbmUiPgogICAgICAgICAgPGRpdj4KICAgICAgICAgICAgPHNwYW4gY2xhc3M9InByaW9yaXR5LXJhbmsiPlBSSU9SSVRZICR7aSsxfTwvc3Bhbj4KICAgICAgICAgICAgPGI+JHt4LnRpdGxlfTwvYj4KICAgICAgICAgIDwvZGl2PgogICAgICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktY29kZSI+JHt4LmxhYmVsfTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPiR7eC5kZXRhaWx9PC9kaXY+CiAgICAgIDwvZGl2PgogICAgYCkuam9pbignJykKICB9PC9kaXY+YCArICgKICAgIGltcG9ydGFudC5sZW5ndGgKICAgICAgPyAnPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPuKAu+WEquWFiOW6puOBr+ioreWumuODqeOCpOODs+aOpei/keODu+WIpOWumueKtuaFi+ODu+ODh+ODvOOCv+acieeEoeODu+mbhuS4reW6puOBi+OCieS9nOOCi+eiuuiqjemghuOBp+OBmeOAguiHquWLleWjsuiyt+aMh+ekuuOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4nCiAgICAgIDogJzxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7nt4rmgKXluqbjga7pq5jjgYTpoIXnm67jga/jgYLjgorjgb7jgZvjgpPjgILpgJrluLjnm6PoppbjgpLntpnntprjgII8L3A+JwogICk7Cn0KCmZ1bmN0aW9uIHBvcnRmb2xpb01lYW5Gb3IoYSxrZXkpewogIGNvbnN0IHZhbGlkPWEuZmlsdGVyKGg9Pk51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjApOwogIGNvbnN0IHRvdGFsPXZhbGlkLnJlZHVjZSgocyxoKT0+cytOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApLDApOwogIGlmKHRvdGFsPD0wKXJldHVybiBudWxsOwoKICBsZXQgbnVtPTAsIGRlbj0wOwogIHZhbGlkLmZvckVhY2goaD0+ewogICAgY29uc3Qgc3Q9c3RhdEZvcihoLGtleSk7CiAgICBjb25zdCB2PU51bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCk7CiAgICBpZihzdCYmc3Quc3RhdHVzPT09J29rJyYmTnVtYmVyLmlzRmluaXRlKE51bWJlcihzdC5tZWFuKSkmJnY+MCl7CiAgICAgIG51bSArPSB2Kk51bWJlcihzdC5tZWFuKTsKICAgICAgZGVuICs9IHY7CiAgICB9CiAgfSk7CiAgcmV0dXJuIGRlbj4wP251bS9kZW46bnVsbDsKfQoKZnVuY3Rpb24gcmVuZGVyUG9ydGZvbGlvU3VtbWFyeSgpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgYm94PSQoJ3BvcnRmb2xpb1N1bW1hcnknKTsKICBpZighYm94KXJldHVybjsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go6Ieq5YuV6ZuG6KiI44GX44G+44GZ44CCPC9wPic7CiAgICByZW5kZXJQcmlvcml0eUFjdGlvbnMoKTsKICAgIHJldHVybjsKICB9CgogIGxldCB0b3RhbENvc3Q9MCwgdG90YWxWYWx1ZT0wLCB0b3RhbE5ldD0wOwogIGNvbnN0IHJvd3M9W107CiAgY29uc3QgZGVjaXNpb25zPXtob2xkOjAsd2F0Y2g6MCx0YWtlOjAsc3RvcDowLHBlbmRpbmc6MH07CgogIGEuZm9yRWFjaChoPT57CiAgICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogICAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogICAgY29uc3Qgc2hhcmVzPU51bWJlcihoLnNoYXJlc3x8MCk7CiAgICBjb25zdCB2YWxpZD1OdW1iZXIuaXNGaW5pdGUoY3VyKSYmY3VyPjAmJmguYXNvZjsKICAgIGNvbnN0IGN1cnJlbnRWYWx1ZT12YWxpZD9jdXIqc2hhcmVzOjA7CiAgICBjb25zdCBpbnZlc3RlZD1OdW1iZXIoaC5jb3N0fHwwKSpzaGFyZXMrYy5idXlGZWU7CgogICAgdG90YWxDb3N0ICs9IGludmVzdGVkOwoKICAgIGlmKHZhbGlkKXsKICAgICAgdG90YWxWYWx1ZSArPSBjdXJyZW50VmFsdWU7CiAgICAgIHRvdGFsTmV0ICs9IGMubmV0Tm93OwoKICAgICAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwogICAgICBpZihkLmxhYmVsPT09J+aQjeWIh+OCiuaknOiojicpZGVjaXNpb25zLnN0b3ArKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+WIqeeiuuaknOiojicpZGVjaXNpb25zLnRha2UrKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+itpuaIkicpZGVjaXNpb25zLndhdGNoKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZknKWRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIGVsc2UgZGVjaXNpb25zLmhvbGQrKzsKCiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6Y3VycmVudFZhbHVlLG5ldDpjLm5ldE5vd30pOwogICAgfWVsc2V7CiAgICAgIGRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6MCxuZXQ6bnVsbH0pOwogICAgfQogIH0pOwoKICBjb25zdCBuZXRQY3Q9dG90YWxDb3N0PjA/dG90YWxOZXQvdG90YWxDb3N0KjEwMDpudWxsOwogIGNvbnN0IG1heFZhbHVlPXJvd3MucmVkdWNlKChtLHIpPT5NYXRoLm1heChtLHIudmFsdWUpLDApOwogIGNvbnN0IGNvbmNlbnRyYXRpb249dG90YWxWYWx1ZT4wP21heFZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CgogIGNvbnN0IG1lYW4yMD1wb3J0Zm9saW9NZWFuRm9yKGEsJzIwZCcpOwogIGNvbnN0IG1lYW4xMjY9cG9ydGZvbGlvTWVhbkZvcihhLCcxMjZkJyk7CiAgY29uc3QgbWVhbjI1Mj1wb3J0Zm9saW9NZWFuRm9yKGEsJzI1MmQnKTsKCiAgY29uc3QgYWxsb2NhdGlvbnM9cm93cwogICAgLmZpbHRlcihyPT5yLnZhbHVlPjApCiAgICAuc29ydCgoeCx5KT0+eS52YWx1ZS14LnZhbHVlKQogICAgLm1hcChyPT57CiAgICAgIGNvbnN0IHc9dG90YWxWYWx1ZT4wP3IudmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKICAgICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJhbGxvYyI+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPjxiPiR7ci5jb2RlfTwvYj4ke3IubmFtZT8nICcrci5uYW1lOicnfSAvICR7dy50b0ZpeGVkKDEpfSU8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJhbGxvY2JhciI+PHNwYW4gc3R5bGU9IndpZHRoOiR7TWF0aC5taW4oMTAwLHcpfSUiPjwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+YDsKICAgIH0pLmpvaW4oJycpOwoKICBib3guaW5uZXJIVE1MPWAKICAgIDxkaXYgY2xhc3M9InBvcnRyb3ciPgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nt4/mipXos4fpoY08L3NwYW4+PGI+JHt5ZW4odG90YWxDb3N0KX08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuipleS+oemhjTwvc3Bhbj48Yj4ke3RvdGFsVmFsdWU+MD95ZW4odG90YWxWYWx1ZSk6J+KAlCd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrzmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7dG90YWxOZXQ+PTA/J3Bvcyc6J25lZyd9Ij4ke3RvdGFsVmFsdWU+MD95ZW4odG90YWxOZXQpOifigJQnfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7bmV0UGN0PT09bnVsbD8n4oCUJzpuZXRQY3QudG9GaXhlZCgyKSsnJSd9PC9zcGFuPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mnIDlpKfpipjmn4Tmr5Tnjoc8L3NwYW4+PGI+JHt0b3RhbFZhbHVlPjA/Y29uY2VudHJhdGlvbi50b0ZpeGVkKDEpKyclJzon4oCUJ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KCiAgICA8ZGl2IGNsYXNzPSJwb3J0cm93IiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7kv53mnInntpnntpo8L3NwYW4+PGI+JHtkZWNpc2lvbnMuaG9sZH08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuitpuaIkjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy53YXRjaH08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuaknOiojjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy50YWtlfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KKL+S/neeVmTwvc3Bhbj48Yj4ke2RlY2lzaW9ucy5zdG9wK2RlY2lzaW9ucy5wZW5kaW5nfTwvYj48L2Rpdj4KICAgIDwvZGl2PgoKICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA3cHgiPvCfk4og6KmV5L6h6aGN5Yqg6YeN44Gu6YGO5Y675bmz5Z2H44Oq44K/44O844OzPC9oND4KICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuefreacnzIw5pelPC9zcGFuPjxiPiR7bWVhbjIwPT09bnVsbD8n4oCUJzptZWFuMjAudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Lit5pyfMTI25pelPC9zcGFuPjxiPiR7bWVhbjEyNj09PW51bGw/J+KAlCc6bWVhbjEyNi50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7plbfmnJ8yNTLml6U8L3NwYW4+PGI+JHttZWFuMjUyPT09bnVsbD8n4oCUJzptZWFuMjUyLnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgIDwvZGl2PgogICAgPHAgY2xhc3M9Im11dGVkIj7igLvlkITpipjmn4Tjga7pgY7ljrvlubPlnYfjg6rjgr/jg7zjg7PjgpLnj77lnKjjga7oqZXkvqHpoY3jgafliqDph43jgZfjgZ/lj4LogIPlgKTjgafjgZnjgILnm7jplqLjgpLogIPmha7jgZfjgZ/lsIbmnaXkuojmuKzjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+CgogICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDVweCI+8J+TpiDpipjmn4Tmp4vmiJA8L2g0PgogICAgJHthbGxvY2F0aW9uc3x8JzxwIGNsYXNzPSJtdXRlZCI+5a6f44OH44O844K/5pyq5Y+W5b6XPC9wPid9CiAgYDsKICByZW5kZXJQcmlvcml0eUFjdGlvbnMoKTsKfQpmdW5jdGlvbiByZW5kZXJIb2xkaW5ncygpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgaWYoIWEubGVuZ3RoKXskKCdob2xkaW5ncycpLmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JztyZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCk7cmV0dXJufQogICQoJ2hvbGRpbmdzJykuaW5uZXJIVE1MPWEubWFwKChoLGkpPT57CiAgICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogICAgY29uc3QgdmFsaWRQcmljZT1OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wJiZoLmFzb2Y7CiAgICBjb25zdCBjbHM9dmFsaWRQcmljZT8oYy5uZXROb3c+PTA/J3Bvcyc6J25lZycpOicnOwogICAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwogICAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwoKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iaG9sZGluZyI+CiAgICAgIDxkaXYgY2xhc3M9ImhvbGRpbmctaGVhZCI+CiAgICAgICAgPGRpdj4KICAgICAgICAgIDxiPiR7aC5jb2RlfTwvYj4KICAgICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj4ke2guY29tcGFueV9uYW1lfHwiIn08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gc2Vjb25kYXJ5IiBvbmNsaWNrPSJyZWZyZXNoSG9sZGluZygke2l9KSI+5pu05pawPC9idXR0b24+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBkYW5nZXIiIG9uY2xpY2s9InJlbW92ZUhvbGRpbmcoJHtpfSkiPuWJiumZpDwvYnV0dG9uPgogICAgICAgIDwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj7jg4fjg7zjgr/ml6UgJHtoLmFzb2Z8fCfigJQnfSAvIOacgOaWsOWPluW+l+e1guWApCAke3ZhbGlkUHJpY2U/eWVuKGguY3VycmVudF9wcmljZSk6J+KAlCd9IC8gJHtoLnNoYXJlc33moKogLyDlj5blvpfljZjkvqEgJHt5ZW4oaC5jb3N0KX08L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImRlY2lzaW9uICR7ZC5jbHN9Ij4KICAgICAgICAke2QubGFiZWx9CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjRweCI+JHtkLnJlYXNvbn08L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMuc3RvcFByaWNlLCdzdG9wJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMudGFrZVByaWNlLCd0YWtlJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKTlrprkv6HpoLzluqY8L3NwYW4+CiAgICAgICAgICA8Yj4ke2QuY29uZmlkZW5jZX0lPC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0iZ2F1Z2UiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke2QuY29uZmlkZW5jZX0lIj48L3NwYW4+PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPuKAu+WIpOWumuS/oemgvOW6puOBr+OAgeODh+ODvOOCv+WFhei2s+W6puODu+acn+mWk+ODiOODrOODs+ODieOBruS4gOiHtOW6puODu+acn+W+heWApOe1seioiOOBruacieeEoeOBi+OCieS9nOOCi+WPguiAg+aMh+aomeOBp+OAgeeahOS4reeiuueOh+OBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICAgIDxkaXYgY2xhc3M9ImFjdGlvbmJveCI+CiAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7ku4rjganjgYbjgZnjgovvvJ88L3NwYW4+CiAgICAgICAgPGIgc3R5bGU9ImRpc3BsYXk6YmxvY2s7bWFyZ2luLXRvcDozcHgiPiR7YWN0aW9uVGV4dChoLGMsZCl9PC9iPgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrzmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7Y2xzfSI+JHt2YWxpZFByaWNlP3llbihjLm5ldE5vdyk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt2YWxpZFByaWNlP2ZtdChjLm5ldE5vd1BjdCkrJyUnOifigJQnfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6LK35LuY5omL5pWw5paZPC9zcGFuPjxiPiR7eWVuKGMuYnV5RmVlKX08L2I+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWjsuWNtOaJi+aVsOaWmSjku4opPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy5zZWxsRmVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnN0b3BQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMuc3RvcE5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy50YWtlUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnRha2VOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844Oq44Oz44Kw5Y+C6ICDPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy50cmFpbFByaWNlKTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemrmOWApOWfuua6ljwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn46vIOWun+e4vuODmeODvOOCueS+oeagvOODrOODs+OCuDwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke3JhbmdlSHRtbCgn55+t5pyfIDIw5pelJyxjdXIsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+S4reacnyAxMjbml6UnLGN1cixzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+mVt+acnyAyNTLml6UnLGN1cixzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TkCDlrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtldkh0bWwoJ+efreacnyAyMOaXpScsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+S4reacnyAxMjbml6UnLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke2V2SHRtbCgn6ZW35pyfIDI1MuaXpScsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KICAgICAgPHAgY2xhc3M9Im11dGVkIj7igLvkvqHmoLzjg6zjg7Pjgrjjg7vmnJ/lvoXlgKTjga/lsIbmnaXkuojmuKzjgafjga/jgarjgY/jgIHlj5blvpflj6/og73jgarpgY7ljrvmoKrkvqHjga7jg63jg7zjg6rjg7PjgrDliY3mlrnjg6rjgr/jg7zjg7PliIbluIPjgpLmnIDmlrDlj5blvpfntYLlgKTjgavlvZPjgabjga/jgoHjgZ/ntbHoqIjlj4LogIPjgafjgZnjgILmnJ/plpPjgYzph43jgarjgovmqJnmnKzjgpLlkKvjgb/jgb7jgZnjgII8L3A+CiAgICA8L2Rpdj5gOwogIH0pLmpvaW4oJycpOwogIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0UXVvdGUoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcXVvdGU/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHE9YXdhaXQgci5qc29uKCk7CiAgaWYocS5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcihxLnJlYXNvbnx8cS5lcnJvcnx8J+Wun+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiBxOwp9Cgphc3luYyBmdW5jdGlvbiBhZGRIb2xkaW5nKCl7CiAgY29uc3QgY29kZT0kKCdob2xkQ29kZScpLnZhbHVlLnRyaW0oKTsKICBjb25zdCBjb3N0PXZhbCgnaG9sZENvc3QnKSwgc2hhcmVzPXZhbCgnaG9sZFNoYXJlcycpOwogIGNvbnN0IHN0b3A9dmFsKCdzdG9wUGN0JyksIHRha2U9dmFsKCd0YWtlUGN0JyksIHRyYWlsPXZhbCgndHJhaWxQY3QnKTsKICBjb25zdCBmZWVNb2RlPSQoJ2ZlZU1vZGUnKS52YWx1ZTsKICBpZighY29kZXx8IWNvc3R8fCFzaGFyZXMpe2FsZXJ0KCfpipjmn4TjgrPjg7zjg4njg7vlj5blvpfljZjkvqHjg7vmoKrmlbDjgpLlhaXlipvjgZfjgabjga0nKTtyZXR1cm59CiAgY29uc3QgYnRuPWV2ZW50Py50YXJnZXQ7IGlmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnfQogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogICAgY29uc3QgaD17CiAgICAgIGNvZGUsCiAgICAgIGNvbXBhbnlfbmFtZToocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fCcnLAogICAgICBjb21wYW55X21hcmtldDoocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8JycsCiAgICAgIGNvbXBhbnlfc2VjdG9yMzM6KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8JycsCiAgICAgIGNvc3QsIHNoYXJlcywgZmVlX21vZGU6ZmVlTW9kZSwKICAgICAgc3RvcF9wY3Q6c3RvcD8/OCwgdGFrZV9wY3Q6dGFrZT8/MTUsIHRyYWlsX3BjdDp0cmFpbD8/NywKICAgICAgY3VycmVudF9wcmljZTpzLmxhc3RfY2xvc2UsIGhpZ2hfMjBkOnMuaGlnaF8yMGQsIGxvd18yMGQ6cy5sb3dfMjBkLAogICAgICByZXR1cm5fMjBkOnMucmV0dXJuXzIwZCwgcmV0dXJuXzEyNmQ6cy5yZXR1cm5fMTI2ZCwgcmV0dXJuXzI1MmQ6cy5yZXR1cm5fMjUyZCwKICAgICAgZm9yd2FyZF9zdGF0czpzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSwKICAgICAgYXNvZjpzLmxhc3RfZGF0ZSwgdXBkYXRlZF9hdDpuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCkKICAgIH07CiAgICBjb25zdCBpZHg9YS5maW5kSW5kZXgoeD0+eC5jb2RlPT09Y29kZSk7CiAgICBpZihpZHg+PTApYVtpZHhdPWg7IGVsc2UgYS5wdXNoKGgpOwogICAgc2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpOwogICAgcmVuZGVySG9sZGluZ3MoKTsKICB9Y2F0Y2goZSl7YWxlcnQoJ+WPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UpfQogIGZpbmFsbHl7aWYoYnRuKXtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZgnfX0KfQoKYXN5bmMgZnVuY3Rpb24gcmVmcmVzaEhvbGRpbmcoaSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKSwgaD1hW2ldOyBpZighaClyZXR1cm47CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShoLmNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgaC5jb21wYW55X25hbWU9KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHxoLmNvbXBhbnlfbmFtZXx8Jyc7CiAgICBoLmNvbXBhbnlfbWFya2V0PShxLmNvbXBhbnkmJnEuY29tcGFueS5tYXJrZXQpfHxoLmNvbXBhbnlfbWFya2V0fHwnJzsKICAgIGguY29tcGFueV9zZWN0b3IzMz0ocS5jb21wYW55JiZxLmNvbXBhbnkuc2VjdG9yMzMpfHxoLmNvbXBhbnlfc2VjdG9yMzN8fCcnOwogICAgaC5jdXJyZW50X3ByaWNlPXMubGFzdF9jbG9zZTsgaC5oaWdoXzIwZD1zLmhpZ2hfMjBkOyBoLmxvd18yMGQ9cy5sb3dfMjBkOwogICAgaC5yZXR1cm5fMjBkPXMucmV0dXJuXzIwZDsgaC5yZXR1cm5fMTI2ZD1zLnJldHVybl8xMjZkOyBoLnJldHVybl8yNTJkPXMucmV0dXJuXzI1MmQ7CiAgICBoLmZvcndhcmRfc3RhdHM9cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e307CiAgICBoLmFzb2Y9cy5sYXN0X2RhdGU7IGgudXBkYXRlZF9hdD1uZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCk7CiAgICBhW2ldPWg7IHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsgcmVuZGVySG9sZGluZ3MoKTsKICB9Y2F0Y2goZSl7YWxlcnQoJ+abtOaWsOOCqOODqeODvDogJytlLm1lc3NhZ2UpfQp9CmZ1bmN0aW9uIHJlbW92ZUhvbGRpbmcoaSl7Y29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTthLnNwbGljZShpLDEpO3NhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTtyZW5kZXJIb2xkaW5ncygpfQoKZnVuY3Rpb24gcmVuZGVyV2F0Y2goKXsKICAkKCd3YXRjaHMnKS5pbm5lckhUTUw9bG9jYWwoJ2ZyZWVfd2F0Y2gnKS5tYXAoeD0+ewogICAgaWYodHlwZW9mIHg9PT0nc3RyaW5nJylyZXR1cm4gYDxkaXYgY2xhc3M9ImJhZGdlIj4ke3h9PC9kaXY+YDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYmFkZ2UiPjxiPiR7eC5jb2RlfTwvYj4ke3gubmFtZT8nICcreC5uYW1lOicnfTwvZGl2PmA7CiAgfSkuam9pbignJyl8fCc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nOwp9CmFzeW5jIGZ1bmN0aW9uIGFkZFdhdGNoKCl7CiAgbGV0IGM9JCgnd2F0Y2hDb2RlJykudmFsdWUudHJpbSgpOyBpZighYylyZXR1cm47CiAgbGV0IGluZm89bnVsbDsKICB0cnl7aW5mbz1hd2FpdCBnZXRDb21wYW55KGMpfWNhdGNoKGUpe30KICBsZXQgYT1sb2NhbCgnZnJlZV93YXRjaCcpOwogIGNvbnN0IGV4aXN0cz1hLnNvbWUoeD0+KHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpPT09Yyk7CiAgaWYoIWV4aXN0cylhLnB1c2goe2NvZGU6YyxuYW1lOmluZm8mJmluZm8ubmFtZT9pbmZvLm5hbWU6Jyd9KTsKICBzYXZlKCdmcmVlX3dhdGNoJyxhKTsKICByZW5kZXJXYXRjaCgpOwp9CmZ1bmN0aW9uIHVwZGF0ZUthYnV0YW4oKXtsZXQgYz0kKCdjb2RlJykudmFsdWUudHJpbSgpOyQoJ2thYnV0YW4nKS5ocmVmPWM/J2h0dHBzOi8va2FidXRhbi5qcC9zdG9jay8/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKTonaHR0cHM6Ly9rYWJ1dGFuLmpwLyd9CiQoJ2NvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9Pnt1cGRhdGVLYWJ1dGFuKCk7c2NoZWR1bGVDb21wYW55TG9va3VwKCdjb2RlJywnY29tcGFueU5hbWUnLCcnKX0pO3VwZGF0ZUthYnV0YW4oKTsKJCgnaG9sZENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnaG9sZENvZGUnLCdob2xkQ29tcGFueU5hbWUnLCcnKSk7CiQoJ3dhdGNoQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCd3YXRjaENvZGUnLCd3YXRjaENvbXBhbnlOYW1lJywnJykpOwoKCmFzeW5jIGZ1bmN0aW9uIGdldEZ1bmRhbWVudGFscyhjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9mdW5kYW1lbnRhbHM/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+axuueul+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiB4LmZ1bmRhbWVudGFsc3x8e307Cn0KCmZ1bmN0aW9uIHNjb3JlTGFiZWwodil7CiAgaWYodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKXJldHVybiAn4oCUJzsKICBjb25zdCBuPU51bWJlcih2KTsKICByZXR1cm4gKG4+MD8nKyc6JycpK24udG9GaXhlZCgxKTsKfQoKZnVuY3Rpb24gcGN0TWF5YmUodil7CiAgcmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDEpKyclJzsKfQoKYXN5bmMgZnVuY3Rpb24gYW5hbHl6ZSgpewogIGNvbnN0IGNvZGU9JCgnY29kZScpLnZhbHVlLnRyaW0oKTsKICBpZighY29kZSl7JCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+mKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOBrSc7cmV0dXJufQogIGNvbnN0IGJ0bj0kKCdhbmFseXplQnRuJyk7IGJ0bi5kaXNhYmxlZD10cnVlOyBidG4udGV4dENvbnRlbnQ9J+WPluW+l+S4reKApic7CiAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J0otUXVhbnRz44GL44KJ5qCq5L6h44O75rG6566X5a6f44OH44O844K/44KS5Y+W5b6X44GX44Gm44GE44G+44GZ4oCmJzsKCiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShjb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgICQoJ3ByaWNlJykudmFsdWU9cy5sYXN0X2Nsb3NlPT1udWxsPycnOmZtdChzLmxhc3RfY2xvc2UsMSk7CiAgICAkKCdyMjAnKS52YWx1ZT1mbXQocy5yZXR1cm5fMjBkKTskKCdyMTI2JykudmFsdWU9Zm10KHMucmV0dXJuXzEyNmQpOyQoJ3IyNTInKS52YWx1ZT1mbXQocy5yZXR1cm5fMjUyZCk7CiAgICAkKCdoaWdoMjAnKS50ZXh0Q29udGVudD1mbXQocy5oaWdoXzIwZCwxKTskKCdsb3cyMCcpLnRleHRDb250ZW50PWZtdChzLmxvd18yMGQsMSk7CiAgICAkKCd2b2wyMCcpLnRleHRDb250ZW50PXMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZD09bnVsbD8n4oCUJzpmbXQocy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkKSsnJSc7CgogICAgZGlzcGxheUNvbXBhbnkoJCgnY29tcGFueU5hbWUnKSxxLmNvbXBhbnl8fG51bGwsJycpOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPSc8YiBjbGFzcz0ib2siPuKchSBKLVF1YW50c+Wun+ODh+ODvOOCv+WPluW+l09LPC9iPjxicj7mnIDntYLjg4fjg7zjgr/ml6U6ICcrKHMubGFzdF9kYXRlfHwn4oCUJykrJyAvIOe1guWApDogJytmbXQocy5sYXN0X2Nsb3NlLDEpKycgLyDjgrXjg7Pjg5fjg6s6ICcrKHMuc2FtcGxlX2NvdW50Pz8n4oCUJykrJ+S7tic7CiAgICAkKCdhbmFseXNpc0V2JykuaW5uZXJIVE1MPQogICAgICBldkh0bWwoJ+efreacnzIw5pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycyMGQnXSkrCiAgICAgIGV2SHRtbCgn5Lit5pyfMTI25pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycxMjZkJ10pKwogICAgICBldkh0bWwoJ+mVt+acnzI1MuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjUyZCddKTsKCiAgICBjb25zdCBzdXBwbHk9cy5zdXBwbHlfcHJveHl8fHt9OwogICAgJCgnc3VwcGx5QXV0bycpLnRleHRDb250ZW50PXNjb3JlTGFiZWwoc3VwcGx5LnNjb3JlKTsKICAgICQoJ3N1cHBseURldGFpbCcpLnRleHRDb250ZW50PQogICAgICAnNeaXpS8yMOaXpeWHuuadpemrmCAnKyhzdXBwbHkudm9sdW1lX3JhdGlvXzVfMjA9PW51bGw/J+KAlCc6TnVtYmVyKHN1cHBseS52b2x1bWVfcmF0aW9fNV8yMCkudG9GaXhlZCgyKSsn5YCNJyk7CgogICAgbGV0IGZ1bmRhbWVudGFscz17fTsKICAgIHRyeXsKICAgICAgZnVuZGFtZW50YWxzPWF3YWl0IGdldEZ1bmRhbWVudGFscyhjb2RlKTsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKGZ1bmRhbWVudGFscy5zY29yZSk7CiAgICAgIGNvbnN0IG09ZnVuZGFtZW50YWxzLm1ldHJpY3N8fHt9OwogICAgICAkKCdlYXJuRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgJ+mWi+ekuiAnKygoZnVuZGFtZW50YWxzLmxhdGVzdCYmZnVuZGFtZW50YWxzLmxhdGVzdC5kYXRlKXx8J+KAlCcpKwogICAgICAgICcgLyDlo7LkuIogJytwY3RNYXliZShtLnNhbGVzX2dyb3d0aF9wY3QpKwogICAgICAgICcgLyDllrbmpa3nm4ogJytwY3RNYXliZShtLm9wX2dyb3d0aF9wY3QpOwogICAgfWNhdGNoKGZlKXsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD0n5LiN5piOJzsKICAgICAgJCgnZWFybkRldGFpbCcpLnRleHRDb250ZW50PSfjgZPjga7jg5fjg6njg7Mv6YqY5p+E44Gn44Gv5Y+W5b6X44Gn44GN44Gq44GE5Y+v6IO95oCn44GC44KKJzsKICAgICAgZnVuZGFtZW50YWxzPXtzY29yZTpudWxsfTsKICAgIH0KCiAgICBjb25zdCBwb2xpY3lTY29yZT12YWwoJ3BvbGljeScpOwogICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD1wb2xpY3lTY29yZT09PW51bGw/J+S4jeaYjic6c2NvcmVMYWJlbChwb2xpY3lTY29yZSk7CgogICAgY29uc3QgZD17CiAgICAgIGNvZGUsCiAgICAgIHByaWNlOnMubGFzdF9jbG9zZSwKICAgICAgcmV0dXJuMjA6cy5yZXR1cm5fMjBkLAogICAgICByZXR1cm4xMjY6cy5yZXR1cm5fMTI2ZCwKICAgICAgcmV0dXJuMjUyOnMucmV0dXJuXzI1MmQsCiAgICAgIGVhcm5pbmdzX3Njb3JlOmZ1bmRhbWVudGFscy5zY29yZSwKICAgICAgcG9saWN5X3Njb3JlOnBvbGljeVNjb3JlLAogICAgICBzdXBwbHlfc2NvcmU6c3VwcGx5LnNjb3JlCiAgICB9OwoKICAgIGNvbnN0IGFyPWF3YWl0IGZldGNoKCcvYXBpL2ZyZWUvYW5hbHl6ZScsewogICAgICBtZXRob2Q6J1BPU1QnLAogICAgICBoZWFkZXJzOnsnQ29udGVudC1UeXBlJzonYXBwbGljYXRpb24vanNvbid9LAogICAgICBib2R5OkpTT04uc3RyaW5naWZ5KGQpLAogICAgICBjYWNoZTonbm8tc3RvcmUnCiAgICB9KTsKICAgIGNvbnN0IHg9YXdhaXQgYXIuanNvbigpOwogICAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+WIhuaekOODh+ODvOOCv+OBjOS4jei2s+OBl+OBpuOBhOOBvuOBmScpOwoKICAgICQoJ3N0YXRlJykudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7CiAgICAkKCdwb3MnKS50ZXh0Q29udGVudD14LnNpZ25hbC5wb3NpdGl2ZV9jb3VudDsKICAgICQoJ25lZycpLnRleHRDb250ZW50PXguc2lnbmFsLm5lZ2F0aXZlX2NvdW50OwoKICAgIGNvbnN0IHNjPXguc2NvcmV8fHt9OwogICAgJCgnc2NvcmVIZXJvJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmUxMDAnKS50ZXh0Q29udGVudD1zYy5zY29yZTEwMD09bnVsbD8n4oCUJzpzYy5zY29yZTEwMCsnIC8gMTAwJzsKICAgICQoJ2NvdmVyYWdlJykudGV4dENvbnRlbnQ9c2MuY292ZXJhZ2VfcGN0PT1udWxsPyfigJQnOnNjLmNvdmVyYWdlX3BjdCsnJSc7CiAgICAkKCdzY29yZUJyZWFrZG93bicpLnRleHRDb250ZW50PQogICAgICAn44OG44Kv44OL44Kr44OrICcrc2NvcmVMYWJlbChzYy50ZWNobmljYWwpKwogICAgICAnIC8g5rG6566XICcrc2NvcmVMYWJlbChzYy5lYXJuaW5ncykrCiAgICAgICcgLyDpnIDntaYgJytzY29yZUxhYmVsKHNjLnN1cHBseSkrCiAgICAgICcgLyDlm73nrZYgJytzY29yZUxhYmVsKHNjLnBvbGljeSk7CgogICAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9CiAgICAgICflrp/jg4fjg7zjgr/kuK3lv4Pjga7nt4/lkIjmjqHngrnjgILlm73nrZbjgYzmnKrlhaXlipvjgarjgonjgIHjgZ3jga7ph43jgb/jga/ku5bjga7lj5blvpfmuIjjgb/opoHlm6Djgbjlho3phY3liIbjgZfjgb7jgZnjgIInOwogIH1jYXRjaChlKXsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfimqDvuI8gJytlLm1lc3NhZ2U7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9JzxzcGFuIGNsYXNzPSJlcnIiPuWPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UrJzwvc3Bhbj4nOwogIH1maW5hbGx5ewogICAgYnRuLmRpc2FibGVkPWZhbHNlOwogICAgYnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafliIbmnpAnOwogIH0KfQoKcmVuZGVySG9sZGluZ3MoKTtyZW5kZXJXYXRjaCgpOwppZignc2VydmljZVdvcmtlcicgaW4gbmF2aWdhdG9yKXtuYXZpZ2F0b3Iuc2VydmljZVdvcmtlci5nZXRSZWdpc3RyYXRpb25zKCkudGhlbihycz0+UHJvbWlzZS5hbGwocnMubWFwKHI9PnIudW5yZWdpc3RlcigpKSkpLmNhdGNoKCgpPT57fSl9CmlmKCdjYWNoZXMnIGluIHdpbmRvdyl7Y2FjaGVzLmtleXMoKS50aGVuKGtleXM9PlByb21pc2UuYWxsKGtleXMubWFwKGs9PmNhY2hlcy5kZWxldGUoaykpKSkuY2F0Y2goKCk9Pnt9KX0KPC9zY3JpcHQ+CjwvbWFpbj4KPC9ib2R5Pgo8L2h0bWw+"
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
