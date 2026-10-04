from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.11-AUTO-POLICY'
SECURITY_CACHE={}
POLICY_SOURCE_CACHE={}

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




POLICY_THEMES = json.loads(r"""[{"key":"ai_semiconductor","name":"AI\u30fb\u534a\u5c0e\u4f53","url":"https://www.meti.go.jp/policy/mono_info_service/ai_semiconductor_frame/ai_semiconductor_frame.html","required_terms":["AI","\u534a\u5c0e\u4f53","10\u5146\u5186"],"strength":100.0,"sector_weights":{"\u96fb\u6c17\u6a5f\u5668":0.75,"\u7cbe\u5bc6\u6a5f\u5668":0.55,"\u60c5\u5831\u30fb\u901a\u4fe1\u696d":0.65,"\u6a5f\u68b0":0.35,"\u975e\u9244\u91d1\u5c5e":0.25},"name_keywords":{"\u534a\u5c0e\u4f53":0.45,"\u96fb\u6a5f":0.2,"\u96fb\u5b50":0.25,"\u901a\u4fe1":0.2,"\u30c7\u30fc\u30bf":0.2,"\u30b7\u30b9\u30c6\u30e0":0.15,"\u96fb\u7dda":0.18,"\u96fb\u5de5":0.18}},{"key":"gx_datacenter","name":"GX\u30fb\u96fb\u529b\u30fb\u30c7\u30fc\u30bf\u30bb\u30f3\u30bf\u30fc","url":"https://www.meti.go.jp/policy/energy_environment/global_warming/gx_strategy_area.html","required_terms":["GX","\u30c7\u30fc\u30bf\u30bb\u30f3\u30bf\u30fc","\u8131\u70ad\u7d20"],"strength":92.0,"sector_weights":{"\u96fb\u6c17\u30fb\u30ac\u30b9\u696d":0.85,"\u975e\u9244\u91d1\u5c5e":0.55,"\u96fb\u6c17\u6a5f\u5668":0.55,"\u5efa\u8a2d\u696d":0.45,"\u6a5f\u68b0":0.35,"\u8f38\u9001\u7528\u6a5f\u5668":0.4,"\u9244\u92fc":0.35,"\u5316\u5b66":0.3},"name_keywords":{"\u96fb\u529b":0.35,"\u96fb\u7dda":0.35,"\u96fb\u5de5":0.35,"\u30b1\u30fc\u30d6\u30eb":0.35,"\u96fb\u6a5f":0.2,"\u30a8\u30cd\u30eb\u30ae\u30fc":0.3,"\u96fb\u6c60":0.3,"\u84c4\u96fb":0.3,"\u91cd\u5de5":0.15}},{"key":"defense","name":"\u9632\u885b\u30fb\u5b87\u5b99\u30fb\u30b5\u30a4\u30d0\u30fc","url":"https://www.mod.go.jp/j/policy/agenda/guideline/plan/plan_01.html","required_terms":["\u9632\u885b\u529b","2027\u5e74\u5ea6","\u5b87\u5b99"],"strength":95.0,"sector_weights":{"\u6a5f\u68b0":0.45,"\u96fb\u6c17\u6a5f\u5668":0.45,"\u8f38\u9001\u7528\u6a5f\u5668":0.2,"\u7cbe\u5bc6\u6a5f\u5668":0.45,"\u60c5\u5831\u30fb\u901a\u4fe1\u696d":0.45,"\u9244\u92fc":0.2,"\u975e\u9244\u91d1\u5c5e":0.2},"name_keywords":{"\u91cd\u5de5":0.45,"\u822a\u7a7a":0.45,"\u9020\u8239":0.45,"\u5b87\u5b99":0.45,"\u9632\u885b":0.45,"\u96fb\u6a5f":0.15,"\u901a\u4fe1":0.2,"\u30b7\u30b9\u30c6\u30e0":0.15}},{"key":"resilience","name":"\u56fd\u571f\u5f37\u9771\u5316\u30fb\u30a4\u30f3\u30d5\u30e9","url":"https://www.cas.go.jp/jp/seisaku/kokudo_kyoujinka/dai1_chuukikeikaku/index.html","required_terms":["\u56fd\u571f\u5f37\u9771\u5316","\u4ee4\u548c\uff18\u5e74\u5ea6","\u4ee4\u548c12\u5e74\u5ea6"],"strength":90.0,"sector_weights":{"\u5efa\u8a2d\u696d":0.8,"\u91d1\u5c5e\u88fd\u54c1":0.5,"\u6a5f\u68b0":0.35,"\u9244\u92fc":0.45,"\u975e\u9244\u91d1\u5c5e":0.45,"\u96fb\u6c17\u6a5f\u5668":0.3,"\u30ac\u30e9\u30b9\u30fb\u571f\u77f3\u88fd\u54c1":0.55},"name_keywords":{"\u5efa\u8a2d":0.35,"\u9053\u8def":0.35,"\u6a4b\u6881":0.4,"\u30bb\u30e1\u30f3\u30c8":0.35,"\u96fb\u7dda":0.25,"\u96fb\u5de5":0.25,"\u30b1\u30fc\u30d6\u30eb":0.25,"\u9244\u5de5":0.25}}]""")


def check_policy_source(theme):
    key = theme["key"]
    now_ts = datetime.now(timezone.utc).timestamp()
    cached = POLICY_SOURCE_CACHE.get(key)
    if cached and now_ts - cached.get("ts", 0) < 21600:
        return cached["result"]

    result = {
        "key": key,
        "name": theme["name"],
        "url": theme["url"],
        "status": "unavailable",
        "checked_at": now(),
        "strength": None,
    }

    try:
        r = requests.get(
            theme["url"],
            timeout=8,
            headers={"User-Agent": "JapanStockAIFree/1.11 policy-source-check"},
        )
        if r.status_code == 200:
            r.encoding = r.apparent_encoding or r.encoding
            text = r.text[:500000]
            hits = [term for term in theme["required_terms"] if term in text]
            ratio = len(hits) / max(1, len(theme["required_terms"]))
            if ratio >= 2 / 3:
                result["status"] = "verified"
                result["strength"] = theme["strength"]
                result["matched_terms"] = hits
            elif hits:
                result["status"] = "partial"
                result["strength"] = theme["strength"] * 0.60
                result["matched_terms"] = hits
            result["http_status"] = r.status_code
        else:
            result["http_status"] = r.status_code
    except Exception as e:
        result["reason"] = str(e)

    POLICY_SOURCE_CACHE[key] = {"ts": now_ts, "result": result}
    return result


def company_policy_relevance(company, theme):
    name = str(company.get("name") or "")
    sector33 = str(company.get("sector33") or "")
    relevance = float(theme["sector_weights"].get(sector33, 0.0))
    reasons = []

    if relevance > 0:
        reasons.append("sector:" + sector33)

    keyword_bonus = 0.0
    for keyword, weight in theme["name_keywords"].items():
        if keyword in name:
            keyword_bonus += weight
            reasons.append("name:" + keyword)

    relevance = min(1.0, relevance + keyword_bonus)
    return relevance, reasons


def policy_auto_score(code):
    company = security_info(code)
    matched = []
    source_states = []

    for theme in POLICY_THEMES:
        source = check_policy_source(theme)
        source_states.append(source)

        if source.get("strength") is None:
            continue

        relevance, reasons = company_policy_relevance(company, theme)
        if relevance < 0.25:
            continue

        contribution = source["strength"] * relevance
        matched.append({
            "key": theme["key"],
            "name": theme["name"],
            "url": theme["url"],
            "source_status": source["status"],
            "source_checked_at": source["checked_at"],
            "policy_strength": round(source["strength"], 1),
            "relevance": round(relevance, 3),
            "contribution": round(contribution, 2),
            "reasons": reasons,
        })

    matched.sort(key=lambda x: x["contribution"], reverse=True)

    if not matched:
        return {
            "status": "insufficient_data",
            "score": None,
            "company": company,
            "matched_themes": [],
            "sources": source_states,
            "method": "official-policy-source + sector/name relevance proxy",
            "note": "No policy theme met the minimum relevance threshold.",
        }

    values = [m["contribution"] for m in matched]
    score = values[0] + sum(values[1:]) * 0.25
    score = min(100.0, score)

    verified_count = sum(
        1 for m in matched if m["source_status"] == "verified"
    )
    confidence = verified_count / len(matched) * 100.0 if matched else 0.0

    return {
        "status": "ok",
        "score": round(score, 2),
        "confidence_pct": round(confidence, 1),
        "company": company,
        "matched_themes": matched,
        "sources": source_states,
        "method": "official-policy-source + sector/name relevance proxy",
        "note": "Policy relevance proxy; it does not prove direct subsidy or earnings benefit.",
    }


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


@APP.get("/api/mobile/policy")
def mobile_policy():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400
    if not jquants_status():
        return jsonify(
            status="unavailable",
            reason="J-Quants API key is not configured",
        )
    try:
        data = policy_auto_score(code)
        return jsonify(status="ok", code=code, policy=data)
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
    policy_mode=str(d.get("policy_mode") or "unknown")
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

    if score100 is None:
        state="unknown"
    elif score100 >= 80:
        state="strong_bullish"
    elif score100 >= 65:
        state="bullish"
    elif score100 >= 45:
        state="neutral"
    elif score100 >= 30:
        state="bearish"
    else:
        state="strong_bearish"

    drivers=[]
    if total_weight:
        for key, value, base_weight in available:
            normalized_weight=base_weight/total_weight
            contribution=clamp(value)*normalized_weight
            drivers.append({
                "key":key,
                "score":round(value,2),
                "weight_pct":round(normalized_weight*100.0,1),
                "contribution":round(contribution,2),
            })

    drivers_sorted=sorted(
        drivers,
        key=lambda x: abs(x["contribution"]),
        reverse=True
    )
    strongest_positive=next(
        (x for x in drivers_sorted if x["contribution"] > 0),
        None
    )
    strongest_negative=next(
        (x for x in drivers_sorted if x["contribution"] < 0),
        None
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
            "drivers":drivers_sorted,
            "strongest_positive":strongest_positive,
            "strongest_negative":strongest_negative,
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
            "policy":"official policy source + deterministic relevance proxy" if policy_mode=="auto" else "manual override"
        }
    )


HTML = base64.b64decode(
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9Ci5wb2xpY3l0aGVtZSBhe2ZvbnQtc2l6ZToxMnB4fQoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+CjxtYWluPgo8ZGl2IGNsYXNzPSJ0b3AiPgogIDxoMT7wn5OIIOaXpeacrOagqkFJIEZSRUU8L2gxPgogIDxkaXYgY2xhc3M9InN1YiI+Si1RdWFudHPlrp/jg4fjg7zjgr8gLyDlm73nrZboh6rli5XpgKPmkLogLyDnt4/lkIjmjqHngrkgLyDmsbrnrpcgLyDpnIDntaZwcm94eTwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn46vIOmKmOafhOWIhuaekDwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZCI+CiAgICA8aW5wdXQgaWQ9ImNvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSDkvosgNzIwMyI+CiAgICA8aW5wdXQgaWQ9InByaWNlIiBwbGFjZWhvbGRlcj0i5Y+W5b6X57WC5YCkIiByZWFkb25seT4KICA8L2Rpdj4KICA8ZGl2IGlkPSJjb21wYW55TmFtZSIgY2xhc3M9InNvdXJjZSBtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZnjgovjgajkvJrnpL7lkI3jgpLooajnpLrjgZfjgb7jgZk8L2Rpdj4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGEgaWQ9ImthYnV0YW4iIGNsYXNzPSJidG4gc2Vjb25kYXJ5IiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+5qCq5o6i44Gn56K66KqNPC9hPgogICAgPGJ1dHRvbiBpZD0iYW5hbHl6ZUJ0biIgb25jbGljaz0iYW5hbHl6ZSgpIj7lrp/jg4fjg7zjgr/jgafliIbmnpA8L2J1dHRvbj4KICA8L2Rpdj4KICA8cCBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeOCjOOBpuaKvOOBmeOBqOOAgUotUXVhbnRz44GL44KJ5Y+W5b6X44Gn44GN44KL5a6f44OH44O844K/44KS6Ieq5YuV5YWl5Yqb44GX44G+44GZ44CCPC9wPgogIDxkaXYgaWQ9InNvdXJjZUJveCIgY2xhc3M9InNvdXJjZSBtdXRlZCI+44OH44O844K/5pyq5Y+W5b6XPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfk4og5qCq5L6h44O744OG44Kv44OL44Kr44Or5a6f57i+PC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XpqLDokL3njocgJTwvc3Bhbj48aW5wdXQgaWQ9InIyMCIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MTI25pelICU8L3NwYW4+PGlucHV0IGlkPSJyMTI2IiByZWFkb25seT48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yNTLml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIyNTIiIHJlYWRvbmx5PjwvZGl2PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6auY5YCkPC9zcGFuPjxiIGlkPSJoaWdoMjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeWuieWApDwvc3Bhbj48YiBpZD0ibG93MjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeW5tOeOh+ODnOODqTwvc3Bhbj48YiBpZD0idm9sMjAiPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+nqSDlrp/jg4fjg7zjgr/opoHlm6A8L2gzPgogIDxwIGNsYXNzPSJtdXRlZCI+5rG6566X44GvSi1RdWFudHPosqHli5njgrXjg57jg6rjg7zjgIHpnIDntabjga/lrp/moKrkvqHjg7vlh7rmnaXpq5hwcm94eeOBi+OCieiHquWLleaOoeeCueOAguWbveetluOBoOOBkeOBr+ePvuaZgueCueOBp+OBr+aJi+WFpeWKm+OBp+OAgeacquWFpeWKm+OBr+OAjOS4jeaYjuOAjeOBp+OBmeOAgjwvcD4KICA8ZGl2IGNsYXNzPSJmYWN0b3JncmlkIj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpc8L3NwYW4+PGIgaWQ9ImVhcm5BdXRvIj7igJQ8L2I+PHNtYWxsIGlkPSJlYXJuRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWPluW+lzwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZyA57WmcHJveHk8L3NwYW4+PGIgaWQ9InN1cHBseUF1dG8iPuKAlDwvYj48c21hbGwgaWQ9InN1cHBseURldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrlj5blvpc8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetlnByb3h5PC9zcGFuPjxiIGlkPSJwb2xpY3lTdGF0ZSI+4oCUPC9iPjxzbWFsbCBpZD0icG9saWN5RGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuWFrOW8j+aUv+etluOCveODvOOCueeiuuiqjeWJjTwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OH44O844K/5YWF6LazPC9zcGFuPjxiIGlkPSJjb3ZlcmFnZSI+4oCUPC9iPjxzbWFsbCBjbGFzcz0ibXV0ZWQiPue3j+WQiOaOoeeCueOBq+S9v+OBiOOBn+mHjeOBvzwvc21hbGw+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0icG9saWN5VGhlbWVzIiBjbGFzcz0icG9saWN5dGhlbWVzIj48L2Rpdj4KICA8ZGl2IHN0eWxlPSJtYXJnaW4tdG9wOjlweCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetluOCueOCs+OCouS4iuabuOOBje+8iOS7u+aEj++8iSAtMTAw44CcMTAwPC9zcGFuPgogICAgPGlucHV0IGlkPSJwb2xpY3kiIGlucHV0bW9kZT0iZGVjaW1hbCIgcGxhY2Vob2xkZXI9IuepuuashOOBquOCieiHquWLleWbveetlnByb3h544KS5L2/55SoIj4KICA8L2Rpdj4KICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+WbveetlnByb3h544Gv44CB5pS/5bqc44Gu5YWs5byP5pS/562W44Oa44O844K444GM54++5Zyo56K66KqN44Gn44GN44KL44GT44Go44Go44CBSi1RdWFudHPjga7mpa3nqK7jg7vkvJrnpL7lkI3jgajjga7plqLpgKPluqbjgpLjg6vjg7zjg6vjgafntYTjgb/lkIjjgo/jgZvjgZ/lj4LogIPlgKTjgafjgZnjgILoo5zliqnph5HmjqHmip7jgoTmpa3nuL7mganmgbXjgpLoqLzmmI7jgZnjgovjgoLjga7jgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6Ag5YiG5p6Q57WQ5p6cPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+54q25oWLPC9zcGFuPjxiIGlkPSJzdGF0ZSI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODl+ODqeOCueagueaLoDwvc3Bhbj48YiBpZD0icG9zIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44Oe44Kk44OK44K55qC55ougPC9zcGFuPjxiIGlkPSJuZWciPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJzY29yZUhlcm8iIGNsYXNzPSJzY29yZWhlcm8iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7nt4/lkIjmjqHngrnvvIjlrp/jg4fjg7zjgr/jg7vjg6vjg7zjg6vjg5njg7zjgrnvvIk8L3NwYW4+CiAgICA8YiBpZD0ic2NvcmUxMDAiPuKAlDwvYj4KICAgIDxzcGFuIGlkPSJzY29yZVN0YXRlUGlsbCIgY2xhc3M9InN0YXRlcGlsbCBzdGF0ZS1uZXV0cmFsIj7igJQ8L3NwYW4+CiAgICA8ZGl2IGlkPSJzY29yZUJyZWFrZG93biIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InNjb3JlUmVhc29uIiBjbGFzcz0ic2NvcmUtcmVhc29uIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxzdHJvbmc+8J+nrSDjgarjgZzjgZPjga7ngrnmlbDvvJ88L3N0cm9uZz4KICAgIDxkaXYgaWQ9InNjb3JlUmVhc29uVGV4dCIgY2xhc3M9Im11dGVkIj7igJQ8L2Rpdj4KICAgIDxkaXYgaWQ9ImRyaXZlckdyaWQiIGNsYXNzPSJkcml2ZXJncmlkIj48L2Rpdj4KICA8L2Rpdj4KICA8cCBpZD0icmVzdWx0IiBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOAjOWun+ODh+ODvOOCv+OBp+WIhuaekOOAjeOCkuaKvOOBl+OBpuOBj+OBoOOBleOBhOOAgjwvcD4KICA8cCBjbGFzcz0ibXV0ZWQiPuaOoeeCueW4r++8mjgw44CcMTAwIOW8t+awlyAvIDY144CcNzkg44KE44KE5by35rCXIC8gNDXjgJw2NCDkuK3nq4sgLyAzMOOAnDQ0IOOChOOChOW8seawlyAvIDDjgJwyOSDlvLHmsJc8L3A+CiAgPGRpdiBpZD0iYW5hbHlzaXNFdiIgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPjwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5qoIOS7iuaXpeOBruWEquWFiOOCouOCr+OCt+ODp+ODszwvaDM+CiAgPGRpdiBpZD0icHJpb3JpdHlBY3Rpb25zIj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go44CB5YSq5YWI44GX44Gm56K66KqN44GZ44KL6YqY5p+E44KS6Ieq5YuV6KGo56S644GX44G+44GZ44CCPC9wPgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQgcG9ydGZvbGlvIj4KICA8aDM+8J+nrSDjg53jg7zjg4jjg5Xjgqnjg6rjgqrlhajkvZM8L2gzPgogIDxkaXYgaWQ9InBvcnRmb2xpb1N1bW1hcnkiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkrwg5L+d5pyJ5qCq44O75pCN5YiH44KKL+WIqeeiujwvaDM+CiAgPGRpdiBjbGFzcz0ibm90ZSI+CiAgICDmkI3liIfjgorjg7vliKnnorrjg7vjg4jjg6zjg7zjg6rjg7PjgrDjga/lj4LogIPjg6njgqTjg7PjgILkv53mnInliKTmlq3jga/lrp/jg4fjg7zjgr/jgajoqK3lrprjg6njgqTjg7Pjga7jg6vjg7zjg6vliKTlrprjgafjgZnjgILnn63kuK3plbfjga/pgY7ljrvjga7jg63jg7zjg6rjg7PjgrDlrp/nuL7liIbluIPjgpLntbHoqIjlj4LogIPjgajjgZfjgabooajnpLrjgZfjgb7jgZnjgIIKICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZENvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8aW5wdXQgaWQ9ImhvbGRDb3N0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLlj5blvpfljZjkvqEiPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjZweCAycHggMCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZFNoYXJlcyIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i5qCq5pWwIj4KICAgIDxzZWxlY3QgaWQ9ImZlZU1vZGUiPgogICAgICA8b3B0aW9uIHZhbHVlPSJub211cmFfbmV0Ij7ph47mnZHjgqrjg7Pjg6njgqTjg7PlsILnlKjmlK/lupfjg7vnj77niak8L29wdGlvbj4KICAgICAgPG9wdGlvbiB2YWx1ZT0ibm9uZSI+5omL5pWw5paZ44Gq44GX77yI5q+U6LyD55So77yJPC9vcHRpb24+CiAgICA8L3NlbGVjdD4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoogJTwvc3Bhbj48aW5wdXQgaWQ9InN0b3BQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjgiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuiAlPC9zcGFuPjxpbnB1dCBpZD0idGFrZVBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iMTUiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODqyAlPC9zcGFuPjxpbnB1dCBpZD0idHJhaWxQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjciPjwvZGl2PgogIDwvZGl2PgogIDxidXR0b24gb25jbGljaz0iYWRkSG9sZGluZygpIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij7lrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZg8L2J1dHRvbj4KICA8cCBjbGFzcz0ibXV0ZWQiPumHjuadkeODjeODg+ODiO+8huOCs+ODvOODq++8j+OBu+OBo+OBqOODgOOCpOODrOOCr+ODiOOBruWbveWGheePvueJqeODu+OCquODs+ODqeOCpOODs+azqOaWh+OBrueojui+vOaJi+aVsOaWmeihqOOCkuS9v+eUqOOAgjwvcD4KICA8ZGl2IGlkPSJob2xkaW5ncyI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkYAg44Km44Kp44OD44OB44Oq44K544OIPC9oMz4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGlucHV0IGlkPSJ3YXRjaENvZGUiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPgogICAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRXYXRjaCgpIj7ov73liqA8L2J1dHRvbj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NHB4IDJweCA4cHgiPumKmOafhOWQje+8muKAlDwvZGl2PgogIDxkaXYgaWQ9IndhdGNocyI+PC9kaXY+CjwvZGl2PgoKPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKZnVuY3Rpb24gdmFsKGlkKXtsZXQgdj0kKGlkKS52YWx1ZS50cmltKCk7cmV0dXJuIHY9PT0nJz9udWxsOk51bWJlcih2KX0KZnVuY3Rpb24gbG9jYWwoayl7dHJ5e3JldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKGspfHwnW10nKX1jYXRjaChlKXtyZXR1cm5bXX19CmZ1bmN0aW9uIHNhdmUoayx2KXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrLEpTT04uc3RyaW5naWZ5KHYpKX0KZnVuY3Rpb24gZm10KHYsZD0yKXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoZCl9CmZ1bmN0aW9uIHllbih2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6J8KlJytNYXRoLnJvdW5kKE51bWJlcih2KSkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyl9CmZ1bmN0aW9uIHN0YXRlSmEocyl7CiAgaWYocz09PSdzdHJvbmdfYnVsbGlzaCcpcmV0dXJuICflvLfmsJcnOwogIGlmKHM9PT0nYnVsbGlzaCcpcmV0dXJuICfjgoTjgoTlvLfmsJcnOwogIGlmKHM9PT0nbmV1dHJhbCcpcmV0dXJuICfkuK3nq4snOwogIGlmKHM9PT0nYmVhcmlzaCcpcmV0dXJuICfjgoTjgoTlvLHmsJcnOwogIGlmKHM9PT0nc3Ryb25nX2JlYXJpc2gnKXJldHVybiAn5byx5rCXJzsKICByZXR1cm4gJ+WIpOWumuS/neeVmSc7Cn0KCmZ1bmN0aW9uIHN0YXRlQ2xhc3Mocyl7CiAgaWYocz09PSdzdHJvbmdfYnVsbGlzaCcpcmV0dXJuICdzdGF0ZS1zdHJvbmctYnVsbCc7CiAgaWYocz09PSdidWxsaXNoJylyZXR1cm4gJ3N0YXRlLWJ1bGwnOwogIGlmKHM9PT0nbmV1dHJhbCcpcmV0dXJuICdzdGF0ZS1uZXV0cmFsJzsKICBpZihzPT09J2JlYXJpc2gnKXJldHVybiAnc3RhdGUtYmVhcic7CiAgaWYocz09PSdzdHJvbmdfYmVhcmlzaCcpcmV0dXJuICdzdGF0ZS1zdHJvbmctYmVhcic7CiAgcmV0dXJuICdzdGF0ZS1uZXV0cmFsJzsKfQoKZnVuY3Rpb24gZmFjdG9ySmEoa2V5KXsKICBpZihrZXk9PT0ndGVjaG5pY2FsJylyZXR1cm4gJ+ODhuOCr+ODi+OCq+ODqyc7CiAgaWYoa2V5PT09J2Vhcm5pbmdzJylyZXR1cm4gJ+axuueulyc7CiAgaWYoa2V5PT09J3N1cHBseScpcmV0dXJuICfpnIDntaZwcm94eSc7CiAgaWYoa2V5PT09J3BvbGljeScpcmV0dXJuICflm73nrZYnOwogIHJldHVybiBrZXl8fCfopoHlm6AnOwp9CgpmdW5jdGlvbiBkcml2ZXJTZW50ZW5jZShzYyl7CiAgY29uc3QgcD1zYyYmc2Muc3Ryb25nZXN0X3Bvc2l0aXZlOwogIGNvbnN0IG49c2MmJnNjLnN0cm9uZ2VzdF9uZWdhdGl2ZTsKICBjb25zdCBzY29yZT1OdW1iZXIoc2MmJnNjLnNjb3JlMTAwKTsKCiAgbGV0IGhlYWQ9Jyc7CiAgaWYoTnVtYmVyLmlzRmluaXRlKHNjb3JlKSl7CiAgICBpZihzY29yZT49ODApaGVhZD0n5Y+W5b6X5riI44G/6KaB5Zug44KS57eP5ZCI44GZ44KL44Go44CB5by344GE44OX44Op44K56KmV5L6h44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTY1KWhlYWQ9J+ODl+ODqeOCueimgeWboOOBjOWEquWLouOBp+OAgeOChOOChOW8t+awl+OBruipleS+oeOBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj00NSloZWFkPSfjg5fjg6njgrnjgajjg57jgqTjg4rjgrnjgYzmi67mipfjgZfjgIHkuK3nq4vlnI/jgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49MzApaGVhZD0n44Oe44Kk44OK44K56KaB5Zug44Gu5b2x6Z+/44GM44KE44KE5by344GP44CB5oWO6YeN5a+E44KK44Gn44GZ44CCJzsKICAgIGVsc2UgaGVhZD0n44Oe44Kk44OK44K56KaB5Zug44Gu5b2x6Z+/44GM5aSn44GN44GP44CB5byx5rCX5a+E44KK44Gn44GZ44CCJzsKICB9CgogIGxldCB0YWlsPVtdOwogIGlmKG4pdGFpbC5wdXNoKCfmnIDlpKfjga7mirzjgZfkuIvjgZLopoHlm6Djga8gJytmYWN0b3JKYShuLmtleSkrJyAnK3Njb3JlTGFiZWwobi5zY29yZSkpOwogIGlmKHApdGFpbC5wdXNoKCfmnIDlpKfjga7mirzjgZfkuIrjgZLopoHlm6Djga8gJytmYWN0b3JKYShwLmtleSkrJyAnK3Njb3JlTGFiZWwocC5zY29yZSkpOwogIHJldHVybiBoZWFkKyh0YWlsLmxlbmd0aD8nICcrdGFpbC5qb2luKCfjgIInKSsn44CCJzonJyk7Cn0KCmZ1bmN0aW9uIGRyaXZlckJveEh0bWwodGl0bGUsZCxraW5kKXsKICBpZighZClyZXR1cm4gYDxkaXYgY2xhc3M9ImRyaXZlcmJveCI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7oqbLlvZPjgarjgZc8L2I+PC9kaXY+YDsKICBjb25zdCBzaWduPU51bWJlcihkLmNvbnRyaWJ1dGlvbik+PTA/JysnOicnOwogIHJldHVybiBgPGRpdiBjbGFzcz0iZHJpdmVyYm94Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+CiAgICA8Yj4ke2ZhY3RvckphKGQua2V5KX0gJHtzY29yZUxhYmVsKGQuc2NvcmUpfTwvYj4KICAgIDxzbWFsbCBjbGFzcz0ibXV0ZWQiPuWGjemFjeWIhuW+jOOBrumHjeOBvyAke2Qud2VpZ2h0X3BjdH0lIC8g5a+E5LiOICR7c2lnbn0ke051bWJlcihkLmNvbnRyaWJ1dGlvbikudG9GaXhlZCgxKX08L3NtYWxsPgogIDwvZGl2PmA7Cn0KZnVuY3Rpb24gcGN0KHYpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgyKSsnJSd9CmZ1bmN0aW9uIHN0YXRGb3IoaCxrZXkpe3JldHVybiBoJiZoLmZvcndhcmRfc3RhdHMmJmguZm9yd2FyZF9zdGF0c1trZXldP2guZm9yd2FyZF9zdGF0c1trZXldOm51bGx9CmZ1bmN0aW9uIGNvbmZpZGVuY2VGb3IoaCl7CiAgY29uc3QgdmFscz1baC5yZXR1cm5fMjBkLGgucmV0dXJuXzEyNmQsaC5yZXR1cm5fMjUyZF0ubWFwKE51bWJlcikuZmlsdGVyKE51bWJlci5pc0Zpbml0ZSk7CiAgY29uc3Qgc3RhdHM9WycyMGQnLCcxMjZkJywnMjUyZCddLm1hcChrPT5zdGF0Rm9yKGgsaykpLmZpbHRlcihzPT5zJiZzLnN0YXR1cz09PSdvaycpOwoKICBpZighTnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKXx8IWguYXNvZilyZXR1cm4gMDsKCiAgY29uc3QgY292ZXJhZ2U9dmFscy5sZW5ndGgvMzsKICBsZXQgYWdyZWVtZW50PTAuNTsKICBpZih2YWxzLmxlbmd0aCl7CiAgICBjb25zdCBwb3M9dmFscy5maWx0ZXIoeD0+eD4wKS5sZW5ndGg7CiAgICBjb25zdCBuZWc9dmFscy5maWx0ZXIoeD0+eDwwKS5sZW5ndGg7CiAgICBhZ3JlZW1lbnQ9TWF0aC5tYXgocG9zLG5lZykvdmFscy5sZW5ndGg7CiAgfQogIGNvbnN0IHN0YXRDb3ZlcmFnZT1zdGF0cy5sZW5ndGgvMzsKCiAgcmV0dXJuIE1hdGgucm91bmQoKGNvdmVyYWdlKjAuNDUgKyBhZ3JlZW1lbnQqMC4yNSArIHN0YXRDb3ZlcmFnZSowLjMwKSoxMDApOwp9CgpmdW5jdGlvbiBkZWNpc2lvbkZvcihoLGMpewogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IWguYXNvZil7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOiflrp/jg4fjg7zjgr/mnKrlj5blvpfjgILlj7PkuIrjga7jgIzmm7TmlrDjgI3jgadKLVF1YW50c+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBj+OBoOOBleOBhOOAgicsCiAgICAgIGNvbmZpZGVuY2U6MAogICAgfTsKICB9CgogIGNvbnN0IHIyMD1OdW1iZXIoaC5yZXR1cm5fMjBkKSwgcjEyNj1OdW1iZXIoaC5yZXR1cm5fMTI2ZCksIHIyNTI9TnVtYmVyKGgucmV0dXJuXzI1MmQpOwogIGNvbnN0IGNvbmZpZGVuY2U9Y29uZmlkZW5jZUZvcihoKTsKCiAgaWYoY3VyPD1jLnN0b3BQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifmkI3liIfjgormpJzoqI4nLGNsczonZC1zdG9wJyxyZWFzb246J+ioreWumuOBl+OBn+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+S7peS4iycsY29uZmlkZW5jZX07CiAgfQogIGlmKGN1cj49Yy50YWtlUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5Yip56K65qSc6KiOJyxjbHM6J2QtdGFrZScscmVhc29uOifoqK3lrprjgZfjgZ/liKnnorrlj4LogIPjg6njgqTjg7Pku6XkuIonLGNvbmZpZGVuY2V9OwogIH0KICBpZihjdXI8PWMudHJhaWxQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpemrmOWApOWfuua6luOBruODiOODrOODvOODquODs+OCsOWPguiAg+ODqeOCpOODs+S7peS4iycsY29uZmlkZW5jZX07CiAgfQoKICBsZXQgcG9zaXRpdmU9MCwgbmVnYXRpdmU9MDsKICBbcjIwLHIxMjYscjI1Ml0uZm9yRWFjaCh4PT57CiAgICBpZihOdW1iZXIuaXNGaW5pdGUoeCkpewogICAgICBpZih4PjApcG9zaXRpdmUrKzsKICAgICAgaWYoeDwwKW5lZ2F0aXZlKys7CiAgICB9CiAgfSk7CgogIGlmKG5lZ2F0aXZlPj0yKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel44O7MTI25pel44O7MjUy5pel44Gu44GG44Gh44Oe44Kk44OK44K55YK+5ZCR44GM5YSq5YuiJyxjb25maWRlbmNlfTsKICB9CiAgaWYocG9zaXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon5L+d5pyJ57aZ57aaJyxjbHM6J2QtaG9sZCcscmVhc29uOifoqK3lrprjg6njgqTjg7PlhoXjgafjgIHopIfmlbDmnJ/plpPjga7kvqHmoLzjg4jjg6zjg7Pjg4njgYzjg5fjg6njgrknLGNvbmZpZGVuY2V9OwogIH0KICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntprvvIjmp5jlrZDopovvvIknLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOAguacn+mWk+WIpeODiOODrOODs+ODieOBr+W8t+W8seOBjOa3t+WcqCcsY29uZmlkZW5jZX07Cn0KZnVuY3Rpb24gZXZIdG1sKHRpdGxlLHMpewogIGlmKCFzfHxzLnN0YXR1cyE9PSdvaycpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c21hbGw+5qiZ5pysICR7bn3ku7Y8L3NtYWxsPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj4KICAgIDxiPuW5s+WdhyAke3BjdChzLm1lYW4pfTwvYj4KICAgIDxzbWFsbD7kuK3lpK7lgKQgJHtwY3Qocy5tZWRpYW4pfTwvc21hbGw+CiAgICA8c21hbGw+5LiK5piH546HICR7cGN0KHMucG9zaXRpdmVfcmF0ZSl9PC9zbWFsbD4KICAgIDxzbWFsbD5QMTDjgJxQOTAgJHtwY3Qocy5wMTApfSDjgJwgJHtwY3Qocy5wOTApfTwvc21hbGw+CiAgICA8c21hbGw+5qiZ5pysICR7cy5ufeS7tjwvc21hbGw+CiAgPC9kaXY+YDsKfQoKCmZ1bmN0aW9uIGRpc3RhbmNlSW5mbyhjdXIsdGFyZ2V0LGtpbmQpewogIGN1cj1OdW1iZXIoY3VyKTsgdGFyZ2V0PU51bWJlcih0YXJnZXQpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhTnVtYmVyLmlzRmluaXRlKHRhcmdldCkpcmV0dXJuICfigJQnOwogIGNvbnN0IGRpZmY9KHRhcmdldC9jdXItMSkqMTAwOwogIGlmKGtpbmQ9PT0nc3RvcCcpewogICAgaWYoZGlmZj49MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gTWF0aC5hYnMoZGlmZikudG9GaXhlZCgyKSsnJSDkuIsnOwogIH0KICBpZihraW5kPT09J3Rha2UnKXsKICAgIGlmKGRpZmY8PTApcmV0dXJuICfjg6njgqTjg7PliLDpgZTmuIjjgb8nOwogICAgcmV0dXJuIGRpZmYudG9GaXhlZCgyKSsnJSDkuIonOwogIH0KICByZXR1cm4gKGRpZmY+PTA/JysnOicnKStkaWZmLnRvRml4ZWQoMikrJyUnOwp9CgpmdW5jdGlvbiBwcmljZVJhbmdlKGN1cixzKXsKICBjdXI9TnVtYmVyKGN1cik7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFzfHxzLnN0YXR1cyE9PSdvaycpcmV0dXJuIG51bGw7CiAgcmV0dXJuIHsKICAgIGxvdzpjdXIqKDErTnVtYmVyKHMucDEwKS8xMDApLAogICAgaGlnaDpjdXIqKDErTnVtYmVyKHMucDkwKS8xMDApLAogICAgbWVkaWFuOmN1ciooMStOdW1iZXIocy5tZWRpYW4pLzEwMCkKICB9Owp9CgpmdW5jdGlvbiByYW5nZUh0bWwodGl0bGUsY3VyLHMpewogIGNvbnN0IHI9cHJpY2VSYW5nZShjdXIscyk7CiAgaWYoIXIpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9InJhbmdlYm94Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaomeacrCAke2595Lu2PC9zcGFuPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfSBQMTDjgJxQOTA8L3NwYW4+CiAgICA8Yj4ke3llbihyLmxvdyl9IOOAnCAke3llbihyLmhpZ2gpfTwvYj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Lit5aSu5YCk5o+b566XICR7eWVuKHIubWVkaWFuKX08L3NwYW4+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gYWN0aW9uVGV4dChoLGMsZCl7CiAgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZknKXJldHVybiAn44G+44Ga5a6f44OH44O844K/44KS5pu05paw44GX44Gm44GL44KJ5Yik5pat44CCJzsKICBpZihkLmxhYmVsPT09J+aQjeWIh+OCiuaknOiojicpcmV0dXJuICfmkI3liIfjgorlj4LogIPjg6njgqTjg7PjgpLkuIvlm57jgaPjgabjgYTjgb7jgZnjgILlrp/pmpvjga7nj77lnKjlgKTjgajms6jmlofmnaHku7bjgpLnorroqo3jgZfjgabjgIHnuK7lsI/jg7vmkqTpgIDjgpLmpJzoqI7jgIInOwogIGlmKGQubGFiZWw9PT0n5Yip56K65qSc6KiOJylyZXR1cm4gJ+WIqeeiuuWPguiAg+ODqeOCpOODs+OBq+WIsOmBlOOBl+OBpuOBhOOBvuOBmeOAguWFqOmDqOWjsuWNtOOBoOOBkeOBp+OBquOBj+OAgeWIhuWJsuWIqeeiuuOCguWAmeijnOOAgic7CiAgaWYoZC5sYWJlbD09PSforabmiJInKXJldHVybiAn6K2m5oiS44K+44O844Oz44CC44OI44Os44O844Oq44Oz44Kw44Op44Kk44Oz44Go5Lit55+t5pyf44Gu5YCk5YuV44GN44KS5YSq5YWI44GX44Gm56K66KqN44CCJzsKICByZXR1cm4gJ+ioreWumuODqeOCpOODs+WGheOAguS/neaciee2mee2muWAmeijnOOBp+OBmeOBjOOAgeeEoeaWmeODh+ODvOOCv+OBr+mBheW7tuOBmeOCi+OBn+OCgeWun+mam+OBruePvuWcqOWApOOCgueiuuiqjeOAgic7Cn0KCgpmdW5jdGlvbiBub211cmFOZXRGZWUoYW1vdW50KXsKICBhbW91bnQ9TnVtYmVyKGFtb3VudHx8MCk7CiAgaWYoYW1vdW50PD0wKXJldHVybiAwOwogIGlmKGFtb3VudDw9MTAwMDAwKXJldHVybiAxNTI7CiAgaWYoYW1vdW50PD0zMDAwMDApcmV0dXJuIDMzMDsKICBpZihhbW91bnQ8PTUwMDAwMClyZXR1cm4gNTI0OwogIGlmKGFtb3VudDw9MTAwMDAwMClyZXR1cm4gMTA0ODsKICBpZihhbW91bnQ8PTIwMDAwMDApcmV0dXJuIDIwOTU7CiAgaWYoYW1vdW50PD0zMDAwMDAwKXJldHVybiAzMTQzOwogIGlmKGFtb3VudDw9NTAwMDAwMClyZXR1cm4gNTIzODsKICBpZihhbW91bnQ8PTEwMDAwMDAwKXJldHVybiAxMDQ3NjsKICBpZihhbW91bnQ8PTIwMDAwMDAwKXJldHVybiAyMDk1MjsKICBpZihhbW91bnQ8PTMwMDAwMDAwKXJldHVybiAzMTQyOTsKICBpZihhbW91bnQ8PTUwMDAwMDAwKXJldHVybiA0MTkwNTsKICByZXR1cm4gNzg1NzE7Cn0KZnVuY3Rpb24gZmVlRm9yKGFtb3VudCxtb2RlKXtyZXR1cm4gbW9kZT09PSdub211cmFfbmV0Jz9ub211cmFOZXRGZWUoYW1vdW50KTowfQoKZnVuY3Rpb24gY2FsY0hvbGRpbmcoaCl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IGNvc3Q9TnVtYmVyKGguY29zdCk7CiAgY29uc3Qgc2hhcmVzPU51bWJlcihoLnNoYXJlcyk7CiAgY29uc3QgYnV5VmFsdWU9Y29zdCpzaGFyZXM7CiAgY29uc3QgYnV5RmVlPWZlZUZvcihidXlWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBjdXJyZW50VmFsdWU9Y3VyKnNoYXJlczsKICBjb25zdCBzZWxsRmVlPWZlZUZvcihjdXJyZW50VmFsdWUsaC5mZWVfbW9kZSk7CiAgY29uc3QgaW52ZXN0ZWQ9YnV5VmFsdWUrYnV5RmVlOwogIGNvbnN0IG5ldE5vdz1jdXJyZW50VmFsdWUtc2VsbEZlZS1pbnZlc3RlZDsKICBjb25zdCBuZXROb3dQY3Q9aW52ZXN0ZWQ/bmV0Tm93L2ludmVzdGVkKjEwMDpudWxsOwoKICBjb25zdCBzdG9wUHJpY2U9Y29zdCooMS1OdW1iZXIoaC5zdG9wX3BjdCkvMTAwKTsKICBjb25zdCB0YWtlUHJpY2U9Y29zdCooMStOdW1iZXIoaC50YWtlX3BjdCkvMTAwKTsKICBjb25zdCBoaWdoMjA9TnVtYmVyKGguaGlnaF8yMGR8fGN1cik7CiAgY29uc3QgdHJhaWxQcmljZT1oaWdoMjAqKDEtTnVtYmVyKGgudHJhaWxfcGN0KS8xMDApOwoKICBjb25zdCBzdG9wVmFsdWU9c3RvcFByaWNlKnNoYXJlczsKICBjb25zdCB0YWtlVmFsdWU9dGFrZVByaWNlKnNoYXJlczsKICBjb25zdCBzdG9wTmV0PXN0b3BWYWx1ZS1mZWVGb3Ioc3RvcFZhbHVlLGguZmVlX21vZGUpLWludmVzdGVkOwogIGNvbnN0IHRha2VOZXQ9dGFrZVZhbHVlLWZlZUZvcih0YWtlVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CgogIHJldHVybiB7YnV5VmFsdWUsYnV5RmVlLGN1cnJlbnRWYWx1ZSxzZWxsRmVlLGludmVzdGVkLG5ldE5vdyxuZXROb3dQY3Qsc3RvcFByaWNlLHRha2VQcmljZSx0cmFpbFByaWNlLHN0b3BOZXQsdGFrZU5ldH07Cn0KCgpjb25zdCBuYW1lVGltZXJzPXt9Owpjb25zdCBuYW1lQ2FjaGU9e307CgpmdW5jdGlvbiBkaXNwbGF5Q29tcGFueSh0YXJnZXQsaW5mbyxwcmVmaXg9JycpewogIGlmKCF0YXJnZXQpcmV0dXJuOwogIGlmKCFpbmZvfHwhaW5mby5uYW1lKXsKICAgIHRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICByZXR1cm47CiAgfQogIGxldCBleHRyYXM9W107CiAgaWYoaW5mby5tYXJrZXQpZXh0cmFzLnB1c2goaW5mby5tYXJrZXQpOwogIGlmKGluZm8uc2VjdG9yMzMpZXh0cmFzLnB1c2goaW5mby5zZWN0b3IzMyk7CiAgdGFyZ2V0LmlubmVySFRNTD0nPGI+JytwcmVmaXgraW5mby5uYW1lKyc8L2I+JysoZXh0cmFzLmxlbmd0aD8nPGJyPjxzcGFuIGNsYXNzPSJtdXRlZCI+JytleHRyYXMuam9pbignIC8gJykrJzwvc3Bhbj4nOicnKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0Q29tcGFueShjb2RlKXsKICBjb25zdCBjPVN0cmluZyhjb2RlfHwnJykudHJpbSgpOwogIGlmKCFjKXJldHVybiBudWxsOwogIGlmKG5hbWVDYWNoZVtjXSlyZXR1cm4gbmFtZUNhY2hlW2NdOwogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3NlY3VyaXR5P2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYykse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfpipjmn4TlkI3jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICBuYW1lQ2FjaGVbY109eDsKICByZXR1cm4geDsKfQoKZnVuY3Rpb24gc2NoZWR1bGVDb21wYW55TG9va3VwKGlucHV0SWQsdGFyZ2V0SWQscHJlZml4PScnKXsKICBjbGVhclRpbWVvdXQobmFtZVRpbWVyc1tpbnB1dElkXSk7CiAgY29uc3QgYz0kKGlucHV0SWQpLnZhbHVlLnRyaW0oKTsKICBjb25zdCB0YXJnZXQ9JCh0YXJnZXRJZCk7CgogIGlmKGMubGVuZ3RoPDQpewogICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muKAlCc7CiAgICByZXR1cm47CiAgfQoKICBuYW1lVGltZXJzW2lucHV0SWRdPXNldFRpbWVvdXQoYXN5bmMoKT0+ewogICAgdHJ5ewogICAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PSfpipjmn4TlkI3jgpLnorroqo3kuK3igKYnOwogICAgICBjb25zdCBpbmZvPWF3YWl0IGdldENvbXBhbnkoYyk7CiAgICAgIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeCk7CiAgICB9Y2F0Y2goZSl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nOwogICAgfQogIH0sNDUwKTsKfQoKCgpmdW5jdGlvbiBwcmlvcml0eUFjdGlvbkZvcihoKXsKICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBjb25zdCB2YWxpZD1OdW1iZXIuaXNGaW5pdGUoY3VyKSYmY3VyPjAmJmguYXNvZjsKICBjb25zdCBuYW1lPWguY29tcGFueV9uYW1lfHwnJzsKICBjb25zdCBsYWJlbD0oaC5jb2RlfHwnJykrKG5hbWU/JyAnK25hbWU6JycpOwogIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKCiAgaWYoIXZhbGlkKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjk2LAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5a6f44OH44O844K/44KS5pu05pawJywKICAgICAgZGV0YWlsOifmnIDmlrDlj5blvpfntYLlgKTjgYzjgYLjgorjgb7jgZvjgpPjgILjgb7jgZrjgIzmm7TmlrDjgI3jgadKLVF1YW50c+ODh+ODvOOCv+OCkuWPluW+l+OAgicsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3Qgc3RvcERpc3Q9KGN1ci9jLnN0b3BQcmljZS0xKSoxMDA7CiAgY29uc3QgdGFrZURpc3Q9KGMudGFrZVByaWNlL2N1ci0xKSoxMDA7CiAgY29uc3QgdHJhaWxEaXN0PShjdXIvYy50cmFpbFByaWNlLTEpKjEwMDsKCiAgaWYoY3VyPD1jLnN0b3BQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZToxMDAsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOaQjeWIh+OCiuWPguiAgyAke3llbihjLnN0b3BQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihzdG9wRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NCwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+aQjeWIh+OCiuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7c3RvcERpc3QudG9GaXhlZCgyKX0lIOOBp+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5MCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+WIsOmBlCcsCiAgICAgIGRldGFpbDpg5pyA5paw5Y+W5b6X57WC5YCkICR7eWVuKGN1cil9IC8g5Yip56K65Y+C6ICDICR7eWVuKGMudGFrZVByaWNlKX1gLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKHRha2VEaXN0PD0zKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjg0LAogICAgICBjbHM6J3ByaW9yaXR5LXRha2UnLAogICAgICB0aXRsZTon5Yip56K644Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjgYLjgaggJHt0YWtlRGlzdC50b0ZpeGVkKDIpfSUg44Gn5Yip56K65Y+C6ICD44Op44Kk44OzYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihkLmxhYmVsPT09J+itpuaIkicpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6ODAsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+itpuaIkuWIpOWumicsCiAgICAgIGRldGFpbDpkLnJlYXNvbiwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihOdW1iZXIuaXNGaW5pdGUodHJhaWxEaXN0KSYmdHJhaWxEaXN0PD0yLjUpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6NzYsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+ODiOODrOODvOODquODs+OCsOODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44OI44Os44O844Oq44Oz44Kw5Y+C6ICDICR7eWVuKGMudHJhaWxQcmljZSl9IOOBvuOBpyAke3RyYWlsRGlzdC50b0ZpeGVkKDIpfSVgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIHJldHVybiB7CiAgICBzY29yZTozMCwKICAgIGNsczoncHJpb3JpdHktZ29vZCcsCiAgICB0aXRsZTon6YCa5bi455uj6KaWJywKICAgIGRldGFpbDpkLnJlYXNvbnx8J+ioreWumuODqeOCpOODs+WGhScsCiAgICBsYWJlbAogIH07Cn0KCmZ1bmN0aW9uIHJlbmRlclByaW9yaXR5QWN0aW9ucygpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgYm94PSQoJ3ByaW9yaXR5QWN0aW9ucycpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJldHVybjsKICB9CgogIGxldCBhY3Rpb25zPWEubWFwKHByaW9yaXR5QWN0aW9uRm9yKTsKCiAgLy8gQ29uY2VudHJhdGlvbiBhbGVydCAocG9ydGZvbGlvLWxldmVsKQogIGNvbnN0IHZhbGlkPWEuZmlsdGVyKGg9Pk51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZik7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw+MCl7CiAgICBsZXQgbWF4SG9sZGluZz1udWxsLCBtYXhWYWx1ZT0wOwogICAgdmFsaWQuZm9yRWFjaChoPT57CiAgICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgICAgaWYodj5tYXhWYWx1ZSl7bWF4VmFsdWU9djttYXhIb2xkaW5nPWh9CiAgICB9KTsKICAgIGNvbnN0IGNvbmNlbnRyYXRpb249bWF4VmFsdWUvdG90YWwqMTAwOwogICAgaWYoY29uY2VudHJhdGlvbj49NjAgJiYgbWF4SG9sZGluZyl7CiAgICAgIGFjdGlvbnMucHVzaCh7CiAgICAgICAgc2NvcmU6NzIsCiAgICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICAgIHRpdGxlOifpm4bkuK3luqbjgpLnorroqo0nLAogICAgICAgIGRldGFpbDpgJHttYXhIb2xkaW5nLmNvZGV9JHttYXhIb2xkaW5nLmNvbXBhbnlfbmFtZT8nICcrbWF4SG9sZGluZy5jb21wYW55X25hbWU6Jyd9IOOBjOODneODvOODiOODleOCqeODquOCquOBriAke2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKX0lYCwKICAgICAgICBsYWJlbDon44Od44O844OI44OV44Kp44Oq44KqJwogICAgICB9KTsKICAgIH0KICB9CgogIGFjdGlvbnMuc29ydCgoeCx5KT0+eS5zY29yZS14LnNjb3JlKTsKCiAgY29uc3QgaW1wb3J0YW50PWFjdGlvbnMuZmlsdGVyKHg9Pnguc2NvcmU+PTcwKTsKICBjb25zdCBzaG93bj0oaW1wb3J0YW50Lmxlbmd0aD9pbXBvcnRhbnQ6YWN0aW9ucykuc2xpY2UoMCw0KTsKCiAgYm94LmlubmVySFRNTD1gPGRpdiBjbGFzcz0icHJpb3JpdHktd3JhcCI+JHsKICAgIHNob3duLm1hcCgoeCxpKT0+YAogICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1pdGVtICR7eC5jbHN9Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1saW5lIj4KICAgICAgICAgIDxkaXY+CiAgICAgICAgICAgIDxzcGFuIGNsYXNzPSJwcmlvcml0eS1yYW5rIj5QUklPUklUWSAke2krMX08L3NwYW4+CiAgICAgICAgICAgIDxiPiR7eC50aXRsZX08L2I+CiAgICAgICAgICA8L2Rpdj4KICAgICAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWNvZGUiPiR7eC5sYWJlbH08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NXB4Ij4ke3guZGV0YWlsfTwvZGl2PgogICAgICA8L2Rpdj4KICAgIGApLmpvaW4oJycpCiAgfTwvZGl2PmAgKyAoCiAgICBpbXBvcnRhbnQubGVuZ3RoCiAgICAgID8gJzxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7igLvlhKrlhYjluqbjga/oqK3lrprjg6njgqTjg7PmjqXov5Hjg7vliKTlrprnirbmhYvjg7vjg4fjg7zjgr/mnInnhKHjg7vpm4bkuK3luqbjgYvjgonkvZzjgovnorroqo3poIbjgafjgZnjgILoh6rli5Xlo7LosrfmjIfnpLrjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+JwogICAgICA6ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+57eK5oCl5bqm44Gu6auY44GE6aCF55uu44Gv44GC44KK44G+44Gb44KT44CC6YCa5bi455uj6KaW44KS57aZ57aa44CCPC9wPicKICApOwp9CgpmdW5jdGlvbiBwb3J0Zm9saW9NZWFuRm9yKGEsa2V5KXsKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wKTsKICBjb25zdCB0b3RhbD12YWxpZC5yZWR1Y2UoKHMsaCk9PnMrTnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKSwwKTsKICBpZih0b3RhbDw9MClyZXR1cm4gbnVsbDsKCiAgbGV0IG51bT0wLCBkZW49MDsKICB2YWxpZC5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IHN0PXN0YXRGb3IoaCxrZXkpOwogICAgY29uc3Qgdj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApOwogICAgaWYoc3QmJnN0LnN0YXR1cz09PSdvaycmJk51bWJlci5pc0Zpbml0ZShOdW1iZXIoc3QubWVhbikpJiZ2PjApewogICAgICBudW0gKz0gdipOdW1iZXIoc3QubWVhbik7CiAgICAgIGRlbiArPSB2OwogICAgfQogIH0pOwogIHJldHVybiBkZW4+MD9udW0vZGVuOm51bGw7Cn0KCmZ1bmN0aW9uIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwb3J0Zm9saW9TdW1tYXJ5Jyk7CiAgaWYoIWJveClyZXR1cm47CgogIGlmKCFhLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOiHquWLlembhuioiOOBl+OBvuOBmeOAgjwvcD4nOwogICAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7CiAgICByZXR1cm47CiAgfQoKICBsZXQgdG90YWxDb3N0PTAsIHRvdGFsVmFsdWU9MCwgdG90YWxOZXQ9MDsKICBjb25zdCByb3dzPVtdOwogIGNvbnN0IGRlY2lzaW9ucz17aG9sZDowLHdhdGNoOjAsdGFrZTowLHN0b3A6MCxwZW5kaW5nOjB9OwoKICBhLmZvckVhY2goaD0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICAgIGNvbnN0IHNoYXJlcz1OdW1iZXIoaC5zaGFyZXN8fDApOwogICAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgICBjb25zdCBjdXJyZW50VmFsdWU9dmFsaWQ/Y3VyKnNoYXJlczowOwogICAgY29uc3QgaW52ZXN0ZWQ9TnVtYmVyKGguY29zdHx8MCkqc2hhcmVzK2MuYnV5RmVlOwoKICAgIHRvdGFsQ29zdCArPSBpbnZlc3RlZDsKCiAgICBpZih2YWxpZCl7CiAgICAgIHRvdGFsVmFsdWUgKz0gY3VycmVudFZhbHVlOwogICAgICB0b3RhbE5ldCArPSBjLm5ldE5vdzsKCiAgICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgICAgaWYoZC5sYWJlbD09PSfmkI3liIfjgormpJzoqI4nKWRlY2lzaW9ucy5zdG9wKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKWRlY2lzaW9ucy50YWtlKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSforabmiJInKWRlY2lzaW9ucy53YXRjaCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZJylkZWNpc2lvbnMucGVuZGluZysrOwogICAgICBlbHNlIGRlY2lzaW9ucy5ob2xkKys7CgogICAgICByb3dzLnB1c2goe2NvZGU6aC5jb2RlLG5hbWU6aC5jb21wYW55X25hbWV8fCcnLHZhbHVlOmN1cnJlbnRWYWx1ZSxuZXQ6Yy5uZXROb3d9KTsKICAgIH1lbHNlewogICAgICBkZWNpc2lvbnMucGVuZGluZysrOwogICAgICByb3dzLnB1c2goe2NvZGU6aC5jb2RlLG5hbWU6aC5jb21wYW55X25hbWV8fCcnLHZhbHVlOjAsbmV0Om51bGx9KTsKICAgIH0KICB9KTsKCiAgY29uc3QgbmV0UGN0PXRvdGFsQ29zdD4wP3RvdGFsTmV0L3RvdGFsQ29zdCoxMDA6bnVsbDsKICBjb25zdCBtYXhWYWx1ZT1yb3dzLnJlZHVjZSgobSxyKT0+TWF0aC5tYXgobSxyLnZhbHVlKSwwKTsKICBjb25zdCBjb25jZW50cmF0aW9uPXRvdGFsVmFsdWU+MD9tYXhWYWx1ZS90b3RhbFZhbHVlKjEwMDowOwoKICBjb25zdCBtZWFuMjA9cG9ydGZvbGlvTWVhbkZvcihhLCcyMGQnKTsKICBjb25zdCBtZWFuMTI2PXBvcnRmb2xpb01lYW5Gb3IoYSwnMTI2ZCcpOwogIGNvbnN0IG1lYW4yNTI9cG9ydGZvbGlvTWVhbkZvcihhLCcyNTJkJyk7CgogIGNvbnN0IGFsbG9jYXRpb25zPXJvd3MKICAgIC5maWx0ZXIocj0+ci52YWx1ZT4wKQogICAgLnNvcnQoKHgseSk9PnkudmFsdWUteC52YWx1ZSkKICAgIC5tYXAocj0+ewogICAgICBjb25zdCB3PXRvdGFsVmFsdWU+MD9yLnZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CiAgICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYWxsb2MiPgogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj48Yj4ke3IuY29kZX08L2I+JHtyLm5hbWU/JyAnK3IubmFtZTonJ30gLyAke3cudG9GaXhlZCgxKX0lPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0iYWxsb2NiYXIiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke01hdGgubWluKDEwMCx3KX0lIj48L3NwYW4+PC9kaXY+CiAgICAgIDwvZGl2PmA7CiAgICB9KS5qb2luKCcnKTsKCiAgYm94LmlubmVySFRNTD1gCiAgICA8ZGl2IGNsYXNzPSJwb3J0cm93Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+57eP5oqV6LOH6aGNPC9zcGFuPjxiPiR7eWVuKHRvdGFsQ29zdCl9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7oqZXkvqHpoY08L3NwYW4+PGI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsVmFsdWUpOifigJQnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L685pCN55uKPC9zcGFuPjxiIGNsYXNzPSIke3RvdGFsTmV0Pj0wPydwb3MnOiduZWcnfSI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsTmV0KTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke25ldFBjdD09PW51bGw/J+KAlCc6bmV0UGN0LnRvRml4ZWQoMikrJyUnfTwvc3Bhbj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pyA5aSn6YqY5p+E5q+U546HPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKSsnJSc6J+KAlCd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGRpdiBjbGFzcz0icG9ydHJvdyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5L+d5pyJ57aZ57aaPC9zcGFuPjxiPiR7ZGVjaXNpb25zLmhvbGR9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7orabmiJI8L3NwYW4+PGI+JHtkZWNpc2lvbnMud2F0Y2h9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrmpJzoqI48L3NwYW4+PGI+JHtkZWNpc2lvbnMudGFrZX08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCii/kv53nlZk8L3NwYW4+PGI+JHtkZWNpc2lvbnMuc3RvcCtkZWNpc2lvbnMucGVuZGluZ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn5OKIOipleS+oemhjeWKoOmHjeOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODszwvaDQ+CiAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nn63mnJ8yMOaXpTwvc3Bhbj48Yj4ke21lYW4yMD09PW51bGw/J+KAlCc6bWVhbjIwLnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS4reacnzEyNuaXpTwvc3Bhbj48Yj4ke21lYW4xMjY9PT1udWxsPyfigJQnOm1lYW4xMjYudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZW35pyfMjUy5pelPC9zcGFuPjxiPiR7bWVhbjI1Mj09PW51bGw/J+KAlCc6bWVhbjI1Mi50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+4oC75ZCE6YqY5p+E44Gu6YGO5Y675bmz5Z2H44Oq44K/44O844Oz44KS54++5Zyo44Gu6KmV5L6h6aGN44Gn5Yqg6YeN44GX44Gf5Y+C6ICD5YCk44Gn44GZ44CC55u46Zai44KS6ICD5oWu44GX44Gf5bCG5p2l5LqI5ris44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgoKICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA1cHgiPvCfk6Yg6YqY5p+E5qeL5oiQPC9oND4KICAgICR7YWxsb2NhdGlvbnN8fCc8cCBjbGFzcz0ibXV0ZWQiPuWun+ODh+ODvOOCv+acquWPluW+lzwvcD4nfQogIGA7CiAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7Cn0KZnVuY3Rpb24gcmVuZGVySG9sZGluZ3MoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGlmKCFhLmxlbmd0aCl7JCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7cmVuZGVyUG9ydGZvbGlvU3VtbWFyeSgpO3JldHVybn0KICAkKCdob2xkaW5ncycpLmlubmVySFRNTD1hLm1hcCgoaCxpKT0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IHZhbGlkUHJpY2U9TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCYmaC5hc29mOwogICAgY29uc3QgY2xzPXZhbGlkUHJpY2U/KGMubmV0Tm93Pj0wPydwb3MnOiduZWcnKTonJzsKICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKCiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImhvbGRpbmciPgogICAgICA8ZGl2IGNsYXNzPSJob2xkaW5nLWhlYWQiPgogICAgICAgIDxkaXY+CiAgICAgICAgICA8Yj4ke2guY29kZX08L2I+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+JHtoLmNvbXBhbnlfbmFtZXx8IiJ9PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0icm93Ij4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIHNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaEhvbGRpbmcoJHtpfSkiPuabtOaWsDwvYnV0dG9uPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gZGFuZ2VyIiBvbmNsaWNrPSJyZW1vdmVIb2xkaW5nKCR7aX0pIj7liYrpmaQ8L2J1dHRvbj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+44OH44O844K/5pelICR7aC5hc29mfHwn4oCUJ30gLyDmnIDmlrDlj5blvpfntYLlgKQgJHt2YWxpZFByaWNlP3llbihoLmN1cnJlbnRfcHJpY2UpOifigJQnfSAvICR7aC5zaGFyZXN95qCqIC8g5Y+W5b6X5Y2Y5L6hICR7eWVuKGguY29zdCl9PC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJkZWNpc2lvbiAke2QuY2xzfSI+CiAgICAgICAgJHtkLmxhYmVsfQogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo0cHgiPiR7ZC5yZWFzb259PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KK44G+44GnPC9zcGFuPgogICAgICAgICAgPGIgY2xhc3M9ImRpc3RhbmNlIj4ke3ZhbGlkUHJpY2U/ZGlzdGFuY2VJbmZvKGN1cixjLnN0b3BQcmljZSwnc3RvcCcpOifigJQnfTwvYj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+C6ICDICR7eWVuKGMuc3RvcFByaWNlKX08L3NwYW4+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K644G+44GnPC9zcGFuPgogICAgICAgICAgPGIgY2xhc3M9ImRpc3RhbmNlIj4ke3ZhbGlkUHJpY2U/ZGlzdGFuY2VJbmZvKGN1cixjLnRha2VQcmljZSwndGFrZScpOifigJQnfTwvYj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+C6ICDICR7eWVuKGMudGFrZVByaWNlKX08L3NwYW4+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Yik5a6a5L+h6aC85bqmPC9zcGFuPgogICAgICAgICAgPGI+JHtkLmNvbmZpZGVuY2V9JTwvYj4KICAgICAgICAgIDxkaXYgY2xhc3M9ImdhdWdlIj48c3BhbiBzdHlsZT0id2lkdGg6JHtkLmNvbmZpZGVuY2V9JSI+PC9zcGFuPjwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NXB4Ij7igLvliKTlrprkv6HpoLzluqbjga/jgIHjg4fjg7zjgr/lhYXotrPluqbjg7vmnJ/plpPjg4jjg6zjg7Pjg4njga7kuIDoh7Tluqbjg7vmnJ/lvoXlgKTntbHoqIjjga7mnInnhKHjgYvjgonkvZzjgovlj4LogIPmjIfmqJnjgafjgIHnmoTkuK3norrnjofjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+CgogICAgICA8ZGl2IGNsYXNzPSJhY3Rpb25ib3giPgogICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5LuK44Gp44GG44GZ44KL77yfPC9zcGFuPgogICAgICAgIDxiIHN0eWxlPSJkaXNwbGF5OmJsb2NrO21hcmdpbi10b3A6M3B4Ij4ke2FjdGlvblRleHQoaCxjLGQpfTwvYj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L685pCN55uKPC9zcGFuPjxiIGNsYXNzPSIke2Nsc30iPiR7dmFsaWRQcmljZT95ZW4oYy5uZXROb3cpOifigJQnfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dmFsaWRQcmljZT9mbXQoYy5uZXROb3dQY3QpKyclJzon4oCUJ308L3NwYW4+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuiyt+S7mOaJi+aVsOaWmTwvc3Bhbj48Yj4ke3llbihjLmJ1eUZlZSl9PC9iPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lo7LljbTmiYvmlbDmlpko5LuKKTwvc3Bhbj48Yj4ke3ZhbGlkUHJpY2U/eWVuKGMuc2VsbEZlZSk6J+KAlCd9PC9iPjwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy5zdG9wUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnN0b3BOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K65Y+C6ICDPC9zcGFuPjxiPiR7eWVuKGMudGFrZVByaWNlKX08L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrwgJHt5ZW4oYy50YWtlTmV0KX08L3NwYW4+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODquODs+OCsOWPguiAgzwvc3Bhbj48Yj4ke3ZhbGlkUHJpY2U/eWVuKGMudHJhaWxQcmljZSk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6Xpq5jlgKTln7rmupY8L3NwYW4+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+OryDlrp/nuL7jg5njg7zjgrnkvqHmoLzjg6zjg7Pjgrg8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtyYW5nZUh0bWwoJ+efreacnyAyMOaXpScsY3VyLHN0YXRGb3IoaCwnMjBkJykpfQogICAgICAgICR7cmFuZ2VIdG1sKCfkuK3mnJ8gMTI25pelJyxjdXIsc3RhdEZvcihoLCcxMjZkJykpfQogICAgICAgICR7cmFuZ2VIdG1sKCfplbfmnJ8gMjUy5pelJyxjdXIsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KCiAgICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA3cHgiPvCfk5Ag5a6f57i+44OZ44O844K55pyf5b6F5YCk77yI57Wx6KiI5Y+C6ICD77yJPC9oND4KICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICAgICR7ZXZIdG1sKCfnn63mnJ8gMjDml6UnLHN0YXRGb3IoaCwnMjBkJykpfQogICAgICAgICR7ZXZIdG1sKCfkuK3mnJ8gMTI25pelJyxzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+mVt+acnyAyNTLml6UnLHN0YXRGb3IoaCwnMjUyZCcpKX0KICAgICAgPC9kaXY+CiAgICAgIDxwIGNsYXNzPSJtdXRlZCI+4oC75L6h5qC844Os44Oz44K444O75pyf5b6F5YCk44Gv5bCG5p2l5LqI5ris44Gn44Gv44Gq44GP44CB5Y+W5b6X5Y+v6IO944Gq6YGO5Y675qCq5L6h44Gu44Ot44O844Oq44Oz44Kw5YmN5pa544Oq44K/44O844Oz5YiG5biD44KS5pyA5paw5Y+W5b6X57WC5YCk44Gr5b2T44Gm44Gv44KB44Gf57Wx6KiI5Y+C6ICD44Gn44GZ44CC5pyf6ZaT44GM6YeN44Gq44KL5qiZ5pys44KS5ZCr44G/44G+44GZ44CCPC9wPgogICAgPC9kaXY+YDsKICB9KS5qb2luKCcnKTsKICByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldFF1b3RlKGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3F1b3RlP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCBxPWF3YWl0IHIuanNvbigpOwogIGlmKHEuc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IocS5yZWFzb258fHEuZXJyb3J8fCflrp/jg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4gcTsKfQoKYXN5bmMgZnVuY3Rpb24gYWRkSG9sZGluZygpewogIGNvbnN0IGNvZGU9JCgnaG9sZENvZGUnKS52YWx1ZS50cmltKCk7CiAgY29uc3QgY29zdD12YWwoJ2hvbGRDb3N0JyksIHNoYXJlcz12YWwoJ2hvbGRTaGFyZXMnKTsKICBjb25zdCBzdG9wPXZhbCgnc3RvcFBjdCcpLCB0YWtlPXZhbCgndGFrZVBjdCcpLCB0cmFpbD12YWwoJ3RyYWlsUGN0Jyk7CiAgY29uc3QgZmVlTW9kZT0kKCdmZWVNb2RlJykudmFsdWU7CiAgaWYoIWNvZGV8fCFjb3N0fHwhc2hhcmVzKXthbGVydCgn6YqY5p+E44Kz44O844OJ44O75Y+W5b6X5Y2Y5L6h44O75qCq5pWw44KS5YWl5Yqb44GX44Gm44GtJyk7cmV0dXJufQogIGNvbnN0IGJ0bj1ldmVudD8udGFyZ2V0OyBpZihidG4pe2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJ30KICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICAgIGNvbnN0IGg9ewogICAgICBjb2RlLAogICAgICBjb21wYW55X25hbWU6KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHwnJywKICAgICAgY29tcGFueV9tYXJrZXQ6KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fCcnLAogICAgICBjb21wYW55X3NlY3RvcjMzOihxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fCcnLAogICAgICBjb3N0LCBzaGFyZXMsIGZlZV9tb2RlOmZlZU1vZGUsCiAgICAgIHN0b3BfcGN0OnN0b3A/PzgsIHRha2VfcGN0OnRha2U/PzE1LCB0cmFpbF9wY3Q6dHJhaWw/PzcsCiAgICAgIGN1cnJlbnRfcHJpY2U6cy5sYXN0X2Nsb3NlLCBoaWdoXzIwZDpzLmhpZ2hfMjBkLCBsb3dfMjBkOnMubG93XzIwZCwKICAgICAgcmV0dXJuXzIwZDpzLnJldHVybl8yMGQsIHJldHVybl8xMjZkOnMucmV0dXJuXzEyNmQsIHJldHVybl8yNTJkOnMucmV0dXJuXzI1MmQsCiAgICAgIGZvcndhcmRfc3RhdHM6cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30sCiAgICAgIGFzb2Y6cy5sYXN0X2RhdGUsIHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpCiAgICB9OwogICAgY29uc3QgaWR4PWEuZmluZEluZGV4KHg9PnguY29kZT09PWNvZGUpOwogICAgaWYoaWR4Pj0wKWFbaWR4XT1oOyBlbHNlIGEucHVzaChoKTsKICAgIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICAgIHJlbmRlckhvbGRpbmdzKCk7CiAgfWNhdGNoKGUpe2FsZXJ0KCflj5blvpfjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KICBmaW5hbGx5e2lmKGJ0bil7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn6KiI566X44GX44Gm5L+d5a2YJ319Cn0KCmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hIb2xkaW5nKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyksIGg9YVtpXTsgaWYoIWgpcmV0dXJuOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoaC5jb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgIGguY29tcGFueV9uYW1lPShxLmNvbXBhbnkmJnEuY29tcGFueS5uYW1lKXx8aC5jb21wYW55X25hbWV8fCcnOwogICAgaC5jb21wYW55X21hcmtldD0ocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8aC5jb21wYW55X21hcmtldHx8Jyc7CiAgICBoLmNvbXBhbnlfc2VjdG9yMzM9KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8aC5jb21wYW55X3NlY3RvcjMzfHwnJzsKICAgIGguY3VycmVudF9wcmljZT1zLmxhc3RfY2xvc2U7IGguaGlnaF8yMGQ9cy5oaWdoXzIwZDsgaC5sb3dfMjBkPXMubG93XzIwZDsKICAgIGgucmV0dXJuXzIwZD1zLnJldHVybl8yMGQ7IGgucmV0dXJuXzEyNmQ9cy5yZXR1cm5fMTI2ZDsgaC5yZXR1cm5fMjUyZD1zLnJldHVybl8yNTJkOwogICAgaC5mb3J3YXJkX3N0YXRzPXMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9OwogICAgaC5hc29mPXMubGFzdF9kYXRlOyBoLnVwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogICAgYVtpXT1oOyBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7IHJlbmRlckhvbGRpbmdzKCk7CiAgfWNhdGNoKGUpe2FsZXJ0KCfmm7TmlrDjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KfQpmdW5jdGlvbiByZW1vdmVIb2xkaW5nKGkpe2NvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7YS5zcGxpY2UoaSwxKTtzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7cmVuZGVySG9sZGluZ3MoKX0KCmZ1bmN0aW9uIHJlbmRlcldhdGNoKCl7CiAgJCgnd2F0Y2hzJykuaW5uZXJIVE1MPWxvY2FsKCdmcmVlX3dhdGNoJykubWFwKHg9PnsKICAgIGlmKHR5cGVvZiB4PT09J3N0cmluZycpcmV0dXJuIGA8ZGl2IGNsYXNzPSJiYWRnZSI+JHt4fTwvZGl2PmA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImJhZGdlIj48Yj4ke3guY29kZX08L2I+JHt4Lm5hbWU/JyAnK3gubmFtZTonJ308L2Rpdj5gOwogIH0pLmpvaW4oJycpfHwnPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JzsKfQphc3luYyBmdW5jdGlvbiBhZGRXYXRjaCgpewogIGxldCBjPSQoJ3dhdGNoQ29kZScpLnZhbHVlLnRyaW0oKTsgaWYoIWMpcmV0dXJuOwogIGxldCBpbmZvPW51bGw7CiAgdHJ5e2luZm89YXdhaXQgZ2V0Q29tcGFueShjKX1jYXRjaChlKXt9CiAgbGV0IGE9bG9jYWwoJ2ZyZWVfd2F0Y2gnKTsKICBjb25zdCBleGlzdHM9YS5zb21lKHg9Pih0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKT09PWMpOwogIGlmKCFleGlzdHMpYS5wdXNoKHtjb2RlOmMsbmFtZTppbmZvJiZpbmZvLm5hbWU/aW5mby5uYW1lOicnfSk7CiAgc2F2ZSgnZnJlZV93YXRjaCcsYSk7CiAgcmVuZGVyV2F0Y2goKTsKfQpmdW5jdGlvbiB1cGRhdGVLYWJ1dGFuKCl7bGV0IGM9JCgnY29kZScpLnZhbHVlLnRyaW0oKTskKCdrYWJ1dGFuJykuaHJlZj1jPydodHRwczovL2thYnV0YW4uanAvc3RvY2svP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYyk6J2h0dHBzOi8va2FidXRhbi5qcC8nfQokKCdjb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT57dXBkYXRlS2FidXRhbigpO3NjaGVkdWxlQ29tcGFueUxvb2t1cCgnY29kZScsJ2NvbXBhbnlOYW1lJywnJyl9KTt1cGRhdGVLYWJ1dGFuKCk7CiQoJ2hvbGRDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2hvbGRDb2RlJywnaG9sZENvbXBhbnlOYW1lJywnJykpOwokKCd3YXRjaENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnd2F0Y2hDb2RlJywnd2F0Y2hDb21wYW55TmFtZScsJycpKTsKCgoKYXN5bmMgZnVuY3Rpb24gZ2V0UG9saWN5KGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3BvbGljeT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5Zu9562W44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgcmV0dXJuIHgucG9saWN5fHx7fTsKfQoKZnVuY3Rpb24gcmVuZGVyUG9saWN5VGhlbWVzKHApewogIGNvbnN0IGJveD0kKCdwb2xpY3lUaGVtZXMnKTsKICBpZighYm94KXJldHVybjsKICBjb25zdCB0aGVtZXM9KHAmJnAubWF0Y2hlZF90aGVtZXMpfHxbXTsKICBpZighdGhlbWVzLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8ZGl2IGNsYXNzPSJwb2xpY3l0aGVtZSI+PGI+6Zai6YCj44OG44O844Oe44Gq44GXIC8g5Yik5a6a5L+d55WZPC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pyA5L2O6Zai6YCj5bqm44KS5rqA44Gf44GZ5YWs5byP5pS/562W44OG44O844Oe44GM44GC44KK44G+44Gb44KT44CCPC9zcGFuPjwvZGl2Pic7CiAgICByZXR1cm47CiAgfQogIGJveC5pbm5lckhUTUw9dGhlbWVzLnNsaWNlKDAsNCkubWFwKHQ9PmAKICAgIDxkaXYgY2xhc3M9InBvbGljeXRoZW1lIj4KICAgICAgPGI+JHt0Lm5hbWV9IC8g6Zai6YCj5bqmICR7KE51bWJlcih0LnJlbGV2YW5jZSkqMTAwKS50b0ZpeGVkKDApfSU8L2I+CiAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5pS/562W5by35bqmICR7TnVtYmVyKHQucG9saWN5X3N0cmVuZ3RoKS50b0ZpeGVkKDApfSAvIOWvhOS4jiAke051bWJlcih0LmNvbnRyaWJ1dGlvbikudG9GaXhlZCgxKX0gLyAke3Quc291cmNlX3N0YXR1c308L3NwYW4+PGJyPgogICAgICA8YSBocmVmPSIke3QudXJsfSIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPuWFrOW8j+OCveODvOOCuTwvYT4KICAgIDwvZGl2PgogIGApLmpvaW4oJycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRGdW5kYW1lbnRhbHMoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvZnVuZGFtZW50YWxzP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfmsbrnrpfjg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4geC5mdW5kYW1lbnRhbHN8fHt9Owp9CgpmdW5jdGlvbiBzY29yZUxhYmVsKHYpewogIGlmKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSlyZXR1cm4gJ+KAlCc7CiAgY29uc3Qgbj1OdW1iZXIodik7CiAgcmV0dXJuIChuPjA/JysnOicnKStuLnRvRml4ZWQoMSk7Cn0KCmZ1bmN0aW9uIHBjdE1heWJlKHYpewogIHJldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgxKSsnJSc7Cn0KCmFzeW5jIGZ1bmN0aW9uIGFuYWx5emUoKXsKICBjb25zdCBjb2RlPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7CiAgaWYoIWNvZGUpeyQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfpipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjga0nO3JldHVybn0KICBjb25zdCBidG49JCgnYW5hbHl6ZUJ0bicpOyBidG4uZGlzYWJsZWQ9dHJ1ZTsgYnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnOwogICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSdKLVF1YW50c+OBi+OCieagquS+oeODu+axuueul+Wun+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBhOOBvuOBmeKApic7CgogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICAkKCdwcmljZScpLnZhbHVlPXMubGFzdF9jbG9zZT09bnVsbD8nJzpmbXQocy5sYXN0X2Nsb3NlLDEpOwogICAgJCgncjIwJykudmFsdWU9Zm10KHMucmV0dXJuXzIwZCk7JCgncjEyNicpLnZhbHVlPWZtdChzLnJldHVybl8xMjZkKTskKCdyMjUyJykudmFsdWU9Zm10KHMucmV0dXJuXzI1MmQpOwogICAgJCgnaGlnaDIwJykudGV4dENvbnRlbnQ9Zm10KHMuaGlnaF8yMGQsMSk7JCgnbG93MjAnKS50ZXh0Q29udGVudD1mbXQocy5sb3dfMjBkLDEpOwogICAgJCgndm9sMjAnKS50ZXh0Q29udGVudD1zLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQ9PW51bGw/J+KAlCc6Zm10KHMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZCkrJyUnOwoKICAgIGRpc3BsYXlDb21wYW55KCQoJ2NvbXBhbnlOYW1lJykscS5jb21wYW55fHxudWxsLCcnKTsKICAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0nPGIgY2xhc3M9Im9rIj7inIUgSi1RdWFudHPlrp/jg4fjg7zjgr/lj5blvpdPSzwvYj48YnI+5pyA57WC44OH44O844K/5pelOiAnKyhzLmxhc3RfZGF0ZXx8J+KAlCcpKycgLyDntYLlgKQ6ICcrZm10KHMubGFzdF9jbG9zZSwxKSsnIC8g44K144Oz44OX44OrOiAnKyhzLnNhbXBsZV9jb3VudD8/J+KAlCcpKyfku7YnOwogICAgJCgnYW5hbHlzaXNFdicpLmlubmVySFRNTD0KICAgICAgZXZIdG1sKCfnn63mnJ8yMOaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjBkJ10pKwogICAgICBldkh0bWwoJ+S4reacnzEyNuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMTI2ZCddKSsKICAgICAgZXZIdG1sKCfplbfmnJ8yNTLml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzI1MmQnXSk7CgogICAgY29uc3Qgc3VwcGx5PXMuc3VwcGx5X3Byb3h5fHx7fTsKICAgICQoJ3N1cHBseUF1dG8nKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKHN1cHBseS5zY29yZSk7CiAgICAkKCdzdXBwbHlEZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgJzXml6UvMjDml6Xlh7rmnaXpq5ggJysoc3VwcGx5LnZvbHVtZV9yYXRpb181XzIwPT1udWxsPyfigJQnOk51bWJlcihzdXBwbHkudm9sdW1lX3JhdGlvXzVfMjApLnRvRml4ZWQoMikrJ+WAjScpOwoKICAgIGxldCBmdW5kYW1lbnRhbHM9e307CiAgICB0cnl7CiAgICAgIGZ1bmRhbWVudGFscz1hd2FpdCBnZXRGdW5kYW1lbnRhbHMoY29kZSk7CiAgICAgICQoJ2Vhcm5BdXRvJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChmdW5kYW1lbnRhbHMuc2NvcmUpOwogICAgICBjb25zdCBtPWZ1bmRhbWVudGFscy5tZXRyaWNzfHx7fTsKICAgICAgJCgnZWFybkRldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgICfplovnpLogJysoKGZ1bmRhbWVudGFscy5sYXRlc3QmJmZ1bmRhbWVudGFscy5sYXRlc3QuZGF0ZSl8fCfigJQnKSsKICAgICAgICAnIC8g5aOy5LiKICcrcGN0TWF5YmUobS5zYWxlc19ncm93dGhfcGN0KSsKICAgICAgICAnIC8g5Za25qWt55uKICcrcGN0TWF5YmUobS5vcF9ncm93dGhfcGN0KTsKICAgIH1jYXRjaChmZSl7CiAgICAgICQoJ2Vhcm5BdXRvJykudGV4dENvbnRlbnQ9J+S4jeaYjic7CiAgICAgICQoJ2Vhcm5EZXRhaWwnKS50ZXh0Q29udGVudD0n44GT44Gu44OX44Op44OzL+mKmOafhOOBp+OBr+WPluW+l+OBp+OBjeOBquOBhOWPr+iDveaAp+OBguOCiic7CiAgICAgIGZ1bmRhbWVudGFscz17c2NvcmU6bnVsbH07CiAgICB9CgogICAgbGV0IGF1dG9Qb2xpY3k9e3Njb3JlOm51bGwsbWF0Y2hlZF90aGVtZXM6W119OwogICAgdHJ5ewogICAgICBhdXRvUG9saWN5PWF3YWl0IGdldFBvbGljeShjb2RlKTsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD1hdXRvUG9saWN5LnNjb3JlPT1udWxsPyfkuI3mmI4nOnNjb3JlTGFiZWwoYXV0b1BvbGljeS5zY29yZSk7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgIGF1dG9Qb2xpY3kuc2NvcmU9PW51bGwKICAgICAgICAgID8gJ+mWoumAo+OBmeOCi+WFrOW8j+aUv+etluODhuODvOODnuOBquOBlycKICAgICAgICAgIDogJ+iHquWLlSAvIOS/oemgvOW6piAnKyhhdXRvUG9saWN5LmNvbmZpZGVuY2VfcGN0Pz8n4oCUJykrJyUgLyAnKygoYXV0b1BvbGljeS5tYXRjaGVkX3RoZW1lc3x8W10pLmxlbmd0aCkrJ+ODhuODvOODnic7CiAgICAgIHJlbmRlclBvbGljeVRoZW1lcyhhdXRvUG9saWN5KTsKICAgIH1jYXRjaChwZSl7CiAgICAgICQoJ3BvbGljeVN0YXRlJykudGV4dENvbnRlbnQ9J+S4jeaYjic7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PSflhazlvI/mlL/nrZbjgr3jg7zjgrnlj5blvpfjgqjjg6njg7wnOwogICAgICAkKCdwb2xpY3lUaGVtZXMnKS5pbm5lckhUTUw9Jyc7CiAgICAgIGF1dG9Qb2xpY3k9e3Njb3JlOm51bGwsbWF0Y2hlZF90aGVtZXM6W119OwogICAgfQoKICAgIGNvbnN0IG1hbnVhbFBvbGljeT12YWwoJ3BvbGljeScpOwogICAgY29uc3QgcG9saWN5U2NvcmU9bWFudWFsUG9saWN5PT09bnVsbD9hdXRvUG9saWN5LnNjb3JlOm1hbnVhbFBvbGljeTsKICAgIGNvbnN0IHBvbGljeU1vZGU9bWFudWFsUG9saWN5PT09bnVsbD8nYXV0byc6J21hbnVhbCc7CiAgICBpZihtYW51YWxQb2xpY3khPT1udWxsKXsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKG1hbnVhbFBvbGljeSk7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PSfmiYvlhaXlipvjgafoh6rli5XlgKTjgpLkuIrmm7jjgY0nOwogICAgfQoKICAgIGNvbnN0IGQ9ewogICAgICBjb2RlLAogICAgICBwcmljZTpzLmxhc3RfY2xvc2UsCiAgICAgIHJldHVybjIwOnMucmV0dXJuXzIwZCwKICAgICAgcmV0dXJuMTI2OnMucmV0dXJuXzEyNmQsCiAgICAgIHJldHVybjI1MjpzLnJldHVybl8yNTJkLAogICAgICBlYXJuaW5nc19zY29yZTpmdW5kYW1lbnRhbHMuc2NvcmUsCiAgICAgIHBvbGljeV9zY29yZTpwb2xpY3lTY29yZSwKICAgICAgcG9saWN5X21vZGU6cG9saWN5TW9kZSwKICAgICAgc3VwcGx5X3Njb3JlOnN1cHBseS5zY29yZQogICAgfTsKCiAgICBjb25zdCBhcj1hd2FpdCBmZXRjaCgnL2FwaS9mcmVlL2FuYWx5emUnLHsKICAgICAgbWV0aG9kOidQT1NUJywKICAgICAgaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSwKICAgICAgYm9keTpKU09OLnN0cmluZ2lmeShkKSwKICAgICAgY2FjaGU6J25vLXN0b3JlJwogICAgfSk7CiAgICBjb25zdCB4PWF3YWl0IGFyLmpzb24oKTsKICAgIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfliIbmnpDjg4fjg7zjgr/jgYzkuI3otrPjgZfjgabjgYTjgb7jgZknKTsKCiAgICAkKCdzdGF0ZScpLnRleHRDb250ZW50PXN0YXRlSmEoeC5zaWduYWwuc3RhdGUpOwogICAgJCgncG9zJykudGV4dENvbnRlbnQ9eC5zaWduYWwucG9zaXRpdmVfY291bnQ7CiAgICAkKCduZWcnKS50ZXh0Q29udGVudD14LnNpZ25hbC5uZWdhdGl2ZV9jb3VudDsKCiAgICBjb25zdCBzYz14LnNjb3JlfHx7fTsKICAgICQoJ3Njb3JlSGVybycpLnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgICQoJ3Njb3JlMTAwJykudGV4dENvbnRlbnQ9c2Muc2NvcmUxMDA9PW51bGw/J+KAlCc6c2Muc2NvcmUxMDArJyAvIDEwMCc7CgogICAgY29uc3QgcGlsbD0kKCdzY29yZVN0YXRlUGlsbCcpOwogICAgcGlsbC5jbGFzc05hbWU9J3N0YXRlcGlsbCAnK3N0YXRlQ2xhc3MoeC5zaWduYWwuc3RhdGUpOwogICAgcGlsbC50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTsKCiAgICAkKCdjb3ZlcmFnZScpLnRleHRDb250ZW50PXNjLmNvdmVyYWdlX3BjdD09bnVsbD8n4oCUJzpzYy5jb3ZlcmFnZV9wY3QrJyUnOwogICAgJCgnc2NvcmVCcmVha2Rvd24nKS50ZXh0Q29udGVudD0KICAgICAgJ+ODhuOCr+ODi+OCq+ODqyAnK3Njb3JlTGFiZWwoc2MudGVjaG5pY2FsKSsKICAgICAgJyAvIOaxuueulyAnK3Njb3JlTGFiZWwoc2MuZWFybmluZ3MpKwogICAgICAnIC8g6ZyA57WmICcrc2NvcmVMYWJlbChzYy5zdXBwbHkpKwogICAgICAnIC8g5Zu9562WICcrc2NvcmVMYWJlbChzYy5wb2xpY3kpOwoKICAgICQoJ3Njb3JlUmVhc29uJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmVSZWFzb25UZXh0JykudGV4dENvbnRlbnQ9ZHJpdmVyU2VudGVuY2Uoc2MpOwogICAgJCgnZHJpdmVyR3JpZCcpLmlubmVySFRNTD0KICAgICAgZHJpdmVyQm94SHRtbCgn5pyA5aSn44Gu44OX44Op44K56KaB5ZugJyxzYy5zdHJvbmdlc3RfcG9zaXRpdmUsJ3Bvc2l0aXZlJykrCiAgICAgIGRyaXZlckJveEh0bWwoJ+acgOWkp+OBruODnuOCpOODiuOCueimgeWboCcsc2Muc3Ryb25nZXN0X25lZ2F0aXZlLCduZWdhdGl2ZScpOwoKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PQogICAgICAn54q25oWL6KGo56S644Gv57eP5ZCI54K544Gr6YCj5YuV44CCODDku6XkuIo95by35rCX44CBNjXjgJw3OT3jgoTjgoTlvLfmsJfjgIE0NeOAnDY0PeS4reeri+OAgTMw44CcNDQ944KE44KE5byx5rCX44CBMjnku6XkuIs95byx5rCX44CCJzsKICB9Y2F0Y2goZSl7CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n4pqg77iPICcrZS5tZXNzYWdlOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPSc8c3BhbiBjbGFzcz0iZXJyIj7lj5blvpfjgqjjg6njg7w6ICcrZS5tZXNzYWdlKyc8L3NwYW4+JzsKICB9ZmluYWxseXsKICAgIGJ0bi5kaXNhYmxlZD1mYWxzZTsKICAgIGJ0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn5YiG5p6QJzsKICB9Cn0KCnJlbmRlckhvbGRpbmdzKCk7cmVuZGVyV2F0Y2goKTsKaWYoJ3NlcnZpY2VXb3JrZXInIGluIG5hdmlnYXRvcil7bmF2aWdhdG9yLnNlcnZpY2VXb3JrZXIuZ2V0UmVnaXN0cmF0aW9ucygpLnRoZW4ocnM9PlByb21pc2UuYWxsKHJzLm1hcChyPT5yLnVucmVnaXN0ZXIoKSkpKS5jYXRjaCgoKT0+e30pfQppZignY2FjaGVzJyBpbiB3aW5kb3cpe2NhY2hlcy5rZXlzKCkudGhlbihrZXlzPT5Qcm9taXNlLmFsbChrZXlzLm1hcChrPT5jYWNoZXMuZGVsZXRlKGspKSkpLmNhdGNoKCgpPT57fSl9Cjwvc2NyaXB0Pgo8L21haW4+CjwvYm9keT4KPC9odG1sPg=="
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
