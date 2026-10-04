from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64, csv, io
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.14-FREE-QUOTE-HYBRID'
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



def stooq_symbol(code):
    code = str(code or "").strip()
    if code.isdigit() and len(code) >= 4:
        code = code[:4]
    return code.lower() + ".jp"


def stooq_quote(code):
    symbol = stooq_symbol(code)
    r = requests.get(
        "https://stooq.com/q/l/",
        params={
            "s": symbol,
            "f": "sd2t2ohlcv",
            "h": "1",
            "e": "csv",
        },
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 12) "
                "AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36"
            )
        },
        timeout=10,
    )
    if r.status_code != 200:
        raise RuntimeError("Stooq HTTP " + str(r.status_code))

    text = r.text.strip()
    if not text or "N/D" in text:
        raise RuntimeError("Stooq returned no usable quote")

    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        raise RuntimeError("Stooq returned no quote row")

    row = rows[0]

    def f(name):
        value = row.get(name)
        if value in (None, "", "N/D"):
            return None
        try:
            return float(value)
        except Exception:
            return None

    close = f("Close")
    date = row.get("Date")
    if close is None or close <= 0 or not date or date == "N/D":
        raise RuntimeError("Stooq quote is incomplete")

    return {
        "symbol": row.get("Symbol") or symbol,
        "date": date,
        "time": row.get("Time") if row.get("Time") not in (None, "", "N/D") else None,
        "open": f("Open"),
        "high": f("High"),
        "low": f("Low"),
        "close": close,
        "volume": f("Volume"),
        "source": "Stooq free quote CSV",
        "mode": "free_snapshot",
    }


def jquants_code(code):
    code = str(code or "").strip()
    if code.isdigit() and len(code) == 4:
        return code + "0"
    return code




POLICY_THEMES = json.loads(r"""[{"key":"ai_semiconductor","name":"AI\u30fb\u534a\u5c0e\u4f53","url":"https://www.meti.go.jp/policy/mono_info_service/ai_semiconductor_frame/ai_semiconductor_frame.html","required_terms":["AI","\u534a\u5c0e\u4f53","10\u5146\u5186"],"strength":100.0,"sector_weights":{"\u96fb\u6c17\u6a5f\u5668":0.75,"\u7cbe\u5bc6\u6a5f\u5668":0.55,"\u60c5\u5831\u30fb\u901a\u4fe1\u696d":0.65,"\u6a5f\u68b0":0.35,"\u975e\u9244\u91d1\u5c5e":0.25},"name_keywords":{"\u534a\u5c0e\u4f53":0.45,"\u96fb\u6a5f":0.2,"\u96fb\u5b50":0.25,"\u901a\u4fe1":0.2,"\u30c7\u30fc\u30bf":0.2,"\u30b7\u30b9\u30c6\u30e0":0.15,"\u96fb\u7dda":0.18,"\u96fb\u5de5":0.18}},{"key":"gx_datacenter","name":"GX\u30fb\u96fb\u529b\u30fb\u30c7\u30fc\u30bf\u30bb\u30f3\u30bf\u30fc","url":"https://www.meti.go.jp/policy/energy_environment/global_warming/gx_strategy_area.html","required_terms":["GX","\u30c7\u30fc\u30bf\u30bb\u30f3\u30bf\u30fc","\u8131\u70ad\u7d20"],"strength":92.0,"sector_weights":{"\u96fb\u6c17\u30fb\u30ac\u30b9\u696d":0.85,"\u975e\u9244\u91d1\u5c5e":0.55,"\u96fb\u6c17\u6a5f\u5668":0.55,"\u5efa\u8a2d\u696d":0.45,"\u6a5f\u68b0":0.35,"\u8f38\u9001\u7528\u6a5f\u5668":0.4,"\u9244\u92fc":0.35,"\u5316\u5b66":0.3},"name_keywords":{"\u96fb\u529b":0.35,"\u96fb\u7dda":0.35,"\u96fb\u5de5":0.35,"\u30b1\u30fc\u30d6\u30eb":0.35,"\u96fb\u6a5f":0.2,"\u30a8\u30cd\u30eb\u30ae\u30fc":0.3,"\u96fb\u6c60":0.3,"\u84c4\u96fb":0.3,"\u91cd\u5de5":0.15}},{"key":"defense","name":"\u9632\u885b\u30fb\u5b87\u5b99\u30fb\u30b5\u30a4\u30d0\u30fc","url":"https://www.mod.go.jp/j/policy/agenda/guideline/plan/plan_01.html","required_terms":["\u9632\u885b\u529b","2027\u5e74\u5ea6","\u5b87\u5b99"],"strength":95.0,"sector_weights":{"\u6a5f\u68b0":0.45,"\u96fb\u6c17\u6a5f\u5668":0.45,"\u8f38\u9001\u7528\u6a5f\u5668":0.2,"\u7cbe\u5bc6\u6a5f\u5668":0.45,"\u60c5\u5831\u30fb\u901a\u4fe1\u696d":0.45,"\u9244\u92fc":0.2,"\u975e\u9244\u91d1\u5c5e":0.2},"name_keywords":{"\u91cd\u5de5":0.45,"\u822a\u7a7a":0.45,"\u9020\u8239":0.45,"\u5b87\u5b99":0.45,"\u9632\u885b":0.45,"\u96fb\u6a5f":0.15,"\u901a\u4fe1":0.2,"\u30b7\u30b9\u30c6\u30e0":0.15}},{"key":"resilience","name":"\u56fd\u571f\u5f37\u9771\u5316\u30fb\u30a4\u30f3\u30d5\u30e9","url":"https://www.cas.go.jp/jp/seisaku/kokudo_kyoujinka/dai1_chuukikeikaku/index.html","required_terms":["\u56fd\u571f\u5f37\u9771\u5316","\u4ee4\u548c\uff18\u5e74\u5ea6","\u4ee4\u548c12\u5e74\u5ea6"],"strength":90.0,"sector_weights":{"\u5efa\u8a2d\u696d":0.8,"\u91d1\u5c5e\u88fd\u54c1":0.5,"\u6a5f\u68b0":0.35,"\u9244\u92fc":0.45,"\u975e\u9244\u91d1\u5c5e":0.45,"\u96fb\u6c17\u6a5f\u5668":0.3,"\u30ac\u30e9\u30b9\u30fb\u571f\u77f3\u88fd\u54c1":0.55},"name_keywords":{"\u5efa\u8a2d":0.35,"\u9053\u8def":0.35,"\u6a4b\u6881":0.4,"\u30bb\u30e1\u30f3\u30c8":0.35,"\u96fb\u7dda":0.25,"\u96fb\u5de5":0.25,"\u30b1\u30fc\u30d6\u30eb":0.25,"\u9244\u5de5":0.25}}]""")

POLICY_REGISTRY_VERIFIED_ON = "2026-10-04"
POLICY_REGISTRY_MAX_AGE_DAYS = 90
POLICY_REGISTRY_STRENGTH_FACTOR = 0.75



def check_policy_source(theme):
    key = theme["key"]
    now_dt = datetime.now(timezone.utc)
    now_ts = now_dt.timestamp()
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
        "registry_verified_on": POLICY_REGISTRY_VERIFIED_ON,
    }

    live_ok = False
    try:
        r = requests.get(
            theme["url"],
            timeout=8,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Linux; Android 12) "
                    "AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36"
                ),
                "Accept-Language": "ja,en-US;q=0.8,en;q=0.6",
            },
        )
        result["http_status"] = r.status_code
        if r.status_code == 200:
            r.encoding = r.apparent_encoding or r.encoding
            text = r.text[:500000]
            hits = [
                term
                for term in theme["required_terms"]
                if term in text
            ]
            ratio = len(hits) / max(1, len(theme["required_terms"]))
            result["matched_terms"] = hits

            if ratio >= 2 / 3:
                result["status"] = "verified_live"
                result["strength"] = theme["strength"]
                live_ok = True
            elif hits:
                result["status"] = "partial_live"
                result["strength"] = theme["strength"] * 0.60
                live_ok = True
    except Exception as e:
        result["reason"] = str(e)

    if not live_ok:
        try:
            verified_date = datetime.fromisoformat(
                POLICY_REGISTRY_VERIFIED_ON
            ).replace(tzinfo=timezone.utc)
            age_days = (now_dt - verified_date).days
        except Exception:
            age_days = POLICY_REGISTRY_MAX_AGE_DAYS + 1

        if age_days <= POLICY_REGISTRY_MAX_AGE_DAYS:
            result["status"] = "verified_registry"
            result["strength"] = (
                theme["strength"]
                * POLICY_REGISTRY_STRENGTH_FACTOR
            )
            result["registry_age_days"] = age_days
            result["fallback_reason"] = (
                "Official source live fetch was blocked or unverifiable; "
                "using last verified official-source registry with reduced confidence."
            )

    POLICY_SOURCE_CACHE[key] = {
        "ts": now_ts,
        "result": result,
    }
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

    status_confidence = {
        "verified_live": 1.00,
        "partial_live": 0.65,
        "verified_registry": 0.70,
    }
    confidence = (
        sum(
            status_confidence.get(
                m["source_status"],
                0.0,
            )
            for m in matched
        )
        / len(matched)
        * 100.0
        if matched
        else 0.0
    )

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

    company = {
        "code": code,
        "name": "",
        "market": "",
        "sector33": "",
    }
    jq_rows = []
    jq_snap = None
    jq_error = None

    if jquants_status():
        try:
            company = security_info(code)
        except Exception:
            pass

        try:
            jq_code = jquants_code(code)
            data = jq_request(
                "/equities/bars/daily",
                {"code": jq_code},
            )
            jq_rows = normalize_quote_rows(data)
            jq_snap = technical_snapshot(jq_rows)
        except Exception as e:
            jq_error = str(e)

    stooq = None
    stooq_error = None
    try:
        stooq = stooq_quote(code)
    except Exception as e:
        stooq_error = str(e)

    if jq_snap is None and stooq is None:
        return jsonify(
            status="error",
            code=code,
            reason=(
                "Free quote source failed"
                + ("; " + stooq_error if stooq_error else "")
                + (
                    "; J-Quants failed: " + jq_error
                    if jq_error else ""
                )
            ),
        ), 502

    if jq_snap is not None:
        snap = dict(jq_snap)
        history_last_date = jq_snap.get("last_date")
    else:
        snap = {
            "status": "partial",
            "last_close": None,
            "last_date": None,
            "return_20d": None,
            "return_126d": None,
            "return_252d": None,
            "volatility_20d_annualized": None,
            "high_20d": None,
            "low_20d": None,
            "sample_count": 0,
            "supply_proxy": {
                "status": "insufficient_data",
                "score": None,
                "method": "history unavailable",
            },
            "forward_return_stats": {
                "20d": {"status": "insufficient_data", "n": 0},
                "126d": {"status": "insufficient_data", "n": 0},
                "252d": {"status": "insufficient_data", "n": 0},
            },
        }
        history_last_date = None

    # Use Stooq only when it is at least as new as J-Quants history.
    use_stooq = False
    if stooq is not None:
        if not history_last_date:
            use_stooq = True
        else:
            use_stooq = str(stooq["date"]) >= str(history_last_date)

    if use_stooq:
        snap["last_close"] = stooq["close"]
        snap["last_date"] = stooq["date"]
        snap["price_source"] = stooq["source"]
        snap["price_time"] = stooq.get("time")
        snap["price_open"] = stooq.get("open")
        snap["price_high"] = stooq.get("high")
        snap["price_low"] = stooq.get("low")
        snap["price_volume"] = stooq.get("volume")
        quote_source = "Stooq free quote CSV"
    else:
        snap["price_source"] = "J-Quants API v2"
        snap["price_time"] = None
        quote_source = "J-Quants API v2"

    snap["history_last_date"] = history_last_date
    snap["history_source"] = (
        "J-Quants API v2"
        if jq_snap is not None
        else None
    )
    snap["history_is_same_as_price"] = (
        bool(history_last_date)
        and str(history_last_date) == str(snap.get("last_date"))
    )

    return jsonify(
        status="ok",
        code=code,
        company=company,
        source=quote_source,
        snapshot=snap,
        rows=jq_rows[-30:],
        free_quote=stooq,
        free_quote_error=stooq_error,
        jquants_error=jq_error,
    )


@APP.get("/api/mobile/free-price-test")
def mobile_free_price_test():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400
    try:
        q = stooq_quote(code)
        return jsonify(
            status="ok",
            code=code,
            quote=q,
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
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9CgoucG9saWN5dGhlbWUgYXtmb250LXNpemU6MTJweH0KLmNvbnRyaWJncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQouY29udHJpYntib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDtiYWNrZ3JvdW5kOiNmZmZ9Ci5jb250cmliIHNwYW57ZGlzcGxheTpibG9ja30KLmNvbnRyaWIgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4O21hcmdpbi10b3A6MnB4fQoKLmNvbnRyaWIgc21hbGx7ZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweDtjb2xvcjojNmI3MjgwO2xpbmUtaGVpZ2h0OjEuMzV9Ci5mcmVzaGJveHtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMXB4O21hcmdpbi10b3A6OXB4O2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLmZyZXNoLW9re2JhY2tncm91bmQ6I2VjZmRmNTtib3JkZXItY29sb3I6I2E3ZjNkMH0KLmZyZXNoLXdhcm57YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1jb2xvcjojZmRlNjhhfQouZnJlc2gtc3RhbGV7YmFja2dyb3VuZDojZmVmMmYyO2JvcmRlci1jb2xvcjojZmVjYWNhfQoKLmZyZXNoYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweH0KLnNvdXJjZWJhZGdle2Rpc3BsYXk6aW5saW5lLWJsb2NrO3BhZGRpbmc6NHB4IDhweDtib3JkZXItcmFkaXVzOjk5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTJweDtmb250LXdlaWdodDo4MDA7bWFyZ2luLXJpZ2h0OjRweH0KLmhpc3Rvcnl3YXJue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDttYXJnaW4tdG9wOjdweDtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyOjFweCBzb2xpZCAjZmRlNjhhfQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5jb250cmliZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+CjxtYWluPgo8ZGl2IGNsYXNzPSJ0b3AiPgogIDxoMT7wn5OIIOaXpeacrOagqkFJIEZSRUU8L2gxPgogIDxkaXYgY2xhc3M9InN1YiI+54Sh5paZ5qCq5L6h44K544OK44OD44OX44K344On44OD44OIIC8gSi1RdWFudHPliIbmnpAgLyDprq7luqbjgqzjg7zjg4kgLyDkv53mnInliKTmlq08L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+OryDpipjmn4TliIbmnpA8L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQiPgogICAgPGlucHV0IGlkPSJjb2RlIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kg5L6LIDcyMDMiPgogICAgPGlucHV0IGlkPSJwcmljZSIgcGxhY2Vob2xkZXI9IuWPluW+l+e1guWApCIgcmVhZG9ubHk+CiAgPC9kaXY+CiAgPGRpdiBpZD0iY29tcGFueU5hbWUiIGNsYXNzPSJzb3VyY2UgbXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GZ44KL44Go5Lya56S+5ZCN44KS6KGo56S644GX44G+44GZPC9kaXY+CiAgPGRpdiBjbGFzcz0icm93Ij4KICAgIDxhIGlkPSJrYWJ1dGFuIiBjbGFzcz0iYnRuIHNlY29uZGFyeSIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPuagquaOouOBp+eiuuiqjTwvYT4KICAgIDxidXR0b24gaWQ9ImFuYWx5emVCdG4iIG9uY2xpY2s9ImFuYWx5emUoKSI+5a6f44OH44O844K/44Gn5YiG5p6QPC9idXR0b24+CiAgPC9kaXY+CiAgPHAgY2xhc3M9Im11dGVkIj7pipjmn4TjgrPjg7zjg4njgpLlhaXjgozjgabmirzjgZnjgajjgIFKLVF1YW50c+OBi+OCieWPluW+l+OBp+OBjeOCi+Wun+ODh+ODvOOCv+OCkuiHquWLleWFpeWKm+OBl+OBvuOBmeOAgjwvcD4KICA8ZGl2IGlkPSJzb3VyY2VCb3giIGNsYXNzPSJzb3VyY2UgbXV0ZWQiPuODh+ODvOOCv+acquWPluW+lzwvZGl2PgogIDxkaXYgaWQ9Imhpc3RvcnlGcmVzaG5lc3NCb3giIGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8Yj7wn5OaIOWIhuaekOWxpeattOOBrumuruW6pjwvYj4KICAgIDxkaXYgaWQ9Imhpc3RvcnlGcmVzaG5lc3NUZXh0IiBjbGFzcz0ibXV0ZWQiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9ImZyZXNobmVzc0JveCIgY2xhc3M9ImZyZXNoYm94IGZyZXNoLXdhcm4iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPGIgaWQ9ImZyZXNobmVzc1RpdGxlIj7jg4fjg7zjgr/prq7luqY8L2I+CiAgICA8ZGl2IGlkPSJmcmVzaG5lc3NEZXRhaWwiIGNsYXNzPSJtdXRlZCI+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfk4og5qCq5L6h44O744OG44Kv44OL44Kr44Or5a6f57i+PC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XpqLDokL3njocgJTwvc3Bhbj48aW5wdXQgaWQ9InIyMCIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MTI25pelICU8L3NwYW4+PGlucHV0IGlkPSJyMTI2IiByZWFkb25seT48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yNTLml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIyNTIiIHJlYWRvbmx5PjwvZGl2PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6auY5YCkPC9zcGFuPjxiIGlkPSJoaWdoMjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeWuieWApDwvc3Bhbj48YiBpZD0ibG93MjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeW5tOeOh+ODnOODqTwvc3Bhbj48YiBpZD0idm9sMjAiPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+nqSDlrp/jg4fjg7zjgr/opoHlm6A8L2gzPgogIDxwIGNsYXNzPSJtdXRlZCI+5rG6566X44GvSi1RdWFudHPosqHli5njgrXjg57jg6rjg7zjgIHpnIDntabjga/lrp/moKrkvqHjg7vlh7rmnaXpq5hwcm94eeOAgeWbveetluOBr+aUv+W6nOWFrOW8j+aUv+etluOCveODvOOCue+8i+alreeori/npL7lkI3jga7plqLpgKPluqbjgYvjgonoh6rli5XmjqHngrnjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBjbGFzcz0iZmFjdG9yZ3JpZCI+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5rG6566XPC9zcGFuPjxiIGlkPSJlYXJuQXV0byI+4oCUPC9iPjxzbWFsbCBpZD0iZWFybkRldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrlj5blvpc8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPumcgOe1pnByb3h5PC9zcGFuPjxiIGlkPSJzdXBwbHlBdXRvIj7igJQ8L2I+PHNtYWxsIGlkPSJzdXBwbHlEZXRhaWwiIGNsYXNzPSJtdXRlZCI+5pyq5Y+W5b6XPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7lm73nrZZwcm94eTwvc3Bhbj48YiBpZD0icG9saWN5U3RhdGUiPuKAlDwvYj48c21hbGwgaWQ9InBvbGljeURldGFpbCIgY2xhc3M9Im11dGVkIj7lhazlvI/mlL/nrZbjgr3jg7zjgrnnorroqo3liY08L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODh+ODvOOCv+WFhei2szwvc3Bhbj48YiBpZD0iY292ZXJhZ2UiPuKAlDwvYj48c21hbGwgY2xhc3M9Im11dGVkIj7nt4/lkIjmjqHngrnjgavkvb/jgYjjgZ/ph43jgb88L3NtYWxsPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InBvbGljeVRoZW1lcyIgY2xhc3M9InBvbGljeXRoZW1lcyI+PC9kaXY+CiAgPGRpdiBzdHlsZT0ibWFyZ2luLXRvcDo5cHgiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lm73nrZbjgrnjgrPjgqLkuIrmm7jjgY3vvIjku7vmhI/vvIkgLTEwMOOAnDEwMDwvc3Bhbj4KICAgIDxpbnB1dCBpZD0icG9saWN5IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLnqbrmrITjgarjgonoh6rli5Xlm73nrZZwcm94eeOCkuS9v+eUqCI+CiAgPC9kaXY+CiAgPHAgY2xhc3M9Im11dGVkIj7igLvlm73nrZZwcm94eeOBr+OAgeaUv+W6nOWFrOW8j+aUv+etluOCveODvOOCueOBqEotUXVhbnRz44Gu5qWt56iu44O75Lya56S+5ZCN44Go44Gu6Zai6YCj5bqm44KS57WE44G/5ZCI44KP44Gb44Gf5Y+C6ICD5YCk44Gn44GZ44CC44Op44Kk44OW56K66KqN44Gn44GN44Gq44GE5aC05ZCI44Gv5pyA57WC56K66KqN5riI44G/5oOF5aCx44KS5L2O5L+h6aC85bqm44Gn5L2/55So44GX44CB44Gd44Gu54q25oWL44KC55S76Z2i44Gr5piO56S644GX44G+44GZ44CC6KOc5Yqp6YeR5o6h5oqe44KE5qWt57i+5oGp5oG144Gd44Gu44KC44Gu44KS6Ki85piO44GZ44KL5oyH5qiZ44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn6egIOWIhuaekOe1kOaenDwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPueKtuaFizwvc3Bhbj48YiBpZD0ic3RhdGUiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg5fjg6njgrnmoLnmi6A8L3NwYW4+PGIgaWQ9InBvcyI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODnuOCpOODiuOCueagueaLoDwvc3Bhbj48YiBpZD0ibmVnIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0ic2NvcmVIZXJvIiBjbGFzcz0ic2NvcmVoZXJvIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+57eP5ZCI5o6h54K577yI5Y+W5b6X44OH44O844K/44O744Or44O844Or44OZ44O844K577yJPC9zcGFuPgogICAgPGIgaWQ9InNjb3JlMTAwIj7igJQ8L2I+CiAgICA8c3BhbiBpZD0ic2NvcmVTdGF0ZVBpbGwiIGNsYXNzPSJzdGF0ZXBpbGwgc3RhdGUtbmV1dHJhbCI+4oCUPC9zcGFuPgogICAgPGRpdiBpZD0ic2NvcmVCcmVha2Rvd24iIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJzY29yZVJlYXNvbiIgY2xhc3M9InNjb3JlLXJlYXNvbiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3Ryb25nPvCfp60g44Gq44Gc44GT44Gu54K55pWw77yfPC9zdHJvbmc+CiAgICA8ZGl2IGlkPSJzY29yZVJlYXNvblRleHQiIGNsYXNzPSJtdXRlZCI+4oCUPC9kaXY+CiAgICA8ZGl2IGlkPSJkcml2ZXJHcmlkIiBjbGFzcz0iZHJpdmVyZ3JpZCI+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0iY29udHJpYnV0aW9uQm94IiBjbGFzcz0ic2NvcmUtcmVhc29uIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxzdHJvbmc+8J+nriDnt4/lkIjngrnjgbjjga7lr4TkuI48L3N0cm9uZz4KICAgIDxkaXYgY2xhc3M9Im11dGVkIj7lj5blvpfjgafjgY3jgZ/opoHlm6DjgaDjgZHjgafph43jgb/jgpLlho3phY3liIbjgZfjgZ/lvozjgIHlkITopoHlm6DjgYznt4/lkIjoqZXkvqHjgpLjganjgozjgaDjgZHmirzjgZfkuIrjgZLvvI/mirzjgZfkuIvjgZLjgZ/jgYvjgpLooajnpLrjgZfjgb7jgZnjgII8L2Rpdj4KICAgIDxkaXYgaWQ9ImNvbnRyaWJ1dGlvbkdyaWQiIGNsYXNzPSJjb250cmliZ3JpZCI+PC9kaXY+CiAgPC9kaXY+CiAgPHAgaWQ9InJlc3VsdCIgY2xhc3M9Im11dGVkIj7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjgIzlrp/jg4fjg7zjgr/jgafliIbmnpDjgI3jgpLmirzjgZfjgabjgY/jgaDjgZXjgYTjgII8L3A+CiAgPHAgY2xhc3M9Im11dGVkIj7mjqHngrnluK/vvJo4MOOAnDEwMCDlvLfmsJcgLyA2NeOAnDc5IOOChOOChOW8t+awlyAvIDQ144CcNjQg5Lit56uLIC8gMzDjgJw0NCDjgoTjgoTlvLHmsJcgLyAw44CcMjkg5byx5rCXPC9wPgogIDxkaXYgaWQ9ImFuYWx5c2lzRXYiIGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+aqCDku4rml6Xjga7lhKrlhYjjgqLjgq/jgrfjg6fjg7M8L2gzPgogIDxkaXYgaWQ9InByaW9yaXR5QWN0aW9ucyI+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOOAgeWEquWFiOOBl+OBpueiuuiqjeOBmeOCi+mKmOafhOOCkuiHquWLleihqOekuuOBl+OBvuOBmeOAgjwvcD4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIHBvcnRmb2xpbyI+CiAgPGgzPvCfp60g44Od44O844OI44OV44Kp44Oq44Kq5YWo5L2TPC9oMz4KICA8ZGl2IGlkPSJwb3J0Zm9saW9TdW1tYXJ5Ij4KICAgIDxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go6Ieq5YuV6ZuG6KiI44GX44G+44GZ44CCPC9wPgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5K8IOS/neacieagquODu+aQjeWIh+OCii/liKnnoro8L2gzPgogIDxkaXYgY2xhc3M9Im5vdGUiPgogICAg5pCN5YiH44KK44O75Yip56K644O744OI44Os44O844Oq44Oz44Kw44Gv5Y+C6ICD44Op44Kk44Oz44CC5L+d5pyJ5Yik5pat44Gv5a6f44OH44O844K/44Go6Kit5a6a44Op44Kk44Oz44Gu44Or44O844Or5Yik5a6a44Gn44GZ44CC55+t5Lit6ZW344Gv6YGO5Y6744Gu44Ot44O844Oq44Oz44Kw5a6f57i+5YiG5biD44KS57Wx6KiI5Y+C6ICD44Go44GX44Gm6KGo56S644GX44G+44GZ44CCCiAgPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6MTBweCI+CiAgICA8aW5wdXQgaWQ9ImhvbGRDb2RlIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPgogICAgPGlucHV0IGlkPSJob2xkQ29zdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiBwbGFjZWhvbGRlcj0i5Y+W5b6X5Y2Y5L6hIj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJob2xkQ29tcGFueU5hbWUiIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbjo2cHggMnB4IDAiPumKmOafhOWQje+8muKAlDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICA8aW5wdXQgaWQ9ImhvbGRTaGFyZXMiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IuagquaVsCI+CiAgICA8c2VsZWN0IGlkPSJmZWVNb2RlIj4KICAgICAgPG9wdGlvbiB2YWx1ZT0ibm9tdXJhX25ldCI+6YeO5p2R44Kq44Oz44Op44Kk44Oz5bCC55So5pSv5bqX44O754++54mpPC9vcHRpb24+CiAgICAgIDxvcHRpb24gdmFsdWU9Im5vbmUiPuaJi+aVsOaWmeOBquOBl++8iOavlOi8g+eUqO+8iTwvb3B0aW9uPgogICAgPC9zZWxlY3Q+CiAgPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KKICU8L3NwYW4+PGlucHV0IGlkPSJzdG9wUGN0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHZhbHVlPSI4Ij48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorogJTwvc3Bhbj48aW5wdXQgaWQ9InRha2VQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjE1Ij48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7jg4jjg6zjg7zjg6sgJTwvc3Bhbj48aW5wdXQgaWQ9InRyYWlsUGN0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHZhbHVlPSI3Ij48L2Rpdj4KICA8L2Rpdj4KICA8YnV0dG9uIG9uY2xpY2s9ImFkZEhvbGRpbmcoKSIgc3R5bGU9Im1hcmdpbi10b3A6MTBweCI+5a6f44OH44O844K/44Gn6KiI566X44GX44Gm5L+d5a2YPC9idXR0b24+CiAgPHAgY2xhc3M9Im11dGVkIj7ph47mnZHjg43jg4Pjg4jvvIbjgrPjg7zjg6vvvI/jgbvjgaPjgajjg4DjgqTjg6zjgq/jg4jjga7lm73lhoXnj77nianjg7vjgqrjg7Pjg6njgqTjg7Pms6jmlofjga7nqI7ovrzmiYvmlbDmlpnooajjgpLkvb/nlKjjgII8L3A+CiAgPGRpdiBpZD0iaG9sZGluZ3MiPjwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5GAIOOCpuOCqeODg+ODgeODquOCueODiDwvaDM+CiAgPGRpdiBjbGFzcz0icm93Ij4KICAgIDxpbnB1dCBpZD0id2F0Y2hDb2RlIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxidXR0b24gb25jbGljaz0iYWRkV2F0Y2goKSI+6L+95YqgPC9idXR0b24+CiAgPC9kaXY+CiAgPGRpdiBpZD0id2F0Y2hDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjRweCAycHggOHB4Ij7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaHMiPjwvZGl2Pgo8L2Rpdj4KCjxzY3JpcHQ+CmNvbnN0ICQ9eD0+ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoeCk7CmZ1bmN0aW9uIHZhbChpZCl7bGV0IHY9JChpZCkudmFsdWUudHJpbSgpO3JldHVybiB2PT09Jyc/bnVsbDpOdW1iZXIodil9CmZ1bmN0aW9uIGxvY2FsKGspe3RyeXtyZXR1cm4gSlNPTi5wYXJzZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrKXx8J1tdJyl9Y2F0Y2goZSl7cmV0dXJuW119fQpmdW5jdGlvbiBzYXZlKGssdil7bG9jYWxTdG9yYWdlLnNldEl0ZW0oayxKU09OLnN0cmluZ2lmeSh2KSl9CmZ1bmN0aW9uIGZtdCh2LGQ9Mil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKGQpfQpmdW5jdGlvbiB5ZW4odil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOifCpScrTWF0aC5yb3VuZChOdW1iZXIodikpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpfQpmdW5jdGlvbiBzdGF0ZUphKHMpewogIGlmKHM9PT0nc3Ryb25nX2J1bGxpc2gnKXJldHVybiAn5by35rCXJzsKICBpZihzPT09J2J1bGxpc2gnKXJldHVybiAn44KE44KE5by35rCXJzsKICBpZihzPT09J25ldXRyYWwnKXJldHVybiAn5Lit56uLJzsKICBpZihzPT09J2JlYXJpc2gnKXJldHVybiAn44KE44KE5byx5rCXJzsKICBpZihzPT09J3N0cm9uZ19iZWFyaXNoJylyZXR1cm4gJ+W8seawlyc7CiAgcmV0dXJuICfliKTlrprkv53nlZknOwp9CgpmdW5jdGlvbiBzdGF0ZUNsYXNzKHMpewogIGlmKHM9PT0nc3Ryb25nX2J1bGxpc2gnKXJldHVybiAnc3RhdGUtc3Ryb25nLWJ1bGwnOwogIGlmKHM9PT0nYnVsbGlzaCcpcmV0dXJuICdzdGF0ZS1idWxsJzsKICBpZihzPT09J25ldXRyYWwnKXJldHVybiAnc3RhdGUtbmV1dHJhbCc7CiAgaWYocz09PSdiZWFyaXNoJylyZXR1cm4gJ3N0YXRlLWJlYXInOwogIGlmKHM9PT0nc3Ryb25nX2JlYXJpc2gnKXJldHVybiAnc3RhdGUtc3Ryb25nLWJlYXInOwogIHJldHVybiAnc3RhdGUtbmV1dHJhbCc7Cn0KCmZ1bmN0aW9uIGZhY3RvckphKGtleSl7CiAgaWYoa2V5PT09J3RlY2huaWNhbCcpcmV0dXJuICfjg4bjgq/jg4vjgqvjg6snOwogIGlmKGtleT09PSdlYXJuaW5ncycpcmV0dXJuICfmsbrnrpcnOwogIGlmKGtleT09PSdzdXBwbHknKXJldHVybiAn6ZyA57WmcHJveHknOwogIGlmKGtleT09PSdwb2xpY3knKXJldHVybiAn5Zu9562WJzsKICByZXR1cm4ga2V5fHwn6KaB5ZugJzsKfQoKZnVuY3Rpb24gZHJpdmVyU2VudGVuY2Uoc2MpewogIGNvbnN0IHA9c2MmJnNjLnN0cm9uZ2VzdF9wb3NpdGl2ZTsKICBjb25zdCBuPXNjJiZzYy5zdHJvbmdlc3RfbmVnYXRpdmU7CiAgY29uc3Qgc2NvcmU9TnVtYmVyKHNjJiZzYy5zY29yZTEwMCk7CgogIGxldCBoZWFkPScnOwogIGlmKE51bWJlci5pc0Zpbml0ZShzY29yZSkpewogICAgaWYoc2NvcmU+PTgwKWhlYWQ9J+WPluW+l+a4iOOBv+imgeWboOOCkue3j+WQiOOBmeOCi+OBqOOAgeW8t+OBhOODl+ODqeOCueipleS+oeOBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj02NSloZWFkPSfjg5fjg6njgrnopoHlm6DjgYzlhKrli6LjgafjgIHjgoTjgoTlvLfmsJfjga7oqZXkvqHjgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49NDUpaGVhZD0n44OX44Op44K544Go44Oe44Kk44OK44K544GM5ouu5oqX44GX44CB5Lit56uL5ZyP44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTMwKWhlYWQ9J+ODnuOCpOODiuOCueimgeWboOOBruW9semfv+OBjOOChOOChOW8t+OBj+OAgeaFjumHjeWvhOOCiuOBp+OBmeOAgic7CiAgICBlbHNlIGhlYWQ9J+ODnuOCpOODiuOCueimgeWboOOBruW9semfv+OBjOWkp+OBjeOBj+OAgeW8seawl+WvhOOCiuOBp+OBmeOAgic7CiAgfQoKICBsZXQgdGFpbD1bXTsKICBpZihuKXRhaWwucHVzaCgn5pyA5aSn44Gu5oq844GX5LiL44GS6KaB5Zug44GvICcrZmFjdG9ySmEobi5rZXkpKycgJytzY29yZUxhYmVsKG4uc2NvcmUpKTsKICBpZihwKXRhaWwucHVzaCgn5pyA5aSn44Gu5oq844GX5LiK44GS6KaB5Zug44GvICcrZmFjdG9ySmEocC5rZXkpKycgJytzY29yZUxhYmVsKHAuc2NvcmUpKTsKICByZXR1cm4gaGVhZCsodGFpbC5sZW5ndGg/JyAnK3RhaWwuam9pbign44CCJykrJ+OAgic6JycpOwp9CgpmdW5jdGlvbiBkcml2ZXJCb3hIdG1sKHRpdGxlLGQsa2luZCl7CiAgaWYoIWQpcmV0dXJuIGA8ZGl2IGNsYXNzPSJkcml2ZXJib3giPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+6Kmy5b2T44Gq44GXPC9iPjwvZGl2PmA7CiAgY29uc3Qgc2lnbj1OdW1iZXIoZC5jb250cmlidXRpb24pPj0wPycrJzonJzsKICByZXR1cm4gYDxkaXYgY2xhc3M9ImRyaXZlcmJveCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPgogICAgPGI+JHtmYWN0b3JKYShkLmtleSl9ICR7c2NvcmVMYWJlbChkLnNjb3JlKX08L2I+CiAgICA8c21hbGwgY2xhc3M9Im11dGVkIj7lho3phY3liIblvozjga7ph43jgb8gJHtkLndlaWdodF9wY3R9JSAvIOWvhOS4jiAke3NpZ259JHtOdW1iZXIoZC5jb250cmlidXRpb24pLnRvRml4ZWQoMSl9PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiBjb250cmlidXRpb25DYXJkSHRtbChkKXsKICBpZighZClyZXR1cm4gJyc7CiAgY29uc3QgYz1OdW1iZXIoZC5jb250cmlidXRpb24pOwogIGNvbnN0IHNpZ249Yz4wPycrJzonJzsKICBjb25zdCBpbXBhY3Q9Yy8yOwogIGNvbnN0IGltcGFjdFNpZ249aW1wYWN0PjA/JysnOicnOwogIHJldHVybiBgPGRpdiBjbGFzcz0iY29udHJpYiI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7ZmFjdG9ySmEoZC5rZXkpfTwvc3Bhbj4KICAgIDxiPiR7c2lnbn0ke2MudG9GaXhlZCgxKX08L2I+CiAgICA8c21hbGw+5YaN6YWN5YiG5b6M6YeN44G/ICR7TnVtYmVyKGQud2VpZ2h0X3BjdCkudG9GaXhlZCgxKX0lPC9zbWFsbD4KICAgIDxzbWFsbD4xMDDngrnmj5vnrpfjgbjjga7lvbHpn78gJHtpbXBhY3RTaWdufSR7aW1wYWN0LnRvRml4ZWQoMSl954K5PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiByZW5kZXJDb250cmlidXRpb25zKHNjKXsKICBjb25zdCBib3g9JCgnY29udHJpYnV0aW9uQm94Jyk7CiAgY29uc3QgZ3JpZD0kKCdjb250cmlidXRpb25HcmlkJyk7CiAgaWYoIWJveHx8IWdyaWQpcmV0dXJuOwogIGNvbnN0IGRzPShzYyYmc2MuZHJpdmVycyl8fFtdOwogIGlmKCFkcy5sZW5ndGgpewogICAgYm94LnN0eWxlLmRpc3BsYXk9J25vbmUnOwogICAgZ3JpZC5pbm5lckhUTUw9Jyc7CiAgICByZXR1cm47CiAgfQogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgZ3JpZC5pbm5lckhUTUw9ZHMubWFwKGNvbnRyaWJ1dGlvbkNhcmRIdG1sKS5qb2luKCcnKTsKfQpmdW5jdGlvbiBwY3Qodil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDIpKyclJ30KZnVuY3Rpb24gc3RhdEZvcihoLGtleSl7cmV0dXJuIGgmJmguZm9yd2FyZF9zdGF0cyYmaC5mb3J3YXJkX3N0YXRzW2tleV0/aC5mb3J3YXJkX3N0YXRzW2tleV06bnVsbH0KCmZ1bmN0aW9uIGRhdGFBZ2VEYXlzKGRhdGVTdHIpewogIGlmKCFkYXRlU3RyKXJldHVybiBudWxsOwogIGNvbnN0IG09U3RyaW5nKGRhdGVTdHIpLm1hdGNoKC9eKFxkezR9KS0oXGR7Mn0pLShcZHsyfSkkLyk7CiAgaWYoIW0pcmV0dXJuIG51bGw7CiAgY29uc3QgZD1EYXRlLlVUQyhOdW1iZXIobVsxXSksTnVtYmVyKG1bMl0pLTEsTnVtYmVyKG1bM10pKTsKICBjb25zdCBub3c9bmV3IERhdGUoKTsKICBjb25zdCB0b2RheT1EYXRlLlVUQyhub3cuZ2V0RnVsbFllYXIoKSxub3cuZ2V0TW9udGgoKSxub3cuZ2V0RGF0ZSgpKTsKICByZXR1cm4gTWF0aC5tYXgoMCxNYXRoLmZsb29yKCh0b2RheS1kKS84NjQwMDAwMCkpOwp9CgpmdW5jdGlvbiBmcmVzaG5lc3NGb3IoZGF0ZVN0cil7CiAgY29uc3QgZGF5cz1kYXRhQWdlRGF5cyhkYXRlU3RyKTsKICBpZihkYXlzPT09bnVsbCl7CiAgICByZXR1cm4ge2xldmVsOid1bmtub3duJyxkYXlzOm51bGwsbGFiZWw6J+aXpeS7mOS4jeaYjicsY2xzOidmcmVzaC13YXJuJyxkZWNpc2lvbl9vazpmYWxzZX07CiAgfQogIGlmKGRheXM8PTQpewogICAgcmV0dXJuIHtsZXZlbDonZnJlc2gnLGRheXMsbGFiZWw6J+muruW6pk9LJyxjbHM6J2ZyZXNoLW9rJyxkZWNpc2lvbl9vazp0cnVlfTsKICB9CiAgaWYoZGF5czw9MTApewogICAgcmV0dXJuIHtsZXZlbDond2FybmluZycsZGF5cyxsYWJlbDon44KE44KE6YGF5bu2JyxjbHM6J2ZyZXNoLXdhcm4nLGRlY2lzaW9uX29rOnRydWV9OwogIH0KICByZXR1cm4ge2xldmVsOidzdGFsZScsZGF5cyxsYWJlbDon5Y+k44GE44OH44O844K/JyxjbHM6J2ZyZXNoLXN0YWxlJyxkZWNpc2lvbl9vazpmYWxzZX07Cn0KCmZ1bmN0aW9uIGZyZXNobmVzc1RleHQoZGF0ZVN0cil7CiAgY29uc3QgZj1mcmVzaG5lc3NGb3IoZGF0ZVN0cik7CiAgaWYoZi5kYXlzPT09bnVsbClyZXR1cm4gJ+acgOe1guODh+ODvOOCv+aXpeOCkueiuuiqjeOBp+OBjeOBvuOBm+OCk+OAgic7CiAgaWYoZi5sZXZlbD09PSdmcmVzaCcpcmV0dXJuIGDmnIDntYLjg4fjg7zjgr/ml6XjgYvjgokgJHtmLmRheXN95pel44CC6YCa5bi444Gu5Y+C6ICD5Yik5a6a44Gr5L2/55So44GX44G+44GZ44CCYDsKICBpZihmLmxldmVsPT09J3dhcm5pbmcnKXJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpeOAgumBheW7tuOBq+azqOaEj+OBl+OBpuOAgeWun+mam+OBruePvuWcqOWApOOCgueiuuiqjeOBl+OBpuOBj+OBoOOBleOBhOOAgmA7CiAgcmV0dXJuIGDmnIDntYLjg4fjg7zjgr/ml6XjgYvjgokgJHtmLmRheXN95pel6YGF44KM44CC5LuK5pel44Gu5aOy6LK35Yik5pat44Gr44Gv5Y+k44GE44Gf44KB44CB5L+d5pyJ5Yik5pat44Gv6Ieq5YuV44Gn5L+d55WZ44GX44G+44GZ44CCYDsKfQoKZnVuY3Rpb24gc2hvd0ZyZXNobmVzcyhkYXRlU3RyKXsKICBjb25zdCBib3g9JCgnZnJlc2huZXNzQm94Jyk7CiAgaWYoIWJveClyZXR1cm47CiAgY29uc3QgZj1mcmVzaG5lc3NGb3IoZGF0ZVN0cik7CiAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICBib3guY2xhc3NOYW1lPSdmcmVzaGJveCAnK2YuY2xzOwogICQoJ2ZyZXNobmVzc1RpdGxlJykudGV4dENvbnRlbnQ9CiAgICBmLmxldmVsPT09J2ZyZXNoJz8n4pyFIOODh+ODvOOCv+muruW6pk9LJzoKICAgIGYubGV2ZWw9PT0nd2FybmluZyc/J+KaoO+4jyDjg4fjg7zjgr/pgYXlu7bjgavms6jmhI8nOgogICAgZi5sZXZlbD09PSdzdGFsZSc/J/Cfm5Eg44OH44O844K/44GM5Y+k44GE44Gf44KB5LuK5pel44Gu5Yik5pat44Gv5L+d55WZJzoKICAgICfimqDvuI8g44OH44O844K/6a6u5bqm44KS56K66KqN44Gn44GN44G+44Gb44KTJzsKICAkKCdmcmVzaG5lc3NEZXRhaWwnKS50ZXh0Q29udGVudD1mcmVzaG5lc3NUZXh0KGRhdGVTdHIpOwp9CgpmdW5jdGlvbiBzaG93SGlzdG9yeUZyZXNobmVzcyhoaXN0b3J5RGF0ZSxwcmljZURhdGUpewogIGNvbnN0IGJveD0kKCdoaXN0b3J5RnJlc2huZXNzQm94Jyk7CiAgaWYoIWJveClyZXR1cm47CiAgaWYoIWhpc3RvcnlEYXRlKXsKICAgIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgICAkKCdoaXN0b3J5RnJlc2huZXNzVGV4dCcpLnRleHRDb250ZW50PScyMOaXpeODuzEyNuaXpeODuzI1MuaXpeOBruWIhuaekOWxpeattOOCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBn+OAguePvuWcqOWApOOBoOOBkeihqOekuuOBl+OBvuOBmeOAgic7CiAgICByZXR1cm47CiAgfQogIGNvbnN0IGhmPWZyZXNobmVzc0ZvcihoaXN0b3J5RGF0ZSk7CiAgaWYoaGYubGV2ZWw9PT0nZnJlc2gnKXsKICAgIGJveC5zdHlsZS5kaXNwbGF5PSdub25lJzsKICAgIHJldHVybjsKICB9CiAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICBjb25zdCBwZD1wcmljZURhdGV8fCfigJQnOwogICQoJ2hpc3RvcnlGcmVzaG5lc3NUZXh0JykudGV4dENvbnRlbnQ9CiAgICBg54++5Zyo5YCk44OH44O844K/5pelICR7cGR9IC8g5YiG5p6Q5bGl5q205pyA57WC5pelICR7aGlzdG9yeURhdGV944CC54++5Zyo5YCk44Gv5paw44GX44GP44Gm44KC44CB44OI44Os44Oz44OJ44O75pyf5b6F5YCk44O76ZyA57WmcHJveHnjga/pgYXlu7bjg4fjg7zjgr/jga7lj4LogIPlgKTjgafjgZnjgIJgOwp9CgpmdW5jdGlvbiBjb25maWRlbmNlRm9yKGgpewogIGNvbnN0IHZhbHM9W2gucmV0dXJuXzIwZCxoLnJldHVybl8xMjZkLGgucmV0dXJuXzI1MmRdLm1hcChOdW1iZXIpLmZpbHRlcihOdW1iZXIuaXNGaW5pdGUpOwogIGNvbnN0IHN0YXRzPVsnMjBkJywnMTI2ZCcsJzI1MmQnXS5tYXAoaz0+c3RhdEZvcihoLGspKS5maWx0ZXIocz0+cyYmcy5zdGF0dXM9PT0nb2snKTsKCiAgaWYoIU51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSl8fCFoLmFzb2YpcmV0dXJuIDA7CgogIGNvbnN0IGNvdmVyYWdlPXZhbHMubGVuZ3RoLzM7CiAgbGV0IGFncmVlbWVudD0wLjU7CiAgaWYodmFscy5sZW5ndGgpewogICAgY29uc3QgcG9zPXZhbHMuZmlsdGVyKHg9Png+MCkubGVuZ3RoOwogICAgY29uc3QgbmVnPXZhbHMuZmlsdGVyKHg9Png8MCkubGVuZ3RoOwogICAgYWdyZWVtZW50PU1hdGgubWF4KHBvcyxuZWcpL3ZhbHMubGVuZ3RoOwogIH0KICBjb25zdCBzdGF0Q292ZXJhZ2U9c3RhdHMubGVuZ3RoLzM7CiAgbGV0IHNjb3JlPShjb3ZlcmFnZSowLjQ1ICsgYWdyZWVtZW50KjAuMjUgKyBzdGF0Q292ZXJhZ2UqMC4zMCkqMTAwOwoKICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3IoaC5hc29mKTsKICBpZihmcmVzaC5sZXZlbD09PSd3YXJuaW5nJylzY29yZSo9MC42MDsKICBpZihmcmVzaC5sZXZlbD09PSdzdGFsZScpc2NvcmU9TWF0aC5taW4oc2NvcmUsMjUpOwogIGlmKGZyZXNoLmxldmVsPT09J3Vua25vd24nKXNjb3JlPU1hdGgubWluKHNjb3JlLDIwKTsKCiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBpZihoaXN0b3J5RnJlc2gubGV2ZWw9PT0nd2FybmluZycpc2NvcmUqPTAuNzU7CiAgaWYoaGlzdG9yeUZyZXNoLmxldmVsPT09J3N0YWxlJylzY29yZT1NYXRoLm1pbihzY29yZSwzNSk7CiAgaWYoaGlzdG9yeUZyZXNoLmxldmVsPT09J3Vua25vd24nKXNjb3JlPU1hdGgubWluKHNjb3JlLDI1KTsKCiAgcmV0dXJuIE1hdGgucm91bmQoc2NvcmUpOwp9CgpmdW5jdGlvbiBkZWNpc2lvbkZvcihoLGMpewogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IWguYXNvZil7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOiflrp/jg4fjg7zjgr/mnKrlj5blvpfjgILlj7PkuIrjga7jgIzmm7TmlrDjgI3jgadKLVF1YW50c+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBj+OBoOOBleOBhOOAgicsCiAgICAgIGNvbmZpZGVuY2U6MAogICAgfTsKICB9CgogIGNvbnN0IHIyMD1OdW1iZXIoaC5yZXR1cm5fMjBkKSwgcjEyNj1OdW1iZXIoaC5yZXR1cm5fMTI2ZCksIHIyNTI9TnVtYmVyKGgucmV0dXJuXzI1MmQpOwogIGNvbnN0IGNvbmZpZGVuY2U9Y29uZmlkZW5jZUZvcihoKTsKICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3IoaC5hc29mKTsKCiAgaWYoIWZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZnvvIjmoKrkvqHlj6TjgYTvvIknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246YCR7ZnJlc2huZXNzVGV4dChoLmFzb2YpfSDmkI3liIfjgorjg7vliKnnorrjg6njgqTjg7Pjgajjga7mr5TovIPjgoLlj4LogIPlgKTmibHjgYTjgafjgZnjgIJgLAogICAgICBjb25maWRlbmNlCiAgICB9OwogIH0KCiAgaWYoY3VyPD1jLnN0b3BQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifmkI3liIfjgormpJzoqI4nLGNsczonZC1zdG9wJyxyZWFzb246J+ioreWumuOBl+OBn+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+S7peS4iycsY29uZmlkZW5jZX07CiAgfQogIGlmKGN1cj49Yy50YWtlUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5Yip56K65qSc6KiOJyxjbHM6J2QtdGFrZScscmVhc29uOifoqK3lrprjgZfjgZ/liKnnorrlj4LogIPjg6njgqTjg7Pku6XkuIonLGNvbmZpZGVuY2V9OwogIH0KCiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBpZighaGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZnvvIjlsaXmrbTlj6TjgYTvvIknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246YOePvuWcqOWApOOBr+WPluW+l+OBp+OBjeOBpuOBhOOBvuOBmeOBjOOAgeWIhuaekOWxpeattOOBryAke2guaGlzdG9yeV9hc29mfHwn5LiN5piOJ33jgILlm7rlrprjga7mkI3liIfjgoov5Yip56K644Op44Kk44Oz44Gr44Gv5pyq5Yiw6YGU44Gn44GZ44GM44CB44OI44Os44Oz44OJ5Yik5pat44Gv5L+d55WZ44GX44G+44GZ44CCYCwKICAgICAgY29uZmlkZW5jZQogICAgfTsKICB9CgogIGlmKGN1cjw9Yy50cmFpbFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel6auY5YCk5Z+65rqW44Gu44OI44Os44O844Oq44Oz44Kw5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CgogIGxldCBwb3NpdGl2ZT0wLCBuZWdhdGl2ZT0wOwogIFtyMjAscjEyNixyMjUyXS5mb3JFYWNoKHg9PnsKICAgIGlmKE51bWJlci5pc0Zpbml0ZSh4KSl7CiAgICAgIGlmKHg+MClwb3NpdGl2ZSsrOwogICAgICBpZih4PDApbmVnYXRpdmUrKzsKICAgIH0KICB9KTsKCiAgaWYobmVnYXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon6K2m5oiSJyxjbHM6J2Qtd2F0Y2gnLHJlYXNvbjonMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7jgYbjgaHjg57jgqTjg4rjgrnlgr7lkJHjgYzlhKrli6InLGNvbmZpZGVuY2V9OwogIH0KICBpZihwb3NpdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntponLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOBp+OAgeikh+aVsOacn+mWk+OBruS+oeagvOODiOODrOODs+ODieOBjOODl+ODqeOCuScsY29uZmlkZW5jZX07CiAgfQogIHJldHVybiB7bGFiZWw6J+S/neaciee2mee2mu+8iOanmOWtkOimi++8iScsY2xzOidkLWhvbGQnLHJlYXNvbjon6Kit5a6a44Op44Kk44Oz5YaF44CC5pyf6ZaT5Yil44OI44Os44Oz44OJ44Gv5by35byx44GM5re35ZyoJyxjb25maWRlbmNlfTsKfQpmdW5jdGlvbiBldkh0bWwodGl0bGUscyl7CiAgaWYoIXN8fHMuc3RhdHVzIT09J29rJyl7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzbWFsbD7mqJnmnKwgJHtufeS7tjwvc21hbGw+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPgogICAgPGI+5bmz5Z2HICR7cGN0KHMubWVhbil9PC9iPgogICAgPHNtYWxsPuS4reWkruWApCAke3BjdChzLm1lZGlhbil9PC9zbWFsbD4KICAgIDxzbWFsbD7kuIrmmIfnjocgJHtwY3Qocy5wb3NpdGl2ZV9yYXRlKX08L3NtYWxsPgogICAgPHNtYWxsPlAxMOOAnFA5MCAke3BjdChzLnAxMCl9IOOAnCAke3BjdChzLnA5MCl9PC9zbWFsbD4KICAgIDxzbWFsbD7mqJnmnKwgJHtzLm595Lu2PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgoKZnVuY3Rpb24gZGlzdGFuY2VJbmZvKGN1cix0YXJnZXQsa2luZCl7CiAgY3VyPU51bWJlcihjdXIpOyB0YXJnZXQ9TnVtYmVyKHRhcmdldCk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFOdW1iZXIuaXNGaW5pdGUodGFyZ2V0KSlyZXR1cm4gJ+KAlCc7CiAgY29uc3QgZGlmZj0odGFyZ2V0L2N1ci0xKSoxMDA7CiAgaWYoa2luZD09PSdzdG9wJyl7CiAgICBpZihkaWZmPj0wKXJldHVybiAn44Op44Kk44Oz5Yiw6YGU5riI44G/JzsKICAgIHJldHVybiBNYXRoLmFicyhkaWZmKS50b0ZpeGVkKDIpKyclIOS4iyc7CiAgfQogIGlmKGtpbmQ9PT0ndGFrZScpewogICAgaWYoZGlmZjw9MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gZGlmZi50b0ZpeGVkKDIpKyclIOS4iic7CiAgfQogIHJldHVybiAoZGlmZj49MD8nKyc6JycpK2RpZmYudG9GaXhlZCgyKSsnJSc7Cn0KCmZ1bmN0aW9uIHByaWNlUmFuZ2UoY3VyLHMpewogIGN1cj1OdW1iZXIoY3VyKTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IXN8fHMuc3RhdHVzIT09J29rJylyZXR1cm4gbnVsbDsKICByZXR1cm4gewogICAgbG93OmN1ciooMStOdW1iZXIocy5wMTApLzEwMCksCiAgICBoaWdoOmN1ciooMStOdW1iZXIocy5wOTApLzEwMCksCiAgICBtZWRpYW46Y3VyKigxK051bWJlcihzLm1lZGlhbikvMTAwKQogIH07Cn0KCmZ1bmN0aW9uIHJhbmdlSHRtbCh0aXRsZSxjdXIscyl7CiAgY29uc3Qgcj1wcmljZVJhbmdlKGN1cixzKTsKICBpZighcil7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5qiZ5pysICR7bn3ku7Y8L3NwYW4+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJyYW5nZWJveCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9IFAxMOOAnFA5MDwvc3Bhbj4KICAgIDxiPiR7eWVuKHIubG93KX0g44CcICR7eWVuKHIuaGlnaCl9PC9iPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7kuK3lpK7lgKTmj5vnrpcgJHt5ZW4oci5tZWRpYW4pfTwvc3Bhbj4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiBhY3Rpb25UZXh0KGgsYyxkKXsKICBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmSd8fGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJylyZXR1cm4gJ+ODh+ODvOOCv+OBjOWPpOOBhOOBn+OCgeS7iuaXpeOBruWIpOaWreOBr+S/neeVmeOAguiovOWIuOS8muekvuOBquOBqeOBp+Wun+mam+OBruePvuWcqOWApOOCkueiuuiqjeOBl+OBpuOBi+OCieWIpOaWreOAgic7CiAgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjlsaXmrbTlj6TjgYTvvIknKXJldHVybiAn54++5Zyo5YCk44Gv56K66KqN5riI44G/44CC5Zu65a6a44Gu5pCN5YiH44KKL+WIqeeiuuODqeOCpOODs+OBoOOBkeeiuuiqjeOBl+OAgeODiOODrOODs+ODieWIpOaWreOBr+WxpeattOabtOaWsOOBvuOBp+S/neeVmeOAgic7CiAgaWYoZC5sYWJlbD09PSfmkI3liIfjgormpJzoqI4nKXJldHVybiAn5pCN5YiH44KK5Y+C6ICD44Op44Kk44Oz44KS5LiL5Zue44Gj44Gm44GE44G+44GZ44CC5a6f6Zqb44Gu54++5Zyo5YCk44Go5rOo5paH5p2h5Lu244KS56K66KqN44GX44Gm44CB57iu5bCP44O75pKk6YCA44KS5qSc6KiO44CCJzsKICBpZihkLmxhYmVsPT09J+WIqeeiuuaknOiojicpcmV0dXJuICfliKnnorrlj4LogIPjg6njgqTjg7PjgavliLDpgZTjgZfjgabjgYTjgb7jgZnjgILlhajpg6jlo7LljbTjgaDjgZHjgafjgarjgY/jgIHliIblibLliKnnorrjgoLlgJnoo5zjgIInOwogIGlmKGQubGFiZWw9PT0n6K2m5oiSJylyZXR1cm4gJ+itpuaIkuOCvuODvOODs+OAguODiOODrOODvOODquODs+OCsOODqeOCpOODs+OBqOS4reefreacn+OBruWApOWLleOBjeOCkuWEquWFiOOBl+OBpueiuuiqjeOAgic7CiAgcmV0dXJuICfoqK3lrprjg6njgqTjg7PlhoXjgILkv53mnInntpnntprlgJnoo5zjgafjgZnjgYzjgIHnhKHmlpnjg4fjg7zjgr/jga/pgYXlu7bjgZnjgovjgZ/jgoHlrp/pmpvjga7nj77lnKjlgKTjgoLnorroqo3jgIInOwp9CgoKZnVuY3Rpb24gbm9tdXJhTmV0RmVlKGFtb3VudCl7CiAgYW1vdW50PU51bWJlcihhbW91bnR8fDApOwogIGlmKGFtb3VudDw9MClyZXR1cm4gMDsKICBpZihhbW91bnQ8PTEwMDAwMClyZXR1cm4gMTUyOwogIGlmKGFtb3VudDw9MzAwMDAwKXJldHVybiAzMzA7CiAgaWYoYW1vdW50PD01MDAwMDApcmV0dXJuIDUyNDsKICBpZihhbW91bnQ8PTEwMDAwMDApcmV0dXJuIDEwNDg7CiAgaWYoYW1vdW50PD0yMDAwMDAwKXJldHVybiAyMDk1OwogIGlmKGFtb3VudDw9MzAwMDAwMClyZXR1cm4gMzE0MzsKICBpZihhbW91bnQ8PTUwMDAwMDApcmV0dXJuIDUyMzg7CiAgaWYoYW1vdW50PD0xMDAwMDAwMClyZXR1cm4gMTA0NzY7CiAgaWYoYW1vdW50PD0yMDAwMDAwMClyZXR1cm4gMjA5NTI7CiAgaWYoYW1vdW50PD0zMDAwMDAwMClyZXR1cm4gMzE0Mjk7CiAgaWYoYW1vdW50PD01MDAwMDAwMClyZXR1cm4gNDE5MDU7CiAgcmV0dXJuIDc4NTcxOwp9CmZ1bmN0aW9uIGZlZUZvcihhbW91bnQsbW9kZSl7cmV0dXJuIG1vZGU9PT0nbm9tdXJhX25ldCc/bm9tdXJhTmV0RmVlKGFtb3VudCk6MH0KCmZ1bmN0aW9uIGNhbGNIb2xkaW5nKGgpewogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBjb25zdCBjb3N0PU51bWJlcihoLmNvc3QpOwogIGNvbnN0IHNoYXJlcz1OdW1iZXIoaC5zaGFyZXMpOwogIGNvbnN0IGJ1eVZhbHVlPWNvc3Qqc2hhcmVzOwogIGNvbnN0IGJ1eUZlZT1mZWVGb3IoYnV5VmFsdWUsaC5mZWVfbW9kZSk7CiAgY29uc3QgY3VycmVudFZhbHVlPWN1cipzaGFyZXM7CiAgY29uc3Qgc2VsbEZlZT1mZWVGb3IoY3VycmVudFZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGludmVzdGVkPWJ1eVZhbHVlK2J1eUZlZTsKICBjb25zdCBuZXROb3c9Y3VycmVudFZhbHVlLXNlbGxGZWUtaW52ZXN0ZWQ7CiAgY29uc3QgbmV0Tm93UGN0PWludmVzdGVkP25ldE5vdy9pbnZlc3RlZCoxMDA6bnVsbDsKCiAgY29uc3Qgc3RvcFByaWNlPWNvc3QqKDEtTnVtYmVyKGguc3RvcF9wY3QpLzEwMCk7CiAgY29uc3QgdGFrZVByaWNlPWNvc3QqKDErTnVtYmVyKGgudGFrZV9wY3QpLzEwMCk7CiAgY29uc3QgaGlnaDIwPU51bWJlcihoLmhpZ2hfMjBkfHxjdXIpOwogIGNvbnN0IHRyYWlsUHJpY2U9aGlnaDIwKigxLU51bWJlcihoLnRyYWlsX3BjdCkvMTAwKTsKCiAgY29uc3Qgc3RvcFZhbHVlPXN0b3BQcmljZSpzaGFyZXM7CiAgY29uc3QgdGFrZVZhbHVlPXRha2VQcmljZSpzaGFyZXM7CiAgY29uc3Qgc3RvcE5ldD1zdG9wVmFsdWUtZmVlRm9yKHN0b3BWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKICBjb25zdCB0YWtlTmV0PXRha2VWYWx1ZS1mZWVGb3IodGFrZVZhbHVlLGguZmVlX21vZGUpLWludmVzdGVkOwoKICByZXR1cm4ge2J1eVZhbHVlLGJ1eUZlZSxjdXJyZW50VmFsdWUsc2VsbEZlZSxpbnZlc3RlZCxuZXROb3csbmV0Tm93UGN0LHN0b3BQcmljZSx0YWtlUHJpY2UsdHJhaWxQcmljZSxzdG9wTmV0LHRha2VOZXR9Owp9CgoKY29uc3QgbmFtZVRpbWVycz17fTsKY29uc3QgbmFtZUNhY2hlPXt9OwoKZnVuY3Rpb24gZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4PScnKXsKICBpZighdGFyZ2V0KXJldHVybjsKICBpZighaW5mb3x8IWluZm8ubmFtZSl7CiAgICB0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nOwogICAgcmV0dXJuOwogIH0KICBsZXQgZXh0cmFzPVtdOwogIGlmKGluZm8ubWFya2V0KWV4dHJhcy5wdXNoKGluZm8ubWFya2V0KTsKICBpZihpbmZvLnNlY3RvcjMzKWV4dHJhcy5wdXNoKGluZm8uc2VjdG9yMzMpOwogIHRhcmdldC5pbm5lckhUTUw9JzxiPicrcHJlZml4K2luZm8ubmFtZSsnPC9iPicrKGV4dHJhcy5sZW5ndGg/Jzxicj48c3BhbiBjbGFzcz0ibXV0ZWQiPicrZXh0cmFzLmpvaW4oJyAvICcpKyc8L3NwYW4+JzonJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldENvbXBhbnkoY29kZSl7CiAgY29uc3QgYz1TdHJpbmcoY29kZXx8JycpLnRyaW0oKTsKICBpZighYylyZXR1cm4gbnVsbDsKICBpZihuYW1lQ2FjaGVbY10pcmV0dXJuIG5hbWVDYWNoZVtjXTsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9zZWN1cml0eT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn6YqY5p+E5ZCN44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgbmFtZUNhY2hlW2NdPXg7CiAgcmV0dXJuIHg7Cn0KCmZ1bmN0aW9uIHNjaGVkdWxlQ29tcGFueUxvb2t1cChpbnB1dElkLHRhcmdldElkLHByZWZpeD0nJyl7CiAgY2xlYXJUaW1lb3V0KG5hbWVUaW1lcnNbaW5wdXRJZF0pOwogIGNvbnN0IGM9JChpbnB1dElkKS52YWx1ZS50cmltKCk7CiAgY29uc3QgdGFyZ2V0PSQodGFyZ2V0SWQpOwoKICBpZihjLmxlbmd0aDw0KXsKICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrigJQnOwogICAgcmV0dXJuOwogIH0KCiAgbmFtZVRpbWVyc1tpbnB1dElkXT1zZXRUaW1lb3V0KGFzeW5jKCk9PnsKICAgIHRyeXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD0n6YqY5p+E5ZCN44KS56K66KqN5Lit4oCmJzsKICAgICAgY29uc3QgaW5mbz1hd2FpdCBnZXRDb21wYW55KGMpOwogICAgICBkaXNwbGF5Q29tcGFueSh0YXJnZXQsaW5mbyxwcmVmaXgpOwogICAgfWNhdGNoKGUpewogICAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIH0KICB9LDQ1MCk7Cn0KCgoKZnVuY3Rpb24gcHJpb3JpdHlBY3Rpb25Gb3IoaCl7CiAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgY29uc3QgbmFtZT1oLmNvbXBhbnlfbmFtZXx8Jyc7CiAgY29uc3QgbGFiZWw9KGguY29kZXx8JycpKyhuYW1lPycgJytuYW1lOicnKTsKICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CgogIGlmKCF2YWxpZCl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NiwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+Wun+ODh+ODvOOCv+OCkuabtOaWsCcsCiAgICAgIGRldGFpbDon5pyA5paw5Y+W5b6X57WC5YCk44GM44GC44KK44G+44Gb44KT44CC44G+44Ga44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgIInLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogIGlmKCFmcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5OSwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+ODh+ODvOOCv+muruW6puOCkueiuuiqjScsCiAgICAgIGRldGFpbDpgJHtmcmVzaG5lc3NUZXh0KGguYXNvZil9IOWun+mam+OBruePvuWcqOWApOOCkuWFiOOBq+eiuuiqjeOAgmAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3Qgc3RvcERpc3Q9KGN1ci9jLnN0b3BQcmljZS0xKSoxMDA7CiAgY29uc3QgdGFrZURpc3Q9KGMudGFrZVByaWNlL2N1ci0xKSoxMDA7CiAgY29uc3QgdHJhaWxEaXN0PShjdXIvYy50cmFpbFByaWNlLTEpKjEwMDsKCiAgaWYoY3VyPD1jLnN0b3BQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZToxMDAsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOaQjeWIh+OCiuWPguiAgyAke3llbihjLnN0b3BQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihzdG9wRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NCwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+aQjeWIh+OCiuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7c3RvcERpc3QudG9GaXhlZCgyKX0lIOOBp+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5MCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+WIsOmBlCcsCiAgICAgIGRldGFpbDpg5pyA5paw5Y+W5b6X57WC5YCkICR7eWVuKGN1cil9IC8g5Yip56K65Y+C6ICDICR7eWVuKGMudGFrZVByaWNlKX1gLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKHRha2VEaXN0PD0zKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjg0LAogICAgICBjbHM6J3ByaW9yaXR5LXRha2UnLAogICAgICB0aXRsZTon5Yip56K644Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjgYLjgaggJHt0YWtlRGlzdC50b0ZpeGVkKDIpfSUg44Gn5Yip56K65Y+C6ICD44Op44Kk44OzYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGlmKCFoaXN0b3J5RnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6NzgsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+WIhuaekOWxpeattOOCkuabtOaWsOW+heOBoScsCiAgICAgIGRldGFpbDpg54++5Zyo5YCkICR7aC5hc29mfHwn4oCUJ30gLyDliIbmnpDlsaXmrbQgJHtoLmhpc3RvcnlfYXNvZnx8J+KAlCd944CC5Zu65a6a5L6h5qC844Op44Kk44Oz5Lul5aSW44Gu5Yik5pat44Gv5L+d55WZ44CCYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihkLmxhYmVsPT09J+itpuaIkicpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6ODAsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+itpuaIkuWIpOWumicsCiAgICAgIGRldGFpbDpkLnJlYXNvbiwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihOdW1iZXIuaXNGaW5pdGUodHJhaWxEaXN0KSYmdHJhaWxEaXN0PD0yLjUpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6NzYsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+ODiOODrOODvOODquODs+OCsOODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44OI44Os44O844Oq44Oz44Kw5Y+C6ICDICR7eWVuKGMudHJhaWxQcmljZSl9IOOBvuOBpyAke3RyYWlsRGlzdC50b0ZpeGVkKDIpfSVgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIHJldHVybiB7CiAgICBzY29yZTozMCwKICAgIGNsczoncHJpb3JpdHktZ29vZCcsCiAgICB0aXRsZTon6YCa5bi455uj6KaWJywKICAgIGRldGFpbDpkLnJlYXNvbnx8J+ioreWumuODqeOCpOODs+WGhScsCiAgICBsYWJlbAogIH07Cn0KCmZ1bmN0aW9uIHJlbmRlclByaW9yaXR5QWN0aW9ucygpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgYm94PSQoJ3ByaW9yaXR5QWN0aW9ucycpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJldHVybjsKICB9CgogIGxldCBhY3Rpb25zPWEubWFwKHByaW9yaXR5QWN0aW9uRm9yKTsKCiAgLy8gQ29uY2VudHJhdGlvbiBhbGVydCAocG9ydGZvbGlvLWxldmVsKQogIGNvbnN0IHZhbGlkPWEuZmlsdGVyKGg9Pk51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZik7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw+MCl7CiAgICBsZXQgbWF4SG9sZGluZz1udWxsLCBtYXhWYWx1ZT0wOwogICAgdmFsaWQuZm9yRWFjaChoPT57CiAgICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgICAgaWYodj5tYXhWYWx1ZSl7bWF4VmFsdWU9djttYXhIb2xkaW5nPWh9CiAgICB9KTsKICAgIGNvbnN0IGNvbmNlbnRyYXRpb249bWF4VmFsdWUvdG90YWwqMTAwOwogICAgaWYoY29uY2VudHJhdGlvbj49NjAgJiYgbWF4SG9sZGluZyl7CiAgICAgIGFjdGlvbnMucHVzaCh7CiAgICAgICAgc2NvcmU6NzIsCiAgICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICAgIHRpdGxlOifpm4bkuK3luqbjgpLnorroqo0nLAogICAgICAgIGRldGFpbDpgJHttYXhIb2xkaW5nLmNvZGV9JHttYXhIb2xkaW5nLmNvbXBhbnlfbmFtZT8nICcrbWF4SG9sZGluZy5jb21wYW55X25hbWU6Jyd9IOOBjOODneODvOODiOODleOCqeODquOCquOBriAke2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKX0lYCwKICAgICAgICBsYWJlbDon44Od44O844OI44OV44Kp44Oq44KqJwogICAgICB9KTsKICAgIH0KICB9CgogIGFjdGlvbnMuc29ydCgoeCx5KT0+eS5zY29yZS14LnNjb3JlKTsKCiAgY29uc3QgaW1wb3J0YW50PWFjdGlvbnMuZmlsdGVyKHg9Pnguc2NvcmU+PTcwKTsKICBjb25zdCBzaG93bj0oaW1wb3J0YW50Lmxlbmd0aD9pbXBvcnRhbnQ6YWN0aW9ucykuc2xpY2UoMCw0KTsKCiAgYm94LmlubmVySFRNTD1gPGRpdiBjbGFzcz0icHJpb3JpdHktd3JhcCI+JHsKICAgIHNob3duLm1hcCgoeCxpKT0+YAogICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1pdGVtICR7eC5jbHN9Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1saW5lIj4KICAgICAgICAgIDxkaXY+CiAgICAgICAgICAgIDxzcGFuIGNsYXNzPSJwcmlvcml0eS1yYW5rIj5QUklPUklUWSAke2krMX08L3NwYW4+CiAgICAgICAgICAgIDxiPiR7eC50aXRsZX08L2I+CiAgICAgICAgICA8L2Rpdj4KICAgICAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWNvZGUiPiR7eC5sYWJlbH08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NXB4Ij4ke3guZGV0YWlsfTwvZGl2PgogICAgICA8L2Rpdj4KICAgIGApLmpvaW4oJycpCiAgfTwvZGl2PmAgKyAoCiAgICBpbXBvcnRhbnQubGVuZ3RoCiAgICAgID8gJzxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7igLvlhKrlhYjluqbjga/oqK3lrprjg6njgqTjg7PmjqXov5Hjg7vliKTlrprnirbmhYvjg7vjg4fjg7zjgr/mnInnhKHjg7vpm4bkuK3luqbjgYvjgonkvZzjgovnorroqo3poIbjgafjgZnjgILoh6rli5Xlo7LosrfmjIfnpLrjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+JwogICAgICA6ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+57eK5oCl5bqm44Gu6auY44GE6aCF55uu44Gv44GC44KK44G+44Gb44KT44CC6YCa5bi455uj6KaW44KS57aZ57aa44CCPC9wPicKICApOwp9CgpmdW5jdGlvbiBwb3J0Zm9saW9NZWFuRm9yKGEsa2V5KXsKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wKTsKICBjb25zdCB0b3RhbD12YWxpZC5yZWR1Y2UoKHMsaCk9PnMrTnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKSwwKTsKICBpZih0b3RhbDw9MClyZXR1cm4gbnVsbDsKCiAgbGV0IG51bT0wLCBkZW49MDsKICB2YWxpZC5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IHN0PXN0YXRGb3IoaCxrZXkpOwogICAgY29uc3Qgdj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApOwogICAgaWYoc3QmJnN0LnN0YXR1cz09PSdvaycmJk51bWJlci5pc0Zpbml0ZShOdW1iZXIoc3QubWVhbikpJiZ2PjApewogICAgICBudW0gKz0gdipOdW1iZXIoc3QubWVhbik7CiAgICAgIGRlbiArPSB2OwogICAgfQogIH0pOwogIHJldHVybiBkZW4+MD9udW0vZGVuOm51bGw7Cn0KCmZ1bmN0aW9uIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwb3J0Zm9saW9TdW1tYXJ5Jyk7CiAgaWYoIWJveClyZXR1cm47CgogIGlmKCFhLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOiHquWLlembhuioiOOBl+OBvuOBmeOAgjwvcD4nOwogICAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7CiAgICByZXR1cm47CiAgfQoKICBsZXQgdG90YWxDb3N0PTAsIHRvdGFsVmFsdWU9MCwgdG90YWxOZXQ9MDsKICBjb25zdCByb3dzPVtdOwogIGNvbnN0IGRlY2lzaW9ucz17aG9sZDowLHdhdGNoOjAsdGFrZTowLHN0b3A6MCxwZW5kaW5nOjB9OwoKICBhLmZvckVhY2goaD0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICAgIGNvbnN0IHNoYXJlcz1OdW1iZXIoaC5zaGFyZXN8fDApOwogICAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgICBjb25zdCBjdXJyZW50VmFsdWU9dmFsaWQ/Y3VyKnNoYXJlczowOwogICAgY29uc3QgaW52ZXN0ZWQ9TnVtYmVyKGguY29zdHx8MCkqc2hhcmVzK2MuYnV5RmVlOwoKICAgIHRvdGFsQ29zdCArPSBpbnZlc3RlZDsKCiAgICBpZih2YWxpZCl7CiAgICAgIHRvdGFsVmFsdWUgKz0gY3VycmVudFZhbHVlOwogICAgICB0b3RhbE5ldCArPSBjLm5ldE5vdzsKCiAgICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgICAgaWYoZC5sYWJlbD09PSfmkI3liIfjgormpJzoqI4nKWRlY2lzaW9ucy5zdG9wKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKWRlY2lzaW9ucy50YWtlKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSforabmiJInKWRlY2lzaW9ucy53YXRjaCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjmoKrkvqHlj6TjgYTvvIknfHxkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOWxpeattOWPpOOBhO+8iScpZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgZWxzZSBkZWNpc2lvbnMuaG9sZCsrOwoKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTpjdXJyZW50VmFsdWUsbmV0OmMubmV0Tm93fSk7CiAgICB9ZWxzZXsKICAgICAgZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTowLG5ldDpudWxsfSk7CiAgICB9CiAgfSk7CgogIGNvbnN0IG5ldFBjdD10b3RhbENvc3Q+MD90b3RhbE5ldC90b3RhbENvc3QqMTAwOm51bGw7CiAgY29uc3QgbWF4VmFsdWU9cm93cy5yZWR1Y2UoKG0scik9Pk1hdGgubWF4KG0sci52YWx1ZSksMCk7CiAgY29uc3QgY29uY2VudHJhdGlvbj10b3RhbFZhbHVlPjA/bWF4VmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKCiAgY29uc3QgbWVhbjIwPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjBkJyk7CiAgY29uc3QgbWVhbjEyNj1wb3J0Zm9saW9NZWFuRm9yKGEsJzEyNmQnKTsKICBjb25zdCBtZWFuMjUyPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjUyZCcpOwoKICBjb25zdCBzdGFsZUhvbGRpbmdzPWEuZmlsdGVyKGg9PnsKICAgIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGguYXNvZik7CiAgICByZXR1cm4gaC5hc29mICYmICFmLmRlY2lzaW9uX29rOwogIH0pOwogIGNvbnN0IHN0YWxlTm90aWNlPXN0YWxlSG9sZGluZ3MubGVuZ3RoCiAgICA/IGA8ZGl2IGNsYXNzPSJmcmVzaGJveCBmcmVzaC1zdGFsZSIgc3R5bGU9Im1hcmdpbi1ib3R0b206OXB4Ij48Yj7wn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvyAke3N0YWxlSG9sZGluZ3MubGVuZ3RofemKmOafhDwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+6KmV5L6h6aGN44O75pCN55uK44Gv5pyA5paw5Y+W5b6X57WC5YCk44OZ44O844K544Gu5Y+C6ICD5YCk44Gn44GZ44CC5LuK5pel44Gu5aOy6LK35Yik5pat44Gr44Gv5L2/44KP44Ga44CB5a6f6Zqb44Gu54++5Zyo5YCk44KS56K66KqN44GX44Gm44GP44Gg44GV44GE44CCPC9kaXY+PC9kaXY+YAogICAgOiAnJzsKCiAgY29uc3Qgc3RhbGVIaXN0b3J5PWEuZmlsdGVyKGg9PnsKICAgIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogICAgcmV0dXJuIChoLmhpc3RvcnlfYXNvZnx8aC5hc29mKSAmJiAhZi5kZWNpc2lvbl9vazsKICB9KTsKICBjb25zdCBoaXN0b3J5Tm90aWNlPXN0YWxlSGlzdG9yeS5sZW5ndGgKICAgID8gYDxkaXYgY2xhc3M9Imhpc3Rvcnl3YXJuIiBzdHlsZT0ibWFyZ2luLWJvdHRvbTo5cHgiPjxiPvCfk5og5YiG5p6Q5bGl5q2044GM5Y+k44GEICR7c3RhbGVIaXN0b3J5Lmxlbmd0aH3pipjmn4Q8L2I+PGRpdiBjbGFzcz0ibXV0ZWQiPuePvuWcqOWApOOBjOaWsOOBl+OBj+OBpuOCguOAgeefreS4remVt+acn+ODiOODrOODs+ODieODu+acn+W+heWApOODu+mcgOe1pnByb3h544Gv6YGF5bu25bGl5q2044Gu5Y+C6ICD5YCk44Gn44GZ44CCPC9kaXY+PC9kaXY+YAogICAgOiAnJzsKCiAgY29uc3QgYWxsb2NhdGlvbnM9cm93cwogICAgLmZpbHRlcihyPT5yLnZhbHVlPjApCiAgICAuc29ydCgoeCx5KT0+eS52YWx1ZS14LnZhbHVlKQogICAgLm1hcChyPT57CiAgICAgIGNvbnN0IHc9dG90YWxWYWx1ZT4wP3IudmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKICAgICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJhbGxvYyI+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPjxiPiR7ci5jb2RlfTwvYj4ke3IubmFtZT8nICcrci5uYW1lOicnfSAvICR7dy50b0ZpeGVkKDEpfSU8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJhbGxvY2JhciI+PHNwYW4gc3R5bGU9IndpZHRoOiR7TWF0aC5taW4oMTAwLHcpfSUiPjwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+YDsKICAgIH0pLmpvaW4oJycpOwoKICBib3guaW5uZXJIVE1MPWAKICAgICR7c3RhbGVOb3RpY2V9CiAgICAke2hpc3RvcnlOb3RpY2V9CiAgICA8ZGl2IGNsYXNzPSJwb3J0cm93Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+57eP5oqV6LOH6aGNPC9zcGFuPjxiPiR7eWVuKHRvdGFsQ29zdCl9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnoqZXkvqHpoY08L3NwYW4+PGI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsVmFsdWUpOifigJQnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+W5b6X57WC5YCk44OZ44O844K55pCN55uKPC9zcGFuPjxiIGNsYXNzPSIke3RvdGFsTmV0Pj0wPydwb3MnOiduZWcnfSI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsTmV0KTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke25ldFBjdD09PW51bGw/J+KAlCc6bmV0UGN0LnRvRml4ZWQoMikrJyUnfTwvc3Bhbj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pyA5aSn6YqY5p+E5q+U546HPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKSsnJSc6J+KAlCd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGRpdiBjbGFzcz0icG9ydHJvdyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5L+d5pyJ57aZ57aaPC9zcGFuPjxiPiR7ZGVjaXNpb25zLmhvbGR9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7orabmiJI8L3NwYW4+PGI+JHtkZWNpc2lvbnMud2F0Y2h9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrmpJzoqI48L3NwYW4+PGI+JHtkZWNpc2lvbnMudGFrZX08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCii/kv53nlZk8L3NwYW4+PGI+JHtkZWNpc2lvbnMuc3RvcCtkZWNpc2lvbnMucGVuZGluZ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn5OKIOipleS+oemhjeWKoOmHjeOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODszwvaDQ+CiAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nn63mnJ8yMOaXpTwvc3Bhbj48Yj4ke21lYW4yMD09PW51bGw/J+KAlCc6bWVhbjIwLnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS4reacnzEyNuaXpTwvc3Bhbj48Yj4ke21lYW4xMjY9PT1udWxsPyfigJQnOm1lYW4xMjYudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZW35pyfMjUy5pelPC9zcGFuPjxiPiR7bWVhbjI1Mj09PW51bGw/J+KAlCc6bWVhbjI1Mi50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+4oC75ZCE6YqY5p+E44Gu6YGO5Y675bmz5Z2H44Oq44K/44O844Oz44KS54++5Zyo44Gu6KmV5L6h6aGN44Gn5Yqg6YeN44GX44Gf5Y+C6ICD5YCk44Gn44GZ44CC55u46Zai44KS6ICD5oWu44GX44Gf5bCG5p2l5LqI5ris44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgoKICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA1cHgiPvCfk6Yg6YqY5p+E5qeL5oiQPC9oND4KICAgICR7YWxsb2NhdGlvbnN8fCc8cCBjbGFzcz0ibXV0ZWQiPuWun+ODh+ODvOOCv+acquWPluW+lzwvcD4nfQogIGA7CiAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7Cn0KZnVuY3Rpb24gcmVuZGVySG9sZGluZ3MoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGlmKCFhLmxlbmd0aCl7JCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7cmVuZGVyUG9ydGZvbGlvU3VtbWFyeSgpO3JldHVybn0KICAkKCdob2xkaW5ncycpLmlubmVySFRNTD1hLm1hcCgoaCxpKT0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IHZhbGlkUHJpY2U9TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCYmaC5hc29mOwogICAgY29uc3QgY2xzPXZhbGlkUHJpY2U/KGMubmV0Tm93Pj0wPydwb3MnOiduZWcnKTonJzsKICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKCiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImhvbGRpbmciPgogICAgICA8ZGl2IGNsYXNzPSJob2xkaW5nLWhlYWQiPgogICAgICAgIDxkaXY+CiAgICAgICAgICA8Yj4ke2guY29kZX08L2I+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+JHtoLmNvbXBhbnlfbmFtZXx8IiJ9PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0icm93Ij4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIHNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaEhvbGRpbmcoJHtpfSkiPuabtOaWsDwvYnV0dG9uPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gZGFuZ2VyIiBvbmNsaWNrPSJyZW1vdmVIb2xkaW5nKCR7aX0pIj7liYrpmaQ8L2J1dHRvbj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+54++5Zyo5YCk44OH44O844K/5pelICR7aC5hc29mfHwn4oCUJ30gLyDmnIDmlrDlj5blvpfntYLlgKQgJHt2YWxpZFByaWNlP3llbihoLmN1cnJlbnRfcHJpY2UpOifigJQnfSAvICR7aC5zaGFyZXN95qCqIC8g5Y+W5b6X5Y2Y5L6hICR7eWVuKGguY29zdCl9PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj7moKrkvqHjgr3jg7zjgrkgJHtoLnByaWNlX3NvdXJjZXx8J+KAlCd9IC8g5YiG5p6Q5bGl5q20ICR7aC5oaXN0b3J5X2Fzb2Z8fCfigJQnfSAke2guaGlzdG9yeV9zb3VyY2U/JygnK2guaGlzdG9yeV9zb3VyY2UrJyknOicnfTwvZGl2PgogICAgICAke3ZhbGlkUHJpY2U/YDxkaXYgY2xhc3M9ImZyZXNoYm94ICR7ZnJlc2huZXNzRm9yKGguYXNvZikuY2xzfSI+PGI+JHsKICAgICAgICBmcmVzaG5lc3NGb3IoaC5hc29mKS5sZXZlbD09PSdmcmVzaCc/J+KchSDprq7luqZPSyc6CiAgICAgICAgZnJlc2huZXNzRm9yKGguYXNvZikubGV2ZWw9PT0nd2FybmluZyc/J+KaoO+4jyDpgYXlu7bms6jmhI8nOgogICAgICAgICfwn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvycKICAgICAgfTwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+JHtmcmVzaG5lc3NUZXh0KGguYXNvZil9PC9kaXY+PC9kaXY+YDonJ30KCiAgICAgIDxkaXYgY2xhc3M9ImRlY2lzaW9uICR7ZC5jbHN9Ij4KICAgICAgICAke2QubGFiZWx9CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjRweCI+JHtkLnJlYXNvbn08L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMuc3RvcFByaWNlLCdzdG9wJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMudGFrZVByaWNlLCd0YWtlJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKTlrprkv6HpoLzluqY8L3NwYW4+CiAgICAgICAgICA8Yj4ke2QuY29uZmlkZW5jZX0lPC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0iZ2F1Z2UiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke2QuY29uZmlkZW5jZX0lIj48L3NwYW4+PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPuKAu+WIpOWumuS/oemgvOW6puOBr+OAgeODh+ODvOOCv+WFhei2s+W6puODu+acn+mWk+ODiOODrOODs+ODieOBruS4gOiHtOW6puODu+acn+W+heWApOe1seioiOOBruacieeEoeOBi+OCieS9nOOCi+WPguiAg+aMh+aomeOBp+OAgeeahOS4reeiuueOh+OBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICAgIDxkaXYgY2xhc3M9ImFjdGlvbmJveCI+CiAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7ku4rjganjgYbjgZnjgovvvJ88L3NwYW4+CiAgICAgICAgPGIgc3R5bGU9ImRpc3BsYXk6YmxvY2s7bWFyZ2luLXRvcDozcHgiPiR7YWN0aW9uVGV4dChoLGMsZCl9PC9iPgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7Y2xzfSI+JHt2YWxpZFByaWNlP3llbihjLm5ldE5vdyk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt2YWxpZFByaWNlP2ZtdChjLm5ldE5vd1BjdCkrJyUnOifigJQnfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6LK35LuY5omL5pWw5paZPC9zcGFuPjxiPiR7eWVuKGMuYnV5RmVlKX08L2I+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWjsuWNtOaJi+aVsOaWmSjku4opPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy5zZWxsRmVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnN0b3BQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMuc3RvcE5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy50YWtlUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnRha2VOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844Oq44Oz44Kw5Y+C6ICDPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy50cmFpbFByaWNlKTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemrmOWApOWfuua6ljwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn46vIOWun+e4vuODmeODvOOCueS+oeagvOODrOODs+OCuDwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke3JhbmdlSHRtbCgn55+t5pyfIDIw5pelJyxjdXIsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+S4reacnyAxMjbml6UnLGN1cixzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+mVt+acnyAyNTLml6UnLGN1cixzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TkCDlrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtldkh0bWwoJ+efreacnyAyMOaXpScsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+S4reacnyAxMjbml6UnLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke2V2SHRtbCgn6ZW35pyfIDI1MuaXpScsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KICAgICAgPHAgY2xhc3M9Im11dGVkIj7igLvkvqHmoLzjg6zjg7Pjgrjjg7vmnJ/lvoXlgKTjga/lsIbmnaXkuojmuKzjgafjga/jgarjgY/jgIHlj5blvpflj6/og73jgarpgY7ljrvmoKrkvqHjga7jg63jg7zjg6rjg7PjgrDliY3mlrnjg6rjgr/jg7zjg7PliIbluIPjgpLmnIDmlrDlj5blvpfntYLlgKTjgavlvZPjgabjga/jgoHjgZ/ntbHoqIjlj4LogIPjgafjgZnjgILmnJ/plpPjgYzph43jgarjgovmqJnmnKzjgpLlkKvjgb/jgb7jgZnjgII8L3A+CiAgICA8L2Rpdj5gOwogIH0pLmpvaW4oJycpOwogIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0UXVvdGUoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcXVvdGU/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHE9YXdhaXQgci5qc29uKCk7CiAgaWYocS5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcihxLnJlYXNvbnx8cS5lcnJvcnx8J+Wun+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiBxOwp9Cgphc3luYyBmdW5jdGlvbiBhZGRIb2xkaW5nKCl7CiAgY29uc3QgY29kZT0kKCdob2xkQ29kZScpLnZhbHVlLnRyaW0oKTsKICBjb25zdCBjb3N0PXZhbCgnaG9sZENvc3QnKSwgc2hhcmVzPXZhbCgnaG9sZFNoYXJlcycpOwogIGNvbnN0IHN0b3A9dmFsKCdzdG9wUGN0JyksIHRha2U9dmFsKCd0YWtlUGN0JyksIHRyYWlsPXZhbCgndHJhaWxQY3QnKTsKICBjb25zdCBmZWVNb2RlPSQoJ2ZlZU1vZGUnKS52YWx1ZTsKICBpZighY29kZXx8IWNvc3R8fCFzaGFyZXMpe2FsZXJ0KCfpipjmn4TjgrPjg7zjg4njg7vlj5blvpfljZjkvqHjg7vmoKrmlbDjgpLlhaXlipvjgZfjgabjga0nKTtyZXR1cm59CiAgY29uc3QgYnRuPWV2ZW50Py50YXJnZXQ7IGlmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnfQogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogICAgY29uc3QgaD17CiAgICAgIGNvZGUsCiAgICAgIGNvbXBhbnlfbmFtZToocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fCcnLAogICAgICBjb21wYW55X21hcmtldDoocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8JycsCiAgICAgIGNvbXBhbnlfc2VjdG9yMzM6KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8JycsCiAgICAgIGNvc3QsIHNoYXJlcywgZmVlX21vZGU6ZmVlTW9kZSwKICAgICAgc3RvcF9wY3Q6c3RvcD8/OCwgdGFrZV9wY3Q6dGFrZT8/MTUsIHRyYWlsX3BjdDp0cmFpbD8/NywKICAgICAgY3VycmVudF9wcmljZTpzLmxhc3RfY2xvc2UsIGhpZ2hfMjBkOnMuaGlnaF8yMGQsIGxvd18yMGQ6cy5sb3dfMjBkLAogICAgICByZXR1cm5fMjBkOnMucmV0dXJuXzIwZCwgcmV0dXJuXzEyNmQ6cy5yZXR1cm5fMTI2ZCwgcmV0dXJuXzI1MmQ6cy5yZXR1cm5fMjUyZCwKICAgICAgZm9yd2FyZF9zdGF0czpzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSwKICAgICAgYXNvZjpzLmxhc3RfZGF0ZSwKICAgICAgaGlzdG9yeV9hc29mOnMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlLAogICAgICBwcmljZV9zb3VyY2U6cy5wcmljZV9zb3VyY2V8fHEuc291cmNlfHwnJywKICAgICAgaGlzdG9yeV9zb3VyY2U6cy5oaXN0b3J5X3NvdXJjZXx8JycsCiAgICAgIHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpCiAgICB9OwogICAgY29uc3QgaWR4PWEuZmluZEluZGV4KHg9PnguY29kZT09PWNvZGUpOwogICAgaWYoaWR4Pj0wKWFbaWR4XT1oOyBlbHNlIGEucHVzaChoKTsKICAgIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICAgIHJlbmRlckhvbGRpbmdzKCk7CiAgfWNhdGNoKGUpe2FsZXJ0KCflj5blvpfjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KICBmaW5hbGx5e2lmKGJ0bil7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn6KiI566X44GX44Gm5L+d5a2YJ319Cn0KCmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hIb2xkaW5nKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyksIGg9YVtpXTsgaWYoIWgpcmV0dXJuOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoaC5jb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgIGguY29tcGFueV9uYW1lPShxLmNvbXBhbnkmJnEuY29tcGFueS5uYW1lKXx8aC5jb21wYW55X25hbWV8fCcnOwogICAgaC5jb21wYW55X21hcmtldD0ocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8aC5jb21wYW55X21hcmtldHx8Jyc7CiAgICBoLmNvbXBhbnlfc2VjdG9yMzM9KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8aC5jb21wYW55X3NlY3RvcjMzfHwnJzsKICAgIGguY3VycmVudF9wcmljZT1zLmxhc3RfY2xvc2U7IGguaGlnaF8yMGQ9cy5oaWdoXzIwZDsgaC5sb3dfMjBkPXMubG93XzIwZDsKICAgIGgucmV0dXJuXzIwZD1zLnJldHVybl8yMGQ7IGgucmV0dXJuXzEyNmQ9cy5yZXR1cm5fMTI2ZDsgaC5yZXR1cm5fMjUyZD1zLnJldHVybl8yNTJkOwogICAgaC5mb3J3YXJkX3N0YXRzPXMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9OwogICAgaC5hc29mPXMubGFzdF9kYXRlOwogICAgaC5oaXN0b3J5X2Fzb2Y9cy5oaXN0b3J5X2xhc3RfZGF0ZXx8cy5sYXN0X2RhdGU7CiAgICBoLnByaWNlX3NvdXJjZT1zLnByaWNlX3NvdXJjZXx8cS5zb3VyY2V8fCcnOwogICAgaC5oaXN0b3J5X3NvdXJjZT1zLmhpc3Rvcnlfc291cmNlfHwnJzsKICAgIGgudXBkYXRlZF9hdD1uZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCk7CiAgICBhW2ldPWg7IHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsgcmVuZGVySG9sZGluZ3MoKTsKICB9Y2F0Y2goZSl7YWxlcnQoJ+abtOaWsOOCqOODqeODvDogJytlLm1lc3NhZ2UpfQp9CmZ1bmN0aW9uIHJlbW92ZUhvbGRpbmcoaSl7Y29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTthLnNwbGljZShpLDEpO3NhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTtyZW5kZXJIb2xkaW5ncygpfQoKZnVuY3Rpb24gcmVuZGVyV2F0Y2goKXsKICAkKCd3YXRjaHMnKS5pbm5lckhUTUw9bG9jYWwoJ2ZyZWVfd2F0Y2gnKS5tYXAoeD0+ewogICAgaWYodHlwZW9mIHg9PT0nc3RyaW5nJylyZXR1cm4gYDxkaXYgY2xhc3M9ImJhZGdlIj4ke3h9PC9kaXY+YDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYmFkZ2UiPjxiPiR7eC5jb2RlfTwvYj4ke3gubmFtZT8nICcreC5uYW1lOicnfTwvZGl2PmA7CiAgfSkuam9pbignJyl8fCc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nOwp9CmFzeW5jIGZ1bmN0aW9uIGFkZFdhdGNoKCl7CiAgbGV0IGM9JCgnd2F0Y2hDb2RlJykudmFsdWUudHJpbSgpOyBpZighYylyZXR1cm47CiAgbGV0IGluZm89bnVsbDsKICB0cnl7aW5mbz1hd2FpdCBnZXRDb21wYW55KGMpfWNhdGNoKGUpe30KICBsZXQgYT1sb2NhbCgnZnJlZV93YXRjaCcpOwogIGNvbnN0IGV4aXN0cz1hLnNvbWUoeD0+KHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpPT09Yyk7CiAgaWYoIWV4aXN0cylhLnB1c2goe2NvZGU6YyxuYW1lOmluZm8mJmluZm8ubmFtZT9pbmZvLm5hbWU6Jyd9KTsKICBzYXZlKCdmcmVlX3dhdGNoJyxhKTsKICByZW5kZXJXYXRjaCgpOwp9CmZ1bmN0aW9uIHVwZGF0ZUthYnV0YW4oKXtsZXQgYz0kKCdjb2RlJykudmFsdWUudHJpbSgpOyQoJ2thYnV0YW4nKS5ocmVmPWM/J2h0dHBzOi8va2FidXRhbi5qcC9zdG9jay8/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKTonaHR0cHM6Ly9rYWJ1dGFuLmpwLyd9CiQoJ2NvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9Pnt1cGRhdGVLYWJ1dGFuKCk7c2NoZWR1bGVDb21wYW55TG9va3VwKCdjb2RlJywnY29tcGFueU5hbWUnLCcnKX0pO3VwZGF0ZUthYnV0YW4oKTsKJCgnaG9sZENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnaG9sZENvZGUnLCdob2xkQ29tcGFueU5hbWUnLCcnKSk7CiQoJ3dhdGNoQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCd3YXRjaENvZGUnLCd3YXRjaENvbXBhbnlOYW1lJywnJykpOwoKCgphc3luYyBmdW5jdGlvbiBnZXRQb2xpY3koY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcG9saWN5P2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCflm73nrZbjg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4geC5wb2xpY3l8fHt9Owp9CgpmdW5jdGlvbiBwb2xpY3lTb3VyY2VTdGF0dXNKYShzKXsKICBpZihzPT09J3ZlcmlmaWVkX2xpdmUnKXJldHVybiAn5YWs5byP44Oa44O844K456K66KqN5riIJzsKICBpZihzPT09J3BhcnRpYWxfbGl2ZScpcmV0dXJuICflhazlvI/jg5rjg7zjgrjpg6jliIbnorroqo0nOwogIGlmKHM9PT0ndmVyaWZpZWRfcmVnaXN0cnknKXJldHVybiAn5pyA57WC56K66KqN5riI5YWs5byP44K944O844K5JzsKICByZXR1cm4gJ+eiuuiqjeS4jeWPryc7Cn0KCmZ1bmN0aW9uIHJlbmRlclBvbGljeVRoZW1lcyhwKXsKICBjb25zdCBib3g9JCgncG9saWN5VGhlbWVzJyk7CiAgaWYoIWJveClyZXR1cm47CiAgY29uc3QgdGhlbWVzPShwJiZwLm1hdGNoZWRfdGhlbWVzKXx8W107CiAgaWYoIXRoZW1lcy5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPGRpdiBjbGFzcz0icG9saWN5dGhlbWUiPjxiPumWoumAo+ODhuODvOODnuOBquOBlyAvIOWIpOWumuS/neeVmTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuacgOS9jumWoumAo+W6puOCkua6gOOBn+OBmeWFrOW8j+aUv+etluODhuODvOODnuOBjOOBguOCiuOBvuOBm+OCk+OAgjwvc3Bhbj48L2Rpdj4nOwogICAgcmV0dXJuOwogIH0KICBib3guaW5uZXJIVE1MPXRoZW1lcy5zbGljZSgwLDQpLm1hcCh0PT5gCiAgICA8ZGl2IGNsYXNzPSJwb2xpY3l0aGVtZSI+CiAgICAgIDxiPiR7dC5uYW1lfSAvIOmWoumAo+W6piAkeyhOdW1iZXIodC5yZWxldmFuY2UpKjEwMCkudG9GaXhlZCgwKX0lPC9iPgogICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuaUv+etluW8t+W6piAke051bWJlcih0LnBvbGljeV9zdHJlbmd0aCkudG9GaXhlZCgwKX0gLyDlr4TkuI4gJHtOdW1iZXIodC5jb250cmlidXRpb24pLnRvRml4ZWQoMSl9IC8gJHtwb2xpY3lTb3VyY2VTdGF0dXNKYSh0LnNvdXJjZV9zdGF0dXMpfTwvc3Bhbj48YnI+CiAgICAgIDxhIGhyZWY9IiR7dC51cmx9IiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+5YWs5byP44K944O844K5PC9hPgogICAgPC9kaXY+CiAgYCkuam9pbignJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldEZ1bmRhbWVudGFscyhjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9mdW5kYW1lbnRhbHM/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+axuueul+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiB4LmZ1bmRhbWVudGFsc3x8e307Cn0KCmZ1bmN0aW9uIHNjb3JlTGFiZWwodil7CiAgaWYodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKXJldHVybiAn4oCUJzsKICBjb25zdCBuPU51bWJlcih2KTsKICByZXR1cm4gKG4+MD8nKyc6JycpK24udG9GaXhlZCgxKTsKfQoKZnVuY3Rpb24gcGN0TWF5YmUodil7CiAgcmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDEpKyclJzsKfQoKYXN5bmMgZnVuY3Rpb24gYW5hbHl6ZSgpewogIGNvbnN0IGNvZGU9JCgnY29kZScpLnZhbHVlLnRyaW0oKTsKICBpZighY29kZSl7JCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+mKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOBrSc7cmV0dXJufQogIGNvbnN0IGJ0bj0kKCdhbmFseXplQnRuJyk7IGJ0bi5kaXNhYmxlZD10cnVlOyBidG4udGV4dENvbnRlbnQ9J+WPluW+l+S4reKApic7CiAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+eEoeaWmeagquS+oeOCueODiuODg+ODl+OCt+ODp+ODg+ODiO+8i0otUXVhbnRz5YiG5p6Q44OH44O844K/44KS5Y+W5b6X44GX44Gm44GE44G+44GZ4oCmJzsKCiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShjb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgICQoJ3ByaWNlJykudmFsdWU9cy5sYXN0X2Nsb3NlPT1udWxsPycnOmZtdChzLmxhc3RfY2xvc2UsMSk7CiAgICAkKCdyMjAnKS52YWx1ZT1mbXQocy5yZXR1cm5fMjBkKTskKCdyMTI2JykudmFsdWU9Zm10KHMucmV0dXJuXzEyNmQpOyQoJ3IyNTInKS52YWx1ZT1mbXQocy5yZXR1cm5fMjUyZCk7CiAgICAkKCdoaWdoMjAnKS50ZXh0Q29udGVudD1mbXQocy5oaWdoXzIwZCwxKTskKCdsb3cyMCcpLnRleHRDb250ZW50PWZtdChzLmxvd18yMGQsMSk7CiAgICAkKCd2b2wyMCcpLnRleHRDb250ZW50PXMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZD09bnVsbD8n4oCUJzpmbXQocy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkKSsnJSc7CgogICAgZGlzcGxheUNvbXBhbnkoJCgnY29tcGFueU5hbWUnKSxxLmNvbXBhbnl8fG51bGwsJycpOwogICAgY29uc3QgcHJpY2VTb3VyY2U9cy5wcmljZV9zb3VyY2V8fHEuc291cmNlfHwn5LiN5piOJzsKICAgIGNvbnN0IGhpc3RvcnlEYXRlPXMuaGlzdG9yeV9sYXN0X2RhdGV8fG51bGw7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9CiAgICAgICc8c3BhbiBjbGFzcz0ic291cmNlYmFkZ2UiPuePvuWcqOWApDwvc3Bhbj48YiBjbGFzcz0ib2siPicrcHJpY2VTb3VyY2UrJzwvYj4nKwogICAgICAnPGJyPuePvuWcqOWApOODh+ODvOOCv+aXpTogJysocy5sYXN0X2RhdGV8fCfigJQnKSsKICAgICAgKHMucHJpY2VfdGltZT8nICcrcy5wcmljZV90aW1lOicnKSsKICAgICAgJyAvIOacgOaWsOWPluW+l+WApDogJytmbXQocy5sYXN0X2Nsb3NlLDEpKwogICAgICAnPGJyPjxzcGFuIGNsYXNzPSJzb3VyY2ViYWRnZSI+5YiG5p6Q5bGl5q20PC9zcGFuPicrCiAgICAgIChzLmhpc3Rvcnlfc291cmNlfHwn5Y+W5b6X44Gq44GXJykrCiAgICAgICcgLyDmnIDntYLml6U6ICcrKGhpc3RvcnlEYXRlfHwn4oCUJykrCiAgICAgICcgLyDlsaXmrbTjgrXjg7Pjg5fjg6s6ICcrKHMuc2FtcGxlX2NvdW50Pz8wKSsn5Lu2JzsKICAgIHNob3dGcmVzaG5lc3Mocy5sYXN0X2RhdGUpOwogICAgc2hvd0hpc3RvcnlGcmVzaG5lc3MoaGlzdG9yeURhdGUscy5sYXN0X2RhdGUpOwogICAgJCgnYW5hbHlzaXNFdicpLmlubmVySFRNTD0KICAgICAgZXZIdG1sKCfnn63mnJ8yMOaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjBkJ10pKwogICAgICBldkh0bWwoJ+S4reacnzEyNuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMTI2ZCddKSsKICAgICAgZXZIdG1sKCfplbfmnJ8yNTLml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzI1MmQnXSk7CgogICAgY29uc3Qgc3VwcGx5PXMuc3VwcGx5X3Byb3h5fHx7fTsKICAgICQoJ3N1cHBseUF1dG8nKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKHN1cHBseS5zY29yZSk7CiAgICAkKCdzdXBwbHlEZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgJzXml6UvMjDml6Xlh7rmnaXpq5ggJysoc3VwcGx5LnZvbHVtZV9yYXRpb181XzIwPT1udWxsPyfigJQnOk51bWJlcihzdXBwbHkudm9sdW1lX3JhdGlvXzVfMjApLnRvRml4ZWQoMikrJ+WAjScpOwoKICAgIGxldCBmdW5kYW1lbnRhbHM9e307CiAgICB0cnl7CiAgICAgIGZ1bmRhbWVudGFscz1hd2FpdCBnZXRGdW5kYW1lbnRhbHMoY29kZSk7CiAgICAgICQoJ2Vhcm5BdXRvJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChmdW5kYW1lbnRhbHMuc2NvcmUpOwogICAgICBjb25zdCBtPWZ1bmRhbWVudGFscy5tZXRyaWNzfHx7fTsKICAgICAgJCgnZWFybkRldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgICfplovnpLogJysoKGZ1bmRhbWVudGFscy5sYXRlc3QmJmZ1bmRhbWVudGFscy5sYXRlc3QuZGF0ZSl8fCfigJQnKSsKICAgICAgICAnIC8g5aOy5LiKICcrcGN0TWF5YmUobS5zYWxlc19ncm93dGhfcGN0KSsKICAgICAgICAnIC8g5Za25qWt55uKICcrcGN0TWF5YmUobS5vcF9ncm93dGhfcGN0KTsKICAgIH1jYXRjaChmZSl7CiAgICAgICQoJ2Vhcm5BdXRvJykudGV4dENvbnRlbnQ9J+S4jeaYjic7CiAgICAgICQoJ2Vhcm5EZXRhaWwnKS50ZXh0Q29udGVudD0n44GT44Gu44OX44Op44OzL+mKmOafhOOBp+OBr+WPluW+l+OBp+OBjeOBquOBhOWPr+iDveaAp+OBguOCiic7CiAgICAgIGZ1bmRhbWVudGFscz17c2NvcmU6bnVsbH07CiAgICB9CgogICAgbGV0IGF1dG9Qb2xpY3k9e3Njb3JlOm51bGwsbWF0Y2hlZF90aGVtZXM6W119OwogICAgdHJ5ewogICAgICBhdXRvUG9saWN5PWF3YWl0IGdldFBvbGljeShjb2RlKTsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD1hdXRvUG9saWN5LnNjb3JlPT1udWxsPyfkuI3mmI4nOnNjb3JlTGFiZWwoYXV0b1BvbGljeS5zY29yZSk7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgIGF1dG9Qb2xpY3kuc2NvcmU9PW51bGwKICAgICAgICAgID8gJ+mWoumAo+OBmeOCi+WFrOW8j+aUv+etluODhuODvOODnuOBquOBlycKICAgICAgICAgIDogJ+iHquWLleWbveetlnByb3h5IC8g5L+h6aC85bqmICcrKGF1dG9Qb2xpY3kuY29uZmlkZW5jZV9wY3Q/PyfigJQnKSsnJSAvICcrKChhdXRvUG9saWN5Lm1hdGNoZWRfdGhlbWVzfHxbXSkubGVuZ3RoKSsn44OG44O844OeJzsKICAgICAgcmVuZGVyUG9saWN5VGhlbWVzKGF1dG9Qb2xpY3kpOwogICAgfWNhdGNoKHBlKXsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD0n5LiN5piOJzsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9J+WFrOW8j+aUv+etluOCveODvOOCueWPluW+l+OCqOODqeODvCc7CiAgICAgICQoJ3BvbGljeVRoZW1lcycpLmlubmVySFRNTD0nJzsKICAgICAgYXV0b1BvbGljeT17c2NvcmU6bnVsbCxtYXRjaGVkX3RoZW1lczpbXX07CiAgICB9CgogICAgY29uc3QgbWFudWFsUG9saWN5PXZhbCgncG9saWN5Jyk7CiAgICBjb25zdCBwb2xpY3lTY29yZT1tYW51YWxQb2xpY3k9PT1udWxsP2F1dG9Qb2xpY3kuc2NvcmU6bWFudWFsUG9saWN5OwogICAgY29uc3QgcG9saWN5TW9kZT1tYW51YWxQb2xpY3k9PT1udWxsPydhdXRvJzonbWFudWFsJzsKICAgIGlmKG1hbnVhbFBvbGljeSE9PW51bGwpewogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PXNjb3JlTGFiZWwobWFudWFsUG9saWN5KTsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9J+aJi+WFpeWKm+OBp+iHquWLleWApOOCkuS4iuabuOOBjSc7CiAgICB9CgogICAgY29uc3QgZD17CiAgICAgIGNvZGUsCiAgICAgIHByaWNlOnMubGFzdF9jbG9zZSwKICAgICAgcmV0dXJuMjA6cy5yZXR1cm5fMjBkLAogICAgICByZXR1cm4xMjY6cy5yZXR1cm5fMTI2ZCwKICAgICAgcmV0dXJuMjUyOnMucmV0dXJuXzI1MmQsCiAgICAgIGVhcm5pbmdzX3Njb3JlOmZ1bmRhbWVudGFscy5zY29yZSwKICAgICAgcG9saWN5X3Njb3JlOnBvbGljeVNjb3JlLAogICAgICBwb2xpY3lfbW9kZTpwb2xpY3lNb2RlLAogICAgICBzdXBwbHlfc2NvcmU6c3VwcGx5LnNjb3JlCiAgICB9OwoKICAgIGNvbnN0IGFyPWF3YWl0IGZldGNoKCcvYXBpL2ZyZWUvYW5hbHl6ZScsewogICAgICBtZXRob2Q6J1BPU1QnLAogICAgICBoZWFkZXJzOnsnQ29udGVudC1UeXBlJzonYXBwbGljYXRpb24vanNvbid9LAogICAgICBib2R5OkpTT04uc3RyaW5naWZ5KGQpLAogICAgICBjYWNoZTonbm8tc3RvcmUnCiAgICB9KTsKICAgIGNvbnN0IHg9YXdhaXQgYXIuanNvbigpOwogICAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+WIhuaekOODh+ODvOOCv+OBjOS4jei2s+OBl+OBpuOBhOOBvuOBmScpOwoKICAgICQoJ3N0YXRlJykudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7CiAgICAkKCdwb3MnKS50ZXh0Q29udGVudD14LnNpZ25hbC5wb3NpdGl2ZV9jb3VudDsKICAgICQoJ25lZycpLnRleHRDb250ZW50PXguc2lnbmFsLm5lZ2F0aXZlX2NvdW50OwoKICAgIGNvbnN0IHNjPXguc2NvcmV8fHt9OwogICAgJCgnc2NvcmVIZXJvJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmUxMDAnKS50ZXh0Q29udGVudD1zYy5zY29yZTEwMD09bnVsbD8n4oCUJzpzYy5zY29yZTEwMCsnIC8gMTAwJzsKCiAgICBjb25zdCBwaWxsPSQoJ3Njb3JlU3RhdGVQaWxsJyk7CiAgICBwaWxsLmNsYXNzTmFtZT0nc3RhdGVwaWxsICcrc3RhdGVDbGFzcyh4LnNpZ25hbC5zdGF0ZSk7CiAgICBwaWxsLnRleHRDb250ZW50PXN0YXRlSmEoeC5zaWduYWwuc3RhdGUpOwoKICAgICQoJ2NvdmVyYWdlJykudGV4dENvbnRlbnQ9c2MuY292ZXJhZ2VfcGN0PT1udWxsPyfigJQnOnNjLmNvdmVyYWdlX3BjdCsnJSc7CiAgICAkKCdzY29yZUJyZWFrZG93bicpLnRleHRDb250ZW50PQogICAgICAn44OG44Kv44OL44Kr44OrICcrc2NvcmVMYWJlbChzYy50ZWNobmljYWwpKwogICAgICAnIC8g5rG6566XICcrc2NvcmVMYWJlbChzYy5lYXJuaW5ncykrCiAgICAgICcgLyDpnIDntaYgJytzY29yZUxhYmVsKHNjLnN1cHBseSkrCiAgICAgICcgLyDlm73nrZZwcm94eSAnK3Njb3JlTGFiZWwoc2MucG9saWN5KTsKCiAgICAkKCdzY29yZVJlYXNvbicpLnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgICQoJ3Njb3JlUmVhc29uVGV4dCcpLnRleHRDb250ZW50PWRyaXZlclNlbnRlbmNlKHNjKTsKICAgICQoJ2RyaXZlckdyaWQnKS5pbm5lckhUTUw9CiAgICAgIGRyaXZlckJveEh0bWwoJ+acgOWkp+OBruODl+ODqeOCueimgeWboCcsc2Muc3Ryb25nZXN0X3Bvc2l0aXZlLCdwb3NpdGl2ZScpKwogICAgICBkcml2ZXJCb3hIdG1sKCfmnIDlpKfjga7jg57jgqTjg4rjgrnopoHlm6AnLHNjLnN0cm9uZ2VzdF9uZWdhdGl2ZSwnbmVnYXRpdmUnKTsKCiAgICByZW5kZXJDb250cmlidXRpb25zKHNjKTsKCiAgICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3Iocy5sYXN0X2RhdGUpOwogICAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSk7CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0KICAgICAgIWZyZXNoLmRlY2lzaW9uX29rCiAgICAgICAgPyAn4pqg77iPIOePvuWcqOWApOODh+ODvOOCv+OBjOWPpOOBhOOBn+OCgeOAgeS7iuaXpeOBruWjsuiyt+WIpOaWreOBqOOBl+OBpuOBr+S9v+eUqOOBl+OBvuOBm+OCk+OAgicKICAgICAgICA6ICFoaXN0b3J5RnJlc2guZGVjaXNpb25fb2sKICAgICAgICAgID8gJ+KaoO+4jyDnj77lnKjlgKTjga/mlrDjgZfjgYTjg4fjg7zjgr/jgpLkvb/jgaPjgabjgYTjgb7jgZnjgYzjgIHnt4/lkIjngrnjg7vnn63kuK3plbfmnJ/jg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjga/pgYXlu7bjgZfjgZ/liIbmnpDlsaXmrbTjga7lj4LogIPlgKTjgafjgZnjgILlm7rlrprjga7mkI3liIfjgoov5Yip56K644Op44Kk44Oz56K66KqN44KS5YSq5YWI44GX44Gm44GP44Gg44GV44GE44CCJwogICAgICAgICAgOiAn54++5Zyo5YCk44Go5YiG5p6Q5bGl5q2044Gu6a6u5bqm44KS56K66KqN5riI44G/44CC54q25oWL6KGo56S644Gv57eP5ZCI54K544Gr6YCj5YuV44GX44G+44GZ44CCJzsKICB9Y2F0Y2goZSl7CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n4pqg77iPICcrZS5tZXNzYWdlOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPSc8c3BhbiBjbGFzcz0iZXJyIj7lj5blvpfjgqjjg6njg7w6ICcrZS5tZXNzYWdlKyc8L3NwYW4+JzsKICB9ZmluYWxseXsKICAgIGJ0bi5kaXNhYmxlZD1mYWxzZTsKICAgIGJ0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn5YiG5p6QJzsKICB9Cn0KCnJlbmRlckhvbGRpbmdzKCk7cmVuZGVyV2F0Y2goKTsKaWYoJ3NlcnZpY2VXb3JrZXInIGluIG5hdmlnYXRvcil7bmF2aWdhdG9yLnNlcnZpY2VXb3JrZXIuZ2V0UmVnaXN0cmF0aW9ucygpLnRoZW4ocnM9PlByb21pc2UuYWxsKHJzLm1hcChyPT5yLnVucmVnaXN0ZXIoKSkpKS5jYXRjaCgoKT0+e30pfQppZignY2FjaGVzJyBpbiB3aW5kb3cpe2NhY2hlcy5rZXlzKCkudGhlbihrZXlzPT5Qcm9taXNlLmFsbChrZXlzLm1hcChrPT5jYWNoZXMuZGVsZXRlKGspKSkpLmNhdGNoKCgpPT57fSl9Cjwvc2NyaXB0Pgo8L21haW4+CjwvYm9keT4KPC9odG1sPg=="
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
