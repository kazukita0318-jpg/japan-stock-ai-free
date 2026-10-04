from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64, csv, io
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.15-FINMIND-HYBRID'
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




def finmind_symbol(code):
    code = str(code or "").strip()
    if code.isdigit() and len(code) >= 4:
        code = code[:4]
    return code + ".T"


def finmind_quote(code):
    symbol = finmind_symbol(code)
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=21)

    headers = {
        "User-Agent": "JapanStockAIFree/1.15",
    }
    token = os.getenv("FINMIND_TOKEN")
    if token:
        headers["Authorization"] = "Bearer " + token.strip()

    r = requests.get(
        "https://api.finmindtrade.com/api/v4/data",
        params={
            "dataset": "JapanStockPrice",
            "data_id": symbol,
            "start_date": start.isoformat(),
            "end_date": today.isoformat(),
        },
        headers=headers,
        timeout=12,
    )
    if r.status_code != 200:
        raise RuntimeError("FinMind HTTP " + str(r.status_code))

    payload = r.json()
    if int(payload.get("status") or 0) not in (0, 200):
        raise RuntimeError(
            "FinMind API: " + str(payload.get("msg") or "unknown error")
        )

    rows = payload.get("data") or []
    if not rows:
        raise RuntimeError("FinMind returned no JapanStockPrice rows")

    rows = sorted(
        rows,
        key=lambda x: str(x.get("date") or ""),
    )
    row = rows[-1]

    def pick(*names):
        for name in names:
            value = row.get(name)
            if value not in (None, "", "N/D"):
                try:
                    return float(value)
                except Exception:
                    pass
        return None

    close = pick("Close", "close", "Adj_Close")
    date = str(row.get("date") or "")
    if close is None or close <= 0 or not date:
        raise RuntimeError("FinMind latest row is incomplete")

    return {
        "symbol": symbol,
        "date": date[:10],
        "time": None,
        "open": pick("Open", "open"),
        "high": pick("High", "max", "high"),
        "low": pick("Low", "min", "low"),
        "close": close,
        "adj_close": pick("Adj_Close", "adj_close"),
        "volume": pick("Volume", "volume"),
        "source": "FinMind JapanStockPrice",
        "mode": "free_daily_eod",
        "registered_token": bool(token),
    }


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

    finmind = None
    finmind_error = None
    try:
        finmind = finmind_quote(code)
    except Exception as e:
        finmind_error = str(e)

    stooq = None
    stooq_error = None
    # Keep Stooq only as a secondary free fallback.
    if finmind is None:
        try:
            stooq = stooq_quote(code)
        except Exception as e:
            stooq_error = str(e)

    if jq_snap is None and finmind is None and stooq is None:
        return jsonify(
            status="error",
            code=code,
            reason=(
                "Free quote sources failed"
                + ("; FinMind: " + finmind_error if finmind_error else "")
                + ("; Stooq: " + stooq_error if stooq_error else "")
                + ("; J-Quants: " + jq_error if jq_error else "")
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

    candidate = None
    if finmind is not None:
        candidate = finmind
    elif stooq is not None:
        candidate = stooq

    use_free = False
    if candidate is not None:
        if not history_last_date:
            use_free = True
        else:
            use_free = str(candidate["date"]) >= str(history_last_date)

    if use_free:
        snap["last_close"] = candidate["close"]
        snap["last_date"] = candidate["date"]
        snap["price_source"] = candidate["source"]
        snap["price_time"] = candidate.get("time")
        snap["price_open"] = candidate.get("open")
        snap["price_high"] = candidate.get("high")
        snap["price_low"] = candidate.get("low")
        snap["price_volume"] = candidate.get("volume")
        quote_source = candidate["source"]
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
        finmind_quote=finmind,
        finmind_error=finmind_error,
        stooq_quote=stooq,
        stooq_error=stooq_error,
        jquants_error=jq_error,
    )


@APP.get("/api/mobile/free-price-test")
def mobile_free_price_test():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400

    results = {}

    try:
        results["finmind"] = {
            "status": "ok",
            "quote": finmind_quote(code),
        }
    except Exception as e:
        results["finmind"] = {
            "status": "error",
            "reason": str(e),
        }

    try:
        results["stooq"] = {
            "status": "ok",
            "quote": stooq_quote(code),
        }
    except Exception as e:
        results["stooq"] = {
            "status": "error",
            "reason": str(e),
        }

    ok = any(
        v.get("status") == "ok"
        for v in results.values()
    )

    return jsonify(
        status="ok" if ok else "error",
        code=code,
        sources=results,
    ), (200 if ok else 502)


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
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9CgoucG9saWN5dGhlbWUgYXtmb250LXNpemU6MTJweH0KLmNvbnRyaWJncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQouY29udHJpYntib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDtiYWNrZ3JvdW5kOiNmZmZ9Ci5jb250cmliIHNwYW57ZGlzcGxheTpibG9ja30KLmNvbnRyaWIgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4O21hcmdpbi10b3A6MnB4fQoKLmNvbnRyaWIgc21hbGx7ZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweDtjb2xvcjojNmI3MjgwO2xpbmUtaGVpZ2h0OjEuMzV9Ci5mcmVzaGJveHtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMXB4O21hcmdpbi10b3A6OXB4O2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLmZyZXNoLW9re2JhY2tncm91bmQ6I2VjZmRmNTtib3JkZXItY29sb3I6I2E3ZjNkMH0KLmZyZXNoLXdhcm57YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1jb2xvcjojZmRlNjhhfQouZnJlc2gtc3RhbGV7YmFja2dyb3VuZDojZmVmMmYyO2JvcmRlci1jb2xvcjojZmVjYWNhfQoKLmZyZXNoYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweH0KLnNvdXJjZWJhZGdle2Rpc3BsYXk6aW5saW5lLWJsb2NrO3BhZGRpbmc6NHB4IDhweDtib3JkZXItcmFkaXVzOjk5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTJweDtmb250LXdlaWdodDo4MDA7bWFyZ2luLXJpZ2h0OjRweH0KLmhpc3Rvcnl3YXJue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDttYXJnaW4tdG9wOjdweDtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyOjFweCBzb2xpZCAjZmRlNjhhfQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5jb250cmliZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+CjxtYWluPgo8ZGl2IGNsYXNzPSJ0b3AiPgogIDxoMT7wn5OIIOaXpeacrOagqkFJIEZSRUU8L2gxPgogIDxkaXYgY2xhc3M9InN1YiI+RmluTWluZOeEoeaWmeaXpei2syAvIEotUXVhbnRz5YiG5p6QIC8g6a6u5bqm44Ks44O844OJIC8g5L+d5pyJ5Yik5patPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfjq8g6YqY5p+E5YiG5p6QPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkIj4KICAgIDxpbnB1dCBpZD0iY29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIOS+iyA3MjAzIj4KICAgIDxpbnB1dCBpZD0icHJpY2UiIHBsYWNlaG9sZGVyPSLlj5blvpfntYLlgKQiIHJlYWRvbmx5PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbXBhbnlOYW1lIiBjbGFzcz0ic291cmNlIG11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBmeOCi+OBqOS8muekvuWQjeOCkuihqOekuuOBl+OBvuOBmTwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8YSBpZD0ia2FidXRhbiIgY2xhc3M9ImJ0biBzZWNvbmRhcnkiIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7moKrmjqLjgafnorroqo08L2E+CiAgICA8YnV0dG9uIGlkPSJhbmFseXplQnRuIiBvbmNsaWNrPSJhbmFseXplKCkiPuWun+ODh+ODvOOCv+OBp+WIhuaekDwvYnV0dG9uPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl44KM44Gm5oq844GZ44Go44CBSi1RdWFudHPjgYvjgonlj5blvpfjgafjgY3jgovlrp/jg4fjg7zjgr/jgpLoh6rli5XlhaXlipvjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBpZD0ic291cmNlQm94IiBjbGFzcz0ic291cmNlIG11dGVkIj7jg4fjg7zjgr/mnKrlj5blvpc8L2Rpdj4KICA8ZGl2IGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9Im1hcmdpbi10b3A6N3B4Ij4KICAgIDxiPvCfhpMg54Sh5paZ54++5Zyo5YCk44Gr44Gk44GE44GmPC9iPgogICAgPGRpdiBjbGFzcz0ibXV0ZWQiPkZpbk1pbmTjga/ml6XmnKzmoKrjga7ml6XotrPjg4fjg7zjgr/jgafjgZnjgILlj5blvJXkuK3jga7jg6rjgqLjg6vjgr/jgqTjg6DkvqHmoLzjgafjga/jgarjgY/jgIHlj5blvpfjgafjgY3jgovmnIDmlrDllrbmpa3ml6Xjga7ntYLlgKTjgpLnj77lnKjlgKTjgajjgZfjgablhKrlhYjjgZfjgb7jgZnjgII8L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzQm94IiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPGI+8J+TmiDliIbmnpDlsaXmrbTjga7prq7luqY8L2I+CiAgICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzVGV4dCIgY2xhc3M9Im11dGVkIj48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJmcmVzaG5lc3NCb3giIGNsYXNzPSJmcmVzaGJveCBmcmVzaC13YXJuIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxiIGlkPSJmcmVzaG5lc3NUaXRsZSI+44OH44O844K/6a6u5bqmPC9iPgogICAgPGRpdiBpZD0iZnJlc2huZXNzRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5OKIOagquS+oeODu+ODhuOCr+ODi+OCq+ODq+Wun+e4vjwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6aiw6JC9546HICU8L3NwYW4+PGlucHV0IGlkPSJyMjAiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjEyNuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjEyNiIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjUy5pelICU8L3NwYW4+PGlucHV0IGlkPSJyMjUyIiByZWFkb25seT48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemrmOWApDwvc3Bhbj48YiBpZD0iaGlnaDIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlronlgKQ8L3NwYW4+PGIgaWQ9ImxvdzIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlubTnjofjg5zjg6k8L3NwYW4+PGIgaWQ9InZvbDIwIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6kg5a6f44OH44O844K/6KaB5ZugPC9oMz4KICA8cCBjbGFzcz0ibXV0ZWQiPuaxuueul+OBr0otUXVhbnRz6LKh5YuZ44K144Oe44Oq44O844CB6ZyA57Wm44Gv5a6f5qCq5L6h44O75Ye65p2l6auYcHJveHnjgIHlm73nrZbjga/mlL/lupzlhazlvI/mlL/nrZbjgr3jg7zjgrnvvIvmpa3nqK4v56S+5ZCN44Gu6Zai6YCj5bqm44GL44KJ6Ieq5YuV5o6h54K544GX44G+44GZ44CCPC9wPgogIDxkaXYgY2xhc3M9ImZhY3RvcmdyaWQiPgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaxuueulzwvc3Bhbj48YiBpZD0iZWFybkF1dG8iPuKAlDwvYj48c21hbGwgaWQ9ImVhcm5EZXRhaWwiIGNsYXNzPSJtdXRlZCI+5pyq5Y+W5b6XPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7pnIDntaZwcm94eTwvc3Bhbj48YiBpZD0ic3VwcGx5QXV0byI+4oCUPC9iPjxzbWFsbCBpZD0ic3VwcGx5RGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWPluW+lzwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Zu9562WcHJveHk8L3NwYW4+PGIgaWQ9InBvbGljeVN0YXRlIj7igJQ8L2I+PHNtYWxsIGlkPSJwb2xpY3lEZXRhaWwiIGNsYXNzPSJtdXRlZCI+5YWs5byP5pS/562W44K944O844K556K66KqN5YmNPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg4fjg7zjgr/lhYXotrM8L3NwYW4+PGIgaWQ9ImNvdmVyYWdlIj7igJQ8L2I+PHNtYWxsIGNsYXNzPSJtdXRlZCI+57eP5ZCI5o6h54K544Gr5L2/44GI44Gf6YeN44G/PC9zbWFsbD48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJwb2xpY3lUaGVtZXMiIGNsYXNzPSJwb2xpY3l0aGVtZXMiPjwvZGl2PgogIDxkaXYgc3R5bGU9Im1hcmdpbi10b3A6OXB4Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Zu9562W44K544Kz44Ki5LiK5pu444GN77yI5Lu75oSP77yJIC0xMDDjgJwxMDA8L3NwYW4+CiAgICA8aW5wdXQgaWQ9InBvbGljeSIgaW5wdXRtb2RlPSJkZWNpbWFsIiBwbGFjZWhvbGRlcj0i56m65qyE44Gq44KJ6Ieq5YuV5Zu9562WcHJveHnjgpLkvb/nlKgiPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+4oC75Zu9562WcHJveHnjga/jgIHmlL/lupzlhazlvI/mlL/nrZbjgr3jg7zjgrnjgahKLVF1YW50c+OBrualreeoruODu+S8muekvuWQjeOBqOOBrumWoumAo+W6puOCkue1hOOBv+WQiOOCj+OBm+OBn+WPguiAg+WApOOBp+OBmeOAguODqeOCpOODlueiuuiqjeOBp+OBjeOBquOBhOWgtOWQiOOBr+acgOe1gueiuuiqjea4iOOBv+aDheWgseOCkuS9juS/oemgvOW6puOBp+S9v+eUqOOBl+OAgeOBneOBrueKtuaFi+OCgueUu+mdouOBq+aYjuekuuOBl+OBvuOBmeOAguijnOWKqemHkeaOoeaKnuOChOalree4vuaBqeaBteOBneOBruOCguOBruOCkuiovOaYjuOBmeOCi+aMh+aomeOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+noCDliIbmnpDntZDmnpw8L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nirbmhYs8L3NwYW4+PGIgaWQ9InN0YXRlIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OX44Op44K55qC55ougPC9zcGFuPjxiIGlkPSJwb3MiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg57jgqTjg4rjgrnmoLnmi6A8L3NwYW4+PGIgaWQ9Im5lZyI+4oCUPC9iPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InNjb3JlSGVybyIgY2xhc3M9InNjb3JlaGVybyIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+WQiOaOoeeCue+8iOWPluW+l+ODh+ODvOOCv+ODu+ODq+ODvOODq+ODmeODvOOCue+8iTwvc3Bhbj4KICAgIDxiIGlkPSJzY29yZTEwMCI+4oCUPC9iPgogICAgPHNwYW4gaWQ9InNjb3JlU3RhdGVQaWxsIiBjbGFzcz0ic3RhdGVwaWxsIHN0YXRlLW5ldXRyYWwiPuKAlDwvc3Bhbj4KICAgIDxkaXYgaWQ9InNjb3JlQnJlYWtkb3duIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0ic2NvcmVSZWFzb24iIGNsYXNzPSJzY29yZS1yZWFzb24iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPHN0cm9uZz7wn6etIOOBquOBnOOBk+OBrueCueaVsO+8nzwvc3Ryb25nPgogICAgPGRpdiBpZD0ic2NvcmVSZWFzb25UZXh0IiBjbGFzcz0ibXV0ZWQiPuKAlDwvZGl2PgogICAgPGRpdiBpZD0iZHJpdmVyR3JpZCIgY2xhc3M9ImRyaXZlcmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbnRyaWJ1dGlvbkJveCIgY2xhc3M9InNjb3JlLXJlYXNvbiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3Ryb25nPvCfp64g57eP5ZCI54K544G444Gu5a+E5LiOPC9zdHJvbmc+CiAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5Y+W5b6X44Gn44GN44Gf6KaB5Zug44Gg44GR44Gn6YeN44G/44KS5YaN6YWN5YiG44GX44Gf5b6M44CB5ZCE6KaB5Zug44GM57eP5ZCI6KmV5L6h44KS44Gp44KM44Gg44GR5oq844GX5LiK44GS77yP5oq844GX5LiL44GS44Gf44GL44KS6KGo56S644GX44G+44GZ44CCPC9kaXY+CiAgICA8ZGl2IGlkPSJjb250cmlidXRpb25HcmlkIiBjbGFzcz0iY29udHJpYmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxwIGlkPSJyZXN1bHQiIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44CM5a6f44OH44O844K/44Gn5YiG5p6Q44CN44KS5oq844GX44Gm44GP44Gg44GV44GE44CCPC9wPgogIDxwIGNsYXNzPSJtdXRlZCI+5o6h54K55biv77yaODDjgJwxMDAg5by35rCXIC8gNjXjgJw3OSDjgoTjgoTlvLfmsJcgLyA0NeOAnDY0IOS4reeriyAvIDMw44CcNDQg44KE44KE5byx5rCXIC8gMOOAnDI5IOW8seawlzwvcD4KICA8ZGl2IGlkPSJhbmFseXNpc0V2IiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfmqgg5LuK5pel44Gu5YSq5YWI44Ki44Kv44K344On44OzPC9oMz4KICA8ZGl2IGlkPSJwcmlvcml0eUFjdGlvbnMiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCBwb3J0Zm9saW8iPgogIDxoMz7wn6etIOODneODvOODiOODleOCqeODquOCquWFqOS9kzwvaDM+CiAgPGRpdiBpZD0icG9ydGZvbGlvU3VtbWFyeSI+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOiHquWLlembhuioiOOBl+OBvuOBmeOAgjwvcD4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+SvCDkv53mnInmoKrjg7vmkI3liIfjgoov5Yip56K6PC9oMz4KICA8ZGl2IGNsYXNzPSJub3RlIj4KICAgIOaQjeWIh+OCiuODu+WIqeeiuuODu+ODiOODrOODvOODquODs+OCsOOBr+WPguiAg+ODqeOCpOODs+OAguS/neacieWIpOaWreOBr+Wun+ODh+ODvOOCv+OBqOioreWumuODqeOCpOODs+OBruODq+ODvOODq+WIpOWumuOBp+OBmeOAguefreS4remVt+OBr+mBjuWOu+OBruODreODvOODquODs+OCsOWun+e4vuWIhuW4g+OCkue1seioiOWPguiAg+OBqOOBl+OBpuihqOekuuOBl+OBvuOBmeOAggogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPgogICAgPGlucHV0IGlkPSJob2xkQ29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxpbnB1dCBpZD0iaG9sZENvc3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgcGxhY2Vob2xkZXI9IuWPluW+l+WNmOS+oSI+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NnB4IDJweCAwIj7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGlucHV0IGlkPSJob2xkU2hhcmVzIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLmoKrmlbAiPgogICAgPHNlbGVjdCBpZD0iZmVlTW9kZSI+CiAgICAgIDxvcHRpb24gdmFsdWU9Im5vbXVyYV9uZXQiPumHjuadkeOCquODs+ODqeOCpOODs+WwgueUqOaUr+W6l+ODu+ePvueJqTwvb3B0aW9uPgogICAgICA8b3B0aW9uIHZhbHVlPSJub25lIj7miYvmlbDmlpnjgarjgZfvvIjmr5TovIPnlKjvvIk8L29wdGlvbj4KICAgIDwvc2VsZWN0PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiiAlPC9zcGFuPjxpbnB1dCBpZD0ic3RvcFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iOCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K6ICU8L3NwYW4+PGlucHV0IGlkPSJ0YWtlUGN0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHZhbHVlPSIxNSI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844OrICU8L3NwYW4+PGlucHV0IGlkPSJ0cmFpbFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iNyI+PC9kaXY+CiAgPC9kaXY+CiAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRIb2xkaW5nKCkiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPuWun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmDwvYnV0dG9uPgogIDxwIGNsYXNzPSJtdXRlZCI+6YeO5p2R44ON44OD44OI77yG44Kz44O844Or77yP44G744Gj44Go44OA44Kk44Os44Kv44OI44Gu5Zu95YaF54++54mp44O744Kq44Oz44Op44Kk44Oz5rOo5paH44Gu56iO6L685omL5pWw5paZ6KGo44KS5L2/55So44CCPC9wPgogIDxkaXYgaWQ9ImhvbGRpbmdzIj48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+RgCDjgqbjgqnjg4Pjg4Hjg6rjgrnjg4g8L2gzPgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8aW5wdXQgaWQ9IndhdGNoQ29kZSIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8YnV0dG9uIG9uY2xpY2s9ImFkZFdhdGNoKCkiPui/veWKoDwvYnV0dG9uPgogIDwvZGl2PgogIDxkaXYgaWQ9IndhdGNoQ29tcGFueU5hbWUiIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbjo0cHggMnB4IDhweCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBpZD0id2F0Y2hzIj48L2Rpdj4KPC9kaXY+Cgo8c2NyaXB0Pgpjb25zdCAkPXg9PmRvY3VtZW50LmdldEVsZW1lbnRCeUlkKHgpOwpmdW5jdGlvbiB2YWwoaWQpe2xldCB2PSQoaWQpLnZhbHVlLnRyaW0oKTtyZXR1cm4gdj09PScnP251bGw6TnVtYmVyKHYpfQpmdW5jdGlvbiBsb2NhbChrKXt0cnl7cmV0dXJuIEpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oayl8fCdbXScpfWNhdGNoKGUpe3JldHVybltdfX0KZnVuY3Rpb24gc2F2ZShrLHYpe2xvY2FsU3RvcmFnZS5zZXRJdGVtKGssSlNPTi5zdHJpbmdpZnkodikpfQpmdW5jdGlvbiBmbXQodixkPTIpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZChkKX0KZnVuY3Rpb24geWVuKHYpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzonwqUnK01hdGgucm91bmQoTnVtYmVyKHYpKS50b0xvY2FsZVN0cmluZygnamEtSlAnKX0KZnVuY3Rpb24gc3RhdGVKYShzKXsKICBpZihzPT09J3N0cm9uZ19idWxsaXNoJylyZXR1cm4gJ+W8t+awlyc7CiAgaWYocz09PSdidWxsaXNoJylyZXR1cm4gJ+OChOOChOW8t+awlyc7CiAgaWYocz09PSduZXV0cmFsJylyZXR1cm4gJ+S4reeriyc7CiAgaWYocz09PSdiZWFyaXNoJylyZXR1cm4gJ+OChOOChOW8seawlyc7CiAgaWYocz09PSdzdHJvbmdfYmVhcmlzaCcpcmV0dXJuICflvLHmsJcnOwogIHJldHVybiAn5Yik5a6a5L+d55WZJzsKfQoKZnVuY3Rpb24gc3RhdGVDbGFzcyhzKXsKICBpZihzPT09J3N0cm9uZ19idWxsaXNoJylyZXR1cm4gJ3N0YXRlLXN0cm9uZy1idWxsJzsKICBpZihzPT09J2J1bGxpc2gnKXJldHVybiAnc3RhdGUtYnVsbCc7CiAgaWYocz09PSduZXV0cmFsJylyZXR1cm4gJ3N0YXRlLW5ldXRyYWwnOwogIGlmKHM9PT0nYmVhcmlzaCcpcmV0dXJuICdzdGF0ZS1iZWFyJzsKICBpZihzPT09J3N0cm9uZ19iZWFyaXNoJylyZXR1cm4gJ3N0YXRlLXN0cm9uZy1iZWFyJzsKICByZXR1cm4gJ3N0YXRlLW5ldXRyYWwnOwp9CgpmdW5jdGlvbiBmYWN0b3JKYShrZXkpewogIGlmKGtleT09PSd0ZWNobmljYWwnKXJldHVybiAn44OG44Kv44OL44Kr44OrJzsKICBpZihrZXk9PT0nZWFybmluZ3MnKXJldHVybiAn5rG6566XJzsKICBpZihrZXk9PT0nc3VwcGx5JylyZXR1cm4gJ+mcgOe1pnByb3h5JzsKICBpZihrZXk9PT0ncG9saWN5JylyZXR1cm4gJ+Wbveetlic7CiAgcmV0dXJuIGtleXx8J+imgeWboCc7Cn0KCmZ1bmN0aW9uIGRyaXZlclNlbnRlbmNlKHNjKXsKICBjb25zdCBwPXNjJiZzYy5zdHJvbmdlc3RfcG9zaXRpdmU7CiAgY29uc3Qgbj1zYyYmc2Muc3Ryb25nZXN0X25lZ2F0aXZlOwogIGNvbnN0IHNjb3JlPU51bWJlcihzYyYmc2Muc2NvcmUxMDApOwoKICBsZXQgaGVhZD0nJzsKICBpZihOdW1iZXIuaXNGaW5pdGUoc2NvcmUpKXsKICAgIGlmKHNjb3JlPj04MCloZWFkPSflj5blvpfmuIjjgb/opoHlm6DjgpLnt4/lkIjjgZnjgovjgajjgIHlvLfjgYTjg5fjg6njgrnoqZXkvqHjgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49NjUpaGVhZD0n44OX44Op44K56KaB5Zug44GM5YSq5Yui44Gn44CB44KE44KE5by35rCX44Gu6KmV5L6h44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTQ1KWhlYWQ9J+ODl+ODqeOCueOBqOODnuOCpOODiuOCueOBjOaLruaKl+OBl+OAgeS4reeri+Wcj+OBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj0zMCloZWFkPSfjg57jgqTjg4rjgrnopoHlm6Djga7lvbHpn7/jgYzjgoTjgoTlvLfjgY/jgIHmhY7ph43lr4TjgorjgafjgZnjgIInOwogICAgZWxzZSBoZWFkPSfjg57jgqTjg4rjgrnopoHlm6Djga7lvbHpn7/jgYzlpKfjgY3jgY/jgIHlvLHmsJflr4TjgorjgafjgZnjgIInOwogIH0KCiAgbGV0IHRhaWw9W107CiAgaWYobil0YWlsLnB1c2goJ+acgOWkp+OBruaKvOOBl+S4i+OBkuimgeWboOOBryAnK2ZhY3RvckphKG4ua2V5KSsnICcrc2NvcmVMYWJlbChuLnNjb3JlKSk7CiAgaWYocCl0YWlsLnB1c2goJ+acgOWkp+OBruaKvOOBl+S4iuOBkuimgeWboOOBryAnK2ZhY3RvckphKHAua2V5KSsnICcrc2NvcmVMYWJlbChwLnNjb3JlKSk7CiAgcmV0dXJuIGhlYWQrKHRhaWwubGVuZ3RoPycgJyt0YWlsLmpvaW4oJ+OAgicpKyfjgIInOicnKTsKfQoKZnVuY3Rpb24gZHJpdmVyQm94SHRtbCh0aXRsZSxkLGtpbmQpewogIGlmKCFkKXJldHVybiBgPGRpdiBjbGFzcz0iZHJpdmVyYm94Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuipsuW9k+OBquOBlzwvYj48L2Rpdj5gOwogIGNvbnN0IHNpZ249TnVtYmVyKGQuY29udHJpYnV0aW9uKT49MD8nKyc6Jyc7CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJkcml2ZXJib3giPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj4KICAgIDxiPiR7ZmFjdG9ySmEoZC5rZXkpfSAke3Njb3JlTGFiZWwoZC5zY29yZSl9PC9iPgogICAgPHNtYWxsIGNsYXNzPSJtdXRlZCI+5YaN6YWN5YiG5b6M44Gu6YeN44G/ICR7ZC53ZWlnaHRfcGN0fSUgLyDlr4TkuI4gJHtzaWdufSR7TnVtYmVyKGQuY29udHJpYnV0aW9uKS50b0ZpeGVkKDEpfTwvc21hbGw+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gY29udHJpYnV0aW9uQ2FyZEh0bWwoZCl7CiAgaWYoIWQpcmV0dXJuICcnOwogIGNvbnN0IGM9TnVtYmVyKGQuY29udHJpYnV0aW9uKTsKICBjb25zdCBzaWduPWM+MD8nKyc6Jyc7CiAgY29uc3QgaW1wYWN0PWMvMjsKICBjb25zdCBpbXBhY3RTaWduPWltcGFjdD4wPycrJzonJzsKICByZXR1cm4gYDxkaXYgY2xhc3M9ImNvbnRyaWIiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke2ZhY3RvckphKGQua2V5KX08L3NwYW4+CiAgICA8Yj4ke3NpZ259JHtjLnRvRml4ZWQoMSl9PC9iPgogICAgPHNtYWxsPuWGjemFjeWIhuW+jOmHjeOBvyAke051bWJlcihkLndlaWdodF9wY3QpLnRvRml4ZWQoMSl9JTwvc21hbGw+CiAgICA8c21hbGw+MTAw54K55o+b566X44G444Gu5b2x6Z+/ICR7aW1wYWN0U2lnbn0ke2ltcGFjdC50b0ZpeGVkKDEpfeeCuTwvc21hbGw+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gcmVuZGVyQ29udHJpYnV0aW9ucyhzYyl7CiAgY29uc3QgYm94PSQoJ2NvbnRyaWJ1dGlvbkJveCcpOwogIGNvbnN0IGdyaWQ9JCgnY29udHJpYnV0aW9uR3JpZCcpOwogIGlmKCFib3h8fCFncmlkKXJldHVybjsKICBjb25zdCBkcz0oc2MmJnNjLmRyaXZlcnMpfHxbXTsKICBpZighZHMubGVuZ3RoKXsKICAgIGJveC5zdHlsZS5kaXNwbGF5PSdub25lJzsKICAgIGdyaWQuaW5uZXJIVE1MPScnOwogICAgcmV0dXJuOwogIH0KICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogIGdyaWQuaW5uZXJIVE1MPWRzLm1hcChjb250cmlidXRpb25DYXJkSHRtbCkuam9pbignJyk7Cn0KZnVuY3Rpb24gcGN0KHYpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgyKSsnJSd9CmZ1bmN0aW9uIHN0YXRGb3IoaCxrZXkpe3JldHVybiBoJiZoLmZvcndhcmRfc3RhdHMmJmguZm9yd2FyZF9zdGF0c1trZXldP2guZm9yd2FyZF9zdGF0c1trZXldOm51bGx9CgpmdW5jdGlvbiBkYXRhQWdlRGF5cyhkYXRlU3RyKXsKICBpZighZGF0ZVN0cilyZXR1cm4gbnVsbDsKICBjb25zdCBtPVN0cmluZyhkYXRlU3RyKS5tYXRjaCgvXihcZHs0fSktKFxkezJ9KS0oXGR7Mn0pJC8pOwogIGlmKCFtKXJldHVybiBudWxsOwogIGNvbnN0IGQ9RGF0ZS5VVEMoTnVtYmVyKG1bMV0pLE51bWJlcihtWzJdKS0xLE51bWJlcihtWzNdKSk7CiAgY29uc3Qgbm93PW5ldyBEYXRlKCk7CiAgY29uc3QgdG9kYXk9RGF0ZS5VVEMobm93LmdldEZ1bGxZZWFyKCksbm93LmdldE1vbnRoKCksbm93LmdldERhdGUoKSk7CiAgcmV0dXJuIE1hdGgubWF4KDAsTWF0aC5mbG9vcigodG9kYXktZCkvODY0MDAwMDApKTsKfQoKZnVuY3Rpb24gZnJlc2huZXNzRm9yKGRhdGVTdHIpewogIGNvbnN0IGRheXM9ZGF0YUFnZURheXMoZGF0ZVN0cik7CiAgaWYoZGF5cz09PW51bGwpewogICAgcmV0dXJuIHtsZXZlbDondW5rbm93bicsZGF5czpudWxsLGxhYmVsOifml6Xku5jkuI3mmI4nLGNsczonZnJlc2gtd2FybicsZGVjaXNpb25fb2s6ZmFsc2V9OwogIH0KICBpZihkYXlzPD00KXsKICAgIHJldHVybiB7bGV2ZWw6J2ZyZXNoJyxkYXlzLGxhYmVsOifprq7luqZPSycsY2xzOidmcmVzaC1vaycsZGVjaXNpb25fb2s6dHJ1ZX07CiAgfQogIGlmKGRheXM8PTEwKXsKICAgIHJldHVybiB7bGV2ZWw6J3dhcm5pbmcnLGRheXMsbGFiZWw6J+OChOOChOmBheW7ticsY2xzOidmcmVzaC13YXJuJyxkZWNpc2lvbl9vazp0cnVlfTsKICB9CiAgcmV0dXJuIHtsZXZlbDonc3RhbGUnLGRheXMsbGFiZWw6J+WPpOOBhOODh+ODvOOCvycsY2xzOidmcmVzaC1zdGFsZScsZGVjaXNpb25fb2s6ZmFsc2V9Owp9CgpmdW5jdGlvbiBmcmVzaG5lc3NUZXh0KGRhdGVTdHIpewogIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGRhdGVTdHIpOwogIGlmKGYuZGF5cz09PW51bGwpcmV0dXJuICfmnIDntYLjg4fjg7zjgr/ml6XjgpLnorroqo3jgafjgY3jgb7jgZvjgpPjgIInOwogIGlmKGYubGV2ZWw9PT0nZnJlc2gnKXJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpeOAgumAmuW4uOOBruWPguiAg+WIpOWumuOBq+S9v+eUqOOBl+OBvuOBmeOAgmA7CiAgaWYoZi5sZXZlbD09PSd3YXJuaW5nJylyZXR1cm4gYOacgOe1guODh+ODvOOCv+aXpeOBi+OCiSAke2YuZGF5c33ml6XjgILpgYXlu7bjgavms6jmhI/jgZfjgabjgIHlrp/pmpvjga7nj77lnKjlgKTjgoLnorroqo3jgZfjgabjgY/jgaDjgZXjgYTjgIJgOwogIHJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpemBheOCjOOAguS7iuaXpeOBruWjsuiyt+WIpOaWreOBq+OBr+WPpOOBhOOBn+OCgeOAgeS/neacieWIpOaWreOBr+iHquWLleOBp+S/neeVmeOBl+OBvuOBmeOAgmA7Cn0KCmZ1bmN0aW9uIHNob3dGcmVzaG5lc3MoZGF0ZVN0cil7CiAgY29uc3QgYm94PSQoJ2ZyZXNobmVzc0JveCcpOwogIGlmKCFib3gpcmV0dXJuOwogIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGRhdGVTdHIpOwogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgYm94LmNsYXNzTmFtZT0nZnJlc2hib3ggJytmLmNsczsKICAkKCdmcmVzaG5lc3NUaXRsZScpLnRleHRDb250ZW50PQogICAgZi5sZXZlbD09PSdmcmVzaCc/J+KchSDjg4fjg7zjgr/prq7luqZPSyc6CiAgICBmLmxldmVsPT09J3dhcm5pbmcnPyfimqDvuI8g44OH44O844K/6YGF5bu244Gr5rOo5oSPJzoKICAgIGYubGV2ZWw9PT0nc3RhbGUnPyfwn5uRIOODh+ODvOOCv+OBjOWPpOOBhOOBn+OCgeS7iuaXpeOBruWIpOaWreOBr+S/neeVmSc6CiAgICAn4pqg77iPIOODh+ODvOOCv+muruW6puOCkueiuuiqjeOBp+OBjeOBvuOBm+OCkyc7CiAgJCgnZnJlc2huZXNzRGV0YWlsJykudGV4dENvbnRlbnQ9ZnJlc2huZXNzVGV4dChkYXRlU3RyKTsKfQoKZnVuY3Rpb24gc2hvd0hpc3RvcnlGcmVzaG5lc3MoaGlzdG9yeURhdGUscHJpY2VEYXRlKXsKICBjb25zdCBib3g9JCgnaGlzdG9yeUZyZXNobmVzc0JveCcpOwogIGlmKCFib3gpcmV0dXJuOwogIGlmKCFoaXN0b3J5RGF0ZSl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnaGlzdG9yeUZyZXNobmVzc1RleHQnKS50ZXh0Q29udGVudD0nMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7liIbmnpDlsaXmrbTjgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ/jgILnj77lnKjlgKTjgaDjgZHooajnpLrjgZfjgb7jgZnjgIInOwogICAgcmV0dXJuOwogIH0KICBjb25zdCBoZj1mcmVzaG5lc3NGb3IoaGlzdG9yeURhdGUpOwogIGlmKGhmLmxldmVsPT09J2ZyZXNoJyl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nbm9uZSc7CiAgICByZXR1cm47CiAgfQogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgY29uc3QgcGQ9cHJpY2VEYXRlfHwn4oCUJzsKICAkKCdoaXN0b3J5RnJlc2huZXNzVGV4dCcpLnRleHRDb250ZW50PQogICAgYOePvuWcqOWApOODh+ODvOOCv+aXpSAke3BkfSAvIOWIhuaekOWxpeattOacgOe1guaXpSAke2hpc3RvcnlEYXRlfeOAguePvuWcqOWApOOBr+aWsOOBl+OBj+OBpuOCguOAgeODiOODrOODs+ODieODu+acn+W+heWApOODu+mcgOe1pnByb3h544Gv6YGF5bu244OH44O844K/44Gu5Y+C6ICD5YCk44Gn44GZ44CCYDsKfQoKZnVuY3Rpb24gY29uZmlkZW5jZUZvcihoKXsKICBjb25zdCB2YWxzPVtoLnJldHVybl8yMGQsaC5yZXR1cm5fMTI2ZCxoLnJldHVybl8yNTJkXS5tYXAoTnVtYmVyKS5maWx0ZXIoTnVtYmVyLmlzRmluaXRlKTsKICBjb25zdCBzdGF0cz1bJzIwZCcsJzEyNmQnLCcyNTJkJ10ubWFwKGs9PnN0YXRGb3IoaCxrKSkuZmlsdGVyKHM9PnMmJnMuc3RhdHVzPT09J29rJyk7CgogIGlmKCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpfHwhaC5hc29mKXJldHVybiAwOwoKICBjb25zdCBjb3ZlcmFnZT12YWxzLmxlbmd0aC8zOwogIGxldCBhZ3JlZW1lbnQ9MC41OwogIGlmKHZhbHMubGVuZ3RoKXsKICAgIGNvbnN0IHBvcz12YWxzLmZpbHRlcih4PT54PjApLmxlbmd0aDsKICAgIGNvbnN0IG5lZz12YWxzLmZpbHRlcih4PT54PDApLmxlbmd0aDsKICAgIGFncmVlbWVudD1NYXRoLm1heChwb3MsbmVnKS92YWxzLmxlbmd0aDsKICB9CiAgY29uc3Qgc3RhdENvdmVyYWdlPXN0YXRzLmxlbmd0aC8zOwogIGxldCBzY29yZT0oY292ZXJhZ2UqMC40NSArIGFncmVlbWVudCowLjI1ICsgc3RhdENvdmVyYWdlKjAuMzApKjEwMDsKCiAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKGguYXNvZik7CiAgaWYoZnJlc2gubGV2ZWw9PT0nd2FybmluZycpc2NvcmUqPTAuNjA7CiAgaWYoZnJlc2gubGV2ZWw9PT0nc3RhbGUnKXNjb3JlPU1hdGgubWluKHNjb3JlLDI1KTsKICBpZihmcmVzaC5sZXZlbD09PSd1bmtub3duJylzY29yZT1NYXRoLm1pbihzY29yZSwyMCk7CgogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgaWYoaGlzdG9yeUZyZXNoLmxldmVsPT09J3dhcm5pbmcnKXNjb3JlKj0wLjc1OwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSdzdGFsZScpc2NvcmU9TWF0aC5taW4oc2NvcmUsMzUpOwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSd1bmtub3duJylzY29yZT1NYXRoLm1pbihzY29yZSwyNSk7CgogIHJldHVybiBNYXRoLnJvdW5kKHNjb3JlKTsKfQoKZnVuY3Rpb24gZGVjaXNpb25Gb3IoaCxjKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFoLmFzb2YpewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVmScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjon5a6f44OH44O844K/5pyq5Y+W5b6X44CC5Y+z5LiK44Gu44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgY/jgaDjgZXjgYTjgIInLAogICAgICBjb25maWRlbmNlOjAKICAgIH07CiAgfQoKICBjb25zdCByMjA9TnVtYmVyKGgucmV0dXJuXzIwZCksIHIxMjY9TnVtYmVyKGgucmV0dXJuXzEyNmQpLCByMjUyPU51bWJlcihoLnJldHVybl8yNTJkKTsKICBjb25zdCBjb25maWRlbmNlPWNvbmZpZGVuY2VGb3IoaCk7CiAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKGguYXNvZik7CgogIGlmKCFmcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOmAke2ZyZXNobmVzc1RleHQoaC5hc29mKX0g5pCN5YiH44KK44O75Yip56K644Op44Kk44Oz44Go44Gu5q+U6LyD44KC5Y+C6ICD5YCk5omx44GE44Gn44GZ44CCYCwKICAgICAgY29uZmlkZW5jZQogICAgfTsKICB9CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5pCN5YiH44KK5qSc6KiOJyxjbHM6J2Qtc3RvcCcscmVhc29uOifoqK3lrprjgZfjgZ/mkI3liIfjgorlj4LogIPjg6njgqTjg7Pku6XkuIsnLGNvbmZpZGVuY2V9OwogIH0KICBpZihjdXI+PWMudGFrZVByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+WIqeeiuuaknOiojicsY2xzOidkLXRha2UnLHJlYXNvbjon6Kit5a6a44GX44Gf5Yip56K65Y+C6ICD44Op44Kk44Oz5Lul5LiKJyxjb25maWRlbmNlfTsKICB9CgogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgaWYoIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZ77yI5bGl5q205Y+k44GE77yJJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOmDnj77lnKjlgKTjga/lj5blvpfjgafjgY3jgabjgYTjgb7jgZnjgYzjgIHliIbmnpDlsaXmrbTjga8gJHtoLmhpc3RvcnlfYXNvZnx8J+S4jeaYjid944CC5Zu65a6a44Gu5pCN5YiH44KKL+WIqeeiuuODqeOCpOODs+OBq+OBr+acquWIsOmBlOOBp+OBmeOBjOOAgeODiOODrOODs+ODieWIpOaWreOBr+S/neeVmeOBl+OBvuOBmeOAgmAsCiAgICAgIGNvbmZpZGVuY2UKICAgIH07CiAgfQoKICBpZihjdXI8PWMudHJhaWxQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpemrmOWApOWfuua6luOBruODiOODrOODvOODquODs+OCsOWPguiAg+ODqeOCpOODs+S7peS4iycsY29uZmlkZW5jZX07CiAgfQoKICBsZXQgcG9zaXRpdmU9MCwgbmVnYXRpdmU9MDsKICBbcjIwLHIxMjYscjI1Ml0uZm9yRWFjaCh4PT57CiAgICBpZihOdW1iZXIuaXNGaW5pdGUoeCkpewogICAgICBpZih4PjApcG9zaXRpdmUrKzsKICAgICAgaWYoeDwwKW5lZ2F0aXZlKys7CiAgICB9CiAgfSk7CgogIGlmKG5lZ2F0aXZlPj0yKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel44O7MTI25pel44O7MjUy5pel44Gu44GG44Gh44Oe44Kk44OK44K55YK+5ZCR44GM5YSq5YuiJyxjb25maWRlbmNlfTsKICB9CiAgaWYocG9zaXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon5L+d5pyJ57aZ57aaJyxjbHM6J2QtaG9sZCcscmVhc29uOifoqK3lrprjg6njgqTjg7PlhoXjgafjgIHopIfmlbDmnJ/plpPjga7kvqHmoLzjg4jjg6zjg7Pjg4njgYzjg5fjg6njgrknLGNvbmZpZGVuY2V9OwogIH0KICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntprvvIjmp5jlrZDopovvvIknLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOAguacn+mWk+WIpeODiOODrOODs+ODieOBr+W8t+W8seOBjOa3t+WcqCcsY29uZmlkZW5jZX07Cn0KZnVuY3Rpb24gZXZIdG1sKHRpdGxlLHMpewogIGlmKCFzfHxzLnN0YXR1cyE9PSdvaycpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c21hbGw+5qiZ5pysICR7bn3ku7Y8L3NtYWxsPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj4KICAgIDxiPuW5s+WdhyAke3BjdChzLm1lYW4pfTwvYj4KICAgIDxzbWFsbD7kuK3lpK7lgKQgJHtwY3Qocy5tZWRpYW4pfTwvc21hbGw+CiAgICA8c21hbGw+5LiK5piH546HICR7cGN0KHMucG9zaXRpdmVfcmF0ZSl9PC9zbWFsbD4KICAgIDxzbWFsbD5QMTDjgJxQOTAgJHtwY3Qocy5wMTApfSDjgJwgJHtwY3Qocy5wOTApfTwvc21hbGw+CiAgICA8c21hbGw+5qiZ5pysICR7cy5ufeS7tjwvc21hbGw+CiAgPC9kaXY+YDsKfQoKCmZ1bmN0aW9uIGRpc3RhbmNlSW5mbyhjdXIsdGFyZ2V0LGtpbmQpewogIGN1cj1OdW1iZXIoY3VyKTsgdGFyZ2V0PU51bWJlcih0YXJnZXQpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhTnVtYmVyLmlzRmluaXRlKHRhcmdldCkpcmV0dXJuICfigJQnOwogIGNvbnN0IGRpZmY9KHRhcmdldC9jdXItMSkqMTAwOwogIGlmKGtpbmQ9PT0nc3RvcCcpewogICAgaWYoZGlmZj49MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gTWF0aC5hYnMoZGlmZikudG9GaXhlZCgyKSsnJSDkuIsnOwogIH0KICBpZihraW5kPT09J3Rha2UnKXsKICAgIGlmKGRpZmY8PTApcmV0dXJuICfjg6njgqTjg7PliLDpgZTmuIjjgb8nOwogICAgcmV0dXJuIGRpZmYudG9GaXhlZCgyKSsnJSDkuIonOwogIH0KICByZXR1cm4gKGRpZmY+PTA/JysnOicnKStkaWZmLnRvRml4ZWQoMikrJyUnOwp9CgpmdW5jdGlvbiBwcmljZVJhbmdlKGN1cixzKXsKICBjdXI9TnVtYmVyKGN1cik7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFzfHxzLnN0YXR1cyE9PSdvaycpcmV0dXJuIG51bGw7CiAgcmV0dXJuIHsKICAgIGxvdzpjdXIqKDErTnVtYmVyKHMucDEwKS8xMDApLAogICAgaGlnaDpjdXIqKDErTnVtYmVyKHMucDkwKS8xMDApLAogICAgbWVkaWFuOmN1ciooMStOdW1iZXIocy5tZWRpYW4pLzEwMCkKICB9Owp9CgpmdW5jdGlvbiByYW5nZUh0bWwodGl0bGUsY3VyLHMpewogIGNvbnN0IHI9cHJpY2VSYW5nZShjdXIscyk7CiAgaWYoIXIpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9InJhbmdlYm94Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaomeacrCAke2595Lu2PC9zcGFuPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfSBQMTDjgJxQOTA8L3NwYW4+CiAgICA8Yj4ke3llbihyLmxvdyl9IOOAnCAke3llbihyLmhpZ2gpfTwvYj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Lit5aSu5YCk5o+b566XICR7eWVuKHIubWVkaWFuKX08L3NwYW4+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gYWN0aW9uVGV4dChoLGMsZCl7CiAgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZknfHxkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOagquS+oeWPpOOBhO+8iScpcmV0dXJuICfjg4fjg7zjgr/jgYzlj6TjgYTjgZ/jgoHku4rml6Xjga7liKTmlq3jga/kv53nlZnjgILoqLzliLjkvJrnpL7jgarjganjgaflrp/pmpvjga7nj77lnKjlgKTjgpLnorroqo3jgZfjgabjgYvjgonliKTmlq3jgIInOwogIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5bGl5q205Y+k44GE77yJJylyZXR1cm4gJ+ePvuWcqOWApOOBr+eiuuiqjea4iOOBv+OAguWbuuWumuOBruaQjeWIh+OCii/liKnnorrjg6njgqTjg7PjgaDjgZHnorroqo3jgZfjgIHjg4jjg6zjg7Pjg4nliKTmlq3jga/lsaXmrbTmm7TmlrDjgb7jgafkv53nlZnjgIInOwogIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylyZXR1cm4gJ+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+OCkuS4i+WbnuOBo+OBpuOBhOOBvuOBmeOAguWun+mam+OBruePvuWcqOWApOOBqOazqOaWh+adoeS7tuOCkueiuuiqjeOBl+OBpuOAgee4ruWwj+ODu+aSpOmAgOOCkuaknOiojuOAgic7CiAgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKXJldHVybiAn5Yip56K65Y+C6ICD44Op44Kk44Oz44Gr5Yiw6YGU44GX44Gm44GE44G+44GZ44CC5YWo6YOo5aOy5Y2044Gg44GR44Gn44Gq44GP44CB5YiG5Ymy5Yip56K644KC5YCZ6KOc44CCJzsKICBpZihkLmxhYmVsPT09J+itpuaIkicpcmV0dXJuICforabmiJLjgr7jg7zjg7PjgILjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PjgajkuK3nn63mnJ/jga7lgKTli5XjgY3jgpLlhKrlhYjjgZfjgabnorroqo3jgIInOwogIHJldHVybiAn6Kit5a6a44Op44Kk44Oz5YaF44CC5L+d5pyJ57aZ57aa5YCZ6KOc44Gn44GZ44GM44CB54Sh5paZ44OH44O844K/44Gv6YGF5bu244GZ44KL44Gf44KB5a6f6Zqb44Gu54++5Zyo5YCk44KC56K66KqN44CCJzsKfQoKCmZ1bmN0aW9uIG5vbXVyYU5ldEZlZShhbW91bnQpewogIGFtb3VudD1OdW1iZXIoYW1vdW50fHwwKTsKICBpZihhbW91bnQ8PTApcmV0dXJuIDA7CiAgaWYoYW1vdW50PD0xMDAwMDApcmV0dXJuIDE1MjsKICBpZihhbW91bnQ8PTMwMDAwMClyZXR1cm4gMzMwOwogIGlmKGFtb3VudDw9NTAwMDAwKXJldHVybiA1MjQ7CiAgaWYoYW1vdW50PD0xMDAwMDAwKXJldHVybiAxMDQ4OwogIGlmKGFtb3VudDw9MjAwMDAwMClyZXR1cm4gMjA5NTsKICBpZihhbW91bnQ8PTMwMDAwMDApcmV0dXJuIDMxNDM7CiAgaWYoYW1vdW50PD01MDAwMDAwKXJldHVybiA1MjM4OwogIGlmKGFtb3VudDw9MTAwMDAwMDApcmV0dXJuIDEwNDc2OwogIGlmKGFtb3VudDw9MjAwMDAwMDApcmV0dXJuIDIwOTUyOwogIGlmKGFtb3VudDw9MzAwMDAwMDApcmV0dXJuIDMxNDI5OwogIGlmKGFtb3VudDw9NTAwMDAwMDApcmV0dXJuIDQxOTA1OwogIHJldHVybiA3ODU3MTsKfQpmdW5jdGlvbiBmZWVGb3IoYW1vdW50LG1vZGUpe3JldHVybiBtb2RlPT09J25vbXVyYV9uZXQnP25vbXVyYU5ldEZlZShhbW91bnQpOjB9CgpmdW5jdGlvbiBjYWxjSG9sZGluZyhoKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgY29zdD1OdW1iZXIoaC5jb3N0KTsKICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzKTsKICBjb25zdCBidXlWYWx1ZT1jb3N0KnNoYXJlczsKICBjb25zdCBidXlGZWU9ZmVlRm9yKGJ1eVZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGN1cnJlbnRWYWx1ZT1jdXIqc2hhcmVzOwogIGNvbnN0IHNlbGxGZWU9ZmVlRm9yKGN1cnJlbnRWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBpbnZlc3RlZD1idXlWYWx1ZStidXlGZWU7CiAgY29uc3QgbmV0Tm93PWN1cnJlbnRWYWx1ZS1zZWxsRmVlLWludmVzdGVkOwogIGNvbnN0IG5ldE5vd1BjdD1pbnZlc3RlZD9uZXROb3cvaW52ZXN0ZWQqMTAwOm51bGw7CgogIGNvbnN0IHN0b3BQcmljZT1jb3N0KigxLU51bWJlcihoLnN0b3BfcGN0KS8xMDApOwogIGNvbnN0IHRha2VQcmljZT1jb3N0KigxK051bWJlcihoLnRha2VfcGN0KS8xMDApOwogIGNvbnN0IGhpZ2gyMD1OdW1iZXIoaC5oaWdoXzIwZHx8Y3VyKTsKICBjb25zdCB0cmFpbFByaWNlPWhpZ2gyMCooMS1OdW1iZXIoaC50cmFpbF9wY3QpLzEwMCk7CgogIGNvbnN0IHN0b3BWYWx1ZT1zdG9wUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHRha2VWYWx1ZT10YWtlUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHN0b3BOZXQ9c3RvcFZhbHVlLWZlZUZvcihzdG9wVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CiAgY29uc3QgdGFrZU5ldD10YWtlVmFsdWUtZmVlRm9yKHRha2VWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKCiAgcmV0dXJuIHtidXlWYWx1ZSxidXlGZWUsY3VycmVudFZhbHVlLHNlbGxGZWUsaW52ZXN0ZWQsbmV0Tm93LG5ldE5vd1BjdCxzdG9wUHJpY2UsdGFrZVByaWNlLHRyYWlsUHJpY2Usc3RvcE5ldCx0YWtlTmV0fTsKfQoKCmNvbnN0IG5hbWVUaW1lcnM9e307CmNvbnN0IG5hbWVDYWNoZT17fTsKCmZ1bmN0aW9uIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeD0nJyl7CiAgaWYoIXRhcmdldClyZXR1cm47CiAgaWYoIWluZm98fCFpbmZvLm5hbWUpewogICAgdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIHJldHVybjsKICB9CiAgbGV0IGV4dHJhcz1bXTsKICBpZihpbmZvLm1hcmtldClleHRyYXMucHVzaChpbmZvLm1hcmtldCk7CiAgaWYoaW5mby5zZWN0b3IzMylleHRyYXMucHVzaChpbmZvLnNlY3RvcjMzKTsKICB0YXJnZXQuaW5uZXJIVE1MPSc8Yj4nK3ByZWZpeCtpbmZvLm5hbWUrJzwvYj4nKyhleHRyYXMubGVuZ3RoPyc8YnI+PHNwYW4gY2xhc3M9Im11dGVkIj4nK2V4dHJhcy5qb2luKCcgLyAnKSsnPC9zcGFuPic6JycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRDb21wYW55KGNvZGUpewogIGNvbnN0IGM9U3RyaW5nKGNvZGV8fCcnKS50cmltKCk7CiAgaWYoIWMpcmV0dXJuIG51bGw7CiAgaWYobmFtZUNhY2hlW2NdKXJldHVybiBuYW1lQ2FjaGVbY107CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvc2VjdXJpdHk/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+mKmOafhOWQjeOCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIG5hbWVDYWNoZVtjXT14OwogIHJldHVybiB4Owp9CgpmdW5jdGlvbiBzY2hlZHVsZUNvbXBhbnlMb29rdXAoaW5wdXRJZCx0YXJnZXRJZCxwcmVmaXg9JycpewogIGNsZWFyVGltZW91dChuYW1lVGltZXJzW2lucHV0SWRdKTsKICBjb25zdCBjPSQoaW5wdXRJZCkudmFsdWUudHJpbSgpOwogIGNvbnN0IHRhcmdldD0kKHRhcmdldElkKTsKCiAgaWYoYy5sZW5ndGg8NCl7CiAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya4oCUJzsKICAgIHJldHVybjsKICB9CgogIG5hbWVUaW1lcnNbaW5wdXRJZF09c2V0VGltZW91dChhc3luYygpPT57CiAgICB0cnl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9J+mKmOafhOWQjeOCkueiuuiqjeS4reKApic7CiAgICAgIGNvbnN0IGluZm89YXdhaXQgZ2V0Q29tcGFueShjKTsKICAgICAgZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4KTsKICAgIH1jYXRjaChlKXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICB9CiAgfSw0NTApOwp9CgoKCmZ1bmN0aW9uIHByaW9yaXR5QWN0aW9uRm9yKGgpewogIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogIGNvbnN0IG5hbWU9aC5jb21wYW55X25hbWV8fCcnOwogIGNvbnN0IGxhYmVsPShoLmNvZGV8fCcnKSsobmFtZT8nICcrbmFtZTonJyk7CiAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwoKICBpZighdmFsaWQpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTYsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOiflrp/jg4fjg7zjgr/jgpLmm7TmlrAnLAogICAgICBkZXRhaWw6J+acgOaWsOWPluW+l+e1guWApOOBjOOBguOCiuOBvuOBm+OCk+OAguOBvuOBmuOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44CCJywKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3IoaC5hc29mKTsKICBpZighZnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTksCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifjg4fjg7zjgr/prq7luqbjgpLnorroqo0nLAogICAgICBkZXRhaWw6YCR7ZnJlc2huZXNzVGV4dChoLmFzb2YpfSDlrp/pmpvjga7nj77lnKjlgKTjgpLlhYjjgavnorroqo3jgIJgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IHN0b3BEaXN0PShjdXIvYy5zdG9wUHJpY2UtMSkqMTAwOwogIGNvbnN0IHRha2VEaXN0PShjLnRha2VQcmljZS9jdXItMSkqMTAwOwogIGNvbnN0IHRyYWlsRGlzdD0oY3VyL2MudHJhaWxQcmljZS0xKSoxMDA7CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6MTAwLAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5pCN5YiH44KK44Op44Kk44Oz5Yiw6YGUJywKICAgICAgZGV0YWlsOmDmnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4oY3VyKX0gLyDmkI3liIfjgorlj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoc3RvcERpc3Q8PTMpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTQsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOOBguOBqCAke3N0b3BEaXN0LnRvRml4ZWQoMil9JSDjgafmkI3liIfjgorlj4LogIPjg6njgqTjg7NgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKGN1cj49Yy50YWtlUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTAsCiAgICAgIGNsczoncHJpb3JpdHktdGFrZScsCiAgICAgIHRpdGxlOifliKnnorrjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOWIqeeiuuWPguiAgyAke3llbihjLnRha2VQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZih0YWtlRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo4NCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7dGFrZURpc3QudG9GaXhlZCgyKX0lIOOBp+WIqeeiuuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBpZighaGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjc4LAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOifliIbmnpDlsaXmrbTjgpLmm7TmlrDlvoXjgaEnLAogICAgICBkZXRhaWw6YOePvuWcqOWApCAke2guYXNvZnx8J+KAlCd9IC8g5YiG5p6Q5bGl5q20ICR7aC5oaXN0b3J5X2Fzb2Z8fCfigJQnfeOAguWbuuWumuS+oeagvOODqeOCpOODs+S7peWkluOBruWIpOaWreOBr+S/neeVmeOAgmAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoZC5sYWJlbD09PSforabmiJInKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjgwLAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOiforabmiJLliKTlrponLAogICAgICBkZXRhaWw6ZC5yZWFzb24sCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoTnVtYmVyLmlzRmluaXRlKHRyYWlsRGlzdCkmJnRyYWlsRGlzdDw9Mi41KXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjc2LAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOifjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOODiOODrOODvOODquODs+OCsOWPguiAgyAke3llbihjLnRyYWlsUHJpY2UpfSDjgb7jgacgJHt0cmFpbERpc3QudG9GaXhlZCgyKX0lYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICByZXR1cm4gewogICAgc2NvcmU6MzAsCiAgICBjbHM6J3ByaW9yaXR5LWdvb2QnLAogICAgdGl0bGU6J+mAmuW4uOebo+imlicsCiAgICBkZXRhaWw6ZC5yZWFzb258fCfoqK3lrprjg6njgqTjg7PlhoUnLAogICAgbGFiZWwKICB9Owp9CgpmdW5jdGlvbiByZW5kZXJQcmlvcml0eUFjdGlvbnMoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwcmlvcml0eUFjdGlvbnMnKTsKICBpZighYm94KXJldHVybjsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go44CB5YSq5YWI44GX44Gm56K66KqN44GZ44KL6YqY5p+E44KS6Ieq5YuV6KGo56S644GX44G+44GZ44CCPC9wPic7CiAgICByZXR1cm47CiAgfQoKICBsZXQgYWN0aW9ucz1hLm1hcChwcmlvcml0eUFjdGlvbkZvcik7CgogIC8vIENvbmNlbnRyYXRpb24gYWxlcnQgKHBvcnRmb2xpby1sZXZlbCkKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wJiZoLmFzb2YpOwogIGNvbnN0IHRvdGFsPXZhbGlkLnJlZHVjZSgocyxoKT0+cytOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApLDApOwogIGlmKHRvdGFsPjApewogICAgbGV0IG1heEhvbGRpbmc9bnVsbCwgbWF4VmFsdWU9MDsKICAgIHZhbGlkLmZvckVhY2goaD0+ewogICAgICBjb25zdCB2PU51bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCk7CiAgICAgIGlmKHY+bWF4VmFsdWUpe21heFZhbHVlPXY7bWF4SG9sZGluZz1ofQogICAgfSk7CiAgICBjb25zdCBjb25jZW50cmF0aW9uPW1heFZhbHVlL3RvdGFsKjEwMDsKICAgIGlmKGNvbmNlbnRyYXRpb24+PTYwICYmIG1heEhvbGRpbmcpewogICAgICBhY3Rpb25zLnB1c2goewogICAgICAgIHNjb3JlOjcyLAogICAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgICB0aXRsZTon6ZuG5Lit5bqm44KS56K66KqNJywKICAgICAgICBkZXRhaWw6YCR7bWF4SG9sZGluZy5jb2RlfSR7bWF4SG9sZGluZy5jb21wYW55X25hbWU/JyAnK21heEhvbGRpbmcuY29tcGFueV9uYW1lOicnfSDjgYzjg53jg7zjg4jjg5Xjgqnjg6rjgqrjga4gJHtjb25jZW50cmF0aW9uLnRvRml4ZWQoMSl9JWAsCiAgICAgICAgbGFiZWw6J+ODneODvOODiOODleOCqeODquOCqicKICAgICAgfSk7CiAgICB9CiAgfQoKICBhY3Rpb25zLnNvcnQoKHgseSk9Pnkuc2NvcmUteC5zY29yZSk7CgogIGNvbnN0IGltcG9ydGFudD1hY3Rpb25zLmZpbHRlcih4PT54LnNjb3JlPj03MCk7CiAgY29uc3Qgc2hvd249KGltcG9ydGFudC5sZW5ndGg/aW1wb3J0YW50OmFjdGlvbnMpLnNsaWNlKDAsNCk7CgogIGJveC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9InByaW9yaXR5LXdyYXAiPiR7CiAgICBzaG93bi5tYXAoKHgsaSk9PmAKICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktaXRlbSAke3guY2xzfSI+CiAgICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktbGluZSI+CiAgICAgICAgICA8ZGl2PgogICAgICAgICAgICA8c3BhbiBjbGFzcz0icHJpb3JpdHktcmFuayI+UFJJT1JJVFkgJHtpKzF9PC9zcGFuPgogICAgICAgICAgICA8Yj4ke3gudGl0bGV9PC9iPgogICAgICAgICAgPC9kaXY+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1jb2RlIj4ke3gubGFiZWx9PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjVweCI+JHt4LmRldGFpbH08L2Rpdj4KICAgICAgPC9kaXY+CiAgICBgKS5qb2luKCcnKQogIH08L2Rpdj5gICsgKAogICAgaW1wb3J0YW50Lmxlbmd0aAogICAgICA/ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+4oC75YSq5YWI5bqm44Gv6Kit5a6a44Op44Kk44Oz5o6l6L+R44O75Yik5a6a54q25oWL44O744OH44O844K/5pyJ54Sh44O76ZuG5Lit5bqm44GL44KJ5L2c44KL56K66KqN6aCG44Gn44GZ44CC6Ieq5YuV5aOy6LK35oyH56S644Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPicKICAgICAgOiAnPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPue3iuaApeW6puOBrumrmOOBhOmgheebruOBr+OBguOCiuOBvuOBm+OCk+OAgumAmuW4uOebo+imluOCkue2mee2muOAgjwvcD4nCiAgKTsKfQoKZnVuY3Rpb24gcG9ydGZvbGlvTWVhbkZvcihhLGtleSl7CiAgY29uc3QgdmFsaWQ9YS5maWx0ZXIoaD0+TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCk7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw8PTApcmV0dXJuIG51bGw7CgogIGxldCBudW09MCwgZGVuPTA7CiAgdmFsaWQuZm9yRWFjaChoPT57CiAgICBjb25zdCBzdD1zdGF0Rm9yKGgsa2V5KTsKICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGlmKHN0JiZzdC5zdGF0dXM9PT0nb2snJiZOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHN0Lm1lYW4pKSYmdj4wKXsKICAgICAgbnVtICs9IHYqTnVtYmVyKHN0Lm1lYW4pOwogICAgICBkZW4gKz0gdjsKICAgIH0KICB9KTsKICByZXR1cm4gZGVuPjA/bnVtL2RlbjpudWxsOwp9CgpmdW5jdGlvbiByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBib3g9JCgncG9ydGZvbGlvU3VtbWFyeScpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwogICAgcmV0dXJuOwogIH0KCiAgbGV0IHRvdGFsQ29zdD0wLCB0b3RhbFZhbHVlPTAsIHRvdGFsTmV0PTA7CiAgY29uc3Qgcm93cz1bXTsKICBjb25zdCBkZWNpc2lvbnM9e2hvbGQ6MCx3YXRjaDowLHRha2U6MCxzdG9wOjAscGVuZGluZzowfTsKCiAgYS5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogICAgY29uc3QgY3VycmVudFZhbHVlPXZhbGlkP2N1cipzaGFyZXM6MDsKICAgIGNvbnN0IGludmVzdGVkPU51bWJlcihoLmNvc3R8fDApKnNoYXJlcytjLmJ1eUZlZTsKCiAgICB0b3RhbENvc3QgKz0gaW52ZXN0ZWQ7CgogICAgaWYodmFsaWQpewogICAgICB0b3RhbFZhbHVlICs9IGN1cnJlbnRWYWx1ZTsKICAgICAgdG90YWxOZXQgKz0gYy5uZXROb3c7CgogICAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICAgIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylkZWNpc2lvbnMuc3RvcCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yip56K65qSc6KiOJylkZWNpc2lvbnMudGFrZSsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n6K2m5oiSJylkZWNpc2lvbnMud2F0Y2grKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmSd8fGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjlsaXmrbTlj6TjgYTvvIknKWRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIGVsc2UgZGVjaXNpb25zLmhvbGQrKzsKCiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6Y3VycmVudFZhbHVlLG5ldDpjLm5ldE5vd30pOwogICAgfWVsc2V7CiAgICAgIGRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6MCxuZXQ6bnVsbH0pOwogICAgfQogIH0pOwoKICBjb25zdCBuZXRQY3Q9dG90YWxDb3N0PjA/dG90YWxOZXQvdG90YWxDb3N0KjEwMDpudWxsOwogIGNvbnN0IG1heFZhbHVlPXJvd3MucmVkdWNlKChtLHIpPT5NYXRoLm1heChtLHIudmFsdWUpLDApOwogIGNvbnN0IGNvbmNlbnRyYXRpb249dG90YWxWYWx1ZT4wP21heFZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CgogIGNvbnN0IG1lYW4yMD1wb3J0Zm9saW9NZWFuRm9yKGEsJzIwZCcpOwogIGNvbnN0IG1lYW4xMjY9cG9ydGZvbGlvTWVhbkZvcihhLCcxMjZkJyk7CiAgY29uc3QgbWVhbjI1Mj1wb3J0Zm9saW9NZWFuRm9yKGEsJzI1MmQnKTsKCiAgY29uc3Qgc3RhbGVIb2xkaW5ncz1hLmZpbHRlcihoPT57CiAgICBjb25zdCBmPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogICAgcmV0dXJuIGguYXNvZiAmJiAhZi5kZWNpc2lvbl9vazsKICB9KTsKICBjb25zdCBzdGFsZU5vdGljZT1zdGFsZUhvbGRpbmdzLmxlbmd0aAogICAgPyBgPGRpdiBjbGFzcz0iZnJlc2hib3ggZnJlc2gtc3RhbGUiIHN0eWxlPSJtYXJnaW4tYm90dG9tOjlweCI+PGI+8J+bkSDlj6TjgYTmoKrkvqHjg4fjg7zjgr8gJHtzdGFsZUhvbGRpbmdzLmxlbmd0aH3pipjmn4Q8L2I+PGRpdiBjbGFzcz0ibXV0ZWQiPuipleS+oemhjeODu+aQjeebiuOBr+acgOaWsOWPluW+l+e1guWApOODmeODvOOCueOBruWPguiAg+WApOOBp+OBmeOAguS7iuaXpeOBruWjsuiyt+WIpOaWreOBq+OBr+S9v+OCj+OBmuOAgeWun+mam+OBruePvuWcqOWApOOCkueiuuiqjeOBl+OBpuOBj+OBoOOBleOBhOOAgjwvZGl2PjwvZGl2PmAKICAgIDogJyc7CgogIGNvbnN0IHN0YWxlSGlzdG9yeT1hLmZpbHRlcihoPT57CiAgICBjb25zdCBmPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICAgIHJldHVybiAoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZikgJiYgIWYuZGVjaXNpb25fb2s7CiAgfSk7CiAgY29uc3QgaGlzdG9yeU5vdGljZT1zdGFsZUhpc3RvcnkubGVuZ3RoCiAgICA/IGA8ZGl2IGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9Im1hcmdpbi1ib3R0b206OXB4Ij48Yj7wn5OaIOWIhuaekOWxpeattOOBjOWPpOOBhCAke3N0YWxlSGlzdG9yeS5sZW5ndGh96YqY5p+EPC9iPjxkaXYgY2xhc3M9Im11dGVkIj7nj77lnKjlgKTjgYzmlrDjgZfjgY/jgabjgoLjgIHnn63kuK3plbfmnJ/jg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjg7vpnIDntaZwcm94eeOBr+mBheW7tuWxpeattOOBruWPguiAg+WApOOBp+OBmeOAgjwvZGl2PjwvZGl2PmAKICAgIDogJyc7CgogIGNvbnN0IGFsbG9jYXRpb25zPXJvd3MKICAgIC5maWx0ZXIocj0+ci52YWx1ZT4wKQogICAgLnNvcnQoKHgseSk9PnkudmFsdWUteC52YWx1ZSkKICAgIC5tYXAocj0+ewogICAgICBjb25zdCB3PXRvdGFsVmFsdWU+MD9yLnZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CiAgICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYWxsb2MiPgogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj48Yj4ke3IuY29kZX08L2I+JHtyLm5hbWU/JyAnK3IubmFtZTonJ30gLyAke3cudG9GaXhlZCgxKX0lPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0iYWxsb2NiYXIiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke01hdGgubWluKDEwMCx3KX0lIj48L3NwYW4+PC9kaXY+CiAgICAgIDwvZGl2PmA7CiAgICB9KS5qb2luKCcnKTsKCiAgYm94LmlubmVySFRNTD1gCiAgICAke3N0YWxlTm90aWNlfQogICAgJHtoaXN0b3J5Tm90aWNlfQogICAgPGRpdiBjbGFzcz0icG9ydHJvdyI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+aKleizh+mhjTwvc3Bhbj48Yj4ke3llbih0b3RhbENvc3QpfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+W5b6X57WC5YCk44OZ44O844K56KmV5L6h6aGNPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbFZhbHVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWPluW+l+e1guWApOODmeODvOOCueaQjeebijwvc3Bhbj48YiBjbGFzcz0iJHt0b3RhbE5ldD49MD8ncG9zJzonbmVnJ30iPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbE5ldCk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHtuZXRQY3Q9PT1udWxsPyfigJQnOm5ldFBjdC50b0ZpeGVkKDIpKyclJ308L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuacgOWkp+mKmOafhOavlOeOhzwvc3Bhbj48Yj4ke3RvdGFsVmFsdWU+MD9jb25jZW50cmF0aW9uLnRvRml4ZWQoMSkrJyUnOifigJQnfTwvYj48L2Rpdj4KICAgIDwvZGl2PgoKICAgIDxkaXYgY2xhc3M9InBvcnRyb3ciIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS/neaciee2mee2mjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy5ob2xkfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6K2m5oiSPC9zcGFuPjxiPiR7ZGVjaXNpb25zLndhdGNofTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K65qSc6KiOPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnRha2V9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoov5L+d55WZPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnN0b3ArZGVjaXNpb25zLnBlbmRpbmd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TiiDoqZXkvqHpoY3liqDph43jga7pgY7ljrvlubPlnYfjg6rjgr/jg7zjg7M8L2g0PgogICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+55+t5pyfMjDml6U8L3NwYW4+PGI+JHttZWFuMjA9PT1udWxsPyfigJQnOm1lYW4yMC50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7kuK3mnJ8xMjbml6U8L3NwYW4+PGI+JHttZWFuMTI2PT09bnVsbD8n4oCUJzptZWFuMTI2LnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPumVt+acnzI1MuaXpTwvc3Bhbj48Yj4ke21lYW4yNTI9PT1udWxsPyfigJQnOm1lYW4yNTIudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgPC9kaXY+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+WQhOmKmOafhOOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODs+OCkuePvuWcqOOBruipleS+oemhjeOBp+WKoOmHjeOBl+OBn+WPguiAg+WApOOBp+OBmeOAguebuOmWouOCkuiAg+aFruOBl+OBn+WwhuadpeS6iOa4rOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgNXB4Ij7wn5OmIOmKmOafhOani+aIkDwvaDQ+CiAgICAke2FsbG9jYXRpb25zfHwnPHAgY2xhc3M9Im11dGVkIj7lrp/jg4fjg7zjgr/mnKrlj5blvpc8L3A+J30KICBgOwogIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwp9CmZ1bmN0aW9uIHJlbmRlckhvbGRpbmdzKCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBpZighYS5sZW5ndGgpeyQoJ2hvbGRpbmdzJykuaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nO3JlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTtyZXR1cm59CiAgJCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9YS5tYXAoKGgsaSk9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCB2YWxpZFByaWNlPU51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZjsKICAgIGNvbnN0IGNscz12YWxpZFByaWNlPyhjLm5ldE5vdz49MD8ncG9zJzonbmVnJyk6Jyc7CiAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CgogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJob2xkaW5nIj4KICAgICAgPGRpdiBjbGFzcz0iaG9sZGluZy1oZWFkIj4KICAgICAgICA8ZGl2PgogICAgICAgICAgPGI+JHtoLmNvZGV9PC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPiR7aC5jb21wYW55X25hbWV8fCIifTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9InJvdyI+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hIb2xkaW5nKCR7aX0pIj7mm7TmlrA8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlSG9sZGluZygke2l9KSI+5YmK6ZmkPC9idXR0b24+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuePvuWcqOWApOODh+ODvOOCv+aXpSAke2guYXNvZnx8J+KAlCd9IC8g5pyA5paw5Y+W5b6X57WC5YCkICR7dmFsaWRQcmljZT95ZW4oaC5jdXJyZW50X3ByaWNlKTon4oCUJ30gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5qCq5L6h44K944O844K5ICR7aC5wcmljZV9zb3VyY2V8fCfigJQnfSAvIOWIhuaekOWxpeattCAke2guaGlzdG9yeV9hc29mfHwn4oCUJ30gJHtoLmhpc3Rvcnlfc291cmNlPycoJytoLmhpc3Rvcnlfc291cmNlKycpJzonJ308L2Rpdj4KICAgICAgJHt2YWxpZFByaWNlP2A8ZGl2IGNsYXNzPSJmcmVzaGJveCAke2ZyZXNobmVzc0ZvcihoLmFzb2YpLmNsc30iPjxiPiR7CiAgICAgICAgZnJlc2huZXNzRm9yKGguYXNvZikubGV2ZWw9PT0nZnJlc2gnPyfinIUg6a6u5bqmT0snOgogICAgICAgIGZyZXNobmVzc0ZvcihoLmFzb2YpLmxldmVsPT09J3dhcm5pbmcnPyfimqDvuI8g6YGF5bu25rOo5oSPJzoKICAgICAgICAn8J+bkSDlj6TjgYTmoKrkvqHjg4fjg7zjgr8nCiAgICAgIH08L2I+PGRpdiBjbGFzcz0ibXV0ZWQiPiR7ZnJlc2huZXNzVGV4dChoLmFzb2YpfTwvZGl2PjwvZGl2PmA6Jyd9CgogICAgICA8ZGl2IGNsYXNzPSJkZWNpc2lvbiAke2QuY2xzfSI+CiAgICAgICAgJHtkLmxhYmVsfQogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo0cHgiPiR7ZC5yZWFzb259PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KK44G+44GnPC9zcGFuPgogICAgICAgICAgPGIgY2xhc3M9ImRpc3RhbmNlIj4ke3ZhbGlkUHJpY2U/ZGlzdGFuY2VJbmZvKGN1cixjLnN0b3BQcmljZSwnc3RvcCcpOifigJQnfTwvYj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+C6ICDICR7eWVuKGMuc3RvcFByaWNlKX08L3NwYW4+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K644G+44GnPC9zcGFuPgogICAgICAgICAgPGIgY2xhc3M9ImRpc3RhbmNlIj4ke3ZhbGlkUHJpY2U/ZGlzdGFuY2VJbmZvKGN1cixjLnRha2VQcmljZSwndGFrZScpOifigJQnfTwvYj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+C6ICDICR7eWVuKGMudGFrZVByaWNlKX08L3NwYW4+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Yik5a6a5L+h6aC85bqmPC9zcGFuPgogICAgICAgICAgPGI+JHtkLmNvbmZpZGVuY2V9JTwvYj4KICAgICAgICAgIDxkaXYgY2xhc3M9ImdhdWdlIj48c3BhbiBzdHlsZT0id2lkdGg6JHtkLmNvbmZpZGVuY2V9JSI+PC9zcGFuPjwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NXB4Ij7igLvliKTlrprkv6HpoLzluqbjga/jgIHjg4fjg7zjgr/lhYXotrPluqbjg7vmnJ/plpPjg4jjg6zjg7Pjg4njga7kuIDoh7Tluqbjg7vmnJ/lvoXlgKTntbHoqIjjga7mnInnhKHjgYvjgonkvZzjgovlj4LogIPmjIfmqJnjgafjgIHnmoTkuK3norrnjofjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+CgogICAgICA8ZGl2IGNsYXNzPSJhY3Rpb25ib3giPgogICAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5LuK44Gp44GG44GZ44KL77yfPC9zcGFuPgogICAgICAgIDxiIHN0eWxlPSJkaXNwbGF5OmJsb2NrO21hcmdpbi10b3A6M3B4Ij4ke2FjdGlvblRleHQoaCxjLGQpfTwvYj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+W5b6X57WC5YCk44OZ44O844K55pCN55uKPC9zcGFuPjxiIGNsYXNzPSIke2Nsc30iPiR7dmFsaWRQcmljZT95ZW4oYy5uZXROb3cpOifigJQnfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dmFsaWRQcmljZT9mbXQoYy5uZXROb3dQY3QpKyclJzon4oCUJ308L3NwYW4+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuiyt+S7mOaJi+aVsOaWmTwvc3Bhbj48Yj4ke3llbihjLmJ1eUZlZSl9PC9iPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lo7LljbTmiYvmlbDmlpko5LuKKTwvc3Bhbj48Yj4ke3ZhbGlkUHJpY2U/eWVuKGMuc2VsbEZlZSk6J+KAlCd9PC9iPjwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy5zdG9wUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnN0b3BOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K65Y+C6ICDPC9zcGFuPjxiPiR7eWVuKGMudGFrZVByaWNlKX08L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrwgJHt5ZW4oYy50YWtlTmV0KX08L3NwYW4+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODquODs+OCsOWPguiAgzwvc3Bhbj48Yj4ke3ZhbGlkUHJpY2U/eWVuKGMudHJhaWxQcmljZSk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6Xpq5jlgKTln7rmupY8L3NwYW4+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+OryDlrp/nuL7jg5njg7zjgrnkvqHmoLzjg6zjg7Pjgrg8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtyYW5nZUh0bWwoJ+efreacnyAyMOaXpScsY3VyLHN0YXRGb3IoaCwnMjBkJykpfQogICAgICAgICR7cmFuZ2VIdG1sKCfkuK3mnJ8gMTI25pelJyxjdXIsc3RhdEZvcihoLCcxMjZkJykpfQogICAgICAgICR7cmFuZ2VIdG1sKCfplbfmnJ8gMjUy5pelJyxjdXIsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KCiAgICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA3cHgiPvCfk5Ag5a6f57i+44OZ44O844K55pyf5b6F5YCk77yI57Wx6KiI5Y+C6ICD77yJPC9oND4KICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICAgICR7ZXZIdG1sKCfnn63mnJ8gMjDml6UnLHN0YXRGb3IoaCwnMjBkJykpfQogICAgICAgICR7ZXZIdG1sKCfkuK3mnJ8gMTI25pelJyxzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+mVt+acnyAyNTLml6UnLHN0YXRGb3IoaCwnMjUyZCcpKX0KICAgICAgPC9kaXY+CiAgICAgIDxwIGNsYXNzPSJtdXRlZCI+4oC75L6h5qC844Os44Oz44K444O75pyf5b6F5YCk44Gv5bCG5p2l5LqI5ris44Gn44Gv44Gq44GP44CB5Y+W5b6X5Y+v6IO944Gq6YGO5Y675qCq5L6h44Gu44Ot44O844Oq44Oz44Kw5YmN5pa544Oq44K/44O844Oz5YiG5biD44KS5pyA5paw5Y+W5b6X57WC5YCk44Gr5b2T44Gm44Gv44KB44Gf57Wx6KiI5Y+C6ICD44Gn44GZ44CC5pyf6ZaT44GM6YeN44Gq44KL5qiZ5pys44KS5ZCr44G/44G+44GZ44CCPC9wPgogICAgPC9kaXY+YDsKICB9KS5qb2luKCcnKTsKICByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldFF1b3RlKGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3F1b3RlP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCBxPWF3YWl0IHIuanNvbigpOwogIGlmKHEuc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IocS5yZWFzb258fHEuZXJyb3J8fCflrp/jg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4gcTsKfQoKYXN5bmMgZnVuY3Rpb24gYWRkSG9sZGluZygpewogIGNvbnN0IGNvZGU9JCgnaG9sZENvZGUnKS52YWx1ZS50cmltKCk7CiAgY29uc3QgY29zdD12YWwoJ2hvbGRDb3N0JyksIHNoYXJlcz12YWwoJ2hvbGRTaGFyZXMnKTsKICBjb25zdCBzdG9wPXZhbCgnc3RvcFBjdCcpLCB0YWtlPXZhbCgndGFrZVBjdCcpLCB0cmFpbD12YWwoJ3RyYWlsUGN0Jyk7CiAgY29uc3QgZmVlTW9kZT0kKCdmZWVNb2RlJykudmFsdWU7CiAgaWYoIWNvZGV8fCFjb3N0fHwhc2hhcmVzKXthbGVydCgn6YqY5p+E44Kz44O844OJ44O75Y+W5b6X5Y2Y5L6h44O75qCq5pWw44KS5YWl5Yqb44GX44Gm44GtJyk7cmV0dXJufQogIGNvbnN0IGJ0bj1ldmVudD8udGFyZ2V0OyBpZihidG4pe2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJ30KICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICAgIGNvbnN0IGg9ewogICAgICBjb2RlLAogICAgICBjb21wYW55X25hbWU6KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHwnJywKICAgICAgY29tcGFueV9tYXJrZXQ6KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fCcnLAogICAgICBjb21wYW55X3NlY3RvcjMzOihxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fCcnLAogICAgICBjb3N0LCBzaGFyZXMsIGZlZV9tb2RlOmZlZU1vZGUsCiAgICAgIHN0b3BfcGN0OnN0b3A/PzgsIHRha2VfcGN0OnRha2U/PzE1LCB0cmFpbF9wY3Q6dHJhaWw/PzcsCiAgICAgIGN1cnJlbnRfcHJpY2U6cy5sYXN0X2Nsb3NlLCBoaWdoXzIwZDpzLmhpZ2hfMjBkLCBsb3dfMjBkOnMubG93XzIwZCwKICAgICAgcmV0dXJuXzIwZDpzLnJldHVybl8yMGQsIHJldHVybl8xMjZkOnMucmV0dXJuXzEyNmQsIHJldHVybl8yNTJkOnMucmV0dXJuXzI1MmQsCiAgICAgIGZvcndhcmRfc3RhdHM6cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30sCiAgICAgIGFzb2Y6cy5sYXN0X2RhdGUsCiAgICAgIGhpc3RvcnlfYXNvZjpzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSwKICAgICAgcHJpY2Vfc291cmNlOnMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8JycsCiAgICAgIGhpc3Rvcnlfc291cmNlOnMuaGlzdG9yeV9zb3VyY2V8fCcnLAogICAgICB1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKQogICAgfTsKICAgIGNvbnN0IGlkeD1hLmZpbmRJbmRleCh4PT54LmNvZGU9PT1jb2RlKTsKICAgIGlmKGlkeD49MClhW2lkeF09aDsgZWxzZSBhLnB1c2goaCk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSl9CiAgZmluYWxseXtpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmCd9fQp9Cgphc3luYyBmdW5jdGlvbiByZWZyZXNoSG9sZGluZyhpKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpLCBoPWFbaV07IGlmKCFoKXJldHVybjsKICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGguY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBoLmNvbXBhbnlfbmFtZT0ocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fGguY29tcGFueV9uYW1lfHwnJzsKICAgIGguY29tcGFueV9tYXJrZXQ9KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fGguY29tcGFueV9tYXJrZXR8fCcnOwogICAgaC5jb21wYW55X3NlY3RvcjMzPShxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fGguY29tcGFueV9zZWN0b3IzM3x8Jyc7CiAgICBoLmN1cnJlbnRfcHJpY2U9cy5sYXN0X2Nsb3NlOyBoLmhpZ2hfMjBkPXMuaGlnaF8yMGQ7IGgubG93XzIwZD1zLmxvd18yMGQ7CiAgICBoLnJldHVybl8yMGQ9cy5yZXR1cm5fMjBkOyBoLnJldHVybl8xMjZkPXMucmV0dXJuXzEyNmQ7IGgucmV0dXJuXzI1MmQ9cy5yZXR1cm5fMjUyZDsKICAgIGguZm9yd2FyZF9zdGF0cz1zLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fTsKICAgIGguYXNvZj1zLmxhc3RfZGF0ZTsKICAgIGguaGlzdG9yeV9hc29mPXMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlOwogICAgaC5wcmljZV9zb3VyY2U9cy5wcmljZV9zb3VyY2V8fHEuc291cmNlfHwnJzsKICAgIGguaGlzdG9yeV9zb3VyY2U9cy5oaXN0b3J5X3NvdXJjZXx8Jyc7CiAgICBoLnVwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogICAgYVtpXT1oOyBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7IHJlbmRlckhvbGRpbmdzKCk7CiAgfWNhdGNoKGUpe2FsZXJ0KCfmm7TmlrDjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KfQpmdW5jdGlvbiByZW1vdmVIb2xkaW5nKGkpe2NvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7YS5zcGxpY2UoaSwxKTtzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7cmVuZGVySG9sZGluZ3MoKX0KCmZ1bmN0aW9uIHJlbmRlcldhdGNoKCl7CiAgJCgnd2F0Y2hzJykuaW5uZXJIVE1MPWxvY2FsKCdmcmVlX3dhdGNoJykubWFwKHg9PnsKICAgIGlmKHR5cGVvZiB4PT09J3N0cmluZycpcmV0dXJuIGA8ZGl2IGNsYXNzPSJiYWRnZSI+JHt4fTwvZGl2PmA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImJhZGdlIj48Yj4ke3guY29kZX08L2I+JHt4Lm5hbWU/JyAnK3gubmFtZTonJ308L2Rpdj5gOwogIH0pLmpvaW4oJycpfHwnPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JzsKfQphc3luYyBmdW5jdGlvbiBhZGRXYXRjaCgpewogIGxldCBjPSQoJ3dhdGNoQ29kZScpLnZhbHVlLnRyaW0oKTsgaWYoIWMpcmV0dXJuOwogIGxldCBpbmZvPW51bGw7CiAgdHJ5e2luZm89YXdhaXQgZ2V0Q29tcGFueShjKX1jYXRjaChlKXt9CiAgbGV0IGE9bG9jYWwoJ2ZyZWVfd2F0Y2gnKTsKICBjb25zdCBleGlzdHM9YS5zb21lKHg9Pih0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKT09PWMpOwogIGlmKCFleGlzdHMpYS5wdXNoKHtjb2RlOmMsbmFtZTppbmZvJiZpbmZvLm5hbWU/aW5mby5uYW1lOicnfSk7CiAgc2F2ZSgnZnJlZV93YXRjaCcsYSk7CiAgcmVuZGVyV2F0Y2goKTsKfQpmdW5jdGlvbiB1cGRhdGVLYWJ1dGFuKCl7bGV0IGM9JCgnY29kZScpLnZhbHVlLnRyaW0oKTskKCdrYWJ1dGFuJykuaHJlZj1jPydodHRwczovL2thYnV0YW4uanAvc3RvY2svP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYyk6J2h0dHBzOi8va2FidXRhbi5qcC8nfQokKCdjb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT57dXBkYXRlS2FidXRhbigpO3NjaGVkdWxlQ29tcGFueUxvb2t1cCgnY29kZScsJ2NvbXBhbnlOYW1lJywnJyl9KTt1cGRhdGVLYWJ1dGFuKCk7CiQoJ2hvbGRDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2hvbGRDb2RlJywnaG9sZENvbXBhbnlOYW1lJywnJykpOwokKCd3YXRjaENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnd2F0Y2hDb2RlJywnd2F0Y2hDb21wYW55TmFtZScsJycpKTsKCgoKYXN5bmMgZnVuY3Rpb24gZ2V0UG9saWN5KGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3BvbGljeT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5Zu9562W44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgcmV0dXJuIHgucG9saWN5fHx7fTsKfQoKZnVuY3Rpb24gcG9saWN5U291cmNlU3RhdHVzSmEocyl7CiAgaWYocz09PSd2ZXJpZmllZF9saXZlJylyZXR1cm4gJ+WFrOW8j+ODmuODvOOCuOeiuuiqjea4iCc7CiAgaWYocz09PSdwYXJ0aWFsX2xpdmUnKXJldHVybiAn5YWs5byP44Oa44O844K46YOo5YiG56K66KqNJzsKICBpZihzPT09J3ZlcmlmaWVkX3JlZ2lzdHJ5JylyZXR1cm4gJ+acgOe1gueiuuiqjea4iOWFrOW8j+OCveODvOOCuSc7CiAgcmV0dXJuICfnorroqo3kuI3lj68nOwp9CgpmdW5jdGlvbiByZW5kZXJQb2xpY3lUaGVtZXMocCl7CiAgY29uc3QgYm94PSQoJ3BvbGljeVRoZW1lcycpOwogIGlmKCFib3gpcmV0dXJuOwogIGNvbnN0IHRoZW1lcz0ocCYmcC5tYXRjaGVkX3RoZW1lcyl8fFtdOwogIGlmKCF0aGVtZXMubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxkaXYgY2xhc3M9InBvbGljeXRoZW1lIj48Yj7plqLpgKPjg4bjg7zjg57jgarjgZcgLyDliKTlrprkv53nlZk8L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7mnIDkvY7plqLpgKPluqbjgpLmuoDjgZ/jgZnlhazlvI/mlL/nrZbjg4bjg7zjg57jgYzjgYLjgorjgb7jgZvjgpPjgII8L3NwYW4+PC9kaXY+JzsKICAgIHJldHVybjsKICB9CiAgYm94LmlubmVySFRNTD10aGVtZXMuc2xpY2UoMCw0KS5tYXAodD0+YAogICAgPGRpdiBjbGFzcz0icG9saWN5dGhlbWUiPgogICAgICA8Yj4ke3QubmFtZX0gLyDplqLpgKPluqYgJHsoTnVtYmVyKHQucmVsZXZhbmNlKSoxMDApLnRvRml4ZWQoMCl9JTwvYj4KICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mlL/nrZblvLfluqYgJHtOdW1iZXIodC5wb2xpY3lfc3RyZW5ndGgpLnRvRml4ZWQoMCl9IC8g5a+E5LiOICR7TnVtYmVyKHQuY29udHJpYnV0aW9uKS50b0ZpeGVkKDEpfSAvICR7cG9saWN5U291cmNlU3RhdHVzSmEodC5zb3VyY2Vfc3RhdHVzKX08L3NwYW4+PGJyPgogICAgICA8YSBocmVmPSIke3QudXJsfSIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPuWFrOW8j+OCveODvOOCuTwvYT4KICAgIDwvZGl2PgogIGApLmpvaW4oJycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRGdW5kYW1lbnRhbHMoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvZnVuZGFtZW50YWxzP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfmsbrnrpfjg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4geC5mdW5kYW1lbnRhbHN8fHt9Owp9CgpmdW5jdGlvbiBzY29yZUxhYmVsKHYpewogIGlmKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSlyZXR1cm4gJ+KAlCc7CiAgY29uc3Qgbj1OdW1iZXIodik7CiAgcmV0dXJuIChuPjA/JysnOicnKStuLnRvRml4ZWQoMSk7Cn0KCmZ1bmN0aW9uIHBjdE1heWJlKHYpewogIHJldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgxKSsnJSc7Cn0KCmFzeW5jIGZ1bmN0aW9uIGFuYWx5emUoKXsKICBjb25zdCBjb2RlPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7CiAgaWYoIWNvZGUpeyQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfpipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjga0nO3JldHVybn0KICBjb25zdCBidG49JCgnYW5hbHl6ZUJ0bicpOyBidG4uZGlzYWJsZWQ9dHJ1ZTsgYnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnOwogICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSdGaW5NaW5k54Sh5paZ5pel6Laz77yLSi1RdWFudHPliIbmnpDjg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKYnOwoKICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgJCgncHJpY2UnKS52YWx1ZT1zLmxhc3RfY2xvc2U9PW51bGw/Jyc6Zm10KHMubGFzdF9jbG9zZSwxKTsKICAgICQoJ3IyMCcpLnZhbHVlPWZtdChzLnJldHVybl8yMGQpOyQoJ3IxMjYnKS52YWx1ZT1mbXQocy5yZXR1cm5fMTI2ZCk7JCgncjI1MicpLnZhbHVlPWZtdChzLnJldHVybl8yNTJkKTsKICAgICQoJ2hpZ2gyMCcpLnRleHRDb250ZW50PWZtdChzLmhpZ2hfMjBkLDEpOyQoJ2xvdzIwJykudGV4dENvbnRlbnQ9Zm10KHMubG93XzIwZCwxKTsKICAgICQoJ3ZvbDIwJykudGV4dENvbnRlbnQ9cy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkPT1udWxsPyfigJQnOmZtdChzLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQpKyclJzsKCiAgICBkaXNwbGF5Q29tcGFueSgkKCdjb21wYW55TmFtZScpLHEuY29tcGFueXx8bnVsbCwnJyk7CiAgICBjb25zdCBwcmljZVNvdXJjZT1zLnByaWNlX3NvdXJjZXx8cS5zb3VyY2V8fCfkuI3mmI4nOwogICAgY29uc3QgaGlzdG9yeURhdGU9cy5oaXN0b3J5X2xhc3RfZGF0ZXx8bnVsbDsKICAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0KICAgICAgJzxzcGFuIGNsYXNzPSJzb3VyY2ViYWRnZSI+54++5Zyo5YCkPC9zcGFuPjxiIGNsYXNzPSJvayI+JytwcmljZVNvdXJjZSsnPC9iPicrCiAgICAgICc8YnI+54++5Zyo5YCk44OH44O844K/5pelOiAnKyhzLmxhc3RfZGF0ZXx8J+KAlCcpKwogICAgICAocy5wcmljZV90aW1lPycgJytzLnByaWNlX3RpbWU6JycpKwogICAgICAnIC8g5pyA5paw5Y+W5b6X5YCkOiAnK2ZtdChzLmxhc3RfY2xvc2UsMSkrCiAgICAgICc8YnI+PHNwYW4gY2xhc3M9InNvdXJjZWJhZGdlIj7liIbmnpDlsaXmrbQ8L3NwYW4+JysKICAgICAgKHMuaGlzdG9yeV9zb3VyY2V8fCflj5blvpfjgarjgZcnKSsKICAgICAgJyAvIOacgOe1guaXpTogJysoaGlzdG9yeURhdGV8fCfigJQnKSsKICAgICAgJyAvIOWxpeattOOCteODs+ODl+ODqzogJysocy5zYW1wbGVfY291bnQ/PzApKyfku7YnOwogICAgc2hvd0ZyZXNobmVzcyhzLmxhc3RfZGF0ZSk7CiAgICBzaG93SGlzdG9yeUZyZXNobmVzcyhoaXN0b3J5RGF0ZSxzLmxhc3RfZGF0ZSk7CiAgICAkKCdhbmFseXNpc0V2JykuaW5uZXJIVE1MPQogICAgICBldkh0bWwoJ+efreacnzIw5pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycyMGQnXSkrCiAgICAgIGV2SHRtbCgn5Lit5pyfMTI25pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycxMjZkJ10pKwogICAgICBldkh0bWwoJ+mVt+acnzI1MuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjUyZCddKTsKCiAgICBjb25zdCBzdXBwbHk9cy5zdXBwbHlfcHJveHl8fHt9OwogICAgJCgnc3VwcGx5QXV0bycpLnRleHRDb250ZW50PXNjb3JlTGFiZWwoc3VwcGx5LnNjb3JlKTsKICAgICQoJ3N1cHBseURldGFpbCcpLnRleHRDb250ZW50PQogICAgICAnNeaXpS8yMOaXpeWHuuadpemrmCAnKyhzdXBwbHkudm9sdW1lX3JhdGlvXzVfMjA9PW51bGw/J+KAlCc6TnVtYmVyKHN1cHBseS52b2x1bWVfcmF0aW9fNV8yMCkudG9GaXhlZCgyKSsn5YCNJyk7CgogICAgbGV0IGZ1bmRhbWVudGFscz17fTsKICAgIHRyeXsKICAgICAgZnVuZGFtZW50YWxzPWF3YWl0IGdldEZ1bmRhbWVudGFscyhjb2RlKTsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKGZ1bmRhbWVudGFscy5zY29yZSk7CiAgICAgIGNvbnN0IG09ZnVuZGFtZW50YWxzLm1ldHJpY3N8fHt9OwogICAgICAkKCdlYXJuRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgJ+mWi+ekuiAnKygoZnVuZGFtZW50YWxzLmxhdGVzdCYmZnVuZGFtZW50YWxzLmxhdGVzdC5kYXRlKXx8J+KAlCcpKwogICAgICAgICcgLyDlo7LkuIogJytwY3RNYXliZShtLnNhbGVzX2dyb3d0aF9wY3QpKwogICAgICAgICcgLyDllrbmpa3nm4ogJytwY3RNYXliZShtLm9wX2dyb3d0aF9wY3QpOwogICAgfWNhdGNoKGZlKXsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD0n5LiN5piOJzsKICAgICAgJCgnZWFybkRldGFpbCcpLnRleHRDb250ZW50PSfjgZPjga7jg5fjg6njg7Mv6YqY5p+E44Gn44Gv5Y+W5b6X44Gn44GN44Gq44GE5Y+v6IO95oCn44GC44KKJzsKICAgICAgZnVuZGFtZW50YWxzPXtzY29yZTpudWxsfTsKICAgIH0KCiAgICBsZXQgYXV0b1BvbGljeT17c2NvcmU6bnVsbCxtYXRjaGVkX3RoZW1lczpbXX07CiAgICB0cnl7CiAgICAgIGF1dG9Qb2xpY3k9YXdhaXQgZ2V0UG9saWN5KGNvZGUpOwogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PWF1dG9Qb2xpY3kuc2NvcmU9PW51bGw/J+S4jeaYjic6c2NvcmVMYWJlbChhdXRvUG9saWN5LnNjb3JlKTsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgYXV0b1BvbGljeS5zY29yZT09bnVsbAogICAgICAgICAgPyAn6Zai6YCj44GZ44KL5YWs5byP5pS/562W44OG44O844Oe44Gq44GXJwogICAgICAgICAgOiAn6Ieq5YuV5Zu9562WcHJveHkgLyDkv6HpoLzluqYgJysoYXV0b1BvbGljeS5jb25maWRlbmNlX3BjdD8/J+KAlCcpKyclIC8gJysoKGF1dG9Qb2xpY3kubWF0Y2hlZF90aGVtZXN8fFtdKS5sZW5ndGgpKyfjg4bjg7zjg54nOwogICAgICByZW5kZXJQb2xpY3lUaGVtZXMoYXV0b1BvbGljeSk7CiAgICB9Y2F0Y2gocGUpewogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PSfkuI3mmI4nOwogICAgICAkKCdwb2xpY3lEZXRhaWwnKS50ZXh0Q29udGVudD0n5YWs5byP5pS/562W44K944O844K55Y+W5b6X44Ko44Op44O8JzsKICAgICAgJCgncG9saWN5VGhlbWVzJykuaW5uZXJIVE1MPScnOwogICAgICBhdXRvUG9saWN5PXtzY29yZTpudWxsLG1hdGNoZWRfdGhlbWVzOltdfTsKICAgIH0KCiAgICBjb25zdCBtYW51YWxQb2xpY3k9dmFsKCdwb2xpY3knKTsKICAgIGNvbnN0IHBvbGljeVNjb3JlPW1hbnVhbFBvbGljeT09PW51bGw/YXV0b1BvbGljeS5zY29yZTptYW51YWxQb2xpY3k7CiAgICBjb25zdCBwb2xpY3lNb2RlPW1hbnVhbFBvbGljeT09PW51bGw/J2F1dG8nOidtYW51YWwnOwogICAgaWYobWFudWFsUG9saWN5IT09bnVsbCl7CiAgICAgICQoJ3BvbGljeVN0YXRlJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChtYW51YWxQb2xpY3kpOwogICAgICAkKCdwb2xpY3lEZXRhaWwnKS50ZXh0Q29udGVudD0n5omL5YWl5Yqb44Gn6Ieq5YuV5YCk44KS5LiK5pu444GNJzsKICAgIH0KCiAgICBjb25zdCBkPXsKICAgICAgY29kZSwKICAgICAgcHJpY2U6cy5sYXN0X2Nsb3NlLAogICAgICByZXR1cm4yMDpzLnJldHVybl8yMGQsCiAgICAgIHJldHVybjEyNjpzLnJldHVybl8xMjZkLAogICAgICByZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCwKICAgICAgZWFybmluZ3Nfc2NvcmU6ZnVuZGFtZW50YWxzLnNjb3JlLAogICAgICBwb2xpY3lfc2NvcmU6cG9saWN5U2NvcmUsCiAgICAgIHBvbGljeV9tb2RlOnBvbGljeU1vZGUsCiAgICAgIHN1cHBseV9zY29yZTpzdXBwbHkuc2NvcmUKICAgIH07CgogICAgY29uc3QgYXI9YXdhaXQgZmV0Y2goJy9hcGkvZnJlZS9hbmFseXplJyx7CiAgICAgIG1ldGhvZDonUE9TVCcsCiAgICAgIGhlYWRlcnM6eydDb250ZW50LVR5cGUnOidhcHBsaWNhdGlvbi9qc29uJ30sCiAgICAgIGJvZHk6SlNPTi5zdHJpbmdpZnkoZCksCiAgICAgIGNhY2hlOiduby1zdG9yZScKICAgIH0pOwogICAgY29uc3QgeD1hd2FpdCBhci5qc29uKCk7CiAgICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5YiG5p6Q44OH44O844K/44GM5LiN6Laz44GX44Gm44GE44G+44GZJyk7CgogICAgJCgnc3RhdGUnKS50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTsKICAgICQoJ3BvcycpLnRleHRDb250ZW50PXguc2lnbmFsLnBvc2l0aXZlX2NvdW50OwogICAgJCgnbmVnJykudGV4dENvbnRlbnQ9eC5zaWduYWwubmVnYXRpdmVfY291bnQ7CgogICAgY29uc3Qgc2M9eC5zY29yZXx8e307CiAgICAkKCdzY29yZUhlcm8nKS5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgICAkKCdzY29yZTEwMCcpLnRleHRDb250ZW50PXNjLnNjb3JlMTAwPT1udWxsPyfigJQnOnNjLnNjb3JlMTAwKycgLyAxMDAnOwoKICAgIGNvbnN0IHBpbGw9JCgnc2NvcmVTdGF0ZVBpbGwnKTsKICAgIHBpbGwuY2xhc3NOYW1lPSdzdGF0ZXBpbGwgJytzdGF0ZUNsYXNzKHguc2lnbmFsLnN0YXRlKTsKICAgIHBpbGwudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7CgogICAgJCgnY292ZXJhZ2UnKS50ZXh0Q29udGVudD1zYy5jb3ZlcmFnZV9wY3Q9PW51bGw/J+KAlCc6c2MuY292ZXJhZ2VfcGN0KyclJzsKICAgICQoJ3Njb3JlQnJlYWtkb3duJykudGV4dENvbnRlbnQ9CiAgICAgICfjg4bjgq/jg4vjgqvjg6sgJytzY29yZUxhYmVsKHNjLnRlY2huaWNhbCkrCiAgICAgICcgLyDmsbrnrpcgJytzY29yZUxhYmVsKHNjLmVhcm5pbmdzKSsKICAgICAgJyAvIOmcgOe1piAnK3Njb3JlTGFiZWwoc2Muc3VwcGx5KSsKICAgICAgJyAvIOWbveetlnByb3h5ICcrc2NvcmVMYWJlbChzYy5wb2xpY3kpOwoKICAgICQoJ3Njb3JlUmVhc29uJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmVSZWFzb25UZXh0JykudGV4dENvbnRlbnQ9ZHJpdmVyU2VudGVuY2Uoc2MpOwogICAgJCgnZHJpdmVyR3JpZCcpLmlubmVySFRNTD0KICAgICAgZHJpdmVyQm94SHRtbCgn5pyA5aSn44Gu44OX44Op44K56KaB5ZugJyxzYy5zdHJvbmdlc3RfcG9zaXRpdmUsJ3Bvc2l0aXZlJykrCiAgICAgIGRyaXZlckJveEh0bWwoJ+acgOWkp+OBruODnuOCpOODiuOCueimgeWboCcsc2Muc3Ryb25nZXN0X25lZ2F0aXZlLCduZWdhdGl2ZScpOwoKICAgIHJlbmRlckNvbnRyaWJ1dGlvbnMoc2MpOwoKICAgIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihzLmxhc3RfZGF0ZSk7CiAgICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlKTsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PQogICAgICAhZnJlc2guZGVjaXNpb25fb2sKICAgICAgICA/ICfimqDvuI8g54++5Zyo5YCk44OH44O844K/44GM5Y+k44GE44Gf44KB44CB5LuK5pel44Gu5aOy6LK35Yik5pat44Go44GX44Gm44Gv5L2/55So44GX44G+44Gb44KT44CCJwogICAgICAgIDogIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vawogICAgICAgICAgPyAn4pqg77iPIOePvuWcqOWApOOBr0Zpbk1pbmTnrYnjga7mlrDjgZfjgYTml6XotrPjgpLkvb/jgaPjgabjgYTjgb7jgZnjgYzjgIHnt4/lkIjngrnjg7vnn63kuK3plbfmnJ/jg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjga9KLVF1YW50c+OBrumBheW7tuOBl+OBn+WIhuaekOWxpeattOOCkuWQq+OCgOWPguiAg+WApOOBp+OBmeOAguWbuuWumuOBruaQjeWIh+OCii/liKnnorrjg6njgqTjg7Pnorroqo3jgpLlhKrlhYjjgZfjgabjgY/jgaDjgZXjgYTjgIInCiAgICAgICAgICA6ICfnj77lnKjlgKTjgajliIbmnpDlsaXmrbTjga7prq7luqbjgpLnorroqo3muIjjgb/jgILnirbmhYvooajnpLrjga/nt4/lkIjngrnjgavpgKPli5XjgZfjgb7jgZnjgIInOwogIH1jYXRjaChlKXsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfimqDvuI8gJytlLm1lc3NhZ2U7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9JzxzcGFuIGNsYXNzPSJlcnIiPuWPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UrJzwvc3Bhbj4nOwogIH1maW5hbGx5ewogICAgYnRuLmRpc2FibGVkPWZhbHNlOwogICAgYnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafliIbmnpAnOwogIH0KfQoKcmVuZGVySG9sZGluZ3MoKTtyZW5kZXJXYXRjaCgpOwppZignc2VydmljZVdvcmtlcicgaW4gbmF2aWdhdG9yKXtuYXZpZ2F0b3Iuc2VydmljZVdvcmtlci5nZXRSZWdpc3RyYXRpb25zKCkudGhlbihycz0+UHJvbWlzZS5hbGwocnMubWFwKHI9PnIudW5yZWdpc3RlcigpKSkpLmNhdGNoKCgpPT57fSl9CmlmKCdjYWNoZXMnIGluIHdpbmRvdyl7Y2FjaGVzLmtleXMoKS50aGVuKGtleXM9PlByb21pc2UuYWxsKGtleXMubWFwKGs9PmNhY2hlcy5kZWxldGUoaykpKSkuY2F0Y2goKCk9Pnt9KX0KPC9zY3JpcHQ+CjwvbWFpbj4KPC9ib2R5Pgo8L2h0bWw+"
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
