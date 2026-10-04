from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64, csv, io
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.18-FACTOR-FRESHNESS'
SECURITY_CACHE={}
POLICY_SOURCE_CACHE={}
FINMIND_HISTORY_CACHE={}

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


def finmind_request_rows(code, days=900):
    symbol = finmind_symbol(code)
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=int(days))

    cache_key = symbol + ":" + str(days)
    now_ts = datetime.now(timezone.utc).timestamp()
    cached = FINMIND_HISTORY_CACHE.get(cache_key)
    if cached and now_ts - cached.get("ts", 0) < 1200:
        return cached["rows"]

    headers = {
        "User-Agent": "JapanStockAIFree/1.17",
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
        timeout=18,
    )
    if r.status_code != 200:
        raise RuntimeError("FinMind HTTP " + str(r.status_code))

    payload = r.json()
    status = payload.get("status")
    if status not in (None, 0, 200, "200"):
        raise RuntimeError(
            "FinMind API: " + str(payload.get("msg") or status)
        )

    raw_rows = payload.get("data") or []
    if not raw_rows:
        raise RuntimeError("FinMind returned no JapanStockPrice rows")

    rows = sorted(
        raw_rows,
        key=lambda x: str(x.get("date") or ""),
    )
    FINMIND_HISTORY_CACHE[cache_key] = {
        "ts": now_ts,
        "rows": rows,
    }
    return rows


def normalize_finmind_rows(raw_rows, code):
    out = []

    def num(x):
        if x in (None, "", "N/D"):
            return None
        try:
            return float(x)
        except Exception:
            return None

    for x in raw_rows:
        raw_close = num(x.get("Close"))
        adj_close = num(x.get("Adj_Close"))
        if raw_close is None and adj_close is None:
            continue

        close = adj_close if adj_close is not None else raw_close
        factor = (
            adj_close / raw_close
            if (
                adj_close is not None
                and raw_close not in (None, 0)
            )
            else 1.0
        )

        raw_open = num(x.get("Open"))
        raw_high = num(x.get("High"))
        raw_low = num(x.get("Low"))

        out.append({
            "date": str(x.get("date") or "")[:10],
            "code": str(code),
            "open": raw_open * factor if raw_open is not None else None,
            "high": raw_high * factor if raw_high is not None else None,
            "low": raw_low * factor if raw_low is not None else None,
            "close": close,
            "volume": num(x.get("Volume")),
            "turnover": None,
            "raw_open": raw_open,
            "raw_high": raw_high,
            "raw_low": raw_low,
            "raw_close": raw_close if raw_close is not None else close,
            "adjusted_close": adj_close,
            "adjustment_factor": factor,
            "source": "FinMind JapanStockPrice",
        })

    out = [r for r in out if r["date"] and r.get("close") is not None]
    out.sort(key=lambda x: x["date"])
    return out


def finmind_history(code, days=900):
    raw = finmind_request_rows(code, days=days)
    rows = normalize_finmind_rows(raw, code)
    if not rows:
        raise RuntimeError("FinMind history normalization returned no rows")
    return rows


def finmind_quote(code):
    rows = finmind_history(code, days=45)
    row = rows[-1]

    return {
        "symbol": finmind_symbol(code),
        "date": row["date"],
        "time": None,
        "open": row.get("raw_open"),
        "high": row.get("raw_high"),
        "low": row.get("raw_low"),
        "close": row.get("raw_close"),
        "adj_close": row.get("adjusted_close"),
        "volume": row.get("volume"),
        "source": "FinMind JapanStockPrice",
        "mode": "free_daily_eod",
        "registered_token": bool(os.getenv("FINMIND_TOKEN")),
    }


def finmind_snapshot(code, days=900):
    rows = finmind_history(code, days=days)
    snap = technical_snapshot(rows)

    latest = rows[-1]
    recent = rows[-20:] if len(rows) >= 20 else rows
    raw_highs = [
        r.get("raw_high")
        for r in recent
        if isinstance(r.get("raw_high"), (int, float))
    ]
    raw_lows = [
        r.get("raw_low")
        for r in recent
        if isinstance(r.get("raw_low"), (int, float))
    ]

    # Returns/statistics use adjusted prices; current valuation uses raw close.
    snap["last_close"] = latest.get("raw_close") or latest.get("close")
    snap["last_date"] = latest["date"]
    snap["price_source"] = "FinMind JapanStockPrice"
    snap["price_time"] = None
    snap["history_last_date"] = latest["date"]
    snap["history_source"] = "FinMind JapanStockPrice"
    snap["history_sample_count"] = len(rows)
    snap["history_window_days"] = days
    snap["history_adjustment"] = "Adj_Close based returns"
    snap["high_20d"] = max(raw_highs) if raw_highs else snap.get("high_20d")
    snap["low_20d"] = min(raw_lows) if raw_lows else snap.get("low_20d")
    snap["price_open"] = latest.get("raw_open")
    snap["price_high"] = latest.get("raw_high")
    snap["price_low"] = latest.get("raw_low")
    snap["price_volume"] = latest.get("volume")
    snap["history_is_same_as_price"] = True
    return snap, rows




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



def age_days_from_date(date_text):
    text = str(date_text or "")[:10]
    try:
        d = datetime.fromisoformat(text).date()
        today = datetime.now(timezone.utc).date()
        return max(0, (today - d).days)
    except Exception:
        return None


def financial_freshness(date_text):
    age = age_days_from_date(date_text)

    if age is None:
        return {
            "age_days": None,
            "factor": 0.30,
            "pct": 30,
            "label": "date_unknown",
        }

    if age <= 100:
        factor, label = 1.00, "fresh"
    elif age <= 135:
        factor, label = 0.75, "slightly_old"
    elif age <= 190:
        factor, label = 0.50, "old"
    elif age <= 280:
        factor, label = 0.30, "very_old"
    else:
        factor, label = 0.15, "stale"

    return {
        "age_days": age,
        "factor": factor,
        "pct": int(round(factor * 100)),
        "label": label,
    }


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

    freshness = financial_freshness(latest.get("date"))

    return {
        "status": "ok" if score is not None else "partial",
        "score": round(score, 2) if score is not None else None,
        "coverage": round(coverage * 100.0, 1),
        "latest": latest,
        "previous": previous,
        "metrics": detail,
        "equity_ratio_pct": equity_ratio,
        "freshness": freshness,
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

    if jquants_status():
        try:
            company = security_info(code)
        except Exception:
            pass

    finmind_snap = None
    finmind_rows = []
    finmind_error = None

    try:
        finmind_snap, finmind_rows = finmind_snapshot(
            code,
            days=900,
        )
    except Exception as e:
        finmind_error = str(e)

    # J-Quants price history is kept only as a fallback.
    jq_rows = []
    jq_snap = None
    jq_error = None
    if finmind_snap is None and jquants_status():
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
    if finmind_snap is None and jq_snap is None:
        try:
            stooq = stooq_quote(code)
        except Exception as e:
            stooq_error = str(e)

    if finmind_snap is None and jq_snap is None and stooq is None:
        return jsonify(
            status="error",
            code=code,
            reason=(
                "Price/history sources failed"
                + ("; FinMind: " + finmind_error if finmind_error else "")
                + ("; J-Quants: " + jq_error if jq_error else "")
                + ("; Stooq: " + stooq_error if stooq_error else "")
            ),
        ), 502

    if finmind_snap is not None:
        snap = dict(finmind_snap)
        quote_source = "FinMind JapanStockPrice"
        history_rows = finmind_rows
    elif jq_snap is not None:
        snap = dict(jq_snap)
        snap["price_source"] = "J-Quants API v2"
        snap["price_time"] = None
        snap["history_last_date"] = jq_snap.get("last_date")
        snap["history_source"] = "J-Quants API v2"
        snap["history_sample_count"] = len(jq_rows)
        snap["history_window_days"] = None
        snap["history_adjustment"] = "J-Quants adjusted prices"
        snap["history_is_same_as_price"] = True
        quote_source = "J-Quants API v2"
        history_rows = jq_rows
    else:
        snap = {
            "status": "partial",
            "last_close": stooq["close"],
            "last_date": stooq["date"],
            "return_20d": None,
            "return_126d": None,
            "return_252d": None,
            "volatility_20d_annualized": None,
            "high_20d": stooq.get("high"),
            "low_20d": stooq.get("low"),
            "sample_count": 1,
            "price_source": stooq["source"],
            "price_time": stooq.get("time"),
            "history_last_date": None,
            "history_source": None,
            "history_sample_count": 0,
            "history_window_days": None,
            "history_adjustment": None,
            "history_is_same_as_price": False,
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
        quote_source = stooq["source"]
        history_rows = []

    return jsonify(
        status="ok",
        code=code,
        company=company,
        source=quote_source,
        snapshot=snap,
        rows=history_rows[-30:],
        history={
            "source": snap.get("history_source"),
            "last_date": snap.get("history_last_date"),
            "sample_count": snap.get("history_sample_count")
                or snap.get("sample_count")
                or 0,
            "window_days": snap.get("history_window_days"),
            "adjustment": snap.get("history_adjustment"),
        },
        finmind_error=finmind_error,
        jquants_error=jq_error,
        stooq_error=stooq_error,
    )


@APP.get("/api/mobile/free-price-test")
def mobile_free_price_test():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400

    results = {}

    try:
        snap, rows = finmind_snapshot(code, days=900)
        results["finmind"] = {
            "status": "ok",
            "quote": {
                "date": snap.get("last_date"),
                "close": snap.get("last_close"),
                "source": snap.get("price_source"),
            },
            "history": {
                "last_date": snap.get("history_last_date"),
                "sample_count": len(rows),
                "return_20d": snap.get("return_20d"),
                "return_126d": snap.get("return_126d"),
                "return_252d": snap.get("return_252d"),
            },
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


@APP.get("/api/mobile/history-test")
def mobile_history_test():
    code = request.args.get("code", "").strip()
    if not code:
        return jsonify(error="code required"), 400

    try:
        snap, rows = finmind_snapshot(code, days=900)
        return jsonify(
            status="ok",
            code=code,
            source="FinMind JapanStockPrice",
            first_date=rows[0]["date"],
            last_date=rows[-1]["date"],
            sample_count=len(rows),
            return_20d=snap.get("return_20d"),
            return_126d=snap.get("return_126d"),
            return_252d=snap.get("return_252d"),
            volatility_20d_annualized=snap.get(
                "volatility_20d_annualized"
            ),
            supply_proxy=snap.get("supply_proxy"),
            forward_return_stats=snap.get(
                "forward_return_stats"
            ),
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

    def freshness_num(name, default=1.0):
        v = num(name)
        if v is None:
            return default
        return max(0.0, min(1.0, v))

    market_freshness = freshness_num("market_freshness", 1.0)
    earnings_freshness = freshness_num("earnings_freshness", 1.0)
    policy_freshness = freshness_num("policy_freshness", 1.0)

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
        ("technical", tech, 0.40, market_freshness),
        ("earnings", earnings, 0.30, earnings_freshness),
        ("supply", supply, 0.20, market_freshness),
        ("policy", policy, 0.10, policy_freshness),
    ]
    available=[
        x for x in factor_defs
        if x[1] is not None and x[3] > 0
    ]
    total_base_weight=sum(x[2] for x in available)
    total_weight=sum(x[2] * x[3] for x in available)

    raw_score=(
        sum(
            clamp(x[1]) * x[2] * x[3]
            for x in available
        ) / total_weight
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
        for key, value, base_weight, freshness_factor in available:
            effective_weight = base_weight * freshness_factor
            normalized_weight = effective_weight / total_weight
            contribution = clamp(value) * normalized_weight
            drivers.append({
                "key": key,
                "score": round(value, 2),
                "base_weight_pct": round(base_weight * 100.0, 1),
                "freshness_pct": round(freshness_factor * 100.0, 1),
                "effective_weight_pct_before_normalize": round(
                    effective_weight * 100.0, 1
                ),
                "weight_pct": round(normalized_weight * 100.0, 1),
                "contribution": round(contribution, 2),
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
            "available_base_weight_pct":round(total_base_weight*100.0,1),
            "technical":round(tech,2) if tech is not None else None,
            "earnings":earnings,
            "supply":supply,
            "policy":policy,
            "freshness":{
                "market_pct":round(market_freshness*100.0,1),
                "earnings_pct":round(earnings_freshness*100.0,1),
                "policy_pct":round(policy_freshness*100.0,1),
            },
            "method":"freshness-adjusted deterministic weighted score",
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
            "market_data":"FinMind/J-Quants real market data with freshness weighting",
            "earnings":"J-Quants financial summary when available",
            "supply":"real price/volume proxy; not margin balance",
            "policy":"official policy source + deterministic relevance proxy" if policy_mode=="auto" else "manual override"
        }
    )



FREE_UNIVERSE_CACHE={}
@APP.get('/api/free/market-universe')
def free_market_universe():
    now=datetime.now(timezone.utc).timestamp()
    cached=FREE_UNIVERSE_CACHE.get('result')
    if cached and now-FREE_UNIVERSE_CACHE.get('ts',0)<86400:
        return jsonify(cached)
    try:
        headers={'User-Agent':'JapanStockAIFree/market-scan'}
        token=os.getenv('FINMIND_TOKEN','').strip()
        if token:headers['Authorization']='Bearer '+token
        response=requests.get('https://api.finmindtrade.com/api/v4/data',params={'dataset':'JapanStockInfo'},headers=headers,timeout=20)
        response.raise_for_status()
        payload=response.json()
        if payload.get('status') not in (None,0,200,'200'):
            raise ValueError('鬯ｩ�ｫ闖ｫ�ｶ雎悟ｹ�初�つ鬮ｫ蛹�ｽｽ�ｧ驛｢�ｧ髮区ｧｫ蠕宣辧蜍溷ｹｲ邵ｲ蝣､�ｸ�ｺ鬮ｦ�ｪ遶擾ｽｪ驍ｵ�ｺ陝ｶ蜻ｻ�ｽ骰具ｽｸ�ｺ�ｽ�ｧ驍ｵ�ｺ陷会ｽｱ隨ｳ�ｽ')
        catalog={}
        for row in payload.get('data') or []:
            symbol=str(row.get('stock_id') or '').strip()
            if not symbol.endswith('.T'):continue
            code=symbol[:-2]
            if len(code)!=4 or not code.isalnum():continue
            catalog[code]={'code':code,'name':str(row.get('stock_name') or '').strip(),'sector':row.get('Sector') or '', 'catalog_date':row.get('date') or ''}
        if not catalog:raise ValueError('鬯ｩ�ｫ闖ｫ�ｶ雎悟ｹ�初�つ鬮ｫ蛹�ｽｽ�ｧ驍ｵ�ｺ隶呵ｶ｣�ｽ�ｩ�ｽ�ｺ驍ｵ�ｺ�ｽ�ｧ驍ｵ�ｺ�ｽ�ｽ')
        result={'status':'ok','stocks':[catalog[k] for k in sorted(catalog)],'count':len(catalog),'source':'FinMind JapanStockInfo (.T)','fetched_at':datetime.now(timezone.utc).isoformat()}
        FREE_UNIVERSE_CACHE.update(result=result,ts=now)
        return jsonify(result)
    except Exception:
        return jsonify(status='error',reason='髴取ｻゑｽｽ�｡髫ｴ竏晉函�ｽ�ｽ鬯ｩ�ｫ闖ｫ�ｶ雎悟ｹ�初�つ鬮ｫ蛹�ｽｽ�ｧ驛｢�ｧ髮区ｧｫ蠕宣辧蜍溷ｹｲ邵ｲ蝣､�ｸ�ｺ鬮ｦ�ｪ遶擾ｽｪ驍ｵ�ｺ陝ｶ蜻ｻ�ｽ骰具ｽｸ�ｲ郢ｧ莠･�ｽ鬯ｮ�｢髦ｮ蜻ｻ�ｽ蟶昶��ｽ�ｺ驍ｵ�ｺ闔会ｽ｣遯ｶ�ｻ髯ｷﾂ隶主･�ｽｽ�ｺ�ｽ�ｦ鬮ｫ�ｧ�ｽ�ｦ驍ｵ�ｺ陷会ｽｱ遯ｶ�ｻ驍ｵ�ｺ闕ｳ蟯ｩ蜻ｳ驍ｵ�ｺ髴郁ｲｻ�ｼ讓抵ｽｸ�ｲ�ｽ�ｽ'),502


NEWS_ADVICE_CACHE={}
@APP.get('/api/free/news-advice')
def free_news_advice():
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    from urllib.parse import urlparse
    from html import unescape
    import re
    now=datetime.now(timezone.utc)
    cached=NEWS_ADVICE_CACHE.get('result')
    if cached and now.timestamp()-NEWS_ADVICE_CACHE.get('ts',0)<900:return jsonify(cached)
    try:
        response=requests.get('https://news.google.com/rss/search',params={'q':'譌･譛ｬ譬ｪ OR 譌･邨悟ｹｳ蝮� OR 譌･驫 OR 莨∵･ｭ豎ｺ邂� when:2d','hl':'ja','gl':'JP','ceid':'JP:ja'},headers={'User-Agent':'JapanStockAIFree/news'},timeout=15)
        response.raise_for_status()
        if len(response.content)>2000000:raise ValueError('feed size')
        root=ET.fromstring(response.content)
        items=[];seen=set();sources={}
        for item in root.findall('.//item'):
            title=unescape(item.findtext('title') or '').strip()
            link=item.findtext('link') or ''
            if not title or title in seen or urlparse(link).scheme!='https' or urlparse(link).hostname!='news.google.com':continue
            if '譬ｪ萓｡繝ｻ譬ｪ蠑乗ュ蝣ｱ' in title:continue
            try:
                published=parsedate_to_datetime(item.findtext('pubDate') or '')
                if published.tzinfo is None:published=published.replace(tzinfo=timezone.utc)
                age=(now-published).total_seconds()
                if age < -3600 or age>72*3600:continue
            except Exception:continue
            source=(item.findtext('source') or '').strip()
            if sources.get(source,0)>=2:continue
            raw=item.findtext('description') or ''
            summary=unescape(re.sub('<[^>]+>',' ',raw))
            summary=re.sub(r'\s+',' ',summary).strip()[:300]
            text=title+' '+summary
            if any(k in text for k in ['譌･驫','驥大茜','蛻ｩ荳翫￡','蛻ｩ荳九￡']):
                topic='驥題檮謾ｿ遲悶�驥大茜';comment='驥大茜縺ｮ繝九Η繝ｼ繧ｹ縺ｯ縲�橿陦後→蛟溷�縺ｮ螟壹＞莨∵･ｭ縺ｧ蠖ｱ髻ｿ縺碁＆縺�ｈ縲よ帆遲悶′豎ｺ螳壽ｸ医∩縺九∽ｺ域Φ繝ｻ隕ｳ貂ｬ縺ｪ縺ｮ縺九ｒ蜴滓枚縺ｧ遒ｺ隱阪＠繧医≧縺ｭ笙･'
            elif any(k in text for k in ['轤ｺ譖ｿ','蜀�ｮ�','蜀�ｫ�','繝峨Ν蜀�']):
                topic='轤ｺ譖ｿ';comment='轤ｺ譖ｿ縺悟虚縺上→縲∬ｼｸ蜃ｺ莨∵･ｭ縺ｨ霈ｸ蜈･繧ｳ繧ｹ繝医�螟ｧ縺阪＞莨∵･ｭ縺ｧ蜿励￠豁｢繧∵婿縺悟､峨ｏ繧九ｈ縲り�蛻��驫俶氛縺ｮ豎ｺ邂怜燕謠舌ｂ遒ｺ隱阪〒縺吶◇笙･'
            elif any(k in text for k in ['豎ｺ邂�','蠅礼寢','貂帷寢','讌ｭ邵ｾ','荳頑婿菫ｮ豁｣','荳区婿菫ｮ豁｣']):
                topic='豎ｺ邂励�讌ｭ邵ｾ';comment='蛻ｩ逶翫�蠅玲ｸ帙□縺代〒縺ｪ縺上∽ｼ夂､ｾ莠域Φ繧�ｸょ�ｴ縺ｮ譛溷ｾ�→縺ｮ驕輔＞繧りｦ九ｈ縺��縲ゆｸ譎ら噪縺ｪ隕∝屏縺九←縺�°繧ょ次譁�〒繝√ぉ繝�け笙･'
            elif any(k in text for k in ['蜊雁ｰ惹ｽ�','AI','繝��繧ｿ繧ｻ繝ｳ繧ｿ繝ｼ']):
                topic='蜊雁ｰ惹ｽ薙�AI';comment='繝��繝槭�蜍｢縺�↓蜉�縺医※縲∝ｮ滄圀縺ｮ蜿玲ｳｨ繧�茜逶翫↓縺､縺ｪ縺後ｋ隧ｱ縺九ｒ遒ｺ隱阪＠繧医≧縺ｭ縲る未騾｣驫俶氛縺吶∋縺ｦ縺ｫ蜷後§蠖ｱ髻ｿ縺後≠繧九→縺ｯ髯舌ｉ縺ｪ縺�ｈ操笨ｨ'
            elif any(k in text for k in ['蜴滓ｲｹ','繧ｨ繝阪Ν繧ｮ繝ｼ','荳ｭ譚ｱ']):
                topic='雉�ｺ舌�繧ｨ繝阪Ν繧ｮ繝ｼ';comment='雉�ｺ蝉ｾ｡譬ｼ縺ｯ縲∬ｳ�ｺ舌ｒ螢ｲ繧倶ｼ∵･ｭ縺ｨ菴ｿ縺�ｼ∵･ｭ縺ｧ蠖ｱ髻ｿ縺悟�縺九ｌ繧九ｈ縲ゅさ繧ｹ繝医ｒ萓｡譬ｼ縺ｫ霆｢雖√〒縺阪ｋ縺九ｂ遒ｺ隱阪＠縺ｦ縺ｭ笙･'
            else:
                topic='蟶ょ�ｴ蜈ｨ菴�';comment='縺薙�繝九Η繝ｼ繧ｹ縺瑚�蛻��驫俶氛縺ｫ縺ｩ縺�未菫ゅ☆繧九°縲√∪縺壹�蜴滓枚繧定ｦ九※縺ｿ繧医≧縺ｭ縲りｦ句�縺励�蜍｢縺�□縺代〒諤･縺後★縲∵�ｪ萓｡縺ｨ蜃ｺ譚･鬮倥ｂ荳邱偵↓遒ｺ隱坂勍'
            items.append({'title':title[:300],'link':link,'source':source or '驟堺ｿ｡蜈�ｸ肴�','published_at':published.isoformat(),'topic':topic,'comment':comment})
            seen.add(title);sources[source]=sources.get(source,0)+1
        items.sort(key=lambda x:x['published_at'],reverse=True)
        result={'status':'ok','items':items[:5],'fetched_at':now.isoformat(),'method':'RSS headline/summary topic rules; article full text is not read'}
        NEWS_ADVICE_CACHE.update(result=result,ts=now.timestamp())
        return jsonify(result)
    except Exception:
        return jsonify(status='error',reason='繝九Η繝ｼ繧ｹ繧貞叙蠕励〒縺阪∪縺帙ｓ縺ｧ縺励◆縲よ凾髢薙ｒ遨ｺ縺代※蜀榊ｺｦ隧ｦ縺励※縺上□縺輔＞縲�'),502

HTML = base64.b64decode(
    'PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9CgoucG9saWN5dGhlbWUgYXtmb250LXNpemU6MTJweH0KLmNvbnRyaWJncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQouY29udHJpYntib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDtiYWNrZ3JvdW5kOiNmZmZ9Ci5jb250cmliIHNwYW57ZGlzcGxheTpibG9ja30KLmNvbnRyaWIgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4O21hcmdpbi10b3A6MnB4fQoKLmNvbnRyaWIgc21hbGx7ZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweDtjb2xvcjojNmI3MjgwO2xpbmUtaGVpZ2h0OjEuMzV9Ci5mcmVzaGJveHtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMXB4O21hcmdpbi10b3A6OXB4O2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLmZyZXNoLW9re2JhY2tncm91bmQ6I2VjZmRmNTtib3JkZXItY29sb3I6I2E3ZjNkMH0KLmZyZXNoLXdhcm57YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1jb2xvcjojZmRlNjhhfQouZnJlc2gtc3RhbGV7YmFja2dyb3VuZDojZmVmMmYyO2JvcmRlci1jb2xvcjojZmVjYWNhfQoKLmZyZXNoYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweH0KLnNvdXJjZWJhZGdle2Rpc3BsYXk6aW5saW5lLWJsb2NrO3BhZGRpbmc6NHB4IDhweDtib3JkZXItcmFkaXVzOjk5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTJweDtmb250LXdlaWdodDo4MDA7bWFyZ2luLXJpZ2h0OjRweH0KLmhpc3Rvcnl3YXJue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDttYXJnaW4tdG9wOjdweDtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyOjFweCBzb2xpZCAjZmRlNjhhfQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5jb250cmliZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cgouc3RvY2stZGV0YWlsc3tib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxNHB4O21hcmdpbi10b3A6MTBweDtiYWNrZ3JvdW5kOndoaXRlfQouc3RvY2stZGV0YWlscz5zdW1tYXJ5e3BhZGRpbmc6MTZweDtjdXJzb3I6cG9pbnRlcjtmb250LXdlaWdodDo3MDA7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnN0b2NrLWRldGFpbHNbb3Blbl0+c3VtbWFyeXtib3JkZXItYm90dG9tOjFweCBzb2xpZCAjZTVlN2VifQouc3RvY2stZGV0YWlscz4uaG9sZGluZ3tib3JkZXI6MDttYXJnaW46MH0KLndhdGNoLWNvbnRlbnR7cGFkZGluZzoxMnB4fQoKLndhdGNoLXRpbWluZ3tkaXNwbGF5OmlubGluZS1ibG9jaztmb250LXNpemU6MTJweDtmb250LXdlaWdodDo3MDA7Ym9yZGVyLXJhZGl1czoyMHB4O3BhZGRpbmc6NXB4IDlweDttYXJnaW4tbGVmdDo2cHg7dmVydGljYWwtYWxpZ246bWlkZGxlfQoud2F0Y2gtYnV5e2JhY2tncm91bmQ6I2RjZmNlNztjb2xvcjojMTY2NTM0fS53YXRjaC1uZXV0cmFse2JhY2tncm91bmQ6I2YxZjVmOTtjb2xvcjojMzM0MTU1fS53YXRjaC1wZW5kaW5ne2JhY2tncm91bmQ6I2ZlZjNjNztjb2xvcjojOTI0MDBlfQoKLnJhZGVuLW5ld3Mtc2NlbmV7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTJweDttYXJnaW46MTJweCAwfS5yYWRlbi1hdmF0YXJ7d2lkdGg6MTEwcHg7aGVpZ2h0OjEzMHB4O29iamVjdC1maXQ6Y292ZXI7Ym9yZGVyLXJhZGl1czoxOHB4O2ZsZXgtc2hyaW5rOjB9LnJhZGVuLWJ1YmJsZXtiYWNrZ3JvdW5kOndoaXRlO2JvcmRlci1yYWRpdXM6MThweDtwYWRkaW5nOjE0cHg7cG9zaXRpb246cmVsYXRpdmU7ZmxleDoxO2JvcmRlcjoxcHggc29saWQgI2ZiY2ZlOH0ucmFkZW4tYnViYmxlOmJlZm9yZXtjb250ZW50OicnO3Bvc2l0aW9uOmFic29sdXRlO2xlZnQ6LTEwcHg7dG9wOjM1cHg7Ym9yZGVyLXRvcDoxMHB4IHNvbGlkIHRyYW5zcGFyZW50O2JvcmRlci1ib3R0b206MTBweCBzb2xpZCB0cmFuc3BhcmVudDtib3JkZXItcmlnaHQ6MTBweCBzb2xpZCB3aGl0ZX0ucmFkZW4tbmV3cy1jYXJke2JvcmRlci10b3A6MXB4IHNvbGlkICNmYmNmZTg7cGFkZGluZzoxNHB4IDA7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0ucmFkZW4tbmV3cy1jb21tZW50e2JhY2tncm91bmQ6d2hpdGU7Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTJweDtsaW5lLWhlaWdodDoxLjg7bWFyZ2luLXRvcDoxMHB4fUBtZWRpYShtYXgtd2lkdGg6NDAwcHgpey5yYWRlbi1hdmF0YXJ7d2lkdGg6ODJweDtoZWlnaHQ6MTEwcHh9LnJhZGVuLWJ1YmJsZXtwYWRkaW5nOjEwcHh9fQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8bWFpbj4KPGRpdiBjbGFzcz0idG9wIj4KICA8aDE+8J+TiCDml6XmnKzmoKpBSSBGUkVFPC9oMT4KICA8ZGl2IGNsYXNzPSJzdWIiPuimgeWboOWIpemuruW6piAvIEZpbk1pbmTkvqHmoLzlsaXmrbQgLyBKLVF1YW50c+axuueulyAvIOWbveetljwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn46vIOmKmOafhOWIhuaekDwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZCI+CiAgICA8aW5wdXQgaWQ9ImNvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSDkvosgNzIwMyI+CiAgICA8aW5wdXQgaWQ9InByaWNlIiBwbGFjZWhvbGRlcj0i5Y+W5b6X57WC5YCkIiByZWFkb25seT4KICA8L2Rpdj4KICA8ZGl2IGlkPSJjb21wYW55TmFtZSIgY2xhc3M9InNvdXJjZSBtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZnjgovjgajkvJrnpL7lkI3jgpLooajnpLrjgZfjgb7jgZk8L2Rpdj4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGEgaWQ9ImthYnV0YW4iIGNsYXNzPSJidG4gc2Vjb25kYXJ5IiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+5qCq5o6i44Gn56K66KqNPC9hPgogICAgPGJ1dHRvbiBpZD0iYW5hbHl6ZUJ0biIgb25jbGljaz0iYW5hbHl6ZSgpIj7lrp/jg4fjg7zjgr/jgafliIbmnpA8L2J1dHRvbj4KICA8L2Rpdj4KICA8cCBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeOCjOOBpuaKvOOBmeOBqOOAgeePvuWcqOWApOOBqDIw5pel44O7MTI25pel44O7MjUy5pel44Gu5L6h5qC85bGl5q2044GvRmluTWluZOaXpeacrOagquaXpei2s+OCkuacgOWEquWFiOOBl+OBvuOBmeOAgumKmOafhOWQjeODu+axuueul+ODu+WbveetluWIpOWumuOBr0otUXVhbnRz562J44KS5L2/55So44GX44G+44GZ44CCPC9wPgogIDxkaXYgaWQ9InNvdXJjZUJveCIgY2xhc3M9InNvdXJjZSBtdXRlZCI+44OH44O844K/5pyq5Y+W5b6XPC9kaXY+CiAgPGRpdiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJtYXJnaW4tdG9wOjdweCI+CiAgICA8Yj7wn4aTIOeEoeaWmeS+oeagvOWxpeattOOBq+OBpOOBhOOBpjwvYj4KICAgIDxkaXYgY2xhc3M9Im11dGVkIj5GaW5NaW5k44Gu5pel5pys5qCq5pel6Laz44KS57SEOTAw5pel5YiG5Y+W5b6X44GX44CB5pyA5paw5Za25qWt5pel44Gu57WC5YCk44O7MjAvMTI2LzI1MuaXpeODquOCv+ODvOODs+ODuzIw5pel6auY5a6J44O75Ye65p2l6auYcHJveHnjg7vjg63jg7zjg6rjg7PjgrDlrp/nuL7liIbluIPjgpLoqIjnrpfjgZfjgb7jgZnjgILlj5blvJXkuK3jga7jg6rjgqLjg6vjgr/jgqTjg6DkvqHmoLzjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzQm94IiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPGI+8J+TmiDkvqHmoLzliIbmnpDlsaXmrbTjga7prq7luqY8L2I+CiAgICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzVGV4dCIgY2xhc3M9Im11dGVkIj48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJmcmVzaG5lc3NCb3giIGNsYXNzPSJmcmVzaGJveCBmcmVzaC13YXJuIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxiIGlkPSJmcmVzaG5lc3NUaXRsZSI+44OH44O844K/6a6u5bqmPC9iPgogICAgPGRpdiBpZD0iZnJlc2huZXNzRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5OKIOagquS+oeODu+ODhuOCr+ODi+OCq+ODq+Wun+e4vjwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6aiw6JC9546HICU8L3NwYW4+PGlucHV0IGlkPSJyMjAiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjEyNuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjEyNiIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjUy5pelICU8L3NwYW4+PGlucHV0IGlkPSJyMjUyIiByZWFkb25seT48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemrmOWApDwvc3Bhbj48YiBpZD0iaGlnaDIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlronlgKQ8L3NwYW4+PGIgaWQ9ImxvdzIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlubTnjofjg5zjg6k8L3NwYW4+PGIgaWQ9InZvbDIwIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6kg5a6f44OH44O844K/6KaB5ZugPC9oMz4KICA8cCBjbGFzcz0ibXV0ZWQiPuS+oeagvOODu+mcgOe1puODu+axuueul+ODu+WbveetluOBr+OBneOCjOOBnuOCjOWIpeOBq+muruW6puOCkuWIpOWumuOBl+OBvuOBmeOAguWPpOOBhOimgeWboOOBr+WApOOCkua2iOOBleOBmuOAgee3j+WQiOeCueOBuOOBrumHjeOBv+OBoOOBkeiHquWLleOBp+S4i+OBkuOBvuOBmeOAgjwvcD4KICA8ZGl2IGNsYXNzPSJmYWN0b3JncmlkIj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpc8L3NwYW4+PGIgaWQ9ImVhcm5BdXRvIj7igJQ8L2I+PHNtYWxsIGlkPSJlYXJuRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWPluW+lzwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZyA57WmcHJveHk8L3NwYW4+PGIgaWQ9InN1cHBseUF1dG8iPuKAlDwvYj48c21hbGwgaWQ9InN1cHBseURldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrlj5blvpc8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetlnByb3h5PC9zcGFuPjxiIGlkPSJwb2xpY3lTdGF0ZSI+4oCUPC9iPjxzbWFsbCBpZD0icG9saWN5RGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuWFrOW8j+aUv+etluOCveODvOOCueeiuuiqjeWJjTwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5a6f5Yq544OH44O844K/5YWF6LazPC9zcGFuPjxiIGlkPSJjb3ZlcmFnZSI+4oCUPC9iPjxzbWFsbCBjbGFzcz0ibXV0ZWQiPumuruW6puOBvuOBp+WPjeaYoOOBl+OBn+mHjeOBvzwvc21hbGw+PC9kaXY+CiAgPC9kaXY+CiAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDZweCI+8J+VkiDopoHlm6DliKXjga7prq7luqY8L2g0PgogIDxkaXYgY2xhc3M9ImZhY3RvcmdyaWQiPgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS+oeagvOWxpeattDwvc3Bhbj48YiBpZD0ibWFya2V0RnJlc2giPuKAlDwvYj48c21hbGwgaWQ9Im1hcmtldEZyZXNoRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWIpOWumjwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5rG6566XPC9zcGFuPjxiIGlkPSJlYXJuRnJlc2giPuKAlDwvYj48c21hbGwgaWQ9ImVhcm5GcmVzaERldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrliKTlrpo8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetljwvc3Bhbj48YiBpZD0icG9saWN5RnJlc2giPuKAlDwvYj48c21hbGwgaWQ9InBvbGljeUZyZXNoRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWIpOWumjwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6a6u5bqm5Y+N5pig5b6M44Kr44OQ44O8546HPC9zcGFuPjxiIGlkPSJmcmVzaENvdmVyYWdlIj7igJQ8L2I+PHNtYWxsIGNsYXNzPSJtdXRlZCI+5Y+k44GE6KaB5Zug44Gv6YeN44G/44KS5rib6KGwPC9zbWFsbD48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJwb2xpY3lUaGVtZXMiIGNsYXNzPSJwb2xpY3l0aGVtZXMiPjwvZGl2PgogIDxkaXYgc3R5bGU9Im1hcmdpbi10b3A6OXB4Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Zu9562W44K544Kz44Ki5LiK5pu444GN77yI5Lu75oSP77yJIC0xMDDjgJwxMDA8L3NwYW4+CiAgICA8aW5wdXQgaWQ9InBvbGljeSIgaW5wdXRtb2RlPSJkZWNpbWFsIiBwbGFjZWhvbGRlcj0i56m65qyE44Gq44KJ6Ieq5YuV5Zu9562WcHJveHnjgpLkvb/nlKgiPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+4oC75Zu9562WcHJveHnjga/jgIHmlL/lupzlhazlvI/mlL/nrZbjgr3jg7zjgrnjgahKLVF1YW50c+OBrualreeoruODu+S8muekvuWQjeOBqOOBrumWoumAo+W6puOCkue1hOOBv+WQiOOCj+OBm+OBn+WPguiAg+WApOOBp+OBmeOAguODqeOCpOODlueiuuiqjeOBp+OBjeOBquOBhOWgtOWQiOOBr+acgOe1gueiuuiqjea4iOOBv+aDheWgseOCkuS9juS/oemgvOW6puOBp+S9v+eUqOOBl+OAgeOBneOBrueKtuaFi+OCgueUu+mdouOBq+aYjuekuuOBl+OBvuOBmeOAguijnOWKqemHkeaOoeaKnuOChOalree4vuaBqeaBteOBneOBruOCguOBruOCkuiovOaYjuOBmeOCi+aMh+aomeOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+noCDliIbmnpDntZDmnpw8L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nirbmhYs8L3NwYW4+PGIgaWQ9InN0YXRlIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OX44Op44K55qC55ougPC9zcGFuPjxiIGlkPSJwb3MiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg57jgqTjg4rjgrnmoLnmi6A8L3NwYW4+PGIgaWQ9Im5lZyI+4oCUPC9iPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InNjb3JlSGVybyIgY2xhc3M9InNjb3JlaGVybyIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+WQiOaOoeeCue+8iOWPluW+l+ODh+ODvOOCv+ODu+ODq+ODvOODq+ODmeODvOOCue+8iTwvc3Bhbj4KICAgIDxiIGlkPSJzY29yZTEwMCI+4oCUPC9iPgogICAgPHNwYW4gaWQ9InNjb3JlU3RhdGVQaWxsIiBjbGFzcz0ic3RhdGVwaWxsIHN0YXRlLW5ldXRyYWwiPuKAlDwvc3Bhbj4KICAgIDxkaXYgaWQ9InNjb3JlQnJlYWtkb3duIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0ic2NvcmVSZWFzb24iIGNsYXNzPSJzY29yZS1yZWFzb24iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPHN0cm9uZz7wn6etIOOBquOBnOOBk+OBrueCueaVsO+8nzwvc3Ryb25nPgogICAgPGRpdiBpZD0ic2NvcmVSZWFzb25UZXh0IiBjbGFzcz0ibXV0ZWQiPuKAlDwvZGl2PgogICAgPGRpdiBpZD0iZHJpdmVyR3JpZCIgY2xhc3M9ImRyaXZlcmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbnRyaWJ1dGlvbkJveCIgY2xhc3M9InNjb3JlLXJlYXNvbiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3Ryb25nPvCfp64g57eP5ZCI54K544G444Gu5a+E5LiOPC9zdHJvbmc+CiAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5Y+W5b6X44Gn44GN44Gf6KaB5Zug44Gr6a6u5bqm5L+C5pWw44KS5o6b44GR44Gm44GL44KJ6YeN44G/44KS5YaN6YWN5YiG44GX44CB5ZCE6KaB5Zug44GM57eP5ZCI6KmV5L6h44KS44Gp44KM44Gg44GR5oq844GX5LiK44GS77yP5oq844GX5LiL44GS44Gf44GL44KS6KGo56S644GX44G+44GZ44CCPC9kaXY+CiAgICA8ZGl2IGlkPSJjb250cmlidXRpb25HcmlkIiBjbGFzcz0iY29udHJpYmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxwIGlkPSJyZXN1bHQiIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44CM5a6f44OH44O844K/44Gn5YiG5p6Q44CN44KS5oq844GX44Gm44GP44Gg44GV44GE44CCPC9wPgogIDxwIGNsYXNzPSJtdXRlZCI+5o6h54K55biv77yaODDjgJwxMDAg5by35rCXIC8gNjXjgJw3OSDjgoTjgoTlvLfmsJcgLyA0NeOAnDY0IOS4reeriyAvIDMw44CcNDQg44KE44KE5byx5rCXIC8gMOOAnDI5IOW8seawlzwvcD4KICA8ZGl2IGlkPSJhbmFseXNpc0V2IiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfmqgg5LuK5pel44Gu5YSq5YWI44Ki44Kv44K344On44OzPC9oMz4KICA8ZGl2IGlkPSJwcmlvcml0eUFjdGlvbnMiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CjxoMz7wn5OIIOaXpeacrOagquOBruazqOebruODu+S4i+iQveitpuaIku+8iOeEoeaWmeW3oeWbnu+8iTwvaDM+CjxwIGNsYXNzPSJtdXRlZCI+5a++6LGh44GvRmluTWluZOOBruaXpeacrOagqumKmOafhOS4gOimp++8iC5U77yJ44CC5L+d5pyJ44O744Km44Kp44OD44OB55m76Yyy44Gv5LiN6KaB44Gn44GZ44CC54Sh5paZ5p6g44GnMjDpipjmn4TjgZrjgaToqr/jgbnjgIHlj5blvpfjgafjgY3jgZ/nr4Tlm7LjgYvjgonlgJnoo5zjgpLooajnpLrjgZfjgb7jgZnjgILlhajluILloLTjgpLlkIzmmYLjgavmr5TovIPjgZfjgZ/jg6njg7Pjgq3jg7PjgrDjgafjga/jgYLjgorjgb7jgZvjgpPjgILlkITmrITjga/mnIDlpKc1MOS7tuOCkuihqOekuuOBl+OBvuOBmeOAguaXpei2s+e1guWApOOBp+WIpOWumuOBl+OBvuOBmeOAgjwvcD4KPGxhYmVsIGZvcj0ibWFya2V0R2VucmUiPuiqv+OBueOCi+OCuOODo+ODs+ODqzwvbGFiZWw+CjxzZWxlY3QgaWQ9Im1hcmtldEdlbnJlIiBvbmNoYW5nZT0iY2hhbmdlTWFya2V0R2VucmUoKSIgc3R5bGU9IndpZHRoOjEwMCU7cGFkZGluZzoxMnB4O2JvcmRlcjoxcHggc29saWQgI2RkZDtib3JkZXItcmFkaXVzOjEycHg7bWFyZ2luOjhweCAwIj48b3B0aW9uIHZhbHVlPSLlhajmpa3nqK4iPuWFqOalreeorjwvb3B0aW9uPjwvc2VsZWN0Pgo8YnV0dG9uIGlkPSJtYXJrZXRHZW5yZUxvYWQiIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9ImxvYWRNYXJrZXRHZW5yZXMoKSIgc3R5bGU9Im1hcmdpbi1ib3R0b206OHB4Ij7jgrjjg6Pjg7Pjg6vkuIDopqfjgpLoqq3jgb/ovrzjgoA8L2J1dHRvbj4KPHAgY2xhc3M9Im11dGVkIj7lj5blvpflhYPjga7mpa3nqK7jgpLkvb/jgaPjgZ/ni6zoh6rjga7liIbpoZ7jgafjgZnjgIJBSeODu+WbveetluOBquOBqeOBruODhuODvOODnuWIhumhnuOBqOOBr+eVsOOBquOCiuOBvuOBmeOAguOCuOODo+ODs+ODq+OBlOOBqOOBq+e2muOBjeOBi+OCieW3oeWbnuOBp+OBjeOBvuOBmeOAgjwvcD4KPGJ1dHRvbiBpZD0ibW92ZW1lbnRCdG4iIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hNb3ZlbWVudCgpIj7mrKHjga4yMOmKmOafhOOCkuiqv+OBueOCizwvYnV0dG9uPgo8YnV0dG9uIGlkPSJtb3ZlbWVudEFsbEJ0biIgb25jbGljaz0icmVmcmVzaE1vdmVtZW50KHRydWUpIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumBuOaKnuOCuOODo+ODs+ODq+OCkuWFqOmDqOiqv+OBueOCizwvYnV0dG9uPgo8cCBjbGFzcz0ibXV0ZWQiPjHlm57mirzjgZnjgajpgbjmip7jgrjjg6Pjg7Pjg6vjgpLlhYjpoK3jgYvjgonmnIDlvozjgb7jgafoh6rli5Xlt6Hlm57jgZfjgb7jgZnjgILnhKHmlpnmnqDjgavlkIjjgo/jgZvntIQxM+enkuS7peS4iuOBmuOBpOmWk+malOOCkuepuuOBkeOBvuOBmeOAgjEwMOmKmOafhOOBquOCieW+heOBoeaZgumWk+OBoOOBkeOBp+e0hDIy5YiG44GL44GL44KK44G+44GZ44CC5beh5Zue5Lit44Gv44GT44Gu44Oa44O844K444KS6ZaL44GE44Gf44G+44G+44Gr44GX44Gm44GP44Gg44GV44GE44CCPC9wPgo8YnV0dG9uIGlkPSJtb3ZlbWVudFN0b3AiIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InN0b3BNb3ZlbWVudCgpIiBkaXNhYmxlZCBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPuW3oeWbnuOCkuWBnOatojwvYnV0dG9uPgo8cCBpZD0ibW92ZW1lbnRDb3ZlcmFnZSIgY2xhc3M9Im11dGVkIj48L3A+CjxwIGlkPSJtb3ZlbWVudFN0YXR1cyIgY2xhc3M9Im11dGVkIiByb2xlPSJzdGF0dXMiIGFyaWEtbGl2ZT0icG9saXRlIj7mnKrmm7TmlrDjgILlj5blvpfjgZfjgZ/jg4fjg7zjgr/jga/jgZPjga7jg5bjg6njgqbjgrbjgavkv53lrZjjgZfjgb7jgZnjgII8L3A+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7wn4yfIOacrOaXpeOBruazqOebruWAmeijnO+8iOacgOaWsOWPluW+l+aXpeODmeODvOOCue+8iSA8c3BhbiBpZD0ibW92ZW1lbnRVcENvdW50Ij48L3NwYW4+PC9zdW1tYXJ5PjxkaXYgY2xhc3M9IndhdGNoLWNvbnRlbnQiIGlkPSJtb3ZlbWVudFVwIj48L2Rpdj48L2RldGFpbHM+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7imqDvuI8g5LiL6JC96K2m5oiS6YqY5p+EIDxzcGFuIGlkPSJtb3ZlbWVudERvd25Db3VudCI+PC9zcGFuPjwvc3VtbWFyeT48ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50IiBpZD0ibW92ZW1lbnREb3duIj48L2Rpdj48L2RldGFpbHM+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7jgZ3jga7ku5bjg7vliKTlrprkv53nlZkgPHNwYW4gaWQ9Im1vdmVtZW50T3RoZXJDb3VudCI+PC9zcGFuPjwvc3VtbWFyeT48ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50IiBpZD0ibW92ZW1lbnRPdGhlciI+PC9kaXY+PC9kZXRhaWxzPgo8cCBjbGFzcz0ibXV0ZWQiPuS7ruODq+ODvOODq++8muebtOi/kTHllrbmpa3ml6XvvIsxJeS7peS4iuOBi+OBpDXllrbmpa3ml6Xjg5fjg6njgrnjgpLms6jnm67lgJnoo5zjgIHiiJIxJeS7peS4i+OBi+OBpDXllrbmpa3ml6Xjgb7jgZ/jga8yMOWWtualreaXpeODnuOCpOODiuOCueOCkuS4i+iQveitpuaIkuOBqOOBl+OBvuOBmeOAguWHuuadpemrmOWil+WKoOOBr+ijnOWKqeihqOekuuOAguS4i+iQveS6iOa4rOODu+Wjsuiyt+aOqOWlqOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAguWIhuWJsuiqv+aVtOOBruOBquOBhOWxpeattOOBr+WApOWLleOBjeOBjOatquOCgOWgtOWQiOOBjOOBguOCiuOBvuOBmeOAgjwvcD4KPC9kaXY+CjxkaXYgY2xhc3M9ImNhcmQgcG9ydGZvbGlvIj4KICA8aDM+8J+nrSDjg53jg7zjg4jjg5Xjgqnjg6rjgqrlhajkvZM8L2gzPgogIDxkaXYgaWQ9InBvcnRmb2xpb1N1bW1hcnkiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkrwg5L+d5pyJ5qCq44O75pCN5YiH44KKL+WIqeeiujwvaDM+CiAgPGRpdiBjbGFzcz0ibm90ZSI+CiAgICDnj77lnKjlgKTjgajkvqHmoLzliIbmnpDlsaXmrbTjga9GaW5NaW5k5pel6Laz44KS5YSq5YWI44GX44Gm44CB5pCN55uK44O75pCN5YiH44KK6Led6Zui44O75Yip56K66Led6Zui44O744OI44Os44O844Oq44Oz44Kw44O755+t5Lit6ZW35a6f57i+44O744Od44O844OI44OV44Kp44Oq44Kq6KmV5L6h44KS6Ieq5YuV5YaN6KiI566X44GX44G+44GZ44CCRmluTWluZOWPluW+l+WkseaVl+aZguOBoOOBkeS7luOCveODvOOCueOBuOODleOCqeODvOODq+ODkOODg+OCr+OBl+OBvuOBmeOAggogIDwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Im1hcmdpbi10b3A6OXB4Ij4KICAgIDxidXR0b24gaWQ9InJlZnJlc2hBbGxCdG4iIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hBbGxIb2xkaW5ncyh0cnVlKSI+5L+d5pyJ5qCq44KS5pyA5paw57WC5YCk44Gn5LiA5ous5pu05pawPC9idXR0b24+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZGluZ1JlZnJlc2hTdGF0dXMiIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbjo3cHggMnB4IDAiPuS/neacieagquOBruiHquWLleabtOaWsOOBrzMw5YiG44GU44Go44Gr5pyA5aSnMeWbnuOBp+OBmeOAgjwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPgogICAgPGlucHV0IGlkPSJob2xkQ29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxpbnB1dCBpZD0iaG9sZENvc3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgcGxhY2Vob2xkZXI9IuWPluW+l+WNmOS+oSI+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NnB4IDJweCAwIj7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGlucHV0IGlkPSJob2xkU2hhcmVzIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLmoKrmlbAiPgogICAgPHNlbGVjdCBpZD0iZmVlTW9kZSI+CiAgICAgIDxvcHRpb24gdmFsdWU9Im5vbXVyYV9uZXQiPumHjuadkeOCquODs+ODqeOCpOODs+WwgueUqOaUr+W6l+ODu+ePvueJqTwvb3B0aW9uPgogICAgICA8b3B0aW9uIHZhbHVlPSJub25lIj7miYvmlbDmlpnjgarjgZfvvIjmr5TovIPnlKjvvIk8L29wdGlvbj4KICAgIDwvc2VsZWN0PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiiAlPC9zcGFuPjxpbnB1dCBpZD0ic3RvcFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iOCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K6ICU8L3NwYW4+PGlucHV0IGlkPSJ0YWtlUGN0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHZhbHVlPSIxNSI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844OrICU8L3NwYW4+PGlucHV0IGlkPSJ0cmFpbFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iNyI+PC9kaXY+CiAgPC9kaXY+CiAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRIb2xkaW5nKCkiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPuWun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmDwvYnV0dG9uPgogIDxwIGNsYXNzPSJtdXRlZCI+6YeO5p2R44ON44OD44OI77yG44Kz44O844Or77yP44G744Gj44Go44OA44Kk44Os44Kv44OI44Gu5Zu95YaF54++54mp44O744Kq44Oz44Op44Kk44Oz5rOo5paH44Gu56iO6L685omL5pWw5paZ6KGo44KS5L2/55So44CCPC9wPgogIDxkaXYgaWQ9ImhvbGRpbmdzIj48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+RgCDjgqbjgqnjg4Pjg4Hjg6rjgrnjg4g8L2gzPgogIDxidXR0b24gaWQ9IndhdGNoQnVsa0J0biIgY2xhc3M9InNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaEFsbFdhdGNoKCkiPuiyt+OBhOaZguOBruWPguiAg+OCkuS4gOaLrOabtOaWsDwvYnV0dG9uPgogIDxwIGlkPSJ3YXRjaEJ1bGtTdGF0dXMiIGNsYXNzPSJtdXRlZCIgcm9sZT0ic3RhdHVzIiBhcmlhLWxpdmU9InBvbGl0ZSI+5YWo6YqY5p+E44KS6aCG55Wq44Gr5YiG5p6Q44GX44G+44GZ44CC6YqY5p+E44GU44Go44Gr57SEMTPnp5Ljga7plpPpmpTjgpLnqbrjgZHjgb7jgZnjgII8L3A+CiAgPGRpdiBjbGFzcz0icm93Ij4KICAgIDxpbnB1dCBpZD0id2F0Y2hDb2RlIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxidXR0b24gb25jbGljaz0iYWRkV2F0Y2goKSI+6L+95YqgPC9idXR0b24+CiAgPC9kaXY+CiAgPGRpdiBpZD0id2F0Y2hDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjRweCAycHggOHB4Ij7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaHMiPjwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiIHN0eWxlPSJiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCgxMzVkZWcsI2ZmZjdlZCwjZmRmMmY4KTtib3JkZXI6MXB4IHNvbGlkICNmYmNmZTgiPgo8aDM+8J+QmiDjgonjgafjgpPjgaHjgoPjgpPjga7jg4vjg6Xjg7zjgrnjgbLjgajjgZPjgag8L2gzPgo8cCBpZD0icmFkZW5BZHZpY2VEYXRlIiBjbGFzcz0ibXV0ZWQiPjwvcD4KPGRpdiBjbGFzcz0icmFkZW4tbmV3cy1zY2VuZSI+PGltZyBzcmM9ImRhdGE6aW1hZ2Uvd2VicDtiYXNlNjQsVWtsR1JpalJBQUJYUlVKUVZsQTRJQnpSQUFCUWh3S2RBU3JnQWVBQlBsRWdqVVVqb2lFVktvYVFPQVVFc3JacDVGY0FiSUd2ZlZhbmZyNzY3cEFlWWZRVCtObGI3bWU0L090Znc5TVg5dzlTbis3ZW1YMDRmOEwwUytidjZmdjdqNmp2OWo5THIxbFBSbTg1MzFrdjd0LzUvUzk5UUQvLyszYnozL2JyekQrVHY3ZjhzZk9YeXBlOS8zLy9QZjkzL0pmSS85Ny85ditwOGRucWY5Ti83LzkxL3NQWXYrV2ZoeitIL2hmM20rTEg5bC82UDgvNCsvTkQvZzlRajgxL3JYL0Qvdy83eWY1bjkyZnZKL0svYkwrN2VGWjFYL00vOVBxSGZFWDRUL2svNTM4blBnTSswLzczK3I5YWZ0VC8zZjlOOEF2OUMvdVAvQy93Lzd6LzRuLy8vWkgvTThaejh0L3Z2KzEvcC95bSt3citpZjNyL2xmNUgvVC9zMzhvWC9mL3NmOXgrN1B1bitvdi9UL28vOXA4aVA4Ly91bi9VL3h2K2svYXIvLy9Xai8vL2NqKzVuLzQvNGZ3di9zcC85RU0vK1pTenhvZWEwTzI1SjlPN2VKcmkzencvL1NBcjlzbStMY0Vldk52bGdJQW5BdnNFL3lXRkhNNjlWK2JLK2FJRXNnRGIrcjAraUdpZDJ1WTJyekU4a01yMUp2MkZVYnkwOURTY201T1V5cm85R29XM282R2lGU3p6Y21rcFRIc1lObDVZU096d25zUjNwMjhnQnc4WkhjVU5DSzMwTnlKWld3S0xLeDhYRWFPaDErSkVIYm40bEtYVUN2L3IycWRBbUc1ODZHMUhYdWtqZDY0aTcwbTBWNGFNR0N5NGt0MmZsbkt4K0ZCQXRFb1AySnFFcmlLalEzZnQ4akk2eStUN3VmME1mMDE4SGxGOWdmYWJUVmN3Vi8yd3RPaUswaGNEdG5MN1E0REtoT1g3Z1pQUFl3aXpzclZCNGpjMnlkLzNkWTNPZVEwMDVXajZSOG9sb2ptZ1J6aCs0U3Q3TU92Mmg4S0VPS3BhSXJkZStaajRUSnlrT2FlRndSaU5WMWRFTHNka2UzYzZ3L1J6K3V1Sk8yS1dEVzkwa2dVV1NYZSthT01iZHJLNG4vakVWT2Z4SjZ5WkJnYkhvZGpuckQ4RXVnWW8xeGpkUDFaRkw0WnhnbE9hYVdMaG9meVgwaVNPZE90NlkzNXNjRTBzbG1QYlR6Ti9kTHNyMEN4VURxQ0xyWGtraDAza0UybyszQ0QzNk40SjlzK0VKejg0NnFkdmJnNzFUdmQ2OXh0SlB6VGNlLzdvVHRQZC9jY1ppS0p4dldHTlhGM0twMkhPcWhkMkhDcXdSUGp5Zm0xN0p5cWpyS2E2NEdQZmhpSUMrN2REbDVTUWw0QTRwbitkYVNTS1puUUtjOXgyeUgrcklMcHJFM2F6UFNIT21HOFlsTE9HK1NRbzl6M3hGeTV1YzNXRjkzMG1iUDZkOUtNWUhMRWluMHFqYi9maFJYN2JvdGZwSStNc2llekpGZUdYM0V4SXlxeEZYL0dSeEVpenJCUGxEbG9SRzUyWXo1WmJCUlo3ZjYzZjV6c015bWdhWTdjaTg3ZzZXWXdqSS9xelMvTmxqM0VhTmpVVEhSZXBoMG1pd1A1THg4d1UzcUhqMlB3cGtwNTNiVFR0Y1Z4MWJST2xxWmZ2NGZzQXZmSUNJeFJHMTkwRU9iSURuSW1UTUY5SnUvRDZhWXN6SjhPSGNvcW1mUGk0c1R2TUNQTGdSSjZFWHJtNUFrSHdRREJ5aDNnY2xDb0VMbU1tcGkwVjd2U1ZBV2grSGJKWVUxRzdtZDljNjhoQ0NhSllNMGU3cUV1K2UrUEVUakt5OVpLMXM1V0VXODg0TmtjMzVxRm4zNGhFeGpSOVk5bllmL3prVFZEMkNOMVFZYWdybENVcDJpRDk1RjlpWlYrWXh6Q1dGTWpvTk9LMnJHQTFTaWw1cnR2ZGV1ay9pYXlhZU5Kc3VMKzJLYnJldWRVSVhFQlFpY3dqaHhyOVl6S2crYVJjNldTU21uTnRUM2cvRGtOMExPNk9mTXF5ZjdWREhiVnhpQ1c2TU4rVy8xSE0raGRCTHlyZnMwcGRyN2xadlVsNm5YUUVQc251WGMwQWVRajRuT2VoQyt4aDhlRThHYU5vU01BOU5KV1c1V3hDUnhidCtDblVjL2Y5dzNvZUNuZzM2L0pJNlluc1VTYmNyd2VBdERibmNLZEhzRjlZTFN0YVAzYW5wTjJQQUFFaUVhM284SUcyY05rQlNrUGlNeU9zWWRoOU80SGNqdlg3VGRqemFWc3owQ3pGcTJwQmtBZGZRMDJXNjh5MFlNcnJsQS91MGRKVVBLZFRGUEZuL0x4dFIrOVFtS2dCSmRKaXdId01DeDlFMHlEU3RLVzRkOU56bnhjc2NkOFEyYjNnTzFqSkNwOXEwM2xPUFRkY2hJbnlmM0hhM3R4OFNpeVNyRFZwU0hSem5panRmdjhnN2ZzcVRNbU1sMC9QOFR2YXdFTkJzSFZHbkRtejJFQzZ2ODBJaEM3WmM1dzhuZlNOcE9UdGRpRDVPUSt0OXU2K21GdVJsdlpFN2dCNnpYa1ZKMXU4YWdYRDlBWWtxVG5ibHFWblBmN0U0cUJ2ZCtYMS95aVFIeWVKRkpLL1pSbjdrYkczUlh5bUtMUDRkMlh6RTZQcm9vMGxKNTg0L0lIMG0zSmpJR24vZXpmbFhxSHNMczNrYXgzNnNFL2VrNHBzQWZJOTNIV0hUVzA1OWF0RTA5S09aL2hjQlFqVDdpMUM4VjRyRHhOYmRRcUVHTmZ6SHF2bG0wMnExZkRhUXZHOHdoZVpMV0FMYjFjYjZhRlQvdU8yVWtYZlF4Z1YyZzhEdVc0dVR4Q0FDN3FJMWxOWDYzZUpEVXpwbjBVdXZ1RVJZTDhrWHdvYVd6czJJajJncEkyTHdFbThMdDlNeHdhaVNmTWIyTlhJVTVKYmpFOWM5dUJIRWNsYmIxYTUxZjVENzdpd094dDYwSUdSQmsvSVBOUlJaRitBMzl0eUhaMlpjT0lQRjRqOElFV1NJSjBCUEZQcUdOSm9QR1BEdHV2Yk9jeHU3UkxNY2YwbEY3SkEvQzUwWXZ4TTBDV2tTTVM3QUxxUmtxTnlTdTg3RnRzTjh1WVJwbVhSVE9YdUVwT1hHV2FFM0VSazYxWWYvT2ZleTRDcDArbnkrTnhsQlU4T0NsZytoKzk4QVgwR3dVSVJKQjhwTWs4UDkvdzg5UThJeWdvM042QmJjNzhnMXhTd2MxdjFPL2xUcEdIRDRnVHZtREVFclQ0QWkvSGI5K1oxL0FyQ2VZZVpZSGRRQXE0NHBUK01BNldZay9GTTR2QmFjYmxXOW9kb2V2TkhWVWJJdlNLUk9JZlYvenB2dmd5cVlZYTZsUFJLL1lVN1hybTgxTGw2ejRCL3ZYUGtBb3dFUFNuMkJMY1FBeXp3RDJJempJVGRoMWNmVUJnR29Yb0N4N2l4NVdKcEw5N3lIbzhVL0xyYldmRzI1ZGpTbFhTL3N1QXBzdVB0VlVJOVZiQ1FPRngwaS84djkxWU5QenVkZnZRaG5ZeUJQVXpOVVRwY3V6TjZSRDlBUG9VdGgxNXdxbVZ6RFlhVXRBNEswVDN4STI3YTE2YTRpUUd5bGVwcEE2SDV1SkQxS3c5K1JrOWk4QU96d2x0Z3BoVC9QamRMaGFnMVN6dUxJMDBwbVlUSk02S0ZUT3l6OVE5NWgvaElaak5FVGYxUzZ1aG41cHZ1RXhNN21mWnZZdWlsRDVQeXpFUHJpc2NRWVlLNHBKUG5WRFdUMUh1SVozT2RraVl1NGZHTncxNitGVTYza0hQZWU4S2RyZXQ3cUhsK2Zhb08zT1Z6amVmVjdXaHZ2Q1JRcnJTZWhuQVJOMnpKVVovazM4cnlyeng4bGhicWNwK3hEeWlleFV2VWQ3OFV0WFM1Mk5mUjlMa3Z0bi9QUVg0OFNYZmtYc1o3andOOEFCN2ZhZ29YR29WUU53RE8xREYveDBzcWw3b0pLNXRCQkhxR3pqTTVjZmZ4MjNnUlNGZC9JbjhzaGVycnQ1K2x3VkgxMEsxbnRzd3FXT1ltem9DRVNxaWhka0YvTkZoWlR4YWZtb3daY3puUkFsSkx2SnhSRG9PYzdGcmtFQkdta1YxMTFoU001cmNiQkpqQml1YlNxTTY3SFdQYjdxYmpxdkNGVUpFb2psSVVRK1J3MWdOWHhqa2FFVTNvSGlQNDl6Y2ZEN3JFWnI3d2JCaFlzVmJIeGtpQWpISmNDWXN6N1FOaFN3STBpUXZXaE5vcWx0QURRblBxRFRuWStkU1ZHNmZ3NGlPSDM2K3VUZG5lTTNWT2F4ZktnRU83ckxkd1dZdzh5TjN4ZmYxTTJSWFVFUDZWN21zRXExMXoxNWk1b2ZqTWtvVmFPWHAzNmdIS3p1UGlDMmZYYmV2aWRWQjZlMTlpbGwySEZHNi9QZU82VlJlaGd5UEcva09QQkQ0d2xXK3p2YXBsT0ZzcCtUN2hXYndFaEhPSE04eFdTa2U4bmtZbzMyV011MlNnei8xSEtEWThUdGJPSHdPdmZ2RnYvZnROVTFyakcwbWlSVmFOaDVsVGxzZlB5bHlQYUM4VnZ6N205RnJGc3hSajlxcHcycTJxZnFYeDlkV0JMdnNTY2h2VHlJNi96Y29qNkJsaU1sdWZ2WTlYbXdjZWhWZVdJQVJyYTFlaEZ0RzRPV0h3TlVLdHYyTkRmM1ArQnlsblRpVHpzWXZIRHJXMnVyMkkyQWNlOWJMVHBoOStybG53R2RTWjdtekdaOHNuOUQrNGpSd1lvVEtyeDV4NEZmdWNsUHMwamZla1VkeW9KTU40Y3hVN3VZVFpmV3JncXRXOWIzWThDR3FvRUxoM25JS0p1VFdkaExwYTJTRm5vazgyY1VKbG1uRkxuZzdqQm80NlJxTHgyMGxDM1ZmcENYL1JWTWlpT1FZajQ4cnE1NlVsWCtudWpka2FXc213R2Y3S3EvRmt5WFRYUCthY1ZnM2FiYUR1YWw2M1d1K2cweHRzUVVUQmg0ZGt5K1A1QzdBWFhYa1J0aWdScitpb2QyeWR1QWVUQ3BWQkQ3T3kzM1diN2tHY0ZpbFZQMmJhVi9VeVl1Qm5OamdvN0RUQkNwaHB0NUxmQ0t1UVR6MHk4M2pnMURJTVptYTEzZXVZUStBV2MxcndobmwrT3FJa2N4MFJmVVU4NTRlWkdUYTdUZmpyYmtIdkRKTWJwVk82OEhZcTkrZVlrL1M1eWpMVEwrdjB2TCtYcks2bTVUWlpoejd0WDYvandxWlJpSVgxU2pyempSQmlZS3QvM2pNbUU2WEZRbmVPcU9IRzZWaU0xMGFkdXcrMVJQMVlDdnVodnd6d0lHTVU2Z1dlLytSWTY3M01aQ2VXZmpTdXhsSXZ1UmtqWXptYnZESk1Ea0xYY1ZrOVlVTk04SXdjWWVVaVEzOXdKb2pqbStNSlJkc3Z6ZzZmMWdoVW9tdDdvQWc0VW1GbjNxTzdIaGxPd09iZlg2SWJYT0hJV0I5d2V3c2VjbC9jdW4va3lSbFVqYlNtTi9rb0xXVXh2T0VNamNEKzFuTUd6NElwYU5kKzdZUW0xMW5FcTZMWjY5NHlKeDNxQlovOUdMMlBya2V5Q0lVRGRWc1puLy9TRFZsRkxOYUE2bkU2c25JbHpNN0c3aW1EaGJSUjhtYXU4L3NXZGhNYmZhZFVrd2ZqSStrVGhGUG5JTjc5a0FkYlVFYUtUdHc3NXBBZWZjdFMyNGUzbHpyTFl4RVBuNlRGTGQ3SW9CZkY4UUFZNDd0cHNyRk1GdUgvc0kwUXYwcGVtOW8rZHJwVXZ6QUNaZGl1dU1NdisxSUFtakwrWlh5Z1FUNjdMWDVUZ2hNUXdBV0hmK00wT0ZHVXZBYi8yYk9HTE4xeC9scm10NHdCUzdPblYzV3RHK3ErYmZhNTRJOThmcit3NHkza0paajlvVU9XVmhETlE4YnlBeDlhdDEwR1RDVUk5V2g4NUl3T05NaTJQWXBkMW13NDNtQkU5elZTRU9XOEVucmFDaU5DejBGUytKcWxMOXYzazk4R2xVdjgraXV6eEdRdmZQZ1plUDNrTC84YndEMS9BWDRIb0N5RGx3YU8wSnJJaWRFZHY3UDQyNlZNVXBudUlvcHJhYXl0YTVtY2xJeFRvdHdIZTYxc2pTUk9RQUpCN1dzZUd4Z2tIV2FVR3R5eXZrS3o2RUw5V25tTFBWdXZjK1M3RnhObW5kSjh4dGVIY1U0K1dHMHluZEQ3RnMyREU0WXRqVXhvcXcySk9Tdi9BUlhsbEVxZUNxYUJwbVZDRGtVbXBLQkNVeFI2RGNvbE5xM0VZSXlsQkN3d1UzNjU1WmI4Q3N1c1cwM01SQ1o4bW51WWpudzZCZ2VTU0FONWEzdTVZVWk5MmpTN083T0F0WTkwOGNnVndhUVZycmZQZGRwaW5EeXUrT1I5RzFmcE1FM0w2bEhDd2loRmFRVE9Ia2pWekRPZi84cm5GVFhxVWZwRzFMZ3duZU9UN21rdlpEVXZwUlhCSy8ydnFFS1NyOW9KaDZiNDRRSTVXMGZvbC9pS2hTbzY0SGs4dTVJVjlmZVphT1l2S1FXOGF0RHlweERjc0loZ3k2UGN2KzNZMDF2bytOcFJKZnpkWS95ei9pUFhyN3AvNUVqekxvejZUSmRuREsyM1hPRzJJRmM1U08wZitJcHJFV1R1QndsZHduWUJBcjc0czhCNlh0VFQrMkMraCtRd1dEWkJCbkpuZjZmeEN6VzlPNHRaNHltV3dndnlzK2pTdXBmSVpMT0FLM3krWHFON3Y5NThrYU5NSHhwdkFwYXc5QXFpK1BiMm1vSHJZUjMyUWtXelpKbWY4YlBVWWN0dVBRVy9saDBob1Rhd3dCUnBrWHRONUJGcGRaOUErRUJERWkreHhuTXM3aVk2YTVkRDdScmZIejU1NE9ITnBNSjZLYVN5NElRMFhFQmhqYkhXcy9mUGVLakFQTzdwZjZFNkhzTXRUdGxvWUN6ektwV05DSEdFcVFvQkNxRlU3cEdFQzArbWRHdEtMbkxNVklrNjJOQW0rdllOc1NQRkpmdGIwTWZza2wvbGN2UHBmaFphcVhUQ1hYSEFMdWRIZWYrckFueWQ5SmNCQ2IrN0dLVVZFQjVQOGxRVjBBSzhPWlFtRU0yNUpYRHF0YVpoYWtKTjMrTWRlMzVrS2lwQzZ4c1F2MUpIWkprTXNMTVRybEtTdk5KMzZyTXlFZ0ZmSXJ3REgrS2paM2ptWDNIVnpDdFI2QVp2STY3MHdZMEw4VkoxVkZsSWZuWml6ZEtUdVNxWGUwTVgvZVZxL29qK0ZyR2FIVkFHZ2E4OWRHTDZ4Ly92TWNiWkp3YjJBcnRuMEVpajRsQS9UOUpuK1VBZnpVaHlmdng2WU9GL2oxTnlHZE1LRDJPZkZ5S0JneWJVZGVVODJLZisxb2o4dHlZWStaelB5MGpCS2pHbk5MaGFvWjU0TjNhd3pZS2hwMUZmeDRsM0NvTFd4NE9Sa3NIbmdQeWpndVpRbWJpTSt3Z3hvSy9wZDNHWjZSVWl2aXJZUlZmaGlkL2p6V0JDcXpEZFM4QkhOL1QvZUZNbms1OUx0Ym5jV1JTY2JWMDA3dDRjcDNSZHRvQ1NIckd4cU55M0IrQzlDMzdXdkRsSUNiazBBTytMNlJ0amUwaStuUStZSDNGVHByVHJSQW1hZlkyZUZuM2d2bEFZNHBkb2d3T0xMcUMwS0J4NEFnN09OZUdEOGpncWRIckt3ZXVPM2FROXYvSmNOSHpscmRHZmIwWmFBN1praHFQL1NsRHZUYWc0VGFFSTVEUHkvSjJSR0drbVE3UDhNUmpNbWdiS0lLelk2MXNzR3NIMDNKOWFCTnc3YTI2UVpzNmVSU0VVdnFtV044MGVXT1Z3YklGZnJqWXAwTUpSODhuMFYyLy9NbTVBcCticmxBbXNGRGlrQkNuYkpWdUZ0OWRmZ2VvQXdteUdUOEZwaVdqQ0V1MlBvbTA3Z2lmVzRqQWM1bVFtaDVkUXZtb0ZjYXJWVUlNTVQrL09nalgrTWhtS1NwcWNZeXVMdUlla1lmdXRFSjE1d1RJY3krRStKbEhJNkxyYVBteFF4L0RHUVU0cCthOGZWRkE0ZmNqUGYwQWpWWEFOK0UvUjBqMDk4emo0Tm1td3pHOU1TVUduMHNSTDNnQTZwR0ViWkRnVXY2cnFKN0svcjFQUXBOQXpsVExsLzFvbDlNV1ovZllraXlNVHZlU0JOZGN1RjY1TUFUTjJ2dDRCcTg0MGZVR3BMbGlocUZZNzdheXFnd0dYNjlGNER3SFE0SDIyd2dNNm1TZDBJUThxYThzdUVyL1NLSjVXbGIzNjJITzVKamZ6NzVSY0NTKzM2VnpwSk1NcXVYNDRkN2NnY2VjLzh3K2ZWNGZFSXFmWGZiQVlVRnhEU3lzVkxiSWZwSXAzZWc0MGk1SVJEOFBrZ2FENDhGQ2h6WUk4c256TnAxaDd4SEdQanVkUVZNZjNPdDFINGdHc3REdWNTRzZ0azVJVHIyMjB2d28zQkxTYW9zZkpOM3JFOE5SR0x4ZjhjYkpGdDFlWVl6ZEVXbnN6eGx2T095VVlzeW1EcnlKdHpUTHlSYlhwZUZ0YloxZUpGa3ZKNy9va2xzQVNScXpFTW1TQUQ2NVp5bysyQVp4ME5vejZ2ZVNOTWJtbFJpU0tPTG54cExTNGU4QitUT25qUmo1ZHZMS2FLbzBFZC9pQ0lyc3pVeE1CTnNZNWpQbGs1RnFHVHgvN3RYV0czUUFFR2tSbTRQcTF2SUlXeUlzNjlWMnJ1SlhZUmJGZW1yS3Zzem85eVVYOVFuM29JbjZUNjNTNXVLSWpLMHhRbnpnOUdnV1M2MTZRTFppNTB2NkZESlRKSkQ4ajdxY0hKYS9Bc1dVdEZ3WWJka2dnRWFBWDlxcHBZbUFXaUducmxZaEdsSWRpbkNZMWpSTFQ0eUsxWmtrVG80b1JvV2t5WWdXdUhTekZtUHF2MVpSaFdmVW9zUklBMEVIeEhQTkh1eXZrS3RMV2Z0MkVBeUdUdTFTK05sKzlnMDFWSGc4UDBNbGp0Qm1qcjNmempXc3A4ZC9sZUg1OVVDditnQUEvdjdDOURjMkxaZ0FXUng5MVJIb2puWnhMcDNITlpVMkxxYlM2cW5aMmpEcFFReERtbzBVVFh6eUUwOFNhcUtNOGRXdCtVaEg1b1RIOEhGQjVBVUs3RnhtRGtSS3NJVlQxWTBHWGp1a2FnTjl6WkFJUDM1aENDQk5LNWVzRG52cEk0WTVGQ0w1MTZWa3BVYVl3dit0WkhhY3YvWFpIdHduZ3RYYnJIZlBJTGtrK0pFWG1FcmxkUHliNGNNVExlK2c2azc2ZXoyM0craW9YMkZEN1NBYXE4NjFDZDVNM2xjUzFaam5aaDFFd2FxN2dJaElVQTY5UC9xNlYwM0NscHQwRUhkR1hxZmZYUWI4KzFrdUNzY05tY2RFVG40UFYwVUlHZzU0Z3ZhY01Lb2twS2pPRDhiMVpwLzRXRVNDR3VYTkwrc2JUZ2NaRmxJZlM2SmlaL2lTMHFvb0NRNmhpOFhlcTVjbWFDc0RlNGUrUWRmTFA2dTYxSlkwUXhlU3lRSnowSk5uNlNuQmoxQ25sZDRvQ1llMEFmUEZQaDlaeUgydTYzYkhCcEQvM0NPQzc3M1BvWm5WN05kQTJiekRtamFiTlJuZi9HNi9rU1phclhSN21oNWd1bW9tdFk3RVdiZ1lHQmtCajFpZzFEb1k2a2MxQ2FYT1hXdjNLbGpKSTN3cHlCN2w1UFVIQWFkL3dkUHREcVBtK2o2Y3dnYnJjMWMxZmRKakhoTWFyWXZFWkxyWjlkbEt0OWZhVi9LYlg1OTcwMlBNR3RPWmJEVm1WeW4xekVDZElaUEkxcUFwTnl3NHJvZS9MeTlGWDF0TnRmS1hRSEY4ZVoxYVVlTUlOU0d4Y0ZQZnYwbnloQStVYmxIZzEzNnpOQkowSW9HQWU2eDk5bkhhV0pUSWh6b0FtczdyK21WZ2YzaWVxMXRaMWFwMVhCTm1RK2xIY2JSNndGY21nVjZsK2wrd09reTJTNWJhS0lRNmRRa01XWUJZS0Y0UERxL21qM2JjdzhOQ2dLMWVpM1NUVy84dTBDNXRsQ05LQ3VHbHpDVVNwMURvbjFnNjAyeFV0NjNrTmd5amd1aHNnT1YvWXpkMmlvTW9tckRqdUNpK2dRanhPQXE5V2NUczBMT2JiL3RmSWhrMzZ0amVqMlhRVVZyY2dMTGZwa0FhaVRVczhQeWVDYkFEa0ZKZEx6QStHY3Y2RCsrQjBxL0xQanBMRGQrcVNRMjdjUTJwUVhib2tlWGN1V3FVeWtTZDhzNVUxZGt0NUtBKzRPZ0pENmFxM2ZucFZZNCt0WTNsVEpYaVEzU1I5SlEzVGpXOVJXTm1WRGVBZy9JcGxIcTZ2dXhzR1NWNzQ1TXROemFYN3RIaWdJTHNzUzR1aDhIR2J2L0FJOVVzbVJQYXRJRzZjcVIyTjRLdzJFQ21KYktHNS9LV05mY3NWS3hzZk1IelVRdjJTS29HZjhRZDhjeE9KQUdBR1h2c2Y5dDZkQzdkdXBkaExUOVF4YUU4bkI0TllSWHhPRTJiR2JhWHp3YmV0WkM0djZqSEpucld6NFpZYTZRZllQY2xWK3lGdUxyVXA3RUFwMUlJdVo0UndOZWkwT3pHZXRlMTNDT1FXeTk1aWR6bzI2SU94RUxEeE9nUkRWOTNjSnFIeW1DdHVhWnFOckFCL0NZSkp2Q20yeWlsQXJIbGtSeWtKeCsyK0htL2dJYjN3WndtU1BXRTBzaGRHNVFWV0lvRjdCQ0hsZGFJakdZK3VxV3pOWStibkpoMUQrSVJhODdkMHJKYjlMZFgwaWZ4OUhsQTlYVWs2TkhRenJON3JYS1BXWjdQWHlueTJZVGpjNVBwZWYvWFUvK1ZtMzVlTUV1YStZSWpaZFJBODdJR2V2MC93WHFMbzIzdW5LYjlHSWtPb3AyWUhHTVo5eVkvU2plaFdXRWg2MmZFU1lZaVE0SmtZWk5aT29PL0cxSmhGYUwzd01ObEJvd25yVjc2UkpwV1grNVZLeG5PY01nZG1ZeGNlSVp3MEdUTWhmNlFaYUlKT3lNb25iNHVFS05FRldPT2N2c3VzTStHblUzN2JrbDFmcW1zVzUvRndFUHc4K1BockU1TE1QamgxaVpyMHF3TzFBZUV2OVRTRW9Qd3BxTnJZRGR6RkZUa2crTTJQb2FNNGFYaXdUdktnbUJEMDkzcytQY05uSXBmZkpwVTZHTE9YVVJmK0srYXZRTURXc3V3RUVnQ1lZeHpyMVR3WkZ4R1NYNXRWN0hRdnNnQ3lWQWtsb3BNVmZVcFVJK0lZd2wvaE40dXhUeFpJMzhDbmlSd1g2VDZEVURRZkUwZng4U000b0ViZ2RLb3luaWJpV3o4WEZrUUs0Q092VnNXcHB0d1JFOWJ1NUtOekVqYUdTd2ExTG9tVzRqdEsyc2RmbTNVWE5GZFRGWFZOSHhvQTVIOHNtU2FNUjZRdDM4WU9mRjlTeU1aTTRoZVBJU1NHSktLUXZ1NDU5L0x2WEFpRzdQWnVOa3dFMUxIcys1VHZMNHVvemxKcHFUb3Q1czYvTDcyZkwzeE82WkFxMGxxVU04OXhJcmQ4dnlVRXJyM3RYZHUzOTJsVnhvQytIMkRvM3V1bXFlcVd3d3VqcEhKTGl4cHZaU2w3aUFmdG85WjNIU1BhRnJ5K0Y4WnFOSUcvOWtYOXZkZmt4alhIUlYxWkpRVmRUbTc4TnlTUWlGU0ZEbnozOUpsbkRMUENIWk5oMC92S2tGc0lpSnBPVkplV3luRFc5MHRzbC9vVCt6TDU2RFd3ZDBBZE1HUmlQZlpsOE5ZUTdEeHNlMFBkVnc2ZWJhRHdFTzVPNTRHbHUrdmE4OUwyVFhLWFpvUEdTczl0M2lCbUROWkNQaHRxd1pUUjZqSFhUdVJOUjBJcWJjMGQraWpQdmJyV2p4N1N2UzRFbEYzVEZOdkFhZVp1aEgxcUVlb0d6Skw2VEhQZ2VxUHM5YzU1dzRXL2pnMVVyejFoeDg1R00wUkJpRkJLTTlmdnJXODU0K0x3LzJOdFA5RXV1aG5kWklITmRyQ0NSRFdtNUc5SytIOHZQN1JBRTNrTlB6RzI3cWtpNi9yTjZPWjRubE9IS3NIRUh6Z0VWallRdG1nYlR6akhjbG5sVi9mQVRJaEt3SkQzZkxVUnVacGExVlBVYUdBUzgvTTlDeXJRYm5PdjRwQ1haMkgwdHNjSzQwcUl3NjZJYVBBRFJJVkNyK0lhZmVKTUVpVWMreWNCR3hES1pxVU0yTVdRd3FuVGhNUjlQNHQxcVRhaXFjcmVDbXdPVkl3SFNBQzVYMEJkUDJ3MWs0ZkhwTzRXZmhSREZ2cTBDNzk0cGZYYVNYYzlUTVgrb2J0UW5ndGhGdTNMVi9lTXliLzlhUWs0VXRVZkdhcUJ2WXZPOVdxR1pBa1R4dEdWS1lFVWxyVmoyZkNOb2NrQzVhdGg3UVdXU25YVzZJSEJCeFhHdHJHSnFscm1WWFJ2QjZ2ZHNqcDVtRGpUd0ZBbmRybXZGcy9MaDByY091VjVSM2pkUHFIcUlCeE55VGprN3E3QU9oSWFydjBEM0dqZFN0NHhHbkJxUGQ4bndlZ3R4QUl5TE5XTS9QNVNjajh2UHRlYkRBcDZyb2ZIZmordmlMaGxTVis4TUNzbkVtUUpxMXNyaFlXbTRLZzA4Mk5HZmlTRzMzenJMRXpHQk9Ia2ttdkw2dDZRcTZxb3Zsdmt0Nkh0N0xIRW8rTVNVNTFJNDQ1aXROOEZzRU5vLzR0bjZSb1RLR1hRamZMTXZ0cE0rUG1pUlh4aFJycUZrSFpuUTNSNG02cG5VQ3JVOWl4NEJURkh3dVllUmcvZ3piQzBYMnY3UDJhWkV5OTF3TlpRT0RGcXdWR1p3VFcwQlF3MFZodmF2MDhsb1BGU1FFNklBMFVzYUlXQ0Z0R1JDR2RuV3NCekY4dXdlZGZjdE9hOENWQ1ozWm00STNING14eGZnQ3pVcWhJcGc0TmVVdWZxdGpLbTJzZStZelVoRTdGVExROFI1OEZOWnBRTTdZWmNHdk5ZbFAwM2ZrZmU0ZVNGa0JiNlBIbFlpWHVVQ2E1VVd2ZG8vZWxJMWprQUVhcHU5TlJMZExvU0ZKeHNhT3dDNHJmd3Q2NGI1S3c3Lzc5S3kvaUNNUVNpYWZKWE1rNStGajY2ZFg1eVFYOGF1QTBGeldDZEMyZVkrNk1vTHdlbnUzbjJ4N1NGc0lLc01rdTNLUkQ5b1oydnZCeFNpYm0wN0ZCYWtMV0Rxb2pVQmNvTmFmdHNiZGRsRlpONm5rZmo2MEZWUlNEMmhHdjBxUnVxRUYwYVFVN2dETUpuWm9LdHhZc2NBOTloVW5SdSsza3dJZWE1K1UrQUJ0dmk1SkZybDFEbk5WSS9uWjhmWTQ5MnFydmZJZTArQWRUS2tCTFdxSVpLZ0R2ZjJjR28yVHZ2RmdEUFViekdwUm5BcERuK2NoVHMva0xJcmJFOEF0NzJFVmJBeFBQRUdaRUpKSFpucXJTSGp5ck9aL25VU0tDUTVHODc1UjhPNlo2bm8vYUh6RWV5VEQzV0ZNa3RSR3poYnBsY2FERGNuVlI4SkxHVm50R3Q3bm5RR2YrajNQTkM1bUVFRCtycVlLVWtab09xQUczRER2YXJqN1FKWWc3RmYvdy9mTmZWTnJDejZDQVE5N0VIdEhPdDFNWHQvKzZacC9JR2tJRFVrOHZneVVqakhPVERGNWwrVklFQTcwTkhiWnlPM2ZnR0tBRWw0SWticmxYUnZkNGRTazVGSHV5ZEExMlBCM2dqZmlHQS84Wk1yNU53dGdoL3VQVXdyb3Jqck9CQXpyK3Racm13cGpJeDBuaGVNK3NxZW9RU05WTTBuV2NLTTFXcVo0M2lsaEkrWmpZWGRnbW5PYVlvaGJVYzBNMHRsa3RHY2JWZUJQMG9VaXZvZ0kxK21BVjMwd29IMzZIRnoxUURndEgrKzZFQjRJTnlCZG9URVE0TWFQU1NRQU10SzBFSktBQnc3Rm1oSnAvbEs3WkpBU0NSa2FXdnBGTmJlSW8vM0h4d3dvY0ZYV1R4TndjWDNXN3g5ZnlWZ0x3aEZPbDRYNEVPSWhJUGVJOXVHNjdhWktScTBMMUkvZXpKR0tqWmk2a2k4eDMySGFYRWYzQnA4cGxMTGFJRkJqSEozNmFiTFlnNUI0emlXZCt1aDNXKzBKbWhkUm5XcnpwM05JenRSTUc2bmVPY0N1UC9mUklVMGRyLzYrcGpOb2xadkJjbENuYmxGYmZLNU1tMzlBUTBYRWp4SXRLaE5VVUNnN2lyNXB0dUdQRjE2bjNRdEFzUW55UllyaDVjZVZSUGVTVThUV0pvZFJkWTM0UkFlY0luRWFYOVgyam4zQ3EyaGtJQlpvc29kMzhDL1krTkpwcUpTZitzMnU5aTlGUHI2MlRXQm8wdGlMMnFpaHFCYW5vRVNXeGVIZW4vTjBGcmFSaXFNSE9FK1hqMnMvTHFjVWFsZ24vSUdPRVVnaGFhUlNRNUhNR3RnUUROQ2ZzN3Z5VXd6Zmo5OHVFOUxNcDgyU0ZVQlZTanpNY0F4NTJEb2dSRCthNDVCZFpWVFltV2VDdm5jVHArSWY5bEVaQ2tYaVdHdDJNbzQwdW9tbHBwTVNiTi84eTBFUWg0eEhaWkFucHlueGZuS3FMU2VYS29yYXNrdTFGanBmRnl1b3FHSlhFQVFYblhwMzFMRmNDM3ZyRUliclByc1JvV0kvQnNGU01MbWxnL0JQT0dqM2F4TitXOE5xZmNzNHFHcmlxaEdZR1ZocXc0RWgvRk5McXpaZUZraTlUSUVpM1hHYm9zQUsxTGZPT29RdHhGdi94bjJpcEZWRWZ4L0Q4TDE3QkVNcVRYZldiODRxR0FGL2IwSVFEVjQxSi9pV2tjWVNTUWJwM0VQZU1EbFdoMmZSR1ZRc3BLMkJJcVhmRlBVTlJMUGdjb2N5ejZPVXNLYUs3VERJcExBckxCRGV2eXRvY3VqU2Q5UVF3Z2N1QUJTK1gra3hIWCtTd3RyVlZsUDIxR3g4U29nRWhXWXBJUERmd1FQV0M5NEE0VFpVSEwwWXNxV0wyUytCcngrOFlGMkJaWTVQOEM4ZnFNdyt5a1QyRnpicm1ZNHNrdEVPM3M0RTNJYUYyK1dkK21NOEMzclVzNjlMVzJmSE5hc3VNNEc2L1Q5N2VDelBrQUVGYjVob0ZyRG52T3U3ZzQ1Um9iV3MxenRVZmFKYTB1Z1p2cmtReXA1NG5pT3pkd09QRjZnRVZoam5DQXZ6bnJEdTBtb3BXaGNjRFlrMXkzcmVGT2Fxa0ZvTzJETlNVMnR1L2VaaGNVY1ZsTFNDNklDbzN2VnFSakRBaVZNOWVKU2s0OU1IaG5BWEVzaC9jS0xoRXpXUSt0dHowQzV0akY1MXdybWdmVzIrdy92cDRLTmhMckxDL3NWcDIwRUdjTTlHeUwzQjlOUGx4TmVaWTFhVldGTDNhU3RUM1drbkFvUnRxOGFhbWVMU3NtVTk5L2E2ZWYzR0IvU1lNeFpXVlVMY3QvV3diWWlWNXVhbEc1ZE5iOTdra0U1dGdYcVcrQXh0YW9pVTMwS0ZKWUdJYU9RKzJSbkdNdjRYcEpvdVFNN1JhcUw2NkxIc0tFdXErTExFUVdEdEwxNVBkOFJPNW9zdHJZQlg0azRTVUVrKzB2ZWlrVmF4VmUzUkxaeDNydG1RazNMVjVUdmxDSG9tWVc3L3BmcVRLV0p2V2VNdjhIT3h1Vlp1WnRwZUtnaGZ2RjQ2Q2lDSTMyL0lBTlRtL2JDWElQM0RBaXN1alhlRFJPMTNSVG00UGZ1cE1lN01ya1c1bVlIWW13Q1hNZ2I4SXZDeWN4TGovNjZXU2VOUEFDanFJK25PSzRzRnoxMWpZbVhoa1I2OEl1TXRyWHBUbS9aNUhubkpsa1FyaFJFRFpkV0hCVnR3ZXh4WDlVN0xDc1JaT1B2bDBCOFB6RmxJZTRKeml2Tzc2TUhqeXROV0V4bDRVYjFjVE14MkU4Y3JRVGpyOWNuRXgyY0wrNG5HUm4xa3I5T0lOZHZGV2UrVlpDUlpkclBJbnVIYkNvK3lkSTc1VUtBcHdScm5mSjJ6VWE1OU43QTY5WVJJQXc4U3Vzd1R2VUJ3UXJhK3NuMGRxTmNxbjBOMUw1ME1pb2pNcUs3RkVKZ2NBVU5kMlZBRU1HcEpmdUE2b1Fwelg0a3g1SlhiZFRBOXlBUUN3amlzZVhnWHY3NzlkbVpVVTNXYjA0aS90TGZJS3ExUDcxbFdiQ1lIbzQ4YXF2OURpcEJCWkVsNWpPWUYrbUpUWlB5Q3dlVW40OVF0Rk83TWJqeE5XVEp5UXJ4dEJKdTVma2w3S2pIcUY1UjRxMWtMT285SjB5VTBTckZsZXdKM1BMUlNxdGVWNXN6Ty9HcFowNFJtVlVSOTVxVEFuVThScy9kcWtxSlJDbUljZjhHRVZZK1JJSWxTNzcrMFdBS1lDcVVwMTZxQy9JcUxsQTVLdkdYUVczODNiaDV6WVhOd3BWd1ZOQ0J4MGlIVVl2azRtRng4MVVKb3R1L08wRUErS05icitJQnd3RWc5d3g3a0dtZzc0T0ovWkk3WnRpNHhOa1IrbFpTaFhTZ3VqQlNLbmJuUHpzemo2RmpvektnNXRCQnJOSk5GYmNJdUs4eUt3UVVLdm9aK01nd3RCeW83QWJZVnBkc2FjZE1Zd0ZJRzhUTGx6R0hrc0lqSzVVZTBEd1llN3FINGtzTFFrbXJnS2NlK1JoUnUzaThkNkkrVjJhT3dmU3NBbXg3KzR4R0dTR3J2dlZVQ042TitlejBESkpwQkFjV1JIb3ZmSHM2Y29oUkJ5cW9pZW9vU2FXbW8yamNPZHdQY1hpSEYxWGpNT0E3YlprQWJDaE5EL2lmejZtdUJhbHhLSlJKdkljZXhrZVJUZU0yVkJJRVhwcGk3OVE2NzdPcFFKamI5ZXNzam1sNk1vWUpvNldvU253UjJrQUlyTDBuT3JjVDdqMXR3OVBuZEFicVRYS3ZrSWd1OXhkL0JGVjR1dDNBZDdBNmI1MXJCNXo3dlRSYVdVeVZxUzIyTWJ5dG0yWnZsMkxxVk1LTFdoSGtEZDYzc2s4ejdzc0xyZkdiOHc2bUNKdlRUdzZZNHlWbWViZFo3SUd3ZXhjUzBCR0dpeG1pL2Z2Y0FTcWR3RHZaTTA1a2JtRU1TTk5tSmhDYlVIQTA1NkJ4ZFJYNkVkcWhTUTJvbjBVVktkR2FNKytMN3A5RXJ5Z1V5QkNHK0dvb0tmTElZb0hxbTBrZVRhV3JleGIrZTR1R1NxbkVhcVdHTm45ZFRwbDcwbkJVSDdZZ0hKUUc5TzZGeFA1bzdBZDJnblFZdldtYXBZY1Z6LzdKUE1mbzFmSUdLQlp1RnJHWUdwS2E1b2VvSjN0SXhKTmVpL0tYdW5JSXpqUkNNNmI1UTJxVG53V0Q3SXdBQlE1YktXSGtOVHpUTDF4ODhzdTBFQ3Iya1VxaG5VOVh2bXpzRkpDK1ZncXlJTlVOODZLK1pGa1lvNVprZUtkTlZ3VGdFYlNtR3F6aUZvc01rZzBzbHJ0QmMvbFFiWi9MSExJTGljWWJLaUdoLzZtTUw0b0piQ1kzR0FwcHBhWEh5eFBTWCtFeElsT2U1dGFWY1lVSmttdEE3b1pNTDZvRmlnU1JXVFBSZnUrQlVuQTFjbmZOUGZwRUhiVUVqb3ovZk9tNnRLRzZSSU1CbGIxRmNEdXZlU0JNRm04NXJKZ1VDWm4zaGFlYy9ZSG4zOGZHOStsMTNlaTk4Q1gyOVVmYUhXeTJRelo2Y20zTFgrWFg1UHZQWHg2RUMvdjNUaXplZ1F5OGxaSkl6QjFXVTBjU3I4OWU4VjFBWTdzcmtXZis1NC9SRE4vMG56eDNiMlZCRlo4YmV3em9kdnFVSldpS0wzdE1wTDZ2U3piSll0NGZiY2xmbkhDRno2RzIvQ0svREVDL0wrSGJwWmJYZFgzVWE2VGcvQVB0R3M4bno3ZHpZVkFDTC8rT2x0akRWS294TlNJcGhya2VnOXg4UDYzSWxxTUI0TlNmUkZFUHRiZFduTWVXKzhOZlVrdnUzM20xOXZhZGFKMGdWblJyRFUxTGNHbVJVRkcvSnkwU1I4djhzVGRBeWh1a3RNdlF2Ulg4TGRwYzQ4OTdmUEtMKzNsc2xLbHlhN0hISVFLdENteENISjlsWUUrWTIzMC9oZWIwNEZ4WmtsNENiMms5ZWhHWmZDL1pkaGVwbUY2Ri84NUhmMnhYblhJTkowcXdJSHpaVklydUE4WnZ3WW9xTkdTNFkrMmlSNnNiL1d5akFIa1RYVndkZnVoc2JWbVVUOXFVcm9rVlFkOVdPc0JUVytTb05sbzBRWGpaWHlEVEQzdnZGVEE4T3ArUkVxL1FGMTl0TVZOeHE4N1BpUm0rSklrbU9vVmJVSDdNWEpYSnY5bUo5aEl6KzBkOG9WdlFiM1IyYkU3bkptbnRaVDdSSjYxSEFVYmZERjlsRmhINVNteVpFbDAzUjFuVHl4UUJHMWpHWS9KL3Q2ZXlvcEp0dStzeFAyUnNwWjBzVEFUTUlMaUhkNjh1dHhNb20yaTdpRFpncC9pbFhwQTM0cW1SaDhuRU43YXh5cmhsQTFjbnRTcGMxSTROZ2xpQzVnbTQ5cEM0SU9jVm1VaTIweGc2RVdYWW51ckxUZzhnK3dFa0hxSXA0a2pwMjdpczdhMHhHZCt4SkNqNFJHV0VzbzN6UitXcmJ1WGgzK25PM2RvQkJ0RUZKcGZDK3p1dHZieFc3NTY5a1dET0RxZHl5T0hadDJianNoZHcwN1RXZEhvekRYNXAvQ1EyMmpYL0c5bUExNjRzQWppQlRzS1pSM3RQS3JkMzVyVmljenFkNUNobWZ6ZjNiWGJFb3NIOU9ySjduMUg4ODFxcGhIeXBaNzg2QlVYS1FpeHBNTTlqekJOTXdrNlJaMURQWThMM1lXbm9wWUlJcm5JR3lpTk9ZS2UvV1RaTGxBbTA2d24vSTU4cmhnRnBsUTM1SXlSZTg2REtQaG9RSFo1TVlQeW11cUR2dnIzakRJZFhjWWU0TXVCRThLV1FWKys5TmtMTWdOcm8vbW53UW1EYUNTNEdVdnV6VnFWdGMxQWxOUW1kdEhveVpzVkRRamNaVWVkdXFoNzlxUGQxTk12amtmOXBhNnNhTkRKRzlQekdRdFRUY0JsMHhTT0ZDSTJDWkpnNk1ZSnlNWVB5cmlwQVBVVXZVMnZHY2lTZ29HeFBpUXlnT2VHRU1vZWh6dDdWRlVxaGJ1N0JjR2JyeFlWdXlsN2VkQXYwdGpHejhlcWY1VnBUL3dFazFUdG83MFFzNXJOczcyNk5STkZDa05mZFJXQ2hTK2dkWUxRWCtWWHZCWlJkVCtwMmlyOFdQQ3gyTzNHNW9IN2N5M1RKWmNhQWtVVzViNmVKZUNUWGVzOWc5M29xZnMwMzdidkFpTW9HUVBJZFROM2d5OHQ2c1BYV2dUMlovQ3o5WElkZXN3VkhPNGpoT1FHNWdkT2lRem5xbmZmNnRGVUZydmN4WU5uUi9mVUJjWmVxcW5PamJBSVdXNVpVVmFOUklRWmtITGR0T3IyZ1laejd5WjRPam05YmprZVJwT0J5eTZFaE9wYmZDaG9xMjlHN2xsTVZQQURCK3JsSEZUSEhHNFZnZG1CR00rTlgyS0JidklpMkZKL00rRHJVTmRCVEZRSVd2N1ExODJtRnpYWFlRcDNXSHVFMkVYeGVjNXQvUWdVYW4wZ3NWSStkL0VDSnJhSXhrV3JGVHZBaExRZlBPYmVmSVlNZWtiZW8ycGdjUHBuRlR6MUZZMWxkbTFETTl1Z3RRRVVCNWhSTitUeTNTOHU3Z0N6TjhieEVWazE2VDgyNktIUm11eTRuM3A0cFNLemNyNFg4NmJBcFZYRXdGbmlSa3NkU0RQTkRvOVVHTXdNN1JoRlVTUnJsZno2NHZpTGdOa242QndsZkxmZFhWRUg0WjhyZy9EUjdiRkxINlIrM1ZWUHdENTJDbVo5aEZxQUMxQy93R3ZySkpMSldpcmF4R3k3OU41dGZOTFliLzQ0U3FlcjgvZWIxenB0N3l3RnNXNVhTN01HNU9BNHYzNVFRR2cwbkdhRzNTa2FvYURobEpDbmQ2alNzNkFCeWRCamxnSnFGbVE4Y1JNZkROLzZibzVWY0h4YUEzYzNORVZwT20rOENUbFpHdWZCS0l5UUJ4OHp0U25mK1dFTzZOQUdFdElUWnl4c0gvN2RzODcvNWRIekxkY0pWampLVjgwdDFiOGN3ak1HSmVUUmVEZlF3bmlId0cyaUpDcFQvcWZaYlBucXZPNkg2S0RYZzBHS0Rid2IvanNHaE9Nemh6N1FhZFJVY0hwTEY1cmJNQWJBQzJ3SzBCbVpUK0F5OHp2bHR4SjZnZmxyMHQ5NUhhbXRZQjFaNU9hdVVmWTdvQ3c2bC9ZRU9OdnI5NXhscHdpS3pma25iTnZHakkydEpUN0ZxckZnMkw5UDNYZnhUbk9abGZNZ1l6SGlSNDd0RGV1TWpocldLaFdMWnB5SmVhYlZqclBVTER1eG5KV1VYLzgza0FhRnNMN0owdERnSkpWdzBISGFMSUpMODdOaFNDeVRVTlI4WDEwbHhNcW9KNDF0Ymg2NXltY2d5bmxZWnQ5c2t2SzZzMHZWZlNDTmRRWTVEWFM1V3B1NVdVd1JYVE9iY0lNZmdlZGhGY0psOEdFbTU4cG51S0JqL2ZDVW9YZlVRMVN0eG1CREQxeUxRRStZK3lVWHhkUHkrZGRiemptMHdJTHlHc2NreHdxZWRVbXVYbzNqNVJyZnl4amFhWTRTTWNKVzdXU1czaHNLbVdtZVBwRW45RDF0Y2Q2d29QTkZYY0NKNjIwdm1sVnlGeDVMbm43RDArUkJZVUJFVWhtZ3VVd2czZnhEN3Y3Z3NXK0dnNVZjbXhENkxOQ25waS82ejg3N2N2eWZDdEI2dGVxMDJqNzl0WWVyYzhVRFBiYVhiVnJwOG1Va1BKa2tGNGQ4NEtqNXMveThIVGtPSzVFbjJpNjNPRTBtK1pHSkx2djRzdlNXek55OS9ic2pWQlBtMmwvVXFRUjF0ZENiRjh1NldPQ1JBZHhLajNhQ2J3azRFdS9qM0dhMStJenNXbTFHdGk5YkZTR21ESFhMTXBVYmw4VzJpcmN0WjRTRkVxTHl2Ri93SFRDV0M5T2ZzQkluQ2lxWHFMSzhHSTVtazFBem5kUGdJb0FzSjlCQ2J2VGVOZlBRM0tzbXdsTktsQmhhVzUzTFovdUJUUnJtOXhwNXIxTW1JYXpjV2lFaXVqNitFNDVJUDVDS3E0VXR2cU5OVEw5Qm04WE5MaXRsZ2dONjVDRXlNd2dWcnlhREYwT2JXbTQ5ZGtxV0YzUng3UW83QXpOckV0UXBHbU5waURneUZ5WHRqSzVXSmoyR1AwQWQ1YWhoYys3WHBZRVNTV3orQkx3dVNyNENBRTV5cFAvazdPUzQ2Vk1TSy9GalRRQ3FubFlUWVB5a21nVnAvdDZvNjJRbldSVG8xSnFjbDFoaEpXT2JYNi9JYlRjMGdtZHg0WVZiUDVleUFaVDhnaFhSK3BmSEQ3cjVPd3NkaFlPdk5zVm1tU1BFeEw4djdlYko0SDRUM0RCVkhuRXllZ3JtcGNXUE0rRW9uWmVBNlRRSjI5L3l4V3B5MnR4c250QlhOS0ljeHZQM0M1L3dMNXlhdFF6bEZjL1V1amF1dit2TVdWWTY4MHJnZVV1cE1vQVgyMjJGTm1VZ3BGNEJpRUw3bHhBRmhxeWVQUlQwZnpydzhZT0hUNHlwZHY1ZEdzWmx1ZWY5Z1ZWNGNuNW4reU1PaDhFT21oR2xMRmwwcDg5Sys3QU1TZkJiUUZ4clVTMGlUZU9NaFJnSjQ4QUNSWjVncUtBQ0dCTWY3K241Wm5EcFc5RGNFK2tPZlNuTXlhekxnR1dQZ0pQUE4ybUYvTDgzc3NBOGZjd0JaZDNjUDlTTmFvU09JWUxkWEFBRUtKMlFzVFFQc1gzN04yTVNnQ2NyOTdVejE5c2t5ZEVoOTYvenY2WXlITzR2TmZ3eTBIaVlHeTB6aW9jU2pmbDA3U1lOT0ZNQ3dNU2N6Q2ZKMXRWbXk0ODFKZmFqUGhkN0ovTStQWTludDV3M21YYXVMQk8ralc4QjFOQlZhSUI4NXIyclIzZ3d1THEyM2tpNXAzRzNJYitQVytST0FJSUJ4aHphSFZaeW9TTjlVNGQ2Skp0bFhUWFZ3MlIvVHlod3Z6VERqWlV5REFobm83Ly9LbHYzaVZLYVk3M1ZET3R3YkF2dnBwRmFHK00vdno5SHN6clFreEljVFRDR21tUCtxQzdVOU5hKzA5RzlHYVVmbWdreEZoN1YxVW10eCtsQ091V3ZESjBLb0FWQTV0NldrOEU4Y1J3Q3VVQUJDWVZTc2pMZmQxUHNwb09FOUNyRXdxVW5rcTJWT0tEc21nU3dDS29tbWR5bTR6L1QrendSWS9ySmdLNnMzN0tBR2Ewd1dZUkx5bVZTNzVEZTdlYThFZjRMUjUxVDQ0ZXF2V1FwSG5LL1JwZ3M1MWREZlVPUTV1ZmZKUGVIRXZFajhDUDZPVzFnbVltSWtXTzIwamUyQmtqelQyQ0pGeHRaZG1BMmxKYXE5MGRhNDNNZW5oTUtxVXZtYUlSL1Yzc0ZrRHZRU1JoZnkyVnRCVkF5Wk9rRmpxQmg2VllxK0pnRk9USjdIY1RNNFJyelBsVjNFb2NUa2J3Qm9VRnZ2OTR3SnRUNjdlVVFDL2ZPR2M2ZVQ5YnpBbkNKQm85S2QreEJsRjJtOTYxYURad3llaU1CekFhUUo3cE45STM2Tlh2MVZrOHZ1cHFzL3hHL21FOEx0UXlZWGxtejlGeWNiQ1NENzJvbEQzZ3VKUEVTbGV5blNXbVRBU1QzT1N4S0htdXg5UXQ3djlZVlRmOGpFY1I1S29yZUxkejdCQStMMC9iYXF5YnErRUhINUxHV2pHbElrd1gzN21TcjVtVmI1dlFFb1dRanNSNmhGWnF6NWdMVkVHRHFmOGRzMWNMbkN1VlprWWZETzJlSSsybDVOd2pmaG1DSTlBM2huUFVTcTB0QkVqUW5nK1Z0RjNjcW96S3VzaW9raGNZNXZ0WndYLzRRd0JVWDR2VUZ2ZE9sRENmTHdndE1lSUlJQ0RBbUxvb1hrVE1aOG8xUnFaUi9TOHF1WFlGdm9RTWwvSVMvaFVBN1BKZ3pmelQzdjA5dWRZcld5cWQvWmpOaGphSFlIR1AvaWdwOXF0dFlzQXlhb2pUaXd1ZXVIYkNoUXFXWFl5dVd3eHZ5bW9TL2NtbU1qcW5ONGs1Rlo4Y0xuaVdlNmFiMldWbDd6NEtyL2NUdXZIemZZYlNzZklSNnVubnRTeGlwMTB6eHdRbkI0ckNYMzkyS3M4SWhHSHBLaHVKOE9hdEI3YVdPS3c4OHhwOHZ6WndWMEhUbi9VUUxBQWdGL2ZZSEJJcU0yTk1FcDBYUWI3NTdIdm9tejA3aHJDL0x2cGxKN2IyWDVQVk5iclpzUGFEayt6RDNONDIzV1V3SzZkbnlXTDdTOTBXbS80ZUNWTFV4Y0hONE90THRGdFFBYW11WU50cXF2MCsvdnB0WUdodE4vcDR4Vmo4b3JWZUR2dHJoc3dqNGNCWDNlam5CWDV3Qmt0MjBFemhJRXJ1WHJQeGY3VjVvTjkyS2tKVjU2UU95aUZrbWtsU3Myano4bUo3OXQ4RHBhNUw5S3ovcmNPY2wyZXNKc1VUcVlGb0tMSzFzTEFuYmVtMW1xbE1TWTZRS3JscDBWNllqMUtRV3pBaFQxcHNtV0RyNlNuODM5eVI0cE0rNytDaGpScDhFamkxdThCcjJldXdWNUdhRE52bTRpWnZ4ZmNRRGMwRFZ3ZzdLQXBCZWV2clA1MW9HRDhqZWF6V1NSS0hpQkMySjhmR0ZHdmdXYklXZC9EcytqMjJaQTBSVTBrWGthUWJEclo0OEF0UU1vZmNNb3FhQmlKeVFJTk5lWnJjYVFoTFV1QjNzMGdEOFoxTDZINUp0L3pXcEtkMUpFakg4WUtxNXkzakwxSFFKd0JNcno1MmNMTUIySGM4cVlEM3M5RzE0SkgwZExDUGZTM3ZENVIyTTZnK2hSbWhkcWV5S1dUWEZVL05jTk83N1lZUDkwTlZXL2U5cm1ldEEwWCtJcG5HaUJWaXN3NmpkQnRlNzVabFFRcHZZK1NvT3pYQ0xEWVcrZUEwZi9HUXo1WWN1YU9qSkZSdVpISkV1U1FyTDE4Y3BaZWtGUG9yWkwwL1ZBcXdpdU1WVlJsUjkxYnFWcm5KUHFtd01OcVlMRktKRyt2ZlVqNlF0Y2tTWDF4UTk1TVZXMFJzQTQvakxtWldGT2RIZW9QVjQ4SjFDZmYxcjlQVWN2TVRCU2pYT0lYTTdWOGtCVVNGSUc0MGVYLzJpb3c3bThpQjVSd05sSGE4NFJ2Qk1obVFjVUZhZnRhdTg0Vkk3YnltUW10cXRzVWFQVjN4TGJESUZVVGUwRHFyc0xuNkZhclIzMjZuMjZTMWlMcWNQNnJPTWNCYm0xbURpS3JIaEJ0bHFwVHU2aXBoMUxpRzBqQndEL2p3aUxpWUlWcXBnZmF3VW0yUTAyckF6SlZNczRoSzJXM1RvaXdxZ0M1dzFadldhOUZVajdNakZHMVV6N3haSGdyWnZUYk8zVTF5aDlBMzlrQjIwclpSQmRLSWgvRHpacDBxMllETXJ0emRLTW9HdW5HTVVDNGpneXQwS2NUVGtzZUZrYU9RcVc4Q0kxMVBweEM5b3pkN1JTeE5LTjdSeHJPWis2cnJCalhFOHlCdFM1VU5XZ2J6YW10YThrMkEzT2NzZ2h0cG1ReHNjQk1zZTE3ZDc0ZUkwZ01ObzVHN2JCdXZQNWo0Zm9mWThBOHJHODV6bTRleFdaaVhTOGVYY0FHQ0VYRXRpOFhKM3FmTVR3SjFkUWZwNllWNERjSytmL1V1WUxZMm53UjM1RnE2dWpjU1N3ek9LSnhqQnYwM21HQStySi9RcmwrQ1hIZHVUWjFZTHVsNWY4aWRnSnMvWFFsOHlVRjB5VDhhZ3dKdmM4eUoxanF3QS9UYVpRcFMyQ3BSLzJLN0tTNzRjVlhJLzhBd2RLZUtBWkw2YmFvalZLeHByb0lVK01PSmw5NzhGZGNuRDNCdVlyU3ZlQVMrMFJTUm5EcUpRQzJvSmJyMy8xYVB2c0ZySjlqSVdaeXhmOVFuemZiTlA1emZMSmovWEVsR05CeW1md1JJUTlxYVRtZS9xSWw4ZzRiUFJPTGJxQTlERkVYMEFnVXVIZnJxZ2lLajM0WWxVNFd5di9TUmUxUEtZMEY2Ty9tc3MrRE10elhNQ3poMGJCQ09lcVdnbmNDQ2xxN3pHa3NBWElNSEJ2MTRxcmxrcVNwOEtNdlRMYUZXMHVUd2RpdWFpSVlEMXMyUW96YmswRnIyN0xUL2w4QmZhRzB4OEJ3UG5ZcTh1cDJvV2s1RjB3RHBaOXZTNG5zVEhubUxFS014RHEyb3J3M0dIRkZ1UVN2QmtRR1NrTDF4RHJoMFBRb3lnZjNxNjhtRnRMbkFjeFdHQS9iRFBXeE9SbEpBWGhiUFp0a05vRjNhSkNEdVdJbVhrKyt0NERlZncxZUUvanlUdVBMRW04eEJKaTN0SUJFc3hYYmtUdTlyenlzVjM2UkJ6TkMwYTRiRE1SQUlBU2s4T2pic21jTENCUHcrbGRwN1o0T1hSKy8vZ3Q1WG1wdUhGZTV3VUtYNnFDbHNYMUhHcURTYlVTMitOTE5IQllCODVoMTdRZUVxYUF5RFp2MCttS21QNHVQK05CNDRzYytIcUdFS203WHpNa2NmVjF3bE56OWpQNUx4YkphWFEvL2xNcTB1dk5jSGdQdi85VHJsQzYrbm0rVWVrWnQwUlFSM0pCMG95emxGa3NNUGFnTlIvNzRnWWFpK29BRTIvVHErWkJWSW9PN1ZwTDJkZ05yU3YyUkxJNzNpcjRiMVZic3ZlNVhRYUZjTVhTTFNEakhSZjVlak9aRkJ5bTFzYS9kam1OUVhGOEkrUHZZNTJ5VFVkaW4rWmNESnhYa254OXdTMDBOdDAyZUJkUWhjeDhHZ1J6NHEwbVJ6OUZvNnJsSjZSclZxcGZPTU5RcHUrNUNGbXpQQmpUTmJMandmZDVwVjR2WE5Dd29MZkdtSmI0RStaUmYrRFRGam9QOEpCVVJOVEdsS2s2RVBKcXo3YW1qTHVtckErS3dnZGN4YXU0UmhHdWhpYSs3VjI3UEJpMHVmSjRmUjczZDdxWjh1NDAxRExUNlNkM1ZjcGZCRGZFaHc5T2I0dmNKUU0xK1ZmNjcyem1jdGVBSnlxZ2JUVzFlMXM1YmtyZHNNK0JqbHZzblplUzAvcUwvT1YwaDg5QlBHaFlYdnZiSlRlTTlFUjBLUDdCVVQ3ZitXR2lsM3lPZFdLZnJ4VUdFM1prT1pNdStxOW4zMy9QUjJxek5BNjRGS1orZUs2OGpjemxObnFvZ1pCMHRHM1JPUVl0eDVweE5Wb2xYQzZBdXFuL3BWYzdDK2h6ZXJzamlpalZuVkZmVzhBd2ZkY0puamdsbzFuUmliMmljWDNMM0VjbTNiZDAwYUc3eWd4Z0Uyb0ovbjM5OFZQcFcxcXV3SXoxR1Q0SW5QNG8vZ1AxeERNMzNMeWQzQUxMaGw0YlRsakZrSXVRYy84alNuWDA1QjNFM093VWZuUWlLdkh2VU1QOUdscDVGRWFlU2Izbkd0eWxvTC9wNzBqejhPVmsxWUs4U3UyZk0wNzZVdXZxdEJjWGVPUlMxellvRkE3eWk5L0FXVTVPVFl1UzlBZGZNSGVBUHhNOHlpWkhDRzZZc2F4L0RhMEdodnJmNFliTXU5RW9tWGtCQU9odytGM3NLQnNyTzdYU3IwUnhtTGU5VzF2VGtYeklJbU1iYVkvUGRtRG5LcjFucXh3TzBYM3lyR3cwT3Qza0xHd1QyRUtCcHR1QTRlWXc4b29yUHc3TEZEdXJ6cm9TSWY5ODZlYmR1T0owSDBmUGpDTHNaV3lPSGdmbUF1cmY5dXg0WUFrOENLSTJFQlRXTithN2gxcktZcnZZK0hqamJBWEtnelAwQ1R3NW9ZQXVUcVA4eEhaSUhPRFlJY210T2VFTFF1cW1lSklHNlFQdlMxY3RFaVUzZUVGM3ZQSmE4bC8xWmFXVmx4dmwwRUNSai8xbVVLTldSVDdTeTNTNll3QkVOK0hwWnhXN0ptZzRhdi9mNjNJanZMTEJIbmhpUk1RRFVUa0lWSXI4S3ZFWENGWUlLTXhUYWY2L2djSXo0aEk1Vkk5R3BnVDBSTGo5bWV6dlNPcFlycEJuS2JYMGZ3ME1TY0RDMWRubjhITisxQndMN1ZJR3p0djdiUUs4N3VLUDVKaE53RXJIYkIyMS9RV0lOcUpDVlBWRGU4MmJyRllXdi9OTkcvY200NjB4ZUtOQUNYQnhoZG5IS1JPTzJzZmZmWkhFV3d3a0dqSzY3dGlpVWxTbmd2ejBZTWNnR0FOcmhjVExTTEp3OExjRm41OHlzUGRhcXZ1L3NtVlRNNGNnVFFBQmZRS0VDTFBrcWw1T2VyeFZHaWcvSWhQeUhHWlU3ejdreWdKZ0RuQjRRVlFrNGhTbHRtOG9TdUxBbUFlckZlMnJPOE9DV28vTVlIT2U0S09TOEt1RERKRWx4bkhic29rT3BNa3E2Y0V1U2FvNkZsOThyN1cvN1UxNlhlRXRPRW16ekhnRGVBOGlUZWtqMVpNZ0hlUzZMM3hhNTJiUjBFWlUrMzFtWUJZcVFTb0JQTDVsU2lJaTRZd1l5Wk5yZjQ3blFTc3ZqYjc2UU5LTmRSa2R2TGtYTXdzdVBvT2E1RzFHM1pwQy8rT2puUU41ZjlhL00wZWZrdUpjd203TFRXOGhyVXJRZWVlVWMzT1pNOVJsN2FLalR4VHVkUElPa0FLZFNkMUViT3B0V3RrNVR4MXdCRElPMEhoajhPdXltd0JZcVpDVUVKYmR3TEdHVUNjeERoYlNGdzRqNXNrRE4rMm90S1FQeVVoTGY0d0Jpb09VVlpYSkNmTCtUOUZIRVZWKzl3dFR5RHN4enRLSGtJbisxN3JZSU1OUjBSUGVoTEF1SWxvbkV6K0hvSWxaNW93QU9WUjdiYVVrSzd1YnI5UWwrMHFKWFpTcXUzRWRwWG9LeEJuMUQxNTFxN2krOHBTSmpNN01TUnE0Mkh3YzE1V3dJRlp3WStJVHpYRTRMUndRNnpKYUsvVkNPQ25seHJ2dzZrRXRmaE0weENNVVJyVVZ1cGZGWmhKVEZtajA3ZE1GSEdIRTNHOHZmRDlhZWJvNjRiZTZMdGVsd0ZyMVd5bW1JemZCODNYVEkrM3UrSm13Mks0WnFxc2FTeXVOMm1CYmVKUENxT1l0TDRIZGJnUnViWllacEpUdEpoZ2drMENaSlR5QTR5K0x4bG84OVhmRm5jUmhydVBGMU05eEhPVDVBMzVVTDdNR0IxbytaUGNPeEM1N1FGMHpyNnlkemVNbWdHeTJLbHBZclBkRk5DZFpES3lDdmxJOHFWL2ZKZ3VpclIrTEVSWjNRNXpmTEw3MEFjUXY5dSthdzFiL0lHZjVvNnE5NHgzdHZ3T2NqeklocnczUE9qTCtpNHBPMmlrbzNMRXM4UlREeEdaaVBYcnZkeWN3emhzbm9zRkpYRm0zeDdpNkJscTIyN3dvMXZqVFJaTTV4RUpaMnNzWFpMTG1ldFJoSjk3UndVTWQvRlo3Y0dQam9ZTEFsTDZ2cnpGdTRzY1VWdWJQQW1GY0xaV0R6K3hLK2xjTFJBdmord3Q3MlpSU1o4aFNYWVY2bU8xNm5Qd1NTdGFISm1SbHlZSTBLSWVSM2FWWUZtYzV1ajNQbzhiK0VmaC9CZ3pUZjBkRUlNaHIyMlg2ZktUTnJVZ3RKSHE2ajRPYzJ6d2lVMUxiYjJHVDduZEg5K0Z2emlBbEo3eEtVaTdmbmY1encyNThnNmMxZUkxWEtpRlNZdGZqdUNvOWwrN1lmeHlKTEx4dklseWJ4aDdSZndaTHYwTUI2eDUwTHl4bVBVU0tLNld1SXZKM29ieGMvWHhCZG5ZZkhJNEw5VWtoTmRwTG4yU3V4bXduQW43N2pYOWVLd0JuVGc5ZTRxWHFVZis0OHVtL2c0eHNIY2MreGNoZ1VPQnJWN2lzMGw1cm83ZThNaGlnRTEwTzRaYVM0Qk9IRWhkSkRMbjEya1pCVmZkdTc4MXZBRjdSZHMwZUNNR2ZRUFdHQjVsSjVzbFQ0YXA4Ni9kOXpCNVJoYm1LYjkxQWhKN2VTQnh5bFFBWGM3ejhVWW9nU05zRkZOUUZLVytCeitzVUVVWVJuSWRwWU5sT25WbWEzUXkrUEhtWnFJQkRLTS9aalhQNkR5alVnazExbkZXWG5rTS9DclRQcDNHZURIa0oyOXpLK016QkZmMkJoZU83U05RQm5Tclh0d0RWOVlLTitmZ2lEb0NNSjFsenNnUThKb0ZiQ0ErdVJJOERXa3dISFlQNkN3dmVGREZ1R1BVUTQ2OXZPKzNqNHkvb1psZytnNlN2bS80VCtyWUdWdE9KWTkxUkNmSW9zclVOOVNPN3Rhb1VpODZWNy82bmRjWXhnMW5ieTU3ZWpoN1oxWmQ4WnpFa1BwK2VhZFZPQmxXZkVJQWRLZVo2bEs0bGNHRFdBYnBwaFNjZzNTZFpza3Y3Q3UzNVZDVksveXptdkh2YVlXa3dLVTNjd1dCTlB1VEMya3grQkQzVzFsUjN0YnFKSDAzYm9ycFE0ZE4wTWVlNUhBaVFzUk1Kak02ejV0UWtnU2JCYnZsUjRoUGpnMVhyODFTN3liYzMvNUxNU2pJL1d3QjNSd0wwRU5lZ0V6MEJwV0kyS2ZTcFVIZ3lvSXpQaEwyald5ZFRCWUxYS2tMVUhPZkF0Z1AzbHBIUlpBVjdwMjBPbUZrWnJzVXFSMUQ5akZGZWJmRTh1d3llZWQ2V1Z1T1drYmRzODBYdm5DUGlzRUo5eGh5OVJLeFAyWnVLS0ZDTlpXZG1WK0c2eFh3VjRUcVhVNnNXUzhmU21aV2k1TGpUQzRrU2x1QXRMc1MrQVNzUndSYlVSOWV0bEhCbVRwTG9Pa3AvclpyajhsVDlrYXo3cnRnYk9Ua1VYRWhhYmFleXRvWTB6RytIUTJzNnVBYVRQU2QwN1M0U2h1dzdSRDQ1bUcxZEtLYnlRWGRRU0ZuREtTekViclFMa09jZmkwM3BDZ1haYWh3WXpyZm1acEMwR2phNVBJVnNkcUd1ajluVlQraEV0V2VuTWpidHJBQjM5TXVXT0dOa2RWSUZabjNicVB2THRmbHF4MmE4TVYrMmJjcXByUjkxdzFOclR4VEJPdlRSZjNpdmJOcEVnYnozUVJNVzhCUVlwRVBOS3pNQi9Nc2RRZm1rN0NyR2xweXRaOWtaMTFCajlDTzRDT3RrbTJXb0tVSU1LVmd3RkUxRG1vay9LSDg1dEx6L1hEakJ6Y0cwQVdzcG1PWXhYbFhHM0huSW9QWjYrbTZIdHJEUFZhaTNabk13VWordFlqeFJPUjFEVGtBWVFXNmhySDV1ZURMemZ3WkVTWkVhNGR0MXRQdXR3aHMvcFJjZkgvdWszQ0h2azdZZHAwMHJMQlA4OCtGOHlHME9NZnpxbEhEdTdITHVvS1Ywa2VSdU5DaVMxeTBSQmlkeUE1Ri9oajlUNitjYjJ1Q0tYT3lUcVk4M1RFNUVxT3ZNZGFha2g4d2tXcDBiLytEN2hKNGVCRFExSXFObVFCeWl6ZHhFOHNHNEN6eUs0UkJ3V0xUV0toRzRPNWlyS1o0N1VHZVYvMEVIMzVSdkJtZG4wYlpYbC9nRU5HNDFTemZMNW41bFR0VnZhbnNzdHRLc3ptSlRtVjVUZEYvUzA4TEp6Mi9LVXpITzB5T01pTldHd0F5NkJqeTRrUGtkNzRJdm44R0pnTEhqSkloa05uaTJ4cjFJdXFPOFNsQnBwM0xPN0NmUVFRdEJ2cWREM2NCdDV1cFVKMzVNWlQwczc1ekJVNWxuZXZua2FQMXF3VkI2Zm5GK3o2elBPLzk1NXpkRk8vZHRvaXNtVm1iNG9NNlJYa00rNGZXdG9SOUsxQmV5K2VsWGdzRDNGVjlDeXRtYXZpQTZUbmhKeXI4SXVWb1dNamRlc2s5bkg1MGt1OW1RZGlXaEhtRzlGYkFHczZPbngvM1k3bUYvNGNURjVVbysybjNYNFFhNGxBdDQ0YTZRM0J4NHRWTWQvclRkMW9mR1N0OE1xbFoyTFlvaXM2T25mbHVNWFd0RkFFblJ4Tzg3VElhM2ZWZXBxcXhzeGlQUnBYN1BUQm82N011cCtFdkh1bFBIaVFKa3FwN3lieUMveWRQUndEdTZIZGhxaUlsK3RiMndETVM0VERRQ2hTaFJpMUF1WFFSNkNIZ0hnRUlGeDcvVW9uMWNDK1lYajZHemc1Z05CZVZhYkRVZ1BLRzdETUdZKzBtTyt5T0wvTm1EQTFBczhYeFlLU2lFbkxNekt6MTJEOUV3eWVkaGJnemlHcW9zVWdEcGpPaXVBNWJxK2RzTmxhSkYvQVArUlVWUGV0NjdCVzB4eEQwQ3ZyN1lFenJCQlRVTkJkd1hudFQxNmk0dkVZM3BZMW5ZMlcyS1RCYW5RTGFIaVhHU0NsL3dXTWJTQmhucnVoWUhmTTRMQnk1MHo5aU5jQXowN3dOK0FRSTc4SHErWHZJTXBBcHdrY1VPTkkvTjFsT2xsVnFVaWZnNGEwOWdaRHdsd1BzRlN1TlRSV2ZEVklxY0Mzc2FUVk1pQzFEV3ZndzZlT085TXlNMk1ER0YzS0RKQS9sUVNGWTJCYkg4WXhJeUJoTmFyQ1dveGRLejh1Skx2cXRDcmtJUmpQbTFMRzE2ZldobnpjaTZhWUV4cXFsRWF1akx6NEVhUkx4RGhWbVE3M0RiaW4yUFFCWnBDU1dxRCtVSUZCNzdCT1hQd25ZS3Zoa1c0REdhK0MxSmI3Zm12TDE4dGJLbEVVb0xDMVBRK0lXS1dWbEk0cG5EdUszdDVySUVwTjdtQU9xZmNBVkd5NXpIa1NVTjIvQm8rUUJVV28zUVREelpaRGl2ZXBIVEFHQUczYzVBZ2hmT2VHdHB5Zzhiei9EU2xjRDErdStzb0s2aFhqUU8yQU9ZSi8zSFNqOGlUVGdPWE5zQ0pKWUk2ZzhlMkpUdUFWeDF2Q0ducTBjWWxITW1CalRUK3RZMkszYkt0VGVuUzFxcmdnQmxQdlArMkcrY1VqS0dNWFBJYjNmUVdxMFNxdHR4NFdVTnZFdm1DTWtHWkxJOGpTV2c0am4wRWtJeWdlcFZ2NHVLeFhCY0FPMGZyWnlHVE56aWU3bmNlQVRaQkZpV1haTEExODdqemxNL3ZzNWhON2hQNmVBK3ZNamlMa1FXcjA0SlZUUlhxZWdFa0pHOHB2TXVrNWt4MlZPV0VZV0RqckdIZlc4dGV6bExYeFRTUGNka1M1YldGVEFtZlE0RVNkTFVEb3pjd2tCU1dtOURaMGw0MDdORHUxWnc4OFY1eFlLcEozNGhHZTRxQW4zQ2xBa29Xc3hMeFNpVGJuSlgva2QwMVdpTjNiWUtQcUhHS0YvblloakZIZHY4cmMxTkZDbU14VUtVOENJbHprc3g1N053dFVBTEZ4MXBtcFJvMERnd1hCeWVrWURWOGptcDI0L2lBWUZSWXFHQTVIVlhnZUlvMTU5a0kzZldldWltSTQ3aUFTYU5WckRydDljL01FWWJQaFAxSS9lcGwwNnBvekwrckNoOVV2eWJhTnF2R0hVQ0pzK0FDZ09UQVRzQXNXVmd5UUdoT1ljY3RRZUQ3QnNiY3JkM3ZGbG9zcThwRkU3Y2FvdHkrcGRFTFYzcHp2UGUzeTV6cFYyUTBhekV0cEN3MDd1NTlZZFcxaWVPYlBQMG4vVzN4REc2SlNLSFJhL0lLUm5MUjlkZGlSZ0pUalA1eXNyRDFOLy9pYXlmV2Z0N1VQZXdPOGxjMTlWSmVKaWV2QUVEY2VIQzIySjlIV0tuMnMxazFWR2QvbkpjSGpnOEYzS25XaXdHWTRSR3YvZ0ZhcFdsTkhNNmZ1RzBVR0JZRUMrc0RwUmMza0VyRXVhb2JvQTJURUM5dGRpeWVyUkd4R21mUDVYdDdvT0Q4OHFPeHE5WFUxS3l2by9TRkt0MXg2bjhsOTRhc0J3NGJtR3BGLzVFaDBkRGN0bkkxdVpwTFhvSkF5V0gwT3hGN0s4eUJuVGorVERWeEE2MUc1b3cvRWc4VEhzS09ZMTA1VzR4Y29HNEFKa0RmOWxwTWwrdEM3RDVja3E2ckFSRlc0ZzZURnJMYUNjb1JuOWgxVWNIVitET25ITjZhRzNaSEUvWTFpSlo3VTJXYWZrVi9GRUcxRnZHY3RyaG4xWEh5R21xU2Mva0dxQmhtMWRRRWZMdjdnRTRkcy9RSG0waVFuMkkyQzNMSHc2MlZUOVdvamdadDFLT0Y5MGN0TzBUMjZTZi9TUTlnNEdiSVFQbERYd1BVZWduUi9qZmlvSCtoaGRqUzB4dEU5eFRHT3Q1QU5oMHM5QVQvZVF4cjlhdHdPNnVZRDFSUW9TWmN4T2JabzB2SU1iMzJUSG9idXp1U1FrRHBQem51M0hiK0lLOUoxOE00SUpwZkVkSDBLRjdMZnpmRVpkNHpPZldRMDE3d0R0UHhhN2t4ck10SlpTaDVNMXppTmYvTnZSRWgvanBuS1BzdTFIZTlCbnptS2RhNklYanVmSTRTejkzVzJxMCtGN1VObmdpR3c4QXhWaHdGd1Z0OXg3UkpWSWdXQWVFdU1oc3EwZ3hPb3p0WVRhbVZVTkExc0Rnei9uTnFHSko0alN5N1kvMUw2M2NMNUM0VVNUZzU5YlV5YVpBOTVuM0s5WTBpejM3aVBuQ0I2OG16STFBejdhM1czQXp3Y1VBT1Izc0hlanhKT1pwZUxHa0xQTEUzVjFPS0ZKVTJNMmo1ejJqQUNEWGU4aHpFR1p1UDFSdjcwVTkvNVJKanpQU2VEZ1Y4TlQwTitQSGtXdjBaSnZCMkxSYnFUOFM1ZXY0VFJUMzkvblo1L0FFNlc4cUhPTG5wQnlNMDNGdFNVUWNSbzl2UCsxU0ZESXlpKzhWRlRYb2ZRWmoyL1Q5NWRmUnlLME8rR3ozNWNuYWwxU0tHS2RMOWtTOS9iN09TdC8rSmJ1aGU1NDZGa3MxSHZUM3VFRFJNRHpsZmlGd2RQZmV6bDlGeEdOOFdiRzRjMTRUSFc1UGJYK3V6SlRaaDB5bDdTT3JMZ3NjWlA2VWYvR3JNQ09SUDFUV3BKaER6OEY5b0N2di9qbDljTjM3QWxxNDlWbkw4OVlnZk9UYjcvK3ZYNTlnVEFmNXF5TzY3SHJ3Sys5RXE3U3VodzB4OFEydHFDQUpEWWErTjNuRkhDaFZ5Z2FDYlpoWWo3eUhnQ0NvWEpsMWtvNTJTOUZhUFNBQXpWYjUvQnk2SWYzcW0wTUh4ZEV0QW4zWVhzOXk3Q1FLdlZ4WEc4d0doMmFwS3hzeVN1V3R5bVZzUFpzZ0liTWFlNzZiRGQ2UnBuZTE1cy9BN1RkNTR3U0xjZUNvVVJxajQxNVRTRFdmaVpoT3FGMTVZazhCRWk1VklLNmhPMFFKaWMxMjlmYWkwOFBFTmdlb2lzcW9RS0NxbzRNcHNUZ3cxdEV4cnQ1b2ZlT0ZLYXA5N25FWWt6LzMyL0RxRFZvRDlkWk90am5HUFJrckZFWVArbXBlcnhCQkg3N0VrSXU1TGlleVdGeEdRRVBkUUtwNGRSQ1FJRjJzRWVUdDVoUW5JNlBuR1JqcGdBRWlETExTZFVPRWZhQnJQMkgydzZXUGlvWmZQWlpzVkczSDRUTzRTWkh1M2dlQ2trRVFzbXNkdXUxajBnVlVhL0VGNEVrZitTZjZwcS9icUVURjdURzJIV0E3UVhMb09EY1QySGZ6ZHVJVDR4Q2FTUDJhVWtvZnNGOXdwRGRnZHJmWSs5NWtJdzBidVMvNUNMNk5NZzZKdkxUczhFZ0srTjdwaStUUzhOeS9tUHEwQWlaOS90M3U5S1VLNFZINHNrZUk3QngrRjVsU0lPT0hhQlNMck5WTTQrMHAvVUFsYTU5Q2IzRVVaMi8xSHp4M09zTkJoNTZKTFk1VlpnTG10SS93MXZHVDZsWWxvTENaTG5RcDVQQmlBTzVITHlNbHJmOGZJYlRnRFB0MXNnaXRtQ01IQWFxQkJUdkJ5b0Z0dlhoSzB5TklDRXVsbEFsOGJPV0xuRXJvdkozRGtKMjhwaC9IZ3FXTzhjeEtIcmtld0lyOXF6c0Irc0IrUFJKYkZFNXcrOUh6aEZTMXBLNktpbzdWb2JRTnVUVG82REVuQzRYelU3Nmh1NHU4eEJ5bmxaMkloeS9ENjYyNEQzb3JCVGZ0Sk9zblk3Snh3ZTRacExhbmNPYjBmTWxLVHFyQTczOWVxTUM2NjZPakM1RnN5S2NJNk0zRmoyQnhXRmFsOUxMaFBUZ1MzQWZzQUZSRDZkTmtacGhqS0VacmY4NjNxeHhQb2x4U0dvME05Mk9PSEptS3hkc2lqbWVjYkNKUmhuQ0RZcE9kcjBnTCtwTTQ4dFFvTkVLS0xidDVKMXFhYzRtaGtHZXpOWklXM2xWTFFaWWx3KzNNbVZVZEkzcXptRStvMlMzR3FhdmNvK2gyTFVaN0YxZUFtWFF5OG83V2R4dkR2TC8zZUtLU3JqWFR6dzZacHA1Z3BnSU8xL2oweUx4d0ZsbXFYVDhOQUR3RVRtWmtyaG5EK1pGN3FQUFRXSGdKa1BzV1VMYW1YNU14azlEYU45Z3krcmg1RHRvWXRRUmwyWWJ6ODBQRE1YT0VGSm5JMHd4cGhvTEVQdDJqcGV0ejVjdzFYUzBpbDFTYmJPeVlsWmQxekZnY0x5Ti96Ym5hVjlodk9yVmRRYUUyRFAvZTlsQmN1UStOa0RZTWl1MkZ1TmkrRzNGM2VNTDBoNGFnYlFJQWVKZ0FVM1JHM0ZmdmVuVEViLzVXNVM4Ukx6UytFZ3M5WjdhSjBJTm9BaG44WGRuVHBKZkIxWE5jZXFjdTJoNm43OTQreENFUkRYQWV0bG0vdmY4RWN5M0dNcFh6d1pscDAreVBzRkZCNnY4eXBiTFQ5bml0ZThZVm1qWHp2ZlRZaFhPUlVNMHppTGVwdUpUZWFnMkl0OVFnbHU2Zmk1TEVGbVlJb2Y0RkRqaDFUaVRHVnRGZmEvYm03VTNXYmNXUEVXN0xjNkFCalVSaVI2SXc2VWp4UDQxK3ova1lDNTZGNC9ueDJ2bW9tL3ZrS0psVHBUeHFiclExcmlpZGNEa3ZBWW1RK3BrNGJQcmVrS0lEM0xKbThyaUxjV1hORE04VldScnl4VGFXb2FMZFlDS3Mwdnp5SElrTi9IVFFxdHYrV2RyZ2NoQVcwR0VxYVhQSGdmNTRhd256aXhTc0pidThyZDN4eTVHY3BpeTlvN3BhN2dPMVJOL3d4UjZsSUNkOVpBYUxsYnM0REhoVWpzSjNUMmN2bVc4MXExOGJQeWZVOW5wY3Q3cVNaNDhYNlhvOCs4NXM4Y1IwSW9oRjNPSVJxbWpLQ2JZMXA0QmpkWGdMbU9NUE9PWjlwaytwdUJUK2s3ZEpubUlobXdLUGFLcEJHdWcyQk1CRERDTEZVZVhaR3lLM1dnbXpuN2w2M3FQSlNQbjZqaFpzK3lZRno4MXg3a1FYYzYyWU5CUzUzNjR1dGc5UDlWV2pjcG9XN0xtd2pZUnpMZkJudWY1RlVOOXZjZ1RQZFM1Z05COFh5aFhrOEl1dkl5VEx6R1lXZG9zYWJ3MHJQYU9GRlA2ejdPTFVmSlIvWUdhRTREQ0sxdUx3OTBobGhQOEcwS2lrUm44bkJxeXc5QThiUlZmNGtMOTQrVURwMlRaM2QrTDJQWkdwVWYwcm1MeW9rakZ2Q0NkZTVwL1lXS2dlUDFDS2h1MmdPaElyYjQ5eEpVM05YNks0T0lXU1pjeHExUVlLc2RXd2hFeWUvZmFSUENnc1orM1Rmb2J3MlJaRC80WnB4YndDK25wZEREZGJPT0dWSmxSNUxDUmF3ZXlMek01MEFQYXRxL1NhcGl4ckNPbmNkcUtuUGR5a2Z2NUJIUytQVDB6UDI0MTY0NTFLc3FGenE4R1ZySUJBZW9NeldRNlJMM0xxWHY2b2RTK2pMb1djU01xV0VDYlUrcmhKRGNuVkRRci9nWmxOZm85ejlBTEs4K0hwR2E3WjJJYlJEVHV3Rk5EZFBJQU94Ky9OZTBsUHFDMkVLajdoOGNCVm9LOU1WNW9hSzlvRkF3dkZVc01SRWpidzNkQWpYUGpOeURGUUJtbU53cEJMYWNwZ2ZwVjR3UUNIVFpNS1FldXliTm4zT1JQMkorNjl1bU5ieHZlTUMyOVVZSXBkYUI3NUtnRnBleS82Z0hmd2xQVlZuaEk0ajJPek0wWWpBZ3BKbjkvSWF0bjBXU00yZk9QK1J5N2lFSTdKSjNBcms1NUxQbDU1SVIwT1hqYXIwb1VPVCtBYkI5dE0yNVdTakVJaHhIczUzazVWRTN6SUFTRFVlRDR6Rk1zeUJ3STVVZEcxaW9lMUR4QkJPZEU2T3hCZG9BZmFlK2R1UEVOeVJCNk1DejBqTndSWmk1L3M3SjNaN0k3c3ozemRsU0VpMERIbU1oTEsvWlMra2dmNUd5ZEdIcmV6Y2VnZktYWmt4ZlNPT29GcUFUbFJaYVkyUCtyLzc1ZmhDUnJLQU1Ud2FuMGI5RUpuanJPenJ1MTZDVUMvY1Vnc3ZuQWxxWGpieTcreW1VemN3YlZqSUpEenpTa0F1dFMyR0RLcENMd0xvUlRqd2phcnFLbjNPVkZvUW1wWWFvdnMwNTVRTjc3VnQ4WTE2eUlETlZxK1REUk9JN0JNYnZqK1ZDMkM0cjhYRkVBSUZEY0JZeGdSMTE5Y0RlV1hwcm94Vko2VGJvWCtHeU0zdG9nQmd1NXJNQy9QQ3QvRE5CZGxKTG5HTm9iWlBGR0Fha1Fma2JFZUhwTk5hb3paZFhhN0N3NXZQMTRQajJyd3dwd04xS3BoTHlCRm1GL0FtYytDbnA5UnlHR3JENitGMFFBenRMTUUrS2Y3MTBOSEt6QTdXamJJTjZYM2pma1FTYWFQSnhMUi9FZFJMc1k2cTVZamErQUw5TERZd2J0T3VmQnhCcHNHWGYyZUVJVzZrdUVwR0hpTmlPUVNINTdNVGliQjJxS2Jobk5kWURId05EeFlZK3hON0ticHZLb2hxYm94V1BDVlBzdHJnUW5hR0o2azRqNlY5MStQQXVOcDlVQXpMdlo5WUszMmUxTStkZVAvSjNVTVc1bEZlaDY3dWdSYkNtVG9kUmNHc2NBL1dleG9tY29YYnhwdFNlYm0yVXBBUk5wOHdlcXZXU2JLei92SmVyVGVMWTJqUVcxTnRLYmRVRjdURE5hdC9sa1pJa1Ezc1MvNjFBRkhJNENkVzE1d1EyVGhabmR3YVcvUUxXam9OMDZibWpiSUpmUVB3NkZrMXBmY09BVzhmVzFpeFJKSVk5L2ppNTBBTVlJYmhnMithZnZaNXBETllZUXlGdEpXc2FoOUI4Y2Q1dmhjdWZyRjcvNDZSUUJvZHAvVjNYb0NMVU1DdVhLcUkwR2lZN2tkSjM5MzVPdCtUUXZ0TWV0dk1OWE5PMmNTVXlhai8zM2ovbVUvQ2k3YXkzbHJtK1gxS1A3UitKQ2EvcStweXlkQlFZQzhacFkwZUphdVZZM0xZWWdGYWJQb0lJM1lXL1IraXJoTmVjQlAzNHRIWU02UWpST0EzdlVvZGZ5NzZiOHYxUjlJTDFuY2tFa0szb3ZndFNobWFOOVRZZU5ub1dFWW4zdmRQTXY5UmF2ZEdNbE1FUVhQeDB3UDVidm5VV0JuQnJvQnA2YXVQWnNwK0JIZFVVL0RscHJ1OEJYRmV1UHN5aEFqSGJ3ZFo1R3o1VlFRK3J6UG42cm8xeGVDY1FLc1N0bTY0M3VRMGFDN2p6QUU4MG5UaElmc1haMUQyZHlGRmk4T3dyaUhSb1hXT28yeFg1WTB4Wnp4aE5RMDJ5MHBqTk9SZmlGNzVlU3R3OVNrbE5IV2dMVEszK0lxdFJGNFBzQnlzcHdzRE1iR1dJczJ3cGxVV042aUFvV21JSnJTVm0xUDhCQzlHTEVoazVmRG9UcG82R3dlSEpJTWdOMjRzb3pYV1RVT010OWh4UW1xdjFOUlBGRXRoUDdCdDhTR0d5b05IVndNUjdTcjFUVGhnRE1PR3VzMUhzTWRYNVBFbSthZ2V2VUdRaml1dFRaRGlCOVF5N0QvWklvUm1JTC91RUllT3FhZnJmdlhYNi9hVm0xVzltSFltcXNmblVXQ0N6WmpGbkoxTHVVRGN6dHVEelJpeXlBMmZPNXNwYUxmbkNnaExSSDBEeTQvKytJaFYyZHJtc0VxYktzWEM4VFZIZkdGZHdZNnlmbXBwS0l6Snpzb0hycEhFczdGNVF5YzZQaFFrcHlHNEFnbUh1bTFmYzZNMUJaZ2szdkR3Y0tVTzc5a0o2bDloM3ZvTTZqZ3hJanhEUzc1S01JTTd2eGlhczVvV2hBYmdNVEZKS2RseHAyck44S3IyNStxMkJSNU96YkExWFlxKzBmUUFvZ0l0WFF2SDdINDQyWWRBVWJ5NnFyZCttek1BYXllWXBtZGdjZktRYTJXWk5PY2djckVUVFhRdHRBQTdOL254cTRTTk0wMkg5VUllbU5LRHdaUjVZUHJjdkdhT0dNQnpuYklwb3U0MkV6d1BCNHRwNlBQcy9yejF5VUYyVGRoWTVvZXN2dHV0NHNmVXY0d1ZFeGY4RHJ4bXF6UjBTTkU1ZXJ2eUFzYUIyUGhveGxDQWZ1MTlFNXRyZ0JxV1BjTlVhZlVHQ1JaYjF2eUhHUStxbFBtOFIybmRMNEt6bFc0OHFVVHgvYkxWckU0Vm1MazdpSFdVZi9pWk1MN3gxbXR2eVh2TUM3T2NFbk9BZnNBQVRQYzhUL2Q0QnhERy9YWTB5TXJ2dlk5YlduK2x0djFaQlpNNlZlcFROYS9xMHRrak12Um1wWkxCQzIxVTZ3NFFDSlZSWURBQ3pNalpOb1lZYmIreWFONysxZjlnVE1jNXYrTHIxMHVJdEEvZFpmTWJ4amUvOHpsaFc2OElyNVlONzNRbWJLNnErYklrMThhOUh4ZUZ4VWRMMWVtSDBqb1U4SEtxNDhGU080eHpWOGIxY1NFU0FzTHF6YmVKSFg2NVR4ZWRTemdkYzVhckd6cU1zbHBMdXBrQnp0Zm5pTmE2K0lUdzR1LyswUVVIOUxSTnV0K055bDU5aHJLLzBBNTZwdWhxZXdsczNodFJvcHN6RTN5L096eGhmUjhHZWxEOURGUUVJeG5DWkUvWThYRGpCTHJmNGR1ZE9JekJneWVySFpJMHNiOXd3MDJNczljQzZ3UjRzOU90aVFEMlZIUkFUckd4VGM2YW9rOWR5akZpUC9Lc3RHa1JnMFJYZmtoRFRTbTlzaFFUTHRQR1hkWGdzR0FIRU0vZ0x6VXh4M2lWRVBHSUV2dm5ZaUoyNzJzWmN3OVZ0M0VQVUwrK01DMy9NMWIzM3gxcWtMNittOWNuZk44MUxrN2F5ZW54ZnlnUTJaU1dJQ1VXdDZEWWI1ZkczaVdDWHZEQnRNRWkxemM1NkRUUDZRYVRhR01Xd3B2YWR5MUR5c01QSEdjOENGSG42WVlSQXgzbE8rOWFyU2YzdVlKRXlPRzZ5Y2FvN3RYMGxtNEYwKzg3L2hQd2Z4MGpKcjI1MkNOUUNrN1EycDJ1RGQ4aUo2aGJ0bGlPODVVc0g1ekdxSVZCSnp4U20wci83OVBMK2Zvb2Nra2diMmFZZUJvTFA4ZDJGQ0RiKzBGYXBOdFN4TkpCSE9OSmhBNmtabThxOVh3N1FubThHOFlCV1dZaVhBUFFibXdReXp5dUdPWUl3UVRNaFN0dFdRQTRtei90bjZZc2diQkdrOFNnd3h0YTdSWnR6Nng5TDJ5RjhScU1WSlZDWllYY2cxbVhXR2Y0aUpWVW9uMG1ldWpFYzdaOFBYUWVWUW9BWmErMExQZXBuRC9FNm9rQmI5OElGMTNTTThnOW9ua1cwbDF5VjZiVVdia0pWRlBoNDU1UlU3MXlqS3BqZEdGbFpJQkE0UnFiYlJ2bDdrbjBzWmRGUEhRT0ZzSEN0bXRIenJqUkZlQWp3dW9UQnNEeWN4dUFNTm01emM1LzJwemt0NDhMNUFwSERtNUtXTGVhSlNxdENNT3BvR1lFKzhtSWUva2NoZWZsdVUyc2NLQkJNQzAyK1h3LzJXUWVSWUNQcVZVVUI0NDRiYUVySUZGK1BJRDRPcUF6SnA2b0hBWk9aY0R5NCtDZG5NaEdEbGt5czVHdnUyNXltVzQ2aTg1RWEraHNaakxScUkrbTAzMEtvdng1R3ZKbmJ0WW1ZRytFa1Bhb0Z3NnIrMWU1UTBOZGI1M1ZpZldMMGdRR0Z3VlBDUlIxeU1OOUkybjNmaU1ZRlE1TldPUVRJd2FKRTBwLy8zQ1gxeFFoZEF1ZDFEbTl2a0hnUEJUTE9FQ0lWdTNYMnFjSkJaS0xtM0hGN0VTSlVnem9PcHNWMmVsR05TNVJSczNkSXR0RkI4MmRVdjhmVUNkNkZwbXYvMUV3ZmZsR1VYUWJBVktSOW9yVVNaOXh3TWRIejNFQ0ZDQTlRNUpJK0FEY0Vjc0VlVXV4NHQ3RWc4eEZ1OGxIdWNrQ1BMT3dPb3I1emV4ZWJoZEd2Z2cxWHc2eGJxeXNqalNZVEVOYXF5NWNMOHdMd25la3pQL1dKS2lNTjJvQWVWbittSVRxQUthMkM1YTZXZVBxK09ScGR2YWx2S0dKVm1TREZFdnMwRHJrVVpRb1M4MlJFRUhvWlNYb2JzblIvektoMEpZZk9yVERHaTdmeDFVU0lvU3Y3Njk3S2ZKeWRUekpVR3JRckU4RmFkamlhUXZ0eUduQkR1NFNteXpDanJLTGV3bzNKbnlNSTU2NjlkSjVmSUVFbXozSGVOZGVEd1U5czU2NXRvWGZGdzNaNkplbW42cmdacERxVVpVUGRtQjlLRy8yTFZJd2wrZDZqMm9KSVVCQnM2aXNBUVIwdkNHYUlsT05pT2s1ZG9kZ1BOdjR6TlhZcFlUUEQ5UnNOdVR2OXpUTjhXYTQzUE9ZaGFaSkNNbnBkOE1XRWJVMHZQQitUSFFWR1h0NlRQUXgrQXZZZG50ZmxrZmFwSXF1VnlxbnI1RmIxWmRqSVMxYWVZYVN6cXFrS3BJczk1b0NydVJYQTlOK28zZjY1NkRrUGREeUFSSzc4MW9TM1p5WFUwaXFFTU5MTkxmRGtmeG5FRnpkR0EzVGs3alpNL09ZcllnRkRIL1JINGZaSHg0L1RvODM5WFBlMTV3R2pCRjY3bTIzcmdJTTh0T08zTFVsbkgrbk02ZTZHUHZENGt3cFFvUitxa2tBY3FVQ3d6VDBHdmJleDVLcmEwbzB5dnVSUFMwY3N1Yk44NUJFTDR2eS9qdTQwUnlrbGNsdHhIbFdsSWkzUFdVb0VnaWtYU080bGxQTm04OHZBY1BoWVRQUHZNZm9NT1JpbnFDc0c5RlJRdnl0OU1HeFlLcWpwa0xkWlRHRXNEc0V4R3EwR1h0SERta0NNVGFQeGRXMjh6VXprOUxDQzhkandaeWZKT2g1U3dGaUF1aWllWEtMTjBqUG01Vkl5ZnNjN0NRYnl0MWc2dEJyUkhGWGUyVmkwRW1zK0laeGdSOHkxQUFZZDhNOU14UU1HRUhsWFBGY2ZTbjZyaStJeHhMeHJpTzNtOElqMTAxVUNnVmNjNktMVUMvc2NBS21jTm9admh0bGErVys1a3NOaW94bkptOEF3NmM2c2xQb2JuczlUS3Y2alF0MjBTT3M5cG1PVFpSWFFOQktrU1JjaENyb3VLRzIyc0xTaTlxcURVQ0tyQTVBd0o0Z0ljSEk3THp0WjBoN09pUUNJeWI0NDg0QlplZ2VEc3c3UVo0amlKMVF3blpMS2FsditZV01kR3Qxa2xDZzJlN2tQenlMczJPdFZEVnErdExsekxxY3lkVE9ub1N1Z3UxTzJuMlkyYXhCVmxVMDJRbThJRHkydTBab0QzZmh0c3NpUGZJUlJ4aG9aNktOMFA1K0xuME1ha1ZWcFM5c09JcC9VbG01S0diSFpqeEhaRmV1T3ZiV09sNmNDcUVyWEZFR0pXZlV2WEdvaTdEWDJ2U01MMzNDSithLzRGUnllL1VwcUFKQTdPOXVlTFVjL3Nwd2grQnZpcDRMeUVtdGJNU1VxZmlIL1lpa09LelF2MXVYQitJdnZoNk1Uc0hVbndTWWlMWFN5SGgwTFFUYkRPNEVobythZXNKNS9sNjNOYUNRNzBVV2daY0toU2RzRnBXYzIxU0k1QXdmY2FjOXczbmJBcVIxbnBrM1pBN0NZdjdhV3ZZdFNMZGROSmVLcFNBS281ai9YT1NkTWFkMXlES3ZkazFKekM0M1hsWDVreW1TZEZwSnVjaDk3dG8rODdneTJ6dVNlTTdMc3Z5SC9FeSt0RUx3NEpqeitwaytGOTU0Z0Z6OWJtY3p1Sk5JbWhjRDdNZCttSUVMeklDS0lYWXp6bUcxa1dYVnZ2SUMxNzFBcUVQYWFvd0UwUGYzcUVUZGlKNlpjMUVGYjNBd1pRdjhKdXpQZE1kc1Jzb3JRMjlhOFB4aGFKTlV3L2M0SDdBSWxqVFNjdS9HRTJqUGVIaURJYnJUc05lVnhudUQxU2dBUmlCM1F2RUFERGJYTHZ2YUJBM1Q0UkthUHJwdHpWd3c2SWJ2TDF2TlR4Yy8zMW83R1M3VS9FNnE3blJvbU9pYldrMlhQUitlWC9zeHd2MDZpWUFiT3JxZWxHQi9Vby9yTGxUUXR5SHFwY09pT2VpamhXSDJWSnQvaFpTdEp6UEdub1Qrc0ZGbTBzbGVzNEVjSjVHSUdXeFBuL0g5Zk1scEkvendwUCtZQjNiZXFrMmplWktFbWIxY2FhOXl1WlA4S3A1bTA5REtNeGY0bnJ5ekxSVVcrMjFQUU5vd2lGV2M3b21JbXF5Q3gwdTRmK2E4S0hnWXNkQ2xFTzk4eVhDVTBNSGdjaHk2Rys0Nkp5M3R3VG5XVE45bDUwMXhuWC9YTUhRaS9Oc1htdEs5S1U4QzVOL25FblM2OGtRQTFNZncwenM2NUtSSkRsaEdKb0Q1RzdhL0E2dE10Rm5PbDdQRXJwSWxvVmFUSEdkYk1uYjVmVmY4Y1VCSTZibmxJRGdNZW5BVFZ6OHltMU9JUHRFOFFSS1VibG93M2d5RkhYTGJQeE95TjhHa2NjelZJZVpINXU4TjA4RitETkNxRC9iYlREb1JXNTRhOFE3WDNwbmkzU2k0R1RKK1pMYWZxRGFxS0d4Q25YeXJUTEdYOHdsV2pNNkVzNVhJT3p2bUt3TFNoellFenZaNUovcW9VSHB3bVZFQitLN1JKNFFyZ05CYWUwOWc3VDdydnppYmhYK3ZnUEIzSE13ZmhYcStRbkw2cnBnRnFHTHUrQ0xRc1ZvekUwYnNEWWd4SDZpVXYxZUoxYXFTRTRsMXcvMnpsQTB2Z3U5VEx0S2IyaE9EcHVQemg4a1Y2TUdDbkVzQk5pS1ZsekVYeTAyVXZQY1VIOGU0cjVuQ2JvWHNNNUkxeStwSmloZjM2UmVMbmZBM3NWUkY3T2c1eUhTcllJT1JoWmUyS0k1NFdjc2IvTXQ1UGM5Y0w5aUtjMm9yTWpNaGcyVHRMWDVud0djQzl1TUxUdWVabCswTnFORjBBZ2FjZ0huWmtKTm9saHpUZlVrcGNoY2IvcmVDSW5IekdKQ2tuNFQyTDZqd3R2Z2VCY3FQakFnRlNLWThqTDFnOFN2blBFUk8yU1FKVmc3aStIYlFINysrSUlpbzZ4SVlJNUp1d3hiRkpZTDVFdGpQT1ExTVNyS2U4bURvaVR0TGFaOGVMdk9yV29NRnZkR3Q4ai92TU9HRXVsaXM0VkhKTHpjMlBZWWo3QWt6dktsNlczeExQYmNvMXo4TE00eTYwVUJTWkFGOHpPeVBCdVBWNlZKZHFrdlhUa1JDbWt6anE2bFM5Uk9UaDEyRkJBYkRvaXFDNTNmOUdYRHdseHR4WTFXdVdsZnFheUNUZjBSYzMxNXUwTTNSMHV3cmlDWWZVSEVnNUY5NHNWQkRHUE16bU1GekxOUWZnNnUzd0FBT3MzS0lUSXZEMjdNYXdLV2hCTlN0YzFUblRoVDl6N29vZkxmemdkM3lGMnlkeUVHemN1bkVGdHF6L3V0aHA1ZmpFc0lnN1pTZWd1bWN2eTQ5QUlvNDhVQ05RWlh4RUZYc2JvY3NBV3k2TnZpQS9pL0poN1Y5OHhndU1wMGF0cThtYVl4ODc1NjE5OU9XODljZHo0WGQzakNVZTdtR1hvU3JhMkZSRFdOMVY3SHlJMVVleXFBWHFaOHhFcCt1clplWlZQVDBtRU5BWVQrcmM0MUhIV2gyTTh5QWFCWVY1cWJ3UkFEck5ncnNqMjVzU1JWL095MmRUVDN0ODZkVTN5eWE5ZmRkMHJRUC9BK09hYVFuVDlYK2dQQmJPVW9wTjJRMHN1U0t5b0ZqVzZxWXAwNEppZGNmRWs0cUtuU3laYnVDbisxZXdKblRVc0VCMTFVQzZ1SDF4VC9sdmpSd2t1WEw0Mzl0emlVeW5lNGJSWFQwVk1BVzBoemhqblB6OEJ0djkycnJvNWtPL1Q1M3IwbGxZMXFEbDhIbWVvZjhNbGptZ0FuRkZFU3dicjZ4SFo2WVBlQTIzT09UWGRVTGVkM3U5NHRpTkhRTjhQWlFaeG5mY2RtZ0NRS1dsVHgyNFIrODR5VkFZZEhLSm5Kcjl5U1JTd1lsdG81YzMrZ3R2MVBLcThRdC9oc0hyOTlSRzB4V3h2MkxMNFRCWHhUcWJEUnF2bHJqekhTTnJmamdDdnFmc1hlOG5HUkpDcXgvV09mTGVVdTdVMUFpZ0g3d2N4RGN0Ykkwc0lldU53alRWaWFvUXNlQWtjTEJmSENQNjA5MDF6L0FWdjZmUnZkYnBUZjQyNzcrYXlaQk9WVlVxWGZGWDF2WnRRWFFIa0NIL01tenlaWlVHZ0FFd240UWFRWGZaa29NV3hQQWw4Y3BDR1FyV1FONmdySzBTcXRab2VlZzR1YUg5ZDh1QS9ENjdXOTFlUEdVM0l1UmtXUFBFSDRHdm5jUGRrTFQ3a2tlOXJJaE1yUEJaUnAyUE9iVnUrN2JpbUZ2cGkzckNTUzY4Q0FtMktXMWhmRUdSd0ljK2NvRmhhTkVrc1B0WHZNbWNxMTgwU2hqcUw1bHNaanZzOEh1VHU3bmxPUUZacVpmWUcxSy9QNm9nc0pqZjlreUNUNE56dmNsRHdFaHk2ZFNWSjhOcmFMdVlLcm15NGNWWGxmSm9lMVhTWCtCRnFSM08yWmp4c2lhemcxNUZkeFlmYWhwSmRVN3gyVnArcGYwT25zMlhOaWN2TjFqMS9jZjE4dytMcHA4MFZsUzJuWDlXM3MrT0Jxa212MWNpZG5ydUMzQmVyVlBVeHUrTTVoWWxKTUQxb3duclFscW8rODVkTGpMLzlEVjNRYnhTMHFoeXI2a3NCS29POUJmWXJueS80Q2cvNHg3ZEFMb21DOHZYcnIwekpWOVp0ZW1KcmJJSDVtRE5VR3lCYnR2SkQ1NVAvT0R0M09tN2FEZkg4Uk1UNGZINnJZQ2pxK2VYYmtqcENBY3R1dU4xSGFZUG01RzVyTHlRLy91VUdRVVorbDhqbjlLRGU2eDJ0SkZvMTJkTjRQTkZZc1JyWHZiQzlrdU5CaWdJWlR3VElHWUYxckl4RTNjQlZIbUpqTGtvc0lmWmhJZWFocTdZK1NWOU0zZkhiK3BTNTNnWjNzUkNDaDZTZHdKQzZTSzJsaUYrQU91c2wyOG9jZmRIMnBSbzlzMW5vSzJHMFBYaHJUcStDZTRyNFRyVmxzY3RZQndBRVFlUmVWclU4NmtQTXg0bkhOTzJXRm1UTHVoQnVkazlmbTVkZlBRVVd4TElyYlZ2akZwa3RQMEsyZjRXbEdSQ3ViWkZzeDZMeTZsbGdPajBNQkF3clVyd3hWOUswdHVsSDhEdWxNNUErcGcyQ3FtZWg1MWhaSHpFcGY0YXBPby8vUDYyU3htdElTVFRkUk1LbmZOQ1cwZ3ZJME9QYlpDL0M0Y2V2YlVMZFdYOUF5dHgvKzRrVmNiT2NsbVBPZU5iUGtveWlUcklvaTJwc3ZKVm1aZ1ZnT1ZwRVI4ZkFRM01QMFNkY3pMb2x0bVM5bVVTZjNtMXpCUitHU0dDZXhZREtabSttcms4L0VEL2lSb2NHbGgyeFYvZEVGa3pweGVSL1JQYW02UE8rU0pydDA0TGxML3JaNDJGS1c3S2Nkc2srWWtCcDEvbVo2ZUczSmVIek8velQwM3RRM2dPT3hWMkJsYXBDaXdVekpKTW9WbXRqOFU2NTd1TCtvZE9NRUNKV1IvN1lwaDQ3a1h6bllmQ21XY2dBL1RhUm9wMDBnNnVDRng3N1RiTUNzTHdadmlDUEdoVkZVOEVnT09CNUxmRy80V1FQaEJmV2JnSnMzM29GS0R2YTFGR3pWdXlrLzBSMmNmdDRHWWhMcjlTZTJ2M1J5KzM3d25nbmdjMncvbTlpTTVudzdQcnFVNmpLRFo4cFJ3azJHbjBYbmowOUtZUWVidTBGSkdveDFtOWdzU2dpNzIrTkZLRWhSRkNDRWE1MlFjMWdFNVBVYUJNNGZ6b3RYcnhUL3JuUTBqRnp2OUZia3hUU2JYOXZYNDdrMkFmcGpKZEN4VVRXLy9QYVl6TlZyM0tnWFd4RVlzN1ZGdFJneEVUZzd3aWdDNzJtRkN5cDFLLzNMTDZjVlhKdUV3MTJvZVBvWUhxaHZvdVVQSlVWTldCM1F4alRabGZrR1dtRzVEaG10VkJ5d3E4T2JvWE1UTEMxaHBqWHFTTzJSdVdvREMwbzJEa25iU2ZKeE55b2dFUnVRRzZGR0kxZGZoYnY0UUxiVGZOODFiNDE2VXFOdVRpbUZzRHh0R2h5RGhzSXdIS3g2MGgxS0I2V05GbWp1NFNaZ1grRlZZVHJlSS9IUkVNYWFJVTZTekRoT3pyVjFyZ2FrQ0tMMWFSNzd4VHZac2tLRFAyQzN4MHozdnhTTUxSN1htOE1rUVNhVFRCUXE1QVNzQTdUdzZsMHJsSFNVbHFGZG9aK2VDbk5EYmFIYVQrR2ZoemtKQjFyQlBDaEppSWZjWE1RUklhdmxCVmxqSG1lcFlsZ3JualBxMzVhK05MZHFGdmVzdTBycTduMVlpWmpDWXE2OEQzTGdDOG4wNEo3WmFleFI2c1ZENDlZbUdRMERESjRVcXJRQzRQUGM5Nmt1N3pqK2RxRWtDNmQvbnZwYUxCeFhKM0k5YkJFWkdEQ1MvbW9sRWc5WXQwdHZPMEg0MWFuRFhrRmZpcWlaTFpPTGp6RXV1U3dCZHdIZURIVTN1MlUwOVgxSzh6d2NhL2FDTmgvUTZ3YWtCTHhRSU1oMFQwbnArVnZpYUYvNGFTN0ZrcG1QWWJzREE1TjNOWXd3bjAvekwwWG40V0Rndlh3RmlUbzZFdWlhSlo0eHQrNTBmaC9uUlI1M2p5bVdEY1BTR2pPa2ZwQUdVbHBFRzdOaW1tR2RTaU1qZjBjUWFpOUdNV2lBRXM3ZlFrcUt5K0srdFNMSTRjQlk4THpCU212N2hROHNZczY0UFZhV0lHNlhXdlNWek5yaWEwNkI1ZTVvekI4cUNpZk9pWk5FTmtnS3k0aWJOQ0lCbXRud1lsNDVwQS9SbXNhUHpSdUZCUWtrR1JHcVJCam9kN3Jra0M2NDNlV2gycFhkU21wemlnZ3VpZG1TMnprckZGVWJLaWt3Q21RT3VCQWxtOXk0SG5zTFBaL3BFQXNuMy9PSGtRQzhoSklJL05oaUo1bmYxLzhOZ2loaHo1anNqbEswTXBOR0h6Z1oxNC82QjFiYjBsbWJJdGdOUDhaK1FMWjNVZUVnMDZYRDBTWnVYTnVIWklPKytzTlphb25KSnpEUzFTS1lzeVVhemFPTlZLRlhKaHEyZHBLUXRaZmZVdHh4M1U2cWhvRGFWTm94MSsza3NhbVY3OG9UTW9UZyt1OTdGamQzdnRTenlOMS9DMGErMWR1K2JZSmtZRVpVZDd6TklldWNGb3l5KzcwWSt6cCtFYTM2QVJjWE9EeXpvSXBrTldaZ1FkSjdXNU9JOUljMG5xUlcrSzRVWFVESmhxZlFJS1hoaHI2N3ozTjhHVk9KeDJMUm5sN3AzblRXdHhXOGphbStKWW5UQXQxWlh6Kyt1ZG1ZYWIzTkFZQlZPaUJ3YStlTGxGL0lON2R0NFdncnBqbWZzemxoaFZLTjR6b2grS29oU3BDVWdSU25yTkt0WUQ5T2pGeTVpY1o4REt1WHZ0b2hVaUxlbUNLVjNJaStQZXdaMGJ0bUo1VWluNW9jWnZyRG9OdVY3dWllMlpZQzFROEtTQ2lFcGk5bGZqa29NVDRvMTQyM3BCQUZvbS82Y1JVT1h5TjRZbkNVR09YRjJsRk95U2ZkRlBiQWd0VTJEbUlFbUladG9IdWFWckswUyt1Z1N6OHFJTGdBTTIrWlRrRkN6SnRzMzNMNWljYmRjbWtsK2xVcDY1dVkzZ2NwMjlCbklpOHUxN0thdFg5aEhXZWRtSGpPd1RnMUpKTHRPZWh0L2dFWUcrZ1lUcXpJZjhrWVR2MkQ3NGgyWUdxRXROalM1QW5DcmorZG4xcWNSYTJQclI0bm9HekJHblFOVG05M3N1UDdNYlpzL0dyU0Z6T2VMbWNUSWpzVU9OeXNXNEluQ0t0bUJLaHd1Nzd2YnVFWmRQUldkK0RVSHJmK01OUlZQTEJXb0ZIdTIvRzg0WHZ3MGg3by8vSkpwTm53Z1R3WEVMVmNqT0p4TkF4Q3orc0xJeWxVNmhJb1g4SFVNMExiQ3dYNTRPSzZraUwwL3RRZDhVNFNIOCtzNWVSaUpSeDZnYmVCU3dzU2sxRDZKOWFyQjNqYWFyZWNPRTF6QkJ1U3FKcjR4ZjM3SVNtM3drL01CRE15cFFGQW4ycFozYWc1dHczRVo1UG1mUHlSSEFRcHJNclYvNW5hMmp6ZG9tMUM3YnpkZ1J3Nno5M1cweGN0RHhsY2Z1ZytPTVhMdG1kT3NneVMyRGIzeDFxSWM5VW1rNk5ndktIbklpTXpBSnFrYzl1TlBUbDQyMW1PMTZzVHZyNENVMVBGMkNYVW1kbjdWL3Job3MzcHRhZEZUY1R6MUZBbkQ5N0ZUYTVBc1l0TjNlS3NoM05rWjJFdkhGcll0UzNRUW80aTVBRHAzSEs5ZTJIN1dsZ1RKVThqSm9nZ2ZmL0tNUndxdFhPQnFVVGNBbnkvN2ZUbUtqVmJCczZBZWREejFCZW5QVlZwVzQrRmJjOGQvZkNQcFBMODhjK2QrSGE1bGhDcmREMGw4Z2FpRXB4MXc3bzVLSmdjR3dvNlVSQ0tydysyVW90UUR4dW5CS09qVHFKVWx3d3JtREluNjJkbUl5RUZJYXZldUtRTWdUYXZoVW9ubUl3TUtIK3VONE5sUi90cURCVGtnMERWeW5TNzhFVmlXY2xwRGRCYXNHcHNsTGR5R0Z6eEZrdkNWVXBMaVA2QlpocFlMeVhBR21ueW83MGNrR1dzQjA2RUlQZUNhL1pYUk5ycGFHQ285aGJ5VUZhb0NqNzNna3dQeDBJV2wwWTErS1I2cTAxMzNPd0hpWXdBdHFrbmR2MXJkT0gzK21QZGxKYXdXRG5scFBMcm81bjhwd0Mza09wcHhNVnhGczFYakx3cGVIa2lhTnlXbjFnbVY5Q1pJVnNZUjBKbUtYOXBlVWFaa0pWSGdmMEJVVWgwNGtVc2wxN0lSVVR2ekUwemIyMmpoblQrcmRETWsyeUY1R1RycUt1VlR3dWJrMXhXazBFOG4yNjFLMkJscjZnUjQrQ28rNjJzUm1LWmw3cDJRUGVZREZOdEZ1NnZVMnBYTDR5alVZTGVDeUovcHM2RTdqM2xld1ZwTVdRbSt2S0JmSndoRXR1UjhQQStUbTJKNFVOWCtPeWlyam5rZ09EMnV3WkVFTFIyZmFlV0R2RTFYenczMkRualVScUMvMHk4NGlMVUt6QlU1MFhRaUpHVzN4em5zSkRPbjBmazFqSUdsVkI1SVg5NmF2WXdxY1FhMlk4TDFiU09SK1cxOUhaS0J4SVc4bFRvSUdDejNuekR1WjVhdlc2TU1MV0dPSWgzWUcrZm9CYk5WYTcvWGY5YXA3bDB6RXNRL3JJVmxvSEFpWDk3YnFTSXY2RjZUM1pPT01nc2w2VG5lM3REWlcrNWtBNjhjWitGK0tXOVE0RkNyMVpIUUhyZVRhc21Fc25Lb0IvTGp5cEU5S1lENEZrVjZQY1JVeU1FOS9CSCtZcWo3djB1OU0zZi9ZNzV0dkYvTUhkQ3NHTjI5NUE0NE05Zjh6MmEwY2NsbEQ5SEh5Y0syamkyWGlBc0F0aEVVSVhGek9LSDV3OUFKOXhVa2xTTUhCZTNnRzl6WEloV0FSQk1zZ0xLR3R3ZWE4OFNVVUtrWFd6Q29SeVZUVkpMVm5UUTBxRU9VZWZ6OUtnVUNhRDVWYUJqamxJd1JobzJyTE5UTUZGMElZQ0x2K3N1Q0ZRUmlwYmJjYkMza2hCVC9scE0vTXZGcEFrM3R5emV3eG9lNzJYTStQMnBzT01ZSXFia2FTNDNIQVBNNVM5eHNZMVV4K2l3YzFHQ1ozWXpzeVJ0dmR3V1BHNUhSOUc3MFZEaXBnKzFBdDMrM3Y5dmtYQ0Nqb2FSRy81SWxkenQ3a0w3V3BnRWJxZGMwaXdjUWtNYmxjZnQ3VnJVMXF4Z2pDeEtmcVhFR1F1SUxibGVQNGl5QjNBZ2FNSmVCTEFMRVB4NHlteXliNk9rQlo4d0lNZHhIcTNOd1RoWlNYNVVONFBlSkZYK00ydVdrOXliZHhiWGVXdkN2RFpxWUVZYkZrUnBjT2wrTmZ3bHlOTURTdDdzWFJDTUJ0bzhoV2kwTmNURSt3SUtTaW5sTDJBekZwS2tuOTk1cDN5clhFVnFFeFRFWXRhdFdLV252ZmtqcWN0UkExNUQvdzNXM2lCaXdhU0E4U3MvQ2lQQkZVYWp5Y1dmWkVSR1cxQkx6VVQ1UkpOZVdpM2picjJLSlR0MW41WFFxaXRqVkVoTjlER0cyYzc4OHcxSktPYkRoQis5Vzc5Vk9TaXh2NTZmR2RWLzlKbm9ORlh0dEtjV1UremJVS2hGaFYyUmk4TDZKZEVoYlluKzBGSFVDU2pXa0k3bXM0TXkrU2szNUxSdVk5Vi9yN0hEdDJUOFd3c2l6TWxCRVAwZVJCMktvMGhlWU81RzN1WkYyZTh1cFg1VHRuNTJPU3ovbDNWcVBFbEpvVFpOeUVpbzFTVWFUbGhrMGUycllqK3hmbWxtNU9nYTRNbW92WHlzMCtHMzBseHlZWFE3dVpmZnN2RVNGNSt1dzRhWjR2QVl1dUNQNmVBbm80VGFjd0VheUdFODZqMzlmc2p1TDNmaDFJazdoTmpPZC9ON2MzeXVCWW9meE43RjYrVS9YbWp1TGcyRklBNUFSemUwTlBSb3o3NUI3YlBjRWFpcmVPKzl4Y1NhbmwyZ1M5UVZwUFdPeTZnVkRiODlvTGFYUWVhUGt1UHpMcjRoSGZpQkNRRjRlUmlNakxHdCtDdEFNU2RwQ2NOTGpBK2I1QStXOTNYUlA1M2Iwck85VTQxbWRIWnRleER1ZjZmbkc4T0ZHRFR1ZVJlMWZac3pVd2NjeGNqWXQrckNMRjFoNkdBRmxmUzM5ZE9iN2xwVmxubXZlNDVlZjNHL1E0dEg5K2dKRm1aK3o1S2l1SWU4TElIR2VGWTduOEd4S2VmZXMvQmpoSFd1SFJrMlIrcy9EWXBxU1pidU5iK0F4TWE4dFB2MExHdXdDTUhDMEpRQk4waEx5UlVJQiszQVlXQXVpWXNrKy9uT0ZHMnFpWHc2YzNIcmZpVVB3Z1A1UTFvSjNoNUhKWnM1TEgzd2dCOEtwQkVtcDI4LzgvM0hEWnBCc3JRUXgxL25yblVOSVJTMWFvUVpaMHM4aTh0Rno5RGdGOXZ3VGtuaHkxOHFpMzRyUXFObTNkcTJIOXlFdWZoZ016TDNYWUhlb3Q0TkZLaWw3d2NadFJyUHcxMUtBa2N6bzJBWkpOVFprc2xLeTRHd1AzeENmVStrVHg3cjlQeWE5MG45Q01OcTg1Qkp5SVlZaEhIZ0taak8zSzQyTUJsV0EyR3BnUEsvRDJJSnlxRW9ZR2JYMlkxOTE1Q0RyUTMzRGZZWDY4cGVmczJiYzIyZ1JVbDNsNXMvV1dvTUttWWtQb0Zsa1pNYVhZbEEwM0pBeGE3bTdHVkJSdkh1YlptTXdVR3FzcElGMGpyU1B1czNQMHFkbmU4QTdOTVhuRm9HeURSclN4c2ZQVnRKR1VnYk9kL2tFZndhUEJldmdFU1A5eENiUEpINDRuNlA5TGhXdjFYL0xoUVAyRDIydUpsaHRrd3lpNEpqaFovVmNITW5CK1h1TFhidmpLaU1VZDJ1akw4ZzlVaDdieHQvOUpodE5CWW9Kci9mTW4xUEc0TFRtWEs1Rzg5RWNNODA5a2QvV2dSOXQzSkE0Y21haTBIV3d4d3ExbE54bWthODFoK2VVdElOSjBKOEwzMkVkWE56dm1jeHNXVjFaN3FpSEhQZVFBcWxTSWtXUGJhWkE1TDRlS0ROSHFrdUlwOE1FUjMxN3B6QWxCc1dMVFN5VmdjQVhJTXQ4U09SRzZLRmtSS1FQL3UrRzBaeTd1cGNDR2hRWE1aUkM2TlNzZVltNFBZK3pCUWtYZEdhb2lWcWF5SGZIUFhJcGFNMzN0TjU5Rk52c1dYNkRvNXhTK1JtWnBRc054VmdGOGdvYzJ5L1diMy9Ybm5iNnBYZEo2NjI1OC9jbit4em4xeUhYNkZOUEVsN29TTHJsL3Bod0h0dEJIM0hNOG1GV3kyL1lYOGVQS25rNmhUSjcyM2x6Q0VkbjdQRVh6aWZ4OUNLQnhUMGpDV1lmUE1hblY5d2tFNWc2VklVUTFRNUhXQ3F2SlFtRDZpMWNwYVFvTldsSnE5cTZyWEdCMEdsWEVWcUgxSFQ4OEFhUjVuclVPOXhPMGFsMkhRQkdLcDdmYXVxQmJaY1l1bUVBTmQ4ZG5iR2ZxM2wvQUVlODcyVDRSRkJjcXg0dWxZMzBKK2JkZmJuaEFKMEgyVjRtQkZ0TWVKWkR4b05KS1dKQWp3OVhvNldUOUVQUGJ2MndRRHZFQmlRSVNMOG1lYlprZ3RjRmRraGRDTGd3WW96bG5zemR6WmZvU29kQ21NaGV0ZFQ1Y0xra1MySkcraGZPZGQ4enVySG0vYW5RNWIxbjFNNURJeUM1Sk9aRUVhOTk5N2x1SmVlazI4YWJ2cG0rNzAvMFNsd0ZERkcxSlphdEp6dkp4NkNoY2Q2UVFVaXl0M29yb3UwMldIcXp0eXBESWNMaHhFNllxSkpCcUlIQmxkNWlaL2hTTThpM3A5Y2RTODFpc1cxSS94RFkvc0s5M2ZGS2F2TmdhaDE2VHJ5WTlrWmFjL2xiOTgzSVJ0UVlwQWRBRGNmSzZxdEpHTDhFZzVRMjI4cWRRTDVKTlBuZ2pmS1J5WUs5cUpZS1lYMUgvdlk2RU1aY3V6NWtreFY1bkw4SERmSThCL09LNmdWaVJLSEFnWmhkZlZYR3M2a3pGRVZNMnZWWUExeHo1aTNnZFpDci9MVmR6SXlXcXpBcFdjaFcxS1RVeCtlaHVsZEFnVEJSOVl5VjBZVStFNFJzSW1EcEJPNDZRa1JJQlNlRzg3RGNVYzdWbjZJWlM0L29kRGRUNS9DVEhmZDdIODJuOTVwMjAyVjdHVHByRUJsK3RYekRnKzRSMXhsZkdOQVBvbU4xUzNGMFJaT0ZDTEtPRTNUeGdPZWR3cTQvNnFZK3JidFgwZDNUcnhKNTd0WVFIVHg3NFdRYUtoQ1FsQ1AveEVmSGhKMFd3aGJiTVdBS0o2OXR4UTZ2SjNWcXJEb3NKVVVVQy9ENUxTOXpHcW55VGtBejREcjd0ZVd3SDhwRHAxdEw1TkM4VFlYZzJFdnpRM3hpTmtIVHlWMDJPM2RqMjBvcVRnTVZSWDRwUDFOSjQ1bm1LMTRUTUZtZ0gwT2RWZjFVQSswc2VVWWdyVWhZSmlsRys1YXZjWGNuWmZOWFlmcVhDM1Q5WktWSm5YaVlUbXp1SlFoT25ZV2V0cmtGT01hckN3MXRCbDJGMUs2ODNpRTlBQW45bDNVM0ZnUHdZTnl3Z1pkN0I1RGJvMS8yNFFTN0NIcDhlYkQ0dGx3Sm9FNkxJbERQRWpRNk9QUk80UzZhVnVDZXlaMnVzTi9HRmxTUUlsc2dQZWhMT1prY0hYMER1NzFvOWlzZVBiZGpUcXN6eHI4dGRyVmhrcWtOWEpHVXpyOUZJaHVzaUdPZUxjdjRWc0hxZjc2WFI2dmV5S2hsVGpqczlHeXBXbjF5bDRCRUk3bk5OYUt2QVVzU3BwMzdxMklwaGRldlFlenNmUTFlNkFibmtBU3k2Z1hCQ2ZydmhRNWI4b2FIcGJhNUZTYnRYM3JSV0U0Uit6dXVyVlgybzZjUXdFNGZ6eEJMcnNhRUNpZS9QSE1vb2VrOWNING9iT2hJMDBMMXcycDlkU0dwS1pyaVZmdXROQklJSzByckQ0T3ZFSWFuRUoxc3JLQThzeXM3dXM0ZGwwMG4ra3pUOHh3dmVaN0xtQmdvMDJMOUlsSWptWHNPL2JEa1BVVmt0L2ExSzVSaDI3S0VlbEkwNFhON3lYb2FWMHhteXR5RHdmZTlJelNjSjZFNTVHZVhRLzZUQVdORFg3dFg1cGNIOGhHZGppVjV4ZGJTY0RHRTVtZVpOeEIwb0ZLYUlEbGhlMmxpMWVOUEQrMGlHcm4xUXY3KzZReVBIc0FNYURuNUh3Yk1SMkpPbmdvMTZPaTB5c1FjcjE4WDh6OW5FdzMxODBENnFQU0ZUTEg0Z1RwbUFZQW4ySFBnMHN1a1did21CVndJeFZESDhwQnV3aUZwemxEajRVRFpoc1U3bTY1VVF1S0QvTzdseWxBZVZ3Y1ZSQUdpRnR6anhVY0RFLzNjOXVXa2o3YjBJWXorSnl3NHowaFVsdmMrMWhKT09xTm5iYjlOUjJFdnpGOGpCTDQyMjNHTU5YcURjRC9oNytZeGNiOEhFSmo3MkVzL3hidThWSTBHbU9mWkFBRXZRTEdudDY1UnZneXlzTy9vVkJoeXV5bGdWWVgvQlFyVHZLMVJvRk5KdFMvVTFabHVFdmwwSnB1RDZjV1dGeE0rTWZqcU9kNERCUnVmUk8rTTJsSFUyditOMDJWMzdtNTFoT3NsdmsydWJjcnFsRE5LcmZFSTdXRW0wZEdEVGs4TGNhTUllUDB1eS9WV1BCR1pkWDE3a0UwQkV1bGNzNXJhYklzU0xxS01MYWRNYzhoUjU4VTNNa0lNRW9OclVEekVseXpRcDZEU1dxWE1aQjFsRVMzczh4U3ZSYU82RmRVZ2hlckZhdk53aEp6a1F6WFlVekFQTHBnM3RGdlRJSjl3ckROb1JPMDVtay9KUDRZM0c2R3kvSkpCNFFWeXV6K0hnSjNPQVZZRVI2eDVqVW1VbGNSKzQyY2ZSMVZkTkdvMjVxbk9ENUE2K0FqNVI5VXVHRTB4M2laSk9nNjJ1aDA2Qk5jS0wwUTdPMWdSbjNVeWo2dGsvWGk5MWtwNDEwTkdESGVkRUdQNTAxVldTV1JaeGRFRlltV0ZnM25RRlk4WG9jZWNVbEVETFZYcXVSWHhCZXpNclFEc2s5QWJDQlNKRE14SHJqR1BybWVlSkhSdkpWM214a1ZzeVRiSnREOWRkeVlxV2Y0MzE1dHhhUnc5MVAxNDRwVGJnK2FLWDhkRFUwa1dEN1lUQ1FrY2pJNEdscms4TG5NRmxNTVAxUzdYTkZwckFWaG01V20vazh4bkRIeXFaMi81Mm92YmlnMVduNHZEeml4cTFHZWUvRThXNzNkVGJvaWU4NUNmVVhXUFA1UlNLWjEzSnd0d0VjMkZsRzI4VzFvSVU2NitqelNTKzFSaHJFemJwMElUZEVvNlI1c29pQWN3RjFtS1RGS1h4Y0NZMjVXaTNmbUQyU2tBTlJGVjZhSmRubWJaYkp2bmZycUtIMnFRN1IrQ2VVK252VU1odzBhOE5hY0FRVit0cjZGNitBaFhsZzh1ZEF4SzRRZXA0NFJMNFNiRWJ4Titpc1hVSjlsbkt2Z3EyU3lBZVdLK2VpeTk5aDA3MHdhU3V6d21XTmgwd1UzamZDYklBR2RyS2Q2eTR0MkxLSjgrSTg0a29UOEV5bjVuNFZxN01iQm15aHhJRkFidW1RRTFlNVFGS0VoenE4cXhVdEVVRFR1bzVWSVJKS2RYS1BYUi9OR1IvUm5YOG5abG1mb3lhaFZPaVQ1THgvOWhHbTNPT0pDMkY5VzhUcGtHREVvUTRua0tSOFlzZk56UEw4Y3V6Q0lHMmVKTURsSi9Jcm9qSmxTVEtTa0RHQUhqTGZBUUN5Szc0cGVUQjFsTktERUREeHFVYklyelMzc3JUWTBBUk9rMVZyM2w0bUR2TzBpYnFLWDdLTGRUWHdLWDVYa2JzVG9xNVZaTGp0bklISko0NTFDVFpVa3pBWGVzUnVSS05DMDVHRzZBK3lJV1dFS0NnRXpHUmRXYmFqVG10b01kR05RUGtuaUpNZWcxL3BKOExvTUVXN0pYTThkeGFrOEVhcGYzOHhic1FwREMvNEdFRHhDQmlhQVhqbzFEaEdaeWVReHc2ajNHa0JzTzYyQyt0OURRLytZZ3FpV3dBR2d4RU9teHFjZXVuWGJhdGxlaURWVFlaN1JGdm9sQkwzU3V3N2VvWGFsa3RRWjJjM2RHeWg0RlBoeXQ4SkVnODJtaEwxbGFjbnhUU2ZkZTZVZFlKYjVZeXVYdHYzMmJuZkoxdDIzcGExcnNVckdKSmo3cE55amU4VG9vOTAwNStOSiszNnA1VHBNS2VFZ2pjcnlSSm9UWGJkWm12OFV2TUlYdU1Kcy9kVFN4WkhIRk9XUlN4REdmTk5FbUNOdzJrQzY5d1psS0FTUUVlS0M0bWhNYnZVTGJwN2V4NWNJeXF3VDRsRGtacnhVV0tZUVZRQzBLWkVMU201cDJ6bHNDNXNRdDM5cURva0YwMDNmNURiZjFpdWxncjBRdHpCbzN0SmgyUklzU1gwZWo4dUtMeUE4VStLSm5oK0EvNEtLVVdzb04xUlZKTk5oMk9iaERyZjhpbEp6eVU0TUxVUlZnMFZGVDM5dExDR05ZaHVBQTk2Z2dWWTJQOEptcHd0QXBiOU5aTnVyVlZOS0kyblNOK1RRQkw2dG5pb0Jzb1RsSC9rQkxZYWpGNXp0UEFKMnNsaGp6Z0VYRnpscTUyV2hGZWlHRFFlbUJIRHZrbXF0R1IyNXpkTG84NStpbGYrQU9TMHdvTG9EQmZsMklOaGlJRmt4cnFoOEJIZ1JYTUM2SzNJSC9pSHlqOHpJNFhVV0MzY1VxNmVMbm5sS2lFYitOUjgrNGQ5Z2NlRVRJODV4UXlOMTYvNFkrWHA0SHFjYys2eWxkczhvZVUzc0h2OVJ3Um1VZ0tvYzlJUm10SWJ1M2R6SmVXbzJOa2FoeUpkVGlicmRMRmhKUjdiTlNzcHpkYWlZUWVmRDZJRDIrQTI0VWlDU3pKelFiSktmSkhXcENZbmVydXZkRHIyVzZXNzI5YWRrV1BJR3d4cVlDU09RZzNuV0xpdDVnWiswVG1PR0FrUmxzd1JReDMrN1NHS1h0YUFmblh5U2swL2R3MTNxc0wreXZ3aG9tWVlyT3IvTnl2ZEFjMXUxdzcya0doN1B4REhCSWpBMVhKdE9sYVZMV2FrMGJIZFBkR2hpdzRqUGtFMzA3VS9YeHhkVkZFYzYvZmF4TkNUZWlnM2srd1d5TVNPMkpNVllISVpxczdJZ3BmcGxrU09kczUrZVhRVTN3QXJUTHNrWXFFbytiTE5pZjIydkVYMWp1QlpQOHFHWUlOaEw1aUxwYzV2L3BJMzhZaVdtQ2hxY0EvNVVLQmZYTVg5RDNXb3AzamJzSHU1SEI4c1JKUWtiOVc1cVh6Y21wcHRWTkFVL29ubjJzYVZwaTRnbDc5M1VQY3NBS3I4c3hSZGJ2ZmlIZmRYeFVBbEJaclY2ZXlUMlVpUnpCTis1MVhkeWwwM1FrblNpMkM3RFNTTUhxVGhZNkdkQTNVUGxWbWV3SmRLbVNMSHBCcGhJdHFnNEVXb3BpcHhCWlVkVWJwSlIydkRFRVc2cXRyZjhrUUhpOEJad0t4M2szU1QzYVJqWXFJaVhYc0IyMVJnYUVLOEJNS1FHUzRQaCtQSWhkSlNvU04wNks4NnBPK2hoSEZLSFdCdmRYZzFWL1gvay9UREh6QTNlQzZzY1YyUVJJYncybENzMHdzanFrVU9rUXAzVkxoaEZOODZHRnlGZnRLenBQcTlNMUFkdGdkNU9WcEhjTFQ0Qm5ucURiL3Z4cHBaQnpJU01HUE4rTWN3cHYxYy9YdExvbkl1QmlEZU90Rmh3eVhLL0x3dUF0azlIbjlHNWhPZkZtTzU1T3UxMS8yYy92ZmZtKzVSMUtxUDBiandvTTM0dkw0Wnk5cXIxMSsxRS9jeEI0YzNSM1ByU2lMTVZ2QmVqRXZSaWFEd2J1b2FiMkc2aFEvL01zODh0Ry8zWk8rMDhYa1VJZVUzMG1qdFl2b3NacGhMT2pyblN5Zkdob01Nekk2M2RVSXlYVmlURXEreEt4RnBteGJaRFBTTVBXK1ZZMHdKRVBqbTNKRUptQzhxTDdzYUd4SitRaUVxUUNRYTNLNURMRVQ2UTcwek9pR1BtOFo1a0U0U3d6b1l2bDJTNUVSUjkzTm1YZDFYODNzTzVZYVJ2S2tCV0gwWTFMaW9zN2RSNnZhVWJsSnMyYitOQTlNTDhHVURQQWc3Z1pCOHVvUEg1YkNnZnBxNnRWUjNlbVQxT2Y5R1JLdlJrTHZHK3ZDaXZhL0pUTUxIWkgyNGcrQUlaK3hUdnpyL3gvVjNqeHcvVzFFYnRwKy91dzc3c2o3QWJrcXlkdlhrVE12TWZQNkYwZ3p5OW90YmdxRy9MQi85ZVBWYXVmSS9nQkNHQnRwdDNLWDdrMjdWaHNUZGxmWGxmS1M3YVk3Y3NlWkVvdzBiczhnNWVEWWdmRXJkdGhYRHpBbStFb0JvUk9VZXg4d3o5b3RCSi81NXZLQUJ1Nit1enlBaUZEWG5pQXBGK2NqYlpWUjlKSFo4QnFtcklvREZzMnVwZkNDVFVuU0sveVlzSjNrZkliTzVDQk9iUnJNUm5ERUcyb0JlSWhiV3lSZ3doeTVpdlhWWFMwRHNUSmpxbEM3UTFNTU4vTUFISmtJTE5oQjY5MG9yVWR2cHNEd3RIQ0pDSFBKamgyQ0RIVUFEWmhrSWFmejI1TkNtUkJrcUxRc09TOVUrMWRFcXkyMDFhMkRKQ3B3NS9wdzhpVmM2am11SllXemZUQiszWktrUWpJbm4vd21ibWZ0SXZoMmF6b1lCRGdsbXpJVE5RRnhHLzdmR0Y5aUFCVlVMRU5FUWxuajRtaGNHUTFzbTY0L0U1U2RMd0RHMk9iT2RZVnhTYkRTUXJzR2NQQ3pxeEd4OCtUZ0hscEpjajQ2L2NjZitPQjd5QTMzMnE2RzFHQ1kxK0VESm9FOWdLZnJOMzZJaVNHN0VISmIxNXlxY1ZNbm55MnE4V1JVeDFpcFFwUitSYk9vQ3lYZEpZdU0rSCtlb0J4TitMYTN2V3R4aEMvRWM1M2JvTUhjQ3RvaXgzM2RpamQvYVg5SlZkZEhEdVZvVG9QZWxjOTJUTE5xU25ha09zdXJkUm84MmQ3d2tGbUIxWEhENFJrMWJHV3V6TTM2TWRaZzBnMzhHTk9wbmRZWjE2ZlE5bkszdmlTZU1yTEhwbEkvUys3cFI2YnZYWktUQ2RocEJPWDlZUjVBQ25zeUFvMDhDUEhlQkxxWWZraStTNlpaOG9zK3g5NWZkN3lzZCswajYzYk12U3NQVFVvUGNQQUtSRUFVM2txZmltQ0grbGZLL1BTK1U0a0VBdUFwSkcwTlEvL3BYdkg0ZEs3Y251cTRSTGxiV1ovNUJTbWxmRGJjK1RpOTVFWnE2NXNlY2NacEVwNE5wKzF5MWI3cnE3YnNWS2tpMGwzNTd1MzhuWTYrM2dyeGtJM2tBRldxY1JUbG9GK2tYcDBHU2FjTTZDbi9wNTlpZ2MvTFZIR3ZRZG5BUlVzQlV5N20rQ2RldWhyZHFHYmg2cFMwVys3ak5uczhqZWZ6Nit4WEV2blZzdTc3UDIzS0xYa0dLRE9iTkdBR0o0cE1tNWRyREdIRm5Jd2NXSmJ6a1lrMnRtVmJJSDk5UXVTZGZkRFZqU1NhMlZaSjJNcFZSQStKak1ZbWFvRmw0ZzhuZ1Rma2ozSnYwL2YxTlk5d0RZblpmMEZwV2Q4YUtLc1k1MFFaM1NRRmFPU1NEaXNJNHMwSEVOVWQxU1VGcElUR2xpLytNcmVId0R2cVc0S2Z6Q1YxYVpRYXlqanRTbk9pcHl1K2UzK29KVGw2ekRkeE5oSDFBNG4xV3JOVy9IMHltY0tHMk1iV3hkREVCZjVRY0RDd1hxdlNQbzZtVzNRNGhPWnRWSTJHUFBBUVkwM01jWjA2M1poR2tlS3h5R3UwOFVYNDdlV3lMa1NCYVJwZFdYbHZYdDZwK1FEWHFIMmxDMVhzUk5WQU02UlNyYzNnejI3Vi9mNFFvQUVUY0xTbUlZWVc3SmhxSTk5SGlPTkxBZDcyYjFrODJuU2ZzekUwcS90VzhkMFNWUStGcXlLUVIvd3FocTVOb2pCUTRZMUUzUk9HVWxQb1V3NzJ5eEdrT0hsU25MamR0TEhJaVVRTFA5bzNKamdLM2ovWnNOK2lvSmszeGplcVIrSFJtODE5endCQVJJNEcvME0zRVAwbGZaVkVteUFGWkZoekNYSTdKQ0hGZ0pPNG5jeE9nVlduUW42ZHpiWE5UN0N2alNHbGd3MXRVdFhqdi9yS1V1NHNaVHgyQi9KWjZXVk5XdkVMOFZUdWEwOStnRGZBbUg1WlAwYmg4OStaYld4YkZwbUVLeWduZ09ZaGRLTWRVOEg4cnVPbHoyUHlidUNxMXFVaVpUUjdGa2ZMY0ltT0hHZnhWUEZXUjVIQjZMOFloN2hjWWlST2cvK2VYMzE2SUx3ZEFRSnM5R3d5WmJzSUZNcGNiM2VHaG54eFkzZzlNYlh6WWc2QVRWbzM0R0hMRVIrT3dkbFpPSldPWVdqSlFaOUxtVndEeTFGTGphYWR5ZkdWbmRQYXFCc09BR3lBTThOVkNwTml3Z0ZPZFp4TXNDT2NsTUhmaW5kOGpubndydVFsZ29TdVp5b2Y2d1MzczBiMURxUHY3R1l3RzFnOGJ0YnUwbU9IQXJOekZLbDRlcXpKTm5CQzl6cUk3ZktpM2tqTjVuRzhkdDZlTG94OS9YNmJhdUc4YXNwVkFtSnIvWHl4Rm03SmVVTlZaczE0Umh4OUlmWk5FWVAvZEs3REkzRi9EbHdzNEx5eGY2M2M1ZFpzaW1YREFkYlJnTEVwdDRmVW8yNGhFeTM4ajRvcGt0KzFvS0JUZU1vcStMT0IvZ2R4ZkN0amNXSDRLcGgvY3Q4THV5ekZHQ1I3OHN1RUJCVGFFRlluRDMxRlVQZkk0YytPU3FUUVMvRXp6c0lxTkFPOHBUM1FzdFh2VytnNlV2ZFZLQkxzQ0kxdkxPTmtvTDRjUXpwbWk5bEhQSXF3a0J5cGV3K1l3OUdDeTkrSWNwZ3lYRDlEQmNJSTl5eXQ0b2dDNWtibm92bDErRWhiT0JPS21TMHVOYTBHaDRUYS9OeTdrUXdLOEMwN1pOUlBNWXpxM2dxaFIrdGMzVHY4bWxJYXZQZ2o2Zy9iRFJ2QU1nd0xxTk9vVWZFTitPenJaS3VkN3IvMEprT0hiUUtQUDgyZk5LdXR1TWVqN0pES1BhTlVDNVZhb0lTdzE1eEpUZ0twVGRuVFZNSkUyTzd0WHJCQ3lkSXlxSkhQaldmNmlpS2V6ak5UZVcyV04rMGZ0enhib1VwczYxTFNTdHFpVWIvSDVITElQeWh4VVBDaDJYNEJtVmpHR3pWZDNKdGVaN1BQM1lyTzFlTFN3ZjRmUkF5SlBzTzBkWGJ4c1JyekNwaElFdXljYmh2THZWczVKc0EzMkc0RHVQZDJ5VFoxOWdBT0tGb21lUFdtRXAyMkpqbmpYZnVGeER4bHFsdjhhVU9OTEc3SzRaVnhCd2lCUThQSWJvODV6QTJlMFFvTmZkeUlWODYxNXVPUml6KzNXMlUvTU5BNFZEUHdFb2pZM2JRdjY3b1ZsL21tMmNzZUlXUDlxM2lnRVdjWFdOdm01WGhJU3dsZCs1V251T0hFdTlaT2RYeG5JK2xERG5LRE1VOU1ZY2dKelVSSjdOUk9lL29ocG1xbm9CSHcxenNKTlc3TU9wWE12N3FkZ2dhekgvWVkydGNaWkMwa2xFOG1GZjliL1RldGNJbTZodU1ZT0MvSDZyM2lTN0VLQXpqMGEvWkp4bXZuVGFqZGpodXRoMDVsbngyMU1wQ2dJR2RlUUtJeWErZktReVhEMU95RTYxK0Q0TGZzcWJvVUZnYk5iT08vc1EyckRPeXZ6d0REenFnbWxJMUovTVFxeU9UVU5PQ2NBQUZudzEycjJUeHVUVFYxcmx2RE9OZlRMMWMrUXE4bm54WHAvWEZXdGx6dXdscnJ4QWMvVFBabVVOUnlaeFM2ekZ0UFFwMnNwME1udU8vUzl6SmExT0ZlRlRPQUpXaytvQmcrK0NwUGV5Q29OcGdRanZ3Sy96ME1vSTlTZWMza0FkUnd6RTJnbnVOQzY5b2pPT050NG9GZDhOVnNHd1UyU21ScGJncW1ZcG9qeklOdUt6c0ZjeEluM01nZmk1V0MzZXVQZGFwN3dsajAvTERIOUpNTFdlUVYrN0RrbmpDejNLSFh5bGtTZ1YvTm1vanJGN3U4ak5tWVVtSTRoamFpVlY0YXpxd1ZhK1I1K1lmdDk3QlB5bk9SLzdYSzVmYjE4NEZ3SCtzSGFIMVJ2eHM2TUVlc1IzMTE5SzNWdWdVOWszWk5sZjNUQVdpWC8wek5NcWRzNGtsVXhBN1JyeXdHMm52YkNTR1BpSGtMR2xWZjgxMk8vd2NLclRtYlM0Z2xOdkk2ZnFKV042WU1BdlYvQlErNDJtRzVwTHZjbG9acWdxTk9oNW5mTmdIY1BNVXVwdUtoeGZjYzc1VGEvMUozZVZrMGZVaWIyLzA2UyswWGFjSmVQZmg3R0RNc0w3aXN1OTBRSnJvRGxZZkY4U2NMdzN2UVdlMTk4bWlHVGY3eTAyYVpiQUgyY1JBL1RucS8vcGFUbXJ0R0RqNEF1QzAxN2xWaUJuZGpNS20xaVNGbEVia0thTlBjRVk4WnRRcTBpTk9DYmVRdkZFemVHQnNwN0J3c3NCVHpZcDJLL01JSTVKV1luak5kWWNEZ3Q4UmsyZ3I3amF3SDlPSjRCTm5mNmJVaUs3ZnhWREVWb0ppb1Y1ZHBnSll3NCtjREFvOUJzektaeWErZ0hhR0hMenlnb2llUDRMNEpSdVdWbHdPMFNRWmxNZDlGN3krZE85YVZ4Tkw5WXVQOFdURkRwSSs2SEY1OXlleHBUOXlleWJCd3M0ZUErMW90UEx0ci9QWVNsdlB0VHMzaGlaOWJXZ1RhTE95T2QwR2ZMZmp1WDBzMmoyNmhQT055V0N1OE9JSWtTNmF4RFVBYU84TXpLVllSb1FNNFJRWUFWQnhBRVVlQ2svREJoRlFWUlFEQWlpZEM0YklnWXdhbm9zQVBwc2IraXViL2ZFR2F0ME1IY2JORytoeFJWQVVZMkc1MGN2MS9hNXdoTWVqMllaR0hpY0lMK0V2aWo2ay93Z1p4ZkcwenNYRUhseFQ0bTc4MC9HWFhrTWRWM2lGQjZWQWNURy9oSEZldmNSaTFzN2t1WGRGRU9WZjQzd0NRNG5qemhEZ3ExdHVhRWpXL0hTK0orM3NuampGcm5ZTW1PdE1ndDZQUUNkbVhpeWdJMlFhaWxvbEhzaUdWaG95anpEUUE2S1phWnF4NGRTaFpjN2dNZjllZlZLUmdrQlcxUHpaaFFrZHJaU0JIUHlPQlFVTFZLQjhmTDkyS0xLNDAvYWZOUWg2bk9tTGlGcDNVU2lGRUFSMlVrNGN2QmdTaFNnNUUwb1pCd1d5M0lka2MvQTFuL2MwV0JzcTR5RTJqcGR2N09WZWFMSjVvOStDaE9MTmk2M29YQWh0UmFna2wyQUE2Vy90RHRaREF2ejNPeHlxZzZVaTFvbExzeUNpbmwvb3RYLzM3TDlxVnZXK3VMbGIvRjlhVmh5SW82MHZEN04zN2JkR0xORnJpYW1nMUsxM0dObC9kZlI1RFVGQ0toeXA1QndvTDhkSGNXc3hxZEhYRlFJeTAxRVRJWFFsM0lCUVJBaUZUSUdCOXVWYmhyMFZpN1ZuMCtTem9oLy80eGVTZFFwV1kyRXY2OGQwK1lDNGhTVEcxN1pmQ2dkKzh4RUM0K3N5MzZmL1pOVHFyK2lWaEFqVEEyTXU4Vi81cWdwRDNOWVpMNVdOcnBQUlNlanhQcitNU3hLc2MxMXEvMExvb0dqNXphWCtuQWVmWW00ZW9JYkp5akRBbUw5cnVMS0ZnMk5MWHR1OVp2cmc1MTdjcW1QTFI3UVpYc0UzajhBMjNSYnhTVDRYRDJqL3hJdWlrRXhYRHY2ZlNWV1NHNjQvbnF0Q2tZYklYSFpMMVpSS0hmNkxlQUl6WW42cW50QjVkYktOYjVBSUhQRzd3dEgxTDZadWdUZjNkZlR2Z0c2b0xGMVg0SlM1dm5DYlV0Nk90Yko1QlhhT2dzK2tYVG04NURsd2o4L1E5RFhSRlBTNklWeTVud05SajZpTVp4bTdXcVVsSU1YV3o5WmxoSlNYZTl5WlJmemcyZkUvVnJiWkgzSHBuVlNjVlZFMk9vZUJjTU45aG9pNlNlRGVUTlhWVXVhYU5UK3R1cVJXWmZYNHlNcUlFTHJJRVBpRnVMMFR0NmJ5amV6U1hUUVZoanlJTFFMYndwZUZvcXpUdkhKSG5lZ0c1N1J4YXdoZFozak5YcjZtRE1jckkyYnBLeC9vanBub0loS2Q0Y01CTCs4Y0pYUHVxb2t4Q2JSSlIyT1lsSmEzZ1ZwSk1OL2RhSFNZWFdYck8xeG16dzZpZ3JQV2lQNmlXSFkzU0ltbUVPNDhUTE0zckQ4dzFZcGxZNzN4NHh2OUtHcmhJRzdqV0ZRQm9UNkFGKzFWOTFSek03RWJ5ZXVSc3VkY2k1SnllOVJTWGFZWXRmeHoxb3J0R3dQZXFzMjluOXZ1dTlzRkgrQWhSbDB3Qnc3MjdscEF6M1BOWXRjRE9iR2JBTWdDaGFHaDloOUIvOUtYMFhlSG0yM0EyTnp1ZWFLNXJYTDM2RnRwTEdvM3p3M1VUT1I3YTZ6QUVKdEZ6YkNPMjR2UFcvVkJnTDlKemM5TVRCYktYRmlxVDF6WlVtQmxqeXFzZG1ROUJZVUxvTnVJdmRlK1RYWENId3BNeWR2TVhMSCtqcHl1d0dLWWc2STRyeGdNT25VNDJadWhGemQwa08wbGt0eDF5NE4vbzRBM3NGeTZibnRlbFFJZXlKSUljYUNPY3RYOVpsenpqWVVkNmVodzRvOGlVQzdyNTM2TmxUUE5VMHpDVkVEbFQ2ZUdiV3dlOVpMQjluZkdQeC95VUpTNXZZSWhKUDJQNlVMa0k5SjRReFBRemc4Uk1rb1ZCZVVEVHZjVGIzRm9peTF0dGZIZERNMlVINVE0d2ZZSXZjemVKQUF5UHV4VmVXMExUQ1JhdXhNUk5tc2tOK3JMVEh4SE54azhaY0dSSVIyZHU1dmM3bXdDc3MzRWFpajRGVzl5QkVZZVVCZEpCQ1BBVEV6YUEyMjgwZDV5SWZEcU9CM2h5QzZrMUtQalE0dmhHV3ZxZ2h4WUFCQkpJRlVuemk1MFM0cThHMmxrekh0cWZtL1RqTlNSbXNjUEZneVFOL1VSRkhpNm85TjFDdXBDcnVhQUpNTnZtRnA4RFZ0Nk5KU2xNMXJ6R0xOMjZRbCtMczh4TzBxMm10UGx4REhlb0ZMZDRDamY5TndzZDZjeU1lVFRrWGdGZkZSSzVjZXJkMHgyeXk2SDh2VG9FZTNBRDlTYXZzN25BZkN2Yi96MjdkNTVoR2ZzOGNYYjg1bjVVaFg4UnJMQithQ3F1MkJxYkxGTEQ2ajc5N2tRMjVXcVAwODZJdGFDL1R3bDVENFZtSkxIaWlEbjhRcWdQOEhqTFp5c3FvbGRnTjNndjJOWlExbGlKQU56UUllbXpaL1Y2RjFMMzc3YksvRksxQVF3ZTNNc2l3RmhMM2U2S0c2R1B5U3ZlUDhlUS9jMVZsVlBNTFNrVXNJRnNLMG52cmU3YTM4S05JRU42NVBZTVExTEFrWGRCMDA3SUNtcG5aZ1J1bWcrQWROL3o2UWZ6QzMrZDdESTNxSXN2MitzdjF5NHVORzdJSnJlemZ0TWNUUEx4N0NtN2NzbXEyYWdMSnd4anVkczBiR29rbnZ5NU9SQ3hlR25raWFJU1d6M0hrOVJMdFB5dXFOL0dsT3dodXhSZkJnRXZYKzUzM3o3ZGZndU0wd2lKdXYrZzk3V2dmOFRncDdOSzZNbFdxcGo4eVdIam0xL0xDM1JYQi9wTTZrbkN1RGVMUjJpQzhCV3grVzVYOHpYaHJIUjYzcHhBWDhOZEx4dkx2K2hUbHYvZG5WM3JjTGhJSW02SytDM3lkMEkvMW9zOVFWWUhlWnFnQUJ0bkNXRlBSd1hKQWZUUVlremZlczNjS1d4cE9ReVd4L2prY2pPcDdOWDJ3RTl5cldDaUdDaWVoTm5OK0FwaWFBb1lFdzd2S3BpZTQ0TnVNZnVHVXJhMkc5R0piV1dYWXJEMmtKaWRhU3kzbFhnOVFnWGhpWW01bGFXZzZWZDQ3b2tPZktLMlBOU1ZodzkwNGg2bDQ3Q29JbkM2RUpvelhXaE5sazhaclNkMFk4M2dnbi9KZ2ltVVdLd2RRL2pZOFR5Y2tQL3E5c3pJTGlhY3JIcEtRTEhuWndpUU9QZ3YwOUc3M3JwVFMwL0J3SkVHaVJ2RWhUaEtSWmpINlRCTXFldzU1cTV0Vys5eHB4N21uckNYc3FJejJiT1BXZ2xoK2dZczhBeUZPeStBMkJZVzhoVkdyVVdLK2VYVDN2SHJycSt4TTFRbDhzQmtHTVcwWUhoOUgzRHVoUVZubDJvcFFjM1kxcWdDR1NEVDliVzA3WWFGVGZGbmhEZi9jamE4c0R1TkRZeEFoNC9VL3ZjL2lBLzd6enVneHRRK2hRMHkwczV4Z25DUEFkQXlGdlNIVTBlYUNqY1YzNUU0OElZTXJ4VEdpS0I1YTdxSWNLN3RPT1dmbGFIMlYwOThBaHJwbWl1L1NZSjM3cGZBNyt0bVg1RkVnZ05yMHQ3MSt4NXZhaU9KWVpPRzBEUEpBMWhQVDZvdUtxNERDOFRMTzlQWGFEbXNhMzF4bmRVb1FleVFteGdvWjY0MEVoMjBNVjEyVzJnd0RjRlU5RGhweEFjRmhRR0FDMjhTWHMzNk9rVTNKODFkZlgveUtRR3NxTW5kMGQ1dG1IeHR0SmVKUXNjNkhBRGZLYXhVTDlvUStqSVJzSUcwSVAreXBZYXphVndWbS80RzlKOUZDaDZtNGFHSVY0SmlXMTh1OGlBWGU2ZTlFNHQzVlFreVpCeC8xZWUyRC8xcngyYXk4YVhFcmNIZ2s3d2Rxei9oUi8xODNqNVpjVi9DK1ZYblB0TXlscTNaZ0FwcE9USEVIWTB4V0dFUFRqZkhSK210bTB0VitZVlo5OS9LZ2lqVEpaNzE4eW1RNjVnam5KN0RlTldndXJvdDkvWGR0eFRzaktJTVR1bVJ4R1o1UXgrVkNxMmR4bWFaM2ZYWGtHVnV6dUJzQU1lbXdua2FneVJWNWxFeXFKTkFzRWpua0s3US9DTkxPQkZ1cFR2SERic1BkcjI4NUF4MUFFSHdzaC9DQ1Y0cG5DU1VkektoUjZwRkZKOVpUNnJnRDNmZENneE9hVzNzeWN2Y3E3MWJoVTBPLzdFOXBreUo3a2tKYUYvN29BRVRrdTgxTmV3L3J1amxaMFM3VllybGZHYzQ5K0d6eG5sR0NleHQrc2xRZ3Qwc3hhc29IM21wdkFuNnR1VEh0K1R5VG1tZzU3UmlCd2s0T1VRTGVQT0pKUjlsMkFNV3B3NlkzUVBnT3RjTFArNWtpLzBsWkdEbnEyeVpaalVzOFVJUWI1UTFYRGtNdnRZZ0ZwUTY2V1dyck1PTzgvUW9GT3VwMzVQRk02UjlRS3VDSWtKcTNwS01FTDVtTnV5aUx1QWR6ZGZ3SXN3NWZaUEdNL3Q2T2ZOOXRLN2t5cHBDSDk4b1dZUnZqamdrbEIrOUxrQ21ERmxFS3daNFFXQnl0UlowWTRIR1VOVU9XUFhWV3lZbWwrU2pBaXpldTh1eXZSNHJWQWg5c0ZZYmg3anNabmNOakhkUmdZRVdSanVHUlJqZ3dPTDlXSlFEQlRpaGxoaEthdWRxNzVXS2d6Y1lORGpDcXJ1RElZOG51TUI1QXkwY2NRWnpZZDY2MWtkSjZ4SzMwaFc3eWFWdHlZMFJEZ2lSUUNldXh2QjBaQlg3NDZyRDdIOVd0TE9rUUgrbmVmUkdlaDhwQ0lra0NtYlVNSlkwUHRzTGhUVDR1K2Z4Yk9nVmZleWpJZUhpR1JEdCtXaVJTTDhnSExaME1GdFRlYzA2djA1U3JKMCtWVU5zUndyazBKUmRIbDExQVBNZmVRYytLODVCcUEwN1B6MHNuSE03aHlzaU5IS2ZWM2E2WGcxRDRCR2RvQ0JRVHZOK2R0YUtFbFAzNGtwbEF3K09NZlB1WnRLZ1I4ZnhaZTJ2eURXU1dZWTkzdnFIenhGYUJMSzFodU9oQmVoaitraThpQ2RXVER2dDRodHZ1emt0cENYZWJpMEFCQWRqTlhWeWRrbTNPK1JUMkVDRlZ3b3IzdEZHUlp2bE4yQW5yU3p1MmJiOStJTWhLOXNEQTd2SDlIaDJRUVhsRjVrMVIwUStRVmRqalQ3cUo1Q1cxQ3h5YnVmRU16cDAyb2wrSFNteld0b3RZWlVxRUZpdWh3YW1ESnZNOGtQeHlUM2Y0OVgySXhnbk9KR0k5TW5GZ2poajZkK2czd3B1VzdHd0JiZmdja0I3Vnd6aUFtR1FSMkNnVGpxaGxLN05pc3YxMEd5OFFKamZEeWc3T2FkbVdGd2x2M0V3cDJlQVVkYmprMlZaWGxVcVJnNXhvWUhWbmtuc2d1YmJNcGx3MFIycFpCUS9lYjh2VmgzUGdhYlg3WnpPNGEyQjI1ZlNnTGJBcVVxQWgwQmhnZzBXc0N6a3F6VDBYSXpxWUZiZ2tlbU0xTmZKN2dJVDRKWVhsTkk1WS9VRzhsWmlqTDVOWFpMZXFEOThmUnIrK29aTkw2eU9IeE5WbzluR25ZdjRpL05odlBwZWtmalZXYTB3cDMyc3VQM0k2RFhic3ZFU1JDbC93dEVXSVFZTnd5bDlFUXR5c1E4OXZCdVhJS1gvTlRaeHNhbXVoWHNhZkMxV0NyTUR4ZjNDVjZySE9mNHBxZVA0Z3B6L0xRb0JzUHAxY1VEWWhoRlMvVy9DSlNIVXRtVHlGN2wwYnp1VXlFZjJUTFc4ajNoNXM4dVpndGNzaUR5dDE2eVFSbFpIZU9sYmVhaGh4eXp4RTN5cDlsQitEUDN0UlRydHMzam1kMUduSXdNM2xsdVBQbnNJYnlPZjBYbnhuTHk3KythR25KUzhRYnFtMDRYd3h5WEtHYzI0cUhZUE4zQjdwdDVtTnhPVStxKzM5aGtqT0JXcWFjdHpWbmEzckd0cU5sM0FnWnEwTE9rMW9RSjA4NkRVa2crZllheGYwSWpyUkJEd2pGcWZLRHlYRit2QytXUSsyK3ZLVG1HMjlXOFFHWktvd3NaUFRkQlhQYkdMWlRab0F0Q1M4elp4ejZNNnBBRXFpdUdFcURCTFlNZmtiTG1NaE9uanA0VEExeXpvMFBRZXBOT2p1UU1ONEdJbnkvSUxmVnZUVm5JWFRiZ29ncUhLSkVkdU4yQ2RUL0JWQTY0c29CbTk5MHp1NmlGbXFyUkZZQ1lXVXlBYWZZT2tuS0NadEpvcWRIMG5xdjJLamcxd1pzRmRJR1NWb2JEMDg1NzUwamJvYWdML0YxNGJMQk5ZY2JNenJTQ25LaXM4aWdMZTZQQm91S2FMazVtYzY5b1U3dE9VT29mZUJBbGUzMGhPRXRoa2dxU2JuSnNqempuUkk5RDBQNWNsTnhMaTVFanU1OUNTMGJ0TURTMkZHbERKYkQ2c1MrV1JkUTVEZzZQaVkxNWtVOFI3NURMdEhVOFp4bUNVUU1aQ0YyUXQ1eTMyZmNTNDlBanlXM3UvS2RkMUFLNGx5NWdaU3Jyb0d0bitFSjE3SXJSWElvUGFvbUpDOHh1RStrRDhxVVFZTGNYR0Z6NXB4a2l5QWd3dUJ2THpjWkdtaE92ZnF1cFQrOWN2T2Y1Y3pTZGgwZXhoa2dwRUZOU0VUTzZyaHBqVGRYWGN5TmlpY2FmTU10SzRJeHlITnJYSDhxNkNlTjdwYThpaFdJd2xCaWRMZ0NpalBITWQ1d3lYRlByc2wwYkNSNmJFeENmdVFwRUVlYnpocW1Pc0U5VHlqRzdTM1VhM1VjRUl2WC8rdmF2UUxkeUhHZW1VUTVEL1RMRW1EeG5PMStITjBjenlIeW40bnhVVG9FNDRlMGxSQU5ER3dBTTVMVU5VRm1jcU4rMVhUV2s4MGQ4UFdiT2htdXo5OE5QR3g1NzNsTkllNnBaSDVZOStRT2lLWUpRUVJmRHZCOXI1ekNTV3hzSHJ3eU9penhBNElQVzVOSG1GSFRTUzhjNS9hUUpDZmcwRkg0Q0NpWHY3OVU5aFNvU2pSYXBpaHFXTGtIbnRTUTc5U1AwVG9PRnNJS3JqQjRWMHFQVW41cTBDQmJ2THRpSTJYSHUvOHEzZ1V6aSs1TXAwbTh5UU1xMGhwNGZDRnBhZG9lcGk5NmNTcmJ2cTdNenhYQkpuTlQrRDgrM1U4ejZuai9CVFpjNEV5Tk5ySmxPTWY2L2Uvb01BVDYrbUdvUktqekFKWG9KK3BuSDlONmxFQkhtQUFLR0dBRnc0dFFQV090Vnc5Rjh2bHNrc0t0Mk1CQ01qeDFNUmUzRjFtTk90TnpXNS8zeFZEUHUzVlhkVktSTjgvMm53dmRTcXlhRkJod2Yrb3B4cWpPYm03ZWl6b0NHK292cHV3Ri80QnEvb21DQzNIWXFYZjJQS3FIV2RYN1F0QjQ1eXdOaWFWd2xsdlJIci9kczREWkFkdzl2NFA0ZGpxdXphRUIwT0EvdEJ6M1hiOUNuRUd1eFFWYm5KQzlEakp6R3FWK3NEUEJ3M0lXc3FtVW9ZeUNBcEd2N2d3UEVpNzFra0JmeDY3YlBDdG1FcVBZajlhUzZXUWw4QXIrc2tQMTc4Z2JSazdZSEwycThJVElyRjJ0TVhZVTFWS000UXpocjlCQmVIMlJhUzhOQkJEckdXSXZyWmxST2d5bWRqY1Y5VzRvV0lYdUFvT2ZDUG1lTGxGcTd3ZDZwd1ExZEEvMEowUm9HbjJmamNGRE5xN2ZkQ3NZMnUwSmN2eVVNYXpqc3FwUTN2YTkwK0JTMVJDN3dFbWl4dWJyYU1Rc0xxRHBiUVE2RFlDUmNSMXpqalRCbGwwTkpZZUN2WkVrajNadzE4TjZNYVB6ZCthTEpEZWFJa2p0K0tYblNrQWJkM2oya21qVlRPMHFYNE1xYWZERVFjRHJ2WU9VZVFWL0h1VDFiVm14NHF4UDhmSjNXdGE0MDFRYXcvQmFRV1BTTG5ib3laQ1VvV3dqQ1NlSkpaWDMzMnRPQWd5Y3ovU1ViY1lEaGFnQkhCSnk1VmduSTdZN1BDczB3cTV4V2hHNjN6OVVTTnJ4V1p0RTdNYzk0aUM1VmJ4QjRoVTBoS2xxVUJLZjdWd2JVZzNWQzN0aVhaQ2R6Y0lPcFljLytxWWFTdXVqbzBwNmR3QjNFQjl2RkFVYlQrWFhFQUplRXpPVUlnd3A3RWV2Ky9EYkZ6bWg3NzIyd0RsNWRKYmRpYXBtK0lUMGJNTndWbGMzL2RXcnZaRFgzS0o0TjNmNTFKc2E3SXQ4a25FbDZYRjZ1Wk5KUUgyajJ2R094TUo4cDFjWVdjQmt6Q25OcUdjM21OQ2hENUQ1R0RBL0ExL0xwSDY3MjdyS3Ywd3NQRmhhTW1BSHRYaXBJbmJDUHBWT21TODM5dVQwSkdBd3JJZ29vR3l5eE1iSVFBVmd4aW9QT2dNTGNoTXRwZy82SzM0SGo4d0ZxcW1Kek4xUlJnRkt3RmdPTW1UaE53N3RYRWJ2ZkRES0Q0S2QzRGJvYlBOK3BKQTFJc1A2MTliVlpHd1EvUCtLTmxhYTR0ZmhIUHdrL2tuUGVwdkdLRkFnWEcyalgvVTBrNm9zdnpjQkhpNG5qSGIzUXBtZ1NseTlud1MwSW9UTkNnSUtDdzkrQTh1eGNRL1ZJL3diZEVnN2xqZG4rWmlzODJrYUhIVVBsT2RTY1ZubnJ1VWhyWkZQK1QySGJyVWlBeEFTQTdRenRGRGNVT2g1NFpkNHhSK0dIMUwxWnprczZuakhCbDJXZFdabUpKb2pia3N1YkdDRDFQVThQU0dBTU54UjA5NTM0eXhKRU9ubllLMnB5czI3M0VSbVdnM3cvalJ6WER1NzBKY045YThqbDBmTGtGdnNVNjc0b3Y5TDJzaVVBd3krZmpUY3pMZzB5d1dXL2xjUmVNM2NIaVZTNXk2WVJCZ2NacEd1UjJNOWZVbW1mUXBGZUt0d2EyMnZNc1F2Vk9TZ2xYcXpGWDRuQUFPYXlIQkgxb2RnN1pXM0xxK0xJaWZMNzVDS3hBL2NsOGxYRkd6WElSZ0d0VDQyZ3lDR00rMlJWcGdFYmxUYjVlWG1BUWwycUg4d3N1VWZHeXFLaGEvMDZTcnVqdys1M3JKS1N0ekFFajlnUlFYVHlXWmdjWGNMVkdjMVpzcU5pRC9RZTBmeUx2SWtFc1pKMHNOWWdWM2hac3hFOTUxOFZtVXRtY0FDQUVGQzI5Tk5mSGNzT3lWWFQ1RXVTQkx1QldZUndtd1BLdE9Xc0JJTTh2U2F2Sm04WjM5QzZoM0ZUbi9DVHR4bE8rd0xGUldKR1Zpem1aTGdGbnJZaHRWTHBPWlNCbE43YkdaK1VWVmFhM3VRZTUxVTcveFhFYThIbVlVR3pUZVgyRW84dHFzWDVra051UkV5ZC9weWJ3WFg4Sm55UGtVeHAraCsvUFJlMlVRV2I1dWRsbG40QWxlanZFS1pDRmdnc3lsRUdhZWNzTUp5ZEhiQWlaOUp4eVVVVUI3RUxJNTVQdGlIYVRqc2dLNnN6OGZzSVpQb2MwVlNURTJLRjJMdlRLdGRNOFNsZGtzTERPdUJWdlJqeXJCL01SMU5INWpQMk1hcGcvMXE0b21zbStzZ2RyUXJ1OWR6cHFxODI1Z3NYamx0akpWd2M5K2RBRTJsWkgvbkprUVRtcTNZcUY5ZUtuQy85UExXaDVrcXZycHBvSEs4T205R2d1dlhhNzl6YkhQU0d6U1Boc0lzOVl0eDNzcjhvU29UQ1BpRmxDY0FHY3M1WGFkWXVVRU50RkR0N2xBU21QUnlKRGZhbmNDR1U1OGwxN1pHMlQ3NVN6WkpOSDVXbDQ2Q0NKREx5QnR3TS84V3B2MkxNeVZ6SmJ5bmFvSElUSmlhUGFLanJwbktESWlkcDBlVGlub0xHcFl6MTlqYXFrVFgwQkxzTXdqS3pORCtkOHl5T0U4VzIrSVZOL2hSa1c5eW5QUWpmTStxd3NhMnpldkk3VS9icCtOZElZeDhrOVVwWGNEVk8zWGNCSG5RQkRSNG5vamphMmRLVTBkaERNZUFVd3ZjUHhRRjZRTzNLSG5vVjE1NlRiOHVHb3d3aVN3TkIrOE5FRkZUc3NkWmh6QlNBbkpONzRLRzltNkhUUVIyYUJZUGx2VThMOTBrZisrWkJvUFcwY25Kdk1kNzVJU0VoSjRRZWd3Q1QrWERRTllvQ0RvTkZudkFPQmlmLzlFOE4xY09yM1hDb0xoMjZJby91Q0JNM1ZXR056VkE2WmJzV1lYdzh0Y2FZeVRaR1VQVm01SGNvVWxwRkdPd01oc2JBUkRrWm1MK0JCRlV3ekFZcDV2eTFBS0hoYlI5NEVabDFqVnBtOEZ2bTNzdFZjRmpMK1ZGQlpkT2l2eHU4R3c5WXJaVEtoTWpKb1BlTWwyeDFYU3RscGhOeCtsRzV2N1RyL0F5MDVCNU96V2hESFNaK29QQ1lxWiswc0hmZXJYays5Q1pnYnJ5NTNkbVRNaERPd1ZXQk9CcU5jT1BuRDYvMTBRaklRUFRYOFFoYWhPcUlwRzI4QkVlbHdhYWtQNGhUSVhtOGlWYS9tb3IwV0dOV2thOW5qKzYzYWNRL1JGalQzUDNjQlFyUEl1VjBoRFBQQ1VMOFBHMG1CTkNRTWZrRVlHRnFhOHJWZUFuUk0xbndWN253UmFSZldRTDQ1U0R4OFdjMzQ5QXh3SWN3L1IvRFpHL2x6TDFTT1hzWllKK1FId2tLRTNxSmE5emJ5UFBWR0dzdUtFcFJwQnFqd3l3M0JqRnY5ZjFhWWdWT1RVdVBRUXRheTBUWmg4ZUN1ME1nQ2tBL1Q4MGNKMXQxNkgzUXZ0cjRjYUZxZ1l5VHdsQXgrMkY5cFhmRXFXays2ZFdVeWxkOUVuanJjOFphQjZrN01vL0J5ZUdJR2FJMXNvdGhoOEhNdUFkRTJpSTI4WFF0bnhna0U5UWp4WmpobnBjS3g0MmkzTTlvNUFOQTdPOGpYdmpXT3VvV2ZCN0d2VHM1bjE5VzgwOU1HOGsvb3RhYURLdkk5QkJybE1DNENQTXFhVEtYTXA3aVZNd3VMZFE4eU9wNTlDRi9lMVZTeGFqSlBOVDZXdzNSYkNiMUNQWFRLOXZ2OFpMcXI5TzBlUCttV0RqRWZnL1BGdUdQd25pNGhlZE11SStyS1NLVGNTa3VjTTR4dGx4eGYyYWQ5aVlxWEtBVzhsMTB4YW9Wd0FtZyttbDdOTGVXdmpRVFNuN0hDeEdmQlRzWHNKZHhRRTJaMzVtNEh0NHBpLy9VKzdSaXJqeEhocG1zbHU0aVpBK0piaW9TR0ZNSWRSR3UxQ2xTSGRYV3cwWTFDN1lmekxLdis1ejNwN29qQ3pGaUM3UFJnUWtIa1ZaSkg3WGJQVTloTldqNmh6S2FZWFpnL3hxQUhob2QvcE5XSS9ibmxZdWVmWGhmY1lWckdUaUx2REVUZGZZazR3MzBsOWl6RzBZSjcwUENWOUZKY0g2WENKMU9teTNFUm1XdHdUVTBXMXlOVmJ0bk0rRVhnSXVMc0NqSU9Ha2pwUHpXeU1ML2ZxTzB0VllqaDJtNDdjc2ZOcVhJUHJpaERhK3AyTVUyZ1E3UVBGRlhMSXdvZFh2QWR1NHBpV1VFVnlUTHh4RVRCL0YzcURWWno4V1V5cUJDR1NBNWRjQUFhVlJIVkkrMVEzMXFIRVU5cC9xc2V4cTNsTUNGZytEUTM2WE0zSlppUWw5RE9IU3VkczJBazRwY0lDSXplTmVHcW82dGFDb2h1cXJ0d1dIajZzU25Id1IrUDBGYVk2NnhLMk9VM1BxbUxoUUZiTG04YWFpOHdNaW1jYXpGTGpxTWlXa2N0VEpHS1VIOGY4bmpaVTdVaVVnU3BSOTh6c3B5dnpDc1EyeG96cjRPNGs0bS9Ya0kzQmEwb21hQjFCSzdVbi84TXJZQm9Cd1RpSWtmTEs4N2pPTHp6MUltZytvMW1UYmtxQi92emVLeTJoaTZrZU1nZlh4eE9ka1o5MjBUTnlhY3JaSk9HNzZvUkFOL3VjVnlST3FUL0Fsam96NXY0ZWtROEF2SGt1MWlHM3luY2tkbW1QR0xZSXJDaHVBdEdBdUxGdXJvZ2NLc2RMNUxlV0lZYmVJWjJBVHM1TENndEdmdFJYMFlNZEE0TmdFVGY0RnlNanN4SWlWRnE0QlZyMXFuVzU3aTlwUDBiK2p1QnlkZjJQZGIyR3hHcG1DRWgzZ0RrUTVQUXpMMGFJdjNMTEx3T1Y0RjJmTDVBdzRRTFkwMFFBMzVQLzVPUnlkcU82bzRhV3JXck1pMDhBZExRWE5GOEtMeGFYSEQrb0FMWXdUUFpUY0dSWnBoay96SnZ6eVZ5OGxzWmtXU1RVeCtOU2kvRERMRDZtTE9wYkkybmNjN2Z6T2M3cjZCcGFsUnBEblFkYU9zR0VCSm9uVlpYenBrR2Y4QjNYb1JidW5iNlhsUmRRc3NnV1ZWWmh2bVJsdyttNmV6YlBIWEdGSHY4MTUvdXlIS2tqM0FLelp6Y3BjUHExWnE4MmcvbzVIR3ZLczJ5Y1ZEemtBSHd3ZG5XZ04xSUJGMTI1bGFUT3hLVnhvUHdzNHozUUJUNU5xYWxkRnZ5Wi9UZEtoY1BmeUd3ZjRJeVIyZDl6N1hmbUtQbjluZ0pZN2NON20za2RWcFJMT3BlZW9saG9xd3BpYTZRWUtiWXJqV2ZWbmlLWEt4MnMvYUpWQzQyYmJXRGM5aGs2Q3dwUUljMGxUbWJKK09RVkdKTUg4a2tHcVN2Y3ZIZVcvQnh2OGh3WGJ5V1ZWRnhNNlpJWDBhRlNERk9jVXplWW54NXRlWFVSOGRHVWI0ZGdYLzBSSDlKWUcycmtsYkFoOExVcjQyZnd1NDhIU1NwSVIzRDFGNDVMak41dGlyeEpUdHcyNGFYSEhWaXMxY2c0NTB0Qm9URWFBdmJ3NDRCMUhBSC9WdXoyYVRzT2hweTJkeHM2eFpaR29wd0ZvYitacFFva0xYcml0NndnNWpiSVB3Z1M3NklCQWF1TWszRFJFcWUzdDkzajAvR0RkdHFHWU1TcGZFL2VwR0thMDdsZWFQcmh5UFVHVTkwNTdacW1nbktrcEkrcEdPYkpSRXorTms0YlJieHo5L3lqbnY0aXdYQk51cExwNW8xVG80bDVUTzI4WEZ5aDNMY3pyUTBkbUZMVHVZNGRhVHIwWmUyMWFpOXhZSlUraG1WQi9qWWdTUTVFWmxqUHB6UDBsN1ArOVBHdU0yZy9HZ2lROEttU0YwWURnYjFFY3cxOFFDdldGRDhoNUU5RVdSVUFhSkpISC9rd0h3SkVSQm1HMDM3R0FCZXFOYk0vdktBNVZRQTNObzdIM05uaDYvK0ZjcU5WVG1GYU1KcThHNXhLd2kvNTVKa2x4K0NUaEp2MXpLODZXeWhlYWY3V295UzF4dlBEQzJuRWFzK0JXSDJlWVMzWUpJNFFqZVRZZ29wUnVJUFlhWW51SHNzaWpvZ1NTREtOeTdQbFBFQ2NLcWtUYzNuZStSK1ltb2IzUXlHNGhkZ29XTlhMU0lSbUh1MWJSK3NiM3REL2lRSlJvV3QvUm8wS09hNXlheWJNcUV1ZmVzOTdpMjlpT2trVEVtRnFSRFFmMnltKzNtd2pyeklIQUFDa2oyMlJmQmxjWVgyL0UrMlZkY0lJdG1HcVowQy9heVVNcHl1NlQvOE9DM09IZU5aczBLMEpSc09vNWVjd2dTVWdXK2tVR0s4QW1VeHpZVlhoMVNmZlkvOXdHVTlTNWZKMEZUUG1IcjhZRnlqUisrS05ERmtYNTlhWTFrZnN2RTVWbk1KSDVValJCUXZNRGNraVhlbndsbCtwb0thdE1XUTlMZE9oVHI4bXdrbUJXUDFMcnNNTGNjSkRxbFhud2pucDFycy9IOEpTSk9QRFh3eGIzSXJiQktOSmRIQXN4WWpkaW9TY1dTUGRHbkRjdnI1Mk5jeTNQVzJHcUdSQ21FbFREU1YvRHl1RkJQRXdwRk1hNElUUGhyeU1FSmphT1dSd3AxYXBPcE1pVCtMMm9vUTdNRUlMODBuV29wOWFBUGxiTytlYzVWTXdoZVQ1UHRRaHljRlpvTm53Zm51eWF0MTIzYWhBcnJoeTRjcExBaHBMcDlVOTZteUtSaFQyMFc2WDdpaENvaFg1S0lNZ2cxVFdVQUszR29zSk4rekRjQk5mUWNoV3J0YWdRRUNIcU45cTk4UTFNZVBwNmRYaklXODFsSDhobTVyMWVpcEFjQVdRUWxmNVlrN1Uxck0xclVkVTBZMDJ5aFNqeXUzZ0xzY3d4Z0xIZ3RpWUloTzIrYzRRWUtjTm41aXZjWHhTM21vQzZjZkNsa0lEWlQvclNUakxSNUtMY2VVWC9qTHRFZWVUWkpHOEJvNUNsbzFDa29HcHVYdGZTY0RsNXJSQjA0bHpSTDA3OXNFeWU0Y2h5MUI0dnRyRzdUS1hqRDkwMHd3RXo4dlV4ZytpS3dEUzhsVnhUQTJCY0JqQUpSZzU1UXUyNnAyM0VRQTZoUHJhaFYrV1ZRUFdnM0JYU1Zacms4U2p0dEdSeHNIZVhVUnFFajFaL3A4anZFN284Wm1kaUIveUluNDg0OHpTcVBmRGx2cEh6cFl2d0NEWmpxOEZkTCtIRUQ0YVR6YWU4ZmtUcENkdDVhZFlyYkV6NTNuNVZ4SmxHcHpqa0h1ZjREenF3QXF3RXlqcUN3VHl1ZWs0ZFV5YjNCcE5uM3krMHpXRTlDTXlScU01RVFMYWlPNEs2Z1VDclFSU0lLMHZnM1IyK2dVUXVyRFNwd3ZRQUx2RHBVOEJ1Y3dHQ1N0YzBpOWxsZC82a3RRNlU1QjkwcDhoc1hyN1JpYmc1VHlZWXQ3ZmFSdmZkSlNSYU95VGxWYVo4Q0F6NHJ5dUxGeklGTHNVZkpCU3pycHcrMjYxTmtVcE9QSW80bFBNbHZYL0pLcFB0ZzFHWThDdjRteXAyUVVJVkNrVHMzZHVzZWc4TUNJZ0s5YTFJNXN1cmRHc1FuOUFuMzJJRkNsRkU5ZThMZG5tenBOamczQmVBbTNnZHcxdldsd0JtU2dmbFVxMnZ5NFQzN0pISWdnTzJKQjlJL2M3TzI3bFZidXQ1NFprKzI4Z2ZzbnZVaGtESGZ5a2RXczh4VGwwK2NXMlRPb3RtZGlEakNNankvQlZiVmQzU1paYlFhUlFKTVM5OGxxZk9KeFpDREw3NUZCdlhjc200SkExNFZ2Qkx1QjNpRU9aNjc0UjVDQmY0dkFpQklnbHNTTzMvWEtRbjAxbzJGd1pOOElpNk5pdjZqSTl5c3RWL3YxVzBUV295NGZaLzVuTE9IWGN4NG1lQnF0Y0NDTmQ4Tk1LbHR5b0hqTW12V3A5c2N4RXV1U3B4NW1LNTFyQVVlR3FkQmQ1UWxRZUs1S2hRdndDR1VMbHk4V2p5V0VlWEZycGYwUEdmWWdTeEp5ZnV3aGlPK0NxNkVkUWwzZ2l2dEpkTytrZThWeEhIaHgzcmZqblZQWGRkYmdRV1VQNXpGMGJUbnE4YjY5amdleXNnTmJKTm1UYXllNmlWbWdrVnM4YkxJbXhUelBKUkh3RytzdFNOMGhHNVNIcTlBWnBQUGo1b3U2Q01reXZQcXlxODlDRXJqQnhwdEc5ZWNyd1RXb3RxZTJEZ1JhSTFEaXhNMHBZVjgxaERnSVVsendhVFVjaTFWQ0lMM3hQYkdnWklmZDl2Ry9NMlhBekVRd0xKVXRpSm9DUkg5T0VsWm03MUhaY3pkYW5lQVVqaFNWV0t4UXFkZThqYk9WaE0yaVY5WWdjR3FSelJWRGVPL3RoNFA0Z0VxbElsMWJBN0dyY2FLeWliSlp6d0lLb0U4S3BtTjBPT2Y5Q3NNd3JSRFhMOHFZR3U3bGxtRlZLZ2NJRW5nMHZpL0FxcU9zZHhLanVZUG9McUF0ZEJNQm1GSlZ4WGtUTFVnN2tzY0RyNXhXYm9ubHZmVmk3eFBTMGFqYmlHT1lzb2VrSSttNm5oT3h3L2JwTGVMTlFOSWhkNy9HandRRjM3akJzSzd2eDFhdkVpWlZnZHlwcUozZEdjN25kYVZqZVZTQ3JYQ2pjNEhpa0JWc0RvKzNoS3Y5RkROV1Q2YUllS0kwSThSaEJoSWNtNS9XSFV6VjlndEpmcm5jamR4RlBCTkpRQzcvUlY0OXlHOTVST0h5LzYwcWZPMjNHTVJrQS9rSkRyQ2NEMy9VM3dkUjQwMWkxUVhUSkVUVlFHNFFUbE9TMXhpZlpRNExWKzRYRVRJWkpIOEs5K0Rta09aRHViU1VZbUdVc3VnanltcnYxRUJLandSbkw1dUdWRUdOMkhHMzFnc3AxYlRjaE1JZ3RBdHFBWjJMSDJjOUZOemFYRTJHbTg5WFMyMUpsTlg5ckp6N3AwenZoaG1abGsvblhhZHFtd1AzcmpEaEdyTzVmWkcyazQ0c00wYjJkLytSS2psRDdvaEYyd2hwMFEyclcwdVNCL1hNMnNnT1pQYkpvaTFldlpjRjBLZlRUbWJqcStWaXFMMWxHT0x0d3RpS0RvcTB4Wm9FN0t4RHFwM1ZuZ3F2OW5YVmVmKzVBd2lCUHlVeHRwRGltbTFYc3VHY2l5T1BtZGhyM3MxV0VxalAwNnRUOEs1eFp0dTJHc3FiM2ZVN3R3UTJ6LzFwZloxSm9kSnVNNWpZKzZBbTBhTWpTZ3J4LzlNOHlVMnpRL0wwOGhOSlVNNHZEZUFhZFJHaXZUL1pSMnhUL2EyYTlJL3VNaGh3bStuZVNLMUpMV2NVR0EyclQ2WU9BR0F3TjN1R1p6TFJRbnVNTHM2UXlldTVWa1FMQW45aDd5Q3J5bzRYc09oMithSlEwTjJpOHBNcW9Edm9wU2RsZFZrWDl1M0QwSWxka2Npc2hFbDNCZTlQTjdDQ2p3NWJVcklndzNkbS9uaXl6dU9MSllRSjliaDdlWit4bUUwMUZSVVNVLzNDOUsvMlFpTWRaWGpwR1JBZkh2dFdJS2JHY2dQamRDQXVPT1R5Nk1RalF4UklvNS9BKzY1Y3ZIT2FyQ0loOWUxd2VuZ2VwamlZWDNBUUpmVHNRNmhVdlU0MHlIY2ZRMWdoTUxCNStPQWljODZ0b3g1dEJ1aWlLRmc0K0E4c3dMM2MySGo2bDN0MGRLLzFOQy9lUnZ4NXJGYkY4MHZUSk9oNy9CaUxpMUlZeHlNb0UzNHRnNXlnVDhZV25yZjFIL1VRaTRQZW91M0IxWElFN2p5WUQ4dndNaEE2Yi9IQTUxbjhyNHNYVzFrWENnYzFPVndaK3FGcm8vTlM1elJZaXI5UG5EN3NTdkcxdnZwTjF1YXFVdjY1NEhSRjZ0YitpL1hQeWxNZGZzMnBwdWliMllBQmI3dkg3MzVDRjNJaUppVU9Db3N4a0R6Q2JoRDhXYjBJT3MrdkVzcEZlZGg2d0IzYldhcDNDeXl5OXRodURMSWFwODRINkNlVEQ4d1o0SGhzZkdxVFVTdk4rdVd4MTZTRWZYNEJhV05FZDIwWERRM1lxMmkwL0ZIZEJMT2NzUGF6azVRUklpTUkrQ0ZFY1FTQitVYStiaGVnbUNrWlM1OE9TT0daeGZuOGFFWG9lQ0kzajRnbGQrNWlXTlpJYitXTGJlSUVpOXFtUU0rVUZWZHFlem4zbUNEajJBUWNQMXFvVUhNYldJYVhMUVc3a2VLYlZqaWtEZ0JuczhoR3Fidy9paVZYZ3p0VnJXR3dwRHVRMUhnNnp6bi9FbzIvRVlSZ21HeDJ0NUxtc1d3QzhaUDZmUXdacXNSK2FzT2JMQXp2OWYrS2lLREJ5NmlRakFQN1JmdXlhUUd3WENRSnpkV0MvanZiTkI0TGxjNW13WEp5OVlaRUtxeUZyN0hxbVk3c21xb3NWeW9jN2ViZEh6Tmtjc1Z3SUxFVUxzOTQxMlJoUkFWSVA5MEpBU3hVcFh4L05pYjFiSENyWVZsQWxWVVhaeGdwenVZLzNhWm1XYU5DcFM5bGFQcUIwdmRyOWhVRk5ob24zbVZGTGdtVU9MT3N5aGdkd0dGQks0NzhkSlErbHdNbFFJMVUxaWplcXZMNEV4NGNGSWNuR1Y3eTJGZ0pMVUxjeEFjZ1l4U2srdXArUjkyc2hkOXhWSzNWWVNobDNxdGtXaHRjNTcrbUNQZzEycTlQWnRFOWYxS28yUFcrSGJaSWhDYWZkTXg0UzFleGlPd0pXSnV6RmhNdCtrcHFzci9rSUNMelVkUUZ3TzE4YVB4UnZXRmV1REs5WkM4b25ZeDlBdzRGR3IxaSt0REdBdzUxeWVBMjZ4b2lPVHkrb1RLVkFKenR3ZW50WTZIZmN5d3o4RUtKM3ZOTm56anltOHd4OTVTZFRIOWtMYjdiWUkwTm45bkUwdzFFaDRINzIvOEp3UllnakhBVFpvK2Q5WUpFcTliMVk2bG42Z2ZPamlDekQ1KzcyZDlDRHA2dFF4Mk9oMmhCU0FRUEgxcjVsSmhKUXRhTVc0bFFVcTZZNnI3M3RXSU9lV0trQUFOenpURWRBWWp0MW5rQ2RqQzRaRjZEQUpMaUJUNjFBZGdqdWZPYi96dUlUUUYvSE45SUlra2Y5RUdWb1BvN1BPdzduOWk1N3JLeWpRWnVXZi9VZitIVVNuaW1IR2kxY2d5Y1V3QUVOOUwweWo3SHlOUHpHTHNTMjl5WkE3a3hwamFtb09NN2lYQnpyd1l6VDFKT2NJejIvOVVkbENpZ3ZpM29hYmxsZVlCcFFCZmN2UWpreTl6c3R0WTY2cE1FZkp3cjI1YnozS0NSTjJ0elZzVE82eVVJNlR4MjkxZ0dZMWlEM1h5UnhJSHdYSW5rU2NhdXBRNUVYdDZURHRWa29hcTJmOUVJMmQrUUkraUMrWmd4OGkwb0pJZmFHUHVMRjMrZnV3cTR0WnJVamFlekJGUFMvbHd1MHhIK09sS3QrdmlGeHdCTVphYUdhNkk2NkkyaW9ZRkhWVzdFZzVCdzFQNGJaanlDMyswaTVzNjFtVENBMmxUWlorMEJ1MVVaSHF4WVlWVnFiQ2FyRjgxaFk5MlU0VEpyRCtHTDBTT3pQbHVUdXlyVkpmMVFQaGF6VWFWOUxjbUxXNUgrbmRMcWNaQm1Bd1A2bXdRb3F2SVNZdU9EbUM1b3IwbDl2ckZ6U0JmS2VsL0tiRXdtNGpqL2QvLzNnbkt0S3p6eVI3Wm5GTzlhQ0kyeTdJOGg3bUlTRW9SQUppc0lTSy9Sd2w2S3RDN09xcVNRc1ZDWFdDWlN4N01nWldGSUNpTHFKaEl4dEFlNVN2N0I2Ri9zVHZpUkRzL0hDSk12QkR0K3c4QVFrNFZ3c2Z6U0FCZnFZdnNpUHUxTWIxYVprSDRKQUJ1VEd0U05IQVQ3TGNmU0ZrR3ZpaGsxSVU4dkIrRUFrVXBhdE9IeXFiS09sTFhEYW1zeWMrcjV6d1Q5WHlKRjVKcVFGRE9tTFhnNW1NVVQ1SUxYUG5iMTdERHk3RmFjanR3aTFCUXRkRVZ1K1ZXZWFpaGUzTHRnOGRMOUpPL2pmaG4rMjRQaGxvMFlxZXdtcWhITVhWNE9CTWZtbXZXWnYyRVU4SlNGbjVmVGdsSk5CM2x5Zjl0RXNONmVjQU93bHlFVkFvVDdhOEJ4RlN0MS9XMlBqUVN0MU1PVE11alZxRk0zcW8rZUcwalh2T0xqM2cvMU9GZzJ2NW5veHluRkp5RStpVXZCNUVoNmhsbGJSOTRZYkJqa2JLVFpKN0FGemswazQ0RGxtNVA1RERsRVVaYk5mdGVHYmxyWStaOVZTMFdrNlF5SmlPb2dvekcvU05lVVNFTlY1V1Byb2VtSzBqSkM2RGVBNUgvcFluMUZCdWRDamxuNkliTlJXV3p6TDBTQTN0aFdvV2crcldOWDJkd1BsZzNUK1htTzQ0bTAvbXRLUHIzcmh1a0phM3h6Nng4aEhyaUlRWEdWY1pOSHJwM3E5bS85OXE1Vkt5dlg0OFVjUW1LT3J2VWs1QjExVzdzcEUrTWE4TFM0eE1ROXJMWkRLeG1XSFFsVWtxanpaTTVRVDBxb1JoUFNoNVFrY3NsN28rSkZ4c1kvVGVWaDFLcld1Nm5SZko0QVhGZUhkZHZ2U3lRRFV6YVdFazR2VWxhNTlZcGk1UWdYSDh5ZEp3dEt0VEs5T3V4RjdnR1V0KzFGRDNBVjMzRmtyTU9NckNVeHRtaEFZbjIvWXBFVktibXNOaGFidkswSkE0NEVxa3M0VUNRTUVJdlpYNzJYQ0tVNVY2RnRvOTNlenV4QzNHMkprMG1Yalp6cytNeVFlamVRWlMzQ09pMTh2WnhpcDh1dUc5akV5SlJ0YTk3TlRyZ2dIVDdhRTB0VmFzVndrNzdFekJGZDdZZ29LcmpuNWR5UWxrVGltN1E0Y0dUSjVsMm1BcW1KQnFRSTBraDIyWGpOT2p4UmhHcUJjdzV3VkFqcllkLzNYVis1NHFmWm5XemE4STdxd2J6QUt5ektnNExlZWdoeHphMXlsanJVN3oyYTRXL0JsUkpOTVJKdnpzbHNyUXZRYUJ5VG5ySEpZK2EzZHNuUE9CdXk3NEJWSGVZa01mdGJ0dER6VTZreDlKRVpnSzdhNU5Qa0l4WFJuN2xDczhuZE5VVzk3V2NSWTdYQ0hPOVlpcDdlSE9rTXlidFBQNUNBWFpnRllPMmJla29SNENnTnprWklxaGI1cHB5VWVCTEFjYzc1YnlDWjZkUkpyZnB1K3NTejN1OUNNSnpPeVJxNjJFYXVPZ0owRUtCcC9hRml0QnRpeTl6SWRTK0FEMjhHL2RQWENadGUzTWd5WHBSOW1YRjNmTHhtb05lOWQ3QnZOczRFYzUzakZPdng5RFVEK2crVWpXemh5YW9vK2NPbEJQK3NWaGRjcDRNeGl2clBYKzZhYjNWbUZNY1d6K3FnOEY5VzVhalN3WjIrVWlSRDluT0lzQTJBUFUzSUNoVW1XMm1VVHBna1I0aUZFK01yZElmRzRucDZIV1NuZTdtdE9UbUM5VzliMFNtMDM1MzlmZFVVUS9mT3FtMHRDWWR0c1crR0JzY3JOTjg3WXY1SVlLR0tEOUcyVTY5WHNUd2FzKzFEeTArVnhNN0JxYll6MVNGbHZSeThPWUlLYklqdmxGb3U2eFdMakRNM0tBTldReDErekpGbmFyV0Q2a2hpM1RtcXNrbUl6cWpBcG9CMmpyZndXNWg2YjZIU1VkSEFUZ3o4bGV3eDArT0s5YkMwVy82SkVkempqTHMrb0NDK0tObjlBT0hDSjhtTjVpQi9QM2EwOStpNTNlMGd3ZWRmRHQwMEJaUW5qY3pHZ2UyNEVLUjVRejZHcnV3ZC9Da2ZZOE5SVlhJTXpLTk93WGNZL2JBZ2tCTDJHTDA3dGVJRnd3Y1BMejZuaGVVYXBSNFhxeEVlbEoyRFhIQWZzOFJKM2lRNFFXVTY5Q2RBWlhINmFYajdlakhyNWpKcUxOeXdDUTRvWUV4cXBzVTdtdUtraU5POG1hRnVjKy8wN2x5TXdGdDk4N0VUbXh2dXd2YjhQT3FiVFQ2a3diM3hGaTVIa0lPSXBOQUc4Q056R1h0ODFVMnJBcmQ3cmR0UUtNZTVVUUJBOXJqeVJxeFh0eGNtMWJSZnNrckFFN0cyekljczBwNDUrZVRxYzA4MWlqRHZzL1pGSVNWdm5RNkJBUFdoWFQ0ZkJhNkM0VjB1Y2RYY3YwbGlGQmxlc01WcVJRYWhFTXIrbkVBL3UzM0ZkYVc4dFF3cTRra2xEQmxhQTYrbXF2Uy9VczZ2MVZxMmRwbTQ2YXRVbUpMOG5Yei9Oc0tVYmdrR0Z0c3RDeWthbExidlI5cjd6eE8vSkNhZWZmU1QybjUxem5zQVdHVTFIdmtTcndwK1FWU05tRGlvVGE0NERDREJsQklRdThKZS9tZWliSjVsSWYyNERmSkE2RDN6S2dQbnViQ3RpK0E1RDdlbUxCTjZ4WjkyV0o5ZnoxcE1Bc2h5eGl4MlZDS0JIWkJ4bXMramtzUEdWTW94akhZenRWanAvVXpLanA2Y1hDYUZXTndBSjhON2gyR1VBd2lrak5WVERLSEk0Ym1wQWRvZ0NmNlRCWkwrWDIxaGF2aVMrSGNJa2E1S2dmZEl2QTNJdHVZSkVoM3dGMnVVUmszcmN3azMrOHJuMG51QlBZVitoOVhmMGoxSEdTcDN5NUZITGFUOGdiZHQwMUpjOTQxS2pBU0xUZUpEdjlaU2FPMUZLNDhuLzM3WVgvQVg5NnF3SDdOd2RJUjJQRisxSlE0VDRpbGJxNTcxTkN3b2pXdUVqU0wxbEZFNzVQY3luWjU0Ym9NSFNPbDJQaWNjMElPRUQvSnZGMGFBaDN4TmI3dVE3dFhrMUhJaGlFTmJBZC9nT3UzVGZ0MkJIbkFKS05BWXpwMmZwZmVMelVOWllJMVdHT01xbzlqSkFGbk55VEQ3MXJkMkI2V2dnVE11d3Y3ekxwWElwZHRRYzcrQUZDUnNiR003NFlZZGVnUW5NcjVTRjl4dzcrNGptbVY5T0xzUTc2VURoaG8yWTdra2tjMkYwdXZhT2xjZHY5MFJGTTBURytTOUdkaDBud0thOGdkb0RteGI1NldLSlkyZmZoNWdobnNFVTlIcTZhZjdZekVneTFMUmpkKzAvTWJWNElGYlJDNDRkZWNiK1RyeEY0ZmpwYzVLZGRSTDg4K2VsMDJ1UWgwSFlVRzB4Y2ZmNGgySzQ0K2gxTmhPbmNXMDhjLzh6aDFLSlhxZS82Y1I0dnBXU1FSSmJLNTk4Q2lnOW94S1VmK2pVUjh2eWVBYVQvR1FCekJEc2pxVlMwWURWbDVMQnV3NjR4YzBJZ0FMTDlpK0owWkQzVlA1RnR4aDZxZzRacm9Qa0RBWUVKTXlBYWI5alI0NnFiWi9hamhWcnZsS1VFMC9rWjBRSGZMZTlJZWFOWkNHdktFNE9veEp3bGxRY2hJNGxTZXpURGw5MVVsZFZiMlFFcVV0Z1FRV0dZK2RxelhOaVFuUmxiRjdYeVVmck5paGxmUnk2Q1ZTNXlEd1F3Z091UlNsM0hHTGN6dFFWWERLYm5RN1RRc0htU1FaUW9seVZjVEJUdEpXNDYzZ1pTMlpzUS95TWV0anQ3V2Jib0JGaVVQRjgxdS9ISDdwckFLdUtxWGs3NVM4MUdPM203TkNrOWFPKy9sODE4NEN0aDVtaG80ZW83UXpZeGZRSTNPcFJkaFRqV0FiMzlzNlBSTkVFb2hvKytYL24zRDZaN01TWXU4YW1uL0JPWm5LWlZIR1VuTFN2MUtRQkhvL3BOZDV0dnBBU0tWL2ZteFgza3BVd2prM2prZUM5N0ovUU9mNit5S0JXTTJMeUoySkJvK1Z4Z2R2SnRtZnNvVU1vTTNCUVpnNEZuSzlsVmRlWnNYL0ZxMU5Zdk00UHhMV1NNVXp2OFB4NTI2NVNhQVpTK1d2eG9Rb2ZqV3NacWlmcGdyZnE1eHJUYi82OEltampyMU1BZU1URFZMSnQvLzhpMTZudUR2UHN0ZUhCelpXWjZGck5iLzBiTTNSQUdBRnNySXhyUVV1SDBJbE9WdlhFb0lpQzNMK1NQVDFzYWV4cTlUU045eUJSM1ozdDJ2NXhxS1FSaWx5OVY2a0JSSjd2aVM1b1JPZEFYUEdHbzg1WTJuWnJvb0xIWlFDT25tdERjR3JFQmZ3MDlSWHlYZVpydi9OOWtKUnpCOWhFNDZxK3J0a2R1TzNoblp2QVpTZjE4MkY0cjBWTXpJTUFYc3d2Uy80b3RKRHA4QnowS1ZmbkY3VTl1ZTlTeENtd2Q1b1p2MGpteEhIdTJRSHdVa2FuR1hnL0pwaVFTcUI2N3lsRDhiTWxLOWhlc3YxMnN3VkdKTmJ0V0tXdW8rL2Q4T0ZEMjh3eElYNFEvbm50NCsrK2dFQVpHaG52eWo5eG8rT1hIaGZQTy9pb2h2cjYwWE1ZQlhTWFJDbWFBK0xBV3o2NE5ncHFlNWhrRnNONW55YkRPMVRiVDhxK0x5d3RQTytqN0ZzWXpiMmF3bDlFMDFYd3BJT0hOcDJRRVpFY0lQYjAycVJ6WkJBU2N0VGh5UDNpdnRJd2twek1CSDFUK09mZ3VBNEdjZGhuc1dJNDhBbDFiSUhvK0pIUW1Lc2pBaEs1c3dWZFdJVG5XNmxiSWdTRFc3NUhQaFR2cG1lcGpHRHpjYjFiSHRGVUpCekkwMFhjRjVtMytIN1FKUUtteC9iMjZrVzA2WWdGMEJyT1pZNXNKRDlzTjRjb0E4Y0NSMTlvL0Y2MEJVU0xLK2lhOW1aY2pzdmpLTVp6YWNGaUlqSW04QmY5amxaN2JaSWZwb1RZQUlSZmVWSHAwWTZjRU4xbVRXZENFanl1bUMxWFp5WDM2ZmM2OExLQXJwc05Ib3k2L2p5Vkk1VzVQeTdyL2JhTGVnMnl6cXBCR1JESEtVaW5Ma082TmtyTk8zWmxSMEY5SkpaaXBQM21OamlneXBldmNKWFJ1K3l1ZjNyWU1McXJCYU5GOHA1L08waGhhVzU3dU9JSU1KVEJZWnJtUFJ6d3I3OWZyWllSSzlYN1g2L0NIWXRjV3NGN210ZDVxWGJoUlEzV25JQ29sU0hSWGdFZThseHczVXcyYVlqUWdRUDljbDdVZFRTV1FCUDVBR0YvRkdKT2JxT1MrbkdQQ0FING5PTzdjWDUvVER4NnQrMkswMmg2Qk9VWVI1b2c3MXc1OGd4bzErdm45dkc2QzB4YUI5dms5bDB5VklnYlVnVEZySlROZ2RLdlNsQUZXTmM3RC9mbjlIbUxUVm5RRlRrck51YWtwcVFNUW1haFZXZWhqd3luYU5vT1ZXT0daTTFRR1djaS92dTRCd0thWjQ4Z215bjdjMmxXUG1YOUxSUjNFa2gvaFNScEVIUHV4dk1tS2tNbTBSNTRmNmRJUmQ0b1dmaVFpbXZXSDhHa0JKMnJONnEydmY4anM5cDI3T0RhWXNlV1p3MzhmQ1NrZlgwTmxnT2R5LzAreW5CQ20va2RldExSNzJBd21DWDM3U1N5UmRIWis0YWJudElIREw5SC9FdU5NcmxKZmhabE9qc29GMENMRmh4K0pLN0k2aXd1RXRFVjNOWlpCRjd3RDl4bnlFY2FvenFjakY5RTVSR295UHMrZEJETUVmRmM0dXhaUjBYS0J5YzI2VmwwOEgxaFZJdmZVZmMzWGd0QkU1Q2R3cEtlYVYwZnkwUUlXNy95SkR0YWpMbm1TRE9kQTRHTGtSMTErNmlWeldSUFlLeGJiLzlseG4rMnpaQzBOK3FGeFdqeW5CdCtSMVR5cWdOY3RIeHkvU2JXanQ4Qmw1Q0tZSmJTR1BsWnR0QVhTbG9lcldyS0hLMHpHa0J5R1dndGJ2dUo0T1VDVG1yTzVxK1g5N09Od3d5ZlJTYnBWR0JKVTFyK3R2eWZ6MFRnNDNuaElWV1lwMnI3V0VFVUpHZlk0alBxRmllM0RhYThRVy9VL2xPUy9DLzNHYlcxZXg1ZzFhVG9BUytvN0pxeWVmZEMySkNHbi9zZnVVdjhYcmJmcVBRcGVZTjQ0YkhidjhyczhSVFlOaSsvKzZSUjFBK1p4UWlXYW05NGVsdCtzZ1FkNmMyYVQxRDF3MExmUW9OcEtPdGgycVllSy9VTzdoUFNsTmpjRDFIbUhKYS9wb2lFcldobHJyb2FraksxbVF2RGVlMVovRFArT1d5a3NtOWFuc2tLYUc0T0ZGY2JFT2VWV1Y2TFE2WGN2V04yaitqQ0N3eWo1Z2RqdDF1UUQveExHWUcvbmE1ekJkZERPMDVkQyszemNLRTZLbzJDYkFkY3RvZDcwYXM5ZXI1a3RsMjZFVEs3UmNVSEVGYUtVOHMvL1poQnI0RGkxcUJVTHptdFo1MWZjbW1oeXNseDE1NFk5QXZxVllYU0NpNXRjOVlnZlIwcXFjbHA2ckozZEc0Mm4wUnNBVFFzM3dDSUx1VVJ6Y2dDcXBQNGZYUGpWSW9JYkg4elRMUWlNWWpCZ0VwOXpPeWlPMFNLeTdmdWh2RE5LWE5HeG5Xak9DQWdMR3pXTi9GVkVYWmw1aFEvdXNmMjFuOTJJMk5hNzUyQkhZUDRma3MrUkI3QSthcms5K3BXaEk5bGUxU0RMNS9jMlZCOW1yMHMzd0ZGTVpVcWpld3o5b1EySFdEV2V0UTdiQmpHVEZOellad0ZmYUJGRjhDT2I3ditlbFlOZ1M0eTZ1TStrTjFMWm1lWVhueDcreUV4RjU1RXRhSlVHR1RORTBTZ3loaHVveDJTQlVGeFY2SzVRNkxxL09jN3k0MjR5TE5rNG9Gci91bmk2RGI4NTRBM3dzNm1CZE5LSXQxRGZCTzMvM212YlhYK04xSktLT3NBaVI2djlRc1hHbnhwNG1jZ1Y0c0FyY2VtNVZhMGdheWt2YUtPUHBtY2pCZUVKaUprdjJ3SzA0ZlhaMXZ4V3ZpUURmcllSWk9jQ051bUpxMjZCMUpUQ0k0eFVDNVhCVjZDeVNoNklGNFN4WW5YQ0tVMjFNZ1ErSG0zMC8yWm1pRXRoSG9HaEJNVUQxdThzRUtSSDgzZndpSkEvd3RobmNIUkVBdjhna0xpVGZaRWtjSDdwYTg5M1NhTVpvbkplNExjVDdEZGFsSUltSmsySUZGaXZnU0ZlNTBmRnppV2NpVWpUVnl6a0JyeUJxUVcvMkp4cHFYRkR2ZkF5VzIrT1RRUEdvajVWMTExcm1uU3NwTXBMVVNnK3cxUk4vT1lSc2Y4cWZ6b09waVkvUUlQQVlsRnI2NEZab0lsVm9zNXlxRWNmb3lmOFdMWkFzVnozd2F0YlI0ekxVYmw3REUvdVJJZEY4c29mYXdGRjYxS29MSXBxSHZ0aXhtVW9aaElOMDI1MDROUkFZVzhXd2p4Y2MzcU5jTmNlS2UzNzBWOUFvQysvOWRwb2VCZHYzZzRtK0N1QWVKcEFYZnBLSWxKdTI3Uk1QaHJLelQ2c0hDYnQ2Syt3M0tUa2YzdU45ZEJBeTdadE12eUs0MG5CU2lQRXZtdFprRW9HVHh5Z0JZMUUvUVlIV3E2cTBBTGc4R3hobXJvOXlJdWI0ZE1lMnNFd3FSY1RVWWdQcTR6ZjJadXRsd1VrUWZscVlhUE1DMGhmRkV1WnVYZjlxeWNZdVhXcEtBSCtIUWc3R3NMd2dLRWxFNTdmOU5SaGF2d2w1amc2RkR2UFpqQWk4MkNOSEFnL2NzQnl3dnZnZTM2dG9ybzAyMmNqbDkreE9heHluMUFoeStUTm8xdzM5S3BGVFpjYVhwd3h6Z2xlSXVlS2tuK29XVEdBeUNFazNTU2x6bTl5Ym1Cc0twTWlHVmxPYjdDU0VnVElDQmpDTFZBQnRNUEJqUmZVT09xRVdCalVPZTJCQVhtcWI5b1ZVSUZORGlHbkxRWUhnZTBCVWRhR2RPdFJ5dHhGSldGMEFQeVNvd1I0UUNYeWw5UG5tV3dCY3ltSW1xa1VCdnZ0bHN5U0RTSWZPelp2em5ST0hMelh6azFGekpxQUtwUEFSRG8zMEJ5M0pTS3VEL3FoQW9BUkRDNjJ5anhRUFFxZDNzajR3RVBsbHpvSitycTZidjY1UjhwM0RRdHk2a1FVRzRRVExvdHpJMktzdWNuQ0RRTU0rMnZwMjBhckpKbE1ldlEvVjRZKzJ1K3djRTBCN24vWEJwSUhmK01IM1BSYXNIbGFOM1d4VElBYW9qeDVPejdIZ2U0dDZCRWtHTUEzR0dGeUNsL0toMFE1MkQyVXUvNWxsSm8xbEx3Rkt6NHFGcVp1OGVXNEpncUhzdC9ac1hvckxERFBkaVMvRGJBSFRrMCsvaDVVWEdLQzc2YVhNVFkzU3ZxN2xYRUtYbThDU2VnSEttdzNXazE2Y2VoNnVhMEkvSXlxZFQ1U1Z3VUdhcnM2SHBNa0s1a3p6a1FFUzU4RWs5WjBZYlJSTWtoSkdNKzRnaXlrRnNuOWpPbHRGUFdLUGNTZ00wUTBUa1lTNTQ4UlliYzFGb3pBYlphQWN2ZjFBQisrOXEwRVljYmI3TVFqVkdQRG5SZ0tjZk1mTG81cTJEUDkvUVhjdWtuSmM0WXBYYUlCQkNmQThzOEMrbjA1cC95d3RBV3ZYYkl5cHo5cjVQVUhpenZnVkdqYnZIM0szbktXQXpkSXdsWVh5SDVEZzNzUmkyWm8rOS9hb0RnTWxaQ0ZzcjZFaFVhU2NiSFpJUEpFdHhBWEZCZlA1aWJBYlVjTkNIdEdMNGgyQkNPSVY2bzRKRTU2dE8yU1Y5K3NaRE9YNGlnNFd4emtzUGh0cG5LbUdZekdjZXdUQlR2UXp4ckl6NjB0QmgzNCs3ekkrd1YvOTE3aTdnVm1wK2NteDE5Y3plUVcyaTNROEZSTDVQOUloT1dJakx5Nm91VkVLUHR6YmtCMzJPdVZhYUJBeDNqOGlVZzRQWTdXd25USXJmZFMwV1l4d255cjJnQWFIbERqY0pHUXVDelNYMzJnTVZyRDhwSm42anpjWUhxRUJma1RFU3RSeFE3ckxYaFI0ckNFQkNOTzJteWR6Z25CRitpb2p4amkwd24rK3E1SlNWQmVFbHdvSkcwN1AwZk51RUFWUWdqbzhnR0dnSEl1MytoZ2xvUG1jTXhybWJZaTlKUGxPVmhZOE1HcXhyRWZ3QnlDYWErclhXOEhrZDRUU3EzR0pHTVlhUzRUYWtBeXVyaUpxSXhwS1h3cGJhMCtodzhqV25SRXJFdnV3V1hURmYrc0doSGtYSS90RXp0bFNaTGNGNEJBbDdtQzdVWVp0cW9iSWV3dktoM0dQanY4VXV4ZTRBY3lOS3AwWXdCdHh2VnA3NUV2K0xqL3lLcDd1enVWOVhJc0psMnBrbEhZYU1Jalh0M1hDZ3ZQVWVTNmRBdlIwK3JSNnhmeFBHeHVVQnJEMGF0SlVlQmJialJwRkhhSGZtclFIYW1qTCtOVTBOZkNLWkYyTXhqLzFxb1NTaSs5Ky9XZXVtUlhUUnJxNVErOXBWZ29TWVBiUlJhUTBnODhoc2REWGxuNTBQaW9iMTkvUUg0VFFyb3owemY1WG5qMFhuSVRjM0k0UHZtNnFyc21vOHRNS0Q2YWhqLzEyOCtmTHBwWjJTQTRKdDVjMzlSTWgwc1YveFd6c3NiYkpBd3U4OHRVZDRISi9hVUNWeDFjbFRZMU5hQUxhd2lpMXl0NUJRM3AwUXJ0MENDK1JxZUExQVNSNzh6RlI0MzJkOU5nUnhraVRtQUFBPSIgYWx0PSLlhJLng4/poqjkuq3jgonjgafjgpPjga7jg5XjgqHjg7PjgqLjg7zjg4giIGNsYXNzPSJyYWRlbi1hdmF0YXIiPjxkaXYgY2xhc3M9InJhZGVuLWJ1YmJsZSI+PGI+5LuK5pel44Gu44OL44Ol44O844K544CB5LiA57eS44Gr6KaL44KI44GG44Gt4pmlPC9iPjxwIGlkPSJyYWRlbk5ld3NTdGF0dXMiIGNsYXNzPSJtdXRlZCIgcm9sZT0ic3RhdHVzIj7jg4vjg6Xjg7zjgrnjgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKY8L3A+PGJ1dHRvbiBpZD0icmFkZW5OZXdzQnRuIiBjbGFzcz0ic21hbGxidG4gc2Vjb25kYXJ5IiBvbmNsaWNrPSJsb2FkUmFkZW5OZXdzKCkiPuODi+ODpeODvOOCueOCkuabtOaWsDwvYnV0dG9uPjwvZGl2PjwvZGl2Pgo8ZGl2IGlkPSJyYWRlbk5ld3NJdGVtcyI+PC9kaXY+PGRldGFpbHM+PHN1bW1hcnk+5L+d5pyJ5qCq44O75L+d5a2Y44OH44O844K/44Gu56K66KqN44Od44Kk44Oz44OIPC9zdW1tYXJ5PjxkaXYgaWQ9InJhZGVuQWR2aWNlIiBhcmlhLWxpdmU9InBvbGl0ZSI+PC9kaXY+PC9kZXRhaWxzPgo8cCBjbGFzcz0ibXV0ZWQiPuODi+ODpeODvOOCueOBr1JTU+OBruimi+WHuuOBl+ODu+mFjeS/oeamguimgeOCkuWPluW+l+OBl+OAgeipsemhjOOBq+W/nOOBmOOBn+eiuuiqjeODneOCpOODs+ODiOOCkuihqOekuuOBl+OBvuOBmeOAguiomOS6i+WFqOaWh+OCkuiqreOCgEFJ5YiG5p6Q44Gn44Gv44GC44KK44G+44Gb44KT44CC55S75YOP44Gv44OV44Kh44Oz44Ki44O844OI44CB44Kz44Oh44Oz44OI44Gv44Ki44OX44Oq54us6Ieq44Gu44KC44Gu44Gn44CB5pys5Lq644Gu55m66KiA44KE5YWs5byP44Gu5oqV6LOH5Yqp6KiA44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgo8L2Rpdj4KPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKbGV0IHJhZGVuTmV3c0J1c3k9ZmFsc2U7CmZ1bmN0aW9uIHJlYWRSYWRlbk5ld3MoKXt0cnl7cmV0dXJuIEpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfcmFkZW5fbmV3c192MScpfHwnbnVsbCcpfWNhdGNoKGUpe3JldHVybiBudWxsfX0KZnVuY3Rpb24gcmVuZGVyUmFkZW5OZXdzKGZhaWxlZD1mYWxzZSl7CiAgY29uc3QgYm94PSQoJ3JhZGVuTmV3c0l0ZW1zJyksc3RhdHVzPSQoJ3JhZGVuTmV3c1N0YXR1cycpO2lmKCFib3gpcmV0dXJuOwogIGNvbnN0IG5ld3M9cmVhZFJhZGVuTmV3cygpO2JveC5yZXBsYWNlQ2hpbGRyZW4oKTsKICBpZighbmV3cyl7c3RhdHVzLnRleHRDb250ZW50PWZhaWxlZD8n44OL44Ol44O844K55pyq5Y+W5b6X44CC5YaN5bqm5pu05paw44GX44Gm44Gt44CCJzon44OL44Ol44O844K544KS5Y+W5b6X44GX44Gm44GE44G+44GZ4oCmJztyZXR1cm47fQogIGNvbnN0IHZhbGlkPShuZXdzLml0ZW1zfHxbXSkuZmlsdGVyKGl0ZW09Pntjb25zdCBhZ2U9RGF0ZS5ub3coKS1EYXRlLnBhcnNlKGl0ZW0ucHVibGlzaGVkX2F0KTtyZXR1cm4gTnVtYmVyLmlzRmluaXRlKGFnZSkmJmFnZT49LTM2MDAwMDAmJmFnZTw9NzIqMzYwMDAwMH0pOwogIHN0YXR1cy50ZXh0Q29udGVudD0oZmFpbGVkPyflj5blvpflpLHmlZfjg7vkv53lrZjmuIjjgb/liIbjgpLooajnpLrjgIInOicnKSsn5Y+W5b6XICcrbmV3IERhdGUobmV3cy5mZXRjaGVkX2F0KS50b0xvY2FsZVN0cmluZygnamEtSlAnKSsnIC8g6YGO5Y67NzLmmYLplpPjga7phY3kv6HjgYvjgokgJyt2YWxpZC5sZW5ndGgrJ+S7tic7CiAgaWYoIXZhbGlkLmxlbmd0aCl7Y29uc3QgcD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdwJyk7cC50ZXh0Q29udGVudD0n5p2h5Lu244Gr5ZCI44GG5paw44GX44GE44OL44Ol44O844K544GM44GC44KK44G+44Gb44KT44CCJztib3guYXBwZW5kQ2hpbGQocCk7fQogIGZvcihjb25zdCBpdGVtIG9mIHZhbGlkKXsKICAgIGNvbnN0IGNhcmQ9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnZGl2Jyk7Y2FyZC5jbGFzc05hbWU9J3JhZGVuLW5ld3MtY2FyZCc7CiAgICBjb25zdCBhPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2EnKTthLnRleHRDb250ZW50PWl0ZW0udGl0bGU7CiAgICB0cnl7Y29uc3QgdXJsPW5ldyBVUkwoaXRlbS5saW5rKTtpZih1cmwucHJvdG9jb2whPT0naHR0cHM6J3x8dXJsLmhvc3RuYW1lIT09J25ld3MuZ29vZ2xlLmNvbScpY29udGludWU7YS5ocmVmPXVybC5ocmVmfWNhdGNoKGUpe2NvbnRpbnVlO30KICAgIGEudGFyZ2V0PSdfYmxhbmsnO2EucmVsPSdub29wZW5lciBub3JlZmVycmVyJztjYXJkLmFwcGVuZENoaWxkKGEpOwogICAgY29uc3QgbWV0YT1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdwJyk7bWV0YS5jbGFzc05hbWU9J211dGVkJzttZXRhLnRleHRDb250ZW50PWl0ZW0uc291cmNlKycgLyAnK25ldyBEYXRlKGl0ZW0ucHVibGlzaGVkX2F0KS50b0xvY2FsZVN0cmluZygnamEtSlAnKSsnIC8gJytpdGVtLnRvcGljO2NhcmQuYXBwZW5kQ2hpbGQobWV0YSk7CiAgICBjb25zdCBjb21tZW50PWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3AnKTtjb21tZW50LmNsYXNzTmFtZT0ncmFkZW4tbmV3cy1jb21tZW50Jztjb21tZW50LnRleHRDb250ZW50PWl0ZW0uY29tbWVudDtjYXJkLmFwcGVuZENoaWxkKGNvbW1lbnQpO2JveC5hcHBlbmRDaGlsZChjYXJkKTsKICB9Cn0KYXN5bmMgZnVuY3Rpb24gbG9hZFJhZGVuTmV3cygpewogIGlmKHJhZGVuTmV3c0J1c3kpcmV0dXJuO3JhZGVuTmV3c0J1c3k9dHJ1ZTtjb25zdCBidG49JCgncmFkZW5OZXdzQnRuJyk7YnRuLmRpc2FibGVkPXRydWU7CiAgJCgncmFkZW5OZXdzU3RhdHVzJykudGV4dENvbnRlbnQ9J+acgOaWsOOBrumFjeS/oeOCkueiuuiqjeS4reKApic7CiAgdHJ5ewogICAgY29uc3QgcmVzcG9uc2U9YXdhaXQgZmV0Y2goJy9hcGkvZnJlZS9uZXdzLWFkdmljZScse2NhY2hlOiduby1zdG9yZSd9KSxuZXdzPWF3YWl0IHJlc3BvbnNlLmpzb24oKTsKICAgIGlmKCFyZXNwb25zZS5va3x8bmV3cy5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcihuZXdzLnJlYXNvbnx8J+WPluW+l+OCqOODqeODvCcpOwogICAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oJ2ZyZWVfcmFkZW5fbmV3c192MScsSlNPTi5zdHJpbmdpZnkobmV3cykpO3JlbmRlclJhZGVuTmV3cygpOwogIH1jYXRjaChlKXtyZW5kZXJSYWRlbk5ld3ModHJ1ZSl9ZmluYWxseXtyYWRlbk5ld3NCdXN5PWZhbHNlO2J0bi5kaXNhYmxlZD1mYWxzZTt9Cn0KZnVuY3Rpb24gYWR2aWNlSmFwYW5EYXkobm93PW5ldyBEYXRlKCkpewogIHJldHVybiBuZXcgRGF0ZShub3cuZ2V0VGltZSgpKzkqMzYwMDAwMCkudG9JU09TdHJpbmcoKS5zbGljZSgwLDEwKTsKfQpmdW5jdGlvbiBidWlsZFJhZGVuQWR2aWNlKGhvbGRpbmdzLHdhdGNoZXMsbW92ZW1lbnQsbm93PW5ldyBEYXRlKCkpewogIGNvbnN0IGRheT1hZHZpY2VKYXBhbkRheShub3cpOwogIGNvbnN0IGFnZT1kYXRlPT57CiAgICBjb25zdCB0ZXh0PVN0cmluZyhkYXRlfHwnJykuc2xpY2UoMCwxMCk7aWYoIS9eXGR7NH0tXGR7Mn0tXGR7Mn0kLy50ZXN0KHRleHQpKXJldHVybiBudWxsOwogICAgY29uc3Qgc3RhbXA9RGF0ZS5wYXJzZSh0ZXh0KydUMDA6MDA6MDBaJyk7aWYoIU51bWJlci5pc0Zpbml0ZShzdGFtcCkpcmV0dXJuIG51bGw7CiAgICByZXR1cm4gTWF0aC5mbG9vcigoRGF0ZS5wYXJzZShkYXkrJ1QwMDowMDowMFonKS1zdGFtcCkvODY0MDAwMDApOwogIH07CiAgY29uc3QgZnJlc2g9ZGF0ZT0+e2NvbnN0IG49YWdlKGRhdGUpO3JldHVybiBuIT09bnVsbCYmbj49MCYmbjw9NH07CiAgY29uc3QgbmFtZXM9YT0+YS5zbGljZSgwLDMpLm1hcCh4PT54LmNvbXBhbnlfbmFtZXx8eC5jb2RlKS5qb2luKCfjgIEnKSsoYS5sZW5ndGg+Mz8nIOOBu+OBiyc6JycpOwogIGNvbnN0IGxpbmVzPVtdOwogIGNvbnN0IHN0YWxlPWhvbGRpbmdzLmZpbHRlcih4PT4hZnJlc2goeC5hc29mKSk7CiAgY29uc3QgdmFsaWQ9aG9sZGluZ3MuZmlsdGVyKHg9PmZyZXNoKHguYXNvZikmJk51bWJlcih4LmN1cnJlbnRfcHJpY2UpPjAmJk51bWJlcih4LmNvc3QpPjAmJk51bWJlcih4LnNoYXJlcyk+MCk7CiAgY29uc3Qgc3RvcD12YWxpZC5maWx0ZXIoeD0+eC5zdG9wX3BjdCE9bnVsbCYmTnVtYmVyLmlzRmluaXRlKE51bWJlcih4LnN0b3BfcGN0KSkmJk51bWJlcih4LmN1cnJlbnRfcHJpY2UpPD1OdW1iZXIoeC5jb3N0KSooMS1OdW1iZXIoeC5zdG9wX3BjdCkvMTAwKSk7CiAgY29uc3QgdGFrZT12YWxpZC5maWx0ZXIoeD0+eC50YWtlX3BjdCE9bnVsbCYmTnVtYmVyLmlzRmluaXRlKE51bWJlcih4LnRha2VfcGN0KSkmJk51bWJlcih4LmN1cnJlbnRfcHJpY2UpPj1OdW1iZXIoeC5jb3N0KSooMStOdW1iZXIoeC50YWtlX3BjdCkvMTAwKSk7CiAgaWYoc3RvcC5sZW5ndGgpbGluZXMucHVzaChgJHtuYW1lcyhzdG9wKX3jga/jgIHkv53lrZjjgZXjgozjgZ/ntYLlgKTjgYzoqK3lrprjga7mkI3liIfjgorjg6njgqTjg7Pku6XkuIvjgaDjgojjgILjgb7jgZrku4rjga7kvqHmoLzjgajjgIHmsbrjgoHjgabjgYTjgZ/jg6vjg7zjg6vjgpLnorroqo3jgZfjgojjgYbjga3imaVgKTsKICBpZih0YWtlLmxlbmd0aClsaW5lcy5wdXNoKGAke25hbWVzKHRha2UpfeOBr+OAgeS/neWtmOOBleOCjOOBn+e1guWApOOBjOioreWumuOBruWIqeeiuuODqeOCpOODs+OBq+WxiuOBhOOBpuOBhOOCi+OCiOOAguaJi+aVsOaWmei+vOOBv+OBruaQjeebiuOCkuimi+OBpuOAgeWjsuOCi+agquaVsOOCkuiQveOBoeedgOOBhOOBpuiAg+OBiOOCiOOBhuOBreKZpWApOwogIGlmKHN0YWxlLmxlbmd0aClsaW5lcy5wdXNoKGAke25hbWVzKHN0YWxlKX3jga/jgIHmoKrkvqHjga7ml6Xku5jjgYzlj6TjgYTjg7vkuI3mmI7jg7vmnKrmnaXml6Xjgarjga7jgafliKTmlq3jga/jgYrpoJDjgZHjgILmnIDmlrDntYLlgKTjgpLmm7TmlrDjgZfjgabjgYvjgonkuIDnt5LjgavopovjgojjgYbjga3imaVgKTsKICBjb25zdCB0b3RhbD12YWxpZC5yZWR1Y2UoKHN1bSx4KT0+c3VtK051bWJlcih4LmN1cnJlbnRfcHJpY2UpKk51bWJlcih4LnNoYXJlcyksMCk7CiAgY29uc3QgbGFyZ2U9dmFsaWQuZmluZCh4PT5OdW1iZXIoeC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoeC5zaGFyZXMpPnRvdGFsKjAuNSk7CiAgaWYodmFsaWQubGVuZ3RoPj0yJiZsYXJnZSlsaW5lcy5wdXNoKGAke2xhcmdlLmNvbXBhbnlfbmFtZXx8bGFyZ2UuY29kZX3jgYzjgIHml6Xku5jjgpLnorroqo3jgafjgY3jgZ/kv53mnInmoKrjga7oqZXkvqHpoY3jga7ljYrliIbjgpLotoXjgYjjgabjgYTjgovjgojjgILkuIDpipjmn4Tjgbjjga7lgY/jgorjgoLnorroqo3jgZfjgabjgYrjgZPjgYbjga3jgIJgKTsKICBjb25zdCBlbnRyaWVzPU9iamVjdC52YWx1ZXMobW92ZW1lbnR8fHt9KSxyZWNlbnQ9ZW50cmllcy5maWx0ZXIoeD0+eCYmIXguZXJyb3ImJngucSYmZnJlc2goKHgucS5zbmFwc2hvdHx8e30pLmxhc3RfZGF0ZSkpOwogIGlmKGVudHJpZXMubGVuZ3RoJiYhcmVjZW50Lmxlbmd0aClsaW5lcy5wdXNoKCflt6Hlm57ntZDmnpzjgavmlrDjgZfjgYTmoKrkvqHjg4fjg7zjgr/jgYzopovjgaTjgYvjgonjgarjgYTjgojjgILjgIzmnKzml6Xjga7lgJnoo5zjgI3jgajjgZfjgabkvb/jgYbliY3jgavjg4fjg7zjgr/ml6XjgpLnorroqo3jgZfjgabjga3imaUnKTsKICBlbHNlIGlmKHJlY2VudC5sZW5ndGgpbGluZXMucHVzaChg5beh5Zue57WQ5p6c44Gr44Gv44CBNOaXpeS7peWGheOBruaXpeS7mOOBruODh+ODvOOCv+OBjCR7cmVjZW50Lmxlbmd0aH3pipjmn4TjgYLjgovjgojjgILlvZPml6Xjga7lgKTli5XjgY3jgajjga/pmZDjgonjgarjgYTjga7jgafjgIHmsJfjgavjgarjgovlgJnoo5zjga/jg4fjg7zjgr/ml6Xjgajjg4Hjg6Pjg7zjg4jjgoLopovjgabjga3wn5GA4pyoYCk7CiAgaWYoIWhvbGRpbmdzLmxlbmd0aCYmd2F0Y2hlcy5sZW5ndGgpbGluZXMucHVzaChg44Km44Kp44OD44OB44Oq44K544OI44GvJHt3YXRjaGVzLmxlbmd0aH3pipjmn4TjgILosrfjgYTlgJnoo5zjga7ooajnpLrjgaDjgZHjgafmsbrjgoHjgZrjgIHmoLnmi6Djg7vjg4fjg7zjgr/ml6Xjg7vosrfjgaPjgZ/lvozjga7mkI3liIfjgorjg6vjg7zjg6vjgpLnorroqo3jgZfjgabjga3imaVgKTsKICBpZighaG9sZGluZ3MubGVuZ3RoJiYhd2F0Y2hlcy5sZW5ndGgpbGluZXMucHVzaCgn5rCX44Gr44Gq44KL6YqY5p+E44KS44Km44Kp44OD44OB44Oq44K544OI44Gr5YWl44KM44Gm44G/44KI44GG44Gt44CC5pyA5Yid44Gv5bCR44GX44Ga44Gk5q+U44G544Gm44CB6Ieq5YiG44GM6Kqs5piO44Gn44GN44KL6YqY5p+E44KS5o6i44GX44Gm44GE44GT44GG4pmlJyk7CiAgY29uc3QgdGlwcz1bCiAgICAn5LuK5pel44Gu44Gy44Go44GT44Go77ya6LK344GG5YmN44Gr44CM44Gp44GT44G+44Gn5LiL44GM44Gj44Gf44KJ6KaL55u044GZ44GL44CN44KS5rG644KB44Gm44GK44GT44GG44CC54Sm44KJ44Ga44GE44GT44GG44Gt4pmlJywKICAgICfku4rml6Xjga7jgbLjgajjgZPjgajvvJrkuIrjgYzjgaPjgabjgYTjgovnkIbnlLHjgajjgIHoh6rliIbjgYzosrfjgYTjgZ/jgYTnkIbnlLHjgpLliIbjgZHjgabogIPjgYjjgabjgb/jgojjgYbjga3wn5GA4pyoJywKICAgICfku4rml6Xjga7jgbLjgajjgZPjgajvvJrliKnnm4rjga7lpKfjgY3jgZXjgaDjgZHjgafjgarjgY/jgIHmkI3jgZfjgZ/loLTlkIjjga7ph5HpoY3jgoLopovjgabjgYrjgZPjgYbjgILmoKrmlbDjga7oqr/mlbTjgoLlpKfkuovjgafjgZnjgZ7imaUnLAogICAgJ+S7iuaXpeOBruOBsuOBqOOBk+OBqO+8muS9leOCguOBl+OBquOBhOaXpeOBjOOBguOBo+OBpuOCguWkp+S4iOWkq+OAguadoeS7tuOBjOaPg+OBhuOBvuOBp+W+heOBpOOBruOCguOAgeiHquWIhuOBp+mBuOOBtuihjOWLleOBoOOCiOKZpScsCiAgICAn5LuK5pel44Gu44Gy44Go44GT44Go77ya5bmz5Z2H44Gu5pyf5b6F5YCk44GM6auY44GP44Gm44KC44CB5q+O5Zue44Gd44Gu6YCa44KK44Gr44Gv44Gq44KJ44Gq44GE44KI44CC5LiL5oyv44KM44Gu5bmF44KC5LiA57eS44Gr56K66KqN44GX44KI44GG44Gt44CCJywKICAgICfku4rml6Xjga7jgbLjgajjgZPjgajvvJrpgY7ljrvjga7lgKTli5XjgY3jgavliqDjgYjjgabjgIHmsbrnrpfjga7plovnpLrml6Xjgajjg4fjg7zjgr/jga7prq7luqbjgoLnorroqo3jgZfjgojjgYbjga3imaUnLAogICAgJ+S7iuaXpeOBruOBsuOBqOOBk+OBqO+8muaMr+OCiui/lOOCiuOBr+OAjOW9k+OBn+OBo+OBn+OBi+OAjeOBoOOBkeOBp+OBquOBj+OAgeOAjOaxuuOCgeOBn+ODq+ODvOODq+OCkuWuiOOCjOOBn+OBi+OAjeOCguimi+OBpuOBv+OCiOOBhuOBreKZpScKICBdOwogIGxpbmVzLnB1c2godGlwc1tNYXRoLmZsb29yKERhdGUucGFyc2UoZGF5KydUMDA6MDA6MDBaJykvODY0MDAwMDApJXRpcHMubGVuZ3RoXSk7CiAgcmV0dXJuIHtkYXksbGluZXN9Owp9CmZ1bmN0aW9uIHJlbmRlckRhaWx5QWR2aWNlKCl7CiAgY29uc3QgYm94PSQoJ3JhZGVuQWR2aWNlJyk7aWYoIWJveClyZXR1cm47CiAgbGV0IG1vdmVtZW50PXt9O3RyeXttb3ZlbWVudD1KU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKCdmcmVlX21vdmVtZW50X3YxJyl8fCd7fScpfWNhdGNoKGUpe30KICBjb25zdCBhZHZpY2U9YnVpbGRSYWRlbkFkdmljZShsb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKSxsb2NhbCgnZnJlZV93YXRjaCcpLG1vdmVtZW50KTsKICAkKCdyYWRlbkFkdmljZURhdGUnKS50ZXh0Q29udGVudD1hZHZpY2UuZGF5KyfvvIjml6XmnKzmmYLplpPvvInvvI/ml6Xku5jjgajkv53lrZjmuIjjgb/jg4fjg7zjgr/jgavlv5zjgZjjgabmm7TmlrAnOwogIGJveC5yZXBsYWNlQ2hpbGRyZW4oKTsKICBmb3IoY29uc3QgbGluZSBvZiBhZHZpY2UubGluZXMpe2NvbnN0IHA9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgncCcpO3Auc3R5bGUubGluZUhlaWdodD0nMS44JztwLnRleHRDb250ZW50PWxpbmU7Ym94LmFwcGVuZENoaWxkKHApO30KfQpmdW5jdGlvbiB2YWwoaWQpe2xldCB2PSQoaWQpLnZhbHVlLnRyaW0oKTtyZXR1cm4gdj09PScnP251bGw6TnVtYmVyKHYpfQpmdW5jdGlvbiBsb2NhbChrKXt0cnl7cmV0dXJuIEpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oayl8fCdbXScpfWNhdGNoKGUpe3JldHVybltdfX0KZnVuY3Rpb24gc2F2ZShrLHYpe2xvY2FsU3RvcmFnZS5zZXRJdGVtKGssSlNPTi5zdHJpbmdpZnkodikpfQpmdW5jdGlvbiBmbXQodixkPTIpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZChkKX0KZnVuY3Rpb24geWVuKHYpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzonwqUnK01hdGgucm91bmQoTnVtYmVyKHYpKS50b0xvY2FsZVN0cmluZygnamEtSlAnKX0KZnVuY3Rpb24gc3RhdGVKYShzKXsKICBpZihzPT09J3N0cm9uZ19idWxsaXNoJylyZXR1cm4gJ+W8t+awlyc7CiAgaWYocz09PSdidWxsaXNoJylyZXR1cm4gJ+OChOOChOW8t+awlyc7CiAgaWYocz09PSduZXV0cmFsJylyZXR1cm4gJ+S4reeriyc7CiAgaWYocz09PSdiZWFyaXNoJylyZXR1cm4gJ+OChOOChOW8seawlyc7CiAgaWYocz09PSdzdHJvbmdfYmVhcmlzaCcpcmV0dXJuICflvLHmsJcnOwogIHJldHVybiAn5Yik5a6a5L+d55WZJzsKfQoKZnVuY3Rpb24gc3RhdGVDbGFzcyhzKXsKICBpZihzPT09J3N0cm9uZ19idWxsaXNoJylyZXR1cm4gJ3N0YXRlLXN0cm9uZy1idWxsJzsKICBpZihzPT09J2J1bGxpc2gnKXJldHVybiAnc3RhdGUtYnVsbCc7CiAgaWYocz09PSduZXV0cmFsJylyZXR1cm4gJ3N0YXRlLW5ldXRyYWwnOwogIGlmKHM9PT0nYmVhcmlzaCcpcmV0dXJuICdzdGF0ZS1iZWFyJzsKICBpZihzPT09J3N0cm9uZ19iZWFyaXNoJylyZXR1cm4gJ3N0YXRlLXN0cm9uZy1iZWFyJzsKICByZXR1cm4gJ3N0YXRlLW5ldXRyYWwnOwp9CgpmdW5jdGlvbiBmYWN0b3JKYShrZXkpewogIGlmKGtleT09PSd0ZWNobmljYWwnKXJldHVybiAn44OG44Kv44OL44Kr44OrJzsKICBpZihrZXk9PT0nZWFybmluZ3MnKXJldHVybiAn5rG6566XJzsKICBpZihrZXk9PT0nc3VwcGx5JylyZXR1cm4gJ+mcgOe1pnByb3h5JzsKICBpZihrZXk9PT0ncG9saWN5JylyZXR1cm4gJ+Wbveetlic7CiAgcmV0dXJuIGtleXx8J+imgeWboCc7Cn0KCmZ1bmN0aW9uIGRyaXZlclNlbnRlbmNlKHNjKXsKICBjb25zdCBwPXNjJiZzYy5zdHJvbmdlc3RfcG9zaXRpdmU7CiAgY29uc3Qgbj1zYyYmc2Muc3Ryb25nZXN0X25lZ2F0aXZlOwogIGNvbnN0IHNjb3JlPU51bWJlcihzYyYmc2Muc2NvcmUxMDApOwoKICBsZXQgaGVhZD0nJzsKICBpZihOdW1iZXIuaXNGaW5pdGUoc2NvcmUpKXsKICAgIGlmKHNjb3JlPj04MCloZWFkPSflj5blvpfmuIjjgb/opoHlm6DjgpLnt4/lkIjjgZnjgovjgajjgIHlvLfjgYTjg5fjg6njgrnoqZXkvqHjgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49NjUpaGVhZD0n44OX44Op44K56KaB5Zug44GM5YSq5Yui44Gn44CB44KE44KE5by35rCX44Gu6KmV5L6h44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTQ1KWhlYWQ9J+ODl+ODqeOCueOBqOODnuOCpOODiuOCueOBjOaLruaKl+OBl+OAgeS4reeri+Wcj+OBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj0zMCloZWFkPSfjg57jgqTjg4rjgrnopoHlm6Djga7lvbHpn7/jgYzjgoTjgoTlvLfjgY/jgIHmhY7ph43lr4TjgorjgafjgZnjgIInOwogICAgZWxzZSBoZWFkPSfjg57jgqTjg4rjgrnopoHlm6Djga7lvbHpn7/jgYzlpKfjgY3jgY/jgIHlvLHmsJflr4TjgorjgafjgZnjgIInOwogIH0KCiAgbGV0IHRhaWw9W107CiAgaWYobil0YWlsLnB1c2goJ+acgOWkp+OBruaKvOOBl+S4i+OBkuimgeWboOOBryAnK2ZhY3RvckphKG4ua2V5KSsnICcrc2NvcmVMYWJlbChuLnNjb3JlKSk7CiAgaWYocCl0YWlsLnB1c2goJ+acgOWkp+OBruaKvOOBl+S4iuOBkuimgeWboOOBryAnK2ZhY3RvckphKHAua2V5KSsnICcrc2NvcmVMYWJlbChwLnNjb3JlKSk7CiAgcmV0dXJuIGhlYWQrKHRhaWwubGVuZ3RoPycgJyt0YWlsLmpvaW4oJ+OAgicpKyfjgIInOicnKTsKfQoKZnVuY3Rpb24gZHJpdmVyQm94SHRtbCh0aXRsZSxkLGtpbmQpewogIGlmKCFkKXJldHVybiBgPGRpdiBjbGFzcz0iZHJpdmVyYm94Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuipsuW9k+OBquOBlzwvYj48L2Rpdj5gOwogIGNvbnN0IHNpZ249TnVtYmVyKGQuY29udHJpYnV0aW9uKT49MD8nKyc6Jyc7CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJkcml2ZXJib3giPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj4KICAgIDxiPiR7ZmFjdG9ySmEoZC5rZXkpfSAke3Njb3JlTGFiZWwoZC5zY29yZSl9PC9iPgogICAgPHNtYWxsIGNsYXNzPSJtdXRlZCI+5YaN6YWN5YiG5b6M44Gu6YeN44G/ICR7ZC53ZWlnaHRfcGN0fSUgLyDlr4TkuI4gJHtzaWdufSR7TnVtYmVyKGQuY29udHJpYnV0aW9uKS50b0ZpeGVkKDEpfTwvc21hbGw+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gY29udHJpYnV0aW9uQ2FyZEh0bWwoZCl7CiAgaWYoIWQpcmV0dXJuICcnOwogIGNvbnN0IGM9TnVtYmVyKGQuY29udHJpYnV0aW9uKTsKICBjb25zdCBzaWduPWM+MD8nKyc6Jyc7CiAgY29uc3QgaW1wYWN0PWMvMjsKICBjb25zdCBpbXBhY3RTaWduPWltcGFjdD4wPycrJzonJzsKICByZXR1cm4gYDxkaXYgY2xhc3M9ImNvbnRyaWIiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke2ZhY3RvckphKGQua2V5KX08L3NwYW4+CiAgICA8Yj4ke3NpZ259JHtjLnRvRml4ZWQoMSl9PC9iPgogICAgPHNtYWxsPuWfuua6lumHjeOBvyAke051bWJlcihkLmJhc2Vfd2VpZ2h0X3BjdD8/MCkudG9GaXhlZCgxKX0lIMOXIOmuruW6piAke051bWJlcihkLmZyZXNobmVzc19wY3Q/PzEwMCkudG9GaXhlZCgwKX0lPC9zbWFsbD4KICAgIDxzbWFsbD7lho3phY3liIblvozph43jgb8gJHtOdW1iZXIoZC53ZWlnaHRfcGN0KS50b0ZpeGVkKDEpfSU8L3NtYWxsPgogICAgPHNtYWxsPjEwMOeCueaPm+eul+OBuOOBruW9semfvyAke2ltcGFjdFNpZ259JHtpbXBhY3QudG9GaXhlZCgxKX3ngrk8L3NtYWxsPgogIDwvZGl2PmA7Cn0KCmZ1bmN0aW9uIHJlbmRlckNvbnRyaWJ1dGlvbnMoc2MpewogIGNvbnN0IGJveD0kKCdjb250cmlidXRpb25Cb3gnKTsKICBjb25zdCBncmlkPSQoJ2NvbnRyaWJ1dGlvbkdyaWQnKTsKICBpZighYm94fHwhZ3JpZClyZXR1cm47CiAgY29uc3QgZHM9KHNjJiZzYy5kcml2ZXJzKXx8W107CiAgaWYoIWRzLmxlbmd0aCl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nbm9uZSc7CiAgICBncmlkLmlubmVySFRNTD0nJzsKICAgIHJldHVybjsKICB9CiAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICBncmlkLmlubmVySFRNTD1kcy5tYXAoY29udHJpYnV0aW9uQ2FyZEh0bWwpLmpvaW4oJycpOwp9CmZ1bmN0aW9uIHBjdCh2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoMikrJyUnfQpmdW5jdGlvbiBzdGF0Rm9yKGgsa2V5KXtyZXR1cm4gaCYmaC5mb3J3YXJkX3N0YXRzJiZoLmZvcndhcmRfc3RhdHNba2V5XT9oLmZvcndhcmRfc3RhdHNba2V5XTpudWxsfQoKZnVuY3Rpb24gZGF0YUFnZURheXMoZGF0ZVN0cil7CiAgaWYoIWRhdGVTdHIpcmV0dXJuIG51bGw7CiAgY29uc3QgbT1TdHJpbmcoZGF0ZVN0cikubWF0Y2goL14oXGR7NH0pLShcZHsyfSktKFxkezJ9KSQvKTsKICBpZighbSlyZXR1cm4gbnVsbDsKICBjb25zdCBkPURhdGUuVVRDKE51bWJlcihtWzFdKSxOdW1iZXIobVsyXSktMSxOdW1iZXIobVszXSkpOwogIGNvbnN0IG5vdz1uZXcgRGF0ZSgpOwogIGNvbnN0IHRvZGF5PURhdGUuVVRDKG5vdy5nZXRGdWxsWWVhcigpLG5vdy5nZXRNb250aCgpLG5vdy5nZXREYXRlKCkpOwogIHJldHVybiBNYXRoLm1heCgwLE1hdGguZmxvb3IoKHRvZGF5LWQpLzg2NDAwMDAwKSk7Cn0KCmZ1bmN0aW9uIGZyZXNobmVzc0ZvcihkYXRlU3RyKXsKICBjb25zdCBkYXlzPWRhdGFBZ2VEYXlzKGRhdGVTdHIpOwogIGlmKGRheXM9PT1udWxsKXsKICAgIHJldHVybiB7bGV2ZWw6J3Vua25vd24nLGRheXM6bnVsbCxsYWJlbDon5pel5LuY5LiN5piOJyxjbHM6J2ZyZXNoLXdhcm4nLGRlY2lzaW9uX29rOmZhbHNlfTsKICB9CiAgaWYoZGF5czw9NCl7CiAgICByZXR1cm4ge2xldmVsOidmcmVzaCcsZGF5cyxsYWJlbDon6a6u5bqmT0snLGNsczonZnJlc2gtb2snLGRlY2lzaW9uX29rOnRydWV9OwogIH0KICBpZihkYXlzPD0xMCl7CiAgICByZXR1cm4ge2xldmVsOid3YXJuaW5nJyxkYXlzLGxhYmVsOifjgoTjgoTpgYXlu7YnLGNsczonZnJlc2gtd2FybicsZGVjaXNpb25fb2s6dHJ1ZX07CiAgfQogIHJldHVybiB7bGV2ZWw6J3N0YWxlJyxkYXlzLGxhYmVsOiflj6TjgYTjg4fjg7zjgr8nLGNsczonZnJlc2gtc3RhbGUnLGRlY2lzaW9uX29rOmZhbHNlfTsKfQoKZnVuY3Rpb24gZnJlc2huZXNzVGV4dChkYXRlU3RyKXsKICBjb25zdCBmPWZyZXNobmVzc0ZvcihkYXRlU3RyKTsKICBpZihmLmRheXM9PT1udWxsKXJldHVybiAn5pyA57WC44OH44O844K/5pel44KS56K66KqN44Gn44GN44G+44Gb44KT44CCJzsKICBpZihmLmxldmVsPT09J2ZyZXNoJylyZXR1cm4gYOacgOe1guODh+ODvOOCv+aXpeOBi+OCiSAke2YuZGF5c33ml6XjgILpgJrluLjjga7lj4LogIPliKTlrprjgavkvb/nlKjjgZfjgb7jgZnjgIJgOwogIGlmKGYubGV2ZWw9PT0nd2FybmluZycpcmV0dXJuIGDmnIDntYLjg4fjg7zjgr/ml6XjgYvjgokgJHtmLmRheXN95pel44CC6YGF5bu244Gr5rOo5oSP44GX44Gm44CB5a6f6Zqb44Gu54++5Zyo5YCk44KC56K66KqN44GX44Gm44GP44Gg44GV44GE44CCYDsKICByZXR1cm4gYOacgOe1guODh+ODvOOCv+aXpeOBi+OCiSAke2YuZGF5c33ml6XpgYXjgozjgILku4rml6Xjga7lo7LosrfliKTmlq3jgavjga/lj6TjgYTjgZ/jgoHjgIHkv53mnInliKTmlq3jga/oh6rli5Xjgafkv53nlZnjgZfjgb7jgZnjgIJgOwp9CgpmdW5jdGlvbiBzaG93RnJlc2huZXNzKGRhdGVTdHIpewogIGNvbnN0IGJveD0kKCdmcmVzaG5lc3NCb3gnKTsKICBpZighYm94KXJldHVybjsKICBjb25zdCBmPWZyZXNobmVzc0ZvcihkYXRlU3RyKTsKICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogIGJveC5jbGFzc05hbWU9J2ZyZXNoYm94ICcrZi5jbHM7CiAgJCgnZnJlc2huZXNzVGl0bGUnKS50ZXh0Q29udGVudD0KICAgIGYubGV2ZWw9PT0nZnJlc2gnPyfinIUg44OH44O844K/6a6u5bqmT0snOgogICAgZi5sZXZlbD09PSd3YXJuaW5nJz8n4pqg77iPIOODh+ODvOOCv+mBheW7tuOBq+azqOaEjyc6CiAgICBmLmxldmVsPT09J3N0YWxlJz8n8J+bkSDjg4fjg7zjgr/jgYzlj6TjgYTjgZ/jgoHku4rml6Xjga7liKTmlq3jga/kv53nlZknOgogICAgJ+KaoO+4jyDjg4fjg7zjgr/prq7luqbjgpLnorroqo3jgafjgY3jgb7jgZvjgpMnOwogICQoJ2ZyZXNobmVzc0RldGFpbCcpLnRleHRDb250ZW50PWZyZXNobmVzc1RleHQoZGF0ZVN0cik7Cn0KCmZ1bmN0aW9uIHNob3dIaXN0b3J5RnJlc2huZXNzKGhpc3RvcnlEYXRlLHByaWNlRGF0ZSl7CiAgY29uc3QgYm94PSQoJ2hpc3RvcnlGcmVzaG5lc3NCb3gnKTsKICBpZighYm94KXJldHVybjsKICBpZighaGlzdG9yeURhdGUpewogICAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgICQoJ2hpc3RvcnlGcmVzaG5lc3NUZXh0JykudGV4dENvbnRlbnQ9JzIw5pel44O7MTI25pel44O7MjUy5pel44Gu5YiG5p6Q5bGl5q2044KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44Gf44CC54++5Zyo5YCk44Gg44GR6KGo56S644GX44G+44GZ44CCJzsKICAgIHJldHVybjsKICB9CiAgY29uc3QgaGY9ZnJlc2huZXNzRm9yKGhpc3RvcnlEYXRlKTsKICBpZihoZi5sZXZlbD09PSdmcmVzaCcpewogICAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgIGJveC5jbGFzc05hbWU9J2ZyZXNoYm94IGZyZXNoLW9rJzsKICAgICQoJ2hpc3RvcnlGcmVzaG5lc3NUZXh0JykudGV4dENvbnRlbnQ9CiAgICAgIGDkvqHmoLzlsaXmrbTjgoLmnIDmlrDllrbmpa3ml6UgJHtoaXN0b3J5RGF0ZX0g44G+44Gn5Y+W5b6X44CC55+t5Lit6ZW344O76ZyA57WmcHJveHnjg7vjg4jjg6zjg7zjg6rjg7PjgrDjgpLpgJrluLjoqIjnrpfjgZfjgb7jgZnjgIJgOwogICAgcmV0dXJuOwogIH0KICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogIGNvbnN0IHBkPXByaWNlRGF0ZXx8J+KAlCc7CiAgJCgnaGlzdG9yeUZyZXNobmVzc1RleHQnKS50ZXh0Q29udGVudD0KICAgIGDnj77lnKjlgKTjg4fjg7zjgr/ml6UgJHtwZH0gLyDkvqHmoLzlsaXmrbTmnIDntYLml6UgJHtoaXN0b3J5RGF0ZX3jgILkvqHmoLzlsaXmrbTjgYzlj6TjgYTloLTlkIjjgaDjgZHjgIHjg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjg7vpnIDntaZwcm94eeOCkuWPguiAg+WApOaJseOBhOOBq+OBl+OBvuOBmeOAgmA7Cn0KCmZ1bmN0aW9uIGNvbmZpZGVuY2VGb3IoaCl7CiAgY29uc3QgdmFscz1baC5yZXR1cm5fMjBkLGgucmV0dXJuXzEyNmQsaC5yZXR1cm5fMjUyZF0ubWFwKE51bWJlcikuZmlsdGVyKE51bWJlci5pc0Zpbml0ZSk7CiAgY29uc3Qgc3RhdHM9WycyMGQnLCcxMjZkJywnMjUyZCddLm1hcChrPT5zdGF0Rm9yKGgsaykpLmZpbHRlcihzPT5zJiZzLnN0YXR1cz09PSdvaycpOwoKICBpZighTnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKXx8IWguYXNvZilyZXR1cm4gMDsKCiAgY29uc3QgY292ZXJhZ2U9dmFscy5sZW5ndGgvMzsKICBsZXQgYWdyZWVtZW50PTAuNTsKICBpZih2YWxzLmxlbmd0aCl7CiAgICBjb25zdCBwb3M9dmFscy5maWx0ZXIoeD0+eD4wKS5sZW5ndGg7CiAgICBjb25zdCBuZWc9dmFscy5maWx0ZXIoeD0+eDwwKS5sZW5ndGg7CiAgICBhZ3JlZW1lbnQ9TWF0aC5tYXgocG9zLG5lZykvdmFscy5sZW5ndGg7CiAgfQogIGNvbnN0IHN0YXRDb3ZlcmFnZT1zdGF0cy5sZW5ndGgvMzsKICBsZXQgc2NvcmU9KGNvdmVyYWdlKjAuNDUgKyBhZ3JlZW1lbnQqMC4yNSArIHN0YXRDb3ZlcmFnZSowLjMwKSoxMDA7CgogIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogIGlmKGZyZXNoLmxldmVsPT09J3dhcm5pbmcnKXNjb3JlKj0wLjYwOwogIGlmKGZyZXNoLmxldmVsPT09J3N0YWxlJylzY29yZT1NYXRoLm1pbihzY29yZSwyNSk7CiAgaWYoZnJlc2gubGV2ZWw9PT0ndW5rbm93bicpc2NvcmU9TWF0aC5taW4oc2NvcmUsMjApOwoKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSd3YXJuaW5nJylzY29yZSo9MC43NTsKICBpZihoaXN0b3J5RnJlc2gubGV2ZWw9PT0nc3RhbGUnKXNjb3JlPU1hdGgubWluKHNjb3JlLDM1KTsKICBpZihoaXN0b3J5RnJlc2gubGV2ZWw9PT0ndW5rbm93bicpc2NvcmU9TWF0aC5taW4oc2NvcmUsMjUpOwoKICByZXR1cm4gTWF0aC5yb3VuZChzY29yZSk7Cn0KCmZ1bmN0aW9uIGRlY2lzaW9uRm9yKGgsYyl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhaC5hc29mKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246J+Wun+ODh+ODvOOCv+acquWPluW+l+OAguWPs+S4iuOBruOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44GX44Gm44GP44Gg44GV44GE44CCJywKICAgICAgY29uZmlkZW5jZTowCiAgICB9OwogIH0KCiAgY29uc3QgcjIwPU51bWJlcihoLnJldHVybl8yMGQpLCByMTI2PU51bWJlcihoLnJldHVybl8xMjZkKSwgcjI1Mj1OdW1iZXIoaC5yZXR1cm5fMjUyZCk7CiAgY29uc3QgY29uZmlkZW5jZT1jb25maWRlbmNlRm9yKGgpOwogIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihoLmFzb2YpOwoKICBpZighZnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVme+8iOagquS+oeWPpOOBhO+8iScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjpgJHtmcmVzaG5lc3NUZXh0KGguYXNvZil9IOaQjeWIh+OCiuODu+WIqeeiuuODqeOCpOODs+OBqOOBruavlOi8g+OCguWPguiAg+WApOaJseOBhOOBp+OBmeOAgmAsCiAgICAgIGNvbmZpZGVuY2UKICAgIH07CiAgfQoKICBpZihjdXI8PWMuc3RvcFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+aQjeWIh+OCiuaknOiojicsY2xzOidkLXN0b3AnLHJlYXNvbjon6Kit5a6a44GX44Gf5pCN5YiH44KK5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifliKnnorrmpJzoqI4nLGNsczonZC10YWtlJyxyZWFzb246J+ioreWumuOBl+OBn+WIqeeiuuWPguiAg+ODqeOCpOODs+S7peS4iicsY29uZmlkZW5jZX07CiAgfQoKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGlmKCFoaXN0b3J5RnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVme+8iOWxpeattOWPpOOBhO+8iScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjpg54++5Zyo5YCk44Gv5Y+W5b6X44Gn44GN44Gm44GE44G+44GZ44GM44CB5L6h5qC85bGl5q2044GvICR7aC5oaXN0b3J5X2Fzb2Z8fCfkuI3mmI4nfeOAguWbuuWumuOBruaQjeWIh+OCii/liKnnorrjg6njgqTjg7Pjgavjga/mnKrliLDpgZTjgafjgZnjgYzjgIHjg4jjg6zjg7Pjg4nliKTmlq3jga/kv53nlZnjgZfjgb7jgZnjgIJgLAogICAgICBjb25maWRlbmNlCiAgICB9OwogIH0KCiAgaWYoY3VyPD1jLnRyYWlsUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon6K2m5oiSJyxjbHM6J2Qtd2F0Y2gnLHJlYXNvbjonMjDml6Xpq5jlgKTln7rmupbjga7jg4jjg6zjg7zjg6rjg7PjgrDlj4LogIPjg6njgqTjg7Pku6XkuIsnLGNvbmZpZGVuY2V9OwogIH0KCiAgbGV0IHBvc2l0aXZlPTAsIG5lZ2F0aXZlPTA7CiAgW3IyMCxyMTI2LHIyNTJdLmZvckVhY2goeD0+ewogICAgaWYoTnVtYmVyLmlzRmluaXRlKHgpKXsKICAgICAgaWYoeD4wKXBvc2l0aXZlKys7CiAgICAgIGlmKHg8MCluZWdhdGl2ZSsrOwogICAgfQogIH0pOwoKICBpZihuZWdhdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpeODuzEyNuaXpeODuzI1MuaXpeOBruOBhuOBoeODnuOCpOODiuOCueWCvuWQkeOBjOWEquWLoicsY29uZmlkZW5jZX07CiAgfQogIGlmKHBvc2l0aXZlPj0yKXsKICAgIHJldHVybiB7bGFiZWw6J+S/neaciee2mee2micsY2xzOidkLWhvbGQnLHJlYXNvbjon6Kit5a6a44Op44Kk44Oz5YaF44Gn44CB6KSH5pWw5pyf6ZaT44Gu5L6h5qC844OI44Os44Oz44OJ44GM44OX44Op44K5Jyxjb25maWRlbmNlfTsKICB9CiAgcmV0dXJuIHtsYWJlbDon5L+d5pyJ57aZ57aa77yI5qeY5a2Q6KaL77yJJyxjbHM6J2QtaG9sZCcscmVhc29uOifoqK3lrprjg6njgqTjg7PlhoXjgILmnJ/plpPliKXjg4jjg6zjg7Pjg4njga/lvLflvLHjgYzmt7flnKgnLGNvbmZpZGVuY2V9Owp9CmZ1bmN0aW9uIGV2SHRtbCh0aXRsZSxzKXsKICBpZighc3x8cy5zdGF0dXMhPT0nb2snKXsKICAgIGNvbnN0IG49cyYmcy5uIT09dW5kZWZpbmVkP3MubjowOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7jg4fjg7zjgr/kuI3otrM8L2I+PHNtYWxsPuaomeacrCAke2595Lu2PC9zbWFsbD48L2Rpdj5gOwogIH0KICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+CiAgICA8Yj7lubPlnYcgJHtwY3Qocy5tZWFuKX08L2I+CiAgICA8c21hbGw+5Lit5aSu5YCkICR7cGN0KHMubWVkaWFuKX08L3NtYWxsPgogICAgPHNtYWxsPuS4iuaYh+eOhyAke3BjdChzLnBvc2l0aXZlX3JhdGUpfTwvc21hbGw+CiAgICA8c21hbGw+UDEw44CcUDkwICR7cGN0KHMucDEwKX0g44CcICR7cGN0KHMucDkwKX08L3NtYWxsPgogICAgPHNtYWxsPuaomeacrCAke3Mubn3ku7Y8L3NtYWxsPgogIDwvZGl2PmA7Cn0KCgpmdW5jdGlvbiBkaXN0YW5jZUluZm8oY3VyLHRhcmdldCxraW5kKXsKICBjdXI9TnVtYmVyKGN1cik7IHRhcmdldD1OdW1iZXIodGFyZ2V0KTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IU51bWJlci5pc0Zpbml0ZSh0YXJnZXQpKXJldHVybiAn4oCUJzsKICBjb25zdCBkaWZmPSh0YXJnZXQvY3VyLTEpKjEwMDsKICBpZihraW5kPT09J3N0b3AnKXsKICAgIGlmKGRpZmY+PTApcmV0dXJuICfjg6njgqTjg7PliLDpgZTmuIjjgb8nOwogICAgcmV0dXJuIE1hdGguYWJzKGRpZmYpLnRvRml4ZWQoMikrJyUg5LiLJzsKICB9CiAgaWYoa2luZD09PSd0YWtlJyl7CiAgICBpZihkaWZmPD0wKXJldHVybiAn44Op44Kk44Oz5Yiw6YGU5riI44G/JzsKICAgIHJldHVybiBkaWZmLnRvRml4ZWQoMikrJyUg5LiKJzsKICB9CiAgcmV0dXJuIChkaWZmPj0wPycrJzonJykrZGlmZi50b0ZpeGVkKDIpKyclJzsKfQoKZnVuY3Rpb24gcHJpY2VSYW5nZShjdXIscyl7CiAgY3VyPU51bWJlcihjdXIpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhc3x8cy5zdGF0dXMhPT0nb2snKXJldHVybiBudWxsOwogIHJldHVybiB7CiAgICBsb3c6Y3VyKigxK051bWJlcihzLnAxMCkvMTAwKSwKICAgIGhpZ2g6Y3VyKigxK051bWJlcihzLnA5MCkvMTAwKSwKICAgIG1lZGlhbjpjdXIqKDErTnVtYmVyKHMubWVkaWFuKS8xMDApCiAgfTsKfQoKZnVuY3Rpb24gcmFuZ2VIdG1sKHRpdGxlLGN1cixzKXsKICBjb25zdCByPXByaWNlUmFuZ2UoY3VyLHMpOwogIGlmKCFyKXsKICAgIGNvbnN0IG49cyYmcy5uIT09dW5kZWZpbmVkP3MubjowOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJyYW5nZWJveCI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7jg4fjg7zjgr/kuI3otrM8L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7mqJnmnKwgJHtufeS7tjwvc3Bhbj48L2Rpdj5gOwogIH0KICByZXR1cm4gYDxkaXYgY2xhc3M9InJhbmdlYm94Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX0gUDEw44CcUDkwPC9zcGFuPgogICAgPGI+JHt5ZW4oci5sb3cpfSDjgJwgJHt5ZW4oci5oaWdoKX08L2I+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuS4reWkruWApOaPm+eulyAke3llbihyLm1lZGlhbil9PC9zcGFuPgogIDwvZGl2PmA7Cn0KCmZ1bmN0aW9uIGFjdGlvblRleHQoaCxjLGQpewogIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjmoKrkvqHlj6TjgYTvvIknKXJldHVybiAn44OH44O844K/44GM5Y+k44GE44Gf44KB5LuK5pel44Gu5Yik5pat44Gv5L+d55WZ44CC6Ki85Yi45Lya56S+44Gq44Gp44Gn5a6f6Zqb44Gu54++5Zyo5YCk44KS56K66KqN44GX44Gm44GL44KJ5Yik5pat44CCJzsKICBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOWxpeattOWPpOOBhO+8iScpcmV0dXJuICfnj77lnKjlgKTjga/norroqo3muIjjgb/jgILlm7rlrprjga7mkI3liIfjgoov5Yip56K644Op44Kk44Oz44Gg44GR56K66KqN44GX44CB44OI44Os44Oz44OJ5Yik5pat44Gv5L6h5qC85bGl5q205pu05paw44G+44Gn5L+d55WZ44CCJzsKICBpZihkLmxhYmVsPT09J+aQjeWIh+OCiuaknOiojicpcmV0dXJuICfmkI3liIfjgorlj4LogIPjg6njgqTjg7PjgpLkuIvlm57jgaPjgabjgYTjgb7jgZnjgILlrp/pmpvjga7nj77lnKjlgKTjgajms6jmlofmnaHku7bjgpLnorroqo3jgZfjgabjgIHnuK7lsI/jg7vmkqTpgIDjgpLmpJzoqI7jgIInOwogIGlmKGQubGFiZWw9PT0n5Yip56K65qSc6KiOJylyZXR1cm4gJ+WIqeeiuuWPguiAg+ODqeOCpOODs+OBq+WIsOmBlOOBl+OBpuOBhOOBvuOBmeOAguWFqOmDqOWjsuWNtOOBoOOBkeOBp+OBquOBj+OAgeWIhuWJsuWIqeeiuuOCguWAmeijnOOAgic7CiAgaWYoZC5sYWJlbD09PSforabmiJInKXJldHVybiAn6K2m5oiS44K+44O844Oz44CC44OI44Os44O844Oq44Oz44Kw44Op44Kk44Oz44Go5Lit55+t5pyf44Gu5YCk5YuV44GN44KS5YSq5YWI44GX44Gm56K66KqN44CCJzsKICByZXR1cm4gJ+ioreWumuODqeOCpOODs+WGheOAguS/neaciee2mee2muWAmeijnOOBp+OBmeOBjOOAgeeEoeaWmeODh+ODvOOCv+OBr+mBheW7tuOBmeOCi+OBn+OCgeWun+mam+OBruePvuWcqOWApOOCgueiuuiqjeOAgic7Cn0KCgpmdW5jdGlvbiBub211cmFOZXRGZWUoYW1vdW50KXsKICBhbW91bnQ9TnVtYmVyKGFtb3VudHx8MCk7CiAgaWYoYW1vdW50PD0wKXJldHVybiAwOwogIGlmKGFtb3VudDw9MTAwMDAwKXJldHVybiAxNTI7CiAgaWYoYW1vdW50PD0zMDAwMDApcmV0dXJuIDMzMDsKICBpZihhbW91bnQ8PTUwMDAwMClyZXR1cm4gNTI0OwogIGlmKGFtb3VudDw9MTAwMDAwMClyZXR1cm4gMTA0ODsKICBpZihhbW91bnQ8PTIwMDAwMDApcmV0dXJuIDIwOTU7CiAgaWYoYW1vdW50PD0zMDAwMDAwKXJldHVybiAzMTQzOwogIGlmKGFtb3VudDw9NTAwMDAwMClyZXR1cm4gNTIzODsKICBpZihhbW91bnQ8PTEwMDAwMDAwKXJldHVybiAxMDQ3NjsKICBpZihhbW91bnQ8PTIwMDAwMDAwKXJldHVybiAyMDk1MjsKICBpZihhbW91bnQ8PTMwMDAwMDAwKXJldHVybiAzMTQyOTsKICBpZihhbW91bnQ8PTUwMDAwMDAwKXJldHVybiA0MTkwNTsKICByZXR1cm4gNzg1NzE7Cn0KZnVuY3Rpb24gZmVlRm9yKGFtb3VudCxtb2RlKXtyZXR1cm4gbW9kZT09PSdub211cmFfbmV0Jz9ub211cmFOZXRGZWUoYW1vdW50KTowfQoKZnVuY3Rpb24gY2FsY0hvbGRpbmcoaCl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IGNvc3Q9TnVtYmVyKGguY29zdCk7CiAgY29uc3Qgc2hhcmVzPU51bWJlcihoLnNoYXJlcyk7CiAgY29uc3QgYnV5VmFsdWU9Y29zdCpzaGFyZXM7CiAgY29uc3QgYnV5RmVlPWZlZUZvcihidXlWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBjdXJyZW50VmFsdWU9Y3VyKnNoYXJlczsKICBjb25zdCBzZWxsRmVlPWZlZUZvcihjdXJyZW50VmFsdWUsaC5mZWVfbW9kZSk7CiAgY29uc3QgaW52ZXN0ZWQ9YnV5VmFsdWUrYnV5RmVlOwogIGNvbnN0IG5ldE5vdz1jdXJyZW50VmFsdWUtc2VsbEZlZS1pbnZlc3RlZDsKICBjb25zdCBuZXROb3dQY3Q9aW52ZXN0ZWQ/bmV0Tm93L2ludmVzdGVkKjEwMDpudWxsOwoKICBjb25zdCBzdG9wUHJpY2U9Y29zdCooMS1OdW1iZXIoaC5zdG9wX3BjdCkvMTAwKTsKICBjb25zdCB0YWtlUHJpY2U9Y29zdCooMStOdW1iZXIoaC50YWtlX3BjdCkvMTAwKTsKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGNvbnN0IGhpZ2gyMD1OdW1iZXIoaC5oaWdoXzIwZHx8Y3VyKTsKICBjb25zdCB0cmFpbFByaWNlPWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vawogICAgPyBoaWdoMjAqKDEtTnVtYmVyKGgudHJhaWxfcGN0KS8xMDApCiAgICA6IG51bGw7CgogIGNvbnN0IHN0b3BWYWx1ZT1zdG9wUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHRha2VWYWx1ZT10YWtlUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHN0b3BOZXQ9c3RvcFZhbHVlLWZlZUZvcihzdG9wVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CiAgY29uc3QgdGFrZU5ldD10YWtlVmFsdWUtZmVlRm9yKHRha2VWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKCiAgcmV0dXJuIHtidXlWYWx1ZSxidXlGZWUsY3VycmVudFZhbHVlLHNlbGxGZWUsaW52ZXN0ZWQsbmV0Tm93LG5ldE5vd1BjdCxzdG9wUHJpY2UsdGFrZVByaWNlLHRyYWlsUHJpY2Usc3RvcE5ldCx0YWtlTmV0fTsKfQoKCmNvbnN0IG5hbWVUaW1lcnM9e307CmNvbnN0IG5hbWVDYWNoZT17fTsKCmZ1bmN0aW9uIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeD0nJyl7CiAgaWYoIXRhcmdldClyZXR1cm47CiAgaWYoIWluZm98fCFpbmZvLm5hbWUpewogICAgdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIHJldHVybjsKICB9CiAgbGV0IGV4dHJhcz1bXTsKICBpZihpbmZvLm1hcmtldClleHRyYXMucHVzaChpbmZvLm1hcmtldCk7CiAgaWYoaW5mby5zZWN0b3IzMylleHRyYXMucHVzaChpbmZvLnNlY3RvcjMzKTsKICB0YXJnZXQuaW5uZXJIVE1MPSc8Yj4nK3ByZWZpeCtpbmZvLm5hbWUrJzwvYj4nKyhleHRyYXMubGVuZ3RoPyc8YnI+PHNwYW4gY2xhc3M9Im11dGVkIj4nK2V4dHJhcy5qb2luKCcgLyAnKSsnPC9zcGFuPic6JycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRDb21wYW55KGNvZGUpewogIGNvbnN0IGM9U3RyaW5nKGNvZGV8fCcnKS50cmltKCk7CiAgaWYoIWMpcmV0dXJuIG51bGw7CiAgaWYobmFtZUNhY2hlW2NdKXJldHVybiBuYW1lQ2FjaGVbY107CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvc2VjdXJpdHk/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+mKmOafhOWQjeOCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIG5hbWVDYWNoZVtjXT14OwogIHJldHVybiB4Owp9CgpmdW5jdGlvbiBzY2hlZHVsZUNvbXBhbnlMb29rdXAoaW5wdXRJZCx0YXJnZXRJZCxwcmVmaXg9JycpewogIGNsZWFyVGltZW91dChuYW1lVGltZXJzW2lucHV0SWRdKTsKICBjb25zdCBjPSQoaW5wdXRJZCkudmFsdWUudHJpbSgpOwogIGNvbnN0IHRhcmdldD0kKHRhcmdldElkKTsKCiAgaWYoYy5sZW5ndGg8NCl7CiAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya4oCUJzsKICAgIHJldHVybjsKICB9CgogIG5hbWVUaW1lcnNbaW5wdXRJZF09c2V0VGltZW91dChhc3luYygpPT57CiAgICB0cnl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9J+mKmOafhOWQjeOCkueiuuiqjeS4reKApic7CiAgICAgIGNvbnN0IGluZm89YXdhaXQgZ2V0Q29tcGFueShjKTsKICAgICAgZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4KTsKICAgIH1jYXRjaChlKXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICB9CiAgfSw0NTApOwp9CgoKCmZ1bmN0aW9uIHByaW9yaXR5QWN0aW9uRm9yKGgpewogIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogIGNvbnN0IG5hbWU9aC5jb21wYW55X25hbWV8fCcnOwogIGNvbnN0IGxhYmVsPShoLmNvZGV8fCcnKSsobmFtZT8nICcrbmFtZTonJyk7CiAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwoKICBpZighdmFsaWQpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTYsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOiflrp/jg4fjg7zjgr/jgpLmm7TmlrAnLAogICAgICBkZXRhaWw6J+acgOaWsOWPluW+l+e1guWApOOBjOOBguOCiuOBvuOBm+OCk+OAguOBvuOBmuOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44CCJywKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3IoaC5hc29mKTsKICBpZighZnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTksCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifjg4fjg7zjgr/prq7luqbjgpLnorroqo0nLAogICAgICBkZXRhaWw6YCR7ZnJlc2huZXNzVGV4dChoLmFzb2YpfSDlrp/pmpvjga7nj77lnKjlgKTjgpLlhYjjgavnorroqo3jgIJgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IHN0b3BEaXN0PShjdXIvYy5zdG9wUHJpY2UtMSkqMTAwOwogIGNvbnN0IHRha2VEaXN0PShjLnRha2VQcmljZS9jdXItMSkqMTAwOwogIGNvbnN0IHRyYWlsRGlzdD0oY3VyL2MudHJhaWxQcmljZS0xKSoxMDA7CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6MTAwLAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5pCN5YiH44KK44Op44Kk44Oz5Yiw6YGUJywKICAgICAgZGV0YWlsOmDmnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4oY3VyKX0gLyDmkI3liIfjgorlj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoc3RvcERpc3Q8PTMpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTQsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOOBguOBqCAke3N0b3BEaXN0LnRvRml4ZWQoMil9JSDjgafmkI3liIfjgorlj4LogIPjg6njgqTjg7NgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKGN1cj49Yy50YWtlUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTAsCiAgICAgIGNsczoncHJpb3JpdHktdGFrZScsCiAgICAgIHRpdGxlOifliKnnorrjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOWIqeeiuuWPguiAgyAke3llbihjLnRha2VQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZih0YWtlRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo4NCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7dGFrZURpc3QudG9GaXhlZCgyKX0lIOOBp+WIqeeiuuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBpZighaGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjc4LAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOifkvqHmoLzlsaXmrbTjgpLmm7TmlrDlvoXjgaEnLAogICAgICBkZXRhaWw6YOePvuWcqOWApCAke2guYXNvZnx8J+KAlCd9IC8g5L6h5qC85bGl5q20ICR7aC5oaXN0b3J5X2Fzb2Z8fCfigJQnfeOAguWbuuWumuS+oeagvOODqeOCpOODs+S7peWkluOBruWIpOaWreOBr+S/neeVmeOAgmAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoZC5sYWJlbD09PSforabmiJInKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjgwLAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOiforabmiJLliKTlrponLAogICAgICBkZXRhaWw6ZC5yZWFzb24sCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoTnVtYmVyLmlzRmluaXRlKHRyYWlsRGlzdCkmJnRyYWlsRGlzdDw9Mi41KXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjc2LAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOifjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOODiOODrOODvOODquODs+OCsOWPguiAgyAke3llbihjLnRyYWlsUHJpY2UpfSDjgb7jgacgJHt0cmFpbERpc3QudG9GaXhlZCgyKX0lYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICByZXR1cm4gewogICAgc2NvcmU6MzAsCiAgICBjbHM6J3ByaW9yaXR5LWdvb2QnLAogICAgdGl0bGU6J+mAmuW4uOebo+imlicsCiAgICBkZXRhaWw6ZC5yZWFzb258fCfoqK3lrprjg6njgqTjg7PlhoUnLAogICAgbGFiZWwKICB9Owp9CgpmdW5jdGlvbiByZW5kZXJQcmlvcml0eUFjdGlvbnMoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwcmlvcml0eUFjdGlvbnMnKTsKICBpZighYm94KXJldHVybjsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go44CB5YSq5YWI44GX44Gm56K66KqN44GZ44KL6YqY5p+E44KS6Ieq5YuV6KGo56S644GX44G+44GZ44CCPC9wPic7CiAgICByZXR1cm47CiAgfQoKICBsZXQgYWN0aW9ucz1hLm1hcChwcmlvcml0eUFjdGlvbkZvcik7CgogIC8vIENvbmNlbnRyYXRpb24gYWxlcnQgKHBvcnRmb2xpby1sZXZlbCkKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wJiZoLmFzb2YpOwogIGNvbnN0IHRvdGFsPXZhbGlkLnJlZHVjZSgocyxoKT0+cytOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApLDApOwogIGlmKHRvdGFsPjApewogICAgbGV0IG1heEhvbGRpbmc9bnVsbCwgbWF4VmFsdWU9MDsKICAgIHZhbGlkLmZvckVhY2goaD0+ewogICAgICBjb25zdCB2PU51bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCk7CiAgICAgIGlmKHY+bWF4VmFsdWUpe21heFZhbHVlPXY7bWF4SG9sZGluZz1ofQogICAgfSk7CiAgICBjb25zdCBjb25jZW50cmF0aW9uPW1heFZhbHVlL3RvdGFsKjEwMDsKICAgIGlmKGNvbmNlbnRyYXRpb24+PTYwICYmIG1heEhvbGRpbmcpewogICAgICBhY3Rpb25zLnB1c2goewogICAgICAgIHNjb3JlOjcyLAogICAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgICB0aXRsZTon6ZuG5Lit5bqm44KS56K66KqNJywKICAgICAgICBkZXRhaWw6YCR7bWF4SG9sZGluZy5jb2RlfSR7bWF4SG9sZGluZy5jb21wYW55X25hbWU/JyAnK21heEhvbGRpbmcuY29tcGFueV9uYW1lOicnfSDjgYzjg53jg7zjg4jjg5Xjgqnjg6rjgqrjga4gJHtjb25jZW50cmF0aW9uLnRvRml4ZWQoMSl9JWAsCiAgICAgICAgbGFiZWw6J+ODneODvOODiOODleOCqeODquOCqicKICAgICAgfSk7CiAgICB9CiAgfQoKICBhY3Rpb25zLnNvcnQoKHgseSk9Pnkuc2NvcmUteC5zY29yZSk7CgogIGNvbnN0IGltcG9ydGFudD1hY3Rpb25zLmZpbHRlcih4PT54LnNjb3JlPj03MCk7CiAgY29uc3Qgc2hvd249KGltcG9ydGFudC5sZW5ndGg/aW1wb3J0YW50OmFjdGlvbnMpLnNsaWNlKDAsNCk7CgogIGJveC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9InByaW9yaXR5LXdyYXAiPiR7CiAgICBzaG93bi5tYXAoKHgsaSk9PmAKICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktaXRlbSAke3guY2xzfSI+CiAgICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktbGluZSI+CiAgICAgICAgICA8ZGl2PgogICAgICAgICAgICA8c3BhbiBjbGFzcz0icHJpb3JpdHktcmFuayI+UFJJT1JJVFkgJHtpKzF9PC9zcGFuPgogICAgICAgICAgICA8Yj4ke3gudGl0bGV9PC9iPgogICAgICAgICAgPC9kaXY+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1jb2RlIj4ke3gubGFiZWx9PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjVweCI+JHt4LmRldGFpbH08L2Rpdj4KICAgICAgPC9kaXY+CiAgICBgKS5qb2luKCcnKQogIH08L2Rpdj5gICsgKAogICAgaW1wb3J0YW50Lmxlbmd0aAogICAgICA/ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+4oC75YSq5YWI5bqm44Gv6Kit5a6a44Op44Kk44Oz5o6l6L+R44O75Yik5a6a54q25oWL44O744OH44O844K/5pyJ54Sh44O76ZuG5Lit5bqm44GL44KJ5L2c44KL56K66KqN6aCG44Gn44GZ44CC6Ieq5YuV5aOy6LK35oyH56S644Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPicKICAgICAgOiAnPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPue3iuaApeW6puOBrumrmOOBhOmgheebruOBr+OBguOCiuOBvuOBm+OCk+OAgumAmuW4uOebo+imluOCkue2mee2muOAgjwvcD4nCiAgKTsKfQoKZnVuY3Rpb24gcG9ydGZvbGlvTWVhbkZvcihhLGtleSl7CiAgY29uc3QgdmFsaWQ9YS5maWx0ZXIoaD0+TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCk7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw8PTApcmV0dXJuIG51bGw7CgogIGxldCBudW09MCwgZGVuPTA7CiAgdmFsaWQuZm9yRWFjaChoPT57CiAgICBjb25zdCBzdD1zdGF0Rm9yKGgsa2V5KTsKICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGlmKHN0JiZzdC5zdGF0dXM9PT0nb2snJiZOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHN0Lm1lYW4pKSYmdj4wKXsKICAgICAgbnVtICs9IHYqTnVtYmVyKHN0Lm1lYW4pOwogICAgICBkZW4gKz0gdjsKICAgIH0KICB9KTsKICByZXR1cm4gZGVuPjA/bnVtL2RlbjpudWxsOwp9CgpmdW5jdGlvbiByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBib3g9JCgncG9ydGZvbGlvU3VtbWFyeScpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwogICAgcmV0dXJuOwogIH0KCiAgbGV0IHRvdGFsQ29zdD0wLCB0b3RhbFZhbHVlPTAsIHRvdGFsTmV0PTA7CiAgY29uc3Qgcm93cz1bXTsKICBjb25zdCBkZWNpc2lvbnM9e2hvbGQ6MCx3YXRjaDowLHRha2U6MCxzdG9wOjAscGVuZGluZzowfTsKCiAgYS5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogICAgY29uc3QgY3VycmVudFZhbHVlPXZhbGlkP2N1cipzaGFyZXM6MDsKICAgIGNvbnN0IGludmVzdGVkPU51bWJlcihoLmNvc3R8fDApKnNoYXJlcytjLmJ1eUZlZTsKCiAgICB0b3RhbENvc3QgKz0gaW52ZXN0ZWQ7CgogICAgaWYodmFsaWQpewogICAgICB0b3RhbFZhbHVlICs9IGN1cnJlbnRWYWx1ZTsKICAgICAgdG90YWxOZXQgKz0gYy5uZXROb3c7CgogICAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICAgIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylkZWNpc2lvbnMuc3RvcCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yip56K65qSc6KiOJylkZWNpc2lvbnMudGFrZSsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n6K2m5oiSJylkZWNpc2lvbnMud2F0Y2grKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmSd8fGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjlsaXmrbTlj6TjgYTvvIknKWRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIGVsc2UgZGVjaXNpb25zLmhvbGQrKzsKCiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6Y3VycmVudFZhbHVlLG5ldDpjLm5ldE5vd30pOwogICAgfWVsc2V7CiAgICAgIGRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6MCxuZXQ6bnVsbH0pOwogICAgfQogIH0pOwoKICBjb25zdCBuZXRQY3Q9dG90YWxDb3N0PjA/dG90YWxOZXQvdG90YWxDb3N0KjEwMDpudWxsOwogIGNvbnN0IG1heFZhbHVlPXJvd3MucmVkdWNlKChtLHIpPT5NYXRoLm1heChtLHIudmFsdWUpLDApOwogIGNvbnN0IGNvbmNlbnRyYXRpb249dG90YWxWYWx1ZT4wP21heFZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CgogIGNvbnN0IG1lYW4yMD1wb3J0Zm9saW9NZWFuRm9yKGEsJzIwZCcpOwogIGNvbnN0IG1lYW4xMjY9cG9ydGZvbGlvTWVhbkZvcihhLCcxMjZkJyk7CiAgY29uc3QgbWVhbjI1Mj1wb3J0Zm9saW9NZWFuRm9yKGEsJzI1MmQnKTsKCiAgY29uc3Qgc3RhbGVIb2xkaW5ncz1hLmZpbHRlcihoPT57CiAgICBjb25zdCBmPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogICAgcmV0dXJuIGguYXNvZiAmJiAhZi5kZWNpc2lvbl9vazsKICB9KTsKICBjb25zdCBzdGFsZU5vdGljZT1zdGFsZUhvbGRpbmdzLmxlbmd0aAogICAgPyBgPGRpdiBjbGFzcz0iZnJlc2hib3ggZnJlc2gtc3RhbGUiIHN0eWxlPSJtYXJnaW4tYm90dG9tOjlweCI+PGI+8J+bkSDlj6TjgYTmoKrkvqHjg4fjg7zjgr8gJHtzdGFsZUhvbGRpbmdzLmxlbmd0aH3pipjmn4Q8L2I+PGRpdiBjbGFzcz0ibXV0ZWQiPuipleS+oemhjeODu+aQjeebiuOBr+acgOaWsOWPluW+l+e1guWApOODmeODvOOCueOBruWPguiAg+WApOOBp+OBmeOAguS7iuaXpeOBruWjsuiyt+WIpOaWreOBq+OBr+S9v+OCj+OBmuOAgeWun+mam+OBruePvuWcqOWApOOCkueiuuiqjeOBl+OBpuOBj+OBoOOBleOBhOOAgjwvZGl2PjwvZGl2PmAKICAgIDogJyc7CgogIGNvbnN0IHN0YWxlSGlzdG9yeT1hLmZpbHRlcihoPT57CiAgICBjb25zdCBmPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICAgIHJldHVybiAoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZikgJiYgIWYuZGVjaXNpb25fb2s7CiAgfSk7CiAgY29uc3QgaGlzdG9yeU5vdGljZT1zdGFsZUhpc3RvcnkubGVuZ3RoCiAgICA/IGA8ZGl2IGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9Im1hcmdpbi1ib3R0b206OXB4Ij48Yj7wn5OaIOS+oeagvOWxpeattOOBjOWPpOOBhCAke3N0YWxlSGlzdG9yeS5sZW5ndGh96YqY5p+EPC9iPjxkaXYgY2xhc3M9Im11dGVkIj7kvqHmoLzlsaXmrbTjgYzlj6TjgYTloLTlkIjjgIHnn63kuK3plbfmnJ/jg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjg7vpnIDntaZwcm94eeOBr+WPguiAg+WApOOBp+OBmeOAgjwvZGl2PjwvZGl2PmAKICAgIDogJyc7CgogIGNvbnN0IGFsbG9jYXRpb25zPXJvd3MKICAgIC5maWx0ZXIocj0+ci52YWx1ZT4wKQogICAgLnNvcnQoKHgseSk9PnkudmFsdWUteC52YWx1ZSkKICAgIC5tYXAocj0+ewogICAgICBjb25zdCB3PXRvdGFsVmFsdWU+MD9yLnZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CiAgICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYWxsb2MiPgogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj48Yj4ke3IuY29kZX08L2I+JHtyLm5hbWU/JyAnK3IubmFtZTonJ30gLyAke3cudG9GaXhlZCgxKX0lPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0iYWxsb2NiYXIiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke01hdGgubWluKDEwMCx3KX0lIj48L3NwYW4+PC9kaXY+CiAgICAgIDwvZGl2PmA7CiAgICB9KS5qb2luKCcnKTsKCiAgYm94LmlubmVySFRNTD1gCiAgICAke3N0YWxlTm90aWNlfQogICAgJHtoaXN0b3J5Tm90aWNlfQogICAgPGRpdiBjbGFzcz0icG9ydHJvdyI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+aKleizh+mhjTwvc3Bhbj48Yj4ke3llbih0b3RhbENvc3QpfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+W5b6X57WC5YCk44OZ44O844K56KmV5L6h6aGNPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbFZhbHVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWPluW+l+e1guWApOODmeODvOOCueaQjeebijwvc3Bhbj48YiBjbGFzcz0iJHt0b3RhbE5ldD49MD8ncG9zJzonbmVnJ30iPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbE5ldCk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHtuZXRQY3Q9PT1udWxsPyfigJQnOm5ldFBjdC50b0ZpeGVkKDIpKyclJ308L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuacgOWkp+mKmOafhOavlOeOhzwvc3Bhbj48Yj4ke3RvdGFsVmFsdWU+MD9jb25jZW50cmF0aW9uLnRvRml4ZWQoMSkrJyUnOifigJQnfTwvYj48L2Rpdj4KICAgIDwvZGl2PgoKICAgIDxkaXYgY2xhc3M9InBvcnRyb3ciIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS/neaciee2mee2mjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy5ob2xkfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6K2m5oiSPC9zcGFuPjxiPiR7ZGVjaXNpb25zLndhdGNofTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K65qSc6KiOPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnRha2V9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoov5L+d55WZPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnN0b3ArZGVjaXNpb25zLnBlbmRpbmd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TiiDoqZXkvqHpoY3liqDph43jga7pgY7ljrvlubPlnYfjg6rjgr/jg7zjg7M8L2g0PgogICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+55+t5pyfMjDml6U8L3NwYW4+PGI+JHttZWFuMjA9PT1udWxsPyfigJQnOm1lYW4yMC50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7kuK3mnJ8xMjbml6U8L3NwYW4+PGI+JHttZWFuMTI2PT09bnVsbD8n4oCUJzptZWFuMTI2LnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPumVt+acnzI1MuaXpTwvc3Bhbj48Yj4ke21lYW4yNTI9PT1udWxsPyfigJQnOm1lYW4yNTIudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgPC9kaXY+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+WQhOmKmOafhOOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODs+OCkuePvuWcqOOBruipleS+oemhjeOBp+WKoOmHjeOBl+OBn+WPguiAg+WApOOBp+OBmeOAguebuOmWouOCkuiAg+aFruOBl+OBn+WwhuadpeS6iOa4rOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgNXB4Ij7wn5OmIOmKmOafhOani+aIkDwvaDQ+CiAgICAke2FsbG9jYXRpb25zfHwnPHAgY2xhc3M9Im11dGVkIj7lrp/jg4fjg7zjgr/mnKrlj5blvpc8L3A+J30KICBgOwogIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwp9CmZ1bmN0aW9uIHJlbmRlckhvbGRpbmdzKCl7CiAgcmVuZGVyRGFpbHlBZHZpY2UoKTsKICBpZigkKCdtb3ZlbWVudFVwJykpcmVuZGVyTW92ZW1lbnQoKTsKICBjb25zdCBvcGVuZWQ9bmV3IFNldChBcnJheS5mcm9tKCQoJ2hvbGRpbmdzJykucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlsc1tvcGVuXScpKS5tYXAoZWw9PmVsLmRhdGFzZXQuc3RvY2spKTsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGlmKCFhLmxlbmd0aCl7JCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7cmVuZGVyUG9ydGZvbGlvU3VtbWFyeSgpO3JldHVybn0KICAkKCdob2xkaW5ncycpLmlubmVySFRNTD1hLm1hcCgoaCxpKT0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IHZhbGlkUHJpY2U9TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCYmaC5hc29mOwogICAgY29uc3QgY2xzPXZhbGlkUHJpY2U/KGMubmV0Tm93Pj0wPydwb3MnOiduZWcnKTonJzsKICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKCiAgICByZXR1cm4gYDxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIiBkYXRhLXN0b2NrPSIke3dhdGNoRXNjYXBlKGguY29kZSl9IiAke29wZW5lZC5oYXMoU3RyaW5nKGguY29kZSkpPydvcGVuJzonJ30+PHN1bW1hcnk+JHt3YXRjaEVzY2FwZShoLmNvbXBhbnlfbmFtZXx8aC5jb2RlKX08L3N1bW1hcnk+PGRpdiBjbGFzcz0iaG9sZGluZyI+CiAgICAgIDxkaXYgY2xhc3M9ImhvbGRpbmctaGVhZCI+CiAgICAgICAgPGRpdj4KICAgICAgICAgIDxiPiR7aC5jb2RlfTwvYj4KICAgICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj4ke2guY29tcGFueV9uYW1lfHwiIn08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gc2Vjb25kYXJ5IiBvbmNsaWNrPSJlZGl0SG9sZGluZ1NoYXJlcygke2l9KSI+5qCq5pWw5aSJ5pu0PC9idXR0b24+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hIb2xkaW5nKCR7aX0pIj7mm7TmlrA8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlSG9sZGluZygke2l9KSI+5YmK6ZmkPC9idXR0b24+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuePvuWcqOWApOODh+ODvOOCv+aXpSAke2guYXNvZnx8J+KAlCd9IC8g5pyA5paw5Y+W5b6X57WC5YCkICR7dmFsaWRQcmljZT95ZW4oaC5jdXJyZW50X3ByaWNlKTon4oCUJ30gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5qCq5L6h44K944O844K5ICR7aC5wcmljZV9zb3VyY2V8fCfigJQnfSAvIOS+oeagvOWxpeattCAke2guaGlzdG9yeV9hc29mfHwn4oCUJ30gJHtoLmhpc3Rvcnlfc291cmNlPycoJytoLmhpc3Rvcnlfc291cmNlKycpJzonJ308L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuWGjeioiOeulyAke2gudXBkYXRlZF9hdD9uZXcgRGF0ZShoLnVwZGF0ZWRfYXQpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpOifigJQnfSAvIOaQjeebiuODu+WbuuWumuODqeOCpOODs+i3nembouOBr+OBk+OBruacgOaWsOWPluW+l+e1guWApOOCkuS9v+eUqDwvZGl2PgogICAgICAke3ZhbGlkUHJpY2U/YDxkaXYgY2xhc3M9ImZyZXNoYm94ICR7ZnJlc2huZXNzRm9yKGguYXNvZikuY2xzfSI+PGI+JHsKICAgICAgICBmcmVzaG5lc3NGb3IoaC5hc29mKS5sZXZlbD09PSdmcmVzaCc/J+KchSDprq7luqZPSyc6CiAgICAgICAgZnJlc2huZXNzRm9yKGguYXNvZikubGV2ZWw9PT0nd2FybmluZyc/J+KaoO+4jyDpgYXlu7bms6jmhI8nOgogICAgICAgICfwn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvycKICAgICAgfTwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+JHtmcmVzaG5lc3NUZXh0KGguYXNvZil9PC9kaXY+PC9kaXY+YDonJ30KCiAgICAgIDxkaXYgY2xhc3M9ImRlY2lzaW9uICR7ZC5jbHN9Ij4KICAgICAgICAke2QubGFiZWx9CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjRweCI+JHtkLnJlYXNvbn08L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMuc3RvcFByaWNlLCdzdG9wJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMudGFrZVByaWNlLCd0YWtlJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKTlrprkv6HpoLzluqY8L3NwYW4+CiAgICAgICAgICA8Yj4ke2QuY29uZmlkZW5jZX0lPC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0iZ2F1Z2UiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke2QuY29uZmlkZW5jZX0lIj48L3NwYW4+PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPuKAu+WIpOWumuS/oemgvOW6puOBr+OAgeODh+ODvOOCv+WFhei2s+W6puODu+acn+mWk+ODiOODrOODs+ODieOBruS4gOiHtOW6puODu+acn+W+heWApOe1seioiOOBruacieeEoeOBi+OCieS9nOOCi+WPguiAg+aMh+aomeOBp+OAgeeahOS4reeiuueOh+OBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICAgIDxkaXYgY2xhc3M9ImFjdGlvbmJveCI+CiAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7ku4rjganjgYbjgZnjgovvvJ88L3NwYW4+CiAgICAgICAgPGIgc3R5bGU9ImRpc3BsYXk6YmxvY2s7bWFyZ2luLXRvcDozcHgiPiR7YWN0aW9uVGV4dChoLGMsZCl9PC9iPgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7Y2xzfSI+JHt2YWxpZFByaWNlP3llbihjLm5ldE5vdyk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt2YWxpZFByaWNlP2ZtdChjLm5ldE5vd1BjdCkrJyUnOifigJQnfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6LK35LuY5omL5pWw5paZPC9zcGFuPjxiPiR7eWVuKGMuYnV5RmVlKX08L2I+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWjsuWNtOaJi+aVsOaWmSjku4opPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy5zZWxsRmVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnN0b3BQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMuc3RvcE5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy50YWtlUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnRha2VOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844Oq44Oz44Kw5Y+C6ICDPC9zcGFuPjxiPiR7dmFsaWRQcmljZSYmYy50cmFpbFByaWNlIT09bnVsbD95ZW4oYy50cmFpbFByaWNlKTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke2ZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKS5kZWNpc2lvbl9vaz8nMjDml6Xpq5jlgKTln7rmupYnOiflsaXmrbTjgYzlj6TjgYTjgZ/jgoHkv53nlZknfTwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn46vIOWun+e4vuODmeODvOOCueS+oeagvOODrOODs+OCuDwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke3JhbmdlSHRtbCgn55+t5pyfIDIw5pelJyxjdXIsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+S4reacnyAxMjbml6UnLGN1cixzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+mVt+acnyAyNTLml6UnLGN1cixzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TkCDlrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtldkh0bWwoJ+efreacnyAyMOaXpScsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+S4reacnyAxMjbml6UnLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke2V2SHRtbCgn6ZW35pyfIDI1MuaXpScsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KICAgICAgPHAgY2xhc3M9Im11dGVkIj7igLvkvqHmoLzjg6zjg7Pjgrjjg7vmnJ/lvoXlgKTjga/lsIbmnaXkuojmuKzjgafjga/jgarjgY/jgIHlj5blvpflj6/og73jgarpgY7ljrvmoKrkvqHjga7jg63jg7zjg6rjg7PjgrDliY3mlrnjg6rjgr/jg7zjg7PliIbluIPjgpLmnIDmlrDlj5blvpfntYLlgKTjgavlvZPjgabjga/jgoHjgZ/ntbHoqIjlj4LogIPjgafjgZnjgILmnJ/plpPjgYzph43jgarjgovmqJnmnKzjgpLlkKvjgb/jgb7jgZnjgII8L3A+CiAgICA8L2Rpdj48L2RldGFpbHM+YDsKICB9KS5qb2luKCcnKTsKICByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldFF1b3RlKGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3F1b3RlP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCBxPWF3YWl0IHIuanNvbigpOwogIGlmKHEuc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IocS5yZWFzb258fHEuZXJyb3J8fCflrp/jg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4gcTsKfQoKYXN5bmMgZnVuY3Rpb24gYWRkSG9sZGluZygpewogIGNvbnN0IGNvZGU9JCgnaG9sZENvZGUnKS52YWx1ZS50cmltKCk7CiAgY29uc3QgY29zdD12YWwoJ2hvbGRDb3N0JyksIHNoYXJlcz12YWwoJ2hvbGRTaGFyZXMnKTsKICBjb25zdCBzdG9wPXZhbCgnc3RvcFBjdCcpLCB0YWtlPXZhbCgndGFrZVBjdCcpLCB0cmFpbD12YWwoJ3RyYWlsUGN0Jyk7CiAgY29uc3QgZmVlTW9kZT0kKCdmZWVNb2RlJykudmFsdWU7CiAgaWYoIWNvZGV8fCFjb3N0fHwhc2hhcmVzKXthbGVydCgn6YqY5p+E44Kz44O844OJ44O75Y+W5b6X5Y2Y5L6h44O75qCq5pWw44KS5YWl5Yqb44GX44Gm44GtJyk7cmV0dXJufQogIGNvbnN0IGJ0bj1ldmVudD8udGFyZ2V0OyBpZihidG4pe2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJ30KICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICAgIGNvbnN0IGg9ewogICAgICBjb2RlLAogICAgICBjb21wYW55X25hbWU6KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHwnJywKICAgICAgY29tcGFueV9tYXJrZXQ6KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fCcnLAogICAgICBjb21wYW55X3NlY3RvcjMzOihxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fCcnLAogICAgICBjb3N0LCBzaGFyZXMsIGZlZV9tb2RlOmZlZU1vZGUsCiAgICAgIHN0b3BfcGN0OnN0b3A/PzgsIHRha2VfcGN0OnRha2U/PzE1LCB0cmFpbF9wY3Q6dHJhaWw/PzcsCiAgICAgIGN1cnJlbnRfcHJpY2U6cy5sYXN0X2Nsb3NlLCBoaWdoXzIwZDpzLmhpZ2hfMjBkLCBsb3dfMjBkOnMubG93XzIwZCwKICAgICAgcmV0dXJuXzIwZDpzLnJldHVybl8yMGQsIHJldHVybl8xMjZkOnMucmV0dXJuXzEyNmQsIHJldHVybl8yNTJkOnMucmV0dXJuXzI1MmQsCiAgICAgIGZvcndhcmRfc3RhdHM6cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30sCiAgICAgIGFzb2Y6cy5sYXN0X2RhdGUsCiAgICAgIGhpc3RvcnlfYXNvZjpzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSwKICAgICAgcHJpY2Vfc291cmNlOnMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8JycsCiAgICAgIGhpc3Rvcnlfc291cmNlOnMuaGlzdG9yeV9zb3VyY2V8fCcnLAogICAgICB1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSwKICAgICAgcHJpY2Vfc3luY2VkOnRydWUKICAgIH07CiAgICBjb25zdCBpZHg9YS5maW5kSW5kZXgoeD0+eC5jb2RlPT09Y29kZSk7CiAgICBpZihpZHg+PTApYVtpZHhdPWg7IGVsc2UgYS5wdXNoKGgpOwogICAgc2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpOwogICAgcmVuZGVySG9sZGluZ3MoKTsKICB9Y2F0Y2goZSl7YWxlcnQoJ+WPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UpfQogIGZpbmFsbHl7aWYoYnRuKXtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZgnfX0KfQoKCmZ1bmN0aW9uIGFwcGx5UXVvdGVUb0hvbGRpbmcoaCxxKXsKICBjb25zdCBzPShxJiZxLnNuYXBzaG90KXx8e307CiAgaC5jb21wYW55X25hbWU9KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHxoLmNvbXBhbnlfbmFtZXx8Jyc7CiAgaC5jb21wYW55X21hcmtldD0ocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8aC5jb21wYW55X21hcmtldHx8Jyc7CiAgaC5jb21wYW55X3NlY3RvcjMzPShxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fGguY29tcGFueV9zZWN0b3IzM3x8Jyc7CiAgaC5jdXJyZW50X3ByaWNlPXMubGFzdF9jbG9zZTsKICBoLmhpZ2hfMjBkPXMuaGlnaF8yMGQ7CiAgaC5sb3dfMjBkPXMubG93XzIwZDsKICBoLnJldHVybl8yMGQ9cy5yZXR1cm5fMjBkOwogIGgucmV0dXJuXzEyNmQ9cy5yZXR1cm5fMTI2ZDsKICBoLnJldHVybl8yNTJkPXMucmV0dXJuXzI1MmQ7CiAgaC5mb3J3YXJkX3N0YXRzPXMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9OwogIGguYXNvZj1zLmxhc3RfZGF0ZTsKICBoLmhpc3RvcnlfYXNvZj1zLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZTsKICBoLnByaWNlX3NvdXJjZT1zLnByaWNlX3NvdXJjZXx8cS5zb3VyY2V8fCcnOwogIGguaGlzdG9yeV9zb3VyY2U9cy5oaXN0b3J5X3NvdXJjZXx8Jyc7CiAgaC51cGRhdGVkX2F0PW5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTsKICBoLnByaWNlX3N5bmNlZD10cnVlOwogIHJldHVybiBoOwp9CgpmdW5jdGlvbiBzeW5jQW5hbHl6ZWRRdW90ZVRvSG9sZGluZyhjb2RlLHEpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgaT1hLmZpbmRJbmRleCh4PT5TdHJpbmcoeC5jb2RlKT09PVN0cmluZyhjb2RlKSk7CiAgaWYoaTwwKXJldHVybiBmYWxzZTsKICBhW2ldPWFwcGx5UXVvdGVUb0hvbGRpbmcoYVtpXSxxKTsKICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgcmVuZGVySG9sZGluZ3MoKTsKICByZXR1cm4gdHJ1ZTsKfQoKYXN5bmMgZnVuY3Rpb24gcmVmcmVzaEFsbEhvbGRpbmdzKGZvcmNlPWZhbHNlKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IHN0YXR1cz0kKCdob2xkaW5nUmVmcmVzaFN0YXR1cycpOwogIGNvbnN0IGJ0bj0kKCdyZWZyZXNoQWxsQnRuJyk7CgogIGlmKCFhLmxlbmd0aCl7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PSfkv53mnInmoKrjga/mnKrnmbvpjLLjgafjgZnjgIInOwogICAgcmV0dXJuOwogIH0KCiAgY29uc3Qga2V5PSdmcmVlX2hvbGRpbmdzX2xhc3RfYXV0b19yZWZyZXNoX3YxNic7CiAgY29uc3QgbGFzdD1OdW1iZXIobG9jYWxTdG9yYWdlLmdldEl0ZW0oa2V5KXx8MCk7CiAgY29uc3Qgbm93TXM9RGF0ZS5ub3coKTsKICBjb25zdCB3YWl0TXM9MzAqNjAqMTAwMDsKCiAgaWYoIWZvcmNlICYmIGxhc3QgJiYgbm93TXMtbGFzdDx3YWl0TXMpewogICAgY29uc3QgbWluPU1hdGguY2VpbCgod2FpdE1zLShub3dNcy1sYXN0KSkvNjAwMDApOwogICAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1g6Ieq5YuV5pu05paw5riI44G/44CC5qyh44Gu6Ieq5YuV5pu05paw44G+44Gn57SEJHttaW595YiG44CCYDsKICAgIHJldHVybjsKICB9CgogIGlmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSfmm7TmlrDkuK3igKYnfQogIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9YOS/neacieagqiAke2EubGVuZ3RofemKmOafhOOBruacgOaWsOe1guWApOOCkuWPluW+l+S4reKApmA7CgogIGxldCBvaz0wLCBuZz0wOwogIGZvcihsZXQgaT0wO2k8YS5sZW5ndGg7aSsrKXsKICAgIHRyeXsKICAgICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShhW2ldLmNvZGUpOwogICAgICBhW2ldPWFwcGx5UXVvdGVUb0hvbGRpbmcoYVtpXSxxKTsKICAgICAgb2srKzsKICAgIH1jYXRjaChlKXsKICAgICAgbmcrKzsKICAgICAgYVtpXS5sYXN0X3JlZnJlc2hfZXJyb3I9U3RyaW5nKGUubWVzc2FnZXx8ZSk7CiAgICB9CiAgfQoKICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oa2V5LFN0cmluZyhEYXRlLm5vdygpKSk7CiAgcmVuZGVySG9sZGluZ3MoKTsKCiAgaWYoc3RhdHVzKXsKICAgIHN0YXR1cy50ZXh0Q29udGVudD1g5pyA5paw57WC5YCk44Gn5YaN6KiI566X77ya5oiQ5YqfICR7b2t96YqY5p+EJHtuZz9gIC8g5aSx5pWXICR7bmd96YqY5p+EYDonJ33jgIJgOwogIH0KICBpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+S/neacieagquOCkuacgOaWsOe1guWApOOBp+S4gOaLrOabtOaWsCd9Cn0KCmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hIb2xkaW5nKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyksIGg9YVtpXTsgaWYoIWgpcmV0dXJuOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoaC5jb2RlKTsKICAgIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhoLHEpOwogICAgc2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpOwogICAgcmVuZGVySG9sZGluZ3MoKTsKICAgIGNvbnN0IHN0YXR1cz0kKCdob2xkaW5nUmVmcmVzaFN0YXR1cycpOwogICAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1gJHtoLmNvZGV9IOOCkuacgOaWsOWPluW+l+e1guWApOOBp+WGjeioiOeul+OBl+OBvuOBl+OBn+OAgmA7CiAgfWNhdGNoKGUpe2FsZXJ0KCfmm7TmlrDjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KfQpmdW5jdGlvbiBlZGl0SG9sZGluZ1NoYXJlcyhpKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpLCBoPWFbaV07CiAgaWYoIWgpcmV0dXJuOwogIGNvbnN0IGlucHV0PXByb21wdChgJHtoLmNvZGV9IOOBruaWsOOBl+OBhOS/neacieagquaVsOOCkuWFpeWKm+OBl+OBpuOBre+8iDHmoKrku6XkuIrjga7mlbTmlbDvvIlgLFN0cmluZyhoLnNoYXJlcykpOwogIGlmKGlucHV0PT09bnVsbClyZXR1cm47CiAgY29uc3QgdGV4dD1pbnB1dC50cmltKCk7CiAgY29uc3Qgc2hhcmVzPU51bWJlcih0ZXh0KTsKICBpZighL15bMC05XSskLy50ZXN0KHRleHQpfHwhTnVtYmVyLmlzU2FmZUludGVnZXIoc2hhcmVzKXx8c2hhcmVzPDEpewogICAgYWxlcnQoJ+agquaVsOOBrzHku6XkuIrjga7mlbTmlbDjgaflhaXlipvjgZfjgabjga0nKTtyZXR1cm47CiAgfQogIGguc2hhcmVzPXNoYXJlczsKICBoLnNoYXJlc191cGRhdGVkX2F0PW5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTsKICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgcmVuZGVySG9sZGluZ3MoKTsKICBjb25zdCBzdGF0dXM9JCgnaG9sZGluZ1JlZnJlc2hTdGF0dXMnKTsKICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWAke2guY29kZX0g44KSICR7c2hhcmVzfeagquOBq+WkieabtOOBl+OAgeaQjeebiuODu+aJi+aVsOaWmeODu+S/neacieWFqOS9k+OBrumbhuioiOOCkuWGjeioiOeul+OBl+OBvuOBl+OBn+OAgmA7Cn0KZnVuY3Rpb24gcmVtb3ZlSG9sZGluZyhpKXtjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpO2Euc3BsaWNlKGksMSk7c2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpO3JlbmRlckhvbGRpbmdzKCl9CgpmdW5jdGlvbiB3YXRjaEVzY2FwZSh2YWx1ZSl7CiAgcmV0dXJuIFN0cmluZyh2YWx1ZT8/JycpLnJlcGxhY2UoL1smPD4iJ10vZyxjPT4oeycmJzonJmFtcDsnLCc8JzonJmx0OycsJz4nOicmZ3Q7JywnIic6JyZxdW90OycsIiciOicmIzM5Oyd9W2NdKSk7Cn0KY29uc3Qgd2F0Y2hCdXN5PW5ldyBTZXQoKTsKbGV0IHdhdGNoQnVsa0J1c3k9ZmFsc2U7CmxldCBtb3ZlbWVudEJ1c3k9ZmFsc2U7CmZ1bmN0aW9uIHJlbmRlcldhdGNoKCl7CiAgcmVuZGVyRGFpbHlBZHZpY2UoKTsKICBpZigkKCdtb3ZlbWVudFVwJykpcmVuZGVyTW92ZW1lbnQoKTsKICBjb25zdCBidWxrQnRuPSQoJ3dhdGNoQnVsa0J0bicpOwogIGlmKGJ1bGtCdG4pe2J1bGtCdG4uZGlzYWJsZWQ9bW92ZW1lbnRCdXN5fHx3YXRjaEJ1bGtCdXN5fHx3YXRjaEJ1c3kuc2l6ZT4wO2J1bGtCdG4udGV4dENvbnRlbnQ9d2F0Y2hCdWxrQnVzeT8n5LiA5ous5YiG5p6Q5Lit4oCmJzon6LK344GE5pmC44Gu5Y+C6ICD44KS5LiA5ous5pu05pawJ30KICBjb25zdCBvcGVuZWQ9bmV3IFNldChBcnJheS5mcm9tKCQoJ3dhdGNocycpLnF1ZXJ5U2VsZWN0b3JBbGwoJ2RldGFpbHNbb3Blbl0nKSkubWFwKGVsPT5lbC5kYXRhc2V0LnN0b2NrKSk7CiAgY29uc3QgY2FjaGU9bG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfd2F0Y2hfYW5hbHlzaXNfdjEnKTsKICBsZXQgYW5hbHlzZXM9e307dHJ5e2FuYWx5c2VzPUpTT04ucGFyc2UoY2FjaGV8fCd7fScpfWNhdGNoKGUpe30KICAkKCd3YXRjaHMnKS5pbm5lckhUTUw9bG9jYWwoJ2ZyZWVfd2F0Y2gnKS5tYXAoKHgsaSk9PnsKICAgIGNvbnN0IGNvZGU9U3RyaW5nKHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpOwogICAgY29uc3QgbmFtZT10eXBlb2YgeD09PSdzdHJpbmcnPycnOngubmFtZTsKICAgIGNvbnN0IHRpbWluZz13YXRjaFRpbWluZyhhbmFseXNlc1tjb2RlXSk7CiAgICBjb25zdCBiYWRnZUNsYXNzPXRpbWluZy5sYWJlbD09PSfosrfjgYTlgJnoo5zvvIjmnaHku7bkuIDoh7TvvIknPyd3YXRjaC1idXknOnRpbWluZy5sYWJlbD09PSfmp5jlrZDoposnPyd3YXRjaC1uZXV0cmFsJzond2F0Y2gtcGVuZGluZyc7CiAgICByZXR1cm4gYDxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIiBkYXRhLXN0b2NrPSIke3dhdGNoRXNjYXBlKGNvZGUpfSIgJHtvcGVuZWQuaGFzKGNvZGUpPydvcGVuJzonJ30+CiAgICAgIDxzdW1tYXJ5PiR7d2F0Y2hFc2NhcGUobmFtZXx8Y29kZSl9IDxzcGFuIGNsYXNzPSJ3YXRjaC10aW1pbmcgJHtiYWRnZUNsYXNzfSI+JHt3YXRjaEJ1c3kuaGFzKGNvZGUpPyfliIbmnpDkuK3igKYnOndhdGNoRXNjYXBlKHRpbWluZy5sYWJlbCl9PC9zcGFuPjwvc3VtbWFyeT4KICAgICAgPGRpdiBjbGFzcz0id2F0Y2gtY29udGVudCI+PGI+JHt3YXRjaEVzY2FwZShjb2RlKX0gJHt3YXRjaEVzY2FwZShuYW1lfHwnJyl9PC9iPgogICAgICAgIDxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Im1hcmdpbjoxMHB4IDAiPjxidXR0b24gY2xhc3M9InNtYWxsYnRuIHNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaFdhdGNoKCR7aX0pIiAke3dhdGNoQnVsa0J1c3l8fHdhdGNoQnVzeS5oYXMoY29kZSk/J2Rpc2FibGVkJzonJ30+JHt3YXRjaEJ1c3kuaGFzKGNvZGUpPyfliIbmnpDkuK3igKYnOifosrfjgYTmmYLjga7lj4LogIPjgpLmm7TmlrAnfTwvYnV0dG9uPjxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlV2F0Y2goJHtpfSkiPuWJiumZpDwvYnV0dG9uPjwvZGl2PgogICAgICAgICR7d2F0Y2hBbmFseXNpc0h0bWwoYW5hbHlzZXNbY29kZV0pfQogICAgICA8L2Rpdj48L2RldGFpbHM+YDsKICB9KS5qb2luKCcnKXx8JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7Cn0KZnVuY3Rpb24gd2F0Y2hUaW1pbmcoYSl7CiAgaWYoIWEpcmV0dXJuIHtsYWJlbDon5pyq5YiG5p6QJyxyZWFzb246J+OAjOiyt+OBhOaZguOBruWPguiAg+OCkuabtOaWsOOAjeOBp+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBvuOBmeOAgid9OwogIGNvbnN0IHM9YS5zbmFwc2hvdHx8e30sc2M9YS5zY29yZXx8e307CiAgaWYoIWZyZXNobmVzc0ZvcihzLmxhc3RfZGF0ZSkuZGVjaXNpb25fb2t8fCFmcmVzaG5lc3NGb3Iocy5oaXN0b3J5X2xhc3RfZGF0ZXx8cy5sYXN0X2RhdGUpLmRlY2lzaW9uX29rfHxkYXRhQWdlRGF5cyhzLmxhc3RfZGF0ZSk+NHx8ZGF0YUFnZURheXMocy5oaXN0b3J5X2xhc3RfZGF0ZXx8cy5sYXN0X2RhdGUpPjQpCiAgICByZXR1cm4ge2xhYmVsOifliKTlrprkv53nlZknLHJlYXNvbjon5qCq5L6h44O75L6h5qC85bGl5q2044GM5Y+k44GE44CB44G+44Gf44Gv5pel5LuY5LiN5piO44Gn44GZ44CCJ307CiAgaWYoc2Muc2NvcmUxMDA9PW51bGx8fCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHNjLnNjb3JlMTAwKSl8fHNjLmNvdmVyYWdlX3BjdD09bnVsbHx8IU51bWJlci5pc0Zpbml0ZShOdW1iZXIoc2MuY292ZXJhZ2VfcGN0KSl8fE51bWJlcihzYy5jb3ZlcmFnZV9wY3QpPDcwfHxzLmxhc3RfY2xvc2U9PW51bGx8fCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHMubGFzdF9jbG9zZSkpfHxOdW1iZXIocy5sYXN0X2Nsb3NlKTw9MCkKICAgIHJldHVybiB7bGFiZWw6J+WIpOWumuS/neeVmScscmVhc29uOifprq7luqbjgpLliqDlkbPjgZfjgZ/jg4fjg7zjgr/lhYXotrPluqbjgYw3MCXmnKrmuoDjgIHjgb7jgZ/jga/nt4/lkIjngrnjgpLnrpflh7rjgafjgY3jgb7jgZvjgpPjgIInfTsKICBjb25zdCBwcmljZT1OdW1iZXIocy5sYXN0X2Nsb3NlKSxoaWdoPU51bWJlcihzLmhpZ2hfMjBkKSxsb3c9TnVtYmVyKHMubG93XzIwZCk7CiAgY29uc3QgcG9zaXRpb249aGlnaD5sb3c/KHByaWNlLWxvdykvKGhpZ2gtbG93KTpudWxsOwogIGlmKE51bWJlcihzYy5zY29yZTEwMCk8NDUpcmV0dXJuIHtsYWJlbDon5qeY5a2Q6KaLJyxyZWFzb246J+e3j+WQiOeCueOBjDQ154K55pyq5rqA44Gn44CB5byx44GE6KaB57Sg44GM5YSq5Yui44Gn44GZ44CCJ307CiAgaWYocG9zaXRpb24hPT1udWxsJiZwb3NpdGlvbj49MC45KXJldHVybiB7bGFiZWw6J+mrmOWApOWcj+ODu+i/veOBhOiyt+OBhOazqOaEjycscmVhc29uOicyMOaXpemWk+OBrumrmOWApOODu+WuieWApOOBruevhOWbsuOBp+S4iuS9jTEwJeOBq+S9jee9ruOBl+OBpuOBhOOBvuOBmeOAgid9OwogIGlmKE51bWJlcihzYy5zY29yZTEwMCk+PTY1JiZzLnJldHVybl8yMGQhPW51bGwmJk51bWJlcihzLnJldHVybl8yMGQpPjApCiAgICByZXR1cm4ge2xhYmVsOifosrfjgYTlgJnoo5zvvIjmnaHku7bkuIDoh7TvvIknLHJlYXNvbjon57eP5ZCI54K5NjXngrnku6XkuIrjg7syMOaXpemosOiQveeOh+ODl+ODqeOCueODu+ODh+ODvOOCv+WFhei2s+W6pjcwJeS7peS4iuOAguizvOWFpeWJjeOBq+ePvuWcqOWApOOCkueiuuiqjeOBl+OBpuOBj+OBoOOBleOBhOOAgid9OwogIHJldHVybiB7bGFiZWw6J+anmOWtkOimiycscmVhc29uOifosrfjgYTlgJnoo5zjga7mnaHku7bjgYzmj4PjgaPjgabjgYTjgb7jgZvjgpPjgIInfTsKfQpmdW5jdGlvbiB3YXRjaEFuYWx5c2lzSHRtbChhKXsKICBpZighYSlyZXR1cm4gJzxwIGNsYXNzPSJtdXRlZCI+5pyq5YiG5p6Q44CC44CM6LK344GE5pmC44Gu5Y+C6ICD44KS5pu05paw44CN44KS5oq844GX44Gm44Gt44CCPC9wPic7CiAgY29uc3Qgcz1hLnNuYXBzaG90fHx7fSxzYz1hLnNjb3JlfHx7fSx0PXdhdGNoVGltaW5nKGEpOwogIHJldHVybiBgPGRpdiBjbGFzcz0iZGVjaXNpb24gZC13YXRjaCI+JHt3YXRjaEVzY2FwZSh0LmxhYmVsKX08ZGl2IGNsYXNzPSJtdXRlZCI+JHt3YXRjaEVzY2FwZSh0LnJlYXNvbil9PC9kaXY+PC9kaXY+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuWIhuaekOabtOaWsCAke3dhdGNoRXNjYXBlKG5ldyBEYXRlKGEudXBkYXRlZF9hdCkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJykpfTxicj7mnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4ocy5sYXN0X2Nsb3NlKX0gLyDjg4fjg7zjgr/ml6UgJHt3YXRjaEVzY2FwZShzLmxhc3RfZGF0ZXx8J+KAlCcpfTxicj7kvqHmoLzlsaXmrbQgJHt3YXRjaEVzY2FwZShzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZXx8J+KAlCcpfSAvIOOCveODvOOCuSAke3dhdGNoRXNjYXBlKHMucHJpY2Vfc291cmNlfHxhLnNvdXJjZXx8J+KAlCcpfTwvcD4KICAgIDxkaXYgY2xhc3M9ImdyaWQzIj48ZGl2IGNsYXNzPSJrcGkiPue3j+WQiOeCuTxiPiR7c2Muc2NvcmUxMDA9PW51bGw/J+KAlCc6Zm10KHNjLnNjb3JlMTAwLDEpKycgLyAxMDAnfTwvYj48L2Rpdj48ZGl2IGNsYXNzPSJrcGkiPuODh+ODvOOCv+WFhei2s+W6pjxiPiR7Zm10KHNjLmNvdmVyYWdlX3BjdCwxKX0lPC9iPjwvZGl2PjxkaXYgY2xhc3M9ImtwaSI+44OI44Os44Oz44OJPGI+JHt3YXRjaEVzY2FwZShzdGF0ZUphKChhLnNpZ25hbHx8e30pLnN0YXRlKSl9PC9iPjwvZGl2PjwvZGl2PgogICAgPHA+44OG44Kv44OL44Kr44OrICR7c2NvcmVMYWJlbChzYy50ZWNobmljYWwpfSAvIOaxuueulyAke3Njb3JlTGFiZWwoc2MuZWFybmluZ3MpfSAvIOmcgOe1piAke3Njb3JlTGFiZWwoc2Muc3VwcGx5KX0gLyDlm73nrZZwcm94eSAke3Njb3JlTGFiZWwoc2MucG9saWN5KX08L3A+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPiR7d2F0Y2hFc2NhcGUoZHJpdmVyU2VudGVuY2Uoc2MpKX08L3A+CiAgICA8ZGl2IGNsYXNzPSJncmlkMyI+PGRpdiBjbGFzcz0ia3BpIj4yMOaXpemosOiQveeOhzxiPiR7cGN0KHMucmV0dXJuXzIwZCl9PC9iPjwvZGl2PjxkaXYgY2xhc3M9ImtwaSI+MTI25pel6aiw6JC9546HPGI+JHtwY3Qocy5yZXR1cm5fMTI2ZCl9PC9iPjwvZGl2PjxkaXYgY2xhc3M9ImtwaSI+MjUy5pel6aiw6JC9546HPGI+JHtwY3Qocy5yZXR1cm5fMjUyZCl9PC9iPjwvZGl2PjwvZGl2PgogICAgPHAgY2xhc3M9Im11dGVkIj415pel77yPMjDml6Xlh7rmnaXpq5jmr5QgJHtmbXQoKHMuc3VwcGx5X3Byb3h5fHx7fSkudm9sdW1lX3JhdGlvXzVfMjApfeWAjSAvIDIw5pel6auY5YCkICR7eWVuKHMuaGlnaF8yMGQpfSAvIOWuieWApCAke3llbihzLmxvd18yMGQpfTwvcD4KICAgIDxoND7lrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PjxkaXYgY2xhc3M9ImdyaWQzIj4ke2V2SHRtbCgn55+t5pyfMjDml6UnLChzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSlbJzIwZCddKX0ke2V2SHRtbCgn5Lit5pyfMTI25pelJywocy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30pWycxMjZkJ10pfSR7ZXZIdG1sKCfplbfmnJ8yNTLml6UnLChzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSlbJzI1MmQnXSl9PC9kaXY+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuiyt+OBhOWAmeijnOOBr+adoeS7tuWIpOWumuOBp+OAgeWwhuadpeOBruWIqeebiuOChOiyt+OBhOaZguOCkuS/neiovOOBmeOCi+OCguOBruOBp+OBr+OBguOCiuOBvuOBm+OCk+OAguaxuueul+ODu+WbveetluOBr+WPluW+l+OBp+OBjeOBn+imgee0oOOBruOBv+S9v+eUqOOBl+OAgeS4jeaYjuWApOOBr+ijnOOBhOOBvuOBm+OCk+OAguS+oeagvOODu+WHuuadpemrmOOBr+mBheW7tuOBmeOCi+WgtOWQiOOBjOOBguOCiuOBvuOBmeOAguacn+W+heWApOOBr+mBjuWOu+OBrumHjeikh+acn+mWk+OCkuWQq+OCgOe1seioiOWPguiAg+OBp+OBmeOAgjwvcD5gOwp9CmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hXYXRjaChpLGJ1bGs9ZmFsc2UpewogIGlmKG1vdmVtZW50QnVzeXx8d2F0Y2hCdWxrQnVzeSYmIWJ1bGspcmV0dXJuIGZhbHNlOwogIGNvbnN0IGl0ZW09bG9jYWwoJ2ZyZWVfd2F0Y2gnKVtpXTtpZihpdGVtPT09dW5kZWZpbmVkKXJldHVybjsKICBjb25zdCBjb2RlPVN0cmluZyh0eXBlb2YgaXRlbT09PSdzdHJpbmcnP2l0ZW06aXRlbS5jb2RlKTsKICBpZih3YXRjaEJ1c3kuaGFzKGNvZGUpKXJldHVybiBmYWxzZTsKICB3YXRjaEJ1c3kuYWRkKGNvZGUpO3JlbmRlcldhdGNoKCk7CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShjb2RlKSxzPXEuc25hcHNob3R8fHt9OwogICAgbGV0IGY9e3Njb3JlOm51bGwsZnJlc2huZXNzOntmYWN0b3I6MH19LHA9e3Njb3JlOm51bGwsbWF0Y2hlZF90aGVtZXM6W119OwogICAgdHJ5e2Y9YXdhaXQgZ2V0RnVuZGFtZW50YWxzKGNvZGUpfWNhdGNoKGUpe30KICAgIHRyeXtwPWF3YWl0IGdldFBvbGljeShjb2RlKX1jYXRjaChlKXt9CiAgICBjb25zdCByZXNwb25zZT1hd2FpdCBmZXRjaCgnL2FwaS9mcmVlL2FuYWx5emUnLHttZXRob2Q6J1BPU1QnLGhlYWRlcnM6eydDb250ZW50LVR5cGUnOidhcHBsaWNhdGlvbi9qc29uJ30sY2FjaGU6J25vLXN0b3JlJyxib2R5OkpTT04uc3RyaW5naWZ5KHsKICAgICAgY29kZSxwcmljZTpzLmxhc3RfY2xvc2UscmV0dXJuMjA6cy5yZXR1cm5fMjBkLHJldHVybjEyNjpzLnJldHVybl8xMjZkLHJldHVybjI1MjpzLnJldHVybl8yNTJkLAogICAgICBlYXJuaW5nc19zY29yZTpmLnNjb3JlLHBvbGljeV9zY29yZTpwLnNjb3JlLHBvbGljeV9tb2RlOidhdXRvJyxzdXBwbHlfc2NvcmU6KHMuc3VwcGx5X3Byb3h5fHx7fSkuc2NvcmUsCiAgICAgIG1hcmtldF9mcmVzaG5lc3M6bWFya2V0RnJlc2huZXNzRmFjdG9yKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlKSwKICAgICAgZWFybmluZ3NfZnJlc2huZXNzOmYuZnJlc2huZXNzJiZmLmZyZXNobmVzcy5mYWN0b3IhPT11bmRlZmluZWQ/Zi5mcmVzaG5lc3MuZmFjdG9yOihmLnNjb3JlPT1udWxsPzA6MSksCiAgICAgIHBvbGljeV9mcmVzaG5lc3M6cG9saWN5RnJlc2huZXNzRmFjdG9yKHAsZmFsc2UpCiAgICB9KX0pOwogICAgY29uc3QgcmVzdWx0PWF3YWl0IHJlc3BvbnNlLmpzb24oKTtpZighcmVzcG9uc2Uub2t8fHJlc3VsdC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcihyZXN1bHQucmVhc29ufHxyZXN1bHQuZXJyb3J8fCfliIbmnpDjgavlpLHmlZfjgZfjgb7jgZfjgZ8nKTsKICAgIGNvbnN0IGxpc3Q9bG9jYWwoJ2ZyZWVfd2F0Y2gnKTsKICAgIGNvbnN0IGluZGV4PWxpc3QuZmluZEluZGV4KHg9PlN0cmluZyh0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKT09PWNvZGUpOwogICAgaWYoaW5kZXg8MClyZXR1cm4gZmFsc2U7CiAgICBsZXQgY2FjaGU9e307dHJ5e2NhY2hlPUpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfd2F0Y2hfYW5hbHlzaXNfdjEnKXx8J3t9Jyl9Y2F0Y2goZSl7fQogICAgY2FjaGVbY29kZV09e3NuYXBzaG90OnMsc291cmNlOnEuc291cmNlLHNjb3JlOnJlc3VsdC5zY29yZSxzaWduYWw6cmVzdWx0LnNpZ25hbCx1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKX07CiAgICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgnZnJlZV93YXRjaF9hbmFseXNpc192MScsSlNPTi5zdHJpbmdpZnkoY2FjaGUpKTsKICAgIGNvbnN0IG5hbWU9KHEuY29tcGFueXx8e30pLm5hbWV8fCh0eXBlb2YgbGlzdFtpbmRleF09PT0nc3RyaW5nJz8nJzpsaXN0W2luZGV4XS5uYW1lKXx8Jyc7CiAgICBsaXN0W2luZGV4XT17Li4uKHR5cGVvZiBsaXN0W2luZGV4XT09PSdvYmplY3QnP2xpc3RbaW5kZXhdOnt9KSxjb2RlLG5hbWV9O3NhdmUoJ2ZyZWVfd2F0Y2gnLGxpc3QpOwogICAgcmV0dXJuIHRydWU7CiAgfWNhdGNoKGUpe2lmKCFidWxrKWFsZXJ0KCfjgqbjgqnjg4Pjg4HliIbmnpDjgqjjg6njg7zvvJonK2UubWVzc2FnZSsn44CC5L+d5a2Y5riI44G/57WQ5p6c44GM44GC44KM44Gw5YmN5Zue5YiG44KS6KGo56S644GX44G+44GZ44CCJyk7cmV0dXJuIGZhbHNlO30KICBmaW5hbGx5e3dhdGNoQnVzeS5kZWxldGUoY29kZSk7cmVuZGVyV2F0Y2goKX0KfQphc3luYyBmdW5jdGlvbiByZWZyZXNoQWxsV2F0Y2goKXsKICBpZihtb3ZlbWVudEJ1c3l8fHdhdGNoQnVsa0J1c3l8fHdhdGNoQnVzeS5zaXplKXJldHVybjsKICBjb25zdCBjb2Rlcz1bLi4ubmV3IFNldChsb2NhbCgnZnJlZV93YXRjaCcpLm1hcCh4PT5TdHJpbmcodHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSkpKV07CiAgY29uc3Qgc3RhdHVzPSQoJ3dhdGNoQnVsa1N0YXR1cycpOwogIGlmKCFjb2Rlcy5sZW5ndGgpe3N0YXR1cy50ZXh0Q29udGVudD0n44Km44Kp44OD44OB44Oq44K544OI44Gv5pyq55m76Yyy44Gn44GZ44CCJztyZXR1cm47fQogIHdhdGNoQnVsa0J1c3k9dHJ1ZTtyZW5kZXJXYXRjaCgpOwogIGxldCBkb25lPTAsZmFpbGVkPTAsc2tpcHBlZD0wOwogIHRyeXsKICAgIGZvcihsZXQgbj0wO248Y29kZXMubGVuZ3RoO24rKyl7CiAgICAgIGNvbnN0IGNvZGU9Y29kZXNbbl07CiAgICAgIGNvbnN0IGluZGV4PWxvY2FsKCdmcmVlX3dhdGNoJykuZmluZEluZGV4KHg9PlN0cmluZyh0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKT09PWNvZGUpOwogICAgICBpZihpbmRleDwwKXtza2lwcGVkKys7Y29udGludWU7fQogICAgICBzdGF0dXMudGV4dENvbnRlbnQ9YCR7bisxfSAvICR7Y29kZXMubGVuZ3RofemKmOafhO+8miR7Y29kZX0g44KS5YiG5p6Q5Lit4oCmYDsKICAgICAgaWYoYXdhaXQgcmVmcmVzaFdhdGNoKGluZGV4LHRydWUpKWRvbmUrKztlbHNlIGZhaWxlZCsrOwogICAgICBpZihuPGNvZGVzLmxlbmd0aC0xKXsKICAgICAgICBzdGF0dXMudGV4dENvbnRlbnQ9YCR7bisxfSAvICR7Y29kZXMubGVuZ3RofemKmOafhOOCkuWHpueQhua4iOOBv+OAguasoeOBruWIhuaekOOBvuOBp+e0hDEz56eS4oCm77yI5oiQ5YqfICR7ZG9uZX0gLyDlpLHmlZcgJHtmYWlsZWR977yJYDsKICAgICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlPT5zZXRUaW1lb3V0KHJlc29sdmUsMTMwMDApKTsKICAgICAgfQogICAgfQogICAgc3RhdHVzLnRleHRDb250ZW50PWDkuIDmi6zmm7TmlrDlrozkuobvvJrmiJDlip8gJHtkb25lfSAvIOWkseaVlyAke2ZhaWxlZH0gLyDliYrpmaTmuIjjgb8gJHtza2lwcGVkfeOAguWkseaVl+OBl+OBn+mKmOafhOOBr+WJjeWbnuOBrue1kOaenOOBjOOBguOCjOOBsOihqOekuuOBl+OBvuOBmeOAgumAlOS4reOBp+i/veWKoOOBl+OBn+mKmOafhOOBr+asoeWbnuOBruWvvuixoeOBp+OBmeOAgmA7CiAgfWNhdGNoKGUpe3N0YXR1cy50ZXh0Q29udGVudD0n5LiA5ous5pu05paw44KS5Lit5pat44GX44G+44GX44Gf77yaJytlLm1lc3NhZ2U7fQogIGZpbmFsbHl7d2F0Y2hCdWxrQnVzeT1mYWxzZTtyZW5kZXJXYXRjaCgpO30KfQpsZXQgbW92ZW1lbnRTdG9wUmVxdWVzdGVkPWZhbHNlLG1vdmVtZW50V2FpdFRpbWVyPW51bGwsbW92ZW1lbnRXYWl0UmVzb2x2ZT1udWxsOwpjb25zdCBNQVJLRVRfR0VOUkVTPXsiU2VtaWNvbmR1Y3RvcnMiOiAi5Y2K5bCO5L2T44O76Zu75a2Q5qmf5ZmoIiwgIkluZHVzdHJpYWwgRWxlY3Ryb25pY3MiOiAi5Y2K5bCO5L2T44O76Zu75a2Q5qmf5ZmoIiwgIkF1ZGlvL1ZpZGVvIEVxdWlwbWVudCI6ICLljYrlsI7kvZPjg7vpm7vlrZDmqZ/lmagiLCAiQ29tcHV0ZXJzL0NvbnN1bWVyIEVsZWN0cm9uaWNzIjogIuWNiuWwjuS9k+ODu+mbu+WtkOapn+WZqCIsICJOZXR3b3JraW5nIjogIuWNiuWwjuS9k+ODu+mbu+WtkOapn+WZqCIsICJQcmVjaXNpb24gUHJvZHVjdHMiOiAi5Y2K5bCO5L2T44O76Zu75a2Q5qmf5ZmoIiwgIldhdGNoZXMvQ2xvY2tzL1BhcnRzIjogIuWNiuWwjuS9k+ODu+mbu+WtkOapn+WZqCIsICJDb21wdXRlciBTZXJ2aWNlcyI6ICJJVOODu+mAmuS/oSIsICJTb2Z0d2FyZSI6ICJJVOODu+mAmuS/oSIsICJJbnRlcm5ldC9PbmxpbmUiOiAiSVTjg7vpgJrkv6EiLCAiV2lyZWQgVGVsZWNvbW11bmljYXRpb25zIFNlcnZpY2VzIjogIklU44O76YCa5L+hIiwgIldpcmVsZXNzIFRlbGVjb21tdW5pY2F0aW9ucyBTZXJ2aWNlcyI6ICJJVOODu+mAmuS/oSIsICJBdXRvICYgQ29tbWVyY2lhbCBWZWhpY2xlIFBhcnRzIjogIuiHquWLlei7iuODu+i8uOmAgeapn+WZqCIsICJBdXRvbW9iaWxlcyI6ICLoh6rli5Xou4rjg7vovLjpgIHmqZ/lmagiLCAiQ29tbWVyY2lhbCBWZWhpY2xlcyI6ICLoh6rli5Xou4rjg7vovLjpgIHmqZ/lmagiLCAiVGlyZXMiOiAi6Ieq5YuV6LuK44O76Ly46YCB5qmf5ZmoIiwgIkFlcm9zcGFjZSBQcm9kdWN0cy9QYXJ0cyI6ICLoh6rli5Xou4rjg7vovLjpgIHmqZ/lmagiLCAiRGVmZW5zZSBFcXVpcG1lbnQvUHJvZHVjdHMiOiAi6Ieq5YuV6LuK44O76Ly46YCB5qmf5ZmoIiwgIkluZHVzdHJpYWwgTWFjaGluZXJ5IjogIuapn+aisOODu+eUo+alreioreWCmSIsICJJbmR1c3RyaWFsIFByb2R1Y3RzIjogIuapn+aisOODu+eUo+alreioreWCmSIsICJNb2JpbGUgTWFjaGluZXJ5IjogIuapn+aisOODu+eUo+alreioreWCmSIsICJFbGVjdHJpYyBVdGlsaXRpZXMiOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIkdhcyBVdGlsaXRpZXMiOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIk11bHRpdXRpbGl0aWVzIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJSZW5ld2FibGUgRW5lcmd5IEdlbmVyYXRpb24iOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIk1ham9yIE9pbCAmIEdhcyI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiT2lsICYgR2FzIFByb2R1Y3RzL1NlcnZpY2VzIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJPaWwgRXh0cmFjdGlvbiI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiQ29hbCI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiQWx1bWludW0iOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIkNvbW1vZGl0eSBDaGVtaWNhbHMiOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIlNwZWNpYWx0eSBDaGVtaWNhbHMiOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIk5vbi1GZXJyb3VzIE1ldGFscyI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiSXJvbi9TdGVlbCI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiR2VuZXJhbCBNaW5pbmciOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIlByZWNpb3VzIE1ldGFscyI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiUGFwZXIvUHVscCI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiQ29udGFpbmVycy9QYWNrYWdpbmciOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIkJpb3RlY2hub2xvZ3kiOiAi5Yy76Jas5ZOB44O75Yy755mCIiwgIlBoYXJtYWNldXRpY2FscyI6ICLljLvolqzlk4Hjg7vljLvnmYIiLCAiTWVkaWNhbCBFcXVpcG1lbnQvU3VwcGxpZXMiOiAi5Yy76Jas5ZOB44O75Yy755mCIiwgIkhlYWx0aGNhcmUgUHJvdmlzaW9uIjogIuWMu+iWrOWTgeODu+WMu+eZgiIsICJEcnVnIFJldGFpbCI6ICLljLvolqzlk4Hjg7vljLvnmYIiLCAiQmFua2luZyI6ICLph5Hono3jg7vkv53pmboiLCAiTWFqb3IgSW50ZXJuYXRpb25hbCBCYW5rcyI6ICLph5Hono3jg7vkv53pmboiLCAiQ29uc3VtZXIgRmluYW5jZSI6ICLph5Hono3jg7vkv53pmboiLCAiRmluYW5jZSBDb21wYW5pZXMiOiAi6YeR6J6N44O75L+d6Zm6IiwgIkZ1bGwtTGluZSBJbnN1cmFuY2UiOiAi6YeR6J6N44O75L+d6Zm6IiwgIkxpZmUgSW5zdXJhbmNlIjogIumHkeiejeODu+S/nemZuiIsICJOb24tTGlmZSBJbnN1cmFuY2UiOiAi6YeR6J6N44O75L+d6Zm6IiwgIkludmVzdG1lbnQgQWR2aXNvcnMiOiAi6YeR6J6N44O75L+d6Zm6IiwgIk1vcnRnYWdlcyI6ICLph5Hono3jg7vkv53pmboiLCAiU2VjdXJpdGllcyI6ICLph5Hono3jg7vkv53pmboiLCAiQ29uc3RydWN0aW9uIjogIuW7uuioreODu+S4jeWLleeUoyIsICJSZXNpZGVudGlhbCBCdWlsZGluZyBDb25zdHJ1Y3Rpb24iOiAi5bu66Kit44O75LiN5YuV55SjIiwgIkJ1aWxkaW5nIE1hdGVyaWFscy9Qcm9kdWN0cyI6ICLlu7roqK3jg7vkuI3li5XnlKMiLCAiUmVhbCBFc3RhdGUgQWdlbnRzL0Jyb2tlcnMiOiAi5bu66Kit44O75LiN5YuV55SjIiwgIlJlYWwgRXN0YXRlIERldmVsb3BlcnMiOiAi5bu66Kit44O75LiN5YuV55SjIiwgIkZvb2QgUHJvZHVjdHMiOiAi6aOf5ZOB44O76L6y5p6X5rC055SjIiwgIkZhcm1pbmciOiAi6aOf5ZOB44O76L6y5p6X5rC055SjIiwgIkZpc2hpbmciOiAi6aOf5ZOB44O76L6y5p6X5rC055SjIiwgIkFsY29ob2xpYyBCZXZlcmFnZXMvRHJpbmtzIjogIumjn+WTgeODu+i+suael+awtOeUoyIsICJOb24tQWxjb2hvbGljIEJldmVyYWdlcy9Ecmlua3MiOiAi6aOf5ZOB44O76L6y5p6X5rC055SjIiwgIlRvYmFjY28iOiAi6aOf5ZOB44O76L6y5p6X5rC055SjIiwgIkNsb3RoaW5nIFJldGFpbCI6ICLlsI/lo7Ljg7vlpJbpo58iLCAiRm9vZCBSZXRhaWwiOiAi5bCP5aOy44O75aSW6aOfIiwgIkhvbWUgR29vZHMgUmV0YWlsIjogIuWwj+WjsuODu+WklumjnyIsICJNaXhlZCBSZXRhaWxpbmciOiAi5bCP5aOy44O75aSW6aOfIiwgIlJlc3RhdXJhbnRzIjogIuWwj+WjsuODu+WklumjnyIsICJTcGVjaWFsdHkgUmV0YWlsIjogIuWwj+WjsuODu+WklumjnyIsICJDbG90aGluZyI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiRm9vdHdlYXIiOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIkZ1cm5pdHVyZSI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiSG91c2V3YXJlcyI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiTm9uZHVyYWJsZSBIb3VzZWhvbGQgUHJvZHVjdHMiOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIlBlcnNvbmFsIENhcmUgUHJvZHVjdHMvQXBwbGlhbmNlcyI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiU3BvcnRzIEdvb2RzIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJMZWlzdXJlIEdvb2RzIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJBaXIgRnJlaWdodCI6ICLpgYvovLjjg7vnianmtYEiLCAiUGFzc2VuZ2VyIEFpcmxpbmVzIjogIumBi+i8uOODu+eJqea1gSIsICJQYXNzZW5nZXIgVHJhbnNwb3J0LCBPdGhlciI6ICLpgYvovLjjg7vnianmtYEiLCAiUmFpbHJvYWRzIjogIumBi+i8uOODu+eJqea1gSIsICJUcmFuc3BvcnRhdGlvbiBTZXJ2aWNlcyI6ICLpgYvovLjjg7vnianmtYEiLCAiVHJ1Y2tpbmciOiAi6YGL6Ly444O754mp5rWBIiwgIldhdGVyIFRyYW5zcG9ydC9TaGlwcGluZyI6ICLpgYvovLjjg7vnianmtYEiLCAiQnJvYWRjYXN0aW5nIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJHYW1ibGluZyBJbmR1c3RyaWVzIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJIb3RlbHMiOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIk1vdGlvbiBQaWN0dXJlL1NvdW5kIFJlY29yZGluZyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiUHJpbnRpbmciOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIlB1Ymxpc2hpbmciOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIlJlY3JlYXRpb25hbCBTZXJ2aWNlcyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiVG91cmlzbSI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiVG95cyAmIEdhbWVzIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJXaG9sZXNhbGVycyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiQWNjb3VudGluZyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiQWR2ZXJ0aXNpbmcvTWFya2V0aW5nL1B1YmxpYyBSZWxhdGlvbnMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkNvbnN1bWVyIFNlcnZpY2VzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJEaXZlcnNpZmllZCBCdXNpbmVzcyBTZXJ2aWNlcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiRGl2ZXJzaWZpZWQgSG9sZGluZyBDb21wYW5pZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkVtcGxveW1lbnQvVHJhaW5pbmcgU2VydmljZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkVudmlyb25tZW50L1dhc3RlIE1hbmFnZW1lbnQiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkdlbmVyYWwgU2VydmljZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIlRlY2huaWNhbCBTZXJ2aWNlcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiV2F0ZXIgVXRpbGl0aWVzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJTaGVsbCBjb21wYW5pZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkNsb3NlZC1FbmQgRnVuZHMiOiAiRVRG44O744OV44Kh44Oz44OJIiwgIkV4Y2hhbmdlLVRyYWRlZCBGdW5kcyI6ICJFVEbjg7vjg5XjgqHjg7Pjg4kiLCAiTXV0dWFsICYgT3RoZXIgRnVuZHMiOiAiRVRG44O744OV44Kh44Oz44OJIn07CmZ1bmN0aW9uIHN0b2NrR2VucmUoc3RvY2spe3JldHVybiBNQVJLRVRfR0VOUkVTW3N0b2NrLnNlY3Rvcl18fCfjgZ3jga7ku5bjg7vmpa3nqK7kuI3mmI4nfQpmdW5jdGlvbiBzZWxlY3RlZE1hcmtldEdlbnJlKCl7cmV0dXJuIGxvY2FsU3RvcmFnZS5nZXRJdGVtKCdmcmVlX21hcmtldF9nZW5yZV92MScpfHwn5YWo5qWt56iuJ30KZnVuY3Rpb24gYWxsTWFya2V0U3RvY2tzKCl7cmV0dXJuIGxvY2FsKCdmcmVlX21hcmtldF91bml2ZXJzZV92MScpfQpmdW5jdGlvbiByZW5kZXJNYXJrZXRHZW5yZXMoKXsKICBjb25zdCBzZWxlY3Q9JCgnbWFya2V0R2VucmUnKTtpZighc2VsZWN0KXJldHVybjsKICBjb25zdCBzZWxlY3RlZD1zZWxlY3RlZE1hcmtldEdlbnJlKCksY291bnRzPW5ldyBNYXAoKTsKICBmb3IoY29uc3Qgc3RvY2sgb2YgYWxsTWFya2V0U3RvY2tzKCkpe2NvbnN0IGdlbnJlPXN0b2NrR2VucmUoc3RvY2spO2NvdW50cy5zZXQoZ2VucmUsKGNvdW50cy5nZXQoZ2VucmUpfHwwKSsxKX0KICBzZWxlY3QuaW5uZXJIVE1MPSc8b3B0aW9uIHZhbHVlPSLlhajmpa3nqK4iPuWFqOalreeoru+8iCcrYWxsTWFya2V0U3RvY2tzKCkubGVuZ3RoKyfpipjmn4TvvIk8L29wdGlvbj4nK1suLi5jb3VudHMua2V5cygpXS5zb3J0KChhLGIpPT5hLmxvY2FsZUNvbXBhcmUoYiwnamEnKSkubWFwKGdlbnJlPT5gPG9wdGlvbiB2YWx1ZT0iJHt3YXRjaEVzY2FwZShnZW5yZSl9Ij4ke3dhdGNoRXNjYXBlKGdlbnJlKX3vvIgke2NvdW50cy5nZXQoZ2VucmUpfemKmOafhO+8iTwvb3B0aW9uPmApLmpvaW4oJycpOwogIGlmKHNlbGVjdGVkIT09J+WFqOalreeoricmJiFjb3VudHMuaGFzKHNlbGVjdGVkKSlsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgnZnJlZV9tYXJrZXRfZ2VucmVfdjEnLCflhajmpa3nqK4nKTsKICBzZWxlY3QudmFsdWU9c2VsZWN0ZWRNYXJrZXRHZW5yZSgpO3NlbGVjdC5kaXNhYmxlZD1tb3ZlbWVudEJ1c3l8fCFhbGxNYXJrZXRTdG9ja3MoKS5sZW5ndGg7CiAgJCgnbWFya2V0R2VucmVMb2FkJykuZGlzYWJsZWQ9bW92ZW1lbnRCdXN5Owp9CmZ1bmN0aW9uIGNoYW5nZU1hcmtldEdlbnJlKCl7CiAgaWYobW92ZW1lbnRCdXN5KXJldHVybjsKICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgnZnJlZV9tYXJrZXRfZ2VucmVfdjEnLCQoJ21hcmtldEdlbnJlJykudmFsdWUpOwogICQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9c2VsZWN0ZWRNYXJrZXRHZW5yZSgpKyfjgavliIfjgormm7/jgYjjgb7jgZfjgZ/jgILlgJnoo5zjgajlt6Hlm57lr77osaHjgpLjgZPjga7jgrjjg6Pjg7Pjg6vjgavntZ7jgorjgb7jgZnjgIInOwogIHJlbmRlck1vdmVtZW50KCk7Cn0KYXN5bmMgZnVuY3Rpb24gbG9hZE1hcmtldEdlbnJlcygpewogIGNvbnN0IGJ0bj0kKCdtYXJrZXRHZW5yZUxvYWQnKTtidG4uZGlzYWJsZWQ9dHJ1ZTsKICB0cnl7YXdhaXQgZW5zdXJlTWFya2V0VW5pdmVyc2UoKTtyZW5kZXJNb3ZlbWVudCgpOyQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9J+OCuOODo+ODs+ODq+S4gOimp+OCkuiqreOBv+i+vOOBv+OBvuOBl+OBn+OAguiqv+OBueOBn+OBhOOCuOODo+ODs+ODq+OCkumBuOOCk+OBp+OBreOAgid9CiAgY2F0Y2goZSl7JCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD1lLm1lc3NhZ2V9CiAgZmluYWxseXtidG4uZGlzYWJsZWQ9bW92ZW1lbnRCdXN5O30KfQpmdW5jdGlvbiBtYXJrZXRDdXJzb3JLZXkoKXtyZXR1cm4gJ2ZyZWVfbWFya2V0X2N1cnNvcl9nZW5yZV92MTonK3NlbGVjdGVkTWFya2V0R2VucmUoKX0KZnVuY3Rpb24gcmVnaXN0ZXJlZE1vdmVtZW50U3RvY2tzKCl7Y29uc3QgZ2VucmU9c2VsZWN0ZWRNYXJrZXRHZW5yZSgpO3JldHVybiBhbGxNYXJrZXRTdG9ja3MoKS5maWx0ZXIoc3RvY2s9PmdlbnJlPT09J+WFqOalreeorid8fHN0b2NrR2VucmUoc3RvY2spPT09Z2VucmUpfQphc3luYyBmdW5jdGlvbiBlbnN1cmVNYXJrZXRVbml2ZXJzZSgpewogIGlmKGFsbE1hcmtldFN0b2NrcygpLmxlbmd0aClyZXR1cm47CiAgY29uc3QgcmVzcG9uc2U9YXdhaXQgZmV0Y2goJy9hcGkvZnJlZS9tYXJrZXQtdW5pdmVyc2UnLHtjYWNoZTonbm8tc3RvcmUnfSkscmVzdWx0PWF3YWl0IHJlc3BvbnNlLmpzb24oKTsKICBpZighcmVzcG9uc2Uub2t8fHJlc3VsdC5zdGF0dXMhPT0nb2snfHwhQXJyYXkuaXNBcnJheShyZXN1bHQuc3RvY2tzKXx8IXJlc3VsdC5zdG9ja3MubGVuZ3RoKXRocm93IG5ldyBFcnJvcihyZXN1bHQucmVhc29ufHwn6YqY5p+E5LiA6Kan44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgc2F2ZSgnZnJlZV9tYXJrZXRfdW5pdmVyc2VfdjEnLHJlc3VsdC5zdG9ja3MpOwogIGxvY2FsU3RvcmFnZS5zZXRJdGVtKCdmcmVlX21hcmtldF91bml2ZXJzZV9mZXRjaGVkJyxyZXN1bHQuZmV0Y2hlZF9hdCk7Cn0KZnVuY3Rpb24gc3RvcE1vdmVtZW50KCl7CiAgbW92ZW1lbnRTdG9wUmVxdWVzdGVkPXRydWU7CiAgaWYobW92ZW1lbnRXYWl0VGltZXIhPT1udWxsKWNsZWFyVGltZW91dChtb3ZlbWVudFdhaXRUaW1lcik7CiAgaWYobW92ZW1lbnRXYWl0UmVzb2x2ZSl7bW92ZW1lbnRXYWl0UmVzb2x2ZSgpO21vdmVtZW50V2FpdFJlc29sdmU9bnVsbDt9CiAgJCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD0n5YGc5q2i5Lit4oCm5Y+W5b6X5Lit44GuMemKmOafhOOBjOWujOS6huOBl+OBn+OCieatouOBvuOCiuOBvuOBmeOAgic7Cn0KZnVuY3Rpb24gbW92ZW1lbnRXYWl0KCl7cmV0dXJuIG5ldyBQcm9taXNlKHJlc29sdmU9Pnttb3ZlbWVudFdhaXRSZXNvbHZlPXJlc29sdmU7bW92ZW1lbnRXYWl0VGltZXI9c2V0VGltZW91dCgoKT0+e21vdmVtZW50V2FpdFRpbWVyPW51bGw7bW92ZW1lbnRXYWl0UmVzb2x2ZT1udWxsO3Jlc29sdmUoKX0sMTMwMDApfSl9CmZ1bmN0aW9uIGNvbXBhY3RNb3ZlbWVudFF1b3RlKHEpewogIGNvbnN0IHM9cS5zbmFwc2hvdHx8e307CiAgY29uc3QgZmllbGRzPVsnbGFzdF9kYXRlJywnbGFzdF9jbG9zZScsJ3JldHVybl8yMGQnLCdwcmljZV9zb3VyY2UnLCdoaXN0b3J5X2FkanVzdG1lbnQnXTsKICBjb25zdCBzbmFwc2hvdD1PYmplY3QuZnJvbUVudHJpZXMoZmllbGRzLm1hcChrPT5bayxzW2tdPz9udWxsXSkpOwogIHNuYXBzaG90LnN1cHBseV9wcm94eT17dm9sdW1lX3JhdGlvXzVfMjA6KHMuc3VwcGx5X3Byb3h5fHx7fSkudm9sdW1lX3JhdGlvXzVfMjA/P251bGx9OwogIHJldHVybiB7c291cmNlOnEuc291cmNlfHwnJyxjb21wYW55OntuYW1lOihxLmNvbXBhbnl8fHt9KS5uYW1lfHwnJ30sc25hcHNob3Qscm93czoocS5yb3dzfHxbXSkuc2xpY2UoLTYpLm1hcChyPT4oe2RhdGU6ci5kYXRlLGNsb3NlOnIuY2xvc2V9KSl9Owp9CmZ1bmN0aW9uIHJlYWRNb3ZlbWVudCgpe3RyeXtyZXR1cm4gSlNPTi5wYXJzZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbSgnZnJlZV9tb3ZlbWVudF92MScpfHwne30nKX1jYXRjaChlKXtyZXR1cm4ge319fQpmdW5jdGlvbiBtb3ZlbWVudE1ldHJpY3MocSl7CiAgY29uc3Qgcz0ocXx8e30pLnNuYXBzaG90fHx7fTsKICBjb25zdCByb3dzPSgocXx8e30pLnJvd3N8fFtdKS5maWx0ZXIocj0+dHlwZW9mIHIuY2xvc2U9PT0nbnVtYmVyJyYmTnVtYmVyLmlzRmluaXRlKHIuY2xvc2UpJiZyLmNsb3NlPjAmJnIuZGF0ZSkuc29ydCgoYSxiKT0+U3RyaW5nKGEuZGF0ZSkubG9jYWxlQ29tcGFyZShTdHJpbmcoYi5kYXRlKSkpOwogIGNvbnN0IGxhc3Q9cm93cy5hdCgtMSksZGF0ZT1sYXN0JiZsYXN0LmRhdGU7CiAgY29uc3QgcmV0PW49PnJvd3MubGVuZ3RoPm4/KHJvd3MuYXQoLTEpLmNsb3NlL3Jvd3MuYXQoLTEtbikuY2xvc2UtMSkqMTAwOm51bGw7CiAgY29uc3QgZGF5PXJldCgxKSxmaXZlPXJldCg1KSx0d2VudHk9dHlwZW9mIHMucmV0dXJuXzIwZD09PSdudW1iZXInJiZOdW1iZXIuaXNGaW5pdGUocy5yZXR1cm5fMjBkKT9zLnJldHVybl8yMGQ6cmV0KDIwKTsKICBjb25zdCB2b2x1bWU9KHMuc3VwcGx5X3Byb3h5fHx7fSkudm9sdW1lX3JhdGlvXzVfMjA7CiAgbGV0IGtpbmQ9J290aGVyJyxyZWFzb249J+azqOebruODu+S4i+iQveitpuaIkuOBruadoeS7tuOBq+ipsuW9k+OBl+OBvuOBm+OCk+OAgic7CiAgaWYoIWRhdGV8fGRhdGFBZ2VEYXlzKGRhdGUpPT09bnVsbHx8ZGF0YUFnZURheXMoZGF0ZSk+NHx8cy5sYXN0X2RhdGUhPT1kYXRlKXtyZWFzb249J+WxpeattOOBjOWPpOOBhOODu+S4jei2s+OAgeOBvuOBn+OBr+ePvuWcqOWApOOBqOWxpeattOOBruaXpeS7mOOBjOS4jeS4gOiHtOOAguWIpOWumuS/neeVmeOBp+OBmeOAgic7fQogIGVsc2UgaWYoZGF5PT09bnVsbHx8Zml2ZT09PW51bGwpe3JlYXNvbj0nMeWWtualreaXpeODuzXllrbmpa3ml6Xjga7mr5TovIPjgavlv4XopoHjgarlsaXmrbTjgYzkuI3otrPjgILliKTlrprkv53nlZnjgafjgZnjgIInO30KICBlbHNlIGlmKGRheT49MSYmZml2ZT4wKXtraW5kPSd1cCc7cmVhc29uPSfnm7Tov5Ex5Za25qWt5pel77yLMSXku6XkuIrjgIE15Za25qWt5pel44KC44OX44Op44K544CC5LiK5piH44Gu5YuV44GN44GM57aa44GE44Gm44GE44KL5Y+C6ICD5YCZ6KOc44CCJzt9CiAgZWxzZSBpZihkYXk8PS0xJiYoZml2ZTwwfHx0d2VudHkhPT1udWxsJiZ0d2VudHk8MCkpe2tpbmQ9J2Rvd24nO3JlYXNvbj0n55u06L+RMeWWtualreaXpeKIkjEl5Lul5LiL44CBNeWWtualreaXpeOBvuOBn+OBrzIw5Za25qWt5pel44KC44Oe44Kk44OK44K544CC5LiL5ZCR44GN44Gu5YuV44GN44Gr5rOo5oSP44CCJzt9CiAgcmV0dXJuIHtyb3dzLGRhdGUsZGF5LGZpdmUsdHdlbnR5LHZvbHVtZSxraW5kLHJlYXNvbn07Cn0KZnVuY3Rpb24gbW92ZW1lbnRDaGFydChyb3dzKXsKICBpZihyb3dzLmxlbmd0aDwyKXJldHVybiAnPHAgY2xhc3M9Im11dGVkIj7jg4Hjg6Pjg7zjg4jnlKjjga7lsaXmrbTkuI3otrM8L3A+JzsKICBjb25zdCB2YWx1ZXM9cm93cy5tYXAocj0+ci5jbG9zZSksbG93PU1hdGgubWluKC4uLnZhbHVlcyksaGlnaD1NYXRoLm1heCguLi52YWx1ZXMpOwogIGNvbnN0IHBvaW50cz12YWx1ZXMubWFwKCh2LGkpPT5gJHsoOCtpKjI4NC8odmFsdWVzLmxlbmd0aC0xKSkudG9GaXhlZCgxKX0sJHsoaGlnaD09PWxvdz80MDo3Mi0odi1sb3cpKjY0LyhoaWdoLWxvdykpLnRvRml4ZWQoMSl9YCkuam9pbignICcpOwogIGNvbnN0IGNvbG9yPXZhbHVlcy5hdCgtMSk+PXZhbHVlc1swXT8nIzE1ODAzZCc6JyNiOTFjMWMnOwogIHJldHVybiBgPHN2ZyB2aWV3Qm94PSIwIDAgMzAwIDgwIiByb2xlPSJpbWciIGFyaWEtbGFiZWw9IuebtOi/keacgOWkpzbllrbmpa3ml6Xjga7ntYLlgKTmjqjnp7siIHN0eWxlPSJ3aWR0aDoxMDAlO2hlaWdodDoxMDBweCI+PHBvbHlsaW5lIHBvaW50cz0iJHtwb2ludHN9IiBmaWxsPSJub25lIiBzdHJva2U9IiR7Y29sb3J9IiBzdHJva2Utd2lkdGg9IjIuNSIvPjwvc3ZnPjxkaXYgY2xhc3M9Im11dGVkIj4ke3dhdGNoRXNjYXBlKHJvd3NbMF0uZGF0ZSl9IOKGkiAke3dhdGNoRXNjYXBlKHJvd3MuYXQoLTEpLmRhdGUpfSAvIOacgOWuiSAke3llbihsb3cpfeODu+acgOmrmCAke3llbihoaWdoKX08L2Rpdj5gOwp9CmZ1bmN0aW9uIHJlbmRlck1vdmVtZW50KCl7CiAgcmVuZGVyRGFpbHlBZHZpY2UoKTsKICByZW5kZXJNYXJrZXRHZW5yZXMoKTsKICBjb25zdCBjYWNoZT1yZWFkTW92ZW1lbnQoKSxvcGVuZWQ9bmV3IFNldChBcnJheS5mcm9tKGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoJ2RldGFpbHNbZGF0YS1tb3ZlbWVudF1bb3Blbl0nKSkubWFwKGVsPT5lbC5kYXRhc2V0Lm1vdmVtZW50KSk7CiAgY29uc3QgdW5pdmVyc2U9cmVnaXN0ZXJlZE1vdmVtZW50U3RvY2tzKCk7CiAgY29uc3QgY2hlY2tlZD11bml2ZXJzZS5maWx0ZXIoeD0+Y2FjaGVbeC5jb2RlXSk7CiAgY29uc3QgZnJlc2g9Y2hlY2tlZC5maWx0ZXIoeD0+IWNhY2hlW3guY29kZV0uZXJyb3ImJmRhdGFBZ2VEYXlzKCgoY2FjaGVbeC5jb2RlXS5xfHx7fSkuc25hcHNob3R8fHt9KS5sYXN0X2RhdGUpIT09bnVsbCYmZGF0YUFnZURheXMoKChjYWNoZVt4LmNvZGVdLnF8fHt9KS5zbmFwc2hvdHx8e30pLmxhc3RfZGF0ZSk8PTQpLmxlbmd0aDsKICBjb25zdCBjb3ZlcmFnZT0kKCdtb3ZlbWVudENvdmVyYWdlJyk7aWYoY292ZXJhZ2UpY292ZXJhZ2UudGV4dENvbnRlbnQ9YCR7c2VsZWN0ZWRNYXJrZXRHZW5yZSgpfe+8muS4gOimpyAke3VuaXZlcnNlLmxlbmd0aH3pipjmn4QgLyDlj5blvpfoqabooYzmuIjjgb8gJHtjaGVja2VkLmxlbmd0aH0gLyA05pel5Lul5YaF44Gu44OH44O844K/ICR7ZnJlc2h9IC8g5pyq6Kq/5p+7ICR7dW5pdmVyc2UubGVuZ3RoLWNoZWNrZWQubGVuZ3RofeOAguS4gOimp+OBq+OBr+WPpOOBhOeZu+mMsuOChOWPluW+l+S4jeWPr+OBrumKmOafhOOBjOWQq+OBvuOCjOOCi+WgtOWQiOOBjOOBguOCiuOBvuOBmeOAgmA7CiAgY29uc3QgZ3JvdXBzPXt1cDpbXSxkb3duOltdLG90aGVyOltdfTsKICBmb3IoY29uc3Qgc3RvY2sgb2YgcmVnaXN0ZXJlZE1vdmVtZW50U3RvY2tzKCkpewogICAgY29uc3QgYT1jYWNoZVtzdG9jay5jb2RlXTtpZighYSljb250aW51ZTtjb25zdCBtPW1vdmVtZW50TWV0cmljcyhhJiZhLnEpOwogICAgaWYoIWF8fGEuZXJyb3Ipe20ua2luZD0nb3RoZXInO20ucmVhc29uPWEmJmEuZXJyb3I/J+WPluW+l+WkseaVl+OAguWIpOWumuS/neeVme+8iOWJjeWbnuWxpeattOOBjOOBguOCjOOBsOWPguiAg+ihqOekuu+8ieOAgic6J+acquabtOaWsOOAgic7fQogICAgZ3JvdXBzW20ua2luZF0ucHVzaCh7c3RvY2ssYSxtfSk7CiAgfQogIGdyb3Vwcy51cC5zb3J0KChhLGIpPT5iLm0uZGF5LWEubS5kYXkpO2dyb3Vwcy5kb3duLnNvcnQoKGEsYik9PmEubS5kYXktYi5tLmRheSk7CiAgY29uc3QgaWRzPXt1cDonVXAnLGRvd246J0Rvd24nLG90aGVyOidPdGhlcid9OwogIGZvcihjb25zdCBraW5kIG9mIE9iamVjdC5rZXlzKGdyb3VwcykpewogICAgJCgnbW92ZW1lbnQnK2lkc1traW5kXSsnQ291bnQnKS50ZXh0Q29udGVudD1ncm91cHNba2luZF0ubGVuZ3RoKyfku7YnOwogICAgJCgnbW92ZW1lbnQnK2lkc1traW5kXSkuaW5uZXJIVE1MPWdyb3Vwc1traW5kXS5zbGljZSgwLDUwKS5tYXAoKHtzdG9jayxhLG19KT0+ewogICAgICBjb25zdCBuYW1lPShhJiZhLnEmJmEucS5jb21wYW55JiZhLnEuY29tcGFueS5uYW1lKXx8c3RvY2submFtZXx8c3RvY2suY29kZTsKICAgICAgY29uc3QgbGFiZWw9a2luZD09PSd1cCc/J+azqOebruWAmeijnCc6a2luZD09PSdkb3duJz8n5LiL6JC96K2m5oiSJzon44Gd44Gu5LuW44O75L+d55WZJzsKICAgICAgY29uc3Qgcz0oYSYmYS5xJiZhLnEuc25hcHNob3QpfHx7fTsKICAgICAgcmV0dXJuIGA8ZGV0YWlscyBjbGFzcz0ic3RvY2stZGV0YWlscyIgZGF0YS1tb3ZlbWVudD0iJHt3YXRjaEVzY2FwZShzdG9jay5jb2RlKX0iICR7b3BlbmVkLmhhcyhzdG9jay5jb2RlKT8nb3Blbic6Jyd9PjxzdW1tYXJ5PiR7d2F0Y2hFc2NhcGUobmFtZSl9IDxzcGFuIGNsYXNzPSJ3YXRjaC10aW1pbmcgd2F0Y2gtbmV1dHJhbCI+JHtsYWJlbH0gLyAx5Za25qWt5pelICR7cGN0KG0uZGF5KX08L3NwYW4+PC9zdW1tYXJ5PjxkaXYgY2xhc3M9IndhdGNoLWNvbnRlbnQiPgogICAgICAgIDxiPiR7d2F0Y2hFc2NhcGUoc3RvY2suY29kZSl9ICR7d2F0Y2hFc2NhcGUobmFtZSl9PC9iPjxwIGNsYXNzPSJtdXRlZCI+44K444Oj44Oz44Or77yaJHt3YXRjaEVzY2FwZShzdG9ja0dlbnJlKHN0b2NrKSl9PC9wPjxwPiR7d2F0Y2hFc2NhcGUobS5yZWFzb24pfTwvcD4KICAgICAgICA8cCBjbGFzcz0ibXV0ZWQiPuWxpeattOaXpSAke3dhdGNoRXNjYXBlKG0uZGF0ZXx8J+KAlCcpfe+8iCR7bS5kYXRlJiZkYXRhQWdlRGF5cyhtLmRhdGUpPT09MD8n5pys5pel44Gu5pel6Laz44OH44O844K/Jzon5b2T5pel44OH44O844K/44Gn44Gv44GC44KK44G+44Gb44KTJ33vvIkgLyDlj5blvpcgJHt3YXRjaEVzY2FwZShhP25ldyBEYXRlKGEudXBkYXRlZF9hdCkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyk6J+KAlCcpfTxicj7kvqHmoLzjgr3jg7zjgrkgJHt3YXRjaEVzY2FwZShzLnByaWNlX3NvdXJjZXx8KGEmJmEucSYmYS5xLnNvdXJjZSl8fCfigJQnKX0gLyDlsaXmrbToqr/mlbQgJHt3YXRjaEVzY2FwZShzLmhpc3RvcnlfYWRqdXN0bWVudHx8J+eiuuiqjeOBp+OBjeOBvuOBm+OCkycpfTwvcD4KICAgICAgICAke21vdmVtZW50Q2hhcnQobS5yb3dzKX08ZGl2IGNsYXNzPSJncmlkMyI+PGRpdiBjbGFzcz0ia3BpIj4x5Za25qWt5pelPGI+JHtwY3QobS5kYXkpfTwvYj48L2Rpdj48ZGl2IGNsYXNzPSJrcGkiPjXllrbmpa3ml6U8Yj4ke3BjdChtLmZpdmUpfTwvYj48L2Rpdj48ZGl2IGNsYXNzPSJrcGkiPjIw5Za25qWt5pelPGI+JHtwY3QobS50d2VudHkpfTwvYj48L2Rpdj48L2Rpdj4KICAgICAgICA8cCBjbGFzcz0ibXV0ZWQiPjXml6XvvI8yMOaXpeW5s+Wdh+WHuuadpemrmOavlCAke2ZtdChtLnZvbHVtZSl95YCNICR7dHlwZW9mIG0udm9sdW1lPT09J251bWJlcicmJm0udm9sdW1lPj0xLjI/J++8iOWHuuadpemrmOWil+WKoO+8iSc6Jyd9IC8g5pyA5paw5bGl5q2057WC5YCkICR7eWVuKG0ucm93cy5sZW5ndGg/bS5yb3dzLmF0KC0xKS5jbG9zZTpudWxsKX08L3A+CiAgICAgIDwvZGl2PjwvZGV0YWlscz5gOwogICAgfSkuam9pbignJyl8fCc8cCBjbGFzcz0ibXV0ZWQiPuWPluW+l+a4iOOBv+OBruevhOWbsuOBq+ipsuW9k+WAmeijnOOBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4nOwogIH0KfQphc3luYyBmdW5jdGlvbiByZWZyZXNoTW92ZW1lbnQoYWxsR2VucmU9ZmFsc2UpewogIGlmKG1vdmVtZW50QnVzeXx8d2F0Y2hCdWxrQnVzeXx8d2F0Y2hCdXN5LnNpemUpeyQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9J+OBu+OBi+OBruWIhuaekOOBjOWujOS6huOBl+OBpuOBi+OCieabtOaWsOOBl+OBpuOBreOAgic7cmV0dXJuO30KICBtb3ZlbWVudEJ1c3k9dHJ1ZTttb3ZlbWVudFN0b3BSZXF1ZXN0ZWQ9ZmFsc2U7CiAgJCgnbW92ZW1lbnRBbGxCdG4nKS5kaXNhYmxlZD10cnVlOwogIGNvbnN0IGJ0bj0kKCdtb3ZlbWVudEJ0bicpO2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0Q29udGVudD0n6YqY5p+E5LiA6Kan44KS56K66KqN5Lit4oCmJzskKCdtb3ZlbWVudFN0b3AnKS5kaXNhYmxlZD1mYWxzZTtyZW5kZXJXYXRjaCgpOwogIGxldCBkb25lPTAsZmFpbGVkPTA7CiAgdHJ5ewogICAgYXdhaXQgZW5zdXJlTWFya2V0VW5pdmVyc2UoKTsKICAgIGNvbnN0IGFsbD1yZWdpc3RlcmVkTW92ZW1lbnRTdG9ja3MoKTsKICAgIGlmKCFhbGwubGVuZ3RoKXskKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PSfjgZPjga7jgrjjg6Pjg7Pjg6vjga7pipjmn4Tjga/jgYLjgorjgb7jgZvjgpPjgIInO3JldHVybjt9CiAgICBjb25zdCBjdXJzb3JLZXk9bWFya2V0Q3Vyc29yS2V5KCk7CiAgICBsZXQgY3Vyc29yPU51bWJlcihsb2NhbFN0b3JhZ2UuZ2V0SXRlbShjdXJzb3JLZXkpfHwoc2VsZWN0ZWRNYXJrZXRHZW5yZSgpPT09J+WFqOalreeoric/bG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfbWFya2V0X2N1cnNvcl92MScpOm51bGwpfHwwKTsKICAgIGlmKCFOdW1iZXIuaXNTYWZlSW50ZWdlcihjdXJzb3IpfHxjdXJzb3I8MHx8Y3Vyc29yPj1hbGwubGVuZ3RoKWN1cnNvcj0wOwogICAgaWYoYWxsR2VucmUpY3Vyc29yPTA7CiAgICBjb25zdCBiYXRjaD1hbGxHZW5yZT9hbGw6YWxsLnNsaWNlKGN1cnNvcixjdXJzb3IrMjApOwogICAgZm9yKGxldCBpPTA7aTxiYXRjaC5sZW5ndGg7aSsrKXsKICAgICAgaWYobW92ZW1lbnRTdG9wUmVxdWVzdGVkKWJyZWFrOwogICAgICBjb25zdCBzdG9jaz1iYXRjaFtpXTsKICAgICAgJCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD1gJHtpKzF9IC8gJHtiYXRjaC5sZW5ndGh96YqY5p+E77yI5LiA6KanICR7Y3Vyc29yK2krMX0gLyAke2FsbC5sZW5ndGh977yJ77yaJHtzdG9jay5uYW1lfHxzdG9jay5jb2RlfSDjga7lsaXmrbTjgpLlj5blvpfkuK3igKZgOwogICAgICBjb25zdCBjYWNoZT1yZWFkTW92ZW1lbnQoKTsKICAgICAgdHJ5e2NvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoc3RvY2suY29kZSk7Y2FjaGVbc3RvY2suY29kZV09e3E6Y29tcGFjdE1vdmVtZW50UXVvdGUocSksdXBkYXRlZF9hdDpuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksZXJyb3I6bnVsbH07ZG9uZSsrO30KICAgICAgY2F0Y2goZSl7Y2FjaGVbc3RvY2suY29kZV09ey4uLihjYWNoZVtzdG9jay5jb2RlXXx8e30pLHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpLGVycm9yOlN0cmluZyhlLm1lc3NhZ2UpfTtmYWlsZWQrKzt9CiAgICAgIGxvY2FsU3RvcmFnZS5zZXRJdGVtKCdmcmVlX21vdmVtZW50X3YxJyxKU09OLnN0cmluZ2lmeShjYWNoZSkpOwogICAgICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbShjdXJzb3JLZXksU3RyaW5nKChjdXJzb3IraSsxKSVhbGwubGVuZ3RoKSk7cmVuZGVyTW92ZW1lbnQoKTsKICAgICAgaWYoaTxiYXRjaC5sZW5ndGgtMSYmIW1vdmVtZW50U3RvcFJlcXVlc3RlZCl7JCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD1gJHtpKzF9IC8gJHtiYXRjaC5sZW5ndGh96YqY5p+E44KS5Yem55CG5riI44G/44CC5qyh44Gu5Y+W5b6X44G+44Gn57SEMTPnp5LigKZgO2F3YWl0IG1vdmVtZW50V2FpdCgpO30KICAgIH0KICAgICQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9YCR7bW92ZW1lbnRTdG9wUmVxdWVzdGVkPyflgZzmraLjgZfjgb7jgZfjgZ8nOifku4rlm57jga7lt6Hlm57lrozkuoYnfe+8muWPluW+l+aIkOWKnyAke2RvbmV9IC8g5aSx5pWXICR7ZmFpbGVkfeOAguasoeWbnuOBr+OBk+OBruOCuOODo+ODs+ODq+OBrue2muOBjeOBi+OCieiqv+OBueOBvuOBmeOAguWAmeijnOOBr+WQhOODh+ODvOOCv+aXpeOBrue1guWApOODmeODvOOCueOBp+OAgeWFqOmKmOafhOW3oeWbnuOBq+OBr+aZgumWk+OBjOOBi+OBi+OCiuOBvuOBmeOAgmA7CiAgfWNhdGNoKGUpeyQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9J+W3oeWbnuOCkuS4reaWreOBl+OBvuOBl+OBn++8micrZS5tZXNzYWdlO30KICBmaW5hbGx5e21vdmVtZW50QnVzeT1mYWxzZTtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PSfmrKHjga4yMOmKmOafhOOCkuiqv+OBueOCiyc7JCgnbW92ZW1lbnRBbGxCdG4nKS5kaXNhYmxlZD1mYWxzZTskKCdtb3ZlbWVudFN0b3AnKS5kaXNhYmxlZD10cnVlO3JlbmRlck1vdmVtZW50KCk7cmVuZGVyV2F0Y2goKTt9Cn0KZnVuY3Rpb24gcmVtb3ZlV2F0Y2goaSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV93YXRjaCcpLCB4PWFbaV07CiAgaWYoeD09PXVuZGVmaW5lZClyZXR1cm47CiAgY29uc3QgY29kZT10eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlOwogIGlmKCFjb25maXJtKGAke2NvZGV9IOOCkuOCpuOCqeODg+ODgeODquOCueODiOOBi+OCieWJiumZpOOBl+OBvuOBmeOBi++8n2ApKXJldHVybjsKICBhLnNwbGljZShpLDEpOwogIHNhdmUoJ2ZyZWVfd2F0Y2gnLGEpOwogIHRyeXtjb25zdCBjYWNoZT1KU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKCdmcmVlX3dhdGNoX2FuYWx5c2lzX3YxJyl8fCd7fScpO2RlbGV0ZSBjYWNoZVtTdHJpbmcoY29kZSldO2xvY2FsU3RvcmFnZS5zZXRJdGVtKCdmcmVlX3dhdGNoX2FuYWx5c2lzX3YxJyxKU09OLnN0cmluZ2lmeShjYWNoZSkpfWNhdGNoKGUpe30KICByZW5kZXJXYXRjaCgpOwp9CmFzeW5jIGZ1bmN0aW9uIGFkZFdhdGNoKCl7CiAgbGV0IGM9JCgnd2F0Y2hDb2RlJykudmFsdWUudHJpbSgpOyBpZighYylyZXR1cm47CiAgbGV0IGluZm89bnVsbDsKICB0cnl7aW5mbz1hd2FpdCBnZXRDb21wYW55KGMpfWNhdGNoKGUpe30KICBsZXQgYT1sb2NhbCgnZnJlZV93YXRjaCcpOwogIGNvbnN0IGV4aXN0cz1hLnNvbWUoeD0+KHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpPT09Yyk7CiAgaWYoIWV4aXN0cylhLnB1c2goe2NvZGU6YyxuYW1lOmluZm8mJmluZm8ubmFtZT9pbmZvLm5hbWU6Jyd9KTsKICBzYXZlKCdmcmVlX3dhdGNoJyxhKTsKICByZW5kZXJXYXRjaCgpOwp9CmZ1bmN0aW9uIHVwZGF0ZUthYnV0YW4oKXtsZXQgYz0kKCdjb2RlJykudmFsdWUudHJpbSgpOyQoJ2thYnV0YW4nKS5ocmVmPWM/J2h0dHBzOi8va2FidXRhbi5qcC9zdG9jay8/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKTonaHR0cHM6Ly9rYWJ1dGFuLmpwLyd9CiQoJ2NvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9Pnt1cGRhdGVLYWJ1dGFuKCk7c2NoZWR1bGVDb21wYW55TG9va3VwKCdjb2RlJywnY29tcGFueU5hbWUnLCcnKX0pO3VwZGF0ZUthYnV0YW4oKTsKJCgnaG9sZENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnaG9sZENvZGUnLCdob2xkQ29tcGFueU5hbWUnLCcnKSk7CiQoJ3dhdGNoQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCd3YXRjaENvZGUnLCd3YXRjaENvbXBhbnlOYW1lJywnJykpOwoKCgphc3luYyBmdW5jdGlvbiBnZXRQb2xpY3koY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcG9saWN5P2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCflm73nrZbjg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4geC5wb2xpY3l8fHt9Owp9CgpmdW5jdGlvbiBwb2xpY3lTb3VyY2VTdGF0dXNKYShzKXsKICBpZihzPT09J3ZlcmlmaWVkX2xpdmUnKXJldHVybiAn5YWs5byP44Oa44O844K456K66KqN5riIJzsKICBpZihzPT09J3BhcnRpYWxfbGl2ZScpcmV0dXJuICflhazlvI/jg5rjg7zjgrjpg6jliIbnorroqo0nOwogIGlmKHM9PT0ndmVyaWZpZWRfcmVnaXN0cnknKXJldHVybiAn5pyA57WC56K66KqN5riI5YWs5byP44K944O844K5JzsKICByZXR1cm4gJ+eiuuiqjeS4jeWPryc7Cn0KCmZ1bmN0aW9uIHJlbmRlclBvbGljeVRoZW1lcyhwKXsKICBjb25zdCBib3g9JCgncG9saWN5VGhlbWVzJyk7CiAgaWYoIWJveClyZXR1cm47CiAgY29uc3QgdGhlbWVzPShwJiZwLm1hdGNoZWRfdGhlbWVzKXx8W107CiAgaWYoIXRoZW1lcy5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPGRpdiBjbGFzcz0icG9saWN5dGhlbWUiPjxiPumWoumAo+ODhuODvOODnuOBquOBlyAvIOWIpOWumuS/neeVmTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuacgOS9jumWoumAo+W6puOCkua6gOOBn+OBmeWFrOW8j+aUv+etluODhuODvOODnuOBjOOBguOCiuOBvuOBm+OCk+OAgjwvc3Bhbj48L2Rpdj4nOwogICAgcmV0dXJuOwogIH0KICBib3guaW5uZXJIVE1MPXRoZW1lcy5zbGljZSgwLDQpLm1hcCh0PT5gCiAgICA8ZGl2IGNsYXNzPSJwb2xpY3l0aGVtZSI+CiAgICAgIDxiPiR7dC5uYW1lfSAvIOmWoumAo+W6piAkeyhOdW1iZXIodC5yZWxldmFuY2UpKjEwMCkudG9GaXhlZCgwKX0lPC9iPgogICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuaUv+etluW8t+W6piAke051bWJlcih0LnBvbGljeV9zdHJlbmd0aCkudG9GaXhlZCgwKX0gLyDlr4TkuI4gJHtOdW1iZXIodC5jb250cmlidXRpb24pLnRvRml4ZWQoMSl9IC8gJHtwb2xpY3lTb3VyY2VTdGF0dXNKYSh0LnNvdXJjZV9zdGF0dXMpfTwvc3Bhbj48YnI+CiAgICAgIDxhIGhyZWY9IiR7dC51cmx9IiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+5YWs5byP44K944O844K5PC9hPgogICAgPC9kaXY+CiAgYCkuam9pbignJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldEZ1bmRhbWVudGFscyhjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9mdW5kYW1lbnRhbHM/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+axuueul+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiB4LmZ1bmRhbWVudGFsc3x8e307Cn0KCgpmdW5jdGlvbiBtYXJrZXRGcmVzaG5lc3NGYWN0b3IoZGF0ZVN0cil7CiAgY29uc3QgZj1mcmVzaG5lc3NGb3IoZGF0ZVN0cik7CiAgaWYoZi5sZXZlbD09PSdmcmVzaCcpcmV0dXJuIDEuMDA7CiAgaWYoZi5sZXZlbD09PSd3YXJuaW5nJylyZXR1cm4gMC43MDsKICBpZihmLmxldmVsPT09J3N0YWxlJylyZXR1cm4gMC4yNTsKICByZXR1cm4gMC4yMDsKfQoKZnVuY3Rpb24gZnJlc2huZXNzUGN0VGV4dCh2KXsKICBpZih2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpcmV0dXJuICfigJQnOwogIHJldHVybiBNYXRoLnJvdW5kKE51bWJlcih2KSoxMDApKyclJzsKfQoKZnVuY3Rpb24gZmluYW5jaWFsRnJlc2huZXNzTGFiZWwobGFiZWwpewogIGlmKGxhYmVsPT09J2ZyZXNoJylyZXR1cm4gJ+aWsOOBl+OBhCc7CiAgaWYobGFiZWw9PT0nc2xpZ2h0bHlfb2xkJylyZXR1cm4gJ+OChOOChOWPpOOBhCc7CiAgaWYobGFiZWw9PT0nb2xkJylyZXR1cm4gJ+WPpOOBhCc7CiAgaWYobGFiZWw9PT0ndmVyeV9vbGQnKXJldHVybiAn44GL44Gq44KK5Y+k44GEJzsKICBpZihsYWJlbD09PSdzdGFsZScpcmV0dXJuICfpnZ7luLjjgavlj6TjgYQnOwogIHJldHVybiAn6ZaL56S65pel5LiN5piOJzsKfQoKZnVuY3Rpb24gcG9saWN5RnJlc2huZXNzRmFjdG9yKHAsbWFudWFsTW9kZSl7CiAgaWYobWFudWFsTW9kZSlyZXR1cm4gMS4wMDsKICBpZighcHx8cC5zY29yZT09PW51bGx8fHAuc2NvcmU9PT11bmRlZmluZWQpcmV0dXJuIDA7CiAgY29uc3QgYz1OdW1iZXIocC5jb25maWRlbmNlX3BjdCk7CiAgaWYoTnVtYmVyLmlzRmluaXRlKGMpKXJldHVybiBNYXRoLm1heCgwLE1hdGgubWluKDEsYy8xMDApKTsKICByZXR1cm4gMC41MDsKfQoKZnVuY3Rpb24gc2NvcmVMYWJlbCh2KXsKICBpZih2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpcmV0dXJuICfigJQnOwogIGNvbnN0IG49TnVtYmVyKHYpOwogIHJldHVybiAobj4wPycrJzonJykrbi50b0ZpeGVkKDEpOwp9CgpmdW5jdGlvbiBwY3RNYXliZSh2KXsKICByZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoMSkrJyUnOwp9Cgphc3luYyBmdW5jdGlvbiBhbmFseXplKCl7CiAgY29uc3QgY29kZT0kKCdjb2RlJykudmFsdWUudHJpbSgpOwogIGlmKCFjb2RlKXskKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44GtJztyZXR1cm59CiAgY29uc3QgYnRuPSQoJ2FuYWx5emVCdG4nKTsgYnRuLmRpc2FibGVkPXRydWU7IGJ0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJzsKICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0nRmluTWluZOe0hDkwMOaXpeS+oeagvOWxpeattO+8i0otUXVhbnRz5rG6566X44OH44O844K/44KS5Y+W5b6X44GX44Gm44GE44G+44GZ4oCmJzsKCiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShjb2RlKSwgcz1xLnNuYXBzaG90fHx7fTsKICAgICQoJ3ByaWNlJykudmFsdWU9cy5sYXN0X2Nsb3NlPT1udWxsPycnOmZtdChzLmxhc3RfY2xvc2UsMSk7CiAgICAkKCdyMjAnKS52YWx1ZT1mbXQocy5yZXR1cm5fMjBkKTskKCdyMTI2JykudmFsdWU9Zm10KHMucmV0dXJuXzEyNmQpOyQoJ3IyNTInKS52YWx1ZT1mbXQocy5yZXR1cm5fMjUyZCk7CiAgICAkKCdoaWdoMjAnKS50ZXh0Q29udGVudD1mbXQocy5oaWdoXzIwZCwxKTskKCdsb3cyMCcpLnRleHRDb250ZW50PWZtdChzLmxvd18yMGQsMSk7CiAgICAkKCd2b2wyMCcpLnRleHRDb250ZW50PXMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZD09bnVsbD8n4oCUJzpmbXQocy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkKSsnJSc7CgogICAgZGlzcGxheUNvbXBhbnkoJCgnY29tcGFueU5hbWUnKSxxLmNvbXBhbnl8fG51bGwsJycpOwogICAgY29uc3QgcHJpY2VTb3VyY2U9cy5wcmljZV9zb3VyY2V8fHEuc291cmNlfHwn5LiN5piOJzsKICAgIGNvbnN0IGhpc3RvcnlEYXRlPXMuaGlzdG9yeV9sYXN0X2RhdGV8fG51bGw7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9CiAgICAgICc8c3BhbiBjbGFzcz0ic291cmNlYmFkZ2UiPuePvuWcqOWApDwvc3Bhbj48YiBjbGFzcz0ib2siPicrcHJpY2VTb3VyY2UrJzwvYj4nKwogICAgICAnPGJyPuePvuWcqOWApOODh+ODvOOCv+aXpTogJysocy5sYXN0X2RhdGV8fCfigJQnKSsKICAgICAgKHMucHJpY2VfdGltZT8nICcrcy5wcmljZV90aW1lOicnKSsKICAgICAgJyAvIOacgOaWsOWPluW+l+WApDogJytmbXQocy5sYXN0X2Nsb3NlLDEpKwogICAgICAnPGJyPjxzcGFuIGNsYXNzPSJzb3VyY2ViYWRnZSI+5L6h5qC85bGl5q20PC9zcGFuPicrCiAgICAgIChzLmhpc3Rvcnlfc291cmNlfHwn5Y+W5b6X44Gq44GXJykrCiAgICAgICcgLyDmnIDntYLml6U6ICcrKGhpc3RvcnlEYXRlfHwn4oCUJykrCiAgICAgICcgLyDlsaXmrbTjgrXjg7Pjg5fjg6s6ICcrKHMuc2FtcGxlX2NvdW50Pz8wKSsn5Lu2JzsKICAgIHNob3dGcmVzaG5lc3Mocy5sYXN0X2RhdGUpOwogICAgc2hvd0hpc3RvcnlGcmVzaG5lc3MoaGlzdG9yeURhdGUscy5sYXN0X2RhdGUpOwoKICAgIGNvbnN0IG1hcmtldEZyZXNobmVzcz1tYXJrZXRGcmVzaG5lc3NGYWN0b3IoaGlzdG9yeURhdGV8fHMubGFzdF9kYXRlKTsKICAgICQoJ21hcmtldEZyZXNoJykudGV4dENvbnRlbnQ9ZnJlc2huZXNzUGN0VGV4dChtYXJrZXRGcmVzaG5lc3MpOwogICAgY29uc3QgbWFya2V0QWdlPWRhdGFBZ2VEYXlzKGhpc3RvcnlEYXRlfHxzLmxhc3RfZGF0ZSk7CiAgICAkKCdtYXJrZXRGcmVzaERldGFpbCcpLnRleHRDb250ZW50PQogICAgICBg5L6h5qC85bGl5q20ICR7KGhpc3RvcnlEYXRlfHxzLmxhc3RfZGF0ZXx8J+KAlCcpfSAvICR7bWFya2V0QWdlPT09bnVsbD8n5pel5pWw5LiN5piOJzptYXJrZXRBZ2UrJ+aXpSd9YDsKCiAgICBjb25zdCBzeW5jZWRIb2xkaW5nPXN5bmNBbmFseXplZFF1b3RlVG9Ib2xkaW5nKGNvZGUscSk7CiAgICBpZihzeW5jZWRIb2xkaW5nKXsKICAgICAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgICAgIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9YCR7Y29kZX0g44Gu5L+d5pyJ5qCq44KS5YiG5p6Q5pmC44Gu5pyA5paw5Y+W5b6X57WC5YCkICR7Zm10KHMubGFzdF9jbG9zZSwxKX0g44Gn5YaN6KiI566X44GX44G+44GX44Gf44CCYDsKICAgIH0KCiAgICAkKCdhbmFseXNpc0V2JykuaW5uZXJIVE1MPQogICAgICBldkh0bWwoJ+efreacnzIw5pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycyMGQnXSkrCiAgICAgIGV2SHRtbCgn5Lit5pyfMTI25pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycxMjZkJ10pKwogICAgICBldkh0bWwoJ+mVt+acnzI1MuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjUyZCddKTsKCiAgICBjb25zdCBzdXBwbHk9cy5zdXBwbHlfcHJveHl8fHt9OwogICAgJCgnc3VwcGx5QXV0bycpLnRleHRDb250ZW50PXNjb3JlTGFiZWwoc3VwcGx5LnNjb3JlKTsKICAgICQoJ3N1cHBseURldGFpbCcpLnRleHRDb250ZW50PQogICAgICAnNeaXpS8yMOaXpeWHuuadpemrmCAnKyhzdXBwbHkudm9sdW1lX3JhdGlvXzVfMjA9PW51bGw/J+KAlCc6TnVtYmVyKHN1cHBseS52b2x1bWVfcmF0aW9fNV8yMCkudG9GaXhlZCgyKSsn5YCNJyk7CgogICAgbGV0IGZ1bmRhbWVudGFscz17fTsKICAgIHRyeXsKICAgICAgZnVuZGFtZW50YWxzPWF3YWl0IGdldEZ1bmRhbWVudGFscyhjb2RlKTsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKGZ1bmRhbWVudGFscy5zY29yZSk7CiAgICAgIGNvbnN0IG09ZnVuZGFtZW50YWxzLm1ldHJpY3N8fHt9OwogICAgICBjb25zdCBlZj1mdW5kYW1lbnRhbHMuZnJlc2huZXNzfHx7fTsKICAgICAgJCgnZWFybkRldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgICfplovnpLogJysoKGZ1bmRhbWVudGFscy5sYXRlc3QmJmZ1bmRhbWVudGFscy5sYXRlc3QuZGF0ZSl8fCfigJQnKSsKICAgICAgICAnIC8g5aOy5LiKICcrcGN0TWF5YmUobS5zYWxlc19ncm93dGhfcGN0KSsKICAgICAgICAnIC8g5Za25qWt55uKICcrcGN0TWF5YmUobS5vcF9ncm93dGhfcGN0KTsKICAgICAgJCgnZWFybkZyZXNoJykudGV4dENvbnRlbnQ9CiAgICAgICAgZWYuZmFjdG9yPT09dW5kZWZpbmVkPyfigJQnOk1hdGgucm91bmQoTnVtYmVyKGVmLmZhY3RvcikqMTAwKSsnJSc7CiAgICAgICQoJ2Vhcm5GcmVzaERldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgIGZpbmFuY2lhbEZyZXNobmVzc0xhYmVsKGVmLmxhYmVsKSsKICAgICAgICAnIC8gJysoZWYuYWdlX2RheXM9PT1udWxsfHxlZi5hZ2VfZGF5cz09PXVuZGVmaW5lZD8n6ZaL56S65pel5LiN5piOJzplZi5hZ2VfZGF5cysn5pel5YmNJyk7CiAgICB9Y2F0Y2goZmUpewogICAgICAkKCdlYXJuQXV0bycpLnRleHRDb250ZW50PSfkuI3mmI4nOwogICAgICAkKCdlYXJuRGV0YWlsJykudGV4dENvbnRlbnQ9J+OBk+OBruODl+ODqeODsy/pipjmn4Tjgafjga/lj5blvpfjgafjgY3jgarjgYTlj6/og73mgKfjgYLjgoonOwogICAgICAkKCdlYXJuRnJlc2gnKS50ZXh0Q29udGVudD0n4oCUJzsKICAgICAgJCgnZWFybkZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9J+axuueul+ODh+ODvOOCv+OBquOBlyc7CiAgICAgIGZ1bmRhbWVudGFscz17c2NvcmU6bnVsbCxmcmVzaG5lc3M6e2ZhY3RvcjowfX07CiAgICB9CgogICAgbGV0IGF1dG9Qb2xpY3k9e3Njb3JlOm51bGwsbWF0Y2hlZF90aGVtZXM6W119OwogICAgdHJ5ewogICAgICBhdXRvUG9saWN5PWF3YWl0IGdldFBvbGljeShjb2RlKTsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD1hdXRvUG9saWN5LnNjb3JlPT1udWxsPyfkuI3mmI4nOnNjb3JlTGFiZWwoYXV0b1BvbGljeS5zY29yZSk7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgIGF1dG9Qb2xpY3kuc2NvcmU9PW51bGwKICAgICAgICAgID8gJ+mWoumAo+OBmeOCi+WFrOW8j+aUv+etluODhuODvOODnuOBquOBlycKICAgICAgICAgIDogJ+iHquWLleWbveetlnByb3h5IC8g5L+h6aC85bqmICcrKGF1dG9Qb2xpY3kuY29uZmlkZW5jZV9wY3Q/PyfigJQnKSsnJSAvICcrKChhdXRvUG9saWN5Lm1hdGNoZWRfdGhlbWVzfHxbXSkubGVuZ3RoKSsn44OG44O844OeJzsKICAgICAgY29uc3QgcGY9cG9saWN5RnJlc2huZXNzRmFjdG9yKGF1dG9Qb2xpY3ksZmFsc2UpOwogICAgICAkKCdwb2xpY3lGcmVzaCcpLnRleHRDb250ZW50PWZyZXNobmVzc1BjdFRleHQocGYpOwogICAgICAkKCdwb2xpY3lGcmVzaERldGFpbCcpLnRleHRDb250ZW50PQogICAgICAgIGF1dG9Qb2xpY3kuc2NvcmU9PW51bGwKICAgICAgICAgID8gJ+mWoumAo+ODhuODvOODnuOBquOBlycKICAgICAgICAgIDogKChhdXRvUG9saWN5Lm1hdGNoZWRfdGhlbWVzfHxbXSkuc29tZSh0PT50LnNvdXJjZV9zdGF0dXM9PT0ndmVyaWZpZWRfbGl2ZScpCiAgICAgICAgICAgICAgPyAn5YWs5byP44Oa44O844K444KS44Op44Kk44OW56K66KqNJwogICAgICAgICAgICAgIDogJ+eiuuiqjea4iOOBv+WFrOW8j+OCveODvOOCueOCkuS9v+eUqCcpOwogICAgICByZW5kZXJQb2xpY3lUaGVtZXMoYXV0b1BvbGljeSk7CiAgICB9Y2F0Y2gocGUpewogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PSfkuI3mmI4nOwogICAgICAkKCdwb2xpY3lEZXRhaWwnKS50ZXh0Q29udGVudD0n5YWs5byP5pS/562W44K944O844K55Y+W5b6X44Ko44Op44O8JzsKICAgICAgJCgncG9saWN5RnJlc2gnKS50ZXh0Q29udGVudD0n4oCUJzsKICAgICAgJCgncG9saWN5RnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0n5Y+W5b6X44Ko44Op44O8JzsKICAgICAgJCgncG9saWN5VGhlbWVzJykuaW5uZXJIVE1MPScnOwogICAgICBhdXRvUG9saWN5PXtzY29yZTpudWxsLG1hdGNoZWRfdGhlbWVzOltdfTsKICAgIH0KCiAgICBjb25zdCBtYW51YWxQb2xpY3k9dmFsKCdwb2xpY3knKTsKICAgIGNvbnN0IHBvbGljeVNjb3JlPW1hbnVhbFBvbGljeT09PW51bGw/YXV0b1BvbGljeS5zY29yZTptYW51YWxQb2xpY3k7CiAgICBjb25zdCBwb2xpY3lNb2RlPW1hbnVhbFBvbGljeT09PW51bGw/J2F1dG8nOidtYW51YWwnOwogICAgaWYobWFudWFsUG9saWN5IT09bnVsbCl7CiAgICAgICQoJ3BvbGljeVN0YXRlJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChtYW51YWxQb2xpY3kpOwogICAgICAkKCdwb2xpY3lEZXRhaWwnKS50ZXh0Q29udGVudD0n5omL5YWl5Yqb44Gn6Ieq5YuV5YCk44KS5LiK5pu444GNJzsKICAgICAgJCgncG9saWN5RnJlc2gnKS50ZXh0Q29udGVudD0nMTAwJSc7CiAgICAgICQoJ3BvbGljeUZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9J+ODpuODvOOCtuODvOaJi+WFpeWKm+WApCc7CiAgICB9CgogICAgY29uc3QgZD17CiAgICAgIGNvZGUsCiAgICAgIHByaWNlOnMubGFzdF9jbG9zZSwKICAgICAgcmV0dXJuMjA6cy5yZXR1cm5fMjBkLAogICAgICByZXR1cm4xMjY6cy5yZXR1cm5fMTI2ZCwKICAgICAgcmV0dXJuMjUyOnMucmV0dXJuXzI1MmQsCiAgICAgIGVhcm5pbmdzX3Njb3JlOmZ1bmRhbWVudGFscy5zY29yZSwKICAgICAgcG9saWN5X3Njb3JlOnBvbGljeVNjb3JlLAogICAgICBwb2xpY3lfbW9kZTpwb2xpY3lNb2RlLAogICAgICBzdXBwbHlfc2NvcmU6c3VwcGx5LnNjb3JlLAogICAgICBtYXJrZXRfZnJlc2huZXNzOm1hcmtldEZyZXNobmVzcywKICAgICAgZWFybmluZ3NfZnJlc2huZXNzOk51bWJlcigKICAgICAgICBmdW5kYW1lbnRhbHMuZnJlc2huZXNzJiZmdW5kYW1lbnRhbHMuZnJlc2huZXNzLmZhY3RvciE9PXVuZGVmaW5lZAogICAgICAgICAgPyBmdW5kYW1lbnRhbHMuZnJlc2huZXNzLmZhY3RvcgogICAgICAgICAgOiAoZnVuZGFtZW50YWxzLnNjb3JlPT1udWxsPzA6MSkKICAgICAgKSwKICAgICAgcG9saWN5X2ZyZXNobmVzczpwb2xpY3lGcmVzaG5lc3NGYWN0b3IoCiAgICAgICAgYXV0b1BvbGljeSwKICAgICAgICBtYW51YWxQb2xpY3khPT1udWxsCiAgICAgICkKICAgIH07CgogICAgY29uc3QgYXI9YXdhaXQgZmV0Y2goJy9hcGkvZnJlZS9hbmFseXplJyx7CiAgICAgIG1ldGhvZDonUE9TVCcsCiAgICAgIGhlYWRlcnM6eydDb250ZW50LVR5cGUnOidhcHBsaWNhdGlvbi9qc29uJ30sCiAgICAgIGJvZHk6SlNPTi5zdHJpbmdpZnkoZCksCiAgICAgIGNhY2hlOiduby1zdG9yZScKICAgIH0pOwogICAgY29uc3QgeD1hd2FpdCBhci5qc29uKCk7CiAgICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5YiG5p6Q44OH44O844K/44GM5LiN6Laz44GX44Gm44GE44G+44GZJyk7CgogICAgJCgnc3RhdGUnKS50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTsKICAgICQoJ3BvcycpLnRleHRDb250ZW50PXguc2lnbmFsLnBvc2l0aXZlX2NvdW50OwogICAgJCgnbmVnJykudGV4dENvbnRlbnQ9eC5zaWduYWwubmVnYXRpdmVfY291bnQ7CgogICAgY29uc3Qgc2M9eC5zY29yZXx8e307CiAgICAkKCdzY29yZUhlcm8nKS5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgICAkKCdzY29yZTEwMCcpLnRleHRDb250ZW50PXNjLnNjb3JlMTAwPT1udWxsPyfigJQnOnNjLnNjb3JlMTAwKycgLyAxMDAnOwoKICAgIGNvbnN0IHBpbGw9JCgnc2NvcmVTdGF0ZVBpbGwnKTsKICAgIHBpbGwuY2xhc3NOYW1lPSdzdGF0ZXBpbGwgJytzdGF0ZUNsYXNzKHguc2lnbmFsLnN0YXRlKTsKICAgIHBpbGwudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7CgogICAgJCgnY292ZXJhZ2UnKS50ZXh0Q29udGVudD1zYy5jb3ZlcmFnZV9wY3Q9PW51bGw/J+KAlCc6c2MuY292ZXJhZ2VfcGN0KyclJzsKICAgICQoJ2ZyZXNoQ292ZXJhZ2UnKS50ZXh0Q29udGVudD1zYy5jb3ZlcmFnZV9wY3Q9PW51bGw/J+KAlCc6c2MuY292ZXJhZ2VfcGN0KyclJzsKICAgIGNvbnN0IHNmPXNjLmZyZXNobmVzc3x8e307CiAgICBpZihzZi5tYXJrZXRfcGN0IT09dW5kZWZpbmVkKSQoJ21hcmtldEZyZXNoJykudGV4dENvbnRlbnQ9TWF0aC5yb3VuZChOdW1iZXIoc2YubWFya2V0X3BjdCkpKyclJzsKICAgIGlmKHNmLmVhcm5pbmdzX3BjdCE9PXVuZGVmaW5lZCkkKCdlYXJuRnJlc2gnKS50ZXh0Q29udGVudD1NYXRoLnJvdW5kKE51bWJlcihzZi5lYXJuaW5nc19wY3QpKSsnJSc7CiAgICBpZihzZi5wb2xpY3lfcGN0IT09dW5kZWZpbmVkKSQoJ3BvbGljeUZyZXNoJykudGV4dENvbnRlbnQ9TWF0aC5yb3VuZChOdW1iZXIoc2YucG9saWN5X3BjdCkpKyclJzsKCiAgICAkKCdzY29yZUJyZWFrZG93bicpLnRleHRDb250ZW50PQogICAgICAn44OG44Kv44OL44Kr44OrICcrc2NvcmVMYWJlbChzYy50ZWNobmljYWwpKwogICAgICAnIC8g5rG6566XICcrc2NvcmVMYWJlbChzYy5lYXJuaW5ncykrCiAgICAgICcgLyDpnIDntaYgJytzY29yZUxhYmVsKHNjLnN1cHBseSkrCiAgICAgICcgLyDlm73nrZZwcm94eSAnK3Njb3JlTGFiZWwoc2MucG9saWN5KTsKCiAgICAkKCdzY29yZVJlYXNvbicpLnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgICQoJ3Njb3JlUmVhc29uVGV4dCcpLnRleHRDb250ZW50PWRyaXZlclNlbnRlbmNlKHNjKTsKICAgICQoJ2RyaXZlckdyaWQnKS5pbm5lckhUTUw9CiAgICAgIGRyaXZlckJveEh0bWwoJ+acgOWkp+OBruODl+ODqeOCueimgeWboCcsc2Muc3Ryb25nZXN0X3Bvc2l0aXZlLCdwb3NpdGl2ZScpKwogICAgICBkcml2ZXJCb3hIdG1sKCfmnIDlpKfjga7jg57jgqTjg4rjgrnopoHlm6AnLHNjLnN0cm9uZ2VzdF9uZWdhdGl2ZSwnbmVnYXRpdmUnKTsKCiAgICByZW5kZXJDb250cmlidXRpb25zKHNjKTsKCiAgICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3Iocy5sYXN0X2RhdGUpOwogICAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSk7CiAgICBsZXQgcmVzdWx0VGV4dD0KICAgICAgIWZyZXNoLmRlY2lzaW9uX29rCiAgICAgICAgPyAn4pqg77iPIOePvuWcqOWApOODh+ODvOOCv+OBjOWPpOOBhOOBn+OCgeOAgeS7iuaXpeOBruWjsuiyt+WIpOaWreOBqOOBl+OBpuOBr+S9v+eUqOOBl+OBvuOBm+OCk+OAgicKICAgICAgICA6ICFoaXN0b3J5RnJlc2guZGVjaXNpb25fb2sKICAgICAgICAgID8gJ+KaoO+4jyDnj77lnKjlgKTjga/mlrDjgZfjgYTml6XotrPjgpLkvb/jgaPjgabjgYTjgb7jgZnjgYzjgIHkvqHmoLzlsaXmrbTjgYzlj6TjgYTjgZ/jgoHnt4/lkIjngrnjg7vnn63kuK3plbfmnJ/jg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjga/lj4LogIPlgKTjgafjgZnjgIInCiAgICAgICAgICA6ICfnj77lnKjlgKTjgajkvqHmoLzlsaXmrbTjga7prq7luqbjgpLnorroqo3muIjjgb/jgIInOwoKICAgIGNvbnN0IGVmYWN0b3I9TnVtYmVyKAogICAgICBmdW5kYW1lbnRhbHMuZnJlc2huZXNzJiZmdW5kYW1lbnRhbHMuZnJlc2huZXNzLmZhY3RvciE9PXVuZGVmaW5lZAogICAgICAgID8gZnVuZGFtZW50YWxzLmZyZXNobmVzcy5mYWN0b3IKICAgICAgICA6IDEKICAgICk7CiAgICBpZihmdW5kYW1lbnRhbHMuc2NvcmUhPT1udWxsJiZlZmFjdG9yPDEpewogICAgICByZXN1bHRUZXh0Kz1gIOaxuueul+OBr+mWi+ekuuOBi+OCieaZgumWk+OBjOe1jOOBo+OBpuOBhOOCi+OBn+OCgeOAgee3j+WQiOeCueOBp+OBr+Wfuua6lumHjeOBvzMwJeOBq+muruW6piR7TWF0aC5yb3VuZChlZmFjdG9yKjEwMCl9JeOCkuaOm+OBkeOBpuW9semfv+OCkuW8seOCgeOBpuOBhOOBvuOBmeOAgmA7CiAgICB9CiAgICBpZihwb2xpY3lNb2RlPT09J2F1dG8nJiZwb2xpY3lTY29yZSE9PW51bGwpewogICAgICBjb25zdCBwZj1wb2xpY3lGcmVzaG5lc3NGYWN0b3IoYXV0b1BvbGljeSxmYWxzZSk7CiAgICAgIGlmKHBmPDEpewogICAgICAgIHJlc3VsdFRleHQrPWAg5Zu9562WcHJveHnjgoLjgr3jg7zjgrnnorroqo3nirbmhYvjgavlv5zjgZjjgabprq7luqYke01hdGgucm91bmQocGYqMTAwKX0l44Gn6YeN44G/6Kq/5pW044GX44Gm44GE44G+44GZ44CCYDsKICAgICAgfQogICAgfQogICAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9cmVzdWx0VGV4dDsKICB9Y2F0Y2goZSl7CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD0n4pqg77iPICcrZS5tZXNzYWdlOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPSc8c3BhbiBjbGFzcz0iZXJyIj7lj5blvpfjgqjjg6njg7w6ICcrZS5tZXNzYWdlKyc8L3NwYW4+JzsKICB9ZmluYWxseXsKICAgIGJ0bi5kaXNhYmxlZD1mYWxzZTsKICAgIGJ0bi50ZXh0Q29udGVudD0n5a6f44OH44O844K/44Gn5YiG5p6QJzsKICB9Cn0KCnJlbmRlckhvbGRpbmdzKCk7cmVuZGVyV2F0Y2goKTtyZW5kZXJNb3ZlbWVudCgpO3JlbmRlckRhaWx5QWR2aWNlKCk7CnNldEludGVydmFsKCgpPT57cmVuZGVyRGFpbHlBZHZpY2UoKTtyZW5kZXJSYWRlbk5ld3MoKX0sNjAwMDApOwpyZW5kZXJSYWRlbk5ld3MoKTtsb2FkUmFkZW5OZXdzKCk7c2V0SW50ZXJ2YWwobG9hZFJhZGVuTmV3cywzMCo2MDAwMCk7CmRvY3VtZW50LmFkZEV2ZW50TGlzdGVuZXIoJ3Zpc2liaWxpdHljaGFuZ2UnLCgpPT57aWYoIWRvY3VtZW50LmhpZGRlbilyZW5kZXJEYWlseUFkdmljZSgpfSk7CndpbmRvdy5hZGRFdmVudExpc3RlbmVyKCdzdG9yYWdlJyxyZW5kZXJEYWlseUFkdmljZSk7CnNldFRpbWVvdXQoKCk9PnJlZnJlc2hBbGxIb2xkaW5ncyhmYWxzZSksNDAwKTsKaWYoJ3NlcnZpY2VXb3JrZXInIGluIG5hdmlnYXRvcil7bmF2aWdhdG9yLnNlcnZpY2VXb3JrZXIuZ2V0UmVnaXN0cmF0aW9ucygpLnRoZW4ocnM9PlByb21pc2UuYWxsKHJzLm1hcChyPT5yLnVucmVnaXN0ZXIoKSkpKS5jYXRjaCgoKT0+e30pfQppZignY2FjaGVzJyBpbiB3aW5kb3cpe2NhY2hlcy5rZXlzKCkudGhlbihrZXlzPT5Qcm9taXNlLmFsbChrZXlzLm1hcChrPT5jYWNoZXMuZGVsZXRlKGspKSkpLmNhdGNoKCgpPT57fSl9Cjwvc2NyaXB0Pgo8L21haW4+CjwvYm9keT4KPC9odG1sPg=='
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
