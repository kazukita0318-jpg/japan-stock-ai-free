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


HTML = base64.b64decode(
    'PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9CgoucG9saWN5dGhlbWUgYXtmb250LXNpemU6MTJweH0KLmNvbnRyaWJncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQouY29udHJpYntib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDtiYWNrZ3JvdW5kOiNmZmZ9Ci5jb250cmliIHNwYW57ZGlzcGxheTpibG9ja30KLmNvbnRyaWIgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4O21hcmdpbi10b3A6MnB4fQoKLmNvbnRyaWIgc21hbGx7ZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweDtjb2xvcjojNmI3MjgwO2xpbmUtaGVpZ2h0OjEuMzV9Ci5mcmVzaGJveHtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMXB4O21hcmdpbi10b3A6OXB4O2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLmZyZXNoLW9re2JhY2tncm91bmQ6I2VjZmRmNTtib3JkZXItY29sb3I6I2E3ZjNkMH0KLmZyZXNoLXdhcm57YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1jb2xvcjojZmRlNjhhfQouZnJlc2gtc3RhbGV7YmFja2dyb3VuZDojZmVmMmYyO2JvcmRlci1jb2xvcjojZmVjYWNhfQoKLmZyZXNoYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweH0KLnNvdXJjZWJhZGdle2Rpc3BsYXk6aW5saW5lLWJsb2NrO3BhZGRpbmc6NHB4IDhweDtib3JkZXItcmFkaXVzOjk5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTJweDtmb250LXdlaWdodDo4MDA7bWFyZ2luLXJpZ2h0OjRweH0KLmhpc3Rvcnl3YXJue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDttYXJnaW4tdG9wOjdweDtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyOjFweCBzb2xpZCAjZmRlNjhhfQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5jb250cmliZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+CjxtYWluPgo8ZGl2IGNsYXNzPSJ0b3AiPgogIDxoMT7wn5OIIOaXpeacrOagqkFJIEZSRUU8L2gxPgogIDxkaXYgY2xhc3M9InN1YiI+6KaB5Zug5Yil6a6u5bqmIC8gRmluTWluZOS+oeagvOWxpeattCAvIEotUXVhbnRz5rG6566XIC8g5Zu9562WPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfjq8g6YqY5p+E5YiG5p6QPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkIj4KICAgIDxpbnB1dCBpZD0iY29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIOS+iyA3MjAzIj4KICAgIDxpbnB1dCBpZD0icHJpY2UiIHBsYWNlaG9sZGVyPSLlj5blvpfntYLlgKQiIHJlYWRvbmx5PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbXBhbnlOYW1lIiBjbGFzcz0ic291cmNlIG11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBmeOCi+OBqOS8muekvuWQjeOCkuihqOekuuOBl+OBvuOBmTwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8YSBpZD0ia2FidXRhbiIgY2xhc3M9ImJ0biBzZWNvbmRhcnkiIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7moKrmjqLjgafnorroqo08L2E+CiAgICA8YnV0dG9uIGlkPSJhbmFseXplQnRuIiBvbmNsaWNrPSJhbmFseXplKCkiPuWun+ODh+ODvOOCv+OBp+WIhuaekDwvYnV0dG9uPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl44KM44Gm5oq844GZ44Go44CB54++5Zyo5YCk44GoMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7kvqHmoLzlsaXmrbTjga9GaW5NaW5k5pel5pys5qCq5pel6Laz44KS5pyA5YSq5YWI44GX44G+44GZ44CC6YqY5p+E5ZCN44O75rG6566X44O75Zu9562W5Yik5a6a44GvSi1RdWFudHPnrYnjgpLkvb/nlKjjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBpZD0ic291cmNlQm94IiBjbGFzcz0ic291cmNlIG11dGVkIj7jg4fjg7zjgr/mnKrlj5blvpc8L2Rpdj4KICA8ZGl2IGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9Im1hcmdpbi10b3A6N3B4Ij4KICAgIDxiPvCfhpMg54Sh5paZ5L6h5qC85bGl5q2044Gr44Gk44GE44GmPC9iPgogICAgPGRpdiBjbGFzcz0ibXV0ZWQiPkZpbk1pbmTjga7ml6XmnKzmoKrml6XotrPjgpLntIQ5MDDml6XliIblj5blvpfjgZfjgIHmnIDmlrDllrbmpa3ml6Xjga7ntYLlgKTjg7syMC8xMjYvMjUy5pel44Oq44K/44O844Oz44O7MjDml6Xpq5jlronjg7vlh7rmnaXpq5hwcm94eeODu+ODreODvOODquODs+OCsOWun+e4vuWIhuW4g+OCkuioiOeul+OBl+OBvuOBmeOAguWPluW8leS4reOBruODquOCouODq+OCv+OCpOODoOS+oeagvOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9Imhpc3RvcnlGcmVzaG5lc3NCb3giIGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8Yj7wn5OaIOS+oeagvOWIhuaekOWxpeattOOBrumuruW6pjwvYj4KICAgIDxkaXYgaWQ9Imhpc3RvcnlGcmVzaG5lc3NUZXh0IiBjbGFzcz0ibXV0ZWQiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9ImZyZXNobmVzc0JveCIgY2xhc3M9ImZyZXNoYm94IGZyZXNoLXdhcm4iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPGIgaWQ9ImZyZXNobmVzc1RpdGxlIj7jg4fjg7zjgr/prq7luqY8L2I+CiAgICA8ZGl2IGlkPSJmcmVzaG5lc3NEZXRhaWwiIGNsYXNzPSJtdXRlZCI+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfk4og5qCq5L6h44O744OG44Kv44OL44Kr44Or5a6f57i+PC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XpqLDokL3njocgJTwvc3Bhbj48aW5wdXQgaWQ9InIyMCIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MTI25pelICU8L3NwYW4+PGlucHV0IGlkPSJyMTI2IiByZWFkb25seT48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yNTLml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIyNTIiIHJlYWRvbmx5PjwvZGl2PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6auY5YCkPC9zcGFuPjxiIGlkPSJoaWdoMjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeWuieWApDwvc3Bhbj48YiBpZD0ibG93MjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeW5tOeOh+ODnOODqTwvc3Bhbj48YiBpZD0idm9sMjAiPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+nqSDlrp/jg4fjg7zjgr/opoHlm6A8L2gzPgogIDxwIGNsYXNzPSJtdXRlZCI+5L6h5qC844O76ZyA57Wm44O75rG6566X44O75Zu9562W44Gv44Gd44KM44Ge44KM5Yil44Gr6a6u5bqm44KS5Yik5a6a44GX44G+44GZ44CC5Y+k44GE6KaB5Zug44Gv5YCk44KS5raI44GV44Ga44CB57eP5ZCI54K544G444Gu6YeN44G/44Gg44GR6Ieq5YuV44Gn5LiL44GS44G+44GZ44CCPC9wPgogIDxkaXYgY2xhc3M9ImZhY3RvcmdyaWQiPgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaxuueulzwvc3Bhbj48YiBpZD0iZWFybkF1dG8iPuKAlDwvYj48c21hbGwgaWQ9ImVhcm5EZXRhaWwiIGNsYXNzPSJtdXRlZCI+5pyq5Y+W5b6XPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7pnIDntaZwcm94eTwvc3Bhbj48YiBpZD0ic3VwcGx5QXV0byI+4oCUPC9iPjxzbWFsbCBpZD0ic3VwcGx5RGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWPluW+lzwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Zu9562WcHJveHk8L3NwYW4+PGIgaWQ9InBvbGljeVN0YXRlIj7igJQ8L2I+PHNtYWxsIGlkPSJwb2xpY3lEZXRhaWwiIGNsYXNzPSJtdXRlZCI+5YWs5byP5pS/562W44K944O844K556K66KqN5YmNPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7lrp/lirnjg4fjg7zjgr/lhYXotrM8L3NwYW4+PGIgaWQ9ImNvdmVyYWdlIj7igJQ8L2I+PHNtYWxsIGNsYXNzPSJtdXRlZCI+6a6u5bqm44G+44Gn5Y+N5pig44GX44Gf6YeN44G/PC9zbWFsbD48L2Rpdj4KICA8L2Rpdj4KICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgNnB4Ij7wn5WSIOimgeWboOWIpeOBrumuruW6pjwvaDQ+CiAgPGRpdiBjbGFzcz0iZmFjdG9yZ3JpZCI+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5L6h5qC85bGl5q20PC9zcGFuPjxiIGlkPSJtYXJrZXRGcmVzaCI+4oCUPC9iPjxzbWFsbCBpZD0ibWFya2V0RnJlc2hEZXRhaWwiIGNsYXNzPSJtdXRlZCI+5pyq5Yik5a6aPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpc8L3NwYW4+PGIgaWQ9ImVhcm5GcmVzaCI+4oCUPC9iPjxzbWFsbCBpZD0iZWFybkZyZXNoRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWIpOWumjwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Zu9562WPC9zcGFuPjxiIGlkPSJwb2xpY3lGcmVzaCI+4oCUPC9iPjxzbWFsbCBpZD0icG9saWN5RnJlc2hEZXRhaWwiIGNsYXNzPSJtdXRlZCI+5pyq5Yik5a6aPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7prq7luqblj43mmKDlvozjgqvjg5Djg7znjoc8L3NwYW4+PGIgaWQ9ImZyZXNoQ292ZXJhZ2UiPuKAlDwvYj48c21hbGwgY2xhc3M9Im11dGVkIj7lj6TjgYTopoHlm6Djga/ph43jgb/jgpLmuJvoobA8L3NtYWxsPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InBvbGljeVRoZW1lcyIgY2xhc3M9InBvbGljeXRoZW1lcyI+PC9kaXY+CiAgPGRpdiBzdHlsZT0ibWFyZ2luLXRvcDo5cHgiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lm73nrZbjgrnjgrPjgqLkuIrmm7jjgY3vvIjku7vmhI/vvIkgLTEwMOOAnDEwMDwvc3Bhbj4KICAgIDxpbnB1dCBpZD0icG9saWN5IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLnqbrmrITjgarjgonoh6rli5Xlm73nrZZwcm94eeOCkuS9v+eUqCI+CiAgPC9kaXY+CiAgPHAgY2xhc3M9Im11dGVkIj7igLvlm73nrZZwcm94eeOBr+OAgeaUv+W6nOWFrOW8j+aUv+etluOCveODvOOCueOBqEotUXVhbnRz44Gu5qWt56iu44O75Lya56S+5ZCN44Go44Gu6Zai6YCj5bqm44KS57WE44G/5ZCI44KP44Gb44Gf5Y+C6ICD5YCk44Gn44GZ44CC44Op44Kk44OW56K66KqN44Gn44GN44Gq44GE5aC05ZCI44Gv5pyA57WC56K66KqN5riI44G/5oOF5aCx44KS5L2O5L+h6aC85bqm44Gn5L2/55So44GX44CB44Gd44Gu54q25oWL44KC55S76Z2i44Gr5piO56S644GX44G+44GZ44CC6KOc5Yqp6YeR5o6h5oqe44KE5qWt57i+5oGp5oG144Gd44Gu44KC44Gu44KS6Ki85piO44GZ44KL5oyH5qiZ44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn6egIOWIhuaekOe1kOaenDwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPueKtuaFizwvc3Bhbj48YiBpZD0ic3RhdGUiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg5fjg6njgrnmoLnmi6A8L3NwYW4+PGIgaWQ9InBvcyI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODnuOCpOODiuOCueagueaLoDwvc3Bhbj48YiBpZD0ibmVnIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0ic2NvcmVIZXJvIiBjbGFzcz0ic2NvcmVoZXJvIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+57eP5ZCI5o6h54K577yI5Y+W5b6X44OH44O844K/44O744Or44O844Or44OZ44O844K577yJPC9zcGFuPgogICAgPGIgaWQ9InNjb3JlMTAwIj7igJQ8L2I+CiAgICA8c3BhbiBpZD0ic2NvcmVTdGF0ZVBpbGwiIGNsYXNzPSJzdGF0ZXBpbGwgc3RhdGUtbmV1dHJhbCI+4oCUPC9zcGFuPgogICAgPGRpdiBpZD0ic2NvcmVCcmVha2Rvd24iIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJzY29yZVJlYXNvbiIgY2xhc3M9InNjb3JlLXJlYXNvbiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3Ryb25nPvCfp60g44Gq44Gc44GT44Gu54K55pWw77yfPC9zdHJvbmc+CiAgICA8ZGl2IGlkPSJzY29yZVJlYXNvblRleHQiIGNsYXNzPSJtdXRlZCI+4oCUPC9kaXY+CiAgICA8ZGl2IGlkPSJkcml2ZXJHcmlkIiBjbGFzcz0iZHJpdmVyZ3JpZCI+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0iY29udHJpYnV0aW9uQm94IiBjbGFzcz0ic2NvcmUtcmVhc29uIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxzdHJvbmc+8J+nriDnt4/lkIjngrnjgbjjga7lr4TkuI48L3N0cm9uZz4KICAgIDxkaXYgY2xhc3M9Im11dGVkIj7lj5blvpfjgafjgY3jgZ/opoHlm6Djgavprq7luqbkv4LmlbDjgpLmjpvjgZHjgabjgYvjgonph43jgb/jgpLlho3phY3liIbjgZfjgIHlkITopoHlm6DjgYznt4/lkIjoqZXkvqHjgpLjganjgozjgaDjgZHmirzjgZfkuIrjgZLvvI/mirzjgZfkuIvjgZLjgZ/jgYvjgpLooajnpLrjgZfjgb7jgZnjgII8L2Rpdj4KICAgIDxkaXYgaWQ9ImNvbnRyaWJ1dGlvbkdyaWQiIGNsYXNzPSJjb250cmliZ3JpZCI+PC9kaXY+CiAgPC9kaXY+CiAgPHAgaWQ9InJlc3VsdCIgY2xhc3M9Im11dGVkIj7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjgIzlrp/jg4fjg7zjgr/jgafliIbmnpDjgI3jgpLmirzjgZfjgabjgY/jgaDjgZXjgYTjgII8L3A+CiAgPHAgY2xhc3M9Im11dGVkIj7mjqHngrnluK/vvJo4MOOAnDEwMCDlvLfmsJcgLyA2NeOAnDc5IOOChOOChOW8t+awlyAvIDQ144CcNjQg5Lit56uLIC8gMzDjgJw0NCDjgoTjgoTlvLHmsJcgLyAw44CcMjkg5byx5rCXPC9wPgogIDxkaXYgaWQ9ImFuYWx5c2lzRXYiIGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+aqCDku4rml6Xjga7lhKrlhYjjgqLjgq/jgrfjg6fjg7M8L2gzPgogIDxkaXYgaWQ9InByaW9yaXR5QWN0aW9ucyI+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOOAgeWEquWFiOOBl+OBpueiuuiqjeOBmeOCi+mKmOafhOOCkuiHquWLleihqOekuuOBl+OBvuOBmeOAgjwvcD4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIHBvcnRmb2xpbyI+CiAgPGgzPvCfp60g44Od44O844OI44OV44Kp44Oq44Kq5YWo5L2TPC9oMz4KICA8ZGl2IGlkPSJwb3J0Zm9saW9TdW1tYXJ5Ij4KICAgIDxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go6Ieq5YuV6ZuG6KiI44GX44G+44GZ44CCPC9wPgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5K8IOS/neacieagquODu+aQjeWIh+OCii/liKnnoro8L2gzPgogIDxkaXYgY2xhc3M9Im5vdGUiPgogICAg54++5Zyo5YCk44Go5L6h5qC85YiG5p6Q5bGl5q2044GvRmluTWluZOaXpei2s+OCkuWEquWFiOOBl+OBpuOAgeaQjeebiuODu+aQjeWIh+OCiui3nembouODu+WIqeeiuui3nembouODu+ODiOODrOODvOODquODs+OCsOODu+efreS4remVt+Wun+e4vuODu+ODneODvOODiOODleOCqeODquOCquipleS+oeOCkuiHquWLleWGjeioiOeul+OBl+OBvuOBmeOAgkZpbk1pbmTlj5blvpflpLHmlZfmmYLjgaDjgZHku5bjgr3jg7zjgrnjgbjjg5Xjgqnjg7zjg6vjg5Djg4Pjgq/jgZfjgb7jgZnjgIIKICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJyb3ciIHN0eWxlPSJtYXJnaW4tdG9wOjlweCI+CiAgICA8YnV0dG9uIGlkPSJyZWZyZXNoQWxsQnRuIiBjbGFzcz0ic2Vjb25kYXJ5IiBvbmNsaWNrPSJyZWZyZXNoQWxsSG9sZGluZ3ModHJ1ZSkiPuS/neacieagquOCkuacgOaWsOe1guWApOOBp+S4gOaLrOabtOaWsDwvYnV0dG9uPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRpbmdSZWZyZXNoU3RhdHVzIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46N3B4IDJweCAwIj7kv53mnInmoKrjga7oh6rli5Xmm7TmlrDjga8zMOWIhuOBlOOBqOOBq+acgOWkpzHlm57jgafjgZnjgII8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZENvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8aW5wdXQgaWQ9ImhvbGRDb3N0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLlj5blvpfljZjkvqEiPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjZweCAycHggMCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZFNoYXJlcyIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i5qCq5pWwIj4KICAgIDxzZWxlY3QgaWQ9ImZlZU1vZGUiPgogICAgICA8b3B0aW9uIHZhbHVlPSJub211cmFfbmV0Ij7ph47mnZHjgqrjg7Pjg6njgqTjg7PlsILnlKjmlK/lupfjg7vnj77niak8L29wdGlvbj4KICAgICAgPG9wdGlvbiB2YWx1ZT0ibm9uZSI+5omL5pWw5paZ44Gq44GX77yI5q+U6LyD55So77yJPC9vcHRpb24+CiAgICA8L3NlbGVjdD4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoogJTwvc3Bhbj48aW5wdXQgaWQ9InN0b3BQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjgiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuiAlPC9zcGFuPjxpbnB1dCBpZD0idGFrZVBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iMTUiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODqyAlPC9zcGFuPjxpbnB1dCBpZD0idHJhaWxQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjciPjwvZGl2PgogIDwvZGl2PgogIDxidXR0b24gb25jbGljaz0iYWRkSG9sZGluZygpIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij7lrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZg8L2J1dHRvbj4KICA8cCBjbGFzcz0ibXV0ZWQiPumHjuadkeODjeODg+ODiO+8huOCs+ODvOODq++8j+OBu+OBo+OBqOODgOOCpOODrOOCr+ODiOOBruWbveWGheePvueJqeODu+OCquODs+ODqeOCpOODs+azqOaWh+OBrueojui+vOaJi+aVsOaWmeihqOOCkuS9v+eUqOOAgjwvcD4KICA8ZGl2IGlkPSJob2xkaW5ncyI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkYAg44Km44Kp44OD44OB44Oq44K544OIPC9oMz4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGlucHV0IGlkPSJ3YXRjaENvZGUiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPgogICAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRXYXRjaCgpIj7ov73liqA8L2J1dHRvbj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NHB4IDJweCA4cHgiPumKmOafhOWQje+8muKAlDwvZGl2PgogIDxkaXYgaWQ9IndhdGNocyI+PC9kaXY+CjwvZGl2PgoKPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKZnVuY3Rpb24gdmFsKGlkKXtsZXQgdj0kKGlkKS52YWx1ZS50cmltKCk7cmV0dXJuIHY9PT0nJz9udWxsOk51bWJlcih2KX0KZnVuY3Rpb24gbG9jYWwoayl7dHJ5e3JldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKGspfHwnW10nKX1jYXRjaChlKXtyZXR1cm5bXX19CmZ1bmN0aW9uIHNhdmUoayx2KXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrLEpTT04uc3RyaW5naWZ5KHYpKX0KZnVuY3Rpb24gZm10KHYsZD0yKXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoZCl9CmZ1bmN0aW9uIHllbih2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6J8KlJytNYXRoLnJvdW5kKE51bWJlcih2KSkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyl9CmZ1bmN0aW9uIHN0YXRlSmEocyl7CiAgaWYocz09PSdzdHJvbmdfYnVsbGlzaCcpcmV0dXJuICflvLfmsJcnOwogIGlmKHM9PT0nYnVsbGlzaCcpcmV0dXJuICfjgoTjgoTlvLfmsJcnOwogIGlmKHM9PT0nbmV1dHJhbCcpcmV0dXJuICfkuK3nq4snOwogIGlmKHM9PT0nYmVhcmlzaCcpcmV0dXJuICfjgoTjgoTlvLHmsJcnOwogIGlmKHM9PT0nc3Ryb25nX2JlYXJpc2gnKXJldHVybiAn5byx5rCXJzsKICByZXR1cm4gJ+WIpOWumuS/neeVmSc7Cn0KCmZ1bmN0aW9uIHN0YXRlQ2xhc3Mocyl7CiAgaWYocz09PSdzdHJvbmdfYnVsbGlzaCcpcmV0dXJuICdzdGF0ZS1zdHJvbmctYnVsbCc7CiAgaWYocz09PSdidWxsaXNoJylyZXR1cm4gJ3N0YXRlLWJ1bGwnOwogIGlmKHM9PT0nbmV1dHJhbCcpcmV0dXJuICdzdGF0ZS1uZXV0cmFsJzsKICBpZihzPT09J2JlYXJpc2gnKXJldHVybiAnc3RhdGUtYmVhcic7CiAgaWYocz09PSdzdHJvbmdfYmVhcmlzaCcpcmV0dXJuICdzdGF0ZS1zdHJvbmctYmVhcic7CiAgcmV0dXJuICdzdGF0ZS1uZXV0cmFsJzsKfQoKZnVuY3Rpb24gZmFjdG9ySmEoa2V5KXsKICBpZihrZXk9PT0ndGVjaG5pY2FsJylyZXR1cm4gJ+ODhuOCr+ODi+OCq+ODqyc7CiAgaWYoa2V5PT09J2Vhcm5pbmdzJylyZXR1cm4gJ+axuueulyc7CiAgaWYoa2V5PT09J3N1cHBseScpcmV0dXJuICfpnIDntaZwcm94eSc7CiAgaWYoa2V5PT09J3BvbGljeScpcmV0dXJuICflm73nrZYnOwogIHJldHVybiBrZXl8fCfopoHlm6AnOwp9CgpmdW5jdGlvbiBkcml2ZXJTZW50ZW5jZShzYyl7CiAgY29uc3QgcD1zYyYmc2Muc3Ryb25nZXN0X3Bvc2l0aXZlOwogIGNvbnN0IG49c2MmJnNjLnN0cm9uZ2VzdF9uZWdhdGl2ZTsKICBjb25zdCBzY29yZT1OdW1iZXIoc2MmJnNjLnNjb3JlMTAwKTsKCiAgbGV0IGhlYWQ9Jyc7CiAgaWYoTnVtYmVyLmlzRmluaXRlKHNjb3JlKSl7CiAgICBpZihzY29yZT49ODApaGVhZD0n5Y+W5b6X5riI44G/6KaB5Zug44KS57eP5ZCI44GZ44KL44Go44CB5by344GE44OX44Op44K56KmV5L6h44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTY1KWhlYWQ9J+ODl+ODqeOCueimgeWboOOBjOWEquWLouOBp+OAgeOChOOChOW8t+awl+OBruipleS+oeOBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj00NSloZWFkPSfjg5fjg6njgrnjgajjg57jgqTjg4rjgrnjgYzmi67mipfjgZfjgIHkuK3nq4vlnI/jgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49MzApaGVhZD0n44Oe44Kk44OK44K56KaB5Zug44Gu5b2x6Z+/44GM44KE44KE5by344GP44CB5oWO6YeN5a+E44KK44Gn44GZ44CCJzsKICAgIGVsc2UgaGVhZD0n44Oe44Kk44OK44K56KaB5Zug44Gu5b2x6Z+/44GM5aSn44GN44GP44CB5byx5rCX5a+E44KK44Gn44GZ44CCJzsKICB9CgogIGxldCB0YWlsPVtdOwogIGlmKG4pdGFpbC5wdXNoKCfmnIDlpKfjga7mirzjgZfkuIvjgZLopoHlm6Djga8gJytmYWN0b3JKYShuLmtleSkrJyAnK3Njb3JlTGFiZWwobi5zY29yZSkpOwogIGlmKHApdGFpbC5wdXNoKCfmnIDlpKfjga7mirzjgZfkuIrjgZLopoHlm6Djga8gJytmYWN0b3JKYShwLmtleSkrJyAnK3Njb3JlTGFiZWwocC5zY29yZSkpOwogIHJldHVybiBoZWFkKyh0YWlsLmxlbmd0aD8nICcrdGFpbC5qb2luKCfjgIInKSsn44CCJzonJyk7Cn0KCmZ1bmN0aW9uIGRyaXZlckJveEh0bWwodGl0bGUsZCxraW5kKXsKICBpZighZClyZXR1cm4gYDxkaXYgY2xhc3M9ImRyaXZlcmJveCI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7oqbLlvZPjgarjgZc8L2I+PC9kaXY+YDsKICBjb25zdCBzaWduPU51bWJlcihkLmNvbnRyaWJ1dGlvbik+PTA/JysnOicnOwogIHJldHVybiBgPGRpdiBjbGFzcz0iZHJpdmVyYm94Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+CiAgICA8Yj4ke2ZhY3RvckphKGQua2V5KX0gJHtzY29yZUxhYmVsKGQuc2NvcmUpfTwvYj4KICAgIDxzbWFsbCBjbGFzcz0ibXV0ZWQiPuWGjemFjeWIhuW+jOOBrumHjeOBvyAke2Qud2VpZ2h0X3BjdH0lIC8g5a+E5LiOICR7c2lnbn0ke051bWJlcihkLmNvbnRyaWJ1dGlvbikudG9GaXhlZCgxKX08L3NtYWxsPgogIDwvZGl2PmA7Cn0KCmZ1bmN0aW9uIGNvbnRyaWJ1dGlvbkNhcmRIdG1sKGQpewogIGlmKCFkKXJldHVybiAnJzsKICBjb25zdCBjPU51bWJlcihkLmNvbnRyaWJ1dGlvbik7CiAgY29uc3Qgc2lnbj1jPjA/JysnOicnOwogIGNvbnN0IGltcGFjdD1jLzI7CiAgY29uc3QgaW1wYWN0U2lnbj1pbXBhY3Q+MD8nKyc6Jyc7CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJjb250cmliIj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHtmYWN0b3JKYShkLmtleSl9PC9zcGFuPgogICAgPGI+JHtzaWdufSR7Yy50b0ZpeGVkKDEpfTwvYj4KICAgIDxzbWFsbD7ln7rmupbph43jgb8gJHtOdW1iZXIoZC5iYXNlX3dlaWdodF9wY3Q/PzApLnRvRml4ZWQoMSl9JSDDlyDprq7luqYgJHtOdW1iZXIoZC5mcmVzaG5lc3NfcGN0Pz8xMDApLnRvRml4ZWQoMCl9JTwvc21hbGw+CiAgICA8c21hbGw+5YaN6YWN5YiG5b6M6YeN44G/ICR7TnVtYmVyKGQud2VpZ2h0X3BjdCkudG9GaXhlZCgxKX0lPC9zbWFsbD4KICAgIDxzbWFsbD4xMDDngrnmj5vnrpfjgbjjga7lvbHpn78gJHtpbXBhY3RTaWdufSR7aW1wYWN0LnRvRml4ZWQoMSl954K5PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiByZW5kZXJDb250cmlidXRpb25zKHNjKXsKICBjb25zdCBib3g9JCgnY29udHJpYnV0aW9uQm94Jyk7CiAgY29uc3QgZ3JpZD0kKCdjb250cmlidXRpb25HcmlkJyk7CiAgaWYoIWJveHx8IWdyaWQpcmV0dXJuOwogIGNvbnN0IGRzPShzYyYmc2MuZHJpdmVycyl8fFtdOwogIGlmKCFkcy5sZW5ndGgpewogICAgYm94LnN0eWxlLmRpc3BsYXk9J25vbmUnOwogICAgZ3JpZC5pbm5lckhUTUw9Jyc7CiAgICByZXR1cm47CiAgfQogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgZ3JpZC5pbm5lckhUTUw9ZHMubWFwKGNvbnRyaWJ1dGlvbkNhcmRIdG1sKS5qb2luKCcnKTsKfQpmdW5jdGlvbiBwY3Qodil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDIpKyclJ30KZnVuY3Rpb24gc3RhdEZvcihoLGtleSl7cmV0dXJuIGgmJmguZm9yd2FyZF9zdGF0cyYmaC5mb3J3YXJkX3N0YXRzW2tleV0/aC5mb3J3YXJkX3N0YXRzW2tleV06bnVsbH0KCmZ1bmN0aW9uIGRhdGFBZ2VEYXlzKGRhdGVTdHIpewogIGlmKCFkYXRlU3RyKXJldHVybiBudWxsOwogIGNvbnN0IG09U3RyaW5nKGRhdGVTdHIpLm1hdGNoKC9eKFxkezR9KS0oXGR7Mn0pLShcZHsyfSkkLyk7CiAgaWYoIW0pcmV0dXJuIG51bGw7CiAgY29uc3QgZD1EYXRlLlVUQyhOdW1iZXIobVsxXSksTnVtYmVyKG1bMl0pLTEsTnVtYmVyKG1bM10pKTsKICBjb25zdCBub3c9bmV3IERhdGUoKTsKICBjb25zdCB0b2RheT1EYXRlLlVUQyhub3cuZ2V0RnVsbFllYXIoKSxub3cuZ2V0TW9udGgoKSxub3cuZ2V0RGF0ZSgpKTsKICByZXR1cm4gTWF0aC5tYXgoMCxNYXRoLmZsb29yKCh0b2RheS1kKS84NjQwMDAwMCkpOwp9CgpmdW5jdGlvbiBmcmVzaG5lc3NGb3IoZGF0ZVN0cil7CiAgY29uc3QgZGF5cz1kYXRhQWdlRGF5cyhkYXRlU3RyKTsKICBpZihkYXlzPT09bnVsbCl7CiAgICByZXR1cm4ge2xldmVsOid1bmtub3duJyxkYXlzOm51bGwsbGFiZWw6J+aXpeS7mOS4jeaYjicsY2xzOidmcmVzaC13YXJuJyxkZWNpc2lvbl9vazpmYWxzZX07CiAgfQogIGlmKGRheXM8PTQpewogICAgcmV0dXJuIHtsZXZlbDonZnJlc2gnLGRheXMsbGFiZWw6J+muruW6pk9LJyxjbHM6J2ZyZXNoLW9rJyxkZWNpc2lvbl9vazp0cnVlfTsKICB9CiAgaWYoZGF5czw9MTApewogICAgcmV0dXJuIHtsZXZlbDond2FybmluZycsZGF5cyxsYWJlbDon44KE44KE6YGF5bu2JyxjbHM6J2ZyZXNoLXdhcm4nLGRlY2lzaW9uX29rOnRydWV9OwogIH0KICByZXR1cm4ge2xldmVsOidzdGFsZScsZGF5cyxsYWJlbDon5Y+k44GE44OH44O844K/JyxjbHM6J2ZyZXNoLXN0YWxlJyxkZWNpc2lvbl9vazpmYWxzZX07Cn0KCmZ1bmN0aW9uIGZyZXNobmVzc1RleHQoZGF0ZVN0cil7CiAgY29uc3QgZj1mcmVzaG5lc3NGb3IoZGF0ZVN0cik7CiAgaWYoZi5kYXlzPT09bnVsbClyZXR1cm4gJ+acgOe1guODh+ODvOOCv+aXpeOCkueiuuiqjeOBp+OBjeOBvuOBm+OCk+OAgic7CiAgaWYoZi5sZXZlbD09PSdmcmVzaCcpcmV0dXJuIGDmnIDntYLjg4fjg7zjgr/ml6XjgYvjgokgJHtmLmRheXN95pel44CC6YCa5bi444Gu5Y+C6ICD5Yik5a6a44Gr5L2/55So44GX44G+44GZ44CCYDsKICBpZihmLmxldmVsPT09J3dhcm5pbmcnKXJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpeOAgumBheW7tuOBq+azqOaEj+OBl+OBpuOAgeWun+mam+OBruePvuWcqOWApOOCgueiuuiqjeOBl+OBpuOBj+OBoOOBleOBhOOAgmA7CiAgcmV0dXJuIGDmnIDntYLjg4fjg7zjgr/ml6XjgYvjgokgJHtmLmRheXN95pel6YGF44KM44CC5LuK5pel44Gu5aOy6LK35Yik5pat44Gr44Gv5Y+k44GE44Gf44KB44CB5L+d5pyJ5Yik5pat44Gv6Ieq5YuV44Gn5L+d55WZ44GX44G+44GZ44CCYDsKfQoKZnVuY3Rpb24gc2hvd0ZyZXNobmVzcyhkYXRlU3RyKXsKICBjb25zdCBib3g9JCgnZnJlc2huZXNzQm94Jyk7CiAgaWYoIWJveClyZXR1cm47CiAgY29uc3QgZj1mcmVzaG5lc3NGb3IoZGF0ZVN0cik7CiAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICBib3guY2xhc3NOYW1lPSdmcmVzaGJveCAnK2YuY2xzOwogICQoJ2ZyZXNobmVzc1RpdGxlJykudGV4dENvbnRlbnQ9CiAgICBmLmxldmVsPT09J2ZyZXNoJz8n4pyFIOODh+ODvOOCv+muruW6pk9LJzoKICAgIGYubGV2ZWw9PT0nd2FybmluZyc/J+KaoO+4jyDjg4fjg7zjgr/pgYXlu7bjgavms6jmhI8nOgogICAgZi5sZXZlbD09PSdzdGFsZSc/J/Cfm5Eg44OH44O844K/44GM5Y+k44GE44Gf44KB5LuK5pel44Gu5Yik5pat44Gv5L+d55WZJzoKICAgICfimqDvuI8g44OH44O844K/6a6u5bqm44KS56K66KqN44Gn44GN44G+44Gb44KTJzsKICAkKCdmcmVzaG5lc3NEZXRhaWwnKS50ZXh0Q29udGVudD1mcmVzaG5lc3NUZXh0KGRhdGVTdHIpOwp9CgpmdW5jdGlvbiBzaG93SGlzdG9yeUZyZXNobmVzcyhoaXN0b3J5RGF0ZSxwcmljZURhdGUpewogIGNvbnN0IGJveD0kKCdoaXN0b3J5RnJlc2huZXNzQm94Jyk7CiAgaWYoIWJveClyZXR1cm47CiAgaWYoIWhpc3RvcnlEYXRlKXsKICAgIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgICAkKCdoaXN0b3J5RnJlc2huZXNzVGV4dCcpLnRleHRDb250ZW50PScyMOaXpeODuzEyNuaXpeODuzI1MuaXpeOBruWIhuaekOWxpeattOOCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBn+OAguePvuWcqOWApOOBoOOBkeihqOekuuOBl+OBvuOBmeOAgic7CiAgICByZXR1cm47CiAgfQogIGNvbnN0IGhmPWZyZXNobmVzc0ZvcihoaXN0b3J5RGF0ZSk7CiAgaWYoaGYubGV2ZWw9PT0nZnJlc2gnKXsKICAgIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgICBib3guY2xhc3NOYW1lPSdmcmVzaGJveCBmcmVzaC1vayc7CiAgICAkKCdoaXN0b3J5RnJlc2huZXNzVGV4dCcpLnRleHRDb250ZW50PQogICAgICBg5L6h5qC85bGl5q2044KC5pyA5paw5Za25qWt5pelICR7aGlzdG9yeURhdGV9IOOBvuOBp+WPluW+l+OAguefreS4remVt+ODu+mcgOe1pnByb3h544O744OI44Os44O844Oq44Oz44Kw44KS6YCa5bi46KiI566X44GX44G+44GZ44CCYDsKICAgIHJldHVybjsKICB9CiAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICBjb25zdCBwZD1wcmljZURhdGV8fCfigJQnOwogICQoJ2hpc3RvcnlGcmVzaG5lc3NUZXh0JykudGV4dENvbnRlbnQ9CiAgICBg54++5Zyo5YCk44OH44O844K/5pelICR7cGR9IC8g5L6h5qC85bGl5q205pyA57WC5pelICR7aGlzdG9yeURhdGV944CC5L6h5qC85bGl5q2044GM5Y+k44GE5aC05ZCI44Gg44GR44CB44OI44Os44Oz44OJ44O75pyf5b6F5YCk44O76ZyA57WmcHJveHnjgpLlj4LogIPlgKTmibHjgYTjgavjgZfjgb7jgZnjgIJgOwp9CgpmdW5jdGlvbiBjb25maWRlbmNlRm9yKGgpewogIGNvbnN0IHZhbHM9W2gucmV0dXJuXzIwZCxoLnJldHVybl8xMjZkLGgucmV0dXJuXzI1MmRdLm1hcChOdW1iZXIpLmZpbHRlcihOdW1iZXIuaXNGaW5pdGUpOwogIGNvbnN0IHN0YXRzPVsnMjBkJywnMTI2ZCcsJzI1MmQnXS5tYXAoaz0+c3RhdEZvcihoLGspKS5maWx0ZXIocz0+cyYmcy5zdGF0dXM9PT0nb2snKTsKCiAgaWYoIU51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSl8fCFoLmFzb2YpcmV0dXJuIDA7CgogIGNvbnN0IGNvdmVyYWdlPXZhbHMubGVuZ3RoLzM7CiAgbGV0IGFncmVlbWVudD0wLjU7CiAgaWYodmFscy5sZW5ndGgpewogICAgY29uc3QgcG9zPXZhbHMuZmlsdGVyKHg9Png+MCkubGVuZ3RoOwogICAgY29uc3QgbmVnPXZhbHMuZmlsdGVyKHg9Png8MCkubGVuZ3RoOwogICAgYWdyZWVtZW50PU1hdGgubWF4KHBvcyxuZWcpL3ZhbHMubGVuZ3RoOwogIH0KICBjb25zdCBzdGF0Q292ZXJhZ2U9c3RhdHMubGVuZ3RoLzM7CiAgbGV0IHNjb3JlPShjb3ZlcmFnZSowLjQ1ICsgYWdyZWVtZW50KjAuMjUgKyBzdGF0Q292ZXJhZ2UqMC4zMCkqMTAwOwoKICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3IoaC5hc29mKTsKICBpZihmcmVzaC5sZXZlbD09PSd3YXJuaW5nJylzY29yZSo9MC42MDsKICBpZihmcmVzaC5sZXZlbD09PSdzdGFsZScpc2NvcmU9TWF0aC5taW4oc2NvcmUsMjUpOwogIGlmKGZyZXNoLmxldmVsPT09J3Vua25vd24nKXNjb3JlPU1hdGgubWluKHNjb3JlLDIwKTsKCiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBpZihoaXN0b3J5RnJlc2gubGV2ZWw9PT0nd2FybmluZycpc2NvcmUqPTAuNzU7CiAgaWYoaGlzdG9yeUZyZXNoLmxldmVsPT09J3N0YWxlJylzY29yZT1NYXRoLm1pbihzY29yZSwzNSk7CiAgaWYoaGlzdG9yeUZyZXNoLmxldmVsPT09J3Vua25vd24nKXNjb3JlPU1hdGgubWluKHNjb3JlLDI1KTsKCiAgcmV0dXJuIE1hdGgucm91bmQoc2NvcmUpOwp9CgpmdW5jdGlvbiBkZWNpc2lvbkZvcihoLGMpewogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IWguYXNvZil7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOiflrp/jg4fjg7zjgr/mnKrlj5blvpfjgILlj7PkuIrjga7jgIzmm7TmlrDjgI3jgadKLVF1YW50c+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBj+OBoOOBleOBhOOAgicsCiAgICAgIGNvbmZpZGVuY2U6MAogICAgfTsKICB9CgogIGNvbnN0IHIyMD1OdW1iZXIoaC5yZXR1cm5fMjBkKSwgcjEyNj1OdW1iZXIoaC5yZXR1cm5fMTI2ZCksIHIyNTI9TnVtYmVyKGgucmV0dXJuXzI1MmQpOwogIGNvbnN0IGNvbmZpZGVuY2U9Y29uZmlkZW5jZUZvcihoKTsKICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3IoaC5hc29mKTsKCiAgaWYoIWZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZnvvIjmoKrkvqHlj6TjgYTvvIknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246YCR7ZnJlc2huZXNzVGV4dChoLmFzb2YpfSDmkI3liIfjgorjg7vliKnnorrjg6njgqTjg7Pjgajjga7mr5TovIPjgoLlj4LogIPlgKTmibHjgYTjgafjgZnjgIJgLAogICAgICBjb25maWRlbmNlCiAgICB9OwogIH0KCiAgaWYoY3VyPD1jLnN0b3BQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifmkI3liIfjgormpJzoqI4nLGNsczonZC1zdG9wJyxyZWFzb246J+ioreWumuOBl+OBn+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+S7peS4iycsY29uZmlkZW5jZX07CiAgfQogIGlmKGN1cj49Yy50YWtlUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5Yip56K65qSc6KiOJyxjbHM6J2QtdGFrZScscmVhc29uOifoqK3lrprjgZfjgZ/liKnnorrlj4LogIPjg6njgqTjg7Pku6XkuIonLGNvbmZpZGVuY2V9OwogIH0KCiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBpZighaGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZnvvIjlsaXmrbTlj6TjgYTvvIknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246YOePvuWcqOWApOOBr+WPluW+l+OBp+OBjeOBpuOBhOOBvuOBmeOBjOOAgeS+oeagvOWxpeattOOBryAke2guaGlzdG9yeV9hc29mfHwn5LiN5piOJ33jgILlm7rlrprjga7mkI3liIfjgoov5Yip56K644Op44Kk44Oz44Gr44Gv5pyq5Yiw6YGU44Gn44GZ44GM44CB44OI44Os44Oz44OJ5Yik5pat44Gv5L+d55WZ44GX44G+44GZ44CCYCwKICAgICAgY29uZmlkZW5jZQogICAgfTsKICB9CgogIGlmKGN1cjw9Yy50cmFpbFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel6auY5YCk5Z+65rqW44Gu44OI44Os44O844Oq44Oz44Kw5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CgogIGxldCBwb3NpdGl2ZT0wLCBuZWdhdGl2ZT0wOwogIFtyMjAscjEyNixyMjUyXS5mb3JFYWNoKHg9PnsKICAgIGlmKE51bWJlci5pc0Zpbml0ZSh4KSl7CiAgICAgIGlmKHg+MClwb3NpdGl2ZSsrOwogICAgICBpZih4PDApbmVnYXRpdmUrKzsKICAgIH0KICB9KTsKCiAgaWYobmVnYXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon6K2m5oiSJyxjbHM6J2Qtd2F0Y2gnLHJlYXNvbjonMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7jgYbjgaHjg57jgqTjg4rjgrnlgr7lkJHjgYzlhKrli6InLGNvbmZpZGVuY2V9OwogIH0KICBpZihwb3NpdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntponLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOBp+OAgeikh+aVsOacn+mWk+OBruS+oeagvOODiOODrOODs+ODieOBjOODl+ODqeOCuScsY29uZmlkZW5jZX07CiAgfQogIHJldHVybiB7bGFiZWw6J+S/neaciee2mee2mu+8iOanmOWtkOimi++8iScsY2xzOidkLWhvbGQnLHJlYXNvbjon6Kit5a6a44Op44Kk44Oz5YaF44CC5pyf6ZaT5Yil44OI44Os44Oz44OJ44Gv5by35byx44GM5re35ZyoJyxjb25maWRlbmNlfTsKfQpmdW5jdGlvbiBldkh0bWwodGl0bGUscyl7CiAgaWYoIXN8fHMuc3RhdHVzIT09J29rJyl7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzbWFsbD7mqJnmnKwgJHtufeS7tjwvc21hbGw+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPgogICAgPGI+5bmz5Z2HICR7cGN0KHMubWVhbil9PC9iPgogICAgPHNtYWxsPuS4reWkruWApCAke3BjdChzLm1lZGlhbil9PC9zbWFsbD4KICAgIDxzbWFsbD7kuIrmmIfnjocgJHtwY3Qocy5wb3NpdGl2ZV9yYXRlKX08L3NtYWxsPgogICAgPHNtYWxsPlAxMOOAnFA5MCAke3BjdChzLnAxMCl9IOOAnCAke3BjdChzLnA5MCl9PC9zbWFsbD4KICAgIDxzbWFsbD7mqJnmnKwgJHtzLm595Lu2PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgoKZnVuY3Rpb24gZGlzdGFuY2VJbmZvKGN1cix0YXJnZXQsa2luZCl7CiAgY3VyPU51bWJlcihjdXIpOyB0YXJnZXQ9TnVtYmVyKHRhcmdldCk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFOdW1iZXIuaXNGaW5pdGUodGFyZ2V0KSlyZXR1cm4gJ+KAlCc7CiAgY29uc3QgZGlmZj0odGFyZ2V0L2N1ci0xKSoxMDA7CiAgaWYoa2luZD09PSdzdG9wJyl7CiAgICBpZihkaWZmPj0wKXJldHVybiAn44Op44Kk44Oz5Yiw6YGU5riI44G/JzsKICAgIHJldHVybiBNYXRoLmFicyhkaWZmKS50b0ZpeGVkKDIpKyclIOS4iyc7CiAgfQogIGlmKGtpbmQ9PT0ndGFrZScpewogICAgaWYoZGlmZjw9MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gZGlmZi50b0ZpeGVkKDIpKyclIOS4iic7CiAgfQogIHJldHVybiAoZGlmZj49MD8nKyc6JycpK2RpZmYudG9GaXhlZCgyKSsnJSc7Cn0KCmZ1bmN0aW9uIHByaWNlUmFuZ2UoY3VyLHMpewogIGN1cj1OdW1iZXIoY3VyKTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IXN8fHMuc3RhdHVzIT09J29rJylyZXR1cm4gbnVsbDsKICByZXR1cm4gewogICAgbG93OmN1ciooMStOdW1iZXIocy5wMTApLzEwMCksCiAgICBoaWdoOmN1ciooMStOdW1iZXIocy5wOTApLzEwMCksCiAgICBtZWRpYW46Y3VyKigxK051bWJlcihzLm1lZGlhbikvMTAwKQogIH07Cn0KCmZ1bmN0aW9uIHJhbmdlSHRtbCh0aXRsZSxjdXIscyl7CiAgY29uc3Qgcj1wcmljZVJhbmdlKGN1cixzKTsKICBpZighcil7CiAgICBjb25zdCBuPXMmJnMubiE9PXVuZGVmaW5lZD9zLm46MDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+44OH44O844K/5LiN6LazPC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5qiZ5pysICR7bn3ku7Y8L3NwYW4+PC9kaXY+YDsKICB9CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJyYW5nZWJveCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9IFAxMOOAnFA5MDwvc3Bhbj4KICAgIDxiPiR7eWVuKHIubG93KX0g44CcICR7eWVuKHIuaGlnaCl9PC9iPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7kuK3lpK7lgKTmj5vnrpcgJHt5ZW4oci5tZWRpYW4pfTwvc3Bhbj4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiBhY3Rpb25UZXh0KGgsYyxkKXsKICBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmSd8fGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJylyZXR1cm4gJ+ODh+ODvOOCv+OBjOWPpOOBhOOBn+OCgeS7iuaXpeOBruWIpOaWreOBr+S/neeVmeOAguiovOWIuOS8muekvuOBquOBqeOBp+Wun+mam+OBruePvuWcqOWApOOCkueiuuiqjeOBl+OBpuOBi+OCieWIpOaWreOAgic7CiAgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjlsaXmrbTlj6TjgYTvvIknKXJldHVybiAn54++5Zyo5YCk44Gv56K66KqN5riI44G/44CC5Zu65a6a44Gu5pCN5YiH44KKL+WIqeeiuuODqeOCpOODs+OBoOOBkeeiuuiqjeOBl+OAgeODiOODrOODs+ODieWIpOaWreOBr+S+oeagvOWxpeattOabtOaWsOOBvuOBp+S/neeVmeOAgic7CiAgaWYoZC5sYWJlbD09PSfmkI3liIfjgormpJzoqI4nKXJldHVybiAn5pCN5YiH44KK5Y+C6ICD44Op44Kk44Oz44KS5LiL5Zue44Gj44Gm44GE44G+44GZ44CC5a6f6Zqb44Gu54++5Zyo5YCk44Go5rOo5paH5p2h5Lu244KS56K66KqN44GX44Gm44CB57iu5bCP44O75pKk6YCA44KS5qSc6KiO44CCJzsKICBpZihkLmxhYmVsPT09J+WIqeeiuuaknOiojicpcmV0dXJuICfliKnnorrlj4LogIPjg6njgqTjg7PjgavliLDpgZTjgZfjgabjgYTjgb7jgZnjgILlhajpg6jlo7LljbTjgaDjgZHjgafjgarjgY/jgIHliIblibLliKnnorrjgoLlgJnoo5zjgIInOwogIGlmKGQubGFiZWw9PT0n6K2m5oiSJylyZXR1cm4gJ+itpuaIkuOCvuODvOODs+OAguODiOODrOODvOODquODs+OCsOODqeOCpOODs+OBqOS4reefreacn+OBruWApOWLleOBjeOCkuWEquWFiOOBl+OBpueiuuiqjeOAgic7CiAgcmV0dXJuICfoqK3lrprjg6njgqTjg7PlhoXjgILkv53mnInntpnntprlgJnoo5zjgafjgZnjgYzjgIHnhKHmlpnjg4fjg7zjgr/jga/pgYXlu7bjgZnjgovjgZ/jgoHlrp/pmpvjga7nj77lnKjlgKTjgoLnorroqo3jgIInOwp9CgoKZnVuY3Rpb24gbm9tdXJhTmV0RmVlKGFtb3VudCl7CiAgYW1vdW50PU51bWJlcihhbW91bnR8fDApOwogIGlmKGFtb3VudDw9MClyZXR1cm4gMDsKICBpZihhbW91bnQ8PTEwMDAwMClyZXR1cm4gMTUyOwogIGlmKGFtb3VudDw9MzAwMDAwKXJldHVybiAzMzA7CiAgaWYoYW1vdW50PD01MDAwMDApcmV0dXJuIDUyNDsKICBpZihhbW91bnQ8PTEwMDAwMDApcmV0dXJuIDEwNDg7CiAgaWYoYW1vdW50PD0yMDAwMDAwKXJldHVybiAyMDk1OwogIGlmKGFtb3VudDw9MzAwMDAwMClyZXR1cm4gMzE0MzsKICBpZihhbW91bnQ8PTUwMDAwMDApcmV0dXJuIDUyMzg7CiAgaWYoYW1vdW50PD0xMDAwMDAwMClyZXR1cm4gMTA0NzY7CiAgaWYoYW1vdW50PD0yMDAwMDAwMClyZXR1cm4gMjA5NTI7CiAgaWYoYW1vdW50PD0zMDAwMDAwMClyZXR1cm4gMzE0Mjk7CiAgaWYoYW1vdW50PD01MDAwMDAwMClyZXR1cm4gNDE5MDU7CiAgcmV0dXJuIDc4NTcxOwp9CmZ1bmN0aW9uIGZlZUZvcihhbW91bnQsbW9kZSl7cmV0dXJuIG1vZGU9PT0nbm9tdXJhX25ldCc/bm9tdXJhTmV0RmVlKGFtb3VudCk6MH0KCmZ1bmN0aW9uIGNhbGNIb2xkaW5nKGgpewogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBjb25zdCBjb3N0PU51bWJlcihoLmNvc3QpOwogIGNvbnN0IHNoYXJlcz1OdW1iZXIoaC5zaGFyZXMpOwogIGNvbnN0IGJ1eVZhbHVlPWNvc3Qqc2hhcmVzOwogIGNvbnN0IGJ1eUZlZT1mZWVGb3IoYnV5VmFsdWUsaC5mZWVfbW9kZSk7CiAgY29uc3QgY3VycmVudFZhbHVlPWN1cipzaGFyZXM7CiAgY29uc3Qgc2VsbEZlZT1mZWVGb3IoY3VycmVudFZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGludmVzdGVkPWJ1eVZhbHVlK2J1eUZlZTsKICBjb25zdCBuZXROb3c9Y3VycmVudFZhbHVlLXNlbGxGZWUtaW52ZXN0ZWQ7CiAgY29uc3QgbmV0Tm93UGN0PWludmVzdGVkP25ldE5vdy9pbnZlc3RlZCoxMDA6bnVsbDsKCiAgY29uc3Qgc3RvcFByaWNlPWNvc3QqKDEtTnVtYmVyKGguc3RvcF9wY3QpLzEwMCk7CiAgY29uc3QgdGFrZVByaWNlPWNvc3QqKDErTnVtYmVyKGgudGFrZV9wY3QpLzEwMCk7CiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBjb25zdCBoaWdoMjA9TnVtYmVyKGguaGlnaF8yMGR8fGN1cik7CiAgY29uc3QgdHJhaWxQcmljZT1oaXN0b3J5RnJlc2guZGVjaXNpb25fb2sKICAgID8gaGlnaDIwKigxLU51bWJlcihoLnRyYWlsX3BjdCkvMTAwKQogICAgOiBudWxsOwoKICBjb25zdCBzdG9wVmFsdWU9c3RvcFByaWNlKnNoYXJlczsKICBjb25zdCB0YWtlVmFsdWU9dGFrZVByaWNlKnNoYXJlczsKICBjb25zdCBzdG9wTmV0PXN0b3BWYWx1ZS1mZWVGb3Ioc3RvcFZhbHVlLGguZmVlX21vZGUpLWludmVzdGVkOwogIGNvbnN0IHRha2VOZXQ9dGFrZVZhbHVlLWZlZUZvcih0YWtlVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CgogIHJldHVybiB7YnV5VmFsdWUsYnV5RmVlLGN1cnJlbnRWYWx1ZSxzZWxsRmVlLGludmVzdGVkLG5ldE5vdyxuZXROb3dQY3Qsc3RvcFByaWNlLHRha2VQcmljZSx0cmFpbFByaWNlLHN0b3BOZXQsdGFrZU5ldH07Cn0KCgpjb25zdCBuYW1lVGltZXJzPXt9Owpjb25zdCBuYW1lQ2FjaGU9e307CgpmdW5jdGlvbiBkaXNwbGF5Q29tcGFueSh0YXJnZXQsaW5mbyxwcmVmaXg9JycpewogIGlmKCF0YXJnZXQpcmV0dXJuOwogIGlmKCFpbmZvfHwhaW5mby5uYW1lKXsKICAgIHRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICByZXR1cm47CiAgfQogIGxldCBleHRyYXM9W107CiAgaWYoaW5mby5tYXJrZXQpZXh0cmFzLnB1c2goaW5mby5tYXJrZXQpOwogIGlmKGluZm8uc2VjdG9yMzMpZXh0cmFzLnB1c2goaW5mby5zZWN0b3IzMyk7CiAgdGFyZ2V0LmlubmVySFRNTD0nPGI+JytwcmVmaXgraW5mby5uYW1lKyc8L2I+JysoZXh0cmFzLmxlbmd0aD8nPGJyPjxzcGFuIGNsYXNzPSJtdXRlZCI+JytleHRyYXMuam9pbignIC8gJykrJzwvc3Bhbj4nOicnKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0Q29tcGFueShjb2RlKXsKICBjb25zdCBjPVN0cmluZyhjb2RlfHwnJykudHJpbSgpOwogIGlmKCFjKXJldHVybiBudWxsOwogIGlmKG5hbWVDYWNoZVtjXSlyZXR1cm4gbmFtZUNhY2hlW2NdOwogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3NlY3VyaXR5P2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYykse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfpipjmn4TlkI3jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICBuYW1lQ2FjaGVbY109eDsKICByZXR1cm4geDsKfQoKZnVuY3Rpb24gc2NoZWR1bGVDb21wYW55TG9va3VwKGlucHV0SWQsdGFyZ2V0SWQscHJlZml4PScnKXsKICBjbGVhclRpbWVvdXQobmFtZVRpbWVyc1tpbnB1dElkXSk7CiAgY29uc3QgYz0kKGlucHV0SWQpLnZhbHVlLnRyaW0oKTsKICBjb25zdCB0YXJnZXQ9JCh0YXJnZXRJZCk7CgogIGlmKGMubGVuZ3RoPDQpewogICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muKAlCc7CiAgICByZXR1cm47CiAgfQoKICBuYW1lVGltZXJzW2lucHV0SWRdPXNldFRpbWVvdXQoYXN5bmMoKT0+ewogICAgdHJ5ewogICAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PSfpipjmn4TlkI3jgpLnorroqo3kuK3igKYnOwogICAgICBjb25zdCBpbmZvPWF3YWl0IGdldENvbXBhbnkoYyk7CiAgICAgIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeCk7CiAgICB9Y2F0Y2goZSl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nOwogICAgfQogIH0sNDUwKTsKfQoKCgpmdW5jdGlvbiBwcmlvcml0eUFjdGlvbkZvcihoKXsKICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICBjb25zdCB2YWxpZD1OdW1iZXIuaXNGaW5pdGUoY3VyKSYmY3VyPjAmJmguYXNvZjsKICBjb25zdCBuYW1lPWguY29tcGFueV9uYW1lfHwnJzsKICBjb25zdCBsYWJlbD0oaC5jb2RlfHwnJykrKG5hbWU/JyAnK25hbWU6JycpOwogIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKCiAgaWYoIXZhbGlkKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjk2LAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5a6f44OH44O844K/44KS5pu05pawJywKICAgICAgZGV0YWlsOifmnIDmlrDlj5blvpfntYLlgKTjgYzjgYLjgorjgb7jgZvjgpPjgILjgb7jgZrjgIzmm7TmlrDjgI3jgadKLVF1YW50c+ODh+ODvOOCv+OCkuWPluW+l+OAgicsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKGguYXNvZik7CiAgaWYoIWZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjk5LAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon44OH44O844K/6a6u5bqm44KS56K66KqNJywKICAgICAgZGV0YWlsOmAke2ZyZXNobmVzc1RleHQoaC5hc29mKX0g5a6f6Zqb44Gu54++5Zyo5YCk44KS5YWI44Gr56K66KqN44CCYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBzdG9wRGlzdD0oY3VyL2Muc3RvcFByaWNlLTEpKjEwMDsKICBjb25zdCB0YWtlRGlzdD0oYy50YWtlUHJpY2UvY3VyLTEpKjEwMDsKICBjb25zdCB0cmFpbERpc3Q9KGN1ci9jLnRyYWlsUHJpY2UtMSkqMTAwOwoKICBpZihjdXI8PWMuc3RvcFByaWNlKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjEwMCwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+aQjeWIh+OCiuODqeOCpOODs+WIsOmBlCcsCiAgICAgIGRldGFpbDpg5pyA5paw5Y+W5b6X57WC5YCkICR7eWVuKGN1cil9IC8g5pCN5YiH44KK5Y+C6ICDICR7eWVuKGMuc3RvcFByaWNlKX1gLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKHN0b3BEaXN0PD0zKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjk0LAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5pCN5YiH44KK44Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjgYLjgaggJHtzdG9wRGlzdC50b0ZpeGVkKDIpfSUg44Gn5pCN5YiH44KK5Y+C6ICD44Op44Kk44OzYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihjdXI+PWMudGFrZVByaWNlKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjkwLAogICAgICBjbHM6J3ByaW9yaXR5LXRha2UnLAogICAgICB0aXRsZTon5Yip56K644Op44Kk44Oz5Yiw6YGUJywKICAgICAgZGV0YWlsOmDmnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4oY3VyKX0gLyDliKnnorrlj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYodGFrZURpc3Q8PTMpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6ODQsCiAgICAgIGNsczoncHJpb3JpdHktdGFrZScsCiAgICAgIHRpdGxlOifliKnnorrjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOOBguOBqCAke3Rha2VEaXN0LnRvRml4ZWQoMil9JSDjgafliKnnorrlj4LogIPjg6njgqTjg7NgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgaWYoIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo3OCwKICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICB0aXRsZTon5L6h5qC85bGl5q2044KS5pu05paw5b6F44GhJywKICAgICAgZGV0YWlsOmDnj77lnKjlgKQgJHtoLmFzb2Z8fCfigJQnfSAvIOS+oeagvOWxpeattCAke2guaGlzdG9yeV9hc29mfHwn4oCUJ33jgILlm7rlrprkvqHmoLzjg6njgqTjg7Pku6XlpJbjga7liKTmlq3jga/kv53nlZnjgIJgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKGQubGFiZWw9PT0n6K2m5oiSJyl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo4MCwKICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICB0aXRsZTon6K2m5oiS5Yik5a6aJywKICAgICAgZGV0YWlsOmQucmVhc29uLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKE51bWJlci5pc0Zpbml0ZSh0cmFpbERpc3QpJiZ0cmFpbERpc3Q8PTIuNSl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo3NiwKICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICB0aXRsZTon44OI44Os44O844Oq44Oz44Kw44Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjg4jjg6zjg7zjg6rjg7PjgrDlj4LogIMgJHt5ZW4oYy50cmFpbFByaWNlKX0g44G+44GnICR7dHJhaWxEaXN0LnRvRml4ZWQoMil9JWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgcmV0dXJuIHsKICAgIHNjb3JlOjMwLAogICAgY2xzOidwcmlvcml0eS1nb29kJywKICAgIHRpdGxlOifpgJrluLjnm6PoppYnLAogICAgZGV0YWlsOmQucmVhc29ufHwn6Kit5a6a44Op44Kk44Oz5YaFJywKICAgIGxhYmVsCiAgfTsKfQoKZnVuY3Rpb24gcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBib3g9JCgncHJpb3JpdHlBY3Rpb25zJyk7CiAgaWYoIWJveClyZXR1cm47CgogIGlmKCFhLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOOAgeWEquWFiOOBl+OBpueiuuiqjeOBmeOCi+mKmOafhOOCkuiHquWLleihqOekuuOBl+OBvuOBmeOAgjwvcD4nOwogICAgcmV0dXJuOwogIH0KCiAgbGV0IGFjdGlvbnM9YS5tYXAocHJpb3JpdHlBY3Rpb25Gb3IpOwoKICAvLyBDb25jZW50cmF0aW9uIGFsZXJ0IChwb3J0Zm9saW8tbGV2ZWwpCiAgY29uc3QgdmFsaWQ9YS5maWx0ZXIoaD0+TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCYmaC5hc29mKTsKICBjb25zdCB0b3RhbD12YWxpZC5yZWR1Y2UoKHMsaCk9PnMrTnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKSwwKTsKICBpZih0b3RhbD4wKXsKICAgIGxldCBtYXhIb2xkaW5nPW51bGwsIG1heFZhbHVlPTA7CiAgICB2YWxpZC5mb3JFYWNoKGg9PnsKICAgICAgY29uc3Qgdj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApOwogICAgICBpZih2Pm1heFZhbHVlKXttYXhWYWx1ZT12O21heEhvbGRpbmc9aH0KICAgIH0pOwogICAgY29uc3QgY29uY2VudHJhdGlvbj1tYXhWYWx1ZS90b3RhbCoxMDA7CiAgICBpZihjb25jZW50cmF0aW9uPj02MCAmJiBtYXhIb2xkaW5nKXsKICAgICAgYWN0aW9ucy5wdXNoKHsKICAgICAgICBzY29yZTo3MiwKICAgICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgICAgdGl0bGU6J+mbhuS4reW6puOCkueiuuiqjScsCiAgICAgICAgZGV0YWlsOmAke21heEhvbGRpbmcuY29kZX0ke21heEhvbGRpbmcuY29tcGFueV9uYW1lPycgJyttYXhIb2xkaW5nLmNvbXBhbnlfbmFtZTonJ30g44GM44Od44O844OI44OV44Kp44Oq44Kq44GuICR7Y29uY2VudHJhdGlvbi50b0ZpeGVkKDEpfSVgLAogICAgICAgIGxhYmVsOifjg53jg7zjg4jjg5Xjgqnjg6rjgqonCiAgICAgIH0pOwogICAgfQogIH0KCiAgYWN0aW9ucy5zb3J0KCh4LHkpPT55LnNjb3JlLXguc2NvcmUpOwoKICBjb25zdCBpbXBvcnRhbnQ9YWN0aW9ucy5maWx0ZXIoeD0+eC5zY29yZT49NzApOwogIGNvbnN0IHNob3duPShpbXBvcnRhbnQubGVuZ3RoP2ltcG9ydGFudDphY3Rpb25zKS5zbGljZSgwLDQpOwoKICBib3guaW5uZXJIVE1MPWA8ZGl2IGNsYXNzPSJwcmlvcml0eS13cmFwIj4kewogICAgc2hvd24ubWFwKCh4LGkpPT5gCiAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWl0ZW0gJHt4LmNsc30iPgogICAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWxpbmUiPgogICAgICAgICAgPGRpdj4KICAgICAgICAgICAgPHNwYW4gY2xhc3M9InByaW9yaXR5LXJhbmsiPlBSSU9SSVRZICR7aSsxfTwvc3Bhbj4KICAgICAgICAgICAgPGI+JHt4LnRpdGxlfTwvYj4KICAgICAgICAgIDwvZGl2PgogICAgICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktY29kZSI+JHt4LmxhYmVsfTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPiR7eC5kZXRhaWx9PC9kaXY+CiAgICAgIDwvZGl2PgogICAgYCkuam9pbignJykKICB9PC9kaXY+YCArICgKICAgIGltcG9ydGFudC5sZW5ndGgKICAgICAgPyAnPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPuKAu+WEquWFiOW6puOBr+ioreWumuODqeOCpOODs+aOpei/keODu+WIpOWumueKtuaFi+ODu+ODh+ODvOOCv+acieeEoeODu+mbhuS4reW6puOBi+OCieS9nOOCi+eiuuiqjemghuOBp+OBmeOAguiHquWLleWjsuiyt+aMh+ekuuOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4nCiAgICAgIDogJzxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7nt4rmgKXluqbjga7pq5jjgYTpoIXnm67jga/jgYLjgorjgb7jgZvjgpPjgILpgJrluLjnm6PoppbjgpLntpnntprjgII8L3A+JwogICk7Cn0KCmZ1bmN0aW9uIHBvcnRmb2xpb01lYW5Gb3IoYSxrZXkpewogIGNvbnN0IHZhbGlkPWEuZmlsdGVyKGg9Pk51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjApOwogIGNvbnN0IHRvdGFsPXZhbGlkLnJlZHVjZSgocyxoKT0+cytOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApLDApOwogIGlmKHRvdGFsPD0wKXJldHVybiBudWxsOwoKICBsZXQgbnVtPTAsIGRlbj0wOwogIHZhbGlkLmZvckVhY2goaD0+ewogICAgY29uc3Qgc3Q9c3RhdEZvcihoLGtleSk7CiAgICBjb25zdCB2PU51bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCk7CiAgICBpZihzdCYmc3Quc3RhdHVzPT09J29rJyYmTnVtYmVyLmlzRmluaXRlKE51bWJlcihzdC5tZWFuKSkmJnY+MCl7CiAgICAgIG51bSArPSB2Kk51bWJlcihzdC5tZWFuKTsKICAgICAgZGVuICs9IHY7CiAgICB9CiAgfSk7CiAgcmV0dXJuIGRlbj4wP251bS9kZW46bnVsbDsKfQoKZnVuY3Rpb24gcmVuZGVyUG9ydGZvbGlvU3VtbWFyeSgpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgYm94PSQoJ3BvcnRmb2xpb1N1bW1hcnknKTsKICBpZighYm94KXJldHVybjsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go6Ieq5YuV6ZuG6KiI44GX44G+44GZ44CCPC9wPic7CiAgICByZW5kZXJQcmlvcml0eUFjdGlvbnMoKTsKICAgIHJldHVybjsKICB9CgogIGxldCB0b3RhbENvc3Q9MCwgdG90YWxWYWx1ZT0wLCB0b3RhbE5ldD0wOwogIGNvbnN0IHJvd3M9W107CiAgY29uc3QgZGVjaXNpb25zPXtob2xkOjAsd2F0Y2g6MCx0YWtlOjAsc3RvcDowLHBlbmRpbmc6MH07CgogIGEuZm9yRWFjaChoPT57CiAgICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogICAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogICAgY29uc3Qgc2hhcmVzPU51bWJlcihoLnNoYXJlc3x8MCk7CiAgICBjb25zdCB2YWxpZD1OdW1iZXIuaXNGaW5pdGUoY3VyKSYmY3VyPjAmJmguYXNvZjsKICAgIGNvbnN0IGN1cnJlbnRWYWx1ZT12YWxpZD9jdXIqc2hhcmVzOjA7CiAgICBjb25zdCBpbnZlc3RlZD1OdW1iZXIoaC5jb3N0fHwwKSpzaGFyZXMrYy5idXlGZWU7CgogICAgdG90YWxDb3N0ICs9IGludmVzdGVkOwoKICAgIGlmKHZhbGlkKXsKICAgICAgdG90YWxWYWx1ZSArPSBjdXJyZW50VmFsdWU7CiAgICAgIHRvdGFsTmV0ICs9IGMubmV0Tm93OwoKICAgICAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwogICAgICBpZihkLmxhYmVsPT09J+aQjeWIh+OCiuaknOiojicpZGVjaXNpb25zLnN0b3ArKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+WIqeeiuuaknOiojicpZGVjaXNpb25zLnRha2UrKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+itpuaIkicpZGVjaXNpb25zLndhdGNoKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZknfHxkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOagquS+oeWPpOOBhO+8iSd8fGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5bGl5q205Y+k44GE77yJJylkZWNpc2lvbnMucGVuZGluZysrOwogICAgICBlbHNlIGRlY2lzaW9ucy5ob2xkKys7CgogICAgICByb3dzLnB1c2goe2NvZGU6aC5jb2RlLG5hbWU6aC5jb21wYW55X25hbWV8fCcnLHZhbHVlOmN1cnJlbnRWYWx1ZSxuZXQ6Yy5uZXROb3d9KTsKICAgIH1lbHNlewogICAgICBkZWNpc2lvbnMucGVuZGluZysrOwogICAgICByb3dzLnB1c2goe2NvZGU6aC5jb2RlLG5hbWU6aC5jb21wYW55X25hbWV8fCcnLHZhbHVlOjAsbmV0Om51bGx9KTsKICAgIH0KICB9KTsKCiAgY29uc3QgbmV0UGN0PXRvdGFsQ29zdD4wP3RvdGFsTmV0L3RvdGFsQ29zdCoxMDA6bnVsbDsKICBjb25zdCBtYXhWYWx1ZT1yb3dzLnJlZHVjZSgobSxyKT0+TWF0aC5tYXgobSxyLnZhbHVlKSwwKTsKICBjb25zdCBjb25jZW50cmF0aW9uPXRvdGFsVmFsdWU+MD9tYXhWYWx1ZS90b3RhbFZhbHVlKjEwMDowOwoKICBjb25zdCBtZWFuMjA9cG9ydGZvbGlvTWVhbkZvcihhLCcyMGQnKTsKICBjb25zdCBtZWFuMTI2PXBvcnRmb2xpb01lYW5Gb3IoYSwnMTI2ZCcpOwogIGNvbnN0IG1lYW4yNTI9cG9ydGZvbGlvTWVhbkZvcihhLCcyNTJkJyk7CgogIGNvbnN0IHN0YWxlSG9sZGluZ3M9YS5maWx0ZXIoaD0+ewogICAgY29uc3QgZj1mcmVzaG5lc3NGb3IoaC5hc29mKTsKICAgIHJldHVybiBoLmFzb2YgJiYgIWYuZGVjaXNpb25fb2s7CiAgfSk7CiAgY29uc3Qgc3RhbGVOb3RpY2U9c3RhbGVIb2xkaW5ncy5sZW5ndGgKICAgID8gYDxkaXYgY2xhc3M9ImZyZXNoYm94IGZyZXNoLXN0YWxlIiBzdHlsZT0ibWFyZ2luLWJvdHRvbTo5cHgiPjxiPvCfm5Eg5Y+k44GE5qCq5L6h44OH44O844K/ICR7c3RhbGVIb2xkaW5ncy5sZW5ndGh96YqY5p+EPC9iPjxkaXYgY2xhc3M9Im11dGVkIj7oqZXkvqHpoY3jg7vmkI3nm4rjga/mnIDmlrDlj5blvpfntYLlgKTjg5njg7zjgrnjga7lj4LogIPlgKTjgafjgZnjgILku4rml6Xjga7lo7LosrfliKTmlq3jgavjga/kvb/jgo/jgZrjgIHlrp/pmpvjga7nj77lnKjlgKTjgpLnorroqo3jgZfjgabjgY/jgaDjgZXjgYTjgII8L2Rpdj48L2Rpdj5gCiAgICA6ICcnOwoKICBjb25zdCBzdGFsZUhpc3Rvcnk9YS5maWx0ZXIoaD0+ewogICAgY29uc3QgZj1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgICByZXR1cm4gKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpICYmICFmLmRlY2lzaW9uX29rOwogIH0pOwogIGNvbnN0IGhpc3RvcnlOb3RpY2U9c3RhbGVIaXN0b3J5Lmxlbmd0aAogICAgPyBgPGRpdiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJtYXJnaW4tYm90dG9tOjlweCI+PGI+8J+TmiDkvqHmoLzlsaXmrbTjgYzlj6TjgYQgJHtzdGFsZUhpc3RvcnkubGVuZ3RofemKmOafhDwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+5L6h5qC85bGl5q2044GM5Y+k44GE5aC05ZCI44CB55+t5Lit6ZW35pyf44OI44Os44Oz44OJ44O75pyf5b6F5YCk44O76ZyA57WmcHJveHnjga/lj4LogIPlgKTjgafjgZnjgII8L2Rpdj48L2Rpdj5gCiAgICA6ICcnOwoKICBjb25zdCBhbGxvY2F0aW9ucz1yb3dzCiAgICAuZmlsdGVyKHI9PnIudmFsdWU+MCkKICAgIC5zb3J0KCh4LHkpPT55LnZhbHVlLXgudmFsdWUpCiAgICAubWFwKHI9PnsKICAgICAgY29uc3Qgdz10b3RhbFZhbHVlPjA/ci52YWx1ZS90b3RhbFZhbHVlKjEwMDowOwogICAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImFsbG9jIj4KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+PGI+JHtyLmNvZGV9PC9iPiR7ci5uYW1lPycgJytyLm5hbWU6Jyd9IC8gJHt3LnRvRml4ZWQoMSl9JTwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImFsbG9jYmFyIj48c3BhbiBzdHlsZT0id2lkdGg6JHtNYXRoLm1pbigxMDAsdyl9JSI+PC9zcGFuPjwvZGl2PgogICAgICA8L2Rpdj5gOwogICAgfSkuam9pbignJyk7CgogIGJveC5pbm5lckhUTUw9YAogICAgJHtzdGFsZU5vdGljZX0KICAgICR7aGlzdG9yeU5vdGljZX0KICAgIDxkaXYgY2xhc3M9InBvcnRyb3ciPgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nt4/mipXos4fpoY08L3NwYW4+PGI+JHt5ZW4odG90YWxDb3N0KX08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWPluW+l+e1guWApOODmeODvOOCueipleS+oemhjTwvc3Bhbj48Yj4ke3RvdGFsVmFsdWU+MD95ZW4odG90YWxWYWx1ZSk6J+KAlCd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7dG90YWxOZXQ+PTA/J3Bvcyc6J25lZyd9Ij4ke3RvdGFsVmFsdWU+MD95ZW4odG90YWxOZXQpOifigJQnfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7bmV0UGN0PT09bnVsbD8n4oCUJzpuZXRQY3QudG9GaXhlZCgyKSsnJSd9PC9zcGFuPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mnIDlpKfpipjmn4Tmr5Tnjoc8L3NwYW4+PGI+JHt0b3RhbFZhbHVlPjA/Y29uY2VudHJhdGlvbi50b0ZpeGVkKDEpKyclJzon4oCUJ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KCiAgICA8ZGl2IGNsYXNzPSJwb3J0cm93IiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7kv53mnInntpnntpo8L3NwYW4+PGI+JHtkZWNpc2lvbnMuaG9sZH08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuitpuaIkjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy53YXRjaH08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuaknOiojjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy50YWtlfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KKL+S/neeVmTwvc3Bhbj48Yj4ke2RlY2lzaW9ucy5zdG9wK2RlY2lzaW9ucy5wZW5kaW5nfTwvYj48L2Rpdj4KICAgIDwvZGl2PgoKICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA3cHgiPvCfk4og6KmV5L6h6aGN5Yqg6YeN44Gu6YGO5Y675bmz5Z2H44Oq44K/44O844OzPC9oND4KICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuefreacnzIw5pelPC9zcGFuPjxiPiR7bWVhbjIwPT09bnVsbD8n4oCUJzptZWFuMjAudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Lit5pyfMTI25pelPC9zcGFuPjxiPiR7bWVhbjEyNj09PW51bGw/J+KAlCc6bWVhbjEyNi50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7plbfmnJ8yNTLml6U8L3NwYW4+PGI+JHttZWFuMjUyPT09bnVsbD8n4oCUJzptZWFuMjUyLnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgIDwvZGl2PgogICAgPHAgY2xhc3M9Im11dGVkIj7igLvlkITpipjmn4Tjga7pgY7ljrvlubPlnYfjg6rjgr/jg7zjg7PjgpLnj77lnKjjga7oqZXkvqHpoY3jgafliqDph43jgZfjgZ/lj4LogIPlgKTjgafjgZnjgILnm7jplqLjgpLogIPmha7jgZfjgZ/lsIbmnaXkuojmuKzjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+CgogICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDVweCI+8J+TpiDpipjmn4Tmp4vmiJA8L2g0PgogICAgJHthbGxvY2F0aW9uc3x8JzxwIGNsYXNzPSJtdXRlZCI+5a6f44OH44O844K/5pyq5Y+W5b6XPC9wPid9CiAgYDsKICByZW5kZXJQcmlvcml0eUFjdGlvbnMoKTsKfQpmdW5jdGlvbiByZW5kZXJIb2xkaW5ncygpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgaWYoIWEubGVuZ3RoKXskKCdob2xkaW5ncycpLmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JztyZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCk7cmV0dXJufQogICQoJ2hvbGRpbmdzJykuaW5uZXJIVE1MPWEubWFwKChoLGkpPT57CiAgICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogICAgY29uc3QgdmFsaWRQcmljZT1OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wJiZoLmFzb2Y7CiAgICBjb25zdCBjbHM9dmFsaWRQcmljZT8oYy5uZXROb3c+PTA/J3Bvcyc6J25lZycpOicnOwogICAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwogICAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwoKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iaG9sZGluZyI+CiAgICAgIDxkaXYgY2xhc3M9ImhvbGRpbmctaGVhZCI+CiAgICAgICAgPGRpdj4KICAgICAgICAgIDxiPiR7aC5jb2RlfTwvYj4KICAgICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj4ke2guY29tcGFueV9uYW1lfHwiIn08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gc2Vjb25kYXJ5IiBvbmNsaWNrPSJlZGl0SG9sZGluZ1NoYXJlcygke2l9KSI+5qCq5pWw5aSJ5pu0PC9idXR0b24+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hIb2xkaW5nKCR7aX0pIj7mm7TmlrA8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlSG9sZGluZygke2l9KSI+5YmK6ZmkPC9idXR0b24+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuePvuWcqOWApOODh+ODvOOCv+aXpSAke2guYXNvZnx8J+KAlCd9IC8g5pyA5paw5Y+W5b6X57WC5YCkICR7dmFsaWRQcmljZT95ZW4oaC5jdXJyZW50X3ByaWNlKTon4oCUJ30gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5qCq5L6h44K944O844K5ICR7aC5wcmljZV9zb3VyY2V8fCfigJQnfSAvIOS+oeagvOWxpeattCAke2guaGlzdG9yeV9hc29mfHwn4oCUJ30gJHtoLmhpc3Rvcnlfc291cmNlPycoJytoLmhpc3Rvcnlfc291cmNlKycpJzonJ308L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuWGjeioiOeulyAke2gudXBkYXRlZF9hdD9uZXcgRGF0ZShoLnVwZGF0ZWRfYXQpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpOifigJQnfSAvIOaQjeebiuODu+WbuuWumuODqeOCpOODs+i3nembouOBr+OBk+OBruacgOaWsOWPluW+l+e1guWApOOCkuS9v+eUqDwvZGl2PgogICAgICAke3ZhbGlkUHJpY2U/YDxkaXYgY2xhc3M9ImZyZXNoYm94ICR7ZnJlc2huZXNzRm9yKGguYXNvZikuY2xzfSI+PGI+JHsKICAgICAgICBmcmVzaG5lc3NGb3IoaC5hc29mKS5sZXZlbD09PSdmcmVzaCc/J+KchSDprq7luqZPSyc6CiAgICAgICAgZnJlc2huZXNzRm9yKGguYXNvZikubGV2ZWw9PT0nd2FybmluZyc/J+KaoO+4jyDpgYXlu7bms6jmhI8nOgogICAgICAgICfwn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvycKICAgICAgfTwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+JHtmcmVzaG5lc3NUZXh0KGguYXNvZil9PC9kaXY+PC9kaXY+YDonJ30KCiAgICAgIDxkaXYgY2xhc3M9ImRlY2lzaW9uICR7ZC5jbHN9Ij4KICAgICAgICAke2QubGFiZWx9CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjRweCI+JHtkLnJlYXNvbn08L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMuc3RvcFByaWNlLCdzdG9wJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMudGFrZVByaWNlLCd0YWtlJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKTlrprkv6HpoLzluqY8L3NwYW4+CiAgICAgICAgICA8Yj4ke2QuY29uZmlkZW5jZX0lPC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0iZ2F1Z2UiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke2QuY29uZmlkZW5jZX0lIj48L3NwYW4+PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPuKAu+WIpOWumuS/oemgvOW6puOBr+OAgeODh+ODvOOCv+WFhei2s+W6puODu+acn+mWk+ODiOODrOODs+ODieOBruS4gOiHtOW6puODu+acn+W+heWApOe1seioiOOBruacieeEoeOBi+OCieS9nOOCi+WPguiAg+aMh+aomeOBp+OAgeeahOS4reeiuueOh+OBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICAgIDxkaXYgY2xhc3M9ImFjdGlvbmJveCI+CiAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7ku4rjganjgYbjgZnjgovvvJ88L3NwYW4+CiAgICAgICAgPGIgc3R5bGU9ImRpc3BsYXk6YmxvY2s7bWFyZ2luLXRvcDozcHgiPiR7YWN0aW9uVGV4dChoLGMsZCl9PC9iPgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7Y2xzfSI+JHt2YWxpZFByaWNlP3llbihjLm5ldE5vdyk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt2YWxpZFByaWNlP2ZtdChjLm5ldE5vd1BjdCkrJyUnOifigJQnfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6LK35LuY5omL5pWw5paZPC9zcGFuPjxiPiR7eWVuKGMuYnV5RmVlKX08L2I+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWjsuWNtOaJi+aVsOaWmSjku4opPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy5zZWxsRmVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnN0b3BQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMuc3RvcE5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy50YWtlUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnRha2VOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844Oq44Oz44Kw5Y+C6ICDPC9zcGFuPjxiPiR7dmFsaWRQcmljZSYmYy50cmFpbFByaWNlIT09bnVsbD95ZW4oYy50cmFpbFByaWNlKTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke2ZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKS5kZWNpc2lvbl9vaz8nMjDml6Xpq5jlgKTln7rmupYnOiflsaXmrbTjgYzlj6TjgYTjgZ/jgoHkv53nlZknfTwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn46vIOWun+e4vuODmeODvOOCueS+oeagvOODrOODs+OCuDwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke3JhbmdlSHRtbCgn55+t5pyfIDIw5pelJyxjdXIsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+S4reacnyAxMjbml6UnLGN1cixzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+mVt+acnyAyNTLml6UnLGN1cixzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TkCDlrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtldkh0bWwoJ+efreacnyAyMOaXpScsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+S4reacnyAxMjbml6UnLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke2V2SHRtbCgn6ZW35pyfIDI1MuaXpScsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KICAgICAgPHAgY2xhc3M9Im11dGVkIj7igLvkvqHmoLzjg6zjg7Pjgrjjg7vmnJ/lvoXlgKTjga/lsIbmnaXkuojmuKzjgafjga/jgarjgY/jgIHlj5blvpflj6/og73jgarpgY7ljrvmoKrkvqHjga7jg63jg7zjg6rjg7PjgrDliY3mlrnjg6rjgr/jg7zjg7PliIbluIPjgpLmnIDmlrDlj5blvpfntYLlgKTjgavlvZPjgabjga/jgoHjgZ/ntbHoqIjlj4LogIPjgafjgZnjgILmnJ/plpPjgYzph43jgarjgovmqJnmnKzjgpLlkKvjgb/jgb7jgZnjgII8L3A+CiAgICA8L2Rpdj5gOwogIH0pLmpvaW4oJycpOwogIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0UXVvdGUoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcXVvdGU/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHE9YXdhaXQgci5qc29uKCk7CiAgaWYocS5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcihxLnJlYXNvbnx8cS5lcnJvcnx8J+Wun+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiBxOwp9Cgphc3luYyBmdW5jdGlvbiBhZGRIb2xkaW5nKCl7CiAgY29uc3QgY29kZT0kKCdob2xkQ29kZScpLnZhbHVlLnRyaW0oKTsKICBjb25zdCBjb3N0PXZhbCgnaG9sZENvc3QnKSwgc2hhcmVzPXZhbCgnaG9sZFNoYXJlcycpOwogIGNvbnN0IHN0b3A9dmFsKCdzdG9wUGN0JyksIHRha2U9dmFsKCd0YWtlUGN0JyksIHRyYWlsPXZhbCgndHJhaWxQY3QnKTsKICBjb25zdCBmZWVNb2RlPSQoJ2ZlZU1vZGUnKS52YWx1ZTsKICBpZighY29kZXx8IWNvc3R8fCFzaGFyZXMpe2FsZXJ0KCfpipjmn4TjgrPjg7zjg4njg7vlj5blvpfljZjkvqHjg7vmoKrmlbDjgpLlhaXlipvjgZfjgabjga0nKTtyZXR1cm59CiAgY29uc3QgYnRuPWV2ZW50Py50YXJnZXQ7IGlmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnfQogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogICAgY29uc3QgaD17CiAgICAgIGNvZGUsCiAgICAgIGNvbXBhbnlfbmFtZToocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fCcnLAogICAgICBjb21wYW55X21hcmtldDoocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8JycsCiAgICAgIGNvbXBhbnlfc2VjdG9yMzM6KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8JycsCiAgICAgIGNvc3QsIHNoYXJlcywgZmVlX21vZGU6ZmVlTW9kZSwKICAgICAgc3RvcF9wY3Q6c3RvcD8/OCwgdGFrZV9wY3Q6dGFrZT8/MTUsIHRyYWlsX3BjdDp0cmFpbD8/NywKICAgICAgY3VycmVudF9wcmljZTpzLmxhc3RfY2xvc2UsIGhpZ2hfMjBkOnMuaGlnaF8yMGQsIGxvd18yMGQ6cy5sb3dfMjBkLAogICAgICByZXR1cm5fMjBkOnMucmV0dXJuXzIwZCwgcmV0dXJuXzEyNmQ6cy5yZXR1cm5fMTI2ZCwgcmV0dXJuXzI1MmQ6cy5yZXR1cm5fMjUyZCwKICAgICAgZm9yd2FyZF9zdGF0czpzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSwKICAgICAgYXNvZjpzLmxhc3RfZGF0ZSwKICAgICAgaGlzdG9yeV9hc29mOnMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlLAogICAgICBwcmljZV9zb3VyY2U6cy5wcmljZV9zb3VyY2V8fHEuc291cmNlfHwnJywKICAgICAgaGlzdG9yeV9zb3VyY2U6cy5oaXN0b3J5X3NvdXJjZXx8JycsCiAgICAgIHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpLAogICAgICBwcmljZV9zeW5jZWQ6dHJ1ZQogICAgfTsKICAgIGNvbnN0IGlkeD1hLmZpbmRJbmRleCh4PT54LmNvZGU9PT1jb2RlKTsKICAgIGlmKGlkeD49MClhW2lkeF09aDsgZWxzZSBhLnB1c2goaCk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSl9CiAgZmluYWxseXtpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmCd9fQp9CgoKZnVuY3Rpb24gYXBwbHlRdW90ZVRvSG9sZGluZyhoLHEpewogIGNvbnN0IHM9KHEmJnEuc25hcHNob3QpfHx7fTsKICBoLmNvbXBhbnlfbmFtZT0ocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fGguY29tcGFueV9uYW1lfHwnJzsKICBoLmNvbXBhbnlfbWFya2V0PShxLmNvbXBhbnkmJnEuY29tcGFueS5tYXJrZXQpfHxoLmNvbXBhbnlfbWFya2V0fHwnJzsKICBoLmNvbXBhbnlfc2VjdG9yMzM9KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8aC5jb21wYW55X3NlY3RvcjMzfHwnJzsKICBoLmN1cnJlbnRfcHJpY2U9cy5sYXN0X2Nsb3NlOwogIGguaGlnaF8yMGQ9cy5oaWdoXzIwZDsKICBoLmxvd18yMGQ9cy5sb3dfMjBkOwogIGgucmV0dXJuXzIwZD1zLnJldHVybl8yMGQ7CiAgaC5yZXR1cm5fMTI2ZD1zLnJldHVybl8xMjZkOwogIGgucmV0dXJuXzI1MmQ9cy5yZXR1cm5fMjUyZDsKICBoLmZvcndhcmRfc3RhdHM9cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e307CiAgaC5hc29mPXMubGFzdF9kYXRlOwogIGguaGlzdG9yeV9hc29mPXMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlOwogIGgucHJpY2Vfc291cmNlPXMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8Jyc7CiAgaC5oaXN0b3J5X3NvdXJjZT1zLmhpc3Rvcnlfc291cmNlfHwnJzsKICBoLnVwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogIGgucHJpY2Vfc3luY2VkPXRydWU7CiAgcmV0dXJuIGg7Cn0KCmZ1bmN0aW9uIHN5bmNBbmFseXplZFF1b3RlVG9Ib2xkaW5nKGNvZGUscSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBpPWEuZmluZEluZGV4KHg9PlN0cmluZyh4LmNvZGUpPT09U3RyaW5nKGNvZGUpKTsKICBpZihpPDApcmV0dXJuIGZhbHNlOwogIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhhW2ldLHEpOwogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICByZW5kZXJIb2xkaW5ncygpOwogIHJldHVybiB0cnVlOwp9Cgphc3luYyBmdW5jdGlvbiByZWZyZXNoQWxsSG9sZGluZ3MoZm9yY2U9ZmFsc2UpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgY29uc3QgYnRuPSQoJ3JlZnJlc2hBbGxCdG4nKTsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9J+S/neacieagquOBr+acqueZu+mMsuOBp+OBmeOAgic7CiAgICByZXR1cm47CiAgfQoKICBjb25zdCBrZXk9J2ZyZWVfaG9sZGluZ3NfbGFzdF9hdXRvX3JlZnJlc2hfdjE2JzsKICBjb25zdCBsYXN0PU51bWJlcihsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrZXkpfHwwKTsKICBjb25zdCBub3dNcz1EYXRlLm5vdygpOwogIGNvbnN0IHdhaXRNcz0zMCo2MCoxMDAwOwoKICBpZighZm9yY2UgJiYgbGFzdCAmJiBub3dNcy1sYXN0PHdhaXRNcyl7CiAgICBjb25zdCBtaW49TWF0aC5jZWlsKCh3YWl0TXMtKG5vd01zLWxhc3QpKS82MDAwMCk7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWDoh6rli5Xmm7TmlrDmuIjjgb/jgILmrKHjga7oh6rli5Xmm7TmlrDjgb7jgafntIQke21pbn3liIbjgIJgOwogICAgcmV0dXJuOwogIH0KCiAgaWYoYnRuKXtidG4uZGlzYWJsZWQ9dHJ1ZTtidG4udGV4dENvbnRlbnQ9J+abtOaWsOS4reKApid9CiAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1g5L+d5pyJ5qCqICR7YS5sZW5ndGh96YqY5p+E44Gu5pyA5paw57WC5YCk44KS5Y+W5b6X5Lit4oCmYDsKCiAgbGV0IG9rPTAsIG5nPTA7CiAgZm9yKGxldCBpPTA7aTxhLmxlbmd0aDtpKyspewogICAgdHJ5ewogICAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGFbaV0uY29kZSk7CiAgICAgIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhhW2ldLHEpOwogICAgICBvaysrOwogICAgfWNhdGNoKGUpewogICAgICBuZysrOwogICAgICBhW2ldLmxhc3RfcmVmcmVzaF9lcnJvcj1TdHJpbmcoZS5tZXNzYWdlfHxlKTsKICAgIH0KICB9CgogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrZXksU3RyaW5nKERhdGUubm93KCkpKTsKICByZW5kZXJIb2xkaW5ncygpOwoKICBpZihzdGF0dXMpewogICAgc3RhdHVzLnRleHRDb250ZW50PWDmnIDmlrDntYLlgKTjgaflho3oqIjnrpfvvJrmiJDlip8gJHtva33pipjmn4Qke25nP2AgLyDlpLHmlZcgJHtuZ33pipjmn4RgOicnfeOAgmA7CiAgfQogIGlmKGJ0bil7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5L+d5pyJ5qCq44KS5pyA5paw57WC5YCk44Gn5LiA5ous5pu05pawJ30KfQoKYXN5bmMgZnVuY3Rpb24gcmVmcmVzaEhvbGRpbmcoaSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKSwgaD1hW2ldOyBpZighaClyZXR1cm47CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShoLmNvZGUpOwogICAgYVtpXT1hcHBseVF1b3RlVG9Ib2xkaW5nKGgscSk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogICAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWAke2guY29kZX0g44KS5pyA5paw5Y+W5b6X57WC5YCk44Gn5YaN6KiI566X44GX44G+44GX44Gf44CCYDsKICB9Y2F0Y2goZSl7YWxlcnQoJ+abtOaWsOOCqOODqeODvDogJytlLm1lc3NhZ2UpfQp9CmZ1bmN0aW9uIGVkaXRIb2xkaW5nU2hhcmVzKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyksIGg9YVtpXTsKICBpZighaClyZXR1cm47CiAgY29uc3QgaW5wdXQ9cHJvbXB0KGAke2guY29kZX0g44Gu5paw44GX44GE5L+d5pyJ5qCq5pWw44KS5YWl5Yqb44GX44Gm44Gt77yIMeagquS7peS4iuOBruaVtOaVsO+8iWAsU3RyaW5nKGguc2hhcmVzKSk7CiAgaWYoaW5wdXQ9PT1udWxsKXJldHVybjsKICBjb25zdCB0ZXh0PWlucHV0LnRyaW0oKTsKICBjb25zdCBzaGFyZXM9TnVtYmVyKHRleHQpOwogIGlmKCEvXlswLTldKyQvLnRlc3QodGV4dCl8fCFOdW1iZXIuaXNTYWZlSW50ZWdlcihzaGFyZXMpfHxzaGFyZXM8MSl7CiAgICBhbGVydCgn5qCq5pWw44GvMeS7peS4iuOBruaVtOaVsOOBp+WFpeWKm+OBl+OBpuOBrScpO3JldHVybjsKICB9CiAgaC5zaGFyZXM9c2hhcmVzOwogIGguc2hhcmVzX3VwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICByZW5kZXJIb2xkaW5ncygpOwogIGNvbnN0IHN0YXR1cz0kKCdob2xkaW5nUmVmcmVzaFN0YXR1cycpOwogIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9YCR7aC5jb2RlfSDjgpIgJHtzaGFyZXN95qCq44Gr5aSJ5pu044GX44CB5pCN55uK44O75omL5pWw5paZ44O75L+d5pyJ5YWo5L2T44Gu6ZuG6KiI44KS5YaN6KiI566X44GX44G+44GX44Gf44CCYDsKfQpmdW5jdGlvbiByZW1vdmVIb2xkaW5nKGkpe2NvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7YS5zcGxpY2UoaSwxKTtzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7cmVuZGVySG9sZGluZ3MoKX0KCmZ1bmN0aW9uIHdhdGNoRXNjYXBlKHZhbHVlKXsKICByZXR1cm4gU3RyaW5nKHZhbHVlPz8nJykucmVwbGFjZSgvWyY8PiInXS9nLGM9Pih7JyYnOicmYW1wOycsJzwnOicmbHQ7JywnPic6JyZndDsnLCciJzonJnF1b3Q7JywiJyI6JyYjMzk7J31bY10pKTsKfQpmdW5jdGlvbiByZW5kZXJXYXRjaCgpewogICQoJ3dhdGNocycpLmlubmVySFRNTD1sb2NhbCgnZnJlZV93YXRjaCcpLm1hcCgoeCxpKT0+ewogICAgY29uc3QgY29kZT10eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlOwogICAgY29uc3QgbmFtZT10eXBlb2YgeD09PSdzdHJpbmcnPycnOngubmFtZTsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYmFkZ2UiPjxiPiR7d2F0Y2hFc2NhcGUoY29kZSl9PC9iPiR7bmFtZT8nICcrd2F0Y2hFc2NhcGUobmFtZSk6Jyd9IDxidXR0b24gdHlwZT0iYnV0dG9uIiBjbGFzcz0ic21hbGxidG4gZGFuZ2VyIiBvbmNsaWNrPSJyZW1vdmVXYXRjaCgke2l9KSI+5YmK6ZmkPC9idXR0b24+PC9kaXY+YDsKICB9KS5qb2luKCcnKXx8JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7Cn0KZnVuY3Rpb24gcmVtb3ZlV2F0Y2goaSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV93YXRjaCcpLCB4PWFbaV07CiAgaWYoeD09PXVuZGVmaW5lZClyZXR1cm47CiAgY29uc3QgY29kZT10eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlOwogIGlmKCFjb25maXJtKGAke2NvZGV9IOOCkuOCpuOCqeODg+ODgeODquOCueODiOOBi+OCieWJiumZpOOBl+OBvuOBmeOBi++8n2ApKXJldHVybjsKICBhLnNwbGljZShpLDEpOwogIHNhdmUoJ2ZyZWVfd2F0Y2gnLGEpOwogIHJlbmRlcldhdGNoKCk7Cn0KYXN5bmMgZnVuY3Rpb24gYWRkV2F0Y2goKXsKICBsZXQgYz0kKCd3YXRjaENvZGUnKS52YWx1ZS50cmltKCk7IGlmKCFjKXJldHVybjsKICBsZXQgaW5mbz1udWxsOwogIHRyeXtpbmZvPWF3YWl0IGdldENvbXBhbnkoYyl9Y2F0Y2goZSl7fQogIGxldCBhPWxvY2FsKCdmcmVlX3dhdGNoJyk7CiAgY29uc3QgZXhpc3RzPWEuc29tZSh4PT4odHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSk9PT1jKTsKICBpZighZXhpc3RzKWEucHVzaCh7Y29kZTpjLG5hbWU6aW5mbyYmaW5mby5uYW1lP2luZm8ubmFtZTonJ30pOwogIHNhdmUoJ2ZyZWVfd2F0Y2gnLGEpOwogIHJlbmRlcldhdGNoKCk7Cn0KZnVuY3Rpb24gdXBkYXRlS2FidXRhbigpe2xldCBjPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7JCgna2FidXRhbicpLmhyZWY9Yz8naHR0cHM6Ly9rYWJ1dGFuLmpwL3N0b2NrLz9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpOidodHRwczovL2thYnV0YW4uanAvJ30KJCgnY29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+e3VwZGF0ZUthYnV0YW4oKTtzY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2NvZGUnLCdjb21wYW55TmFtZScsJycpfSk7dXBkYXRlS2FidXRhbigpOwokKCdob2xkQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCdob2xkQ29kZScsJ2hvbGRDb21wYW55TmFtZScsJycpKTsKJCgnd2F0Y2hDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ3dhdGNoQ29kZScsJ3dhdGNoQ29tcGFueU5hbWUnLCcnKSk7CgoKCmFzeW5jIGZ1bmN0aW9uIGdldFBvbGljeShjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9wb2xpY3k/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+WbveetluODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiB4LnBvbGljeXx8e307Cn0KCmZ1bmN0aW9uIHBvbGljeVNvdXJjZVN0YXR1c0phKHMpewogIGlmKHM9PT0ndmVyaWZpZWRfbGl2ZScpcmV0dXJuICflhazlvI/jg5rjg7zjgrjnorroqo3muIgnOwogIGlmKHM9PT0ncGFydGlhbF9saXZlJylyZXR1cm4gJ+WFrOW8j+ODmuODvOOCuOmDqOWIhueiuuiqjSc7CiAgaWYocz09PSd2ZXJpZmllZF9yZWdpc3RyeScpcmV0dXJuICfmnIDntYLnorroqo3muIjlhazlvI/jgr3jg7zjgrknOwogIHJldHVybiAn56K66KqN5LiN5Y+vJzsKfQoKZnVuY3Rpb24gcmVuZGVyUG9saWN5VGhlbWVzKHApewogIGNvbnN0IGJveD0kKCdwb2xpY3lUaGVtZXMnKTsKICBpZighYm94KXJldHVybjsKICBjb25zdCB0aGVtZXM9KHAmJnAubWF0Y2hlZF90aGVtZXMpfHxbXTsKICBpZighdGhlbWVzLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8ZGl2IGNsYXNzPSJwb2xpY3l0aGVtZSI+PGI+6Zai6YCj44OG44O844Oe44Gq44GXIC8g5Yik5a6a5L+d55WZPC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pyA5L2O6Zai6YCj5bqm44KS5rqA44Gf44GZ5YWs5byP5pS/562W44OG44O844Oe44GM44GC44KK44G+44Gb44KT44CCPC9zcGFuPjwvZGl2Pic7CiAgICByZXR1cm47CiAgfQogIGJveC5pbm5lckhUTUw9dGhlbWVzLnNsaWNlKDAsNCkubWFwKHQ9PmAKICAgIDxkaXYgY2xhc3M9InBvbGljeXRoZW1lIj4KICAgICAgPGI+JHt0Lm5hbWV9IC8g6Zai6YCj5bqmICR7KE51bWJlcih0LnJlbGV2YW5jZSkqMTAwKS50b0ZpeGVkKDApfSU8L2I+CiAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5pS/562W5by35bqmICR7TnVtYmVyKHQucG9saWN5X3N0cmVuZ3RoKS50b0ZpeGVkKDApfSAvIOWvhOS4jiAke051bWJlcih0LmNvbnRyaWJ1dGlvbikudG9GaXhlZCgxKX0gLyAke3BvbGljeVNvdXJjZVN0YXR1c0phKHQuc291cmNlX3N0YXR1cyl9PC9zcGFuPjxicj4KICAgICAgPGEgaHJlZj0iJHt0LnVybH0iIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7lhazlvI/jgr3jg7zjgrk8L2E+CiAgICA8L2Rpdj4KICBgKS5qb2luKCcnKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0RnVuZGFtZW50YWxzKGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL2Z1bmRhbWVudGFscz9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5rG6566X44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgcmV0dXJuIHguZnVuZGFtZW50YWxzfHx7fTsKfQoKCmZ1bmN0aW9uIG1hcmtldEZyZXNobmVzc0ZhY3RvcihkYXRlU3RyKXsKICBjb25zdCBmPWZyZXNobmVzc0ZvcihkYXRlU3RyKTsKICBpZihmLmxldmVsPT09J2ZyZXNoJylyZXR1cm4gMS4wMDsKICBpZihmLmxldmVsPT09J3dhcm5pbmcnKXJldHVybiAwLjcwOwogIGlmKGYubGV2ZWw9PT0nc3RhbGUnKXJldHVybiAwLjI1OwogIHJldHVybiAwLjIwOwp9CgpmdW5jdGlvbiBmcmVzaG5lc3NQY3RUZXh0KHYpewogIGlmKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSlyZXR1cm4gJ+KAlCc7CiAgcmV0dXJuIE1hdGgucm91bmQoTnVtYmVyKHYpKjEwMCkrJyUnOwp9CgpmdW5jdGlvbiBmaW5hbmNpYWxGcmVzaG5lc3NMYWJlbChsYWJlbCl7CiAgaWYobGFiZWw9PT0nZnJlc2gnKXJldHVybiAn5paw44GX44GEJzsKICBpZihsYWJlbD09PSdzbGlnaHRseV9vbGQnKXJldHVybiAn44KE44KE5Y+k44GEJzsKICBpZihsYWJlbD09PSdvbGQnKXJldHVybiAn5Y+k44GEJzsKICBpZihsYWJlbD09PSd2ZXJ5X29sZCcpcmV0dXJuICfjgYvjgarjgorlj6TjgYQnOwogIGlmKGxhYmVsPT09J3N0YWxlJylyZXR1cm4gJ+mdnuW4uOOBq+WPpOOBhCc7CiAgcmV0dXJuICfplovnpLrml6XkuI3mmI4nOwp9CgpmdW5jdGlvbiBwb2xpY3lGcmVzaG5lc3NGYWN0b3IocCxtYW51YWxNb2RlKXsKICBpZihtYW51YWxNb2RlKXJldHVybiAxLjAwOwogIGlmKCFwfHxwLnNjb3JlPT09bnVsbHx8cC5zY29yZT09PXVuZGVmaW5lZClyZXR1cm4gMDsKICBjb25zdCBjPU51bWJlcihwLmNvbmZpZGVuY2VfcGN0KTsKICBpZihOdW1iZXIuaXNGaW5pdGUoYykpcmV0dXJuIE1hdGgubWF4KDAsTWF0aC5taW4oMSxjLzEwMCkpOwogIHJldHVybiAwLjUwOwp9CgpmdW5jdGlvbiBzY29yZUxhYmVsKHYpewogIGlmKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSlyZXR1cm4gJ+KAlCc7CiAgY29uc3Qgbj1OdW1iZXIodik7CiAgcmV0dXJuIChuPjA/JysnOicnKStuLnRvRml4ZWQoMSk7Cn0KCmZ1bmN0aW9uIHBjdE1heWJlKHYpewogIHJldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgxKSsnJSc7Cn0KCmFzeW5jIGZ1bmN0aW9uIGFuYWx5emUoKXsKICBjb25zdCBjb2RlPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7CiAgaWYoIWNvZGUpeyQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfpipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjga0nO3JldHVybn0KICBjb25zdCBidG49JCgnYW5hbHl6ZUJ0bicpOyBidG4uZGlzYWJsZWQ9dHJ1ZTsgYnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnOwogICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSdGaW5NaW5k57SEOTAw5pel5L6h5qC85bGl5q2077yLSi1RdWFudHPmsbrnrpfjg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKYnOwoKICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgJCgncHJpY2UnKS52YWx1ZT1zLmxhc3RfY2xvc2U9PW51bGw/Jyc6Zm10KHMubGFzdF9jbG9zZSwxKTsKICAgICQoJ3IyMCcpLnZhbHVlPWZtdChzLnJldHVybl8yMGQpOyQoJ3IxMjYnKS52YWx1ZT1mbXQocy5yZXR1cm5fMTI2ZCk7JCgncjI1MicpLnZhbHVlPWZtdChzLnJldHVybl8yNTJkKTsKICAgICQoJ2hpZ2gyMCcpLnRleHRDb250ZW50PWZtdChzLmhpZ2hfMjBkLDEpOyQoJ2xvdzIwJykudGV4dENvbnRlbnQ9Zm10KHMubG93XzIwZCwxKTsKICAgICQoJ3ZvbDIwJykudGV4dENvbnRlbnQ9cy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkPT1udWxsPyfigJQnOmZtdChzLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQpKyclJzsKCiAgICBkaXNwbGF5Q29tcGFueSgkKCdjb21wYW55TmFtZScpLHEuY29tcGFueXx8bnVsbCwnJyk7CiAgICBjb25zdCBwcmljZVNvdXJjZT1zLnByaWNlX3NvdXJjZXx8cS5zb3VyY2V8fCfkuI3mmI4nOwogICAgY29uc3QgaGlzdG9yeURhdGU9cy5oaXN0b3J5X2xhc3RfZGF0ZXx8bnVsbDsKICAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0KICAgICAgJzxzcGFuIGNsYXNzPSJzb3VyY2ViYWRnZSI+54++5Zyo5YCkPC9zcGFuPjxiIGNsYXNzPSJvayI+JytwcmljZVNvdXJjZSsnPC9iPicrCiAgICAgICc8YnI+54++5Zyo5YCk44OH44O844K/5pelOiAnKyhzLmxhc3RfZGF0ZXx8J+KAlCcpKwogICAgICAocy5wcmljZV90aW1lPycgJytzLnByaWNlX3RpbWU6JycpKwogICAgICAnIC8g5pyA5paw5Y+W5b6X5YCkOiAnK2ZtdChzLmxhc3RfY2xvc2UsMSkrCiAgICAgICc8YnI+PHNwYW4gY2xhc3M9InNvdXJjZWJhZGdlIj7kvqHmoLzlsaXmrbQ8L3NwYW4+JysKICAgICAgKHMuaGlzdG9yeV9zb3VyY2V8fCflj5blvpfjgarjgZcnKSsKICAgICAgJyAvIOacgOe1guaXpTogJysoaGlzdG9yeURhdGV8fCfigJQnKSsKICAgICAgJyAvIOWxpeattOOCteODs+ODl+ODqzogJysocy5zYW1wbGVfY291bnQ/PzApKyfku7YnOwogICAgc2hvd0ZyZXNobmVzcyhzLmxhc3RfZGF0ZSk7CiAgICBzaG93SGlzdG9yeUZyZXNobmVzcyhoaXN0b3J5RGF0ZSxzLmxhc3RfZGF0ZSk7CgogICAgY29uc3QgbWFya2V0RnJlc2huZXNzPW1hcmtldEZyZXNobmVzc0ZhY3RvcihoaXN0b3J5RGF0ZXx8cy5sYXN0X2RhdGUpOwogICAgJCgnbWFya2V0RnJlc2gnKS50ZXh0Q29udGVudD1mcmVzaG5lc3NQY3RUZXh0KG1hcmtldEZyZXNobmVzcyk7CiAgICBjb25zdCBtYXJrZXRBZ2U9ZGF0YUFnZURheXMoaGlzdG9yeURhdGV8fHMubGFzdF9kYXRlKTsKICAgICQoJ21hcmtldEZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgIGDkvqHmoLzlsaXmrbQgJHsoaGlzdG9yeURhdGV8fHMubGFzdF9kYXRlfHwn4oCUJyl9IC8gJHttYXJrZXRBZ2U9PT1udWxsPyfml6XmlbDkuI3mmI4nOm1hcmtldEFnZSsn5pelJ31gOwoKICAgIGNvbnN0IHN5bmNlZEhvbGRpbmc9c3luY0FuYWx5emVkUXVvdGVUb0hvbGRpbmcoY29kZSxxKTsKICAgIGlmKHN5bmNlZEhvbGRpbmcpewogICAgICBjb25zdCBzdGF0dXM9JCgnaG9sZGluZ1JlZnJlc2hTdGF0dXMnKTsKICAgICAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1gJHtjb2RlfSDjga7kv53mnInmoKrjgpLliIbmnpDmmYLjga7mnIDmlrDlj5blvpfntYLlgKQgJHtmbXQocy5sYXN0X2Nsb3NlLDEpfSDjgaflho3oqIjnrpfjgZfjgb7jgZfjgZ/jgIJgOwogICAgfQoKICAgICQoJ2FuYWx5c2lzRXYnKS5pbm5lckhUTUw9CiAgICAgIGV2SHRtbCgn55+t5pyfMjDml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzIwZCddKSsKICAgICAgZXZIdG1sKCfkuK3mnJ8xMjbml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzEyNmQnXSkrCiAgICAgIGV2SHRtbCgn6ZW35pyfMjUy5pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycyNTJkJ10pOwoKICAgIGNvbnN0IHN1cHBseT1zLnN1cHBseV9wcm94eXx8e307CiAgICAkKCdzdXBwbHlBdXRvJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChzdXBwbHkuc2NvcmUpOwogICAgJCgnc3VwcGx5RGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICc15pelLzIw5pel5Ye65p2l6auYICcrKHN1cHBseS52b2x1bWVfcmF0aW9fNV8yMD09bnVsbD8n4oCUJzpOdW1iZXIoc3VwcGx5LnZvbHVtZV9yYXRpb181XzIwKS50b0ZpeGVkKDIpKyflgI0nKTsKCiAgICBsZXQgZnVuZGFtZW50YWxzPXt9OwogICAgdHJ5ewogICAgICBmdW5kYW1lbnRhbHM9YXdhaXQgZ2V0RnVuZGFtZW50YWxzKGNvZGUpOwogICAgICAkKCdlYXJuQXV0bycpLnRleHRDb250ZW50PXNjb3JlTGFiZWwoZnVuZGFtZW50YWxzLnNjb3JlKTsKICAgICAgY29uc3QgbT1mdW5kYW1lbnRhbHMubWV0cmljc3x8e307CiAgICAgIGNvbnN0IGVmPWZ1bmRhbWVudGFscy5mcmVzaG5lc3N8fHt9OwogICAgICAkKCdlYXJuRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgJ+mWi+ekuiAnKygoZnVuZGFtZW50YWxzLmxhdGVzdCYmZnVuZGFtZW50YWxzLmxhdGVzdC5kYXRlKXx8J+KAlCcpKwogICAgICAgICcgLyDlo7LkuIogJytwY3RNYXliZShtLnNhbGVzX2dyb3d0aF9wY3QpKwogICAgICAgICcgLyDllrbmpa3nm4ogJytwY3RNYXliZShtLm9wX2dyb3d0aF9wY3QpOwogICAgICAkKCdlYXJuRnJlc2gnKS50ZXh0Q29udGVudD0KICAgICAgICBlZi5mYWN0b3I9PT11bmRlZmluZWQ/J+KAlCc6TWF0aC5yb3VuZChOdW1iZXIoZWYuZmFjdG9yKSoxMDApKyclJzsKICAgICAgJCgnZWFybkZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgZmluYW5jaWFsRnJlc2huZXNzTGFiZWwoZWYubGFiZWwpKwogICAgICAgICcgLyAnKyhlZi5hZ2VfZGF5cz09PW51bGx8fGVmLmFnZV9kYXlzPT09dW5kZWZpbmVkPyfplovnpLrml6XkuI3mmI4nOmVmLmFnZV9kYXlzKyfml6XliY0nKTsKICAgIH1jYXRjaChmZSl7CiAgICAgICQoJ2Vhcm5BdXRvJykudGV4dENvbnRlbnQ9J+S4jeaYjic7CiAgICAgICQoJ2Vhcm5EZXRhaWwnKS50ZXh0Q29udGVudD0n44GT44Gu44OX44Op44OzL+mKmOafhOOBp+OBr+WPluW+l+OBp+OBjeOBquOBhOWPr+iDveaAp+OBguOCiic7CiAgICAgICQoJ2Vhcm5GcmVzaCcpLnRleHRDb250ZW50PSfigJQnOwogICAgICAkKCdlYXJuRnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0n5rG6566X44OH44O844K/44Gq44GXJzsKICAgICAgZnVuZGFtZW50YWxzPXtzY29yZTpudWxsLGZyZXNobmVzczp7ZmFjdG9yOjB9fTsKICAgIH0KCiAgICBsZXQgYXV0b1BvbGljeT17c2NvcmU6bnVsbCxtYXRjaGVkX3RoZW1lczpbXX07CiAgICB0cnl7CiAgICAgIGF1dG9Qb2xpY3k9YXdhaXQgZ2V0UG9saWN5KGNvZGUpOwogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PWF1dG9Qb2xpY3kuc2NvcmU9PW51bGw/J+S4jeaYjic6c2NvcmVMYWJlbChhdXRvUG9saWN5LnNjb3JlKTsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgYXV0b1BvbGljeS5zY29yZT09bnVsbAogICAgICAgICAgPyAn6Zai6YCj44GZ44KL5YWs5byP5pS/562W44OG44O844Oe44Gq44GXJwogICAgICAgICAgOiAn6Ieq5YuV5Zu9562WcHJveHkgLyDkv6HpoLzluqYgJysoYXV0b1BvbGljeS5jb25maWRlbmNlX3BjdD8/J+KAlCcpKyclIC8gJysoKGF1dG9Qb2xpY3kubWF0Y2hlZF90aGVtZXN8fFtdKS5sZW5ndGgpKyfjg4bjg7zjg54nOwogICAgICBjb25zdCBwZj1wb2xpY3lGcmVzaG5lc3NGYWN0b3IoYXV0b1BvbGljeSxmYWxzZSk7CiAgICAgICQoJ3BvbGljeUZyZXNoJykudGV4dENvbnRlbnQ9ZnJlc2huZXNzUGN0VGV4dChwZik7CiAgICAgICQoJ3BvbGljeUZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgYXV0b1BvbGljeS5zY29yZT09bnVsbAogICAgICAgICAgPyAn6Zai6YCj44OG44O844Oe44Gq44GXJwogICAgICAgICAgOiAoKGF1dG9Qb2xpY3kubWF0Y2hlZF90aGVtZXN8fFtdKS5zb21lKHQ9PnQuc291cmNlX3N0YXR1cz09PSd2ZXJpZmllZF9saXZlJykKICAgICAgICAgICAgICA/ICflhazlvI/jg5rjg7zjgrjjgpLjg6njgqTjg5bnorroqo0nCiAgICAgICAgICAgICAgOiAn56K66KqN5riI44G/5YWs5byP44K944O844K544KS5L2/55SoJyk7CiAgICAgIHJlbmRlclBvbGljeVRoZW1lcyhhdXRvUG9saWN5KTsKICAgIH1jYXRjaChwZSl7CiAgICAgICQoJ3BvbGljeVN0YXRlJykudGV4dENvbnRlbnQ9J+S4jeaYjic7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PSflhazlvI/mlL/nrZbjgr3jg7zjgrnlj5blvpfjgqjjg6njg7wnOwogICAgICAkKCdwb2xpY3lGcmVzaCcpLnRleHRDb250ZW50PSfigJQnOwogICAgICAkKCdwb2xpY3lGcmVzaERldGFpbCcpLnRleHRDb250ZW50PSflj5blvpfjgqjjg6njg7wnOwogICAgICAkKCdwb2xpY3lUaGVtZXMnKS5pbm5lckhUTUw9Jyc7CiAgICAgIGF1dG9Qb2xpY3k9e3Njb3JlOm51bGwsbWF0Y2hlZF90aGVtZXM6W119OwogICAgfQoKICAgIGNvbnN0IG1hbnVhbFBvbGljeT12YWwoJ3BvbGljeScpOwogICAgY29uc3QgcG9saWN5U2NvcmU9bWFudWFsUG9saWN5PT09bnVsbD9hdXRvUG9saWN5LnNjb3JlOm1hbnVhbFBvbGljeTsKICAgIGNvbnN0IHBvbGljeU1vZGU9bWFudWFsUG9saWN5PT09bnVsbD8nYXV0byc6J21hbnVhbCc7CiAgICBpZihtYW51YWxQb2xpY3khPT1udWxsKXsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKG1hbnVhbFBvbGljeSk7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PSfmiYvlhaXlipvjgafoh6rli5XlgKTjgpLkuIrmm7jjgY0nOwogICAgICAkKCdwb2xpY3lGcmVzaCcpLnRleHRDb250ZW50PScxMDAlJzsKICAgICAgJCgncG9saWN5RnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0n44Om44O844K244O85omL5YWl5Yqb5YCkJzsKICAgIH0KCiAgICBjb25zdCBkPXsKICAgICAgY29kZSwKICAgICAgcHJpY2U6cy5sYXN0X2Nsb3NlLAogICAgICByZXR1cm4yMDpzLnJldHVybl8yMGQsCiAgICAgIHJldHVybjEyNjpzLnJldHVybl8xMjZkLAogICAgICByZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCwKICAgICAgZWFybmluZ3Nfc2NvcmU6ZnVuZGFtZW50YWxzLnNjb3JlLAogICAgICBwb2xpY3lfc2NvcmU6cG9saWN5U2NvcmUsCiAgICAgIHBvbGljeV9tb2RlOnBvbGljeU1vZGUsCiAgICAgIHN1cHBseV9zY29yZTpzdXBwbHkuc2NvcmUsCiAgICAgIG1hcmtldF9mcmVzaG5lc3M6bWFya2V0RnJlc2huZXNzLAogICAgICBlYXJuaW5nc19mcmVzaG5lc3M6TnVtYmVyKAogICAgICAgIGZ1bmRhbWVudGFscy5mcmVzaG5lc3MmJmZ1bmRhbWVudGFscy5mcmVzaG5lc3MuZmFjdG9yIT09dW5kZWZpbmVkCiAgICAgICAgICA/IGZ1bmRhbWVudGFscy5mcmVzaG5lc3MuZmFjdG9yCiAgICAgICAgICA6IChmdW5kYW1lbnRhbHMuc2NvcmU9PW51bGw/MDoxKQogICAgICApLAogICAgICBwb2xpY3lfZnJlc2huZXNzOnBvbGljeUZyZXNobmVzc0ZhY3RvcigKICAgICAgICBhdXRvUG9saWN5LAogICAgICAgIG1hbnVhbFBvbGljeSE9PW51bGwKICAgICAgKQogICAgfTsKCiAgICBjb25zdCBhcj1hd2FpdCBmZXRjaCgnL2FwaS9mcmVlL2FuYWx5emUnLHsKICAgICAgbWV0aG9kOidQT1NUJywKICAgICAgaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSwKICAgICAgYm9keTpKU09OLnN0cmluZ2lmeShkKSwKICAgICAgY2FjaGU6J25vLXN0b3JlJwogICAgfSk7CiAgICBjb25zdCB4PWF3YWl0IGFyLmpzb24oKTsKICAgIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfliIbmnpDjg4fjg7zjgr/jgYzkuI3otrPjgZfjgabjgYTjgb7jgZknKTsKCiAgICAkKCdzdGF0ZScpLnRleHRDb250ZW50PXN0YXRlSmEoeC5zaWduYWwuc3RhdGUpOwogICAgJCgncG9zJykudGV4dENvbnRlbnQ9eC5zaWduYWwucG9zaXRpdmVfY291bnQ7CiAgICAkKCduZWcnKS50ZXh0Q29udGVudD14LnNpZ25hbC5uZWdhdGl2ZV9jb3VudDsKCiAgICBjb25zdCBzYz14LnNjb3JlfHx7fTsKICAgICQoJ3Njb3JlSGVybycpLnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgICQoJ3Njb3JlMTAwJykudGV4dENvbnRlbnQ9c2Muc2NvcmUxMDA9PW51bGw/J+KAlCc6c2Muc2NvcmUxMDArJyAvIDEwMCc7CgogICAgY29uc3QgcGlsbD0kKCdzY29yZVN0YXRlUGlsbCcpOwogICAgcGlsbC5jbGFzc05hbWU9J3N0YXRlcGlsbCAnK3N0YXRlQ2xhc3MoeC5zaWduYWwuc3RhdGUpOwogICAgcGlsbC50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTsKCiAgICAkKCdjb3ZlcmFnZScpLnRleHRDb250ZW50PXNjLmNvdmVyYWdlX3BjdD09bnVsbD8n4oCUJzpzYy5jb3ZlcmFnZV9wY3QrJyUnOwogICAgJCgnZnJlc2hDb3ZlcmFnZScpLnRleHRDb250ZW50PXNjLmNvdmVyYWdlX3BjdD09bnVsbD8n4oCUJzpzYy5jb3ZlcmFnZV9wY3QrJyUnOwogICAgY29uc3Qgc2Y9c2MuZnJlc2huZXNzfHx7fTsKICAgIGlmKHNmLm1hcmtldF9wY3QhPT11bmRlZmluZWQpJCgnbWFya2V0RnJlc2gnKS50ZXh0Q29udGVudD1NYXRoLnJvdW5kKE51bWJlcihzZi5tYXJrZXRfcGN0KSkrJyUnOwogICAgaWYoc2YuZWFybmluZ3NfcGN0IT09dW5kZWZpbmVkKSQoJ2Vhcm5GcmVzaCcpLnRleHRDb250ZW50PU1hdGgucm91bmQoTnVtYmVyKHNmLmVhcm5pbmdzX3BjdCkpKyclJzsKICAgIGlmKHNmLnBvbGljeV9wY3QhPT11bmRlZmluZWQpJCgncG9saWN5RnJlc2gnKS50ZXh0Q29udGVudD1NYXRoLnJvdW5kKE51bWJlcihzZi5wb2xpY3lfcGN0KSkrJyUnOwoKICAgICQoJ3Njb3JlQnJlYWtkb3duJykudGV4dENvbnRlbnQ9CiAgICAgICfjg4bjgq/jg4vjgqvjg6sgJytzY29yZUxhYmVsKHNjLnRlY2huaWNhbCkrCiAgICAgICcgLyDmsbrnrpcgJytzY29yZUxhYmVsKHNjLmVhcm5pbmdzKSsKICAgICAgJyAvIOmcgOe1piAnK3Njb3JlTGFiZWwoc2Muc3VwcGx5KSsKICAgICAgJyAvIOWbveetlnByb3h5ICcrc2NvcmVMYWJlbChzYy5wb2xpY3kpOwoKICAgICQoJ3Njb3JlUmVhc29uJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmVSZWFzb25UZXh0JykudGV4dENvbnRlbnQ9ZHJpdmVyU2VudGVuY2Uoc2MpOwogICAgJCgnZHJpdmVyR3JpZCcpLmlubmVySFRNTD0KICAgICAgZHJpdmVyQm94SHRtbCgn5pyA5aSn44Gu44OX44Op44K56KaB5ZugJyxzYy5zdHJvbmdlc3RfcG9zaXRpdmUsJ3Bvc2l0aXZlJykrCiAgICAgIGRyaXZlckJveEh0bWwoJ+acgOWkp+OBruODnuOCpOODiuOCueimgeWboCcsc2Muc3Ryb25nZXN0X25lZ2F0aXZlLCduZWdhdGl2ZScpOwoKICAgIHJlbmRlckNvbnRyaWJ1dGlvbnMoc2MpOwoKICAgIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihzLmxhc3RfZGF0ZSk7CiAgICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlKTsKICAgIGxldCByZXN1bHRUZXh0PQogICAgICAhZnJlc2guZGVjaXNpb25fb2sKICAgICAgICA/ICfimqDvuI8g54++5Zyo5YCk44OH44O844K/44GM5Y+k44GE44Gf44KB44CB5LuK5pel44Gu5aOy6LK35Yik5pat44Go44GX44Gm44Gv5L2/55So44GX44G+44Gb44KT44CCJwogICAgICAgIDogIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vawogICAgICAgICAgPyAn4pqg77iPIOePvuWcqOWApOOBr+aWsOOBl+OBhOaXpei2s+OCkuS9v+OBo+OBpuOBhOOBvuOBmeOBjOOAgeS+oeagvOWxpeattOOBjOWPpOOBhOOBn+OCgee3j+WQiOeCueODu+efreS4remVt+acn+ODiOODrOODs+ODieODu+acn+W+heWApOOBr+WPguiAg+WApOOBp+OBmeOAgicKICAgICAgICAgIDogJ+ePvuWcqOWApOOBqOS+oeagvOWxpeattOOBrumuruW6puOCkueiuuiqjea4iOOBv+OAgic7CgogICAgY29uc3QgZWZhY3Rvcj1OdW1iZXIoCiAgICAgIGZ1bmRhbWVudGFscy5mcmVzaG5lc3MmJmZ1bmRhbWVudGFscy5mcmVzaG5lc3MuZmFjdG9yIT09dW5kZWZpbmVkCiAgICAgICAgPyBmdW5kYW1lbnRhbHMuZnJlc2huZXNzLmZhY3RvcgogICAgICAgIDogMQogICAgKTsKICAgIGlmKGZ1bmRhbWVudGFscy5zY29yZSE9PW51bGwmJmVmYWN0b3I8MSl7CiAgICAgIHJlc3VsdFRleHQrPWAg5rG6566X44Gv6ZaL56S644GL44KJ5pmC6ZaT44GM57WM44Gj44Gm44GE44KL44Gf44KB44CB57eP5ZCI54K544Gn44Gv5Z+65rqW6YeN44G/MzAl44Gr6a6u5bqmJHtNYXRoLnJvdW5kKGVmYWN0b3IqMTAwKX0l44KS5o6b44GR44Gm5b2x6Z+/44KS5byx44KB44Gm44GE44G+44GZ44CCYDsKICAgIH0KICAgIGlmKHBvbGljeU1vZGU9PT0nYXV0bycmJnBvbGljeVNjb3JlIT09bnVsbCl7CiAgICAgIGNvbnN0IHBmPXBvbGljeUZyZXNobmVzc0ZhY3RvcihhdXRvUG9saWN5LGZhbHNlKTsKICAgICAgaWYocGY8MSl7CiAgICAgICAgcmVzdWx0VGV4dCs9YCDlm73nrZZwcm94eeOCguOCveODvOOCueeiuuiqjeeKtuaFi+OBq+W/nOOBmOOBpumuruW6piR7TWF0aC5yb3VuZChwZioxMDApfSXjgafph43jgb/oqr/mlbTjgZfjgabjgYTjgb7jgZnjgIJgOwogICAgICB9CiAgICB9CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD1yZXN1bHRUZXh0OwogIH1jYXRjaChlKXsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfimqDvuI8gJytlLm1lc3NhZ2U7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9JzxzcGFuIGNsYXNzPSJlcnIiPuWPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UrJzwvc3Bhbj4nOwogIH1maW5hbGx5ewogICAgYnRuLmRpc2FibGVkPWZhbHNlOwogICAgYnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafliIbmnpAnOwogIH0KfQoKcmVuZGVySG9sZGluZ3MoKTtyZW5kZXJXYXRjaCgpOwpzZXRUaW1lb3V0KCgpPT5yZWZyZXNoQWxsSG9sZGluZ3MoZmFsc2UpLDQwMCk7CmlmKCdzZXJ2aWNlV29ya2VyJyBpbiBuYXZpZ2F0b3Ipe25hdmlnYXRvci5zZXJ2aWNlV29ya2VyLmdldFJlZ2lzdHJhdGlvbnMoKS50aGVuKHJzPT5Qcm9taXNlLmFsbChycy5tYXAocj0+ci51bnJlZ2lzdGVyKCkpKSkuY2F0Y2goKCk9Pnt9KX0KaWYoJ2NhY2hlcycgaW4gd2luZG93KXtjYWNoZXMua2V5cygpLnRoZW4oa2V5cz0+UHJvbWlzZS5hbGwoa2V5cy5tYXAoaz0+Y2FjaGVzLmRlbGV0ZShrKSkpKS5jYXRjaCgoKT0+e30pfQo8L3NjcmlwdD4KPC9tYWluPgo8L2JvZHk+CjwvaHRtbD4='
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
