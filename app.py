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
            raise ValueError('鬩ｫ菫ｶ豌幄叉ﾂ髫包ｽｧ郢ｧ雋槫徐陟募干縲堤ｸｺ髦ｪ竏ｪ邵ｺ蟶呻ｽ鍋ｸｺ�ｧ邵ｺ蜉ｱ笳�')
        catalog={}
        for row in payload.get('data') or []:
            symbol=str(row.get('stock_id') or '').strip()
            if not symbol.endswith('.T'):continue
            code=symbol[:-2]
            if len(code)!=4 or not code.isalnum():continue
            catalog[code]={'code':code,'name':str(row.get('stock_name') or '').strip(),'sector':row.get('Sector') or '', 'catalog_date':row.get('date') or ''}
        if not catalog:raise ValueError('鬩ｫ菫ｶ豌幄叉ﾂ髫包ｽｧ邵ｺ讙趣ｽｩ�ｺ邵ｺ�ｧ邵ｺ�ｽ')
        result={'status':'ok','stocks':[catalog[k] for k in sorted(catalog)],'count':len(catalog),'source':'FinMind JapanStockInfo (.T)','fetched_at':datetime.now(timezone.utc).isoformat()}
        FREE_UNIVERSE_CACHE.update(result=result,ts=now)
        return jsonify(result)
    except Exception:
        return jsonify(status='error',reason='霎滂ｽ｡隴∝生�ｽ鬩ｫ菫ｶ豌幄叉ﾂ髫包ｽｧ郢ｧ雋槫徐陟募干縲堤ｸｺ髦ｪ竏ｪ邵ｺ蟶呻ｽ鍋ｸｲ繧亥�鬮｢阮呻ｽ帝→�ｺ邵ｺ莉｣窶ｻ陷讎奇ｽｺ�ｦ髫ｧ�ｦ邵ｺ蜉ｱ窶ｻ邵ｺ荳岩味邵ｺ霈費ｼ樒ｸｲ�ｽ'),502

HTML = base64.b64decode(
    'PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9CgoucG9saWN5dGhlbWUgYXtmb250LXNpemU6MTJweH0KLmNvbnRyaWJncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQouY29udHJpYntib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDtiYWNrZ3JvdW5kOiNmZmZ9Ci5jb250cmliIHNwYW57ZGlzcGxheTpibG9ja30KLmNvbnRyaWIgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4O21hcmdpbi10b3A6MnB4fQoKLmNvbnRyaWIgc21hbGx7ZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweDtjb2xvcjojNmI3MjgwO2xpbmUtaGVpZ2h0OjEuMzV9Ci5mcmVzaGJveHtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMXB4O21hcmdpbi10b3A6OXB4O2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLmZyZXNoLW9re2JhY2tncm91bmQ6I2VjZmRmNTtib3JkZXItY29sb3I6I2E3ZjNkMH0KLmZyZXNoLXdhcm57YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1jb2xvcjojZmRlNjhhfQouZnJlc2gtc3RhbGV7YmFja2dyb3VuZDojZmVmMmYyO2JvcmRlci1jb2xvcjojZmVjYWNhfQoKLmZyZXNoYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweH0KLnNvdXJjZWJhZGdle2Rpc3BsYXk6aW5saW5lLWJsb2NrO3BhZGRpbmc6NHB4IDhweDtib3JkZXItcmFkaXVzOjk5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTJweDtmb250LXdlaWdodDo4MDA7bWFyZ2luLXJpZ2h0OjRweH0KLmhpc3Rvcnl3YXJue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDttYXJnaW4tdG9wOjdweDtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyOjFweCBzb2xpZCAjZmRlNjhhfQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5jb250cmliZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cgouc3RvY2stZGV0YWlsc3tib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxNHB4O21hcmdpbi10b3A6MTBweDtiYWNrZ3JvdW5kOndoaXRlfQouc3RvY2stZGV0YWlscz5zdW1tYXJ5e3BhZGRpbmc6MTZweDtjdXJzb3I6cG9pbnRlcjtmb250LXdlaWdodDo3MDA7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnN0b2NrLWRldGFpbHNbb3Blbl0+c3VtbWFyeXtib3JkZXItYm90dG9tOjFweCBzb2xpZCAjZTVlN2VifQouc3RvY2stZGV0YWlscz4uaG9sZGluZ3tib3JkZXI6MDttYXJnaW46MH0KLndhdGNoLWNvbnRlbnR7cGFkZGluZzoxMnB4fQoKLndhdGNoLXRpbWluZ3tkaXNwbGF5OmlubGluZS1ibG9jaztmb250LXNpemU6MTJweDtmb250LXdlaWdodDo3MDA7Ym9yZGVyLXJhZGl1czoyMHB4O3BhZGRpbmc6NXB4IDlweDttYXJnaW4tbGVmdDo2cHg7dmVydGljYWwtYWxpZ246bWlkZGxlfQoud2F0Y2gtYnV5e2JhY2tncm91bmQ6I2RjZmNlNztjb2xvcjojMTY2NTM0fS53YXRjaC1uZXV0cmFse2JhY2tncm91bmQ6I2YxZjVmOTtjb2xvcjojMzM0MTU1fS53YXRjaC1wZW5kaW5ne2JhY2tncm91bmQ6I2ZlZjNjNztjb2xvcjojOTI0MDBlfQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8bWFpbj4KPGRpdiBjbGFzcz0idG9wIj4KICA8aDE+8J+TiCDml6XmnKzmoKpBSSBGUkVFPC9oMT4KICA8ZGl2IGNsYXNzPSJzdWIiPuimgeWboOWIpemuruW6piAvIEZpbk1pbmTkvqHmoLzlsaXmrbQgLyBKLVF1YW50c+axuueulyAvIOWbveetljwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn46vIOmKmOafhOWIhuaekDwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZCI+CiAgICA8aW5wdXQgaWQ9ImNvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSDkvosgNzIwMyI+CiAgICA8aW5wdXQgaWQ9InByaWNlIiBwbGFjZWhvbGRlcj0i5Y+W5b6X57WC5YCkIiByZWFkb25seT4KICA8L2Rpdj4KICA8ZGl2IGlkPSJjb21wYW55TmFtZSIgY2xhc3M9InNvdXJjZSBtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZnjgovjgajkvJrnpL7lkI3jgpLooajnpLrjgZfjgb7jgZk8L2Rpdj4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGEgaWQ9ImthYnV0YW4iIGNsYXNzPSJidG4gc2Vjb25kYXJ5IiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+5qCq5o6i44Gn56K66KqNPC9hPgogICAgPGJ1dHRvbiBpZD0iYW5hbHl6ZUJ0biIgb25jbGljaz0iYW5hbHl6ZSgpIj7lrp/jg4fjg7zjgr/jgafliIbmnpA8L2J1dHRvbj4KICA8L2Rpdj4KICA8cCBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeOCjOOBpuaKvOOBmeOBqOOAgeePvuWcqOWApOOBqDIw5pel44O7MTI25pel44O7MjUy5pel44Gu5L6h5qC85bGl5q2044GvRmluTWluZOaXpeacrOagquaXpei2s+OCkuacgOWEquWFiOOBl+OBvuOBmeOAgumKmOafhOWQjeODu+axuueul+ODu+WbveetluWIpOWumuOBr0otUXVhbnRz562J44KS5L2/55So44GX44G+44GZ44CCPC9wPgogIDxkaXYgaWQ9InNvdXJjZUJveCIgY2xhc3M9InNvdXJjZSBtdXRlZCI+44OH44O844K/5pyq5Y+W5b6XPC9kaXY+CiAgPGRpdiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJtYXJnaW4tdG9wOjdweCI+CiAgICA8Yj7wn4aTIOeEoeaWmeS+oeagvOWxpeattOOBq+OBpOOBhOOBpjwvYj4KICAgIDxkaXYgY2xhc3M9Im11dGVkIj5GaW5NaW5k44Gu5pel5pys5qCq5pel6Laz44KS57SEOTAw5pel5YiG5Y+W5b6X44GX44CB5pyA5paw5Za25qWt5pel44Gu57WC5YCk44O7MjAvMTI2LzI1MuaXpeODquOCv+ODvOODs+ODuzIw5pel6auY5a6J44O75Ye65p2l6auYcHJveHnjg7vjg63jg7zjg6rjg7PjgrDlrp/nuL7liIbluIPjgpLoqIjnrpfjgZfjgb7jgZnjgILlj5blvJXkuK3jga7jg6rjgqLjg6vjgr/jgqTjg6DkvqHmoLzjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzQm94IiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPGI+8J+TmiDkvqHmoLzliIbmnpDlsaXmrbTjga7prq7luqY8L2I+CiAgICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzVGV4dCIgY2xhc3M9Im11dGVkIj48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJmcmVzaG5lc3NCb3giIGNsYXNzPSJmcmVzaGJveCBmcmVzaC13YXJuIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxiIGlkPSJmcmVzaG5lc3NUaXRsZSI+44OH44O844K/6a6u5bqmPC9iPgogICAgPGRpdiBpZD0iZnJlc2huZXNzRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5OKIOagquS+oeODu+ODhuOCr+ODi+OCq+ODq+Wun+e4vjwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6aiw6JC9546HICU8L3NwYW4+PGlucHV0IGlkPSJyMjAiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjEyNuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjEyNiIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjUy5pelICU8L3NwYW4+PGlucHV0IGlkPSJyMjUyIiByZWFkb25seT48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemrmOWApDwvc3Bhbj48YiBpZD0iaGlnaDIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlronlgKQ8L3NwYW4+PGIgaWQ9ImxvdzIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlubTnjofjg5zjg6k8L3NwYW4+PGIgaWQ9InZvbDIwIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6kg5a6f44OH44O844K/6KaB5ZugPC9oMz4KICA8cCBjbGFzcz0ibXV0ZWQiPuS+oeagvOODu+mcgOe1puODu+axuueul+ODu+WbveetluOBr+OBneOCjOOBnuOCjOWIpeOBq+muruW6puOCkuWIpOWumuOBl+OBvuOBmeOAguWPpOOBhOimgeWboOOBr+WApOOCkua2iOOBleOBmuOAgee3j+WQiOeCueOBuOOBrumHjeOBv+OBoOOBkeiHquWLleOBp+S4i+OBkuOBvuOBmeOAgjwvcD4KICA8ZGl2IGNsYXNzPSJmYWN0b3JncmlkIj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpc8L3NwYW4+PGIgaWQ9ImVhcm5BdXRvIj7igJQ8L2I+PHNtYWxsIGlkPSJlYXJuRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWPluW+lzwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZyA57WmcHJveHk8L3NwYW4+PGIgaWQ9InN1cHBseUF1dG8iPuKAlDwvYj48c21hbGwgaWQ9InN1cHBseURldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrlj5blvpc8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetlnByb3h5PC9zcGFuPjxiIGlkPSJwb2xpY3lTdGF0ZSI+4oCUPC9iPjxzbWFsbCBpZD0icG9saWN5RGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuWFrOW8j+aUv+etluOCveODvOOCueeiuuiqjeWJjTwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5a6f5Yq544OH44O844K/5YWF6LazPC9zcGFuPjxiIGlkPSJjb3ZlcmFnZSI+4oCUPC9iPjxzbWFsbCBjbGFzcz0ibXV0ZWQiPumuruW6puOBvuOBp+WPjeaYoOOBl+OBn+mHjeOBvzwvc21hbGw+PC9kaXY+CiAgPC9kaXY+CiAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDZweCI+8J+VkiDopoHlm6DliKXjga7prq7luqY8L2g0PgogIDxkaXYgY2xhc3M9ImZhY3RvcmdyaWQiPgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS+oeagvOWxpeattDwvc3Bhbj48YiBpZD0ibWFya2V0RnJlc2giPuKAlDwvYj48c21hbGwgaWQ9Im1hcmtldEZyZXNoRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWIpOWumjwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5rG6566XPC9zcGFuPjxiIGlkPSJlYXJuRnJlc2giPuKAlDwvYj48c21hbGwgaWQ9ImVhcm5GcmVzaERldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrliKTlrpo8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetljwvc3Bhbj48YiBpZD0icG9saWN5RnJlc2giPuKAlDwvYj48c21hbGwgaWQ9InBvbGljeUZyZXNoRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWIpOWumjwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6a6u5bqm5Y+N5pig5b6M44Kr44OQ44O8546HPC9zcGFuPjxiIGlkPSJmcmVzaENvdmVyYWdlIj7igJQ8L2I+PHNtYWxsIGNsYXNzPSJtdXRlZCI+5Y+k44GE6KaB5Zug44Gv6YeN44G/44KS5rib6KGwPC9zbWFsbD48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJwb2xpY3lUaGVtZXMiIGNsYXNzPSJwb2xpY3l0aGVtZXMiPjwvZGl2PgogIDxkaXYgc3R5bGU9Im1hcmdpbi10b3A6OXB4Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Zu9562W44K544Kz44Ki5LiK5pu444GN77yI5Lu75oSP77yJIC0xMDDjgJwxMDA8L3NwYW4+CiAgICA8aW5wdXQgaWQ9InBvbGljeSIgaW5wdXRtb2RlPSJkZWNpbWFsIiBwbGFjZWhvbGRlcj0i56m65qyE44Gq44KJ6Ieq5YuV5Zu9562WcHJveHnjgpLkvb/nlKgiPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+4oC75Zu9562WcHJveHnjga/jgIHmlL/lupzlhazlvI/mlL/nrZbjgr3jg7zjgrnjgahKLVF1YW50c+OBrualreeoruODu+S8muekvuWQjeOBqOOBrumWoumAo+W6puOCkue1hOOBv+WQiOOCj+OBm+OBn+WPguiAg+WApOOBp+OBmeOAguODqeOCpOODlueiuuiqjeOBp+OBjeOBquOBhOWgtOWQiOOBr+acgOe1gueiuuiqjea4iOOBv+aDheWgseOCkuS9juS/oemgvOW6puOBp+S9v+eUqOOBl+OAgeOBneOBrueKtuaFi+OCgueUu+mdouOBq+aYjuekuuOBl+OBvuOBmeOAguijnOWKqemHkeaOoeaKnuOChOalree4vuaBqeaBteOBneOBruOCguOBruOCkuiovOaYjuOBmeOCi+aMh+aomeOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+noCDliIbmnpDntZDmnpw8L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nirbmhYs8L3NwYW4+PGIgaWQ9InN0YXRlIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OX44Op44K55qC55ougPC9zcGFuPjxiIGlkPSJwb3MiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg57jgqTjg4rjgrnmoLnmi6A8L3NwYW4+PGIgaWQ9Im5lZyI+4oCUPC9iPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InNjb3JlSGVybyIgY2xhc3M9InNjb3JlaGVybyIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+WQiOaOoeeCue+8iOWPluW+l+ODh+ODvOOCv+ODu+ODq+ODvOODq+ODmeODvOOCue+8iTwvc3Bhbj4KICAgIDxiIGlkPSJzY29yZTEwMCI+4oCUPC9iPgogICAgPHNwYW4gaWQ9InNjb3JlU3RhdGVQaWxsIiBjbGFzcz0ic3RhdGVwaWxsIHN0YXRlLW5ldXRyYWwiPuKAlDwvc3Bhbj4KICAgIDxkaXYgaWQ9InNjb3JlQnJlYWtkb3duIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0ic2NvcmVSZWFzb24iIGNsYXNzPSJzY29yZS1yZWFzb24iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPHN0cm9uZz7wn6etIOOBquOBnOOBk+OBrueCueaVsO+8nzwvc3Ryb25nPgogICAgPGRpdiBpZD0ic2NvcmVSZWFzb25UZXh0IiBjbGFzcz0ibXV0ZWQiPuKAlDwvZGl2PgogICAgPGRpdiBpZD0iZHJpdmVyR3JpZCIgY2xhc3M9ImRyaXZlcmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbnRyaWJ1dGlvbkJveCIgY2xhc3M9InNjb3JlLXJlYXNvbiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3Ryb25nPvCfp64g57eP5ZCI54K544G444Gu5a+E5LiOPC9zdHJvbmc+CiAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5Y+W5b6X44Gn44GN44Gf6KaB5Zug44Gr6a6u5bqm5L+C5pWw44KS5o6b44GR44Gm44GL44KJ6YeN44G/44KS5YaN6YWN5YiG44GX44CB5ZCE6KaB5Zug44GM57eP5ZCI6KmV5L6h44KS44Gp44KM44Gg44GR5oq844GX5LiK44GS77yP5oq844GX5LiL44GS44Gf44GL44KS6KGo56S644GX44G+44GZ44CCPC9kaXY+CiAgICA8ZGl2IGlkPSJjb250cmlidXRpb25HcmlkIiBjbGFzcz0iY29udHJpYmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxwIGlkPSJyZXN1bHQiIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44CM5a6f44OH44O844K/44Gn5YiG5p6Q44CN44KS5oq844GX44Gm44GP44Gg44GV44GE44CCPC9wPgogIDxwIGNsYXNzPSJtdXRlZCI+5o6h54K55biv77yaODDjgJwxMDAg5by35rCXIC8gNjXjgJw3OSDjgoTjgoTlvLfmsJcgLyA0NeOAnDY0IOS4reeriyAvIDMw44CcNDQg44KE44KE5byx5rCXIC8gMOOAnDI5IOW8seawlzwvcD4KICA8ZGl2IGlkPSJhbmFseXNpc0V2IiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfmqgg5LuK5pel44Gu5YSq5YWI44Ki44Kv44K344On44OzPC9oMz4KICA8ZGl2IGlkPSJwcmlvcml0eUFjdGlvbnMiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CjxoMz7wn5OIIOaXpeacrOagquOBruazqOebruODu+S4i+iQveitpuaIku+8iOeEoeaWmeW3oeWbnu+8iTwvaDM+CjxwIGNsYXNzPSJtdXRlZCI+5a++6LGh44GvRmluTWluZOOBruaXpeacrOagqumKmOafhOS4gOimp++8iC5U77yJ44CC5L+d5pyJ44O744Km44Kp44OD44OB55m76Yyy44Gv5LiN6KaB44Gn44GZ44CC54Sh5paZ5p6g44GnMjDpipjmn4TjgZrjgaToqr/jgbnjgIHlj5blvpfjgafjgY3jgZ/nr4Tlm7LjgYvjgonlgJnoo5zjgpLooajnpLrjgZfjgb7jgZnjgILlhajluILloLTjgpLlkIzmmYLjgavmr5TovIPjgZfjgZ/jg6njg7Pjgq3jg7PjgrDjgafjga/jgYLjgorjgb7jgZvjgpPjgILlkITmrITjga/mnIDlpKc1MOS7tuOCkuihqOekuuOBl+OBvuOBmeOAguaXpei2s+e1guWApOOBp+WIpOWumuOBl+OBvuOBmeOAgjwvcD4KPGxhYmVsIGZvcj0ibWFya2V0R2VucmUiPuiqv+OBueOCi+OCuOODo+ODs+ODqzwvbGFiZWw+CjxzZWxlY3QgaWQ9Im1hcmtldEdlbnJlIiBvbmNoYW5nZT0iY2hhbmdlTWFya2V0R2VucmUoKSIgc3R5bGU9IndpZHRoOjEwMCU7cGFkZGluZzoxMnB4O2JvcmRlcjoxcHggc29saWQgI2RkZDtib3JkZXItcmFkaXVzOjEycHg7bWFyZ2luOjhweCAwIj48b3B0aW9uIHZhbHVlPSLlhajmpa3nqK4iPuWFqOalreeorjwvb3B0aW9uPjwvc2VsZWN0Pgo8YnV0dG9uIGlkPSJtYXJrZXRHZW5yZUxvYWQiIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9ImxvYWRNYXJrZXRHZW5yZXMoKSIgc3R5bGU9Im1hcmdpbi1ib3R0b206OHB4Ij7jgrjjg6Pjg7Pjg6vkuIDopqfjgpLoqq3jgb/ovrzjgoA8L2J1dHRvbj4KPHAgY2xhc3M9Im11dGVkIj7lj5blvpflhYPjga7mpa3nqK7jgpLkvb/jgaPjgZ/ni6zoh6rjga7liIbpoZ7jgafjgZnjgIJBSeODu+WbveetluOBquOBqeOBruODhuODvOODnuWIhumhnuOBqOOBr+eVsOOBquOCiuOBvuOBmeOAguOCuOODo+ODs+ODq+OBlOOBqOOBq+e2muOBjeOBi+OCieW3oeWbnuOBp+OBjeOBvuOBmeOAgjwvcD4KPGJ1dHRvbiBpZD0ibW92ZW1lbnRCdG4iIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hNb3ZlbWVudCgpIj7mrKHjga4yMOmKmOafhOOCkuiqv+OBueOCizwvYnV0dG9uPgo8YnV0dG9uIGlkPSJtb3ZlbWVudEFsbEJ0biIgb25jbGljaz0icmVmcmVzaE1vdmVtZW50KHRydWUpIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumBuOaKnuOCuOODo+ODs+ODq+OCkuWFqOmDqOiqv+OBueOCizwvYnV0dG9uPgo8cCBjbGFzcz0ibXV0ZWQiPjHlm57mirzjgZnjgajpgbjmip7jgrjjg6Pjg7Pjg6vjgpLlhYjpoK3jgYvjgonmnIDlvozjgb7jgafoh6rli5Xlt6Hlm57jgZfjgb7jgZnjgILnhKHmlpnmnqDjgavlkIjjgo/jgZvntIQxM+enkuS7peS4iuOBmuOBpOmWk+malOOCkuepuuOBkeOBvuOBmeOAgjEwMOmKmOafhOOBquOCieW+heOBoeaZgumWk+OBoOOBkeOBp+e0hDIy5YiG44GL44GL44KK44G+44GZ44CC5beh5Zue5Lit44Gv44GT44Gu44Oa44O844K444KS6ZaL44GE44Gf44G+44G+44Gr44GX44Gm44GP44Gg44GV44GE44CCPC9wPgo8YnV0dG9uIGlkPSJtb3ZlbWVudFN0b3AiIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InN0b3BNb3ZlbWVudCgpIiBkaXNhYmxlZCBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPuW3oeWbnuOCkuWBnOatojwvYnV0dG9uPgo8cCBpZD0ibW92ZW1lbnRDb3ZlcmFnZSIgY2xhc3M9Im11dGVkIj48L3A+CjxwIGlkPSJtb3ZlbWVudFN0YXR1cyIgY2xhc3M9Im11dGVkIiByb2xlPSJzdGF0dXMiIGFyaWEtbGl2ZT0icG9saXRlIj7mnKrmm7TmlrDjgILlj5blvpfjgZfjgZ/jg4fjg7zjgr/jga/jgZPjga7jg5bjg6njgqbjgrbjgavkv53lrZjjgZfjgb7jgZnjgII8L3A+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7wn4yfIOacrOaXpeOBruazqOebruWAmeijnO+8iOacgOaWsOWPluW+l+aXpeODmeODvOOCue+8iSA8c3BhbiBpZD0ibW92ZW1lbnRVcENvdW50Ij48L3NwYW4+PC9zdW1tYXJ5PjxkaXYgY2xhc3M9IndhdGNoLWNvbnRlbnQiIGlkPSJtb3ZlbWVudFVwIj48L2Rpdj48L2RldGFpbHM+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7imqDvuI8g5LiL6JC96K2m5oiS6YqY5p+EIDxzcGFuIGlkPSJtb3ZlbWVudERvd25Db3VudCI+PC9zcGFuPjwvc3VtbWFyeT48ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50IiBpZD0ibW92ZW1lbnREb3duIj48L2Rpdj48L2RldGFpbHM+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7jgZ3jga7ku5bjg7vliKTlrprkv53nlZkgPHNwYW4gaWQ9Im1vdmVtZW50T3RoZXJDb3VudCI+PC9zcGFuPjwvc3VtbWFyeT48ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50IiBpZD0ibW92ZW1lbnRPdGhlciI+PC9kaXY+PC9kZXRhaWxzPgo8cCBjbGFzcz0ibXV0ZWQiPuS7ruODq+ODvOODq++8muebtOi/kTHllrbmpa3ml6XvvIsxJeS7peS4iuOBi+OBpDXllrbmpa3ml6Xjg5fjg6njgrnjgpLms6jnm67lgJnoo5zjgIHiiJIxJeS7peS4i+OBi+OBpDXllrbmpa3ml6Xjgb7jgZ/jga8yMOWWtualreaXpeODnuOCpOODiuOCueOCkuS4i+iQveitpuaIkuOBqOOBl+OBvuOBmeOAguWHuuadpemrmOWil+WKoOOBr+ijnOWKqeihqOekuuOAguS4i+iQveS6iOa4rOODu+Wjsuiyt+aOqOWlqOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAguWIhuWJsuiqv+aVtOOBruOBquOBhOWxpeattOOBr+WApOWLleOBjeOBjOatquOCgOWgtOWQiOOBjOOBguOCiuOBvuOBmeOAgjwvcD4KPC9kaXY+CjxkaXYgY2xhc3M9ImNhcmQgcG9ydGZvbGlvIj4KICA8aDM+8J+nrSDjg53jg7zjg4jjg5Xjgqnjg6rjgqrlhajkvZM8L2gzPgogIDxkaXYgaWQ9InBvcnRmb2xpb1N1bW1hcnkiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkrwg5L+d5pyJ5qCq44O75pCN5YiH44KKL+WIqeeiujwvaDM+CiAgPGRpdiBjbGFzcz0ibm90ZSI+CiAgICDnj77lnKjlgKTjgajkvqHmoLzliIbmnpDlsaXmrbTjga9GaW5NaW5k5pel6Laz44KS5YSq5YWI44GX44Gm44CB5pCN55uK44O75pCN5YiH44KK6Led6Zui44O75Yip56K66Led6Zui44O744OI44Os44O844Oq44Oz44Kw44O755+t5Lit6ZW35a6f57i+44O744Od44O844OI44OV44Kp44Oq44Kq6KmV5L6h44KS6Ieq5YuV5YaN6KiI566X44GX44G+44GZ44CCRmluTWluZOWPluW+l+WkseaVl+aZguOBoOOBkeS7luOCveODvOOCueOBuOODleOCqeODvOODq+ODkOODg+OCr+OBl+OBvuOBmeOAggogIDwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Im1hcmdpbi10b3A6OXB4Ij4KICAgIDxidXR0b24gaWQ9InJlZnJlc2hBbGxCdG4iIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hBbGxIb2xkaW5ncyh0cnVlKSI+5L+d5pyJ5qCq44KS5pyA5paw57WC5YCk44Gn5LiA5ous5pu05pawPC9idXR0b24+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZGluZ1JlZnJlc2hTdGF0dXMiIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbjo3cHggMnB4IDAiPuS/neacieagquOBruiHquWLleabtOaWsOOBrzMw5YiG44GU44Go44Gr5pyA5aSnMeWbnuOBp+OBmeOAgjwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPgogICAgPGlucHV0IGlkPSJob2xkQ29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxpbnB1dCBpZD0iaG9sZENvc3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgcGxhY2Vob2xkZXI9IuWPluW+l+WNmOS+oSI+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NnB4IDJweCAwIj7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGlucHV0IGlkPSJob2xkU2hhcmVzIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLmoKrmlbAiPgogICAgPHNlbGVjdCBpZD0iZmVlTW9kZSI+CiAgICAgIDxvcHRpb24gdmFsdWU9Im5vbXVyYV9uZXQiPumHjuadkeOCquODs+ODqeOCpOODs+WwgueUqOaUr+W6l+ODu+ePvueJqTwvb3B0aW9uPgogICAgICA8b3B0aW9uIHZhbHVlPSJub25lIj7miYvmlbDmlpnjgarjgZfvvIjmr5TovIPnlKjvvIk8L29wdGlvbj4KICAgIDwvc2VsZWN0PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiiAlPC9zcGFuPjxpbnB1dCBpZD0ic3RvcFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iOCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K6ICU8L3NwYW4+PGlucHV0IGlkPSJ0YWtlUGN0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHZhbHVlPSIxNSI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844OrICU8L3NwYW4+PGlucHV0IGlkPSJ0cmFpbFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iNyI+PC9kaXY+CiAgPC9kaXY+CiAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRIb2xkaW5nKCkiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPuWun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmDwvYnV0dG9uPgogIDxwIGNsYXNzPSJtdXRlZCI+6YeO5p2R44ON44OD44OI77yG44Kz44O844Or77yP44G744Gj44Go44OA44Kk44Os44Kv44OI44Gu5Zu95YaF54++54mp44O744Kq44Oz44Op44Kk44Oz5rOo5paH44Gu56iO6L685omL5pWw5paZ6KGo44KS5L2/55So44CCPC9wPgogIDxkaXYgaWQ9ImhvbGRpbmdzIj48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+RgCDjgqbjgqnjg4Pjg4Hjg6rjgrnjg4g8L2gzPgogIDxidXR0b24gaWQ9IndhdGNoQnVsa0J0biIgY2xhc3M9InNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaEFsbFdhdGNoKCkiPuiyt+OBhOaZguOBruWPguiAg+OCkuS4gOaLrOabtOaWsDwvYnV0dG9uPgogIDxwIGlkPSJ3YXRjaEJ1bGtTdGF0dXMiIGNsYXNzPSJtdXRlZCIgcm9sZT0ic3RhdHVzIiBhcmlhLWxpdmU9InBvbGl0ZSI+5YWo6YqY5p+E44KS6aCG55Wq44Gr5YiG5p6Q44GX44G+44GZ44CC6YqY5p+E44GU44Go44Gr57SEMTPnp5Ljga7plpPpmpTjgpLnqbrjgZHjgb7jgZnjgII8L3A+CiAgPGRpdiBjbGFzcz0icm93Ij4KICAgIDxpbnB1dCBpZD0id2F0Y2hDb2RlIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxidXR0b24gb25jbGljaz0iYWRkV2F0Y2goKSI+6L+95YqgPC9idXR0b24+CiAgPC9kaXY+CiAgPGRpdiBpZD0id2F0Y2hDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjRweCAycHggOHB4Ij7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaHMiPjwvZGl2Pgo8L2Rpdj4KCjxzY3JpcHQ+CmNvbnN0ICQ9eD0+ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoeCk7CmZ1bmN0aW9uIHZhbChpZCl7bGV0IHY9JChpZCkudmFsdWUudHJpbSgpO3JldHVybiB2PT09Jyc/bnVsbDpOdW1iZXIodil9CmZ1bmN0aW9uIGxvY2FsKGspe3RyeXtyZXR1cm4gSlNPTi5wYXJzZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrKXx8J1tdJyl9Y2F0Y2goZSl7cmV0dXJuW119fQpmdW5jdGlvbiBzYXZlKGssdil7bG9jYWxTdG9yYWdlLnNldEl0ZW0oayxKU09OLnN0cmluZ2lmeSh2KSl9CmZ1bmN0aW9uIGZtdCh2LGQ9Mil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKGQpfQpmdW5jdGlvbiB5ZW4odil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOifCpScrTWF0aC5yb3VuZChOdW1iZXIodikpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpfQpmdW5jdGlvbiBzdGF0ZUphKHMpewogIGlmKHM9PT0nc3Ryb25nX2J1bGxpc2gnKXJldHVybiAn5by35rCXJzsKICBpZihzPT09J2J1bGxpc2gnKXJldHVybiAn44KE44KE5by35rCXJzsKICBpZihzPT09J25ldXRyYWwnKXJldHVybiAn5Lit56uLJzsKICBpZihzPT09J2JlYXJpc2gnKXJldHVybiAn44KE44KE5byx5rCXJzsKICBpZihzPT09J3N0cm9uZ19iZWFyaXNoJylyZXR1cm4gJ+W8seawlyc7CiAgcmV0dXJuICfliKTlrprkv53nlZknOwp9CgpmdW5jdGlvbiBzdGF0ZUNsYXNzKHMpewogIGlmKHM9PT0nc3Ryb25nX2J1bGxpc2gnKXJldHVybiAnc3RhdGUtc3Ryb25nLWJ1bGwnOwogIGlmKHM9PT0nYnVsbGlzaCcpcmV0dXJuICdzdGF0ZS1idWxsJzsKICBpZihzPT09J25ldXRyYWwnKXJldHVybiAnc3RhdGUtbmV1dHJhbCc7CiAgaWYocz09PSdiZWFyaXNoJylyZXR1cm4gJ3N0YXRlLWJlYXInOwogIGlmKHM9PT0nc3Ryb25nX2JlYXJpc2gnKXJldHVybiAnc3RhdGUtc3Ryb25nLWJlYXInOwogIHJldHVybiAnc3RhdGUtbmV1dHJhbCc7Cn0KCmZ1bmN0aW9uIGZhY3RvckphKGtleSl7CiAgaWYoa2V5PT09J3RlY2huaWNhbCcpcmV0dXJuICfjg4bjgq/jg4vjgqvjg6snOwogIGlmKGtleT09PSdlYXJuaW5ncycpcmV0dXJuICfmsbrnrpcnOwogIGlmKGtleT09PSdzdXBwbHknKXJldHVybiAn6ZyA57WmcHJveHknOwogIGlmKGtleT09PSdwb2xpY3knKXJldHVybiAn5Zu9562WJzsKICByZXR1cm4ga2V5fHwn6KaB5ZugJzsKfQoKZnVuY3Rpb24gZHJpdmVyU2VudGVuY2Uoc2MpewogIGNvbnN0IHA9c2MmJnNjLnN0cm9uZ2VzdF9wb3NpdGl2ZTsKICBjb25zdCBuPXNjJiZzYy5zdHJvbmdlc3RfbmVnYXRpdmU7CiAgY29uc3Qgc2NvcmU9TnVtYmVyKHNjJiZzYy5zY29yZTEwMCk7CgogIGxldCBoZWFkPScnOwogIGlmKE51bWJlci5pc0Zpbml0ZShzY29yZSkpewogICAgaWYoc2NvcmU+PTgwKWhlYWQ9J+WPluW+l+a4iOOBv+imgeWboOOCkue3j+WQiOOBmeOCi+OBqOOAgeW8t+OBhOODl+ODqeOCueipleS+oeOBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj02NSloZWFkPSfjg5fjg6njgrnopoHlm6DjgYzlhKrli6LjgafjgIHjgoTjgoTlvLfmsJfjga7oqZXkvqHjgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49NDUpaGVhZD0n44OX44Op44K544Go44Oe44Kk44OK44K544GM5ouu5oqX44GX44CB5Lit56uL5ZyP44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTMwKWhlYWQ9J+ODnuOCpOODiuOCueimgeWboOOBruW9semfv+OBjOOChOOChOW8t+OBj+OAgeaFjumHjeWvhOOCiuOBp+OBmeOAgic7CiAgICBlbHNlIGhlYWQ9J+ODnuOCpOODiuOCueimgeWboOOBruW9semfv+OBjOWkp+OBjeOBj+OAgeW8seawl+WvhOOCiuOBp+OBmeOAgic7CiAgfQoKICBsZXQgdGFpbD1bXTsKICBpZihuKXRhaWwucHVzaCgn5pyA5aSn44Gu5oq844GX5LiL44GS6KaB5Zug44GvICcrZmFjdG9ySmEobi5rZXkpKycgJytzY29yZUxhYmVsKG4uc2NvcmUpKTsKICBpZihwKXRhaWwucHVzaCgn5pyA5aSn44Gu5oq844GX5LiK44GS6KaB5Zug44GvICcrZmFjdG9ySmEocC5rZXkpKycgJytzY29yZUxhYmVsKHAuc2NvcmUpKTsKICByZXR1cm4gaGVhZCsodGFpbC5sZW5ndGg/JyAnK3RhaWwuam9pbign44CCJykrJ+OAgic6JycpOwp9CgpmdW5jdGlvbiBkcml2ZXJCb3hIdG1sKHRpdGxlLGQsa2luZCl7CiAgaWYoIWQpcmV0dXJuIGA8ZGl2IGNsYXNzPSJkcml2ZXJib3giPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+6Kmy5b2T44Gq44GXPC9iPjwvZGl2PmA7CiAgY29uc3Qgc2lnbj1OdW1iZXIoZC5jb250cmlidXRpb24pPj0wPycrJzonJzsKICByZXR1cm4gYDxkaXYgY2xhc3M9ImRyaXZlcmJveCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPgogICAgPGI+JHtmYWN0b3JKYShkLmtleSl9ICR7c2NvcmVMYWJlbChkLnNjb3JlKX08L2I+CiAgICA8c21hbGwgY2xhc3M9Im11dGVkIj7lho3phY3liIblvozjga7ph43jgb8gJHtkLndlaWdodF9wY3R9JSAvIOWvhOS4jiAke3NpZ259JHtOdW1iZXIoZC5jb250cmlidXRpb24pLnRvRml4ZWQoMSl9PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiBjb250cmlidXRpb25DYXJkSHRtbChkKXsKICBpZighZClyZXR1cm4gJyc7CiAgY29uc3QgYz1OdW1iZXIoZC5jb250cmlidXRpb24pOwogIGNvbnN0IHNpZ249Yz4wPycrJzonJzsKICBjb25zdCBpbXBhY3Q9Yy8yOwogIGNvbnN0IGltcGFjdFNpZ249aW1wYWN0PjA/JysnOicnOwogIHJldHVybiBgPGRpdiBjbGFzcz0iY29udHJpYiI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7ZmFjdG9ySmEoZC5rZXkpfTwvc3Bhbj4KICAgIDxiPiR7c2lnbn0ke2MudG9GaXhlZCgxKX08L2I+CiAgICA8c21hbGw+5Z+65rqW6YeN44G/ICR7TnVtYmVyKGQuYmFzZV93ZWlnaHRfcGN0Pz8wKS50b0ZpeGVkKDEpfSUgw5cg6a6u5bqmICR7TnVtYmVyKGQuZnJlc2huZXNzX3BjdD8/MTAwKS50b0ZpeGVkKDApfSU8L3NtYWxsPgogICAgPHNtYWxsPuWGjemFjeWIhuW+jOmHjeOBvyAke051bWJlcihkLndlaWdodF9wY3QpLnRvRml4ZWQoMSl9JTwvc21hbGw+CiAgICA8c21hbGw+MTAw54K55o+b566X44G444Gu5b2x6Z+/ICR7aW1wYWN0U2lnbn0ke2ltcGFjdC50b0ZpeGVkKDEpfeeCuTwvc21hbGw+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gcmVuZGVyQ29udHJpYnV0aW9ucyhzYyl7CiAgY29uc3QgYm94PSQoJ2NvbnRyaWJ1dGlvbkJveCcpOwogIGNvbnN0IGdyaWQ9JCgnY29udHJpYnV0aW9uR3JpZCcpOwogIGlmKCFib3h8fCFncmlkKXJldHVybjsKICBjb25zdCBkcz0oc2MmJnNjLmRyaXZlcnMpfHxbXTsKICBpZighZHMubGVuZ3RoKXsKICAgIGJveC5zdHlsZS5kaXNwbGF5PSdub25lJzsKICAgIGdyaWQuaW5uZXJIVE1MPScnOwogICAgcmV0dXJuOwogIH0KICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogIGdyaWQuaW5uZXJIVE1MPWRzLm1hcChjb250cmlidXRpb25DYXJkSHRtbCkuam9pbignJyk7Cn0KZnVuY3Rpb24gcGN0KHYpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgyKSsnJSd9CmZ1bmN0aW9uIHN0YXRGb3IoaCxrZXkpe3JldHVybiBoJiZoLmZvcndhcmRfc3RhdHMmJmguZm9yd2FyZF9zdGF0c1trZXldP2guZm9yd2FyZF9zdGF0c1trZXldOm51bGx9CgpmdW5jdGlvbiBkYXRhQWdlRGF5cyhkYXRlU3RyKXsKICBpZighZGF0ZVN0cilyZXR1cm4gbnVsbDsKICBjb25zdCBtPVN0cmluZyhkYXRlU3RyKS5tYXRjaCgvXihcZHs0fSktKFxkezJ9KS0oXGR7Mn0pJC8pOwogIGlmKCFtKXJldHVybiBudWxsOwogIGNvbnN0IGQ9RGF0ZS5VVEMoTnVtYmVyKG1bMV0pLE51bWJlcihtWzJdKS0xLE51bWJlcihtWzNdKSk7CiAgY29uc3Qgbm93PW5ldyBEYXRlKCk7CiAgY29uc3QgdG9kYXk9RGF0ZS5VVEMobm93LmdldEZ1bGxZZWFyKCksbm93LmdldE1vbnRoKCksbm93LmdldERhdGUoKSk7CiAgcmV0dXJuIE1hdGgubWF4KDAsTWF0aC5mbG9vcigodG9kYXktZCkvODY0MDAwMDApKTsKfQoKZnVuY3Rpb24gZnJlc2huZXNzRm9yKGRhdGVTdHIpewogIGNvbnN0IGRheXM9ZGF0YUFnZURheXMoZGF0ZVN0cik7CiAgaWYoZGF5cz09PW51bGwpewogICAgcmV0dXJuIHtsZXZlbDondW5rbm93bicsZGF5czpudWxsLGxhYmVsOifml6Xku5jkuI3mmI4nLGNsczonZnJlc2gtd2FybicsZGVjaXNpb25fb2s6ZmFsc2V9OwogIH0KICBpZihkYXlzPD00KXsKICAgIHJldHVybiB7bGV2ZWw6J2ZyZXNoJyxkYXlzLGxhYmVsOifprq7luqZPSycsY2xzOidmcmVzaC1vaycsZGVjaXNpb25fb2s6dHJ1ZX07CiAgfQogIGlmKGRheXM8PTEwKXsKICAgIHJldHVybiB7bGV2ZWw6J3dhcm5pbmcnLGRheXMsbGFiZWw6J+OChOOChOmBheW7ticsY2xzOidmcmVzaC13YXJuJyxkZWNpc2lvbl9vazp0cnVlfTsKICB9CiAgcmV0dXJuIHtsZXZlbDonc3RhbGUnLGRheXMsbGFiZWw6J+WPpOOBhOODh+ODvOOCvycsY2xzOidmcmVzaC1zdGFsZScsZGVjaXNpb25fb2s6ZmFsc2V9Owp9CgpmdW5jdGlvbiBmcmVzaG5lc3NUZXh0KGRhdGVTdHIpewogIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGRhdGVTdHIpOwogIGlmKGYuZGF5cz09PW51bGwpcmV0dXJuICfmnIDntYLjg4fjg7zjgr/ml6XjgpLnorroqo3jgafjgY3jgb7jgZvjgpPjgIInOwogIGlmKGYubGV2ZWw9PT0nZnJlc2gnKXJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpeOAgumAmuW4uOOBruWPguiAg+WIpOWumuOBq+S9v+eUqOOBl+OBvuOBmeOAgmA7CiAgaWYoZi5sZXZlbD09PSd3YXJuaW5nJylyZXR1cm4gYOacgOe1guODh+ODvOOCv+aXpeOBi+OCiSAke2YuZGF5c33ml6XjgILpgYXlu7bjgavms6jmhI/jgZfjgabjgIHlrp/pmpvjga7nj77lnKjlgKTjgoLnorroqo3jgZfjgabjgY/jgaDjgZXjgYTjgIJgOwogIHJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpemBheOCjOOAguS7iuaXpeOBruWjsuiyt+WIpOaWreOBq+OBr+WPpOOBhOOBn+OCgeOAgeS/neacieWIpOaWreOBr+iHquWLleOBp+S/neeVmeOBl+OBvuOBmeOAgmA7Cn0KCmZ1bmN0aW9uIHNob3dGcmVzaG5lc3MoZGF0ZVN0cil7CiAgY29uc3QgYm94PSQoJ2ZyZXNobmVzc0JveCcpOwogIGlmKCFib3gpcmV0dXJuOwogIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGRhdGVTdHIpOwogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgYm94LmNsYXNzTmFtZT0nZnJlc2hib3ggJytmLmNsczsKICAkKCdmcmVzaG5lc3NUaXRsZScpLnRleHRDb250ZW50PQogICAgZi5sZXZlbD09PSdmcmVzaCc/J+KchSDjg4fjg7zjgr/prq7luqZPSyc6CiAgICBmLmxldmVsPT09J3dhcm5pbmcnPyfimqDvuI8g44OH44O844K/6YGF5bu244Gr5rOo5oSPJzoKICAgIGYubGV2ZWw9PT0nc3RhbGUnPyfwn5uRIOODh+ODvOOCv+OBjOWPpOOBhOOBn+OCgeS7iuaXpeOBruWIpOaWreOBr+S/neeVmSc6CiAgICAn4pqg77iPIOODh+ODvOOCv+muruW6puOCkueiuuiqjeOBp+OBjeOBvuOBm+OCkyc7CiAgJCgnZnJlc2huZXNzRGV0YWlsJykudGV4dENvbnRlbnQ9ZnJlc2huZXNzVGV4dChkYXRlU3RyKTsKfQoKZnVuY3Rpb24gc2hvd0hpc3RvcnlGcmVzaG5lc3MoaGlzdG9yeURhdGUscHJpY2VEYXRlKXsKICBjb25zdCBib3g9JCgnaGlzdG9yeUZyZXNobmVzc0JveCcpOwogIGlmKCFib3gpcmV0dXJuOwogIGlmKCFoaXN0b3J5RGF0ZSl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnaGlzdG9yeUZyZXNobmVzc1RleHQnKS50ZXh0Q29udGVudD0nMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7liIbmnpDlsaXmrbTjgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ/jgILnj77lnKjlgKTjgaDjgZHooajnpLrjgZfjgb7jgZnjgIInOwogICAgcmV0dXJuOwogIH0KICBjb25zdCBoZj1mcmVzaG5lc3NGb3IoaGlzdG9yeURhdGUpOwogIGlmKGhmLmxldmVsPT09J2ZyZXNoJyl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgYm94LmNsYXNzTmFtZT0nZnJlc2hib3ggZnJlc2gtb2snOwogICAgJCgnaGlzdG9yeUZyZXNobmVzc1RleHQnKS50ZXh0Q29udGVudD0KICAgICAgYOS+oeagvOWxpeattOOCguacgOaWsOWWtualreaXpSAke2hpc3RvcnlEYXRlfSDjgb7jgaflj5blvpfjgILnn63kuK3plbfjg7vpnIDntaZwcm94eeODu+ODiOODrOODvOODquODs+OCsOOCkumAmuW4uOioiOeul+OBl+OBvuOBmeOAgmA7CiAgICByZXR1cm47CiAgfQogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgY29uc3QgcGQ9cHJpY2VEYXRlfHwn4oCUJzsKICAkKCdoaXN0b3J5RnJlc2huZXNzVGV4dCcpLnRleHRDb250ZW50PQogICAgYOePvuWcqOWApOODh+ODvOOCv+aXpSAke3BkfSAvIOS+oeagvOWxpeattOacgOe1guaXpSAke2hpc3RvcnlEYXRlfeOAguS+oeagvOWxpeattOOBjOWPpOOBhOWgtOWQiOOBoOOBkeOAgeODiOODrOODs+ODieODu+acn+W+heWApOODu+mcgOe1pnByb3h544KS5Y+C6ICD5YCk5omx44GE44Gr44GX44G+44GZ44CCYDsKfQoKZnVuY3Rpb24gY29uZmlkZW5jZUZvcihoKXsKICBjb25zdCB2YWxzPVtoLnJldHVybl8yMGQsaC5yZXR1cm5fMTI2ZCxoLnJldHVybl8yNTJkXS5tYXAoTnVtYmVyKS5maWx0ZXIoTnVtYmVyLmlzRmluaXRlKTsKICBjb25zdCBzdGF0cz1bJzIwZCcsJzEyNmQnLCcyNTJkJ10ubWFwKGs9PnN0YXRGb3IoaCxrKSkuZmlsdGVyKHM9PnMmJnMuc3RhdHVzPT09J29rJyk7CgogIGlmKCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpfHwhaC5hc29mKXJldHVybiAwOwoKICBjb25zdCBjb3ZlcmFnZT12YWxzLmxlbmd0aC8zOwogIGxldCBhZ3JlZW1lbnQ9MC41OwogIGlmKHZhbHMubGVuZ3RoKXsKICAgIGNvbnN0IHBvcz12YWxzLmZpbHRlcih4PT54PjApLmxlbmd0aDsKICAgIGNvbnN0IG5lZz12YWxzLmZpbHRlcih4PT54PDApLmxlbmd0aDsKICAgIGFncmVlbWVudD1NYXRoLm1heChwb3MsbmVnKS92YWxzLmxlbmd0aDsKICB9CiAgY29uc3Qgc3RhdENvdmVyYWdlPXN0YXRzLmxlbmd0aC8zOwogIGxldCBzY29yZT0oY292ZXJhZ2UqMC40NSArIGFncmVlbWVudCowLjI1ICsgc3RhdENvdmVyYWdlKjAuMzApKjEwMDsKCiAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKGguYXNvZik7CiAgaWYoZnJlc2gubGV2ZWw9PT0nd2FybmluZycpc2NvcmUqPTAuNjA7CiAgaWYoZnJlc2gubGV2ZWw9PT0nc3RhbGUnKXNjb3JlPU1hdGgubWluKHNjb3JlLDI1KTsKICBpZihmcmVzaC5sZXZlbD09PSd1bmtub3duJylzY29yZT1NYXRoLm1pbihzY29yZSwyMCk7CgogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgaWYoaGlzdG9yeUZyZXNoLmxldmVsPT09J3dhcm5pbmcnKXNjb3JlKj0wLjc1OwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSdzdGFsZScpc2NvcmU9TWF0aC5taW4oc2NvcmUsMzUpOwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSd1bmtub3duJylzY29yZT1NYXRoLm1pbihzY29yZSwyNSk7CgogIHJldHVybiBNYXRoLnJvdW5kKHNjb3JlKTsKfQoKZnVuY3Rpb24gZGVjaXNpb25Gb3IoaCxjKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFoLmFzb2YpewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVmScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjon5a6f44OH44O844K/5pyq5Y+W5b6X44CC5Y+z5LiK44Gu44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgY/jgaDjgZXjgYTjgIInLAogICAgICBjb25maWRlbmNlOjAKICAgIH07CiAgfQoKICBjb25zdCByMjA9TnVtYmVyKGgucmV0dXJuXzIwZCksIHIxMjY9TnVtYmVyKGgucmV0dXJuXzEyNmQpLCByMjUyPU51bWJlcihoLnJldHVybl8yNTJkKTsKICBjb25zdCBjb25maWRlbmNlPWNvbmZpZGVuY2VGb3IoaCk7CiAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKGguYXNvZik7CgogIGlmKCFmcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOmAke2ZyZXNobmVzc1RleHQoaC5hc29mKX0g5pCN5YiH44KK44O75Yip56K644Op44Kk44Oz44Go44Gu5q+U6LyD44KC5Y+C6ICD5YCk5omx44GE44Gn44GZ44CCYCwKICAgICAgY29uZmlkZW5jZQogICAgfTsKICB9CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5pCN5YiH44KK5qSc6KiOJyxjbHM6J2Qtc3RvcCcscmVhc29uOifoqK3lrprjgZfjgZ/mkI3liIfjgorlj4LogIPjg6njgqTjg7Pku6XkuIsnLGNvbmZpZGVuY2V9OwogIH0KICBpZihjdXI+PWMudGFrZVByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+WIqeeiuuaknOiojicsY2xzOidkLXRha2UnLHJlYXNvbjon6Kit5a6a44GX44Gf5Yip56K65Y+C6ICD44Op44Kk44Oz5Lul5LiKJyxjb25maWRlbmNlfTsKICB9CgogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgaWYoIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZ77yI5bGl5q205Y+k44GE77yJJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOmDnj77lnKjlgKTjga/lj5blvpfjgafjgY3jgabjgYTjgb7jgZnjgYzjgIHkvqHmoLzlsaXmrbTjga8gJHtoLmhpc3RvcnlfYXNvZnx8J+S4jeaYjid944CC5Zu65a6a44Gu5pCN5YiH44KKL+WIqeeiuuODqeOCpOODs+OBq+OBr+acquWIsOmBlOOBp+OBmeOBjOOAgeODiOODrOODs+ODieWIpOaWreOBr+S/neeVmeOBl+OBvuOBmeOAgmAsCiAgICAgIGNvbmZpZGVuY2UKICAgIH07CiAgfQoKICBpZihjdXI8PWMudHJhaWxQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpemrmOWApOWfuua6luOBruODiOODrOODvOODquODs+OCsOWPguiAg+ODqeOCpOODs+S7peS4iycsY29uZmlkZW5jZX07CiAgfQoKICBsZXQgcG9zaXRpdmU9MCwgbmVnYXRpdmU9MDsKICBbcjIwLHIxMjYscjI1Ml0uZm9yRWFjaCh4PT57CiAgICBpZihOdW1iZXIuaXNGaW5pdGUoeCkpewogICAgICBpZih4PjApcG9zaXRpdmUrKzsKICAgICAgaWYoeDwwKW5lZ2F0aXZlKys7CiAgICB9CiAgfSk7CgogIGlmKG5lZ2F0aXZlPj0yKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel44O7MTI25pel44O7MjUy5pel44Gu44GG44Gh44Oe44Kk44OK44K55YK+5ZCR44GM5YSq5YuiJyxjb25maWRlbmNlfTsKICB9CiAgaWYocG9zaXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon5L+d5pyJ57aZ57aaJyxjbHM6J2QtaG9sZCcscmVhc29uOifoqK3lrprjg6njgqTjg7PlhoXjgafjgIHopIfmlbDmnJ/plpPjga7kvqHmoLzjg4jjg6zjg7Pjg4njgYzjg5fjg6njgrknLGNvbmZpZGVuY2V9OwogIH0KICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntprvvIjmp5jlrZDopovvvIknLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOAguacn+mWk+WIpeODiOODrOODs+ODieOBr+W8t+W8seOBjOa3t+WcqCcsY29uZmlkZW5jZX07Cn0KZnVuY3Rpb24gZXZIdG1sKHRpdGxlLHMpewogIGlmKCFzfHxzLnN0YXR1cyE9PSdvaycpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c21hbGw+5qiZ5pysICR7bn3ku7Y8L3NtYWxsPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj4KICAgIDxiPuW5s+WdhyAke3BjdChzLm1lYW4pfTwvYj4KICAgIDxzbWFsbD7kuK3lpK7lgKQgJHtwY3Qocy5tZWRpYW4pfTwvc21hbGw+CiAgICA8c21hbGw+5LiK5piH546HICR7cGN0KHMucG9zaXRpdmVfcmF0ZSl9PC9zbWFsbD4KICAgIDxzbWFsbD5QMTDjgJxQOTAgJHtwY3Qocy5wMTApfSDjgJwgJHtwY3Qocy5wOTApfTwvc21hbGw+CiAgICA8c21hbGw+5qiZ5pysICR7cy5ufeS7tjwvc21hbGw+CiAgPC9kaXY+YDsKfQoKCmZ1bmN0aW9uIGRpc3RhbmNlSW5mbyhjdXIsdGFyZ2V0LGtpbmQpewogIGN1cj1OdW1iZXIoY3VyKTsgdGFyZ2V0PU51bWJlcih0YXJnZXQpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhTnVtYmVyLmlzRmluaXRlKHRhcmdldCkpcmV0dXJuICfigJQnOwogIGNvbnN0IGRpZmY9KHRhcmdldC9jdXItMSkqMTAwOwogIGlmKGtpbmQ9PT0nc3RvcCcpewogICAgaWYoZGlmZj49MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gTWF0aC5hYnMoZGlmZikudG9GaXhlZCgyKSsnJSDkuIsnOwogIH0KICBpZihraW5kPT09J3Rha2UnKXsKICAgIGlmKGRpZmY8PTApcmV0dXJuICfjg6njgqTjg7PliLDpgZTmuIjjgb8nOwogICAgcmV0dXJuIGRpZmYudG9GaXhlZCgyKSsnJSDkuIonOwogIH0KICByZXR1cm4gKGRpZmY+PTA/JysnOicnKStkaWZmLnRvRml4ZWQoMikrJyUnOwp9CgpmdW5jdGlvbiBwcmljZVJhbmdlKGN1cixzKXsKICBjdXI9TnVtYmVyKGN1cik7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFzfHxzLnN0YXR1cyE9PSdvaycpcmV0dXJuIG51bGw7CiAgcmV0dXJuIHsKICAgIGxvdzpjdXIqKDErTnVtYmVyKHMucDEwKS8xMDApLAogICAgaGlnaDpjdXIqKDErTnVtYmVyKHMucDkwKS8xMDApLAogICAgbWVkaWFuOmN1ciooMStOdW1iZXIocy5tZWRpYW4pLzEwMCkKICB9Owp9CgpmdW5jdGlvbiByYW5nZUh0bWwodGl0bGUsY3VyLHMpewogIGNvbnN0IHI9cHJpY2VSYW5nZShjdXIscyk7CiAgaWYoIXIpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9InJhbmdlYm94Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaomeacrCAke2595Lu2PC9zcGFuPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfSBQMTDjgJxQOTA8L3NwYW4+CiAgICA8Yj4ke3llbihyLmxvdyl9IOOAnCAke3llbihyLmhpZ2gpfTwvYj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Lit5aSu5YCk5o+b566XICR7eWVuKHIubWVkaWFuKX08L3NwYW4+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gYWN0aW9uVGV4dChoLGMsZCl7CiAgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZknfHxkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOagquS+oeWPpOOBhO+8iScpcmV0dXJuICfjg4fjg7zjgr/jgYzlj6TjgYTjgZ/jgoHku4rml6Xjga7liKTmlq3jga/kv53nlZnjgILoqLzliLjkvJrnpL7jgarjganjgaflrp/pmpvjga7nj77lnKjlgKTjgpLnorroqo3jgZfjgabjgYvjgonliKTmlq3jgIInOwogIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5bGl5q205Y+k44GE77yJJylyZXR1cm4gJ+ePvuWcqOWApOOBr+eiuuiqjea4iOOBv+OAguWbuuWumuOBruaQjeWIh+OCii/liKnnorrjg6njgqTjg7PjgaDjgZHnorroqo3jgZfjgIHjg4jjg6zjg7Pjg4nliKTmlq3jga/kvqHmoLzlsaXmrbTmm7TmlrDjgb7jgafkv53nlZnjgIInOwogIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylyZXR1cm4gJ+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+OCkuS4i+WbnuOBo+OBpuOBhOOBvuOBmeOAguWun+mam+OBruePvuWcqOWApOOBqOazqOaWh+adoeS7tuOCkueiuuiqjeOBl+OBpuOAgee4ruWwj+ODu+aSpOmAgOOCkuaknOiojuOAgic7CiAgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKXJldHVybiAn5Yip56K65Y+C6ICD44Op44Kk44Oz44Gr5Yiw6YGU44GX44Gm44GE44G+44GZ44CC5YWo6YOo5aOy5Y2044Gg44GR44Gn44Gq44GP44CB5YiG5Ymy5Yip56K644KC5YCZ6KOc44CCJzsKICBpZihkLmxhYmVsPT09J+itpuaIkicpcmV0dXJuICforabmiJLjgr7jg7zjg7PjgILjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PjgajkuK3nn63mnJ/jga7lgKTli5XjgY3jgpLlhKrlhYjjgZfjgabnorroqo3jgIInOwogIHJldHVybiAn6Kit5a6a44Op44Kk44Oz5YaF44CC5L+d5pyJ57aZ57aa5YCZ6KOc44Gn44GZ44GM44CB54Sh5paZ44OH44O844K/44Gv6YGF5bu244GZ44KL44Gf44KB5a6f6Zqb44Gu54++5Zyo5YCk44KC56K66KqN44CCJzsKfQoKCmZ1bmN0aW9uIG5vbXVyYU5ldEZlZShhbW91bnQpewogIGFtb3VudD1OdW1iZXIoYW1vdW50fHwwKTsKICBpZihhbW91bnQ8PTApcmV0dXJuIDA7CiAgaWYoYW1vdW50PD0xMDAwMDApcmV0dXJuIDE1MjsKICBpZihhbW91bnQ8PTMwMDAwMClyZXR1cm4gMzMwOwogIGlmKGFtb3VudDw9NTAwMDAwKXJldHVybiA1MjQ7CiAgaWYoYW1vdW50PD0xMDAwMDAwKXJldHVybiAxMDQ4OwogIGlmKGFtb3VudDw9MjAwMDAwMClyZXR1cm4gMjA5NTsKICBpZihhbW91bnQ8PTMwMDAwMDApcmV0dXJuIDMxNDM7CiAgaWYoYW1vdW50PD01MDAwMDAwKXJldHVybiA1MjM4OwogIGlmKGFtb3VudDw9MTAwMDAwMDApcmV0dXJuIDEwNDc2OwogIGlmKGFtb3VudDw9MjAwMDAwMDApcmV0dXJuIDIwOTUyOwogIGlmKGFtb3VudDw9MzAwMDAwMDApcmV0dXJuIDMxNDI5OwogIGlmKGFtb3VudDw9NTAwMDAwMDApcmV0dXJuIDQxOTA1OwogIHJldHVybiA3ODU3MTsKfQpmdW5jdGlvbiBmZWVGb3IoYW1vdW50LG1vZGUpe3JldHVybiBtb2RlPT09J25vbXVyYV9uZXQnP25vbXVyYU5ldEZlZShhbW91bnQpOjB9CgpmdW5jdGlvbiBjYWxjSG9sZGluZyhoKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgY29zdD1OdW1iZXIoaC5jb3N0KTsKICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzKTsKICBjb25zdCBidXlWYWx1ZT1jb3N0KnNoYXJlczsKICBjb25zdCBidXlGZWU9ZmVlRm9yKGJ1eVZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGN1cnJlbnRWYWx1ZT1jdXIqc2hhcmVzOwogIGNvbnN0IHNlbGxGZWU9ZmVlRm9yKGN1cnJlbnRWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBpbnZlc3RlZD1idXlWYWx1ZStidXlGZWU7CiAgY29uc3QgbmV0Tm93PWN1cnJlbnRWYWx1ZS1zZWxsRmVlLWludmVzdGVkOwogIGNvbnN0IG5ldE5vd1BjdD1pbnZlc3RlZD9uZXROb3cvaW52ZXN0ZWQqMTAwOm51bGw7CgogIGNvbnN0IHN0b3BQcmljZT1jb3N0KigxLU51bWJlcihoLnN0b3BfcGN0KS8xMDApOwogIGNvbnN0IHRha2VQcmljZT1jb3N0KigxK051bWJlcihoLnRha2VfcGN0KS8xMDApOwogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgY29uc3QgaGlnaDIwPU51bWJlcihoLmhpZ2hfMjBkfHxjdXIpOwogIGNvbnN0IHRyYWlsUHJpY2U9aGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rCiAgICA/IGhpZ2gyMCooMS1OdW1iZXIoaC50cmFpbF9wY3QpLzEwMCkKICAgIDogbnVsbDsKCiAgY29uc3Qgc3RvcFZhbHVlPXN0b3BQcmljZSpzaGFyZXM7CiAgY29uc3QgdGFrZVZhbHVlPXRha2VQcmljZSpzaGFyZXM7CiAgY29uc3Qgc3RvcE5ldD1zdG9wVmFsdWUtZmVlRm9yKHN0b3BWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKICBjb25zdCB0YWtlTmV0PXRha2VWYWx1ZS1mZWVGb3IodGFrZVZhbHVlLGguZmVlX21vZGUpLWludmVzdGVkOwoKICByZXR1cm4ge2J1eVZhbHVlLGJ1eUZlZSxjdXJyZW50VmFsdWUsc2VsbEZlZSxpbnZlc3RlZCxuZXROb3csbmV0Tm93UGN0LHN0b3BQcmljZSx0YWtlUHJpY2UsdHJhaWxQcmljZSxzdG9wTmV0LHRha2VOZXR9Owp9CgoKY29uc3QgbmFtZVRpbWVycz17fTsKY29uc3QgbmFtZUNhY2hlPXt9OwoKZnVuY3Rpb24gZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4PScnKXsKICBpZighdGFyZ2V0KXJldHVybjsKICBpZighaW5mb3x8IWluZm8ubmFtZSl7CiAgICB0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nOwogICAgcmV0dXJuOwogIH0KICBsZXQgZXh0cmFzPVtdOwogIGlmKGluZm8ubWFya2V0KWV4dHJhcy5wdXNoKGluZm8ubWFya2V0KTsKICBpZihpbmZvLnNlY3RvcjMzKWV4dHJhcy5wdXNoKGluZm8uc2VjdG9yMzMpOwogIHRhcmdldC5pbm5lckhUTUw9JzxiPicrcHJlZml4K2luZm8ubmFtZSsnPC9iPicrKGV4dHJhcy5sZW5ndGg/Jzxicj48c3BhbiBjbGFzcz0ibXV0ZWQiPicrZXh0cmFzLmpvaW4oJyAvICcpKyc8L3NwYW4+JzonJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldENvbXBhbnkoY29kZSl7CiAgY29uc3QgYz1TdHJpbmcoY29kZXx8JycpLnRyaW0oKTsKICBpZighYylyZXR1cm4gbnVsbDsKICBpZihuYW1lQ2FjaGVbY10pcmV0dXJuIG5hbWVDYWNoZVtjXTsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9zZWN1cml0eT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn6YqY5p+E5ZCN44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgbmFtZUNhY2hlW2NdPXg7CiAgcmV0dXJuIHg7Cn0KCmZ1bmN0aW9uIHNjaGVkdWxlQ29tcGFueUxvb2t1cChpbnB1dElkLHRhcmdldElkLHByZWZpeD0nJyl7CiAgY2xlYXJUaW1lb3V0KG5hbWVUaW1lcnNbaW5wdXRJZF0pOwogIGNvbnN0IGM9JChpbnB1dElkKS52YWx1ZS50cmltKCk7CiAgY29uc3QgdGFyZ2V0PSQodGFyZ2V0SWQpOwoKICBpZihjLmxlbmd0aDw0KXsKICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrigJQnOwogICAgcmV0dXJuOwogIH0KCiAgbmFtZVRpbWVyc1tpbnB1dElkXT1zZXRUaW1lb3V0KGFzeW5jKCk9PnsKICAgIHRyeXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD0n6YqY5p+E5ZCN44KS56K66KqN5Lit4oCmJzsKICAgICAgY29uc3QgaW5mbz1hd2FpdCBnZXRDb21wYW55KGMpOwogICAgICBkaXNwbGF5Q29tcGFueSh0YXJnZXQsaW5mbyxwcmVmaXgpOwogICAgfWNhdGNoKGUpewogICAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIH0KICB9LDQ1MCk7Cn0KCgoKZnVuY3Rpb24gcHJpb3JpdHlBY3Rpb25Gb3IoaCl7CiAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgY29uc3QgbmFtZT1oLmNvbXBhbnlfbmFtZXx8Jyc7CiAgY29uc3QgbGFiZWw9KGguY29kZXx8JycpKyhuYW1lPycgJytuYW1lOicnKTsKICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CgogIGlmKCF2YWxpZCl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NiwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+Wun+ODh+ODvOOCv+OCkuabtOaWsCcsCiAgICAgIGRldGFpbDon5pyA5paw5Y+W5b6X57WC5YCk44GM44GC44KK44G+44Gb44KT44CC44G+44Ga44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgIInLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogIGlmKCFmcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5OSwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+ODh+ODvOOCv+muruW6puOCkueiuuiqjScsCiAgICAgIGRldGFpbDpgJHtmcmVzaG5lc3NUZXh0KGguYXNvZil9IOWun+mam+OBruePvuWcqOWApOOCkuWFiOOBq+eiuuiqjeOAgmAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3Qgc3RvcERpc3Q9KGN1ci9jLnN0b3BQcmljZS0xKSoxMDA7CiAgY29uc3QgdGFrZURpc3Q9KGMudGFrZVByaWNlL2N1ci0xKSoxMDA7CiAgY29uc3QgdHJhaWxEaXN0PShjdXIvYy50cmFpbFByaWNlLTEpKjEwMDsKCiAgaWYoY3VyPD1jLnN0b3BQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZToxMDAsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOaQjeWIh+OCiuWPguiAgyAke3llbihjLnN0b3BQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihzdG9wRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NCwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+aQjeWIh+OCiuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7c3RvcERpc3QudG9GaXhlZCgyKX0lIOOBp+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5MCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+WIsOmBlCcsCiAgICAgIGRldGFpbDpg5pyA5paw5Y+W5b6X57WC5YCkICR7eWVuKGN1cil9IC8g5Yip56K65Y+C6ICDICR7eWVuKGMudGFrZVByaWNlKX1gLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKHRha2VEaXN0PD0zKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjg0LAogICAgICBjbHM6J3ByaW9yaXR5LXRha2UnLAogICAgICB0aXRsZTon5Yip56K644Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjgYLjgaggJHt0YWtlRGlzdC50b0ZpeGVkKDIpfSUg44Gn5Yip56K65Y+C6ICD44Op44Kk44OzYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGlmKCFoaXN0b3J5RnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6NzgsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+S+oeagvOWxpeattOOCkuabtOaWsOW+heOBoScsCiAgICAgIGRldGFpbDpg54++5Zyo5YCkICR7aC5hc29mfHwn4oCUJ30gLyDkvqHmoLzlsaXmrbQgJHtoLmhpc3RvcnlfYXNvZnx8J+KAlCd944CC5Zu65a6a5L6h5qC844Op44Kk44Oz5Lul5aSW44Gu5Yik5pat44Gv5L+d55WZ44CCYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihkLmxhYmVsPT09J+itpuaIkicpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6ODAsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+itpuaIkuWIpOWumicsCiAgICAgIGRldGFpbDpkLnJlYXNvbiwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihOdW1iZXIuaXNGaW5pdGUodHJhaWxEaXN0KSYmdHJhaWxEaXN0PD0yLjUpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6NzYsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+ODiOODrOODvOODquODs+OCsOODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44OI44Os44O844Oq44Oz44Kw5Y+C6ICDICR7eWVuKGMudHJhaWxQcmljZSl9IOOBvuOBpyAke3RyYWlsRGlzdC50b0ZpeGVkKDIpfSVgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIHJldHVybiB7CiAgICBzY29yZTozMCwKICAgIGNsczoncHJpb3JpdHktZ29vZCcsCiAgICB0aXRsZTon6YCa5bi455uj6KaWJywKICAgIGRldGFpbDpkLnJlYXNvbnx8J+ioreWumuODqeOCpOODs+WGhScsCiAgICBsYWJlbAogIH07Cn0KCmZ1bmN0aW9uIHJlbmRlclByaW9yaXR5QWN0aW9ucygpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgYm94PSQoJ3ByaW9yaXR5QWN0aW9ucycpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJldHVybjsKICB9CgogIGxldCBhY3Rpb25zPWEubWFwKHByaW9yaXR5QWN0aW9uRm9yKTsKCiAgLy8gQ29uY2VudHJhdGlvbiBhbGVydCAocG9ydGZvbGlvLWxldmVsKQogIGNvbnN0IHZhbGlkPWEuZmlsdGVyKGg9Pk51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZik7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw+MCl7CiAgICBsZXQgbWF4SG9sZGluZz1udWxsLCBtYXhWYWx1ZT0wOwogICAgdmFsaWQuZm9yRWFjaChoPT57CiAgICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgICAgaWYodj5tYXhWYWx1ZSl7bWF4VmFsdWU9djttYXhIb2xkaW5nPWh9CiAgICB9KTsKICAgIGNvbnN0IGNvbmNlbnRyYXRpb249bWF4VmFsdWUvdG90YWwqMTAwOwogICAgaWYoY29uY2VudHJhdGlvbj49NjAgJiYgbWF4SG9sZGluZyl7CiAgICAgIGFjdGlvbnMucHVzaCh7CiAgICAgICAgc2NvcmU6NzIsCiAgICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICAgIHRpdGxlOifpm4bkuK3luqbjgpLnorroqo0nLAogICAgICAgIGRldGFpbDpgJHttYXhIb2xkaW5nLmNvZGV9JHttYXhIb2xkaW5nLmNvbXBhbnlfbmFtZT8nICcrbWF4SG9sZGluZy5jb21wYW55X25hbWU6Jyd9IOOBjOODneODvOODiOODleOCqeODquOCquOBriAke2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKX0lYCwKICAgICAgICBsYWJlbDon44Od44O844OI44OV44Kp44Oq44KqJwogICAgICB9KTsKICAgIH0KICB9CgogIGFjdGlvbnMuc29ydCgoeCx5KT0+eS5zY29yZS14LnNjb3JlKTsKCiAgY29uc3QgaW1wb3J0YW50PWFjdGlvbnMuZmlsdGVyKHg9Pnguc2NvcmU+PTcwKTsKICBjb25zdCBzaG93bj0oaW1wb3J0YW50Lmxlbmd0aD9pbXBvcnRhbnQ6YWN0aW9ucykuc2xpY2UoMCw0KTsKCiAgYm94LmlubmVySFRNTD1gPGRpdiBjbGFzcz0icHJpb3JpdHktd3JhcCI+JHsKICAgIHNob3duLm1hcCgoeCxpKT0+YAogICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1pdGVtICR7eC5jbHN9Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1saW5lIj4KICAgICAgICAgIDxkaXY+CiAgICAgICAgICAgIDxzcGFuIGNsYXNzPSJwcmlvcml0eS1yYW5rIj5QUklPUklUWSAke2krMX08L3NwYW4+CiAgICAgICAgICAgIDxiPiR7eC50aXRsZX08L2I+CiAgICAgICAgICA8L2Rpdj4KICAgICAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWNvZGUiPiR7eC5sYWJlbH08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NXB4Ij4ke3guZGV0YWlsfTwvZGl2PgogICAgICA8L2Rpdj4KICAgIGApLmpvaW4oJycpCiAgfTwvZGl2PmAgKyAoCiAgICBpbXBvcnRhbnQubGVuZ3RoCiAgICAgID8gJzxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7igLvlhKrlhYjluqbjga/oqK3lrprjg6njgqTjg7PmjqXov5Hjg7vliKTlrprnirbmhYvjg7vjg4fjg7zjgr/mnInnhKHjg7vpm4bkuK3luqbjgYvjgonkvZzjgovnorroqo3poIbjgafjgZnjgILoh6rli5Xlo7LosrfmjIfnpLrjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+JwogICAgICA6ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+57eK5oCl5bqm44Gu6auY44GE6aCF55uu44Gv44GC44KK44G+44Gb44KT44CC6YCa5bi455uj6KaW44KS57aZ57aa44CCPC9wPicKICApOwp9CgpmdW5jdGlvbiBwb3J0Zm9saW9NZWFuRm9yKGEsa2V5KXsKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wKTsKICBjb25zdCB0b3RhbD12YWxpZC5yZWR1Y2UoKHMsaCk9PnMrTnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKSwwKTsKICBpZih0b3RhbDw9MClyZXR1cm4gbnVsbDsKCiAgbGV0IG51bT0wLCBkZW49MDsKICB2YWxpZC5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IHN0PXN0YXRGb3IoaCxrZXkpOwogICAgY29uc3Qgdj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApOwogICAgaWYoc3QmJnN0LnN0YXR1cz09PSdvaycmJk51bWJlci5pc0Zpbml0ZShOdW1iZXIoc3QubWVhbikpJiZ2PjApewogICAgICBudW0gKz0gdipOdW1iZXIoc3QubWVhbik7CiAgICAgIGRlbiArPSB2OwogICAgfQogIH0pOwogIHJldHVybiBkZW4+MD9udW0vZGVuOm51bGw7Cn0KCmZ1bmN0aW9uIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwb3J0Zm9saW9TdW1tYXJ5Jyk7CiAgaWYoIWJveClyZXR1cm47CgogIGlmKCFhLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOiHquWLlembhuioiOOBl+OBvuOBmeOAgjwvcD4nOwogICAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7CiAgICByZXR1cm47CiAgfQoKICBsZXQgdG90YWxDb3N0PTAsIHRvdGFsVmFsdWU9MCwgdG90YWxOZXQ9MDsKICBjb25zdCByb3dzPVtdOwogIGNvbnN0IGRlY2lzaW9ucz17aG9sZDowLHdhdGNoOjAsdGFrZTowLHN0b3A6MCxwZW5kaW5nOjB9OwoKICBhLmZvckVhY2goaD0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICAgIGNvbnN0IHNoYXJlcz1OdW1iZXIoaC5zaGFyZXN8fDApOwogICAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgICBjb25zdCBjdXJyZW50VmFsdWU9dmFsaWQ/Y3VyKnNoYXJlczowOwogICAgY29uc3QgaW52ZXN0ZWQ9TnVtYmVyKGguY29zdHx8MCkqc2hhcmVzK2MuYnV5RmVlOwoKICAgIHRvdGFsQ29zdCArPSBpbnZlc3RlZDsKCiAgICBpZih2YWxpZCl7CiAgICAgIHRvdGFsVmFsdWUgKz0gY3VycmVudFZhbHVlOwogICAgICB0b3RhbE5ldCArPSBjLm5ldE5vdzsKCiAgICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgICAgaWYoZC5sYWJlbD09PSfmkI3liIfjgormpJzoqI4nKWRlY2lzaW9ucy5zdG9wKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKWRlY2lzaW9ucy50YWtlKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSforabmiJInKWRlY2lzaW9ucy53YXRjaCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjmoKrkvqHlj6TjgYTvvIknfHxkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOWxpeattOWPpOOBhO+8iScpZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgZWxzZSBkZWNpc2lvbnMuaG9sZCsrOwoKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTpjdXJyZW50VmFsdWUsbmV0OmMubmV0Tm93fSk7CiAgICB9ZWxzZXsKICAgICAgZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTowLG5ldDpudWxsfSk7CiAgICB9CiAgfSk7CgogIGNvbnN0IG5ldFBjdD10b3RhbENvc3Q+MD90b3RhbE5ldC90b3RhbENvc3QqMTAwOm51bGw7CiAgY29uc3QgbWF4VmFsdWU9cm93cy5yZWR1Y2UoKG0scik9Pk1hdGgubWF4KG0sci52YWx1ZSksMCk7CiAgY29uc3QgY29uY2VudHJhdGlvbj10b3RhbFZhbHVlPjA/bWF4VmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKCiAgY29uc3QgbWVhbjIwPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjBkJyk7CiAgY29uc3QgbWVhbjEyNj1wb3J0Zm9saW9NZWFuRm9yKGEsJzEyNmQnKTsKICBjb25zdCBtZWFuMjUyPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjUyZCcpOwoKICBjb25zdCBzdGFsZUhvbGRpbmdzPWEuZmlsdGVyKGg9PnsKICAgIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGguYXNvZik7CiAgICByZXR1cm4gaC5hc29mICYmICFmLmRlY2lzaW9uX29rOwogIH0pOwogIGNvbnN0IHN0YWxlTm90aWNlPXN0YWxlSG9sZGluZ3MubGVuZ3RoCiAgICA/IGA8ZGl2IGNsYXNzPSJmcmVzaGJveCBmcmVzaC1zdGFsZSIgc3R5bGU9Im1hcmdpbi1ib3R0b206OXB4Ij48Yj7wn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvyAke3N0YWxlSG9sZGluZ3MubGVuZ3RofemKmOafhDwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+6KmV5L6h6aGN44O75pCN55uK44Gv5pyA5paw5Y+W5b6X57WC5YCk44OZ44O844K544Gu5Y+C6ICD5YCk44Gn44GZ44CC5LuK5pel44Gu5aOy6LK35Yik5pat44Gr44Gv5L2/44KP44Ga44CB5a6f6Zqb44Gu54++5Zyo5YCk44KS56K66KqN44GX44Gm44GP44Gg44GV44GE44CCPC9kaXY+PC9kaXY+YAogICAgOiAnJzsKCiAgY29uc3Qgc3RhbGVIaXN0b3J5PWEuZmlsdGVyKGg9PnsKICAgIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogICAgcmV0dXJuIChoLmhpc3RvcnlfYXNvZnx8aC5hc29mKSAmJiAhZi5kZWNpc2lvbl9vazsKICB9KTsKICBjb25zdCBoaXN0b3J5Tm90aWNlPXN0YWxlSGlzdG9yeS5sZW5ndGgKICAgID8gYDxkaXYgY2xhc3M9Imhpc3Rvcnl3YXJuIiBzdHlsZT0ibWFyZ2luLWJvdHRvbTo5cHgiPjxiPvCfk5og5L6h5qC85bGl5q2044GM5Y+k44GEICR7c3RhbGVIaXN0b3J5Lmxlbmd0aH3pipjmn4Q8L2I+PGRpdiBjbGFzcz0ibXV0ZWQiPuS+oeagvOWxpeattOOBjOWPpOOBhOWgtOWQiOOAgeefreS4remVt+acn+ODiOODrOODs+ODieODu+acn+W+heWApOODu+mcgOe1pnByb3h544Gv5Y+C6ICD5YCk44Gn44GZ44CCPC9kaXY+PC9kaXY+YAogICAgOiAnJzsKCiAgY29uc3QgYWxsb2NhdGlvbnM9cm93cwogICAgLmZpbHRlcihyPT5yLnZhbHVlPjApCiAgICAuc29ydCgoeCx5KT0+eS52YWx1ZS14LnZhbHVlKQogICAgLm1hcChyPT57CiAgICAgIGNvbnN0IHc9dG90YWxWYWx1ZT4wP3IudmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKICAgICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJhbGxvYyI+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPjxiPiR7ci5jb2RlfTwvYj4ke3IubmFtZT8nICcrci5uYW1lOicnfSAvICR7dy50b0ZpeGVkKDEpfSU8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJhbGxvY2JhciI+PHNwYW4gc3R5bGU9IndpZHRoOiR7TWF0aC5taW4oMTAwLHcpfSUiPjwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+YDsKICAgIH0pLmpvaW4oJycpOwoKICBib3guaW5uZXJIVE1MPWAKICAgICR7c3RhbGVOb3RpY2V9CiAgICAke2hpc3RvcnlOb3RpY2V9CiAgICA8ZGl2IGNsYXNzPSJwb3J0cm93Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+57eP5oqV6LOH6aGNPC9zcGFuPjxiPiR7eWVuKHRvdGFsQ29zdCl9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnoqZXkvqHpoY08L3NwYW4+PGI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsVmFsdWUpOifigJQnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+W5b6X57WC5YCk44OZ44O844K55pCN55uKPC9zcGFuPjxiIGNsYXNzPSIke3RvdGFsTmV0Pj0wPydwb3MnOiduZWcnfSI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsTmV0KTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke25ldFBjdD09PW51bGw/J+KAlCc6bmV0UGN0LnRvRml4ZWQoMikrJyUnfTwvc3Bhbj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pyA5aSn6YqY5p+E5q+U546HPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKSsnJSc6J+KAlCd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGRpdiBjbGFzcz0icG9ydHJvdyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5L+d5pyJ57aZ57aaPC9zcGFuPjxiPiR7ZGVjaXNpb25zLmhvbGR9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7orabmiJI8L3NwYW4+PGI+JHtkZWNpc2lvbnMud2F0Y2h9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrmpJzoqI48L3NwYW4+PGI+JHtkZWNpc2lvbnMudGFrZX08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCii/kv53nlZk8L3NwYW4+PGI+JHtkZWNpc2lvbnMuc3RvcCtkZWNpc2lvbnMucGVuZGluZ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn5OKIOipleS+oemhjeWKoOmHjeOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODszwvaDQ+CiAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nn63mnJ8yMOaXpTwvc3Bhbj48Yj4ke21lYW4yMD09PW51bGw/J+KAlCc6bWVhbjIwLnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS4reacnzEyNuaXpTwvc3Bhbj48Yj4ke21lYW4xMjY9PT1udWxsPyfigJQnOm1lYW4xMjYudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZW35pyfMjUy5pelPC9zcGFuPjxiPiR7bWVhbjI1Mj09PW51bGw/J+KAlCc6bWVhbjI1Mi50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+4oC75ZCE6YqY5p+E44Gu6YGO5Y675bmz5Z2H44Oq44K/44O844Oz44KS54++5Zyo44Gu6KmV5L6h6aGN44Gn5Yqg6YeN44GX44Gf5Y+C6ICD5YCk44Gn44GZ44CC55u46Zai44KS6ICD5oWu44GX44Gf5bCG5p2l5LqI5ris44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgoKICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA1cHgiPvCfk6Yg6YqY5p+E5qeL5oiQPC9oND4KICAgICR7YWxsb2NhdGlvbnN8fCc8cCBjbGFzcz0ibXV0ZWQiPuWun+ODh+ODvOOCv+acquWPluW+lzwvcD4nfQogIGA7CiAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7Cn0KZnVuY3Rpb24gcmVuZGVySG9sZGluZ3MoKXsKICBpZigkKCdtb3ZlbWVudFVwJykpcmVuZGVyTW92ZW1lbnQoKTsKICBjb25zdCBvcGVuZWQ9bmV3IFNldChBcnJheS5mcm9tKCQoJ2hvbGRpbmdzJykucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlsc1tvcGVuXScpKS5tYXAoZWw9PmVsLmRhdGFzZXQuc3RvY2spKTsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGlmKCFhLmxlbmd0aCl7JCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5pyq55m76YyyPC9wPic7cmVuZGVyUG9ydGZvbGlvU3VtbWFyeSgpO3JldHVybn0KICAkKCdob2xkaW5ncycpLmlubmVySFRNTD1hLm1hcCgoaCxpKT0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IHZhbGlkUHJpY2U9TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCYmaC5hc29mOwogICAgY29uc3QgY2xzPXZhbGlkUHJpY2U/KGMubmV0Tm93Pj0wPydwb3MnOiduZWcnKTonJzsKICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKCiAgICByZXR1cm4gYDxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIiBkYXRhLXN0b2NrPSIke3dhdGNoRXNjYXBlKGguY29kZSl9IiAke29wZW5lZC5oYXMoU3RyaW5nKGguY29kZSkpPydvcGVuJzonJ30+PHN1bW1hcnk+JHt3YXRjaEVzY2FwZShoLmNvbXBhbnlfbmFtZXx8aC5jb2RlKX08L3N1bW1hcnk+PGRpdiBjbGFzcz0iaG9sZGluZyI+CiAgICAgIDxkaXYgY2xhc3M9ImhvbGRpbmctaGVhZCI+CiAgICAgICAgPGRpdj4KICAgICAgICAgIDxiPiR7aC5jb2RlfTwvYj4KICAgICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj4ke2guY29tcGFueV9uYW1lfHwiIn08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gc2Vjb25kYXJ5IiBvbmNsaWNrPSJlZGl0SG9sZGluZ1NoYXJlcygke2l9KSI+5qCq5pWw5aSJ5pu0PC9idXR0b24+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hIb2xkaW5nKCR7aX0pIj7mm7TmlrA8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlSG9sZGluZygke2l9KSI+5YmK6ZmkPC9idXR0b24+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuePvuWcqOWApOODh+ODvOOCv+aXpSAke2guYXNvZnx8J+KAlCd9IC8g5pyA5paw5Y+W5b6X57WC5YCkICR7dmFsaWRQcmljZT95ZW4oaC5jdXJyZW50X3ByaWNlKTon4oCUJ30gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5qCq5L6h44K944O844K5ICR7aC5wcmljZV9zb3VyY2V8fCfigJQnfSAvIOS+oeagvOWxpeattCAke2guaGlzdG9yeV9hc29mfHwn4oCUJ30gJHtoLmhpc3Rvcnlfc291cmNlPycoJytoLmhpc3Rvcnlfc291cmNlKycpJzonJ308L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuWGjeioiOeulyAke2gudXBkYXRlZF9hdD9uZXcgRGF0ZShoLnVwZGF0ZWRfYXQpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpOifigJQnfSAvIOaQjeebiuODu+WbuuWumuODqeOCpOODs+i3nembouOBr+OBk+OBruacgOaWsOWPluW+l+e1guWApOOCkuS9v+eUqDwvZGl2PgogICAgICAke3ZhbGlkUHJpY2U/YDxkaXYgY2xhc3M9ImZyZXNoYm94ICR7ZnJlc2huZXNzRm9yKGguYXNvZikuY2xzfSI+PGI+JHsKICAgICAgICBmcmVzaG5lc3NGb3IoaC5hc29mKS5sZXZlbD09PSdmcmVzaCc/J+KchSDprq7luqZPSyc6CiAgICAgICAgZnJlc2huZXNzRm9yKGguYXNvZikubGV2ZWw9PT0nd2FybmluZyc/J+KaoO+4jyDpgYXlu7bms6jmhI8nOgogICAgICAgICfwn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvycKICAgICAgfTwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+JHtmcmVzaG5lc3NUZXh0KGguYXNvZil9PC9kaXY+PC9kaXY+YDonJ30KCiAgICAgIDxkaXYgY2xhc3M9ImRlY2lzaW9uICR7ZC5jbHN9Ij4KICAgICAgICAke2QubGFiZWx9CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjRweCI+JHtkLnJlYXNvbn08L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMuc3RvcFByaWNlLCdzdG9wJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMudGFrZVByaWNlLCd0YWtlJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKTlrprkv6HpoLzluqY8L3NwYW4+CiAgICAgICAgICA8Yj4ke2QuY29uZmlkZW5jZX0lPC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0iZ2F1Z2UiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke2QuY29uZmlkZW5jZX0lIj48L3NwYW4+PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPuKAu+WIpOWumuS/oemgvOW6puOBr+OAgeODh+ODvOOCv+WFhei2s+W6puODu+acn+mWk+ODiOODrOODs+ODieOBruS4gOiHtOW6puODu+acn+W+heWApOe1seioiOOBruacieeEoeOBi+OCieS9nOOCi+WPguiAg+aMh+aomeOBp+OAgeeahOS4reeiuueOh+OBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICAgIDxkaXYgY2xhc3M9ImFjdGlvbmJveCI+CiAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7ku4rjganjgYbjgZnjgovvvJ88L3NwYW4+CiAgICAgICAgPGIgc3R5bGU9ImRpc3BsYXk6YmxvY2s7bWFyZ2luLXRvcDozcHgiPiR7YWN0aW9uVGV4dChoLGMsZCl9PC9iPgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7Y2xzfSI+JHt2YWxpZFByaWNlP3llbihjLm5ldE5vdyk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt2YWxpZFByaWNlP2ZtdChjLm5ldE5vd1BjdCkrJyUnOifigJQnfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6LK35LuY5omL5pWw5paZPC9zcGFuPjxiPiR7eWVuKGMuYnV5RmVlKX08L2I+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWjsuWNtOaJi+aVsOaWmSjku4opPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy5zZWxsRmVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnN0b3BQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMuc3RvcE5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy50YWtlUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnRha2VOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844Oq44Oz44Kw5Y+C6ICDPC9zcGFuPjxiPiR7dmFsaWRQcmljZSYmYy50cmFpbFByaWNlIT09bnVsbD95ZW4oYy50cmFpbFByaWNlKTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke2ZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKS5kZWNpc2lvbl9vaz8nMjDml6Xpq5jlgKTln7rmupYnOiflsaXmrbTjgYzlj6TjgYTjgZ/jgoHkv53nlZknfTwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn46vIOWun+e4vuODmeODvOOCueS+oeagvOODrOODs+OCuDwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke3JhbmdlSHRtbCgn55+t5pyfIDIw5pelJyxjdXIsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+S4reacnyAxMjbml6UnLGN1cixzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+mVt+acnyAyNTLml6UnLGN1cixzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TkCDlrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtldkh0bWwoJ+efreacnyAyMOaXpScsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+S4reacnyAxMjbml6UnLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke2V2SHRtbCgn6ZW35pyfIDI1MuaXpScsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KICAgICAgPHAgY2xhc3M9Im11dGVkIj7igLvkvqHmoLzjg6zjg7Pjgrjjg7vmnJ/lvoXlgKTjga/lsIbmnaXkuojmuKzjgafjga/jgarjgY/jgIHlj5blvpflj6/og73jgarpgY7ljrvmoKrkvqHjga7jg63jg7zjg6rjg7PjgrDliY3mlrnjg6rjgr/jg7zjg7PliIbluIPjgpLmnIDmlrDlj5blvpfntYLlgKTjgavlvZPjgabjga/jgoHjgZ/ntbHoqIjlj4LogIPjgafjgZnjgILmnJ/plpPjgYzph43jgarjgovmqJnmnKzjgpLlkKvjgb/jgb7jgZnjgII8L3A+CiAgICA8L2Rpdj48L2RldGFpbHM+YDsKICB9KS5qb2luKCcnKTsKICByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldFF1b3RlKGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3F1b3RlP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCBxPWF3YWl0IHIuanNvbigpOwogIGlmKHEuc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IocS5yZWFzb258fHEuZXJyb3J8fCflrp/jg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4gcTsKfQoKYXN5bmMgZnVuY3Rpb24gYWRkSG9sZGluZygpewogIGNvbnN0IGNvZGU9JCgnaG9sZENvZGUnKS52YWx1ZS50cmltKCk7CiAgY29uc3QgY29zdD12YWwoJ2hvbGRDb3N0JyksIHNoYXJlcz12YWwoJ2hvbGRTaGFyZXMnKTsKICBjb25zdCBzdG9wPXZhbCgnc3RvcFBjdCcpLCB0YWtlPXZhbCgndGFrZVBjdCcpLCB0cmFpbD12YWwoJ3RyYWlsUGN0Jyk7CiAgY29uc3QgZmVlTW9kZT0kKCdmZWVNb2RlJykudmFsdWU7CiAgaWYoIWNvZGV8fCFjb3N0fHwhc2hhcmVzKXthbGVydCgn6YqY5p+E44Kz44O844OJ44O75Y+W5b6X5Y2Y5L6h44O75qCq5pWw44KS5YWl5Yqb44GX44Gm44GtJyk7cmV0dXJufQogIGNvbnN0IGJ0bj1ldmVudD8udGFyZ2V0OyBpZihidG4pe2J0bi5kaXNhYmxlZD10cnVlO2J0bi50ZXh0Q29udGVudD0n5Y+W5b6X5Lit4oCmJ30KICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICAgIGNvbnN0IGg9ewogICAgICBjb2RlLAogICAgICBjb21wYW55X25hbWU6KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHwnJywKICAgICAgY29tcGFueV9tYXJrZXQ6KHEuY29tcGFueSYmcS5jb21wYW55Lm1hcmtldCl8fCcnLAogICAgICBjb21wYW55X3NlY3RvcjMzOihxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fCcnLAogICAgICBjb3N0LCBzaGFyZXMsIGZlZV9tb2RlOmZlZU1vZGUsCiAgICAgIHN0b3BfcGN0OnN0b3A/PzgsIHRha2VfcGN0OnRha2U/PzE1LCB0cmFpbF9wY3Q6dHJhaWw/PzcsCiAgICAgIGN1cnJlbnRfcHJpY2U6cy5sYXN0X2Nsb3NlLCBoaWdoXzIwZDpzLmhpZ2hfMjBkLCBsb3dfMjBkOnMubG93XzIwZCwKICAgICAgcmV0dXJuXzIwZDpzLnJldHVybl8yMGQsIHJldHVybl8xMjZkOnMucmV0dXJuXzEyNmQsIHJldHVybl8yNTJkOnMucmV0dXJuXzI1MmQsCiAgICAgIGZvcndhcmRfc3RhdHM6cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30sCiAgICAgIGFzb2Y6cy5sYXN0X2RhdGUsCiAgICAgIGhpc3RvcnlfYXNvZjpzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSwKICAgICAgcHJpY2Vfc291cmNlOnMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8JycsCiAgICAgIGhpc3Rvcnlfc291cmNlOnMuaGlzdG9yeV9zb3VyY2V8fCcnLAogICAgICB1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSwKICAgICAgcHJpY2Vfc3luY2VkOnRydWUKICAgIH07CiAgICBjb25zdCBpZHg9YS5maW5kSW5kZXgoeD0+eC5jb2RlPT09Y29kZSk7CiAgICBpZihpZHg+PTApYVtpZHhdPWg7IGVsc2UgYS5wdXNoKGgpOwogICAgc2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpOwogICAgcmVuZGVySG9sZGluZ3MoKTsKICB9Y2F0Y2goZSl7YWxlcnQoJ+WPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UpfQogIGZpbmFsbHl7aWYoYnRuKXtidG4uZGlzYWJsZWQ9ZmFsc2U7YnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZgnfX0KfQoKCmZ1bmN0aW9uIGFwcGx5UXVvdGVUb0hvbGRpbmcoaCxxKXsKICBjb25zdCBzPShxJiZxLnNuYXBzaG90KXx8e307CiAgaC5jb21wYW55X25hbWU9KHEuY29tcGFueSYmcS5jb21wYW55Lm5hbWUpfHxoLmNvbXBhbnlfbmFtZXx8Jyc7CiAgaC5jb21wYW55X21hcmtldD0ocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8aC5jb21wYW55X21hcmtldHx8Jyc7CiAgaC5jb21wYW55X3NlY3RvcjMzPShxLmNvbXBhbnkmJnEuY29tcGFueS5zZWN0b3IzMyl8fGguY29tcGFueV9zZWN0b3IzM3x8Jyc7CiAgaC5jdXJyZW50X3ByaWNlPXMubGFzdF9jbG9zZTsKICBoLmhpZ2hfMjBkPXMuaGlnaF8yMGQ7CiAgaC5sb3dfMjBkPXMubG93XzIwZDsKICBoLnJldHVybl8yMGQ9cy5yZXR1cm5fMjBkOwogIGgucmV0dXJuXzEyNmQ9cy5yZXR1cm5fMTI2ZDsKICBoLnJldHVybl8yNTJkPXMucmV0dXJuXzI1MmQ7CiAgaC5mb3J3YXJkX3N0YXRzPXMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9OwogIGguYXNvZj1zLmxhc3RfZGF0ZTsKICBoLmhpc3RvcnlfYXNvZj1zLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZTsKICBoLnByaWNlX3NvdXJjZT1zLnByaWNlX3NvdXJjZXx8cS5zb3VyY2V8fCcnOwogIGguaGlzdG9yeV9zb3VyY2U9cy5oaXN0b3J5X3NvdXJjZXx8Jyc7CiAgaC51cGRhdGVkX2F0PW5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTsKICBoLnByaWNlX3N5bmNlZD10cnVlOwogIHJldHVybiBoOwp9CgpmdW5jdGlvbiBzeW5jQW5hbHl6ZWRRdW90ZVRvSG9sZGluZyhjb2RlLHEpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgaT1hLmZpbmRJbmRleCh4PT5TdHJpbmcoeC5jb2RlKT09PVN0cmluZyhjb2RlKSk7CiAgaWYoaTwwKXJldHVybiBmYWxzZTsKICBhW2ldPWFwcGx5UXVvdGVUb0hvbGRpbmcoYVtpXSxxKTsKICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgcmVuZGVySG9sZGluZ3MoKTsKICByZXR1cm4gdHJ1ZTsKfQoKYXN5bmMgZnVuY3Rpb24gcmVmcmVzaEFsbEhvbGRpbmdzKGZvcmNlPWZhbHNlKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IHN0YXR1cz0kKCdob2xkaW5nUmVmcmVzaFN0YXR1cycpOwogIGNvbnN0IGJ0bj0kKCdyZWZyZXNoQWxsQnRuJyk7CgogIGlmKCFhLmxlbmd0aCl7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PSfkv53mnInmoKrjga/mnKrnmbvpjLLjgafjgZnjgIInOwogICAgcmV0dXJuOwogIH0KCiAgY29uc3Qga2V5PSdmcmVlX2hvbGRpbmdzX2xhc3RfYXV0b19yZWZyZXNoX3YxNic7CiAgY29uc3QgbGFzdD1OdW1iZXIobG9jYWxTdG9yYWdlLmdldEl0ZW0oa2V5KXx8MCk7CiAgY29uc3Qgbm93TXM9RGF0ZS5ub3coKTsKICBjb25zdCB3YWl0TXM9MzAqNjAqMTAwMDsKCiAgaWYoIWZvcmNlICYmIGxhc3QgJiYgbm93TXMtbGFzdDx3YWl0TXMpewogICAgY29uc3QgbWluPU1hdGguY2VpbCgod2FpdE1zLShub3dNcy1sYXN0KSkvNjAwMDApOwogICAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1g6Ieq5YuV5pu05paw5riI44G/44CC5qyh44Gu6Ieq5YuV5pu05paw44G+44Gn57SEJHttaW595YiG44CCYDsKICAgIHJldHVybjsKICB9CgogIGlmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSfmm7TmlrDkuK3igKYnfQogIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9YOS/neacieagqiAke2EubGVuZ3RofemKmOafhOOBruacgOaWsOe1guWApOOCkuWPluW+l+S4reKApmA7CgogIGxldCBvaz0wLCBuZz0wOwogIGZvcihsZXQgaT0wO2k8YS5sZW5ndGg7aSsrKXsKICAgIHRyeXsKICAgICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShhW2ldLmNvZGUpOwogICAgICBhW2ldPWFwcGx5UXVvdGVUb0hvbGRpbmcoYVtpXSxxKTsKICAgICAgb2srKzsKICAgIH1jYXRjaChlKXsKICAgICAgbmcrKzsKICAgICAgYVtpXS5sYXN0X3JlZnJlc2hfZXJyb3I9U3RyaW5nKGUubWVzc2FnZXx8ZSk7CiAgICB9CiAgfQoKICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oa2V5LFN0cmluZyhEYXRlLm5vdygpKSk7CiAgcmVuZGVySG9sZGluZ3MoKTsKCiAgaWYoc3RhdHVzKXsKICAgIHN0YXR1cy50ZXh0Q29udGVudD1g5pyA5paw57WC5YCk44Gn5YaN6KiI566X77ya5oiQ5YqfICR7b2t96YqY5p+EJHtuZz9gIC8g5aSx5pWXICR7bmd96YqY5p+EYDonJ33jgIJgOwogIH0KICBpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+S/neacieagquOCkuacgOaWsOe1guWApOOBp+S4gOaLrOabtOaWsCd9Cn0KCmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hIb2xkaW5nKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyksIGg9YVtpXTsgaWYoIWgpcmV0dXJuOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoaC5jb2RlKTsKICAgIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhoLHEpOwogICAgc2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpOwogICAgcmVuZGVySG9sZGluZ3MoKTsKICAgIGNvbnN0IHN0YXR1cz0kKCdob2xkaW5nUmVmcmVzaFN0YXR1cycpOwogICAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1gJHtoLmNvZGV9IOOCkuacgOaWsOWPluW+l+e1guWApOOBp+WGjeioiOeul+OBl+OBvuOBl+OBn+OAgmA7CiAgfWNhdGNoKGUpe2FsZXJ0KCfmm7TmlrDjgqjjg6njg7w6ICcrZS5tZXNzYWdlKX0KfQpmdW5jdGlvbiBlZGl0SG9sZGluZ1NoYXJlcyhpKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpLCBoPWFbaV07CiAgaWYoIWgpcmV0dXJuOwogIGNvbnN0IGlucHV0PXByb21wdChgJHtoLmNvZGV9IOOBruaWsOOBl+OBhOS/neacieagquaVsOOCkuWFpeWKm+OBl+OBpuOBre+8iDHmoKrku6XkuIrjga7mlbTmlbDvvIlgLFN0cmluZyhoLnNoYXJlcykpOwogIGlmKGlucHV0PT09bnVsbClyZXR1cm47CiAgY29uc3QgdGV4dD1pbnB1dC50cmltKCk7CiAgY29uc3Qgc2hhcmVzPU51bWJlcih0ZXh0KTsKICBpZighL15bMC05XSskLy50ZXN0KHRleHQpfHwhTnVtYmVyLmlzU2FmZUludGVnZXIoc2hhcmVzKXx8c2hhcmVzPDEpewogICAgYWxlcnQoJ+agquaVsOOBrzHku6XkuIrjga7mlbTmlbDjgaflhaXlipvjgZfjgabjga0nKTtyZXR1cm47CiAgfQogIGguc2hhcmVzPXNoYXJlczsKICBoLnNoYXJlc191cGRhdGVkX2F0PW5ldyBEYXRlKCkudG9JU09TdHJpbmcoKTsKICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgcmVuZGVySG9sZGluZ3MoKTsKICBjb25zdCBzdGF0dXM9JCgnaG9sZGluZ1JlZnJlc2hTdGF0dXMnKTsKICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWAke2guY29kZX0g44KSICR7c2hhcmVzfeagquOBq+WkieabtOOBl+OAgeaQjeebiuODu+aJi+aVsOaWmeODu+S/neacieWFqOS9k+OBrumbhuioiOOCkuWGjeioiOeul+OBl+OBvuOBl+OBn+OAgmA7Cn0KZnVuY3Rpb24gcmVtb3ZlSG9sZGluZyhpKXtjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpO2Euc3BsaWNlKGksMSk7c2F2ZSgnZnJlZV9ob2xkaW5nc192MTMnLGEpO3JlbmRlckhvbGRpbmdzKCl9CgpmdW5jdGlvbiB3YXRjaEVzY2FwZSh2YWx1ZSl7CiAgcmV0dXJuIFN0cmluZyh2YWx1ZT8/JycpLnJlcGxhY2UoL1smPD4iJ10vZyxjPT4oeycmJzonJmFtcDsnLCc8JzonJmx0OycsJz4nOicmZ3Q7JywnIic6JyZxdW90OycsIiciOicmIzM5Oyd9W2NdKSk7Cn0KY29uc3Qgd2F0Y2hCdXN5PW5ldyBTZXQoKTsKbGV0IHdhdGNoQnVsa0J1c3k9ZmFsc2U7CmxldCBtb3ZlbWVudEJ1c3k9ZmFsc2U7CmZ1bmN0aW9uIHJlbmRlcldhdGNoKCl7CiAgaWYoJCgnbW92ZW1lbnRVcCcpKXJlbmRlck1vdmVtZW50KCk7CiAgY29uc3QgYnVsa0J0bj0kKCd3YXRjaEJ1bGtCdG4nKTsKICBpZihidWxrQnRuKXtidWxrQnRuLmRpc2FibGVkPW1vdmVtZW50QnVzeXx8d2F0Y2hCdWxrQnVzeXx8d2F0Y2hCdXN5LnNpemU+MDtidWxrQnRuLnRleHRDb250ZW50PXdhdGNoQnVsa0J1c3k/J+S4gOaLrOWIhuaekOS4reKApic6J+iyt+OBhOaZguOBruWPguiAg+OCkuS4gOaLrOabtOaWsCd9CiAgY29uc3Qgb3BlbmVkPW5ldyBTZXQoQXJyYXkuZnJvbSgkKCd3YXRjaHMnKS5xdWVyeVNlbGVjdG9yQWxsKCdkZXRhaWxzW29wZW5dJykpLm1hcChlbD0+ZWwuZGF0YXNldC5zdG9jaykpOwogIGNvbnN0IGNhY2hlPWxvY2FsU3RvcmFnZS5nZXRJdGVtKCdmcmVlX3dhdGNoX2FuYWx5c2lzX3YxJyk7CiAgbGV0IGFuYWx5c2VzPXt9O3RyeXthbmFseXNlcz1KU09OLnBhcnNlKGNhY2hlfHwne30nKX1jYXRjaChlKXt9CiAgJCgnd2F0Y2hzJykuaW5uZXJIVE1MPWxvY2FsKCdmcmVlX3dhdGNoJykubWFwKCh4LGkpPT57CiAgICBjb25zdCBjb2RlPVN0cmluZyh0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKTsKICAgIGNvbnN0IG5hbWU9dHlwZW9mIHg9PT0nc3RyaW5nJz8nJzp4Lm5hbWU7CiAgICBjb25zdCB0aW1pbmc9d2F0Y2hUaW1pbmcoYW5hbHlzZXNbY29kZV0pOwogICAgY29uc3QgYmFkZ2VDbGFzcz10aW1pbmcubGFiZWw9PT0n6LK344GE5YCZ6KOc77yI5p2h5Lu25LiA6Ie077yJJz8nd2F0Y2gtYnV5Jzp0aW1pbmcubGFiZWw9PT0n5qeY5a2Q6KaLJz8nd2F0Y2gtbmV1dHJhbCc6J3dhdGNoLXBlbmRpbmcnOwogICAgcmV0dXJuIGA8ZGV0YWlscyBjbGFzcz0ic3RvY2stZGV0YWlscyIgZGF0YS1zdG9jaz0iJHt3YXRjaEVzY2FwZShjb2RlKX0iICR7b3BlbmVkLmhhcyhjb2RlKT8nb3Blbic6Jyd9PgogICAgICA8c3VtbWFyeT4ke3dhdGNoRXNjYXBlKG5hbWV8fGNvZGUpfSA8c3BhbiBjbGFzcz0id2F0Y2gtdGltaW5nICR7YmFkZ2VDbGFzc30iPiR7d2F0Y2hCdXN5Lmhhcyhjb2RlKT8n5YiG5p6Q5Lit4oCmJzp3YXRjaEVzY2FwZSh0aW1pbmcubGFiZWwpfTwvc3Bhbj48L3N1bW1hcnk+CiAgICAgIDxkaXYgY2xhc3M9IndhdGNoLWNvbnRlbnQiPjxiPiR7d2F0Y2hFc2NhcGUoY29kZSl9ICR7d2F0Y2hFc2NhcGUobmFtZXx8JycpfTwvYj4KICAgICAgICA8ZGl2IGNsYXNzPSJyb3ciIHN0eWxlPSJtYXJnaW46MTBweCAwIj48YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hXYXRjaCgke2l9KSIgJHt3YXRjaEJ1bGtCdXN5fHx3YXRjaEJ1c3kuaGFzKGNvZGUpPydkaXNhYmxlZCc6Jyd9PiR7d2F0Y2hCdXN5Lmhhcyhjb2RlKT8n5YiG5p6Q5Lit4oCmJzon6LK344GE5pmC44Gu5Y+C6ICD44KS5pu05pawJ308L2J1dHRvbj48YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBkYW5nZXIiIG9uY2xpY2s9InJlbW92ZVdhdGNoKCR7aX0pIj7liYrpmaQ8L2J1dHRvbj48L2Rpdj4KICAgICAgICAke3dhdGNoQW5hbHlzaXNIdG1sKGFuYWx5c2VzW2NvZGVdKX0KICAgICAgPC9kaXY+PC9kZXRhaWxzPmA7CiAgfSkuam9pbignJyl8fCc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nOwp9CmZ1bmN0aW9uIHdhdGNoVGltaW5nKGEpewogIGlmKCFhKXJldHVybiB7bGFiZWw6J+acquWIhuaekCcscmVhc29uOifjgIzosrfjgYTmmYLjga7lj4LogIPjgpLmm7TmlrDjgI3jgafjg4fjg7zjgr/jgpLlj5blvpfjgZfjgb7jgZnjgIInfTsKICBjb25zdCBzPWEuc25hcHNob3R8fHt9LHNjPWEuc2NvcmV8fHt9OwogIGlmKCFmcmVzaG5lc3NGb3Iocy5sYXN0X2RhdGUpLmRlY2lzaW9uX29rfHwhZnJlc2huZXNzRm9yKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlKS5kZWNpc2lvbl9va3x8ZGF0YUFnZURheXMocy5sYXN0X2RhdGUpPjR8fGRhdGFBZ2VEYXlzKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlKT40KQogICAgcmV0dXJuIHtsYWJlbDon5Yik5a6a5L+d55WZJyxyZWFzb246J+agquS+oeODu+S+oeagvOWxpeattOOBjOWPpOOBhOOAgeOBvuOBn+OBr+aXpeS7mOS4jeaYjuOBp+OBmeOAgid9OwogIGlmKHNjLnNjb3JlMTAwPT1udWxsfHwhTnVtYmVyLmlzRmluaXRlKE51bWJlcihzYy5zY29yZTEwMCkpfHxzYy5jb3ZlcmFnZV9wY3Q9PW51bGx8fCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHNjLmNvdmVyYWdlX3BjdCkpfHxOdW1iZXIoc2MuY292ZXJhZ2VfcGN0KTw3MHx8cy5sYXN0X2Nsb3NlPT1udWxsfHwhTnVtYmVyLmlzRmluaXRlKE51bWJlcihzLmxhc3RfY2xvc2UpKXx8TnVtYmVyKHMubGFzdF9jbG9zZSk8PTApCiAgICByZXR1cm4ge2xhYmVsOifliKTlrprkv53nlZknLHJlYXNvbjon6a6u5bqm44KS5Yqg5ZGz44GX44Gf44OH44O844K/5YWF6Laz5bqm44GMNzAl5pyq5rqA44CB44G+44Gf44Gv57eP5ZCI54K544KS566X5Ye644Gn44GN44G+44Gb44KT44CCJ307CiAgY29uc3QgcHJpY2U9TnVtYmVyKHMubGFzdF9jbG9zZSksaGlnaD1OdW1iZXIocy5oaWdoXzIwZCksbG93PU51bWJlcihzLmxvd18yMGQpOwogIGNvbnN0IHBvc2l0aW9uPWhpZ2g+bG93PyhwcmljZS1sb3cpLyhoaWdoLWxvdyk6bnVsbDsKICBpZihOdW1iZXIoc2Muc2NvcmUxMDApPDQ1KXJldHVybiB7bGFiZWw6J+anmOWtkOimiycscmVhc29uOifnt4/lkIjngrnjgYw0NeeCueacqua6gOOBp+OAgeW8seOBhOimgee0oOOBjOWEquWLouOBp+OBmeOAgid9OwogIGlmKHBvc2l0aW9uIT09bnVsbCYmcG9zaXRpb24+PTAuOSlyZXR1cm4ge2xhYmVsOifpq5jlgKTlnI/jg7vov73jgYTosrfjgYTms6jmhI8nLHJlYXNvbjonMjDml6XplpPjga7pq5jlgKTjg7vlronlgKTjga7nr4Tlm7LjgafkuIrkvY0xMCXjgavkvY3nva7jgZfjgabjgYTjgb7jgZnjgIInfTsKICBpZihOdW1iZXIoc2Muc2NvcmUxMDApPj02NSYmcy5yZXR1cm5fMjBkIT1udWxsJiZOdW1iZXIocy5yZXR1cm5fMjBkKT4wKQogICAgcmV0dXJuIHtsYWJlbDon6LK344GE5YCZ6KOc77yI5p2h5Lu25LiA6Ie077yJJyxyZWFzb246J+e3j+WQiOeCuTY154K55Lul5LiK44O7MjDml6XpqLDokL3njofjg5fjg6njgrnjg7vjg4fjg7zjgr/lhYXotrPluqY3MCXku6XkuIrjgILos7zlhaXliY3jgavnj77lnKjlgKTjgpLnorroqo3jgZfjgabjgY/jgaDjgZXjgYTjgIInfTsKICByZXR1cm4ge2xhYmVsOifmp5jlrZDoposnLHJlYXNvbjon6LK344GE5YCZ6KOc44Gu5p2h5Lu244GM5o+D44Gj44Gm44GE44G+44Gb44KT44CCJ307Cn0KZnVuY3Rpb24gd2F0Y2hBbmFseXNpc0h0bWwoYSl7CiAgaWYoIWEpcmV0dXJuICc8cCBjbGFzcz0ibXV0ZWQiPuacquWIhuaekOOAguOAjOiyt+OBhOaZguOBruWPguiAg+OCkuabtOaWsOOAjeOCkuaKvOOBl+OBpuOBreOAgjwvcD4nOwogIGNvbnN0IHM9YS5zbmFwc2hvdHx8e30sc2M9YS5zY29yZXx8e30sdD13YXRjaFRpbWluZyhhKTsKICByZXR1cm4gYDxkaXYgY2xhc3M9ImRlY2lzaW9uIGQtd2F0Y2giPiR7d2F0Y2hFc2NhcGUodC5sYWJlbCl9PGRpdiBjbGFzcz0ibXV0ZWQiPiR7d2F0Y2hFc2NhcGUodC5yZWFzb24pfTwvZGl2PjwvZGl2PgogICAgPHAgY2xhc3M9Im11dGVkIj7liIbmnpDmm7TmlrAgJHt3YXRjaEVzY2FwZShuZXcgRGF0ZShhLnVwZGF0ZWRfYXQpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpKX08YnI+5pyA5paw5Y+W5b6X57WC5YCkICR7eWVuKHMubGFzdF9jbG9zZSl9IC8g44OH44O844K/5pelICR7d2F0Y2hFc2NhcGUocy5sYXN0X2RhdGV8fCfigJQnKX08YnI+5L6h5qC85bGl5q20ICR7d2F0Y2hFc2NhcGUocy5oaXN0b3J5X2xhc3RfZGF0ZXx8cy5sYXN0X2RhdGV8fCfigJQnKX0gLyDjgr3jg7zjgrkgJHt3YXRjaEVzY2FwZShzLnByaWNlX3NvdXJjZXx8YS5zb3VyY2V8fCfigJQnKX08L3A+CiAgICA8ZGl2IGNsYXNzPSJncmlkMyI+PGRpdiBjbGFzcz0ia3BpIj7nt4/lkIjngrk8Yj4ke3NjLnNjb3JlMTAwPT1udWxsPyfigJQnOmZtdChzYy5zY29yZTEwMCwxKSsnIC8gMTAwJ308L2I+PC9kaXY+PGRpdiBjbGFzcz0ia3BpIj7jg4fjg7zjgr/lhYXotrPluqY8Yj4ke2ZtdChzYy5jb3ZlcmFnZV9wY3QsMSl9JTwvYj48L2Rpdj48ZGl2IGNsYXNzPSJrcGkiPuODiOODrOODs+ODiTxiPiR7d2F0Y2hFc2NhcGUoc3RhdGVKYSgoYS5zaWduYWx8fHt9KS5zdGF0ZSkpfTwvYj48L2Rpdj48L2Rpdj4KICAgIDxwPuODhuOCr+ODi+OCq+ODqyAke3Njb3JlTGFiZWwoc2MudGVjaG5pY2FsKX0gLyDmsbrnrpcgJHtzY29yZUxhYmVsKHNjLmVhcm5pbmdzKX0gLyDpnIDntaYgJHtzY29yZUxhYmVsKHNjLnN1cHBseSl9IC8g5Zu9562WcHJveHkgJHtzY29yZUxhYmVsKHNjLnBvbGljeSl9PC9wPgogICAgPHAgY2xhc3M9Im11dGVkIj4ke3dhdGNoRXNjYXBlKGRyaXZlclNlbnRlbmNlKHNjKSl9PC9wPgogICAgPGRpdiBjbGFzcz0iZ3JpZDMiPjxkaXYgY2xhc3M9ImtwaSI+MjDml6XpqLDokL3njoc8Yj4ke3BjdChzLnJldHVybl8yMGQpfTwvYj48L2Rpdj48ZGl2IGNsYXNzPSJrcGkiPjEyNuaXpemosOiQveeOhzxiPiR7cGN0KHMucmV0dXJuXzEyNmQpfTwvYj48L2Rpdj48ZGl2IGNsYXNzPSJrcGkiPjI1MuaXpemosOiQveeOhzxiPiR7cGN0KHMucmV0dXJuXzI1MmQpfTwvYj48L2Rpdj48L2Rpdj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+NeaXpe+8jzIw5pel5Ye65p2l6auY5q+UICR7Zm10KChzLnN1cHBseV9wcm94eXx8e30pLnZvbHVtZV9yYXRpb181XzIwKX3lgI0gLyAyMOaXpemrmOWApCAke3llbihzLmhpZ2hfMjBkKX0gLyDlronlgKQgJHt5ZW4ocy5sb3dfMjBkKX08L3A+CiAgICA8aDQ+5a6f57i+44OZ44O844K55pyf5b6F5YCk77yI57Wx6KiI5Y+C6ICD77yJPC9oND48ZGl2IGNsYXNzPSJncmlkMyI+JHtldkh0bWwoJ+efreacnzIw5pelJywocy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30pWycyMGQnXSl9JHtldkh0bWwoJ+S4reacnzEyNuaXpScsKHMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9KVsnMTI2ZCddKX0ke2V2SHRtbCgn6ZW35pyfMjUy5pelJywocy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e30pWycyNTJkJ10pfTwvZGl2PgogICAgPHAgY2xhc3M9Im11dGVkIj7osrfjgYTlgJnoo5zjga/mnaHku7bliKTlrprjgafjgIHlsIbmnaXjga7liKnnm4rjgoTosrfjgYTmmYLjgpLkv53oqLzjgZnjgovjgoLjga7jgafjga/jgYLjgorjgb7jgZvjgpPjgILmsbrnrpfjg7vlm73nrZbjga/lj5blvpfjgafjgY3jgZ/opoHntKDjga7jgb/kvb/nlKjjgZfjgIHkuI3mmI7lgKTjga/oo5zjgYTjgb7jgZvjgpPjgILkvqHmoLzjg7vlh7rmnaXpq5jjga/pgYXlu7bjgZnjgovloLTlkIjjgYzjgYLjgorjgb7jgZnjgILmnJ/lvoXlgKTjga/pgY7ljrvjga7ph43opIfmnJ/plpPjgpLlkKvjgoDntbHoqIjlj4LogIPjgafjgZnjgII8L3A+YDsKfQphc3luYyBmdW5jdGlvbiByZWZyZXNoV2F0Y2goaSxidWxrPWZhbHNlKXsKICBpZihtb3ZlbWVudEJ1c3l8fHdhdGNoQnVsa0J1c3kmJiFidWxrKXJldHVybiBmYWxzZTsKICBjb25zdCBpdGVtPWxvY2FsKCdmcmVlX3dhdGNoJylbaV07aWYoaXRlbT09PXVuZGVmaW5lZClyZXR1cm47CiAgY29uc3QgY29kZT1TdHJpbmcodHlwZW9mIGl0ZW09PT0nc3RyaW5nJz9pdGVtOml0ZW0uY29kZSk7CiAgaWYod2F0Y2hCdXN5Lmhhcyhjb2RlKSlyZXR1cm4gZmFsc2U7CiAgd2F0Y2hCdXN5LmFkZChjb2RlKTtyZW5kZXJXYXRjaCgpOwogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSkscz1xLnNuYXBzaG90fHx7fTsKICAgIGxldCBmPXtzY29yZTpudWxsLGZyZXNobmVzczp7ZmFjdG9yOjB9fSxwPXtzY29yZTpudWxsLG1hdGNoZWRfdGhlbWVzOltdfTsKICAgIHRyeXtmPWF3YWl0IGdldEZ1bmRhbWVudGFscyhjb2RlKX1jYXRjaChlKXt9CiAgICB0cnl7cD1hd2FpdCBnZXRQb2xpY3koY29kZSl9Y2F0Y2goZSl7fQogICAgY29uc3QgcmVzcG9uc2U9YXdhaXQgZmV0Y2goJy9hcGkvZnJlZS9hbmFseXplJyx7bWV0aG9kOidQT1NUJyxoZWFkZXJzOnsnQ29udGVudC1UeXBlJzonYXBwbGljYXRpb24vanNvbid9LGNhY2hlOiduby1zdG9yZScsYm9keTpKU09OLnN0cmluZ2lmeSh7CiAgICAgIGNvZGUscHJpY2U6cy5sYXN0X2Nsb3NlLHJldHVybjIwOnMucmV0dXJuXzIwZCxyZXR1cm4xMjY6cy5yZXR1cm5fMTI2ZCxyZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCwKICAgICAgZWFybmluZ3Nfc2NvcmU6Zi5zY29yZSxwb2xpY3lfc2NvcmU6cC5zY29yZSxwb2xpY3lfbW9kZTonYXV0bycsc3VwcGx5X3Njb3JlOihzLnN1cHBseV9wcm94eXx8e30pLnNjb3JlLAogICAgICBtYXJrZXRfZnJlc2huZXNzOm1hcmtldEZyZXNobmVzc0ZhY3RvcihzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSksCiAgICAgIGVhcm5pbmdzX2ZyZXNobmVzczpmLmZyZXNobmVzcyYmZi5mcmVzaG5lc3MuZmFjdG9yIT09dW5kZWZpbmVkP2YuZnJlc2huZXNzLmZhY3RvcjooZi5zY29yZT09bnVsbD8wOjEpLAogICAgICBwb2xpY3lfZnJlc2huZXNzOnBvbGljeUZyZXNobmVzc0ZhY3RvcihwLGZhbHNlKQogICAgfSl9KTsKICAgIGNvbnN0IHJlc3VsdD1hd2FpdCByZXNwb25zZS5qc29uKCk7aWYoIXJlc3BvbnNlLm9rfHxyZXN1bHQuc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IocmVzdWx0LnJlYXNvbnx8cmVzdWx0LmVycm9yfHwn5YiG5p6Q44Gr5aSx5pWX44GX44G+44GX44GfJyk7CiAgICBjb25zdCBsaXN0PWxvY2FsKCdmcmVlX3dhdGNoJyk7CiAgICBjb25zdCBpbmRleD1saXN0LmZpbmRJbmRleCh4PT5TdHJpbmcodHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSk9PT1jb2RlKTsKICAgIGlmKGluZGV4PDApcmV0dXJuIGZhbHNlOwogICAgbGV0IGNhY2hlPXt9O3RyeXtjYWNoZT1KU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKCdmcmVlX3dhdGNoX2FuYWx5c2lzX3YxJyl8fCd7fScpfWNhdGNoKGUpe30KICAgIGNhY2hlW2NvZGVdPXtzbmFwc2hvdDpzLHNvdXJjZTpxLnNvdXJjZSxzY29yZTpyZXN1bHQuc2NvcmUsc2lnbmFsOnJlc3VsdC5zaWduYWwsdXBkYXRlZF9hdDpuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCl9OwogICAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oJ2ZyZWVfd2F0Y2hfYW5hbHlzaXNfdjEnLEpTT04uc3RyaW5naWZ5KGNhY2hlKSk7CiAgICBjb25zdCBuYW1lPShxLmNvbXBhbnl8fHt9KS5uYW1lfHwodHlwZW9mIGxpc3RbaW5kZXhdPT09J3N0cmluZyc/Jyc6bGlzdFtpbmRleF0ubmFtZSl8fCcnOwogICAgbGlzdFtpbmRleF09ey4uLih0eXBlb2YgbGlzdFtpbmRleF09PT0nb2JqZWN0Jz9saXN0W2luZGV4XTp7fSksY29kZSxuYW1lfTtzYXZlKCdmcmVlX3dhdGNoJyxsaXN0KTsKICAgIHJldHVybiB0cnVlOwogIH1jYXRjaChlKXtpZighYnVsaylhbGVydCgn44Km44Kp44OD44OB5YiG5p6Q44Ko44Op44O877yaJytlLm1lc3NhZ2UrJ+OAguS/neWtmOa4iOOBv+e1kOaenOOBjOOBguOCjOOBsOWJjeWbnuWIhuOCkuihqOekuuOBl+OBvuOBmeOAgicpO3JldHVybiBmYWxzZTt9CiAgZmluYWxseXt3YXRjaEJ1c3kuZGVsZXRlKGNvZGUpO3JlbmRlcldhdGNoKCl9Cn0KYXN5bmMgZnVuY3Rpb24gcmVmcmVzaEFsbFdhdGNoKCl7CiAgaWYobW92ZW1lbnRCdXN5fHx3YXRjaEJ1bGtCdXN5fHx3YXRjaEJ1c3kuc2l6ZSlyZXR1cm47CiAgY29uc3QgY29kZXM9Wy4uLm5ldyBTZXQobG9jYWwoJ2ZyZWVfd2F0Y2gnKS5tYXAoeD0+U3RyaW5nKHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpKSldOwogIGNvbnN0IHN0YXR1cz0kKCd3YXRjaEJ1bGtTdGF0dXMnKTsKICBpZighY29kZXMubGVuZ3RoKXtzdGF0dXMudGV4dENvbnRlbnQ9J+OCpuOCqeODg+ODgeODquOCueODiOOBr+acqueZu+mMsuOBp+OBmeOAgic7cmV0dXJuO30KICB3YXRjaEJ1bGtCdXN5PXRydWU7cmVuZGVyV2F0Y2goKTsKICBsZXQgZG9uZT0wLGZhaWxlZD0wLHNraXBwZWQ9MDsKICB0cnl7CiAgICBmb3IobGV0IG49MDtuPGNvZGVzLmxlbmd0aDtuKyspewogICAgICBjb25zdCBjb2RlPWNvZGVzW25dOwogICAgICBjb25zdCBpbmRleD1sb2NhbCgnZnJlZV93YXRjaCcpLmZpbmRJbmRleCh4PT5TdHJpbmcodHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSk9PT1jb2RlKTsKICAgICAgaWYoaW5kZXg8MCl7c2tpcHBlZCsrO2NvbnRpbnVlO30KICAgICAgc3RhdHVzLnRleHRDb250ZW50PWAke24rMX0gLyAke2NvZGVzLmxlbmd0aH3pipjmn4TvvJoke2NvZGV9IOOCkuWIhuaekOS4reKApmA7CiAgICAgIGlmKGF3YWl0IHJlZnJlc2hXYXRjaChpbmRleCx0cnVlKSlkb25lKys7ZWxzZSBmYWlsZWQrKzsKICAgICAgaWYobjxjb2Rlcy5sZW5ndGgtMSl7CiAgICAgICAgc3RhdHVzLnRleHRDb250ZW50PWAke24rMX0gLyAke2NvZGVzLmxlbmd0aH3pipjmn4TjgpLlh6bnkIbmuIjjgb/jgILmrKHjga7liIbmnpDjgb7jgafntIQxM+enkuKApu+8iOaIkOWKnyAke2RvbmV9IC8g5aSx5pWXICR7ZmFpbGVkfe+8iWA7CiAgICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZT0+c2V0VGltZW91dChyZXNvbHZlLDEzMDAwKSk7CiAgICAgIH0KICAgIH0KICAgIHN0YXR1cy50ZXh0Q29udGVudD1g5LiA5ous5pu05paw5a6M5LqG77ya5oiQ5YqfICR7ZG9uZX0gLyDlpLHmlZcgJHtmYWlsZWR9IC8g5YmK6Zmk5riI44G/ICR7c2tpcHBlZH3jgILlpLHmlZfjgZfjgZ/pipjmn4Tjga/liY3lm57jga7ntZDmnpzjgYzjgYLjgozjgbDooajnpLrjgZfjgb7jgZnjgILpgJTkuK3jgafov73liqDjgZfjgZ/pipjmn4Tjga/mrKHlm57jga7lr77osaHjgafjgZnjgIJgOwogIH1jYXRjaChlKXtzdGF0dXMudGV4dENvbnRlbnQ9J+S4gOaLrOabtOaWsOOCkuS4reaWreOBl+OBvuOBl+OBn++8micrZS5tZXNzYWdlO30KICBmaW5hbGx5e3dhdGNoQnVsa0J1c3k9ZmFsc2U7cmVuZGVyV2F0Y2goKTt9Cn0KbGV0IG1vdmVtZW50U3RvcFJlcXVlc3RlZD1mYWxzZSxtb3ZlbWVudFdhaXRUaW1lcj1udWxsLG1vdmVtZW50V2FpdFJlc29sdmU9bnVsbDsKY29uc3QgTUFSS0VUX0dFTlJFUz17IlNlbWljb25kdWN0b3JzIjogIuWNiuWwjuS9k+ODu+mbu+WtkOapn+WZqCIsICJJbmR1c3RyaWFsIEVsZWN0cm9uaWNzIjogIuWNiuWwjuS9k+ODu+mbu+WtkOapn+WZqCIsICJBdWRpby9WaWRlbyBFcXVpcG1lbnQiOiAi5Y2K5bCO5L2T44O76Zu75a2Q5qmf5ZmoIiwgIkNvbXB1dGVycy9Db25zdW1lciBFbGVjdHJvbmljcyI6ICLljYrlsI7kvZPjg7vpm7vlrZDmqZ/lmagiLCAiTmV0d29ya2luZyI6ICLljYrlsI7kvZPjg7vpm7vlrZDmqZ/lmagiLCAiUHJlY2lzaW9uIFByb2R1Y3RzIjogIuWNiuWwjuS9k+ODu+mbu+WtkOapn+WZqCIsICJXYXRjaGVzL0Nsb2Nrcy9QYXJ0cyI6ICLljYrlsI7kvZPjg7vpm7vlrZDmqZ/lmagiLCAiQ29tcHV0ZXIgU2VydmljZXMiOiAiSVTjg7vpgJrkv6EiLCAiU29mdHdhcmUiOiAiSVTjg7vpgJrkv6EiLCAiSW50ZXJuZXQvT25saW5lIjogIklU44O76YCa5L+hIiwgIldpcmVkIFRlbGVjb21tdW5pY2F0aW9ucyBTZXJ2aWNlcyI6ICJJVOODu+mAmuS/oSIsICJXaXJlbGVzcyBUZWxlY29tbXVuaWNhdGlvbnMgU2VydmljZXMiOiAiSVTjg7vpgJrkv6EiLCAiQXV0byAmIENvbW1lcmNpYWwgVmVoaWNsZSBQYXJ0cyI6ICLoh6rli5Xou4rjg7vovLjpgIHmqZ/lmagiLCAiQXV0b21vYmlsZXMiOiAi6Ieq5YuV6LuK44O76Ly46YCB5qmf5ZmoIiwgIkNvbW1lcmNpYWwgVmVoaWNsZXMiOiAi6Ieq5YuV6LuK44O76Ly46YCB5qmf5ZmoIiwgIlRpcmVzIjogIuiHquWLlei7iuODu+i8uOmAgeapn+WZqCIsICJBZXJvc3BhY2UgUHJvZHVjdHMvUGFydHMiOiAi6Ieq5YuV6LuK44O76Ly46YCB5qmf5ZmoIiwgIkRlZmVuc2UgRXF1aXBtZW50L1Byb2R1Y3RzIjogIuiHquWLlei7iuODu+i8uOmAgeapn+WZqCIsICJJbmR1c3RyaWFsIE1hY2hpbmVyeSI6ICLmqZ/morDjg7vnlKPmpa3oqK3lgpkiLCAiSW5kdXN0cmlhbCBQcm9kdWN0cyI6ICLmqZ/morDjg7vnlKPmpa3oqK3lgpkiLCAiTW9iaWxlIE1hY2hpbmVyeSI6ICLmqZ/morDjg7vnlKPmpa3oqK3lgpkiLCAiRWxlY3RyaWMgVXRpbGl0aWVzIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJHYXMgVXRpbGl0aWVzIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJNdWx0aXV0aWxpdGllcyI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiUmVuZXdhYmxlIEVuZXJneSBHZW5lcmF0aW9uIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJNYWpvciBPaWwgJiBHYXMiOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIk9pbCAmIEdhcyBQcm9kdWN0cy9TZXJ2aWNlcyI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiT2lsIEV4dHJhY3Rpb24iOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIkNvYWwiOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIkFsdW1pbnVtIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJDb21tb2RpdHkgQ2hlbWljYWxzIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJTcGVjaWFsdHkgQ2hlbWljYWxzIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJOb24tRmVycm91cyBNZXRhbHMiOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIklyb24vU3RlZWwiOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIkdlbmVyYWwgTWluaW5nIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJQcmVjaW91cyBNZXRhbHMiOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIlBhcGVyL1B1bHAiOiAi57Sg5p2Q44O75YyW5a2m44O76YeR5bGeIiwgIkNvbnRhaW5lcnMvUGFja2FnaW5nIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJCaW90ZWNobm9sb2d5IjogIuWMu+iWrOWTgeODu+WMu+eZgiIsICJQaGFybWFjZXV0aWNhbHMiOiAi5Yy76Jas5ZOB44O75Yy755mCIiwgIk1lZGljYWwgRXF1aXBtZW50L1N1cHBsaWVzIjogIuWMu+iWrOWTgeODu+WMu+eZgiIsICJIZWFsdGhjYXJlIFByb3Zpc2lvbiI6ICLljLvolqzlk4Hjg7vljLvnmYIiLCAiRHJ1ZyBSZXRhaWwiOiAi5Yy76Jas5ZOB44O75Yy755mCIiwgIkJhbmtpbmciOiAi6YeR6J6N44O75L+d6Zm6IiwgIk1ham9yIEludGVybmF0aW9uYWwgQmFua3MiOiAi6YeR6J6N44O75L+d6Zm6IiwgIkNvbnN1bWVyIEZpbmFuY2UiOiAi6YeR6J6N44O75L+d6Zm6IiwgIkZpbmFuY2UgQ29tcGFuaWVzIjogIumHkeiejeODu+S/nemZuiIsICJGdWxsLUxpbmUgSW5zdXJhbmNlIjogIumHkeiejeODu+S/nemZuiIsICJMaWZlIEluc3VyYW5jZSI6ICLph5Hono3jg7vkv53pmboiLCAiTm9uLUxpZmUgSW5zdXJhbmNlIjogIumHkeiejeODu+S/nemZuiIsICJJbnZlc3RtZW50IEFkdmlzb3JzIjogIumHkeiejeODu+S/nemZuiIsICJNb3J0Z2FnZXMiOiAi6YeR6J6N44O75L+d6Zm6IiwgIlNlY3VyaXRpZXMiOiAi6YeR6J6N44O75L+d6Zm6IiwgIkNvbnN0cnVjdGlvbiI6ICLlu7roqK3jg7vkuI3li5XnlKMiLCAiUmVzaWRlbnRpYWwgQnVpbGRpbmcgQ29uc3RydWN0aW9uIjogIuW7uuioreODu+S4jeWLleeUoyIsICJCdWlsZGluZyBNYXRlcmlhbHMvUHJvZHVjdHMiOiAi5bu66Kit44O75LiN5YuV55SjIiwgIlJlYWwgRXN0YXRlIEFnZW50cy9Ccm9rZXJzIjogIuW7uuioreODu+S4jeWLleeUoyIsICJSZWFsIEVzdGF0ZSBEZXZlbG9wZXJzIjogIuW7uuioreODu+S4jeWLleeUoyIsICJGb29kIFByb2R1Y3RzIjogIumjn+WTgeODu+i+suael+awtOeUoyIsICJGYXJtaW5nIjogIumjn+WTgeODu+i+suael+awtOeUoyIsICJGaXNoaW5nIjogIumjn+WTgeODu+i+suael+awtOeUoyIsICJBbGNvaG9saWMgQmV2ZXJhZ2VzL0RyaW5rcyI6ICLpo5/lk4Hjg7vovrLmnpfmsLTnlKMiLCAiTm9uLUFsY29ob2xpYyBCZXZlcmFnZXMvRHJpbmtzIjogIumjn+WTgeODu+i+suael+awtOeUoyIsICJUb2JhY2NvIjogIumjn+WTgeODu+i+suael+awtOeUoyIsICJDbG90aGluZyBSZXRhaWwiOiAi5bCP5aOy44O75aSW6aOfIiwgIkZvb2QgUmV0YWlsIjogIuWwj+WjsuODu+WklumjnyIsICJIb21lIEdvb2RzIFJldGFpbCI6ICLlsI/lo7Ljg7vlpJbpo58iLCAiTWl4ZWQgUmV0YWlsaW5nIjogIuWwj+WjsuODu+WklumjnyIsICJSZXN0YXVyYW50cyI6ICLlsI/lo7Ljg7vlpJbpo58iLCAiU3BlY2lhbHR5IFJldGFpbCI6ICLlsI/lo7Ljg7vlpJbpo58iLCAiQ2xvdGhpbmciOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIkZvb3R3ZWFyIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJGdXJuaXR1cmUiOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIkhvdXNld2FyZXMiOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIk5vbmR1cmFibGUgSG91c2Vob2xkIFByb2R1Y3RzIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJQZXJzb25hbCBDYXJlIFByb2R1Y3RzL0FwcGxpYW5jZXMiOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIlNwb3J0cyBHb29kcyI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiTGVpc3VyZSBHb29kcyI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiQWlyIEZyZWlnaHQiOiAi6YGL6Ly444O754mp5rWBIiwgIlBhc3NlbmdlciBBaXJsaW5lcyI6ICLpgYvovLjjg7vnianmtYEiLCAiUGFzc2VuZ2VyIFRyYW5zcG9ydCwgT3RoZXIiOiAi6YGL6Ly444O754mp5rWBIiwgIlJhaWxyb2FkcyI6ICLpgYvovLjjg7vnianmtYEiLCAiVHJhbnNwb3J0YXRpb24gU2VydmljZXMiOiAi6YGL6Ly444O754mp5rWBIiwgIlRydWNraW5nIjogIumBi+i8uOODu+eJqea1gSIsICJXYXRlciBUcmFuc3BvcnQvU2hpcHBpbmciOiAi6YGL6Ly444O754mp5rWBIiwgIkJyb2FkY2FzdGluZyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiR2FtYmxpbmcgSW5kdXN0cmllcyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiSG90ZWxzIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJNb3Rpb24gUGljdHVyZS9Tb3VuZCBSZWNvcmRpbmciOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIlByaW50aW5nIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJQdWJsaXNoaW5nIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJSZWNyZWF0aW9uYWwgU2VydmljZXMiOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIlRvdXJpc20iOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIlRveXMgJiBHYW1lcyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiV2hvbGVzYWxlcnMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkFjY291bnRpbmciOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkFkdmVydGlzaW5nL01hcmtldGluZy9QdWJsaWMgUmVsYXRpb25zIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJDb25zdW1lciBTZXJ2aWNlcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiRGl2ZXJzaWZpZWQgQnVzaW5lc3MgU2VydmljZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkRpdmVyc2lmaWVkIEhvbGRpbmcgQ29tcGFuaWVzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJFbXBsb3ltZW50L1RyYWluaW5nIFNlcnZpY2VzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJFbnZpcm9ubWVudC9XYXN0ZSBNYW5hZ2VtZW50IjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJHZW5lcmFsIFNlcnZpY2VzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJUZWNobmljYWwgU2VydmljZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIldhdGVyIFV0aWxpdGllcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiU2hlbGwgY29tcGFuaWVzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJDbG9zZWQtRW5kIEZ1bmRzIjogIkVURuODu+ODleOCoeODs+ODiSIsICJFeGNoYW5nZS1UcmFkZWQgRnVuZHMiOiAiRVRG44O744OV44Kh44Oz44OJIiwgIk11dHVhbCAmIE90aGVyIEZ1bmRzIjogIkVURuODu+ODleOCoeODs+ODiSJ9OwpmdW5jdGlvbiBzdG9ja0dlbnJlKHN0b2NrKXtyZXR1cm4gTUFSS0VUX0dFTlJFU1tzdG9jay5zZWN0b3JdfHwn44Gd44Gu5LuW44O75qWt56iu5LiN5piOJ30KZnVuY3Rpb24gc2VsZWN0ZWRNYXJrZXRHZW5yZSgpe3JldHVybiBsb2NhbFN0b3JhZ2UuZ2V0SXRlbSgnZnJlZV9tYXJrZXRfZ2VucmVfdjEnKXx8J+WFqOalreeorid9CmZ1bmN0aW9uIGFsbE1hcmtldFN0b2Nrcygpe3JldHVybiBsb2NhbCgnZnJlZV9tYXJrZXRfdW5pdmVyc2VfdjEnKX0KZnVuY3Rpb24gcmVuZGVyTWFya2V0R2VucmVzKCl7CiAgY29uc3Qgc2VsZWN0PSQoJ21hcmtldEdlbnJlJyk7aWYoIXNlbGVjdClyZXR1cm47CiAgY29uc3Qgc2VsZWN0ZWQ9c2VsZWN0ZWRNYXJrZXRHZW5yZSgpLGNvdW50cz1uZXcgTWFwKCk7CiAgZm9yKGNvbnN0IHN0b2NrIG9mIGFsbE1hcmtldFN0b2NrcygpKXtjb25zdCBnZW5yZT1zdG9ja0dlbnJlKHN0b2NrKTtjb3VudHMuc2V0KGdlbnJlLChjb3VudHMuZ2V0KGdlbnJlKXx8MCkrMSl9CiAgc2VsZWN0LmlubmVySFRNTD0nPG9wdGlvbiB2YWx1ZT0i5YWo5qWt56iuIj7lhajmpa3nqK7vvIgnK2FsbE1hcmtldFN0b2NrcygpLmxlbmd0aCsn6YqY5p+E77yJPC9vcHRpb24+JytbLi4uY291bnRzLmtleXMoKV0uc29ydCgoYSxiKT0+YS5sb2NhbGVDb21wYXJlKGIsJ2phJykpLm1hcChnZW5yZT0+YDxvcHRpb24gdmFsdWU9IiR7d2F0Y2hFc2NhcGUoZ2VucmUpfSI+JHt3YXRjaEVzY2FwZShnZW5yZSl977yIJHtjb3VudHMuZ2V0KGdlbnJlKX3pipjmn4TvvIk8L29wdGlvbj5gKS5qb2luKCcnKTsKICBpZihzZWxlY3RlZCE9PSflhajmpa3nqK4nJiYhY291bnRzLmhhcyhzZWxlY3RlZCkpbG9jYWxTdG9yYWdlLnNldEl0ZW0oJ2ZyZWVfbWFya2V0X2dlbnJlX3YxJywn5YWo5qWt56iuJyk7CiAgc2VsZWN0LnZhbHVlPXNlbGVjdGVkTWFya2V0R2VucmUoKTtzZWxlY3QuZGlzYWJsZWQ9bW92ZW1lbnRCdXN5fHwhYWxsTWFya2V0U3RvY2tzKCkubGVuZ3RoOwogICQoJ21hcmtldEdlbnJlTG9hZCcpLmRpc2FibGVkPW1vdmVtZW50QnVzeTsKfQpmdW5jdGlvbiBjaGFuZ2VNYXJrZXRHZW5yZSgpewogIGlmKG1vdmVtZW50QnVzeSlyZXR1cm47CiAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oJ2ZyZWVfbWFya2V0X2dlbnJlX3YxJywkKCdtYXJrZXRHZW5yZScpLnZhbHVlKTsKICAkKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PXNlbGVjdGVkTWFya2V0R2VucmUoKSsn44Gr5YiH44KK5pu/44GI44G+44GX44Gf44CC5YCZ6KOc44Go5beh5Zue5a++6LGh44KS44GT44Gu44K444Oj44Oz44Or44Gr57We44KK44G+44GZ44CCJzsKICByZW5kZXJNb3ZlbWVudCgpOwp9CmFzeW5jIGZ1bmN0aW9uIGxvYWRNYXJrZXRHZW5yZXMoKXsKICBjb25zdCBidG49JCgnbWFya2V0R2VucmVMb2FkJyk7YnRuLmRpc2FibGVkPXRydWU7CiAgdHJ5e2F3YWl0IGVuc3VyZU1hcmtldFVuaXZlcnNlKCk7cmVuZGVyTW92ZW1lbnQoKTskKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PSfjgrjjg6Pjg7Pjg6vkuIDopqfjgpLoqq3jgb/ovrzjgb/jgb7jgZfjgZ/jgILoqr/jgbnjgZ/jgYTjgrjjg6Pjg7Pjg6vjgpLpgbjjgpPjgafjga3jgIInfQogIGNhdGNoKGUpeyQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9ZS5tZXNzYWdlfQogIGZpbmFsbHl7YnRuLmRpc2FibGVkPW1vdmVtZW50QnVzeTt9Cn0KZnVuY3Rpb24gbWFya2V0Q3Vyc29yS2V5KCl7cmV0dXJuICdmcmVlX21hcmtldF9jdXJzb3JfZ2VucmVfdjE6JytzZWxlY3RlZE1hcmtldEdlbnJlKCl9CmZ1bmN0aW9uIHJlZ2lzdGVyZWRNb3ZlbWVudFN0b2Nrcygpe2NvbnN0IGdlbnJlPXNlbGVjdGVkTWFya2V0R2VucmUoKTtyZXR1cm4gYWxsTWFya2V0U3RvY2tzKCkuZmlsdGVyKHN0b2NrPT5nZW5yZT09PSflhajmpa3nqK4nfHxzdG9ja0dlbnJlKHN0b2NrKT09PWdlbnJlKX0KYXN5bmMgZnVuY3Rpb24gZW5zdXJlTWFya2V0VW5pdmVyc2UoKXsKICBpZihhbGxNYXJrZXRTdG9ja3MoKS5sZW5ndGgpcmV0dXJuOwogIGNvbnN0IHJlc3BvbnNlPWF3YWl0IGZldGNoKCcvYXBpL2ZyZWUvbWFya2V0LXVuaXZlcnNlJyx7Y2FjaGU6J25vLXN0b3JlJ30pLHJlc3VsdD1hd2FpdCByZXNwb25zZS5qc29uKCk7CiAgaWYoIXJlc3BvbnNlLm9rfHxyZXN1bHQuc3RhdHVzIT09J29rJ3x8IUFycmF5LmlzQXJyYXkocmVzdWx0LnN0b2Nrcyl8fCFyZXN1bHQuc3RvY2tzLmxlbmd0aCl0aHJvdyBuZXcgRXJyb3IocmVzdWx0LnJlYXNvbnx8J+mKmOafhOS4gOimp+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHNhdmUoJ2ZyZWVfbWFya2V0X3VuaXZlcnNlX3YxJyxyZXN1bHQuc3RvY2tzKTsKICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgnZnJlZV9tYXJrZXRfdW5pdmVyc2VfZmV0Y2hlZCcscmVzdWx0LmZldGNoZWRfYXQpOwp9CmZ1bmN0aW9uIHN0b3BNb3ZlbWVudCgpewogIG1vdmVtZW50U3RvcFJlcXVlc3RlZD10cnVlOwogIGlmKG1vdmVtZW50V2FpdFRpbWVyIT09bnVsbCljbGVhclRpbWVvdXQobW92ZW1lbnRXYWl0VGltZXIpOwogIGlmKG1vdmVtZW50V2FpdFJlc29sdmUpe21vdmVtZW50V2FpdFJlc29sdmUoKTttb3ZlbWVudFdhaXRSZXNvbHZlPW51bGw7fQogICQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9J+WBnOatouS4reKApuWPluW+l+S4reOBrjHpipjmn4TjgYzlrozkuobjgZfjgZ/jgonmraLjgb7jgorjgb7jgZnjgIInOwp9CmZ1bmN0aW9uIG1vdmVtZW50V2FpdCgpe3JldHVybiBuZXcgUHJvbWlzZShyZXNvbHZlPT57bW92ZW1lbnRXYWl0UmVzb2x2ZT1yZXNvbHZlO21vdmVtZW50V2FpdFRpbWVyPXNldFRpbWVvdXQoKCk9Pnttb3ZlbWVudFdhaXRUaW1lcj1udWxsO21vdmVtZW50V2FpdFJlc29sdmU9bnVsbDtyZXNvbHZlKCl9LDEzMDAwKX0pfQpmdW5jdGlvbiBjb21wYWN0TW92ZW1lbnRRdW90ZShxKXsKICBjb25zdCBzPXEuc25hcHNob3R8fHt9OwogIGNvbnN0IGZpZWxkcz1bJ2xhc3RfZGF0ZScsJ2xhc3RfY2xvc2UnLCdyZXR1cm5fMjBkJywncHJpY2Vfc291cmNlJywnaGlzdG9yeV9hZGp1c3RtZW50J107CiAgY29uc3Qgc25hcHNob3Q9T2JqZWN0LmZyb21FbnRyaWVzKGZpZWxkcy5tYXAoaz0+W2ssc1trXT8/bnVsbF0pKTsKICBzbmFwc2hvdC5zdXBwbHlfcHJveHk9e3ZvbHVtZV9yYXRpb181XzIwOihzLnN1cHBseV9wcm94eXx8e30pLnZvbHVtZV9yYXRpb181XzIwPz9udWxsfTsKICByZXR1cm4ge3NvdXJjZTpxLnNvdXJjZXx8JycsY29tcGFueTp7bmFtZToocS5jb21wYW55fHx7fSkubmFtZXx8Jyd9LHNuYXBzaG90LHJvd3M6KHEucm93c3x8W10pLnNsaWNlKC02KS5tYXAocj0+KHtkYXRlOnIuZGF0ZSxjbG9zZTpyLmNsb3NlfSkpfTsKfQpmdW5jdGlvbiByZWFkTW92ZW1lbnQoKXt0cnl7cmV0dXJuIEpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfbW92ZW1lbnRfdjEnKXx8J3t9Jyl9Y2F0Y2goZSl7cmV0dXJuIHt9fX0KZnVuY3Rpb24gbW92ZW1lbnRNZXRyaWNzKHEpewogIGNvbnN0IHM9KHF8fHt9KS5zbmFwc2hvdHx8e307CiAgY29uc3Qgcm93cz0oKHF8fHt9KS5yb3dzfHxbXSkuZmlsdGVyKHI9PnR5cGVvZiByLmNsb3NlPT09J251bWJlcicmJk51bWJlci5pc0Zpbml0ZShyLmNsb3NlKSYmci5jbG9zZT4wJiZyLmRhdGUpLnNvcnQoKGEsYik9PlN0cmluZyhhLmRhdGUpLmxvY2FsZUNvbXBhcmUoU3RyaW5nKGIuZGF0ZSkpKTsKICBjb25zdCBsYXN0PXJvd3MuYXQoLTEpLGRhdGU9bGFzdCYmbGFzdC5kYXRlOwogIGNvbnN0IHJldD1uPT5yb3dzLmxlbmd0aD5uPyhyb3dzLmF0KC0xKS5jbG9zZS9yb3dzLmF0KC0xLW4pLmNsb3NlLTEpKjEwMDpudWxsOwogIGNvbnN0IGRheT1yZXQoMSksZml2ZT1yZXQoNSksdHdlbnR5PXR5cGVvZiBzLnJldHVybl8yMGQ9PT0nbnVtYmVyJyYmTnVtYmVyLmlzRmluaXRlKHMucmV0dXJuXzIwZCk/cy5yZXR1cm5fMjBkOnJldCgyMCk7CiAgY29uc3Qgdm9sdW1lPShzLnN1cHBseV9wcm94eXx8e30pLnZvbHVtZV9yYXRpb181XzIwOwogIGxldCBraW5kPSdvdGhlcicscmVhc29uPSfms6jnm67jg7vkuIvokL3orabmiJLjga7mnaHku7bjgavoqbLlvZPjgZfjgb7jgZvjgpPjgIInOwogIGlmKCFkYXRlfHxkYXRhQWdlRGF5cyhkYXRlKT09PW51bGx8fGRhdGFBZ2VEYXlzKGRhdGUpPjR8fHMubGFzdF9kYXRlIT09ZGF0ZSl7cmVhc29uPSflsaXmrbTjgYzlj6TjgYTjg7vkuI3otrPjgIHjgb7jgZ/jga/nj77lnKjlgKTjgajlsaXmrbTjga7ml6Xku5jjgYzkuI3kuIDoh7TjgILliKTlrprkv53nlZnjgafjgZnjgIInO30KICBlbHNlIGlmKGRheT09PW51bGx8fGZpdmU9PT1udWxsKXtyZWFzb249JzHllrbmpa3ml6Xjg7s15Za25qWt5pel44Gu5q+U6LyD44Gr5b+F6KaB44Gq5bGl5q2044GM5LiN6Laz44CC5Yik5a6a5L+d55WZ44Gn44GZ44CCJzt9CiAgZWxzZSBpZihkYXk+PTEmJmZpdmU+MCl7a2luZD0ndXAnO3JlYXNvbj0n55u06L+RMeWWtualreaXpe+8izEl5Lul5LiK44CBNeWWtualreaXpeOCguODl+ODqeOCueOAguS4iuaYh+OBruWLleOBjeOBjOe2muOBhOOBpuOBhOOCi+WPguiAg+WAmeijnOOAgic7fQogIGVsc2UgaWYoZGF5PD0tMSYmKGZpdmU8MHx8dHdlbnR5IT09bnVsbCYmdHdlbnR5PDApKXtraW5kPSdkb3duJztyZWFzb249J+ebtOi/kTHllrbmpa3ml6XiiJIxJeS7peS4i+OAgTXllrbmpa3ml6Xjgb7jgZ/jga8yMOWWtualreaXpeOCguODnuOCpOODiuOCueOAguS4i+WQkeOBjeOBruWLleOBjeOBq+azqOaEj+OAgic7fQogIHJldHVybiB7cm93cyxkYXRlLGRheSxmaXZlLHR3ZW50eSx2b2x1bWUsa2luZCxyZWFzb259Owp9CmZ1bmN0aW9uIG1vdmVtZW50Q2hhcnQocm93cyl7CiAgaWYocm93cy5sZW5ndGg8MilyZXR1cm4gJzxwIGNsYXNzPSJtdXRlZCI+44OB44Oj44O844OI55So44Gu5bGl5q205LiN6LazPC9wPic7CiAgY29uc3QgdmFsdWVzPXJvd3MubWFwKHI9PnIuY2xvc2UpLGxvdz1NYXRoLm1pbiguLi52YWx1ZXMpLGhpZ2g9TWF0aC5tYXgoLi4udmFsdWVzKTsKICBjb25zdCBwb2ludHM9dmFsdWVzLm1hcCgodixpKT0+YCR7KDgraSoyODQvKHZhbHVlcy5sZW5ndGgtMSkpLnRvRml4ZWQoMSl9LCR7KGhpZ2g9PT1sb3c/NDA6NzItKHYtbG93KSo2NC8oaGlnaC1sb3cpKS50b0ZpeGVkKDEpfWApLmpvaW4oJyAnKTsKICBjb25zdCBjb2xvcj12YWx1ZXMuYXQoLTEpPj12YWx1ZXNbMF0/JyMxNTgwM2QnOicjYjkxYzFjJzsKICByZXR1cm4gYDxzdmcgdmlld0JveD0iMCAwIDMwMCA4MCIgcm9sZT0iaW1nIiBhcmlhLWxhYmVsPSLnm7Tov5HmnIDlpKc25Za25qWt5pel44Gu57WC5YCk5o6o56e7IiBzdHlsZT0id2lkdGg6MTAwJTtoZWlnaHQ6MTAwcHgiPjxwb2x5bGluZSBwb2ludHM9IiR7cG9pbnRzfSIgZmlsbD0ibm9uZSIgc3Ryb2tlPSIke2NvbG9yfSIgc3Ryb2tlLXdpZHRoPSIyLjUiLz48L3N2Zz48ZGl2IGNsYXNzPSJtdXRlZCI+JHt3YXRjaEVzY2FwZShyb3dzWzBdLmRhdGUpfSDihpIgJHt3YXRjaEVzY2FwZShyb3dzLmF0KC0xKS5kYXRlKX0gLyDmnIDlrokgJHt5ZW4obG93KX3jg7vmnIDpq5ggJHt5ZW4oaGlnaCl9PC9kaXY+YDsKfQpmdW5jdGlvbiByZW5kZXJNb3ZlbWVudCgpewogIHJlbmRlck1hcmtldEdlbnJlcygpOwogIGNvbnN0IGNhY2hlPXJlYWRNb3ZlbWVudCgpLG9wZW5lZD1uZXcgU2V0KEFycmF5LmZyb20oZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlsc1tkYXRhLW1vdmVtZW50XVtvcGVuXScpKS5tYXAoZWw9PmVsLmRhdGFzZXQubW92ZW1lbnQpKTsKICBjb25zdCB1bml2ZXJzZT1yZWdpc3RlcmVkTW92ZW1lbnRTdG9ja3MoKTsKICBjb25zdCBjaGVja2VkPXVuaXZlcnNlLmZpbHRlcih4PT5jYWNoZVt4LmNvZGVdKTsKICBjb25zdCBmcmVzaD1jaGVja2VkLmZpbHRlcih4PT4hY2FjaGVbeC5jb2RlXS5lcnJvciYmZGF0YUFnZURheXMoKChjYWNoZVt4LmNvZGVdLnF8fHt9KS5zbmFwc2hvdHx8e30pLmxhc3RfZGF0ZSkhPT1udWxsJiZkYXRhQWdlRGF5cygoKGNhY2hlW3guY29kZV0ucXx8e30pLnNuYXBzaG90fHx7fSkubGFzdF9kYXRlKTw9NCkubGVuZ3RoOwogIGNvbnN0IGNvdmVyYWdlPSQoJ21vdmVtZW50Q292ZXJhZ2UnKTtpZihjb3ZlcmFnZSljb3ZlcmFnZS50ZXh0Q29udGVudD1gJHtzZWxlY3RlZE1hcmtldEdlbnJlKCl977ya5LiA6KanICR7dW5pdmVyc2UubGVuZ3RofemKmOafhCAvIOWPluW+l+ippuihjOa4iOOBvyAke2NoZWNrZWQubGVuZ3RofSAvIDTml6Xku6XlhoXjga7jg4fjg7zjgr8gJHtmcmVzaH0gLyDmnKroqr/mn7sgJHt1bml2ZXJzZS5sZW5ndGgtY2hlY2tlZC5sZW5ndGh944CC5LiA6Kan44Gr44Gv5Y+k44GE55m76Yyy44KE5Y+W5b6X5LiN5Y+v44Gu6YqY5p+E44GM5ZCr44G+44KM44KL5aC05ZCI44GM44GC44KK44G+44GZ44CCYDsKICBjb25zdCBncm91cHM9e3VwOltdLGRvd246W10sb3RoZXI6W119OwogIGZvcihjb25zdCBzdG9jayBvZiByZWdpc3RlcmVkTW92ZW1lbnRTdG9ja3MoKSl7CiAgICBjb25zdCBhPWNhY2hlW3N0b2NrLmNvZGVdO2lmKCFhKWNvbnRpbnVlO2NvbnN0IG09bW92ZW1lbnRNZXRyaWNzKGEmJmEucSk7CiAgICBpZighYXx8YS5lcnJvcil7bS5raW5kPSdvdGhlcic7bS5yZWFzb249YSYmYS5lcnJvcj8n5Y+W5b6X5aSx5pWX44CC5Yik5a6a5L+d55WZ77yI5YmN5Zue5bGl5q2044GM44GC44KM44Gw5Y+C6ICD6KGo56S677yJ44CCJzon5pyq5pu05paw44CCJzt9CiAgICBncm91cHNbbS5raW5kXS5wdXNoKHtzdG9jayxhLG19KTsKICB9CiAgZ3JvdXBzLnVwLnNvcnQoKGEsYik9PmIubS5kYXktYS5tLmRheSk7Z3JvdXBzLmRvd24uc29ydCgoYSxiKT0+YS5tLmRheS1iLm0uZGF5KTsKICBjb25zdCBpZHM9e3VwOidVcCcsZG93bjonRG93bicsb3RoZXI6J090aGVyJ307CiAgZm9yKGNvbnN0IGtpbmQgb2YgT2JqZWN0LmtleXMoZ3JvdXBzKSl7CiAgICAkKCdtb3ZlbWVudCcraWRzW2tpbmRdKydDb3VudCcpLnRleHRDb250ZW50PWdyb3Vwc1traW5kXS5sZW5ndGgrJ+S7tic7CiAgICAkKCdtb3ZlbWVudCcraWRzW2tpbmRdKS5pbm5lckhUTUw9Z3JvdXBzW2tpbmRdLnNsaWNlKDAsNTApLm1hcCgoe3N0b2NrLGEsbX0pPT57CiAgICAgIGNvbnN0IG5hbWU9KGEmJmEucSYmYS5xLmNvbXBhbnkmJmEucS5jb21wYW55Lm5hbWUpfHxzdG9jay5uYW1lfHxzdG9jay5jb2RlOwogICAgICBjb25zdCBsYWJlbD1raW5kPT09J3VwJz8n5rOo55uu5YCZ6KOcJzpraW5kPT09J2Rvd24nPyfkuIvokL3orabmiJInOifjgZ3jga7ku5bjg7vkv53nlZknOwogICAgICBjb25zdCBzPShhJiZhLnEmJmEucS5zbmFwc2hvdCl8fHt9OwogICAgICByZXR1cm4gYDxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIiBkYXRhLW1vdmVtZW50PSIke3dhdGNoRXNjYXBlKHN0b2NrLmNvZGUpfSIgJHtvcGVuZWQuaGFzKHN0b2NrLmNvZGUpPydvcGVuJzonJ30+PHN1bW1hcnk+JHt3YXRjaEVzY2FwZShuYW1lKX0gPHNwYW4gY2xhc3M9IndhdGNoLXRpbWluZyB3YXRjaC1uZXV0cmFsIj4ke2xhYmVsfSAvIDHllrbmpa3ml6UgJHtwY3QobS5kYXkpfTwvc3Bhbj48L3N1bW1hcnk+PGRpdiBjbGFzcz0id2F0Y2gtY29udGVudCI+CiAgICAgICAgPGI+JHt3YXRjaEVzY2FwZShzdG9jay5jb2RlKX0gJHt3YXRjaEVzY2FwZShuYW1lKX08L2I+PHAgY2xhc3M9Im11dGVkIj7jgrjjg6Pjg7Pjg6vvvJoke3dhdGNoRXNjYXBlKHN0b2NrR2VucmUoc3RvY2spKX08L3A+PHA+JHt3YXRjaEVzY2FwZShtLnJlYXNvbil9PC9wPgogICAgICAgIDxwIGNsYXNzPSJtdXRlZCI+5bGl5q205pelICR7d2F0Y2hFc2NhcGUobS5kYXRlfHwn4oCUJyl977yIJHttLmRhdGUmJmRhdGFBZ2VEYXlzKG0uZGF0ZSk9PT0wPyfmnKzml6Xjga7ml6XotrPjg4fjg7zjgr8nOiflvZPml6Xjg4fjg7zjgr/jgafjga/jgYLjgorjgb7jgZvjgpMnfe+8iSAvIOWPluW+lyAke3dhdGNoRXNjYXBlKGE/bmV3IERhdGUoYS51cGRhdGVkX2F0KS50b0xvY2FsZVN0cmluZygnamEtSlAnKTon4oCUJyl9PGJyPuS+oeagvOOCveODvOOCuSAke3dhdGNoRXNjYXBlKHMucHJpY2Vfc291cmNlfHwoYSYmYS5xJiZhLnEuc291cmNlKXx8J+KAlCcpfSAvIOWxpeattOiqv+aVtCAke3dhdGNoRXNjYXBlKHMuaGlzdG9yeV9hZGp1c3RtZW50fHwn56K66KqN44Gn44GN44G+44Gb44KTJyl9PC9wPgogICAgICAgICR7bW92ZW1lbnRDaGFydChtLnJvd3MpfTxkaXYgY2xhc3M9ImdyaWQzIj48ZGl2IGNsYXNzPSJrcGkiPjHllrbmpa3ml6U8Yj4ke3BjdChtLmRheSl9PC9iPjwvZGl2PjxkaXYgY2xhc3M9ImtwaSI+NeWWtualreaXpTxiPiR7cGN0KG0uZml2ZSl9PC9iPjwvZGl2PjxkaXYgY2xhc3M9ImtwaSI+MjDllrbmpa3ml6U8Yj4ke3BjdChtLnR3ZW50eSl9PC9iPjwvZGl2PjwvZGl2PgogICAgICAgIDxwIGNsYXNzPSJtdXRlZCI+NeaXpe+8jzIw5pel5bmz5Z2H5Ye65p2l6auY5q+UICR7Zm10KG0udm9sdW1lKX3lgI0gJHt0eXBlb2YgbS52b2x1bWU9PT0nbnVtYmVyJyYmbS52b2x1bWU+PTEuMj8n77yI5Ye65p2l6auY5aKX5Yqg77yJJzonJ30gLyDmnIDmlrDlsaXmrbTntYLlgKQgJHt5ZW4obS5yb3dzLmxlbmd0aD9tLnJvd3MuYXQoLTEpLmNsb3NlOm51bGwpfTwvcD4KICAgICAgPC9kaXY+PC9kZXRhaWxzPmA7CiAgICB9KS5qb2luKCcnKXx8JzxwIGNsYXNzPSJtdXRlZCI+5Y+W5b6X5riI44G/44Gu56+E5Zuy44Gr6Kmy5b2T5YCZ6KOc44Gv44GC44KK44G+44Gb44KT44CCPC9wPic7CiAgfQp9CmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hNb3ZlbWVudChhbGxHZW5yZT1mYWxzZSl7CiAgaWYobW92ZW1lbnRCdXN5fHx3YXRjaEJ1bGtCdXN5fHx3YXRjaEJ1c3kuc2l6ZSl7JCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD0n44G744GL44Gu5YiG5p6Q44GM5a6M5LqG44GX44Gm44GL44KJ5pu05paw44GX44Gm44Gt44CCJztyZXR1cm47fQogIG1vdmVtZW50QnVzeT10cnVlO21vdmVtZW50U3RvcFJlcXVlc3RlZD1mYWxzZTsKICAkKCdtb3ZlbWVudEFsbEJ0bicpLmRpc2FibGVkPXRydWU7CiAgY29uc3QgYnRuPSQoJ21vdmVtZW50QnRuJyk7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSfpipjmn4TkuIDopqfjgpLnorroqo3kuK3igKYnOyQoJ21vdmVtZW50U3RvcCcpLmRpc2FibGVkPWZhbHNlO3JlbmRlcldhdGNoKCk7CiAgbGV0IGRvbmU9MCxmYWlsZWQ9MDsKICB0cnl7CiAgICBhd2FpdCBlbnN1cmVNYXJrZXRVbml2ZXJzZSgpOwogICAgY29uc3QgYWxsPXJlZ2lzdGVyZWRNb3ZlbWVudFN0b2NrcygpOwogICAgaWYoIWFsbC5sZW5ndGgpeyQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9J+OBk+OBruOCuOODo+ODs+ODq+OBrumKmOafhOOBr+OBguOCiuOBvuOBm+OCk+OAgic7cmV0dXJuO30KICAgIGNvbnN0IGN1cnNvcktleT1tYXJrZXRDdXJzb3JLZXkoKTsKICAgIGxldCBjdXJzb3I9TnVtYmVyKGxvY2FsU3RvcmFnZS5nZXRJdGVtKGN1cnNvcktleSl8fChzZWxlY3RlZE1hcmtldEdlbnJlKCk9PT0n5YWo5qWt56iuJz9sb2NhbFN0b3JhZ2UuZ2V0SXRlbSgnZnJlZV9tYXJrZXRfY3Vyc29yX3YxJyk6bnVsbCl8fDApOwogICAgaWYoIU51bWJlci5pc1NhZmVJbnRlZ2VyKGN1cnNvcil8fGN1cnNvcjwwfHxjdXJzb3I+PWFsbC5sZW5ndGgpY3Vyc29yPTA7CiAgICBpZihhbGxHZW5yZSljdXJzb3I9MDsKICAgIGNvbnN0IGJhdGNoPWFsbEdlbnJlP2FsbDphbGwuc2xpY2UoY3Vyc29yLGN1cnNvcisyMCk7CiAgICBmb3IobGV0IGk9MDtpPGJhdGNoLmxlbmd0aDtpKyspewogICAgICBpZihtb3ZlbWVudFN0b3BSZXF1ZXN0ZWQpYnJlYWs7CiAgICAgIGNvbnN0IHN0b2NrPWJhdGNoW2ldOwogICAgICAkKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PWAke2krMX0gLyAke2JhdGNoLmxlbmd0aH3pipjmn4TvvIjkuIDopqcgJHtjdXJzb3IraSsxfSAvICR7YWxsLmxlbmd0aH3vvInvvJoke3N0b2NrLm5hbWV8fHN0b2NrLmNvZGV9IOOBruWxpeattOOCkuWPluW+l+S4reKApmA7CiAgICAgIGNvbnN0IGNhY2hlPXJlYWRNb3ZlbWVudCgpOwogICAgICB0cnl7Y29uc3QgcT1hd2FpdCBnZXRRdW90ZShzdG9jay5jb2RlKTtjYWNoZVtzdG9jay5jb2RlXT17cTpjb21wYWN0TW92ZW1lbnRRdW90ZShxKSx1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxlcnJvcjpudWxsfTtkb25lKys7fQogICAgICBjYXRjaChlKXtjYWNoZVtzdG9jay5jb2RlXT17Li4uKGNhY2hlW3N0b2NrLmNvZGVdfHx7fSksdXBkYXRlZF9hdDpuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksZXJyb3I6U3RyaW5nKGUubWVzc2FnZSl9O2ZhaWxlZCsrO30KICAgICAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oJ2ZyZWVfbW92ZW1lbnRfdjEnLEpTT04uc3RyaW5naWZ5KGNhY2hlKSk7CiAgICAgIGxvY2FsU3RvcmFnZS5zZXRJdGVtKGN1cnNvcktleSxTdHJpbmcoKGN1cnNvcitpKzEpJWFsbC5sZW5ndGgpKTtyZW5kZXJNb3ZlbWVudCgpOwogICAgICBpZihpPGJhdGNoLmxlbmd0aC0xJiYhbW92ZW1lbnRTdG9wUmVxdWVzdGVkKXskKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PWAke2krMX0gLyAke2JhdGNoLmxlbmd0aH3pipjmn4TjgpLlh6bnkIbmuIjjgb/jgILmrKHjga7lj5blvpfjgb7jgafntIQxM+enkuKApmA7YXdhaXQgbW92ZW1lbnRXYWl0KCk7fQogICAgfQogICAgJCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD1gJHttb3ZlbWVudFN0b3BSZXF1ZXN0ZWQ/J+WBnOatouOBl+OBvuOBl+OBnyc6J+S7iuWbnuOBruW3oeWbnuWujOS6hid977ya5Y+W5b6X5oiQ5YqfICR7ZG9uZX0gLyDlpLHmlZcgJHtmYWlsZWR944CC5qyh5Zue44Gv44GT44Gu44K444Oj44Oz44Or44Gu57aa44GN44GL44KJ6Kq/44G544G+44GZ44CC5YCZ6KOc44Gv5ZCE44OH44O844K/5pel44Gu57WC5YCk44OZ44O844K544Gn44CB5YWo6YqY5p+E5beh5Zue44Gr44Gv5pmC6ZaT44GM44GL44GL44KK44G+44GZ44CCYDsKICB9Y2F0Y2goZSl7JCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD0n5beh5Zue44KS5Lit5pat44GX44G+44GX44Gf77yaJytlLm1lc3NhZ2U7fQogIGZpbmFsbHl7bW92ZW1lbnRCdXN5PWZhbHNlO2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+asoeOBrjIw6YqY5p+E44KS6Kq/44G544KLJzskKCdtb3ZlbWVudEFsbEJ0bicpLmRpc2FibGVkPWZhbHNlOyQoJ21vdmVtZW50U3RvcCcpLmRpc2FibGVkPXRydWU7cmVuZGVyTW92ZW1lbnQoKTtyZW5kZXJXYXRjaCgpO30KfQpmdW5jdGlvbiByZW1vdmVXYXRjaChpKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX3dhdGNoJyksIHg9YVtpXTsKICBpZih4PT09dW5kZWZpbmVkKXJldHVybjsKICBjb25zdCBjb2RlPXR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGU7CiAgaWYoIWNvbmZpcm0oYCR7Y29kZX0g44KS44Km44Kp44OD44OB44Oq44K544OI44GL44KJ5YmK6Zmk44GX44G+44GZ44GL77yfYCkpcmV0dXJuOwogIGEuc3BsaWNlKGksMSk7CiAgc2F2ZSgnZnJlZV93YXRjaCcsYSk7CiAgdHJ5e2NvbnN0IGNhY2hlPUpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfd2F0Y2hfYW5hbHlzaXNfdjEnKXx8J3t9Jyk7ZGVsZXRlIGNhY2hlW1N0cmluZyhjb2RlKV07bG9jYWxTdG9yYWdlLnNldEl0ZW0oJ2ZyZWVfd2F0Y2hfYW5hbHlzaXNfdjEnLEpTT04uc3RyaW5naWZ5KGNhY2hlKSl9Y2F0Y2goZSl7fQogIHJlbmRlcldhdGNoKCk7Cn0KYXN5bmMgZnVuY3Rpb24gYWRkV2F0Y2goKXsKICBsZXQgYz0kKCd3YXRjaENvZGUnKS52YWx1ZS50cmltKCk7IGlmKCFjKXJldHVybjsKICBsZXQgaW5mbz1udWxsOwogIHRyeXtpbmZvPWF3YWl0IGdldENvbXBhbnkoYyl9Y2F0Y2goZSl7fQogIGxldCBhPWxvY2FsKCdmcmVlX3dhdGNoJyk7CiAgY29uc3QgZXhpc3RzPWEuc29tZSh4PT4odHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSk9PT1jKTsKICBpZighZXhpc3RzKWEucHVzaCh7Y29kZTpjLG5hbWU6aW5mbyYmaW5mby5uYW1lP2luZm8ubmFtZTonJ30pOwogIHNhdmUoJ2ZyZWVfd2F0Y2gnLGEpOwogIHJlbmRlcldhdGNoKCk7Cn0KZnVuY3Rpb24gdXBkYXRlS2FidXRhbigpe2xldCBjPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7JCgna2FidXRhbicpLmhyZWY9Yz8naHR0cHM6Ly9rYWJ1dGFuLmpwL3N0b2NrLz9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpOidodHRwczovL2thYnV0YW4uanAvJ30KJCgnY29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+e3VwZGF0ZUthYnV0YW4oKTtzY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2NvZGUnLCdjb21wYW55TmFtZScsJycpfSk7dXBkYXRlS2FidXRhbigpOwokKCdob2xkQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCdob2xkQ29kZScsJ2hvbGRDb21wYW55TmFtZScsJycpKTsKJCgnd2F0Y2hDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ3dhdGNoQ29kZScsJ3dhdGNoQ29tcGFueU5hbWUnLCcnKSk7CgoKCmFzeW5jIGZ1bmN0aW9uIGdldFBvbGljeShjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9wb2xpY3k/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+WbveetluODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiB4LnBvbGljeXx8e307Cn0KCmZ1bmN0aW9uIHBvbGljeVNvdXJjZVN0YXR1c0phKHMpewogIGlmKHM9PT0ndmVyaWZpZWRfbGl2ZScpcmV0dXJuICflhazlvI/jg5rjg7zjgrjnorroqo3muIgnOwogIGlmKHM9PT0ncGFydGlhbF9saXZlJylyZXR1cm4gJ+WFrOW8j+ODmuODvOOCuOmDqOWIhueiuuiqjSc7CiAgaWYocz09PSd2ZXJpZmllZF9yZWdpc3RyeScpcmV0dXJuICfmnIDntYLnorroqo3muIjlhazlvI/jgr3jg7zjgrknOwogIHJldHVybiAn56K66KqN5LiN5Y+vJzsKfQoKZnVuY3Rpb24gcmVuZGVyUG9saWN5VGhlbWVzKHApewogIGNvbnN0IGJveD0kKCdwb2xpY3lUaGVtZXMnKTsKICBpZighYm94KXJldHVybjsKICBjb25zdCB0aGVtZXM9KHAmJnAubWF0Y2hlZF90aGVtZXMpfHxbXTsKICBpZighdGhlbWVzLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8ZGl2IGNsYXNzPSJwb2xpY3l0aGVtZSI+PGI+6Zai6YCj44OG44O844Oe44Gq44GXIC8g5Yik5a6a5L+d55WZPC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pyA5L2O6Zai6YCj5bqm44KS5rqA44Gf44GZ5YWs5byP5pS/562W44OG44O844Oe44GM44GC44KK44G+44Gb44KT44CCPC9zcGFuPjwvZGl2Pic7CiAgICByZXR1cm47CiAgfQogIGJveC5pbm5lckhUTUw9dGhlbWVzLnNsaWNlKDAsNCkubWFwKHQ9PmAKICAgIDxkaXYgY2xhc3M9InBvbGljeXRoZW1lIj4KICAgICAgPGI+JHt0Lm5hbWV9IC8g6Zai6YCj5bqmICR7KE51bWJlcih0LnJlbGV2YW5jZSkqMTAwKS50b0ZpeGVkKDApfSU8L2I+CiAgICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5pS/562W5by35bqmICR7TnVtYmVyKHQucG9saWN5X3N0cmVuZ3RoKS50b0ZpeGVkKDApfSAvIOWvhOS4jiAke051bWJlcih0LmNvbnRyaWJ1dGlvbikudG9GaXhlZCgxKX0gLyAke3BvbGljeVNvdXJjZVN0YXR1c0phKHQuc291cmNlX3N0YXR1cyl9PC9zcGFuPjxicj4KICAgICAgPGEgaHJlZj0iJHt0LnVybH0iIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7lhazlvI/jgr3jg7zjgrk8L2E+CiAgICA8L2Rpdj4KICBgKS5qb2luKCcnKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0RnVuZGFtZW50YWxzKGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL2Z1bmRhbWVudGFscz9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5rG6566X44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgcmV0dXJuIHguZnVuZGFtZW50YWxzfHx7fTsKfQoKCmZ1bmN0aW9uIG1hcmtldEZyZXNobmVzc0ZhY3RvcihkYXRlU3RyKXsKICBjb25zdCBmPWZyZXNobmVzc0ZvcihkYXRlU3RyKTsKICBpZihmLmxldmVsPT09J2ZyZXNoJylyZXR1cm4gMS4wMDsKICBpZihmLmxldmVsPT09J3dhcm5pbmcnKXJldHVybiAwLjcwOwogIGlmKGYubGV2ZWw9PT0nc3RhbGUnKXJldHVybiAwLjI1OwogIHJldHVybiAwLjIwOwp9CgpmdW5jdGlvbiBmcmVzaG5lc3NQY3RUZXh0KHYpewogIGlmKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSlyZXR1cm4gJ+KAlCc7CiAgcmV0dXJuIE1hdGgucm91bmQoTnVtYmVyKHYpKjEwMCkrJyUnOwp9CgpmdW5jdGlvbiBmaW5hbmNpYWxGcmVzaG5lc3NMYWJlbChsYWJlbCl7CiAgaWYobGFiZWw9PT0nZnJlc2gnKXJldHVybiAn5paw44GX44GEJzsKICBpZihsYWJlbD09PSdzbGlnaHRseV9vbGQnKXJldHVybiAn44KE44KE5Y+k44GEJzsKICBpZihsYWJlbD09PSdvbGQnKXJldHVybiAn5Y+k44GEJzsKICBpZihsYWJlbD09PSd2ZXJ5X29sZCcpcmV0dXJuICfjgYvjgarjgorlj6TjgYQnOwogIGlmKGxhYmVsPT09J3N0YWxlJylyZXR1cm4gJ+mdnuW4uOOBq+WPpOOBhCc7CiAgcmV0dXJuICfplovnpLrml6XkuI3mmI4nOwp9CgpmdW5jdGlvbiBwb2xpY3lGcmVzaG5lc3NGYWN0b3IocCxtYW51YWxNb2RlKXsKICBpZihtYW51YWxNb2RlKXJldHVybiAxLjAwOwogIGlmKCFwfHxwLnNjb3JlPT09bnVsbHx8cC5zY29yZT09PXVuZGVmaW5lZClyZXR1cm4gMDsKICBjb25zdCBjPU51bWJlcihwLmNvbmZpZGVuY2VfcGN0KTsKICBpZihOdW1iZXIuaXNGaW5pdGUoYykpcmV0dXJuIE1hdGgubWF4KDAsTWF0aC5taW4oMSxjLzEwMCkpOwogIHJldHVybiAwLjUwOwp9CgpmdW5jdGlvbiBzY29yZUxhYmVsKHYpewogIGlmKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSlyZXR1cm4gJ+KAlCc7CiAgY29uc3Qgbj1OdW1iZXIodik7CiAgcmV0dXJuIChuPjA/JysnOicnKStuLnRvRml4ZWQoMSk7Cn0KCmZ1bmN0aW9uIHBjdE1heWJlKHYpewogIHJldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgxKSsnJSc7Cn0KCmFzeW5jIGZ1bmN0aW9uIGFuYWx5emUoKXsKICBjb25zdCBjb2RlPSQoJ2NvZGUnKS52YWx1ZS50cmltKCk7CiAgaWYoIWNvZGUpeyQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfpipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjga0nO3JldHVybn0KICBjb25zdCBidG49JCgnYW5hbHl6ZUJ0bicpOyBidG4uZGlzYWJsZWQ9dHJ1ZTsgYnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnOwogICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSdGaW5NaW5k57SEOTAw5pel5L6h5qC85bGl5q2077yLSi1RdWFudHPmsbrnrpfjg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKYnOwoKICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLCBzPXEuc25hcHNob3R8fHt9OwogICAgJCgncHJpY2UnKS52YWx1ZT1zLmxhc3RfY2xvc2U9PW51bGw/Jyc6Zm10KHMubGFzdF9jbG9zZSwxKTsKICAgICQoJ3IyMCcpLnZhbHVlPWZtdChzLnJldHVybl8yMGQpOyQoJ3IxMjYnKS52YWx1ZT1mbXQocy5yZXR1cm5fMTI2ZCk7JCgncjI1MicpLnZhbHVlPWZtdChzLnJldHVybl8yNTJkKTsKICAgICQoJ2hpZ2gyMCcpLnRleHRDb250ZW50PWZtdChzLmhpZ2hfMjBkLDEpOyQoJ2xvdzIwJykudGV4dENvbnRlbnQ9Zm10KHMubG93XzIwZCwxKTsKICAgICQoJ3ZvbDIwJykudGV4dENvbnRlbnQ9cy52b2xhdGlsaXR5XzIwZF9hbm51YWxpemVkPT1udWxsPyfigJQnOmZtdChzLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQpKyclJzsKCiAgICBkaXNwbGF5Q29tcGFueSgkKCdjb21wYW55TmFtZScpLHEuY29tcGFueXx8bnVsbCwnJyk7CiAgICBjb25zdCBwcmljZVNvdXJjZT1zLnByaWNlX3NvdXJjZXx8cS5zb3VyY2V8fCfkuI3mmI4nOwogICAgY29uc3QgaGlzdG9yeURhdGU9cy5oaXN0b3J5X2xhc3RfZGF0ZXx8bnVsbDsKICAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0KICAgICAgJzxzcGFuIGNsYXNzPSJzb3VyY2ViYWRnZSI+54++5Zyo5YCkPC9zcGFuPjxiIGNsYXNzPSJvayI+JytwcmljZVNvdXJjZSsnPC9iPicrCiAgICAgICc8YnI+54++5Zyo5YCk44OH44O844K/5pelOiAnKyhzLmxhc3RfZGF0ZXx8J+KAlCcpKwogICAgICAocy5wcmljZV90aW1lPycgJytzLnByaWNlX3RpbWU6JycpKwogICAgICAnIC8g5pyA5paw5Y+W5b6X5YCkOiAnK2ZtdChzLmxhc3RfY2xvc2UsMSkrCiAgICAgICc8YnI+PHNwYW4gY2xhc3M9InNvdXJjZWJhZGdlIj7kvqHmoLzlsaXmrbQ8L3NwYW4+JysKICAgICAgKHMuaGlzdG9yeV9zb3VyY2V8fCflj5blvpfjgarjgZcnKSsKICAgICAgJyAvIOacgOe1guaXpTogJysoaGlzdG9yeURhdGV8fCfigJQnKSsKICAgICAgJyAvIOWxpeattOOCteODs+ODl+ODqzogJysocy5zYW1wbGVfY291bnQ/PzApKyfku7YnOwogICAgc2hvd0ZyZXNobmVzcyhzLmxhc3RfZGF0ZSk7CiAgICBzaG93SGlzdG9yeUZyZXNobmVzcyhoaXN0b3J5RGF0ZSxzLmxhc3RfZGF0ZSk7CgogICAgY29uc3QgbWFya2V0RnJlc2huZXNzPW1hcmtldEZyZXNobmVzc0ZhY3RvcihoaXN0b3J5RGF0ZXx8cy5sYXN0X2RhdGUpOwogICAgJCgnbWFya2V0RnJlc2gnKS50ZXh0Q29udGVudD1mcmVzaG5lc3NQY3RUZXh0KG1hcmtldEZyZXNobmVzcyk7CiAgICBjb25zdCBtYXJrZXRBZ2U9ZGF0YUFnZURheXMoaGlzdG9yeURhdGV8fHMubGFzdF9kYXRlKTsKICAgICQoJ21hcmtldEZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgIGDkvqHmoLzlsaXmrbQgJHsoaGlzdG9yeURhdGV8fHMubGFzdF9kYXRlfHwn4oCUJyl9IC8gJHttYXJrZXRBZ2U9PT1udWxsPyfml6XmlbDkuI3mmI4nOm1hcmtldEFnZSsn5pelJ31gOwoKICAgIGNvbnN0IHN5bmNlZEhvbGRpbmc9c3luY0FuYWx5emVkUXVvdGVUb0hvbGRpbmcoY29kZSxxKTsKICAgIGlmKHN5bmNlZEhvbGRpbmcpewogICAgICBjb25zdCBzdGF0dXM9JCgnaG9sZGluZ1JlZnJlc2hTdGF0dXMnKTsKICAgICAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1gJHtjb2RlfSDjga7kv53mnInmoKrjgpLliIbmnpDmmYLjga7mnIDmlrDlj5blvpfntYLlgKQgJHtmbXQocy5sYXN0X2Nsb3NlLDEpfSDjgaflho3oqIjnrpfjgZfjgb7jgZfjgZ/jgIJgOwogICAgfQoKICAgICQoJ2FuYWx5c2lzRXYnKS5pbm5lckhUTUw9CiAgICAgIGV2SHRtbCgn55+t5pyfMjDml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzIwZCddKSsKICAgICAgZXZIdG1sKCfkuK3mnJ8xMjbml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzEyNmQnXSkrCiAgICAgIGV2SHRtbCgn6ZW35pyfMjUy5pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycyNTJkJ10pOwoKICAgIGNvbnN0IHN1cHBseT1zLnN1cHBseV9wcm94eXx8e307CiAgICAkKCdzdXBwbHlBdXRvJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChzdXBwbHkuc2NvcmUpOwogICAgJCgnc3VwcGx5RGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICc15pelLzIw5pel5Ye65p2l6auYICcrKHN1cHBseS52b2x1bWVfcmF0aW9fNV8yMD09bnVsbD8n4oCUJzpOdW1iZXIoc3VwcGx5LnZvbHVtZV9yYXRpb181XzIwKS50b0ZpeGVkKDIpKyflgI0nKTsKCiAgICBsZXQgZnVuZGFtZW50YWxzPXt9OwogICAgdHJ5ewogICAgICBmdW5kYW1lbnRhbHM9YXdhaXQgZ2V0RnVuZGFtZW50YWxzKGNvZGUpOwogICAgICAkKCdlYXJuQXV0bycpLnRleHRDb250ZW50PXNjb3JlTGFiZWwoZnVuZGFtZW50YWxzLnNjb3JlKTsKICAgICAgY29uc3QgbT1mdW5kYW1lbnRhbHMubWV0cmljc3x8e307CiAgICAgIGNvbnN0IGVmPWZ1bmRhbWVudGFscy5mcmVzaG5lc3N8fHt9OwogICAgICAkKCdlYXJuRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgJ+mWi+ekuiAnKygoZnVuZGFtZW50YWxzLmxhdGVzdCYmZnVuZGFtZW50YWxzLmxhdGVzdC5kYXRlKXx8J+KAlCcpKwogICAgICAgICcgLyDlo7LkuIogJytwY3RNYXliZShtLnNhbGVzX2dyb3d0aF9wY3QpKwogICAgICAgICcgLyDllrbmpa3nm4ogJytwY3RNYXliZShtLm9wX2dyb3d0aF9wY3QpOwogICAgICAkKCdlYXJuRnJlc2gnKS50ZXh0Q29udGVudD0KICAgICAgICBlZi5mYWN0b3I9PT11bmRlZmluZWQ/J+KAlCc6TWF0aC5yb3VuZChOdW1iZXIoZWYuZmFjdG9yKSoxMDApKyclJzsKICAgICAgJCgnZWFybkZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgZmluYW5jaWFsRnJlc2huZXNzTGFiZWwoZWYubGFiZWwpKwogICAgICAgICcgLyAnKyhlZi5hZ2VfZGF5cz09PW51bGx8fGVmLmFnZV9kYXlzPT09dW5kZWZpbmVkPyfplovnpLrml6XkuI3mmI4nOmVmLmFnZV9kYXlzKyfml6XliY0nKTsKICAgIH1jYXRjaChmZSl7CiAgICAgICQoJ2Vhcm5BdXRvJykudGV4dENvbnRlbnQ9J+S4jeaYjic7CiAgICAgICQoJ2Vhcm5EZXRhaWwnKS50ZXh0Q29udGVudD0n44GT44Gu44OX44Op44OzL+mKmOafhOOBp+OBr+WPluW+l+OBp+OBjeOBquOBhOWPr+iDveaAp+OBguOCiic7CiAgICAgICQoJ2Vhcm5GcmVzaCcpLnRleHRDb250ZW50PSfigJQnOwogICAgICAkKCdlYXJuRnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0n5rG6566X44OH44O844K/44Gq44GXJzsKICAgICAgZnVuZGFtZW50YWxzPXtzY29yZTpudWxsLGZyZXNobmVzczp7ZmFjdG9yOjB9fTsKICAgIH0KCiAgICBsZXQgYXV0b1BvbGljeT17c2NvcmU6bnVsbCxtYXRjaGVkX3RoZW1lczpbXX07CiAgICB0cnl7CiAgICAgIGF1dG9Qb2xpY3k9YXdhaXQgZ2V0UG9saWN5KGNvZGUpOwogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PWF1dG9Qb2xpY3kuc2NvcmU9PW51bGw/J+S4jeaYjic6c2NvcmVMYWJlbChhdXRvUG9saWN5LnNjb3JlKTsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgYXV0b1BvbGljeS5zY29yZT09bnVsbAogICAgICAgICAgPyAn6Zai6YCj44GZ44KL5YWs5byP5pS/562W44OG44O844Oe44Gq44GXJwogICAgICAgICAgOiAn6Ieq5YuV5Zu9562WcHJveHkgLyDkv6HpoLzluqYgJysoYXV0b1BvbGljeS5jb25maWRlbmNlX3BjdD8/J+KAlCcpKyclIC8gJysoKGF1dG9Qb2xpY3kubWF0Y2hlZF90aGVtZXN8fFtdKS5sZW5ndGgpKyfjg4bjg7zjg54nOwogICAgICBjb25zdCBwZj1wb2xpY3lGcmVzaG5lc3NGYWN0b3IoYXV0b1BvbGljeSxmYWxzZSk7CiAgICAgICQoJ3BvbGljeUZyZXNoJykudGV4dENvbnRlbnQ9ZnJlc2huZXNzUGN0VGV4dChwZik7CiAgICAgICQoJ3BvbGljeUZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgYXV0b1BvbGljeS5zY29yZT09bnVsbAogICAgICAgICAgPyAn6Zai6YCj44OG44O844Oe44Gq44GXJwogICAgICAgICAgOiAoKGF1dG9Qb2xpY3kubWF0Y2hlZF90aGVtZXN8fFtdKS5zb21lKHQ9PnQuc291cmNlX3N0YXR1cz09PSd2ZXJpZmllZF9saXZlJykKICAgICAgICAgICAgICA/ICflhazlvI/jg5rjg7zjgrjjgpLjg6njgqTjg5bnorroqo0nCiAgICAgICAgICAgICAgOiAn56K66KqN5riI44G/5YWs5byP44K944O844K544KS5L2/55SoJyk7CiAgICAgIHJlbmRlclBvbGljeVRoZW1lcyhhdXRvUG9saWN5KTsKICAgIH1jYXRjaChwZSl7CiAgICAgICQoJ3BvbGljeVN0YXRlJykudGV4dENvbnRlbnQ9J+S4jeaYjic7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PSflhazlvI/mlL/nrZbjgr3jg7zjgrnlj5blvpfjgqjjg6njg7wnOwogICAgICAkKCdwb2xpY3lGcmVzaCcpLnRleHRDb250ZW50PSfigJQnOwogICAgICAkKCdwb2xpY3lGcmVzaERldGFpbCcpLnRleHRDb250ZW50PSflj5blvpfjgqjjg6njg7wnOwogICAgICAkKCdwb2xpY3lUaGVtZXMnKS5pbm5lckhUTUw9Jyc7CiAgICAgIGF1dG9Qb2xpY3k9e3Njb3JlOm51bGwsbWF0Y2hlZF90aGVtZXM6W119OwogICAgfQoKICAgIGNvbnN0IG1hbnVhbFBvbGljeT12YWwoJ3BvbGljeScpOwogICAgY29uc3QgcG9saWN5U2NvcmU9bWFudWFsUG9saWN5PT09bnVsbD9hdXRvUG9saWN5LnNjb3JlOm1hbnVhbFBvbGljeTsKICAgIGNvbnN0IHBvbGljeU1vZGU9bWFudWFsUG9saWN5PT09bnVsbD8nYXV0byc6J21hbnVhbCc7CiAgICBpZihtYW51YWxQb2xpY3khPT1udWxsKXsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKG1hbnVhbFBvbGljeSk7CiAgICAgICQoJ3BvbGljeURldGFpbCcpLnRleHRDb250ZW50PSfmiYvlhaXlipvjgafoh6rli5XlgKTjgpLkuIrmm7jjgY0nOwogICAgICAkKCdwb2xpY3lGcmVzaCcpLnRleHRDb250ZW50PScxMDAlJzsKICAgICAgJCgncG9saWN5RnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0n44Om44O844K244O85omL5YWl5Yqb5YCkJzsKICAgIH0KCiAgICBjb25zdCBkPXsKICAgICAgY29kZSwKICAgICAgcHJpY2U6cy5sYXN0X2Nsb3NlLAogICAgICByZXR1cm4yMDpzLnJldHVybl8yMGQsCiAgICAgIHJldHVybjEyNjpzLnJldHVybl8xMjZkLAogICAgICByZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCwKICAgICAgZWFybmluZ3Nfc2NvcmU6ZnVuZGFtZW50YWxzLnNjb3JlLAogICAgICBwb2xpY3lfc2NvcmU6cG9saWN5U2NvcmUsCiAgICAgIHBvbGljeV9tb2RlOnBvbGljeU1vZGUsCiAgICAgIHN1cHBseV9zY29yZTpzdXBwbHkuc2NvcmUsCiAgICAgIG1hcmtldF9mcmVzaG5lc3M6bWFya2V0RnJlc2huZXNzLAogICAgICBlYXJuaW5nc19mcmVzaG5lc3M6TnVtYmVyKAogICAgICAgIGZ1bmRhbWVudGFscy5mcmVzaG5lc3MmJmZ1bmRhbWVudGFscy5mcmVzaG5lc3MuZmFjdG9yIT09dW5kZWZpbmVkCiAgICAgICAgICA/IGZ1bmRhbWVudGFscy5mcmVzaG5lc3MuZmFjdG9yCiAgICAgICAgICA6IChmdW5kYW1lbnRhbHMuc2NvcmU9PW51bGw/MDoxKQogICAgICApLAogICAgICBwb2xpY3lfZnJlc2huZXNzOnBvbGljeUZyZXNobmVzc0ZhY3RvcigKICAgICAgICBhdXRvUG9saWN5LAogICAgICAgIG1hbnVhbFBvbGljeSE9PW51bGwKICAgICAgKQogICAgfTsKCiAgICBjb25zdCBhcj1hd2FpdCBmZXRjaCgnL2FwaS9mcmVlL2FuYWx5emUnLHsKICAgICAgbWV0aG9kOidQT1NUJywKICAgICAgaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSwKICAgICAgYm9keTpKU09OLnN0cmluZ2lmeShkKSwKICAgICAgY2FjaGU6J25vLXN0b3JlJwogICAgfSk7CiAgICBjb25zdCB4PWF3YWl0IGFyLmpzb24oKTsKICAgIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfliIbmnpDjg4fjg7zjgr/jgYzkuI3otrPjgZfjgabjgYTjgb7jgZknKTsKCiAgICAkKCdzdGF0ZScpLnRleHRDb250ZW50PXN0YXRlSmEoeC5zaWduYWwuc3RhdGUpOwogICAgJCgncG9zJykudGV4dENvbnRlbnQ9eC5zaWduYWwucG9zaXRpdmVfY291bnQ7CiAgICAkKCduZWcnKS50ZXh0Q29udGVudD14LnNpZ25hbC5uZWdhdGl2ZV9jb3VudDsKCiAgICBjb25zdCBzYz14LnNjb3JlfHx7fTsKICAgICQoJ3Njb3JlSGVybycpLnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgICQoJ3Njb3JlMTAwJykudGV4dENvbnRlbnQ9c2Muc2NvcmUxMDA9PW51bGw/J+KAlCc6c2Muc2NvcmUxMDArJyAvIDEwMCc7CgogICAgY29uc3QgcGlsbD0kKCdzY29yZVN0YXRlUGlsbCcpOwogICAgcGlsbC5jbGFzc05hbWU9J3N0YXRlcGlsbCAnK3N0YXRlQ2xhc3MoeC5zaWduYWwuc3RhdGUpOwogICAgcGlsbC50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTsKCiAgICAkKCdjb3ZlcmFnZScpLnRleHRDb250ZW50PXNjLmNvdmVyYWdlX3BjdD09bnVsbD8n4oCUJzpzYy5jb3ZlcmFnZV9wY3QrJyUnOwogICAgJCgnZnJlc2hDb3ZlcmFnZScpLnRleHRDb250ZW50PXNjLmNvdmVyYWdlX3BjdD09bnVsbD8n4oCUJzpzYy5jb3ZlcmFnZV9wY3QrJyUnOwogICAgY29uc3Qgc2Y9c2MuZnJlc2huZXNzfHx7fTsKICAgIGlmKHNmLm1hcmtldF9wY3QhPT11bmRlZmluZWQpJCgnbWFya2V0RnJlc2gnKS50ZXh0Q29udGVudD1NYXRoLnJvdW5kKE51bWJlcihzZi5tYXJrZXRfcGN0KSkrJyUnOwogICAgaWYoc2YuZWFybmluZ3NfcGN0IT09dW5kZWZpbmVkKSQoJ2Vhcm5GcmVzaCcpLnRleHRDb250ZW50PU1hdGgucm91bmQoTnVtYmVyKHNmLmVhcm5pbmdzX3BjdCkpKyclJzsKICAgIGlmKHNmLnBvbGljeV9wY3QhPT11bmRlZmluZWQpJCgncG9saWN5RnJlc2gnKS50ZXh0Q29udGVudD1NYXRoLnJvdW5kKE51bWJlcihzZi5wb2xpY3lfcGN0KSkrJyUnOwoKICAgICQoJ3Njb3JlQnJlYWtkb3duJykudGV4dENvbnRlbnQ9CiAgICAgICfjg4bjgq/jg4vjgqvjg6sgJytzY29yZUxhYmVsKHNjLnRlY2huaWNhbCkrCiAgICAgICcgLyDmsbrnrpcgJytzY29yZUxhYmVsKHNjLmVhcm5pbmdzKSsKICAgICAgJyAvIOmcgOe1piAnK3Njb3JlTGFiZWwoc2Muc3VwcGx5KSsKICAgICAgJyAvIOWbveetlnByb3h5ICcrc2NvcmVMYWJlbChzYy5wb2xpY3kpOwoKICAgICQoJ3Njb3JlUmVhc29uJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmVSZWFzb25UZXh0JykudGV4dENvbnRlbnQ9ZHJpdmVyU2VudGVuY2Uoc2MpOwogICAgJCgnZHJpdmVyR3JpZCcpLmlubmVySFRNTD0KICAgICAgZHJpdmVyQm94SHRtbCgn5pyA5aSn44Gu44OX44Op44K56KaB5ZugJyxzYy5zdHJvbmdlc3RfcG9zaXRpdmUsJ3Bvc2l0aXZlJykrCiAgICAgIGRyaXZlckJveEh0bWwoJ+acgOWkp+OBruODnuOCpOODiuOCueimgeWboCcsc2Muc3Ryb25nZXN0X25lZ2F0aXZlLCduZWdhdGl2ZScpOwoKICAgIHJlbmRlckNvbnRyaWJ1dGlvbnMoc2MpOwoKICAgIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihzLmxhc3RfZGF0ZSk7CiAgICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlKTsKICAgIGxldCByZXN1bHRUZXh0PQogICAgICAhZnJlc2guZGVjaXNpb25fb2sKICAgICAgICA/ICfimqDvuI8g54++5Zyo5YCk44OH44O844K/44GM5Y+k44GE44Gf44KB44CB5LuK5pel44Gu5aOy6LK35Yik5pat44Go44GX44Gm44Gv5L2/55So44GX44G+44Gb44KT44CCJwogICAgICAgIDogIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vawogICAgICAgICAgPyAn4pqg77iPIOePvuWcqOWApOOBr+aWsOOBl+OBhOaXpei2s+OCkuS9v+OBo+OBpuOBhOOBvuOBmeOBjOOAgeS+oeagvOWxpeattOOBjOWPpOOBhOOBn+OCgee3j+WQiOeCueODu+efreS4remVt+acn+ODiOODrOODs+ODieODu+acn+W+heWApOOBr+WPguiAg+WApOOBp+OBmeOAgicKICAgICAgICAgIDogJ+ePvuWcqOWApOOBqOS+oeagvOWxpeattOOBrumuruW6puOCkueiuuiqjea4iOOBv+OAgic7CgogICAgY29uc3QgZWZhY3Rvcj1OdW1iZXIoCiAgICAgIGZ1bmRhbWVudGFscy5mcmVzaG5lc3MmJmZ1bmRhbWVudGFscy5mcmVzaG5lc3MuZmFjdG9yIT09dW5kZWZpbmVkCiAgICAgICAgPyBmdW5kYW1lbnRhbHMuZnJlc2huZXNzLmZhY3RvcgogICAgICAgIDogMQogICAgKTsKICAgIGlmKGZ1bmRhbWVudGFscy5zY29yZSE9PW51bGwmJmVmYWN0b3I8MSl7CiAgICAgIHJlc3VsdFRleHQrPWAg5rG6566X44Gv6ZaL56S644GL44KJ5pmC6ZaT44GM57WM44Gj44Gm44GE44KL44Gf44KB44CB57eP5ZCI54K544Gn44Gv5Z+65rqW6YeN44G/MzAl44Gr6a6u5bqmJHtNYXRoLnJvdW5kKGVmYWN0b3IqMTAwKX0l44KS5o6b44GR44Gm5b2x6Z+/44KS5byx44KB44Gm44GE44G+44GZ44CCYDsKICAgIH0KICAgIGlmKHBvbGljeU1vZGU9PT0nYXV0bycmJnBvbGljeVNjb3JlIT09bnVsbCl7CiAgICAgIGNvbnN0IHBmPXBvbGljeUZyZXNobmVzc0ZhY3RvcihhdXRvUG9saWN5LGZhbHNlKTsKICAgICAgaWYocGY8MSl7CiAgICAgICAgcmVzdWx0VGV4dCs9YCDlm73nrZZwcm94eeOCguOCveODvOOCueeiuuiqjeeKtuaFi+OBq+W/nOOBmOOBpumuruW6piR7TWF0aC5yb3VuZChwZioxMDApfSXjgafph43jgb/oqr/mlbTjgZfjgabjgYTjgb7jgZnjgIJgOwogICAgICB9CiAgICB9CiAgICAkKCdyZXN1bHQnKS50ZXh0Q29udGVudD1yZXN1bHRUZXh0OwogIH1jYXRjaChlKXsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfimqDvuI8gJytlLm1lc3NhZ2U7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9JzxzcGFuIGNsYXNzPSJlcnIiPuWPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UrJzwvc3Bhbj4nOwogIH1maW5hbGx5ewogICAgYnRuLmRpc2FibGVkPWZhbHNlOwogICAgYnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafliIbmnpAnOwogIH0KfQoKcmVuZGVySG9sZGluZ3MoKTtyZW5kZXJXYXRjaCgpO3JlbmRlck1vdmVtZW50KCk7CnNldFRpbWVvdXQoKCk9PnJlZnJlc2hBbGxIb2xkaW5ncyhmYWxzZSksNDAwKTsKaWYoJ3NlcnZpY2VXb3JrZXInIGluIG5hdmlnYXRvcil7bmF2aWdhdG9yLnNlcnZpY2VXb3JrZXIuZ2V0UmVnaXN0cmF0aW9ucygpLnRoZW4ocnM9PlByb21pc2UuYWxsKHJzLm1hcChyPT5yLnVucmVnaXN0ZXIoKSkpKS5jYXRjaCgoKT0+e30pfQppZignY2FjaGVzJyBpbiB3aW5kb3cpe2NhY2hlcy5rZXlzKCkudGhlbihrZXlzPT5Qcm9taXNlLmFsbChrZXlzLm1hcChrPT5jYWNoZXMuZGVsZXRlKGspKSkpLmNhdGNoKCgpPT57fSl9Cjwvc2NyaXB0Pgo8L21haW4+CjwvYm9keT4KPC9odG1sPg=='
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
