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
            raise ValueError('\u9298\u67c4\u4e00\u89a7\u3092\u53d6\u5f97\u3067\u304d\u307e\u305b\u3093\u3067\u3057\u305f')
        catalog={}
        for row in payload.get('data') or []:
            symbol=str(row.get('stock_id') or '').strip()
            if not symbol.endswith('.T'):continue
            code=symbol[:-2]
            if len(code)!=4 or not code.isalnum():continue
            catalog[code]={'code':code,'name':str(row.get('stock_name') or '').strip(),'sector':row.get('Sector') or '', 'catalog_date':row.get('date') or ''}
        if not catalog:raise ValueError('\u9298\u67c4\u4e00\u89a7\u304c\u7a7a\u3067\u3059')
        result={'status':'ok','stocks':[catalog[k] for k in sorted(catalog)],'count':len(catalog),'source':'FinMind JapanStockInfo (.T)','fetched_at':datetime.now(timezone.utc).isoformat()}
        FREE_UNIVERSE_CACHE.update(result=result,ts=now)
        return jsonify(result)
    except Exception:
        return jsonify(status='error',reason='\u7121\u6599\u306e\u9298\u67c4\u4e00\u89a7\u3092\u53d6\u5f97\u3067\u304d\u307e\u305b\u3093\u3002\u6642\u9593\u3092\u7a7a\u3051\u3066\u518d\u5ea6\u8a66\u3057\u3066\u304f\u3060\u3055\u3044\u3002'),502


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
    if cached and cached.get('items') and now.timestamp()-NEWS_ADVICE_CACHE.get('ts',0)<900 and request.args.get('refresh')!='1':return jsonify(cached)
    try:
        response=requests.get('https://news.google.com/rss',params={'hl':'ja','gl':'JP','ceid':'JP:ja'},headers={'User-Agent':'JapanStockAIFree/news'},timeout=15)
        response.raise_for_status()
        if len(response.content)>2000000:raise ValueError('feed size')
        root=ET.fromstring(response.content)
        items=[];seen=set();sources={}
        for item in root.findall('.//item'):
            title=unescape(item.findtext('title') or '').strip()
            link=item.findtext('link') or ''
            if not title or title in seen or urlparse(link).scheme!='https' or urlparse(link).hostname!='news.google.com':continue
            if '\u682a\u4fa1\u30fb\u682a\u5f0f\u60c5\u5831' in title:continue
            try:
                published=parsedate_to_datetime(item.findtext('pubDate') or '')
                if published.tzinfo is None:published=published.replace(tzinfo=timezone.utc)
                age=(now-published).total_seconds()
                if age < -3600 or age>72*3600:continue
            except Exception:continue
            source=(item.findtext('source') or '').strip()
            if sources.get(source,0)>=4:continue
            raw=item.findtext('description') or ''
            summary=unescape(re.sub('<[^>]+>',' ',raw))
            summary=re.sub(r'\s+',' ',summary).strip()[:300]
            text=title+' '+summary
            if any(k in text for k in ['\u65e5\u9280','\u91d1\u5229','\u5229\u4e0a\u3052','\u5229\u4e0b\u3052']):
                topic='\u91d1\u878d\u653f\u7b56\u30fb\u91d1\u5229';comment='\u91d1\u5229\u306e\u30cb\u30e5\u30fc\u30b9\u306f\u3001\u9280\u884c\u3068\u501f\u5165\u306e\u591a\u3044\u4f01\u696d\u3067\u5f71\u97ff\u304c\u9055\u3046\u3088\u3002\u653f\u7b56\u304c\u6c7a\u5b9a\u6e08\u307f\u304b\u3001\u4e88\u60f3\u30fb\u89b3\u6e2c\u306a\u306e\u304b\u3092\u539f\u6587\u3067\u78ba\u8a8d\u3057\u3088\u3046\u306d\u2665'
            elif any(k in text for k in ['\u70ba\u66ff','\u5186\u5b89','\u5186\u9ad8','\u30c9\u30eb\u5186']):
                topic='\u70ba\u66ff';comment='\u70ba\u66ff\u304c\u52d5\u304f\u3068\u3001\u8f38\u51fa\u4f01\u696d\u3068\u8f38\u5165\u30b3\u30b9\u30c8\u306e\u5927\u304d\u3044\u4f01\u696d\u3067\u53d7\u3051\u6b62\u3081\u65b9\u304c\u5909\u308f\u308b\u3088\u3002\u81ea\u5206\u306e\u9298\u67c4\u306e\u6c7a\u7b97\u524d\u63d0\u3082\u78ba\u8a8d\u3067\u3059\u305e\u2665'
            elif any(k in text for k in ['\u6c7a\u7b97','\u5897\u76ca','\u6e1b\u76ca','\u696d\u7e3e','\u4e0a\u65b9\u4fee\u6b63','\u4e0b\u65b9\u4fee\u6b63']):
                topic='\u6c7a\u7b97\u30fb\u696d\u7e3e';comment='\u5229\u76ca\u306e\u5897\u6e1b\u3060\u3051\u3067\u306a\u304f\u3001\u4f1a\u793e\u4e88\u60f3\u3084\u5e02\u5834\u306e\u671f\u5f85\u3068\u306e\u9055\u3044\u3082\u898b\u3088\u3046\u306d\u3002\u4e00\u6642\u7684\u306a\u8981\u56e0\u304b\u3069\u3046\u304b\u3082\u539f\u6587\u3067\u30c1\u30a7\u30c3\u30af\u2665'
            elif any(k in text for k in ['\u534a\u5c0e\u4f53','AI','\u30c7\u30fc\u30bf\u30bb\u30f3\u30bf\u30fc']):
                topic='\u534a\u5c0e\u4f53\u30fbAI';comment='\u30c6\u30fc\u30de\u306e\u52e2\u3044\u306b\u52a0\u3048\u3066\u3001\u5b9f\u969b\u306e\u53d7\u6ce8\u3084\u5229\u76ca\u306b\u3064\u306a\u304c\u308b\u8a71\u304b\u3092\u78ba\u8a8d\u3057\u3088\u3046\u306d\u3002\u95a2\u9023\u9298\u67c4\u3059\u3079\u3066\u306b\u540c\u3058\u5f71\u97ff\u304c\u3042\u308b\u3068\u306f\u9650\u3089\u306a\u3044\u3088\U0001f440\u2728'
            elif any(k in text for k in ['\u539f\u6cb9','\u30a8\u30cd\u30eb\u30ae\u30fc','\u4e2d\u6771']):
                topic='\u8cc7\u6e90\u30fb\u30a8\u30cd\u30eb\u30ae\u30fc';comment='\u8cc7\u6e90\u4fa1\u683c\u306f\u3001\u8cc7\u6e90\u3092\u58f2\u308b\u4f01\u696d\u3068\u4f7f\u3046\u4f01\u696d\u3067\u5f71\u97ff\u304c\u5206\u304b\u308c\u308b\u3088\u3002\u30b3\u30b9\u30c8\u3092\u4fa1\u683c\u306b\u8ee2\u5ac1\u3067\u304d\u308b\u304b\u3082\u78ba\u8a8d\u3057\u3066\u306d\u2665'
            else:
                topic='\u5e02\u5834\u5168\u4f53';comment='\u3053\u306e\u30cb\u30e5\u30fc\u30b9\u304c\u81ea\u5206\u306e\u9298\u67c4\u306b\u3069\u3046\u95a2\u4fc2\u3059\u308b\u304b\u3001\u307e\u305a\u306f\u539f\u6587\u3092\u898b\u3066\u307f\u3088\u3046\u306d\u3002\u898b\u51fa\u3057\u306e\u52e2\u3044\u3060\u3051\u3067\u6025\u304c\u305a\u3001\u682a\u4fa1\u3068\u51fa\u6765\u9ad8\u3082\u4e00\u7dd2\u306b\u78ba\u8a8d\u2665'
            items.append({'title':title[:300],'link':link,'source':source or '\u914d\u4fe1\u5143\u4e0d\u660e','published_at':published.isoformat(),'topic':topic,'comment':comment})
            seen.add(title);sources[source]=sources.get(source,0)+1
        items.sort(key=lambda x:x['published_at'],reverse=True)
        result={'status':'ok','items':items[:30],'fetched_at':now.isoformat(),'method':'RSS headline/summary topic rules; article full text is not read'}
        NEWS_ADVICE_CACHE.update(result=result,ts=now.timestamp())
        return jsonify(result)
    except Exception:
        return jsonify(status='error',reason='\u30cb\u30e5\u30fc\u30b9\u3092\u53d6\u5f97\u3067\u304d\u307e\u305b\u3093\u3067\u3057\u305f\u3002\u6642\u9593\u3092\u7a7a\u3051\u3066\u518d\u5ea6\u8a66\u3057\u3066\u304f\u3060\u3055\u3044\u3002'),502

HTML = base64.b64decode(
    'PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9CgoucG9saWN5dGhlbWUgYXtmb250LXNpemU6MTJweH0KLmNvbnRyaWJncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQouY29udHJpYntib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDtiYWNrZ3JvdW5kOiNmZmZ9Ci5jb250cmliIHNwYW57ZGlzcGxheTpibG9ja30KLmNvbnRyaWIgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4O21hcmdpbi10b3A6MnB4fQoKLmNvbnRyaWIgc21hbGx7ZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweDtjb2xvcjojNmI3MjgwO2xpbmUtaGVpZ2h0OjEuMzV9Ci5mcmVzaGJveHtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMXB4O21hcmdpbi10b3A6OXB4O2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLmZyZXNoLW9re2JhY2tncm91bmQ6I2VjZmRmNTtib3JkZXItY29sb3I6I2E3ZjNkMH0KLmZyZXNoLXdhcm57YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1jb2xvcjojZmRlNjhhfQouZnJlc2gtc3RhbGV7YmFja2dyb3VuZDojZmVmMmYyO2JvcmRlci1jb2xvcjojZmVjYWNhfQoKLmZyZXNoYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweH0KLnNvdXJjZWJhZGdle2Rpc3BsYXk6aW5saW5lLWJsb2NrO3BhZGRpbmc6NHB4IDhweDtib3JkZXItcmFkaXVzOjk5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTJweDtmb250LXdlaWdodDo4MDA7bWFyZ2luLXJpZ2h0OjRweH0KLmhpc3Rvcnl3YXJue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDttYXJnaW4tdG9wOjdweDtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyOjFweCBzb2xpZCAjZmRlNjhhfQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5jb250cmliZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cgouc3RvY2stZGV0YWlsc3tib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxNHB4O21hcmdpbi10b3A6MTBweDtiYWNrZ3JvdW5kOndoaXRlfQouc3RvY2stZGV0YWlscz5zdW1tYXJ5e3BhZGRpbmc6MTZweDtjdXJzb3I6cG9pbnRlcjtmb250LXdlaWdodDo3MDA7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnN0b2NrLWRldGFpbHNbb3Blbl0+c3VtbWFyeXtib3JkZXItYm90dG9tOjFweCBzb2xpZCAjZTVlN2VifQouc3RvY2stZGV0YWlscz4uaG9sZGluZ3tib3JkZXI6MDttYXJnaW46MH0KLndhdGNoLWNvbnRlbnR7cGFkZGluZzoxMnB4fQoKLndhdGNoLXRpbWluZ3tkaXNwbGF5OmlubGluZS1ibG9jaztmb250LXNpemU6MTJweDtmb250LXdlaWdodDo3MDA7Ym9yZGVyLXJhZGl1czoyMHB4O3BhZGRpbmc6NXB4IDlweDttYXJnaW4tbGVmdDo2cHg7dmVydGljYWwtYWxpZ246bWlkZGxlfQoud2F0Y2gtYnV5e2JhY2tncm91bmQ6I2RjZmNlNztjb2xvcjojMTY2NTM0fS53YXRjaC1uZXV0cmFse2JhY2tncm91bmQ6I2YxZjVmOTtjb2xvcjojMzM0MTU1fS53YXRjaC1wZW5kaW5ne2JhY2tncm91bmQ6I2ZlZjNjNztjb2xvcjojOTI0MDBlfQoKLnJhZGVuLW5ld3Mtc2NlbmV7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTJweDttYXJnaW46MTJweCAwfS5yYWRlbi1hdmF0YXJ7d2lkdGg6MTEwcHg7aGVpZ2h0OjEzMHB4O29iamVjdC1maXQ6Y292ZXI7Ym9yZGVyLXJhZGl1czoxOHB4O2ZsZXgtc2hyaW5rOjB9LnJhZGVuLWJ1YmJsZXtiYWNrZ3JvdW5kOndoaXRlO2JvcmRlci1yYWRpdXM6MThweDtwYWRkaW5nOjE0cHg7cG9zaXRpb246cmVsYXRpdmU7ZmxleDoxO2JvcmRlcjoxcHggc29saWQgI2ZiY2ZlOH0ucmFkZW4tYnViYmxlOmJlZm9yZXtjb250ZW50OicnO3Bvc2l0aW9uOmFic29sdXRlO2xlZnQ6LTEwcHg7dG9wOjM1cHg7Ym9yZGVyLXRvcDoxMHB4IHNvbGlkIHRyYW5zcGFyZW50O2JvcmRlci1ib3R0b206MTBweCBzb2xpZCB0cmFuc3BhcmVudDtib3JkZXItcmlnaHQ6MTBweCBzb2xpZCB3aGl0ZX0ucmFkZW4tbmV3cy1jYXJke2JvcmRlci10b3A6MXB4IHNvbGlkICNmYmNmZTg7cGFkZGluZzoxNHB4IDA7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0ucmFkZW4tbmV3cy1jb21tZW50e2JhY2tncm91bmQ6d2hpdGU7Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTJweDtsaW5lLWhlaWdodDoxLjg7bWFyZ2luLXRvcDoxMHB4fUBtZWRpYShtYXgtd2lkdGg6NDAwcHgpey5yYWRlbi1hdmF0YXJ7d2lkdGg6ODJweDtoZWlnaHQ6MTEwcHh9LnJhZGVuLWJ1YmJsZXtwYWRkaW5nOjEwcHh9fQo8L3N0eWxlPgo8L2hlYWQ+Cjxib2R5Pgo8bWFpbj4KPGRpdiBjbGFzcz0idG9wIj4KICA8aDE+8J+TiCDml6XmnKzmoKpBSSBGUkVFPC9oMT4KICA8ZGl2IGNsYXNzPSJzdWIiPuimgeWboOWIpemuruW6piAvIEZpbk1pbmTkvqHmoLzlsaXmrbQgLyBKLVF1YW50c+axuueulyAvIOWbveetljwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn46vIOmKmOafhOWIhuaekDwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZCI+CiAgICA8aW5wdXQgaWQ9ImNvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSDkvosgNzIwMyI+CiAgICA8aW5wdXQgaWQ9InByaWNlIiBwbGFjZWhvbGRlcj0i5Y+W5b6X57WC5YCkIiByZWFkb25seT4KICA8L2Rpdj4KICA8ZGl2IGlkPSJjb21wYW55TmFtZSIgY2xhc3M9InNvdXJjZSBtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZnjgovjgajkvJrnpL7lkI3jgpLooajnpLrjgZfjgb7jgZk8L2Rpdj4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGEgaWQ9ImthYnV0YW4iIGNsYXNzPSJidG4gc2Vjb25kYXJ5IiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+5qCq5o6i44Gn56K66KqNPC9hPgogICAgPGJ1dHRvbiBpZD0iYW5hbHl6ZUJ0biIgb25jbGljaz0iYW5hbHl6ZSgpIj7lrp/jg4fjg7zjgr/jgafliIbmnpA8L2J1dHRvbj4KICA8L2Rpdj4KICA8cCBjbGFzcz0ibXV0ZWQiPumKmOafhOOCs+ODvOODieOCkuWFpeOCjOOBpuaKvOOBmeOBqOOAgeePvuWcqOWApOOBqDIw5pel44O7MTI25pel44O7MjUy5pel44Gu5L6h5qC85bGl5q2044GvRmluTWluZOaXpeacrOagquaXpei2s+OCkuacgOWEquWFiOOBl+OBvuOBmeOAgumKmOafhOWQjeODu+axuueul+ODu+WbveetluWIpOWumuOBr0otUXVhbnRz562J44KS5L2/55So44GX44G+44GZ44CCPC9wPgogIDxkaXYgaWQ9InNvdXJjZUJveCIgY2xhc3M9InNvdXJjZSBtdXRlZCI+44OH44O844K/5pyq5Y+W5b6XPC9kaXY+CiAgPGRpdiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJtYXJnaW4tdG9wOjdweCI+CiAgICA8Yj7wn4aTIOeEoeaWmeS+oeagvOWxpeattOOBq+OBpOOBhOOBpjwvYj4KICAgIDxkaXYgY2xhc3M9Im11dGVkIj5GaW5NaW5k44Gu5pel5pys5qCq5pel6Laz44KS57SEOTAw5pel5YiG5Y+W5b6X44GX44CB5pyA5paw5Za25qWt5pel44Gu57WC5YCk44O7MjAvMTI2LzI1MuaXpeODquOCv+ODvOODs+ODuzIw5pel6auY5a6J44O75Ye65p2l6auYcHJveHnjg7vjg63jg7zjg6rjg7PjgrDlrp/nuL7liIbluIPjgpLoqIjnrpfjgZfjgb7jgZnjgILlj5blvJXkuK3jga7jg6rjgqLjg6vjgr/jgqTjg6DkvqHmoLzjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzQm94IiBjbGFzcz0iaGlzdG9yeXdhcm4iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPGI+8J+TmiDkvqHmoLzliIbmnpDlsaXmrbTjga7prq7luqY8L2I+CiAgICA8ZGl2IGlkPSJoaXN0b3J5RnJlc2huZXNzVGV4dCIgY2xhc3M9Im11dGVkIj48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJmcmVzaG5lc3NCb3giIGNsYXNzPSJmcmVzaGJveCBmcmVzaC13YXJuIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxiIGlkPSJmcmVzaG5lc3NUaXRsZSI+44OH44O844K/6a6u5bqmPC9iPgogICAgPGRpdiBpZD0iZnJlc2huZXNzRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5OKIOagquS+oeODu+ODhuOCr+ODi+OCq+ODq+Wun+e4vjwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6aiw6JC9546HICU8L3NwYW4+PGlucHV0IGlkPSJyMjAiIHJlYWRvbmx5PjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPjEyNuaXpSAlPC9zcGFuPjxpbnB1dCBpZD0icjEyNiIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjUy5pelICU8L3NwYW4+PGlucHV0IGlkPSJyMjUyIiByZWFkb25seT48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpemrmOWApDwvc3Bhbj48YiBpZD0iaGlnaDIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlronlgKQ8L3NwYW4+PGIgaWQ9ImxvdzIwIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XlubTnjofjg5zjg6k8L3NwYW4+PGIgaWQ9InZvbDIwIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfp6kg5a6f44OH44O844K/6KaB5ZugPC9oMz4KICA8cCBjbGFzcz0ibXV0ZWQiPuS+oeagvOODu+mcgOe1puODu+axuueul+ODu+WbveetluOBr+OBneOCjOOBnuOCjOWIpeOBq+muruW6puOCkuWIpOWumuOBl+OBvuOBmeOAguWPpOOBhOimgeWboOOBr+WApOOCkua2iOOBleOBmuOAgee3j+WQiOeCueOBuOOBrumHjeOBv+OBoOOBkeiHquWLleOBp+S4i+OBkuOBvuOBmeOAgjwvcD4KICA8ZGl2IGNsYXNzPSJmYWN0b3JncmlkIj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7msbrnrpc8L3NwYW4+PGIgaWQ9ImVhcm5BdXRvIj7igJQ8L2I+PHNtYWxsIGlkPSJlYXJuRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWPluW+lzwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZyA57WmcHJveHk8L3NwYW4+PGIgaWQ9InN1cHBseUF1dG8iPuKAlDwvYj48c21hbGwgaWQ9InN1cHBseURldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrlj5blvpc8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetlnByb3h5PC9zcGFuPjxiIGlkPSJwb2xpY3lTdGF0ZSI+4oCUPC9iPjxzbWFsbCBpZD0icG9saWN5RGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuWFrOW8j+aUv+etluOCveODvOOCueeiuuiqjeWJjTwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5a6f5Yq544OH44O844K/5YWF6LazPC9zcGFuPjxiIGlkPSJjb3ZlcmFnZSI+4oCUPC9iPjxzbWFsbCBjbGFzcz0ibXV0ZWQiPumuruW6puOBvuOBp+WPjeaYoOOBl+OBn+mHjeOBvzwvc21hbGw+PC9kaXY+CiAgPC9kaXY+CiAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDZweCI+8J+VkiDopoHlm6DliKXjga7prq7luqY8L2g0PgogIDxkaXYgY2xhc3M9ImZhY3RvcmdyaWQiPgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS+oeagvOWxpeattDwvc3Bhbj48YiBpZD0ibWFya2V0RnJlc2giPuKAlDwvYj48c21hbGwgaWQ9Im1hcmtldEZyZXNoRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWIpOWumjwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5rG6566XPC9zcGFuPjxiIGlkPSJlYXJuRnJlc2giPuKAlDwvYj48c21hbGwgaWQ9ImVhcm5GcmVzaERldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrliKTlrpo8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWbveetljwvc3Bhbj48YiBpZD0icG9saWN5RnJlc2giPuKAlDwvYj48c21hbGwgaWQ9InBvbGljeUZyZXNoRGV0YWlsIiBjbGFzcz0ibXV0ZWQiPuacquWIpOWumjwvc21hbGw+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6a6u5bqm5Y+N5pig5b6M44Kr44OQ44O8546HPC9zcGFuPjxiIGlkPSJmcmVzaENvdmVyYWdlIj7igJQ8L2I+PHNtYWxsIGNsYXNzPSJtdXRlZCI+5Y+k44GE6KaB5Zug44Gv6YeN44G/44KS5rib6KGwPC9zbWFsbD48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJwb2xpY3lUaGVtZXMiIGNsYXNzPSJwb2xpY3l0aGVtZXMiPjwvZGl2PgogIDxkaXYgc3R5bGU9Im1hcmdpbi10b3A6OXB4Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Zu9562W44K544Kz44Ki5LiK5pu444GN77yI5Lu75oSP77yJIC0xMDDjgJwxMDA8L3NwYW4+CiAgICA8aW5wdXQgaWQ9InBvbGljeSIgaW5wdXRtb2RlPSJkZWNpbWFsIiBwbGFjZWhvbGRlcj0i56m65qyE44Gq44KJ6Ieq5YuV5Zu9562WcHJveHnjgpLkvb/nlKgiPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+4oC75Zu9562WcHJveHnjga/jgIHmlL/lupzlhazlvI/mlL/nrZbjgr3jg7zjgrnjgahKLVF1YW50c+OBrualreeoruODu+S8muekvuWQjeOBqOOBrumWoumAo+W6puOCkue1hOOBv+WQiOOCj+OBm+OBn+WPguiAg+WApOOBp+OBmeOAguODqeOCpOODlueiuuiqjeOBp+OBjeOBquOBhOWgtOWQiOOBr+acgOe1gueiuuiqjea4iOOBv+aDheWgseOCkuS9juS/oemgvOW6puOBp+S9v+eUqOOBl+OAgeOBneOBrueKtuaFi+OCgueUu+mdouOBq+aYjuekuuOBl+OBvuOBmeOAguijnOWKqemHkeaOoeaKnuOChOalree4vuaBqeaBteOBneOBruOCguOBruOCkuiovOaYjuOBmeOCi+aMh+aomeOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+noCDliIbmnpDntZDmnpw8L2gzPgogIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nirbmhYs8L3NwYW4+PGIgaWQ9InN0YXRlIj7igJQ8L2I+PC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OX44Op44K55qC55ougPC9zcGFuPjxiIGlkPSJwb3MiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg57jgqTjg4rjgrnmoLnmi6A8L3NwYW4+PGIgaWQ9Im5lZyI+4oCUPC9iPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InNjb3JlSGVybyIgY2xhc3M9InNjb3JlaGVybyIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+WQiOaOoeeCue+8iOWPluW+l+ODh+ODvOOCv+ODu+ODq+ODvOODq+ODmeODvOOCue+8iTwvc3Bhbj4KICAgIDxiIGlkPSJzY29yZTEwMCI+4oCUPC9iPgogICAgPHNwYW4gaWQ9InNjb3JlU3RhdGVQaWxsIiBjbGFzcz0ic3RhdGVwaWxsIHN0YXRlLW5ldXRyYWwiPuKAlDwvc3Bhbj4KICAgIDxkaXYgaWQ9InNjb3JlQnJlYWtkb3duIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0ic2NvcmVSZWFzb24iIGNsYXNzPSJzY29yZS1yZWFzb24iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPHN0cm9uZz7wn6etIOOBquOBnOOBk+OBrueCueaVsO+8nzwvc3Ryb25nPgogICAgPGRpdiBpZD0ic2NvcmVSZWFzb25UZXh0IiBjbGFzcz0ibXV0ZWQiPuKAlDwvZGl2PgogICAgPGRpdiBpZD0iZHJpdmVyR3JpZCIgY2xhc3M9ImRyaXZlcmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbnRyaWJ1dGlvbkJveCIgY2xhc3M9InNjb3JlLXJlYXNvbiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3Ryb25nPvCfp64g57eP5ZCI54K544G444Gu5a+E5LiOPC9zdHJvbmc+CiAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5Y+W5b6X44Gn44GN44Gf6KaB5Zug44Gr6a6u5bqm5L+C5pWw44KS5o6b44GR44Gm44GL44KJ6YeN44G/44KS5YaN6YWN5YiG44GX44CB5ZCE6KaB5Zug44GM57eP5ZCI6KmV5L6h44KS44Gp44KM44Gg44GR5oq844GX5LiK44GS77yP5oq844GX5LiL44GS44Gf44GL44KS6KGo56S644GX44G+44GZ44CCPC9kaXY+CiAgICA8ZGl2IGlkPSJjb250cmlidXRpb25HcmlkIiBjbGFzcz0iY29udHJpYmdyaWQiPjwvZGl2PgogIDwvZGl2PgogIDxwIGlkPSJyZXN1bHQiIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl5Yqb44GX44Gm44CM5a6f44OH44O844K/44Gn5YiG5p6Q44CN44KS5oq844GX44Gm44GP44Gg44GV44GE44CCPC9wPgogIDxwIGNsYXNzPSJtdXRlZCI+5o6h54K55biv77yaODDjgJwxMDAg5by35rCXIC8gNjXjgJw3OSDjgoTjgoTlvLfmsJcgLyA0NeOAnDY0IOS4reeriyAvIDMw44CcNDQg44KE44KE5byx5rCXIC8gMOOAnDI5IOW8seawlzwvcD4KICA8ZGl2IGlkPSJhbmFseXNpc0V2IiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfmqgg5LuK5pel44Gu5YSq5YWI44Ki44Kv44K344On44OzPC9oMz4KICA8ZGl2IGlkPSJwcmlvcml0eUFjdGlvbnMiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CjxoMz7wn5OIIOaXpeacrOagquOBruazqOebruODu+S4i+iQveitpuaIku+8iOeEoeaWmeW3oeWbnu+8iTwvaDM+CjxwIGNsYXNzPSJtdXRlZCI+5a++6LGh44GvRmluTWluZOOBruaXpeacrOagqumKmOafhOS4gOimp++8iC5U77yJ44CC5L+d5pyJ44O744Km44Kp44OD44OB55m76Yyy44Gv5LiN6KaB44Gn44GZ44CC54Sh5paZ5p6g44GnMjDpipjmn4TjgZrjgaToqr/jgbnjgIHlj5blvpfjgafjgY3jgZ/nr4Tlm7LjgYvjgonlgJnoo5zjgpLooajnpLrjgZfjgb7jgZnjgILlhajluILloLTjgpLlkIzmmYLjgavmr5TovIPjgZfjgZ/jg6njg7Pjgq3jg7PjgrDjgafjga/jgYLjgorjgb7jgZvjgpPjgILlkITmrITjga/mnIDlpKc1MOS7tuOCkuihqOekuuOBl+OBvuOBmeOAguaXpei2s+e1guWApOOBp+WIpOWumuOBl+OBvuOBmeOAgjwvcD4KPGxhYmVsIGZvcj0ibWFya2V0R2VucmUiPuiqv+OBueOCi+OCuOODo+ODs+ODqzwvbGFiZWw+CjxzZWxlY3QgaWQ9Im1hcmtldEdlbnJlIiBvbmNoYW5nZT0iY2hhbmdlTWFya2V0R2VucmUoKSIgc3R5bGU9IndpZHRoOjEwMCU7cGFkZGluZzoxMnB4O2JvcmRlcjoxcHggc29saWQgI2RkZDtib3JkZXItcmFkaXVzOjEycHg7bWFyZ2luOjhweCAwIj48b3B0aW9uIHZhbHVlPSLlhajmpa3nqK4iPuWFqOalreeorjwvb3B0aW9uPjwvc2VsZWN0Pgo8YnV0dG9uIGlkPSJtYXJrZXRHZW5yZUxvYWQiIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9ImxvYWRNYXJrZXRHZW5yZXMoKSIgc3R5bGU9Im1hcmdpbi1ib3R0b206OHB4Ij7jgrjjg6Pjg7Pjg6vkuIDopqfjgpLoqq3jgb/ovrzjgoA8L2J1dHRvbj4KPHAgY2xhc3M9Im11dGVkIj7lj5blvpflhYPjga7mpa3nqK7jgpLkvb/jgaPjgZ/ni6zoh6rjga7liIbpoZ7jgafjgZnjgIJBSeODu+WbveetluOBquOBqeOBruODhuODvOODnuWIhumhnuOBqOOBr+eVsOOBquOCiuOBvuOBmeOAguOCuOODo+ODs+ODq+OBlOOBqOOBq+e2muOBjeOBi+OCieW3oeWbnuOBp+OBjeOBvuOBmeOAgjwvcD4KPGJ1dHRvbiBpZD0ibW92ZW1lbnRCdG4iIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hNb3ZlbWVudCgpIj7mrKHjga4yMOmKmOafhOOCkuiqv+OBueOCizwvYnV0dG9uPgo8YnV0dG9uIGlkPSJtb3ZlbWVudEFsbEJ0biIgb25jbGljaz0icmVmcmVzaE1vdmVtZW50KHRydWUpIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumBuOaKnuOCuOODo+ODs+ODq+OCkuWFqOmDqOiqv+OBueOCizwvYnV0dG9uPgo8cCBjbGFzcz0ibXV0ZWQiPjHlm57mirzjgZnjgajpgbjmip7jgrjjg6Pjg7Pjg6vjgpLlhYjpoK3jgYvjgonmnIDlvozjgb7jgafoh6rli5Xlt6Hlm57jgZfjgb7jgZnjgILnhKHmlpnmnqDjgavlkIjjgo/jgZvntIQxM+enkuS7peS4iuOBmuOBpOmWk+malOOCkuepuuOBkeOBvuOBmeOAgjEwMOmKmOafhOOBquOCieW+heOBoeaZgumWk+OBoOOBkeOBp+e0hDIy5YiG44GL44GL44KK44G+44GZ44CC5beh5Zue5Lit44Gv44GT44Gu44Oa44O844K444KS6ZaL44GE44Gf44G+44G+44Gr44GX44Gm44GP44Gg44GV44GE44CCPC9wPgo8YnV0dG9uIGlkPSJtb3ZlbWVudFN0b3AiIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InN0b3BNb3ZlbWVudCgpIiBkaXNhYmxlZCBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPuW3oeWbnuOCkuWBnOatojwvYnV0dG9uPgo8cCBpZD0ibW92ZW1lbnRDb3ZlcmFnZSIgY2xhc3M9Im11dGVkIj48L3A+CjxwIGlkPSJtb3ZlbWVudFN0YXR1cyIgY2xhc3M9Im11dGVkIiByb2xlPSJzdGF0dXMiIGFyaWEtbGl2ZT0icG9saXRlIj7mnKrmm7TmlrDjgILlj5blvpfjgZfjgZ/jg4fjg7zjgr/jga/jgZPjga7jg5bjg6njgqbjgrbjgavkv53lrZjjgZfjgb7jgZnjgII8L3A+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7wn4yfIOacrOaXpeOBruazqOebruWAmeijnO+8iOacgOaWsOWPluW+l+aXpeODmeODvOOCue+8iSA8c3BhbiBpZD0ibW92ZW1lbnRVcENvdW50Ij48L3NwYW4+PC9zdW1tYXJ5PjxkaXYgY2xhc3M9IndhdGNoLWNvbnRlbnQiIGlkPSJtb3ZlbWVudFVwIj48L2Rpdj48L2RldGFpbHM+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7imqDvuI8g5LiL6JC96K2m5oiS6YqY5p+EIDxzcGFuIGlkPSJtb3ZlbWVudERvd25Db3VudCI+PC9zcGFuPjwvc3VtbWFyeT48ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50IiBpZD0ibW92ZW1lbnREb3duIj48L2Rpdj48L2RldGFpbHM+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7jgZ3jga7ku5bjg7vliKTlrprkv53nlZkgPHNwYW4gaWQ9Im1vdmVtZW50T3RoZXJDb3VudCI+PC9zcGFuPjwvc3VtbWFyeT48ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50IiBpZD0ibW92ZW1lbnRPdGhlciI+PC9kaXY+PC9kZXRhaWxzPgo8cCBjbGFzcz0ibXV0ZWQiPuS7ruODq+ODvOODq++8muebtOi/kTHllrbmpa3ml6XvvIsxJeS7peS4iuOBi+OBpDXllrbmpa3ml6Xjg5fjg6njgrnjgpLms6jnm67lgJnoo5zjgIHiiJIxJeS7peS4i+OBi+OBpDXllrbmpa3ml6Xjgb7jgZ/jga8yMOWWtualreaXpeODnuOCpOODiuOCueOCkuS4i+iQveitpuaIkuOBqOOBl+OBvuOBmeOAguWHuuadpemrmOWil+WKoOOBr+ijnOWKqeihqOekuuOAguS4i+iQveS6iOa4rOODu+Wjsuiyt+aOqOWlqOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAguWIhuWJsuiqv+aVtOOBruOBquOBhOWxpeattOOBr+WApOWLleOBjeOBjOatquOCgOWgtOWQiOOBjOOBguOCiuOBvuOBmeOAgjwvcD4KPC9kaXY+CjxkaXYgY2xhc3M9ImNhcmQgcG9ydGZvbGlvIj4KICA8aDM+8J+nrSDjg53jg7zjg4jjg5Xjgqnjg6rjgqrlhajkvZM8L2gzPgogIDxkaXYgaWQ9InBvcnRmb2xpb1N1bW1hcnkiPgogICAgPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkrwg5L+d5pyJ5qCq44O75pCN5YiH44KKL+WIqeeiujwvaDM+CiAgPGRpdiBjbGFzcz0ibm90ZSI+CiAgICDnj77lnKjlgKTjgajkvqHmoLzliIbmnpDlsaXmrbTjga9GaW5NaW5k5pel6Laz44KS5YSq5YWI44GX44Gm44CB5pCN55uK44O75pCN5YiH44KK6Led6Zui44O75Yip56K66Led6Zui44O744OI44Os44O844Oq44Oz44Kw44O755+t5Lit6ZW35a6f57i+44O744Od44O844OI44OV44Kp44Oq44Kq6KmV5L6h44KS6Ieq5YuV5YaN6KiI566X44GX44G+44GZ44CCRmluTWluZOWPluW+l+WkseaVl+aZguOBoOOBkeS7luOCveODvOOCueOBuOODleOCqeODvOODq+ODkOODg+OCr+OBl+OBvuOBmeOAggogIDwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Im1hcmdpbi10b3A6OXB4Ij4KICAgIDxidXR0b24gaWQ9InJlZnJlc2hBbGxCdG4iIGNsYXNzPSJzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hBbGxIb2xkaW5ncyh0cnVlKSI+5L+d5pyJ5qCq44KS5pyA5paw57WC5YCk44Gn5LiA5ous5pu05pawPC9idXR0b24+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZGluZ1JlZnJlc2hTdGF0dXMiIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbjo3cHggMnB4IDAiPuS/neacieagquOBruiHquWLleabtOaWsOOBrzMw5YiG44GU44Go44Gr5pyA5aSnMeWbnuOBp+OBmeOAgjwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPgogICAgPGlucHV0IGlkPSJob2xkQ29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxpbnB1dCBpZD0iaG9sZENvc3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgcGxhY2Vob2xkZXI9IuWPluW+l+WNmOS+oSI+CiAgPC9kaXY+CiAgPGRpdiBpZD0iaG9sZENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NnB4IDJweCAwIj7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGlucHV0IGlkPSJob2xkU2hhcmVzIiBpbnB1dG1vZGU9Im51bWVyaWMiIHBsYWNlaG9sZGVyPSLmoKrmlbAiPgogICAgPHNlbGVjdCBpZD0iZmVlTW9kZSI+CiAgICAgIDxvcHRpb24gdmFsdWU9Im5vbXVyYV9uZXQiPumHjuadkeOCquODs+ODqeOCpOODs+WwgueUqOaUr+W6l+ODu+ePvueJqTwvb3B0aW9uPgogICAgICA8b3B0aW9uIHZhbHVlPSJub25lIj7miYvmlbDmlpnjgarjgZfvvIjmr5TovIPnlKjvvIk8L29wdGlvbj4KICAgIDwvc2VsZWN0PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiiAlPC9zcGFuPjxpbnB1dCBpZD0ic3RvcFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iOCI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K6ICU8L3NwYW4+PGlucHV0IGlkPSJ0YWtlUGN0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHZhbHVlPSIxNSI+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844OrICU8L3NwYW4+PGlucHV0IGlkPSJ0cmFpbFBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iNyI+PC9kaXY+CiAgPC9kaXY+CiAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRIb2xkaW5nKCkiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPuWun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmDwvYnV0dG9uPgogIDxwIGNsYXNzPSJtdXRlZCI+6YeO5p2R44ON44OD44OI77yG44Kz44O844Or77yP44G744Gj44Go44OA44Kk44Os44Kv44OI44Gu5Zu95YaF54++54mp44O744Kq44Oz44Op44Kk44Oz5rOo5paH44Gu56iO6L685omL5pWw5paZ6KGo44KS5L2/55So44CCPC9wPgogIDxkaXYgaWQ9ImhvbGRpbmdzIj48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+RgCDjgqbjgqnjg4Pjg4Hjg6rjgrnjg4g8L2gzPgogIDxidXR0b24gaWQ9IndhdGNoQnVsa0J0biIgY2xhc3M9InNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaEFsbFdhdGNoKCkiPuiyt+OBhOaZguOBruWPguiAg+OCkuS4gOaLrOabtOaWsDwvYnV0dG9uPgogIDxwIGlkPSJ3YXRjaEJ1bGtTdGF0dXMiIGNsYXNzPSJtdXRlZCIgcm9sZT0ic3RhdHVzIiBhcmlhLWxpdmU9InBvbGl0ZSI+5YWo6YqY5p+E44KS6aCG55Wq44Gr5YiG5p6Q44GX44G+44GZ44CC6YqY5p+E44GU44Go44Gr57SEMTPnp5Ljga7plpPpmpTjgpLnqbrjgZHjgb7jgZnjgII8L3A+CiAgPGRpdiBjbGFzcz0icm93Ij4KICAgIDxpbnB1dCBpZD0id2F0Y2hDb2RlIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIj4KICAgIDxidXR0b24gb25jbGljaz0iYWRkV2F0Y2goKSI+6L+95YqgPC9idXR0b24+CiAgPC9kaXY+CiAgPGRpdiBpZD0id2F0Y2hDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjRweCAycHggOHB4Ij7pipjmn4TlkI3vvJrigJQ8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaHMiPjwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiIHN0eWxlPSJiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCgxMzVkZWcsI2ZmZjdlZCwjZmRmMmY4KTtib3JkZXI6MXB4IHNvbGlkICNmYmNmZTgiPgo8aDM+8J+QmiDnt4/lkIjjg4vjg6Xjg7zjgrnjgYvjgonpipjmn4Tjg4Hjgqfjg4Pjgq88L2gzPgo8cCBpZD0icmFkZW5BZHZpY2VEYXRlIiBjbGFzcz0ibXV0ZWQiPjwvcD4KPGRpdiBjbGFzcz0icmFkZW4tbmV3cy1zY2VuZSI+PGltZyBzcmM9ImRhdGE6aW1hZ2Uvd2VicDtiYXNlNjQsVWtsR1JpalJBQUJYUlVKUVZsQTRJQnpSQUFCUWh3S2RBU3JnQWVBQlBsRWdqVVVqb2lFVktvYVFPQVVFc3JacDVGY0FiSUd2ZlZhbmZyNzY3cEFlWWZRVCtObGI3bWU0L090Znc5TVg5dzlTbis3ZW1YMDRmOEwwUytidjZmdjdqNmp2OWo5THIxbFBSbTg1MzFrdjd0LzUvUzk5UUQvLyszYnozL2JyekQrVHY3ZjhzZk9YeXBlOS8zLy9QZjkzL0pmSS85Ny85ditwOGRucWY5Ti83LzkxL3NQWXYrV2ZoeitIL2hmM20rTEg5bC82UDgvNCsvTkQvZzlRajgxL3JYL0Qvdy83eWY1bjkyZnZKL0svYkwrN2VGWjFYL00vOVBxSGZFWDRUL2svNTM4blBnTSswLzczK3I5YWZ0VC8zZjlOOEF2OUMvdVAvQy93Lzd6LzRuLy8vWkgvTThaejh0L3Z2KzEvcC95bSt3citpZjNyL2xmNUgvVC9zMzhvWC9mL3NmOXgrN1B1bitvdi9UL28vOXA4aVA4Ly91bi9VL3h2K2svYXIvLy9Xai8vL2NqKzVuLzQvNGZ3di9zcC85RU0vK1pTenhvZWEwTzI1SjlPN2VKcmkzencvL1NBcjlzbStMY0Vldk52bGdJQW5BdnNFL3lXRkhNNjlWK2JLK2FJRXNnRGIrcjAraUdpZDJ1WTJyekU4a01yMUp2MkZVYnkwOURTY201T1V5cm85R29XM282R2lGU3p6Y21rcFRIc1lObDVZU096d25zUjNwMjhnQnc4WkhjVU5DSzMwTnlKWld3S0xLeDhYRWFPaDErSkVIYm40bEtYVUN2L3IycWRBbUc1ODZHMUhYdWtqZDY0aTcwbTBWNGFNR0N5NGt0MmZsbkt4K0ZCQXRFb1AySnFFcmlLalEzZnQ4akk2eStUN3VmME1mMDE4SGxGOWdmYWJUVmN3Vi8yd3RPaUswaGNEdG5MN1E0REtoT1g3Z1pQUFl3aXpzclZCNGpjMnlkLzNkWTNPZVEwMDVXajZSOG9sb2ptZ1J6aCs0U3Q3TU92Mmg4S0VPS3BhSXJkZStaajRUSnlrT2FlRndSaU5WMWRFTHNka2UzYzZ3L1J6K3V1Sk8yS1dEVzkwa2dVV1NYZSthT01iZHJLNG4vakVWT2Z4SjZ5WkJnYkhvZGpuckQ4RXVnWW8xeGpkUDFaRkw0WnhnbE9hYVdMaG9meVgwaVNPZE90NlkzNXNjRTBzbG1QYlR6Ti9kTHNyMEN4VURxQ0xyWGtraDAza0UybyszQ0QzNk40SjlzK0VKejg0NnFkdmJnNzFUdmQ2OXh0SlB6VGNlLzdvVHRQZC9jY1ppS0p4dldHTlhGM0twMkhPcWhkMkhDcXdSUGp5Zm0xN0p5cWpyS2E2NEdQZmhpSUMrN2REbDVTUWw0QTRwbitkYVNTS1puUUtjOXgyeUgrcklMcHJFM2F6UFNIT21HOFlsTE9HK1NRbzl6M3hGeTV1YzNXRjkzMG1iUDZkOUtNWUhMRWluMHFqYi9maFJYN2JvdGZwSStNc2llekpGZUdYM0V4SXlxeEZYL0dSeEVpenJCUGxEbG9SRzUyWXo1WmJCUlo3ZjYzZjV6c015bWdhWTdjaTg3ZzZXWXdqSS9xelMvTmxqM0VhTmpVVEhSZXBoMG1pd1A1THg4d1UzcUhqMlB3cGtwNTNiVFR0Y1Z4MWJST2xxWmZ2NGZzQXZmSUNJeFJHMTkwRU9iSURuSW1UTUY5SnUvRDZhWXN6SjhPSGNvcW1mUGk0c1R2TUNQTGdSSjZFWHJtNUFrSHdRREJ5aDNnY2xDb0VMbU1tcGkwVjd2U1ZBV2grSGJKWVUxRzdtZDljNjhoQ0NhSllNMGU3cUV1K2UrUEVUakt5OVpLMXM1V0VXODg0TmtjMzVxRm4zNGhFeGpSOVk5bllmL3prVFZEMkNOMVFZYWdybENVcDJpRDk1RjlpWlYrWXh6Q1dGTWpvTk9LMnJHQTFTaWw1cnR2ZGV1ay9pYXlhZU5Kc3VMKzJLYnJldWRVSVhFQlFpY3dqaHhyOVl6S2crYVJjNldTU21uTnRUM2cvRGtOMExPNk9mTXF5ZjdWREhiVnhpQ1c2TU4rVy8xSE0raGRCTHlyZnMwcGRyN2xadlVsNm5YUUVQc251WGMwQWVRajRuT2VoQyt4aDhlRThHYU5vU01BOU5KV1c1V3hDUnhidCtDblVjL2Y5dzNvZUNuZzM2L0pJNlluc1VTYmNyd2VBdERibmNLZEhzRjlZTFN0YVAzYW5wTjJQQUFFaUVhM284SUcyY05rQlNrUGlNeU9zWWRoOU80SGNqdlg3VGRqemFWc3owQ3pGcTJwQmtBZGZRMDJXNjh5MFlNcnJsQS91MGRKVVBLZFRGUEZuL0x4dFIrOVFtS2dCSmRKaXdId01DeDlFMHlEU3RLVzRkOU56bnhjc2NkOFEyYjNnTzFqSkNwOXEwM2xPUFRkY2hJbnlmM0hhM3R4OFNpeVNyRFZwU0hSem5panRmdjhnN2ZzcVRNbU1sMC9QOFR2YXdFTkJzSFZHbkRtejJFQzZ2ODBJaEM3WmM1dzhuZlNOcE9UdGRpRDVPUSt0OXU2K21GdVJsdlpFN2dCNnpYa1ZKMXU4YWdYRDlBWWtxVG5ibHFWblBmN0U0cUJ2ZCtYMS95aVFIeWVKRkpLL1pSbjdrYkczUlh5bUtMUDRkMlh6RTZQcm9vMGxKNTg0L0lIMG0zSmpJR24vZXpmbFhxSHNMczNrYXgzNnNFL2VrNHBzQWZJOTNIV0hUVzA1OWF0RTA5S09aL2hjQlFqVDdpMUM4VjRyRHhOYmRRcUVHTmZ6SHF2bG0wMnExZkRhUXZHOHdoZVpMV0FMYjFjYjZhRlQvdU8yVWtYZlF4Z1YyZzhEdVc0dVR4Q0FDN3FJMWxOWDYzZUpEVXpwbjBVdXZ1RVJZTDhrWHdvYVd6czJJajJncEkyTHdFbThMdDlNeHdhaVNmTWIyTlhJVTVKYmpFOWM5dUJIRWNsYmIxYTUxZjVENzdpd094dDYwSUdSQmsvSVBOUlJaRitBMzl0eUhaMlpjT0lQRjRqOElFV1NJSjBCUEZQcUdOSm9QR1BEdHV2Yk9jeHU3UkxNY2YwbEY3SkEvQzUwWXZ4TTBDV2tTTVM3QUxxUmtxTnlTdTg3RnRzTjh1WVJwbVhSVE9YdUVwT1hHV2FFM0VSazYxWWYvT2ZleTRDcDArbnkrTnhsQlU4T0NsZytoKzk4QVgwR3dVSVJKQjhwTWs4UDkvdzg5UThJeWdvM042QmJjNzhnMXhTd2MxdjFPL2xUcEdIRDRnVHZtREVFclQ0QWkvSGI5K1oxL0FyQ2VZZVpZSGRRQXE0NHBUK01BNldZay9GTTR2QmFjYmxXOW9kb2V2TkhWVWJJdlNLUk9JZlYvenB2dmd5cVlZYTZsUFJLL1lVN1hybTgxTGw2ejRCL3ZYUGtBb3dFUFNuMkJMY1FBeXp3RDJJempJVGRoMWNmVUJnR29Yb0N4N2l4NVdKcEw5N3lIbzhVL0xyYldmRzI1ZGpTbFhTL3N1QXBzdVB0VlVJOVZiQ1FPRngwaS84djkxWU5QenVkZnZRaG5ZeUJQVXpOVVRwY3V6TjZSRDlBUG9VdGgxNXdxbVZ6RFlhVXRBNEswVDN4STI3YTE2YTRpUUd5bGVwcEE2SDV1SkQxS3c5K1JrOWk4QU96d2x0Z3BoVC9QamRMaGFnMVN6dUxJMDBwbVlUSk02S0ZUT3l6OVE5NWgvaElaak5FVGYxUzZ1aG41cHZ1RXhNN21mWnZZdWlsRDVQeXpFUHJpc2NRWVlLNHBKUG5WRFdUMUh1SVozT2RraVl1NGZHTncxNitGVTYza0hQZWU4S2RyZXQ3cUhsK2Zhb08zT1Z6amVmVjdXaHZ2Q1JRcnJTZWhuQVJOMnpKVVovazM4cnlyeng4bGhicWNwK3hEeWlleFV2VWQ3OFV0WFM1Mk5mUjlMa3Z0bi9QUVg0OFNYZmtYc1o3andOOEFCN2ZhZ29YR29WUU53RE8xREYveDBzcWw3b0pLNXRCQkhxR3pqTTVjZmZ4MjNnUlNGZC9JbjhzaGVycnQ1K2x3VkgxMEsxbnRzd3FXT1ltem9DRVNxaWhka0YvTkZoWlR4YWZtb3daY3puUkFsSkx2SnhSRG9PYzdGcmtFQkdta1YxMTFoU001cmNiQkpqQml1YlNxTTY3SFdQYjdxYmpxdkNGVUpFb2psSVVRK1J3MWdOWHhqa2FFVTNvSGlQNDl6Y2ZEN3JFWnI3d2JCaFlzVmJIeGtpQWpISmNDWXN6N1FOaFN3STBpUXZXaE5vcWx0QURRblBxRFRuWStkU1ZHNmZ3NGlPSDM2K3VUZG5lTTNWT2F4ZktnRU83ckxkd1dZdzh5TjN4ZmYxTTJSWFVFUDZWN21zRXExMXoxNWk1b2ZqTWtvVmFPWHAzNmdIS3p1UGlDMmZYYmV2aWRWQjZlMTlpbGwySEZHNi9QZU82VlJlaGd5UEcva09QQkQ0d2xXK3p2YXBsT0ZzcCtUN2hXYndFaEhPSE04eFdTa2U4bmtZbzMyV011MlNnei8xSEtEWThUdGJPSHdPdmZ2RnYvZnROVTFyakcwbWlSVmFOaDVsVGxzZlB5bHlQYUM4VnZ6N205RnJGc3hSajlxcHcycTJxZnFYeDlkV0JMdnNTY2h2VHlJNi96Y29qNkJsaU1sdWZ2WTlYbXdjZWhWZVdJQVJyYTFlaEZ0RzRPV0h3TlVLdHYyTkRmM1ArQnlsblRpVHpzWXZIRHJXMnVyMkkyQWNlOWJMVHBoOStybG53R2RTWjdtekdaOHNuOUQrNGpSd1lvVEtyeDV4NEZmdWNsUHMwamZla1VkeW9KTU40Y3hVN3VZVFpmV3JncXRXOWIzWThDR3FvRUxoM25JS0p1VFdkaExwYTJTRm5vazgyY1VKbG1uRkxuZzdqQm80NlJxTHgyMGxDM1ZmcENYL1JWTWlpT1FZajQ4cnE1NlVsWCtudWpka2FXc213R2Y3S3EvRmt5WFRYUCthY1ZnM2FiYUR1YWw2M1d1K2cweHRzUVVUQmg0ZGt5K1A1QzdBWFhYa1J0aWdScitpb2QyeWR1QWVUQ3BWQkQ3T3kzM1diN2tHY0ZpbFZQMmJhVi9VeVl1Qm5OamdvN0RUQkNwaHB0NUxmQ0t1UVR6MHk4M2pnMURJTVptYTEzZXVZUStBV2MxcndobmwrT3FJa2N4MFJmVVU4NTRlWkdUYTdUZmpyYmtIdkRKTWJwVk82OEhZcTkrZVlrL1M1eWpMVEwrdjB2TCtYcks2bTVUWlpoejd0WDYvandxWlJpSVgxU2pyempSQmlZS3QvM2pNbUU2WEZRbmVPcU9IRzZWaU0xMGFkdXcrMVJQMVlDdnVodnd6d0lHTVU2Z1dlLytSWTY3M01aQ2VXZmpTdXhsSXZ1UmtqWXptYnZESk1Ea0xYY1ZrOVlVTk04SXdjWWVVaVEzOXdKb2pqbStNSlJkc3Z6ZzZmMWdoVW9tdDdvQWc0VW1GbjNxTzdIaGxPd09iZlg2SWJYT0hJV0I5d2V3c2VjbC9jdW4va3lSbFVqYlNtTi9rb0xXVXh2T0VNamNEKzFuTUd6NElwYU5kKzdZUW0xMW5FcTZMWjY5NHlKeDNxQlovOUdMMlBya2V5Q0lVRGRWc1puLy9TRFZsRkxOYUE2bkU2c25JbHpNN0c3aW1EaGJSUjhtYXU4L3NXZGhNYmZhZFVrd2ZqSStrVGhGUG5JTjc5a0FkYlVFYUtUdHc3NXBBZWZjdFMyNGUzbHpyTFl4RVBuNlRGTGQ3SW9CZkY4UUFZNDd0cHNyRk1GdUgvc0kwUXYwcGVtOW8rZHJwVXZ6QUNaZGl1dU1NdisxSUFtakwrWlh5Z1FUNjdMWDVUZ2hNUXdBV0hmK00wT0ZHVXZBYi8yYk9HTE4xeC9scm10NHdCUzdPblYzV3RHK3ErYmZhNTRJOThmcit3NHkza0paajlvVU9XVmhETlE4YnlBeDlhdDEwR1RDVUk5V2g4NUl3T05NaTJQWXBkMW13NDNtQkU5elZTRU9XOEVucmFDaU5DejBGUytKcWxMOXYzazk4R2xVdjgraXV6eEdRdmZQZ1plUDNrTC84YndEMS9BWDRIb0N5RGx3YU8wSnJJaWRFZHY3UDQyNlZNVXBudUlvcHJhYXl0YTVtY2xJeFRvdHdIZTYxc2pTUk9RQUpCN1dzZUd4Z2tIV2FVR3R5eXZrS3o2RUw5V25tTFBWdXZjK1M3RnhObW5kSjh4dGVIY1U0K1dHMHluZEQ3RnMyREU0WXRqVXhvcXcySk9Tdi9BUlhsbEVxZUNxYUJwbVZDRGtVbXBLQkNVeFI2RGNvbE5xM0VZSXlsQkN3d1UzNjU1WmI4Q3N1c1cwM01SQ1o4bW51WWpudzZCZ2VTU0FONWEzdTVZVWk5MmpTN083T0F0WTkwOGNnVndhUVZycmZQZGRwaW5EeXUrT1I5RzFmcE1FM0w2bEhDd2loRmFRVE9Ia2pWekRPZi84cm5GVFhxVWZwRzFMZ3duZU9UN21rdlpEVXZwUlhCSy8ydnFFS1NyOW9KaDZiNDRRSTVXMGZvbC9pS2hTbzY0SGs4dTVJVjlmZVphT1l2S1FXOGF0RHlweERjc0loZ3k2UGN2KzNZMDF2bytOcFJKZnpkWS95ei9pUFhyN3AvNUVqekxvejZUSmRuREsyM1hPRzJJRmM1U08wZitJcHJFV1R1QndsZHduWUJBcjc0czhCNlh0VFQrMkMraCtRd1dEWkJCbkpuZjZmeEN6VzlPNHRaNHltV3dndnlzK2pTdXBmSVpMT0FLM3krWHFON3Y5NThrYU5NSHhwdkFwYXc5QXFpK1BiMm1vSHJZUjMyUWtXelpKbWY4YlBVWWN0dVBRVy9saDBob1Rhd3dCUnBrWHRONUJGcGRaOUErRUJERWkreHhuTXM3aVk2YTVkRDdScmZIejU1NE9ITnBNSjZLYVN5NElRMFhFQmhqYkhXcy9mUGVLakFQTzdwZjZFNkhzTXRUdGxvWUN6ektwV05DSEdFcVFvQkNxRlU3cEdFQzArbWRHdEtMbkxNVklrNjJOQW0rdllOc1NQRkpmdGIwTWZza2wvbGN2UHBmaFphcVhUQ1hYSEFMdWRIZWYrckFueWQ5SmNCQ2IrN0dLVVZFQjVQOGxRVjBBSzhPWlFtRU0yNUpYRHF0YVpoYWtKTjMrTWRlMzVrS2lwQzZ4c1F2MUpIWkprTXNMTVRybEtTdk5KMzZyTXlFZ0ZmSXJ3REgrS2paM2ptWDNIVnpDdFI2QVp2STY3MHdZMEw4VkoxVkZsSWZuWml6ZEtUdVNxWGUwTVgvZVZxL29qK0ZyR2FIVkFHZ2E4OWRHTDZ4Ly92TWNiWkp3YjJBcnRuMEVpajRsQS9UOUpuK1VBZnpVaHlmdng2WU9GL2oxTnlHZE1LRDJPZkZ5S0JneWJVZGVVODJLZisxb2o4dHlZWStaelB5MGpCS2pHbk5MaGFvWjU0TjNhd3pZS2hwMUZmeDRsM0NvTFd4NE9Sa3NIbmdQeWpndVpRbWJpTSt3Z3hvSy9wZDNHWjZSVWl2aXJZUlZmaGlkL2p6V0JDcXpEZFM4QkhOL1QvZUZNbms1OUx0Ym5jV1JTY2JWMDA3dDRjcDNSZHRvQ1NIckd4cU55M0IrQzlDMzdXdkRsSUNiazBBTytMNlJ0amUwaStuUStZSDNGVHByVHJSQW1hZlkyZUZuM2d2bEFZNHBkb2d3T0xMcUMwS0J4NEFnN09OZUdEOGpncWRIckt3ZXVPM2FROXYvSmNOSHpscmRHZmIwWmFBN1praHFQL1NsRHZUYWc0VGFFSTVEUHkvSjJSR0drbVE3UDhNUmpNbWdiS0lLelk2MXNzR3NIMDNKOWFCTnc3YTI2UVpzNmVSU0VVdnFtV044MGVXT1Z3YklGZnJqWXAwTUpSODhuMFYyLy9NbTVBcCticmxBbXNGRGlrQkNuYkpWdUZ0OWRmZ2VvQXdteUdUOEZwaVdqQ0V1MlBvbTA3Z2lmVzRqQWM1bVFtaDVkUXZtb0ZjYXJWVUlNTVQrL09nalgrTWhtS1NwcWNZeXVMdUlla1lmdXRFSjE1d1RJY3krRStKbEhJNkxyYVBteFF4L0RHUVU0cCthOGZWRkE0ZmNqUGYwQWpWWEFOK0UvUjBqMDk4emo0Tm1td3pHOU1TVUduMHNSTDNnQTZwR0ViWkRnVXY2cnFKN0svcjFQUXBOQXpsVExsLzFvbDlNV1ovZllraXlNVHZlU0JOZGN1RjY1TUFUTjJ2dDRCcTg0MGZVR3BMbGlocUZZNzdheXFnd0dYNjlGNER3SFE0SDIyd2dNNm1TZDBJUThxYThzdUVyL1NLSjVXbGIzNjJITzVKamZ6NzVSY0NTKzM2VnpwSk1NcXVYNDRkN2NnY2VjLzh3K2ZWNGZFSXFmWGZiQVlVRnhEU3lzVkxiSWZwSXAzZWc0MGk1SVJEOFBrZ2FENDhGQ2h6WUk4c256TnAxaDd4SEdQanVkUVZNZjNPdDFINGdHc3REdWNTRzZ0azVJVHIyMjB2d28zQkxTYW9zZkpOM3JFOE5SR0x4ZjhjYkpGdDFlWVl6ZEVXbnN6eGx2T095VVlzeW1EcnlKdHpUTHlSYlhwZUZ0YloxZUpGa3ZKNy9va2xzQVNScXpFTW1TQUQ2NVp5bysyQVp4ME5vejZ2ZVNOTWJtbFJpU0tPTG54cExTNGU4QitUT25qUmo1ZHZMS2FLbzBFZC9pQ0lyc3pVeE1CTnNZNWpQbGs1RnFHVHgvN3RYV0czUUFFR2tSbTRQcTF2SUlXeUlzNjlWMnJ1SlhZUmJGZW1yS3Zzem85eVVYOVFuM29JbjZUNjNTNXVLSWpLMHhRbnpnOUdnV1M2MTZRTFppNTB2NkZESlRKSkQ4ajdxY0hKYS9Bc1dVdEZ3WWJka2dnRWFBWDlxcHBZbUFXaUducmxZaEdsSWRpbkNZMWpSTFQ0eUsxWmtrVG80b1JvV2t5WWdXdUhTekZtUHF2MVpSaFdmVW9zUklBMEVIeEhQTkh1eXZrS3RMV2Z0MkVBeUdUdTFTK05sKzlnMDFWSGc4UDBNbGp0Qm1qcjNmempXc3A4ZC9sZUg1OVVDditnQUEvdjdDOURjMkxaZ0FXUng5MVJIb2puWnhMcDNITlpVMkxxYlM2cW5aMmpEcFFReERtbzBVVFh6eUUwOFNhcUtNOGRXdCtVaEg1b1RIOEhGQjVBVUs3RnhtRGtSS3NJVlQxWTBHWGp1a2FnTjl6WkFJUDM1aENDQk5LNWVzRG52cEk0WTVGQ0w1MTZWa3BVYVl3dit0WkhhY3YvWFpIdHduZ3RYYnJIZlBJTGtrK0pFWG1FcmxkUHliNGNNVExlK2c2azc2ZXoyM0craW9YMkZEN1NBYXE4NjFDZDVNM2xjUzFaam5aaDFFd2FxN2dJaElVQTY5UC9xNlYwM0NscHQwRUhkR1hxZmZYUWI4KzFrdUNzY05tY2RFVG40UFYwVUlHZzU0Z3ZhY01Lb2twS2pPRDhiMVpwLzRXRVNDR3VYTkwrc2JUZ2NaRmxJZlM2SmlaL2lTMHFvb0NRNmhpOFhlcTVjbWFDc0RlNGUrUWRmTFA2dTYxSlkwUXhlU3lRSnowSk5uNlNuQmoxQ25sZDRvQ1llMEFmUEZQaDlaeUgydTYzYkhCcEQvM0NPQzc3M1BvWm5WN05kQTJiekRtamFiTlJuZi9HNi9rU1phclhSN21oNWd1bW9tdFk3RVdiZ1lHQmtCajFpZzFEb1k2a2MxQ2FYT1hXdjNLbGpKSTN3cHlCN2w1UFVIQWFkL3dkUHREcVBtK2o2Y3dnYnJjMWMxZmRKakhoTWFyWXZFWkxyWjlkbEt0OWZhVi9LYlg1OTcwMlBNR3RPWmJEVm1WeW4xekVDZElaUEkxcUFwTnl3NHJvZS9MeTlGWDF0TnRmS1hRSEY4ZVoxYVVlTUlOU0d4Y0ZQZnYwbnloQStVYmxIZzEzNnpOQkowSW9HQWU2eDk5bkhhV0pUSWh6b0FtczdyK21WZ2YzaWVxMXRaMWFwMVhCTm1RK2xIY2JSNndGY21nVjZsK2wrd09reTJTNWJhS0lRNmRRa01XWUJZS0Y0UERxL21qM2JjdzhOQ2dLMWVpM1NUVy84dTBDNXRsQ05LQ3VHbHpDVVNwMURvbjFnNjAyeFV0NjNrTmd5amd1aHNnT1YvWXpkMmlvTW9tckRqdUNpK2dRanhPQXE5V2NUczBMT2JiL3RmSWhrMzZ0amVqMlhRVVZyY2dMTGZwa0FhaVRVczhQeWVDYkFEa0ZKZEx6QStHY3Y2RCsrQjBxL0xQanBMRGQrcVNRMjdjUTJwUVhib2tlWGN1V3FVeWtTZDhzNVUxZGt0NUtBKzRPZ0pENmFxM2ZucFZZNCt0WTNsVEpYaVEzU1I5SlEzVGpXOVJXTm1WRGVBZy9JcGxIcTZ2dXhzR1NWNzQ1TXROemFYN3RIaWdJTHNzUzR1aDhIR2J2L0FJOVVzbVJQYXRJRzZjcVIyTjRLdzJFQ21KYktHNS9LV05mY3NWS3hzZk1IelVRdjJTS29HZjhRZDhjeE9KQUdBR1h2c2Y5dDZkQzdkdXBkaExUOVF4YUU4bkI0TllSWHhPRTJiR2JhWHp3YmV0WkM0djZqSEpucld6NFpZYTZRZllQY2xWK3lGdUxyVXA3RUFwMUlJdVo0UndOZWkwT3pHZXRlMTNDT1FXeTk1aWR6bzI2SU94RUxEeE9nUkRWOTNjSnFIeW1DdHVhWnFOckFCL0NZSkp2Q20yeWlsQXJIbGtSeWtKeCsyK0htL2dJYjN3WndtU1BXRTBzaGRHNVFWV0lvRjdCQ0hsZGFJakdZK3VxV3pOWStibkpoMUQrSVJhODdkMHJKYjlMZFgwaWZ4OUhsQTlYVWs2TkhRenJON3JYS1BXWjdQWHlueTJZVGpjNVBwZWYvWFUvK1ZtMzVlTUV1YStZSWpaZFJBODdJR2V2MC93WHFMbzIzdW5LYjlHSWtPb3AyWUhHTVo5eVkvU2plaFdXRWg2MmZFU1lZaVE0SmtZWk5aT29PL0cxSmhGYUwzd01ObEJvd25yVjc2UkpwV1grNVZLeG5PY01nZG1ZeGNlSVp3MEdUTWhmNlFaYUlKT3lNb25iNHVFS05FRldPT2N2c3VzTStHblUzN2JrbDFmcW1zVzUvRndFUHc4K1BockU1TE1QamgxaVpyMHF3TzFBZUV2OVRTRW9Qd3BxTnJZRGR6RkZUa2crTTJQb2FNNGFYaXdUdktnbUJEMDkzcytQY05uSXBmZkpwVTZHTE9YVVJmK0srYXZRTURXc3V3RUVnQ1lZeHpyMVR3WkZ4R1NYNXRWN0hRdnNnQ3lWQWtsb3BNVmZVcFVJK0lZd2wvaE40dXhUeFpJMzhDbmlSd1g2VDZEVURRZkUwZng4U000b0ViZ2RLb3luaWJpV3o4WEZrUUs0Q092VnNXcHB0d1JFOWJ1NUtOekVqYUdTd2ExTG9tVzRqdEsyc2RmbTNVWE5GZFRGWFZOSHhvQTVIOHNtU2FNUjZRdDM4WU9mRjlTeU1aTTRoZVBJU1NHSktLUXZ1NDU5L0x2WEFpRzdQWnVOa3dFMUxIcys1VHZMNHVvemxKcHFUb3Q1czYvTDcyZkwzeE82WkFxMGxxVU04OXhJcmQ4dnlVRXJyM3RYZHUzOTJsVnhvQytIMkRvM3V1bXFlcVd3d3VqcEhKTGl4cHZaU2w3aUFmdG85WjNIU1BhRnJ5K0Y4WnFOSUcvOWtYOXZkZmt4alhIUlYxWkpRVmRUbTc4TnlTUWlGU0ZEbnozOUpsbkRMUENIWk5oMC92S2tGc0lpSnBPVkplV3luRFc5MHRzbC9vVCt6TDU2RFd3ZDBBZE1HUmlQZlpsOE5ZUTdEeHNlMFBkVnc2ZWJhRHdFTzVPNTRHbHUrdmE4OUwyVFhLWFpvUEdTczl0M2lCbUROWkNQaHRxd1pUUjZqSFhUdVJOUjBJcWJjMGQraWpQdmJyV2p4N1N2UzRFbEYzVEZOdkFhZVp1aEgxcUVlb0d6Skw2VEhQZ2VxUHM5YzU1dzRXL2pnMVVyejFoeDg1R00wUkJpRkJLTTlmdnJXODU0K0x3LzJOdFA5RXV1aG5kWklITmRyQ0NSRFdtNUc5SytIOHZQN1JBRTNrTlB6RzI3cWtpNi9yTjZPWjRubE9IS3NIRUh6Z0VWallRdG1nYlR6akhjbG5sVi9mQVRJaEt3SkQzZkxVUnVacGExVlBVYUdBUzgvTTlDeXJRYm5PdjRwQ1haMkgwdHNjSzQwcUl3NjZJYVBBRFJJVkNyK0lhZmVKTUVpVWMreWNCR3hES1pxVU0yTVdRd3FuVGhNUjlQNHQxcVRhaXFjcmVDbXdPVkl3SFNBQzVYMEJkUDJ3MWs0ZkhwTzRXZmhSREZ2cTBDNzk0cGZYYVNYYzlUTVgrb2J0UW5ndGhGdTNMVi9lTXliLzlhUWs0VXRVZkdhcUJ2WXZPOVdxR1pBa1R4dEdWS1lFVWxyVmoyZkNOb2NrQzVhdGg3UVdXU25YVzZJSEJCeFhHdHJHSnFscm1WWFJ2QjZ2ZHNqcDVtRGpUd0ZBbmRybXZGcy9MaDByY091VjVSM2pkUHFIcUlCeE55VGprN3E3QU9oSWFydjBEM0dqZFN0NHhHbkJxUGQ4bndlZ3R4QUl5TE5XTS9QNVNjajh2UHRlYkRBcDZyb2ZIZmordmlMaGxTVis4TUNzbkVtUUpxMXNyaFlXbTRLZzA4Mk5HZmlTRzMzenJMRXpHQk9Ia2ttdkw2dDZRcTZxb3Zsdmt0Nkh0N0xIRW8rTVNVNTFJNDQ1aXROOEZzRU5vLzR0bjZSb1RLR1hRamZMTXZ0cE0rUG1pUlh4aFJycUZrSFpuUTNSNG02cG5VQ3JVOWl4NEJURkh3dVllUmcvZ3piQzBYMnY3UDJhWkV5OTF3TlpRT0RGcXdWR1p3VFcwQlF3MFZodmF2MDhsb1BGU1FFNklBMFVzYUlXQ0Z0R1JDR2RuV3NCekY4dXdlZGZjdE9hOENWQ1ozWm00STNING14eGZnQ3pVcWhJcGc0TmVVdWZxdGpLbTJzZStZelVoRTdGVExROFI1OEZOWnBRTTdZWmNHdk5ZbFAwM2ZrZmU0ZVNGa0JiNlBIbFlpWHVVQ2E1VVd2ZG8vZWxJMWprQUVhcHU5TlJMZExvU0ZKeHNhT3dDNHJmd3Q2NGI1S3c3Lzc5S3kvaUNNUVNpYWZKWE1rNStGajY2ZFg1eVFYOGF1QTBGeldDZEMyZVkrNk1vTHdlbnUzbjJ4N1NGc0lLc01rdTNLUkQ5b1oydnZCeFNpYm0wN0ZCYWtMV0Rxb2pVQmNvTmFmdHNiZGRsRlpONm5rZmo2MEZWUlNEMmhHdjBxUnVxRUYwYVFVN2dETUpuWm9LdHhZc2NBOTloVW5SdSsza3dJZWE1K1UrQUJ0dmk1SkZybDFEbk5WSS9uWjhmWTQ5MnFydmZJZTArQWRUS2tCTFdxSVpLZ0R2ZjJjR28yVHZ2RmdEUFViekdwUm5BcERuK2NoVHMva0xJcmJFOEF0NzJFVmJBeFBQRUdaRUpKSFpucXJTSGp5ck9aL25VU0tDUTVHODc1UjhPNlo2bm8vYUh6RWV5VEQzV0ZNa3RSR3poYnBsY2FERGNuVlI4SkxHVm50R3Q3bm5RR2YrajNQTkM1bUVFRCtycVlLVWtab09xQUczRER2YXJqN1FKWWc3RmYvdy9mTmZWTnJDejZDQVE5N0VIdEhPdDFNWHQvKzZacC9JR2tJRFVrOHZneVVqakhPVERGNWwrVklFQTcwTkhiWnlPM2ZnR0tBRWw0SWticmxYUnZkNGRTazVGSHV5ZEExMlBCM2dqZmlHQS84Wk1yNU53dGdoL3VQVXdyb3Jqck9CQXpyK3Racm13cGpJeDBuaGVNK3NxZW9RU05WTTBuV2NLTTFXcVo0M2lsaEkrWmpZWGRnbW5PYVlvaGJVYzBNMHRsa3RHY2JWZUJQMG9VaXZvZ0kxK21BVjMwd29IMzZIRnoxUURndEgrKzZFQjRJTnlCZG9URVE0TWFQU1NRQU10SzBFSktBQnc3Rm1oSnAvbEs3WkpBU0NSa2FXdnBGTmJlSW8vM0h4d3dvY0ZYV1R4TndjWDNXN3g5ZnlWZ0x3aEZPbDRYNEVPSWhJUGVJOXVHNjdhWktScTBMMUkvZXpKR0tqWmk2a2k4eDMySGFYRWYzQnA4cGxMTGFJRkJqSEozNmFiTFlnNUI0emlXZCt1aDNXKzBKbWhkUm5XcnpwM05JenRSTUc2bmVPY0N1UC9mUklVMGRyLzYrcGpOb2xadkJjbENuYmxGYmZLNU1tMzlBUTBYRWp4SXRLaE5VVUNnN2lyNXB0dUdQRjE2bjNRdEFzUW55UllyaDVjZVZSUGVTVThUV0pvZFJkWTM0UkFlY0luRWFYOVgyam4zQ3EyaGtJQlpvc29kMzhDL1krTkpwcUpTZitzMnU5aTlGUHI2MlRXQm8wdGlMMnFpaHFCYW5vRVNXeGVIZW4vTjBGcmFSaXFNSE9FK1hqMnMvTHFjVWFsZ24vSUdPRVVnaGFhUlNRNUhNR3RnUUROQ2ZzN3Z5VXd6Zmo5OHVFOUxNcDgyU0ZVQlZTanpNY0F4NTJEb2dSRCthNDVCZFpWVFltV2VDdm5jVHArSWY5bEVaQ2tYaVdHdDJNbzQwdW9tbHBwTVNiTi84eTBFUWg0eEhaWkFucHlueGZuS3FMU2VYS29yYXNrdTFGanBmRnl1b3FHSlhFQVFYblhwMzFMRmNDM3ZyRUliclByc1JvV0kvQnNGU01MbWxnL0JQT0dqM2F4TitXOE5xZmNzNHFHcmlxaEdZR1ZocXc0RWgvRk5McXpaZUZraTlUSUVpM1hHYm9zQUsxTGZPT29RdHhGdi94bjJpcEZWRWZ4L0Q4TDE3QkVNcVRYZldiODRxR0FGL2IwSVFEVjQxSi9pV2tjWVNTUWJwM0VQZU1EbFdoMmZSR1ZRc3BLMkJJcVhmRlBVTlJMUGdjb2N5ejZPVXNLYUs3VERJcExBckxCRGV2eXRvY3VqU2Q5UVF3Z2N1QUJTK1gra3hIWCtTd3RyVlZsUDIxR3g4U29nRWhXWXBJUERmd1FQV0M5NEE0VFpVSEwwWXNxV0wyUytCcngrOFlGMkJaWTVQOEM4ZnFNdyt5a1QyRnpicm1ZNHNrdEVPM3M0RTNJYUYyK1dkK21NOEMzclVzNjlMVzJmSE5hc3VNNEc2L1Q5N2VDelBrQUVGYjVob0ZyRG52T3U3ZzQ1Um9iV3MxenRVZmFKYTB1Z1p2cmtReXA1NG5pT3pkd09QRjZnRVZoam5DQXZ6bnJEdTBtb3BXaGNjRFlrMXkzcmVGT2Fxa0ZvTzJETlNVMnR1L2VaaGNVY1ZsTFNDNklDbzN2VnFSakRBaVZNOWVKU2s0OU1IaG5BWEVzaC9jS0xoRXpXUSt0dHowQzV0akY1MXdybWdmVzIrdy92cDRLTmhMckxDL3NWcDIwRUdjTTlHeUwzQjlOUGx4TmVaWTFhVldGTDNhU3RUM1drbkFvUnRxOGFhbWVMU3NtVTk5L2E2ZWYzR0IvU1lNeFpXVlVMY3QvV3diWWlWNXVhbEc1ZE5iOTdra0U1dGdYcVcrQXh0YW9pVTMwS0ZKWUdJYU9RKzJSbkdNdjRYcEpvdVFNN1JhcUw2NkxIc0tFdXErTExFUVdEdEwxNVBkOFJPNW9zdHJZQlg0azRTVUVrKzB2ZWlrVmF4VmUzUkxaeDNydG1RazNMVjVUdmxDSG9tWVc3L3BmcVRLV0p2V2VNdjhIT3h1Vlp1WnRwZUtnaGZ2RjQ2Q2lDSTMyL0lBTlRtL2JDWElQM0RBaXN1alhlRFJPMTNSVG00UGZ1cE1lN01ya1c1bVlIWW13Q1hNZ2I4SXZDeWN4TGovNjZXU2VOUEFDanFJK25PSzRzRnoxMWpZbVhoa1I2OEl1TXRyWHBUbS9aNUhubkpsa1FyaFJFRFpkV0hCVnR3ZXh4WDlVN0xDc1JaT1B2bDBCOFB6RmxJZTRKeml2Tzc2TUhqeXROV0V4bDRVYjFjVE14MkU4Y3JRVGpyOWNuRXgyY0wrNG5HUm4xa3I5T0lOZHZGV2UrVlpDUlpkclBJbnVIYkNvK3lkSTc1VUtBcHdScm5mSjJ6VWE1OU43QTY5WVJJQXc4U3Vzd1R2VUJ3UXJhK3NuMGRxTmNxbjBOMUw1ME1pb2pNcUs3RkVKZ2NBVU5kMlZBRU1HcEpmdUE2b1Fwelg0a3g1SlhiZFRBOXlBUUN3amlzZVhnWHY3NzlkbVpVVTNXYjA0aS90TGZJS3ExUDcxbFdiQ1lIbzQ4YXF2OURpcEJCWkVsNWpPWUYrbUpUWlB5Q3dlVW40OVF0Rk83TWJqeE5XVEp5UXJ4dEJKdTVma2w3S2pIcUY1UjRxMWtMT285SjB5VTBTckZsZXdKM1BMUlNxdGVWNXN6Ty9HcFowNFJtVlVSOTVxVEFuVThScy9kcWtxSlJDbUljZjhHRVZZK1JJSWxTNzcrMFdBS1lDcVVwMTZxQy9JcUxsQTVLdkdYUVczODNiaDV6WVhOd3BWd1ZOQ0J4MGlIVVl2azRtRng4MVVKb3R1L08wRUErS05icitJQnd3RWc5d3g3a0dtZzc0T0ovWkk3WnRpNHhOa1IrbFpTaFhTZ3VqQlNLbmJuUHpzemo2RmpvektnNXRCQnJOSk5GYmNJdUs4eUt3UVVLdm9aK01nd3RCeW83QWJZVnBkc2FjZE1Zd0ZJRzhUTGx6R0hrc0lqSzVVZTBEd1llN3FINGtzTFFrbXJnS2NlK1JoUnUzaThkNkkrVjJhT3dmU3NBbXg3KzR4R0dTR3J2dlZVQ042TitlejBESkpwQkFjV1JIb3ZmSHM2Y29oUkJ5cW9pZW9vU2FXbW8yamNPZHdQY1hpSEYxWGpNT0E3YlprQWJDaE5EL2lmejZtdUJhbHhLSlJKdkljZXhrZVJUZU0yVkJJRVhwcGk3OVE2NzdPcFFKamI5ZXNzam1sNk1vWUpvNldvU253UjJrQUlyTDBuT3JjVDdqMXR3OVBuZEFicVRYS3ZrSWd1OXhkL0JGVjR1dDNBZDdBNmI1MXJCNXo3dlRSYVdVeVZxUzIyTWJ5dG0yWnZsMkxxVk1LTFdoSGtEZDYzc2s4ejdzc0xyZkdiOHc2bUNKdlRUdzZZNHlWbWViZFo3SUd3ZXhjUzBCR0dpeG1pL2Z2Y0FTcWR3RHZaTTA1a2JtRU1TTk5tSmhDYlVIQTA1NkJ4ZFJYNkVkcWhTUTJvbjBVVktkR2FNKytMN3A5RXJ5Z1V5QkNHK0dvb0tmTElZb0hxbTBrZVRhV3JleGIrZTR1R1NxbkVhcVdHTm45ZFRwbDcwbkJVSDdZZ0hKUUc5TzZGeFA1bzdBZDJnblFZdldtYXBZY1Z6LzdKUE1mbzFmSUdLQlp1RnJHWUdwS2E1b2VvSjN0SXhKTmVpL0tYdW5JSXpqUkNNNmI1UTJxVG53V0Q3SXdBQlE1YktXSGtOVHpUTDF4ODhzdTBFQ3Iya1VxaG5VOVh2bXpzRkpDK1ZncXlJTlVOODZLK1pGa1lvNVprZUtkTlZ3VGdFYlNtR3F6aUZvc01rZzBzbHJ0QmMvbFFiWi9MSExJTGljWWJLaUdoLzZtTUw0b0piQ1kzR0FwcHBhWEh5eFBTWCtFeElsT2U1dGFWY1lVSmttdEE3b1pNTDZvRmlnU1JXVFBSZnUrQlVuQTFjbmZOUGZwRUhiVUVqb3ovZk9tNnRLRzZSSU1CbGIxRmNEdXZlU0JNRm04NXJKZ1VDWm4zaGFlYy9ZSG4zOGZHOStsMTNlaTk4Q1gyOVVmYUhXeTJRelo2Y20zTFgrWFg1UHZQWHg2RUMvdjNUaXplZ1F5OGxaSkl6QjFXVTBjU3I4OWU4VjFBWTdzcmtXZis1NC9SRE4vMG56eDNiMlZCRlo4YmV3em9kdnFVSldpS0wzdE1wTDZ2U3piSll0NGZiY2xmbkhDRno2RzIvQ0svREVDL0wrSGJwWmJYZFgzVWE2VGcvQVB0R3M4bno3ZHpZVkFDTC8rT2x0akRWS294TlNJcGhya2VnOXg4UDYzSWxxTUI0TlNmUkZFUHRiZFduTWVXKzhOZlVrdnUzM20xOXZhZGFKMGdWblJyRFUxTGNHbVJVRkcvSnkwU1I4djhzVGRBeWh1a3RNdlF2Ulg4TGRwYzQ4OTdmUEtMKzNsc2xLbHlhN0hISVFLdENteENISjlsWUUrWTIzMC9oZWIwNEZ4WmtsNENiMms5ZWhHWmZDL1pkaGVwbUY2Ri84NUhmMnhYblhJTkowcXdJSHpaVklydUE4WnZ3WW9xTkdTNFkrMmlSNnNiL1d5akFIa1RYVndkZnVoc2JWbVVUOXFVcm9rVlFkOVdPc0JUVytTb05sbzBRWGpaWHlEVEQzdnZGVEE4T3ArUkVxL1FGMTl0TVZOeHE4N1BpUm0rSklrbU9vVmJVSDdNWEpYSnY5bUo5aEl6KzBkOG9WdlFiM1IyYkU3bkptbnRaVDdSSjYxSEFVYmZERjlsRmhINVNteVpFbDAzUjFuVHl4UUJHMWpHWS9KL3Q2ZXlvcEp0dStzeFAyUnNwWjBzVEFUTUlMaUhkNjh1dHhNb20yaTdpRFpncC9pbFhwQTM0cW1SaDhuRU43YXh5cmhsQTFjbnRTcGMxSTROZ2xpQzVnbTQ5cEM0SU9jVm1VaTIweGc2RVdYWW51ckxUZzhnK3dFa0hxSXA0a2pwMjdpczdhMHhHZCt4SkNqNFJHV0VzbzN6UitXcmJ1WGgzK25PM2RvQkJ0RUZKcGZDK3p1dHZieFc3NTY5a1dET0RxZHl5T0hadDJianNoZHcwN1RXZEhvekRYNXAvQ1EyMmpYL0c5bUExNjRzQWppQlRzS1pSM3RQS3JkMzVyVmljenFkNUNobWZ6ZjNiWGJFb3NIOU9ySjduMUg4ODFxcGhIeXBaNzg2QlVYS1FpeHBNTTlqekJOTXdrNlJaMURQWThMM1lXbm9wWUlJcm5JR3lpTk9ZS2UvV1RaTGxBbTA2d24vSTU4cmhnRnBsUTM1SXlSZTg2REtQaG9RSFo1TVlQeW11cUR2dnIzakRJZFhjWWU0TXVCRThLV1FWKys5TmtMTWdOcm8vbW53UW1EYUNTNEdVdnV6VnFWdGMxQWxOUW1kdEhveVpzVkRRamNaVWVkdXFoNzlxUGQxTk12amtmOXBhNnNhTkRKRzlQekdRdFRUY0JsMHhTT0ZDSTJDWkpnNk1ZSnlNWVB5cmlwQVBVVXZVMnZHY2lTZ29HeFBpUXlnT2VHRU1vZWh6dDdWRlVxaGJ1N0JjR2JyeFlWdXlsN2VkQXYwdGpHejhlcWY1VnBUL3dFazFUdG83MFFzNXJOczcyNk5STkZDa05mZFJXQ2hTK2dkWUxRWCtWWHZCWlJkVCtwMmlyOFdQQ3gyTzNHNW9IN2N5M1RKWmNhQWtVVzViNmVKZUNUWGVzOWc5M29xZnMwMzdidkFpTW9HUVBJZFROM2d5OHQ2c1BYV2dUMlovQ3o5WElkZXN3VkhPNGpoT1FHNWdkT2lRem5xbmZmNnRGVUZydmN4WU5uUi9mVUJjWmVxcW5PamJBSVdXNVpVVmFOUklRWmtITGR0T3IyZ1laejd5WjRPam05YmprZVJwT0J5eTZFaE9wYmZDaG9xMjlHN2xsTVZQQURCK3JsSEZUSEhHNFZnZG1CR00rTlgyS0JidklpMkZKL00rRHJVTmRCVEZRSVd2N1ExODJtRnpYWFlRcDNXSHVFMkVYeGVjNXQvUWdVYW4wZ3NWSStkL0VDSnJhSXhrV3JGVHZBaExRZlBPYmVmSVlNZWtiZW8ycGdjUHBuRlR6MUZZMWxkbTFETTl1Z3RRRVVCNWhSTitUeTNTOHU3Z0N6TjhieEVWazE2VDgyNktIUm11eTRuM3A0cFNLemNyNFg4NmJBcFZYRXdGbmlSa3NkU0RQTkRvOVVHTXdNN1JoRlVTUnJsZno2NHZpTGdOa242QndsZkxmZFhWRUg0WjhyZy9EUjdiRkxINlIrM1ZWUHdENTJDbVo5aEZxQUMxQy93R3ZySkpMSldpcmF4R3k3OU41dGZOTFliLzQ0U3FlcjgvZWIxenB0N3l3RnNXNVhTN01HNU9BNHYzNVFRR2cwbkdhRzNTa2FvYURobEpDbmQ2alNzNkFCeWRCamxnSnFGbVE4Y1JNZkROLzZibzVWY0h4YUEzYzNORVZwT20rOENUbFpHdWZCS0l5UUJ4OHp0U25mK1dFTzZOQUdFdElUWnl4c0gvN2RzODcvNWRIekxkY0pWampLVjgwdDFiOGN3ak1HSmVUUmVEZlF3bmlId0cyaUpDcFQvcWZaYlBucXZPNkg2S0RYZzBHS0Rid2IvanNHaE9Nemh6N1FhZFJVY0hwTEY1cmJNQWJBQzJ3SzBCbVpUK0F5OHp2bHR4SjZnZmxyMHQ5NUhhbXRZQjFaNU9hdVVmWTdvQ3c2bC9ZRU9OdnI5NXhscHdpS3pma25iTnZHakkydEpUN0ZxckZnMkw5UDNYZnhUbk9abGZNZ1l6SGlSNDd0RGV1TWpocldLaFdMWnB5SmVhYlZqclBVTER1eG5KV1VYLzgza0FhRnNMN0owdERnSkpWdzBISGFMSUpMODdOaFNDeVRVTlI4WDEwbHhNcW9KNDF0Ymg2NXltY2d5bmxZWnQ5c2t2SzZzMHZWZlNDTmRRWTVEWFM1V3B1NVdVd1JYVE9iY0lNZmdlZGhGY0psOEdFbTU4cG51S0JqL2ZDVW9YZlVRMVN0eG1CREQxeUxRRStZK3lVWHhkUHkrZGRiemptMHdJTHlHc2NreHdxZWRVbXVYbzNqNVJyZnl4amFhWTRTTWNKVzdXU1czaHNLbVdtZVBwRW45RDF0Y2Q2d29QTkZYY0NKNjIwdm1sVnlGeDVMbm43RDArUkJZVUJFVWhtZ3VVd2czZnhEN3Y3Z3NXK0dnNVZjbXhENkxOQ25waS82ejg3N2N2eWZDdEI2dGVxMDJqNzl0WWVyYzhVRFBiYVhiVnJwOG1Va1BKa2tGNGQ4NEtqNXMveThIVGtPSzVFbjJpNjNPRTBtK1pHSkx2djRzdlNXek55OS9ic2pWQlBtMmwvVXFRUjF0ZENiRjh1NldPQ1JBZHhLajNhQ2J3azRFdS9qM0dhMStJenNXbTFHdGk5YkZTR21ESFhMTXBVYmw4VzJpcmN0WjRTRkVxTHl2Ri93SFRDV0M5T2ZzQkluQ2lxWHFMSzhHSTVtazFBem5kUGdJb0FzSjlCQ2J2VGVOZlBRM0tzbXdsTktsQmhhVzUzTFovdUJUUnJtOXhwNXIxTW1JYXpjV2lFaXVqNitFNDVJUDVDS3E0VXR2cU5OVEw5Qm04WE5MaXRsZ2dONjVDRXlNd2dWcnlhREYwT2JXbTQ5ZGtxV0YzUng3UW83QXpOckV0UXBHbU5waURneUZ5WHRqSzVXSmoyR1AwQWQ1YWhoYys3WHBZRVNTV3orQkx3dVNyNENBRTV5cFAvazdPUzQ2Vk1TSy9GalRRQ3FubFlUWVB5a21nVnAvdDZvNjJRbldSVG8xSnFjbDFoaEpXT2JYNi9JYlRjMGdtZHg0WVZiUDVleUFaVDhnaFhSK3BmSEQ3cjVPd3NkaFlPdk5zVm1tU1BFeEw4djdlYko0SDRUM0RCVkhuRXllZ3JtcGNXUE0rRW9uWmVBNlRRSjI5L3l4V3B5MnR4c250QlhOS0ljeHZQM0M1L3dMNXlhdFF6bEZjL1V1amF1dit2TVdWWTY4MHJnZVV1cE1vQVgyMjJGTm1VZ3BGNEJpRUw3bHhBRmhxeWVQUlQwZnpydzhZT0hUNHlwZHY1ZEdzWmx1ZWY5Z1ZWNGNuNW4reU1PaDhFT21oR2xMRmwwcDg5Sys3QU1TZkJiUUZ4clVTMGlUZU9NaFJnSjQ4QUNSWjVncUtBQ0dCTWY3K241Wm5EcFc5RGNFK2tPZlNuTXlhekxnR1dQZ0pQUE4ybUYvTDgzc3NBOGZjd0JaZDNjUDlTTmFvU09JWUxkWEFBRUtKMlFzVFFQc1gzN04yTVNnQ2NyOTdVejE5c2t5ZEVoOTYvenY2WXlITzR2TmZ3eTBIaVlHeTB6aW9jU2pmbDA3U1lOT0ZNQ3dNU2N6Q2ZKMXRWbXk0ODFKZmFqUGhkN0ovTStQWTludDV3M21YYXVMQk8ralc4QjFOQlZhSUI4NXIyclIzZ3d1THEyM2tpNXAzRzNJYitQVytST0FJSUJ4aHphSFZaeW9TTjlVNGQ2Skp0bFhUWFZ3MlIvVHlod3Z6VERqWlV5REFobm83Ly9LbHYzaVZLYVk3M1ZET3R3YkF2dnBwRmFHK00vdno5SHN6clFreEljVFRDR21tUCtxQzdVOU5hKzA5RzlHYVVmbWdreEZoN1YxVW10eCtsQ091V3ZESjBLb0FWQTV0NldrOEU4Y1J3Q3VVQUJDWVZTc2pMZmQxUHNwb09FOUNyRXdxVW5rcTJWT0tEc21nU3dDS29tbWR5bTR6L1QrendSWS9ySmdLNnMzN0tBR2Ewd1dZUkx5bVZTNzVEZTdlYThFZjRMUjUxVDQ0ZXF2V1FwSG5LL1JwZ3M1MWREZlVPUTV1ZmZKUGVIRXZFajhDUDZPVzFnbVltSWtXTzIwamUyQmtqelQyQ0pGeHRaZG1BMmxKYXE5MGRhNDNNZW5oTUtxVXZtYUlSL1Yzc0ZrRHZRU1JoZnkyVnRCVkF5Wk9rRmpxQmg2VllxK0pnRk9USjdIY1RNNFJyelBsVjNFb2NUa2J3Qm9VRnZ2OTR3SnRUNjdlVVFDL2ZPR2M2ZVQ5YnpBbkNKQm85S2QreEJsRjJtOTYxYURad3llaU1CekFhUUo3cE45STM2Tlh2MVZrOHZ1cHFzL3hHL21FOEx0UXlZWGxtejlGeWNiQ1NENzJvbEQzZ3VKUEVTbGV5blNXbVRBU1QzT1N4S0htdXg5UXQ3djlZVlRmOGpFY1I1S29yZUxkejdCQStMMC9iYXF5YnErRUhINUxHV2pHbElrd1gzN21TcjVtVmI1dlFFb1dRanNSNmhGWnF6NWdMVkVHRHFmOGRzMWNMbkN1VlprWWZETzJlSSsybDVOd2pmaG1DSTlBM2huUFVTcTB0QkVqUW5nK1Z0RjNjcW96S3VzaW9raGNZNXZ0WndYLzRRd0JVWDR2VUZ2ZE9sRENmTHdndE1lSUlJQ0RBbUxvb1hrVE1aOG8xUnFaUi9TOHF1WFlGdm9RTWwvSVMvaFVBN1BKZ3pmelQzdjA5dWRZcld5cWQvWmpOaGphSFlIR1AvaWdwOXF0dFlzQXlhb2pUaXd1ZXVIYkNoUXFXWFl5dVd3eHZ5bW9TL2NtbU1qcW5ONGs1Rlo4Y0xuaVdlNmFiMldWbDd6NEtyL2NUdXZIemZZYlNzZklSNnVubnRTeGlwMTB6eHdRbkI0ckNYMzkyS3M4SWhHSHBLaHVKOE9hdEI3YVdPS3c4OHhwOHZ6WndWMEhUbi9VUUxBQWdGL2ZZSEJJcU0yTk1FcDBYUWI3NTdIdm9tejA3aHJDL0x2cGxKN2IyWDVQVk5iclpzUGFEayt6RDNONDIzV1V3SzZkbnlXTDdTOTBXbS80ZUNWTFV4Y0hONE90THRGdFFBYW11WU50cXF2MCsvdnB0WUdodE4vcDR4Vmo4b3JWZUR2dHJoc3dqNGNCWDNlam5CWDV3Qmt0MjBFemhJRXJ1WHJQeGY3VjVvTjkyS2tKVjU2UU95aUZrbWtsU3Myano4bUo3OXQ4RHBhNUw5S3ovcmNPY2wyZXNKc1VUcVlGb0tMSzFzTEFuYmVtMW1xbE1TWTZRS3JscDBWNllqMUtRV3pBaFQxcHNtV0RyNlNuODM5eVI0cE0rNytDaGpScDhFamkxdThCcjJldXdWNUdhRE52bTRpWnZ4ZmNRRGMwRFZ3ZzdLQXBCZWV2clA1MW9HRDhqZWF6V1NSS0hpQkMySjhmR0ZHdmdXYklXZC9EcytqMjJaQTBSVTBrWGthUWJEclo0OEF0UU1vZmNNb3FhQmlKeVFJTk5lWnJjYVFoTFV1QjNzMGdEOFoxTDZINUp0L3pXcEtkMUpFakg4WUtxNXkzakwxSFFKd0JNcno1MmNMTUIySGM4cVlEM3M5RzE0SkgwZExDUGZTM3ZENVIyTTZnK2hSbWhkcWV5S1dUWEZVL05jTk83N1lZUDkwTlZXL2U5cm1ldEEwWCtJcG5HaUJWaXN3NmpkQnRlNzVabFFRcHZZK1NvT3pYQ0xEWVcrZUEwZi9HUXo1WWN1YU9qSkZSdVpISkV1U1FyTDE4Y3BaZWtGUG9yWkwwL1ZBcXdpdU1WVlJsUjkxYnFWcm5KUHFtd01OcVlMRktKRyt2ZlVqNlF0Y2tTWDF4UTk1TVZXMFJzQTQvakxtWldGT2RIZW9QVjQ4SjFDZmYxcjlQVWN2TVRCU2pYT0lYTTdWOGtCVVNGSUc0MGVYLzJpb3c3bThpQjVSd05sSGE4NFJ2Qk1obVFjVUZhZnRhdTg0Vkk3YnltUW10cXRzVWFQVjN4TGJESUZVVGUwRHFyc0xuNkZhclIzMjZuMjZTMWlMcWNQNnJPTWNCYm0xbURpS3JIaEJ0bHFwVHU2aXBoMUxpRzBqQndEL2p3aUxpWUlWcXBnZmF3VW0yUTAyckF6SlZNczRoSzJXM1RvaXdxZ0M1dzFadldhOUZVajdNakZHMVV6N3haSGdyWnZUYk8zVTF5aDlBMzlrQjIwclpSQmRLSWgvRHpacDBxMllETXJ0emRLTW9HdW5HTVVDNGpneXQwS2NUVGtzZUZrYU9RcVc4Q0kxMVBweEM5b3pkN1JTeE5LTjdSeHJPWis2cnJCalhFOHlCdFM1VU5XZ2J6YW10YThrMkEzT2NzZ2h0cG1ReHNjQk1zZTE3ZDc0ZUkwZ01ObzVHN2JCdXZQNWo0Zm9mWThBOHJHODV6bTRleFdaaVhTOGVYY0FHQ0VYRXRpOFhKM3FmTVR3SjFkUWZwNllWNERjSytmL1V1WUxZMm53UjM1RnE2dWpjU1N3ek9LSnhqQnYwM21HQStySi9RcmwrQ1hIZHVUWjFZTHVsNWY4aWRnSnMvWFFsOHlVRjB5VDhhZ3dKdmM4eUoxanF3QS9UYVpRcFMyQ3BSLzJLN0tTNzRjVlhJLzhBd2RLZUtBWkw2YmFvalZLeHByb0lVK01PSmw5NzhGZGNuRDNCdVlyU3ZlQVMrMFJTUm5EcUpRQzJvSmJyMy8xYVB2c0ZySjlqSVdaeXhmOVFuemZiTlA1emZMSmovWEVsR05CeW1md1JJUTlxYVRtZS9xSWw4ZzRiUFJPTGJxQTlERkVYMEFnVXVIZnJxZ2lLajM0WWxVNFd5di9TUmUxUEtZMEY2Ty9tc3MrRE10elhNQ3poMGJCQ09lcVdnbmNDQ2xxN3pHa3NBWElNSEJ2MTRxcmxrcVNwOEtNdlRMYUZXMHVUd2RpdWFpSVlEMXMyUW96YmswRnIyN0xUL2w4QmZhRzB4OEJ3UG5ZcTh1cDJvV2s1RjB3RHBaOXZTNG5zVEhubUxFS014RHEyb3J3M0dIRkZ1UVN2QmtRR1NrTDF4RHJoMFBRb3lnZjNxNjhtRnRMbkFjeFdHQS9iRFBXeE9SbEpBWGhiUFp0a05vRjNhSkNEdVdJbVhrKyt0NERlZncxZUUvanlUdVBMRW04eEJKaTN0SUJFc3hYYmtUdTlyenlzVjM2UkJ6TkMwYTRiRE1SQUlBU2s4T2pic21jTENCUHcrbGRwN1o0T1hSKy8vZ3Q1WG1wdUhGZTV3VUtYNnFDbHNYMUhHcURTYlVTMitOTE5IQllCODVoMTdRZUVxYUF5RFp2MCttS21QNHVQK05CNDRzYytIcUdFS203WHpNa2NmVjF3bE56OWpQNUx4YkphWFEvL2xNcTB1dk5jSGdQdi85VHJsQzYrbm0rVWVrWnQwUlFSM0pCMG95emxGa3NNUGFnTlIvNzRnWWFpK29BRTIvVHErWkJWSW9PN1ZwTDJkZ05yU3YyUkxJNzNpcjRiMVZic3ZlNVhRYUZjTVhTTFNEakhSZjVlak9aRkJ5bTFzYS9kam1OUVhGOEkrUHZZNTJ5VFVkaW4rWmNESnhYa254OXdTMDBOdDAyZUJkUWhjeDhHZ1J6NHEwbVJ6OUZvNnJsSjZSclZxcGZPTU5RcHUrNUNGbXpQQmpUTmJMandmZDVwVjR2WE5Dd29MZkdtSmI0RStaUmYrRFRGam9QOEpCVVJOVEdsS2s2RVBKcXo3YW1qTHVtckErS3dnZGN4YXU0UmhHdWhpYSs3VjI3UEJpMHVmSjRmUjczZDdxWjh1NDAxRExUNlNkM1ZjcGZCRGZFaHc5T2I0dmNKUU0xK1ZmNjcyem1jdGVBSnlxZ2JUVzFlMXM1YmtyZHNNK0JqbHZzblplUzAvcUwvT1YwaDg5QlBHaFlYdnZiSlRlTTlFUjBLUDdCVVQ3ZitXR2lsM3lPZFdLZnJ4VUdFM1prT1pNdStxOW4zMy9QUjJxek5BNjRGS1orZUs2OGpjemxObnFvZ1pCMHRHM1JPUVl0eDVweE5Wb2xYQzZBdXFuL3BWYzdDK2h6ZXJzamlpalZuVkZmVzhBd2ZkY0puamdsbzFuUmliMmljWDNMM0VjbTNiZDAwYUc3eWd4Z0Uyb0ovbjM5OFZQcFcxcXV3SXoxR1Q0SW5QNG8vZ1AxeERNMzNMeWQzQUxMaGw0YlRsakZrSXVRYy84alNuWDA1QjNFM093VWZuUWlLdkh2VU1QOUdscDVGRWFlU2Izbkd0eWxvTC9wNzBqejhPVmsxWUs4U3UyZk0wNzZVdXZxdEJjWGVPUlMxellvRkE3eWk5L0FXVTVPVFl1UzlBZGZNSGVBUHhNOHlpWkhDRzZZc2F4L0RhMEdodnJmNFliTXU5RW9tWGtCQU9odytGM3NLQnNyTzdYU3IwUnhtTGU5VzF2VGtYeklJbU1iYVkvUGRtRG5LcjFucXh3TzBYM3lyR3cwT3Qza0xHd1QyRUtCcHR1QTRlWXc4b29yUHc3TEZEdXJ6cm9TSWY5ODZlYmR1T0owSDBmUGpDTHNaV3lPSGdmbUF1cmY5dXg0WUFrOENLSTJFQlRXTithN2gxcktZcnZZK0hqamJBWEtnelAwQ1R3NW9ZQXVUcVA4eEhaSUhPRFlJY210T2VFTFF1cW1lSklHNlFQdlMxY3RFaVUzZUVGM3ZQSmE4bC8xWmFXVmx4dmwwRUNSai8xbVVLTldSVDdTeTNTNll3QkVOK0hwWnhXN0ptZzRhdi9mNjNJanZMTEJIbmhpUk1RRFVUa0lWSXI4S3ZFWENGWUlLTXhUYWY2L2djSXo0aEk1Vkk5R3BnVDBSTGo5bWV6dlNPcFlycEJuS2JYMGZ3ME1TY0RDMWRubjhITisxQndMN1ZJR3p0djdiUUs4N3VLUDVKaE53RXJIYkIyMS9RV0lOcUpDVlBWRGU4MmJyRllXdi9OTkcvY200NjB4ZUtOQUNYQnhoZG5IS1JPTzJzZmZmWkhFV3d3a0dqSzY3dGlpVWxTbmd2ejBZTWNnR0FOcmhjVExTTEp3OExjRm41OHlzUGRhcXZ1L3NtVlRNNGNnVFFBQmZRS0VDTFBrcWw1T2VyeFZHaWcvSWhQeUhHWlU3ejdreWdKZ0RuQjRRVlFrNGhTbHRtOG9TdUxBbUFlckZlMnJPOE9DV28vTVlIT2U0S09TOEt1RERKRWx4bkhic29rT3BNa3E2Y0V1U2FvNkZsOThyN1cvN1UxNlhlRXRPRW16ekhnRGVBOGlUZWtqMVpNZ0hlUzZMM3hhNTJiUjBFWlUrMzFtWUJZcVFTb0JQTDVsU2lJaTRZd1l5Wk5yZjQ3blFTc3ZqYjc2UU5LTmRSa2R2TGtYTXdzdVBvT2E1RzFHM1pwQy8rT2puUU41ZjlhL00wZWZrdUpjd203TFRXOGhyVXJRZWVlVWMzT1pNOVJsN2FLalR4VHVkUElPa0FLZFNkMUViT3B0V3RrNVR4MXdCRElPMEhoajhPdXltd0JZcVpDVUVKYmR3TEdHVUNjeERoYlNGdzRqNXNrRE4rMm90S1FQeVVoTGY0d0Jpb09VVlpYSkNmTCtUOUZIRVZWKzl3dFR5RHN4enRLSGtJbisxN3JZSU1OUjBSUGVoTEF1SWxvbkV6K0hvSWxaNW93QU9WUjdiYVVrSzd1YnI5UWwrMHFKWFpTcXUzRWRwWG9LeEJuMUQxNTFxN2krOHBTSmpNN01TUnE0Mkh3YzE1V3dJRlp3WStJVHpYRTRMUndRNnpKYUsvVkNPQ25seHJ2dzZrRXRmaE0weENNVVJyVVZ1cGZGWmhKVEZtajA3ZE1GSEdIRTNHOHZmRDlhZWJvNjRiZTZMdGVsd0ZyMVd5bW1JemZCODNYVEkrM3UrSm13Mks0WnFxc2FTeXVOMm1CYmVKUENxT1l0TDRIZGJnUnViWllacEpUdEpoZ2drMENaSlR5QTR5K0x4bG84OVhmRm5jUmhydVBGMU05eEhPVDVBMzVVTDdNR0IxbytaUGNPeEM1N1FGMHpyNnlkemVNbWdHeTJLbHBZclBkRk5DZFpES3lDdmxJOHFWL2ZKZ3VpclIrTEVSWjNRNXpmTEw3MEFjUXY5dSthdzFiL0lHZjVvNnE5NHgzdHZ3T2NqeklocnczUE9qTCtpNHBPMmlrbzNMRXM4UlREeEdaaVBYcnZkeWN3emhzbm9zRkpYRm0zeDdpNkJscTIyN3dvMXZqVFJaTTV4RUpaMnNzWFpMTG1ldFJoSjk3UndVTWQvRlo3Y0dQam9ZTEFsTDZ2cnpGdTRzY1VWdWJQQW1GY0xaV0R6K3hLK2xjTFJBdmord3Q3MlpSU1o4aFNYWVY2bU8xNm5Qd1NTdGFISm1SbHlZSTBLSWVSM2FWWUZtYzV1ajNQbzhiK0VmaC9CZ3pUZjBkRUlNaHIyMlg2ZktUTnJVZ3RKSHE2ajRPYzJ6d2lVMUxiYjJHVDduZEg5K0Z2emlBbEo3eEtVaTdmbmY1encyNThnNmMxZUkxWEtpRlNZdGZqdUNvOWwrN1lmeHlKTEx4dklseWJ4aDdSZndaTHYwTUI2eDUwTHl4bVBVU0tLNld1SXZKM29ieGMvWHhCZG5ZZkhJNEw5VWtoTmRwTG4yU3V4bXduQW43N2pYOWVLd0JuVGc5ZTRxWHFVZis0OHVtL2c0eHNIY2MreGNoZ1VPQnJWN2lzMGw1cm83ZThNaGlnRTEwTzRaYVM0Qk9IRWhkSkRMbjEya1pCVmZkdTc4MXZBRjdSZHMwZUNNR2ZRUFdHQjVsSjVzbFQ0YXA4Ni9kOXpCNVJoYm1LYjkxQWhKN2VTQnh5bFFBWGM3ejhVWW9nU05zRkZOUUZLVytCeitzVUVVWVJuSWRwWU5sT25WbWEzUXkrUEhtWnFJQkRLTS9aalhQNkR5alVnazExbkZXWG5rTS9DclRQcDNHZURIa0oyOXpLK016QkZmMkJoZU83U05RQm5Tclh0d0RWOVlLTitmZ2lEb0NNSjFsenNnUThKb0ZiQ0ErdVJJOERXa3dISFlQNkN3dmVGREZ1R1BVUTQ2OXZPKzNqNHkvb1psZytnNlN2bS80VCtyWUdWdE9KWTkxUkNmSW9zclVOOVNPN3Rhb1VpODZWNy82bmRjWXhnMW5ieTU3ZWpoN1oxWmQ4WnpFa1BwK2VhZFZPQmxXZkVJQWRLZVo2bEs0bGNHRFdBYnBwaFNjZzNTZFpza3Y3Q3UzNVZDVksveXptdkh2YVlXa3dLVTNjd1dCTlB1VEMya3grQkQzVzFsUjN0YnFKSDAzYm9ycFE0ZE4wTWVlNUhBaVFzUk1Kak02ejV0UWtnU2JCYnZsUjRoUGpnMVhyODFTN3liYzMvNUxNU2pJL1d3QjNSd0wwRU5lZ0V6MEJwV0kyS2ZTcFVIZ3lvSXpQaEwyald5ZFRCWUxYS2tMVUhPZkF0Z1AzbHBIUlpBVjdwMjBPbUZrWnJzVXFSMUQ5akZGZWJmRTh1d3llZWQ2V1Z1T1drYmRzODBYdm5DUGlzRUo5eGh5OVJLeFAyWnVLS0ZDTlpXZG1WK0c2eFh3VjRUcVhVNnNXUzhmU21aV2k1TGpUQzRrU2x1QXRMc1MrQVNzUndSYlVSOWV0bEhCbVRwTG9Pa3AvclpyajhsVDlrYXo3cnRnYk9Ua1VYRWhhYmFleXRvWTB6RytIUTJzNnVBYVRQU2QwN1M0U2h1dzdSRDQ1bUcxZEtLYnlRWGRRU0ZuREtTekViclFMa09jZmkwM3BDZ1haYWh3WXpyZm1acEMwR2phNVBJVnNkcUd1ajluVlQraEV0V2VuTWpidHJBQjM5TXVXT0dOa2RWSUZabjNicVB2THRmbHF4MmE4TVYrMmJjcXByUjkxdzFOclR4VEJPdlRSZjNpdmJOcEVnYnozUVJNVzhCUVlwRVBOS3pNQi9Nc2RRZm1rN0NyR2xweXRaOWtaMTFCajlDTzRDT3RrbTJXb0tVSU1LVmd3RkUxRG1vay9LSDg1dEx6L1hEakJ6Y0cwQVdzcG1PWXhYbFhHM0huSW9QWjYrbTZIdHJEUFZhaTNabk13VWordFlqeFJPUjFEVGtBWVFXNmhySDV1ZURMemZ3WkVTWkVhNGR0MXRQdXR3aHMvcFJjZkgvdWszQ0h2azdZZHAwMHJMQlA4OCtGOHlHME9NZnpxbEhEdTdITHVvS1Ywa2VSdU5DaVMxeTBSQmlkeUE1Ri9oajlUNitjYjJ1Q0tYT3lUcVk4M1RFNUVxT3ZNZGFha2g4d2tXcDBiLytEN2hKNGVCRFExSXFObVFCeWl6ZHhFOHNHNEN6eUs0UkJ3V0xUV0toRzRPNWlyS1o0N1VHZVYvMEVIMzVSdkJtZG4wYlpYbC9nRU5HNDFTemZMNW41bFR0VnZhbnNzdHRLc3ptSlRtVjVUZEYvUzA4TEp6Mi9LVXpITzB5T01pTldHd0F5NkJqeTRrUGtkNzRJdm44R0pnTEhqSkloa05uaTJ4cjFJdXFPOFNsQnBwM0xPN0NmUVFRdEJ2cWREM2NCdDV1cFVKMzVNWlQwczc1ekJVNWxuZXZua2FQMXF3VkI2Zm5GK3o2elBPLzk1NXpkRk8vZHRvaXNtVm1iNG9NNlJYa00rNGZXdG9SOUsxQmV5K2VsWGdzRDNGVjlDeXRtYXZpQTZUbmhKeXI4SXVWb1dNamRlc2s5bkg1MGt1OW1RZGlXaEhtRzlGYkFHczZPbngvM1k3bUYvNGNURjVVbysybjNYNFFhNGxBdDQ0YTZRM0J4NHRWTWQvclRkMW9mR1N0OE1xbFoyTFlvaXM2T25mbHVNWFd0RkFFblJ4Tzg3VElhM2ZWZXBxcXhzeGlQUnBYN1BUQm82N011cCtFdkh1bFBIaVFKa3FwN3lieUMveWRQUndEdTZIZGhxaUlsK3RiMndETVM0VERRQ2hTaFJpMUF1WFFSNkNIZ0hnRUlGeDcvVW9uMWNDK1lYajZHemc1Z05CZVZhYkRVZ1BLRzdETUdZKzBtTyt5T0wvTm1EQTFBczhYeFlLU2lFbkxNekt6MTJEOUV3eWVkaGJnemlHcW9zVWdEcGpPaXVBNWJxK2RzTmxhSkYvQVArUlVWUGV0NjdCVzB4eEQwQ3ZyN1lFenJCQlRVTkJkd1hudFQxNmk0dkVZM3BZMW5ZMlcyS1RCYW5RTGFIaVhHU0NsL3dXTWJTQmhucnVoWUhmTTRMQnk1MHo5aU5jQXowN3dOK0FRSTc4SHErWHZJTXBBcHdrY1VPTkkvTjFsT2xsVnFVaWZnNGEwOWdaRHdsd1BzRlN1TlRSV2ZEVklxY0Mzc2FUVk1pQzFEV3ZndzZlT085TXlNMk1ER0YzS0RKQS9sUVNGWTJCYkg4WXhJeUJoTmFyQ1dveGRLejh1Skx2cXRDcmtJUmpQbTFMRzE2ZldobnpjaTZhWUV4cXFsRWF1akx6NEVhUkx4RGhWbVE3M0RiaW4yUFFCWnBDU1dxRCtVSUZCNzdCT1hQd25ZS3Zoa1c0REdhK0MxSmI3Zm12TDE4dGJLbEVVb0xDMVBRK0lXS1dWbEk0cG5EdUszdDVySUVwTjdtQU9xZmNBVkd5NXpIa1NVTjIvQm8rUUJVV28zUVREelpaRGl2ZXBIVEFHQUczYzVBZ2hmT2VHdHB5Zzhiei9EU2xjRDErdStzb0s2aFhqUU8yQU9ZSi8zSFNqOGlUVGdPWE5zQ0pKWUk2ZzhlMkpUdUFWeDF2Q0ducTBjWWxITW1CalRUK3RZMkszYkt0VGVuUzFxcmdnQmxQdlArMkcrY1VqS0dNWFBJYjNmUVdxMFNxdHR4NFdVTnZFdm1DTWtHWkxJOGpTV2c0am4wRWtJeWdlcFZ2NHVLeFhCY0FPMGZyWnlHVE56aWU3bmNlQVRaQkZpV1haTEExODdqemxNL3ZzNWhON2hQNmVBK3ZNamlMa1FXcjA0SlZUUlhxZWdFa0pHOHB2TXVrNWt4MlZPV0VZV0RqckdIZlc4dGV6bExYeFRTUGNka1M1YldGVEFtZlE0RVNkTFVEb3pjd2tCU1dtOURaMGw0MDdORHUxWnc4OFY1eFlLcEozNGhHZTRxQW4zQ2xBa29Xc3hMeFNpVGJuSlgva2QwMVdpTjNiWUtQcUhHS0YvblloakZIZHY4cmMxTkZDbU14VUtVOENJbHprc3g1N053dFVBTEZ4MXBtcFJvMERnd1hCeWVrWURWOGptcDI0L2lBWUZSWXFHQTVIVlhnZUlvMTU5a0kzZldldWltSTQ3aUFTYU5WckRydDljL01FWWJQaFAxSS9lcGwwNnBvekwrckNoOVV2eWJhTnF2R0hVQ0pzK0FDZ09UQVRzQXNXVmd5UUdoT1ljY3RRZUQ3QnNiY3JkM3ZGbG9zcThwRkU3Y2FvdHkrcGRFTFYzcHp2UGUzeTV6cFYyUTBhekV0cEN3MDd1NTlZZFcxaWVPYlBQMG4vVzN4REc2SlNLSFJhL0lLUm5MUjlkZGlSZ0pUalA1eXNyRDFOLy9pYXlmV2Z0N1VQZXdPOGxjMTlWSmVKaWV2QUVEY2VIQzIySjlIV0tuMnMxazFWR2QvbkpjSGpnOEYzS25XaXdHWTRSR3YvZ0ZhcFdsTkhNNmZ1RzBVR0JZRUMrc0RwUmMza0VyRXVhb2JvQTJURUM5dGRpeWVyUkd4R21mUDVYdDdvT0Q4OHFPeHE5WFUxS3l2by9TRkt0MXg2bjhsOTRhc0J3NGJtR3BGLzVFaDBkRGN0bkkxdVpwTFhvSkF5V0gwT3hGN0s4eUJuVGorVERWeEE2MUc1b3cvRWc4VEhzS09ZMTA1VzR4Y29HNEFKa0RmOWxwTWwrdEM3RDVja3E2ckFSRlc0ZzZURnJMYUNjb1JuOWgxVWNIVitET25ITjZhRzNaSEUvWTFpSlo3VTJXYWZrVi9GRUcxRnZHY3RyaG4xWEh5R21xU2Mva0dxQmhtMWRRRWZMdjdnRTRkcy9RSG0waVFuMkkyQzNMSHc2MlZUOVdvamdadDFLT0Y5MGN0TzBUMjZTZi9TUTlnNEdiSVFQbERYd1BVZWduUi9qZmlvSCtoaGRqUzB4dEU5eFRHT3Q1QU5oMHM5QVQvZVF4cjlhdHdPNnVZRDFSUW9TWmN4T2JabzB2SU1iMzJUSG9idXp1U1FrRHBQem51M0hiK0lLOUoxOE00SUpwZkVkSDBLRjdMZnpmRVpkNHpPZldRMDE3d0R0UHhhN2t4ck10SlpTaDVNMXppTmYvTnZSRWgvanBuS1BzdTFIZTlCbnptS2RhNklYanVmSTRTejkzVzJxMCtGN1VObmdpR3c4QXhWaHdGd1Z0OXg3UkpWSWdXQWVFdU1oc3EwZ3hPb3p0WVRhbVZVTkExc0Rnei9uTnFHSko0alN5N1kvMUw2M2NMNUM0VVNUZzU5YlV5YVpBOTVuM0s5WTBpejM3aVBuQ0I2OG16STFBejdhM1czQXp3Y1VBT1Izc0hlanhKT1pwZUxHa0xQTEUzVjFPS0ZKVTJNMmo1ejJqQUNEWGU4aHpFR1p1UDFSdjcwVTkvNVJKanpQU2VEZ1Y4TlQwTitQSGtXdjBaSnZCMkxSYnFUOFM1ZXY0VFJUMzkvblo1L0FFNlc4cUhPTG5wQnlNMDNGdFNVUWNSbzl2UCsxU0ZESXlpKzhWRlRYb2ZRWmoyL1Q5NWRmUnlLME8rR3ozNWNuYWwxU0tHS2RMOWtTOS9iN09TdC8rSmJ1aGU1NDZGa3MxSHZUM3VFRFJNRHpsZmlGd2RQZmV6bDlGeEdOOFdiRzRjMTRUSFc1UGJYK3V6SlRaaDB5bDdTT3JMZ3NjWlA2VWYvR3JNQ09SUDFUV3BKaER6OEY5b0N2di9qbDljTjM3QWxxNDlWbkw4OVlnZk9UYjcvK3ZYNTlnVEFmNXF5TzY3SHJ3Sys5RXE3U3VodzB4OFEydHFDQUpEWWErTjNuRkhDaFZ5Z2FDYlpoWWo3eUhnQ0NvWEpsMWtvNTJTOUZhUFNBQXpWYjUvQnk2SWYzcW0wTUh4ZEV0QW4zWVhzOXk3Q1FLdlZ4WEc4d0doMmFwS3hzeVN1V3R5bVZzUFpzZ0liTWFlNzZiRGQ2UnBuZTE1cy9BN1RkNTR3U0xjZUNvVVJxajQxNVRTRFdmaVpoT3FGMTVZazhCRWk1VklLNmhPMFFKaWMxMjlmYWkwOFBFTmdlb2lzcW9RS0NxbzRNcHNUZ3cxdEV4cnQ1b2ZlT0ZLYXA5N25FWWt6LzMyL0RxRFZvRDlkWk90am5HUFJrckZFWVArbXBlcnhCQkg3N0VrSXU1TGlleVdGeEdRRVBkUUtwNGRSQ1FJRjJzRWVUdDVoUW5JNlBuR1JqcGdBRWlETExTZFVPRWZhQnJQMkgydzZXUGlvWmZQWlpzVkczSDRUTzRTWkh1M2dlQ2trRVFzbXNkdXUxajBnVlVhL0VGNEVrZitTZjZwcS9icUVURjdURzJIV0E3UVhMb09EY1QySGZ6ZHVJVDR4Q2FTUDJhVWtvZnNGOXdwRGRnZHJmWSs5NWtJdzBidVMvNUNMNk5NZzZKdkxUczhFZ0srTjdwaStUUzhOeS9tUHEwQWlaOS90M3U5S1VLNFZINHNrZUk3QngrRjVsU0lPT0hhQlNMck5WTTQrMHAvVUFsYTU5Q2IzRVVaMi8xSHp4M09zTkJoNTZKTFk1VlpnTG10SS93MXZHVDZsWWxvTENaTG5RcDVQQmlBTzVITHlNbHJmOGZJYlRnRFB0MXNnaXRtQ01IQWFxQkJUdkJ5b0Z0dlhoSzB5TklDRXVsbEFsOGJPV0xuRXJvdkozRGtKMjhwaC9IZ3FXTzhjeEtIcmtld0lyOXF6c0Irc0IrUFJKYkZFNXcrOUh6aEZTMXBLNktpbzdWb2JRTnVUVG82REVuQzRYelU3Nmh1NHU4eEJ5bmxaMkloeS9ENjYyNEQzb3JCVGZ0Sk9zblk3Snh3ZTRacExhbmNPYjBmTWxLVHFyQTczOWVxTUM2NjZPakM1RnN5S2NJNk0zRmoyQnhXRmFsOUxMaFBUZ1MzQWZzQUZSRDZkTmtacGhqS0VacmY4NjNxeHhQb2x4U0dvME05Mk9PSEptS3hkc2lqbWVjYkNKUmhuQ0RZcE9kcjBnTCtwTTQ4dFFvTkVLS0xidDVKMXFhYzRtaGtHZXpOWklXM2xWTFFaWWx3KzNNbVZVZEkzcXptRStvMlMzR3FhdmNvK2gyTFVaN0YxZUFtWFF5OG83V2R4dkR2TC8zZUtLU3JqWFR6dzZacHA1Z3BnSU8xL2oweUx4d0ZsbXFYVDhOQUR3RVRtWmtyaG5EK1pGN3FQUFRXSGdKa1BzV1VMYW1YNU14azlEYU45Z3krcmg1RHRvWXRRUmwyWWJ6ODBQRE1YT0VGSm5JMHd4cGhvTEVQdDJqcGV0ejVjdzFYUzBpbDFTYmJPeVlsWmQxekZnY0x5Ti96Ym5hVjlodk9yVmRRYUUyRFAvZTlsQmN1UStOa0RZTWl1MkZ1TmkrRzNGM2VNTDBoNGFnYlFJQWVKZ0FVM1JHM0ZmdmVuVEViLzVXNVM4Ukx6UytFZ3M5WjdhSjBJTm9BaG44WGRuVHBKZkIxWE5jZXFjdTJoNm43OTQreENFUkRYQWV0bG0vdmY4RWN5M0dNcFh6d1pscDAreVBzRkZCNnY4eXBiTFQ5bml0ZThZVm1qWHp2ZlRZaFhPUlVNMHppTGVwdUpUZWFnMkl0OVFnbHU2Zmk1TEVGbVlJb2Y0RkRqaDFUaVRHVnRGZmEvYm03VTNXYmNXUEVXN0xjNkFCalVSaVI2SXc2VWp4UDQxK3ova1lDNTZGNC9ueDJ2bW9tL3ZrS0psVHBUeHFiclExcmlpZGNEa3ZBWW1RK3BrNGJQcmVrS0lEM0xKbThyaUxjV1hORE04VldScnl4VGFXb2FMZFlDS3Mwdnp5SElrTi9IVFFxdHYrV2RyZ2NoQVcwR0VxYVhQSGdmNTRhd256aXhTc0pidThyZDN4eTVHY3BpeTlvN3BhN2dPMVJOL3d4UjZsSUNkOVpBYUxsYnM0REhoVWpzSjNUMmN2bVc4MXExOGJQeWZVOW5wY3Q3cVNaNDhYNlhvOCs4NXM4Y1IwSW9oRjNPSVJxbWpLQ2JZMXA0QmpkWGdMbU9NUE9PWjlwaytwdUJUK2s3ZEpubUlobXdLUGFLcEJHdWcyQk1CRERDTEZVZVhaR3lLM1dnbXpuN2w2M3FQSlNQbjZqaFpzK3lZRno4MXg3a1FYYzYyWU5CUzUzNjR1dGc5UDlWV2pjcG9XN0xtd2pZUnpMZkJudWY1RlVOOXZjZ1RQZFM1Z05COFh5aFhrOEl1dkl5VEx6R1lXZG9zYWJ3MHJQYU9GRlA2ejdPTFVmSlIvWUdhRTREQ0sxdUx3OTBobGhQOEcwS2lrUm44bkJxeXc5QThiUlZmNGtMOTQrVURwMlRaM2QrTDJQWkdwVWYwcm1MeW9rakZ2Q0NkZTVwL1lXS2dlUDFDS2h1MmdPaElyYjQ5eEpVM05YNks0T0lXU1pjeHExUVlLc2RXd2hFeWUvZmFSUENnc1orM1Rmb2J3MlJaRC80WnB4YndDK25wZEREZGJPT0dWSmxSNUxDUmF3ZXlMek01MEFQYXRxL1NhcGl4ckNPbmNkcUtuUGR5a2Z2NUJIUytQVDB6UDI0MTY0NTFLc3FGenE4R1ZySUJBZW9NeldRNlJMM0xxWHY2b2RTK2pMb1djU01xV0VDYlUrcmhKRGNuVkRRci9nWmxOZm85ejlBTEs4K0hwR2E3WjJJYlJEVHV3Rk5EZFBJQU94Ky9OZTBsUHFDMkVLajdoOGNCVm9LOU1WNW9hSzlvRkF3dkZVc01SRWpidzNkQWpYUGpOeURGUUJtbU53cEJMYWNwZ2ZwVjR3UUNIVFpNS1FldXliTm4zT1JQMkorNjl1bU5ieHZlTUMyOVVZSXBkYUI3NUtnRnBleS82Z0hmd2xQVlZuaEk0ajJPek0wWWpBZ3BKbjkvSWF0bjBXU00yZk9QK1J5N2lFSTdKSjNBcms1NUxQbDU1SVIwT1hqYXIwb1VPVCtBYkI5dE0yNVdTakVJaHhIczUzazVWRTN6SUFTRFVlRDR6Rk1zeUJ3STVVZEcxaW9lMUR4QkJPZEU2T3hCZG9BZmFlK2R1UEVOeVJCNk1DejBqTndSWmk1L3M3SjNaN0k3c3ozemRsU0VpMERIbU1oTEsvWlMra2dmNUd5ZEdIcmV6Y2VnZktYWmt4ZlNPT29GcUFUbFJaYVkyUCtyLzc1ZmhDUnJLQU1Ud2FuMGI5RUpuanJPenJ1MTZDVUMvY1Vnc3ZuQWxxWGpieTcreW1VemN3YlZqSUpEenpTa0F1dFMyR0RLcENMd0xvUlRqd2phcnFLbjNPVkZvUW1wWWFvdnMwNTVRTjc3VnQ4WTE2eUlETlZxK1REUk9JN0JNYnZqK1ZDMkM0cjhYRkVBSUZEY0JZeGdSMTE5Y0RlV1hwcm94Vko2VGJvWCtHeU0zdG9nQmd1NXJNQy9QQ3QvRE5CZGxKTG5HTm9iWlBGR0Fha1Fma2JFZUhwTk5hb3paZFhhN0N3NXZQMTRQajJyd3dwd04xS3BoTHlCRm1GL0FtYytDbnA5UnlHR3JENitGMFFBenRMTUUrS2Y3MTBOSEt6QTdXamJJTjZYM2pma1FTYWFQSnhMUi9FZFJMc1k2cTVZamErQUw5TERZd2J0T3VmQnhCcHNHWGYyZUVJVzZrdUVwR0hpTmlPUVNINTdNVGliQjJxS2Jobk5kWURId05EeFlZK3hON0ticHZLb2hxYm94V1BDVlBzdHJnUW5hR0o2azRqNlY5MStQQXVOcDlVQXpMdlo5WUszMmUxTStkZVAvSjNVTVc1bEZlaDY3dWdSYkNtVG9kUmNHc2NBL1dleG9tY29YYnhwdFNlYm0yVXBBUk5wOHdlcXZXU2JLei92SmVyVGVMWTJqUVcxTnRLYmRVRjdURE5hdC9sa1pJa1Ezc1MvNjFBRkhJNENkVzE1d1EyVGhabmR3YVcvUUxXam9OMDZibWpiSUpmUVB3NkZrMXBmY09BVzhmVzFpeFJKSVk5L2ppNTBBTVlJYmhnMithZnZaNXBETllZUXlGdEpXc2FoOUI4Y2Q1dmhjdWZyRjcvNDZSUUJvZHAvVjNYb0NMVU1DdVhLcUkwR2lZN2tkSjM5MzVPdCtUUXZ0TWV0dk1OWE5PMmNTVXlhai8zM2ovbVUvQ2k3YXkzbHJtK1gxS1A3UitKQ2EvcStweXlkQlFZQzhacFkwZUphdVZZM0xZWWdGYWJQb0lJM1lXL1IraXJoTmVjQlAzNHRIWU02UWpST0EzdlVvZGZ5NzZiOHYxUjlJTDFuY2tFa0szb3ZndFNobWFOOVRZZU5ub1dFWW4zdmRQTXY5UmF2ZEdNbE1FUVhQeDB3UDVidm5VV0JuQnJvQnA2YXVQWnNwK0JIZFVVL0RscHJ1OEJYRmV1UHN5aEFqSGJ3ZFo1R3o1VlFRK3J6UG42cm8xeGVDY1FLc1N0bTY0M3VRMGFDN2p6QUU4MG5UaElmc1haMUQyZHlGRmk4T3dyaUhSb1hXT28yeFg1WTB4Wnp4aE5RMDJ5MHBqTk9SZmlGNzVlU3R3OVNrbE5IV2dMVEszK0lxdFJGNFBzQnlzcHdzRE1iR1dJczJ3cGxVV042aUFvV21JSnJTVm0xUDhCQzlHTEVoazVmRG9UcG82R3dlSEpJTWdOMjRzb3pYV1RVT010OWh4UW1xdjFOUlBGRXRoUDdCdDhTR0d5b05IVndNUjdTcjFUVGhnRE1PR3VzMUhzTWRYNVBFbSthZ2V2VUdRaml1dFRaRGlCOVF5N0QvWklvUm1JTC91RUllT3FhZnJmdlhYNi9hVm0xVzltSFltcXNmblVXQ0N6WmpGbkoxTHVVRGN6dHVEelJpeXlBMmZPNXNwYUxmbkNnaExSSDBEeTQvKytJaFYyZHJtc0VxYktzWEM4VFZIZkdGZHdZNnlmbXBwS0l6Snpzb0hycEhFczdGNVF5YzZQaFFrcHlHNEFnbUh1bTFmYzZNMUJaZ2szdkR3Y0tVTzc5a0o2bDloM3ZvTTZqZ3hJanhEUzc1S01JTTd2eGlhczVvV2hBYmdNVEZKS2RseHAyck44S3IyNStxMkJSNU96YkExWFlxKzBmUUFvZ0l0WFF2SDdINDQyWWRBVWJ5NnFyZCttek1BYXllWXBtZGdjZktRYTJXWk5PY2djckVUVFhRdHRBQTdOL254cTRTTk0wMkg5VUllbU5LRHdaUjVZUHJjdkdhT0dNQnpuYklwb3U0MkV6d1BCNHRwNlBQcy9yejF5VUYyVGRoWTVvZXN2dHV0NHNmVXY0d1ZFeGY4RHJ4bXF6UjBTTkU1ZXJ2eUFzYUIyUGhveGxDQWZ1MTlFNXRyZ0JxV1BjTlVhZlVHQ1JaYjF2eUhHUStxbFBtOFIybmRMNEt6bFc0OHFVVHgvYkxWckU0Vm1MazdpSFdVZi9pWk1MN3gxbXR2eVh2TUM3T2NFbk9BZnNBQVRQYzhUL2Q0QnhERy9YWTB5TXJ2dlk5YlduK2x0djFaQlpNNlZlcFROYS9xMHRrak12Um1wWkxCQzIxVTZ3NFFDSlZSWURBQ3pNalpOb1lZYmIreWFONysxZjlnVE1jNXYrTHIxMHVJdEEvZFpmTWJ4amUvOHpsaFc2OElyNVlONzNRbWJLNnErYklrMThhOUh4ZUZ4VWRMMWVtSDBqb1U4SEtxNDhGU080eHpWOGIxY1NFU0FzTHF6YmVKSFg2NVR4ZWRTemdkYzVhckd6cU1zbHBMdXBrQnp0Zm5pTmE2K0lUdzR1LyswUVVIOUxSTnV0K055bDU5aHJLLzBBNTZwdWhxZXdsczNodFJvcHN6RTN5L096eGhmUjhHZWxEOURGUUVJeG5DWkUvWThYRGpCTHJmNGR1ZE9JekJneWVySFpJMHNiOXd3MDJNczljQzZ3UjRzOU90aVFEMlZIUkFUckd4VGM2YW9rOWR5akZpUC9Lc3RHa1JnMFJYZmtoRFRTbTlzaFFUTHRQR1hkWGdzR0FIRU0vZ0x6VXh4M2lWRVBHSUV2dm5ZaUoyNzJzWmN3OVZ0M0VQVUwrK01DMy9NMWIzM3gxcWtMNittOWNuZk44MUxrN2F5ZW54ZnlnUTJaU1dJQ1VXdDZEWWI1ZkczaVdDWHZEQnRNRWkxemM1NkRUUDZRYVRhR01Xd3B2YWR5MUR5c01QSEdjOENGSG42WVlSQXgzbE8rOWFyU2YzdVlKRXlPRzZ5Y2FvN3RYMGxtNEYwKzg3L2hQd2Z4MGpKcjI1MkNOUUNrN1EycDJ1RGQ4aUo2aGJ0bGlPODVVc0g1ekdxSVZCSnp4U20wci83OVBMK2Zvb2Nra2diMmFZZUJvTFA4ZDJGQ0RiKzBGYXBOdFN4TkpCSE9OSmhBNmtabThxOVh3N1FubThHOFlCV1dZaVhBUFFibXdReXp5dUdPWUl3UVRNaFN0dFdRQTRtei90bjZZc2diQkdrOFNnd3h0YTdSWnR6Nng5TDJ5RjhScU1WSlZDWllYY2cxbVhXR2Y0aUpWVW9uMG1ldWpFYzdaOFBYUWVWUW9BWmErMExQZXBuRC9FNm9rQmI5OElGMTNTTThnOW9ua1cwbDF5VjZiVVdia0pWRlBoNDU1UlU3MXlqS3BqZEdGbFpJQkE0UnFiYlJ2bDdrbjBzWmRGUEhRT0ZzSEN0bXRIenJqUkZlQWp3dW9UQnNEeWN4dUFNTm01emM1LzJwemt0NDhMNUFwSERtNUtXTGVhSlNxdENNT3BvR1lFKzhtSWUva2NoZWZsdVUyc2NLQkJNQzAyK1h3LzJXUWVSWUNQcVZVVUI0NDRiYUVySUZGK1BJRDRPcUF6SnA2b0hBWk9aY0R5NCtDZG5NaEdEbGt5czVHdnUyNXltVzQ2aTg1RWEraHNaakxScUkrbTAzMEtvdng1R3ZKbmJ0WW1ZRytFa1Bhb0Z3NnIrMWU1UTBOZGI1M1ZpZldMMGdRR0Z3VlBDUlIxeU1OOUkybjNmaU1ZRlE1TldPUVRJd2FKRTBwLy8zQ1gxeFFoZEF1ZDFEbTl2a0hnUEJUTE9FQ0lWdTNYMnFjSkJaS0xtM0hGN0VTSlVnem9PcHNWMmVsR05TNVJSczNkSXR0RkI4MmRVdjhmVUNkNkZwbXYvMUV3ZmZsR1VYUWJBVktSOW9yVVNaOXh3TWRIejNFQ0ZDQTlRNUpJK0FEY0Vjc0VlVXV4NHQ3RWc4eEZ1OGxIdWNrQ1BMT3dPb3I1emV4ZWJoZEd2Z2cxWHc2eGJxeXNqalNZVEVOYXF5NWNMOHdMd25la3pQL1dKS2lNTjJvQWVWbittSVRxQUthMkM1YTZXZVBxK09ScGR2YWx2S0dKVm1TREZFdnMwRHJrVVpRb1M4MlJFRUhvWlNYb2JzblIvektoMEpZZk9yVERHaTdmeDFVU0lvU3Y3Njk3S2ZKeWRUekpVR3JRckU4RmFkamlhUXZ0eUduQkR1NFNteXpDanJLTGV3bzNKbnlNSTU2NjlkSjVmSUVFbXozSGVOZGVEd1U5czU2NXRvWGZGdzNaNkplbW42cmdacERxVVpVUGRtQjlLRy8yTFZJd2wrZDZqMm9KSVVCQnM2aXNBUVIwdkNHYUlsT05pT2s1ZG9kZ1BOdjR6TlhZcFlUUEQ5UnNOdVR2OXpUTjhXYTQzUE9ZaGFaSkNNbnBkOE1XRWJVMHZQQitUSFFWR1h0NlRQUXgrQXZZZG50ZmxrZmFwSXF1VnlxbnI1RmIxWmRqSVMxYWVZYVN6cXFrS3BJczk1b0NydVJYQTlOK28zZjY1NkRrUGREeUFSSzc4MW9TM1p5WFUwaXFFTU5MTkxmRGtmeG5FRnpkR0EzVGs3alpNL09ZcllnRkRIL1JINGZaSHg0L1RvODM5WFBlMTV3R2pCRjY3bTIzcmdJTTh0T08zTFVsbkgrbk02ZTZHUHZENGt3cFFvUitxa2tBY3FVQ3d6VDBHdmJleDVLcmEwbzB5dnVSUFMwY3N1Yk44NUJFTDR2eS9qdTQwUnlrbGNsdHhIbFdsSWkzUFdVb0VnaWtYU080bGxQTm04OHZBY1BoWVRQUHZNZm9NT1JpbnFDc0c5RlJRdnl0OU1HeFlLcWpwa0xkWlRHRXNEc0V4R3EwR1h0SERta0NNVGFQeGRXMjh6VXprOUxDQzhkandaeWZKT2g1U3dGaUF1aWllWEtMTjBqUG01Vkl5ZnNjN0NRYnl0MWc2dEJyUkhGWGUyVmkwRW1zK0laeGdSOHkxQUFZZDhNOU14UU1HRUhsWFBGY2ZTbjZyaStJeHhMeHJpTzNtOElqMTAxVUNnVmNjNktMVUMvc2NBS21jTm9admh0bGErVys1a3NOaW94bkptOEF3NmM2c2xQb2JuczlUS3Y2alF0MjBTT3M5cG1PVFpSWFFOQktrU1JjaENyb3VLRzIyc0xTaTlxcURVQ0tyQTVBd0o0Z0ljSEk3THp0WjBoN09pUUNJeWI0NDg0QlplZ2VEc3c3UVo0amlKMVF3blpMS2FsditZV01kR3Qxa2xDZzJlN2tQenlMczJPdFZEVnErdExsekxxY3lkVE9ub1N1Z3UxTzJuMlkyYXhCVmxVMDJRbThJRHkydTBab0QzZmh0c3NpUGZJUlJ4aG9aNktOMFA1K0xuME1ha1ZWcFM5c09JcC9VbG01S0diSFpqeEhaRmV1T3ZiV09sNmNDcUVyWEZFR0pXZlV2WEdvaTdEWDJ2U01MMzNDSithLzRGUnllL1VwcUFKQTdPOXVlTFVjL3Nwd2grQnZpcDRMeUVtdGJNU1VxZmlIL1lpa09LelF2MXVYQitJdnZoNk1Uc0hVbndTWWlMWFN5SGgwTFFUYkRPNEVobythZXNKNS9sNjNOYUNRNzBVV2daY0toU2RzRnBXYzIxU0k1QXdmY2FjOXczbmJBcVIxbnBrM1pBN0NZdjdhV3ZZdFNMZGROSmVLcFNBS281ai9YT1NkTWFkMXlES3ZkazFKekM0M1hsWDVreW1TZEZwSnVjaDk3dG8rODdneTJ6dVNlTTdMc3Z5SC9FeSt0RUx3NEpqeitwaytGOTU0Z0Z6OWJtY3p1Sk5JbWhjRDdNZCttSUVMeklDS0lYWXp6bUcxa1dYVnZ2SUMxNzFBcUVQYWFvd0UwUGYzcUVUZGlKNlpjMUVGYjNBd1pRdjhKdXpQZE1kc1Jzb3JRMjlhOFB4aGFKTlV3L2M0SDdBSWxqVFNjdS9HRTJqUGVIaURJYnJUc05lVnhudUQxU2dBUmlCM1F2RUFERGJYTHZ2YUJBM1Q0UkthUHJwdHpWd3c2SWJ2TDF2TlR4Yy8zMW83R1M3VS9FNnE3blJvbU9pYldrMlhQUitlWC9zeHd2MDZpWUFiT3JxZWxHQi9Vby9yTGxUUXR5SHFwY09pT2VpamhXSDJWSnQvaFpTdEp6UEdub1Qrc0ZGbTBzbGVzNEVjSjVHSUdXeFBuL0g5Zk1scEkvendwUCtZQjNiZXFrMmplWktFbWIxY2FhOXl1WlA4S3A1bTA5REtNeGY0bnJ5ekxSVVcrMjFQUU5vd2lGV2M3b21JbXF5Q3gwdTRmK2E4S0hnWXNkQ2xFTzk4eVhDVTBNSGdjaHk2Rys0Nkp5M3R3VG5XVE45bDUwMXhuWC9YTUhRaS9Oc1htdEs5S1U4QzVOL25FblM2OGtRQTFNZncwenM2NUtSSkRsaEdKb0Q1RzdhL0E2dE10Rm5PbDdQRXJwSWxvVmFUSEdkYk1uYjVmVmY4Y1VCSTZibmxJRGdNZW5BVFZ6OHltMU9JUHRFOFFSS1VibG93M2d5RkhYTGJQeE95TjhHa2NjelZJZVpINXU4TjA4RitETkNxRC9iYlREb1JXNTRhOFE3WDNwbmkzU2k0R1RKK1pMYWZxRGFxS0d4Q25YeXJUTEdYOHdsV2pNNkVzNVhJT3p2bUt3TFNoellFenZaNUovcW9VSHB3bVZFQitLN1JKNFFyZ05CYWUwOWc3VDdydnppYmhYK3ZnUEIzSE13ZmhYcStRbkw2cnBnRnFHTHUrQ0xRc1ZvekUwYnNEWWd4SDZpVXYxZUoxYXFTRTRsMXcvMnpsQTB2Z3U5VEx0S2IyaE9EcHVQemg4a1Y2TUdDbkVzQk5pS1ZsekVYeTAyVXZQY1VIOGU0cjVuQ2JvWHNNNUkxeStwSmloZjM2UmVMbmZBM3NWUkY3T2c1eUhTcllJT1JoWmUyS0k1NFdjc2IvTXQ1UGM5Y0w5aUtjMm9yTWpNaGcyVHRMWDVud0djQzl1TUxUdWVabCswTnFORjBBZ2FjZ0huWmtKTm9saHpUZlVrcGNoY2IvcmVDSW5IekdKQ2tuNFQyTDZqd3R2Z2VCY3FQakFnRlNLWThqTDFnOFN2blBFUk8yU1FKVmc3aStIYlFINysrSUlpbzZ4SVlJNUp1d3hiRkpZTDVFdGpQT1ExTVNyS2U4bURvaVR0TGFaOGVMdk9yV29NRnZkR3Q4ai92TU9HRXVsaXM0VkhKTHpjMlBZWWo3QWt6dktsNlczeExQYmNvMXo4TE00eTYwVUJTWkFGOHpPeVBCdVBWNlZKZHFrdlhUa1JDbWt6anE2bFM5Uk9UaDEyRkJBYkRvaXFDNTNmOUdYRHdseHR4WTFXdVdsZnFheUNUZjBSYzMxNXUwTTNSMHV3cmlDWWZVSEVnNUY5NHNWQkRHUE16bU1GekxOUWZnNnUzd0FBT3MzS0lUSXZEMjdNYXdLV2hCTlN0YzFUblRoVDl6N29vZkxmemdkM3lGMnlkeUVHemN1bkVGdHF6L3V0aHA1ZmpFc0lnN1pTZWd1bWN2eTQ5QUlvNDhVQ05RWlh4RUZYc2JvY3NBV3k2TnZpQS9pL0poN1Y5OHhndU1wMGF0cThtYVl4ODc1NjE5OU9XODljZHo0WGQzakNVZTdtR1hvU3JhMkZSRFdOMVY3SHlJMVVleXFBWHFaOHhFcCt1clplWlZQVDBtRU5BWVQrcmM0MUhIV2gyTTh5QWFCWVY1cWJ3UkFEck5ncnNqMjVzU1JWL095MmRUVDN0ODZkVTN5eWE5ZmRkMHJRUC9BK09hYVFuVDlYK2dQQmJPVW9wTjJRMHN1U0t5b0ZqVzZxWXAwNEppZGNmRWs0cUtuU3laYnVDbisxZXdKblRVc0VCMTFVQzZ1SDF4VC9sdmpSd2t1WEw0Mzl0emlVeW5lNGJSWFQwVk1BVzBoemhqblB6OEJ0djkycnJvNWtPL1Q1M3IwbGxZMXFEbDhIbWVvZjhNbGptZ0FuRkZFU3dicjZ4SFo2WVBlQTIzT09UWGRVTGVkM3U5NHRpTkhRTjhQWlFaeG5mY2RtZ0NRS1dsVHgyNFIrODR5VkFZZEhLSm5Kcjl5U1JTd1lsdG81YzMrZ3R2MVBLcThRdC9oc0hyOTlSRzB4V3h2MkxMNFRCWHhUcWJEUnF2bHJqekhTTnJmamdDdnFmc1hlOG5HUkpDcXgvV09mTGVVdTdVMUFpZ0g3d2N4RGN0Ykkwc0lldU53alRWaWFvUXNlQWtjTEJmSENQNjA5MDF6L0FWdjZmUnZkYnBUZjQyNzcrYXlaQk9WVlVxWGZGWDF2WnRRWFFIa0NIL01tenlaWlVHZ0FFd240UWFRWGZaa29NV3hQQWw4Y3BDR1FyV1FONmdySzBTcXRab2VlZzR1YUg5ZDh1QS9ENjdXOTFlUEdVM0l1UmtXUFBFSDRHdm5jUGRrTFQ3a2tlOXJJaE1yUEJaUnAyUE9iVnUrN2JpbUZ2cGkzckNTUzY4Q0FtMktXMWhmRUdSd0ljK2NvRmhhTkVrc1B0WHZNbWNxMTgwU2hqcUw1bHNaanZzOEh1VHU3bmxPUUZacVpmWUcxSy9QNm9nc0pqZjlreUNUNE56dmNsRHdFaHk2ZFNWSjhOcmFMdVlLcm15NGNWWGxmSm9lMVhTWCtCRnFSM08yWmp4c2lhemcxNUZkeFlmYWhwSmRVN3gyVnArcGYwT25zMlhOaWN2TjFqMS9jZjE4dytMcHA4MFZsUzJuWDlXM3MrT0Jxa212MWNpZG5ydUMzQmVyVlBVeHUrTTVoWWxKTUQxb3duclFscW8rODVkTGpMLzlEVjNRYnhTMHFoeXI2a3NCS29POUJmWXJueS80Q2cvNHg3ZEFMb21DOHZYcnIwekpWOVp0ZW1KcmJJSDVtRE5VR3lCYnR2SkQ1NVAvT0R0M09tN2FEZkg4Uk1UNGZINnJZQ2pxK2VYYmtqcENBY3R1dU4xSGFZUG01RzVyTHlRLy91VUdRVVorbDhqbjlLRGU2eDJ0SkZvMTJkTjRQTkZZc1JyWHZiQzlrdU5CaWdJWlR3VElHWUYxckl4RTNjQlZIbUpqTGtvc0lmWmhJZWFocTdZK1NWOU0zZkhiK3BTNTNnWjNzUkNDaDZTZHdKQzZTSzJsaUYrQU91c2wyOG9jZmRIMnBSbzlzMW5vSzJHMFBYaHJUcStDZTRyNFRyVmxzY3RZQndBRVFlUmVWclU4NmtQTXg0bkhOTzJXRm1UTHVoQnVkazlmbTVkZlBRVVd4TElyYlZ2akZwa3RQMEsyZjRXbEdSQ3ViWkZzeDZMeTZsbGdPajBNQkF3clVyd3hWOUswdHVsSDhEdWxNNUErcGcyQ3FtZWg1MWhaSHpFcGY0YXBPby8vUDYyU3htdElTVFRkUk1LbmZOQ1cwZ3ZJME9QYlpDL0M0Y2V2YlVMZFdYOUF5dHgvKzRrVmNiT2NsbVBPZU5iUGtveWlUcklvaTJwc3ZKVm1aZ1ZnT1ZwRVI4ZkFRM01QMFNkY3pMb2x0bVM5bVVTZjNtMXpCUitHU0dDZXhZREtabSttcms4L0VEL2lSb2NHbGgyeFYvZEVGa3pweGVSL1JQYW02UE8rU0pydDA0TGxML3JaNDJGS1c3S2Nkc2srWWtCcDEvbVo2ZUczSmVIek8velQwM3RRM2dPT3hWMkJsYXBDaXdVekpKTW9WbXRqOFU2NTd1TCtvZE9NRUNKV1IvN1lwaDQ3a1h6bllmQ21XY2dBL1RhUm9wMDBnNnVDRng3N1RiTUNzTHdadmlDUEdoVkZVOEVnT09CNUxmRy80V1FQaEJmV2JnSnMzM29GS0R2YTFGR3pWdXlrLzBSMmNmdDRHWWhMcjlTZTJ2M1J5KzM3d25nbmdjMncvbTlpTTVudzdQcnFVNmpLRFo4cFJ3azJHbjBYbmowOUtZUWVidTBGSkdveDFtOWdzU2dpNzIrTkZLRWhSRkNDRWE1MlFjMWdFNVBVYUJNNGZ6b3RYcnhUL3JuUTBqRnp2OUZia3hUU2JYOXZYNDdrMkFmcGpKZEN4VVRXLy9QYVl6TlZyM0tnWFd4RVlzN1ZGdFJneEVUZzd3aWdDNzJtRkN5cDFLLzNMTDZjVlhKdUV3MTJvZVBvWUhxaHZvdVVQSlVWTldCM1F4alRabGZrR1dtRzVEaG10VkJ5d3E4T2JvWE1UTEMxaHBqWHFTTzJSdVdvREMwbzJEa25iU2ZKeE55b2dFUnVRRzZGR0kxZGZoYnY0UUxiVGZOODFiNDE2VXFOdVRpbUZzRHh0R2h5RGhzSXdIS3g2MGgxS0I2V05GbWp1NFNaZ1grRlZZVHJlSS9IUkVNYWFJVTZTekRoT3pyVjFyZ2FrQ0tMMWFSNzd4VHZac2tLRFAyQzN4MHozdnhTTUxSN1htOE1rUVNhVFRCUXE1QVNzQTdUdzZsMHJsSFNVbHFGZG9aK2VDbk5EYmFIYVQrR2ZoemtKQjFyQlBDaEppSWZjWE1RUklhdmxCVmxqSG1lcFlsZ3JualBxMzVhK05MZHFGdmVzdTBycTduMVlpWmpDWXE2OEQzTGdDOG4wNEo3WmFleFI2c1ZENDlZbUdRMERESjRVcXJRQzRQUGM5Nmt1N3pqK2RxRWtDNmQvbnZwYUxCeFhKM0k5YkJFWkdEQ1MvbW9sRWc5WXQwdHZPMEg0MWFuRFhrRmZpcWlaTFpPTGp6RXV1U3dCZHdIZURIVTN1MlUwOVgxSzh6d2NhL2FDTmgvUTZ3YWtCTHhRSU1oMFQwbnArVnZpYUYvNGFTN0ZrcG1QWWJzREE1TjNOWXd3bjAvekwwWG40V0Rndlh3RmlUbzZFdWlhSlo0eHQrNTBmaC9uUlI1M2p5bVdEY1BTR2pPa2ZwQUdVbHBFRzdOaW1tR2RTaU1qZjBjUWFpOUdNV2lBRXM3ZlFrcUt5K0srdFNMSTRjQlk4THpCU212N2hROHNZczY0UFZhV0lHNlhXdlNWek5yaWEwNkI1ZTVvekI4cUNpZk9pWk5FTmtnS3k0aWJOQ0lCbXRud1lsNDVwQS9SbXNhUHpSdUZCUWtrR1JHcVJCam9kN3Jra0M2NDNlV2gycFhkU21wemlnZ3VpZG1TMnprckZGVWJLaWt3Q21RT3VCQWxtOXk0SG5zTFBaL3BFQXNuMy9PSGtRQzhoSklJL05oaUo1bmYxLzhOZ2loaHo1anNqbEswTXBOR0h6Z1oxNC82QjFiYjBsbWJJdGdOUDhaK1FMWjNVZUVnMDZYRDBTWnVYTnVIWklPKytzTlphb25KSnpEUzFTS1lzeVVhemFPTlZLRlhKaHEyZHBLUXRaZmZVdHh4M1U2cWhvRGFWTm94MSsza3NhbVY3OG9UTW9UZyt1OTdGamQzdnRTenlOMS9DMGErMWR1K2JZSmtZRVpVZDd6TklldWNGb3l5KzcwWSt6cCtFYTM2QVJjWE9EeXpvSXBrTldaZ1FkSjdXNU9JOUljMG5xUlcrSzRVWFVESmhxZlFJS1hoaHI2N3ozTjhHVk9KeDJMUm5sN3AzblRXdHhXOGphbStKWW5UQXQxWlh6Kyt1ZG1ZYWIzTkFZQlZPaUJ3YStlTGxGL0lON2R0NFdncnBqbWZzemxoaFZLTjR6b2grS29oU3BDVWdSU25yTkt0WUQ5T2pGeTVpY1o4REt1WHZ0b2hVaUxlbUNLVjNJaStQZXdaMGJ0bUo1VWluNW9jWnZyRG9OdVY3dWllMlpZQzFROEtTQ2lFcGk5bGZqa29NVDRvMTQyM3BCQUZvbS82Y1JVT1h5TjRZbkNVR09YRjJsRk95U2ZkRlBiQWd0VTJEbUlFbUladG9IdWFWckswUyt1Z1N6OHFJTGdBTTIrWlRrRkN6SnRzMzNMNWljYmRjbWtsK2xVcDY1dVkzZ2NwMjlCbklpOHUxN0thdFg5aEhXZWRtSGpPd1RnMUpKTHRPZWh0L2dFWUcrZ1lUcXpJZjhrWVR2MkQ3NGgyWUdxRXROalM1QW5DcmorZG4xcWNSYTJQclI0bm9HekJHblFOVG05M3N1UDdNYlpzL0dyU0Z6T2VMbWNUSWpzVU9OeXNXNEluQ0t0bUJLaHd1Nzd2YnVFWmRQUldkK0RVSHJmK01OUlZQTEJXb0ZIdTIvRzg0WHZ3MGg3by8vSkpwTm53Z1R3WEVMVmNqT0p4TkF4Q3orc0xJeWxVNmhJb1g4SFVNMExiQ3dYNTRPSzZraUwwL3RRZDhVNFNIOCtzNWVSaUpSeDZnYmVCU3dzU2sxRDZKOWFyQjNqYWFyZWNPRTF6QkJ1U3FKcjR4ZjM3SVNtM3drL01CRE15cFFGQW4ycFozYWc1dHczRVo1UG1mUHlSSEFRcHJNclYvNW5hMmp6ZG9tMUM3YnpkZ1J3Nno5M1cweGN0RHhsY2Z1ZytPTVhMdG1kT3NneVMyRGIzeDFxSWM5VW1rNk5ndktIbklpTXpBSnFrYzl1TlBUbDQyMW1PMTZzVHZyNENVMVBGMkNYVW1kbjdWL3Job3MzcHRhZEZUY1R6MUZBbkQ5N0ZUYTVBc1l0TjNlS3NoM05rWjJFdkhGcll0UzNRUW80aTVBRHAzSEs5ZTJIN1dsZ1RKVThqSm9nZ2ZmL0tNUndxdFhPQnFVVGNBbnkvN2ZUbUtqVmJCczZBZWREejFCZW5QVlZwVzQrRmJjOGQvZkNQcFBMODhjK2QrSGE1bGhDcmREMGw4Z2FpRXB4MXc3bzVLSmdjR3dvNlVSQ0tydysyVW90UUR4dW5CS09qVHFKVWx3d3JtREluNjJkbUl5RUZJYXZldUtRTWdUYXZoVW9ubUl3TUtIK3VONE5sUi90cURCVGtnMERWeW5TNzhFVmlXY2xwRGRCYXNHcHNsTGR5R0Z6eEZrdkNWVXBMaVA2QlpocFlMeVhBR21ueW83MGNrR1dzQjA2RUlQZUNhL1pYUk5ycGFHQ285aGJ5VUZhb0NqNzNna3dQeDBJV2wwWTErS1I2cTAxMzNPd0hpWXdBdHFrbmR2MXJkT0gzK21QZGxKYXdXRG5scFBMcm81bjhwd0Mza09wcHhNVnhGczFYakx3cGVIa2lhTnlXbjFnbVY5Q1pJVnNZUjBKbUtYOXBlVWFaa0pWSGdmMEJVVWgwNGtVc2wxN0lSVVR2ekUwemIyMmpoblQrcmRETWsyeUY1R1RycUt1VlR3dWJrMXhXazBFOG4yNjFLMkJscjZnUjQrQ28rNjJzUm1LWmw3cDJRUGVZREZOdEZ1NnZVMnBYTDR5alVZTGVDeUovcHM2RTdqM2xld1ZwTVdRbSt2S0JmSndoRXR1UjhQQStUbTJKNFVOWCtPeWlyam5rZ09EMnV3WkVFTFIyZmFlV0R2RTFYenczMkRualVScUMvMHk4NGlMVUt6QlU1MFhRaUpHVzN4em5zSkRPbjBmazFqSUdsVkI1SVg5NmF2WXdxY1FhMlk4TDFiU09SK1cxOUhaS0J4SVc4bFRvSUdDejNuekR1WjVhdlc2TU1MV0dPSWgzWUcrZm9CYk5WYTcvWGY5YXA3bDB6RXNRL3JJVmxvSEFpWDk3YnFTSXY2RjZUM1pPT01nc2w2VG5lM3REWlcrNWtBNjhjWitGK0tXOVE0RkNyMVpIUUhyZVRhc21Fc25Lb0IvTGp5cEU5S1lENEZrVjZQY1JVeU1FOS9CSCtZcWo3djB1OU0zZi9ZNzV0dkYvTUhkQ3NHTjI5NUE0NE05Zjh6MmEwY2NsbEQ5SEh5Y0syamkyWGlBc0F0aEVVSVhGek9LSDV3OUFKOXhVa2xTTUhCZTNnRzl6WEloV0FSQk1zZ0xLR3R3ZWE4OFNVVUtrWFd6Q29SeVZUVkpMVm5UUTBxRU9VZWZ6OUtnVUNhRDVWYUJqamxJd1JobzJyTE5UTUZGMElZQ0x2K3N1Q0ZRUmlwYmJjYkMza2hCVC9scE0vTXZGcEFrM3R5emV3eG9lNzJYTStQMnBzT01ZSXFia2FTNDNIQVBNNVM5eHNZMVV4K2l3YzFHQ1ozWXpzeVJ0dmR3V1BHNUhSOUc3MFZEaXBnKzFBdDMrM3Y5dmtYQ0Nqb2FSRy81SWxkenQ3a0w3V3BnRWJxZGMwaXdjUWtNYmxjZnQ3VnJVMXF4Z2pDeEtmcVhFR1F1SUxibGVQNGl5QjNBZ2FNSmVCTEFMRVB4NHlteXliNk9rQlo4d0lNZHhIcTNOd1RoWlNYNVVONFBlSkZYK00ydVdrOXliZHhiWGVXdkN2RFpxWUVZYkZrUnBjT2wrTmZ3bHlOTURTdDdzWFJDTUJ0bzhoV2kwTmNURSt3SUtTaW5sTDJBekZwS2tuOTk1cDN5clhFVnFFeFRFWXRhdFdLV252ZmtqcWN0UkExNUQvdzNXM2lCaXdhU0E4U3MvQ2lQQkZVYWp5Y1dmWkVSR1cxQkx6VVQ1UkpOZVdpM2picjJLSlR0MW41WFFxaXRqVkVoTjlER0cyYzc4OHcxSktPYkRoQis5Vzc5Vk9TaXh2NTZmR2RWLzlKbm9ORlh0dEtjV1UremJVS2hGaFYyUmk4TDZKZEVoYlluKzBGSFVDU2pXa0k3bXM0TXkrU2szNUxSdVk5Vi9yN0hEdDJUOFd3c2l6TWxCRVAwZVJCMktvMGhlWU81RzN1WkYyZTh1cFg1VHRuNTJPU3ovbDNWcVBFbEpvVFpOeUVpbzFTVWFUbGhrMGUycllqK3hmbWxtNU9nYTRNbW92WHlzMCtHMzBseHlZWFE3dVpmZnN2RVNGNSt1dzRhWjR2QVl1dUNQNmVBbm80VGFjd0VheUdFODZqMzlmc2p1TDNmaDFJazdoTmpPZC9ON2MzeXVCWW9meE43RjYrVS9YbWp1TGcyRklBNUFSemUwTlBSb3o3NUI3YlBjRWFpcmVPKzl4Y1NhbmwyZ1M5UVZwUFdPeTZnVkRiODlvTGFYUWVhUGt1UHpMcjRoSGZpQkNRRjRlUmlNakxHdCtDdEFNU2RwQ2NOTGpBK2I1QStXOTNYUlA1M2Iwck85VTQxbWRIWnRleER1ZjZmbkc4T0ZHRFR1ZVJlMWZac3pVd2NjeGNqWXQrckNMRjFoNkdBRmxmUzM5ZE9iN2xwVmxubXZlNDVlZjNHL1E0dEg5K2dKRm1aK3o1S2l1SWU4TElIR2VGWTduOEd4S2VmZXMvQmpoSFd1SFJrMlIrcy9EWXBxU1pidU5iK0F4TWE4dFB2MExHdXdDTUhDMEpRQk4waEx5UlVJQiszQVlXQXVpWXNrKy9uT0ZHMnFpWHc2YzNIcmZpVVB3Z1A1UTFvSjNoNUhKWnM1TEgzd2dCOEtwQkVtcDI4LzgvM0hEWnBCc3JRUXgxL25yblVOSVJTMWFvUVpaMHM4aTh0Rno5RGdGOXZ3VGtuaHkxOHFpMzRyUXFObTNkcTJIOXlFdWZoZ016TDNYWUhlb3Q0TkZLaWw3d2NadFJyUHcxMUtBa2N6bzJBWkpOVFprc2xLeTRHd1AzeENmVStrVHg3cjlQeWE5MG45Q01OcTg1Qkp5SVlZaEhIZ0taak8zSzQyTUJsV0EyR3BnUEsvRDJJSnlxRW9ZR2JYMlkxOTE1Q0RyUTMzRGZZWDY4cGVmczJiYzIyZ1JVbDNsNXMvV1dvTUttWWtQb0Zsa1pNYVhZbEEwM0pBeGE3bTdHVkJSdkh1YlptTXdVR3FzcElGMGpyU1B1czNQMHFkbmU4QTdOTVhuRm9HeURSclN4c2ZQVnRKR1VnYk9kL2tFZndhUEJldmdFU1A5eENiUEpINDRuNlA5TGhXdjFYL0xoUVAyRDIydUpsaHRrd3lpNEpqaFovVmNITW5CK1h1TFhidmpLaU1VZDJ1akw4ZzlVaDdieHQvOUpodE5CWW9Kci9mTW4xUEc0TFRtWEs1Rzg5RWNNODA5a2QvV2dSOXQzSkE0Y21haTBIV3d4d3ExbE54bWthODFoK2VVdElOSjBKOEwzMkVkWE56dm1jeHNXVjFaN3FpSEhQZVFBcWxTSWtXUGJhWkE1TDRlS0ROSHFrdUlwOE1FUjMxN3B6QWxCc1dMVFN5VmdjQVhJTXQ4U09SRzZLRmtSS1FQL3UrRzBaeTd1cGNDR2hRWE1aUkM2TlNzZVltNFBZK3pCUWtYZEdhb2lWcWF5SGZIUFhJcGFNMzN0TjU5Rk52c1dYNkRvNXhTK1JtWnBRc054VmdGOGdvYzJ5L1diMy9Ybm5iNnBYZEo2NjI1OC9jbit4em4xeUhYNkZOUEVsN29TTHJsL3Bod0h0dEJIM0hNOG1GV3kyL1lYOGVQS25rNmhUSjcyM2x6Q0VkbjdQRVh6aWZ4OUNLQnhUMGpDV1lmUE1hblY5d2tFNWc2VklVUTFRNUhXQ3F2SlFtRDZpMWNwYVFvTldsSnE5cTZyWEdCMEdsWEVWcUgxSFQ4OEFhUjVuclVPOXhPMGFsMkhRQkdLcDdmYXVxQmJaY1l1bUVBTmQ4ZG5iR2ZxM2wvQUVlODcyVDRSRkJjcXg0dWxZMzBKK2JkZmJuaEFKMEgyVjRtQkZ0TWVKWkR4b05KS1dKQWp3OVhvNldUOUVQUGJ2MndRRHZFQmlRSVNMOG1lYlprZ3RjRmRraGRDTGd3WW96bG5zemR6WmZvU29kQ21NaGV0ZFQ1Y0xra1MySkcraGZPZGQ4enVySG0vYW5RNWIxbjFNNURJeUM1Sk9aRUVhOTk5N2x1SmVlazI4YWJ2cG0rNzAvMFNsd0ZERkcxSlphdEp6dkp4NkNoY2Q2UVFVaXl0M29yb3UwMldIcXp0eXBESWNMaHhFNllxSkpCcUlIQmxkNWlaL2hTTThpM3A5Y2RTODFpc1cxSS94RFkvc0s5M2ZGS2F2TmdhaDE2VHJ5WTlrWmFjL2xiOTgzSVJ0UVlwQWRBRGNmSzZxdEpHTDhFZzVRMjI4cWRRTDVKTlBuZ2pmS1J5WUs5cUpZS1lYMUgvdlk2RU1aY3V6NWtreFY1bkw4SERmSThCL09LNmdWaVJLSEFnWmhkZlZYR3M2a3pGRVZNMnZWWUExeHo1aTNnZFpDci9MVmR6SXlXcXpBcFdjaFcxS1RVeCtlaHVsZEFnVEJSOVl5VjBZVStFNFJzSW1EcEJPNDZRa1JJQlNlRzg3RGNVYzdWbjZJWlM0L29kRGRUNS9DVEhmZDdIODJuOTVwMjAyVjdHVHByRUJsK3RYekRnKzRSMXhsZkdOQVBvbU4xUzNGMFJaT0ZDTEtPRTNUeGdPZWR3cTQvNnFZK3JidFgwZDNUcnhKNTd0WVFIVHg3NFdRYUtoQ1FsQ1AveEVmSGhKMFd3aGJiTVdBS0o2OXR4UTZ2SjNWcXJEb3NKVVVVQy9ENUxTOXpHcW55VGtBejREcjd0ZVd3SDhwRHAxdEw1TkM4VFlYZzJFdnpRM3hpTmtIVHlWMDJPM2RqMjBvcVRnTVZSWDRwUDFOSjQ1bm1LMTRUTUZtZ0gwT2RWZjFVQSswc2VVWWdyVWhZSmlsRys1YXZjWGNuWmZOWFlmcVhDM1Q5WktWSm5YaVlUbXp1SlFoT25ZV2V0cmtGT01hckN3MXRCbDJGMUs2ODNpRTlBQW45bDNVM0ZnUHdZTnl3Z1pkN0I1RGJvMS8yNFFTN0NIcDhlYkQ0dGx3Sm9FNkxJbERQRWpRNk9QUk80UzZhVnVDZXlaMnVzTi9HRmxTUUlsc2dQZWhMT1prY0hYMER1NzFvOWlzZVBiZGpUcXN6eHI4dGRyVmhrcWtOWEpHVXpyOUZJaHVzaUdPZUxjdjRWc0hxZjc2WFI2dmV5S2hsVGpqczlHeXBXbjF5bDRCRUk3bk5OYUt2QVVzU3BwMzdxMklwaGRldlFlenNmUTFlNkFibmtBU3k2Z1hCQ2ZydmhRNWI4b2FIcGJhNUZTYnRYM3JSV0U0Uit6dXVyVlgybzZjUXdFNGZ6eEJMcnNhRUNpZS9QSE1vb2VrOWNING9iT2hJMDBMMXcycDlkU0dwS1pyaVZmdXROQklJSzByckQ0T3ZFSWFuRUoxc3JLQThzeXM3dXM0ZGwwMG4ra3pUOHh3dmVaN0xtQmdvMDJMOUlsSWptWHNPL2JEa1BVVmt0L2ExSzVSaDI3S0VlbEkwNFhON3lYb2FWMHhteXR5RHdmZTlJelNjSjZFNTVHZVhRLzZUQVdORFg3dFg1cGNIOGhHZGppVjV4ZGJTY0RHRTVtZVpOeEIwb0ZLYUlEbGhlMmxpMWVOUEQrMGlHcm4xUXY3KzZReVBIc0FNYURuNUh3Yk1SMkpPbmdvMTZPaTB5c1FjcjE4WDh6OW5FdzMxODBENnFQU0ZUTEg0Z1RwbUFZQW4ySFBnMHN1a1did21CVndJeFZESDhwQnV3aUZwemxEajRVRFpoc1U3bTY1VVF1S0QvTzdseWxBZVZ3Y1ZSQUdpRnR6anhVY0RFLzNjOXVXa2o3YjBJWXorSnl3NHowaFVsdmMrMWhKT09xTm5iYjlOUjJFdnpGOGpCTDQyMjNHTU5YcURjRC9oNytZeGNiOEhFSmo3MkVzL3hidThWSTBHbU9mWkFBRXZRTEdudDY1UnZneXlzTy9vVkJoeXV5bGdWWVgvQlFyVHZLMVJvRk5KdFMvVTFabHVFdmwwSnB1RDZjV1dGeE0rTWZqcU9kNERCUnVmUk8rTTJsSFUyditOMDJWMzdtNTFoT3NsdmsydWJjcnFsRE5LcmZFSTdXRW0wZEdEVGs4TGNhTUllUDB1eS9WV1BCR1pkWDE3a0UwQkV1bGNzNXJhYklzU0xxS01MYWRNYzhoUjU4VTNNa0lNRW9OclVEekVseXpRcDZEU1dxWE1aQjFsRVMzczh4U3ZSYU82RmRVZ2hlckZhdk53aEp6a1F6WFlVekFQTHBnM3RGdlRJSjl3ckROb1JPMDVtay9KUDRZM0c2R3kvSkpCNFFWeXV6K0hnSjNPQVZZRVI2eDVqVW1VbGNSKzQyY2ZSMVZkTkdvMjVxbk9ENUE2K0FqNVI5VXVHRTB4M2laSk9nNjJ1aDA2Qk5jS0wwUTdPMWdSbjNVeWo2dGsvWGk5MWtwNDEwTkdESGVkRUdQNTAxVldTV1JaeGRFRlltV0ZnM25RRlk4WG9jZWNVbEVETFZYcXVSWHhCZXpNclFEc2s5QWJDQlNKRE14SHJqR1BybWVlSkhSdkpWM214a1ZzeVRiSnREOWRkeVlxV2Y0MzE1dHhhUnc5MVAxNDRwVGJnK2FLWDhkRFUwa1dEN1lUQ1FrY2pJNEdscms4TG5NRmxNTVAxUzdYTkZwckFWaG01V20vazh4bkRIeXFaMi81Mm92YmlnMVduNHZEeml4cTFHZWUvRThXNzNkVGJvaWU4NUNmVVhXUFA1UlNLWjEzSnd0d0VjMkZsRzI4VzFvSVU2NitqelNTKzFSaHJFemJwMElUZEVvNlI1c29pQWN3RjFtS1RGS1h4Y0NZMjVXaTNmbUQyU2tBTlJGVjZhSmRubWJaYkp2bmZycUtIMnFRN1IrQ2VVK252VU1odzBhOE5hY0FRVit0cjZGNitBaFhsZzh1ZEF4SzRRZXA0NFJMNFNiRWJ4Titpc1hVSjlsbkt2Z3EyU3lBZVdLK2VpeTk5aDA3MHdhU3V6d21XTmgwd1UzamZDYklBR2RyS2Q2eTR0MkxLSjgrSTg0a29UOEV5bjVuNFZxN01iQm15aHhJRkFidW1RRTFlNVFGS0VoenE4cXhVdEVVRFR1bzVWSVJKS2RYS1BYUi9OR1IvUm5YOG5abG1mb3lhaFZPaVQ1THgvOWhHbTNPT0pDMkY5VzhUcGtHREVvUTRua0tSOFlzZk56UEw4Y3V6Q0lHMmVKTURsSi9Jcm9qSmxTVEtTa0RHQUhqTGZBUUN5Szc0cGVUQjFsTktERUREeHFVYklyelMzc3JUWTBBUk9rMVZyM2w0bUR2TzBpYnFLWDdLTGRUWHdLWDVYa2JzVG9xNVZaTGp0bklISko0NTFDVFpVa3pBWGVzUnVSS05DMDVHRzZBK3lJV1dFS0NnRXpHUmRXYmFqVG10b01kR05RUGtuaUpNZWcxL3BKOExvTUVXN0pYTThkeGFrOEVhcGYzOHhic1FwREMvNEdFRHhDQmlhQVhqbzFEaEdaeWVReHc2ajNHa0JzTzYyQyt0OURRLytZZ3FpV3dBR2d4RU9teHFjZXVuWGJhdGxlaURWVFlaN1JGdm9sQkwzU3V3N2VvWGFsa3RRWjJjM2RHeWg0RlBoeXQ4SkVnODJtaEwxbGFjbnhUU2ZkZTZVZFlKYjVZeXVYdHYzMmJuZkoxdDIzcGExcnNVckdKSmo3cE55amU4VG9vOTAwNStOSiszNnA1VHBNS2VFZ2pjcnlSSm9UWGJkWm12OFV2TUlYdU1Kcy9kVFN4WkhIRk9XUlN4REdmTk5FbUNOdzJrQzY5d1psS0FTUUVlS0M0bWhNYnZVTGJwN2V4NWNJeXF3VDRsRGtacnhVV0tZUVZRQzBLWkVMU201cDJ6bHNDNXNRdDM5cURva0YwMDNmNURiZjFpdWxncjBRdHpCbzN0SmgyUklzU1gwZWo4dUtMeUE4VStLSm5oK0EvNEtLVVdzb04xUlZKTk5oMk9iaERyZjhpbEp6eVU0TUxVUlZnMFZGVDM5dExDR05ZaHVBQTk2Z2dWWTJQOEptcHd0QXBiOU5aTnVyVlZOS0kyblNOK1RRQkw2dG5pb0Jzb1RsSC9rQkxZYWpGNXp0UEFKMnNsaGp6Z0VYRnpscTUyV2hGZWlHRFFlbUJIRHZrbXF0R1IyNXpkTG84NStpbGYrQU9TMHdvTG9EQmZsMklOaGlJRmt4cnFoOEJIZ1JYTUM2SzNJSC9pSHlqOHpJNFhVV0MzY1VxNmVMbm5sS2lFYitOUjgrNGQ5Z2NlRVRJODV4UXlOMTYvNFkrWHA0SHFjYys2eWxkczhvZVUzc0h2OVJ3Um1VZ0tvYzlJUm10SWJ1M2R6SmVXbzJOa2FoeUpkVGlicmRMRmhKUjdiTlNzcHpkYWlZUWVmRDZJRDIrQTI0VWlDU3pKelFiSktmSkhXcENZbmVydXZkRHIyVzZXNzI5YWRrV1BJR3d4cVlDU09RZzNuV0xpdDVnWiswVG1PR0FrUmxzd1JReDMrN1NHS1h0YUFmblh5U2swL2R3MTNxc0wreXZ3aG9tWVlyT3IvTnl2ZEFjMXUxdzcya0doN1B4REhCSWpBMVhKdE9sYVZMV2FrMGJIZFBkR2hpdzRqUGtFMzA3VS9YeHhkVkZFYzYvZmF4TkNUZWlnM2srd1d5TVNPMkpNVllISVpxczdJZ3BmcGxrU09kczUrZVhRVTN3QXJUTHNrWXFFbytiTE5pZjIydkVYMWp1QlpQOHFHWUlOaEw1aUxwYzV2L3BJMzhZaVdtQ2hxY0EvNVVLQmZYTVg5RDNXb3AzamJzSHU1SEI4c1JKUWtiOVc1cVh6Y21wcHRWTkFVL29ubjJzYVZwaTRnbDc5M1VQY3NBS3I4c3hSZGJ2ZmlIZmRYeFVBbEJaclY2ZXlUMlVpUnpCTis1MVhkeWwwM1FrblNpMkM3RFNTTUhxVGhZNkdkQTNVUGxWbWV3SmRLbVNMSHBCcGhJdHFnNEVXb3BpcHhCWlVkVWJwSlIydkRFRVc2cXRyZjhrUUhpOEJad0t4M2szU1QzYVJqWXFJaVhYc0IyMVJnYUVLOEJNS1FHUzRQaCtQSWhkSlNvU04wNks4NnBPK2hoSEZLSFdCdmRYZzFWL1gvay9UREh6QTNlQzZzY1YyUVJJYncybENzMHdzanFrVU9rUXAzVkxoaEZOODZHRnlGZnRLenBQcTlNMUFkdGdkNU9WcEhjTFQ0Qm5ucURiL3Z4cHBaQnpJU01HUE4rTWN3cHYxYy9YdExvbkl1QmlEZU90Rmh3eVhLL0x3dUF0azlIbjlHNWhPZkZtTzU1T3UxMS8yYy92ZmZtKzVSMUtxUDBiandvTTM0dkw0Wnk5cXIxMSsxRS9jeEI0YzNSM1ByU2lMTVZ2QmVqRXZSaWFEd2J1b2FiMkc2aFEvL01zODh0Ry8zWk8rMDhYa1VJZVUzMG1qdFl2b3NacGhMT2pyblN5Zkdob01Nekk2M2RVSXlYVmlURXEreEt4RnBteGJaRFBTTVBXK1ZZMHdKRVBqbTNKRUptQzhxTDdzYUd4SitRaUVxUUNRYTNLNURMRVQ2UTcwek9pR1BtOFo1a0U0U3d6b1l2bDJTNUVSUjkzTm1YZDFYODNzTzVZYVJ2S2tCV0gwWTFMaW9zN2RSNnZhVWJsSnMyYitOQTlNTDhHVURQQWc3Z1pCOHVvUEg1YkNnZnBxNnRWUjNlbVQxT2Y5R1JLdlJrTHZHK3ZDaXZhL0pUTUxIWkgyNGcrQUlaK3hUdnpyL3gvVjNqeHcvVzFFYnRwKy91dzc3c2o3QWJrcXlkdlhrVE12TWZQNkYwZ3p5OW90YmdxRy9MQi85ZVBWYXVmSS9nQkNHQnRwdDNLWDdrMjdWaHNUZGxmWGxmS1M3YVk3Y3NlWkVvdzBiczhnNWVEWWdmRXJkdGhYRHpBbStFb0JvUk9VZXg4d3o5b3RCSi81NXZLQUJ1Nit1enlBaUZEWG5pQXBGK2NqYlpWUjlKSFo4QnFtcklvREZzMnVwZkNDVFVuU0sveVlzSjNrZkliTzVDQk9iUnJNUm5ERUcyb0JlSWhiV3lSZ3doeTVpdlhWWFMwRHNUSmpxbEM3UTFNTU4vTUFISmtJTE5oQjY5MG9yVWR2cHNEd3RIQ0pDSFBKamgyQ0RIVUFEWmhrSWFmejI1TkNtUkJrcUxRc09TOVUrMWRFcXkyMDFhMkRKQ3B3NS9wdzhpVmM2am11SllXemZUQiszWktrUWpJbm4vd21ibWZ0SXZoMmF6b1lCRGdsbXpJVE5RRnhHLzdmR0Y5aUFCVlVMRU5FUWxuajRtaGNHUTFzbTY0L0U1U2RMd0RHMk9iT2RZVnhTYkRTUXJzR2NQQ3pxeEd4OCtUZ0hscEpjajQ2L2NjZitPQjd5QTMzMnE2RzFHQ1kxK0VESm9FOWdLZnJOMzZJaVNHN0VISmIxNXlxY1ZNbm55MnE4V1JVeDFpcFFwUitSYk9vQ3lYZEpZdU0rSCtlb0J4TitMYTN2V3R4aEMvRWM1M2JvTUhjQ3RvaXgzM2RpamQvYVg5SlZkZEhEdVZvVG9QZWxjOTJUTE5xU25ha09zdXJkUm84MmQ3d2tGbUIxWEhENFJrMWJHV3V6TTM2TWRaZzBnMzhHTk9wbmRZWjE2ZlE5bkszdmlTZU1yTEhwbEkvUys3cFI2YnZYWktUQ2RocEJPWDlZUjVBQ25zeUFvMDhDUEhlQkxxWWZraStTNlpaOG9zK3g5NWZkN3lzZCswajYzYk12U3NQVFVvUGNQQUtSRUFVM2txZmltQ0grbGZLL1BTK1U0a0VBdUFwSkcwTlEvL3BYdkg0ZEs3Y251cTRSTGxiV1ovNUJTbWxmRGJjK1RpOTVFWnE2NXNlY2NacEVwNE5wKzF5MWI3cnE3YnNWS2tpMGwzNTd1MzhuWTYrM2dyeGtJM2tBRldxY1JUbG9GK2tYcDBHU2FjTTZDbi9wNTlpZ2MvTFZIR3ZRZG5BUlVzQlV5N20rQ2RldWhyZHFHYmg2cFMwVys3ak5uczhqZWZ6Nit4WEV2blZzdTc3UDIzS0xYa0dLRE9iTkdBR0o0cE1tNWRyREdIRm5Jd2NXSmJ6a1lrMnRtVmJJSDk5UXVTZGZkRFZqU1NhMlZaSjJNcFZSQStKak1ZbWFvRmw0ZzhuZ1Rma2ozSnYwL2YxTlk5d0RZblpmMEZwV2Q4YUtLc1k1MFFaM1NRRmFPU1NEaXNJNHMwSEVOVWQxU1VGcElUR2xpLytNcmVId0R2cVc0S2Z6Q1YxYVpRYXlqanRTbk9pcHl1K2UzK29KVGw2ekRkeE5oSDFBNG4xV3JOVy9IMHltY0tHMk1iV3hkREVCZjVRY0RDd1hxdlNQbzZtVzNRNGhPWnRWSTJHUFBBUVkwM01jWjA2M1poR2tlS3h5R3UwOFVYNDdlV3lMa1NCYVJwZFdYbHZYdDZwK1FEWHFIMmxDMVhzUk5WQU02UlNyYzNnejI3Vi9mNFFvQUVUY0xTbUlZWVc3SmhxSTk5SGlPTkxBZDcyYjFrODJuU2ZzekUwcS90VzhkMFNWUStGcXlLUVIvd3FocTVOb2pCUTRZMUUzUk9HVWxQb1V3NzJ5eEdrT0hsU25MamR0TEhJaVVRTFA5bzNKamdLM2ovWnNOK2lvSmszeGplcVIrSFJtODE5endCQVJJNEcvME0zRVAwbGZaVkVteUFGWkZoekNYSTdKQ0hGZ0pPNG5jeE9nVlduUW42ZHpiWE5UN0N2alNHbGd3MXRVdFhqdi9yS1V1NHNaVHgyQi9KWjZXVk5XdkVMOFZUdWEwOStnRGZBbUg1WlAwYmg4OStaYld4YkZwbUVLeWduZ09ZaGRLTWRVOEg4cnVPbHoyUHlidUNxMXFVaVpUUjdGa2ZMY0ltT0hHZnhWUEZXUjVIQjZMOFloN2hjWWlST2cvK2VYMzE2SUx3ZEFRSnM5R3d5WmJzSUZNcGNiM2VHaG54eFkzZzlNYlh6WWc2QVRWbzM0R0hMRVIrT3dkbFpPSldPWVdqSlFaOUxtVndEeTFGTGphYWR5ZkdWbmRQYXFCc09BR3lBTThOVkNwTml3Z0ZPZFp4TXNDT2NsTUhmaW5kOGpubndydVFsZ29TdVp5b2Y2d1MzczBiMURxUHY3R1l3RzFnOGJ0YnUwbU9IQXJOekZLbDRlcXpKTm5CQzl6cUk3ZktpM2tqTjVuRzhkdDZlTG94OS9YNmJhdUc4YXNwVkFtSnIvWHl4Rm03SmVVTlZaczE0Umh4OUlmWk5FWVAvZEs3REkzRi9EbHdzNEx5eGY2M2M1ZFpzaW1YREFkYlJnTEVwdDRmVW8yNGhFeTM4ajRvcGt0KzFvS0JUZU1vcStMT0IvZ2R4ZkN0amNXSDRLcGgvY3Q4THV5ekZHQ1I3OHN1RUJCVGFFRlluRDMxRlVQZkk0YytPU3FUUVMvRXp6c0lxTkFPOHBUM1FzdFh2VytnNlV2ZFZLQkxzQ0kxdkxPTmtvTDRjUXpwbWk5bEhQSXF3a0J5cGV3K1l3OUdDeTkrSWNwZ3lYRDlEQmNJSTl5eXQ0b2dDNWtibm92bDErRWhiT0JPS21TMHVOYTBHaDRUYS9OeTdrUXdLOEMwN1pOUlBNWXpxM2dxaFIrdGMzVHY4bWxJYXZQZ2o2Zy9iRFJ2QU1nd0xxTk9vVWZFTitPenJaS3VkN3IvMEprT0hiUUtQUDgyZk5LdXR1TWVqN0pES1BhTlVDNVZhb0lTdzE1eEpUZ0twVGRuVFZNSkUyTzd0WHJCQ3lkSXlxSkhQaldmNmlpS2V6ak5UZVcyV04rMGZ0enhib1VwczYxTFNTdHFpVWIvSDVITElQeWh4VVBDaDJYNEJtVmpHR3pWZDNKdGVaN1BQM1lyTzFlTFN3ZjRmUkF5SlBzTzBkWGJ4c1JyekNwaElFdXljYmh2THZWczVKc0EzMkc0RHVQZDJ5VFoxOWdBT0tGb21lUFdtRXAyMkpqbmpYZnVGeER4bHFsdjhhVU9OTEc3SzRaVnhCd2lCUThQSWJvODV6QTJlMFFvTmZkeUlWODYxNXVPUml6KzNXMlUvTU5BNFZEUHdFb2pZM2JRdjY3b1ZsL21tMmNzZUlXUDlxM2lnRVdjWFdOdm01WGhJU3dsZCs1V251T0hFdTlaT2RYeG5JK2xERG5LRE1VOU1ZY2dKelVSSjdOUk9lL29ocG1xbm9CSHcxenNKTlc3TU9wWE12N3FkZ2dhekgvWVkydGNaWkMwa2xFOG1GZjliL1RldGNJbTZodU1ZT0MvSDZyM2lTN0VLQXpqMGEvWkp4bXZuVGFqZGpodXRoMDVsbngyMU1wQ2dJR2RlUUtJeWErZktReVhEMU95RTYxK0Q0TGZzcWJvVUZnYk5iT08vc1EyckRPeXZ6d0REenFnbWxJMUovTVFxeU9UVU5PQ2NBQUZudzEycjJUeHVUVFYxcmx2RE9OZlRMMWMrUXE4bm54WHAvWEZXdGx6dXdscnJ4QWMvVFBabVVOUnlaeFM2ekZ0UFFwMnNwME1udU8vUzl6SmExT0ZlRlRPQUpXaytvQmcrK0NwUGV5Q29OcGdRanZ3Sy96ME1vSTlTZWMza0FkUnd6RTJnbnVOQzY5b2pPT050NG9GZDhOVnNHd1UyU21ScGJncW1ZcG9qeklOdUt6c0ZjeEluM01nZmk1V0MzZXVQZGFwN3dsajAvTERIOUpNTFdlUVYrN0RrbmpDejNLSFh5bGtTZ1YvTm1vanJGN3U4ak5tWVVtSTRoamFpVlY0YXpxd1ZhK1I1K1lmdDk3QlB5bk9SLzdYSzVmYjE4NEZ3SCtzSGFIMVJ2eHM2TUVlc1IzMTE5SzNWdWdVOWszWk5sZjNUQVdpWC8wek5NcWRzNGtsVXhBN1JyeXdHMm52YkNTR1BpSGtMR2xWZjgxMk8vd2NLclRtYlM0Z2xOdkk2ZnFKV042WU1BdlYvQlErNDJtRzVwTHZjbG9acWdxTk9oNW5mTmdIY1BNVXVwdUtoeGZjYzc1VGEvMUozZVZrMGZVaWIyLzA2UyswWGFjSmVQZmg3R0RNc0w3aXN1OTBRSnJvRGxZZkY4U2NMdzN2UVdlMTk4bWlHVGY3eTAyYVpiQUgyY1JBL1RucS8vcGFUbXJ0R0RqNEF1QzAxN2xWaUJuZGpNS20xaVNGbEVia0thTlBjRVk4WnRRcTBpTk9DYmVRdkZFemVHQnNwN0J3c3NCVHpZcDJLL01JSTVKV1luak5kWWNEZ3Q4UmsyZ3I3amF3SDlPSjRCTm5mNmJVaUs3ZnhWREVWb0ppb1Y1ZHBnSll3NCtjREFvOUJzektaeWErZ0hhR0hMenlnb2llUDRMNEpSdVdWbHdPMFNRWmxNZDlGN3krZE85YVZ4Tkw5WXVQOFdURkRwSSs2SEY1OXlleHBUOXlleWJCd3M0ZUErMW90UEx0ci9QWVNsdlB0VHMzaGlaOWJXZ1RhTE95T2QwR2ZMZmp1WDBzMmoyNmhQT055V0N1OE9JSWtTNmF4RFVBYU84TXpLVllSb1FNNFJRWUFWQnhBRVVlQ2svREJoRlFWUlFEQWlpZEM0YklnWXdhbm9zQVBwc2IraXViL2ZFR2F0ME1IY2JORytoeFJWQVVZMkc1MGN2MS9hNXdoTWVqMllaR0hpY0lMK0V2aWo2ay93Z1p4ZkcwenNYRUhseFQ0bTc4MC9HWFhrTWRWM2lGQjZWQWNURy9oSEZldmNSaTFzN2t1WGRGRU9WZjQzd0NRNG5qemhEZ3ExdHVhRWpXL0hTK0orM3NuampGcm5ZTW1PdE1ndDZQUUNkbVhpeWdJMlFhaWxvbEhzaUdWaG95anpEUUE2S1phWnF4NGRTaFpjN2dNZjllZlZLUmdrQlcxUHpaaFFrZHJaU0JIUHlPQlFVTFZLQjhmTDkyS0xLNDAvYWZOUWg2bk9tTGlGcDNVU2lGRUFSMlVrNGN2QmdTaFNnNUUwb1pCd1d5M0lka2MvQTFuL2MwV0JzcTR5RTJqcGR2N09WZWFMSjVvOStDaE9MTmk2M29YQWh0UmFna2wyQUE2Vy90RHRaREF2ejNPeHlxZzZVaTFvbExzeUNpbmwvb3RYLzM3TDlxVnZXK3VMbGIvRjlhVmh5SW82MHZEN04zN2JkR0xORnJpYW1nMUsxM0dObC9kZlI1RFVGQ0toeXA1QndvTDhkSGNXc3hxZEhYRlFJeTAxRVRJWFFsM0lCUVJBaUZUSUdCOXVWYmhyMFZpN1ZuMCtTem9oLy80eGVTZFFwV1kyRXY2OGQwK1lDNGhTVEcxN1pmQ2dkKzh4RUM0K3N5MzZmL1pOVHFyK2lWaEFqVEEyTXU4Vi81cWdwRDNOWVpMNVdOcnBQUlNlanhQcitNU3hLc2MxMXEvMExvb0dqNXphWCtuQWVmWW00ZW9JYkp5akRBbUw5cnVMS0ZnMk5MWHR1OVp2cmc1MTdjcW1QTFI3UVpYc0UzajhBMjNSYnhTVDRYRDJqL3hJdWlrRXhYRHY2ZlNWV1NHNjQvbnF0Q2tZYklYSFpMMVpSS0hmNkxlQUl6WW42cW50QjVkYktOYjVBSUhQRzd3dEgxTDZadWdUZjNkZlR2Z0c2b0xGMVg0SlM1dm5DYlV0Nk90Yko1QlhhT2dzK2tYVG04NURsd2o4L1E5RFhSRlBTNklWeTVud05SajZpTVp4bTdXcVVsSU1YV3o5WmxoSlNYZTl5WlJmemcyZkUvVnJiWkgzSHBuVlNjVlZFMk9vZUJjTU45aG9pNlNlRGVUTlhWVXVhYU5UK3R1cVJXWmZYNHlNcUlFTHJJRVBpRnVMMFR0NmJ5amV6U1hUUVZoanlJTFFMYndwZUZvcXpUdkhKSG5lZ0c1N1J4YXdoZFozak5YcjZtRE1jckkyYnBLeC9vanBub0loS2Q0Y01CTCs4Y0pYUHVxb2t4Q2JSSlIyT1lsSmEzZ1ZwSk1OL2RhSFNZWFdYck8xeG16dzZpZ3JQV2lQNmlXSFkzU0ltbUVPNDhUTE0zckQ4dzFZcGxZNzN4NHh2OUtHcmhJRzdqV0ZRQm9UNkFGKzFWOTFSek03RWJ5ZXVSc3VkY2k1SnllOVJTWGFZWXRmeHoxb3J0R3dQZXFzMjluOXZ1dTlzRkgrQWhSbDB3Qnc3MjdscEF6M1BOWXRjRE9iR2JBTWdDaGFHaDloOUIvOUtYMFhlSG0yM0EyTnp1ZWFLNXJYTDM2RnRwTEdvM3p3M1VUT1I3YTZ6QUVKdEZ6YkNPMjR2UFcvVkJnTDlKemM5TVRCYktYRmlxVDF6WlVtQmxqeXFzZG1ROUJZVUxvTnVJdmRlK1RYWENId3BNeWR2TVhMSCtqcHl1d0dLWWc2STRyeGdNT25VNDJadWhGemQwa08wbGt0eDF5NE4vbzRBM3NGeTZibnRlbFFJZXlKSUljYUNPY3RYOVpsenpqWVVkNmVodzRvOGlVQzdyNTM2TmxUUE5VMHpDVkVEbFQ2ZUdiV3dlOVpMQjluZkdQeC95VUpTNXZZSWhKUDJQNlVMa0k5SjRReFBRemc4Uk1rb1ZCZVVEVHZjVGIzRm9peTF0dGZIZERNMlVINVE0d2ZZSXZjemVKQUF5UHV4VmVXMExUQ1JhdXhNUk5tc2tOK3JMVEh4SE54azhaY0dSSVIyZHU1dmM3bXdDc3MzRWFpajRGVzl5QkVZZVVCZEpCQ1BBVEV6YUEyMjgwZDV5SWZEcU9CM2h5QzZrMUtQalE0dmhHV3ZxZ2h4WUFCQkpJRlVuemk1MFM0cThHMmxrekh0cWZtL1RqTlNSbXNjUEZneVFOL1VSRkhpNm85TjFDdXBDcnVhQUpNTnZtRnA4RFZ0Nk5KU2xNMXJ6R0xOMjZRbCtMczh4TzBxMm10UGx4REhlb0ZMZDRDamY5TndzZDZjeU1lVFRrWGdGZkZSSzVjZXJkMHgyeXk2SDh2VG9FZTNBRDlTYXZzN25BZkN2Yi96MjdkNTVoR2ZzOGNYYjg1bjVVaFg4UnJMQithQ3F1MkJxYkxGTEQ2ajc5N2tRMjVXcVAwODZJdGFDL1R3bDVENFZtSkxIaWlEbjhRcWdQOEhqTFp5c3FvbGRnTjNndjJOWlExbGlKQU56UUllbXpaL1Y2RjFMMzc3YksvRksxQVF3ZTNNc2l3RmhMM2U2S0c2R1B5U3ZlUDhlUS9jMVZsVlBNTFNrVXNJRnNLMG52cmU3YTM4S05JRU42NVBZTVExTEFrWGRCMDA3SUNtcG5aZ1J1bWcrQWROL3o2UWZ6QzMrZDdESTNxSXN2MitzdjF5NHVORzdJSnJlemZ0TWNUUEx4N0NtN2NzbXEyYWdMSnd4anVkczBiR29rbnZ5NU9SQ3hlR25raWFJU1d6M0hrOVJMdFB5dXFOL0dsT3dodXhSZkJnRXZYKzUzM3o3ZGZndU0wd2lKdXYrZzk3V2dmOFRncDdOSzZNbFdxcGo4eVdIam0xL0xDM1JYQi9wTTZrbkN1RGVMUjJpQzhCV3grVzVYOHpYaHJIUjYzcHhBWDhOZEx4dkx2K2hUbHYvZG5WM3JjTGhJSW02SytDM3lkMEkvMW9zOVFWWUhlWnFnQUJ0bkNXRlBSd1hKQWZUUVlremZlczNjS1d4cE9ReVd4L2prY2pPcDdOWDJ3RTl5cldDaUdDaWVoTm5OK0FwaWFBb1lFdzd2S3BpZTQ0TnVNZnVHVXJhMkc5R0piV1dYWXJEMmtKaWRhU3kzbFhnOVFnWGhpWW01bGFXZzZWZDQ3b2tPZktLMlBOU1ZodzkwNGg2bDQ3Q29JbkM2RUpvelhXaE5sazhaclNkMFk4M2dnbi9KZ2ltVVdLd2RRL2pZOFR5Y2tQL3E5c3pJTGlhY3JIcEtRTEhuWndpUU9QZ3YwOUc3M3JwVFMwL0J3SkVHaVJ2RWhUaEtSWmpINlRCTXFldzU1cTV0Vys5eHB4N21uckNYc3FJejJiT1BXZ2xoK2dZczhBeUZPeStBMkJZVzhoVkdyVVdLK2VYVDN2SHJycSt4TTFRbDhzQmtHTVcwWUhoOUgzRHVoUVZubDJvcFFjM1kxcWdDR1NEVDliVzA3WWFGVGZGbmhEZi9jamE4c0R1TkRZeEFoNC9VL3ZjL2lBLzd6enVneHRRK2hRMHkwczV4Z25DUEFkQXlGdlNIVTBlYUNqY1YzNUU0OElZTXJ4VEdpS0I1YTdxSWNLN3RPT1dmbGFIMlYwOThBaHJwbWl1L1NZSjM3cGZBNyt0bVg1RkVnZ05yMHQ3MSt4NXZhaU9KWVpPRzBEUEpBMWhQVDZvdUtxNERDOFRMTzlQWGFEbXNhMzF4bmRVb1FleVFteGdvWjY0MEVoMjBNVjEyVzJnd0RjRlU5RGhweEFjRmhRR0FDMjhTWHMzNk9rVTNKODFkZlgveUtRR3NxTW5kMGQ1dG1IeHR0SmVKUXNjNkhBRGZLYXhVTDlvUStqSVJzSUcwSVAreXBZYXphVndWbS80RzlKOUZDaDZtNGFHSVY0SmlXMTh1OGlBWGU2ZTlFNHQzVlFreVpCeC8xZWUyRC8xcngyYXk4YVhFcmNIZ2s3d2Rxei9oUi8xODNqNVpjVi9DK1ZYblB0TXlscTNaZ0FwcE9USEVIWTB4V0dFUFRqZkhSK210bTB0VitZVlo5OS9LZ2lqVEpaNzE4eW1RNjVnam5KN0RlTldndXJvdDkvWGR0eFRzaktJTVR1bVJ4R1o1UXgrVkNxMmR4bWFaM2ZYWGtHVnV6dUJzQU1lbXdua2FneVJWNWxFeXFKTkFzRWpua0s3US9DTkxPQkZ1cFR2SERic1BkcjI4NUF4MUFFSHdzaC9DQ1Y0cG5DU1VkektoUjZwRkZKOVpUNnJnRDNmZENneE9hVzNzeWN2Y3E3MWJoVTBPLzdFOXBreUo3a2tKYUYvN29BRVRrdTgxTmV3L3J1amxaMFM3VllybGZHYzQ5K0d6eG5sR0NleHQrc2xRZ3Qwc3hhc29IM21wdkFuNnR1VEh0K1R5VG1tZzU3UmlCd2s0T1VRTGVQT0pKUjlsMkFNV3B3NlkzUVBnT3RjTFArNWtpLzBsWkdEbnEyeVpaalVzOFVJUWI1UTFYRGtNdnRZZ0ZwUTY2V1dyck1PTzgvUW9GT3VwMzVQRk02UjlRS3VDSWtKcTNwS01FTDVtTnV5aUx1QWR6ZGZ3SXN3NWZaUEdNL3Q2T2ZOOXRLN2t5cHBDSDk4b1dZUnZqamdrbEIrOUxrQ21ERmxFS3daNFFXQnl0UlowWTRIR1VOVU9XUFhWV3lZbWwrU2pBaXpldTh1eXZSNHJWQWg5c0ZZYmg3anNabmNOakhkUmdZRVdSanVHUlJqZ3dPTDlXSlFEQlRpaGxoaEthdWRxNzVXS2d6Y1lORGpDcXJ1RElZOG51TUI1QXkwY2NRWnpZZDY2MWtkSjZ4SzMwaFc3eWFWdHlZMFJEZ2lSUUNldXh2QjBaQlg3NDZyRDdIOVd0TE9rUUgrbmVmUkdlaDhwQ0lra0NtYlVNSlkwUHRzTGhUVDR1K2Z4Yk9nVmZleWpJZUhpR1JEdCtXaVJTTDhnSExaME1GdFRlYzA2djA1U3JKMCtWVU5zUndyazBKUmRIbDExQVBNZmVRYytLODVCcUEwN1B6MHNuSE03aHlzaU5IS2ZWM2E2WGcxRDRCR2RvQ0JRVHZOK2R0YUtFbFAzNGtwbEF3K09NZlB1WnRLZ1I4ZnhaZTJ2eURXU1dZWTkzdnFIenhGYUJMSzFodU9oQmVoaitraThpQ2RXVER2dDRodHZ1emt0cENYZWJpMEFCQWRqTlhWeWRrbTNPK1JUMkVDRlZ3b3IzdEZHUlp2bE4yQW5yU3p1MmJiOStJTWhLOXNEQTd2SDlIaDJRUVhsRjVrMVIwUStRVmRqalQ3cUo1Q1cxQ3h5YnVmRU16cDAyb2wrSFNteld0b3RZWlVxRUZpdWh3YW1ESnZNOGtQeHlUM2Y0OVgySXhnbk9KR0k5TW5GZ2poajZkK2czd3B1VzdHd0JiZmdja0I3Vnd6aUFtR1FSMkNnVGpxaGxLN05pc3YxMEd5OFFKamZEeWc3T2FkbVdGd2x2M0V3cDJlQVVkYmprMlZaWGxVcVJnNXhvWUhWbmtuc2d1YmJNcGx3MFIycFpCUS9lYjh2VmgzUGdhYlg3WnpPNGEyQjI1ZlNnTGJBcVVxQWgwQmhnZzBXc0N6a3F6VDBYSXpxWUZiZ2tlbU0xTmZKN2dJVDRKWVhsTkk1WS9VRzhsWmlqTDVOWFpMZXFEOThmUnIrK29aTkw2eU9IeE5WbzluR25ZdjRpL05odlBwZWtmalZXYTB3cDMyc3VQM0k2RFhic3ZFU1JDbC93dEVXSVFZTnd5bDlFUXR5c1E4OXZCdVhJS1gvTlRaeHNhbXVoWHNhZkMxV0NyTUR4ZjNDVjZySE9mNHBxZVA0Z3B6L0xRb0JzUHAxY1VEWWhoRlMvVy9DSlNIVXRtVHlGN2wwYnp1VXlFZjJUTFc4ajNoNXM4dVpndGNzaUR5dDE2eVFSbFpIZU9sYmVhaGh4eXp4RTN5cDlsQitEUDN0UlRydHMzam1kMUduSXdNM2xsdVBQbnNJYnlPZjBYbnhuTHk3KythR25KUzhRYnFtMDRYd3h5WEtHYzI0cUhZUE4zQjdwdDVtTnhPVStxKzM5aGtqT0JXcWFjdHpWbmEzckd0cU5sM0FnWnEwTE9rMW9RSjA4NkRVa2crZllheGYwSWpyUkJEd2pGcWZLRHlYRit2QytXUSsyK3ZLVG1HMjlXOFFHWktvd3NaUFRkQlhQYkdMWlRab0F0Q1M4elp4ejZNNnBBRXFpdUdFcURCTFlNZmtiTG1NaE9uanA0VEExeXpvMFBRZXBOT2p1UU1ONEdJbnkvSUxmVnZUVm5JWFRiZ29ncUhLSkVkdU4yQ2RUL0JWQTY0c29CbTk5MHp1NmlGbXFyUkZZQ1lXVXlBYWZZT2tuS0NadEpvcWRIMG5xdjJLamcxd1pzRmRJR1NWb2JEMDg1NzUwamJvYWdML0YxNGJMQk5ZY2JNenJTQ25LaXM4aWdMZTZQQm91S2FMazVtYzY5b1U3dE9VT29mZUJBbGUzMGhPRXRoa2dxU2JuSnNqempuUkk5RDBQNWNsTnhMaTVFanU1OUNTMGJ0TURTMkZHbERKYkQ2c1MrV1JkUTVEZzZQaVkxNWtVOFI3NURMdEhVOFp4bUNVUU1aQ0YyUXQ1eTMyZmNTNDlBanlXM3UvS2RkMUFLNGx5NWdaU3Jyb0d0bitFSjE3SXJSWElvUGFvbUpDOHh1RStrRDhxVVFZTGNYR0Z6NXB4a2l5QWd3dUJ2THpjWkdtaE92ZnF1cFQrOWN2T2Y1Y3pTZGgwZXhoa2dwRUZOU0VUTzZyaHBqVGRYWGN5TmlpY2FmTU10SzRJeHlITnJYSDhxNkNlTjdwYThpaFdJd2xCaWRMZ0NpalBITWQ1d3lYRlByc2wwYkNSNmJFeENmdVFwRUVlYnpocW1Pc0U5VHlqRzdTM1VhM1VjRUl2WC8rdmF2UUxkeUhHZW1VUTVEL1RMRW1EeG5PMStITjBjenlIeW40bnhVVG9FNDRlMGxSQU5ER3dBTTVMVU5VRm1jcU4rMVhUV2s4MGQ4UFdiT2htdXo5OE5QR3g1NzNsTkllNnBaSDVZOStRT2lLWUpRUVJmRHZCOXI1ekNTV3hzSHJ3eU9penhBNElQVzVOSG1GSFRTUzhjNS9hUUpDZmcwRkg0Q0NpWHY3OVU5aFNvU2pSYXBpaHFXTGtIbnRTUTc5U1AwVG9PRnNJS3JqQjRWMHFQVW41cTBDQmJ2THRpSTJYSHUvOHEzZ1V6aSs1TXAwbTh5UU1xMGhwNGZDRnBhZG9lcGk5NmNTcmJ2cTdNenhYQkpuTlQrRDgrM1U4ejZuai9CVFpjNEV5Tk5ySmxPTWY2L2Uvb01BVDYrbUdvUktqekFKWG9KK3BuSDlONmxFQkhtQUFLR0dBRnc0dFFQV090Vnc5Rjh2bHNrc0t0Mk1CQ01qeDFNUmUzRjFtTk90TnpXNS8zeFZEUHUzVlhkVktSTjgvMm53dmRTcXlhRkJod2Yrb3B4cWpPYm03ZWl6b0NHK292cHV3Ri80QnEvb21DQzNIWXFYZjJQS3FIV2RYN1F0QjQ1eXdOaWFWd2xsdlJIci9kczREWkFkdzl2NFA0ZGpxdXphRUIwT0EvdEJ6M1hiOUNuRUd1eFFWYm5KQzlEakp6R3FWK3NEUEJ3M0lXc3FtVW9ZeUNBcEd2N2d3UEVpNzFra0JmeDY3YlBDdG1FcVBZajlhUzZXUWw4QXIrc2tQMTc4Z2JSazdZSEwycThJVElyRjJ0TVhZVTFWS000UXpocjlCQmVIMlJhUzhOQkJEckdXSXZyWmxST2d5bWRqY1Y5VzRvV0lYdUFvT2ZDUG1lTGxGcTd3ZDZwd1ExZEEvMEowUm9HbjJmamNGRE5xN2ZkQ3NZMnUwSmN2eVVNYXpqc3FwUTN2YTkwK0JTMVJDN3dFbWl4dWJyYU1Rc0xxRHBiUVE2RFlDUmNSMXpqalRCbGwwTkpZZUN2WkVrajNadzE4TjZNYVB6ZCthTEpEZWFJa2p0K0tYblNrQWJkM2oya21qVlRPMHFYNE1xYWZERVFjRHJ2WU9VZVFWL0h1VDFiVm14NHF4UDhmSjNXdGE0MDFRYXcvQmFRV1BTTG5ib3laQ1VvV3dqQ1NlSkpaWDMzMnRPQWd5Y3ovU1ViY1lEaGFnQkhCSnk1VmduSTdZN1BDczB3cTV4V2hHNjN6OVVTTnJ4V1p0RTdNYzk0aUM1VmJ4QjRoVTBoS2xxVUJLZjdWd2JVZzNWQzN0aVhaQ2R6Y0lPcFljLytxWWFTdXVqbzBwNmR3QjNFQjl2RkFVYlQrWFhFQUplRXpPVUlnd3A3RWV2Ky9EYkZ6bWg3NzIyd0RsNWRKYmRpYXBtK0lUMGJNTndWbGMzL2RXcnZaRFgzS0o0TjNmNTFKc2E3SXQ4a25FbDZYRjZ1Wk5KUUgyajJ2R094TUo4cDFjWVdjQmt6Q25OcUdjM21OQ2hENUQ1R0RBL0ExL0xwSDY3MjdyS3Ywd3NQRmhhTW1BSHRYaXBJbmJDUHBWT21TODM5dVQwSkdBd3JJZ29vR3l5eE1iSVFBVmd4aW9QT2dNTGNoTXRwZy82SzM0SGo4d0ZxcW1Kek4xUlJnRkt3RmdPTW1UaE53N3RYRWJ2ZkRES0Q0S2QzRGJvYlBOK3BKQTFJc1A2MTliVlpHd1EvUCtLTmxhYTR0ZmhIUHdrL2tuUGVwdkdLRkFnWEcyalgvVTBrNm9zdnpjQkhpNG5qSGIzUXBtZ1NseTlud1MwSW9UTkNnSUtDdzkrQTh1eGNRL1ZJL3diZEVnN2xqZG4rWmlzODJrYUhIVVBsT2RTY1ZubnJ1VWhyWkZQK1QySGJyVWlBeEFTQTdRenRGRGNVT2g1NFpkNHhSK0dIMUwxWnprczZuakhCbDJXZFdabUpKb2pia3N1YkdDRDFQVThQU0dBTU54UjA5NTM0eXhKRU9ubllLMnB5czI3M0VSbVdnM3cvalJ6WER1NzBKY045YThqbDBmTGtGdnNVNjc0b3Y5TDJzaVVBd3krZmpUY3pMZzB5d1dXL2xjUmVNM2NIaVZTNXk2WVJCZ2NacEd1UjJNOWZVbW1mUXBGZUt0d2EyMnZNc1F2Vk9TZ2xYcXpGWDRuQUFPYXlIQkgxb2RnN1pXM0xxK0xJaWZMNzVDS3hBL2NsOGxYRkd6WElSZ0d0VDQyZ3lDR00rMlJWcGdFYmxUYjVlWG1BUWwycUg4d3N1VWZHeXFLaGEvMDZTcnVqdys1M3JKS1N0ekFFajlnUlFYVHlXWmdjWGNMVkdjMVpzcU5pRC9RZTBmeUx2SWtFc1pKMHNOWWdWM2hac3hFOTUxOFZtVXRtY0FDQUVGQzI5Tk5mSGNzT3lWWFQ1RXVTQkx1QldZUndtd1BLdE9Xc0JJTTh2U2F2Sm04WjM5QzZoM0ZUbi9DVHR4bE8rd0xGUldKR1Zpem1aTGdGbnJZaHRWTHBPWlNCbE43YkdaK1VWVmFhM3VRZTUxVTcveFhFYThIbVlVR3pUZVgyRW84dHFzWDVra051UkV5ZC9weWJ3WFg4Sm55UGtVeHAraCsvUFJlMlVRV2I1dWRsbG40QWxlanZFS1pDRmdnc3lsRUdhZWNzTUp5ZEhiQWlaOUp4eVVVVUI3RUxJNTVQdGlIYVRqc2dLNnN6OGZzSVpQb2MwVlNURTJLRjJMdlRLdGRNOFNsZGtzTERPdUJWdlJqeXJCL01SMU5INWpQMk1hcGcvMXE0b21zbStzZ2RyUXJ1OWR6cHFxODI1Z3NYamx0akpWd2M5K2RBRTJsWkgvbkprUVRtcTNZcUY5ZUtuQy85UExXaDVrcXZycHBvSEs4T205R2d1dlhhNzl6YkhQU0d6U1Boc0lzOVl0eDNzcjhvU29UQ1BpRmxDY0FHY3M1WGFkWXVVRU50RkR0N2xBU21QUnlKRGZhbmNDR1U1OGwxN1pHMlQ3NVN6WkpOSDVXbDQ2Q0NKREx5QnR3TS84V3B2MkxNeVZ6SmJ5bmFvSElUSmlhUGFLanJwbktESWlkcDBlVGlub0xHcFl6MTlqYXFrVFgwQkxzTXdqS3pORCtkOHl5T0U4VzIrSVZOL2hSa1c5eW5QUWpmTStxd3NhMnpldkk3VS9icCtOZElZeDhrOVVwWGNEVk8zWGNCSG5RQkRSNG5vamphMmRLVTBkaERNZUFVd3ZjUHhRRjZRTzNLSG5vVjE1NlRiOHVHb3d3aVN3TkIrOE5FRkZUc3NkWmh6QlNBbkpONzRLRzltNkhUUVIyYUJZUGx2VThMOTBrZisrWkJvUFcwY25Kdk1kNzVJU0VoSjRRZWd3Q1QrWERRTllvQ0RvTkZudkFPQmlmLzlFOE4xY09yM1hDb0xoMjZJby91Q0JNM1ZXR056VkE2WmJzV1lYdzh0Y2FZeVRaR1VQVm01SGNvVWxwRkdPd01oc2JBUkRrWm1MK0JCRlV3ekFZcDV2eTFBS0hoYlI5NEVabDFqVnBtOEZ2bTNzdFZjRmpMK1ZGQlpkT2l2eHU4R3c5WXJaVEtoTWpKb1BlTWwyeDFYU3RscGhOeCtsRzV2N1RyL0F5MDVCNU96V2hESFNaK29QQ1lxWiswc0hmZXJYays5Q1pnYnJ5NTNkbVRNaERPd1ZXQk9CcU5jT1BuRDYvMTBRaklRUFRYOFFoYWhPcUlwRzI4QkVlbHdhYWtQNGhUSVhtOGlWYS9tb3IwV0dOV2thOW5qKzYzYWNRL1JGalQzUDNjQlFyUEl1VjBoRFBQQ1VMOFBHMG1CTkNRTWZrRVlHRnFhOHJWZUFuUk0xbndWN253UmFSZldRTDQ1U0R4OFdjMzQ5QXh3SWN3L1IvRFpHL2x6TDFTT1hzWllKK1FId2tLRTNxSmE5emJ5UFBWR0dzdUtFcFJwQnFqd3l3M0JqRnY5ZjFhWWdWT1RVdVBRUXRheTBUWmg4ZUN1ME1nQ2tBL1Q4MGNKMXQxNkgzUXZ0cjRjYUZxZ1l5VHdsQXgrMkY5cFhmRXFXays2ZFdVeWxkOUVuanJjOFphQjZrN01vL0J5ZUdJR2FJMXNvdGhoOEhNdUFkRTJpSTI4WFF0bnhna0U5UWp4WmpobnBjS3g0MmkzTTlvNUFOQTdPOGpYdmpXT3VvV2ZCN0d2VHM1bjE5VzgwOU1HOGsvb3RhYURLdkk5QkJybE1DNENQTXFhVEtYTXA3aVZNd3VMZFE4eU9wNTlDRi9lMVZTeGFqSlBOVDZXdzNSYkNiMUNQWFRLOXZ2OFpMcXI5TzBlUCttV0RqRWZnL1BGdUdQd25pNGhlZE11SStyS1NLVGNTa3VjTTR4dGx4eGYyYWQ5aVlxWEtBVzhsMTB4YW9Wd0FtZyttbDdOTGVXdmpRVFNuN0hDeEdmQlRzWHNKZHhRRTJaMzVtNEh0NHBpLy9VKzdSaXJqeEhocG1zbHU0aVpBK0piaW9TR0ZNSWRSR3UxQ2xTSGRYV3cwWTFDN1lmekxLdis1ejNwN29qQ3pGaUM3UFJnUWtIa1ZaSkg3WGJQVTloTldqNmh6S2FZWFpnL3hxQUhob2QvcE5XSS9ibmxZdWVmWGhmY1lWckdUaUx2REVUZGZZazR3MzBsOWl6RzBZSjcwUENWOUZKY0g2WENKMU9teTNFUm1XdHdUVTBXMXlOVmJ0bk0rRVhnSXVMc0NqSU9Ha2pwUHpXeU1ML2ZxTzB0VllqaDJtNDdjc2ZOcVhJUHJpaERhK3AyTVUyZ1E3UVBGRlhMSXdvZFh2QWR1NHBpV1VFVnlUTHh4RVRCL0YzcURWWno4V1V5cUJDR1NBNWRjQUFhVlJIVkkrMVEzMXFIRVU5cC9xc2V4cTNsTUNGZytEUTM2WE0zSlppUWw5RE9IU3VkczJBazRwY0lDSXplTmVHcW82dGFDb2h1cXJ0d1dIajZzU25Id1IrUDBGYVk2NnhLMk9VM1BxbUxoUUZiTG04YWFpOHdNaW1jYXpGTGpxTWlXa2N0VEpHS1VIOGY4bmpaVTdVaVVnU3BSOTh6c3B5dnpDc1EyeG96cjRPNGs0bS9Ya0kzQmEwb21hQjFCSzdVbi84TXJZQm9Cd1RpSWtmTEs4N2pPTHp6MUltZytvMW1UYmtxQi92emVLeTJoaTZrZU1nZlh4eE9ka1o5MjBUTnlhY3JaSk9HNzZvUkFOL3VjVnlST3FUL0Fsam96NXY0ZWtROEF2SGt1MWlHM3luY2tkbW1QR0xZSXJDaHVBdEdBdUxGdXJvZ2NLc2RMNUxlV0lZYmVJWjJBVHM1TENndEdmdFJYMFlNZEE0TmdFVGY0RnlNanN4SWlWRnE0QlZyMXFuVzU3aTlwUDBiK2p1QnlkZjJQZGIyR3hHcG1DRWgzZ0RrUTVQUXpMMGFJdjNMTEx3T1Y0RjJmTDVBdzRRTFkwMFFBMzVQLzVPUnlkcU82bzRhV3JXck1pMDhBZExRWE5GOEtMeGFYSEQrb0FMWXdUUFpUY0dSWnBoay96SnZ6eVZ5OGxzWmtXU1RVeCtOU2kvRERMRDZtTE9wYkkybmNjN2Z6T2M3cjZCcGFsUnBEblFkYU9zR0VCSm9uVlpYenBrR2Y4QjNYb1JidW5iNlhsUmRRc3NnV1ZWWmh2bVJsdyttNmV6YlBIWEdGSHY4MTUvdXlIS2tqM0FLelp6Y3BjUHExWnE4MmcvbzVIR3ZLczJ5Y1ZEemtBSHd3ZG5XZ04xSUJGMTI1bGFUT3hLVnhvUHdzNHozUUJUNU5xYWxkRnZ5Wi9UZEtoY1BmeUd3ZjRJeVIyZDl6N1hmbUtQbjluZ0pZN2NON20za2RWcFJMT3BlZW9saG9xd3BpYTZRWUtiWXJqV2ZWbmlLWEt4MnMvYUpWQzQyYmJXRGM5aGs2Q3dwUUljMGxUbWJKK09RVkdKTUg4a2tHcVN2Y3ZIZVcvQnh2OGh3WGJ5V1ZWRnhNNlpJWDBhRlNERk9jVXplWW54NXRlWFVSOGRHVWI0ZGdYLzBSSDlKWUcycmtsYkFoOExVcjQyZnd1NDhIU1NwSVIzRDFGNDVMak41dGlyeEpUdHcyNGFYSEhWaXMxY2c0NTB0Qm9URWFBdmJ3NDRCMUhBSC9WdXoyYVRzT2hweTJkeHM2eFpaR29wd0ZvYitacFFva0xYcml0NndnNWpiSVB3Z1M3NklCQWF1TWszRFJFcWUzdDkzajAvR0RkdHFHWU1TcGZFL2VwR0thMDdsZWFQcmh5UFVHVTkwNTdacW1nbktrcEkrcEdPYkpSRXorTms0YlJieHo5L3lqbnY0aXdYQk51cExwNW8xVG80bDVUTzI4WEZ5aDNMY3pyUTBkbUZMVHVZNGRhVHIwWmUyMWFpOXhZSlUraG1WQi9qWWdTUTVFWmxqUHB6UDBsN1ArOVBHdU0yZy9HZ2lROEttU0YwWURnYjFFY3cxOFFDdldGRDhoNUU5RVdSVUFhSkpISC9rd0h3SkVSQm1HMDM3R0FCZXFOYk0vdktBNVZRQTNObzdIM05uaDYvK0ZjcU5WVG1GYU1KcThHNXhLd2kvNTVKa2x4K0NUaEp2MXpLODZXeWhlYWY3V295UzF4dlBEQzJuRWFzK0JXSDJlWVMzWUpJNFFqZVRZZ29wUnVJUFlhWW51SHNzaWpvZ1NTREtOeTdQbFBFQ2NLcWtUYzNuZStSK1ltb2IzUXlHNGhkZ29XTlhMU0lSbUh1MWJSK3NiM3REL2lRSlJvV3QvUm8wS09hNXlheWJNcUV1ZmVzOTdpMjlpT2trVEVtRnFSRFFmMnltKzNtd2pyeklIQUFDa2oyMlJmQmxjWVgyL0UrMlZkY0lJdG1HcVowQy9heVVNcHl1NlQvOE9DM09IZU5aczBLMEpSc09vNWVjd2dTVWdXK2tVR0s4QW1VeHpZVlhoMVNmZlkvOXdHVTlTNWZKMEZUUG1IcjhZRnlqUisrS05ERmtYNTlhWTFrZnN2RTVWbk1KSDVValJCUXZNRGNraVhlbndsbCtwb0thdE1XUTlMZE9oVHI4bXdrbUJXUDFMcnNNTGNjSkRxbFhud2pucDFycy9IOEpTSk9QRFh3eGIzSXJiQktOSmRIQXN4WWpkaW9TY1dTUGRHbkRjdnI1Mk5jeTNQVzJHcUdSQ21FbFREU1YvRHl1RkJQRXdwRk1hNElUUGhyeU1FSmphT1dSd3AxYXBPcE1pVCtMMm9vUTdNRUlMODBuV29wOWFBUGxiTytlYzVWTXdoZVQ1UHRRaHljRlpvTm53Zm51eWF0MTIzYWhBcnJoeTRjcExBaHBMcDlVOTZteUtSaFQyMFc2WDdpaENvaFg1S0lNZ2cxVFdVQUszR29zSk4rekRjQk5mUWNoV3J0YWdRRUNIcU45cTk4UTFNZVBwNmRYaklXODFsSDhobTVyMWVpcEFjQVdRUWxmNVlrN1Uxck0xclVkVTBZMDJ5aFNqeXUzZ0xzY3d4Z0xIZ3RpWUloTzIrYzRRWUtjTm41aXZjWHhTM21vQzZjZkNsa0lEWlQvclNUakxSNUtMY2VVWC9qTHRFZWVUWkpHOEJvNUNsbzFDa29HcHVYdGZTY0RsNXJSQjA0bHpSTDA3OXNFeWU0Y2h5MUI0dnRyRzdUS1hqRDkwMHd3RXo4dlV4ZytpS3dEUzhsVnhUQTJCY0JqQUpSZzU1UXUyNnAyM0VRQTZoUHJhaFYrV1ZRUFdnM0JYU1Zacms4U2p0dEdSeHNIZVhVUnFFajFaL3A4anZFN284Wm1kaUIveUluNDg0OHpTcVBmRGx2cEh6cFl2d0NEWmpxOEZkTCtIRUQ0YVR6YWU4ZmtUcENkdDVhZFlyYkV6NTNuNVZ4SmxHcHpqa0h1ZjREenF3QXF3RXlqcUN3VHl1ZWs0ZFV5YjNCcE5uM3krMHpXRTlDTXlScU01RVFMYWlPNEs2Z1VDclFSU0lLMHZnM1IyK2dVUXVyRFNwd3ZRQUx2RHBVOEJ1Y3dHQ1N0YzBpOWxsZC82a3RRNlU1QjkwcDhoc1hyN1JpYmc1VHlZWXQ3ZmFSdmZkSlNSYU95VGxWYVo4Q0F6NHJ5dUxGeklGTHNVZkpCU3pycHcrMjYxTmtVcE9QSW80bFBNbHZYL0pLcFB0ZzFHWThDdjRteXAyUVVJVkNrVHMzZHVzZWc4TUNJZ0s5YTFJNXN1cmRHc1FuOUFuMzJJRkNsRkU5ZThMZG5tenBOamczQmVBbTNnZHcxdldsd0JtU2dmbFVxMnZ5NFQzN0pISWdnTzJKQjlJL2M3TzI3bFZidXQ1NFprKzI4Z2ZzbnZVaGtESGZ5a2RXczh4VGwwK2NXMlRPb3RtZGlEakNNankvQlZiVmQzU1paYlFhUlFKTVM5OGxxZk9KeFpDREw3NUZCdlhjc200SkExNFZ2Qkx1QjNpRU9aNjc0UjVDQmY0dkFpQklnbHNTTzMvWEtRbjAxbzJGd1pOOElpNk5pdjZqSTl5c3RWL3YxVzBUV295NGZaLzVuTE9IWGN4NG1lQnF0Y0NDTmQ4Tk1LbHR5b0hqTW12V3A5c2N4RXV1U3B4NW1LNTFyQVVlR3FkQmQ1UWxRZUs1S2hRdndDR1VMbHk4V2p5V0VlWEZycGYwUEdmWWdTeEp5ZnV3aGlPK0NxNkVkUWwzZ2l2dEpkTytrZThWeEhIaHgzcmZqblZQWGRkYmdRV1VQNXpGMGJUbnE4YjY5amdleXNnTmJKTm1UYXllNmlWbWdrVnM4YkxJbXhUelBKUkh3RytzdFNOMGhHNVNIcTlBWnBQUGo1b3U2Q01reXZQcXlxODlDRXJqQnhwdEc5ZWNyd1RXb3RxZTJEZ1JhSTFEaXhNMHBZVjgxaERnSVVsendhVFVjaTFWQ0lMM3hQYkdnWklmZDl2Ry9NMlhBekVRd0xKVXRpSm9DUkg5T0VsWm03MUhaY3pkYW5lQVVqaFNWV0t4UXFkZThqYk9WaE0yaVY5WWdjR3FSelJWRGVPL3RoNFA0Z0VxbElsMWJBN0dyY2FLeWliSlp6d0lLb0U4S3BtTjBPT2Y5Q3NNd3JSRFhMOHFZR3U3bGxtRlZLZ2NJRW5nMHZpL0FxcU9zZHhLanVZUG9McUF0ZEJNQm1GSlZ4WGtUTFVnN2tzY0RyNXhXYm9ubHZmVmk3eFBTMGFqYmlHT1lzb2VrSSttNm5oT3h3L2JwTGVMTlFOSWhkNy9HandRRjM3akJzSzd2eDFhdkVpWlZnZHlwcUozZEdjN25kYVZqZVZTQ3JYQ2pjNEhpa0JWc0RvKzNoS3Y5RkROV1Q2YUllS0kwSThSaEJoSWNtNS9XSFV6VjlndEpmcm5jamR4RlBCTkpRQzcvUlY0OXlHOTVST0h5LzYwcWZPMjNHTVJrQS9rSkRyQ2NEMy9VM3dkUjQwMWkxUVhUSkVUVlFHNFFUbE9TMXhpZlpRNExWKzRYRVRJWkpIOEs5K0Rta09aRHViU1VZbUdVc3VnanltcnYxRUJLandSbkw1dUdWRUdOMkhHMzFnc3AxYlRjaE1JZ3RBdHFBWjJMSDJjOUZOemFYRTJHbTg5WFMyMUpsTlg5ckp6N3AwenZoaG1abGsvblhhZHFtd1AzcmpEaEdyTzVmWkcyazQ0c00wYjJkLytSS2psRDdvaEYyd2hwMFEyclcwdVNCL1hNMnNnT1pQYkpvaTFldlpjRjBLZlRUbWJqcStWaXFMMWxHT0x0d3RpS0RvcTB4Wm9FN0t4RHFwM1ZuZ3F2OW5YVmVmKzVBd2lCUHlVeHRwRGltbTFYc3VHY2l5T1BtZGhyM3MxV0VxalAwNnRUOEs1eFp0dTJHc3FiM2ZVN3R3UTJ6LzFwZloxSm9kSnVNNWpZKzZBbTBhTWpTZ3J4LzlNOHlVMnpRL0wwOGhOSlVNNHZEZUFhZFJHaXZUL1pSMnhUL2EyYTlJL3VNaGh3bStuZVNLMUpMV2NVR0EyclQ2WU9BR0F3TjN1R1p6TFJRbnVNTHM2UXlldTVWa1FMQW45aDd5Q3J5bzRYc09oMithSlEwTjJpOHBNcW9Edm9wU2RsZFZrWDl1M0QwSWxka2Npc2hFbDNCZTlQTjdDQ2p3NWJVcklndzNkbS9uaXl6dU9MSllRSjliaDdlWit4bUUwMUZSVVNVLzNDOUsvMlFpTWRaWGpwR1JBZkh2dFdJS2JHY2dQamRDQXVPT1R5Nk1RalF4UklvNS9BKzY1Y3ZIT2FyQ0loOWUxd2VuZ2VwamlZWDNBUUpmVHNRNmhVdlU0MHlIY2ZRMWdoTUxCNStPQWljODZ0b3g1dEJ1aWlLRmc0K0E4c3dMM2MySGo2bDN0MGRLLzFOQy9lUnZ4NXJGYkY4MHZUSk9oNy9CaUxpMUlZeHlNb0UzNHRnNXlnVDhZV25yZjFIL1VRaTRQZW91M0IxWElFN2p5WUQ4dndNaEE2Yi9IQTUxbjhyNHNYVzFrWENnYzFPVndaK3FGcm8vTlM1elJZaXI5UG5EN3NTdkcxdnZwTjF1YXFVdjY1NEhSRjZ0YitpL1hQeWxNZGZzMnBwdWliMllBQmI3dkg3MzVDRjNJaUppVU9Db3N4a0R6Q2JoRDhXYjBJT3MrdkVzcEZlZGg2d0IzYldhcDNDeXl5OXRodURMSWFwODRINkNlVEQ4d1o0SGhzZkdxVFVTdk4rdVd4MTZTRWZYNEJhV05FZDIwWERRM1lxMmkwL0ZIZEJMT2NzUGF6azVRUklpTUkrQ0ZFY1FTQitVYStiaGVnbUNrWlM1OE9TT0daeGZuOGFFWG9lQ0kzajRnbGQrNWlXTlpJYitXTGJlSUVpOXFtUU0rVUZWZHFlem4zbUNEajJBUWNQMXFvVUhNYldJYVhMUVc3a2VLYlZqaWtEZ0JuczhoR3Fidy9paVZYZ3p0VnJXR3dwRHVRMUhnNnp6bi9FbzIvRVlSZ21HeDJ0NUxtc1d3QzhaUDZmUXdacXNSK2FzT2JMQXp2OWYrS2lLREJ5NmlRakFQN1JmdXlhUUd3WENRSnpkV0MvanZiTkI0TGxjNW13WEp5OVlaRUtxeUZyN0hxbVk3c21xb3NWeW9jN2ViZEh6Tmtjc1Z3SUxFVUxzOTQxMlJoUkFWSVA5MEpBU3hVcFh4L05pYjFiSENyWVZsQWxWVVhaeGdwenVZLzNhWm1XYU5DcFM5bGFQcUIwdmRyOWhVRk5ob24zbVZGTGdtVU9MT3N5aGdkd0dGQks0NzhkSlErbHdNbFFJMVUxaWplcXZMNEV4NGNGSWNuR1Y3eTJGZ0pMVUxjeEFjZ1l4U2srdXArUjkyc2hkOXhWSzNWWVNobDNxdGtXaHRjNTcrbUNQZzEycTlQWnRFOWYxS28yUFcrSGJaSWhDYWZkTXg0UzFleGlPd0pXSnV6RmhNdCtrcHFzci9rSUNMelVkUUZ3TzE4YVB4UnZXRmV1REs5WkM4b25ZeDlBdzRGR3IxaSt0REdBdzUxeWVBMjZ4b2lPVHkrb1RLVkFKenR3ZW50WTZIZmN5d3o4RUtKM3ZOTm56anltOHd4OTVTZFRIOWtMYjdiWUkwTm45bkUwdzFFaDRINzIvOEp3UllnakhBVFpvK2Q5WUpFcTliMVk2bG42Z2ZPamlDekQ1KzcyZDlDRHA2dFF4Mk9oMmhCU0FRUEgxcjVsSmhKUXRhTVc0bFFVcTZZNnI3M3RXSU9lV0trQUFOenpURWRBWWp0MW5rQ2RqQzRaRjZEQUpMaUJUNjFBZGdqdWZPYi96dUlUUUYvSE45SUlra2Y5RUdWb1BvN1BPdzduOWk1N3JLeWpRWnVXZi9VZitIVVNuaW1IR2kxY2d5Y1V3QUVOOUwweWo3SHlOUHpHTHNTMjl5WkE3a3hwamFtb09NN2lYQnpyd1l6VDFKT2NJejIvOVVkbENpZ3ZpM29hYmxsZVlCcFFCZmN2UWpreTl6c3R0WTY2cE1FZkp3cjI1YnozS0NSTjJ0elZzVE82eVVJNlR4MjkxZ0dZMWlEM1h5UnhJSHdYSW5rU2NhdXBRNUVYdDZURHRWa29hcTJmOUVJMmQrUUkraUMrWmd4OGkwb0pJZmFHUHVMRjMrZnV3cTR0WnJVamFlekJGUFMvbHd1MHhIK09sS3QrdmlGeHdCTVphYUdhNkk2NkkyaW9ZRkhWVzdFZzVCdzFQNGJaanlDMyswaTVzNjFtVENBMmxUWlorMEJ1MVVaSHF4WVlWVnFiQ2FyRjgxaFk5MlU0VEpyRCtHTDBTT3pQbHVUdXlyVkpmMVFQaGF6VWFWOUxjbUxXNUgrbmRMcWNaQm1Bd1A2bXdRb3F2SVNZdU9EbUM1b3IwbDl2ckZ6U0JmS2VsL0tiRXdtNGpqL2QvLzNnbkt0S3p6eVI3Wm5GTzlhQ0kyeTdJOGg3bUlTRW9SQUppc0lTSy9Sd2w2S3RDN09xcVNRc1ZDWFdDWlN4N01nWldGSUNpTHFKaEl4dEFlNVN2N0I2Ri9zVHZpUkRzL0hDSk12QkR0K3c4QVFrNFZ3c2Z6U0FCZnFZdnNpUHUxTWIxYVprSDRKQUJ1VEd0U05IQVQ3TGNmU0ZrR3ZpaGsxSVU4dkIrRUFrVXBhdE9IeXFiS09sTFhEYW1zeWMrcjV6d1Q5WHlKRjVKcVFGRE9tTFhnNW1NVVQ1SUxYUG5iMTdERHk3RmFjanR3aTFCUXRkRVZ1K1ZXZWFpaGUzTHRnOGRMOUpPL2pmaG4rMjRQaGxvMFlxZXdtcWhITVhWNE9CTWZtbXZXWnYyRVU4SlNGbjVmVGdsSk5CM2x5Zjl0RXNONmVjQU93bHlFVkFvVDdhOEJ4RlN0MS9XMlBqUVN0MU1PVE11alZxRk0zcW8rZUcwalh2T0xqM2cvMU9GZzJ2NW5veHluRkp5RStpVXZCNUVoNmhsbGJSOTRZYkJqa2JLVFpKN0FGemswazQ0RGxtNVA1RERsRVVaYk5mdGVHYmxyWStaOVZTMFdrNlF5SmlPb2dvekcvU05lVVNFTlY1V1Byb2VtSzBqSkM2RGVBNUgvcFluMUZCdWRDamxuNkliTlJXV3p6TDBTQTN0aFdvV2crcldOWDJkd1BsZzNUK1htTzQ0bTAvbXRLUHIzcmh1a0phM3h6Nng4aEhyaUlRWEdWY1pOSHJwM3E5bS85OXE1Vkt5dlg0OFVjUW1LT3J2VWs1QjExVzdzcEUrTWE4TFM0eE1ROXJMWkRLeG1XSFFsVWtxanpaTTVRVDBxb1JoUFNoNVFrY3NsN28rSkZ4c1kvVGVWaDFLcld1Nm5SZko0QVhGZUhkZHZ2U3lRRFV6YVdFazR2VWxhNTlZcGk1UWdYSDh5ZEp3dEt0VEs5T3V4RjdnR1V0KzFGRDNBVjMzRmtyTU9NckNVeHRtaEFZbjIvWXBFVktibXNOaGFidkswSkE0NEVxa3M0VUNRTUVJdlpYNzJYQ0tVNVY2RnRvOTNlenV4QzNHMkprMG1Yalp6cytNeVFlamVRWlMzQ09pMTh2WnhpcDh1dUc5akV5SlJ0YTk3TlRyZ2dIVDdhRTB0VmFzVndrNzdFekJGZDdZZ29LcmpuNWR5UWxrVGltN1E0Y0dUSjVsMm1BcW1KQnFRSTBraDIyWGpOT2p4UmhHcUJjdzV3VkFqcllkLzNYVis1NHFmWm5XemE4STdxd2J6QUt5ektnNExlZWdoeHphMXlsanJVN3oyYTRXL0JsUkpOTVJKdnpzbHNyUXZRYUJ5VG5ySEpZK2EzZHNuUE9CdXk3NEJWSGVZa01mdGJ0dER6VTZreDlKRVpnSzdhNU5Qa0l4WFJuN2xDczhuZE5VVzk3V2NSWTdYQ0hPOVlpcDdlSE9rTXlidFBQNUNBWFpnRllPMmJla29SNENnTnprWklxaGI1cHB5VWVCTEFjYzc1YnlDWjZkUkpyZnB1K3NTejN1OUNNSnpPeVJxNjJFYXVPZ0owRUtCcC9hRml0QnRpeTl6SWRTK0FEMjhHL2RQWENadGUzTWd5WHBSOW1YRjNmTHhtb05lOWQ3QnZOczRFYzUzakZPdng5RFVEK2crVWpXemh5YW9vK2NPbEJQK3NWaGRjcDRNeGl2clBYKzZhYjNWbUZNY1d6K3FnOEY5VzVhalN3WjIrVWlSRDluT0lzQTJBUFUzSUNoVW1XMm1VVHBna1I0aUZFK01yZElmRzRucDZIV1NuZTdtdE9UbUM5VzliMFNtMDM1MzlmZFVVUS9mT3FtMHRDWWR0c1crR0JzY3JOTjg3WXY1SVlLR0tEOUcyVTY5WHNUd2FzKzFEeTArVnhNN0JxYll6MVNGbHZSeThPWUlLYklqdmxGb3U2eFdMakRNM0tBTldReDErekpGbmFyV0Q2a2hpM1RtcXNrbUl6cWpBcG9CMmpyZndXNWg2YjZIU1VkSEFUZ3o4bGV3eDArT0s5YkMwVy82SkVkempqTHMrb0NDK0tObjlBT0hDSjhtTjVpQi9QM2EwOStpNTNlMGd3ZWRmRHQwMEJaUW5qY3pHZ2UyNEVLUjVRejZHcnV3ZC9Da2ZZOE5SVlhJTXpLTk93WGNZL2JBZ2tCTDJHTDA3dGVJRnd3Y1BMejZuaGVVYXBSNFhxeEVlbEoyRFhIQWZzOFJKM2lRNFFXVTY5Q2RBWlhINmFYajdlakhyNWpKcUxOeXdDUTRvWUV4cXBzVTdtdUtraU5POG1hRnVjKy8wN2x5TXdGdDk4N0VUbXh2dXd2YjhQT3FiVFQ2a3diM3hGaTVIa0lPSXBOQUc4Q056R1h0ODFVMnJBcmQ3cmR0UUtNZTVVUUJBOXJqeVJxeFh0eGNtMWJSZnNrckFFN0cyekljczBwNDUrZVRxYzA4MWlqRHZzL1pGSVNWdm5RNkJBUFdoWFQ0ZkJhNkM0VjB1Y2RYY3YwbGlGQmxlc01WcVJRYWhFTXIrbkVBL3UzM0ZkYVc4dFF3cTRra2xEQmxhQTYrbXF2Uy9VczZ2MVZxMmRwbTQ2YXRVbUpMOG5Yei9Oc0tVYmdrR0Z0c3RDeWthbExidlI5cjd6eE8vSkNhZWZmU1QybjUxem5zQVdHVTFIdmtTcndwK1FWU05tRGlvVGE0NERDREJsQklRdThKZS9tZWliSjVsSWYyNERmSkE2RDN6S2dQbnViQ3RpK0E1RDdlbUxCTjZ4WjkyV0o5ZnoxcE1Bc2h5eGl4MlZDS0JIWkJ4bXMramtzUEdWTW94akhZenRWanAvVXpLanA2Y1hDYUZXTndBSjhON2gyR1VBd2lrak5WVERLSEk0Ym1wQWRvZ0NmNlRCWkwrWDIxaGF2aVMrSGNJa2E1S2dmZEl2QTNJdHVZSkVoM3dGMnVVUmszcmN3azMrOHJuMG51QlBZVitoOVhmMGoxSEdTcDN5NUZITGFUOGdiZHQwMUpjOTQxS2pBU0xUZUpEdjlaU2FPMUZLNDhuLzM3WVgvQVg5NnF3SDdOd2RJUjJQRisxSlE0VDRpbGJxNTcxTkN3b2pXdUVqU0wxbEZFNzVQY3luWjU0Ym9NSFNPbDJQaWNjMElPRUQvSnZGMGFBaDN4TmI3dVE3dFhrMUhJaGlFTmJBZC9nT3UzVGZ0MkJIbkFKS05BWXpwMmZwZmVMelVOWllJMVdHT01xbzlqSkFGbk55VEQ3MXJkMkI2V2dnVE11d3Y3ekxwWElwZHRRYzcrQUZDUnNiR003NFlZZGVnUW5NcjVTRjl4dzcrNGptbVY5T0xzUTc2VURoaG8yWTdra2tjMkYwdXZhT2xjZHY5MFJGTTBURytTOUdkaDBud0thOGdkb0RteGI1NldLSlkyZmZoNWdobnNFVTlIcTZhZjdZekVneTFMUmpkKzAvTWJWNElGYlJDNDRkZWNiK1RyeEY0ZmpwYzVLZGRSTDg4K2VsMDJ1UWgwSFlVRzB4Y2ZmNGgySzQ0K2gxTmhPbmNXMDhjLzh6aDFLSlhxZS82Y1I0dnBXU1FSSmJLNTk4Q2lnOW94S1VmK2pVUjh2eWVBYVQvR1FCekJEc2pxVlMwWURWbDVMQnV3NjR4YzBJZ0FMTDlpK0owWkQzVlA1RnR4aDZxZzRacm9Qa0RBWUVKTXlBYWI5alI0NnFiWi9hamhWcnZsS1VFMC9rWjBRSGZMZTlJZWFOWkNHdktFNE9veEp3bGxRY2hJNGxTZXpURGw5MVVsZFZiMlFFcVV0Z1FRV0dZK2RxelhOaVFuUmxiRjdYeVVmck5paGxmUnk2Q1ZTNXlEd1F3Z091UlNsM0hHTGN6dFFWWERLYm5RN1RRc0htU1FaUW9seVZjVEJUdEpXNDYzZ1pTMlpzUS95TWV0anQ3V2Jib0JGaVVQRjgxdS9ISDdwckFLdUtxWGs3NVM4MUdPM203TkNrOWFPKy9sODE4NEN0aDVtaG80ZW83UXpZeGZRSTNPcFJkaFRqV0FiMzlzNlBSTkVFb2hvKytYL24zRDZaN01TWXU4YW1uL0JPWm5LWlZIR1VuTFN2MUtRQkhvL3BOZDV0dnBBU0tWL2ZteFgza3BVd2prM2prZUM5N0ovUU9mNit5S0JXTTJMeUoySkJvK1Z4Z2R2SnRtZnNvVU1vTTNCUVpnNEZuSzlsVmRlWnNYL0ZxMU5Zdk00UHhMV1NNVXp2OFB4NTI2NVNhQVpTK1d2eG9Rb2ZqV3NacWlmcGdyZnE1eHJUYi82OEltampyMU1BZU1URFZMSnQvLzhpMTZudUR2UHN0ZUhCelpXWjZGck5iLzBiTTNSQUdBRnNySXhyUVV1SDBJbE9WdlhFb0lpQzNMK1NQVDFzYWV4cTlUU045eUJSM1ozdDJ2NXhxS1FSaWx5OVY2a0JSSjd2aVM1b1JPZEFYUEdHbzg1WTJuWnJvb0xIWlFDT25tdERjR3JFQmZ3MDlSWHlYZVpydi9OOWtKUnpCOWhFNDZxK3J0a2R1TzNoblp2QVpTZjE4MkY0cjBWTXpJTUFYc3d2Uy80b3RKRHA4QnowS1ZmbkY3VTl1ZTlTeENtd2Q1b1p2MGpteEhIdTJRSHdVa2FuR1hnL0pwaVFTcUI2N3lsRDhiTWxLOWhlc3YxMnN3VkdKTmJ0V0tXdW8rL2Q4T0ZEMjh3eElYNFEvbm50NCsrK2dFQVpHaG52eWo5eG8rT1hIaGZQTy9pb2h2cjYwWE1ZQlhTWFJDbWFBK0xBV3o2NE5ncHFlNWhrRnNONW55YkRPMVRiVDhxK0x5d3RQTytqN0ZzWXpiMmF3bDlFMDFYd3BJT0hOcDJRRVpFY0lQYjAycVJ6WkJBU2N0VGh5UDNpdnRJd2twek1CSDFUK09mZ3VBNEdjZGhuc1dJNDhBbDFiSUhvK0pIUW1Lc2pBaEs1c3dWZFdJVG5XNmxiSWdTRFc3NUhQaFR2cG1lcGpHRHpjYjFiSHRGVUpCekkwMFhjRjVtMytIN1FKUUtteC9iMjZrVzA2WWdGMEJyT1pZNXNKRDlzTjRjb0E4Y0NSMTlvL0Y2MEJVU0xLK2lhOW1aY2pzdmpLTVp6YWNGaUlqSW04QmY5amxaN2JaSWZwb1RZQUlSZmVWSHAwWTZjRU4xbVRXZENFanl1bUMxWFp5WDM2ZmM2OExLQXJwc05Ib3k2L2p5Vkk1VzVQeTdyL2JhTGVnMnl6cXBCR1JESEtVaW5Ma082TmtyTk8zWmxSMEY5SkpaaXBQM21OamlneXBldmNKWFJ1K3l1ZjNyWU1McXJCYU5GOHA1L08waGhhVzU3dU9JSU1KVEJZWnJtUFJ6d3I3OWZyWllSSzlYN1g2L0NIWXRjV3NGN210ZDVxWGJoUlEzV25JQ29sU0hSWGdFZThseHczVXcyYVlqUWdRUDljbDdVZFRTV1FCUDVBR0YvRkdKT2JxT1MrbkdQQ0FING5PTzdjWDUvVER4NnQrMkswMmg2Qk9VWVI1b2c3MXc1OGd4bzErdm45dkc2QzB4YUI5dms5bDB5VklnYlVnVEZySlROZ2RLdlNsQUZXTmM3RC9mbjlIbUxUVm5RRlRrck51YWtwcVFNUW1haFZXZWhqd3luYU5vT1ZXT0daTTFRR1djaS92dTRCd0thWjQ4Z215bjdjMmxXUG1YOUxSUjNFa2gvaFNScEVIUHV4dk1tS2tNbTBSNTRmNmRJUmQ0b1dmaVFpbXZXSDhHa0JKMnJONnEydmY4anM5cDI3T0RhWXNlV1p3MzhmQ1NrZlgwTmxnT2R5LzAreW5CQ20va2RldExSNzJBd21DWDM3U1N5UmRIWis0YWJudElIREw5SC9FdU5NcmxKZmhabE9qc29GMENMRmh4K0pLN0k2aXd1RXRFVjNOWlpCRjd3RDl4bnlFY2FvenFjakY5RTVSR295UHMrZEJETUVmRmM0dXhaUjBYS0J5YzI2VmwwOEgxaFZJdmZVZmMzWGd0QkU1Q2R3cEtlYVYwZnkwUUlXNy95SkR0YWpMbm1TRE9kQTRHTGtSMTErNmlWeldSUFlLeGJiLzlseG4rMnpaQzBOK3FGeFdqeW5CdCtSMVR5cWdOY3RIeHkvU2JXanQ4Qmw1Q0tZSmJTR1BsWnR0QVhTbG9lcldyS0hLMHpHa0J5R1dndGJ2dUo0T1VDVG1yTzVxK1g5N09Od3d5ZlJTYnBWR0JKVTFyK3R2eWZ6MFRnNDNuaElWV1lwMnI3V0VFVUpHZlk0alBxRmllM0RhYThRVy9VL2xPUy9DLzNHYlcxZXg1ZzFhVG9BUytvN0pxeWVmZEMySkNHbi9zZnVVdjhYcmJmcVBRcGVZTjQ0YkhidjhyczhSVFlOaSsvKzZSUjFBK1p4UWlXYW05NGVsdCtzZ1FkNmMyYVQxRDF3MExmUW9OcEtPdGgycVllSy9VTzdoUFNsTmpjRDFIbUhKYS9wb2lFcldobHJyb2FraksxbVF2RGVlMVovRFArT1d5a3NtOWFuc2tLYUc0T0ZGY2JFT2VWV1Y2TFE2WGN2V04yaitqQ0N3eWo1Z2RqdDF1UUQveExHWUcvbmE1ekJkZERPMDVkQyszemNLRTZLbzJDYkFkY3RvZDcwYXM5ZXI1a3RsMjZFVEs3UmNVSEVGYUtVOHMvL1poQnI0RGkxcUJVTHptdFo1MWZjbW1oeXNseDE1NFk5QXZxVllYU0NpNXRjOVlnZlIwcXFjbHA2ckozZEc0Mm4wUnNBVFFzM3dDSUx1VVJ6Y2dDcXBQNGZYUGpWSW9JYkg4elRMUWlNWWpCZ0VwOXpPeWlPMFNLeTdmdWh2RE5LWE5HeG5Xak9DQWdMR3pXTi9GVkVYWmw1aFEvdXNmMjFuOTJJMk5hNzUyQkhZUDRma3MrUkI3QSthcms5K3BXaEk5bGUxU0RMNS9jMlZCOW1yMHMzd0ZGTVpVcWpld3o5b1EySFdEV2V0UTdiQmpHVEZOellad0ZmYUJGRjhDT2I3ditlbFlOZ1M0eTZ1TStrTjFMWm1lWVhueDcreUV4RjU1RXRhSlVHR1RORTBTZ3loaHVveDJTQlVGeFY2SzVRNkxxL09jN3k0MjR5TE5rNG9Gci91bmk2RGI4NTRBM3dzNm1CZE5LSXQxRGZCTzMvM212YlhYK04xSktLT3NBaVI2djlRc1hHbnhwNG1jZ1Y0c0FyY2VtNVZhMGdheWt2YUtPUHBtY2pCZUVKaUprdjJ3SzA0ZlhaMXZ4V3ZpUURmcllSWk9jQ051bUpxMjZCMUpUQ0k0eFVDNVhCVjZDeVNoNklGNFN4WW5YQ0tVMjFNZ1ErSG0zMC8yWm1pRXRoSG9HaEJNVUQxdThzRUtSSDgzZndpSkEvd3RobmNIUkVBdjhna0xpVGZaRWtjSDdwYTg5M1NhTVpvbkplNExjVDdEZGFsSUltSmsySUZGaXZnU0ZlNTBmRnppV2NpVWpUVnl6a0JyeUJxUVcvMkp4cHFYRkR2ZkF5VzIrT1RRUEdvajVWMTExcm1uU3NwTXBMVVNnK3cxUk4vT1lSc2Y4cWZ6b09waVkvUUlQQVlsRnI2NEZab0lsVm9zNXlxRWNmb3lmOFdMWkFzVnozd2F0YlI0ekxVYmw3REUvdVJJZEY4c29mYXdGRjYxS29MSXBxSHZ0aXhtVW9aaElOMDI1MDROUkFZVzhXd2p4Y2MzcU5jTmNlS2UzNzBWOUFvQysvOWRwb2VCZHYzZzRtK0N1QWVKcEFYZnBLSWxKdTI3Uk1QaHJLelQ2c0hDYnQ2Syt3M0tUa2YzdU45ZEJBeTdadE12eUs0MG5CU2lQRXZtdFprRW9HVHh5Z0JZMUUvUVlIV3E2cTBBTGc4R3hobXJvOXlJdWI0ZE1lMnNFd3FSY1RVWWdQcTR6ZjJadXRsd1VrUWZscVlhUE1DMGhmRkV1WnVYZjlxeWNZdVhXcEtBSCtIUWc3R3NMd2dLRWxFNTdmOU5SaGF2d2w1amc2RkR2UFpqQWk4MkNOSEFnL2NzQnl3dnZnZTM2dG9ybzAyMmNqbDkreE9heHluMUFoeStUTm8xdzM5S3BGVFpjYVhwd3h6Z2xlSXVlS2tuK29XVEdBeUNFazNTU2x6bTl5Ym1Cc0twTWlHVmxPYjdDU0VnVElDQmpDTFZBQnRNUEJqUmZVT09xRVdCalVPZTJCQVhtcWI5b1ZVSUZORGlHbkxRWUhnZTBCVWRhR2RPdFJ5dHhGSldGMEFQeVNvd1I0UUNYeWw5UG5tV3dCY3ltSW1xa1VCdnZ0bHN5U0RTSWZPelp2em5ST0hMelh6azFGekpxQUtwUEFSRG8zMEJ5M0pTS3VEL3FoQW9BUkRDNjJ5anhRUFFxZDNzajR3RVBsbHpvSitycTZidjY1UjhwM0RRdHk2a1FVRzRRVExvdHpJMktzdWNuQ0RRTU0rMnZwMjBhckpKbE1ldlEvVjRZKzJ1K3djRTBCN24vWEJwSUhmK01IM1BSYXNIbGFOM1d4VElBYW9qeDVPejdIZ2U0dDZCRWtHTUEzR0dGeUNsL0toMFE1MkQyVXUvNWxsSm8xbEx3Rkt6NHFGcVp1OGVXNEpncUhzdC9ac1hvckxERFBkaVMvRGJBSFRrMCsvaDVVWEdLQzc2YVhNVFkzU3ZxN2xYRUtYbThDU2VnSEttdzNXazE2Y2VoNnVhMEkvSXlxZFQ1U1Z3VUdhcnM2SHBNa0s1a3p6a1FFUzU4RWs5WjBZYlJSTWtoSkdNKzRnaXlrRnNuOWpPbHRGUFdLUGNTZ00wUTBUa1lTNTQ4UlliYzFGb3pBYlphQWN2ZjFBQisrOXEwRVljYmI3TVFqVkdQRG5SZ0tjZk1mTG81cTJEUDkvUVhjdWtuSmM0WXBYYUlCQkNmQThzOEMrbjA1cC95d3RBV3ZYYkl5cHo5cjVQVUhpenZnVkdqYnZIM0szbktXQXpkSXdsWVh5SDVEZzNzUmkyWm8rOS9hb0RnTWxaQ0ZzcjZFaFVhU2NiSFpJUEpFdHhBWEZCZlA1aWJBYlVjTkNIdEdMNGgyQkNPSVY2bzRKRTU2dE8yU1Y5K3NaRE9YNGlnNFd4emtzUGh0cG5LbUdZekdjZXdUQlR2UXp4ckl6NjB0QmgzNCs3ekkrd1YvOTE3aTdnVm1wK2NteDE5Y3plUVcyaTNROEZSTDVQOUloT1dJakx5Nm91VkVLUHR6YmtCMzJPdVZhYUJBeDNqOGlVZzRQWTdXd25USXJmZFMwV1l4d255cjJnQWFIbERqY0pHUXVDelNYMzJnTVZyRDhwSm42anpjWUhxRUJma1RFU3RSeFE3ckxYaFI0ckNFQkNOTzJteWR6Z25CRitpb2p4amkwd24rK3E1SlNWQmVFbHdvSkcwN1AwZk51RUFWUWdqbzhnR0dnSEl1MytoZ2xvUG1jTXhybWJZaTlKUGxPVmhZOE1HcXhyRWZ3QnlDYWErclhXOEhrZDRUU3EzR0pHTVlhUzRUYWtBeXVyaUpxSXhwS1h3cGJhMCtodzhqV25SRXJFdnV3V1hURmYrc0doSGtYSS90RXp0bFNaTGNGNEJBbDdtQzdVWVp0cW9iSWV3dktoM0dQanY4VXV4ZTRBY3lOS3AwWXdCdHh2VnA3NUV2K0xqL3lLcDd1enVWOVhJc0psMnBrbEhZYU1Jalh0M1hDZ3ZQVWVTNmRBdlIwK3JSNnhmeFBHeHVVQnJEMGF0SlVlQmJialJwRkhhSGZtclFIYW1qTCtOVTBOZkNLWkYyTXhqLzFxb1NTaSs5Ky9XZXVtUlhUUnJxNVErOXBWZ29TWVBiUlJhUTBnODhoc2REWGxuNTBQaW9iMTkvUUg0VFFyb3owemY1WG5qMFhuSVRjM0k0UHZtNnFyc21vOHRNS0Q2YWhqLzEyOCtmTHBwWjJTQTRKdDVjMzlSTWgwc1YveFd6c3NiYkpBd3U4OHRVZDRISi9hVUNWeDFjbFRZMU5hQUxhd2lpMXl0NUJRM3AwUXJ0MENDK1JxZUExQVNSNzh6RlI0MzJkOU5nUnhraVRtQUFBPSIgYWx0PSLlhJLng4/poqjkuq3jgonjgafjgpPjga7jg5XjgqHjg7PjgqLjg7zjg4giIGNsYXNzPSJyYWRlbi1hdmF0YXIiPjxkaXYgY2xhc3M9InJhZGVuLWJ1YmJsZSI+PGI+44GE44KN44KT44Gq44OL44Ol44O844K544GL44KJ44CB5qCq5byP44Gr6Zai5L+C44GZ44KL6Kmx44KS6KaL44Gk44GR44KI44GG44Gt4pmlPC9iPjxwIGlkPSJyYWRlbk5ld3NTdGF0dXMiIGNsYXNzPSJtdXRlZCIgcm9sZT0ic3RhdHVzIj7jg4vjg6Xjg7zjgrnjgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKY8L3A+PGJ1dHRvbiBpZD0icmFkZW5OZXdzQnRuIiBjbGFzcz0ic21hbGxidG4gc2Vjb25kYXJ5IiBvbmNsaWNrPSJsb2FkUmFkZW5OZXdzKCkiPuODi+ODpeODvOOCueOCkuabtOaWsDwvYnV0dG9uPjwvZGl2PjwvZGl2Pgo8ZGV0YWlscyBjbGFzcz0ic3RvY2stZGV0YWlscyI+PHN1bW1hcnk+8J+MnyDjg4vjg6Xjg7zjgrnplqLpgKPjga7ms6jnm67lgJnoo5wgPHNwYW4gaWQ9Im5ld3NVcENvdW50Ij48L3NwYW4+PC9zdW1tYXJ5PjxkaXYgaWQ9Im5ld3NVcFN0b2NrcyIgY2xhc3M9IndhdGNoLWNvbnRlbnQiPjwvZGl2PjwvZGV0YWlscz4KPGRldGFpbHMgY2xhc3M9InN0b2NrLWRldGFpbHMiPjxzdW1tYXJ5PuKaoO+4jyDjg4vjg6Xjg7zjgrnplqLpgKPjga7kuIvokL3orabmiJLlgJnoo5wgPHNwYW4gaWQ9Im5ld3NEb3duQ291bnQiPjwvc3Bhbj48L3N1bW1hcnk+PGRpdiBpZD0ibmV3c0Rvd25TdG9ja3MiIGNsYXNzPSJ3YXRjaC1jb250ZW50Ij48L2Rpdj48L2RldGFpbHM+CjxkZXRhaWxzIGNsYXNzPSJzdG9jay1kZXRhaWxzIj48c3VtbWFyeT7norroqo3jgZfjgZ/nt4/lkIjjg4vjg6Xjg7zjgrk8L3N1bW1hcnk+PGRpdiBpZD0icmFkZW5OZXdzSXRlbXMiPjwvZGl2PjwvZGV0YWlscz48ZGV0YWlscz48c3VtbWFyeT7kv53mnInmoKrjg7vkv53lrZjjg4fjg7zjgr/jga7norroqo3jg53jgqTjg7Pjg4g8L3N1bW1hcnk+PGRpdiBpZD0icmFkZW5BZHZpY2UiIGFyaWEtbGl2ZT0icG9saXRlIj48L2Rpdj48L2RldGFpbHM+CjxwIGNsYXNzPSJtdXRlZCI+57eP5ZCI44OL44Ol44O844K577yI5pS/5rK744O756S+5Lya44O75Zu96Zqb44O757WM5riI44O756eR5a2m44Gq44Gp77yJ44Gu6KaL5Ye644GX44Go5qWt56iu44KS54Wn5ZCI44GX44CB5b2x6Z+/44KS56K66KqN44GZ44KL5YCZ6KOc44KS6KGo56S644GX44G+44GZ44CC5YWo6KiY5LqL44KS57ay576F44GZ44KL44KC44Gu44Gn44Gv44Gq44GP44CB57eP5ZCI6YWN5L+h44GL44KJ5pyA5aSnMzDku7bjgpLnorroqo3jgZfjgb7jgZnjgILmoKrlvI/jgajjga7plqLpgKPmnaHku7bjgavkuIDoh7TjgZfjgarjgYTjg4vjg6Xjg7zjgrnjga/pipjmn4TlgJnoo5zjgbjlj43mmKDjgZfjgb7jgZvjgpPjgILlkITmrITmnIDlpKcxMOmKmOafhOODu+OCs+ODvOODiemghuOBp+OAgeaOqOWlqOmghuS9jeOBp+OBr+OBguOCiuOBvuOBm+OCk+OAguiomOS6i+WFqOaWh+OCkuiqreOCgEFJ5YiG5p6Q44KE5qCq5L6h5LqI5ris44Gn44Gv44Gq44GP44CB5ZCM44GY6YqY5p+E44GM5Lih5pa544Gr5Ye644KL5aC05ZCI44KC44GC44KK44G+44GZ44CC44OL44Ol44O844K544Gu5LqL5a6f6Zai5L+C44Go5b2x6Z+/44Gv5Y6f5paH44Gn56K66KqN44GX44Gm44GP44Gg44GV44GE44CC55S75YOP44Gv44OV44Kh44Oz44Ki44O844OI44CB44Kz44Oh44Oz44OI44Gv44Ki44OX44Oq54us6Ieq44Gu44KC44Gu44Gn44CB5pys5Lq644Gu55m66KiA44KE5YWs5byP44Gu5oqV6LOH5Yqp6KiA44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgo8L2Rpdj4KPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKbGV0IHJhZGVuTmV3c0J1c3k9ZmFsc2U7CmZ1bmN0aW9uIHJlYWRSYWRlbk5ld3MoKXt0cnl7cmV0dXJuIEpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfcmFkZW5fbmV3c192MScpfHwnbnVsbCcpfWNhdGNoKGUpe3JldHVybiBudWxsfX0KZnVuY3Rpb24gbmV3c0NhbmRpZGF0ZVJ1bGVzKGl0ZW0pewogIGNvbnN0IHRpdGxlPVN0cmluZyhpdGVtLnRpdGxlfHwnJyk7CiAgY29uc3QgcmlzaW5nPS/kuIrmmId85oCl6aiwfOmrmOmosHzlgKTkuIrjgYzjgop85LiK5oyv44KMLzsKICBjb25zdCBmYWxsaW5nPS/kuIvokL185oCl6JC9fOS9juS4i3zlgKTkuIvjgYzjgop85LiL5oyv44KMLzsKICBjb25zdCBydWxlcz1bXTsKICBjb25zdCBhZGQ9KHVwLGRvd24scmVhc29uKT0+cnVsZXMucHVzaCh7dXAsZG93bixyZWFzb259KTsKICBpZigv5Y6f5rK5Ly50ZXN0KHRpdGxlKSl7CiAgICBpZihyaXNpbmcudGVzdCh0aXRsZSkmJiFmYWxsaW5nLnRlc3QodGl0bGUpKWFkZChbJ01ham9yIE9pbCAmIEdhcycsJ09pbCBFeHRyYWN0aW9uJ10sWydQYXNzZW5nZXIgQWlybGluZXMnLCdUcnVja2luZyddLCfljp/msrnkuIrmmIfjga7opovlh7rjgZfvvJros4fmupDosqnlo7Ljgajnh4PmlpnjgrPjgrnjg4jjga7lvbHpn7/jgpLliIbjgZHjgabnorroqo3jgIInKTsKICAgIGlmKGZhbGxpbmcudGVzdCh0aXRsZSkmJiFyaXNpbmcudGVzdCh0aXRsZSkpYWRkKFsnUGFzc2VuZ2VyIEFpcmxpbmVzJywnVHJ1Y2tpbmcnXSxbJ01ham9yIE9pbCAmIEdhcycsJ09pbCBFeHRyYWN0aW9uJ10sJ+WOn+ayueS4i+iQveOBruimi+WHuuOBl++8mueHg+aWmeOCs+OCueODiOS9juS4i+OBqOizh+a6kOWPjuebiuOBuOOBruW9semfv+OCkueiuuiqjeOAgicpOwogIH0KICBpZigv5YaG5a6JLy50ZXN0KHRpdGxlKSYmIS/lhobpq5h85YaG5a6JLioo5piv5q2jfOS4gOacjXzmra/mraLjgoEpLy50ZXN0KHRpdGxlKSlhZGQoWydBdXRvbW9iaWxlcycsJ0F1dG8gJiBDb21tZXJjaWFsIFZlaGljbGUgUGFydHMnXSxbJ1Bhc3NlbmdlciBBaXJsaW5lcycsJ0Zvb2QgUHJvZHVjdHMnXSwn5YaG5a6J44Gu6KaL5Ye644GX77ya5rW35aSW5aOy5LiK44Go6Ly45YWl44O754eD5paZ44Kz44K544OI44Gu5b2x6Z+/44KS56K66KqN44CC54K65pu/5LqI57SE44Gq44Gp44Gn5b2x6Z+/44Gv55Ww44Gq44KK44G+44GZ44CCJyk7CiAgaWYoL+WGhumrmC8udGVzdCh0aXRsZSkmJiEv5YaG5a6JfOWGhumrmC4qKOS4gOacjXzmra/mraLjgoEpLy50ZXN0KHRpdGxlKSlhZGQoWydQYXNzZW5nZXIgQWlybGluZXMnLCdGb29kIFByb2R1Y3RzJ10sWydBdXRvbW9iaWxlcycsJ0F1dG8gJiBDb21tZXJjaWFsIFZlaGljbGUgUGFydHMnXSwn5YaG6auY44Gu6KaL5Ye644GX77ya6Ly45YWl44Kz44K544OI44Go5rW35aSW5aOy5LiK44Gu5YaG5o+b566X44G444Gu5b2x6Z+/44KS56K66KqN44CCJyk7CiAgaWYoL+aXpemKgHzml6XmnKzpioDooYwvLnRlc3QodGl0bGUpJiYhL+imi+mAgeOCinzmja7jgYjnva7jgY185ZCm5a6afOaSpOWbni8udGVzdCh0aXRsZSkpewogICAgaWYoL+WIqeS4iuOBki8udGVzdCh0aXRsZSkmJiEv5Yip5LiL44GSLy50ZXN0KHRpdGxlKSlhZGQoWydCYW5raW5nJywnTWFqb3IgSW50ZXJuYXRpb25hbCBCYW5rcyddLFsnUmVhbCBFc3RhdGUgRGV2ZWxvcGVycyddLCfml6XpioDjga7liKnkuIrjgZLplqLpgKPvvJrpioDooYzjga7liKnjgZbjgoTjgajkuI3li5XnlKPjga7lgJ/lhaXjgrPjgrnjg4jjgpLnorroqo3jgILmsbrlrprjg7vkuojmg7Pjga7pgZXjgYTjgavms6jmhI/jgIInKTsKICAgIGlmKC/liKnkuIvjgZIvLnRlc3QodGl0bGUpJiYhL+WIqeS4iuOBki8udGVzdCh0aXRsZSkpYWRkKFsnUmVhbCBFc3RhdGUgRGV2ZWxvcGVycyddLFsnQmFua2luZycsJ01ham9yIEludGVybmF0aW9uYWwgQmFua3MnXSwn5pel6YqA44Gu5Yip5LiL44GS6Zai6YCj77ya5YCf5YWl44Kz44K544OI44Go6YqA6KGM44Gu5Yip44GW44KE44KS56K66KqN44CC5rG65a6a44O75LqI5oOz44Gu6YGV44GE44Gr5rOo5oSP44CCJyk7CiAgfQogIGlmKC/ljYrlsI7kvZMvLnRlc3QodGl0bGUpKXsKICAgIGlmKC/ovLjlh7ropo/liLZ85Yi26KOBfOmcgOimgea4m3zmuJvpgJ8vLnRlc3QodGl0bGUpKWFkZChbXSxbJ1NlbWljb25kdWN0b3JzJ10sJ+WNiuWwjuS9k+OBruimj+WItuODu+mcgOimgea4m+mWoumAo++8muWvvuixoeijveWTgeOAgeiyqeWjsuWFiOOAgemBqeeUqOaZguacn+OCkueiuuiqjeOAgicpOwogICAgZWxzZSBpZigv6ZyA6KaB5aKXfOmcgOimgeaLoeWkp3zlj5fms6jlopd85aKX55uKLy50ZXN0KHRpdGxlKSlhZGQoWydTZW1pY29uZHVjdG9ycyddLFtdLCfljYrlsI7kvZPjga7pnIDopoHjg7vmpa3nuL7mlLnlloTplqLpgKPvvJroqJjkuovjga7lr77osaHkvIHmpa3jgajoh6rliIbjga7pipjmn4Tjga7pgZXjgYTjgpLnorroqo3jgIInKTsKICB9CiAgaWYoL+mWoueoji8udGVzdCh0aXRsZSkmJi/lvJXjgY3kuIrjgZJ85byV5LiK44GSfOWil+eojnzov73liqDplqLnqI4vLnRlc3QodGl0bGUpJiYhL+aSpOWbnnzopovpgIHjgop85YWN6ZmkLy50ZXN0KHRpdGxlKSlhZGQoW10sWydBdXRvbW9iaWxlcycsJ0F1dG8gJiBDb21tZXJjaWFsIFZlaGljbGUgUGFydHMnXSwn6Zai56iO5byV44GN5LiK44GS6Zai6YCj77ya5a++6LGh5Zu944O75ZOB55uu44Go5ZCE56S+44Gu54++5Zyw55Sf55Sj5q+U546H44KS56K66KqN44CCJyk7CiAgcmV0dXJuIHJ1bGVzOwp9CmZ1bmN0aW9uIGJ1aWxkTmV3c0NhbmRpZGF0ZXMoaXRlbXMsc3RvY2tzKXsKICBjb25zdCBncm91cHM9e3VwOm5ldyBNYXAoKSxkb3duOm5ldyBNYXAoKX07CiAgZm9yKGNvbnN0IGl0ZW0gb2YgaXRlbXMpewogICAgZm9yKGNvbnN0IHJ1bGUgb2YgbmV3c0NhbmRpZGF0ZVJ1bGVzKGl0ZW0pKXsKICAgICAgZm9yKGNvbnN0IGtpbmQgb2YgWyd1cCcsJ2Rvd24nXSl7CiAgICAgICAgZm9yKGNvbnN0IHN0b2NrIG9mIHN0b2Nrcyl7CiAgICAgICAgICBpZighcnVsZVtraW5kXS5pbmNsdWRlcyhzdG9jay5zZWN0b3IpKWNvbnRpbnVlOwogICAgICAgICAgaWYoIWdyb3Vwc1traW5kXS5oYXMoc3RvY2suY29kZSkpZ3JvdXBzW2tpbmRdLnNldChzdG9jay5jb2RlLHtzdG9jayxldmlkZW5jZTpbXX0pOwogICAgICAgICAgY29uc3QgZW50cnk9Z3JvdXBzW2tpbmRdLmdldChzdG9jay5jb2RlKTsKICAgICAgICAgIGlmKCFlbnRyeS5ldmlkZW5jZS5zb21lKGU9PmUuaXRlbS5saW5rPT09aXRlbS5saW5rKSllbnRyeS5ldmlkZW5jZS5wdXNoKHtpdGVtLHJlYXNvbjpydWxlLnJlYXNvbn0pOwogICAgICAgIH0KICAgICAgfQogICAgfQogIH0KICByZXR1cm4ge3VwOlsuLi5ncm91cHMudXAudmFsdWVzKCldLnNvcnQoKGEsYik9PmEuc3RvY2suY29kZS5sb2NhbGVDb21wYXJlKGIuc3RvY2suY29kZSkpLGRvd246Wy4uLmdyb3Vwcy5kb3duLnZhbHVlcygpXS5zb3J0KChhLGIpPT5hLnN0b2NrLmNvZGUubG9jYWxlQ29tcGFyZShiLnN0b2NrLmNvZGUpKX07Cn0KZnVuY3Rpb24gbWFrZU5ld3NMaW5rKGl0ZW0pewogIHRyeXtjb25zdCB1cmw9bmV3IFVSTChpdGVtLmxpbmspO2lmKHVybC5wcm90b2NvbCE9PSdodHRwczonfHx1cmwuaG9zdG5hbWUhPT0nbmV3cy5nb29nbGUuY29tJylyZXR1cm4gbnVsbDsKICAgIGNvbnN0IGE9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnYScpO2EudGV4dENvbnRlbnQ9aXRlbS50aXRsZTthLmhyZWY9dXJsLmhyZWY7YS50YXJnZXQ9J19ibGFuayc7YS5yZWw9J25vb3BlbmVyIG5vcmVmZXJyZXInO3JldHVybiBhOwogIH1jYXRjaChlKXtyZXR1cm4gbnVsbDt9Cn0KZnVuY3Rpb24gcmVuZGVyTmV3c0NhbmRpZGF0ZUdyb3VwKGtpbmQsZW50cmllcyl7CiAgY29uc3QgYm94PSQoa2luZD09PSd1cCc/J25ld3NVcFN0b2Nrcyc6J25ld3NEb3duU3RvY2tzJyk7aWYoIWJveClyZXR1cm47CiAgY29uc3Qgb3BlbmVkPW5ldyBTZXQoQXJyYXkuZnJvbShib3gucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlsc1tvcGVuXScpKS5tYXAoZWw9PmVsLmRhdGFzZXQubmV3c3N0b2NrKSk7CiAgYm94LnJlcGxhY2VDaGlsZHJlbigpOyQoa2luZD09PSd1cCc/J25ld3NVcENvdW50JzonbmV3c0Rvd25Db3VudCcpLnRleHRDb250ZW50PWVudHJpZXMubGVuZ3RoKyfku7YnOwogIGlmKCFlbnRyaWVzLmxlbmd0aCl7Y29uc3QgcD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdwJyk7cC5jbGFzc05hbWU9J211dGVkJztwLnRleHRDb250ZW50PSfku4rlm57jga7jg4vjg6Xjg7zjgrnjgafjgIHmnaHku7bjgavkuIDoh7TjgZnjgovlgJnoo5zjga/jgYLjgorjgb7jgZvjgpPjgIInO2JveC5hcHBlbmRDaGlsZChwKTtyZXR1cm47fQogIGNvbnN0IG1vdmVtZW50PXJlYWRNb3ZlbWVudCgpOwogIGZvcihjb25zdCBlbnRyeSBvZiBlbnRyaWVzLnNsaWNlKDAsMTApKXsKICAgIGNvbnN0IHN0b2NrPWVudHJ5LnN0b2NrLGE9bW92ZW1lbnRbc3RvY2suY29kZV0scT1hJiZhLnEsbT1tb3ZlbWVudE1ldHJpY3MocSk7CiAgICBjb25zdCBuYW1lPShxJiZxLmNvbXBhbnkmJnEuY29tcGFueS5uYW1lKXx8c3RvY2submFtZXx8c3RvY2suY29kZTsKICAgIGNvbnN0IGRldGFpbHM9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnZGV0YWlscycpO2RldGFpbHMuY2xhc3NOYW1lPSdzdG9jay1kZXRhaWxzJztkZXRhaWxzLmRhdGFzZXQubmV3c3N0b2NrPXN0b2NrLmNvZGU7ZGV0YWlscy5vcGVuPW9wZW5lZC5oYXMoc3RvY2suY29kZSk7CiAgICBjb25zdCBzdW1tYXJ5PWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3N1bW1hcnknKTtzdW1tYXJ5LnRleHRDb250ZW50PW5hbWUrJ++8iCcrc3RvY2suY29kZSsn77yJJztkZXRhaWxzLmFwcGVuZENoaWxkKHN1bW1hcnkpOwogICAgY29uc3QgY29udGVudD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdkaXYnKTtjb250ZW50LmNsYXNzTmFtZT0nd2F0Y2gtY29udGVudCc7CiAgICBjb25zdCBpbnRybz1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdwJyk7aW50cm8uY2xhc3NOYW1lPSdyYWRlbi1uZXdzLWNvbW1lbnQnO2ludHJvLnRleHRDb250ZW50PWtpbmQ9PT0ndXAnPyfjgZPjga7mpa3nqK7jgavov73jgYTpoqjjgajjgarjgorlvpfjgovjg4vjg6Xjg7zjgrnjgaDjgojjgILkvJrnpL7jgZTjgajjga7lvbHpn7/jgajjgIHku4rjga7moKrkvqHjgpLnorroqo3jgZfjgabjgb/jgojjgYbjga3imaUnOifjgZPjga7mpa3nqK7jga7pgIbpoqjjgavjgarjgorlvpfjgovjg4vjg6Xjg7zjgrnjgaDjgojjgILkuIvokL3jgYzmsbrjgb7jgaPjgZ/jgo/jgZHjgafjga/jgarjgYTjga7jgafjgIHkvJrnpL7jgZTjgajjga7lvbHpn7/jgpLnorroqo3jgZfjgojjgYbjga3imaUnO2NvbnRlbnQuYXBwZW5kQ2hpbGQoaW50cm8pOwogICAgY29uc3QgaW5mbz1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdwJyk7aW5mby5jbGFzc05hbWU9J211dGVkJztpbmZvLnRleHRDb250ZW50PSfmpa3nqK7vvJonKyhzdG9jay5zZWN0b3J8fCfkuI3mmI4nKSsnIC8gJysocSYmIWEuZXJyb3I/J+S/neWtmOWxpeattCAnKyhtLmRhdGV8fCfigJQnKSsnIC8gMeWWtualreaXpSAnK3BjdChtLmRheSkrJyAvIDXllrbmpa3ml6UgJytwY3QobS5maXZlKTon5qCq5L6h5bGl5q2044Gv5pyq5Y+W5b6X44O75Y+W5b6X5aSx5pWX44CC44OL44Ol44O844K544Go44Gu6Zai6YCj44Gg44GR44KS6KGo56S644CCJyk7Y29udGVudC5hcHBlbmRDaGlsZChpbmZvKTsKICAgIGZvcihjb25zdCBldmlkZW5jZSBvZiBlbnRyeS5ldmlkZW5jZS5zbGljZSgwLDMpKXsKICAgICAgY29uc3QgbGluaz1tYWtlTmV3c0xpbmsoZXZpZGVuY2UuaXRlbSk7aWYobGluayljb250ZW50LmFwcGVuZENoaWxkKGxpbmspOwogICAgICBjb25zdCBwPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3AnKTtwLmNsYXNzTmFtZT0nbXV0ZWQnO3AudGV4dENvbnRlbnQ9ZXZpZGVuY2UuaXRlbS5zb3VyY2UrJyAvICcrbmV3IERhdGUoZXZpZGVuY2UuaXRlbS5wdWJsaXNoZWRfYXQpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpKycgLyAnK2V2aWRlbmNlLnJlYXNvbjtjb250ZW50LmFwcGVuZENoaWxkKHApOwogICAgfQogICAgY29uc3QgYnRuPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2J1dHRvbicpO2J0bi5jbGFzc05hbWU9J3NtYWxsYnRuIHNlY29uZGFyeSc7YnRuLnRleHRDb250ZW50PSfjgqbjgqnjg4Pjg4Hjg6rjgrnjg4jjgavov73liqAnO2J0bi5vbmNsaWNrPSgpPT57CiAgICAgIGNvbnN0IHdhdGNoZXM9bG9jYWwoJ2ZyZWVfd2F0Y2gnKTtpZighd2F0Y2hlcy5zb21lKHg9PlN0cmluZyh0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKT09PXN0b2NrLmNvZGUpKXt3YXRjaGVzLnB1c2goe2NvZGU6c3RvY2suY29kZSxuYW1lfSk7c2F2ZSgnZnJlZV93YXRjaCcsd2F0Y2hlcyk7cmVuZGVyV2F0Y2goKTt9CiAgICAgIGJ0bi50ZXh0Q29udGVudD0n44Km44Kp44OD44OB44Oq44K544OI44Gr55m76Yyy5riI44G/JztidG4uZGlzYWJsZWQ9dHJ1ZTsKICAgIH07Y29udGVudC5hcHBlbmRDaGlsZChidG4pO2RldGFpbHMuYXBwZW5kQ2hpbGQoY29udGVudCk7Ym94LmFwcGVuZENoaWxkKGRldGFpbHMpOwogIH0KfQpmdW5jdGlvbiByZW5kZXJSYWRlbk5ld3MoZmFpbGVkPWZhbHNlKXsKICBjb25zdCBib3g9JCgncmFkZW5OZXdzSXRlbXMnKSxzdGF0dXM9JCgncmFkZW5OZXdzU3RhdHVzJyk7aWYoIWJveClyZXR1cm47CiAgY29uc3QgbmV3cz1yZWFkUmFkZW5OZXdzKCk7Ym94LnJlcGxhY2VDaGlsZHJlbigpOwogIGlmKCFuZXdzKXtzdGF0dXMudGV4dENvbnRlbnQ9ZmFpbGVkPyfjg4vjg6Xjg7zjgrnmnKrlj5blvpfjgILlho3luqbmm7TmlrDjgZfjgabjga3jgIInOifjg4vjg6Xjg7zjgrnjgpLlj5blvpfjgZfjgabjgYTjgb7jgZnigKYnO3JlbmRlck5ld3NDYW5kaWRhdGVHcm91cCgndXAnLFtdKTtyZW5kZXJOZXdzQ2FuZGlkYXRlR3JvdXAoJ2Rvd24nLFtdKTtyZXR1cm47fQogIGNvbnN0IHZhbGlkPShuZXdzLml0ZW1zfHxbXSkuZmlsdGVyKGl0ZW09Pntjb25zdCBhZ2U9RGF0ZS5ub3coKS1EYXRlLnBhcnNlKGl0ZW0ucHVibGlzaGVkX2F0KTtyZXR1cm4gTnVtYmVyLmlzRmluaXRlKGFnZSkmJmFnZT49LTM2MDAwMDAmJmFnZTw9NzIqMzYwMDAwMH0pOwogIGNvbnN0IGdyb3Vwcz1idWlsZE5ld3NDYW5kaWRhdGVzKHZhbGlkLGFsbE1hcmtldFN0b2NrcygpKTtyZW5kZXJOZXdzQ2FuZGlkYXRlR3JvdXAoJ3VwJyxncm91cHMudXApO3JlbmRlck5ld3NDYW5kaWRhdGVHcm91cCgnZG93bicsZ3JvdXBzLmRvd24pOwogIHN0YXR1cy50ZXh0Q29udGVudD0oZmFpbGVkPyflj5blvpflpLHmlZfjg7vkv53lrZjmuIjjgb/liIbjgpLooajnpLrjgIInOicnKSsn5Y+W5b6XICcrbmV3IERhdGUobmV3cy5mZXRjaGVkX2F0KS50b0xvY2FsZVN0cmluZygnamEtSlAnKSsnIC8g6YWN5L+hICcrdmFsaWQubGVuZ3RoKyfku7YgLyAnK2FsbE1hcmtldFN0b2NrcygpLmxlbmd0aCsn6YqY5p+E44Gu5qWt56iu44Go54Wn5ZCIJzsKICBpZighdmFsaWQubGVuZ3RoKXtjb25zdCBwPWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3AnKTtwLnRleHRDb250ZW50PSfmnaHku7bjgavlkIjjgYbmlrDjgZfjgYTjg4vjg6Xjg7zjgrnjgYzjgYLjgorjgb7jgZvjgpPjgIInO2JveC5hcHBlbmRDaGlsZChwKTt9CiAgZm9yKGNvbnN0IGl0ZW0gb2YgdmFsaWQpewogICAgY29uc3QgY2FyZD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdkaXYnKTtjYXJkLmNsYXNzTmFtZT0ncmFkZW4tbmV3cy1jYXJkJztjb25zdCBhPW1ha2VOZXdzTGluayhpdGVtKTtpZighYSljb250aW51ZTtjYXJkLmFwcGVuZENoaWxkKGEpOwogICAgY29uc3QgcD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdwJyk7cC5jbGFzc05hbWU9J211dGVkJztwLnRleHRDb250ZW50PWl0ZW0uc291cmNlKycgLyAnK25ldyBEYXRlKGl0ZW0ucHVibGlzaGVkX2F0KS50b0xvY2FsZVN0cmluZygnamEtSlAnKTtjYXJkLmFwcGVuZENoaWxkKHApO2JveC5hcHBlbmRDaGlsZChjYXJkKTsKICB9Cn0KYXN5bmMgZnVuY3Rpb24gbG9hZFJhZGVuTmV3cygpewogIGlmKHJhZGVuTmV3c0J1c3kpcmV0dXJuO3JhZGVuTmV3c0J1c3k9dHJ1ZTtjb25zdCBidG49JCgncmFkZW5OZXdzQnRuJyk7YnRuLmRpc2FibGVkPXRydWU7CiAgJCgncmFkZW5OZXdzU3RhdHVzJykudGV4dENvbnRlbnQ9J+acgOaWsOOBrumFjeS/oeOCkueiuuiqjeS4reKApic7CiAgdHJ5ewogICAgY29uc3QgcmVzcG9uc2U9YXdhaXQgZmV0Y2goJy9hcGkvZnJlZS9uZXdzLWFkdmljZT9yZWZyZXNoPTEnLHtjYWNoZTonbm8tc3RvcmUnfSksbmV3cz1hd2FpdCByZXNwb25zZS5qc29uKCk7CiAgICBpZighcmVzcG9uc2Uub2t8fG5ld3Muc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IobmV3cy5yZWFzb258fCflj5blvpfjgqjjg6njg7wnKTsKICAgIGxvY2FsU3RvcmFnZS5zZXRJdGVtKCdmcmVlX3JhZGVuX25ld3NfdjEnLEpTT04uc3RyaW5naWZ5KG5ld3MpKTsKICAgIGF3YWl0IGVuc3VyZU1hcmtldFVuaXZlcnNlKCk7cmVuZGVyUmFkZW5OZXdzKCk7CiAgfWNhdGNoKGUpe3JlbmRlclJhZGVuTmV3cyh0cnVlKX1maW5hbGx5e3JhZGVuTmV3c0J1c3k9ZmFsc2U7YnRuLmRpc2FibGVkPWZhbHNlO30KfQpmdW5jdGlvbiBhZHZpY2VKYXBhbkRheShub3c9bmV3IERhdGUoKSl7CiAgcmV0dXJuIG5ldyBEYXRlKG5vdy5nZXRUaW1lKCkrOSozNjAwMDAwKS50b0lTT1N0cmluZygpLnNsaWNlKDAsMTApOwp9CmZ1bmN0aW9uIGJ1aWxkUmFkZW5BZHZpY2UoaG9sZGluZ3Msd2F0Y2hlcyxtb3ZlbWVudCxub3c9bmV3IERhdGUoKSl7CiAgY29uc3QgZGF5PWFkdmljZUphcGFuRGF5KG5vdyk7CiAgY29uc3QgYWdlPWRhdGU9PnsKICAgIGNvbnN0IHRleHQ9U3RyaW5nKGRhdGV8fCcnKS5zbGljZSgwLDEwKTtpZighL15cZHs0fS1cZHsyfS1cZHsyfSQvLnRlc3QodGV4dCkpcmV0dXJuIG51bGw7CiAgICBjb25zdCBzdGFtcD1EYXRlLnBhcnNlKHRleHQrJ1QwMDowMDowMFonKTtpZighTnVtYmVyLmlzRmluaXRlKHN0YW1wKSlyZXR1cm4gbnVsbDsKICAgIHJldHVybiBNYXRoLmZsb29yKChEYXRlLnBhcnNlKGRheSsnVDAwOjAwOjAwWicpLXN0YW1wKS84NjQwMDAwMCk7CiAgfTsKICBjb25zdCBmcmVzaD1kYXRlPT57Y29uc3Qgbj1hZ2UoZGF0ZSk7cmV0dXJuIG4hPT1udWxsJiZuPj0wJiZuPD00fTsKICBjb25zdCBuYW1lcz1hPT5hLnNsaWNlKDAsMykubWFwKHg9PnguY29tcGFueV9uYW1lfHx4LmNvZGUpLmpvaW4oJ+OAgScpKyhhLmxlbmd0aD4zPycg44G744GLJzonJyk7CiAgY29uc3QgbGluZXM9W107CiAgY29uc3Qgc3RhbGU9aG9sZGluZ3MuZmlsdGVyKHg9PiFmcmVzaCh4LmFzb2YpKTsKICBjb25zdCB2YWxpZD1ob2xkaW5ncy5maWx0ZXIoeD0+ZnJlc2goeC5hc29mKSYmTnVtYmVyKHguY3VycmVudF9wcmljZSk+MCYmTnVtYmVyKHguY29zdCk+MCYmTnVtYmVyKHguc2hhcmVzKT4wKTsKICBjb25zdCBzdG9wPXZhbGlkLmZpbHRlcih4PT54LnN0b3BfcGN0IT1udWxsJiZOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHguc3RvcF9wY3QpKSYmTnVtYmVyKHguY3VycmVudF9wcmljZSk8PU51bWJlcih4LmNvc3QpKigxLU51bWJlcih4LnN0b3BfcGN0KS8xMDApKTsKICBjb25zdCB0YWtlPXZhbGlkLmZpbHRlcih4PT54LnRha2VfcGN0IT1udWxsJiZOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHgudGFrZV9wY3QpKSYmTnVtYmVyKHguY3VycmVudF9wcmljZSk+PU51bWJlcih4LmNvc3QpKigxK051bWJlcih4LnRha2VfcGN0KS8xMDApKTsKICBpZihzdG9wLmxlbmd0aClsaW5lcy5wdXNoKGAke25hbWVzKHN0b3ApfeOBr+OAgeS/neWtmOOBleOCjOOBn+e1guWApOOBjOioreWumuOBruaQjeWIh+OCiuODqeOCpOODs+S7peS4i+OBoOOCiOOAguOBvuOBmuS7iuOBruS+oeagvOOBqOOAgeaxuuOCgeOBpuOBhOOBn+ODq+ODvOODq+OCkueiuuiqjeOBl+OCiOOBhuOBreKZpWApOwogIGlmKHRha2UubGVuZ3RoKWxpbmVzLnB1c2goYCR7bmFtZXModGFrZSl944Gv44CB5L+d5a2Y44GV44KM44Gf57WC5YCk44GM6Kit5a6a44Gu5Yip56K644Op44Kk44Oz44Gr5bGK44GE44Gm44GE44KL44KI44CC5omL5pWw5paZ6L6844G/44Gu5pCN55uK44KS6KaL44Gm44CB5aOy44KL5qCq5pWw44KS6JC944Gh552A44GE44Gm6ICD44GI44KI44GG44Gt4pmlYCk7CiAgaWYoc3RhbGUubGVuZ3RoKWxpbmVzLnB1c2goYCR7bmFtZXMoc3RhbGUpfeOBr+OAgeagquS+oeOBruaXpeS7mOOBjOWPpOOBhOODu+S4jeaYjuODu+acquadpeaXpeOBquOBruOBp+WIpOaWreOBr+OBiumgkOOBkeOAguacgOaWsOe1guWApOOCkuabtOaWsOOBl+OBpuOBi+OCieS4gOe3kuOBq+imi+OCiOOBhuOBreKZpWApOwogIGNvbnN0IHRvdGFsPXZhbGlkLnJlZHVjZSgoc3VtLHgpPT5zdW0rTnVtYmVyKHguY3VycmVudF9wcmljZSkqTnVtYmVyKHguc2hhcmVzKSwwKTsKICBjb25zdCBsYXJnZT12YWxpZC5maW5kKHg9Pk51bWJlcih4LmN1cnJlbnRfcHJpY2UpKk51bWJlcih4LnNoYXJlcyk+dG90YWwqMC41KTsKICBpZih2YWxpZC5sZW5ndGg+PTImJmxhcmdlKWxpbmVzLnB1c2goYCR7bGFyZ2UuY29tcGFueV9uYW1lfHxsYXJnZS5jb2RlfeOBjOOAgeaXpeS7mOOCkueiuuiqjeOBp+OBjeOBn+S/neacieagquOBruipleS+oemhjeOBruWNiuWIhuOCkui2heOBiOOBpuOBhOOCi+OCiOOAguS4gOmKmOafhOOBuOOBruWBj+OCiuOCgueiuuiqjeOBl+OBpuOBiuOBk+OBhuOBreOAgmApOwogIGNvbnN0IGVudHJpZXM9T2JqZWN0LnZhbHVlcyhtb3ZlbWVudHx8e30pLHJlY2VudD1lbnRyaWVzLmZpbHRlcih4PT54JiYheC5lcnJvciYmeC5xJiZmcmVzaCgoeC5xLnNuYXBzaG90fHx7fSkubGFzdF9kYXRlKSk7CiAgaWYoZW50cmllcy5sZW5ndGgmJiFyZWNlbnQubGVuZ3RoKWxpbmVzLnB1c2goJ+W3oeWbnue1kOaenOOBq+aWsOOBl+OBhOagquS+oeODh+ODvOOCv+OBjOimi+OBpOOBi+OCieOBquOBhOOCiOOAguOAjOacrOaXpeOBruWAmeijnOOAjeOBqOOBl+OBpuS9v+OBhuWJjeOBq+ODh+ODvOOCv+aXpeOCkueiuuiqjeOBl+OBpuOBreKZpScpOwogIGVsc2UgaWYocmVjZW50Lmxlbmd0aClsaW5lcy5wdXNoKGDlt6Hlm57ntZDmnpzjgavjga/jgIE05pel5Lul5YaF44Gu5pel5LuY44Gu44OH44O844K/44GMJHtyZWNlbnQubGVuZ3RofemKmOafhOOBguOCi+OCiOOAguW9k+aXpeOBruWApOWLleOBjeOBqOOBr+mZkOOCieOBquOBhOOBruOBp+OAgeawl+OBq+OBquOCi+WAmeijnOOBr+ODh+ODvOOCv+aXpeOBqOODgeODo+ODvOODiOOCguimi+OBpuOBrfCfkYDinKhgKTsKICBpZighaG9sZGluZ3MubGVuZ3RoJiZ3YXRjaGVzLmxlbmd0aClsaW5lcy5wdXNoKGDjgqbjgqnjg4Pjg4Hjg6rjgrnjg4jjga8ke3dhdGNoZXMubGVuZ3RofemKmOafhOOAguiyt+OBhOWAmeijnOOBruihqOekuuOBoOOBkeOBp+axuuOCgeOBmuOAgeagueaLoOODu+ODh+ODvOOCv+aXpeODu+iyt+OBo+OBn+W+jOOBruaQjeWIh+OCiuODq+ODvOODq+OCkueiuuiqjeOBl+OBpuOBreKZpWApOwogIGlmKCFob2xkaW5ncy5sZW5ndGgmJiF3YXRjaGVzLmxlbmd0aClsaW5lcy5wdXNoKCfmsJfjgavjgarjgovpipjmn4TjgpLjgqbjgqnjg4Pjg4Hjg6rjgrnjg4jjgavlhaXjgozjgabjgb/jgojjgYbjga3jgILmnIDliJ3jga/lsJHjgZfjgZrjgaTmr5TjgbnjgabjgIHoh6rliIbjgYzoqqzmmI7jgafjgY3jgovpipjmn4TjgpLmjqLjgZfjgabjgYTjgZPjgYbimaUnKTsKICBjb25zdCB0aXBzPVsKICAgICfku4rml6Xjga7jgbLjgajjgZPjgajvvJrosrfjgYbliY3jgavjgIzjganjgZPjgb7jgafkuIvjgYzjgaPjgZ/jgonopovnm7TjgZnjgYvjgI3jgpLmsbrjgoHjgabjgYrjgZPjgYbjgILnhKbjgonjgZrjgYTjgZPjgYbjga3imaUnLAogICAgJ+S7iuaXpeOBruOBsuOBqOOBk+OBqO+8muS4iuOBjOOBo+OBpuOBhOOCi+eQhueUseOBqOOAgeiHquWIhuOBjOiyt+OBhOOBn+OBhOeQhueUseOCkuWIhuOBkeOBpuiAg+OBiOOBpuOBv+OCiOOBhuOBrfCfkYDinKgnLAogICAgJ+S7iuaXpeOBruOBsuOBqOOBk+OBqO+8muWIqeebiuOBruWkp+OBjeOBleOBoOOBkeOBp+OBquOBj+OAgeaQjeOBl+OBn+WgtOWQiOOBrumHkemhjeOCguimi+OBpuOBiuOBk+OBhuOAguagquaVsOOBruiqv+aVtOOCguWkp+S6i+OBp+OBmeOBnuKZpScsCiAgICAn5LuK5pel44Gu44Gy44Go44GT44Go77ya5L2V44KC44GX44Gq44GE5pel44GM44GC44Gj44Gm44KC5aSn5LiI5aSr44CC5p2h5Lu244GM5o+D44GG44G+44Gn5b6F44Gk44Gu44KC44CB6Ieq5YiG44Gn6YG444G26KGM5YuV44Gg44KI4pmlJywKICAgICfku4rml6Xjga7jgbLjgajjgZPjgajvvJrlubPlnYfjga7mnJ/lvoXlgKTjgYzpq5jjgY/jgabjgoLjgIHmr47lm57jgZ3jga7pgJrjgorjgavjga/jgarjgonjgarjgYTjgojjgILkuIvmjK/jgozjga7luYXjgoLkuIDnt5Ljgavnorroqo3jgZfjgojjgYbjga3jgIInLAogICAgJ+S7iuaXpeOBruOBsuOBqOOBk+OBqO+8mumBjuWOu+OBruWApOWLleOBjeOBq+WKoOOBiOOBpuOAgeaxuueul+OBrumWi+ekuuaXpeOBqOODh+ODvOOCv+OBrumuruW6puOCgueiuuiqjeOBl+OCiOOBhuOBreKZpScsCiAgICAn5LuK5pel44Gu44Gy44Go44GT44Go77ya5oyv44KK6L+U44KK44Gv44CM5b2T44Gf44Gj44Gf44GL44CN44Gg44GR44Gn44Gq44GP44CB44CM5rG644KB44Gf44Or44O844Or44KS5a6I44KM44Gf44GL44CN44KC6KaL44Gm44G/44KI44GG44Gt4pmlJwogIF07CiAgbGluZXMucHVzaCh0aXBzW01hdGguZmxvb3IoRGF0ZS5wYXJzZShkYXkrJ1QwMDowMDowMFonKS84NjQwMDAwMCkldGlwcy5sZW5ndGhdKTsKICByZXR1cm4ge2RheSxsaW5lc307Cn0KZnVuY3Rpb24gcmVuZGVyRGFpbHlBZHZpY2UoKXsKICBjb25zdCBib3g9JCgncmFkZW5BZHZpY2UnKTtpZighYm94KXJldHVybjsKICBsZXQgbW92ZW1lbnQ9e307dHJ5e21vdmVtZW50PUpTT04ucGFyc2UobG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfbW92ZW1lbnRfdjEnKXx8J3t9Jyl9Y2F0Y2goZSl7fQogIGNvbnN0IGFkdmljZT1idWlsZFJhZGVuQWR2aWNlKGxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpLGxvY2FsKCdmcmVlX3dhdGNoJyksbW92ZW1lbnQpOwogICQoJ3JhZGVuQWR2aWNlRGF0ZScpLnRleHRDb250ZW50PWFkdmljZS5kYXkrJ++8iOaXpeacrOaZgumWk++8ie+8j+aXpeS7mOOBqOS/neWtmOa4iOOBv+ODh+ODvOOCv+OBq+W/nOOBmOOBpuabtOaWsCc7CiAgYm94LnJlcGxhY2VDaGlsZHJlbigpOwogIGZvcihjb25zdCBsaW5lIG9mIGFkdmljZS5saW5lcyl7Y29uc3QgcD1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdwJyk7cC5zdHlsZS5saW5lSGVpZ2h0PScxLjgnO3AudGV4dENvbnRlbnQ9bGluZTtib3guYXBwZW5kQ2hpbGQocCk7fQp9CmZ1bmN0aW9uIHZhbChpZCl7bGV0IHY9JChpZCkudmFsdWUudHJpbSgpO3JldHVybiB2PT09Jyc/bnVsbDpOdW1iZXIodil9CmZ1bmN0aW9uIGxvY2FsKGspe3RyeXtyZXR1cm4gSlNPTi5wYXJzZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrKXx8J1tdJyl9Y2F0Y2goZSl7cmV0dXJuW119fQpmdW5jdGlvbiBzYXZlKGssdil7bG9jYWxTdG9yYWdlLnNldEl0ZW0oayxKU09OLnN0cmluZ2lmeSh2KSl9CmZ1bmN0aW9uIGZtdCh2LGQ9Mil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKGQpfQpmdW5jdGlvbiB5ZW4odil7cmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOifCpScrTWF0aC5yb3VuZChOdW1iZXIodikpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpfQpmdW5jdGlvbiBzdGF0ZUphKHMpewogIGlmKHM9PT0nc3Ryb25nX2J1bGxpc2gnKXJldHVybiAn5by35rCXJzsKICBpZihzPT09J2J1bGxpc2gnKXJldHVybiAn44KE44KE5by35rCXJzsKICBpZihzPT09J25ldXRyYWwnKXJldHVybiAn5Lit56uLJzsKICBpZihzPT09J2JlYXJpc2gnKXJldHVybiAn44KE44KE5byx5rCXJzsKICBpZihzPT09J3N0cm9uZ19iZWFyaXNoJylyZXR1cm4gJ+W8seawlyc7CiAgcmV0dXJuICfliKTlrprkv53nlZknOwp9CgpmdW5jdGlvbiBzdGF0ZUNsYXNzKHMpewogIGlmKHM9PT0nc3Ryb25nX2J1bGxpc2gnKXJldHVybiAnc3RhdGUtc3Ryb25nLWJ1bGwnOwogIGlmKHM9PT0nYnVsbGlzaCcpcmV0dXJuICdzdGF0ZS1idWxsJzsKICBpZihzPT09J25ldXRyYWwnKXJldHVybiAnc3RhdGUtbmV1dHJhbCc7CiAgaWYocz09PSdiZWFyaXNoJylyZXR1cm4gJ3N0YXRlLWJlYXInOwogIGlmKHM9PT0nc3Ryb25nX2JlYXJpc2gnKXJldHVybiAnc3RhdGUtc3Ryb25nLWJlYXInOwogIHJldHVybiAnc3RhdGUtbmV1dHJhbCc7Cn0KCmZ1bmN0aW9uIGZhY3RvckphKGtleSl7CiAgaWYoa2V5PT09J3RlY2huaWNhbCcpcmV0dXJuICfjg4bjgq/jg4vjgqvjg6snOwogIGlmKGtleT09PSdlYXJuaW5ncycpcmV0dXJuICfmsbrnrpcnOwogIGlmKGtleT09PSdzdXBwbHknKXJldHVybiAn6ZyA57WmcHJveHknOwogIGlmKGtleT09PSdwb2xpY3knKXJldHVybiAn5Zu9562WJzsKICByZXR1cm4ga2V5fHwn6KaB5ZugJzsKfQoKZnVuY3Rpb24gZHJpdmVyU2VudGVuY2Uoc2MpewogIGNvbnN0IHA9c2MmJnNjLnN0cm9uZ2VzdF9wb3NpdGl2ZTsKICBjb25zdCBuPXNjJiZzYy5zdHJvbmdlc3RfbmVnYXRpdmU7CiAgY29uc3Qgc2NvcmU9TnVtYmVyKHNjJiZzYy5zY29yZTEwMCk7CgogIGxldCBoZWFkPScnOwogIGlmKE51bWJlci5pc0Zpbml0ZShzY29yZSkpewogICAgaWYoc2NvcmU+PTgwKWhlYWQ9J+WPluW+l+a4iOOBv+imgeWboOOCkue3j+WQiOOBmeOCi+OBqOOAgeW8t+OBhOODl+ODqeOCueipleS+oeOBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj02NSloZWFkPSfjg5fjg6njgrnopoHlm6DjgYzlhKrli6LjgafjgIHjgoTjgoTlvLfmsJfjga7oqZXkvqHjgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49NDUpaGVhZD0n44OX44Op44K544Go44Oe44Kk44OK44K544GM5ouu5oqX44GX44CB5Lit56uL5ZyP44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTMwKWhlYWQ9J+ODnuOCpOODiuOCueimgeWboOOBruW9semfv+OBjOOChOOChOW8t+OBj+OAgeaFjumHjeWvhOOCiuOBp+OBmeOAgic7CiAgICBlbHNlIGhlYWQ9J+ODnuOCpOODiuOCueimgeWboOOBruW9semfv+OBjOWkp+OBjeOBj+OAgeW8seawl+WvhOOCiuOBp+OBmeOAgic7CiAgfQoKICBsZXQgdGFpbD1bXTsKICBpZihuKXRhaWwucHVzaCgn5pyA5aSn44Gu5oq844GX5LiL44GS6KaB5Zug44GvICcrZmFjdG9ySmEobi5rZXkpKycgJytzY29yZUxhYmVsKG4uc2NvcmUpKTsKICBpZihwKXRhaWwucHVzaCgn5pyA5aSn44Gu5oq844GX5LiK44GS6KaB5Zug44GvICcrZmFjdG9ySmEocC5rZXkpKycgJytzY29yZUxhYmVsKHAuc2NvcmUpKTsKICByZXR1cm4gaGVhZCsodGFpbC5sZW5ndGg/JyAnK3RhaWwuam9pbign44CCJykrJ+OAgic6JycpOwp9CgpmdW5jdGlvbiBkcml2ZXJCb3hIdG1sKHRpdGxlLGQsa2luZCl7CiAgaWYoIWQpcmV0dXJuIGA8ZGl2IGNsYXNzPSJkcml2ZXJib3giPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+PGI+6Kmy5b2T44Gq44GXPC9iPjwvZGl2PmA7CiAgY29uc3Qgc2lnbj1OdW1iZXIoZC5jb250cmlidXRpb24pPj0wPycrJzonJzsKICByZXR1cm4gYDxkaXYgY2xhc3M9ImRyaXZlcmJveCI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPgogICAgPGI+JHtmYWN0b3JKYShkLmtleSl9ICR7c2NvcmVMYWJlbChkLnNjb3JlKX08L2I+CiAgICA8c21hbGwgY2xhc3M9Im11dGVkIj7lho3phY3liIblvozjga7ph43jgb8gJHtkLndlaWdodF9wY3R9JSAvIOWvhOS4jiAke3NpZ259JHtOdW1iZXIoZC5jb250cmlidXRpb24pLnRvRml4ZWQoMSl9PC9zbWFsbD4KICA8L2Rpdj5gOwp9CgpmdW5jdGlvbiBjb250cmlidXRpb25DYXJkSHRtbChkKXsKICBpZighZClyZXR1cm4gJyc7CiAgY29uc3QgYz1OdW1iZXIoZC5jb250cmlidXRpb24pOwogIGNvbnN0IHNpZ249Yz4wPycrJzonJzsKICBjb25zdCBpbXBhY3Q9Yy8yOwogIGNvbnN0IGltcGFjdFNpZ249aW1wYWN0PjA/JysnOicnOwogIHJldHVybiBgPGRpdiBjbGFzcz0iY29udHJpYiI+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPiR7ZmFjdG9ySmEoZC5rZXkpfTwvc3Bhbj4KICAgIDxiPiR7c2lnbn0ke2MudG9GaXhlZCgxKX08L2I+CiAgICA8c21hbGw+5Z+65rqW6YeN44G/ICR7TnVtYmVyKGQuYmFzZV93ZWlnaHRfcGN0Pz8wKS50b0ZpeGVkKDEpfSUgw5cg6a6u5bqmICR7TnVtYmVyKGQuZnJlc2huZXNzX3BjdD8/MTAwKS50b0ZpeGVkKDApfSU8L3NtYWxsPgogICAgPHNtYWxsPuWGjemFjeWIhuW+jOmHjeOBvyAke051bWJlcihkLndlaWdodF9wY3QpLnRvRml4ZWQoMSl9JTwvc21hbGw+CiAgICA8c21hbGw+MTAw54K55o+b566X44G444Gu5b2x6Z+/ICR7aW1wYWN0U2lnbn0ke2ltcGFjdC50b0ZpeGVkKDEpfeeCuTwvc21hbGw+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gcmVuZGVyQ29udHJpYnV0aW9ucyhzYyl7CiAgY29uc3QgYm94PSQoJ2NvbnRyaWJ1dGlvbkJveCcpOwogIGNvbnN0IGdyaWQ9JCgnY29udHJpYnV0aW9uR3JpZCcpOwogIGlmKCFib3h8fCFncmlkKXJldHVybjsKICBjb25zdCBkcz0oc2MmJnNjLmRyaXZlcnMpfHxbXTsKICBpZighZHMubGVuZ3RoKXsKICAgIGJveC5zdHlsZS5kaXNwbGF5PSdub25lJzsKICAgIGdyaWQuaW5uZXJIVE1MPScnOwogICAgcmV0dXJuOwogIH0KICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogIGdyaWQuaW5uZXJIVE1MPWRzLm1hcChjb250cmlidXRpb25DYXJkSHRtbCkuam9pbignJyk7Cn0KZnVuY3Rpb24gcGN0KHYpe3JldHVybiAodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKT8n4oCUJzpOdW1iZXIodikudG9GaXhlZCgyKSsnJSd9CmZ1bmN0aW9uIHN0YXRGb3IoaCxrZXkpe3JldHVybiBoJiZoLmZvcndhcmRfc3RhdHMmJmguZm9yd2FyZF9zdGF0c1trZXldP2guZm9yd2FyZF9zdGF0c1trZXldOm51bGx9CgpmdW5jdGlvbiBkYXRhQWdlRGF5cyhkYXRlU3RyKXsKICBpZighZGF0ZVN0cilyZXR1cm4gbnVsbDsKICBjb25zdCBtPVN0cmluZyhkYXRlU3RyKS5tYXRjaCgvXihcZHs0fSktKFxkezJ9KS0oXGR7Mn0pJC8pOwogIGlmKCFtKXJldHVybiBudWxsOwogIGNvbnN0IGQ9RGF0ZS5VVEMoTnVtYmVyKG1bMV0pLE51bWJlcihtWzJdKS0xLE51bWJlcihtWzNdKSk7CiAgY29uc3Qgbm93PW5ldyBEYXRlKCk7CiAgY29uc3QgdG9kYXk9RGF0ZS5VVEMobm93LmdldEZ1bGxZZWFyKCksbm93LmdldE1vbnRoKCksbm93LmdldERhdGUoKSk7CiAgcmV0dXJuIE1hdGgubWF4KDAsTWF0aC5mbG9vcigodG9kYXktZCkvODY0MDAwMDApKTsKfQoKZnVuY3Rpb24gZnJlc2huZXNzRm9yKGRhdGVTdHIpewogIGNvbnN0IGRheXM9ZGF0YUFnZURheXMoZGF0ZVN0cik7CiAgaWYoZGF5cz09PW51bGwpewogICAgcmV0dXJuIHtsZXZlbDondW5rbm93bicsZGF5czpudWxsLGxhYmVsOifml6Xku5jkuI3mmI4nLGNsczonZnJlc2gtd2FybicsZGVjaXNpb25fb2s6ZmFsc2V9OwogIH0KICBpZihkYXlzPD00KXsKICAgIHJldHVybiB7bGV2ZWw6J2ZyZXNoJyxkYXlzLGxhYmVsOifprq7luqZPSycsY2xzOidmcmVzaC1vaycsZGVjaXNpb25fb2s6dHJ1ZX07CiAgfQogIGlmKGRheXM8PTEwKXsKICAgIHJldHVybiB7bGV2ZWw6J3dhcm5pbmcnLGRheXMsbGFiZWw6J+OChOOChOmBheW7ticsY2xzOidmcmVzaC13YXJuJyxkZWNpc2lvbl9vazp0cnVlfTsKICB9CiAgcmV0dXJuIHtsZXZlbDonc3RhbGUnLGRheXMsbGFiZWw6J+WPpOOBhOODh+ODvOOCvycsY2xzOidmcmVzaC1zdGFsZScsZGVjaXNpb25fb2s6ZmFsc2V9Owp9CgpmdW5jdGlvbiBmcmVzaG5lc3NUZXh0KGRhdGVTdHIpewogIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGRhdGVTdHIpOwogIGlmKGYuZGF5cz09PW51bGwpcmV0dXJuICfmnIDntYLjg4fjg7zjgr/ml6XjgpLnorroqo3jgafjgY3jgb7jgZvjgpPjgIInOwogIGlmKGYubGV2ZWw9PT0nZnJlc2gnKXJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpeOAgumAmuW4uOOBruWPguiAg+WIpOWumuOBq+S9v+eUqOOBl+OBvuOBmeOAgmA7CiAgaWYoZi5sZXZlbD09PSd3YXJuaW5nJylyZXR1cm4gYOacgOe1guODh+ODvOOCv+aXpeOBi+OCiSAke2YuZGF5c33ml6XjgILpgYXlu7bjgavms6jmhI/jgZfjgabjgIHlrp/pmpvjga7nj77lnKjlgKTjgoLnorroqo3jgZfjgabjgY/jgaDjgZXjgYTjgIJgOwogIHJldHVybiBg5pyA57WC44OH44O844K/5pel44GL44KJICR7Zi5kYXlzfeaXpemBheOCjOOAguS7iuaXpeOBruWjsuiyt+WIpOaWreOBq+OBr+WPpOOBhOOBn+OCgeOAgeS/neacieWIpOaWreOBr+iHquWLleOBp+S/neeVmeOBl+OBvuOBmeOAgmA7Cn0KCmZ1bmN0aW9uIHNob3dGcmVzaG5lc3MoZGF0ZVN0cil7CiAgY29uc3QgYm94PSQoJ2ZyZXNobmVzc0JveCcpOwogIGlmKCFib3gpcmV0dXJuOwogIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGRhdGVTdHIpOwogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgYm94LmNsYXNzTmFtZT0nZnJlc2hib3ggJytmLmNsczsKICAkKCdmcmVzaG5lc3NUaXRsZScpLnRleHRDb250ZW50PQogICAgZi5sZXZlbD09PSdmcmVzaCc/J+KchSDjg4fjg7zjgr/prq7luqZPSyc6CiAgICBmLmxldmVsPT09J3dhcm5pbmcnPyfimqDvuI8g44OH44O844K/6YGF5bu244Gr5rOo5oSPJzoKICAgIGYubGV2ZWw9PT0nc3RhbGUnPyfwn5uRIOODh+ODvOOCv+OBjOWPpOOBhOOBn+OCgeS7iuaXpeOBruWIpOaWreOBr+S/neeVmSc6CiAgICAn4pqg77iPIOODh+ODvOOCv+muruW6puOCkueiuuiqjeOBp+OBjeOBvuOBm+OCkyc7CiAgJCgnZnJlc2huZXNzRGV0YWlsJykudGV4dENvbnRlbnQ9ZnJlc2huZXNzVGV4dChkYXRlU3RyKTsKfQoKZnVuY3Rpb24gc2hvd0hpc3RvcnlGcmVzaG5lc3MoaGlzdG9yeURhdGUscHJpY2VEYXRlKXsKICBjb25zdCBib3g9JCgnaGlzdG9yeUZyZXNobmVzc0JveCcpOwogIGlmKCFib3gpcmV0dXJuOwogIGlmKCFoaXN0b3J5RGF0ZSl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnaGlzdG9yeUZyZXNobmVzc1RleHQnKS50ZXh0Q29udGVudD0nMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7liIbmnpDlsaXmrbTjgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ/jgILnj77lnKjlgKTjgaDjgZHooajnpLrjgZfjgb7jgZnjgIInOwogICAgcmV0dXJuOwogIH0KICBjb25zdCBoZj1mcmVzaG5lc3NGb3IoaGlzdG9yeURhdGUpOwogIGlmKGhmLmxldmVsPT09J2ZyZXNoJyl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgYm94LmNsYXNzTmFtZT0nZnJlc2hib3ggZnJlc2gtb2snOwogICAgJCgnaGlzdG9yeUZyZXNobmVzc1RleHQnKS50ZXh0Q29udGVudD0KICAgICAgYOS+oeagvOWxpeattOOCguacgOaWsOWWtualreaXpSAke2hpc3RvcnlEYXRlfSDjgb7jgaflj5blvpfjgILnn63kuK3plbfjg7vpnIDntaZwcm94eeODu+ODiOODrOODvOODquODs+OCsOOCkumAmuW4uOioiOeul+OBl+OBvuOBmeOAgmA7CiAgICByZXR1cm47CiAgfQogIGJveC5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgY29uc3QgcGQ9cHJpY2VEYXRlfHwn4oCUJzsKICAkKCdoaXN0b3J5RnJlc2huZXNzVGV4dCcpLnRleHRDb250ZW50PQogICAgYOePvuWcqOWApOODh+ODvOOCv+aXpSAke3BkfSAvIOS+oeagvOWxpeattOacgOe1guaXpSAke2hpc3RvcnlEYXRlfeOAguS+oeagvOWxpeattOOBjOWPpOOBhOWgtOWQiOOBoOOBkeOAgeODiOODrOODs+ODieODu+acn+W+heWApOODu+mcgOe1pnByb3h544KS5Y+C6ICD5YCk5omx44GE44Gr44GX44G+44GZ44CCYDsKfQoKZnVuY3Rpb24gY29uZmlkZW5jZUZvcihoKXsKICBjb25zdCB2YWxzPVtoLnJldHVybl8yMGQsaC5yZXR1cm5fMTI2ZCxoLnJldHVybl8yNTJkXS5tYXAoTnVtYmVyKS5maWx0ZXIoTnVtYmVyLmlzRmluaXRlKTsKICBjb25zdCBzdGF0cz1bJzIwZCcsJzEyNmQnLCcyNTJkJ10ubWFwKGs9PnN0YXRGb3IoaCxrKSkuZmlsdGVyKHM9PnMmJnMuc3RhdHVzPT09J29rJyk7CgogIGlmKCFOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpfHwhaC5hc29mKXJldHVybiAwOwoKICBjb25zdCBjb3ZlcmFnZT12YWxzLmxlbmd0aC8zOwogIGxldCBhZ3JlZW1lbnQ9MC41OwogIGlmKHZhbHMubGVuZ3RoKXsKICAgIGNvbnN0IHBvcz12YWxzLmZpbHRlcih4PT54PjApLmxlbmd0aDsKICAgIGNvbnN0IG5lZz12YWxzLmZpbHRlcih4PT54PDApLmxlbmd0aDsKICAgIGFncmVlbWVudD1NYXRoLm1heChwb3MsbmVnKS92YWxzLmxlbmd0aDsKICB9CiAgY29uc3Qgc3RhdENvdmVyYWdlPXN0YXRzLmxlbmd0aC8zOwogIGxldCBzY29yZT0oY292ZXJhZ2UqMC40NSArIGFncmVlbWVudCowLjI1ICsgc3RhdENvdmVyYWdlKjAuMzApKjEwMDsKCiAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKGguYXNvZik7CiAgaWYoZnJlc2gubGV2ZWw9PT0nd2FybmluZycpc2NvcmUqPTAuNjA7CiAgaWYoZnJlc2gubGV2ZWw9PT0nc3RhbGUnKXNjb3JlPU1hdGgubWluKHNjb3JlLDI1KTsKICBpZihmcmVzaC5sZXZlbD09PSd1bmtub3duJylzY29yZT1NYXRoLm1pbihzY29yZSwyMCk7CgogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgaWYoaGlzdG9yeUZyZXNoLmxldmVsPT09J3dhcm5pbmcnKXNjb3JlKj0wLjc1OwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSdzdGFsZScpc2NvcmU9TWF0aC5taW4oc2NvcmUsMzUpOwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSd1bmtub3duJylzY29yZT1NYXRoLm1pbihzY29yZSwyNSk7CgogIHJldHVybiBNYXRoLnJvdW5kKHNjb3JlKTsKfQoKZnVuY3Rpb24gZGVjaXNpb25Gb3IoaCxjKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFoLmFzb2YpewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVmScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjon5a6f44OH44O844K/5pyq5Y+W5b6X44CC5Y+z5LiK44Gu44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgZfjgabjgY/jgaDjgZXjgYTjgIInLAogICAgICBjb25maWRlbmNlOjAKICAgIH07CiAgfQoKICBjb25zdCByMjA9TnVtYmVyKGgucmV0dXJuXzIwZCksIHIxMjY9TnVtYmVyKGgucmV0dXJuXzEyNmQpLCByMjUyPU51bWJlcihoLnJldHVybl8yNTJkKTsKICBjb25zdCBjb25maWRlbmNlPWNvbmZpZGVuY2VGb3IoaCk7CiAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKGguYXNvZik7CgogIGlmKCFmcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOmAke2ZyZXNobmVzc1RleHQoaC5hc29mKX0g5pCN5YiH44KK44O75Yip56K644Op44Kk44Oz44Go44Gu5q+U6LyD44KC5Y+C6ICD5YCk5omx44GE44Gn44GZ44CCYCwKICAgICAgY29uZmlkZW5jZQogICAgfTsKICB9CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon5pCN5YiH44KK5qSc6KiOJyxjbHM6J2Qtc3RvcCcscmVhc29uOifoqK3lrprjgZfjgZ/mkI3liIfjgorlj4LogIPjg6njgqTjg7Pku6XkuIsnLGNvbmZpZGVuY2V9OwogIH0KICBpZihjdXI+PWMudGFrZVByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+WIqeeiuuaknOiojicsY2xzOidkLXRha2UnLHJlYXNvbjon6Kit5a6a44GX44Gf5Yip56K65Y+C6ICD44Op44Kk44Oz5Lul5LiKJyxjb25maWRlbmNlfTsKICB9CgogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgaWYoIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBsYWJlbDon5Yik5a6a5L+d55WZ77yI5bGl5q205Y+k44GE77yJJywKICAgICAgY2xzOidkLXdhdGNoJywKICAgICAgcmVhc29uOmDnj77lnKjlgKTjga/lj5blvpfjgafjgY3jgabjgYTjgb7jgZnjgYzjgIHkvqHmoLzlsaXmrbTjga8gJHtoLmhpc3RvcnlfYXNvZnx8J+S4jeaYjid944CC5Zu65a6a44Gu5pCN5YiH44KKL+WIqeeiuuODqeOCpOODs+OBq+OBr+acquWIsOmBlOOBp+OBmeOBjOOAgeODiOODrOODs+ODieWIpOaWreOBr+S/neeVmeOBl+OBvuOBmeOAgmAsCiAgICAgIGNvbmZpZGVuY2UKICAgIH07CiAgfQoKICBpZihjdXI8PWMudHJhaWxQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpemrmOWApOWfuua6luOBruODiOODrOODvOODquODs+OCsOWPguiAg+ODqeOCpOODs+S7peS4iycsY29uZmlkZW5jZX07CiAgfQoKICBsZXQgcG9zaXRpdmU9MCwgbmVnYXRpdmU9MDsKICBbcjIwLHIxMjYscjI1Ml0uZm9yRWFjaCh4PT57CiAgICBpZihOdW1iZXIuaXNGaW5pdGUoeCkpewogICAgICBpZih4PjApcG9zaXRpdmUrKzsKICAgICAgaWYoeDwwKW5lZ2F0aXZlKys7CiAgICB9CiAgfSk7CgogIGlmKG5lZ2F0aXZlPj0yKXsKICAgIHJldHVybiB7bGFiZWw6J+itpuaIkicsY2xzOidkLXdhdGNoJyxyZWFzb246JzIw5pel44O7MTI25pel44O7MjUy5pel44Gu44GG44Gh44Oe44Kk44OK44K55YK+5ZCR44GM5YSq5YuiJyxjb25maWRlbmNlfTsKICB9CiAgaWYocG9zaXRpdmU+PTIpewogICAgcmV0dXJuIHtsYWJlbDon5L+d5pyJ57aZ57aaJyxjbHM6J2QtaG9sZCcscmVhc29uOifoqK3lrprjg6njgqTjg7PlhoXjgafjgIHopIfmlbDmnJ/plpPjga7kvqHmoLzjg4jjg6zjg7Pjg4njgYzjg5fjg6njgrknLGNvbmZpZGVuY2V9OwogIH0KICByZXR1cm4ge2xhYmVsOifkv53mnInntpnntprvvIjmp5jlrZDopovvvIknLGNsczonZC1ob2xkJyxyZWFzb246J+ioreWumuODqeOCpOODs+WGheOAguacn+mWk+WIpeODiOODrOODs+ODieOBr+W8t+W8seOBjOa3t+WcqCcsY29uZmlkZW5jZX07Cn0KZnVuY3Rpb24gZXZIdG1sKHRpdGxlLHMpewogIGlmKCFzfHxzLnN0YXR1cyE9PSdvaycpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c21hbGw+5qiZ5pysICR7bn3ku7Y8L3NtYWxsPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0iZXYiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj4KICAgIDxiPuW5s+WdhyAke3BjdChzLm1lYW4pfTwvYj4KICAgIDxzbWFsbD7kuK3lpK7lgKQgJHtwY3Qocy5tZWRpYW4pfTwvc21hbGw+CiAgICA8c21hbGw+5LiK5piH546HICR7cGN0KHMucG9zaXRpdmVfcmF0ZSl9PC9zbWFsbD4KICAgIDxzbWFsbD5QMTDjgJxQOTAgJHtwY3Qocy5wMTApfSDjgJwgJHtwY3Qocy5wOTApfTwvc21hbGw+CiAgICA8c21hbGw+5qiZ5pysICR7cy5ufeS7tjwvc21hbGw+CiAgPC9kaXY+YDsKfQoKCmZ1bmN0aW9uIGRpc3RhbmNlSW5mbyhjdXIsdGFyZ2V0LGtpbmQpewogIGN1cj1OdW1iZXIoY3VyKTsgdGFyZ2V0PU51bWJlcih0YXJnZXQpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhTnVtYmVyLmlzRmluaXRlKHRhcmdldCkpcmV0dXJuICfigJQnOwogIGNvbnN0IGRpZmY9KHRhcmdldC9jdXItMSkqMTAwOwogIGlmKGtpbmQ9PT0nc3RvcCcpewogICAgaWYoZGlmZj49MClyZXR1cm4gJ+ODqeOCpOODs+WIsOmBlOa4iOOBvyc7CiAgICByZXR1cm4gTWF0aC5hYnMoZGlmZikudG9GaXhlZCgyKSsnJSDkuIsnOwogIH0KICBpZihraW5kPT09J3Rha2UnKXsKICAgIGlmKGRpZmY8PTApcmV0dXJuICfjg6njgqTjg7PliLDpgZTmuIjjgb8nOwogICAgcmV0dXJuIGRpZmYudG9GaXhlZCgyKSsnJSDkuIonOwogIH0KICByZXR1cm4gKGRpZmY+PTA/JysnOicnKStkaWZmLnRvRml4ZWQoMikrJyUnOwp9CgpmdW5jdGlvbiBwcmljZVJhbmdlKGN1cixzKXsKICBjdXI9TnVtYmVyKGN1cik7CiAgaWYoIU51bWJlci5pc0Zpbml0ZShjdXIpfHxjdXI8PTB8fCFzfHxzLnN0YXR1cyE9PSdvaycpcmV0dXJuIG51bGw7CiAgcmV0dXJuIHsKICAgIGxvdzpjdXIqKDErTnVtYmVyKHMucDEwKS8xMDApLAogICAgaGlnaDpjdXIqKDErTnVtYmVyKHMucDkwKS8xMDApLAogICAgbWVkaWFuOmN1ciooMStOdW1iZXIocy5tZWRpYW4pLzEwMCkKICB9Owp9CgpmdW5jdGlvbiByYW5nZUh0bWwodGl0bGUsY3VyLHMpewogIGNvbnN0IHI9cHJpY2VSYW5nZShjdXIscyk7CiAgaWYoIXIpewogICAgY29uc3Qgbj1zJiZzLm4hPT11bmRlZmluZWQ/cy5uOjA7CiAgICByZXR1cm4gYDxkaXYgY2xhc3M9InJhbmdlYm94Ij48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7dGl0bGV9PC9zcGFuPjxiPuODh+ODvOOCv+S4jei2szwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaomeacrCAke2595Lu2PC9zcGFuPjwvZGl2PmA7CiAgfQogIHJldHVybiBgPGRpdiBjbGFzcz0icmFuZ2Vib3giPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfSBQMTDjgJxQOTA8L3NwYW4+CiAgICA8Yj4ke3llbihyLmxvdyl9IOOAnCAke3llbihyLmhpZ2gpfTwvYj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+5Lit5aSu5YCk5o+b566XICR7eWVuKHIubWVkaWFuKX08L3NwYW4+CiAgPC9kaXY+YDsKfQoKZnVuY3Rpb24gYWN0aW9uVGV4dChoLGMsZCl7CiAgaWYoZC5sYWJlbD09PSfliKTlrprkv53nlZknfHxkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOagquS+oeWPpOOBhO+8iScpcmV0dXJuICfjg4fjg7zjgr/jgYzlj6TjgYTjgZ/jgoHku4rml6Xjga7liKTmlq3jga/kv53nlZnjgILoqLzliLjkvJrnpL7jgarjganjgaflrp/pmpvjga7nj77lnKjlgKTjgpLnorroqo3jgZfjgabjgYvjgonliKTmlq3jgIInOwogIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5bGl5q205Y+k44GE77yJJylyZXR1cm4gJ+ePvuWcqOWApOOBr+eiuuiqjea4iOOBv+OAguWbuuWumuOBruaQjeWIh+OCii/liKnnorrjg6njgqTjg7PjgaDjgZHnorroqo3jgZfjgIHjg4jjg6zjg7Pjg4nliKTmlq3jga/kvqHmoLzlsaXmrbTmm7TmlrDjgb7jgafkv53nlZnjgIInOwogIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylyZXR1cm4gJ+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs+OCkuS4i+WbnuOBo+OBpuOBhOOBvuOBmeOAguWun+mam+OBruePvuWcqOWApOOBqOazqOaWh+adoeS7tuOCkueiuuiqjeOBl+OBpuOAgee4ruWwj+ODu+aSpOmAgOOCkuaknOiojuOAgic7CiAgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKXJldHVybiAn5Yip56K65Y+C6ICD44Op44Kk44Oz44Gr5Yiw6YGU44GX44Gm44GE44G+44GZ44CC5YWo6YOo5aOy5Y2044Gg44GR44Gn44Gq44GP44CB5YiG5Ymy5Yip56K644KC5YCZ6KOc44CCJzsKICBpZihkLmxhYmVsPT09J+itpuaIkicpcmV0dXJuICforabmiJLjgr7jg7zjg7PjgILjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PjgajkuK3nn63mnJ/jga7lgKTli5XjgY3jgpLlhKrlhYjjgZfjgabnorroqo3jgIInOwogIHJldHVybiAn6Kit5a6a44Op44Kk44Oz5YaF44CC5L+d5pyJ57aZ57aa5YCZ6KOc44Gn44GZ44GM44CB54Sh5paZ44OH44O844K/44Gv6YGF5bu244GZ44KL44Gf44KB5a6f6Zqb44Gu54++5Zyo5YCk44KC56K66KqN44CCJzsKfQoKCmZ1bmN0aW9uIG5vbXVyYU5ldEZlZShhbW91bnQpewogIGFtb3VudD1OdW1iZXIoYW1vdW50fHwwKTsKICBpZihhbW91bnQ8PTApcmV0dXJuIDA7CiAgaWYoYW1vdW50PD0xMDAwMDApcmV0dXJuIDE1MjsKICBpZihhbW91bnQ8PTMwMDAwMClyZXR1cm4gMzMwOwogIGlmKGFtb3VudDw9NTAwMDAwKXJldHVybiA1MjQ7CiAgaWYoYW1vdW50PD0xMDAwMDAwKXJldHVybiAxMDQ4OwogIGlmKGFtb3VudDw9MjAwMDAwMClyZXR1cm4gMjA5NTsKICBpZihhbW91bnQ8PTMwMDAwMDApcmV0dXJuIDMxNDM7CiAgaWYoYW1vdW50PD01MDAwMDAwKXJldHVybiA1MjM4OwogIGlmKGFtb3VudDw9MTAwMDAwMDApcmV0dXJuIDEwNDc2OwogIGlmKGFtb3VudDw9MjAwMDAwMDApcmV0dXJuIDIwOTUyOwogIGlmKGFtb3VudDw9MzAwMDAwMDApcmV0dXJuIDMxNDI5OwogIGlmKGFtb3VudDw9NTAwMDAwMDApcmV0dXJuIDQxOTA1OwogIHJldHVybiA3ODU3MTsKfQpmdW5jdGlvbiBmZWVGb3IoYW1vdW50LG1vZGUpe3JldHVybiBtb2RlPT09J25vbXVyYV9uZXQnP25vbXVyYU5ldEZlZShhbW91bnQpOjB9CgpmdW5jdGlvbiBjYWxjSG9sZGluZyhoKXsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgY29zdD1OdW1iZXIoaC5jb3N0KTsKICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzKTsKICBjb25zdCBidXlWYWx1ZT1jb3N0KnNoYXJlczsKICBjb25zdCBidXlGZWU9ZmVlRm9yKGJ1eVZhbHVlLGguZmVlX21vZGUpOwogIGNvbnN0IGN1cnJlbnRWYWx1ZT1jdXIqc2hhcmVzOwogIGNvbnN0IHNlbGxGZWU9ZmVlRm9yKGN1cnJlbnRWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBpbnZlc3RlZD1idXlWYWx1ZStidXlGZWU7CiAgY29uc3QgbmV0Tm93PWN1cnJlbnRWYWx1ZS1zZWxsRmVlLWludmVzdGVkOwogIGNvbnN0IG5ldE5vd1BjdD1pbnZlc3RlZD9uZXROb3cvaW52ZXN0ZWQqMTAwOm51bGw7CgogIGNvbnN0IHN0b3BQcmljZT1jb3N0KigxLU51bWJlcihoLnN0b3BfcGN0KS8xMDApOwogIGNvbnN0IHRha2VQcmljZT1jb3N0KigxK051bWJlcihoLnRha2VfcGN0KS8xMDApOwogIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3IoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZik7CiAgY29uc3QgaGlnaDIwPU51bWJlcihoLmhpZ2hfMjBkfHxjdXIpOwogIGNvbnN0IHRyYWlsUHJpY2U9aGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rCiAgICA/IGhpZ2gyMCooMS1OdW1iZXIoaC50cmFpbF9wY3QpLzEwMCkKICAgIDogbnVsbDsKCiAgY29uc3Qgc3RvcFZhbHVlPXN0b3BQcmljZSpzaGFyZXM7CiAgY29uc3QgdGFrZVZhbHVlPXRha2VQcmljZSpzaGFyZXM7CiAgY29uc3Qgc3RvcE5ldD1zdG9wVmFsdWUtZmVlRm9yKHN0b3BWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKICBjb25zdCB0YWtlTmV0PXRha2VWYWx1ZS1mZWVGb3IodGFrZVZhbHVlLGguZmVlX21vZGUpLWludmVzdGVkOwoKICByZXR1cm4ge2J1eVZhbHVlLGJ1eUZlZSxjdXJyZW50VmFsdWUsc2VsbEZlZSxpbnZlc3RlZCxuZXROb3csbmV0Tm93UGN0LHN0b3BQcmljZSx0YWtlUHJpY2UsdHJhaWxQcmljZSxzdG9wTmV0LHRha2VOZXR9Owp9CgoKY29uc3QgbmFtZVRpbWVycz17fTsKY29uc3QgbmFtZUNhY2hlPXt9OwoKZnVuY3Rpb24gZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4PScnKXsKICBpZighdGFyZ2V0KXJldHVybjsKICBpZighaW5mb3x8IWluZm8ubmFtZSl7CiAgICB0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nOwogICAgcmV0dXJuOwogIH0KICBsZXQgZXh0cmFzPVtdOwogIGlmKGluZm8ubWFya2V0KWV4dHJhcy5wdXNoKGluZm8ubWFya2V0KTsKICBpZihpbmZvLnNlY3RvcjMzKWV4dHJhcy5wdXNoKGluZm8uc2VjdG9yMzMpOwogIHRhcmdldC5pbm5lckhUTUw9JzxiPicrcHJlZml4K2luZm8ubmFtZSsnPC9iPicrKGV4dHJhcy5sZW5ndGg/Jzxicj48c3BhbiBjbGFzcz0ibXV0ZWQiPicrZXh0cmFzLmpvaW4oJyAvICcpKyc8L3NwYW4+JzonJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldENvbXBhbnkoY29kZSl7CiAgY29uc3QgYz1TdHJpbmcoY29kZXx8JycpLnRyaW0oKTsKICBpZighYylyZXR1cm4gbnVsbDsKICBpZihuYW1lQ2FjaGVbY10pcmV0dXJuIG5hbWVDYWNoZVtjXTsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9zZWN1cml0eT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGMpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn6YqY5p+E5ZCN44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgbmFtZUNhY2hlW2NdPXg7CiAgcmV0dXJuIHg7Cn0KCmZ1bmN0aW9uIHNjaGVkdWxlQ29tcGFueUxvb2t1cChpbnB1dElkLHRhcmdldElkLHByZWZpeD0nJyl7CiAgY2xlYXJUaW1lb3V0KG5hbWVUaW1lcnNbaW5wdXRJZF0pOwogIGNvbnN0IGM9JChpbnB1dElkKS52YWx1ZS50cmltKCk7CiAgY29uc3QgdGFyZ2V0PSQodGFyZ2V0SWQpOwoKICBpZihjLmxlbmd0aDw0KXsKICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9cHJlZml4Kyfpipjmn4TlkI3vvJrigJQnOwogICAgcmV0dXJuOwogIH0KCiAgbmFtZVRpbWVyc1tpbnB1dElkXT1zZXRUaW1lb3V0KGFzeW5jKCk9PnsKICAgIHRyeXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD0n6YqY5p+E5ZCN44KS56K66KqN5Lit4oCmJzsKICAgICAgY29uc3QgaW5mbz1hd2FpdCBnZXRDb21wYW55KGMpOwogICAgICBkaXNwbGF5Q29tcGFueSh0YXJnZXQsaW5mbyxwcmVmaXgpOwogICAgfWNhdGNoKGUpewogICAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIH0KICB9LDQ1MCk7Cn0KCgoKZnVuY3Rpb24gcHJpb3JpdHlBY3Rpb25Gb3IoaCl7CiAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgY29uc3QgbmFtZT1oLmNvbXBhbnlfbmFtZXx8Jyc7CiAgY29uc3QgbGFiZWw9KGguY29kZXx8JycpKyhuYW1lPycgJytuYW1lOicnKTsKICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CgogIGlmKCF2YWxpZCl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NiwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+Wun+ODh+ODvOOCv+OCkuabtOaWsCcsCiAgICAgIGRldGFpbDon5pyA5paw5Y+W5b6X57WC5YCk44GM44GC44KK44G+44Gb44KT44CC44G+44Ga44CM5pu05paw44CN44GnSi1RdWFudHPjg4fjg7zjgr/jgpLlj5blvpfjgIInLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogIGlmKCFmcmVzaC5kZWNpc2lvbl9vayl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5OSwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+ODh+ODvOOCv+muruW6puOCkueiuuiqjScsCiAgICAgIGRldGFpbDpgJHtmcmVzaG5lc3NUZXh0KGguYXNvZil9IOWun+mam+OBruePvuWcqOWApOOCkuWFiOOBq+eiuuiqjeOAgmAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3Qgc3RvcERpc3Q9KGN1ci9jLnN0b3BQcmljZS0xKSoxMDA7CiAgY29uc3QgdGFrZURpc3Q9KGMudGFrZVByaWNlL2N1ci0xKSoxMDA7CiAgY29uc3QgdHJhaWxEaXN0PShjdXIvYy50cmFpbFByaWNlLTEpKjEwMDsKCiAgaWYoY3VyPD1jLnN0b3BQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZToxMDAsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOaQjeWIh+OCiuWPguiAgyAke3llbihjLnN0b3BQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihzdG9wRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5NCwKICAgICAgY2xzOidwcmlvcml0eS1oaWdoJywKICAgICAgdGl0bGU6J+aQjeWIh+OCiuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7c3RvcERpc3QudG9GaXhlZCgyKX0lIOOBp+aQjeWIh+OCiuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo5MCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+WIsOmBlCcsCiAgICAgIGRldGFpbDpg5pyA5paw5Y+W5b6X57WC5YCkICR7eWVuKGN1cil9IC8g5Yip56K65Y+C6ICDICR7eWVuKGMudGFrZVByaWNlKX1gLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKHRha2VEaXN0PD0zKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjg0LAogICAgICBjbHM6J3ByaW9yaXR5LXRha2UnLAogICAgICB0aXRsZTon5Yip56K644Op44Kk44Oz5o6l6L+RJywKICAgICAgZGV0YWlsOmDjgYLjgaggJHt0YWtlRGlzdC50b0ZpeGVkKDIpfSUg44Gn5Yip56K65Y+C6ICD44Op44Kk44OzYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGlmKCFoaXN0b3J5RnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6NzgsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+S+oeagvOWxpeattOOCkuabtOaWsOW+heOBoScsCiAgICAgIGRldGFpbDpg54++5Zyo5YCkICR7aC5hc29mfHwn4oCUJ30gLyDkvqHmoLzlsaXmrbQgJHtoLmhpc3RvcnlfYXNvZnx8J+KAlCd944CC5Zu65a6a5L6h5qC844Op44Kk44Oz5Lul5aSW44Gu5Yik5pat44Gv5L+d55WZ44CCYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihkLmxhYmVsPT09J+itpuaIkicpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6ODAsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+itpuaIkuWIpOWumicsCiAgICAgIGRldGFpbDpkLnJlYXNvbiwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZihOdW1iZXIuaXNGaW5pdGUodHJhaWxEaXN0KSYmdHJhaWxEaXN0PD0yLjUpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6NzYsCiAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgdGl0bGU6J+ODiOODrOODvOODquODs+OCsOODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44OI44Os44O844Oq44Oz44Kw5Y+C6ICDICR7eWVuKGMudHJhaWxQcmljZSl9IOOBvuOBpyAke3RyYWlsRGlzdC50b0ZpeGVkKDIpfSVgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIHJldHVybiB7CiAgICBzY29yZTozMCwKICAgIGNsczoncHJpb3JpdHktZ29vZCcsCiAgICB0aXRsZTon6YCa5bi455uj6KaWJywKICAgIGRldGFpbDpkLnJlYXNvbnx8J+ioreWumuODqeOCpOODs+WGhScsCiAgICBsYWJlbAogIH07Cn0KCmZ1bmN0aW9uIHJlbmRlclByaW9yaXR5QWN0aW9ucygpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3QgYm94PSQoJ3ByaW9yaXR5QWN0aW9ucycpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajjgIHlhKrlhYjjgZfjgabnorroqo3jgZnjgovpipjmn4TjgpLoh6rli5XooajnpLrjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJldHVybjsKICB9CgogIGxldCBhY3Rpb25zPWEubWFwKHByaW9yaXR5QWN0aW9uRm9yKTsKCiAgLy8gQ29uY2VudHJhdGlvbiBhbGVydCAocG9ydGZvbGlvLWxldmVsKQogIGNvbnN0IHZhbGlkPWEuZmlsdGVyKGg9Pk51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZik7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw+MCl7CiAgICBsZXQgbWF4SG9sZGluZz1udWxsLCBtYXhWYWx1ZT0wOwogICAgdmFsaWQuZm9yRWFjaChoPT57CiAgICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgICAgaWYodj5tYXhWYWx1ZSl7bWF4VmFsdWU9djttYXhIb2xkaW5nPWh9CiAgICB9KTsKICAgIGNvbnN0IGNvbmNlbnRyYXRpb249bWF4VmFsdWUvdG90YWwqMTAwOwogICAgaWYoY29uY2VudHJhdGlvbj49NjAgJiYgbWF4SG9sZGluZyl7CiAgICAgIGFjdGlvbnMucHVzaCh7CiAgICAgICAgc2NvcmU6NzIsCiAgICAgICAgY2xzOidwcmlvcml0eS1taWQnLAogICAgICAgIHRpdGxlOifpm4bkuK3luqbjgpLnorroqo0nLAogICAgICAgIGRldGFpbDpgJHttYXhIb2xkaW5nLmNvZGV9JHttYXhIb2xkaW5nLmNvbXBhbnlfbmFtZT8nICcrbWF4SG9sZGluZy5jb21wYW55X25hbWU6Jyd9IOOBjOODneODvOODiOODleOCqeODquOCquOBriAke2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKX0lYCwKICAgICAgICBsYWJlbDon44Od44O844OI44OV44Kp44Oq44KqJwogICAgICB9KTsKICAgIH0KICB9CgogIGFjdGlvbnMuc29ydCgoeCx5KT0+eS5zY29yZS14LnNjb3JlKTsKCiAgY29uc3QgaW1wb3J0YW50PWFjdGlvbnMuZmlsdGVyKHg9Pnguc2NvcmU+PTcwKTsKICBjb25zdCBzaG93bj0oaW1wb3J0YW50Lmxlbmd0aD9pbXBvcnRhbnQ6YWN0aW9ucykuc2xpY2UoMCw0KTsKCiAgYm94LmlubmVySFRNTD1gPGRpdiBjbGFzcz0icHJpb3JpdHktd3JhcCI+JHsKICAgIHNob3duLm1hcCgoeCxpKT0+YAogICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1pdGVtICR7eC5jbHN9Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1saW5lIj4KICAgICAgICAgIDxkaXY+CiAgICAgICAgICAgIDxzcGFuIGNsYXNzPSJwcmlvcml0eS1yYW5rIj5QUklPUklUWSAke2krMX08L3NwYW4+CiAgICAgICAgICAgIDxiPiR7eC50aXRsZX08L2I+CiAgICAgICAgICA8L2Rpdj4KICAgICAgICAgIDxkaXYgY2xhc3M9InByaW9yaXR5LWNvZGUiPiR7eC5sYWJlbH08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NXB4Ij4ke3guZGV0YWlsfTwvZGl2PgogICAgICA8L2Rpdj4KICAgIGApLmpvaW4oJycpCiAgfTwvZGl2PmAgKyAoCiAgICBpbXBvcnRhbnQubGVuZ3RoCiAgICAgID8gJzxwIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij7igLvlhKrlhYjluqbjga/oqK3lrprjg6njgqTjg7PmjqXov5Hjg7vliKTlrprnirbmhYvjg7vjg4fjg7zjgr/mnInnhKHjg7vpm4bkuK3luqbjgYvjgonkvZzjgovnorroqo3poIbjgafjgZnjgILoh6rli5Xlo7LosrfmjIfnpLrjgafjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+JwogICAgICA6ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+57eK5oCl5bqm44Gu6auY44GE6aCF55uu44Gv44GC44KK44G+44Gb44KT44CC6YCa5bi455uj6KaW44KS57aZ57aa44CCPC9wPicKICApOwp9CgpmdW5jdGlvbiBwb3J0Zm9saW9NZWFuRm9yKGEsa2V5KXsKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wKTsKICBjb25zdCB0b3RhbD12YWxpZC5yZWR1Y2UoKHMsaCk9PnMrTnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKSwwKTsKICBpZih0b3RhbDw9MClyZXR1cm4gbnVsbDsKCiAgbGV0IG51bT0wLCBkZW49MDsKICB2YWxpZC5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IHN0PXN0YXRGb3IoaCxrZXkpOwogICAgY29uc3Qgdj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApOwogICAgaWYoc3QmJnN0LnN0YXR1cz09PSdvaycmJk51bWJlci5pc0Zpbml0ZShOdW1iZXIoc3QubWVhbikpJiZ2PjApewogICAgICBudW0gKz0gdipOdW1iZXIoc3QubWVhbik7CiAgICAgIGRlbiArPSB2OwogICAgfQogIH0pOwogIHJldHVybiBkZW4+MD9udW0vZGVuOm51bGw7Cn0KCmZ1bmN0aW9uIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwb3J0Zm9saW9TdW1tYXJ5Jyk7CiAgaWYoIWJveClyZXR1cm47CgogIGlmKCFhLmxlbmd0aCl7CiAgICBib3guaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOiHquWLlembhuioiOOBl+OBvuOBmeOAgjwvcD4nOwogICAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7CiAgICByZXR1cm47CiAgfQoKICBsZXQgdG90YWxDb3N0PTAsIHRvdGFsVmFsdWU9MCwgdG90YWxOZXQ9MDsKICBjb25zdCByb3dzPVtdOwogIGNvbnN0IGRlY2lzaW9ucz17aG9sZDowLHdhdGNoOjAsdGFrZTowLHN0b3A6MCxwZW5kaW5nOjB9OwoKICBhLmZvckVhY2goaD0+ewogICAgY29uc3QgYz1jYWxjSG9sZGluZyhoKTsKICAgIGNvbnN0IGN1cj1OdW1iZXIoaC5jdXJyZW50X3ByaWNlKTsKICAgIGNvbnN0IHNoYXJlcz1OdW1iZXIoaC5zaGFyZXN8fDApOwogICAgY29uc3QgdmFsaWQ9TnVtYmVyLmlzRmluaXRlKGN1cikmJmN1cj4wJiZoLmFzb2Y7CiAgICBjb25zdCBjdXJyZW50VmFsdWU9dmFsaWQ/Y3VyKnNoYXJlczowOwogICAgY29uc3QgaW52ZXN0ZWQ9TnVtYmVyKGguY29zdHx8MCkqc2hhcmVzK2MuYnV5RmVlOwoKICAgIHRvdGFsQ29zdCArPSBpbnZlc3RlZDsKCiAgICBpZih2YWxpZCl7CiAgICAgIHRvdGFsVmFsdWUgKz0gY3VycmVudFZhbHVlOwogICAgICB0b3RhbE5ldCArPSBjLm5ldE5vdzsKCiAgICAgIGNvbnN0IGQ9ZGVjaXNpb25Gb3IoaCxjKTsKICAgICAgaWYoZC5sYWJlbD09PSfmkI3liIfjgormpJzoqI4nKWRlY2lzaW9ucy5zdG9wKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSfliKnnorrmpJzoqI4nKWRlY2lzaW9ucy50YWtlKys7CiAgICAgIGVsc2UgaWYoZC5sYWJlbD09PSforabmiJInKWRlY2lzaW9ucy53YXRjaCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjmoKrkvqHlj6TjgYTvvIknfHxkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOWxpeattOWPpOOBhO+8iScpZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgZWxzZSBkZWNpc2lvbnMuaG9sZCsrOwoKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTpjdXJyZW50VmFsdWUsbmV0OmMubmV0Tm93fSk7CiAgICB9ZWxzZXsKICAgICAgZGVjaXNpb25zLnBlbmRpbmcrKzsKICAgICAgcm93cy5wdXNoKHtjb2RlOmguY29kZSxuYW1lOmguY29tcGFueV9uYW1lfHwnJyx2YWx1ZTowLG5ldDpudWxsfSk7CiAgICB9CiAgfSk7CgogIGNvbnN0IG5ldFBjdD10b3RhbENvc3Q+MD90b3RhbE5ldC90b3RhbENvc3QqMTAwOm51bGw7CiAgY29uc3QgbWF4VmFsdWU9cm93cy5yZWR1Y2UoKG0scik9Pk1hdGgubWF4KG0sci52YWx1ZSksMCk7CiAgY29uc3QgY29uY2VudHJhdGlvbj10b3RhbFZhbHVlPjA/bWF4VmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKCiAgY29uc3QgbWVhbjIwPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjBkJyk7CiAgY29uc3QgbWVhbjEyNj1wb3J0Zm9saW9NZWFuRm9yKGEsJzEyNmQnKTsKICBjb25zdCBtZWFuMjUyPXBvcnRmb2xpb01lYW5Gb3IoYSwnMjUyZCcpOwoKICBjb25zdCBzdGFsZUhvbGRpbmdzPWEuZmlsdGVyKGg9PnsKICAgIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGguYXNvZik7CiAgICByZXR1cm4gaC5hc29mICYmICFmLmRlY2lzaW9uX29rOwogIH0pOwogIGNvbnN0IHN0YWxlTm90aWNlPXN0YWxlSG9sZGluZ3MubGVuZ3RoCiAgICA/IGA8ZGl2IGNsYXNzPSJmcmVzaGJveCBmcmVzaC1zdGFsZSIgc3R5bGU9Im1hcmdpbi1ib3R0b206OXB4Ij48Yj7wn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvyAke3N0YWxlSG9sZGluZ3MubGVuZ3RofemKmOafhDwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+6KmV5L6h6aGN44O75pCN55uK44Gv5pyA5paw5Y+W5b6X57WC5YCk44OZ44O844K544Gu5Y+C6ICD5YCk44Gn44GZ44CC5LuK5pel44Gu5aOy6LK35Yik5pat44Gr44Gv5L2/44KP44Ga44CB5a6f6Zqb44Gu54++5Zyo5YCk44KS56K66KqN44GX44Gm44GP44Gg44GV44GE44CCPC9kaXY+PC9kaXY+YAogICAgOiAnJzsKCiAgY29uc3Qgc3RhbGVIaXN0b3J5PWEuZmlsdGVyKGg9PnsKICAgIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogICAgcmV0dXJuIChoLmhpc3RvcnlfYXNvZnx8aC5hc29mKSAmJiAhZi5kZWNpc2lvbl9vazsKICB9KTsKICBjb25zdCBoaXN0b3J5Tm90aWNlPXN0YWxlSGlzdG9yeS5sZW5ndGgKICAgID8gYDxkaXYgY2xhc3M9Imhpc3Rvcnl3YXJuIiBzdHlsZT0ibWFyZ2luLWJvdHRvbTo5cHgiPjxiPvCfk5og5L6h5qC85bGl5q2044GM5Y+k44GEICR7c3RhbGVIaXN0b3J5Lmxlbmd0aH3pipjmn4Q8L2I+PGRpdiBjbGFzcz0ibXV0ZWQiPuS+oeagvOWxpeattOOBjOWPpOOBhOWgtOWQiOOAgeefreS4remVt+acn+ODiOODrOODs+ODieODu+acn+W+heWApOODu+mcgOe1pnByb3h544Gv5Y+C6ICD5YCk44Gn44GZ44CCPC9kaXY+PC9kaXY+YAogICAgOiAnJzsKCiAgY29uc3QgYWxsb2NhdGlvbnM9cm93cwogICAgLmZpbHRlcihyPT5yLnZhbHVlPjApCiAgICAuc29ydCgoeCx5KT0+eS52YWx1ZS14LnZhbHVlKQogICAgLm1hcChyPT57CiAgICAgIGNvbnN0IHc9dG90YWxWYWx1ZT4wP3IudmFsdWUvdG90YWxWYWx1ZSoxMDA6MDsKICAgICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJhbGxvYyI+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPjxiPiR7ci5jb2RlfTwvYj4ke3IubmFtZT8nICcrci5uYW1lOicnfSAvICR7dy50b0ZpeGVkKDEpfSU8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJhbGxvY2JhciI+PHNwYW4gc3R5bGU9IndpZHRoOiR7TWF0aC5taW4oMTAwLHcpfSUiPjwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+YDsKICAgIH0pLmpvaW4oJycpOwoKICBib3guaW5uZXJIVE1MPWAKICAgICR7c3RhbGVOb3RpY2V9CiAgICAke2hpc3RvcnlOb3RpY2V9CiAgICA8ZGl2IGNsYXNzPSJwb3J0cm93Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+57eP5oqV6LOH6aGNPC9zcGFuPjxiPiR7eWVuKHRvdGFsQ29zdCl9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnoqZXkvqHpoY08L3NwYW4+PGI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsVmFsdWUpOifigJQnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+W5b6X57WC5YCk44OZ44O844K55pCN55uKPC9zcGFuPjxiIGNsYXNzPSIke3RvdGFsTmV0Pj0wPydwb3MnOiduZWcnfSI+JHt0b3RhbFZhbHVlPjA/eWVuKHRvdGFsTmV0KTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke25ldFBjdD09PW51bGw/J+KAlCc6bmV0UGN0LnRvRml4ZWQoMikrJyUnfTwvc3Bhbj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pyA5aSn6YqY5p+E5q+U546HPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP2NvbmNlbnRyYXRpb24udG9GaXhlZCgxKSsnJSc6J+KAlCd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGRpdiBjbGFzcz0icG9ydHJvdyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5L+d5pyJ57aZ57aaPC9zcGFuPjxiPiR7ZGVjaXNpb25zLmhvbGR9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7orabmiJI8L3NwYW4+PGI+JHtkZWNpc2lvbnMud2F0Y2h9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrmpJzoqI48L3NwYW4+PGI+JHtkZWNpc2lvbnMudGFrZX08L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCii/kv53nlZk8L3NwYW4+PGI+JHtkZWNpc2lvbnMuc3RvcCtkZWNpc2lvbnMucGVuZGluZ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn5OKIOipleS+oemhjeWKoOmHjeOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODszwvaDQ+CiAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7nn63mnJ8yMOaXpTwvc3Bhbj48Yj4ke21lYW4yMD09PW51bGw/J+KAlCc6bWVhbjIwLnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS4reacnzEyNuaXpTwvc3Bhbj48Yj4ke21lYW4xMjY9PT1udWxsPyfigJQnOm1lYW4xMjYudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6ZW35pyfMjUy5pelPC9zcGFuPjxiPiR7bWVhbjI1Mj09PW51bGw/J+KAlCc6bWVhbjI1Mi50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICA8L2Rpdj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+4oC75ZCE6YqY5p+E44Gu6YGO5Y675bmz5Z2H44Oq44K/44O844Oz44KS54++5Zyo44Gu6KmV5L6h6aGN44Gn5Yqg6YeN44GX44Gf5Y+C6ICD5YCk44Gn44GZ44CC55u46Zai44KS6ICD5oWu44GX44Gf5bCG5p2l5LqI5ris44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgoKICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA1cHgiPvCfk6Yg6YqY5p+E5qeL5oiQPC9oND4KICAgICR7YWxsb2NhdGlvbnN8fCc8cCBjbGFzcz0ibXV0ZWQiPuWun+ODh+ODvOOCv+acquWPluW+lzwvcD4nfQogIGA7CiAgcmVuZGVyUHJpb3JpdHlBY3Rpb25zKCk7Cn0KZnVuY3Rpb24gcmVuZGVySG9sZGluZ3MoKXsKICByZW5kZXJEYWlseUFkdmljZSgpOwogIGlmKCQoJ21vdmVtZW50VXAnKSlyZW5kZXJNb3ZlbWVudCgpOwogIGNvbnN0IG9wZW5lZD1uZXcgU2V0KEFycmF5LmZyb20oJCgnaG9sZGluZ3MnKS5xdWVyeVNlbGVjdG9yQWxsKCdkZXRhaWxzW29wZW5dJykpLm1hcChlbD0+ZWwuZGF0YXNldC5zdG9jaykpOwogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgaWYoIWEubGVuZ3RoKXskKCdob2xkaW5ncycpLmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JztyZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCk7cmV0dXJufQogICQoJ2hvbGRpbmdzJykuaW5uZXJIVE1MPWEubWFwKChoLGkpPT57CiAgICBjb25zdCBjPWNhbGNIb2xkaW5nKGgpOwogICAgY29uc3QgdmFsaWRQcmljZT1OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wJiZoLmFzb2Y7CiAgICBjb25zdCBjbHM9dmFsaWRQcmljZT8oYy5uZXROb3c+PTA/J3Bvcyc6J25lZycpOicnOwogICAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwogICAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwoKICAgIHJldHVybiBgPGRldGFpbHMgY2xhc3M9InN0b2NrLWRldGFpbHMiIGRhdGEtc3RvY2s9IiR7d2F0Y2hFc2NhcGUoaC5jb2RlKX0iICR7b3BlbmVkLmhhcyhTdHJpbmcoaC5jb2RlKSk/J29wZW4nOicnfT48c3VtbWFyeT4ke3dhdGNoRXNjYXBlKGguY29tcGFueV9uYW1lfHxoLmNvZGUpfTwvc3VtbWFyeT48ZGl2IGNsYXNzPSJob2xkaW5nIj4KICAgICAgPGRpdiBjbGFzcz0iaG9sZGluZy1oZWFkIj4KICAgICAgICA8ZGl2PgogICAgICAgICAgPGI+JHtoLmNvZGV9PC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPiR7aC5jb21wYW55X25hbWV8fCIifTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9InJvdyI+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9ImVkaXRIb2xkaW5nU2hhcmVzKCR7aX0pIj7moKrmlbDlpInmm7Q8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIHNlY29uZGFyeSIgb25jbGljaz0icmVmcmVzaEhvbGRpbmcoJHtpfSkiPuabtOaWsDwvYnV0dG9uPgogICAgICAgICAgPGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gZGFuZ2VyIiBvbmNsaWNrPSJyZW1vdmVIb2xkaW5nKCR7aX0pIj7liYrpmaQ8L2J1dHRvbj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+54++5Zyo5YCk44OH44O844K/5pelICR7aC5hc29mfHwn4oCUJ30gLyDmnIDmlrDlj5blvpfntYLlgKQgJHt2YWxpZFByaWNlP3llbihoLmN1cnJlbnRfcHJpY2UpOifigJQnfSAvICR7aC5zaGFyZXN95qCqIC8g5Y+W5b6X5Y2Y5L6hICR7eWVuKGguY29zdCl9PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj7moKrkvqHjgr3jg7zjgrkgJHtoLnByaWNlX3NvdXJjZXx8J+KAlCd9IC8g5L6h5qC85bGl5q20ICR7aC5oaXN0b3J5X2Fzb2Z8fCfigJQnfSAke2guaGlzdG9yeV9zb3VyY2U/JygnK2guaGlzdG9yeV9zb3VyY2UrJyknOicnfTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5YaN6KiI566XICR7aC51cGRhdGVkX2F0P25ldyBEYXRlKGgudXBkYXRlZF9hdCkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyk6J+KAlCd9IC8g5pCN55uK44O75Zu65a6a44Op44Kk44Oz6Led6Zui44Gv44GT44Gu5pyA5paw5Y+W5b6X57WC5YCk44KS5L2/55SoPC9kaXY+CiAgICAgICR7dmFsaWRQcmljZT9gPGRpdiBjbGFzcz0iZnJlc2hib3ggJHtmcmVzaG5lc3NGb3IoaC5hc29mKS5jbHN9Ij48Yj4kewogICAgICAgIGZyZXNobmVzc0ZvcihoLmFzb2YpLmxldmVsPT09J2ZyZXNoJz8n4pyFIOmuruW6pk9LJzoKICAgICAgICBmcmVzaG5lc3NGb3IoaC5hc29mKS5sZXZlbD09PSd3YXJuaW5nJz8n4pqg77iPIOmBheW7tuazqOaEjyc6CiAgICAgICAgJ/Cfm5Eg5Y+k44GE5qCq5L6h44OH44O844K/JwogICAgICB9PC9iPjxkaXYgY2xhc3M9Im11dGVkIj4ke2ZyZXNobmVzc1RleHQoaC5hc29mKX08L2Rpdj48L2Rpdj5gOicnfQoKICAgICAgPGRpdiBjbGFzcz0iZGVjaXNpb24gJHtkLmNsc30iPgogICAgICAgICR7ZC5sYWJlbH0KICAgICAgICA8ZGl2IGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6NHB4Ij4ke2QucmVhc29ufTwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuOBvuOBpzwvc3Bhbj4KICAgICAgICAgIDxiIGNsYXNzPSJkaXN0YW5jZSI+JHt2YWxpZFByaWNlP2Rpc3RhbmNlSW5mbyhjdXIsYy5zdG9wUHJpY2UsJ3N0b3AnKTon4oCUJ308L2I+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWPguiAgyAke3llbihjLnN0b3BQcmljZSl9PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuOBvuOBpzwvc3Bhbj4KICAgICAgICAgIDxiIGNsYXNzPSJkaXN0YW5jZSI+JHt2YWxpZFByaWNlP2Rpc3RhbmNlSW5mbyhjdXIsYy50YWtlUHJpY2UsJ3Rha2UnKTon4oCUJ308L2I+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWPguiAgyAke3llbihjLnRha2VQcmljZSl9PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuWIpOWumuS/oemgvOW6pjwvc3Bhbj4KICAgICAgICAgIDxiPiR7ZC5jb25maWRlbmNlfSU8L2I+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJnYXVnZSI+PHNwYW4gc3R5bGU9IndpZHRoOiR7ZC5jb25maWRlbmNlfSUiPjwvc3Bhbj48L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjVweCI+4oC75Yik5a6a5L+h6aC85bqm44Gv44CB44OH44O844K/5YWF6Laz5bqm44O75pyf6ZaT44OI44Os44Oz44OJ44Gu5LiA6Ie05bqm44O75pyf5b6F5YCk57Wx6KiI44Gu5pyJ54Sh44GL44KJ5L2c44KL5Y+C6ICD5oyH5qiZ44Gn44CB55qE5Lit56K6546H44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgoKICAgICAgPGRpdiBjbGFzcz0iYWN0aW9uYm94Ij4KICAgICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuS7iuOBqeOBhuOBmeOCi++8nzwvc3Bhbj4KICAgICAgICA8YiBzdHlsZT0iZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweCI+JHthY3Rpb25UZXh0KGgsYyxkKX08L2I+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWPluW+l+e1guWApOODmeODvOOCueaQjeebijwvc3Bhbj48YiBjbGFzcz0iJHtjbHN9Ij4ke3ZhbGlkUHJpY2U/eWVuKGMubmV0Tm93KTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3ZhbGlkUHJpY2U/Zm10KGMubmV0Tm93UGN0KSsnJSc6J+KAlCd9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7osrfku5jmiYvmlbDmlpk8L3NwYW4+PGI+JHt5ZW4oYy5idXlGZWUpfTwvYj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5aOy5Y205omL5pWw5paZKOS7iik8L3NwYW4+PGI+JHt2YWxpZFByaWNlP3llbihjLnNlbGxGZWUpOifigJQnfTwvYj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5pCN5YiH44KK5Y+C6ICDPC9zcGFuPjxiPiR7eWVuKGMuc3RvcFByaWNlKX08L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7miYvmlbDmlpnovrwgJHt5ZW4oYy5zdG9wTmV0KX08L3NwYW4+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnRha2VQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMudGFrZU5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg4jjg6zjg7zjg6rjg7PjgrDlj4LogIM8L3NwYW4+PGI+JHt2YWxpZFByaWNlJiZjLnRyYWlsUHJpY2UhPT1udWxsP3llbihjLnRyYWlsUHJpY2UpOifigJQnfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPiR7ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpLmRlY2lzaW9uX29rPycyMOaXpemrmOWApOWfuua6lic6J+WxpeattOOBjOWPpOOBhOOBn+OCgeS/neeVmSd9PC9zcGFuPjwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDxoNCBzdHlsZT0ibWFyZ2luOjEycHggMCA3cHgiPvCfjq8g5a6f57i+44OZ44O844K55L6h5qC844Os44Oz44K4PC9oND4KICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICAgICR7cmFuZ2VIdG1sKCfnn63mnJ8gMjDml6UnLGN1cixzdGF0Rm9yKGgsJzIwZCcpKX0KICAgICAgICAke3JhbmdlSHRtbCgn5Lit5pyfIDEyNuaXpScsY3VyLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke3JhbmdlSHRtbCgn6ZW35pyfIDI1MuaXpScsY3VyLHN0YXRGb3IoaCwnMjUyZCcpKX0KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn5OQIOWun+e4vuODmeODvOOCueacn+W+heWApO+8iOe1seioiOWPguiAg++8iTwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke2V2SHRtbCgn55+t5pyfIDIw5pelJyxzdGF0Rm9yKGgsJzIwZCcpKX0KICAgICAgICAke2V2SHRtbCgn5Lit5pyfIDEyNuaXpScsc3RhdEZvcihoLCcxMjZkJykpfQogICAgICAgICR7ZXZIdG1sKCfplbfmnJ8gMjUy5pelJyxzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgogICAgICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+S+oeagvOODrOODs+OCuOODu+acn+W+heWApOOBr+WwhuadpeS6iOa4rOOBp+OBr+OBquOBj+OAgeWPluW+l+WPr+iDveOBqumBjuWOu+agquS+oeOBruODreODvOODquODs+OCsOWJjeaWueODquOCv+ODvOODs+WIhuW4g+OCkuacgOaWsOWPluW+l+e1guWApOOBq+W9k+OBpuOBr+OCgeOBn+e1seioiOWPguiAg+OBp+OBmeOAguacn+mWk+OBjOmHjeOBquOCi+aomeacrOOCkuWQq+OBv+OBvuOBmeOAgjwvcD4KICAgIDwvZGl2PjwvZGV0YWlscz5gOwogIH0pLmpvaW4oJycpOwogIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0UXVvdGUoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcXVvdGU/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHE9YXdhaXQgci5qc29uKCk7CiAgaWYocS5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcihxLnJlYXNvbnx8cS5lcnJvcnx8J+Wun+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiBxOwp9Cgphc3luYyBmdW5jdGlvbiBhZGRIb2xkaW5nKCl7CiAgY29uc3QgY29kZT0kKCdob2xkQ29kZScpLnZhbHVlLnRyaW0oKTsKICBjb25zdCBjb3N0PXZhbCgnaG9sZENvc3QnKSwgc2hhcmVzPXZhbCgnaG9sZFNoYXJlcycpOwogIGNvbnN0IHN0b3A9dmFsKCdzdG9wUGN0JyksIHRha2U9dmFsKCd0YWtlUGN0JyksIHRyYWlsPXZhbCgndHJhaWxQY3QnKTsKICBjb25zdCBmZWVNb2RlPSQoJ2ZlZU1vZGUnKS52YWx1ZTsKICBpZighY29kZXx8IWNvc3R8fCFzaGFyZXMpe2FsZXJ0KCfpipjmn4TjgrPjg7zjg4njg7vlj5blvpfljZjkvqHjg7vmoKrmlbDjgpLlhaXlipvjgZfjgabjga0nKTtyZXR1cm59CiAgY29uc3QgYnRuPWV2ZW50Py50YXJnZXQ7IGlmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnfQogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogICAgY29uc3QgaD17CiAgICAgIGNvZGUsCiAgICAgIGNvbXBhbnlfbmFtZToocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fCcnLAogICAgICBjb21wYW55X21hcmtldDoocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8JycsCiAgICAgIGNvbXBhbnlfc2VjdG9yMzM6KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8JycsCiAgICAgIGNvc3QsIHNoYXJlcywgZmVlX21vZGU6ZmVlTW9kZSwKICAgICAgc3RvcF9wY3Q6c3RvcD8/OCwgdGFrZV9wY3Q6dGFrZT8/MTUsIHRyYWlsX3BjdDp0cmFpbD8/NywKICAgICAgY3VycmVudF9wcmljZTpzLmxhc3RfY2xvc2UsIGhpZ2hfMjBkOnMuaGlnaF8yMGQsIGxvd18yMGQ6cy5sb3dfMjBkLAogICAgICByZXR1cm5fMjBkOnMucmV0dXJuXzIwZCwgcmV0dXJuXzEyNmQ6cy5yZXR1cm5fMTI2ZCwgcmV0dXJuXzI1MmQ6cy5yZXR1cm5fMjUyZCwKICAgICAgZm9yd2FyZF9zdGF0czpzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSwKICAgICAgYXNvZjpzLmxhc3RfZGF0ZSwKICAgICAgaGlzdG9yeV9hc29mOnMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlLAogICAgICBwcmljZV9zb3VyY2U6cy5wcmljZV9zb3VyY2V8fHEuc291cmNlfHwnJywKICAgICAgaGlzdG9yeV9zb3VyY2U6cy5oaXN0b3J5X3NvdXJjZXx8JycsCiAgICAgIHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpLAogICAgICBwcmljZV9zeW5jZWQ6dHJ1ZQogICAgfTsKICAgIGNvbnN0IGlkeD1hLmZpbmRJbmRleCh4PT54LmNvZGU9PT1jb2RlKTsKICAgIGlmKGlkeD49MClhW2lkeF09aDsgZWxzZSBhLnB1c2goaCk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSl9CiAgZmluYWxseXtpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmCd9fQp9CgoKZnVuY3Rpb24gYXBwbHlRdW90ZVRvSG9sZGluZyhoLHEpewogIGNvbnN0IHM9KHEmJnEuc25hcHNob3QpfHx7fTsKICBoLmNvbXBhbnlfbmFtZT0ocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fGguY29tcGFueV9uYW1lfHwnJzsKICBoLmNvbXBhbnlfbWFya2V0PShxLmNvbXBhbnkmJnEuY29tcGFueS5tYXJrZXQpfHxoLmNvbXBhbnlfbWFya2V0fHwnJzsKICBoLmNvbXBhbnlfc2VjdG9yMzM9KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8aC5jb21wYW55X3NlY3RvcjMzfHwnJzsKICBoLmN1cnJlbnRfcHJpY2U9cy5sYXN0X2Nsb3NlOwogIGguaGlnaF8yMGQ9cy5oaWdoXzIwZDsKICBoLmxvd18yMGQ9cy5sb3dfMjBkOwogIGgucmV0dXJuXzIwZD1zLnJldHVybl8yMGQ7CiAgaC5yZXR1cm5fMTI2ZD1zLnJldHVybl8xMjZkOwogIGgucmV0dXJuXzI1MmQ9cy5yZXR1cm5fMjUyZDsKICBoLmZvcndhcmRfc3RhdHM9cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e307CiAgaC5hc29mPXMubGFzdF9kYXRlOwogIGguaGlzdG9yeV9hc29mPXMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlOwogIGgucHJpY2Vfc291cmNlPXMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8Jyc7CiAgaC5oaXN0b3J5X3NvdXJjZT1zLmhpc3Rvcnlfc291cmNlfHwnJzsKICBoLnVwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogIGgucHJpY2Vfc3luY2VkPXRydWU7CiAgcmV0dXJuIGg7Cn0KCmZ1bmN0aW9uIHN5bmNBbmFseXplZFF1b3RlVG9Ib2xkaW5nKGNvZGUscSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBpPWEuZmluZEluZGV4KHg9PlN0cmluZyh4LmNvZGUpPT09U3RyaW5nKGNvZGUpKTsKICBpZihpPDApcmV0dXJuIGZhbHNlOwogIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhhW2ldLHEpOwogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICByZW5kZXJIb2xkaW5ncygpOwogIHJldHVybiB0cnVlOwp9Cgphc3luYyBmdW5jdGlvbiByZWZyZXNoQWxsSG9sZGluZ3MoZm9yY2U9ZmFsc2UpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgY29uc3QgYnRuPSQoJ3JlZnJlc2hBbGxCdG4nKTsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9J+S/neacieagquOBr+acqueZu+mMsuOBp+OBmeOAgic7CiAgICByZXR1cm47CiAgfQoKICBjb25zdCBrZXk9J2ZyZWVfaG9sZGluZ3NfbGFzdF9hdXRvX3JlZnJlc2hfdjE2JzsKICBjb25zdCBsYXN0PU51bWJlcihsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrZXkpfHwwKTsKICBjb25zdCBub3dNcz1EYXRlLm5vdygpOwogIGNvbnN0IHdhaXRNcz0zMCo2MCoxMDAwOwoKICBpZighZm9yY2UgJiYgbGFzdCAmJiBub3dNcy1sYXN0PHdhaXRNcyl7CiAgICBjb25zdCBtaW49TWF0aC5jZWlsKCh3YWl0TXMtKG5vd01zLWxhc3QpKS82MDAwMCk7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWDoh6rli5Xmm7TmlrDmuIjjgb/jgILmrKHjga7oh6rli5Xmm7TmlrDjgb7jgafntIQke21pbn3liIbjgIJgOwogICAgcmV0dXJuOwogIH0KCiAgaWYoYnRuKXtidG4uZGlzYWJsZWQ9dHJ1ZTtidG4udGV4dENvbnRlbnQ9J+abtOaWsOS4reKApid9CiAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1g5L+d5pyJ5qCqICR7YS5sZW5ndGh96YqY5p+E44Gu5pyA5paw57WC5YCk44KS5Y+W5b6X5Lit4oCmYDsKCiAgbGV0IG9rPTAsIG5nPTA7CiAgZm9yKGxldCBpPTA7aTxhLmxlbmd0aDtpKyspewogICAgdHJ5ewogICAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGFbaV0uY29kZSk7CiAgICAgIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhhW2ldLHEpOwogICAgICBvaysrOwogICAgfWNhdGNoKGUpewogICAgICBuZysrOwogICAgICBhW2ldLmxhc3RfcmVmcmVzaF9lcnJvcj1TdHJpbmcoZS5tZXNzYWdlfHxlKTsKICAgIH0KICB9CgogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrZXksU3RyaW5nKERhdGUubm93KCkpKTsKICByZW5kZXJIb2xkaW5ncygpOwoKICBpZihzdGF0dXMpewogICAgc3RhdHVzLnRleHRDb250ZW50PWDmnIDmlrDntYLlgKTjgaflho3oqIjnrpfvvJrmiJDlip8gJHtva33pipjmn4Qke25nP2AgLyDlpLHmlZcgJHtuZ33pipjmn4RgOicnfeOAgmA7CiAgfQogIGlmKGJ0bil7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5L+d5pyJ5qCq44KS5pyA5paw57WC5YCk44Gn5LiA5ous5pu05pawJ30KfQoKYXN5bmMgZnVuY3Rpb24gcmVmcmVzaEhvbGRpbmcoaSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKSwgaD1hW2ldOyBpZighaClyZXR1cm47CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShoLmNvZGUpOwogICAgYVtpXT1hcHBseVF1b3RlVG9Ib2xkaW5nKGgscSk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogICAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWAke2guY29kZX0g44KS5pyA5paw5Y+W5b6X57WC5YCk44Gn5YaN6KiI566X44GX44G+44GX44Gf44CCYDsKICB9Y2F0Y2goZSl7YWxlcnQoJ+abtOaWsOOCqOODqeODvDogJytlLm1lc3NhZ2UpfQp9CmZ1bmN0aW9uIGVkaXRIb2xkaW5nU2hhcmVzKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyksIGg9YVtpXTsKICBpZighaClyZXR1cm47CiAgY29uc3QgaW5wdXQ9cHJvbXB0KGAke2guY29kZX0g44Gu5paw44GX44GE5L+d5pyJ5qCq5pWw44KS5YWl5Yqb44GX44Gm44Gt77yIMeagquS7peS4iuOBruaVtOaVsO+8iWAsU3RyaW5nKGguc2hhcmVzKSk7CiAgaWYoaW5wdXQ9PT1udWxsKXJldHVybjsKICBjb25zdCB0ZXh0PWlucHV0LnRyaW0oKTsKICBjb25zdCBzaGFyZXM9TnVtYmVyKHRleHQpOwogIGlmKCEvXlswLTldKyQvLnRlc3QodGV4dCl8fCFOdW1iZXIuaXNTYWZlSW50ZWdlcihzaGFyZXMpfHxzaGFyZXM8MSl7CiAgICBhbGVydCgn5qCq5pWw44GvMeS7peS4iuOBruaVtOaVsOOBp+WFpeWKm+OBl+OBpuOBrScpO3JldHVybjsKICB9CiAgaC5zaGFyZXM9c2hhcmVzOwogIGguc2hhcmVzX3VwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICByZW5kZXJIb2xkaW5ncygpOwogIGNvbnN0IHN0YXR1cz0kKCdob2xkaW5nUmVmcmVzaFN0YXR1cycpOwogIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9YCR7aC5jb2RlfSDjgpIgJHtzaGFyZXN95qCq44Gr5aSJ5pu044GX44CB5pCN55uK44O75omL5pWw5paZ44O75L+d5pyJ5YWo5L2T44Gu6ZuG6KiI44KS5YaN6KiI566X44GX44G+44GX44Gf44CCYDsKfQpmdW5jdGlvbiByZW1vdmVIb2xkaW5nKGkpe2NvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7YS5zcGxpY2UoaSwxKTtzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7cmVuZGVySG9sZGluZ3MoKX0KCmZ1bmN0aW9uIHdhdGNoRXNjYXBlKHZhbHVlKXsKICByZXR1cm4gU3RyaW5nKHZhbHVlPz8nJykucmVwbGFjZSgvWyY8PiInXS9nLGM9Pih7JyYnOicmYW1wOycsJzwnOicmbHQ7JywnPic6JyZndDsnLCciJzonJnF1b3Q7JywiJyI6JyYjMzk7J31bY10pKTsKfQpjb25zdCB3YXRjaEJ1c3k9bmV3IFNldCgpOwpsZXQgd2F0Y2hCdWxrQnVzeT1mYWxzZTsKbGV0IG1vdmVtZW50QnVzeT1mYWxzZTsKZnVuY3Rpb24gcmVuZGVyV2F0Y2goKXsKICByZW5kZXJEYWlseUFkdmljZSgpOwogIGlmKCQoJ21vdmVtZW50VXAnKSlyZW5kZXJNb3ZlbWVudCgpOwogIGNvbnN0IGJ1bGtCdG49JCgnd2F0Y2hCdWxrQnRuJyk7CiAgaWYoYnVsa0J0bil7YnVsa0J0bi5kaXNhYmxlZD1tb3ZlbWVudEJ1c3l8fHdhdGNoQnVsa0J1c3l8fHdhdGNoQnVzeS5zaXplPjA7YnVsa0J0bi50ZXh0Q29udGVudD13YXRjaEJ1bGtCdXN5PyfkuIDmi6zliIbmnpDkuK3igKYnOifosrfjgYTmmYLjga7lj4LogIPjgpLkuIDmi6zmm7TmlrAnfQogIGNvbnN0IG9wZW5lZD1uZXcgU2V0KEFycmF5LmZyb20oJCgnd2F0Y2hzJykucXVlcnlTZWxlY3RvckFsbCgnZGV0YWlsc1tvcGVuXScpKS5tYXAoZWw9PmVsLmRhdGFzZXQuc3RvY2spKTsKICBjb25zdCBjYWNoZT1sb2NhbFN0b3JhZ2UuZ2V0SXRlbSgnZnJlZV93YXRjaF9hbmFseXNpc192MScpOwogIGxldCBhbmFseXNlcz17fTt0cnl7YW5hbHlzZXM9SlNPTi5wYXJzZShjYWNoZXx8J3t9Jyl9Y2F0Y2goZSl7fQogICQoJ3dhdGNocycpLmlubmVySFRNTD1sb2NhbCgnZnJlZV93YXRjaCcpLm1hcCgoeCxpKT0+ewogICAgY29uc3QgY29kZT1TdHJpbmcodHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZSk7CiAgICBjb25zdCBuYW1lPXR5cGVvZiB4PT09J3N0cmluZyc/Jyc6eC5uYW1lOwogICAgY29uc3QgdGltaW5nPXdhdGNoVGltaW5nKGFuYWx5c2VzW2NvZGVdKTsKICAgIGNvbnN0IGJhZGdlQ2xhc3M9dGltaW5nLmxhYmVsPT09J+iyt+OBhOWAmeijnO+8iOadoeS7tuS4gOiHtO+8iSc/J3dhdGNoLWJ1eSc6dGltaW5nLmxhYmVsPT09J+anmOWtkOimiyc/J3dhdGNoLW5ldXRyYWwnOid3YXRjaC1wZW5kaW5nJzsKICAgIHJldHVybiBgPGRldGFpbHMgY2xhc3M9InN0b2NrLWRldGFpbHMiIGRhdGEtc3RvY2s9IiR7d2F0Y2hFc2NhcGUoY29kZSl9IiAke29wZW5lZC5oYXMoY29kZSk/J29wZW4nOicnfT4KICAgICAgPHN1bW1hcnk+JHt3YXRjaEVzY2FwZShuYW1lfHxjb2RlKX0gPHNwYW4gY2xhc3M9IndhdGNoLXRpbWluZyAke2JhZGdlQ2xhc3N9Ij4ke3dhdGNoQnVzeS5oYXMoY29kZSk/J+WIhuaekOS4reKApic6d2F0Y2hFc2NhcGUodGltaW5nLmxhYmVsKX08L3NwYW4+PC9zdW1tYXJ5PgogICAgICA8ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50Ij48Yj4ke3dhdGNoRXNjYXBlKGNvZGUpfSAke3dhdGNoRXNjYXBlKG5hbWV8fCcnKX08L2I+CiAgICAgICAgPGRpdiBjbGFzcz0icm93IiBzdHlsZT0ibWFyZ2luOjEwcHggMCI+PGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gc2Vjb25kYXJ5IiBvbmNsaWNrPSJyZWZyZXNoV2F0Y2goJHtpfSkiICR7d2F0Y2hCdWxrQnVzeXx8d2F0Y2hCdXN5Lmhhcyhjb2RlKT8nZGlzYWJsZWQnOicnfT4ke3dhdGNoQnVzeS5oYXMoY29kZSk/J+WIhuaekOS4reKApic6J+iyt+OBhOaZguOBruWPguiAg+OCkuabtOaWsCd9PC9idXR0b24+PGJ1dHRvbiBjbGFzcz0ic21hbGxidG4gZGFuZ2VyIiBvbmNsaWNrPSJyZW1vdmVXYXRjaCgke2l9KSI+5YmK6ZmkPC9idXR0b24+PC9kaXY+CiAgICAgICAgJHt3YXRjaEFuYWx5c2lzSHRtbChhbmFseXNlc1tjb2RlXSl9CiAgICAgIDwvZGl2PjwvZGV0YWlscz5gOwogIH0pLmpvaW4oJycpfHwnPHAgY2xhc3M9Im11dGVkIj7mnKrnmbvpjLI8L3A+JzsKfQpmdW5jdGlvbiB3YXRjaFRpbWluZyhhKXsKICBpZighYSlyZXR1cm4ge2xhYmVsOifmnKrliIbmnpAnLHJlYXNvbjon44CM6LK344GE5pmC44Gu5Y+C6ICD44KS5pu05paw44CN44Gn44OH44O844K/44KS5Y+W5b6X44GX44G+44GZ44CCJ307CiAgY29uc3Qgcz1hLnNuYXBzaG90fHx7fSxzYz1hLnNjb3JlfHx7fTsKICBpZighZnJlc2huZXNzRm9yKHMubGFzdF9kYXRlKS5kZWNpc2lvbl9va3x8IWZyZXNobmVzc0ZvcihzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSkuZGVjaXNpb25fb2t8fGRhdGFBZ2VEYXlzKHMubGFzdF9kYXRlKT40fHxkYXRhQWdlRGF5cyhzLmhpc3RvcnlfbGFzdF9kYXRlfHxzLmxhc3RfZGF0ZSk+NCkKICAgIHJldHVybiB7bGFiZWw6J+WIpOWumuS/neeVmScscmVhc29uOifmoKrkvqHjg7vkvqHmoLzlsaXmrbTjgYzlj6TjgYTjgIHjgb7jgZ/jga/ml6Xku5jkuI3mmI7jgafjgZnjgIInfTsKICBpZihzYy5zY29yZTEwMD09bnVsbHx8IU51bWJlci5pc0Zpbml0ZShOdW1iZXIoc2Muc2NvcmUxMDApKXx8c2MuY292ZXJhZ2VfcGN0PT1udWxsfHwhTnVtYmVyLmlzRmluaXRlKE51bWJlcihzYy5jb3ZlcmFnZV9wY3QpKXx8TnVtYmVyKHNjLmNvdmVyYWdlX3BjdCk8NzB8fHMubGFzdF9jbG9zZT09bnVsbHx8IU51bWJlci5pc0Zpbml0ZShOdW1iZXIocy5sYXN0X2Nsb3NlKSl8fE51bWJlcihzLmxhc3RfY2xvc2UpPD0wKQogICAgcmV0dXJuIHtsYWJlbDon5Yik5a6a5L+d55WZJyxyZWFzb246J+muruW6puOCkuWKoOWRs+OBl+OBn+ODh+ODvOOCv+WFhei2s+W6puOBjDcwJeacqua6gOOAgeOBvuOBn+OBr+e3j+WQiOeCueOCkueul+WHuuOBp+OBjeOBvuOBm+OCk+OAgid9OwogIGNvbnN0IHByaWNlPU51bWJlcihzLmxhc3RfY2xvc2UpLGhpZ2g9TnVtYmVyKHMuaGlnaF8yMGQpLGxvdz1OdW1iZXIocy5sb3dfMjBkKTsKICBjb25zdCBwb3NpdGlvbj1oaWdoPmxvdz8ocHJpY2UtbG93KS8oaGlnaC1sb3cpOm51bGw7CiAgaWYoTnVtYmVyKHNjLnNjb3JlMTAwKTw0NSlyZXR1cm4ge2xhYmVsOifmp5jlrZDoposnLHJlYXNvbjon57eP5ZCI54K544GMNDXngrnmnKrmuoDjgafjgIHlvLHjgYTopoHntKDjgYzlhKrli6LjgafjgZnjgIInfTsKICBpZihwb3NpdGlvbiE9PW51bGwmJnBvc2l0aW9uPj0wLjkpcmV0dXJuIHtsYWJlbDon6auY5YCk5ZyP44O76L+944GE6LK344GE5rOo5oSPJyxyZWFzb246JzIw5pel6ZaT44Gu6auY5YCk44O75a6J5YCk44Gu56+E5Zuy44Gn5LiK5L2NMTAl44Gr5L2N572u44GX44Gm44GE44G+44GZ44CCJ307CiAgaWYoTnVtYmVyKHNjLnNjb3JlMTAwKT49NjUmJnMucmV0dXJuXzIwZCE9bnVsbCYmTnVtYmVyKHMucmV0dXJuXzIwZCk+MCkKICAgIHJldHVybiB7bGFiZWw6J+iyt+OBhOWAmeijnO+8iOadoeS7tuS4gOiHtO+8iScscmVhc29uOifnt4/lkIjngrk2NeeCueS7peS4iuODuzIw5pel6aiw6JC9546H44OX44Op44K544O744OH44O844K/5YWF6Laz5bqmNzAl5Lul5LiK44CC6LO85YWl5YmN44Gr54++5Zyo5YCk44KS56K66KqN44GX44Gm44GP44Gg44GV44GE44CCJ307CiAgcmV0dXJuIHtsYWJlbDon5qeY5a2Q6KaLJyxyZWFzb246J+iyt+OBhOWAmeijnOOBruadoeS7tuOBjOaPg+OBo+OBpuOBhOOBvuOBm+OCk+OAgid9Owp9CmZ1bmN0aW9uIHdhdGNoQW5hbHlzaXNIdG1sKGEpewogIGlmKCFhKXJldHVybiAnPHAgY2xhc3M9Im11dGVkIj7mnKrliIbmnpDjgILjgIzosrfjgYTmmYLjga7lj4LogIPjgpLmm7TmlrDjgI3jgpLmirzjgZfjgabjga3jgII8L3A+JzsKICBjb25zdCBzPWEuc25hcHNob3R8fHt9LHNjPWEuc2NvcmV8fHt9LHQ9d2F0Y2hUaW1pbmcoYSk7CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJkZWNpc2lvbiBkLXdhdGNoIj4ke3dhdGNoRXNjYXBlKHQubGFiZWwpfTxkaXYgY2xhc3M9Im11dGVkIj4ke3dhdGNoRXNjYXBlKHQucmVhc29uKX08L2Rpdj48L2Rpdj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+5YiG5p6Q5pu05pawICR7d2F0Y2hFc2NhcGUobmV3IERhdGUoYS51cGRhdGVkX2F0KS50b0xvY2FsZVN0cmluZygnamEtSlAnKSl9PGJyPuacgOaWsOWPluW+l+e1guWApCAke3llbihzLmxhc3RfY2xvc2UpfSAvIOODh+ODvOOCv+aXpSAke3dhdGNoRXNjYXBlKHMubGFzdF9kYXRlfHwn4oCUJyl9PGJyPuS+oeagvOWxpeattCAke3dhdGNoRXNjYXBlKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlfHwn4oCUJyl9IC8g44K944O844K5ICR7d2F0Y2hFc2NhcGUocy5wcmljZV9zb3VyY2V8fGEuc291cmNlfHwn4oCUJyl9PC9wPgogICAgPGRpdiBjbGFzcz0iZ3JpZDMiPjxkaXYgY2xhc3M9ImtwaSI+57eP5ZCI54K5PGI+JHtzYy5zY29yZTEwMD09bnVsbD8n4oCUJzpmbXQoc2Muc2NvcmUxMDAsMSkrJyAvIDEwMCd9PC9iPjwvZGl2PjxkaXYgY2xhc3M9ImtwaSI+44OH44O844K/5YWF6Laz5bqmPGI+JHtmbXQoc2MuY292ZXJhZ2VfcGN0LDEpfSU8L2I+PC9kaXY+PGRpdiBjbGFzcz0ia3BpIj7jg4jjg6zjg7Pjg4k8Yj4ke3dhdGNoRXNjYXBlKHN0YXRlSmEoKGEuc2lnbmFsfHx7fSkuc3RhdGUpKX08L2I+PC9kaXY+PC9kaXY+CiAgICA8cD7jg4bjgq/jg4vjgqvjg6sgJHtzY29yZUxhYmVsKHNjLnRlY2huaWNhbCl9IC8g5rG6566XICR7c2NvcmVMYWJlbChzYy5lYXJuaW5ncyl9IC8g6ZyA57WmICR7c2NvcmVMYWJlbChzYy5zdXBwbHkpfSAvIOWbveetlnByb3h5ICR7c2NvcmVMYWJlbChzYy5wb2xpY3kpfTwvcD4KICAgIDxwIGNsYXNzPSJtdXRlZCI+JHt3YXRjaEVzY2FwZShkcml2ZXJTZW50ZW5jZShzYykpfTwvcD4KICAgIDxkaXYgY2xhc3M9ImdyaWQzIj48ZGl2IGNsYXNzPSJrcGkiPjIw5pel6aiw6JC9546HPGI+JHtwY3Qocy5yZXR1cm5fMjBkKX08L2I+PC9kaXY+PGRpdiBjbGFzcz0ia3BpIj4xMjbml6XpqLDokL3njoc8Yj4ke3BjdChzLnJldHVybl8xMjZkKX08L2I+PC9kaXY+PGRpdiBjbGFzcz0ia3BpIj4yNTLml6XpqLDokL3njoc8Yj4ke3BjdChzLnJldHVybl8yNTJkKX08L2I+PC9kaXY+PC9kaXY+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPjXml6XvvI8yMOaXpeWHuuadpemrmOavlCAke2ZtdCgocy5zdXBwbHlfcHJveHl8fHt9KS52b2x1bWVfcmF0aW9fNV8yMCl95YCNIC8gMjDml6Xpq5jlgKQgJHt5ZW4ocy5oaWdoXzIwZCl9IC8g5a6J5YCkICR7eWVuKHMubG93XzIwZCl9PC9wPgogICAgPGg0PuWun+e4vuODmeODvOOCueacn+W+heWApO+8iOe1seioiOWPguiAg++8iTwvaDQ+PGRpdiBjbGFzcz0iZ3JpZDMiPiR7ZXZIdG1sKCfnn63mnJ8yMOaXpScsKHMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9KVsnMjBkJ10pfSR7ZXZIdG1sKCfkuK3mnJ8xMjbml6UnLChzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSlbJzEyNmQnXSl9JHtldkh0bWwoJ+mVt+acnzI1MuaXpScsKHMuZm9yd2FyZF9yZXR1cm5fc3RhdHN8fHt9KVsnMjUyZCddKX08L2Rpdj4KICAgIDxwIGNsYXNzPSJtdXRlZCI+6LK344GE5YCZ6KOc44Gv5p2h5Lu25Yik5a6a44Gn44CB5bCG5p2l44Gu5Yip55uK44KE6LK344GE5pmC44KS5L+d6Ki844GZ44KL44KC44Gu44Gn44Gv44GC44KK44G+44Gb44KT44CC5rG6566X44O75Zu9562W44Gv5Y+W5b6X44Gn44GN44Gf6KaB57Sg44Gu44G/5L2/55So44GX44CB5LiN5piO5YCk44Gv6KOc44GE44G+44Gb44KT44CC5L6h5qC844O75Ye65p2l6auY44Gv6YGF5bu244GZ44KL5aC05ZCI44GM44GC44KK44G+44GZ44CC5pyf5b6F5YCk44Gv6YGO5Y6744Gu6YeN6KSH5pyf6ZaT44KS5ZCr44KA57Wx6KiI5Y+C6ICD44Gn44GZ44CCPC9wPmA7Cn0KYXN5bmMgZnVuY3Rpb24gcmVmcmVzaFdhdGNoKGksYnVsaz1mYWxzZSl7CiAgaWYobW92ZW1lbnRCdXN5fHx3YXRjaEJ1bGtCdXN5JiYhYnVsaylyZXR1cm4gZmFsc2U7CiAgY29uc3QgaXRlbT1sb2NhbCgnZnJlZV93YXRjaCcpW2ldO2lmKGl0ZW09PT11bmRlZmluZWQpcmV0dXJuOwogIGNvbnN0IGNvZGU9U3RyaW5nKHR5cGVvZiBpdGVtPT09J3N0cmluZyc/aXRlbTppdGVtLmNvZGUpOwogIGlmKHdhdGNoQnVzeS5oYXMoY29kZSkpcmV0dXJuIGZhbHNlOwogIHdhdGNoQnVzeS5hZGQoY29kZSk7cmVuZGVyV2F0Y2goKTsKICB0cnl7CiAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGNvZGUpLHM9cS5zbmFwc2hvdHx8e307CiAgICBsZXQgZj17c2NvcmU6bnVsbCxmcmVzaG5lc3M6e2ZhY3RvcjowfX0scD17c2NvcmU6bnVsbCxtYXRjaGVkX3RoZW1lczpbXX07CiAgICB0cnl7Zj1hd2FpdCBnZXRGdW5kYW1lbnRhbHMoY29kZSl9Y2F0Y2goZSl7fQogICAgdHJ5e3A9YXdhaXQgZ2V0UG9saWN5KGNvZGUpfWNhdGNoKGUpe30KICAgIGNvbnN0IHJlc3BvbnNlPWF3YWl0IGZldGNoKCcvYXBpL2ZyZWUvYW5hbHl6ZScse21ldGhvZDonUE9TVCcsaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSxjYWNoZTonbm8tc3RvcmUnLGJvZHk6SlNPTi5zdHJpbmdpZnkoewogICAgICBjb2RlLHByaWNlOnMubGFzdF9jbG9zZSxyZXR1cm4yMDpzLnJldHVybl8yMGQscmV0dXJuMTI2OnMucmV0dXJuXzEyNmQscmV0dXJuMjUyOnMucmV0dXJuXzI1MmQsCiAgICAgIGVhcm5pbmdzX3Njb3JlOmYuc2NvcmUscG9saWN5X3Njb3JlOnAuc2NvcmUscG9saWN5X21vZGU6J2F1dG8nLHN1cHBseV9zY29yZToocy5zdXBwbHlfcHJveHl8fHt9KS5zY29yZSwKICAgICAgbWFya2V0X2ZyZXNobmVzczptYXJrZXRGcmVzaG5lc3NGYWN0b3Iocy5oaXN0b3J5X2xhc3RfZGF0ZXx8cy5sYXN0X2RhdGUpLAogICAgICBlYXJuaW5nc19mcmVzaG5lc3M6Zi5mcmVzaG5lc3MmJmYuZnJlc2huZXNzLmZhY3RvciE9PXVuZGVmaW5lZD9mLmZyZXNobmVzcy5mYWN0b3I6KGYuc2NvcmU9PW51bGw/MDoxKSwKICAgICAgcG9saWN5X2ZyZXNobmVzczpwb2xpY3lGcmVzaG5lc3NGYWN0b3IocCxmYWxzZSkKICAgIH0pfSk7CiAgICBjb25zdCByZXN1bHQ9YXdhaXQgcmVzcG9uc2UuanNvbigpO2lmKCFyZXNwb25zZS5va3x8cmVzdWx0LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHJlc3VsdC5yZWFzb258fHJlc3VsdC5lcnJvcnx8J+WIhuaekOOBq+WkseaVl+OBl+OBvuOBl+OBnycpOwogICAgY29uc3QgbGlzdD1sb2NhbCgnZnJlZV93YXRjaCcpOwogICAgY29uc3QgaW5kZXg9bGlzdC5maW5kSW5kZXgoeD0+U3RyaW5nKHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpPT09Y29kZSk7CiAgICBpZihpbmRleDwwKXJldHVybiBmYWxzZTsKICAgIGxldCBjYWNoZT17fTt0cnl7Y2FjaGU9SlNPTi5wYXJzZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbSgnZnJlZV93YXRjaF9hbmFseXNpc192MScpfHwne30nKX1jYXRjaChlKXt9CiAgICBjYWNoZVtjb2RlXT17c25hcHNob3Q6cyxzb3VyY2U6cS5zb3VyY2Usc2NvcmU6cmVzdWx0LnNjb3JlLHNpZ25hbDpyZXN1bHQuc2lnbmFsLHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpfTsKICAgIGxvY2FsU3RvcmFnZS5zZXRJdGVtKCdmcmVlX3dhdGNoX2FuYWx5c2lzX3YxJyxKU09OLnN0cmluZ2lmeShjYWNoZSkpOwogICAgY29uc3QgbmFtZT0ocS5jb21wYW55fHx7fSkubmFtZXx8KHR5cGVvZiBsaXN0W2luZGV4XT09PSdzdHJpbmcnPycnOmxpc3RbaW5kZXhdLm5hbWUpfHwnJzsKICAgIGxpc3RbaW5kZXhdPXsuLi4odHlwZW9mIGxpc3RbaW5kZXhdPT09J29iamVjdCc/bGlzdFtpbmRleF06e30pLGNvZGUsbmFtZX07c2F2ZSgnZnJlZV93YXRjaCcsbGlzdCk7CiAgICByZXR1cm4gdHJ1ZTsKICB9Y2F0Y2goZSl7aWYoIWJ1bGspYWxlcnQoJ+OCpuOCqeODg+ODgeWIhuaekOOCqOODqeODvO+8micrZS5tZXNzYWdlKyfjgILkv53lrZjmuIjjgb/ntZDmnpzjgYzjgYLjgozjgbDliY3lm57liIbjgpLooajnpLrjgZfjgb7jgZnjgIInKTtyZXR1cm4gZmFsc2U7fQogIGZpbmFsbHl7d2F0Y2hCdXN5LmRlbGV0ZShjb2RlKTtyZW5kZXJXYXRjaCgpfQp9CmFzeW5jIGZ1bmN0aW9uIHJlZnJlc2hBbGxXYXRjaCgpewogIGlmKG1vdmVtZW50QnVzeXx8d2F0Y2hCdWxrQnVzeXx8d2F0Y2hCdXN5LnNpemUpcmV0dXJuOwogIGNvbnN0IGNvZGVzPVsuLi5uZXcgU2V0KGxvY2FsKCdmcmVlX3dhdGNoJykubWFwKHg9PlN0cmluZyh0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKSkpXTsKICBjb25zdCBzdGF0dXM9JCgnd2F0Y2hCdWxrU3RhdHVzJyk7CiAgaWYoIWNvZGVzLmxlbmd0aCl7c3RhdHVzLnRleHRDb250ZW50PSfjgqbjgqnjg4Pjg4Hjg6rjgrnjg4jjga/mnKrnmbvpjLLjgafjgZnjgIInO3JldHVybjt9CiAgd2F0Y2hCdWxrQnVzeT10cnVlO3JlbmRlcldhdGNoKCk7CiAgbGV0IGRvbmU9MCxmYWlsZWQ9MCxza2lwcGVkPTA7CiAgdHJ5ewogICAgZm9yKGxldCBuPTA7bjxjb2Rlcy5sZW5ndGg7bisrKXsKICAgICAgY29uc3QgY29kZT1jb2Rlc1tuXTsKICAgICAgY29uc3QgaW5kZXg9bG9jYWwoJ2ZyZWVfd2F0Y2gnKS5maW5kSW5kZXgoeD0+U3RyaW5nKHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpPT09Y29kZSk7CiAgICAgIGlmKGluZGV4PDApe3NraXBwZWQrKztjb250aW51ZTt9CiAgICAgIHN0YXR1cy50ZXh0Q29udGVudD1gJHtuKzF9IC8gJHtjb2Rlcy5sZW5ndGh96YqY5p+E77yaJHtjb2RlfSDjgpLliIbmnpDkuK3igKZgOwogICAgICBpZihhd2FpdCByZWZyZXNoV2F0Y2goaW5kZXgsdHJ1ZSkpZG9uZSsrO2Vsc2UgZmFpbGVkKys7CiAgICAgIGlmKG48Y29kZXMubGVuZ3RoLTEpewogICAgICAgIHN0YXR1cy50ZXh0Q29udGVudD1gJHtuKzF9IC8gJHtjb2Rlcy5sZW5ndGh96YqY5p+E44KS5Yem55CG5riI44G/44CC5qyh44Gu5YiG5p6Q44G+44Gn57SEMTPnp5LigKbvvIjmiJDlip8gJHtkb25lfSAvIOWkseaVlyAke2ZhaWxlZH3vvIlgOwogICAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmU9PnNldFRpbWVvdXQocmVzb2x2ZSwxMzAwMCkpOwogICAgICB9CiAgICB9CiAgICBzdGF0dXMudGV4dENvbnRlbnQ9YOS4gOaLrOabtOaWsOWujOS6hu+8muaIkOWKnyAke2RvbmV9IC8g5aSx5pWXICR7ZmFpbGVkfSAvIOWJiumZpOa4iOOBvyAke3NraXBwZWR944CC5aSx5pWX44GX44Gf6YqY5p+E44Gv5YmN5Zue44Gu57WQ5p6c44GM44GC44KM44Gw6KGo56S644GX44G+44GZ44CC6YCU5Lit44Gn6L+95Yqg44GX44Gf6YqY5p+E44Gv5qyh5Zue44Gu5a++6LGh44Gn44GZ44CCYDsKICB9Y2F0Y2goZSl7c3RhdHVzLnRleHRDb250ZW50PSfkuIDmi6zmm7TmlrDjgpLkuK3mlq3jgZfjgb7jgZfjgZ/vvJonK2UubWVzc2FnZTt9CiAgZmluYWxseXt3YXRjaEJ1bGtCdXN5PWZhbHNlO3JlbmRlcldhdGNoKCk7fQp9CmxldCBtb3ZlbWVudFN0b3BSZXF1ZXN0ZWQ9ZmFsc2UsbW92ZW1lbnRXYWl0VGltZXI9bnVsbCxtb3ZlbWVudFdhaXRSZXNvbHZlPW51bGw7CmNvbnN0IE1BUktFVF9HRU5SRVM9eyJTZW1pY29uZHVjdG9ycyI6ICLljYrlsI7kvZPjg7vpm7vlrZDmqZ/lmagiLCAiSW5kdXN0cmlhbCBFbGVjdHJvbmljcyI6ICLljYrlsI7kvZPjg7vpm7vlrZDmqZ/lmagiLCAiQXVkaW8vVmlkZW8gRXF1aXBtZW50IjogIuWNiuWwjuS9k+ODu+mbu+WtkOapn+WZqCIsICJDb21wdXRlcnMvQ29uc3VtZXIgRWxlY3Ryb25pY3MiOiAi5Y2K5bCO5L2T44O76Zu75a2Q5qmf5ZmoIiwgIk5ldHdvcmtpbmciOiAi5Y2K5bCO5L2T44O76Zu75a2Q5qmf5ZmoIiwgIlByZWNpc2lvbiBQcm9kdWN0cyI6ICLljYrlsI7kvZPjg7vpm7vlrZDmqZ/lmagiLCAiV2F0Y2hlcy9DbG9ja3MvUGFydHMiOiAi5Y2K5bCO5L2T44O76Zu75a2Q5qmf5ZmoIiwgIkNvbXB1dGVyIFNlcnZpY2VzIjogIklU44O76YCa5L+hIiwgIlNvZnR3YXJlIjogIklU44O76YCa5L+hIiwgIkludGVybmV0L09ubGluZSI6ICJJVOODu+mAmuS/oSIsICJXaXJlZCBUZWxlY29tbXVuaWNhdGlvbnMgU2VydmljZXMiOiAiSVTjg7vpgJrkv6EiLCAiV2lyZWxlc3MgVGVsZWNvbW11bmljYXRpb25zIFNlcnZpY2VzIjogIklU44O76YCa5L+hIiwgIkF1dG8gJiBDb21tZXJjaWFsIFZlaGljbGUgUGFydHMiOiAi6Ieq5YuV6LuK44O76Ly46YCB5qmf5ZmoIiwgIkF1dG9tb2JpbGVzIjogIuiHquWLlei7iuODu+i8uOmAgeapn+WZqCIsICJDb21tZXJjaWFsIFZlaGljbGVzIjogIuiHquWLlei7iuODu+i8uOmAgeapn+WZqCIsICJUaXJlcyI6ICLoh6rli5Xou4rjg7vovLjpgIHmqZ/lmagiLCAiQWVyb3NwYWNlIFByb2R1Y3RzL1BhcnRzIjogIuiHquWLlei7iuODu+i8uOmAgeapn+WZqCIsICJEZWZlbnNlIEVxdWlwbWVudC9Qcm9kdWN0cyI6ICLoh6rli5Xou4rjg7vovLjpgIHmqZ/lmagiLCAiSW5kdXN0cmlhbCBNYWNoaW5lcnkiOiAi5qmf5qKw44O755Sj5qWt6Kit5YKZIiwgIkluZHVzdHJpYWwgUHJvZHVjdHMiOiAi5qmf5qKw44O755Sj5qWt6Kit5YKZIiwgIk1vYmlsZSBNYWNoaW5lcnkiOiAi5qmf5qKw44O755Sj5qWt6Kit5YKZIiwgIkVsZWN0cmljIFV0aWxpdGllcyI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiR2FzIFV0aWxpdGllcyI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiTXVsdGl1dGlsaXRpZXMiOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIlJlbmV3YWJsZSBFbmVyZ3kgR2VuZXJhdGlvbiI6ICLpm7vlipvjg7vjgqzjgrnjg7vjgqjjg43jg6vjgq7jg7wiLCAiTWFqb3IgT2lsICYgR2FzIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJPaWwgJiBHYXMgUHJvZHVjdHMvU2VydmljZXMiOiAi6Zu75Yqb44O744Ks44K544O744Ko44ON44Or44Ku44O8IiwgIk9pbCBFeHRyYWN0aW9uIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJDb2FsIjogIumbu+WKm+ODu+OCrOOCueODu+OCqOODjeODq+OCruODvCIsICJBbHVtaW51bSI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiQ29tbW9kaXR5IENoZW1pY2FscyI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiU3BlY2lhbHR5IENoZW1pY2FscyI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiTm9uLUZlcnJvdXMgTWV0YWxzIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJJcm9uL1N0ZWVsIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJHZW5lcmFsIE1pbmluZyI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiUHJlY2lvdXMgTWV0YWxzIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJQYXBlci9QdWxwIjogIue0oOadkOODu+WMluWtpuODu+mHkeWxniIsICJDb250YWluZXJzL1BhY2thZ2luZyI6ICLntKDmnZDjg7vljJblrabjg7vph5HlsZ4iLCAiQmlvdGVjaG5vbG9neSI6ICLljLvolqzlk4Hjg7vljLvnmYIiLCAiUGhhcm1hY2V1dGljYWxzIjogIuWMu+iWrOWTgeODu+WMu+eZgiIsICJNZWRpY2FsIEVxdWlwbWVudC9TdXBwbGllcyI6ICLljLvolqzlk4Hjg7vljLvnmYIiLCAiSGVhbHRoY2FyZSBQcm92aXNpb24iOiAi5Yy76Jas5ZOB44O75Yy755mCIiwgIkRydWcgUmV0YWlsIjogIuWMu+iWrOWTgeODu+WMu+eZgiIsICJCYW5raW5nIjogIumHkeiejeODu+S/nemZuiIsICJNYWpvciBJbnRlcm5hdGlvbmFsIEJhbmtzIjogIumHkeiejeODu+S/nemZuiIsICJDb25zdW1lciBGaW5hbmNlIjogIumHkeiejeODu+S/nemZuiIsICJGaW5hbmNlIENvbXBhbmllcyI6ICLph5Hono3jg7vkv53pmboiLCAiRnVsbC1MaW5lIEluc3VyYW5jZSI6ICLph5Hono3jg7vkv53pmboiLCAiTGlmZSBJbnN1cmFuY2UiOiAi6YeR6J6N44O75L+d6Zm6IiwgIk5vbi1MaWZlIEluc3VyYW5jZSI6ICLph5Hono3jg7vkv53pmboiLCAiSW52ZXN0bWVudCBBZHZpc29ycyI6ICLph5Hono3jg7vkv53pmboiLCAiTW9ydGdhZ2VzIjogIumHkeiejeODu+S/nemZuiIsICJTZWN1cml0aWVzIjogIumHkeiejeODu+S/nemZuiIsICJDb25zdHJ1Y3Rpb24iOiAi5bu66Kit44O75LiN5YuV55SjIiwgIlJlc2lkZW50aWFsIEJ1aWxkaW5nIENvbnN0cnVjdGlvbiI6ICLlu7roqK3jg7vkuI3li5XnlKMiLCAiQnVpbGRpbmcgTWF0ZXJpYWxzL1Byb2R1Y3RzIjogIuW7uuioreODu+S4jeWLleeUoyIsICJSZWFsIEVzdGF0ZSBBZ2VudHMvQnJva2VycyI6ICLlu7roqK3jg7vkuI3li5XnlKMiLCAiUmVhbCBFc3RhdGUgRGV2ZWxvcGVycyI6ICLlu7roqK3jg7vkuI3li5XnlKMiLCAiRm9vZCBQcm9kdWN0cyI6ICLpo5/lk4Hjg7vovrLmnpfmsLTnlKMiLCAiRmFybWluZyI6ICLpo5/lk4Hjg7vovrLmnpfmsLTnlKMiLCAiRmlzaGluZyI6ICLpo5/lk4Hjg7vovrLmnpfmsLTnlKMiLCAiQWxjb2hvbGljIEJldmVyYWdlcy9Ecmlua3MiOiAi6aOf5ZOB44O76L6y5p6X5rC055SjIiwgIk5vbi1BbGNvaG9saWMgQmV2ZXJhZ2VzL0RyaW5rcyI6ICLpo5/lk4Hjg7vovrLmnpfmsLTnlKMiLCAiVG9iYWNjbyI6ICLpo5/lk4Hjg7vovrLmnpfmsLTnlKMiLCAiQ2xvdGhpbmcgUmV0YWlsIjogIuWwj+WjsuODu+WklumjnyIsICJGb29kIFJldGFpbCI6ICLlsI/lo7Ljg7vlpJbpo58iLCAiSG9tZSBHb29kcyBSZXRhaWwiOiAi5bCP5aOy44O75aSW6aOfIiwgIk1peGVkIFJldGFpbGluZyI6ICLlsI/lo7Ljg7vlpJbpo58iLCAiUmVzdGF1cmFudHMiOiAi5bCP5aOy44O75aSW6aOfIiwgIlNwZWNpYWx0eSBSZXRhaWwiOiAi5bCP5aOy44O75aSW6aOfIiwgIkNsb3RoaW5nIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJGb290d2VhciI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiRnVybml0dXJlIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJIb3VzZXdhcmVzIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJOb25kdXJhYmxlIEhvdXNlaG9sZCBQcm9kdWN0cyI6ICLnlJ/mtLvnlKjlk4Hjg7vooaPmlpkiLCAiUGVyc29uYWwgQ2FyZSBQcm9kdWN0cy9BcHBsaWFuY2VzIjogIueUn+a0u+eUqOWTgeODu+iho+aWmSIsICJTcG9ydHMgR29vZHMiOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIkxlaXN1cmUgR29vZHMiOiAi55Sf5rS755So5ZOB44O76KGj5paZIiwgIkFpciBGcmVpZ2h0IjogIumBi+i8uOODu+eJqea1gSIsICJQYXNzZW5nZXIgQWlybGluZXMiOiAi6YGL6Ly444O754mp5rWBIiwgIlBhc3NlbmdlciBUcmFuc3BvcnQsIE90aGVyIjogIumBi+i8uOODu+eJqea1gSIsICJSYWlscm9hZHMiOiAi6YGL6Ly444O754mp5rWBIiwgIlRyYW5zcG9ydGF0aW9uIFNlcnZpY2VzIjogIumBi+i8uOODu+eJqea1gSIsICJUcnVja2luZyI6ICLpgYvovLjjg7vnianmtYEiLCAiV2F0ZXIgVHJhbnNwb3J0L1NoaXBwaW5nIjogIumBi+i8uOODu+eJqea1gSIsICJCcm9hZGNhc3RpbmciOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIkdhbWJsaW5nIEluZHVzdHJpZXMiOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIkhvdGVscyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiTW90aW9uIFBpY3R1cmUvU291bmQgUmVjb3JkaW5nIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJQcmludGluZyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiUHVibGlzaGluZyI6ICLlqK/mpb3jg7vjg6Hjg4fjgqPjgqLjg7voprPlhYkiLCAiUmVjcmVhdGlvbmFsIFNlcnZpY2VzIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJUb3VyaXNtIjogIuWor+alveODu+ODoeODh+OCo+OCouODu+ims+WFiSIsICJUb3lzICYgR2FtZXMiOiAi5aiv5qW944O744Oh44OH44Kj44Ki44O76Kaz5YWJIiwgIldob2xlc2FsZXJzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJBY2NvdW50aW5nIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJBZHZlcnRpc2luZy9NYXJrZXRpbmcvUHVibGljIFJlbGF0aW9ucyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiQ29uc3VtZXIgU2VydmljZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIkRpdmVyc2lmaWVkIEJ1c2luZXNzIFNlcnZpY2VzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJEaXZlcnNpZmllZCBIb2xkaW5nIENvbXBhbmllcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiRW1wbG95bWVudC9UcmFpbmluZyBTZXJ2aWNlcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiRW52aXJvbm1lbnQvV2FzdGUgTWFuYWdlbWVudCI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiR2VuZXJhbCBTZXJ2aWNlcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiVGVjaG5pY2FsIFNlcnZpY2VzIjogIuWVhuekvuODu+OCteODvOODk+OCuSIsICJXYXRlciBVdGlsaXRpZXMiOiAi5ZWG56S+44O744K144O844OT44K5IiwgIlNoZWxsIGNvbXBhbmllcyI6ICLllYbnpL7jg7vjgrXjg7zjg5PjgrkiLCAiQ2xvc2VkLUVuZCBGdW5kcyI6ICJFVEbjg7vjg5XjgqHjg7Pjg4kiLCAiRXhjaGFuZ2UtVHJhZGVkIEZ1bmRzIjogIkVURuODu+ODleOCoeODs+ODiSIsICJNdXR1YWwgJiBPdGhlciBGdW5kcyI6ICJFVEbjg7vjg5XjgqHjg7Pjg4kifTsKZnVuY3Rpb24gc3RvY2tHZW5yZShzdG9jayl7cmV0dXJuIE1BUktFVF9HRU5SRVNbc3RvY2suc2VjdG9yXXx8J+OBneOBruS7luODu+alreeoruS4jeaYjid9CmZ1bmN0aW9uIHNlbGVjdGVkTWFya2V0R2VucmUoKXtyZXR1cm4gbG9jYWxTdG9yYWdlLmdldEl0ZW0oJ2ZyZWVfbWFya2V0X2dlbnJlX3YxJyl8fCflhajmpa3nqK4nfQpmdW5jdGlvbiBhbGxNYXJrZXRTdG9ja3MoKXtyZXR1cm4gbG9jYWwoJ2ZyZWVfbWFya2V0X3VuaXZlcnNlX3YxJyl9CmZ1bmN0aW9uIHJlbmRlck1hcmtldEdlbnJlcygpewogIGNvbnN0IHNlbGVjdD0kKCdtYXJrZXRHZW5yZScpO2lmKCFzZWxlY3QpcmV0dXJuOwogIGNvbnN0IHNlbGVjdGVkPXNlbGVjdGVkTWFya2V0R2VucmUoKSxjb3VudHM9bmV3IE1hcCgpOwogIGZvcihjb25zdCBzdG9jayBvZiBhbGxNYXJrZXRTdG9ja3MoKSl7Y29uc3QgZ2VucmU9c3RvY2tHZW5yZShzdG9jayk7Y291bnRzLnNldChnZW5yZSwoY291bnRzLmdldChnZW5yZSl8fDApKzEpfQogIHNlbGVjdC5pbm5lckhUTUw9JzxvcHRpb24gdmFsdWU9IuWFqOalreeoriI+5YWo5qWt56iu77yIJythbGxNYXJrZXRTdG9ja3MoKS5sZW5ndGgrJ+mKmOafhO+8iTwvb3B0aW9uPicrWy4uLmNvdW50cy5rZXlzKCldLnNvcnQoKGEsYik9PmEubG9jYWxlQ29tcGFyZShiLCdqYScpKS5tYXAoZ2VucmU9PmA8b3B0aW9uIHZhbHVlPSIke3dhdGNoRXNjYXBlKGdlbnJlKX0iPiR7d2F0Y2hFc2NhcGUoZ2VucmUpfe+8iCR7Y291bnRzLmdldChnZW5yZSl96YqY5p+E77yJPC9vcHRpb24+YCkuam9pbignJyk7CiAgaWYoc2VsZWN0ZWQhPT0n5YWo5qWt56iuJyYmIWNvdW50cy5oYXMoc2VsZWN0ZWQpKWxvY2FsU3RvcmFnZS5zZXRJdGVtKCdmcmVlX21hcmtldF9nZW5yZV92MScsJ+WFqOalreeoricpOwogIHNlbGVjdC52YWx1ZT1zZWxlY3RlZE1hcmtldEdlbnJlKCk7c2VsZWN0LmRpc2FibGVkPW1vdmVtZW50QnVzeXx8IWFsbE1hcmtldFN0b2NrcygpLmxlbmd0aDsKICAkKCdtYXJrZXRHZW5yZUxvYWQnKS5kaXNhYmxlZD1tb3ZlbWVudEJ1c3k7Cn0KZnVuY3Rpb24gY2hhbmdlTWFya2V0R2VucmUoKXsKICBpZihtb3ZlbWVudEJ1c3kpcmV0dXJuOwogIGxvY2FsU3RvcmFnZS5zZXRJdGVtKCdmcmVlX21hcmtldF9nZW5yZV92MScsJCgnbWFya2V0R2VucmUnKS52YWx1ZSk7CiAgJCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD1zZWxlY3RlZE1hcmtldEdlbnJlKCkrJ+OBq+WIh+OCiuabv+OBiOOBvuOBl+OBn+OAguWAmeijnOOBqOW3oeWbnuWvvuixoeOCkuOBk+OBruOCuOODo+ODs+ODq+OBq+e1nuOCiuOBvuOBmeOAgic7CiAgcmVuZGVyTW92ZW1lbnQoKTsKfQphc3luYyBmdW5jdGlvbiBsb2FkTWFya2V0R2VucmVzKCl7CiAgY29uc3QgYnRuPSQoJ21hcmtldEdlbnJlTG9hZCcpO2J0bi5kaXNhYmxlZD10cnVlOwogIHRyeXthd2FpdCBlbnN1cmVNYXJrZXRVbml2ZXJzZSgpO3JlbmRlck1vdmVtZW50KCk7JCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD0n44K444Oj44Oz44Or5LiA6Kan44KS6Kqt44G/6L6844G/44G+44GX44Gf44CC6Kq/44G544Gf44GE44K444Oj44Oz44Or44KS6YG444KT44Gn44Gt44CCJ30KICBjYXRjaChlKXskKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PWUubWVzc2FnZX0KICBmaW5hbGx5e2J0bi5kaXNhYmxlZD1tb3ZlbWVudEJ1c3k7fQp9CmZ1bmN0aW9uIG1hcmtldEN1cnNvcktleSgpe3JldHVybiAnZnJlZV9tYXJrZXRfY3Vyc29yX2dlbnJlX3YxOicrc2VsZWN0ZWRNYXJrZXRHZW5yZSgpfQpmdW5jdGlvbiByZWdpc3RlcmVkTW92ZW1lbnRTdG9ja3MoKXtjb25zdCBnZW5yZT1zZWxlY3RlZE1hcmtldEdlbnJlKCk7cmV0dXJuIGFsbE1hcmtldFN0b2NrcygpLmZpbHRlcihzdG9jaz0+Z2VucmU9PT0n5YWo5qWt56iuJ3x8c3RvY2tHZW5yZShzdG9jayk9PT1nZW5yZSl9CmFzeW5jIGZ1bmN0aW9uIGVuc3VyZU1hcmtldFVuaXZlcnNlKCl7CiAgaWYoYWxsTWFya2V0U3RvY2tzKCkubGVuZ3RoKXJldHVybjsKICBjb25zdCByZXNwb25zZT1hd2FpdCBmZXRjaCgnL2FwaS9mcmVlL21hcmtldC11bml2ZXJzZScse2NhY2hlOiduby1zdG9yZSd9KSxyZXN1bHQ9YXdhaXQgcmVzcG9uc2UuanNvbigpOwogIGlmKCFyZXNwb25zZS5va3x8cmVzdWx0LnN0YXR1cyE9PSdvayd8fCFBcnJheS5pc0FycmF5KHJlc3VsdC5zdG9ja3MpfHwhcmVzdWx0LnN0b2Nrcy5sZW5ndGgpdGhyb3cgbmV3IEVycm9yKHJlc3VsdC5yZWFzb258fCfpipjmn4TkuIDopqfjgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICBzYXZlKCdmcmVlX21hcmtldF91bml2ZXJzZV92MScscmVzdWx0LnN0b2Nrcyk7CiAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oJ2ZyZWVfbWFya2V0X3VuaXZlcnNlX2ZldGNoZWQnLHJlc3VsdC5mZXRjaGVkX2F0KTsKfQpmdW5jdGlvbiBzdG9wTW92ZW1lbnQoKXsKICBtb3ZlbWVudFN0b3BSZXF1ZXN0ZWQ9dHJ1ZTsKICBpZihtb3ZlbWVudFdhaXRUaW1lciE9PW51bGwpY2xlYXJUaW1lb3V0KG1vdmVtZW50V2FpdFRpbWVyKTsKICBpZihtb3ZlbWVudFdhaXRSZXNvbHZlKXttb3ZlbWVudFdhaXRSZXNvbHZlKCk7bW92ZW1lbnRXYWl0UmVzb2x2ZT1udWxsO30KICAkKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PSflgZzmraLkuK3igKblj5blvpfkuK3jga4x6YqY5p+E44GM5a6M5LqG44GX44Gf44KJ5q2i44G+44KK44G+44GZ44CCJzsKfQpmdW5jdGlvbiBtb3ZlbWVudFdhaXQoKXtyZXR1cm4gbmV3IFByb21pc2UocmVzb2x2ZT0+e21vdmVtZW50V2FpdFJlc29sdmU9cmVzb2x2ZTttb3ZlbWVudFdhaXRUaW1lcj1zZXRUaW1lb3V0KCgpPT57bW92ZW1lbnRXYWl0VGltZXI9bnVsbDttb3ZlbWVudFdhaXRSZXNvbHZlPW51bGw7cmVzb2x2ZSgpfSwxMzAwMCl9KX0KZnVuY3Rpb24gY29tcGFjdE1vdmVtZW50UXVvdGUocSl7CiAgY29uc3Qgcz1xLnNuYXBzaG90fHx7fTsKICBjb25zdCBmaWVsZHM9WydsYXN0X2RhdGUnLCdsYXN0X2Nsb3NlJywncmV0dXJuXzIwZCcsJ3ByaWNlX3NvdXJjZScsJ2hpc3RvcnlfYWRqdXN0bWVudCddOwogIGNvbnN0IHNuYXBzaG90PU9iamVjdC5mcm9tRW50cmllcyhmaWVsZHMubWFwKGs9PltrLHNba10/P251bGxdKSk7CiAgc25hcHNob3Quc3VwcGx5X3Byb3h5PXt2b2x1bWVfcmF0aW9fNV8yMDoocy5zdXBwbHlfcHJveHl8fHt9KS52b2x1bWVfcmF0aW9fNV8yMD8/bnVsbH07CiAgcmV0dXJuIHtzb3VyY2U6cS5zb3VyY2V8fCcnLGNvbXBhbnk6e25hbWU6KHEuY29tcGFueXx8e30pLm5hbWV8fCcnfSxzbmFwc2hvdCxyb3dzOihxLnJvd3N8fFtdKS5zbGljZSgtNikubWFwKHI9Pih7ZGF0ZTpyLmRhdGUsY2xvc2U6ci5jbG9zZX0pKX07Cn0KZnVuY3Rpb24gcmVhZE1vdmVtZW50KCl7dHJ5e3JldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKCdmcmVlX21vdmVtZW50X3YxJyl8fCd7fScpfWNhdGNoKGUpe3JldHVybiB7fX19CmZ1bmN0aW9uIG1vdmVtZW50TWV0cmljcyhxKXsKICBjb25zdCBzPShxfHx7fSkuc25hcHNob3R8fHt9OwogIGNvbnN0IHJvd3M9KChxfHx7fSkucm93c3x8W10pLmZpbHRlcihyPT50eXBlb2Ygci5jbG9zZT09PSdudW1iZXInJiZOdW1iZXIuaXNGaW5pdGUoci5jbG9zZSkmJnIuY2xvc2U+MCYmci5kYXRlKS5zb3J0KChhLGIpPT5TdHJpbmcoYS5kYXRlKS5sb2NhbGVDb21wYXJlKFN0cmluZyhiLmRhdGUpKSk7CiAgY29uc3QgbGFzdD1yb3dzLmF0KC0xKSxkYXRlPWxhc3QmJmxhc3QuZGF0ZTsKICBjb25zdCByZXQ9bj0+cm93cy5sZW5ndGg+bj8ocm93cy5hdCgtMSkuY2xvc2Uvcm93cy5hdCgtMS1uKS5jbG9zZS0xKSoxMDA6bnVsbDsKICBjb25zdCBkYXk9cmV0KDEpLGZpdmU9cmV0KDUpLHR3ZW50eT10eXBlb2Ygcy5yZXR1cm5fMjBkPT09J251bWJlcicmJk51bWJlci5pc0Zpbml0ZShzLnJldHVybl8yMGQpP3MucmV0dXJuXzIwZDpyZXQoMjApOwogIGNvbnN0IHZvbHVtZT0ocy5zdXBwbHlfcHJveHl8fHt9KS52b2x1bWVfcmF0aW9fNV8yMDsKICBsZXQga2luZD0nb3RoZXInLHJlYXNvbj0n5rOo55uu44O75LiL6JC96K2m5oiS44Gu5p2h5Lu244Gr6Kmy5b2T44GX44G+44Gb44KT44CCJzsKICBpZighZGF0ZXx8ZGF0YUFnZURheXMoZGF0ZSk9PT1udWxsfHxkYXRhQWdlRGF5cyhkYXRlKT40fHxzLmxhc3RfZGF0ZSE9PWRhdGUpe3JlYXNvbj0n5bGl5q2044GM5Y+k44GE44O75LiN6Laz44CB44G+44Gf44Gv54++5Zyo5YCk44Go5bGl5q2044Gu5pel5LuY44GM5LiN5LiA6Ie044CC5Yik5a6a5L+d55WZ44Gn44GZ44CCJzt9CiAgZWxzZSBpZihkYXk9PT1udWxsfHxmaXZlPT09bnVsbCl7cmVhc29uPScx5Za25qWt5pel44O7NeWWtualreaXpeOBruavlOi8g+OBq+W/heimgeOBquWxpeattOOBjOS4jei2s+OAguWIpOWumuS/neeVmeOBp+OBmeOAgic7fQogIGVsc2UgaWYoZGF5Pj0xJiZmaXZlPjApe2tpbmQ9J3VwJztyZWFzb249J+ebtOi/kTHllrbmpa3ml6XvvIsxJeS7peS4iuOAgTXllrbmpa3ml6XjgoLjg5fjg6njgrnjgILkuIrmmIfjga7li5XjgY3jgYzntprjgYTjgabjgYTjgovlj4LogIPlgJnoo5zjgIInO30KICBlbHNlIGlmKGRheTw9LTEmJihmaXZlPDB8fHR3ZW50eSE9PW51bGwmJnR3ZW50eTwwKSl7a2luZD0nZG93bic7cmVhc29uPSfnm7Tov5Ex5Za25qWt5pel4oiSMSXku6XkuIvjgIE15Za25qWt5pel44G+44Gf44GvMjDllrbmpa3ml6XjgoLjg57jgqTjg4rjgrnjgILkuIvlkJHjgY3jga7li5XjgY3jgavms6jmhI/jgIInO30KICByZXR1cm4ge3Jvd3MsZGF0ZSxkYXksZml2ZSx0d2VudHksdm9sdW1lLGtpbmQscmVhc29ufTsKfQpmdW5jdGlvbiBtb3ZlbWVudENoYXJ0KHJvd3MpewogIGlmKHJvd3MubGVuZ3RoPDIpcmV0dXJuICc8cCBjbGFzcz0ibXV0ZWQiPuODgeODo+ODvOODiOeUqOOBruWxpeattOS4jei2szwvcD4nOwogIGNvbnN0IHZhbHVlcz1yb3dzLm1hcChyPT5yLmNsb3NlKSxsb3c9TWF0aC5taW4oLi4udmFsdWVzKSxoaWdoPU1hdGgubWF4KC4uLnZhbHVlcyk7CiAgY29uc3QgcG9pbnRzPXZhbHVlcy5tYXAoKHYsaSk9PmAkeyg4K2kqMjg0Lyh2YWx1ZXMubGVuZ3RoLTEpKS50b0ZpeGVkKDEpfSwkeyhoaWdoPT09bG93PzQwOjcyLSh2LWxvdykqNjQvKGhpZ2gtbG93KSkudG9GaXhlZCgxKX1gKS5qb2luKCcgJyk7CiAgY29uc3QgY29sb3I9dmFsdWVzLmF0KC0xKT49dmFsdWVzWzBdPycjMTU4MDNkJzonI2I5MWMxYyc7CiAgcmV0dXJuIGA8c3ZnIHZpZXdCb3g9IjAgMCAzMDAgODAiIHJvbGU9ImltZyIgYXJpYS1sYWJlbD0i55u06L+R5pyA5aSnNuWWtualreaXpeOBrue1guWApOaOqOenuyIgc3R5bGU9IndpZHRoOjEwMCU7aGVpZ2h0OjEwMHB4Ij48cG9seWxpbmUgcG9pbnRzPSIke3BvaW50c30iIGZpbGw9Im5vbmUiIHN0cm9rZT0iJHtjb2xvcn0iIHN0cm9rZS13aWR0aD0iMi41Ii8+PC9zdmc+PGRpdiBjbGFzcz0ibXV0ZWQiPiR7d2F0Y2hFc2NhcGUocm93c1swXS5kYXRlKX0g4oaSICR7d2F0Y2hFc2NhcGUocm93cy5hdCgtMSkuZGF0ZSl9IC8g5pyA5a6JICR7eWVuKGxvdyl944O75pyA6auYICR7eWVuKGhpZ2gpfTwvZGl2PmA7Cn0KZnVuY3Rpb24gcmVuZGVyTW92ZW1lbnQoKXsKICByZW5kZXJEYWlseUFkdmljZSgpOwogIHJlbmRlck1hcmtldEdlbnJlcygpOwogIGlmKCQoJ3JhZGVuTmV3c0l0ZW1zJykpcmVuZGVyUmFkZW5OZXdzKCk7CiAgY29uc3QgY2FjaGU9cmVhZE1vdmVtZW50KCksb3BlbmVkPW5ldyBTZXQoQXJyYXkuZnJvbShkb2N1bWVudC5xdWVyeVNlbGVjdG9yQWxsKCdkZXRhaWxzW2RhdGEtbW92ZW1lbnRdW29wZW5dJykpLm1hcChlbD0+ZWwuZGF0YXNldC5tb3ZlbWVudCkpOwogIGNvbnN0IHVuaXZlcnNlPXJlZ2lzdGVyZWRNb3ZlbWVudFN0b2NrcygpOwogIGNvbnN0IGNoZWNrZWQ9dW5pdmVyc2UuZmlsdGVyKHg9PmNhY2hlW3guY29kZV0pOwogIGNvbnN0IGZyZXNoPWNoZWNrZWQuZmlsdGVyKHg9PiFjYWNoZVt4LmNvZGVdLmVycm9yJiZkYXRhQWdlRGF5cygoKGNhY2hlW3guY29kZV0ucXx8e30pLnNuYXBzaG90fHx7fSkubGFzdF9kYXRlKSE9PW51bGwmJmRhdGFBZ2VEYXlzKCgoY2FjaGVbeC5jb2RlXS5xfHx7fSkuc25hcHNob3R8fHt9KS5sYXN0X2RhdGUpPD00KS5sZW5ndGg7CiAgY29uc3QgY292ZXJhZ2U9JCgnbW92ZW1lbnRDb3ZlcmFnZScpO2lmKGNvdmVyYWdlKWNvdmVyYWdlLnRleHRDb250ZW50PWAke3NlbGVjdGVkTWFya2V0R2VucmUoKX3vvJrkuIDopqcgJHt1bml2ZXJzZS5sZW5ndGh96YqY5p+EIC8g5Y+W5b6X6Kmm6KGM5riI44G/ICR7Y2hlY2tlZC5sZW5ndGh9IC8gNOaXpeS7peWGheOBruODh+ODvOOCvyAke2ZyZXNofSAvIOacquiqv+afuyAke3VuaXZlcnNlLmxlbmd0aC1jaGVja2VkLmxlbmd0aH3jgILkuIDopqfjgavjga/lj6TjgYTnmbvpjLLjgoTlj5blvpfkuI3lj6/jga7pipjmn4TjgYzlkKvjgb7jgozjgovloLTlkIjjgYzjgYLjgorjgb7jgZnjgIJgOwogIGNvbnN0IGdyb3Vwcz17dXA6W10sZG93bjpbXSxvdGhlcjpbXX07CiAgZm9yKGNvbnN0IHN0b2NrIG9mIHJlZ2lzdGVyZWRNb3ZlbWVudFN0b2NrcygpKXsKICAgIGNvbnN0IGE9Y2FjaGVbc3RvY2suY29kZV07aWYoIWEpY29udGludWU7Y29uc3QgbT1tb3ZlbWVudE1ldHJpY3MoYSYmYS5xKTsKICAgIGlmKCFhfHxhLmVycm9yKXttLmtpbmQ9J290aGVyJzttLnJlYXNvbj1hJiZhLmVycm9yPyflj5blvpflpLHmlZfjgILliKTlrprkv53nlZnvvIjliY3lm57lsaXmrbTjgYzjgYLjgozjgbDlj4LogIPooajnpLrvvInjgIInOifmnKrmm7TmlrDjgIInO30KICAgIGdyb3Vwc1ttLmtpbmRdLnB1c2goe3N0b2NrLGEsbX0pOwogIH0KICBncm91cHMudXAuc29ydCgoYSxiKT0+Yi5tLmRheS1hLm0uZGF5KTtncm91cHMuZG93bi5zb3J0KChhLGIpPT5hLm0uZGF5LWIubS5kYXkpOwogIGNvbnN0IGlkcz17dXA6J1VwJyxkb3duOidEb3duJyxvdGhlcjonT3RoZXInfTsKICBmb3IoY29uc3Qga2luZCBvZiBPYmplY3Qua2V5cyhncm91cHMpKXsKICAgICQoJ21vdmVtZW50JytpZHNba2luZF0rJ0NvdW50JykudGV4dENvbnRlbnQ9Z3JvdXBzW2tpbmRdLmxlbmd0aCsn5Lu2JzsKICAgICQoJ21vdmVtZW50JytpZHNba2luZF0pLmlubmVySFRNTD1ncm91cHNba2luZF0uc2xpY2UoMCw1MCkubWFwKCh7c3RvY2ssYSxtfSk9PnsKICAgICAgY29uc3QgbmFtZT0oYSYmYS5xJiZhLnEuY29tcGFueSYmYS5xLmNvbXBhbnkubmFtZSl8fHN0b2NrLm5hbWV8fHN0b2NrLmNvZGU7CiAgICAgIGNvbnN0IGxhYmVsPWtpbmQ9PT0ndXAnPyfms6jnm67lgJnoo5wnOmtpbmQ9PT0nZG93bic/J+S4i+iQveitpuaIkic6J+OBneOBruS7luODu+S/neeVmSc7CiAgICAgIGNvbnN0IHM9KGEmJmEucSYmYS5xLnNuYXBzaG90KXx8e307CiAgICAgIHJldHVybiBgPGRldGFpbHMgY2xhc3M9InN0b2NrLWRldGFpbHMiIGRhdGEtbW92ZW1lbnQ9IiR7d2F0Y2hFc2NhcGUoc3RvY2suY29kZSl9IiAke29wZW5lZC5oYXMoc3RvY2suY29kZSk/J29wZW4nOicnfT48c3VtbWFyeT4ke3dhdGNoRXNjYXBlKG5hbWUpfSA8c3BhbiBjbGFzcz0id2F0Y2gtdGltaW5nIHdhdGNoLW5ldXRyYWwiPiR7bGFiZWx9IC8gMeWWtualreaXpSAke3BjdChtLmRheSl9PC9zcGFuPjwvc3VtbWFyeT48ZGl2IGNsYXNzPSJ3YXRjaC1jb250ZW50Ij4KICAgICAgICA8Yj4ke3dhdGNoRXNjYXBlKHN0b2NrLmNvZGUpfSAke3dhdGNoRXNjYXBlKG5hbWUpfTwvYj48cCBjbGFzcz0ibXV0ZWQiPuOCuOODo+ODs+ODq++8miR7d2F0Y2hFc2NhcGUoc3RvY2tHZW5yZShzdG9jaykpfTwvcD48cD4ke3dhdGNoRXNjYXBlKG0ucmVhc29uKX08L3A+CiAgICAgICAgPHAgY2xhc3M9Im11dGVkIj7lsaXmrbTml6UgJHt3YXRjaEVzY2FwZShtLmRhdGV8fCfigJQnKX3vvIgke20uZGF0ZSYmZGF0YUFnZURheXMobS5kYXRlKT09PTA/J+acrOaXpeOBruaXpei2s+ODh+ODvOOCvyc6J+W9k+aXpeODh+ODvOOCv+OBp+OBr+OBguOCiuOBvuOBm+OCkyd977yJIC8g5Y+W5b6XICR7d2F0Y2hFc2NhcGUoYT9uZXcgRGF0ZShhLnVwZGF0ZWRfYXQpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpOifigJQnKX08YnI+5L6h5qC844K944O844K5ICR7d2F0Y2hFc2NhcGUocy5wcmljZV9zb3VyY2V8fChhJiZhLnEmJmEucS5zb3VyY2UpfHwn4oCUJyl9IC8g5bGl5q206Kq/5pW0ICR7d2F0Y2hFc2NhcGUocy5oaXN0b3J5X2FkanVzdG1lbnR8fCfnorroqo3jgafjgY3jgb7jgZvjgpMnKX08L3A+CiAgICAgICAgJHttb3ZlbWVudENoYXJ0KG0ucm93cyl9PGRpdiBjbGFzcz0iZ3JpZDMiPjxkaXYgY2xhc3M9ImtwaSI+MeWWtualreaXpTxiPiR7cGN0KG0uZGF5KX08L2I+PC9kaXY+PGRpdiBjbGFzcz0ia3BpIj415Za25qWt5pelPGI+JHtwY3QobS5maXZlKX08L2I+PC9kaXY+PGRpdiBjbGFzcz0ia3BpIj4yMOWWtualreaXpTxiPiR7cGN0KG0udHdlbnR5KX08L2I+PC9kaXY+PC9kaXY+CiAgICAgICAgPHAgY2xhc3M9Im11dGVkIj415pel77yPMjDml6XlubPlnYflh7rmnaXpq5jmr5QgJHtmbXQobS52b2x1bWUpfeWAjSAke3R5cGVvZiBtLnZvbHVtZT09PSdudW1iZXInJiZtLnZvbHVtZT49MS4yPyfvvIjlh7rmnaXpq5jlopfliqDvvIknOicnfSAvIOacgOaWsOWxpeattOe1guWApCAke3llbihtLnJvd3MubGVuZ3RoP20ucm93cy5hdCgtMSkuY2xvc2U6bnVsbCl9PC9wPgogICAgICA8L2Rpdj48L2RldGFpbHM+YDsKICAgIH0pLmpvaW4oJycpfHwnPHAgY2xhc3M9Im11dGVkIj7lj5blvpfmuIjjgb/jga7nr4Tlm7LjgavoqbLlvZPlgJnoo5zjga/jgYLjgorjgb7jgZvjgpPjgII8L3A+JzsKICB9Cn0KYXN5bmMgZnVuY3Rpb24gcmVmcmVzaE1vdmVtZW50KGFsbEdlbnJlPWZhbHNlKXsKICBpZihtb3ZlbWVudEJ1c3l8fHdhdGNoQnVsa0J1c3l8fHdhdGNoQnVzeS5zaXplKXskKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PSfjgbvjgYvjga7liIbmnpDjgYzlrozkuobjgZfjgabjgYvjgonmm7TmlrDjgZfjgabjga3jgIInO3JldHVybjt9CiAgbW92ZW1lbnRCdXN5PXRydWU7bW92ZW1lbnRTdG9wUmVxdWVzdGVkPWZhbHNlOwogICQoJ21vdmVtZW50QWxsQnRuJykuZGlzYWJsZWQ9dHJ1ZTsKICBjb25zdCBidG49JCgnbW92ZW1lbnRCdG4nKTtidG4uZGlzYWJsZWQ9dHJ1ZTtidG4udGV4dENvbnRlbnQ9J+mKmOafhOS4gOimp+OCkueiuuiqjeS4reKApic7JCgnbW92ZW1lbnRTdG9wJykuZGlzYWJsZWQ9ZmFsc2U7cmVuZGVyV2F0Y2goKTsKICBsZXQgZG9uZT0wLGZhaWxlZD0wOwogIHRyeXsKICAgIGF3YWl0IGVuc3VyZU1hcmtldFVuaXZlcnNlKCk7CiAgICBjb25zdCBhbGw9cmVnaXN0ZXJlZE1vdmVtZW50U3RvY2tzKCk7CiAgICBpZighYWxsLmxlbmd0aCl7JCgnbW92ZW1lbnRTdGF0dXMnKS50ZXh0Q29udGVudD0n44GT44Gu44K444Oj44Oz44Or44Gu6YqY5p+E44Gv44GC44KK44G+44Gb44KT44CCJztyZXR1cm47fQogICAgY29uc3QgY3Vyc29yS2V5PW1hcmtldEN1cnNvcktleSgpOwogICAgbGV0IGN1cnNvcj1OdW1iZXIobG9jYWxTdG9yYWdlLmdldEl0ZW0oY3Vyc29yS2V5KXx8KHNlbGVjdGVkTWFya2V0R2VucmUoKT09PSflhajmpa3nqK4nP2xvY2FsU3RvcmFnZS5nZXRJdGVtKCdmcmVlX21hcmtldF9jdXJzb3JfdjEnKTpudWxsKXx8MCk7CiAgICBpZighTnVtYmVyLmlzU2FmZUludGVnZXIoY3Vyc29yKXx8Y3Vyc29yPDB8fGN1cnNvcj49YWxsLmxlbmd0aCljdXJzb3I9MDsKICAgIGlmKGFsbEdlbnJlKWN1cnNvcj0wOwogICAgY29uc3QgYmF0Y2g9YWxsR2VucmU/YWxsOmFsbC5zbGljZShjdXJzb3IsY3Vyc29yKzIwKTsKICAgIGZvcihsZXQgaT0wO2k8YmF0Y2gubGVuZ3RoO2krKyl7CiAgICAgIGlmKG1vdmVtZW50U3RvcFJlcXVlc3RlZClicmVhazsKICAgICAgY29uc3Qgc3RvY2s9YmF0Y2hbaV07CiAgICAgICQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9YCR7aSsxfSAvICR7YmF0Y2gubGVuZ3RofemKmOafhO+8iOS4gOimpyAke2N1cnNvcitpKzF9IC8gJHthbGwubGVuZ3Rofe+8ie+8miR7c3RvY2submFtZXx8c3RvY2suY29kZX0g44Gu5bGl5q2044KS5Y+W5b6X5Lit4oCmYDsKICAgICAgY29uc3QgY2FjaGU9cmVhZE1vdmVtZW50KCk7CiAgICAgIHRyeXtjb25zdCBxPWF3YWl0IGdldFF1b3RlKHN0b2NrLmNvZGUpO2NhY2hlW3N0b2NrLmNvZGVdPXtxOmNvbXBhY3RNb3ZlbWVudFF1b3RlKHEpLHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpLGVycm9yOm51bGx9O2RvbmUrKzt9CiAgICAgIGNhdGNoKGUpe2NhY2hlW3N0b2NrLmNvZGVdPXsuLi4oY2FjaGVbc3RvY2suY29kZV18fHt9KSx1cGRhdGVkX2F0Om5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxlcnJvcjpTdHJpbmcoZS5tZXNzYWdlKX07ZmFpbGVkKys7fQogICAgICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgnZnJlZV9tb3ZlbWVudF92MScsSlNPTi5zdHJpbmdpZnkoY2FjaGUpKTsKICAgICAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oY3Vyc29yS2V5LFN0cmluZygoY3Vyc29yK2krMSklYWxsLmxlbmd0aCkpO3JlbmRlck1vdmVtZW50KCk7CiAgICAgIGlmKGk8YmF0Y2gubGVuZ3RoLTEmJiFtb3ZlbWVudFN0b3BSZXF1ZXN0ZWQpeyQoJ21vdmVtZW50U3RhdHVzJykudGV4dENvbnRlbnQ9YCR7aSsxfSAvICR7YmF0Y2gubGVuZ3RofemKmOafhOOCkuWHpueQhua4iOOBv+OAguasoeOBruWPluW+l+OBvuOBp+e0hDEz56eS4oCmYDthd2FpdCBtb3ZlbWVudFdhaXQoKTt9CiAgICB9CiAgICAkKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PWAke21vdmVtZW50U3RvcFJlcXVlc3RlZD8n5YGc5q2i44GX44G+44GX44GfJzon5LuK5Zue44Gu5beh5Zue5a6M5LqGJ33vvJrlj5blvpfmiJDlip8gJHtkb25lfSAvIOWkseaVlyAke2ZhaWxlZH3jgILmrKHlm57jga/jgZPjga7jgrjjg6Pjg7Pjg6vjga7ntprjgY3jgYvjgonoqr/jgbnjgb7jgZnjgILlgJnoo5zjga/lkITjg4fjg7zjgr/ml6Xjga7ntYLlgKTjg5njg7zjgrnjgafjgIHlhajpipjmn4Tlt6Hlm57jgavjga/mmYLplpPjgYzjgYvjgYvjgorjgb7jgZnjgIJgOwogIH1jYXRjaChlKXskKCdtb3ZlbWVudFN0YXR1cycpLnRleHRDb250ZW50PSflt6Hlm57jgpLkuK3mlq3jgZfjgb7jgZfjgZ/vvJonK2UubWVzc2FnZTt9CiAgZmluYWxseXttb3ZlbWVudEJ1c3k9ZmFsc2U7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5qyh44GuMjDpipjmn4TjgpLoqr/jgbnjgosnOyQoJ21vdmVtZW50QWxsQnRuJykuZGlzYWJsZWQ9ZmFsc2U7JCgnbW92ZW1lbnRTdG9wJykuZGlzYWJsZWQ9dHJ1ZTtyZW5kZXJNb3ZlbWVudCgpO3JlbmRlcldhdGNoKCk7fQp9CmZ1bmN0aW9uIHJlbW92ZVdhdGNoKGkpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfd2F0Y2gnKSwgeD1hW2ldOwogIGlmKHg9PT11bmRlZmluZWQpcmV0dXJuOwogIGNvbnN0IGNvZGU9dHlwZW9mIHg9PT0nc3RyaW5nJz94OnguY29kZTsKICBpZighY29uZmlybShgJHtjb2RlfSDjgpLjgqbjgqnjg4Pjg4Hjg6rjgrnjg4jjgYvjgonliYrpmaTjgZfjgb7jgZnjgYvvvJ9gKSlyZXR1cm47CiAgYS5zcGxpY2UoaSwxKTsKICBzYXZlKCdmcmVlX3dhdGNoJyxhKTsKICB0cnl7Y29uc3QgY2FjaGU9SlNPTi5wYXJzZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbSgnZnJlZV93YXRjaF9hbmFseXNpc192MScpfHwne30nKTtkZWxldGUgY2FjaGVbU3RyaW5nKGNvZGUpXTtsb2NhbFN0b3JhZ2Uuc2V0SXRlbSgnZnJlZV93YXRjaF9hbmFseXNpc192MScsSlNPTi5zdHJpbmdpZnkoY2FjaGUpKX1jYXRjaChlKXt9CiAgcmVuZGVyV2F0Y2goKTsKfQphc3luYyBmdW5jdGlvbiBhZGRXYXRjaCgpewogIGxldCBjPSQoJ3dhdGNoQ29kZScpLnZhbHVlLnRyaW0oKTsgaWYoIWMpcmV0dXJuOwogIGxldCBpbmZvPW51bGw7CiAgdHJ5e2luZm89YXdhaXQgZ2V0Q29tcGFueShjKX1jYXRjaChlKXt9CiAgbGV0IGE9bG9jYWwoJ2ZyZWVfd2F0Y2gnKTsKICBjb25zdCBleGlzdHM9YS5zb21lKHg9Pih0eXBlb2YgeD09PSdzdHJpbmcnP3g6eC5jb2RlKT09PWMpOwogIGlmKCFleGlzdHMpYS5wdXNoKHtjb2RlOmMsbmFtZTppbmZvJiZpbmZvLm5hbWU/aW5mby5uYW1lOicnfSk7CiAgc2F2ZSgnZnJlZV93YXRjaCcsYSk7CiAgcmVuZGVyV2F0Y2goKTsKfQpmdW5jdGlvbiB1cGRhdGVLYWJ1dGFuKCl7bGV0IGM9JCgnY29kZScpLnZhbHVlLnRyaW0oKTskKCdrYWJ1dGFuJykuaHJlZj1jPydodHRwczovL2thYnV0YW4uanAvc3RvY2svP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoYyk6J2h0dHBzOi8va2FidXRhbi5qcC8nfQokKCdjb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT57dXBkYXRlS2FidXRhbigpO3NjaGVkdWxlQ29tcGFueUxvb2t1cCgnY29kZScsJ2NvbXBhbnlOYW1lJywnJyl9KTt1cGRhdGVLYWJ1dGFuKCk7CiQoJ2hvbGRDb2RlJykuYWRkRXZlbnRMaXN0ZW5lcignaW5wdXQnLCgpPT5zY2hlZHVsZUNvbXBhbnlMb29rdXAoJ2hvbGRDb2RlJywnaG9sZENvbXBhbnlOYW1lJywnJykpOwokKCd3YXRjaENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnd2F0Y2hDb2RlJywnd2F0Y2hDb21wYW55TmFtZScsJycpKTsKCgoKYXN5bmMgZnVuY3Rpb24gZ2V0UG9saWN5KGNvZGUpewogIGNvbnN0IHI9YXdhaXQgZmV0Y2goJy9hcGkvbW9iaWxlL3BvbGljeT9jb2RlPScrZW5jb2RlVVJJQ29tcG9uZW50KGNvZGUpLHtjYWNoZTonbm8tc3RvcmUnfSk7CiAgY29uc3QgeD1hd2FpdCByLmpzb24oKTsKICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5Zu9562W44OH44O844K/44KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJyk7CiAgcmV0dXJuIHgucG9saWN5fHx7fTsKfQoKZnVuY3Rpb24gcG9saWN5U291cmNlU3RhdHVzSmEocyl7CiAgaWYocz09PSd2ZXJpZmllZF9saXZlJylyZXR1cm4gJ+WFrOW8j+ODmuODvOOCuOeiuuiqjea4iCc7CiAgaWYocz09PSdwYXJ0aWFsX2xpdmUnKXJldHVybiAn5YWs5byP44Oa44O844K46YOo5YiG56K66KqNJzsKICBpZihzPT09J3ZlcmlmaWVkX3JlZ2lzdHJ5JylyZXR1cm4gJ+acgOe1gueiuuiqjea4iOWFrOW8j+OCveODvOOCuSc7CiAgcmV0dXJuICfnorroqo3kuI3lj68nOwp9CgpmdW5jdGlvbiByZW5kZXJQb2xpY3lUaGVtZXMocCl7CiAgY29uc3QgYm94PSQoJ3BvbGljeVRoZW1lcycpOwogIGlmKCFib3gpcmV0dXJuOwogIGNvbnN0IHRoZW1lcz0ocCYmcC5tYXRjaGVkX3RoZW1lcyl8fFtdOwogIGlmKCF0aGVtZXMubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxkaXYgY2xhc3M9InBvbGljeXRoZW1lIj48Yj7plqLpgKPjg4bjg7zjg57jgarjgZcgLyDliKTlrprkv53nlZk8L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7mnIDkvY7plqLpgKPluqbjgpLmuoDjgZ/jgZnlhazlvI/mlL/nrZbjg4bjg7zjg57jgYzjgYLjgorjgb7jgZvjgpPjgII8L3NwYW4+PC9kaXY+JzsKICAgIHJldHVybjsKICB9CiAgYm94LmlubmVySFRNTD10aGVtZXMuc2xpY2UoMCw0KS5tYXAodD0+YAogICAgPGRpdiBjbGFzcz0icG9saWN5dGhlbWUiPgogICAgICA8Yj4ke3QubmFtZX0gLyDplqLpgKPluqYgJHsoTnVtYmVyKHQucmVsZXZhbmNlKSoxMDApLnRvRml4ZWQoMCl9JTwvYj4KICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mlL/nrZblvLfluqYgJHtOdW1iZXIodC5wb2xpY3lfc3RyZW5ndGgpLnRvRml4ZWQoMCl9IC8g5a+E5LiOICR7TnVtYmVyKHQuY29udHJpYnV0aW9uKS50b0ZpeGVkKDEpfSAvICR7cG9saWN5U291cmNlU3RhdHVzSmEodC5zb3VyY2Vfc3RhdHVzKX08L3NwYW4+PGJyPgogICAgICA8YSBocmVmPSIke3QudXJsfSIgdGFyZ2V0PSJfYmxhbmsiIHJlbD0ibm9vcGVuZXIiPuWFrOW8j+OCveODvOOCuTwvYT4KICAgIDwvZGl2PgogIGApLmpvaW4oJycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRGdW5kYW1lbnRhbHMoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvZnVuZGFtZW50YWxzP2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCfmsbrnrpfjg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4geC5mdW5kYW1lbnRhbHN8fHt9Owp9CgoKZnVuY3Rpb24gbWFya2V0RnJlc2huZXNzRmFjdG9yKGRhdGVTdHIpewogIGNvbnN0IGY9ZnJlc2huZXNzRm9yKGRhdGVTdHIpOwogIGlmKGYubGV2ZWw9PT0nZnJlc2gnKXJldHVybiAxLjAwOwogIGlmKGYubGV2ZWw9PT0nd2FybmluZycpcmV0dXJuIDAuNzA7CiAgaWYoZi5sZXZlbD09PSdzdGFsZScpcmV0dXJuIDAuMjU7CiAgcmV0dXJuIDAuMjA7Cn0KCmZ1bmN0aW9uIGZyZXNobmVzc1BjdFRleHQodil7CiAgaWYodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKXJldHVybiAn4oCUJzsKICByZXR1cm4gTWF0aC5yb3VuZChOdW1iZXIodikqMTAwKSsnJSc7Cn0KCmZ1bmN0aW9uIGZpbmFuY2lhbEZyZXNobmVzc0xhYmVsKGxhYmVsKXsKICBpZihsYWJlbD09PSdmcmVzaCcpcmV0dXJuICfmlrDjgZfjgYQnOwogIGlmKGxhYmVsPT09J3NsaWdodGx5X29sZCcpcmV0dXJuICfjgoTjgoTlj6TjgYQnOwogIGlmKGxhYmVsPT09J29sZCcpcmV0dXJuICflj6TjgYQnOwogIGlmKGxhYmVsPT09J3Zlcnlfb2xkJylyZXR1cm4gJ+OBi+OBquOCiuWPpOOBhCc7CiAgaWYobGFiZWw9PT0nc3RhbGUnKXJldHVybiAn6Z2e5bi444Gr5Y+k44GEJzsKICByZXR1cm4gJ+mWi+ekuuaXpeS4jeaYjic7Cn0KCmZ1bmN0aW9uIHBvbGljeUZyZXNobmVzc0ZhY3RvcihwLG1hbnVhbE1vZGUpewogIGlmKG1hbnVhbE1vZGUpcmV0dXJuIDEuMDA7CiAgaWYoIXB8fHAuc2NvcmU9PT1udWxsfHxwLnNjb3JlPT09dW5kZWZpbmVkKXJldHVybiAwOwogIGNvbnN0IGM9TnVtYmVyKHAuY29uZmlkZW5jZV9wY3QpOwogIGlmKE51bWJlci5pc0Zpbml0ZShjKSlyZXR1cm4gTWF0aC5tYXgoMCxNYXRoLm1pbigxLGMvMTAwKSk7CiAgcmV0dXJuIDAuNTA7Cn0KCmZ1bmN0aW9uIHNjb3JlTGFiZWwodil7CiAgaWYodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKXJldHVybiAn4oCUJzsKICBjb25zdCBuPU51bWJlcih2KTsKICByZXR1cm4gKG4+MD8nKyc6JycpK24udG9GaXhlZCgxKTsKfQoKZnVuY3Rpb24gcGN0TWF5YmUodil7CiAgcmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDEpKyclJzsKfQoKYXN5bmMgZnVuY3Rpb24gYW5hbHl6ZSgpewogIGNvbnN0IGNvZGU9JCgnY29kZScpLnZhbHVlLnRyaW0oKTsKICBpZighY29kZSl7JCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+mKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOBrSc7cmV0dXJufQogIGNvbnN0IGJ0bj0kKCdhbmFseXplQnRuJyk7IGJ0bi5kaXNhYmxlZD10cnVlOyBidG4udGV4dENvbnRlbnQ9J+WPluW+l+S4reKApic7CiAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J0Zpbk1pbmTntIQ5MDDml6XkvqHmoLzlsaXmrbTvvItKLVF1YW50c+axuueul+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBhOOBvuOBmeKApic7CgogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICAkKCdwcmljZScpLnZhbHVlPXMubGFzdF9jbG9zZT09bnVsbD8nJzpmbXQocy5sYXN0X2Nsb3NlLDEpOwogICAgJCgncjIwJykudmFsdWU9Zm10KHMucmV0dXJuXzIwZCk7JCgncjEyNicpLnZhbHVlPWZtdChzLnJldHVybl8xMjZkKTskKCdyMjUyJykudmFsdWU9Zm10KHMucmV0dXJuXzI1MmQpOwogICAgJCgnaGlnaDIwJykudGV4dENvbnRlbnQ9Zm10KHMuaGlnaF8yMGQsMSk7JCgnbG93MjAnKS50ZXh0Q29udGVudD1mbXQocy5sb3dfMjBkLDEpOwogICAgJCgndm9sMjAnKS50ZXh0Q29udGVudD1zLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQ9PW51bGw/J+KAlCc6Zm10KHMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZCkrJyUnOwoKICAgIGRpc3BsYXlDb21wYW55KCQoJ2NvbXBhbnlOYW1lJykscS5jb21wYW55fHxudWxsLCcnKTsKICAgIGNvbnN0IHByaWNlU291cmNlPXMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8J+S4jeaYjic7CiAgICBjb25zdCBoaXN0b3J5RGF0ZT1zLmhpc3RvcnlfbGFzdF9kYXRlfHxudWxsOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPQogICAgICAnPHNwYW4gY2xhc3M9InNvdXJjZWJhZGdlIj7nj77lnKjlgKQ8L3NwYW4+PGIgY2xhc3M9Im9rIj4nK3ByaWNlU291cmNlKyc8L2I+JysKICAgICAgJzxicj7nj77lnKjlgKTjg4fjg7zjgr/ml6U6ICcrKHMubGFzdF9kYXRlfHwn4oCUJykrCiAgICAgIChzLnByaWNlX3RpbWU/JyAnK3MucHJpY2VfdGltZTonJykrCiAgICAgICcgLyDmnIDmlrDlj5blvpflgKQ6ICcrZm10KHMubGFzdF9jbG9zZSwxKSsKICAgICAgJzxicj48c3BhbiBjbGFzcz0ic291cmNlYmFkZ2UiPuS+oeagvOWxpeattDwvc3Bhbj4nKwogICAgICAocy5oaXN0b3J5X3NvdXJjZXx8J+WPluW+l+OBquOBlycpKwogICAgICAnIC8g5pyA57WC5pelOiAnKyhoaXN0b3J5RGF0ZXx8J+KAlCcpKwogICAgICAnIC8g5bGl5q2044K144Oz44OX44OrOiAnKyhzLnNhbXBsZV9jb3VudD8/MCkrJ+S7tic7CiAgICBzaG93RnJlc2huZXNzKHMubGFzdF9kYXRlKTsKICAgIHNob3dIaXN0b3J5RnJlc2huZXNzKGhpc3RvcnlEYXRlLHMubGFzdF9kYXRlKTsKCiAgICBjb25zdCBtYXJrZXRGcmVzaG5lc3M9bWFya2V0RnJlc2huZXNzRmFjdG9yKGhpc3RvcnlEYXRlfHxzLmxhc3RfZGF0ZSk7CiAgICAkKCdtYXJrZXRGcmVzaCcpLnRleHRDb250ZW50PWZyZXNobmVzc1BjdFRleHQobWFya2V0RnJlc2huZXNzKTsKICAgIGNvbnN0IG1hcmtldEFnZT1kYXRhQWdlRGF5cyhoaXN0b3J5RGF0ZXx8cy5sYXN0X2RhdGUpOwogICAgJCgnbWFya2V0RnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgYOS+oeagvOWxpeattCAkeyhoaXN0b3J5RGF0ZXx8cy5sYXN0X2RhdGV8fCfigJQnKX0gLyAke21hcmtldEFnZT09PW51bGw/J+aXpeaVsOS4jeaYjic6bWFya2V0QWdlKyfml6UnfWA7CgogICAgY29uc3Qgc3luY2VkSG9sZGluZz1zeW5jQW5hbHl6ZWRRdW90ZVRvSG9sZGluZyhjb2RlLHEpOwogICAgaWYoc3luY2VkSG9sZGluZyl7CiAgICAgIGNvbnN0IHN0YXR1cz0kKCdob2xkaW5nUmVmcmVzaFN0YXR1cycpOwogICAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWAke2NvZGV9IOOBruS/neacieagquOCkuWIhuaekOaZguOBruacgOaWsOWPluW+l+e1guWApCAke2ZtdChzLmxhc3RfY2xvc2UsMSl9IOOBp+WGjeioiOeul+OBl+OBvuOBl+OBn+OAgmA7CiAgICB9CgogICAgJCgnYW5hbHlzaXNFdicpLmlubmVySFRNTD0KICAgICAgZXZIdG1sKCfnn63mnJ8yMOaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjBkJ10pKwogICAgICBldkh0bWwoJ+S4reacnzEyNuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMTI2ZCddKSsKICAgICAgZXZIdG1sKCfplbfmnJ8yNTLml6UnLHMuZm9yd2FyZF9yZXR1cm5fc3RhdHMmJnMuZm9yd2FyZF9yZXR1cm5fc3RhdHNbJzI1MmQnXSk7CgogICAgY29uc3Qgc3VwcGx5PXMuc3VwcGx5X3Byb3h5fHx7fTsKICAgICQoJ3N1cHBseUF1dG8nKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKHN1cHBseS5zY29yZSk7CiAgICAkKCdzdXBwbHlEZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgJzXml6UvMjDml6Xlh7rmnaXpq5ggJysoc3VwcGx5LnZvbHVtZV9yYXRpb181XzIwPT1udWxsPyfigJQnOk51bWJlcihzdXBwbHkudm9sdW1lX3JhdGlvXzVfMjApLnRvRml4ZWQoMikrJ+WAjScpOwoKICAgIGxldCBmdW5kYW1lbnRhbHM9e307CiAgICB0cnl7CiAgICAgIGZ1bmRhbWVudGFscz1hd2FpdCBnZXRGdW5kYW1lbnRhbHMoY29kZSk7CiAgICAgICQoJ2Vhcm5BdXRvJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChmdW5kYW1lbnRhbHMuc2NvcmUpOwogICAgICBjb25zdCBtPWZ1bmRhbWVudGFscy5tZXRyaWNzfHx7fTsKICAgICAgY29uc3QgZWY9ZnVuZGFtZW50YWxzLmZyZXNobmVzc3x8e307CiAgICAgICQoJ2Vhcm5EZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgICAn6ZaL56S6ICcrKChmdW5kYW1lbnRhbHMubGF0ZXN0JiZmdW5kYW1lbnRhbHMubGF0ZXN0LmRhdGUpfHwn4oCUJykrCiAgICAgICAgJyAvIOWjsuS4iiAnK3BjdE1heWJlKG0uc2FsZXNfZ3Jvd3RoX3BjdCkrCiAgICAgICAgJyAvIOWWtualreebiiAnK3BjdE1heWJlKG0ub3BfZ3Jvd3RoX3BjdCk7CiAgICAgICQoJ2Vhcm5GcmVzaCcpLnRleHRDb250ZW50PQogICAgICAgIGVmLmZhY3Rvcj09PXVuZGVmaW5lZD8n4oCUJzpNYXRoLnJvdW5kKE51bWJlcihlZi5mYWN0b3IpKjEwMCkrJyUnOwogICAgICAkKCdlYXJuRnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgICBmaW5hbmNpYWxGcmVzaG5lc3NMYWJlbChlZi5sYWJlbCkrCiAgICAgICAgJyAvICcrKGVmLmFnZV9kYXlzPT09bnVsbHx8ZWYuYWdlX2RheXM9PT11bmRlZmluZWQ/J+mWi+ekuuaXpeS4jeaYjic6ZWYuYWdlX2RheXMrJ+aXpeWJjScpOwogICAgfWNhdGNoKGZlKXsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD0n5LiN5piOJzsKICAgICAgJCgnZWFybkRldGFpbCcpLnRleHRDb250ZW50PSfjgZPjga7jg5fjg6njg7Mv6YqY5p+E44Gn44Gv5Y+W5b6X44Gn44GN44Gq44GE5Y+v6IO95oCn44GC44KKJzsKICAgICAgJCgnZWFybkZyZXNoJykudGV4dENvbnRlbnQ9J+KAlCc7CiAgICAgICQoJ2Vhcm5GcmVzaERldGFpbCcpLnRleHRDb250ZW50PSfmsbrnrpfjg4fjg7zjgr/jgarjgZcnOwogICAgICBmdW5kYW1lbnRhbHM9e3Njb3JlOm51bGwsZnJlc2huZXNzOntmYWN0b3I6MH19OwogICAgfQoKICAgIGxldCBhdXRvUG9saWN5PXtzY29yZTpudWxsLG1hdGNoZWRfdGhlbWVzOltdfTsKICAgIHRyeXsKICAgICAgYXV0b1BvbGljeT1hd2FpdCBnZXRQb2xpY3koY29kZSk7CiAgICAgICQoJ3BvbGljeVN0YXRlJykudGV4dENvbnRlbnQ9YXV0b1BvbGljeS5zY29yZT09bnVsbD8n5LiN5piOJzpzY29yZUxhYmVsKGF1dG9Qb2xpY3kuc2NvcmUpOwogICAgICAkKCdwb2xpY3lEZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgICBhdXRvUG9saWN5LnNjb3JlPT1udWxsCiAgICAgICAgICA/ICfplqLpgKPjgZnjgovlhazlvI/mlL/nrZbjg4bjg7zjg57jgarjgZcnCiAgICAgICAgICA6ICfoh6rli5Xlm73nrZZwcm94eSAvIOS/oemgvOW6piAnKyhhdXRvUG9saWN5LmNvbmZpZGVuY2VfcGN0Pz8n4oCUJykrJyUgLyAnKygoYXV0b1BvbGljeS5tYXRjaGVkX3RoZW1lc3x8W10pLmxlbmd0aCkrJ+ODhuODvOODnic7CiAgICAgIGNvbnN0IHBmPXBvbGljeUZyZXNobmVzc0ZhY3RvcihhdXRvUG9saWN5LGZhbHNlKTsKICAgICAgJCgncG9saWN5RnJlc2gnKS50ZXh0Q29udGVudD1mcmVzaG5lc3NQY3RUZXh0KHBmKTsKICAgICAgJCgncG9saWN5RnJlc2hEZXRhaWwnKS50ZXh0Q29udGVudD0KICAgICAgICBhdXRvUG9saWN5LnNjb3JlPT1udWxsCiAgICAgICAgICA/ICfplqLpgKPjg4bjg7zjg57jgarjgZcnCiAgICAgICAgICA6ICgoYXV0b1BvbGljeS5tYXRjaGVkX3RoZW1lc3x8W10pLnNvbWUodD0+dC5zb3VyY2Vfc3RhdHVzPT09J3ZlcmlmaWVkX2xpdmUnKQogICAgICAgICAgICAgID8gJ+WFrOW8j+ODmuODvOOCuOOCkuODqeOCpOODlueiuuiqjScKICAgICAgICAgICAgICA6ICfnorroqo3muIjjgb/lhazlvI/jgr3jg7zjgrnjgpLkvb/nlKgnKTsKICAgICAgcmVuZGVyUG9saWN5VGhlbWVzKGF1dG9Qb2xpY3kpOwogICAgfWNhdGNoKHBlKXsKICAgICAgJCgncG9saWN5U3RhdGUnKS50ZXh0Q29udGVudD0n5LiN5piOJzsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9J+WFrOW8j+aUv+etluOCveODvOOCueWPluW+l+OCqOODqeODvCc7CiAgICAgICQoJ3BvbGljeUZyZXNoJykudGV4dENvbnRlbnQ9J+KAlCc7CiAgICAgICQoJ3BvbGljeUZyZXNoRGV0YWlsJykudGV4dENvbnRlbnQ9J+WPluW+l+OCqOODqeODvCc7CiAgICAgICQoJ3BvbGljeVRoZW1lcycpLmlubmVySFRNTD0nJzsKICAgICAgYXV0b1BvbGljeT17c2NvcmU6bnVsbCxtYXRjaGVkX3RoZW1lczpbXX07CiAgICB9CgogICAgY29uc3QgbWFudWFsUG9saWN5PXZhbCgncG9saWN5Jyk7CiAgICBjb25zdCBwb2xpY3lTY29yZT1tYW51YWxQb2xpY3k9PT1udWxsP2F1dG9Qb2xpY3kuc2NvcmU6bWFudWFsUG9saWN5OwogICAgY29uc3QgcG9saWN5TW9kZT1tYW51YWxQb2xpY3k9PT1udWxsPydhdXRvJzonbWFudWFsJzsKICAgIGlmKG1hbnVhbFBvbGljeSE9PW51bGwpewogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PXNjb3JlTGFiZWwobWFudWFsUG9saWN5KTsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9J+aJi+WFpeWKm+OBp+iHquWLleWApOOCkuS4iuabuOOBjSc7CiAgICAgICQoJ3BvbGljeUZyZXNoJykudGV4dENvbnRlbnQ9JzEwMCUnOwogICAgICAkKCdwb2xpY3lGcmVzaERldGFpbCcpLnRleHRDb250ZW50PSfjg6bjg7zjgrbjg7zmiYvlhaXlipvlgKQnOwogICAgfQoKICAgIGNvbnN0IGQ9ewogICAgICBjb2RlLAogICAgICBwcmljZTpzLmxhc3RfY2xvc2UsCiAgICAgIHJldHVybjIwOnMucmV0dXJuXzIwZCwKICAgICAgcmV0dXJuMTI2OnMucmV0dXJuXzEyNmQsCiAgICAgIHJldHVybjI1MjpzLnJldHVybl8yNTJkLAogICAgICBlYXJuaW5nc19zY29yZTpmdW5kYW1lbnRhbHMuc2NvcmUsCiAgICAgIHBvbGljeV9zY29yZTpwb2xpY3lTY29yZSwKICAgICAgcG9saWN5X21vZGU6cG9saWN5TW9kZSwKICAgICAgc3VwcGx5X3Njb3JlOnN1cHBseS5zY29yZSwKICAgICAgbWFya2V0X2ZyZXNobmVzczptYXJrZXRGcmVzaG5lc3MsCiAgICAgIGVhcm5pbmdzX2ZyZXNobmVzczpOdW1iZXIoCiAgICAgICAgZnVuZGFtZW50YWxzLmZyZXNobmVzcyYmZnVuZGFtZW50YWxzLmZyZXNobmVzcy5mYWN0b3IhPT11bmRlZmluZWQKICAgICAgICAgID8gZnVuZGFtZW50YWxzLmZyZXNobmVzcy5mYWN0b3IKICAgICAgICAgIDogKGZ1bmRhbWVudGFscy5zY29yZT09bnVsbD8wOjEpCiAgICAgICksCiAgICAgIHBvbGljeV9mcmVzaG5lc3M6cG9saWN5RnJlc2huZXNzRmFjdG9yKAogICAgICAgIGF1dG9Qb2xpY3ksCiAgICAgICAgbWFudWFsUG9saWN5IT09bnVsbAogICAgICApCiAgICB9OwoKICAgIGNvbnN0IGFyPWF3YWl0IGZldGNoKCcvYXBpL2ZyZWUvYW5hbHl6ZScsewogICAgICBtZXRob2Q6J1BPU1QnLAogICAgICBoZWFkZXJzOnsnQ29udGVudC1UeXBlJzonYXBwbGljYXRpb24vanNvbid9LAogICAgICBib2R5OkpTT04uc3RyaW5naWZ5KGQpLAogICAgICBjYWNoZTonbm8tc3RvcmUnCiAgICB9KTsKICAgIGNvbnN0IHg9YXdhaXQgYXIuanNvbigpOwogICAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+WIhuaekOODh+ODvOOCv+OBjOS4jei2s+OBl+OBpuOBhOOBvuOBmScpOwoKICAgICQoJ3N0YXRlJykudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7CiAgICAkKCdwb3MnKS50ZXh0Q29udGVudD14LnNpZ25hbC5wb3NpdGl2ZV9jb3VudDsKICAgICQoJ25lZycpLnRleHRDb250ZW50PXguc2lnbmFsLm5lZ2F0aXZlX2NvdW50OwoKICAgIGNvbnN0IHNjPXguc2NvcmV8fHt9OwogICAgJCgnc2NvcmVIZXJvJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmUxMDAnKS50ZXh0Q29udGVudD1zYy5zY29yZTEwMD09bnVsbD8n4oCUJzpzYy5zY29yZTEwMCsnIC8gMTAwJzsKCiAgICBjb25zdCBwaWxsPSQoJ3Njb3JlU3RhdGVQaWxsJyk7CiAgICBwaWxsLmNsYXNzTmFtZT0nc3RhdGVwaWxsICcrc3RhdGVDbGFzcyh4LnNpZ25hbC5zdGF0ZSk7CiAgICBwaWxsLnRleHRDb250ZW50PXN0YXRlSmEoeC5zaWduYWwuc3RhdGUpOwoKICAgICQoJ2NvdmVyYWdlJykudGV4dENvbnRlbnQ9c2MuY292ZXJhZ2VfcGN0PT1udWxsPyfigJQnOnNjLmNvdmVyYWdlX3BjdCsnJSc7CiAgICAkKCdmcmVzaENvdmVyYWdlJykudGV4dENvbnRlbnQ9c2MuY292ZXJhZ2VfcGN0PT1udWxsPyfigJQnOnNjLmNvdmVyYWdlX3BjdCsnJSc7CiAgICBjb25zdCBzZj1zYy5mcmVzaG5lc3N8fHt9OwogICAgaWYoc2YubWFya2V0X3BjdCE9PXVuZGVmaW5lZCkkKCdtYXJrZXRGcmVzaCcpLnRleHRDb250ZW50PU1hdGgucm91bmQoTnVtYmVyKHNmLm1hcmtldF9wY3QpKSsnJSc7CiAgICBpZihzZi5lYXJuaW5nc19wY3QhPT11bmRlZmluZWQpJCgnZWFybkZyZXNoJykudGV4dENvbnRlbnQ9TWF0aC5yb3VuZChOdW1iZXIoc2YuZWFybmluZ3NfcGN0KSkrJyUnOwogICAgaWYoc2YucG9saWN5X3BjdCE9PXVuZGVmaW5lZCkkKCdwb2xpY3lGcmVzaCcpLnRleHRDb250ZW50PU1hdGgucm91bmQoTnVtYmVyKHNmLnBvbGljeV9wY3QpKSsnJSc7CgogICAgJCgnc2NvcmVCcmVha2Rvd24nKS50ZXh0Q29udGVudD0KICAgICAgJ+ODhuOCr+ODi+OCq+ODqyAnK3Njb3JlTGFiZWwoc2MudGVjaG5pY2FsKSsKICAgICAgJyAvIOaxuueulyAnK3Njb3JlTGFiZWwoc2MuZWFybmluZ3MpKwogICAgICAnIC8g6ZyA57WmICcrc2NvcmVMYWJlbChzYy5zdXBwbHkpKwogICAgICAnIC8g5Zu9562WcHJveHkgJytzY29yZUxhYmVsKHNjLnBvbGljeSk7CgogICAgJCgnc2NvcmVSZWFzb24nKS5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgICAkKCdzY29yZVJlYXNvblRleHQnKS50ZXh0Q29udGVudD1kcml2ZXJTZW50ZW5jZShzYyk7CiAgICAkKCdkcml2ZXJHcmlkJykuaW5uZXJIVE1MPQogICAgICBkcml2ZXJCb3hIdG1sKCfmnIDlpKfjga7jg5fjg6njgrnopoHlm6AnLHNjLnN0cm9uZ2VzdF9wb3NpdGl2ZSwncG9zaXRpdmUnKSsKICAgICAgZHJpdmVyQm94SHRtbCgn5pyA5aSn44Gu44Oe44Kk44OK44K56KaB5ZugJyxzYy5zdHJvbmdlc3RfbmVnYXRpdmUsJ25lZ2F0aXZlJyk7CgogICAgcmVuZGVyQ29udHJpYnV0aW9ucyhzYyk7CgogICAgY29uc3QgZnJlc2g9ZnJlc2huZXNzRm9yKHMubGFzdF9kYXRlKTsKICAgIGNvbnN0IGhpc3RvcnlGcmVzaD1mcmVzaG5lc3NGb3Iocy5oaXN0b3J5X2xhc3RfZGF0ZXx8cy5sYXN0X2RhdGUpOwogICAgbGV0IHJlc3VsdFRleHQ9CiAgICAgICFmcmVzaC5kZWNpc2lvbl9vawogICAgICAgID8gJ+KaoO+4jyDnj77lnKjlgKTjg4fjg7zjgr/jgYzlj6TjgYTjgZ/jgoHjgIHku4rml6Xjga7lo7LosrfliKTmlq3jgajjgZfjgabjga/kvb/nlKjjgZfjgb7jgZvjgpPjgIInCiAgICAgICAgOiAhaGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rCiAgICAgICAgICA/ICfimqDvuI8g54++5Zyo5YCk44Gv5paw44GX44GE5pel6Laz44KS5L2/44Gj44Gm44GE44G+44GZ44GM44CB5L6h5qC85bGl5q2044GM5Y+k44GE44Gf44KB57eP5ZCI54K544O755+t5Lit6ZW35pyf44OI44Os44Oz44OJ44O75pyf5b6F5YCk44Gv5Y+C6ICD5YCk44Gn44GZ44CCJwogICAgICAgICAgOiAn54++5Zyo5YCk44Go5L6h5qC85bGl5q2044Gu6a6u5bqm44KS56K66KqN5riI44G/44CCJzsKCiAgICBjb25zdCBlZmFjdG9yPU51bWJlcigKICAgICAgZnVuZGFtZW50YWxzLmZyZXNobmVzcyYmZnVuZGFtZW50YWxzLmZyZXNobmVzcy5mYWN0b3IhPT11bmRlZmluZWQKICAgICAgICA/IGZ1bmRhbWVudGFscy5mcmVzaG5lc3MuZmFjdG9yCiAgICAgICAgOiAxCiAgICApOwogICAgaWYoZnVuZGFtZW50YWxzLnNjb3JlIT09bnVsbCYmZWZhY3RvcjwxKXsKICAgICAgcmVzdWx0VGV4dCs9YCDmsbrnrpfjga/plovnpLrjgYvjgonmmYLplpPjgYzntYzjgaPjgabjgYTjgovjgZ/jgoHjgIHnt4/lkIjngrnjgafjga/ln7rmupbph43jgb8zMCXjgavprq7luqYke01hdGgucm91bmQoZWZhY3RvcioxMDApfSXjgpLmjpvjgZHjgablvbHpn7/jgpLlvLHjgoHjgabjgYTjgb7jgZnjgIJgOwogICAgfQogICAgaWYocG9saWN5TW9kZT09PSdhdXRvJyYmcG9saWN5U2NvcmUhPT1udWxsKXsKICAgICAgY29uc3QgcGY9cG9saWN5RnJlc2huZXNzRmFjdG9yKGF1dG9Qb2xpY3ksZmFsc2UpOwogICAgICBpZihwZjwxKXsKICAgICAgICByZXN1bHRUZXh0Kz1gIOWbveetlnByb3h544KC44K944O844K556K66KqN54q25oWL44Gr5b+c44GY44Gm6a6u5bqmJHtNYXRoLnJvdW5kKHBmKjEwMCl9JeOBp+mHjeOBv+iqv+aVtOOBl+OBpuOBhOOBvuOBmeOAgmA7CiAgICAgIH0KICAgIH0KICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PXJlc3VsdFRleHQ7CiAgfWNhdGNoKGUpewogICAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+KaoO+4jyAnK2UubWVzc2FnZTsKICAgICQoJ3NvdXJjZUJveCcpLmlubmVySFRNTD0nPHNwYW4gY2xhc3M9ImVyciI+5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSsnPC9zcGFuPic7CiAgfWZpbmFsbHl7CiAgICBidG4uZGlzYWJsZWQ9ZmFsc2U7CiAgICBidG4udGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCv+OBp+WIhuaekCc7CiAgfQp9CgpyZW5kZXJIb2xkaW5ncygpO3JlbmRlcldhdGNoKCk7cmVuZGVyTW92ZW1lbnQoKTtyZW5kZXJEYWlseUFkdmljZSgpOwpzZXRJbnRlcnZhbCgoKT0+e3JlbmRlckRhaWx5QWR2aWNlKCk7cmVuZGVyUmFkZW5OZXdzKCl9LDYwMDAwKTsKcmVuZGVyUmFkZW5OZXdzKCk7bG9hZFJhZGVuTmV3cygpO3NldEludGVydmFsKGxvYWRSYWRlbk5ld3MsMzAqNjAwMDApOwpkb2N1bWVudC5hZGRFdmVudExpc3RlbmVyKCd2aXNpYmlsaXR5Y2hhbmdlJywoKT0+e2lmKCFkb2N1bWVudC5oaWRkZW4pcmVuZGVyRGFpbHlBZHZpY2UoKX0pOwp3aW5kb3cuYWRkRXZlbnRMaXN0ZW5lcignc3RvcmFnZScscmVuZGVyRGFpbHlBZHZpY2UpOwpzZXRUaW1lb3V0KCgpPT5yZWZyZXNoQWxsSG9sZGluZ3MoZmFsc2UpLDQwMCk7CmlmKCdzZXJ2aWNlV29ya2VyJyBpbiBuYXZpZ2F0b3Ipe25hdmlnYXRvci5zZXJ2aWNlV29ya2VyLmdldFJlZ2lzdHJhdGlvbnMoKS50aGVuKHJzPT5Qcm9taXNlLmFsbChycy5tYXAocj0+ci51bnJlZ2lzdGVyKCkpKSkuY2F0Y2goKCk9Pnt9KX0KaWYoJ2NhY2hlcycgaW4gd2luZG93KXtjYWNoZXMua2V5cygpLnRoZW4oa2V5cz0+UHJvbWlzZS5hbGwoa2V5cy5tYXAoaz0+Y2FjaGVzLmRlbGV0ZShrKSkpKS5jYXRjaCgoKT0+e30pfQo8L3NjcmlwdD4KPC9tYWluPgo8L2JvZHk+CjwvaHRtbD4='
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
