from flask import Flask, jsonify, request, Response
import sqlite3, os, math, statistics, json, base64, csv, io
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

APP=Flask(__name__)
DB=os.path.join(os.path.dirname(__file__),'events.db')
VERSION='FREE-MOBILE-1.17-FINMIND-HISTORY'
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
    "PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9ImphIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPG1ldGEgaHR0cC1lcXVpdj0iQ29udGVudC1UeXBlIiBjb250ZW50PSJ0ZXh0L2h0bWw7IGNoYXJzZXQ9dXRmLTgiPgo8bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSx2aWV3cG9ydC1maXQ9Y292ZXIiPgo8bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzExMTgyNyI+Cjx0aXRsZT7ml6XmnKzmoKpBSSBGUkVFPC90aXRsZT4KPHN0eWxlPgoqe2JveC1zaXppbmc6Ym9yZGVyLWJveH0KYm9keXttYXJnaW46MDtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzExMTgyNztmb250LWZhbWlseTpzeXN0ZW0tdWksLWFwcGxlLXN5c3RlbSwiTm90byBTYW5zIEpQIixzYW5zLXNlcmlmfQptYWlue21heC13aWR0aDo3NjBweDttYXJnaW46YXV0bztwYWRkaW5nOjEycHggMTJweCA4MHB4fQoudG9we2JhY2tncm91bmQ6IzExMTgyNztjb2xvcjojZmZmO3BhZGRpbmc6MTVweDtib3JkZXItcmFkaXVzOjAgMCAxOHB4IDE4cHg7cG9zaXRpb246c3RpY2t5O3RvcDowO3otaW5kZXg6M30KaDF7Zm9udC1zaXplOjIxcHg7bWFyZ2luOjB9IGgze21hcmdpbjo0cHggMCAxMnB4fQouc3ViLC5tdXRlZHtmb250LXNpemU6MTJweDtjb2xvcjojNmI3MjgwfS50b3AgLnN1Yntjb2xvcjojZDFkNWRifQouY2FyZHtiYWNrZ3JvdW5kOiNmZmY7Ym9yZGVyLXJhZGl1czoxNnB4O3BhZGRpbmc6MTRweDttYXJnaW46MTBweCAwO2JveC1zaGFkb3c6MCAycHggMTBweCAjMDAwMX0KLmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHh9LmdyaWQze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDMsMWZyKTtnYXA6N3B4fQppbnB1dCxzZWxlY3QsYnV0dG9uLGEuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo0NnB4O2JvcmRlci1yYWRpdXM6MTFweDtmb250LXNpemU6MTVweDtwYWRkaW5nOjlweH0KaW5wdXQsc2VsZWN0e2JvcmRlcjoxcHggc29saWQgI2QxZDVkYjtiYWNrZ3JvdW5kOiNmZmZ9CmJ1dHRvbixhLmJ0bntib3JkZXI6MDtiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtmb250LXdlaWdodDo3MDA7dGV4dC1kZWNvcmF0aW9uOm5vbmU7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyfQouc2Vjb25kYXJ5e2JhY2tncm91bmQ6I2U1ZTdlYiFpbXBvcnRhbnQ7Y29sb3I6IzExMTgyNyFpbXBvcnRhbnR9Ci5kYW5nZXJ7YmFja2dyb3VuZDojZmVlMmUyIWltcG9ydGFudDtjb2xvcjojOTkxYjFiIWltcG9ydGFudH0KLmtwaXtiYWNrZ3JvdW5kOiNmOWZhZmI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttaW4td2lkdGg6MH0KLmtwaSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE4cHg7b3ZlcmZsb3ctd3JhcDphbnl3aGVyZX0KLnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjdweDttYXJnaW46N3B4IDB9Ci5iYWRnZXtkaXNwbGF5OmlubGluZS1ibG9jaztwYWRkaW5nOjRweCA4cHg7Ym9yZGVyLXJhZGl1czo5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTFweDttYXJnaW46MnB4fQoub2t7Y29sb3I6IzA0Nzg1N30ud2Fybntjb2xvcjojYjQ1MzA5fS5lcnJ7Y29sb3I6I2I5MWMxY30KLnNvdXJjZXtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDttYXJnaW4tdG9wOjhweH0KLmhvbGRpbmd7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTRweDtwYWRkaW5nOjEycHg7bWFyZ2luLXRvcDoxMHB4fQouaG9sZGluZy1oZWFke2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Z2FwOjhweH0KLmhvbGRpbmctaGVhZCBie2ZvbnQtc2l6ZToxOHB4fQoucG9ze2NvbG9yOiMwNDc4NTd9Lm5lZ3tjb2xvcjojYjkxYzFjfQouc21hbGxidG57bWluLWhlaWdodDozNnB4O3BhZGRpbmc6NnB4IDEwcHg7Zm9udC1zaXplOjEycHg7d2lkdGg6YXV0b30KLm5vdGV7YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7Zm9udC1zaXplOjEycHg7Y29sb3I6IzkyNDAwZX0KLmRlY2lzaW9ue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7bWFyZ2luLXRvcDo4cHg7Zm9udC13ZWlnaHQ6ODAwfQouZC1ob2xke2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDY1ZjQ2fQouZC13YXRjaHtiYWNrZ3JvdW5kOiNmZmZiZWI7Y29sb3I6IzkyNDAwZX0KLmQtdGFrZXtiYWNrZ3JvdW5kOiNlZmY2ZmY7Y29sb3I6IzFkNGVkOH0KLmQtc3RvcHtiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmV2e2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZXYgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxNnB4O21hcmdpbjoycHggMH0KCi5ldiBzbWFsbHtkaXNwbGF5OmJsb2NrO2NvbG9yOiM2YjcyODA7bGluZS1oZWlnaHQ6MS40NX0KLmdhdWdle2hlaWdodDo5cHg7YmFja2dyb3VuZDojZTVlN2ViO2JvcmRlci1yYWRpdXM6OTk5cHg7b3ZlcmZsb3c6aGlkZGVuO21hcmdpbi10b3A6NnB4fQouZ2F1Z2U+c3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQouYWN0aW9uYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjExcHg7bWFyZ2luLXRvcDo4cHg7YmFja2dyb3VuZDojZjlmYWZiO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94e2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjEwcHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnJhbmdlYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweDttYXJnaW46M3B4IDB9CgouZGlzdGFuY2V7Zm9udC13ZWlnaHQ6ODAwfQoucG9ydGZvbGlve2JhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjZmZmZmZmLCNmOGZhZmMpfQoucG9ydHJvd3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LDFmcik7Z2FwOjdweH0KLnBvcnRtaW5pe2JhY2tncm91bmQ6I2ZmZjtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6OXB4fQoucG9ydG1pbmkgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxN3B4O21hcmdpbi10b3A6MnB4fQouYWxsb2N7bWFyZ2luLXRvcDo4cHh9Ci5hbGxvY2JhcntoZWlnaHQ6MTBweDtiYWNrZ3JvdW5kOiNlNWU3ZWI7Ym9yZGVyLXJhZGl1czo5OTlweDtvdmVyZmxvdzpoaWRkZW59CgouYWxsb2NiYXIgc3BhbntkaXNwbGF5OmJsb2NrO2hlaWdodDoxMDAlO2JhY2tncm91bmQ6IzExMTgyNztib3JkZXItcmFkaXVzOjk5OXB4fQoucHJpb3JpdHktd3JhcHtkaXNwbGF5OmdyaWQ7Z2FwOjhweDttYXJnaW4tdG9wOjhweH0KLnByaW9yaXR5LWl0ZW17Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MTFweDtib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7YmFja2dyb3VuZDojZmZmfQoucHJpb3JpdHktaXRlbSBie2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjE2cHh9Ci5wcmlvcml0eS1oaWdoe2JhY2tncm91bmQ6I2ZlZjJmMjtib3JkZXItY29sb3I6I2ZlY2FjYX0KLnByaW9yaXR5LW1pZHtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyLWNvbG9yOiNmZGU2OGF9Ci5wcmlvcml0eS10YWtle2JhY2tncm91bmQ6I2VmZjZmZjtib3JkZXItY29sb3I6I2JmZGJmZX0KLnByaW9yaXR5LWluZm97YmFja2dyb3VuZDojZjhmYWZjfQoucHJpb3JpdHktZ29vZHtiYWNrZ3JvdW5kOiNlY2ZkZjU7Ym9yZGVyLWNvbG9yOiNhN2YzZDB9Ci5wcmlvcml0eS1yYW5re2ZvbnQtc2l6ZToxMXB4O2ZvbnQtd2VpZ2h0OjgwMDtsZXR0ZXItc3BhY2luZzouMDNlbX0KLnByaW9yaXR5LWxpbmV7ZGlzcGxheTpmbGV4O2p1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO2dhcDo4cHg7YWxpZ24taXRlbXM6ZmxleC1zdGFydH0KCi5wcmlvcml0eS1jb2Rle3doaXRlLXNwYWNlOm5vd3JhcDtmb250LXdlaWdodDo4MDB9Ci5mYWN0b3Jncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4fQouZmFjdG9ye2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYjtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo5cHg7YmFja2dyb3VuZDojZmZmfQouZmFjdG9yIGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MThweDttYXJnaW4tdG9wOjJweH0KLnNjb3JlaGVyb3tiYWNrZ3JvdW5kOiMxMTE4Mjc7Y29sb3I6I2ZmZjtib3JkZXItcmFkaXVzOjE2cHg7cGFkZGluZzoxNHB4O21hcmdpbi10b3A6MTBweH0KLnNjb3JlaGVybyAubXV0ZWR7Y29sb3I6I2QxZDVkYn0KCi5zY29yZWhlcm8gYntmb250LXNpemU6MzRweDtkaXNwbGF5OmJsb2NrO2xpbmUtaGVpZ2h0OjF9Ci5zY29yZS1yZWFzb257bWFyZ2luLXRvcDoxMHB4O3BhZGRpbmc6MTBweDtib3JkZXItcmFkaXVzOjEycHg7YmFja2dyb3VuZDojZjhmYWZjO2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLnNjb3JlLXJlYXNvbiBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0KLnN0YXRlcGlsbHtkaXNwbGF5OmlubGluZS1ibG9jaztib3JkZXItcmFkaXVzOjk5OXB4O3BhZGRpbmc6NXB4IDEwcHg7Zm9udC13ZWlnaHQ6ODAwO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6N3B4fQouc3RhdGUtc3Ryb25nLWJ1bGx7YmFja2dyb3VuZDojZGNmY2U3O2NvbG9yOiMxNjY1MzR9Ci5zdGF0ZS1idWxse2JhY2tncm91bmQ6I2VjZmRmNTtjb2xvcjojMDQ3ODU3fQouc3RhdGUtbmV1dHJhbHtiYWNrZ3JvdW5kOiNmM2Y0ZjY7Y29sb3I6IzM3NDE1MX0KLnN0YXRlLWJlYXJ7YmFja2dyb3VuZDojZmZmN2VkO2NvbG9yOiM5YTM0MTJ9Ci5zdGF0ZS1zdHJvbmctYmVhcntiYWNrZ3JvdW5kOiNmZWYyZjI7Y29sb3I6Izk5MWIxYn0KLmRyaXZlcmdyaWR7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyO2dhcDo4cHg7bWFyZ2luLXRvcDo4cHh9Ci5kcml2ZXJib3h7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmZmZ9CgouZHJpdmVyYm94IGJ7Zm9udC1zaXplOjE1cHg7bGluZS1oZWlnaHQ6MS4zfQoucG9saWN5dGhlbWVze2Rpc3BsYXk6Z3JpZDtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQoucG9saWN5dGhlbWV7Ym9yZGVyOjFweCBzb2xpZCAjZTVlN2ViO2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDtiYWNrZ3JvdW5kOiNmOGZhZmN9Ci5wb2xpY3l0aGVtZSBie2Rpc3BsYXk6YmxvY2t9CgoucG9saWN5dGhlbWUgYXtmb250LXNpemU6MTJweH0KLmNvbnRyaWJncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KDQsMWZyKTtnYXA6N3B4O21hcmdpbi10b3A6OHB4fQouY29udHJpYntib3JkZXI6MXB4IHNvbGlkICNlNWU3ZWI7Ym9yZGVyLXJhZGl1czoxMnB4O3BhZGRpbmc6MTBweDtiYWNrZ3JvdW5kOiNmZmZ9Ci5jb250cmliIHNwYW57ZGlzcGxheTpibG9ja30KLmNvbnRyaWIgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToxOHB4O21hcmdpbi10b3A6MnB4fQoKLmNvbnRyaWIgc21hbGx7ZGlzcGxheTpibG9jazttYXJnaW4tdG9wOjNweDtjb2xvcjojNmI3MjgwO2xpbmUtaGVpZ2h0OjEuMzV9Ci5mcmVzaGJveHtib3JkZXItcmFkaXVzOjE0cHg7cGFkZGluZzoxMXB4O21hcmdpbi10b3A6OXB4O2JvcmRlcjoxcHggc29saWQgI2U1ZTdlYn0KLmZyZXNoLW9re2JhY2tncm91bmQ6I2VjZmRmNTtib3JkZXItY29sb3I6I2E3ZjNkMH0KLmZyZXNoLXdhcm57YmFja2dyb3VuZDojZmZmYmViO2JvcmRlci1jb2xvcjojZmRlNjhhfQouZnJlc2gtc3RhbGV7YmFja2dyb3VuZDojZmVmMmYyO2JvcmRlci1jb2xvcjojZmVjYWNhfQoKLmZyZXNoYm94IGJ7ZGlzcGxheTpibG9jaztmb250LXNpemU6MTZweH0KLnNvdXJjZWJhZGdle2Rpc3BsYXk6aW5saW5lLWJsb2NrO3BhZGRpbmc6NHB4IDhweDtib3JkZXItcmFkaXVzOjk5OXB4O2JhY2tncm91bmQ6I2VlZjJmZjtmb250LXNpemU6MTJweDtmb250LXdlaWdodDo4MDA7bWFyZ2luLXJpZ2h0OjRweH0KLmhpc3Rvcnl3YXJue2JvcmRlci1yYWRpdXM6MTJweDtwYWRkaW5nOjlweDttYXJnaW4tdG9wOjdweDtiYWNrZ3JvdW5kOiNmZmZiZWI7Ym9yZGVyOjFweCBzb2xpZCAjZmRlNjhhfQoKCkBtZWRpYShtYXgtd2lkdGg6NTYwcHgpey5jb250cmliZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LmRyaXZlcmdyaWR7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn19CgpAbWVkaWEobWF4LXdpZHRoOjU2MHB4KXsuZmFjdG9yZ3JpZHtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn19CgoKQG1lZGlhKG1heC13aWR0aDo1NjBweCl7LnBvcnRyb3d7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnJ9fQoKCgpAbWVkaWEobWF4LXdpZHRoOjQ4MHB4KXsuZ3JpZDN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmciAxZnIgMWZyfS5rcGkgYntmb250LXNpemU6MTZweH19Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+CjxtYWluPgo8ZGl2IGNsYXNzPSJ0b3AiPgogIDxoMT7wn5OIIOaXpeacrOagqkFJIEZSRUU8L2gxPgogIDxkaXYgY2xhc3M9InN1YiI+RmluTWluZOS+oeagvOWxpeattCAvIOacgOaWsOe1guWApCAvIOS/neacieagquWGjeioiOeulyAvIEotUXVhbnRz5rG6566XPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfjq8g6YqY5p+E5YiG5p6QPC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkIj4KICAgIDxpbnB1dCBpZD0iY29kZSIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i6YqY5p+E44Kz44O844OJIOS+iyA3MjAzIj4KICAgIDxpbnB1dCBpZD0icHJpY2UiIHBsYWNlaG9sZGVyPSLlj5blvpfntYLlgKQiIHJlYWRvbmx5PgogIDwvZGl2PgogIDxkaXYgaWQ9ImNvbXBhbnlOYW1lIiBjbGFzcz0ic291cmNlIG11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPumKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBmeOCi+OBqOS8muekvuWQjeOCkuihqOekuuOBl+OBvuOBmTwvZGl2PgogIDxkaXYgY2xhc3M9InJvdyI+CiAgICA8YSBpZD0ia2FidXRhbiIgY2xhc3M9ImJ0biBzZWNvbmRhcnkiIHRhcmdldD0iX2JsYW5rIiByZWw9Im5vb3BlbmVyIj7moKrmjqLjgafnorroqo08L2E+CiAgICA8YnV0dG9uIGlkPSJhbmFseXplQnRuIiBvbmNsaWNrPSJhbmFseXplKCkiPuWun+ODh+ODvOOCv+OBp+WIhuaekDwvYnV0dG9uPgogIDwvZGl2PgogIDxwIGNsYXNzPSJtdXRlZCI+6YqY5p+E44Kz44O844OJ44KS5YWl44KM44Gm5oq844GZ44Go44CB54++5Zyo5YCk44GoMjDml6Xjg7sxMjbml6Xjg7syNTLml6Xjga7kvqHmoLzlsaXmrbTjga9GaW5NaW5k5pel5pys5qCq5pel6Laz44KS5pyA5YSq5YWI44GX44G+44GZ44CC6YqY5p+E5ZCN44O75rG6566X44O75Zu9562W5Yik5a6a44GvSi1RdWFudHPnrYnjgpLkvb/nlKjjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBpZD0ic291cmNlQm94IiBjbGFzcz0ic291cmNlIG11dGVkIj7jg4fjg7zjgr/mnKrlj5blvpc8L2Rpdj4KICA8ZGl2IGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9Im1hcmdpbi10b3A6N3B4Ij4KICAgIDxiPvCfhpMg54Sh5paZ5L6h5qC85bGl5q2044Gr44Gk44GE44GmPC9iPgogICAgPGRpdiBjbGFzcz0ibXV0ZWQiPkZpbk1pbmTjga7ml6XmnKzmoKrml6XotrPjgpLntIQ5MDDml6XliIblj5blvpfjgZfjgIHmnIDmlrDllrbmpa3ml6Xjga7ntYLlgKTjg7syMC8xMjYvMjUy5pel44Oq44K/44O844Oz44O7MjDml6Xpq5jlronjg7vlh7rmnaXpq5hwcm94eeODu+ODreODvOODquODs+OCsOWun+e4vuWIhuW4g+OCkuioiOeul+OBl+OBvuOBmeOAguWPluW8leS4reOBruODquOCouODq+OCv+OCpOODoOS+oeagvOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9Imhpc3RvcnlGcmVzaG5lc3NCb3giIGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8Yj7wn5OaIOS+oeagvOWIhuaekOWxpeattOOBrumuruW6pjwvYj4KICAgIDxkaXYgaWQ9Imhpc3RvcnlGcmVzaG5lc3NUZXh0IiBjbGFzcz0ibXV0ZWQiPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9ImZyZXNobmVzc0JveCIgY2xhc3M9ImZyZXNoYm94IGZyZXNoLXdhcm4iIHN0eWxlPSJkaXNwbGF5Om5vbmUiPgogICAgPGIgaWQ9ImZyZXNobmVzc1RpdGxlIj7jg4fjg7zjgr/prq7luqY8L2I+CiAgICA8ZGl2IGlkPSJmcmVzaG5lc3NEZXRhaWwiIGNsYXNzPSJtdXRlZCI+PC9kaXY+CiAgPC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfk4og5qCq5L6h44O744OG44Kv44OL44Kr44Or5a6f57i+PC9oMz4KICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MjDml6XpqLDokL3njocgJTwvc3Bhbj48aW5wdXQgaWQ9InIyMCIgcmVhZG9ubHk+PC9kaXY+CiAgICA8ZGl2PjxzcGFuIGNsYXNzPSJtdXRlZCI+MTI25pelICU8L3NwYW4+PGlucHV0IGlkPSJyMTI2IiByZWFkb25seT48L2Rpdj4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj4yNTLml6UgJTwvc3Bhbj48aW5wdXQgaWQ9InIyNTIiIHJlYWRvbmx5PjwvZGl2PgogIDwvZGl2PgogIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPjIw5pel6auY5YCkPC9zcGFuPjxiIGlkPSJoaWdoMjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeWuieWApDwvc3Bhbj48YiBpZD0ibG93MjAiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj4yMOaXpeW5tOeOh+ODnOODqTwvc3Bhbj48YiBpZD0idm9sMjAiPuKAlDwvYj48L2Rpdj4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+nqSDlrp/jg4fjg7zjgr/opoHlm6A8L2gzPgogIDxwIGNsYXNzPSJtdXRlZCI+5rG6566X44GvSi1RdWFudHPosqHli5njgrXjg57jg6rjg7zjgIHpnIDntabjga/lrp/moKrkvqHjg7vlh7rmnaXpq5hwcm94eeOAgeWbveetluOBr+aUv+W6nOWFrOW8j+aUv+etluOCveODvOOCue+8i+alreeori/npL7lkI3jga7plqLpgKPluqbjgYvjgonoh6rli5XmjqHngrnjgZfjgb7jgZnjgII8L3A+CiAgPGRpdiBjbGFzcz0iZmFjdG9yZ3JpZCI+CiAgICA8ZGl2IGNsYXNzPSJmYWN0b3IiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5rG6566XPC9zcGFuPjxiIGlkPSJlYXJuQXV0byI+4oCUPC9iPjxzbWFsbCBpZD0iZWFybkRldGFpbCIgY2xhc3M9Im11dGVkIj7mnKrlj5blvpc8L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPumcgOe1pnByb3h5PC9zcGFuPjxiIGlkPSJzdXBwbHlBdXRvIj7igJQ8L2I+PHNtYWxsIGlkPSJzdXBwbHlEZXRhaWwiIGNsYXNzPSJtdXRlZCI+5pyq5Y+W5b6XPC9zbWFsbD48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImZhY3RvciI+PHNwYW4gY2xhc3M9Im11dGVkIj7lm73nrZZwcm94eTwvc3Bhbj48YiBpZD0icG9saWN5U3RhdGUiPuKAlDwvYj48c21hbGwgaWQ9InBvbGljeURldGFpbCIgY2xhc3M9Im11dGVkIj7lhazlvI/mlL/nrZbjgr3jg7zjgrnnorroqo3liY08L3NtYWxsPjwvZGl2PgogICAgPGRpdiBjbGFzcz0iZmFjdG9yIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODh+ODvOOCv+WFhei2szwvc3Bhbj48YiBpZD0iY292ZXJhZ2UiPuKAlDwvYj48c21hbGwgY2xhc3M9Im11dGVkIj7nt4/lkIjmjqHngrnjgavkvb/jgYjjgZ/ph43jgb88L3NtYWxsPjwvZGl2PgogIDwvZGl2PgogIDxkaXYgaWQ9InBvbGljeVRoZW1lcyIgY2xhc3M9InBvbGljeXRoZW1lcyI+PC9kaXY+CiAgPGRpdiBzdHlsZT0ibWFyZ2luLXRvcDo5cHgiPgogICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lm73nrZbjgrnjgrPjgqLkuIrmm7jjgY3vvIjku7vmhI/vvIkgLTEwMOOAnDEwMDwvc3Bhbj4KICAgIDxpbnB1dCBpZD0icG9saWN5IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLnqbrmrITjgarjgonoh6rli5Xlm73nrZZwcm94eeOCkuS9v+eUqCI+CiAgPC9kaXY+CiAgPHAgY2xhc3M9Im11dGVkIj7igLvlm73nrZZwcm94eeOBr+OAgeaUv+W6nOWFrOW8j+aUv+etluOCveODvOOCueOBqEotUXVhbnRz44Gu5qWt56iu44O75Lya56S+5ZCN44Go44Gu6Zai6YCj5bqm44KS57WE44G/5ZCI44KP44Gb44Gf5Y+C6ICD5YCk44Gn44GZ44CC44Op44Kk44OW56K66KqN44Gn44GN44Gq44GE5aC05ZCI44Gv5pyA57WC56K66KqN5riI44G/5oOF5aCx44KS5L2O5L+h6aC85bqm44Gn5L2/55So44GX44CB44Gd44Gu54q25oWL44KC55S76Z2i44Gr5piO56S644GX44G+44GZ44CC6KOc5Yqp6YeR5o6h5oqe44KE5qWt57i+5oGp5oG144Gd44Gu44KC44Gu44KS6Ki85piO44GZ44KL5oyH5qiZ44Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn6egIOWIhuaekOe1kOaenDwvaDM+CiAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPueKtuaFizwvc3Bhbj48YiBpZD0ic3RhdGUiPuKAlDwvYj48L2Rpdj4KICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7jg5fjg6njgrnmoLnmi6A8L3NwYW4+PGIgaWQ9InBvcyI+4oCUPC9iPjwvZGl2PgogICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODnuOCpOODiuOCueagueaLoDwvc3Bhbj48YiBpZD0ibmVnIj7igJQ8L2I+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0ic2NvcmVIZXJvIiBjbGFzcz0ic2NvcmVoZXJvIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+57eP5ZCI5o6h54K577yI5Y+W5b6X44OH44O844K/44O744Or44O844Or44OZ44O844K577yJPC9zcGFuPgogICAgPGIgaWQ9InNjb3JlMTAwIj7igJQ8L2I+CiAgICA8c3BhbiBpZD0ic2NvcmVTdGF0ZVBpbGwiIGNsYXNzPSJzdGF0ZXBpbGwgc3RhdGUtbmV1dHJhbCI+4oCUPC9zcGFuPgogICAgPGRpdiBpZD0ic2NvcmVCcmVha2Rvd24iIGNsYXNzPSJtdXRlZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij48L2Rpdj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJzY29yZVJlYXNvbiIgY2xhc3M9InNjb3JlLXJlYXNvbiIgc3R5bGU9ImRpc3BsYXk6bm9uZSI+CiAgICA8c3Ryb25nPvCfp60g44Gq44Gc44GT44Gu54K55pWw77yfPC9zdHJvbmc+CiAgICA8ZGl2IGlkPSJzY29yZVJlYXNvblRleHQiIGNsYXNzPSJtdXRlZCI+4oCUPC9kaXY+CiAgICA8ZGl2IGlkPSJkcml2ZXJHcmlkIiBjbGFzcz0iZHJpdmVyZ3JpZCI+PC9kaXY+CiAgPC9kaXY+CiAgPGRpdiBpZD0iY29udHJpYnV0aW9uQm94IiBjbGFzcz0ic2NvcmUtcmVhc29uIiBzdHlsZT0iZGlzcGxheTpub25lIj4KICAgIDxzdHJvbmc+8J+nriDnt4/lkIjngrnjgbjjga7lr4TkuI48L3N0cm9uZz4KICAgIDxkaXYgY2xhc3M9Im11dGVkIj7lj5blvpfjgafjgY3jgZ/opoHlm6DjgaDjgZHjgafph43jgb/jgpLlho3phY3liIbjgZfjgZ/lvozjgIHlkITopoHlm6DjgYznt4/lkIjoqZXkvqHjgpLjganjgozjgaDjgZHmirzjgZfkuIrjgZLvvI/mirzjgZfkuIvjgZLjgZ/jgYvjgpLooajnpLrjgZfjgb7jgZnjgII8L2Rpdj4KICAgIDxkaXYgaWQ9ImNvbnRyaWJ1dGlvbkdyaWQiIGNsYXNzPSJjb250cmliZ3JpZCI+PC9kaXY+CiAgPC9kaXY+CiAgPHAgaWQ9InJlc3VsdCIgY2xhc3M9Im11dGVkIj7pipjmn4TjgrPjg7zjg4njgpLlhaXlipvjgZfjgabjgIzlrp/jg4fjg7zjgr/jgafliIbmnpDjgI3jgpLmirzjgZfjgabjgY/jgaDjgZXjgYTjgII8L3A+CiAgPHAgY2xhc3M9Im11dGVkIj7mjqHngrnluK/vvJo4MOOAnDEwMCDlvLfmsJcgLyA2NeOAnDc5IOOChOOChOW8t+awlyAvIDQ144CcNjQg5Lit56uLIC8gMzDjgJw0NCDjgoTjgoTlvLHmsJcgLyAw44CcMjkg5byx5rCXPC9wPgogIDxkaXYgaWQ9ImFuYWx5c2lzRXYiIGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij48L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIj4KICA8aDM+8J+aqCDku4rml6Xjga7lhKrlhYjjgqLjgq/jgrfjg6fjg7M8L2gzPgogIDxkaXYgaWQ9InByaW9yaXR5QWN0aW9ucyI+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuS/neacieagquOCkueZu+mMsuOBmeOCi+OBqOOAgeWEquWFiOOBl+OBpueiuuiqjeOBmeOCi+mKmOafhOOCkuiHquWLleihqOekuuOBl+OBvuOBmeOAgjwvcD4KICA8L2Rpdj4KPC9kaXY+Cgo8ZGl2IGNsYXNzPSJjYXJkIHBvcnRmb2xpbyI+CiAgPGgzPvCfp60g44Od44O844OI44OV44Kp44Oq44Kq5YWo5L2TPC9oMz4KICA8ZGl2IGlkPSJwb3J0Zm9saW9TdW1tYXJ5Ij4KICAgIDxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go6Ieq5YuV6ZuG6KiI44GX44G+44GZ44CCPC9wPgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImNhcmQiPgogIDxoMz7wn5K8IOS/neacieagquODu+aQjeWIh+OCii/liKnnoro8L2gzPgogIDxkaXYgY2xhc3M9Im5vdGUiPgogICAg54++5Zyo5YCk44Go5L6h5qC85YiG5p6Q5bGl5q2044GvRmluTWluZOaXpei2s+OCkuWEquWFiOOBl+OBpuOAgeaQjeebiuODu+aQjeWIh+OCiui3nembouODu+WIqeeiuui3nembouODu+ODiOODrOODvOODquODs+OCsOODu+efreS4remVt+Wun+e4vuODu+ODneODvOODiOODleOCqeODquOCquipleS+oeOCkuiHquWLleWGjeioiOeul+OBl+OBvuOBmeOAgkZpbk1pbmTlj5blvpflpLHmlZfmmYLjgaDjgZHku5bjgr3jg7zjgrnjgbjjg5Xjgqnjg7zjg6vjg5Djg4Pjgq/jgZfjgb7jgZnjgIIKICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJyb3ciIHN0eWxlPSJtYXJnaW4tdG9wOjlweCI+CiAgICA8YnV0dG9uIGlkPSJyZWZyZXNoQWxsQnRuIiBjbGFzcz0ic2Vjb25kYXJ5IiBvbmNsaWNrPSJyZWZyZXNoQWxsSG9sZGluZ3ModHJ1ZSkiPuS/neacieagquOCkuacgOaWsOe1guWApOOBp+S4gOaLrOabtOaWsDwvYnV0dG9uPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRpbmdSZWZyZXNoU3RhdHVzIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46N3B4IDJweCAwIj7kv53mnInmoKrjga7oh6rli5Xmm7TmlrDjga8zMOWIhuOBlOOBqOOBq+acgOWkpzHlm57jgafjgZnjgII8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZENvZGUiIGlucHV0bW9kZT0ibnVtZXJpYyIgcGxhY2Vob2xkZXI9IumKmOafhOOCs+ODvOODiSI+CiAgICA8aW5wdXQgaWQ9ImhvbGRDb3N0IiBpbnB1dG1vZGU9ImRlY2ltYWwiIHBsYWNlaG9sZGVyPSLlj5blvpfljZjkvqEiPgogIDwvZGl2PgogIDxkaXYgaWQ9ImhvbGRDb21wYW55TmFtZSIgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luOjZweCAycHggMCI+6YqY5p+E5ZCN77ya4oCUPC9kaXY+CiAgPGRpdiBjbGFzcz0iZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxpbnB1dCBpZD0iaG9sZFNoYXJlcyIgaW5wdXRtb2RlPSJudW1lcmljIiBwbGFjZWhvbGRlcj0i5qCq5pWwIj4KICAgIDxzZWxlY3QgaWQ9ImZlZU1vZGUiPgogICAgICA8b3B0aW9uIHZhbHVlPSJub211cmFfbmV0Ij7ph47mnZHjgqrjg7Pjg6njgqTjg7PlsILnlKjmlK/lupfjg7vnj77niak8L29wdGlvbj4KICAgICAgPG9wdGlvbiB2YWx1ZT0ibm9uZSI+5omL5pWw5paZ44Gq44GX77yI5q+U6LyD55So77yJPC9vcHRpb24+CiAgICA8L3NlbGVjdD4KICA8L2Rpdj4KICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgIDxkaXY+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoogJTwvc3Bhbj48aW5wdXQgaWQ9InN0b3BQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjgiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWIqeeiuiAlPC9zcGFuPjxpbnB1dCBpZD0idGFrZVBjdCIgaW5wdXRtb2RlPSJkZWNpbWFsIiB2YWx1ZT0iMTUiPjwvZGl2PgogICAgPGRpdj48c3BhbiBjbGFzcz0ibXV0ZWQiPuODiOODrOODvOODqyAlPC9zcGFuPjxpbnB1dCBpZD0idHJhaWxQY3QiIGlucHV0bW9kZT0iZGVjaW1hbCIgdmFsdWU9IjciPjwvZGl2PgogIDwvZGl2PgogIDxidXR0b24gb25jbGljaz0iYWRkSG9sZGluZygpIiBzdHlsZT0ibWFyZ2luLXRvcDoxMHB4Ij7lrp/jg4fjg7zjgr/jgafoqIjnrpfjgZfjgabkv53lrZg8L2J1dHRvbj4KICA8cCBjbGFzcz0ibXV0ZWQiPumHjuadkeODjeODg+ODiO+8huOCs+ODvOODq++8j+OBu+OBo+OBqOODgOOCpOODrOOCr+ODiOOBruWbveWGheePvueJqeODu+OCquODs+ODqeOCpOODs+azqOaWh+OBrueojui+vOaJi+aVsOaWmeihqOOCkuS9v+eUqOOAgjwvcD4KICA8ZGl2IGlkPSJob2xkaW5ncyI+PC9kaXY+CjwvZGl2PgoKPGRpdiBjbGFzcz0iY2FyZCI+CiAgPGgzPvCfkYAg44Km44Kp44OD44OB44Oq44K544OIPC9oMz4KICA8ZGl2IGNsYXNzPSJyb3ciPgogICAgPGlucHV0IGlkPSJ3YXRjaENvZGUiIHBsYWNlaG9sZGVyPSLpipjmn4TjgrPjg7zjg4kiPgogICAgPGJ1dHRvbiBvbmNsaWNrPSJhZGRXYXRjaCgpIj7ov73liqA8L2J1dHRvbj4KICA8L2Rpdj4KICA8ZGl2IGlkPSJ3YXRjaENvbXBhbnlOYW1lIiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW46NHB4IDJweCA4cHgiPumKmOafhOWQje+8muKAlDwvZGl2PgogIDxkaXYgaWQ9IndhdGNocyI+PC9kaXY+CjwvZGl2PgoKPHNjcmlwdD4KY29uc3QgJD14PT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZCh4KTsKZnVuY3Rpb24gdmFsKGlkKXtsZXQgdj0kKGlkKS52YWx1ZS50cmltKCk7cmV0dXJuIHY9PT0nJz9udWxsOk51bWJlcih2KX0KZnVuY3Rpb24gbG9jYWwoayl7dHJ5e3JldHVybiBKU09OLnBhcnNlKGxvY2FsU3RvcmFnZS5nZXRJdGVtKGspfHwnW10nKX1jYXRjaChlKXtyZXR1cm5bXX19CmZ1bmN0aW9uIHNhdmUoayx2KXtsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrLEpTT04uc3RyaW5naWZ5KHYpKX0KZnVuY3Rpb24gZm10KHYsZD0yKXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoZCl9CmZ1bmN0aW9uIHllbih2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6J8KlJytNYXRoLnJvdW5kKE51bWJlcih2KSkudG9Mb2NhbGVTdHJpbmcoJ2phLUpQJyl9CmZ1bmN0aW9uIHN0YXRlSmEocyl7CiAgaWYocz09PSdzdHJvbmdfYnVsbGlzaCcpcmV0dXJuICflvLfmsJcnOwogIGlmKHM9PT0nYnVsbGlzaCcpcmV0dXJuICfjgoTjgoTlvLfmsJcnOwogIGlmKHM9PT0nbmV1dHJhbCcpcmV0dXJuICfkuK3nq4snOwogIGlmKHM9PT0nYmVhcmlzaCcpcmV0dXJuICfjgoTjgoTlvLHmsJcnOwogIGlmKHM9PT0nc3Ryb25nX2JlYXJpc2gnKXJldHVybiAn5byx5rCXJzsKICByZXR1cm4gJ+WIpOWumuS/neeVmSc7Cn0KCmZ1bmN0aW9uIHN0YXRlQ2xhc3Mocyl7CiAgaWYocz09PSdzdHJvbmdfYnVsbGlzaCcpcmV0dXJuICdzdGF0ZS1zdHJvbmctYnVsbCc7CiAgaWYocz09PSdidWxsaXNoJylyZXR1cm4gJ3N0YXRlLWJ1bGwnOwogIGlmKHM9PT0nbmV1dHJhbCcpcmV0dXJuICdzdGF0ZS1uZXV0cmFsJzsKICBpZihzPT09J2JlYXJpc2gnKXJldHVybiAnc3RhdGUtYmVhcic7CiAgaWYocz09PSdzdHJvbmdfYmVhcmlzaCcpcmV0dXJuICdzdGF0ZS1zdHJvbmctYmVhcic7CiAgcmV0dXJuICdzdGF0ZS1uZXV0cmFsJzsKfQoKZnVuY3Rpb24gZmFjdG9ySmEoa2V5KXsKICBpZihrZXk9PT0ndGVjaG5pY2FsJylyZXR1cm4gJ+ODhuOCr+ODi+OCq+ODqyc7CiAgaWYoa2V5PT09J2Vhcm5pbmdzJylyZXR1cm4gJ+axuueulyc7CiAgaWYoa2V5PT09J3N1cHBseScpcmV0dXJuICfpnIDntaZwcm94eSc7CiAgaWYoa2V5PT09J3BvbGljeScpcmV0dXJuICflm73nrZYnOwogIHJldHVybiBrZXl8fCfopoHlm6AnOwp9CgpmdW5jdGlvbiBkcml2ZXJTZW50ZW5jZShzYyl7CiAgY29uc3QgcD1zYyYmc2Muc3Ryb25nZXN0X3Bvc2l0aXZlOwogIGNvbnN0IG49c2MmJnNjLnN0cm9uZ2VzdF9uZWdhdGl2ZTsKICBjb25zdCBzY29yZT1OdW1iZXIoc2MmJnNjLnNjb3JlMTAwKTsKCiAgbGV0IGhlYWQ9Jyc7CiAgaWYoTnVtYmVyLmlzRmluaXRlKHNjb3JlKSl7CiAgICBpZihzY29yZT49ODApaGVhZD0n5Y+W5b6X5riI44G/6KaB5Zug44KS57eP5ZCI44GZ44KL44Go44CB5by344GE44OX44Op44K56KmV5L6h44Gn44GZ44CCJzsKICAgIGVsc2UgaWYoc2NvcmU+PTY1KWhlYWQ9J+ODl+ODqeOCueimgeWboOOBjOWEquWLouOBp+OAgeOChOOChOW8t+awl+OBruipleS+oeOBp+OBmeOAgic7CiAgICBlbHNlIGlmKHNjb3JlPj00NSloZWFkPSfjg5fjg6njgrnjgajjg57jgqTjg4rjgrnjgYzmi67mipfjgZfjgIHkuK3nq4vlnI/jgafjgZnjgIInOwogICAgZWxzZSBpZihzY29yZT49MzApaGVhZD0n44Oe44Kk44OK44K56KaB5Zug44Gu5b2x6Z+/44GM44KE44KE5by344GP44CB5oWO6YeN5a+E44KK44Gn44GZ44CCJzsKICAgIGVsc2UgaGVhZD0n44Oe44Kk44OK44K56KaB5Zug44Gu5b2x6Z+/44GM5aSn44GN44GP44CB5byx5rCX5a+E44KK44Gn44GZ44CCJzsKICB9CgogIGxldCB0YWlsPVtdOwogIGlmKG4pdGFpbC5wdXNoKCfmnIDlpKfjga7mirzjgZfkuIvjgZLopoHlm6Djga8gJytmYWN0b3JKYShuLmtleSkrJyAnK3Njb3JlTGFiZWwobi5zY29yZSkpOwogIGlmKHApdGFpbC5wdXNoKCfmnIDlpKfjga7mirzjgZfkuIrjgZLopoHlm6Djga8gJytmYWN0b3JKYShwLmtleSkrJyAnK3Njb3JlTGFiZWwocC5zY29yZSkpOwogIHJldHVybiBoZWFkKyh0YWlsLmxlbmd0aD8nICcrdGFpbC5qb2luKCfjgIInKSsn44CCJzonJyk7Cn0KCmZ1bmN0aW9uIGRyaXZlckJveEh0bWwodGl0bGUsZCxraW5kKXsKICBpZighZClyZXR1cm4gYDxkaXYgY2xhc3M9ImRyaXZlcmJveCI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7oqbLlvZPjgarjgZc8L2I+PC9kaXY+YDsKICBjb25zdCBzaWduPU51bWJlcihkLmNvbnRyaWJ1dGlvbik+PTA/JysnOicnOwogIHJldHVybiBgPGRpdiBjbGFzcz0iZHJpdmVyYm94Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+CiAgICA8Yj4ke2ZhY3RvckphKGQua2V5KX0gJHtzY29yZUxhYmVsKGQuc2NvcmUpfTwvYj4KICAgIDxzbWFsbCBjbGFzcz0ibXV0ZWQiPuWGjemFjeWIhuW+jOOBrumHjeOBvyAke2Qud2VpZ2h0X3BjdH0lIC8g5a+E5LiOICR7c2lnbn0ke051bWJlcihkLmNvbnRyaWJ1dGlvbikudG9GaXhlZCgxKX08L3NtYWxsPgogIDwvZGl2PmA7Cn0KCmZ1bmN0aW9uIGNvbnRyaWJ1dGlvbkNhcmRIdG1sKGQpewogIGlmKCFkKXJldHVybiAnJzsKICBjb25zdCBjPU51bWJlcihkLmNvbnRyaWJ1dGlvbik7CiAgY29uc3Qgc2lnbj1jPjA/JysnOicnOwogIGNvbnN0IGltcGFjdD1jLzI7CiAgY29uc3QgaW1wYWN0U2lnbj1pbXBhY3Q+MD8nKyc6Jyc7CiAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJjb250cmliIj4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHtmYWN0b3JKYShkLmtleSl9PC9zcGFuPgogICAgPGI+JHtzaWdufSR7Yy50b0ZpeGVkKDEpfTwvYj4KICAgIDxzbWFsbD7lho3phY3liIblvozph43jgb8gJHtOdW1iZXIoZC53ZWlnaHRfcGN0KS50b0ZpeGVkKDEpfSU8L3NtYWxsPgogICAgPHNtYWxsPjEwMOeCueaPm+eul+OBuOOBruW9semfvyAke2ltcGFjdFNpZ259JHtpbXBhY3QudG9GaXhlZCgxKX3ngrk8L3NtYWxsPgogIDwvZGl2PmA7Cn0KCmZ1bmN0aW9uIHJlbmRlckNvbnRyaWJ1dGlvbnMoc2MpewogIGNvbnN0IGJveD0kKCdjb250cmlidXRpb25Cb3gnKTsKICBjb25zdCBncmlkPSQoJ2NvbnRyaWJ1dGlvbkdyaWQnKTsKICBpZighYm94fHwhZ3JpZClyZXR1cm47CiAgY29uc3QgZHM9KHNjJiZzYy5kcml2ZXJzKXx8W107CiAgaWYoIWRzLmxlbmd0aCl7CiAgICBib3guc3R5bGUuZGlzcGxheT0nbm9uZSc7CiAgICBncmlkLmlubmVySFRNTD0nJzsKICAgIHJldHVybjsKICB9CiAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICBncmlkLmlubmVySFRNTD1kcy5tYXAoY29udHJpYnV0aW9uQ2FyZEh0bWwpLmpvaW4oJycpOwp9CmZ1bmN0aW9uIHBjdCh2KXtyZXR1cm4gKHY9PT1udWxsfHx2PT09dW5kZWZpbmVkfHxOdW1iZXIuaXNOYU4oTnVtYmVyKHYpKSk/J+KAlCc6TnVtYmVyKHYpLnRvRml4ZWQoMikrJyUnfQpmdW5jdGlvbiBzdGF0Rm9yKGgsa2V5KXtyZXR1cm4gaCYmaC5mb3J3YXJkX3N0YXRzJiZoLmZvcndhcmRfc3RhdHNba2V5XT9oLmZvcndhcmRfc3RhdHNba2V5XTpudWxsfQoKZnVuY3Rpb24gZGF0YUFnZURheXMoZGF0ZVN0cil7CiAgaWYoIWRhdGVTdHIpcmV0dXJuIG51bGw7CiAgY29uc3QgbT1TdHJpbmcoZGF0ZVN0cikubWF0Y2goL14oXGR7NH0pLShcZHsyfSktKFxkezJ9KSQvKTsKICBpZighbSlyZXR1cm4gbnVsbDsKICBjb25zdCBkPURhdGUuVVRDKE51bWJlcihtWzFdKSxOdW1iZXIobVsyXSktMSxOdW1iZXIobVszXSkpOwogIGNvbnN0IG5vdz1uZXcgRGF0ZSgpOwogIGNvbnN0IHRvZGF5PURhdGUuVVRDKG5vdy5nZXRGdWxsWWVhcigpLG5vdy5nZXRNb250aCgpLG5vdy5nZXREYXRlKCkpOwogIHJldHVybiBNYXRoLm1heCgwLE1hdGguZmxvb3IoKHRvZGF5LWQpLzg2NDAwMDAwKSk7Cn0KCmZ1bmN0aW9uIGZyZXNobmVzc0ZvcihkYXRlU3RyKXsKICBjb25zdCBkYXlzPWRhdGFBZ2VEYXlzKGRhdGVTdHIpOwogIGlmKGRheXM9PT1udWxsKXsKICAgIHJldHVybiB7bGV2ZWw6J3Vua25vd24nLGRheXM6bnVsbCxsYWJlbDon5pel5LuY5LiN5piOJyxjbHM6J2ZyZXNoLXdhcm4nLGRlY2lzaW9uX29rOmZhbHNlfTsKICB9CiAgaWYoZGF5czw9NCl7CiAgICByZXR1cm4ge2xldmVsOidmcmVzaCcsZGF5cyxsYWJlbDon6a6u5bqmT0snLGNsczonZnJlc2gtb2snLGRlY2lzaW9uX29rOnRydWV9OwogIH0KICBpZihkYXlzPD0xMCl7CiAgICByZXR1cm4ge2xldmVsOid3YXJuaW5nJyxkYXlzLGxhYmVsOifjgoTjgoTpgYXlu7YnLGNsczonZnJlc2gtd2FybicsZGVjaXNpb25fb2s6dHJ1ZX07CiAgfQogIHJldHVybiB7bGV2ZWw6J3N0YWxlJyxkYXlzLGxhYmVsOiflj6TjgYTjg4fjg7zjgr8nLGNsczonZnJlc2gtc3RhbGUnLGRlY2lzaW9uX29rOmZhbHNlfTsKfQoKZnVuY3Rpb24gZnJlc2huZXNzVGV4dChkYXRlU3RyKXsKICBjb25zdCBmPWZyZXNobmVzc0ZvcihkYXRlU3RyKTsKICBpZihmLmRheXM9PT1udWxsKXJldHVybiAn5pyA57WC44OH44O844K/5pel44KS56K66KqN44Gn44GN44G+44Gb44KT44CCJzsKICBpZihmLmxldmVsPT09J2ZyZXNoJylyZXR1cm4gYOacgOe1guODh+ODvOOCv+aXpeOBi+OCiSAke2YuZGF5c33ml6XjgILpgJrluLjjga7lj4LogIPliKTlrprjgavkvb/nlKjjgZfjgb7jgZnjgIJgOwogIGlmKGYubGV2ZWw9PT0nd2FybmluZycpcmV0dXJuIGDmnIDntYLjg4fjg7zjgr/ml6XjgYvjgokgJHtmLmRheXN95pel44CC6YGF5bu244Gr5rOo5oSP44GX44Gm44CB5a6f6Zqb44Gu54++5Zyo5YCk44KC56K66KqN44GX44Gm44GP44Gg44GV44GE44CCYDsKICByZXR1cm4gYOacgOe1guODh+ODvOOCv+aXpeOBi+OCiSAke2YuZGF5c33ml6XpgYXjgozjgILku4rml6Xjga7lo7LosrfliKTmlq3jgavjga/lj6TjgYTjgZ/jgoHjgIHkv53mnInliKTmlq3jga/oh6rli5Xjgafkv53nlZnjgZfjgb7jgZnjgIJgOwp9CgpmdW5jdGlvbiBzaG93RnJlc2huZXNzKGRhdGVTdHIpewogIGNvbnN0IGJveD0kKCdmcmVzaG5lc3NCb3gnKTsKICBpZighYm94KXJldHVybjsKICBjb25zdCBmPWZyZXNobmVzc0ZvcihkYXRlU3RyKTsKICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogIGJveC5jbGFzc05hbWU9J2ZyZXNoYm94ICcrZi5jbHM7CiAgJCgnZnJlc2huZXNzVGl0bGUnKS50ZXh0Q29udGVudD0KICAgIGYubGV2ZWw9PT0nZnJlc2gnPyfinIUg44OH44O844K/6a6u5bqmT0snOgogICAgZi5sZXZlbD09PSd3YXJuaW5nJz8n4pqg77iPIOODh+ODvOOCv+mBheW7tuOBq+azqOaEjyc6CiAgICBmLmxldmVsPT09J3N0YWxlJz8n8J+bkSDjg4fjg7zjgr/jgYzlj6TjgYTjgZ/jgoHku4rml6Xjga7liKTmlq3jga/kv53nlZknOgogICAgJ+KaoO+4jyDjg4fjg7zjgr/prq7luqbjgpLnorroqo3jgafjgY3jgb7jgZvjgpMnOwogICQoJ2ZyZXNobmVzc0RldGFpbCcpLnRleHRDb250ZW50PWZyZXNobmVzc1RleHQoZGF0ZVN0cik7Cn0KCmZ1bmN0aW9uIHNob3dIaXN0b3J5RnJlc2huZXNzKGhpc3RvcnlEYXRlLHByaWNlRGF0ZSl7CiAgY29uc3QgYm94PSQoJ2hpc3RvcnlGcmVzaG5lc3NCb3gnKTsKICBpZighYm94KXJldHVybjsKICBpZighaGlzdG9yeURhdGUpewogICAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgICQoJ2hpc3RvcnlGcmVzaG5lc3NUZXh0JykudGV4dENvbnRlbnQ9JzIw5pel44O7MTI25pel44O7MjUy5pel44Gu5YiG5p6Q5bGl5q2044KS5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44Gf44CC54++5Zyo5YCk44Gg44GR6KGo56S644GX44G+44GZ44CCJzsKICAgIHJldHVybjsKICB9CiAgY29uc3QgaGY9ZnJlc2huZXNzRm9yKGhpc3RvcnlEYXRlKTsKICBpZihoZi5sZXZlbD09PSdmcmVzaCcpewogICAgYm94LnN0eWxlLmRpc3BsYXk9J2Jsb2NrJzsKICAgIGJveC5jbGFzc05hbWU9J2ZyZXNoYm94IGZyZXNoLW9rJzsKICAgICQoJ2hpc3RvcnlGcmVzaG5lc3NUZXh0JykudGV4dENvbnRlbnQ9CiAgICAgIGDkvqHmoLzlsaXmrbTjgoLmnIDmlrDllrbmpa3ml6UgJHtoaXN0b3J5RGF0ZX0g44G+44Gn5Y+W5b6X44CC55+t5Lit6ZW344O76ZyA57WmcHJveHnjg7vjg4jjg6zjg7zjg6rjg7PjgrDjgpLpgJrluLjoqIjnrpfjgZfjgb7jgZnjgIJgOwogICAgcmV0dXJuOwogIH0KICBib3guc3R5bGUuZGlzcGxheT0nYmxvY2snOwogIGNvbnN0IHBkPXByaWNlRGF0ZXx8J+KAlCc7CiAgJCgnaGlzdG9yeUZyZXNobmVzc1RleHQnKS50ZXh0Q29udGVudD0KICAgIGDnj77lnKjlgKTjg4fjg7zjgr/ml6UgJHtwZH0gLyDkvqHmoLzlsaXmrbTmnIDntYLml6UgJHtoaXN0b3J5RGF0ZX3jgILkvqHmoLzlsaXmrbTjgYzlj6TjgYTloLTlkIjjgaDjgZHjgIHjg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjg7vpnIDntaZwcm94eeOCkuWPguiAg+WApOaJseOBhOOBq+OBl+OBvuOBmeOAgmA7Cn0KCmZ1bmN0aW9uIGNvbmZpZGVuY2VGb3IoaCl7CiAgY29uc3QgdmFscz1baC5yZXR1cm5fMjBkLGgucmV0dXJuXzEyNmQsaC5yZXR1cm5fMjUyZF0ubWFwKE51bWJlcikuZmlsdGVyKE51bWJlci5pc0Zpbml0ZSk7CiAgY29uc3Qgc3RhdHM9WycyMGQnLCcxMjZkJywnMjUyZCddLm1hcChrPT5zdGF0Rm9yKGgsaykpLmZpbHRlcihzPT5zJiZzLnN0YXR1cz09PSdvaycpOwoKICBpZighTnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKXx8IWguYXNvZilyZXR1cm4gMDsKCiAgY29uc3QgY292ZXJhZ2U9dmFscy5sZW5ndGgvMzsKICBsZXQgYWdyZWVtZW50PTAuNTsKICBpZih2YWxzLmxlbmd0aCl7CiAgICBjb25zdCBwb3M9dmFscy5maWx0ZXIoeD0+eD4wKS5sZW5ndGg7CiAgICBjb25zdCBuZWc9dmFscy5maWx0ZXIoeD0+eDwwKS5sZW5ndGg7CiAgICBhZ3JlZW1lbnQ9TWF0aC5tYXgocG9zLG5lZykvdmFscy5sZW5ndGg7CiAgfQogIGNvbnN0IHN0YXRDb3ZlcmFnZT1zdGF0cy5sZW5ndGgvMzsKICBsZXQgc2NvcmU9KGNvdmVyYWdlKjAuNDUgKyBhZ3JlZW1lbnQqMC4yNSArIHN0YXRDb3ZlcmFnZSowLjMwKSoxMDA7CgogIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogIGlmKGZyZXNoLmxldmVsPT09J3dhcm5pbmcnKXNjb3JlKj0wLjYwOwogIGlmKGZyZXNoLmxldmVsPT09J3N0YWxlJylzY29yZT1NYXRoLm1pbihzY29yZSwyNSk7CiAgaWYoZnJlc2gubGV2ZWw9PT0ndW5rbm93bicpc2NvcmU9TWF0aC5taW4oc2NvcmUsMjApOwoKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGlmKGhpc3RvcnlGcmVzaC5sZXZlbD09PSd3YXJuaW5nJylzY29yZSo9MC43NTsKICBpZihoaXN0b3J5RnJlc2gubGV2ZWw9PT0nc3RhbGUnKXNjb3JlPU1hdGgubWluKHNjb3JlLDM1KTsKICBpZihoaXN0b3J5RnJlc2gubGV2ZWw9PT0ndW5rbm93bicpc2NvcmU9TWF0aC5taW4oc2NvcmUsMjUpOwoKICByZXR1cm4gTWF0aC5yb3VuZChzY29yZSk7Cn0KCmZ1bmN0aW9uIGRlY2lzaW9uRm9yKGgsYyl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhaC5hc29mKXsKICAgIHJldHVybiB7CiAgICAgIGxhYmVsOifliKTlrprkv53nlZknLAogICAgICBjbHM6J2Qtd2F0Y2gnLAogICAgICByZWFzb246J+Wun+ODh+ODvOOCv+acquWPluW+l+OAguWPs+S4iuOBruOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44GX44Gm44GP44Gg44GV44GE44CCJywKICAgICAgY29uZmlkZW5jZTowCiAgICB9OwogIH0KCiAgY29uc3QgcjIwPU51bWJlcihoLnJldHVybl8yMGQpLCByMTI2PU51bWJlcihoLnJldHVybl8xMjZkKSwgcjI1Mj1OdW1iZXIoaC5yZXR1cm5fMjUyZCk7CiAgY29uc3QgY29uZmlkZW5jZT1jb25maWRlbmNlRm9yKGgpOwogIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihoLmFzb2YpOwoKICBpZighZnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVme+8iOagquS+oeWPpOOBhO+8iScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjpgJHtmcmVzaG5lc3NUZXh0KGguYXNvZil9IOaQjeWIh+OCiuODu+WIqeeiuuODqeOCpOODs+OBqOOBruavlOi8g+OCguWPguiAg+WApOaJseOBhOOBp+OBmeOAgmAsCiAgICAgIGNvbmZpZGVuY2UKICAgIH07CiAgfQoKICBpZihjdXI8PWMuc3RvcFByaWNlKXsKICAgIHJldHVybiB7bGFiZWw6J+aQjeWIh+OCiuaknOiojicsY2xzOidkLXN0b3AnLHJlYXNvbjon6Kit5a6a44GX44Gf5pCN5YiH44KK5Y+C6ICD44Op44Kk44Oz5Lul5LiLJyxjb25maWRlbmNlfTsKICB9CiAgaWYoY3VyPj1jLnRha2VQcmljZSl7CiAgICByZXR1cm4ge2xhYmVsOifliKnnorrmpJzoqI4nLGNsczonZC10YWtlJyxyZWFzb246J+ioreWumuOBl+OBn+WIqeeiuuWPguiAg+ODqeOCpOODs+S7peS4iicsY29uZmlkZW5jZX07CiAgfQoKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGlmKCFoaXN0b3J5RnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgbGFiZWw6J+WIpOWumuS/neeVme+8iOWxpeattOWPpOOBhO+8iScsCiAgICAgIGNsczonZC13YXRjaCcsCiAgICAgIHJlYXNvbjpg54++5Zyo5YCk44Gv5Y+W5b6X44Gn44GN44Gm44GE44G+44GZ44GM44CB5L6h5qC85bGl5q2044GvICR7aC5oaXN0b3J5X2Fzb2Z8fCfkuI3mmI4nfeOAguWbuuWumuOBruaQjeWIh+OCii/liKnnorrjg6njgqTjg7Pjgavjga/mnKrliLDpgZTjgafjgZnjgYzjgIHjg4jjg6zjg7Pjg4nliKTmlq3jga/kv53nlZnjgZfjgb7jgZnjgIJgLAogICAgICBjb25maWRlbmNlCiAgICB9OwogIH0KCiAgaWYoY3VyPD1jLnRyYWlsUHJpY2UpewogICAgcmV0dXJuIHtsYWJlbDon6K2m5oiSJyxjbHM6J2Qtd2F0Y2gnLHJlYXNvbjonMjDml6Xpq5jlgKTln7rmupbjga7jg4jjg6zjg7zjg6rjg7PjgrDlj4LogIPjg6njgqTjg7Pku6XkuIsnLGNvbmZpZGVuY2V9OwogIH0KCiAgbGV0IHBvc2l0aXZlPTAsIG5lZ2F0aXZlPTA7CiAgW3IyMCxyMTI2LHIyNTJdLmZvckVhY2goeD0+ewogICAgaWYoTnVtYmVyLmlzRmluaXRlKHgpKXsKICAgICAgaWYoeD4wKXBvc2l0aXZlKys7CiAgICAgIGlmKHg8MCluZWdhdGl2ZSsrOwogICAgfQogIH0pOwoKICBpZihuZWdhdGl2ZT49Mil7CiAgICByZXR1cm4ge2xhYmVsOiforabmiJInLGNsczonZC13YXRjaCcscmVhc29uOicyMOaXpeODuzEyNuaXpeODuzI1MuaXpeOBruOBhuOBoeODnuOCpOODiuOCueWCvuWQkeOBjOWEquWLoicsY29uZmlkZW5jZX07CiAgfQogIGlmKHBvc2l0aXZlPj0yKXsKICAgIHJldHVybiB7bGFiZWw6J+S/neaciee2mee2micsY2xzOidkLWhvbGQnLHJlYXNvbjon6Kit5a6a44Op44Kk44Oz5YaF44Gn44CB6KSH5pWw5pyf6ZaT44Gu5L6h5qC844OI44Os44Oz44OJ44GM44OX44Op44K5Jyxjb25maWRlbmNlfTsKICB9CiAgcmV0dXJuIHtsYWJlbDon5L+d5pyJ57aZ57aa77yI5qeY5a2Q6KaL77yJJyxjbHM6J2QtaG9sZCcscmVhc29uOifoqK3lrprjg6njgqTjg7PlhoXjgILmnJ/plpPliKXjg4jjg6zjg7Pjg4njga/lvLflvLHjgYzmt7flnKgnLGNvbmZpZGVuY2V9Owp9CmZ1bmN0aW9uIGV2SHRtbCh0aXRsZSxzKXsKICBpZighc3x8cy5zdGF0dXMhPT0nb2snKXsKICAgIGNvbnN0IG49cyYmcy5uIT09dW5kZWZpbmVkP3MubjowOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJldiI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7jg4fjg7zjgr/kuI3otrM8L2I+PHNtYWxsPuaomeacrCAke2595Lu2PC9zbWFsbD48L2Rpdj5gOwogIH0KICByZXR1cm4gYDxkaXYgY2xhc3M9ImV2Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX08L3NwYW4+CiAgICA8Yj7lubPlnYcgJHtwY3Qocy5tZWFuKX08L2I+CiAgICA8c21hbGw+5Lit5aSu5YCkICR7cGN0KHMubWVkaWFuKX08L3NtYWxsPgogICAgPHNtYWxsPuS4iuaYh+eOhyAke3BjdChzLnBvc2l0aXZlX3JhdGUpfTwvc21hbGw+CiAgICA8c21hbGw+UDEw44CcUDkwICR7cGN0KHMucDEwKX0g44CcICR7cGN0KHMucDkwKX08L3NtYWxsPgogICAgPHNtYWxsPuaomeacrCAke3Mubn3ku7Y8L3NtYWxsPgogIDwvZGl2PmA7Cn0KCgpmdW5jdGlvbiBkaXN0YW5jZUluZm8oY3VyLHRhcmdldCxraW5kKXsKICBjdXI9TnVtYmVyKGN1cik7IHRhcmdldD1OdW1iZXIodGFyZ2V0KTsKICBpZighTnVtYmVyLmlzRmluaXRlKGN1cil8fGN1cjw9MHx8IU51bWJlci5pc0Zpbml0ZSh0YXJnZXQpKXJldHVybiAn4oCUJzsKICBjb25zdCBkaWZmPSh0YXJnZXQvY3VyLTEpKjEwMDsKICBpZihraW5kPT09J3N0b3AnKXsKICAgIGlmKGRpZmY+PTApcmV0dXJuICfjg6njgqTjg7PliLDpgZTmuIjjgb8nOwogICAgcmV0dXJuIE1hdGguYWJzKGRpZmYpLnRvRml4ZWQoMikrJyUg5LiLJzsKICB9CiAgaWYoa2luZD09PSd0YWtlJyl7CiAgICBpZihkaWZmPD0wKXJldHVybiAn44Op44Kk44Oz5Yiw6YGU5riI44G/JzsKICAgIHJldHVybiBkaWZmLnRvRml4ZWQoMikrJyUg5LiKJzsKICB9CiAgcmV0dXJuIChkaWZmPj0wPycrJzonJykrZGlmZi50b0ZpeGVkKDIpKyclJzsKfQoKZnVuY3Rpb24gcHJpY2VSYW5nZShjdXIscyl7CiAgY3VyPU51bWJlcihjdXIpOwogIGlmKCFOdW1iZXIuaXNGaW5pdGUoY3VyKXx8Y3VyPD0wfHwhc3x8cy5zdGF0dXMhPT0nb2snKXJldHVybiBudWxsOwogIHJldHVybiB7CiAgICBsb3c6Y3VyKigxK051bWJlcihzLnAxMCkvMTAwKSwKICAgIGhpZ2g6Y3VyKigxK051bWJlcihzLnA5MCkvMTAwKSwKICAgIG1lZGlhbjpjdXIqKDErTnVtYmVyKHMubWVkaWFuKS8xMDApCiAgfTsKfQoKZnVuY3Rpb24gcmFuZ2VIdG1sKHRpdGxlLGN1cixzKXsKICBjb25zdCByPXByaWNlUmFuZ2UoY3VyLHMpOwogIGlmKCFyKXsKICAgIGNvbnN0IG49cyYmcy5uIT09dW5kZWZpbmVkP3MubjowOwogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJyYW5nZWJveCI+PHNwYW4gY2xhc3M9Im11dGVkIj4ke3RpdGxlfTwvc3Bhbj48Yj7jg4fjg7zjgr/kuI3otrM8L2I+PHNwYW4gY2xhc3M9Im11dGVkIj7mqJnmnKwgJHtufeS7tjwvc3Bhbj48L2Rpdj5gOwogIH0KICByZXR1cm4gYDxkaXYgY2xhc3M9InJhbmdlYm94Ij4KICAgIDxzcGFuIGNsYXNzPSJtdXRlZCI+JHt0aXRsZX0gUDEw44CcUDkwPC9zcGFuPgogICAgPGI+JHt5ZW4oci5sb3cpfSDjgJwgJHt5ZW4oci5oaWdoKX08L2I+CiAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuS4reWkruWApOaPm+eulyAke3llbihyLm1lZGlhbil9PC9zcGFuPgogIDwvZGl2PmA7Cn0KCmZ1bmN0aW9uIGFjdGlvblRleHQoaCxjLGQpewogIGlmKGQubGFiZWw9PT0n5Yik5a6a5L+d55WZJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjmoKrkvqHlj6TjgYTvvIknKXJldHVybiAn44OH44O844K/44GM5Y+k44GE44Gf44KB5LuK5pel44Gu5Yik5pat44Gv5L+d55WZ44CC6Ki85Yi45Lya56S+44Gq44Gp44Gn5a6f6Zqb44Gu54++5Zyo5YCk44KS56K66KqN44GX44Gm44GL44KJ5Yik5pat44CCJzsKICBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVme+8iOWxpeattOWPpOOBhO+8iScpcmV0dXJuICfnj77lnKjlgKTjga/norroqo3muIjjgb/jgILlm7rlrprjga7mkI3liIfjgoov5Yip56K644Op44Kk44Oz44Gg44GR56K66KqN44GX44CB44OI44Os44Oz44OJ5Yik5pat44Gv5L6h5qC85bGl5q205pu05paw44G+44Gn5L+d55WZ44CCJzsKICBpZihkLmxhYmVsPT09J+aQjeWIh+OCiuaknOiojicpcmV0dXJuICfmkI3liIfjgorlj4LogIPjg6njgqTjg7PjgpLkuIvlm57jgaPjgabjgYTjgb7jgZnjgILlrp/pmpvjga7nj77lnKjlgKTjgajms6jmlofmnaHku7bjgpLnorroqo3jgZfjgabjgIHnuK7lsI/jg7vmkqTpgIDjgpLmpJzoqI7jgIInOwogIGlmKGQubGFiZWw9PT0n5Yip56K65qSc6KiOJylyZXR1cm4gJ+WIqeeiuuWPguiAg+ODqeOCpOODs+OBq+WIsOmBlOOBl+OBpuOBhOOBvuOBmeOAguWFqOmDqOWjsuWNtOOBoOOBkeOBp+OBquOBj+OAgeWIhuWJsuWIqeeiuuOCguWAmeijnOOAgic7CiAgaWYoZC5sYWJlbD09PSforabmiJInKXJldHVybiAn6K2m5oiS44K+44O844Oz44CC44OI44Os44O844Oq44Oz44Kw44Op44Kk44Oz44Go5Lit55+t5pyf44Gu5YCk5YuV44GN44KS5YSq5YWI44GX44Gm56K66KqN44CCJzsKICByZXR1cm4gJ+ioreWumuODqeOCpOODs+WGheOAguS/neaciee2mee2muWAmeijnOOBp+OBmeOBjOOAgeeEoeaWmeODh+ODvOOCv+OBr+mBheW7tuOBmeOCi+OBn+OCgeWun+mam+OBruePvuWcqOWApOOCgueiuuiqjeOAgic7Cn0KCgpmdW5jdGlvbiBub211cmFOZXRGZWUoYW1vdW50KXsKICBhbW91bnQ9TnVtYmVyKGFtb3VudHx8MCk7CiAgaWYoYW1vdW50PD0wKXJldHVybiAwOwogIGlmKGFtb3VudDw9MTAwMDAwKXJldHVybiAxNTI7CiAgaWYoYW1vdW50PD0zMDAwMDApcmV0dXJuIDMzMDsKICBpZihhbW91bnQ8PTUwMDAwMClyZXR1cm4gNTI0OwogIGlmKGFtb3VudDw9MTAwMDAwMClyZXR1cm4gMTA0ODsKICBpZihhbW91bnQ8PTIwMDAwMDApcmV0dXJuIDIwOTU7CiAgaWYoYW1vdW50PD0zMDAwMDAwKXJldHVybiAzMTQzOwogIGlmKGFtb3VudDw9NTAwMDAwMClyZXR1cm4gNTIzODsKICBpZihhbW91bnQ8PTEwMDAwMDAwKXJldHVybiAxMDQ3NjsKICBpZihhbW91bnQ8PTIwMDAwMDAwKXJldHVybiAyMDk1MjsKICBpZihhbW91bnQ8PTMwMDAwMDAwKXJldHVybiAzMTQyOTsKICBpZihhbW91bnQ8PTUwMDAwMDAwKXJldHVybiA0MTkwNTsKICByZXR1cm4gNzg1NzE7Cn0KZnVuY3Rpb24gZmVlRm9yKGFtb3VudCxtb2RlKXtyZXR1cm4gbW9kZT09PSdub211cmFfbmV0Jz9ub211cmFOZXRGZWUoYW1vdW50KTowfQoKZnVuY3Rpb24gY2FsY0hvbGRpbmcoaCl7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IGNvc3Q9TnVtYmVyKGguY29zdCk7CiAgY29uc3Qgc2hhcmVzPU51bWJlcihoLnNoYXJlcyk7CiAgY29uc3QgYnV5VmFsdWU9Y29zdCpzaGFyZXM7CiAgY29uc3QgYnV5RmVlPWZlZUZvcihidXlWYWx1ZSxoLmZlZV9tb2RlKTsKICBjb25zdCBjdXJyZW50VmFsdWU9Y3VyKnNoYXJlczsKICBjb25zdCBzZWxsRmVlPWZlZUZvcihjdXJyZW50VmFsdWUsaC5mZWVfbW9kZSk7CiAgY29uc3QgaW52ZXN0ZWQ9YnV5VmFsdWUrYnV5RmVlOwogIGNvbnN0IG5ldE5vdz1jdXJyZW50VmFsdWUtc2VsbEZlZS1pbnZlc3RlZDsKICBjb25zdCBuZXROb3dQY3Q9aW52ZXN0ZWQ/bmV0Tm93L2ludmVzdGVkKjEwMDpudWxsOwoKICBjb25zdCBzdG9wUHJpY2U9Y29zdCooMS1OdW1iZXIoaC5zdG9wX3BjdCkvMTAwKTsKICBjb25zdCB0YWtlUHJpY2U9Y29zdCooMStOdW1iZXIoaC50YWtlX3BjdCkvMTAwKTsKICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKGguaGlzdG9yeV9hc29mfHxoLmFzb2YpOwogIGNvbnN0IGhpZ2gyMD1OdW1iZXIoaC5oaWdoXzIwZHx8Y3VyKTsKICBjb25zdCB0cmFpbFByaWNlPWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vawogICAgPyBoaWdoMjAqKDEtTnVtYmVyKGgudHJhaWxfcGN0KS8xMDApCiAgICA6IG51bGw7CgogIGNvbnN0IHN0b3BWYWx1ZT1zdG9wUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHRha2VWYWx1ZT10YWtlUHJpY2Uqc2hhcmVzOwogIGNvbnN0IHN0b3BOZXQ9c3RvcFZhbHVlLWZlZUZvcihzdG9wVmFsdWUsaC5mZWVfbW9kZSktaW52ZXN0ZWQ7CiAgY29uc3QgdGFrZU5ldD10YWtlVmFsdWUtZmVlRm9yKHRha2VWYWx1ZSxoLmZlZV9tb2RlKS1pbnZlc3RlZDsKCiAgcmV0dXJuIHtidXlWYWx1ZSxidXlGZWUsY3VycmVudFZhbHVlLHNlbGxGZWUsaW52ZXN0ZWQsbmV0Tm93LG5ldE5vd1BjdCxzdG9wUHJpY2UsdGFrZVByaWNlLHRyYWlsUHJpY2Usc3RvcE5ldCx0YWtlTmV0fTsKfQoKCmNvbnN0IG5hbWVUaW1lcnM9e307CmNvbnN0IG5hbWVDYWNoZT17fTsKCmZ1bmN0aW9uIGRpc3BsYXlDb21wYW55KHRhcmdldCxpbmZvLHByZWZpeD0nJyl7CiAgaWYoIXRhcmdldClyZXR1cm47CiAgaWYoIWluZm98fCFpbmZvLm5hbWUpewogICAgdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya5Y+W5b6X44Gn44GN44G+44Gb44KT44Gn44GX44GfJzsKICAgIHJldHVybjsKICB9CiAgbGV0IGV4dHJhcz1bXTsKICBpZihpbmZvLm1hcmtldClleHRyYXMucHVzaChpbmZvLm1hcmtldCk7CiAgaWYoaW5mby5zZWN0b3IzMylleHRyYXMucHVzaChpbmZvLnNlY3RvcjMzKTsKICB0YXJnZXQuaW5uZXJIVE1MPSc8Yj4nK3ByZWZpeCtpbmZvLm5hbWUrJzwvYj4nKyhleHRyYXMubGVuZ3RoPyc8YnI+PHNwYW4gY2xhc3M9Im11dGVkIj4nK2V4dHJhcy5qb2luKCcgLyAnKSsnPC9zcGFuPic6JycpOwp9Cgphc3luYyBmdW5jdGlvbiBnZXRDb21wYW55KGNvZGUpewogIGNvbnN0IGM9U3RyaW5nKGNvZGV8fCcnKS50cmltKCk7CiAgaWYoIWMpcmV0dXJuIG51bGw7CiAgaWYobmFtZUNhY2hlW2NdKXJldHVybiBuYW1lQ2FjaGVbY107CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvc2VjdXJpdHk/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+mKmOafhOWQjeOCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIG5hbWVDYWNoZVtjXT14OwogIHJldHVybiB4Owp9CgpmdW5jdGlvbiBzY2hlZHVsZUNvbXBhbnlMb29rdXAoaW5wdXRJZCx0YXJnZXRJZCxwcmVmaXg9JycpewogIGNsZWFyVGltZW91dChuYW1lVGltZXJzW2lucHV0SWRdKTsKICBjb25zdCBjPSQoaW5wdXRJZCkudmFsdWUudHJpbSgpOwogIGNvbnN0IHRhcmdldD0kKHRhcmdldElkKTsKCiAgaWYoYy5sZW5ndGg8NCl7CiAgICBpZih0YXJnZXQpdGFyZ2V0LnRleHRDb250ZW50PXByZWZpeCsn6YqY5p+E5ZCN77ya4oCUJzsKICAgIHJldHVybjsKICB9CgogIG5hbWVUaW1lcnNbaW5wdXRJZF09c2V0VGltZW91dChhc3luYygpPT57CiAgICB0cnl7CiAgICAgIGlmKHRhcmdldCl0YXJnZXQudGV4dENvbnRlbnQ9J+mKmOafhOWQjeOCkueiuuiqjeS4reKApic7CiAgICAgIGNvbnN0IGluZm89YXdhaXQgZ2V0Q29tcGFueShjKTsKICAgICAgZGlzcGxheUNvbXBhbnkodGFyZ2V0LGluZm8scHJlZml4KTsKICAgIH1jYXRjaChlKXsKICAgICAgaWYodGFyZ2V0KXRhcmdldC50ZXh0Q29udGVudD1wcmVmaXgrJ+mKmOafhOWQje+8muWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnyc7CiAgICB9CiAgfSw0NTApOwp9CgoKCmZ1bmN0aW9uIHByaW9yaXR5QWN0aW9uRm9yKGgpewogIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgY29uc3QgY3VyPU51bWJlcihoLmN1cnJlbnRfcHJpY2UpOwogIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogIGNvbnN0IG5hbWU9aC5jb21wYW55X25hbWV8fCcnOwogIGNvbnN0IGxhYmVsPShoLmNvZGV8fCcnKSsobmFtZT8nICcrbmFtZTonJyk7CiAgY29uc3QgZD1kZWNpc2lvbkZvcihoLGMpOwoKICBpZighdmFsaWQpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTYsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOiflrp/jg4fjg7zjgr/jgpLmm7TmlrAnLAogICAgICBkZXRhaWw6J+acgOaWsOWPluW+l+e1guWApOOBjOOBguOCiuOBvuOBm+OCk+OAguOBvuOBmuOAjOabtOaWsOOAjeOBp0otUXVhbnRz44OH44O844K/44KS5Y+W5b6X44CCJywKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBjb25zdCBmcmVzaD1mcmVzaG5lc3NGb3IoaC5hc29mKTsKICBpZighZnJlc2guZGVjaXNpb25fb2spewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTksCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifjg4fjg7zjgr/prq7luqbjgpLnorroqo0nLAogICAgICBkZXRhaWw6YCR7ZnJlc2huZXNzVGV4dChoLmFzb2YpfSDlrp/pmpvjga7nj77lnKjlgKTjgpLlhYjjgavnorroqo3jgIJgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGNvbnN0IHN0b3BEaXN0PShjdXIvYy5zdG9wUHJpY2UtMSkqMTAwOwogIGNvbnN0IHRha2VEaXN0PShjLnRha2VQcmljZS9jdXItMSkqMTAwOwogIGNvbnN0IHRyYWlsRGlzdD0oY3VyL2MudHJhaWxQcmljZS0xKSoxMDA7CgogIGlmKGN1cjw9Yy5zdG9wUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6MTAwLAogICAgICBjbHM6J3ByaW9yaXR5LWhpZ2gnLAogICAgICB0aXRsZTon5pCN5YiH44KK44Op44Kk44Oz5Yiw6YGUJywKICAgICAgZGV0YWlsOmDmnIDmlrDlj5blvpfntYLlgKQgJHt5ZW4oY3VyKX0gLyDmkI3liIfjgorlj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfWAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoc3RvcERpc3Q8PTMpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTQsCiAgICAgIGNsczoncHJpb3JpdHktaGlnaCcsCiAgICAgIHRpdGxlOifmkI3liIfjgorjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOOBguOBqCAke3N0b3BEaXN0LnRvRml4ZWQoMil9JSDjgafmkI3liIfjgorlj4LogIPjg6njgqTjg7NgLAogICAgICBsYWJlbAogICAgfTsKICB9CgogIGlmKGN1cj49Yy50YWtlUHJpY2UpewogICAgcmV0dXJuIHsKICAgICAgc2NvcmU6OTAsCiAgICAgIGNsczoncHJpb3JpdHktdGFrZScsCiAgICAgIHRpdGxlOifliKnnorrjg6njgqTjg7PliLDpgZQnLAogICAgICBkZXRhaWw6YOacgOaWsOWPluW+l+e1guWApCAke3llbihjdXIpfSAvIOWIqeeiuuWPguiAgyAke3llbihjLnRha2VQcmljZSl9YCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICBpZih0YWtlRGlzdDw9Myl7CiAgICByZXR1cm4gewogICAgICBzY29yZTo4NCwKICAgICAgY2xzOidwcmlvcml0eS10YWtlJywKICAgICAgdGl0bGU6J+WIqeeiuuODqeOCpOODs+aOpei/kScsCiAgICAgIGRldGFpbDpg44GC44GoICR7dGFrZURpc3QudG9GaXhlZCgyKX0lIOOBp+WIqeeiuuWPguiAg+ODqeOCpOODs2AsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgY29uc3QgaGlzdG9yeUZyZXNoPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICBpZighaGlzdG9yeUZyZXNoLmRlY2lzaW9uX29rKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjc4LAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOifkvqHmoLzlsaXmrbTjgpLmm7TmlrDlvoXjgaEnLAogICAgICBkZXRhaWw6YOePvuWcqOWApCAke2guYXNvZnx8J+KAlCd9IC8g5L6h5qC85bGl5q20ICR7aC5oaXN0b3J5X2Fzb2Z8fCfigJQnfeOAguWbuuWumuS+oeagvOODqeOCpOODs+S7peWkluOBruWIpOaWreOBr+S/neeVmeOAgmAsCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoZC5sYWJlbD09PSforabmiJInKXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjgwLAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOiforabmiJLliKTlrponLAogICAgICBkZXRhaWw6ZC5yZWFzb24sCiAgICAgIGxhYmVsCiAgICB9OwogIH0KCiAgaWYoTnVtYmVyLmlzRmluaXRlKHRyYWlsRGlzdCkmJnRyYWlsRGlzdDw9Mi41KXsKICAgIHJldHVybiB7CiAgICAgIHNjb3JlOjc2LAogICAgICBjbHM6J3ByaW9yaXR5LW1pZCcsCiAgICAgIHRpdGxlOifjg4jjg6zjg7zjg6rjg7PjgrDjg6njgqTjg7PmjqXov5EnLAogICAgICBkZXRhaWw6YOODiOODrOODvOODquODs+OCsOWPguiAgyAke3llbihjLnRyYWlsUHJpY2UpfSDjgb7jgacgJHt0cmFpbERpc3QudG9GaXhlZCgyKX0lYCwKICAgICAgbGFiZWwKICAgIH07CiAgfQoKICByZXR1cm4gewogICAgc2NvcmU6MzAsCiAgICBjbHM6J3ByaW9yaXR5LWdvb2QnLAogICAgdGl0bGU6J+mAmuW4uOebo+imlicsCiAgICBkZXRhaWw6ZC5yZWFzb258fCfoqK3lrprjg6njgqTjg7PlhoUnLAogICAgbGFiZWwKICB9Owp9CgpmdW5jdGlvbiByZW5kZXJQcmlvcml0eUFjdGlvbnMoKXsKICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogIGNvbnN0IGJveD0kKCdwcmlvcml0eUFjdGlvbnMnKTsKICBpZighYm94KXJldHVybjsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGJveC5pbm5lckhUTUw9JzxwIGNsYXNzPSJtdXRlZCI+5L+d5pyJ5qCq44KS55m76Yyy44GZ44KL44Go44CB5YSq5YWI44GX44Gm56K66KqN44GZ44KL6YqY5p+E44KS6Ieq5YuV6KGo56S644GX44G+44GZ44CCPC9wPic7CiAgICByZXR1cm47CiAgfQoKICBsZXQgYWN0aW9ucz1hLm1hcChwcmlvcml0eUFjdGlvbkZvcik7CgogIC8vIENvbmNlbnRyYXRpb24gYWxlcnQgKHBvcnRmb2xpby1sZXZlbCkKICBjb25zdCB2YWxpZD1hLmZpbHRlcihoPT5OdW1iZXIuaXNGaW5pdGUoTnVtYmVyKGguY3VycmVudF9wcmljZSkpJiZOdW1iZXIoaC5jdXJyZW50X3ByaWNlKT4wJiZoLmFzb2YpOwogIGNvbnN0IHRvdGFsPXZhbGlkLnJlZHVjZSgocyxoKT0+cytOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSpOdW1iZXIoaC5zaGFyZXN8fDApLDApOwogIGlmKHRvdGFsPjApewogICAgbGV0IG1heEhvbGRpbmc9bnVsbCwgbWF4VmFsdWU9MDsKICAgIHZhbGlkLmZvckVhY2goaD0+ewogICAgICBjb25zdCB2PU51bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCk7CiAgICAgIGlmKHY+bWF4VmFsdWUpe21heFZhbHVlPXY7bWF4SG9sZGluZz1ofQogICAgfSk7CiAgICBjb25zdCBjb25jZW50cmF0aW9uPW1heFZhbHVlL3RvdGFsKjEwMDsKICAgIGlmKGNvbmNlbnRyYXRpb24+PTYwICYmIG1heEhvbGRpbmcpewogICAgICBhY3Rpb25zLnB1c2goewogICAgICAgIHNjb3JlOjcyLAogICAgICAgIGNsczoncHJpb3JpdHktbWlkJywKICAgICAgICB0aXRsZTon6ZuG5Lit5bqm44KS56K66KqNJywKICAgICAgICBkZXRhaWw6YCR7bWF4SG9sZGluZy5jb2RlfSR7bWF4SG9sZGluZy5jb21wYW55X25hbWU/JyAnK21heEhvbGRpbmcuY29tcGFueV9uYW1lOicnfSDjgYzjg53jg7zjg4jjg5Xjgqnjg6rjgqrjga4gJHtjb25jZW50cmF0aW9uLnRvRml4ZWQoMSl9JWAsCiAgICAgICAgbGFiZWw6J+ODneODvOODiOODleOCqeODquOCqicKICAgICAgfSk7CiAgICB9CiAgfQoKICBhY3Rpb25zLnNvcnQoKHgseSk9Pnkuc2NvcmUteC5zY29yZSk7CgogIGNvbnN0IGltcG9ydGFudD1hY3Rpb25zLmZpbHRlcih4PT54LnNjb3JlPj03MCk7CiAgY29uc3Qgc2hvd249KGltcG9ydGFudC5sZW5ndGg/aW1wb3J0YW50OmFjdGlvbnMpLnNsaWNlKDAsNCk7CgogIGJveC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9InByaW9yaXR5LXdyYXAiPiR7CiAgICBzaG93bi5tYXAoKHgsaSk9PmAKICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktaXRlbSAke3guY2xzfSI+CiAgICAgICAgPGRpdiBjbGFzcz0icHJpb3JpdHktbGluZSI+CiAgICAgICAgICA8ZGl2PgogICAgICAgICAgICA8c3BhbiBjbGFzcz0icHJpb3JpdHktcmFuayI+UFJJT1JJVFkgJHtpKzF9PC9zcGFuPgogICAgICAgICAgICA8Yj4ke3gudGl0bGV9PC9iPgogICAgICAgICAgPC9kaXY+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwcmlvcml0eS1jb2RlIj4ke3gubGFiZWx9PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjVweCI+JHt4LmRldGFpbH08L2Rpdj4KICAgICAgPC9kaXY+CiAgICBgKS5qb2luKCcnKQogIH08L2Rpdj5gICsgKAogICAgaW1wb3J0YW50Lmxlbmd0aAogICAgICA/ICc8cCBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+4oC75YSq5YWI5bqm44Gv6Kit5a6a44Op44Kk44Oz5o6l6L+R44O75Yik5a6a54q25oWL44O744OH44O844K/5pyJ54Sh44O76ZuG5Lit5bqm44GL44KJ5L2c44KL56K66KqN6aCG44Gn44GZ44CC6Ieq5YuV5aOy6LK35oyH56S644Gn44Gv44GC44KK44G+44Gb44KT44CCPC9wPicKICAgICAgOiAnPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPue3iuaApeW6puOBrumrmOOBhOmgheebruOBr+OBguOCiuOBvuOBm+OCk+OAgumAmuW4uOebo+imluOCkue2mee2muOAgjwvcD4nCiAgKTsKfQoKZnVuY3Rpb24gcG9ydGZvbGlvTWVhbkZvcihhLGtleSl7CiAgY29uc3QgdmFsaWQ9YS5maWx0ZXIoaD0+TnVtYmVyLmlzRmluaXRlKE51bWJlcihoLmN1cnJlbnRfcHJpY2UpKSYmTnVtYmVyKGguY3VycmVudF9wcmljZSk+MCk7CiAgY29uc3QgdG90YWw9dmFsaWQucmVkdWNlKChzLGgpPT5zK051bWJlcihoLmN1cnJlbnRfcHJpY2UpKk51bWJlcihoLnNoYXJlc3x8MCksMCk7CiAgaWYodG90YWw8PTApcmV0dXJuIG51bGw7CgogIGxldCBudW09MCwgZGVuPTA7CiAgdmFsaWQuZm9yRWFjaChoPT57CiAgICBjb25zdCBzdD1zdGF0Rm9yKGgsa2V5KTsKICAgIGNvbnN0IHY9TnVtYmVyKGguY3VycmVudF9wcmljZSkqTnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGlmKHN0JiZzdC5zdGF0dXM9PT0nb2snJiZOdW1iZXIuaXNGaW5pdGUoTnVtYmVyKHN0Lm1lYW4pKSYmdj4wKXsKICAgICAgbnVtICs9IHYqTnVtYmVyKHN0Lm1lYW4pOwogICAgICBkZW4gKz0gdjsKICAgIH0KICB9KTsKICByZXR1cm4gZGVuPjA/bnVtL2RlbjpudWxsOwp9CgpmdW5jdGlvbiByZW5kZXJQb3J0Zm9saW9TdW1tYXJ5KCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBib3g9JCgncG9ydGZvbGlvU3VtbWFyeScpOwogIGlmKCFib3gpcmV0dXJuOwoKICBpZighYS5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPHAgY2xhc3M9Im11dGVkIj7kv53mnInmoKrjgpLnmbvpjLLjgZnjgovjgajoh6rli5Xpm4boqIjjgZfjgb7jgZnjgII8L3A+JzsKICAgIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwogICAgcmV0dXJuOwogIH0KCiAgbGV0IHRvdGFsQ29zdD0wLCB0b3RhbFZhbHVlPTAsIHRvdGFsTmV0PTA7CiAgY29uc3Qgcm93cz1bXTsKICBjb25zdCBkZWNpc2lvbnM9e2hvbGQ6MCx3YXRjaDowLHRha2U6MCxzdG9wOjAscGVuZGluZzowfTsKCiAgYS5mb3JFYWNoKGg9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CiAgICBjb25zdCBzaGFyZXM9TnVtYmVyKGguc2hhcmVzfHwwKTsKICAgIGNvbnN0IHZhbGlkPU51bWJlci5pc0Zpbml0ZShjdXIpJiZjdXI+MCYmaC5hc29mOwogICAgY29uc3QgY3VycmVudFZhbHVlPXZhbGlkP2N1cipzaGFyZXM6MDsKICAgIGNvbnN0IGludmVzdGVkPU51bWJlcihoLmNvc3R8fDApKnNoYXJlcytjLmJ1eUZlZTsKCiAgICB0b3RhbENvc3QgKz0gaW52ZXN0ZWQ7CgogICAgaWYodmFsaWQpewogICAgICB0b3RhbFZhbHVlICs9IGN1cnJlbnRWYWx1ZTsKICAgICAgdG90YWxOZXQgKz0gYy5uZXROb3c7CgogICAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICAgIGlmKGQubGFiZWw9PT0n5pCN5YiH44KK5qSc6KiOJylkZWNpc2lvbnMuc3RvcCsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n5Yip56K65qSc6KiOJylkZWNpc2lvbnMudGFrZSsrOwogICAgICBlbHNlIGlmKGQubGFiZWw9PT0n6K2m5oiSJylkZWNpc2lvbnMud2F0Y2grKzsKICAgICAgZWxzZSBpZihkLmxhYmVsPT09J+WIpOWumuS/neeVmSd8fGQubGFiZWw9PT0n5Yik5a6a5L+d55WZ77yI5qCq5L6h5Y+k44GE77yJJ3x8ZC5sYWJlbD09PSfliKTlrprkv53nlZnvvIjlsaXmrbTlj6TjgYTvvIknKWRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIGVsc2UgZGVjaXNpb25zLmhvbGQrKzsKCiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6Y3VycmVudFZhbHVlLG5ldDpjLm5ldE5vd30pOwogICAgfWVsc2V7CiAgICAgIGRlY2lzaW9ucy5wZW5kaW5nKys7CiAgICAgIHJvd3MucHVzaCh7Y29kZTpoLmNvZGUsbmFtZTpoLmNvbXBhbnlfbmFtZXx8JycsdmFsdWU6MCxuZXQ6bnVsbH0pOwogICAgfQogIH0pOwoKICBjb25zdCBuZXRQY3Q9dG90YWxDb3N0PjA/dG90YWxOZXQvdG90YWxDb3N0KjEwMDpudWxsOwogIGNvbnN0IG1heFZhbHVlPXJvd3MucmVkdWNlKChtLHIpPT5NYXRoLm1heChtLHIudmFsdWUpLDApOwogIGNvbnN0IGNvbmNlbnRyYXRpb249dG90YWxWYWx1ZT4wP21heFZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CgogIGNvbnN0IG1lYW4yMD1wb3J0Zm9saW9NZWFuRm9yKGEsJzIwZCcpOwogIGNvbnN0IG1lYW4xMjY9cG9ydGZvbGlvTWVhbkZvcihhLCcxMjZkJyk7CiAgY29uc3QgbWVhbjI1Mj1wb3J0Zm9saW9NZWFuRm9yKGEsJzI1MmQnKTsKCiAgY29uc3Qgc3RhbGVIb2xkaW5ncz1hLmZpbHRlcihoPT57CiAgICBjb25zdCBmPWZyZXNobmVzc0ZvcihoLmFzb2YpOwogICAgcmV0dXJuIGguYXNvZiAmJiAhZi5kZWNpc2lvbl9vazsKICB9KTsKICBjb25zdCBzdGFsZU5vdGljZT1zdGFsZUhvbGRpbmdzLmxlbmd0aAogICAgPyBgPGRpdiBjbGFzcz0iZnJlc2hib3ggZnJlc2gtc3RhbGUiIHN0eWxlPSJtYXJnaW4tYm90dG9tOjlweCI+PGI+8J+bkSDlj6TjgYTmoKrkvqHjg4fjg7zjgr8gJHtzdGFsZUhvbGRpbmdzLmxlbmd0aH3pipjmn4Q8L2I+PGRpdiBjbGFzcz0ibXV0ZWQiPuipleS+oemhjeODu+aQjeebiuOBr+acgOaWsOWPluW+l+e1guWApOODmeODvOOCueOBruWPguiAg+WApOOBp+OBmeOAguS7iuaXpeOBruWjsuiyt+WIpOaWreOBq+OBr+S9v+OCj+OBmuOAgeWun+mam+OBruePvuWcqOWApOOCkueiuuiqjeOBl+OBpuOBj+OBoOOBleOBhOOAgjwvZGl2PjwvZGl2PmAKICAgIDogJyc7CgogIGNvbnN0IHN0YWxlSGlzdG9yeT1hLmZpbHRlcihoPT57CiAgICBjb25zdCBmPWZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKTsKICAgIHJldHVybiAoaC5oaXN0b3J5X2Fzb2Z8fGguYXNvZikgJiYgIWYuZGVjaXNpb25fb2s7CiAgfSk7CiAgY29uc3QgaGlzdG9yeU5vdGljZT1zdGFsZUhpc3RvcnkubGVuZ3RoCiAgICA/IGA8ZGl2IGNsYXNzPSJoaXN0b3J5d2FybiIgc3R5bGU9Im1hcmdpbi1ib3R0b206OXB4Ij48Yj7wn5OaIOS+oeagvOWxpeattOOBjOWPpOOBhCAke3N0YWxlSGlzdG9yeS5sZW5ndGh96YqY5p+EPC9iPjxkaXYgY2xhc3M9Im11dGVkIj7kvqHmoLzlsaXmrbTjgYzlj6TjgYTloLTlkIjjgIHnn63kuK3plbfmnJ/jg4jjg6zjg7Pjg4njg7vmnJ/lvoXlgKTjg7vpnIDntaZwcm94eeOBr+WPguiAg+WApOOBp+OBmeOAgjwvZGl2PjwvZGl2PmAKICAgIDogJyc7CgogIGNvbnN0IGFsbG9jYXRpb25zPXJvd3MKICAgIC5maWx0ZXIocj0+ci52YWx1ZT4wKQogICAgLnNvcnQoKHgseSk9PnkudmFsdWUteC52YWx1ZSkKICAgIC5tYXAocj0+ewogICAgICBjb25zdCB3PXRvdGFsVmFsdWU+MD9yLnZhbHVlL3RvdGFsVmFsdWUqMTAwOjA7CiAgICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYWxsb2MiPgogICAgICAgIDxkaXYgY2xhc3M9Im11dGVkIj48Yj4ke3IuY29kZX08L2I+JHtyLm5hbWU/JyAnK3IubmFtZTonJ30gLyAke3cudG9GaXhlZCgxKX0lPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0iYWxsb2NiYXIiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke01hdGgubWluKDEwMCx3KX0lIj48L3NwYW4+PC9kaXY+CiAgICAgIDwvZGl2PmA7CiAgICB9KS5qb2luKCcnKTsKCiAgYm94LmlubmVySFRNTD1gCiAgICAke3N0YWxlTm90aWNlfQogICAgJHtoaXN0b3J5Tm90aWNlfQogICAgPGRpdiBjbGFzcz0icG9ydHJvdyI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPue3j+aKleizh+mhjTwvc3Bhbj48Yj4ke3llbih0b3RhbENvc3QpfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Y+W5b6X57WC5YCk44OZ44O844K56KmV5L6h6aGNPC9zcGFuPjxiPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbFZhbHVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWPluW+l+e1guWApOODmeODvOOCueaQjeebijwvc3Bhbj48YiBjbGFzcz0iJHt0b3RhbE5ldD49MD8ncG9zJzonbmVnJ30iPiR7dG90YWxWYWx1ZT4wP3llbih0b3RhbE5ldCk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHtuZXRQY3Q9PT1udWxsPyfigJQnOm5ldFBjdC50b0ZpeGVkKDIpKyclJ308L3NwYW4+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuacgOWkp+mKmOafhOavlOeOhzwvc3Bhbj48Yj4ke3RvdGFsVmFsdWU+MD9jb25jZW50cmF0aW9uLnRvRml4ZWQoMSkrJyUnOifigJQnfTwvYj48L2Rpdj4KICAgIDwvZGl2PgoKICAgIDxkaXYgY2xhc3M9InBvcnRyb3ciIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgIDxkaXYgY2xhc3M9InBvcnRtaW5pIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuS/neaciee2mee2mjwvc3Bhbj48Yj4ke2RlY2lzaW9ucy5ob2xkfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6K2m5oiSPC9zcGFuPjxiPiR7ZGVjaXNpb25zLndhdGNofTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0icG9ydG1pbmkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+5Yip56K65qSc6KiOPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnRha2V9PC9iPjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJwb3J0bWluaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgoov5L+d55WZPC9zcGFuPjxiPiR7ZGVjaXNpb25zLnN0b3ArZGVjaXNpb25zLnBlbmRpbmd9PC9iPjwvZGl2PgogICAgPC9kaXY+CgogICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TiiDoqZXkvqHpoY3liqDph43jga7pgY7ljrvlubPlnYfjg6rjgr/jg7zjg7M8L2g0PgogICAgPGRpdiBjbGFzcz0iZ3JpZDMiPgogICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+55+t5pyfMjDml6U8L3NwYW4+PGI+JHttZWFuMjA9PT1udWxsPyfigJQnOm1lYW4yMC50b0ZpeGVkKDIpKyclJ308L2I+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7kuK3mnJ8xMjbml6U8L3NwYW4+PGI+JHttZWFuMTI2PT09bnVsbD8n4oCUJzptZWFuMTI2LnRvRml4ZWQoMikrJyUnfTwvYj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPumVt+acnzI1MuaXpTwvc3Bhbj48Yj4ke21lYW4yNTI9PT1udWxsPyfigJQnOm1lYW4yNTIudG9GaXhlZCgyKSsnJSd9PC9iPjwvZGl2PgogICAgPC9kaXY+CiAgICA8cCBjbGFzcz0ibXV0ZWQiPuKAu+WQhOmKmOafhOOBrumBjuWOu+W5s+Wdh+ODquOCv+ODvOODs+OCkuePvuWcqOOBruipleS+oemhjeOBp+WKoOmHjeOBl+OBn+WPguiAg+WApOOBp+OBmeOAguebuOmWouOCkuiAg+aFruOBl+OBn+WwhuadpeS6iOa4rOOBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgNXB4Ij7wn5OmIOmKmOafhOani+aIkDwvaDQ+CiAgICAke2FsbG9jYXRpb25zfHwnPHAgY2xhc3M9Im11dGVkIj7lrp/jg4fjg7zjgr/mnKrlj5blvpc8L3A+J30KICBgOwogIHJlbmRlclByaW9yaXR5QWN0aW9ucygpOwp9CmZ1bmN0aW9uIHJlbmRlckhvbGRpbmdzKCl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBpZighYS5sZW5ndGgpeyQoJ2hvbGRpbmdzJykuaW5uZXJIVE1MPSc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nO3JlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTtyZXR1cm59CiAgJCgnaG9sZGluZ3MnKS5pbm5lckhUTUw9YS5tYXAoKGgsaSk9PnsKICAgIGNvbnN0IGM9Y2FsY0hvbGRpbmcoaCk7CiAgICBjb25zdCB2YWxpZFByaWNlPU51bWJlci5pc0Zpbml0ZShOdW1iZXIoaC5jdXJyZW50X3ByaWNlKSkmJk51bWJlcihoLmN1cnJlbnRfcHJpY2UpPjAmJmguYXNvZjsKICAgIGNvbnN0IGNscz12YWxpZFByaWNlPyhjLm5ldE5vdz49MD8ncG9zJzonbmVnJyk6Jyc7CiAgICBjb25zdCBkPWRlY2lzaW9uRm9yKGgsYyk7CiAgICBjb25zdCBjdXI9TnVtYmVyKGguY3VycmVudF9wcmljZSk7CgogICAgcmV0dXJuIGA8ZGl2IGNsYXNzPSJob2xkaW5nIj4KICAgICAgPGRpdiBjbGFzcz0iaG9sZGluZy1oZWFkIj4KICAgICAgICA8ZGl2PgogICAgICAgICAgPGI+JHtoLmNvZGV9PC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPiR7aC5jb21wYW55X25hbWV8fCIifTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9InJvdyI+CiAgICAgICAgICA8YnV0dG9uIGNsYXNzPSJzbWFsbGJ0biBzZWNvbmRhcnkiIG9uY2xpY2s9InJlZnJlc2hIb2xkaW5nKCR7aX0pIj7mm7TmlrA8L2J1dHRvbj4KICAgICAgICAgIDxidXR0b24gY2xhc3M9InNtYWxsYnRuIGRhbmdlciIgb25jbGljaz0icmVtb3ZlSG9sZGluZygke2l9KSI+5YmK6ZmkPC9idXR0b24+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuePvuWcqOWApOODh+ODvOOCv+aXpSAke2guYXNvZnx8J+KAlCd9IC8g5pyA5paw5Y+W5b6X57WC5YCkICR7dmFsaWRQcmljZT95ZW4oaC5jdXJyZW50X3ByaWNlKTon4oCUJ30gLyAke2guc2hhcmVzfeagqiAvIOWPluW+l+WNmOS+oSAke3llbihoLmNvc3QpfTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJtdXRlZCI+5qCq5L6h44K944O844K5ICR7aC5wcmljZV9zb3VyY2V8fCfigJQnfSAvIOS+oeagvOWxpeattCAke2guaGlzdG9yeV9hc29mfHwn4oCUJ30gJHtoLmhpc3Rvcnlfc291cmNlPycoJytoLmhpc3Rvcnlfc291cmNlKycpJzonJ308L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiPuWGjeioiOeulyAke2gudXBkYXRlZF9hdD9uZXcgRGF0ZShoLnVwZGF0ZWRfYXQpLnRvTG9jYWxlU3RyaW5nKCdqYS1KUCcpOifigJQnfSAvIOaQjeebiuODu+WbuuWumuODqeOCpOODs+i3nembouOBr+OBk+OBruacgOaWsOWPluW+l+e1guWApOOCkuS9v+eUqDwvZGl2PgogICAgICAke3ZhbGlkUHJpY2U/YDxkaXYgY2xhc3M9ImZyZXNoYm94ICR7ZnJlc2huZXNzRm9yKGguYXNvZikuY2xzfSI+PGI+JHsKICAgICAgICBmcmVzaG5lc3NGb3IoaC5hc29mKS5sZXZlbD09PSdmcmVzaCc/J+KchSDprq7luqZPSyc6CiAgICAgICAgZnJlc2huZXNzRm9yKGguYXNvZikubGV2ZWw9PT0nd2FybmluZyc/J+KaoO+4jyDpgYXlu7bms6jmhI8nOgogICAgICAgICfwn5uRIOWPpOOBhOagquS+oeODh+ODvOOCvycKICAgICAgfTwvYj48ZGl2IGNsYXNzPSJtdXRlZCI+JHtmcmVzaG5lc3NUZXh0KGguYXNvZil9PC9kaXY+PC9kaXY+YDonJ30KCiAgICAgIDxkaXYgY2xhc3M9ImRlY2lzaW9uICR7ZC5jbHN9Ij4KICAgICAgICAke2QubGFiZWx9CiAgICAgICAgPGRpdiBjbGFzcz0ibXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjRweCI+JHtkLnJlYXNvbn08L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6OHB4Ij4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7mkI3liIfjgorjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMuc3RvcFByaWNlLCdzdG9wJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy5zdG9wUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrjgb7jgac8L3NwYW4+CiAgICAgICAgICA8YiBjbGFzcz0iZGlzdGFuY2UiPiR7dmFsaWRQcmljZT9kaXN0YW5jZUluZm8oY3VyLGMudGFrZVByaWNlLCd0YWtlJyk6J+KAlCd9PC9iPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7lj4LogIMgJHt5ZW4oYy50YWtlUHJpY2UpfTwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPgogICAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7liKTlrprkv6HpoLzluqY8L3NwYW4+CiAgICAgICAgICA8Yj4ke2QuY29uZmlkZW5jZX0lPC9iPgogICAgICAgICAgPGRpdiBjbGFzcz0iZ2F1Z2UiPjxzcGFuIHN0eWxlPSJ3aWR0aDoke2QuY29uZmlkZW5jZX0lIj48L3NwYW4+PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPHAgY2xhc3M9Im11dGVkIiBzdHlsZT0ibWFyZ2luLXRvcDo1cHgiPuKAu+WIpOWumuS/oemgvOW6puOBr+OAgeODh+ODvOOCv+WFhei2s+W6puODu+acn+mWk+ODiOODrOODs+ODieOBruS4gOiHtOW6puODu+acn+W+heWApOe1seioiOOBruacieeEoeOBi+OCieS9nOOCi+WPguiAg+aMh+aomeOBp+OAgeeahOS4reeiuueOh+OBp+OBr+OBguOCiuOBvuOBm+OCk+OAgjwvcD4KCiAgICAgIDxkaXYgY2xhc3M9ImFjdGlvbmJveCI+CiAgICAgICAgPHNwYW4gY2xhc3M9Im11dGVkIj7ku4rjganjgYbjgZnjgovvvJ88L3NwYW4+CiAgICAgICAgPGIgc3R5bGU9ImRpc3BsYXk6YmxvY2s7bWFyZ2luLXRvcDozcHgiPiR7YWN0aW9uVGV4dChoLGMsZCl9PC9iPgogICAgICA8L2Rpdj4KCiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDo4cHgiPgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7lj5blvpfntYLlgKTjg5njg7zjgrnmkI3nm4o8L3NwYW4+PGIgY2xhc3M9IiR7Y2xzfSI+JHt2YWxpZFByaWNlP3llbihjLm5ldE5vdyk6J+KAlCd9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+JHt2YWxpZFByaWNlP2ZtdChjLm5ldE5vd1BjdCkrJyUnOifigJQnfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+6LK35LuY5omL5pWw5paZPC9zcGFuPjxiPiR7eWVuKGMuYnV5RmVlKX08L2I+PC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuWjsuWNtOaJi+aVsOaWmSjku4opPC9zcGFuPjxiPiR7dmFsaWRQcmljZT95ZW4oYy5zZWxsRmVlKTon4oCUJ308L2I+PC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjhweCI+CiAgICAgICAgPGRpdiBjbGFzcz0ia3BpIj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaQjeWIh+OCiuWPguiAgzwvc3Bhbj48Yj4ke3llbihjLnN0b3BQcmljZSl9PC9iPjxzcGFuIGNsYXNzPSJtdXRlZCI+5omL5pWw5paZ6L68ICR7eWVuKGMuc3RvcE5ldCl9PC9zcGFuPjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImtwaSI+PHNwYW4gY2xhc3M9Im11dGVkIj7liKnnorrlj4LogIM8L3NwYW4+PGI+JHt5ZW4oYy50YWtlUHJpY2UpfTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuaJi+aVsOaWmei+vCAke3llbihjLnRha2VOZXQpfTwvc3Bhbj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJrcGkiPjxzcGFuIGNsYXNzPSJtdXRlZCI+44OI44Os44O844Oq44Oz44Kw5Y+C6ICDPC9zcGFuPjxiPiR7dmFsaWRQcmljZSYmYy50cmFpbFByaWNlIT09bnVsbD95ZW4oYy50cmFpbFByaWNlKTon4oCUJ308L2I+PHNwYW4gY2xhc3M9Im11dGVkIj4ke2ZyZXNobmVzc0ZvcihoLmhpc3RvcnlfYXNvZnx8aC5hc29mKS5kZWNpc2lvbl9vaz8nMjDml6Xpq5jlgKTln7rmupYnOiflsaXmrbTjgYzlj6TjgYTjgZ/jgoHkv53nlZknfTwvc3Bhbj48L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8aDQgc3R5bGU9Im1hcmdpbjoxMnB4IDAgN3B4Ij7wn46vIOWun+e4vuODmeODvOOCueS+oeagvOODrOODs+OCuDwvaDQ+CiAgICAgIDxkaXYgY2xhc3M9ImdyaWQzIj4KICAgICAgICAke3JhbmdlSHRtbCgn55+t5pyfIDIw5pelJyxjdXIsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+S4reacnyAxMjbml6UnLGN1cixzdGF0Rm9yKGgsJzEyNmQnKSl9CiAgICAgICAgJHtyYW5nZUh0bWwoJ+mVt+acnyAyNTLml6UnLGN1cixzdGF0Rm9yKGgsJzI1MmQnKSl9CiAgICAgIDwvZGl2PgoKICAgICAgPGg0IHN0eWxlPSJtYXJnaW46MTJweCAwIDdweCI+8J+TkCDlrp/nuL7jg5njg7zjgrnmnJ/lvoXlgKTvvIjntbHoqIjlj4LogIPvvIk8L2g0PgogICAgICA8ZGl2IGNsYXNzPSJncmlkMyI+CiAgICAgICAgJHtldkh0bWwoJ+efreacnyAyMOaXpScsc3RhdEZvcihoLCcyMGQnKSl9CiAgICAgICAgJHtldkh0bWwoJ+S4reacnyAxMjbml6UnLHN0YXRGb3IoaCwnMTI2ZCcpKX0KICAgICAgICAke2V2SHRtbCgn6ZW35pyfIDI1MuaXpScsc3RhdEZvcihoLCcyNTJkJykpfQogICAgICA8L2Rpdj4KICAgICAgPHAgY2xhc3M9Im11dGVkIj7igLvkvqHmoLzjg6zjg7Pjgrjjg7vmnJ/lvoXlgKTjga/lsIbmnaXkuojmuKzjgafjga/jgarjgY/jgIHlj5blvpflj6/og73jgarpgY7ljrvmoKrkvqHjga7jg63jg7zjg6rjg7PjgrDliY3mlrnjg6rjgr/jg7zjg7PliIbluIPjgpLmnIDmlrDlj5blvpfntYLlgKTjgavlvZPjgabjga/jgoHjgZ/ntbHoqIjlj4LogIPjgafjgZnjgILmnJ/plpPjgYzph43jgarjgovmqJnmnKzjgpLlkKvjgb/jgb7jgZnjgII8L3A+CiAgICA8L2Rpdj5gOwogIH0pLmpvaW4oJycpOwogIHJlbmRlclBvcnRmb2xpb1N1bW1hcnkoKTsKfQoKYXN5bmMgZnVuY3Rpb24gZ2V0UXVvdGUoY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcXVvdGU/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHE9YXdhaXQgci5qc29uKCk7CiAgaWYocS5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcihxLnJlYXNvbnx8cS5lcnJvcnx8J+Wun+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiBxOwp9Cgphc3luYyBmdW5jdGlvbiBhZGRIb2xkaW5nKCl7CiAgY29uc3QgY29kZT0kKCdob2xkQ29kZScpLnZhbHVlLnRyaW0oKTsKICBjb25zdCBjb3N0PXZhbCgnaG9sZENvc3QnKSwgc2hhcmVzPXZhbCgnaG9sZFNoYXJlcycpOwogIGNvbnN0IHN0b3A9dmFsKCdzdG9wUGN0JyksIHRha2U9dmFsKCd0YWtlUGN0JyksIHRyYWlsPXZhbCgndHJhaWxQY3QnKTsKICBjb25zdCBmZWVNb2RlPSQoJ2ZlZU1vZGUnKS52YWx1ZTsKICBpZighY29kZXx8IWNvc3R8fCFzaGFyZXMpe2FsZXJ0KCfpipjmn4TjgrPjg7zjg4njg7vlj5blvpfljZjkvqHjg7vmoKrmlbDjgpLlhaXlipvjgZfjgabjga0nKTtyZXR1cm59CiAgY29uc3QgYnRuPWV2ZW50Py50YXJnZXQ7IGlmKGJ0bil7YnRuLmRpc2FibGVkPXRydWU7YnRuLnRleHRDb250ZW50PSflj5blvpfkuK3igKYnfQogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICBjb25zdCBhPWxvY2FsKCdmcmVlX2hvbGRpbmdzX3YxMycpOwogICAgY29uc3QgaD17CiAgICAgIGNvZGUsCiAgICAgIGNvbXBhbnlfbmFtZToocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fCcnLAogICAgICBjb21wYW55X21hcmtldDoocS5jb21wYW55JiZxLmNvbXBhbnkubWFya2V0KXx8JycsCiAgICAgIGNvbXBhbnlfc2VjdG9yMzM6KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8JycsCiAgICAgIGNvc3QsIHNoYXJlcywgZmVlX21vZGU6ZmVlTW9kZSwKICAgICAgc3RvcF9wY3Q6c3RvcD8/OCwgdGFrZV9wY3Q6dGFrZT8/MTUsIHRyYWlsX3BjdDp0cmFpbD8/NywKICAgICAgY3VycmVudF9wcmljZTpzLmxhc3RfY2xvc2UsIGhpZ2hfMjBkOnMuaGlnaF8yMGQsIGxvd18yMGQ6cy5sb3dfMjBkLAogICAgICByZXR1cm5fMjBkOnMucmV0dXJuXzIwZCwgcmV0dXJuXzEyNmQ6cy5yZXR1cm5fMTI2ZCwgcmV0dXJuXzI1MmQ6cy5yZXR1cm5fMjUyZCwKICAgICAgZm9yd2FyZF9zdGF0czpzLmZvcndhcmRfcmV0dXJuX3N0YXRzfHx7fSwKICAgICAgYXNvZjpzLmxhc3RfZGF0ZSwKICAgICAgaGlzdG9yeV9hc29mOnMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlLAogICAgICBwcmljZV9zb3VyY2U6cy5wcmljZV9zb3VyY2V8fHEuc291cmNlfHwnJywKICAgICAgaGlzdG9yeV9zb3VyY2U6cy5oaXN0b3J5X3NvdXJjZXx8JycsCiAgICAgIHVwZGF0ZWRfYXQ6bmV3IERhdGUoKS50b0lTT1N0cmluZygpLAogICAgICBwcmljZV9zeW5jZWQ6dHJ1ZQogICAgfTsKICAgIGNvbnN0IGlkeD1hLmZpbmRJbmRleCh4PT54LmNvZGU9PT1jb2RlKTsKICAgIGlmKGlkeD49MClhW2lkeF09aDsgZWxzZSBhLnB1c2goaCk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogIH1jYXRjaChlKXthbGVydCgn5Y+W5b6X44Ko44Op44O8OiAnK2UubWVzc2FnZSl9CiAgZmluYWxseXtpZihidG4pe2J0bi5kaXNhYmxlZD1mYWxzZTtidG4udGV4dENvbnRlbnQ9J+Wun+ODh+ODvOOCv+OBp+ioiOeul+OBl+OBpuS/neWtmCd9fQp9CgoKZnVuY3Rpb24gYXBwbHlRdW90ZVRvSG9sZGluZyhoLHEpewogIGNvbnN0IHM9KHEmJnEuc25hcHNob3QpfHx7fTsKICBoLmNvbXBhbnlfbmFtZT0ocS5jb21wYW55JiZxLmNvbXBhbnkubmFtZSl8fGguY29tcGFueV9uYW1lfHwnJzsKICBoLmNvbXBhbnlfbWFya2V0PShxLmNvbXBhbnkmJnEuY29tcGFueS5tYXJrZXQpfHxoLmNvbXBhbnlfbWFya2V0fHwnJzsKICBoLmNvbXBhbnlfc2VjdG9yMzM9KHEuY29tcGFueSYmcS5jb21wYW55LnNlY3RvcjMzKXx8aC5jb21wYW55X3NlY3RvcjMzfHwnJzsKICBoLmN1cnJlbnRfcHJpY2U9cy5sYXN0X2Nsb3NlOwogIGguaGlnaF8yMGQ9cy5oaWdoXzIwZDsKICBoLmxvd18yMGQ9cy5sb3dfMjBkOwogIGgucmV0dXJuXzIwZD1zLnJldHVybl8yMGQ7CiAgaC5yZXR1cm5fMTI2ZD1zLnJldHVybl8xMjZkOwogIGgucmV0dXJuXzI1MmQ9cy5yZXR1cm5fMjUyZDsKICBoLmZvcndhcmRfc3RhdHM9cy5mb3J3YXJkX3JldHVybl9zdGF0c3x8e307CiAgaC5hc29mPXMubGFzdF9kYXRlOwogIGguaGlzdG9yeV9hc29mPXMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlOwogIGgucHJpY2Vfc291cmNlPXMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8Jyc7CiAgaC5oaXN0b3J5X3NvdXJjZT1zLmhpc3Rvcnlfc291cmNlfHwnJzsKICBoLnVwZGF0ZWRfYXQ9bmV3IERhdGUoKS50b0lTT1N0cmluZygpOwogIGgucHJpY2Vfc3luY2VkPXRydWU7CiAgcmV0dXJuIGg7Cn0KCmZ1bmN0aW9uIHN5bmNBbmFseXplZFF1b3RlVG9Ib2xkaW5nKGNvZGUscSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTsKICBjb25zdCBpPWEuZmluZEluZGV4KHg9PlN0cmluZyh4LmNvZGUpPT09U3RyaW5nKGNvZGUpKTsKICBpZihpPDApcmV0dXJuIGZhbHNlOwogIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhhW2ldLHEpOwogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICByZW5kZXJIb2xkaW5ncygpOwogIHJldHVybiB0cnVlOwp9Cgphc3luYyBmdW5jdGlvbiByZWZyZXNoQWxsSG9sZGluZ3MoZm9yY2U9ZmFsc2UpewogIGNvbnN0IGE9bG9jYWwoJ2ZyZWVfaG9sZGluZ3NfdjEzJyk7CiAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgY29uc3QgYnRuPSQoJ3JlZnJlc2hBbGxCdG4nKTsKCiAgaWYoIWEubGVuZ3RoKXsKICAgIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9J+S/neacieagquOBr+acqueZu+mMsuOBp+OBmeOAgic7CiAgICByZXR1cm47CiAgfQoKICBjb25zdCBrZXk9J2ZyZWVfaG9sZGluZ3NfbGFzdF9hdXRvX3JlZnJlc2hfdjE2JzsKICBjb25zdCBsYXN0PU51bWJlcihsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrZXkpfHwwKTsKICBjb25zdCBub3dNcz1EYXRlLm5vdygpOwogIGNvbnN0IHdhaXRNcz0zMCo2MCoxMDAwOwoKICBpZighZm9yY2UgJiYgbGFzdCAmJiBub3dNcy1sYXN0PHdhaXRNcyl7CiAgICBjb25zdCBtaW49TWF0aC5jZWlsKCh3YWl0TXMtKG5vd01zLWxhc3QpKS82MDAwMCk7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWDoh6rli5Xmm7TmlrDmuIjjgb/jgILmrKHjga7oh6rli5Xmm7TmlrDjgb7jgafntIQke21pbn3liIbjgIJgOwogICAgcmV0dXJuOwogIH0KCiAgaWYoYnRuKXtidG4uZGlzYWJsZWQ9dHJ1ZTtidG4udGV4dENvbnRlbnQ9J+abtOaWsOS4reKApid9CiAgaWYoc3RhdHVzKXN0YXR1cy50ZXh0Q29udGVudD1g5L+d5pyJ5qCqICR7YS5sZW5ndGh96YqY5p+E44Gu5pyA5paw57WC5YCk44KS5Y+W5b6X5Lit4oCmYDsKCiAgbGV0IG9rPTAsIG5nPTA7CiAgZm9yKGxldCBpPTA7aTxhLmxlbmd0aDtpKyspewogICAgdHJ5ewogICAgICBjb25zdCBxPWF3YWl0IGdldFF1b3RlKGFbaV0uY29kZSk7CiAgICAgIGFbaV09YXBwbHlRdW90ZVRvSG9sZGluZyhhW2ldLHEpOwogICAgICBvaysrOwogICAgfWNhdGNoKGUpewogICAgICBuZysrOwogICAgICBhW2ldLmxhc3RfcmVmcmVzaF9lcnJvcj1TdHJpbmcoZS5tZXNzYWdlfHxlKTsKICAgIH0KICB9CgogIHNhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTsKICBsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrZXksU3RyaW5nKERhdGUubm93KCkpKTsKICByZW5kZXJIb2xkaW5ncygpOwoKICBpZihzdGF0dXMpewogICAgc3RhdHVzLnRleHRDb250ZW50PWDmnIDmlrDntYLlgKTjgaflho3oqIjnrpfvvJrmiJDlip8gJHtva33pipjmn4Qke25nP2AgLyDlpLHmlZcgJHtuZ33pipjmn4RgOicnfeOAgmA7CiAgfQogIGlmKGJ0bil7YnRuLmRpc2FibGVkPWZhbHNlO2J0bi50ZXh0Q29udGVudD0n5L+d5pyJ5qCq44KS5pyA5paw57WC5YCk44Gn5LiA5ous5pu05pawJ30KfQoKYXN5bmMgZnVuY3Rpb24gcmVmcmVzaEhvbGRpbmcoaSl7CiAgY29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKSwgaD1hW2ldOyBpZighaClyZXR1cm47CiAgdHJ5ewogICAgY29uc3QgcT1hd2FpdCBnZXRRdW90ZShoLmNvZGUpOwogICAgYVtpXT1hcHBseVF1b3RlVG9Ib2xkaW5nKGgscSk7CiAgICBzYXZlKCdmcmVlX2hvbGRpbmdzX3YxMycsYSk7CiAgICByZW5kZXJIb2xkaW5ncygpOwogICAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgICBpZihzdGF0dXMpc3RhdHVzLnRleHRDb250ZW50PWAke2guY29kZX0g44KS5pyA5paw5Y+W5b6X57WC5YCk44Gn5YaN6KiI566X44GX44G+44GX44Gf44CCYDsKICB9Y2F0Y2goZSl7YWxlcnQoJ+abtOaWsOOCqOODqeODvDogJytlLm1lc3NhZ2UpfQp9CmZ1bmN0aW9uIHJlbW92ZUhvbGRpbmcoaSl7Y29uc3QgYT1sb2NhbCgnZnJlZV9ob2xkaW5nc192MTMnKTthLnNwbGljZShpLDEpO3NhdmUoJ2ZyZWVfaG9sZGluZ3NfdjEzJyxhKTtyZW5kZXJIb2xkaW5ncygpfQoKZnVuY3Rpb24gcmVuZGVyV2F0Y2goKXsKICAkKCd3YXRjaHMnKS5pbm5lckhUTUw9bG9jYWwoJ2ZyZWVfd2F0Y2gnKS5tYXAoeD0+ewogICAgaWYodHlwZW9mIHg9PT0nc3RyaW5nJylyZXR1cm4gYDxkaXYgY2xhc3M9ImJhZGdlIj4ke3h9PC9kaXY+YDsKICAgIHJldHVybiBgPGRpdiBjbGFzcz0iYmFkZ2UiPjxiPiR7eC5jb2RlfTwvYj4ke3gubmFtZT8nICcreC5uYW1lOicnfTwvZGl2PmA7CiAgfSkuam9pbignJyl8fCc8cCBjbGFzcz0ibXV0ZWQiPuacqueZu+mMsjwvcD4nOwp9CmFzeW5jIGZ1bmN0aW9uIGFkZFdhdGNoKCl7CiAgbGV0IGM9JCgnd2F0Y2hDb2RlJykudmFsdWUudHJpbSgpOyBpZighYylyZXR1cm47CiAgbGV0IGluZm89bnVsbDsKICB0cnl7aW5mbz1hd2FpdCBnZXRDb21wYW55KGMpfWNhdGNoKGUpe30KICBsZXQgYT1sb2NhbCgnZnJlZV93YXRjaCcpOwogIGNvbnN0IGV4aXN0cz1hLnNvbWUoeD0+KHR5cGVvZiB4PT09J3N0cmluZyc/eDp4LmNvZGUpPT09Yyk7CiAgaWYoIWV4aXN0cylhLnB1c2goe2NvZGU6YyxuYW1lOmluZm8mJmluZm8ubmFtZT9pbmZvLm5hbWU6Jyd9KTsKICBzYXZlKCdmcmVlX3dhdGNoJyxhKTsKICByZW5kZXJXYXRjaCgpOwp9CmZ1bmN0aW9uIHVwZGF0ZUthYnV0YW4oKXtsZXQgYz0kKCdjb2RlJykudmFsdWUudHJpbSgpOyQoJ2thYnV0YW4nKS5ocmVmPWM/J2h0dHBzOi8va2FidXRhbi5qcC9zdG9jay8/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjKTonaHR0cHM6Ly9rYWJ1dGFuLmpwLyd9CiQoJ2NvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9Pnt1cGRhdGVLYWJ1dGFuKCk7c2NoZWR1bGVDb21wYW55TG9va3VwKCdjb2RlJywnY29tcGFueU5hbWUnLCcnKX0pO3VwZGF0ZUthYnV0YW4oKTsKJCgnaG9sZENvZGUnKS5hZGRFdmVudExpc3RlbmVyKCdpbnB1dCcsKCk9PnNjaGVkdWxlQ29tcGFueUxvb2t1cCgnaG9sZENvZGUnLCdob2xkQ29tcGFueU5hbWUnLCcnKSk7CiQoJ3dhdGNoQ29kZScpLmFkZEV2ZW50TGlzdGVuZXIoJ2lucHV0JywoKT0+c2NoZWR1bGVDb21wYW55TG9va3VwKCd3YXRjaENvZGUnLCd3YXRjaENvbXBhbnlOYW1lJywnJykpOwoKCgphc3luYyBmdW5jdGlvbiBnZXRQb2xpY3koY29kZSl7CiAgY29uc3Qgcj1hd2FpdCBmZXRjaCgnL2FwaS9tb2JpbGUvcG9saWN5P2NvZGU9JytlbmNvZGVVUklDb21wb25lbnQoY29kZSkse2NhY2hlOiduby1zdG9yZSd9KTsKICBjb25zdCB4PWF3YWl0IHIuanNvbigpOwogIGlmKHguc3RhdHVzIT09J29rJyl0aHJvdyBuZXcgRXJyb3IoeC5yZWFzb258fHguZXJyb3J8fCflm73nrZbjg4fjg7zjgr/jgpLlj5blvpfjgafjgY3jgb7jgZvjgpPjgafjgZfjgZ8nKTsKICByZXR1cm4geC5wb2xpY3l8fHt9Owp9CgpmdW5jdGlvbiBwb2xpY3lTb3VyY2VTdGF0dXNKYShzKXsKICBpZihzPT09J3ZlcmlmaWVkX2xpdmUnKXJldHVybiAn5YWs5byP44Oa44O844K456K66KqN5riIJzsKICBpZihzPT09J3BhcnRpYWxfbGl2ZScpcmV0dXJuICflhazlvI/jg5rjg7zjgrjpg6jliIbnorroqo0nOwogIGlmKHM9PT0ndmVyaWZpZWRfcmVnaXN0cnknKXJldHVybiAn5pyA57WC56K66KqN5riI5YWs5byP44K944O844K5JzsKICByZXR1cm4gJ+eiuuiqjeS4jeWPryc7Cn0KCmZ1bmN0aW9uIHJlbmRlclBvbGljeVRoZW1lcyhwKXsKICBjb25zdCBib3g9JCgncG9saWN5VGhlbWVzJyk7CiAgaWYoIWJveClyZXR1cm47CiAgY29uc3QgdGhlbWVzPShwJiZwLm1hdGNoZWRfdGhlbWVzKXx8W107CiAgaWYoIXRoZW1lcy5sZW5ndGgpewogICAgYm94LmlubmVySFRNTD0nPGRpdiBjbGFzcz0icG9saWN5dGhlbWUiPjxiPumWoumAo+ODhuODvOODnuOBquOBlyAvIOWIpOWumuS/neeVmTwvYj48c3BhbiBjbGFzcz0ibXV0ZWQiPuacgOS9jumWoumAo+W6puOCkua6gOOBn+OBmeWFrOW8j+aUv+etluODhuODvOODnuOBjOOBguOCiuOBvuOBm+OCk+OAgjwvc3Bhbj48L2Rpdj4nOwogICAgcmV0dXJuOwogIH0KICBib3guaW5uZXJIVE1MPXRoZW1lcy5zbGljZSgwLDQpLm1hcCh0PT5gCiAgICA8ZGl2IGNsYXNzPSJwb2xpY3l0aGVtZSI+CiAgICAgIDxiPiR7dC5uYW1lfSAvIOmWoumAo+W6piAkeyhOdW1iZXIodC5yZWxldmFuY2UpKjEwMCkudG9GaXhlZCgwKX0lPC9iPgogICAgICA8c3BhbiBjbGFzcz0ibXV0ZWQiPuaUv+etluW8t+W6piAke051bWJlcih0LnBvbGljeV9zdHJlbmd0aCkudG9GaXhlZCgwKX0gLyDlr4TkuI4gJHtOdW1iZXIodC5jb250cmlidXRpb24pLnRvRml4ZWQoMSl9IC8gJHtwb2xpY3lTb3VyY2VTdGF0dXNKYSh0LnNvdXJjZV9zdGF0dXMpfTwvc3Bhbj48YnI+CiAgICAgIDxhIGhyZWY9IiR7dC51cmx9IiB0YXJnZXQ9Il9ibGFuayIgcmVsPSJub29wZW5lciI+5YWs5byP44K944O844K5PC9hPgogICAgPC9kaXY+CiAgYCkuam9pbignJyk7Cn0KCmFzeW5jIGZ1bmN0aW9uIGdldEZ1bmRhbWVudGFscyhjb2RlKXsKICBjb25zdCByPWF3YWl0IGZldGNoKCcvYXBpL21vYmlsZS9mdW5kYW1lbnRhbHM/Y29kZT0nK2VuY29kZVVSSUNvbXBvbmVudChjb2RlKSx7Y2FjaGU6J25vLXN0b3JlJ30pOwogIGNvbnN0IHg9YXdhaXQgci5qc29uKCk7CiAgaWYoeC5zdGF0dXMhPT0nb2snKXRocm93IG5ldyBFcnJvcih4LnJlYXNvbnx8eC5lcnJvcnx8J+axuueul+ODh+ODvOOCv+OCkuWPluW+l+OBp+OBjeOBvuOBm+OCk+OBp+OBl+OBnycpOwogIHJldHVybiB4LmZ1bmRhbWVudGFsc3x8e307Cn0KCmZ1bmN0aW9uIHNjb3JlTGFiZWwodil7CiAgaWYodj09PW51bGx8fHY9PT11bmRlZmluZWR8fE51bWJlci5pc05hTihOdW1iZXIodikpKXJldHVybiAn4oCUJzsKICBjb25zdCBuPU51bWJlcih2KTsKICByZXR1cm4gKG4+MD8nKyc6JycpK24udG9GaXhlZCgxKTsKfQoKZnVuY3Rpb24gcGN0TWF5YmUodil7CiAgcmV0dXJuICh2PT09bnVsbHx8dj09PXVuZGVmaW5lZHx8TnVtYmVyLmlzTmFOKE51bWJlcih2KSkpPyfigJQnOk51bWJlcih2KS50b0ZpeGVkKDEpKyclJzsKfQoKYXN5bmMgZnVuY3Rpb24gYW5hbHl6ZSgpewogIGNvbnN0IGNvZGU9JCgnY29kZScpLnZhbHVlLnRyaW0oKTsKICBpZighY29kZSl7JCgncmVzdWx0JykudGV4dENvbnRlbnQ9J+mKmOafhOOCs+ODvOODieOCkuWFpeWKm+OBl+OBpuOBrSc7cmV0dXJufQogIGNvbnN0IGJ0bj0kKCdhbmFseXplQnRuJyk7IGJ0bi5kaXNhYmxlZD10cnVlOyBidG4udGV4dENvbnRlbnQ9J+WPluW+l+S4reKApic7CiAgJCgncmVzdWx0JykudGV4dENvbnRlbnQ9J0Zpbk1pbmTntIQ5MDDml6XkvqHmoLzlsaXmrbTvvItKLVF1YW50c+axuueul+ODh+ODvOOCv+OCkuWPluW+l+OBl+OBpuOBhOOBvuOBmeKApic7CgogIHRyeXsKICAgIGNvbnN0IHE9YXdhaXQgZ2V0UXVvdGUoY29kZSksIHM9cS5zbmFwc2hvdHx8e307CiAgICAkKCdwcmljZScpLnZhbHVlPXMubGFzdF9jbG9zZT09bnVsbD8nJzpmbXQocy5sYXN0X2Nsb3NlLDEpOwogICAgJCgncjIwJykudmFsdWU9Zm10KHMucmV0dXJuXzIwZCk7JCgncjEyNicpLnZhbHVlPWZtdChzLnJldHVybl8xMjZkKTskKCdyMjUyJykudmFsdWU9Zm10KHMucmV0dXJuXzI1MmQpOwogICAgJCgnaGlnaDIwJykudGV4dENvbnRlbnQ9Zm10KHMuaGlnaF8yMGQsMSk7JCgnbG93MjAnKS50ZXh0Q29udGVudD1mbXQocy5sb3dfMjBkLDEpOwogICAgJCgndm9sMjAnKS50ZXh0Q29udGVudD1zLnZvbGF0aWxpdHlfMjBkX2FubnVhbGl6ZWQ9PW51bGw/J+KAlCc6Zm10KHMudm9sYXRpbGl0eV8yMGRfYW5udWFsaXplZCkrJyUnOwoKICAgIGRpc3BsYXlDb21wYW55KCQoJ2NvbXBhbnlOYW1lJykscS5jb21wYW55fHxudWxsLCcnKTsKICAgIGNvbnN0IHByaWNlU291cmNlPXMucHJpY2Vfc291cmNlfHxxLnNvdXJjZXx8J+S4jeaYjic7CiAgICBjb25zdCBoaXN0b3J5RGF0ZT1zLmhpc3RvcnlfbGFzdF9kYXRlfHxudWxsOwogICAgJCgnc291cmNlQm94JykuaW5uZXJIVE1MPQogICAgICAnPHNwYW4gY2xhc3M9InNvdXJjZWJhZGdlIj7nj77lnKjlgKQ8L3NwYW4+PGIgY2xhc3M9Im9rIj4nK3ByaWNlU291cmNlKyc8L2I+JysKICAgICAgJzxicj7nj77lnKjlgKTjg4fjg7zjgr/ml6U6ICcrKHMubGFzdF9kYXRlfHwn4oCUJykrCiAgICAgIChzLnByaWNlX3RpbWU/JyAnK3MucHJpY2VfdGltZTonJykrCiAgICAgICcgLyDmnIDmlrDlj5blvpflgKQ6ICcrZm10KHMubGFzdF9jbG9zZSwxKSsKICAgICAgJzxicj48c3BhbiBjbGFzcz0ic291cmNlYmFkZ2UiPuS+oeagvOWxpeattDwvc3Bhbj4nKwogICAgICAocy5oaXN0b3J5X3NvdXJjZXx8J+WPluW+l+OBquOBlycpKwogICAgICAnIC8g5pyA57WC5pelOiAnKyhoaXN0b3J5RGF0ZXx8J+KAlCcpKwogICAgICAnIC8g5bGl5q2044K144Oz44OX44OrOiAnKyhzLnNhbXBsZV9jb3VudD8/MCkrJ+S7tic7CiAgICBzaG93RnJlc2huZXNzKHMubGFzdF9kYXRlKTsKICAgIHNob3dIaXN0b3J5RnJlc2huZXNzKGhpc3RvcnlEYXRlLHMubGFzdF9kYXRlKTsKCiAgICBjb25zdCBzeW5jZWRIb2xkaW5nPXN5bmNBbmFseXplZFF1b3RlVG9Ib2xkaW5nKGNvZGUscSk7CiAgICBpZihzeW5jZWRIb2xkaW5nKXsKICAgICAgY29uc3Qgc3RhdHVzPSQoJ2hvbGRpbmdSZWZyZXNoU3RhdHVzJyk7CiAgICAgIGlmKHN0YXR1cylzdGF0dXMudGV4dENvbnRlbnQ9YCR7Y29kZX0g44Gu5L+d5pyJ5qCq44KS5YiG5p6Q5pmC44Gu5pyA5paw5Y+W5b6X57WC5YCkICR7Zm10KHMubGFzdF9jbG9zZSwxKX0g44Gn5YaN6KiI566X44GX44G+44GX44Gf44CCYDsKICAgIH0KCiAgICAkKCdhbmFseXNpc0V2JykuaW5uZXJIVE1MPQogICAgICBldkh0bWwoJ+efreacnzIw5pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycyMGQnXSkrCiAgICAgIGV2SHRtbCgn5Lit5pyfMTI25pelJyxzLmZvcndhcmRfcmV0dXJuX3N0YXRzJiZzLmZvcndhcmRfcmV0dXJuX3N0YXRzWycxMjZkJ10pKwogICAgICBldkh0bWwoJ+mVt+acnzI1MuaXpScscy5mb3J3YXJkX3JldHVybl9zdGF0cyYmcy5mb3J3YXJkX3JldHVybl9zdGF0c1snMjUyZCddKTsKCiAgICBjb25zdCBzdXBwbHk9cy5zdXBwbHlfcHJveHl8fHt9OwogICAgJCgnc3VwcGx5QXV0bycpLnRleHRDb250ZW50PXNjb3JlTGFiZWwoc3VwcGx5LnNjb3JlKTsKICAgICQoJ3N1cHBseURldGFpbCcpLnRleHRDb250ZW50PQogICAgICAnNeaXpS8yMOaXpeWHuuadpemrmCAnKyhzdXBwbHkudm9sdW1lX3JhdGlvXzVfMjA9PW51bGw/J+KAlCc6TnVtYmVyKHN1cHBseS52b2x1bWVfcmF0aW9fNV8yMCkudG9GaXhlZCgyKSsn5YCNJyk7CgogICAgbGV0IGZ1bmRhbWVudGFscz17fTsKICAgIHRyeXsKICAgICAgZnVuZGFtZW50YWxzPWF3YWl0IGdldEZ1bmRhbWVudGFscyhjb2RlKTsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD1zY29yZUxhYmVsKGZ1bmRhbWVudGFscy5zY29yZSk7CiAgICAgIGNvbnN0IG09ZnVuZGFtZW50YWxzLm1ldHJpY3N8fHt9OwogICAgICAkKCdlYXJuRGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgJ+mWi+ekuiAnKygoZnVuZGFtZW50YWxzLmxhdGVzdCYmZnVuZGFtZW50YWxzLmxhdGVzdC5kYXRlKXx8J+KAlCcpKwogICAgICAgICcgLyDlo7LkuIogJytwY3RNYXliZShtLnNhbGVzX2dyb3d0aF9wY3QpKwogICAgICAgICcgLyDllrbmpa3nm4ogJytwY3RNYXliZShtLm9wX2dyb3d0aF9wY3QpOwogICAgfWNhdGNoKGZlKXsKICAgICAgJCgnZWFybkF1dG8nKS50ZXh0Q29udGVudD0n5LiN5piOJzsKICAgICAgJCgnZWFybkRldGFpbCcpLnRleHRDb250ZW50PSfjgZPjga7jg5fjg6njg7Mv6YqY5p+E44Gn44Gv5Y+W5b6X44Gn44GN44Gq44GE5Y+v6IO95oCn44GC44KKJzsKICAgICAgZnVuZGFtZW50YWxzPXtzY29yZTpudWxsfTsKICAgIH0KCiAgICBsZXQgYXV0b1BvbGljeT17c2NvcmU6bnVsbCxtYXRjaGVkX3RoZW1lczpbXX07CiAgICB0cnl7CiAgICAgIGF1dG9Qb2xpY3k9YXdhaXQgZ2V0UG9saWN5KGNvZGUpOwogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PWF1dG9Qb2xpY3kuc2NvcmU9PW51bGw/J+S4jeaYjic6c2NvcmVMYWJlbChhdXRvUG9saWN5LnNjb3JlKTsKICAgICAgJCgncG9saWN5RGV0YWlsJykudGV4dENvbnRlbnQ9CiAgICAgICAgYXV0b1BvbGljeS5zY29yZT09bnVsbAogICAgICAgICAgPyAn6Zai6YCj44GZ44KL5YWs5byP5pS/562W44OG44O844Oe44Gq44GXJwogICAgICAgICAgOiAn6Ieq5YuV5Zu9562WcHJveHkgLyDkv6HpoLzluqYgJysoYXV0b1BvbGljeS5jb25maWRlbmNlX3BjdD8/J+KAlCcpKyclIC8gJysoKGF1dG9Qb2xpY3kubWF0Y2hlZF90aGVtZXN8fFtdKS5sZW5ndGgpKyfjg4bjg7zjg54nOwogICAgICByZW5kZXJQb2xpY3lUaGVtZXMoYXV0b1BvbGljeSk7CiAgICB9Y2F0Y2gocGUpewogICAgICAkKCdwb2xpY3lTdGF0ZScpLnRleHRDb250ZW50PSfkuI3mmI4nOwogICAgICAkKCdwb2xpY3lEZXRhaWwnKS50ZXh0Q29udGVudD0n5YWs5byP5pS/562W44K944O844K55Y+W5b6X44Ko44Op44O8JzsKICAgICAgJCgncG9saWN5VGhlbWVzJykuaW5uZXJIVE1MPScnOwogICAgICBhdXRvUG9saWN5PXtzY29yZTpudWxsLG1hdGNoZWRfdGhlbWVzOltdfTsKICAgIH0KCiAgICBjb25zdCBtYW51YWxQb2xpY3k9dmFsKCdwb2xpY3knKTsKICAgIGNvbnN0IHBvbGljeVNjb3JlPW1hbnVhbFBvbGljeT09PW51bGw/YXV0b1BvbGljeS5zY29yZTptYW51YWxQb2xpY3k7CiAgICBjb25zdCBwb2xpY3lNb2RlPW1hbnVhbFBvbGljeT09PW51bGw/J2F1dG8nOidtYW51YWwnOwogICAgaWYobWFudWFsUG9saWN5IT09bnVsbCl7CiAgICAgICQoJ3BvbGljeVN0YXRlJykudGV4dENvbnRlbnQ9c2NvcmVMYWJlbChtYW51YWxQb2xpY3kpOwogICAgICAkKCdwb2xpY3lEZXRhaWwnKS50ZXh0Q29udGVudD0n5omL5YWl5Yqb44Gn6Ieq5YuV5YCk44KS5LiK5pu444GNJzsKICAgIH0KCiAgICBjb25zdCBkPXsKICAgICAgY29kZSwKICAgICAgcHJpY2U6cy5sYXN0X2Nsb3NlLAogICAgICByZXR1cm4yMDpzLnJldHVybl8yMGQsCiAgICAgIHJldHVybjEyNjpzLnJldHVybl8xMjZkLAogICAgICByZXR1cm4yNTI6cy5yZXR1cm5fMjUyZCwKICAgICAgZWFybmluZ3Nfc2NvcmU6ZnVuZGFtZW50YWxzLnNjb3JlLAogICAgICBwb2xpY3lfc2NvcmU6cG9saWN5U2NvcmUsCiAgICAgIHBvbGljeV9tb2RlOnBvbGljeU1vZGUsCiAgICAgIHN1cHBseV9zY29yZTpzdXBwbHkuc2NvcmUKICAgIH07CgogICAgY29uc3QgYXI9YXdhaXQgZmV0Y2goJy9hcGkvZnJlZS9hbmFseXplJyx7CiAgICAgIG1ldGhvZDonUE9TVCcsCiAgICAgIGhlYWRlcnM6eydDb250ZW50LVR5cGUnOidhcHBsaWNhdGlvbi9qc29uJ30sCiAgICAgIGJvZHk6SlNPTi5zdHJpbmdpZnkoZCksCiAgICAgIGNhY2hlOiduby1zdG9yZScKICAgIH0pOwogICAgY29uc3QgeD1hd2FpdCBhci5qc29uKCk7CiAgICBpZih4LnN0YXR1cyE9PSdvaycpdGhyb3cgbmV3IEVycm9yKHgucmVhc29ufHx4LmVycm9yfHwn5YiG5p6Q44OH44O844K/44GM5LiN6Laz44GX44Gm44GE44G+44GZJyk7CgogICAgJCgnc3RhdGUnKS50ZXh0Q29udGVudD1zdGF0ZUphKHguc2lnbmFsLnN0YXRlKTsKICAgICQoJ3BvcycpLnRleHRDb250ZW50PXguc2lnbmFsLnBvc2l0aXZlX2NvdW50OwogICAgJCgnbmVnJykudGV4dENvbnRlbnQ9eC5zaWduYWwubmVnYXRpdmVfY291bnQ7CgogICAgY29uc3Qgc2M9eC5zY29yZXx8e307CiAgICAkKCdzY29yZUhlcm8nKS5zdHlsZS5kaXNwbGF5PSdibG9jayc7CiAgICAkKCdzY29yZTEwMCcpLnRleHRDb250ZW50PXNjLnNjb3JlMTAwPT1udWxsPyfigJQnOnNjLnNjb3JlMTAwKycgLyAxMDAnOwoKICAgIGNvbnN0IHBpbGw9JCgnc2NvcmVTdGF0ZVBpbGwnKTsKICAgIHBpbGwuY2xhc3NOYW1lPSdzdGF0ZXBpbGwgJytzdGF0ZUNsYXNzKHguc2lnbmFsLnN0YXRlKTsKICAgIHBpbGwudGV4dENvbnRlbnQ9c3RhdGVKYSh4LnNpZ25hbC5zdGF0ZSk7CgogICAgJCgnY292ZXJhZ2UnKS50ZXh0Q29udGVudD1zYy5jb3ZlcmFnZV9wY3Q9PW51bGw/J+KAlCc6c2MuY292ZXJhZ2VfcGN0KyclJzsKICAgICQoJ3Njb3JlQnJlYWtkb3duJykudGV4dENvbnRlbnQ9CiAgICAgICfjg4bjgq/jg4vjgqvjg6sgJytzY29yZUxhYmVsKHNjLnRlY2huaWNhbCkrCiAgICAgICcgLyDmsbrnrpcgJytzY29yZUxhYmVsKHNjLmVhcm5pbmdzKSsKICAgICAgJyAvIOmcgOe1piAnK3Njb3JlTGFiZWwoc2Muc3VwcGx5KSsKICAgICAgJyAvIOWbveetlnByb3h5ICcrc2NvcmVMYWJlbChzYy5wb2xpY3kpOwoKICAgICQoJ3Njb3JlUmVhc29uJykuc3R5bGUuZGlzcGxheT0nYmxvY2snOwogICAgJCgnc2NvcmVSZWFzb25UZXh0JykudGV4dENvbnRlbnQ9ZHJpdmVyU2VudGVuY2Uoc2MpOwogICAgJCgnZHJpdmVyR3JpZCcpLmlubmVySFRNTD0KICAgICAgZHJpdmVyQm94SHRtbCgn5pyA5aSn44Gu44OX44Op44K56KaB5ZugJyxzYy5zdHJvbmdlc3RfcG9zaXRpdmUsJ3Bvc2l0aXZlJykrCiAgICAgIGRyaXZlckJveEh0bWwoJ+acgOWkp+OBruODnuOCpOODiuOCueimgeWboCcsc2Muc3Ryb25nZXN0X25lZ2F0aXZlLCduZWdhdGl2ZScpOwoKICAgIHJlbmRlckNvbnRyaWJ1dGlvbnMoc2MpOwoKICAgIGNvbnN0IGZyZXNoPWZyZXNobmVzc0ZvcihzLmxhc3RfZGF0ZSk7CiAgICBjb25zdCBoaXN0b3J5RnJlc2g9ZnJlc2huZXNzRm9yKHMuaGlzdG9yeV9sYXN0X2RhdGV8fHMubGFzdF9kYXRlKTsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PQogICAgICAhZnJlc2guZGVjaXNpb25fb2sKICAgICAgICA/ICfimqDvuI8g54++5Zyo5YCk44OH44O844K/44GM5Y+k44GE44Gf44KB44CB5LuK5pel44Gu5aOy6LK35Yik5pat44Go44GX44Gm44Gv5L2/55So44GX44G+44Gb44KT44CCJwogICAgICAgIDogIWhpc3RvcnlGcmVzaC5kZWNpc2lvbl9vawogICAgICAgICAgPyAn4pqg77iPIOePvuWcqOWApOOBr+aWsOOBl+OBhOaXpei2s+OCkuS9v+OBo+OBpuOBhOOBvuOBmeOBjOOAgeS+oeagvOWxpeattOOBjOWPpOOBhOOBn+OCgee3j+WQiOeCueODu+efreS4remVt+acn+ODiOODrOODs+ODieODu+acn+W+heWApOOBr+WPguiAg+WApOOBp+OBmeOAguWbuuWumuOBruaQjeWIh+OCii/liKnnorrjg6njgqTjg7Pnorroqo3jgpLlhKrlhYjjgZfjgabjgY/jgaDjgZXjgYTjgIInCiAgICAgICAgICA6ICfnj77lnKjlgKTjgajliIbmnpDlsaXmrbTjga7prq7luqbjgpLnorroqo3muIjjgb/jgILnirbmhYvooajnpLrjga/nt4/lkIjngrnjgavpgKPli5XjgZfjgb7jgZnjgIInOwogIH1jYXRjaChlKXsKICAgICQoJ3Jlc3VsdCcpLnRleHRDb250ZW50PSfimqDvuI8gJytlLm1lc3NhZ2U7CiAgICAkKCdzb3VyY2VCb3gnKS5pbm5lckhUTUw9JzxzcGFuIGNsYXNzPSJlcnIiPuWPluW+l+OCqOODqeODvDogJytlLm1lc3NhZ2UrJzwvc3Bhbj4nOwogIH1maW5hbGx5ewogICAgYnRuLmRpc2FibGVkPWZhbHNlOwogICAgYnRuLnRleHRDb250ZW50PSflrp/jg4fjg7zjgr/jgafliIbmnpAnOwogIH0KfQoKcmVuZGVySG9sZGluZ3MoKTtyZW5kZXJXYXRjaCgpOwpzZXRUaW1lb3V0KCgpPT5yZWZyZXNoQWxsSG9sZGluZ3MoZmFsc2UpLDQwMCk7CmlmKCdzZXJ2aWNlV29ya2VyJyBpbiBuYXZpZ2F0b3Ipe25hdmlnYXRvci5zZXJ2aWNlV29ya2VyLmdldFJlZ2lzdHJhdGlvbnMoKS50aGVuKHJzPT5Qcm9taXNlLmFsbChycy5tYXAocj0+ci51bnJlZ2lzdGVyKCkpKSkuY2F0Y2goKCk9Pnt9KX0KaWYoJ2NhY2hlcycgaW4gd2luZG93KXtjYWNoZXMua2V5cygpLnRoZW4oa2V5cz0+UHJvbWlzZS5hbGwoa2V5cy5tYXAoaz0+Y2FjaGVzLmRlbGV0ZShrKSkpKS5jYXRjaCgoKT0+e30pfQo8L3NjcmlwdD4KPC9tYWluPgo8L2JvZHk+CjwvaHRtbD4="
).decode("utf-8")

@APP.get("/")
def index():
    return Response(HTML, content_type="text/html; charset=utf-8")

if __name__=="__main__":
    APP.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
