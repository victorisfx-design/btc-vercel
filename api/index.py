"""
BTC Options Breakout Algo — Flask Backend (Vercel-compatible)
Delta Exchange | Flask + SSE | Firestore trade history
Strategy: 2 strikes | 10 min confirmation | TP=user-defined | SL=50% of entry
"""

import json, os, sqlite3, time, hmac, hashlib, random, threading, csv, io
from datetime import datetime, timezone, timedelta
from typing import Optional
from flask import Flask, request, jsonify, Response, send_from_directory
from flask_cors import CORS
import requests as req_lib

# ── CONFIGURATION ─────────────────────────────────────────────────────────
API_KEY    = os.environ.get("DELTA_API_KEY",    "iiu3aJNuen38GAPAbvMccMjrpYDJ6e")
API_SECRET = os.environ.get("DELTA_API_SECRET", "SQp0jPormIyaxUIc1qGf545zt5LNijVZm2R0cL7tekJ6DDZeVtP9PxmY9pA4")

# Firebase / Firestore
FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "btcalgo-42230")
FIREBASE_WEB_API_KEY = os.environ.get("FIREBASE_WEB_API_KEY", "AIzaSyCiAW7GB-PgYZ4stAQKS5pm0f7rQlVJu4o")  # for REST API
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

DELTA_REST_URL  = "https://api.delta.exchange"
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR      = os.path.join(BASE_DIR, "..", "static")
DATA_DIR        = "/tmp"
DB_PATH         = os.path.join(DATA_DIR, "trades.db")
CONFIRM_SECONDS = 10 * 60
SL_MULTIPLIER   = 0.5

app = Flask(__name__, static_folder=STATIC_DIR)
CORS(app)

# ── SSE EVENT QUEUE ───────────────────────────────────────────────────────
_sse_listeners: list = []
_sse_lock = threading.Lock()

def push_event(data: dict):
    payload = f"data: {json.dumps(data)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_listeners:
            try:
                q.append(payload)
            except Exception:
                dead.append(q)
        for d in dead:
            _sse_listeners.remove(d)

# ── FIRESTORE (with robust REST API fallback) ─────────────────────────────
_firestore_client = None
_firestore_enabled = False
_firestore_use_rest = False

def init_firestore():
    global _firestore_client, _firestore_enabled, _firestore_use_rest
    if not FIREBASE_PROJECT_ID:
        print("[FIRESTORE] Project ID not set — using SQLite only")
        return
    
    # 1. Try connecting using Google Cloud Client SDK (uses service account / credentials)
    try:
        from google.cloud import firestore
        import google.auth
        
        # Check if auth/credentials can be resolved
        try:
            _, project = google.auth.default()
        except Exception:
            pass # Keep trying to initialize explicitly below
            
        _firestore_client = firestore.Client(project=FIREBASE_PROJECT_ID)
        _firestore_enabled = True
        _firestore_use_rest = False
        print(f"[FIRESTORE] Connected using google-cloud-firestore Client SDK: {FIREBASE_PROJECT_ID}")
        return
    except Exception as e:
        print(f"[FIRESTORE] Client SDK connection skipped ({e}) — activating Firestore REST API fallback")
        
    # 2. Fallback: Initialize via Firestore REST API (no Service Account JSON needed!)
    _firestore_enabled = True
    _firestore_use_rest = True
    print(f"[FIRESTORE] Connected using REST API: {FIREBASE_PROJECT_ID}")

def _to_firestore_fields(d: dict) -> dict:
    fields = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, bool):
            fields[k] = {"booleanValue": v}
        elif isinstance(v, int):
            fields[k] = {"integerValue": str(v)}  # Firestore REST int64 must be a string representation
        elif isinstance(v, float):
            fields[k] = {"doubleValue": v}
        else:
            fields[k] = {"stringValue": str(v)}
    return fields

def _from_firestore_fields(fields: dict) -> dict:
    d = {}
    for k, val in fields.items():
        if "stringValue" in val:
            d[k] = val["stringValue"]
        elif "integerValue" in val:
            d[k] = int(val["integerValue"])
        elif "doubleValue" in val:
            d[k] = float(val["doubleValue"])
        elif "booleanValue" in val:
            d[k] = val["booleanValue"]
    return d

def firestore_save_trade(trade: dict):
    if not _firestore_enabled:
        return
    try:
        if not _firestore_use_rest and _firestore_client:
            col = _firestore_client.collection("btc_algo_trades")
            col.add({**trade, "saved_at": datetime.now(timezone.utc).isoformat()})
            print("[FIRESTORE] Saved trade successfully via SDK")
        else:
            # Firestore REST API Write
            url = f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/databases/(default)/documents/btc_algo_trades"
            fields = _to_firestore_fields({**trade, "saved_at": datetime.now(timezone.utc).isoformat()})
            r = req_lib.post(url, json={"fields": fields}, timeout=10)
            if r.status_code in (200, 201):
                print("[FIRESTORE REST] Saved trade successfully via REST")
            else:
                print(f"[FIRESTORE REST] Save trade error: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[FIRESTORE] Save trade error: {e}")

def firestore_get_trades(limit=100) -> list:
    if not _firestore_enabled:
        return []
    try:
        if not _firestore_use_rest and _firestore_client:
            col  = _firestore_client.collection("btc_algo_trades")
            docs = col.order_by("timestamp", direction="DESCENDING").limit(limit).stream()
            return [{"id": d.id, **d.to_dict()} for d in docs]
        else:
            # Firestore REST API Query (POST to runQuery)
            url = f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery"
            query = {
                "structuredQuery": {
                    "from": [{"collectionId": "btc_algo_trades"}],
                    "orderBy": [{"field": {"fieldPath": "timestamp"}, "direction": "DESCENDING"}],
                    "limit": limit
                }
            }
            r = req_lib.post(url, json=query, timeout=10)
            if r.status_code != 200:
                print(f"[FIRESTORE REST] Get trades error: {r.status_code} - {r.text}")
                return []
            
            results = r.json()
            trades = []
            for res in results:
                doc = res.get("document")
                if not doc:
                    continue
                name = doc.get("name", "")
                doc_id = name.split("/")[-1]
                fields = doc.get("fields", {})
                trade = {"id": doc_id, **_from_firestore_fields(fields)}
                trades.append(trade)
            return trades
    except Exception as e:
        print(f"[FIRESTORE] Get trades error: {e}")
        return []

def firestore_save_account_snapshot(snapshot: dict):
    if not _firestore_enabled:
        return
    try:
        if not _firestore_use_rest and _firestore_client:
            ref = _firestore_client.collection("btc_algo_account").document("current")
            ref.set({**snapshot, "updated_at": datetime.now(timezone.utc).isoformat()})
            print("[FIRESTORE] Saved account snapshot successfully via SDK")
        else:
            # Firestore REST API Write (PATCH to write single document)
            url = f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/databases/(default)/documents/btc_algo_account/current"
            fields = _to_firestore_fields({**snapshot, "updated_at": datetime.now(timezone.utc).isoformat()})
            r = req_lib.patch(url, json={"fields": fields}, timeout=10)
            if r.status_code in (200, 201):
                print("[FIRESTORE REST] Saved account snapshot successfully via REST")
            else:
                print(f"[FIRESTORE REST] Save snapshot error: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[FIRESTORE] Account snapshot error: {e}")

# Initialize Firestore
threading.Thread(target=init_firestore, daemon=True).start()

# ── LOCAL DATABASE (SQLite fallback + fast local cache) ────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp     TEXT,
            strike        TEXT,
            side          TEXT,
            entry_price   REAL,
            exit_price    REAL,
            tp_price      REAL,
            sl_price      REAL,
            points        REAL,
            result        TEXT,
            balance_after REAL,
            notes         TEXT,
            triggered_by  TEXT,
            duration_secs INTEGER
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS account (
            id      INTEGER PRIMARY KEY,
            balance REAL NOT NULL DEFAULT 100000
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            date          TEXT PRIMARY KEY,
            trades        INTEGER DEFAULT 0,
            wins          INTEGER DEFAULT 0,
            points        REAL DEFAULT 0,
            max_drawdown  REAL DEFAULT 0,
            start_balance REAL DEFAULT 0,
            end_balance   REAL DEFAULT 0
        )
    """)
    c.execute("INSERT OR IGNORE INTO account (id, balance) VALUES (1, 100000)")
    conn.commit()
    conn.close()

init_db()

def get_balance():
    conn = sqlite3.connect(DB_PATH)
    row  = conn.execute("SELECT balance FROM account WHERE id=1").fetchone()
    conn.close()
    return row[0] if row else 100000.0

def update_balance(b):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE account SET balance=? WHERE id=1", (b,))
    conn.commit()
    conn.close()

def save_trade(t):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO trades
            (timestamp, strike, side, entry_price, exit_price,
             tp_price, sl_price, points, result, balance_after, notes, triggered_by, duration_secs)
        VALUES
            (:timestamp, :strike, :side, :entry_price, :exit_price,
             :tp_price, :sl_price, :points, :result, :balance_after, :notes, :triggered_by, :duration_secs)
    """, t)
    conn.commit()
    conn.close()
    # Mirror to Firestore async
    threading.Thread(target=firestore_save_trade, args=(t,), daemon=True).start()

def get_trades(limit=200):
    # Prefer Firestore if enabled, else SQLite
    if _firestore_enabled:
        fs_trades = firestore_get_trades(limit)
        if fs_trades:
            return fs_trades
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    cols = ["id","timestamp","strike","side","entry_price","exit_price",
            "tp_price","sl_price","points","result","balance_after","notes",
            "triggered_by","duration_secs"]
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        result.append(d)
    return result

def get_stats():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT result, points FROM trades").fetchall()
    conn.close()
    total = len(rows)
    wins  = sum(1 for r in rows if r[0] == "PROFIT")
    pts   = sum(r[1] for r in rows if r[1] is not None)
    
    # Profit factor
    gross_profit = sum(r[1] for r in rows if r[1] is not None and r[1] > 0)
    gross_loss   = abs(sum(r[1] for r in rows if r[1] is not None and r[1] < 0))
    pf = round(gross_profit / gross_loss, 2) if gross_loss else 0.0
    
    # Today stats
    today = datetime.now(timezone.utc).date().isoformat()
    conn2 = sqlite3.connect(DB_PATH)
    today_rows = conn2.execute(
        "SELECT result, points FROM trades WHERE timestamp LIKE ?", (f"{today}%",)
    ).fetchall()
    conn2.close()
    today_pts = sum(r[1] for r in today_rows if r[1] is not None)
    today_trades = len(today_rows)
    
    return {
        "total":         total,
        "wins":          wins,
        "losses":        total - wins,
        "win_rate":      round(wins / total * 100, 1) if total else 0,
        "total_points":  round(pts, 2),
        "profit_factor": pf,
        "today_pts":     round(today_pts, 2),
        "today_trades":  today_trades,
        "firestore":     _firestore_enabled
    }

def get_daily_breakdown():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT substr(timestamp,1,10) as day,
               COUNT(*) as trades,
               SUM(CASE WHEN result='PROFIT' THEN 1 ELSE 0 END) as wins,
               SUM(points) as pts
        FROM trades
        GROUP BY day ORDER BY day DESC LIMIT 14
    """).fetchall()
    conn.close()
    return [{"date": r[0], "trades": r[1], "wins": r[2], "points": round(r[3] or 0, 2)} for r in rows]

# ── DELTA CONNECTION HEALTH ───────────────────────────────────────────────
class ConnectionHealth:
    def __init__(self):
        self.status      = "CHECKING"   # CHECKING | CONNECTED | ERROR | NO_CREDS
        self.error       = ""
        self.last_check  = None
        self.latency_ms  = None

conn_health = ConnectionHealth()

account_info = {
    "name": "Unknown", "email": "", "phone": "",
    "delta_balance": None, "detected": False,
    "user_id": None, "trading_enabled": False
}

def detect_account():
    global account_info, conn_health
    if not API_KEY or API_KEY == "":
        conn_health.status = "NO_CREDS"
        conn_health.error  = "API key not configured"
        push_event({"type": "conn", "health": _conn_health_dict()})
        return

    conn_health.status = "CHECKING"
    t0 = time.time()

    try:
        path    = "/v2/profile"
        headers = make_signature("GET", path)
        r = req_lib.get(DELTA_REST_URL + path, headers=headers, timeout=10)
        latency = int((time.time() - t0) * 1000)
        conn_health.latency_ms = latency
        conn_health.last_check = datetime.now(timezone.utc).isoformat()

        if r.status_code == 200:
            data = r.json().get("result", {})
            account_info["name"]            = data.get("name") or data.get("username") or "Trader"
            account_info["email"]           = data.get("email", "")
            account_info["user_id"]         = data.get("id")
            account_info["trading_enabled"] = data.get("is_trading_enabled", True)
            account_info["detected"]        = True
            conn_health.status              = "CONNECTED"
            conn_health.error               = ""
        elif r.status_code == 401:
            conn_health.status = "ERROR"
            conn_health.error  = "Invalid API key or signature"
        elif r.status_code == 403:
            conn_health.status = "ERROR"
            conn_health.error  = "API key does not have required permissions"
        else:
            conn_health.status = "ERROR"
            conn_health.error  = f"HTTP {r.status_code}: {r.text[:120]}"
    except req_lib.exceptions.Timeout:
        conn_health.status = "ERROR"
        conn_health.error  = "Connection timed out"
    except Exception as e:
        conn_health.status = "ERROR"
        conn_health.error  = str(e)[:120]

    # Fetch wallet balance
    try:
        path    = "/v2/wallet/balances"
        headers = make_signature("GET", path)
        r = req_lib.get(DELTA_REST_URL + path, headers=headers, timeout=10)
        if r.status_code == 200:
            result = r.json().get("result", [])
            for asset in result:
                sym = asset.get("asset_symbol", "")
                if sym in ("USDT", "USD", "INR"):
                    bal = float(asset.get("available_balance", 0) or 0)
                    if bal > 0:
                        account_info["delta_balance"] = bal
                        update_balance(bal)
                        break
    except Exception as e:
        print(f"[WALLET] {e}")

    push_event({"type": "conn",    "health": _conn_health_dict()})
    push_event({"type": "account", "account": account_info})
    # Snapshot to Firestore
    threading.Thread(target=firestore_save_account_snapshot, args=({
        **account_info, "conn_status": conn_health.status
    },), daemon=True).start()

def _conn_health_dict():
    return {
        "status":     conn_health.status,
        "error":      conn_health.error,
        "last_check": conn_health.last_check,
        "latency_ms": conn_health.latency_ms,
    }

def periodic_health_check():
    """Re-check connection every 5 minutes"""
    while True:
        time.sleep(300)
        detect_account()

threading.Thread(target=detect_account,          daemon=True).start()
threading.Thread(target=periodic_health_check,   daemon=True).start()

# ── ALGO STATE ────────────────────────────────────────────────────────────
class AlgoState:
    def reset(self):
        self.running         = False
        self.status          = "IDLE"
        self.config          = {}
        self.prices          = {"strike1": None, "strike2": None}
        self.contracts       = {"strike1": None, "strike2": None}
        self.breakout_side   = None
        self.breakout_time   = None
        self.position        = None
        self.logs            = []
        self._thread         = None
        self.trade_open_time = None
        # Risk management state
        self.daily_trades    = 0
        self.daily_pnl       = 0.0
        self.session_start   = datetime.now(timezone.utc).isoformat()

    def __init__(self):
        self.reset()
        self._lock = threading.Lock()

    def log(self, msg):
        entry = {
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "msg":  msg
        }
        self.logs.insert(0, entry)
        self.logs = self.logs[:200]
        push_event({"type": "log", "msg": msg, "time": entry["time"]})

state = AlgoState()

def push_state():
    elapsed = int(time.time() - state.breakout_time) if state.breakout_time else 0
    push_event({
        "type":              "state",
        "status":            state.status,
        "running":           state.running,
        "prices":            state.prices,
        "config":            state.config,
        "position":          state.position,
        "balance":           get_balance(),
        "breakout_side":     state.breakout_side,
        "confirm_elapsed":   elapsed,
        "confirm_remaining": max(0, CONFIRM_SECONDS - elapsed),
        "confirm_total":     CONFIRM_SECONDS,
        "logs":              state.logs[:20],
        "stats":             get_stats(),
        "account":           account_info,
        "conn_health":       _conn_health_dict(),
        "daily_trades":      state.daily_trades,
        "daily_pnl":         state.daily_pnl,
        "ts":                datetime.now(timezone.utc).isoformat()
    })

# ── DELTA HELPERS ─────────────────────────────────────────────────────────
def make_signature(method: str, path: str, payload: str = "") -> dict:
    ts  = str(int(time.time()))
    msg = f"{method}{ts}{path}{payload}"
    sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {
        "api-key":      API_KEY,
        "timestamp":    ts,
        "signature":    sig,
        "Content-Type": "application/json"
    }

def fetch_contract(strike, opt_type):
    url = (f"{DELTA_REST_URL}/v2/products"
           f"?contract_type=put_options,call_options&state=live&page_size=200")
    try:
        r = req_lib.get(url, timeout=10)
        if r.status_code == 200:
            products = r.json().get("result", [])
            ctype = "call_options" if opt_type.upper() == "CE" else "put_options"
            sv    = int(float(strike))
            for p in products:
                if (p.get("contract_type") == ctype
                        and "BTC" in p.get("underlying_asset", {}).get("symbol", "")
                        and int(float(p.get("strike_price", 0))) == sv):
                    return p
    except Exception as e:
        print(f"[API] fetch_contract: {e}")
    return None

def place_order(product_id, side):
    path    = "/v2/orders"
    payload = json.dumps({
        "product_id": product_id,
        "size": 1, "side": side,
        "order_type": "market_order"
    })
    headers = make_signature("POST", path, payload)
    try:
        r = req_lib.post(DELTA_REST_URL + path, headers=headers, data=payload, timeout=10)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# ── SIMULATION FEED ───────────────────────────────────────────────────────
def simulate_feed():
    cfg  = state.config
    b1   = float(cfg.get("level1", 575))
    b2   = float(cfg.get("level2", 547))
    tick = 0
    while state.running:
        tick += 1
        if tick % 45 == 0:
            b1 += random.choice([-1, 1]) * random.uniform(15, 50)
            b2 += random.choice([-1, 1]) * random.uniform(15, 50)
        else:
            b1 += random.uniform(-3, 3)
            b2 += random.uniform(-3, 3)
        b1 = max(5, b1)
        b2 = max(5, b2)
        state.prices["strike1"] = round(b1, 1)
        state.prices["strike2"] = round(b2, 1)
        check_breakout()
        push_state()
        time.sleep(2)

# ── RISK MANAGEMENT ───────────────────────────────────────────────────────
def check_risk_limits() -> tuple[bool, str]:
    """Returns (allowed, reason). False = trade blocked."""
    cfg = state.config
    max_daily_trades = int(cfg.get("max_daily_trades", 0) or 0)
    max_daily_loss   = float(cfg.get("max_daily_loss", 0) or 0)

    if max_daily_trades > 0 and state.daily_trades >= max_daily_trades:
        return False, f"Daily trade limit reached ({max_daily_trades} trades)"

    if max_daily_loss > 0 and state.daily_pnl <= -abs(max_daily_loss):
        return False, f"Daily loss limit hit (₹{abs(max_daily_loss):,.0f})"

    return True, ""

# ── BREAKOUT ENGINE ───────────────────────────────────────────────────────
def check_breakout():
    if state.status == "TRADING":
        monitor_position()
        return
    if state.status not in ("MONITORING", "CONFIRMING"):
        return

    cfg = state.config
    p1  = state.prices.get("strike1")
    p2  = state.prices.get("strike2")
    lv1 = float(cfg.get("level1", 0))
    lv2 = float(cfg.get("level2", 0))
    now = time.time()

    if state.status == "MONITORING":
        candidates = [
            ("strike1", p1, lv1, cfg.get("strike1",""), cfg.get("type1","")),
            ("strike2", p2, lv2, cfg.get("strike2",""), cfg.get("type2",""))
        ]
        for key, price, level, s_strike, s_type in candidates:
            if price and level and abs(price - level) / level > 0.03:
                state.breakout_side = key
                state.breakout_time = now
                state.status        = "CONFIRMING"
                state.log(f"⚡ BREAKOUT DETECTED — {s_strike} {s_type} @ {price} (Level: {level}) | Confirming 10 min...")
                break

    elif state.status == "CONFIRMING":
        bs    = state.breakout_side
        price = p1 if bs == "strike1" else p2
        level = lv1 if bs == "strike1" else lv2

        if price and level and abs(price - level) / level < 0.01:
            state.status        = "MONITORING"
            state.breakout_side = None
            state.breakout_time = None
            state.log("↩️  Price returned inside level — breakout cancelled. Back to monitoring.")
            return

        if now - state.breakout_time >= CONFIRM_SECONDS:
            allowed, reason = check_risk_limits()
            if not allowed:
                state.status        = "MONITORING"
                state.breakout_side = None
                state.breakout_time = None
                state.log(f"🚫 TRADE BLOCKED — {reason}")
                return
            fire_trade()

def fire_trade():
    cfg = state.config
    bs  = state.breakout_side

    if bs == "strike1":
        tkey  = "strike2"
        broke = f"{cfg.get('strike1','')} {cfg.get('type1','')}"
        label = f"{cfg.get('strike2','')} {cfg.get('type2','')}"
        ep    = state.prices.get("strike2") or float(cfg.get("level2", 100))
    else:
        tkey  = "strike1"
        broke = f"{cfg.get('strike2','')} {cfg.get('type2','')}"
        label = f"{cfg.get('strike1','')} {cfg.get('type1','')}"
        ep    = state.prices.get("strike1") or float(cfg.get("level1", 100))

    tp = float(cfg.get("tp_price", round(ep * 2.0, 2)))
    sl = round(ep * SL_MULTIPLIER, 2)

    order = {"status": "simulation"}
    c     = state.contracts.get(tkey)
    if conn_health.status == "CONNECTED" and c:
        order = place_order(c["id"], "buy")

    state.trade_open_time = time.time()
    state.position = {
        "label":         label,
        "broke_label":   broke,
        "trade_key":     tkey,
        "entry_price":   ep,
        "current_price": ep,
        "tp":            tp,
        "sl":            sl,
        "open_time":     datetime.now(timezone.utc).isoformat(),
        "product_id":    c["id"] if c else None,
        "pnl":           0.0,
        "pnl_pct":       0.0,
        "order":         order,
        "high":          ep,
        "low":           ep,
    }
    state.status = "TRADING"
    mode = "LIVE" if conn_health.status == "CONNECTED" and c else "SIM"
    state.log(f"🟢 TRADE PLACED [{mode}] → BUY {label} @ {ep:.1f} | TP: {tp:.1f} | SL: {sl:.1f}")

def monitor_position():
    pos = state.position
    if not pos:
        return
    current              = state.prices.get(pos["trade_key"]) or pos["entry_price"]
    pos["current_price"] = current
    pos["pnl"]           = round(current - pos["entry_price"], 2)
    pos["pnl_pct"]       = round((current / pos["entry_price"] - 1) * 100, 2)
    pos["high"]          = max(pos.get("high", current), current)
    pos["low"]           = min(pos.get("low",  current), current)
    if current >= pos["tp"]:
        exit_trade("TP HIT", current)
    elif current <= pos["sl"]:
        exit_trade("SL HIT", current)

def exit_trade(reason, exit_price):
    pos     = state.position
    pts     = round(exit_price - pos["entry_price"], 2)
    bal     = get_balance()
    new_bal = round(bal + pts, 2)
    update_balance(new_bal)
    result  = "PROFIT" if pts >= 0 else "LOSS"
    dur     = int(time.time() - state.trade_open_time) if state.trade_open_time else 0

    trade_record = {
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "strike":        pos["label"],
        "side":          "BUY",
        "entry_price":   pos["entry_price"],
        "exit_price":    exit_price,
        "tp_price":      pos["tp"],
        "sl_price":      pos["sl"],
        "points":        pts,
        "result":        result,
        "balance_after": new_bal,
        "notes":         f"{reason} | Triggered by: {pos.get('broke_label','')}",
        "triggered_by":  pos.get("broke_label", ""),
        "duration_secs": dur,
    }
    save_trade(trade_record)

    state.daily_trades += 1
    state.daily_pnl    += pts

    if conn_health.status == "CONNECTED" and pos.get("product_id"):
        place_order(pos["product_id"], "sell")

    emoji = "🟢" if pts >= 0 else "🔴"
    state.log(
        f"{emoji} EXIT [{reason}] {pos['label']} @ {exit_price:.1f} | "
        f"Pts: {pts:+.1f} | Duration: {dur//60}m {dur%60}s | Bal: ₹{new_bal:,.0f}"
    )

    state.position      = None
    state.trade_open_time = None
    state.breakout_side = None
    state.breakout_time = None
    state.status        = "MONITORING"

    push_event({
        "type":    "history",
        "trades":  get_trades(200),
        "balance": new_bal,
        "stats":   get_stats(),
        "daily":   get_daily_breakdown(),
    })

# ── ROUTES ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")

@app.route("/api/events")
def sse_stream():
    def stream():
        q = []
        with _sse_lock:
            _sse_listeners.append(q)
        elapsed = int(time.time() - state.breakout_time) if state.breakout_time else 0
        init_payload = {
            "type":              "state",
            "status":            state.status,
            "running":           state.running,
            "prices":            state.prices,
            "config":            state.config,
            "position":          state.position,
            "balance":           get_balance(),
            "breakout_side":     state.breakout_side,
            "confirm_elapsed":   elapsed,
            "confirm_remaining": max(0, CONFIRM_SECONDS - elapsed),
            "confirm_total":     CONFIRM_SECONDS,
            "logs":              state.logs[:20],
            "stats":             get_stats(),
            "account":           account_info,
            "conn_health":       _conn_health_dict(),
            "daily_trades":      state.daily_trades,
            "daily_pnl":         state.daily_pnl,
            "ts":                datetime.now(timezone.utc).isoformat()
        }
        yield f"data: {json.dumps(init_payload)}\n\n"
        yield f"data: {json.dumps({'type':'history','trades':get_trades(200),'balance':get_balance(),'stats':get_stats(),'daily':get_daily_breakdown()})}\n\n"
        yield f"data: {json.dumps({'type':'conn','health':_conn_health_dict()})}\n\n"

        try:
            while True:
                if q:
                    yield q.pop(0)
                else:
                    yield ": heartbeat\n\n"
                    time.sleep(1)
        except GeneratorExit:
            with _sse_lock:
                if q in _sse_listeners:
                    _sse_listeners.remove(q)

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.route("/api/start", methods=["POST"])
def start():
    if state.running:
        return jsonify({"ok": False, "msg": "Already running"})
    data = request.get_json()
    state.reset()
    state.config = data
    if data.get("balance"):
        update_balance(float(data["balance"]))

    def _setup():
        state.contracts["strike1"] = fetch_contract(data.get("strike1"), data.get("type1"))
        state.contracts["strike2"] = fetch_contract(data.get("strike2"), data.get("type2"))
        state.running = True
        state.status  = "MONITORING"
        mode = "LIVE" if conn_health.status == "CONNECTED" else "SIMULATION"
        state.log(
            f"🚀 Started [{mode}] | {data.get('strike1')} {data.get('type1')} @ {data.get('level1')} "
            f"| {data.get('strike2')} {data.get('type2')} @ {data.get('level2')} "
            f"| TP: {data.get('tp_price')} | SL: 50% auto"
        )
        if not state.contracts["strike1"] and not state.contracts["strike2"]:
            state.log("⚠️  No live contracts found — running in simulation mode")
        t = threading.Thread(target=simulate_feed, daemon=True)
        state._thread = t
        t.start()

    threading.Thread(target=_setup, daemon=True).start()
    return jsonify({"ok": True, "msg": "Algo starting..."})

@app.route("/api/stop", methods=["POST"])
def stop():
    state.running = False
    state.status  = "IDLE"
    state.log("⛔ Algo stopped by user")
    push_state()
    return jsonify({"ok": True})

@app.route("/api/history")
def history():
    return jsonify({
        "trades":  get_trades(200),
        "balance": get_balance(),
        "stats":   get_stats(),
        "daily":   get_daily_breakdown(),
        "account": account_info,
        "conn_health": _conn_health_dict(),
        "firestore_enabled": _firestore_enabled,
    })

@app.route("/api/balance", methods=["POST"])
def set_balance():
    data = request.get_json()
    update_balance(float(data.get("balance", 100000)))
    return jsonify({"ok": True, "balance": get_balance()})

@app.route("/api/connection")
def connection_status():
    """Force a fresh connection check"""
    threading.Thread(target=detect_account, daemon=True).start()
    return jsonify({
        "conn_health": _conn_health_dict(),
        "account":     account_info,
        "firestore":   _firestore_enabled,
    })

@app.route("/api/export/csv")
def export_csv():
    """Export all trades as CSV"""
    trades = get_trades(10000)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "timestamp","strike","side","entry_price","exit_price",
        "tp_price","sl_price","points","result","balance_after",
        "triggered_by","duration_secs","notes"
    ])
    writer.writeheader()
    for t in trades:
        writer.writerow({k: t.get(k, "") for k in writer.fieldnames})
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=btc_algo_trades.csv"}
    )

@app.route("/api/stats/daily")
def daily_stats():
    return jsonify({"daily": get_daily_breakdown()})

if __name__ == "__main__":
    print("=" * 60)
    print("  BTC Options Algo — Flask + Firestore (Vercel-ready)")
    print("  Dashboard: http://localhost:5000")
    print(f"  Delta API: {'SET' if API_KEY else 'NOT SET'}")
    print(f"  Firestore: {'SET' if FIREBASE_PROJECT_ID else 'NOT CONFIGURED'}")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
