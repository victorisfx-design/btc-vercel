"""
BTC Options Breakout Algo — Flask Backend (Vercel-compatible)
Delta Exchange | Flask + SSE (replaces WebSocket for Vercel hosting)
Strategy: 2 strikes | 10 min confirmation | TP=user-defined | SL=50% of entry
"""

import json, os, sqlite3, time, hmac, hashlib, random, threading
from datetime import datetime, timezone
from typing import Optional
from flask import Flask, request, jsonify, Response, send_from_directory
from flask_cors import CORS
import requests as req_lib

# ── CONFIGURATION ─────────────────────────────────────────────────────────
API_KEY    = os.environ.get("DELTA_API_KEY",    "iiu3aJNuen38GAPAbvMccMjrpYDJ6e")
API_SECRET = os.environ.get("DELTA_API_SECRET", "SQp0jPormIyaxUIc1qGf545zt5LNijVZm2R0cL7tekJ6DDZeVtP9PxmY9pA4")

DELTA_REST_URL  = "https://api.delta.exchange"
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR      = os.path.join(BASE_DIR, "..", "static")
DATA_DIR        = "/tmp"          # Vercel writable dir
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

# ── DATABASE ──────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp    TEXT,
            strike       TEXT,
            side         TEXT,
            entry_price  REAL,
            exit_price   REAL,
            tp_price     REAL,
            sl_price     REAL,
            points       REAL,
            result       TEXT,
            balance_after REAL,
            notes        TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS account (
            id      INTEGER PRIMARY KEY,
            balance REAL NOT NULL DEFAULT 100000
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
             tp_price, sl_price, points, result, balance_after, notes)
        VALUES
            (:timestamp, :strike, :side, :entry_price, :exit_price,
             :tp_price, :sl_price, :points, :result, :balance_after, :notes)
    """, t)
    conn.commit()
    conn.close()

def get_trades(limit=100):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    cols = ["id","timestamp","strike","side","entry_price","exit_price",
            "tp_price","sl_price","points","result","balance_after","notes"]
    return [dict(zip(cols, r)) for r in rows]

def get_stats():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT result, points FROM trades").fetchall()
    conn.close()
    total = len(rows)
    wins  = sum(1 for r in rows if r[0] == "PROFIT")
    pts   = sum(r[1] for r in rows if r[1] is not None)
    return {
        "total":        total,
        "wins":         wins,
        "losses":       total - wins,
        "win_rate":     round(wins / total * 100, 1) if total else 0,
        "total_points": round(pts, 2)
    }

# ── DELTA ACCOUNT DETECTION ───────────────────────────────────────────────
account_info = {"name":"Unknown","email":"","delta_balance":None,"detected":False}

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

def detect_account():
    global account_info
    try:
        path    = "/v2/profile"
        headers = make_signature("GET", path)
        r = req_lib.get(DELTA_REST_URL + path, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json().get("result", {})
            account_info["name"]     = data.get("name") or data.get("username") or "Trader"
            account_info["email"]    = data.get("email", "")
            account_info["detected"] = True
    except Exception as e:
        print(f"[ACCOUNT] {e}")

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

# Run account detection in background on startup
threading.Thread(target=detect_account, daemon=True).start()

# ── ALGO STATE ────────────────────────────────────────────────────────────
class AlgoState:
    def reset(self):
        self.running       = False
        self.status        = "IDLE"
        self.config        = {}
        self.prices        = {"strike1": None, "strike2": None}
        self.contracts     = {"strike1": None, "strike2": None}
        self.breakout_side = None
        self.breakout_time = None
        self.position      = None
        self.logs          = []
        self._thread       = None

    def __init__(self):
        self.reset()
        self._lock = threading.Lock()

    def log(self, msg):
        entry = {
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "msg":  msg
        }
        self.logs.insert(0, entry)
        self.logs = self.logs[:100]
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
        "ts":                datetime.now(timezone.utc).isoformat()
    })

# ── DELTA HELPERS ─────────────────────────────────────────────────────────
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
    payload = json.dumps({"product_id": product_id, "size": 1, "side": side, "order_type": "market_order"})
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
                state.log(f"⚡ BREAKOUT — {s_strike} {s_type} @ {price} (Level: {level}). Confirming 10 min...")
                break

    elif state.status == "CONFIRMING":
        bs    = state.breakout_side
        price = p1 if bs == "strike1" else p2
        level = lv1 if bs == "strike1" else lv2

        if price and level and abs(price - level) / level < 0.01:
            state.status        = "MONITORING"
            state.breakout_side = None
            state.breakout_time = None
            state.log("↩️  Price returned inside level — cancelled. Back to monitoring.")
            return

        if now - state.breakout_time >= CONFIRM_SECONDS:
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
    if API_KEY and c:
        order = place_order(c["id"], "buy")

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
        "order":         order
    }
    state.status = "TRADING"
    state.log(f"🟢 TRADE PLACED → BUY {label} @ {ep} | TP: {tp} | SL: {sl}")

def monitor_position():
    pos = state.position
    if not pos:
        return
    current              = state.prices.get(pos["trade_key"]) or pos["entry_price"]
    pos["current_price"] = current
    pos["pnl"]           = round(current - pos["entry_price"], 2)
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
    result = "PROFIT" if pts >= 0 else "LOSS"

    save_trade({
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
        "notes":         f"{reason} | Triggered by: {pos.get('broke_label','')}"
    })

    if API_KEY and pos.get("product_id"):
        place_order(pos["product_id"], "sell")

    emoji = "🟢" if pts >= 0 else "🔴"
    state.log(f"{emoji} EXIT [{reason}] {pos['label']} @ {exit_price} | Pts: {pts:+.1f} | Bal: ₹{new_bal:,.0f}")

    state.position      = None
    state.breakout_side = None
    state.breakout_time = None
    state.status        = "MONITORING"

    push_event({
        "type":    "history",
        "trades":  get_trades(100),
        "balance": new_bal,
        "stats":   get_stats()
    })

# ── ROUTES ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")

@app.route("/api/events")
def sse_stream():
    """Server-Sent Events endpoint — replaces WebSocket"""
    def stream():
        q = []
        with _sse_lock:
            _sse_listeners.append(q)
        # Send initial state immediately
        elapsed = int(time.time() - state.breakout_time) if state.breakout_time else 0
        init = {
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
            "ts":                datetime.now(timezone.utc).isoformat()
        }
        yield f"data: {json.dumps(init)}\n\n"
        yield f"data: {json.dumps({'type':'history','trades':get_trades(100),'balance':get_balance(),'stats':get_stats()})}\n\n"

        try:
            while True:
                if q:
                    yield q.pop(0)
                else:
                    # heartbeat every 15s to keep connection alive
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

    # Fetch contracts in background
    def _setup():
        state.contracts["strike1"] = fetch_contract(data.get("strike1"), data.get("type1"))
        state.contracts["strike2"] = fetch_contract(data.get("strike2"), data.get("type2"))
        state.running = True
        state.status  = "MONITORING"
        state.log(f"🚀 Started | {data.get('strike1')} {data.get('type1')} @ {data.get('level1')} | "
                  f"{data.get('strike2')} {data.get('type2')} @ {data.get('level2')} | TP: {data.get('tp_price')}")
        # Start price feed
        if not state.contracts["strike1"] and not state.contracts["strike2"]:
            state.log("⚠️  No live contracts found — running simulation mode")
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
        "trades":  get_trades(100),
        "balance": get_balance(),
        "stats":   get_stats(),
        "account": account_info
    })

@app.route("/api/balance", methods=["POST"])
def set_balance():
    data = request.get_json()
    update_balance(float(data.get("balance", 100000)))
    return jsonify({"ok": True, "balance": get_balance()})

if __name__ == "__main__":
    print("=" * 55)
    print("  BTC Options Algo — Flask (Vercel-ready)")
    print("  Dashboard: http://localhost:5000")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
