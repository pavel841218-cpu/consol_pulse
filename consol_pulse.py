import asyncio
import aiohttp
from aiohttp import web, WSMsgType
import os
import time
import json
import gc
import logging
from collections import defaultdict, deque
from html import escape

# ============================================================
# ПАМП-ХАНТЕР v17.6 — Hardened WS (Fixes from v16.5)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "15"))
OI_SAMPLE_INTERVAL_SEC = int(os.environ.get("OI_SAMPLE_INTERVAL_SEC", "900"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))
PERSIST_INTERVAL_SEC = int(os.environ.get("PERSIST_INTERVAL_SEC", "300"))
PERSIST_PATH = os.environ.get("PERSIST_PATH", "/tmp/pump_hunter_state.json")

# --- Юниверс ---
MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "200"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "200"))
MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "80000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.0001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "10.0"))
MIN_LISTING_AGE_DAYS = float(os.environ.get("MIN_LISTING_AGE_DAYS", "14"))

# --- Fast Path & Timeouts ---
FAST_SIGNAL_RVOL = float(os.environ.get("FAST_SIGNAL_RVOL", "4.0"))
FAST_SIGNAL_PCT = float(os.environ.get("FAST_SIGNAL_PCT", "4.0"))
FAST_CONFIRM_TIMEOUT_SEC = float(os.environ.get("FAST_CONFIRM_TIMEOUT_SEC", "2.5"))
CONFIRM_TIMEOUT_SEC = float(os.environ.get("CONFIRM_TIMEOUT_SEC", "6.0"))

# --- Защита ---
ANTI_HIGH_MAX_PCT = float(os.environ.get("ANTI_HIGH_MAX_PCT", "5.0"))
MAX_IMPULSE_PCT = float(os.environ.get("MAX_IMPULSE_PCT", "15.0"))
MAX_GRIND_PCT = float(os.environ.get("MAX_GRIND_PCT", "250.0"))
MAX_24H_CHANGE_PCT = float(os.environ.get("MAX_24H_CHANGE_PCT", "25.0"))
TRACK_TIMEOUT_SEC = int(os.environ.get("TRACK_TIMEOUT_SEC", str(12 * 3600)))
WICK_DUMP_THRESHOLD = float(os.environ.get("WICK_DUMP_THRESHOLD", "0.45"))
SCAM_RVOL_RATIO = float(os.environ.get("SCAM_RVOL_RATIO", "3.0"))
CONTEXT_LOOKBACK = int(os.environ.get("CONTEXT_LOOKBACK", "10"))
CLIMAX_MIN_ELAPSED_FRACTION = float(os.environ.get("CLIMAX_MIN_ELAPSED_FRACTION", "0.25"))

TIMEFRAMES = {
    "impulse": {
        "granularity_min": 15,
        "lookback_candles": int(os.environ.get("IMPULSE_LOOKBACK_CANDLES", "2")),
        "base_candles": int(os.environ.get("IMPULSE_BASE_CANDLES", "96")),
        "min_pct": float(os.environ.get("IMPULSE_MIN_PCT", "4.0")),
        "max_pct": MAX_IMPULSE_PCT,
        "min_rvol": float(os.environ.get("IMPULSE_MIN_RVOL", "2.0")),
        "oi_window_hours": float(os.environ.get("IMPULSE_OI_WINDOW_HOURS", "1.5")),
        "fetch_limit": 120,
        "emoji": "⚡",
        "label": "ИМПУЛЬС 15м",
    },
    "grind": {
        "granularity_min": 60,
        "lookback_candles": int(os.environ.get("GRIND_LOOKBACK_CANDLES", "12")),
        "base_candles": int(os.environ.get("GRIND_BASE_CANDLES", "72")),
        "min_pct": float(os.environ.get("GRIND_MIN_PCT", "14.0")),
        "max_pct": MAX_GRIND_PCT,
        "min_rvol": float(os.environ.get("GRIND_MIN_RVOL", "1.8")),
        "oi_window_hours": float(os.environ.get("GRIND_OI_WINDOW_HOURS", "12.0")),
        "fetch_limit": 100,
        "emoji": "📈",
        "label": "РАЗГОН 1ч",
    },
}
BREAKOUT_TOLERANCE = float(os.environ.get("BREAKOUT_TOLERANCE", "0.985"))
CONFIRM_PCT_RATIO = float(os.environ.get("CONFIRM_PCT_RATIO", "0.5"))
MIN_CONFIRMATIONS = int(os.environ.get("MIN_CONFIRMATIONS", "1"))

SPARK_ENABLED = os.environ.get("SPARK_ENABLED", "true").lower() == "true"
SPARK_MIN_PCT = float(os.environ.get("SPARK_MIN_PCT", "3.0"))
SPARK_MIN_RVOL = float(os.environ.get("SPARK_MIN_RVOL", "2.0"))
SPARK_BASE_CANDLES = int(os.environ.get("SPARK_BASE_CANDLES", "96"))
SPARK_MIN_ELAPSED_MIN = float(os.environ.get("SPARK_MIN_ELAPSED_MIN", "2.0"))
SPARK_MAX_ELAPSED_MIN = float(os.environ.get("SPARK_MAX_ELAPSED_MIN", "6.0"))
SPARK_CONFIRM_RATIO = float(os.environ.get("SPARK_CONFIRM_RATIO", "0.4"))
SPARK_COOLDOWN_SEC = int(os.environ.get("SPARK_COOLDOWN_SEC", str(2 * 3600)))

SCORE_WATCH_THRESHOLD = float(os.environ.get("SCORE_WATCH_THRESHOLD", "35"))
SCORE_SIGNAL_THRESHOLD = float(os.environ.get("SCORE_SIGNAL_THRESHOLD", "55"))
OI_MIN_GROWTH_PCT = float(os.environ.get("OI_MIN_GROWTH_PCT", "8.0"))
FUNDING_EXTREME_NEG = float(os.environ.get("FUNDING_EXTREME_NEG", "-0.0005"))
FUNDING_MILD_NEG = float(os.environ.get("FUNDING_MILD_NEG", "-0.0002"))

CLIMAX_RVOL_THRESHOLD = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "15.0"))
CLIMAX_OI_DROP_PCT = float(os.environ.get("CLIMAX_OI_DROP_PCT", "-5.0"))

ACCUM_ENABLED = os.environ.get("ACCUM_ENABLED", "true").lower() == "true"
ACCUM_WINDOW_HOURS = int(os.environ.get("ACCUM_WINDOW_HOURS", "3"))
ACCUM_MAX_PRICE_MOVE_PCT = float(os.environ.get("ACCUM_MAX_PRICE_MOVE_PCT", "6.0"))
ACCUM_MIN_OI_GROWTH_PCT = float(os.environ.get("ACCUM_MIN_OI_GROWTH_PCT", "15.0"))
ACCUM_MIN_SOURCES = int(os.environ.get("ACCUM_MIN_SOURCES", "2"))
ACCUM_MIN_EFFICIENCY_INVERSE = float(os.environ.get("ACCUM_MAX_EFFICIENCY", "0.35"))
ACCUM_COOLDOWN_SEC = int(os.environ.get("ACCUM_COOLDOWN_SEC", str(2 * 3600)))

COOLDOWN_SEC = int(os.environ.get("COOLDOWN_SEC", str(6 * 3600)))
REJECT_COOLDOWN_SEC = int(os.environ.get("REJECT_COOLDOWN_SEC", str(20 * 60)))
CLIMAX_COOLDOWN_SEC = int(os.environ.get("CLIMAX_COOLDOWN_SEC", "1800"))

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"
BINGX_BASE = "https://open-api.bingx.com"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v17.6")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_SIGNAL = {}
LAST_REJECT = {}
ACCUM_LAST_SIGNAL = {}
LAST_SPARK_SIGNAL = {}
LAST_CLIMAX_SIGNAL = {}
PUMP_TRACKS = {}
PUMP_CONTEXT = {} 

KUCOIN_STATUS = {"is_down": False, "last_check": 0}

WS_CANDLES = defaultdict(lambda: {"15": deque(maxlen=150), "60": deque(maxlen=150)})
WS_OI = {}
WS_SUBSCRIBED = set()

OI_HIST_MAXLEN = int(14 * 3600 / OI_SAMPLE_INTERVAL_SEC) + 10
OI_HISTORY = defaultdict(lambda: defaultdict(lambda: deque(maxlen=OI_HIST_MAXLEN)))

STATS = {
    "scans": 0, "oi_samples": 0, "triggers": 0, "signals": 0, "fast_signals": 0, "fast_unconfirmed": 0,
    "watch_signals": 0, "accum_alerts": 0, "spark_alerts": 0, "climax_alerts": 0, "dump_alerts": 0, "scam_alerts": 0,
    "rejected_no_breakout": 0, "rejected_no_confirm": 0, "confirm_unavailable": 0, "rejected_low_score": 0, "rejected_anti_high": 0,
    "state_loaded": False, "state_saved_count": 0,
    "kucoin_candles": 0, "bitget_candles": 0, "ws_ticks": 0,
}

# ============================================================
# HTTP UTILS & PERSISTENCE
# ============================================================

async def safe_call(coro, timeout):
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except Exception:
        return None

async def http_get(url, params=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None
    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status in (429, 403):
                        if "kucoin" in url:
                            KUCOIN_STATUS["is_down"] = True
                            KUCOIN_STATUS["last_check"] = time.time()
                        if attempt < retries:
                            await asyncio.sleep(1.0 * (attempt + 1))
                            continue
                        return None
                    if r.status >= 400: return None
                    if "kucoin" in url: KUCOIN_STATUS["is_down"] = False
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries: await asyncio.sleep(0.4)
        except Exception: break
    return None

def num(v, default=0.0):
    try: return float(v) if v is not None else default
    except (TypeError, ValueError): return default

def norm(s):
    if not s: return ""
    s = str(s).upper()
    if s.startswith("XBT"): s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s

def fmt_oi(val):
    if val is None: return "н/д"
    s = f"{val:+.1f}%"
    if val <= -OI_MIN_GROWTH_PCT: s += " (шорт-сквиз)"
    return s

def save_state():
    try:
        data = {
            "oi_history": {base: {exch: list(dq) for exch, dq in exch_map.items()} for base, exch_map in OI_HISTORY.items()},
            "last_signal": LAST_SIGNAL, "accum_last_signal": ACCUM_LAST_SIGNAL,
            "last_climax_signal": LAST_CLIMAX_SIGNAL, "pump_tracks": PUMP_TRACKS,
            "pump_context": PUMP_CONTEXT, "saved_at": time.time(),
        }
        tmp_path = PERSIST_PATH + ".tmp"
        with open(tmp_path, "w") as f: json.dump(data, f)
        os.replace(tmp_path, PERSIST_PATH)
        STATS["state_saved_count"] += 1
    except Exception: pass

def load_state():
    if not os.path.exists(PERSIST_PATH): return
    try:
        with open(PERSIST_PATH, "r") as f: data = json.load(f)
        now = time.time()
        for base, exch_map in data.get("oi_history", {}).items():
            for exch, points in exch_map.items():
                for ts, val in points:
                    if now - ts < 14 * 3600: OI_HISTORY[base][exch].append((ts, val))
        
        LAST_SIGNAL.update(data.get("last_signal", {}))
        ACCUM_LAST_SIGNAL.update(data.get("accum_last_signal", {}))
        LAST_CLIMAX_SIGNAL.update(data.get("last_climax_signal", {}))
        PUMP_CONTEXT.update(data.get("pump_context", {}))
        
        for b, track in data.get("pump_tracks", {}).items():
            if now - track.get("start_ts", 0) < TRACK_TIMEOUT_SEC:
                PUMP_TRACKS[b] = track
                
        STATS["state_loaded"] = True
    except Exception: pass

async def persist_loop():
    while True:
        try: await asyncio.sleep(PERSIST_INTERVAL_SEC); save_state()
        except asyncio.CancelledError: raise
        except Exception: pass

# ============================================================
# WEBSOCKET (Bybit)
# ============================================================

def apply_ws_candle(base, tf, data):
    if not data: return
    row = data[0]
    ts = int(row["start"])
    o, c, h, l, v = num(row["open"]), num(row["close"]), num(row["high"]), num(row["low"]), num(row["volume"])
    confirm = row["confirm"]

    buf = WS_CANDLES[base][tf]
    candle = {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v, "closed": confirm}

    if buf and buf[-1]["ts"] == ts: buf[-1] = candle
    else: buf.append(candle)
    
    STATS["ws_ticks"] += 1

def apply_ws_ticker(base, data):
    if isinstance(data, list): data = data[0]
    oi = num(data.get("openInterest"))
    if oi > 0:
        WS_OI[base] = oi

async def ws_sub_manager(ws):
    global WS_SUBSCRIBED
    while not ws.closed:
        try:
            current_uni = set(UNIVERSE.keys()) | set(PUMP_TRACKS.keys())
            new_subs = current_uni - WS_SUBSCRIBED
            
            if new_subs:
                args = []
                for b in new_subs:
                    args.extend([f"kline.15.{b}USDT", f"kline.60.{b}USDT", f"tickers.{b}USDT"])
                
                for i in range(0, len(args), 10):
                    await ws.send_json({"op": "subscribe", "args": args[i:i+10]})
                    await asyncio.sleep(0.1)
                WS_SUBSCRIBED.update(new_subs)
                log.info("🔔 WS подписка на %d новых монет", len(new_subs))
                
            await asyncio.sleep(10)
        except asyncio.CancelledError: break
        except Exception: await asyncio.sleep(5)

async def bybit_ws_loop():
    global WS_SUBSCRIBED
    backoff = 1
    while True:
        try:
            log.info("🔌 Bybit WS connecting...")
            async with SESSION.ws_connect(BYBIT_WS, heartbeat=20) as ws:
                log.info("✅ Bybit WS connected")
                WS_SUBSCRIBED = set()
                sub_task = asyncio.create_task(ws_sub_manager(ws))

                try:
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                topic = payload.get("topic", "")
                                if topic.startswith("kline."):
                                    parts = topic.split(".")
                                    tf, sym = parts[1], parts[2].replace("USDT", "")
                                    if sym in UNIVERSE or sym in PUMP_TRACKS: apply_ws_candle(sym, tf, payload.get("data", []))
                                elif topic.startswith("tickers."):
                                    sym = topic.split(".")[1].replace("USDT", "")
                                    if sym in UNIVERSE or sym in PUMP_TRACKS: apply_ws_ticker(sym, payload.get("data", {}))
                            except Exception: pass
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR): break
                finally: sub_task.cancel()
        except asyncio.CancelledError: raise
        except Exception as e:
            log.error(f"WS Error: {e}")
            await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

# ============================================================
# REST FETCHERS
# ============================================================

def parse_rows(rows):
    out = []
    for r in rows:
        try:
            ts = int(r[0])
            if ts < 10 ** 12: ts *= 1000
            o, h, l, c, v = num(r[1]), num(r[2]), num(r[3]), num(r[4]), num(r[5])
            if o > 0 and c > 0: out.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError): continue
    out.sort(key=lambda x: x["ts"])
    return out

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    if not data or data.get("code") != "200000": return {}
    res = {}
    for row in data.get("data", []):
        symbol = row.get("symbol", "")
        if not symbol.endswith("USDTM"): continue
        base = norm(row.get("baseCurrency"))
        if not base: continue
        res[base] = {
            "symbol": symbol, "price": num(row.get("markPrice")),
            "volume24": num(row.get("turnoverOf24h")), "change24": num(row.get("priceChgPct")) * 100,
            "first_open_ms": num(row.get("firstOpenDate", 0))
        }
    return res

async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
    if not data or data.get("code") != "00000": return {}
    res = {}
    for row in data.get("data", []):
        symbol = row.get("symbol", "")
        if not symbol.endswith("USDT"): continue
        base = norm(symbol[:-4])
        if not base: continue
        price = num(row.get("lastPr"))
        vol = num(row.get("usdtVolume")) or (num(row.get("baseVolume")) * price)
        res[base] = {"symbol": symbol, "price": price, "volume24": vol, "change24": num(row.get("chgUtc")) * 100}
    return res

async def fetch_bybit_tickers_rest():
    data = await http_get(f"{BYBIT_BASE}/v5/market/tickers", params={"category": "linear"})
    if not data or data.get("retCode") != 0: return {}
    res = {}
    for row in data.get("result", {}).get("list", []):
        symbol = row.get("symbol", "")
        if not symbol.endswith("USDT"): continue
        base = norm(symbol[:-4])
        if not base: continue
        res[base] = {"symbol": symbol}
    return res

async def fetch_bingx_tickers():
    data = await http_get(f"{BINGX_BASE}/openApi/swap/v2/quote/tickers")
    if not data or data.get("code") != 0: return {}
    res = {}
    for row in data.get("data", []):
        symbol = row.get("symbol", "")
        if not symbol.endswith("-USDT"): continue
        base = norm(symbol[:-5])
        if not base: continue
        res[base] = {"symbol": symbol}
    return res

async def fetch_kucoin_candles(symbol, gran_min, limit):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - limit * gran_min * 60 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {"symbol": symbol, "granularity": str(gran_min), "from": from_ms, "to": now_ms})
    if not data or not isinstance(data.get("data"), list): return []
    STATS["kucoin_candles"] += 1
    return parse_rows(data["data"])

async def fetch_bitget_candles(symbol, gran_min, limit):
    gran = "15m" if gran_min == 15 else "1H"
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {"symbol": symbol, "productType": "USDT-FUTURES", "granularity": gran, "limit": str(limit)})
    if not data or data.get("code") != "00000": return []
    STATS["bitget_candles"] += 1
    return parse_rows(data.get("data", []))

async def fetch_kucoin_oi(symbol):
    if not symbol.endswith("USDTM"): return 0.0
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}")
    if data and isinstance(data.get("data"), dict): return num(data["data"].get("openInterest"))
    return 0.0

async def fetch_bitget_oi(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {"symbol": symbol, "productType": "USDT-FUTURES"})
    if not data or data.get("code") != "00000": return 0.0
    raw = data.get("data", {})
    row = raw.get("openList", [{}])[0] if isinstance(raw.get("openList"), list) else raw
    return num(row.get("openInterest") or row.get("amount"))

async def fetch_best_funding(base, kc_sym):
    if kc_sym.endswith("USDTM"):
        d = await http_get(f"{KUCOIN_BASE}/api/v1/funding-rate/{kc_sym}/current")
        if d and isinstance(d.get("data"), dict) and "value" in d["data"]:
            return num(d["data"]["value"])
    d = await http_get(f"{BYBIT_BASE}/v5/market/tickers", {"category": "linear", "symbol": f"{base}USDT"})
    if d and d.get("retCode") == 0:
        lst = d.get("result", {}).get("list", [])
        if lst: return num(lst[0].get("fundingRate"))
    return None

# ============================================================
# МЕТРИКИ & АНАЛИТИКА
# ============================================================

def efficiency_ratio(candles, idx_now, window):
    start = max(0, idx_now - window + 1)
    closes = [c["close"] for c in candles[start:idx_now + 1]]
    if len(closes) < 2: return 0.0
    net = abs(closes[-1] - closes[0])
    path = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))
    return net / path if path > 0 else 0.0

def calc_tf_metrics(candles, tf_cfg):
    lookback, base_n = tf_cfg["lookback_candles"], tf_cfg["base_candles"]
    if len(candles) < lookback + base_n + 2: return None
    idx_now = len(candles) - 2
    idx_past = idx_now - lookback
    if idx_past < 0: return None
    close_now, close_past = candles[idx_now]["close"], candles[idx_past]["close"]
    if close_past <= 0: return None
    pct = ((close_now / close_past) - 1) * 100
    
    if pct > tf_cfg["max_pct"]: return None
        
    base_slice = candles[max(0, idx_past - base_n):idx_past]
    if not base_slice: return None
    base_high = max(c["close"] for c in base_slice)
    breakout = close_now > base_high * BREAKOUT_TOLERANCE
    vol_now = candles[idx_now]["volume"] * candles[idx_now]["close"]
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = vol_now / avg_vol if avg_vol > 0 else 0
    return {"close": close_now, "pct": pct, "breakout": breakout, "base_high": base_high, "rvol": rvol, "efficiency": efficiency_ratio(candles, idx_now, lookback)}

def calc_live_candle_metrics(candles):
    """Используем CLIMAX_MIN_ELAPSED_FRACTION как в v16.5"""
    if len(candles) < 22: return None
    cur = candles[-1]
    prev = candles[-21:-1]
    if cur["open"] <= 0: return None
    
    op, cl, hi, lo = cur["open"], cur["close"], cur["high"], cur["low"]
    rng = hi - lo
    if rng <= 0: return None
    
    upper_wick = hi - max(op, cl)
    wick_ratio = upper_wick / rng
    is_red = cl < op
    pct_from_open = ((cl - op) / op) * 100
    
    prev_vols = [c["volume"] * c["close"] for c in prev if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    if avg_vol <= 0: return None
    
    elapsed_min = (time.time() * 1000 - cur["ts"]) / 1000 / 60
    frac = max(min(elapsed_min / 15.0, 1.0), CLIMAX_MIN_ELAPSED_FRACTION)
    rvol = (cur["volume"] * cur["close"] / frac) / avg_vol
    
    return {"wick_ratio": wick_ratio, "is_red": is_red, "pct_from_open": pct_from_open, "rvol": rvol, "pct": pct_from_open}

def estimate_base_rvol(candles, idx_now):
    if len(candles) < CONTEXT_LOOKBACK + 2: return 0.0, 0.0
    best_rvol = 0.0
    best_price = 0.0
    start_idx = max(0, idx_now - CONTEXT_LOOKBACK)
    for i in range(start_idx, idx_now):
        c = candles[i]
        if c["open"] <= 0: continue
        prev = candles[max(0, i - 20):i]
        if not prev: continue
        avg_vol = sum(p["volume"] * p["close"] for p in prev) / len(prev)
        if avg_vol <= 0: continue
        rvol = (c["volume"] * c["close"]) / avg_vol
        pct = ((c["close"] - c["open"]) / c["open"]) * 100
        if pct > 1.0 and rvol > best_rvol:
            best_rvol = rvol
            best_price = c["close"]
    return best_rvol, best_price

def calc_spark_metrics(candles):
    if len(candles) < SPARK_BASE_CANDLES + 3: return None
    idx_now = len(candles) - 1
    current = candles[idx_now]
    if current["open"] <= 0: return None
    elapsed_min = (time.time() * 1000 - current["ts"]) / 1000 / 60
    if elapsed_min < SPARK_MIN_ELAPSED_MIN or elapsed_min > SPARK_MAX_ELAPSED_MIN: return None
    pct = ((current["close"] / current["open"]) - 1) * 100
    base_slice = candles[max(0, idx_now - SPARK_BASE_CANDLES):idx_now]
    if not base_slice: return None
    base_high = max(c["close"] for c in base_slice)
    breakout = current["close"] > base_high
    vol_usd = current["volume"] * current["close"]
    
    frac = max(min(elapsed_min / 15.0, 1.0), CLIMAX_MIN_ELAPSED_FRACTION)
    normalized_vol = vol_usd / frac
    
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = normalized_vol / avg_vol if avg_vol > 0 else 0
    return {"close": current["close"], "pct": pct, "breakout": breakout, "base_high": base_high, "rvol": rvol, "elapsed_min": elapsed_min}

def oi_growth_pct(base, exch, hours):
    hist = OI_HISTORY[base][exch]
    if len(hist) < 2: return None
    now_ts, now_oi = hist[-1]
    min_span = max(OI_SAMPLE_INTERVAL_SEC * 1.5, min(hours * 3600 * 0.3, OI_SAMPLE_INTERVAL_SEC * 6))
    if now_ts - hist[0][0] < min_span: return None
    target_ts = time.time() - hours * 3600
    past = None
    for ts, val in hist:
        if ts <= target_ts: past = (ts, val)
        else: break
    if past is None: past = hist[0]
    past_oi = past[1]
    if past_oi <= 0: return None
    return ((now_oi / past_oi) - 1) * 100

def score_signal(tf_key, kc, oi_deltas, funding_rate):
    tf_cfg = TIMEFRAMES[tf_key]
    momentum_score = min(40.0, (kc["pct"] / tf_cfg["min_pct"]) * 20.0) if tf_cfg["min_pct"] > 0 else 0.0
    volume_score = min(25.0, (kc["rvol"] / tf_cfg["min_rvol"]) * 12.0) if tf_cfg["min_rvol"] > 0 else 0.0
    best_oi_delta = 0.0
    is_squeeze = False
    for d in oi_deltas.values():
        if d is None: continue
        if abs(d) > abs(best_oi_delta): best_oi_delta = d
    oi_score = min(20.0, (abs(best_oi_delta) / OI_MIN_GROWTH_PCT) * 10.0) if OI_MIN_GROWTH_PCT > 0 else 0.0
    if best_oi_delta < 0 and abs(best_oi_delta) >= OI_MIN_GROWTH_PCT: is_squeeze = True
    funding_bonus = 0.0
    if funding_rate is not None:
        if funding_rate <= FUNDING_EXTREME_NEG: funding_bonus = 10.0
        elif funding_rate <= FUNDING_MILD_NEG: funding_bonus = 5.0
    chop_penalty = (1.0 - kc["efficiency"]) * 15.0
    score = momentum_score + volume_score + oi_score + funding_bonus - chop_penalty
    return score, {"momentum": momentum_score, "volume": volume_score, "oi": oi_score, "funding": funding_bonus, "chop_penalty": -chop_penalty}, best_oi_delta, is_squeeze

def calc_accumulation(base, candles_1h):
    window = ACCUM_WINDOW_HOURS
    if len(candles_1h) < window + 2: return None
    idx_now = len(candles_1h) - 2
    idx_past = idx_now - window
    if idx_past < 0: return None
    start_close, now_close = candles_1h[idx_past]["close"], candles_1h[idx_now]["close"]
    if start_close <= 0: return None
    price_change_pct = abs((now_close - start_close) / start_close) * 100
    if price_change_pct > ACCUM_MAX_PRICE_MOVE_PCT: return None
    if efficiency_ratio(candles_1h, idx_now, window) > ACCUM_MIN_EFFICIENCY_INVERSE: return None
    oi_deltas, growing_sources, max_delta = {}, 0, 0.0
    for exch in ("kucoin", "bitget", "bybit"):
        d = oi_growth_pct(base, exch, window)
        oi_deltas[exch] = d
        if d is not None and d >= ACCUM_MIN_OI_GROWTH_PCT:
            growing_sources += 1
            max_delta = max(max_delta, d)
    if growing_sources < ACCUM_MIN_SOURCES: return None
    return {"price_change_pct": price_change_pct, "efficiency": efficiency_ratio(candles_1h, idx_now, window), "oi_deltas": oi_deltas, "max_delta": max_delta, "close": now_close}

# ============================================================
# TELEGRAM
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or SESSION is None: return False
    try:
        async with SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return False
            return (await r.json(content_type=None)).get("ok", False)
    except Exception: return False

async def send_accumulation_alert(base, acc, source=""):
    msg = (f"🎯 <b>НАКОПЛЕНИЕ [{source}]: {escape(base)}USDT</b>\n\nЦена изменилась на {acc['price_change_pct']:.1f}% за {ACCUM_WINDOW_HOURS}ч (efficiency {acc['efficiency']:.2f}),\nно OI вырос до +{acc['max_delta']:.1f}%.\n\n<b>📊 OI Δ</b>\n  KuCoin: {fmt_oi(acc['oi_deltas'].get('kucoin'))}\n  Bitget: {fmt_oi(acc['oi_deltas'].get('bitget'))}\n  Bybit:  {fmt_oi(acc['oi_deltas'].get('bybit'))}\n\nТекущая цена: <b>{acc['close']:.6g}</b>")
    if await send_tg(msg): STATS["accum_alerts"] += 1

async def send_pump_signal(base, tf_key, kc, bg, bb, oi_deltas, change_24h, score, breakdown, is_squeeze, funding_rate, is_watch, source, fast_status=None):
    tf_cfg = TIMEFRAMES[tf_key]
    live_metrics = calc_live_candle_metrics(list(WS_CANDLES[base]["15"]))
    
    is_fast = fast_status is not None
    is_dump = False
    
    if live_metrics and not is_watch:
        if kc["rvol"] > 8.0 and live_metrics["wick_ratio"] > WICK_DUMP_THRESHOLD and live_metrics["is_red"]:
            is_dump = True

    if is_dump:
        tier_emoji = "🚨"
        tier_label = "EXIT LIQUIDITY (СЛИВ)"
        STATS["dump_alerts"] += 1
    elif is_watch:
        tier_emoji = "👀"
        tier_label = "НАБЛЮДЕНИЕ"
    elif is_fast:
        tier_emoji = "🚀"
        tier_label = "FAST SIGNAL"
    else:
        tier_emoji = tf_cfg["emoji"]
        tier_label = tf_cfg["label"]
    
    conf_lines = []
    if bg: conf_lines.append(f"  Bitget: +{bg['pct']:.1f}% | RVOL {bg['rvol']:.1f}x")
    if bb: conf_lines.append(f"  Bybit:  +{bb['pct']:.1f}% | RVOL {bb['rvol']:.1f}x")
    if not conf_lines: conf_lines.append("  не проверено (нет ответа бирж)")

    funding_line = ""
    if funding_rate is not None:
        funding_line = f"\nFunding: {funding_rate*100:+.3f}%" + (" ⚠️ экстремальный" if funding_rate <= FUNDING_EXTREME_NEG else "")
    else:
        funding_line = "\nFunding: н/д"
    
    context_line = ""
    ctx = PUMP_CONTEXT.get(base)
    
    if not ctx:
        candles_hist = list(WS_CANDLES[base]["15"])
        if candles_hist:
            base_rvol, base_price = estimate_base_rvol(candles_hist, len(candles_hist) - 1)
            if base_rvol > 0:
                PUMP_CONTEXT[base] = {"start_rvol": base_rvol, "start_price": base_price, "ts": time.time()}
                ctx = PUMP_CONTEXT[base]
                
    if ctx:
        start_rvol = ctx["start_rvol"]
        rvol_diff = kc["rvol"] / start_rvol if start_rvol > 0 else 0
        
        if rvol_diff > SCAM_RVOL_RATIO:
            context_line = f"\n🚨 <b>СКАМ / FULL DUMP:</b> Текущий RVOL ({kc['rvol']:.1f}x) в {rvol_diff:.1f} раз больше базового ({start_rvol:.1f}x). Кит сливает всё!"
            tier_emoji = "🚨"
            tier_label = "SCAM / FULL DUMP"
            STATS["scam_alerts"] += 1
        else:
            context_line = f"\n🧹 <b>STOP HUNT / WEAK HANDS:</b> Текущий RVOL ({kc['rvol']:.1f}x) ~= базовому ({start_rvol:.1f}x). Снятие стопов."
    else:
        context_line = f"\n🔄 <b>Контекст:</b> Не удалось найти историческую базу. Фиксируем текущий RVOL {kc['rvol']:.1f}x как базовый."
        PUMP_CONTEXT[base] = {"start_rvol": kc["rvol"], "start_price": kc["close"], "ts": time.time()}

    wick_str = ""
    if live_metrics:
        color = "🔴 Красная" if live_metrics["is_red"] else "🟢 Зеленая"
        wick_str = f"\n🕯 Свеча: {color} (тело {live_metrics['pct_from_open']:+.1f}%) | Верхняя тень: <b>{live_metrics['wick_ratio']*100:.0f}%</b>"

    msg = (f"{tier_emoji} <b>{tier_label}: {escape(base)}USDT</b>  [score {score:.0f}]\n\n"
           f"<b>🎯 {source}</b>\n  Рост: <b>+{kc['pct']:.1f}%</b>\n  Пробой базы: <b>{kc['base_high']:.6g} → {kc['close']:.6g}</b>\n  RVOL: <b>{kc['rvol']:.2f}x</b> | Efficiency: {kc['efficiency']:.2f}{wick_str}{context_line}\n\n"
           f"<b>✅ Подтверждение</b>\n" + ("\n".join(conf_lines)) + "\n\n"
           f"<b>📊 OI Δ</b>\n  KuCoin: {fmt_oi(oi_deltas.get('kucoin'))}\n  Bitget: {fmt_oi(oi_deltas.get('bitget'))}\n  Bybit:  {fmt_oi(oi_deltas.get('bybit'))}{funding_line}\n\n"
           f"<b>⛔ Инвалидация:</b> закрытие ниже {kc['base_high'] * BREAKOUT_TOLERANCE:.6g}\n"
           f"<b>24h change:</b> {change_24h:+.1f}%\n\n"
           f"<i>Score: моментум {breakdown['momentum']:.0f} + объём {breakdown['volume']:.0f} + OI {breakdown['oi']:.0f} + funding {breakdown['funding']:.0f} {breakdown['chop_penalty']:+.0f} (шум)</i>")
    
    if is_fast and fast_status != "ok":
        msg += "\n⚠️ Fast path: Bitget не ответил вовремя — сигнал по одной бирже, без кросс-проверки."
    if is_squeeze: msg += "\n⚠️ Часть роста OI отрицательна — похоже на шорт-сквиз."

    if await send_tg(msg):
        if is_watch: STATS["watch_signals"] += 1
        else:
            STATS["signals"] += 1
            if is_fast:
                STATS["fast_signals"] += 1
                if fast_status != "ok": STATS["fast_unconfirmed"] += 1
            PUMP_TRACKS[base] = {"start_ts": time.time(), "entry": kc["close"], "tf": tf_key, "source": source}

async def send_spark_alert(base, spark, bg_spark, oi_deltas, source):
    bg_line = f"  Bitget: +{bg_spark['pct']:.1f}%" if bg_spark else "  нет данных"
    msg = (f"✨ <b>СПАРК [{source}]: {escape(base)}USDT</b>\n\nДвижение только началось — ~{spark['elapsed_min']:.0f} мин назад.\nРост: <b>+{spark['pct']:.1f}%</b> | RVOL: <b>{spark['rvol']:.1f}x</b>\nПробой: {spark['base_high']:.6g} → {spark['close']:.6g}\n\n<b>Подтверждение (Bitget)</b>\n{bg_line}\n\n<b>📊 OI</b>\n  KuCoin: {fmt_oi(oi_deltas.get('kucoin'))}\n  Bitget: {fmt_oi(oi_deltas.get('bitget'))}\n  Bybit:  {fmt_oi(oi_deltas.get('bybit'))}\n\n⚠️ Самая ранняя и самая шумная стадия — ниже уверенность. Цель — скорость.")
    if await send_tg(msg): 
        STATS["spark_alerts"] += 1
        if base not in PUMP_CONTEXT:
            PUMP_CONTEXT[base] = {"start_rvol": spark["rvol"], "start_price": spark["close"], "ts": time.time()}

async def send_climax_alert(base, live, oi_bb, reason):
    msg = (f"🥊 <b>КУЛЬМИНАЦИЯ / СЛИВ КИТА!</b>\n\n📌 <b>{escape(base)}USDT</b>\n⚠️ {reason}\n\n📈 Текущая свеча: <b>{live['pct']:+.2f}%</b>\n⚡ RVOL: <b>{live['rvol']:.1f}x</b>\n📊 OI (Bybit, 15 мин): <b>{fmt_oi(oi_bb)}</b>\n\n🚨 <i>Рассмотри выход из лонга. Возможен резкий разворот.</i>")
    if await send_tg(msg): STATS["climax_alerts"] += 1

async def handle_telegram_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg or (CHAT_ID and str(msg.get("chat", {}).get("id", "")) != str(CHAT_ID)): return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/status") or text.startswith("/health"):
        await send_tg(f"📊 <b>PUMP-HUNTER v17.6 (Hardened)</b>\n⏱ Аптайм: {(time.time()-START_TIME)/3600:.1f}ч\n🌐 Юниверс: {len(UNIVERSE)} пар | Треки: {len(PUMP_TRACKS)}\n⚡ WS Ticks: {STATS['ws_ticks']}\n✅ Сигналов: {STATS['signals']} (Fast: {STATS['fast_signals']}, без подтв: {STATS['fast_unconfirmed']})\n👀 Наблюдений: {STATS['watch_signals']}\n🚨 Сливов: {STATS['dump_alerts']} | Скамов: {STATS['scam_alerts']}\n✨ Спарков: {STATS['spark_alerts']} | 🥊 Кульминаций: {STATS['climax_alerts']}\n\n<b>Отсев:</b> без пробоя {STATS['rejected_no_breakout']}, нет подтв {STATS['rejected_no_confirm']}, нет данных {STATS['confirm_unavailable']}, низкий score {STATS['rejected_low_score']}")

async def telegram_poll_loop():
    if not BOT_TOKEN: return
    offset = None
    while True:
        try:
            params = {"timeout": 25}
            if offset: params["offset"] = offset
            async with SESSION.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates", params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    for u in data.get("result", []):
                        offset = u["update_id"] + 1
                        await handle_telegram_update(u)
                else: await asyncio.sleep(3)
        except asyncio.CancelledError: raise
        except Exception: await asyncio.sleep(5)

# ============================================================
# PROCESS SYMBOL & TRACKER
# ============================================================

async def check_climax(base, item):
    track = PUMP_TRACKS.get(base)
    if not track: return False
    now = time.time()

    if now - track.get("start_ts", 0) > TRACK_TIMEOUT_SEC:
        del PUMP_TRACKS[base]
        STATS["tracks_timed_out"] += 1
        return True

    candles = list(WS_CANDLES[base]["15"])
    if not candles: return False
    live = calc_live_candle_metrics(candles)
    
    if not live: return False

    if live["close"] < track["entry"] * 0.95:
        del PUMP_TRACKS[base]
        STATS["tracks_invalidated"] += 1
        return True

    oi_bb = oi_growth_pct(base, "bybit", 0.25)
    reason = None
    if live["rvol"] >= CLIMAX_RVOL_THRESHOLD:
        reason = "Аномальный объём (возможная выгрузка в стакан)"
    elif oi_bb is not None and oi_bb <= CLIMAX_OI_DROP_PCT and live["pct"] > 5.0:
        reason = "Резкое падение OI на росте (фиксация позиций)"

    if reason and now - LAST_CLIMAX_SIGNAL.get(base, 0) > CLIMAX_COOLDOWN_SEC:
        LAST_CLIMAX_SIGNAL[base] = now
        await send_climax_alert(base, live, oi_bb, reason)
        del PUMP_TRACKS[base]
        return True
    return False

async def quick_confirm(item, tf_cfg):
    candles = await safe_call(fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"]), FAST_CONFIRM_TIMEOUT_SEC)
    if not candles: return None, "unavailable"
    m = calc_tf_metrics(candles, tf_cfg)
    if not m: return None, "unavailable"
    if m["pct"] >= tf_cfg["min_pct"] * CONFIRM_PCT_RATIO: return m, "ok"
    return m, "contradict"

async def is_live_candle_hot(base, item):
    candles = list(WS_CANDLES[base]["15"])
    if not candles: return False
    live = candles[-1]
    if live["open"] <= 0: return False
    return ((live["close"] - live["open"]) / live["open"]) * 100 > ANTI_HIGH_MAX_PCT

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        PUMP_TRACKS.pop(base, None)
        return
    now = time.time()

    if base in PUMP_TRACKS:
        await check_climax(base, item)
        return

    if now - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC or now - LAST_REJECT.get(base, 0) < REJECT_COOLDOWN_SEC: return

    change24 = item.get("change24", 0)
    if change24 > MAX_24H_CHANGE_PCT:
        STATS["rejected_anti_high"] += 1
        LAST_REJECT[base] = time.time()
        return

    best = None
    live_hot = None

    for tf_key, tf_cfg in TIMEFRAMES.items():
        tf_map = "15" if tf_key == "impulse" else "60"
        candles = list(WS_CANDLES[base][tf_map])
        if not candles: continue

        if tf_key == "grind" and ACCUM_ENABLED and now - ACCUM_LAST_SIGNAL.get(base, 0) >= ACCUM_COOLDOWN_SEC:
            acc = calc_accumulation(base, candles)
            if acc: ACCUM_LAST_SIGNAL[base] = time.time(); await send_accumulation_alert(base, acc, "WS")

        if tf_key == "impulse" and SPARK_ENABLED and now - LAST_SPARK_SIGNAL.get(base, 0) >= SPARK_COOLDOWN_SEC:
            spark = calc_spark_metrics(candles)
            if spark and spark["breakout"] and spark["pct"] >= SPARK_MIN_PCT and spark["rvol"] >= SPARK_MIN_RVOL:
                bg_spark = calc_spark_metrics(await fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"]))
                if bg_spark and bg_spark["pct"] >= SPARK_MIN_PCT * SPARK_CONFIRM_RATIO:
                    oi_spark = {exch: oi_growth_pct(base, exch, 1.0) for exch in ("kucoin", "bitget", "bybit")}
                    LAST_SPARK_SIGNAL[base] = now; await send_spark_alert(base, spark, bg_spark, oi_spark, "WS")

        kc = calc_tf_metrics(candles, tf_cfg)
        if not kc or kc["pct"] < tf_cfg["min_pct"] * 0.5: continue
        
        if live_hot is None:
            live_hot = await is_live_candle_hot(base, item)
        if live_hot:
            STATS["rejected_anti_high"] += 1; LAST_REJECT[base] = time.time(); break

        if not kc["breakout"]: STATS["rejected_no_breakout"] += 1; continue

        oi_deltas = {exch: oi_growth_pct(base, exch, tf_cfg["oi_window_hours"]) for exch in ("kucoin", "bitget", "bybit")}

        if kc["rvol"] >= FAST_SIGNAL_RVOL and kc["pct"] >= FAST_SIGNAL_PCT:
            bg_fast, status = await quick_confirm(item, tf_cfg)
            if status == "contradict":
                STATS["rejected_no_confirm"] += 1
                LAST_REJECT[base] = time.time()
                continue
            STATS["triggers"] += 1
            score, breakdown, best_oi, is_sq = score_signal(tf_key, kc, oi_deltas, None)
            if best is None or score > best["score"]:
                best = {"score": score, "tf_key": tf_key, "kc": kc, "oi_deltas": oi_deltas, "bg": bg_fast, "bb": None,
                        "is_squeeze": is_sq, "breakdown": breakdown, "funding": None, "source": "WS", "fast_status": status}
            break

        bg_c, bb_c, funding = await asyncio.gather(
            safe_call(fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"]), CONFIRM_TIMEOUT_SEC),
            safe_call(fetch_kucoin_candles(item["symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"]), CONFIRM_TIMEOUT_SEC),
            safe_call(fetch_best_funding(base, item["symbol"]), CONFIRM_TIMEOUT_SEC),
        )
        bg = calc_tf_metrics(bg_c, tf_cfg) if bg_c else None
        bb = calc_tf_metrics(bb_c, tf_cfg) if bb_c else None

        available = [m for m in (bg, bb) if m]
        if not available:
            STATS["confirm_unavailable"] += 1
            continue
            
        confirm_threshold = tf_cfg["min_pct"] * CONFIRM_PCT_RATIO
        if sum(1 for m in available if m["pct"] >= confirm_threshold) < MIN_CONFIRMATIONS:
            STATS["rejected_no_confirm"] += 1; LAST_REJECT[base] = time.time(); continue

        STATS["triggers"] += 1
        score, breakdown, best_oi, is_sq = score_signal(tf_key, kc, oi_deltas, funding)

        if best is None or score > best["score"]:
            best = {"score": score, "tf_key": tf_key, "kc": kc, "oi_deltas": oi_deltas, "bg": bg, "bb": bb,
                    "is_squeeze": is_sq, "breakdown": breakdown, "funding": funding, "source": "WS", "fast_status": None}

    if not best: return
    if best["score"] < SCORE_WATCH_THRESHOLD:
        STATS["rejected_low_score"] += 1; LAST_REJECT[base] = time.time(); return
        
    is_watch = best["score"] < SCORE_SIGNAL_THRESHOLD
    LAST_SIGNAL[base] = time.time()
    await send_pump_signal(base, best["tf_key"], best["kc"], best["bg"], best["bb"], best["oi_deltas"], change24, best["score"], best["breakdown"], best["is_squeeze"], best["funding"], is_watch, best["source"], best["fast_status"])

# ============================================================
# BACKGROUND LOOPS
# ============================================================

async def sample_oi_for_symbol(base):
    item = UNIVERSE.get(base)
    if not item: return
    now = time.time()
    kc_oi, bg_oi = await asyncio.gather(fetch_kucoin_oi(item["symbol"]), fetch_bitget_oi(item["bitget_symbol"]))
    bb_oi = WS_OI.get(base, 0.0)
    
    if kc_oi > 0: OI_HISTORY[base]["kucoin"].append((now, kc_oi))
    if bg_oi > 0: OI_HISTORY[base]["bitget"].append((now, bg_oi))
    if bb_oi > 0: OI_HISTORY[base]["bybit"].append((now, bb_oi))
    STATS["oi_samples"] += 1

async def oi_sample_cycle():
    if not UNIVERSE: return
    bases = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    sem = asyncio.Semaphore(10)
    async def worker(b):
        async with sem: await sample_oi_for_symbol(b)
    await asyncio.gather(*[worker(b) for b in bases])
    gc.collect()

async def refresh_universe():
    global UNIVERSE
    kc, bg, bb, bingx = await asyncio.gather(
        fetch_kucoin_contracts(), 
        fetch_bitget_tickers(), 
        fetch_bybit_tickers_rest(),
        fetch_bingx_tickers(),
        return_exceptions=True
    )
    
    if isinstance(kc, Exception): kc = {}
    if isinstance(bg, Exception): bg = {}
    if isinstance(bb, Exception): bb = {}
    if isinstance(bingx, Exception): bingx = {}
    
    if not kc or not bg or not bb or not bingx:
        log.warning("⚠️ Не все биржи ответили. Ждем...")
        return

    common_bases = set(kc.keys()) & set(bg.keys()) & set(bb.keys()) & set(bingx.keys())
    log.info(f"🔍 Пересечение 4-х бирж (KuCoin+Bitget+Bybit+BingX): {len(common_bases)} монет")

    now_ms = time.time() * 1000
    min_age_ms = MIN_LISTING_AGE_DAYS * 86400 * 1000
    
    uni = {}
    for base in common_bases:
        info = kc[base]
        if info["volume24"] < MIN_24H_VOLUME_USDT or not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT): continue
        if info.get("first_open_ms", 0) > 0 and (now_ms - info["first_open_ms"]) < min_age_ms: continue
        
        uni[base] = {
            "symbol": info.get("symbol", f"{base}USDTM"), 
            "bitget_symbol": f"{base}USDT", 
            "bingx_symbol": bingx[base]["symbol"],
            "volume24": info["volume24"], 
            "change24": info.get("change24", 0)
        }
    
    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс (Только общие + BingX): %d пар", len(UNIVERSE))
    gc.collect()

async def universe_loop():
    while True:
        try: await asyncio.sleep(UNIVERSE_REFRESH_SEC); await refresh_universe()
        except asyncio.CancelledError: raise
        except Exception: await asyncio.sleep(10)

async def oi_loop():
    while True:
        try:
            if UNIVERSE: await oi_sample_cycle()
        except asyncio.CancelledError: raise
        except Exception: pass
        await asyncio.sleep(OI_SAMPLE_INTERVAL_SEC)

async def scan_loop():
    while True:
        try:
            ordered = list(dict.fromkeys(list(PUMP_TRACKS.keys()) + list(UNIVERSE.keys())))
            active = ordered[:MAX_SCAN_CANDIDATES]
            
            if active:
                STATS["scans"] += 1
                sem = asyncio.Semaphore(15)
                async def worker(b):
                    async with sem: await process_symbol(b)
                await asyncio.gather(*[worker(b) for b in active])
                gc.collect()
        except asyncio.CancelledError: raise
        except Exception: pass
        await asyncio.sleep(SCAN_INTERVAL_SEC)

# ============================================================
# WEB & STARTUP
# ============================================================

async def index(req):
    return web.Response(text=f"v17.6 | Uni: {len(UNIVERSE)} | WS Ticks: {STATS['ws_ticks']} | Signals: {STATS['signals']} | Dumps: {STATS['dump_alerts']}", content_type="text/plain")

async def start(app):
    global SESSION, HTTP_SEMAPHORE
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(15)
    load_state()
    if BOT_TOKEN:
        try:
            async with SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", params={"drop_pending_updates": "true"}) as r: pass
        except Exception: pass

    await refresh_universe()
    
    app["ws_task"] = asyncio.create_task(bybit_ws_loop())
    app["universe_task"] = asyncio.create_task(universe_loop())
    app["oi_task"] = asyncio.create_task(oi_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["poll_task"] = asyncio.create_task(telegram_poll_loop())
    app["persist_task"] = asyncio.create_task(persist_loop())

    await send_tg(f"🚀 <b>PUMP-HUNTER v17.6 (Hardened) запущен.</b>\nВнедрены защиты из v16.5: честный н/д, индивидуальные тайм-ауты, защита холодной свечи.")

async def stop(app):
    for key in ("ws_task", "universe_task", "oi_task", "scan_task", "poll_task", "persist_task"):
        t = app.get(key)
        if t: t.cancel()
    save_state()
    if SESSION and not SESSION.closed: await SESSION.close()

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(start)
app.on_cleanup.append(stop)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
