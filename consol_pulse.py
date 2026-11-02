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
# ПАМП-ХАНТЕР v12.0 — FINAL Tri-Exchange (Debug Gate WS)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "15"))
OI_SAMPLE_INTERVAL_SEC = int(os.environ.get("OI_SAMPLE_INTERVAL_SEC", "300"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "300"))
PERSIST_INTERVAL_SEC = int(os.environ.get("PERSIST_INTERVAL_SEC", "300"))
PERSIST_PATH = os.environ.get("PERSIST_PATH", "/tmp/pump_hunter_state.json")

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "60"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "60"))
MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "300000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "1.0"))
TOP_GAINERS_COUNT = int(os.environ.get("TOP_GAINERS_COUNT", "20"))

TIMEFRAMES = {
    "impulse": {
        "granularity_min": 15,
        "lookback_candles": int(os.environ.get("IMPULSE_LOOKBACK_CANDLES", "2")),
        "base_candles": int(os.environ.get("IMPULSE_BASE_CANDLES", "96")),
        "min_pct": float(os.environ.get("IMPULSE_MIN_PCT", "4.0")),
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
SPARK_BASE_CANDLES = int(os.environ.get("SPARK_BASE_CANDLES", "24"))
SPARK_MIN_ELAPSED_MIN = float(os.environ.get("SPARK_MIN_ELAPSED_MIN", "2.0"))
SPARK_CONFIRM_RATIO = float(os.environ.get("SPARK_CONFIRM_RATIO", "0.4"))
SPARK_COOLDOWN_SEC = int(os.environ.get("SPARK_COOLDOWN_SEC", str(2 * 3600)))

SCORE_WATCH_THRESHOLD = float(os.environ.get("SCORE_WATCH_THRESHOLD", "35"))
SCORE_SIGNAL_THRESHOLD = float(os.environ.get("SCORE_SIGNAL_THRESHOLD", "55"))
OI_MIN_GROWTH_PCT = float(os.environ.get("OI_MIN_GROWTH_PCT", "8.0"))
FUNDING_EXTREME_NEG = float(os.environ.get("FUNDING_EXTREME_NEG", "-0.0005"))
FUNDING_MILD_NEG = float(os.environ.get("FUNDING_MILD_NEG", "-0.0002"))
CLIMAX_RVOL_THRESHOLD = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "15.0")) 
CLIMAX_OI_DROP_PCT = float(os.environ.get("CLIMAX_OI_DROP_PCT", "-5.0")) 

ANTI_HIGH_MAX_PCT = float(os.environ.get("ANTI_HIGH_MAX_PCT", "5.0"))

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

OKX_BASE = "https://www.okx.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"
GATE_BASE = "https://api.gateio.ws/api/v4"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
GATE_WS = "wss://fx-ws.gateio.ws/v4/ws/usdt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v12.0")

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

CANDLES = defaultdict(lambda: {"15": deque(maxlen=150), "60": deque(maxlen=150)})
LIVE_OI = {}

OI_HIST_MAXLEN = int((14 * 3600) / OI_SAMPLE_INTERVAL_SEC) + 10
OI_HISTORY = defaultdict(lambda: deque(maxlen=OI_HIST_MAXLEN))

WS_TASKS = {}
WS_SUBSCRIBED_KEYS = {"bybit": set(), "gate": set()}

STATS = {
    "scans": 0, "oi_samples": 0, "triggers": 0, "signals": 0,
    "watch_signals": 0, "accum_alerts": 0, "spark_alerts": 0, "climax_alerts": 0,
    "rejected_no_breakout": 0, "rejected_no_confirm": 0, "rejected_low_score": 0, "rejected_anti_high": 0,
    "state_loaded": False, "state_saved_count": 0,
    "ws_ticks_bybit": 0, "ws_ticks_gate": 0,
    "ws_oi_bybit": 0, "ws_oi_gate": 0,
}

# ============================================================
# HTTP UTILS & PERSISTENCE
# ============================================================

async def http_get(url, params=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None
    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status == 429:
                        if attempt < retries:
                            await asyncio.sleep(1.0 * (attempt + 1))
                            continue
                        return None
                    if r.status >= 400:
                        return None
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries:
                await asyncio.sleep(0.4)
        except Exception:
            break
    return None

def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default

def norm(s):
    if not s: return ""
    s = str(s).upper()
    if s.startswith("XBT"): s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s

def save_state():
    try:
        data = {
            "LAST_SIGNAL": LAST_SIGNAL, "ACCUM_LAST_SIGNAL": ACCUM_LAST_SIGNAL,
            "LAST_SPARK_SIGNAL": LAST_SPARK_SIGNAL, "LAST_CLIMAX_SIGNAL": LAST_CLIMAX_SIGNAL,
            "PUMP_TRACKS": PUMP_TRACKS
        }
        with open(PERSIST_PATH, "w") as f:
            json.dump(data, f)
        STATS["state_saved_count"] += 1
    except Exception:
        pass

def load_state():
    try:
        if os.path.exists(PERSIST_PATH):
            with open(PERSIST_PATH, "r") as f:
                data = json.load(f)
                LAST_SIGNAL.update(data.get("LAST_SIGNAL", {}))
                ACCUM_LAST_SIGNAL.update(data.get("ACCUM_LAST_SIGNAL", {}))
                LAST_SPARK_SIGNAL.update(data.get("LAST_SPARK_SIGNAL", {}))
                LAST_CLIMAX_SIGNAL.update(data.get("LAST_CLIMAX_SIGNAL", {}))
                PUMP_TRACKS.update(data.get("PUMP_TRACKS", {}))
                STATS["state_loaded"] = True
    except Exception:
        pass

async def persist_loop():
    while True:
        await asyncio.sleep(PERSIST_INTERVAL_SEC)
        save_state()

# ============================================================
# WEBSOCKET DATA HANDLERS
# ============================================================

def apply_bybit_candle(base, tf, data):
    if not data: return
    row = data[0]
    ts = int(row["start"])
    o, c, h, l, v = num(row["open"]), num(row["close"]), num(row["high"]), num(row["low"]), num(row["volume"])
    confirm = row["confirm"]
    buf = CANDLES[base][tf]
    candle = {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v, "closed": confirm}
    if buf and buf[-1]["ts"] == ts: buf[-1] = candle
    else: buf.append(candle)
    STATS["ws_ticks_bybit"] += 1

def apply_bybit_ticker(base, data):
    if isinstance(data, list): data = data[0]
    oi = num(data.get("openInterest"))
    if oi > 0:
        LIVE_OI[base] = oi
        STATS["ws_oi_bybit"] += 1

def apply_gate_candle(base, data, interval):
    try:
        ts = int(data["t"]) * 1000
        o, c, h, l, v = num(data["o"]), num(data["c"]), num(data["h"]), num(data["l"]), num(data["v"])
        tf_key = "15" if interval == "15m" else "60"
        buf = CANDLES[base][tf_key]
        candle = {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v, "closed": True}
        if buf and buf[-1]["ts"] == ts: buf[-1] = candle
        else: buf.append(candle)
        STATS["ws_ticks_gate"] += 1
    except Exception as e:
        log.error(f"Gate candle error: {e} | Data: {data}")

# ============================================================
# EXCHANGES REST API
# ============================================================

async def fetch_bitget_contracts():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
    if not data or data.get("code") != "00000": return {}
    res = {}
    for row in data.get("data", []):
        symbol = row.get("symbol", "")
        if not symbol.endswith("USDT"): continue
        base = norm(symbol[:-4])
        if not base: continue
        price = num(row.get("lastPr"))
        vol_usdt = num(row.get("usdtVolume"))
        if vol_usdt == 0: vol_usdt = num(row.get("quoteVolume"))
        if vol_usdt == 0: vol_usdt = num(row.get("baseVolume")) * price
        res[base] = {"price": price, "volume24": vol_usdt, "change24": num(row.get("chgUtc")) * 100, "first_open_ms": 0}
    return res

async def fetch_gate_tickers():
    data = await http_get(f"{GATE_BASE}/futures/usdt/tickers", timeout=10)
    if not data or not isinstance(data, list): return {}
    res = {}
    for row in data:
        contract = row.get("contract", "")
        if not contract.endswith("_USDT"): continue
        base = norm(contract.replace("_USDT", ""))
        if not base: continue
        price = num(row.get("last"))
        vol_usdt = num(row.get("volume_24h_quote"))
        if vol_usdt == 0: vol_usdt = num(row.get("volume_24h_usd"))
        if vol_usdt == 0: vol_usdt = num(row.get("volume_24h_settle"))
        res[base] = {"price": price, "volume24": vol_usdt, "change24": num(row.get("change_percentage")), "first_open_ms": 0}
    return res

async def fetch_okx_funding(inst_id):
    data = await http_get(f"{OKX_BASE}/api/v5/public/funding-rate", params={"instId": inst_id})
    if data and data.get("code") == "0":
        lst = data.get("data", [])
        if lst: return num(lst[0].get("fundingRate"))
    return 0.0

async def fetch_bybit_funding(symbol):
    data = await http_get(f"{BYBIT_BASE}/v5/market/funding/history", {"category": "linear", "symbol": symbol, "limit": 1})
    if not data or data.get("retCode") != 0: return 0.0
    lst = data.get("result", {}).get("list", [])
    if lst: return num(lst[0].get("fundingRate"))
    return 0.0

async def fetch_best_funding(base, inst_id):
    rate = await fetch_okx_funding(inst_id)
    if rate != 0.0: return rate, "OKX"
    rate = await fetch_bybit_funding(f"{base}USDT")
    return rate, "Bybit"

async def fetch_bitget_candles(symbol, gran_min, limit=120):
    gran = "15m" if gran_min == 15 else "1H"
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {"symbol": symbol, "productType": "USDT-FUTURES", "granularity": gran, "limit": str(limit)})
    if not data or data.get("code") != "00000": return []
    out = []
    for r in reversed(data.get("data", [])):
        out.append({"ts": int(r[0]), "open": num(r[1]), "high": num(r[2]), "low": num(r[3]), "close": num(r[4]), "volume": num(r[5]), "closed": True})
    return out

# ============================================================
# WEBSOCKET LOOPS
# ============================================================

async def bybit_ws_loop():
    backoff = 1
    while True:
        try:
            log.info("🔌 Bybit WS connecting...")
            async with SESSION.ws_connect(BYBIT_WS, heartbeat=20) as ws:
                log.info("✅ Bybit WS connected")
                WS_SUBSCRIBED_KEYS["bybit"] = set()
                sub_task = asyncio.create_task(ws_sub_manager_bybit(ws))
                try:
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                topic = payload.get("topic", "")
                                if topic.startswith("kline."):
                                    parts = topic.split(".")
                                    tf, sym = parts[1], parts[2].replace("USDT", "")
                                    if sym in UNIVERSE or sym in PUMP_TRACKS: apply_bybit_candle(sym, tf, payload.get("data", []))
                                elif topic.startswith("tickers."):
                                    sym = topic.split(".")[1].replace("USDT", "")
                                    if sym in UNIVERSE or sym in PUMP_TRACKS: apply_bybit_ticker(sym, payload.get("data", {}))
                            except Exception: pass
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR): break
                finally: sub_task.cancel()
        except asyncio.CancelledError: raise
        except Exception as e:
            log.error(f"Bybit WS Error: {e}")
            await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

async def gate_ws_loop():
    backoff = 1
    while True:
        try:
            log.info("🔌 Gate.io WS connecting...")
            async with SESSION.ws_connect(GATE_WS) as ws:
                log.info("✅ Gate.io WS connected")
                WS_SUBSCRIBED_KEYS["gate"] = set()
                sub_task = asyncio.create_task(ws_sub_manager_gate(ws))
                ping_task = asyncio.create_task(gate_ping_loop(ws))
                try:
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                channel = payload.get("channel")
                                if channel == "futures.candlesticks":
                                    results = payload.get("result", [])
                                    if isinstance(results, list):
                                        for res in results:
                                            if isinstance(res, dict):
                                                base = norm(res.get("a", "").replace("_USDT", ""))
                                                interval = res.get("n", "15m")
                                                if base in UNIVERSE or base in PUMP_TRACKS:
                                                    apply_gate_candle(base, res, interval)
                                    elif isinstance(results, dict):
                                        base = norm(results.get("a", "").replace("_USDT", ""))
                                        interval = results.get("n", "15m")
                                        if base in UNIVERSE or base in PUMP_TRACKS:
                                            apply_gate_candle(base, results, interval)
                                elif channel == "futures.tickers":
                                    results = payload.get("result", [])
                                    if isinstance(results, list):
                                        for res in results:
                                            if isinstance(res, dict):
                                                base = norm(res.get("contract", "").replace("_USDT", ""))
                                                if base in UNIVERSE or base in PUMP_TRACKS:
                                                    oi = num(res.get("open_interest"))
                                                    if oi > 0:
                                                        LIVE_OI[base] = oi
                                                        STATS["ws_oi_gate"] += 1
                            except Exception as e:
                                log.error(f"Gate WS Parse Error: {e} | Data: {msg.data[:100]}")
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR): break
                finally:
                    sub_task.cancel(); ping_task.cancel()
        except asyncio.CancelledError: raise
        except Exception as e:
            log.error(f"Gate WS Error: {e}")
            await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

async def gate_ping_loop(ws):
    while not ws.closed:
        await asyncio.sleep(15)
        try: await ws.send_json({"time": int(time.time()), "channel": "futures.ping"})
        except: break

async def ws_sub_manager_bybit(ws):
    while not ws.closed:
        try:
            current_uni = set(UNIVERSE.keys()) | set(PUMP_TRACKS.keys())
            new_subs = current_uni - WS_SUBSCRIBED_KEYS["bybit"]
            if new_subs:
                args = []
                for b in new_subs:
                    args.extend([f"kline.15.{b}USDT", f"kline.60.{b}USDT", f"tickers.{b}USDT"])
                for i in range(0, len(args), 10):
                    await ws.send_json({"op": "subscribe", "args": args[i:i+10]})
                    await asyncio.sleep(0.1)
                WS_SUBSCRIBED_KEYS["bybit"].update(new_subs)
            await asyncio.sleep(10)
        except asyncio.CancelledError: break
        except Exception: await asyncio.sleep(5)

async def ws_sub_manager_gate(ws):
    while not ws.closed:
        try:
            current_uni = set(UNIVERSE.keys()) | set(PUMP_TRACKS.keys())
            new_subs = current_uni - WS_SUBSCRIBED_KEYS["gate"]
            if new_subs:
                for b in new_subs:
                    contract = f"{b}_USDT"
                    await ws.send_json({"time": int(time.time()), "channel": "futures.candlesticks", "event": "subscribe", "payload": ["15m", contract]})
                    await asyncio.sleep(0.05)
                    await ws.send_json({"time": int(time.time()), "channel": "futures.candlesticks", "event": "subscribe", "payload": ["1h", contract]})
                    await asyncio.sleep(0.05)
                    await ws.send_json({"time": int(time.time()), "channel": "futures.tickers", "event": "subscribe", "payload": [contract]})
                    await asyncio.sleep(0.05)
                WS_SUBSCRIBED_KEYS["gate"].update(new_subs)
                log.info(f"🔔 Gate WS подписка на {len(new_subs)} монет")
            await asyncio.sleep(10)
        except asyncio.CancelledError: break
        except Exception: await asyncio.sleep(5)

async def bootstrap_candles():
    log.info("🕯 Загрузка истории свечей через Bybit REST...")
    sem = asyncio.Semaphore(5)
    async def fetch(base):
        async with sem:
            d15 = await http_get(f"{BYBIT_BASE}/v5/market/kline", {"category": "linear", "symbol": f"{base}USDT", "interval": "15", "limit": 150})
            if d15 and d15.get("retCode") == 0:
                for r in reversed(d15["result"]["list"]):
                    CANDLES[base]["15"].append({"ts": int(r[0]), "open": num(r[1]), "high": num(r[2]), "low": num(r[3]), "close": num(r[4]), "volume": num(r[5]), "closed": True})
            d60 = await http_get(f"{BYBIT_BASE}/v5/market/kline", {"category": "linear", "symbol": f"{base}USDT", "interval": "60", "limit": 150})
            if d60 and d60.get("retCode") == 0:
                for r in reversed(d60["result"]["list"]):
                    CANDLES[base]["60"].append({"ts": int(r[0]), "open": num(r[1]), "high": num(r[2]), "low": num(r[3]), "close": num(r[4]), "volume": num(r[5]), "closed": True})
    await asyncio.gather(*[fetch(b) for b in list(UNIVERSE.keys())])
    log.info("✅ История загружена")

# ============================================================
# LIVE OI & METRICS
# ============================================================

async def live_oi_sampling_loop():
    while True:
        try:
            await asyncio.sleep(OI_SAMPLE_INTERVAL_SEC)
            now = time.time()
            for base, oi in LIVE_OI.items():
                if oi > 0: OI_HISTORY[base].append((now, oi))
            STATS["oi_samples"] += 1
        except asyncio.CancelledError: raise
        except Exception: pass

def oi_growth_pct(base, hours):
    hist = OI_HISTORY.get(base)
    if not hist or len(hist) < 2: return 0.0
    now = time.time()
    target_ts = now - (hours * 3600)
    old_val = hist[0][1]
    for ts, val in hist:
        if ts >= target_ts: old_val = val; break
    curr_val = hist[-1][1]
    if old_val <= 0: return 0.0
    return ((curr_val - old_val) / old_val) * 100.0

def efficiency_ratio(candles, lookback):
    if len(candles) < lookback + 1: return 0.5
    recent = candles[-lookback:]
    net_move = abs(recent[-1]["close"] - recent[0]["open"])
    path_sum = sum(abs(c["close"] - c["open"]) for c in recent)
    if path_sum == 0: return 0.0
    return net_move / path_sum

def calc_tf_metrics(candles, cfg):
    lb, bc = cfg["lookback_candles"], cfg["base_candles"]
    if len(candles) < bc + lb: return None
    base_cs, recent_cs = candles[-(bc + lb):-lb], candles[-lb:]
    vols = [c["volume"] for c in base_cs]
    avg_vol = sum(vols) / len(vols) if vols else 1.0
    recent_vol = sum(c["volume"] for c in recent_cs)
    expected_vol = avg_vol * lb if len(base_cs) > 0 else 1.0
    rvol = recent_vol / expected_vol if expected_vol > 0 else 1.0
    start_p, end_p = base_cs[-1]["close"], recent_cs[-1]["close"]
    pct = ((end_p - start_p) / start_p) * 100 if start_p > 0 else 0.0
    highs_base = max(c["high"] for c in base_cs)
    breakout = end_p >= highs_base * BREAKOUT_TOLERANCE
    return {"pct": pct, "rvol": rvol, "breakout": breakout, "start_price": start_p, "end_price": end_p, "high_base": highs_base}

def calc_spark_metrics(candles, is_live=True):
    if len(candles) < SPARK_BASE_CANDLES + 3: return None
    if is_live:
        idx_now = len(candles) - 1
        current = candles[idx_now]
        elapsed_min = (time.time() * 1000 - current["ts"]) / 1000 / 60
        if elapsed_min < SPARK_MIN_ELAPSED_MIN: return None
        norm_factor = min(elapsed_min / 15.0, 1.0)
    else:
        idx_now = len(candles) - 2
        current = candles[idx_now]
        elapsed_min, norm_factor = 15.0, 1.0
    if current["open"] <= 0: return None
    pct = ((current["close"] - current["open"]) / current["open"]) * 100
    base_slice = candles[max(0, idx_now - SPARK_BASE_CANDLES):idx_now]
    if not base_slice: return None
    base_high = max(c["high"] for c in base_slice)
    breakout = current["close"] >= base_high * BREAKOUT_TOLERANCE
    vol_usd_so_far = current["volume"] * current["close"]
    normalized_vol = vol_usd_so_far / norm_factor
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = normalized_vol / avg_vol if avg_vol > 0 else 0
    return {"close": current["close"], "pct": pct, "breakout": breakout, "base_high": base_high, "rvol": rvol, "elapsed_min": elapsed_min}

def score_signal(tf_key, kc, oi_delta, funding):
    score, breakdown = 0.0, []
    pct, rvol = kc["pct"], kc["rvol"]
    if rvol > CLIMAX_RVOL_THRESHOLD: return -1, ["⚠️ Аномальный RVOL (возможен слив)"], False
    if tf_key == "impulse":
        if pct >= 8.0: score += 30; breakdown.append(f"Импульс +{pct:.1f}% (+30)")
        elif pct >= 4.0: score += 20; breakdown.append(f"Импульс +{pct:.1f}% (+20)")
        if rvol >= 4.0: score += 25; breakdown.append(f"RVOL {rvol:.1f}x (+25)")
        elif rvol >= 2.0: score += 15; breakdown.append(f"RVOL {rvol:.1f}x (+15)")
    else:
        if pct >= 25.0: score += 30; breakdown.append(f"Разгон +{pct:.1f}% (+30)")
        elif pct >= 14.0: score += 20; breakdown.append(f"Разгон +{pct:.1f}% (+20)")
        if rvol >= 3.0: score += 25; breakdown.append(f"RVOL {rvol:.1f}x (+25)")
        elif rvol >= 1.8: score += 15; breakdown.append(f"RVOL {rvol:.1f}x (+15)")
    if oi_delta >= 15.0: score += 25; breakdown.append(f"OI +{oi_delta:.1f}% (+25)")
    elif oi_delta >= 8.0: score += 15; breakdown.append(f"OI +{oi_delta:.1f}% (+15)")
    is_squeeze = oi_delta < -3.0
    if is_squeeze: score += 15; breakdown.append(f"Шорт-сквиз OI ({oi_delta:.1f}%) (+15)")
    if funding <= FUNDING_EXTREME_NEG: score += 20; breakdown.append(f"Фандинг экстремальный {funding*100:.4f}% (+20)")
    elif funding <= FUNDING_MILD_NEG: score += 10; breakdown.append(f"Фандинг отрицательный {funding*100:.4f}% (+10)")
    return score, breakdown, is_squeeze

def calc_accumulation(base, candles):
    lb = ACCUM_WINDOW_HOURS * 4
    if len(candles) < lb + 10: return None
    recent = candles[-lb:]
    high, low = max(c["high"] for c in recent), min(c["low"] for c in recent)
    if low <= 0: return None
    move_pct = ((high - low) / low) * 100
    if move_pct > ACCUM_MAX_PRICE_MOVE_PCT: return None
    oi_growth = oi_growth_pct(base, ACCUM_WINDOW_HOURS)
    if oi_growth < ACCUM_MIN_OI_GROWTH_PCT: return None
    eff = efficiency_ratio(candles, lb)
    if eff > ACCUM_MIN_EFFICIENCY_INVERSE: return None
    return {"move_pct": move_pct, "oi_growth": oi_growth, "eff": eff}

# ============================================================
# TELEGRAM & MESSAGES
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or not SESSION: return
    try:
        async with SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}) as r: pass
    except Exception: pass

async def send_pump_signal(base, tf_key, kc, bg, bb, oi_delta, change24, score, breakdown, is_squeeze, funding, is_watch):
    cfg = TIMEFRAMES[tf_key]
    badge = "👀 НАБЛЮДЕНИЕ" if is_watch else f"🚨 СИГНАЛ [{cfg['label']}]"
    if is_squeeze: badge = f"🔥 СКВИЗ [{cfg['label']}]"
    oi_str = f"Bybit (Live): <b>{oi_delta:+.1f}%</b>"
    breakdown_str = "\n".join([f"• {b}" for b in breakdown])
    msg = (f"<b>{badge}</b>\n\n📌 <b>{escape(base)}USDT</b>\n📈 Движение: <b>+{kc['pct']:.2f}%</b> (24h: {change24:+.1f}%)\n⚡ RVOL: <b>{kc['rvol']:.1f}x</b>\n🎯 Score: <b>{score:.0f} / 100</b>\n\n📊 <b>Открытый интерес (OI):</b>\n{oi_str}\n\n💰 <b>Фандинг:</b> <code>{funding*100:.4f}%</code>\n\n🔍 <b>Детали скоринга:</b>\n{breakdown_str}")
    if is_watch: STATS["watch_signals"] += 1
    else:
        STATS["signals"] += 1
        PUMP_TRACKS[base] = {"start_price": kc["end_price"], "start_rvol": kc["rvol"], "start_ts": time.time()}
    await send_tg(msg)

async def send_accumulation_alert(base, acc):
    msg = (f"📦 <b>НАКОПЛЕНИЕ ПОЗИЦИИ</b>\n\n📌 <b>{escape(base)}USDT</b>\n📊 Диапазон за {ACCUM_WINDOW_HOURS}ч: <b>+{acc['move_pct']:.2f}%</b>\n📈 Рост OI (Live): <b>+{acc['oi_growth']:.1f}%</b>\n⚙️ Эффективность движения: <b>{acc['eff']:.2f}</b>")
    STATS["accum_alerts"] += 1
    await send_tg(msg)

async def send_spark_alert(base, spark, bg_spark, oi_delta):
    msg = (f"⚡ <b>СПАРК-ИМПУЛЬС (15м)</b>\n\n📌 <b>{escape(base)}USDT</b>\n🚀 Выстрел: <b>+{spark['pct']:.2f}%</b> (RVOL: {spark['rvol']:.1f}x)\n📊 OI (Live): <b>{oi_delta:+.1f}%</b>")
    STATS["spark_alerts"] += 1
    await send_tg(msg)

async def send_climax_alert(base, kc, oi_delta, funding):
    msg = (f"🥊 <b>КУЛЬМИНАЦИЯ / СЛИВ КИТА!</b>\n\n📌 <b>{escape(base)}USDT</b>\n⚠️ <b>Аномальный объем или падение OI!</b>\n\n📈 Движение: <b>+{kc['pct']:.2f}%</b>\n⚡ Аномальный RVOL: <b>{kc['rvol']:.1f}x</b>\n📊 OI (Live): <b>{oi_delta:+.1f}%</b>\n💰 Фандинг: <code>{funding*100:.4f}%</code>\n\n🚨 <i>ВЫХОДИ ИЗ ЛОНГА. Возможен резкий дамп!</i>")
    STATS["climax_alerts"] += 1
    await send_tg(msg)

async def handle_telegram_update(update):
    msg = update.get("message")
    if not msg or str(msg.get("chat", {}).get("id", "")) != str(CHAT_ID): return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/status") or text.startswith("/health"):
        uptime = (time.time() - START_TIME) / 3600
        await send_tg(f"📊 <b>PUMP-HUNTER v12.0</b>\n⏱ Uptime: <b>{uptime:.2f}h</b>\n🌐 Universe: <b>{len(UNIVERSE)}</b> | Tracking: <b>{len(PUMP_TRACKS)}</b>\n⚡ WS Bybit: <b>{STATS['ws_ticks_bybit']}</b> | WS Gate: <b>{STATS['ws_ticks_gate']}</b>\n📊 OI Bybit: <b>{STATS['ws_oi_bybit']}</b> | OI Gate: <b>{STATS['ws_oi_gate']}</b>\n🚨 Signals: <b>{STATS['signals']}</b> | Climax: <b>{STATS['climax_alerts']}</b>")

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
# UNIVERSE & SCAN LOOPS
# ============================================================

async def refresh_universe():
    global UNIVERSE
    bg, gate = await asyncio.gather(fetch_bitget_contracts(), fetch_gate_tickers(), return_exceptions=True)
    
    if isinstance(bg, Exception): bg = {}
    if isinstance(gate, Exception): gate = {}
    
    combined = {}
    if bg: combined.update(bg)
    if gate: combined.update(gate)
    
    if not combined: return False
        
    valid_coins = {}
    for base, info in combined.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT or not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT): continue
        valid_coins[base] = {"okx_symbol": f"{base}-USDT-SWAP", "bitget_symbol": f"{base}USDT", "volume24": info["volume24"], "change24": info["change24"]}
    
    if not valid_coins: return False
    
    sorted_by_vol = sorted(valid_coins.items(), key=lambda x: x[1]["volume24"], reverse=True)
    top_volume = dict(sorted_by_vol[:MAX_UNIVERSE_SYMBOLS - TOP_GAINERS_COUNT])
    
    remaining = {k: v for k, v in valid_coins.items() if k not in top_volume}
    sorted_by_gain = sorted(remaining.items(), key=lambda x: x[1]["change24"], reverse=True)
    top_gainers = dict(sorted_by_gain[:TOP_GAINERS_COUNT])
    
    UNIVERSE = {**top_volume, **top_gainers}
    log.info("🌐 Юниверс обновлен: %d пар", len(UNIVERSE))
    gc.collect()
    return True

async def universe_loop():
    while True:
        try: await asyncio.sleep(UNIVERSE_REFRESH_SEC); await refresh_universe()
        except asyncio.CancelledError: raise
        except Exception: await asyncio.sleep(30)

async def process_symbol(base):
    now = time.time()
    if base in PUMP_TRACKS:
        track = PUMP_TRACKS[base]
        candles_15 = list(CANDLES[base]["15"])
        if len(candles_15) > 5:
            kc = calc_tf_metrics(candles_15, TIMEFRAMES["impulse"])
            if kc:
                oi_delta = oi_growth_pct(base, 0.25)
                funding_rate, _ = await fetch_best_funding(base, UNIVERSE.get(base, {}).get("okx_symbol", f"{base}-USDT-SWAP"))
                is_dump = (kc["rvol"] >= CLIMAX_RVOL_THRESHOLD) or (oi_delta <= CLIMAX_OI_DROP_PCT and kc["pct"] > 5.0)
                if is_dump and now - LAST_CLIMAX_SIGNAL.get(base, 0) > CLIMAX_COOLDOWN_SEC:
                    LAST_CLIMAX_SIGNAL[base] = now; await send_climax_alert(base, kc, oi_delta, funding_rate); del PUMP_TRACKS[base]; return
                if kc["end_price"] < track["start_price"] * 0.95: del PUMP_TRACKS[base]; return
    item = UNIVERSE.get(base)
    if not item or base in PUMP_TRACKS or now - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC or now - LAST_REJECT.get(base, 0) < REJECT_COOLDOWN_SEC: return
    best = None
    for tf_key, tf_cfg in TIMEFRAMES.items():
        tf_map = "15" if tf_key == "impulse" else "60"
        candles = list(CANDLES[base][tf_map])
        if not candles: continue
        if tf_key == "grind" and ACCUM_ENABLED and now - ACCUM_LAST_SIGNAL.get(base, 0) >= ACCUM_COOLDOWN_SEC:
            acc = calc_accumulation(base, candles)
            if acc: ACCUM_LAST_SIGNAL[base] = time.time(); await send_accumulation_alert(base, acc)
        if tf_key == "impulse" and SPARK_ENABLED and now - LAST_SPARK_SIGNAL.get(base, 0) >= SPARK_COOLDOWN_SEC:
            spark = calc_spark_metrics(candles, is_live=True)
            if spark and spark["breakout"] and spark["pct"] >= SPARK_MIN_PCT and spark["rvol"] >= SPARK_MIN_RVOL:
                bg_candles = await fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"])
                bg_spark = calc_spark_metrics(bg_candles, is_live=False)
                if bg_spark and bg_spark["pct"] >= SPARK_MIN_PCT * SPARK_CONFIRM_RATIO:
                    oi_delta_spark = oi_growth_pct(base, 1.0); LAST_SPARK_SIGNAL[base] = now; await send_spark_alert(base, spark, bg_spark, oi_delta_spark)
        kc = calc_tf_metrics(candles, tf_cfg)
        if not kc or kc["pct"] < tf_cfg["min_pct"] * 0.5: continue
        if not kc["breakout"]: STATS["rejected_no_breakout"] += 1; continue
        current_live = candles[-1]
        if current_live["open"] > 0:
            live_pct = ((current_live["close"] - current_live["open"]) / current_live["open"]) * 100
            if live_pct > ANTI_HIGH_MAX_PCT: STATS["rejected_anti_high"] += 1; LAST_REJECT[base] = time.time(); continue
        bg_candles = await fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"])
        bb_candles = list(CANDLES[base][tf_map])
        bg = calc_tf_metrics(bg_candles, tf_cfg); bb = calc_tf_metrics(bb_candles, tf_cfg)
        confirm_threshold = tf_cfg["min_pct"] * CONFIRM_PCT_RATIO
        confirmations = sum(1 for m in (bg, bb) if m and m["pct"] >= confirm_threshold)
        if confirmations < MIN_CONFIRMATIONS: STATS["rejected_no_confirm"] += 1; LAST_REJECT[base] = time.time(); continue
        STATS["triggers"] += 1
        oi_delta = oi_growth_pct(base, tf_cfg["oi_window_hours"])
        funding_rate, _ = await fetch_best_funding(base, item["okx_symbol"])
        score, breakdown, is_squeeze = score_signal(tf_key, kc, oi_delta, funding_rate)
        if score == -1: LAST_REJECT[base] = time.time(); continue
        if best is None or score > best[0]: best = (score, tf_key, kc, oi_delta, bg, bb, is_squeeze, breakdown, funding_rate)
    if best is None: return
    score, tf_key, kc, oi_delta, bg, bb, is_squeeze, breakdown, funding_rate = best
    if score < SCORE_WATCH_THRESHOLD: STATS["rejected_low_score"] += 1; LAST_REJECT[base] = time.time(); return
    is_watch = score < SCORE_SIGNAL_THRESHOLD
    LAST_SIGNAL[base] = time.time()
    await send_pump_signal(base, tf_key, kc, bg, bb, oi_delta, item.get("change24", 0), score, breakdown, is_squeeze, funding_rate, is_watch)

async def scan_loop():
    while True:
        try:
            active_bases = set(UNIVERSE.keys()) | set(PUMP_TRACKS.keys())
            if active_bases:
                STATS["scans"] += 1
                sem = asyncio.Semaphore(10)
                async def worker(b):
                    async with sem: await process_symbol(b)
                await asyncio.gather(*[worker(b) for b in active_bases])
                gc.collect()
        except Exception: pass
        await asyncio.sleep(SCAN_INTERVAL_SEC)

# ============================================================
# WEB & STARTUP
# ============================================================

async def index(req): return web.Response(text=f"v12.0 | Uni: {len(UNIVERSE)} | Track: {len(PUMP_TRACKS)} | Bybit WS: {STATS['ws_ticks_bybit']} | Gate WS: {STATS['ws_ticks_gate']} | Signals: {STATS['signals']}")

async def start(app):
    global SESSION, HTTP_SEMAPHORE
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30))
    HTTP_SEMAPHORE = asyncio.Semaphore(10)
    load_state()
    if BOT_TOKEN: await SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", params={"drop_pending_updates": "true"})
    
    log.info("⏳ Загрузка первичного юниверса...")
    while not UNIVERSE:
        if not await refresh_universe():
            log.warning("⚠️ Юниверс пуст, повтор через 10 секунд...")
            await asyncio.sleep(10)
            
    await bootstrap_candles()
    
    WS_TASKS["bybit"] = asyncio.create_task(bybit_ws_loop()); app["ws_bybit_task"] = WS_TASKS["bybit"]
    WS_TASKS["gate"] = asyncio.create_task(gate_ws_loop()); app["ws_gate_task"] = WS_TASKS["gate"]
    app["universe_task"] = asyncio.create_task(universe_loop())
    app["oi_task"] = asyncio.create_task(live_oi_sampling_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["poll_task"] = asyncio.create_task(telegram_poll_loop())
    app["persist_task"] = asyncio.create_task(persist_loop())
    await send_tg("🚀 <b>PUMP-HUNTER v12.0 запущен!</b>")

async def stop(app):
    for key in app: 
        if key.endswith("_task"): app[key].cancel()
    save_state()
    if SESSION and not SESSION.closed: await SESSION.close()

app = web.Application()
app.router.add_get("/", index)
app.on_startup.append(start)
app.on_cleanup.append(stop)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
