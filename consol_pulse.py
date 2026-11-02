import asyncio
import aiohttp
from aiohttp import web
import os
import time
import json
import gc
import logging
from collections import defaultdict, deque
from html import escape

# ============================================================
# ПАМП-ХАНТЕР v16.3 — Sort Fix + Fast Path
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "60"))
OI_SAMPLE_INTERVAL_SEC = int(os.environ.get("OI_SAMPLE_INTERVAL_SEC", "900"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))
PERSIST_INTERVAL_SEC = int(os.environ.get("PERSIST_INTERVAL_SEC", "300"))
PERSIST_PATH = os.environ.get("PERSIST_PATH", "/tmp/pump_hunter_state.json")

# --- Юниверс ---
MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "250"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "250"))
MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "80000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.0001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "10.0"))
MIN_LISTING_AGE_DAYS = float(os.environ.get("MIN_LISTING_AGE_DAYS", "14"))

# --- Fast Path ---
FAST_SIGNAL_RVOL = float(os.environ.get("FAST_SIGNAL_RVOL", "4.0"))
FAST_SIGNAL_PCT = float(os.environ.get("FAST_SIGNAL_PCT", "6.0"))
CONFIRM_TIMEOUT_SEC = float(os.environ.get("CONFIRM_TIMEOUT_SEC", "3.0"))

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
SPARK_BASE_CANDLES = int(os.environ.get("SPARK_BASE_CANDLES", "96"))
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

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v16.3")

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

KUCOIN_STATUS = {"is_down": False, "last_check": 0}

OI_HIST_MAXLEN = int(14 * 3600 / OI_SAMPLE_INTERVAL_SEC) + 10
OI_HISTORY = defaultdict(lambda: defaultdict(lambda: deque(maxlen=OI_HIST_MAXLEN)))

STATS = {
    "scans": 0, "oi_samples": 0, "triggers": 0, "signals": 0, "fast_signals": 0,
    "watch_signals": 0, "accum_alerts": 0, "spark_alerts": 0, "climax_alerts": 0,
    "rejected_no_breakout": 0, "rejected_no_confirm": 0, "rejected_low_score": 0, "rejected_anti_high": 0,
    "state_loaded": False, "state_saved_count": 0,
    "kucoin_candles": 0, "bitget_candles": 0,
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

def save_state():
    try:
        data = {
            "oi_history": {base: {exch: list(dq) for exch, dq in exch_map.items()} for base, exch_map in OI_HISTORY.items()},
            "last_signal": LAST_SIGNAL, "accum_last_signal": ACCUM_LAST_SIGNAL,
            "last_climax_signal": LAST_CLIMAX_SIGNAL, "pump_tracks": PUMP_TRACKS, "saved_at": time.time(),
        }
        with open(PERSIST_PATH, "w") as f: json.dump(data, f)
        STATS["state_saved_count"] += 1
    except Exception: pass

def load_state():
    if not os.path.exists(PERSIST_PATH): return
    try:
        with open(PERSIST_PATH, "r") as f: data = json.load(f)
        for base, exch_map in data.get("oi_history", {}).items():
            for exch, points in exch_map.items():
                for ts, val in points: OI_HISTORY[base][exch].append((ts, val))
        LAST_SIGNAL.update(data.get("last_signal", {}))
        ACCUM_LAST_SIGNAL.update(data.get("accum_last_signal", {}))
        LAST_CLIMAX_SIGNAL.update(data.get("last_climax_signal", {}))
        PUMP_TRACKS.update(data.get("pump_tracks", {}))
        STATS["state_loaded"] = True
    except Exception: pass

async def persist_loop():
    while True:
        try: await asyncio.sleep(PERSIST_INTERVAL_SEC); save_state()
        except asyncio.CancelledError: raise
        except Exception: pass

# ============================================================
# FETCHERS
# ============================================================

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

async def fetch_candles(base, item, granularity_min, limit):
    if not KUCOIN_STATUS["is_down"] or time.time() - KUCOIN_STATUS["last_check"] > 600:
        kc_sym = item.get("symbol")
        if kc_sym:
            now_ms = int(time.time() * 1000)
            from_ms = now_ms - limit * granularity_min * 60 * 1000
            data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {"symbol": kc_sym, "granularity": str(granularity_min), "from": from_ms, "to": now_ms})
            if data and isinstance(data.get("data"), list):
                out = []
                for r in data["data"]:
                    # KuCoin Futures: [ts, open, high, low, close, volume]
                    out.append({"ts": int(r[0]), "open": num(r[1]), "high": num(r[2]), "low": num(r[3]), "close": num(r[4]), "volume": num(r[5])})
                
                # 🚀 КРИТИЧЕСКИЙ ФИКС: Сортируем по времени!
                out.sort(key=lambda x: x["ts"])
                
                STATS["kucoin_candles"] += 1
                return out, "KUCOIN"
    
    candles = await fetch_bitget_candles(item["bitget_symbol"], granularity_min, limit)
    STATS["bitget_candles"] += 1
    candles.sort(key=lambda x: x["ts"]) # Тоже сортируем на всякий случай
    return candles, "BITGET"

async def fetch_bitget_candles(symbol, gran_min, limit):
    gran = "15m" if gran_min == 15 else "1H"
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {"symbol": symbol, "productType": "USDT-FUTURES", "granularity": gran, "limit": str(limit)})
    if not data or data.get("code") != "00000": return []
    out = []
    for r in reversed(data.get("data", [])):
        # Bitget: [ts, open, high, low, close, volume]
        out.append({"ts": int(r[0]), "open": num(r[1]), "high": num(r[2]), "low": num(r[3]), "close": num(r[4]), "volume": num(r[5])})
    return out

async def fetch_bybit_candles(base, gran_min, limit):
    interval = "60" if gran_min == 60 else str(gran_min)
    data = await http_get(f"{BYBIT_BASE}/v5/market/kline", {"category": "linear", "symbol": f"{base}USDT", "interval": interval, "limit": limit})
    if not data or data.get("retCode") != 0: return []
    out = []
    for r in reversed(data.get("result", {}).get("list", [])):
        # Bybit: [ts, open, high, low, close, volume]
        out.append({"ts": int(r[0]), "open": num(r[1]), "high": num(r[2]), "low": num(r[3]), "close": num(r[4]), "volume": num(r[5])})
    return out

# ============================================================
# OI & FUNDING
# ============================================================

async def fetch_kucoin_oi(symbol):
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}")
    if data and isinstance(data.get("data"), dict): return num(data["data"].get("openInterest"))
    return 0.0

async def fetch_bitget_oi(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {"symbol": symbol, "productType": "USDT-FUTURES"})
    if not data or data.get("code") != "00000": return 0.0
    raw = data.get("data", {})
    row = raw.get("openList", [{}])[0] if isinstance(raw.get("openList"), list) else raw
    return num(row.get("openInterest") or row.get("amount"))

async def fetch_bybit_oi(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/open-interest", {"category": "linear", "symbol": f"{base}USDT", "intervalTime": "15min", "limit": 1})
    if not data or data.get("retCode") != 0: return 0.0
    lst = data.get("result", {}).get("list", [])
    if lst: return num(lst[0].get("openInterest"))
    return 0.0

async def fetch_best_funding(base, kc_sym):
    d = await http_get(f"{KUCOIN_BASE}/api/v1/funding-rate/{kc_sym}/current")
    if d and isinstance(d.get("data"), dict) and "value" in d["data"]:
        return num(d["data"]["value"])
    d = await http_get(f"{BYBIT_BASE}/v5/market/tickers", {"category": "linear", "symbol": f"{base}USDT"})
    if d and d.get("retCode") == 0:
        lst = d.get("result", {}).get("list", [])
        if lst: return num(lst[0].get("fundingRate"))
    return 0.0

# ============================================================
# МЕТРИКИ & АНАЛИТИКА
# ============================================================

def efficiency_ratio(candles, idx_now, window):
    start = max(0, idx_now - window + 1)
    closes = [c["close"] for c in candles[start:idx_now + 1]]
    if len(closes) < 3: return 0.0
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
    base_slice = candles[max(0, idx_past - base_n):idx_past]
    if not base_slice: return None
    base_high = max(c["close"] for c in base_slice)
    breakout = close_now > base_high * BREAKOUT_TOLERANCE
    vol_now = candles[idx_now]["volume"] * candles[idx_now]["close"]
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = vol_now / avg_vol if avg_vol > 0 else 0
    return {"close": close_now, "pct": pct, "breakout": breakout, "base_high": base_high, "rvol": rvol, "efficiency": efficiency_ratio(candles, idx_now, lookback)}

def calc_spark_metrics(candles):
    if len(candles) < SPARK_BASE_CANDLES + 3: return None
    idx_now = len(candles) - 1
    current = candles[idx_now]
    if current["open"] <= 0: return None
    elapsed_min = (time.time() * 1000 - current["ts"]) / 1000 / 60
    if elapsed_min < SPARK_MIN_ELAPSED_MIN: return None
    pct = ((current["close"] / current["open"]) - 1) * 100
    base_slice = candles[max(0, idx_now - SPARK_BASE_CANDLES):idx_now]
    if not base_slice: return None
    base_high = max(c["close"] for c in base_slice)
    breakout = current["close"] > base_high * BREAKOUT_TOLERANCE
    vol_usd = current["volume"] * current["close"]
    normalized_vol = vol_usd / min(elapsed_min / 15.0, 1.0)
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
        oi_deltas[exch] = d if d is not None else 0.0
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
    msg = (f"🎯 <b>НАКОПЛЕНИЕ [{source}]: {escape(base)}USDT</b>\n\nЦена изменилась на {acc['price_change_pct']:.1f}% за {ACCUM_WINDOW_HOURS}ч (efficiency {acc['efficiency']:.2f}),\nно OI вырос до +{acc['max_delta']:.1f}%.\n\n<b>📊 OI Δ</b>\n  KuCoin: {acc['oi_deltas'].get('kucoin', 0):+.1f}%\n  Bitget: {acc['oi_deltas'].get('bitget', 0):+.1f}%\n  Bybit:  {acc['oi_deltas'].get('bybit', 0):+.1f}%\n\nТекущая цена: <b>{acc['close']:.6g}</b>")
    if await send_tg(msg): STATS["accum_alerts"] += 1

async def send_pump_signal(base, tf_key, kc, bg, bb, oi_deltas, change_24h, score, breakdown, is_squeeze, funding_rate, is_watch, source, is_fast=False):
    tf_cfg = TIMEFRAMES[tf_key]
    tier_emoji = "👀" if is_watch else ("🚀" if is_fast else tf_cfg["emoji"])
    tier_label = "НАБЛЮДЕНИЕ" if is_watch else ("FAST SIGNAL" if is_fast else tf_cfg["label"])
    
    conf_lines = []
    if bg: conf_lines.append(f"  Bitget: +{bg['pct']:.1f}% | RVOL {bg['rvol']:.1f}x")
    if bb: conf_lines.append(f"  Bybit:  +{bb['pct']:.1f}% | RVOL {bb['rvol']:.1f}x")
    if not conf_lines: conf_lines.append("  ожидание ответа...")
    
    funding_line = f"\nFunding: {funding_rate*100:+.3f}%" + (" ⚠️ экстремальный" if funding_rate <= FUNDING_EXTREME_NEG else "") if funding_rate is not None else ""
    
    msg = (f"{tier_emoji} <b>{tier_label}: {escape(base)}USDT</b>  [score {score:.0f}]\n\n"
           f"<b>🎯 {source}</b>\n  Рост: <b>+{kc['pct']:.1f}%</b>\n  Пробой базы: <b>{kc['base_high']:.6g} → {kc['close']:.6g}</b>\n  RVOL: <b>{kc['rvol']:.2f}x</b> | Efficiency: {kc['efficiency']:.2f}\n\n"
           f"<b>✅ Подтверждение</b>\n" + ("\n".join(conf_lines)) + "\n\n"
           f"<b>📊 OI Δ</b>\n  KuCoin: {oi_deltas.get('kucoin') or 0:+.1f}%\n  Bitget: {oi_deltas.get('bitget') or 0:+.1f}%\n  Bybit:  {oi_deltas.get('bybit') or 0:+.1f}%{funding_line}\n\n"
           f"<b>⛔ Инвалидация:</b> закрытие ниже {kc['base_high'] * BREAKOUT_TOLERANCE:.6g}\n"
           f"<b>24h change:</b> {change_24h:+.1f}%\n\n"
           f"<i>Score: моментум {breakdown['momentum']:.0f} + объём {breakdown['volume']:.0f} + OI {breakdown['oi']:.0f} + funding {breakdown['funding']:.0f} {breakdown['chop_penalty']:+.0f} (шум)</i>")
    if is_squeeze: msg += "\n⚠️ Часть роста OI отрицательна — похоже на шорт-сквиз."
    if kc["rvol"] >= 8.0: msg += "\n⚠️ Аномально высокий RVOL — риск резкого разворота."

    if await send_tg(msg):
        if is_watch: STATS["watch_signals"] += 1
        else:
            STATS["signals"] += 1
            if is_fast: STATS["fast_signals"] += 1
            PUMP_TRACKS[base] = {"start_ts": time.time(), "entry": kc["close"], "tf": tf_key, "source": source}

async def send_spark_alert(base, spark, bg_spark, oi_deltas, source):
    def _oi(val):
        if val is None: return "нет данных"
        return f"{val:+.1f}%" + (" (шорт-сквиз)" if val <= -OI_MIN_GROWTH_PCT else "")
    bg_line = f"  Bitget: +{bg_spark['pct']:.1f}%" if bg_spark else "  нет данных"
    msg = (f"✨ <b>СПАРК [{source}]: {escape(base)}USDT</b>\n\nДвижение только началось — ~{spark['elapsed_min']:.0f} мин назад.\nРост: <b>+{spark['pct']:.1f}%</b> | RVOL: <b>{spark['rvol']:.1f}x</b>\nПробой: {spark['base_high']:.6g} → {spark['close']:.6g}\n\n<b>Подтверждение (Bitget)</b>\n{bg_line}\n\n<b>📊 OI</b>\n  KuCoin: {_oi(oi_deltas.get('kucoin'))}\n  Bitget: {_oi(oi_deltas.get('bitget'))}\n  Bybit:  {_oi(oi_deltas.get('bybit'))}\n\n⚠️ Самая ранняя и самая шумная стадия — ниже уверенность. Цель — скорость.")
    if await send_tg(msg): STATS["spark_alerts"] += 1

async def send_climax_alert(base, spark, oi_bb):
    msg = (f"🥊 <b>КУЛЬМИНАЦИЯ / СЛИВ КИТА!</b>\n\n📌 <b>{escape(base)}USDT</b>\n⚠️ <b>Аномальный объем или падение OI!</b>\n\n📈 Движение: <b>+{spark['pct']:.2f}%</b>\n⚡ Аномальный RVOL: <b>{spark['rvol']:.1f}x</b>\n📊 OI (Bybit): <b>{oi_bb:+.1f}%</b>\n\n🚨 <i>ВЫХОДИ ИЗ ЛОНГА. Возможен резкий дамп!</i>")
    if await send_tg(msg): STATS["climax_alerts"] += 1

async def handle_telegram_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg or (CHAT_ID and str(msg.get("chat", {}).get("id", "")) != str(CHAT_ID)): return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/status") or text.startswith("/health"):
        await send_tg(f"📊 <b>PUMP-HUNTER v16.3 (Sort Fix)</b>\n⏱ Аптайм: {(time.time()-START_TIME)/3600:.1f}ч\n🌐 Юниверс: {len(UNIVERSE)} пар\n📊 Свечи: KuCoin {STATS['kucoin_candles']} | Bitget {STATS['bitget_candles']}\n✅ Сигналов: {STATS['signals']} (Fast: {STATS['fast_signals']})\n👀 Наблюдений: {STATS['watch_signals']}\n🎯 Накоплений: {STATS['accum_alerts']} | ✨ Спарков: {STATS['spark_alerts']} | 🥊 Сливов: {STATS['climax_alerts']}\n📍 На трекинге: {len(PUMP_TRACKS)}")

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
    candles, _ = await fetch_candles(base, item, 15, 20)
    spark = calc_spark_metrics(candles)
    if spark:
        oi_bb = oi_growth_pct(base, "bybit", 0.25) or 0.0
        is_dump = (spark["rvol"] >= CLIMAX_RVOL_THRESHOLD) or (oi_bb <= CLIMAX_OI_DROP_PCT and spark["pct"] > 5.0)
        if is_dump and time.time() - LAST_CLIMAX_SIGNAL.get(base, 0) > CLIMAX_COOLDOWN_SEC:
            LAST_CLIMAX_SIGNAL[base] = time.time()
            await send_climax_alert(base, spark, oi_bb)
            del PUMP_TRACKS[base]
            return True
    track = PUMP_TRACKS[base]
    if spark and spark["close"] < track["entry"] * 0.95:
        del PUMP_TRACKS[base]
    return False

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item: return
    now = time.time()

    if base in PUMP_TRACKS:
        await check_climax(base, item)
        return

    if now - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC or now - LAST_REJECT.get(base, 0) < REJECT_COOLDOWN_SEC: return

    best = None
    for tf_key, tf_cfg in TIMEFRAMES.items():
        candles, source = await fetch_candles(base, item, tf_cfg["granularity_min"], tf_cfg["fetch_limit"])
        if not candles: continue

        if tf_key == "grind" and ACCUM_ENABLED and now - ACCUM_LAST_SIGNAL.get(base, 0) >= ACCUM_COOLDOWN_SEC:
            acc = calc_accumulation(base, candles)
            if acc: ACCUM_LAST_SIGNAL[base] = time.time(); await send_accumulation_alert(base, acc, source)

        if tf_key == "impulse" and SPARK_ENABLED and now - LAST_SPARK_SIGNAL.get(base, 0) >= SPARK_COOLDOWN_SEC:
            spark = calc_spark_metrics(candles)
            if spark and spark["breakout"] and spark["pct"] >= SPARK_MIN_PCT and spark["rvol"] >= SPARK_MIN_RVOL:
                bg_spark = calc_spark_metrics(await fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"]))
                if bg_spark and bg_spark["pct"] >= SPARK_MIN_PCT * SPARK_CONFIRM_RATIO:
                    oi_spark = {exch: oi_growth_pct(base, exch, 1.0) for exch in ("kucoin", "bitget", "bybit")}
                    LAST_SPARK_SIGNAL[base] = now; await send_spark_alert(base, spark, bg_spark, oi_spark, source)

        kc = calc_tf_metrics(candles, tf_cfg)
        if not kc or kc["pct"] < tf_cfg["min_pct"] * 0.5: continue
        
        # Анти-Хай
        live_candles, _ = await fetch_candles(base, item, 15, 2)
        if live_candles:
            live = live_candles[-1]
            if live["open"] > 0 and ((live["close"] - live["open"]) / live["open"]) * 100 > ANTI_HIGH_MAX_PCT:
                STATS["rejected_anti_high"] += 1; LAST_REJECT[base] = time.time(); continue

        if not kc["breakout"]: STATS["rejected_no_breakout"] += 1; continue

        # 🚀 FAST PATH
        is_fast_trigger = (kc["rvol"] >= FAST_SIGNAL_RVOL and kc["pct"] >= FAST_SIGNAL_PCT)
        
        if is_fast_trigger:
            STATS["triggers"] += 1
            oi_deltas = {exch: oi_growth_pct(base, exch, tf_cfg["oi_window_hours"]) for exch in ("kucoin", "bitget", "bybit")}
            funding = 0.0
            score, breakdown, best_oi, is_sq = score_signal(tf_key, kc, oi_deltas, funding)
            
            if best is None or score > best[0]:
                best = (score, tf_key, kc, oi_deltas, None, None, is_sq, breakdown, funding, source, True)
            break

        # 🐢 ОБЫЧНЫЙ ПУТЬ
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"]),
                    fetch_bybit_candles(base, tf_cfg["granularity_min"], tf_cfg["fetch_limit"]),
                    fetch_best_funding(base, item["symbol"])
                ),
                timeout=CONFIRM_TIMEOUT_SEC
            )
            candles_bg, candles_bb, funding = results
            bg = calc_tf_metrics(candles_bg, tf_cfg)
            bb = calc_tf_metrics(candles_bb, tf_cfg)
        except asyncio.TimeoutError:
            bg, bb, funding = None, None, 0.0
        
        confirm_threshold = tf_cfg["min_pct"] * CONFIRM_PCT_RATIO
        if sum(1 for m in (bg, bb) if m and m["pct"] >= confirm_threshold) < MIN_CONFIRMATIONS and not is_fast_trigger:
            STATS["rejected_no_confirm"] += 1; LAST_REJECT[base] = time.time(); continue

        STATS["triggers"] += 1
        oi_deltas = {exch: oi_growth_pct(base, exch, tf_cfg["oi_window_hours"]) for exch in ("kucoin", "bitget", "bybit")}
        score, breakdown, best_oi, is_sq = score_signal(tf_key, kc, oi_deltas, funding)

        if best is None or score > best[0]:
            best = (score, tf_key, kc, oi_deltas, bg, bb, is_sq, breakdown, funding, source, False)

    if best:
        score, tf_key, kc, oi_deltas, bg, bb, is_sq, breakdown, funding, source, is_fast = best
        if score < SCORE_WATCH_THRESHOLD:
            STATS["rejected_low_score"] += 1; LAST_REJECT[base] = time.time(); return
            
        is_watch = score < SCORE_SIGNAL_THRESHOLD
        LAST_SIGNAL[base] = time.time()
        await send_pump_signal(base, tf_key, kc, bg, bb, oi_deltas, item.get("change24", 0), score, breakdown, is_sq, funding, is_watch, source, is_fast)

# ============================================================
# BACKGROUND LOOPS
# ============================================================

async def sample_oi_for_symbol(base):
    item = UNIVERSE.get(base)
    if not item: return
    now = time.time()
    kc_oi, bg_oi, bb_oi = await asyncio.gather(fetch_kucoin_oi(item["symbol"]), fetch_bitget_oi(item["bitget_symbol"]), fetch_bybit_oi(base))
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
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    
    if not kc:
        log.warning("⚠️ KuCoin недоступен. Используем Bitget...")
        kc = bg
    if not kc: return

    bg_set = set(bg.keys()) if bg else set(kc.keys())
    now_ms = time.time() * 1000
    min_age_ms = MIN_LISTING_AGE_DAYS * 86400 * 1000
    
    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT or not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT): continue
        if base not in bg_set: continue
        if info.get("first_open_ms", 0) > 0 and (now_ms - info["first_open_ms"]) < min_age_ms: continue
        uni[base] = {"symbol": info.get("symbol", f"{base}USDTM"), "bitget_symbol": f"{base}USDT", "volume24": info["volume24"], "change24": info.get("change24", 0)}
    
    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс: %d пар", len(UNIVERSE))
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
            active = list(set(UNIVERSE.keys()) | set(PUMP_TRACKS.keys()))[:MAX_SCAN_CANDIDATES]
            if active:
                STATS["scans"] += 1
                sem = asyncio.Semaphore(10)
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
    return web.Response(text=f"v16.3 | Uni: {len(UNIVERSE)} | Tracks: {len(PUMP_TRACKS)} | Signals: {STATS['signals']} | Fast: {STATS['fast_signals']}", content_type="text/plain")

async def start(app):
    global SESSION, HTTP_SEMAPHORE
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(10)
    load_state()
    if BOT_TOKEN:
        try:
            async with SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", params={"drop_pending_updates": "true"}) as r: pass
        except Exception: pass

    await refresh_universe()
    app["universe_task"] = asyncio.create_task(universe_loop())
    app["oi_task"] = asyncio.create_task(oi_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["poll_task"] = asyncio.create_task(telegram_poll_loop())
    app["persist_task"] = asyncio.create_task(persist_loop())

    await send_tg(f"🚀 <b>PUMP-HUNTER v16.3 (Sort Fix) запущен.</b>\nОшибка со временем свечей исправлена.")

async def stop(app):
    for key in ("universe_task", "oi_task", "scan_task", "poll_task", "persist_task"):
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
