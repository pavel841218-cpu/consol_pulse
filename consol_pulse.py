import asyncio
import aiohttp
from aiohttp import web, WSMsgType
import os
import time
import json
import gc
import logging
from html import escape
from collections import defaultdict, deque
from typing import Optional

# ============================================================
# PUMP-SCREENER v17.1 — TICK-LEVEL RVOL + BOOTSTRAP
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID   = os.environ.get("CHAT_ID", "")
PORT      = int(os.environ.get("PORT", "10000"))

# ─── Timeframes ───
BAR                = "5m"
BAR_SECONDS        = 300
SHELF_REFRESH_SEC  = int(os.environ.get("SHELF_REFRESH_SEC", "30"))
TICK_SCAN_SEC      = int(os.environ.get("TICK_SCAN_SEC", "2"))
UNIVERSE_REFRESH   = int(os.environ.get("UNIVERSE_REFRESH_SEC", "300"))
SIGNAL_COOLDOWN    = int(os.environ.get("SIGNAL_COOLDOWN_SEC", "1800"))
CANDLE_BUFFER_SIZE = 30
OI_POLL_INTERVAL   = int(os.environ.get("OI_POLL_INTERVAL", "300"))
VOL_SAMPLE_MAXLEN  = 400   # ~13 мин при 2-сек сэмплах

# ─── Universe ───
MIN_VOL_24H   = float(os.environ.get("MIN_24H_VOLUME_USDT", "200000"))
MIN_PRICE     = float(os.environ.get("MIN_PRICE_USDT", "0.000001"))
MAX_PRICE     = float(os.environ.get("MAX_PRICE_USDT", "50.0"))
MAX_UNIVERSE  = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "150"))
MIN_CHANGE_24H = float(os.environ.get("MIN_CHANGE_24H_PCT", "-50.0"))
MAX_CHANGE_24H = float(os.environ.get("MAX_CHANGE_24H_PCT", "200.0"))

# ─── Shelf ───
SH_MIN_CANDLES      = int(os.environ.get("SHELF_MIN_CANDLES", "6"))
SH_MAX_CANDLES      = int(os.environ.get("SHELF_MAX_CANDLES", "20"))
SH_RANGE_MIN_PCT    = float(os.environ.get("SHELF_RANGE_MIN_PCT", "0.30"))
SH_RANGE_MAX_PCT    = float(os.environ.get("SHELF_RANGE_MAX_PCT", "6.0"))
SH_MAX_NET_MOVE     = float(os.environ.get("SHELF_MAX_NET_MOVE_PCT", "2.00"))
SH_MAX_DRIFT        = float(os.environ.get("SHELF_MAX_DRIFT_PCT", "1.50"))
SH_MIN_INSIDE       = float(os.environ.get("SHELF_MIN_INSIDE_RATIO", "0.75"))
SH_MAX_DIST_HIGH    = float(os.environ.get("SHELF_MAX_DIST_FROM_HIGH_PCT", "2.0"))
SH_VOL_STABILITY    = float(os.environ.get("SHELF_VOL_STABILITY_MAX", "4.0"))
SH_MIN_AVG_VOL_USDT = float(os.environ.get("SHELF_MIN_AVG_VOL_USDT", "2000"))
SH_MAX_AGE_SEC      = int(os.environ.get("SHELF_MAX_AGE_SEC", str(6 * 3600)))

# ─── Прямой пробой ───
A_MIN_BREAKOUT_PCT = float(os.environ.get("DIRECT_BREAKOUT_MIN_PCT", "0.35"))

# ─── TICK RVOL (главное) ───
TICK_MIN_RVOL_10S  = float(os.environ.get("TICK_MIN_RVOL_10S", "3.0"))
TICK_MIN_VOL_10S   = float(os.environ.get("TICK_MIN_VOL_10S", "1500"))
TICK_MIN_PCT_10S   = float(os.environ.get("TICK_MIN_PCT_10S", "0.5"))
TICK_MIN_RVOL_30S  = float(os.environ.get("TICK_MIN_RVOL_30S", "2.0"))
TICK_MIN_VOL_30S   = float(os.environ.get("TICK_MIN_VOL_30S", "4000"))

# ─── Sweep ───
B_SWEEP_LOOKBACK       = int(os.environ.get("SWEEP_LOOKBACK", "6"))
B_SWEEP_MIN_BREAK_PCT  = float(os.environ.get("SWEEP_BREAKDOWN_MIN_PCT", "0.10"))
B_SWEEP_MAX_VOL_RATIO  = float(os.environ.get("SWEEP_MAX_VOL_RATIO", "0.45"))
B_SWEEP_MUST_CLOSE_IN  = os.environ.get("SWEEP_MUST_CLOSE_INSIDE", "1") == "1"
B_SWEEP_MAX_AGE        = int(os.environ.get("SWEEP_MAX_AGE_CANDLES", "5"))
B_EXP_MIN_VS_SWEEP     = float(os.environ.get("EXPANSION_MIN_RATIO_VS_SWEEP", "3.0"))

# ─── OI & Anti-top ───
ANTI_TOP_MAX_PCT   = float(os.environ.get("ANTI_TOP_MAX_PCT", "3.0"))
REQUIRE_OI_GROWTH  = os.environ.get("REQUIRE_OI_GROWTH", "0") == "1"

# ─── Track ───
TRK_TIMEOUT_SEC     = int(os.environ.get("TRACK_TIMEOUT_SEC", str(6 * 3600)))
TRK_HOLD_TOL_PCT    = float(os.environ.get("TRACK_HOLD_LEVEL_TOLERANCE_PCT", "0.10"))
TRK_FAKEOUT_PCT     = float(os.environ.get("TRACK_FAKEOUT_BACK_INSIDE_PCT", "0.30"))
TRK_PROFIT_ALERT    = float(os.environ.get("TRACK_PROFIT_ALERT_PCT", "2.5"))
TRK_INVALIDATE_PCT  = float(os.environ.get("TRACK_DRAWDOWN_INVALIDATE_PCT", "-2.5"))

# ─── Climax ───
CLX_MIN_RVOL      = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "3.0"))
CLX_MIN_WICK      = float(os.environ.get("CLIMAX_MIN_WICK_RATIO", "0.40"))
CLX_MIN_PROFIT    = float(os.environ.get("CLIMAX_MIN_PROFIT_PCT", "3.0"))

OKX_REST = "https://www.okx.com"
OKX_WS   = "wss://ws.okx.com:8443/ws/v5/public"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("SCREENER")

SESSION: Optional[aiohttp.ClientSession] = None
HTTP_SEM: Optional[asyncio.Semaphore] = None
START_TS = time.time()

UNIVERSE: dict[str, dict] = {}
CANDLES: dict[str, deque] = defaultdict(lambda: deque(maxlen=CANDLE_BUFFER_SIZE))
SHELVES: dict[str, dict] = {}
TRACKS: dict[str, dict] = {}
LAST_SIGNAL: dict[str, float] = {}
CT_VAL: dict[str, float] = {}
OI_HISTORY = defaultdict(lambda: deque(maxlen=12))

VOL_TRACKER: dict[str, dict] = {}

WS_TASK: Optional[asyncio.Task] = None
WS_SUBSCRIBED: set[str] = set()
WS_DESIRED: set[str] = set()

STATS = defaultdict(int)

# ═══════════════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════════════

def num(v, default=0.0) -> float:
    try: return float(v) if v is not None else default
    except (TypeError, ValueError): return default

def base_of(inst_id: str) -> str:
    if not inst_id: return ""
    s = inst_id.upper()
    if s.endswith("-USDT-SWAP"): return s[:-10]
    if s.endswith("-USDT"): return s[:-5]
    return s

async def http_get(url, params=None, timeout=8, retries=1):
    if not SESSION or SESSION.closed: return None
    for attempt in range(retries + 1):
        try:
            async with HTTP_SEM:
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status == 429:
                        STATS["http_429"] += 1
                        if attempt < retries: await asyncio.sleep(0.8 * (attempt + 1)); continue
                        return None
                    if r.status >= 400: return None
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries: await asyncio.sleep(0.3)
        except Exception: break
    return None

# ═══════════════════════════════════════════════════════════════
#  OKX REST & BOOTSTRAP
# ═══════════════════════════════════════════════════════════════

async def load_instruments():
    data = await http_get(f"{OKX_REST}/api/v5/public/instruments", params={"instType": "SWAP"}, timeout=15)
    if not data or data.get("code") != "0": return
    n = 0
    for inst in data.get("data", []):
        iid = str(inst.get("instId", "")).upper()
        if iid.endswith("-USDT-SWAP"):
            CT_VAL[iid] = num(inst.get("ctVal"), 1.0) or 1.0
            n += 1
    log.info("📐 ctVal: %d", n)

async def refresh_universe():
    global UNIVERSE
    data = await http_get(f"{OKX_REST}/api/v5/market/tickers", params={"instType": "SWAP"}, timeout=12)
    if not data or data.get("code") != "0": return

    uni = {}
    for row in data.get("data", []):
        iid = str(row.get("instId", "")).upper()
        if not iid.endswith("-USDT-SWAP"): continue
        base = base_of(iid)
        if not base: continue

        price = num(row.get("last"))
        vol_usdt = num(row.get("volCcy24h")) * price
        open24 = num(row.get("open24h"))
        change24 = ((price / open24) - 1) * 100 if open24 > 0 else 0.0

        if vol_usdt < MIN_VOL_24H: continue
        if not (MIN_PRICE <= price <= MAX_PRICE): continue
        if change24 < MIN_CHANGE_24H or change24 > MAX_CHANGE_24H: continue

        uni[base] = {"symbol": iid, "price": price, "vol24": vol_usdt, "change24": change24}

    sorted_uni = dict(sorted(uni.items(), key=lambda x: x[1]["vol24"], reverse=True)[:MAX_UNIVERSE])
    UNIVERSE = sorted_uni
    log.info("🌐 Universe: %d", len(UNIVERSE))
    gc.collect()

async def bootstrap_candles():
    """Загружает историю свечей для всех монет при старте"""
    if not UNIVERSE: return
    log.info("🕯 Загрузка истории свечей для %d монет...", len(UNIVERSE))
    sem = asyncio.Semaphore(10)
    
    async def fetch_one(base, inst_id):
        async with sem:
            try:
                data = await http_get(
                    f"{OKX_REST}/api/v5/market/candles", 
                    params={"instId": inst_id, "bar": BAR, "limit": str(CANDLE_BUFFER_SIZE)},
                    timeout=8
                )
                if data and data.get("code") == "0":
                    candles = []
                    for row in data.get("data", []):
                        candles.append({
                            "ts": int(row[0]), "open": num(row[1]), "high": num(row[2]),
                            "low": num(row[3]), "close": num(row[4]), "volume": num(row[7]), "closed": row[8] == "1"
                        })
                    candles.sort(key=lambda x: x["ts"])
                    # Заменяем буфер на исторический
                    CANDLES[base] = deque(candles, maxlen=CANDLE_BUFFER_SIZE)
            except Exception:
                pass

    await asyncio.gather(*[fetch_one(b, i["symbol"]) for b, i in UNIVERSE.items()])
    log.info("✅ История загружена")

async def fetch_okx_oi(inst_id):
    data = await http_get(f"{OKX_REST}/api/v5/public/open-interest", params={"instType": "SWAP", "instId": inst_id}, timeout=6)
    if not data or data.get("code") != "0": return 0.0
    rows = data.get("data", [])
    if rows and isinstance(rows[0], dict):
        return num(rows[0].get("oiCcy") or rows[0].get("oi"))
    return 0.0

def get_oi_trend(base):
    hist = OI_HISTORY.get(base)
    if not hist or len(hist) < 2: return 0.0
    old, new = hist[0][1], hist[-1][1]
    if old <= 0: return 0.0
    return ((new - old) / old) * 100.0

async def oi_poll_loop():
    while True:
        try:
            await asyncio.sleep(OI_POLL_INTERVAL)
            if not UNIVERSE: continue
            log.info("📊 OI sample...")
            sem = asyncio.Semaphore(10)
            async def fetch_one(base):
                async with sem:
                    item = UNIVERSE.get(base)
                    if item:
                        oi = await fetch_okx_oi(item["symbol"])
                        if oi > 0: OI_HISTORY[base].append((time.time(), oi))
            await asyncio.gather(*[fetch_one(b) for b in UNIVERSE.keys()])
        except asyncio.CancelledError: raise
        except Exception as e: log.error("OI loop: %s", e)

# ═══════════════════════════════════════════════════════════════
#  OKX WS
# ═══════════════════════════════════════════════════════════════

async def ws_send(ws, op, inst_ids):
    if not inst_ids: return
    for i in range(0, len(inst_ids), 30):
        chunk = inst_ids[i:i+30]
        msg = {"op": op, "args": [{"channel": f"candle{BAR}", "instId": x} for x in chunk]}
        try: await ws.send_json(msg)
        except: return

async def ws_manager(ws):
    try:
        while not ws.closed:
            desired = set(WS_DESIRED)
            current = set(WS_SUBSCRIBED)
            to_sub = list(desired - current)
            to_unsub = list(current - desired)
            if to_sub:
                await ws_send(ws, "subscribe", to_sub)
                WS_SUBSCRIBED.update(to_sub)
            if to_unsub:
                await ws_send(ws, "unsubscribe", to_unsub)
                WS_SUBSCRIBED.difference_update(to_unsub)
            await asyncio.sleep(5)
    except asyncio.CancelledError: raise
    except Exception: pass

async def ws_ping_loop(ws):
    try:
        while not ws.closed:
            await asyncio.sleep(25)
            if ws.closed: break
            try: await ws.send_str("ping")
            except: break
    except asyncio.CancelledError: raise
    except: pass

def _apply_candle(base, row):
    if not row or len(row) < 9: return
    try:
        c = {"ts": int(row[0]), "open": num(row[1]), "high": num(row[2]), "low": num(row[3]), "close": num(row[4]), "volume": num(row[7]), "closed": row[8] == "1"}
    except: return
    if c["open"] <= 0: return
    buf = CANDLES[base]
    if buf and buf[-1]["ts"] == c["ts"]: buf[-1] = c
    else: buf.append(c); STATS["candles"] += 1

async def websocket_loop():
    global WS_SUBSCRIBED
    backoff = 1
    while True:
        try:
            log.info("🔌 WS connecting...")
            async with SESSION.ws_connect(OKX_WS, heartbeat=None, autoping=True, receive_timeout=45, max_msg_size=2*1024*1024) as ws:
                WS_SUBSCRIBED = set()
                STATS["ws_reconn"] += 1
                ping_t = asyncio.create_task(ws_ping_loop(ws))
                mgr_t = asyncio.create_task(ws_manager(ws))
                try:
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            raw = msg.data
                            if raw == "pong": continue
                            if raw == "ping": await ws.send_str("pong"); continue
                            try: payload = json.loads(raw)
                            except: continue
                            if payload.get("event"): continue
                            arg = payload.get("arg") or {}
                            if not arg.get("channel", "").startswith("candle"): continue
                            base = base_of(str(arg.get("instId", "")))
                            if base:
                                for r in payload.get("data", []): _apply_candle(base, r)
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR): break
                finally:
                    for t in (mgr_t, ping_t): t.cancel()
            backoff = 1
        except asyncio.CancelledError: raise
        except Exception:
            await asyncio.sleep(backoff); backoff = min(backoff*2, 30)

def sync_ws_subs():
    global WS_DESIRED
    WS_DESIRED = {u["symbol"] for u in UNIVERSE.values()}

# ═══════════════════════════════════════════════════════════════
#  TICK-LEVEL VOLUME TRACKER
# ═══════════════════════════════════════════════════════════════

def sample_volume(base: str):
    buf = CANDLES.get(base)
    if not buf or len(buf) < 2: return
    live = buf[-1]
    if live["open"] <= 0: return

    tr = VOL_TRACKER.get(base)
    if tr is None:
        tr = {
            "candle_ts": live["ts"],
            "cum_base": 0.0,
            "last_vol": live["volume"],
            "samples": deque(maxlen=VOL_SAMPLE_MAXLEN),
        }
        VOL_TRACKER[base] = tr
        tr["samples"].append((time.time(), live["volume"], live["close"]))
        return

    if tr["candle_ts"] != live["ts"]:
        tr["cum_base"] += tr["last_vol"]
        tr["candle_ts"] = live["ts"]
        tr["last_vol"] = 0.0

    tr["last_vol"] = live["volume"]
    cum_total = tr["cum_base"] + live["volume"]
    tr["samples"].append((time.time(), cum_total, live["close"]))

def tick_metrics(base: str, shelf: dict) -> Optional[dict]:
    tr = VOL_TRACKER.get(base)
    if not tr or len(tr["samples"]) < 6: return None

    samples = tr["samples"]
    now_ts, now_cum, now_price = samples[-1]
    oldest_ts = samples[0][0]
    history_sec = now_ts - oldest_ts

    if history_sec < 10: return None

    v10 = v30 = v60 = 0.0
    p10 = p30 = None

    for ts, cum, price in samples:
        age = now_ts - ts
        if age >= 10 and p10 is None:
            p10 = price
            v10 = now_cum - cum
        if age >= 30 and p30 is None:
            p30 = price
            v30 = now_cum - cum
        if age >= 60 and v60 == 0.0:
            v60 = now_cum - cum

    per_sec = shelf["avg_vol"] / BAR_SECONDS if shelf["avg_vol"] > 0 else 1.0

    return {
        "price": now_price,
        "v10": v10, "v30": v30, "v60": v60,
        "rvol10": v10 / (per_sec * 10) if per_sec > 0 else 0.0,
        "rvol30": v30 / (per_sec * 30) if per_sec > 0 else 0.0,
        "rvol60": v60 / (per_sec * 60) if per_sec > 0 else 0.0,
        "pct_10s": (now_price - p10) / p10 * 100 if p10 and p10 > 0 else 0.0,
        "pct_30s": (now_price - p30) / p30 * 100 if p30 and p30 > 0 else 0.0,
        "history_sec": history_sec,
    }

# ═══════════════════════════════════════════════════════════════
#  ANALYZER (candles)
# ═══════════════════════════════════════════════════════════════

def detect_shelf(candles):
    if len(candles) < SH_MIN_CANDLES: return None
    best = None
    for n in range(SH_MIN_CANDLES, min(SH_MAX_CANDLES, len(candles)) + 1):
        cs = candles[-n:]
        high, low = max(c["high"] for c in cs), min(c["low"] for c in cs)
        if low <= 0: continue
        width = (high - low) / low * 100
        if width < SH_RANGE_MIN_PCT or width > SH_RANGE_MAX_PCT: continue
        
        first_c, last_c = cs[0]["close"], cs[-1]["close"]
        if first_c <= 0: continue
        net_move = abs(last_c - first_c) / first_c * 100
        if net_move > SH_MAX_NET_MOVE: continue
        
        mid = (high + low) / 2
        if abs(last_c - mid) / mid * 100 > SH_MAX_DRIFT: continue
        if sum(1 for c in cs if low <= c["close"] <= high) / len(cs) < SH_MIN_INSIDE: continue
        if (high - last_c) / last_c * 100 > SH_MAX_DIST_HIGH: continue
        
        vols = [c["volume"] for c in cs]
        avg_vol = sum(vols) / len(vols)
        if avg_vol < SH_MIN_AVG_VOL_USDT: continue
        vmin, vmax = min(vols), max(vols)
        vol_stab = vmax / vmin if vmin > 0 else 999
        if vol_stab > SH_VOL_STABILITY: continue

        cand = {"high": high, "low": low, "width_pct": width, "candles": n, "avg_vol": avg_vol, "vol_stability": vol_stab, "score": (SH_RANGE_MAX_PCT - width)*2 + (SH_VOL_STABILITY - vol_stab)}
        if best is None or cand["score"] > best["score"]: best = cand
    return best

def find_sweep(shelf, closed):
    if len(closed) < B_SWEEP_LOOKBACK: return None, 0
    recent = closed[-B_SWEEP_LOOKBACK:]
    for i in range(len(recent) - 1, -1, -1):
        c = recent[i]
        age = len(recent) - i
        if age > B_SWEEP_MAX_AGE: break
        if c["low"] >= shelf["low"] * (1 - B_SWEEP_MIN_BREAK_PCT / 100): continue
        if B_SWEEP_MUST_CLOSE_IN and c["close"] < shelf["low"]: continue
        if c["volume"] > shelf["avg_vol"] * B_SWEEP_MAX_VOL_RATIO: continue
        return c, age
    return None, 0

# ═══════════════════════════════════════════════════════════════
#  TICK SIGNAL CHECKS
# ═══════════════════════════════════════════════════════════════

def check_tick_breakout(base: str, shelf: dict, tm: dict) -> Optional[dict]:
    above = (tm["price"] - shelf["high"]) / shelf["high"] * 100
    if above < A_MIN_BREAKOUT_PCT: return None

    oi_change = get_oi_trend(base)
    is_too_high = above > ANTI_TOP_MAX_PCT
    is_squeeze = oi_change < 0

    if is_too_high or (REQUIRE_OI_GROWTH and is_squeeze):
        reasons = []
        if is_too_high: reasons.append(f"Улетела на +{above:.1f}%")
        if is_squeeze: reasons.append(f"OI падает ({oi_change:.1f}%)")
        return {"is_watch": True, "watch_reason": " | ".join(reasons), "above_pct": above, "price": tm["price"], "rvol": tm["rvol10"], "oi_change": oi_change, "trigger": "TICK"}

    if (tm["rvol10"] >= TICK_MIN_RVOL_10S and tm["v10"] >= TICK_MIN_VOL_10S and tm["pct_10s"] >= TICK_MIN_PCT_10S):
        return {"is_watch": False, "level": "TICK_10S", "above_pct": above, "price": tm["price"], "rvol": tm["rvol10"], "rvol30": tm["rvol30"], "v10": tm["v10"], "v30": tm["v30"], "pct_10s": tm["pct_10s"], "pct_30s": tm["pct_30s"], "volume": tm["v10"], "entry": tm["price"], "oi_change": oi_change, "trigger": "TICK_10S"}

    if (tm["history_sec"] >= 30 and tm["rvol30"] >= TICK_MIN_RVOL_30S and tm["v30"] >= TICK_MIN_VOL_30S and tm["pct_30s"] >= TICK_MIN_PCT_10S):
        return {"is_watch": False, "level": "TICK_30S", "above_pct": above, "price": tm["price"], "rvol": tm["rvol30"], "rvol10": tm["rvol10"], "v10": tm["v10"], "v30": tm["v30"], "pct_10s": tm["pct_10s"], "pct_30s": tm["pct_30s"], "volume": tm["v30"], "entry": tm["price"], "oi_change": oi_change, "trigger": "TICK_30S"}

    return None

def check_tick_sweep(base: str, shelf: dict, tm: dict) -> Optional[dict]:
    sweep = shelf.get("sweep")
    if not sweep or sweep["volume"] <= 0: return None
    if tm["v30"] < sweep["volume"] * B_EXP_MIN_VS_SWEEP: return None

    above = (tm["price"] - shelf["high"]) / shelf["high"] * 100
    if above < -A_MIN_BREAKOUT_PCT: return None

    oi_change = get_oi_trend(base)
    is_too_high = above > ANTI_TOP_MAX_PCT
    is_squeeze = oi_change < 0

    if is_too_high or (REQUIRE_OI_GROWTH and is_squeeze):
        reasons = []
        if is_too_high: reasons.append(f"Улетела на +{above:.1f}%")
        if is_squeeze: reasons.append(f"OI падает ({oi_change:.1f}%)")
        return {"is_watch": True, "watch_reason": " | ".join(reasons), "above_pct": above, "price": tm["price"], "rvol": tm["rvol10"], "oi_change": oi_change, "trigger": "TICK_SWEEP"}

    if tm["rvol10"] >= TICK_MIN_RVOL_10S and tm["pct_10s"] >= TICK_MIN_PCT_10S:
        return {"is_watch": False, "level": "SWEEP_10S", "above_pct": above, "price": tm["price"], "rvol": tm["rvol10"], "rvol30": tm["rvol30"], "v10": tm["v10"], "v30": tm["v30"], "pct_10s": tm["pct_10s"], "pct_30s": tm["pct_30s"], "volume": tm["v10"], "sweep_age": shelf.get("sweep_age", 0), "sweep_vol_ratio": sweep["volume"] / shelf["avg_vol"], "entry": tm["price"], "oi_change": oi_change, "trigger": "TICK_SWEEP"}

    return None

# ═══════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════

async def tg(text):
    if not BOT_TOKEN or not CHAT_ID or not SESSION: return False
    try:
        async with SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: STATS["tg_fails"] += 1; return False
            return True
    except: STATS["tg_fails"] += 1; return False

def h_shelf(s): return f"📦 Полка: <code>{s['low']:.8g} — {s['high']:.8g}</code>\n📐 Ширина: <b>{s['width_pct']:.2f}%</b> | Свечей: <b>{s['candles']}×{BAR}</b>\n📊 Avg vol: <b>${s['avg_vol']:,.0f}</b>"
def h_oi(oi): return f"📊 OI (15m): <b>{oi:+.1f}%</b>" + (" (⚠️ Шорт-сквиз!)" if oi < 0 else "")

async def tg_tick_signal(base, shelf, sig, info):
    badge = sig.get("level", "TICK")
    if sig["is_watch"]:
        msg = f"👀 <b>НАБЛЮДЕНИЕ</b>\n\n📌 <b>{escape(base)}USDT</b>\n⚠️ {sig['watch_reason']}\n\n💵 Цена: <code>{sig['price']:.8g}</code>\n📈 Импульс: <b>+{sig['above_pct']:.2f}%</b>\n📊 RVOL(10s): <b>{sig['rvol']:.1f}x</b>\n{h_oi(sig['oi_change'])}"
        if await tg(msg): STATS["tg_watch"] += 1
        return

    msg = (
        f"🚀 <b>ТИК-СИГНАЛ [{badge}]</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"{h_shelf(shelf)}\n\n"
        f"💵 Цена: <code>{sig['price']:.8g}</code>\n"
        f"📈 Выше верха: <b>+{sig['above_pct']:.2f}%</b>\n\n"
        f"⚡ <b>RVOL (тики):</b>\n"
        f"   • 10s: <b>{sig['rvol']:.1f}x</b> (${sig['v10']:,.0f})\n"
        f"   • 30s: <b>{sig.get('rvol30', 0):.1f}x</b> (${sig.get('v30', 0):,.0f})\n\n"
        f"📈 Импульс цены 10s: <b>{sig['pct_10s']:+.2f}%</b>\n"
        f"{h_oi(sig['oi_change'])}"
    )
    if await tg(msg): STATS["sig_tick"] += 1

async def tg_sweep(base, shelf, sig, info):
    msg = (
        f"🎯 <b>SWEEP → TICK-ВЗРЫВ</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"{h_shelf(shelf)}\n\n"
        f"🧊 Sweep ({sig['sweep_age']} свечей назад): сухой <b>{sig['sweep_vol_ratio']*100:.1f}%</b>\n\n"
        f"💵 Цена: <code>{sig['price']:.8g}</code>\n"
        f"📈 Импульс: <b>{sig['above_pct']:+.2f}%</b>\n\n"
        f"⚡ RVOL(10s): <b>{sig['rvol']:.1f}x</b> | RVOL(30s): <b>{sig.get('rvol30', 0):.1f}x</b>\n"
        f"{h_oi(sig['oi_change'])}"
    )
    if await tg(msg): STATS["sig_sweep"] += 1

async def tg_continuation(base, trk, price, pct):
    msg = f"✅ <b>CONTINUATION</b>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{price:.8g}</code>\n📈 Профит: <b>+{pct:.2f}%</b>"
    if await tg(msg): STATS["trk_cont"] += 1

async def tg_fakeout(base, trk, price):
    msg = f"❌ <b>FAKEOUT</b>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{price:.8g}</code>\n📉 Полка: <code>{trk['sh_low']:.8g} — {trk['sh_high']:.8g}</code>"
    if await tg(msg): STATS["trk_fakeout"] += 1

async def tg_climax(base, trk, price, pct):
    msg = f"🥊 <b>CLIMAX</b>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{price:.8g}</code>\n📈 Профит: <b>+{pct:.2f}%</b>"
    if await tg(msg): STATS["trk_climax"] += 1

async def tg_stop_hunt(base, trk, live, wick_ratio):
    msg = f"🎯 <b>STOP HUNT (снятие стопов)</b>\n<i>Слабые руки вышли — ждем продолжение</i>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{live['close']:.8g}</code>\n🩸 Нижняя тень: <b>{wick_ratio*100:.0f}%</b>"
    if await tg(msg): STATS["trk_stophunt"] += 1

# ═══════════════════════════════════════════════════════════════
#  PROCESSING
# ═══════════════════════════════════════════════════════════════

def start_track(base, shelf, entry, kind):
    TRACKS[base] = {"entry": entry, "kind": kind, "ts": time.time(), "sh_high": shelf["high"], "sh_low": shelf["low"], "max_price": entry, "flags": set()}
    log.info("📍 TRACK %s entry=%.8g kind=%s", base, entry, kind)

async def process_shelf_tick(base: str):
    shelf = SHELVES.get(base)
    info = UNIVERSE.get(base)
    if not shelf or base in TRACKS or not info: return
    if time.time() - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN: return

    tm = tick_metrics(base, shelf)
    if not tm: return

    sig = check_tick_breakout(base, shelf, tm)
    if sig:
        if sig["is_watch"]:
            await tg_tick_signal(base, shelf, sig, info)
        else:
            shelf["stage"] = "SIGNALED"
            await tg_tick_signal(base, shelf, sig, info)
            start_track(base, shelf, sig["entry"], "tick")
        LAST_SIGNAL[base] = time.time()
        return

    if shelf.get("sweep"):
        sig = check_tick_sweep(base, shelf, tm)
        if sig:
            if sig["is_watch"]:
                await tg_tick_signal(base, shelf, sig, info)
            else:
                shelf["stage"] = "SIGNALED"
                await tg_sweep(base, shelf, sig, info)
                start_track(base, shelf, sig["entry"], "sweep")
            LAST_SIGNAL[base] = time.time()

async def process_track_tick(base: str):
    trk = TRACKS.get(base)
    if not trk: return
    if time.time() - trk["ts"] > TRK_TIMEOUT_SEC:
        TRACKS.pop(base, None); return

    tm = tick_metrics(base, SHELVES.get(base, {}))
    buf = list(CANDLES.get(base, []))
    if not buf or len(buf) < 2: return
    live = buf[-1]
    if live["open"] <= 0: return

    price = live["close"]
    trk["max_price"] = max(trk["max_price"], price)
    change = (price - trk["entry"]) / trk["entry"] * 100

    if change <= TRK_INVALIDATE_PCT:
        TRACKS.pop(base, None); return

    c_range = live["high"] - live["low"]
    lower_wick = min(live["open"], live["close"]) - live["low"]
    wick_ratio = lower_wick / c_range if c_range > 0 else 0
    is_stophunt = (live["low"] < trk["sh_low"] * 0.999 and wick_ratio > 0.4 and price > trk["sh_low"] * 0.998)

    if is_stophunt and "stophunt" not in trk["flags"]:
        await tg_stop_hunt(base, trk, live, wick_ratio)
        trk["flags"].add("stophunt")

    back_inside = (trk["sh_high"] - price) / trk["sh_high"] * 100
    if ("fakeout" not in trk["flags"] and price < trk["sh_high"] and back_inside > TRK_FAKEOUT_PCT and not is_stophunt):
        await tg_fakeout(base, trk, price)
        trk["flags"].add("fakeout")
        TRACKS.pop(base, None); return

    if ("cont" not in trk["flags"] and price > trk["sh_high"] * (1 + TRK_HOLD_TOL_PCT / 100) and change >= TRK_PROFIT_ALERT):
        await tg_continuation(base, trk, price, change)
        trk["flags"].add("cont")

    if "climax" not in trk["flags"] and tm:
        rng = live["high"] - live["low"]
        if rng > 0 and tm["rvol10"] >= CLX_MIN_RVOL:
            upper_wick = live["high"] - max(live["open"], live["close"])
            peak = (live["high"] - trk["entry"]) / trk["entry"] * 100
            if (upper_wick / rng) >= CLX_MIN_WICK and peak >= CLX_MIN_PROFIT:
                await tg_climax(base, trk, price, change)
                trk["flags"].add("climax")
                TRACKS.pop(base, None)

# ═══════════════════════════════════════════════════════════════
#  LOOPS
# ═══════════════════════════════════════════════════════════════

async def shelf_refresh_loop():
    while True:
        try:
            if UNIVERSE:
                found = {}
                for base in UNIVERSE.keys():
                    buf = list(CANDLES.get(base, []))
                    if len(buf) < SH_MIN_CANDLES + 2: continue
                    shelf = detect_shelf(buf[:-1])
                    if not shelf: continue
                    old = SHELVES.get(base)
                    if old:
                        shelf.update({"created_ts": old.get("created_ts", time.time()), "stage": old.get("stage", "SHELF"), "sweep": old.get("sweep"), "sweep_age": old.get("sweep_age"), "sweep_ts": old.get("sweep_ts")})
                    else:
                        shelf.update({"created_ts": time.time(), "stage": "SHELF"})
                    found[base] = shelf
                
                for base, sh in found.items():
                    if sh.get("sweep"): continue
                    buf = list(CANDLES.get(base, []))
                    if len(buf) < B_SWEEP_LOOKBACK + 2: continue
                    sweep, age = find_sweep(sh, buf[:-1])
                    if sweep:
                        sh["sweep"] = sweep
                        sh["sweep_age"] = age
                        sh["sweep_ts"] = time.time()
                        log.info("🧊 SWEEP %s | vol=$%.0f", base, sweep["volume"])
                
                now = time.time()
                for b, sh in list(found.items()):
                    if now - sh["created_ts"] > SH_MAX_AGE_SEC and b not in TRACKS: del found[b]
                
                SHELVES.clear(); SHELVES.update(found)
                STATS["shelves"] = len(SHELVES)
        except asyncio.CancelledError: raise
        except Exception as e: log.exception("shelf loop: %s", e)
        await asyncio.sleep(SHELF_REFRESH_SEC)

async def tick_scan_loop():
    while True:
        try:
            STATS["ticks"] += 1  # <-- СЧЕТЧИК ПЕРЕНЕСЕН СЮДА

            active = set(SHELVES.keys()) | set(TRACKS.keys())
            for base in active:
                sample_volume(base)

            if active:
                sem = asyncio.Semaphore(10)
                async def worker(b):
                    async with sem:
                        try:
                            await process_shelf_tick(b)
                            await process_track_tick(b)
                        except Exception:
                            STATS["errors"] += 1
                await asyncio.gather(*[worker(b) for b in active])

        except asyncio.CancelledError: raise
        except Exception as e: log.exception("tick loop: %s", e)
        await asyncio.sleep(TICK_SCAN_SEC)

async def universe_loop():
    await refresh_universe()
    await bootstrap_candles()  # <-- ДОБАВЛЕНО
    sync_ws_subs()
    
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH)
            await refresh_universe()
            await bootstrap_candles()  # <-- ДОБАВЛЕНО
            sync_ws_subs()
        except Exception: await asyncio.sleep(15)

# ═══════════════════════════════════════════════════════════════
#  TELEGRAM COMMANDS
# ═══════════════════════════════════════════════════════════════

async def handle_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg or str(msg.get("chat", {}).get("id", "")) != str(CHAT_ID): return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/status") or text.startswith("/health"):
        await tg(
            f"📊 <b>PUMP-SCREENER v17.1</b>\n"
            f"⏱ Uptime: <b>{(time.time()-START_TS)/3600:.2f}h</b>\n"
            f"🌐 Universe: <b>{len(UNIVERSE)}</b>\n"
            f"📦 Shelves: <b>{len(SHELVES)}</b>\n"
            f"📍 Tracks: <b>{len(TRACKS)}</b>\n"
            f"🔌 WS subs: <b>{len(WS_SUBSCRIBED)}</b>\n"
            f"📊 Tick scans: <b>{STATS['ticks']}</b>\n\n"
            f"🚀 Signals: tick=<b>{STATS['sig_tick']}</b> sweep=<b>{STATS['sig_sweep']}</b> watch=<b>{STATS['tg_watch']}</b>\n"
            f"🎯 StopHunt: <b>{STATS['trk_stophunt']}</b> | Fakeout: <b>{STATS['trk_fakeout']}</b>"
        )
    elif text.startswith("/start"): 
        await tg("👋 Бот запущен. /status для статистики.")

async def tg_poll_loop():
    if not BOT_TOKEN: return
    offset = None
    while True:
        try:
            params = {"timeout": 25}
            if offset is not None: params["offset"] = offset
            async with SESSION.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates", params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    if data and data.get("ok"):
                        for u in data.get("result", []): 
                            offset = u["update_id"] + 1
                            await handle_update(u)
                else: await asyncio.sleep(3)
        except asyncio.CancelledError: raise
        except: await asyncio.sleep(5)

# ═══════════════════════════════════════════════════════════════
#  WEB & STARTUP
# ═══════════════════════════════════════════════════════════════

async def index(_): 
    return web.Response(text=f"v17.1 | Uni={len(UNIVERSE)} | Shelves={len(SHELVES)} | Tracks={len(TRACKS)} | WS={len(WS_SUBSCRIBED)} | Tick={STATS['ticks']}", content_type="text/plain")

async def on_start(app):
    global SESSION, HTTP_SEM, WS_TASK
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=20))
    HTTP_SEM = asyncio.Semaphore(15)
    
    if BOT_TOKEN: 
        await SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", params={"drop_pending_updates": "false"})
    
    await load_instruments()
    await refresh_universe()      # <-- ДОБАВЛЕНО
    await bootstrap_candles()     # <-- ДОБАВЛЕНО
    
    WS_TASK = asyncio.create_task(websocket_loop()); app["ws_task"] = WS_TASK
    app["uni_task"] = asyncio.create_task(universe_loop())
    app["shelf_task"] = asyncio.create_task(shelf_refresh_loop())
    app["tick_task"] = asyncio.create_task(tick_scan_loop())
    app["oi_task"] = asyncio.create_task(oi_poll_loop())
    app["tg_task"] = asyncio.create_task(tg_poll_loop())
    
    await tg("🚀 <b>PUMP-SCREENER v17.1 STARTED</b>\nTICK-LEVEL RVOL + BOOTSTRAP")

async def on_stop(app):
    for k in ("ws_task", "uni_task", "shelf_task", "tick_task", "oi_task", "tg_task"):
        if t := app.get(k): t.cancel()
    if SESSION and not SESSION.closed: await SESSION.close()

app = web.Application()
app.router.add_get("/", index); app.router.add_get("/health", index)
app.on_startup.append(on_start); app.on_cleanup.append(on_stop)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
