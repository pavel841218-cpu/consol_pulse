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
# PUMP-SCREENER v16.0 — OKX 5m WS + OI + STOP HUNT
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID   = os.environ.get("CHAT_ID", "")
PORT      = int(os.environ.get("PORT", "10000"))

# ─── Timeframes ───
BAR                = "5m"
BAR_SECONDS        = 300
SCAN_INTERVAL_SEC  = int(os.environ.get("SCAN_INTERVAL_SEC", "15"))
UNIVERSE_REFRESH   = int(os.environ.get("UNIVERSE_REFRESH_SEC", "300"))
SIGNAL_COOLDOWN    = int(os.environ.get("SIGNAL_COOLDOWN_SEC", "1800"))
CANDLE_BUFFER_SIZE = 30
OI_POLL_INTERVAL   = int(os.environ.get("OI_POLL_INTERVAL", "300"))

# ─── Universe filter ───
MIN_VOL_24H   = float(os.environ.get("MIN_24H_VOLUME_USDT", "200000"))
MIN_PRICE     = float(os.environ.get("MIN_PRICE_USDT", "0.000001"))
MAX_PRICE     = float(os.environ.get("MAX_PRICE_USDT", "50.0"))
MAX_UNIVERSE  = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "150"))
MIN_CHANGE_24H = float(os.environ.get("MIN_CHANGE_24H_PCT", "-50.0"))
MAX_CHANGE_24H = float(os.environ.get("MAX_CHANGE_24H_PCT", "200.0"))

# ─── Shelf (полка) ───
SH_MIN_CANDLES      = int(os.environ.get("SHELF_MIN_CANDLES", "10"))
SH_MAX_CANDLES      = int(os.environ.get("SHELF_MAX_CANDLES", "20"))
SH_RANGE_MIN_PCT    = float(os.environ.get("SHELF_RANGE_MIN_PCT", "0.30"))
SH_RANGE_MAX_PCT    = float(os.environ.get("SHELF_RANGE_MAX_PCT", "3.50"))
SH_MAX_NET_MOVE     = float(os.environ.get("SHELF_MAX_NET_MOVE_PCT", "2.00"))
SH_MAX_DRIFT        = float(os.environ.get("SHELF_MAX_DRIFT_PCT", "1.50"))
SH_MIN_INSIDE       = float(os.environ.get("SHELF_MIN_INSIDE_RATIO", "0.75"))
SH_MAX_DIST_HIGH    = float(os.environ.get("SHELF_MAX_DIST_FROM_HIGH_PCT", "1.20"))
SH_VOL_STABILITY    = float(os.environ.get("SHELF_VOL_STABILITY_MAX", "2.5"))
SH_MIN_AVG_VOL_USDT = float(os.environ.get("SHELF_MIN_AVG_VOL_USDT", "2000"))
SH_MAX_AGE_SEC      = int(os.environ.get("SHELF_MAX_AGE_SEC", str(6 * 3600)))

# ─── Branch A: direct breakout ───
A_MIN_BREAKOUT_PCT = float(os.environ.get("DIRECT_BREAKOUT_MIN_PCT", "0.35"))
A_MIN_RVOL         = float(os.environ.get("DIRECT_BREAKOUT_MIN_RVOL", "1.8"))
A_MIN_VOL_USDT     = float(os.environ.get("DIRECT_BREAKOUT_MIN_VOL_USDT", "3000"))
A_MIN_PCT_CHANGE   = float(os.environ.get("DIRECT_BREAKOUT_MIN_PCT_CHANGE", "1.5"))

# ─── Branch B: sweep → expansion ───
B_SWEEP_LOOKBACK       = int(os.environ.get("SWEEP_LOOKBACK", "6"))
B_SWEEP_MIN_BREAK_PCT  = float(os.environ.get("SWEEP_BREAKDOWN_MIN_PCT", "0.10"))
B_SWEEP_MAX_VOL_RATIO  = float(os.environ.get("SWEEP_MAX_VOL_RATIO", "0.45"))
B_SWEEP_MUST_CLOSE_IN  = os.environ.get("SWEEP_MUST_CLOSE_INSIDE", "1") == "1"
B_SWEEP_MAX_AGE        = int(os.environ.get("SWEEP_MAX_AGE_CANDLES", "5"))
B_EXP_MIN_VS_SWEEP     = float(os.environ.get("EXPANSION_MIN_RATIO_VS_SWEEP", "3.0"))
B_EXP_MIN_VS_PREV      = float(os.environ.get("EXPANSION_MIN_VOL_VS_PREV", "3.0"))
B_EXP_MIN_VS_AVG       = float(os.environ.get("EXPANSION_MIN_VOL_VS_AVG", "0.6"))
B_EARLY_ENABLED        = os.environ.get("EARLY_SIGNAL_ENABLED", "1") == "1"
B_EARLY_MIN_VS_PREV    = float(os.environ.get("EARLY_MIN_VOL_VS_PREV", "8.0"))

# ─── Live candle gates ───
LIVE_MIN_ELAPSED_SEC = int(os.environ.get("LIVE_MIN_ELAPSED_SEC", "60"))
LIVE_MIN_VOL_USDT    = float(os.environ.get("LIVE_MIN_CURRENT_VOL_USDT", "2000"))

# ─── OI & Anti-Top ───
ANTI_TOP_MAX_PCT   = float(os.environ.get("ANTI_TOP_MAX_PCT", "3.0"))
REQUIRE_OI_GROWTH  = os.environ.get("REQUIRE_OI_GROWTH", "0") == "1"

# ─── Track (post-signal) ───
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
OI_HISTORY = defaultdict(lambda: deque(maxlen=12)) # 12 замеров (1 час)

WS_TASK: Optional[asyncio.Task] = None
WS_SUBSCRIBED: set[str] = set()
WS_DESIRED: set[str] = set()

STATS = defaultdict(int)

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
#  OKX REST & OI
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
    log.info("📐 ctVal: %d contracts", n)

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
    log.info("🌐 Universe: %d pairs", len(UNIVERSE))
    gc.collect()

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
            log.info("📊 Сбор OI...")
            sem = asyncio.Semaphore(10)
            async def fetch_one(base):
                async with sem:
                    item = UNIVERSE.get(base)
                    if item:
                        oi = await fetch_okx_oi(item["symbol"])
                        if oi > 0: OI_HISTORY[base].append((time.time(), oi))
            await asyncio.gather(*[fetch_one(b) for b in UNIVERSE.keys()])
        except asyncio.CancelledError: raise
        except Exception as e: log.error("OI loop error: %s", e)

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
        except Exception as e:
            await asyncio.sleep(backoff); backoff = min(backoff*2, 30)

def sync_ws_subs():
    global WS_DESIRED
    WS_DESIRED = {u["symbol"] for u in UNIVERSE.values()}

# ═══════════════════════════════════════════════════════════════
#  ANALYZER
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

def live_metrics(candles):
    if len(candles) < 3: return None
    live, prev = candles[-1], candles[-2]
    base = candles[-25:-1]
    if not base or live["open"] <= 0: return None
    
    elapsed = max((time.time() * 1000 - live["ts"]) / 1000, 5)
    if elapsed < LIVE_MIN_ELAPSED_SEC or live["volume"] < LIVE_MIN_VOL_USDT: return None
    
    avg_base = sum(c["volume"] for c in base) / len(base)
    if avg_base <= 0: return None
    
    projected = live["volume"] * (BAR_SECONDS / elapsed)
    return {
        "price": live["close"], "open": live["open"], "high": live["high"], "low": live["low"],
        "volume": live["volume"], "prev_volume": prev["volume"], "rvol": projected / avg_base,
        "vol_vs_prev": projected / prev["volume"] if prev["volume"] > 0 else 0.0,
        "pct_change": (live["close"] - live["open"]) / live["open"] * 100, "elapsed": elapsed
    }

def check_direct(shelf, lm, base):
    above = (lm["price"] - shelf["high"]) / shelf["high"] * 100
    if above < A_MIN_BREAKOUT_PCT or lm["rvol"] < A_MIN_RVOL or lm["volume"] < A_MIN_VOL_USDT or lm["pct_change"] < A_MIN_PCT_CHANGE: return None
    
    oi_change = get_oi_trend(base)
    is_too_high = above > ANTI_TOP_MAX_PCT
    is_squeeze = oi_change < 0

    if is_too_high or (REQUIRE_OI_GROWTH and is_squeeze):
        reasons = []
        if is_too_high: reasons.append(f"Улетела на +{above:.1f}%")
        if is_squeeze: reasons.append(f"OI падает ({oi_change:.1f}%)")
        return {"is_watch": True, "watch_reason": " | ".join(reasons), "above_pct": above, "price": lm["price"], "rvol": lm["rvol"], "oi_change": oi_change}

    return {"is_watch": False, "above_pct": above, "rvol": lm["rvol"], "vol_vs_prev": lm["vol_vs_prev"], "volume": lm["volume"], "pct_change": lm["pct_change"], "price": lm["price"], "entry": lm["price"], "oi_change": oi_change}

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

def check_sweep(shelf, lm, sweep, age, base):
    if sweep["volume"] <= 0: return None
    exp = lm["volume"] / sweep["volume"]
    if exp < B_EXP_MIN_VS_SWEEP: return None
    above = (lm["price"] - shelf["high"]) / shelf["high"] * 100
    if above < -A_MIN_BREAKOUT_PCT: return None
        
    oi_change = get_oi_trend(base)
    is_too_high = above > ANTI_TOP_MAX_PCT
    is_squeeze = oi_change < 0

    if is_too_high or (REQUIRE_OI_GROWTH and is_squeeze):
        reasons = []
        if is_too_high: reasons.append(f"Улетела на +{above:.1f}%")
        if is_squeeze: reasons.append(f"OI падает ({oi_change:.1f}%)")
        return {"is_watch": True, "watch_reason": " | ".join(reasons), "above_pct": above, "price": lm["price"], "rvol": lm["rvol"], "oi_change": oi_change}

    payload = {"is_watch": False, "above_pct": above, "rvol": lm["rvol"], "vol_vs_prev": lm["vol_vs_prev"], "volume": lm["volume"], "pct_change": lm["pct_change"], "price": lm["price"], "entry": lm["price"], "sweep_age": age, "sweep_vol_ratio": sweep["volume"] / shelf["avg_vol"], "exp_vs_sweep": exp, "oi_change": oi_change}
    if B_EARLY_ENABLED and lm["vol_vs_prev"] >= B_EARLY_MIN_VS_PREV and lm["pct_change"] >= A_MIN_PCT_CHANGE: return {**payload, "level": "EARLY"}
    if lm["vol_vs_prev"] >= B_EXP_MIN_VS_PREV and lm["rvol"] >= B_EXP_MIN_VS_AVG: return {**payload, "level": "CONFIRMED"}
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

async def tg_direct(base, shelf, sig, info):
    msg = f"🚀 <b>ПРОБОЙ ПОЛКИ (direct)</b>\n\n📌 <b>{escape(base)}USDT</b>\n{h_shelf(shelf)}\n\n💵 Цена: <code>{sig['price']:.8g}</code>\n📈 Выше верха: <b>+{sig['above_pct']:.2f}%</b>\n⚡ Vol vs prev: <b>{sig['vol_vs_prev']:.1f}x</b>\n📊 RVOL: <b>{sig['rvol']:.2f}x</b>\n{h_oi(sig['oi_change'])}\n\n💰 Залив: <b>${sig['volume']:,.0f}</b>"
    if await tg(msg): STATS["sig_direct"] += 1

async def tg_sweep(base, shelf, sig, info):
    badge = "⚡ РАННИЙ" if sig["level"] == "EARLY" else "🎯 ПОДТВЕРЖДЁН"
    msg = f"<b>SWEEP → ВЗРЫВ ОБЪЁМА</b> [{badge}]\n\n📌 <b>{escape(base)}USDT</b>\n{h_shelf(shelf)}\n\n🧊 Sweep ({sig['sweep_age']} свечей назад): сухой объём <b>{sig['sweep_vol_ratio']*100:.1f}%</b>\n💥 Взрыв: vs prev <b>{sig['vol_vs_prev']:.1f}x</b> | vs avg <b>{sig['rvol']:.2f}x</b>\n\n💵 Цена: <code>{sig['price']:.8g}</code>\n📈 Импульс: <b>{sig['above_pct']:+.2f}%</b>\n{h_oi(sig['oi_change'])}\n\n💰 Залив: <b>${sig['volume']:,.0f}</b>"
    if await tg(msg): STATS["sig_sweep"] += 1

async def tg_watch(base, shelf, sig, info, kind):
    msg = f"👀 <b>НАБЛЮДЕНИЕ [{kind}]</b>\n\n📌 <b>{escape(base)}USDT</b>\n⚠️ <b>Причина:</b> {sig['watch_reason']}\n\n💵 Цена: <code>{sig['price']:.8g}</code>\n📈 Импульс: <b>+{sig['above_pct']:.2f}%</b>\n📊 RVOL: <b>{sig['rvol']:.1f}x</b>\n{h_oi(sig['oi_change'])}\n\n<i>Входить рискованно. Смотрим.</i>"
    if await tg(msg): STATS["tg_watch"] += 1

async def tg_continuation(base, trk, lm):
    p = (lm["price"] - trk["entry"]) / trk["entry"] * 100
    msg = f"✅ <b>CONTINUATION</b>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{lm['price']:.8g}</code>\n📈 Профит: <b>+{p:.2f}%</b>\n📊 RVOL: <b>{lm['rvol']:.2f}x</b>"
    if await tg(msg): STATS["trk_cont"] += 1

async def tg_fakeout(base, trk, price):
    msg = f"❌ <b>FAKEOUT — вернулась в полку</b>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{price:.8g}</code>\n📉 Полка: <code>{trk['sh_low']:.8g} — {trk['sh_high']:.8g}</code>"
    if await tg(msg): STATS["trk_fakeout"] += 1

async def tg_climax(base, trk, lm):
    p = (lm["price"] - trk["entry"]) / trk["entry"] * 100
    msg = f"🥊 <b>CLIMAX — фиксация</b>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{lm['price']:.8g}</code>\n📈 Профит: <b>+{p:.2f}%</b>\n📊 RVOL: <b>{lm['rvol']:.2f}x</b>"
    if await tg(msg): STATS["trk_climax"] += 1

async def tg_stop_hunt(base, trk, live, wick_ratio):
    msg = f"🎯 <b>STOP HUNT (Выбивание стопов)</b>\n<i>Слабые руки вышли. Ждем продолжение!</i>\n\n📌 <b>{escape(base)}USDT</b>\n💰 Entry: <code>{trk['entry']:.8g}</code>\n💵 Сейчас: <code>{live['close']:.8g}</code>\n\n📉 Пробили низ полки, но выкупили.\n🩸 Нижняя тень: <b>{wick_ratio*100:.0f}%</b> свечи\n⚠️ <b>Не паникуем!</b> Это сбор ликвидности."
    if await tg(msg): STATS["trk_stophunt"] += 1

# ═══════════════════════════════════════════════════════════════
#  PROCESS & TRACK
# ═══════════════════════════════════════════════════════════════

def start_track(base, shelf, entry, kind):
    TRACKS[base] = {"entry": entry, "kind": kind, "ts": time.time(), "sh_high": shelf["high"], "sh_low": shelf["low"], "max_price": entry, "flags": set()}
    log.info("📍 TRACK %s entry=%.8g kind=%s", base, entry, kind)

async def process_shelf(base):
    shelf, info = SHELVES.get(base), UNIVERSE.get(base)
    if not shelf or base in TRACKS or not info or time.time() - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN: return
    
    buf = list(CANDLES.get(base, []))
    if len(buf) < SH_MIN_CANDLES + 3: return
    
    lm = live_metrics(buf)
    if lm:
        sig = check_direct(shelf, lm, base)
        if sig:
            if sig.get("is_watch"): await tg_watch(base, shelf, sig, info, "Direct")
            else:
                shelf["stage"] = "SIGNALED"; await tg_direct(base, shelf, sig, info); start_track(base, shelf, sig["entry"], "direct")
            LAST_SIGNAL[base] = time.time(); return

    if not shelf.get("sweep"):
        sweep, age = find_sweep(shelf, buf[:-1])
        if sweep:
            shelf["sweep"], shelf["sweep_age"], shelf["sweep_ts"] = sweep, age, time.time()
            log.info("🧊 SWEEP %s | vol=$%.0f (%.1f%%)", base, sweep["volume"], sweep["volume"]/shelf["avg_vol"]*100)

    if shelf.get("sweep") and lm:
        age = int((time.time() - shelf["sweep_ts"]) / BAR_SECONDS) + 1
        if age > B_SWEEP_MAX_AGE: shelf.pop("sweep", None); shelf.pop("sweep_ts", None); return
        sig = check_sweep(shelf, lm, shelf["sweep"], age, base)
        if sig:
            if sig.get("is_watch"): await tg_watch(base, shelf, sig, info, "Sweep")
            else:
                shelf["stage"] = "SIGNALED"; await tg_sweep(base, shelf, sig, info); start_track(base, shelf, sig["entry"], "sweep")
            LAST_SIGNAL[base] = time.time(); return

async def process_track(base):
    trk = TRACKS.get(base)
    if not trk: return
    if time.time() - trk["ts"] > TRK_TIMEOUT_SEC: TRACKS.pop(base, None); return
    
    buf = list(CANDLES.get(base, []))
    if len(buf) < 3: return
    
    live = buf[-1]
    if live["open"] <= 0: return
    
    price = live["close"]
    trk["max_price"] = max(trk["max_price"], price)
    change = (price - trk["entry"]) / trk["entry"] * 100

    if change <= TRK_INVALIDATE_PCT: TRACKS.pop(base, None); return

    # STOP HUNT DETECTOR
    c_range = live["high"] - live["low"]
    lower_wick = min(live["open"], live["close"]) - live["low"]
    wick_ratio = lower_wick / c_range if c_range > 0 else 0
    
    is_stophunt = (live["low"] < trk["sh_low"] * 0.999 and wick_ratio > 0.4 and price > trk["sh_low"] * 0.998)

    if is_stophunt and "stophunt" not in trk["flags"]:
        await tg_stop_hunt(base, trk, live, wick_ratio)
        trk["flags"].add("stophunt")

    # FAKEOUT (только если это не stop hunt)
    back_inside = (trk["sh_high"] - price) / trk["sh_high"] * 100
    if ("fakeout" not in trk["flags"] and price < trk["sh_high"] and back_inside > TRK_FAKEOUT_PCT and not is_stophunt):
        await tg_fakeout(base, trk, price); trk["flags"].add("fakeout"); TRACKS.pop(base, None); return

    lm = live_metrics(buf)
    if not lm: return

    if ("cont" not in trk["flags"] and price > trk["sh_high"] * (1 + TRK_HOLD_TOL_PCT / 100) and change >= TRK_PROFIT_ALERT):
        await tg_continuation(base, trk, lm); trk["flags"].add("cont")

    if "climax" not in trk["flags"]:
        if c_range > 0 and lm["rvol"] >= CLX_MIN_RVOL:
            upper_wick = live["high"] - max(live["open"], live["close"])
            peak = (live["high"] - trk["entry"]) / trk["entry"] * 100
            if (upper_wick / c_range) >= CLX_MIN_WICK and peak >= CLX_MIN_PROFIT:
                await tg_climax(base, trk, lm); trk["flags"].add("climax"); TRACKS.pop(base, None)

# ═══════════════════════════════════════════════════════════════
#  LOOPS
# ═══════════════════════════════════════════════════════════════

async def refresh_shelves():
    if not UNIVERSE: return
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
    now = time.time()
    for b, sh in list(found.items()):
        if now - sh["created_ts"] > SH_MAX_AGE_SEC and b not in TRACKS: del found[b]
    SHELVES.clear(); SHELVES.update(found)

async def scan_cycle():
    if not UNIVERSE: return
    STATS["scans"] += 1
    await refresh_shelves()
    bases = set(SHELVES.keys()) | set(TRACKS.keys())
    if not bases: return
    sem = asyncio.Semaphore(8)
    async def worker(b):
        async with sem:
            try: await process_shelf(b); await process_track(b)
            except: STATS["errors"] += 1
    await asyncio.gather(*[worker(b) for b in bases])

async def scan_loop():
    while True:
        try: await scan_cycle()
        except Exception as e: log.exception("scan: %s", e)
        await asyncio.sleep(SCAN_INTERVAL_SEC)

async def universe_loop():
    await refresh_universe(); sync_ws_subs()
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH); await refresh_universe(); sync_ws_subs()
        except Exception: await asyncio.sleep(15)

# ═══════════════════════════════════════════════════════════════
#  TELEGRAM COMMANDS
# ═══════════════════════════════════════════════════════════════

async def handle_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg or str(msg.get("chat", {}).get("id", "")) != str(CHAT_ID): return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/status") or text.startswith("/health"):
        await tg(f"📊 <b>PUMP-SCREENER v16.0</b>\n⏱ Uptime: <b>{(time.time()-START_TS)/3600:.2f}h</b>\n🌐 Universe: <b>{len(UNIVERSE)}</b>\n📦 Shelves: <b>{len(SHELVES)}</b>\n📍 Tracks: <b>{len(TRACKS)}</b>\n🔌 WS subs: <b>{len(WS_SUBSCRIBED)}</b>\n\n🚀 Signals: direct=<b>{STATS['sig_direct']}</b> sweep=<b>{STATS['sig_sweep']}</b> watch=<b>{STATS['tg_watch']}</b>\n🎯 StopHunt: <b>{STATS['trk_stophunt']}</b> | Fakeout: <b>{STATS['trk_fakeout']}</b>")
    elif text.startswith("/start"): await tg("👋 Бот запущен. /status для статистики.")

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
                        for u in data.get("result", []): offset = u["update_id"] + 1; await handle_update(u)
                else: await asyncio.sleep(3)
        except asyncio.CancelledError: raise
        except: await asyncio.sleep(5)

# ═══════════════════════════════════════════════════════════════
#  WEB & STARTUP
# ═══════════════════════════════════════════════════════════════

async def index(_): return web.Response(text=f"v16.0 | Uni={len(UNIVERSE)} | Shelves={len(SHELVES)} | Tracks={len(TRACKS)} | WS={len(WS_SUBSCRIBED)}", content_type="text/plain")

async def on_start(app):
    global SESSION, HTTP_SEM, WS_TASK
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=20))
    HTTP_SEM = asyncio.Semaphore(15)
    if BOT_TOKEN: await SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", params={"drop_pending_updates": "false"})
    await load_instruments()
    WS_TASK = asyncio.create_task(websocket_loop()); app["ws_task"] = WS_TASK
    app["uni_task"] = asyncio.create_task(universe_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["oi_task"] = asyncio.create_task(oi_poll_loop())
    app["tg_task"] = asyncio.create_task(tg_poll_loop())
    await tg("🚀 <b>PUMP-SCREENER v16.0 STARTED</b>\nWS-first + OI + Stop Hunt")

async def on_stop(app):
    for k in ("ws_task", "uni_task", "scan_task", "oi_task", "tg_task"):
        if t := app.get(k): t.cancel()
    if SESSION and not SESSION.closed: await SESSION.close()

app = web.Application()
app.router.add_get("/", index); app.router.add_get("/health", index)
app.on_startup.append(on_start); app.on_cleanup.append(on_stop)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
