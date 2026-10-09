import asyncio
import aiohttp
from aiohttp import web
import os
import time
import gc
import logging
from html import escape

# ============================================================
# ПАМП-ХАНТЕР v12.0 (OPTIMIZED RELEASE)
# - Окно 1.5 часа (6 свечей 15m)
# - Фильтр Anti-Sagging (shelf_position >= 0.50)
# - Отключен гибельный EMA-фильтр медвежьего тренда
# - Динамический BTC-макро предохранитель
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Периодика ---
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "30"))
TRACK_INTERVAL_SEC = int(os.environ.get("TRACK_INTERVAL_SEC", "300"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))
BTC_STATE_REFRESH_SEC = int(os.environ.get("BTC_STATE_REFRESH_SEC", "60"))

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "200"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "150"))
MAX_ACTIVE_TRACKS = int(os.environ.get("MAX_ACTIVE_TRACKS", "20"))

MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "300000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "1.0"))
MIN_LISTING_AGE_DAYS = float(os.environ.get("MIN_LISTING_AGE_DAYS", "14"))

# === Накопление (1.5-часовое окно) ===
SHELF_LOOKBACK_CANDLES = int(os.environ.get("SHELF_LOOKBACK_CANDLES", "6"))  # 6 * 15m = 1.5h
SHELF_HIST_LOOKBACK = int(os.environ.get("SHELF_HIST_LOOKBACK", "12"))
MAX_SHELF_RANGE_PCT = float(os.environ.get("MAX_SHELF_RANGE_PCT", "2.5"))
MIN_CUMULATIVE_RVOL = float(os.environ.get("MIN_CUMULATIVE_RVOL", "4.0"))   # Было 1.8 — отсекаем слабый объём вроде UBUSDT/FARTCOIN[span_11](start_span)[span_11](end_span)
MIN_GREEN_BUY_RATIO = float(os.environ.get("MIN_GREEN_BUY_RATIO", "0.55"))
MIN_SHELF_POSITION = float(os.environ.get("MIN_SHELF_POSITION", "0.65"))  # Было 0.50 — берём только верхнюю треть полки перед самым пробоем[span_12](start_span)[span_12](end_span)[span_13](start_span)[span_13](end_span)
CUMULATIVE_COOLDOWN_SEC = int(os.environ.get("CUMULATIVE_COOLDOWN_SEC", str(3 * 3600)))

# === ФИЛЬТР ТРЕНДА МОНЕТЫ ===
TREND_MAX_24H_DROP_PCT = float(os.environ.get("TREND_MAX_24H_DROP_PCT", "-15.0")) # Не зажимаем монету при сливе
TREND_LOWER_HIGH_TOLERANCE = float(os.environ.get("TREND_LOWER_HIGH_TOLERANCE", "0.98"))
TREND_EMA_BEARISH_ENABLED = False # ОТКЛЮЧЕНО: разрешаем входить на локальном дне

# === МАКРО-ФИЛЬТР BTC ===
BTC_FILTER_ENABLED = os.environ.get("BTC_FILTER_ENABLED", "true").lower() == "true"
BTC_MAX_30M_DROP_PCT = float(os.environ.get("BTC_MAX_30M_DROP_PCT", "-1.2"))

# === Трекер Кульминации ===
CLIMAX_MIN_PROFIT_PCT = float(os.environ.get("CLIMAX_MIN_PROFIT_PCT", "2.5"))
CLIMAX_RVOL_THRESHOLD = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "3.5"))
CLIMAX_MIN_WICK_RATIO = float(os.environ.get("CLIMAX_MIN_WICK_RATIO", "0.45"))
CLIMAX_TRACK_TIMEOUT_SEC = int(os.environ.get("CLIMAX_TRACK_TIMEOUT_SEC", str(24 * 3600)))
CLIMAX_INVALIDATE_PCT = float(os.environ.get("CLIMAX_INVALIDATE_PCT", "-3.0"))

ALLOWED_UPDATES = '["message","edited_message","callback_query","channel_post","edited_channel_post"]'

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v12.0")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_SIGNAL = {}
ACTIVE_TRACKS = {}

BTC_STATE = {"change_30m_pct": 0.0, "last_update": 0.0, "ok": True}

STATS = {
    "scans": 0,
    "track_scans": 0,
    "cumulative_signals": 0,
    "climax_reversals": 0,
    "tracks_timed_out": 0,
    "tracks_invalidated": 0,
    "tracks_skipped_max": 0,
    "rejected_shelf_range": 0,
    "rejected_sagging_low": 0,
    "rejected_cumulative_rvol": 0,
    "rejected_green_ratio": 0,
    "rejected_trend_24h": 0,
    "rejected_trend_lower_highs": 0,
    "rejected_btc_macro": 0,
    "tg_commands_received": 0,
    "tg_send_failures": 0,
    "tg_poll_ok": 0,
    "tg_poll_err": 0,
}

# ============================================================
# HTTP
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
    if not s:
        return ""
    s = str(s).upper()
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s

# ============================================================
# FETCHERS
# ============================================================

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "open":
            continue
        if str(row.get("settleCurrency", "")).upper() != "USDT":
            continue
        symbol = str(row.get("symbol", "")).upper()
        base = norm(row.get("baseCurrency") or symbol)
        if not base:
            continue
        result[base] = {
            "symbol": symbol,
            "price": num(row.get("lastTradePrice") or row.get("markPrice")),
            "volume24": num(row.get("turnoverOf24h")),
            "change24": num(row.get("priceChgPct")) * 100,
            "first_open_ms": num(row.get("firstOpenDate")),
        }
    return result

async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    result = {}
    if not data or data.get("code") != "00000":
        return result
    for row in data.get("data", []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol.endswith("USDT"):
            base = norm(symbol)
            if base:
                result[base] = {"symbol": symbol}
    return result

def _parse_list_candles(rows):
    candles = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                if ts < 10**12:
                    ts *= 1000
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles

async def fetch_kucoin_candles_15m(symbol, limit):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - limit * 15 * 60 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": "15", "from": from_ms, "to": now_ms,
    })
    if not data or not isinstance(data.get("data"), list):
        return []
    return _parse_list_candles(data["data"])

# ============================================================
# ФИЛЬТРЫ И МАТЕМАТИКА
# ============================================================

def check_trend_filter(candles_15m, item):
    change_24h = item.get("change24", 0) or 0
    if change_24h < TREND_MAX_24H_DROP_PCT:
        return False, "trend_24h"

    closed = candles_15m[:-1]
    if len(closed) < 12:
        return True, "ok"

    # Lower Highs проверка только на самых коротких таймфреймах
    recent = closed[-12:]
    half = len(recent) // 2
    first_max = max(c["high"] for c in recent[:half])
    second_max = max(c["high"] for c in recent[half:])
    if first_max > 0 and second_max < first_max * TREND_LOWER_HIGH_TOLERANCE:
        return False, "trend_lower_highs"

    return True, "ok"

async def refresh_btc_state():
    if not BTC_FILTER_ENABLED:
        BTC_STATE["ok"] = True
        return

    candles = await fetch_kucoin_candles_15m("XBTUSDTM", limit=4)
    if len(candles) < 3:
        BTC_STATE["ok"] = True
        BTC_STATE["last_update"] = time.time()
        return

    # Падение BTC за последние 30 минут (2 свечи по 15m)
    current_close = candles[-1]["close"]
    prev_open = candles[-3]["open"]

    if prev_open > 0:
        change_30m = ((current_close - prev_open) / prev_open) * 100
    else:
        change_30m = 0.0

    BTC_STATE["change_30m_pct"] = change_30m
    BTC_STATE["ok"] = change_30m >= BTC_MAX_30M_DROP_PCT
    BTC_STATE["last_update"] = time.time()

    if not BTC_STATE["ok"]:
        log.warning("⚠️ BTC DUMP BLOCK: 30m change = %.2f%%", change_30m)

def check_cumulative_rvol_accumulation(candles_15m, item):
    lookback_shelf = SHELF_LOOKBACK_CANDLES
    hist_lookback = SHELF_HIST_LOOKBACK

    if len(candles_15m) < lookback_shelf + hist_lookback + 1:
        return None

    trend_ok, trend_reason = check_trend_filter(candles_15m, item)
    if not trend_ok:
        key = f"rejected_{trend_reason}"
        if key in STATS:
            STATS[key] += 1
        return None

    closed_candles = candles_15m[:-1]
    shelf_candles = closed_candles[-lookback_shelf:]

    max_h = max(c["high"] for c in shelf_candles)
    min_l = min(c["low"] for c in shelf_candles)
    if min_l <= 0:
        return None

    shelf_range = max_h - min_l
    shelf_range_pct = (shelf_range / min_l) * 100
    if shelf_range_pct > MAX_SHELF_RANGE_PCT:
        STATS["rejected_shelf_range"] += 1
        return None

    last_close = shelf_candles[-1]["close"]
    
    # === ANTI-SAGGING FILTER (Shelf Position) ===
    shelf_position = (last_close - min_l) / (shelf_range + 1e-8)
    if shelf_position < MIN_SHELF_POSITION:
        STATS["rejected_sagging_low"] += 1  # Отбраковываем монеты на дне полки (WLFI/HBAR)!
        return None

    dist_from_low_pct = ((last_close - min_l) / min_l) * 100

    historical = closed_candles[-(lookback_shelf + hist_lookback):-lookback_shelf]
    avg_hist_vol = sum(c["volume"] for c in historical) / len(historical) if historical else 0
    if avg_hist_vol <= 0:
        return None

    total_shelf_vol = sum(c["volume"] for c in shelf_candles)
    cumulative_rvol = total_shelf_vol / (avg_hist_vol * lookback_shelf)
    if cumulative_rvol < MIN_CUMULATIVE_RVOL:
        STATS["rejected_cumulative_rvol"] += 1
        return None

    green_vols = sum(c["volume"] for c in shelf_candles if c["close"] >= c["open"])
    green_ratio = green_vols / total_shelf_vol if total_shelf_vol > 0 else 0
    if green_ratio < MIN_GREEN_BUY_RATIO:
        STATS["rejected_green_ratio"] += 1
        return None

    entry_price = last_close
    stop_loss = min_l * 0.997

    return {
        "shelf_range_pct": round(shelf_range_pct, 2),
        "dist_from_low_pct": round(dist_from_low_pct, 2),
        "shelf_position": round(shelf_position * 100, 1),
        "cumulative_rvol": round(cumulative_rvol, 2),
        "green_ratio_pct": round(green_ratio * 100, 1),
        "entry": entry_price,
        "stop_loss": stop_loss,
        "target_3r": entry_price + (entry_price - stop_loss) * 3,
        "target_6r": entry_price + (entry_price - stop_loss) * 6,
    }

def check_climax_reversal(candles, entry_price):
    if len(candles) < 21:
        return None

    current_candle = candles[-1]
    prev_candles = candles[-21:-1]

    avg_volume = sum(c["volume"] for c in prev_candles) / len(prev_candles) if prev_candles else 0
    if avg_volume <= 0:
        return None

    current_rvol = current_candle["volume"] / avg_volume
    open_p, close_p, high_p, low_p = current_candle["open"], current_candle["close"], current_candle["high"], current_candle["low"]

    candle_range = high_p - low_p
    if candle_range <= 0:
        return None

    upper_wick = high_p - max(open_p, close_p)
    wick_ratio = upper_wick / candle_range
    profit_pct = ((high_p - entry_price) / entry_price) * 100

    if (
        profit_pct >= CLIMAX_MIN_PROFIT_PCT
        and current_rvol >= CLIMAX_RVOL_THRESHOLD
        and wick_ratio >= CLIMAX_MIN_WICK_RATIO
    ):
        return {
            "profit_pct": round(profit_pct, 2),
            "climax_rvol": round(current_rvol, 2),
            "wick_ratio_pct": round(wick_ratio * 100, 1),
            "exit_price": close_p,
            "short_stop_loss": high_p * 1.003,
        }
    return None

# ============================================================
# TELEGRAM
# ============================================================

async def send_tg_to(chat_id, text):
    if not BOT_TOKEN or not chat_id or SESSION is None:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                STATS["tg_send_failures"] += 1
                return False
            payload = await r.json(content_type=None)
            return payload.get("ok", False)
    except Exception:
        STATS["tg_send_failures"] += 1
        return False

async def send_tg(text):
    return await send_tg_to(CHAT_ID, text)

async def send_cumulative_signal(base, data):
    ticker = f"<code>{escape(base)}USDT</code>"
    msg = (
        f"🧊 <b>ТИХОЕ НАКОПЛЕНИЕ v12.0</b>\n\n"
        f"📌 <b>Монета:</b> {ticker}\n"
        f"📐 <b>Полка (1.5h):</b> <b>{data['shelf_range_pct']}%</b> (позиция: {data['shelf_position']}%)\n"
        f"📊 <b>Кумулятивный RVOL:</b> <b>{data['cumulative_rvol']}x</b>\n"
        f"🟢 <b>Доля выкупа ММ:</b> <b>{data['green_ratio_pct']}%</b>\n\n"
        f"🎯 <b>Вход (Лимитка):</b> <code>{data['entry']:.6g}</code>\n"
        f"🛑 <b>Стоп-лосс:</b> <code>{data['stop_loss']:.6g}</code>\n"
        f"🚀 <b>Тейк 1 (3R):</b> <code>{data['target_3r']:.6g}</code>\n"
        f"🚀 <b>Тейк 2 (6R):</b> <code>{data['target_6r']:.6g}</code>\n\n"
        f"⚠️ <i>Локальный поджим верх. Взято на сопровождение.</i>"
    )
    sent = await send_tg(msg)
    if sent:
        STATS["cumulative_signals"] += 1
    return sent

async def send_climax_reversal_signal(base, data):
    ticker = f"<code>{escape(base)}USDT</code>"
    msg = (
        f"🥊 <b>КУЛЬМИНАЦИЯ ПОКУПОК / СИГНАЛ РАЗВОРОТА</b>\n\n"
        f"📌 <b>Монета:</b> {ticker}\n"
        f"💰 <b>Пиковый профит:</b> <b>+{data['profit_pct']}%</b>\n"
        f"📊 <b>RVOL выгрузки:</b> <b>{data['climax_rvol']}x</b>\n"
        f"🩸 <b>Верхняя тень:</b> <b>{data['wick_ratio_pct']}%</b>\n\n"
        f"🛑 <b>ЗАКРЫТЬ ЛОНГ:</b> <code>{data['exit_price']:.6g}</code>\n\n"
        f"📉 <b>РАССМОТРЕТЬ ШОРТ:</b>\n"
        f"  Вход: <code>{data['exit_price']:.6g}</code>\n"
        f"  Стоп: <code>{data['short_stop_loss']:.6g}</code>"
    )
    sent = await send_tg(msg)
    if sent:
        STATS["climax_reversals"] += 1
    return sent

async def send_status_message(target_chat_id=None):
    uptime_hours = (time.time() - START_TIME) / 3600
    btc_status = "OK" if BTC_STATE["ok"] else f"❌ DUMP ({BTC_STATE['change_30m_pct']:+.2f}%)"
    msg = (
        f"📊 <b>Статус PUMP-HUNTER v12.0 OPTIMIZED</b>\n\n"
        f"⏱ Аптайм: {uptime_hours:.1f}ч\n"
        f"🌐 Юниверс: {len(UNIVERSE)} пар\n"
        f"🔍 Сканов новых: {STATS['scans']}\n"
        f"👁 Проверок треков: {STATS['track_scans']}\n"
        f"🧊 Сигналов накопления: {STATS['cumulative_signals']}\n"
        f"🥊 Разворотов: {STATS['climax_reversals']}\n"
        f"📌 Активных треков: {len(ACTIVE_TRACKS)} / {MAX_ACTIVE_TRACKS}\n\n"
        f"<b>BTC Filter:</b> {btc_status}\n\n"
        f"<b>Отсевы накопления:</b>\n"
        f"  широкая полка: {STATS['rejected_shelf_range']}\n"
        f"  сползание на дно: {STATS['rejected_sagging_low']}\n"
        f"  слабый RVOL: {STATS['rejected_cumulative_rvol']}\n"
        f"  слабый выкуп: {STATS['rejected_green_ratio']}\n\n"
        f"<b>Отсевы тренда:</b>\n"
        f"  24h упал: {STATS['rejected_trend_24h']}\n"
        f"  lower highs: {STATS['rejected_trend_lower_highs']}\n"
        f"  BTC macro: {STATS['rejected_btc_macro']}"
    )
    dest = target_chat_id or CHAT_ID
    if dest:
        await send_tg_to(dest, msg)

async def handle_telegram_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return
    sender_chat_id = str(msg.get("chat", {}).get("id", ""))
    text = (msg.get("text") or "").strip()
    if not text:
        return
    STATS["tg_commands_received"] += 1

    lower = text.lower()
    if lower.startswith("/start"):
        await send_tg_to(sender_chat_id, "👋 <b>PUMP-HUNTER v12.0</b>\n/status — отчёт")
    elif lower.startswith("/status"):
        await send_status_message(target_chat_id=sender_chat_id)

async def telegram_poll_loop():
    if not BOT_TOKEN:
        return
    offset = None
    while True:
        try:
            if SESSION is None or SESSION.closed:
                await asyncio.sleep(2)
                continue
            params = {"timeout": 25, "allowed_updates": ALLOWED_UPDATES}
            if offset is not None:
                params["offset"] = offset
            async with SESSION.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params=params, timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status != 200:
                    STATS["tg_poll_err"] += 1
                    await asyncio.sleep(3)
                    continue
                data = await r.json(content_type=None)
                if not data or not data.get("ok"):
                    STATS["tg_poll_err"] += 1
                    await asyncio.sleep(3)
                    continue
                STATS["tg_poll_ok"] += 1
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    try:
                        await handle_telegram_update(update)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            STATS["tg_poll_err"] += 1
            await asyncio.sleep(5)

# ============================================================
# РАЗДЕЛЁННЫЕ ЦИКЛЫ
# ============================================================

async def scan_new_symbol(base, semaphore):
    async with semaphore:
        if base in ACTIVE_TRACKS:
            return
        if time.time() - LAST_SIGNAL.get(base, 0) < CUMULATIVE_COOLDOWN_SEC:
            return

        item = UNIVERSE.get(base)
        if not item:
            return

        kc15 = await fetch_kucoin_candles_15m(
            item["kucoin_symbol"],
            SHELF_LOOKBACK_CANDLES + SHELF_HIST_LOOKBACK + 5,
        )
        if not kc15:
            return

        accum = check_cumulative_rvol_accumulation(kc15, item)
        if accum:
            if len(ACTIVE_TRACKS) >= MAX_ACTIVE_TRACKS:
                STATS["tracks_skipped_max"] += 1
                return

            LAST_SIGNAL[base] = time.time()
            ACTIVE_TRACKS[base] = {"entry": accum["entry"], "ts": time.time()}
            await send_cumulative_signal(base, accum)
            log.info("🧊 НАКОПЛЕНИЕ: %s | RVOL %.2fx | Range %.2f%%", base, accum["cumulative_rvol"], accum["shelf_range_pct"])

async def check_track_symbol(base, semaphore):
    async with semaphore:
        track = ACTIVE_TRACKS.get(base)
        if not track:
            return
        item = UNIVERSE.get(base)
        if not item:
            ACTIVE_TRACKS.pop(base, None)
            return

        kc15 = await fetch_kucoin_candles_15m(item["kucoin_symbol"], 30)
        if not kc15:
            return

        now = time.time()
        if now - track["ts"] > CLIMAX_TRACK_TIMEOUT_SEC:
            ACTIVE_TRACKS.pop(base, None)
            STATS["tracks_timed_out"] += 1
            return

        current_price = kc15[-1]["close"]
        change_from_entry = (current_price - track["entry"]) / track["entry"] * 100
        if change_from_entry <= CLIMAX_INVALIDATE_PCT:
            ACTIVE_TRACKS.pop(base, None)
            STATS["tracks_invalidated"] += 1
            return

        climax = check_climax_reversal(kc15, track["entry"])
        if climax:
            await send_climax_reversal_signal(base, climax)
            ACTIVE_TRACKS.pop(base, None)

async def scan_new_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    all_bases = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    new_bases = [b for b in all_bases if b not in ACTIVE_TRACKS]

    semaphore = asyncio.Semaphore(15)
    await asyncio.gather(*[scan_new_symbol(b, semaphore) for b in new_bases])
    gc.collect()

async def scan_tracks_cycle():
    tracks = list(ACTIVE_TRACKS.keys())
    if not tracks:
        return
    STATS["track_scans"] += 1
    semaphore = asyncio.Semaphore(10)
    await asyncio.gather(*[check_track_symbol(b, semaphore) for b in tracks])
    gc.collect()

# ============================================================
# BACKGROUND WORKERS
# ============================================================

async def refresh_universe():
    global UNIVERSE, LAST_SIGNAL
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    if not kc or not bg:
        return

    bg_set = set(bg.keys())
    now_ms = time.time() * 1000
    min_age_ms = MIN_LISTING_AGE_DAYS * 86400 * 1000
    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT or not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bg_set:
            continue
        first_open = info.get("first_open_ms", 0)
        if first_open > 0 and (now_ms - first_open) < min_age_ms:
            continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }

    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])

    LAST_SIGNAL = {k: v for k, v in LAST_SIGNAL.items() if k in UNIVERSE}
    for b in list(ACTIVE_TRACKS.keys()):
        if b not in UNIVERSE:
            ACTIVE_TRACKS.pop(b, None)

    log.info("🌐 Юниверс обновлён: %d пар", len(UNIVERSE))
    gc.collect()

async def universe_loop():
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            await refresh_universe()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(10)

async def btc_state_loop():
    while True:
        try:
            await refresh_btc_state()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(BTC_STATE_REFRESH_SEC)

async def scan_new_loop():
    while True:
        try:
            if UNIVERSE and BTC_STATE["ok"]:
                await scan_new_cycle()
            elif UNIVERSE and not BTC_STATE["ok"]:
                STATS["rejected_btc_macro"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(SCAN_INTERVAL_SEC)

async def scan_tracks_loop():
    while True:
        try:
            if ACTIVE_TRACKS:
                await scan_tracks_cycle()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(TRACK_INTERVAL_SEC)

# ============================================================
# WEB
# ============================================================

async def index(req):
    return web.Response(
        text=(f"PUMP-HUNTER v12.0 OPTIMIZED | Uni: {len(UNIVERSE)} | "
              f"Signals: {STATS['cumulative_signals']} | "
              f"BTC: {'OK' if BTC_STATE['ok'] else 'BLOCK'}"),
        content_type="text/plain",
    )

async def start(app):
    global SESSION, HTTP_SEMAPHORE, START_TIME
    START_TIME = time.time()
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(20)

    app["poll_task"] = asyncio.create_task(telegram_poll_loop())
    app["universe_task"] = asyncio.create_task(universe_loop())
    app["btc_task"] = asyncio.create_task(btc_state_loop())
    app["scan_new_task"] = asyncio.create_task(scan_new_loop())
    app["scan_tracks_task"] = asyncio.create_task(scan_tracks_loop())

    await refresh_universe()
    await refresh_btc_state()

    await send_tg(
        "🚀 <b>PUMP-HUNTER v12.0 OPTIMIZED запущен!</b>\n\n"
        f"• Окно полки: 1.5 часа (6 свечей)\n"
        f"• Фильтр полки: Вход >= 50% (Anti-Sagging)\n"
        f"• Медвежий EMA-фильтр: ОТКЛЮЧЕН (разрешен подбор дна)\n"
        f"• BTC Предохранитель: 30m Dump Check"
    )

async def stop(app):
    for key in ("universe_task", "scan_new_task", "scan_tracks_task", "btc_task", "poll_task"):
        t = app.get(key)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    if SESSION and not SESSION.closed:
        await SESSION.close()

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(start)
app.on_cleanup.append(stop)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
