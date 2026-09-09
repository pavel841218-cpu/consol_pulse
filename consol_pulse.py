import os
import time
import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone

import aiohttp
from aiohttp import web


# ============================================================
# 🏹 ПАРТИЗАН v6.3 — 1H SHELF & FIX KLINE HISTORY
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get(
    "BOT_TOKEN",
    os.environ.get("TELEGRAM_TOKEN", "")
)

CHAT_ID = os.environ.get(
    "CHAT_ID",
    os.environ.get("TELEGRAM_CHAT_ID", "")
)

PORT = int(os.environ.get("PORT", "10000"))
SELF_URL = os.environ.get("SELF_URL", "")

BINGX_BASE_URL = "https://open-api.bingx.com"

TIMEFRAME = "1h"
KLINE_LIMIT = 500  # Запрашиваем с запасом, чтобы гарантированно получить историю

CHECK_INTERVAL_SECONDS = 30
MAX_CONCURRENT_REQUESTS = 10
SESSION_TIMEOUT = 15

# ------------------------------------------------------------
# РЫНОЧНЫЙ И ЦЕНОВОЙ ФИЛЬТР
# ------------------------------------------------------------

MIN_24H_VOLUME_USDT = 1_000_000

# Диапазон цен (0.0001 - 1.0 USDT)
MIN_PRICE_USDT = 0.0001
MAX_PRICE_USDT = 1.0

EXCLUDED_SYMBOLS = {"USDC", "FDUSD", "USD1", "USDT", "USDE", "TUSD"}
BLACKLIST = {"IRIS-USDT", "IRYS-USDT", "LUNC-USDT", "USTC-USDT"}

# ------------------------------------------------------------
# ПОЛКА И EMA
# ------------------------------------------------------------

SHELF_MIN_CANDLES = 5
SHELF_MAX_CANDLES = 12
MAX_SHELF_WIDTH_PCT = 4.5

# Глубина поиска окончаний полок в истории (в свечах назад)
SHELF_SEARCH_DEPTH = 35 

EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80

EMA_INSIDE_TOLERANCE_PCT = 0.50
EMA80_MAX_DISTANCE_PCT = 2.5

EMA_TOUCH_TOLERANCE_PCT = 1.0
MIN_EMA_INTERACTIONS = 2

# ------------------------------------------------------------
# ИМПУЛЬС
# ------------------------------------------------------------

BREAKOUT_TARGET_PCT = 4.0
BREAKOUT_MAX_CANDLES = 4
FIRST_BREAKOUT_MIN_CLOSE_PCT = 0.15
BREAKOUT_INVALIDATION_PCT = 1.5

OLD_BREAKOUT_LOOKBACK = 5
OLD_BREAKOUT_MIN_PCT = 2.0

RVOL_LOOKBACK = 20
OI_HISTORY_SIZE = 30
ALERT_COOLDOWN_SECONDS = 6 * 3600

DEBUG = os.environ.get("DEBUG", "0") == "1"


# ============================================================
# LOGGING & GLOBALS
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("PARТIZAN_v6.3")

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
last_alert_time = {}
oi_history = defaultdict(lambda: deque(maxlen=OI_HISTORY_SIZE))

stats = {
    "scans": 0,
    "symbols": 0,
    "has_enough_candles": 0,
    "shelves_eval": 0,
    "pass_width": 0,
    "pass_ema20": 0,
    "pass_ema40": 0,
    "pass_ema80": 0,
    "pass_interaction": 0,
    "pass_old_breakout": 0,
    "pass_impulse": 0,
    "pass_target": 0,
    "signals": 0,
    "errors": 0,
}


# ============================================================
# UTILS
# ============================================================

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default

def normalize_symbol(symbol):
    symbol = str(symbol).upper().strip()
    if symbol.endswith("-USDT"):
        return symbol
    if symbol.endswith("USDT"):
        return symbol[:-4] + "-USDT"
    return symbol

def base_symbol(symbol):
    return normalize_symbol(symbol).replace("-USDT", "")

def body_high(candle):
    return max(candle["open"], candle["close"])

def body_low(candle):
    return min(candle["open"], candle["close"])


# ============================================================
# EMA & KLINES
# ============================================================

def calculate_ema(values, period):
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    ema = [values[0]]
    for value in values[1:]:
        ema.append(alpha * value + (1.0 - alpha) * ema[-1])
    return ema

def parse_kline(k):
    # Если BingX отдаёт словарь или список
    if isinstance(k, dict):
        return {
            "ts": int(k.get("time", k.get("timestamp", 0))),
            "open": safe_float(k.get("open")),
            "high": safe_float(k.get("high")),
            "low": safe_float(k.get("low")),
            "close": safe_float(k.get("close")),
            "volume": safe_float(k.get("volume")),
        }
    return {
        "ts": int(k[0]),
        "open": safe_float(k[1]),
        "high": safe_float(k[2]),
        "low": safe_float(k[3]),
        "close": safe_float(k[4]),
        "volume": safe_float(k[5]),
    }


# ============================================================
# HTTP & API
# ============================================================

async def http_get(session, path, params=None):
    url = BINGX_BASE_URL + path
    async with semaphore:
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=SESSION_TIMEOUT)
            ) as response:
                if response.status != 200:
                    return None
                return await response.json()
        except Exception:
            return None

async def fetch_contracts(session):
    data = await http_get(session, "/openApi/swap/v2/quote/contracts")
    if not data:
        return []
    rows = data.get("data", [])
    result = []
    for row in rows:
        symbol = normalize_symbol(row.get("symbol", ""))
        if not symbol.endswith("-USDT") or base_symbol(symbol) in EXCLUDED_SYMBOLS or symbol in BLACKLIST:
            continue
        result.append(symbol)
    return result

async def fetch_tickers(session):
    data = await http_get(session, "/openApi/swap/v2/quote/ticker")
    if not data:
        return {}
    rows = data.get("data", [])
    result = {}
    for row in rows:
        symbol = normalize_symbol(row.get("symbol", ""))
        if not symbol.endswith("-USDT") or symbol in BLACKLIST or base_symbol(symbol) in EXCLUDED_SYMBOLS:
            continue

        price = safe_float(row.get("lastPrice"))
        volume = safe_float(row.get("quoteVolume"))

        # Фильтр по ценовому диапазону и объёму
        if not (MIN_PRICE_USDT <= price <= MAX_PRICE_USDT):
            continue

        if volume < MIN_24H_VOLUME_USDT:
            continue

        result[symbol] = {
            "price": price,
            "volume24h": volume,
            "change24h": safe_float(row.get("priceChangePercent")),
        }
    return result

async def fetch_klines(session, symbol):
    data = await http_get(
        session,
        "/openApi/swap/v3/quote/klines",
        {"symbol": symbol, "interval": TIMEFRAME, "limit": KLINE_LIMIT}
    )
    if not data or not isinstance(data, dict):
        return []
    
    raw = data.get("data", [])
    if not raw:
        return []

    candles = [parse_kline(row) for row in raw]
    # Сортируем строго хронологически (от старых к новым)
    candles.sort(key=lambda x: x["ts"])
    return candles

async def fetch_open_interest(session, symbol):
    try:
        data = await http_get(session, "/openApi/swap/v2/quote/openInterest", {"symbol": symbol})
        if not data:
            return None
        val = data.get("data")
        if isinstance(val, dict):
            val = val.get("openInterest") or val.get("openInterestValue")
        return safe_float(val, None)
    except Exception:
        return None

def calculate_rvol(candles, index):
    if index <= 0:
        return 0.0
    start = max(0, index - RVOL_LOOKBACK)
    previous = candles[start:index]
    if not previous:
        return 0.0
    avg_vol = sum(c["volume"] for c in previous) / len(previous)
    return candles[index]["volume"] / avg_vol if avg_vol > 0 else 0.0


# ============================================================
# ANALYSIS LOGIC
# ============================================================

def build_shelf(candles, start, end):
    shelf = candles[start:end]
    if len(shelf) < SHELF_MIN_CANDLES:
        return None

    top = max(body_high(c) for c in shelf)
    bottom = min(body_low(c) for c in shelf)

    if top <= 0:
        return None

    width_pct = (top - bottom) / top * 100.0
    if width_pct > MAX_SHELF_WIDTH_PCT:
        return None

    return {
        "start": start,
        "end": end,
        "length": len(shelf),
        "top": top,
        "bottom": bottom,
        "width_pct": width_pct,
    }

def evaluate_ema_structure(candles, shelf, ema20, ema40, ema80):
    start, end = shelf["start"], shelf["end"]
    top, bottom = shelf["top"], shelf["bottom"]
    shelf_len = end - start

    if shelf_len <= 0:
        return None

    inside20, inside40 = 0, 0
    ema80_distances = []

    for i in range(start, end):
        e20, e40, e80 = ema20[i], ema40[i], ema80[i]
        tolerance = top * (EMA_INSIDE_TOLERANCE_PCT / 100.0)

        if bottom - tolerance <= e20 <= top + tolerance:
            inside20 += 1
        if bottom - tolerance <= e40 <= top + tolerance:
            inside40 += 1

        if e80 < bottom:
            dist = (bottom - e80) / bottom * 100.0
        elif e80 > top:
            dist = (e80 - top) / top * 100.0
        else:
            dist = 0.0
        ema80_distances.append(dist)

    return {
        "inside20_pct": (inside20 / shelf_len) * 100.0,
        "inside40_pct": (inside40 / shelf_len) * 100.0,
        "ema80_distance_pct": sum(ema80_distances) / len(ema80_distances) if ema80_distances else 999.0,
    }

def evaluate_ema_interaction(candles, shelf, ema20, ema40, ema80):
    start, end = shelf["start"], shelf["end"]
    interactions, bullish_reactions = 0, 0

    for i in range(start, end):
        candle = candles[i]
        zone_low = min(ema20[i], ema40[i], ema80[i])
        zone_high = max(ema20[i], ema40[i], ema80[i])

        tol_low = zone_low * (1.0 - EMA_TOUCH_TOLERANCE_PCT / 100.0)
        tol_high = zone_high * (1.0 + EMA_TOUCH_TOLERANCE_PCT / 100.0)

        if candle["low"] <= tol_high and candle["high"] >= tol_low:
            interactions += 1
            if i + 1 < end and candles[i + 1]["close"] > candle["close"]:
                bullish_reactions += 1

    return {"interactions": interactions, "bullish_reactions": bullish_reactions}

def has_old_breakout(candles, shelf):
    start, top = shelf["start"], shelf["top"]
    previous = candles[max(0, start - OLD_BREAKOUT_LOOKBACK):start]
    for candle in previous:
        if candle["close"] >= top * (1.0 + OLD_BREAKOUT_MIN_PCT / 100.0):
            return True
    return False

def evaluate_first_impulse(candles, shelf):
    end, top = shelf["end"], shelf["top"]
    post = candles[end:min(len(candles), end + BREAKOUT_MAX_CANDLES)]

    if not post:
        return None

    if post[0]["close"] < top * (1.0 + FIRST_BREAKOUT_MIN_CLOSE_PCT / 100.0):
        return None

    target = top * (1.0 + BREAKOUT_TARGET_PCT / 100.0)
    invalidation = top * (1.0 - BREAKOUT_INVALIDATION_PCT / 100.0)

    for i, candle in enumerate(post):
        if candle["close"] < invalidation:
            return {"status": "invalidated"}
        if candle["high"] >= target:
            return {
                "status": "target",
                "candles": i + 1,
                "target_pct": BREAKOUT_TARGET_PCT,
                "target_price": target,
                "high": candle["high"],
            }

    return {"status": "not_reached"}


# ============================================================
# SEARCH PIPELINE
# ============================================================

def find_best_shelf(candles):
    # Достаточно 80 свечей для расчета EMA80 и структуры
    if len(candles) < 80:
        return None

    closes = [c["close"] for c in candles]
    ema20 = calculate_ema(closes, EMA_FAST)
    ema40 = calculate_ema(closes, EMA_MID)
    ema80 = calculate_ema(closes, EMA_SLOW)

    total_closed = len(candles)
    candidates = []

    min_end = max(EMA_SLOW + SHELF_MIN_CANDLES, total_closed - SHELF_SEARCH_DEPTH)
    
    for end in range(total_closed - 1, min_end, -1):
        for length in range(SHELF_MAX_CANDLES, SHELF_MIN_CANDLES - 1, -1):
            start = end - length
            if start < EMA_SLOW:
                continue

            stats["shelves_eval"] += 1

            shelf = build_shelf(candles, start, end)
            if not shelf:
                continue
            stats["pass_width"] += 1

            ema_data = evaluate_ema_structure(candles, shelf, ema20, ema40, ema80)
            if not ema_data or ema_data["inside20_pct"] < 60.0:
                continue
            stats["pass_ema20"] += 1

            if ema_data["inside40_pct"] < 60.0:
                continue
            stats["pass_ema40"] += 1

            if ema_data["ema80_distance_pct"] > EMA80_MAX_DISTANCE_PCT:
                continue
            stats["pass_ema80"] += 1

            interaction = evaluate_ema_interaction(candles, shelf, ema20, ema40, ema80)
            if interaction["interactions"] < MIN_EMA_INTERACTIONS:
                continue
            stats["pass_interaction"] += 1

            if has_old_breakout(candles, shelf):
                continue
            stats["pass_old_breakout"] += 1

            impulse = evaluate_first_impulse(candles, shelf)
            if not impulse or impulse["status"] in ("invalidated", "not_reached"):
                continue
            stats["pass_impulse"] += 1

            if impulse["status"] == "target":
                stats["pass_target"] += 1
                candidates.append({
                    "shelf": shelf,
                    "ema": ema_data,
                    "interaction": interaction,
                    "impulse": impulse,
                })

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["shelf"]["end"], -x["shelf"]["length"]), reverse=True)
    return candidates[0]


# ============================================================
# TELEGRAM & MESSAGES
# ============================================================

async def send_telegram(session, message, parse_mode="HTML"):
    if not BOT_TOKEN or not CHAT_ID:
        logger.error("BOT_TOKEN или CHAT_ID не заданы.")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }

    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error("Ошибка отправки в Telegram: %s", e)
        return False

def build_signal_message(symbol, ticker, candidate, rvol, oi, oi_growth):
    shelf = candidate["shelf"]
    ema = candidate["ema"]
    interaction = candidate["interaction"]
    impulse = candidate["impulse"]

    price = ticker.get("price", 0.0)
    top = shelf["top"]
    bottom = shelf["bottom"]
    current_change = ((price - top) / top * 100.0) if top > 0 else 0.0

    clean_symbol = symbol.replace("-USDT", "")
    
    msg = (
        f"🏹 <b>ПАРТИЗАН v6.3 — ПЕРВЫЙ ИМПУЛЬС</b>\n\n"
        f"🪙 <b>Монета:</b> <code>{symbol}</code> (<code>{clean_symbol}</code>)\n"
        f"💰 <b>Цена:</b> {price:.8g}\n\n"
        f"📦 <b>ПОЛКА</b>\n"
        f"   Нижняя: {bottom:.8g}\n"
        f"   Верхняя: {top:.8g}\n"
        f"   Ширина: {shelf['width_pct']:.2f}%\n"
        f"   Свечей: {shelf['length']}\n\n"
        f"📈 <b>EMA</b>\n"
        f"   EMA20 внутри: {ema['inside20_pct']:.0f}%\n"
        f"   EMA40 внутри: {ema['inside40_pct']:.0f}%\n"
        f"   EMA80 дистанция: {ema['ema80_distance_pct']:.2f}%\n\n"
        f"🔄 <b>ВЗАИМОДЕЙСТВИЕ</b>\n"
        f"   Касаний EMA-зоны: {interaction['interactions']}\n"
        f"   Бычьих реакций: {interaction['bullish_reactions']}\n\n"
        f"🚀 <b>ИМПУЛЬС</b>\n"
        f"   Свечей: {impulse['candles']}\n"
        f"   Цель: +{BREAKOUT_TARGET_PCT:.1f}%\n"
        f"   От верха полки: {current_change:+.2f}%\n\n"
        f"📊 <b>ИНФОРМАЦИЯ</b>\n"
        f"   RVOL: {rvol:.2f}x\n"
    )

    if oi is not None:
        msg += f"   OI: {oi:.2f}\n"
    if oi_growth is not None:
        msg += f"   OI изменение: {oi_growth:+.2f}%\n"

    msg += (
        f"   24h объём: ${ticker.get('volume24h', 0):,.0f}\n\n"
        f"🎯 Условие: первый импульс достиг +{BREAKOUT_TARGET_PCT:.0f}% от верха полки.\n"
        f"⚠️ RVOL/OI не являются фильтрами."
    )
    return msg


# ============================================================
# EVALUATION & SCANNER
# ============================================================

async def evaluate_symbol(session, symbol, ticker):
    try:
        candles = await fetch_klines(session, symbol)
        
        if len(candles) < 85:
            return None

        closed = candles[:-1]
        if len(closed) < 80:
            return None

        stats["has_enough_candles"] += 1

        candidate = find_best_shelf(closed)
        if not candidate:
            return None

        if time.time() - last_alert_time.get(symbol, 0) < ALERT_COOLDOWN_SECONDS:
            return None

        impulse_end = candidate["shelf"]["end"] + candidate["impulse"]["candles"] - 1
        rvol = calculate_rvol(closed, min(impulse_end, len(closed) - 1))

        oi, oi_growth = None, None
        raw_oi = await fetch_open_interest(session, symbol)
        if raw_oi is not None:
            now = time.time()
            history = oi_history[symbol]
            history.append((now, raw_oi))
            oi = raw_oi
            if len(history) >= 2 and history[0][1] > 0:
                oi_growth = (oi - history[0][1]) / history[0][1] * 100.0

        return {
            "symbol": symbol,
            "ticker": ticker,
            "candidate": candidate,
            "rvol": rvol,
            "oi": oi,
            "oi_growth": oi_growth,
        }
    except Exception as e:
        stats["errors"] += 1
        return None

def log_stats(symbols_count, signal_count):
    logger.info(
        "📊 СКАН #%d | Пар=%d | Ок свечей=%d | Проверок полок=%d | "
        "Ширина=%d | EMA20=%d | EMA40=%d | EMA80=%d | "
        "Касания=%d | Без старого пампа=%d | Импульс=%d | Цель +4%%=%d | СИГНАЛЫ=%d",
        stats["scans"],
        symbols_count,
        stats["has_enough_candles"],
        stats["shelves_eval"],
        stats["pass_width"],
        stats["pass_ema20"],
        stats["pass_ema40"],
        stats["pass_ema80"],
        stats["pass_interaction"],
        stats["pass_old_breakout"],
        stats["pass_impulse"],
        stats["pass_target"],
        signal_count
    )

async def scan_market(session):
    contracts = await fetch_contracts(session)
    tickers = await fetch_tickers(session)

    if not contracts or not tickers:
        logger.warning("Не удалось получить contracts/tickers от BingX.")
        return

    symbols = [s for s in contracts if s in tickers and s not in BLACKLIST]
    stats["symbols"] = len(symbols)

    logger.info("🔎 Сканирование %d пар (цена: %.4f - %.1f USDT)...", len(symbols), MIN_PRICE_USDT, MAX_PRICE_USDT)

    tasks = [
        evaluate_symbol(session, sym, tickers[sym])
        for sym in symbols
        if time.time() - last_alert_time.get(sym, 0) >= ALERT_COOLDOWN_SECONDS
    ]

    if not tasks:
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)
    signal_count = 0

    for res in results:
        if isinstance(res, Exception) or not res:
            continue

        symbol = res["symbol"]
        message = build_signal_message(
            symbol, res["ticker"], res["candidate"], res["rvol"], res["oi"], res["oi_growth"]
        )

        if await send_telegram(session, message):
            last_alert_time[symbol] = time.time()
            signal_count += 1
            stats["signals"] += 1
            logger.info("🎯 СИГНАЛ ОТПРАВЛЕН: %s", symbol)

    log_stats(len(symbols), signal_count)


# ============================================================
# SERVER & MAIN
# ============================================================

async def health(request):
    return web.Response(text="ПАРТИЗАН v6.3 OK")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("🌐 Web-сервер запущен на порту %d", PORT)

async def main():
    logger.info("🏹 Запуск ПАРТИЗАН v6.3...")
    await start_web_server()

    timeout = aiohttp.ClientTimeout(total=SESSION_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        welcome_text = (
            "🚀 <b>ПАРТИЗАН v6.3 запущен и готов к работе!</b>\n\n"
            f"🎯 <b>Фильтр цен:</b> {MIN_PRICE_USDT} - {MAX_PRICE_USDT} USDT\n"
            "🔍 <i>Сканирование рынка начато. Ожидайте сигналов пробоя полок.</i>"
        )
        await send_telegram(session, welcome_text)

        while True:
            stats["scans"] += 1
            start_time = time.time()

            try:
                await scan_market(session)
            except Exception as e:
                stats["errors"] += 1
                logger.exception("Ошибка цикла сканирования: %s", e)

            elapsed = time.time() - start_time
            await asyncio.sleep(max(1, CHECK_INTERVAL_SECONDS - elapsed))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("ПАРТИЗАН остановлен.")
