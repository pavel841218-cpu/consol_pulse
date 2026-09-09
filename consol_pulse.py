import os
import time
import asyncio
import logging
from collections import defaultdict, deque

import aiohttp
from aiohttp import web


# ============================================================
# 🏹 ПАРТИЗАН v6.5.2 — REAL-TIME BREAKOUT & DETAILED LOGGING
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", os.environ.get("TELEGRAM_TOKEN", ""))
CHAT_ID = os.environ.get("CHAT_ID", os.environ.get("TELEGRAM_CHAT_ID", ""))
PORT = int(os.environ.get("PORT", "10000"))

BINGX_BASE_URL = "https://open-api.bingx.com"

TIMEFRAME = "1h"
KLINE_LIMIT = 500

CHECK_INTERVAL_SECONDS = 30
MAX_CONCURRENT_REQUESTS = 10
SESSION_TIMEOUT = 15

# РЫНОЧНЫЙ И ЦЕНОВОЙ ФИЛЬТР
MIN_24H_VOLUME_USDT = 1_000_000
MIN_PRICE_USDT = 0.0001
MAX_PRICE_USDT = 1.0

EXCLUDED_SYMBOLS = {"USDC", "FDUSD", "USD1", "USDT", "USDE", "TUSD"}
BLACKLIST = {"IRIS-USDT", "IRYS-USDT", "LUNC-USDT", "USTC-USDT"}

# ПОЛКА И EMA
SHELF_MIN_CANDLES = 5
SHELF_MAX_CANDLES = 12
MAX_SHELF_WIDTH_PCT = 3.5          

EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80

EMA_INSIDE_TOLERANCE_PCT = 0.50
EMA80_MAX_DISTANCE_PCT = 1.5       

EMA_TOUCH_TOLERANCE_PCT = 1.0
MIN_EMA_INTERACTIONS = 2

# ИМПУЛЬС И ОБЪЕМ
BREAKOUT_TARGET_PCT = 4.0          # Цель в 4%
BREAKOUT_MAX_CANDLES = 2          # Только свежий импульс (1-2 свечи)
FIRST_BREAKOUT_MIN_CLOSE_PCT = 0.20
MIN_RVOL = 1.4                    # Настоящий всплеск объема

# ФИЛЬТР МАРКЕТ-МЕЙКЕРОВ (АНТИ-СКВИЗ / АНТИ-СПУФИНГ)
MAX_UPPER_WICK_RATIO = 2.0         # Верхний фитиль не больше 2x от тела
MIN_BODY_PCT = 0.3                # Минимальное тело свечи пробоя
MAX_VOL_TO_OI_RATIO = 3.0         # Защита от накрутки объема

OLD_BREAKOUT_LOOKBACK = 5
OLD_BREAKOUT_MIN_PCT = 2.0

RVOL_LOOKBACK = 20
OI_HISTORY_SIZE = 30
ALERT_COOLDOWN_SECONDS = 6 * 3600


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("PARTIZAN_v6.5")

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
last_alert_time = {}
oi_history = defaultdict(lambda: deque(maxlen=OI_HISTORY_SIZE))

stats = defaultdict(int)

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

def calculate_ema(values, period):
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    ema = [values[0]]
    for value in values[1:]:
        ema.append(alpha * value + (1.0 - alpha) * ema[-1])
    return ema

def parse_kline(k):
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

async def http_get(session, path, params=None):
    url = BINGX_BASE_URL + path
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=SESSION_TIMEOUT)) as response:
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
        if not (MIN_PRICE_USDT <= price <= MAX_PRICE_USDT) or volume < MIN_24H_VOLUME_USDT:
            continue
        result[symbol] = {"price": price, "volume24h": volume, "change24h": safe_float(row.get("priceChangePercent"))}
    return result

async def fetch_klines(session, symbol):
    data = await http_get(session, "/openApi/swap/v3/quote/klines", {"symbol": symbol, "interval": TIMEFRAME, "limit": KLINE_LIMIT})
    if not data or not isinstance(data, dict):
        return []
    raw = data.get("data", [])
    if not raw:
        return []
    candles = [parse_kline(row) for row in raw]
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
    return {"start": start, "end": end, "length": len(shelf), "top": top, "bottom": bottom, "width_pct": width_pct}

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

        dist = abs(top - e80) / top * 100.0 if e80 > top else (abs(bottom - e80) / bottom * 100.0 if e80 < bottom else 0.0)
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
    total_candles = len(candles)

    # ИМПУЛЬС ДОЛЖЕН БЫТЬ СВЕЖИМ: полка должна заканчиваться ровно перед последними 1-2 свечами
    if total_candles - end > BREAKOUT_MAX_CANDLES:
        return None

    post = candles[end:total_candles]
    if not post:
        return None

    if post[0]["close"] < top * (1.0 + FIRST_BREAKOUT_MIN_CLOSE_PCT / 100.0):
        return None

    target = top * (1.0 + BREAKOUT_TARGET_PCT / 100.0)
    for i, candle in enumerate(post):
        if candle["high"] >= target:
            return {
                "status": "target",
                "candles": i + 1,
                "target_pct": BREAKOUT_TARGET_PCT,
                "target_price": target,
                "high": candle["high"],
                "breakout_candle": candle
            }
    return None

def is_market_maker_noise(breakout_candle, open_interest=None):
    open_p = breakout_candle["open"]
    close_p = breakout_candle["close"]
    high_p = breakout_candle["high"]
    
    if open_p <= 0:
        return True

    body = abs(close_p - open_p)
    body_pct = (body / open_p) * 100.0
    upper_wick = high_p - max(open_p, close_p)

    if body_pct < MIN_BODY_PCT:
        return True

    if body > 0 and (upper_wick / body) > MAX_UPPER_WICK_RATIO:
        return True

    if open_interest and open_interest > 0:
        candle_vol_usdt = breakout_candle["volume"] * close_p
        if (candle_vol_usdt / open_interest) > MAX_VOL_TO_OI_RATIO:
            return True

    return False

def find_best_shelf(candles):
    if len(candles) < 80:
        return None

    closes = [c["close"] for c in candles]
    ema20 = calculate_ema(closes, EMA_FAST)
    ema40 = calculate_ema(closes, EMA_MID)
    ema80 = calculate_ema(closes, EMA_SLOW)

    total_closed = len(candles)
    candidates = []

    # Ищем полки, заканчивающиеся строго на 1-3 свечах назад
    min_end = max(EMA_SLOW + SHELF_MIN_CANDLES, total_closed - 3)

    for end in range(total_closed - 1, min_end - 1, -1):
        if ema20[end - 1] <= ema80[end - 1]:
            continue

        for length in range(SHELF_MAX_CANDLES, SHELF_MIN_CANDLES - 1, -1):
            start = end - length
            if start < EMA_SLOW:
                continue

            shelf = build_shelf(candles, start, end)
            if not shelf:
                continue

            ema_data = evaluate_ema_structure(candles, shelf, ema20, ema40, ema80)
            if not ema_data or ema_data["inside20_pct"] < 60.0 or ema_data["inside40_pct"] < 60.0:
                continue

            if ema_data["ema80_distance_pct"] > EMA80_MAX_DISTANCE_PCT:
                continue

            interaction = evaluate_ema_interaction(candles, shelf, ema20, ema40, ema80)
            if interaction["interactions"] < MIN_EMA_INTERACTIONS or has_old_breakout(candles, shelf):
                continue

            impulse = evaluate_first_impulse(candles, shelf)
            if impulse and impulse["status"] == "target":
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

async def evaluate_symbol(session, symbol, ticker):
    try:
        candles = await fetch_klines(session, symbol)
        if len(candles) < 85:
            return None

        closed = candles[:-1]
        if len(closed) < 80:
            return None

        candidate = find_best_shelf(closed)
        if not candidate:
            return None

        if time.time() - last_alert_time.get(symbol, 0) < ALERT_COOLDOWN_SECONDS:
            return None

        impulse_end = candidate["shelf"]["end"] + candidate["impulse"]["candles"] - 1
        impulse_end = min(impulse_end, len(closed) - 1)
        
        rvol = calculate_rvol(closed, impulse_end)

        if rvol < MIN_RVOL:
            logger.debug("❌ %s: Полка есть, но слабый RVOL (%.2f < %.2f)", symbol, rvol, MIN_RVOL)
            return None

        oi, oi_growth = None, None
        raw_oi = await fetch_open_interest(session, symbol)
        if raw_oi is not None:
            now = time.time()
            history = oi_history[symbol]
            history.append((now, raw_oi))
            oi = raw_oi
            if len(history) >= 2 and history[0][1] > 0:
                oi_growth = (oi - history[0][1]) / history[0][1] * 100.0

        breakout_candle = candidate["impulse"]["breakout_candle"]
        if is_market_maker_noise(breakout_candle, oi):
            logger.info("🛡️ %s: Отклонен фильтром Анти-ММ (Сквиз / Доджи / Накрутка объема)", symbol)
            return None

        return {
            "symbol": symbol,
            "ticker": ticker,
            "candidate": candidate,
            "rvol": rvol,
            "oi": oi,
            "oi_growth": oi_growth,
        }
    except Exception as e:
        logger.error("Ошибка при анализе %s: %s", symbol, e)
        return None

async def send_telegram(session, message):
    if not BOT_TOKEN or not CHAT_ID:
        logger.error("❌ BOT_TOKEN или CHAT_ID не настроены в переменной окружения!")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error("Ошибка отправки в Telegram: %s", e)
        return False

def build_signal_message(symbol, ticker, candidate, rvol, oi, oi_growth):
    shelf = candidate["shelf"]
    ema = candidate["ema"]
    impulse = candidate["impulse"]
    price = ticker.get("price", 0.0)
    top = shelf["top"]
    clean_symbol = symbol.replace("-USDT", "")

    return (
        f"🏹 <b>ПАРТИЗАН v6.5 — БЫЧИЙ ПРОБОЙ</b>\n\n"
        f"🪙 <b>Монета:</b> <code>{symbol}</code> (<code>{clean_symbol}</code>)\n"
        f"💰 <b>Цена:</b> {price:.8g}\n\n"
        f"📦 <b>ПОЛКА</b>\n"
        f"   Нижняя: {shelf['bottom']:.8g}\n"
        f"   Верхняя: {top:.8g}\n"
        f"   Ширина: {shelf['width_pct']:.2f}%\n"
        f"   Свечей: {shelf['length']}\n\n"
        f"📈 <b>EMA ВЕЕР (EMA20 > EMA80)</b>\n"
        f"   EMA20 внутри: {ema['inside20_pct']:.0f}%\n"
        f"   EMA40 внутри: {ema['inside40_pct']:.0f}%\n"
        f"   EMA80 дистанция: {ema['ema80_distance_pct']:.2f}%\n\n"
        f"🚀 <b>ИМПУЛЬС И АНТИ-ММ</b>\n"
        f"   Свеча пробоя: {impulse['candles']}\n"
        f"   Цель: +{BREAKOUT_TARGET_PCT:.1f}%\n"
        f"   RVOL: <b>{rvol:.2f}x</b>\n"
        f"   24h объём: ${ticker.get('volume24h', 0):,.0f}\n"
    )

async def scan_market(session):
    contracts = await fetch_contracts(session)
    tickers = await fetch_tickers(session)
    if not contracts or not tickers:
        logger.warning("⚠️ Не удалось получить контракты или тикеры BingX")
        return

    symbols = [s for s in contracts if s in tickers and s not in BLACKLIST]
    logger.info("🔍 Сканирование рынка: %d монет подходят под условия объема и цены", len(symbols))

    tasks = [evaluate_symbol(session, sym, tickers[sym]) for sym in symbols if time.time() - last_alert_time.get(sym, 0) >= ALERT_COOLDOWN_SECONDS]

    if not tasks:
        logger.info("⏳ Все тикеры на кулдауне")
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)
    found_signals = 0

    for res in results:
        if isinstance(res, Exception) or not res:
            continue
        symbol = res["symbol"]
        message = build_signal_message(symbol, res["ticker"], res["candidate"], res["rvol"], res["oi"], res["oi_growth"])
        if await send_telegram(session, message):
            last_alert_time[symbol] = time.time()
            found_signals += 1
            logger.info("🎯 СИГНАЛ ОТПРАВЛЕН В ТЕЛЕГРАМ: %s (RVOL: %.2fx)", symbol, res["rvol"])

    if found_signals == 0:
        logger.info("✅ Сканирование завершено. Подходящих свежих импульсов не найдено.")

async def health(request):
    return web.Response(text="ПАРТИЗАН v6.5.2 OK")

async def main():
    logger.info("🏹 Запуск ПАРТИЗАН v6.5.2 (Verbose Logging & Real-time Fix)...")
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=SESSION_TIMEOUT), connector=aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)) as session:
        await send_telegram(session, "🚀 <b>ПАРТИЗАН v6.5.2 запущен!</b>\nВключено подробное логирование сканирования.")
        while True:
            start_time = time.time()
            try:
                await scan_market(session)
            except Exception as e:
                logger.exception("Ошибка цикла сканирования: %s", e)
            await asyncio.sleep(max(1, CHECK_INTERVAL_SECONDS - (time.time() - start_time)))

if __name__ == "__main__":
    asyncio.run(main())
