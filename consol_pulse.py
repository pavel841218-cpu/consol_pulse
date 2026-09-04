import asyncio
import os
import logging
import time
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
from aiogram import Bot


# ============================================================
#                  ПАРТИЗАН v2 (FINAL TWEAKS)
#      РАННИЙ ПРОБОЙ СЖАТИЯ 1H → ИМПУЛЬС 5M
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
#                      CONFIG
# ============================================================

BOT_TOKEN = (
    os.environ.get("PUMP_BOT_TOKEN")
    or os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
)

CHAT_ID = (
    os.environ.get("PUMP_CHAT_ID")
    or os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
)

PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"
BYBIT_BASE_URL = "https://api.bybit.com"


# ============================================================
#                    ФИЛЬТРЫ РЫНКА
# ============================================================

MIN_24H_VOLUME_USDT = 1_000_000

CHECK_INTERVAL_SECONDS = 30

# Не повторять один и тот же сигнал
ALERT_COOLDOWN_SECONDS = 3 * 3600

# Время жизни запомненной полки
SHELF_MAX_AGE = 6 * 3600


# ============================================================
#                  ФИЛЬТР ПОЛКИ 1H (УЖЕСТОЧЕНЫ)
# ============================================================

BASE_MIN_HOURS = 4
BASE_MAX_HOURS = 12

# Общая ширина полки
MAX_SHELF_WIDTH_PCT = 3.8

# Разброс закрытий внутри полки
MAX_CLOSE_SPREAD_PCT = 2.8

# Максимальный наклон полки
MAX_SHELF_SLOPE_PCT = 1.2

# Последняя закрытая H1 свеча перед текущей
MAX_PRE_BREAKOUT_RANGE_PCT = 3.0


# ============================================================
#                    EMA-СЖАТИЕ И ЗАЩИТА
# ============================================================

MAX_EMA_SPREAD_PCT = 2.0
# ИЗМЕНЕНО: было 2.5, стало 5.0 (мягче)
MAX_DIST_FROM_EMA20_PCT = 5.0
# ИЗМЕНЕНО: было 0.9, стало 2.0 (мягче)
MAX_PULLBACK_FROM_HIGH_PCT = 2.0

EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80


# ============================================================
#                    5M ПРОБОЙ
# ============================================================

MIN_5M_BREAKOUT_PCT = 0.8
MAX_5M_BREAKOUT_PCT = 3.0

MIN_5M_BODY_PCT = 0.8
# ИЗМЕНЕНО: верхний предел тела убран (закомментирован)
# MAX_5M_BODY_PCT = 4.5

MIN_CLOSE_POSITION = 0.70

MIN_5M_RANGE_PCT = 1.0
# ИЗМЕНЕНО: верхний предел диапазона убран (закомментирован)
# MAX_5M_RANGE_PCT = 5.0


# ============================================================
#                    5M ОБЪЁМ & OI
# ============================================================

MIN_5M_RVOL = 1.8
RVOL_LOOKBACK = 12

# OI фильтр теперь не используется (см. check_symbol)
MIN_OI_GROWTH_ALLOWED = -0.5
GOOD_OI_GROWTH = 1.0


# ============================================================
#                  НЕЖЕЛАТЕЛЬНЫЕ ИНСТРУМЕНТЫ
# ============================================================

BAD_SYMBOL_PARTS = (
    "FOOTBALL",
    "INDEX",
    "STKFQ",
    "NCSK",
    "NCFX",  # Форекс пары BingX (EUR/JPY и т.д.)
    "NCCO",  # Товарные синтетики BingX
    "_",
)

BAD_SYMBOL_ENDINGS = (
    "2L-USDT",
    "2S-USDT",
    "3L-USDT",
    "3S-USDT",
)


# ============================================================
#                    STORAGE
# ============================================================

last_signals = {}
ACTIVE_SHELVES = {}
scan_counter = 0


# ============================================================
#                     HELPERS
# ============================================================

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def format_price(price):
    if price is None or price <= 0:
        return "0.00"
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    if price >= 0.01:
        return f"{price:.6f}"
    return f"{price:.8f}"


def cleanup_storage():
    now = time.time()

    expired_signals = [
        s for s, ts in last_signals.items()
        if now - ts > ALERT_COOLDOWN_SECONDS
    ]
    for s in expired_signals:
        del last_signals[s]

    expired_shelves = [
        s for s, sh in ACTIVE_SHELVES.items()
        if now - sh.get("created_at", now) > SHELF_MAX_AGE
    ]
    for s in expired_shelves:
        ACTIVE_SHELVES.pop(s, None)


def is_bad_symbol(symbol):
    symbol = str(symbol).upper()

    for part in BAD_SYMBOL_PARTS:
        if part in symbol:
            return True

    for ending in BAD_SYMBOL_ENDINGS:
        if symbol.endswith(ending):
            return True

    return False


def calculate_ema(prices, period):
    if not prices:
        return 0.0
    if len(prices) < period:
        return prices[-1]

    multiplier = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = (price * multiplier) + (ema * (1 - multiplier))
    return ema


# ============================================================
#                  HEALTH CHECK
# ============================================================

async def health_check(request):
    return web.Response(
        text=(
            f"PARTIZAN v2 ACTIVE | "
            f"Shelves: {len(ACTIVE_SHELVES)} | "
            f"Signals: {len(last_signals)}"
        ),
        status=200
    )


# ============================================================
#                  BINGX SYMBOLS
# ============================================================

async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return {}

            data = await resp.json()
            if data.get("code") != 0:
                return {}

            result = {}
            for item in data.get("data", []):
                symbol = str(item.get("symbol", "")).upper()

                if not symbol.endswith("-USDT"):
                    continue

                if is_bad_symbol(symbol):
                    continue

                volume_24h = safe_float(item.get("quoteVolume"))
                last_price = safe_float(item.get("lastPrice"))

                if last_price <= 0 or volume_24h < MIN_24H_VOLUME_USDT:
                    continue

                result[symbol] = volume_24h

            return result

    except Exception as e:
        logging.error(f"Ошибка тикеров BingX: {e}")
        return {}


# ============================================================
#                     KLINES
# ============================================================

async def fetch_klines(session, symbol, interval, limit, semaphore):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return []

                data = await resp.json()
                candles = data.get("data", [])
                if not isinstance(candles, list):
                    return []

                result = []
                for k in candles:
                    try:
                        if isinstance(k, dict):
                            result.append({
                                "time": int(safe_float(k.get("time") or k.get("timestamp"))),
                                "open": safe_float(k.get("open")),
                                "high": safe_float(k.get("high")),
                                "low": safe_float(k.get("low")),
                                "close": safe_float(k.get("close")),
                                "volume": safe_float(k.get("volume"))
                            })
                        elif isinstance(k, list) and len(k) >= 6:
                            result.append({
                                "time": int(safe_float(k[0])),
                                "open": safe_float(k[1]),
                                "high": safe_float(k[2]),
                                "low": safe_float(k[3]),
                                "close": safe_float(k[4]),
                                "volume": safe_float(k[5])
                            })
                    except Exception:
                        continue

                result.sort(key=lambda x: x["time"])
                return result

        except Exception:
            return []


# ============================================================
#                         OI BYBIT
# ============================================================

async def get_oi_growth(session, symbol, semaphore):
    bybit_symbol = symbol.replace("-", "").upper()
    url = f"{BYBIT_BASE_URL}/v5/market/open-interest"
    params = {"category": "linear", "symbol": bybit_symbol, "intervalTime": "1h", "limit": 3}
    headers = {"User-Agent": "Mozilla/5.0"}

    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status != 200:
                    return 0.0, "None"

                data = await resp.json()
                if data.get("retCode") != 0:
                    return 0.0, "None"

                items = data.get("result", {}).get("list", [])
                if len(items) < 2:
                    return 0.0, "None"

                current_oi = safe_float(items[0].get("openInterest"))
                previous_oi = safe_float(items[1].get("openInterest"))

                if current_oi <= 0 or previous_oi <= 0:
                    return 0.0, "None"

                growth = ((current_oi - previous_oi) / previous_oi) * 100
                return growth, "Bybit"

        except Exception:
            return 0.0, "None"


# ============================================================
#              ПРОВЕРКА EMA СЖАТИЯ
# ============================================================

def check_ema_squeeze(candles):
    if len(candles) < 90:
        return False, 0.0, 0.0

    closed = candles[:-1]
    current_price = candles[-1]["close"]

    closes = [c["close"] for c in closed if c["close"] > 0]
    if len(closes) < 80:
        return False, 0.0, 0.0

    ema20 = calculate_ema(closes, EMA_FAST)
    ema40 = calculate_ema(closes, EMA_MID)
    ema80 = calculate_ema(closes, EMA_SLOW)

    ema_max = max(ema20, ema40, ema80)
    ema_min = min(ema20, ema40, ema80)

    if ema_min <= 0:
        return False, 0.0, 0.0

    spread = ((ema_max - ema_min) / ema_min) * 100
    if spread > MAX_EMA_SPREAD_PCT:
        return False, spread, 0.0

    dist_from_ema20 = ((current_price - ema20) / ema20) * 100
    if dist_from_ema20 > MAX_DIST_FROM_EMA20_PCT:
        return False, spread, dist_from_ema20

    return True, spread, dist_from_ema20


# ============================================================
#              ПОИСК НАСТОЯЩЕЙ ПОЛКИ
# ============================================================

def find_shelf(candles):
    if len(candles) < 90:
        return None

    closed = candles[:-1]
    last = closed[-1]

    if last["low"] <= 0:
        return None

    last_range_pct = ((last["high"] - last["low"]) / last["low"]) * 100
    if last_range_pct > MAX_PRE_BREAKOUT_RANGE_PCT:
        return None

    for hours in range(BASE_MAX_HOURS, BASE_MIN_HOURS - 1, -1):
        part = closed[-hours:]
        if len(part) < BASE_MIN_HOURS:
            continue

        highs = [c["high"] for c in part if c["high"] > 0]
        lows = [c["low"] for c in part if c["low"] > 0]
        closes = [c["close"] for c in part if c["close"] > 0]

        if len(highs) < BASE_MIN_HOURS or len(lows) < BASE_MIN_HOURS or len(closes) < BASE_MIN_HOURS:
            continue

        shelf_high = max(highs)
        shelf_low = min(lows)

        if shelf_low <= 0:
            continue

        width = ((shelf_high - shelf_low) / shelf_low) * 100
        if width > MAX_SHELF_WIDTH_PCT:
            continue

        max_close = max(closes)
        min_close = min(closes)
        close_spread = ((max_close - min_close) / min_close) * 100
        if close_spread > MAX_CLOSE_SPREAD_PCT:
            continue

        third = max(1, len(closes) // 3)
        first_avg = sum(closes[:third]) / third
        last_avg = sum(closes[-third:]) / third

        if first_avg <= 0:
            continue

        slope = abs((last_avg - first_avg) / first_avg) * 100
        if slope > MAX_SHELF_SLOPE_PCT:
            continue

        return {
            "hours": hours,
            "high": shelf_high,
            "low": shelf_low,
            "width": width,
            "close_spread": close_spread,
            "slope": slope,
            "created_at": time.time()
        }

    return None


# ============================================================
#           ПРОВЕРКА 5M ИМПУЛЬСА (С МЯГКИМИ ФИЛЬТРАМИ)
# ============================================================

def check_5m_breakout(candles_5m, shelf):
    if len(candles_5m) < RVOL_LOOKBACK + 1:
        return None

    current = candles_5m[-1]

    o, h, l, c, v = (
        current["open"], current["high"],
        current["low"], current["close"], current["volume"]
    )

    if o <= 0 or h <= 0 or l <= 0 or c <= 0 or v <= 0:
        return None

    # 1. ЗАПРЕТ КРАСНОЙ СВЕЧИ
    if c <= o:
        return None

    # 2. ОТКАТ ОТ ХАЯ (смягчён)
    pullback_pct = ((h - c) / h) * 100
    if pullback_pct > MAX_PULLBACK_FROM_HIGH_PCT:
        return None

    # 3. ПРОБОЙ ПОЛКИ
    breakout_pct = ((c - shelf["high"]) / shelf["high"]) * 100
    if breakout_pct < MIN_5M_BREAKOUT_PCT or breakout_pct > MAX_5M_BREAKOUT_PCT:
        return None

    # 4. ТЕЛО СВЕЧИ (только минимальный порог)
    body_pct = (abs(c - o) / o) * 100
    if body_pct < MIN_5M_BODY_PCT:
        return None

    # 5. ДИАПАЗОН (только минимальный порог)
    range_pct = ((h - l) / l) * 100
    if range_pct < MIN_5M_RANGE_PCT:
        return None

    # 6. ЗАКРЫТИЕ БЛИЗКО К HIGH
    candle_range = h - l
    if candle_range <= 0:
        return None

    close_position = (c - l) / candle_range
    if close_position < MIN_CLOSE_POSITION:
        return None

    # 7. RVOL
    historical_volumes = [
        candle["volume"]
        for candle in candles_5m[-(RVOL_LOOKBACK + 1):-1]
        if candle["volume"] > 0
    ]

    if not historical_volumes:
        return None

    avg_volume = sum(historical_volumes) / len(historical_volumes)
    if avg_volume <= 0:
        return None

    rvol = v / avg_volume
    if rvol < MIN_5M_RVOL:
        return None

    return {
        "price": c,
        "breakout_pct": breakout_pct,
        "body_pct": body_pct,
        "range_pct": range_pct,
        "close_position": close_position,
        "rvol": rvol
    }


# ============================================================
#                 ОТПРАВКА СИГНАЛА
# ============================================================

async def send_signal(bot, symbol, shelf, impulse, oi_growth, oi_source, volume_24h):
    clean_symbol = symbol.replace("-USDT", "").upper()

    breakout_pct = impulse["breakout_pct"]
    body_pct = impulse["body_pct"]
    range_pct = impulse["range_pct"]
    rvol = impulse["rvol"]
    current_price = impulse["price"]

    if oi_source == "None":
        oi_text = "<code>Н/Д</code> ⚠️"
    elif oi_growth >= GOOD_OI_GROWTH:
        oi_text = f"<b>+{oi_growth:.2f}%</b> 🟢 ({oi_source})"
    elif oi_growth >= 0:
        oi_text = f"<code>+{oi_growth:.2f}%</code> 🟡 ({oi_source})"
    else:
        oi_text = f"<code>{oi_growth:.2f}%</code> ⚠️ ({oi_source})"

    score = 0
    if breakout_pct <= 2.0: score += 2
    else: score += 1

    if rvol >= 2.5: score += 2
    elif rvol >= 2.0: score += 1

    if body_pct >= 1.2: score += 1

    if oi_source != "None":
        if oi_growth >= 1.0: score += 2
        elif oi_growth >= 0: score += 1

    if score >= 6: signal_grade = "🔥 СИЛЬНЫЙ"
    elif score >= 4: signal_grade = "🟢 ХОРОШИЙ"
    else: signal_grade = "🟡 РАННИЙ"

    message = (
        f"🚀 <b>ПАРТИЗАН: {clean_symbol}</b>\n"
        f"{signal_grade}\n\n"
        f"💥 Пробой полки: <b>+{breakout_pct:.2f}%</b>\n"
        f"📊 RVOL 5M: <b>{rvol:.1f}x</b>\n"
        f"⚡ Тело 5M: <b>{body_pct:.2f}%</b>\n"
        f"📈 Диапазон 5M: <b>{range_pct:.2f}%</b>\n\n"
        f"├ Полка: <code>{format_price(shelf['low'])} — {format_price(shelf['high'])}</code>\n"
        f"├ Ширина полки: <b>{shelf['width']:.2f}%</b>\n"
        f"├ Длительность: <b>{shelf['hours']}ч</b>\n"
        f"├ EMA20/40/80: <b>{shelf['ema_spread']:.2f}%</b>\n"
        f"├ ОИ 1H: {oi_text}\n"
        f"├ Цена: <code>{format_price(current_price)}</code>\n"
        f"└ Объём 24h: <b>${volume_24h / 1_000_000:.2f}M</b>\n\n"
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График BingX</a>"
    )

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=message,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        return True
    except Exception as e:
        logging.error(f"Ошибка Telegram {symbol}: {e}")
        return False


# ============================================================
#                  СОЗДАНИЕ/ПОЛУЧЕНИЕ ПОЛКИ (FIXED CACHE)
# ============================================================

async def build_shelf_if_needed(session, symbol, semaphore):
    if symbol in ACTIVE_SHELVES:
        shelf = ACTIVE_SHELVES[symbol]
        if time.time() - shelf.get("created_at", 0) < SHELF_MAX_AGE:
            return shelf
        ACTIVE_SHELVES.pop(symbol, None)

    candles_1h = await fetch_klines(session, symbol, "1h", 100, semaphore)
    if len(candles_1h) < 90:
        return None

    ema_ok, ema_spread, dist_ema = check_ema_squeeze(candles_1h)
    if not ema_ok:
        return None

    shelf = find_shelf(candles_1h)
    if shelf is None:
        return None

    shelf["ema_spread"] = ema_spread
    shelf["created_at"] = time.time()

    ACTIVE_SHELVES[symbol] = shelf
    logging.info(f"📦 НОВАЯ ПОЛКА {symbol} | {shelf['hours']}ч | {shelf['width']:.2f}% | EMA {ema_spread:.2f}%")

    return shelf


# ============================================================
#                  ПРОВЕРКА МОНЕТЫ
# ============================================================

async def check_symbol(session, bot, symbol, volume_24h, semaphore):
    now = time.time()

    if symbol in last_signals and now - last_signals[symbol] < ALERT_COOLDOWN_SECONDS:
        return False

    shelf = await build_shelf_if_needed(session, symbol, semaphore)
    if shelf is None:
        return False

    candles_5m = await fetch_klines(session, symbol, "5m", RVOL_LOOKBACK + 2, semaphore)
    if len(candles_5m) < RVOL_LOOKBACK + 1:
        return False

    impulse = check_5m_breakout(candles_5m, shelf)
    if impulse is None:
        return False

    oi_growth, oi_source = await get_oi_growth(session, symbol, semaphore)

    # ИЗМЕНЕНО: блокировка по OI убрана
    # if oi_source != "None" and oi_growth < MIN_OI_GROWTH_ALLOWED:
    #     logging.info(f"⛔ OI FILTER {symbol} | OI {oi_growth:.2f}%")
    #     return False

    success = await send_signal(bot, symbol, shelf, impulse, oi_growth, oi_source, volume_24h)

    if success:
        last_signals[symbol] = now
        ACTIVE_SHELVES.pop(symbol, None)
        logging.info(
            f"🚀 СИГНАЛ {symbol} | Пробой +{impulse['breakout_pct']:.2f}% | "
            f"RVOL {impulse['rvol']:.1f}x | OI {oi_growth:.2f}%"
        )

    return success


# ============================================================
#                       SCANNER
# ============================================================

async def scanner_loop(bot):
    global scan_counter
    semaphore = asyncio.Semaphore(20)
    connector = aiohttp.TCPConnector(limit=40, ttl_dns_cache=300, force_close=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                scan_counter += 1
                start_time = time.time()

                if scan_counter % 20 == 0:
                    cleanup_storage()

                symbols_dict = await fetch_bingx_symbols(session)
                if not symbols_dict:
                    logging.warning("⚠️ BingX не вернул пары")
                    await asyncio.sleep(30)
                    continue

                tasks = [check_symbol(session, bot, sym, vol, semaphore) for sym, vol in symbols_dict.items()]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                signals = sum(1 for r in results if r is True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                elapsed = time.time() - start_time

                logging.info(
                    f"🔎 Скан #{scan_counter} | {elapsed:.1f}с | "
                    f"Пар: {len(symbols_dict)} | Полок: {len(ACTIVE_SHELVES)} | "
                    f"Сигналов: {signals} | Ошибок: {errors}"
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"❌ Ошибка сканера: {e}")

            await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# ============================================================
#                         MAIN
# ============================================================

async def main():
    bot = Bot(token=BOT_TOKEN)
    app = web.Application()
    app.router.add_get("/", health_check)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logging.info(f"🌐 Сервер запущен | PORT {PORT}")

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                "🤖 <b>ПАРТИЗАН v2 запущен (Final Tweaks)</b>\n\n"
                "📦 Жесткая фильтрация полок 1H\n"
                "⚡ Фильтр форекс-мусора BingX\n"
                "🛡 Стабильный кэш полок\n"
                "🚀 Защита от пробоев на дампе\n"
                "🔧 Смягчены фильтры 5M и OI (информационно)"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка стартового сообщения: {e}")

    try:
        await scanner_loop(bot)
    finally:
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
