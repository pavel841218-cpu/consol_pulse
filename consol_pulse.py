import asyncio
import os
import logging
import time
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
from aiogram import Bot


# ============================================================
#                  ПАРТИЗАН v2
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

MIN_24H_VOLUME_USDT = 800_000

CHECK_INTERVAL_SECONDS = 30

# Не повторять один и тот же сигнал
ALERT_COOLDOWN_SECONDS = 3 * 3600

# Через сколько секунд забываем старую полку,
# если пампа так и не произошло
SHELF_MAX_AGE = 6 * 3600


# ============================================================
#                  ФИЛЬТР ПОЛКИ 1H
# ============================================================

BASE_MIN_HOURS = 4
BASE_MAX_HOURS = 12

# Общая ширина полки
MAX_SHELF_WIDTH_PCT = 4.5

# Разброс закрытий внутри полки
MAX_CLOSE_SPREAD_PCT = 3.5

# Максимальный наклон полки
MAX_SHELF_SLOPE_PCT = 1.8

# Последняя закрытая H1 свеча перед текущей
# не должна быть уже сильно разогнанной
MAX_PRE_BREAKOUT_RANGE_PCT = 4.0


# ============================================================
#                    EMA-СЖАТИЕ
# ============================================================

MAX_EMA_SPREAD_PCT = 2.5

EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80


# ============================================================
#                    5M ПРОБОЙ
# ============================================================

# Ранняя зона входа
MIN_5M_BREAKOUT_PCT = 0.8

# После этого уже считаем движение поздним
MAX_5M_BREAKOUT_PCT = 3.2

# Минимальное тело импульсной свечи
MIN_5M_BODY_PCT = 0.8

# Не принимаем ненормально огромную свечу
MAX_5M_BODY_PCT = 5.0

# Закрытие должно быть достаточно близко к максимуму
MIN_CLOSE_POSITION = 0.68

# Диапазон свечи
MIN_5M_RANGE_PCT = 1.2
MAX_5M_RANGE_PCT = 6.0


# ============================================================
#                    5M ОБЪЁМ
# ============================================================

# Объём текущей 5M свечи относительно предыдущих
MIN_5M_RVOL = 1.6

# Сколько предыдущих 5M свечей используем
RVOL_LOOKBACK = 12


# ============================================================
#                     OI
# ============================================================

# Сильное падение OI во время пробоя — плохой признак
MIN_OI_GROWTH_ALLOWED = -1.5

# Хороший рост OI
GOOD_OI_GROWTH = 1.0


# ============================================================
#                  НЕЖЕЛАТЕЛЬНЫЕ ИНСТРУМЕНТЫ
# ============================================================

BAD_SYMBOL_PARTS = (
    "FOOTBALL",
    "INDEX",
    "STKFQ",
    "NCSK",
    "_",
)

# Некоторые производные/плечевые инструменты
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

# Здесь хранятся именно ЗАПОМНЕННЫЕ полки
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

    expired = [
        symbol
        for symbol, timestamp in last_signals.items()
        if now - timestamp > ALERT_COOLDOWN_SECONDS
    ]

    for symbol in expired:
        del last_signals[symbol]

    expired_shelves = [
        symbol
        for symbol, shelf in ACTIVE_SHELVES.items()
        if now - shelf.get("created_at", now) > SHELF_MAX_AGE
    ]

    for symbol in expired_shelves:
        ACTIVE_SHELVES.pop(symbol, None)


def is_bad_symbol(symbol):
    symbol = str(symbol).upper()

    for part in BAD_SYMBOL_PARTS:
        if part in symbol:
            return True

    for ending in BAD_SYMBOL_ENDINGS:
        if symbol.endswith(ending):
            return True

    return False


# ============================================================
#                  EMA CALCULATION
# ============================================================

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
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:

            if resp.status != 200:
                return {}

            data = await resp.json()

            if data.get("code") != 0:
                return {}

            result = {}

            for item in data.get("data", []):

                symbol = str(
                    item.get("symbol", "")
                ).upper()

                if not symbol.endswith("-USDT"):
                    continue

                if is_bad_symbol(symbol):
                    continue

                volume_24h = safe_float(
                    item.get("quoteVolume")
                )

                last_price = safe_float(
                    item.get("lastPrice")
                )

                if last_price <= 0:
                    continue

                if volume_24h < MIN_24H_VOLUME_USDT:
                    continue

                result[symbol] = volume_24h

            return result

    except Exception as e:
        logging.error(
            f"Ошибка тикеров BingX: {e}"
        )
        return {}


# ============================================================
#                     KLINES
# ============================================================

async def fetch_klines(
    session,
    symbol,
    interval,
    limit,
    semaphore
):

    url = (
        f"{BINGX_BASE_URL}"
        f"/openApi/swap/v3/quote/klines"
    )

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    async with semaphore:

        try:

            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:

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
                                "time": int(
                                    safe_float(
                                        k.get("time")
                                        or k.get("timestamp")
                                    )
                                ),

                                "open": safe_float(
                                    k.get("open")
                                ),

                                "high": safe_float(
                                    k.get("high")
                                ),

                                "low": safe_float(
                                    k.get("low")
                                ),

                                "close": safe_float(
                                    k.get("close")
                                ),

                                "volume": safe_float(
                                    k.get("volume")
                                )
                            })

                        elif (
                            isinstance(k, list)
                            and len(k) >= 6
                        ):

                            result.append({
                                "time": int(
                                    safe_float(k[0])
                                ),

                                "open": safe_float(k[1]),
                                "high": safe_float(k[2]),
                                "low": safe_float(k[3]),
                                "close": safe_float(k[4]),
                                "volume": safe_float(k[5])
                            })

                    except Exception:
                        continue

                result.sort(
                    key=lambda x: x["time"]
                )

                return result

        except Exception:
            return []


# ============================================================
#                         OI BYBIT
# ============================================================

async def get_oi_growth(
    session,
    symbol,
    semaphore
):

    bybit_symbol = (
        symbol.replace("-", "")
        .upper()
    )

    url = (
        f"{BYBIT_BASE_URL}"
        f"/v5/market/open-interest"
    )

    params = {
        "category": "linear",
        "symbol": bybit_symbol,
        "intervalTime": "1h",
        "limit": 3
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    async with semaphore:

        try:

            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=4)
            ) as resp:

                if resp.status != 200:
                    return 0.0, "None"

                data = await resp.json()

                if data.get("retCode") != 0:
                    return 0.0, "None"

                items = (
                    data.get("result", {})
                    .get("list", [])
                )

                if len(items) < 2:
                    return 0.0, "None"

                current_oi = safe_float(
                    items[0].get("openInterest")
                )

                previous_oi = safe_float(
                    items[1].get("openInterest")
                )

                if current_oi <= 0 or previous_oi <= 0:
                    return 0.0, "None"

                growth = (
                    (current_oi - previous_oi)
                    / previous_oi
                ) * 100

                return growth, "Bybit"

        except Exception:
            return 0.0, "None"


# ============================================================
#              ПРОВЕРКА EMA СЖАТИЯ
# ============================================================

def check_ema_squeeze(candles):
    if len(candles) < 90:
        return False, 0.0

    # Только полностью закрытые H1
    closed = candles[:-1]

    closes = [
        c["close"]
        for c in closed
        if c["close"] > 0
    ]

    if len(closes) < 80:
        return False, 0.0

    ema20 = calculate_ema(
        closes,
        EMA_FAST
    )

    ema40 = calculate_ema(
        closes,
        EMA_MID
    )

    ema80 = calculate_ema(
        closes,
        EMA_SLOW
    )

    ema_max = max(
        ema20,
        ema40,
        ema80
    )

    ema_min = min(
        ema20,
        ema40,
        ema80
    )

    if ema_min <= 0:
        return False, 0.0

    spread = (
        (ema_max - ema_min)
        / ema_min
    ) * 100

    if spread > MAX_EMA_SPREAD_PCT:
        return False, spread

    return True, spread


# ============================================================
#              ПОИСК НАСТОЯЩЕЙ ПОЛКИ
# ============================================================

def find_shelf(candles):
    """
    Ищем базу среди полностью закрытых H1 свечей.

    ВАЖНО:
    найденная полка потом фиксируется.
    Она не пересчитывается после начала пампа.
    """

    if len(candles) < 90:
        return None

    closed = candles[:-1]

    # Последняя закрытая свеча должна быть спокойной
    last = closed[-1]

    if last["low"] <= 0:
        return None

    last_range_pct = (
        (last["high"] - last["low"])
        / last["low"]
    ) * 100

    if last_range_pct > MAX_PRE_BREAKOUT_RANGE_PCT:
        return None

    # Ищем базу
    for hours in range(
        BASE_MAX_HOURS,
        BASE_MIN_HOURS - 1,
        -1
    ):

        part = closed[-hours:]

        if len(part) < BASE_MIN_HOURS:
            continue

        highs = [
            c["high"]
            for c in part
            if c["high"] > 0
        ]

        lows = [
            c["low"]
            for c in part
            if c["low"] > 0
        ]

        closes = [
            c["close"]
            for c in part
            if c["close"] > 0
        ]

        if (
            len(highs) < BASE_MIN_HOURS
            or len(lows) < BASE_MIN_HOURS
            or len(closes) < BASE_MIN_HOURS
        ):
            continue

        shelf_high = max(highs)
        shelf_low = min(lows)

        if shelf_low <= 0:
            continue

        width = (
            (shelf_high - shelf_low)
            / shelf_low
        ) * 100

        if width > MAX_SHELF_WIDTH_PCT:
            continue

        # Разброс закрытий
        max_close = max(closes)
        min_close = min(closes)

        close_spread = (
            (max_close - min_close)
            / min_close
        ) * 100

        if close_spread > MAX_CLOSE_SPREAD_PCT:
            continue

        # Проверка наклона
        third = max(
            1,
            len(closes) // 3
        )

        first_avg = (
            sum(closes[:third])
            / third
        )

        last_avg = (
            sum(closes[-third:])
            / third
        )

        if first_avg <= 0:
            continue

        slope = abs(
            (last_avg - first_avg)
            / first_avg
        ) * 100

        if slope > MAX_SHELF_SLOPE_PCT:
            continue

        # Средний диапазон
        ranges = []

        for c in part:

            if (
                c["high"] > 0
                and c["low"] > 0
            ):

                ranges.append(
                    c["high"] - c["low"]
                )

        avg_range = (
            sum(ranges) / len(ranges)
            if ranges
            else 0
        )

        if avg_range <= 0:
            continue

        return {
            "hours": hours,
            "high": shelf_high,
            "low": shelf_low,
            "width": width,
            "close_spread": close_spread,
            "slope": slope,
            "avg_range": avg_range,
            "created_at": time.time()
        }

    return None


# ============================================================
#           ПРОВЕРКА 5M ИМПУЛЬСА
# ============================================================

def check_5m_breakout(
    candles_5m,
    shelf
):

    if len(candles_5m) < RVOL_LOOKBACK + 1:
        return None

    current = candles_5m[-1]

    o = current["open"]
    h = current["high"]
    l = current["low"]
    c = current["close"]
    v = current["volume"]

    if (
        o <= 0
        or h <= 0
        or l <= 0
        or c <= 0
        or v <= 0
    ):
        return None

    # Только движение вверх
    if c <= o:
        return None

    # ========================================================
    # ПРОБОЙ ПОЛКИ
    # ========================================================

    breakout_pct = (
        (c - shelf["high"])
        / shelf["high"]
    ) * 100

    # Слишком рано
    if breakout_pct < MIN_5M_BREAKOUT_PCT:
        return None

    # Слишком поздно
    if breakout_pct > MAX_5M_BREAKOUT_PCT:
        return None

    # ========================================================
    # ТЕЛО СВЕЧИ
    # ========================================================

    body_pct = (
        abs(c - o)
        / o
    ) * 100

    if body_pct < MIN_5M_BODY_PCT:
        return None

    if body_pct > MAX_5M_BODY_PCT:
        return None

    # ========================================================
    # ДИАПАЗОН
    # ========================================================

    range_pct = (
        (h - l)
        / l
    ) * 100

    if range_pct < MIN_5M_RANGE_PCT:
        return None

    if range_pct > MAX_5M_RANGE_PCT:
        return None

    # ========================================================
    # ЗАКРЫТИЕ БЛИЗКО К HIGH
    # ========================================================

    candle_range = h - l

    if candle_range <= 0:
        return None

    close_position = (
        (c - l)
        / candle_range
    )

    if close_position < MIN_CLOSE_POSITION:
        return None

    # ========================================================
    # RVOL
    # ========================================================

    historical_volumes = []

    # Берём предыдущие свечи, текущую не включаем
    for candle in candles_5m[
        -(RVOL_LOOKBACK + 1):-1
    ]:

        volume = candle["volume"]

        if volume > 0:
            historical_volumes.append(
                volume
            )

    if not historical_volumes:
        return None

    avg_volume = (
        sum(historical_volumes)
        / len(historical_volumes)
    )

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

async def send_signal(
    bot,
    symbol,
    shelf,
    impulse,
    oi_growth,
    oi_source,
    volume_24h
):

    clean_symbol = (
        symbol
        .replace("-USDT", "")
        .upper()
    )

    breakout_pct = impulse["breakout_pct"]
    body_pct = impulse["body_pct"]
    range_pct = impulse["range_pct"]
    rvol = impulse["rvol"]
    current_price = impulse["price"]

    # ========================================================
    # OI STATUS
    # ========================================================

    if oi_source == "None":

        oi_text = (
            "<code>Н/Д</code> ⚠️"
        )

    elif oi_growth >= GOOD_OI_GROWTH:

        oi_text = (
            f"<b>+{oi_growth:.2f}%</b> "
            f"🟢 ({oi_source})"
        )

    elif oi_growth >= 0:

        oi_text = (
            f"<code>+{oi_growth:.2f}%</code> "
            f"🟡 ({oi_source})"
        )

    else:

        oi_text = (
            f"<code>{oi_growth:.2f}%</code> "
            f"⚠️ ({oi_source})"
        )

    # ========================================================
    # СИЛА СИГНАЛА
    # ========================================================

    score = 0

    if breakout_pct <= 2.0:
        score += 2
    else:
        score += 1

    if rvol >= 2.5:
        score += 2
    elif rvol >= 2.0:
        score += 1

    if body_pct >= 1.2:
        score += 1

    if oi_source != "None":

        if oi_growth >= 1.0:
            score += 2

        elif oi_growth >= 0:
            score += 1

    if score >= 6:
        signal_grade = "🔥 СИЛЬНЫЙ"
    elif score >= 4:
        signal_grade = "🟢 ХОРОШИЙ"
    else:
        signal_grade = "🟡 РАННИЙ"

    message = (
        f"🚀 <b>ПАРТИЗАН: {clean_symbol}</b>\n"
        f"{signal_grade}\n\n"

        f"💥 Пробой полки: "
        f"<b>+{breakout_pct:.2f}%</b>\n"

        f"📊 RVOL 5M: "
        f"<b>{rvol:.1f}x</b>\n"

        f"⚡ Тело 5M: "
        f"<b>{body_pct:.2f}%</b>\n"

        f"📈 Диапазон 5M: "
        f"<b>{range_pct:.2f}%</b>\n\n"

        f"├ Полка: "
        f"<code>{format_price(shelf['low'])}"
        f" — "
        f"{format_price(shelf['high'])}</code>\n"

        f"├ Ширина полки: "
        f"<b>{shelf['width']:.2f}%</b>\n"

        f"├ Длительность: "
        f"<b>{shelf['hours']}ч</b>\n"

        f"├ EMA20/40/80: "
        f"<b>{shelf['ema_spread']:.2f}%</b>\n"

        f"├ ОИ 1H: "
        f"{oi_text}\n"

        f"├ Цена: "
        f"<code>{format_price(current_price)}</code>\n"

        f"└ Объём 24h: "
        f"<b>${volume_24h / 1_000_000:.2f}M</b>\n\n"

        f"🔗 <a href='"
        f"https://bingx.com/ru-ru/futures/forward/"
        f"{symbol}"
        f"'>График BingX</a>"
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

        logging.error(
            f"Ошибка Telegram {symbol}: {e}"
        )

        return False


# ============================================================
#                  СОЗДАНИЕ ПОЛКИ
# ============================================================

async def build_shelf_if_needed(
    session,
    symbol,
    semaphore
):

    # Уже есть активная полка
    if symbol in ACTIVE_SHELVES:

        shelf = ACTIVE_SHELVES[symbol]

        # Если полка ещё свежая — НЕ ПЕРЕСЧИТЫВАЕМ
        if (
            time.time()
            - shelf.get("created_at", 0)
            < SHELF_MAX_AGE
        ):
            return shelf

        ACTIVE_SHELVES.pop(
            symbol,
            None
        )

    candles_1h = await fetch_klines(
        session,
        symbol,
        "1h",
        100,
        semaphore
    )

    if len(candles_1h) < 90:
        return None

    # ========================================================
    # EMA
    # ========================================================

    ema_ok, ema_spread = (
        check_ema_squeeze(candles_1h)
    )

    if not ema_ok:
        return None

    # ========================================================
    # ПОЛКА
    # ========================================================

    shelf = find_shelf(
        candles_1h
    )

    if shelf is None:
        return None

    shelf["ema_spread"] = ema_spread
    shelf["created_at"] = time.time()

    ACTIVE_SHELVES[symbol] = shelf

    logging.info(
        f"📦 НОВАЯ ПОЛКА "
        f"{symbol} | "
        f"{shelf['hours']}ч | "
        f"{shelf['width']:.2f}% | "
        f"EMA {ema_spread:.2f}%"
    )

    return shelf


# ============================================================
#                  ПРОВЕРКА МОНЕТЫ
# ============================================================

async def check_symbol(
    session,
    bot,
    symbol,
    volume_24h,
    semaphore
):

    now = time.time()

    # ========================================================
    # COOLDOWN
    # ========================================================

    if (
        symbol in last_signals
        and
        now - last_signals[symbol]
        < ALERT_COOLDOWN_SECONDS
    ):
        return False

    # ========================================================
    # ПОЛКА
    # ========================================================

    shelf = await build_shelf_if_needed(
        session,
        symbol,
        semaphore
    )

    if shelf is None:
        return False

    # ========================================================
    # 5M
    # ========================================================

    candles_5m = await fetch_klines(
        session,
        symbol,
        "5m",
        RVOL_LOOKBACK + 2,
        semaphore
    )

    if len(candles_5m) < RVOL_LOOKBACK + 1:
        return False

    impulse = check_5m_breakout(
        candles_5m,
        shelf
    )

    if impulse is None:
        return False

    # ========================================================
    # OI
    # ========================================================

    oi_growth, oi_source = (
        await get_oi_growth(
            session,
            symbol,
            semaphore
        )
    )

    # ========================================================
    # СИЛЬНО ПАДАЮЩИЙ OI — НЕ БЕРЁМ
    # ========================================================

    if (
        oi_source != "None"
        and
        oi_growth < MIN_OI_GROWTH_ALLOWED
    ):

        logging.info(
            f"⛔ OI FILTER {symbol} | "
            f"OI {oi_growth:.2f}%"
        )

        return False

    # ========================================================
    # СИГНАЛ
    # ========================================================

    success = await send_signal(
        bot,
        symbol,
        shelf,
        impulse,
        oi_growth,
        oi_source,
        volume_24h
    )

    if success:

        last_signals[symbol] = now

        # Полка больше не нужна
        ACTIVE_SHELVES.pop(
            symbol,
            None
        )

        logging.info(
            f"🚀 СИГНАЛ {symbol} | "
            f"Пробой +{impulse['breakout_pct']:.2f}% | "
            f"RVOL {impulse['rvol']:.1f}x | "
            f"OI {oi_growth:.2f}%"
        )

    return success


# ============================================================
#                       SCANNER
# ============================================================

async def scanner_loop(bot):

    global scan_counter

    # Ограничиваем одновременно работающие запросы
    semaphore = asyncio.Semaphore(20)

    connector = aiohttp.TCPConnector(
        limit=40,
        ttl_dns_cache=300,
        force_close=False
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        while True:

            try:

                scan_counter += 1

                start_time = time.time()

                # =================================================
                # ЧИСТИМ СТАРЫЕ ДАННЫЕ
                # =================================================

                if scan_counter % 20 == 0:
                    cleanup_storage()

                # =================================================
                # ПОЛУЧАЕМ СПИСОК МОНЕТ
                # =================================================

                symbols_dict = (
                    await fetch_bingx_symbols(
                        session
                    )
                )

                if not symbols_dict:

                    logging.warning(
                        "⚠️ BingX не вернул пары"
                    )

                    await asyncio.sleep(30)

                    continue

                # =================================================
                # СКАН
                # =================================================

                tasks = []

                for symbol, volume in (
                    symbols_dict.items()
                ):

                    tasks.append(
                        check_symbol(
                            session,
                            bot,
                            symbol,
                            volume,
                            semaphore
                        )
                    )

                results = await asyncio.gather(
                    *tasks,
                    return_exceptions=True
                )

                signals = sum(
                    1
                    for r in results
                    if r is True
                )

                errors = sum(
                    1
                    for r in results
                    if isinstance(
                        r,
                        Exception
                    )
                )

                elapsed = (
                    time.time()
                    - start_time
                )

                logging.info(
                    f"🔎 Скан #{scan_counter} | "
                    f"{elapsed:.1f}с | "
                    f"Пар: {len(symbols_dict)} | "
                    f"Полок: {len(ACTIVE_SHELVES)} | "
                    f"Сигналов: {signals} | "
                    f"Ошибок: {errors}"
                )

            except asyncio.CancelledError:

                break

            except Exception as e:

                logging.error(
                    f"❌ Ошибка сканера: {e}"
                )

            await asyncio.sleep(
                CHECK_INTERVAL_SECONDS
            )


# ============================================================
#                         MAIN
# ============================================================

async def main():

    bot = Bot(
        token=BOT_TOKEN
    )

    app = web.Application()

    app.router.add_get(
        "/",
        health_check
    )

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT
    )

    await site.start()

    logging.info(
        f"🌐 Сервер запущен | PORT {PORT}"
    )

    try:

        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                "🤖 <b>ПАРТИЗАН v2 запущен</b>\n\n"
                "📦 Фиксированная полка 1H\n"
                "⚡ EMA20/40/80 squeeze\n"
                "🚀 Ранний 5M breakout\n"
                "📊 RVOL 5M\n"
                "💰 OI Bybit\n"
                "🛡 Защита от позднего входа"
            ),
            parse_mode="HTML"
        )

    except Exception as e:

        logging.error(
            f"❌ Ошибка стартового сообщения: {e}"
        )

    try:

        await scanner_loop(bot)

    finally:

        await runner.cleanup()

        await bot.session.close()


# ============================================================
#                        START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        pass
