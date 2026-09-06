import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from aiogram import Bot


# ============================================================
#                 ПАРТИЗАН — v3 LIGHT
# ============================================================
#
# ЛОГИКА:
#
# 1. 15M
# 2. Ищем полку 8–12 свечей
# 3. Границы полки считаются ТОЛЬКО ПО ТЕЛАМ
#    Тени не расширяют полку
# 4. EMA20 / EMA40 / EMA80 должны находиться
#    внутри/рядом с зоной полки
# 5. Цена должна получить реакцию от EMA-зоны
# 6. Первый выход тела свечи выше полки
# 7. RVOL = информация
# 8. OI = информация
# 9. Волатильность = информация
# 10. Поздние пробои не ловим
# 11. После сигнала полка закрывается
#
# ============================================================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = (
    os.environ.get("PUMP_BOT_TOKEN")
    or os.environ.get("BOT_TOKEN")
    or "YOUR_TELEGRAM_BOT_TOKEN"
)

CHAT_ID = (
    os.environ.get("PUMP_CHAT_ID")
    or os.environ.get("CHAT_ID")
    or "YOUR_TELEGRAM_CHAT_ID"
)

PORT = int(os.environ.get("PORT", 10000))


BINGX_BASE_URL = "https://open-api.bingx.com"
BYBIT_BASE_URL = "https://api.bybit.com"


# ============================================================
# ТАЙМФРЕЙМ
# ============================================================

TIMEFRAME = "15m"

# Нам нужно достаточно истории для EMA80
KLINE_LIMIT = 120


# ============================================================
# ПОЛКА
# ============================================================

SHELF_MIN_CANDLES = 8
SHELF_MAX_CANDLES = 12

# Ширина считается ТОЛЬКО по телам свечей
MAX_SHELF_WIDTH_PCT = 4.5

# Небольшой допуск для EMA относительно границ полки
EMA_SHELF_TOLERANCE_PCT = 1.5

# Максимальный разброс EMA20/40/80
# относительно средней EMA
MAX_EMA_CLUSTER_PCT = 3.5


# ============================================================
# ОТБОЙ ОТ EMA
# ============================================================

# Насколько близко цена должна подойти к EMA-зоне
EMA_TOUCH_TOLERANCE_PCT = 1.2

# Сколько последних свечей полки разрешаем
# использовать для поиска реакции
BOUNCE_LOOKBACK_CANDLES = 5


# ============================================================
# ПРОБОЙ
# ============================================================

# Минимальный пробой тела/закрытия над верхом полки
MIN_BREAKOUT_PCT = 0.8

# Если уже улетело выше этого значения —
# считаем вход запоздалым
MAX_BREAKOUT_PCT = 3.5


# ============================================================
# ОБЪЁМ 24H
# ============================================================

# Только защита от совсем неликвидного мусора.
# Это НЕ фильтр пампа.
MIN_24H_VOLUME_USDT = 1_000_000


# ============================================================
# RVOL
# ============================================================

RVOL_LOOKBACK = 20


# ============================================================
# ОИ
# ============================================================

# OI используется ТОЛЬКО как информация.
# Никакого reject по OI здесь нет.


# ============================================================
# СКАНЕР
# ============================================================

CHECK_INTERVAL_SECONDS = 30

MAX_CONCURRENT_REQUESTS = 10

SESSION_MAX_AGE = 1800

ALERT_COOLDOWN_SECONDS = 3 * 3600


# ============================================================
# ХРАНИЛИЩЕ
# ============================================================

last_signals = {}

scan_counter = 0


# ============================================================
# УТИЛИТЫ
# ============================================================

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def normalize_timestamp(value):
    """
    BingX может вернуть timestamp в миллисекундах.
    Приводим к секундам.
    """
    ts = safe_float(value)

    if ts <= 0:
        return 0

    if ts > 10_000_000_000:
        ts /= 1000

    return ts


def format_price(price):
    if not price:
        return "0.00"

    if price >= 1000:
        return f"{price:.2f}"

    if price >= 1:
        return f"{price:.4f}"

    if price >= 0.01:
        return f"{price:.6f}"

    return f"{price:.8f}"


def candle_body_high(candle):
    return max(candle["open"], candle["close"])


def candle_body_low(candle):
    return min(candle["open"], candle["close"])


def candle_range_pct(candle):
    if candle["low"] <= 0:
        return 0.0

    return (
        (candle["high"] - candle["low"])
        / candle["low"]
    ) * 100


# ============================================================
# EMA
# ============================================================

def calculate_ema_series(prices, period):
    if len(prices) < period:
        return []

    multiplier = 2 / (period + 1)

    ema = sum(prices[:period]) / period

    result = [ema]

    for price in prices[period:]:
        ema = (
            price * multiplier
            + ema * (1 - multiplier)
        )

        result.append(ema)

    return result


def calculate_ema_at(prices, period, index):
    """
    EMA на конкретной свече.
    """
    if index + 1 < period:
        return 0.0

    data = prices[:index + 1]

    ema_values = calculate_ema_series(data, period)

    if not ema_values:
        return 0.0

    return ema_values[-1]


# ============================================================
# CLEANUP
# ============================================================

def cleanup_storage():
    now = time.time()

    expired = [
        symbol
        for symbol, timestamp in last_signals.items()
        if now - timestamp > ALERT_COOLDOWN_SECONDS
    ]

    for symbol in expired:
        del last_signals[symbol]


# ============================================================
# HEALTH
# ============================================================

async def health_check(request):
    return web.Response(
        text="Partizan Bot Active",
        status=200
    )


# ============================================================
# BINGX TICKERS
# ============================================================

async def fetch_bingx_symbols(session):

    url = (
        f"{BINGX_BASE_URL}"
        f"/openApi/swap/v2/quote/ticker"
    )

    try:

        timeout = aiohttp.ClientTimeout(total=10)

        async with session.get(
            url,
            timeout=timeout
        ) as response:

            data = await response.json()

            if data.get("code") != 0:
                logging.error(
                    f"BingX ticker code: {data.get('code')}"
                )
                return {}

            result = {}

            for item in data.get("data", []):

                symbol = item.get("symbol", "")

                if not symbol.endswith("-USDT"):
                    continue

                # =================================================
                # Убираем явный мусор
                # =================================================

                bad_parts = [
                    "_",
                    "FOOTBALL",
                    "INDEX",
                    "STKFQ",
                    "NCFX",
                    "NCCO",
                    "NCSK"
                ]

                if any(
                    bad in symbol.upper()
                    for bad in bad_parts
                ):
                    continue

                # Левереджные токены
                if symbol.endswith(
                    (
                        "2L-USDT",
                        "2S-USDT",
                        "3L-USDT",
                        "3S-USDT",
                        "5L-USDT",
                        "5S-USDT",
                        "10L-USDT",
                        "10S-USDT"
                    )
                ):
                    continue

                volume_24h = safe_float(
                    item.get("quoteVolume")
                )

                price = safe_float(
                    item.get("lastPrice")
                )

                if volume_24h < MIN_24H_VOLUME_USDT:
                    continue

                if price <= 0:
                    continue

                # =================================================
                # ВАЖНО:
                #
                # НЕТ фильтра:
                # priceChangePercent >= 1%
                #
                # Мы хотим ловить движение ДО пампа.
                # =================================================

                result[symbol] = {
                    "volume": volume_24h,
                    "price": price
                }

            return result

    except Exception as e:

        logging.error(
            f"Ошибка получения тикеров BingX: {e}"
        )

        return {}


# ============================================================
# KLINES
# ============================================================

async def fetch_klines(
    session,
    symbol,
    semaphore
):

    url = (
        f"{BINGX_BASE_URL}"
        f"/openApi/swap/v3/quote/klines"
    )

    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": KLINE_LIMIT
    }

    async with semaphore:

        try:

            timeout = aiohttp.ClientTimeout(total=8)

            async with session.get(
                url,
                params=params,
                timeout=timeout
            ) as response:

                data = await response.json()

                candles = data.get("data", [])

                if not isinstance(candles, list):
                    return []

                parsed = []

                for k in candles:

                    # =================================================
                    # DICT FORMAT
                    # =================================================

                    if isinstance(k, dict):

                        timestamp = (
                            k.get("time")
                            or k.get("timestamp")
                            or k.get("openTime")
                            or k.get("ts")
                        )

                        parsed.append({
                            "time": normalize_timestamp(timestamp),

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

                    # =================================================
                    # ARRAY FORMAT
                    # =================================================

                    elif (
                        isinstance(k, list)
                        and len(k) >= 6
                    ):

                        parsed.append({
                            "time": normalize_timestamp(k[0]),

                            "open": safe_float(k[1]),

                            "high": safe_float(k[2]),

                            "low": safe_float(k[3]),

                            "close": safe_float(k[4]),

                            "volume": safe_float(k[5])
                        })

                # =====================================================
                # Сортировка
                # =====================================================

                parsed.sort(
                    key=lambda x: x["time"]
                )

                return parsed

        except Exception:
            return []


# ============================================================
# BYBIT OI
# ============================================================

async def get_oi_growth(
    session,
    symbol,
    semaphore
):

    bybit_symbol = (
        symbol
        .replace("-", "")
        .upper()
    )

    url = (
        f"{BYBIT_BASE_URL}"
        f"/v5/market/open-interest"
    )

    params = {
        "category": "linear",
        "symbol": bybit_symbol,
        "intervalTime": "5min",
        "limit": 2
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    async with semaphore:

        try:

            timeout = aiohttp.ClientTimeout(
                total=4
            )

            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout
            ) as response:

                if response.status != 200:
                    return 0.0, "Н/Д"

                data = await response.json()

                if data.get("retCode") != 0:
                    return 0.0, "Н/Д"

                items = (
                    data
                    .get("result", {})
                    .get("list", [])
                )

                if len(items) < 2:
                    return 0.0, "Н/Д"

                items.sort(
                    key=lambda x: safe_float(
                        x.get("timestamp")
                    ),
                    reverse=True
                )

                current_oi = safe_float(
                    items[0].get("openInterest")
                )

                previous_oi = safe_float(
                    items[1].get("openInterest")
                )

                if previous_oi <= 0:
                    return 0.0, "Н/Д"

                growth = (
                    (current_oi - previous_oi)
                    / previous_oi
                ) * 100

                return growth, "Bybit"

        except Exception:
            return 0.0, "Ошибка"


# ============================================================
# ПОИСК ПОЛКИ
# ============================================================

def find_shelf(candles):

    if len(candles) < 90:
        return None

    closed = candles[:-1]

    if len(closed) < 90:
        return None

    prices = [
        c["close"]
        for c in closed
    ]

    best = None

    # =========================================================
    # Ищем полку непосредственно перед текущей свечой.
    #
    # Проверяем несколько вариантов длины:
    # 8 / 9 / 10 / 11 / 12
    # =========================================================

    for length in range(
        SHELF_MIN_CANDLES,
        SHELF_MAX_CANDLES + 1
    ):

        start = len(closed) - length
        end = len(closed)

        shelf = closed[start:end]

        if len(shelf) != length:
            continue

        # =====================================================
        # ГРАНИЦЫ ТОЛЬКО ПО ТЕЛАМ
        #
        # High/Low НЕ используются.
        # =====================================================

        body_high = max(
            candle_body_high(c)
            for c in shelf
        )

        body_low = min(
            candle_body_low(c)
            for c in shelf
        )

        if body_low <= 0:
            continue

        width_pct = (
            (body_high - body_low)
            / body_low
        ) * 100

        if width_pct > MAX_SHELF_WIDTH_PCT:
            continue

        shelf_mid = (
            body_high + body_low
        ) / 2

        if shelf_mid <= 0:
            continue

        # =====================================================
        # EMA20 / EMA40 / EMA80
        # =====================================================

        ema_values = []

        ema_inside_count = 0

        for index in range(start, end):

            ema20 = calculate_ema_at(
                prices,
                20,
                index
            )

            ema40 = calculate_ema_at(
                prices,
                40,
                index
            )

            ema80 = calculate_ema_at(
                prices,
                80,
                index
            )

            if (
                ema20 <= 0
                or ema40 <= 0
                or ema80 <= 0
            ):
                continue

            current_emas = [
                ema20,
                ema40,
                ema80
            ]

            ema_low = min(current_emas)
            ema_high = max(current_emas)

            ema_values.append(
                current_emas
            )

            # =================================================
            # EMA должны находиться в зоне полки.
            #
            # Допуск ±1.5%
            # =================================================

            allowed_low = (
                body_low
                * (
                    1
                    - EMA_SHELF_TOLERANCE_PCT / 100
                )
            )

            allowed_high = (
                body_high
                * (
                    1
                    + EMA_SHELF_TOLERANCE_PCT / 100
                )
            )

            if (
                ema_low >= allowed_low
                and ema_high <= allowed_high
            ):
                ema_inside_count += 1

        if len(ema_values) < length:
            continue

        # =====================================================
        # Не требуем 100% идеальности.
        #
        # Большинство свечей полки должны иметь EMA
        # в районе полки.
        # =====================================================

        ema_inside_ratio = (
            ema_inside_count / length
        )

        if ema_inside_ratio < 0.60:
            continue

        # =====================================================
        # Проверяем средний разброс EMA20/40/80
        # =====================================================

        max_cluster_seen = 0.0

        for emas in ema_values:

            local_mid = sum(emas) / 3

            if local_mid <= 0:
                continue

            cluster_pct = (
                (max(emas) - min(emas))
                / local_mid
            ) * 100

            max_cluster_seen = max(
                max_cluster_seen,
                cluster_pct
            )

        # Здесь допускаем некоторый разброс.
        # Полка не обязана быть идеальной.

        if max_cluster_seen > MAX_EMA_CLUSTER_PCT:
            continue

        # =====================================================
        # Ищем отбой цены от EMA-зоны
        # =====================================================

        bounce_found = False

        bounce_index = None

        bounce_distance = None

        bounce_start = max(
            start,
            end - BOUNCE_LOOKBACK_CANDLES
        )

        for index in range(
            bounce_start,
            end
        ):

            candle = closed[index]

            ema20 = calculate_ema_at(
                prices,
                20,
                index
            )

            ema40 = calculate_ema_at(
                prices,
                40,
                index
            )

            ema80 = calculate_ema_at(
                prices,
                80,
                index
            )

            if min(
                ema20,
                ema40,
                ema80
            ) <= 0:
                continue

            ema_low = min(
                ema20,
                ema40,
                ema80
            )

            ema_high = max(
                ema20,
                ema40,
                ema80
            )

            ema_mid = (
                ema20
                + ema40
                + ema80
            ) / 3

            # =================================================
            # Цена коснулась/подошла к EMA-зоне
            # =================================================

            near_ema = (
                candle["low"]
                <= ema_high
                * (
                    1
                    + EMA_TOUCH_TOLERANCE_PCT / 100
                )
            )

            if not near_ema:
                continue

            # =================================================
            # И главное:
            # после контакта свеча должна показать
            # реакцию вверх.
            # =================================================

            bullish = (
                candle["close"]
                > candle["open"]
            )

            close_above_ema = (
                candle["close"]
                >= ema_mid
            )

            if bullish and close_above_ema:

                bounce_found = True
                bounce_index = index

                bounce_distance = (
                    (
                        candle["close"]
                        - ema_mid
                    )
                    / ema_mid
                ) * 100

                break

        if not bounce_found:
            continue

        # =====================================================
        # Кандидат найден.
        #
        # Если несколько полок — выбираем более узкую.
        # =====================================================

        quality_score = (
            width_pct
            + max_cluster_seen * 0.5
            - length * 0.05
        )

        candidate = {
            "start": start,
            "end": end,

            "length": length,

            "high": body_high,
            "low": body_low,

            "width_pct": width_pct,

            "ema_inside_ratio":
                ema_inside_ratio,

            "ema_cluster_pct":
                max_cluster_seen,

            "bounce_index":
                bounce_index,

            "bounce_distance":
                bounce_distance,

            "quality":
                quality_score
        }

        if (
            best is None
            or candidate["quality"]
            < best["quality"]
        ):
            best = candidate

    return best


# ============================================================
# ПРОВЕРКА ПЕРВОГО ПРОБОЯ
# ============================================================

def check_breakout(
    candles,
    shelf
):

    if len(candles) < 2:
        return None

    current = candles[-1]

    shelf_high = shelf["high"]

    if shelf_high <= 0:
        return None

    # =========================================================
    # ПРОВЕРЯЕМ ПРЕДЫДУЩИЕ ЗАКРЫТЫЕ СВЕЧИ
    #
    # Если до текущей свечи уже был нормальный выход
    # выше полки — текущий импульс уже НЕ первый.
    # =========================================================

    previous_closed = candles[:-1]

    check_count = min(
        4,
        len(previous_closed)
    )

    recent_previous = (
        previous_closed[-check_count:]
    )

    for candle in recent_previous:

        body_high = candle_body_high(candle)

        previous_breakout_pct = (
            (
                body_high
                - shelf_high
            )
            / shelf_high
        ) * 100

        if previous_breakout_pct >= MIN_BREAKOUT_PCT:

            return {
                "valid": False,
                "reason": "old_breakout"
            }

    # =========================================================
    # ТЕКУЩАЯ СВЕЧА
    # =========================================================

    current_body_high = candle_body_high(
        current
    )

    current_close = current["close"]

    if current_close <= 0:
        return None

    # Пробой считаем по закрытию/телу,
    # а не по тени.
    breakout_pct = (
        (
            current_close
            - shelf_high
        )
        / shelf_high
    ) * 100

    body_breakout_pct = (
        (
            current_body_high
            - shelf_high
        )
        / shelf_high
    ) * 100

    # =========================================================
    # Цена ещё не пробила полку
    # =========================================================

    if breakout_pct < MIN_BREAKOUT_PCT:
        return None

    # =========================================================
    # Уже слишком далеко убежала
    # =========================================================

    if breakout_pct > MAX_BREAKOUT_PCT:

        return {
            "valid": False,
            "reason": "late_breakout"
        }

    # =========================================================
    # Свеча должна быть зелёной
    # =========================================================

    if current["close"] <= current["open"]:

        return {
            "valid": False,
            "reason": "not_bullish"
        }

    return {
        "valid": True,

        "breakout_pct":
            breakout_pct,

        "body_breakout_pct":
            body_breakout_pct,

        "current":
            current
    }


# ============================================================
# ОТПРАВКА СИГНАЛА
# ============================================================

async def send_signal(
    bot,
    symbol,
    shelf,
    breakout,
    volume_24h,
    rvol,
    volatility,
    oi_growth,
    oi_source
):

    try:

        coin = (
            symbol
            .split("-")[0]
            .upper()
        )

        breakout_pct = (
            breakout["breakout_pct"]
        )

        current = breakout["current"]

        current_price = current["close"]

        # =====================================================
        # RVOL — ТОЛЬКО ИНФОРМАЦИЯ
        # =====================================================

        rvol_str = (
            f"<b>{rvol:.2f}x</b>"
            if rvol >= 2
            else f"<code>{rvol:.2f}x</code>"
        )

        # =====================================================
        # OI — ТОЛЬКО ИНФОРМАЦИЯ
        # =====================================================

        if oi_source in (
            "Н/Д",
            "Ошибка"
        ):

            oi_str = "<code>Н/Д</code>"

        elif oi_growth >= 0:

            oi_str = (
                f"<b>+{oi_growth:.2f}%</b> 🟢"
            )

        else:

            oi_str = (
                f"<code>{oi_growth:.2f}%</code> 🔴"
            )

        # =====================================================
        # Волатильность — ТОЛЬКО ИНФОРМАЦИЯ
        # =====================================================

        volatility_str = (
            f"<b>{volatility:.2f}x</b>"
        )

        message = (
            f"🚀 <b>ПАРТИЗАН — ПЕРВЫЙ ПРОБОЙ</b>\n\n"

            f"Монета: "
            f"<code>{coin}</code>\n"

            f"ТФ: "
            f"<b>15M</b>\n\n"

            f"💥 Пробой полки: "
            f"<b>+{breakout_pct:.2f}%</b>\n"

            f"📦 Полка: "
            f"<b>{shelf['length']} свечей</b>\n"

            f"📏 Ширина полки: "
            f"<code>{shelf['width_pct']:.2f}%</code>\n\n"

            f"📐 <b>EMA-зона</b>\n"
            f"├ EMA20 / EMA40 / EMA80\n"
            f"├ В полке: "
            f"<code>{shelf['ema_inside_ratio'] * 100:.0f}%</code>\n"
            f"└ Разброс EMA: "
            f"<code>{shelf['ema_cluster_pct']:.2f}%</code>\n\n"

            f"🔥 <b>Отбой от EMA:</b> "
            f"<code>ДА</code>\n\n"

            f"📊 <b>Информация</b>\n"
            f"├ RVOL: {rvol_str}\n"
            f"├ Волатильность: "
            f"{volatility_str}\n"
            f"└ OI: {oi_str}\n\n"

            f"📍 <b>Уровни</b>\n"
            f"├ Верх полки: "
            f"<code>{format_price(shelf['high'])}</code>\n"
            f"├ Цена: "
            f"<code>{format_price(current_price)}</code>\n"
            f"└ Объём 24ч: "
            f"<b>${volume_24h / 1_000_000:.2f}M</b>\n\n"

            f"🔗 "
            f"<a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>"
            f"Открыть BingX"
            f"</a>"
        )

        await bot.send_message(
            chat_id=CHAT_ID,
            text=message,
            parse_mode="HTML",
            disable_web_page_preview=True
        )

        return True

    except Exception as e:

        logging.error(
            f"Ошибка отправки сигнала {symbol}: {e}"
        )

        return False


# ============================================================
# ПРОВЕРКА МОНЕТЫ
# ============================================================

async def check_symbol(
    session,
    bot,
    symbol,
    volume_24h,
    semaphore
):

    now = time.time()

    # =========================================================
    # COOLDOWN
    # =========================================================

    if (
        symbol in last_signals
        and now - last_signals[symbol]
        < ALERT_COOLDOWN_SECONDS
    ):
        return "cooldown"

    # =========================================================
    # СВЕЧИ
    # =========================================================

    candles = await fetch_klines(
        session,
        symbol,
        semaphore
    )

    if len(candles) < 100:
        return "no_data"

    # =========================================================
    # ПОЛКА
    # =========================================================

    shelf = find_shelf(candles)

    if not shelf:
        return "no_shelf"

    # =========================================================
    # ПРОБОЙ
    # =========================================================

    breakout = check_breakout(
        candles,
        shelf
    )

    if not breakout:
        return "shelf"

    if not breakout.get("valid"):

        reason = breakout.get(
            "reason",
            "invalid"
        )

        if reason == "old_breakout":
            return "old_breakout"

        if reason == "late_breakout":
            return "late"

        return "breakout_rejected"

    # =========================================================
    # RVOL
    #
    # НЕ ФИЛЬТР!
    # =========================================================

    current_candle = candles[-1]

    previous_closed = candles[:-1]

    rvol_data = previous_closed[-RVOL_LOOKBACK:]

    avg_volume = 0.0

    if rvol_data:

        avg_volume = (
            sum(
                c["volume"]
                for c in rvol_data
            )
            / len(rvol_data)
        )

    if avg_volume > 0:

        rvol = (
            current_candle["volume"]
            / avg_volume
        )

    else:

        rvol = 1.0

    # =========================================================
    # ВОЛАТИЛЬНОСТЬ
    #
    # НЕ ФИЛЬТР!
    # =========================================================

    shelf_candles = candles[
        shelf["start"]:
        shelf["end"]
    ]

    shelf_ranges = [
        c["high"] - c["low"]
        for c in shelf_candles
        if c["high"] > c["low"]
    ]

    if shelf_ranges:

        avg_shelf_range = (
            sum(shelf_ranges)
            / len(shelf_ranges)
        )

    else:

        avg_shelf_range = 0.0

    current_range = (
        current_candle["high"]
        - current_candle["low"]
    )

    if avg_shelf_range > 0:

        volatility = (
            current_range
            / avg_shelf_range
        )

    else:

        volatility = 1.0

    # =========================================================
    # OI
    #
    # НЕ ФИЛЬТР!
    # =========================================================

    oi_growth, oi_source = (
        await get_oi_growth(
            session,
            symbol,
            semaphore
        )
    )

    # =========================================================
    # СИГНАЛ
    # =========================================================

    success = await send_signal(
        bot=bot,
        symbol=symbol,
        shelf=shelf,
        breakout=breakout,
        volume_24h=volume_24h,
        rvol=rvol,
        volatility=volatility,
        oi_growth=oi_growth,
        oi_source=oi_source
    )

    if success:

        last_signals[symbol] = now

        logging.info(
            f"🚀 СИГНАЛ: {symbol} | "
            f"Пробой +{breakout['breakout_pct']:.2f}% | "
            f"Полка {shelf['length']} свечей | "
            f"EMA в зоне {shelf['ema_inside_ratio'] * 100:.0f}% | "
            f"RVOL {rvol:.2f}x"
        )

        return "signal"

    return "send_error"


# ============================================================
# СКАНЕР
# ============================================================

async def scanner_loop(bot):

    global scan_counter

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_REQUESTS
    )

    while True:

        session_start_time = time.time()

        connector = aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300
        )

        try:

            async with aiohttp.ClientSession(
                connector=connector
            ) as session:

                while True:

                    scan_counter += 1

                    start_time = time.time()

                    # =================================================
                    # Периодическая очистка
                    # =================================================

                    if scan_counter % 30 == 0:
                        cleanup_storage()

                    # =================================================
                    # Перезапускаем HTTP session
                    # =================================================

                    if (
                        time.time()
                        - session_start_time
                        > SESSION_MAX_AGE
                    ):
                        logging.info(
                            "♻️ Перезапуск HTTP-сессии"
                        )
                        break

                    # =================================================
                    # ТИКЕРЫ
                    # =================================================

                    symbols = (
                        await fetch_bingx_symbols(
                            session
                        )
                    )

                    if not symbols:

                        logging.warning(
                            "⚠️ BingX не вернул монеты"
                        )

                        await asyncio.sleep(
                            CHECK_INTERVAL_SECONDS
                        )

                        continue

                    # =================================================
                    # ЗАПУСК ПРОВЕРОК
                    # =================================================

                    tasks = [
                        check_symbol(
                            session,
                            bot,
                            symbol,
                            data["volume"],
                            semaphore
                        )

                        for symbol, data
                        in symbols.items()
                    ]

                    results = await asyncio.gather(
                        *tasks,
                        return_exceptions=True
                    )

                    # =================================================
                    # СТАТИСТИКА
                    # =================================================

                    stats = {
                        "no_shelf": 0,
                        "shelf": 0,
                        "old_breakout": 0,
                        "late": 0,
                        "signal": 0,
                        "cooldown": 0,
                        "no_data": 0,
                        "breakout_rejected": 0,
                        "send_error": 0,
                        "errors": 0
                    }

                    for result in results:

                        if isinstance(
                            result,
                            Exception
                        ):

                            stats["errors"] += 1

                            continue

                        if result in stats:
                            stats[result] += 1

                    elapsed = (
                        time.time()
                        - start_time
                    )

                    # =================================================
                    # ГЛАВНЫЙ ЛОГ
                    # =================================================

                    logging.info(
                        f"🔎 Скан #{scan_counter} | "
                        f"{elapsed:.1f}с | "
                        f"Пар: {len(symbols)} | "
                        f"Полок: {stats['shelf']} | "
                        f"Сигналов: {stats['signal']}"
                    )

                    # =================================================
                    # РАСШИРЕННАЯ ДИАГНОСТИКА
                    # =================================================

                    if scan_counter % 5 == 0:

                        logging.info(
                            "📊 Диагностика | "
                            f"Полки: {stats['shelf']} | "
                            f"Старый пробой: {stats['old_breakout']} | "
                            f"Поздние: {stats['late']} | "
                            f"Нет данных: {stats['no_data']} | "
                            f"Ошибки: {stats['errors']}"
                        )

                    await asyncio.sleep(
                        CHECK_INTERVAL_SECONDS
                    )

        except asyncio.CancelledError:

            break

        except Exception as e:

            logging.error(
                f"Ошибка сканера: {e}"
            )

            await asyncio.sleep(10)


# ============================================================
# MAIN
# ============================================================

async def main():

    # =========================================================
    # ПРОВЕРКА ENV
    # =========================================================

    if (
        not BOT_TOKEN
        or BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN"
    ):

        logging.error(
            "❌ Не найден PUMP_BOT_TOKEN / BOT_TOKEN"
        )

        return

    if (
        not CHAT_ID
        or CHAT_ID == "YOUR_TELEGRAM_CHAT_ID"
    ):

        logging.error(
            "❌ Не найден PUMP_CHAT_ID / CHAT_ID"
        )

        return

    # =========================================================
    # BOT
    # =========================================================

    bot = Bot(
        token=BOT_TOKEN
    )

    # =========================================================
    # WEB SERVER
    # =========================================================

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
        f"🌐 Веб-сервер запущен на порту {PORT}"
    )

    # =========================================================
    # START MESSAGE
    # =========================================================

    try:

        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                "🤖 <b>ПАРТИЗАН v3 LIGHT запущен!</b>\n\n"
                "ТФ: 15M\n"
                "Полка: 8–12 свечей\n"
                "Границы: по телам\n"
                "EMA: 20 / 40 / 80\n"
                "Отбой от EMA: ДА\n"
                "RVOL: информация\n"
                "OI: информация\n"
                "Волатильность: информация\n\n"
                "🎯 Жду первый пробой."
            ),
            parse_mode="HTML"
        )

        logging.info(
            "📨 Стартовое сообщение отправлено"
        )

    except Exception as e:

        logging.error(
            f"❌ Ошибка стартового сообщения: {e}"
        )

    # =========================================================
    # SCANNER
    # =========================================================

    try:

        await scanner_loop(bot)

    finally:

        await runner.cleanup()

        await bot.session.close()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        logging.info(
            "🛑 Бот остановлен"
        )
