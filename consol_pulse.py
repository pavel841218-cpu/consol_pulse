import os
import time
import asyncio
import logging
from collections import deque

import aiohttp
import pandas as pd
from flask import Flask
import threading


# ============================================================
#                    CONFIGURATION
#              HEAVY ARTILLERY / LIQUIDITY SHELF
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))


# ============================================================
#                    MAIN SETTINGS
# ============================================================

CHECK_INTERVAL = 30

MIN_24H_VOLUME_USDT = 800_000

# Минимальный OI для кандидата
MIN_OPEN_INTEREST_USDT = 1_000_000

# Максимальный рост за 24ч.
# Нам нужны ещё не полностью перегретые монеты.
MAX_24H_CHANGE = 50.0


# ============================================================
#                    LIQUIDITY SHELF
# ============================================================

SHELF_CANDLES = 24

# Полка должна быть относительно узкой
MAX_SHELF_RANGE_PCT = 6.0

# EMA20 / EMA40 не должны сильно расходиться
MAX_EMA_SPREAD_PCT = 6.0

# Цена должна находиться недалеко от EMA20
MAX_PRICE_EMA20_DISTANCE_PCT = 5.0

# Минимальная длина базы
MIN_SHELF_HOURS = 5

# Максимальная длина базы
MAX_SHELF_HOURS = 24


# ============================================================
#                    RVOL
# ============================================================

SHORT_RVOL_INTERVAL = "5m"
SHORT_RVOL_LOOKBACK = 12
SHORT_RVOL_RECENT_COUNT = 3

# RVOL для ARMED не должен быть высоким.
# Нам интересен ещё ранний рынок.
MIN_ARMED_RVOL = 0.65

# PRE-BREAKOUT
PREBREAK_RVOL = 1.20

# BREAKOUT
BREAKOUT_RVOL = 1.50

# Сильный пробой
STRONG_BREAKOUT_RVOL = 2.50


# ============================================================
#                    BREAKOUT
# ============================================================

# Насколько цена должна выйти за верх/низ полки
BREAKOUT_BUFFER_PCT = 0.15

# Минимальный импульс после выхода
BREAKOUT_MIN_MOVE_PCT = 1.0


# ============================================================
#                    FOLLOW
# ============================================================

FOLLOW_STEP_PCT = 2.0

FOLLOW_COOLDOWN = 900

MAX_FOLLOW_ALERTS = 12


# ============================================================
#                    WATCH SETTINGS
# ============================================================

# Сколько времени держим монету под наблюдением
WATCH_TTL = 6 * 3600

# Не отправляем повторный ARMED слишком часто
ARMED_ALERT_COOLDOWN = 3600

# Максимальное количество одновременно наблюдаемых монет
MAX_WATCHLIST_SIZE = 100


# ============================================================
#                    API / PERFORMANCE
# ============================================================

MAX_CONCURRENT_REQUESTS = 20

REQUEST_TIMEOUT = 8


# ============================================================
#                    SYMBOL FILTERS
# ============================================================

NON_CRYPTO_PREFIXES = (
    "NCS", "NCCO", "SP500", "US30", "US100", "NAS100",
    "GER40", "UK100", "JPN225", "GOLD", "SILVER",
    "BRENT", "WTI", "OIL", "XAU", "XAG", "XTI",
    "XBR", "EUR", "GBP", "JPY", "AUD", "CAD",
    "CHF", "HK", "DXY"
)

EXCLUDED_SYMBOLS = {
    "USDC-USDT",
    "FDUSD-USDT",
    "USD1-USDT"
}


# ============================================================
#                    MEMORY
# ============================================================

VALID_FUTURES_SYMBOLS = set()
ACTIVE_SYMBOLS = set()

WATCHLIST = {}

PRICE_HISTORY = {}


# ============================================================
#                    LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ============================================================
#                    FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return (
        f"HeavyArtillery | "
        f"Watch={len(WATCHLIST)} | "
        f"Symbols={len(ACTIVE_SYMBOLS)} | "
        f"OI>=${MIN_OPEN_INTEREST_USDT:,.0f}"
    ), 200


def run_flask():
    app.run(
        host="0.0.0.0",
        port=PORT
    )


# ============================================================
#                    HELPERS
# ============================================================

def normalize_symbol(raw):
    symbol = str(raw or "").strip().upper()

    if not symbol:
        return None

    if symbol.endswith("-USDT"):
        return symbol

    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"

    return None


def is_crypto_usdt_symbol(symbol):

    if not symbol:
        return False

    if not symbol.endswith("-USDT"):
        return False

    if symbol in EXCLUDED_SYMBOLS:
        return False

    base = symbol[:-5]

    if any(base.startswith(prefix) for prefix in NON_CRYPTO_PREFIXES):
        return False

    if any(char in base for char in ("(", ")", "/")):
        return False

    return True


def format_price(price):

    if price >= 1000:
        return f"{price:.2f}"

    if price >= 1:
        return f"{price:.4f}"

    if price >= 0.01:
        return f"{price:.6f}"

    if price >= 0.0001:
        return f"{price:.8f}"

    return f"{price:.10f}"


def safe_float(value, default=0.0):

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ============================================================
#                    KLINE PARSER
# ============================================================

def parse_kline(k):

    try:

        if isinstance(k, dict):

            timestamp = (
                k.get("time")
                or k.get("timestamp")
                or k.get("openTime")
                or 0
            )

            o = safe_float(k.get("open"))
            h = safe_float(k.get("high"))
            l = safe_float(k.get("low"))
            c = safe_float(k.get("close"))
            v = safe_float(k.get("volume"))

            return int(timestamp), o, h, l, c, v

        if isinstance(k, (list, tuple)):

            return (
                int(k[0]),
                safe_float(k[1]),
                safe_float(k[2]),
                safe_float(k[3]),
                safe_float(k[4]),
                safe_float(k[5])
            )

    except Exception:
        pass

    return 0, 0.0, 0.0, 0.0, 0.0, 0.0


# ============================================================
#                    EMA
# ============================================================

def calculate_ema(prices, period):

    if not prices:
        return 0.0

    try:
        return float(
            pd.Series(prices)
            .ewm(span=period, adjust=False)
            .mean()
            .iloc[-1]
        )
    except Exception:
        return 0.0


# ============================================================
#              FIND LIQUIDITY SHELF
# ============================================================

def find_liquidity_shelf(candles, current_price):

    """
    Ищет реальную ценовую базу.

    ВАЖНО:
    текущую незакрытую свечу исключаем.

    Возвращает:

    {
        passed,
        low,
        high,
        range_pct,
        ema20,
        ema40,
        ema_spread,
        distance_ema20,
        hours
    }
    """

    if len(candles) < 45:
        return None

    # --------------------------------------------------------
    # Используем только ЗАКРЫТЫЕ свечи.
    # Последняя свеча обычно текущая.
    # --------------------------------------------------------

    closed = candles[:-1]

    if len(closed) < 40:
        return None

    closes = [x["close"] for x in closed]

    ema20 = calculate_ema(closes, 20)
    ema40 = calculate_ema(closes, 40)

    if ema20 <= 0 or ema40 <= 0:
        return None

    ema_spread = abs(ema20 - ema40) / ema40 * 100

    if ema_spread > MAX_EMA_SPREAD_PCT:
        return None

    distance_ema20 = abs(current_price - ema20) / ema20 * 100

    if distance_ema20 > MAX_PRICE_EMA20_DISTANCE_PCT:
        return None

    # --------------------------------------------------------
    # Ищем самую свежую устойчивую базу.
    # --------------------------------------------------------

    best = None

    max_hours = min(
        MAX_SHELF_HOURS,
        len(closed) - 1
    )

    for hours in range(MIN_SHELF_HOURS, max_hours + 1):

        window = closed[-hours:]

        highs = [x["high"] for x in window]
        lows = [x["low"] for x in window]

        shelf_high = max(highs)
        shelf_low = min(lows)

        if shelf_low <= 0:
            continue

        range_pct = (
            (shelf_high - shelf_low)
            / shelf_low
            * 100
        )

        if range_pct > MAX_SHELF_RANGE_PCT:
            continue

        # ----------------------------------------------------
        # Проверяем стабильность закрытий.
        # Большинство закрытий должно быть внутри базы.
        # ----------------------------------------------------

        inside_count = 0

        for candle in window:

            close = candle["close"]

            if shelf_low <= close <= shelf_high:
                inside_count += 1

        stability = inside_count / len(window)

        if stability < 0.80:
            continue

        # ----------------------------------------------------
        # Смотрим, где находится текущая цена относительно базы
        # ----------------------------------------------------

        upper_breakout = (
            shelf_high
            * (1 + BREAKOUT_BUFFER_PCT / 100)
        )

        lower_breakout = (
            shelf_low
            * (1 - BREAKOUT_BUFFER_PCT / 100)
        )

        # Если цена уже далеко за пределами базы,
        # это уже не ARMED.
        if current_price > upper_breakout:
            continue

        if current_price < lower_breakout:
            continue

        best = {
            "low": shelf_low,
            "high": shelf_high,
            "range_pct": range_pct,
            "ema20": ema20,
            "ema40": ema40,
            "ema_spread": ema_spread,
            "distance_ema20": distance_ema20,
            "hours": hours,
            "stability": stability
        }

        # Берём самую свежую подходящую базу.
        break

    return best


# ============================================================
#                    OI
# ============================================================

async def fetch_current_open_interest_usdt(
    session,
    symbol,
    current_price
):

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v2/quote/openInterest"
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    variants = [
        symbol,
        symbol.replace("-", "")
    ]

    for sym in variants:

        try:

            async with session.get(
                url,
                params={"symbol": sym},
                headers=headers,
                timeout=REQUEST_TIMEOUT
            ) as resp:

                if resp.status != 200:
                    continue

                data = await resp.json()

                if data.get("code") != 0:
                    continue

                oi_data = data.get("data")

                if isinstance(oi_data, list):

                    if not oi_data:
                        continue

                    oi_data = oi_data[0]

                if not isinstance(oi_data, dict):
                    continue

                oi_value = (
                    oi_data.get("openInterestValue")
                    or oi_data.get("openInterest")
                    or oi_data.get("value")
                )

                if oi_value is None:
                    continue

                oi_value = safe_float(oi_value)

                if oi_value <= 0:
                    continue

                # Если API вернул количество контрактов
                # вместо USDT-value.
                if oi_value < 500_000 and current_price > 0:
                    oi_value *= current_price

                return oi_value

        except Exception:
            continue

    return None


# ============================================================
#                    KLINES
# ============================================================

async def get_klines(
    session,
    symbol,
    interval,
    limit
):

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v3/quote/klines"
    )

    params = {
        "symbol": symbol.replace("-", ""),
        "interval": interval,
        "limit": limit
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:

        async with session.get(
            url,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        ) as resp:

            if resp.status != 200:
                return []

            data = await resp.json()

            if data.get("code") != 0:
                return []

            candles = data.get("data", [])

            if not isinstance(candles, list):
                return []

            if len(candles) < 3:
                return []

            candles.sort(
                key=lambda x: parse_kline(x)[0]
            )

            result = []

            for k in candles:

                ts, o, h, l, c, v = parse_kline(k)

                if c <= 0:
                    continue

                result.append({
                    "timestamp": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v
                })

            return result

    except Exception:
        return []


# ============================================================
#                    SHORT RVOL
# ============================================================

async def fetch_short_rvol(
    session,
    symbol
):

    candles = await get_klines(
        session,
        symbol,
        SHORT_RVOL_INTERVAL,
        SHORT_RVOL_LOOKBACK + 8
    )

    if len(candles) < 7:
        return 1.0

    volumes = []

    for candle in candles:

        close = candle["close"]
        volume = candle["volume"]

        if close <= 0 or volume <= 0:
            continue

        volumes.append(volume * close)

    if len(volumes) < 6:
        return 1.0

    recent_count = min(
        SHORT_RVOL_RECENT_COUNT,
        len(volumes)
    )

    recent = volumes[-recent_count:]

    historical_end = len(volumes) - recent_count

    historical_start = max(
        0,
        historical_end - SHORT_RVOL_LOOKBACK
    )

    historical = volumes[
        historical_start:historical_end
    ]

    if not historical:
        return 1.0

    avg_historical = sum(historical) / len(historical)

    if avg_historical <= 0:
        return 1.0

    avg_recent = sum(recent) / len(recent)

    return avg_recent / avg_historical


# ============================================================
#                    HOURLY RVOL
# ============================================================

async def fetch_hourly_rvol(
    session,
    symbol
):

    candles = await get_klines(
        session,
        symbol,
        "1h",
        24
    )

    if len(candles) < 8:
        return 1.0

    volumes = []

    for candle in candles:

        close = candle["close"]
        volume = candle["volume"]

        if close <= 0 or volume <= 0:
            continue

        volumes.append(volume * close)

    if len(volumes) < 6:
        return 1.0

    current = volumes[-1]
    historical = volumes[:-1]

    average = sum(historical) / len(historical)

    if average <= 0:
        return 1.0

    return current / average


# ============================================================
#                    TICKERS
# ============================================================

async def update_futures_symbols(session):

    global VALID_FUTURES_SYMBOLS

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v2/quote/contracts"
    )

    try:

        async with session.get(
            url,
            timeout=REQUEST_TIMEOUT
        ) as resp:

            if resp.status != 200:
                return

            data = await resp.json()

            if data.get("code") != 0:
                return

            symbols = set()

            for item in data.get("data", []):

                symbol = normalize_symbol(
                    item.get("symbol")
                )

                if symbol and is_crypto_usdt_symbol(symbol):
                    symbols.add(symbol)

            if symbols:

                VALID_FUTURES_SYMBOLS = symbols

                logging.info(
                    "✅ Фьючерсов загружено: %d",
                    len(symbols)
                )

    except Exception as e:

        logging.warning(
            "Ошибка списка контрактов: %s",
            e
        )


async def get_market_tickers(session):

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v2/quote/ticker"
    )

    try:

        async with session.get(
            url,
            timeout=REQUEST_TIMEOUT
        ) as resp:

            if resp.status != 200:
                return []

            data = await resp.json()

            if data.get("code") != 0:
                return []

            result = []

            for item in data.get("data", []):

                symbol = normalize_symbol(
                    item.get("symbol")
                )

                if not is_crypto_usdt_symbol(symbol):
                    continue

                if (
                    VALID_FUTURES_SYMBOLS
                    and symbol not in VALID_FUTURES_SYMBOLS
                ):
                    continue

                price = safe_float(
                    item.get("lastPrice")
                )

                volume = safe_float(
                    item.get("quoteVolume")
                )

                change = safe_float(
                    item.get("priceChangePercent")
                )

                if price <= 0:
                    continue

                if volume < MIN_24H_VOLUME_USDT:
                    continue

                result.append(
                    (
                        symbol,
                        price,
                        volume,
                        change
                    )
                )

            return result

    except Exception as e:

        logging.warning(
            "Ticker error: %s",
            e
        )

        return []


# ============================================================
#              TELEGRAM
# ============================================================

async def send_telegram_alert(
    session,
    text
):

    if (
        BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN"
        or CHAT_ID == "YOUR_TELEGRAM_CHAT_ID"
    ):

        logging.info(
            "[TG MOCK]\n%s",
            text
        )

        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    try:

        async with session.post(
            url,
            json=payload,
            timeout=10
        ) as resp:

            if resp.status != 200:

                logging.error(
                    "Telegram HTTP %s: %s",
                    resp.status,
                    await resp.text()
                )

                return False

            return True

    except Exception as e:

        logging.error(
            "Telegram error: %s",
            e
        )

        return False


# ============================================================
#              SCORE
# ============================================================

def calculate_shelf_score(
    shelf,
    oi_usdt,
    short_rvol,
    hourly_rvol,
    change_24h
):

    score = 0

    # Узкая база
    if shelf["range_pct"] <= 2:
        score += 3
    elif shelf["range_pct"] <= 4:
        score += 2
    else:
        score += 1

    # EMA
    if shelf["ema_spread"] <= 2:
        score += 2
    elif shelf["ema_spread"] <= 4:
        score += 1

    # Цена около EMA20
    if shelf["distance_ema20"] <= 2:
        score += 2
    elif shelf["distance_ema20"] <= 4:
        score += 1

    # OI
    if oi_usdt >= 10_000_000:
        score += 3
    elif oi_usdt >= 5_000_000:
        score += 2
    else:
        score += 1

    # RVOL пока не обязан быть высоким
    if short_rvol >= 1.2:
        score += 2
    elif short_rvol >= 0.9:
        score += 1

    if hourly_rvol >= 1.5:
        score += 1

    # Не перегретая монета
    if change_24h <= 15:
        score += 2
    elif change_24h <= 30:
        score += 1

    return score


# ============================================================
#              ARM SYMBOL
# ============================================================

async def arm_symbol(
    session,
    symbol,
    price,
    volume_24h,
    change_24h,
    shelf,
    oi_usdt,
    short_rvol,
    hourly_rvol
):

    now = time.time()

    old = WATCHLIST.get(symbol)

    # Не переармливаем слишком часто
    if old:

        if (
            now - old.get("armed_time", 0)
            < ARMED_ALERT_COOLDOWN
        ):
            return

        # Обновляем данные
        old["price"] = price
        old["shelf"] = shelf
        old["oi"] = oi_usdt
        old["short_rvol"] = short_rvol
        old["hourly_rvol"] = hourly_rvol

        return

    if len(WATCHLIST) >= MAX_WATCHLIST_SIZE:
        return

    score = calculate_shelf_score(
        shelf,
        oi_usdt,
        short_rvol,
        hourly_rvol,
        change_24h
    )

    if score < 7:
        return

    clean = symbol.replace("-USDT", "")

    WATCHLIST[symbol] = {
        "state": "ARMED",
        "armed_time": now,
        "last_update": now,

        "price": price,

        "shelf_low": shelf["low"],
        "shelf_high": shelf["high"],

        "shelf_range": shelf["range_pct"],
        "shelf_hours": shelf["hours"],

        "ema20": shelf["ema20"],
        "ema40": shelf["ema40"],

        "oi": oi_usdt,

        "short_rvol": short_rvol,
        "hourly_rvol": hourly_rvol,

        "change_24h": change_24h,
        "volume_24h": volume_24h,

        "score": score,

        "last_alert": 0,
        "last_follow_price": price,
        "follow_count": 0,

        "direction": None
    }

    message = (
        f"🧲 <b>{clean}</b>\n\n"
        f"🎯 <b>ПОЛКА ОБНАРУЖЕНА</b>\n\n"
        f"📦 База: <b>{shelf['hours']}ч</b>\n"
        f"📏 Диапазон базы: <b>{shelf['range_pct']:.2f}%</b>\n"
        f"📈 EMA20: <b>{format_price(shelf['ema20'])}</b>\n"
        f"📉 EMA40: <b>{format_price(shelf['ema40'])}</b>\n"
        f"〽️ EMA spread: <b>{shelf['ema_spread']:.2f}%</b>\n"
        f"💰 Цена: <b>{format_price(price)}</b>\n\n"
        f"👁 OI: <b>${oi_usdt:,.0f}</b>\n"
        f"📊 RVOL 1H: <b>{hourly_rvol:.2f}x</b>\n"
        f"🔥 RVOL 5m: <b>{short_rvol:.2f}x</b>\n"
        f"📊 24h: <b>{change_24h:+.2f}%</b>\n\n"
        f"🏆 <b>Оценка полки: {score}/15</b>\n\n"
        f"👀 <i>Монета поставлена под наблюдение.</i>"
    )

    await send_telegram_alert(
        session,
        message
    )

    logging.info(
        "🧲 ARMED | %s | score=%d | shelf=%.2f%% | OI=$%.0f | RVOL5=%.2fx",
        clean,
        score,
        shelf["range_pct"],
        oi_usdt,
        short_rvol
    )


# ============================================================
#              UPDATE WATCHED SYMBOL
# ============================================================

async def update_watched_symbol(
    session,
    symbol,
    price,
    change_24h,
    volume_24h
):

    item = WATCHLIST.get(symbol)

    if not item:
        return

    now = time.time()

    item["last_update"] = now
    item["price"] = price
    item["change_24h"] = change_24h
    item["volume_24h"] = volume_24h

    # --------------------------------------------------------
    # TTL
    # --------------------------------------------------------

    if now - item["armed_time"] > WATCH_TTL:

        logging.info(
            "⌛ WATCH expired | %s",
            symbol
        )

        del WATCHLIST[symbol]
        return

    # --------------------------------------------------------
    # Получаем актуальные RVOL / OI
    # --------------------------------------------------------

    short_rvol_task = fetch_short_rvol(
        session,
        symbol
    )

    oi_task = fetch_current_open_interest_usdt(
        session,
        symbol,
        price
    )

    short_rvol, oi_usdt = await asyncio.gather(
        short_rvol_task,
        oi_task
    )

    if oi_usdt is not None:
        item["oi"] = oi_usdt

    item["short_rvol"] = short_rvol

    # --------------------------------------------------------
    # Проверка пробоя
    # --------------------------------------------------------

    shelf_high = item["shelf_high"]
    shelf_low = item["shelf_low"]

    upper_breakout = (
        shelf_high
        * (1 + BREAKOUT_BUFFER_PCT / 100)
    )

    lower_breakout = (
        shelf_low
        * (1 - BREAKOUT_BUFFER_PCT / 100)
    )

    # ========================================================
    #                    LONG BREAKOUT
    # ========================================================

    if price >= upper_breakout:

        move_pct = (
            (price - shelf_high)
            / shelf_high
            * 100
        )

        # Если объём ещё слабый — PRE-BREAKOUT
        if (
            short_rvol >= PREBREAK_RVOL
            and item["state"] == "ARMED"
        ):

            item["state"] = "PRE-BREAKOUT"

            clean = symbol.replace("-USDT", "")

            message = (
                f"⚡ <b>{clean}</b>\n\n"
                f"🟡 <b>PRE-BREAKOUT</b>\n\n"
                f"📈 Цена выходит из базы\n"
                f"💰 {format_price(price)}\n"
                f"📦 Верх базы: {format_price(shelf_high)}\n"
                f"🔥 RVOL 5m: <b>{short_rvol:.2f}x</b>\n"
                f"👁 OI: <b>${item.get('oi', 0):,.0f}</b>\n\n"
                f"⏳ <i>Ждём подтверждение объёмом.</i>"
            )

            await send_telegram_alert(
                session,
                message
            )

        # ----------------------------------------------------
        # CONFIRMED BREAKOUT
        # ----------------------------------------------------

        if short_rvol >= BREAKOUT_RVOL:

            if move_pct < BREAKOUT_MIN_MOVE_PCT:
                return

            item["state"] = "FOLLOW"
            item["direction"] = "UP"
            item["follow_count"] = 1
            item["last_follow_price"] = price
            item["last_alert"] = now

            clean = symbol.replace("-USDT", "")

            if short_rvol >= STRONG_BREAKOUT_RVOL:
                power = "🔥🔥 СИЛЬНЕЙШИЙ"
            else:
                power = "🚀 ПОДТВЕРЖДЁННЫЙ"

            message = (
                f"🚀 <b>{clean}</b>\n\n"
                f"{power} <b>ПРОБОЙ ПОЛКИ</b>\n\n"
                f"📈 Выход: <b>+{move_pct:.2f}%</b>\n"
                f"💰 Цена: <b>{format_price(price)}</b>\n"
                f"📦 Верх базы: <b>{format_price(shelf_high)}</b>\n"
                f"🔥 RVOL 5m: <b>{short_rvol:.2f}x</b>\n"
                f"👁 OI: <b>${item.get('oi', 0):,.0f}</b>\n"
                f"📊 24h: <b>{change_24h:+.2f}%</b>\n\n"
                f"⚡ <i>Начинаем сопровождение.</i>"
            )

            await send_telegram_alert(
                session,
                message
            )

            logging.info(
                "🚀 BREAKOUT UP | %s | %.2f%% | RVOL %.2fx",
                clean,
                move_pct,
                short_rvol
            )

            return

    # ========================================================
    #                    SHORT BREAKOUT
    # ========================================================

    if price <= lower_breakout:

        move_pct = (
            (shelf_low - price)
            / shelf_low
            * 100
        )

        if (
            short_rvol >= PREBREAK_RVOL
            and item["state"] == "ARMED"
        ):

            item["state"] = "PRE-BREAKOUT"

            clean = symbol.replace("-USDT", "")

            message = (
                f"⚡ <b>{clean}</b>\n\n"
                f"🟡 <b>PRE-BREAKOUT DOWN</b>\n\n"
                f"📉 Цена выходит вниз из базы\n"
                f"💰 {format_price(price)}\n"
                f"📦 Низ базы: {format_price(shelf_low)}\n"
                f"🔥 RVOL 5m: <b>{short_rvol:.2f}x</b>\n"
                f"👁 OI: <b>${item.get('oi', 0):,.0f}</b>"
            )

            await send_telegram_alert(
                session,
                message
            )

        if short_rvol >= BREAKOUT_RVOL:

            if move_pct < BREAKOUT_MIN_MOVE_PCT:
                return

            item["state"] = "FOLLOW"
            item["direction"] = "DOWN"
            item["follow_count"] = 1
            item["last_follow_price"] = price
            item["last_alert"] = now

            clean = symbol.replace("-USDT", "")

            message = (
                f"🔻 <b>{clean}</b>\n\n"
                f"💥 <b>ПРОБОЙ ПОЛКИ ВНИЗ</b>\n\n"
                f"📉 Выход: <b>-{move_pct:.2f}%</b>\n"
                f"💰 Цена: <b>{format_price(price)}</b>\n"
                f"📦 Низ базы: <b>{format_price(shelf_low)}</b>\n"
                f"🔥 RVOL 5m: <b>{short_rvol:.2f}x</b>\n"
                f"👁 OI: <b>${item.get('oi', 0):,.0f}</b>\n\n"
                f"⚡ <i>Начинаем сопровождение.</i>"
            )

            await send_telegram_alert(
                session,
                message
            )

            return

    # ========================================================
    #                    FOLLOW
    # ========================================================

    if item["state"] != "FOLLOW":
        return

    last_price = item["last_follow_price"]

    if last_price <= 0:
        item["last_follow_price"] = price
        return

    if (
        now - item["last_alert"]
        < FOLLOW_COOLDOWN
    ):
        return

    if item["follow_count"] >= MAX_FOLLOW_ALERTS:
        return

    if item["direction"] == "UP":

        progress = (
            (price - last_price)
            / last_price
            * 100
        )

        if progress >= FOLLOW_STEP_PCT:

            item["follow_count"] += 1
            item["last_follow_price"] = price
            item["last_alert"] = now

            clean = symbol.replace("-USDT", "")

            message = (
                f"📈 <b>{clean}</b>\n\n"
                f"🚀 <b>СОПРОВОЖДЕНИЕ #{item['follow_count']}</b>\n\n"
                f"📈 Продолжение: <b>+{progress:.2f}%</b>\n"
                f"💰 Цена: <b>{format_price(price)}</b>\n"
                f"🔥 RVOL 5m: <b>{short_rvol:.2f}x</b>\n"
                f"👁 OI: <b>${item.get('oi', 0):,.0f}</b>\n\n"
                f"⚡ <i>Импульс продолжается.</i>"
            )

            await send_telegram_alert(
                session,
                message
            )

    elif item["direction"] == "DOWN":

        progress = (
            (last_price - price)
            / last_price
            * 100
        )

        if progress >= FOLLOW_STEP_PCT:

            item["follow_count"] += 1
            item["last_follow_price"] = price
            item["last_alert"] = now

            clean = symbol.replace("-USDT", "")

            message = (
                f"📉 <b>{clean}</b>\n\n"
                f"🔻 <b>СОПРОВОЖДЕНИЕ #{item['follow_count']}</b>\n\n"
                f"📉 Продолжение: <b>-{progress:.2f}%</b>\n"
                f"💰 Цена: <b>{format_price(price)}</b>\n"
                f"🔥 RVOL 5m: <b>{short_rvol:.2f}x</b>\n"
                f"👁 OI: <b>${item.get('oi', 0):,.0f}</b>"
            )

            await send_telegram_alert(
                session,
                message
            )


# ============================================================
#              CLEAN WATCHLIST
# ============================================================

def cleanup_watchlist():

    now = time.time()

    expired = []

    for symbol, item in WATCHLIST.items():

        if (
            now - item.get("armed_time", now)
            > WATCH_TTL
        ):
            expired.append(symbol)

    for symbol in expired:

        WATCHLIST.pop(
            symbol,
            None
        )

        logging.info(
            "⌛ Удалена просроченная полка: %s",
            symbol
        )


# ============================================================
#              DISCOVER NEW SHELVES
# ============================================================

async def discover_shelves(
    session,
    tickers
):

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_REQUESTS
    )

    async def scan_one(item):

        symbol, price, volume_24h, change_24h = item

        # Уже наблюдаем
        if symbol in WATCHLIST:
            return

        # Перегретые сразу отбрасываем
        if change_24h > MAX_24H_CHANGE:
            return

        async with semaphore:

            # Получаем 1H базу
            candles = await get_klines(
                session,
                symbol,
                "1h",
                48
            )

            if len(candles) < 40:
                return

            shelf = find_liquidity_shelf(
                candles,
                price
            )

            if shelf is None:
                return

            # OI
            oi_task = fetch_current_open_interest_usdt(
                session,
                symbol,
                price
            )

            # RVOL
            rvol_task = fetch_short_rvol(
                session,
                symbol
            )

            hourly_task = fetch_hourly_rvol(
                session,
                symbol
            )

            oi_usdt, short_rvol, hourly_rvol = await asyncio.gather(
                oi_task,
                rvol_task,
                hourly_task
            )

            if oi_usdt is None:
                return

            if oi_usdt < MIN_OPEN_INTEREST_USDT:
                return

            if short_rvol < MIN_ARMED_RVOL:
                return

            await arm_symbol(
                session,
                symbol,
                price,
                volume_24h,
                change_24h,
                shelf,
                oi_usdt,
                short_rvol,
                hourly_rvol
            )

    tasks = [
        scan_one(item)
        for item in tickers
    ]

    await asyncio.gather(
        *tasks,
        return_exceptions=True
    )


# ============================================================
#              UPDATE WATCHLIST
# ============================================================

async def update_watchlist(
    session,
    tickers
):

    ticker_map = {
        item[0]: item
        for item in tickers
    }

    symbols = list(WATCHLIST.keys())

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_REQUESTS
    )

    async def update_one(symbol):

        item = ticker_map.get(symbol)

        if not item:
            return

        _, price, volume, change = item

        async with semaphore:

            await update_watched_symbol(
                session,
                symbol,
                price,
                change,
                volume
            )

    await asyncio.gather(
        *[
            update_one(symbol)
            for symbol in symbols
        ],
        return_exceptions=True
    )


# ============================================================
#              STARTUP
# ============================================================

async def send_startup_message(
    session
):

    message = (
        "🟢 <b>HEAVY ARTILLERY ЗАПУЩЕНА</b>\n\n"
        "🧲 <b>Режим:</b> поиск полок ДО пампа\n\n"
        f"👁 Минимальный OI: <b>${MIN_OPEN_INTEREST_USDT:,.0f}</b>\n"
        f"📦 Размер базы: <b>{MIN_SHELF_HOURS}-{MAX_SHELF_HOURS}ч</b>\n"
        f"📏 Максимальная база: <b>{MAX_SHELF_RANGE_PCT:.1f}%</b>\n"
        f"📈 EMA spread: <b>≤ {MAX_EMA_SPREAD_PCT:.1f}%</b>\n"
        f"🔥 ARMED RVOL: <b>≥ {MIN_ARMED_RVOL:.2f}x</b>\n"
        f"⚡ PRE-BREAKOUT: <b>{PREBREAK_RVOL:.2f}x</b>\n"
        f"🚀 BREAKOUT: <b>{BREAKOUT_RVOL:.2f}x</b>\n\n"
        "🎯 <i>WATCH → ARMED → PRE-BREAKOUT → BREAKOUT → FOLLOW</i>"
    )

    await send_telegram_alert(
        session,
        message
    )


# ============================================================
#                    MAIN LOOP
# ============================================================

async def main_loop():

    global ACTIVE_SYMBOLS

    timeout = aiohttp.ClientTimeout(
        total=20
    )

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        logging.info(
            "🚀 HEAVY ARTILLERY STARTED"
        )

        await update_futures_symbols(
            session
        )

        await send_startup_message(
            session
        )

        loop_count = 0

        while True:

            started = time.time()

            loop_count += 1

            # Обновляем список контрактов
            # примерно каждый час
            if loop_count % 120 == 0:

                await update_futures_symbols(
                    session
                )

            tickers = await get_market_tickers(
                session
            )

            if tickers:

                ACTIVE_SYMBOLS = {
                    item[0]
                    for item in tickers
                }

                # ------------------------------------------------
                # 1. Сначала обновляем уже найденные полки
                # ------------------------------------------------

                await update_watchlist(
                    session,
                    tickers
                )

                # ------------------------------------------------
                # 2. Потом ищем НОВЫЕ полки
                # ------------------------------------------------

                await discover_shelves(
                    session,
                    tickers
                )

                # ------------------------------------------------
                # 3. Чистим старые
                # ------------------------------------------------

                cleanup_watchlist()

                logging.info(
                    "📊 HEAVY SCAN | пар=%d | WATCH=%d | время=%.1fs",
                    len(tickers),
                    len(WATCHLIST),
                    time.time() - started
                )

                # ------------------------------------------------
                # Выводим наблюдаемые монеты
                # ------------------------------------------------

                if WATCHLIST:

                    states = []

                    for symbol, item in WATCHLIST.items():

                        states.append(
                            f"{symbol.replace('-USDT','')}"
                            f":{item['state']}"
                        )

                    logging.info(
                        "👁 WATCHLIST: %s",
                        " | ".join(states[:30])
                    )

            else:

                logging.warning(
                    "⚠️ Ticker BingX не получен"
                )

            elapsed = time.time() - started

            sleep_time = max(
                1,
                CHECK_INTERVAL - elapsed
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
#                       START
# ============================================================

if __name__ == "__main__":

    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True
    )

    flask_thread.start()

    asyncio.run(
        main_loop()
    )
