import os
import time
import asyncio
import logging
from collections import deque

import aiohttp
from aiohttp import web


# ============================================================
#                 ПАРТИЗАН v5 — 1H SHELF
# ============================================================
#
# Идея:
#
# 1. Ищем свежую полку на 5–12 закрытых 1H свечей.
# 2. Верх/низ полки считаем ТОЛЬКО по телам свечей.
#    Тени не участвуют.
# 3. EMA20 и EMA40 должны находиться внутри полки.
# 4. EMA80 должна быть совсем рядом с полкой.
# 5. Цена должна взаимодействовать с EMA-зоной во время полки.
# 6. После полки ищем ПЕРВЫЙ нормальный импульс вверх.
# 7. Сигнал только когда цена/закрытие достигает +4%
#    от верхней границы полки.
# 8. RVOL/OI/волатильность НЕ фильтруют сигнал.
#    Они только показываются в сообщении.
# 9. Повторные ретесты не сигналим.
#
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
KLINE_LIMIT = 300

CHECK_INTERVAL_SECONDS = 30

MAX_CONCURRENT_REQUESTS = 10

SESSION_TIMEOUT = 15

# ------------------------------------------------------------
# LIQUIDITY
# ------------------------------------------------------------

MIN_24H_VOLUME_USDT = 1_000_000

# ------------------------------------------------------------
# SHELF
# ------------------------------------------------------------

SHELF_MIN_CANDLES = 5
SHELF_MAX_CANDLES = 12

# Максимальная ширина полки по ТЕЛАМ свечей
MAX_SHELF_WIDTH_PCT = 4.5

# ------------------------------------------------------------
# EMA
# ------------------------------------------------------------

EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80

# EMA20 / EMA40 должны быть внутри полки
EMA_INSIDE_TOLERANCE_PCT = 0.35

# EMA80 должна быть рядом с полкой
EMA80_MAX_DISTANCE_PCT = 2.0

# ------------------------------------------------------------
# PRICE / EMA INTERACTION
# ------------------------------------------------------------

# Насколько близко цена должна находиться к EMA-зоне
EMA_TOUCH_TOLERANCE_PCT = 1.0

# Минимальное количество свечей взаимодействия
MIN_EMA_INTERACTIONS = 2

# ------------------------------------------------------------
# BREAKOUT
# ------------------------------------------------------------

# ГЛАВНОЕ:
# Сигнал только при +4% от верхней границы полки.
BREAKOUT_TARGET_PCT = 4.0

# Смотрим максимум два часа на развитие первой волны.
BREAKOUT_MAX_CANDLES = 2

# Не считаем микродвижения за старый импульс.
OLD_BREAKOUT_MIN_PCT = 2.0

# ------------------------------------------------------------
# RVOL
# ------------------------------------------------------------

RVOL_LOOKBACK = 20

# ------------------------------------------------------------
# OI
# ------------------------------------------------------------

OI_HISTORY_SIZE = 30

# ------------------------------------------------------------
# COOLDOWN
# ------------------------------------------------------------

ALERT_COOLDOWN_SECONDS = 6 * 3600

# ------------------------------------------------------------
# EXCLUSIONS
# ------------------------------------------------------------

EXCLUDED_SYMBOLS = {
    "USDC",
    "FDUSD",
    "USD1",
    "USDT",
    "USDE",
    "TUSD",
}

BLACKLIST = {
    "IRIS-USDT",
    "IRYS-USDT",
    "LUNC-USDT",
    "USTC-USDT",
}

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("PARТIZAN")


# ============================================================
# GLOBALS
# ============================================================

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

last_alert_time = {}

oi_history = {}

stats = {
    "scans": 0,
    "symbols": 0,
    "shelf_found": 0,
    "signals": 0,
    "errors": 0,
}


# ============================================================
# HELPERS
# ============================================================

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def normalize_symbol(symbol):
    symbol = str(symbol).upper()

    if symbol.endswith("-USDT"):
        return symbol

    if symbol.endswith("USDT"):
        return symbol[:-4] + "-USDT"

    return symbol


def base_symbol(symbol):
    return normalize_symbol(symbol).replace("-USDT", "")


def pct_change(a, b):
    if not b:
        return 0.0

    return (a - b) / b * 100.0


def now_ts():
    return time.time()


def is_blacklisted(symbol):
    return normalize_symbol(symbol) in BLACKLIST


# ============================================================
# EMA
# ============================================================

def calculate_ema(values, period):
    if not values:
        return []

    if len(values) < period:
        return []

    multiplier = 2.0 / (period + 1.0)

    ema = sum(values[:period]) / period

    result = [None] * (period - 1)
    result.append(ema)

    for price in values[period:]:
        ema = (price - ema) * multiplier + ema
        result.append(ema)

    return result


# ============================================================
# KLINE PARSER
# ============================================================

def parse_kline(k):
    """
    BingX может возвращать массив:

    [
        timestamp,
        open,
        high,
        low,
        close,
        volume,
        ...
    ]
    """

    try:
        return {
            "ts": int(k[0]),
            "open": safe_float(k[1]),
            "high": safe_float(k[2]),
            "low": safe_float(k[3]),
            "close": safe_float(k[4]),
            "volume": safe_float(k[5]),
        }
    except Exception:
        return None


# ============================================================
# HTTP
# ============================================================

async def http_get(session, path, params=None):
    url = BINGX_BASE_URL + path

    async with semaphore:

        try:
            timeout = aiohttp.ClientTimeout(
                total=SESSION_TIMEOUT
            )

            async with session.get(
                url,
                params=params,
                timeout=timeout
            ) as response:

                if response.status != 200:
                    return None

                data = await response.json()

                return data

        except Exception as e:
            stats["errors"] += 1
            logger.debug("HTTP error %s: %s", path, e)
            return None


# ============================================================
# CONTRACTS
# ============================================================

async def fetch_contracts(session):

    data = await http_get(
        session,
        "/openApi/swap/v2/quote/contracts"
    )

    if not data:
        return []

    result = []

    rows = data.get("data", [])

    if isinstance(rows, dict):
        rows = rows.get("data", [])

    for item in rows:

        if not isinstance(item, dict):
            continue

        symbol = item.get("symbol")

        if not symbol:
            continue

        symbol = normalize_symbol(symbol)

        if not symbol.endswith("-USDT"):
            continue

        if base_symbol(symbol) in EXCLUDED_SYMBOLS:
            continue

        if is_blacklisted(symbol):
            continue

        result.append(symbol)

    return sorted(set(result))


# ============================================================
# TICKERS
# ============================================================

async def fetch_tickers(session):

    data = await http_get(
        session,
        "/openApi/swap/v2/quote/ticker"
    )

    if not data:
        return {}

    rows = data.get("data", [])

    if isinstance(rows, dict):
        rows = [rows]

    result = {}

    for item in rows:

        if not isinstance(item, dict):
            continue

        symbol = item.get("symbol")

        if not symbol:
            continue

        symbol = normalize_symbol(symbol)

        volume = safe_float(
            item.get("quoteVolume")
            or item.get("volume")
            or item.get("turnover")
        )

        last_price = safe_float(
            item.get("lastPrice")
            or item.get("last")
            or item.get("close")
        )

        result[symbol] = {
            "volume24h": volume,
            "price": last_price,
            "change24h": safe_float(
                item.get("priceChangePercent")
                or item.get("changePercent")
            )
        }

    return result


# ============================================================
# KLINES
# ============================================================

async def fetch_klines(session, symbol):

    data = await http_get(
        session,
        "/openApi/swap/v3/quote/klines",
        {
            "symbol": symbol,
            "interval": TIMEFRAME,
            "limit": KLINE_LIMIT,
        }
    )

    if not data:
        return []

    rows = data.get("data", [])

    if isinstance(rows, dict):
        rows = rows.get("data", [])

    result = []

    for row in rows:

        parsed = parse_kline(row)

        if parsed:
            result.append(parsed)

    result.sort(key=lambda x: x["ts"])

    return result


# ============================================================
# OPEN INTEREST
# ============================================================

async def fetch_open_interest(session, symbol):

    data = await http_get(
        session,
        "/openApi/swap/v2/quote/openInterest",
        {
            "symbol": symbol
        }
    )

    if not data:
        return 0.0

    payload = data.get("data")

    if isinstance(payload, dict):

        for key in (
            "openInterest",
            "openInterestAmount",
            "openInterestValue"
        ):

            if key in payload:
                return safe_float(payload[key])

    return 0.0


# ============================================================
# RVOL
# ============================================================

def calculate_rvol(candles):

    if len(candles) < RVOL_LOOKBACK + 1:
        return 0.0

    volumes = [
        x["volume"]
        for x in candles[-RVOL_LOOKBACK-1:-1]
        if x["volume"] > 0
    ]

    if not volumes:
        return 0.0

    average = sum(volumes) / len(volumes)

    if average <= 0:
        return 0.0

    return candles[-1]["volume"] / average


# ============================================================
# OI UPDATE
# ============================================================

def update_oi(symbol, value):

    if value <= 0:
        return None

    if symbol not in oi_history:
        oi_history[symbol] = deque(
            maxlen=OI_HISTORY_SIZE
        )

    oi_history[symbol].append(
        (now_ts(), value)
    )

    history = oi_history[symbol]

    if len(history) < 2:
        return None

    old_value = history[0][1]

    if old_value <= 0:
        return None

    return (value - old_value) / old_value * 100.0


# ============================================================
# BODY SHELF
# ============================================================

def get_body_high(candle):
    return max(
        candle["open"],
        candle["close"]
    )


def get_body_low(candle):
    return min(
        candle["open"],
        candle["close"]
    )


def find_shelf(candles):

    """
    Ищем НЕ одну фиксированную полку,
    а лучшие варианты длиной 5–12 свечей.

    Используем только тела свечей.
    """

    if len(candles) < SHELF_MAX_CANDLES + EMA_SLOW:
        return None

    # Последняя свеча должна быть текущей/развивающимся пробоем.
    # Поэтому shelf берём до неё.
    #
    # Берём несколько вариантов.
    #
    # Ищем самую свежую подходящую полку.

    best = None

    for length in range(
        SHELF_MIN_CANDLES,
        SHELF_MAX_CANDLES + 1
    ):

        # shelf заканчивается перед breakout area
        end = len(candles) - 1
        start = end - length

        if start < EMA_SLOW:
            continue

        shelf = candles[start:end]

        if len(shelf) != length:
            continue

        top = max(
            get_body_high(c)
            for c in shelf
        )

        bottom = min(
            get_body_low(c)
            for c in shelf
        )

        if bottom <= 0:
            continue

        width_pct = (
            (top - bottom) /
            bottom *
            100.0
        )

        if width_pct > MAX_SHELF_WIDTH_PCT:
            continue

        best = {
            "start": start,
            "end": end,
            "length": length,
            "top": top,
            "bottom": bottom,
            "width_pct": width_pct,
        }

        # Предпочитаем самую длинную свежую структуру,
        # но не уходим дальше 12 свечей.
        if length == SHELF_MAX_CANDLES:
            break

    return best


# ============================================================
# EMA STRUCTURE
# ============================================================

def evaluate_ema_structure(
    candles,
    shelf
):

    closes = [
        c["close"]
        for c in candles
    ]

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

    if not ema20 or not ema40 or not ema80:
        return None

    top = shelf["top"]
    bottom = shelf["bottom"]

    # Индексы shelf
    start = shelf["start"]
    end = shelf["end"]

    shelf_ema20 = [
        ema20[i]
        for i in range(start, end)
        if ema20[i] is not None
    ]

    shelf_ema40 = [
        ema40[i]
        for i in range(start, end)
        if ema40[i] is not None
    ]

    shelf_ema80 = [
        ema80[i]
        for i in range(start, end)
        if ema80[i] is not None
    ]

    if not shelf_ema20 or not shelf_ema40 or not shelf_ema80:
        return None

    def inside_pct(values):

        inside = 0

        for value in values:

            if (
                value >= bottom * (
                    1 - EMA_INSIDE_TOLERANCE_PCT / 100
                )
                and
                value <= top * (
                    1 + EMA_INSIDE_TOLERANCE_PCT / 100
                )
            ):
                inside += 1

        return inside / len(values) * 100

    ema20_inside = inside_pct(shelf_ema20)
    ema40_inside = inside_pct(shelf_ema40)

    # EMA20/40 должны быть в полке большую часть времени.
    if ema20_inside < 60:
        return None

    if ema40_inside < 60:
        return None

    # EMA80 должна быть рядом с полкой.
    avg_ema80 = sum(shelf_ema80) / len(shelf_ema80)

    if avg_ema80 < bottom:
        distance_pct = (
            (bottom - avg_ema80) /
            bottom *
            100
        )
    elif avg_ema80 > top:
        distance_pct = (
            (avg_ema80 - top) /
            top *
            100
        )
    else:
        distance_pct = 0.0

    if distance_pct > EMA80_MAX_DISTANCE_PCT:
        return None

    return {
        "ema20": ema20,
        "ema40": ema40,
        "ema80": ema80,
        "ema20_inside": ema20_inside,
        "ema40_inside": ema40_inside,
        "ema80_distance": distance_pct,
    }


# ============================================================
# PRICE / EMA INTERACTION
# ============================================================

def evaluate_ema_interaction(
    candles,
    shelf,
    ema_data
):

    ema20 = ema_data["ema20"]
    ema40 = ema_data["ema40"]
    ema80 = ema_data["ema80"]

    start = shelf["start"]
    end = shelf["end"]

    interactions = 0
    bullish_reactions = 0

    for i in range(start, end):

        candle = candles[i]

        if (
            ema20[i] is None
            or ema40[i] is None
            or ema80[i] is None
        ):
            continue

        price = candle["close"]

        zone_low = min(
            ema20[i],
            ema40[i],
            ema80[i]
        )

        zone_high = max(
            ema20[i],
            ema40[i],
            ema80[i]
        )

        zone_mid = (
            zone_low +
            zone_high
        ) / 2

        if zone_mid <= 0:
            continue

        distance_pct = abs(
            price - zone_mid
        ) / zone_mid * 100

        # Цена близко к EMA-зоне.
        if distance_pct <= EMA_TOUCH_TOLERANCE_PCT:

            interactions += 1

            # Следующая свеча должна показывать
            # хотя бы небольшой положительный отклик.
            if i + 1 < end:

                next_close = candles[i + 1]["close"]

                if next_close > price:
                    bullish_reactions += 1

    if interactions < MIN_EMA_INTERACTIONS:
        return None

    return {
        "interactions": interactions,
        "bullish_reactions": bullish_reactions,
    }


# ============================================================
# OLD IMPULSE CHECK
# ============================================================

def has_old_breakout(
    candles,
    shelf
):

    top = shelf["top"]

    start = shelf["end"]

    # Смотрим небольшую область ДО текущей breakout-свечи.
    # Если цена уже существенно уходила выше полки,
    # это может быть ретест, а не первый импульс.

    recent = candles[
        max(0, start - 3):
        start
    ]

    for candle in recent:

        high = candle["high"]

        if high <= top:
            continue

        move = pct_change(
            high,
            top
        )

        if move >= OLD_BREAKOUT_MIN_PCT:
            return True

    return False


# ============================================================
# BREAKOUT CHECK
# ============================================================

def evaluate_breakout(
    candles,
    shelf
):

    top = shelf["top"]

    # Берём свечи ПОСЛЕ полки.
    post = candles[shelf["end"]:]

    if not post:
        return None

    # Нас интересует только первая волна,
    # максимум две свечи.
    post = post[
        :BREAKOUT_MAX_CANDLES
    ]

    target = top * (
        1 +
        BREAKOUT_TARGET_PCT / 100
    )

    reached = False
    reached_candle = None

    for index, candle in enumerate(post):

        # Для качества используем CLOSE.
        # Таким образом случайный длинный wick
        # не будет сигналом.

        if candle["close"] >= target:

            reached = True
            reached_candle = index + 1
            break

    if not reached:
        return None

    # До достижения +4% цена должна двигаться вверх,
    # а не сначала уйти глубоко вниз.
    first = post[0]

    if first["close"] < top * 0.985:
        return None

    current = post[reached_candle - 1]

    move_pct = pct_change(
        current["close"],
        top
    )

    return {
        "target": target,
        "move_pct": move_pct,
        "candles": reached_candle,
        "price": current["close"],
    }


# ============================================================
# MAIN SIGNAL EVALUATION
# ============================================================

async def evaluate_symbol(
    session,
    symbol,
    ticker
):

    # --------------------------------------------------------
    # LIQUIDITY
    # --------------------------------------------------------

    volume24h = ticker.get(
        "volume24h",
        0
    )

    if volume24h < MIN_24H_VOLUME_USDT:
        return None

    # --------------------------------------------------------
    # KLINES
    # --------------------------------------------------------

    candles = await fetch_klines(
        session,
        symbol
    )

    if len(candles) < EMA_SLOW + SHELF_MAX_CANDLES + 2:
        return None

    # --------------------------------------------------------
    # EMA DATA
    # --------------------------------------------------------

    # Ищем кандидатов с разной длиной полки.
    #
    # Поскольку breakout может находиться уже после
    # нескольких свечей, проверяем свежие точки.
    #
    # Для простоты перебираем возможное окончание полки.
    #
    # Последние 3 свечи достаточно для первой волны.

    candidate_shelves = []

    max_post = min(
        2,
        len(candles) - EMA_SLOW - SHELF_MAX_CANDLES
    )

    # shelf_end_offset:
    # 0 = полка непосредственно перед текущей волной
    # 1 = одна свеча после полки
    # 2 = две свечи после полки

    for post_offset in range(0, 3):

        shelf_end = len(candles) - post_offset - 1

        if shelf_end <= EMA_SLOW:
            continue

        for length in range(
            SHELF_MIN_CANDLES,
            SHELF_MAX_CANDLES + 1
        ):

            start = shelf_end - length

            if start < EMA_SLOW:
                continue

            shelf_candles = candles[
                start:shelf_end
            ]

            if len(shelf_candles) != length:
                continue

            top = max(
                get_body_high(c)
                for c in shelf_candles
            )

            bottom = min(
                get_body_low(c)
                for c in shelf_candles
            )

            if bottom <= 0:
                continue

            width_pct = (
                (top - bottom) /
                bottom *
                100
            )

            if width_pct > MAX_SHELF_WIDTH_PCT:
                continue

            candidate_shelves.append({
                "start": start,
                "end": shelf_end,
                "length": length,
                "top": top,
                "bottom": bottom,
                "width_pct": width_pct,
            })

    if not candidate_shelves:
        return None

    # Сначала самые свежие.
    candidate_shelves.sort(
        key=lambda x: (
            x["end"],
            x["length"]
        ),
        reverse=True
    )

    for shelf in candidate_shelves:

        # ----------------------------------------------------
        # OLD BREAKOUT
        # ----------------------------------------------------

        if has_old_breakout(
            candles,
            shelf
        ):
            continue

        # ----------------------------------------------------
        # EMA STRUCTURE
        # ----------------------------------------------------

        ema_data = evaluate_ema_structure(
            candles,
            shelf
        )

        if not ema_data:
            continue

        # ----------------------------------------------------
        # PRICE / EMA INTERACTION
        # ----------------------------------------------------

        interaction = evaluate_ema_interaction(
            candles,
            shelf,
            ema_data
        )

        if not interaction:
            continue

        # ----------------------------------------------------
        # BREAKOUT
        # ----------------------------------------------------

        breakout = evaluate_breakout(
            candles,
            shelf
        )

        if not breakout:
            continue

        stats["shelf_found"] += 1

        # ----------------------------------------------------
        # RVOL INFO ONLY
        # ----------------------------------------------------

        rvol = calculate_rvol(
            candles
        )

        # ----------------------------------------------------
        # OI INFO ONLY
        # ----------------------------------------------------

        oi_value = await fetch_open_interest(
            session,
            symbol
        )

        oi_change = None

        if oi_value > 0:
            oi_change = update_oi(
                symbol,
                oi_value
            )

        # ----------------------------------------------------
        # SIGNAL
        # ----------------------------------------------------

        return {
            "symbol": symbol,
            "price": breakout["price"],

            "shelf_length": shelf["length"],
            "shelf_top": shelf["top"],
            "shelf_bottom": shelf["bottom"],
            "shelf_width": shelf["width_pct"],

            "breakout_pct": breakout["move_pct"],
            "breakout_candles": breakout["candles"],

            "ema20_inside": ema_data["ema20_inside"],
            "ema40_inside": ema_data["ema40_inside"],
            "ema80_distance": ema_data["ema80_distance"],

            "interactions": interaction["interactions"],
            "bullish_reactions": interaction["bullish_reactions"],

            "rvol": rvol,
            "oi_change": oi_change,

            "volume24h": volume24h,

            "timestamp": int(time.time()),
        }

    return None


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(
    session,
    text
):

    if not BOT_TOKEN or not CHAT_ID:
        logger.error(
            "BOT_TOKEN или CHAT_ID не заданы"
        )
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:

        async with session.post(
            url,
            json=payload,
            timeout=15
        ) as response:

            if response.status != 200:

                body = await response.text()

                logger.error(
                    "Telegram error %s: %s",
                    response.status,
                    body
                )

                return False

            return True

    except Exception as e:

        logger.error(
            "Telegram exception: %s",
            e
        )

        return False


# ============================================================
# MESSAGE
# ============================================================

def format_signal(signal):

    oi_text = "—"

    if signal["oi_change"] is not None:

        sign = "+" if signal["oi_change"] >= 0 else ""

        oi_text = (
            f"{sign}"
            f"{signal['oi_change']:.1f}%"
        )

    return (
        "🏹 ПАРТИЗАН — ПЕРВЫЙ ИМПУЛЬС\n"
        "\n"
        f"🪙 {signal['symbol']}\n"
        f"💰 Цена: {signal['price']:.8g}\n"
        "\n"
        "📦 ПОЛКА\n"
        f"• Свечей: {signal['shelf_length']}\n"
        f"• Диапазон: {signal['shelf_width']:.2f}%\n"
        f"• Верх: {signal['shelf_top']:.8g}\n"
        f"• Низ: {signal['shelf_bottom']:.8g}\n"
        "\n"
        "📈 ПРОБОЙ\n"
        f"• От полки: +{signal['breakout_pct']:.2f}%\n"
        f"• Свечей на импульс: "
        f"{signal['breakout_candles']}\n"
        "\n"
        "〰️ EMA-СТРУКТУРА\n"
        f"• EMA20 в полке: "
        f"{signal['ema20_inside']:.0f}%\n"
        f"• EMA40 в полке: "
        f"{signal['ema40_inside']:.0f}%\n"
        f"• EMA80 от полки: "
        f"{signal['ema80_distance']:.2f}%\n"
        f"• Взаимодействий: "
        f"{signal['interactions']}\n"
        f"• Бычьих реакций: "
        f"{signal['bullish_reactions']}\n"
        "\n"
        "📊 ДОП. ИНФО\n"
        f"• RVOL: {signal['rvol']:.2f}x\n"
        f"• OI: {oi_text}\n"
        f"• 24h объём: "
        f"${signal['volume24h']:,.0f}\n"
        "\n"
        "🎯 Цель отбора: +4% от верхней "
        "границы полки.\n"
        "⚠️ RVOL/OI не являются фильтрами."
    )


# ============================================================
# COOLDOWN
# ============================================================

def can_alert(symbol):

    last = last_alert_time.get(
        symbol,
        0
    )

    return (
        now_ts() - last
        >= ALERT_COOLDOWN_SECONDS
    )


def mark_alert(symbol):

    last_alert_time[symbol] = now_ts()


# ============================================================
# SCAN
# ============================================================

async def scan_market(session):

    stats["scans"] += 1

    contracts = await fetch_contracts(
        session
    )

    if not contracts:
        logger.warning(
            "Не удалось получить contracts"
        )
        return

    tickers = await fetch_tickers(
        session
    )

    if not tickers:
        logger.warning(
            "Не удалось получить tickers"
        )
        return

    symbols = [
        s
        for s in contracts
        if s in tickers
        and tickers[s]["volume24h"]
        >= MIN_24H_VOLUME_USDT
    ]

    stats["symbols"] = len(symbols)

    logger.info(
        "Сканирование: %s ликвидных пар",
        len(symbols)
    )

    # --------------------------------------------------------
    # Parallel scanning
    # --------------------------------------------------------

    tasks = []

    for symbol in symbols:

        if not can_alert(symbol):
            continue

        tasks.append(
            evaluate_symbol(
                session,
                symbol,
                tickers[symbol]
            )
        )

    if not tasks:
        return

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )

    signals = []

    for result in results:

        if isinstance(
            result,
            Exception
        ):
            continue

        if result:
            signals.append(result)

    # --------------------------------------------------------
    # Send
    # --------------------------------------------------------

    for signal in signals:

        symbol = signal["symbol"]

        if not can_alert(symbol):
            continue

        text = format_signal(
            signal
        )

        success = await send_telegram(
            session,
            text
        )

        if success:

            mark_alert(symbol)

            stats["signals"] += 1

            logger.info(
                "🚨 СИГНАЛ %s | +%.2f%% | shelf=%s",
                symbol,
                signal["breakout_pct"],
                signal["shelf_length"]
            )


# ============================================================
# HEALTH SERVER
# ============================================================

async def health(request):

    return web.json_response({
        "status": "ok",
        "bot": "PARTIZAN v5",
        "timeframe": TIMEFRAME,
        "stats": stats,
    })


async def start_server():

    app = web.Application()

    app.router.add_get(
        "/",
        health
    )

    app.router.add_get(
        "/health",
        health
    )

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT
    )

    await site.start()

    logger.info(
        "Health server started on port %s",
        PORT
    )


# ============================================================
# SELF PING
# ============================================================

async def self_ping(session):

    if not SELF_URL:
        return

    while True:

        try:

            async with session.get(
                SELF_URL,
                timeout=10
            ) as response:

                logger.debug(
                    "SELF PING: %s",
                    response.status
                )

        except Exception as e:

            logger.debug(
                "SELF PING error: %s",
                e
            )

        await asyncio.sleep(
            10 * 60
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    if not BOT_TOKEN:
        logger.warning(
            "BOT_TOKEN не задан"
        )

    if not CHAT_ID:
        logger.warning(
            "CHAT_ID не задан"
        )

    await start_server()

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        asyncio.create_task(
            self_ping(session)
        )

        logger.info(
            "🏹 ПАРТИЗАН v5 запущен"
        )

        logger.info(
            "TF=%s | shelf=%s-%s | "
            "breakout=+%.1f%%",
            TIMEFRAME,
            SHELF_MIN_CANDLES,
            SHELF_MAX_CANDLES,
            BREAKOUT_TARGET_PCT
        )

        while True:

            started = time.time()

            try:

                await scan_market(
                    session
                )

            except Exception as e:

                logger.exception(
                    "Ошибка основного цикла: %s",
                    e
                )

            elapsed = time.time() - started

            sleep_time = max(
                5,
                CHECK_INTERVAL_SECONDS - elapsed
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        logger.info(
            "ПАРТИЗАН остановлен"
        )
