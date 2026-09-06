import os
import time
import asyncio
import logging
from collections import defaultdict

import aiohttp
from aiohttp import web


# ============================================================
#              ПАРТИЗАН — EMA / SHELF TEST v4
# ============================================================
#
# ИДЕЯ:
#
# 15M
# 240 свечей истории
# EMA20 / EMA40 / EMA80
#
# 1. Ищем свежую полку 8-12 закрытых свечей.
# 2. Границы полки считаем ТОЛЬКО по телам свечей.
# 3. EMA20/40/80 должны находиться ВНУТРИ структуры полки.
# 4. Цена должна взаимодействовать с EMA-зоной во время полки.
# 5. Не нужен идеальный "отбой" одной свечой.
# 6. После формирования полки ищем ПЕРВЫЙ выход вверх.
# 7. RVOL / OI / volatility — только информация.
# 8. Не используем дневной % как фильтр.
#
# ============================================================


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
# TIMEFRAME / HISTORY
# ============================================================

TIMEFRAME = "15m"

# Было 120.
# Теперь 240 свечей = 60 часов истории.
KLINE_LIMIT = 240


# ============================================================
# SHELF
# ============================================================

SHELF_MIN_CANDLES = 8
SHELF_MAX_CANDLES = 12

# Максимальная ширина полки.
MAX_SHELF_WIDTH_PCT = 4.5


# ============================================================
# EMA
# ============================================================

EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80

# Насколько EMA может находиться около границы тела полки.
# Это НЕ расстояние для касания цены.
EMA_SHELF_TOLERANCE_PCT = 1.0

# Максимальное расстояние между самой высокой
# и самой низкой EMA относительно цены.
MAX_EMA_CLUSTER_PCT = 4.0


# ============================================================
# ВЗАИМОДЕЙСТВИЕ ЦЕНЫ С EMA
# ============================================================

# Максимальное расстояние цены от ближайшей EMA,
# чтобы считать свечу взаимодействующей с EMA.
EMA_PRICE_DISTANCE_PCT = 0.8

# Минимальное количество взаимодействий цены с EMA
# внутри полки.
MIN_EMA_INTERACTIONS = 2

# Сколько последних свечей полки анализировать
# на предмет реакции цены.
REACTION_LOOKBACK = 5


# ============================================================
# BREAKOUT
# ============================================================

MIN_BREAKOUT_PCT = 0.8
MAX_BREAKOUT_PCT = 3.5


# ============================================================
# LIQUIDITY
# ============================================================

# Единственный настоящий рыночный фильтр.
MIN_24H_VOLUME_USDT = 1_000_000


# ============================================================
# INFORMATION ONLY
# ============================================================

RVOL_LOOKBACK = 20


# ============================================================
# SYSTEM
# ============================================================

CHECK_INTERVAL_SECONDS = 30
MAX_CONCURRENT_REQUESTS = 10

SESSION_MAX_AGE = 1800

ALERT_COOLDOWN_SECONDS = 3 * 3600


# ============================================================
# BLACKLIST
# ============================================================

BLACKLIST = {
    "USDC",
    "FDUSD",
}

BAD_PREFIXES = {
    "NCS",
    "SP500",
    "INDEX",
}

BAD_CONTAINS = {
    "_",
    "FOOTBALL",
    "INDEX",
    "STKFQ",
    "NCFX",
    "NCCO",
}

LEVERAGED_SUFFIXES = (
    "2L",
    "2S",
    "3L",
    "3S",
    "5L",
    "5S",
    "10L",
    "10S",
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("PARTIZAN")


# ============================================================
# GLOBALS
# ============================================================

session = None
session_created_at = 0

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

last_alert_time = {}

stats = {
    "scans": 0,
    "pairs": 0,
    "shelves": 0,
    "signals": 0,
    "old_breakouts": 0,
    "late": 0,
    "errors": 0,
}


# ============================================================
# HTTP SESSION
# ============================================================

async def get_session():
    global session, session_created_at

    now = time.time()

    if (
        session is None
        or session.closed
        or now - session_created_at > SESSION_MAX_AGE
    ):
        if session is not None and not session.closed:
            await session.close()

        timeout = aiohttp.ClientTimeout(total=15)

        connector = aiohttp.TCPConnector(
            limit=50,
            ssl=False,
        )

        session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": "PARTIZAN-EMA-TEST/4.0"
            },
        )

        session_created_at = now

    return session


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


def is_valid_symbol(symbol):
    s = base_symbol(symbol)

    if not s:
        return False

    if s in BLACKLIST:
        return False

    for prefix in BAD_PREFIXES:
        if s.startswith(prefix):
            return False

    for bad in BAD_CONTAINS:
        if bad in s:
            return False

    for suffix in LEVERAGED_SUFFIXES:
        if s.endswith(suffix):
            return False

    return True


# ============================================================
# EMA
# ============================================================

def calculate_ema(values, period):
    if not values or len(values) < period:
        return []

    multiplier = 2 / (period + 1)

    ema = []

    initial = sum(values[:period]) / period
    ema.append(initial)

    previous = initial

    for price in values[period:]:
        current = (
            price - previous
        ) * multiplier + previous

        ema.append(current)

        previous = current

    # Возвращаем массив такой же длины,
    # первые значения заполняем None.
    result = [None] * (period - 1)
    result.extend(ema)

    return result


# ============================================================
# KLINE PARSER
# ============================================================

def parse_kline(k):
    """
    BingX обычно:
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

    if isinstance(k, dict):
        return {
            "time": int(
                safe_float(
                    k.get("time")
                    or k.get("timestamp")
                    or k.get("t")
                )
            ),
            "open": safe_float(k.get("open") or k.get("o")),
            "high": safe_float(k.get("high") or k.get("h")),
            "low": safe_float(k.get("low") or k.get("l")),
            "close": safe_float(k.get("close") or k.get("c")),
            "volume": safe_float(k.get("volume") or k.get("v")),
        }

    return {
        "time": int(safe_float(k[0])),
        "open": safe_float(k[1]),
        "high": safe_float(k[2]),
        "low": safe_float(k[3]),
        "close": safe_float(k[4]),
        "volume": safe_float(k[5]),
    }


# ============================================================
# BINGX REQUEST
# ============================================================

async def bingx_get(path, params=None):
    url = BINGX_BASE_URL + path

    for attempt in range(3):
        try:
            sess = await get_session()

            async with semaphore:
                async with sess.get(
                    url,
                    params=params or {},
                ) as resp:

                    if resp.status != 200:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue

                    data = await resp.json()

                    return data

        except Exception as e:
            if attempt == 2:
                logger.debug(
                    "BingX error %s: %s",
                    path,
                    e,
                )

            await asyncio.sleep(
                0.5 * (attempt + 1)
            )

    return None


# ============================================================
# CONTRACTS
# ============================================================

async def fetch_contracts():

    data = await bingx_get(
        "/openApi/swap/v2/quote/contracts"
    )

    if not data:
        return []

    raw = data.get("data", [])

    result = []

    for item in raw:

        if isinstance(item, dict):
            symbol = (
                item.get("symbol")
                or item.get("contractName")
            )
        else:
            symbol = item

        if not symbol:
            continue

        symbol = normalize_symbol(symbol)

        if not symbol.endswith("-USDT"):
            continue

        if not is_valid_symbol(symbol):
            continue

        result.append(symbol)

    return sorted(set(result))


# ============================================================
# 24H TICKERS
# ============================================================

async def fetch_tickers():

    data = await bingx_get(
        "/openApi/swap/v2/quote/ticker"
    )

    if not data:
        return {}

    raw = data.get("data", [])

    if isinstance(raw, dict):
        raw = [raw]

    result = {}

    for item in raw:

        symbol = normalize_symbol(
            item.get("symbol", "")
        )

        if not symbol:
            continue

        volume = safe_float(
            item.get("quoteVolume")
            or item.get("volume")
            or item.get("turnover")
        )

        last_price = safe_float(
            item.get("lastPrice")
            or item.get("last")
            or item.get("price")
        )

        if volume < MIN_24H_VOLUME_USDT:
            continue

        if last_price <= 0:
            continue

        result[symbol] = {
            "volume24h": volume,
            "price": last_price,
            "change": safe_float(
                item.get("priceChangePercent")
                or item.get("changePercent")
            ),
        }

    return result


# ============================================================
# KLINES
# ============================================================

async def fetch_klines(symbol):

    raw_symbol = symbol.replace("-USDT", "-USDT")

    data = await bingx_get(
        "/openApi/swap/v3/quote/klines",
        {
            "symbol": raw_symbol,
            "interval": TIMEFRAME,
            "limit": KLINE_LIMIT,
        },
    )

    if not data:
        return []

    raw = data.get("data", [])

    if not isinstance(raw, list):
        return []

    candles = []

    for k in raw:
        try:
            candle = parse_kline(k)

            if (
                candle["open"] <= 0
                or candle["close"] <= 0
            ):
                continue

            candles.append(candle)

        except Exception:
            continue

    candles.sort(key=lambda x: x["time"])

    return candles


# ============================================================
# BODY HIGH / BODY LOW
# ============================================================

def body_high(candle):
    return max(
        candle["open"],
        candle["close"],
    )


def body_low(candle):
    return min(
        candle["open"],
        candle["close"],
    )


# ============================================================
# SHELF
# ============================================================

def calculate_shelf(candles):

    if not candles:
        return None

    highs = [
        body_high(c)
        for c in candles
    ]

    lows = [
        body_low(c)
        for c in candles
    ]

    top = max(highs)
    bottom = min(lows)

    if bottom <= 0:
        return None

    width_pct = (
        (top - bottom)
        / bottom
        * 100
    )

    if width_pct > MAX_SHELF_WIDTH_PCT:
        return None

    return {
        "top": top,
        "bottom": bottom,
        "width_pct": width_pct,
    }


# ============================================================
# EMA POSITION
# ============================================================

def ema_inside_shelf(
    ema,
    shelf,
):
    if ema is None:
        return False

    top = shelf["top"]
    bottom = shelf["bottom"]

    tolerance = ema * (
        EMA_SHELF_TOLERANCE_PCT / 100
    )

    return (
        bottom - tolerance
        <= ema
        <= top + tolerance
    )


# ============================================================
# EMA STRUCTURE
# ============================================================

def analyze_ema_structure(
    shelf_candles,
    ema20,
    ema40,
    ema80,
):

    if not shelf_candles:
        return None

    shelf = calculate_shelf(shelf_candles)

    if not shelf:
        return None

    checks = {
        "ema20": 0,
        "ema40": 0,
        "ema80": 0,
    }

    valid_count = 0

    cluster_values = []

    for i in range(len(shelf_candles)):

        if (
            i >= len(ema20)
            or i >= len(ema40)
            or i >= len(ema80)
        ):
            continue

        e20 = ema20[i]
        e40 = ema40[i]
        e80 = ema80[i]

        if (
            e20 is None
            or e40 is None
            or e80 is None
        ):
            continue

        valid_count += 1

        if ema_inside_shelf(
            e20,
            shelf,
        ):
            checks["ema20"] += 1

        if ema_inside_shelf(
            e40,
            shelf,
        ):
            checks["ema40"] += 1

        if ema_inside_shelf(
            e80,
            shelf,
        ):
            checks["ema80"] += 1

        cluster_values.extend(
            [e20, e40, e80]
        )

    if valid_count == 0:
        return None

    ema20_pct = (
        checks["ema20"]
        / valid_count
        * 100
    )

    ema40_pct = (
        checks["ema40"]
        / valid_count
        * 100
    )

    ema80_pct = (
        checks["ema80"]
        / valid_count
        * 100
    )

    # Все три EMA должны реально участвовать
    # в структуре полки.
    if (
        ema20_pct < 60
        or ema40_pct < 60
        or ema80_pct < 60
    ):
        return None

    if cluster_values:

        cluster_high = max(cluster_values)
        cluster_low = min(cluster_values)

        middle_price = (
            cluster_high + cluster_low
        ) / 2

        if middle_price <= 0:
            return None

        cluster_pct = (
            (cluster_high - cluster_low)
            / middle_price
            * 100
        )

        if cluster_pct > MAX_EMA_CLUSTER_PCT:
            return None

    else:
        cluster_pct = 0

    average_pct = (
        ema20_pct
        + ema40_pct
        + ema80_pct
    ) / 3

    return {
        "ema20_pct": ema20_pct,
        "ema40_pct": ema40_pct,
        "ema80_pct": ema80_pct,
        "average_pct": average_pct,
        "cluster_pct": cluster_pct,
    }


# ============================================================
# PRICE ↔ EMA INTERACTION
# ============================================================

def check_price_ema_interaction(
    shelf_candles,
    ema20,
    ema40,
    ema80,
):
    """
    Здесь принципиально НЕТ требования:
    "одна свеча должна красиво отбиться".

    Мы смотрим всю полку.

    Считаем взаимодействием:
    - тело свечи находится близко к EMA;
    - либо EMA проходит через тело свечи;
    - либо цена пересекает EMA телом.

    Это позволяет ловить реальные рваные EMA,
    как на примерах CoinGlass.
    """

    interactions = 0

    interaction_details = []

    for i, candle in enumerate(
        shelf_candles
    ):

        if i >= len(ema20):
            continue

        e20 = ema20[i]
        e40 = ema40[i]
        e80 = ema80[i]

        if (
            e20 is None
            or e40 is None
            or e80 is None
        ):
            continue

        body_hi = body_high(candle)
        body_lo = body_low(candle)

        close = candle["close"]
        open_price = candle["open"]

        candle_emas = [
            ("EMA20", e20),
            ("EMA40", e40),
            ("EMA80", e80),
        ]

        touched = []

        for name, ema in candle_emas:

            distance_pct = (
                abs(close - ema)
                / ema
                * 100
            )

            # EMA проходит внутри тела
            inside_body = (
                body_lo <= ema <= body_hi
            )

            # Цена находится рядом
            near_price = (
                distance_pct
                <= EMA_PRICE_DISTANCE_PCT
            )

            # Тело пересекло EMA
            crossed = (
                body_lo <= ema
                and body_hi >= ema
            )

            if (
                inside_body
                or near_price
                or crossed
            ):
                touched.append(name)

        if touched:

            interactions += 1

            interaction_details.append({
                "index": i,
                "emas": touched,
                "bullish": close > open_price,
            })

    # Нужны реальные взаимодействия,
    # а не одно случайное касание.
    if interactions < MIN_EMA_INTERACTIONS:
        return None

    # Последние свечи полки должны показывать
    # хотя бы какую-то бычью реакцию.
    recent = interaction_details[
        -REACTION_LOOKBACK:
    ]

    bullish_reactions = sum(
        1
        for x in recent
        if x["bullish"]
    )

    if bullish_reactions < 1:
        return None

    return {
        "interactions": interactions,
        "bullish_reactions": bullish_reactions,
        "details": interaction_details,
    }


# ============================================================
# FIND SHELF
# ============================================================

def find_shelf(
    candles,
    ema20,
    ema40,
    ema80,
):

    # Последняя свеча считается текущей
    # и не входит в полку.
    closed = candles[:-1]

    if len(closed) < SHELF_MAX_CANDLES:
        return None

    # Проверяем сначала более длинные полки.
    for length in range(
        SHELF_MAX_CANDLES,
        SHELF_MIN_CANDLES - 1,
        -1,
    ):

        start = len(closed) - length
        end = len(closed)

        shelf_candles = closed[
            start:end
        ]

        shelf = calculate_shelf(
            shelf_candles
        )

        if not shelf:
            continue

        ema_structure = (
            analyze_ema_structure(
                shelf_candles,
                ema20[start:end],
                ema40[start:end],
                ema80[start:end],
            )
        )

        if not ema_structure:
            continue

        interaction = (
            check_price_ema_interaction(
                shelf_candles,
                ema20[start:end],
                ema40[start:end],
                ema80[start:end],
            )
        )

        if not interaction:
            continue

        return {
            "start": start,
            "end": end,
            "length": length,
            "top": shelf["top"],
            "bottom": shelf["bottom"],
            "width_pct": shelf["width_pct"],
            "ema": ema_structure,
            "interaction": interaction,
        }

    return None


# ============================================================
# BREAKOUT
# ============================================================

def check_breakout(
    candles,
    shelf,
):

    if len(candles) < 2:
        return None

    current = candles[-1]

    shelf_top = shelf["top"]

    close = current["close"]

    if close <= shelf_top:
        return None

    breakout_pct = (
        (close - shelf_top)
        / shelf_top
        * 100
    )

    if breakout_pct < MIN_BREAKOUT_PCT:
        return None

    if breakout_pct > MAX_BREAKOUT_PCT:
        return "LATE"

    # Проверяем, не было ли уже пробоя
    # в нескольких предыдущих закрытых свечах.
    #
    # Если было — это уже не первый выход.
    previous = candles[:-1]

    check_count = min(
        5,
        len(previous)
    )

    recent_previous = previous[
        -check_count:
    ]

    for candle in recent_previous:

        old_body_high = body_high(candle)

        old_breakout_pct = (
            (old_body_high - shelf_top)
            / shelf_top
            * 100
        )

        if old_breakout_pct >= MIN_BREAKOUT_PCT:
            return "OLD"

    return {
        "breakout_pct": breakout_pct,
        "price": close,
    }


# ============================================================
# RVOL — INFORMATION ONLY
# ============================================================

def calculate_rvol(
    candles,
):

    if len(candles) < RVOL_LOOKBACK + 1:
        return 0.0

    current = candles[-1]

    history = candles[
        -(RVOL_LOOKBACK + 1):-1
    ]

    volumes = [
        c["volume"]
        for c in history
        if c["volume"] > 0
    ]

    if not volumes:
        return 0.0

    average = (
        sum(volumes)
        / len(volumes)
    )

    if average <= 0:
        return 0.0

    return (
        current["volume"]
        / average
    )


# ============================================================
# VOLATILITY — INFORMATION ONLY
# ============================================================

def calculate_volatility(candle):

    if candle["open"] <= 0:
        return 0.0

    return (
        abs(
            candle["close"]
            - candle["open"]
        )
        / candle["open"]
        * 100
    )


# ============================================================
# BYBIT OI — INFORMATION ONLY
# ============================================================

async def fetch_oi(symbol):

    try:

        clean = symbol.replace(
            "-USDT",
            ""
        )

        url = (
            BYBIT_BASE_URL
            + "/v5/market/open-interest"
        )

        sess = await get_session()

        params = {
            "category": "linear",
            "symbol": clean + "USDT",
            "intervalTime": "15min",
            "limit": "2",
        }

        async with semaphore:

            async with sess.get(
                url,
                params=params,
            ) as resp:

                if resp.status != 200:
                    return None

                data = await resp.json()

        result = (
            data
            .get("result", {})
            .get("list", [])
        )

        if not result:
            return None

        current = safe_float(
            result[0].get("openInterest")
        )

        if len(result) < 2:
            return None

        previous = safe_float(
            result[1].get("openInterest")
        )

        if previous <= 0:
            return None

        change = (
            (current - previous)
            / previous
            * 100
        )

        return {
            "current": current,
            "change_pct": change,
        }

    except Exception:
        return None


# ============================================================
# SIGNAL
# ============================================================

async def analyze_symbol(
    symbol,
    ticker,
):

    try:

        candles = await fetch_klines(
            symbol
        )

        # Нужно достаточно истории
        # для нормальной EMA80.
        if len(candles) < 100:
            return None

        closes = [
            c["close"]
            for c in candles
        ]

        ema20 = calculate_ema(
            closes,
            EMA_FAST,
        )

        ema40 = calculate_ema(
            closes,
            EMA_MID,
        )

        ema80 = calculate_ema(
            closes,
            EMA_SLOW,
        )

        if not ema80:
            return None

        # Ищем свежую полку.
        shelf = find_shelf(
            candles,
            ema20,
            ema40,
            ema80,
        )

        if not shelf:
            return None

        stats["shelves"] += 1

        breakout = check_breakout(
            candles,
            shelf,
        )

        if breakout == "OLD":
            stats["old_breakouts"] += 1
            return None

        if breakout == "LATE":
            stats["late"] += 1
            return None

        if not breakout:
            return None

        # ----------------------------------------------------
        # Информация
        # ----------------------------------------------------

        rvol = calculate_rvol(
            candles
        )

        volatility = (
            calculate_volatility(
                candles[-1]
            )
        )

        oi = await fetch_oi(
            symbol
        )

        # ----------------------------------------------------
        # COOLDOWN
        # ----------------------------------------------------

        now = time.time()

        previous_alert = (
            last_alert_time.get(symbol, 0)
        )

        if (
            now - previous_alert
            < ALERT_COOLDOWN_SECONDS
        ):
            return None

        last_alert_time[symbol] = now

        stats["signals"] += 1

        return {
            "symbol": symbol,
            "price": breakout["price"],
            "breakout_pct": breakout[
                "breakout_pct"
            ],
            "shelf": shelf,
            "rvol": rvol,
            "volatility": volatility,
            "oi": oi,
            "volume24h": ticker.get(
                "volume24h",
                0
            ),
        }

    except Exception as e:

        stats["errors"] += 1

        logger.debug(
            "%s: %s",
            symbol,
            e,
        )

        return None


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(text):

    if (
        not BOT_TOKEN
        or BOT_TOKEN.startswith(
            "YOUR_"
        )
    ):
        logger.error(
            "BOT_TOKEN не настроен"
        )
        return False

    if (
        not CHAT_ID
        or CHAT_ID.startswith(
            "YOUR_"
        )
    ):
        logger.error(
            "CHAT_ID не настроен"
        )
        return False

    try:

        sess = await get_session()

        url = (
            "https://api.telegram.org/bot"
            + BOT_TOKEN
            + "/sendMessage"
        )

        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }

        async with sess.post(
            url,
            json=payload,
        ) as resp:

            if resp.status != 200:

                body = await resp.text()

                logger.error(
                    "Telegram %s: %s",
                    resp.status,
                    body,
                )

                return False

            return True

    except Exception as e:

        logger.error(
            "Telegram error: %s",
            e,
        )

        return False


# ============================================================
# FORMAT SIGNAL
# ============================================================

def format_signal(signal):

    shelf = signal["shelf"]

    ema = shelf["ema"]

    interaction = (
        shelf["interaction"]
    )

    oi = signal["oi"]

    if oi is None:
        oi_text = "Н/Д"
    else:
        change = oi["change_pct"]

        if change > 0:
            oi_text = (
                f"{change:+.2f}% 🟢"
            )
        elif change < 0:
            oi_text = (
                f"{change:+.2f}% 🔴"
            )
        else:
            oi_text = "0.00%"

    return (
        "🚀 ПАРТИЗАН — ПЕРВЫЙ ПРОБОЙ\n\n"

        f"Монета: {signal['symbol']}\n"
        f"ТФ: {TIMEFRAME}\n\n"

        f"⚡️ Пробой полки: "
        f"+{signal['breakout_pct']:.2f}%\n"

        f"📦 Полка: "
        f"{shelf['length']} свечей\n"

        f"📏 Ширина полки: "
        f"{shelf['width_pct']:.2f}%\n\n"

        "📐 EMA-структура\n"
        f"├ EMA20 в полке: "
        f"{ema['ema20_pct']:.0f}%\n"
        f"├ EMA40 в полке: "
        f"{ema['ema40_pct']:.0f}%\n"
        f"├ EMA80 в полке: "
        f"{ema['ema80_pct']:.0f}%\n"
        f"├ Среднее: "
        f"{ema['average_pct']:.0f}%\n"
        f"└ Разброс EMA: "
        f"{ema['cluster_pct']:.2f}%\n\n"

        "🔄 Взаимодействие цены с EMA\n"
        f"├ Взаимодействий: "
        f"{interaction['interactions']}\n"
        f"└ Бычьих реакций: "
        f"{interaction['bullish_reactions']}\n\n"

        "📊 Информация\n"
        f"├ RVOL: "
        f"{signal['rvol']:.2f}x\n"
        f"├ Волатильность: "
        f"{signal['volatility']:.2f}%\n"
        f"└ OI: {oi_text}\n\n"

        "🔴 Уровни\n"
        f"├ Верх полки: "
        f"{shelf['top']:.8f}\n"
        f"├ Цена: "
        f"{signal['price']:.8f}\n"
        f"└ Объём 24ч: "
        f"${signal['volume24h']:,.0f}\n\n"

        "🔗 Открыть BingX"
    )


# ============================================================
# PROCESS
# ============================================================

async def process_scan():

    started = time.time()

    contracts = await fetch_contracts()

    if not contracts:

        logger.error(
            "Не удалось получить контракты"
        )

        return

    tickers = await fetch_tickers()

    symbols = [
        s
        for s in contracts
        if s in tickers
    ]

    stats["pairs"] = len(symbols)

    signals = []

    async def worker(symbol):

        result = await analyze_symbol(
            symbol,
            tickers[symbol],
        )

        if result:
            signals.append(result)

    tasks = [
        worker(symbol)
        for symbol in symbols
    ]

    # Небольшими пачками,
    # чтобы не давить API.
    batch_size = MAX_CONCURRENT_REQUESTS

    for i in range(
        0,
        len(tasks),
        batch_size,
    ):

        batch = tasks[
            i:i + batch_size
        ]

        await asyncio.gather(
            *batch,
            return_exceptions=True,
        )

    elapsed = (
        time.time() - started
    )

    stats["scans"] += 1

    for signal in signals:

        logger.info(
            "🚀 СИГНАЛ: %s | "
            "Пробой +%.2f%% | "
            "Полка %d свечей | "
            "EMA20 %.0f%% | "
            "EMA40 %.0f%% | "
            "EMA80 %.0f%% | "
            "Взаимодействий %d | "
            "RVOL %.2fx",
            signal["symbol"],
            signal["breakout_pct"],
            signal["shelf"]["length"],
            signal["shelf"]["ema"]["ema20_pct"],
            signal["shelf"]["ema"]["ema40_pct"],
            signal["shelf"]["ema"]["ema80_pct"],
            signal["shelf"]["interaction"]["interactions"],
            signal["rvol"],
        )

        await send_telegram(
            format_signal(signal)
        )

    logger.info(
        "🔎 Скан #%d | %.1fs | "
        "Пар: %d | Полок: %d | "
        "Сигналов: %d | OLD: %d | LATE: %d",
        stats["scans"],
        elapsed,
        len(symbols),
        stats["shelves"],
        len(signals),
        stats["old_breakouts"],
        stats["late"],
    )


# ============================================================
# HEALTH SERVER
# ============================================================

async def health(request):

    return web.Response(
        text=(
            "PARTIZAN EMA TEST v4 OK\n"
            f"Scans: {stats['scans']}\n"
            f"Pairs: {stats['pairs']}\n"
            f"Shelves: {stats['shelves']}\n"
            f"Signals: {stats['signals']}\n"
        )
    )


async def start_server():

    app = web.Application()

    app.router.add_get(
        "/",
        health,
    )

    app.router.add_get(
        "/health",
        health,
    )

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT,
    )

    await site.start()

    logger.info(
        "🌐 HTTP сервер запущен на порту %d",
        PORT,
    )


# ============================================================
# MAIN
# ============================================================

async def main():

    logger.info(
        "=========================================="
    )

    logger.info(
        "🚀 ПАРТИЗАН EMA TEST v4"
    )

    logger.info(
        "ТФ: %s | История: %d свечей",
        TIMEFRAME,
        KLINE_LIMIT,
    )

    logger.info(
        "Полка: %d-%d свечей",
        SHELF_MIN_CANDLES,
        SHELF_MAX_CANDLES,
    )

    logger.info(
        "EMA: %d / %d / %d",
        EMA_FAST,
        EMA_MID,
        EMA_SLOW,
    )

    logger.info(
        "Взаимодействий EMA: минимум %d",
        MIN_EMA_INTERACTIONS,
    )

    logger.info(
        "RVOL/OI/волатильность: INFO ONLY"
    )

    logger.info(
        "=========================================="
    )

    await start_server()

    # Проверка Telegram при старте.
    await send_telegram(
        "🚀 ПАРТИЗАН EMA TEST v4 запущен\n\n"
        "ТФ: 15M\n"
        "История EMA: 240 свечей\n"
        "EMA: 20 / 40 / 80\n"
        "Полка: 8–12 свечей\n\n"
        "EMA/RVOL/OI логика обновлена.\n"
        "Ждём первый пробой."
    )

    while True:

        try:

            # Сбрасываем статистику полок
            # для каждого отдельного скана.
            stats["shelves"] = 0
            stats["old_breakouts"] = 0
            stats["late"] = 0

            await process_scan()

        except Exception as e:

            logger.exception(
                "Ошибка главного цикла: %s",
                e,
            )

        await asyncio.sleep(
            CHECK_INTERVAL_SECONDS
        )


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        logger.info(
            "ПАРТИЗАН остановлен"
        )
