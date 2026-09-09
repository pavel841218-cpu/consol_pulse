import os
import time
import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone

import aiohttp
from aiohttp import web


# ============================================================
# 🏹 ПАРТИЗАН v6 — 1H SHELF
# ============================================================
#
# ЛОГИКА:
#
# 1. Ищем свежую полку из 5–12 ЗАКРЫТЫХ 1H свечей.
# 2. Верх/низ полки считаются ТОЛЬКО по ТЕЛАМ свечей.
#    Фитили не определяют границы полки.
# 3. EMA20 и EMA40 должны находиться внутри/рядом с полкой.
# 4. EMA80 — КОНТЕКСТНАЯ линия.
#    Она может быть:
#       - внутри полки;
#       - немного ниже;
#       - немного выше;
#       - рядом с полкой.
#    Само нахождение EMA80 выше полки НЕ отменяет сигнал.
# 5. Цена должна несколько раз взаимодействовать с EMA-зоной.
# 6. После полки ищем ПЕРВЫЙ нормальный восходящий импульс.
# 7. Импульсу разрешено развиваться до 4 свечей.
# 8. Сигнал появляется при достижении +4% от верха полки.
# 9. Если после пробоя цена глубоко возвращается в полку —
#    кандидат отменяется.
# 10. RVOL / OI / волатильность НЕ ФИЛЬТРУЮТ сигнал.
#     Они только показываются в сообщении.
# 11. Повторные ретесты после уже состоявшегося импульса
#     не должны генерировать новые сигналы.
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
# РЫНОЧНЫЙ ФИЛЬТР
# ------------------------------------------------------------

MIN_24H_VOLUME_USDT = 1_000_000

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

# ------------------------------------------------------------
# ПОЛКА
# ------------------------------------------------------------

SHELF_MIN_CANDLES = 5
SHELF_MAX_CANDLES = 12

MAX_SHELF_WIDTH_PCT = 4.5

# ------------------------------------------------------------
# EMA
# ------------------------------------------------------------

EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80

# Допуск, при котором EMA20/40 считаются находящимися
# внутри полки.
EMA_INSIDE_TOLERANCE_PCT = 0.50

# EMA80 не должна быть слишком далеко от полки.
# Но она может находиться как ниже, так и выше полки.
EMA80_MAX_DISTANCE_PCT = 2.5

# ------------------------------------------------------------
# ВЗАИМОДЕЙСТВИЕ ЦЕНЫ С EMA
# ------------------------------------------------------------

EMA_TOUCH_TOLERANCE_PCT = 1.0

MIN_EMA_INTERACTIONS = 2

# ------------------------------------------------------------
# ПЕРВЫЙ ИМПУЛЬС
# ------------------------------------------------------------

BREAKOUT_TARGET_PCT = 4.0

# Было слишком жёстко: максимум 2 свечи.
# Теперь даём первому импульсу развиться до 4 закрытых свечей.
BREAKOUT_MAX_CANDLES = 4

# Минимальное закрытие первой пробойной свечи
# относительно верха полки.
FIRST_BREAKOUT_MIN_CLOSE_PCT = 0.15

# Если после пробоя цена закрывается ниже этой зоны,
# считаем импульс сломанным.
BREAKOUT_INVALIDATION_PCT = 1.5

# ------------------------------------------------------------
# СТАРЫЙ ИМПУЛЬС ДО ПОЛКИ
# ------------------------------------------------------------

OLD_BREAKOUT_LOOKBACK = 5

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
# DEBUG
# ------------------------------------------------------------

DEBUG = os.environ.get("DEBUG", "0") == "1"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("PARТIZAN_v6")


# ============================================================
# GLOBALS
# ============================================================

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

last_alert_time = {}

oi_history = defaultdict(
    lambda: deque(maxlen=OI_HISTORY_SIZE)
)

stats = {
    "scans": 0,
    "symbols": 0,
    "candidates": 0,
    "shelves": 0,

    "reject_width": 0,
    "reject_ema20": 0,
    "reject_ema40": 0,
    "reject_ema80": 0,
    "reject_interaction": 0,
    "reject_old_breakout": 0,
    "reject_breakout": 0,
    "reject_retest": 0,

    "breakout_candidates": 0,
    "targets": 0,
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


def pct_change(a, b):
    if not a:
        return 0.0

    return (b - a) / a * 100.0


def body_high(candle):
    return max(candle["open"], candle["close"])


def body_low(candle):
    return min(candle["open"], candle["close"])


# ============================================================
# EMA
# ============================================================

def calculate_ema(values, period):
    if not values:
        return []

    alpha = 2.0 / (period + 1.0)

    ema = [values[0]]

    for value in values[1:]:
        ema.append(
            alpha * value +
            (1.0 - alpha) * ema[-1]
        )

    return ema


# ============================================================
# KLINE PARSER
# ============================================================

def parse_kline(k):
    return {
        "ts": int(k[0]),
        "open": safe_float(k[1]),
        "high": safe_float(k[2]),
        "low": safe_float(k[3]),
        "close": safe_float(k[4]),
        "volume": safe_float(k[5]),
    }


# ============================================================
# HTTP
# ============================================================

async def http_get(session, path, params=None):
    url = BINGX_BASE_URL + path

    async with semaphore:
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(
                    total=SESSION_TIMEOUT
                )
            ) as response:

                if response.status != 200:
                    return None

                return await response.json()

        except Exception as e:
            if DEBUG:
                logger.debug(
                    "HTTP ошибка %s: %s",
                    path,
                    e
                )

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

    rows = data.get("data", [])

    result = []

    for row in rows:
        symbol = normalize_symbol(
            row.get("symbol", "")
        )

        if not symbol.endswith("-USDT"):
            continue

        base = base_symbol(symbol)

        if base in EXCLUDED_SYMBOLS:
            continue

        if symbol in BLACKLIST:
            continue

        result.append(symbol)

    return result


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

    result = {}

    for row in rows:
        symbol = normalize_symbol(
            row.get("symbol", "")
        )

        if not symbol.endswith("-USDT"):
            continue

        if symbol in BLACKLIST:
            continue

        base = base_symbol(symbol)

        if base in EXCLUDED_SYMBOLS:
            continue

        volume = safe_float(
            row.get("quoteVolume")
        )

        if volume < MIN_24H_VOLUME_USDT:
            continue

        result[symbol] = {
            "price": safe_float(row.get("lastPrice")),
            "volume24h": volume,
            "change24h": safe_float(
                row.get("priceChangePercent")
            ),
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

    candles = []

    for row in rows:
        try:
            candles.append(
                parse_kline(row)
            )
        except Exception:
            continue

    candles.sort(
        key=lambda x: x["ts"]
    )

    return candles


# ============================================================
# OPEN INTEREST
# ============================================================

async def fetch_open_interest(session, symbol):
    try:
        api_symbol = symbol.replace(
            "-USDT",
            "-USDT"
        )

        data = await http_get(
            session,
            "/openApi/swap/v2/quote/openInterest",
            {
                "symbol": api_symbol
            }
        )

        if not data:
            return None

        value = data.get("data")

        if isinstance(value, dict):
            value = (
                value.get("openInterest")
                or value.get("openInterestValue")
            )

        oi = safe_float(value, None)

        if oi is None:
            return None

        return oi

    except Exception:
        return None


# ============================================================
# RVOL
# ============================================================

def calculate_rvol(candles, index):
    if index <= 0:
        return 0.0

    start = max(
        0,
        index - RVOL_LOOKBACK
    )

    previous = candles[start:index]

    if not previous:
        return 0.0

    avg_volume = sum(
        c["volume"]
        for c in previous
    ) / len(previous)

    if avg_volume <= 0:
        return 0.0

    return candles[index]["volume"] / avg_volume


# ============================================================
# SHELF
# ============================================================

def build_shelf(candles, start, end):
    """
    end — индекс НЕ включается.

    Полка строится только по телам.
    """

    shelf = candles[start:end]

    if len(shelf) < SHELF_MIN_CANDLES:
        return None

    body_highs = [
        body_high(c)
        for c in shelf
    ]

    body_lows = [
        body_low(c)
        for c in shelf
    ]

    top = max(body_highs)
    bottom = min(body_lows)

    if top <= 0:
        return None

    width_pct = (
        (top - bottom) /
        top *
        100.0
    )

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


# ============================================================
# EMA STRUCTURE
# ============================================================

def evaluate_ema_structure(candles, shelf):
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

    start = shelf["start"]
    end = shelf["end"]

    top = shelf["top"]
    bottom = shelf["bottom"]

    shelf_len = end - start

    if shelf_len <= 0:
        return None

    inside20 = 0
    inside40 = 0

    ema80_distances = []

    for i in range(start, end):

        e20 = ema20[i]
        e40 = ema40[i]
        e80 = ema80[i]

        tolerance = top * (
            EMA_INSIDE_TOLERANCE_PCT / 100.0
        )

        if (
            bottom - tolerance
            <= e20
            <= top + tolerance
        ):
            inside20 += 1

        if (
            bottom - tolerance
            <= e40
            <= top + tolerance
        ):
            inside40 += 1

        # ----------------------------------------------------
        # EMA80
        # ----------------------------------------------------
        #
        # Не требуем EMA80 строго внутри.
        #
        # Считаем расстояние до ближайшей границы.
        #

        if e80 < bottom:
            distance = (
                (bottom - e80) /
                bottom *
                100.0
            )

        elif e80 > top:
            distance = (
                (e80 - top) /
                top *
                100.0
            )

        else:
            distance = 0.0

        ema80_distances.append(
            distance
        )

    inside20_pct = (
        inside20 /
        shelf_len *
        100.0
    )

    inside40_pct = (
        inside40 /
        shelf_len *
        100.0
    )

    ema80_distance = (
        sum(ema80_distances) /
        len(ema80_distances)
        if ema80_distances
        else 999.0
    )

    return {
        "ema20": ema20,
        "ema40": ema40,
        "ema80": ema80,

        "inside20_pct": inside20_pct,
        "inside40_pct": inside40_pct,

        "ema80_distance_pct":
            ema80_distance,
    }


# ============================================================
# EMA INTERACTION
# ============================================================

def evaluate_ema_interaction(
    candles,
    shelf,
    ema_data
):
    start = shelf["start"]
    end = shelf["end"]

    ema20 = ema_data["ema20"]
    ema40 = ema_data["ema40"]
    ema80 = ema_data["ema80"]

    interactions = 0
    bullish_reactions = 0

    for i in range(start, end):

        candle = candles[i]

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

        tolerance_low = (
            zone_low *
            (1.0 - EMA_TOUCH_TOLERANCE_PCT / 100.0)
        )

        tolerance_high = (
            zone_high *
            (1.0 + EMA_TOUCH_TOLERANCE_PCT / 100.0)
        )

        # ----------------------------------------------------
        # Реальное касание/пересечение EMA-зоны.
        #
        # Учитываем весь диапазон свечи,
        # а не только close.
        # ----------------------------------------------------

        touched = (
            candle["low"] <= tolerance_high
            and
            candle["high"] >= tolerance_low
        )

        if not touched:
            continue

        interactions += 1

        # ----------------------------------------------------
        # Бычья реакция.
        # ----------------------------------------------------

        if i + 1 < end:
            next_close = candles[i + 1]["close"]

            if next_close > candle["close"]:
                bullish_reactions += 1

    return {
        "interactions": interactions,
        "bullish_reactions":
            bullish_reactions,
    }


# ============================================================
# OLD BREAKOUT
# ============================================================

def has_old_breakout(
    candles,
    shelf
):
    """
    Отсекаем только настоящий старый импульс
    перед полкой.

    ВАЖНО:
    фитиль сам по себе не считается старым пампом.
    Нужен CLOSE выше верха полки минимум на 2%.
    """

    start = shelf["start"]
    top = shelf["top"]

    check_start = max(
        0,
        start - OLD_BREAKOUT_LOOKBACK
    )

    previous = candles[
        check_start:start
    ]

    for candle in previous:

        close = candle["close"]

        if (
            close >=
            top *
            (1.0 + OLD_BREAKOUT_MIN_PCT / 100.0)
        ):
            return True

    return False


# ============================================================
# FIRST IMPULSE
# ============================================================

def evaluate_first_impulse(
    candles,
    shelf
):
    """
    После полки ищем первый настоящий выход.

    Условия:

    1. Первая свеча должна закрыться хотя бы немного
       выше верха полки.
    2. Затем допускаем продолжение до 4 свечей.
    3. Для сигнала цена должна достичь +4%.
    4. Если цена глубоко возвращается под полку —
       кандидат отменяется.
    """

    end = shelf["end"]
    top = shelf["top"]

    post = candles[
        end:
        min(
            len(candles),
            end + BREAKOUT_MAX_CANDLES
        )
    ]

    if not post:
        return None

    first = post[0]

    first_breakout_level = (
        top *
        (1.0 + FIRST_BREAKOUT_MIN_CLOSE_PCT / 100.0)
    )

    # --------------------------------------------------------
    # Первая свеча должна действительно начать пробой.
    # --------------------------------------------------------

    if first["close"] < first_breakout_level:
        return None

    target = (
        top *
        (1.0 + BREAKOUT_TARGET_PCT / 100.0)
    )

    for i, candle in enumerate(post):

        # ----------------------------------------------------
        # Если закрытие глубоко вернулось под полку,
        # первый импульс сломан.
        # ----------------------------------------------------

        invalidation_level = (
            top *
            (1.0 - BREAKOUT_INVALIDATION_PCT / 100.0)
        )

        if candle["close"] < invalidation_level:
            return {
                "status": "invalidated"
            }

        # ----------------------------------------------------
        # Цель +4%.
        #
        # Используем HIGH для определения факта достижения
        # цели, потому что цена могла пройти +4% внутри свечи,
        # а закрыться немного ниже.
        # ----------------------------------------------------

        if candle["high"] >= target:

            return {
                "status": "target",
                "candles": i + 1,
                "target_pct": BREAKOUT_TARGET_PCT,
                "target_price": target,
                "high": candle["high"],
            }

    return {
        "status": "not_reached"
    }


# ============================================================
# FIND BEST SHELF
# ============================================================

def find_best_shelf(candles):
    """
    Перебираем возможные окончания полки,
    но отдаём приоритет самой свежей конструкции.

    Полка всегда состоит из закрытых свечей.
    """

    if len(candles) < 100:
        return None

    latest_end = len(candles)

    candidates = []

    # Проверяем несколько последних возможных окончаний.
    #
    # Это позволяет не потерять полку из-за одной свечи
    # и одновременно не уходить глубоко в историю.
    #
    for end in range(
        latest_end - 1,
        max(
            0,
            latest_end - BREAKOUT_MAX_CANDLES - 6
        ),
        -1
    ):

        for length in range(
            SHELF_MAX_CANDLES,
            SHELF_MIN_CANDLES - 1,
            -1
        ):

            start = end - length

            if start < EMA_SLOW:
                continue

            shelf = build_shelf(
                candles,
                start,
                end
            )

            if shelf is None:
                stats["reject_width"] += 1
                continue

            stats["shelves"] += 1

            # ------------------------------------------------
            # EMA
            # ------------------------------------------------

            ema_data = evaluate_ema_structure(
                candles,
                shelf
            )

            if not ema_data:
                continue

            # EMA20
            if ema_data["inside20_pct"] < 60.0:
                stats["reject_ema20"] += 1
                continue

            # EMA40
            if ema_data["inside40_pct"] < 60.0:
                stats["reject_ema40"] += 1
                continue

            # EMA80 — только контекст.
            if (
                ema_data["ema80_distance_pct"]
                > EMA80_MAX_DISTANCE_PCT
            ):
                stats["reject_ema80"] += 1
                continue

            # ------------------------------------------------
            # EMA INTERACTION
            # ------------------------------------------------

            interaction = evaluate_ema_interaction(
                candles,
                shelf,
                ema_data
            )

            if (
                interaction["interactions"]
                < MIN_EMA_INTERACTIONS
            ):
                stats["reject_interaction"] += 1
                continue

            # ------------------------------------------------
            # OLD BREAKOUT
            # ------------------------------------------------

            if has_old_breakout(
                candles,
                shelf
            ):
                stats["reject_old_breakout"] += 1
                continue

            # ------------------------------------------------
            # FIRST IMPULSE
            # ------------------------------------------------

            impulse = evaluate_first_impulse(
                candles,
                shelf
            )

            if not impulse:
                stats["reject_breakout"] += 1
                continue

            if impulse["status"] == "invalidated":
                stats["reject_retest"] += 1
                continue

            if impulse["status"] == "not_reached":
                continue

            if impulse["status"] == "target":

                stats["breakout_candidates"] += 1
                stats["targets"] += 1

                candidate = {
                    "shelf": shelf,
                    "ema": ema_data,
                    "interaction": interaction,
                    "impulse": impulse,
                }

                candidates.append(
                    candidate
                )

    if not candidates:
        return None

    # Самая свежая и короткая полка имеет приоритет.
    candidates.sort(
        key=lambda x: (
            x["shelf"]["end"],
            -x["shelf"]["length"]
        ),
        reverse=True
    )

    return candidates[0]


# ============================================================
# OI
# ============================================================

async def get_oi_info(
    session,
    symbol
):
    oi = await fetch_open_interest(
        session,
        symbol
    )

    if oi is None:
        return None, None

    now = time.time()

    history = oi_history[symbol]

    history.append(
        (now, oi)
    )

    if len(history) < 2:
        return oi, None

    old_time, old_oi = history[0]

    if old_oi <= 0:
        return oi, None

    growth = (
        (oi - old_oi) /
        old_oi *
        100.0
    )

    return oi, growth


# ============================================================
# SIGNAL MESSAGE
# ============================================================

def build_signal_message(
    symbol,
    ticker,
    candidate,
    rvol,
    oi,
    oi_growth
):
    shelf = candidate["shelf"]
    ema = candidate["ema"]
    interaction = candidate["interaction"]
    impulse = candidate["impulse"]

    price = ticker.get(
        "price",
        0.0
    )

    top = shelf["top"]
    bottom = shelf["bottom"]

    current_change = (
        (price - top) /
        top *
        100.0
        if top > 0
        else 0.0
    )

    lines = []

    lines.append(
        "🏹 ПАРТИЗАН v6 — ПЕРВЫЙ ИМПУЛЬС"
    )

    lines.append("")
    lines.append(
        f"🪙 {symbol}"
    )

    lines.append(
        f"💰 Цена: {price:.8g}"
    )

    lines.append("")

    lines.append(
        "📦 ПОЛКА"
    )

    lines.append(
        f"   Нижняя: {bottom:.8g}"
    )

    lines.append(
        f"   Верхняя: {top:.8g}"
    )

    lines.append(
        f"   Ширина: {shelf['width_pct']:.2f}%"
    )

    lines.append(
        f"   Свечей: {shelf['length']}"
    )

    lines.append("")

    lines.append(
        "📈 EMA"
    )

    lines.append(
        f"   EMA20 внутри: "
        f"{ema['inside20_pct']:.0f}%"
    )

    lines.append(
        f"   EMA40 внутри: "
        f"{ema['inside40_pct']:.0f}%"
    )

    lines.append(
        f"   EMA80 дистанция: "
        f"{ema['ema80_distance_pct']:.2f}%"
    )

    lines.append("")

    lines.append(
        "🔄 ВЗАИМОДЕЙСТВИЕ"
    )

    lines.append(
        f"   Касаний EMA-зоны: "
        f"{interaction['interactions']}"
    )

    lines.append(
        f"   Бычьих реакций: "
        f"{interaction['bullish_reactions']}"
    )

    lines.append("")

    lines.append(
        "🚀 ИМПУЛЬС"
    )

    lines.append(
        f"   Свечей: "
        f"{impulse['candles']}"
    )

    lines.append(
        f"   Цель: +{BREAKOUT_TARGET_PCT:.1f}%"
    )

    lines.append(
        f"   От верха полки: "
        f"{current_change:+.2f}%"
    )

    lines.append("")

    lines.append(
        "📊 ИНФОРМАЦИЯ"
    )

    lines.append(
        f"   RVOL: {rvol:.2f}x"
    )

    if oi is not None:
        lines.append(
            f"   OI: {oi:.2f}"
        )

    if oi_growth is not None:
        lines.append(
            f"   OI изменение: "
            f"{oi_growth:+.2f}%"
        )

    lines.append(
        f"   24h объём: "
        f"${ticker.get('volume24h', 0):,.0f}"
    )

    lines.append("")

    lines.append(
        "🎯 Условие: первый импульс "
        f"достиг +{BREAKOUT_TARGET_PCT:.0f}% "
        "от верха полки."
    )

    lines.append(
        "⚠️ RVOL/OI не являются фильтрами."
    )

    return "\n".join(lines)


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(
    session,
    message
):
    if not BOT_TOKEN or not CHAT_ID:
        logger.error(
            "BOT_TOKEN или CHAT_ID не заданы."
        )
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
    }

    try:
        async with session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(
                total=15
            )
        ) as response:

            if response.status != 200:
                text = await response.text()

                logger.error(
                    "Telegram HTTP %s: %s",
                    response.status,
                    text
                )

                return False

            return True

    except Exception as e:

        logger.error(
            "Ошибка Telegram: %s",
            e
        )

        return False


# ============================================================
# SYMBOL EVALUATION
# ============================================================

async def evaluate_symbol(
    session,
    symbol,
    ticker
):
    try:

        candles = await fetch_klines(
            session,
            symbol
        )

        if len(candles) < EMA_SLOW + 20:
            return None

        # ====================================================
        # КРИТИЧЕСКИ ВАЖНО:
        #
        # Последняя свеча BingX может быть ТЕКУЩЕЙ,
        # ещё не закрытой.
        #
        # Структуру строим только по ЗАКРЫТЫМ свечам.
        # ====================================================

        closed = candles[:-1]

        if len(closed) < EMA_SLOW + 20:
            return None

        stats["candidates"] += 1

        candidate = find_best_shelf(
            closed
        )

        if not candidate:
            return None

        # ----------------------------------------------------
        # Дополнительная защита от повторного сигнала.
        # ----------------------------------------------------

        last_signal = last_alert_time.get(
            symbol,
            0
        )

        if (
            time.time() - last_signal
            < ALERT_COOLDOWN_SECONDS
        ):
            return None

        # ----------------------------------------------------
        # RVOL — только информация.
        # ----------------------------------------------------

        impulse_end = (
            candidate["shelf"]["end"]
            +
            candidate["impulse"]["candles"]
            - 1
        )

        rvol = calculate_rvol(
            closed,
            min(
                impulse_end,
                len(closed) - 1
            )
        )

        # ----------------------------------------------------
        # OI — только информация.
        # ----------------------------------------------------

        oi, oi_growth = await get_oi_info(
            session,
            symbol
        )

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

        logger.exception(
            "Ошибка %s: %s",
            symbol,
            e
        )

        return None


# ============================================================
# STATS LOG
# ============================================================

def log_stats(
    symbols_count,
    signal_count
):
    logger.info(
        "📊 СКАН #%d | пар=%d | кандидатов=%d | "
        "полок=%d | EMA20=%d | EMA40=%d | "
        "EMA80=%d | interaction=%d | "
        "old=%d | breakout=%d | retest=%d | "
        "target=%d | signals=%d | errors=%d",
        stats["scans"],
        symbols_count,
        stats["candidates"],
        stats["shelves"],
        stats["reject_ema20"],
        stats["reject_ema40"],
        stats["reject_ema80"],
        stats["reject_interaction"],
        stats["reject_old_breakout"],
        stats["reject_breakout"],
        stats["reject_retest"],
        stats["targets"],
        signal_count,
        stats["errors"],
    )


# ============================================================
# MARKET SCAN
# ============================================================

async def scan_market(
    session
):
    contracts = await fetch_contracts(
        session
    )

    tickers = await fetch_tickers(
        session
    )

    if not contracts or not tickers:
        logger.warning(
            "Не удалось получить contracts/tickers."
        )
        return

    symbols = [
        symbol
        for symbol in contracts
        if symbol in tickers
        and symbol not in BLACKLIST
    ]

    stats["symbols"] = len(symbols)

    logger.info(
        "🔎 Начинаю сканирование: %d пар",
        len(symbols)
    )

    tasks = []

    for symbol in symbols:

        last_signal = last_alert_time.get(
            symbol,
            0
        )

        if (
            time.time() - last_signal
            < ALERT_COOLDOWN_SECONDS
        ):
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

    signal_count = 0

    for result in results:

        if isinstance(
            result,
            Exception
        ):
            stats["errors"] += 1
            continue

        if not result:
            continue

        symbol = result["symbol"]

        message = build_signal_message(
            symbol,
            result["ticker"],
            result["candidate"],
            result["rvol"],
            result["oi"],
            result["oi_growth"],
        )

        success = await send_telegram(
            session,
            message
        )

        if success:

            last_alert_time[
                symbol
            ] = time.time()

            stats["signals"] += 1
            signal_count += 1

            logger.info(
                "🏹 СИГНАЛ: %s",
                symbol
            )

    log_stats(
        len(symbols),
        signal_count
    )


# ============================================================
# HEALTH SERVER
# ============================================================

async def health(request):
    return web.Response(
        text="ПАРТИЗАН v6 OK"
    )


async def start_web_server():
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
        "🌐 Health server запущен на порту %d",
        PORT
    )


# ============================================================
# SELF PING
# ============================================================

async def self_ping():
    if not SELF_URL:
        return

    while True:

        await asyncio.sleep(
            10 * 60
        )

        try:

            timeout = aiohttp.ClientTimeout(
                total=10
            )

            async with aiohttp.ClientSession(
                timeout=timeout
            ) as session:

                async with session.get(
                    SELF_URL
                ) as response:

                    logger.info(
                        "💓 Self-ping: HTTP %s",
                        response.status
                    )

        except Exception as e:

            logger.warning(
                "Self-ping ошибка: %s",
                e
            )


# ============================================================
# MAIN
# ============================================================

async def main():

    logger.info(
        "🏹 ПАРТИЗАН v6 запускается..."
    )

    logger.info(
        "🎯 Полка: %d–%d свечей | ширина <= %.1f%%",
        SHELF_MIN_CANDLES,
        SHELF_MAX_CANDLES,
        MAX_SHELF_WIDTH_PCT
    )

    logger.info(
        "📈 EMA20/40: внутри >= 60%%"
    )

    logger.info(
        "📐 EMA80: допускается внутри/ниже/выше, "
        "дистанция <= %.1f%%",
        EMA80_MAX_DISTANCE_PCT
    )

    logger.info(
        "🚀 Импульс: до %d свечей | цель +%.1f%%",
        BREAKOUT_MAX_CANDLES,
        BREAKOUT_TARGET_PCT
    )

    logger.info(
        "📊 RVOL/OI: только информационные"
    )

    if not BOT_TOKEN:
        logger.error(
            "❌ BOT_TOKEN не задан!"
        )

    if not CHAT_ID:
        logger.error(
            "❌ CHAT_ID не задан!"
        )

    await start_web_server()

    asyncio.create_task(
        self_ping()
    )

    timeout = aiohttp.ClientTimeout(
        total=SESSION_TIMEOUT
    )

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        while True:

            stats["scans"] += 1

            scan_started = time.time()

            try:

                await scan_market(
                    session
                )

            except Exception as e:

                stats["errors"] += 1

                logger.exception(
                    "Ошибка основного цикла: %s",
                    e
                )

            elapsed = (
                time.time()
                - scan_started
            )

            sleep_time = max(
                1,
                CHECK_INTERVAL_SECONDS
                - elapsed
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "ПАРТИЗАН остановлен."
        )

    except Exception as e:

        logger.exception(
            "Критическая ошибка: %s",
            e
        )
