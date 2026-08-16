import os
import time
import json
import asyncio
import logging
from collections import defaultdict

import aiohttp
from flask import Flask
import threading


# ============================================================
#                     CONFIGURATION
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

# ============================================================
#                     ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

# Полный поиск новых полок
FULL_SCAN_INTERVAL = 2 * 60 * 60       # 2 часа

# Проверка уже найденных полок
WATCH_INTERVAL = 30                    # 30 секунд

# Минимальный объём фьючерса
MIN_24H_VOLUME_USDT = 800_000

# Не берём монеты, которые уже слишком сильно разогнались
MAX_24H_CHANGE_FOR_NEW_SHELF = 50.0

# ============================================================
#                     ПАРАМЕТРЫ ПОЛКИ
# ============================================================

SHELF_CANDLE_COUNT = 12

# Максимальная ширина базы
MAX_SHELF_WIDTH_PCT = 5.0

# Текущая цена должна находиться недалеко от базы
MAX_PRICE_FROM_SHELF_PCT = 5.0

# Минимальное количество свечей для анализа
MIN_KLINES = 20

# Сколько хранить полку
# Старые полки НЕ удаляются при новом полном скане.
# Удаление только по TTL.
SHELF_TTL_SECONDS = 12 * 60 * 60       # 12 часов

# После сильного пампа продолжаем наблюдать некоторое время
TRIGGERED_TTL_SECONDS = 4 * 60 * 60    # 4 часа


# ============================================================
#                     УРОВНИ ИМПУЛЬСА
# ============================================================

# Относительно верхней границы полки
EARLY_BREAKOUT_PCT = 0.5
CONFIRM_BREAKOUT_PCT = 1.5
PUMP_BREAKOUT_PCT = 2.5

# Для движения вниз
EARLY_BREAKDOWN_PCT = 0.5
CONFIRM_BREAKDOWN_PCT = 1.5
PUMP_BREAKDOWN_PCT = 2.5


# ============================================================
#                       OI
# ============================================================

# OI проверяем ТОЛЬКО при движении.
# Не используем OI для первоначального поиска полки.
MIN_OPEN_INTEREST_USDT = 1_000_000


# ============================================================
#                       RVOL
# ============================================================

RVOL_ENABLED = True

MIN_SHORT_RVOL = 0.74
SHORT_RVOL_INTERVAL = "5m"
SHORT_RVOL_LOOKBACK = 12
SHORT_RVOL_RECENT_COUNT = 3


# ============================================================
#                   ПЕРСИСТЕНТНОСТЬ
# ============================================================

SHELVES_FILE = "shelves.json"


# ============================================================
#              ФИЛЬТР НЕ-КРИПТОВЫХ ИНСТРУМЕНТОВ
# ============================================================

NON_CRYPTO_PREFIXES = (
    "NCS", "NCCO",
    "SP500", "US30", "US100", "NAS100",
    "GER40", "UK100", "JPN225",
    "GOLD", "SILVER",
    "BRENT", "WTI", "OIL",
    "XAU", "XAG", "XTI", "XBR",
    "EUR", "GBP", "JPY", "AUD", "CAD",
    "CHF", "HK", "DXY"
)

EXCLUDED_SYMBOLS = {
    "USDC-USDT",
    "FDUSD-USDT",
    "USD1-USDT"
}


# ============================================================
#                         MEMORY
# ============================================================

# Формат:
#
# SHELVES = {
#     "FUN-USDT": {
#         "created_at": ...,
#         "updated_at": ...,
#         "shelf_low": ...,
#         "shelf_high": ...,
#         "width_pct": ...,
#         "last_price": ...,
#         "status": "ACTIVE",
#         "early_sent": False,
#         "confirm_sent": False,
#         "pump_sent": False
#     }
# }

SHELVES = {}

ACTIVE_SYMBOLS = set()
VALID_FUTURES_SYMBOLS = set()


# ============================================================
#                         LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ============================================================
#                           FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return (
        f"ConsolPulse TEST | "
        f"Shelves={len(SHELVES)} | "
        f"Market={len(ACTIVE_SYMBOLS)}"
    ), 200


def run_flask():
    app.run(
        host="0.0.0.0",
        port=PORT
    )


# ============================================================
#                         HELPERS
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
#                     KLINE PARSER
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

            timestamp = int(k[0])
            o = safe_float(k[1])
            h = safe_float(k[2])
            l = safe_float(k[3])
            c = safe_float(k[4])
            v = safe_float(k[5])

            return timestamp, o, h, l, c, v

    except Exception:
        pass

    return 0, 0, 0, 0, 0, 0


# ============================================================
#                    FILE PERSISTENCE
# ============================================================

def save_shelves():

    try:

        temp_file = SHELVES_FILE + ".tmp"

        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(
                SHELVES,
                f,
                ensure_ascii=False,
                indent=2
            )

        os.replace(temp_file, SHELVES_FILE)

        logging.info(
            "💾 Полки сохранены: %d",
            len(SHELVES)
        )

    except Exception as e:

        logging.error(
            "❌ Ошибка сохранения полок: %s",
            e
        )


def load_shelves():

    global SHELVES

    if not os.path.exists(SHELVES_FILE):

        logging.info(
            "📂 shelves.json пока отсутствует — начинаем с чистого листа"
        )

        SHELVES = {}
        return

    try:

        with open(
            SHELVES_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if isinstance(data, dict):

            SHELVES = data

        else:

            SHELVES = {}

        logging.info(
            "💾 Загружено сохранённых полок: %d",
            len(SHELVES)
        )

    except Exception as e:

        logging.error(
            "❌ Ошибка загрузки shelves.json: %s",
            e
        )

        SHELVES = {}


# ============================================================
#                  TELEGRAM
# ============================================================

async def send_telegram_alert(session, text):

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
#                   STARTUP MESSAGE
# ============================================================

async def send_startup_message(session):

    message = (
        "🟢 <b>CONSOL_PULSE TEST запущен</b>\n\n"

        "🧲 Поиск новых полок: <b>каждые 2 часа</b>\n"
        "👁 Мониторинг полок: <b>каждые 30 сек</b>\n\n"

        f"📦 Макс. ширина полки: "
        f"<b>{MAX_SHELF_WIDTH_PCT:.1f}%</b>\n"

        f"🚀 Ранний выход: "
        f"<b>+{EARLY_BREAKOUT_PCT:.1f}%</b>\n"

        f"⚡ Подтверждение: "
        f"<b>+{CONFIRM_BREAKOUT_PCT:.1f}%</b>\n"

        f"🔥 Памп: "
        f"<b>+{PUMP_BREAKOUT_PCT:.1f}%</b>\n\n"

        f"👁 OI проверяется только при движении: "
        f"<b>${MIN_OPEN_INTEREST_USDT:,.0f}+</b>\n\n"

        "💾 Старые полки сохраняются."
    )

    await send_telegram_alert(
        session,
        message
    )


# ============================================================
#                BINGX CONTRACTS
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
            timeout=10
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

                if (
                    symbol
                    and is_crypto_usdt_symbol(symbol)
                ):
                    symbols.add(symbol)

            if symbols:

                VALID_FUTURES_SYMBOLS = symbols

                logging.info(
                    "✅ Фьючерсов BingX: %d",
                    len(symbols)
                )

    except Exception as e:

        logging.warning(
            "⚠️ Ошибка contracts: %s",
            e
        )


# ============================================================
#                    MARKET TICKERS
# ============================================================

async def get_market_tickers(session):

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v2/quote/ticker"
    )

    try:

        async with session.get(
            url,
            timeout=10
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
            "❌ Ticker error: %s",
            e
        )

        return []


# ============================================================
#                       KLINES
# ============================================================

async def get_klines(
    session,
    symbol,
    interval="1h",
    limit=45
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

    try:

        async with session.get(
            url,
            params=params,
            timeout=8
        ) as resp:

            if resp.status != 200:
                return []

            data = await resp.json()

            if data.get("code") != 0:
                return []

            candles = data.get("data", [])

            if not isinstance(candles, list):
                return []

            candles = sorted(
                candles,
                key=lambda x: parse_kline(x)[0]
            )

            return candles

    except Exception:
        return []


# ============================================================
#                FIND SHELF
# ============================================================

def detect_shelf(candles, current_price):

    if len(candles) < MIN_KLINES:
        return None

    parsed = []

    for k in candles:

        _, o, h, l, c, v = parse_kline(k)

        if (
            h <= 0
            or l <= 0
            or c <= 0
        ):
            continue

        parsed.append({
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v
        })

    if len(parsed) < MIN_KLINES:
        return None

    # Последняя свеча может быть текущей.
    # Для поиска базы используем предыдущие закрытые свечи.
    closed = parsed[:-1]

    if len(closed) < SHELF_CANDLE_COUNT:
        return None

    # Берём последние 12 свечей перед текущим моментом.
    base = closed[-SHELF_CANDLE_COUNT:]

    highs = [
        c["high"]
        for c in base
    ]

    lows = [
        c["low"]
        for c in base
    ]

    closes = [
        c["close"]
        for c in base
    ]

    shelf_high = max(highs)
    shelf_low = min(lows)

    if shelf_low <= 0:
        return None

    shelf_width = (
        (shelf_high - shelf_low)
        / shelf_low
        * 100
    )

    if shelf_width > MAX_SHELF_WIDTH_PCT:
        return None

    # Цена должна быть недалеко от найденной базы.
    distance_from_high = (
        abs(current_price - shelf_high)
        / shelf_high
        * 100
    )

    distance_from_low = (
        abs(current_price - shelf_low)
        / shelf_low
        * 100
    )

    if (
        distance_from_high > MAX_PRICE_FROM_SHELF_PCT
        and distance_from_low > MAX_PRICE_FROM_SHELF_PCT
    ):
        return None

    # Проверяем, что база действительно компактная.
    midpoint = (
        shelf_high + shelf_low
    ) / 2

    if midpoint <= 0:
        return None

    # Насколько текущая цена уже выше/ниже базы.
    breakout_from_high = (
        (current_price - shelf_high)
        / shelf_high
        * 100
    )

    breakdown_from_low = (
        (shelf_low - current_price)
        / shelf_low
        * 100
    )

    return {
        "shelf_low": shelf_low,
        "shelf_high": shelf_high,
        "width_pct": shelf_width,
        "breakout_pct": breakout_from_high,
        "breakdown_pct": breakdown_from_low,
        "last_price": current_price
    }


# ============================================================
#            FULL MARKET SHELF SCAN
# ============================================================

SCAN_CONCURRENCY = 15


async def scan_one_for_shelf(
    session,
    ticker,
    semaphore
):

    symbol, price, volume, change_24h = ticker

    # Не добавляем уже сильно разогнанные монеты
    # в новые полки.
    if abs(change_24h) > MAX_24H_CHANGE_FOR_NEW_SHELF:
        return None

    async with semaphore:

        candles = await get_klines(
            session,
            symbol,
            "1h",
            45
        )

    shelf = detect_shelf(
        candles,
        price
    )

    if shelf is None:
        return None

    shelf["symbol"] = symbol
    shelf["volume_24h"] = volume
    shelf["change_24h"] = change_24h

    return shelf


async def full_shelf_scan(
    session,
    tickers
):

    logging.info(
        "🔎 НАЧАЛО ПОЛНОГО СКАНА ПОЛОК | рынок=%d",
        len(tickers)
    )

    semaphore = asyncio.Semaphore(
        SCAN_CONCURRENCY
    )

    tasks = [
        scan_one_for_shelf(
            session,
            ticker,
            semaphore
        )
        for ticker in tickers
    ]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )

    found = 0
    now = time.time()

    for result in results:

        if not isinstance(result, dict):
            continue

        symbol = result["symbol"]

        # ====================================================
        # ВАЖНО:
        # НЕ заменяем SHELVES.
        # Добавляем/обновляем конкретную монету.
        # Старые монеты остаются.
        # ====================================================

        if symbol not in SHELVES:

            SHELVES[symbol] = {
                "created_at": now,
                "updated_at": now,
                "shelf_low": result["shelf_low"],
                "shelf_high": result["shelf_high"],
                "width_pct": result["width_pct"],
                "last_price": result["last_price"],
                "status": "ACTIVE",
                "early_sent": False,
                "confirm_sent": False,
                "pump_sent": False
            }

            found += 1

            logging.info(
                "🧲 НОВАЯ ПОЛКА | %s | %.8f - %.8f | ширина %.2f%%",
                symbol,
                result["shelf_low"],
                result["shelf_high"],
                result["width_pct"]
            )

        else:

            # Если полка уже существует,
            # НЕ сбрасываем историю сигналов.
            old = SHELVES[symbol]

            old["updated_at"] = now
            old["last_price"] = result["last_price"]

    save_shelves()

    logging.info(
        "✅ ПОЛНЫЙ СКАН ЗАКОНЧЕН | "
        "новых=%d | всего полок=%d",
        found,
        len(SHELVES)
    )


# ============================================================
#                    OPEN INTEREST
# ============================================================

async def fetch_open_interest(
    session,
    symbol,
    current_price
):

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v2/quote/openInterest"
    )

    variants = [
        symbol,
        symbol.replace("-", "")
    ]

    for sym in variants:

        try:

            async with session.get(
                url,
                params={"symbol": sym},
                timeout=5
            ) as resp:

                if resp.status != 200:
                    continue

                data = await resp.json()

                if data.get("code") != 0:
                    continue

                oi_data = data.get("data")

                if (
                    isinstance(oi_data, list)
                    and oi_data
                ):
                    oi_data = oi_data[0]

                if not isinstance(
                    oi_data,
                    dict
                ):
                    continue

                value = (
                    oi_data.get("openInterestValue")
                    or oi_data.get("openInterest")
                    or oi_data.get("value")
                )

                if value is None:
                    continue

                oi = safe_float(value)

                if oi <= 0:
                    continue

                # Если BingX вернул количество контрактов
                if (
                    oi < 500_000
                    and current_price > 0
                ):
                    oi *= current_price

                return oi

        except Exception:
            continue

    return None


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
        SHORT_RVOL_LOOKBACK + 6
    )

    if len(candles) < 5:
        return 1.0

    volumes = []

    for k in candles:

        _, _, _, _, close, volume = parse_kline(k)

        if close <= 0 or volume <= 0:
            continue

        volumes.append(
            close * volume
        )

    if len(volumes) < 5:
        return 1.0

    recent_count = min(
        SHORT_RVOL_RECENT_COUNT,
        len(volumes)
    )

    recent = volumes[
        -recent_count:
    ]

    historical_end = (
        len(volumes)
        - recent_count
    )

    historical_start = max(
        0,
        historical_end
        - SHORT_RVOL_LOOKBACK
    )

    historical = volumes[
        historical_start:
        historical_end
    ]

    if not historical:
        return 1.0

    avg = sum(historical) / len(historical)

    if avg <= 0:
        return 1.0

    recent_avg = (
        sum(recent)
        / len(recent)
    )

    return recent_avg / avg


# ============================================================
#                 SIGNAL LEVEL
# ============================================================

def get_breakout_level(
    shelf,
    current_price
):

    high = shelf["shelf_high"]
    low = shelf["shelf_low"]

    if high <= 0 or low <= 0:
        return None

    up = (
        (current_price - high)
        / high
        * 100
    )

    down = (
        (low - current_price)
        / low
        * 100
    )

    # UP
    if up >= PUMP_BREAKOUT_PCT:
        return "PUMP_UP", up

    if up >= CONFIRM_BREAKOUT_PCT:
        return "CONFIRM_UP", up

    if up >= EARLY_BREAKOUT_PCT:
        return "EARLY_UP", up

    # DOWN
    if down >= PUMP_BREAKDOWN_PCT:
        return "PUMP_DOWN", down

    if down >= CONFIRM_BREAKDOWN_PCT:
        return "CONFIRM_DOWN", down

    if down >= EARLY_BREAKDOWN_PCT:
        return "EARLY_DOWN", down

    return None


# ============================================================
#                 SEND SIGNAL
# ============================================================

async def send_breakout_signal(
    session,
    symbol,
    shelf,
    price,
    volume_24h,
    change_24h,
    level,
    move_pct
):

    # OI и RVOL запрашиваем только здесь.
    oi = await fetch_open_interest(
        session,
        symbol,
        price
    )

    rvol = await fetch_short_rvol(
        session,
        symbol
    )

    # На раннем сигнале OI не блокирует сигнал.
    # Это важно для молодых альтов.
    if (
        oi is not None
        and oi < MIN_OPEN_INTEREST_USDT
    ):
        oi_text = (
            f"${oi:,.0f} "
            f"⚠️ ниже $1M"
        )
    elif oi is not None:
        oi_text = (
            f"${oi:,.0f}"
        )
    else:
        oi_text = "не получен"

    if level.startswith("EARLY"):

        emoji = "👀"
        title = "РАННИЙ ВЫХОД ИЗ ПОЛКИ"

    elif level.startswith("CONFIRM"):

        emoji = "🚀"
        title = "ПРОБОЙ ПОЛКИ"

    elif level.startswith("PUMP"):

        emoji = "🔥"
        title = "СИЛЬНЫЙ ИМПУЛЬС"

    else:

        emoji = "📡"
        title = "ИМПУЛЬС"

    direction = (
        "🟢 UP"
        if "UP" in level
        else "🔴 DOWN"
    )

    clean = symbol.replace(
        "-USDT",
        ""
    )

    message = (
        f"<code>{clean}</code>\n\n"

        f"{emoji} <b>{clean}USDT — "
        f"{title}</b>\n\n"

        f"{direction}\n"

        f"📈 Движение от полки: "
        f"<b>{move_pct:+.2f}%</b>\n"

        f"🧲 Полка: "
        f"<b>{format_price(shelf['shelf_low'])}"
        f" — "
        f"{format_price(shelf['shelf_high'])}</b>\n"

        f"📏 Ширина полки: "
        f"<b>{shelf['width_pct']:.2f}%</b>\n\n"

        f"💰 Цена: "
        f"<b>{format_price(price)}</b>\n"

        f"📊 24h: "
        f"<b>{change_24h:+.2f}%</b>\n"

        f"🔥 RVOL 5m: "
        f"<b>{rvol:.2f}x</b>\n"

        f"👁 OI: "
        f"<b>{oi_text}</b>\n"

        f"💵 Объём 24h: "
        f"<b>${volume_24h:,.0f}</b>\n\n"

        f"⚡ Уровень: "
        f"<b>{level}</b>"
    )

    await send_telegram_alert(
        session,
        message
    )


# ============================================================
#              WATCH EXISTING SHELVES
# ============================================================

async def monitor_shelves(
    session,
    tickers
):

    if not SHELVES:
        logging.info(
            "👁 WATCH | полок пока нет"
        )
        return

    ticker_map = {
        symbol: (
            price,
            volume,
            change
        )
        for symbol, price, volume, change
        in tickers
    }

    now = time.time()

    signals = 0
    removed = 0

    for symbol in list(SHELVES.keys()):

        shelf = SHELVES[symbol]

        created_at = safe_float(
            shelf.get("created_at")
        )

        status = shelf.get(
            "status",
            "ACTIVE"
        )

        # ====================================================
        # TTL
        # ====================================================

        if status == "ACTIVE":

            if (
                created_at > 0
                and now - created_at
                > SHELF_TTL_SECONDS
            ):

                del SHELVES[symbol]
                removed += 1

                logging.info(
                    "🗑 Старая полка удалена | %s",
                    symbol
                )

                continue

        else:

            if (
                created_at > 0
                and now - created_at
                > TRIGGERED_TTL_SECONDS
            ):

                del SHELVES[symbol]
                removed += 1

                logging.info(
                    "🗑 Завершённая полка удалена | %s",
                    symbol
                )

                continue

        ticker = ticker_map.get(symbol)

        if ticker is None:
            continue

        price, volume_24h, change_24h = ticker

        shelf["last_price"] = price
        shelf["updated_at"] = now

        level_data = get_breakout_level(
            shelf,
            price
        )

        if level_data is None:
            continue

        level, move_pct = level_data

        # ====================================================
        # Не отправляем один и тот же уровень повторно.
        # ====================================================

        if level == "EARLY_UP":

            if shelf.get("early_sent"):
                continue

            shelf["early_sent"] = True

        elif level == "CONFIRM_UP":

            if shelf.get("confirm_sent"):
                continue

            shelf["confirm_sent"] = True

        elif level == "PUMP_UP":

            if shelf.get("pump_sent"):
                continue

            shelf["pump_sent"] = True

        elif level == "EARLY_DOWN":

            if shelf.get("early_sent"):
                continue

            shelf["early_sent"] = True

        elif level == "CONFIRM_DOWN":

            if shelf.get("confirm_sent"):
                continue

            shelf["confirm_sent"] = True

        elif level == "PUMP_DOWN":

            if shelf.get("pump_sent"):
                continue

            shelf["pump_sent"] = True

        # ====================================================
        # Если произошёл выход — переводим полку в TRIGGERED
        # только после сильного импульса.
        # ====================================================

        if level.startswith("PUMP"):

            shelf["status"] = "TRIGGERED"

        await send_breakout_signal(
            session=session,
            symbol=symbol,
            shelf=shelf,
            price=price,
            volume_24h=volume_24h,
            change_24h=change_24h,
            level=level,
            move_pct=move_pct
        )

        signals += 1

        logging.info(
            "🚀 SIGNAL | %s | %s | %.2f%%",
            symbol,
            level,
            move_pct
        )

    if removed or signals:
        save_shelves()

    logging.info(
        "👁 WATCH | полок=%d | сигналов=%d | удалено=%d",
        len(SHELVES),
        signals,
        removed
    )


# ============================================================
#                     MAIN LOOP
# ============================================================

async def main_loop():

    global ACTIVE_SYMBOLS

    timeout = aiohttp.ClientTimeout(
        total=20
    )

    connector = aiohttp.TCPConnector(
        limit=50
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        logging.info(
            "🚀 CONSOL_PULSE TEST STARTED"
        )

        # ----------------------------------------------------
        # Загружаем старые полки
        # ----------------------------------------------------

        load_shelves()

        # ----------------------------------------------------
        # Загружаем контракты
        # ----------------------------------------------------

        await update_futures_symbols(
            session
        )

        await send_startup_message(
            session
        )

        # ----------------------------------------------------
        # Первый полный скан сразу
        # ----------------------------------------------------

        tickers = await get_market_tickers(
            session
        )

        if tickers:

            ACTIVE_SYMBOLS = {
                item[0]
                for item in tickers
            }

            await full_shelf_scan(
                session,
                tickers
            )

        # Следующий полный скан через 2 часа.
        next_full_scan = (
            time.time()
            + FULL_SCAN_INTERVAL
        )

        loop_count = 0

        # ====================================================
        #                    MAIN LOOP
        # ====================================================

        while True:

            started = time.time()
            loop_count += 1

            # ------------------------------------------------
            # Получаем тикеры.
            # Один запрос вместо 500 запросов.
            # ------------------------------------------------

            tickers = await get_market_tickers(
                session
            )

            if tickers:

                ACTIVE_SYMBOLS = {
                    item[0]
                    for item in tickers
                }

                # --------------------------------------------
                # Мониторим только сохранённые полки
                # --------------------------------------------

                await monitor_shelves(
                    session,
                    tickers
                )

                # --------------------------------------------
                # Полный поиск новых полок каждые 2 часа
                # --------------------------------------------

                if time.time() >= next_full_scan:

                    await full_shelf_scan(
                        session,
                        tickers
                    )

                    next_full_scan = (
                        time.time()
                        + FULL_SCAN_INTERVAL
                    )

            else:

                logging.warning(
                    "⚠️ Ticker BingX не получен"
                )

            elapsed = (
                time.time()
                - started
            )

            sleep_time = max(
                1,
                WATCH_INTERVAL - elapsed
            )

            logging.info(
                "📊 WATCH SCAN | "
                "рынок=%d | полок=%d | "
                "время=%.1fs | "
                "следующий полный скан через %.1f мин",
                len(tickers),
                len(SHELVES),
                elapsed,
                max(
                    0,
                    (next_full_scan - time.time())
                    / 60
                )
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
#                         START
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
