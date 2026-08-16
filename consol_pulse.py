import os
import time
import json
import asyncio
import logging
from pathlib import Path

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
#                     SCAN SETTINGS
# ============================================================

FULL_SCAN_INTERVAL = 20 * 60      # полный поиск новых полок каждые 20 минут
WATCH_INTERVAL = 30               # мониторинг найденных полок каждые 30 секунд
SHELF_TTL = 24 * 60 * 60          # обычная полка живёт 24 часа
TRIGGERED_TTL = 4 * 60 * 60       # после сигнала держим ещё 4 часа


# ============================================================
#                    IMPULSE LEVELS
# ============================================================

EARLY_TRIGGER_PCT = 0.5
CONFIRM_TRIGGER_PCT = 1.5
PUMP_TRIGGER_PCT = 2.5


# ============================================================
#                    MARKET FILTERS
# ============================================================

MIN_24H_VOLUME_USDT = 800_000
MIN_OPEN_INTEREST_USDT = 1_000_000


# ============================================================
#                    SHELF SETTINGS
# ============================================================

MIN_SHELF_CANDLES = 6             # Минимум 6 часов консолидации
MAX_SHELF_CANDLES = 36            # Расширено до 36 часов

MAX_SHELF_WIDTH_PCT = 10.0        # тело полки не шире 10%
MAX_SHELF_WICK_WIDTH_PCT = 14.0   # с тенями не шире 14%

EMA_FAST = 20
EMA_SLOW = 40
EMA_MAX_SPREAD_PCT = 10.0         # Допустимый спред EMA20/40


# ============================================================
#                     RVOL SETTINGS
# ============================================================

SHORT_RVOL_INTERVAL = "5m"
SHORT_RVOL_LOOKBACK = 12
SHORT_RVOL_RECENT_COUNT = 3

MIN_SHORT_RVOL = 0.50
MIN_HOURLY_RVOL = 0.80


# ============================================================
#                  NON-CRYPTO FILTER
# ============================================================

NON_CRYPTO_PREFIXES = (
    "NCS", "NCCO", "SP500", "US30", "US100", "NAS100",
    "GER40", "UK100", "JPN225", "GOLD", "SILVER",
    "BRENT", "WTI", "OIL", "XAU", "XAG", "XTI", "XBR",
    "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "HK", "DXY"
)

EXCLUDED_SYMBOLS = {
    "USDCUSDT",
    "FDUSDUSDT",
    "USD1USDT"
}

SHELVES_FILE = Path("shelves.json")

SHELVES = {}
VALID_FUTURES_SYMBOLS = set()
ACTIVE_SYMBOLS = set()


# ============================================================
#                       LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ============================================================
#                         FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return (
        f"ConsolPulse WATCH (Binance) | "
        f"Shelves={len(SHELVES)} | "
        f"FullScan=20m | "
        f"Watch=30s"
    ), 200


def run_flask():
    app.run(host="0.0.0.0", port=PORT)


# ============================================================
#                       HELPERS
# ============================================================

def normalize_symbol(raw):
    symbol = str(raw or "").strip().upper()

    if not symbol:
        return None

    if symbol.endswith("-USDT"):
        return symbol.replace("-", "")

    if symbol.endswith("USDT"):
        return symbol

    return None


def is_crypto_usdt_symbol(symbol):
    if not symbol or not symbol.endswith("USDT"):
        return False

    if symbol in EXCLUDED_SYMBOLS:
        return False

    base = symbol[:-4]

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


def parse_kline(k):
    try:
        if isinstance(k, dict):
            timestamp = (
                k.get("openTime")
                or k.get("time")
                or k.get("timestamp")
                or 0
            )
            return (
                int(timestamp),
                safe_float(k.get("open")),
                safe_float(k.get("high")),
                safe_float(k.get("low")),
                safe_float(k.get("close")),
                safe_float(k.get("volume"))
            )

        if isinstance(k, (list, tuple)):
            return (
                int(k[0]),
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5])
            )

    except Exception:
        pass

    return 0, 0.0, 0.0, 0.0, 0.0, 0.0


# ============================================================
#                 EMA WITHOUT PANDAS
# ============================================================

def calculate_ema_series(prices, period):
    if not prices:
        return []

    if len(prices) < period:
        return [0.0] * len(prices)

    multiplier = 2.0 / (period + 1.0)
    ema_values = [0.0] * len(prices)

    sma = sum(prices[:period]) / period
    ema_values[period - 1] = sma

    ema = sma

    for i in range(period, len(prices)):
        ema = (prices[i] - ema) * multiplier + ema
        ema_values[i] = ema

    return ema_values


# ============================================================
#                  SHELVES STORAGE
# ============================================================

def save_shelves():
    try:
        temp_file = Path("shelves.json.tmp")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(SHELVES, f, ensure_ascii=False, indent=2)
        temp_file.replace(SHELVES_FILE)
        logging.info("💾 Полки сохранены: %d", len(SHELVES))
    except Exception as e:
        logging.error("❌ Ошибка сохранения shelves.json: %s", e)


def load_shelves():
    global SHELVES
    if not SHELVES_FILE.exists():
        SHELVES = {}
        logging.info("💾 Сохранённых полок нет")
        return

    try:
        with open(SHELVES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            SHELVES = data
            logging.info("💾 Загружено сохранённых полок: %d", len(SHELVES))
        else:
            SHELVES = {}
    except Exception as e:
        logging.error("❌ Ошибка загрузки shelves.json: %s", e)
        SHELVES = {}


# ============================================================
#                    BINANCE FUTURES API
# ============================================================

BASE_URL = "https://fapi.binance.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}


async def update_futures_symbols(session):
    global VALID_FUTURES_SYMBOLS
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"

    try:
        async with session.get(url, headers=HEADERS, timeout=10) as resp:
            if resp.status != 200:
                return

            data = await resp.json()
            symbols = set()

            for item in data.get("symbols", []):
                if (
                    item.get("quoteAsset") == "USDT"
                    and item.get("status") == "TRADING"
                ):
                    sym = item.get("symbol")
                    if sym and is_crypto_usdt_symbol(sym):
                        symbols.add(sym)

            if symbols:
                VALID_FUTURES_SYMBOLS = symbols
                logging.info("✅ Фьючерсов Binance загружено: %d", len(symbols))

    except Exception as e:
        logging.warning("⚠️ Ошибка exchangeInfo: %s", e)


async def get_market_tickers(session):
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"

    try:
        async with session.get(url, headers=HEADERS, timeout=10) as resp:
            if resp.status != 200:
                return []

            data = await resp.json()
            result = []

            for item in data:
                symbol = item.get("symbol")

                if not is_crypto_usdt_symbol(symbol):
                    continue

                if VALID_FUTURES_SYMBOLS and symbol not in VALID_FUTURES_SYMBOLS:
                    continue

                price = safe_float(item.get("lastPrice"))
                quote_volume = safe_float(item.get("quoteVolume"))
                change_24h = safe_float(item.get("priceChangePercent"))

                if price <= 0 or quote_volume < MIN_24H_VOLUME_USDT:
                    continue

                result.append((symbol, price, quote_volume, change_24h))

            return result

    except Exception as e:
        logging.warning("❌ Ticker error: %s", e)
        return []


async def get_klines(session, symbol, interval="1h", limit=100):
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=8) as resp:
            if resp.status != 200:
                logging.debug("⚠️ Binance HTTP Error %s: status %d", symbol, resp.status)
                return []

            data = await resp.json()

            if not isinstance(data, list):
                logging.debug("⚠️ Binance ошибка данных %s: %s", symbol, data)
                return []

            return sorted(data, key=lambda x: parse_kline(x)[0])

    except Exception as e:
        logging.debug("⚠️ Exception Klines %s: %s", symbol, e)
        return []


async def fetch_current_open_interest_usdt(session, symbol, current_price):
    url = f"{BASE_URL}/fapi/v1/openInterest"
    params = {"symbol": symbol}

    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=5) as resp:
            if resp.status != 200:
                return None

            data = await resp.json()
            value = safe_float(data.get("openInterest"))

            if value <= 0:
                return None

            return value * current_price

    except Exception:
        return None


# ============================================================
#                  RVOL CALCULATION
# ============================================================

async def fetch_hourly_rvol(session, symbol):
    klines = await get_klines(session, symbol, "1h", 21)
    if len(klines) < 12:
        return 0.0

    parsed = [parse_kline(k) for k in klines]
    volumes = [v * c for _, _, _, _, c, v in parsed if c > 0 and v > 0]

    if len(volumes) < 5:
        return 1.0

    current_volume = volumes[-1]
    avg_volume = sum(volumes[:-1]) / len(volumes[:-1])

    if avg_volume <= 0:
        return 1.0

    elapsed = time.time() - (int(time.time()) // 3600) * 3600
    projected = current_volume / max(0.1, min(elapsed / 3600, 1.0))

    return projected / avg_volume


async def fetch_short_rvol(session, symbol):
    klines = await get_klines(session, symbol, SHORT_RVOL_INTERVAL, SHORT_RVOL_LOOKBACK + 6)
    if len(klines) < 5:
        return 1.0

    parsed = [parse_kline(k) for k in klines]
    volumes = [v * c for _, _, _, _, c, v in parsed if c > 0 and v > 0]

    if len(volumes) < 5:
        return 1.0

    recent_count = min(SHORT_RVOL_RECENT_COUNT, len(volumes))
    recent = volumes[-recent_count:]
    historical = volumes[max(0, len(volumes) - recent_count - SHORT_RVOL_LOOKBACK):len(volumes) - recent_count]

    if not historical:
        return 1.0

    avg_volume = sum(historical) / len(historical)
    if avg_volume <= 0:
        return 1.0

    return (sum(recent) / len(recent)) / avg_volume


# ============================================================
#                       TELEGRAM
# ============================================================

async def send_telegram_alert(session, text):
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or CHAT_ID == "YOUR_TELEGRAM_CHAT_ID":
        logging.info("[TG MOCK]\n%s", text)
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}

    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        logging.error("Telegram error: %s", e)
        return False


async def send_startup_message(session):
    text = (
        "🟢 <b>CONSOL_PULSE WATCH (BINANCE) ЗАПУЩЕН</b>\n\n"
        "🔎 Полный поиск новых полок: <b>каждые 20 минут</b>\n"
        "👁 Мониторинг найденных полок: <b>каждые 30 секунд</b>\n\n"
        f"🟡 Ранний импульс: <b>+{EARLY_TRIGGER_PCT:.1f}%</b>\n"
        f"🟠 Подтверждение: <b>+{CONFIRM_TRIGGER_PCT:.1f}%</b>\n"
        f"🚀 Сильный импульс: <b>+{PUMP_TRIGGER_PCT:.1f}%</b>\n\n"
        f"🧲 Макс. тело полки: <b>{MAX_SHELF_WIDTH_PCT:.1f}%</b>\n"
        f"🧲 Макс. диапазон с тенями: <b>{MAX_SHELF_WICK_WIDTH_PCT:.1f}%</b>\n"
        f"🧲 База: <b>{MIN_SHELF_CANDLES}-{MAX_SHELF_CANDLES}ч</b>\n\n"
        f"🧲 Минимальный OI: <b>${MIN_OPEN_INTEREST_USDT:,.0f}</b>"
    )
    await send_telegram_alert(session, text)


# ============================================================
#                  SHELF DETECTOR
# ============================================================

def check_shelf_before_impulse(candles):
    if len(candles) < 13:
        return None

    closed = candles[:-1]

    if len(closed) < MIN_SHELF_CANDLES:
        return None

    all_closes = [c["close"] for c in closed if c["close"] > 0]
    if len(all_closes) < EMA_SLOW:
        return None

    ema20_series = calculate_ema_series(all_closes, EMA_FAST)
    ema40_series = calculate_ema_series(all_closes, EMA_SLOW)

    candidates = []
    total = len(closed)

    for window in range(MIN_SHELF_CANDLES, MAX_SHELF_CANDLES + 1):
        if window > total:
            continue

        for end_idx in range(total - 1, max(window - 2, total - 20), -1):
            start_idx = end_idx - window + 1
            if start_idx < 0:
                continue

            base = closed[start_idx:end_idx + 1]
            if len(base) < MIN_SHELF_CANDLES:
                continue

            highs = [c["high"] for c in base if c["high"] > 0]
            lows = [c["low"] for c in base if c["low"] > 0]
            base_closes = [c["close"] for c in base if c["close"] > 0]

            if not highs or not lows or not base_closes:
                continue

            shelf_high = max(highs)
            shelf_low = min(lows)

            if shelf_low <= 0:
                continue

            wick_width = ((shelf_high - shelf_low) / shelf_low) * 100
            if wick_width > MAX_SHELF_WICK_WIDTH_PCT:
                continue

            close_high = max(base_closes)
            close_low = min(base_closes)

            if close_low <= 0:
                continue

            body_width = ((close_high - close_low) / close_low) * 100
            if body_width > MAX_SHELF_WIDTH_PCT:
                continue

            if end_idx >= len(ema20_series):
                continue

            ema20 = ema20_series[end_idx]
            ema40 = ema40_series[end_idx]

            if ema20 <= 0 or ema40 <= 0:
                continue

            ema_spread = (abs(ema20 - ema40) / ema40) * 100
            if ema_spread > EMA_MAX_SPREAD_PCT:
                continue

            candle_ranges = []
            for c in base:
                if c["low"] <= 0:
                    continue
                r = ((c["high"] - c["low"]) / c["low"]) * 100
                candle_ranges.append(r)

            if not candle_ranges:
                continue

            quiet_count = sum(1 for r in candle_ranges if r <= 5.0)
            quiet_ratio = quiet_count / len(candle_ranges)

            if quiet_ratio < 0.40:
                continue

            breakout_bonus = 0
            next_idx = end_idx + 1

            if next_idx < total:
                next_candle = closed[next_idx]
                next_high = next_candle["high"]
                next_low = next_candle["low"]

                if next_high > shelf_high:
                    move_up = ((next_high - shelf_high) / shelf_high) * 100
                    if move_up >= 0.5:
                        breakout_bonus = move_up
                elif next_low < shelf_low:
                    move_down = ((shelf_low - next_low) / shelf_low) * 100
                    if move_down >= 0.5:
                        breakout_bonus = move_down

            score = 0
            score += min(window, 16) * 0.20
            score += max(0, 4.0 - body_width)
            score += max(0, 4.0 - ema_spread)
            score += quiet_ratio * 3
            score += breakout_bonus * 2

            candidates.append({
                "score": score,
                "low": shelf_low,
                "high": shelf_high,
                "width": wick_width,
                "body_width": body_width,
                "ema20": ema20,
                "ema40": ema40,
                "candles": window,
                "end_idx": end_idx,
                "breakout_bonus": breakout_bonus
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[0]


# ============================================================
#                  SCAN ONE SYMBOL
# ============================================================

async def scan_one_symbol_for_shelf(session, ticker):
    symbol, price, quote_volume, change_24h = ticker

    candles_raw = await get_klines(session, symbol, "1h", 100)
    if len(candles_raw) < 13:
        return None

    candles = []
    for k in candles_raw:
        _, o, h, l, c, v = parse_kline(k)
        if c > 0 and h > 0 and l > 0:
            candles.append({"open": o, "high": h, "low": l, "close": c, "volume": v})

    shelf = check_shelf_before_impulse(candles)
    if not shelf:
        return None

    now = time.time()

    return {
        "symbol": symbol,
        "low": shelf["low"],
        "high": shelf["high"],
        "width": shelf["width"],
        "body_width": shelf.get("body_width", shelf["width"]),
        "ema20": shelf["ema20"],
        "ema40": shelf["ema40"],
        "candles": shelf["candles"],
        "created": now,
        "updated": now,
        "status": "WATCH",
        "early_sent": False,
        "confirm_sent": False,
        "pump_sent": False,
        "direction": None,
        "last_price": price
    }


# ============================================================
#                    FULL MARKET SCAN
# ============================================================

async def full_market_scan(session):
    global SHELVES

    logging.info("🔎 ПОЛНЫЙ СКАН РЫНКА BINANCE НАЧАТ")

    tickers = await get_market_tickers(session)
    if not tickers:
        logging.warning("⚠️ Тикеры не получены")
        return

    semaphore = asyncio.Semaphore(5)

    async def worker(ticker):
        async with semaphore:
            try:
                res = await scan_one_symbol_for_shelf(session, ticker)
                await asyncio.sleep(0.05)
                return res
            except Exception as e:
                logging.debug("Ошибка %s: %s", ticker[0], e)
                return None

    results = await asyncio.gather(*[worker(t) for t in tickers])

    found = 0
    updated = 0

    for shelf in results:
        if not shelf:
            continue

        symbol = shelf["symbol"]

        if symbol in SHELVES:
            SHELVES[symbol]["updated"] = time.time()
            SHELVES[symbol]["last_price"] = shelf["last_price"]
            updated += 1
        else:
            SHELVES[symbol] = shelf
            found += 1
            logging.info(
                "🧲 НОВАЯ ПОЛКА | %s | %.8f - %.8f | ширина %.2f%% | свечей=%d",
                symbol,
                shelf["low"],
                shelf["high"],
                shelf["width"],
                shelf["candles"]
            )

    cleanup_shelves()
    save_shelves()

    logging.info(
        "🔎 ПОЛНЫЙ СКАН ЗАВЕРШЁН | рынок=%d | новых=%d | обновлено=%d | всего полок=%d",
        len(tickers),
        found,
        updated,
        len(SHELVES)
    )


def cleanup_shelves():
    now = time.time()
    remove = []

    for symbol, shelf in list(SHELVES.items()):
        created = safe_float(shelf.get("created", now), now)
        status = shelf.get("status", "WATCH")
        age = now - created

        if status == "TRIGGERED" and age > TRIGGERED_TTL:
            remove.append(symbol)
        elif status != "TRIGGERED" and age > SHELF_TTL:
            remove.append(symbol)

    for symbol in remove:
        logging.info("🗑 Удалена старая полка: %s", symbol)
        SHELVES.pop(symbol, None)

    if remove:
        save_shelves()


# ============================================================
#                 CHECK SHELF IMPULSE
# ============================================================

async def check_shelf_impulse(session, shelf, ticker):
    symbol, price, quote_volume, change_24h = ticker

    shelf_low = safe_float(shelf.get("low"))
    shelf_high = safe_float(shelf.get("high"))

    if shelf_low <= 0 or shelf_high <= 0:
        return

    up_change = ((price - shelf_high) / shelf_high) * 100
    down_change = ((shelf_low - price) / shelf_low) * 100

    if up_change >= EARLY_TRIGGER_PCT:
        direction = "UP"
        movement = up_change
    elif down_change >= EARLY_TRIGGER_PCT:
        direction = "DOWN"
        movement = down_change
    else:
        shelf["last_price"] = price
        return

    if movement >= PUMP_TRIGGER_PCT:
        level = "PUMP"
        if shelf.get("pump_sent"):
            return
    elif movement >= CONFIRM_TRIGGER_PCT:
        level = "CONFIRM"
        if shelf.get("confirm_sent"):
            return
    else:
        level = "EARLY"
        if shelf.get("early_sent"):
            return

    oi_usdt = await fetch_current_open_interest_usdt(session, symbol, price)
    hourly_rvol, short_rvol = await asyncio.gather(
        fetch_hourly_rvol(session, symbol),
        fetch_short_rvol(session, symbol)
    )

    if oi_usdt is not None and oi_usdt < MIN_OPEN_INTEREST_USDT:
        shelf["last_price"] = price
        return

    if hourly_rvol < MIN_HOURLY_RVOL or short_rvol < MIN_SHORT_RVOL:
        shelf["last_price"] = price
        return

    emoji = "🟡" if level == "EARLY" else "🟠" if level == "CONFIRM" else "🚀"
    title = (
        "РАННИЙ ВЫХОД" if level == "EARLY"
        else "ПОДТВЕРЖДЁННЫЙ ПРОБОЙ" if level == "CONFIRM"
        else "СИЛЬНЫЙ ИМПУЛЬС"
    )
    dir_txt = "🚀 ВВЕРХ" if direction == "UP" else "🔻 ВНИЗ"
    
    # Исправленное имя монеты для отображения
    clean_coin = symbol[:-4] if symbol.endswith("USDT") else symbol

    message = (
        f"{emoji} <b>{title}</b>\n\n"
        f"<code>{symbol}</code>\n"
        f"<b>{clean_coin}/USDT</b>\n\n"
        f"📈 Направление: <b>{dir_txt}</b>\n"
        f"⚡ От полки: <b>{'+' if direction == 'UP' else '-'}{movement:.2f}%</b>\n"
        f"🎯 Уровень: <b>{level}</b>\n\n"
        f"🧲 Полка: <b>{format_price(shelf_low)} — {format_price(shelf_high)}</b>\n"
        f"📐 Ширина: <b>{shelf.get('width', 0):.2f}%</b>\n"
        f"📊 Тело полки: <b>{shelf.get('body_width', 0):.2f}%</b>\n\n"
        f"💰 Цена: <b>{format_price(price)}</b>\n"
        f"📊 24h: <b>{change_24h:+.2f}%</b>\n"
        f"📊 RVOL 1H: <b>{hourly_rvol:.2f}x</b> | 5M: <b>{short_rvol:.2f}x</b>\n"
    )

    if oi_usdt:
        message += f"👁 OI: <b>${oi_usdt:,.0f}</b>"

    await send_telegram_alert(session, message)

    shelf["last_price"] = price
    shelf["direction"] = direction
    shelf["status"] = "TRIGGERED"
    shelf["created"] = time.time()

    if level == "EARLY":
        shelf["early_sent"] = True
    elif level == "CONFIRM":
        shelf["confirm_sent"] = True
    elif level == "PUMP":
        shelf["pump_sent"] = True

    save_shelves()


async def watch_shelves(session, tickers):
    if not SHELVES:
        return 0

    ticker_map = {item[0]: item for item in tickers}
    tasks = []

    for sym, shelf in list(SHELVES.items()):
        ticker = ticker_map.get(sym)
        if ticker:
            tasks.append(check_shelf_impulse(session, shelf, ticker))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    return len(tasks)


# ============================================================
#                       MAIN LOOP
# ============================================================

async def main_loop():
    global ACTIVE_SYMBOLS

    timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        logging.info("🚀 CONSOL_PULSE WATCH (BINANCE) STARTED")

        load_shelves()
        await update_futures_symbols(session)
        await send_startup_message(session)

        await full_market_scan(session)
        last_full_scan = time.time()

        while True:
            started = time.time()

            if time.time() - last_full_scan >= FULL_SCAN_INTERVAL:
                await update_futures_symbols(session)
                await full_market_scan(session)
                last_full_scan = time.time()

            tickers = await get_market_tickers(session)

            if tickers:
                ACTIVE_SYMBOLS = {item[0] for item in tickers}
                watched = await watch_shelves(session, tickers)
                sec_to_full = max(0, FULL_SCAN_INTERVAL - (time.time() - last_full_scan))

                logging.info(
                    "📊 WATCH SCAN | рынок=%d | полок=%d | WATCH=%d | время=%.1fs | след. полный скан через %.1f мин",
                    len(tickers),
                    len(SHELVES),
                    watched,
                    time.time() - started,
                    sec_to_full / 60
                )

            cleanup_shelves()

            sleep_time = max(1, WATCH_INTERVAL - (time.time() - started))
            await asyncio.sleep(sleep_time)


# ============================================================
#                         START
# ============================================================

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    asyncio.run(main_loop())
