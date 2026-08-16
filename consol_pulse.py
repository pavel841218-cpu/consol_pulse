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
#                    SCAN SETTINGS
# ============================================================

# Полный поиск новых полок (уменьшено до 20 мин, чтобы не пропускать накопления)
FULL_SCAN_INTERVAL = 20 * 60

# Проверка уже найденных полок
WATCH_INTERVAL = 30

# Сколько времени живёт обычная полка
SHELF_TTL = 12 * 60 * 60

# После первого сигнала оставляем монету ещё на 4 часа
TRIGGERED_TTL = 4 * 60 * 60


# ============================================================
#                 IMPULSE LEVELS
# ============================================================

EARLY_TRIGGER_PCT = 0.5
CONFIRM_TRIGGER_PCT = 1.5
PUMP_TRIGGER_PCT = 2.5


# ============================================================
#                  MARKET FILTERS
# ============================================================

MIN_24H_VOLUME_USDT = 800_000
MIN_OPEN_INTEREST_USDT = 1_000_000
MIN_CANDLES_REQUIRED = 12


# ============================================================
#                  SHELF SETTINGS
# ============================================================

MAX_SHELF_WIDTH_PCT = 6.0
MIN_SHELF_CANDLES = 8
MAX_SHELF_CANDLES = 24
EMA_MAX_SPREAD_PCT = 6.0


# ============================================================
#                    RVOL SETTINGS
# ============================================================

SHORT_RVOL_INTERVAL = "5m"
SHORT_RVOL_LOOKBACK = 12
SHORT_RVOL_RECENT_COUNT = 3

MIN_SHORT_RVOL = 0.50
MIN_HOURLY_RVOL = 0.80


# ============================================================
#               NON-CRYPTO FILTER
# ============================================================

NON_CRYPTO_PREFIXES = (
    "NCS", "NCCO", "SP500", "US30", "US100", "NAS100",
    "GER40", "UK100", "JPN225", "GOLD", "SILVER",
    "BRENT", "WTI", "OIL", "XAU", "XAG", "XTI", "XBR",
    "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "HK", "DXY"
)

EXCLUDED_SYMBOLS = {"USDC-USDT", "FDUSD-USDT", "USD1-USDT"}
SHELVES_FILE = Path("shelves.json")

SHELVES = {}
VALID_FUTURES_SYMBOLS = set()
ACTIVE_SYMBOLS = set()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)


@app.route("/")
def home():
    return (
        f"ConsolPulse WATCH | "
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
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    return None


def is_crypto_usdt_symbol(symbol):
    if not symbol or not symbol.endswith("-USDT"):
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


def parse_kline(k):
    try:
        if isinstance(k, dict):
            timestamp = k.get("time") or k.get("timestamp") or k.get("openTime") or 0
            return int(timestamp), safe_float(k.get("open")), safe_float(k.get("high")), safe_float(k.get("low")), safe_float(k.get("close")), safe_float(k.get("volume"))
        if isinstance(k, (list, tuple)):
            return int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
    except Exception:
        pass
    return 0, 0.0, 0.0, 0.0, 0.0, 0.0


def calculate_ema(prices, period):
    if not prices or len(prices) < period:
        return 0.0
    try:
        import pandas as pd
        return float(pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1])
    except Exception:
        return 0.0


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
#              BINGX API & DATA FETCHING
# ============================================================

async def update_futures_symbols(session):
    global VALID_FUTURES_SYMBOLS
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return
            data = await resp.json()
            if data.get("code") != 0:
                return
            symbols = {normalize_symbol(item.get("symbol")) for item in data.get("data", [])}
            symbols = {s for s in symbols if s and is_crypto_usdt_symbol(s)}
            if symbols:
                VALID_FUTURES_SYMBOLS = symbols
                logging.info("✅ Фьючерсов загружено: %d", len(symbols))
    except Exception as e:
        logging.warning("⚠️ Ошибка contracts: %s", e)


async def get_market_tickers(session):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            if data.get("code") != 0:
                return []
            result = []
            for item in data.get("data", []):
                symbol = normalize_symbol(item.get("symbol"))
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


async def get_klines(session, symbol, interval="1h", limit=45):
    url = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol.replace("-", ""), "interval": interval, "limit": limit}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=8) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            if data.get("code") != 0:
                return []
            candles = data.get("data", [])
            if not isinstance(candles, list):
                return []
            return sorted(candles, key=lambda x: parse_kline(x)[0])
    except Exception:
        return []


# ============================================================
#                CHECK SHELF (ИСПРАВЛЕНО)
# ============================================================

def check_shelf_before_impulse(candles):
    if len(candles) < 13:
        return None

    closed = candles[:-1]
    if len(closed) < MIN_SHELF_CANDLES:
        return None

    # БЕРЕМ ВСЕ ЗАКРЫТЫЕ СВЕЧИ ДЛЯ ВЫЧИСЛЕНИЯ EMA
    all_closes = [c["close"] for c in closed if c["close"] > 0]
    
    ema20 = calculate_ema(all_closes, 20)
    ema40 = calculate_ema(all_closes, 40)

    if ema20 <= 0 or ema40 <= 0:
        return None

    ema_spread = abs(ema20 - ema40) / ema40 * 100
    if ema_spread > EMA_MAX_SPREAD_PCT:
        return None

    # Полку ищем по последним свечам
    base_count = min(MAX_SHELF_CANDLES, len(closed))
    base = closed[-base_count:]

    highs = [c["high"] for c in base if c["high"] > 0]
    lows = [c["low"] for c in base if c["low"] > 0]

    if not highs or not lows:
        return None

    shelf_high = max(highs)
    shelf_low = min(lows)

    if shelf_low <= 0:
        return None

    shelf_width = (shelf_high - shelf_low) / shelf_low * 100
    if shelf_width > MAX_SHELF_WIDTH_PCT:
        return None

    return {
        "low": shelf_low,
        "high": shelf_high,
        "width": shelf_width,
        "ema20": ema20,
        "ema40": ema40,
        "candles": len(base)
    }


# ============================================================
#                INDICATORS & FILTERS
# ============================================================

async def fetch_current_open_interest_usdt(session, symbol, current_price):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/openInterest"
    headers = {"User-Agent": "Mozilla/5.0"}
    for sym in (symbol, symbol.replace("-", "")):
        try:
            async with session.get(url, params={"symbol": sym}, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
                if data.get("code") != 0:
                    continue
                oi_data = data.get("data")
                if isinstance(oi_data, list) and oi_data:
                    oi_data = oi_data[0]
                if not isinstance(oi_data, dict):
                    continue
                value = oi_data.get("openInterestValue") or oi_data.get("openInterest") or oi_data.get("value")
                if value is None:
                    continue
                value = float(value)
                if value <= 0:
                    continue
                return value * current_price if value < 500_000 and current_price > 0 else value
        except Exception:
            continue
    return None


async def fetch_hourly_rvol(session, symbol):
    klines = await get_klines(session, symbol, "1h", 21)
    if len(klines) < 12:
        return 0.0
    volumes = [v * c for _, _, _, _, c, v in [parse_kline(k) for k in klines] if c > 0 and v > 0]
    if len(volumes) < 5:
        return 1.0
    current_volume = volumes[-1]
    avg_volume = sum(volumes[:-1]) / len(volumes[:-1])
    if avg_volume <= 0:
        return 1.0
    elapsed = max(1, time.time() - (int(time.time()) // 3600) * 3600)
    projected = current_volume / max(0.1, min(elapsed / 3600, 1.0))
    return projected / avg_volume


async def fetch_short_rvol(session, symbol):
    klines = await get_klines(session, symbol, SHORT_RVOL_INTERVAL, SHORT_RVOL_LOOKBACK + 6)
    if len(klines) < 5:
        return 1.0
    volumes = [v * c for _, _, _, _, c, v in [parse_kline(k) for k in klines] if c > 0 and v > 0]
    if len(volumes) < 5:
        return 1.0
    recent = volumes[-min(SHORT_RVOL_RECENT_COUNT, len(volumes)):]
    historical = volumes[max(0, len(volumes) - len(recent) - SHORT_RVOL_LOOKBACK): len(volumes) - len(recent)]
    if not historical:
        return 1.0
    avg_volume = sum(historical) / len(historical)
    return (sum(recent) / len(recent)) / avg_volume if avg_volume > 0 else 1.0


# ============================================================
#              TELEGRAM & NOTIFICATIONS
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
        "🟢 <b>CONSOL_PULSE WATCH ЗАПУЩЕН</b>\n\n"
        "🔎 Полный поиск полок: <b>каждые 20 минут</b>\n"
        "👁 Мониторинг полок: <b>каждые 30 секунд</b>\n\n"
        f"🟡 Ранний импульс: <b>+{EARLY_TRIGGER_PCT:.1f}%</b>\n"
        f"🟠 Подтверждение: <b>+{CONFIRM_TRIGGER_PCT:.1f}%</b>\n"
        f"🚀 Сильный импульс: <b>+{PUMP_TRIGGER_PCT:.1f}%</b>\n\n"
        f"🧲 Минимальный OI: <b>${MIN_OPEN_INTEREST_USDT:,.0f}</b>"
    )
    await send_telegram_alert(session, text)


# ============================================================
#              SCANNERS & WATCHERS
# ============================================================

async def scan_one_symbol_for_shelf(session, ticker):
    symbol, price, quote_volume, change_24h = ticker
    if change_24h > 50:
        return None

    candles_raw = await get_klines(session, symbol, "1h", 45)
    if len(candles_raw) < 13:
        return None

    candles = []
    for k in candles_raw:
        _, o, h, l, c, v = parse_kline(k)
        if c > 0:
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
        "ema20": shelf["ema20"],
        "ema40": shelf["ema40"],
        "created": now,
        "updated": now,
        "status": "WATCH",
        "early_sent": False,
        "confirm_sent": False,
        "pump_sent": False,
        "direction": None,
        "last_price": price
    }


async def full_market_scan(session):
    global SHELVES
    logging.info("🔎 ПОЛНЫЙ СКАН РЫНКА НАЧАТ")
    tickers = await get_market_tickers(session)
    if not tickers:
        return

    semaphore = asyncio.Semaphore(15)

    async def worker(ticker):
        async with semaphore:
            try:
                return await scan_one_symbol_for_shelf(session, ticker)
            except Exception:
                return None

    results = await asyncio.gather(*[worker(t) for t in tickers])
    found = 0

    for shelf in results:
        if not shelf:
            continue
        symbol = shelf["symbol"]
        if symbol in SHELVES:
            SHELVES[symbol]["updated"] = time.time()
            SHELVES[symbol]["last_price"] = shelf["last_price"]
        else:
            SHELVES[symbol] = shelf
            found += 1
            logging.info("🧲 НОВАЯ ПОЛКА | %s | %.8f - %.8f | ширина %.2f%%", symbol, shelf["low"], shelf["high"], shelf["width"])

    cleanup_shelves()
    save_shelves()
    logging.info("🔎 ПОЛНЫЙ СКАН ЗАВЕРШЁН | новых=%d | всего полок=%d", found, len(SHELVES))


def cleanup_shelves():
    now = time.time()
    remove = []
    for symbol, shelf in SHELVES.items():
        created = float(shelf.get("created", now))
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


async def check_shelf_impulse(session, shelf, ticker):
    symbol, price, quote_volume, change_24h = ticker
    shelf_low, shelf_high = float(shelf["low"]), float(shelf["high"])

    if shelf_low <= 0 or shelf_high <= 0:
        return

    up_change = (price - shelf_high) / shelf_high * 100
    down_change = (shelf_low - price) / shelf_low * 100

    if up_change >= EARLY_TRIGGER_PCT:
        direction, movement = "UP", up_change
    elif down_change >= EARLY_TRIGGER_PCT:
        direction, movement = "DOWN", down_change
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

    emoji = "🟡" if level == "EARLY" else ("🟠" if level == "CONFIRM" else "🚀")
    title = "РАННИЙ ВЫХОД" if level == "EARLY" else ("ПОДТВЕРЖДЁННЫЙ ПРОБОЙ" if level == "CONFIRM" else "СИЛЬНЫЙ ИМПУЛЬС")
    dir_txt = "🚀 ВВЕРХ" if direction == "UP" else "🔻 ВНИЗ"
    clean_coin = symbol.replace("-USDT", "")

    message = (
        f"{emoji} <b>{title}</b>\n\n"
        f"<code>{clean_coin}</code>\n"
        f"<b>{clean_coin}USDT</b>\n\n"
        f"📈 Направление: <b>{dir_txt}</b>\n"
        f"⚡ От полки: <b>{'+' if direction == 'UP' else '-'}{movement:.2f}%</b>\n"
        f"🎯 Уровень: <b>{level}</b>\n\n"
        f"🧲 Полка: <b>{format_price(shelf_low)} — {format_price(shelf_high)}</b>\n"
        f"📐 Ширина: <b>{shelf.get('width', 0):.2f}%</b>\n\n"
        f"💰 Цена: <b>{format_price(price)}</b>\n"
        f"📊 24h: <b>{change_24h:+.2f}%</b>\n"
        f"📊 RVOL 1H: <b>{hourly_rvol:.2f}x</b> | 5M: <b>{short_rvol:.2f}x</b>\n"
        f"👁 OI: <b>${oi_usdt:,.0f}</b>" if oi_usdt else ""
    )

    await send_telegram_alert(session, message)

    # ИСПРАВЛЕНИЕ: Обновляем created, чтобы полка не удалялась следующие 4 часа
    shelf["last_price"] = price
    shelf["direction"] = direction
    shelf["status"] = "TRIGGERED"
    shelf["created"] = time.time()  # Перезапуск отсчета TTL

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
    tasks = [check_shelf_impulse(session, shelf, ticker_map[sym]) for sym, shelf in list(SHELVES.items()) if sym in ticker_map]
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
        logging.info("🚀 CONSOL_PULSE WATCH STARTED")
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
                    len(tickers), len(SHELVES), watched, time.time() - started, sec_to_full / 60
                )

            cleanup_shelves()
            await asyncio.sleep(max(1, WATCH_INTERVAL - (time.time() - started)))


if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main_loop())
