import os
import time
import json
import re
import asyncio
import logging
import random
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

# Прокси для обхода ограничений Binance (418 / 429)
PROXY_URL = os.environ.get("PROXY_URL", "http://igfcrdac:2dlh2182mwvd@31.59.20.176:6754")


# ============================================================
#                     SCAN SETTINGS
# ============================================================

FULL_SCAN_INTERVAL = 20 * 60
WATCH_INTERVAL = 30

SHELF_TTL = 12 * 60 * 60
TRIGGERED_TTL = 60 * 60


# ============================================================
#                 EARLY BREAKOUT SETTINGS
# ============================================================

BREAKOUT_TRIGGER_PCT = 0.20
MAX_BREAKOUT_DISTANCE_PCT = 1.80
MAX_15M_MOVE_PCT = 2.0
MIN_5M_BODY_PCT = 0.10


# ============================================================
#                    MARKET FILTERS
# ============================================================

MIN_24H_VOLUME_USDT = 2_000_000
MAX_24H_CHANGE_PCT = 8.0
MIN_OPEN_INTEREST_USDT = 1_500_000
MIN_OI_GROWTH_PCT = 1.5


# ============================================================
#                    SHELF SETTINGS
# ============================================================

MIN_SHELF_CANDLES = 6
MAX_SHELF_CANDLES = 24
MAX_SHELF_WIDTH_PCT = 2.5
MAX_SHELF_WICK_WIDTH_PCT = 3.0
MAX_SHELF_SLOPE_PCT = 1.2

EMA_FAST = 20
EMA_SLOW = 40
EMA_MAX_SPREAD_PCT = 1.5

MIN_QUIET_RATIO = 0.80
MAX_SINGLE_CANDLE_RANGE_PCT = 2.2


# ============================================================
#                 ORDERBOOK (DEPTH) SETTINGS
# ============================================================

DEPTH_LIMIT = 100 
WALL_MULTIPLIER = 4.0 
WALL_DISTANCE_MAX_PCT = 1.5 


# ============================================================
#                     5M SETTINGS
# ============================================================

SHORT_INTERVAL = "5m"
SHORT_LOOKBACK = 18
MIN_5M_RVOL = 1.50
MIN_RECENT_5M_RVOL = 1.20
MAX_RED_BODY_PCT = 1.0


# ============================================================
#                    OI SETTINGS
# ============================================================

OI_PERIOD = "5m"
OI_LOOKBACK = 4


# ============================================================
#                  NON-CRYPTO FILTER
# ============================================================

EXCLUDED_BASES = {
    "USDC", "FDUSD", "USD1", "TUSD", "BUSD", "DAI",
    "EUR", "GBP", "JPY", "AUD",
    "BZ", "NATGAS", "COPPER", "GOLD", "SILVER", "BRENT", "WTI", "OIL",
    "SPY", "QQQ", "SOX", "TQQQ",
    "NVDA", "PLTR", "TSLA", "AAPL", "MSFT", "AMZN", "META", "GOOG", "INTC",
    "NFLX", "COIN", "MSTR", "IREN", "LITE", "SAMSUNG",
    "BMN", "TMT", "CLUS", "NUS", "AMD", "DXY",
}


# ============================================================
#                     STORAGE
# ============================================================

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
        f"ConsolPulse EARLY Binance | "
        f"Shelves={len(SHELVES)} | "
        f"FullScan=20m | "
        f"Watch=30s"
    ), 200

def run_flask():
    cli = logging.getLogger('werkzeug')
    cli.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# ============================================================
#                       HELPERS
# ============================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json"
    }

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def normalize_symbol(symbol):
    if not symbol:
        return None
    symbol = str(symbol).strip().upper()
    if symbol.endswith("-USDT"):
        symbol = symbol[:-5] + "USDT"
    if not symbol.endswith("USDT"):
        return None
    return symbol

def is_crypto_usdt_symbol(symbol):
    symbol = normalize_symbol(symbol)
    if not symbol:
        return False

    base = symbol[:-4]
    if base in EXCLUDED_BASES:
        return False

    if not re.match(r"^[A-Z0-9]{2,15}$", base):
        return False

    for item in EXCLUDED_BASES:
        if base == item:
            return False
        if base.startswith(item) and len(base) > len(item):
            return False

    return True

def format_price(price):
    price = safe_float(price)
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    if price >= 0.01:
        return f"{price:.6f}"
    if price >= 0.0001:
        return f"{price:.8f}"
    return f"{price:.10f}"

def now_ts():
    return time.time()


# ============================================================
#                       KLINE PARSER
# ============================================================

def parse_kline(k):
    try:
        if isinstance(k, dict):
            timestamp = k.get("openTime") or k.get("time") or k.get("timestamp") or 0
            return (
                int(timestamp),
                safe_float(k.get("open")),
                safe_float(k.get("high")),
                safe_float(k.get("low")),
                safe_float(k.get("close")),
                safe_float(k.get("volume"))
            )

        if isinstance(k, (list, tuple)) and len(k) >= 6:
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
#                         EMA
# ============================================================

def calculate_ema_series(prices, period):
    if not prices or len(prices) < period:
        return [0.0] * len(prices)

    multiplier = 2.0 / (period + 1.0)
    result = [0.0] * len(prices)
    sma = sum(prices[:period]) / period
    result[period - 1] = sma
    ema = sma

    for i in range(period, len(prices)):
        ema = ((prices[i] - ema) * multiplier) + ema
        result[i] = ema

    return result


# ============================================================
#                    SHELF STORAGE
# ============================================================

def save_shelves():
    try:
        temp_file = Path("shelves.json.tmp")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(SHELVES, f, ensure_ascii=False, indent=2)
        temp_file.replace(SHELVES_FILE)
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

        if not isinstance(data, dict):
            SHELVES = {}
            return

        valid = {}
        for symbol, shelf in data.items():
            if not is_crypto_usdt_symbol(symbol) or not isinstance(shelf, dict):
                continue

            width = safe_float(shelf.get("width"), 999)
            body_width = safe_float(shelf.get("body_width"), 999)

            if width > MAX_SHELF_WICK_WIDTH_PCT or body_width > MAX_SHELF_WIDTH_PCT:
                continue

            created = safe_float(shelf.get("created"), now_ts())
            if now_ts() - created > SHELF_TTL:
                continue

            if shelf.get("status") == "TRIGGERED" and (now_ts() - created > TRIGGERED_TTL):
                continue

            valid[symbol] = shelf

        SHELVES = valid
        logging.info("💾 Загружено актуальных полок: %d", len(SHELVES))
    except Exception as e:
        logging.error("❌ Ошибка загрузки shelves.json: %s", e)
        SHELVES = {}


# ============================================================
#                    BINANCE API (WITH PROXY)
# ============================================================

BASE_URL = "https://fapi.binance.com"


# ============================================================
#                 FUTURES SYMBOLS
# ============================================================

async def update_futures_symbols(session):
    global VALID_FUTURES_SYMBOLS
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    try:
        async with session.get(url, headers=get_headers(), proxy=PROXY_URL, timeout=10) as resp:
            if resp.status != 200:
                logging.warning("⚠️ exchangeInfo HTTP %d", resp.status)
                return

            data = await resp.json()
            symbols = set()
            for item in data.get("symbols", []):
                if (
                    item.get("quoteAsset") == "USDT"
                    and item.get("status") == "TRADING"
                    and item.get("contractType") == "PERPETUAL"
                ):
                    symbol = normalize_symbol(item.get("symbol"))
                    if symbol and is_crypto_usdt_symbol(symbol):
                        symbols.add(symbol)

            if symbols:
                VALID_FUTURES_SYMBOLS = symbols
                logging.info("✅ Binance USDT-M PERPETUAL: %d", len(symbols))
    except Exception as e:
        logging.warning("⚠️ exchangeInfo error: %s", e)


# ============================================================
#                     24H TICKERS
# ============================================================

async def get_market_tickers(session):
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"
    try:
        async with session.get(url, headers=get_headers(), proxy=PROXY_URL, timeout=10) as resp:
            if resp.status in (418, 429):
                logging.warning("⚠️ Binance Rate Limit (HTTP %d)! Спим 60 сек...", resp.status)
                await asyncio.sleep(60)
                return []

            if resp.status != 200:
                return []

            data = await resp.json()
            result = []
            for item in data:
                symbol = normalize_symbol(item.get("symbol"))
                if not symbol or not is_crypto_usdt_symbol(symbol):
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
        logging.warning("❌ ticker error: %s", e)
        return []


# ============================================================
#                         KLINES
# ============================================================

async def get_klines(session, symbol, interval="1h", limit=100):
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, headers=get_headers(), proxy=PROXY_URL, timeout=10) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            if not isinstance(data, list):
                return []
            return sorted(data, key=lambda x: parse_kline(x)[0])
    except Exception:
        return []


# ============================================================
#                     5M BREAKOUT DATA
# ============================================================

def calculate_rvol_5m(candles):
    if len(candles) < 8:
        return 0.0

    volumes = []
    for candle in candles:
        close = candle["close"]
        volume = candle["volume"]
        if close > 0 and volume > 0:
            volumes.append(close * volume)

    if len(volumes) < 6:
        return 0.0

    current = volumes[-1]
    historical = volumes[-7:-1]
    if not historical:
        return 0.0

    avg = sum(historical) / len(historical)
    return current / avg if avg > 0 else 0.0

def check_5m_breakout(candles, shelf_high):
    if len(candles) < 8:
        return None

    parsed = []
    for k in candles:
        ts, o, h, l, c, v = parse_kline(k)
        if o > 0 and h > 0 and l > 0 and c > 0 and v > 0:
            parsed.append({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})

    if len(parsed) < 8:
        return None

    current = parsed[-1]
    price = current["close"]
    if price <= 0:
        return None

    distance = ((price - shelf_high) / shelf_high) * 100
    if distance < BREAKOUT_TRIGGER_PCT or distance > MAX_BREAKOUT_DISTANCE_PCT:
        return None

    body_pct = ((current["close"] - current["open"]) / current["open"]) * 100
    if body_pct < MIN_5M_BODY_PCT:
        return None

    upper_wick = current["high"] - max(current["open"], current["close"])
    full_range = current["high"] - current["low"]
    if full_range > 0 and (upper_wick / full_range) > 0.55:
        return None

    rvol = calculate_rvol_5m(parsed)
    if rvol < MIN_5M_RVOL:
        return None

    previous = parsed[-2]
    recent_move = ((current["close"] - previous["close"]) / previous["close"]) * 100
    if recent_move < 0:
        return None

    lookback_index = max(0, len(parsed) - 4)
    old_price = parsed[lookback_index]["close"]
    move_15m = ((current["close"] - old_price) / old_price) * 100

    if move_15m > MAX_15M_MOVE_PCT:
        return None

    return {
        "price": price,
        "distance": distance,
        "body_pct": body_pct,
        "rvol": rvol,
        "move_15m": move_15m,
        "timestamp": current["timestamp"]
    }


# ============================================================
#                     OPEN INTEREST
# ============================================================

async def fetch_oi_growth(session, symbol):
    url = f"{BASE_URL}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": OI_PERIOD, "limit": OI_LOOKBACK}
    try:
        async with session.get(url, params=params, headers=get_headers(), proxy=PROXY_URL, timeout=8) as resp:
            if resp.status != 200:
                return None, None
            data = await resp.json()
            if not isinstance(data, list) or len(data) < 2:
                return None, None

            values = []
            for item in data:
                oi_value = safe_float(item.get("sumOpenInterestValue"))
                if oi_value <= 0:
                    oi_value = safe_float(item.get("sumOpenInterest"))
                if oi_value > 0:
                    values.append(oi_value)

            if len(values) < 2:
                return None, None

            oldest, latest = values[0], values[-1]
            if oldest <= 0:
                return None, None

            growth = ((latest - oldest) / oldest) * 100
            return latest, growth
    except Exception:
        return None, None

async def fetch_current_open_interest(session, symbol, price):
    url = f"{BASE_URL}/fapi/v1/openInterest"
    try:
        async with session.get(url, params={"symbol": symbol}, headers=get_headers(), proxy=PROXY_URL, timeout=6) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            oi = safe_float(data.get("openInterest"))
            return oi * price if oi > 0 else None
    except Exception:
        return None


# ============================================================
#                 ORDERBOOK (DEPTH) CHECKER
# ============================================================

async def check_orderbook_wall(session, symbol, current_price):
    """
    Проверяет стакан на наличие крупной стены продавцов (Asks)
    """
    url = f"{BASE_URL}/fapi/v1/depth"
    params = {"symbol": symbol, "limit": DEPTH_LIMIT}
    
    try:
        async with session.get(url, params=params, headers=get_headers(), proxy=PROXY_URL, timeout=5) as resp:
            if resp.status != 200:
                return True, "Depth API status not 200"

            data = await resp.json()
            asks = data.get("asks", [])
            
            if not asks:
                return True, "Empty asks"

            ask_volumes = []
            walls = []

            for ask_price_str, ask_qty_str in asks:
                ask_p = safe_float(ask_price_str)
                ask_q = safe_float(ask_qty_str)
                
                if ask_p <= 0 or ask_q <= 0:
                    continue
                
                vol_usdt = ask_p * ask_q
                dist_pct = ((ask_p - current_price) / current_price) * 100
                
                if 0 <= dist_pct <= WALL_DISTANCE_MAX_PCT:
                    ask_volumes.append(vol_usdt)
                    walls.append((ask_p, vol_usdt, dist_pct))

            if not ask_volumes:
                return True, "No asks in range"

            avg_vol = sum(ask_volumes) / len(ask_volumes)

            for p, vol, dist in walls:
                if vol >= (avg_vol * WALL_MULTIPLIER) and vol > 50_000:
                    logging.info(f"🧱 Найдена стенка по {symbol}: ${vol:,.0f} на уровне {p} (+{dist:.2f}%)")
                    return False, f"Wall at {format_price(p)} (${vol:,.0f})"

            return True, "Clean book"

    except Exception as e:
        logging.warning(f"⚠️ Ошибка проверки стакана {symbol}: {e}")
        return True, "Orderbook check error"


# ============================================================
#                 SHELF DETECTOR
# ============================================================

def check_shelf_before_impulse(candles):
    if len(candles) < 50:
        return None

    closed = candles[:-1]
    total = len(closed)
    if total < MIN_SHELF_CANDLES:
        return None

    closes = [c["close"] for c in closed if c["close"] > 0]
    if len(closes) < EMA_SLOW:
        return None

    ema20 = calculate_ema_series(closes, EMA_FAST)
    ema40 = calculate_ema_series(closes, EMA_SLOW)

    candidates = []
    max_end = total - 1
    min_end = max(MIN_SHELF_CANDLES - 1, total - 12)

    for end_idx in range(max_end, min_end - 1, -1):
        for window in range(MIN_SHELF_CANDLES, MAX_SHELF_CANDLES + 1):
            start_idx = end_idx - window + 1
            if start_idx < 0:
                continue

            base = closed[start_idx:end_idx + 1]
            if len(base) < MIN_SHELF_CANDLES:
                continue

            highs = [c["high"] for c in base if c["high"] > 0]
            lows = [c["low"] for c in base if c["low"] > 0]
            opens = [c["open"] for c in base if c["open"] > 0]
            base_closes = [c["close"] for c in base if c["close"] > 0]

            if not highs or not lows or not opens or not base_closes:
                continue

            shelf_high, shelf_low = max(highs), min(lows)
            if shelf_low <= 0:
                continue

            wick_width = ((shelf_high - shelf_low) / shelf_low) * 100
            if wick_width > MAX_SHELF_WICK_WIDTH_PCT:
                continue

            max_body_price = max(max(opens), max(base_closes))
            min_body_price = min(min(opens), min(base_closes))
            if min_body_price <= 0:
                continue

            body_width = ((max_body_price - min_body_price) / min_body_price) * 100
            if body_width > MAX_SHELF_WIDTH_PCT:
                continue

            slope = (abs(base_closes[-1] - base_closes[0]) / base_closes[0]) * 100
            if slope > MAX_SHELF_SLOPE_PCT:
                continue

            if end_idx >= len(ema20) or end_idx >= len(ema40):
                continue

            ema_spreads = [
                (abs(ema20[i] - ema40[i]) / ema40[i]) * 100
                for i in range(start_idx, end_idx + 1)
                if ema20[i] > 0 and ema40[i] > 0
            ]

            if not ema_spreads or max(ema_spreads) > EMA_MAX_SPREAD_PCT:
                continue

            ranges = [((c["high"] - c["low"]) / c["low"]) * 100 for c in base if c["low"] > 0]
            if not ranges:
                continue

            quiet_count = sum(1 for r in ranges if r <= MAX_SINGLE_CANDLE_RANGE_PCT)
            quiet_ratio = quiet_count / len(ranges)
            if quiet_ratio < MIN_QUIET_RATIO:
                continue

            next_idx = end_idx + 1
            if next_idx >= total:
                continue

            next_candle = closed[next_idx]
            early_move = ((next_candle["high"] - shelf_high) / shelf_high) * 100
            if early_move > MAX_BREAKOUT_DISTANCE_PCT:
                continue

            score = (min(window, 16) * 0.20) + max(0, 2.5 - body_width) + max(0, 1.5 - max(ema_spreads)) + (quiet_ratio * 4)
            age_bonus = total - end_idx
            if age_bonus == 1:
                score += 2.0
            elif age_bonus == 2:
                score += 1.0

            candidates.append({
                "score": score,
                "low": shelf_low,
                "high": shelf_high,
                "width": wick_width,
                "body_width": body_width,
                "ema20": ema20[end_idx],
                "ema40": ema40[end_idx],
                "candles": window,
                "end_idx": end_idx
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
    if abs(change_24h) > MAX_24H_CHANGE_PCT:
        return None

    candles_raw = await get_klines(session, symbol, "1h", 100)
    if len(candles_raw) < 50:
        return None

    candles = []
    for k in candles_raw:
        ts, o, h, l, c, v = parse_kline(k)
        if o > 0 and h > 0 and l > 0 and c > 0 and v > 0:
            candles.append({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})

    shelf = check_shelf_before_impulse(candles)
    if not shelf:
        return None

    created = now_ts()
    return {
        "symbol": symbol,
        "low": shelf["low"],
        "high": shelf["high"],
        "width": shelf["width"],
        "body_width": shelf["body_width"],
        "ema20": shelf["ema20"],
        "ema40": shelf["ema40"],
        "candles": shelf["candles"],
        "created": created,
        "updated": created,
        "status": "WATCH",
        "signal_sent": False,
        "direction": None,
        "last_price": price,
        "last_check": created,
        "signal_time": None
    }


# ============================================================
#                    FULL MARKET SCAN
# ============================================================

async def full_market_scan(session):
    global SHELVES
    logging.info("🔎 ПОЛНЫЙ СКАН BINANCE НАЧАТ")

    tickers = await get_market_tickers(session)
    if not tickers:
        logging.warning("⚠️ Тикеры не получены")
        return

    semaphore = asyncio.Semaphore(8)

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

    found, updated = 0, 0
    for shelf in results:
        if not shelf:
            continue

        symbol = shelf["symbol"]
        if symbol in SHELVES:
            existing = SHELVES[symbol]
            if existing.get("status") == "TRIGGERED":
                continue
            existing["updated"] = now_ts()
            existing["last_price"] = shelf["last_price"]
            updated += 1
            continue

        SHELVES[symbol] = shelf
        found += 1
        logging.info(
            "🧲 НОВАЯ ПОЛКА | %s | %s - %s | тело %.2f%% | диапазон %.2f%% | %dч",
            symbol, format_price(shelf["low"]), format_price(shelf["high"]),
            shelf["body_width"], shelf["width"], shelf["candles"]
        )

    cleanup_shelves()
    save_shelves()

    logging.info(
        "🔎 СКАН ЗАВЕРШЁН | рынок=%d | новых=%d | обновлено=%d | полок=%d",
        len(tickers), found, updated, len(SHELVES)
    )


# ============================================================
#                  CLEANUP SHELVES
# ============================================================

def cleanup_shelves():
    now = now_ts()
    remove = []

    for symbol, shelf in list(SHELVES.items()):
        if not is_crypto_usdt_symbol(symbol):
            remove.append(symbol)
            continue

        width = safe_float(shelf.get("width"), 999)
        body_width = safe_float(shelf.get("body_width"), 999)

        if width > MAX_SHELF_WICK_WIDTH_PCT or body_width > MAX_SHELF_WIDTH_PCT:
            remove.append(symbol)
            continue

        created = safe_float(shelf.get("created"), now)
        age = now - created

        if shelf.get("status") == "TRIGGERED":
            if age > TRIGGERED_TTL:
                remove.append(symbol)
        else:
            if age > SHELF_TTL:
                remove.append(symbol)

    for symbol in remove:
        SHELVES.pop(symbol, None)

    if remove:
        logging.info("🗑 Удалено старых/невалидных полок: %d", len(remove))
        save_shelves()


# ============================================================
#                  CHECK BREAKOUT
# ============================================================

async def check_shelf_impulse(session, shelf, ticker):
    symbol, price, quote_volume, change_24h = ticker

    if shelf.get("signal_sent") or shelf.get("status") == "TRIGGERED":
        shelf["last_price"] = price
        return False

    if abs(change_24h) > MAX_24H_CHANGE_PCT:
        shelf["last_price"] = price
        return False

    shelf_low = safe_float(shelf.get("low"))
    shelf_high = safe_float(shelf.get("high"))
    if shelf_low <= 0 or shelf_high <= 0:
        return False

    up_change = ((price - shelf_high) / shelf_high) * 100
    shelf["last_price"] = price
    shelf["last_check"] = now_ts()

    if up_change < BREAKOUT_TRIGGER_PCT:
        return False

    if up_change > MAX_BREAKOUT_DISTANCE_PCT:
        logging.info("⏭ Поздний пробой | %s | +%.2f%%", symbol, up_change)
        shelf["status"] = "TRIGGERED"
        shelf["signal_sent"] = True
        shelf["signal_time"] = now_ts()
        save_shelves()
        return False

    candles_5m = await get_klines(session, symbol, SHORT_INTERVAL, SHORT_LOOKBACK)
    breakout = check_5m_breakout(candles_5m, shelf_high)
    if not breakout:
        return False

    oi_value, oi_growth = await fetch_oi_growth(session, symbol)
    if oi_value is None:
        oi_value = await fetch_current_open_interest(session, symbol, price)

    if oi_value is None or oi_value < MIN_OPEN_INTEREST_USDT:
        return False

    if oi_growth is not None and oi_growth < MIN_OI_GROWTH_PCT:
        return False

    if oi_growth is None:
        return False

    # --- Проверка стакана (Orderbook Wall Filter) ---
    book_ok, book_info = await check_orderbook_wall(session, symbol, price)
    if not book_ok:
        logging.info("⛔ Сигнал отменён из-за стены в стакане | %s | %s", symbol, book_info)
        return False
    # ------------------------------------------------

    distance = breakout["distance"]
    if distance > MAX_BREAKOUT_DISTANCE_PCT or breakout["move_15m"] > MAX_15M_MOVE_PCT:
        return False

    clean_coin = symbol[:-4] if symbol.endswith("USDT") else symbol

    message = (
        "🚀 <b>РАННИЙ ИМПУЛЬС</b>\n\n"
        f"<code>{clean_coin}/USDT</code>\n\n"
        f"📈 Направление: <b>🚀 ВВЕРХ</b>\n"
        f"⚡ От полки: <b>+{distance:.2f}%</b>\n"
        f"🧲 Полка: <b>{format_price(shelf_low)} — {format_price(shelf_high)}</b>\n"
        f"📐 Диапазон: <b>{shelf.get('width', 0):.2f}%</b>\n"
        f"📊 Тело: <b>{shelf.get('body_width', 0):.2f}%</b>\n\n"
        f"💰 Цена: <b>{format_price(price)}</b>\n"
        f"📊 24h: <b>{change_24h:+.2f}%</b>\n"
        f"🔥 5M RVOL: <b>{breakout['rvol']:.2f}x</b>\n"
        f"⚡ 5M тело: <b>+{breakout['body_pct']:.2f}%</b>\n"
        f"⏱ 15M движение: <b>+{breakout['move_15m']:.2f}%</b>\n"
        f"👁 OI: <b>${oi_value:,.0f}</b>\n"
        f"📈 OI за 15M: <b>+{oi_growth:.2f}%</b>\n"
        f"🧱 Стакан: <b>{book_info}</b>"
    )

    sent = await send_telegram_alert(session, message)

    shelf["last_price"] = price
    shelf["direction"] = "UP"
    shelf["status"] = "TRIGGERED"
    shelf["signal_sent"] = True
    shelf["signal_time"] = now_ts()
    shelf["signal_price"] = price
    shelf["signal_distance"] = distance
    shelf["signal_oi"] = oi_value
    shelf["signal_oi_growth"] = oi_growth
    shelf["signal_rvol"] = breakout["rvol"]

    save_shelves()

    logging.info(
        "🚀 СИГНАЛ | %s | +%.2f%% | RVOL %.2fx | OI +%.2f%% | sent=%s",
        symbol, distance, breakout["rvol"], oi_growth, sent
    )

    return True


# ============================================================
#                       WATCH
# ============================================================

async def watch_shelves(session, tickers):
    if not SHELVES:
        return 0, 0

    ticker_map = {item[0]: item for item in tickers}
    signals = 0
    watched_count = 0

    for symbol, shelf in list(SHELVES.items()):
        ticker = ticker_map.get(symbol)
        if not ticker:
            continue

        watched_count += 1
        res = await check_shelf_impulse(session, shelf, ticker)
        if res is True:
            signals += 1
        await asyncio.sleep(0.05)

    return watched_count, signals


# ============================================================
#                    TELEGRAM
# ============================================================

async def send_telegram_alert(session, text):
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or CHAT_ID == "YOUR_TELEGRAM_CHAT_ID":
        logging.info("[TG MOCK]\n%s", text)
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        logging.error("Telegram error: %s", e)
        return False


# ============================================================
#                    STARTUP MESSAGE
# ============================================================

async def send_startup_message(session):
    text = (
        "🟢 <b>CONSOL_PULSE EARLY BINANCE</b>\n\n"
        "🎯 Режим: <b>ловим начало пампа (Через PROXY)</b>\n\n"
        f"🧲 Полка: <b>{MIN_SHELF_CANDLES}-{MAX_SHELF_CANDLES}ч</b>\n"
        f"📐 Диапазон полки: <b>≤ {MAX_SHELF_WICK_WIDTH_PCT:.1f}%</b>\n"
        f"⚡ Триггер: <b>+{BREAKOUT_TRIGGER_PCT:.2f}%</b>\n"
        f"🚫 Максимум от полки: <b>+{MAX_BREAKOUT_DISTANCE_PCT:.2f}%</b>\n\n"
        f"🔥 5M RVOL: <b>≥ {MIN_5M_RVOL:.2f}x</b>\n"
        f"👁 OI рост 15M: <b>≥ +{MIN_OI_GROWTH_PCT:.1f}%</b>\n"
        f"🧱 Проверка стакана: <b>Включена</b>\n\n"
        "🔕 <b>1 полка = 1 сигнал</b>"
    )
    await send_telegram_alert(session, text)


# ============================================================
#                       MAIN LOOP
# ============================================================

async def main_loop():
    global ACTIVE_SYMBOLS

    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=40, limit_per_host=20)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        logging.info("🚀 CONSOL_PULSE EARLY BINANCE STARTED")

        load_shelves()
        await update_futures_symbols(session)
        await send_startup_message(session)
        await full_market_scan(session)

        last_full_scan = now_ts()

        while True:
            started = now_ts()

            if now_ts() - last_full_scan >= FULL_SCAN_INTERVAL:
                await update_futures_symbols(session)
                await full_market_scan(session)
                last_full_scan = now_ts()

            tickers = await get_market_tickers(session)
            retry = 0
            while not tickers and retry < 3:
                retry += 1
                await asyncio.sleep(3)
                tickers = await get_market_tickers(session)

            if tickers:
                ACTIVE_SYMBOLS = {item[0] for item in tickers}
                watched, signals = await watch_shelves(session, tickers)
                sec_to_full = max(0, FULL_SCAN_INTERVAL - (now_ts() - last_full_scan))

                logging.info(
                    "📊 WATCH | рынок=%d | полок=%d | WATCH=%d | новых сигналов=%d | время=%.1fs | полный скан через %.1f мин",
                    len(tickers), len(SHELVES), watched, signals, now_ts() - started, sec_to_full / 60
                )
            else:
                logging.error("❌ Binance тикеры не получены")

            cleanup_shelves()
            elapsed = now_ts() - started
            sleep_time = max(1, WATCH_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)


# ============================================================
#                         START
# ============================================================

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logging.info("🌐 Flask веб-сервер запущен в фоновом потоке")

    try:
        asyncio.run(main_loop())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Бот остановлен")
