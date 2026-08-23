import asyncio
import logging
import math
import os
import time
import aiohttp
from flask import Flask
import threading

# ============================================================
#                     LOGGING & APP SCAFFOLD
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

@app.route("/")
def health_check():
    return "ConsolPulse Bot is Live", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# ============================================================
#                      CONFIG & CONSTANTS
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

BASE_URL = "https://fapi.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Фильтры поиска
MIN_24H_VOLUME_USDT = 12_000_000   # От $12 млн объемов
MAX_SHELF_WIDTH_PCT = 1.3          # Зажали ширину полки (было 2.5)
MAX_SHELF_WICK_WIDTH_PCT = 1.8     # Макс. ширина с тенями
SHELF_TTL = 3 * 3600              # Время жизни полки — 3 часа

# Триггеры пробоя
BREAKOUT_TRIGGER_PCT = 0.15
MAX_BREAKOUT_DISTANCE_PCT = 2.0
MIN_5M_BODY_PCT = 0.25
MIN_5M_RVOL = 2.0
MIN_OI_GROWTH_PCT = 1.0
MIN_OPEN_INTEREST_USDT = 300_000
MAX_24H_CHANGE_PCT = 20.0
MAX_15M_MOVE_PCT = 4.0

SHORT_INTERVAL = "5m"
SHORT_LOOKBACK = 15

# Исключения (Акции, премаркет, мета)
EXCLUDED_BASES = {
    "USDT", "BUSD", "FDUSD", "USDC", "BTC", "ETH",
    "XAUT", "PAXG", "XAG", "XAU", "COHR", "SPCX", "DELL", 
    "ANTHROPIC", "OPENAI", "ARM", "NVDA", "TSLA", "AAPL", "AMZN"
}

ACTIVE_SHELVES = {}

# ============================================================
#                     UTILITY FUNCTIONS
# ============================================================

def now_ts():
    return int(time.time())

def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def normalize_symbol(symbol):
    return symbol.upper().strip() if symbol else ""

def is_crypto_usdt_symbol(symbol):
    symbol = normalize_symbol(symbol)
    if not symbol or not symbol.endswith("USDT"):
        return False
    
    base = symbol[:-4]

    if any(tag in symbol for tag in ["PRE-", "INDEX", "STOCK", "MOVE"]):
        return False

    if base in EXCLUDED_BASES:
        return False

    return True

def format_price(val):
    if val >= 100:
        return f"{val:.2f}"
    elif val >= 1:
        return f"{val:.4f}"
    else:
        return f"{val:.6f}"

# ============================================================
#                  TELEGRAM & BINANCE API
# ============================================================

async def send_telegram_alert(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram token/chat_id missing!")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with session.post(url, json=payload, timeout=8) as resp:
            return resp.status == 200
    except Exception as e:
        logging.error("Ошибка Telegram API: %s", e)
        return False

async def get_crypto_symbols(session):
    """
    Замена exchangeInfo на ticker/24hr для обхода HTTP 418 на Render
    """
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"
    try:
        async with session.get(url, headers=HEADERS, timeout=10) as resp:
            if resp.status in (418, 429):
                logging.warning("⚠️ Binance Rate Limit (HTTP %d)! Пауза 15 сек...", resp.status)
                await asyncio.sleep(15)
                return []
            if resp.status != 200:
                return []
            
            data = await resp.json()
            symbols = []
            for item in data:
                sym = item.get("symbol", "")
                vol = safe_float(item.get("quoteVolume"))
                if sym.endswith("USDT") and vol >= MIN_24H_VOLUME_USDT:
                    if is_crypto_usdt_symbol(sym):
                        symbols.append(sym)
            return symbols
    except Exception as e:
        logging.error("Ошибка получения тикеров: %s", e)
        return []

async def get_market_tickers(session):
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"
    try:
        async with session.get(url, headers=HEADERS, timeout=10) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            res = {}
            for item in data:
                sym = item.get("symbol")
                if sym:
                    res[sym] = (
                        sym,
                        safe_float(item.get("lastPrice")),
                        safe_float(item.get("quoteVolume")),
                        safe_float(item.get("priceChangePercent"))
                    )
            return res
    except Exception:
        return {}

async def fetch_orderbook_liquidity(session, symbol):
    """ Проверка суммарной ликвидности Top-20 стакана ($15k+) """
    url = f"{BASE_URL}/fapi/v1/depth"
    params = {"symbol": symbol, "limit": 20}
    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=5) as resp:
            if resp.status != 200:
                return 0.0
            data = await resp.json()
            bids_vol = sum(safe_float(p) * safe_float(q) for p, q in data.get("bids", []))
            asks_vol = sum(safe_float(p) * safe_float(q) for p, q in data.get("asks", []))
            return bids_vol + asks_vol
    except Exception:
        return 0.0

async def get_klines(session, symbol, interval, limit=50):
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=8) as resp:
            if resp.status != 200:
                return []
            return await resp.json()
    except Exception:
        return []

def parse_kline(k):
    return (
        int(k[0]),
        safe_float(k[1]),
        safe_float(k[2]),
        safe_float(k[3]),
        safe_float(k[4]),
        safe_float(k[5])
    )

async def fetch_current_open_interest(session, symbol, price):
    url = f"{BASE_URL}/fapi/v1/openInterest"
    try:
        async with session.get(url, params={"symbol": symbol}, headers=HEADERS, timeout=5) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return safe_float(data.get("openInterest")) * price
    except Exception:
        return None

async def fetch_oi_growth(session, symbol):
    url = f"{BASE_URL}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": "15m", "limit": 2}
    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=5) as resp:
            if resp.status != 200:
                return None, None
            data = await resp.json()
            if len(data) < 2:
                return None, None
            old_oi = safe_float(data[0].get("sumOpenInterestValue"))
            curr_oi = safe_float(data[1].get("sumOpenInterestValue"))
            if old_oi <= 0:
                return curr_oi, None
            growth = ((curr_oi - old_oi) / old_oi) * 100
            return curr_oi, growth
    except Exception:
        return None, None

# ============================================================
#                    SHELF ENGINE & BREAKOUT
# ============================================================

def calculate_rvol_5m(parsed_candles):
    if len(parsed_candles) < 7:
        return 1.0
    current_vol = parsed_candles[-1]["volume"]
    base = parsed_candles[-7:-1]
    avg_vol = sum(c["volume"] for c in base) / len(base)
    return current_vol / avg_vol if avg_vol > 0 else 1.0

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

    # 1. Защита от входа на выкате (когда шпиль ушел и откатился)
    high_distance = ((current["high"] - shelf_high) / shelf_high) * 100
    if high_distance > 1.8 and distance < (high_distance * 0.6):
        return None

    # 2. Бычье тело свечи
    body_pct = ((current["close"] - current["open"]) / current["open"]) * 100
    if body_pct < MIN_5M_BODY_PCT:
        return None

    # 3. Фильтр верхней тени у пред. свечи
    last_closed = parsed[-2]
    c_range = last_closed["high"] - last_closed["low"]
    if c_range > 0:
        upper_wick = last_closed["high"] - max(last_closed["open"], last_closed["close"])
        if (upper_wick / c_range) > 0.45:
            return None

    # 4. Проверка фильтра EMA20
    closes = [c["close"] for c in parsed[-20:]]
    ema20 = sum(closes) / len(closes)
    if current["close"] < ema20:
        return None

    # 5. RVOL
    rvol = calculate_rvol_5m(parsed)
    if rvol < MIN_5M_RVOL:
        return None

    lookback = max(0, len(parsed) - 4)
    old_price = parsed[lookback]["close"]
    move_15m = ((current["close"] - old_price) / old_price) * 100
    if move_15m > MAX_15M_MOVE_PCT:
        return None

    return {
        "price": price, "distance": distance, "body_pct": body_pct,
        "rvol": rvol, "move_15m": move_15m, "timestamp": current["timestamp"]
    }

async def check_shelf_impulse(session, shelf, ticker):
    symbol, price, quote_volume, change_24h = ticker

    if shelf.get("signal_sent") or shelf.get("status") == "TRIGGERED":
        return False

    if abs(change_24h) > MAX_24H_CHANGE_PCT:
        return False

    shelf_high = safe_float(shelf.get("high"))
    shelf_low = safe_float(shelf.get("low"))
    if shelf_high <= 0:
        return False

    up_change = ((price - shelf_high) / shelf_high) * 100
    if up_change < BREAKOUT_TRIGGER_PCT:
        return False

    if up_change > MAX_BREAKOUT_DISTANCE_PCT:
        shelf["status"] = "TRIGGERED"
        shelf["signal_sent"] = True
        return False

    # Ликвидность стакана
    liquidity_usd = await fetch_orderbook_liquidity(session, symbol)
    if liquidity_usd < 15_000:
        return False

    # Свечи 5m & Breakout Logic
    candles_5m = await get_klines(session, symbol, SHORT_INTERVAL, SHORT_LOOKBACK)
    breakout = check_5m_breakout(candles_5m, shelf_high)
    if not breakout:
        return False

    # Проверка OI
    oi_value, oi_growth = await fetch_oi_growth(session, symbol)
    if oi_value is None:
        oi_value = await fetch_current_open_interest(session, symbol, price)

    if oi_value is None or oi_value < MIN_OPEN_INTEREST_USDT or (oi_growth is not None and oi_growth < MIN_OI_GROWTH_PCT):
        return False

    clean_coin = symbol[:-4] if symbol.endswith("USDT") else symbol

    message = (
        "🚀 <b>РАННИЙ ИМПУЛЬС</b>\n\n"
        f"<code>{clean_coin}/USDT</code>\n\n"
        f"📈 Направление: <b>🚀 ВВЕРХ</b>\n"
        f"⚡ От полки: <b>+{breakout['distance']:.2f}%</b>\n"
        f"🧲 Полка: <b>{format_price(shelf_low)} — {format_price(shelf_high)}</b>\n"
        f"💧 Ликвидность стакана: <b>${liquidity_usd:,.0f}</b>\n\n"
        f"💰 Цена: <b>{format_price(price)}</b>\n"
        f"📊 24h: <b>{change_24h:+.2f}%</b>\n"
        f"🔥 5M RVOL: <b>{breakout['rvol']:.2f}x</b>\n"
        f"👁 OI: <b>${oi_value:,.0f}</b>\n"
        f"📈 OI за 15M: <b>+{oi_growth if oi_growth else 0:.2f}%</b>"
    )

    await send_telegram_alert(session, message)
    shelf["status"] = "TRIGGERED"
    shelf["signal_sent"] = True
    return True

# ============================================================
#                      SCANNER LOOPS
# ============================================================

async def scan_market(session):
    symbols = await get_crypto_symbols(session)
    if not symbols:
        return

    now = now_ts()
    # Чистка старых полок по TTL (3 часа)
    expired = [s for s, data in ACTIVE_SHELVES.items() if now - data.get("created_at", now) > SHELF_TTL]
    for s in expired:
        del ACTIVE_SHELVES[s]

    for sym in symbols:
        klines = await get_klines(session, sym, "1h", 20)
        if len(klines) < 15:
            continue

        parsed = [parse_kline(k) for k in klines[-15:]]
        highs = [c[2] for c in parsed]
        lows = [c[3] for c in parsed]

        max_h, min_l = max(highs), min(lows)
        if min_l <= 0:
            continue

        width_pct = ((max_h - min_l) / min_l) * 100
        if width_pct <= MAX_SHELF_WIDTH_PCT:
            ACTIVE_SHELVES[sym] = {
                "high": max_h,
                "low": min_l,
                "created_at": now,
                "status": "WATCHING",
                "signal_sent": False
            }

async def watch_loop(session):
    tickers = await get_market_tickers(session)
    if not tickers:
        return

    for sym, shelf in list(ACTIVE_SHELVES.items()):
        if sym in tickers:
            await check_shelf_impulse(session, shelf, tickers[sym])

async def main_loop():
    async with aiohttp.ClientSession() as session:
        last_scan = 0
        while True:
            try:
                now = now_ts()
                if now - last_scan >= 1200: # Полный скан раз в 20 мин
                    await scan_market(session)
                    last_scan = now

                await watch_loop(session)
                await asyncio.sleep(5)
            except Exception as e:
                logging.error("Ошибка в главном цикле: %s", e)
                await asyncio.sleep(10)

if __name__ == "__main__":
    t = threading.Thread(target=run_flask)
    t.daemon = True
    t.start()

    asyncio.run(main_loop())
