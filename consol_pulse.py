import asyncio
import logging
import math
import os
import random
import time
import threading
import aiohttp
from flask import Flask

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
    try:
        port = int(os.environ.get("PORT", 10000))
        cli = logging.getLogger('werkzeug')
        cli.setLevel(logging.ERROR)
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    except Exception as e:
        logging.error("Flask server crash: %s", e)

# ============================================================
#                      CONFIG & CONSTANTS
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

BASE_URL = "https://fapi.binance.com"

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

# Фильтры поиска
MIN_24H_VOLUME_USDT = 12_000_000   
MAX_SHELF_WIDTH_PCT = 1.3          
SHELF_TTL = 3 * 3600              

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

FULL_SCAN_INTERVAL = 15 * 60   # 15 минут
WATCH_INTERVAL = 15            # 15 секунд

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

def is_crypto_usdt_symbol(symbol):
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
        logging.warning("Telegram credentials missing!")
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

async def get_market_tickers(session):
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"
    try:
        async with session.get(url, headers=get_headers(), timeout=10) as resp:
            if resp.status in (418, 429):
                logging.warning(f"⚠️ Binance Rate Limit (HTTP {resp.status})!")
                return {}
            if resp.status != 200:
                return {}

            data = await resp.json()
            result = {}
            for item in data:
                sym = item.get("symbol")
                if sym and is_crypto_usdt_symbol(sym):
                    price = safe_float(item.get("lastPrice"))
                    vol = safe_float(item.get("quoteVolume"))
                    change = safe_float(item.get("priceChangePercent"))
                    if price > 0 and vol >= MIN_24H_VOLUME_USDT:
                        result[sym] = (sym, price, vol, change)
            return result
    except Exception as e:
        logging.error("Ошибка получения тикеров: %s", e)
        return {}

async def fetch_orderbook_liquidity(session, symbol):
    url = f"{BASE_URL}/fapi/v1/depth"
    params = {"symbol": symbol, "limit": 20}
    try:
        async with session.get(url, params=params, headers=get_headers(), timeout=5) as resp:
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
        async with session.get(url, params=params, headers=get_headers(), timeout=8) as resp:
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
        async with session.get(url, params={"symbol": symbol}, headers=get_headers(), timeout=5) as resp:
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
        async with session.get(url, params=params, headers=get_headers(), timeout=5) as resp:
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

    high_distance = ((current["high"] - shelf_high) / shelf_high) * 100
    if high_distance > 1.8 and distance < (high_distance * 0.6):
        return None

    body_pct = ((current["close"] - current["open"]) / current["open"]) * 100
    if body_pct < MIN_5M_BODY_PCT:
        return None

    last_closed = parsed[-2]
    c_range = last_closed["high"] - last_closed["low"]
    if c_range > 0:
        upper_wick = last_closed["high"] - max(last_closed["open"], last_closed["close"])
        if (upper_wick / c_range) > 0.45:
            return None

    closes = [c["close"] for c in parsed[-20:]]
    ema20 = sum(closes) / len(closes)
    if current["close"] < ema20:
        return None

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
        return

    if abs(change_24h) > MAX_24H_CHANGE_PCT:
        return

    shelf_high = safe_float(shelf.get("high"))
    shelf_low = safe_float(shelf.get("low"))
    if shelf_high <= 0:
        return

    up_change = ((price - shelf_high) / shelf_high) * 100
    if up_change < BREAKOUT_TRIGGER_PCT:
        return

    if up_change > MAX_BREAKOUT_DISTANCE_PCT:
        shelf["status"] = "TRIGGERED"
        shelf["signal_sent"] = True
        return

    liquidity_usd = await fetch_orderbook_liquidity(session, symbol)
    if liquidity_usd < 15_000:
        return

    candles_5m = await get_klines(session, symbol, SHORT_INTERVAL, SHORT_LOOKBACK)
    breakout = check_5m_breakout(candles_5m, shelf_high)
    if not breakout:
        return

    oi_value, oi_growth = await fetch_oi_growth(session, symbol)
    if oi_value is None:
        oi_value = await fetch_current_open_interest(session, symbol, price)

    if oi_value is None or oi_value < MIN_OPEN_INTEREST_USDT or (oi_growth is not None and oi_growth < MIN_OI_GROWTH_PCT):
        return

    clean_coin = symbol[:-4] if symbol.endswith("USDT") else symbol

    message = (
        "🚀 <b>РАННИЙ ИМПУЛЬС</b>\n\n"
        f"<code>{clean_coin}</code>\n\n"
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

# ============================================================
#                      SCANNER LOOPS
# ============================================================

async def analyze_symbol_for_shelf(session, sym):
    klines = await get_klines(session, sym, "1h", 20)
    if len(klines) < 15:
        return None

    parsed = [parse_kline(k) for k in klines[-15:]]
    highs = [c[2] for c in parsed]
    lows = [c[3] for c in parsed]

    max_h, min_l = max(highs), min(lows)
    if min_l <= 0:
        return None

    width_pct = ((max_h - min_l) / min_l) * 100
    if width_pct <= MAX_SHELF_WIDTH_PCT:
        return sym, {
            "high": max_h,
            "low": min_l,
            "created_at": now_ts(),
            "status": "WATCHING",
            "signal_sent": False
        }
    return None

async def scan_market(session, tickers):
    now = now_ts()
    expired = [s for s, data in ACTIVE_SHELVES.items() if now - data.get("created_at", now) > SHELF_TTL]
    for s in expired:
        del ACTIVE_SHELVES[s]

    logging.info(f"🔎 Сканирование {len(tickers)} пар на наличие полок...")
    
    symbols = list(tickers.keys())
    chunk_size = 10
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        tasks = [analyze_symbol_for_shelf(session, sym) for sym in chunk]
        results = await asyncio.gather(*tasks)
        
        for res in results:
            if res:
                sym, shelf_data = res
                ACTIVE_SHELVES[sym] = shelf_data
        await asyncio.sleep(0.2)

    logging.info(f"📊 Сканирование завершено. Активных полок в WATCH: {len(ACTIVE_SHELVES)}")

async def watch_loop(session, tickers):
    tasks = []
    for sym, shelf in list(ACTIVE_SHELVES.items()):
        if sym in tickers:
            tasks.append(check_shelf_impulse(session, shelf, tickers[sym]))
    if tasks:
        await asyncio.gather(*tasks)

# ============================================================
#                         MAIN LOOP
# ============================================================

async def main_loop():
    async with aiohttp.ClientSession() as session:
        last_full_scan = 0
        cached_tickers = {}

        while True:
            try:
                now = now_ts()

                # 1. Полное сканирование (раз в 15 минут)
                if now - last_full_scan >= FULL_SCAN_INTERVAL or not cached_tickers:
                    cached_tickers = await get_market_tickers(session)
                    if cached_tickers:
                        await scan_market(session, cached_tickers)
                        last_full_scan = now
                    else:
                        await asyncio.sleep(60)
                        continue

                # 2. Быстрая проверка полок (ТОЛЬКО если есть за чем следить!)
                if ACTIVE_SHELVES:
                    current_tickers = await get_market_tickers(session)
                    if current_tickers:
                        await watch_loop(session, current_tickers)

                await asyncio.sleep(WATCH_INTERVAL)

            except Exception as e:
                logging.error("Ошибка в главном цикле: %s", e)
                await asyncio.sleep(15)

# ============================================================
#                         START
# ============================================================

if __name__ == "__main__":
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    logging.info("🌐 Flask веб-сервер запущен в фоновом потоке")

    try:
        asyncio.run(main_loop())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Бот остановлен")
