import asyncio
import logging
import os
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

# Публичный узел Binance, устойчивый к блокировкам 418 на Render
BASE_URL = "https://fapi1.binance.com"

# Ценовая ниша
MIN_PRICE_TIER = 0.0001
MAX_PRICE_TIER = 1.0000

# Базовый фильтр объёма (чтобы не тянуть все монеты)
BASE_MIN_24H_VOLUME = 10_000

# Анти-зомби
MAX_DROP_FROM_30D_HIGH_PCT = 85.0

# Параметры полки
MIN_SHELF_CANDLES = 12
MAX_SHELF_CANDLES = 48
MAX_SHELF_WICK_WIDTH_PCT = 3.0
MAX_SHELF_WIDTH_PCT = 1.8
MAX_SHELF_SLOPE_PCT = 0.8
EMA_FAST = 20
EMA_SLOW = 40
EMA_MAX_SPREAD_PCT = 1.2

# RVOL базовый
MIN_RVOL_4H = 1.2

# Интервалы
WATCH_INTERVAL = 30            # 30 секунд

EXCLUDED_BASES = {
    "USDT", "BUSD", "FDUSD", "USDC", "BTC", "ETH",
    "XAUT", "PAXG", "XAG", "XAU", "COHR", "SPCX", "DELL",
    "ANTHROPIC", "OPENAI", "ARM", "NVDA", "TSLA", "AAPL", "AMZN"
}

# ============================================================
#                     UTILITY FUNCTIONS
# ============================================================

def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
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
    if val >= 1000:
        return f"{val:.2f}"
    elif val >= 1:
        return f"{val:.4f}"
    elif val >= 0.01:
        return f"{val:.6f}"
    else:
        return f"{val:.8f}"

# ============================================================
#                  EMA и адаптивные пороги
# ============================================================

def calculate_ema_series(data, period):
    if len(data) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for price in data[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return [0.0] * (period - 1) + ema

def get_adaptive_thresholds(price: float) -> dict:
    if price < 0.0005:
        return {
            "tier_name": "ULTRA",
            "min_volume_24h": 50_000,
            "min_breakout_pct": 1.5,
            "max_breakout_pct": 4.5,
            "min_cvd_delta": 2_000,
            "min_buy_ratio": 0.65,
            "min_oi_growth": 4.0,
            "max_spread_pct": 0.80
        }
    elif price < 0.01:
        return {
            "tier_name": "MICRO",
            "min_volume_24h": 100_000,
            "min_breakout_pct": 1.2,
            "max_breakout_pct": 3.5,
            "min_cvd_delta": 5_000,
            "min_buy_ratio": 0.62,
            "min_oi_growth": 3.0,
            "max_spread_pct": 0.50
        }
    elif price < 0.05:
        return {
            "tier_name": "LOW",
            "min_volume_24h": 250_000,
            "min_breakout_pct": 1.0,
            "max_breakout_pct": 3.0,
            "min_cvd_delta": 10_000,
            "min_buy_ratio": 0.60,
            "min_oi_growth": 2.5,
            "max_spread_pct": 0.25
        }
    elif price < 0.20:
        return {
            "tier_name": "MID",
            "min_volume_24h": 500_000,
            "min_breakout_pct": 0.8,
            "max_breakout_pct": 2.5,
            "min_cvd_delta": 20_000,
            "min_buy_ratio": 0.58,
            "min_oi_growth": 1.8,
            "max_spread_pct": 0.15
        }
    elif price <= 1.0:
        return {
            "tier_name": "HEAVY",
            "min_volume_24h": 1_000_000,
            "min_breakout_pct": 0.5,
            "max_breakout_pct": 2.0,
            "min_cvd_delta": 35_000,
            "min_buy_ratio": 0.55,
            "min_oi_growth": 1.2,
            "max_spread_pct": 0.10
        }
    else:
        return None

# ============================================================
#                  BINANCE API HELPERS
# ============================================================

async def get_market_tickers(session):
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"
    try:
        async with session.get(url, timeout=10) as resp:
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
                    if price > 0 and vol >= BASE_MIN_24H_VOLUME:
                        result[sym] = (sym, price, vol, change)
            return result
    except Exception as e:
        logging.error("Ошибка получения тикеров: %s", e)
        return {}

async def get_klines(session, symbol, interval="1h", limit=100):
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=8) as resp:
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
        safe_float(k[7]) # Quote volume (в USDT)
    )

async def fetch_cvd_metrics(session, symbol):
    url = f"{BASE_URL}/fapi/v1/aggTrades"
    params = {"symbol": symbol, "limit": 500}
    try:
        async with session.get(url, params=params, timeout=4) as resp:
            if resp.status == 200:
                trades = await resp.json()
                buy_vol, sell_vol = 0.0, 0.0
                for t in trades:
                    vol = safe_float(t.get("p")) * safe_float(t.get("q"))
                    if t.get("m"):
                        sell_vol += vol
                    else:
                        buy_vol += vol
                total_vol = buy_vol + sell_vol
                if total_vol > 0:
                    return (buy_vol - sell_vol), (buy_vol / total_vol)
    except Exception:
        pass
    return None, None

async def check_book_spread(session, symbol, max_allowed_spread_pct):
    url = f"{BASE_URL}/fapi/v1/depth"
    params = {"symbol": symbol, "limit": 5}
    try:
        async with session.get(url, params=params, timeout=3) as resp:
            if resp.status == 200:
                data = await resp.json()
                bids, asks = data.get("bids", []), data.get("asks", [])
                if bids and asks:
                    bid, ask = safe_float(bids[0][0]), safe_float(asks[0][0])
                    if bid > 0:
                        spread_pct = ((ask - bid) / bid) * 100
                        return spread_pct <= max_allowed_spread_pct
    except Exception:
        pass
    return True

async def fetch_current_open_interest(session, symbol, price):
    url = f"{BASE_URL}/fapi/v1/openInterest"
    try:
        async with session.get(url, params={"symbol": symbol}, timeout=5) as resp:
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
        async with session.get(url, params=params, timeout=5) as resp:
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

async def fetch_rvol_4h(session, symbol):
    klines = await get_klines(session, symbol, "4h", 20)
    if len(klines) < 10:
        return 1.0
    volumes = [safe_float(k[7]) for k in klines]
    if len(volumes) < 5:
        return 1.0
    current = volumes[-1]
    avg = sum(volumes[:-1]) / len(volumes[:-1])
    return current / avg if avg > 0 else 1.0

# ============================================================
#                  SHELF DETECTOR
# ============================================================

def check_fresh_shelf(candles):
    if len(candles) < MIN_SHELF_CANDLES:
        return None

    closed = candles[:-1]
    if len(closed) < MIN_SHELF_CANDLES:
        return None

    recent = closed[-MAX_SHELF_CANDLES:]
    highs = [c["high"] for c in recent if c["high"] > 0]
    lows = [c["low"] for c in recent if c["low"] > 0]
    base_closes = [c["close"] for c in recent if c["close"] > 0]
    opens = [c["open"] for c in recent if c["open"] > 0]

    if not highs or not lows or not base_closes or not opens:
        return None

    shelf_high = max(highs)
    shelf_low = min(lows)

    if shelf_low <= 0:
        return None

    wick_width = ((shelf_high - shelf_low) / shelf_low) * 100
    if wick_width > MAX_SHELF_WICK_WIDTH_PCT:
        return None

    max_body = max(max(opens), max(base_closes))
    min_body = min(min(opens), min(base_closes))
    if ((max_body - min_body) / min_body) * 100 > MAX_SHELF_WIDTH_PCT:
        return None

    first_close = base_closes[0]
    last_close = base_closes[-1]
    slope = (abs(last_close - first_close) / first_close) * 100
    if slope > MAX_SHELF_SLOPE_PCT:
        return None

    closes_all = [c["close"] for c in closed if c["close"] > 0]
    ema20_series = calculate_ema_series(closes_all, EMA_FAST)
    ema40_series = calculate_ema_series(closes_all, EMA_SLOW)
    end_idx = len(closed) - 1
    if end_idx < EMA_SLOW - 1 or ema20_series[end_idx] <= 0 or ema40_series[end_idx] <= 0:
        return None
    spread = (abs(ema20_series[end_idx] - ema40_series[end_idx]) / ema40_series[end_idx]) * 100
    if spread > EMA_MAX_SPREAD_PCT:
        return None

    return {
        "high": shelf_high,
        "low": shelf_low
    }

# ============================================================
#                АНТИ-ЗОМБИ ПРЕДФИЛЬТР
# ============================================================

async def is_asset_alive(session, symbol) -> tuple[bool, str]:
    klines_url = f"{BASE_URL}/fapi/v1/klines"
    try:
        async with session.get(klines_url, params={"symbol": symbol, "interval": "1d", "limit": 30}, timeout=4) as resp:
            if resp.status == 200:
                klines = await resp.json()
                if len(klines) >= 10:
                    highs = [safe_float(k[2]) for k in klines]
                    cur_close = safe_float(klines[-1][4])
                    max_high = max(highs)
                    if max_high > 0:
                        drop = ((max_high - cur_close) / max_high) * 100
                        if drop > MAX_DROP_FROM_30D_HIGH_PCT:
                            return False, f"Глубокий скам (-{drop:.1f}% от 30D High)"
    except Exception:
        pass
    return True, "ALIVE"

# ============================================================
#                  АНАЛИЗ ОДНОГО СИМВОЛА
# ============================================================

async def analyze_symbol(session, symbol, price, quote_volume, change_24h, semaphore):
    async with semaphore:
        try:
            # 1. Быстрые проверки ценовых рамок и объёма
            if not (MIN_PRICE_TIER <= price <= MAX_PRICE_TIER):
                return

            t = get_adaptive_thresholds(price)
            if t is None or quote_volume < t["min_volume_24h"]:
                return

            # 2. Получение свечей для полки (всего 1 легкий запрос)
            candles_raw = await get_klines(session, symbol, "1h", 100)
            if len(candles_raw) < 50:
                return

            candles = []
            for k in candles_raw:
                ts, o, h, l, c, v = parse_kline(k)
                if o > 0 and h > 0 and l > 0 and c > 0:
                    candles.append({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})

            shelf = check_fresh_shelf(candles)
            if not shelf:
                return

            price_change_pct = ((price - shelf["high"]) / shelf["high"]) * 100
            if price_change_pct < t["min_breakout_pct"] or price_change_pct > t["max_breakout_pct"]:
                return

            # Небольшая пауза перед задействованием тяжелых фильтров
            await asyncio.sleep(0.05)

            # 3. Дополнительные проверки (запрашиваются ТОЛЬКО при наличии пробоя полки)
            is_alive, reason = await is_asset_alive(session, symbol)
            if not is_alive:
                return

            rvol_4h = await fetch_rvol_4h(session, symbol)
            if rvol_4h < MIN_RVOL_4H:
                return

            cvd_delta, buy_ratio = await fetch_cvd_metrics(session, symbol)
            if cvd_delta is not None and buy_ratio is not None:
                if cvd_delta < t["min_cvd_delta"] or buy_ratio < t["min_buy_ratio"]:
                    return

            oi_value, oi_growth = await fetch_oi_growth(session, symbol)
            if oi_value is None:
                oi_value = await fetch_current_open_interest(session, symbol, price)
            if oi_value is None or oi_value < 0:
                return
            if oi_growth is not None and oi_growth < t["min_oi_growth"]:
                return

            spread_ok = await check_book_spread(session, symbol, t["max_spread_pct"])
            if not spread_ok:
                return

            # 4. Формирование сигнала
            clean_coin = symbol[:-4] if symbol.endswith("USDT") else symbol
            message = (
                f"🚀 <b>ИМПУЛЬС {t['tier_name']}</b>\n\n"
                f"<code>{clean_coin}</code>\n\n"
                f"📈 Направление: <b>🚀 ВВЕРХ</b>\n"
                f"⚡ От полки: <b>+{price_change_pct:.2f}%</b>\n"
                f"🧲 Полка: <b>{format_price(shelf['low'])} — {format_price(shelf['high'])}</b>\n\n"
                f"💰 Цена: <b>{format_price(price)}</b>\n"
                f"📊 24h: <b>{change_24h:+.2f}%</b>\n"
                f"🔥 RVOL 4H: <b>{rvol_4h:.2f}x</b>\n"
                f"💵 CVD: <b>${cvd_delta:,.0f}</b> ({buy_ratio*100:.1f}% покупок)\n"
                f"👁 OI: <b>${oi_value:,.0f}</b> (+{oi_growth if oi_growth else 0:.2f}%)"
            )
            await send_telegram_alert(session, message)
            logging.info(f"СИГНАЛ: {symbol} | +{price_change_pct:.2f}% | RVOL {rvol_4h:.2f}")

        except Exception as e:
            logging.error(f"Ошибка анализа {symbol}: {e}")

# ============================================================
#                 TELEGRAM SENDER
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
        logging.error("Ошибка Telegram: %s", e)
        return False

# ============================================================
#                   MAIN LOOP
# ============================================================

async def self_ping():
    port = int(os.environ.get("PORT", 10000))
    await asyncio.sleep(30)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"http://localhost:{port}", timeout=5):
                    pass
            except Exception:
                pass
            await asyncio.sleep(600)

async def main_loop():
    async with aiohttp.ClientSession() as session:
        asyncio.create_task(self_ping())

        while True:
            tickers = await get_market_tickers(session)
            if not tickers:
                logging.warning("Пустой список тикеров, повтор через 60 сек")
                await asyncio.sleep(60)
                continue

            logging.info(f"🔍 Анализ {len(tickers)} монет")
            # Снижен параллелизм для исключения бана 418
            semaphore = asyncio.Semaphore(3)
            tasks = []
            for sym, (_, price, vol, change) in tickers.items():
                tasks.append(analyze_symbol(session, sym, price, vol, change, semaphore))
            await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.sleep(WATCH_INTERVAL)

if __name__ == "__main__":
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    logging.info("🌐 Flask веб-сервер запущен")

    try:
        asyncio.run(main_loop())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Бот остановлен")
