import os
import time
import asyncio
import logging
import threading
import aiohttp
from flask import Flask

# ============================================================
#  БОТ «ПАРТИЗАН» (ФИЛЬТР: ПОЛКА + EMA + BINANCE ОИ + ИМПУЛЬС)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE = "https://open-api.bingx.com"

# --- Настройки фильтрации ---
BASE_CANDLES_COUNT = 4          # 4 свечи (4 часа накопления)
MAX_SHELF_WIDTH_PCT = 2.5       # Макс. ширина полки 2.5%
MIN_BREAKOUT_PCT = 3.0          # Пробой от +3.0%
MIN_OI_GROWTH_PCT = 2.0         # Рост ОИ на Binance минимум на +2.0%
MIN_24H_VOLUME_USDT = 3_000_000 # Мин. объем на BingX $3M
CHECK_INTERVAL_SECONDS = 30     
ALERT_COOLDOWN_SECONDS = 4 * 3600 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)

@app.route("/")
def home():
    return "Partizan Bot Active", 200

def run_flask():
    cli = logging.getLogger('werkzeug')
    cli.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=PORT)

def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def format_price(price):
    if price >= 1000: return f"{price:.2f}"
    elif price >= 1: return f"{price:.4f}"
    elif price >= 0.01: return f"{price:.6f}"
    else: return f"{price:.8f}"

def calculate_ema(prices, period):
    if len(prices) < period: return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema

async def get_tradable_symbols(session):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                logging.warning(f"BingX API вернул ошибку: {data}")
                return {}
            result = {}
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT"): continue
                if any(x in sym for x in ["_", "FOOTBALL", "INDEX", "STKFQ"]): continue
                
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = vol
            logging.info(f"Получено {len(result)} ликвидных монет")
            return result
    except Exception as e:
        logging.error(f"Ошибка получения тикеров: {e}")
        return {}

async def get_klines(session, symbol, semaphore):
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": "1h", "limit": 60}
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=8) as resp:
                data = await resp.json()
                candles = data.get("data", [])
                if not isinstance(candles, list):
                    logging.warning(f"Некорректный ответ klines для {symbol}")
                    return []
                
                parsed = [{
                    "high": safe_float(k.get("high")),
                    "low": safe_float(k.get("low")),
                    "close": safe_float(k.get("close"))
                } for k in candles]
                parsed.reverse()
                return parsed
        except Exception as e:
            logging.warning(f"Ошибка klines для {symbol}: {e}")
            return []

async def get_binance_oi_growth(session, symbol, semaphore):
    """
    Проверяет динамику ОИ на Binance Futures API.
    Приводит тикер BingX (например 'TRUMP-USDT') к формату Binance ('TRUMPUSDT').
    """
    binance_symbol = symbol.replace("-", "").upper()
    url = "https://fapi.binance.com/fapi/v1/openInterestHist"
    params = {"symbol": binance_symbol, "period": "1h", "limit": 3}
    headers = {"User-Agent": "Mozilla/5.0"}

    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers, timeout=6) as resp:
                if resp.status != 200:
                    logging.info(f"Binance OI для {symbol}: статус {resp.status}")
                    return 0.0

                data = await resp.json()
                if not isinstance(data, list) or len(data) < 2:
                    return 0.0

                prev_oi = safe_float(data[-2].get("sumOpenInterest"))
                curr_oi = safe_float(data[-1].get("sumOpenInterest"))

                if prev_oi <= 0: return 0.0
                return ((curr_oi - prev_oi) / prev_oi) * 100
        except Exception as e:
            logging.warning(f"Ошибка получения OI для {symbol}: {e}")
            return 0.0

async def send_telegram(session, text):
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logging.info(f"[MOCK]: {text}")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=8): pass
    except Exception as e:
        logging.error(f"Ошибка Telegram: {e}")

async def check_symbol(session, symbol, vol_24h, sent_alerts, semaphore):
    now = time.time()
    if symbol in sent_alerts and (now - sent_alerts[symbol]) < ALERT_COOLDOWN_SECONDS:
        return

    candles = await get_klines(session, symbol, semaphore)
    if len(candles) < 45: return

    base_candles = candles[-(BASE_CANDLES_COUNT + 1):-1]
    current_candle = candles[-1]

    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)
    if base_low <= 0: return

    # 1. Полка (узкий диапазон)
    shelf_width_pct = ((base_high - base_low) / base_low) * 100
    if shelf_width_pct > MAX_SHELF_WIDTH_PCT: return

    # 2. Переплетение EMA20/40 (3 свечи внутри базы)
    closes = [c["close"] for c in candles]
    ema20_list = calculate_ema(closes, 20)
    ema40_list = calculate_ema(closes, 40)

    ema_in_base_count = 0
    for i in range(-4, -1):
        if (base_low <= ema20_list[i] <= base_high) and (base_low <= ema40_list[i] <= base_high):
            ema_in_base_count += 1
    if ema_in_base_count < 3: return

    # 3. Реальный пробой (от +3.0%)
    current_price = current_candle["close"]
    breakout_pct = ((current_price - base_high) / base_high) * 100
    if breakout_pct < MIN_BREAKOUT_PCT: return

    # 4. Проверка заноса денег через Binance Futures OI (минимум +2.0%)
    oi_growth_pct = await get_binance_oi_growth(session, symbol, semaphore)
    if oi_growth_pct < MIN_OI_GROWTH_PCT:
        logging.info(f"OI reject {symbol}: +{oi_growth_pct:.2f}%")
        return

    clean_coin = symbol.split("-")[0].upper()
    message = (
        f"🚀 <b>ИМПУЛЬС С ДЕНЬГАМИ (BINANCE OI)</b>\n\n"
        f"Монета: <code>{clean_coin}</code>\n"
        f"💥 Пробой BingX: <b>+{breakout_pct:.2f}%</b>\n"
        f"📈 Прирост ОИ Binance (1H): <b>+{oi_growth_pct:.2f}%</b>\n"
        f"├ Верх базы: <code>{format_price(base_high)}</code>\n"
        f"├ Текущая цена: <code>{format_price(current_price)}</code>\n"
        f"└ Объем 24h: <b>${vol_24h/1_000_000:.2f}M</b>\n\n"
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть BingX</a>"
    )

    await send_telegram(session, message)
    sent_alerts[symbol] = now
    logging.info(f"СИГНАЛ: {clean_coin} | Пробой: +{breakout_pct:.2f}% | Binance ОИ: +{oi_growth_pct:.2f}%")

async def self_ping():
    """Пингует собственный сервер, чтобы Render не усыплял сервис"""
    await asyncio.sleep(30)
    port = int(os.environ.get("PORT", 10000))
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"http://localhost:{port}", timeout=5):
                    pass
            except Exception:
                pass
            await asyncio.sleep(600)

async def main():
    sent_alerts = {}
    semaphore = asyncio.Semaphore(10)
    async with aiohttp.ClientSession() as session:
        logging.info("Скрипт запущен с отслеживанием ОИ на Binance")
        # Запускаем self-ping
        asyncio.create_task(self_ping())
        await send_telegram(session, "🛡 <b>Бот Партизан обновлен!</b> Подключен модуль фильтрации ОИ напрямую с Binance Futures.")

        while True:
            try:
                symbols_dict = await get_tradable_symbols(session)
                if not symbols_dict:
                    logging.warning("Пустой список тикеров, повтор через 30 сек")
                    await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                    continue

                logging.info(f"Сканирую {len(symbols_dict)} монет")
                tasks = [check_symbol(session, sym, vol, sent_alerts, semaphore) for sym, vol in symbols_dict.items()]
                await asyncio.gather(*tasks)
            except Exception as e:
                logging.error(f"Ошибка цикла: {e}")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        pass
