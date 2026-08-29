import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from aiogram import Bot

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN") or os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID") or os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"

# ===== НАСТРОЙКИ ФИЛЬТРАЦИИ =====
BASE_CANDLES_COUNT = 4          # 4 свечи (4 часа полки)
MAX_SHELF_WIDTH_PCT = 4.5       # Расширили ширину полки до 4.5%
MIN_BREAKOUT_PCT = 2.0          # Пробой от +2.0%
MIN_OI_GROWTH_PCT = 1.5         # Рост ОИ минимум на +1.5%
MIN_24H_VOLUME_USDT = 1_500_000 # Мин. суточный объем $1.5M

CHECK_INTERVAL_SECONDS = 30     
ALERT_COOLDOWN_SECONDS = 4 * 3600 
SESSION_MAX_AGE = 1800

last_signals = {}
scan_counter = 0

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def format_price(price: float) -> str:
    if price is None or price == 0:
        return "0.00"
    if price >= 1000:
        return f"{price:.2f}"
    elif price >= 1:
        return f"{price:.4f}"
    elif price >= 0.01:
        return f"{price:.6f}"
    else:
        return f"{price:.8f}"

def calculate_ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema

def cleanup_storage():
    current_time = time.time()
    expired_signals = [
        sym for sym, t in last_signals.items() 
        if current_time - t > ALERT_COOLDOWN_SECONDS
    ]
    for sym in expired_signals:
        del last_signals[sym]

async def health_check(request):
    return web.Response(text="Partizan Bot Active", status=200)

async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT"):
                    continue
                if any(x in sym for x in ["_", "FOOTBALL", "INDEX", "STKFQ"]):
                    continue
                
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = vol
            return result
    except Exception as e:
        logging.error(f"Ошибка получения тикеров BingX: {e}")
        return {}

async def fetch_klines(session, symbol, semaphore):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": "1h", "limit": 60}
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data = await resp.json()
                candles = data.get("data", [])
                if not isinstance(candles, list):
                    return []
                
                parsed = []
                for k in candles:
                    if isinstance(k, dict):
                        parsed.append({
                            "high": safe_float(k.get("high")),
                            "low": safe_float(k.get("low")),
                            "close": safe_float(k.get("close"))
                        })
                    elif isinstance(k, list) and len(k) >= 5:
                        parsed.append({
                            "high": safe_float(k[2]),
                            "low": safe_float(k[3]),
                            "close": safe_float(k[4])
                        })
                return parsed
        except Exception:
            return []

async def get_oi_growth(session, symbol, semaphore):
    binance_symbol = symbol.replace("-", "").upper()
    binance_url = "https://fapi.binance.com/fapi/v1/openInterestHist"
    params_binance = {"symbol": binance_symbol, "period": "1h", "limit": 5}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    async with semaphore:
        # 1. Запрос к Binance Futures
        try:
            async with session.get(binance_url, params=params_binance, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and len(data) >= 3:
                        prev_oi = safe_float(data[-3].get("sumOpenInterest"))
                        curr_oi = safe_float(data[-2].get("sumOpenInterest"))
                        
                        if prev_oi > 0:
                            growth = ((curr_oi - prev_oi) / prev_oi) * 100
                            logging.debug(f"✅ Binance OI для {symbol}: {growth:.2f}%")
                            return growth, "Binance"
                else:
                    logging.warning(f"⚠️ Binance ответил статусом {resp.status} для {symbol}")
        except Exception as e:
            logging.debug(f"❌ Ошибка подключения к Binance OI ({symbol}): {e}")

        # 2. Фолбэк на BingX (если монеты нет на Binance или IP заблокирован)
        try:
            bingx_url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/openInterestHistory"
            params_bingx = {"symbol": symbol, "interval": "1h", "limit": 5}
            async with session.get(bingx_url, params=params_bingx, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    oi_list = data.get("data", [])
                    if isinstance(oi_list, list) and len(oi_list) >= 3:
                        prev_oi = safe_float(oi_list[-3].get("openInterest"))
                        curr_oi = safe_float(oi_list[-2].get("openInterest"))
                        if prev_oi > 0:
                            growth = ((curr_oi - prev_oi) / prev_oi) * 100
                            logging.debug(f"✅ BingX OI fallback для {symbol}: {growth:.2f}%")
                            return growth, "BingX"
        except Exception as e:
            logging.debug(f"❌ Ошибка подключения к BingX OI ({symbol}): {e}")

        return 0.0, "None"

async def send_signal(bot, symbol, breakout_pct, oi_growth_pct, base_high, current_price, vol_24h, oi_source):
    try:
        clean_coin = symbol.split("-")[0].upper()
        message = (
            f"🚀 <b>ИМПУЛЬС С ДЕНЬГАМИ ({oi_source} OI)</b>\n\n"
            f"Монета: <code>{clean_coin}</code>\n"
            f"💥 Пробой BingX: <b>+{breakout_pct:.2f}%</b>\n"
            f"📈 Прирост ОИ (1H): <b>+{oi_growth_pct:.2f}%</b> ({oi_source})\n"
            f"├ Верх базы: <code>{format_price(base_high)}</code>\n"
            f"├ Текущая цена: <code>{format_price(current_price)}</code>\n"
            f"└ Объем 24h: <b>${vol_24h/1_000_000:.2f}M</b>\n\n"
            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть BingX</a>"
        )
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки сигнала по {symbol}: {e}")
        return False

async def check_symbol(session, bot, symbol, vol_24h, semaphore):
    now = time.time()
    if symbol in last_signals and (now - last_signals[symbol]) < ALERT_COOLDOWN_SECONDS:
        return False

    candles = await fetch_klines(session, symbol, semaphore)
    if len(candles) < 45:
        return False

    base_candles = candles[-(BASE_CANDLES_COUNT + 1):-1]
    current_candle = candles[-1]

    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)
    if base_low <= 0:
        return False

    shelf_width_pct = ((base_high - base_low) / base_low) * 100
    if shelf_width_pct > MAX_SHELF_WIDTH_PCT:
        return False

    closes = [c["close"] for c in candles]
    ema20_list = calculate_ema(closes, 20)
    ema40_list = calculate_ema(closes, 40)

    if len(ema20_list) < 4 or len(ema40_list) < 4:
        return False

    ema_in_base_count = 0
    for i in range(-4, -1):
        if (base_low <= ema20_list[i] <= base_high) and (base_low <= ema40_list[i] <= base_high):
            ema_in_base_count += 1
    if ema_in_base_count < 3:
        return False

    current_price = current_candle["close"]
    breakout_pct = ((current_price - base_high) / base_high) * 100
    if breakout_pct < MIN_BREAKOUT_PCT:
        return False

    # Получение OI с логированием
    oi_growth_pct, oi_source = await get_oi_growth(session, symbol, semaphore)
    logging.info(
        f"OI {symbol}: источник={oi_source}, рост={oi_growth_pct:+.2f}%"
        f" (порог {MIN_OI_GROWTH_PCT}%)"
    )
    if oi_growth_pct < MIN_OI_GROWTH_PCT:
        logging.debug(f"{symbol} отклонён: недостаточный рост OI")
        return False

    last_signals[symbol] = now
    success = await send_signal(bot, symbol, breakout_pct, oi_growth_pct, base_high, current_price, vol_24h, oi_source)
    if success:
        logging.info(f"СИГНАЛ: {symbol} | Пробой: +{breakout_pct:.2f}% | ОИ ({oi_source}): +{oi_growth_pct:.2f}%")
    return success

async def scanner_loop(bot):
    global scan_counter
    semaphore = asyncio.Semaphore(10)
    
    while True:
        try:
            session_start_time = time.time()
            connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, force_close=False)
            
            async with aiohttp.ClientSession(connector=connector) as session:
                while True:
                    scan_counter += 1
                    start_time = time.time()
                    
                    if scan_counter % 30 == 0:
                        cleanup_storage()
                    
                    if time.time() - session_start_time > SESSION_MAX_AGE:
                        break
                    
                    symbols_dict = await fetch_bingx_symbols(session)
                    if not symbols_dict:
                        await asyncio.sleep(30)
                        break
                    
                    total_signals = 0
                    tasks = [check_symbol(session, bot, sym, vol, semaphore) for sym, vol in symbols_dict.items()]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for r in results:
                        if r is True:
                            total_signals += 1
                    
                    elapsed = time.time() - start_time
                    logging.info(f"Скан #{scan_counter} | {elapsed:.1f}с | Пары: {len(symbols_dict)} | Сигналов: {total_signals}")
                    await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                    
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Ошибка сканера: {e}")
            await asyncio.sleep(10)

async def main():
    bot = Bot(token=BOT_TOKEN)
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    logging.info(f"🌐 Веб-сервер запущен на порту {PORT}")
    try:
        await bot.send_message(chat_id=CHAT_ID, text="🤖 Бот «ПАРТИЗАН» успешно запущен и сканирует рынок!")
    except Exception as e:
        logging.error(f"❌ Ошибка отправки стартового сообщения: {e}")

    try:
        await scanner_loop(bot)
    finally:
        await runner.cleanup()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
