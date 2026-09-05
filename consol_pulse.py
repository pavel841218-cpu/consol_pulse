import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from aiogram import Bot

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN") or os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID") or os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"
BYBIT_BASE_URL = "https://api.bybit.com"

# ===== НАСТРОЙКИ СТРАТЕГИИ (15M ТАЙМФРЕЙМ) =====
TIMEFRAME = "15m"               # Быстрый таймфрейм для точной ловли импульсов
SHELF_MIN_CANDLES = 16          # 16 свечей по 15м = 4 часа накопительной полки
SHELF_MAX_WIDTH_PCT = 3.0       # Максимальная ширина полки (3.0%)
BREAKOUT_PCT = 1.8              # Реагируем на пробой от +1.8% (самый начало движения)
EMA_PERIODS = [20, 40, 80]      # Полноценная фильтрация по EMA 80

# Свежесть полки
FRESHNESS_LOOKBACK = 12         # Проверяем последние 3 часа перед полкой
OLD_IMPULSE_PCT = 2.5           # Порог старого импульса

# Метрики
RVOL_LOOKBACK = 20              
MIN_24H_VOLUME_USDT = 1_000_000 # Порог объема 1M$

# ===== ОПТИМИЗАЦИЯ ТРАФИКА ДЛЯ RENDER =====
CHECK_INTERVAL_SECONDS = 30     # Опрос каждые 30 секунд
KLINE_LIMIT = 50                # Качаем всего 50 свечей вместо 100
MIN_PRICE_CHANGE_TO_CHECK = 1.0 # Проверяем монеты с дневным приростом от +1.0%

ALERT_COOLDOWN_SECONDS = 3 * 3600 # 3 часа кулдаун на один символ
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

def candle_range(candle):
    return candle["high"] - candle["low"]

def cleanup_storage():
    now = time.time()
    expired = [sym for sym, t in last_signals.items() if now - t > ALERT_COOLDOWN_SECONDS]
    for sym in expired:
        del last_signals[sym]

async def health_check(request):
    return web.Response(text="Partizan Bot Active", status=200)

async def fetch_bingx_symbols(session):
    """Первичный быстрый фильтр: отбирает только активные монеты без качки свечей"""
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
                if any(x in sym for x in ["_", "FOOTBALL", "INDEX", "STKFQ", "NCFX", "NCCO"]):
                    continue
                
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                price_change = safe_float(item.get("priceChangePercent"))

                # Фильтруем монеты еще до скачивания свечей
                if vol >= MIN_24H_VOLUME_USDT and price > 0 and price_change >= MIN_PRICE_CHANGE_TO_CHECK:
                    result[sym] = vol
            return result
    except Exception as e:
        logging.error(f"Ошибка получения тикеров BingX: {e}")
        return {}

async def fetch_klines(session, symbol, semaphore):
    """Запрос урезанного количества свечей на 15m"""
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": KLINE_LIMIT}
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
                            "open": safe_float(k.get("open")),
                            "high": safe_float(k.get("high")),
                            "low": safe_float(k.get("low")),
                            "close": safe_float(k.get("close")),
                            "volume": safe_float(k.get("volume"))
                        })
                    elif isinstance(k, list) and len(k) >= 6:
                        parsed.append({
                            "open": safe_float(k[1]),
                            "high": safe_float(k[2]),
                            "low": safe_float(k[3]),
                            "close": safe_float(k[4]),
                            "volume": safe_float(k[5])
                        })
                return parsed
        except Exception:
            return []

async def get_oi_growth(session, symbol, semaphore):
    bybit_symbol = symbol.replace("-", "").upper()
    url = f"{BYBIT_BASE_URL}/v5/market/open-interest"
    params = {"category": "linear", "symbol": bybit_symbol, "intervalTime": "5min", "limit": 2}
    headers = {"User-Agent": "Mozilla/5.0"}

    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status != 200:
                    return 0.0, "Н/Д"
                data = await resp.json()
                if data.get("retCode") != 0:
                    return 0.0, "Н/Д"

                items = data.get("result", {}).get("list", [])
                if len(items) < 2:
                    return 0.0, "Н/Д"

                items.sort(key=lambda x: safe_float(x.get("timestamp")), reverse=True)
                current_oi = safe_float(items[0].get("openInterest"))
                previous_oi = safe_float(items[1].get("openInterest"))

                if previous_oi <= 0:
                    return 0.0, "Н/Д"

                growth = ((current_oi - previous_oi) / previous_oi) * 100
                return growth, "Bybit"
        except Exception:
            return 0.0, "Ошибка"

async def send_signal(bot, symbol, breakout_pct, base_high, current_price, vol_24h, rvol, vol_expansion, oi_growth, oi_source):
    try:
        coin = symbol.split("-")[0].upper()
        
        rvol_str = f"<b>{rvol:.2f}x</b> 🔥" if rvol >= 2.0 else f"<code>{rvol:.2f}x</code>"
        
        if oi_source in ["Н/Д", "Ошибка"]:
            oi_str = "<code>Н/Д</code>"
        elif oi_growth > 0:
            oi_str = f"<b>+{oi_growth:.2f}%</b> 🟢"
        else:
            oi_str = f"<code>{oi_growth:.2f}%</code> 🔴"

        vol_str = f"<b>x{vol_expansion:.1f} (Растёт)</b> ⚡" if vol_expansion >= 1.5 else f"<code>x{vol_expansion:.1f} (Норма)</code>"

        message = (
            f"🚀 <b>СВЕЖАЯ ПОЛКА + ПРОБОЙ (15M)</b>\n\n"
            f"Монета: <code>{coin}</code>\n"
            f"💥 Пробой: <b>+{breakout_pct:.2f}%</b>\n\n"
            f"📊 <b>Информационное поле:</b>\n"
            f"├ RVOL (Объем): {rvol_str}\n"
            f"├ Волатильность: {vol_str}\n"
            f"└ Изм. ОИ: {oi_str}\n\n"
            f"📍 <b>Уровни:</b>\n"
            f"├ Верх полки: <code>{format_price(base_high)}</code>\n"
            f"├ Текущая цена: <code>{format_price(current_price)}</code>\n"
            f"└ Объём 24ч: <b>${vol_24h/1_000_000:.2f}M</b>\n\n"
            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть BingX</a>"
        )
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки сигнала {symbol}: {e}")
        return False

def is_fresh_shelf(candles, base_start, base_high):
    start = max(0, base_start - FRESHNESS_LOOKBACK)
    previous = candles[start:base_start]
    if len(previous) < 3:
        return True

    for c in previous:
        if base_high > 0:
            old_breakout_pct = ((c["close"] - base_high) / base_high) * 100
            if old_breakout_pct >= OLD_IMPULSE_PCT:
                return False
    return True

async def check_symbol(session, bot, symbol, vol_24h, semaphore):
    now = time.time()
    if symbol in last_signals and (now - last_signals[symbol]) < ALERT_COOLDOWN_SECONDS:
        return False

    candles = await fetch_klines(session, symbol, semaphore)
    if len(candles) < (SHELF_MIN_CANDLES + FRESHNESS_LOOKBACK):
        return False

    closed_candles = candles[:-1]
    current_candle = candles[-1]

    # 1. Расчет полки по телам (убираем ложные тени)
    base_start = len(closed_candles) - SHELF_MIN_CANDLES
    shelf_candles = closed_candles[base_start:]
    
    base_high = max(c["high"] for c in shelf_candles)
    body_high = max(max(c["open"], c["close"]) for c in shelf_candles)
    body_low = min(min(c["open"], c["close"]) for c in shelf_candles)

    if body_low <= 0:
        return False

    shelf_width_pct = ((body_high - body_low) / body_low) * 100
    if shelf_width_pct > SHELF_MAX_WIDTH_PCT:
        return False

    # 2. Логика EMA (EMA 80 защищает от движения против тренда)
    closes = [c["close"] for c in closed_candles]
    ema20 = calculate_ema(closes, 20)[-1] if len(closes) >= 20 else 0
    ema40 = calculate_ema(closes, 40)[-1] if len(closes) >= 40 else 0

    if ema40 > 0 and not (body_low * 0.98 <= ema20 <= base_high * 1.02):
        return False

    # 3. Проверка свежести полки
    if not is_fresh_shelf(closed_candles, base_start, base_high):
        return False

    # 4. Момент пробоя
    current_price = current_candle["close"]
    breakout_pct = ((current_price - base_high) / base_high) * 100
    if breakout_pct < BREAKOUT_PCT:
        return False

    # === РАСЧЕТ ИНФО-ПОЛЕЙ ===
    lookback_candles = closed_candles[-RVOL_LOOKBACK:]
    avg_volume = sum(c["volume"] for c in lookback_candles) / len(lookback_candles) if lookback_candles else 0
    rvol = (current_candle["volume"] / avg_volume) if avg_volume > 0 else 1.0

    avg_base_range = sum(candle_range(c) for c in shelf_candles) / len(shelf_candles)
    curr_range = candle_range(current_candle)
    vol_expansion = (curr_range / avg_base_range) if avg_base_range > 0 else 1.0

    oi_growth, oi_source = await get_oi_growth(session, symbol, semaphore)

    last_signals[symbol] = now
    success = await send_signal(
        bot, symbol, breakout_pct, base_high, current_price,
        vol_24h, rvol, vol_expansion, oi_growth, oi_source
    )
    if success:
        logging.info(f"СИГНАЛ: {symbol} | Пробой 15M: +{breakout_pct:.2f}% | RVOL: {rvol:.2f}x")
    return success

async def scanner_loop(bot):
    global scan_counter
    semaphore = asyncio.Semaphore(10)
    while True:
        try:
            session_start_time = time.time()
            connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector) as session:
                while True:
                    scan_counter += 1
                    start_time = time.time()
                    if scan_counter % 30 == 0:
                        cleanup_storage()
                    if time.time() - session_start_time > SESSION_MAX_AGE:
                        break
                    
                    symbols = await fetch_bingx_symbols(session)
                    if not symbols:
                        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                        continue
                    
                    tasks = [check_symbol(session, bot, sym, vol, semaphore) for sym, vol in symbols.items()]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    signals = sum(1 for r in results if r is True)
                    elapsed = time.time() - start_time
                    logging.info(f"Скан #{scan_counter} | {elapsed:.1f}с | Проверяется пар: {len(symbols)} | Сигналов: {signals}")
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
        await bot.send_message(chat_id=CHAT_ID, text="🤖 Бот «ПАРТИЗАН» запущен! (Таймфрейм 15M активен)")
    except Exception as e:
        logging.error(f"Ошибка отправки стартового сообщения: {e}")
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
