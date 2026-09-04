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

# ===== НАСТРОЙКИ =====
SHELF_MIN_CANDLES = 12          # минимум 12 свечей в полке (1h)
SHELF_MAX_WIDTH_PCT = 3.0       # ширина полки не более 3%
BREAKOUT_PCT = 4.0              # пробой вверх от верхней границы полки
EMA_PERIODS = [20, 40, 80]      # EMA, которые должны быть внутри полки

# Свежесть полки
FRESHNESS_LOOKBACK = 8          # сколько свечей перед полкой проверяем
OLD_IMPULSE_PCT = 2.0           # если старая свеча закрылась выше полки на 2% и более – полка несвежая
OLD_IMPULSE_RANGE_MULT = 2.5    # множитель среднего диапазона полки для старой свечи

# Расчет RVOL
RVOL_LOOKBACK = 20              # период для среднего объема

MIN_24H_VOLUME_USDT = 1_500_000 # минимальный суточный объём
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
                            "close": safe_float(k.get("close")),
                            "volume": safe_float(k.get("volume"))
                        })
                    elif isinstance(k, list) and len(k) >= 6:
                        parsed.append({
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
    params = {"category": "linear", "symbol": bybit_symbol, "intervalTime": "1h", "limit": 2}
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

                # Сортировка по времени для гарантии корректности
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
        
        # Визуальное оформление инфо-полей
        rvol_str = f"<b>{rvol:.2f}x</b> 🔥" if rvol >= 2.0 else f"<code>{rvol:.2f}x</code>"
        
        if oi_source in ["Н/Д", "Ошибка"]:
            oi_str = "<code>Н/Д</code>"
        elif oi_growth > 0:
            oi_str = f"<b>+{oi_growth:.2f}%</b> 🟢"
        else:
            oi_str = f"<code>{oi_growth:.2f}%</code> 🔴"

        vol_str = f"<b>x{vol_expansion:.1f} (Растёт)</b> ⚡" if vol_expansion >= 1.5 else f"<code>x{vol_expansion:.1f} (Норма)</code>"

        message = (
            f"🚀 <b>СВЕЖАЯ ПОЛКА + ПРОБОЙ</b>\n\n"
            f"Монета: <code>{coin}</code>\n"
            f"💥 Пробой: <b>+{breakout_pct:.2f}%</b>\n\n"
            f"📊 <b>Информационное поле:</b>\n"
            f"├ RVOL (Объем): {rvol_str}\n"
            f"├ Волатильность: {vol_str}\n"
            f"└ Изм. ОИ (1h): {oi_str}\n\n"
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

def is_fresh_shelf(candles, base_start, base_high, base_low, avg_base_range):
    start = max(0, base_start - FRESHNESS_LOOKBACK)
    previous = candles[start:base_start]
    if len(previous) < 3:
        return True

    for c in previous:
        if base_high > 0:
            old_breakout_pct = ((c["close"] - base_high) / base_high) * 100
            if old_breakout_pct >= OLD_IMPULSE_PCT:
                return False
        if avg_base_range > 0 and candle_range(c) >= avg_base_range * OLD_IMPULSE_RANGE_MULT:
            if c["close"] >= base_low:
                return False
    return True

async def check_symbol(session, bot, symbol, vol_24h, semaphore):
    now = time.time()
    if symbol in last_signals and (now - last_signals[symbol]) < ALERT_COOLDOWN_SECONDS:
        return False

    candles = await fetch_klines(session, symbol, semaphore)
    min_required = SHELF_MIN_CANDLES + FRESHNESS_LOOKBACK + max(EMA_PERIODS)
    if len(candles) < min_required:
        return False

    closed_candles = candles[:-1]
    current_candle = candles[-1]
    
    if len(closed_candles) < SHELF_MIN_CANDLES:
        return False

    # 1. Полка
    base_start = len(closed_candles) - SHELF_MIN_CANDLES
    shelf_candles = closed_candles[base_start:]
    base_high = max(c["high"] for c in shelf_candles)
    base_low = min(c["low"] for c in shelf_candles)
    if base_low <= 0:
        return False

    shelf_width_pct = ((base_high - base_low) / base_low) * 100
    if shelf_width_pct > SHELF_MAX_WIDTH_PCT:
        return False

    # 2. Пересечение EMA
    closes = [c["close"] for c in closed_candles]
    for period in EMA_PERIODS:
        ema_list = calculate_ema(closes, period)
        if not ema_list:
            return False
        ema_val = ema_list[-1]
        if not (base_low <= ema_val <= base_high):
            return False

    # 3. Свежесть полки
    avg_base_range = sum(candle_range(c) for c in shelf_candles) / len(shelf_candles)
    if not is_fresh_shelf(closed_candles, base_start, base_high, base_low, avg_base_range):
        return False

    # 4. Пробой
    current_price = current_candle["close"]
    breakout_pct = ((current_price - base_high) / base_high) * 100
    if breakout_pct < BREAKOUT_PCT:
        return False

    # === РАСЧЕТ ИНФО-ПОЛЕЙ (БЕЗ ФИЛЬТРАЦИИ) ===
    
    # RVOL (Расчет по закрытым свечам)
    lookback_candles = closed_candles[-RVOL_LOOKBACK:]
    avg_volume = sum(c["volume"] for c in lookback_candles) / len(lookback_candles) if lookback_candles else 0
    rvol = (current_candle["volume"] / avg_volume) if avg_volume > 0 else 1.0

    # Волатильность (Отношение текущего диапазона к средней полке)
    curr_range = candle_range(current_candle)
    vol_expansion = (curr_range / avg_base_range) if avg_base_range > 0 else 1.0

    # Открытый интерес
    oi_growth, oi_source = await get_oi_growth(session, symbol, semaphore)

    # Отправка сигнала
    last_signals[symbol] = now
    success = await send_signal(
        bot, symbol, breakout_pct, base_high, current_price,
        vol_24h, rvol, vol_expansion, oi_growth, oi_source
    )
    if success:
        logging.info(f"СИГНАЛ: {symbol} | Пробой: +{breakout_pct:.2f}% | RVOL: {rvol:.2f}x | OI: {oi_growth:.2f}%")
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
                        await asyncio.sleep(30)
                        break
                    tasks = [check_symbol(session, bot, sym, vol, semaphore) for sym, vol in symbols.items()]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    signals = sum(1 for r in results if r is True)
                    elapsed = time.time() - start_time
                    logging.info(f"Скан #{scan_counter} | {elapsed:.1f}с | Пар: {len(symbols)} | Сигналов: {signals}")
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
        await bot.send_message(chat_id=CHAT_ID, text="🤖 Бот «ПАРТИЗАН» запущен! (RVOL, OI, Волатильность как инфо)")
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
