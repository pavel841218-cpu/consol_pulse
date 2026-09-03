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

# ===== НАСТРОЙКИ СЖАТИЯ EMA И БАЗЫ =====
MAX_EMA_SPREAD_PCT = 2.5            # Мин. сжатие EMA 20/40/80 (до 2.5%)
MAX_BODY_SPREAD_PCT = 7.5           # Разброс цен закрытия Close в базе (до 7.5%)
MAX_PRE_BREAKOUT_CANDLE_VOL = 4.5   # Волатильность 1H свечи перед пробоем (до 4.5%)
MIN_24H_VOLUME_USDT = 800_000       # Мин. объем за 24 часа ($800K)

# ===== ФИЛЬТРЫ ЗАПОЗДАНИЯ (ЗАЩИТА ОТ ВХОДА НА НАХАХ) =====
MAX_DIST_FROM_EMA20_PCT = 3.0       # Макс. оторванность цены от EMA20 (не более 3%)
MAX_1H_CANDLE_GROWTH_PCT = 4.0      # Макс. рост текущей 1H свечи (не более 4%)

# ===== НАСТРОЙКИ ПРОБОЯ НА 5M =====
MIN_5M_BREAKOUT_PCT = 0.8           # Пробой хая базы на 5M от +0.8%
MIN_5M_VOLATILITY_PCT = 1.6         # Импульс 5M свечи от 1.6%

CHECK_INTERVAL_SECONDS = 30     
ALERT_COOLDOWN_SECONDS = 3 * 3600   # Кулдаун на повторный сигнал — 3 часа
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

def calculate_ema_single(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = (price * k) + (ema * (1 - k))
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
    return web.Response(text="Partizan Bot (Anti-Late Entry) Active", status=200)

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
                if not sym.endswith("-USDT") or any(x in sym for x in ["_", "FOOTBALL", "INDEX", "STKFQ"]):
                    continue
                
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = vol
            return result
    except Exception as e:
        logging.error(f"Ошибка получения тикеров BingX: {e}")
        return {}

async def fetch_klines(session, symbol, interval, limit, semaphore):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
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
                            "close": safe_float(k.get("close"))
                        })
                    elif isinstance(k, list) and len(k) >= 5:
                        parsed.append({
                            "open": safe_float(k[1]),
                            "high": safe_float(k[2]),
                            "low": safe_float(k[3]),
                            "close": safe_float(k[4])
                        })
                return parsed
        except Exception:
            return []

async def get_oi_growth(session, symbol, semaphore):
    bybit_symbol = symbol.replace("-", "").upper()
    url = "https://api.bybit.com/v5/market/open-interest"
    params = {"category": "linear", "symbol": bybit_symbol, "intervalTime": "1h", "limit": 3}
    headers = {"User-Agent": "Mozilla/5.0"}

    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    list_data = data.get("result", {}).get("list", [])
                    if len(list_data) >= 2:
                        curr_oi = safe_float(list_data[0].get("openInterest"))
                        prev_oi = safe_float(list_data[1].get("openInterest"))
                        if prev_oi > 0:
                            growth = ((curr_oi - prev_oi) / prev_oi) * 100
                            return growth, "Bybit"
        except Exception:
            pass
        return 0.0, "None"

def check_1h_ema_squeeze(candles_1h):
    """
    Анализ 1H с фильтром задержки:
    - Сжатие EMA20/40/80
    - Фильтр отрыва цены от EMA20 (до 3.0%)
    - Ограничение роста текущей 1H свечи (до 4.0%)
    """
    if len(candles_1h) < 90:
        return False, 0, 0, 0

    closes = [c["close"] for c in candles_1h[:-1]] # Только закрытые 1H свечи
    current_candle = candles_1h[-1]                # Текущая не закрытая 1H свеча
    current_price = current_candle["close"]

    # 1. Расчет EMA 20, 40, 80 по закрытым свечам
    ema20 = calculate_ema_single(closes, 20)
    ema40 = calculate_ema_single(closes, 40)
    ema80 = calculate_ema_single(closes, 80)

    ema_max = max(ema20, ema40, ema80)
    ema_min = min(ema20, ema40, ema80)
    if ema_min <= 0:
        return False, 0, 0, 0

    ema_spread_pct = ((ema_max - ema_min) / ema_min) * 100
    if ema_spread_pct > MAX_EMA_SPREAD_PCT:
        return False, 0, 0, 0

    # 2. ФИЛЬТР ЗАПОЗДАНИЯ №1: Отрыв цены от EMA20
    distance_from_ema20 = ((current_price - ema20) / ema20) * 100
    if distance_from_ema20 > MAX_DIST_FROM_EMA20_PCT or distance_from_ema20 < -1.0:
        return False, 0, 0, 0

    # 3. ФИЛЬТР ЗАПОЗДАНИЯ №2: Размер формирующейся 1H свечи
    candle_growth = ((current_price - current_candle["open"]) / current_candle["open"]) * 100
    if candle_growth > MAX_1H_CANDLE_GROWTH_PCT:
        return False, 0, 0, 0

    # 4. Плотность цен закрытия (Close) за 6 часов до текущей свечи
    recent_closes = closes[-6:]
    max_close = max(recent_closes)
    min_close = min(recent_closes)
    if min_close <= 0:
        return False, 0, 0, 0

    body_spread_pct = ((max_close - min_close) / min_close) * 100
    if body_spread_pct > MAX_BODY_SPREAD_PCT:
        return False, 0, 0, 0

    # 5. Затухание волатильности на последней закрытой 1H свече перед стартом
    last_closed_candle = candles_1h[-2]
    l_high = last_closed_candle["high"]
    l_low = last_closed_candle["low"]
    if l_low > 0:
        last_candle_vol = ((l_high - l_low) / l_low) * 100
        if last_candle_vol > MAX_PRE_BREAKOUT_CANDLE_VOL:
            return False, 0, 0, 0

    # Зафиксированный хай базы накопления (до импульса)
    flat_candles = candles_1h[-8:-2]
    base_high = max(c["high"] for c in flat_candles)
    base_low = min(c["low"] for c in flat_candles)

    return True, base_high, base_low, ema_spread_pct

async def send_signal(bot, symbol, volatility_5m, breakout_pct, oi_growth_pct, base_high, current_price, ema_spread_pct, vol_24h, oi_source):
    try:
        clean_coin = symbol.split("-")[0].upper()
        
        if oi_source == "None":
            oi_status = "<code>Н/Д</code> ⚠️"
        elif oi_growth_pct >= 1.0:
            oi_status = f"<b>+{oi_growth_pct:.2f}%</b> ({oi_source})"
        else:
            oi_status = f"<code>{oi_growth_pct:.2f}%</code> ⚠️ ({oi_source})"

        message = (
            f"🚀 <b>СИГНАЛ НА ВЗЛЕТ: {clean_coin}</b>\n\n"
            f"💥 Пробой базы 1H: <b>+{breakout_pct:.2f}%</b>\n"
            f"⚡ Сжатие EMA20/40/80: <b>{ema_spread_pct:.2f}%</b>\n"
            f"📊 Импульс 5M свечи: <b>{volatility_5m:.2f}%</b>\n\n"
            f"├ Верх полки (1H): <code>{format_price(base_high)}</code>\n"
            f"├ Текущая цена: <code>{format_price(current_price)}</code>\n"
            f"├ Изменение ОИ (1H): {oi_status}\n"
            f"└ Объем 24h: <b>${vol_24h/1_000_000:.2f}M</b>\n\n"
            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График BingX</a>"
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

    # 1. Проверка поджатия EMA и базы на 1H
    candles_1h = await fetch_klines(session, symbol, "1h", 100, semaphore)
    has_squeeze, base_high, base_low, ema_spread_pct = check_1h_ema_squeeze(candles_1h)

    if not has_squeeze:
        return False

    # 2. Проверка импульсного движения на 5M
    candles_5m = await fetch_klines(session, symbol, "5m", 6, semaphore)
    if len(candles_5m) < 2:
        return False

    current_5m = candles_5m[-1]
    c_open, c_close = current_5m["open"], current_5m["close"]
    c_high, c_low = current_5m["high"], current_5m["low"]

    volatility_5m = ((c_high - c_low) / c_low) * 100 if c_low > 0 else 0
    if volatility_5m < MIN_5M_VOLATILITY_PCT:
        return False

    # 3. Фильтр пробоя верхней границы
    breakout_pct = ((c_close - base_high) / base_high) * 100
    if breakout_pct < MIN_5M_BREAKOUT_PCT or c_close <= c_open:
        return False

    oi_growth_pct, oi_source = await get_oi_growth(session, symbol, semaphore)

    last_signals[symbol] = now
    success = await send_signal(
        bot, symbol, volatility_5m, breakout_pct, oi_growth_pct,
        base_high, c_close, ema_spread_pct, vol_24h, oi_source
    )
    
    if success:
        logging.info(f"СИГНАЛ ОТПРАВЛЕН: {symbol} | Пробой: +{breakout_pct:.2f}% | EMA Спред: {ema_spread_pct:.2f}%")
    
    return success

async def scanner_loop(bot):
    global scan_counter
    semaphore = asyncio.Semaphore(15)
    
    while True:
        try:
            session_start_time = time.time()
            connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300, force_close=False)
            
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
                    logging.info(f"Скан #{scan_counter} | {elapsed:.1f}с | Проверено пар: {len(symbols_dict)} | Сигналов: {total_signals}")
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
        await bot.send_message(chat_id=CHAT_ID, text="🤖 Бот «ПАРТИЗАН» запущен (Фильтр от запозданий включен)!")
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
