import os
import logging
import asyncio
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", 8080))
BINGX_BASE = "https://open-api.bingx.com"

MIN_24H_VOLUME = 1_000_000  # Мин. объем 1M $
WATCH_USERS = set()         # ID пользователей для уведомлений
WATCHED_1M_PAIRS = set()    # Пары на 1m отслеживании

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

logging.basicConfig(level=logging.INFO)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

def calculate_ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for p in prices[period:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema

# --- ВАЛИДАЦИЯ И ФИЛЬТРАЦИЯ ТИКЕРОВ ---
async def get_tickers(session):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = str(item.get("symbol", "")).upper()
                
                # Исключаем не-USDT пары
                if not sym.endswith("-USDT"):
                    continue

                # Исключаем индексы, фонду, металлы и спец. контракты BingX (NC..., FOOTBALL, INDEX и т.д.)
                if sym.startswith(("NC", "INDEX")) or any(x in sym for x in ["FOOTBALL", "NASDAQ", "SP500", "DOWJONES", "XAU", "XAG", "_"]):
                    continue

                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME and price > 0:
                    result[sym] = {"volume": vol, "price": price}
            return result
    except Exception as e:
        logging.error(f"Tickers error: {e}")
        return {}

async def get_klines(session, symbol, interval, limit=100):
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("code") != 0 or not data.get("data"):
                return []
            
            candles = []
            for k in data["data"]:
                candles.append({
                    "open": safe_float(k.get("open")),
                    "high": safe_float(k.get("high")),
                    "low": safe_float(k.get("low")),
                    "close": safe_float(k.get("close")),
                    "volume": safe_float(k.get("volume"))
                })
            return candles[::-1]  # От старых к новым
    except Exception as e:
        logging.error(f"Klines error {symbol} ({interval}): {e}")
        return []

# --- АНАЛИЗАТОРЫ ---
def analyze_market_15m(candles):
    if len(candles) < 100:
        return None, {}

    closes = [c["close"] for c in candles]
    ema7 = calculate_ema(closes, 7)
    ema25 = calculate_ema(closes, 25)
    ema99 = calculate_ema(closes, 99)

    if not (ema7 and ema25 and ema99):
        return None, {}

    e7, e25, e99 = ema7[-1], ema25[-1], ema99[-1]
    last = candles[-1]
    
    avg_vol = sum(c["volume"] for c in candles[-21:-1]) / 20.0
    rvol = last["volume"] / avg_vol if avg_vol > 0 else 0.0

    c_range = last["high"] - last["low"]
    if c_range == 0:
        return None, {}

    upper_wick = last["high"] - max(last["open"], last["close"])
    wick_ratio = upper_wick / c_range

    # 1. ШОРТ (Сброс ММ)
    price_spread = (c_range / last["low"]) * 100
    if rvol >= 3.0 and wick_ratio >= 0.45 and price_spread >= 2.5 and last["close"] < last["open"]:
        return "MM_DISTRIBUTION", {"rvol": rvol, "wick": wick_ratio * 100, "price": last["close"]}

    # 2. ЛОНГ (Веер EMA)
    is_fan = (e7 > e25) and (e25 > e99)
    ema7_rising = len(ema7) > 1 and ema7[-1] > ema7[-2]
    dist_e7 = ((last["close"] - e7) / e7) * 100

    # Фильтр: Вход строго в начале движения (отклонение от EMA7 <= 1.2%)
    is_green_or_tight = last["close"] >= last["open"] or dist_e7 <= 0.8

    if is_fan and ema7_rising and rvol >= 1.8 and 0.1 <= dist_e7 <= 1.2 and wick_ratio < 0.25 and is_green_or_tight:
        return "BULLISH_IMPULSE", {"rvol": rvol, "e7": e7, "e25": e25, "e99": e99, "price": last["close"]}

    return None, {}

def analyze_market_1m(candles):
    if len(candles) < 5:
        return False, {}
    
    last = candles[-1]
    c_range = last["high"] - last["low"]
    if c_range == 0:
        return False, {}

    upper_wick = last["high"] - max(last["open"], last["close"])
    wick_ratio = upper_wick / c_range

    # Поиск фитиля сброса на 1m (>= 40% верхней тени)
    if wick_ratio >= 0.40:
        return True, {"wick": wick_ratio * 100, "price": last["close"]}
    
    return False, {}

# --- ФОНОВЫЕ ЦИКЛЫ ---
async def main_loop(session):
    while True:
        try:
            tickers = await get_tickers(session)
            for symbol in tickers:
                candles_15m = await get_klines(session, symbol, "15m", limit=100)
                sig_type, meta = analyze_market_15m(candles_15m)

                if sig_type:
                    clean_sym = symbol.replace("-USDT", "")
                    kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text=f"👁 Следить за {symbol} (1m)", callback_data=f"watch_1m:{symbol}")
                    ]])

                    if sig_type == "BULLISH_IMPULSE":
                        msg = (
                            f"🚀 **ПАРТИЗАН: ВЕЕР EMA 7/25/99 (LONG)**\n\n"
                            f"Монета: **{clean_sym}**\n"
                            f"📈 Бычий веер: EMA7 > EMA25 > EMA99\n"
                            f"📊 RVOL: **{meta['rvol']:.2f}x**\n"
                            f"💰 Цена: **{meta['price']:.6f}**\n\n"
                            f"🔗 [Открыть График](https://bingx.com/ru-ru/futures/forward/{clean_sym}USDT)"
                        )
                    else: # MM_DISTRIBUTION
                        msg = (
                            f"🩸 **ПАРТИЗАН: СБРОС ММ (SHORT)**\n\n"
                            f"Монета: **{clean_sym}**\n"
                            f"🎯 Верхняя тень: **{meta['wick']:.1f}%**\n"
                            f"📊 RVOL: **{meta['rvol']:.2f}x**\n"
                            f"💰 Цена: **{meta['price']:.6f}**\n\n"
                            f"🔗 [Открыть График](https://bingx.com/ru-ru/futures/forward/{clean_sym}USDT)"
                        )

                    for u_id in WATCH_USERS:
                        try:
                            await bot.send_message(u_id, msg, parse_mode="Markdown", reply_markup=kb, disable_web_page_preview=True)
                        except Exception:
                            pass

                await asyncio.sleep(0.1)
        except Exception as e:
            logging.error(f"Main loop error: {e}")
        
        await asyncio.sleep(60)

async def scan_watched_1m(session):
    while True:
        try:
            for symbol in list(WATCHED_1M_PAIRS):
                candles_1m = await get_klines(session, symbol, "1m", limit=10)
                triggered, meta = analyze_market_1m(candles_1m)

                if triggered:
                    clean_sym = symbol.replace("-USDT", "")
                    msg = (
                        f"⚡️ **1M ТОЧКА ВХОДА СБРОС ММ | {clean_sym}**\n\n"
                        f"🔻 Верхняя тень: **{meta['wick']:.1f}%**\n"
                        f"💰 Цена 1m: **{meta['price']:.6f}**\n"
                        f"🎯 Отличная реакция для точечного ШОРТа!\n\n"
                        f"🔗 [График](https://bingx.com/ru-ru/futures/forward/{clean_sym}USDT)"
                    )
                    for u_id in WATCH_USERS:
                        try:
                            await bot.send_message(u_id, msg, parse_mode="Markdown", disable_web_page_preview=True)
                        except Exception:
                            pass
                    
                    WATCHED_1M_PAIRS.remove(symbol)  # Одноразовое срабатывание

                await asyncio.sleep(0.1)
        except Exception as e:
            logging.error(f"1m scan error: {e}")
            
        await asyncio.sleep(5)

# --- AIOGRAM ХЕНДЛЕРЫ ---
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    WATCH_USERS.add(msg.from_user.id)
    await msg.answer("🤖 Бот запущен! Ожидание сигналов 15m (Партизан) и отслеживание волатильности...")

@dp.callback_query(lambda c: c.data and c.data.startswith("watch_1m:"))
async def process_watch_1m(cb: types.CallbackQuery):
    symbol = cb.data.split(":")[1]
    WATCHED_1M_PAIRS.add(symbol)
    await cb.answer(f"Запущено слежение 1m за {symbol}!", show_alert=False)
    await cb.message.answer(f"🎯 **{symbol}** зафиксирован. Бот ищет микро-фитиль сброса на 1m свечах!")

# --- HEALTH CHECK И СЕРВЕР ---
async def health(request):
    return web.Response(text="OK", status=200)

async def main():
    async with aiohttp.ClientSession() as session:
        app = web.Application()
        app.router.add_get("/", health)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", PORT).start()

        asyncio.create_task(main_loop(session))
        asyncio.create_task(scan_watched_1m(session))

        await bot.delete_webhook(drop_pending_updates=True)

        try:
            await dp.start_polling(bot)
        finally:
            await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
