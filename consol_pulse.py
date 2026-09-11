import asyncio
import os
import time
import logging

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ====================== CONFIG ======================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE = "https://open-api.bingx.com"
TIMEFRAME = "15m"
KLINE_LIMIT = 120

MIN_24H_VOLUME = 800_000
CHECK_INTERVAL = 20
ALERT_COOLDOWN = 2 * 3600

watched_coins = {}
last_signals = {}

# ====================== HELPERS ======================

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def calculate_ema(values, period):
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    ema = [sum(values[:period]) / period]
    for price in values[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema

# ====================== API ======================

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
                if not sym.endswith("-USDT") or any(x in sym for x in ["NCSK", "FOOTBALL", "INDEX", "NASDAQ", "_"]):
                    continue
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME and price > 0:
                    result[sym] = {"volume": vol, "price": price}
            return result
    except Exception as e:
        logging.error(f"Tickers error: {e}")
        return {}

async def get_klines(session, symbol, interval=TIMEFRAME, limit=KLINE_LIMIT):
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
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
                        "ts": int(safe_float(k.get("time") or k.get("timestamp"))),
                        "open": safe_float(k.get("open")),
                        "high": safe_float(k.get("high")),
                        "low": safe_float(k.get("low")),
                        "close": safe_float(k.get("close")),
                        "volume": safe_float(k.get("volume")),
                    })
            parsed.sort(key=lambda x: x["ts"])
            return parsed
    except Exception:
        return []

# ====================== LOGIC ======================

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

    # 1. ШОРТ (Разгрузка ММ): Нужен не просто фитиль, а свеча с размахом (рост минимум на 2.5% внутри свечи)
    price_spread = (c_range / last["low"]) * 100
    if rvol >= 3.0 and wick_ratio >= 0.45 and price_spread >= 2.5 and last["close"] < last["open"]:
        return "MM_DISTRIBUTION", {"rvol": rvol, "wick": wick_ratio * 100, "price": last["close"]}

    # 2. ЛОНГ (Веер EMA): Бычий веер + EMA7 должна расти (наклон вверх), а не заваливаться в боковике
    is_fan = (e7 > e25) and (e25 > e99)
    ema7_rising = len(ema7) > 1 and ema7[-1] > ema7[-2]
    dist_e7 = ((last["close"] - e7) / e7) * 100

    if is_fan and ema7_rising and rvol >= 1.8 and 0.2 <= dist_e7 <= 2.0 and wick_ratio < 0.25:
        return "BULLISH_IMPULSE", {"rvol": rvol, "e7": e7, "e25": e25, "e99": e99, "price": last["close"]}

    return None, {}

# ====================== TELEGRAM KEYBOARD ======================

def get_watch_keyboard(symbol):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"👁 Следить за {symbol} (1m)", callback_data=f"watch_{symbol}")
    ]])

# ====================== BOT INIT ======================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.callback_query(lambda c: c.data and c.data.startswith('watch_'))
async def process_watch(callback_query: CallbackQuery):
    symbol = callback_query.data.split('_')[1]
    watched_coins[symbol] = time.time()
    await callback_query.answer(text=f"👁 {symbol} переведен на 1m радар!")
    await bot.send_message(CHAT_ID, f"🎯 <b>{symbol}</b> зафиксирован. Бот ищет микро-фитиль сброса на 1m свечах!", parse_mode="HTML")

# ====================== SCANNERS ======================

async def scan_watched_1m(session):
    while True:
        now = time.time()
        for symbol, added_time in list(watched_coins.items()):
            if now - added_time > 3600:
                del watched_coins[symbol]
                continue

            candles = await get_klines(session, symbol, interval="1m", limit=20)
            if not candles:
                continue

            last = candles[-1]
            c_range = last["high"] - last["low"]
            if c_range == 0:
                continue

            upper_wick = last["high"] - max(last["open"], last["close"])
            wick_ratio = upper_wick / c_range

            if wick_ratio >= 0.40 and last["close"] < last["open"]:
                coin = symbol.split("-")[0]
                msg = (
                    f"⚡ <b>1M ТОЧКА ВХОДА СБРОС ММ | {coin}</b>\n\n"
                    f"🔻 Верхняя тень: <b>{wick_ratio*100:.1f}%</b>\n"
                    f"💰 Цена 1m: <code>{last['close']:.6f}</code>\n"
                    f"🎯 <i>Отличная реакция для точечного ШОРТа!</i>\n\n"
                    f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График</a>"
                )
                await bot.send_message(CHAT_ID, msg, parse_mode="HTML", disable_web_page_preview=True)
                del watched_coins[symbol]

        await asyncio.sleep(5)

async def main_loop(session):
    while True:
        try:
            tickers = await get_tickers(session)
            now = time.time()

            for symbol, data in tickers.items():
                if symbol in last_signals and now - last_signals[symbol] < ALERT_COOLDOWN:
                    continue

                candles = await get_klines(session, symbol, interval=TIMEFRAME)
                signal_type, meta = analyze_market_15m(candles)

                if signal_type:
                    coin = symbol.split("-")[0]
                    last_signals[symbol] = now

                    if signal_type == "MM_DISTRIBUTION":
                        msg = (
                            f"🚨 <b>ПАРТИЗАН: РАЗГРУЗКА ММ (SHORT)</b>\n\n"
                            f"Монета: <b>{coin}</b>\n"
                            f"📊 RVOL: <b>{meta['rvol']:.2f}x</b>\n"
                            f"🗡 Сбросовый фитиль: <b>{meta['wick']:.1f}%</b>\n"
                            f"💰 Цена: <code>{meta['price']:.6f}</code>\n"
                        )
                    else:
                        msg = (
                            f"🚀 <b>ПАРТИЗАН: ВЕЕР EMA 7/25/99 (LONG)</b>\n\n"
                            f"Монета: <b>{coin}</b>\n"
                            f"📈 Бычий веер: <b>EMA7 > EMA25 > EMA99</b>\n"
                            f"📊 RVOL: <b>{meta['rvol']:.2f}x</b>\n"
                            f"💰 Цена: <code>{meta['price']:.6f}</code>\n"
                        )

                    msg += f"\n🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть График</a>"
                    
                    await bot.send_message(
                        CHAT_ID, 
                        msg, 
                        parse_mode="HTML", 
                        reply_markup=get_watch_keyboard(symbol),
                        disable_web_page_preview=True
                    )

            logging.info(f"Scan done. Watched 1m coins: {len(watched_coins)}")
        except Exception as e:
            logging.error(f"Main loop error: {e}")

        await asyncio.sleep(CHECK_INTERVAL)

# ====================== WEB & MAIN SERVER ======================

async def health(request):
    return web.Response(text=f"Partizan Bot Active | Watched: {len(watched_coins)}")

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
