import os
import logging
import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from aiohttp import web

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PartisanBot")

# ============================================================
# CONFIG v4.3
# ============================================================
TIMEFRAME = "15m"
LIMIT_CANDLES = 120
CHECK_INTERVAL_SECONDS = 20

# Настройки полки (ПАРТИЗАН SQUEEZE)
SHELF_MIN_CANDLES = 6
SHELF_MAX_CANDLES = 18
MAX_SHELF_WIDTH_PCT = 2.5       # Жесткое зажатие (было 4.5%)
EMA_SHELF_TOLERANCE_PCT = 0.8   # Допуск цены около EMA
MAX_EMA_CLUSTER_PCT = 1.5       # EMA20/40/80 должны сливаться в нитку

# Настройки пробоя
MIN_BREAKOUT_PCT = 0.8         # Минимальный вылет цены
MAX_BREAKOUT_PCT = 3.5         # Максимальный вылет (без перегретых свечей)
MIN_RVOL_THRESHOLD = 1.5       # Планка относительного объёма
MIN_VOLUME_24H_USDT = 500_000   # Минимальный объем 24ч

# Повторы и кэш
REPEAT_ALERT_COOLDOWN_SEC = 1800  # 30 минут паузы на одну монету

# ============================================================
# ENV & GLOBAL STATE
# ============================================================
BOT_TOKEN = os.getenv("PUMP_BOT_TOKEN") or os.getenv("BOT_TOKEN") or ""
CHAT_ID = os.getenv("PUMP_CHAT_ID") or os.getenv("CHAT_ID") or ""
PORT = int(os.getenv("PORT", 10000))

session = None
sent_alerts = {}  # {symbol: timestamp}
stats = {"shelves": 0, "old_breakouts": 0, "late": 0}

async def get_session():
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()
    return session

# ============================================================
# TELEGRAM NOTIFIER
# ============================================================
async def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("Telegram token or chat_id not provided.")
        return False
    try:
        sess = await get_session()
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        async with sess.post(url, json=payload) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error("Ошибка отправки Telegram: %s", e)
        return False

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def base_symbol(symbol):
    return symbol.split("-")[0].replace("USDT", "")

def calculate_ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

# ============================================================
# API FETCHERS (BingX & Bybit OI)
# ============================================================
async def fetch_bingx_tickers():
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    sess = await get_session()
    async with sess.get(url) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data.get("data", [])
    return []

async def fetch_bingx_klines(symbol, timeframe=TIMEFRAME, limit=LIMIT_CANDLES):
    url = f"https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol={symbol}&interval={timeframe}&limit={limit}"
    sess = await get_session()
    try:
        async with sess.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw_klines = data.get("data", [])
                candles = []
                for k in raw_klines:
                    candles.append({
                        "time": int(k["time"]),
                        "open": float(k["open"]),
                        "high": float(k["high"]),
                        "low": float(k["low"]),
                        "close": float(k["close"]),
                        "volume": float(k["volume"])
                    })
                candles.sort(key=lambda x: x["time"])
                return candles
    except Exception as e:
        logger.error("Ошибка загрузки свечей %s: %s", symbol, e)
    return []

async def fetch_bybit_oi(symbol):
    bb_symbol = symbol.replace("-", "")
    url = f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={bb_symbol}&intervalTime=15min&limit=2"
    sess = await get_session()
    try:
        async with sess.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                list_data = data.get("result", {}).get("list", [])
                if len(list_data) >= 2:
                    current_oi = float(list_data[0]["openInterest"])
                    prev_oi = float(list_data[1]["openInterest"])
                    if prev_oi > 0:
                        pct = ((current_oi - prev_oi) / prev_oi) * 100
                        return {"current": current_oi, "change_pct": pct}
    except Exception:
        pass
    return None

# ============================================================
# CORE ANALYSIS LOGIC
# ============================================================
def analyze_shelf_and_breakout(candles):
    if len(candles) < 50:
        return None

    df = pd.DataFrame(candles)
    df["ema20"] = calculate_ema(df["close"], 20)
    df["ema40"] = calculate_ema(df["close"], 40)
    df["ema80"] = calculate_ema(df["close"], 80)

    # Текущая свеча (потенциальный пробой)
    curr = df.iloc[-1]
    
    # Ищем полку на истории перед текущей свечой
    shelf_found = None
    for length in range(SHELF_MIN_CANDLES, SHELF_MAX_CANDLES + 1):
        shelf_df = df.iloc[-(length + 1):-1]
        
        top = shelf_df["high"].max()
        bottom = shelf_df["low"].min()
        width_pct = ((top - bottom) / bottom) * 100

        if width_pct > MAX_SHELF_WIDTH_PCT:
            continue

        # Проверка структуры EMA (минимум 80% совпадения)
        in_ema20 = np.abs((shelf_df["close"] - shelf_df["ema20"]) / shelf_df["ema20"]) * 100 <= EMA_SHELF_TOLERANCE_PCT
        in_ema40 = np.abs((shelf_df["close"] - shelf_df["ema40"]) / shelf_df["ema40"]) * 100 <= EMA_SHELF_TOLERANCE_PCT
        in_ema80 = np.abs((shelf_df["close"] - shelf_df["ema80"]) / shelf_df["ema80"]) * 100 <= EMA_SHELF_TOLERANCE_PCT

        ema20_pct = (in_ema20.sum() / length) * 100
        ema40_pct = (in_ema40.sum() / length) * 100
        ema80_pct = (in_ema80.sum() / length) * 100

        if ema20_pct < 80 or ema40_pct < 80 or ema80_pct < 80:
            continue

        # Проверка сжатия кластера EMA
        last_ema20 = shelf_df["ema20"].iloc[-1]
        last_ema40 = shelf_df["ema40"].iloc[-1]
        last_ema80 = shelf_df["ema80"].iloc[-1]
        
        cluster_min = min(last_ema20, last_ema40, last_ema80)
        cluster_max = max(last_ema20, last_ema40, last_ema80)
        cluster_pct = ((cluster_max - cluster_min) / cluster_min) * 100

        if cluster_pct > MAX_EMA_CLUSTER_PCT:
            continue

        # Касания
        touch_threshold = top * 0.998
        interactions = (shelf_df["high"] >= touch_threshold).sum()
        bullish_reactions = ((shelf_df["close"] > shelf_df["open"]) & (shelf_df["high"] >= touch_threshold)).sum()

        shelf_found = {
            "length": length,
            "top": top,
            "bottom": bottom,
            "width_pct": width_pct,
            "ema": {
                "ema20_pct": ema20_pct,
                "ema40_pct": ema40_pct,
                "ema80_pct": ema80_pct,
                "cluster_pct": cluster_pct
            },
            "interaction": {
                "interactions": interactions,
                "bullish_reactions": bullish_reactions
            }
        }
        break

    if not shelf_found:
        return None

    # Анализ пробоя текущей свечи
    shelf_top = shelf_found["top"]
    breakout_pct = ((curr["close"] - shelf_top) / shelf_top) * 100

    if not (MIN_BREAKOUT_PCT <= breakout_pct <= MAX_BREAKOUT_PCT):
        return None

    # Фильтр верхушки (фитиль не более 1.2x от тела)
    body = abs(curr["close"] - curr["open"])
    upper_wick = curr["high"] - max(curr["close"], curr["open"])
    if body > 0 and (upper_wick / body) > 1.2:
        return None

    # RVOL и объем
    avg_vol = df["volume"].iloc[-21:-1].mean()
    rvol = curr["volume"] / avg_vol if avg_vol > 0 else 0

    if rvol < MIN_RVOL_THRESHOLD:
        return None

    volatility = ((curr["high"] - curr["low"]) / curr["low"]) * 100

    return {
        "shelf": shelf_found,
        "breakout_pct": breakout_pct,
        "price": curr["close"],
        "rvol": rvol,
        "volatility": volatility
    }

# ============================================================
# FORMAT SIGNAL FOR TELEGRAM
# ============================================================
def format_signal(signal):
    shelf = signal["shelf"]
    ema = shelf["ema"]
    interaction = shelf["interaction"]
    oi = signal["oi"]

    raw_ticker = base_symbol(signal["symbol"])

    if oi is None:
        oi_text = "Н/Д"
    else:
        ch = oi["change_pct"]
        oi_text = f"{ch:+.2f}% 🟢" if ch > 0 else (f"{ch:+.2f}% 🔴" if ch < 0 else "0.00%")

    return (
        f"<code>{raw_ticker}</code>\n\n"
        "🚀 ПАРТИЗАН — ПЕРВЫЙ ПРОБОЙ (v4.3)\n\n"
        f"Монета: {signal['symbol']}\n"
        f"ТФ: {TIMEFRAME}\n\n"
        f"⚡️ Пробой полки: +{signal['breakout_pct']:.2f}%\n"
        f"📦 Полка: {shelf['length']} свечей\n"
        f"📏 Ширина полки: {shelf['width_pct']:.2f}%\n\n"
        "📐 EMA-структура\n"
        f"├ EMA20 в полке: {ema['ema20_pct']:.0f}%\n"
        f"├ EMA40 в полке: {ema['ema40_pct']:.0f}%\n"
        f"├ EMA80 в полке: {ema['ema80_pct']:.0f}%\n"
        f"└ Разброс EMA: {ema['cluster_pct']:.2f}%\n\n"
        "🔄 Взаимодействие\n"
        f"├ Касаний: {interaction['interactions']}\n"
        f"└ Бычьих реакций: {interaction['bullish_reactions']}\n\n"
        "📊 Информация\n"
        f"├ RVOL: {signal['rvol']:.2f}x\n"
        f"├ Волатильность: {signal['volatility']:.2f}%\n"
        f"└ OI (Bybit): {oi_text}\n\n"
        "🔴 Уровни\n"
        f"├ Верх полки: {shelf['top']:.8f}\n"
        f"├ Цена: {signal['price']:.8f}\n"
        f"└ Честный объём 24ч: ${signal['volume24h']:,.0f}\n"
    )

# ============================================================
# MAIN SCANNER LOOP
# ============================================================
async def process_scan():
    tickers = await fetch_bingx_tickers()
    if not tickers:
        return

    now = datetime.now(timezone.utc).timestamp()
    
    # Фильтруем парами к USDT с достаточным объемом
    valid_symbols = []
    for t in tickers:
        symbol = t.get("symbol", "")
        vol = float(t.get("quoteVolume", 0))
        if symbol.endswith("-USDT") and vol >= MIN_VOLUME_24H_USDT:
            valid_symbols.append((symbol, vol))

    count_shelves = 0
    count_signals = 0

    for symbol, vol24h in valid_symbols:
        # Проверка кулдауна
        if symbol in sent_alerts and (now - sent_alerts[symbol]) < REPEAT_ALERT_COOLDOWN_SEC:
            continue

        candles = await fetch_bingx_klines(symbol)
        if not candles:
            continue

        res = analyze_shelf_and_breakout(candles)
        if res:
            count_shelves += 1
            oi = await fetch_bybit_oi(symbol)
            res["symbol"] = symbol
            res["volume24h"] = vol24h
            res["oi"] = oi

            msg = format_signal(res)
            sent = await send_telegram(msg)
            if sent:
                sent_alerts[symbol] = now
                count_signals += 1

    logger.info(f"Скан завершен | Найдено полок: {count_shelves} | Отправлено сигналов: {count_signals}")

# ============================================================
# DUMMY WEB SERVER FOR RENDER
# ============================================================
async def handle_ping(request):
    return web.Response(text="OK")

async def start_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Веб-сервер запущен на порту %s", PORT)

# ============================================================
# ENTRY POINT
# ============================================================
async def main():
    logger.info("🚀 ПАРТИЗАН EMA TEST v4.3 (Запуск)")
    await start_server()

    # Отбивка в Telegram о запуске
    await send_telegram("🟢 <b>ПАРТИЗАН v4.3 запущен и ведет мониторинг рынка!</b>")

    while True:
        try:
            await process_scan()
        except Exception as e:
            logger.exception("Ошибка главного цикла: %s", e)
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
