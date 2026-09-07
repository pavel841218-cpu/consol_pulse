import os
import time
import asyncio
import logging

import aiohttp
from aiohttp import web


# ============================================================
#              ПАРТИЗАН — EMA / SHELF TEST v4.2 (STRICT VOL)
# ============================================================

BOT_TOKEN = (
    os.environ.get("PUMP_BOT_TOKEN")
    or os.environ.get("BOT_TOKEN")
    or "YOUR_TELEGRAM_BOT_TOKEN"
)

CHAT_ID = (
    os.environ.get("PUMP_CHAT_ID")
    or os.environ.get("CHAT_ID")
    or "YOUR_TELEGRAM_CHAT_ID"
)

PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"
BYBIT_BASE_URL = "https://api.bybit.com"

# ============================================================
# CONFIG / TIMEFRAME
# ============================================================

TIMEFRAME = "15m"
KLINE_LIMIT = 240

# SHELF CONFIG
SHELF_MIN_CANDLES = 6
SHELF_MAX_CANDLES = 18
MAX_SHELF_WIDTH_PCT = 4.5

# EMA CONFIG
EMA_FAST = 20
EMA_MID = 40
EMA_SLOW = 80

EMA_SHELF_TOLERANCE_PCT = 1.0
MAX_EMA_CLUSTER_PCT = 4.0

EMA_PRICE_DISTANCE_PCT = 0.8
MIN_EMA_INTERACTIONS = 2
REACTION_LOOKBACK = 5

# BREAKOUT CONFIG
MIN_BREAKOUT_PCT = 1.0       # Повышено до 1.0%, чтобы убрать микро-пробои (как 0.64% на APEX)
MAX_BREAKOUT_PCT = 8.0       # Максимальный вылет внутри 15m свечи

# VOLUME & LIQUIDITY CONFIG
MIN_24H_VOLUME_USDT = 800_000
MIN_RVOL_THRESHOLD = 1.3     # ЖЕСТКИЙ ФИЛЬТР: RVOL на пробое должен быть >= 1.3x!
RVOL_SPIKE_THRESHOLD = 3.5

RVOL_LOOKBACK = 20
CHECK_INTERVAL_SECONDS = 30
MAX_CONCURRENT_REQUESTS = 10
SESSION_MAX_AGE = 1800
ALERT_COOLDOWN_SECONDS = 3 * 3600

# BLACKLIST
BLACKLIST = {"USDC", "FDUSD"}
BAD_PREFIXES = {"NCS", "SP500", "INDEX"}
BAD_CONTAINS = {"_", "FOOTBALL", "INDEX", "STKFQ", "NCFX", "NCCO"}
LEVERAGED_SUFFIXES = ("2L", "2S", "3L", "3S", "5L", "5S", "10L", "10S")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PARTIZAN")

session = None
session_created_at = 0
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
last_alert_time = {}

stats = {
    "scans": 0, "pairs": 0, "shelves": 0, "signals": 0,
    "old_breakouts": 0, "late": 0, "errors": 0,
}

async def get_session():
    global session, session_created_at
    now = time.time()
    if session is None or session.closed or now - session_created_at > SESSION_MAX_AGE:
        if session is not None and not session.closed:
            await session.close()
        timeout = aiohttp.ClientTimeout(total=15)
        connector = aiohttp.TCPConnector(limit=50, ssl=False)
        session = aiohttp.ClientSession(
            timeout=timeout, connector=connector, headers={"User-Agent": "PARTIZAN-EMA-TEST/4.2"}
        )
        session_created_at = now
    return session

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default

def normalize_symbol(symbol):
    symbol = str(symbol).upper()
    if symbol.endswith("-USDT"): return symbol
    if symbol.endswith("USDT"): return symbol[:-4] + "-USDT"
    return symbol

def base_symbol(symbol):
    return normalize_symbol(symbol).replace("-USDT", "")

def is_valid_symbol(symbol):
    s = base_symbol(symbol)
    if not s or s in BLACKLIST: return False
    for prefix in BAD_PREFIXES:
        if s.startswith(prefix): return False
    for bad in BAD_CONTAINS:
        if bad in s: return False
    for suffix in LEVERAGED_SUFFIXES:
        if s.endswith(suffix): return False
    return True

def calculate_ema(values, period):
    if not values or len(values) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = []
    initial = sum(values[:period]) / period
    ema.append(initial)
    previous = initial
    for price in values[period:]:
        current = (price - previous) * multiplier + previous
        ema.append(current)
        previous = current
    result = [None] * (period - 1)
    result.extend(ema)
    return result

def parse_kline(k):
    if isinstance(k, dict):
        return {
            "time": int(safe_float(k.get("time") or k.get("timestamp") or k.get("t"))),
            "open": safe_float(k.get("open") or k.get("o")),
            "high": safe_float(k.get("high") or k.get("h")),
            "low": safe_float(k.get("low") or k.get("l")),
            "close": safe_float(k.get("close") or k.get("c")),
            "volume": safe_float(k.get("volume") or k.get("v")),
        }
    return {
        "time": int(safe_float(k[0])),
        "open": safe_float(k[1]),
        "high": safe_float(k[2]),
        "low": safe_float(k[3]),
        "close": safe_float(k[4]),
        "volume": safe_float(k[5]),
    }

async def bingx_get(path, params=None):
    url = BINGX_BASE_URL + path
    for attempt in range(3):
        try:
            sess = await get_session()
            async with semaphore:
                async with sess.get(url, params=params or {}) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return await resp.json()
        except Exception as e:
            if attempt == 2:
                logger.debug("BingX error %s: %s", path, e)
            await asyncio.sleep(0.5 * (attempt + 1))
    return None

async def fetch_contracts():
    data = await bingx_get("/openApi/swap/v2/quote/contracts")
    if not data: return []
    raw = data.get("data", [])
    result = []
    for item in raw:
        symbol = item.get("symbol") or item.get("contractName") if isinstance(item, dict) else item
        if not symbol: continue
        symbol = normalize_symbol(symbol)
        if symbol.endswith("-USDT") and is_valid_symbol(symbol):
            result.append(symbol)
    return sorted(set(result))

async def fetch_klines(symbol):
    data = await bingx_get(
        "/openApi/swap/v3/quote/klines",
        {"symbol": symbol, "interval": TIMEFRAME, "limit": KLINE_LIMIT},
    )
    if not data: return []
    raw = data.get("data", [])
    if not isinstance(raw, list): return []
    candles = []
    for k in raw:
        try:
            candle = parse_kline(k)
            if candle["open"] > 0 and candle["close"] > 0:
                candles.append(candle)
        except Exception:
            continue
    candles.sort(key=lambda x: x["time"])
    return candles

def body_high(candle): return max(candle["open"], candle["close"])
def body_low(candle): return min(candle["open"], candle["close"])

def calculate_shelf(candles):
    if not candles: return None
    highs = [body_high(c) for c in candles]
    lows = [body_low(c) for c in candles]
    top = max(highs)
    bottom = min(lows)
    if bottom <= 0: return None
    width_pct = ((top - bottom) / bottom) * 100
    if width_pct > MAX_SHELF_WIDTH_PCT: return None
    return {"top": top, "bottom": bottom, "width_pct": width_pct}

def ema_inside_shelf(ema, shelf):
    if ema is None: return False
    top, bottom = shelf["top"], shelf["bottom"]
    tolerance = ema * (EMA_SHELF_TOLERANCE_PCT / 100)
    return (bottom - tolerance) <= ema <= (top + tolerance)

def analyze_ema_structure(shelf_candles, ema20, ema40, ema80):
    shelf = calculate_shelf(shelf_candles)
    if not shelf: return None
    checks = {"ema20": 0, "ema40": 0, "ema80": 0}
    valid_count = 0
    cluster_values = []

    for i in range(len(shelf_candles)):
        if i >= len(ema20) or i >= len(ema40) or i >= len(ema80): continue
        e20, e40, e80 = ema20[i], ema40[i], ema80[i]
        if e20 is None or e40 is None or e80 is None: continue

        valid_count += 1
        if ema_inside_shelf(e20, shelf): checks["ema20"] += 1
        if ema_inside_shelf(e40, shelf): checks["ema40"] += 1
        if ema_inside_shelf(e80, shelf): checks["ema80"] += 1
        cluster_values.extend([e20, e40, e80])

    if valid_count == 0: return None
    ema20_pct = (checks["ema20"] / valid_count) * 100
    ema40_pct = (checks["ema40"] / valid_count) * 100
    ema80_pct = (checks["ema80"] / valid_count) * 100

    if ema20_pct < 60 or ema40_pct < 60 or ema80_pct < 60: return None

    if cluster_values:
        c_high, c_low = max(cluster_values), min(cluster_values)
        mid = (c_high + c_low) / 2
        if mid <= 0: return None
        cluster_pct = ((c_high - c_low) / mid) * 100
        if cluster_pct > MAX_EMA_CLUSTER_PCT: return None
    else: cluster_pct = 0

    return {
        "ema20_pct": ema20_pct, "ema40_pct": ema40_pct, "ema80_pct": ema80_pct,
        "average_pct": (ema20_pct + ema40_pct + ema80_pct) / 3,
        "cluster_pct": cluster_pct,
    }

def check_price_ema_interaction(shelf_candles, ema20, ema40, ema80):
    interactions = 0
    interaction_details = []
    for i, candle in enumerate(shelf_candles):
        if i >= len(ema20): continue
        e20, e40, e80 = ema20[i], ema40[i], ema80[i]
        if e20 is None or e40 is None or e80 is None: continue

        b_hi, b_lo = body_high(candle), body_low(candle)
        close, open_price = candle["close"], candle["open"]
        touched = []

        for name, ema in [("EMA20", e20), ("EMA40", e40), ("EMA80", e80)]:
            dist = (abs(close - ema) / ema) * 100
            if (b_lo <= ema <= b_hi) or (dist <= EMA_PRICE_DISTANCE_PCT):
                touched.append(name)

        if touched:
            interactions += 1
            interaction_details.append({"index": i, "emas": touched, "bullish": close > open_price})

    if interactions < MIN_EMA_INTERACTIONS: return None
    recent = interaction_details[-REACTION_LOOKBACK:]
    if sum(1 for x in recent if x["bullish"]) < 1: return None

    return {"interactions": interactions, "bullish_reactions": sum(1 for x in recent if x["bullish"])}

def find_shelf(candles, ema20, ema40, ema80):
    closed = candles[:-1]
    if len(closed) < SHELF_MAX_CANDLES: return None

    for length in range(SHELF_MAX_CANDLES, SHELF_MIN_CANDLES - 1, -1):
        start = len(closed) - length
        end = len(closed)
        shelf_candles = closed[start:end]

        shelf = calculate_shelf(shelf_candles)
        if not shelf: continue

        ema_struct = analyze_ema_structure(shelf_candles, ema20[start:end], ema40[start:end], ema80[start:end])
        if not ema_struct: continue

        interact = check_price_ema_interaction(shelf_candles, ema20[start:end], ema40[start:end], ema80[start:end])
        if not interact: continue

        return {
            "start": start, "end": end, "length": length,
            "top": shelf["top"], "bottom": shelf["bottom"], "width_pct": shelf["width_pct"],
            "ema": ema_struct, "interaction": interact,
        }
    return None

def check_breakout(candles, shelf):
    if len(candles) < 2: return None
    current = candles[-1]
    prev_candle = candles[-2]
    shelf_top = shelf["top"]
    close = current["close"]
    open_p = current["open"]
    high = current["high"]

    if close <= shelf_top: return None
    breakout_pct = ((close - shelf_top) / shelf_top) * 100

    if breakout_pct < MIN_BREAKOUT_PCT: return None
    if breakout_pct > MAX_BREAKOUT_PCT: return "LATE"

    # ФИЛЬТР ФИТИЛЯ: Верхняя тень не должна превосходить тело свечи более чем в 1.2 раза
    body_size = abs(close - open_p)
    upper_wick = high - max(close, open_p)
    if body_size > 0 and upper_wick > body_size * 1.2:
        return None

    # ФИЛЬТР ОБЪЕМА: На текущей свече объем должен быть больше, чем на предыдущей
    if current["volume"] <= prev_candle["volume"]:
        return None

    previous = candles[:-1]
    check_count = min(5, len(previous))
    for candle in previous[-check_count:]:
        if ((body_high(candle) - shelf_top) / shelf_top) * 100 >= MIN_BREAKOUT_PCT:
            return "OLD"

    return {"breakout_pct": breakout_pct, "price": close}

def calculate_rvol(candles):
    if len(candles) < RVOL_LOOKBACK + 1: return 0.0
    current = candles[-1]
    history = candles[-(RVOL_LOOKBACK + 1):-1]
    vols = [c["volume"] for c in history if c["volume"] > 0]
    if not vols: return 0.0
    avg = sum(vols) / len(vols)
    return (current["volume"] / avg) if avg > 0 else 0.0

def calculate_volatility(candle):
    return (abs(candle["close"] - candle["open"]) / candle["open"] * 100) if candle["open"] > 0 else 0.0

async def fetch_oi(symbol):
    try:
        clean = symbol.replace("-USDT", "")
        url = BYBIT_BASE_URL + "/v5/market/open-interest"
        sess = await get_session()
        params = {"category": "linear", "symbol": clean + "USDT", "intervalTime": "15min", "limit": "2"}
        async with semaphore:
            async with sess.get(url, params=params) as resp:
                if resp.status != 200: return None
                data = await resp.json()
        res = data.get("result", {}).get("list", [])
        if len(res) < 2: return None
        cur = safe_float(res[0].get("openInterest"))
        prev = safe_float(res[1].get("openInterest"))
        if prev <= 0: return None
        return {"current": cur, "change_pct": ((cur - prev) / prev) * 100}
    except Exception: return None

# ============================================================
# MAIN ANALYZER
# ============================================================

async def analyze_symbol(symbol):
    try:
        candles = await fetch_klines(symbol)

        if len(candles) < 150:
            return None

        # Расчет Rolling 24h Volume
        last_96 = candles[-96:] if len(candles) >= 96 else candles
        rolling_24h_volume = sum(c["volume"] * c["close"] for c in last_96)

        rvol = calculate_rvol(candles)

        # 1. Жесткая проверка RVOL: Объем пробойной свечи должен быть выше среднего!
        if rvol < MIN_RVOL_THRESHOLD:
            return None

        # 2. Фильтр базового суточного объема
        if rolling_24h_volume < MIN_24H_VOLUME_USDT and rvol < RVOL_SPIKE_THRESHOLD:
            return None

        closes = [c["close"] for c in candles]
        ema20 = calculate_ema(closes, EMA_FAST)
        ema40 = calculate_ema(closes, EMA_MID)
        ema80 = calculate_ema(closes, EMA_SLOW)

        if not ema80: return None

        shelf = find_shelf(candles, ema20, ema40, ema80)
        if not shelf: return None

        stats["shelves"] += 1

        breakout = check_breakout(candles, shelf)
        if breakout == "OLD":
            stats["old_breakouts"] += 1
            return None
        if breakout == "LATE":
            stats["late"] += 1
            return None
        if not breakout: return None

        volatility = calculate_volatility(candles[-1])
        oi = await fetch_oi(symbol)

        now = time.time()
        if now - last_alert_time.get(symbol, 0) < ALERT_COOLDOWN_SECONDS:
            return None

        last_alert_time[symbol] = now
        stats["signals"] += 1

        return {
            "symbol": symbol,
            "price": breakout["price"],
            "breakout_pct": breakout["breakout_pct"],
            "shelf": shelf,
            "rvol": rvol,
            "volatility": volatility,
            "oi": oi,
            "volume24h": rolling_24h_volume,
        }

    except Exception as e:
        stats["errors"] += 1
        logger.debug("%s: %s", symbol, e)
        return None

async def send_telegram(text):
    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_") or not CHAT_ID or CHAT_ID.startswith("YOUR_"):
        return False
    try:
        sess = await get_session()
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}
        async with sess.post(url, json=payload) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error("Telegram error: %s", e)
        return False

def format_signal(signal):
    shelf = signal["shelf"]
    ema = shelf["ema"]
    interaction = shelf["interaction"]
    oi = signal["oi"]

    if oi is None: oi_text = "Н/Д"
    else:
        ch = oi["change_pct"]
        oi_text = f"{ch:+.2f}% 🟢" if ch > 0 else (f"{ch:+.2f}% 🔴" if ch < 0 else "0.00%")

    return (
        "🚀 ПАРТИЗАН — ПЕРВЫЙ ПРОБОЙ (v4.2)\n\n"
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

async def process_scan():
    started = time.time()
    contracts = await fetch_contracts()
    if not contracts: return

    stats["pairs"] = len(contracts)
    signals = []

    async def worker(symbol):
        res = await analyze_symbol(symbol)
        if res: signals.append(res)

    tasks = [worker(s) for s in contracts]
    batch_size = MAX_CONCURRENT_REQUESTS

    for i in range(0, len(tasks), batch_size):
        await asyncio.gather(*tasks[i:i + batch_size], return_exceptions=True)

    elapsed = time.time() - started
    stats["scans"] += 1

    for sig in signals:
        await send_telegram(format_signal(sig))

    logger.info(
        "🔎 Скан #%d | %.1fs | Пар: %d | Полок: %d | Сигналов: %d",
        stats["scans"], elapsed, len(contracts), stats["shelves"], len(signals)
    )

async def health(request):
    return web.Response(text=f"PARTIZAN EMA TEST v4.2 OK\nScans: {stats['scans']}\nSignals: {stats['signals']}\n")

async def start_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

async def main():
    logger.info("🚀 ПАРТИЗАН EMA TEST v4.2 (Запуск)")
    await start_server()
    while True:
        try:
            stats["shelves"], stats["old_breakouts"], stats["late"] = 0, 0, 0
            await process_scan()
        except Exception as e:
            logger.exception("Ошибка глав-цикла: %s", e)
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
