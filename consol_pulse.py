import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN") or os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("PUMP_CHAT_ID") or os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"
MEXC_BASE_URL = "https://contract.mexc.com"


# ============================================================
# КОНТЕКСТ 1H — EMA 20/40/80
# ============================================================

EMA_1H_FAST = 20
EMA_1H_MID = 40
EMA_1H_SLOW = 80
KLINES_1H_LIMIT = 120

SHELF_MIN_HOURS = 6
SHELF_MAX_WIDTH_PCT = 6.0
EMA_COMPRESSION_MAX_PCT = 3.5


# ============================================================
# ДЕТЕКТОР 5M (было 15m)
# ============================================================

KLINES_5M_LIMIT = 60

# Пробой полки 1H
MIN_BREAKOUT_FROM_SHELF_PCT = 1.0     # минимум 1.0% над верхом полки
MAX_BREAKOUT_FROM_SHELF_PCT = 3.5     # ✅ anti-late: не > 3.5%

# Свеча 5m
MIN_5M_CHANGE_PCT = 0.8               # было 2.0 для 15m
MIN_5M_VOLUME_MULT = 3.0              # было 3.5
MAX_5M_VOLUME_MULT = 5.5              # ✅ RVOL cap — климакс отсекаем
MIN_5M_BODY_RATIO = 0.55
MIN_5M_CLOSE_STRENGTH = 0.72          # было 0.70

# ✅ Anti-late: за 3 предыдущие 5m свечи (15 мин) не должно быть > 2% роста
MAX_PRIOR_MOVE_PCT = 2.0

# RSI
USE_RSI_FILTER = True
MIN_RSI_6 = 55.0
MAX_RSI_6 = 85.0                      # ✅ не входим в перекупленность


# ============================================================
# OI (MEXC)
# ============================================================

USE_OI_FILTER = True
MIN_OI_GROWTH_PCT = 0.15
MEXC_OI_CACHE_TTL = 300


# ============================================================
# ФИЛЬТР ЦЕНЫ И СИМВОЛОВ
# ============================================================

MIN_24H_VOLUME_USDT = 1_000_000
MIN_PRICE_FOR_SCAN = 0.001
MAX_PRICE_FOR_SCAN = 5.0

# ✅ Усиленный фильтр — убираем NCC, газ, нефть
EXCLUDE_KEYWORDS = [
    "_", "FOOTBALL", "INDEX", "STKFQ", "XAUT", "PAXG",
    "USDC", "USDT_", "GOLD", "OIL", "SILVER",
    "NCC", "NATGAS", "WTI", "BRENT", "GAS", "COPPER",
    "PLATINUM", "ALUMINUM",
]


# ============================================================
# СКАНЕР + WATCH
# ============================================================

CONTEXT_REFRESH_SECONDS = 1800
IMPULSE_SCAN_INTERVAL = 10

ALERT_COOLDOWN_SECONDS = 2 * 3600
MAX_SIGNALS_PER_HOUR = 8

# ✅ Watch
WATCH_TTL_SECONDS = 2 * 3600          # автозакрытие через 2 часа
WATCH_REMINDER_AFTER = 2 * 3600       # напоминание через 2 часа
WATCH_CHECK_INTERVAL = 60             # проверяем каждую минуту

SL_OFFSET = 0.998
TP1_RR = 1.5
TP2_RR = 3.0

# Состояния
last_signals = {}
shelf_cache = {}
signals_this_hour = []
scan_counter = 0
impulse_scan_counter = 0
_mexc_oi_cache = {}

# ✅ Формат: {symbol: {"added_at": ts, "direction": "LONG", "reminded": False}}
watched_coins = {}

reject_stats = {
    "no_breakout": 0, "too_late_breakout": 0,
    "no_change": 0, "too_late_prior": 0,
    "no_volume": 0, "volume_climax": 0,
    "no_body": 0, "no_close": 0,
    "no_rsi": 0, "rsi_overbought": 0,
    "no_oi": 0, "oi_no_data": 0,
}


# ============================================================
# HELPERS
# ============================================================

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def format_price(p):
    if p is None or p == 0:
        return "0.00"
    if p >= 1000:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.6f}"
    return f"{p:.8f}"


def parse_kline(k):
    try:
        if isinstance(k, dict):
            return {
                "time": int(k.get("time", 0)),
                "open": safe_float(k.get("open")),
                "high": safe_float(k.get("high")),
                "low": safe_float(k.get("low")),
                "close": safe_float(k.get("close")),
                "volume": safe_float(k.get("volume")),
            }
        if isinstance(k, (list, tuple)) and len(k) >= 6:
            return {
                "time": int(k[0]),
                "open": safe_float(k[1]),
                "high": safe_float(k[2]),
                "low": safe_float(k[3]),
                "close": safe_float(k[4]),
                "volume": safe_float(k[5]),
            }
    except Exception:
        pass
    return None


def calculate_ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema


def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def candle_range(c):
    return c["high"] - c["low"]


def body_ratio(c):
    r = candle_range(c)
    return abs(c["close"] - c["open"]) / r if r > 0 else 0.0


def close_strength(c):
    r = candle_range(c)
    return (c["close"] - c["low"]) / r if r > 0 else 0.0


def cleanup():
    now = time.time()
    expired = [s for s, t in last_signals.items() if now - t > ALERT_COOLDOWN_SECONDS]
    for s in expired:
        del last_signals[s]

    global signals_this_hour
    signals_this_hour = [t for t in signals_this_hour if now - t < 3600]

    # Чистим OI-кэш
    expired_oi = [
        sym for sym, data in _mexc_oi_cache.items()
        if now - data.get("timestamp", 0) > MEXC_OI_CACHE_TTL
    ]
    for s in expired_oi:
        del _mexc_oi_cache[s]


# ============================================================
# WEB
# ============================================================

async def health_check(request):
    return web.Response(
        text=f"Shelf Breakout 5m | watch={len(watched_coins)} | oi_cache={len(_mexc_oi_cache)}",
        status=200
    )


# ============================================================
# BINGX API
# ============================================================

async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with session.get(url, headers=headers,
                              timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT"):
                    continue
                if any(kw in sym.upper() for kw in EXCLUDE_KEYWORDS):
                    continue
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if price < MIN_PRICE_FOR_SCAN or price > MAX_PRICE_FOR_SCAN:
                    continue
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = {"volume": vol, "price": price}
            return result
    except Exception as e:
        logging.error(f"Ошибка тикеров BingX: {e}")
        return {}


async def fetch_klines(session, symbol, interval, limit, semaphore):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    async with semaphore:
        try:
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=6)
            ) as resp:
                data = await resp.json()
                raw = data.get("data", [])
                if not isinstance(raw, list):
                    return []
                parsed = []
                for k in raw:
                    c = parse_kline(k)
                    if c and c["time"] > 0:
                        parsed.append(c)
                parsed.sort(key=lambda x: x["time"])
                return parsed
        except Exception:
            return []


# ============================================================
# MEXC OI
# ============================================================

def to_mexc_symbol(symbol):
    return symbol.replace("-", "_").upper()


async def get_mexc_oi_delta(session, symbol, semaphore):
    mexc_symbol = to_mexc_symbol(symbol)
    url = f"{MEXC_BASE_URL}/api/v1/contract/ticker"
    params = {"symbol": mexc_symbol}
    headers = {"User-Agent": "Mozilla/5.0"}

    async with semaphore:
        try:
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return 0.0, "MEXC-no-http"
                data = await resp.json()
                if not data.get("success"):
                    return 0.0, "MEXC-no-success"
                ticker = data.get("data", {})
                curr_hold = safe_float(ticker.get("holdVol"))
                if curr_hold <= 0:
                    return 0.0, "MEXC-no-oi"

                now = time.time()
                prev = _mexc_oi_cache.get(symbol)
                _mexc_oi_cache[symbol] = {"holdVol": curr_hold, "timestamp": now}

                if prev and prev.get("holdVol", 0) > 0:
                    if now - prev.get("timestamp", 0) <= MEXC_OI_CACHE_TTL:
                        growth = ((curr_hold - prev["holdVol"]) / prev["holdVol"]) * 100
                        return growth, "MEXC"

                return 0.0, "MEXC-cold"
        except Exception as e:
            logging.debug(f"MEXC OI err {symbol}: {e}")
            return 0.0, "MEXC-err"


# ============================================================
# КОНТЕКСТ 1H
# ============================================================

def analyze_1h_context(candles_1h):
    if len(candles_1h) < EMA_1H_SLOW + 5:
        return None
    closed = candles_1h[:-1]
    shelf = closed[-SHELF_MIN_HOURS:]
    if len(shelf) < SHELF_MIN_HOURS:
        return None

    base_high = max(c["high"] for c in shelf)
    base_low = min(c["low"] for c in shelf)
    if base_low <= 0:
        return None

    shelf_width = ((base_high - base_low) / base_low) * 100
    if shelf_width > SHELF_MAX_WIDTH_PCT:
        return None

    closes = [c["close"] for c in closed]
    ef_list = calculate_ema(closes, EMA_1H_FAST)
    em_list = calculate_ema(closes, EMA_1H_MID)
    es_list = calculate_ema(closes, EMA_1H_SLOW)
    if not ef_list or not em_list or not es_list:
        return None

    ef, em, es = ef_list[-1], em_list[-1], es_list[-1]

    if not (base_low <= ef <= base_high):
        return None
    if not (base_low <= em <= base_high):
        return None
    if es < base_low * 0.92 or es > base_high:
        return None

    compression = ((ef - es) / es) * 100
    if compression > EMA_COMPRESSION_MAX_PCT:
        return None

    return {
        "shelf_high": base_high,
        "shelf_low": base_low,
        "shelf_width_pct": round(shelf_width, 2),
        "ema_compression": round(compression, 2),
        "updated_at": time.time(),
    }


async def build_shelf_cache(session, symbols, semaphore):
    logging.info(f"🔍 Обновление 1H-контекста для {len(symbols)} пар...")
    start = time.time()

    tasks = [fetch_klines(session, sym, "1h", KLINES_1H_LIMIT, semaphore) for sym in symbols]
    all_klines = await asyncio.gather(*tasks, return_exceptions=True)

    new_cache = {}
    for sym, klines in zip(symbols, all_klines):
        if not isinstance(klines, list) or len(klines) < EMA_1H_SLOW + 5:
            continue
        ctx = analyze_1h_context(klines)
        if ctx:
            new_cache[sym] = ctx

    logging.info(f"✅ 1H-контекст: {len(new_cache)} полок из {len(symbols)} за {time.time()-start:.1f}с")
    return new_cache


# ============================================================
# ДЕТЕКТОР 5M
# ============================================================

def detect_impulse_5m(candles_5m, shelf_ctx):
    """
    Работает по LIVE 5m свече (candles_5m[-1]).
    Сигнал отправляется если live свеча уже показывает сильный импульс.
    """
    if len(candles_5m) < 25:
        return None

    current = candles_5m[-1]
    history = candles_5m[-20:-1]

    if current["open"] <= 0 or current["close"] <= 0:
        return None

    base_high = shelf_ctx["shelf_high"]
    base_low = shelf_ctx["shelf_low"]

    # 1. Пробой полки
    breakout_pct = ((current["close"] - base_high) / base_high) * 100
    if breakout_pct < MIN_BREAKOUT_FROM_SHELF_PCT:
        reject_stats["no_breakout"] += 1
        return None

    # ✅ Anti-late: пробой не слишком большой
    if breakout_pct > MAX_BREAKOUT_FROM_SHELF_PCT:
        reject_stats["too_late_breakout"] += 1
        return None

    # 2. Изменение свечи 5m
    change_pct = ((current["close"] - current["open"]) / current["open"]) * 100
    if change_pct < MIN_5M_CHANGE_PCT:
        reject_stats["no_change"] += 1
        return None

    # ✅ Anti-late: предыдущие 3 свечи 5m не должны быть в большом росте
    prior = candles_5m[-4:-1]
    if len(prior) == 3 and prior[0]["open"] > 0:
        prior_move = ((prior[-1]["close"] - prior[0]["open"]) / prior[0]["open"]) * 100
        if prior_move > MAX_PRIOR_MOVE_PCT:
            reject_stats["too_late_prior"] += 1
            return None

    # 3. Объём
    avg_vol = sum(c["volume"] for c in history) / len(history)
    if avg_vol <= 0:
        return None
    vol_mult = current["volume"] / avg_vol
    if vol_mult < MIN_5M_VOLUME_MULT:
        reject_stats["no_volume"] += 1
        return None
    # ✅ RVOL cap — климакс
    if vol_mult > MAX_5M_VOLUME_MULT:
        reject_stats["volume_climax"] += 1
        return None

    # 4. Тело
    if body_ratio(current) < MIN_5M_BODY_RATIO:
        reject_stats["no_body"] += 1
        return None

    # 5. Close strength
    if close_strength(current) < MIN_5M_CLOSE_STRENGTH:
        reject_stats["no_close"] += 1
        return None

    # 6. RSI
    rsi6 = 0
    if USE_RSI_FILTER:
        closes = [c["close"] for c in candles_5m]
        rsi6 = calculate_rsi(closes, 6)
        if rsi6 < MIN_RSI_6:
            reject_stats["no_rsi"] += 1
            return None
        if rsi6 > MAX_RSI_6:
            reject_stats["rsi_overbought"] += 1
            return None

    # 7. SL/TP
    stop_loss = base_low * SL_OFFSET
    risk_pct = ((current["close"] - stop_loss) / current["close"]) * 100
    if risk_pct <= 0:
        return None

    tp1 = current["close"] * (1 + risk_pct * TP1_RR / 100)
    tp2 = current["close"] * (1 + risk_pct * TP2_RR / 100)

    return {
        "current_price": current["close"],
        "change_pct": round(change_pct, 2),
        "breakout_pct": round(breakout_pct, 2),
        "volume_mult": round(vol_mult, 2),
        "body_ratio": round(body_ratio(current), 2),
        "close_strength": round(close_strength(current), 2),
        "rsi6": round(rsi6, 1),
        "current_volume_usdt": int(current["volume"] * current["close"]),
        "shelf_high": base_high,
        "shelf_low": base_low,
        "shelf_width_pct": shelf_ctx["shelf_width_pct"],
        "ema_compression": shelf_ctx["ema_compression"],
        "stop_loss": stop_loss,
        "risk_pct": risk_pct,
        "tp1": tp1,
        "tp2": tp2,
    }


# ============================================================
# TELEGRAM
# ============================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def get_watch_keyboard(symbol, direction):
    """Кнопки для сигнала — следить / пропустить."""
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text=f"👁 Следить за {symbol.split('-')[0]} (1m)",
                callback_data=f"watch|{symbol}|{direction}"
            )
        ]]
    )


def get_unwatch_keyboard(symbol):
    """Кнопка отмены слежения."""
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="❌ Прекратить слежение",
                callback_data=f"unwatch|{symbol}"
            )
        ]]
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("watch|"))
async def process_watch(callback_query: CallbackQuery):
    try:
        _, symbol, direction = callback_query.data.split("|")
        now = time.time()

        watched_coins[symbol] = {
            "added_at": now,
            "direction": direction,
            "reminded": False,
        }
        await callback_query.answer(f"👁 {symbol} → 1M {direction}")

        # Отправляем подтверждение с кнопкой отмены
        await bot.send_message(
            chat_id=callback_query.message.chat.id,
            text=(
                f"🎯 <b>{symbol.split('-')[0]}</b>\n"
                f"Режим 1m: <b>{direction}</b>\n"
                f"Автозакрытие через 2 часа.\n\n"
                f"Бот ждёт микрооткат и реакцию."
            ),
            parse_mode="HTML",
            reply_markup=get_unwatch_keyboard(symbol)
        )
        logging.info(f"👁 Added to watch: {symbol} ({direction})")
    except Exception as e:
        logging.error(f"Watch cb error: {e}")


@dp.callback_query(lambda c: c.data and c.data.startswith("unwatch|"))
async def process_unwatch(callback_query: CallbackQuery):
    try:
        _, symbol = callback_query.data.split("|")
        if symbol in watched_coins:
            del watched_coins[symbol]
            await callback_query.answer("Прекращено")
            await bot.send_message(
                chat_id=callback_query.message.chat.id,
                text=f"❌ <b>{symbol.split('-')[0]}</b> снят с наблюдения",
                parse_mode="HTML"
            )
            logging.info(f"❌ Removed from watch: {symbol}")
        else:
            await callback_query.answer("Уже не в наблюдении")
    except Exception as e:
        logging.error(f"Unwatch cb error: {e}")


# ============================================================
# ALERTS
# ============================================================

async def send_alert(symbol, sig, oi_growth, oi_source):
    try:
        coin = symbol.split("-")[0].upper()

        oi_line = ""
        if USE_OI_FILTER:
            if oi_source == "MEXC-cold":
                oi_line = "📊 OI MEXC: <i>накапливается</i>\n"
            elif oi_source.startswith("MEXC-no") or oi_source == "MEXC-err":
                oi_line = "📊 OI MEXC: <i>нет данных</i>\n"
            else:
                arrow = "📈" if oi_growth > 0 else "📉"
                oi_line = f"{arrow} OI MEXC: <b>{oi_growth:+.2f}%</b>\n"

        message = (
            f"⚡️ <b>ИМПУЛЬС 5M (полка 1H + EMA 20/40/80)</b>\n\n"
            f"🪙 Монета: <code>{coin}</code>\n\n"
            f"💥 Пробой полки: <b>+{sig['breakout_pct']:.2f}%</b>\n"
            f"📈 Свеча 5m: <b>+{sig['change_pct']:.2f}%</b>\n"
            f"🔥 Объём: <b>x{sig['volume_mult']}</b>\n"
            f"💪 Body: <b>{sig['body_ratio']}</b> | Close: <b>{sig['close_strength']}</b>\n"
            f"📉 RSI6: <b>{sig['rsi6']}</b>\n"
            f"{oi_line}\n"
            f"📦 <b>КОНТЕКСТ 1H:</b>\n"
            f"├ Полка: <b>{sig['shelf_width_pct']}%</b>\n"
            f"├ Верх/низ: <code>{format_price(sig['shelf_high'])}</code> / "
            f"<code>{format_price(sig['shelf_low'])}</code>\n"
            f"└ EMA сжатие: <b>{sig['ema_compression']}%</b>\n\n"
            f"💰 <b>Ориентиры:</b>\n"
            f"├ Вход: <code>{format_price(sig['current_price'])}</code>\n"
            f"├ Стоп: <code>{format_price(sig['stop_loss'])}</code> "
            f"(риск {sig['risk_pct']:.2f}%)\n"
            f"├ TP1: <code>{format_price(sig['tp1'])}</code> "
            f"(+{sig['risk_pct'] * TP1_RR:.2f}%)\n"
            f"└ TP2: <code>{format_price(sig['tp2'])}</code> "
            f"(+{sig['risk_pct'] * TP2_RR:.2f}%)\n\n"
            f"💵 Объём свечи: <b>${sig['current_volume_usdt']:,}</b>\n\n"
            f"⚠️ <i>Решение о входе — за вами</i>\n"
            f"🕒 {datetime.now().strftime('%H:%M:%S')}\n"
            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть BingX</a>"
        )

        await bot.send_message(
            chat_id=CHAT_ID,
            text=message,
            parse_mode="HTML",
            reply_markup=get_watch_keyboard(symbol, "LONG"),
            disable_web_page_preview=True
        )
        return True
    except Exception as e:
        logging.error(f"Send alert error {symbol}: {e}")
        return False


# ============================================================
# CHECK SYMBOL
# ============================================================

async def check_symbol(session, symbol, shelf_ctx, semaphore):
    now = time.time()
    if symbol in last_signals and now - last_signals[symbol] < ALERT_COOLDOWN_SECONDS:
        return False
    if len(signals_this_hour) >= MAX_SIGNALS_PER_HOUR:
        return False

    candles = await fetch_klines(session, symbol, "5m", KLINES_5M_LIMIT, semaphore)
    if len(candles) < 25:
        return False

    sig = detect_impulse_5m(candles, shelf_ctx)
    if not sig:
        return False

    oi_growth = 0.0
    oi_source = "MEXC-off"

    if USE_OI_FILTER:
        oi_growth, oi_source = await get_mexc_oi_delta(session, symbol, semaphore)

        if oi_source in ("MEXC-no-success", "MEXC-no-oi"):
            reject_stats["oi_no_data"] += 1
            return False
        if oi_source == "MEXC-cold":
            return False
        if oi_growth < MIN_OI_GROWTH_PCT:
            reject_stats["no_oi"] += 1
            return False

    success = await send_alert(symbol, sig, oi_growth, oi_source)
    if success:
        last_signals[symbol] = now
        signals_this_hour.append(now)
        shelf_cache.pop(symbol, None)
        logging.info(
            f"🚀 АЛЕРТ {symbol} | +{sig['breakout_pct']:.2f}% | "
            f"5m +{sig['change_pct']:.2f}% | x{sig['volume_mult']} | "
            f"RSI6={sig['rsi6']} | OI {oi_growth:+.3f}%"
        )
    return success


# ============================================================
# WATCHDOG: автоудаление watch + напоминания
# ============================================================

async def watch_watchdog():
    """
    Каждые 60 сек:
    - Удаляет из watched_coins те, что старше WATCH_TTL_SECONDS
    - Напоминает через WATCH_REMINDER_AFTER (один раз)
    """
    while True:
        try:
            now = time.time()
            to_delete = []

            for symbol, info in list(watched_coins.items()):
                age = now - info["added_at"]

                # Напоминание через 2 часа
                if age >= WATCH_REMINDER_AFTER and not info.get("reminded", False):
                    info["reminded"] = True
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(
                                f"⏰ <b>НАПОМИНАНИЕ</b>\n\n"
                                f"🪙 <b>{symbol.split('-')[0]}</b> всё ещё в наблюдении\n"
                                f"Направление: <b>{info['direction']}</b>\n"
                                f"Прошло: <b>{int(age / 60)} мин</b>\n\n"
                                f"Нажми, чтобы прекратить:"
                            ),
                            parse_mode="HTML",
                            reply_markup=get_unwatch_keyboard(symbol)
                        )
                        logging.info(f"⏰ Reminder sent for {symbol}")
                    except Exception as e:
                        logging.error(f"Reminder error: {e}")

                # Автоудаление через TTL
                if age >= WATCH_TTL_SECONDS:
                    to_delete.append(symbol)

            for symbol in to_delete:
                del watched_coins[symbol]
                logging.info(f"🗑 Автоудалён из watch: {symbol} (TTL)")
                try:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=f"🗑 <b>{symbol.split('-')[0]}</b> — автозакрытие по TTL (2 часа)",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

        except Exception as e:
            logging.error(f"Watchdog error: {e}")

        await asyncio.sleep(WATCH_CHECK_INTERVAL)


# ============================================================
# SCANNER LOOP
# ============================================================

async def scanner_loop(session):
    global scan_counter, impulse_scan_counter, shelf_cache

    semaphore = asyncio.Semaphore(5)
    last_context_refresh = 0

    while True:
        try:
            if time.time() - last_context_refresh > CONTEXT_REFRESH_SECONDS:
                scan_counter += 1
                symbols_dict = await fetch_bingx_symbols(session)
                if not symbols_dict:
                    await asyncio.sleep(30)
                    continue
                symbols = list(symbols_dict.keys())
                shelf_cache = await build_shelf_cache(session, symbols, semaphore)
                last_context_refresh = time.time()
                logging.info(f"🔄 Контекст #{scan_counter}: {len(shelf_cache)} полок")

            if not shelf_cache:
                await asyncio.sleep(IMPULSE_SCAN_INTERVAL)
                continue

            impulse_scan_counter += 1
            start = time.time()

            tasks = [check_symbol(session, sym, ctx, semaphore)
                     for sym, ctx in list(shelf_cache.items())]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            signals = sum(1 for r in results if r is True)
            elapsed = time.time() - start

            if impulse_scan_counter % 30 == 0 or signals > 0:
                logging.info(
                    f"⚡️ Скан #{impulse_scan_counter} | {elapsed:.1f}с | "
                    f"Полок: {len(shelf_cache)} | Алертов: {signals} | "
                    f"OI: {len(_mexc_oi_cache)} | Watch: {len(watched_coins)}"
                )

            if impulse_scan_counter % 60 == 0:
                logging.info(
                    f"📊 ОТСЕВ: breakout={reject_stats['no_breakout']} | "
                    f"late_brk={reject_stats['too_late_breakout']} | "
                    f"change={reject_stats['no_change']} | "
                    f"late_prior={reject_stats['too_late_prior']} | "
                    f"vol={reject_stats['no_volume']} | "
                    f"climax={reject_stats['volume_climax']} | "
                    f"body={reject_stats['no_body']} | "
                    f"close={reject_stats['no_close']} | "
                    f"rsi={reject_stats['no_rsi']} | "
                    f"rsi_ob={reject_stats['rsi_overbought']} | "
                    f"oi={reject_stats['no_oi']} | "
                    f"oi_nd={reject_stats['oi_no_data']}"
                )

            if impulse_scan_counter % 200 == 0:
                cleanup()

            await asyncio.sleep(IMPULSE_SCAN_INTERVAL)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Scanner error: {e}")
            await asyncio.sleep(10)


# ============================================================
# MAIN
# ============================================================

async def main():
    if not BOT_TOKEN or not CHAT_ID:
        logging.error("Установите PUMP_BOT_TOKEN и PUMP_CHAT_ID")
        return

    # Стираем pending updates на старте
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # Сбрасываем watch — состояние не персистится
    watched_coins.clear()
    logging.info("🧹 Watch list очищен при старте")

    connector = aiohttp.TCPConnector(limit=15, ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=connector)

    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 HTTP на порту {PORT}")

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(
                "⚡️ <b>SHELF BREAKOUT 5M запущен</b>\n\n"
                "🎯 Контекст: 1H полки + EMA 20/40/80\n"
                "💥 Детектор: LIVE 5m свеча + пробой\n"
                "🛡 Anti-late: RVOL cap + prior-move фильтр\n"
                "📊 OI: MEXC holdVol delta\n"
                "💰 Цена: 0.001 — 5.0 USDT\n"
                "👁 Watch: TTL 2 часа + напоминание"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Startup msg error: {e}")

    asyncio.create_task(scanner_loop(session))
    asyncio.create_task(watch_watchdog())

    try:
        await dp.start_polling(bot, handle_signals=False)
    finally:
        await session.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
