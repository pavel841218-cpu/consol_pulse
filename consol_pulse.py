import asyncio
import os
import time
import logging
from collections import deque

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", os.environ.get("TELEGRAM_BOT_TOKEN", ""))
CHAT_ID = os.environ.get("CHAT_ID", os.environ.get("TELEGRAM_CHAT_ID", ""))
PORT = int(os.environ.get("PORT", "10000"))

BINGX_BASE = "https://open-api.bingx.com"
TIMEFRAME = "15m"
KLINE_LIMIT = 150
CHECK_INTERVAL = 20
MIN_24H_VOLUME = 800_000
ALERT_COOLDOWN = 2 * 3600

# ============================================================
# EMA CONFIG
# ============================================================

EMA_FAST = 7
EMA_MID = 25
EMA_SLOW = 99

MIN_EMA_SPREAD_PCT = 1.0        # УСИЛЕНО: было 0.30 (веер должен быть реально раскрыт)
MIN_RVOL = 2.5                  # УСИЛЕНО: было 1.80 (объём должен быть значимым)
LOCAL_LOOKBACK = 5

# Anti-late-entry: если до импульса уже было сильное движение — пропускаем
MAX_PRIOR_MOVE_PCT = 3.0

# Риск-менеджмент
SL_OFFSET = 0.998               # Стоп = min(low) * 0.998
TP1_RR = 1.5                    # TP1 = 1.5R
TP2_RR = 3.0                    # TP2 = 3.0R

# ============================================================
# 1M RADAR CONFIG
# ============================================================

WATCH_TIME = 1200               # УМЕНЬШЕНО: 20 минут вместо 60
ONE_MIN_LIMIT = 30
M1_IMPULSE_MIN = 0.50           # УСИЛЕНО: было 0.25
M1_PULLBACK_MAX = 1.5
M1_WICK_MIN = 0.35              # УСИЛЕНО: было 0.25
M1_REACTION_MIN = 0.30          # УСИЛЕНО: было 0.10
M1_POLL_INTERVAL = 5

# Автоматический радар без клика (True — ставим сразу)
AUTO_WATCH = True
AUTO_WATCH_DURATION = 1200      # 20 минут

# ============================================================
# 4H CONTEXT (глобальный тренд)
# ============================================================

USE_4H_FILTER = True
EMA_4H_PERIOD = 50
MAX_4H_PENALTY_PCT = 5.0        # Допустимое отклонение от EMA50 (4h)

# ============================================================
# SYMBOL FILTER
# ============================================================

BAD_PARTS = {"NCSK", "FOOTBALL", "INDEX", "STKFQ", "_"}
BAD_SYMBOLS = {
    "USDC-USDT", "FDUSD-USDT", "USD1-USDT", "USDE-USDT",
    "TUSD-USDT", "IRIS-USDT", "IRYS-USDT", "LUNC-USDT", "USTC-USDT"
}

# ============================================================
# GLOBALS
# ============================================================

watched_coins = {}
pending_reactions = {}
last_signals = {}

stats = {
    "scans": 0, "symbols": 0, "signals": 0,
    "long": 0, "short": 0,
    "m1_checks": 0, "m1_entries": 0, "errors": 0,
    "rejected_late": 0, "rejected_rvol": 0, "rejected_spread": 0,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("PARTIZAN_EMA")

# ============================================================
# HELPERS
# ============================================================

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def symbol_base(symbol):
    return symbol.replace("-USDT", "")


def is_bad_symbol(symbol):
    if symbol in BAD_SYMBOLS:
        return True
    return any(bad in symbol for bad in BAD_PARTS)


def calculate_ema(values, period):
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    ema = [sum(values[:period]) / period]
    for price in values[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema


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


# ============================================================
# API / KLINES
# ============================================================

async def get_tickers(session):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with session.get(
            url, headers=headers,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                symbol = str(item.get("symbol", "")).upper()
                if not symbol.endswith("-USDT") or is_bad_symbol(symbol):
                    continue
                volume = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if volume >= MIN_24H_VOLUME and price > 0:
                    result[symbol] = {"volume": volume, "price": price}
            return result
    except Exception as e:
        logger.error("Tickers error: %s", e)
        stats["errors"] += 1
        return {}


async def get_klines(session, symbol, interval=TIMEFRAME, limit=KLINE_LIMIT):
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with session.get(
            url, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            data = await resp.json()
            rows = data.get("data", [])
            if not isinstance(rows, list):
                return []
            parsed = []
            for k in rows:
                if isinstance(k, dict):
                    ts = k.get("time") or k.get("timestamp")
                    parsed.append({
                        "ts": int(safe_float(ts)),
                        "open": safe_float(k.get("open")),
                        "high": safe_float(k.get("high")),
                        "low": safe_float(k.get("low")),
                        "close": safe_float(k.get("close")),
                        "volume": safe_float(k.get("volume")),
                    })
                elif isinstance(k, (list, tuple)) and len(k) >= 6:
                    parsed.append({
                        "ts": int(safe_float(k[0])),
                        "open": safe_float(k[1]),
                        "high": safe_float(k[2]),
                        "low": safe_float(k[3]),
                        "close": safe_float(k[4]),
                        "volume": safe_float(k[5]),
                    })
            parsed.sort(key=lambda x: x["ts"])
            return parsed
    except Exception:
        stats["errors"] += 1
        return []


async def check_4h_context(session, symbol, direction):
    """
    Проверяет, не противоречит ли направление 4h тренду.
    LONG: цена не должна быть ниже EMA50(4h) более чем на MAX_4H_PENALTY_PCT
    SHORT: цена не должна быть выше EMA50(4h) более чем на MAX_4H_PENALTY_PCT
    """
    if not USE_4H_FILTER:
        return True

    candles = await get_klines(session, symbol, interval="4h", limit=EMA_4H_PERIOD + 10)
    if len(candles) < EMA_4H_PERIOD + 2:
        return True  # если нет данных — пропускаем

    closed = candles[:-1]
    closes = [c["close"] for c in closed]
    ema50 = calculate_ema(closes, EMA_4H_PERIOD)
    if not ema50:
        return True

    current_price = closed[-1]["close"]
    ema4h = ema50[-1]
    if ema4h <= 0:
        return True

    deviation = ((current_price - ema4h) / ema4h) * 100

    if direction == "LONG" and deviation < -MAX_4H_PENALTY_PCT:
        return False  # цена сильно ниже EMA50 4h — не лонг
    if direction == "SHORT" and deviation > MAX_4H_PENALTY_PCT:
        return False  # цена сильно выше EMA50 4h — не шорт

    return True


# ============================================================
# ANALYSIS (15M STRATEGY)
# ============================================================

def analyze_market_15m(candles):
    if len(candles) < 110:
        return None, {}

    closed = candles[:-1]
    if len(closed) < 105:
        return None, {}

    closes = [c["close"] for c in closed]
    ema7_list = calculate_ema(closes, EMA_FAST)
    ema25_list = calculate_ema(closes, EMA_MID)
    ema99_list = calculate_ema(closes, EMA_SLOW)

    if not (ema7_list and ema25_list and ema99_list):
        return None, {}

    e7 = ema7_list[-1]
    e25 = ema25_list[-1]
    e99 = ema99_list[-1]

    last = closed[-1]
    prev_candles = closed[-LOCAL_LOOKBACK - 1:-1]
    if not prev_candles:
        return None, {}

    # --- RVOL ---
    avg_vol = sum(c["volume"] for c in closed[-21:-1]) / 20.0
    rvol = last["volume"] / avg_vol if avg_vol > 0 else 0.0
    if rvol < MIN_RVOL:
        stats["rejected_rvol"] += 1
        return None, {}

    # --- Качество свечи ---
    candle_range = last["high"] - last["low"]
    if candle_range <= 0:
        return None, {}

    # --- Anti-late-entry ---
    prior_candles = closed[-4:-1]
    prior_move = 0.0
    if prior_candles:
        prior_first = prior_candles[0]["close"]
        prior_last = prior_candles[-1]["close"]
        if prior_first > 0:
            prior_move = ((prior_last - prior_first) / prior_first) * 100
        if abs(prior_move) > MAX_PRIOR_MOVE_PCT:
            stats["rejected_late"] += 1
            return None, {}

    # ==================== LONG ====================
    if e7 > e25 > e99:
        spread = ((e7 - e99) / e99) * 100
        if spread < MIN_EMA_SPREAD_PCT:
            stats["rejected_spread"] += 1
            return None, {}

        max_prev_high = max(c["high"] for c in prev_candles)
        if last["close"] <= max_prev_high:
            return None, {}

        close_pos = (last["close"] - last["low"]) / candle_range
        if close_pos < 0.70:
            return None, {}

        # SL/TP
        stop_loss = min(c["low"] for c in prev_candles) * SL_OFFSET
        risk_pct = ((last["close"] - stop_loss) / last["close"]) * 100
        tp1 = last["close"] * (1 + risk_pct * TP1_RR / 100)
        tp2 = last["close"] * (1 + risk_pct * TP2_RR / 100)

        return "BULLISH_IMPULSE", {
            "price": last["close"],
            "ema7": e7, "ema25": e25, "ema99": e99,
            "spread": spread, "rvol": rvol,
            "impulse": ((last["close"] - last["open"]) / last["open"]) * 100,
            "stop_loss": stop_loss,
            "risk_pct": risk_pct,
            "tp1": tp1, "tp2": tp2,
            "prior_move": prior_move,
            "close_pos": close_pos * 100,
        }

    # ==================== SHORT ====================
    if e7 < e25 < e99:
        spread = ((e99 - e7) / e99) * 100
        if spread < MIN_EMA_SPREAD_PCT:
            stats["rejected_spread"] += 1
            return None, {}

        min_prev_low = min(c["low"] for c in prev_candles)
        if last["close"] >= min_prev_low:
            return None, {}

        close_pos = (last["high"] - last["close"]) / candle_range
        if close_pos < 0.70:
            return None, {}

        stop_loss = max(c["high"] for c in prev_candles) * (2 - SL_OFFSET)
        risk_pct = ((stop_loss - last["close"]) / last["close"]) * 100
        tp1 = last["close"] * (1 - risk_pct * TP1_RR / 100)
        tp2 = last["close"] * (1 - risk_pct * TP2_RR / 100)

        return "BEARISH_IMPULSE", {
            "price": last["close"],
            "ema7": e7, "ema25": e25, "ema99": e99,
            "spread": spread, "rvol": rvol,
            "impulse": ((last["open"] - last["close"]) / last["open"]) * 100,
            "stop_loss": stop_loss,
            "risk_pct": risk_pct,
            "tp1": tp1, "tp2": tp2,
            "prior_move": prior_move,
            "close_pos": close_pos * 100,
        }

    return None, {}


# ============================================================
# TELEGRAM
# ============================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def get_watch_keyboard(symbol, direction):
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text=f"👁 Следить за {symbol_base(symbol)} (1m)",
                callback_data=f"watch|{symbol}|{direction}"
            )
        ]]
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("watch|"))
async def process_watch(callback_query: CallbackQuery):
    try:
        parts = callback_query.data.split("|")
        symbol, direction = parts[1], parts[2]
        watched_coins[symbol] = {
            "time": time.time(),
            "direction": direction,
            "auto": False,
        }
        await callback_query.answer(text=f"👁 {symbol} → 1M {direction}")
        await bot.send_message(
            CHAT_ID or callback_query.from_user.id,
            f"🎯 <b>{symbol_base(symbol)}</b>\n"
            f"Режим 1M: <b>{direction}</b>\n\n"
            f"Бот ждёт микрооткат и реакцию.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error("Watch callback error: %s", e)


async def send_15m_signal(symbol, ticker, signal_type, meta):
    direction = "LONG" if signal_type == "BULLISH_IMPULSE" else "SHORT"
    icon = "🚀" if direction == "LONG" else "🔻"
    ema_line = "EMA7 > EMA25 > EMA99" if direction == "LONG" else "EMA7 < EMA25 < EMA99"

    msg = (
        f"{icon} <b>ПАРТИЗАН — {direction} ИМПУЛЬС</b>\n\n"
        f"🪙 <b>{symbol_base(symbol)}</b>\n\n"
        f"📈 Веер EMA: <b>{ema_line}</b>\n"
        f"📐 Расширение: <b>{meta['spread']:.2f}%</b>\n"
        f"🚀 Импульс 15m: <b>{meta['impulse']:+.2f}%</b>\n"
        f"📊 RVOL: <b>{meta['rvol']:.2f}x</b>\n"
        f"↘️ Движение до входа: <b>{meta['prior_move']:+.2f}%</b>\n"
        f"🎯 Закрытие в верхних: <b>{meta['close_pos']:.0f}%</b>\n\n"
        f"💰 <b>ВХОД:</b> <code>{format_price(meta['price'])}</code>\n"
        f"🛑 <b>Стоп:</b> <code>{format_price(meta['stop_loss'])}</code> "
        f"(риск {meta['risk_pct']:.2f}%)\n"
        f"🎯 <b>TP1:</b> <code>{format_price(meta['tp1'])}</code> "
        f"(+{meta['risk_pct'] * TP1_RR:.2f}%)\n"
        f"🎯 <b>TP2:</b> <code>{format_price(meta['tp2'])}</code> "
        f"(+{meta['risk_pct'] * TP2_RR:.2f}%)\n\n"
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть график</a>"
    )

    if CHAT_ID:
        try:
            await bot.send_message(
                CHAT_ID,
                msg,
                parse_mode="HTML",
                reply_markup=get_watch_keyboard(symbol, direction),
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error("Telegram error: %s", e)


# ============================================================
# 1M RADAR
# ============================================================

def analyze_1m(candles, direction):
    if len(candles) < 15:
        return None
    closed = candles[:-1]
    if len(closed) < 12:
        return None

    prev, last = closed[-2], closed[-1]

    if direction == "LONG":
        impulse = ((prev["close"] - prev["open"]) / prev["open"]) * 100
        if impulse < M1_IMPULSE_MIN or last["close"] >= last["open"]:
            return None
        pullback = ((prev["close"] - last["close"]) / prev["close"]) * 100
        if not (0 < pullback <= M1_PULLBACK_MAX):
            return None

        rng = last["high"] - last["low"]
        if rng <= 0:
            return None
        lower_wick = (min(last["open"], last["close"]) - last["low"]) / rng
        if lower_wick < M1_WICK_MIN:
            return None

        return {
            "type": "LONG_PULLBACK",
            "price": last["close"],
            "pullback": pullback,
            "wick": lower_wick * 100,
        }

    if direction == "SHORT":
        impulse = ((prev["open"] - prev["close"]) / prev["open"]) * 100
        if impulse < M1_IMPULSE_MIN or last["close"] <= last["open"]:
            return None
        pullback = ((last["close"] - prev["close"]) / prev["close"]) * 100
        if not (0 < pullback <= M1_PULLBACK_MAX):
            return None

        rng = last["high"] - last["low"]
        if rng <= 0:
            return None
        upper_wick = (last["high"] - max(last["open"], last["close"])) / rng
        if upper_wick < M1_WICK_MIN:
            return None

        return {
            "type": "SHORT_PULLBACK",
            "price": last["close"],
            "pullback": pullback,
            "wick": upper_wick * 100,
        }

    return None


async def scan_watched_1m(session):
    while True:
        try:
            now = time.time()
            for symbol, info in list(watched_coins.items()):
                if now - info["time"] > WATCH_TIME:
                    del watched_coins[symbol]
                    pending_reactions.pop(symbol, None)
                    continue

                direction = info["direction"]
                candles = await get_klines(
                    session, symbol, interval="1m", limit=ONE_MIN_LIMIT
                )
                if not candles:
                    continue

                stats["m1_checks"] += 1
                result = analyze_1m(candles, direction)
                if not result:
                    continue

                # ----- Первый этап: откат -----
                if symbol not in pending_reactions:
                    pending_reactions[symbol] = {
                        "time": time.time(),
                        "price": result["price"],
                        "pullback": result["pullback"],
                        "wick": result["wick"],
                        "direction": direction,
                    }
                    msg = (
                        f"👁 <b>ПАРТИЗАН 1M — МИКРООТКАТ</b>\n\n"
                        f"🪙 {symbol_base(symbol)}\n"
                        f"Направление: <b>{direction}</b>\n"
                        f"Откат: <b>{result['pullback']:.2f}%</b>\n"
                        f"Фитиль: <b>{result['wick']:.1f}%</b>\n\n"
                        f"⏳ Ждём реакцию..."
                    )
                    if CHAT_ID:
                        await bot.send_message(CHAT_ID, msg, parse_mode="HTML")
                    continue

                # ----- Второй этап: подтверждение реакции -----
                pending = pending_reactions[symbol]
                last = candles[-2]

                if direction == "LONG":
                    reaction = ((last["close"] - pending["price"]) / pending["price"]) * 100
                    if reaction >= M1_REACTION_MIN:
                        stats["m1_entries"] += 1
                        msg = (
                            f"🎯 <b>ПАРТИЗАН — LONG ENTRY</b>\n\n"
                            f"🪙 <b>{symbol_base(symbol)}</b>\n"
                            f"🚀 15M: EMA7 > EMA25 > EMA99\n"
                            f"↘️ Откат: {pending['pullback']:.2f}%\n"
                            f"↗️ Реакция: <b>+{reaction:.2f}%</b>\n"
                            f"💵 Цена: <code>{format_price(last['close'])}</code>\n\n"
                            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График</a>"
                        )
                        if CHAT_ID:
                            await bot.send_message(
                                CHAT_ID, msg,
                                parse_mode="HTML",
                                disable_web_page_preview=True
                            )
                        del watched_coins[symbol]
                        del pending_reactions[symbol]
                else:
                    reaction = ((pending["price"] - last["close"]) / pending["price"]) * 100
                    if reaction >= M1_REACTION_MIN:
                        stats["m1_entries"] += 1
                        msg = (
                            f"🎯 <b>ПАРТИЗАН — SHORT ENTRY</b>\n\n"
                            f"🪙 <b>{symbol_base(symbol)}</b>\n"
                            f"🔴 15M: EMA7 < EMA25 < EMA99\n"
                            f"↗️ Откат: {pending['pullback']:.2f}%\n"
                            f"↘️ Реакция: <b>-{reaction:.2f}%</b>\n"
                            f"💵 Цена: <code>{format_price(last['close'])}</code>\n\n"
                            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График</a>"
                        )
                        if CHAT_ID:
                            await bot.send_message(
                                CHAT_ID, msg,
                                parse_mode="HTML",
                                disable_web_page_preview=True
                            )
                        del watched_coins[symbol]
                        del pending_reactions[symbol]

        except Exception as e:
            stats["errors"] += 1
            logger.error("1M scanner error: %s", e)

        await asyncio.sleep(M1_POLL_INTERVAL)


# ============================================================
# MAIN LOOP
# ============================================================

async def main_loop(session):
    while True:
        scan_started = time.time()
        try:
            tickers = await get_tickers(session)
            stats["scans"] += 1
            stats["symbols"] = len(tickers)
            signal_count = 0
            semaphore = asyncio.Semaphore(10)

            async def process_symbol(symbol, ticker):
                async with semaphore:
                    now = time.time()
                    if symbol in last_signals and now - last_signals[symbol] < ALERT_COOLDOWN:
                        return None

                    candles = await get_klines(
                        session, symbol, interval=TIMEFRAME, limit=KLINE_LIMIT
                    )
                    if not candles:
                        return None

                    signal_type, meta = analyze_market_15m(candles)
                    if not signal_type:
                        return None

                    direction = "LONG" if signal_type == "BULLISH_IMPULSE" else "SHORT"

                    # Проверка 4h контекста
                    ctx_ok = await check_4h_context(session, symbol, direction)
                    if not ctx_ok:
                        logger.debug(f"{symbol}: отклонён по 4h контексту")
                        return None

                    return (symbol, ticker, signal_type, meta, direction)

            tasks = [process_symbol(sym, tick) for sym, tick in tickers.items()]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception) or not result:
                    continue

                symbol, ticker, signal_type, meta, direction = result
                last_signals[symbol] = time.time()
                signal_count += 1
                stats["signals"] += 1
                stats["long" if direction == "LONG" else "short"] += 1

                # Отправляем сигнал
                await send_15m_signal(symbol, ticker, signal_type, meta)

                # АВТОМАТИЧЕСКИЙ РАДАР (без клика)
                if AUTO_WATCH:
                    watched_coins[symbol] = {
                        "time": time.time(),
                        "direction": direction,
                        "auto": True,
                    }
                    logger.info(f"👁 {symbol} автоматически поставлен на 1m радар ({direction})")

            elapsed = time.time() - scan_started
            logger.info(
                f"📊 СКАН #{stats['scans']} | пар={stats['symbols']} | "
                f"сигналов={signal_count} | 1M watched={len(watched_coins)} | "
                f"время={elapsed:.1f}с"
            )

            # Раз в 10 сканов — расширенная статистика
            if stats["scans"] % 10 == 0:
                logger.info(
                    f"📈 СТАТИСТИКА: всего сигналов={stats['signals']} | "
                    f"LONG={stats['long']} | SHORT={stats['short']} | "
                    f"1M входов={stats['m1_entries']} | "
                    f"отсев late={stats['rejected_late']} | "
                    f"rvol={stats['rejected_rvol']} | "
                    f"spread={stats['rejected_spread']} | "
                    f"ошибки={stats['errors']}"
                )

        except Exception as e:
            stats["errors"] += 1
            logger.error("Main loop error: %s", e)

        await asyncio.sleep(CHECK_INTERVAL)


# ============================================================
# WEB
# ============================================================

async def health(request):
    return web.Response(
        text=(
            f"🏹 PARTIZAN EMA ACTIVE | "
            f"Watched 1M: {len(watched_coins)} | "
            f"Signals: {stats['signals']} | "
            f"Entries 1M: {stats['m1_entries']}"
        )
    )


# ============================================================
# MAIN
# ============================================================

async def main():
    logger.info("🏹 ПАРТИЗАН EMA 7/25/99 (v2) запускается...")
    logger.info(
        f"Настройки: spread>={MIN_EMA_SPREAD_PCT}% | "
        f"RVOL>={MIN_RVOL}x | 4h-фильтр={USE_4H_FILTER} | "
        f"Auto-watch={AUTO_WATCH}"
    )

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=30)
    session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Веб-сервер на порту {PORT}")

    asyncio.create_task(main_loop(session))
    asyncio.create_task(scan_watched_1m(session))

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await session.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🏹 ПАРТИЗАН остановлен.")
    except Exception as e:
        logger.exception("Критическая ошибка: %s", e)
