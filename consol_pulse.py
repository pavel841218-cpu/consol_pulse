import asyncio
import logging
import math
import os
import time
from collections import deque
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "ТВОЙ_ТОКЕН_БОТА")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "ТВОЙ_CHAT_ID")

OKX_BASE_URL = "https://www.okx.com"
BITGET_BASE_URL = "https://api.bitget.com"
BYBIT_BASE_URL = "https://api.bybit.com"

# Ускоренный сканинг
SCAN_INTERVAL_SEC = 40
CONCURRENCY_LIMIT = 20

# Настройки раннего детектирования
IMPULSE_BASE_CANDLES = 32      # 8 часов на 15m (вместо 24ч)
SPARK_MIN_PCT = 1.8             # Порог для раннего импульса +1.8%
SCORE_SIGNAL_THRESHOLD = 45    # Порог основного сигнала
SCORE_WATCH_THRESHOLD = 30     # Порог наблюдения

MIN_CONFIRMATIONS = 1           # Минимум 1 подтверждающая биржа (Bitget или Bybit)
CONFIRM_THRESHOLD_PCT = 1.0     # Мин. рост +1.0% на второй бирже

WATCHLIST: Dict[str, Set[int]] = {}
LAST_SIGNAL: Dict[str, float] = {}
OI_HISTORY: Dict[str, deque] = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

bot = Bot(token=TG_BOT_TOKEN)
dp = Dispatcher(bot)

# ==========================================
# API КЛИЕНТЫ БИРЖ (OKX, BITGET, BYBIT)
# ==========================================
async def fetch_okx_candles(session: aiohttp.ClientSession, inst_id: str, bar: str = "15m", limit: int = 100) -> List[dict]:
    url = f"{OKX_BASE_URL}/api/v5/market/candles"
    params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
    try:
        async with session.get(url, params=params, timeout=5) as resp:
            data = await resp.json()
            if data.get("code") == "0" and data.get("data"):
                raw = data["data"]
                candles = []
                for c in reversed(raw):
                    candles.append({
                        "ts": int(c[0]) // 1000,
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5])
                    })
                return candles
    except Exception:
        pass
    return []

async def fetch_bitget_metrics(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    """Подтверждение с Bitget"""
    bg_symbol = f"{symbol}USDT"
    url = f"{BITGET_BASE_URL}/api/v2/mix/market/candles"
    params = {"symbol": bg_symbol, "productType": "USDT-FUTURES", "granularity": "15m", "limit": "2"}
    try:
        async with session.get(url, params=params, timeout=4) as resp:
            data = await resp.json()
            if data.get("code") == "00000" and data.get("data"):
                c = data["data"][-1]
                open_p, close_p = float(c[1]), float(c[4])
                pct = ((close_p - open_p) / open_p) * 100
                return {"pct": pct, "close": close_p}
    except Exception:
        pass
    return None

async def fetch_bybit_metrics(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    """Подтверждение с Bybit"""
    bb_symbol = f"{symbol}USDT"
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    params = {"category": "linear", "symbol": bb_symbol, "interval": "15", "limit": "2"}
    try:
        async with session.get(url, params=params, timeout=4) as resp:
            data = await resp.json()
            if data.get("retCode") == 0 and data["result"]["list"]:
                c = data["result"]["list"][0] # Bybit отдает свечи от новых к старым
                open_p, close_p = float(c[1]), float(c[4])
                pct = ((close_p - open_p) / open_p) * 100
                return {"pct": pct, "close": close_p}
    except Exception:
        pass
    return None

async def fetch_okx_tickers(session: aiohttp.ClientSession) -> List[str]:
    url = f"{OKX_BASE_URL}/api/v5/market/tickers?instType=SWAP"
    try:
        async with session.get(url, timeout=8) as resp:
            data = await resp.json()
            if data.get("code") == "0":
                return [
                    item["instId"] for item in data["data"]
                    if item["instId"].endswith("-USDT-SWAP")
                ]
    except Exception as e:
        logging.error(f"Ошибка тикеров OKX: {e}")
    return []

# ==========================================
# РАСЧЕТ МЕТРИК
# ==========================================
def calc_kaufman_efficiency(candles: List[dict]) -> float:
    if len(candles) < 2:
        return 0.0
    net_change = abs(candles[-1]["close"] - candles[0]["open"])
    sum_abs_changes = sum(abs(candles[i]["close"] - candles[i-1]["close"]) for i in range(1, len(candles)))
    return net_change / sum_abs_changes if sum_abs_changes > 0 else 0.0

def analyze_candles(candles: List[dict], base_lookback: int) -> Optional[dict]:
    if len(candles) < base_lookback + 1:
        return None

    current = candles[-1]
    base_candles = candles[-(base_lookback + 1):-1]

    base_high = max(c["high"] for c in base_candles)
    avg_vol = sum(c["volume"] for c in base_candles) / len(base_candles)
    rvol = current["volume"] / avg_vol if avg_vol > 0 else 1.0

    breakout = current["close"] > base_high
    pct_change = ((current["close"] - current["open"]) / current["open"]) * 100
    er = calc_kaufman_efficiency(candles[-8:])

    return {
        "close": current["close"],
        "pct": pct_change,
        "rvol": rvol,
        "breakout": breakout,
        "base_high": base_high,
        "er": er
    }

# ==========================================
# ОСНОВНАЯ ЛОГИКА И ИСПРАВЛЕНИЕ BEST
# ==========================================
async def process_symbol(session: aiohttp.ClientSession, inst_id: str, sem: asyncio.Semaphore):
    async with sem:
        base_symbol = inst_id.replace("-USDT-SWAP", "")
        
        # 1. Получаем свечи с OKX
        candles = await fetch_okx_candles(session, inst_id, bar="15m", limit=60)
        if not candles:
            return

        okx_m = analyze_candles(candles, IMPULSE_BASE_CANDLES)
        if not okx_m:
            return

        # Фильтр первичного движения: либо пробой базы, либо резкий подход (+1.8%)
        if not okx_m["breakout"] and okx_m["pct"] < SPARK_MIN_PCT:
            return

        # 2. Подтягиваем метрики с других бирж (Bitget и Bybit)
        bg_m, bb_m = await asyncio.gather(
            fetch_bitget_metrics(session, base_symbol),
            fetch_bybit_metrics(session, base_symbol)
        )

        # Проверка подтверждения хотя бы на одной из других бирж
        confirmations = sum(1 for m in (bg_m, bb_m) if m and m["pct"] >= CONFIRM_THRESHOLD_PCT)
        if confirmations < MIN_CONFIRMATIONS:
            return

        # 3. Считаем Итоговый Score
        score = 0.0
        if okx_m["breakout"]:
            score += 30.0
        score += min(okx_m["pct"] * 7, 30)
        score += min(okx_m["rvol"] * 4, 20)
        score += okx_m["er"] * 10
        score += confirmations * 10  # Бонус за подтверждение с нескольких бирж

        if score < SCORE_WATCH_THRESHOLD:
            return

        # Защита от спама (cooldown 15 минут)
        now = time.time()
        if now - LAST_SIGNAL.get(base_symbol, 0) < 900:
            return

        LAST_SIGNAL[base_symbol] = now
        is_watch = score < SCORE_SIGNAL_THRESHOLD

        await send_okx_signal(base_symbol, inst_id, okx_m, bg_m, bb_m, score, is_watch)

# ==========================================
# ТЕЛЕГРАМ И УВЕДОМЛЕНИЯ
# ==========================================
def get_signal_keyboard(inst_id: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(text="👀 Следить", callback_data=f"watch_{inst_id}"),
        InlineKeyboardButton(text="📊 OKX Chart", url=f"https://www.okx.com/trade-swap/{inst_id.lower()}")
    )
    return kb

async def send_okx_signal(
    symbol: str, inst_id: str, okx_m: dict, bg_m: Optional[dict], bb_m: Optional[dict], score: float, is_watch: bool
):
    status_tag = "👁 <b>НАБЛЮДЕНИЕ</b>" if is_watch else "⚡ <b>ИМПУЛЬС (OKX)</b>"
    
    bg_str = f"{bg_m['pct']:+.2f}%" if bg_m else "N/A"
    bb_str = f"{bb_m['pct']:+.2f}%" if bb_m else "N/A"

    msg = (
        f"{status_tag}\n\n"
        f"<b>Монета:</b> #{symbol}\n"
        f"<b>Score:</b> {score:.1f}/100\n"
        f"<b>OKX 15m:</b> {okx_m['pct']:+.2f}%\n"
        f"<b>Bitget / Bybit:</b> {bg_str} | {bb_str}\n"
        f"<b>RVOL:</b> {okx_m['rvol']:.2f}x | <b>ER:</b> {okx_m['er']:.2f}\n"
        f"<b>Статус базы:</b> {'✅ Пробой' if okx_m['breakout'] else '⚠️ Подход к хаю'}\n"
        f"<b>Уровень хая:</b> {okx_m['base_high']}\n"
    )
    try:
        await bot.send_message(
            chat_id=TG_CHAT_ID,
            text=msg,
            parse_mode="HTML",
            reply_markup=get_signal_keyboard(inst_id)
        )
    except Exception as e:
        logging.error(f"Ошибка отправки TG: {e}")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("watch_"))
async def process_watch_callback(callback_query: types.CallbackQuery):
    inst_id = callback_query.data.split("watch_")[1]
    user_id = callback_query.from_user.id

    if inst_id not in WATCHLIST:
        WATCHLIST[inst_id] = set()

    WATCHLIST[inst_id].add(user_id)

    await bot.answer_callback_query(
        callback_query.id,
        text=f"✅ {inst_id} добавлен в твой список наблюдения!",
        show_alert=True
    )

# ==========================================
# ЗАПУСК
# ==========================================
async def scanner_loop():
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                tickers = await fetch_okx_tickers(session)
                if tickers:
                    tasks = [process_symbol(session, t, sem) for t in tickers]
                    await asyncio.gather(*tasks)
            except Exception as e:
                logging.error(f"Ошибка цикла сканера: {e}")
            
            await asyncio.sleep(SCAN_INTERVAL_SEC)

async def on_startup(dp):
    asyncio.create_task(scanner_loop())
    logging.info("🚀 Мультибиржевой PUMP-HUNTER OKX запущен!")

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
