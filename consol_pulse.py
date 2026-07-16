#!/usr/bin/env python3
import os
import sys
import json
import math
import time
import asyncio
import logging
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Set, Tuple, Optional, Any

import aiohttp
from aiohttp import web

# ==========================================================
# LOGGING (управляется переменной окружения LOG_LEVEL)
# ==========================================================
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("ConsolPulsePro")

# ==========================================================
# ENV & CONFIG
# ==========================================================
BOT_NAME_RENDER: str = os.getenv("RENDER_SERVICE_NAME", "consol-pulse-default")
TELEGRAM_TOKEN: Optional[str] = os.getenv("CONSOL_BOT_TOKEN")
TELEGRAM_CHAT_ID: Optional[str] = os.getenv("CONSOL_CHAT_ID")
PORT: int = int(os.getenv("PORT", "7862"))

SELF_URL: str = f"https://{BOT_NAME_RENDER}.onrender.com"

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    logger.error("Telegram ENV variables not found.")
    sys.exit(1)

START_TIME: datetime = datetime.now()
START_MONOTONIC: float = time.monotonic()

# ==========================================================
# GLOBAL STATE & CACHE LIMITS
# ==========================================================
# BREAKOUT_MEMORY хранит: {symbol: (last_base_candle_time, timestamp_of_alert)}
BREAKOUT_MEMORY: OrderedDict[str, Tuple[int, float]] = OrderedDict()
MAX_KLINE_CACHE_SIZE: int = 500

# ==========================================================
# PERSISTENT STORAGE MANAGEMENT (ATOMIC WRITES)
# ==========================================================
BLACKLIST_FILE: str = "blacklist.json"
BLACKLIST: Set[str] = {
    "IRISUSDT", "IRYSUSDT", "LUNCUSDT", 
    "USTCUSDT", "USD1USDT", "USDCUSDT"
}

PLATFORMS_FILE: str = "platforms.json"
CURRENT_PLATFORMS: Dict[str, str] = {}

def load_blacklist() -> None:
    global BLACKLIST
    if not os.path.exists(BLACKLIST_FILE):
        return
    try:
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            BLACKLIST = set(json.load(f))
        logger.info(f"Blacklist loaded: {len(BLACKLIST)}")
    except Exception as e:
        logger.error(f"Error loading blacklist: {e}")

def save_blacklist() -> None:
    tmp_file = f"{BLACKLIST_FILE}.tmp"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(sorted(BLACKLIST), f, indent=2)
        os.replace(tmp_file, BLACKLIST_FILE)
    except Exception as e:
        logger.error(f"Error saving blacklist atomically: {e}")
        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except Exception:
                pass

def load_platforms() -> None:
    global CURRENT_PLATFORMS
    if not os.path.exists(PLATFORMS_FILE):
        return
    try:
        with open(PLATFORMS_FILE, "r", encoding="utf-8") as f:
            CURRENT_PLATFORMS = json.load(f)
        logger.info(f"Platforms configuration loaded: {len(CURRENT_PLATFORMS)} chats")
    except Exception as e:
        logger.error(f"Error loading platforms: {e}")

def save_platforms() -> None:
    tmp_file = f"{PLATFORMS_FILE}.tmp"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(CURRENT_PLATFORMS, f, indent=2)
        os.replace(tmp_file, PLATFORMS_FILE)
    except Exception as e:
        logger.error(f"Error saving platforms atomically: {e}")
        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except Exception:
                pass

# ==========================================================
# STRATEGY FILTERS
# ==========================================================
MIN_PRICE: float = 0.0001
MAX_PRICE: float = 1.0
CHECK_INTERVAL: int = 60
MAX_CONCURRENT_REQUESTS: int = 10
ALERT_COOLDOWN_SECONDS: float = 4 * 3600  # 4 часа в секундах
BINGX_API_URL: str = "https://open-api.bingx.com/openApi/swap/v2/quote"

MAX_CONSOLIDATION_RANGE: float = 2.5
MIN_BREAKOUT_PCT: float = 0.15
MIN_BODY_RATIO: float = 0.60
MIN_HOURLY_VOLUME: float = 50000

VOLUME_X_TRIGGER: float = 1.7
EARLY_VOLUME_X: float = 1.0
EARLY_SIGNAL: bool = True
MAX_VOLUME_CV: float = 0.8

MOMENTUM_THRESHOLD: float = 1.0
ATR_MULTIPLIER: float = 1.3

USE_OPEN_INTEREST: bool = False
OI_HISTORY_DEPTH: int = 15
OI_MIN_HISTORY: int = 5
MIN_OI_GROWTH: float = 0.3

# ==========================================================
# MATH & HELPERS
# ==========================================================
def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
        
    sma = sum(values[:period]) / period
    k = 2 / (period + 1)
    result = sma
    for price in values[period:]:
        result = price * k + result * (1 - k)
    return result

def calculate_cv(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    std = math.sqrt(variance)
    return std / mean

def escape_markdown(text: str) -> str:
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join('\\' + char if char in escape_chars else char for char in text)

# ==========================================================
# MONITOR CORE
# ==========================================================
class ConsolidationMonitor:
    def __init__(self) -> None:
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.last_alert_time: Dict[str, float] = {}  
        self.last_cleanup_date = datetime.now().date()
        self.session: Optional[aiohttp.ClientSession] = None
        self.api_available: bool = True
        
        self.signal_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        
        self.oi_history: Dict[str, List[float]] = {}
        self.monitored_symbols: Set[str] = set()
        
        self.kline_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._iteration: int = 0
        
        # 1. Скорректированный кеш для get_active_symbols (TTL уменьшен до 75 сек)
        self.active_symbols_cache: List[Dict[str, Any]] = []
        self.active_symbols_last_update: float = 0.0
        self.active_symbols_cache_ttl: float = 75.0  # Изменено со 180 на 75 секунд для быстрой реакции на листинги
        
        # 3. Хранит {symbol: (last_calculated_ema, last_processed_closed_timestamp)}
        self.ema_cache: Dict[str, Tuple[float, int]] = {}
        
        # Статистика
        self.stats_all_time: Dict[str, int] = {
            "checked_symbols": 0, "signals": 0, "filtered_by_trend": 0,
            "filtered_by_volume": 0, "filtered_by_breakout": 0, "filtered_by_oi": 0
        }
        self.stats_daily: Dict[str, int] = {
            "checked_symbols": 0, "signals": 0, "filtered_by_trend": 0,
            "filtered_by_volume": 0, "filtered_by_breakout": 0, "filtered_by_oi": 0
        }

    def increment_stat(self, key: str) -> None:
        self.stats_all_time[key] = self.stats_all_time.get(key, 0) + 1
        self.stats_daily[key] = self.stats_daily.get(key, 0) + 1

    async def init_session(self) -> None:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"})

    def stop_monitoring(self, symbol: str) -> None:
        self.monitored_symbols.discard(symbol)
        self.oi_history.pop(symbol, None)

    async def send_telegram_signal(self, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
        if not self.session or self.session.closed:
            return
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            async with self.session.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json=payload,
                timeout=10,
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Telegram error {resp.status}")
        except Exception as e:
            logger.error(f"Telegram send exception: {e}")

    async def tg_sender_worker(self) -> None:
        logger.info("Telegram sender started")
        while True:
            try:
                message, keyboard = await self.signal_queue.get()
                start_time = time.monotonic()
                
                await self.send_telegram_signal(message, keyboard)
                self.signal_queue.task_done()
                
                elapsed = time.monotonic() - start_time
                sleep_time = max(0.05 - elapsed, 0.001)
                await asyncio.sleep(sleep_time)
            except Exception as e:
                logger.error(f"Sender worker error: {e}")
                await asyncio.sleep(1)

    async def ping_api_loop(self) -> None:
        while True:
            sleep_interval = 120  
            try:
                if self.session and not self.session.closed:
                    async with self.session.get(f"{BINGX_API_URL}/contracts", timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            was_available = self.api_available
                            self.api_available = (data.get("code") == 0)
                            if not was_available and self.api_available:
                                logger.info("API connection restored!")
                        elif resp.status == 429:
                            logger.warning("API ping: 429 Too Many Requests. Temporary warning.")
                        elif resp.status in (500, 503):
                            logger.error(f"API ping: Server Error {resp.status}. Disabling API calls.")
                            self.api_available = False
                            sleep_interval = 10  
                        else:
                            logger.error(f"API ping: Unexpected status {resp.status}")
                            self.api_available = False
                            sleep_interval = 10
            except Exception as e:
                logger.error(f"API ping exception: {e}")
                self.api_available = False
                sleep_interval = 10
            await asyncio.sleep(sleep_interval)

    async def get_open_interest(self, symbol: str) -> Optional[float]:
        if not self.session or self.session.closed:
            return None
        try:
            async with self.session.get(f"{BINGX_API_URL}/openInterest?symbol={symbol}", timeout=5) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data.get("code") != 0:
                    return None
                return float(data["data"]["openInterest"])
        except Exception:
            return None

    async def get_active_symbols(self) -> List[Dict[str, Any]]:
        if not self.session or self.session.closed:
            return []
        
        now = time.monotonic()
        if self.active_symbols_cache and (now - self.active_symbols_last_update < self.active_symbols_cache_ttl):
            return self.active_symbols_cache

        try:
            # Заменяем /contracts на /ticker, так как /contracts не возвращает цену (lastPrice)
            async with self.session.get(f"{BINGX_API_URL}/ticker", timeout=15) as resp:
                if resp.status != 200:
                    return self.active_symbols_cache  
                data = await resp.json()
                tickers = data.get("data", [])
                result = []
                for item in tickers:
                    symbol = item.get("symbol", "")
                    clean = symbol.replace("-", "")
                    if not clean.endswith("USDT") or clean in BLACKLIST:
                        continue
                    try:
                        price = float(item.get("lastPrice", 0))
                    except Exception:
                        continue
                    if price < MIN_PRICE or price > MAX_PRICE:
                        continue
                    result.append({"symbol": symbol, "price": price})
                
                self.active_symbols_cache = result
                self.active_symbols_last_update = now
                logger.info(f"Updated active symbols cache: {len(result)} items")
                return result
        except Exception as e:
            logger.error(f"Get active symbols error: {e}")
            return self.active_symbols_cache

    async def load_hourly_klines(self, symbol: str) -> Tuple[Optional[List[Dict[str, Any]]], Optional[float]]:
        if not self.session or self.session.closed:
            return None, None
        
        if symbol in self.kline_cache:
            cached = self.kline_cache[symbol]
            age = time.monotonic() - cached["time"]
            if age < 55:
                self.kline_cache.move_to_end(symbol)
                return cached["klines"], cached["ema200"]
        try:
            url = f"{BINGX_API_URL}/klines?symbol={symbol}&interval=1h&limit=205"
            async with self.session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return None, None
                data = await resp.json()
                klines = data.get("data", [])
                if len(klines) < 205:
                    return None, None
                
                # Исключаем последнюю незакрытую свечу для стабильного EMA200
                closed = klines[-201:-1]
                closes = [float(x["close"]) for x in closed]
                
                # Время последней полностью ЗАКРЫТОЙ свечи (в миллисекундах)
                last_closed_candle_time = int(closed[-1]["time"]

    async def update_oi_history_only(self, symbol: str) -> None:
        """Вспомогательный метод для фонового наполнения истории ОИ без задержки сигналов"""
        oi = await self.get_open_interest(symbol)
        if oi is not None:
            history = self.oi_history.setdefault(symbol, [])
            history.append(oi)
            if len(history) > OI_HISTORY_DEPTH:
                history.pop(0)

    async def check_consolidation_and_pump(self, target_data: Dict[str, Any]) -> None:
        symbol: str = target_data["symbol"]
        current_price: float = target_data["price"]

        async with self.semaphore:
            self.increment_stat("checked_symbols")
            clean: str = symbol.replace("-", "").replace("USDT", "")

            if current_price < MIN_PRICE or current_price > MAX_PRICE:
                return

            try:
                klines, ema80 = await asyncio.wait_for(
                    self.load_hourly_klines(symbol),
                    timeout=12
                )
            except (asyncio.TimeoutError, Exception):
                return

            if klines is None or ema80 is None:
                return

            current = klines[-1]
            current_open = float(current["open"])
            current_high = float(current["high"])
            current_low = float(current["low"])
            current_close = float(current["close"])
            current_volume = float(current["volume"])

            # 1. Фильтр тренда (EMA80) с логированием
            if current_close < ema80:
                self.increment_stat("filtered_by_trend")
                logger.debug(f"{symbol} отклонён: цена ниже EMA80 ({current_close:.6f} < {ema80:.6f})")
                return

            history = klines[-26:-1]
            base: List[Dict[str, Any]] = []
            highest: Optional[float] = None
            lowest: Optional[float] = None
            base_tr_sum = 0.0

            history_start = len(klines) - 26

            for idx in range(len(history) - 1, -1, -1):
                candle = history[idx]
                high = float(candle["high"])
                low = float(candle["low"])
                
                if idx == 0:
                    prev_c = float(klines[history_start - 1]["close"])
                else:
                    prev_c = float(history[idx - 1]["close"])
                
                tr = max(
                    high - low,
                    abs(high - prev_c),
                    abs(low - prev_c)
                )

                if lowest is None or highest is None:
                    highest = high
                    lowest = low
                
                new_highest = max(highest, high)
                new_lowest = min(lowest, low)
                
                if new_lowest <= 0:
                    return
                
                compression = ((new_highest - new_lowest) / new_lowest) * 100
                if compression > MAX_CONSOLIDATION_RANGE:
                    break

                current_base_len = len(base) + 1
                temp_tr_sum = base_tr_sum + tr
                avg_tr_base = temp_tr_sum / current_base_len
                atr_base_pct = (avg_tr_base / new_lowest) * 100

                if atr_base_pct >= 0.6:
                    break
                
                highest = new_highest
                lowest = new_lowest
                base_tr_sum = temp_tr_sum
                base.append(candle)

            period = len(base)
            # 2. Фильтр длины базы
            if period < 3 or period > 24:
                self.increment_stat("filtered_by_breakout")
                logger.debug(f"{symbol} отклонён: длина базы {period} (допустимо 3-24)")
                return

            volumes = [float(x["volume"]) for x in base]
            avg_volume = sum(volumes) / len(volumes)
            # 3. Минимальный средний объём базы
            if avg_volume < MIN_HOURLY_VOLUME:
                self.increment_stat("filtered_by_volume")
                logger.debug(f"{symbol} отклонён: средний объём базы {avg_volume:.0f} < {MIN_HOURLY_VOLUME}")
                return

            cv = calculate_cv(volumes)
            # 4. Коэффициент вариации объёмов
            if cv > MAX_VOLUME_CV:
                self.increment_stat("filtered_by_volume")
                logger.debug(f"{symbol} отклонён: CV объёма {cv:.3f} > {MAX_VOLUME_CV}")
                return

            self.monitored_symbols.add(symbol)
            highest_base = max(float(x["high"]) for x in base)

            breakout = ((current_high - highest_base) / highest_base) * 100
            # 5. Минимальный процент пробоя
            if breakout < MIN_BREAKOUT_PCT:
                self.increment_stat("filtered_by_breakout")
                logger.debug(f"{symbol} отклонён: пробой {breakout:.2f}% < {MIN_BREAKOUT_PCT}%")
                return

            previous_close = float(klines[-2]["close"])
            momentum = ((current_high - previous_close) / previous_close) * 100
            # 6. Momentum
            if momentum < MOMENTUM_THRESHOLD:
                self.increment_stat("filtered_by_breakout")
                logger.debug(f"{symbol} отклонён: momentum {momentum:.2f}% < {MOMENTUM_THRESHOLD}%")
                return

            current_tr = max(
                current_high - current_low,
                abs(current_high - previous_close),
                abs(current_low - previous_close)
            )
            avg_tr_final = base_tr_sum / len(base)

            # 7. ATR multiplier
            if current_tr < avg_tr_final * ATR_MULTIPLIER:
                self.increment_stat("filtered_by_breakout")
                logger.debug(f"{symbol} отклонён: ATR {current_tr:.4f} < {avg_tr_final * ATR_MULTIPLIER:.4f}")
                return

            if current_tr == 0:
                return
                
            body = abs(current_close - current_open)
            body_ratio = body / (current_high - current_low) if (current_high - current_low) > 0 else 0
            # 8. Body ratio
            if body_ratio < MIN_BODY_RATIO:
                self.increment_stat("filtered_by_breakout")
                logger.debug(f"{symbol} отклонён: тело свечи {body_ratio:.2f} < {MIN_BODY_RATIO}")
                return

            growth = ((current_close - current_open) / current_open) * 100
            # 9. Рост внутри свечи
            if growth < 0.8:
                self.increment_stat("filtered_by_breakout")
                logger.debug(f"{symbol} отклонён: рост {growth:.2f}% < 0.8%")
                return

            previous_volume = float(klines[-2]["volume"])
            # 10. Рост объёма относительно предыдущей свечи
            if current_volume < previous_volume * 1.3:
                self.increment_stat("filtered_by_volume")
                logger.debug(f"{symbol} отклонён: объём не вырос к предыдущей свече ({current_volume:.0f} < {previous_volume*1.3:.0f})")
                return

            peak_volume = max(float(x["volume"]) for x in base)
            # 11. Превышение пика объёма базы
            if current_volume < peak_volume * 1.5:
                self.increment_stat("filtered_by_volume")
                logger.debug(f"{symbol} отклонён: объём {current_volume:.0f} < пиковый в базе *1.5 = {peak_volume*1.5:.0f}")
                return

            volume_ratio = current_volume / avg_volume
            required_volume = VOLUME_X_TRIGGER

            if EARLY_SIGNAL:
                now_utc = datetime.now(timezone.utc)
                elapsed_seconds = now_utc.minute * 60 + now_utc.second
                progress = max(elapsed_seconds, 1) / 3600.0
                required_volume = max(EARLY_VOLUME_X, progress * VOLUME_X_TRIGGER)

            # 12. Гибкий триггер объёма
            if volume_ratio < required_volume:
                self.increment_stat("filtered_by_volume")
                logger.debug(f"{symbol} отклонён: volume_ratio {volume_ratio:.2f} < required {required_volume:.2f} (прогресс {progress:.0%})")
                return

            await self.update_oi_history_only(symbol)

            oi_growth = 0.0
            oi_status = "collecting..."
            oi_bonus = False
            
            if USE_OPEN_INTEREST:
                fresh_oi = await self.get_open_interest(symbol)
                history_oi = self.oi_history.get(symbol, [])
                
                if fresh_oi is not None and len(history_oi) >= OI_MIN_HISTORY:
                    previous_oi = history_oi[:-1]
                    average_oi = sum(previous_oi) / len(previous_oi)

                    # 13. Проверка падения OI (сравнение с последним элементом)
                    if len(history_oi) >= 1 and fresh_oi <= history_oi[-1]:
                        self.increment_stat("filtered_by_oi")
                        logger.debug(f"{symbol} отклонён: OI не вырос (тек. {fresh_oi:.0f} <= пред. {history_oi[-1]:.0f})")
                        return

                    if average_oi > 0:
                        oi_growth = ((fresh_oi - average_oi) / average_oi) * 100
                        # 14. Недостаточный рост OI
                        if oi_growth < MIN_OI_GROWTH:
                            self.increment_stat("filtered_by_oi")
                            logger.debug(f"{symbol} отклонён: рост OI {oi_growth:.2f}% < {MIN_OI_GROWTH}%")
                            return
                        oi_status = f"+{oi_growth:.2f}%"
                        oi_bonus = True

            now_monotonic = time.monotonic()
            last_base_candle_time = int(base[-1]["time"])
            
            if symbol in BREAKOUT_MEMORY:
                stored_base_candle_time, last_time = BREAKOUT_MEMORY[symbol]
                if now_monotonic - last_time < ALERT_COOLDOWN_SECONDS and last_base_candle_time <= stored_base_candle_time:
                    return
                BREAKOUT_MEMORY.move_to_end(symbol)
                
            if symbol in self.last_alert_time:
                if now_monotonic - self.last_alert_time[symbol] < ALERT_COOLDOWN_SECONDS:
                    return

            score = 0
            score += min(period // 5, 3)
            score += min(int(volume_ratio), 3)
            score += min(int(growth), 2)
            score += int(body_ratio * 2)
            
            if oi_bonus:
                score += 1
            if cv < 0.3:
                score += 1
            if (current_tr / avg_tr_final) > 2:
                score += 1
            if breakout > 0.4:
                score += 1
                
            score = min(score, 14)

            BREAKOUT_MEMORY[symbol] = (last_base_candle_time, now_monotonic)
            if len(BREAKOUT_MEMORY) > 1000:
                BREAKOUT_MEMORY.popitem(last=False)

            self.last_alert_time[symbol] = now_monotonic
            self.increment_stat("signals")

            if breakout > 0.4:
                signal_type = "🔥 STRONG BREAKOUT"
            else:
                signal_type = "🟢 LONG"

            body_pct = int(body_ratio * 100)
            wick_pct = 100 - body_pct

            platform = CURRENT_PLATFORMS.get(TELEGRAM_CHAT_ID, "BingX")

            clean_esc = clean  
            current_close_esc = str(current_close)  
            
            signal_type_esc = escape_markdown(signal_type)
            breakout_esc = escape_markdown(f"{breakout:.2f}%")
            momentum_esc = escape_markdown(f"{momentum:.2f}%")
            atr_esc = escape_markdown(f"x{current_tr/avg_tr_final:.2f}")
            volume_esc = escape_markdown(f"x{volume_ratio:.2f}")
            oi_status_esc = escape_markdown(oi_status)
            period_esc = escape_markdown(str(period))
            body_pct_esc = escape_markdown(str(body_pct))
            wick_pct_esc = escape_markdown(str(wick_pct))
            score_esc = escape_markdown(f"{score}/14")

            message = (
                f"🔥 *CONSOL PULSE PRO*\n\n"
                f"Тип: {signal_type_esc}\n"
                f"🪙 `{clean_esc}`\n"
                f"📈 Цена: `{current_close_esc}`\n"
                f"🚀 Breakout: *{breakout_esc}*\n"
                f"⚡ Momentum: *{momentum_esc}*\n"
                f"📏 ATR *{atr_esc}*\n"
                f"📊 Volume *{volume_esc}*\n"
                f"💰 OI *{oi_status_esc}*\n"
                f"⏳ База: *{period_esc}* ч\n"
                f"🕯 Тело: *{body_pct_esc}%* / Тени: *{wick_pct_esc}%*\n"
                f"⭐ Score *{score_esc}*"
            )

            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": f"🔄 {platform}", "callback_data": "toggle_platform"},
                        {"text": "❌ Blacklist", "callback_data": f"block_{clean}USDT"}
                    ]
                ]
            }
            logger.info(f"Signal: {clean}")
            
            try:
                self.signal_queue.put_nowait((message, keyboard))
            except asyncio.QueueFull:
                logger.warning("Telegram queue overflow. Lost signal...")
                try:
                    _, _ = self.signal_queue.get_nowait()
                    self.signal_queue.task_done()
                    self.signal_queue.put_nowait((message, keyboard))
                except Exception:
                    pass

    def cleanup(self) -> None:
        now_monotonic = time.monotonic()
        expired = [sym for sym, t in self.last_alert_time.items() if now_monotonic - t > ALERT_COOLDOWN_SECONDS * 2]
        for sym in expired:
            del self.last_alert_time[sym]

        expired_breakout = [sym for sym, data in BREAKOUT_MEMORY.items() if now_monotonic - data[1] > ALERT_COOLDOWN_SECONDS * 2]
        for sym in expired_breakout:
            del BREAKOUT_MEMORY[sym]

        now = datetime.now()
        if now.date() > self.last_cleanup_date:
            self.kline_cache.clear()
            self.monitored_symbols.clear()
            self.oi_history.clear()
            self.ema_cache.clear()  
            
            self.stats_daily = {
                "checked_symbols": 0, "signals": 0, "filtered_by_trend": 0,
                "filtered_by_volume": 0, "filtered_by_breakout": 0, "filtered_by_oi": 0
            }
            
            self.last_cleanup_date = now.date()
            logger.info("Daily cleanup completed.")

    async def start_loop(self) -> None:
        await self.init_session()
        asyncio.create_task(self.tg_sender_worker())
        asyncio.create_task(self.ping_api_loop())

        logger.info("Consol Pulse Pro started.")
        await self.send_telegram_signal("🚀 *Consol Pulse Pro started successfully\\.*")
        await asyncio.sleep(5)

        while True:
            try:
                self._iteration += 1
                
                if self._iteration % 15 == 0:
                    self.cleanup()

                symbols = await self.get_active_symbols()
                if not symbols:
                    logger.warning("No symbols retrieved (API might be down). Sleeping...")
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                for i in range(0, len(symbols), MAX_CONCURRENT_REQUESTS):
                    batch = symbols[i : i + MAX_CONCURRENT_REQUESTS]
                    await asyncio.gather(
                        *[self.check_consolidation_and_pump(s) for s in batch], 
                        return_exceptions=True
                    )
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.exception(f"Loop global exception: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

monitor = ConsolidationMonitor()

# ==========================================================
# WEB ROUTERS
# ==========================================================
async def handle_index(request: web.Request) -> web.Response:
    return web.Response(text="Consol Pulse Pro работает.", status=200)

async def handle_tg_webhook(request: web.Request) -> web.Response:
    try:
        data = await request.json()

        if "callback_query" in data:
            cb = data["callback_query"]
            cb_id = cb["id"]
            cb_data = cb.get("data", "")
            chat_id = str(cb["message"]["chat"]["id"])

            if cb_data == "toggle_platform":
                current_plat = CURRENT_PLATFORMS.get(chat_id, "BingX")
                new_plat = "Bybit" if current_plat == "BingX" else "BingX"
                CURRENT_PLATFORMS[chat_id] = new_plat
                save_platforms()
                
                if monitor.session and not monitor.session.closed:
                    await monitor.session.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": f"Биржа: {new_plat}"},
                        timeout=10
                    )
                return web.Response(text="OK")

            if cb_data.startswith("block_"):
                symbol = cb_data.split("_")[1]
                BLACKLIST.add(symbol)
                save_blacklist()
                monitor.stop_monitoring(symbol)
                
                monitor.active_symbols_cache = []
                monitor.active_symbols_last_update = 0.0
                
                if monitor.session and not monitor.session.closed:
                    await monitor.session.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": f"{symbol} добавлен в BlackList"},
                        timeout=10
                    )
                return web.Response(text="OK")

        if "message" in data:
            text = data["message"].get("text", "").strip()

            if text == "/status":
                status_text = (
                    f"📊 *Consol Pulse Pro*\n\n"
                    f"• EMA200: *✅*\n"
                    f"• Compression: *≤ {escape_markdown(f'{MAX_CONSOLIDATION_RANGE}%')}*\n"
                    f"• Base ATR: *< {escape_markdown('0.6%')}*\n"
                    f"• Breakout: *≥ {escape_markdown(f'{MIN_BREAKOUT_PCT}%')}*\n"
                    f"• Momentum: *≥ {escape_markdown(f'{MOMENTUM_THRESHOLD}%')}*\n"
                    f"• ATR: *≥ x{escape_markdown(str(ATR_MULTIPLIER))}*\n"
                    f"• Body: *≥ {int(MIN_BODY_RATIO*100)}%*\n"
                    f"• Volume: *≥ x{escape_markdown(str(VOLUME_X_TRIGGER))}*\n"
                    f"• CV: *≤ {escape_markdown(str(MAX_VOLUME_CV))}*\n"
                    f"• OI: *≥ {escape_markdown(f'{MIN_OI_GROWTH}%')}*"
                )
                await monitor.send_telegram_signal(status_text)
                
            elif text == "/stats":
                s_all = monitor.stats_all_time
                s_day = monitor.stats_daily
                
                uptime_seconds = int(time.monotonic() - START_MONOTONIC)
                uptime_str = str(timedelta(seconds=uptime_seconds))
                
                uptime_esc = escape_markdown(uptime_str)
                launch_esc = escape_markdown(START_TIME.strftime('%Y-%m-%d %H:%M:%S'))

                stats_text = (
                    f"📈 *Statistics*\n\n"
                    f"⏱ Uptime: *{uptime_esc}*\n"
                    f"🚀 Launch time: *{launch_esc}*\n\n"
                    f"📅 *Daily statistics:*\n"
                    f"• Checked: *{s_day['checked_symbols']}*\n"
                    f"• Signals: *{s_day['signals']}*\n"
                    f"• Trend filter: *{s_day['filtered_by_trend']}*\n"
                    f"• Volume filter: *{s_day['filtered_by_volume']}*\n"
                    f"• Breakout filter: *{s_day['filtered_by_breakout']}*\n"
                    f"• OI filter: *{s_day['filtered_by_oi']}*\n\n"
                    f"🏛 *All-time statistics:*\n"
                    f"• Checked: *{s_all['checked_symbols']}*\n"
                    f"• Signals: *{s_all['signals']}*\n"
                    f"• Trend filter: *{s_all['filtered_by_trend']}*\n"
                    f"• Volume filter: *{s_all['filtered_by_volume']}*\n"
                    f"• Breakout filter: *{s_all['filtered_by_breakout']}*\n"
                    f"• OI filter: *{s_all['filtered_by_oi']}*"
                )
                await monitor.send_telegram_signal(stats_text)
    except Exception as e:
        logger.exception(f"Webhook execution error: {e}")
    return web.Response(text="OK")

async def keep_alive_ping() -> None:
    await asyncio.sleep(30)
    while True:
        try:
            if monitor.session and not monitor.session.closed:
                async with monitor.session.get(SELF_URL, timeout=10):
                    pass
        except Exception:
            pass
        await asyncio.sleep(240)

# ==========================================================
# LIFE-CYCLE MANAGEMENT (ON_CLEANUP HOOK)
# ==========================================================
async def on_cleanup_session(app: web.Application) -> None:
    logger.info("Cleaning up application sessions...")
    if monitor.session and not monitor.session.closed:
        await monitor.session.close()
        logger.info("HTTP session closed successfully.")

# ==========================================================
# ENTRY POINT
# ==========================================================
async def main() -> None:
    load_blacklist()
    load_platforms()
    
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_post("/tg-webhook", handle_tg_webhook)

    app.on_cleanup.append(on_cleanup_session)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    asyncio.create_task(keep_alive_ping())

    try:
        await monitor.start_loop()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Main loop cancelled.")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped manually.")
