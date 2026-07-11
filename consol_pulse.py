#!/usr/bin/env python3
import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
import aiohttp
from aiohttp import web

# =====================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =====================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ConsolidationHunter")

# =====================================================================
# НАСТРОЙКИ
# =====================================================================
BOT_NAME_RENDER = os.getenv("RENDER_SERVICE_NAME", "consol-pulse-default")
TELEGRAM_TOKEN = os.getenv("CONSOL_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CONSOL_CHAT_ID")
PORT = int(os.getenv("PORT", "7862"))

SELF_URL = f"https://{BOT_NAME_RENDER}.onrender.com"

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    logger.error("❌ Не заданы CONSOL_BOT_TOKEN или CONSOL_CHAT_ID. Бот остановлен.")
    sys.exit(1)

# Черный список для мусорных пар, технических индексов и стейблов
BLACKLIST = {"IRISUSDT", "IRYSUSDT", "LUNCUSDT", "USTCUSDT", "USD1USDT", "USDCUSDT"}

# Технические параметры стратегии
MIN_PRICE = 0.0001
MAX_PRICE = 1.0               # Фильтр цены строго до 1.0 USDT
MAX_BOX_RANGE_PCT = 2.0       # Максимальный размах свечи накопления до 2%
VOLUME_X_TRIGGER = 2.5        # Текущий объем незакрытого часа должен быть минимум в 2.5 раза выше среднего

CHECK_INTERVAL = 60           # Проверяем рынок каждую минуту, чтобы поймать 40-60 минуту часа
MAX_CONCURRENT_REQUESTS = 5
ALERT_COOLDOWN = timedelta(hours=4)

# Базовый эндпоинт на фьючерсный API BingX
BINGX_API_URL = "https://open-api.bingx.com/openApi/swap/v2/quote"


class ConsolidationMonitor:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        self.session: aiohttp.ClientSession = None
        self._iteration = 0
        self.current_platform = "BingX" 

    async def init_session(self):
        if self.session is None or self.session.closed:
            headers = {"User-Agent": "Mozilla/5.0"}
            self.session = aiohttp.ClientSession(headers=headers)

    async def send_telegram_signal(self, message: str, reply_markup=None):
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            async with self.session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"Ошибка отправки в TG: {resp.status}")
        except Exception as e:
            logger.error(f"Сбой сети при отправке в TG: {e}")

    async def get_active_symbols(self) -> list:
        url = f"{BINGX_API_URL}/contracts"
        try:
            async with self.session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                raw_symbols = data.get("data", [])

                filtered_symbols = []
                for item in raw_symbols:
                    symbol = item.get("symbol", "") 
                    clean_symbol = symbol.replace("-", "")

                    if not clean_symbol.endswith("USDT") or clean_symbol in BLACKLIST:
                        continue

                    try:
                        price = float(item.get("lastPrice", 0)) if item.get("lastPrice") else MIN_PRICE
                    except ValueError:
                        continue

                    if MIN_PRICE <= price <= MAX_PRICE:
                        filtered_symbols.append(symbol)

                return filtered_symbols
        except Exception as e:
            logger.error(f"Ошибка получения тикеров фьючерсов BingX: {e}")
            return []

    async def check_consolidation_and_pump(self, symbol: str):
        async with self.semaphore:
            # Нам нужна строго 40-60 минута текущего часа
        now_time = datetime.now(datetime.UTC)

            if now_time.minute < 40:
                return

            clean_ticker = symbol.replace("-", "").replace("USDT", "") 

            # Запрашиваем 26 часовых свечей фьючерсов с запасом под лимит 24
            url = f"{BINGX_API_URL}/klines?symbol={symbol}&interval=1h&limit=26"
            try:
                async with self.session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    klines = data.get("data", [])

                    if len(klines) < 10:
                        return

                    # Последняя свеча — текущая НЕЗАКРЫТАЯ
                    current_kline = klines[-1]
                    current_open = float(current_kline.get("open", 0))
                    current_high = float(current_kline.get("high", 0))
                    current_low = float(current_kline.get("low", 0))
                    current_close = float(current_kline.get("close", 0))
                    current_volume_usdt = float(current_kline.get("volume", 0))

                    # Свечи аккумуляции — предыдущие закрытые часы
                    history_klines = klines[:-1]
                    history_klines.reverse()

                    consolidation_candles = []
                    for kline in history_klines:
                        high = float(kline.get("high", 0))
                        low = float(kline.get("low", 0))
                        if low == 0:
                            break
                        candle_range_pct = ((high - low) / low) * 100
                        if candle_range_pct <= MAX_BOX_RANGE_PCT:
                            consolidation_candles.append(kline)
                        else:
                            break

                    # Жесткий фильтр: строго от 5 до 24 свечей накопления
                    detected_period = len(consolidation_candles)
                    if not (5 <= detected_period <= 24):
                        return

                    base_turnovers = [float(k.get("volume", 0)) for k in consolidation_candles]
                    avg_base_turnover = sum(base_turnovers) / len(base_turnovers)
                    if avg_base_turnover == 0:
                        return

                    # Фильтр формы свечи (Наполнение / Отсутствие фитиля сверху)
                    total_range = current_high - current_low
                    if total_range == 0:
                        return
                    
                    ratio = (current_close - current_low) / total_range
                    if ratio < 0.80:
                        return # Монету сливают — игнорируем сигнал

                    base_highs = [float(k.get("high", 0)) for k in consolidation_candles]
                    highest_base_price = max(base_highs)

                    if current_volume_usdt > (avg_base_turnover * VOLUME_X_TRIGGER) and current_close > highest_base_price:
                        now = datetime.now()
                        if self.last_alert_time.get(symbol) and (now - self.last_alert_time[symbol]) < ALERT_COOLDOWN:
                            return

                        self.last_alert_time[symbol] = now

                        reply_markup = {
                            "inline_keyboard": [
                                [
                                    {"text": f"🔄 Мониторинг: {self.current_platform}", "callback_data": f"toggle_platform"},
                                    {"text": "❌ В Чёрный список", "callback_data": f"block_{clean_ticker}USDT"}
                                ]
                            ]
                        }

                        body_pct = int(ratio * 100)
                        wick_pct = 100 - body_pct

                        msg = (
                            f"🔥 **CONSOL_PULSE: ЗАПУСК ТРЕНДА**\n\n"
                            f"🔘 Токен для копирования (нажми): `{clean_ticker}`\n"
                            f"📊 Платформа: `{self.current_platform} Futures`\n\n"
                            f"⏱ Время свечи: `{now_time.minute}-я минута` (незакрытая)\n"
                            f"📈 Текущая цена: `{current_close} USDT`\n"
                            f"⏳ Накопление: `{detected_period} часов подряд` 🕰\n"
                            f"🐳 Объем часа: в `{(current_volume_usdt / avg_base_turnover):.1f} раз` выше среднего\n"
                            f"⚡️ Форма свечи: Наполнение `{body_pct}%` / Фитиль сверху `{wick_pct}%` 🐳\n"
                        )
                        logger.info(f"🔥 НАЙДЕН ПРОБОЙ НЕЗАКРЫТОГО ЧАСА: {clean_ticker}")
                        await self.send_telegram_signal(msg, reply_markup=reply_markup)

            except Exception as e:
                logger.debug(f"Ошибка при проверке {symbol}: {e}")

    def _cleanup_cooldowns(self):
        now = datetime.now()
        expired = [sym for sym, t in self.last_alert_time.items() if (now - t) > ALERT_COOLDOWN * 2]
        for sym in expired:
            del self.last_alert_time[sym]

    async def start_loop(self):
        await self.init_session()
        logger.info("🚀 Бот успешно запущен!")
        await self.send_telegram_signal("🚀 **Бот Consol Pulse запущен! Мониторинг фьючерсов BingX (от 5 до 24 свечей) активен.**")

        while True:
            try:
                self._iteration += 1
                logger.info(f"🔄 Итерация #{self._iteration}: сканирую фьючерсный рынок {self.current_platform}...")

                if self._iteration % 100 == 0:
                    self._cleanup_cooldowns()

                symbols = await self.get_active_symbols()
                if symbols:
                    for i in range(0, len(symbols), MAX_CONCURRENT_REQUESTS):
                        chunk = symbols[i : i + MAX_CONCURRENT_REQUESTS]
                        await asyncio.gather(*[self.check_consolidation_and_pump(s) for s in chunk])
                        await asyncio.sleep(0.2)
            except Exception as e:
                logger.error(f"Ошибка в главном цикле: {e}")
            await asyncio.sleep(CHECK_INTERVAL)


monitor = ConsolidationMonitor()


# ========== ОБРАБОТКА ВЕБХУКОВ ==========

async def handle_index(request):
    return web.Response(text="Бот Consol Pulse работает на фьючерсах BingX! 🚀", status=200)


async def handle_tg_webhook(request):
    try:
        data = await request.json()

        if "callback_query" in data:
            cb = data["callback_query"]
            cb_data = cb.get("data", "")
            cb_id = cb["id"]

            if cb_data == "toggle_platform":
                if monitor.current_platform == "BingX":
                    monitor.current_platform = "Bybit"
                else:
                    monitor.current_platform = "BingX"
                
                async with aiohttp.ClientSession() as sess:
                    await sess.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": f"Переключено на фьючерсы {monitor.current_platform}"}
                    )
                await monitor.send_telegram_signal(f"🔄 Режим сканирования изменен. Текущая целевая биржа: **{monitor.current_platform}**")
                return web.Response(text="OK")

            if cb_data.startswith("block_"):
                sym = cb_data.split("_")[1]
                BLACKLIST.add(sym)

                async with aiohttp.ClientSession() as sess:
                    await sess.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                        json={"callback_query_id": cb_id, "text": f"{sym} в черном списке"}
                    )
                await monitor.send_telegram_signal(f"❌ Фьючерс {sym} добавлен в чёрный список.")
            return web.Response(text="OK")

        if "message" in data and "text" in data["message"]:
            text = data["message"].get("text", "").strip()

            if text == "/status":
                status_msg = (
                    f"📊 **СТАТУС БОТА CONSOL PULSE:**\n\n"
                    f"🎯 Активный мониторинг: `{monitor.current_platform} Futures`\n"
                    f"💲 Макс. цена монеты: `{MAX_PRICE} USDT`\n"
                    f"📦 Затишье: от `5` до `24` свечей (размах до `{MAX_BOX_RANGE_PCT}%`)\n"
                    f"📈 Триггер объема: `x{VOLUME_X_TRIGGER}`\n"
                    f"⏰ Фильтр времени: сканирование только на `40-60 минутах` часа\n"
                    f"🚫 В Чёрном списке: `{len(BLACKLIST)}` монет"
                )
                await monitor.send_telegram_signal(status_msg)
    except Exception as e:
        logger.warning(f"Ошибка в вебхук-обработчике: {e}")
    return web.Response(text="OK")


async def keep_alive_ping():
    await asyncio.sleep(30)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(SELF_URL, timeout=10) as resp:
                    if resp.status == 200:
                        logger.info("📡 Пинг 'keep-alive' успешно отправлен!")
        except Exception as e:
            logger.debug(f"Ошибка пинга анти-сна: {e}")
        await asyncio.sleep(240)


async def main():
    app = web.Application()
    app.router.add_get('/', handle_index)
    app.router.add_post('/tg-webhook', handle_tg_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()

    asyncio.create_task(keep_alive_ping())

    try:
        await monitor.start_loop()
    finally:
        if monitor.session and not monitor.session.closed:
            await monitor.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен.")
