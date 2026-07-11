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
# НАСТРОЙКА ЛОГИРОВАНИЯ (Перенесено наверх ради проверки переменных)
# =====================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ConsolidationHunter")

# =====================================================================
# НАСТРОЙКИ (РЕДАКТИРУЙ ЧЕРЕЗ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ НА RENDER)
# =====================================================================
TELEGRAM_TOKEN = os.getenv("CONSOL_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CONSOL_CHAT_ID")
PORT = int(os.getenv("PORT", "7862"))

# Проверка обязательных переменных (теперь не упадет)
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    logger.error("❌ Не заданы CONSOL_BOT_TOKEN или CONSOL_CHAT_ID. Бот остановлен.")
    sys.exit(1)

# Динамические списки
BLACKLIST = {"IRISUSDT", "IRYSUSDT", "LUNCUSDT", "USTCUSDT"}
WHITELIST = set()

# Технические параметры стратегии (ИДЕАЛЬНАЯ КРИПТО-НОЖКА 1Ч)
MIN_PRICE = 0.0001
MAX_PRICE = 10.0           
MIN_24H_VOLUME = 500000

BOX_PERIOD = 14            # анализируем базу ровно из 14 часовых свечей
MAX_BOX_RANGE_PCT = 2.0    # коридор базы строго не более 2%
VOLUME_X_TRIGGER = 2.5     # часовой объём пробоя в 2.5 раза выше среднего в базе
MIN_PUMP_VOLUME_USDT = 100000  # объём пробойного часа должен быть не менее 100k$

CHECK_INTERVAL = 300       # сканируем рынок раз в 5 минут
MAX_CONCURRENT_REQUESTS = 5
ALERT_COOLDOWN = timedelta(hours=4)  # кулдаун на одну монету

BYBIT_API_URL = "https://api.bybit.com/v5/market"


class ConsolidationMonitor:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        self.session: aiohttp.ClientSession = None
        self._iteration = 0

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
        url = f"{BYBIT_API_URL}/tickers?category=spot"
        try:
            async with self.session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                raw_symbols = data.get("result", {}).get("list", [])

                filtered_symbols = []
                for item in raw_symbols:
                    symbol = item.get("symbol", "")

                    if not symbol.endswith("USDT") or symbol in BLACKLIST:
                        continue
                    if WHITELIST and symbol not in WHITELIST:
                        continue

                    try:
                        price = float(item.get("lastPrice", 0))
                        volume_24h = float(item.get("turnover24h", 0))
                    except ValueError:
                        continue

                    if MIN_PRICE <= price <= MAX_PRICE and volume_24h >= MIN_24H_VOLUME:
                        filtered_symbols.append(symbol)

                return filtered_symbols
        except Exception as e:
            logger.error(f"Ошибка получения тикеров: {e}")
            return []

    async def check_consolidation_and_pump(self, symbol: str):
        async with self.semaphore:
            # Запрашиваем BOX_PERIOD + 2 свечи, чтобы гарантированно иметь закрытый пробойный час
            url = f"{BYBIT_API_URL}/kline?category=spot&symbol={symbol}&interval=60&limit={BOX_PERIOD + 2}"
            try:
                async with self.session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    klines = data.get("result", {}).get("list", [])

                    if len(klines) < BOX_PERIOD + 2:
                        return

                    klines.reverse()

                    # klines[-1] — это текущий НАЧАВШИЙСЯ час (его игнорируем, он не закрыт)
                    # klines[-2] — это последний ПОЛНОСТЬЮ ЗАКРЫТЫЙ час (его проверяем на пробой)
                    trigger_kline = klines[-2]
                    current_volume_usdt = float(trigger_kline[6]) # Индекс 6 - turnover в USDT
                    current_price = float(trigger_kline[4])       # Цена закрытия пробойного часа

                    # Фильтр китов
                    if current_volume_usdt < MIN_PUMP_VOLUME_USDT:
                        return

                    # База строится из свечей ДО пробойной (от klines[-2-BOX_PERIOD] до klines[-2])
                    box_klines = klines[-(BOX_PERIOD + 2):-2]

                    # High, Low, объём в USDT для базы
                    box_highs = [float(k[2]) for k in box_klines]
                    box_lows = [float(k[3]) for k in box_klines]
                    box_turnovers = [float(k[6]) for k in box_klines]

                    highest_box_price = max(box_highs)
                    lowest_box_price = min(box_lows)

                    # Ширина полки крипто-ножки
                    box_range_pct = ((highest_box_price - lowest_box_price) / lowest_box_price) * 100

                    if box_range_pct > MAX_BOX_RANGE_PCT:
                        return

                    avg_box_turnover = sum(box_turnovers) / len(box_turnovers)
                    if avg_box_turnover == 0:
                        return

                    # Пробой верхней границы полки + всплеск объёма
                    if current_volume_usdt > (avg_box_turnover * VOLUME_X_TRIGGER) and current_price > highest_box_price:
                        now = datetime.now()

                        if self.last_alert_time.get(symbol) and (now - self.last_alert_time[symbol]) < ALERT_COOLDOWN:
                            return

                        self.last_alert_time[symbol] = now

                        bingx_fut_url = f"https://bingx.com/ru-ru/futures/forward/{symbol}/"
                        reply_markup = {
                            "inline_keyboard": [
                                [
                                    {"text": "📈 Фьючерсы BingX", "url": bingx_fut_url},
                                    {"text": "❌ В Чёрный список", "callback_data": f"block_{symbol}"}
                                ]
                            ]
                        }

                        msg = (
                            f"🚀 **ОБНАРУЖЕНА КРИПТО-НОЖКА!**\n\n"
                            f"🪙 Монета: **{symbol}**\n"
                            f"💰 Цена пробоя: `{current_price} USDT`\n"
                            f"📉 Длина полки: `{BOX_PERIOD} часов`\n"
                            f"📐 Плотность базы: `{box_range_pct:.2f}%` (строго < 2%)\n"
                            f"📊 Всплеск объёма: в `{(current_volume_usdt / avg_box_turnover):.1f} раз` выше полки!\n"
                            f"🐋 Объём пробойного часа: `${current_volume_usdt:,.0f} USDT` 🐳\n"
                        )
                        logger.info(f"🔥 НАЙДЕНА КРИПТО-НОЖКА: {symbol}")
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
        logger.info("🚀 Бот Крипто-ножки успешно запущен!")
        await self.send_telegram_signal("🚀 **Бот Крипто-ножки запущен! Ищем идеальные узкие полки (до 2%) за 14 часов.**")

        while True:
            try:
                self._iteration += 1
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


# ========== ОБРАБОТКА КОМАНД ==========

# Хэндлер для проверок самого Render (Health Check), убирает 404 ошибки в логах
async def handle_index(request):
    return web.Response(text="Бот Крипто-ножки работает и сканирует рынок! 🚀", status=200)


async def handle_tg_webhook(request):
    try:
        data = await request.json()

        if "callback_query" in data:
            cb = data["callback_query"]
            cb_data = cb.get("data", "")

            if cb_data.startswith("block_"):
                sym = cb_data.split("_")[1]
                BLACKLIST.add(sym)

                if TELEGRAM_TOKEN:
                    async with aiohttp.ClientSession() as sess:
                        await sess.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                            json={"callback_query_id": cb["id"], "text": f"{sym} добавлен в ЧС"}
                        )
                await monitor.send_telegram_signal(f"❌ Монета {sym} добавлена в чёрный список.")
            return web.Response(text="OK")

        if "message" in data and "text" in data["message"]:
            text = data["message"].get("text", "").strip()

            if text == "/status":
                status_msg = (
                    f"📊 **СТАТУС БОТА КРИПТО-НОЖКИ:**\n\n"
                    f"📦 Длина полки: `{BOX_PERIOD} свечей` (до `{MAX_BOX_RANGE_PCT}%`)\n"
                    f"📈 Триггер объёма: `x{VOLUME_X_TRIGGER}`\n"
                    f"🐋 Мин. объём кита: `${MIN_PUMP_VOLUME_USDT:,}`\n"
                    f"🚫 В Чёрном списке: `{len(BLACKLIST)}` монет"
                )
                await monitor.send_telegram_signal(status_msg)
    except Exception as e:
        logger.warning(f"Ошибка в вебхук-обработчике: {e}")
    return web.Response(text="OK")


async def main():
    app = web.Application()
    app.router.add_get('/', handle_index)  # Обрабатываем корень для Render
    app.router.add_post('/tg-webhook', handle_tg_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()

    try:
        await monitor.start_loop()
    finally:
        if monitor.session and not monitor.session.closed:
            await monitor.session.close()
            logger.info("Сессия aiohttp закрыта.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
