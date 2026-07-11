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
# НАСТРОЙКИ (РЕДАКТИРУЙ ЧЕРЕЗ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ НА RENDER)
# =====================================================================
BOT_NAME_RENDER = os.getenv("RENDER_SERVICE_NAME", "consolidation-hunter")
TELEGRAM_TOKEN = os.getenv("CONSOL_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CONSOL_CHAT_ID")
PORT = int(os.getenv("PORT", "7862"))

# Динамические списки
BLACKLIST = {"IRISUSDT", "IRYSUSDT", "LUNCUSDT", "USTCUSDT"}
WHITELIST = set() 

# Технические параметры стратегии (ГЛОБАЛЬНЫЙ ПРОБОЙ БАЗЫ 1Ч)
MIN_PRICE = 0.0001       
MAX_PRICE = 1.0          
MIN_24H_VOLUME = 500000  
BOX_PERIOD = 24            # Анализируем базу за последние 24 часа
MAX_BOX_RANGE_PCT = 5.0    # База должна быть узкой (диапазон макс/мин цены до 5%)
VOLUME_X_TRIGGER = 2.5     # Часовой объем пробоя в 2.5 раза выше среднего в базе
MIN_PUMP_VOLUME_USDT = 100000  # КИТЫ: Объем пробойного часа должен быть не менее 100k$

CHECK_INTERVAL = 300       # Сканируем рынок раз в 5 минут
MAX_CONCURRENT_REQUESTS = 5  
ALERT_COOLDOWN = timedelta(hours=4)  # Кулдаун на одну монету

BYBIT_API_URL = "https://api.bybit.com/v5/market"
SELF_URL = f"https://{BOT_NAME_RENDER}.onrender.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ConsolidationHunter")


class ConsolidationMonitor:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        self.session: aiohttp.ClientSession = None

    async def init_session(self):
        if not self.session or self.session.closed:
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
            url = f"{BYBIT_API_URL}/kline?category=spot&symbol={symbol}&interval=60&limit={BOX_PERIOD + 1}"
            try:
                async with self.session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    klines = data.get("result", {}).get("list", [])
                    
                    if len(klines) < BOX_PERIOD + 1:
                        return
                    
                    klines.reverse()
                    
                    current_kline = klines[-1]
                    current_volume = float(current_kline[5]) 
                    current_price = float(current_kline[4])  
                    
                    # Считаем текущий объем в USDT (цена * количество монет)
                    current_volume_usdt = current_volume * current_price
                    
                    # Фильтр китов: если объем пробойного часа слишком мал, пропускаем
                    if current_volume_usdt < MIN_PUMP_VOLUME_USDT:
                        return
                    
                    box_klines = klines[:-1]
                    box_prices = [float(k[4]) for k in box_klines]
                    box_volumes = [float(k[5]) for k in box_klines]
                    
                    highest_box_price = max(box_prices)
                    lowest_box_price = min(box_prices)
                    
                    box_range_pct = ((highest_box_price - lowest_box_price) / lowest_box_price) * 100
                    
                    if box_range_pct > MAX_BOX_RANGE_PCT:
                        return
                        
                    avg_box_volume = sum(box_volumes) / len(box_volumes)
                    if avg_box_volume == 0:
                        return
                        
                    if current_volume > (avg_box_volume * VOLUME_X_TRIGGER) and current_price > highest_box_price:
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
                            f"🚀 **ГЛОБАЛЬНЫЙ ПРОБОЙ БАЗЫ (1Ч)!**\n\n"
                            f"🪙 Монета: **{symbol}**\n"
                            f"💰 Текущая цена: `{current_price} USDT`\n"
                            f"📉 Период базы: `{BOX_PERIOD} часов`\n"
                            f"📐 Ширина коридора: `{box_range_pct:.2f}%`\n"
                            f"📊 Всплеск объема: в `{(current_volume / avg_box_volume):.1f} раз` выше среднего за сутки!\n"
                            f"🐋 Объем пробоя (1ч): `${current_volume_usdt:,.0f} USDT` (Зашел Кит!)\n"
                        )
                        logger.info(f"🔥 СИГНАЛ БАЗЫ: {symbol}")
                        await self.send_telegram_signal(msg, reply_markup=reply_markup)
                        
            except Exception as e:
                pass

    async def start_loop(self):
        await self.init_session()
        logger.info("🚀 Бот Консолидации (1Ч) успешно запущен!")
        await self.send_telegram_signal("🚀 **Бот часовых консолидаций запущен! Ищем крупные базы с объемами китов.**")

        while True:
            try:
                symbols = await self.get_active_symbols()
                if symbols:
                    for i in range(0, len(symbols), MAX_CONCURRENT_REQUESTS):
                        chunk = symbols[i:i+MAX_CONCURRENT_REQUESTS]
                        await asyncio.gather(*[self.check_consolidation_and_pump(s) for s in chunk])
                        await asyncio.sleep(0.2) 
            except Exception as e:
                logger.error(f"Ошибка в главном цикле: {e}")
            await asyncio.sleep(CHECK_INTERVAL)


monitor = ConsolidationMonitor()


# ========== ОБРАБОТКА КОМАНД ==========
async def handle_tg_webhook(request):
    try:
        data = await request.json()
        
        if "callback_query" in data:
            cb = data["callback_query"]
            cb_data = cb.get("data", "")
            
            if cb_data.startswith("block_"):
                sym = cb_data.split("_")[1]
                BLACKLIST.add(sym)
                
                async with aiohttp.ClientSession() as sess:
                    await sess.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery", 
                                    json={"callback_query_id": cb["id"], "text": f"{sym} добавлен в ЧС"})
                
                await monitor.send_telegram_signal(f"❌ Монета {sym} добавлена в чёрный список.")
            return web.Response(text="OK")

        if "message" in data and "text" in data["message"]:
            text = data["message"].get("text", "").strip()
            
            if text == "/status":
                status_msg = (
                    f"📊 **СТАТУС БОТА (1Ч):**\n\n"
                    f"📦 База: `{BOX_PERIOD} часов` (до `{MAX_BOX_RANGE_PCT}%`)\n"
                    f"📈 Триггер объема: `x{VOLUME_X_TRIGGER}`\n"
                    f"🐋 Мин. часовой объем кита: `${MIN_PUMP_VOLUME_USDT}`\n"
                    f"🚫 В Чёрном списке: `{len(BLACKLIST)}` монет"
                )
                await monitor.send_telegram_signal(status_msg)
    except:
        pass
    return web.Response(text="OK")


async def main():
    app = web.Application()
    app.router.add_post('/tg-webhook', handle_tg_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    await monitor.start_loop()

if __name__ == "__main__":
    asyncio.run(main())
