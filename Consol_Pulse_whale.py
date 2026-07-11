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

# Динамические списки (теперь их можно менять кнопками и командами)
BLACKLIST = {"IRISUSDT", "IRYSUSDT", "LUNCUSDT", "USTCUSDT"}
WHITELIST = set() 

# Технические параметры стратегии
MIN_PRICE = 0.0001       
MAX_PRICE = 1.0          
MIN_24H_VOLUME = 500000  
BOX_PERIOD = 30          
MAX_BOX_RANGE_PCT = 1.5  
VOLUME_X_TRIGGER = 3.5   

CHECK_INTERVAL = 15      
MAX_CONCURRENT_REQUESTS = 5  
ALERT_COOLDOWN = timedelta(minutes=5)  

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
            url = f"{BYBIT_API_URL}/kline?category=spot&symbol={symbol}&interval=1&limit={BOX_PERIOD + 1}"
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
                        
                        # Кнопки под сигналом: Фьючерсы BingX + быстрый ЧС
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
                            f"🚨 **ПРОБОЙ КОНСОЛИДАЦИИ!**\n\n"
                            f"🪙 Монета: #{symbol}\n"
                            f"💰 Текущая цена: `{current_price} USDT`\n"
                            f"📉 Ширина флэта: `{box_range_pct:.2f}%` за {BOX_PERIOD} мин\n"
                            f"📊 Всплеск объема: в `{(current_volume / avg_box_volume):.1f} раз` выше среднего!\n"
                        )
                        logger.info(f"🔥 СИГНАЛ: {symbol}")
                        await self.send_telegram_signal(msg, reply_markup=reply_markup)
                        
            except Exception as e:
                pass

    async def start_loop(self):
        await self.init_session()
        logger.info("🚀 Бот Консолидации успешно запущен на Render!")
        await self.send_telegram_signal("🚀 **Бот консолидации запущен на Render и готов к работе!**")

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


# ========== ОБРАБОТКА КОМАНД И КНОПОК ИЗ TG (WEBHOOK) ==========
async def handle_tg_webhook(request):
    try:
        data = await request.json()
        
        # 1. Обработка нажатий на инлайн-кнопки (Callback Queries)
        if "callback_query" in data:
            cb = data["callback_query"]
            cb_data = cb.get("data", "")
            chat_id = str(cb["message"]["chat"]["id"])
            
            if cb_data.startswith("block_"):
                sym = cb_data.split("_")[1]
                BLACKLIST.add(sym)
                if sym in WHITELIST:
                    WHITELIST.remove(sym)
                    
                # Отвечаем в ТГ, чтобы убрать часики на кнопке
                async with aiohttp.ClientSession() as sess:
                    await sess.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery", 
                                    json={"callback_query_id": cb["id"], "text": f"{sym} добавлен в ЧС"})
                
                await monitor.send_telegram_signal(f"❌ Монета #{sym} добавлена в чёрный список и удалена из сканирования.")
            return web.Response(text="OK")

        # 2. Обработка текстовых команд
        if "message" in data and "text" in data["message"]:
            msg = data["message"]
            text = msg.get("text", "").strip()
            
            if text == "/status":
                status_msg = (
                    f"📊 **ТЕКУЩИЙ СТАТУС БОТА:**\n\n"
                    f"⚙️ Фильтры цены: `{MIN_PRICE}` - `{MAX_PRICE} USDT`\n"
                    f"📦 Период коробки: `{BOX_PERIOD} мин` (макс. `{MAX_BOX_RANGE_PCT}%`)\n"
                    f"📈 Триггер объема: `x{VOLUME_X_TRIGGER}`\n"
                    f"🚫 В Чёрном списке: `{len(BLACKLIST)}` монет\n"
                    f"🏳️ В Белом списке: `{len(WHITELIST) if WHITELIST else 'Выключен (весь рынок)'}`"
                )
                await monitor.send_telegram_signal(status_msg)
                
            elif text.startswith("/add_black "):
                sym = text.split(" ")[1].upper().strip()
                if not sym.endswith("USDT"): sym += "USDT"
                BLACKLIST.add(sym)
                await monitor.send_telegram_signal(f"❌ {sym} добавлен в чёрный список.")
                
            elif text.startswith("/del_black "):
                sym = text.split(" ")[1].upper().strip()
                if not sym.endswith("USDT"): sym += "USDT"
                BLACKLIST.discard(sym)
                await monitor.send_telegram_signal(f"✅ {sym} удален из чёрного списка.")
                
            elif text == "/clear_black":
                BLACKLIST.clear()
                await monitor.send_telegram_signal("🧹 Чёрный список полностью очищен.")

    except Exception as e:
        logger.error(f"Ошибка вебхука: {e}")
        
    return web.Response(text="OK")


# ========== АНТИ-СОН И СЛУЖЕБНЫЕ ЭНДПОИНТЫ ==========
async def web_health_check(request):
    return web.Response(text="OK")

async def keep_alive_ping():
    await asyncio.sleep(20)
    # Автоматическая регистрация Вебхука в Telegram при старте
    webhook_url = f"{SELF_URL}/tg-webhook"
    async with aiohttp.ClientSession() as sess:
        await sess.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook", json={"url": webhook_url})
    logger.info(f"Установлен Webhook на адрес: {webhook_url}")

    while True:
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            async with aiohttp.ClientSession(headers=headers) as ping_session:
                async with ping_session.get(SELF_URL, timeout=5.0) as resp:
                    pass
        except Exception as e:
            logger.error(f"Ошибка самопинга: {e}")
        await asyncio.sleep(240)


async def main():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.critical("Критическая ошибка: Токены не заданы в Environment!")
        sys.exit(1)

    app = web.Application()
    app.router.add_get('/', web_health_check)
    app.router.add_post('/tg-webhook', handle_tg_webhook)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Сервер слушает порт {PORT}")

    asyncio.create_task(monitor.start_loop())
    asyncio.create_task(keep_alive_ping())

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
