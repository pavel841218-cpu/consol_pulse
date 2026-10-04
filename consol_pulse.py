import asyncio
import logging
import os
import time
from collections import deque
from typing import Dict, List, Optional, Set

import aiohttp
from aiohttp import web

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
PORT = int(os.getenv("PORT", "10000"))

OKX_BASE_URL = "https://www.okx.com"
BITGET_BASE_URL = "https://api.bitget.com"
BYBIT_BASE_URL = "https://api.bybit.com"

SCAN_INTERVAL_SEC = int(os.getenv("SCAN_INTERVAL_SEC", "40"))
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "20"))

IMPULSE_BASE_CANDLES = int(os.getenv("IMPULSE_BASE_CANDLES", "32"))
SPARK_MIN_PCT = float(os.getenv("SPARK_MIN_PCT", "1.8"))
SCORE_SIGNAL_THRESHOLD = float(os.getenv("SCORE_SIGNAL_THRESHOLD", "45"))
SCORE_WATCH_THRESHOLD = float(os.getenv("SCORE_WATCH_THRESHOLD", "30"))

MIN_CONFIRMATIONS = int(os.getenv("MIN_CONFIRMATIONS", "1"))
CONFIRM_THRESHOLD_PCT = float(os.getenv("CONFIRM_THRESHOLD_PCT", "1.0"))
SIGNAL_COOLDOWN_SEC = int(os.getenv("SIGNAL_COOLDOWN_SEC", "900"))

# --- Оптимизация хранения истории ---
# Цена/объём: не перекачиваем все 60 свечей каждый цикл — держим скользящее
# окно в памяти и на каждом скане дотягиваем только 2-3 последние свечи.
CANDLE_HISTORY_LEN = int(os.getenv("CANDLE_HISTORY_LEN", "60"))  # 40-60 свечей по 15м = 10-15ч
CANDLE_REFRESH_LIMIT = 3  # сколько последних свечей дотягиваем на обычном скане

# OI: точечная история за последние 4-12ч, с обязательной очисткой старых точек.
OI_SAMPLE_INTERVAL_SEC = int(os.getenv("OI_SAMPLE_INTERVAL_SEC", "300"))
OI_HIST_HOURS = float(os.getenv("OI_HIST_HOURS", "6"))           # окно хранения, 4-12ч
OI_WINDOW_HOURS = float(os.getenv("OI_WINDOW_HOURS", "2.0"))     # окно расчёта роста OI для score
OI_MIN_GROWTH_PCT = float(os.getenv("OI_MIN_GROWTH_PCT", "8.0"))

WATCHLIST: Dict[str, Set[int]] = {}
LAST_SIGNAL: Dict[str, float] = {}

# base_symbol -> deque[candle] — скользящее окно цены/объёма (OKX, 15м)
CANDLE_HISTORY: Dict[str, deque] = {}

# base_symbol -> exchange -> deque[(ts, oi_value)] — точечная история OI,
# очищается от устаревших точек на каждом сэмплинге (а не только "один раз при рестарте" —
# рестарт и так обнуляет память, важнее не дать деку расти бесконечно во время работы)
OI_HIST_MAXLEN = int(OI_HIST_HOURS * 3600 / OI_SAMPLE_INTERVAL_SEC) + 10
OI_HISTORY: Dict[str, Dict[str, deque]] = {}

TICKERS_CACHE: List[str] = []

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("okx-pump-hunter")

SESSION: Optional[aiohttp.ClientSession] = None
STATS = {"scans": 0, "signals": 0}

# ==========================================
# API КЛИЕНТЫ БИРЖ (OKX, BITGET, BYBIT)
# ==========================================

async def fetch_okx_candles(inst_id: str, bar: str = "15m", limit: int = 100) -> List[dict]:
    url = f"{OKX_BASE_URL}/api/v5/market/candles"
    params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json(content_type=None)
            if data.get("code") == "0" and data.get("data"):
                raw = data["data"]
                candles = []
                for c in reversed(raw):
                    candles.append({
                        "ts": int(c[0]) // 1000,
                        "open": float(c[1]), "high": float(c[2]),
                        "low": float(c[3]), "close": float(c[4]), "volume": float(c[5]),
                    })
                return candles
    except Exception:
        pass
    return []


async def fetch_bitget_metrics(symbol: str) -> Optional[dict]:
    bg_symbol = f"{symbol}USDT"
    url = f"{BITGET_BASE_URL}/api/v2/mix/market/candles"
    params = {"symbol": bg_symbol, "productType": "USDT-FUTURES", "granularity": "15m", "limit": "2"}
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            data = await resp.json(content_type=None)
            if data.get("code") == "00000" and data.get("data"):
                c = data["data"][-1]
                open_p, close_p = float(c[1]), float(c[4])
                pct = ((close_p - open_p) / open_p) * 100
                return {"pct": pct, "close": close_p}
    except Exception:
        pass
    return None


async def fetch_bybit_metrics(symbol: str) -> Optional[dict]:
    bb_symbol = f"{symbol}USDT"
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    params = {"category": "linear", "symbol": bb_symbol, "interval": "15", "limit": "2"}
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            data = await resp.json(content_type=None)
            if data.get("retCode") == 0 and data["result"]["list"]:
                c = data["result"]["list"][0]
                open_p, close_p = float(c[1]), float(c[4])
                pct = ((close_p - open_p) / open_p) * 100
                return {"pct": pct, "close": close_p}
    except Exception:
        pass
    return None


async def fetch_okx_oi(inst_id: str) -> float:
    url = f"{OKX_BASE_URL}/api/v5/public/open-interest"
    params = {"instType": "SWAP", "instId": inst_id}
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json(content_type=None)
            if data.get("code") == "0" and data.get("data"):
                row = data["data"][0]
                val = row.get("oiCcy") or row.get("oi")
                return float(val) if val is not None else 0.0
    except Exception:
        pass
    return 0.0


async def fetch_bitget_oi(symbol: str) -> float:
    url = f"{BITGET_BASE_URL}/api/v2/mix/market/open-interest"
    params = {"symbol": f"{symbol}USDT", "productType": "USDT-FUTURES"}
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json(content_type=None)
            if data.get("code") != "00000":
                return 0.0
            raw = data.get("data")
            row = {}
            if isinstance(raw, dict):
                items = raw.get("openList") or raw.get("openInterestList") or raw.get("list")
                row = items[0] if isinstance(items, list) and items else raw
            elif isinstance(raw, list) and raw:
                row = raw[0]
            val = row.get("openInterest") or row.get("amount") or row.get("size")
            return float(val) if val is not None else 0.0
    except Exception:
        pass
    return 0.0


async def fetch_bybit_oi(symbol: str) -> float:
    url = f"{BYBIT_BASE_URL}/v5/market/open-interest"
    params = {"category": "linear", "symbol": f"{symbol}USDT", "intervalTime": "1h", "limit": 1}
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json(content_type=None)
            if data.get("retCode") != 0:
                return 0.0
            items = data.get("result", {}).get("list")
            if not items:
                return 0.0
            val = items[0].get("openInterest")
            return float(val) if val is not None else 0.0
    except Exception:
        pass
    return 0.0


def prune_oi_history(now: float):
    """Обязательная очистка устаревших точек — не даём деку расти бесконечно."""
    cutoff = now - OI_HIST_HOURS * 3600
    for exch_map in OI_HISTORY.values():
        for dq in exch_map.values():
            while dq and dq[0][0] < cutoff:
                dq.popleft()


def oi_growth_pct(base: str, exchange: str, window_hours: float) -> Optional[float]:
    dq = OI_HISTORY.get(base, {}).get(exchange)
    if not dq or len(dq) < 2:
        return None
    now_ts, now_oi = dq[-1]
    min_span = max(OI_SAMPLE_INTERVAL_SEC * 1.5, min(window_hours * 3600 * 0.3, OI_SAMPLE_INTERVAL_SEC * 6))
    if now_ts - dq[0][0] < min_span:
        return None
    target_ts = time.time() - window_hours * 3600
    past = None
    for ts, val in dq:
        if ts <= target_ts:
            past = (ts, val)
        else:
            break
    if past is None:
        past = dq[0]
    past_ts, past_oi = past
    if past_oi <= 0:
        return None
    return ((now_oi / past_oi) - 1) * 100


async def sample_oi_for_symbol(base: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            okx_oi, bg_oi, bb_oi = await asyncio.gather(
                fetch_okx_oi(f"{base}-USDT-SWAP"),
                fetch_bitget_oi(base),
                fetch_bybit_oi(base),
            )
        except Exception:
            return
        now = time.time()
        exch_map = OI_HISTORY.setdefault(base, {"okx": deque(maxlen=OI_HIST_MAXLEN),
                                                  "bitget": deque(maxlen=OI_HIST_MAXLEN),
                                                  "bybit": deque(maxlen=OI_HIST_MAXLEN)})
        if okx_oi > 0:
            exch_map["okx"].append((now, okx_oi))
        if bg_oi > 0:
            exch_map["bitget"].append((now, bg_oi))
        if bb_oi > 0:
            exch_map["bybit"].append((now, bb_oi))


async def oi_sample_cycle():
    if not TICKERS_CACHE:
        return
    sem = asyncio.Semaphore(15)
    bases = [t.replace("-USDT-SWAP", "") for t in TICKERS_CACHE]
    await asyncio.gather(*[sample_oi_for_symbol(b, sem) for b in bases])
    prune_oi_history(time.time())


async def oi_loop():
    while True:
        try:
            await oi_sample_cycle()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Ошибка сэмплинга OI")
        await asyncio.sleep(OI_SAMPLE_INTERVAL_SEC)


async def fetch_okx_tickers() -> List[str]:
    url = f"{OKX_BASE_URL}/api/v5/market/tickers?instType=SWAP"
    try:
        async with SESSION.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            data = await resp.json(content_type=None)
            if data.get("code") == "0":
                return [item["instId"] for item in data["data"] if item["instId"].endswith("-USDT-SWAP")]
    except Exception as e:
        log.error(f"Ошибка тикеров OKX: {e}")
    return []

# ==========================================
# РАСЧЁТ МЕТРИК
# ==========================================

def calc_kaufman_efficiency(candles: List[dict]) -> float:
    if len(candles) < 2:
        return 0.0
    net_change = abs(candles[-1]["close"] - candles[0]["open"])
    sum_abs_changes = sum(abs(candles[i]["close"] - candles[i - 1]["close"]) for i in range(1, len(candles)))
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
    return {"close": current["close"], "pct": pct_change, "rvol": rvol,
            "breakout": breakout, "base_high": base_high, "er": er}

# ==========================================
# ОСНОВНАЯ ЛОГИКА
# ==========================================

def merge_candles(history: deque, new_candles: List[dict]):
    for c in new_candles:
        if history and history[-1]["ts"] == c["ts"]:
            history[-1] = c  # обновляем ещё формирующуюся/только что закрытую свечу
        else:
            history.append(c)


async def get_cached_candles(inst_id: str, base_symbol: str) -> List[dict]:
    history = CANDLE_HISTORY.setdefault(base_symbol, deque(maxlen=CANDLE_HISTORY_LEN))
    if not history:
        # первый раз видим символ — набираем полное окно
        full = await fetch_okx_candles(inst_id, bar="15m", limit=CANDLE_HISTORY_LEN)
        for c in full:
            history.append(c)
    else:
        # дальше — только последние пара свечей, не весь массив
        fresh = await fetch_okx_candles(inst_id, bar="15m", limit=CANDLE_REFRESH_LIMIT)
        merge_candles(history, fresh)
    return list(history)


async def process_symbol(inst_id: str, sem: asyncio.Semaphore):
    async with sem:
        base_symbol = inst_id.replace("-USDT-SWAP", "")
        candles = await get_cached_candles(inst_id, base_symbol)
        if not candles:
            return

        okx_m = analyze_candles(candles, IMPULSE_BASE_CANDLES)
        if not okx_m:
            return

        if not okx_m["breakout"] and okx_m["pct"] < SPARK_MIN_PCT:
            return

        bg_m, bb_m = await asyncio.gather(
            fetch_bitget_metrics(base_symbol),
            fetch_bybit_metrics(base_symbol),
        )

        confirmations = sum(1 for m in (bg_m, bb_m) if m and m["pct"] >= CONFIRM_THRESHOLD_PCT)
        if confirmations < MIN_CONFIRMATIONS:
            return

        oi_deltas = {exch: oi_growth_pct(base_symbol, exch, OI_WINDOW_HOURS) for exch in ("okx", "bitget", "bybit")}
        best_oi_delta = 0.0
        is_squeeze = False
        for d in oi_deltas.values():
            if d is not None and abs(d) > abs(best_oi_delta):
                best_oi_delta = d
        if best_oi_delta < 0 and abs(best_oi_delta) >= OI_MIN_GROWTH_PCT:
            is_squeeze = True
        oi_score = min(15.0, (abs(best_oi_delta) / OI_MIN_GROWTH_PCT) * 10.0) if OI_MIN_GROWTH_PCT > 0 else 0.0

        score = 0.0
        if okx_m["breakout"]:
            score += 30.0
        score += min(okx_m["pct"] * 7, 30)
        score += min(okx_m["rvol"] * 4, 20)
        score += okx_m["er"] * 10
        score += confirmations * 10
        score += oi_score

        if score < SCORE_WATCH_THRESHOLD:
            return

        now = time.time()
        if now - LAST_SIGNAL.get(base_symbol, 0) < SIGNAL_COOLDOWN_SEC:
            return

        LAST_SIGNAL[base_symbol] = now
        is_watch = score < SCORE_SIGNAL_THRESHOLD
        STATS["signals"] += 1
        await send_okx_signal(base_symbol, inst_id, okx_m, bg_m, bb_m, score, is_watch, oi_deltas, is_squeeze)

# ==========================================
# TELEGRAM (чистый HTTP, без aiogram)
# ==========================================

def get_signal_keyboard(inst_id: str) -> dict:
    return {
        "inline_keyboard": [[
            {"text": "👀 Следить", "callback_data": f"watch_{inst_id}"},
            {"text": "📊 OKX Chart", "url": f"https://www.okx.com/trade-swap/{inst_id.lower()}"},
        ]]
    }


async def send_tg_message(text: str, reply_markup: Optional[dict] = None) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID or SESSION is None:
        return False
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json=payload, timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return False
            data = await r.json(content_type=None)
            return data.get("ok", False)
    except Exception as e:
        log.error(f"Ошибка отправки TG: {e}")
        return False


async def answer_callback_query(callback_id: str, text: str, show_alert: bool = True):
    if not TG_BOT_TOKEN or SESSION is None:
        return
    try:
        await SESSION.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text, "show_alert": show_alert},
            timeout=aiohttp.ClientTimeout(total=8),
        )
    except Exception:
        pass


async def send_okx_signal(symbol: str, inst_id: str, okx_m: dict, bg_m: Optional[dict],
                           bb_m: Optional[dict], score: float, is_watch: bool,
                           oi_deltas: Optional[dict] = None, is_squeeze: bool = False):
    status_tag = "👁 <b>НАБЛЮДЕНИЕ</b>" if is_watch else "⚡ <b>ИМПУЛЬС (OKX)</b>"
    bg_str = f"{bg_m['pct']:+.2f}%" if bg_m else "N/A"
    bb_str = f"{bb_m['pct']:+.2f}%" if bb_m else "N/A"

    oi_deltas = oi_deltas or {}

    def _oi(val):
        if val is None:
            return "нет данных"
        tag = " (шорт-сквиз)" if val <= -OI_MIN_GROWTH_PCT else ""
        return f"{val:+.1f}%{tag}"

    msg = (
        f"{status_tag}\n\n"
        f"<b>Монета:</b> #{symbol}\n"
        f"<b>Score:</b> {score:.1f}/100\n"
        f"<b>OKX 15m:</b> {okx_m['pct']:+.2f}%\n"
        f"<b>Bitget / Bybit:</b> {bg_str} | {bb_str}\n"
        f"<b>RVOL:</b> {okx_m['rvol']:.2f}x | <b>ER:</b> {okx_m['er']:.2f}\n"
        f"<b>Статус базы:</b> {'✅ Пробой' if okx_m['breakout'] else '⚠️ Подход к хаю'}\n"
        f"<b>Уровень хая:</b> {okx_m['base_high']}\n\n"
        f"<b>📊 OI Δ ({OI_WINDOW_HOURS:g}ч)</b>\n"
        f"  OKX: {_oi(oi_deltas.get('okx'))}\n"
        f"  Bitget: {_oi(oi_deltas.get('bitget'))}\n"
        f"  Bybit: {_oi(oi_deltas.get('bybit'))}\n"
    )
    if is_squeeze:
        msg += "\n⚠️ Часть роста OI отрицательна — похоже на шорт-сквиз."
    await send_tg_message(msg, reply_markup=get_signal_keyboard(inst_id))


async def handle_telegram_update(update: dict):
    cq = update.get("callback_query")
    if cq and cq.get("data", "").startswith("watch_"):
        inst_id = cq["data"].split("watch_", 1)[1]
        user_id = cq.get("from", {}).get("id")
        WATCHLIST.setdefault(inst_id, set())
        if user_id is not None:
            WATCHLIST[inst_id].add(user_id)
        await answer_callback_query(cq["id"], f"✅ {inst_id} добавлен в твой список наблюдения!")
        return

    msg = update.get("message")
    if not msg:
        return
    chat_id = str(msg.get("chat", {}).get("id", ""))
    if TG_CHAT_ID and chat_id != str(TG_CHAT_ID):
        return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/start"):
        await send_tg_message("👋 <b>OKX Pump Hunter запущен.</b>")
    elif text.startswith("/status"):
        await send_tg_message(f"📊 Сканов: {STATS['scans']} | Сигналов: {STATS['signals']}")


async def telegram_poll_loop():
    if not TG_BOT_TOKEN:
        return
    offset = None
    while True:
        try:
            params = {"timeout": 25}
            if offset is not None:
                params["offset"] = offset
            async with SESSION.get(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates",
                params=params, timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                data = await r.json(content_type=None) if r.status == 200 else None
            if data and data.get("ok"):
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    try:
                        await handle_telegram_update(update)
                    except Exception:
                        log.exception("Ошибка обработки апдейта TG")
            else:
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Ошибка поллинга TG")
            await asyncio.sleep(5)

# ==========================================
# СКАНЕР
# ==========================================

async def scanner_loop():
    global TICKERS_CACHE
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    while True:
        try:
            tickers = await fetch_okx_tickers()
            if tickers:
                TICKERS_CACHE = tickers
                STATS["scans"] += 1
                await asyncio.gather(*[process_symbol(t, sem) for t in tickers])
        except Exception as e:
            log.error(f"Ошибка цикла сканера: {e}")
        await asyncio.sleep(SCAN_INTERVAL_SEC)

# ==========================================
# ВЕБ-СЕРВЕР (нужен Render'у, чтобы не убить деплой по таймауту порта)
# ==========================================

async def index(request):
    return web.Response(text=f"OKX PUMP HUNTER | scans={STATS['scans']} signals={STATS['signals']}",
                         content_type="text/plain")


async def on_startup(app):
    global SESSION
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    app["scanner_task"] = asyncio.create_task(scanner_loop())
    app["poll_task"] = asyncio.create_task(telegram_poll_loop())
    app["oi_task"] = asyncio.create_task(oi_loop())
    log.info("🚀 Мультибиржевой PUMP-HUNTER OKX запущен (веб-сервис)!")
    await send_tg_message("🚀 <b>OKX Pump Hunter запущен</b> (веб-сервис).")


async def on_cleanup(app):
    for key in ("scanner_task", "poll_task", "oi_task"):
        t = app.get(key)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    if SESSION and not SESSION.closed:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
