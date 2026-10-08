import asyncio
import aiohttp
from aiohttp import web
import os
import time
import gc
import logging
from html import escape

# ============================================================
# ПАМП-ХАНТЕР v11.4 (Telegram Fix Edition)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Периодика сканирования ---
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "30"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "200"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "150"))

MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "300000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "1.0"))
MIN_LISTING_AGE_DAYS = float(os.environ.get("MIN_LISTING_AGE_DAYS", "14"))

# === Настройки тихого кумулятивного накопления ===
SHELF_LOOKBACK_CANDLES = int(os.environ.get("SHELF_LOOKBACK_CANDLES", "16"))
SHELF_HIST_LOOKBACK = int(os.environ.get("SHELF_HIST_LOOKBACK", "20"))
MAX_SHELF_RANGE_PCT = float(os.environ.get("MAX_SHELF_RANGE_PCT", "2.0"))
MIN_CUMULATIVE_RVOL = float(os.environ.get("MIN_CUMULATIVE_RVOL", "1.8"))
MIN_GREEN_BUY_RATIO = float(os.environ.get("MIN_GREEN_BUY_RATIO", "0.60"))
MAX_DIST_FROM_LOW_PCT = float(os.environ.get("MAX_DIST_FROM_LOW_PCT", "0.8"))
CUMULATIVE_COOLDOWN_SEC = int(os.environ.get("CUMULATIVE_COOLDOWN_SEC", str(4 * 3600)))

# === Трекер Кульминации ===
CLIMAX_MIN_PROFIT_PCT = float(os.environ.get("CLIMAX_MIN_PROFIT_PCT", "2.5"))
CLIMAX_RVOL_THRESHOLD = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "3.5"))
CLIMAX_MIN_WICK_RATIO = float(os.environ.get("CLIMAX_MIN_WICK_RATIO", "0.45"))
CLIMAX_TRACK_TIMEOUT_SEC = int(os.environ.get("CLIMAX_TRACK_TIMEOUT_SEC", str(24 * 3600)))
CLIMAX_INVALIDATE_PCT = float(os.environ.get("CLIMAX_INVALIDATE_PCT", "-3.0"))

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v11.4")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_SIGNAL = {}
ACTIVE_TRACKS = {}

STATS = {
    "scans": 0,
    "cumulative_signals": 0,
    "climax_reversals": 0,
    "tracks_timed_out": 0,
    "tracks_invalidated": 0,
    "rejected_shelf_range": 0,
    "rejected_chasing_highs": 0,
    "rejected_cumulative_rvol": 0,
    "rejected_green_ratio": 0,
    "tg_send_failures": 0,
    "tg_commands_received": 0,
    "tg_poll_errors": 0,
    "tg_poll_idle_cycles": 0,
}


# ============================================================
# HTTP UTILS
# ============================================================

async def http_get(url, params=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None

    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)
                ) as r:
                    if r.status == 429:
                        if attempt < retries:
                            await asyncio.sleep(1.0 * (attempt + 1))
                            continue
                        return None
                    if r.status >= 400:
                        return None
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries:
                await asyncio.sleep(0.4)
        except Exception:
            break
    return None


def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def norm(s):
    if not s:
        return ""
    s = str(s).upper()
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s


# ============================================================
# FETCHERS
# ============================================================

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "open":
            continue
        if str(row.get("settleCurrency", "")).upper() != "USDT":
            continue
        symbol = str(row.get("symbol", "")).upper()
        base = norm(row.get("baseCurrency") or symbol)
        if not base:
            continue
        result[base] = {
            "symbol": symbol,
            "price": num(row.get("lastTradePrice") or row.get("markPrice")),
            "volume24": num(row.get("turnoverOf24h")),
            "change24": num(row.get("priceChgPct")) * 100,
            "first_open_ms": num(row.get("firstOpenDate")),
        }
    return result


async def fetch_bitget_tickers():
    data = await http_get(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        {"productType": "USDT-FUTURES"},
    )
    result = {}
    if not data or data.get("code") != "00000":
        return result
    for row in data.get("data", []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol.endswith("USDT"):
            base = norm(symbol)
            if base:
                result[base] = {"symbol": symbol}
    return result


def _parse_list_candles(rows):
    candles = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = (
                int(row[0]), num(row[1]), num(row[2]),
                num(row[3]), num(row[4]), num(row[5]),
            )
            if o > 0 and c > 0:
                if ts < 10**12:
                    ts *= 1000
                candles.append({
                    "ts": ts, "open": o, "high": h, "low": l,
                    "close": c, "volume": v,
                })
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_kucoin_candles_15m(symbol, limit):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - limit * 15 * 60 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": "15",
        "from": from_ms, "to": now_ms,
    })
    if not data or not isinstance(data.get("data"), list):
        return []
    return _parse_list_candles(data["data"])


# ============================================================
# CORE ALGORITHMS
# ============================================================

def check_cumulative_rvol_accumulation(candles_15m):
    lookback_shelf = SHELF_LOOKBACK_CANDLES
    hist_lookback = SHELF_HIST_LOOKBACK

    if len(candles_15m) < lookback_shelf + hist_lookback + 1:
        return None

    closed_candles = candles_15m[:-1]
    shelf_candles = closed_candles[-lookback_shelf:]

    max_h = max(c["high"] for c in shelf_candles)
    min_l = min(c["low"] for c in shelf_candles)
    if min_l <= 0:
        return None

    shelf_range_pct = ((max_h - min_l) / min_l) * 100
    if shelf_range_pct > MAX_SHELF_RANGE_PCT:
        STATS["rejected_shelf_range"] += 1
        return None

    last_close = shelf_candles[-1]["close"]
    dist_from_low_pct = ((last_close - min_l) / min_l) * 100
    if dist_from_low_pct > MAX_DIST_FROM_LOW_PCT:
        STATS["rejected_chasing_highs"] += 1
        return None

    historical = closed_candles[-(lookback_shelf + hist_lookback):-lookback_shelf]
    avg_hist_vol = sum(c["volume"] for c in historical) / len(historical) if historical else 0
    if avg_hist_vol <= 0:
        return None

    total_shelf_vol = sum(c["volume"] for c in shelf_candles)
    cumulative_rvol = total_shelf_vol / (avg_hist_vol * lookback_shelf)
    if cumulative_rvol < MIN_CUMULATIVE_RVOL:
        STATS["rejected_cumulative_rvol"] += 1
        return None

    green_vols = sum(c["volume"] for c in shelf_candles if c["close"] >= c["open"])
    green_ratio = green_vols / total_shelf_vol if total_shelf_vol > 0 else 0
    if green_ratio < MIN_GREEN_BUY_RATIO:
        STATS["rejected_green_ratio"] += 1
        return None

    entry_price = last_close
    stop_loss = min_l * 0.997

    return {
        "shelf_range_pct": round(shelf_range_pct, 2),
        "dist_from_low_pct": round(dist_from_low_pct, 2),
        "cumulative_rvol": round(cumulative_rvol, 2),
        "green_ratio_pct": round(green_ratio * 100, 1),
        "entry": entry_price,
        "stop_loss": stop_loss,
        "target_3r": entry_price + (entry_price - stop_loss) * 3,
        "target_6r": entry_price + (entry_price - stop_loss) * 6,
    }


def check_climax_reversal(candles, entry_price):
    if len(candles) < 21:
        return None

    current_candle = candles[-1]
    prev_candles = candles[-21:-1]

    avg_volume = sum(c["volume"] for c in prev_candles) / len(prev_candles) if prev_candles else 0
    if avg_volume <= 0:
        return None

    current_rvol = current_candle["volume"] / avg_volume

    open_p = current_candle["open"]
    close_p = current_candle["close"]
    high_p = current_candle["high"]
    low_p = current_candle["low"]

    candle_range = high_p - low_p
    if candle_range <= 0:
        return None

    upper_wick = high_p - max(open_p, close_p)
    wick_ratio = upper_wick / candle_range

    profit_pct = ((high_p - entry_price) / entry_price) * 100

    if (
        profit_pct >= CLIMAX_MIN_PROFIT_PCT
        and current_rvol >= CLIMAX_RVOL_THRESHOLD
        and wick_ratio >= CLIMAX_MIN_WICK_RATIO
    ):
        return {
            "profit_pct": round(profit_pct, 2),
            "climax_rvol": round(current_rvol, 2),
            "wick_ratio_pct": round(wick_ratio * 100, 1),
            "exit_price": close_p,
            "short_stop_loss": high_p * 1.003,
        }

    return None


# ============================================================
# TELEGRAM MESSAGING
# ============================================================

async def send_tg_to(chat_id, text):
    if not BOT_TOKEN or not chat_id or SESSION is None or SESSION.closed:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            payload = await r.json(content_type=None)
            if r.status != 200 or not payload.get("ok", False):
                STATS["tg_send_failures"] += 1
                log.warning(
                    "Telegram send failed: HTTP=%s payload=%s",
                    r.status, str(payload)[:200],
                )
                return False
            return True
    except Exception as e:
        STATS["tg_send_failures"] += 1
        log.warning("Telegram send error: %s", e)
        return False


async def send_tg(text):
    return await send_tg_to(CHAT_ID, text)


async def send_cumulative_signal(base, data):
    ticker = f"<code>{escape(base)}USDT</code>"
    msg = (
        f"🧊 <b>ТИХОЕ НАКОПЛЕНИЕ (Cumulative RVOL)</b>\n\n"
        f"📌 <b>Монета:</b> {ticker}\n"
        f"📐 <b>Ширина полки (4h):</b> <b>{data['shelf_range_pct']}%</b> "
        f"(от дна: {data['dist_from_low_pct']}%)\n"
        f"📊 <b>Кумулятивный RVOL:</b> <b>{data['cumulative_rvol']}x</b>\n"
        f"🟢 <b>Доля выкупа ММ:</b> <b>{data['green_ratio_pct']}%</b>\n\n"
        f"🎯 <b>Вход (Лимитка):</b> <code>{data['entry']:.6g}</code>\n"
        f"🛑 <b>Стоп-лосс:</b> <code>{data['stop_loss']:.6g}</code>\n"
        f"🚀 <b>Тейк 1 (3R):</b> <code>{data['target_3r']:.6g}</code>\n"
        f"🚀 <b>Тейк 2 (6R):</b> <code>{data['target_6r']:.6g}</code>\n\n"
        f"⚠️ <i>Сигнал KuCoin. Взято на сопровождение до климакса/инвалидации.</i>"
    )
    sent = await send_tg(msg)
    if sent:
        STATS["cumulative_signals"] += 1
    return sent


async def send_climax_reversal_signal(base, data):
    ticker = f"<code>{escape(base)}USDT</code>"
    msg = (
        f"🥊 <b>КУЛЬМИНАЦИЯ ПОКУПОК / СИГНАЛ РАЗВОРОТА</b>\n\n"
        f"📌 <b>Монета:</b> {ticker}\n"
        f"💰 <b>Зафиксированный пиковый профит:</b> <b>+{data['profit_pct']}%</b>\n"
        f"📊 <b>Аномальный RVOL выгрузки:</b> <b>{data['climax_rvol']}x</b>\n"
        f"🩸 <b>Верхняя тень (фитиль):</b> <b>{data['wick_ratio_pct']}%</b> "
        f"(ММ обгружает лонги)\n\n"
        f"🛑 <b>ЗАКРЫТЬ ЛОНГ по цене:</b> <code>{data['exit_price']:.6g}</code>\n\n"
        f"📉 <b>РАССМОТРЕТЬ ШОРТ:</b>\n"
        f"  Вход: <code>{data['exit_price']:.6g}</code>\n"
        f"  Стоп-лосс: <code>{data['short_stop_loss']:.6g}</code>"
    )
    sent = await send_tg(msg)
    if sent:
        STATS["climax_reversals"] += 1
    return sent


async def send_status_message(target_chat_id=None):
    uptime_hours = (time.time() - START_TIME) / 3600
    msg = (
        f"📊 <b>Статус PUMP-HUNTER v11.4</b>\n\n"
        f"⏱ Аптайм: {uptime_hours:.1f}ч\n"
        f"🌐 Юниверс: {len(UNIVERSE)} пар\n"
        f"🔍 Сканов: {STATS['scans']}\n"
        f"🧊 Сигналов накопления: {STATS['cumulative_signals']}\n"
        f"🥊 Разворотов (климакс): {STATS['climax_reversals']}\n"
        f"👁 Активных треков: {len(ACTIVE_TRACKS)} "
        f"(тайм-аут: {STATS['tracks_timed_out']}, "
        f"инвалидировано: {STATS['tracks_invalidated']})\n\n"
        f"<b>Причины отсева кандидатов:</b>\n"
        f"  широкая полка (>2%): {STATS['rejected_shelf_range']}\n"
        f"  вход на хаях полки: {STATS['rejected_chasing_highs']}\n"
        f"  слабый RVOL (<1.8x): {STATS['rejected_cumulative_rvol']}\n"
        f"  слабый выкуп (<60%): {STATS['rejected_green_ratio']}\n\n"
        f"<b>Telegram:</b>\n"
        f"  команд принято: {STATS['tg_commands_received']}\n"
        f"  ошибок отправки: {STATS['tg_send_failures']}\n"
        f"  ошибок polling: {STATS['tg_poll_errors']}\n"
        f"  пустых циклов: {STATS['tg_poll_idle_cycles']}"
    )
    dest = target_chat_id or CHAT_ID
    if dest:
        await send_tg_to(dest, msg)


# ============================================================
# TELEGRAM HANDLER + POLLING
# ============================================================

async def handle_telegram_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return

    sender_chat_id = str(msg.get("chat", {}).get("id", ""))
    text = (msg.get("text") or "").strip()

    if not text:
        return

    STATS["tg_commands_received"] += 1
    log.info("📨 TG command from %s: %s", sender_chat_id, text[:60])

    lower = text.lower()

    if lower.startswith("/start"):
        await send_tg_to(
            sender_chat_id,
            "👋 <b>PUMP-HUNTER v11.4 запущен.</b>\n"
            "Команды:\n"
            "/status — полный отчёт по боту",
        )
    elif lower.startswith("/status"):
        await send_tg_to(sender_chat_id, "⏳ Собираю статус...")
        try:
            await send_status_message(target_chat_id=sender_chat_id)
        except Exception as e:
            log.exception("send_status_message failed")
            await send_tg_to(
                sender_chat_id,
                f"⚠️ Ошибка статуса: <code>{escape(str(e))}</code>",
            )
    else:
        await send_tg_to(
            sender_chat_id,
            "❓ Неизвестная команда. Используй /status",
        )


async def telegram_poll_loop():
    if not BOT_TOKEN:
        log.warning("BOT_TOKEN не задан — polling отключён")
        return

    offset = None
    idle_count = 0
    log.info("📡 Поллинг Telegram запущен")

    while True:
        try:
            if SESSION is None or SESSION.closed:
                await asyncio.sleep(2)
                continue

            params = {"timeout": 25}
            if offset is not None:
                params["offset"] = offset

            async with SESSION.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status != 200:
                    body = await r.text()
                    STATS["tg_poll_errors"] += 1
                    log.warning("getUpdates HTTP %s: %s", r.status, body[:300])
                    await asyncio.sleep(3)
                    continue

                data = await r.json(content_type=None)

                # === ЛОГИРУЕМ ЛЮБОЙ НЕУСПЕХ ===
                if not data or not data.get("ok"):
                    STATS["tg_poll_errors"] += 1
                    log.warning(
                        "⚠️ getUpdates not ok: code=%s desc=%s",
                        data.get("error_code") if data else "no-data",
                        str(data.get("description", ""))[:200] if data else "",
                    )
                    await asyncio.sleep(3)
                    continue

                result = data.get("result", [])

                if not result:
                    idle_count += 1
                    STATS["tg_poll_idle_cycles"] += 1
                    if idle_count == 20:
                        log.warning(
                            "⚠️ 20 пустых getUpdates подряд — "
                            "вероятно, конфликт инстансов или Render уснул"
                        )
                        idle_count = 0
                else:
                    idle_count = 0
                    log.info("📬 getUpdates вернул %d апдейтов", len(result))
                    for update in result:
                        offset = update["update_id"] + 1
                        try:
                            await handle_telegram_update(update)
                        except Exception:
                            log.exception("handle_telegram_update failed")

        except asyncio.CancelledError:
            raise
        except Exception:
            STATS["tg_poll_errors"] += 1
            log.exception("Telegram poll error")
            await asyncio.sleep(5)


# ============================================================
# PROCESS SYMBOL
# ============================================================

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return

    kc15 = await fetch_kucoin_candles_15m(
        item["kucoin_symbol"],
        SHELF_LOOKBACK_CANDLES + SHELF_HIST_LOOKBACK + 5,
    )
    if not kc15:
        return

    # === Активный трек: следим за климаксом ===
    if base in ACTIVE_TRACKS:
        track = ACTIVE_TRACKS[base]
        now = time.time()

        if now - track["ts"] > CLIMAX_TRACK_TIMEOUT_SEC:
            del ACTIVE_TRACKS[base]
            STATS["tracks_timed_out"] += 1
            log.info("⏰ Track timed out: %s", base)
            return

        current_price = kc15[-1]["close"]
        change_from_entry = (current_price - track["entry"]) / track["entry"] * 100
        if change_from_entry <= CLIMAX_INVALIDATE_PCT:
            del ACTIVE_TRACKS[base]
            STATS["tracks_invalidated"] += 1
            log.info("🚫 Track invalidated: %s (%.2f%%)", base, change_from_entry)
            return

        climax = check_climax_reversal(kc15, track["entry"])
        if climax:
            await send_climax_reversal_signal(base, climax)
            log.info(
                "🥊 CLIMAX: %s | profit=%+.2f%% | RVOL=%.2fx | wick=%.1f%%",
                base, climax["profit_pct"], climax["climax_rvol"],
                climax["wick_ratio_pct"],
            )
            del ACTIVE_TRACKS[base]

        return

    # === Новые сигналы: ищем накопление ===
    if time.time() - LAST_SIGNAL.get(base, 0) >= CUMULATIVE_COOLDOWN_SEC:
        accum = check_cumulative_rvol_accumulation(kc15)
        if accum:
            LAST_SIGNAL[base] = time.time()
            ACTIVE_TRACKS[base] = {
                "entry": accum["entry"],
                "ts": time.time(),
            }
            await send_cumulative_signal(base, accum)
            log.info(
                "🧊 НАКОПЛЕНИЕ: %s | RVOL %.2fx | Range %.2f%%",
                base, accum["cumulative_rvol"], accum["shelf_range_pct"],
            )


# ============================================================
# BACKGROUND WORKERS
# ============================================================

async def refresh_universe():
    global UNIVERSE, LAST_SIGNAL
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    if not kc or not bg:
        return

    bg_set = set(bg.keys())
    now_ms = time.time() * 1000
    min_age_ms = MIN_LISTING_AGE_DAYS * 86400 * 1000
    uni = {}

    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bg_set:
            continue
        first_open = info.get("first_open_ms", 0)
        if first_open > 0 and (now_ms - first_open) < min_age_ms:
            continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }

    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])

    # Очистка устаревших записей
    LAST_SIGNAL = {k: v for k, v in LAST_SIGNAL.items() if k in UNIVERSE}
    for b in list(ACTIVE_TRACKS.keys()):
        if b not in UNIVERSE:
            ACTIVE_TRACKS.pop(b, None)

    log.info("🌐 Юниверс обновлён: %d пар", len(UNIVERSE))
    gc.collect()


async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    cands = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(15)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
            except Exception as e:
                log.exception("Error %s: %s", base, e)

    await asyncio.gather(*[worker(b) for b in cands])
    gc.collect()


async def _initial_universe_load():
    try:
        await refresh_universe()
        log.info("✅ Начальный universe загружен: %d пар", len(UNIVERSE))
    except Exception:
        log.exception("Initial universe load failed")


async def universe_loop():
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            await refresh_universe()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(10)


async def scan_loop():
    while True:
        try:
            if UNIVERSE:
                await scan_cycle()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("scan_loop error")
        await asyncio.sleep(SCAN_INTERVAL_SEC)


# ============================================================
# WEB SERVER
# ============================================================

async def index(req):
    return web.Response(
        text=(
            f"PUMP-HUNTER v11.4 | Uni: {len(UNIVERSE)} | "
            f"Cumulative: {STATS['cumulative_signals']} | "
            f"Climax: {STATS['climax_reversals']} | "
            f"Tracks: {len(ACTIVE_TRACKS)} | "
            f"TG_fail: {STATS['tg_send_failures']} | "
            f"TG_cmds: {STATS['tg_commands_received']} | "
            f"TG_poll_err: {STATS['tg_poll_errors']}"
        ),
        content_type="text/plain",
    )


async def start(app):
    global SESSION, HTTP_SEMAPHORE, START_TIME
    START_TIME = time.time()
    SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
    )
    HTTP_SEMAPHORE = asyncio.Semaphore(20)

    # === deleteWebhook БЕЗ drop_pending_updates ===
    if BOT_TOKEN:
        try:
            async with SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
                params={"drop_pending_updates": "false"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                log.info("deleteWebhook: HTTP %s", r.status)
        except Exception as e:
            log.warning("deleteWebhook error: %s", e)

    # === СНАЧАЛА polling, чтобы команды не терялись ===
    app["poll_task"] = asyncio.create_task(telegram_poll_loop())
    app["universe_task"] = asyncio.create_task(universe_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["init_task"] = asyncio.create_task(_initial_universe_load())

    await send_tg(
        "🚀 <b>PUMP-HUNTER v11.4 запущен!</b>\n"
        "Кумулятивный RVOL + Защита от хаев + Авто-очистка треков.\n"
        "Команды: /status"
    )


async def stop(app):
    for key in ("universe_task", "scan_task", "poll_task", "init_task"):
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
app.on_startup.append(start)
app.on_cleanup.append(stop)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
