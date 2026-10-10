import asyncio
import aiohttp
from aiohttp import web
import os
import time
import gc
import logging
from html import escape
from collections import defaultdict

# ============================================================
# IMPULSE HUNTER v13.3 — TEST MODE (low traffic + logging)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Периодика (трафик-оптимизированная) ---
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "60"))
CHECK_ROTATION_SEC = int(os.environ.get("CHECK_ROTATION_SEC", "300"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "1800"))
OI_WARMUP_SEC = int(os.environ.get("OI_WARMUP_SEC", "600"))

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "60"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "60"))

MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "500000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.0001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "10.0"))

# === Триггер импульса (СМЯГЧЕНО) ===
IMPULSE_MIN_MOVE_PCT = float(os.environ.get("IMPULSE_MIN_MOVE_PCT", "1.5"))
IMPULSE_MAX_MOVE_PCT = float(os.environ.get("IMPULSE_MAX_MOVE_PCT", "15.0"))
IMPULSE_MIN_RVOL = float(os.environ.get("IMPULSE_MIN_RVOL", "2.5"))
IMPULSE_MIN_CLOSE_POS = float(os.environ.get("IMPULSE_MIN_CLOSE_POS", "0.60"))
IMPULSE_MIN_VOLUME_USD = float(os.environ.get("IMPULSE_MIN_VOLUME_USD", "15000"))

# === OI ===
OI_MIN_GROWTH_PCT = float(os.environ.get("OI_MIN_GROWTH_PCT", "5.0"))
OI_WINDOW_MIN = int(os.environ.get("OI_WINDOW_MIN", "15"))

# === Фильтры защиты ===
MAX_24H_CHANGE = float(os.environ.get("MAX_24H_CHANGE", "80.0"))
MIN_24H_CHANGE = float(os.environ.get("MIN_24H_CHANGE", "-5.0"))

COOLDOWN_SEC = int(os.environ.get("COOLDOWN_SEC", str(45 * 60)))

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"

ALLOWED_UPDATES = '["message","edited_message","callback_query"]'

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("IMPULSE-v13.3")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_SIGNAL = {}
LAST_CHECKED = {}
OI_HISTORY = defaultdict(list)

STATS = {
    "scans": 0,
    "symbol_checks": 0,
    "move_pass": 0,        # дошли до rvol-фильтра
    "rvol_pass": 0,        # дошли до close-фильтра
    "triggers": 0,
    "signals": 0,
    "rejected_move": 0,
    "rejected_rvol": 0,
    "rejected_close": 0,
    "rejected_24h": 0,
    "rejected_oi": 0,
    "rejected_oi_no_history": 0,
    "tg_commands": 0,
    "tg_fails": 0,
}


# ============================================================
# HTTP
# ============================================================

async def http_get(url, params=None, timeout=6):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None
    try:
        async with HTTP_SEMAPHORE:
            async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status == 429:
                    await asyncio.sleep(1.5)
                    return None
                if r.status >= 400:
                    return None
                return await r.json(content_type=None)
    except Exception:
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
        }
    return result


async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    result = set()
    if not data or data.get("code") != "00000":
        return result
    for row in data.get("data", []):
        if isinstance(row, dict):
            symbol = str(row.get("symbol", "")).upper()
            if symbol.endswith("USDT"):
                base = norm(symbol)
                if base:
                    result.add(base)
    return result


async def fetch_kucoin_candles_1m(symbol, limit=10):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - limit * 60 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": "1", "from": from_ms, "to": now_ms,
    })
    if not data or not isinstance(data.get("data"), list):
        return []
    candles = []
    for row in data["data"]:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts * 1000, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bitget_oi(base):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {
        "symbol": f"{base}USDT", "productType": "USDT-FUTURES",
    })
    if not data or data.get("code") != "00000":
        return 0.0
    raw = data.get("data")
    row = {}
    if isinstance(raw, dict):
        items = raw.get("list")
        row = items[0] if isinstance(items, list) and items else raw
    elif isinstance(raw, list) and raw:
        row = raw[0]
    if not isinstance(row, dict):
        return 0.0
    return num(row.get("amount") or row.get("openInterest") or row.get("openInterestUsd"))


# ============================================================
# IMPULSE DETECTOR
# ============================================================

def calc_impulse(candles):
    if len(candles) < 6:
        return None

    c = candles[-2]  # последняя закрытая 1m
    prev = candles[:-2]

    o, h, l, cl, v = c["open"], c["high"], c["low"], c["close"], c["volume"]
    if o <= 0 or cl <= 0:
        return None

    move_pct = ((cl / o) - 1) * 100
    rng = h - l
    if rng <= 0:
        return None

    close_pos = (cl - l) / rng
    vol_usd = v * cl

    prev_vols = [x["volume"] * x["close"] for x in prev[-5:] if x["volume"] > 0]
    if not prev_vols:
        return None
    avg = sum(prev_vols) / len(prev_vols)
    rvol = vol_usd / avg if avg > 0 else 0

    return {
        "close": cl,
        "move_pct": move_pct,
        "rvol": rvol,
        "volume_usd": vol_usd,
        "close_pos": close_pos,
    }


def oi_growth(base, window_min=OI_WINDOW_MIN):
    hist = OI_HISTORY.get(base)
    if not hist or len(hist) < 2:
        return 0.0

    now_ts, now_oi = hist[-1]
    target = now_ts - window_min * 60

    old = None
    for ts, oi in hist:
        if ts <= target:
            old = (ts, oi)
        else:
            break
    if old is None:
        return 0.0

    old_oi = old[1]
    if old_oi <= 0:
        return 0.0
    return ((now_oi / old_oi) - 1) * 100


# ============================================================
# TELEGRAM
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or SESSION is None:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            payload = await r.json(content_type=None)
            if not payload.get("ok"):
                STATS["tg_fails"] += 1
                return False
            return True
    except Exception:
        STATS["tg_fails"] += 1
        return False


async def send_signal(base, imp, oi_delta, change_24h, volume_24h):
    ticker = f"<code>{escape(base)}USDT</code>"
    oi_note = "растёт ✓" if oi_delta >= OI_MIN_GROWTH_PCT else "не подтверждает"

    msg = (
        f"⚡🚀 <b>ИМПУЛЬС: {ticker}</b>\n\n"
        f"📈 <b>Цена:</b> <code>{imp['close']:.6g}</code>\n"
        f"💥 <b>Move 1m:</b> <b>+{imp['move_pct']:.2f}%</b>\n"
        f"📊 <b>RVOL:</b> <b>{imp['rvol']:.1f}x</b>\n"
        f"💵 <b>Объём 1m:</b> ${imp['volume_usd']:,.0f}\n"
        f"🎯 <b>Close pos:</b> {imp['close_pos']:.2f}\n\n"
        f"📉 <b>OI Δ ({OI_WINDOW_MIN}м):</b> <b>{oi_delta:+.1f}%</b> ({oi_note})\n"
        f"📊 <b>24h change:</b> {change_24h:+.2f}%\n"
        f"💰 <b>Объём 24h:</b> ${volume_24h/1_000_000:.1f}M\n\n"
        f"⚡ Вертикальный импульс (TEST MODE v13.3)."
    )
    sent = await send_tg(msg)
    if sent:
        STATS["signals"] += 1


async def send_status(target_chat_id=None):
    uptime = (time.time() - START_TIME) / 3600
    msg = (
        f"📊 <b>IMPULSE v13.3 (TEST)</b>\n\n"
        f"⏱ Аптайм: {uptime:.1f}ч\n"
        f"🌐 Юниверс: {len(UNIVERSE)} пар\n"
        f"🔍 Сканов: {STATS['scans']}\n"
        f"👁 Проверок монет: {STATS['symbol_checks']}\n\n"
        f"<b>Воронка:</b>\n"
        f"  🟡 move-pass: {STATS['move_pass']}\n"
        f"  🟠 rvol-pass: {STATS['rvol_pass']}\n"
        f"  ⚡ Триггеров: {STATS['triggers']}\n"
        f"  🚀 Сигналов: {STATS['signals']}\n\n"
        f"<b>Отсевы:</b>\n"
        f"  move: {STATS['rejected_move']}\n"
        f"  rvol: {STATS['rejected_rvol']}\n"
        f"  close: {STATS['rejected_close']}\n"
        f"  24h: {STATS['rejected_24h']}\n"
        f"  OI (нет истории): {STATS['rejected_oi_no_history']}\n"
        f"  OI (падает): {STATS['rejected_oi']}\n\n"
        f"<b>Пороги:</b>\n"
        f"  move ≥ {IMPULSE_MIN_MOVE_PCT}% и ≤ {IMPULSE_MAX_MOVE_PCT}%\n"
        f"  rvol ≥ {IMPULSE_MIN_RVOL}x\n"
        f"  close ≥ {IMPULSE_MIN_CLOSE_POS}\n"
        f"  vol ≥ ${IMPULSE_MIN_VOLUME_USD:,.0f}"
    )
    dest = target_chat_id or CHAT_ID
    if dest:
        await send_tg(dest, msg)


async def handle_update(update):
    msg = update.get("message")
    if not msg:
        return
    chat_id = str(msg.get("chat", {}).get("id", ""))
    text = (msg.get("text") or "").strip().lower()
    if not text:
        return
    STATS["tg_commands"] += 1
    if text.startswith("/start"):
        await send_tg("👋 <b>IMPULSE v13.3 TEST MODE</b>\n/status — отчёт")
    elif text.startswith("/status"):
        await send_status(target_chat_id=chat_id)


async def tg_poll_loop():
    if not BOT_TOKEN:
        return
    offset = None
    log.info("📡 Polling started")
    while True:
        try:
            if SESSION is None or SESSION.closed:
                await asyncio.sleep(2)
                continue
            params = {"timeout": 25, "allowed_updates": ALLOWED_UPDATES}
            if offset is not None:
                params["offset"] = offset
            async with SESSION.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params=params, timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status != 200:
                    await asyncio.sleep(3)
                    continue
                data = await r.json(content_type=None)
                if not data or not data.get("ok"):
                    if data and data.get("error_code") == 409:
                        await asyncio.sleep(15)
                    else:
                        await asyncio.sleep(3)
                    continue
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    try:
                        await handle_update(update)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(5)


# ============================================================
# PROCESS SYMBOL
# ============================================================

async def process_symbol(base, sem):
    async with sem:
        # Кулдаун после сигнала
        if time.time() - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC:
            return

        # Ротация — монета проверяется раз в CHECK_ROTATION_SEC
        if time.time() - LAST_CHECKED.get(base, 0) < CHECK_ROTATION_SEC:
            return

        item = UNIVERSE.get(base)
        if not item:
            return

        LAST_CHECKED[base] = time.time()
        STATS["symbol_checks"] += 1

        # Фильтр 24h change
        change_24h = item.get("change24", 0)
        if change_24h >= MAX_24H_CHANGE or change_24h <= MIN_24H_CHANGE:
            STATS["rejected_24h"] += 1
            return

        candles = await fetch_kucoin_candles_1m(item["kucoin_symbol"], 10)
        imp = calc_impulse(candles)
        if not imp:
            return

        # 1. Move фильтр
        if imp["move_pct"] < IMPULSE_MIN_MOVE_PCT or imp["move_pct"] > IMPULSE_MAX_MOVE_PCT:
            STATS["rejected_move"] += 1
            return

        # Прошли move — логируем и считаем
        STATS["move_pass"] += 1
        log.info(
            "MOVE-PASS %s | move=+%.2f%% rvol=%.1fx close=%.2f vol=$%.0f",
            base, imp["move_pct"], imp["rvol"], imp["close_pos"], imp["volume_usd"]
        )

        # 2. RVOL фильтр
        if imp["rvol"] < IMPULSE_MIN_RVOL or imp["volume_usd"] < IMPULSE_MIN_VOLUME_USD:
            STATS["rejected_rvol"] += 1
            return

        STATS["rvol_pass"] += 1
        log.info(
            "RVOL-PASS %s | move=+%.2f%% rvol=%.1fx close=%.2f vol=$%.0f",
            base, imp["move_pct"], imp["rvol"], imp["close_pos"], imp["volume_usd"]
        )

        # 3. Close position фильтр
        if imp["close_pos"] < IMPULSE_MIN_CLOSE_POS:
            STATS["rejected_close"] += 1
            return

        STATS["triggers"] += 1
        log.info(
            "⚡ TRIGGER %s | move=+%.2f%% rvol=%.1fx close=%.2f vol=$%.0f",
            base, imp["move_pct"], imp["rvol"], imp["close_pos"], imp["volume_usd"]
        )

        # 4. OI проверка
        oi_val = await fetch_bitget_oi(base)
        if oi_val > 0:
            OI_HISTORY[base].append((time.time(), oi_val))
            cutoff = time.time() - 2 * 3600
            OI_HISTORY[base] = [(ts, v) for ts, v in OI_HISTORY[base] if ts >= cutoff]

        hist = OI_HISTORY.get(base, [])
        if len(hist) < 2:
            STATS["rejected_oi_no_history"] += 1
            log.info("REJECT %s: OI history too short (%d points)", base, len(hist))
            return

        oi_delta = oi_growth(base)

        if oi_delta < -10.0:
            STATS["rejected_oi"] += 1
            log.info("REJECT %s: OI falls %.1f%%", base, oi_delta)
            return

        LAST_SIGNAL[base] = time.time()
        await send_signal(base, imp, oi_delta, change_24h, item.get("volume24", 0))
        log.info("🚀 SIGNAL %s | OI %+.1f%%", base, oi_delta)


# ============================================================
# LOOPS
# ============================================================

async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    cands = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    sem = asyncio.Semaphore(8)
    await asyncio.gather(*[process_symbol(b, sem) for b in cands])
    gc.collect()


async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bg_set = await fetch_bitget_tickers()
    if not kc or not bg_set:
        return

    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bg_set:
            continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }

    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])

    # Чистим стейт
    for b in list(OI_HISTORY.keys()):
        if b not in UNIVERSE:
            OI_HISTORY.pop(b, None)
    for b in list(LAST_CHECKED.keys()):
        if b not in UNIVERSE:
            LAST_CHECKED.pop(b, None)

    log.info("🌐 Юниверс: %d пар", len(UNIVERSE))


async def oi_warmup_loop():
    while True:
        try:
            if UNIVERSE:
                bases = list(UNIVERSE.keys())
                sem = asyncio.Semaphore(8)

                async def one(b):
                    async with sem:
                        v = await fetch_bitget_oi(b)
                        if v > 0:
                            OI_HISTORY[b].append((time.time(), v))
                            cutoff = time.time() - 2 * 3600
                            OI_HISTORY[b] = [(ts, x) for ts, x in OI_HISTORY[b] if ts >= cutoff]

                await asyncio.gather(*[one(b) for b in bases])
                log.info("💊 OI warmup: %d монет обновлено", len(bases))
        except Exception as e:
            log.exception("OI warmup: %s", e)
        await asyncio.sleep(OI_WARMUP_SEC)


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
            log.exception("scan error")
        await asyncio.sleep(SCAN_INTERVAL_SEC)


# ============================================================
# WEB
# ============================================================

async def index(req):
    return web.Response(
        text=f"IMPULSE v13.3 TEST | Uni: {len(UNIVERSE)} | "
             f"Checks: {STATS['symbol_checks']} | "
             f"Move-pass: {STATS['move_pass']} | "
             f"Rvol-pass: {STATS['rvol_pass']} | "
             f"Triggers: {STATS['triggers']} | "
             f"Signals: {STATS['signals']}",
        content_type="text/plain",
    )


async def start(app):
    global SESSION, HTTP_SEMAPHORE, START_TIME
    START_TIME = time.time()
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(15)

    if BOT_TOKEN:
        try:
            async with SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
                params={"drop_pending_updates": "false"},
                timeout=aiohttp.ClientTimeout(total=4),
            ) as r:
                log.info("deleteWebhook: %s", r.status)
        except Exception:
            pass

    app["poll_task"] = asyncio.create_task(tg_poll_loop())
    app["universe_task"] = asyncio.create_task(universe_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["oi_warmup_task"] = asyncio.create_task(oi_warmup_loop())

    await refresh_universe()
    await send_tg(
        "🚀 <b>IMPULSE v13.3 TEST MODE</b>\n\n"
        f"• Юниверс: {len(UNIVERSE)} монет\n"
        f"• Порог move: {IMPULSE_MIN_MOVE_PCT}%–{IMPULSE_MAX_MOVE_PCT}%\n"
        f"• Порог RVOL: ≥ {IMPULSE_MIN_RVOL}x\n"
        f"• Порог close: ≥ {IMPULSE_MIN_CLOSE_POS}\n"
        f"• Мин. объём: ${IMPULSE_MIN_VOLUME_USD:,.0f}\n\n"
        "Логируем MOVE-PASS и RVOL-PASS. /status для отчёта."
    )


async def stop(app):
    for k in ("poll_task", "universe_task", "scan_task", "oi_warmup_task"):
        t = app.get(k)
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
