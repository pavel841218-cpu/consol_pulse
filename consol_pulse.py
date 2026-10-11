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
# IMPULSE HUNTER v13.7 — TEST MODE
# Fixes:
#   - IMPULSE_MIN_MOVE_PCT: 0.5% → 0.1%
#   - Логирование RVOL-PASS (видно что между rvol и move)
#   - Раздельный счётчик rejected_volume_usd
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Периодика ---
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "60"))
CHECK_ROTATION_SEC = int(os.environ.get("CHECK_ROTATION_SEC", "120"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "1800"))
OI_WARMUP_SEC = int(os.environ.get("OI_WARMUP_SEC", "600"))

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "60"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "60"))

MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "150000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.0001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "10.0"))

# === Триггер импульса (RVOL → MOVE → CLOSE) ===
IMPULSE_MIN_RVOL = float(os.environ.get("IMPULSE_MIN_RVOL", "2.2"))
IMPULSE_MIN_MOVE_PCT = float(os.environ.get("IMPULSE_MIN_MOVE_PCT", "0.1"))
IMPULSE_MAX_MOVE_PCT = float(os.environ.get("IMPULSE_MAX_MOVE_PCT", "15.0"))
IMPULSE_MIN_CLOSE_POS = float(os.environ.get("IMPULSE_MIN_CLOSE_POS", "0.55"))
IMPULSE_MIN_VOLUME_USD = float(os.environ.get("IMPULSE_MIN_VOLUME_USD", "3000"))

# === Защита от разворота на текущей свече ===
REVERSAL_CURRENT_MOVE_PCT = float(os.environ.get("REVERSAL_CURRENT_MOVE_PCT", "-0.5"))
REVERSAL_BELOW_PREV_CLOSE_PCT = float(os.environ.get("REVERSAL_BELOW_PREV_CLOSE_PCT", "0.997"))

# === OI (справочно, не блокирует) ===
OI_MIN_GROWTH_PCT = float(os.environ.get("OI_MIN_GROWTH_PCT", "5.0"))
OI_WINDOW_MIN = int(os.environ.get("OI_WINDOW_MIN", "15"))
OI_FALL_REJECT_PCT = float(os.environ.get("OI_FALL_REJECT_PCT", "-15.0"))

# === Фильтры защиты ===
MAX_24H_CHANGE = float(os.environ.get("MAX_24H_CHANGE", "25.0"))
MIN_24H_CHANGE = float(os.environ.get("MIN_24H_CHANGE", "-5.0"))

COOLDOWN_SEC = int(os.environ.get("COOLDOWN_SEC", str(45 * 60)))

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"

ALLOWED_UPDATES = '["message","edited_message","callback_query"]'

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("IMPULSE-v13.7")

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
    "rvol_pass": 0,
    "volume_usd_pass": 0,
    "move_pass": 0,
    "close_pass": 0,
    "triggers": 0,
    "signals": 0,
    "rejected_24h": 0,
    "rejected_no_candles": 0,
    "rejected_rvol": 0,
    "rejected_volume_usd": 0,
    "rejected_move": 0,
    "rejected_close": 0,
    "rejected_reversal": 0,
    "rejected_oi_fall": 0,
    "oi_na": 0,
    "tg_commands": 0,
    "tg_fails": 0,
    "errors": 0,
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
    except Exception as e:
        log.debug("http_get error: %s", e)
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


def check_reversal(candles):
    if len(candles) < 2:
        return None

    current = candles[-1]
    prev_closed = candles[-2]

    if current["open"] <= 0 or current["close"] <= 0:
        return None

    current_move = ((current["close"] / current["open"]) - 1) * 100

    if current_move < REVERSAL_CURRENT_MOVE_PCT:
        return {"reason": "current_red", "current_move": current_move}

    if current["close"] < prev_closed["close"] * REVERSAL_BELOW_PREV_CLOSE_PCT:
        return {
            "reason": "under_prev_close",
            "current_close": current["close"],
            "prev_close": prev_closed["close"],
        }

    return None


def oi_growth(base, window_min=OI_WINDOW_MIN):
    hist = OI_HISTORY.get(base)
    if not hist or len(hist) < 2:
        return None

    now_ts, now_oi = hist[-1]
    target = now_ts - window_min * 60

    old = None
    for ts, oi in hist:
        if ts <= target:
            old = (ts, oi)
        else:
            break
    if old is None:
        return None

    old_oi = old[1]
    if old_oi <= 0:
        return None
    return ((now_oi / old_oi) - 1) * 100


# ============================================================
# TELEGRAM
# ============================================================

async def send_tg(text, chat_id=None):
    if not BOT_TOKEN or SESSION is None:
        return False
    target = chat_id or CHAT_ID
    if not target:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": target,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            payload = await r.json(content_type=None)
            if not payload.get("ok"):
                STATS["tg_fails"] += 1
                log.warning("Telegram send failed: %s", str(payload)[:200])
                return False
            return True
    except Exception as e:
        STATS["tg_fails"] += 1
        log.warning("Telegram send error: %s", e)
        return False


async def send_signal(base, imp, oi_delta, change_24h, volume_24h):
    ticker = f"<code>{escape(base)}USDT</code>"

    if oi_delta is None:
        oi_line = "📉 <b>OI:</b> <i>нет данных</i>"
    elif oi_delta >= OI_MIN_GROWTH_PCT:
        oi_line = f"📉 <b>OI Δ ({OI_WINDOW_MIN}м):</b> <b>{oi_delta:+.1f}%</b> (растёт ✓)"
    else:
        oi_line = f"📉 <b>OI Δ ({OI_WINDOW_MIN}м):</b> <b>{oi_delta:+.1f}%</b>"

    msg = (
        f"⚡🚀 <b>ИМПУЛЬС: {ticker}</b>\n\n"
        f"📈 <b>Цена:</b> <code>{imp['close']:.6g}</code>\n"
        f"💥 <b>Move 1m:</b> <b>+{imp['move_pct']:.2f}%</b>\n"
        f"📊 <b>RVOL:</b> <b>{imp['rvol']:.1f}x</b>\n"
        f"💵 <b>Объём 1m:</b> ${imp['volume_usd']:,.0f}\n"
        f"🎯 <b>Close pos:</b> {imp['close_pos']:.2f}\n\n"
        f"{oi_line}\n"
        f"📊 <b>24h change:</b> {change_24h:+.2f}%\n"
        f"💰 <b>Объём 24h:</b> ${volume_24h/1_000_000:.1f}M\n\n"
        f"⚡ Вертикальный импульс (TEST MODE v13.7)."
    )
    sent = await send_tg(msg)
    if sent:
        STATS["signals"] += 1


async def send_status(target_chat_id=None):
    uptime = (time.time() - START_TIME) / 3600
    msg = (
        f"📊 <b>IMPULSE v13.7 (TEST)</b>\n\n"
        f"⏱ Аптайм: {uptime:.1f}ч\n"
        f"🌐 Юниверс: {len(UNIVERSE)} пар\n"
        f"🔍 Сканов: {STATS['scans']}\n"
        f"👁 Проверок монет: {STATS['symbol_checks']}\n\n"
        f"<b>Воронка:</b>\n"
        f"  🟠 rvol-pass: {STATS['rvol_pass']}\n"
        f"  💰 vol-usd-pass: {STATS['volume_usd_pass']}\n"
        f"  🟡 move-pass: {STATS['move_pass']}\n"
        f"  🟢 close-pass: {STATS['close_pass']}\n"
        f"  ⚡ Триггеров: {STATS['triggers']}\n"
        f"  🚀 Сигналов: {STATS['signals']}\n\n"
        f"<b>Отсевы:</b>\n"
        f"  24h: {STATS['rejected_24h']}\n"
        f"  no candles: {STATS['rejected_no_candles']}\n"
        f"  rvol: {STATS['rejected_rvol']}\n"
        f"  vol_usd: {STATS['rejected_volume_usd']}\n"
        f"  move: {STATS['rejected_move']}\n"
        f"  close: {STATS['rejected_close']}\n"
        f"  reversal: {STATS['rejected_reversal']}\n"
        f"  OI fall: {STATS['rejected_oi_fall']}\n"
        f"  OI N/A: {STATS['oi_na']}\n\n"
        f"<b>Telegram:</b> cmds: {STATS['tg_commands']} | fails: {STATS['tg_fails']}\n\n"
        f"<b>Пороги:</b>\n"
        f"  rvol ≥ {IMPULSE_MIN_RVOL}x\n"
        f"  vol ≥ ${IMPULSE_MIN_VOLUME_USD:,.0f}\n"
        f"  move ≥ {IMPULSE_MIN_MOVE_PCT}%\n"
        f"  close ≥ {IMPULSE_MIN_CLOSE_POS}\n"
        f"  max 24h: {MAX_24H_CHANGE}%\n"
    )
    await send_tg(msg, chat_id=target_chat_id)


async def handle_update(update):
    try:
        msg = update.get("message") or update.get("channel_post")
        if not msg:
            return
        chat_id = str(msg.get("chat", {}).get("id", ""))
        text = (msg.get("text") or "").strip()
        if not text:
            return

        STATS["tg_commands"] += 1
        log.info("📨 TG command from %s: %s", chat_id, text[:60])

        lower = text.lower()
        if lower.startswith("/start"):
            await send_tg(
                "👋 <b>IMPULSE v13.7 TEST MODE</b>\n\n"
                "Команды:\n"
                "/status — отчёт по боту",
                chat_id=chat_id,
            )
        elif lower.startswith("/status"):
            await send_status(target_chat_id=chat_id)
        else:
            await send_tg("❓ Неизвестная команда. /status", chat_id=chat_id)
    except Exception as e:
        STATS["errors"] += 1
        log.exception("handle_update failed: %s", e)


async def tg_poll_loop():
    if not BOT_TOKEN:
        log.warning("BOT_TOKEN не задан — polling отключён")
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
                        log.warning("409 conflict — другой инстанс, ждём 15с")
                        await asyncio.sleep(15)
                    else:
                        await asyncio.sleep(3)
                    continue
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    await handle_update(update)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            STATS["errors"] += 1
            log.exception("tg_poll_loop error: %s", e)
            await asyncio.sleep(5)


# ============================================================
# PROCESS SYMBOL
# ============================================================

async def process_symbol(base, sem):
    async with sem:
        if time.time() - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC:
            return
        if time.time() - LAST_CHECKED.get(base, 0) < CHECK_ROTATION_SEC:
            return

        item = UNIVERSE.get(base)
        if not item:
            return

        LAST_CHECKED[base] = time.time()
        STATS["symbol_checks"] += 1

        # 1. 24h change
        change_24h = item.get("change24", 0)
        if change_24h >= MAX_24H_CHANGE or change_24h <= MIN_24H_CHANGE:
            STATS["rejected_24h"] += 1
            return

        # 2. Свечи
        candles = await fetch_kucoin_candles_1m(item["kucoin_symbol"], 10)
        if len(candles) < 6:
            STATS["rejected_no_candles"] += 1
            return

        imp = calc_impulse(candles)
        if not imp:
            STATS["rejected_no_candles"] += 1
            return

        # 3. RVOL
        if imp["rvol"] < IMPULSE_MIN_RVOL:
            STATS["rejected_rvol"] += 1
            return

        STATS["rvol_pass"] += 1
        log.info(
            "🟠 RVOL-PASS %s | rvol=%.2fx vol=$%.0f move=+%.2f%% close=%.2f",
            base, imp["rvol"], imp["volume_usd"], imp["move_pct"], imp["close_pos"]
        )

        # 4. Объём USD
        if imp["volume_usd"] < IMPULSE_MIN_VOLUME_USD:
            STATS["rejected_volume_usd"] += 1
            return

        STATS["volume_usd_pass"] += 1

        # 5. MOVE
        if imp["move_pct"] < IMPULSE_MIN_MOVE_PCT or imp["move_pct"] > IMPULSE_MAX_MOVE_PCT:
            STATS["rejected_move"] += 1
            return

        STATS["move_pass"] += 1
        log.info(
            "🟡 MOVE-PASS %s | rvol=%.2fx move=+%.2f%% close=%.2f vol=$%.0f",
            base, imp["rvol"], imp["move_pct"], imp["close_pos"], imp["volume_usd"]
        )

        # 6. CLOSE POS
        if imp["close_pos"] < IMPULSE_MIN_CLOSE_POS:
            STATS["rejected_close"] += 1
            return

        STATS["close_pass"] += 1

        # 7. REVERSAL GUARD
        rev = check_reversal(candles)
        if rev:
            STATS["rejected_reversal"] += 1
            log.info(
                "REJECT %s: reversal [%s] current=%.2f%%",
                base, rev["reason"], rev.get("current_move", 0)
            )
            return

        # 8. TRIGGER
        STATS["triggers"] += 1
        log.info(
            "⚡ TRIGGER %s | rvol=%.2fx move=+%.2f%% close=%.2f vol=$%.0f",
            base, imp["rvol"], imp["move_pct"], imp["close_pos"], imp["volume_usd"]
        )

        # 9. OI справочно
        oi_val = await fetch_bitget_oi(base)
        if oi_val > 0:
            OI_HISTORY[base].append((time.time(), oi_val))
            cutoff = time.time() - 2 * 3600
            OI_HISTORY[base] = [(ts, v) for ts, v in OI_HISTORY[base] if ts >= cutoff]

        oi_delta = oi_growth(base)

        if oi_delta is None:
            STATS["oi_na"] += 1
        elif oi_delta < OI_FALL_REJECT_PCT:
            STATS["rejected_oi_fall"] += 1
            log.info("REJECT %s: OI falls %.1f%%", base, oi_delta)
            return

        LAST_SIGNAL[base] = time.time()
        await send_signal(base, imp, oi_delta, change_24h, item.get("volume24", 0))
        log.info(
            "🚀 SIGNAL %s | OI %s",
            base, f"{oi_delta:+.1f}%" if oi_delta is not None else "N/A"
        )


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
        log.warning("Universe refresh failed")
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
        except asyncio.CancelledError:
            raise
        except Exception as e:
            STATS["errors"] += 1
            log.exception("OI warmup: %s", e)
        await asyncio.sleep(OI_WARMUP_SEC)


async def universe_loop():
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            await refresh_universe()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            STATS["errors"] += 1
            log.exception("universe_loop: %s", e)
            await asyncio.sleep(10)


async def scan_loop():
    while True:
        try:
            if UNIVERSE:
                await scan_cycle()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            STATS["errors"] += 1
            log.exception("scan_loop: %s", e)
        await asyncio.sleep(SCAN_INTERVAL_SEC)


# ============================================================
# WEB
# ============================================================

async def index(req):
    return web.Response(
        text=(
            f"IMPULSE v13.7 | Uni: {len(UNIVERSE)} | "
            f"Checks: {STATS['symbol_checks']} | "
            f"RVOL-pass: {STATS['rvol_pass']} | "
            f"Vol-pass: {STATS['volume_usd_pass']} | "
            f"Move-pass: {STATS['move_pass']} | "
            f"Close-pass: {STATS['close_pass']} | "
            f"Triggers: {STATS['triggers']} | "
            f"Signals: {STATS['signals']} | "
            f"Reversal: {STATS['rejected_reversal']} | "
            f"Errors: {STATS['errors']}"
        ),
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
        except Exception as e:
            log.warning("deleteWebhook: %s", e)

    app["poll_task"] = asyncio.create_task(tg_poll_loop())
    app["universe_task"] = asyncio.create_task(universe_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["oi_warmup_task"] = asyncio.create_task(oi_warmup_loop())

    await refresh_universe()

    await send_tg(
        "🚀 <b>IMPULSE v13.7 TEST MODE</b>\n\n"
        f"• Юниверс: {len(UNIVERSE)} монет\n"
        f"• RVOL ≥ {IMPULSE_MIN_RVOL}x\n"
        f"• Vol ≥ ${IMPULSE_MIN_VOLUME_USD:,.0f}\n"
        f"• Move ≥ {IMPULSE_MIN_MOVE_PCT}%\n"
        f"• Close ≥ {IMPULSE_MIN_CLOSE_POS}\n"
        f"• Max 24h: {MAX_24H_CHANGE}%\n\n"
        "Логируем RVOL-PASS. /status для отчёта."
    )

    log.info("✅ Impulse Hunter v13.7 started")


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
