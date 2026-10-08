import asyncio
import aiohttp
from aiohttp import web
import os
import time
import gc
import logging
from html import escape

# ============================================================
# ПАМП-ХАНТЕР v11.4 (Production Ready Edition)
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

# === Настройки тихого кумулятивного накопления (CUMULATIVE RVOL) ===
SHELF_LOOKBACK_CANDLES = int(os.environ.get("SHELF_LOOKBACK_CANDLES", "16"))   # 16x15m = 4 часа
SHELF_HIST_LOOKBACK = int(os.environ.get("SHELF_HIST_LOOKBACK", "20"))
MAX_SHELF_RANGE_PCT = float(os.environ.get("MAX_SHELF_RANGE_PCT", "2.0"))       # Макс ширина полки 2%
MIN_CUMULATIVE_RVOL = float(os.environ.get("MIN_CUMULATIVE_RVOL", "1.8"))      # Суммарный RVOL > 1.8x
MIN_GREEN_BUY_RATIO = float(os.environ.get("MIN_GREEN_BUY_RATIO", "0.60"))     # > 60% объема на выкуп
MAX_DIST_FROM_LOW_PCT = float(os.environ.get("MAX_DIST_FROM_LOW_PCT", "0.8"))  # Вход не выше 0.8% от дна полки
CUMULATIVE_COOLDOWN_SEC = int(os.environ.get("CUMULATIVE_COOLDOWN_SEC", str(4 * 3600)))

# === Трекер Кульминации и Переворота в Шорт (CLIMAX TRACKER) ===
CLIMAX_MIN_PROFIT_PCT = float(os.environ.get("CLIMAX_MIN_PROFIT_PCT", "2.5"))   # Профит от +2.5%
CLIMAX_RVOL_THRESHOLD = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "3.5"))   # RVOL выгрузки
CLIMAX_MIN_WICK_RATIO = float(os.environ.get("CLIMAX_MIN_WICK_RATIO", "0.45"))   # Верхняя тень > 45%
CLIMAX_TRACK_TIMEOUT_SEC = int(os.environ.get("CLIMAX_TRACK_TIMEOUT_SEC", str(24 * 3600))) # Авто-снятие через 24ч
CLIMAX_INVALIDATE_PCT = float(os.environ.get("CLIMAX_INVALIDATE_PCT", "-3.0"))  # Снятие с трека если цена ушла на -3%

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v11.4")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_SIGNAL = {}
ACTIVE_TRACKS = {}  # {base: {"entry": float, "ts": float}}

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
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
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
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
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
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                if ts < 10**12:
                    ts *= 1000
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    
