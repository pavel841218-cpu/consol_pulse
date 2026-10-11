import asyncio
import aiohttp
from aiohttp import web, WSMsgType
import os
import time
import gc
import logging
from html import escape
from collections import defaultdict, deque

# ============================================================
# PUMP-HUNTER v12.1
# FLAT -> ABSORPTION -> BREAKOUT -> PUMP -> CLIMAX
#
# Fixes vs v12.0:
#   1. drop_pending_updates: false (не теряем команды)
#   2. Логирование ошибок в send_tg
#   3. STATS["flat_found"] = len(SHELVES) (не накопительный)
#   4. Обработка ping от сервера KuCoin WS
#   5. Кеш 15m свечей для активных треков на 60 сек
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# ---------------- PERIODS ----------------
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "30"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))
FLAT_SCAN_INTERVAL_SEC = int(os.environ.get("FLAT_SCAN_INTERVAL_SEC", "300"))
CANDLE_CACHE_SEC = int(os.environ.get("CANDLE_CACHE_SEC", "60"))  # NEW

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "200"))
MAX_FLAT_CANDIDATES = int(os.environ.get("MAX_FLAT_CANDIDATES", "120"))
MAX_ACTIVE_SHELVES = int(os.environ.get("MAX_ACTIVE_SHELVES", "25"))

MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "300000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "1.0"))
MIN_LISTING_AGE_DAYS = float(os.environ.get("MIN_LISTING_AGE_DAYS", "14"))

# ---------------- SHELF / FLAT ----------------
SHELF_MIN_CANDLES = int(os.environ.get("SHELF_MIN_CANDLES", "8"))
SHELF_MAX_CANDLES = int(os.environ.get("SHELF_MAX_CANDLES", "24"))
SHELF_RANGE_MIN_PCT = float(os.environ.get("SHELF_RANGE_MIN_PCT", "0.30"))
SHELF_RANGE_MAX_PCT = float(os.environ.get("SHELF_RANGE_MAX_PCT", "2.50"))
SHELF_MAX_NET_MOVE_PCT = float(os.environ.get("SHELF_MAX_NET_MOVE_PCT", "1.80"))
SHELF_MAX_DRIFT_PCT = float(os.environ.get("SHELF_MAX_DRIFT_PCT", "1.50"))
SHELF_MIN_INSIDE_RATIO = float(os.environ.get("SHELF_MIN_INSIDE_RATIO", "0.80"))
SHELF_MAX_DIST_FROM_HIGH_PCT = float(os.environ.get("SHELF_MAX_DIST_FROM_HIGH_PCT", "1.00"))
SHELF_MAX_AGE_SEC = int(os.environ.get("SHELF_MAX_AGE_SEC", str(8 * 3600)))

# ---------------- REAL-TIME ABSORPTION ----------------
ABS_WINDOW_MIN = float(os.environ.get("ABS_WINDOW_MIN", "10"))
ABS_MIN_TRADES = int(os.environ.get("ABS_MIN_TRADES", "80"))
ABS_MIN_BUY_RATIO = float(os.environ.get("ABS_MIN_BUY_RATIO", "0.60"))
ABS_MIN_CVD_USDT = float(os.environ.get("ABS_MIN_CVD_USDT", "5000"))
ABS_MIN_SPREAD_PCT = float(os.environ.get("ABS_MIN_SPREAD_PCT", "0.15"))
ABS_MAX_SPREAD_PCT = float(os.environ.get("ABS_MAX_SPREAD_PCT", "1.20"))
ABS_MIN_VOLUME_USDT = float(os.environ.get("ABS_MIN_VOLUME_USDT", "15000"))
ABS_MIN_EFFICIENCY = float(os.environ.get("ABS_MIN_EFFICIENCY", "12000"))
ABS_MIN_EFFICIENCY_SCORE = float(os.environ.get("ABS_MIN_EFFICIENCY_SCORE", "1.50"))
ABS_MIN_CVD_PERSISTENCE = float(os.environ.get("ABS_MIN_CVD_PERSISTENCE", "0.65"))
ABS_MIN_ZONE_HOLD_SEC = int(os.environ.get("ABS_MIN_ZONE_HOLD_SEC", "300"))
ABS_MAX_ABOVE_SHELF_PCT = float(os.environ.get("ABS_MAX_ABOVE_SHELF_PCT", "0.50"))

BUCKET_SEC = int(os.environ.get("BUCKET_SEC", "5"))
TRADE_BUFFER_MIN = int(os.environ.get("TRADE_BUFFER_MIN", "30"))

# ---------------- BREAKOUT ----------------
BREAKOUT_MIN_PCT = float(os.environ.get("BREAKOUT_MIN_PCT", "0.25"))
BREAKOUT_CONFIRM_PCT = float(os.environ.get("BREAKOUT_CONFIRM_PCT", "0.60"))
BREAKOUT_MAX_PCT = float(os.environ.get("BREAKOUT_MAX_PCT", "4.00"))
BREAKOUT_CONFIRM_WINDOW_SEC = int(os.environ.get("BREAKOUT_CONFIRM_WINDOW_SEC", "180"))
BREAKOUT_MAX_RETURN_INTO_SHELF_PCT = float(
    os.environ.get("BREAKOUT_MAX_RETURN_INTO_SHELF_PCT", "0.15")
)
BREAKOUT_MIN_FLOW_BUY_RATIO = float(os.environ.get("BREAKOUT_MIN_FLOW_BUY_RATIO", "0.55"))
BREAKOUT_MIN_CVD_USDT = float(os.environ.get("BREAKOUT_MIN_CVD_USDT", "2500"))

# ---------------- CLIMAX ----------------
CLIMAX_MIN_PROFIT_PCT = float(os.environ.get("CLIMAX_MIN_PROFIT_PCT", "2.5"))
CLIMAX_RVOL_THRESHOLD = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "3.5"))
CLIMAX_MIN_WICK_RATIO = float(os.environ.get("CLIMAX_MIN_WICK_RATIO", "0.45"))
CLIMAX_TRACK_TIMEOUT_SEC = int(
    os.environ.get("CLIMAX_TRACK_TIMEOUT_SEC", str(24 * 3600))
)
CLIMAX_INVALIDATE_PCT = float(os.environ.get("CLIMAX_INVALIDATE_PCT", "-3.0"))

# ---------------- COOLDOWNS ----------------
SIGNAL_COOLDOWN_SEC = int(os.environ.get("SIGNAL_COOLDOWN_SEC", str(6 * 3600)))
ABS_CONFIRM_COOLDOWN_SEC = int(os.environ.get("ABS_CONFIRM_COOLDOWN_SEC", "900"))

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
KUCOIN_WS_TOKEN_URL = f"{KUCOIN_BASE}/api/v1/bullet-public"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("PUMP-HUNTER-v12.1")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
SHELVES = {}
ACTIVE_TRACKS = {}
LAST_SIGNAL = {}

TRADE_BUFFER = defaultdict(lambda: deque(maxlen=25000))

# NEW: кеш 15m свечей для активных треков
CANDLE_CACHE = {}  # base -> {"ts": float, "data": list}

WS_TASK = None
WS_SUBSCRIBED = set()

STATS = {
    "scans": 0,
    "flat_scans": 0,
    "flat_found": 0,
    "absorption_watch": 0,
    "absorption_confirmed": 0,
    "breakouts": 0,
    "pump_signals": 0,
    "climax_reversals": 0,
    "tracks_timed_out": 0,
    "tracks_invalidated": 0,
    "false_breakouts": 0,
    "rejected_shelf_range": 0,
    "rejected_shelf_drift": 0,
    "rejected_shelf_position": 0,
    "rejected_abs_spread": 0,
    "rejected_abs_buy_ratio": 0,
    "rejected_abs_cvd": 0,
    "rejected_abs_efficiency": 0,
    "ws_trades": 0,
    "ws_reconnects": 0,
    "ws_pings": 0,
    "tg_fails": 0,
    "errors": 0,
}

# ============================================================
# HTTP
# ============================================================

async def http_get(url, params=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None

    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout),
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


async def http_post(url, params=None, json=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None

    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.post(
                    url,
                    params=params,
                    json=json,
                    timeout=aiohttp.ClientTimeout(total=timeout),
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

    for suffix in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break

    return s


# ============================================================
# KUCOIN REST
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
            ts = int(row[0])

            if ts < 10**12:
                ts *= 1000

            o, h, l, c, v = map(num, row[1:6])

            if o > 0 and c > 0:
                candles.append({
                    "ts": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                })

        except (TypeError, ValueError, IndexError):
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_kucoin_candles(symbol, granularity_min, limit, cache_key=None):
    """
    Если cache_key передан — кешируем результат на CANDLE_CACHE_SEC секунд.
    """
    if cache_key:
        cached = CANDLE_CACHE.get(cache_key)
        if cached and (time.time() - cached["ts"]) < CANDLE_CACHE_SEC:
            return cached["data"]

    now_ms = int(time.time() * 1000)
    from_ms = now_ms - limit * granularity_min * 60 * 1000

    data = await http_get(
        f"{KUCOIN_BASE}/api/v1/kline/query",
        {
            "symbol": symbol,
            "granularity": str(granularity_min),
            "from": from_ms,
            "to": now_ms,
        },
    )

    if not data or not isinstance(data.get("data"), list):
        return []

    candles = _parse_list_candles(data["data"])

    if cache_key:
        CANDLE_CACHE[cache_key] = {
            "ts": time.time(),
            "data": candles,
        }

    return candles


# ============================================================
# KUCOIN FUTURES WEBSOCKET
# ============================================================

async def get_kucoin_ws_connection_info():
    data = await http_post(KUCOIN_WS_TOKEN_URL)

    if not data or data.get("code") != "200000":
        return None

    payload = data.get("data") or {}
    token = payload.get("token")
    servers = payload.get("instanceServers") or []

    if not token or not servers:
        return None

    server = servers[0]
    endpoint = server.get("endpoint")
    ping_interval = int(server.get("pingInterval", 18000))

    if not endpoint:
        return None

    return {
        "token": token,
        "endpoint": endpoint,
        "ping_interval": max(5000, ping_interval),
    }


def add_trade(base, ts_ms, price, size, side):
    if price <= 0 or size <= 0:
        return

    usd = price * size

    TRADE_BUFFER[base].append({
        "ts": ts_ms / 1000.0,
        "price": price,
        "usd": usd,
        "side": side,
    })

    STATS["ws_trades"] += 1


def desired_ws_symbols():
    return {
        shelf["kucoin_symbol"]
        for shelf in SHELVES.values()
        if shelf.get("state") in (
            "FLAT_DETECTED",
            "ABSORPTION_WATCH",
            "ABSORPTION_CONFIRMED",
            "BREAKOUT",
        )
    }


async def ws_subscribe(ws, symbol, subscribe=True):
    msg = {
        "id": str(int(time.time() * 1000)),
        "type": "subscribe" if subscribe else "unsubscribe",
        "topic": f"/contractMarket/execution:{symbol}",
        "response": True,
    }

    await ws.send_json(msg)


async def ws_ping_loop(ws, interval_ms):
    try:
        while not ws.closed:
            await asyncio.sleep(max(interval_ms / 1000.0 * 0.7, 5))

            if ws.closed:
                break

            await ws.send_json({
                "id": str(int(time.time() * 1000)),
                "type": "ping",
            })

    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def ws_subscription_manager(ws):
    global WS_SUBSCRIBED

    try:
        while not ws.closed:
            desired = desired_ws_symbols()

            for symbol in desired - WS_SUBSCRIBED:
                try:
                    await ws_subscribe(ws, symbol, True)
                    WS_SUBSCRIBED.add(symbol)
                except Exception:
                    pass

            for symbol in WS_SUBSCRIBED - desired:
                try:
                    await ws_subscribe(ws, symbol, False)
                except Exception:
                    pass

                WS_SUBSCRIBED.discard(symbol)

            await asyncio.sleep(5)

    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def websocket_loop():
    global WS_SUBSCRIBED

    while True:
        try:
            info = await get_kucoin_ws_connection_info()

            if not info:
                await asyncio.sleep(10)
                continue

            url = (
                f"{info['endpoint']}"
                f"?token={info['token']}"
                f"&connectId={int(time.time() * 1000)}"
            )

            log.info("🔌 KuCoin Futures WS connecting...")

            async with SESSION.ws_connect(
                url,
                heartbeat=None,
                autoping=True,
                receive_timeout=45,
                max_msg_size=2 * 1024 * 1024,
            ) as ws:

                STATS["ws_reconnects"] += 1
                WS_SUBSCRIBED = set()

                ping_task = asyncio.create_task(
                    ws_ping_loop(ws, info["ping_interval"])
                )

                sub_task = asyncio.create_task(
                    ws_subscription_manager(ws)
                )

                try:
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:

                            try:
                                payload = msg.json()
                            except Exception:
                                continue

                            msg_type = payload.get("type", "")

                            # NEW: обработка ping от сервера
                            if msg_type == "ping":
                                STATS["ws_pings"] += 1
                                try:
                                    await ws.send_json({
                                        "id": payload.get("id", str(int(time.time() * 1000))),
                                        "type": "pong",
                                    })
                                except Exception:
                                    pass
                                continue

                            if msg_type != "message":
                                continue

                            data = payload.get("data") or {}

                            symbol = str(
                                data.get("symbol", "")
                            ).upper()

                            if not symbol:
                                topic = str(payload.get("topic", ""))

                                if ":" in topic:
                                    symbol = topic.split(":", 1)[1].upper()

                            base = norm(symbol)

                            if not base:
                                continue

                            price = num(data.get("price"))
                            size = num(data.get("size"))
                            side = str(data.get("side", "")).lower()

                            if side not in ("buy", "sell"):
                                continue

                            ts_raw = num(data.get("ts"))

                            if ts_raw <= 0:
                                ts_raw = time.time() * 1000

                            elif ts_raw > 10**15:
                                ts_raw /= 1_000_000.0

                            elif ts_raw > 10**12:
                                ts_raw /= 1_000.0

                            add_trade(
                                base,
                                int(ts_raw),
                                price,
                                size,
                                side,
                            )

                        elif msg.type in (
                            WSMsgType.CLOSED,
                            WSMsgType.ERROR,
                        ):
                            break

                finally:
                    sub_task.cancel()

                    try:
                        await sub_task
                    except asyncio.CancelledError:
                        pass

                    ping_task.cancel()

                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except asyncio.CancelledError:
            raise

        except Exception as e:
            log.warning("KuCoin WS error: %s", e)

        WS_SUBSCRIBED = set()
        await asyncio.sleep(3)


# ============================================================
# SHELF DETECTION
# ============================================================

def detect_shelf(candles_15m):
    closed = candles_15m[:-1]

    if len(closed) < SHELF_MIN_CANDLES + 5:
        return None

    max_n = min(SHELF_MAX_CANDLES, len(closed))
    best = None

    for n in range(SHELF_MIN_CANDLES, max_n + 1):

        cs = closed[-n:]

        high = max(c["high"] for c in cs)
        low = min(c["low"] for c in cs)

        if low <= 0:
            continue

        width = (high - low) / low * 100.0

        if width < SHELF_RANGE_MIN_PCT:
            continue

        if width > SHELF_RANGE_MAX_PCT:
            STATS["rejected_shelf_range"] += 1
            continue

        first_close = cs[0]["close"]
        last_close = cs[-1]["close"]

        net_move = abs(last_close - first_close) / first_close * 100.0

        if net_move > SHELF_MAX_NET_MOVE_PCT:
            STATS["rejected_shelf_drift"] += 1
            continue

        mid = (high + low) / 2.0
        drift = abs(last_close - mid) / mid * 100.0

        if drift > SHELF_MAX_DRIFT_PCT:
            continue

        inside = sum(
            1
            for c in cs
            if low <= c["close"] <= high
        ) / len(cs)

        if inside < SHELF_MIN_INSIDE_RATIO:
            continue

        dist_high = (high - last_close) / last_close * 100.0

        if dist_high < 0 or dist_high > SHELF_MAX_DIST_FROM_HIGH_PCT:
            STATS["rejected_shelf_position"] += 1
            continue

        score = (
            (SHELF_RANGE_MAX_PCT - width) * 2.0
            + inside * 2.0
            + (SHELF_MAX_NET_MOVE_PCT - net_move)
        )

        candidate = {
            "high": high,
            "low": low,
            "width_pct": width,
            "net_move_pct": net_move,
            "inside_ratio": inside,
            "candles": n,
            "start_ts": cs[0]["ts"],
            "last_close": last_close,
            "score": score,
        }

        if best is None or candidate["score"] > best["score"]:
            best = candidate

    return best


# ============================================================
# REAL-TIME FLOW METRICS
# ============================================================

def cleanup_trade_buffer(base):
    dq = TRADE_BUFFER.get(base)

    if not dq:
        return

    cutoff = time.time() - TRADE_BUFFER_MIN * 60

    while dq and dq[0]["ts"] < cutoff:
        dq.popleft()


def get_recent_trades(base, window_sec):
    cleanup_trade_buffer(base)

    dq = TRADE_BUFFER.get(base)

    if not dq:
        return []

    cutoff = time.time() - window_sec

    return [
        t for t in dq
        if t["ts"] >= cutoff
    ]


def bucket_metrics(trades, bucket_sec=5):
    if not trades:
        return []

    buckets = {}

    for t in trades:

        bucket_ts = (
            int(t["ts"] // bucket_sec)
            * bucket_sec
        )

        if bucket_ts not in buckets:
            buckets[bucket_ts] = {
                "buy": 0.0,
                "sell": 0.0,
                "total": 0.0,
                "trades": 0,
                "last_price": t["price"],
                "high": t["price"],
                "low": t["price"],
            }

        x = buckets[bucket_ts]

        x["total"] += t["usd"]
        x["trades"] += 1
        x["last_price"] = t["price"]
        x["high"] = max(x["high"], t["price"])
        x["low"] = min(x["low"], t["price"])

        if t["side"] == "buy":
            x["buy"] += t["usd"]
        else:
            x["sell"] += t["usd"]

    result = []
    cvd = 0.0

    for ts in sorted(buckets):

        x = buckets[ts]
        delta = x["buy"] - x["sell"]

        cvd += delta

        x["delta"] = delta
        x["buy_ratio"] = (
            x["buy"] / x["total"]
            if x["total"] > 0
            else 0.5
        )
        x["cvd"] = cvd

        result.append((ts, x))

    return result


def calculate_absorption(base, shelf):
    trades = get_recent_trades(
        base,
        int(ABS_WINDOW_MIN * 60),
    )

    if len(trades) < ABS_MIN_TRADES:
        return None

    prices = [t["price"] for t in trades]

    high = max(prices)
    low = min(prices)

    if low <= 0:
        return None

    spread_pct = (
        (high - low)
        / low
        * 100.0
    )

    if (
        spread_pct < ABS_MIN_SPREAD_PCT
        or spread_pct > ABS_MAX_SPREAD_PCT
    ):
        STATS["rejected_abs_spread"] += 1
        return None

    total = sum(t["usd"] for t in trades)
    buy = sum(
        t["usd"]
        for t in trades
        if t["side"] == "buy"
    )
    sell = total - buy

    if total < ABS_MIN_VOLUME_USDT:
        return None

    buy_ratio = buy / total if total > 0 else 0.0
    cvd = buy - sell

    if buy_ratio < ABS_MIN_BUY_RATIO:
        STATS["rejected_abs_buy_ratio"] += 1
        return None

    if cvd < ABS_MIN_CVD_USDT:
        STATS["rejected_abs_cvd"] += 1
        return None

    buckets = bucket_metrics(
        trades,
        BUCKET_SEC,
    )

    if len(buckets) < 4:
        return None

    positive_buckets = sum(
        1
        for _, b in buckets
        if b["delta"] > 0
    )

    cvd_persistence = (
        positive_buckets / len(buckets)
    )

    if cvd_persistence < ABS_MIN_CVD_PERSISTENCE:
        return None

    efficiency = (
        buy
        / max(spread_pct, 0.05)
    )

    old_window_sec = int(
        ABS_WINDOW_MIN * 120
    )

    old_trades = get_recent_trades(
        base,
        old_window_sec,
    )

    cutoff = time.time() - ABS_WINDOW_MIN * 60

    old_trades = [
        t
        for t in old_trades
        if t["ts"] < cutoff
    ]

    old_buy = sum(
        t["usd"]
        for t in old_trades
        if t["side"] == "buy"
    )

    old_prices = [
        t["price"]
        for t in old_trades
    ]

    if old_prices and old_buy > 0:

        old_spread = (
            (max(old_prices) - min(old_prices))
            / min(old_prices)
            * 100.0
        )

        old_efficiency = (
            old_buy
            / max(old_spread, 0.05)
        )

        efficiency_score = (
            efficiency
            / max(old_efficiency, 1.0)
        )

    else:
        efficiency_score = 0.0

    if (
        efficiency < ABS_MIN_EFFICIENCY
        and efficiency_score < ABS_MIN_EFFICIENCY_SCORE
    ):
        STATS["rejected_abs_efficiency"] += 1
        return None

    current_price = prices[-1]

    above_shelf_pct = (
        (current_price - shelf["high"])
        / shelf["high"]
        * 100.0
    )

    if above_shelf_pct > ABS_MAX_ABOVE_SHELF_PCT:
        return None

    hold_sec = (
        trades[-1]["ts"]
        - trades[0]["ts"]
    )

    if hold_sec < ABS_MIN_ZONE_HOLD_SEC:
        return None

    return {
        "window_min": ABS_WINDOW_MIN,
        "trades": len(trades),
        "total_volume": total,
        "buy_volume": buy,
        "sell_volume": sell,
        "buy_ratio": buy_ratio,
        "cvd": cvd,
        "spread_pct": spread_pct,
        "efficiency": efficiency,
        "efficiency_score": efficiency_score,
        "cvd_persistence": cvd_persistence,
        "current_price": current_price,
        "above_shelf_pct": above_shelf_pct,
        "hold_sec": hold_sec,
    }


# ============================================================
# BREAKOUT
# ============================================================

def get_current_price(base):
    dq = TRADE_BUFFER.get(base)

    if dq:
        return dq[-1]["price"]

    return 0.0


def breakout_metrics(base, shelf):
    price = get_current_price(base)

    if price <= 0 or shelf["high"] <= 0:
        return None

    above_pct = (
        (price - shelf["high"])
        / shelf["high"]
        * 100.0
    )

    return {
        "price": price,
        "above_pct": above_pct,
    }


def breakout_flow(base):
    trades = get_recent_trades(base, 60)

    if not trades:
        return None

    total = sum(t["usd"] for t in trades)

    if total <= 0:
        return None

    buy = sum(
        t["usd"]
        for t in trades
        if t["side"] == "buy"
    )

    sell = total - buy

    return {
        "total": total,
        "buy": buy,
        "sell": sell,
        "buy_ratio": buy / total,
        "cvd": buy - sell,
        "trades": len(trades),
    }


# ============================================================
# CLIMAX
# ============================================================

def check_climax_reversal(candles, entry_price):
    if len(candles) < 21 or entry_price <= 0:
        return None

    current = candles[-1]
    previous = candles[-21:-1]

    avg_volume = (
        sum(c["volume"] for c in previous)
        / len(previous)
    )

    if avg_volume <= 0:
        return None

    elapsed_min = (
        time.time() * 1000
        - current["ts"]
    ) / 1000 / 60

    elapsed_fraction = max(
        min(elapsed_min / 15.0, 1.0),
        0.05,
    )

    current_rvol = (
        current["volume"]
        / elapsed_fraction
    ) / avg_volume

    op = current["open"]
    cp = current["close"]
    hp = current["high"]
    lp = current["low"]

    candle_range = hp - lp

    if candle_range <= 0:
        return None

    upper_wick = hp - max(op, cp)
    wick_ratio = upper_wick / candle_range

    profit_pct = (
        (hp - entry_price)
        / entry_price
        * 100.0
    )

    if (
        profit_pct >= CLIMAX_MIN_PROFIT_PCT
        and current_rvol >= CLIMAX_RVOL_THRESHOLD
        and wick_ratio >= CLIMAX_MIN_WICK_RATIO
    ):
        return {
            "profit_pct": round(profit_pct, 2),
            "climax_rvol": round(current_rvol, 2),
            "wick_ratio_pct": round(wick_ratio * 100, 1),
            "exit_price": cp,
            "short_stop_loss": hp * 1.003,
        }

    return None


# ============================================================
# TELEGRAM
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or SESSION is None:
        return False

    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:

            if r.status != 200:
                STATS["tg_fails"] += 1
                body = await r.text()
                log.warning(
                    "TG send HTTP %s: %s",
                    r.status,
                    body[:200],
                )
                return False

            payload = await r.json(content_type=None)

            if not payload.get("ok", False):
                STATS["tg_fails"] += 1
                log.warning(
                    "TG send not ok: %s",
                    str(payload)[:200],
                )
                return False

            return True

    except Exception as e:
        STATS["tg_fails"] += 1
        log.warning("TG send error: %s", e)
        return False


async def send_absorption_watch(base, shelf, data):
    msg = (
        f"👀 <b>ABSORPTION WATCH</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"📦 Полка: <code>{shelf['low']:.8g} — {shelf['high']:.8g}</code>\n"
        f"📐 Width: <b>{shelf['width_pct']:.2f}%</b>\n"
        f"⏱ Shelf: <b>{shelf['candles']} × 15m</b>\n\n"
        f"💰 Flow: <b>${data['total_volume']:,.0f}</b>\n"
        f"🟢 Buy Ratio: <b>{data['buy_ratio'] * 100:.1f}%</b>\n"
        f"📈 CVD: <b>+${data['cvd']:,.0f}</b>\n"
        f"📏 Compression: <b>{data['spread_pct']:.2f}%</b>\n"
        f"🧲 Absorption Efficiency: <b>{data['efficiency']:,.0f}</b>\n"
        f"📊 CVD Persistence: <b>{data['cvd_persistence'] * 100:.0f}%</b>\n\n"
        f"⏳ <i>Ждём истощение предложения и выход выше полки.</i>"
    )

    await send_tg(msg)


async def send_pump_signal(base, shelf, data, breakout):
    msg = (
        f"🟢 <b>PUMP CONFIRMED</b>\n"
        f"<i>ABSORPTION → BREAKOUT</i>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"📦 Полка: <code>{shelf['low']:.8g} — {shelf['high']:.8g}</code>\n"
        f"📐 Width: <b>{shelf['width_pct']:.2f}%</b>\n"
        f"⏱ Shelf: <b>{shelf['candles']} × 15m</b>\n\n"
        f"🧲 <b>Absorption</b>\n"
        f"  Buy Ratio: <b>{data['buy_ratio'] * 100:.1f}%</b>\n"
        f"  CVD: <b>+${data['cvd']:,.0f}</b>\n"
        f"  Compression: <b>{data['spread_pct']:.2f}%</b>\n"
        f"  Efficiency Score: <b>{data['efficiency_score']:.2f}x</b>\n\n"
        f"🚀 Breakout: <b>+{breakout['above_pct']:.2f}%</b>\n"
        f"💵 Price: <code>{breakout['price']:.8g}</code>\n\n"
        f"🔥 <b>Цена вышла из полки и удерживается выше неё.</b>"
    )

    sent = await send_tg(msg)

    if sent:
        STATS["pump_signals"] += 1

    return sent


async def send_climax_reversal_signal(base, data):
    msg = (
        f"🥊 <b>КУЛЬМИНАЦИЯ / ВОЗМОЖНЫЙ РАЗВОРОТ</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"💰 Пиковый профит: <b>+{data['profit_pct']}%</b>\n"
        f"📊 RVOL: <b>{data['climax_rvol']}x</b>\n"
        f"🩸 Верхняя тень: <b>{data['wick_ratio_pct']}%</b>\n\n"
        f"🛑 Закрыть лонг: <code>{data['exit_price']:.8g}</code>\n"
        f"📉 Рассмотреть шорт: <code>{data['exit_price']:.8g}</code>\n"
        f"🛑 Стоп: <code>{data['short_stop_loss']:.8g}</code>"
    )

    sent = await send_tg(msg)

    if sent:
        STATS["climax_reversals"] += 1

    return sent


async def send_status_message():
    uptime_hours = (
        time.time() - START_TIME
    ) / 3600

    msg = (
        f"📊 <b>PUMP-HUNTER v12.1</b>\n\n"
        f"⏱ Аптайм: {uptime_hours:.1f}ч\n"
        f"🌐 Universe: {len(UNIVERSE)}\n"
        f"📦 Shelves: {len(SHELVES)}\n"
        f"👁 Tracks: {len(ACTIVE_TRACKS)}\n\n"
        f"🧲 Absorption Watch: {STATS['absorption_watch']}\n"
        f"✅ Absorption Confirmed: {STATS['absorption_confirmed']}\n"
        f"🚀 Pump Signals: {STATS['pump_signals']}\n"
        f"🥊 Climaxes: {STATS['climax_reversals']}\n"
        f"❌ False Breakouts: {STATS['false_breakouts']}\n\n"
        f"🔌 WS Trades: {STATS['ws_trades']}\n"
        f"🔁 WS Reconnects: {STATS['ws_reconnects']}\n"
        f"💓 WS Pings: {STATS['ws_pings']}\n"
        f"📨 TG Fails: {STATS['tg_fails']}\n"
        f"⚠️ Errors: {STATS['errors']}"
    )

    await send_tg(msg)


async def handle_telegram_update(update):
    msg = (
        update.get("message")
        or update.get("channel_post")
    )

    if not msg:
        return

    chat_id = str(
        msg.get("chat", {}).get("id", "")
    )

    if CHAT_ID and chat_id != str(CHAT_ID):
        return

    text = (
        msg.get("text")
        or ""
    ).strip().lower()

    if text.startswith("/start"):
        await send_tg(
            "👋 <b>PUMP-HUNTER v12.1 запущен.</b>\n"
            "FLAT → ABSORPTION → BREAKOUT → PUMP"
        )

    elif text.startswith("/status"):
        await send_status_message()


async def telegram_poll_loop():
    if not BOT_TOKEN:
        return

    offset = None

    while True:
        try:
            params = {"timeout": 25}

            if offset is not None:
                params["offset"] = offset

            async with SESSION.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:

                if r.status == 200:

                    data = await r.json(
                        content_type=None
                    )

                    if data and data.get("ok"):

                        for update in data.get(
                            "result",
                            []
                        ):
                            offset = (
                                update["update_id"]
                                + 1
                            )

                            await handle_telegram_update(
                                update
                            )

                else:
                    await asyncio.sleep(3)

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(5)


# ============================================================
# UNIVERSE
# ============================================================

async def refresh_universe():
    global UNIVERSE

    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()

    if not kc or not bg:
        return

    bg_set = set(bg.keys())
    now_ms = time.time() * 1000
    min_age_ms = (
        MIN_LISTING_AGE_DAYS
        * 86400
        * 1000
    )

    uni = {}

    for base, info in kc.items():

        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue

        if not (
            MIN_PRICE_USDT
            <= info["price"]
            <= MAX_PRICE_USDT
        ):
            continue

        if base not in bg_set:
            continue

        first_open = info.get(
            "first_open_ms",
            0
        )

        if (
            first_open > 0
            and now_ms - first_open < min_age_ms
        ):
            continue

        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }

    sorted_u = sorted(
        uni.items(),
        key=lambda x: x[1]["volume24"],
        reverse=True,
    )

    UNIVERSE = dict(
        sorted_u[:MAX_UNIVERSE_SYMBOLS]
    )

    log.info(
        "🌐 Universe: %d pairs",
        len(UNIVERSE),
    )

    gc.collect()


# ============================================================
# FLAT SCANNER
# ============================================================

async def refresh_shelves():
    if not UNIVERSE:
        return

    STATS["flat_scans"] += 1

    bases = list(UNIVERSE.keys())[
        :MAX_FLAT_CANDIDATES
    ]

    semaphore = asyncio.Semaphore(10)
    found = {}

    async def worker(base):
        async with semaphore:
            try:
                item = UNIVERSE.get(base)

                if not item:
                    return

                candles = await fetch_kucoin_candles(
                    item["kucoin_symbol"],
                    15,
                    SHELF_MAX_CANDLES + 20,
                    cache_key=f"flat15m_{base}",
                )

                if not candles:
                    return

                shelf = detect_shelf(candles)

                if shelf:
                    shelf["base"] = base
                    shelf["kucoin_symbol"] = item[
                        "kucoin_symbol"
                    ]
                    shelf["state"] = "FLAT_DETECTED"
                    shelf["created_ts"] = time.time()

                    found[base] = shelf

            except Exception:
                STATS["errors"] += 1

    await asyncio.gather(
        *[
            worker(base)
            for base in bases
        ]
    )

    now = time.time()

    for base, old in list(SHELVES.items()):

        if base in found:

            new = found[base]

            new["created_ts"] = old.get(
                "created_ts",
                new["created_ts"],
            )

            if old.get("state") in (
                "ABSORPTION_WATCH",
                "ABSORPTION_CONFIRMED",
                "BREAKOUT",
            ):
                new["state"] = old["state"]
                new["absorption"] = old.get(
                    "absorption"
                )
                new["absorption_started"] = old.get(
                    "absorption_started"
                )
                new["breakout_ts"] = old.get(
                    "breakout_ts"
                )
                new["breakout_price"] = old.get(
                    "breakout_price"
                )

            found[base] = new

        elif base in ACTIVE_TRACKS:
            found[base] = old

    for base in list(found.keys()):
        age = now - found[base].get(
            "created_ts",
            now,
        )

        if (
            age > SHELF_MAX_AGE_SEC
            and base not in ACTIVE_TRACKS
        ):
            del found[base]

    ordered = sorted(
        found.items(),
        key=lambda x: (
            x[0] in ACTIVE_TRACKS,
            x[1].get("state")
            in (
                "ABSORPTION_WATCH",
                "ABSORPTION_CONFIRMED",
                "BREAKOUT",
            ),
            x[1].get("score", 0),
        ),
        reverse=True,
    )

    SHELVES.clear()

    for base, shelf in ordered[
        :MAX_ACTIVE_SHELVES
    ]:
        SHELVES[base] = shelf

    # FIX: не накопительный счётчик
    STATS["flat_found"] = len(SHELVES)

    log.info(
        "📦 Flat scan: %d shelves | active tracks: %d",
        len(SHELVES),
        len(ACTIVE_TRACKS),
    )


# ============================================================
# STATE ENGINE
# ============================================================

async def process_watch(base):
    shelf = SHELVES.get(base)

    if not shelf:
        return

    now = time.time()

    # --------------------------------------------------------
    # 1. FLAT -> ABSORPTION WATCH
    # --------------------------------------------------------

    if shelf.get("state") in (
        "FLAT_DETECTED",
        "ABSORPTION_WATCH",
    ):

        absdata = calculate_absorption(
            base,
            shelf,
        )

        if absdata:

            if shelf.get("state") == "FLAT_DETECTED":

                shelf["state"] = "ABSORPTION_WATCH"
                shelf["absorption_started"] = now
                STATS["absorption_watch"] += 1

                await send_absorption_watch(
                    base,
                    shelf,
                    absdata,
                )

            shelf["absorption"] = absdata

            started = shelf.get(
                "absorption_started",
                now,
            )

            if (
                now - started
                >= ABS_CONFIRM_COOLDOWN_SEC
                and absdata["buy_ratio"]
                >= ABS_MIN_BUY_RATIO
                and absdata["cvd"]
                >= ABS_MIN_CVD_USDT
            ):

                if shelf.get(
                    "state"
                ) != "ABSORPTION_CONFIRMED":

                    shelf["state"] = (
                        "ABSORPTION_CONFIRMED"
                    )

                    shelf[
                        "absorption_confirmed_ts"
                    ] = now

                    STATS[
                        "absorption_confirmed"
                    ] += 1

                    log.info(
                        "🧲 ABSORPTION CONFIRMED %s | "
                        "buy %.1f%% | CVD %.0f | spread %.2f%%",
                        base,
                        absdata["buy_ratio"] * 100,
                        absdata["cvd"],
                        absdata["spread_pct"],
                    )

    # --------------------------------------------------------
    # 2. ABSORPTION CONFIRMED -> BREAKOUT
    # --------------------------------------------------------

    if shelf.get("state") == (
        "ABSORPTION_CONFIRMED"
    ):

        bm = breakout_metrics(
            base,
            shelf,
        )

        if not bm:
            return

        if (
            BREAKOUT_MIN_PCT
            <= bm["above_pct"]
            <= BREAKOUT_MAX_PCT
        ):

            shelf["state"] = "BREAKOUT"
            shelf["breakout_ts"] = now
            shelf["breakout_price"] = (
                bm["price"]
            )

            STATS["breakouts"] += 1

            log.info(
                "🚀 BREAKOUT %s +%.2f%%",
                base,
                bm["above_pct"],
            )

    # --------------------------------------------------------
    # 3. BREAKOUT -> PUMP CONFIRMED
    # --------------------------------------------------------

    if shelf.get("state") == "BREAKOUT":

        bm = breakout_metrics(
            base,
            shelf,
        )

        flow = breakout_flow(base)

        if not bm:
            return

        elapsed = (
            now
            - shelf.get(
                "breakout_ts",
                now,
            )
        )

        if bm["above_pct"] > BREAKOUT_MAX_PCT:
            shelf["state"] = "FLAT_DETECTED"
            STATS["false_breakouts"] += 1
            return

        if (
            bm["above_pct"]
            < -BREAKOUT_MAX_RETURN_INTO_SHELF_PCT
        ):

            shelf["state"] = "FLAT_DETECTED"
            shelf.pop("breakout_ts", None)
            shelf.pop("breakout_price", None)

            STATS["false_breakouts"] += 1

            log.info(
                "❌ FALSE BREAKOUT %s",
                base,
            )

            return

        if elapsed > BREAKOUT_CONFIRM_WINDOW_SEC:

            shelf["state"] = "FLAT_DETECTED"

            STATS["false_breakouts"] += 1

            return

        if (
            bm["above_pct"]
            >= BREAKOUT_CONFIRM_PCT
            and flow
            and flow["buy_ratio"]
            >= BREAKOUT_MIN_FLOW_BUY_RATIO
            and flow["cvd"]
            >= BREAKOUT_MIN_CVD_USDT
        ):

            shelf["state"] = "PUMP_CONFIRMED"

            entry = bm["price"]

            ACTIVE_TRACKS[base] = {
                "entry": entry,
                "ts": now,
                "shelf_high": shelf["high"],
            }

            absdata = (
                shelf.get("absorption")
                or calculate_absorption(
                    base,
                    shelf,
                )
            )

            if absdata:
                await send_pump_signal(
                    base,
                    shelf,
                    absdata,
                    bm,
                )

            else:
                await send_tg(
                    f"🟢 <b>PUMP CONFIRMED</b>\n\n"
                    f"📌 <b>{escape(base)}USDT</b>\n"
                    f"🚀 Breakout: "
                    f"<b>+{bm['above_pct']:.2f}%</b>\n"
                    f"💵 Price: "
                    f"<code>{bm['price']:.8g}</code>"
                )

            LAST_SIGNAL[base] = now


# ============================================================
# ACTIVE PUMP TRACKER
# ============================================================

async def process_active_track(base):
    track = ACTIVE_TRACKS.get(base)

    if not track:
        return

    now = time.time()

    if (
        now - track["ts"]
        > CLIMAX_TRACK_TIMEOUT_SEC
    ):

        ACTIVE_TRACKS.pop(
            base,
            None,
        )

        STATS["tracks_timed_out"] += 1

        return

    price = get_current_price(base)

    if price > 0:

        change = (
            (price - track["entry"])
            / track["entry"]
            * 100
        )

        if change <= CLIMAX_INVALIDATE_PCT:

            ACTIVE_TRACKS.pop(
                base,
                None,
            )

            STATS["tracks_invalidated"] += 1

            return

    item = UNIVERSE.get(base)

    if not item:
        return

    # NEW: кешированные 15m свечи для активных треков
    candles = await fetch_kucoin_candles(
        item["kucoin_symbol"],
        15,
        25,
        cache_key=f"track15m_{base}",
    )

    if not candles:
        return

    climax = check_climax_reversal(
        candles,
        track["entry"],
    )

    if climax:

        await send_climax_reversal_signal(
            base,
            climax,
        )

        ACTIVE_TRACKS.pop(
            base,
            None,
        )


# ============================================================
# SCAN CYCLE
# ============================================================

async def scan_cycle():
    if not UNIVERSE:
        return

    STATS["scans"] += 1

    bases = list(SHELVES.keys())

    semaphore = asyncio.Semaphore(8)

    async def worker(base):
        async with semaphore:
            try:
                await process_watch(base)
                await process_active_track(base)

            except Exception:
                STATS["errors"] += 1
                log.exception(
                    "Process error %s",
                    base,
                )

    await asyncio.gather(
        *[
            worker(base)
            for base in bases
        ]
    )


# ============================================================
# LOOPS
# ============================================================

async def universe_loop():
    while True:

        try:
            await asyncio.sleep(
                UNIVERSE_REFRESH_SEC
            )

            await refresh_universe()

        except asyncio.CancelledError:
            raise

        except Exception:
            STATS["errors"] += 1
            await asyncio.sleep(10)


async def flat_loop():
    while True:

        try:
            if UNIVERSE:
                await refresh_shelves()

        except asyncio.CancelledError:
            raise

        except Exception:
            STATS["errors"] += 1

        await asyncio.sleep(
            FLAT_SCAN_INTERVAL_SEC
        )


async def scan_loop():
    while True:

        try:
            await scan_cycle()

        except asyncio.CancelledError:
            raise

        except Exception:
            STATS["errors"] += 1

        await asyncio.sleep(
            SCAN_INTERVAL_SEC
        )


# ============================================================
# WEB
# ============================================================

async def index(req):
    return web.Response(
        text=(
            f"PUMP-HUNTER v12.1 | "
            f"Uni={len(UNIVERSE)} | "
            f"Shelves={len(SHELVES)} | "
            f"Tracks={len(ACTIVE_TRACKS)} | "
            f"Pumps={STATS['pump_signals']} | "
            f"WS trades={STATS['ws_trades']}"
        ),
        content_type="text/plain",
    )


# ============================================================
# START / STOP
# ============================================================

async def start(app):
    global SESSION
    global HTTP_SEMAPHORE
    global START_TIME
    global WS_TASK

    START_TIME = time.time()

    SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=30,
            ttl_dns_cache=300,
        )
    )

    HTTP_SEMAPHORE = asyncio.Semaphore(20)

    if BOT_TOKEN:

        try:
            async with SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
                params={
                    "drop_pending_updates": "false"  # FIX
                },
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:

                log.info(
                    "deleteWebhook: HTTP %s",
                    r.status,
                )

        except Exception as e:
            log.warning(
                "deleteWebhook error: %s",
                e,
            )

    await refresh_universe()
    await refresh_shelves()

    app["universe_task"] = asyncio.create_task(
        universe_loop()
    )

    app["flat_task"] = asyncio.create_task(
        flat_loop()
    )

    app["scan_task"] = asyncio.create_task(
        scan_loop()
    )

    app["poll_task"] = asyncio.create_task(
        telegram_poll_loop()
    )

    WS_TASK = asyncio.create_task(
        websocket_loop()
    )

    app["ws_task"] = WS_TASK

    await send_tg(
        "🚀 <b>PUMP-HUNTER v12.1 запущен</b>\n"
        "FLAT → ABSORPTION → BREAKOUT → PUMP → CLIMAX\n\n"
        f"• Universe: {len(UNIVERSE)}\n"
        f"• Shelves: {len(SHELVES)}\n"
        f"• WS: KuCoin Futures execution stream"
    )


async def stop(app):
    for key in (
        "universe_task",
        "flat_task",
        "scan_task",
        "poll_task",
        "ws_task",
    ):

        task = app.get(key)

        if task:
            task.cancel()

            try:
                await task

            except asyncio.CancelledError:
                pass

    if SESSION and not SESSION.closed:
        await SESSION.close()


app = web.Application()

app.router.add_get(
    "/",
    index,
)

app.router.add_get(
    "/health",
    index,
)

app.on_startup.append(start)
app.on_cleanup.append(stop)


if __name__ == "__main__":
    web.run_app(
        app,
        host="0.0.0.0",
        port=PORT,
    )
