# -*- coding: utf-8 -*-
"""
Skaner krypto V5.0

Zostaje logika V4.0. W tej wersji:
- Najwyżej 1 nowy alert na skan. Drugi setup dostaje BLOCK_.
- Filtr 3h: alert tylko gdy zamknięcie świecy 4H było nie dawniej niż 3 godziny temu.
  Starszy setup odpada, nawet jeśli warunki nadal się świecą.
- Świeży sygnał BTC/ETH (ten skan albo ostatnie COOLDOWN_HOURS) wycisza alty.
- Cooldown pary po wysłanym alercie (domyślnie 12h).
- Płynność: średni obrót 20×4H poniżej MIN_QUOTE_VOLUME odpada.
- Alt słabszy od BTC o REL_LAG_PCT na 12h odpada.
- Kontrtrend 4H wyłączony, chyba że ALLOW_COUNTERTREND=1.
- Paper log: paper_krypto.csv. Po alercie śledzi SL, TP albo wyjście po 12 świecach.
- Brak danych pary widać w digestcie, nie znika po cichu.
- Raz dziennie, gdy nie ma alertu, Telegram dostaje „brak nowych sygnałów”.
- Wiadomości Telegrama są w HTML.

CACHE: CRYPTO_CACHE_PATH albo cache_krypto.json.
PAPER: PAPER_PATH albo paper_krypto.csv.
Na Actions commituj oba pliki.
"""

import asyncio
import csv
import datetime
import html
import json
import logging
import math
import os
import re
import tempfile
import time
from zoneinfo import ZoneInfo

import ccxt.async_support as ccxt
import gspread
import httpx
import numpy as np
import pandas as pd
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip() or None
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_TASKS_CREDENTIALS")

CORE_CRYPTO = ["BTC/GBP", "ETH/GBP"]
ALT_CRYPTO = [
    "SOL/GBP", "TAO/USD", "RENDER/USD",
    "ONDO/USD", "XRP/GBP", "LINK/GBP",
    "SUI/GBP", "AAVE/GBP", "FET/USD",
    "NEAR/USD", "HNT/USD", "ARB/USD",
]

env_crypto = os.getenv("CRYPTO_PAIRS")
if env_crypto:
    SYMBOLS = [s.strip() for s in env_crypto.split(",") if s.strip()]
else:
    SYMBOLS = CORE_CRYPTO + ALT_CRYPTO

CACHE_FILE = os.getenv("CRYPTO_CACHE_PATH", "cache_krypto.json")
PAPER_FILE = os.getenv("PAPER_PATH", "paper_krypto.csv")
OHLCV_4H_LIMIT = 300
OHLCV_1D_LIMIT = 260
MIN_RR = 1.2
FEE_PCT = 0.60
BTC_DUMP_4H = -2.5
BTC_DUMP_12H = -4.0
PAPER_BARS = 12
PAPER_FIELDS = ["ts", "symbol", "signal", "entry", "sl", "tp", "quote", "status", "exit", "bars", "note"]


def env_float(name, default):
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logging.warning("Zła wartość %s=%s, zostawiam %s", name, raw, default)
        return default


def env_int(name, default):
    return int(env_float(name, default))


def env_flag(name):
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "tak"}


MAX_ALERTS = 1
COOLDOWN_SEC = max(1, env_int("COOLDOWN_HOURS", 12)) * 3600
FRESH_HOURS = 3
FRESH_SEC = FRESH_HOURS * 3600
CANDLE_4H_SEC = 4 * 3600
MIN_QUOTE_VOLUME = env_float("MIN_QUOTE_VOLUME", 25000)
REL_LAG_PCT = env_float("REL_LAG_PCT", 3.0)
ALLOW_COUNTERTREND = env_flag("ALLOW_COUNTERTREND")

STATUS_TEXT = {
    "NONE": "⚪ Neutralny",
    "BLOCK_DAILY": "🛡 Blokada (1D pod SMA200)",
    "BLOCK_TREND": "⛔ Blokada (brak trendu 4H)",
    "BLOCK_BTC": "🛡 Blokada (BTC dump)",
    "BLOCK_LIQ": "⛔ Blokada (płynność)",
    "BLOCK_REL": "🛡 Blokada (słabszy od BTC)",
    "BLOCK_COOLDOWN": "⏳ Cooldown po ostatnim alercie",
    "BLOCK_FRESH": "⏳ Poza oknem 3h",
    "BUY_REBOUND": "⚡️ SZYBKIE ODBICIE (Popyt)",
    "BUY_PINBAR": "🕯️ PINBAR ODBICIE",
    "BUY_BOUNCE": "🟡 STREFA ODBICIA",
    "BUY_COUNTER": "⚠️ KONTRTREND 4H (1D nadal nad SMA200)",
}


def within_fresh_window(ts_open, now=None, fresh_sec=FRESH_SEC):
    """Świeca 4H jest świeża przez 3h od zamknięcia. Lekki ujemny wiek to zegar, nie przyszłość."""
    now = time.time() if now is None else now
    age = now - (int(ts_open) + CANDLE_4H_SEC)
    return -120 <= age <= fresh_sec


def h(value) -> str:
    return html.escape(str(value), quote=False)


def link(label, url) -> str:
    return f'<a href="{html.escape(url, quote=True)}">{h(label)}</a>'


def allowed_chat_ids():
    found = set()
    if TELEGRAM_CHAT_ID:
        found.add(str(TELEGRAM_CHAT_ID))
    extra = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS") or ""
    for part in extra.split(","):
        part = part.strip()
        if part:
            found.add(part)
    return found


def chat_allowed(chat_id) -> bool:
    return chat_id is not None and str(chat_id) in allowed_chat_ids()


def is_status_command(text: str) -> bool:
    if not text:
        return False
    head = text.split()[0].split("@", 1)[0]
    return head in {"/status", "/stan", "/start"}


def quote_of(symbol: str) -> str:
    return symbol.split("/")[-1].upper()


def money(amount: float, quote: str) -> str:
    sign = "£" if quote == "GBP" else "$" if quote == "USD" else ""
    suffix = "" if sign else f" {quote}"
    if amount >= 1:
        body = f"{amount:,.2f}"
    elif amount >= 0.01:
        body = f"{amount:,.4f}"
    else:
        body = f"{amount:,.6f}"
    return f"{sign}{body}{suffix}"


def fnum(row, key):
    try:
        value = float(row[key])
    except (KeyError, TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def fail_row(symbol, reason):
    line = f"⚠️ {h(symbol)}: {h(reason)}\n\n"
    return {
        "ok": False,
        "symbol": symbol,
        "digest_row": {
            "symbol": symbol,
            "price": "—",
            "rsi": "—",
            "daily": reason,
            "pinbar": "—",
            "status": reason,
            "digest_line": line,
        },
        "pending": None,
        "suppress_key": None,
        "paper_touched": False,
    }


def init_google_sheet():
    if not GOOGLE_CREDENTIALS or not SPREADSHEET_ID:
        logging.info("Brak Google Sheets. Pomijam arkusz.")
        return None
    try:
        raw_id = SPREADSHEET_ID.strip()
        if "/d/" in raw_id:
            raw_id = raw_id.split("/d/")[1].split("/")[0]
        clean_sheet_id = re.sub(r"[^a-zA-Z0-9-_]", "", raw_id)
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        if isinstance(creds_dict.get("private_key"), str):
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds).open_by_key(clean_sheet_id).worksheet("Krypto")
    except Exception as exc:
        logging.error("Błąd inicjalizacji Google Sheets: %s", exc)
        return None


def _unix_in_key(key: str):
    for part in key.split("_"):
        if part.isdigit() and len(part) >= 10:
            return int(part)
    return None


def prune_cache(cache_data):
    now_ts = int(time.time())
    keep = {}
    for key, value in cache_data.items():
        if key.startswith("ALERT_") or key.startswith("BLOCK_"):
            stamped = _unix_in_key(key)
            if stamped is not None and (now_ts - stamped) > 14 * 86400:
                continue
        elif key.startswith("COOLDOWN_"):
            try:
                if int(value) < now_ts:
                    continue
            except (TypeError, ValueError):
                continue
        elif key.startswith("HEARTBEAT_"):
            day = key.replace("HEARTBEAT_", "", 1)
            try:
                stamped = datetime.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
                if (datetime.datetime.now(datetime.timezone.utc) - stamped).days > 14:
                    continue
            except ValueError:
                continue
        keep[key] = value
    return keep


def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        logging.error("Nie udało się wczytać cache: %s", exc)
        return {}


def save_cache(cache_data):
    if cache_data is None:
        return
    temp_name = None
    try:
        cleaned = prune_cache(cache_data)
        cache_data.clear()
        cache_data.update(cleaned)
        directory = os.path.dirname(os.path.abspath(CACHE_FILE)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(dir=directory, prefix=".cache-", suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(cache_data, handle, indent=2, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, CACHE_FILE)
        temp_name = None
    except Exception as exc:
        logging.error("Nie udało się zapisać cache: %s", exc)
        if temp_name:
            try:
                os.unlink(temp_name)
            except OSError:
                pass


def load_paper():
    if not os.path.exists(PAPER_FILE):
        return []
    try:
        with open(PAPER_FILE, "r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    except Exception as exc:
        logging.error("Nie udało się wczytać paper log: %s", exc)
        return []


def save_paper(rows):
    directory = os.path.dirname(os.path.abspath(PAPER_FILE)) or "."
    os.makedirs(directory, exist_ok=True)
    temp_name = None
    try:
        fd, temp_name = tempfile.mkstemp(dir=directory, prefix=".paper-", suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=PAPER_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in PAPER_FIELDS})
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, PAPER_FILE)
        temp_name = None
    except Exception as exc:
        logging.error("Nie udało się zapisać paper log: %s", exc)
        if temp_name:
            try:
                os.unlink(temp_name)
            except OSError:
                pass


def walk_paper(df, trade, max_bars=PAPER_BARS):
    """SL ma pierwszeństwo, także gdy SL i TP wpadają w tę samą świecę."""
    if str(trade.get("status") or "OPEN") != "OPEN":
        return False
    try:
        entry_ts = int(float(trade["ts"]))
        sl = float(trade["sl"])
        tp = float(trade["tp"])
    except (KeyError, TypeError, ValueError):
        return False
    bars = 0
    for rec in df.iloc[:-1].itertuples(index=False):
        if int(rec.timestamp) // 1000 <= entry_ts:
            continue
        bars += 1
        low = float(rec.low)
        high = float(rec.high)
        close = float(rec.close)
        hit_sl = low <= sl
        hit_tp = high >= tp
        if hit_sl and hit_tp:
            trade.update(status="SL", exit=sl, bars=bars, note="SL i TP w jednej świecy, liczę SL")
            return True
        if hit_sl:
            trade.update(status="SL", exit=sl, bars=bars, note="")
            return True
        if hit_tp:
            trade.update(status="TP", exit=tp, bars=bars, note="")
            return True
        if bars >= max_bars:
            trade.update(status="TIME", exit=close, bars=bars, note="zamknięcie po czasie")
            return True
    return False


def select_alerts(pending, max_alerts, core_blocks_alts):
    pool = [item for item in pending if item["is_core"]] if core_blocks_alts else list(pending)
    pool.sort(key=lambda item: (not item["is_core"], -float(item["rr"])))
    kept = pool[:max_alerts]
    kept_ids = {id(item) for item in kept}
    dropped = [item for item in pending if id(item) not in kept_ids]
    return kept, dropped


async def send_telegram_alert_async(http_client, msg: str, custom_chat_id=None):
    """True = wysłane, False = błąd, None = brak tokena albo czatu."""
    chat_id = custom_chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_TOKEN or not chat_id:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in (1, 2):
        try:
            response = await http_client.post(url, json=payload, timeout=15.0)
            response.raise_for_status()
            return True
        except Exception as exc:
            logging.error("Błąd wysyłki Telegram (próba %s): %s", attempt, exc)
            if attempt == 1:
                await asyncio.sleep(1.0)
    return False


def split_messages(header: str, blocks, limit=3800):
    if not blocks:
        return []
    cont = "📋 <b>ciąg dalszy</b>\n───────────────────\n\n"
    messages = []
    current = header
    for block in blocks:
        if len(current) + len(block) > limit and current != header:
            messages.append(current.rstrip())
            current = cont + block
        else:
            current += block
    if current.strip():
        messages.append(current.rstrip())
    return messages


def write_github_step_summary(digest_rows, health_msg=""):
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_file:
        return
    try:
        with open(summary_file, "a", encoding="utf-8") as handle:
            if health_msg:
                handle.write(f"> {health_msg}\n\n")
            handle.write("### Podsumowanie skanera krypto V5.0\n\n")
            handle.write("| Moneta | Cena | RSI 4H | 1D | Pinbar | Stan |\n")
            handle.write("| --- | --- | --- | --- | --- | --- |\n")
            for row in digest_rows:
                handle.write(
                    "| **{symbol}** | {price} | {rsi} | {daily} | {pinbar} | {status} |\n".format(
                        symbol=str(row["symbol"]).replace("|", "/"),
                        price=str(row["price"]).replace("|", "/"),
                        rsi=row["rsi"],
                        daily=str(row["daily"]).replace("|", "/"),
                        pinbar=str(row["pinbar"]).replace("|", "/"),
                        status=str(row["status"]).replace("|", "/"),
                    )
                )
    except Exception as exc:
        logging.error("Błąd GITHUB_STEP_SUMMARY: %s", exc)


async def process_telegram_commands_async(http_client, cache, digest_rows):
    if not TELEGRAM_TOKEN:
        return
    last_offset = int(cache.get("telegram_update_offset", 0) or 0)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_offset + 1}&timeout=2"
    try:
        response = await http_client.get(url, timeout=10.0)
        if response.status_code != 200:
            logging.error("Telegram getUpdates HTTP %s", response.status_code)
            return
        reply_to = None
        for update in response.json().get("result", []):
            cache["telegram_update_offset"] = update["update_id"]
            message = update.get("message") or update.get("edited_message") or {}
            text = str(message.get("text", "")).strip().lower()
            chat_id = message.get("chat", {}).get("id")
            if is_status_command(text) and chat_allowed(chat_id):
                reply_to = chat_id
            elif is_status_command(text):
                logging.info("Pominięto /status z czatu %s", chat_id)
        if reply_to is None:
            return
        blocks = [row["digest_line"] for row in digest_rows]
        header = "📋 <b>AKTUALNY STAN RYNKU KRYPTO</b>\n───────────────────\n\n"
        for part in split_messages(header, blocks):
            await send_telegram_alert_async(http_client, part, custom_chat_id=reply_to)
            await asyncio.sleep(0.4)
    except Exception as exc:
        logging.error("Błąd Telegram Commands: %s", exc)


async def fetch_ohlcv_async(exchange_inst, symbol, timeframe="4h", limit=OHLCV_4H_LIMIT):
    try:
        ohlcv = await exchange_inst.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv:
            logging.warning("Pusta historia %s %s", symbol, timeframe)
            return None
        return pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    except Exception as exc:
        logging.warning("Błąd pobierania %s (%s): %s", symbol, timeframe, exc)
        return None


async def get_gbp_per_usd(exchange_inst):
    """Ile GBP za 1 USD. None, gdy kursu nie ma."""
    try:
        ticker = await exchange_inst.fetch_ticker("GBP/USD")
        gbp_usd = float(ticker["last"])
        if gbp_usd > 0:
            return 1.0 / gbp_usd
    except Exception as exc:
        logging.warning("Brak kursu GBP/USD, pary USD bez przeliczenia: %s", exc)
    return None


def calculate_indicators(df, min_wick_ratio=0.60, max_upper_wick_ratio=0.20):
    df = df.copy()
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean().replace(0, 1e-10)
    rs = avg_gain / avg_loss
    df["rsi"] = (100 - (100 / (1 + rs))).round(2)

    rsi_min = df["rsi"].rolling(window=14).min()
    rsi_max = df["rsi"].rolling(window=14).max()
    stoch_rsi = (df["rsi"] - rsi_min) / (rsi_max - rsi_min + 1e-10) * 100
    df["stoch_k"] = stoch_rsi.rolling(window=3).mean().round(2)
    df["stoch_d"] = df["stoch_k"].rolling(window=3).mean().round(2)

    df["sma20"] = df["close"].rolling(window=20).mean()
    df["sma50"] = df["close"].rolling(window=50).mean()
    df["sma200"] = df["close"].rolling(window=200).mean()
    df["std20"] = df["close"].rolling(window=20).std(ddof=0)
    df["bb_upper"] = df["sma20"] + (df["std20"] * 2)
    df["bb_lower"] = df["sma20"] - (df["std20"] * 2)
    df["vol_sma20"] = df["volume"].rolling(window=20).mean()

    true_range = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = true_range.rolling(window=14).mean()

    span = df["high"] - df["low"]
    body = (df["close"] - df["open"]).abs()
    lower_wick = df[["open", "close"]].min(axis=1) - df["low"]
    upper_wick = df["high"] - df[["open", "close"]].max(axis=1)
    safe_span = span.replace(0, np.nan)
    df["lower_wick_ratio"] = lower_wick / safe_span
    df["pinbar"] = (
        (span > 0)
        & (df["lower_wick_ratio"] >= min_wick_ratio).fillna(False)
        & ((upper_wick / safe_span) <= max_upper_wick_ratio).fillna(False)
        & ((body / safe_span) <= 0.35).fillna(False)
    )
    return df


def closed_pair(df):
    """CCXT dokleja niedomkniętą świecę na końcu. Sygnał jest na iloc[-2]."""
    if df is None or len(df) < 5:
        return None, None
    return df.iloc[-2], df.iloc[-3]


def close_change(df, older=-5, newer=-2):
    if df is None or len(df) < abs(older) + 1:
        return None
    new = fnum(df.iloc[newer], "close")
    old = fnum(df.iloc[older], "close")
    if new is None or old is None or old <= 0:
        return None
    return (new - old) / old * 100


def volume_ratio(row) -> int:
    volume = fnum(row, "volume")
    average = fnum(row, "vol_sma20")
    if volume is None or average is None or average <= 0:
        return 0
    return int((volume / average) * 100)


def candle_bits(row):
    opened = fnum(row, "open")
    closed = fnum(row, "close")
    low = fnum(row, "low")
    lower_band = fnum(row, "bb_lower")
    rsi = fnum(row, "rsi")
    if None in (opened, closed, low, rsi):
        return None
    return {
        "green": closed > opened,
        "bb": lower_band is not None and low <= lower_band,
        "rsi": rsi,
        "pinbar": bool(row["pinbar"]) if pd.notna(row.get("pinbar", False)) else False,
        "vol": volume_ratio(row),
    }


def setup_type(row, rsi_max, pinbar_rsi, vol_floor):
    bits = candle_bits(row)
    if bits is None or bits["vol"] < 80:
        return "NONE"
    if bits["pinbar"] and bits["rsi"] <= pinbar_rsi:
        return "BUY_PINBAR"
    if bits["vol"] < vol_floor:
        return "NONE"
    if bits["bb"] and bits["rsi"] <= rsi_max and (bits["green"] or bits["vol"] >= 150):
        return "BUY_REBOUND"
    if bits["bb"] and bits["rsi"] <= 38 and bits["green"]:
        return "BUY_BOUNCE"
    return "NONE"


def countertrend_ok(row) -> bool:
    bits = candle_bits(row)
    if bits is None or not bits["green"] or not bits["bb"]:
        return False
    if bits["pinbar"] and bits["rsi"] <= 30 and bits["vol"] >= 130:
        return True
    return bits["rsi"] <= 25 and bits["vol"] >= 150


def trend_4h(price, row):
    sma200 = fnum(row, "sma200")
    if sma200 is None:
        return False, "Brak SMA200 (4H) — blokada"
    if price >= sma200:
        return True, "Powyżej SMA200 (4H)"
    return False, "Poniżej SMA200 (4H)"


def daily_state(df_1d):
    if df_1d is None or len(df_1d) < 210:
        return False, "Brak historii 1D (blokada)"
    frame = calculate_indicators(df_1d)
    row, _prev = closed_pair(frame)
    if row is None:
        return False, "Brak zamkniętej świecy 1D (blokada)"
    close = fnum(row, "close")
    sma200 = fnum(row, "sma200")
    sma50 = fnum(row, "sma50")
    rsi = fnum(row, "rsi")
    if close is None or sma200 is None:
        return False, "Brak SMA200 (1D) — blokada"
    above = close >= sma200
    label = "Powyżej SMA200 (1D)" if above else "Poniżej SMA200 (1D)"
    if rsi is not None:
        label += f", RSI {rsi:.1f}"
    if not above:
        return False, label
    if sma50 is not None and rsi is not None and rsi < 38 and close < sma50:
        return False, label + " — wodospad pod SMA50"
    return True, label


def resolve_signal(raw, above_4h, daily_ok, dumping, is_core, extreme):
    if raw == "NONE":
        return "NONE", "NONE"
    if not daily_ok:
        return "NONE", "BLOCK_DAILY"
    if dumping and not is_core:
        return "NONE", "BLOCK_BTC"
    if above_4h:
        return raw, raw
    if extreme and raw in {"BUY_PINBAR", "BUY_REBOUND"}:
        return "BUY_COUNTER", "BUY_COUNTER"
    return "NONE", "BLOCK_TREND"


def stoch_turning_up(current, previous) -> bool:
    current_k = fnum(current, "stoch_k")
    previous_k = fnum(previous, "stoch_k")
    if current_k is None or previous_k is None:
        return False
    return (previous_k <= 25 or current_k <= 25) and current_k > previous_k


async def process_symbol(
    exchange_inst, symbol, usd_to_gbp, cache, papers,
    btc_dumping=False, btc_ret_12h=None, pre_fetched_df_4h=None,
):
    try:
        is_core = symbol in CORE_CRYPTO
        pinbar_wick = 0.55 if is_core else 0.60
        rsi_max = 35.0 if is_core else 33.0
        pinbar_rsi = 38.0 if is_core else 36.0
        vol_floor = 110

        if pre_fetched_df_4h is not None:
            raw_df = pre_fetched_df_4h
            df_1d = await fetch_ohlcv_async(exchange_inst, symbol, timeframe="1d", limit=OHLCV_1D_LIMIT)
        else:
            raw_df, df_1d = await asyncio.gather(
                fetch_ohlcv_async(exchange_inst, symbol, timeframe="4h", limit=OHLCV_4H_LIMIT),
                fetch_ohlcv_async(exchange_inst, symbol, timeframe="1d", limit=OHLCV_1D_LIMIT),
            )

        if raw_df is None or len(raw_df) < 210:
            logging.warning("Za mało świec 4H dla %s: %s", symbol, 0 if raw_df is None else len(raw_df))
            return fail_row(symbol, "Brak historii 4H")

        paper_touched = False
        for trade in papers:
            if trade.get("symbol") == symbol and walk_paper(raw_df, trade):
                paper_touched = True
                logging.info(
                    "Paper %s %s → %s po %s świecach",
                    symbol, trade.get("signal"), trade.get("status"), trade.get("bars"),
                )

        df = calculate_indicators(raw_df, min_wick_ratio=pinbar_wick, max_upper_wick_ratio=0.20)
        last_row, prev_row = closed_pair(df)
        if last_row is None or prev_row is None:
            return fail_row(symbol, "Brak zamkniętej świecy 4H")

        price = fnum(last_row, "close")
        signal_low = fnum(last_row, "low")
        sma20 = fnum(last_row, "sma20")
        bb_upper = fnum(last_row, "bb_upper")
        atr = fnum(last_row, "atr")
        rsi_4h = fnum(last_row, "rsi")
        if None in (price, signal_low, rsi_4h) or price <= 0:
            return fail_row(symbol, "Niepełna świeca 4H")
        if atr is None:
            atr = signal_low * 0.01
        if sma20 is None:
            sma20 = price
        if bb_upper is None:
            bb_upper = price * 1.05

        above_4h, trend_txt = trend_4h(price, last_row)
        daily_ok, daily_txt = daily_state(df_1d)
        raw = setup_type(last_row, rsi_max, pinbar_rsi, vol_floor)
        extreme = ALLOW_COUNTERTREND and (not above_4h) and countertrend_ok(last_row)
        final_signal, status_key = resolve_signal(raw, above_4h, daily_ok, btc_dumping, is_core, extreme)

        avg_vol = fnum(last_row, "vol_sma20") or 0.0
        quote_vol = avg_vol * price
        illiquid = quote_vol < MIN_QUOTE_VOLUME
        alt_12h = close_change(df)
        lagging = (
            not is_core
            and btc_ret_12h is not None
            and alt_12h is not None
            and alt_12h < btc_ret_12h - REL_LAG_PCT
        )
        if final_signal != "NONE" and lagging:
            logging.info("%s słabszy od BTC: %.2f%% vs %.2f%%", symbol, alt_12h, btc_ret_12h)
            final_signal, status_key = "NONE", "BLOCK_REL"
        if illiquid:
            logging.info("%s płynność %.0f < %.0f", symbol, quote_vol, MIN_QUOTE_VOLUME)
            final_signal, status_key = "NONE", "BLOCK_LIQ"

        prev_raw = setup_type(prev_row, rsi_max, pinbar_rsi, vol_floor)
        prev_above, _prev_trend = trend_4h(fnum(prev_row, "close") or 0, prev_row)
        prev_extreme = ALLOW_COUNTERTREND and (not prev_above) and countertrend_ok(prev_row)
        prev_final, _prev_status = resolve_signal(
            prev_raw, prev_above, daily_ok, btc_dumping, is_core, prev_extreme
        )

        ts_closed = int(last_row["timestamp"] / 1000)
        prev_ts = int(prev_row["timestamp"] / 1000)
        signal_for_key = final_signal if final_signal != "NONE" else raw
        candle_key = f"ALERT_{symbol}_{ts_closed}_{signal_for_key}"
        prev_blocked = bool(cache.get(f"BLOCK_{symbol}_{prev_ts}"))
        already = bool(cache.get(candle_key))
        is_new = final_signal != prev_final or prev_blocked
        cooled = int(cache.get(f"COOLDOWN_{symbol}") or 0) > ts_closed

        vol_ratio = volume_ratio(last_row)
        if vol_ratio >= 120:
            volume_str = f"{vol_ratio}% średniej (wysoki)"
        elif vol_ratio <= 80:
            volume_str = f"{vol_ratio}% średniej (niski)"
        elif vol_ratio == 0:
            volume_str = "brak danych"
        else:
            volume_str = f"{vol_ratio}% średniej (standard)"

        quote = quote_of(symbol)
        gbp_note = ""
        if quote == "USD" and usd_to_gbp:
            gbp_note = f" (≈ {money(price * usd_to_gbp, 'GBP')})"

        wick = fnum(last_row, "lower_wick_ratio")
        is_pinbar = bool(last_row["pinbar"]) if pd.notna(last_row.get("pinbar", False)) else False
        pinbar_txt = f"{int(round(wick * 100))}%" if is_pinbar and wick is not None else "Brak"
        status_txt = STATUS_TEXT.get(status_key, STATUS_TEXT["NONE"])
        price_disp = money(price, quote) + gbp_note
        tv_symbol = symbol.replace("/", "")
        tv_link = f"https://www.tradingview.com/chart/?symbol=KRAKEN:{tv_symbol}"
        icon = "🏛" if is_core else "🚀"

        digest_line = (
            f"{icon} {link(symbol, tv_link)}: <code>{h(price_disp)}</code>\n"
            f"├ RSI (4H): <code>{h(rsi_4h)}</code> | {h(trend_txt)}\n"
            f"├ 1D: {h(daily_txt)}\n"
            f"├ Pinbar: {h(pinbar_txt)}\n"
            f"└ Stan: {h(status_txt)}\n\n"
        )
        digest_row = {
            "symbol": symbol,
            "icon": icon,
            "price": price_disp,
            "rsi": rsi_4h,
            "daily": daily_txt,
            "pinbar": pinbar_txt,
            "status": status_txt,
            "digest_line": digest_line,
        }

        pending = None
        suppress_key = None
        if raw != "NONE" and final_signal == "NONE":
            suppress_key = f"BLOCK_{symbol}_{ts_closed}"

        sendable = final_signal != "NONE"
        block_status = None
        if sendable and cooled:
            block_status = "BLOCK_COOLDOWN"
            logging.info("%s w cooldownie do świecy po %s", symbol, cache.get(f"COOLDOWN_{symbol}"))
        elif sendable and not within_fresh_window(ts_closed):
            block_status = "BLOCK_FRESH"
            logging.info("%s poza oknem %sh od zamknięcia świecy 4H", symbol, FRESH_HOURS)
        if block_status:
            status_txt = STATUS_TEXT[block_status]
            digest_row["status"] = status_txt
            head, _sep, _old = digest_line.rpartition("└ Stan: ")
            digest_row["digest_line"] = f"{head}└ Stan: {h(status_txt)}\n\n"
            sendable = False
            suppress_key = f"BLOCK_{symbol}_{ts_closed}"

        if sendable:
            soft_sl = max(1e-12, signal_low - (1.5 * atr))
            hard_sl = max(1e-12, min(soft_sl * 0.98, signal_low * 0.95))
            tp = sma20 if price < sma20 else bb_upper
            tp_name = "Średnia SMA 20" if price < sma20 else "Górna BB 20"
            risk_pct = round(((price - soft_sl) / price) * 100, 2)
            hard_risk_pct = round(((price - hard_sl) / price) * 100, 2)
            tp_pct = round(((tp - price) / price) * 100, 2)
            net_tp = max(0.0, tp_pct - FEE_PCT)
            net_risk = risk_pct + FEE_PCT
            rr_ratio = round(net_tp / net_risk, 2) if net_risk > 0 else 0.0

            if already:
                logging.info("%s %s już wysłany dla %s", symbol, final_signal, ts_closed)
            elif not is_new:
                logging.info("%s %s to kontynuacja, nie nowy setup", symbol, final_signal)
            elif rr_ratio < MIN_RR:
                logging.info("%s %s pominięty: R:R %.2f < %.2f", symbol, final_signal, rr_ratio, MIN_RR)
                suppress_key = f"BLOCK_{symbol}_{ts_closed}"
            else:
                bits = candle_bits(last_row) or {}
                confirmations = [
                    f"📈 <b>Trend 4H:</b> {h(trend_txt)}",
                    f"🛡 <b>Kontekst 1D:</b> {h(daily_txt)}",
                ]
                if final_signal == "BUY_COUNTER":
                    confirmations.append("⚠️ <b>Kontrtrend:</b> 4H pod SMA200, 1D nadal nad SMA200. Mniejszy rozmiar.")
                if is_pinbar:
                    confirmations.append(f"🕯 <b>Knot popytowy 4H:</b> {h(pinbar_txt)} (próg {int(pinbar_wick * 100)}%)")
                if bits.get("green"):
                    confirmations.append("🟩 <b>Bycze zamknięcie</b> świecy 4H")
                if bits.get("bb"):
                    confirmations.append("📉 <b>Test dolnej wstęgi Bollingera (20, 2)</b>")
                if stoch_turning_up(last_row, prev_row):
                    confirmations.append(f"🔄 <b>Stochastic RSI w górę</b> (%K: <code>{h(fnum(last_row, 'stoch_k'))}</code>)")
                confirmations.append(f"📊 <b>Wolumen:</b> {h(volume_str)}")
                if not is_core and alt_12h is not None and btc_ret_12h is not None:
                    confirmations.append(f"📎 <b>12h vs BTC:</b> {alt_12h:.1f}% / BTC {btc_ret_12h:.1f}%")
                conf_msg = "\n" + "\n".join(confirmations)

                if rr_ratio >= 1.5:
                    entry_line = f"├ 🟢 <b>Kupuj (Market):</b> <code>{h(money(price, quote))}</code>"
                    badge = "🟢"
                else:
                    limit_price = (price + signal_low) / 2
                    lim_risk = round(((limit_price - soft_sl) / limit_price) * 100, 2)
                    lim_tp = round(((tp - limit_price) / limit_price) * 100, 2)
                    lim_rr = round(max(0.0, lim_tp - FEE_PCT) / (lim_risk + FEE_PCT), 2) if (lim_risk + FEE_PCT) > 0 else rr_ratio
                    entry_line = (
                        f"├ 🟡 <b>Ustaw Limit (50% knota):</b> <code>{h(money(limit_price, quote))}</code> "
                        f"(R:R limitu <code>1 : {lim_rr}</code>)"
                    )
                    badge = "🟡"

                if final_signal == "BUY_COUNTER":
                    header = f"⚠️ <b>KONTRTREND 4H — TYLKO PRZY TRENDZIE 1D</b> {badge}"
                elif final_signal == "BUY_BOUNCE":
                    header = f"🟡 <b>STREFA ODBICIA (4H)</b> {badge}"
                else:
                    header = f"⚡️ <b>ODBICIE 4H</b> {badge}"

                fx_line = f"\n💷 <b>Orientacyjnie:</b> <code>{h(money(price * usd_to_gbp, 'GBP'))}</code>" if gbp_note else ""
                body = (
                    f"{header}\n"
                    f"───────────────────\n"
                    f"{icon} <b>Para:</b> {link(symbol, tv_link)}\n"
                    f"💰 <b>Cena ({h(quote)}):</b> <code>{h(money(price, quote))}</code>{fx_line}\n"
                    f"📊 <b>RSI (4H):</b> <code>{h(rsi_4h)}</code>\n\n"
                    f"⚙️ <b>PLAN TRANSAKCYJNY ({h(quote)})</b>\n"
                    f"{entry_line}\n"
                    f"├ ⚠️ <b>Wyjście (zamknięcie 4H pod):</b> <code>{h(money(soft_sl, quote))}</code> (<code>-{risk_pct}%</code>)\n"
                    f"├ 🛡 <b>Awaryjny SL:</b> <code>{h(money(hard_sl, quote))}</code> (<code>-{hard_risk_pct}%</code>)\n"
                    f"├ 🎯 <b>Cel ({h(tp_name)}):</b> <code>{h(money(tp, quote))}</code> (<code>+{tp_pct}%</code>)\n"
                    f"└ ⚖️ <b>R:R netto (po {FEE_PCT:.2f}%):</b> <code>1 : {rr_ratio}</code>\n\n"
                    f"📈 <b>Potwierdzenia:</b>{conf_msg}\n\n"
                    f"👁 {link('Wykres na TradingView', tv_link)}"
                )
                pending = {
                    "body": body,
                    "symbol": symbol,
                    "status": status_txt,
                    "price": money(price, quote),
                    "rsi": rsi_4h,
                    "rr": rr_ratio,
                    "is_core": is_core,
                    "ts": ts_closed,
                    "candle_key": candle_key,
                    "signal": final_signal,
                    "entry": price,
                    "sl": soft_sl,
                    "tp": tp,
                    "quote": quote,
                }
                suppress_key = None

        return {
            "ok": True,
            "symbol": symbol,
            "digest_row": digest_row,
            "pending": pending,
            "suppress_key": suppress_key,
            "paper_touched": paper_touched,
        }
    except Exception as exc:
        logging.exception("Błąd dla %s: %s", symbol, exc)
        return fail_row(symbol, "Błąd obliczeń")


def btc_context(df_4h):
    if df_4h is None or len(df_4h) < 8:
        logging.error("Brak danych BTC. Alty NIE są blokowane dumpem.")
        return False, None
    frame = calculate_indicators(df_4h)
    change_4h = close_change(frame, older=-3, newer=-2)
    change_12h = close_change(frame, older=-5, newer=-2)
    if change_4h is None or change_12h is None:
        logging.error("BTC bez cen zamknięcia. Alty NIE są blokowane dumpem.")
        return False, None
    dumping = change_4h <= BTC_DUMP_4H or change_12h <= BTC_DUMP_12H
    if dumping:
        logging.warning("BTC DUMP 4H %.2f%% / 12H %.2f%%. Alty zablokowane.", change_4h, change_12h)
    else:
        logging.info("BTC guard off: 4H %.2f%%, 12H %.2f%%.", change_4h, change_12h)
    return dumping, change_12h


async def main():
    started = time.time()
    logging.info(
        "Skaner krypto V5.0, pary: %s, max alertów: %s, okno: %sh, kontrtrend: %s",
        len(SYMBOLS), MAX_ALERTS, FRESH_HOURS, ALLOW_COUNTERTREND,
    )

    uk_now = datetime.datetime.now(ZoneInfo("Europe/London"))
    today_str = uk_now.strftime("%Y-%m-%d")
    now_str = uk_now.strftime("%Y-%m-%d %H:%M")
    github_event = os.getenv("GITHUB_EVENT_NAME", "")

    sheet = init_google_sheet()
    cache = load_cache()
    papers = load_paper()
    exchange = ccxt.kraken({
        "enableRateLimit": True,
        "timeout": 20000,
        "options": {"defaultType": "spot"},
    })

    try:
        usd_to_gbp = await get_gbp_per_usd(exchange)
        btc_df = await fetch_ohlcv_async(exchange, "BTC/GBP", timeframe="4h", limit=OHLCV_4H_LIMIT)
        btc_dumping, btc_ret_12h = btc_context(btc_df)

        digest_rows = []
        pending = []
        failed = []
        success = 0
        paper_touched = False
        semaphore = asyncio.Semaphore(3)

        async def guarded(symbol):
            async with semaphore:
                prefetched = btc_df if symbol == "BTC/GBP" else None
                return await process_symbol(
                    exchange, symbol, usd_to_gbp, cache, papers,
                    btc_dumping=btc_dumping, btc_ret_12h=btc_ret_12h, pre_fetched_df_4h=prefetched,
                )

        async with httpx.AsyncClient() as http_client:
            results = await asyncio.gather(*(guarded(symbol) for symbol in SYMBOLS), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logging.error("Task pary padł: %s", result)
                    continue
                if result.get("ok"):
                    success += 1
                else:
                    failed.append(result.get("symbol") or "?")
                if result.get("paper_touched"):
                    paper_touched = True
                if result.get("digest_row"):
                    digest_rows.append(result["digest_row"])
                if result.get("suppress_key"):
                    cache[result["suppress_key"]] = True
                if result.get("pending"):
                    pending.append(result["pending"])

            if paper_touched:
                save_paper(papers)

            last_core = int(cache.get("LAST_CORE_TS") or 0)
            core_recent = last_core > 0 and (time.time() - last_core) < COOLDOWN_SEC
            core_now = any(item["is_core"] for item in pending)
            kept, dropped = select_alerts(pending, MAX_ALERTS, core_recent or core_now)
            for item in dropped:
                cache[f"BLOCK_{item['symbol']}_{item['ts']}"] = True
                logging.info("Pominięty alert %s (limit %s albo świeży sygnał BTC/ETH)", item["symbol"], MAX_ALERTS)

            save_cache(cache)
            elapsed = round(time.time() - started, 2)
            missed = ", ".join(failed) if failed else "brak"
            health = (
                f"Autodiagnostyka krypto V5.0: {success}/{len(SYMBOLS)} par w {elapsed}s. "
                f"Bez danych: {missed}. BTC dump={btc_dumping}. "
                f"Alerty do wysyłki: {len(kept)}."
            )
            logging.info(health)
            write_github_step_summary(digest_rows, health_msg=health)
            await process_telegram_commands_async(http_client, cache, digest_rows)
            save_cache(cache)

            if github_event == "workflow_dispatch" and digest_rows:
                header = "🎯 <b>RĘCZNY SKAN RYNKU KRYPTO</b>\n───────────────────\n\n"
                blocks = [row["digest_line"] for row in digest_rows]
                for part in split_messages(header, blocks):
                    sent = await send_telegram_alert_async(http_client, part)
                    if sent is False:
                        logging.error("Ręczny digest nie wyszedł w całości.")
                        break
                    await asyncio.sleep(0.4)

            sheet_rows = []
            sent_count = 0
            for item in kept:
                sent = await send_telegram_alert_async(http_client, item["body"])
                if sent is not True:
                    logging.error("Nie blokuję %s — Telegram nie przyjął alertu.", item["candle_key"])
                    continue
                sent_count += 1
                cache[item["candle_key"]] = True
                cache[f"COOLDOWN_{item['symbol']}"] = item["ts"] + COOLDOWN_SEC
                if item["is_core"]:
                    cache["LAST_CORE_TS"] = item["ts"]
                save_cache(cache)
                papers.append({
                    "ts": item["ts"],
                    "symbol": item["symbol"],
                    "signal": item["signal"],
                    "entry": item["entry"],
                    "sl": item["sl"],
                    "tp": item["tp"],
                    "quote": item["quote"],
                    "status": "OPEN",
                    "exit": "",
                    "bars": "",
                    "note": "",
                })
                save_paper(papers)
                sheet_rows.append([
                    now_str, item["symbol"], item["status"], item["price"], f"RSI: {item['rsi']}",
                ])
                await asyncio.sleep(0.3)

            if sheet and sheet_rows:
                try:
                    await asyncio.to_thread(sheet.append_rows, sheet_rows)
                    logging.info("Zapisano %s wierszy do arkusza Krypto.", len(sheet_rows))
                except Exception as exc:
                    logging.error("Błąd zapisu Google Sheets: %s", exc)

            heartbeat_key = f"HEARTBEAT_{today_str}"
            if sent_count == 0 and github_event != "workflow_dispatch" and not cache.get(heartbeat_key):
                note = (
                    "📋 <b>Skan krypto V5.0</b>\n"
                    "Brak nowych alertów.\n"
                    f"Pary: <code>{success}/{len(SYMBOLS)}</code>. Bez danych: {h(missed)}.\n"
                    f"BTC dump: <code>{'tak' if btc_dumping else 'nie'}</code>."
                )
                sent = await send_telegram_alert_async(http_client, note)
                if sent is True:
                    cache[heartbeat_key] = True
            save_cache(cache)
        logging.info("Koniec skanu w %ss.", round(time.time() - started, 2))
    finally:
        await exchange.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        logging.critical("KRYTYCZNY BŁĄD SKANERA KRYPTO: %s", exc, exc_info=True)
        clean = h(str(exc))[:500]
        err_msg = f"🚨 <b>KRYTYCZNY BŁĄD SKANERA KRYPTO</b>\n<code>{clean}</code>"

        async def send_critical_error():
            async with httpx.AsyncClient() as http_client:
                await send_telegram_alert_async(http_client, err_msg)

        asyncio.run(send_critical_error())
        raise
