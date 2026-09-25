# -*- coding: utf-8 -*-
import asyncio
import datetime
import json
import logging
import os
import tempfile
import time
from zoneinfo import ZoneInfo

import ccxt
import gspread
import httpx
import pandas as pd
from google.oauth2.service_account import Credentials

# --- 1. KONFIGURACJA LOGOWANIA ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 2. ZMIENNE ŚRODOWISKOWE I STAŁE ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
GOOGLE_CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS') or os.getenv('GOOGLE_TASKS_CREDENTIALS')

CORE_CRYPTO = ['BTC/GBP', 'ETH/GBP']
ALT_CRYPTO = [
    'SOL/GBP', 'TAO/USD', 'RENDER/USD', 
    'ONDO/USD', 'XRP/GBP', 'LINK/GBP', 
    'SUI/GBP', 'AAVE/GBP', 'FET/USD', 
    'NEAR/USD'
]

env_crypto = os.getenv('CRYPTO_PAIRS')
if env_crypto:
    SYMBOLS = [s.strip() for s in env_crypto.split(',') if s.strip()]
else:
    SYMBOLS = CORE_CRYPTO + ALT_CRYPTO

CACHE_FILE = 'cache_krypto.json'

exchange = ccxt.kraken({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})


# --- 3. INICJALIZACJA USŁUG I I/O ---
def init_google_sheet():
    if not GOOGLE_CREDENTIALS or not SPREADSHEET_ID:
        return None
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        sheet_client = gspread.authorize(creds)
        return sheet_client.open_by_key(SPREADSHEET_ID).sheet1
    except Exception as e:
        logging.error(f"Błąd inicjalizacji Google Sheets: {e}")
        return None

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Nie można załadować pliku cache: {e}")
    return {}

def save_cache(cache_data):
    try:
        now_ts = int(time.time())
        retention_period = 14 * 86400  # Czyszczenie alertów starszych niż 14 dni

        cleaned_cache = {}
        for k, v in cache_data.items():
            if k.startswith("ALERT_"):
                parts = k.split("_")
                if len(parts) >= 3 and parts[-2].isdigit():
                    ts = int(parts[-2])
                    if now_ts - ts > retention_period:
                        continue
            cleaned_cache[k] = v

        dir_name = os.path.dirname(os.path.abspath(CACHE_FILE)) or "."
        with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False, encoding="utf-8") as tf:
            json.dump(cleaned_cache, tf, indent=2, ensure_ascii=False)
            temp_name = tf.name
        os.replace(temp_name, CACHE_FILE)
    except Exception as e:
        logging.error(f"Nie udało się zapisać cache: {e}")

async def send_telegram_alert_async(http_client: httpx.AsyncClient, msg: str, custom_chat_id=None):
    chat_id = custom_chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown", "disable_web_page_preview": True}
    try:
        response = await http_client.post(url, json=payload, timeout=10.0)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Nie udało się wysłać wiadomości na Telegram: {e}")

def write_github_step_summary(digest_rows, health_msg=""):
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_file:
        try:
            with open(summary_file, "a", encoding="utf-8") as f:
                if health_msg:
                    f.write(f"> {health_msg}\n\n")
                f.write("### 📊 Podsumowanie Skanera Krypto (Tryb Analityczny)\n\n")
                f.write("| Moneta | Cena | RSI 4H | Formacja | Stan |\n")
                f.write("| --- | --- | --- | --- | --- |\n")
                for r in digest_rows:
                    f.write(f"| **{r['symbol']}** | {r['price']} | {r['rsi']} | {r['pinbar']} | {r['status']} |\n")
        except Exception as e:
            logging.error(f"Błąd zapisu GITHUB_STEP_SUMMARY: {e}")

async def process_telegram_commands_async(http_client: httpx.AsyncClient, cache: dict, digest_rows: list):
    if not TELEGRAM_TOKEN:
        return

    last_offset = cache.get("telegram_update_offset", 0)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_offset + 1}&timeout=2"
    try:
        resp = await http_client.get(url, timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            for update in data.get("result", []):
                cache["telegram_update_offset"] = update["update_id"]
                msg_obj = update.get("message", {})
                text = msg_obj.get("text", "").strip().lower()
                chat_id = msg_obj.get("chat", {}).get("id")

                if text.startswith(("/status", "/stan", "/start")):
                    status_msg = "📋 **AKTUALNY STAN RYNKU (KRYPTO)**\n\n"
                    for r in digest_rows:
                        sym_disp = r['symbol']
                        tv_sym = r.get('tv_symbol', sym_disp.replace("/", ""))
                        tv_link = f"[{sym_disp}](https://www.tradingview.com/chart/?symbol=KRAKEN:{tv_sym})"
                        status_msg += f"{r['icon']} {tv_link}: {r['price']}\n├ RSI (4H): {r['rsi']} | {r['pinbar']}\n└ Stan: {r['status']}\n\n"
                    target_chat = chat_id or TELEGRAM_CHAT_ID
                    if target_chat:
                        await send_telegram_alert_async(http_client, status_msg, custom_chat_id=target_chat)
    except Exception as e:
        logging.error(f"Błąd przetwarzania komend Telegram: {e}")


# --- 4. ANALIZA TECHNICZNA I DANE ---
async def fetch_ohlcv_async(symbol, timeframe='4h', limit=100):
    try:
        ohlcv = await asyncio.to_thread(exchange.fetch_ohlcv, symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df
    except Exception as e:
        logging.warning(f"Błąd pobierania {symbol}: {e}")
        return None

async def get_gbp_rate_async():
    try:
        ticker = await asyncio.to_thread(exchange.fetch_ticker, 'USD/GBP')
        return float(ticker['last'])
    except Exception:
        return 0.79

def calculate_indicators(df, min_wick_ratio=0.60, max_upper_wick_ratio=0.20):
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean().replace(0, 1e-10)
    
    rs = avg_gain / avg_loss
    df['rsi'] = round(100 - (100 / (1 + rs)), 2)
    
    df['sma20'] = df['close'].rolling(window=20).mean()
    df['std20'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['sma20'] + (df['std20'] * 2)
    df['bb_lower'] = df['sma20'] - (df['std20'] * 2)

    df['vol_sma20'] = df['volume'].rolling(window=20).mean()
    
    range_candle = df['high'] - df['low']
    body = abs(df['close'] - df['open'])
    
    lower_wick = df[['open', 'close']].min(axis=1) - df['low']
    upper_wick = df['high'] - df[['open', 'close']].max(axis=1)
    
    valid_range = range_candle > 0
    has_strong_lower_wick = (lower_wick / range_candle) >= min_wick_ratio
    has_small_upper_wick = (upper_wick / range_candle) <= max_upper_wick_ratio
    has_small_body = (body / range_candle) <= 0.35
    
    df['pinbar'] = valid_range & has_strong_lower_wick & has_small_upper_wick & has_small_body
    return df


# --- 5. GŁÓWNA PĘTLA APLIKACJI ---
async def main():
    start_time = time.time()
    logging.info("Uruchamianie Skanera Krypto (Pełny Tryb Analityczny V2)...")

    uk_now = datetime.datetime.now(ZoneInfo('Europe/London'))
    today_str = uk_now.strftime('%Y-%m-%d')
    now_str = uk_now.strftime('%Y-%m-%d %H:%M')

    sheet = init_google_sheet()
    cache = load_cache()
    usd_to_gbp = await get_gbp_rate_async()

    digest_lines = []
    digest_summary_rows = []
    pending_alerts = []
    rows_to_append_sheets = []
    success_count = 0

    async with httpx.AsyncClient() as http_client:
        for symbol in SYMBOLS:
            try:
                is_core = symbol in CORE_CRYPTO
                crypto_icon = '🏛️' if is_core else '🚀'
                
                pinbar_threshold = 0.50 if is_core else 0.60
                rsi_signal_threshold = 35.0 if is_core else 33.0
                max_pinbar_rsi = 42.0 if is_core else 38.0
                
                df = await fetch_ohlcv_async(symbol)
                if df is None or len(df) < 25:
                    continue

                df = calculate_indicators(df, min_wick_ratio=pinbar_threshold, max_upper_wick_ratio=0.20)

                # Ocena OSTATNIO ZAMKNIĘTEJ świecy 4H (iloc[-2]) oraz przedostatniej (iloc[-3])
                last_row = df.iloc[-2]
                prev_row = df.iloc[-3]

                price_native = float(last_row['close'])
                rsi_4h = float(last_row['rsi'])
                prev_rsi_4h = float(prev_row['rsi'])

                is_pinbar = bool(last_row['pinbar'])
                prev_pinbar = bool(prev_row['pinbar'])

                ts_closed = int(last_row['timestamp'] / 1000)

                vol_target = last_row['volume']
                vol_sma = last_row['vol_sma20']
                volume_str = "Standardowy"
                if pd.notna(vol_sma) and vol_sma > 0:
                    vol_ratio = int((vol_target / vol_sma) * 100)
                    volume_str = f"{vol_ratio}% średniej {'🟢 (Wysoki)' if vol_ratio >= 120 else ('🔴 (Niski)' if vol_ratio <= 80 else '🟡 (Standard)')}"

                tv_clean_symbol = symbol.replace("/", "")

                if 'USD' in symbol:
                    price_gbp = price_native * usd_to_gbp
                    symbol_display = symbol.replace('USD', 'GBP')
                else:
                    price_gbp = price_native
                    symbol_display = symbol

                price_disp = f"£{price_gbp:.2f}" if price_gbp >= 1.0 else f"£{price_gbp:.4f}"

                # --- POPRAWIONA LOGIKA WARUNKÓW ---
                bb_touched = last_row['low'] <= last_row['bb_lower']
                max_rsi_for_bb = 45.0  # Filtr zapobiegający alertom przy wysokim RSI mimo knotu do BB

                current_signal_type = 'NONE'
                if (rsi_4h <= rsi_signal_threshold) or (bb_touched and rsi_4h <= max_rsi_for_bb):
                    current_signal_type = 'BUY_REBOUND'
                elif is_pinbar and rsi_4h <= max_pinbar_rsi:
                    current_signal_type = 'BUY_PINBAR'

                status_map = {
                    'NONE': '⚪ Neutralny',
                    'BUY_REBOUND': '⚡️ SZYBKIE ODBICIE',
                    'BUY_PINBAR': '🕯️ PINBAR ODBICIE'
                }
                status_txt = status_map.get(current_signal_type, '⚪ Neutralny')
                pinbar_txt = f"Pinbar {int(pinbar_threshold * 100)}%" if is_pinbar else "Brak"

                tv_link_symbol = f"[{symbol_display}](https://www.tradingview.com/chart/?symbol=KRAKEN:{tv_clean_symbol})"

                digest_lines.append(
                    f"{crypto_icon} {tv_link_symbol}: `{price_disp}`\n"
                    f"├ RSI (4H): `{rsi_4h}` | {pinbar_txt}\n"
                    f"└ Stan: {status_txt}\n\n"
                )
                digest_summary_rows.append({
                    'symbol': symbol_display, 'icon': crypto_icon,
                    'price': price_disp, 'rsi': rsi_4h, 'pinbar': pinbar_txt,
                    'status': status_txt, 'tv_symbol': tv_clean_symbol
                })

                # --- POPRAWIONA LOGIKA BEZPIECZEŃSTWA ALERTÓW ---
                if current_signal_type != 'NONE':
                    candle_key = f"ALERT_{symbol_display}_{ts_closed}_{current_signal_type}"
                    already_alerted = (cache.get(candle_key) is not None)

                    # Sprawdzenie crossovera (czy na poprzedniej świecy sygnał NIE występował)
                    is_new_signal = False
                    if current_signal_type == 'BUY_REBOUND':
                        prev_bb_touched = prev_row['low'] <= prev_row['bb_lower']
                        prev_was_rebound = (prev_rsi_4h <= rsi_signal_threshold) or (prev_bb_touched and prev_rsi_4h <= max_rsi_for_bb)
                        is_new_signal = not prev_was_rebound
                    elif current_signal_type == 'BUY_PINBAR':
                        prev_was_pinbar = prev_pinbar and (prev_rsi_4h <= max_pinbar_rsi)
                        is_new_signal = not prev_was_pinbar

                    should_alert = not already_alerted and is_new_signal

                    if should_alert:
                        tv_link = f"https://www.tradingview.com/chart/?symbol=KRAKEN:{tv_clean_symbol}"

                        confirmations = []
                        if is_pinbar:
                            confirmations.append(f"🕯️ **Knot popytowy na świecy 4H (Pinbar >= {int(pinbar_threshold * 100)}%)**")
                        if bb_touched:
                            confirmations.append("📉 **Knot świecy przetestował dolną Wstęgę Bollingera (20, 2)**")
                        confirmations.append(f"📊 **Wolumen:** {volume_str}")

                        conf_msg = "\n" + "\n".join(confirmations)

                        body = (
                            f"⚡️ **SZYBKA OKAZJA NA ODBICIE (SWING)**\n\n"
                            f"{crypto_icon} **Moneta:** [{symbol_display}]({tv_link})\n"
                            f"💰 **Cena:** `{price_disp}`\n"
                            f"📊 **RSI (4H):** `{rsi_4h}`{conf_msg}\n\n"
                            f"👁 **Oceń wykres i dołek knota:**\n"
                            f"🔗 📈 [Zobacz wykres na TradingView]({tv_link})"
                        )

                        pending_alerts.append((body, symbol_display, status_txt, price_disp, rsi_4h))
                        cache[candle_key] = True

                success_count += 1

            except Exception as e:
                logging.error(f"Błąd dla {symbol}: {e}")

        elapsed = round(time.time() - start_time, 2)
        health_summary = f"🩺 **Autodiagnostyka Krypto:** Przeanalizowano `{success_count}/{len(SYMBOLS)}` monet w `{elapsed}s`."
        logging.info(health_summary.replace('**', '').replace('`', ''))

        write_github_step_summary(digest_summary_rows, health_msg=health_summary)
        await process_telegram_commands_async(http_client, cache, digest_summary_rows)

        # Sekwencyjne wysyłanie alertów z pauzą 0.3s (ochrona przed limitami Telegrama HTTP 429)
        if pending_alerts:
            for msg, sym_disp, status_txt, price_disp, rsi_val in pending_alerts:
                await send_telegram_alert_async(http_client, msg)
                await asyncio.sleep(0.3)
                rows_to_append_sheets.append([
                    now_str,
                    sym_disp,
                    status_txt,
                    price_disp,
                    f"RSI: {rsi_val}"
                ])

        if sheet and rows_to_append_sheets:
            try:
                await asyncio.to_thread(sheet.append_rows, rows_to_append_sheets)
                logging.info(f"Zapisano {len(rows_to_append_sheets)} wierszy krypto w Google Sheets.")
            except Exception as e:
                logging.error(f"Nie udało się zapisać wierszy w Google Sheets: {e}")

        # Raport wieczorny po 21:00
        if uk_now.hour >= 21 and cache.get('DIGEST_DATE') != today_str:
            if digest_lines:
                digest_msg = '📋 **CODZIENNE PODSUMOWANIE RYNKU (KRYPTO - 21:00)**\n\n' + ''.join(digest_lines)
                await send_telegram_alert_async(http_client, digest_msg)
                cache['DIGEST_DATE'] = today_str

        save_cache(cache)

    logging.info(f"Skaner krypto zakończył działanie w czasie: {elapsed}s.")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical(f"🚨 KRYTYCZNY BŁĄD SKANERA KRYPTO: {e}", exc_info=True)
        err_msg = f'🚨 **🔴 KRYTYCZNY BŁĄD SKANERA KRYPTO:**\n`{str(e)}`'
        
        async def send_critical_error():
            async with httpx.AsyncClient() as http_client:
                await send_telegram_alert_async(http_client, err_msg)
        
        asyncio.run(send_critical_error())
        raise e
