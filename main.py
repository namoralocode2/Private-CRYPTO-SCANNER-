# -*- coding: utf-8 -*-
import asyncio
import datetime
import json
import logging
import os
import re
import tempfile
import time
from zoneinfo import ZoneInfo

import ccxt.async_support as ccxt
import gspread
import httpx
import pandas as pd
import numpy as np
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

# Sztywny podział na 3 paczki po dokładnie 6 aktywów (3 x 6 = 18) z nowymi altami (AVAX, APT, INJ, TIA)
CRYPTO_P1 = ['BTC/GBP', 'ETH/GBP', 'SOL/GBP', 'TAO/USD', 'RENDER/USD', 'ONDO/USD']
CRYPTO_P2 = ['XRP/GBP', 'LINK/GBP', 'SUI/GBP', 'AAVE/GBP', 'FET/USD', 'NEAR/USD']
CRYPTO_P3 = ['HNT/USD', 'ARB/USD', 'AVAX/USD', 'APT/USD', 'INJ/USD', 'TIA/USD']

SYMBOLS = CRYPTO_P1 + CRYPTO_P2 + CRYPTO_P3

CACHE_FILE = 'cache_krypto.json'


# --- 3. INICJALIZACJA USŁUG I I/O ---
def init_google_sheet():
    if not GOOGLE_CREDENTIALS or not SPREADSHEET_ID:
        logging.error("Brak GOOGLE_CREDENTIALS lub SPREADSHEET_ID w środowisku!")
        return None
    try:
        raw_id = SPREADSHEET_ID
        if "/d/" in raw_id:
            raw_id = raw_id.split("/d/")[1].split("/")[0]
            
        clean_sheet_id = re.sub(r'[^a-zA-Z0-9-_]', '', raw_id)
        
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        sheet_client = gspread.authorize(creds)
        sheet = sheet_client.open_by_key(clean_sheet_id).worksheet("Krypto")
        return sheet
    except Exception as e:
        logging.error(f"Błąd inicjalizacji Google Sheets: {e}")
        return None

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_cache(cache_data):
    try:
        now_ts = int(time.time())
        retention_period = 14 * 86400
        cleaned_cache = {}
        for k, v in cache_data.items():
            if k.startswith("ALERT_"):
                parts = k.split("_")
                ts = next((int(p) for p in parts if p.isdigit() and len(p) >= 10), None)
                if ts and (now_ts - ts > retention_period):
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
        logging.error(f"Błąd wysyłki Telegram: {e}")

def write_github_step_summary(digest_rows, health_msg=""):
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_file:
        try:
            with open(summary_file, "a", encoding="utf-8") as f:
                if health_msg:
                    f.write(f"> {health_msg}\n\n")
                f.write("### 📊 Podsumowanie Skanera Krypto (Tryb Pancerny)\n\n")
                f.write("| Moneta | Cena | RSI 4H | Formacja | Stan |\n")
                f.write("| --- | --- | --- | --- | --- |\n")
                for r in digest_rows:
                    f.write(f"| **{r['symbol']}** | {r['price']} | {r['rsi']} | {r['pinbar']} | {r['status']} |\n")
        except Exception:
            pass

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
                msg_obj = update.get("message") or update.get("edited_message") or {}
                text = str(msg_obj.get("text", "")).strip().lower()
                chat_id = msg_obj.get("chat", {}).get("id")

                if text.startswith(("/status", "/stan", "/start")):
                    p1_rows = [r for r in digest_rows if r['raw_symbol'] in CRYPTO_P1]
                    p2_rows = [r for r in digest_rows if r['raw_symbol'] in CRYPTO_P2]
                    p3_rows = [r for r in digest_rows if r['raw_symbol'] in CRYPTO_P3]

                    target_chat = chat_id or TELEGRAM_CHAT_ID

                    if p1_rows:
                        status_msg_p1 = "🚀 **CRYPTO MARKET STATUS (1/3)**\n───────────────────\n\n"
                        for r in p1_rows:
                            tv_link = f"[{r['symbol']}](https://www.tradingview.com/chart/?symbol=KRAKEN:{r['tv_symbol']})"
                            status_msg_p1 += (
                                f"🚀 {tv_link}: `{r['price']}`\n"
                                f"├ RSI (4H): `{r['rsi']}` | {r.get('sma200', 'No SMA 200')}\n"
                                f"├ Pinbar: {r['pinbar']}\n"
                                f"└ Status: {r['status']}\n\n"
                            )
                        await send_telegram_alert_async(http_client, status_msg_p1.strip(), custom_chat_id=target_chat)
                        await asyncio.sleep(0.5)

                    if p2_rows:
                        status_msg_p2 = "🚀 **CRYPTO MARKET STATUS (2/3)**\n───────────────────\n\n"
                        for r in p2_rows:
                            tv_link = f"[{r['symbol']}](https://www.tradingview.com/chart/?symbol=KRAKEN:{r['tv_symbol']})"
                            status_msg_p2 += (
                                f"🚀 {tv_link}: `{r['price']}`\n"
                                f"├ RSI (4H): `{r['rsi']}` | {r.get('sma200', 'No SMA 200')}\n"
                                f"├ Pinbar: {r['pinbar']}\n"
                                f"└ Status: {r['status']}\n\n"
                            )
                        await send_telegram_alert_async(http_client, status_msg_p2.strip(), custom_chat_id=target_chat)
                        await asyncio.sleep(0.5)

                    if p3_rows:
                        status_msg_p3 = "🚀 **CRYPTO MARKET STATUS (3/3)**\n───────────────────\n\n"
                        for r in p3_rows:
                            tv_link = f"[{r['symbol']}](https://www.tradingview.com/chart/?symbol=KRAKEN:{r['tv_symbol']})"
                            status_msg_p3 += (
                                f"🚀 {tv_link}: `{r['price']}`\n"
                                f"├ RSI (4H): `{r['rsi']}` | {r.get('sma200', 'No SMA 200')}\n"
                                f"├ Pinbar: {r['pinbar']}\n"
                                f"└ Status: {r['status']}\n\n"
                            )
                        await send_telegram_alert_async(http_client, status_msg_p3.strip(), custom_chat_id=target_chat)
    except Exception as e:
        logging.error(f"Błąd Telegram Commands: {e}")


# --- 4. ANALIZA TECHNICZNA I DANE ---
async def fetch_ohlcv_async(exchange_inst, symbol, timeframe='4h', limit=250):
    try:
        ohlcv = await exchange_inst.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df
    except Exception as e:
        logging.warning(f"Błąd pobierania {symbol} ({timeframe}): {e}")
        return None

async def get_gbp_rate_async(exchange_inst):
    try:
        ticker = await exchange_inst.fetch_ticker('GBP/USD')
        gbp_usd = float(ticker['last'])
        if gbp_usd > 0:
            return 1.0 / gbp_usd
    except Exception:
        pass
    return 0.79

def calculate_indicators(df, min_wick_ratio=0.60, max_upper_wick_ratio=0.20):
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean().replace(0, 1e-10)
    
    rs = avg_gain / avg_loss
    df['rsi'] = round(100 - (100 / (1 + rs)), 2)
    
    stoch_period = 14
    rsi_min = df['rsi'].rolling(window=stoch_period).min()
    rsi_max = df['rsi'].rolling(window=stoch_period).max()
    stoch_rsi = (df['rsi'] - rsi_min) / (rsi_max - rsi_min + 1e-10) * 100
    df['stoch_k'] = round(stoch_rsi.rolling(window=3).mean(), 2)
    df['stoch_d'] = round(df['stoch_k'].rolling(window=3).mean(), 2)
    
    df['sma20'] = df['close'].rolling(window=20).mean()
    df['sma50'] = df['close'].rolling(window=50).mean()
    df['sma200'] = df['close'].rolling(window=200).mean()
    df['std20'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['sma20'] + (df['std20'] * 2)
    df['bb_lower'] = df['sma20'] - (df['std20'] * 2)

    df['vol_sma20'] = df['volume'].rolling(window=20).mean()
    
    tr1 = df['high'] - df['low']
    tr2 = abs(df['high'] - df['close'].shift(1))
    tr3 = abs(df['low'] - df['close'].shift(1))
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = df['tr'].rolling(window=14).mean()

    range_candle = df['high'] - df['low']
    body = abs(df['close'] - df['open'])
    
    lower_wick = df[['open', 'close']].min(axis=1) - df['low']
    upper_wick = df['high'] - df[['open', 'close']].max(axis=1)
    
    valid_range = range_candle > 0
    safe_range = range_candle.replace(0, np.nan)
    
    has_strong_lower_wick = (lower_wick / safe_range) >= min_wick_ratio
    has_small_upper_wick = (upper_wick / safe_range) <= max_upper_wick_ratio
    has_small_body = (body / safe_range) <= 0.35
    
    df['pinbar'] = valid_range & has_strong_lower_wick.fillna(False) & has_small_upper_wick.fillna(False) & has_small_body.fillna(False)
    return df


def eval_signal(row, rsi_signal_threshold, max_pinbar_rsi, is_above_sma200, vol_ratio):
    """RYGORSTYCZNA weryfikacja sygnału z filtrem świec zielonych."""
    if vol_ratio < 110:
        return 'NONE'

    rsi_val = float(row['rsi'])
    is_pinbar = bool(row['pinbar'])
    bb_touched = float(row['low']) <= float(row['bb_lower'])
    
    is_green = float(row['close']) > float(row['open'])
    
    if not is_above_sma200:
        if is_pinbar and rsi_val <= 30.0 and is_green:
            return 'BUY_PINBAR'
        if rsi_val <= 25.0 and is_green and vol_ratio >= 150:
            return 'BUY_REBOUND'
        return 'NONE'

    if is_pinbar and rsi_val <= max_pinbar_rsi:
        return 'BUY_PINBAR'
    
    if rsi_val <= rsi_signal_threshold and (is_green or vol_ratio >= 150):
        return 'BUY_REBOUND'
        
    if bb_touched and rsi_val <= 35.0 and is_green:
        return 'BUY_BOUNCE'
        
    return 'NONE'


async def process_symbol(exchange_inst, symbol: str, usd_to_gbp: float, cache: dict, btc_dumping: bool = False, pre_fetched_df_4h=None):
    try:
        is_core = symbol in ['BTC/GBP', 'ETH/GBP']
        crypto_icon = '🚀'
        
        pinbar_threshold = 0.50 if is_core else 0.60
        rsi_signal_threshold = 35.0 if is_core else 33.0
        max_pinbar_rsi = 40.0 if is_core else 36.0
        
        if pre_fetched_df_4h is not None:
            df = pre_fetched_df_4h
            df_1d = await fetch_ohlcv_async(exchange_inst, symbol, timeframe='1d', limit=30)
        else:
            t_4h = fetch_ohlcv_async(exchange_inst, symbol, timeframe='4h')
            t_1d = fetch_ohlcv_async(exchange_inst, symbol, timeframe='1d', limit=30)
            df, df_1d = await asyncio.gather(t_4h, t_1d)

        if df is None or len(df) < 50:
            return None

        df = calculate_indicators(df, min_wick_ratio=pinbar_threshold, max_upper_wick_ratio=0.20)

        last_row = df.iloc[-2]
        prev_row = df.iloc[-3]

        price_native = float(last_row['close'])
        signal_low_native = float(last_row['low'])
        sma20_native = float(last_row['sma20'])
        bb_upper_native = float(last_row['bb_upper'])
        atr_native = float(last_row['atr']) if pd.notna(last_row['atr']) else (signal_low_native * 0.01)
        
        rsi_4h = float(last_row['rsi'])

        sma200_val = last_row['sma200']
        sma50_val = last_row['sma50']
        
        if pd.notna(sma200_val):
            is_above_sma200 = (price_native >= float(sma200_val))
            sma200_txt = "Above 200 SMA" if is_above_sma200 else "Below 200 SMA"
        elif pd.notna(sma50_val):
            is_above_sma200 = (price_native >= float(sma50_val))
            sma200_txt = "Above 50 SMA (New)" if is_above_sma200 else "Below 50 SMA (New)"
        else:
            is_above_sma200 = True
            sma200_txt = "No SMA (New)"

        stoch_k = float(last_row['stoch_k'])
        prev_stoch_k = float(prev_row['stoch_k'])
        stoch_turning_up = (prev_stoch_k <= 25.0 or stoch_k <= 25.0) and (stoch_k > prev_stoch_k)

        is_pinbar = bool(last_row['pinbar'])
        ts_closed = int(last_row['timestamp'] / 1000)

        def get_vol_ratio(r):
            v_t = float(r['volume'])
            v_s = float(r['vol_sma20']) if pd.notna(r['vol_sma20']) else 0.0
            return int((v_t / v_s) * 100) if v_s > 0 else 100

        vol_ratio = get_vol_ratio(last_row)
        prev_vol_ratio = get_vol_ratio(prev_row)
        
        volume_str = f"{vol_ratio}% of avg {'🟢 (High)' if vol_ratio >= 120 else ('🔴 (Low)' if vol_ratio <= 80 else '🟡 (Standard)')}"

        is_1d_safe = True
        if df_1d is not None and len(df_1d) >= 14:
            df_1d = calculate_indicators(df_1d)
            last_1d = df_1d.iloc[-2]
            rsi_1d = float(last_1d['rsi'])
            if pd.notna(last_1d['sma50']):
                if rsi_1d < 38.0 and float(last_1d['close']) < float(last_1d['sma50']):
                    is_1d_safe = False

        tv_clean_symbol = symbol.replace("/", "")
        fx_ratio = usd_to_gbp if 'USD' in symbol else 1.0
        symbol_display = symbol.replace('USD', 'GBP') if 'USD' in symbol else symbol

        price_gbp = price_native * fx_ratio
        low_gbp = signal_low_native * fx_ratio
        sma20_gbp = sma20_native * fx_ratio
        bb_upper_gbp = bb_upper_native * fx_ratio
        atr_gbp = atr_native * fx_ratio

        price_disp = f"£{price_gbp:,.2f}" if price_gbp >= 1.0 else f"£{price_gbp:,.4f}"

        soft_sl_gbp = max(0.000001, low_gbp - (1.5 * atr_gbp))
        hard_sl_gbp = max(0.000001, min(soft_sl_gbp * 0.98, low_gbp * 0.95))
        tp_gbp = sma20_gbp if price_gbp < sma20_gbp else bb_upper_gbp

        soft_sl_disp = f"£{soft_sl_gbp:,.2f}" if soft_sl_gbp >= 1.0 else f"£{soft_sl_gbp:,.4f}"
        hard_sl_disp = f"£{hard_sl_gbp:,.2f}" if hard_sl_gbp >= 1.0 else f"£{hard_sl_gbp:,.4f}"
        tp_disp = f"£{tp_gbp:,.2f}" if tp_gbp >= 1.0 else f"£{tp_gbp:,.4f}"

        risk_pct = round(((price_gbp - soft_sl_gbp) / price_gbp) * 100, 2)
        hard_risk_pct = round(((price_gbp - hard_sl_gbp) / price_gbp) * 100, 2)
        tp_pct = round(((tp_gbp - price_gbp) / price_gbp) * 100, 2)

        net_tp_pct = max(0.0, tp_pct - 0.60)
        net_risk_pct = risk_pct + 0.60
        rr_ratio = round(net_tp_pct / net_risk_pct, 2) if net_risk_pct > 0 else 0.0

        bb_touched = float(last_row['low']) <= float(last_row['bb_lower'])

        raw_signal = eval_signal(last_row, rsi_signal_threshold, max_pinbar_rsi, is_above_sma200, vol_ratio)

        blocked_by_guard = False
        if raw_signal != 'NONE':
            if (btc_dumping and not is_core) or not is_1d_safe:
                blocked_by_guard = True
                current_signal_type = 'NONE'
            else:
                current_signal_type = raw_signal
        else:
            current_signal_type = 'NONE'

        status_map = {
            'NONE': '🛡 Blocked (Downside Risk)' if blocked_by_guard else '⚪ Neutral',
            'BUY_REBOUND': '⚡️ QUICK REBOUND (Demand)',
            'BUY_PINBAR': '🕯️ PINBAR REBOUND',
            'BUY_BOUNCE': '🟡 BOUNCE ZONE'
        }
        status_txt = status_map.get(current_signal_type, '⚪ Neutral')
        pinbar_txt = f"{int(pinbar_threshold * 100)}%" if is_pinbar else "None"

        tv_link_symbol = f"[{symbol_display}](https://www.tradingview.com/chart/?symbol=KRAKEN:{tv_clean_symbol})"

        digest_line = (
            f"{crypto_icon} {tv_link_symbol}: `{price_disp}`\n"
            f"├ RSI (4H): `{rsi_4h}` | {sma200_txt}\n"
            f"├ Pinbar: {pinbar_txt}\n"
            f"└ Status: {status_txt}\n\n"
        )
        digest_summary_row = {
            'raw_symbol': symbol,
            'symbol': symbol_display, 'icon': crypto_icon,
            'price': price_disp, 'rsi': rsi_4h, 'sma200': sma200_txt, 'pinbar': pinbar_txt,
            'status': status_txt, 'tv_symbol': tv_clean_symbol
        }

        pending_alert = None
        candle_key = None

        if current_signal_type != 'NONE':
            candle_key = f"ALERT_{symbol_display}_{ts_closed}_{current_signal_type}"
            already_alerted = (cache.get(candle_key) is not None)

            prev_sma200_val = prev_row['sma200']
            prev_is_above_sma200 = pd.notna(prev_sma200_val) and (float(prev_row['close']) >= float(prev_sma200_val)) if pd.notna(prev_sma200_val) else is_above_sma200
            
            prev_raw_signal = eval_signal(prev_row, rsi_signal_threshold, max_pinbar_rsi, prev_is_above_sma200, prev_vol_ratio)
            is_new_signal = (prev_raw_signal != current_signal_type)

            should_alert = not already_alerted and is_new_signal and (rr_ratio >= 1.2)

            if should_alert:
                tv_link = f"https://www.tradingview.com/chart/?symbol=KRAKEN:{tv_clean_symbol}"
                confirmations = []
                confirmations.append(f"📈 **Main Trend:** {sma200_txt}")
                confirmations.append("🛡 **Macro Trend (1D):** Safe (No Dump)")
                if is_pinbar:
                    confirmations.append(f"🕯 **Demand Wick 4H (Pinbar >= {int(pinbar_threshold * 100)}%)**")
                if float(last_row['close']) > float(last_row['open']):
                    confirmations.append("🟩 **Bullish Close:** Candle closed green")
                if bb_touched:
                    confirmations.append("📉 **Wick tested Lower Bollinger Band (20, 2)**")
                if stoch_turning_up:
                    confirmations.append(f"🔄 **Stochastic RSI curving up (%K: `{stoch_k}`)**")
                confirmations.append(f"📊 **Volume:** {volume_str}")

                conf_msg = "\n" + "\n".join(confirmations)
                tp_target_name = "Upper BB 20" if price_gbp >= sma20_gbp else "Middle BB 20"

                if rr_ratio >= 1.5:
                    status_emoji = "🟢"
                    entry_line = f"├ 🟢 **Buy (Market):** `{price_disp}`"
                else:
                    status_emoji = "🟡"
                    limit_price_gbp = (price_gbp + low_gbp) / 2
                    limit_price_disp = f"£{limit_price_gbp:,.2f}" if limit_price_gbp >= 1.0 else f"£{limit_price_gbp:,.4f}"
                    
                    lim_risk_pct = round(((limit_price_gbp - soft_sl_gbp) / limit_price_gbp) * 100, 2)
                    lim_tp_pct = round(((tp_gbp - limit_price_gbp) / limit_price_gbp) * 100, 2)
                    lim_net_tp = max(0.0, lim_tp_pct - 0.60)
                    lim_net_risk = lim_risk_pct + 0.60
                    lim_rr = round(lim_net_tp / lim_net_risk, 2) if lim_net_risk > 0 else rr_ratio
                    
                    entry_line = f"├ 🟡 **Set Limit (50% Wick):** `{limit_price_disp}` (Better R:R -> `1 : {lim_rr}`)"

                alert_header = f"🟡 **BOUNCE ZONE (4H)** {status_emoji}" if current_signal_type == 'BUY_BOUNCE' else f"⚡️ **QUICK REBOUND OPPORTUNITY (SWING 4H)** {status_emoji}"

                body = (
                    f"{alert_header}\n"
                    f"───────────────────\n"
                    f"{crypto_icon} **Asset:** [{symbol_display}]({tv_link})\n"
                    f"💰 **Market Price:** `{price_disp}`\n"
                    f"📊 **RSI (4H):** `{rsi_4h}`\n\n"
                    f"⚙️ **TRADE PLAN**\n"
                    f"{entry_line}\n"
                    f"├ ⚠️ **Exit (4H Close Below):** `{soft_sl_disp}` (`-{risk_pct}%`)\n"
                    f"├ 🛡 **Emergency SL (Exchange Crash):** `{hard_sl_disp}` (`-{hard_risk_pct}%`)\n"
                    f"├ 🎯 **Take Profit ({tp_target_name}):** Dynamic `{tp_disp}` (`+{tp_pct}%`)\n"
                    f"├ ⚖️ **R:R Ratio (Net):** `1 : {rr_ratio}`\n"
                    f"└ 👉 **ACTION:** Exit at PHYSICAL TOUCH of {tp_target_name} on 4H (levels follow price)\n\n"
                    f"📈 **Market Confirmations:**{conf_msg}\n\n"
                    f"👁 [View Chart on TradingView]({tv_link})"
                )
                pending_alert = (body, symbol_display, status_txt, price_disp, rsi_4h)

        return {
            'digest_line': digest_line,
            'digest_summary_row': digest_summary_row,
            'pending_alert': pending_alert,
            'candle_key': candle_key
        }

    except Exception as e:
        logging.error(f"Błąd dla {symbol}: {e}")
        return None


# --- 5. GŁÓWNA PĘTLA APLIKACJI ---
async def main():
    start_time = time.time()
    logging.info("Uruchamianie Skanera Krypto V3.0 (Tryb Pancerny Ochrony Kapitału)...")

    uk_now = datetime.datetime.now(ZoneInfo('Europe/London'))
    today_str = uk_now.strftime('%Y-%m-%d')
    now_str = uk_now.strftime('%Y-%m-%d %H:%M')

    sheet = init_google_sheet()
    cache = load_cache()

    exchange = ccxt.kraken({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })

    try:
        usd_to_gbp = await get_gbp_rate_async(exchange)

        btc_dumping = False
        btc_prefetched_df = None
        try:
            btc_prefetched_df = await fetch_ohlcv_async(exchange, 'BTC/GBP')
            if btc_prefetched_df is not None and len(btc_prefetched_df) >= 50:
                btc_calc = calculate_indicators(btc_prefetched_df.copy())
                btc_last = btc_calc.iloc[-2]
                btc_prev = btc_calc.iloc[-3]
                
                btc_change = ((float(btc_last['close']) - float(btc_prev['close'])) / float(btc_prev['close'])) * 100
                btc_rsi = float(btc_last['rsi'])
                
                if btc_change <= -1.5 or btc_rsi < 36.0:
                    btc_dumping = True
                    logging.warning(f"⚠️ BTC DUMP DETECTED (Zmiana: {btc_change:.2f}%, RSI: {btc_rsi}). Wstrzymano sygnały dla Altcoinów.")
        except Exception as e:
            logging.error(f"Błąd sprawdzania statusu BTC: {e}")

        digest_lines_p1 = []
        digest_lines_p2 = []
        digest_lines_p3 = []
        digest_summary_rows = []
        pending_alerts = []
        rows_to_append_sheets = []
        success_count = 0

        semaphore = asyncio.Semaphore(3)

        async def sem_process(symbol):
            async with semaphore:
                pre_df = btc_prefetched_df if symbol == 'BTC/GBP' else None
                return await process_symbol(exchange, symbol, usd_to_gbp, cache, btc_dumping=btc_dumping, pre_fetched_df_4h=pre_df)

        async with httpx.AsyncClient() as http_client:
            tasks = [sem_process(symbol) for symbol in SYMBOLS]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, Exception) or res is None:
                    continue

                success_count += 1
                raw_sym = res['digest_summary_row']['raw_symbol']
                if raw_sym in CRYPTO_P1:
                    digest_lines_p1.append(res['digest_line'])
                elif raw_sym in CRYPTO_P2:
                    digest_lines_p2.append(res['digest_line'])
                else:
                    digest_lines_p3.append(res['digest_line'])

                digest_summary_rows.append(res['digest_summary_row'])

                if res['pending_alert'] and res['candle_key']:
                    pending_alerts.append(res['pending_alert'])
                    cache[res['candle_key']] = True

            elapsed = round(time.time() - start_time, 2)
            health_summary = f"🩺 **Autodiagnostyka Krypto:** Przeanalizowano `{success_count}/{len(SYMBOLS)}` monet w `{elapsed}s`."
            logging.info(health_summary.replace('**', '').replace('`', ''))

            write_github_step_summary(digest_summary_rows, health_msg=health_summary)

            await process_telegram_commands_async(http_client, cache, digest_summary_rows)

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
                    logging.error(f"Błąd zapisu w Google Sheets: {e}")

            # Raport daily o 08:00 rano (Pancerna blokada przed duplikacją)
            if uk_now.hour == 8 and cache.get('DIGEST_DATE') != today_str:
                cache['DIGEST_DATE'] = today_str
                save_cache(cache)

                if digest_lines_p1:
                    msg_p1 = "🌅 **MORNING CRYPTO MARKET DIGEST (1/3)**\n───────────────────\n\n" + ''.join(digest_lines_p1).strip()
                    await send_telegram_alert_async(http_client, msg_p1)
                    await asyncio.sleep(0.5)

                if digest_lines_p2:
                    msg_p2 = "🌅 **MORNING CRYPTO MARKET DIGEST (2/3)**\n───────────────────\n\n" + ''.join(digest_lines_p2).strip()
                    await send_telegram_alert_async(http_client, msg_p2)
                    await asyncio.sleep(0.5)

                if digest_lines_p3:
                    msg_p3 = "🌅 **MORNING CRYPTO MARKET DIGEST (3/3)**\n───────────────────\n\n" + ''.join(digest_lines_p3).strip()
                    await send_telegram_alert_async(http_client, msg_p3)

            save_cache(cache)

        logging.info(f"Skaner krypto zakończył działanie w czasie: {elapsed}s.")

    finally:
        await exchange.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        clean_err = str(e).replace('_', '\\_').replace('*', '\\*')
        logging.critical(f"🚨 KRYTYCZNY BŁĄD SKANERA KRYPTO: {e}", exc_info=True)
        err_msg = f'🚨 **🔴 CRITICAL CRYPTO SCANNER ERROR:**\n`{clean_err}`'
        async def send_critical_error():
            async with httpx.AsyncClient() as http_client:
                await send_telegram_alert_async(http_client, err_msg)
        asyncio.run(send_critical_error())
        raise e
