# -*- coding: utf-8 -*-
"""
Automated Crypto Market Scanner (Commercial Edition V3.0 - Morning Digest & Active BTC Guard)
--------------------------------------------------------------------------------
Scans 24 top crypto assets on Kraken in USD ($), analyzing 4H RSI (14), 
1D RSI, 1D 200 SMA trend alignment, and 4H Demand Pinbars. 
Includes Active BTC Guard to block altcoin trades during market crashes.
Sends a single morning digest daily at 08:00 UK time.
"""

import os
import sys
import asyncio
import aiohttp
import logging
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import json
import pytz

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Secrets & Environment Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
USER_TZ = os.getenv('TIMEZONE', 'Europe/London')
EXCHANGE_NAME = os.getenv('EXCHANGE', 'kraken').lower()

# Active Kraken USD Pairs (Guaranteed 4 packs x 6 pairs = 24 total)
CRYPTO_P1 = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'AVAX/USD', 'DOT/USD']
CRYPTO_P2 = ['LINK/USD', 'ADA/USD', 'SUI/USD', 'NEAR/USD', 'APT/USD', 'TIA/USD']
CRYPTO_P3 = ['RENDER/USD', 'FET/USD', 'UNI/USD', 'OP/USD', 'ARB/USD', 'ATOM/USD']
CRYPTO_P4 = ['SEI/USD', 'INJ/USD', 'LTC/USD', 'BCH/USD', 'FIL/USD', 'XLM/USD']

PAIRS = CRYPTO_P1 + CRYPTO_P2 + CRYPTO_P3 + CRYPTO_P4

CACHE_FILE = 'crypto_cache.json'

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache):
    try:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        retention_period = 14 * 86400  # 14 days

        cleaned_cache = {}
        for k, v in cache.items():
            if k.startswith("ALERT_"):
                parts = k.split("_")
                ts = next((int(p) for p in parts if p.isdigit() and len(p) >= 10), None)
                if ts:
                    ts_sec = ts // 1000 if ts > 10000000000 else ts
                    if (now_ts - ts_sec) > retention_period:
                        continue
            cleaned_cache[k] = v

        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cleaned_cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Error saving cache: {e}")

async def send_telegram_alert_async(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Missing Telegram bot token or Chat ID.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with session.post(url, json=payload, timeout=10.0) as response:
            if response.status != 200:
                logging.error(f"Telegram API Error: status {response.status}")
    except Exception as e:
        logging.error(f"Error sending message to Telegram: {e}")

    await asyncio.sleep(0.1)

async def send_chunked_digest(session, title, digest_results):
    if not digest_results:
        return
    
    res_dict = {sym: item for item, sym in digest_results if item is not None}
    
    packs = [
        (CRYPTO_P1, "1/4"),
        (CRYPTO_P2, "2/4"),
        (CRYPTO_P3, "3/4"),
        (CRYPTO_P4, "4/4")
    ]
    
    for pack_symbols, pack_label in packs:
        lines = []
        for sym in pack_symbols:
            if sym in res_dict:
                lines.append(res_dict[sym])
            else:
                logging.warning(f"Missing data for symbol {sym} in Digest.")
        
        if len(lines) > 0:
            msg_text = f"{title} ({pack_label})\n───────────────────\n\n" + "\n\n".join(lines)
            await send_telegram_alert_async(session, msg_text.strip())
            await asyncio.sleep(0.5)

def calculate_indicators(df_4h, min_wick_ratio=0.55):
    delta = df_4h['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean().replace(0, 1e-10)
    
    rs = avg_gain / avg_loss
    df_4h['rsi'] = round(100 - (100 / (1 + rs)), 2)
    
    stoch_period = 14
    rsi_min = df_4h['rsi'].rolling(window=stoch_period).min()
    rsi_max = df_4h['rsi'].rolling(window=stoch_period).max()
    stoch_rsi = (df_4h['rsi'] - rsi_min) / (rsi_max - rsi_min + 1e-10) * 100
    df_4h['stoch_k'] = round(stoch_rsi.rolling(window=3).mean(), 2)
    df_4h['stoch_d'] = round(df_4h['stoch_k'].rolling(window=3).mean(), 2)
    
    df_4h['sma20'] = df_4h['close'].rolling(window=20).mean()
    df_4h['sma50'] = df_4h['close'].rolling(window=50).mean()
    df_4h['std20'] = df_4h['close'].rolling(window=20).std()
    df_4h['bb_upper'] = df_4h['sma20'] + (df_4h['std20'] * 2)
    df_4h['bb_lower'] = df_4h['sma20'] - (df_4h['std20'] * 2)

    df_4h['vol_sma20'] = df_4h['volume'].rolling(window=20).mean()
    
    range_candle = df_4h['high'] - df_4h['low']
    body = abs(df_4h['close'] - df_4h['open'])
    lower_wick = df_4h[['open', 'close']].min(axis=1) - df_4h['low']
    upper_wick = df_4h['high'] - df_4h[['open', 'close']].max(axis=1)
    
    valid_range = range_candle > 0
    safe_range = range_candle.replace(0, np.nan)
    
    has_strong_lower_wick = (lower_wick / safe_range) >= min_wick_ratio
    has_small_upper_wick = (upper_wick / safe_range) <= 0.25
    has_small_body = (body / safe_range) <= 0.35
    
    df_4h['pinbar'] = valid_range & has_strong_lower_wick.fillna(False) & has_small_upper_wick.fillna(False) & has_small_body.fillna(False)
    return df_4h

async def analyze_crypto(exchange, symbol, session, cache, local_now, now_ts, today_str, github_event, btc_dump):
    try:
        f_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=100)
        f_1d = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=300)
        ohlcv_4h, ohlcv_1d = await asyncio.gather(f_4h, f_1d)
        
        if not ohlcv_4h or not ohlcv_1d or len(ohlcv_4h) < 25 or len(ohlcv_1d) < 50:
            logging.error(f"Incomplete OHLCV data for {symbol}")
            return None, None, None

        df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_1d = pd.DataFrame(ohlcv_1d, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        df_4h = calculate_indicators(df_4h)
        df_1d['SMA200'] = df_1d['close'].rolling(200).mean()
        df_1d['SMA50'] = df_1d['close'].rolling(50).mean()

        rsi_1d_val = "N/A"
        if len(df_1d) >= 15:
            delta_1d = df_1d['close'].diff()
            gain_1d = delta_1d.where(delta_1d > 0, 0)
            loss_1d = -delta_1d.where(delta_1d < 0, 0)
            avg_gain_1d = gain_1d.ewm(alpha=1/14, adjust=False).mean()
            avg_loss_1d = loss_1d.ewm(alpha=1/14, adjust=False).mean().replace(0, 1e-10)
            rs_1d = avg_gain_1d / avg_loss_1d
            df_1d['rsi_1d'] = round(100 - (100 / (1 + rs_1d)), 2)
            rsi_1d_val = float(df_1d['rsi_1d'].iloc[-1])

        closed_row = df_4h.iloc[-2]
        prev_closed_row = df_4h.iloc[-3]
        closed_1d = df_1d.iloc[-2]
        
        candle_ts = int(closed_row['timestamp'])

        price = float(closed_row['close'])
        signal_low = float(closed_row['low'])
        sma20_val = float(closed_row['sma20'])
        bb_upper_val = float(closed_row['bb_upper'])
        rsi_4h = float(closed_row['rsi'])
        prev_rsi_4h = float(prev_closed_row['rsi'])

        sma200_val = closed_1d['SMA200']
        sma50_val = closed_1d['SMA50']

        if pd.notna(sma200_val):
            is_above_sma200 = (price >= float(sma200_val))
            trend_1d_status = "Above 200 SMA" if is_above_sma200 else "Below 200 SMA"
        elif pd.notna(sma50_val):
            is_above_sma200 = (price >= float(sma50_val))
            trend_1d_status = "Above 50 SMA (New)" if is_above_sma200 else "Below 50 SMA (New)"
        else:
            is_above_sma200 = True
            trend_1d_status = "No SMA (New)"

        stoch_k = float(closed_row['stoch_k'])
        prev_stoch_k = float(prev_closed_row['stoch_k'])
        stoch_turning_up = (prev_stoch_k <= 25.0 or stoch_k <= 25.0) and (stoch_k > prev_stoch_k)

        is_pinbar = bool(closed_row['pinbar'])

        vol_target = float(closed_row['volume'])
        vol_sma = float(closed_row['vol_sma20']) if pd.notna(closed_row['vol_sma20']) else 0.0
        volume_str = "Standard"
        vol_ratio = 100
        if vol_sma > 0:
            vol_ratio = int((vol_target / vol_sma) * 100)
            vol_emoji = '🟢 (High)' if vol_ratio >= 120 else ('🔴 (Low)' if vol_ratio <= 80 else '🟡 (Standard)')
            volume_str = f"{vol_ratio}% of avg {vol_emoji}"

        prev_vol_target = float(prev_closed_row['volume'])
        prev_vol_sma = float(prev_closed_row['vol_sma20']) if pd.notna(prev_closed_row['vol_sma20']) else 0.0
        prev_vol_ratio = int((prev_vol_target / prev_vol_sma) * 100) if prev_vol_sma > 0 else 100

        price_str = f"${price:,.2f}" if price >= 1.0 else f"${price:,.4f}"

        is_core = symbol in ['BTC/USD', 'ETH/USD']
        if is_core:
            rsi_signal_threshold = 38.0
            max_pinbar_rsi = 42.0
            bb_rebound_rsi_limit = 38.0
            bounce_rsi_limit = 55.0
        else:
            rsi_signal_threshold = 32.0
            max_pinbar_rsi = 38.0
            bb_rebound_rsi_limit = 35.0
            bounce_rsi_limit = 50.0

        bb_touched = float(closed_row['low']) <= float(closed_row['bb_lower'])
        is_green = float(closed_row['close']) > float(closed_row['open'])

        raw_signal = 'NONE'
        if vol_ratio >= 100:
            if is_above_sma200:
                if is_pinbar and rsi_4h <= max_pinbar_rsi:
                    raw_signal = 'BUY_PINBAR'
                elif rsi_4h <= rsi_signal_threshold or (bb_touched and rsi_4h <= bb_rebound_rsi_limit):
                    if is_green or vol_ratio >= 130:
                        raw_signal = 'BUY_REBOUND'
                elif bb_touched and rsi_4h <= bounce_rsi_limit and is_green:
                    raw_signal = 'BUY_BOUNCE'

        if btc_dump and symbol != 'BTC/USD' and raw_signal.startswith('BUY'):
            logging.info(f"🛡️ BTC Guard blocked {raw_signal} signal on {symbol}.")
            raw_signal = 'NONE'

        current_signal_type = raw_signal

        soft_sl = max(0.000001, signal_low * 0.995)
        hard_sl = max(0.000001, min(soft_sl * 0.98, signal_low * 0.95))
        
        tp_level = bb_upper_val if price >= sma20_val else sma20_val

        soft_sl_str = f"${soft_sl:,.2f}" if soft_sl >= 1.0 else f"${soft_sl:,.4f}"
        hard_sl_str = f"${hard_sl:,.2f}" if hard_sl >= 1.0 else f"${hard_sl:,.4f}"
        tp_str = f"${tp_level:,.2f}" if tp_level >= 1.0 else f"${tp_level:,.4f}"

        risk_pct = round(((price - soft_sl) / price) * 100, 2)
        hard_risk_pct = round(((price - hard_sl) / price) * 100, 2)
        tp_pct = round(((tp_level - price) / price) * 100, 2)
        rr_ratio = round(tp_pct / risk_pct, 2) if risk_pct > 0 else 0.0

        status_map = {
            'NONE': '⚪ Neutral',
            'BUY_REBOUND': '⚡️ QUICK REBOUND',
            'BUY_PINBAR': '🕯️ PINBAR REBOUND',
            'BUY_BOUNCE': '🟡 BOUNCE ZONE'
        }
        status_txt = status_map.get(current_signal_type, '⚪ Neutral')
        pinbar_txt = "55%" if is_pinbar else "None"

        clean_symbol = symbol.replace('/', '')
        tv_url = f"https://www.tradingview.com/chart/?symbol=KRAKEN:{clean_symbol}"
        clickable_symbol = f'<a href="{tv_url}"><b>{symbol}</b></a>'

        digest_item = (
            f"📈 {clickable_symbol} • <code>{price_str}</code>\n"
            f" ├ RSI (4H): <code>{rsi_4h}</code>\n"
            f" ├ RSI (1D): <code>{rsi_1d_val}</code> | {trend_1d_status}\n"
            f" ├ Pinbar: {pinbar_txt}\n"
            f" └ Status: {status_txt}"
        )

        pending_alert = None
        candle_key = None

        if current_signal_type != 'NONE':
            candle_key = f"ALERT_{symbol}_{candle_ts}_{current_signal_type}"
            already_alerted = (cache.get(candle_key) is not None)

            prev_bb_touched = float(prev_closed_row['low']) <= float(prev_closed_row['bb_lower'])
            prev_pinbar = bool(prev_closed_row['pinbar'])

            is_new_signal = False
            if prev_vol_ratio < 100:
                is_new_signal = True
            else:
                if current_signal_type == 'BUY_REBOUND':
                    prev_was_rebound = (prev_rsi_4h <= rsi_signal_threshold) or (prev_bb_touched and prev_rsi_4h <= bb_rebound_rsi_limit)
                    is_new_signal = not prev_was_rebound
                elif current_signal_type == 'BUY_PINBAR':
                    prev_was_pinbar = prev_pinbar and (prev_rsi_4h <= max_pinbar_rsi)
                    is_new_signal = not prev_was_pinbar
                elif current_signal_type == 'BUY_BOUNCE':
                    prev_was_bounce = prev_bb_touched and (prev_rsi_4h <= bounce_rsi_limit)
                    is_new_signal = not prev_was_bounce

            should_alert = not already_alerted and is_new_signal and (rr_ratio >= 1.2)

            if should_alert:
                confirmations = []
                confirmations.append(f"Main Trend: {trend_1d_status}")
                if is_pinbar:
                    confirmations.append("Demand Wick 4H (Pinbar >= 55%)")
                if is_green:
                    confirmations.append("Bullish Close: Candle closed green")
                if bb_touched:
                    confirmations.append("Tested Lower Bollinger Band (20, 2)")
                if rsi_4h <= rsi_signal_threshold:
                    confirmations.append(f"RSI 4H: <code>{rsi_4h}</code> | (1D): <code>{rsi_1d_val}</code>")
                if stoch_turning_up:
                    confirmations.append(f"Stochastic RSI curving up (%K: <code>{stoch_k}</code>)")
                confirmations.append(f"Volume: {volume_str}")

                conf_msg = "\n├── " + "\n├── ".join(confirmations[:-1]) + f"\n└── {confirmations[-1]}" if len(confirmations) > 1 else f"\n└── {confirmations[0]}"

                tp_target_name = "Middle BB 20" if price < sma20_val else "Upper BB 20"

                if rr_ratio >= 1.5:
                    status_emoji = "🟢"
                    entry_line = f"├── 🟢 Buy (Market): <code>{price_str}</code>"
                else:
                    status_emoji = "🟡"
                    limit_price = (price + signal_low) / 2
                    limit_price_str = f"${limit_price:,.2f}" if limit_price >= 1.0 else f"${limit_price:,.4f}"
                    entry_line = f"├── 🟡 Set Limit (50% Wick): <code>{limit_price_str}</code>"

                alert_header = f"🟡 <b>BOUNCE ZONE (4H)</b> {status_emoji}" if current_signal_type == 'BUY_BOUNCE' else f"⚡ <b>CRYPTO OPPORTUNITY (SWING 4H)</b> {status_emoji}"

                body = (
                    f"{alert_header}\n"
                    f"───────────────────\n"
                    f"📈 Pair: {clickable_symbol}\n"
                    f"💰 Market Price: <code>{price_str}</code>\n"
                    f"📊 RSI: <code>{rsi_4h}</code> (4H) | <code>{rsi_1d_val}</code> (1D)\n\n"
                    f"⚙️ TRADE PLAN\n"
                    f"{entry_line}\n"
                    f"├── ⚠️ Soft SL (4H): <code>{soft_sl_str}</code> (<code>-{risk_pct}%</code>)\n"
                    f"├── 🛡️ Emerg. SL: <code>{hard_sl_str}</code> (<code>-{hard_risk_pct}%</code>)\n"
                    f"├── 🎯 TP ({tp_target_name}): <code>{tp_str}</code> (<code>+{tp_pct}%</code>)\n"
                    f"├── ⚖️ R:R Ratio: <code>1:{rr_ratio}</code>\n"
                    f"└── 👉 ACTION: Touch of {tp_target_name} on 4H\n\n"
                    f"📈 Market Confirmations:{conf_msg}\n\n"
                    f'👁️ <a href="{tv_url}">View Chart on TradingView</a>'
                )

                pending_alert = body
                candle_key = f"ALERT_{symbol}_{candle_ts}_{current_signal_type}"

        return (digest_item, symbol), pending_alert, candle_key

    except Exception as e:
        logging.error(f"Error analyzing {symbol}: {e}")
        return None, None, None

async def main():
    cache = load_cache()
    
    try:
        exchange_class = getattr(ccxt, EXCHANGE_NAME)
        exchange = exchange_class({'enableRateLimit': True})
    except AttributeError:
        logging.warning(f"Exchange '{EXCHANGE_NAME}' not found. Defaulting to Kraken.")
        exchange = ccxt.kraken({'enableRateLimit': True})
    
    try:
        tz = pytz.timezone(USER_TZ)
    except Exception:
        tz = pytz.timezone('UTC')

    local_now = datetime.now(tz)
    today_str = local_now.strftime('%Y-%m-%d')
    now_ts = datetime.now(timezone.utc).timestamp()
    github_event = os.getenv('GITHUB_EVENT_NAME', '')

    # --- BTC GUARD IMPLEMENTATION ---
    btc_dump = False
    try:
        btc_data = await exchange.fetch_ohlcv('BTC/USD', timeframe='4h', limit=50)
        if btc_data and len(btc_data) >= 20:
            btc_df = pd.DataFrame(btc_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            btc_df = calculate_indicators(btc_df)
            btc_last = btc_df.iloc[-2]
            btc_prev = btc_df.iloc[-3]
            btc_change = ((float(btc_last['close']) - float(btc_prev['close'])) / float(btc_prev['close'])) * 100
            btc_rsi = float(btc_last['rsi'])
            
            if btc_change <= -4.0 or btc_rsi < 28.0:
                btc_dump = True
                logging.warning(f"🚨 ACTIVE BTC GUARD TRIGGERED: Market dump (Change: {btc_change:.2f}%, RSI: {btc_rsi}). Altcoin signals blocked.")
    except Exception as e:
        logging.error(f"Error checking background BTC status: {e}")
    # ---------------------------------

    semaphore = asyncio.Semaphore(3)

    async def sem_process(session, pair):
        async with semaphore:
            return await analyze_crypto(exchange, pair, session, cache, local_now, now_ts, today_str, github_event, btc_dump)

    try:
        async with aiohttp.ClientSession() as session:
            tasks = [sem_process(session, pair) for pair in PAIRS]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            digest_lines = []
            for res in results:
                if isinstance(res, Exception) or res is None:
                    continue
                digest_tuple, alert_msg, candle_key = res
                if digest_tuple:
                    digest_lines.append(digest_tuple)
                if alert_msg:
                    await send_telegram_alert_async(session, alert_msg)
                    if candle_key:
                        cache[candle_key] = True

            # --- TWARDA WYSYŁKA TYLKO RAZ DZIENNIE O 08:00 RANO ---
            if local_now.hour == 8 and cache.get('DIGEST_DATE') != today_str:
                cache['DIGEST_DATE'] = today_str
                
                if digest_lines:
                    digest_title = "🌅 <b>MORNING CRYPTO MARKET DIGEST</b>"
                    await send_chunked_digest(session, digest_title, digest_lines)

    finally:
        await exchange.close()
        save_cache(cache)

if __name__ == '__main__':
    asyncio.run(main())
