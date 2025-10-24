import itertools
import os
import pandas as pd
from datetime import datetime, timedelta
import historical_fetcher
import fractal_detector 
import liquidity_checker
import confirmation_checker_cancelled as confirmation_checker
from db_manager import DatabaseManager
import config
import time
from bisect import bisect_right
from market_structure_detector import (
    run_full_market_structure_chain,
    log_all_fractals,
    startup_find_valid_bos,
)

def safe_float(val, default=0.0):
    try:
        if val is None or val == '' or (isinstance(val, float) and (val != val)):
            return default
        return float(val)
    except Exception:
        return default

timeframes = ['5m']
fibo_levels = [0.618]
rrs = [2]
ns = [2]
symbols = ['SOLUSDT']
stop_values = [1.035]
combinations = list(itertools.product(fibo_levels, stop_values, rrs))

structure_timeframe = '15m'

def get_chunks(start_date, end_date, chunk_days=90, overlap_days=1):
    dt_format = "%Y-%m-%d %H:%M:%S"
    chunks = []
    start = datetime.strptime(start_date, dt_format)
    end = datetime.strptime(end_date, dt_format)
    while start < end:
        chunk_end = min(start + timedelta(days=chunk_days), end)
        overlap_end = min(chunk_end + timedelta(days=overlap_days), end)
        chunks.append((
            start.strftime(dt_format),
            overlap_end.strftime(dt_format)
        ))
        start = chunk_end
    return chunks

def truncate_candles_table(symbol, interval):
    db = DatabaseManager(config.db_config)
    cur = db.conn.cursor()
    cur.execute(f"TRUNCATE TABLE candles_{interval};")
    db.conn.commit()
    cur.close()
    db.close()
    print(f"✅ {symbol} {interval} için candle tablosu sıfırlandı.")

def truncate_fraktal_ve_liquidity_tablolari(interval):
    db = DatabaseManager(config.db_config)
    cur = db.conn.cursor()
    cur.execute(f"TRUNCATE TABLE fractal_{interval};")
    cur.execute("TRUNCATE TABLE open_liquidity;")
    cur.execute("TRUNCATE TABLE confirmed_liquidity;")
    db.conn.commit()
    cur.close()
    db.close()
    print(f"✅ {interval} için fraktal & likidite tabloları sıfırlandı.")

def add_atr_threshold_to_candles(candles, atr_period=14, threshold_window=300):
    df = pd.DataFrame(candles)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    prev_close = df['close'].shift(1)
    tr = pd.concat([
        (df['high'] - df['low']).abs(),
        (df['high'] - prev_close).abs(),
        (df['low'] - prev_close).abs()
    ], axis=1).max(axis=1)
    df['ATR14'] = tr.rolling(window=atr_period, min_periods=1).mean()
    df['mean_atr_100'] = df['ATR14'].rolling(window=threshold_window, min_periods=1).mean()
    df['std_atr_100'] = df['ATR14'].rolling(window=threshold_window, min_periods=1).std(ddof=0)
    df['k'] = df['std_atr_100'] / df['mean_atr_100']
    df['ATR_eşiği'] = df['mean_atr_100'] - df['k'] * df['std_atr_100']
    for i, row in df.iterrows():
        candles[i]['ATR14'] = row['ATR14']
        candles[i]['ATR_eşiği'] = row['ATR_eşiği']
    return candles

def fetch_and_store(symbol, interval, start_str, end_str):
    candles = historical_fetcher.fetch_historical_klines(symbol, interval, start_str=start_str, end_str=end_str)
    candles = add_atr_threshold_to_candles(candles, atr_period=14, threshold_window=300)
    time.sleep(0.4)
    db = DatabaseManager(config.db_config)
    db.insert_bulk_candles(symbol, candles, interval)
    db.close()
    print(f"✅ {interval} candles verisi DB'ye yazıldı.")

def call_detect_fractals(symbol, tf, n=2, start_str=None, end_str=None):
    db = DatabaseManager(config.db_config)
    with db.conn.cursor() as cur:
        cur.execute(f"""
            SELECT open_time, open, high, low, close, volume, close_time 
            FROM candles_{tf}
            WHERE symbol = %s AND open_time >= %s AND open_time < %s
            ORDER BY open_time
        """, (symbol, start_str, end_str))
        candles = [
            {
                'open_time': row[0],
                'open': safe_float(row[1]),
                'high': safe_float(row[2]),
                'low': safe_float(row[3]),
                'close': safe_float(row[4]),
                'volume': safe_float(row[5]),
                'close_time': row[6]
            }
            for row in cur.fetchall()
        ]

    candles = sorted(candles, key=lambda x: x['open_time'])
    times = [c['open_time'] for c in candles]
    if len(times) != len(set(times)):
        print("‼️ UYARI: Candles listesinde duplicate (tekrar eden zaman) var!")
    missing = []
    for i in range(1, len(times)):
        if (times[i] - times[i-1]).total_seconds() > 60*15:
            missing.append((times[i-1], times[i]))
    if missing:
        print("‼️ UYARI: Eksik mum aralıkları:", missing)
    print(f"DEBUG: Candles toplam sayısı: {len(candles)}")

    import fractal_detector
    fractal_detector.detect_fractals_full(symbol, candles, db, tf, n=n)
    db.close()

def call_check_liquidity(symbol, tf, start_str=None, end_str=None):
    db = DatabaseManager(config.db_config)
    with db.conn.cursor() as cur:
        cur.execute(f"""
            SELECT open_time, open, high, low, close, volume, close_time 
            FROM candles_{tf}
            WHERE symbol = %s AND open_time >= %s AND open_time < %s
            ORDER BY open_time
        """, (symbol, start_str, end_str))
        candles = [
            {
                'open_time': row[0],
                'open': safe_float(row[1]),
                'high': safe_float(row[2]),
                'low': safe_float(row[3]),
                'close': safe_float(row[4]),
                'volume': safe_float(row[5]),
                'close_time': row[6]
            }
            for row in cur.fetchall()
        ]
        cur.execute(f"""
            SELECT fractal_time, fractal_type, price, candle_high, candle_low
            FROM fractal_{tf}
            WHERE symbol = %s
            ORDER BY fractal_time ASC
        """, (symbol,))
        rows = cur.fetchall()
        fractals = [{
            'fractal_time': r[0],
            'fractal_type': r[1],
            'fractal_price': safe_float(r[2]),
            'candle_high': safe_float(r[3]) if r[3] is not None else None,
            'candle_low': safe_float(r[4]) if r[4] is not None else None
        } for r in rows]
    valid_fractals = liquidity_checker.get_valid_liquidity_fractals(fractals, candles)
    print(f"✅ {tf} için valid (likidite avı olan) fraktal sayısı: {len(valid_fractals)}")
    db.close()

def call_check_confirmations(symbol, tf, rr, fibo_level, stop_value, log_filename, n, trade_logs, start_str=None, end_str=None):
    db = DatabaseManager(config.db_config)
    with db.conn.cursor() as cur:
        cur.execute(f"""
            SELECT open_time, open, high, low, close, volume, close_time 
            FROM candles_{tf}
            WHERE symbol = %s AND open_time >= %s AND open_time < %s
            ORDER BY open_time
        """, (symbol, start_str, end_str))
        candles = [
            {
                'open_time': row[0],
                'open': safe_float(row[1]),
                'high': safe_float(row[2]),
                'low': safe_float(row[3]),
                'close': safe_float(row[4]),
                'volume': safe_float(row[5]),
                'close_time': row[6]
            }
            for row in cur.fetchall()
        ]
        cur.execute(f"""
            SELECT fractal_time, fractal_type, price, candle_high, candle_low
            FROM fractal_{tf}
            WHERE symbol = %s
            ORDER BY fractal_time ASC
        """, (symbol,))
        rows = cur.fetchall()
        all_fractals = [{
            'fractal_time': r[0],
            'fractal_type': r[1],
            'fractal_price': safe_float(r[2]),
            'candle_high': safe_float(r[3]) if r[3] is not None else None,
            'candle_low': safe_float(r[4]) if r[4] is not None else None
        } for r in rows]
    logs = []
    def file_log(*args, **kwargs):
        msg = " ".join(str(a) for a in args)
        logs.append(msg)

    result = confirmation_checker.run_confirmation_chain(
        all_fractals, candles, trade_logs, symbol=symbol, n=n, rr=rr, fibo_level=fibo_level, stop_value=stop_value,
        log_func=file_log,
        detailed_logs=detailed_logs
    )

    valid_fractals = liquidity_checker.get_valid_liquidity_fractals(all_fractals, candles)
    confirmation_checker.mark_cancelled_trades(detailed_logs, valid_fractals, candles)

    with open(log_filename, "w", encoding="utf-8") as f:
        for l in logs:
            f.write(l + "\n")
    db.close()
    return result

def get_active_trend(trend_log, time_5m):
    if not time_5m or time_5m == "":
        return ""
    try:
        times = []
        for t in trend_log:
            t0 = t[0]
            if not isinstance(t0, datetime):
                try:
                    if "-" in str(t0):
                        t0 = datetime.fromisoformat(str(t0).replace('Z',''))
                    else:
                        t0 = datetime.strptime(str(t0), "%d.%m.%Y %H:%M")
                except Exception as e:
                    print("trend_log içinde datetime parse edilemedi:", t[0], e)
            times.append(t0)

        if not isinstance(time_5m, datetime):
            try:
                if "-" in str(time_5m):
                    time_5m = datetime.fromisoformat(str(time_5m).replace('Z',''))
                else:
                    time_5m = datetime.strptime(str(time_5m), "%d.%m.%Y %H:%M")
            except Exception as e:
                print("time_5m parse hatası:", time_5m, e)
                return ""

        times, trend_log_sorted = zip(*sorted(zip(times, trend_log)))
        idx = bisect_right(times, time_5m) - 1

        if idx >= 0:
            return trend_log_sorted[idx][1]
        else:
            print(f"[trend warning] time_5m ({time_5m}) için uygun trend bulunamadı.")
    except Exception as e:
        print(f"[trend error] {e}, time: {time_5m}")
    return ""

def full_gridsearch(trade_logs, start_str, end_str):
    import csv
    os.makedirs("logs", exist_ok=True)
    summary_lines = []

    for symbol in symbols:
        # 15m için: sadece bir kez truncate + doldur
        truncate_candles_table(symbol, structure_timeframe)
        truncate_fraktal_ve_liquidity_tablolari(structure_timeframe)
        fetch_and_store(symbol, structure_timeframe, start_str, end_str)
        call_detect_fractals(symbol, structure_timeframe, n=2, start_str=start_str, end_str=end_str)
        # Sonra tekrar truncate yok! Artık analiz yapabiliriz.

        # 5m için: sadece bir kez truncate + doldur
        tf = timeframes[0]
        truncate_candles_table(symbol, tf)
        fetch_and_store(symbol, tf, start_str, end_str)
        for n in ns:
            truncate_fraktal_ve_liquidity_tablolari(tf)
            call_detect_fractals(symbol, tf, n=n, start_str=start_str, end_str=end_str)
            # Sonra tekrar truncate yok!
            for fibo_level, stop_value, rr in combinations:
                log_filename = f"logs/{symbol}_{tf}_fibo{fibo_level}_stop{stop_value}_rr{rr}_n{n}.txt"
                print(f"\n🚦 Kombinasyon: {log_filename}")
                call_check_liquidity(symbol, tf, start_str=start_str, end_str=end_str)
                result = call_check_confirmations(symbol, tf, rr, fibo_level, stop_value, log_filename, n, trade_logs, start_str=start_str, end_str=end_str)
                summary = f"{log_filename}: Trade={result['total']} | TP={result['tp']} | SL={result['sl']}"
                summary_lines.append(summary)
                print(summary)

    # BURADAN SONRA TEKRAR TRUNCATE YOK!
    with open("logs/summary_results.txt", "w", encoding="utf-8") as f:
        for l in summary_lines:
            f.write(l + "\n")
    print("🎯 Grid search tamamlandı. Özet: logs/summary_results.txt dosyasında.")

    db = DatabaseManager(config.db_config)
    with db.conn.cursor() as cur:
        cur.execute(f"""
            SELECT open_time, open, high, low, close, volume, close_time 
            FROM candles_{structure_timeframe}
            WHERE symbol = %s AND open_time >= %s AND open_time < %s
            ORDER BY open_time
        """, (symbols[0], start_str, end_str))
        candles_1h = [
            {
                'open_time': row[0],
                'open': safe_float(row[1]),
                'high': safe_float(row[2]),
                'low': safe_float(row[3]),
                'close': safe_float(row[4]),
                'volume': safe_float(row[5]),
                'close_time': row[6]
            }
            for row in cur.fetchall()
        ]
        cur.execute(f"SELECT fractal_time, fractal_type, price FROM fractal_{structure_timeframe} WHERE symbol = %s ORDER BY fractal_time", (symbols[0],))
        fracts_1h = [{'fractal_time': row[0], 'fractal_type': row[1], 'price': safe_float(row[2])} for row in cur.fetchall()]
    db.close()

    up_fractals_1h = [f for f in fracts_1h if f['fractal_type'] == 'UP']
    down_fractals_1h = [f for f in fracts_1h if f['fractal_type'] == 'DOWN']

    fractallog = log_all_fractals(up_fractals_1h, down_fractals_1h)
    with open(f"logs/all_fractals_{symbols[0]}_{structure_timeframe}_{start_str[:10]}_{end_str[:10]}.txt", "w", encoding="utf-8") as f:
        for line in fractallog:
            f.write(line + "\n")
    print(f"✅ Bütün fraktallar log dosyasına yazıldı.")

    logs, trend_log = run_full_market_structure_chain(
        candles_1h, up_fractals_1h, down_fractals_1h, n_pullback=2
    )

    # --- LOG GÜVENLİ DÜZLEŞTİRME ---
    def flatten_logs(data):
        flat = []
        if isinstance(data, list):
            for item in data:
                flat.extend(flatten_logs(item))
        else:
            flat.append(str(data))
        return flat

    flat_logs = flatten_logs(logs)
    output_path = f"logs/market_structure_{symbols[0]}_{structure_timeframe}_{start_str[:10]}_{end_str[:10]}.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("=== MARKET STRUCTURE ANALYSIS ===\n")
        f.write(f"Sembol: {symbols[0]}\nZaman Aralığı: {structure_timeframe}\n\n")
        for line in flat_logs:
            f.write(str(line) + "\n")

        f.write("\n=== TREND DEĞİŞİMLERİ ===\n")
        for t, tr in trend_log:
            f.write(f"{t} → {tr}\n")


    print(f"✅ Market structure analizi tamamlandı. Sonuçlar: {output_path}")

    startup_logs = startup_find_valid_bos(
        candles_1h, up_fractals_1h, down_fractals_1h, n_pullback=2
    )

    log_lines = startup_logs['logs'] if isinstance(startup_logs, dict) and 'logs' in startup_logs else startup_logs
    with open(f"logs/startup_bos_log_{symbols[0]}_{structure_timeframe}_{start_str[:10]}_{end_str[:10]}.txt", "w", encoding="utf-8") as f:
        for line in log_lines:
            f.write(str(line) + "\n")
    print(f"✅ Startup valid BoS log dosyasına yazıldı.")

    if len(trade_logs) > 0 and isinstance(trade_logs[0], dict):
        for log in trade_logs:
            t = log.get('liq_candle_time')
            t_dt = None
            if t:
                if isinstance(t, datetime):
                    t_dt = t
                else:
                    try:
                        if "-" in str(t):
                            t_dt = datetime.fromisoformat(str(t).replace('Z',''))
                        else:
                            t_dt = datetime.strptime(str(t), "%d.%m.%Y %H:%M")
                    except Exception as e:
                        print("[trend_1h parse error]", e, t)
            log['trend_1h'] = get_active_trend(trend_log, t_dt) if t_dt else ""

        df_trades = pd.DataFrame(trade_logs)
        df_trades.to_csv(f"logs/trade_log_with_cancelled.csv", index=False, encoding="utf-8-sig", sep=";")
        print(f"✅ trade_log_with_cancelled.csv dosyasına 1h trend ve cancelled bilgisi eklendi!")

        print("\n--- DEBUG: trend_log ilk 5 kayıt ---")
        for t in trend_log[:5]:
            print(t)
        print(f"trend_log toplam kayıt: {len(trend_log)}\n")

        print("--- DEBUG: trade_logs ilk 10 kayıt ---")
        for i, log in enumerate(trade_logs[:10]):
            print(f"{i}: liq_candle_time={log.get('liq_candle_time')}, trend_1h={log.get('trend_1h')}, status={log.get('status')}")
        print("\n--- DEBUG: trend_1h BOŞ OLANLAR ---")
        for i, log in enumerate(trade_logs):
            if not log.get('trend_1h'):
                print(f"{i}: liq_candle_time={log.get('liq_candle_time')}, trend_1h={log.get('trend_1h')}, status={log.get('status')}")

if __name__ == "__main__":
    start_str = "2025-10-01 00:00:00"
    end_str   = "2025-10-15 00:00:00"
    chunk_list = get_chunks(start_str, end_str, chunk_days=90, overlap_days=1)
    print(f"Chunk listesi: {chunk_list}")
    for i, (chunk_start, chunk_end) in enumerate(chunk_list, 1):
        print(f"\n=== CHUNK {i}: {chunk_start} — {chunk_end} arası backtest başlatılıyor ===")
        detailed_logs = []

        full_gridsearch(detailed_logs, chunk_start, chunk_end)
        if detailed_logs:
            df = pd.DataFrame(detailed_logs)
            df.to_csv("logs/trade_log_with_cancelled.csv", index=False, encoding="utf-8-sig", sep=";")
            print("CSV kaydedildi.")
        else:
            print("Hiç trade yok!")
