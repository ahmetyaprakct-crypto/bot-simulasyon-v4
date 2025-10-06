import logging
import config
import psycopg2
from db_manager import DatabaseManager
from datetime import datetime

import historical_fetcher
import fractal_detector
import liquidity_checker
import confirmation_checker

SYMBOL = ""

def liq_time_str(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S') if dt else '-'

def get_conn():
    return psycopg2.connect(**config.db_config)

def fetch_and_store(symbol, interval, start_str=None, end_str=None):
    candles = historical_fetcher.fetch_historical_klines(symbol, interval, start_str=start_str, end_str=end_str)
    db = DatabaseManager(config.db_config)
    db.insert_bulk_candles(symbol, candles)
    db.close()
    print(f"✅ {interval} candles verisi DB'ye yazıldı.")

def call_detect_fractals(tf):
    candles = historical_fetcher.fetch_historical_klines(SYMBOL, tf, limit=1000)
    db = DatabaseManager(config.db_config)
    fractal_detector.detect_fractals(tf, candles, db)
    db.close()

def call_check_liquidity(tf):
    candles = historical_fetcher.fetch_historical_klines(SYMBOL, tf, limit=1000)
    db = DatabaseManager(config.db_config)
    cur = db.conn.cursor()
    cur.execute(f"""
        SELECT fractal_time, price, fractal_type
        FROM fractal_{tf}
        ORDER BY fractal_time ASC
    """)
    rows = cur.fetchall()
    fractals = [{'fractal_time': r[0], 'price': float(r[1]), 'type': r[2]} for r in rows]
    cur.close()
    # GÜNCELLENEN fonksiyon:
    valid_fractals = liquidity_checker.get_valid_liquidity_fractals(fractals, candles)
    # İstersen veritabanına valid_fractals'ı yazabilirsin
    db.close()
    print(f"✅ {tf} için likidite avı olan fraktallar bulundu ({len(valid_fractals)})")

def call_check_confirmations(tf):
    candles = historical_fetcher.fetch_historical_klines(SYMBOL, tf, limit=1000)
    db = DatabaseManager(config.db_config)

    cur = db.conn.cursor()
    cur.execute(f"""
        SELECT fractal_time, price, fractal_type
        FROM fractal_{tf}
        ORDER BY fractal_time ASC
    """)
    rows = cur.fetchall()
    all_fractals = [{
        'fractal_time': r[0],
        'fractal_price': float(r[1]),
        'fractal_type': r[2]
    } for r in rows]
    cur.close()

    # GÜNCELLENEN fonksiyon: Artık likidite ve confirmation zincirini tek fonksiyonda yapıyor.
    confirmation_checker.run_confirmation_chain(all_fractals, candles, rr=2, fibo_level=0.618)
    db.close()

# Trade Simülasyonu
def simulate_outcome(symbol, interval, entry_ratio, rr_ratio, version_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT fractal_time, price, fractal_type
        FROM fractal_{interval}
        ORDER BY fractal_time ASC
    """)
    all_fractals = [{
        'fractal_time': r[0],
        'fractal_type': r[2],
        'fractal_price': float(r[1])
    } for r in cur.fetchall()]
    cur.execute(f"""
        SELECT open_time, high, low, close
        FROM candles_{interval}
        ORDER BY open_time ASC
    """)
    candles = [{'open_time': r[0], 'high': float(r[1]), 'low': float(r[2]), 'close': float(r[3])} for r in cur.fetchall()]
    cur.close()
    conn.close()

    # Zincirin tamamını yeniden çalıştır
    print(f"▶️ Trade simülasyonu başlatılıyor ({interval} - {version_id})")
    confirmation_checker.run_confirmation_chain(all_fractals, candles, rr=rr_ratio, fibo_level=entry_ratio)
    print(f"✅ Simülasyon tamamlandı.")

if __name__ == "__main__":
    # Candle, fraktal, likidite ve trade zincirini sırayla kur
    for tf in ['1m', '3m', '5m']:
        fetch_and_store(SYMBOL, tf)
        call_detect_fractals(tf)
        call_check_liquidity(tf)
        call_check_confirmations(tf)

    versions = [
        {'id': 'v1', 'entry_ratio': 0.6, 'rr': 2, 'tf': '1m'},
        {'id': 'v2', 'entry_ratio': 0.6, 'rr': 3, 'tf': '1m'},
        {'id': 'v3', 'entry_ratio': 0.6, 'rr': 4, 'tf': '1m'}
    ]
    for v in versions:
        simulate_outcome(SYMBOL, v['tf'], v['entry_ratio'], v['rr'], v['id'])

    print("🚀 Tüm backtestler tamamlandı.")
