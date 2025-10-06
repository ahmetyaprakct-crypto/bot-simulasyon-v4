import requests
from datetime import datetime,timedelta
from db_manager import DatabaseManager
from fractal_detector import detect_fractals_full
from liquidity_checker import get_valid_liquidity_fractals
from confirmation_checker import run_confirmation_chain
from notifier import check_and_send_alarms
import requests
import time

start_str = "2025-05-15 00:00:00"
end_str   = "2025-05-17 00:00:00"

def safe_float(val, default=0.0):
    try:
        if val is None or val == '' or (isinstance(val, float) and (val != val)):
            return default
        return float(val)
    except Exception:
        return default

def fetch_historical_klines(symbol, interval='1m', start_str=None, end_str=None, max_retries=5):
    url = "https://fapi.binance.com/fapi/v1/klines"
    all_candles = []
    dt_format = "%Y-%m-%d %H:%M:%S"

    if not start_str or not end_str:
        raise ValueError("start_str ve end_str gereklidir!")

    current_start = datetime.strptime(start_str, dt_format)
    end_dt = datetime.strptime(end_str, dt_format)

    while current_start < end_dt:
        current_end = min(current_start + timedelta(hours=12), end_dt)
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': int(current_start.timestamp() * 1000),
            'endTime': int(current_end.timestamp() * 1000),
            'limit': 1000
        }

        # === Timeout ve retry ekliyoruz ===
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                break  # Başarılıysa döngüyü kır
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                print(f"[{symbol}] API isteği zaman aşımı! Tekrar deneniyor... (Deneme {attempt+1})")
                time.sleep(3)
            except Exception as e:
                print(f"[{symbol}] Diğer API hatası: {e} (Deneme {attempt+1})")
                time.sleep(3)
        else:
            raise Exception(f"[{symbol}] API hatası: {max_retries} defa denenip başarısız oldu.")

        if not data:
            break  # Hiç veri dönmediyse, daha fazla ilerleme

        for item in data:
            all_candles.append({
                'open_time': datetime.fromtimestamp(item[0] / 1000),
                'open': safe_float(item[1]),
                'high': safe_float(item[2]),
                'low': safe_float(item[3]),
                'close': safe_float(item[4]),
                'volume': safe_float(item[5]),
                'close_time': datetime.fromtimestamp(item[6] / 1000),
            })

        last_open_time = data[-1][0] / 1000
        current_start = datetime.fromtimestamp(last_open_time) + timedelta(seconds=1)
        # time.sleep(0.2)  # API limitine takılmamak için mini bekleme (opsiyonel)

    return sorted(all_candles, key=lambda x: x['open_time'])

def get_fractals_from_db(db, symbol):
    with db.conn.cursor() as cur:
        cur.execute("""
            SELECT fractal_time, fractal_type, price
            FROM fractals
            WHERE symbol = %s;
        """, (symbol,))
        rows = cur.fetchall()
    return [{'fractal_time': row[0], 'fractal_type': row[1], 'price': row[2]} for row in rows]

def get_liquidity_taken_fractals(db, symbol):
    with db.conn.cursor() as cur:
        cur.execute("""
            SELECT fractal_time, fractal_type, fractal_price, liquidity_time
            FROM open_liquidity
            WHERE symbol = %s AND liquidity_taken = TRUE;
        """, (symbol,))
        rows = cur.fetchall()
    return [{
        'fractal_time': row[0],
        'fractal_type': row[1],
        'fractal_price': row[2],
        'liquidity_time': row[3]
    } for row in rows]

if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'dbname': 'likidite_db',
        'user': 'postgres',
        'password': '933464'
    }

    symbol = ""

    # 1. Veri çek
    candles = fetch_historical_klines(
        "symbol", "5m",
        start_str="2025-06-01 00:00:00",
        end_str="2025-06-14 00:00:00"
    )
    print(f"Toplam çekilen mum sayısı: {len(candles)}")


    # 2. DB bağlantısı
    db = DatabaseManager(db_config)

    # 3. Mumları veritabanına yaz
    db.insert_candles(symbol, candles)

    # 4. Fraktalları tespit et
    detect_fractals_full(symbol, candles, db)

    # 5. Fraktalları veritabanından al
    fractals = get_fractals_from_db(db, symbol)

    # 6. Likidite kontrolü yap
    valid_fractals = get_valid_liquidity_fractals(fractals, candles)

    # 7. Likidite alınanları al
    liquidity_fractals = get_liquidity_taken_fractals(db, symbol)

    # 8. Onay kontrolü yap
    run_confirmation_chain(fractals, candles, rr=2, fibo_level=0.618)

    # 9. Alarm gönder (tek seferlik)
    check_and_send_alarms(db)

    # 10. Bağlantıyı kapat
    db.close()
