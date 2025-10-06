import asyncio
import websockets
import json
import requests
from datetime import datetime, timedelta
from db_manager import DatabaseManager
from fractal_detector import detect_fractals
from liquidity_checker import check_liquidity
from confirmation_checker import check_confirmations
from notifier import check_and_send_alarms

# İzlenecek pariteler
SYMBOLS = ['BNBUSDT', 'ETHUSDT', 'SOLUSDT', 'BTCUSDT', 'XRPUSDT']  # istediğin kadar ekleyebilirsin

# Binance WebSocket base URL
WS_BASE_URL = "wss://stream.binance.com:9443/ws"

def sync_missing_candles(symbol, db):
    with db.conn.cursor() as cur:
        # 1. Şu an kaç kayıt var?
        cur.execute("SELECT COUNT(*) FROM candles_1m WHERE symbol = %s;", (symbol,))
        count = cur.fetchone()[0]

    if count < 1000:
        # Veri azsa → 1000 dakikalık geçmişi yükle
        print(f"📥 [{symbol}] Veri az ({count}), 1000 mum çekiliyor.")
        now = datetime.now()
        start_time = now - timedelta(minutes=1000)
        start_time_ms = int(start_time.timestamp() * 1000)
    else:
        # Veri yeterliyse → sadece eksik 10 dakikaya bak
        with db.conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(open_time)
                FROM candles_1m
                WHERE symbol = %s;
            """, (symbol,))
            last_time = cur.fetchone()[0]
        start_time_ms = int((last_time - timedelta(minutes=10)).timestamp() * 1000)
        print(f"🔁 [{symbol}] Eksik mumlar aranıyor.")

    now_ms = int(datetime.now().timestamp() * 1000)

    if start_time_ms >= now_ms:
        print(f"✅ [{symbol}] Eksik mum yok.")
        return

    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": start_time_ms,
        "endTime": now_ms,
        "limit": 1000
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        for item in data:
            candle = {
                'open_time': datetime.fromtimestamp(item[0] / 1000).replace(microsecond=0),
                'open': float(item[1]),
                'high': float(item[2]),
                'low': float(item[3]),
                'close': float(item[4]),
                'volume': float(item[5]),
                'close_time': datetime.fromtimestamp(item[6] / 1000).replace(microsecond=0)
            }
            db.insert_single_candle(symbol, candle)

        print(f"✅ [{symbol}] {len(data)} mum başarıyla yüklendi.")

    except Exception as e:
        print(f"⛔ [{symbol}] REST veri çekme hatası: {e}")



async def handle_symbol(symbol, db):
    sync_missing_candles(symbol, db)

    url = f"{WS_BASE_URL}/{symbol.lower()}@kline_1m"

    async with websockets.connect(url) as ws:
        print(f"📡 [{symbol}] WebSocket bağlantısı kuruldu.")

        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            kline = data['k']
            if not kline['x']:
                continue  # mum kapanmadıysa atla

            candle = {
                'open_time': datetime.fromtimestamp(kline['t'] / 1000).replace(microsecond=0),
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c']),
                'volume': float(kline['v']),
                'close_time': datetime.fromtimestamp(kline['T'] / 1000).replace(microsecond=0)
            }

            db.insert_single_candle(symbol, candle)

            # Zincir işlemler: fraktal, likidite, onay, alarm
            with db.conn.cursor() as cur:
                cur.execute("""
                    SELECT open_time, open, high, low, close, volume, close_time
                    FROM candles_1m
                    WHERE symbol = %s
                    ORDER BY open_time DESC
                    LIMIT 100;
                """, (symbol,))
                rows = cur.fetchall()

            candles = [{
                'open_time': row[0],
                'open': float(row[1]),
                'high': float(row[2]),
                'low': float(row[3]),
                'close': float(row[4]),
                'volume': float(row[5]),
                'close_time': row[6]
            } for row in reversed(rows)]

            detect_fractals(symbol, candles, db)

            with db.conn.cursor() as cur:
                cur.execute("""
                    SELECT fractal_time, fractal_type, price
                    FROM fractals
                    WHERE symbol = %s;
                """, (symbol,))
                fractals = [{'fractal_time': r[0], 'fractal_type': r[1], 'price': r[2]} for r in cur.fetchall()]

            check_liquidity(symbol, candles, fractals, db)

            with db.conn.cursor() as cur:
                cur.execute("""
                    SELECT fractal_time, fractal_type, fractal_price, liquidity_time
                    FROM open_liquidity
                    WHERE symbol = %s AND liquidity_taken = TRUE;
                """, (symbol,))
                liquidity_fractals = [{
                    'fractal_time': r[0],
                    'fractal_type': r[1],
                    'fractal_price': r[2],
                    'liquidity_time': r[3]
                } for r in cur.fetchall()]

            check_confirmations(symbol, candles, liquidity_fractals, fractals, db)

            check_and_send_alarms(db)

async def main():
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'dbname': 'likidite_db',
        'user': 'postgres',
        'password': '933464'
    }

    db = DatabaseManager(db_config)

    tasks = [handle_symbol(symbol, db) for symbol in SYMBOLS]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
