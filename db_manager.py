import psycopg2
from psycopg2.extras import execute_values

def safe_float(val, default=0.0):
    try:
        if val is None or val == '' or (isinstance(val, float) and (val != val)):
            return default
        return float(val)
    except Exception:
        return default

class DatabaseManager:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        # Varsayılan olarak 1m tablo aç; başka interval için ayrıca çağırabilirsin!
        self.create_candles_table('1m')
        self.create_fractals_table('1m')
        self.create_candles_table('15m')
        self.create_fractals_table('15m')
        self.create_candles_table('5m')
        self.create_fractals_table('5m')
        self.create_open_liquidity_table()
        self.create_confirmed_liquidity_table()

    def create_candles_table(self, interval='1m'):
        table_name = f"candles_{interval}"
        with self.conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    open_time TIMESTAMP NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume NUMERIC,
                    close_time TIMESTAMP,
                    UNIQUE(symbol, open_time)
                );
            """)
            self.conn.commit()

    def create_fractals_table(self, interval='1m'):
        table_name = f"fractal_{interval}"
        with self.conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    fractal_time TIMESTAMP NOT NULL,
                    fractal_type TEXT CHECK (fractal_type IN ('UP', 'DOWN')),
                    price NUMERIC,
                    candle_high NUMERIC,
                    candle_low NUMERIC
                );
            """)
            self.conn.commit()


    def create_open_liquidity_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS open_liquidity (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    fractal_time TIMESTAMP NOT NULL,
                    fractal_type TEXT CHECK (fractal_type IN ('UP', 'DOWN')),
                    fractal_price NUMERIC,
                    liquidity_taken BOOLEAN,
                    liquidity_time TIMESTAMP,
                    timeframe TEXT,
                    UNIQUE(symbol, fractal_time, fractal_type)
                );
            """)
            self.conn.commit()

    def create_confirmed_liquidity_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS confirmed_liquidity (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    fractal_time TIMESTAMP NOT NULL,
                    fractal_type TEXT CHECK (fractal_type IN ('UP', 'DOWN')),
                    fractal_price NUMERIC,
                    confirm_time TIMESTAMP,
                    confirm_price NUMERIC,
                    confirmed BOOLEAN DEFAULT FALSE,
                    status TEXT CHECK (status IN ('waiting', 'aborted', 'confirmed')) DEFAULT 'waiting',
                    notified BOOLEAN DEFAULT FALSE,
                    timeframe TEXT,
                    liquidity_price NUMERIC
                );
            """)
            self.conn.commit()

    def insert_bulk_candles(self, symbol, candles, interval):
        table_name = f"candles_{interval}"
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name} WHERE symbol = %s;", (symbol,))
            self.conn.commit()
            execute_values(cur, f"""
                INSERT INTO {table_name} (
                    symbol, open_time, open, high, low, close, volume, close_time
                ) VALUES %s
                ON CONFLICT (symbol, open_time) DO NOTHING
            """, [
                (
                    symbol,
                    c['open_time'], c['open'], c['high'], c['low'],
                    c['close'], c['volume'], c['close_time']
                ) for c in candles
            ])
            self.conn.commit()

    def get_prior_opposite_fractal(self, symbol, interval, current_time, opposite_type):
        table_name = f"fractal_{interval}"
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT fractal_time, price, fractal_type
                FROM {table_name}
                WHERE symbol = %s AND fractal_type = %s AND fractal_time < %s
                ORDER BY fractal_time DESC
                LIMIT 1;
            """, (symbol, opposite_type, current_time))
            row = cur.fetchone()
            if row:
                return {
                    'fractal_time': row[0],
                    'fractal_price': safe_float(row[1]),
                    'fractal_type': row[2]
                }
            return None

    def insert_single_candle(self, symbol, candle, interval='1m'):
        table_name = f"candles_{interval}"
        with self.conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table_name} (
                    symbol, open_time, open, high, low, close, volume, close_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                symbol,
                candle['open_time'], candle['open'], candle['high'], candle['low'],
                candle['close'], candle['volume'], candle['close_time']
            ))
            self.conn.commit()

    def insert_fractal(self, symbol, open_time, fractal_type, price, interval='1m', candle_high=None, candle_low=None):
        table_name = f"fractal_{interval}"
        with self.conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table_name} (symbol, fractal_time, fractal_type, price, candle_high, candle_low)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (symbol, open_time, fractal_type, price, candle_high, candle_low))
            self.conn.commit()


    def close(self):
        self.conn.close()
