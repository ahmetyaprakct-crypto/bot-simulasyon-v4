import sys

def safe_float(val, default=0.0):
    try:
        if val is None or val == '' or (isinstance(val, float) and (val != val)):
            return default
        return float(val)
    except Exception:
        return default


def clear_fractals_table(symbol, db, tf):
    table = f"fractal_{tf}"
    with db.conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE symbol = %s;", (symbol,))
        db.conn.commit()
    print(f"✅ {table} tablosu temizlendi.")

import traceback

def detect_fractals_full(symbol, candles, db=None, tf=None, n=2, write_to_db=True):
    """
    Belirtilen mum verileri üzerinden n fraktal aralığına göre
    UP ve DOWN fraktalları tespit eder. 
    DB'ye kaydeder ve aynı zamanda liste olarak döndürür.
    """
    log_file = open('fractal_log.txt', 'w', encoding='utf-8')
    sys.stdout = log_file

    if write_to_db and db and tf:
        clear_fractals_table(symbol, db, tf)

    total_up = total_down = 0
    all_fractals = []

    for i in range(n, len(candles) - n):
        center = candles[i]
        center['high'] = safe_float(center.get('high'))
        center['low'] = safe_float(center.get('low'))

        # Komşu mumların high ve low değerleri
        neighbors = [
            (candles[j]['open_time'], safe_float(candles[j].get('high')), safe_float(candles[j].get('low')))
            for j in range(i - n, i + n + 1) if j != i
        ]
        komsu_high = [v[1] for v in neighbors]
        komsu_low = [v[2] for v in neighbors]

        # Fraktal kontrolü
        is_up_fractal = all(center['high'] > h for h in komsu_high)
        is_down_fractal = all(center['low'] < l for l in komsu_low)

        print(f"Fraktal test: {center['open_time']} | UP: {is_up_fractal} | DOWN: {is_down_fractal}")

        if is_up_fractal:
            print(f"🔍 Fractal UP tespit edildi: {center['open_time']} - {center['high']}")
            all_fractals.append(('UP', center['open_time'], center['high'], center['low']))
            try:
                if write_to_db and db:
                    db.insert_fractal(
                        symbol, center['open_time'], 'UP', center['high'], tf,
                        candle_high=center['high'], candle_low=center['low']
                    )
                total_up += 1
            except Exception as e:
                print(f"INSERT ERROR UP {center['open_time']}: {e}")
                print(traceback.format_exc())
                if db:
                    db.conn.rollback()

        if is_down_fractal:
            print(f"🔍 Fractal DOWN tespit edildi: {center['open_time']} - {center['low']}")
            all_fractals.append(('DOWN', center['open_time'], center['low'], center['high']))
            try:
                if write_to_db and db:
                    db.insert_fractal(
                        symbol, center['open_time'], 'DOWN', center['low'], tf,
                        candle_high=center['high'], candle_low=center['low']
                    )

                total_down += 1
            except Exception as e:
                print(f"INSERT ERROR DOWN {center['open_time']}: {e}")
                print(traceback.format_exc())
                if db:
                    db.conn.rollback()

    # Özet logları
    print(f"▶️ Tespit edilen toplam UP fraktal: {total_up}")
    print(f"▶️ Tespit edilen toplam DOWN fraktal: {total_down}")
    print(f"▶️ Toplam tespit edilen fraktal: {len(all_fractals)}")

    # DB doğrulama
    if write_to_db and db and tf:
        db.conn.commit()
        with db.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM fractal_{tf} WHERE symbol = %s", (symbol,))
            count_in_db = cur.fetchone()[0]
            print(f"💾 DB'deki gerçek fraktal kayıt sayısı: {count_in_db}")


    # Log çıktısını geri al
    sys.stdout = sys.__stdout__
    log_file.close()

    # --- [YENİ] n=3 fraktallar confirmation için kullanılacak şekilde döndürülür ---
    return [
        {'fractal_time': t, 'fractal_type': typ, 'fractal_price': p}
        for (typ, t, p, _) in all_fractals
    ]
