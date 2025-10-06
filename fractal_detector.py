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

def detect_fractals_full(symbol, candles, db, tf, n=2):
    log_file = open('fractal_log.txt', 'w', encoding='utf-8')
    sys.stdout = log_file

    clear_fractals_table(symbol, db, tf)
    total_up = total_down = 0
    all_fractals = []

    for i in range(n, len(candles) - n):
        center = candles[i]
        center['high'] = safe_float(center.get('high'))
        center['low']  = safe_float(center.get('low'))
            # 07:45 mumu için özel debug logları:
        if str(center['open_time']) == "2025-07-10 07:45:00":
            print("==[07:45 DEBUG]==")
            print("Center mum:", center)
            print("Komşular (2 öncesi ve 2 sonrası):")
            for j in range(i-2, i+3):
                print(f"  {candles[j]['open_time']} | High: {candles[j]['high']} | Low: {candles[j]['low']}")
            print("=================")

        neighbors = [(candles[j]['open_time'], safe_float(candles[j].get('high')), safe_float(candles[j].get('low')))
                     for j in range(i-n, i+n+1) if j != i]
        komsu_high = [v[1] for v in neighbors]
        komsu_low  = [v[2] for v in neighbors]

        is_up_fractal = all(center['high'] > h for h in komsu_high)
        is_down_fractal = all(center['low'] < l for l in komsu_low)

        print(f"Fraktal test: {center['open_time']} | UP: {is_up_fractal} | DOWN: {is_down_fractal}")
        print("Komşu high'lar:", [(candles[j]['open_time'], safe_float(candles[j].get('high'))) for j in range(i-n, i+n+1) if j != i])
        print("Komşu low'lar :", [(candles[j]['open_time'], safe_float(candles[j].get('low'))) for j in range(i-n, i+n+1) if j != i])

        if is_up_fractal:
            print(f"🔍 Fractal UP tespit edildi: {center['open_time']} - {center['high']}")
            all_fractals.append(('UP', center['open_time'], center['high'], center['low']))
            try:
                db.insert_fractal(
                    symbol, center['open_time'], 'UP', center['high'], tf,
                    candle_high=center['high'], candle_low=center['low']
                )
                total_up += 1

                # -- 07:45 özel kontrolü --
                if str(center['open_time']) in ["2025-07-10 07:45:00", "2025-07-10 07:45:00+00:00"]:
                    print("[DEBUG][AFTER INSERT] DB Sorgusu (UP, 07:45):")
                    with db.conn.cursor() as cur:
                        cur.execute(f"""
                            SELECT symbol, fractal_time, fractal_type, price, candle_high, candle_low
                            FROM fractal_{tf}
                            WHERE symbol = %s AND fractal_time = %s AND fractal_type = 'UP'
                        """, (symbol, center['open_time']))
                        sonuc = cur.fetchall()
                        print("07:45 DB Sonucu:", sonuc)
                    print("====")

            except Exception as e:
                print(f"INSERT ERROR UP {center['open_time']}: {e}")

        if is_down_fractal:
            print(f"🔍 Fractal DOWN tespit edildi: {center['open_time']} - {center['low']}")
            all_fractals.append(('DOWN', center['open_time'], center['low'], center['high']))
            try:
                db.insert_fractal(
                    symbol, center['open_time'], 'DOWN', center['low'], tf,
                    candle_high=center['high'], candle_low=center['low']
                )
                total_down += 1
            except Exception as e:
                print(f"INSERT ERROR DOWN {center['open_time']}: {e}")

    print(f"▶️ Tespit edilen toplam UP fraktal: {total_up}")
    print(f"▶️ Tespit edilen toplam DOWN fraktal: {total_down}")
    print(f"▶️ Toplam tespit edilen fraktal: {len(all_fractals)}")

    # --- 07:45 Sorgusu - Fonksiyon sonunda tekrar! ---
    with db.conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM fractal_{tf} WHERE symbol = %s", (symbol,))
        count_in_db = cur.fetchone()[0]
        print(f"💾 DB'deki gerçek fraktal kayıt sayısı: {count_in_db}")

        # 07:45 tekrar kontrolü
        cur.execute(f"""
            SELECT symbol, fractal_time, fractal_type, price, candle_high, candle_low
            FROM fractal_{tf}
            WHERE symbol = %s AND fractal_time = %s AND fractal_type = 'UP'
        """, (symbol, "2025-07-10 07:45:00"))
        sonuc = cur.fetchall()
        print("[SON] 07:45 UP DB Sorgusu (fonksiyon sonu):", sonuc)
    sys.stdout = sys.__stdout__
    log_file.close()
