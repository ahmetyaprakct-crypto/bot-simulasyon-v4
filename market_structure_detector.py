from typing import List, Dict, Optional, Tuple
from db_manager import DatabaseManager

def find_first_two_fractals(up_fractals, down_fractals):
    if not up_fractals or not down_fractals:
        return None, None
    first_up = min(up_fractals, key=lambda f: f['fractal_time'])
    first_down = min(down_fractals, key=lambda f: f['fractal_time'])
    return first_up, first_down


def find_next_fractal(fractals, curr_time, curr_price=None, trend=None):
    """
    Zaman olarak sonraki fraktalı bulur.
    Trend belirtilmişse fiyat yönü kontrolü yapar:
      trend='UP'   → yeni fraktal fiyatı mevcut fraktal fiyatından yüksek olmalı.
      trend='DOWN' → yeni fraktal fiyatı mevcut fraktal fiyatından düşük olmalı.
    """
    candidates = [f for f in fractals if f['fractal_time'] > curr_time]
    if not candidates:
        return None

    if curr_price is not None:
        curr_price = float(curr_price)

    if trend == 'UP' and curr_price is not None:
        candidates = [f for f in candidates if float(f['price']) > curr_price]
    elif trend == 'DOWN' and curr_price is not None:
        candidates = [f for f in candidates if float(f['price']) < curr_price]

    if not candidates:
        return None

    chosen = min(candidates, key=lambda f: f['fractal_time'])
    print(f"DEBUG NEXT FRACTAL FOUND ({trend}): {chosen['fractal_time']} | price={chosen['price']} | ref={curr_price}")
    return chosen



def find_first_fractal_break(candles, up_fractal, down_fractal, start_index=0):
    for i in range(start_index, len(candles)):
        c = candles[i]
        if c['open_time'] > up_fractal['fractal_time'] and c['close'] > up_fractal['price']:
            return 'UP', up_fractal, c, i
        if c['open_time'] > down_fractal['fractal_time'] and c['close'] < down_fractal['price']:
            return 'DOWN', down_fractal, c, i
    return None, None, None, None


def find_first_pullback_fractal(candles: List[Dict], trend: str, start_time, end_time, n=2, used_pb=None) -> Optional[Dict]:
    segment = [c for c in candles if start_time <= c['open_time'] < end_time]
    candidate_indexes = []
    for i in range(n, len(segment)-n):
        if trend == 'UP':
            is_lowest = all(segment[i]['low'] < segment[j]['low'] for j in range(i-n, i+n+1) if j != i)
            if is_lowest and (used_pb is None or segment[i]['open_time'] not in used_pb):
                candidate_indexes.append(i)
        elif trend == 'DOWN':
            is_highest = all(segment[i]['high'] > segment[j]['high'] for j in range(i-n, i+n+1) if j != i)
            if is_highest and (used_pb is None or segment[i]['open_time'] not in used_pb):
                candidate_indexes.append(i)
    if candidate_indexes:
        return segment[candidate_indexes[0]]
    return None


def inducement_taken(candles: List[Dict], trend: str, inducement_candle: Dict, end_time) -> Optional[Dict]:
    segment = [c for c in candles if inducement_candle['open_time'] < c['open_time'] <= end_time]
    for c in segment:
        if trend == 'UP' and c['low'] < inducement_candle['low']:
            return c
        elif trend == 'DOWN' and c['high'] > inducement_candle['high']:
            return c
    return None


def get_extreme_level(candles: List[Dict], trend: str, start_time, end_time) -> Tuple[Optional[float], Optional[str]]:
    segment = [c for c in candles if start_time <= c['open_time'] <= end_time]
    if not segment:
        return None, None
    if trend == 'UP':
        min_low = min(c['low'] for c in segment)
        t = next(c['open_time'] for c in segment if c['low'] == min_low)
        return min_low, t
    else:
        max_high = max(c['high'] for c in segment)
        t = next(c['open_time'] for c in segment if c['high'] == max_high)
        return max_high, t


def get_segment_high_low(candles: List[Dict], start_time, end_time):
    segment = [c for c in candles if start_time <= c['open_time'] <= end_time]
    if not segment:
        return None, None, None, None

    max_high = max(segment, key=lambda c: c['high'])
    min_low = min(segment, key=lambda c: c['low'])
    return max_high['high'], max_high['open_time'], min_low['low'], min_low['open_time']


def log_all_fractals(up_fractals, down_fractals):
    logs = ["==== TÜM UP FRAKTALLAR ===="]
    for f in sorted(up_fractals, key=lambda f: f['fractal_time']):
        logs.append(f"UP | {f['fractal_time']} | {f['price']}")
    logs.append("==== TÜM DOWN FRAKTALLAR ====")
    for f in sorted(down_fractals, key=lambda f: f['fractal_time']):
        logs.append(f"DOWN | {f['fractal_time']} | {f['price']}")
    return logs


def find_all_pullback_fractals(candles: List[Dict], trend: str, start_time, end_time, n=2, used_pb=None) -> List[Dict]:
    segment = [c for c in candles if start_time <= c['open_time'] < end_time]
    pullbacks = []
    for i in range(n, len(segment)-n):
        if trend == 'UP':
            is_lowest = all(segment[i]['low'] < segment[j]['low'] for j in range(i-n, i+n+1) if j != i)
            if is_lowest and (used_pb is None or segment[i]['open_time'] not in used_pb):
                pullbacks.append(segment[i])
        elif trend == 'DOWN':
            is_highest = all(segment[i]['high'] > segment[j]['high'] for j in range(i-n, i+n+1) if j != i)
            if is_highest and (used_pb is None or segment[i]['open_time'] not in used_pb):
                pullbacks.append(segment[i])
    return pullbacks


def is_any_pullback_induced(candles: List[Dict], trend: str, pullbacks: List[Dict], end_time):
    for pb in pullbacks:
        segment = [c for c in candles if pb['open_time'] < c['open_time'] <= end_time]
        for c in segment:
            if trend == 'UP' and c['low'] < pb['low']:
                return True, pb, c
            elif trend == 'DOWN' and c['high'] > pb['high']:
                return True, pb, c
    return False, None, None


def startup_find_valid_bos(candles, up_fractals, down_fractals, n_fractal=10, n_pullback=2):
    logs = []
    used_pb = set()

    curr_up, curr_down = find_first_two_fractals(up_fractals, down_fractals)
    if not curr_up or not curr_down:
        logs.append("Başlangıç için yeterli fraktal yok!")
        return logs

    logs.append(f"İlk UP fraktal: {curr_up['fractal_time']} | {curr_up['price']}")
    logs.append(f"İlk DOWN fraktal: {curr_down['fractal_time']} | {curr_down['price']}")

    for _ in range(100):
        trend, break_fractal, break_candle, break_idx = find_first_fractal_break(candles, curr_up, curr_down)
        if not trend:
            logs.append("Henüz fraktal kırılımı olmadı.")
            break

        next_up = find_next_fractal(up_fractals, curr_up['fractal_time']) if trend == 'UP' else None
        next_down = find_next_fractal(down_fractals, curr_down['fractal_time']) if trend == 'DOWN' else None

        kritik_seviye, kritik_zaman = get_extreme_level(candles, trend, break_fractal['fractal_time'], break_candle['open_time'])
        pullbacks = find_all_pullback_fractals(candles, trend, break_fractal['fractal_time'], break_candle['open_time'], n=n_pullback, used_pb=used_pb)

        if not pullbacks:
            logs.append(f"⚠️ {trend} kırılımı var ({break_fractal['fractal_time']} → {break_candle['open_time']}), ancak pullback fraktal yok.")
            if trend == 'UP' and next_up: curr_up = next_up
            if trend == 'DOWN' and next_down: curr_down = next_down
            continue

        induced, pb, by = is_any_pullback_induced(candles, trend, pullbacks, break_candle['open_time'])
        if not induced:
            logs.append(f"❌ INVALID BOS — {trend} kırılımı: {break_fractal['fractal_time']} → {break_candle['open_time']}")
            logs.append(f"   ↪ Pullback sayısı: {len(pullbacks)} — Ancak inducement bulunamadı.")
            if trend == 'UP' and next_up: curr_up = next_up
            if trend == 'DOWN' and next_down: curr_down = next_down
            continue

        logs.append(f"🔹 Break: {trend} | {break_fractal['fractal_time']} kırıldı → {break_candle['open_time']}")
        for p in pullbacks:
            logs.append(f"   ↪ Pullback: {p['open_time']} | {'low' if trend == 'UP' else 'high'}={p['low'] if trend == 'UP' else p['high']}")
        logs.append(f"✅ VALID BOS — Trend: {trend}")
        logs.append(f"   ↪ Induced PB: {pb['open_time']} | Fiyat: {pb['high'] if trend == 'DOWN' else pb['low']}")
        logs.append(f"   ↪ Inducement geldi: {by['open_time']} | High={by['high']} Low={by['low']}")
        return logs

    logs.append("100 defa döndü, abort.")
    return logs


def format_price(val):
    try:
        return f"{float(val):.1f}"
    except Exception:
        return str(val)


from typing import List, Dict, Tuple

def run_full_market_structure_chain(candles, up_fractals, down_fractals, n_fractal=10, n_pullback=2):
    print("DEBUG: FINAL FIX v4 — inducement güncellemeli tam sürüm")
    logs, trend_log = [], []
    used_pullbacks, used_fractals = set(), set()

    curr_up, curr_down = find_first_two_fractals(up_fractals, down_fractals)
    if not curr_up or not curr_down:
        logs.append("Başlangıç için yeterli fraktal yok!")
        return logs, trend_log

    curr_hh_price = float(curr_up['price'])
    curr_ll_price = float(curr_down['price'])
    next_hh, next_ll = None, None

    # --- Startup ---
    for _ in range(100):
        trend, break_fractal, break_candle, _ = find_first_fractal_break(candles, curr_up, curr_down)
        if not trend:
            logs.append("Valid BoS bulunamadı.")
            return logs, trend_log

        pullbacks = find_all_pullback_fractals(
            candles, trend, break_fractal['fractal_time'], break_candle['open_time'],
            n=n_pullback, used_pb=used_pullbacks
        )
        induced, pb, by = is_any_pullback_induced(candles, trend, pullbacks, break_candle['open_time'])
        if not induced:
            logs.append(f"[INIT] ❌ INVALID {trend} BoS: {break_fractal['fractal_time']} kırıldı @ {break_candle['open_time']}, inducement yok.")
            if trend == 'UP':
                curr_up = find_next_fractal(up_fractals, curr_up['fractal_time'], curr_price=curr_hh_price, trend='UP')
                if curr_up: curr_hh_price = float(curr_up['price'])
            else:
                curr_down = find_next_fractal(down_fractals, curr_down['fractal_time'], curr_price=curr_ll_price, trend='DOWN')
                if curr_down: curr_ll_price = float(curr_down['price'])
            continue

        segment = [c for c in candles if break_fractal['fractal_time'] <= c['open_time'] <= break_candle['open_time']]
        kritik = max(c['high'] for c in segment) if trend == 'DOWN' else min(c['low'] for c in segment)

        logs.append(f"[INIT] ✅ VALID {trend} BoS")
        logs.append(f"   ↪ Kırılan fraktal: {break_fractal['fractal_time']} | Fiyat: {break_fractal['price']}")
        logs.append(f"   ↪ Kıran mum:       {break_candle['open_time']} | Close: {break_candle['close']}")
        logs.append(f"   ↪ Kritik seviye:   {kritik}")
        trend_log.append((break_candle['open_time'], trend))

        segment_start_time = break_candle['open_time']
        if trend == 'UP':
            curr_hh_price = float(break_fractal['price'])
            next_hh = find_next_fractal(up_fractals, break_fractal['fractal_time'], curr_price=curr_hh_price, trend='UP')
        else:
            curr_ll_price = float(break_fractal['price'])
            next_ll = find_next_fractal(down_fractals, break_fractal['fractal_time'], curr_price=curr_ll_price, trend='DOWN')
        break
    else:
        logs.append("Zincir başlatılamadı (startup).")
        return logs, trend_log

    # --- Trend zinciri ---
    while True:
        # ===== DOWN TREND =====
        if trend == 'DOWN':
            if not next_ll:
                logs.append("➡️ Yeni LL yok, DOWN zinciri bitti.")
                break
            if next_ll['fractal_time'] in used_fractals:
                break

            ll_price = float(next_ll['price'])
            print("DEBUG LL CHECK:", next_ll['fractal_time'], ll_price, curr_ll_price)
            if ll_price >= curr_ll_price:
                print("DEBUG PRICE FILTER TRIGGERED (DOWN):", next_ll['fractal_time'], ll_price, curr_ll_price)
                logs.append(f"⏩ LL {next_ll['fractal_time']} tamamen atlandı (fiyat {ll_price} ≥ mevcut LL {curr_ll_price}).")
                used_fractals.add(next_ll['fractal_time'])
                next_ll = find_next_fractal(down_fractals, next_ll['fractal_time'], curr_price=curr_ll_price, trend='DOWN')
                continue

            breaker = next((c for c in candles if c['open_time'] > next_ll['fractal_time'] and c['close'] < next_ll['price']), None)
            if not breaker:
                logs.append(f"❌ LL {next_ll['fractal_time']} kırılmadı, bekleniyor...")
                break

            pullbacks = find_all_pullback_fractals(candles, 'DOWN', next_ll['fractal_time'], breaker['open_time'], n=n_pullback)
            induced, pb, by = is_any_pullback_induced(candles, 'DOWN', pullbacks, breaker['open_time'])
            if not induced:
                logs.append(f"❌ LL {next_ll['fractal_time']} kırıldı ama inducement yok, yeni LL aranıyor.")
                used_fractals.add(next_ll['fractal_time'])
                curr_ll_price = min(curr_ll_price, ll_price)  # <-- kritik ekleme
                segment_start_time = next_ll['fractal_time']
                next_ll = find_next_fractal(down_fractals, next_ll['fractal_time'], curr_price=curr_ll_price, trend='DOWN')
                continue

            seg = [c for c in candles if next_ll['fractal_time'] <= c['open_time'] <= breaker['open_time']]
            kritik = max(c['high'] for c in seg)
            logs.append(f"[CONT] ✅ DOWN devam → LL: {next_ll['fractal_time']} | Fiyat: {ll_price}")
            logs.append(f"   ↪ Yeni kritik seviye: {kritik}")
            trend_log.append((breaker['open_time'], trend))
            used_fractals.add(next_ll['fractal_time'])
            curr_ll_price = ll_price
            segment_start_time = next_ll['fractal_time']
            next_ll = find_next_fractal(down_fractals, segment_start_time, curr_price=curr_ll_price, trend='DOWN')

        # ===== UP TREND =====
        else:
            if not next_hh:
                logs.append("➡️ Yeni HH yok, UP zinciri bitti.")
                break
            if next_hh['fractal_time'] in used_fractals:
                break

            hh_price = float(next_hh['price'])
            print("DEBUG HH CHECK:", next_hh['fractal_time'], hh_price, curr_hh_price)
            if hh_price <= curr_hh_price:
                print("DEBUG PRICE FILTER TRIGGERED (UP):", next_hh['fractal_time'], hh_price, curr_hh_price)
                logs.append(f"⏩ HH {next_hh['fractal_time']} tamamen atlandı (fiyat {hh_price} ≤ mevcut HH {curr_hh_price}).")
                used_fractals.add(next_hh['fractal_time'])
                next_hh = find_next_fractal(up_fractals, next_hh['fractal_time'], curr_price=curr_hh_price, trend='UP')
                continue

            breaker = next((c for c in candles if c['open_time'] > next_hh['fractal_time'] and c['close'] > next_hh['price']), None)
            if not breaker:
                logs.append(f"❌ HH {next_hh['fractal_time']} kırılmadı, bekleniyor...")
                break

            pullbacks = find_all_pullback_fractals(candles, 'UP', next_hh['fractal_time'], breaker['open_time'], n=n_pullback)
            induced, pb, by = is_any_pullback_induced(candles, 'UP', pullbacks, breaker['open_time'])
            if not induced:
                logs.append(f"❌ HH {next_hh['fractal_time']} kırıldı ama inducement yok, yeni HH aranıyor.")
                used_fractals.add(next_hh['fractal_time'])
                curr_hh_price = max(curr_hh_price, hh_price)  # <-- kritik ekleme
                segment_start_time = next_hh['fractal_time']
                next_hh = find_next_fractal(up_fractals, next_hh['fractal_time'], curr_price=curr_hh_price, trend='UP')
                continue

            seg = [c for c in candles if next_hh['fractal_time'] <= c['open_time'] <= breaker['open_time']]
            kritik = min(c['low'] for c in seg)
            logs.append(f"[CONT] ✅ UP devam → HH: {next_hh['fractal_time']} | Fiyat: {hh_price}")
            logs.append(f"   ↪ Yeni kritik seviye: {kritik}")
            trend_log.append((breaker['open_time'], trend))
            used_fractals.add(next_hh['fractal_time'])
            curr_hh_price = hh_price
            segment_start_time = next_hh['fractal_time']
            next_hh = find_next_fractal(up_fractals, segment_start_time, curr_price=curr_hh_price, trend='UP')

        # --- CHOCH kontrolü ---
        for cc in candles:
            if cc['open_time'] > segment_start_time:
                if trend == 'DOWN' and cc['high'] > kritik:
                    logs.append(f"[CHOCH] Trend DOWN → UP @ {cc['open_time']} | Close={cc['close']}")
                    trend = 'UP'
                    trend_log.append((cc['open_time'], trend))
                    segment_start_time = cc['open_time']
                    next_hh = find_next_fractal(up_fractals, segment_start_time, curr_price=curr_hh_price, trend='UP')
                    break
                if trend == 'UP' and cc['low'] < kritik:
                    logs.append(f"[CHOCH] Trend UP → DOWN @ {cc['open_time']} | Close={cc['close']}")
                    trend = 'DOWN'
                    trend_log.append((cc['open_time'], trend))
                    segment_start_time = cc['open_time']
                    next_ll = find_next_fractal(down_fractals, segment_start_time, curr_price=curr_ll_price, trend='DOWN')
                    break

    return logs, trend_log



















