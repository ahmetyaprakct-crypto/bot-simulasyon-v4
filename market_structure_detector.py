from typing import List, Dict, Optional, Tuple
from db_manager import DatabaseManager



def find_first_two_fractals(up_fractals, down_fractals):
    if not up_fractals or not down_fractals:
        return None, None
    first_up = min(up_fractals, key=lambda f: f['fractal_time'])
    first_down = min(down_fractals, key=lambda f: f['fractal_time'])
    return first_up, first_down

def find_next_fractal(fractals, curr_time):
    nexts = [f for f in fractals if f['fractal_time'] > curr_time]
    if not nexts:
        return None
    return min(nexts, key=lambda f: f['fractal_time'])

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
            if is_lowest:
                if used_pb is None or segment[i]['open_time'] not in used_pb:
                    candidate_indexes.append(i)
        elif trend == 'DOWN':
            is_highest = all(segment[i]['high'] > segment[j]['high'] for j in range(i-n, i+n+1) if j != i)
            if is_highest:
                if used_pb is None or segment[i]['open_time'] not in used_pb:
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
    """
    Verilen aralıkta segmentin en yüksek high ve en düşük low değerlerini ve zamanlarını döndürür.
    """
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

        pullbacks = find_all_pullback_fractals(
            candles, trend, break_fractal['fractal_time'], break_candle['open_time'], n=n_pullback, used_pb=used_pb
        )

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
    return f"{val:.1f}" if isinstance(val, float) else str(val)

def run_full_market_structure_chain(
    candles, up_fractals, down_fractals, n_fractal=10, n_pullback=2
):
    logs = []
    trend_log = []
    used_pullbacks = set()
    used_lls = set()

    curr_up, curr_down = find_first_two_fractals(up_fractals, down_fractals)
    if not curr_up or not curr_down:
        logs.append("Başlangıç için yeterli fraktal yok!")
        return logs, trend_log

    for _ in range(100):
        trend, break_fractal, break_candle, _ = find_first_fractal_break(
            candles, curr_up, curr_down
        )
        if not trend:
            logs.append("Valid BoS bulunamadı.")
            return logs, trend_log

        pullbacks = find_all_pullback_fractals(
            candles, trend, break_fractal['fractal_time'], break_candle['open_time'],
            n=n_pullback, used_pb=used_pullbacks
        )
        induced, pb, by = is_any_pullback_induced(
            candles, trend, pullbacks, break_candle['open_time']
        )
        if not induced:
            logs.append(f"[INIT] ❌ INVALID {trend} BoS: {break_fractal['fractal_time']} kırıldı @ {break_candle['open_time']}, inducement yok.")
            if trend == 'UP':
                curr_up = find_next_fractal(up_fractals, curr_up['fractal_time'])
            else:
                curr_down = find_next_fractal(down_fractals, curr_down['fractal_time'])
            continue

        # Kritik seviye hesapla
        segment_candles = [
            c for c in candles if break_fractal['fractal_time'] <= c['open_time'] <= break_candle['open_time']
        ]
        kritik_seviye = (
            max(c['high'] for c in segment_candles)
            if trend == 'DOWN'
            else min(c['low'] for c in segment_candles)
        )

        logs.append(f"[INIT] ✅ VALID {trend} BoS")
        logs.append(f"   ↪ Kırılan fraktal: {break_fractal['fractal_time']} | Fiyat: {format_price(break_fractal['price'])}")
        logs.append(f"   ↪ Kıran mum:       {break_candle['open_time']} | Close: {format_price(break_candle['close'])}")
        for p in pullbacks:
            logs.append(f"   ↪ Pullback:        {p['open_time']} | Fiyat: {format_price(p['high'] if trend == 'DOWN' else p['low'])}")
        logs.append(f"   ↪ Induced PB:      {pb['open_time']} kırıldı → {by['open_time']}")
        logs.append(f"   ↪ Kritik seviye:   {format_price(kritik_seviye)}")
        trend_log.append((break_candle['open_time'], trend))

        # --- Yeni segment başlangıcı: Kıran mumdan n kadar geri başla
        n = n_pullback  # veya başka bir parametre, algoritmandaki n ile aynı olmalı
        breaker_idx = next(idx for idx, c in enumerate(candles) if c['open_time'] == break_candle['open_time'])
        segment_start_idx = max(breaker_idx - n, 0)
        segment_start_time = candles[segment_start_idx]['open_time']
        break
    else:
        logs.append("Zincir başlatılamadı (startup).")
        return logs, trend_log

    # --- Segment zinciri ---
    while True:
        if trend == 'DOWN':
            next_ll = find_next_fractal(down_fractals, segment_start_time)
            if not next_ll:
                logs.append("➡️ Artık yeni LL (DOWN fraktal) yok.")
                break
            if next_ll['fractal_time'] in used_lls:
                break
            breaker = next((x for x in candles if x['open_time'] > next_ll['fractal_time'] and x['close'] < next_ll['price']), None)
            if not breaker:
                logs.append(f"❌ LL {next_ll['fractal_time']} kırılmadı, bekleniyor...")
                break

            pullbacks = find_all_pullback_fractals(
                candles, 'DOWN', next_ll['fractal_time'], breaker['open_time'], n=n_pullback, used_pb=used_pullbacks
            )
            induced, pb, by = is_any_pullback_induced(candles, 'DOWN', pullbacks, breaker['open_time'])
            if not induced:
                logs.append(f"❌ LL {next_ll['fractal_time']} kırıldı ama inducement yok, devam edilmiyor.")
                break

            segment_candles = [c for c in candles if next_ll['fractal_time'] <= c['open_time'] <= breaker['open_time']]
            kritik_seviye = max(c['high'] for c in segment_candles)

            logs.append(f"[CONT] ✅ DOWN devam → LL: {next_ll['fractal_time']} | Fiyat: {format_price(next_ll['price'])}")
            logs.append(f"   ↪ Kıran mum: {breaker['open_time']} | Close: {format_price(breaker['close'])}")
            for p in pullbacks:
                logs.append(f"   ↪ Pullback: {p['open_time']} | High: {format_price(p['high'])}")
            logs.append(f"   ↪ Induced PB: {pb['open_time']} → {by['open_time']}")
            logs.append(f"   ↪ Yeni kritik seviye: {format_price(kritik_seviye)}")
            trend_log.append((breaker['open_time'], trend))
            # Burada yeni segment başlangıcı yine kıran mumdan n geri alınmalı!
            breaker_idx = next(idx for idx, c in enumerate(candles) if c['open_time'] == breaker['open_time'])
            segment_start_idx = max(breaker_idx - n, 0)
            segment_start_time = candles[segment_start_idx]['open_time']
            used_lls.add(next_ll['fractal_time'])
            used_pullbacks.update(p['open_time'] for p in pullbacks)
        else:
            next_hh = find_next_fractal(up_fractals, segment_start_time)
            if not next_hh:
                logs.append("➡️ Artık yeni HH (UP fraktal) yok.")
                break
            if next_hh['fractal_time'] in used_lls:
                break
            breaker = next((x for x in candles if x['open_time'] > next_hh['fractal_time'] and x['close'] > next_hh['price']), None)
            if not breaker:
                logs.append(f"❌ HH {next_hh['fractal_time']} kırılmadı, bekleniyor...")
                break

            pullbacks = find_all_pullback_fractals(
                candles, 'UP', next_hh['fractal_time'], breaker['open_time'], n=n_pullback, used_pb=used_pullbacks
            )
            induced, pb, by = is_any_pullback_induced(candles, 'UP', pullbacks, breaker['open_time'])
            if not induced:
                logs.append(f"❌ HH {next_hh['fractal_time']} kırıldı ama inducement yok, devam edilmiyor.")
                break

            segment_candles = [c for c in candles if next_hh['fractal_time'] <= c['open_time'] <= breaker['open_time']]
            kritik_seviye = min(c['low'] for c in segment_candles)

            logs.append(f"[CONT] ✅ UP devam → HH: {next_hh['fractal_time']} | Fiyat: {format_price(next_hh['price'])}")
            logs.append(f"   ↪ Kıran mum: {breaker['open_time']} | Close: {format_price(breaker['close'])}")
            for p in pullbacks:
                logs.append(f"   ↪ Pullback: {p['open_time']} | Low: {format_price(p['low'])}")
            logs.append(f"   ↪ Induced PB: {pb['open_time']} → {by['open_time']}")
            logs.append(f"   ↪ Yeni kritik seviye: {format_price(kritik_seviye)}")
            trend_log.append((breaker['open_time'], trend))
            breaker_idx = next(idx for idx, c in enumerate(candles) if c['open_time'] == breaker['open_time'])
            segment_start_idx = max(breaker_idx - n, 0)
            segment_start_time = candles[segment_start_idx]['open_time']
            used_lls.add(next_hh['fractal_time'])
            used_pullbacks.update(p['open_time'] for p in pullbacks)

        # --- CHOCH kontrolü ---
        for cc in candles:
            if cc['open_time'] > segment_start_time:
                if trend == 'DOWN' and cc['high'] > kritik_seviye:
                    logs.append(f"[CHOCH] Trend DOWN → UP @ {cc['open_time']} | Close={format_price(cc['close'])} kritik={format_price(kritik_seviye)}")
                    trend = 'UP'
                    trend_log.append((cc['open_time'], trend))
                    breaker_idx = next(idx for idx, c in enumerate(candles) if c['open_time'] == cc['open_time'])
                    segment_start_idx = max(breaker_idx - n, 0)
                    segment_start_time = candles[segment_start_idx]['open_time']
                    break
                if trend == 'UP' and cc['low'] < kritik_seviye:
                    logs.append(f"[CHOCH] Trend UP → DOWN @ {cc['open_time']} | Close={format_price(cc['close'])} kritik={format_price(kritik_seviye)}")
                    trend = 'DOWN'
                    trend_log.append((cc['open_time'], trend))
                    breaker_idx = next(idx for idx, c in enumerate(candles) if c['open_time'] == cc['open_time'])
                    segment_start_idx = max(breaker_idx - n, 0)
                    segment_start_time = candles[segment_start_idx]['open_time']
                    break

    return logs, trend_log










