from typing import List, Dict, Optional, Tuple
from datetime import datetime
from db_manager import DatabaseManager


# ======================
# GENEL YARDIMCI FONKSİYONLAR
# ======================

def log_all_fractals(up_fractals, down_fractals):
    logs = ["==== TÜM UP FRAKTALLAR ===="]
    for f in sorted(up_fractals, key=lambda f: f['fractal_time']):
        logs.append(f"UP | {f['fractal_time']} | {f['price']}")
    logs.append("==== TÜM DOWN FRAKTALLAR ====")
    for f in sorted(down_fractals, key=lambda f: f['fractal_time']):
        logs.append(f"DOWN | {f['fractal_time']} | {f['price']}")
    return logs


def find_first_two_fractals(up_fractals, down_fractals):
    if not up_fractals or not down_fractals:
        return None, None
    first_up = min(up_fractals, key=lambda f: f["fractal_time"])
    first_down = min(down_fractals, key=lambda f: f["fractal_time"])
    return first_up, first_down


def find_first_fractal_break(candles, up_fractal, down_fractal):
    for i in range(len(candles)):
        c = candles[i]
        if c["open_time"] > up_fractal["fractal_time"] and c["close"] > up_fractal["price"]:
            return "UP", up_fractal, c, i
        if c["open_time"] > down_fractal["fractal_time"] and c["close"] < down_fractal["price"]:
            return "DOWN", down_fractal, c, i
    return None, None, None, None


def find_all_pullback_fractals(candles: List[Dict], trend: str, start_time, end_time, n=2) -> List[Dict]:
    segment = [c for c in candles if start_time <= c["open_time"] < end_time]
    pullbacks = []
    for i in range(n, len(segment) - n):
        if trend == "UP":
            is_lowest = all(segment[i]["low"] < segment[j]["low"] for j in range(i - n, i + n + 1) if j != i)
            if is_lowest:
                pullbacks.append(segment[i])
        elif trend == "DOWN":
            is_highest = all(segment[i]["high"] > segment[j]["high"] for j in range(i - n, i + n + 1) if j != i)
            if is_highest:
                pullbacks.append(segment[i])
    return pullbacks


def is_any_pullback_induced(candles: List[Dict], trend: str, pullbacks: List[Dict], end_time):
    for pb in pullbacks:
        segment = [c for c in candles if pb["open_time"] < c["open_time"] <= end_time]
        for c in segment:
            if trend == "UP" and c["low"] < pb["low"]:
                return True, pb, c
            elif trend == "DOWN" and c["high"] > pb["high"]:
                return True, pb, c
    return False, None, None


def format_price(val):
    try:
        return f"{float(val):.1f}"
    except Exception:
        return str(val)


# ======================
# STABLE STARTUP
# ======================

def startup_find_valid_bos(candles, up_fractals, down_fractals, n_pullback=2):
    logs = []
    curr_up, curr_down = find_first_two_fractals(up_fractals, down_fractals)
    if not curr_up or not curr_down:
        logs.append("Başlangıç için yeterli fraktal yok!")
        return logs, None, None, None

    used_pullbacks = set()

    for _ in range(100):
        trend, break_fractal, break_candle, _ = find_first_fractal_break(candles, curr_up, curr_down)
        if not trend:
            logs.append("Henüz fraktal kırılımı olmadı.")
            break

        pullbacks = find_all_pullback_fractals(
            candles, trend, break_fractal["fractal_time"], break_candle["open_time"],
            n=n_pullback
        )
        induced, pb, by = is_any_pullback_induced(candles, trend, pullbacks, break_candle["open_time"])

        if not induced:
            logs.append(f"❌ INVALID BOS — {trend} kırılımı: {break_fractal['fractal_time']} → {break_candle['open_time']}")
            # ❗ burada güncelleme yapılmazsa aynı fraktal tekrar denenir
            if trend == "UP":
                next_up = next((f for f in up_fractals if f["fractal_time"] > curr_up["fractal_time"]), None)
                if not next_up:
                    break
                curr_up = next_up
            else:
                next_down = next((f for f in down_fractals if f["fractal_time"] > curr_down["fractal_time"]), None)
                if not next_down:
                    break
                curr_down = next_down
            continue

        logs.append(f"✅ VALID BOS — Trend: {trend}")
        logs.append(f"   ↪ Kırılan fraktal: {break_fractal['fractal_time']} | Fiyat: {break_fractal['price']}")
        logs.append(f"   ↪ Kıran mum: {break_candle['open_time']} | Close: {break_candle['close']}")
        return logs, trend, break_candle["open_time"], break_fractal

    logs.append("Valid BoS bulunamadı.")
    return logs, None, None, None

# ======================
# UP TREND ZİNCİRİ
# ======================

def run_market_structure_uptrend(candles, up_fractals, down_fractals, kritik_baslangic, start_time, n_pullback=2, trend_log=None):
    logs = []
    trend = "UP"
    kritik = kritik_baslangic
    aktif_fraktal = None
    bos_state = "WAIT_BREAK"

    for c in candles:
        # kıran mum dahil edilmezse bazı HH'ler kaçıyor, bu yüzden sadece 'küçük' kıyas
        if c["open_time"] < start_time:
            continue

        # CHOCH bağımsız kontrol
        if c["close"] < kritik:
            # --- CHOCH sonrası kritik seviye yeniden hesapla ---
            segment = [x for x in candles if start_time <= x["open_time"] <= c["open_time"]]
            if segment:
                kritik = max(x["high"] for x in segment)  # UP→DOWN geçişinde en yüksek high
                kritik_zaman = next(x["open_time"] for x in segment if x["high"] == kritik)
            else:
                kritik_zaman = c["open_time"]

            logs.append(f"[CHOCH] Trend UP → DOWN @ {c['open_time']} | Yeni kritik: {format_price(kritik)} | Zaman: {kritik_zaman}")
            logs.append("[RESTART] CHOCH sonrası yeni DOWN zinciri başlatılıyor.")
            trend = "DOWN"

            if trend_log is not None:
                trend_log.append((c["open_time"], trend))

            # --- Debounce + doğru argümanlar ---
            next_down_fractals = [f for f in down_fractals if f["fractal_time"] > c["open_time"]]
            next_start_time = c["open_time"]

            sub_logs, kritik, trend = run_market_structure_downtrend(
                candles,
                down_fractals,
                up_fractals,
                kritik,           # kritik_baslangic
                next_start_time,  # start_time
                n_pullback=n_pullback
            )
            logs.extend(sub_logs)
            break

        # === 2️⃣ Yeni HH arama ===
        if bos_state == "WAIT_BREAK":
            # 1️⃣ Önce kıran mumda fraktal var mı kontrol et
            aktif_fraktal = next((f for f in up_fractals if f["fractal_time"] == start_time), None)

            # 2️⃣ Eğer yoksa, sıradaki fraktalı al
            if not aktif_fraktal:
                aktif_fraktal = next((f for f in up_fractals if f["fractal_time"] > start_time), None)

            # 3️⃣ HH adayı bulunduysa logla
            if aktif_fraktal:
                logs.append(f"[CONT] 🔍 Yeni HH adayı @ {aktif_fraktal['fractal_time']} | Fiyat: {aktif_fraktal['price']}")
                bos_state = "WAIT_VALIDATION"
                continue

        # 3️⃣ HH kırılımı kontrolü
        if bos_state == "WAIT_VALIDATION" and aktif_fraktal:
            if c["close"] > float(aktif_fraktal["price"]):
                segment = [x for x in candles if aktif_fraktal["fractal_time"] <= x["open_time"] <= c["open_time"]]
                pullbacks = find_all_pullback_fractals(candles, "UP", aktif_fraktal["fractal_time"], c["open_time"], n=n_pullback)
                induced, pb, by = is_any_pullback_induced(candles, "UP", pullbacks, c["open_time"])

                if induced:
                    kritik = min(x["low"] for x in segment)
                    kritik_zaman = next(x["open_time"] for x in segment if x["low"] == kritik)
                    logs.append(f"[CONT] ✅ VALID UP BoS: {aktif_fraktal['fractal_time']} → {c['open_time']}")
                    logs.append(f"   ↪ Yeni kritik seviye: {format_price(kritik)} | Zaman: {kritik_zaman}")
                else:
                    logs.append(f"[CONT] ❌ INVALID UP BoS: {aktif_fraktal['fractal_time']} → {c['open_time']}")
                    logs.append(f"   ↪ Kritik seviye aynı kaldı: {format_price(kritik)}")

                start_time = c["open_time"]
                bos_state = "WAIT_BREAK"
                aktif_fraktal = None

    return logs, kritik, trend

# ======================
# DOWN TREND ZİNCİRİ
# ======================

def run_market_structure_downtrend(candles, down_fractals, up_fractals, kritik_baslangic, start_time, n_pullback=2, trend_log=None):
    logs = []
    trend = "DOWN"
    kritik = kritik_baslangic
    aktif_fraktal = None
    bos_state = "WAIT_BREAK"

    for c in candles:
        if c["open_time"] <= start_time:
            continue

        # CHOCH bağımsız kontrol
        if c["close"] > kritik:
            # --- CHOCH sonrası kritik seviye yeniden hesapla ---
            segment = [x for x in candles if start_time <= x["open_time"] <= c["open_time"]]
            if segment:
                kritik = min(x["low"] for x in segment)  # DOWN→UP geçişinde en düşük low
                kritik_zaman = next(x["open_time"] for x in segment if x["low"] == kritik)
            else:
                kritik_zaman = c["open_time"]

            logs.append(f"[CHOCH] Trend DOWN → UP @ {c['open_time']} | Yeni kritik: {format_price(kritik)} | Zaman: {kritik_zaman}")
            logs.append("[RESTART] CHOCH sonrası yeni UP zinciri başlatılıyor.")
            trend = "UP"
            
            if trend_log is not None:
                trend_log.append((c["open_time"], trend))

            # --- Debounce + doğru argümanlar ---
            next_up_fractals = [f for f in up_fractals if f["fractal_time"] > c["open_time"]]
            next_start_time = c["open_time"]

            sub_logs, kritik, trend = run_market_structure_uptrend(
                candles,
                up_fractals,
                down_fractals,
                kritik,           # kritik_baslangic
                next_start_time,  # start_time
                n_pullback=n_pullback
            )
            logs.extend(sub_logs)
            break

        # === 2️⃣ Yeni LL arama ===
        if bos_state == "WAIT_BREAK":
            # 1️⃣ Önce kıran mumda fraktal var mı kontrol et
            aktif_fraktal = next((f for f in down_fractals if f["fractal_time"] == start_time), None)

            # 2️⃣ Yoksa sıradaki fraktalı al
            if not aktif_fraktal:
                aktif_fraktal = next((f for f in down_fractals if f["fractal_time"] > start_time), None)

            if aktif_fraktal:
                logs.append(f"[CONT] 🔍 Yeni LL adayı @ {aktif_fraktal['fractal_time']} | Fiyat: {aktif_fraktal['price']}")
                bos_state = "WAIT_VALIDATION"
                continue

        # === 3️⃣ LL kırılımı kontrolü ===
        if bos_state == "WAIT_VALIDATION" and aktif_fraktal:
            if c["close"] < float(aktif_fraktal["price"]):
                segment = [x for x in candles if aktif_fraktal["fractal_time"] <= x["open_time"] <= c["open_time"]]
                pullbacks = find_all_pullback_fractals(candles, "DOWN", aktif_fraktal["fractal_time"], c["open_time"], n=n_pullback)
                induced, pb, by = is_any_pullback_induced(candles, "DOWN", pullbacks, c["open_time"])

                if induced:
                    kritik = max(x["high"] for x in segment)
                    kritik_zaman = next(x["open_time"] for x in segment if x["high"] == kritik)
                    logs.append(f"[CONT] ✅ VALID DOWN BoS: {aktif_fraktal['fractal_time']} → {c['open_time']}")
                    logs.append(f"   ↪ Yeni kritik seviye: {format_price(kritik)} | Zaman: {kritik_zaman}")
                else:
                    logs.append(f"[CONT] ❌ INVALID DOWN BoS: {aktif_fraktal['fractal_time']} → {c['open_time']}")
                    logs.append(f"   ↪ Kritik seviye aynı kaldı: {format_price(kritik)}")

                start_time = c["open_time"]
                bos_state = "WAIT_BREAK"
                aktif_fraktal = None

    logs.append(f"[SON] DOWN trend zinciri tamamlandı. Kritik: {format_price(kritik)}")
    return logs, kritik, trend

# ======================
# ANA FONKSİYON
# ======================

def run_full_market_structure_chain(candles, up_fractals, down_fractals, n_pullback=2):
    logs, trend_log = [], []

    startup_logs, trend, break_time, break_fractal = startup_find_valid_bos(
        candles, up_fractals, down_fractals, n_pullback=n_pullback
    )
    logs.extend(startup_logs if isinstance(startup_logs, list) else [startup_logs])

    if not trend or not break_time:
        logs.append("Startup başarısız, zincir başlatılamadı.")
        return logs, trend_log

    # Kritik seviye hesapla
    segment = [c for c in candles if break_fractal["fractal_time"] <= c["open_time"] <= break_time]
    kritik = min(c["low"] for c in segment) if trend == "UP" else max(c["high"] for c in segment)

    logs.append(f"[INIT] Trend {trend} başlatıldı. Kritik seviye: {format_price(kritik)}")
    trend_log.append((break_time, trend))

    # UP trend zinciri
    if trend == "UP":
        chain_logs, kritik, trend = run_market_structure_uptrend(
            candles, up_fractals, down_fractals, kritik, break_time, n_pullback=n_pullback, trend_log=trend_log
        )
        logs.extend(chain_logs)

    # DOWN trend zinciri
    elif trend == "DOWN":
        chain_logs, kritik, trend = run_market_structure_downtrend(
            candles, down_fractals, up_fractals, kritik, break_time, n_pullback=n_pullback, trend_log=trend_log
        )
        logs.extend(chain_logs)

    # Logları tamamen düzleştir
    flat_logs = []
    for l in logs:
        if isinstance(l, list):
            flat_logs.extend(l)
        else:
            flat_logs.append(l)

    logs.append(f"[SON] Trend zinciri tamamlandı ({trend}).")
    return flat_logs, trend_log
