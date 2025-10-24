from liquidity_checker import get_valid_liquidity_fractals
import csv
import pandas as pd
trade_logs = []  # Tüm işlemler buraya eklenecek

def float_str(val, d=2):
    try:
        return f"{float(val):.{d}f}"
    except:
        return ""

def log_trade_to_txt(trade_data, path="trade_log_debug.txt"):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"ENTRY TIME: {trade_data.get('entry_time')}\n")
        f.write(f"YÖN: {trade_data.get('direction')}\n")
        f.write(f"SONUÇ: {trade_data.get('outcome')}\n")
        f.write(f"ENTRY FİYAT: {trade_data.get('entry_price')}\n")
        f.write(f"STOP FİYAT: {trade_data.get('sl_price')}\n")
        f.write(f"TP FİYAT: {trade_data.get('tp_price')}\n")
        f.write("\n-- Likidite Alan Mum --\n")
        f.write(str(trade_data.get('liq_candle', 'YOK')) + "\n")
        f.write("-- Likidite Alınan Mum --\n")
        f.write(str(trade_data.get('liq_fractal', 'YOK')) + "\n")
        f.write("-- Breakout (Kırılım) Mum --\n")
        f.write(str(trade_data.get('breakout_candle', 'YOK')) + "\n")
        f.write("------\n\n")

def find_prior_opposite_fractal(fractals, liq_time, liq_type):
    opposite_type = 'DOWN' if liq_type == 'UP' else 'UP'
    candidates = [f for f in fractals if f.get('fractal_type', f.get('type')) == opposite_type and f['fractal_time'] < liq_time]
    if candidates:
        return max(candidates, key=lambda x: x['fractal_time'])
    return None

def find_breakout_candle(candles, after_time, trigger_price, direction):
    for c in candles:
        if c['open_time'] <= after_time:
            continue
        if direction == 'UP' and c['close'] > trigger_price:
            return c
        if direction == 'DOWN' and c['close'] < trigger_price:
            return c
    return None

def find_next_opposite_fractal(fractals, after_time, f_type):
    opposite_type = 'DOWN' if f_type == 'UP' else 'UP'
    future = [f for f in fractals if f.get('fractal_type', f.get('type')) == opposite_type and f['fractal_time'] > after_time]
    if future:
        return min(future, key=lambda x: x['fractal_time'])
    return None

def is_range_market_by_price(candles, liq_candle_time, window=36):
    idx = None
    for i, c in enumerate(candles):
        if str(c['open_time']) == str(liq_candle_time):
            idx = i
            break
    if idx is None or idx < window:
        return False, None, None
    prev_candles = candles[idx-window:idx]
    range_high = max(c['high'] for c in prev_candles)
    range_low  = min(c['low'] for c in prev_candles)
    return True, range_high, range_low

# --- YARDIMCI: Ters tarafta likidite alındıysa cancelled statusunu işle ---
def mark_cancelled_trades(detailed_logs, valid_fractals, candles):
    # print("\n--- Cancelled Kontrolü Başladı (High/Low Kırılımı) ---")
    cancelled_count = 0
    opposite_liq_but_completed = 0

    for log in detailed_logs:
        # Sadece entry_time gelmiş ve status completed olanları kontrol et!
        if log.get("status", "completed") == "completed" and log.get("entry_time"):
            entry_time = log["entry_time"]
            liq_fractal_time = log.get("liq_fractal_time")
            liq_candle_time = log.get("liq_candle_time")
            f_type = log.get("liq_fractal_type")
            direction = log.get("direction")

            if not (entry_time and liq_fractal_time and f_type and direction):
                # print(f"[SKIP] Eksik data: {log}")
                continue

            opposite_type = "DOWN" if f_type == "UP" else "UP"
            found_cancel = False
            found_opposite_liq = False
            breakout_fractal_time = log.get("breakout_fractal_time")

            for vf in valid_fractals:
                # 1) Karşı tarafta, entry_time öncesi, arada likidite alınmış mı?
                if (
                    vf.get('fractal_type') == opposite_type and
                    vf.get('fractal_time') != breakout_fractal_time and
                    str(vf.get('fractal_time')) < str(liq_candle_time) and
                    str(vf.get('liquidity_time')) > str(liq_candle_time) and
                    str(vf.get('liquidity_time')) <= str(entry_time)
                ):
                    found_opposite_liq = True
                    liq_mum_time = vf.get("liquidity_time")
                    liq_mum = next((c for c in candles if str(c['open_time']) == str(liq_mum_time)), None)
                    if not liq_mum:
                        continue

                    liq_high = float(liq_mum["high"])
                    liq_low = float(liq_mum["low"])

                    cancelled = False
                    for c in candles:
                        if str(c['open_time']) <= str(liq_mum_time) or str(c['open_time']) > str(entry_time):
                            continue
                        close_price = float(c["close"])

                        # DOWN işlemler için
                        if direction == "DOWN":
                            if close_price > liq_high:
                                cancelled = True
                                break
                            elif close_price < liq_low:
                                break
                        # UP işlemler için
                        elif direction == "UP":
                            if close_price < liq_low:
                                cancelled = True
                                break
                            elif close_price > liq_high:
                                break

                    if cancelled:
                        log["status"] = "cancelled"
                        log["cancelled_liq_time"] = vf.get("liquidity_time")
                        log["cancelled_fractal_time"] = vf.get("fractal_time")
                        found_cancel = True
                        cancelled_count += 1
                        # print(f"[CANCELLED] {log['liq_fractal_time']} → {entry_time} | Karşı fraktal: {vf.get('fractal_time')} lik: {vf.get('liquidity_time')}")
                        break
                    else:
                        # print(f"[OPPOSITE-LIQ-NOT-CANCELLED] {log['liq_fractal_time']} → {entry_time} | Karşı fraktal: {vf.get('fractal_time')} lik: {vf.get('liquidity_time')}")
                        opposite_liq_but_completed += 1

            if not found_cancel and not found_opposite_liq:
                # print(f"[COMPLETED] {log['liq_fractal_time']} → {entry_time}")
                pass

    # print(f"Toplam CANCELLED: {cancelled_count}")
    # print(f"Toplam Karşı likiditeyle tamamlanan işlem: {opposite_liq_but_completed}")
    return detailed_logs

def run_confirmation_chain(
    fractals, candles, trade_logs, symbol=None, n=None, rr=None, fibo_level=None,
    stop_value=None, log_func=None, detailed_logs=None
):
    from backtest import add_atr_threshold_to_candles
    add_atr_threshold_to_candles(candles, atr_period=14, threshold_window=300)
    if detailed_logs is None:
        detailed_logs = []
    total_trades = 0
    tp_count = 0
    sl_count = 0

    # FİLTRE PARAMETRELERİ
    MAX_LIQUIDITY_LAG_BARS = 50
    ENTRY_TIMEOUT_BARS = 50

    def log(*args):
        msg = " ".join(str(a) for a in args)
        if log_func:
            log_func(msg)
        else:
            print(msg)

    valid_fractals = get_valid_liquidity_fractals(fractals, candles)
    # --- Aynı likidite alan mumda birden fazla fraktal varsa, sadece en yenisini al ---
    filtered_valid_fractals = []
    last_liq_time = None
    group = []
    valid_fractals_sorted = sorted(valid_fractals, key=lambda x: (x['liquidity_time'], x['fractal_time']))
    for vf in valid_fractals_sorted:
        liq_time = vf['liquidity_time']
        if liq_time != last_liq_time and group:
            filtered_valid_fractals.append(max(group, key=lambda x: x['fractal_time']))
            group = []
        group.append(vf)
        last_liq_time = liq_time
    if group:
        filtered_valid_fractals.append(max(group, key=lambda x: x['fractal_time']))

    valid_fractals = filtered_valid_fractals

    pending_fractal = None  # Son alınan ve henüz breakout gelmemiş fraktal

    for vf in valid_fractals:
        pending_fractal = vf

        # FİLTRE 1: Likidite gecikme kontrolü
        liq_fractal_idx = next((i for i, c in enumerate(candles) if c['open_time'] == vf['fractal_time']), None)
        liq_candle_time = vf.get('liquidity_time')
        if liq_fractal_idx is not None and liq_candle_time:
            try:
                liq_candle_idx = next(i for i, c in enumerate(candles) if c['open_time'] == liq_candle_time)
            except StopIteration:
                liq_candle_idx = None
            if liq_candle_idx is not None and liq_candle_idx - liq_fractal_idx > MAX_LIQUIDITY_LAG_BARS:
                log(f"⏩ [LONG LIQUIDITY LAG] {vf['fractal_time']} - Likidite alımı uzun sürdü ({liq_candle_idx - liq_fractal_idx} bar). Trade atlandı.")
                pending_fractal = None
                continue

        # --- is_in_range ekleniyor ---
        range_flag = False
        range_high, range_low = None, None
        is_in_range = None
        # liq_candle_idx = likidite ALAN mumun index'i!
        liq_candle_idx = next((i for i, c in enumerate(candles) if c['open_time'] == vf['liquidity_time']), None)
        window = 36
        if liq_candle_idx is not None and liq_candle_idx >= window:
            prev_candles = candles[liq_candle_idx-window:liq_candle_idx]
            range_high = max(c['high'] for c in prev_candles)
            range_low = min(c['low'] for c in prev_candles)
            range_flag = True
            liq_candle = candles[liq_candle_idx]
            liq_candle_price = liq_candle['low'] if liq_candle else None
            if range_low is not None and range_high is not None and liq_candle_price is not None:
                is_in_range = range_low <= liq_candle_price <= range_high


        if pending_fractal is None:
            continue

        f_type = pending_fractal.get('fractal_type', pending_fractal.get('type'))
        prior_opposite = find_prior_opposite_fractal(fractals, pending_fractal['liquidity_time'], f_type)
        if not prior_opposite:
            log(f"❌ Ters fraktal yok | {f_type} @ {pending_fractal['fractal_time']}")
            pending_fractal = None
            continue

        direction = 'UP' if f_type == 'DOWN' else 'DOWN'
        trigger_price = prior_opposite.get('fractal_price', prior_opposite.get('price'))

        sequence_broken = False
        liq_time = pending_fractal['liquidity_time']
        liq_price = pending_fractal['fractal_price']
        found_breakout = False
        breakout_candle = None
        for c in candles:
            if c['open_time'] <= liq_time:
                continue
            if direction == 'UP' and c['close'] > trigger_price:
                found_breakout = True
                breakout_candle = c
                break
            if direction == 'DOWN' and c['close'] < trigger_price:
                found_breakout = True
                breakout_candle = c
                break
            if f_type == 'DOWN' and c['close'] < liq_price:
                sequence_broken = True
                broken_time = c['open_time']
                break
            if f_type == 'UP' and c['close'] > liq_price:
                sequence_broken = True
                broken_time = c['open_time']
                break

        if sequence_broken:
            log(f"⛔ Zincir iptal: Kural dışı kapanış! {liq_time} sonrası {broken_time} mumu zinciri bozdu.")
            pending_fractal = None
            continue

        if not found_breakout:
            continue

        breakout_time = breakout_candle['open_time']
        breakout_idx = next(i for i, c in enumerate(candles) if c['open_time'] == breakout_time)
        if breakout_idx >= 2:
            time_2_before = candles[breakout_idx - 2]['open_time']
        else:
            time_2_before = candles[0]['open_time']
        next_opposite_fractal = find_next_opposite_fractal(
            fractals, time_2_before, f_type)
        if not next_opposite_fractal:
            log(f"❌ Breakout sonrası yeni ters fraktal yok (fibo çekilemedi) | {pending_fractal['fractal_time']}")
            pending_fractal = None
            continue

        liq_candle = pending_fractal['liquidity_candle']
        fibo_end = float(next_opposite_fractal.get('fractal_price', next_opposite_fractal.get('price')))
        fibo_high = max(float(liq_candle['high']), fibo_end)
        fibo_low  = min(float(liq_candle['low']), fibo_end)
        fib_range = fibo_high - fibo_low

        if direction == 'UP':
            entry = fibo_high - fib_range * fibo_level
            sl    = fibo_high - fib_range * stop_value
            tp    = entry - rr * (sl - entry)
        else:
            entry = fibo_low + fib_range * fibo_level
            sl    = fibo_low + fib_range * stop_value
            tp    = entry - rr * (sl - entry)

        fib_ready_time = next_opposite_fractal['fractal_time']
        candles_for_entry = [c for c in candles if c['open_time'] >= fib_ready_time][:ENTRY_TIMEOUT_BARS]

        trade_outcome = None
        entry_found = False
        outcome_candle = None
        entry_filled_time = None
        entry_filled_candle = None

        for c in candles_for_entry:
            if not entry_found:
                if c['low'] <= entry <= c['high']:
                    entry_found = True
                    entry_filled_time = c['open_time']
                    entry_filled_candle = c
                    log(
                        f"Entry fill edilen mum: {entry_filled_time} | open={c['open']} high={c['high']} "
                        f"low={c['low']} close={c['close']}"
                    )
                    if direction == 'UP':
                        tp_hit = c['high'] >= tp
                        sl_hit = c['low'] <= sl
                        if tp_hit and sl_hit:
                            o = c['open']
                            if o <= sl:
                                trade_outcome = 'SL'
                            elif o >= tp:
                                trade_outcome = 'TP'
                            else:
                                trade_outcome = 'SL' if (o - sl) < (tp - o) else 'TP'
                            break
                        elif sl_hit:
                            trade_outcome = 'SL'
                            break
                        elif tp_hit:
                            trade_outcome = 'TP'
                            break
                    else:
                        tp_hit = c['low'] <= tp
                        sl_hit = c['high'] >= sl
                        if tp_hit and sl_hit:
                            o = c['open']
                            if o >= sl:
                                trade_outcome = 'SL'
                            elif o <= tp:
                                trade_outcome = 'TP'
                            else:
                                trade_outcome = 'SL' if (sl - o) < (o - tp) else 'TP'
                            break
                        elif sl_hit:
                            trade_outcome = 'SL'
                            break
                        elif tp_hit:
                            trade_outcome = 'TP'
                            break
            elif entry_found:
                if direction == 'UP':
                    if c['low'] <= sl:
                        trade_outcome = 'SL'
                        break
                    if c['high'] >= tp:
                        trade_outcome = 'TP'
                        break
                else:
                    if c['high'] >= sl:
                        trade_outcome = 'SL'
                        break
                    if c['low'] <= tp:
                        trade_outcome = 'TP'
                        break

        # KORUYUCU PATCH: Entry fill olmasına rağmen outcome boşsa, default outcome ata
        if entry_found and not trade_outcome:
            trade_outcome = 'SL'

        if not entry_found:
            log(f"🚫 Entry {ENTRY_TIMEOUT_BARS} bar içinde gelmedi, işlem alınmadı.")
            pending_fractal = None
            continue

        if entry_found:
            total_trades += 1
            if trade_outcome == 'TP':
                tp_count += 1
            elif trade_outcome == 'SL':
                sl_count += 1

            detailed_logs.append({
                "symbol": symbol,
                "direction": direction,
                "outcome": trade_outcome,
                "status": "completed",
                "liq_fractal_time": pending_fractal.get("fractal_time", "") if pending_fractal else "",
                "liq_candle_time": liq_candle.get("open_time", "") if liq_candle else "",
                "is_range": is_in_range,
                "range_high": range_high if range_flag else "",
                "range_low": range_low if range_flag else "",
                "entry_time": entry_filled_time,
                "entry_price": float_str(entry),
                "sl_price": float_str(sl),
                "tp_price": float_str(tp),
                "liq_candle_open": float_str(liq_candle.get("open", "")) if liq_candle else "",
                "liq_candle_high": float_str(liq_candle.get("high", "")) if liq_candle else "",
                "liq_candle_low": float_str(liq_candle.get("low", "")) if liq_candle else "",
                "liq_candle_close": float_str(liq_candle.get("close", "")) if liq_candle else "",
                "liq_fractal_price": float_str(pending_fractal.get("fractal_price", "")) if pending_fractal else "",
                "liq_fractal_type": pending_fractal.get("fractal_type", "") if pending_fractal else "",
                "breakout_fractal_time": prior_opposite.get('fractal_time', "") if prior_opposite else "",
                "breakout_time": breakout_candle.get("open_time", "") if breakout_candle else "",
                "breakout_open": float_str(breakout_candle.get("open", "")) if breakout_candle else "",
                "breakout_high": float_str(breakout_candle.get("high", "")) if breakout_candle else "",
                "breakout_low": float_str(breakout_candle.get("low", "")) if breakout_candle else "",
                "breakout_close": float_str(breakout_candle.get("close", "")) if breakout_candle else "",
                "volume": entry_filled_candle.get('volume') if entry_filled_candle else "",
                "entry_atr": float_str(entry_filled_candle.get('ATR14', "")) if entry_filled_candle else "",
                "ATR_eşiği": float_str(entry_filled_candle.get('ATR_eşiği', "")) if entry_filled_candle else "",
                "entry_volume": entry_filled_candle.get('volume', None) if entry_filled_candle else "",
                "breakout_volume": breakout_candle.get('volume', None) if breakout_candle else "",
            })

        pending_fractal = None  # Breakout geldiyse sıfırla!

    log(f"\n🔔 Toplam işlem: {total_trades} | TP: {tp_count} | SL: {sl_count}")
    return {"total": total_trades, "tp": tp_count, "sl": sl_count}

# --- KULLANIM ÖRNEĞİ ---
# 1. detailed_logs = []
# 2. run_confirmation_chain(..., detailed_logs=detailed_logs)
# 3. valid_fractals = get_valid_liquidity_fractals(fractals, candles)
# 4. detailed_logs = mark_cancelled_trades(detailed_logs, valid_fractals)

