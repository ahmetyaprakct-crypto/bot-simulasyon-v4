def calculate_trade_levels(
    side: str,
    liq_price: float,
    recent_fractal_price: float,
    balance: float,
    risk_percent: float = 0.015
):
    entry = recent_fractal_price + (liq_price - recent_fractal_price) * 0.6 if side == 'SELL' \
        else liq_price + (recent_fractal_price - liq_price) * 0.6

    stop = liq_price * 1.001 if side == 'SELL' else liq_price * 0.999

    risk_per_unit = abs(entry - stop)
    risk_usdt = balance * risk_percent
    quantity = risk_usdt / risk_per_unit

    tp = entry - 2 * risk_per_unit if side == 'SELL' else entry + 2 * risk_per_unit

    entry = round(entry, 3)
    stop = round(stop, 3)
    tp = round(tp, 3)
    quantity = round(quantity, 4)

    return {
        'entry': entry,
        'stop': stop,
        'tp': tp,
        'quantity': quantity
    }
