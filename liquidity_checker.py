def get_valid_liquidity_fractals(fractals, candles):
    valid_fractals = []
    for fr in fractals:
        f_time = fr['fractal_time']
        f_type = fr.get('type', fr.get('fractal_type'))
        f_price = float(fr.get('price', fr.get('fractal_price')))
        for candle in candles:
            if candle['open_time'] <= f_time:
                continue

            high = float(candle['high'])
            low = float(candle['low'])
            close = float(candle['close'])

            # DOWN fraktal için: low'un altında fitil ve üstünde kapanış varsa, likidite alınmış!
            if f_type == 'DOWN':
                if close < f_price:
                    # Zincir bozuldu: DOWN fraktal için fiyattan düşük kapanış
                    break
                if low < f_price and close > f_price:
                    valid_fractals.append({
                        'fractal_time': f_time,
                        'fractal_type': f_type,
                        'fractal_price': f_price,
                        'liquidity_time': candle['open_time'],
                        'liquidity_candle': candle
                    })
                    break

            # UP fraktal için: high'un üstünde fitil ve altında kapanış varsa, likidite alınmış!
            elif f_type == 'UP':
                if close > f_price:
                    # Zincir bozuldu: UP fraktal için fiyattan yüksek kapanış
                    break
                if high > f_price and close < f_price:
                    valid_fractals.append({
                        'fractal_time': f_time,
                        'fractal_type': f_type,
                        'fractal_price': f_price,
                        'liquidity_time': candle['open_time'],
                        'liquidity_candle': candle
                    })
                    break
    return valid_fractals
