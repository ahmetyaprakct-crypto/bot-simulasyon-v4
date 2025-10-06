def find_all_inducements_with_fractal(candles, trend='UP', n=2):
    inducement_list = []
    candidate_indexes = []
    for i in range(1, len(candles)):
        if trend == 'UP':
            if candles[i]['low'] < candles[i-1]['low']:
                candidate_indexes.append(i)
        elif trend == 'DOWN':
            if candles[i]['high'] > candles[i-1]['high']:
                candidate_indexes.append(i)
    for idx in candidate_indexes:
        left = max(0, idx-n)
        right = min(len(candles), idx+n+1)
        segment = candles[left:right]
        if trend == 'UP':
            lows = [c['low'] for c in segment]
            if candles[idx]['low'] == min(lows):
                inducement_list.append(candles[idx])
        elif trend == 'DOWN':
            highs = [c['high'] for c in segment]
            if candles[idx]['high'] == max(highs):
                inducement_list.append(candles[idx])
    return inducement_list
