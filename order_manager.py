import requests
import time
import hmac
import hashlib
import math

API_KEY = '140c09d8e2aa0d21687231aed32a272f70441b464bacc34dac132dee1bfbee69'
API_SECRET = '494f3d48e92b606da64cc4eadc0b35ac2d798b0ad3774b436e84775c449e300e'
BASE_URL = 'https://testnet.binancefuture.com'

def _sign_params(params: dict, secret: str) -> dict:
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    params['signature'] = signature
    return params

def get_quantity_precision(symbol):
    try:
        info = requests.get(BASE_URL + "/fapi/v1/exchangeInfo").json()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        step_size = float(f['stepSize'])
                        precision = abs(int(round(math.log10(step_size))))
                        return precision
    except:
        return 3

def get_price_precision(symbol):
    try:
        info = requests.get(BASE_URL + "/fapi/v1/exchangeInfo").json()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'PRICE_FILTER':
                        tick_size = float(f['tickSize'])
                        precision = abs(int(round(math.log10(tick_size))))
                        return precision
    except:
        return 2

def get_current_price(symbol):
    try:
        response = requests.get(f"{BASE_URL}/fapi/v1/ticker/price", params={"symbol": symbol})
        response.raise_for_status()
        return float(response.json()["price"])
    except:
        return None

def send_limit_order(symbol, side, quantity, price):
    quantity_precision = get_quantity_precision(symbol)
    price_precision = get_price_precision(symbol)

    quantity = round(quantity, quantity_precision)
    price = round(price, price_precision)

    params = {
        'symbol': symbol,
        'side': side,
        'type': 'LIMIT',
        'timeInForce': 'GTC',
        'quantity': quantity,
        'price': price,
        'timestamp': int(time.time() * 1000)
    }

    signed_params = _sign_params(params, API_SECRET)
    headers = {'X-MBX-APIKEY': API_KEY}

    try:
        response = requests.post(BASE_URL + '/fapi/v1/order', headers=headers, params=signed_params)
        response.raise_for_status()
        print(f"✅ Limit emir gönderildi: {response.json()}")
        return response.json()
    except Exception as e:
        print(f"⛔ Limit emir hatası: {e}")
        print(f"Yanıt: {response.text}")
        return None

def send_stop_loss_order(symbol, side, stop_price):
    stop_price = round(stop_price, get_price_precision(symbol))
    opposite_side = 'SELL' if side == 'BUY' else 'BUY'

    params = {
        'symbol': symbol,
        'side': opposite_side,
        'type': 'STOP_MARKET',
        'stopPrice': stop_price,
        'closePosition': True,
        'timestamp': int(time.time() * 1000),
        'recvWindow': 5000
    }

    signed_params = _sign_params(params, API_SECRET)
    headers = {'X-MBX-APIKEY': API_KEY}

    try:
        response = requests.post(BASE_URL + '/fapi/v1/order', headers=headers, params=signed_params)
        print(f"⛔ Yanıt: {response.text}")  # ← Bunu aktif tut!
        response.raise_for_status()
        print(f"✅ Stop-loss gönderildi: {response.json()}")
        return response.json()
    except Exception as e:
        print(f"⛔ Stop-loss hatası: {e}")
        print(f"Yanıt: {response.text}")
        return None


def send_take_profit_order(symbol, side, tp_price):
    tp_price = round(tp_price, get_price_precision(symbol))
    opposite_side = 'SELL' if side == 'BUY' else 'BUY'

    params = {
        'symbol': symbol,
        'side': opposite_side,
        'type': 'TAKE_PROFIT_MARKET',
        'stopPrice': tp_price,
        'closePosition': True,   # Kapatıcı emir
        'timestamp': int(time.time() * 1000),
        'recvWindow': 5000
    }

    signed_params = _sign_params(params, API_SECRET)
    headers = {'X-MBX-APIKEY': API_KEY}

    try:
        response = requests.post(BASE_URL + '/fapi/v1/order', headers=headers, params=signed_params)
        print(f"⛔ Yanıt: {response.text}")
        response.raise_for_status()
        print(f"✅ Take-profit gönderildi: {response.json()}")
        return response.json()
    except Exception as e:
        print(f"⛔ Take-profit hatası: {e}")
        print(f"Yanıt: {response.text}")
        return None


def get_order_status(symbol, order_id):
    params = {
        'symbol': symbol,
        'orderId': order_id,
        'timestamp': int(time.time() * 1000)
    }
    signed_params = _sign_params(params, API_SECRET)
    headers = {'X-MBX-APIKEY': API_KEY}

    try:
        response = requests.get(BASE_URL + '/fapi/v1/order', headers=headers, params=signed_params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⛔ Emir durumu alınamadı: {e}")
        return None

def cancel_opposite_orders(symbol, position_side):
    """
    Pozisyon fill olduğunda karşıt (orphan) emirleri iptal eder.
    """
    params = {
        'symbol': symbol,
        'timestamp': int(time.time() * 1000)
    }
    signed_params = _sign_params(params, API_SECRET)
    headers = {'X-MBX-APIKEY': API_KEY}

    try:
        response = requests.get(BASE_URL + '/fapi/v1/openOrders', headers=headers, params=signed_params)
        response.raise_for_status()
        orders = response.json()

        for order in orders:
            if order['positionSide'] == position_side and order['symbol'] == symbol:
                cancel_params = {
                    'symbol': symbol,
                    'orderId': order['orderId'],
                    'timestamp': int(time.time() * 1000)
                }
                signed_cancel = _sign_params(cancel_params, API_SECRET)
                cancel_response = requests.delete(BASE_URL + '/fapi/v1/order', headers=headers, params=signed_cancel)
                cancel_response.raise_for_status()
                print(f"🗑️ Orphan order iptal edildi: {order['orderId']}")

    except Exception as e:
        print(f"⛔ Orphan order iptal hatası: {e}")

def watch_order_fill_and_set_protection(symbol, side, order_id, stop_price, tp_price, quantity, timeout=1800):
    from db_manager import DatabaseManager
    import config

    db = DatabaseManager(config.db_config)
    print(f"⏳ Emir izleniyor: {order_id}")
    start_time = time.time()

    while time.time() - start_time < timeout:
        status = get_order_status(symbol, order_id)
        if status and status.get("status") == "FILLED":
            print("✅ Emir gerçekleşti, SL ve TP gönderiliyor...")

            sl_result = send_stop_loss_order(symbol, side, stop_price)
            tp_result = send_take_profit_order(symbol, quantity, tp_price)

            sl_id = sl_result.get("orderId") if sl_result else None
            tp_id = tp_result.get("orderId") if tp_result else None

            db.update_order_log_sl_tp(order_id, sl_id, tp_id)

            # 🔥 Orphan emirleri iptal et
            cancel_opposite_orders(symbol, position_side='BOTH')

            return True

        time.sleep(3)

    print("❌ Zaman aşımı. Emir gerçekleşmedi.")
    return False

def is_valid_limit_price(symbol, side, limit_price):
    current_price = get_current_price(symbol)
    if current_price is None:
        return False
    return limit_price <= current_price if side == 'BUY' else limit_price >= current_price

def is_valid_stop_price(symbol, side, stop_price):
    current_price = get_current_price(symbol)
    if current_price is None:
        return False
    return stop_price > current_price if side == 'BUY' else stop_price < current_price

def cancel_order(symbol, order_id):
    params = {
        'symbol': symbol,
        'orderId': order_id,
        'timestamp': int(time.time() * 1000)
    }
    signed_params = _sign_params(params, API_SECRET)
    headers = {'X-MBX-APIKEY': API_KEY}

    try:
        response = requests.delete(BASE_URL + '/fapi/v1/order', headers=headers, params=signed_params)
        response.raise_for_status()
        print(f"✅ Emir iptal edildi: {response.json()}")
        return response.json()
    except Exception as e:
        print(f"⛔ Emir iptal hatası: {e}")
        return None

def watch_order_fill_and_cancel_opposite(symbol, side, order_id, opposite_order_id, stop_price, tp_price, quantity, timeout=1800):
    import time
    from db_manager import DatabaseManager
    import config

    db = DatabaseManager(config.db_config)
    print(f"⏳ Emir izleniyor: {order_id}")
    start_time = time.time()

    while time.time() - start_time < timeout:
        status = get_order_status(symbol, order_id)
        if status and status.get("status") == "FILLED":
            print("✅ Emir gerçekleşti, karşıt emir iptal ediliyor...")

            if opposite_order_id:
                cancel_order(symbol, opposite_order_id)

            sl_result = send_stop_loss_order(symbol, side, stop_price)
            tp_result = send_take_profit_order(symbol, side, tp_price)

            sl_id = sl_result.get("orderId") if sl_result else None
            tp_id = tp_result.get("orderId") if tp_result else None

            # 🔐 Güncelleme veritabanına
            db.update_order_log_sl_tp(order_id, sl_id, tp_id)

            return True

        time.sleep(3)

    print("❌ Zaman aşımı. Emir gerçekleşmedi.")
    return False
