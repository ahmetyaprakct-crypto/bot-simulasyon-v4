import requests
import time
from order_manager import (
    send_limit_order,
    send_stop_loss_order,
    send_take_profit_order,
    watch_order_fill_and_cancel_opposite,
    is_valid_limit_price,
    get_current_price,
    is_valid_stop_price
)
from trade_utils import calculate_trade_levels
from db_manager import DatabaseManager
import config

db = DatabaseManager(config.db_config)

# Telegram bilgileri
TELEGRAM_TOKEN = '7574433733:AAFrGjUy7s7m5sK-XN8tzD_navH6K8orAU0'
TELEGRAM_CHAT_ID = '6092650959'

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
    except Exception as e:
        print(f"Telegram mesajı gönderilemedi: {e}")

def check_and_send_alarms(db):
    with db.conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, fractal_time, fractal_type, fractal_price,
                   confirm_time, confirm_price
            FROM confirmed_liquidity
            WHERE confirmed = TRUE AND status = 'confirmed' AND notified = FALSE;
        """)
        rows = cur.fetchall()

        for row in rows:
            id, symbol, f_time, f_type, f_price, c_time, c_price = row

            try:
                cur.execute("UPDATE confirmed_liquidity SET notified = TRUE WHERE id = %s;", (id,))
                db.conn.commit()

                side = 'BUY' if f_type == 'DOWN' else 'SELL'
                liq_price = float(f_price)

                # Onay sonrası ilk ters fraktal
                opposite_type = 'DOWN' if side == 'SELL' else 'UP'
                with db.conn.cursor() as cur2:
                    cur2.execute("""
                        SELECT price FROM fractals
                        WHERE symbol = %s AND fractal_time > %s AND fractal_type = %s
                        ORDER BY fractal_time ASC LIMIT 1;
                    """, (symbol, c_time, opposite_type))
                    r = cur2.fetchone()

                recent_fractal_price = float(r[0]) if r else liq_price

                levels = calculate_trade_levels(
                    side=side,
                    liq_price=liq_price,
                    recent_fractal_price=recent_fractal_price,
                    balance=1000
                )

                entry = levels['entry']
                stop = levels['stop']
                tp = levels['tp']
                qty = levels['quantity']

                message = (
                    f"🚨 [ALARM] {symbol} - {side}\n"
                    f"Giriş: {entry}\nStop: {stop}\nTP: {tp}\n"
                    f"Miktar: {qty}"
                )
                send_telegram_message(message)

                sl_order = send_stop_loss_order(symbol, side, stop)
                tp_order = send_take_profit_order(symbol, tp)
                sl_order_id = sl_order.get("orderId") if sl_order else None
                tp_order_id = tp_order.get("orderId") if tp_order else None

                if is_valid_limit_price(symbol, side, entry):
                    entry_order = send_limit_order(symbol, side, qty, entry)
                    if entry_order:
                        entry_order_id = entry_order["orderId"]

                        db.insert_order_log(
                            alarm_id=id,
                            symbol=symbol,
                            side=side,
                            entry=entry,
                            stop=stop,
                            tp=tp,
                            qty=qty,
                            entry_order_id=entry_order_id,
                            sl_order_id=sl_order_id,
                            tp_order_id=tp_order_id
                        )

                        watch_order_fill_and_cancel_opposite(
                            symbol=symbol,
                            order_id=entry_order_id,
                            sl_order_id=sl_order_id,
                            tp_order_id=tp_order_id
                        )
                else:
                    print(f"⚠️ Limit fiyat uygunsuz: {entry}")

            except Exception as e:
                print(f"⚠️ Alarm işleminde hata: {e}")


def retry_failed_protections(db):
    with db.conn.cursor() as cur:
        cur.execute("""
                SELECT id, symbol, side, stop_price, tp_price, quantity
                FROM order_log
                WHERE (sl_sent = FALSE OR tp_sent = FALSE) AND entry_order_id IS NOT NULL;
            """)

        rows = cur.fetchall()

        for row in rows:
            id, symbol, side, stop, tp, qty = row
            print(f"\n🔁 Koruma emirleri yeniden deneniyor [#{id} - {symbol}]")

            sl_result = None
            tp_result = None

            if not stop or not tp:
                print("⚠️ Stop veya TP bilgisi eksik, atlanıyor.")
                continue

            sl_sent = True
            tp_sent = True

            if not is_valid_stop_price(symbol, side, stop):
                print(f"⛔ Geçersiz SL fiyatı: {stop}")
                sl_sent = False
            else:
                sl_result = send_stop_loss_order(symbol, side, stop)
                sl_sent = sl_result is not None

            if not is_valid_stop_price(symbol, side, tp):
                print(f"⛔ Geçersiz TP fiyatı: {tp}")
                tp_sent = False
            else:
                tp_result = send_take_profit_order(symbol, tp)
                tp_sent = tp_result is not None

            sl_id = sl_result.get("orderId") if sl_result else None
            tp_id = tp_result.get("orderId") if tp_result else None

            db.update_order_log_sl_tp(
                entry_order_id=id,
                sl_order_id=sl_id,
                tp_order_id=tp_id,
                sl_sent=sl_sent,
                tp_sent=tp_sent
            )

# 🔁 Sürekli kontrol eden döngü
if __name__ == "__main__":
   # print("🔁 Alarm sistemi aktif. Onaylı likiditeler izleniyor...")
    #while True:
     #   check_and_send_alarms(db)
      #  time.sleep(30)  # Her 30 saniyede bir kontrol
       # retry_failed_protections(db)
    
    from order_manager import send_limit_order, send_stop_loss_order, send_take_profit_order, get_current_price

    # Örnek sembol ve miktar
    symbol = "ETHUSDT"
    side = "BUY"
    qty = 0.01

    # Mevcut fiyatı al
    current_price = get_current_price(symbol)

    # Giriş, stop ve tp fiyatları
    entry = round(current_price - 100, 1)  # 100 USD aşağıdan giriş
    stop = round(entry - 400, 1)           # 400 USD aşağıdan stop
    tp = round(entry + 500, 1)             # 500 USD yukarıdan tp

    print(f"Entry: {entry}, Stop: {stop}, TP: {tp}")

    # Limit emri
    order = send_limit_order(symbol, side, qty, entry)
    if order:
        order_id = order["orderId"]
        print("Limit order gönderildi.")
        
        # Stop-loss emri
        sl_order = send_stop_loss_order(symbol, side, stop)
        if sl_order:
            print("Stop-loss gönderildi.")
        
        # Take-profit emri
        tp_order = send_take_profit_order(symbol,tp)
        if tp_order:
            print("Take-profit gönderildi.")
