#live liq
import websocket
import json
import sqlite3
from datetime import datetime

def on_message(ws, message):
    message = json.loads(message)
    if 'e' in message and message['e'] == 'forceOrder':
        conn = sqlite3.connect('liquidations?live.db')
        c = conn.cursor()
        data = message['o']
        c.execute('''INSERT INTO binance_liquidations (timestamp, symbol, price, quantity, side)
                     VALUES (?, ?, ?, ?, ?)''',
                  (datetime.fromtimestamp(data['T'] / 1000), data['s'], 
                   float(data['p']), float(data['q']), data['S']))
        conn.commit()
        conn.close()

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": ["btcusdt@forceOrder"],
        "id": 1
    }))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://fstream.binance.com/ws",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
