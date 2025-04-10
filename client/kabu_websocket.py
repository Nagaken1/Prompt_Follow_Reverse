import websocket
import threading
import json
import time
from datetime import datetime

from handler.price_handler import PriceHandler


class KabuWebSocketClient:
    """
    KabuステーションのWebSocketクライアント。
    push配信を受信し、PriceHandler に現値を渡す。
    """

    def __init__(self, price_handler: PriceHandler):
        self.ws = None
        self.thread = None
        self.running = False
        self.price_handler = price_handler

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            price = data.get("CurrentPrice")
            timestamp_str = data.get("CurrentPriceTime")
            current_price_status = data.get("CurrentPriceStatus")
            dummy = False
            force_finalize = False

            if price is not None and timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str)
                self.price_handler.handle_tick(price, timestamp, current_price_status, dummy, force_finalize)

        except Exception as e:
            print(f"[ERROR] メッセージ処理エラー: {e}")

    def on_error(self, ws, error):
        print(f"[ERROR] WebSocket エラー: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("[INFO] WebSocket 切断")

    def on_open(self, ws):
        print("[INFO] WebSocket 接続成功")

    def start(self):
        self.running = True

        def run():
            while self.running:
                try:
                    self.ws = websocket.WebSocketApp(
                        "ws://localhost:18080/kabusapi/websocket",
                        on_message=self.on_message,
                        on_error=self.on_error,
                        on_close=self.on_close,
                        on_open=self.on_open
                    )
                    self.ws.run_forever()
                except Exception as e:
                    print(f"[ERROR] 再接続エラー: {e}")
                    time.sleep(5)

        self.thread = threading.Thread(target=run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
        if self.thread:
            self.thread.join()
