import asyncio
import websockets
import pandas as pd
import json
import time
import os
import sys
import inspect

SEND_INTERVAL = 0.1  # 秒
DURATION = 600     # 送信時間（秒）= 10分

# ✅ デバッグ出力
print("✅ このDummyServerWebSocket.pyは最新です")
print("[確認] 実行中のファイル:", os.path.abspath(__file__))
print("✅ 実行中のPythonインタプリタ:", sys.executable)

# ✅ 受信関数（正しい2引数）
async def tick_sender(websocket, path):

    # 実行ファイルのあるディレクトリを基準に相対パスを構築
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    EXCEL_PATH = os.path.join(BASE_DIR, "mnt", "data", "DummyTick.xlsx")

    print("[DEBUG] tick_sender() 呼び出されました")
    print("✅ tick_sender 定義:", inspect.signature(tick_sender))  # ← ここで確認！

    print("[接続] クライアントが接続しました")

    try:
        df = pd.read_excel(EXCEL_PATH)
        df.fillna("", inplace=True)

        start_time = time.time()
        idx = 0

        while time.time() - start_time < DURATION:
            if idx >= len(df):
                print("✔️ データをすべて送信しました")
                break

            row = df.iloc[idx]
            tick = {
                "Symbol": "165120019",  # 固定
                "Price": float(row["Price"]),
                "Volume": 1,  # 仮の出来高
                "Time": str(row["Time"])
            }

            await websocket.send(json.dumps(tick))
            print("📤 送信:", tick)
            await asyncio.sleep(SEND_INTERVAL)
            idx += 1

    except Exception as e:
        print(f"[ERROR] 送信中にエラー: {e}")

# ✅ サーバー起動
async def main():
    async with websockets.serve(tick_sender, "localhost", 9000):
        print("✅ 疑似Tickサーバー起動中 ws://localhost:9000")
        await asyncio.Future()  # 永久待機

if __name__ == "__main__":
    asyncio.run(main())
