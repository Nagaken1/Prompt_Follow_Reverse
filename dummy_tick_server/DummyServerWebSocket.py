import asyncio
import websockets
import pandas as pd
import json
import time
import os
import sys
import inspect

SEND_INTERVAL = 0.1  # 秒
DURATION = 600       # 送信時間（秒）

# ✅ 設定読み込み
# ルートからたどる（どこで実行してもOKにする）
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
import_path = os.path.join(project_root, "config", "settings.json")

with open(import_path, "r", encoding="utf-8") as f:
    settings = json.load(f)

python_exec = settings.get("DUMMY_SERVER_PYTHON_EXECUTABLE", sys.executable)

# ✅ デバッグ出力
print("✅ このDummyServerWebSocket.pyは最新です")
print("[確認] 実行中のファイル:", os.path.abspath(__file__))
print("✅ 実行中のPythonインタプリタ:", python_exec)

# ✅ 受信関数
async def tick_sender(websocket, path):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TICK_DIR = os.path.join(BASE_DIR, "dummy_tick_data")

    print("[DEBUG] tick_sender() 呼び出されました")
    print("✅ tick_sender 定義:", inspect.signature(tick_sender))
    print("[接続] クライアントが接続しました")

    try:
        # ✅ 最新のCSVファイルを取得
        csv_files = [
            os.path.join(TICK_DIR, f)
            for f in os.listdir(TICK_DIR)
            if f.endswith(".csv")
        ]
        if not csv_files:
            raise FileNotFoundError("dummy_tick_data フォルダにCSVファイルが見つかりません")

        latest_csv = max(csv_files, key=os.path.getmtime)
        print(f"[INFO] 最新のCSVファイルを使用: {latest_csv}")

        df = pd.read_csv(latest_csv)
        df.fillna("", inplace=True)

        start_time = time.time()
        idx = 0

        while time.time() - start_time < DURATION:
            if idx >= len(df):
                print("✔️ データをすべて送信しました")
                break

            row = df.iloc[idx]
            tick = {
                "Symbol": "165120019",
                "Price": float(row["Price"]),
                "Volume": 1,
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