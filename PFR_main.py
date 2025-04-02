import os
import sys
import time
import json
import requests
from datetime import datetime, time as dtime
from datetime import timedelta
import pandas as pd
import csv

from config.logger import setup_logger
from config.settings import API_BASE_URL, get_api_password
from config.settings import ENABLE_TICK_OUTPUT

from client.kabu_websocket import KabuWebSocketClient
from handler.price_handler import PriceHandler
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from utils.time_util import get_exchange_code, get_trade_date, is_night_session
from utils.symbol_resolver import get_active_term, get_symbol_code

def get_last_line(file_path: str) -> str:
    """
    ファイルの最終行を返す（空なら "" を返す）。
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else ""
    except Exception as e:
        print(f"[ERROR] 最終行取得に失敗: {e}")
        return ""


def export_latest_minutes_from_files(base_dir: str, minutes: int = 3, output_file: str = "latest_ohlc.csv", prev_last_line: str = "") -> str:
    """
    ディレクトリ内のCSVファイルから、最新2つを読み込み、N分間のデータを抽出して出力。
    変更があった場合に最新の最終行を返す。
    """
    try:
        files = [
            f for f in os.listdir(base_dir)
            if f.endswith("_nikkei_mini_future.csv") and f[:8].isdigit()
        ]

        if len(files) < 1:
            print("[警告] 対象CSVファイルが見つかりませんでした")
            return prev_last_line

        files_sorted = sorted(files, reverse=True)
        target_files = files_sorted[:2]

        combined_df = pd.DataFrame()

        for fname in reversed(target_files):
            path = os.path.join(base_dir, fname)
            try:
                temp_df = pd.read_csv(path)
                temp_df["Time"] = pd.to_datetime(temp_df["Time"])
                combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
            except Exception as e:
                print(f"[警告] {fname} の読み込みに失敗: {e}")

        if combined_df.empty:
            print("[警告] ファイル読み込みに失敗しました")
            return prev_last_line

        latest_time = combined_df["Time"].max()
        start_time = latest_time - timedelta(minutes=minutes - 1)
        latest_df = combined_df[combined_df["Time"] >= start_time].copy()
        latest_df.sort_values("Time", inplace=True)

        # ↓ 日付のフォーマットを統一（YYYY/MM/DD HH:MM:SS）
        latest_df["Time"] = latest_df["Time"].dt.strftime("%Y/%m/%d %H:%M:%S")

        latest_df.to_csv(output_file, index=False)
        print(f"[更新] {output_file} に最新{minutes}分を書き出しました。")

        # 最終行を取得して返す
        with open(output_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else prev_last_line

    except Exception as e:
        print(f"[エラー] 処理中に例外が発生しました: {e}")
        return prev_last_line

def get_last_line_of_latest_source(base_dir: str) -> str:
    """
    base_dir 内で最も新しい _nikkei_mini_future.csv の最終行を取得する。
    余計な改行・空白を除去して比較できるようにする。
    """
    try:
        files = [
            f for f in os.listdir(base_dir)
            if f.endswith("_nikkei_mini_future.csv") and f[:8].isdigit()
        ]
        if not files:
            return ""

        # 日付順にソートして最新ファイルを取得
        latest_file = sorted(files, reverse=True)[0]
        latest_path = os.path.join(base_dir, latest_file)

        with open(latest_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else ""
    except Exception as e:
        print(f"[ERROR] ソースファイルの最終行取得に失敗: {e}")
        return ""

def get_token() -> str:
    """APIトークンを取得する"""
    url = f"{API_BASE_URL}/token"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"APIPassword": get_api_password()})

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        return response.json()["Token"]
    except Exception as e:
        print(f"[ERROR] トークン取得失敗: {e}")
        return None


def register_symbol(symbol_code: str, exchange_code: int, token: str) -> bool:
    """銘柄をKabuステーションに登録"""
    url = f"{API_BASE_URL}/register"
    headers = {"Content-Type": "application/json", "X-API-KEY": token}
    payload = {
        "Symbols": [
            {"Symbol": symbol_code, "Exchange": exchange_code}
        ]
    }

    try:
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        print("[OK] 銘柄登録成功:", response.json())
        return True
    except Exception as e:
        print(f"[ERROR] 銘柄登録失敗: {e}")
        return False

def export_connection_info(symbol_code: str, exchange_code: int, token: str, output_file: str = "connection_info.csv"):
    """
    symbol_code, exchange_code, token をCSVファイルに1行で出力する。
    """
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["SymbolCode", "ExchangeCode", "Token"])
            writer.writerow([symbol_code, exchange_code, token])
        print(f"[INFO] 接続情報を {output_file} に書き出しました。")
    except Exception as e:
        print(f"[ERROR] 接続情報の書き出しに失敗しました: {e}")

def main():
    now = datetime.now().replace(tzinfo=None)
    # カレントディレクトリ変更（スクリプトのある場所を基準に）
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)
    prev_last_line = ""
    # ログ設定
    setup_logger()

    token = get_token()
    if not token:
        return

    active_term = get_active_term(now)
    symbol_code = get_symbol_code(active_term, token)
    if not symbol_code:
        print("[ERROR] 銘柄コード取得失敗")
        return

    exchange_code = get_exchange_code(now)
    if not register_symbol(symbol_code, exchange_code, token):
        return

    # 接続情報を出力
    export_connection_info(symbol_code, exchange_code, token)

    # 初期化
    ohlc_writer = OHLCWriter()
    tick_writer = TickWriter(enable_output=ENABLE_TICK_OUTPUT)
    price_handler = PriceHandler(ohlc_writer, tick_writer)
    #last_export_minute = None

    trade_date = get_trade_date(datetime.now())
    END_TIME = datetime.combine(trade_date, dtime(6, 5)) if is_night_session(now) else None

    if END_TIME and datetime.now().replace(tzinfo=None) >= END_TIME:
        print("[INFO] すでに取引終了時刻を過ぎているため、起動せず終了します。")
        return

    # WebSocketクライアント起動
    ws_client = KabuWebSocketClient(price_handler)
    ws_client.start()

    last_checked_minute = -1

    try:
        while True:
            now = datetime.now().replace(tzinfo=None)

            if END_TIME and datetime.now().replace(tzinfo=None) >= END_TIME:
                print("[INFO] 取引終了時刻になったため、自動終了します。")
                break

            if now.minute != last_checked_minute and now.second == 1:
                for attempt in range(5):
                    current_last_line = get_last_line_of_latest_source("csv")

                    print(f"[DEBUG] 前回の最終行: {repr(prev_last_line)}")
                    print(f"[DEBUG] 今回の最終行: {repr(current_last_line)}")

                    if current_last_line != prev_last_line:
                        print("[INFO] ソースファイルが更新されたため、最新3分を書き出します。")
                        new_last_line = export_latest_minutes_from_files(
                            base_dir="csv",
                            minutes=3,
                            output_file="latest_ohlc.csv",
                            prev_last_line=prev_last_line
                        )
                        prev_last_line = new_last_line.strip()
                        break
                    else:
                        time.sleep(1)  # 最大5回リトライ

                last_checked_minute = now.minute  # 次の分まで再実行しない
        time.sleep(1)

    finally:
        price_handler.finalize_ohlc()
        ohlc_writer.close()
        if tick_writer:
            tick_writer.close()
        ws_client.stop()

if __name__ == "__main__":

    main()
