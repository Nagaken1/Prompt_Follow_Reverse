
import os
import csv
from datetime import timedelta
import pandas as pd

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