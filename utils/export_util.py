
import os
import csv
from datetime import timedelta
import pandas as pd
from typing import Optional
from datetime import datetime
from typing import Tuple

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

def export_latest_minutes_to_csv(base_dir: str, minutes: int = 3, output_file: str = "latest_ohlc.csv", prev_last_line: str = "") -> str:
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

def export_latest_minutes_to_pd(base_dir: str, minutes: int = 3, prev_last_line: str = "") -> Tuple[str, pd.DataFrame]:
    """
    ディレクトリ内のCSVファイルから、更新日時が新しい2つを読み込み、
    最新N分間のデータを抽出して返す。
    戻り値は (最終行の文字列, 最新N分のDataFrame)。
    """
    try:
        # 拡張子が.csvのファイルを取得
        csv_files = [
            os.path.join(base_dir, f)
            for f in os.listdir(base_dir)
            if f.endswith(".csv")
        ]

        if len(csv_files) < 1:
            print("[警告] 対象CSVファイルが見つかりませんでした")
            return prev_last_line, pd.DataFrame()

        # 更新日時でソート（新しい順）
        csv_files.sort(key=os.path.getmtime, reverse=True)

        # 最新の2つを対象とする
        target_files = csv_files[:2]

        combined_df = pd.DataFrame()

        for path in reversed(target_files):
            try:
                temp_df = pd.read_csv(path)
                temp_df["Time"] = pd.to_datetime(temp_df["Time"])
                combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
            except Exception as e:
                print(f"[警告] {os.path.basename(path)} の読み込みに失敗: {e}")

        if combined_df.empty:
            print("[警告] ファイル読み込みに失敗しました")
            return prev_last_line, pd.DataFrame()

        # 最新N分のデータを抽出
        latest_time = combined_df["Time"].max()
        start_time = latest_time - timedelta(minutes=minutes - 1)
        latest_df = combined_df[combined_df["Time"] >= start_time].copy()
        latest_df.sort_values("Time", inplace=True)

        # 日付フォーマットの変換（表示用）
        latest_df["Time"] = latest_df["Time"].dt.strftime("%Y/%m/%d %H:%M:%S")

        # 最終行の取得
        if not latest_df.empty:
            last_row_str = ",".join(map(str, latest_df.iloc[-1].values))
        else:
            last_row_str = prev_last_line

        return last_row_str, latest_df

    except Exception as e:
        print(f"[エラー] 処理中に例外が発生しました: {e}")
        return prev_last_line, pd.DataFrame()

def get_last_ohlc_time_from_csv(base_dir: str) -> Optional[datetime]:
    """
    base_dir 内のすべての CSV ファイルを対象に、
    最も新しい（最大の）Time を持つ OHLC の時刻を返す。
    """
    try:
        csv_files = [
            os.path.join(base_dir, f)
            for f in os.listdir(base_dir)
            if f.endswith(".csv")
        ]

        if not csv_files:
            print(f"[WARN] CSVファイルが見つかりません（ディレクトリ: {base_dir}）")
            return None

        latest_time = None

        for path in csv_files:
            try:
                df = pd.read_csv(path)
                if not df.empty and "Time" in df.columns:
                    df["Time"] = pd.to_datetime(df["Time"])
                    max_time = df["Time"].max()
                    if latest_time is None or max_time > latest_time:
                        latest_time = max_time
            except Exception as e:
                print(f"[WARN] ファイル読み取り失敗: {os.path.basename(path)} → {e}")

        if latest_time:
            return latest_time.replace(second=0, microsecond=0)
        else:
            print("[WARN] 有効なTimeを持つデータが見つかりませんでした")
            return None

    except Exception as e:
        print(f"[ERROR] get_last_ohlc_time_from_csv エラー: {e}")
        return None