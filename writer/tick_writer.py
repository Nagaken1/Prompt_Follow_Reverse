import os
import csv
from datetime import datetime


class TickWriter:
    """
    受信したすべてのティックデータ（価格・時刻）をCSVファイルに記録するクラス。
    日付ごとにファイルを分割し、「csv/」フォルダに保存する。
    """

    def __init__(self, enable_output=True):
        self.enable_output = enable_output
        self.current_date = datetime.now().date()

        # Tick 出力ファイルの初期化
        self.file = None
        self.writer = None

        self.first_file_path = "latest_first_tick.csv"  # ← PyMin直下に固定

        if self.enable_output:
            date_str = self.current_date.strftime("%Y%m%d")
            tick_dir = "csv"
            os.makedirs(tick_dir, exist_ok=True)

            self.file_path = os.path.join(tick_dir, f"{date_str}_tick.csv")
            self.file = open(self.file_path, "a", newline="", encoding="utf-8")
            self.writer = csv.writer(self.file)

            # ファイルが空ならヘッダーを書き込む
            if self.file.tell() == 0:
                self.writer.writerow(["Time", "Price"])

        #  first_tick 出力ファイルは常に最新1行を保持（固定名ファイル）
        self.first_file_path =  "latest_first_tick.csv"
        os.makedirs("csv", exist_ok=True)
        self.last_written_minute = None  # 1分ごとの重複記録防止用

        # 初回オープンでヘッダーが必要なら書く（tell()で確認）
        self.first_file = open(self.first_file_path, "a", newline="", encoding="utf-8")
        self.first_writer = csv.writer(self.first_file)
        if self.first_file.tell() == 0:
            self.first_writer.writerow(["Time", "Price"])

    def write_tick(self, price, timestamp: datetime):
        """
        TickデータをCSVファイルに追記する。日付が変わった場合は新しいファイルに切り替える。
        また、first_tickは常に最新1件を上書きで保存する。
        """

        # 日付が変わったら通常ファイルのみ切り替える
        if timestamp.date() != self.current_date:
            if self.enable_output and self.file:
                self.file.close()
                date_str = timestamp.strftime("%Y%m%d")
                tick_dir = "csv"
                self.file_path = os.path.join(tick_dir, f"{date_str}_tick.csv")
                self.file = open(self.file_path, "a", newline="", encoding="utf-8")
                self.writer = csv.writer(self.file)
                if self.file.tell() == 0:
                    self.writer.writerow(["Time", "Price"])

            self.current_date = timestamp.date()
            self.last_written_minute = None  # 日付変更時にリセット

        # 書き込む内容を準備
        row = [timestamp.strftime("%Y/%m/%d %H:%M:%S"), price]

        #  first_tick は常に最新1件を上書き（1分ごとに1回だけ）
        tick_minute = timestamp.replace(second=0, microsecond=0)
        if self.last_written_minute != tick_minute:
            with open(self.first_file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Price"])
                writer.writerow(row)
            self.last_written_minute = tick_minute

        # 通常のTick出力（有効時のみ）
        if self.enable_output and self.writer:
            self.writer.writerow(row)
            self.file.flush()

    def close(self):
        """
        ファイルを閉じる。
        """
        if self.file:
            self.file.close()
        if self.first_file:
            self.first_file.close()