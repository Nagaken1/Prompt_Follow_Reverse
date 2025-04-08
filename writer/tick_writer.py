import os
import csv
from datetime import datetime
from utils.time_util import get_trade_date

class TickWriter:
    """
    受信したすべてのティックデータ（価格・時刻）をCSVファイルに記録するクラス。
    日付ごとにファイルを分割し、「csv/」フォルダに保存する。
    """

    def __init__(self, enable_output=True):
        self.enable_output = enable_output
        self.current_date = datetime.now().date()
        self.first_file = None
        self.current_price_status = None

        # Tick 出力ファイルの初期化
        self.file = None
        self.writer = None

        if self.enable_output:
            date_str = self.current_date.strftime("%Y%m%d")
            tick_dir = "tick_csv"
            os.makedirs(tick_dir, exist_ok=True)

            self.file_path = os.path.join(tick_dir, f"{date_str}_tick.csv")
            self.file = open(self.file_path, "a", newline="", encoding="utf-8")
            self.writer = csv.writer(self.file)

            # ファイルが空ならヘッダーを書き込む
            if self.file.tell() == 0:
                self.writer.writerow(["Time", "Price","CurrentPriceStatus"])


    def write_tick(self, price, timestamp: datetime, current_price_status):
        """
        TickデータをCSVファイルに追記する。取引日が変わった場合は新しいファイルに切り替える。
        """
        trade_date = get_trade_date(timestamp)  # ← 取引日取得

        if trade_date != self.current_date:
            if self.enable_output and self.file:
                self.file.close()
            tick_dir = "tick_csv"
            os.makedirs(tick_dir, exist_ok=True)

            self.file_path = os.path.join(tick_dir, f"{trade_date.strftime('%Y%m%d')}_tick.csv")
            self.file = open(self.file_path, "a", newline="", encoding="utf-8")
            self.writer = csv.writer(self.file)

            if self.file.tell() == 0:
                self.writer.writerow(["Time", "Price", "CurrentPriceStatus"])

            self.current_date = trade_date
            self.last_written_minute = None  # 取引日変更時にリセット

        row = [timestamp.strftime("%Y/%m/%d %H:%M:%S"), price, current_price_status]

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