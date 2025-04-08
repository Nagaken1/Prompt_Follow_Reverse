import os
import csv
from datetime import datetime
from utils.time_util import get_trade_date

class TickWriter:
    """
    受信したすべてのティックデータ（価格・時刻）をCSVファイルに記録するクラス。
    日付ごとにファイルを分割し、「tick_csv/」フォルダに保存する。
    """

    def __init__(self, enable_output=True):
        self.enable_output = enable_output
        self.current_date = None
        self.file = None
        self.writer = None
        self.file_path = None

    def write_tick(self, price, timestamp: datetime, current_price_status):
        """
        TickデータをCSVファイルに追記する。取引日が変わった場合は新しいファイルに切り替える。
        """
        if not self.enable_output:
            return

        trade_date = get_trade_date(timestamp)

        if trade_date != self.current_date or self.writer is None:
            if self.file:
                self.file.close()

            tick_dir = "tick_csv"
            os.makedirs(tick_dir, exist_ok=True)
            self.file_path = os.path.join(tick_dir, f"{trade_date.strftime('%Y%m%d')}_tick.csv")
            self.file = open(self.file_path, "a", newline="", encoding="utf-8")
            self.writer = csv.writer(self.file)

            if self.file.tell() == 0:
                self.writer.writerow(["Time", "Price", "CurrentPriceStatus"])

            self.current_date = trade_date

        row = [timestamp.strftime("%Y/%m/%d %H:%M:%S"), price, current_price_status]
        self.writer.writerow(row)
        self.file.flush()

    def close(self):
        """
        ファイルを閉じる。
        """
        if self.file:
            self.file.close()