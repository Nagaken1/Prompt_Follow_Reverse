import os
import csv
from datetime import datetime
from utils.time_util import get_trade_date


class OHLCWriter:
    """
    OHLCを取引日ごとのCSVファイルに保存するクラス。
    """

    def __init__(self, output_dir="csv"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.current_trade_date = None
        self.file = None
        self.writer = None

    def _open_new_file(self, trade_date: datetime.date):
        """
        指定された取引日のファイルを開く
        """
        if self.file:
            self.file.close()

        self.current_trade_date = trade_date
        filename = os.path.join(
            self.output_dir,
            f"{trade_date.strftime('%Y%m%d')}_nikkei_mini_future.csv"
        )
        self.file = open(filename, "a", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)

        if os.stat(filename).st_size == 0:
            self.writer.writerow(["Time","TradeTime" ,"Open", "High", "Low", "Close", "Dummy", "ContractMonth"])


    def write_row(self, ohlc: dict):
        """
        OHLCを1行書き込む（取引日を見てファイル分割）
        """
        time: datetime = ohlc["time"]
        trade_date = get_trade_date(time)

        if self.current_trade_date != trade_date:
            self._open_new_file(trade_date)

        # Dummy フラグ（dummy または real）
        dummy_flag = "dummy" if ohlc.get("is_dummy") else "real"

        # 限月（ダミーの場合は dummy）
        contract_month = ohlc.get("contract_month", "")
        if dummy_flag == "dummy":
            contract_month = "dummy"

        # trade_time（取引日＋時刻）
        trade_time = datetime.combine(trade_date, time.time())

        self.writer.writerow([
            time.strftime("%Y/%m/%d %H:%M:%S"),
            trade_time.strftime("%Y/%m/%d %H:%M:%S"),
            ohlc["open"],
            ohlc["high"],
            ohlc["low"],
            ohlc["close"],
            dummy_flag,
            contract_month
        ])
        self.file.flush()
        os.fsync(self.file.fileno())

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
