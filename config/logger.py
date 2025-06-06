import os
import sys
from datetime import datetime
import atexit
import pandas as pd
from utils.time_util import get_trade_date

class DualLogger:
    """
    標準出力とログファイルの両方に出力するロガークラス。
    タイムスタンプ付きでログを記録。
    """

    def __init__(self, log_file_path: str):
        self.terminal = sys.__stdout__
        self.log = open(log_file_path, "a", encoding="utf-8")
        self.buffer = ""

    def write(self, message: str):
        self.buffer += message

        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            timestamp = datetime.now().strftime("[%Y/%m/%d %H:%M:%S] ")
            full_line = timestamp + line + "\n"

            self.terminal.write(full_line)
            self.log.write(full_line)
            self.flush()

    def flush(self):
        if self.buffer:
            timestamp = datetime.now().strftime("[%Y/%m/%d %H:%M:%S] ")
            full_line = timestamp + self.buffer
            self.terminal.write(full_line)
            self.log.write(full_line)
            self.buffer = ""

        self.terminal.flush()
        self.log.flush()


def setup_logger():
    """
    ログフォルダを作成し、標準出力をDualLoggerに差し替える。
    日付ベースのログファイルに出力。
    """
    log_dir = "log"
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("%Y%m%d") + "_PFR_log.txt"
    log_path = os.path.join(log_dir, log_filename)

    sys.stdout = DualLogger(log_path)
    sys.stderr = sys.stdout

    atexit.register(sys.stdout.flush) # 終了時に flush を確実におこなう

def log_timeline_data(**data):
    """
    指定された日付のTimeLineログCSVに、1行の時系列データを追記する関数。

    機能概要:
    - 'log/YYYYMMDD_TimeLineLog.csv' が存在するかを確認。
    - 存在しない場合はヘッダー付きで新規作成。
    - 存在する場合は現在のタイムスタンプで1行データを追記。
    - 渡されなかったカラムは "False" で自動補完される。

    引数:
    - **data: 任意のカラム名とその値をキーワード引数で指定。
             使用可能なカラムは以下のとおり:

        ['First_Tick','Time', 'Open', 'High', 'Low', 'Close',
         'シグナル', 'ポジション', '狙い建値', '実際の建値']

    使用例:
        log_timeline_data(
            date_str="20250403",
            Open=39010,
            Close=39020,
            シグナル="買い"
        )
    """

    log_dir = "log"
    os.makedirs(log_dir, exist_ok=True)
    date_str = get_trade_date(datetime.now())
    file_path = os.path.join(log_dir, f"{date_str}_TimeLineLog.csv")

    headers = ['First_Tick','Time', 'Open', 'High', 'Low', 'Close', 'Dummy', 'ContractMonth', 'Signal', 'Position', '狙い建値', '実際の建値']

    timestamp = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    # 初期化された空の辞書
    row = {}

    # 各カラム（header）ごとに値を取り出す
    for key in headers:
        if key in data:
            row[key] = data[key]  # 渡された値を使う
        else:
            row[key] = "False"    # 未指定なら "False" をセット

    df = pd.DataFrame([row], columns=headers, index=[timestamp])
    df.index.name = "Timestamp"

    if not os.path.isfile(file_path):
        df.to_csv(file_path, encoding='utf-8-sig')
    else:
        df.to_csv(file_path, mode='a', header=False, encoding='utf-8-sig')

def log_timeline_data_from_pd(df: pd.DataFrame):
    """
    OHLCデータを含むDataFrameを1行ずつ log_timeline_data() に渡して
    'log/YYYYMMDD_TimeLineLog.csv' に記録する関数。

    想定されるカラム: 'Time', 'Open', 'High', 'Low', 'Close'

    他のカラム（例：シグナル、ポジション等）を追加する場合は
    log_timeline_data() の呼び出し部分で編集可能。
    """
    for _, row in df.iterrows():
        log_timeline_data(
            Time=row["Time"],
            Open=row["Open"],
            High=row["High"],
            Low=row["Low"],
            Close=row["Close"],
            Dummy=row["Dummy"],
            ContractMonth=row["ContractMonth"]
        )