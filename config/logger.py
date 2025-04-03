import os
import sys
from datetime import datetime
import atexit
import pandas as pd

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

    log_filename = datetime.now().strftime("%Y%m%d") + "_PyMin_log.txt"
    log_path = os.path.join(log_dir, log_filename)

    sys.stdout = DualLogger(log_path)
    sys.stderr = sys.stdout

    atexit.register(sys.stdout.flush) # 終了時に flush を確実におこなう

def log_timeline_data(date_str: str, **data):
    """
    指定された日付のTimeLineログCSVに、1行の時系列データを追記する関数。

    機能概要:
    - 'log/YYYYMMDD_TimeLineLog.csv' が存在するかを確認。
    - 存在しない場合はヘッダー付きで新規作成。
    - 存在する場合は現在のタイムスタンプで1行データを追記。
    - 渡されなかったカラムは "False" で自動補完される。

    引数:
    - date_str (str): 対象となる取引日（形式: 'YYYYMMDD'）
    - **data: 任意のカラム名とその値をキーワード引数で指定。
             使用可能なカラムは以下のとおり:

        ['First_Tick', 'Open', 'High', 'Low', 'Close',
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
    file_path = os.path.join(log_dir, f"{date_str}_TimeLineLog.csv")

    headers = ['First_Tick', 'Open', 'High', 'Low', 'Close',
            'シグナル', 'ポジション', '狙い建値', '実際の建値']

    timestamp = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    # 未指定の項目は "False" で埋める
    row = {key: data.get(key, "False") for key in headers}
    df = pd.DataFrame([row], columns=headers, index=[timestamp])
    df.index.name = "Timestamp"

    if not os.path.isfile(file_path):
        df.to_csv(file_path, encoding='utf-8-sig')
    else:
        df.to_csv(file_path, mode='a', header=False, encoding='utf-8-sig')