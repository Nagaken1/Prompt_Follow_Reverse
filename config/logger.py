import os
import sys
from datetime import datetime
import atexit


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