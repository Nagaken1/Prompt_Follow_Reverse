from datetime import datetime, timedelta, time as dtime
from utils.time_util import is_pre_closing_period, get_next_closing_time

class OHLCBuilder:
    """
    ティックデータから1分足のOHLCを構築するクラス。
    プレクロージング補完に対応。
    """

    def __init__(self):
        self.current_minute = None
        self.ohlc = None
        self.closing_completed_session = None
        self.last_dummy_minute = None

        # 👇必要なフィールドを再定義
        self.first_price_of_next_session = None

    def update(self, price: float, timestamp: datetime, contract_month=None) -> dict:
        minute = timestamp.replace(second=0, microsecond=0, tzinfo=None)
        print(f"[DEBUG][update] 呼び出し: price={price}, timestamp={timestamp}, minute={minute}")

        if self.current_minute is None:
            self.current_minute = minute
            self.ohlc = {
                "time": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "is_dummy": False,
                "contract_month": contract_month
            }
            return None

        if minute > self.current_minute:
            completed = self.ohlc.copy()
            self.current_minute = minute
            self.ohlc = {
                "time": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "is_dummy": False,
                "contract_month": contract_month
            }
            return completed

        if minute == self.current_minute:
            self.ohlc["high"] = max(self.ohlc["high"], price)
            self.ohlc["low"] = min(self.ohlc["low"], price)
            self.ohlc["close"] = price
            return None

    def _finalize_ohlc(self) -> dict:
        return self.ohlc

    def force_finalize(self, now: datetime) -> dict:
        """
        クロージングtickなどで、明示的に現在時刻のOHLCを強制構築する。
        - OHLCがまだ当該時刻まで進んでいなければダミーで生成
        - 既に6:00の足がある場合はそのまま返す
        """
        target_minute = now.replace(second=0, microsecond=0)

        if self.ohlc is None or self.ohlc["time"] < target_minute:
            last_close = self.ohlc["close"] if self.ohlc else 0
            return {
                "time": target_minute,
                "open": last_close,
                "high": last_close,
                "low": last_close,
                "close": last_close,
                "is_dummy": True,
                "contract_month": "dummy"
            }

        return self.ohlc.copy()

    def _get_session_id(self, dt: datetime) -> str:
        t = dt.time()
        if t < dtime(6, 0):
            session_date = (dt - timedelta(days=1)).date()
            session_type = "night"
        elif t < dtime(15, 30):
            session_date = dt.date()
            session_type = "day"
        else:
            session_date = dt.date()
            session_type = "night"
        return f"{session_date.strftime('%Y%m%d')}_{session_type}"
