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
        self.first_price_of_next_session = None
        self.closing_completed_session = None
        self.pre_close_count = None
        self.last_dummy_minute = None

        # プレクロージング補完トリガー用
        self.same_timestamp_count = 0
        self.last_tick_timestamp = None

    def update(self, price: float, timestamp: datetime, contract_month=None) -> dict:
        minute = timestamp.replace(second=0, microsecond=0, tzinfo=None)
        print(f"[DEBUG][update] 呼び出し: price={price}, timestamp={timestamp}, minute={minute}")

        # 同一timestampの連続カウント（秒まで）
        if timestamp == self.last_tick_timestamp:
            self.same_timestamp_count += 1
        else:
            self.same_timestamp_count = 1
            self.last_tick_timestamp = timestamp

        # ==== プレクロージング補完のトリガー条件 ====
        if self.same_timestamp_count >= 60 and self.pre_close_count is None:
            if is_pre_closing_period(timestamp.time()):
                base_minute = minute
                end_minute = get_next_closing_time(base_minute)

                remaining = int((end_minute - base_minute).total_seconds() // 60)
                if remaining > 0:
                    print(f"[TRIGGER] timestamp連続({self.same_timestamp_count}回) → プレクロージング補完開始: {base_minute} → {remaining}分補完")
                    self.pre_close_count = remaining
                    self._pre_close_base_price = price
                    self._pre_close_base_minute = base_minute

        # 初回
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

        # 通常の分切り替え
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

        # プレクロージング補完のダミー出力
        if self.pre_close_count and self.pre_close_count > 0:
            next_dummy_time = self._pre_close_base_minute + timedelta(minutes=5 - self.pre_close_count)
            dummy = {
                "time": next_dummy_time,
                "open": self._pre_close_base_price,
                "high": self._pre_close_base_price,
                "low": self._pre_close_base_price,
                "close": self._pre_close_base_price,
                "is_dummy": True,
                "contract_month": "dummy"
            }
            self.pre_close_count -= 1
            self.current_minute = next_dummy_time
            self.ohlc = dummy
            self.last_dummy_minute = next_dummy_time

            print(f"[DUMMY] プレクロージング補完 {5 - self.pre_close_count}/5: {dummy['time']}")

            if self.pre_close_count == 0:
                self.pre_close_count = None
                print("[INFO] プレクロージング補完完了 → 通常処理に復帰")

            return dummy

        # 同一分内の更新
        if minute == self.current_minute:
            self.ohlc["high"] = max(self.ohlc["high"], price)
            self.ohlc["low"] = min(self.ohlc["low"], price)
            self.ohlc["close"] = price
            return None

    def _finalize_ohlc(self) -> dict:
        return self.ohlc

    def force_finalize(self) -> dict:
        if self.ohlc is None:
            return None
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
