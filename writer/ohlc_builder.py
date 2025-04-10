from datetime import datetime, timedelta, time as dtime


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

    def force_finalize(self, now: datetime, force_dummy=False) -> dict:
        """
        クロージングtickなどで、明示的に現在時刻のOHLCを強制構築する。
        - OHLCがまだ当該時刻まで進んでいなければダミーで生成
        - 既に6:00の足がある場合はそのまま返す
        """
        target_minute = now.replace(second=0, microsecond=0)

        if self.ohlc and self.ohlc["time"] == target_minute:
            ohlc_copy = self.ohlc.copy()
            if force_dummy:
                ohlc_copy["is_dummy"] = True
            return ohlc_copy

        if self.ohlc and self.ohlc["time"] > target_minute:
            return None

        close_price = self.ohlc["close"] if self.ohlc else 0
        return {
            "time": target_minute,
            "open": close_price,
            "high": close_price,
            "low": close_price,
            "close": close_price,
            "is_dummy": True,
            "contract_month": self.ohlc["contract_month"] if self.ohlc else "dummy"
    }
