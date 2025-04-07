from datetime import datetime, timedelta, time


class OHLCBuilder:
    """
    ティックデータから1分足のOHLCを構築するクラス。
    日中・夜間セッションごとにクロージング補完に対応。
    """

    def __init__(self):
        self.current_minute = None
        self.ohlc = None
        self.first_price_of_next_session = None
        self.closing_completed_session = None  # ← セッション単位で記録
        self.last_dummy_minute = None

    def update(self, price: float, timestamp: datetime, contract_month=None) -> dict:
        minute = timestamp.replace(second=0, microsecond=0, tzinfo=None)
        print(f"[DEBUG][update] 呼び出し: price={price}, timestamp={timestamp}, minute={minute}")

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

        # 通常の分切り替えを優先
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


        # 同一分内の更新
        if minute == self.current_minute:
            self.ohlc["high"] = max(self.ohlc["high"], price)
            self.ohlc["low"] = min(self.ohlc["low"], price)
            self.ohlc["close"] = price
            return None

    def _finalize_ohlc(self) -> dict:
        """
        現在保持している最新のOHLC（確定済み or 補完）を返す。
        """
        return self.ohlc


    def force_finalize(self) -> dict:
        """
        クロージングtickなどで明示的にOHLCを確定・取得する。
        """
        if self.ohlc is None:
            return None
        return self.ohlc.copy()

    def _get_session_id(self, dt: datetime) -> str:
        """
        日中・夜間セッションごとのIDを返す。
        """
        t = dt.time()

        if t < time(6, 0):
            # 深夜は前日夜間セッションに属する
            session_date = (dt - timedelta(days=1)).date()
            session_type = "night"
        elif t < time(15, 30):
            session_date = dt.date()
            session_type = "day"
        else:
            session_date = dt.date()
            session_type = "night"

        return f"{session_date.strftime('%Y%m%d')}_{session_type}"
