
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from writer.ohlc_builder import OHLCBuilder
from utils.time_util import is_closing_end, is_market_closed,is_pre_closing_minute,is_closing_minute
from datetime import datetime, timedelta, time as dtime
from utils.symbol_resolver import get_active_term
from utils.export_util import get_last_ohlc_time_from_csv
import pandas as pd
from typing import Optional
from utils.future_info_util import get_previous_close_price  # 事前に作るユーティリティ想定


class PriceHandler:
    """
    ティックを受信してOHLCを生成し、
    ファイルへの出力を管理するクラス。
    """
    def __init__(self, ohlc_writer: OHLCWriter, tick_writer: TickWriter):

        self.ohlc_builder = OHLCBuilder()
        self.ohlc_writer = ohlc_writer
        self.tick_writer = tick_writer

        self.latest_price = None
        self.latest_timestamp = None
        self.latest_price_status = None
        self.last_written_minute = get_last_ohlc_time_from_csv("csv")


    def get_latest_price(self) -> Optional[float]:
        """最新の価格を返す"""
        return self.latest_price

    def get_latest_timestamp(self) -> Optional[datetime]:
        """最新の価格時刻を返す"""
        return self.latest_timestamp

    def get_current_price_status(self) -> Optional[int]:
        """最新の現値ステータスを返す"""
        return self.latest_price_status

    def handle_tick(self, price: float, timestamp: datetime, current_price_status: int) -> None:

        # 通常処理ここから
        self.latest_price = price
        self.latest_timestamp = timestamp
        self.latest_price_status = current_price_status

        contract_month = get_active_term(timestamp)

        if self.tick_writer is not None:
            self.tick_writer.write_tick(price, timestamp, current_price_status)

        # ===== update() を繰り返し呼んで OHLC を返すまで処理 =====
        while True:
            ohlc = self.ohlc_builder.update(price, timestamp, contract_month=contract_month)
            if not ohlc:
                break

            ohlc_time = ohlc["time"].replace(second=0, microsecond=0)
            current_tick_minute = timestamp.replace(second=0, microsecond=0, tzinfo=None)

            if ohlc_time >= current_tick_minute and not ohlc["is_dummy"]:
                print(f"[SKIP] {ohlc_time} は現在分または未来分 → 未確定でスキップ")
                break

            if self.last_written_minute and ohlc["is_dummy"] and ohlc_time == self.last_written_minute:
                print(f"[SKIP] 同一のダミーは出力済みのためスキップ: {ohlc_time}")
                break

            if self.last_written_minute and ohlc_time <= self.last_written_minute:
                print(f"[SKIP] 重複のため {ohlc_time} をスキップ")
                break

            self.ohlc_writer.write_row(ohlc)
            self.last_written_minute = ohlc_time
            self.ohlc_builder.current_minute = ohlc_time
            print(f"[WRITE] OHLC確定: {ohlc_time} 値: {ohlc}")

        # ===== クロージングtick用の強制確定処理（15:45 or 6:00）=====
        if is_closing_minute(timestamp.time()):
            print(f"[INFO][handle_tick] クロージングtickをhandle_tickに送ります: {price} @ {timestamp}")

            final_ohlc = self.ohlc_builder.force_finalize(timestamp)
            if final_ohlc:
                final_time = final_ohlc["time"].replace(second=0, microsecond=0)
                if not self.last_written_minute or final_time > self.last_written_minute:
                    self.ohlc_writer.write_row(final_ohlc)
                    self.last_written_minute = final_time
                    print(f"[INFO][handle_tick] クロージングOHLCを強制出力: {final_time}")
                else:
                    print(f"[INFO][handle_tick] クロージングOHLCはすでに出力済み: {final_time}")

    def fill_missing_minutes(self, start_minute: datetime, end_minute: dtime):
        """
        指定された開始時刻からend_timeまでの間の欠損分を1分足で補完する
        :param start_minute: 最後に出力されたOHLCの時刻（datetime型）
        :param end_time: 現在時点での最新timestampの「時:分」（dtime型）
        """
        current = start_minute + timedelta(minutes=1)

        print(f"[INFO][fill_missing_minutes] 補完処理開始: {current.time()}〜{end_minute}")

        while current.time() < end_minute:
            if is_market_closed(current):
                print(f"[SKIP][fill_missing_minutes] 市場閉場中のため補完スキップ: {current}")
                current += timedelta(minutes=1)
                continue

            if self.last_written_minute and current <= self.last_written_minute:
                print(f"[SKIP][fill_missing_minutes] すでに出力済み: {current} <= {self.last_written_minute}")
                current += timedelta(minutes=1)
                continue

            dummy = {
                "time": current,
                "open": self.latest_price,
                "high": self.latest_price,
                "low": self.latest_price,
                "close": self.latest_price,
                "is_dummy": True,
                "contract_month": "dummy"
            }

            print(f"[FILL][fill_missing_minutes] ダミー補完: {current}")
            self.ohlc_writer.write_row(dummy)
            self.last_written_minute = current
            self.ohlc_builder.current_minute = current
            self.ohlc_builder.ohlc = dummy

            current += timedelta(minutes=1)

    def finalize_ohlc(self):
        final = self.ohlc_builder._finalize_ohlc()
        if final:
            final_time = final["time"].replace(second=0, microsecond=0)
            if not self.last_written_minute or final_time > self.last_written_minute:
                print(f"[DEBUG][finalize_ohlc] 終了時最終OHLC書き込み: {final_time}")
                self.ohlc_writer.write_row(final)
                self.last_written_minute = final_time
            else:
                print(f"[DEBUG][finalize_ohlc] 重複でスキップ: {final_time}")
        else:
            print(f"[DEBUG][finalize_ohlc] 最終OHLCなし")

        if self.tick_writer:
            self.tick_writer.close()
