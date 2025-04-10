
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from writer.ohlc_builder import OHLCBuilder
from utils.time_util import  is_market_closed,is_closing_minute
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

        self.previous_timestamp = None
        self.same_timestamp_count = 0

        # ✅ 追加：前回セッションの終値を保持（補完の初期値に使用）
        self.previous_close_price = get_previous_close_price(datetime.now())

        if self.previous_close_price:
            print(f"[INFO][__init__] 前回セッションの終値: {self.previous_close_price}")
        else:
            print("[WARN][__init__] 前回セッションの終値が取得できませんでした。")

    def get_latest_price(self) -> Optional[float]:
        """最新の価格を返す"""
        return self.latest_price

    def get_latest_timestamp(self) -> Optional[datetime]:
        """最新の価格時刻を返す"""
        return self.latest_timestamp

    def get_current_price_status(self) -> Optional[int]:
        """最新の現値ステータスを返す"""
        return self.latest_price_status

    def get_same_timestamp_count(self) -> int:
        """
        同じtimestampのtickが何回連続して来ているかを返す。
        """
        return self.same_timestamp_count

    def handle_tick(self, price: float, timestamp: datetime, current_price_status: int , dummy:bool ,force_finalize: bool) -> None:

        # 通常処理ここから
        self.latest_price = price
        self.latest_timestamp = timestamp
        self.latest_price_status = current_price_status

        contract_month = get_active_term(timestamp)

        if self.tick_writer is not None:
            self.tick_writer.write_tick(price, timestamp, current_price_status)

        #  同一timestampの連続カウント処理
        if self.previous_timestamp == timestamp:
            self.same_timestamp_count += 1
        else:
            self.same_timestamp_count = 1  # 初回を1としてカウント
            self.previous_timestamp = timestamp


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

        # ===== 強制確定処理（クロージングおよび同一タイムスタンプが続いた場合）=====
        if force_finalize:
            print(f"[INFO][handle_tick] tickをhandle_tickに送ります: {price} @ {timestamp}")

            final_ohlc = self.ohlc_builder.force_finalize(timestamp,force_dummy=dummy)
            if final_ohlc:
                final_time = final_ohlc["time"].replace(second=0, microsecond=0)
                if not self.last_written_minute or final_time > self.last_written_minute:
                    self.ohlc_writer.write_row(final_ohlc)
                    self.last_written_minute = final_time
                    print(f"[INFO][handle_tick] OHLCを強制出力: {final_time}")
                else:
                    print(f"[INFO][handle_tick] OHLCはすでに出力済み: {final_time}")

    def fill_missing_minutes(self, start_minute: datetime, end_minute: dtime, base_price: Optional[float] = None):
        """
        指定された開始時刻から end_time までの間の欠損分を1分足で補完する。
        :param start_minute: 最後に出力されたOHLCの時刻（datetime型）
        :param end_minute: 現在時点での最新timestampの「時:分」（dtime型）
        :param base_price: OHLCがない場合に使用する補完基準価格（例: 直前の終値）
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
                "open": base_price,
                "high": base_price,
                "low": base_price,
                "close": base_price,
                "is_dummy": True,
                "contract_month": "dummy"
            }

            print(f"[FILL][fill_missing_minutes] ダミー補完: {current}")
            self.ohlc_writer.write_row(dummy)
            self.last_written_minute = current

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
