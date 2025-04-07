
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from writer.ohlc_builder import OHLCBuilder
from utils.time_util import is_closing_end, is_market_closed
from datetime import datetime, timedelta, time as dtime
from utils.symbol_resolver import get_active_term
from utils.export_util import export_latest_minutes_to_pd
import pandas as pd
from typing import Optional

class PriceHandler:
    """
    ティックを受信してOHLCを生成し、
    ファイルへの出力を管理するクラス。
    """
    def __init__(self, ohlc_writer: OHLCWriter, tick_writer: TickWriter):

        self.ohlc_builder = OHLCBuilder()
        self.ohlc_writer = ohlc_writer
        self.tick_writer = tick_writer
        self.last_written_minute = None
        self.latest_price = None
        self.latest_timestamp = None
        self.latest_price_status = None

    def get_latest_price(self) -> Optional[float]:
        """最新の価格を返す"""
        return self.latest_price

    def get_latest_timestamp(self) -> Optional[datetime]:
        """最新の価格時刻を返す"""
        return self.latest_timestamp

    def get_current_price_status(self) -> Optional[int]:
        """最新の現値ステータスを返す"""
        return self.latest_price_status

    def handle_tick(self, price: float, timestamp: datetime,current_price_status: int) -> Optional[pd.DataFrame]:
        self.latest_price = price
        self.latest_timestamp = timestamp
        self.latest_price_status = current_price_status

        contract_month = get_active_term(timestamp)

        if self.tick_writer is not None:
            self.tick_writer.write_tick(price, timestamp,current_price_status)

        # 次セッションの最初の価格を記録（ダミー補完に使用）
        if (
            self.ohlc_builder.first_price_of_next_session is None
            and not is_closing_end(timestamp)
            and self.ohlc_builder.closing_completed_session != self.ohlc_builder._get_session_id(timestamp)
        ):
            self.ohlc_builder.first_price_of_next_session = price

        df = None  # ✅ 最後に返すdf

        # ===== update() を繰り返し呼んで OHLC を返すまで処理 =====
        while True:
            ohlc = self.ohlc_builder.update(price, timestamp, contract_month=contract_month)
            if not ohlc:
                break  # 返ってこなければループ終了

            ohlc_time = ohlc["time"].replace(second=0, microsecond=0)
            current_tick_minute = timestamp.replace(second=0, microsecond=0, tzinfo=None)

            # 同一分または未来分（未確定） → 通常はスキップ
            if ohlc_time >= current_tick_minute and not ohlc["is_dummy"]:
                print(f"[SKIP] {ohlc_time} は現在分または未来分 → 未確定でスキップ")
                break

            # ダミーの重複を防ぐ（同一分で複数回出さない）
            if self.last_written_minute and ohlc["is_dummy"] and ohlc_time == self.last_written_minute:
                print(f"[SKIP] 同一のダミーは出力済みのためスキップ: {ohlc_time}")
                break

            # 通常の重複チェック
            if self.last_written_minute and ohlc_time <= self.last_written_minute:
                print(f"[SKIP] 重複のため {ohlc_time} をスキップ")
                break

            # 書き込み処理
            self.ohlc_writer.write_row(ohlc)
            self.last_written_minute = ohlc_time
            self.ohlc_builder.current_minute = ohlc_time
            print(f"[WRITE] OHLC確定: {ohlc_time} 値: {ohlc}")

            # ✅ OHLC確定ごとにdfを取得
            new_last_line, latest_df = export_latest_minutes_to_pd(
                base_dir="csv",
                minutes=3,
                prev_last_line=getattr(self, "prev_last_line", "")
            )
            self.prev_last_line = new_last_line.strip()
            df = latest_df  # ✅ 最後に返す用に保存
            #print("[DEBUG][handle_tick] latest_df:\n", latest_df)

        # ===== クロージングtick用の強制確定処理（15:45 or 6:00）=====
        if (timestamp.hour == 15 and timestamp.minute == 45) or (timestamp.hour == 6 and timestamp.minute == 0):
            print(f"[INFO][handle_tick] クロージングtickをhandle_tickに送ります: {price} @ {timestamp}")

            final_ohlc = self.ohlc_builder.force_finalize()
            if final_ohlc:
                final_time = final_ohlc["time"].replace(second=0, microsecond=0)
                if not self.last_written_minute or final_time > self.last_written_minute:
                    self.ohlc_writer.write_row(final_ohlc)
                    self.last_written_minute = final_time
                    print(f"[INFO][handle_tick] クロージングOHLCを強制出力: {final_time}")
                    # ✅ クロージングも出力
                    new_last_line, latest_df = export_latest_minutes_to_pd(
                        base_dir="csv",
                        minutes=3,
                        prev_last_line=getattr(self, "prev_last_line", "")
                    )
                    self.prev_last_line = new_last_line.strip()
                    df = latest_df
                    #print(df)
                else:
                    print(f"[INFO][handle_tick] クロージングOHLCはすでに出力済み: {final_time}")

        return df  # ✅ mainなどから受け取れるように返す

    def fill_missing_minutes(self, now: datetime):
        if is_market_closed(now):
            print(f"[DEBUG][fill_missing_minutes] 市場閉場中のため補完スキップ: {now}")
            return

        if self.ohlc_builder.current_minute is None or self.ohlc_builder.ohlc is None:
            print(f"[DEBUG][fill_missing_minutes] current_minute 未定義のため補完スキップ")
            return

        current_minute = now.replace(second=0, microsecond=0, tzinfo=None)
        last_minute = self.ohlc_builder.current_minute

        print(f"[DEBUG] fill_missing_minutes() 呼び出し")
        print(f"[DEBUG] last_ohlc.ohlc_time = {last_minute}")
        print(f"[DEBUG] current_minute = {current_minute}")
        print(f"[DEBUG] self.last_written_minute = {self.last_written_minute}")

        # last_written_minute を基準に補完スキップを判断
        if current_minute <= last_minute:
            print(f"[DEBUG][fill_missing_minutes] 補完不要: now={now}, current={current_minute}, last_written_minute={self.last_written_minute}")
            return

        while last_minute + timedelta(minutes=1) <= current_minute:
            print(f"[DEBUG][fill_missing_minutes] 補完開始: from {last_minute + timedelta(minutes=1)} to {current_minute - timedelta(minutes=1)}")
            next_minute = last_minute + timedelta(minutes=1)
            print(f"[TRACE] next_minute = {next_minute}")

            if is_market_closed(next_minute):
                print(f"[DEBUG][fill_missing_minutes] 補完対象が無音時間のためスキップ: {next_minute}")
                last_minute = next_minute
                continue

            last_minute = next_minute
            last_close = self.ohlc_builder.ohlc["close"]

            dummy = {
                "time": last_minute,
                "open": last_close,
                "high": last_close,
                "low": last_close,
                "close": last_close,
                "is_dummy": True,
                "contract_month": "dummy"
            }

            dummy_time = dummy["time"].replace(second=0, microsecond=0)
            print(f"[TRACE] dummy_time = {dummy_time}, last_written_minute = {self.last_written_minute}")

            if not self.last_written_minute or dummy_time > self.last_written_minute:
                print(f"[DEBUG][fill_missing_minutes] ダミー補完: {dummy_time}")
                self.ohlc_writer.write_row(dummy)
                self.last_written_minute = dummy_time
                self.ohlc_builder.current_minute = dummy_time
                self.ohlc_builder.ohlc = dummy
            else:
                print(f"[DEBUG][fill_missing_minutes] 重複のため補完打ち切り: {dummy_time}")
                break

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
