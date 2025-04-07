
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from writer.ohlc_builder import OHLCBuilder
from utils.time_util import is_closing_end, is_market_closed
from datetime import datetime, timedelta, time as dtime
from utils.symbol_resolver import get_active_term
from utils.export_util import export_latest_minutes_to_pd, get_last_ohlc_time_from_csv
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
        self.last_written_minute = None
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

        return df  # ✅ mainなどから受け取れるように返す

    def trigger_closing_tick(self, price: float, timestamp: datetime) -> Optional[pd.DataFrame]:
        """クロージングtick用のOHLC強制確定処理"""
        print(f"[INFO][trigger_closing_tick] クロージングtickを処理します: {price} @ {timestamp}")

        self.latest_price = price
        self.latest_timestamp = timestamp
        self.latest_price_status = None  # クロージングtickではstatusは使わない前提

        final_ohlc = self.ohlc_builder.force_finalize()
        df = None

        if final_ohlc:
            final_time = final_ohlc["time"].replace(second=0, microsecond=0)
            if not self.last_written_minute or final_time > self.last_written_minute:
                self.ohlc_writer.write_row(final_ohlc)
                self.last_written_minute = final_time
                print(f"[INFO][trigger_closing_tick] クロージングOHLCを強制出力: {final_time}")

                new_last_line, latest_df = export_latest_minutes_to_pd(
                    base_dir="csv",
                    minutes=3,
                    prev_last_line=getattr(self, "prev_last_line", "")
                )
                self.prev_last_line = new_last_line.strip()
                df = latest_df
            else:
                print(f"[INFO][trigger_closing_tick] クロージングOHLCはすでに出力済み: {final_time}")
        else:
            print("[INFO][trigger_closing_tick] force_finalize() でOHLCが生成されませんでした")

        return df

    def fill_missing_minutes(self, now: datetime):
        if is_market_closed(now):
            print(f"[DEBUG][fill_missing_minutes] 市場閉場中のため補完スキップ: {now}")
            return

        # 🔧 OHLC未初期化なら前日のCSVから終値を取得して初期化
        if self.ohlc_builder.current_minute is None or self.ohlc_builder.ohlc is None:
            print(f"[INFO][fill_missing_minutes] current_minute 未定義のため前日の終値から補完を開始します")
            prev_close = get_previous_close_price(now)
            if prev_close is None:
                print(f"[WARN] 前日終値が取得できなかったため補完スキップ")
                return

            prev_date = now.date() - timedelta(days=1)
            last_time = datetime.combine(prev_date, datetime.min.time()) + timedelta(hours=15, minutes=15)

            dummy = {
                "time": last_time,
                "open": prev_close,
                "high": prev_close,
                "low": prev_close,
                "close": prev_close,
                "is_dummy": True,
                "contract_month": "from_prev_day"
            }

            self.ohlc_builder.ohlc = dummy
            self.ohlc_builder.current_minute = last_time
            self.last_written_minute = last_time

        # 🧮 通常の補完処理
        current_minute = now.replace(second=0, microsecond=0, tzinfo=None)
        last_minute = self.ohlc_builder.current_minute

        if current_minute <= last_minute:
            print(f"[DEBUG][fill_missing_minutes] 補完不要: now={now}, current={current_minute}, last_written_minute={self.last_written_minute}")
            return

        while last_minute + timedelta(minutes=1) <= current_minute:
            next_minute = last_minute + timedelta(minutes=1)

            # クロージング時間はtickが来るのを待つので補完しない（15:45 or 6:00）
            if (next_minute.hour == 15 and next_minute.minute == 45) or \
            (next_minute.hour == 6 and next_minute.minute == 0):
                print(f"[DEBUG][fill_missing_minutes] クロージング時間はtick待ちのため補完しません: {next_minute}")
                break

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
