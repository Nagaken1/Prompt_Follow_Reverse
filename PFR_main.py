import os
import sys
import time
from datetime import datetime, time as dtime
from datetime import timedelta
import logging

from config.logger import setup_logger
from config.settings import ENABLE_TICK_OUTPUT, DUMMY_TICK_TEST_MODE, DUMMY_URL
from client.kabu_websocket import KabuWebSocketClient
from client.dummy_websocket_client import DummyWebSocketClient
from handler.price_handler import PriceHandler
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from utils.time_util import get_exchange_code, is_opening_minute,get_initial_checked_minute,get_session_end_time,get_closing_tick_time,should_trigger_closing
from utils.symbol_resolver import get_active_term, get_symbol_code
from utils.export_util import export_connection_info, export_latest_minutes_to_pd
from utils.future_info_util import get_token, register_symbol,get_previous_close_price


def setup_environment():
    print("実行中のPython:", sys.executable)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)
    setup_logger()
    return base_dir


def initialize_components(now):
    ohlc_writer = OHLCWriter()
    tick_writer = TickWriter(enable_output=ENABLE_TICK_OUTPUT)
    price_handler = PriceHandler(ohlc_writer, tick_writer)

    if DUMMY_TICK_TEST_MODE:
        ws_client = DummyWebSocketClient(price_handler, uri=DUMMY_URL)
        end_time = None
        last_checked_minute = get_initial_checked_minute(now)
        return ohlc_writer, tick_writer, price_handler, ws_client, end_time, last_checked_minute

    token = get_token()
    if not token:
        print("[ERROR] トークン取得に失敗しました")
        return None, None, None, None, None

    active_term = get_active_term(now)
    symbol_code = get_symbol_code(active_term, token)
    if not symbol_code:
        print("[ERROR] 銘柄コード取得失敗")
        return None, None, None, None, None

    exchange_code = get_exchange_code(now)
    if not register_symbol(symbol_code, exchange_code, token):
        print("[ERROR] 銘柄登録失敗")
        return None, None, None, None, None

    export_connection_info(symbol_code, exchange_code, token)
    ws_client = KabuWebSocketClient(price_handler)

    end_time = get_session_end_time(now)

    last_checked_minute = get_initial_checked_minute(now)

    return ohlc_writer, tick_writer, price_handler, ws_client, end_time, last_checked_minute


def update_if_changed(prev_last_line):
    new_last_line, df = export_latest_minutes_to_pd(
        base_dir="csv",
        minutes=3,
        prev_last_line=prev_last_line
    )
    if new_last_line != prev_last_line and df is not None and not df.empty:
        print("[INFO] 最新3分のDataFrame ↓↓↓")
        print("[DEBUG] update_if_changed called, new_last_line:", new_last_line)
        print(df.reset_index(drop=True).to_string())
        sys.stdout.flush()  # 強制的に出力を反映
        print("[INFO] ↑↑↑ DataFrameここまで")
        print("-" * 50)
        #ここにザラバ中の処理を書く
        return new_last_line.strip()
    else:
        return prev_last_line

def run_main_loop(price_handler, ws_client, end_time=None, last_checked_minute=None):
    ws_client.start()
    closing_finalized = False
    prev_last_line = ""

    try:
        while True:
            price = price_handler.get_latest_price()
            timestamp = price_handler.get_latest_timestamp()
            status = price_handler.get_current_price_status()

            now = datetime.now().replace(second=0, microsecond=0) if timestamp is None else timestamp.replace(second=0, microsecond=0, tzinfo=None)

            same_count = price_handler.get_same_timestamp_count()
            #print(f"[DEBUG][run_main_loop] 同一timestamp継続カウント: {same_count}")

            real_now = datetime.now().replace(second=0, microsecond=0)

            # ✅ サーキットブレイク中の補完処理
            if timestamp is None and status == 12:
                last_checked_minute = handle_gap_fill(
                    price_handler, last_checked_minute, now, reason="サーキットブレイク中"
                )
                time.sleep(1)
                continue

            # ✅ セッション再開
            if is_opening_minute(now.time()) and closing_finalized:
                print(f"[INFO] セッション再開 {now.time()} により closing_finalized をリセット")
                closing_finalized = False

            # ✅ 自動終了
            if end_time and now >= end_time:
                print("[INFO] 取引終了時刻に達したため終了")
                break

            # ✅ 通常 or プレクロージング補完
            if last_checked_minute is None:
                print(f"[INFO] 初回tick検出。補完スキップ: {now}")
                last_checked_minute = now
            else:
                last_checked_minute = handle_gap_fill(
                    price_handler, last_checked_minute, now
                )
            prev_last_line = update_if_changed(prev_last_line)

            if same_count > 300 and status != 12:#同じタイムスタンプが続いた場合かつ、サーキットブレーカーではない
                tick_time = now
                print(f"[INFO] tickをhandle_tickに強制送信（same_count={same_count}）: {price} @ {tick_time}")
                print("同じタイムスタンプが続いた場合かつ、サーキットブレーカーではない")
                price_handler.handle_tick(price or 0, tick_time, 1 , False ,True)
                prev_last_line = update_if_changed(prev_last_line)
                continue

            if not closing_finalized and should_trigger_closing(now.time(), real_now.time()):  # tickが15:45または6:00に来た場合
                 # クロージングtickが来ていたらそのtimestampを使う。
                tick_time = now

                # ✅ 15:40〜15:44 の補完（tick_time を含めないように -1分）
                last_checked_minute = handle_gap_fill(
                    price_handler, last_checked_minute, tick_time, is_closing=True
                )

                # ✅ 15:45 or 6:00 の足を強制的に確定させる
                print(f"[INFO] tickをhandle_tickに強制送信（same_count={same_count}）: {price} @ {tick_time}")
                print('tickが15:45または6:00に来た場合')
                price_handler.handle_tick(price or 0, tick_time, 1 ,False, True)

                prev_last_line = update_if_changed(prev_last_line)
                closing_finalized = True

            # ✅ クロージングtick補完＋出力
            if (
                not closing_finalized and same_count > 300 and real_now >= get_closing_tick_time(now, real_now)  # 実時間がクロージング時刻を超えている
            ):
                # クロージングtickが来ていたらそのtimestampを使う。来ていなければ日付固定で補完。
                tick_time = now if should_trigger_closing(now.time(), real_now.time()) else get_closing_tick_time(now, real_now)

                # ✅ 15:40〜15:44 の補完（tick_time を含めないように -1分）
                last_checked_minute = handle_gap_fill(
                    price_handler, last_checked_minute, tick_time, is_closing=True
                )

                # ✅ 15:45 or 6:00 の足を強制的に確定させる
                print(f"[INFO] tickをhandle_tickに強制送信（same_count={same_count}）: {price} @ {tick_time}")
                print('tickが15:45または6:00に来なかった場合')
                price_handler.handle_tick(price or 0, tick_time, 1 ,True, True)

                prev_last_line = update_if_changed(prev_last_line)
                closing_finalized = True

    finally:
        shutdown(price_handler, ws_client)


def handle_gap_fill(price_handler, last_checked_minute, now, is_closing=False, reason=""):
    diff = int((now - last_checked_minute).total_seconds() // 60) - 1 if last_checked_minute else 0

    if diff > 0:
        label = "[INFO]" if not is_closing else "[CLOSE]"
        reason_text = f" ({reason})" if reason else ""
        print(f"{label} 分飛び補完: {diff}分 (from {last_checked_minute} to {now}){reason_text}")

        base_price = get_previous_close_price(now)

        price_handler.fill_missing_minutes(
            start_minute=last_checked_minute,
            end_minute=now.time(),
            base_price=base_price
        )
    elif is_closing:
        print(f"[CLOSE] クロージング時刻 {now.time()} に到達 → 補完は不要")

    return now

def shutdown(price_handler, ws_client):
    price_handler.finalize_ohlc()
    if ws_client:
        ws_client.stop()


def main():
    now = datetime.now().replace(tzinfo=None)

    setup_environment()

    ohlc_writer, tick_writer, price_handler, ws_client, end_time, last_checked_minute  = initialize_components(now)
    if not ws_client:
        return

    run_main_loop(price_handler, ws_client, end_time,last_checked_minute )


if __name__ == "__main__":
    main()
