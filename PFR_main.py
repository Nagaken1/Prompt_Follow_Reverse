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
from utils.time_util import get_exchange_code, get_trade_date, is_night_session, is_closing_minute, is_opening_minute, is_day_session
from utils.symbol_resolver import get_active_term, get_symbol_code
from utils.export_util import export_connection_info, export_latest_minutes_to_pd,get_last_ohlc_time_from_csv
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
        return ohlc_writer, tick_writer, price_handler, ws_client, None

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

    if is_night_session(now):
        end_time = datetime.combine(get_trade_date(now), dtime(6, 5))
    elif is_day_session(now):
        end_time = datetime.combine(now.date(), dtime(15, 50))
    else:
        end_time = None

    # ✅ 補完開始基準の初期設定
    last_ohlc_time = get_last_ohlc_time_from_csv("csv")

    if last_ohlc_time:
        if is_day_session(now) and last_ohlc_time.time() >= dtime(5, 0):
            # 日中で、夜間が終わっていれば → 8:44から始める → 8:45が補完対象となる
            price_handler.last_checked_minute = datetime.combine(now.date(), dtime(8, 44))
            print(f"[INFO] 日中セッション補完を 8:45 から開始します")
        elif is_night_session(now) and last_ohlc_time.time() >= dtime(14, 0):
            # 夜間で、日中が終わっていれば → 16:59から始める → 17:00が補完対象となる
            price_handler.last_checked_minute = datetime.combine(now.date(), dtime(16, 59))
            print(f"[INFO] 夜間セッション補完を 17:00 から開始します")
        else:
            # 通常どおり、最後に出力されたOHLCの時刻から補完
            price_handler.last_checked_minute = last_ohlc_time
            print(f"[INFO] 最後に出力されたOHLC時刻から補完を開始: {last_ohlc_time}")
    else:
        print("[INFO] 出力済みOHLCが見つからなかったため、補完開始時刻は起動時刻以降")

    return ohlc_writer, tick_writer, price_handler, ws_client, end_time


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


def run_main_loop(price_handler, ws_client, end_time=None):
    ws_client.start()
    last_checked_minute = None  # datetime型として初期化
    closing_finalized = False
    prev_last_line = ""

    try:
        while True:
            price = price_handler.get_latest_price()
            timestamp = price_handler.get_latest_timestamp()
            status = price_handler.get_current_price_status()

            now = datetime.now().replace(second=0, microsecond=0) if timestamp is None else timestamp.replace(second=0, microsecond=0, tzinfo=None)

            # サーキットブレイク中（tickなし）
            if timestamp is None:
                if status == 12:
                    if last_checked_minute is None:
                        last_checked_minute = now
                        print(f"[INFO] 初回サーキットブレイクtick。補完スキップ: {now}")
                    else:
                        diff = int((now - last_checked_minute).total_seconds() // 60) - 1
                        if diff > 0:
                            print(f"[INFO] サーキットブレイク中の分飛び補完: {diff}分")
                            last_close =  get_previous_close_price(datetime.now())
                            price_handler.fill_missing_minutes(
                                start_minute=last_checked_minute,
                                end_minute=now.time(),
                                base_price=last_close
                            )
                            last_checked_minute = now
                    time.sleep(1)
                    continue

            # セッション再開判定（夜→朝など）
            if is_opening_minute(now.time()) and closing_finalized:
                print(f"[INFO] セッション再開 {now.time()} により closing_finalized をリセット")
                closing_finalized = False

            # セッション終了判定
            if end_time and now >= end_time:
                print("[INFO] 取引終了時刻に達したため終了")
                break

            # 通常補完処理（tickあり）
            if not is_closing_minute(now.time()):
                if last_checked_minute is None:
                    print(f"[INFO] 初回tick検出。補完スキップ: {now}")
                    last_checked_minute = now
                else:
                    diff = int((now - last_checked_minute).total_seconds() // 60) - 1
                    if diff > 0:
                        print(f"[INFO] 分飛び補完: {diff}分 (from {last_checked_minute} to {now})")
                        last_close = get_previous_close_price(datetime.now())
                        price_handler.fill_missing_minutes(
                            start_minute=last_checked_minute,
                            end_minute=now.time(),
                            base_price=last_close
                        )
                    last_checked_minute = now
                prev_last_line = update_if_changed(prev_last_line)

            # プレクロージング or クロージング時刻 → 補完＋クロージングtick処理
            else:
                if not closing_finalized:
                    diff = int((now - last_checked_minute).total_seconds() // 60) - 1 if last_checked_minute else 0
                    if diff > 0:
                        print(f"[INFO] 分飛び補完: {diff}分 (from {last_checked_minute} to {now})")
                    else:
                        print(f"[INFO] クロージング時刻 {now.time()} に到達 → 補完は不要")

                    last_close =  get_previous_close_price(datetime.now())
                    price_handler.fill_missing_minutes(
                        start_minute=last_checked_minute,
                        end_minute=now.time(),
                        base_price=last_close
                    )
                    print(f"[INFO] クロージングtickをhandle_tickに送信: {price} @ {now}")
                    price_handler.handle_tick(price or 0, now, 1)
                    prev_last_line = update_if_changed(prev_last_line)
                    closing_finalized = True
                    last_checked_minute = now


            time.sleep(1)


    finally:
        shutdown(price_handler, ws_client)


def shutdown(price_handler, ws_client):
    price_handler.finalize_ohlc()
    if ws_client:
        ws_client.stop()


def main():
    now = datetime.now().replace(tzinfo=None)

    setup_environment()

    ohlc_writer, tick_writer, price_handler, ws_client, end_time = initialize_components(now)
    if not ws_client:
        return

    run_main_loop(price_handler, ws_client, end_time)


if __name__ == "__main__":
    main()
