import os
import sys
import time
from datetime import datetime, time as dtime

from config.logger import setup_logger
from config.settings import ENABLE_TICK_OUTPUT, DUMMY_TICK_TEST_MODE, DUMMY_URL
from client.kabu_websocket import KabuWebSocketClient
from client.dummy_websocket_client import DummyWebSocketClient
from handler.price_handler import PriceHandler
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from utils.time_util import get_exchange_code, get_trade_date, is_night_session, is_closing_minute, is_opening_minute
from utils.symbol_resolver import get_active_term, get_symbol_code
from utils.export_util import export_connection_info, export_latest_minutes_to_pd
from utils.future_info_util import get_token, register_symbol


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
    end_time = datetime.combine(get_trade_date(now), dtime(6, 5)) if is_night_session(now) else None

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
        print("[INFO] ↑↑↑ DataFrameここまで")
        print("-" * 50)
        #ここにザラバ中の処理を書く
        return new_last_line.strip()
    else:
        return prev_last_line


def run_main_loop(price_handler, ws_client, end_time):
    ws_client.start()
    last_checked_minute = -1
    closing_finalized = False
    prev_last_line = ""

    try:
        while True:
            price = price_handler.get_latest_price()
            timestamp = price_handler.get_latest_timestamp()
            status = price_handler.get_current_price_status()

            now = datetime.now().replace(second=0, microsecond=0) if timestamp is None else timestamp.replace(second=0, microsecond=0, tzinfo=None)

            if timestamp is None:
                if status == 12 and now.minute != last_checked_minute:
                    print(f"[INFO] サーキットブレイク中でも fill_missing_minutes を呼び出します。")
                    price_handler.fill_missing_minutes(now)
                    prev_last_line = update_if_changed(prev_last_line)
                    last_checked_minute = now.minute
                time.sleep(1)
                continue

            if is_opening_minute(now.time()) and closing_finalized:
                print(f"[INFO] セッション開始 {now.time()} のため closing_finalized をリセットします。")
                closing_finalized = False

            if end_time and now >= end_time:
                print("[INFO] 取引終了時刻になったため、自動終了します。")
                break

            if not is_closing_minute(now.time()):
                if now.minute != last_checked_minute:
                    print(f"[INFO] {now.strftime('%Y/%m/%d %H:%M:%S')} に fill_missing_minutes を呼び出します。")
                    price_handler.fill_missing_minutes(now)
                    prev_last_line = update_if_changed(prev_last_line)
                    last_checked_minute = now.minute
            else:
                if not closing_finalized:
                    print(f"[INFO] クロージングtickをhandle_tickに送ります: {price} @ {now}")
                    price_handler.handle_tick(price or 0, now, 1)
                    prev_last_line = update_if_changed(prev_last_line)
                    closing_finalized = True
                    last_checked_minute = now.minute

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
