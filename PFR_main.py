import os
import sys
import time
from datetime import datetime, time as dtime
from datetime import timedelta
import pandas as pd

from config.logger import setup_logger
from config.settings import ENABLE_TICK_OUTPUT, DUMMY_TICK_TEST_MODE,DUMMY_URL
from client.kabu_websocket import KabuWebSocketClient
from handler.price_handler import PriceHandler
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from utils.time_util import get_exchange_code, get_trade_date, is_night_session, is_closing_minute
from utils.symbol_resolver import get_active_term, get_symbol_code
from utils.export_util import export_connection_info, export_latest_minutes_to_pd
from utils.future_info_util import get_token ,register_symbol,get_cb_info
from client.dummy_websocket_client import DummyWebSocketClient


def main():
    print("実行中のPython:", sys.executable)
    now = datetime.now().replace(tzinfo=None)
    # カレントディレクトリ変更（スクリプトのある場所を基準に）
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)
    prev_last_line = ""
    # ログ設定
    setup_logger()


    # 初期化
    ohlc_writer = OHLCWriter()
    tick_writer = TickWriter(enable_output=ENABLE_TICK_OUTPUT)
    price_handler = PriceHandler(ohlc_writer, tick_writer)
    #last_export_minute = None

    if DUMMY_TICK_TEST_MODE:
        # ダミーWebSocketクライアント起動
        ws_client = DummyWebSocketClient(price_handler, uri = DUMMY_URL)
    else:
        token = get_token()
        if not token:
            return

        active_term = get_active_term(now)
        symbol_code = get_symbol_code(active_term, token)
        if not symbol_code:
            print("[ERROR] 銘柄コード取得失敗")
            return

        exchange_code = get_exchange_code(now)
        if not register_symbol(symbol_code, exchange_code, token):
            return

        # 接続情報を出力
        export_connection_info(symbol_code, exchange_code, token)

        # WebSocketクライアント起動
        ws_client = KabuWebSocketClient(price_handler)

    trade_date = get_trade_date(datetime.now())
    END_TIME = datetime.combine(trade_date, dtime(6, 5)) if is_night_session(now) else None

    ws_client.start()

    last_checked_minute = -1
    closing_finalized = False

    try:
        while True:
            price = price_handler.get_latest_price()
            timestamp = price_handler.get_latest_timestamp()
            status = price_handler.get_current_price_status()

            if timestamp is None:
                # ✅ tickが来ていない状態でも、statusだけはログする
                if status == 12:
                    now = datetime.now().replace(second=0, microsecond=0)
                    print(f"[INFO] {now.strftime('%Y/%m/%d %H:%M:%S')} サーキットブレイク実施中（tick未到達）")
                time.sleep(1)
                continue

            now = timestamp.replace(second=0, microsecond=0)

            # ✅ セッション開始時刻でフラグをリセット
            if now.time() == dtime(8, 45) or now.time() == dtime(17, 0):
                if closing_finalized:
                    print(f"[INFO] セッション開始 {now.time()} のため closing_finalized をリセットします。")
                    closing_finalized = False

            # ✅ 終了判定
            if END_TIME and now >= END_TIME:
                print("[INFO] 取引終了時刻になったため、自動終了します。")
                break

            # ✅ ザラバ中：足の補完処理のみ（出力はhandle_tick任せ）
            if not is_closing_minute(now.time()):
                if now.minute != last_checked_minute:

                    print(f"[INFO] {now.strftime('%Y/%m/%d %H:%M:%S')} に fill_missing_minutes を呼び出します。")
                    price_handler.fill_missing_minutes(now)

                    # ✅ 最新3分を取得して差分があれば出力
                    new_last_line, df = export_latest_minutes_to_pd(
                        base_dir="csv",
                        minutes=3,
                        prev_last_line=prev_last_line
                    )
                    if new_last_line != prev_last_line and df is not None and not df.empty:
                        prev_last_line = new_last_line.strip()
                        print("[INFO] handle_tick により最新3分が更新されました:")
                        print("[INFO] 最新3分のDataFrame ↓↓↓")
                        print(df)
                        print("[INFO] ↑↑↑ DataFrameここまで")
                        print("-" * 50)
                    else:
                        print("[DEBUG] dfはNoneまたは空でした")

                    last_checked_minute = now.minute

            # ✅ プレクロージング：15:45 or 6:00 に1回だけ処理
            else:
                if ((now.hour == 15 and now.minute == 45) or (now.hour == 6 and now.minute == 0)) \
                    and not closing_finalized:
                    print(f"[INFO] クロージングtickをhandle_tickに送ります: {price} @ {now}")
                    price_handler.handle_tick(price or 0, now, 1)

                    # ✅ 最新3分を取得して差分があれば出力
                    new_last_line, df = export_latest_minutes_to_pd(
                        base_dir="csv",
                        minutes=3,
                        prev_last_line=prev_last_line
                    )
                    if new_last_line != prev_last_line and df is not None and not df.empty:
                        prev_last_line = new_last_line.strip()
                        print("[INFO] handle_tick により最新3分が更新されました:")
                        print("[INFO] 最新3分のDataFrame ↓↓↓")
                        print(df)
                        print("[INFO] ↑↑↑ DataFrameここまで")
                        print("-" * 50)
                    else:
                        print("[DEBUG] dfはNoneまたは空でした")

                    closing_finalized = True
                    last_checked_minute = now.minute

            time.sleep(1)

    finally:
        price_handler.finalize_ohlc()
        ohlc_writer.close()
        if tick_writer:
            tick_writer.close()
        ws_client.stop()

if __name__ == "__main__":

    main()
