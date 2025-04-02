import os
import time

from datetime import datetime, time as dtime
from config.logger import setup_logger
from config.settings import ENABLE_TICK_OUTPUT

from client.kabu_websocket import KabuWebSocketClient
from handler.price_handler import PriceHandler
from writer.ohlc_writer import OHLCWriter
from writer.tick_writer import TickWriter
from utils.time_util import get_exchange_code, get_trade_date, is_night_session
from utils.symbol_resolver import get_active_term, get_symbol_code
from utils.export_util import export_connection_info, export_latest_minutes_from_files
from utils.future_info_util import get_token, get_last_line_of_latest_source ,register_symbol

def main():
    now = datetime.now().replace(tzinfo=None)
    # カレントディレクトリ変更（スクリプトのある場所を基準に）
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)
    prev_last_line = ""
    # ログ設定
    setup_logger()

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

    # 初期化
    ohlc_writer = OHLCWriter()
    tick_writer = TickWriter(enable_output=ENABLE_TICK_OUTPUT)
    price_handler = PriceHandler(ohlc_writer, tick_writer)
    #last_export_minute = None

    trade_date = get_trade_date(datetime.now())
    END_TIME = datetime.combine(trade_date, dtime(6, 5)) if is_night_session(now) else None

    if END_TIME and datetime.now().replace(tzinfo=None) >= END_TIME:
        print("[INFO] すでに取引終了時刻を過ぎているため、起動せず終了します。")
        return

    # WebSocketクライアント起動
    ws_client = KabuWebSocketClient(price_handler)
    ws_client.start()

    last_checked_minute = -1

    try:
        while True:
            now = datetime.now().replace(tzinfo=None)

            if END_TIME and datetime.now().replace(tzinfo=None) >= END_TIME:
                print("[INFO] 取引終了時刻になったため、自動終了します。")
                break

            # --- 追加: 毎分0秒に1回 tick を取得 ---
            if now.second == 0 and now.minute != last_tick_minute:
                if price_handler.latest_price is not None and price_handler.latest_timestamp is not None:
                    print(f"[TICK] {now.strftime('%H:%M:%S')} 時点のTick: {price_handler.latest_price} @ {price_handler.latest_timestamp}")
                else:
                    print(f"[TICK] {now.strftime('%H:%M:%S')} 時点: 最新Tickなし")
                price_handler.record_latest_tick()
                last_tick_minute = now.minute

            if now.minute != last_checked_minute and now.second == 1:
                for attempt in range(5):
                    current_last_line = get_last_line_of_latest_source("csv")

                    print(f"[DEBUG] 前回の最終行: {repr(prev_last_line)}")
                    print(f"[DEBUG] 今回の最終行: {repr(current_last_line)}")

                    if current_last_line != prev_last_line:
                        print("[INFO] ソースファイルが更新されたため、最新3分を書き出します。")
                        new_last_line = export_latest_minutes_from_files(
                            base_dir="csv",
                            minutes=3,
                            output_file="latest_ohlc.csv",
                            prev_last_line=prev_last_line
                        )
                        prev_last_line = new_last_line.strip()
                        break
                    else:
                        time.sleep(1)  # 最大5回リトライ

                last_checked_minute = now.minute  # 次の分まで再実行しない
        time.sleep(1)

    finally:
        price_handler.finalize_ohlc()
        ohlc_writer.close()
        if tick_writer:
            tick_writer.close()
        ws_client.stop()

if __name__ == "__main__":

    main()
