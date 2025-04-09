from datetime import datetime, time as dtime, timedelta


def is_market_closed(now: datetime) -> bool:
    """
    市場が一時的に閉じている時間帯かどうかを判定。
    - 日中 → 15:46〜16:59
    - 夜間 → 06:01〜08:44
    """
    t = now.time()
    return dtime(15, 46) <= t <= dtime(16, 59) or dtime(6, 1) <= t <= dtime(8, 44)


def get_exchange_code(now: datetime) -> int:
    """
    現在の時刻に応じて、kabuステーション用の取引所コードを返す。
    - 23: 日中（8:43〜15:47）
    - 24: 夜間（それ以外）
    """
    t = now.time()
    return 23 if dtime(8, 43) <= t <= dtime(15, 47) else 24


def is_closing_end(ts: datetime) -> bool:
    """
    クロージングセッションの終了直前の時刻かを判定する。
    - 日中クロージング → 15:59
    - 夜間クロージング → 5:59
    """
    t = ts.time()
    return t == dtime(15, 59) or t == dtime(5, 59)


def get_trade_date(now: datetime) -> datetime.date:
    """
    ナイトセッション起点（17:00）での取引日を返し、土日なら前営業日に補正する。
    """
    trade_date = (now + timedelta(days=1)).date() if now.time() >= dtime(17, 0) else now.date()

    # 土曜（5）、日曜（6）は前営業日に補正
    while trade_date.weekday() >= 5:
        trade_date -= timedelta(days=1)

    return trade_date

def is_night_session(now: datetime) -> bool:
    """
    現在の時刻が夜間セッション中かを判定。
    17:00〜翌6:00未満を夜間セッションとみなす。
    """
    t = now.time()
    return t >= dtime(17, 0) or t < dtime(6, 0)

def is_closing_minute(minute: dtime) -> bool:
    """
    クロージング時間（15:45 または 6:00）かを判定
    """
    # t = minute.time() ← これは不要
    t = minute  # そのまま使えばOK
    return t == dtime(15, 45) or t == dtime(6, 0)

def is_opening_minute(minute: dtime) -> bool:
    """
    オープニング時間（8:45 または 17:00）かを判定
    """
    # t = minute.time() ← これは不要
    t = minute  # そのまま使えばOK
    return t == dtime(8, 45) or t == dtime(17, 0)

def is_pre_closing_minute(minute: dtime) -> bool:
    """
    プレクロージング時間（15:40 または 5:55）かを判定
    """
    # t = minute.time() ← これは不要
    t = minute  # そのまま使えばOK
    return t == dtime(15, 40) or t == dtime(5, 55)

def get_next_closing_time(now: datetime) -> datetime:
    """
    現在時刻に基づいて、次に訪れるクロージング時刻（15:45 または 翌6:00）を返す。
    """
    today = now.date()
    day_closing = datetime.combine(today, dtime(15, 45))
    night_closing = datetime.combine(today, dtime(6, 0))

    if now.time() < dtime(6, 0):
        return night_closing
    elif now.time() < dtime(15, 45):
        return day_closing
    else:
        # 翌営業日の 6:00（夜間クロージング）
        return night_closing + timedelta(days=1)

def is_pre_closing_period(t: dtime) -> bool:
    """
    プレクロージングの時間帯に該当するかを判定
    日中: 15:35〜15:44
    夜間: 05:50〜05:59
    """
    return (dtime(15, 35) <= t <= dtime(15, 44)) or (dtime(5, 50) <= t <= dtime(5, 59))

def is_day_session(now: datetime) -> bool:
    """
    日中セッション中かを判定。
    通常、8:45〜15:45未満を日中セッションとみなす。
    """
    t = now.time()
    return dtime(8, 45) <= t < dtime(15, 45)

def set_initial_checked_minute(price_handler, now: datetime, base_dir: str = "csv"):
    from datetime import datetime, time as dtime
    from utils.time_util import is_day_session, is_night_session
    from utils.export_util import get_last_ohlc_time_from_csv

    last_ohlc_time = get_last_ohlc_time_from_csv(base_dir)

    if last_ohlc_time:
        if is_day_session(now) and last_ohlc_time.time() >= dtime(5, 0):
            price_handler.last_checked_minute = datetime.combine(now.date(), dtime(8, 44))
            print(f"[INFO] 日中セッション補完を 8:45 から開始します")
        elif is_night_session(now) and last_ohlc_time.time() >= dtime(14, 0):
            price_handler.last_checked_minute = datetime.combine(now.date(), dtime(16, 59))
            print(f"[INFO] 夜間セッション補完を 17:00 から開始します")
        else:
            price_handler.last_checked_minute = last_ohlc_time
            print(f"[INFO] 最後に出力されたOHLC時刻から補完を開始: {last_ohlc_time}")
    else:
        print("[INFO] 出力済みOHLCが見つからなかったため、補完開始時刻は起動時刻以降")


def get_session_end_time(now: datetime) -> datetime | None:
    """
    現在時刻に基づいて、日中または夜間セッションの終了時刻を返す。
    対応するセッションでなければ None を返す。
    """
    if is_night_session(now):
        return datetime.combine(get_trade_date(now), dtime(6, 5))
    elif is_day_session(now):
        return datetime.combine(now.date(), dtime(15, 50))
    else:
        return None