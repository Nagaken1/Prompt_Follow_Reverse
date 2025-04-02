import requests
from config.settings import API_BASE_URL, FUTURE_CODE
from datetime import datetime, timedelta, time as dtime

# 銘柄コードキャッシュ
_symbol_cache = {}

def get_active_term(now: datetime) -> int:
    """
    現在時刻に基づいて、有効な限月（YYYYMM形式）を返す。
    仕様：
    - 通常：3の倍数月に切り上げた期近
    - 第2金曜以降：期近を+3ヶ月に交代
    - 第2木曜の 8:45〜15:45：期先（期近+3ヶ月）を返す
    """

    year = now.year
    month = now.month

    # --- 3の倍数月に切り上げ（期近） ---
    future_month = ((month - 1) // 3 + 1) * 3
    if future_month > 12:
        future_month -= 12
        year += 1

    base_term_year = year
    base_term_month = future_month

    # --- 該当月の第2金曜と第2木曜を計算 ---
    if future_month in [3, 6, 9, 12]:
        first_day = datetime(base_term_year, base_term_month, 1)
        weekday = first_day.weekday()  # 月曜=0, 金曜=4

        days_to_second_friday = ((4 - weekday) % 7) + 7
        second_friday = first_day + timedelta(days=days_to_second_friday)
        second_thursday = second_friday - timedelta(days=1)

        # 日時ベースの比較のため date() を分離
        now_date = now.date()
        now_time = now.time()

        # SQの金曜以降 → +3ヶ月（期近交代）
        if now_date >= second_friday.date():
            base_term_month += 3
            if base_term_month > 12:
                base_term_month -= 12
                base_term_year += 1

        # 第2木曜かつ 8:45〜15:45 の間 → 期先（+6ヶ月）
        elif now_date == second_thursday.date():
            if dtime(8, 45) <= now_time <= dtime(15, 45):
                base_term_month += 6
                if base_term_month > 12:
                    base_term_month -= 12
                    base_term_year += 1

    return base_term_year * 100 + base_term_month


def get_symbol_code(term: int, token: str) -> str:
    """
    限月（YYYYMM）から銘柄コードを取得する。
    結果はキャッシュに保存して再利用。
    """
    if term in _symbol_cache:
        return _symbol_cache[term]

    url = f"{API_BASE_URL}/symbolname/future"
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": token
    }
    params = {
        "FutureCode": FUTURE_CODE,
        "DerivMonth": term
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        symbol = response.json()["Symbol"]
        _symbol_cache[term] = symbol
        print(f"[DEBUG] 銘柄コード取得成功: {symbol}")
        return symbol
    except Exception as e:
        print(f"[ERROR] 銘柄コード取得失敗: {e}")
        return None
