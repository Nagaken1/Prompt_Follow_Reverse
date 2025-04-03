import os
from config.settings import API_BASE_URL, get_api_password
import json
import requests

def get_token() -> str:
    """APIトークンを取得する"""
    url = f"{API_BASE_URL}/token"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"APIPassword": get_api_password()})

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        return response.json()["Token"]
    except Exception as e:
        print(f"[ERROR] トークン取得失敗: {e}")
        return None


def get_last_line_of_latest_source(base_dir: str) -> str:
    """
    base_dir 内で最も新しい _nikkei_mini_future.csv の最終行を取得する。
    余計な改行・空白を除去して比較できるようにする。
    """
    try:
        files = [
            f for f in os.listdir(base_dir)
            if f.endswith("_nikkei_mini_future.csv") and f[:8].isdigit()
        ]
        if not files:
            return ""

        # 日付順にソートして最新ファイルを取得
        latest_file = sorted(files, reverse=True)[0]
        latest_path = os.path.join(base_dir, latest_file)

        with open(latest_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else ""
    except Exception as e:
        print(f"[ERROR] ソースファイルの最終行取得に失敗: {e}")
        return ""



def register_symbol(symbol_code: str, exchange_code: int, token: str) -> bool:
    """銘柄をKabuステーションに登録"""
    url = f"{API_BASE_URL}/register"
    headers = {"Content-Type": "application/json", "X-API-KEY": token}
    payload = {
        "Symbols": [
            {"Symbol": symbol_code, "Exchange": exchange_code}
        ]
    }

    try:
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        print("[OK] 銘柄登録成功:", response.json())
        return True
    except Exception as e:
        print(f"[ERROR] 銘柄登録失敗: {e}")
        return False

def get_positions(token):
    """保有情報を取得"""
    url = f"{API_BASE_URL}/positions"
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': token
    }
    response = requests.get(url, headers=headers)
    return response.json()

    # 実行例
    #token = get_token(API_KEY)
    #positions = get_positions(token)

    #for pos in positions:
    #    print({
    #        "約定番号": pos['ExecutionID'],
    #        "銘柄コード": pos['Symbol'],
    #        "値段（約定価格）": pos['Price'],
    #        "残数量（保有数量）": pos['LeavesQty'],
    #        "拘束数量": pos['HoldQty'],
    #        "売買区分": pos['Side'],
    #        "評価損益額": pos['ProfitLoss']
    #        "手数料": pos['Commission'],
    #        "手数料消費税": pos['CommissionTax'],
    #        "銘柄種別": pos['SecurityType']
    #    })

def get_orders(token):
    """注文情報を取得"""
    url = f"{API_BASE_URL}/orders"
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': token
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("注文取得失敗:", response.text)

    # 実行例
    #orders = get_orders(token)
    #for order in orders:
    #   print({
    #       "注文番号": order["Id"],
    #       "状態": order["State"],  # 状態: 受付済, 一部約定など
    #       "執行条件": order["OrdType"],
    #       "銘柄コード": order["Symbol"],
    #       "発注数量": order["OrderQty"],
    #       "約定数量": order["CumQty"],
    #       "売買区分": order["Side"],  # 1=買い, 2=売り
    #       "取引区分": order["CashMargin"],
    #   })
    #   details = order.get("Details", [])
    #   for i, detail in enumerate(details):
    #       rec_type = detail.get("RecType")
    #       print(f"  - 明細{i+1}: RecType = {rec_type}")

def get_future_Trade_Limit(token):
    """先物の取引余力を取得"""
    url = f"{API_BASE_URL}/wallet/future"
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': token
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def has_position(side: int, token: str) -> bool:
    """
    指定された売買区分（1=買い, 2=売り）で建玉が存在するかどうかを判定する。

    Args:
        side (int): 売買区分。1=買い、2=売り
        token (str): kabuステーショントークン

    Returns:
        bool: 指定区分で建玉がある場合は True、なければ False
    """
    try:
        positions = get_positions(token)
        for pos in positions:
            if pos.get("Side") == side and pos.get("LeavesQty", 0) > 0:
                return True
        return False
    except Exception as e:
        print(f"[ERROR] 建玉判定に失敗: {e}")
        return False

    # 実行例
    #if __name__ == "__main__":
    #token = get_token()

    #if has_position(1, token):
    #    print("買い建玉あり")
    #else:
    #    print("買い建玉なし")

    #if has_position(2, token):
    #    print("売り建玉あり")
    #else:
    #    print("売り建玉なし")

def no_positions(token: str) -> bool:
    """
    買い・売りいずれの建玉も保有していない場合に True を返す。

    Args:
        token (str): kabuステーショントークン

    Returns:
        bool: 建玉を一切保有していなければ True、それ以外は False
    """
    return not has_position(1, token) and not has_position(2, token)

    # 実行例
    #if __name__ == "__main__":
    #    token = get_token()

    #    if no_positions(token):
    #        print("建玉は一切ありません")
    #    else:
    #        print("建玉があります")

def no_active_orders(token: str) -> bool:
    """
    発注中（Stateが1〜4）の注文が存在しない場合に True を返す。

    Args:
        token (str): kabuステーショントークン

    Returns:
        bool: 発注中の注文が無ければ True、それ以外は False

    定義値	説明
    1	待機（発注待機）
    2	処理中（発注送信中）
    3	処理済（発注済・訂正済）
    4	訂正取消送信中
    5	終了（発注エラー・取消済・全約定・失効・期限切れ）
    """
    try:
        orders = get_orders(token)
        for order in orders:
            state = order.get("State")  # または order.get("OrderState")
            if state in [1, 2, 3, 4]:
                return False
        return True
    except Exception as e:
        print(f"[ERROR] 注文状態の確認中にエラー: {e}")
        return False

    # 実行例
    #if __name__ == "__main__":
    #    token = get_token()

    #    if no_active_orders(token):
    #        print("発注中の注文はありません")
    #    else:
    #        print("現在発注中の注文があります")