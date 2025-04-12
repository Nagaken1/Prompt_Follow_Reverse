from datetime import datetime
from utils.symbol_resolver import get_active_term, get_symbol_code

class Order:
    def __init__(self, token):
        self.token = token  # APIトークンを保持
        self._cached_term = None  # 限月コードのキャッシュ
        self._cached_symbol = None  # 銘柄コードのキャッシュ

    def _send_order(self, payload):
        # 実際のAPI送信処理（共通化）
        print(f"Sending order with payload: {payload}")
        # API呼び出しの処理をここで実装
    
    def _get_cached_symbol(self):
        # 限月コードがキャッシュされているか確認
        now = datetime.now()
        active_term = get_active_term(now)
        
        if self._cached_term != active_term:
            # 限月コードが変更された場合、再取得
            self._cached_term = active_term
            self._cached_symbol = get_symbol_code(active_term, self.token)
            if not self._cached_symbol:
                print(f"[ERROR] 銘柄コードが取得できませんでした。")
                return None
        
        return self._cached_symbol
    
    def buy_entry_market(self, qty: int):
        # キャッシュから銘柄コードを取得
        symbol = self._get_cached_symbol()
        if not symbol:
            return
        
        # 成行注文のパラメータ
        payload = {
            "Symbol": symbol,
            "Exchange": 23,
            "TradeType": 1,
            "TimeInForce": 1,
            "Side": "1",  # 買い注文
            "Qty": qty,
            "Price": 0,  # 成行なので価格は0
            "OrdType": 1  # 成行注文
        }

        # 注文送信
        self._send_order(payload)