import os
import json

# 設定ファイルのパス
SETTINGS_PATH = os.path.join(os.path.dirname(__file__), "settings.json")

# デフォルト値（ファイルがない場合のフォールバック用）
DEFAULT_SETTINGS = {
    "API_PASSWORD": "your_kabu_api_password",
    "API_BASE_URL": "http://localhost:18080/kabusapi",
    "FUTURE_CODE": "NK225mini",
    "ENABLE_TICK_OUTPUT": "true"
}


def load_settings() -> dict:
    """
    settings.json を読み込んで辞書として返す。
    存在しない場合はデフォルト設定を返す。
    """
    if os.path.exists(SETTINGS_PATH):
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return DEFAULT_SETTINGS.copy()


# グローバル設定変数
SETTINGS = load_settings()
ENABLE_TICK_OUTPUT = SETTINGS.get("ENABLE_TICK_OUTPUT", True)

# 各種設定値にアクセスするためのエイリアス
API_BASE_URL = SETTINGS.get("API_BASE_URL")
API_PASSWORD = SETTINGS.get("API_PASSWORD")
FUTURE_CODE = SETTINGS.get("FUTURE_CODE")


def get_api_password() -> str:
    return API_PASSWORD
