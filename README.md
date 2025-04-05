# PFR（Prompt Follow Reverse）システムの概要

このシステムは、日経225mini先物のティックデータをリアルタイムで受信し、1分足のOHLC（Open, High, Low, Close）を構築・出力するソフトウェアです。
さらに、後に売買戦略を追加できるように設計されています。

---

## 🔧 構成ファイルと役割

```
Prompt_Follow_Reverse/
├── PFR_main.py              - メイン制御ループ
├── dummy_tick_server/
│   └── DummyServerWebSocket.py  - ティックデータの送信シミュレーター
├── writer/
│   ├── ohlc_writer.py       - OHLCのファイル出力
│   └── tick_writer.py       - ティックデータの記録
├── handler/
│   └── price_handler.py     - ティック処理・OHLC管理
├── utils/
│   ├── time_util.py         - 時間帯の判定（ザラバ、プレクロージングなど）
│   ├── export_util.py       - 最新3分データの出力補助
│   ├── future_info_util.py  - 限月の判定
│   └── symbol_resolver.py   - 銘柄IDの補助
├── config/
│   └── settings.json        - Pythonパスなどの設定
└── csv/                     - 出力されたOHLCファイル群
```

---

## 🧠 処理の流れ（簡易フロー）

1. **DummyServerWebSocket.py** からティック送信
2. **price_handler.py** がティックを受信
3. ティックをもとに OHLC を構築
4. 1分足が確定すればファイル出力
5. `export_latest_minutes_to_pd` により直近3分間のDataFrameを取得・表示

---

## 📌 売買戦略を実装するには？

以下のような `strategy.py` を作成し、1分毎に呼び出せるようにしてください：

```python
def on_new_minute(df):
    # dfは直近3分のデータ
    latest = df.iloc[-1]
    if latest["Close"] > latest["Open"]:
        print("買いシグナル")
    else:
        print("売りシグナル")
```

---

## ⚠️ 注意点

- ティックは **1秒間に複数回届きます** が、OHLCは1分ごとに出力されます
- `price_handler` が常に最新ティックを保持しているので、
  1分の境目で `handle_tick` を使えば **その分の最初のティックの価格** が取得可能です
- クロージング（15:45、6:00）では特別な処理があります

---

## ✅ 実行方法

1. `DummyServerWebSocket.py` を右クリック or コマンドで起動
2. `PFR_main.py` を実行

---

## 📁 出力ファイル

- `csv/` に `yyyymmdd_nikkei_mini_future.csv` が1分ごとに生成・追記されます
- 各行が1分足のOHLCデータです

---

## 📞 お問い合わせ

不明点があれば、チームリーダーまたは開発者までご連絡ください。
