import os
import csv
import pandas as pd

class LevelManager:
    def __init__(self):
        self.filepath = os.path.join("csv", "levels.csv")
        self.headers = ["Resistance", "Support"]

        # ファイルが存在しなければ初期化
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()
                # 初期値を1行書く
                writer.writerow({"Resistance": "", "Support": ""})

    def read_levels(self) -> dict:
        """CSVファイルから1行目の Resistance と Support を読み取る"""
        with open(self.filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                return {
                    "Resistance": row["Resistance"],
                    "Support": row["Support"]
                }
        return {"Resistance": "", "Support": ""}  # 空なら

    def write_levels(self, resistance: str, support: str):
        """1行目の Resistance と Support を上書き"""
        rows = []

        # 既存行を読み取り
        with open(self.filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # 最初の行を更新（なければ新規追加）
        if rows:
            rows[0]["Resistance"] = resistance
            rows[0]["Support"] = support
        else:
            rows.append({"Resistance": resistance, "Support": support})

        # 書き戻す
        with open(self.filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()
            writer.writerows(rows)

class Rule_Class:
    def __init__(self, df, latest_price):

        self.df = df
        self.latest_price =  latest_price
        self.Resistance = None
        self.Support = None
        self.Signal = None

    def Main(self):
        lm = LevelManager()

        #抵抗線・支持線の更新
        levels = lm.read_levels()

        if not levels["Resistance"]:
            # High列の最初の3行での最大値を求め、抵抗線とする
            new_resistance = self.df.iloc[:3]['High'].max()
            lm.write_levels(resistance=new_resistance, support=levels.get("Support"))
            self.Resistance = new_resistance

        if not levels["Support"]:
            # Low列の最初の3行での最小値を求め、支持線とする
            new_support = self.df.iloc[:3]['Low'].min()
            lm.write_levels(resistance=levels.get("Resistance"), support=new_support)
            self.Support = new_support

        if self.df.at[0,'High'] < self.df.at[1,'High'] and self.df.at[1,'High'] > self.df.at[2,'High']:
            #抵抗線更新された
            new_resistance = self.df.at[1,'High']
            lm.write_levels(resistance=new_resistance, support=levels.get("Support"))
            self.Resistance = new_resistance
        else:
            self.Resistance=levels.get("Resistance")

        if self.df.at[0,'Low'] > self.df.at[1,'Low'] and self.df.at[1,'Low'] < self.df.at[2,'Low']:
            #支持線更新された
            new_support = self.df.at[1,'Low']
            lm.write_levels(resistance=levels.get("Resistance"), support=new_support)
            self.Support = new_support
        else:
            self.Support=levels.get("Support")

        #シグナル生成
        if self.latest_price < self.Resistance:
            self.Signal = 1

        elif self.latest_price > self.Support:
            self.Signal = -1

        else:
            self.Signal = 0