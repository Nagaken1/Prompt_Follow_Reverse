def fund_management_by_fixed_ratio(account_balance: float, required_margin_amount: float, max_loss: float) -> int:
    """
    固定比率による資金管理計算

    :param account_balance: 有効証拠金
    :param required_margin_amount: 当初証拠金
    :param max_loss: 最大損失
    :return: 建て玉（取引）可能な最大数
    """
    try:
        delta = required_margin_amount + max_loss
        next_amount_level = 0.0
        num_transaction = 0
        before_result = 0

        while account_balance >= next_amount_level:
            num_transaction += 1

            if num_transaction == 1:
                before_result = 1
            else:
                before_result += num_transaction - 1

            next_amount_level = before_result * delta

        return num_transaction - 1

    except Exception as e:
        print(f"[ERROR] fund_management_by_fixed_ratio 実行時エラー: {e}")
        return 0