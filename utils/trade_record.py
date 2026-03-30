###修改說明###
# 1. 移除舊版基於 tp_pct / sl_pct 的運算邏輯。
# 2. 修改函數參數，直接接收從 execution.py 傳來的 size, entry_price, stop_loss_price。
# 3. 專注於 CSV 記錄與簡單的部位狀態管理，不再做重複的價格抓取。

import os
import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone


class TradeRecord:
    def __init__(self, signal_df: pd.DataFrame):
        self.signal_df = signal_df
        trade_folder = Path(__file__).parent.parent / 'data' / 'Trade'
        trade_folder.mkdir(parents=True, exist_ok=True)

        self.file_path_trade = trade_folder / 'bybit_trade_record.csv'

        # 定義記錄表的欄位
        self.col_order = [
            'rec_time', 'symbol', 'action', 'side', 'size',
            'entry_price', 'stop_loss_price', 'close_price', 'pnl'
        ]

        if not os.path.exists(self.file_path_trade) or os.path.getsize(self.file_path_trade) == 0:
            df_empty = pd.DataFrame(columns=self.col_order)
            df_empty.to_csv(self.file_path_trade, index=False)

    def _log_trade(self, symbol, action, side, size, entry_price, stop_loss_price, close_price=0.0, pnl=0.0):
        """統一的 CSV 寫入函數"""
        rec_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        new_row = {
            'rec_time': rec_time,
            'symbol': symbol,
            'action': action,  # e.g., 'open', 'close'
            'side': side,  # e.g., 'long', 'short'
            'size': size,
            'entry_price': entry_price,
            'stop_loss_price': stop_loss_price,
            'close_price': close_price,
            'pnl': pnl
        }

        df_new = pd.DataFrame([new_row])
        df_new.to_csv(self.file_path_trade, mode='a', index=False, header=False)
        logger.info(f"Trade recorded: {action} {side} {symbol} | Size: {size}")

    def open_long(self, symbol, size, entry_price, stop_loss_price):
        self._log_trade(symbol, 'open', 'long', size, entry_price, stop_loss_price)
        return True

    def open_short(self, symbol, size, entry_price, stop_loss_price):
        self._log_trade(symbol, 'open', 'short', size, entry_price, stop_loss_price)
        return True

    def close_long(self, symbol, size, close_price, pnl):
        self._log_trade(symbol, 'close', 'long', size, 0.0, 0.0, close_price, pnl)
        return True

    def close_short(self, symbol, size, close_price, pnl):
        self._log_trade(symbol, 'close', 'short', size, 0.0, 0.0, close_price, pnl)
        return True