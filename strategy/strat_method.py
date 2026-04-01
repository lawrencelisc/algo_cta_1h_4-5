import pandas as pd
import numpy as np
import os
from pathlib import Path
from loguru import logger
from datetime import datetime


class CreateSignal:
    strat_list = ['cta_momentum']
    strat_folder = Path(__file__).parent.parent / 'data' / 'StratData'
    signal_folder = Path(__file__).parent.parent / 'data' / 'Signal'
    signal_filename = 'signal_table.csv'
    signal_path = signal_folder / signal_filename

    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df
        self.signal_folder.mkdir(parents=True, exist_ok=True)

        wfa_path = Path(__file__).parent.parent / 'config' / 'wfa_best_params.csv'
        try:
            self.wfa_df = pd.read_csv(wfa_path).set_index('symbol')
        except Exception:
            self.wfa_df = pd.DataFrame()

    def strat_cta_momentum(self, row):
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        res: str = '1h'

        strat_filename = f"{name}_{res}_{symbol}.csv"
        file_path = self.strat_folder / res / strat_filename

        try:
            df = pd.read_csv(file_path, index_col=0)
            df.index = pd.to_datetime(df.index)
        except Exception as e:
            logger.error(f"Failed to read data for {symbol}: {e}")
            return 0

        required_cols = ['SMA_200', 'Donchian_High', 'Donchian_Low', 'K_Body_Abs', 'ATR_14', 'VIX_Proxy']
        if not all(col in df.columns for col in required_cols):
            return 0

        best_atr_mult = 0.5
        if not self.wfa_df.empty and symbol in self.wfa_df.index:
            best_atr_mult = float(self.wfa_df.loc[symbol, 'best_atr_mult'])

        # ====================================================
        # 🌟 核心升級：VIX 恐慌指數過濾器 (雙向安全閥)
        # ====================================================
        VIX_MIN = 20.0  # 底線：市場太死寂(沒波動)時不進場
        VIX_MAX = 70.0  # 天花板：市場太瘋狂(如黑天鵝/瘋狂插針)時強制罷工不進場

        # 必須同時符合大於底線且小於天花板，才算是「安全的波動環境」
        cond_vix = (df['VIX_Proxy'] > VIX_MIN) & (df['VIX_Proxy'] < VIX_MAX)
        cond_mom = df['K_Body_Abs'] > (best_atr_mult * df['ATR_14'])
        # ====================================================

        # 進場：突破順勢通道
        long_entry = cond_vix & cond_mom & (df['close'] > df['SMA_200']) & (df['close'] > df['Donchian_High'].shift(1))
        short_entry = cond_vix & cond_mom & (df['close'] < df['SMA_200']) & (df['close'] < df['Donchian_Low'].shift(1))

        # 出場：跌破反向通道
        long_exit = df['close'] < df['Donchian_Low'].shift(1)
        short_exit = df['close'] > df['Donchian_High'].shift(1)

        # 實盤中的倉位狀態機
        le = long_entry.to_numpy()
        se = short_entry.to_numpy()
        lx = long_exit.to_numpy()
        sx = short_exit.to_numpy()

        signals = np.zeros(len(df))
        current_pos = 0

        for i in range(len(df)):
            if current_pos == 0:
                if le[i]:
                    current_pos = 1
                elif se[i]:
                    current_pos = -1
            elif current_pos == 1:
                if lx[i]: current_pos = 0
            elif current_pos == -1:
                if sx[i]: current_pos = 0
            signals[i] = current_pos

        df['signal'] = signals
        df.to_csv(file_path)

        # 回傳最新一根 K 線的倉位狀態
        return int(df['signal'].iloc[-1])

    def split_sub(self):
        count = 0
        combine_signal_df = pd.DataFrame()

        for _, row in self.strat_df.iterrows():
            current_signal = self.strat_cta_momentum(row)
            count += 1
            lastest_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            new_row = {
                'date': lastest_date,
                'name': str(row['name']),
                'symbol': str(row['symbol']),
                'saved_csv': f"{row['name']}_1h_{row['symbol']}.csv",
                'signal': current_signal
            }
            new_row_df = pd.DataFrame([new_row])

            if combine_signal_df.empty:
                combine_signal_df = new_row_df
            else:
                combine_signal_df = pd.concat([combine_signal_df, new_row_df], ignore_index=True)

        if os.path.exists(self.signal_path):
            try:
                existing_signal_df = pd.read_csv(self.signal_path)
                combined_df = pd.concat([existing_signal_df, combine_signal_df], ignore_index=True)
            except pd.errors.EmptyDataError:
                combined_df = combine_signal_df
        else:
            combined_df = combine_signal_df

        combined_df.to_csv(self.signal_path, index=False)
        return combine_signal_df